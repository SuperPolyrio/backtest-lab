"""Async registry facade over the existing paper registry tables.

The project stores Phase-1 registry state in ``quant.paper_*`` tables, with
``paper_market_registry_tokens`` as the token-centric read model.  This facade
keeps the module boundary requested by the dynamic registry spec while routing
all durable writes through the existing repository implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .models import LifecycleSignal, NormalizedMarket, NormalizedMarketToken
from .repository import MarketRegistryRepository, _jsonb
from .token_universe import MarketRegistryToken, MarketUniverseConfig, compute_universe_decisions


@dataclass(frozen=True)
class UpsertResult:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0


MarketRecord = dict[str, Any]


class MarketRegistry:
    """Idempotent registry operations used by reconcilers and tests."""

    def __init__(self, conn: Any, *, config: MarketUniverseConfig | None = None) -> None:
        self.conn = conn
        self.repo = MarketRegistryRepository(conn)
        self.config = config or MarketUniverseConfig()

    async def upsert_market(self, market: NormalizedMarket) -> UpsertResult:
        existing = await self.get_market(market.market_id)
        old_hash = str(existing.get("metadata_hash") or "") if existing else ""
        unchanged = bool(existing and old_hash == market.metadata_hash)
        state = _market_state(market)
        source = str(market.raw.get("_registry_source") or "market_registry")
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.paper_market_registry_markets (
                    market_key, market_id, gamma_market_id, condition_id, market_slug, market_title,
                    market_state, active, closed, resolved, archived, deprecated,
                    token_count, subscription_token_count, execution_token_count,
                    winning_asset_id, winning_outcome, resolution_status, resolution_source, resolved_time,
                    metadata_hash, last_source, raw_metadata, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, FALSE,
                    0, 0, 0,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, now()
                )
                ON CONFLICT (market_key) DO UPDATE SET
                    market_id = COALESCE(EXCLUDED.market_id, quant.paper_market_registry_markets.market_id),
                    gamma_market_id = COALESCE(EXCLUDED.gamma_market_id, quant.paper_market_registry_markets.gamma_market_id),
                    condition_id = COALESCE(EXCLUDED.condition_id, quant.paper_market_registry_markets.condition_id),
                    market_slug = COALESCE(EXCLUDED.market_slug, quant.paper_market_registry_markets.market_slug),
                    market_title = COALESCE(EXCLUDED.market_title, quant.paper_market_registry_markets.market_title),
                    market_state = EXCLUDED.market_state,
                    active = EXCLUDED.active,
                    closed = EXCLUDED.closed,
                    resolved = EXCLUDED.resolved,
                    archived = EXCLUDED.archived,
                    winning_asset_id = COALESCE(EXCLUDED.winning_asset_id, quant.paper_market_registry_markets.winning_asset_id),
                    winning_outcome = COALESCE(EXCLUDED.winning_outcome, quant.paper_market_registry_markets.winning_outcome),
                    resolution_status = COALESCE(EXCLUDED.resolution_status, quant.paper_market_registry_markets.resolution_status),
                    resolution_source = COALESCE(EXCLUDED.resolution_source, quant.paper_market_registry_markets.resolution_source),
                    resolved_time = COALESCE(EXCLUDED.resolved_time, quant.paper_market_registry_markets.resolved_time),
                    metadata_hash = EXCLUDED.metadata_hash,
                    last_source = EXCLUDED.last_source,
                    raw_metadata = EXCLUDED.raw_metadata,
                    updated_at = now()
                """,
                (
                    _market_key(market),
                    _int_or_none(market.market_id),
                    str(market.market_id),
                    market.condition_id,
                    market.slug,
                    market.question,
                    state,
                    bool(market.active) if market.active is not None else True,
                    bool(market.closed) if market.closed is not None else False,
                    bool(market.is_resolved) if market.is_resolved is not None else False,
                    bool(market.archived) if market.archived is not None else False,
                    market.winning_asset_id,
                    market.winning_outcome,
                    market.resolution_status,
                    source if market.is_resolved else None,
                    None,
                    market.metadata_hash,
                    source,
                    _jsonb(market.raw),
                ),
            )
        if unchanged:
            return UpsertResult(unchanged=1)
        event_type = "MARKET_METADATA_UPDATED" if existing else "MARKET_DISCOVERED"
        event = LifecycleSignal(
            source=source,
            event_type=event_type,
            market_id=market.market_id,
            condition_id=market.condition_id,
            asset_id=None,
            source_ts=None,
            local_receive_ts=_datetime_or_now(market.raw.get("_local_receive_ts")),
            payload=market.raw,
            payload_hash=market.metadata_hash,
        )
        await self.write_lifecycle_event(event, existing.get("market_state") if existing else None, state, event_type.lower())
        return UpsertResult(updated=1 if existing else 0, inserted=0 if existing else 1)

    async def upsert_tokens(self, tokens: list[NormalizedMarketToken]) -> UpsertResult:
        counts: dict[str, int] = {}
        for token in tokens:
            counts[token.market_id] = counts.get(token.market_id, 0) + 1
        registry_tokens = [_to_registry_token(token, token_count=counts.get(token.market_id, len(tokens))) for token in tokens]
        decisions = compute_universe_decisions(registry_tokens, config=self.config)
        run_id = self.repo.begin_sync_run("market_registry_upsert", meta={"token_count": len(tokens)})
        try:
            summary = self.repo.persist_decisions(decisions, run_id=run_id, source="market_registry")
            self.repo.finish_sync_run(run_id, status="success", summary=summary)
            return UpsertResult(inserted=summary.tokens_upserted)
        except Exception as exc:
            try:
                self.conn.rollback()
            except Exception:
                pass
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    async def write_lifecycle_event(
        self,
        event: LifecycleSignal,
        old_state: str | None,
        new_state: str | None,
        reason: str,
    ) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.paper_market_lifecycle_events (
                    asset_id, market_id, condition_id, event_type,
                    old_state, new_state, source, reason, raw_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING event_id
                """,
                (
                    event.asset_id,
                    _int_or_none(event.market_id),
                    event.condition_id,
                    event.event_type,
                    old_state,
                    new_state,
                    event.source,
                    reason,
                    _jsonb(event.payload),
                ),
            )
            row = cur.fetchone()
        return int(row["event_id"])

    async def get_market(self, market_id: str) -> MarketRecord | None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM quant.paper_market_registry_markets
                WHERE market_key = %s OR market_id::text = %s OR gamma_market_id = %s OR condition_id = %s OR market_slug = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (str(market_id), str(market_id), str(market_id), str(market_id), str(market_id)),
            )
            row = cur.fetchone()
            if row:
                return dict(row)
            cur.execute(
                """
                SELECT *
                FROM quant.paper_market_registry_tokens
                WHERE market_id::text = %s OR gamma_market_id = %s OR condition_id = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (str(market_id), str(market_id), str(market_id)),
            )
            row = cur.fetchone()
        return dict(row) if row else None

    async def get_market_by_asset_id(self, asset_id: str) -> MarketRecord | None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM quant.paper_market_registry_tokens WHERE asset_id = %s", (str(asset_id),))
            row = cur.fetchone()
        return dict(row) if row else None

    async def list_candidate_subscription_tokens(self) -> list[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT asset_id
                FROM quant.paper_candidate_market_registry_tokens
                ORDER BY updated_at DESC
                """
            )
            return [str(row["asset_id"]) for row in cur.fetchall()]

    async def list_execution_tokens(self) -> list[str]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT asset_id
                FROM quant.paper_execution_market_registry_tokens
                ORDER BY updated_at DESC
                """
            )
            return [str(row["asset_id"]) for row in cur.fetchall()]


def _to_registry_token(token: NormalizedMarketToken, *, token_count: int) -> MarketRegistryToken:
    return MarketRegistryToken(
        asset_id=token.asset_id,
        market_id=_int_or_zero(token.market_id),
        gamma_market_id=str(token.market_id),
        condition_id=token.condition_id,
        outcome_name=(token.outcome_name or "UNKNOWN").upper(),
        outcome_index=token.outcome_index,
        active=True,
        closed=False,
        resolved=False,
        status_present=True,
        token_count=token_count,
        book_status=None,
        latest_book_at=None,
        source="market_registry",
    )


def _market_key(market: NormalizedMarket) -> str:
    for value in (market.condition_id, market.market_id, market.slug):
        text = str(value or "").strip()
        if text:
            return text
    raise ValueError("market_id, condition_id, or slug is required")


def _market_state(market: NormalizedMarket) -> str:
    if market.is_resolved or market.winning_asset_id or market.winning_outcome:
        return "RESOLVED"
    if market.archived:
        return "ARCHIVED"
    if market.closed or market.active is False or market.accepting_orders is False or market.enable_order_book is False:
        return "CLOSING"
    if not market.condition_id:
        return "DISCOVERED"
    return "TRADABLE_PENDING_BOOK"


def _int_or_none(value: object) -> int | None:
    try:
        return int(str(value))
    except Exception:
        return None


def _int_or_zero(value: object) -> int:
    parsed = _int_or_none(value)
    return int(parsed or 0)


def _datetime_or_now(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)
