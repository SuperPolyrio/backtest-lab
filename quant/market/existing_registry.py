"""Read-only paper market registry adapter over the existing Postgres schema."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from .token_universe import (
    MarketRegistryToken,
    MarketUniverseConfig,
    TokenUniverseDiff,
    build_token_universe_diff,
    compute_universe_decisions,
)


class ExistingMarketRegistry:
    """Expose the existing ``core.*`` market data as a paper registry source."""

    def __init__(self, conn: Any, *, config: MarketUniverseConfig | None = None) -> None:
        self.conn = conn
        self.config = config or MarketUniverseConfig()

    def fetch_tokens(
        self,
        *,
        limit: int | None = 5_000,
        market_slug: str | None = None,
        asset_id: str | None = None,
        asset_ids: list[str] | None = None,
        candidates_only: bool = True,
        include_book: bool = True,
    ) -> list[MarketRegistryToken]:
        params: list[Any] = []
        filters = ["mt.token_id IS NOT NULL", "mt.token_id <> ''"]
        if market_slug:
            filters.append("m.slug = %s")
            params.append(market_slug)
        if asset_id:
            filters.append("mt.token_id = %s")
            params.append(str(asset_id))
        if asset_ids:
            filters.append("mt.token_id = ANY(%s)")
            params.append([str(item) for item in asset_ids])
        if candidates_only:
            filters.append("COALESCE(mt.active, TRUE) = TRUE")
            filters.append("m.condition_id IS NOT NULL")
            filters.append("m.condition_id <> ''")
            filters.append("COALESCE(mss.is_trading_closed, FALSE) = FALSE")
            filters.append("COALESCE(mss.is_resolved, FALSE) = FALSE")
            if self.config.require_status_snapshot:
                filters.append("mss.market_id IS NOT NULL")
            if self.config.exclude_placeholders:
                filters.append("m.slug NOT ILIKE 'trade-indexer-placeholder-%%'")
        limit_sql = ""
        order_sql = ""
        if limit is not None:
            # A bounded discovery request needs a deterministic newest-first
            # slice.  An unbounded reconciliation consumes every row, so the
            # same wide-row ORDER BY has no semantic value and can spill more
            # than PostgreSQL's temp_file_limit for the full token universe.
            order_sql = "ORDER BY m.created_at DESC NULLS LAST, m.id DESC, mt.outcome_index ASC, mt.token_id ASC"
            limit_sql = "LIMIT %s"
            params.append(max(0, int(limit)))
        book_columns = """
                COALESCE(lb.snapshot_timestamp, lb.fetched_at) AS latest_book_at,
                lb.book_status,
                lb.best_bid,
                lb.best_ask,
                lb.source AS book_source,
                lb.storage_tier,
        """ if include_book else """
                NULL::timestamptz AS latest_book_at,
                NULL::text AS book_status,
                NULL::numeric AS best_bid,
                NULL::numeric AS best_ask,
                NULL::text AS book_source,
                NULL::text AS storage_tier,
        """
        book_join = """
            LEFT JOIN LATERAL (
                SELECT
                    obs.snapshot_timestamp,
                    obs.fetched_at,
                    obs.book_status,
                    obs.best_bid,
                    obs.best_ask,
                    obs.source,
                    obs.storage_tier
                FROM quant.clob_orderbook_snapshots obs
                WHERE obs.token_id = mt.token_id
                ORDER BY COALESCE(obs.snapshot_timestamp, obs.fetched_at) DESC, obs.snapshot_id DESC
                LIMIT 1
            ) lb ON TRUE
        """ if include_book else ""

        sql = f"""
            SELECT
                mt.token_id AS asset_id,
                m.id AS market_id,
                m.gamma_market_id,
                m.slug AS market_slug,
                COALESCE(m.title, m.slug) AS market_title,
                m.condition_id,
                UPPER(COALESCE(mt.outcome, 'UNKNOWN')) AS outcome_name,
                mt.outcome_index,
                COALESCE(mt.active, TRUE) AS active,
                COALESCE(mss.is_trading_closed, FALSE) AS closed,
                COALESCE(mss.is_resolved, FALSE) AS resolved,
                (
                    m.slug ILIKE 'arch-%%'
                    OR m.slug ILIKE '%%-arch-%%'
                    OR m.title ILIKE 'ARCH:%%'
                    OR m.title ILIKE '[ARCH]%%'
                ) AS archived,
                (
                    m.slug ILIKE '%%deprecated%%'
                    OR m.title ILIKE '%%deprecated%%'
                ) AS deprecated,
                (mss.market_id IS NOT NULL) AS status_present,
                mss.completion_status,
                COALESCE(tc.token_count, 0) AS token_count,
                COALESCE(mt.end_date, m.end_date) AS end_date,
                {book_columns}
                win.token_id AS winning_asset_id,
                UPPER(mss.settlement_outcome) AS winning_outcome,
                CASE
                    WHEN COALESCE(mss.is_resolved, FALSE) AND mss.settlement_outcome IS NOT NULL THEN 'RESOLVED'
                    ELSE NULL
                END AS resolution_status,
                CASE
                    WHEN COALESCE(mss.is_resolved, FALSE) AND mss.settlement_outcome IS NOT NULL
                        THEN COALESCE(mss.settlement_source, mss.completion_source, 'core.market_status_snapshot')
                    ELSE NULL
                END AS resolution_source,
                CASE
                    WHEN COALESCE(mss.is_resolved, FALSE) AND mss.settlement_outcome IS NOT NULL
                        THEN COALESCE(mss.settlement_event_time, mss.completion_time, mss.gamma_closed_time, mss.updated_at)
                    ELSE NULL
                END AS resolved_time
            FROM core.market_tokens mt
            JOIN core.markets m ON m.id = mt.market_id
            LEFT JOIN core.market_status_snapshot mss ON mss.market_id = m.id
            LEFT JOIN LATERAL (
                SELECT mtw.token_id
                FROM core.market_tokens mtw
                WHERE mtw.market_id = m.id
                  AND UPPER(COALESCE(mtw.outcome, '')) = UPPER(COALESCE(mss.settlement_outcome, ''))
                  AND mtw.token_id IS NOT NULL
                  AND mtw.token_id <> ''
                ORDER BY mtw.outcome_index ASC NULLS LAST, mtw.token_id ASC
                LIMIT 1
            ) win ON TRUE
            LEFT JOIN LATERAL (
                SELECT COUNT(*) AS token_count
                FROM core.market_tokens mtc
                WHERE mtc.market_id = m.id
                  AND mtc.token_id IS NOT NULL
                  AND mtc.token_id <> ''
                  AND COALESCE(mtc.active, TRUE) = TRUE
            ) tc ON TRUE
            {book_join}
            WHERE {" AND ".join(filters)}
            {order_sql}
            {limit_sql}
        """
        with self.conn.cursor() as cur:
            previous_work_mem: str | None = None
            if limit is None:
                # The authoritative universe is a few hundred thousand rows
                # joined against multi-million-row source tables.  PostgreSQL's
                # default work_mem made the hash plan spill past the 1 GiB
                # temp_file_limit.  Raise memory only for this one unbounded
                # read, then restore it before the caller continues the sync.
                cur.execute("SELECT current_setting('work_mem') AS work_mem")
                setting = cur.fetchone()
                previous_work_mem = str(setting["work_mem"])
                cur.execute(
                    "SELECT set_config('work_mem', %s, true)",
                    ("512MB",),
                )
            cur.execute(sql, params)
            rows = cur.fetchall()
            if previous_work_mem is not None:
                cur.execute(
                    "SELECT set_config('work_mem', %s, true)",
                    (previous_work_mem,),
                )
            return [MarketRegistryToken.from_row(dict(row)) for row in rows]

    def fetch_tokens_by_asset_ids(
        self,
        asset_ids: list[str],
        *,
        include_book: bool = True,
    ) -> list[MarketRegistryToken]:
        ids = [str(item).strip() for item in asset_ids if str(item).strip()]
        if not ids:
            return []
        return self.fetch_tokens(
            limit=None,
            asset_ids=ids,
            candidates_only=False,
            include_book=include_book,
        )

    def source_watermark(self) -> datetime:
        """Return a DB-clock upper bound for an incremental source read."""

        with self.conn.cursor() as cur:
            cur.execute("SELECT clock_timestamp() AS source_watermark")
            row = cur.fetchone()
        return row["source_watermark"]

    def list_changed_asset_ids(
        self,
        *,
        since: datetime,
        until: datetime,
    ) -> list[str]:
        """Return every token from markets changed inside a closed source window."""

        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH changed_markets AS (
                    SELECT mt.market_id
                    FROM core.market_tokens mt
                    WHERE mt.updated_at >= %s
                      AND mt.updated_at < %s
                    UNION
                    SELECT mss.market_id
                    FROM core.market_status_snapshot mss
                    WHERE mss.updated_at >= %s
                      AND mss.updated_at < %s
                )
                SELECT mt.token_id AS asset_id
                FROM changed_markets changed
                JOIN core.market_tokens mt ON mt.market_id = changed.market_id
                WHERE mt.token_id IS NOT NULL
                  AND mt.token_id <> ''
                ORDER BY mt.token_id
                """,
                (since, until, since, until),
            )
            return [str(row["asset_id"]) for row in cur.fetchall()]

    def snapshot_and_diff(
        self,
        *,
        limit: int | None = 5_000,
        previous: TokenUniverseDiff | None = None,
        generation: int | None = None,
        now: datetime | None = None,
    ) -> TokenUniverseDiff:
        decisions = compute_universe_decisions(
            self.fetch_tokens(limit=limit),
            config=self.config,
            now=now,
        )
        return build_token_universe_diff(decisions, previous=previous, generation=generation)

    def fetch_source_counts(self) -> dict[str, Any]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM core.markets) AS markets_total,
                    (
                        SELECT COUNT(*)
                        FROM core.market_tokens
                        WHERE token_id IS NOT NULL AND token_id <> ''
                    ) AS tokens_total,
                    (
                        SELECT COUNT(*)
                        FROM core.market_status_snapshot
                        WHERE completion_status = 'OPEN'
                    ) AS open_markets_with_status,
                    (
                        SELECT COUNT(*)
                        FROM core.market_status_snapshot
                        WHERE is_trading_closed = TRUE
                    ) AS trading_closed_markets,
                    (
                        SELECT COUNT(*)
                        FROM core.markets m
                        LEFT JOIN core.market_status_snapshot mss ON mss.market_id = m.id
                        WHERE mss.market_id IS NULL
                    ) AS markets_without_status_snapshot,
                    (
                        SELECT COUNT(*)
                        FROM core.markets
                        WHERE slug ILIKE 'trade-indexer-placeholder-%%'
                    ) AS placeholder_markets,
                    (
                        SELECT COUNT(*)
                        FROM quant.clob_orderbook_snapshots
                        WHERE COALESCE(snapshot_timestamp, fetched_at)
                            >= now() - (%s::text || ' seconds')::interval
                    ) AS fresh_book_snapshots
                """,
                [int(self.config.book_ttl_seconds)],
            )
            row = cur.fetchone()
        return dict(row or {})
