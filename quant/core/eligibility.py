"""Eligibility rules for quant price builds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .db import ClickHouseClient


@dataclass(frozen=True)
class TradeStats:
    token_id: str
    trade_count: int
    first_block: int | None
    last_block: int | None


def fetch_orderfilled_trade_stats(ch: ClickHouseClient, token_ids: list[str]) -> dict[str, TradeStats]:
    if not token_ids:
        return {}
    quoted = ",".join("'" + token.replace("\\", "\\\\").replace("'", "\\'").lower() + "'" for token in token_ids)
    table = ch.settings.orderfilled_table
    rows = ch.query_json_rows(
        f"""
        SELECT
            token_key AS token_id,
            count() AS trade_count,
            min(block_number) AS first_block,
            max(block_number) AS last_block
        FROM (
            SELECT lower(token_id) AS token_key, block_number
            FROM {table}
            WHERE lower(token_id) IN ({quoted})
        )
        GROUP BY token_key
        """
    )
    stats: dict[str, TradeStats] = {}
    for row in rows:
        token_id = str(row.get("token_id") or "").lower()
        if not token_id:
            continue
        stats[token_id] = TradeStats(
            token_id=token_id,
            trade_count=int(row.get("trade_count") or 0),
            first_block=int(row["first_block"]) if row.get("first_block") is not None else None,
            last_block=int(row["last_block"]) if row.get("last_block") is not None else None,
        )
    return stats


def refresh_eligibility(conn: Any, ch: ClickHouseClient | None = None, *, batch_size: int = 1000) -> int:
    """Refresh eligibility rows.

    A token is eligible when it is not archived/deprecated/duplicate and has at
    least one OrderFilled trade. Frontend-only no-trade markets are deliberately
    excluded in phase one, matching the backtest use case.
    """

    total = 0
    offset = 0
    client = ch or ClickHouseClient()
    while True:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    token_id, token_id_hex, market_id, market_slug, token_side, archived, deprecated,
                    duplicate_group_key,
                    row_number() OVER (
                        PARTITION BY duplicate_group_key, token_side
                        ORDER BY created_at NULLS LAST, market_id ASC
                    ) AS duplicate_rank
                FROM quant.market_token_metadata
                ORDER BY market_id ASC, token_id ASC
                LIMIT %s OFFSET %s
                """,
                (batch_size, offset),
            )
            rows = [dict(row) for row in cur.fetchall()]
        if not rows:
            break
        stats = fetch_orderfilled_trade_stats(client, [str(row["token_id_hex"]) for row in rows if row.get("token_id_hex")])
        upserts = []
        for row in rows:
            token_id = str(row["token_id"])
            token_id_hex = str(row.get("token_id_hex") or "").lower()
            token_stats = stats.get(token_id_hex)
            trade_count = token_stats.trade_count if token_stats else 0
            archived = bool(row.get("archived"))
            deprecated = bool(row.get("deprecated"))
            duplicate = int(row.get("duplicate_rank") or 1) > 1
            eligible = bool(trade_count > 0 and not archived and not deprecated and not duplicate)
            reasons = []
            if trade_count <= 0:
                reasons.append("no_orderfilled_trades")
            if archived:
                reasons.append("archived")
            if deprecated:
                reasons.append("deprecated")
            if duplicate:
                reasons.append("duplicate_market")
            upserts.append(
                (
                    token_id,
                    int(row["market_id"]),
                    row.get("market_slug"),
                    row.get("token_side"),
                    eligible,
                    trade_count > 0,
                    archived,
                    deprecated,
                    duplicate,
                    ",".join(reasons) if reasons else None,
                    trade_count,
                    token_stats.first_block if token_stats else None,
                    token_stats.last_block if token_stats else None,
                )
            )
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO quant.market_price_eligibility (
                    token_id, market_id, market_slug, token_side, eligible,
                    has_orderfilled_trades, is_archived, is_deprecated, is_duplicate_market,
                    skip_reason, orderfilled_trade_count, first_orderfilled_block, last_orderfilled_block,
                    checked_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (token_id) DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    market_slug = EXCLUDED.market_slug,
                    token_side = EXCLUDED.token_side,
                    eligible = EXCLUDED.eligible,
                    has_orderfilled_trades = EXCLUDED.has_orderfilled_trades,
                    is_archived = EXCLUDED.is_archived,
                    is_deprecated = EXCLUDED.is_deprecated,
                    is_duplicate_market = EXCLUDED.is_duplicate_market,
                    skip_reason = EXCLUDED.skip_reason,
                    orderfilled_trade_count = EXCLUDED.orderfilled_trade_count,
                    first_orderfilled_block = EXCLUDED.first_orderfilled_block,
                    last_orderfilled_block = EXCLUDED.last_orderfilled_block,
                    checked_at = now()
                """,
                upserts,
            )
        total += len(upserts)
        offset += batch_size
    return total
