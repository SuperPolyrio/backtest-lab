"""Explicit priority targets for quant price builders."""

from __future__ import annotations

import json
from typing import Any


def upsert_price_build_targets_for_market(
    conn: Any,
    *,
    source: str,
    market_slug: str,
    token_side: str | None = None,
    priority: int = 1000,
    reason: str = "requested",
    from_ts: int | None = None,
    to_ts: int | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
) -> int:
    """Mark a market/token side as an explicit build target.

    The price runner always considers active targets before the bulk production
    universe. Backtest requests use this so a missing market can be backfilled
    ahead of broad historical work.
    """

    side = str(token_side or "").strip().upper()
    values: list[Any] = [
        str(source),
        int(priority),
        reason,
        int(from_ts) if from_ts is not None else None,
        int(to_ts) if to_ts is not None else None,
        int(from_block) if from_block is not None else None,
        int(to_block) if to_block is not None else None,
        str(market_slug).strip(),
    ]
    filters = ["m.market_slug = %s"]
    if side:
        filters.append("m.token_side = %s")
        values.append(side)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO quant.market_price_build_targets (
                source, token_id, market_id, market_slug, token_side, priority, reason,
                requested_from_ts, requested_to_ts, requested_from_block, requested_to_block,
                status, updated_at
            )
            SELECT
                %s, m.token_id, m.market_id, m.market_slug, m.token_side, %s, %s,
                CASE WHEN %s::double precision IS NULL THEN NULL ELSE to_timestamp(%s::double precision) END,
                CASE WHEN %s::double precision IS NULL THEN NULL ELSE to_timestamp(%s::double precision) END,
                %s::bigint, %s::bigint,
                'active', now()
            FROM quant.market_token_metadata m
            WHERE {" AND ".join(filters)}
            ON CONFLICT (source, token_id) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                priority = GREATEST(quant.market_price_build_targets.priority, EXCLUDED.priority),
                reason = EXCLUDED.reason,
                requested_from_ts = COALESCE(EXCLUDED.requested_from_ts, quant.market_price_build_targets.requested_from_ts),
                requested_to_ts = COALESCE(EXCLUDED.requested_to_ts, quant.market_price_build_targets.requested_to_ts),
                requested_from_block = COALESCE(EXCLUDED.requested_from_block, quant.market_price_build_targets.requested_from_block),
                requested_to_block = COALESCE(EXCLUDED.requested_to_block, quant.market_price_build_targets.requested_to_block),
                status = 'active',
                updated_at = now()
            """,
            (
                values[0],
                values[1],
                values[2],
                values[3],
                values[3],
                values[4],
                values[4],
                values[5],
                values[6],
                *values[7:],
            ),
        )
        return cur.rowcount or 0


def target_reason(base: str, meta: dict[str, Any] | None = None) -> str:
    if not meta:
        return base
    return f"{base}:{json.dumps(meta, sort_keys=True, separators=(',', ':'))}"[:1000]
