"""Batch runner primitives for quant price builds.

The runner can be used for one-shot historical jobs or as a daemon on the local
collector. It builds the production price sources independently from the
backtest API:

* frontend: time-based Polymarket prices-history rows.
* orderfilled_block_close: block-number based trade close rows.
* api_trades: timestamp-based Polymarket data-api trade rows.
"""

from __future__ import annotations

import argparse
import gc
import json
import re
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from ..core.db import ClickHouseClient, PostgresSettings, postgres_connection
from ..core.eligibility import fetch_orderfilled_trade_stats, refresh_eligibility
from ..core.metadata import (
    refresh_market_event_memberships,
    refresh_market_token_metadata,
    refresh_missing_market_token_metadata,
)
from ..core.schema import create_schema
from ..prices.block_close_algorithm import orderfilled_block_close_sql, quote_clickhouse_string
from ..prices.block_close_backfill import backfill_block_close_prices, insert_block_close_rows
from ..prices.frontend_backfill import insert_frontend_points, backfill_frontend_prices
from ..prices.frontend_client import FrontendPriceClient
from ..prices.api_trades_algorithm import api_trades_sql
from ..prices.api_trades_backfill import backfill_api_trades, insert_api_trade_rows


SEPTEMBER_2025_TS = 1756684800
FRONTEND_SOURCE = "frontend"
BLOCK_CLOSE_SOURCE = "orderfilled_block_close"
API_TRADES_SOURCE = "api_trades"
BLOCK_CLOSE_QUERY_TOKEN_BATCH_SIZE = 200
SLUG_TIMESTAMP_RE = re.compile(r"(?:^|-)(\d{10})(?:$|-)")


def utc_now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def ts_to_utc(timestamp: int) -> datetime:
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc)


def maybe_datetime_to_ts(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    try:
        return int(value)
    except Exception:
        return None


def frontend_target_end_ts(token: dict[str, Any], end_ts: int) -> int:
    token_end_ts = int(end_ts)
    end_date_ts = maybe_datetime_to_ts(token.get("end_date"))
    if end_date_ts and bool(token.get("closed")):
        token_end_ts = min(token_end_ts, end_date_ts + 86400)
    requested_to_ts = maybe_datetime_to_ts(token.get("requested_to_ts"))
    if requested_to_ts:
        token_end_ts = min(token_end_ts, requested_to_ts)
    return token_end_ts


def is_short_crypto_market(token: dict[str, Any]) -> bool:
    slug = str(token.get("market_slug") or "").lower()
    title = str(token.get("market_title") or "").lower()
    text = f"{slug} {title}"
    if "updown" not in text:
        return False
    if not any(symbol in text for symbol in ("btc", "bitcoin", "eth", "ethereum", "sol", "xrp", "doge")):
        return False
    return any(marker in text for marker in ("5m", "15m", "30m", "1h", "hour"))


def parse_slug_timestamp(token: dict[str, Any]) -> int | None:
    slug = str(token.get("market_slug") or "")
    matches = SLUG_TIMESTAMP_RE.findall(slug)
    if not matches:
        return None
    try:
        value = int(matches[-1])
    except ValueError:
        return None
    if 1_600_000_000 <= value <= 2_200_000_000:
        return value
    return None


def classify_frontend_tier(
    token: dict[str, Any],
    *,
    recent_days: int,
    backtest_min_orderfilled_trades: int,
    defer_low_value: bool,
    now_ts: int,
) -> str:
    if token.get("target_priority"):
        return "target_full"
    trade_count = int(token.get("orderfilled_trade_count") or 0)
    active = bool(token.get("active")) and not bool(token.get("closed"))
    end_date_ts = maybe_datetime_to_ts(token.get("end_date"))
    if trade_count <= 0:
        if not active or (end_date_ts and end_date_ts < int(now_ts)):
            return "skipped_no_orderfilled"
        return "waiting_no_orderfilled"
    if is_short_crypto_market(token):
        return "crypto_short_full"
    if active:
        return "active_full"
    if end_date_ts and end_date_ts >= int(now_ts) - int(recent_days) * 86400:
        return "recent_full"
    if trade_count >= int(backtest_min_orderfilled_trades):
        return "backtest_value_full"
    if defer_low_value:
        return "deferred_low_value"
    if end_date_ts:
        return "longtail_event_window"
    return "backtest_value_full"


def start_run(conn: Any, *, source: str, mode: str, meta: dict[str, Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.market_price_build_runs (source, mode, status, started_at, meta)
            VALUES (%s, %s, 'running', clock_timestamp(), %s::jsonb)
            RETURNING run_id
            """,
            (source, mode, json.dumps(meta, sort_keys=True)),
        )
        row = cur.fetchone()
        return int(row["run_id"])


def finish_run(conn: Any, *, run_id: int, status: str, rows_written: int, markets_complete: int, error_count: int = 0, last_error: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.market_price_build_runs
            SET status = %s,
                finished_at = clock_timestamp(),
                rows_written = %s,
                markets_complete = %s,
                error_count = %s,
                last_error = %s
            WHERE run_id = %s
            """,
            (status, rows_written, markets_complete, error_count, last_error, run_id),
        )


def shard_sql(alias: str, *, shard_count: int) -> str:
    if int(shard_count) <= 1:
        return "TRUE"
    return f"mod(abs(hashtext({alias}.token_id)), %s) = %s"


def shard_params(*, shard_count: int, shard_index: int) -> tuple[int, int] | tuple:
    if int(shard_count) <= 1:
        return ()
    return (int(shard_count), int(shard_index))


def market_shard_sql(alias: str, *, shard_count: int) -> str:
    if int(shard_count) <= 1:
        return "TRUE"
    return f"mod(abs(hashtext({alias}.market_id::text)), %s) = %s"


def fetch_frontend_build_candidates(
    conn: Any,
    *,
    since_ts: int,
    end_ts: int,
    limit: int,
    min_orderfilled_trades: int,
    include_active_without_trades: bool,
    targets_only: bool,
    all_markets: bool,
    shard_index: int = 0,
    shard_count: int = 1,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                m.token_id, m.market_id, m.market_slug, m.token_side,
                m.active, m.closed, m.end_date, m.created_at, m.market_title,
                e.eligible, e.orderfilled_trade_count,
                t.priority AS target_priority, t.reason AS target_reason,
                t.requested_from_ts, t.requested_to_ts,
                s.last_complete_ts, s.attempt_count, s.last_error
            FROM quant.market_token_metadata m
            LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            LEFT JOIN quant.market_price_build_targets t
                ON t.source = %s AND t.token_id = m.token_id AND t.status = 'active'
            LEFT JOIN quant.market_price_build_market_state s
                ON s.source = %s AND s.token_id = m.token_id
            WHERE
                COALESCE(e.is_archived, m.archived, FALSE) = FALSE
                AND COALESCE(e.is_deprecated, m.deprecated, FALSE) = FALSE
                AND COALESCE(e.is_duplicate_market, FALSE) = FALSE
                AND {shard_sql("m", shard_count=shard_count)}
                AND m.created_at >= to_timestamp(%s)
                AND COALESCE(s.status, '') NOT IN ('skipped', 'deferred')
                AND (
                    t.token_id IS NOT NULL
                    OR (
                        %s = TRUE
                        AND %s = FALSE
                        AND (
                            COALESCE(e.orderfilled_trade_count, 0) > 0
                            OR m.closed = TRUE
                            OR m.active = FALSE
                            OR m.end_date < now()
                        )
                        AND (
                            s.last_complete_ts IS NULL
                            OR s.last_complete_ts < LEAST(
                                to_timestamp(%s),
                                COALESCE(m.end_date + interval '1 day', to_timestamp(%s))
                            )
                        )
                    )
                    OR (
                        %s = FALSE
                        AND COALESCE(e.eligible, FALSE) = TRUE
                        AND COALESCE(e.orderfilled_trade_count, 0) >= %s
                    )
                    OR (
                        %s = TRUE
                        AND %s = FALSE
                        AND m.active = TRUE
                        AND m.closed = FALSE
                    )
                )
                AND (
                    s.last_complete_ts IS NULL
                    OR s.last_complete_ts < LEAST(
                        to_timestamp(%s),
                        COALESCE(t.requested_to_ts, to_timestamp(%s)),
                        CASE
                            WHEN m.closed = TRUE THEN COALESCE(m.end_date + interval '1 day', to_timestamp(%s))
                            ELSE to_timestamp(%s)
                        END
                    )
                )
            ORDER BY
                CASE WHEN t.token_id IS NOT NULL THEN 0 ELSE 1 END,
                t.priority DESC NULLS LAST,
                s.last_complete_ts ASC NULLS FIRST,
                m.active DESC,
                e.orderfilled_trade_count DESC NULLS LAST,
                m.market_id ASC,
                m.outcome_index ASC,
                m.token_id ASC
            LIMIT %s
            """,
            (
                FRONTEND_SOURCE,
                FRONTEND_SOURCE,
                *shard_params(shard_count=shard_count, shard_index=shard_index),
                int(since_ts),
                bool(all_markets),
                bool(targets_only),
                int(end_ts),
                int(end_ts),
                bool(targets_only),
                int(min_orderfilled_trades),
                bool(include_active_without_trades),
                bool(targets_only),
                int(end_ts),
                int(end_ts),
                int(end_ts),
                int(end_ts),
                int(limit),
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_api_trades_build_candidates(
    conn: Any,
    *,
    since_ts: int,
    end_ts: int,
    limit: int,
    min_orderfilled_trades: int,
    include_active_without_trades: bool,
    shard_index: int = 0,
    shard_count: int = 1,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                m.token_id, m.market_id, m.market_slug, m.token_side,
                m.active, m.closed, m.end_date, m.created_at,
                e.eligible, e.orderfilled_trade_count,
                t.priority AS target_priority, t.reason AS target_reason,
                t.requested_from_ts, t.requested_to_ts,
                s.last_complete_ts, s.attempt_count, s.last_error
            FROM quant.market_token_metadata m
            LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            LEFT JOIN quant.market_price_build_targets t
                ON t.source = %s AND t.token_id = m.token_id AND t.status = 'active'
            LEFT JOIN quant.market_price_build_market_state s
                ON s.source = %s AND s.token_id = m.token_id
            WHERE
                COALESCE(e.is_archived, m.archived, FALSE) = FALSE
                AND COALESCE(e.is_deprecated, m.deprecated, FALSE) = FALSE
                AND COALESCE(e.is_duplicate_market, FALSE) = FALSE
                AND m.token_id IS NOT NULL
                AND {shard_sql("m", shard_count=shard_count)}
                AND m.created_at >= to_timestamp(%s)
                AND (
                    t.token_id IS NOT NULL
                    OR (
                        COALESCE(e.eligible, FALSE) = TRUE
                        AND COALESCE(e.orderfilled_trade_count, 0) >= %s
                    )
                    OR (
                        %s = TRUE
                        AND m.active = TRUE
                        AND m.closed = FALSE
                    )
                )
                AND (
                    s.last_complete_ts IS NULL
                    OR s.last_complete_ts < LEAST(
                        to_timestamp(%s),
                        COALESCE(t.requested_to_ts, to_timestamp(%s)),
                        CASE
                            WHEN m.closed = TRUE THEN COALESCE(m.end_date + interval '1 day', to_timestamp(%s))
                            ELSE to_timestamp(%s)
                        END
                    )
                )
            ORDER BY
                CASE WHEN t.token_id IS NOT NULL THEN 0 ELSE 1 END,
                t.priority DESC NULLS LAST,
                s.last_complete_ts ASC NULLS FIRST,
                m.active DESC,
                e.orderfilled_trade_count DESC NULLS LAST,
                m.market_id ASC,
                m.outcome_index ASC,
                m.token_id ASC
            LIMIT %s
            """,
            (
                API_TRADES_SOURCE,
                API_TRADES_SOURCE,
                *shard_params(shard_count=shard_count, shard_index=shard_index),
                int(since_ts),
                int(min_orderfilled_trades),
                bool(include_active_without_trades),
                int(end_ts),
                int(end_ts),
                int(end_ts),
                int(end_ts),
                int(limit),
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_block_close_build_candidates(
    conn: Any,
    *,
    since_ts: int,
    created_before_ts: int | None,
    limit: int,
    min_orderfilled_trades: int,
    live_only: bool = False,
    live_lookback_days: int = 7,
    shard_index: int = 0,
    shard_count: int = 1,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                m.token_id, m.token_id_hex, m.market_id, m.market_slug, m.token_side,
                m.active, m.closed, m.end_date, m.created_at,
                e.eligible,
                COALESCE(e.first_orderfilled_block, oms.first_orderfilled_block) AS first_orderfilled_block,
                GREATEST(
                    COALESCE(e.last_orderfilled_block, 0),
                    COALESCE(oms.last_orderfilled_block, 0)
                ) AS last_orderfilled_block,
                GREATEST(
                    COALESCE(e.orderfilled_trade_count, 0),
                    COALESCE(oms.trade_count, 0)
                ) AS orderfilled_trade_count,
                t.priority AS target_priority, t.reason AS target_reason,
                t.requested_from_block, t.requested_to_block,
                s.last_complete_block, s.attempt_count, s.last_error,
                s.status AS build_status,
                bc.has_block_rows
            FROM quant.market_token_metadata m
            JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            LEFT JOIN quant.market_orderfilled_market_stats oms ON oms.market_id = m.market_id
            LEFT JOIN quant.market_price_build_targets t
                ON t.source = %s AND t.token_id = m.token_id AND t.status = 'active'
            LEFT JOIN quant.market_price_build_market_state s
                ON s.source = %s AND s.token_id = m.token_id
            LEFT JOIN LATERAL (
                SELECT 1 AS has_block_rows
                FROM quant.market_token_block_close p
                WHERE p.token_id = m.token_id
                LIMIT 1
            ) bc ON TRUE
            WHERE
                e.eligible = TRUE
                AND (
                    t.token_id IS NOT NULL
                    OR GREATEST(COALESCE(e.orderfilled_trade_count, 0), COALESCE(oms.trade_count, 0)) >= %s
                )
                AND m.token_id_hex IS NOT NULL
                AND COALESCE(e.first_orderfilled_block, oms.first_orderfilled_block) IS NOT NULL
                AND GREATEST(COALESCE(e.last_orderfilled_block, 0), COALESCE(oms.last_orderfilled_block, 0)) > 0
                AND {shard_sql("m", shard_count=shard_count)}
                AND m.created_at >= to_timestamp(%s)
                AND (%s::bigint IS NULL OR m.created_at < to_timestamp(%s))
                AND (
                    %s = FALSE
                    OR t.token_id IS NOT NULL
                    OR m.end_date IS NULL
                    OR m.end_date >= now() - (%s::text || ' days')::interval
                )
                AND (
                    (
                        t.token_id IS NOT NULL
                        AND t.requested_to_block IS NOT NULL
                        AND (
                            s.last_complete_block IS NULL
                            OR s.last_complete_block < t.requested_to_block
                        )
                    )
                    OR (
                        (t.token_id IS NULL OR t.requested_to_block IS NULL)
                        AND (
                            s.last_complete_block IS NULL
                            OR s.last_complete_block < GREATEST(
                                COALESCE(e.last_orderfilled_block, 0),
                                COALESCE(oms.last_orderfilled_block, 0)
                            )
                            OR (
                                bc.has_block_rows IS NULL
                                AND COALESCE(s.status, '') <> 'empty'
                            )
                        )
                    )
                )
            ORDER BY
                CASE WHEN t.token_id IS NOT NULL THEN 0 ELSE 1 END,
                t.priority DESC NULLS LAST,
                m.active DESC,
                GREATEST(
                    COALESCE(e.last_orderfilled_block, 0),
                    COALESCE(oms.last_orderfilled_block, 0)
                ) DESC,
                s.last_complete_block ASC NULLS FIRST,
                e.orderfilled_trade_count DESC NULLS LAST,
                m.market_id ASC,
                m.outcome_index ASC,
                m.token_id ASC
            LIMIT %s
            """,
            (
                BLOCK_CLOSE_SOURCE,
                BLOCK_CLOSE_SOURCE,
                int(min_orderfilled_trades),
                *shard_params(shard_count=shard_count, shard_index=shard_index),
                int(since_ts),
                int(created_before_ts) if created_before_ts is not None else None,
                int(created_before_ts) if created_before_ts is not None else None,
                bool(live_only),
                int(live_lookback_days),
                int(limit),
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_block_close_market_sibling_candidates(
    conn: Any,
    *,
    market_ids: list[int],
    since_ts: int,
    created_before_ts: int | None,
    limit: int,
    min_orderfilled_trades: int,
) -> list[dict[str, Any]]:
    if not market_ids:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                m.token_id, m.token_id_hex, m.market_id, m.market_slug, m.token_side,
                m.active, m.closed, m.end_date, m.created_at,
                e.eligible,
                COALESCE(e.first_orderfilled_block, oms.first_orderfilled_block) AS first_orderfilled_block,
                GREATEST(
                    COALESCE(e.last_orderfilled_block, 0),
                    COALESCE(oms.last_orderfilled_block, 0)
                ) AS last_orderfilled_block,
                GREATEST(
                    COALESCE(e.orderfilled_trade_count, 0),
                    COALESCE(oms.trade_count, 0)
                ) AS orderfilled_trade_count,
                t.priority AS target_priority, t.reason AS target_reason,
                t.requested_from_block, t.requested_to_block,
                s.last_complete_block, s.attempt_count, s.last_error,
                s.status AS build_status,
                bc.has_block_rows
            FROM quant.market_token_metadata m
            JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            LEFT JOIN quant.market_orderfilled_market_stats oms ON oms.market_id = m.market_id
            LEFT JOIN quant.market_price_build_targets t
                ON t.source = %s AND t.token_id = m.token_id AND t.status = 'active'
            LEFT JOIN quant.market_price_build_market_state s
                ON s.source = %s AND s.token_id = m.token_id
            LEFT JOIN LATERAL (
                SELECT 1 AS has_block_rows
                FROM quant.market_token_block_close p
                WHERE p.token_id = m.token_id
                LIMIT 1
            ) bc ON TRUE
            WHERE
                m.market_id = ANY(%s::bigint[])
                AND e.eligible = TRUE
                AND (
                    t.token_id IS NOT NULL
                    OR GREATEST(COALESCE(e.orderfilled_trade_count, 0), COALESCE(oms.trade_count, 0)) >= %s
                )
                AND m.token_id_hex IS NOT NULL
                AND COALESCE(e.first_orderfilled_block, oms.first_orderfilled_block) IS NOT NULL
                AND GREATEST(COALESCE(e.last_orderfilled_block, 0), COALESCE(oms.last_orderfilled_block, 0)) > 0
                AND m.created_at >= to_timestamp(%s)
                AND (%s::bigint IS NULL OR m.created_at < to_timestamp(%s))
                AND (
                    (
                        t.token_id IS NOT NULL
                        AND t.requested_to_block IS NOT NULL
                        AND (
                            s.last_complete_block IS NULL
                            OR s.last_complete_block < t.requested_to_block
                        )
                    )
                    OR (
                        (t.token_id IS NULL OR t.requested_to_block IS NULL)
                        AND (
                            s.last_complete_block IS NULL
                            OR s.last_complete_block < GREATEST(
                                COALESCE(e.last_orderfilled_block, 0),
                                COALESCE(oms.last_orderfilled_block, 0)
                            )
                            OR (
                                bc.has_block_rows IS NULL
                                AND COALESCE(s.status, '') <> 'empty'
                            )
                        )
                    )
                )
            ORDER BY
                m.market_id ASC,
                m.outcome_index ASC,
                m.token_id ASC
            LIMIT %s
            """,
            (
                BLOCK_CLOSE_SOURCE,
                BLOCK_CLOSE_SOURCE,
                [int(market_id) for market_id in market_ids],
                int(min_orderfilled_trades),
                int(since_ts),
                int(created_before_ts) if created_before_ts is not None else None,
                int(created_before_ts) if created_before_ts is not None else None,
                int(limit),
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_block_close_market_chronological_candidates(
    conn: Any,
    *,
    since_ts: int,
    limit: int,
    shard_index: int = 0,
    shard_count: int = 1,
) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                m.market_id,
                min(m.created_at) AS sort_date,
                bool_or(e.token_id IS NULL AND oms.market_id IS NOT NULL) AS needs_eligibility,
                bool_or(
                    COALESCE(e.eligible, FALSE) = TRUE
                    AND e.first_orderfilled_block IS NOT NULL
                    AND e.last_orderfilled_block IS NOT NULL
                    AND (
                        s.last_complete_block IS NULL
                        OR s.last_complete_block < e.last_orderfilled_block
                        OR (
                            bc.has_block_rows IS NULL
                            AND COALESCE(s.status, '') <> 'empty'
                        )
                    )
                ) AS needs_block_close
            FROM quant.market_token_metadata m
            LEFT JOIN quant.market_orderfilled_market_stats oms ON oms.market_id = m.market_id
            LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            LEFT JOIN quant.market_price_build_market_state s
                ON s.source = %s AND s.token_id = m.token_id
            LEFT JOIN LATERAL (
                SELECT 1 AS has_block_rows
                FROM quant.market_token_block_close p
                WHERE p.token_id = m.token_id
                LIMIT 1
            ) bc ON TRUE
            WHERE
                m.token_id_hex IS NOT NULL
                AND {market_shard_sql("m", shard_count=shard_count)}
                AND m.created_at >= to_timestamp(%s)
            GROUP BY m.market_id
            HAVING
                bool_or(e.token_id IS NULL AND oms.market_id IS NOT NULL)
                OR bool_or(
                    COALESCE(e.eligible, FALSE) = TRUE
                    AND e.first_orderfilled_block IS NOT NULL
                    AND e.last_orderfilled_block IS NOT NULL
                    AND (
                        s.last_complete_block IS NULL
                        OR s.last_complete_block < e.last_orderfilled_block
                        OR (
                            bc.has_block_rows IS NULL
                            AND COALESCE(s.status, '') <> 'empty'
                        )
                    )
                )
            ORDER BY
                CASE
                    WHEN bool_or(
                        COALESCE(e.eligible, FALSE) = TRUE
                        AND e.first_orderfilled_block IS NOT NULL
                        AND e.last_orderfilled_block IS NOT NULL
                        AND (
                            s.last_complete_block IS NULL
                            OR s.last_complete_block < e.last_orderfilled_block
                            OR (
                                bc.has_block_rows IS NULL
                                AND COALESCE(s.status, '') <> 'empty'
                            )
                        )
                    ) THEN 0
                    WHEN bool_or(e.token_id IS NULL AND oms.market_id IS NOT NULL) THEN 1
                    ELSE 2
                END ASC,
                min(m.created_at) ASC NULLS LAST,
                m.market_id ASC
            LIMIT %s
            """,
            (
                BLOCK_CLOSE_SOURCE,
                *shard_params(shard_count=shard_count, shard_index=shard_index),
                int(since_ts),
                int(limit),
            ),
        )
        return [int(row["market_id"]) for row in cur.fetchall()]


def fetch_price_market_chronological_candidates(
    conn: Any,
    *,
    since_ts: int,
    end_ts: int,
    limit: int,
    frontend_min_orderfilled_trades: int,
    frontend_include_active_without_trades: bool,
    shard_index: int = 0,
    shard_count: int = 1,
) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                m.market_id,
                min(m.created_at) AS sort_date,
                bool_or(e.token_id IS NULL AND oms.market_id IS NOT NULL) AS needs_eligibility,
                bool_or(
                    COALESCE(e.eligible, FALSE) = TRUE
                    AND e.first_orderfilled_block IS NOT NULL
                    AND e.last_orderfilled_block IS NOT NULL
                    AND (
                        sb.last_complete_block IS NULL
                        OR sb.last_complete_block < e.last_orderfilled_block
                        OR (
                            bc.has_block_rows IS NULL
                            AND COALESCE(sb.status, '') <> 'empty'
                        )
                    )
                ) AS needs_block_close,
                bool_or(
                    (
                        (
                            COALESCE(e.eligible, FALSE) = TRUE
                            AND COALESCE(e.orderfilled_trade_count, 0) >= %s
                        )
                        OR (
                            %s = TRUE
                            AND m.active = TRUE
                            AND m.closed = FALSE
                        )
                    )
                    AND (
                        sf.last_complete_ts IS NULL
                        OR sf.last_complete_ts < LEAST(
                            to_timestamp(%s),
                            COALESCE(m.end_date + interval '1 day', to_timestamp(%s))
                        )
                    )
                ) AS needs_frontend
            FROM quant.market_token_metadata m
            LEFT JOIN quant.market_orderfilled_market_stats oms ON oms.market_id = m.market_id
            LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            LEFT JOIN quant.market_price_build_market_state sb
                ON sb.source = %s AND sb.token_id = m.token_id
            LEFT JOIN quant.market_price_build_market_state sf
                ON sf.source = %s AND sf.token_id = m.token_id
            LEFT JOIN LATERAL (
                SELECT 1 AS has_block_rows
                FROM quant.market_token_block_close p
                WHERE p.token_id = m.token_id
                LIMIT 1
            ) bc ON TRUE
            WHERE
                m.token_id_hex IS NOT NULL
                AND {market_shard_sql("m", shard_count=shard_count)}
                AND m.created_at >= to_timestamp(%s)
            GROUP BY m.market_id
            HAVING
                bool_or(e.token_id IS NULL AND oms.market_id IS NOT NULL)
                OR bool_or(
                    COALESCE(e.eligible, FALSE) = TRUE
                    AND e.first_orderfilled_block IS NOT NULL
                    AND e.last_orderfilled_block IS NOT NULL
                    AND (
                        sb.last_complete_block IS NULL
                        OR sb.last_complete_block < e.last_orderfilled_block
                        OR (
                            bc.has_block_rows IS NULL
                            AND COALESCE(sb.status, '') <> 'empty'
                        )
                    )
                )
                OR bool_or(
                    (
                        (
                            COALESCE(e.eligible, FALSE) = TRUE
                            AND COALESCE(e.orderfilled_trade_count, 0) >= %s
                        )
                        OR (
                            %s = TRUE
                            AND m.active = TRUE
                            AND m.closed = FALSE
                        )
                    )
                    AND (
                        sf.last_complete_ts IS NULL
                        OR sf.last_complete_ts < LEAST(
                            to_timestamp(%s),
                            COALESCE(m.end_date + interval '1 day', to_timestamp(%s))
                        )
                    )
                )
            ORDER BY
                CASE
                    WHEN bool_or(
                        COALESCE(e.eligible, FALSE) = TRUE
                        AND e.first_orderfilled_block IS NOT NULL
                        AND e.last_orderfilled_block IS NOT NULL
                        AND (
                            sb.last_complete_block IS NULL
                            OR sb.last_complete_block < e.last_orderfilled_block
                            OR (
                                bc.has_block_rows IS NULL
                                AND COALESCE(sb.status, '') <> 'empty'
                            )
                        )
                    ) THEN 0
                    WHEN bool_or(e.token_id IS NULL AND oms.market_id IS NOT NULL) THEN 1
                    ELSE 2
                END ASC,
                min(m.created_at) ASC NULLS LAST,
                m.market_id ASC
            LIMIT %s
            """,
            (
                int(frontend_min_orderfilled_trades),
                bool(frontend_include_active_without_trades),
                int(end_ts),
                int(end_ts),
                BLOCK_CLOSE_SOURCE,
                FRONTEND_SOURCE,
                *shard_params(shard_count=shard_count, shard_index=shard_index),
                int(since_ts),
                int(frontend_min_orderfilled_trades),
                bool(frontend_include_active_without_trades),
                int(end_ts),
                int(end_ts),
                int(limit),
            ),
        )
        return [int(row["market_id"]) for row in cur.fetchall()]


def fetch_market_tokens_for_block_close(conn: Any, *, market_ids: list[int]) -> list[dict[str, Any]]:
    if not market_ids:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH selected_market_ids AS (
                SELECT market_id, ordinal
                FROM unnest(%s::bigint[]) WITH ORDINALITY AS ids(market_id, ordinal)
            ),
            selected AS (
                SELECT m.*, ids.ordinal
                FROM selected_market_ids ids
                JOIN quant.market_token_metadata m ON m.market_id = ids.market_id
                WHERE m.token_id_hex IS NOT NULL
            ),
            duplicate_keys AS (
                SELECT DISTINCT duplicate_group_key, token_side
                FROM selected
                WHERE duplicate_group_key IS NOT NULL
            ),
            duplicate_rank AS (
                SELECT
                    m.token_id,
                    row_number() OVER (
                        PARTITION BY m.duplicate_group_key, m.token_side
                        ORDER BY COALESCE(m.created_at, to_timestamp(4102444800)), m.market_id
                    ) AS duplicate_rank
                FROM quant.market_token_metadata m
                JOIN duplicate_keys k
                    ON k.duplicate_group_key = m.duplicate_group_key
                    AND k.token_side = m.token_side
            )
            SELECT
                m.token_id, m.token_id_hex, m.market_id, m.market_slug, m.token_side,
                m.active, m.closed, m.end_date, m.created_at, m.archived, m.deprecated,
                m.duplicate_group_key, m.outcome_index,
                COALESCE(dr.duplicate_rank > 1, FALSE) AS is_duplicate_market,
                e.first_orderfilled_block, e.last_orderfilled_block, e.orderfilled_trade_count,
                e.eligible,
                GREATEST(COALESCE(tb.priority, 0), COALESCE(tf.priority, 0)) AS target_priority,
                COALESCE(tb.reason, tf.reason) AS target_reason,
                tb.requested_from_block, tb.requested_to_block,
                tf.requested_from_ts, tf.requested_to_ts,
                sb.last_complete_block,
                sf.last_complete_ts,
                GREATEST(COALESCE(sb.attempt_count, 0), COALESCE(sf.attempt_count, 0)) AS attempt_count,
                COALESCE(sb.last_error, sf.last_error) AS last_error,
                bc.has_block_rows
            FROM selected m
            LEFT JOIN duplicate_rank dr ON dr.token_id = m.token_id
            LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            LEFT JOIN quant.market_price_build_targets tb
                ON tb.source = %s AND tb.token_id = m.token_id AND tb.status = 'active'
            LEFT JOIN quant.market_price_build_targets tf
                ON tf.source = %s AND tf.token_id = m.token_id AND tf.status = 'active'
            LEFT JOIN quant.market_price_build_market_state sb
                ON sb.source = %s AND sb.token_id = m.token_id
            LEFT JOIN quant.market_price_build_market_state sf
                ON sf.source = %s AND sf.token_id = m.token_id
            LEFT JOIN LATERAL (
                SELECT 1 AS has_block_rows
                FROM quant.market_token_block_close p
                WHERE p.token_id = m.token_id
                LIMIT 1
            ) bc ON TRUE
            ORDER BY m.ordinal ASC, m.outcome_index ASC, m.token_id ASC
            """,
            (
                [int(market_id) for market_id in market_ids],
                BLOCK_CLOSE_SOURCE,
                FRONTEND_SOURCE,
                BLOCK_CLOSE_SOURCE,
                FRONTEND_SOURCE,
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_market_orderfilled_trade_stats(ch: ClickHouseClient, tokens: list[dict[str, Any]]) -> dict[str, Any]:
    token_ids = sorted({str(token.get("token_id_hex") or "").lower() for token in tokens if token.get("token_id_hex")})
    if not token_ids:
        return {}
    quoted_tokens = ",".join(quote_clickhouse_string(token_id) for token_id in token_ids)
    table = ch.settings.orderfilled_table
    rows = ch.query_json_rows(
        f"""
        SELECT
            lower(token_id) AS token_id,
            count() AS trade_count,
            min(block_number) AS first_block,
            max(block_number) AS last_block
        FROM {table}
        WHERE
            token_id IN ({quoted_tokens})
        GROUP BY token_id
        """
    )
    return {str(row["token_id"]).lower(): row for row in rows}


def refresh_market_token_eligibility(conn: Any, ch: ClickHouseClient, tokens: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not tokens:
        return []
    stats = fetch_market_orderfilled_trade_stats(ch, tokens)
    upserts = []
    refreshed = []
    for token in tokens:
        token_id = str(token["token_id"])
        token_id_hex = str(token.get("token_id_hex") or "").lower()
        token_stats = stats.get(token_id_hex)
        trade_count = int(token_stats.get("trade_count") or 0) if token_stats else 0
        first_block = int(token_stats["first_block"]) if token_stats and token_stats.get("first_block") is not None else None
        last_block = int(token_stats["last_block"]) if token_stats and token_stats.get("last_block") is not None else None
        archived = bool(token.get("archived"))
        deprecated = bool(token.get("deprecated"))
        duplicate = bool(token.get("is_duplicate_market"))
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
                int(token["market_id"]),
                token.get("market_slug"),
                token.get("token_side"),
                eligible,
                trade_count > 0,
                archived,
                deprecated,
                duplicate,
                ",".join(reasons) if reasons else None,
                trade_count,
                first_block,
                last_block,
            )
        )
        updated = dict(token)
        updated["eligible"] = eligible
        updated["orderfilled_trade_count"] = trade_count
        updated["first_orderfilled_block"] = first_block
        updated["last_orderfilled_block"] = last_block
        refreshed.append(updated)
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
    return refreshed


def refresh_orderfilled_market_stats(
    conn: Any,
    ch: ClickHouseClient,
    *,
    batch_size: int = 10000,
    commit_batches: bool = False,
) -> int:
    table = ch.settings.orderfilled_table
    rows = ch.query_json_rows(
        f"""
        SELECT
            market_id,
            count() AS trade_count,
            min(block_number) AS first_orderfilled_block,
            max(block_number) AS last_orderfilled_block
        FROM {table}
        WHERE market_id != 0
        GROUP BY market_id
        """
    )
    values = [
        (
            int(row["market_id"]),
            int(row.get("trade_count") or 0),
            int(row["first_orderfilled_block"]) if row.get("first_orderfilled_block") is not None else None,
            int(row["last_orderfilled_block"]) if row.get("last_orderfilled_block") is not None else None,
        )
        for row in rows
        if int(row.get("market_id") or 0) > 0
    ]
    for index in range(0, len(values), int(batch_size)):
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO quant.market_orderfilled_market_stats AS existing (
                    market_id, trade_count, first_orderfilled_block, last_orderfilled_block, refreshed_at
                ) VALUES (%s, %s, %s, %s, clock_timestamp())
                ON CONFLICT (market_id) DO UPDATE SET
                    trade_count = EXCLUDED.trade_count,
                    first_orderfilled_block = EXCLUDED.first_orderfilled_block,
                    last_orderfilled_block = EXCLUDED.last_orderfilled_block,
                    refreshed_at = clock_timestamp()
                WHERE (
                    existing.trade_count,
                    existing.first_orderfilled_block,
                    existing.last_orderfilled_block
                ) IS DISTINCT FROM (
                    EXCLUDED.trade_count,
                    EXCLUDED.first_orderfilled_block,
                    EXCLUDED.last_orderfilled_block
                )
                """,
                values[index : index + int(batch_size)],
            )
        if commit_batches:
            conn.commit()
    return len(values)


def fetch_eligibility_refresh_candidates(
    conn: Any,
    *,
    since_ts: int,
    limit: int,
    shard_index: int = 0,
    shard_count: int = 1,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                m.token_id, m.token_id_hex, m.market_id, m.market_slug, m.token_side,
                m.archived, m.deprecated, m.duplicate_group_key,
                row_number() OVER (
                    PARTITION BY m.duplicate_group_key, m.token_side
                    ORDER BY m.created_at NULLS LAST, m.market_id ASC
                ) AS duplicate_rank
            FROM quant.market_token_metadata m
            LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            LEFT JOIN quant.market_price_build_targets t
                ON t.source = %s AND t.token_id = m.token_id AND t.status = 'active'
            LEFT JOIN quant.market_orderfilled_market_stats oms ON oms.market_id = m.market_id
            WHERE
                m.token_id_hex IS NOT NULL
                AND {shard_sql("m", shard_count=shard_count)}
                AND m.created_at >= to_timestamp(%s)
            ORDER BY
                CASE WHEN t.token_id IS NOT NULL THEN 0 ELSE 1 END,
                t.priority DESC NULLS LAST,
                m.active DESC,
                oms.last_orderfilled_block DESC NULLS LAST,
                e.checked_at ASC NULLS FIRST,
                m.market_id DESC,
                m.token_id ASC
            LIMIT %s
            """,
            (
                BLOCK_CLOSE_SOURCE,
                *shard_params(shard_count=shard_count, shard_index=shard_index),
                int(since_ts),
                int(limit),
            ),
        )
        return [dict(row) for row in cur.fetchall()]


def refresh_since_eligibility(
    conn: Any,
    ch: ClickHouseClient,
    *,
    since_ts: int,
    batch_size: int = 1000,
    shard_index: int = 0,
    shard_count: int = 1,
) -> int:
    rows = fetch_eligibility_refresh_candidates(conn, since_ts=since_ts, limit=batch_size, shard_index=shard_index, shard_count=shard_count)
    if not rows:
        return 0
    stats = fetch_orderfilled_trade_stats(ch, [str(row["token_id_hex"]) for row in rows if row.get("token_id_hex")])
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
    return len(upserts)


def update_frontend_state(
    conn: Any,
    *,
    token: dict[str, Any],
    status: str,
    last_complete_ts: int | None,
    rows_written: int,
    last_error: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.market_price_build_market_state (
                source, token_id, market_id, market_slug, token_side,
                status, last_complete_ts, attempt_count, last_error, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s), 1, %s, now())
            ON CONFLICT (source, token_id) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                status = EXCLUDED.status,
                last_complete_ts = GREATEST(
                    COALESCE(quant.market_price_build_market_state.last_complete_ts, to_timestamp(0)),
                    EXCLUDED.last_complete_ts
                ),
                attempt_count = quant.market_price_build_market_state.attempt_count + 1,
                last_error = EXCLUDED.last_error,
                updated_at = now()
            """,
            (
                FRONTEND_SOURCE,
                token["token_id"],
                token["market_id"],
                token.get("market_slug"),
                token["token_side"],
                status,
                int(last_complete_ts or SEPTEMBER_2025_TS),
                last_error[:4000] if last_error else None,
            ),
        )


def update_api_trades_state(
    conn: Any,
    *,
    token: dict[str, Any],
    status: str,
    last_complete_ts: int | None,
    rows_written: int,
    last_error: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.market_price_build_market_state (
                source, token_id, market_id, market_slug, token_side,
                status, last_complete_ts, attempt_count, last_error, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s), 1, %s, now())
            ON CONFLICT (source, token_id) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                status = EXCLUDED.status,
                last_complete_ts = GREATEST(
                    COALESCE(quant.market_price_build_market_state.last_complete_ts, to_timestamp(0)),
                    EXCLUDED.last_complete_ts
                ),
                attempt_count = quant.market_price_build_market_state.attempt_count + 1,
                last_error = EXCLUDED.last_error,
                updated_at = now()
            """,
            (
                API_TRADES_SOURCE,
                token["token_id"],
                token["market_id"],
                token.get("market_slug"),
                token["token_side"],
                status,
                int(last_complete_ts or SEPTEMBER_2025_TS),
                last_error[:4000] if last_error else None,
            ),
        )


def mark_frontend_skipped_no_orderfilled(
    conn: Any,
    *,
    since_ts: int,
    end_ts: int,
    limit: int,
    shard_index: int,
    shard_count: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH candidates AS (
                SELECT
                    m.token_id, m.market_id, m.market_slug, m.token_side,
                    LEAST(
                        to_timestamp(%s),
                        COALESCE(m.end_date + interval '1 day', to_timestamp(%s))
                    ) AS last_complete_ts
                FROM quant.market_token_metadata m
                LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
                LEFT JOIN quant.market_price_build_targets t
                    ON t.source = %s AND t.token_id = m.token_id AND t.status = 'active'
                LEFT JOIN quant.market_price_build_market_state s
                    ON s.source = %s AND s.token_id = m.token_id
                WHERE
                    COALESCE(e.is_archived, m.archived, FALSE) = FALSE
                    AND COALESCE(e.is_deprecated, m.deprecated, FALSE) = FALSE
                    AND COALESCE(e.is_duplicate_market, FALSE) = FALSE
                    AND {shard_sql("m", shard_count=shard_count)}
                    AND m.created_at >= to_timestamp(%s)
                    AND t.token_id IS NULL
                    AND COALESCE(e.orderfilled_trade_count, 0) <= 0
                    AND (
                        m.closed = TRUE
                        OR m.active = FALSE
                        OR m.end_date < now()
                    )
                    AND COALESCE(s.status, '') NOT IN ('skipped', 'deferred')
                ORDER BY m.created_at ASC NULLS LAST, m.market_id ASC, m.token_id ASC
                LIMIT %s
            )
            INSERT INTO quant.market_price_build_market_state (
                source, token_id, market_id, market_slug, token_side,
                status, last_complete_ts, attempt_count, last_error, updated_at
            )
            SELECT
                %s, token_id, market_id, market_slug, token_side,
                'skipped', last_complete_ts, 1, 'frontend_tier:skipped_no_orderfilled', now()
            FROM candidates
            ON CONFLICT (source, token_id) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                status = EXCLUDED.status,
                last_complete_ts = GREATEST(
                    COALESCE(quant.market_price_build_market_state.last_complete_ts, to_timestamp(0)),
                    EXCLUDED.last_complete_ts
                ),
                attempt_count = quant.market_price_build_market_state.attempt_count + 1,
                last_error = EXCLUDED.last_error,
                updated_at = now()
            """,
            (
                int(end_ts),
                int(end_ts),
                FRONTEND_SOURCE,
                FRONTEND_SOURCE,
                *shard_params(shard_count=shard_count, shard_index=shard_index),
                int(since_ts),
                int(limit),
                FRONTEND_SOURCE,
            ),
        )
        return cur.rowcount or 0


def mark_frontend_deferred_low_value(
    conn: Any,
    *,
    since_ts: int,
    end_ts: int,
    recent_days: int,
    min_orderfilled_trades: int,
    limit: int,
    shard_index: int,
    shard_count: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH candidates AS (
                SELECT
                    m.token_id, m.market_id, m.market_slug, m.token_side,
                    LEAST(
                        to_timestamp(%s),
                        COALESCE(m.end_date + interval '1 day', to_timestamp(%s))
                    ) AS last_complete_ts
                FROM quant.market_token_metadata m
                LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
                LEFT JOIN quant.market_price_build_targets t
                    ON t.source = %s AND t.token_id = m.token_id AND t.status = 'active'
                LEFT JOIN quant.market_price_build_market_state s
                    ON s.source = %s AND s.token_id = m.token_id
                WHERE
                    COALESCE(e.is_archived, m.archived, FALSE) = FALSE
                    AND COALESCE(e.is_deprecated, m.deprecated, FALSE) = FALSE
                    AND COALESCE(e.is_duplicate_market, FALSE) = FALSE
                    AND {shard_sql("m", shard_count=shard_count)}
                    AND m.created_at >= to_timestamp(%s)
                    AND t.token_id IS NULL
                    AND COALESCE(e.orderfilled_trade_count, 0) > 0
                    AND COALESCE(e.orderfilled_trade_count, 0) < %s
                    AND NOT (m.active = TRUE AND m.closed = FALSE)
                    AND (m.end_date IS NULL OR m.end_date < now() - (%s::text || ' days')::interval)
                    AND lower(COALESCE(m.market_slug, '')) NOT LIKE '%%updown%%'
                    AND COALESCE(s.status, '') NOT IN ('skipped', 'deferred')
                ORDER BY m.created_at ASC NULLS LAST, m.market_id ASC, m.token_id ASC
                LIMIT %s
            )
            INSERT INTO quant.market_price_build_market_state (
                source, token_id, market_id, market_slug, token_side,
                status, last_complete_ts, attempt_count, last_error, updated_at
            )
            SELECT
                %s, token_id, market_id, market_slug, token_side,
                'deferred', last_complete_ts, 1, 'frontend_tier:deferred_low_value', now()
            FROM candidates
            ON CONFLICT (source, token_id) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                status = EXCLUDED.status,
                last_complete_ts = GREATEST(
                    COALESCE(quant.market_price_build_market_state.last_complete_ts, to_timestamp(0)),
                    EXCLUDED.last_complete_ts
                ),
                attempt_count = quant.market_price_build_market_state.attempt_count + 1,
                last_error = EXCLUDED.last_error,
                updated_at = now()
            """,
            (
                int(end_ts),
                int(end_ts),
                FRONTEND_SOURCE,
                FRONTEND_SOURCE,
                *shard_params(shard_count=shard_count, shard_index=shard_index),
                int(since_ts),
                int(min_orderfilled_trades),
                int(recent_days),
                int(limit),
                FRONTEND_SOURCE,
            ),
        )
        return cur.rowcount or 0


def update_block_state(
    conn: Any,
    *,
    token: dict[str, Any],
    status: str,
    last_complete_block: int | None,
    last_error: str | None = None,
) -> None:
    reset_stale_watermark = not bool(token.get("has_block_rows"))
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.market_price_build_market_state (
                source, token_id, market_id, market_slug, token_side,
                status, last_complete_block, attempt_count, last_error, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s, now())
            ON CONFLICT (source, token_id) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                status = EXCLUDED.status,
                last_complete_block = CASE
                    WHEN EXCLUDED.status IN ('complete', 'empty') AND %s THEN EXCLUDED.last_complete_block
                    WHEN EXCLUDED.status IN ('complete', 'empty') THEN GREATEST(
                        COALESCE(quant.market_price_build_market_state.last_complete_block, 0),
                        EXCLUDED.last_complete_block
                    )
                    ELSE COALESCE(quant.market_price_build_market_state.last_complete_block, EXCLUDED.last_complete_block)
                END,
                attempt_count = quant.market_price_build_market_state.attempt_count + 1,
                last_error = EXCLUDED.last_error,
                updated_at = now()
            """,
            (
                BLOCK_CLOSE_SOURCE,
                token["token_id"],
                token["market_id"],
                token.get("market_slug"),
                token["token_side"],
                status,
                int(last_complete_block or token.get("first_orderfilled_block") or 0),
                last_error[:4000] if last_error else None,
                reset_stale_watermark,
            ),
        )


def count_block_close_rows_for_tokens(conn: Any, tokens: list[dict[str, Any]]) -> dict[str, int]:
    if not tokens:
        return {}
    token_ids = [str(token["token_id"]) for token in tokens]
    from_blocks = [int(token["planned_from_block"]) for token in tokens]
    to_blocks = [int(token["planned_to_block"]) for token in tokens]
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH planned AS (
                SELECT *
                FROM unnest(%s::text[], %s::bigint[], %s::bigint[])
                    AS p(token_id, from_block, to_block)
            )
            SELECT p.token_id, COUNT(b.block_number) AS rows
            FROM planned p
            LEFT JOIN quant.market_token_block_close b
                ON b.token_id = p.token_id
                AND b.block_number >= p.from_block
                AND b.block_number <= p.to_block
            GROUP BY p.token_id
            """,
            (token_ids, from_blocks, to_blocks),
        )
        return {str(row["token_id"]): int(row["rows"] or 0) for row in cur.fetchall()}


def repair_block_close_complete_without_rows(conn: Any, *, limit: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH broken AS (
                SELECT s.token_id
                FROM quant.market_price_build_market_state s
                JOIN quant.market_price_eligibility e ON e.token_id = s.token_id
                WHERE s.source = %s
                  AND s.status = 'complete'
                  AND COALESCE(e.has_orderfilled_trades, FALSE) = TRUE
                  AND NOT EXISTS (
                      SELECT 1
                      FROM quant.market_token_block_close b
                      WHERE b.token_id = s.token_id
                  )
                ORDER BY e.orderfilled_trade_count DESC NULLS LAST, s.updated_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE quant.market_price_build_market_state s
            SET status = 'pending',
                last_complete_block = NULL,
                last_error = 'repaired: complete state had OrderFilled trades but no block close rows',
                updated_at = now()
            FROM broken
            WHERE s.source = %s
              AND s.token_id = broken.token_id
            """,
            (BLOCK_CLOSE_SOURCE, int(limit), BLOCK_CLOSE_SOURCE),
        )
        return int(cur.rowcount or 0)


def fetch_missing_market_progress_market_ids(
    conn: Any,
    *,
    since_ts: int,
    limit: int,
    shard_index: int = 0,
    shard_count: int = 1,
) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT m.market_id
            FROM quant.market_token_metadata m
            LEFT JOIN quant.market_price_build_market_progress p ON p.market_id = m.market_id
            LEFT JOIN quant.market_orderfilled_market_stats oms ON oms.market_id = m.market_id
            LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            WHERE
                p.market_id IS NULL
                AND m.created_at >= to_timestamp(%s)
                AND {market_shard_sql("m", shard_count=shard_count)}
            GROUP BY m.market_id
            HAVING
                bool_or(COALESCE(e.has_orderfilled_trades, FALSE))
                OR bool_or(COALESCE(e.eligible, FALSE))
                OR max(COALESCE(oms.trade_count, 0)) > 0
            ORDER BY min(m.created_at) ASC NULLS LAST, m.market_id ASC
            LIMIT %s
            """,
            (
                int(since_ts),
                *shard_params(shard_count=shard_count, shard_index=shard_index),
                int(limit),
            ),
        )
        return [int(row["market_id"]) for row in cur.fetchall()]


def backfill_missing_market_progress(
    conn: Any,
    *,
    since_ts: int,
    end_ts: int,
    limit: int = 5000,
    shard_index: int = 0,
    shard_count: int = 1,
) -> int:
    market_ids = fetch_missing_market_progress_market_ids(
        conn,
        since_ts=since_ts,
        limit=limit,
        shard_index=shard_index,
        shard_count=shard_count,
    )
    if not market_ids:
        return 0
    update_market_progress(conn, market_ids=market_ids, end_ts=end_ts)
    return len(market_ids)


def update_market_progress(conn: Any, *, market_ids: list[int], end_ts: int) -> None:
    if not market_ids:
        return
    with conn.cursor() as cur:
        for market_id in sorted({int(value) for value in market_ids}):
            cur.execute(
                """
                WITH meta AS (
                    SELECT *
                    FROM quant.market_token_metadata
                    WHERE market_id = %s
                ),
                rollup AS (
                    SELECT
                        m.market_id,
                        max(m.market_slug) AS market_slug,
                        count(*)::integer AS token_count,
                        count(*) FILTER (WHERE COALESCE(e.eligible, FALSE))::integer AS eligible_token_count,
                        min(e.first_orderfilled_block) FILTER (WHERE COALESCE(e.eligible, FALSE)) AS first_orderfilled_block,
                        max(e.last_orderfilled_block) FILTER (WHERE COALESCE(e.eligible, FALSE)) AS last_orderfilled_block,
                        min(sb.last_complete_block) FILTER (WHERE COALESCE(e.eligible, FALSE)) AS min_block_complete,
                        max(sb.last_complete_block) FILTER (WHERE COALESCE(e.eligible, FALSE)) AS max_block_complete,
                        min(sf.last_complete_ts) FILTER (WHERE COALESCE(e.eligible, FALSE)) AS min_frontend_complete_ts,
                        max(sf.last_complete_ts) FILTER (WHERE COALESCE(e.eligible, FALSE)) AS max_frontend_complete_ts,
                        count(*) FILTER (WHERE sb.status = 'error' OR sf.status = 'error') AS error_count,
                        max(COALESCE(sb.last_error, sf.last_error)) FILTER (WHERE sb.last_error IS NOT NULL OR sf.last_error IS NOT NULL) AS last_error,
                        bool_and(
                            COALESCE(e.eligible, FALSE) = FALSE
                            OR (
                                e.last_orderfilled_block IS NOT NULL
                                AND sb.last_complete_block IS NOT NULL
                                AND sb.last_complete_block >= e.last_orderfilled_block
                            )
                        ) AS block_complete,
                        bool_and(
                            COALESCE(e.eligible, FALSE) = FALSE
                            OR (
                                sf.last_complete_ts IS NOT NULL
                                AND sf.last_complete_ts >= LEAST(
                                    to_timestamp(%s),
                                    COALESCE(m.end_date + interval '1 day', to_timestamp(%s))
                                )
                            )
                        ) AS frontend_complete
                    FROM meta m
                    LEFT JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
                    LEFT JOIN quant.market_price_build_market_state sb
                        ON sb.source = %s AND sb.token_id = m.token_id
                    LEFT JOIN quant.market_price_build_market_state sf
                        ON sf.source = %s AND sf.token_id = m.token_id
                    GROUP BY m.market_id
                )
                INSERT INTO quant.market_price_build_market_progress (
                    market_id, market_slug, status, token_count, eligible_token_count,
                    first_orderfilled_block, last_orderfilled_block,
                    min_block_complete, max_block_complete,
                    min_frontend_complete_ts, max_frontend_complete_ts,
                    block_rows_written, frontend_rows_written,
                    error_count, last_error, updated_at
                )
                SELECT
                    r.market_id,
                    r.market_slug,
                    CASE
                        WHEN r.error_count > 0 THEN 'error'
                        WHEN r.eligible_token_count = 0 THEN 'skipped'
                        WHEN r.block_complete AND r.frontend_complete THEN 'complete'
                        ELSE 'running'
                    END AS status,
                    r.token_count,
                    r.eligible_token_count,
                    r.first_orderfilled_block,
                    r.last_orderfilled_block,
                    r.min_block_complete,
                    r.max_block_complete,
                    r.min_frontend_complete_ts,
                    r.max_frontend_complete_ts,
                    COALESCE((SELECT count(*) FROM quant.market_token_block_close p WHERE p.market_id = r.market_id), 0),
                    COALESCE((SELECT count(*) FROM quant.market_token_frontend_price_1m p WHERE p.market_id = r.market_id), 0),
                    r.error_count,
                    r.last_error,
                    clock_timestamp()
                FROM rollup r
                ON CONFLICT (market_id) DO UPDATE SET
                    market_slug = EXCLUDED.market_slug,
                    status = EXCLUDED.status,
                    token_count = EXCLUDED.token_count,
                    eligible_token_count = EXCLUDED.eligible_token_count,
                    first_orderfilled_block = EXCLUDED.first_orderfilled_block,
                    last_orderfilled_block = EXCLUDED.last_orderfilled_block,
                    min_block_complete = EXCLUDED.min_block_complete,
                    max_block_complete = EXCLUDED.max_block_complete,
                    min_frontend_complete_ts = EXCLUDED.min_frontend_complete_ts,
                    max_frontend_complete_ts = EXCLUDED.max_frontend_complete_ts,
                    block_rows_written = EXCLUDED.block_rows_written,
                    frontend_rows_written = EXCLUDED.frontend_rows_written,
                    error_count = EXCLUDED.error_count,
                    last_error = EXCLUDED.last_error,
                    updated_at = clock_timestamp()
                """,
                (
                    market_id,
                    int(end_ts),
                    int(end_ts),
                    BLOCK_CLOSE_SOURCE,
                    FRONTEND_SOURCE,
                ),
            )


def insert_single_token_block_close_rows(conn: Any, token: dict[str, Any], rows: list[dict[str, Any]]) -> int:
    clickhouse_token_id = str(token.get("clickhouse_token_id") or token.get("token_id_hex") or "").lower()
    if not clickhouse_token_id:
        return 0
    return insert_block_close_rows(conn, {clickhouse_token_id: token}, rows)


def insert_market_block_close_rows(
    conn: Any,
    *,
    tokens_by_clickhouse_id: dict[str, dict[str, Any]],
    planned_ranges: dict[str, tuple[int, int]],
    rows: list[dict[str, Any]],
) -> int:
    filtered_rows = []
    for row in rows:
        clickhouse_token_id = str(row.get("token_id") or "").lower()
        token = tokens_by_clickhouse_id.get(clickhouse_token_id)
        planned_range = planned_ranges.get(clickhouse_token_id)
        if not token or not planned_range:
            continue
        block_number = int(row["block_number"])
        start_block, to_block = planned_range
        if block_number < start_block or block_number > to_block:
            continue
        filtered_rows.append(row)
    return insert_block_close_rows(conn, tokens_by_clickhouse_id, filtered_rows)


def run_frontend_for_tokens(
    conn: Any,
    client: FrontendPriceClient,
    tokens: list[dict[str, Any]],
    *,
    since_ts: int,
    end_ts: int,
    window_seconds: int,
    overlap_seconds: int,
    fidelity_minutes: int,
    min_orderfilled_trades: int,
    include_active_without_trades: bool,
    all_markets: bool,
    recent_days: int,
    event_window_days: int,
    backtest_min_orderfilled_trades: int,
    defer_low_value: bool,
    crypto_window_seconds: int,
    crypto_after_seconds: int,
    request_pause_seconds: float,
) -> dict[str, int]:
    rows_written = 0
    failures = 0
    touched = 0
    skipped = 0
    for token in tokens:
        is_target = bool(token.get("target_priority"))
        is_trade_eligible = bool(token.get("eligible")) and int(token.get("orderfilled_trade_count") or 0) >= int(min_orderfilled_trades)
        is_live_fill = bool(include_active_without_trades) and bool(token.get("active")) and not bool(token.get("closed"))
        tier = classify_frontend_tier(
            token,
            recent_days=recent_days,
            backtest_min_orderfilled_trades=backtest_min_orderfilled_trades,
            defer_low_value=defer_low_value,
            now_ts=end_ts,
        )
        if bool(all_markets) and tier in {"skipped_no_orderfilled", "deferred_low_value"} and not is_target:
            skipped += 1
            state_status = "skipped" if tier == "skipped_no_orderfilled" else "deferred"
            update_frontend_state(
                conn,
                token=token,
                status=state_status,
                last_complete_ts=frontend_target_end_ts(token, end_ts),
                rows_written=0,
                last_error=f"frontend_tier:{tier}",
            )
            conn.commit()
            print(
                json.dumps(
                    {
                        "source": FRONTEND_SOURCE,
                        "token_id": token["token_id"],
                        "market_slug": token.get("market_slug"),
                        "frontend_tier": tier,
                        "status": state_status,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            continue
        is_all_market_fill = bool(all_markets) and tier != "skipped_no_orderfilled"
        if not (is_target or is_all_market_fill or is_trade_eligible or is_live_fill):
            continue
        last_ts = maybe_datetime_to_ts(token.get("last_complete_ts"))
        requested_from_ts = maybe_datetime_to_ts(token.get("requested_from_ts"))
        requested_to_ts = maybe_datetime_to_ts(token.get("requested_to_ts"))
        created_at_ts = maybe_datetime_to_ts(token.get("created_at"))
        end_date_ts = maybe_datetime_to_ts(token.get("end_date"))
        event_start_ts = None
        event_end_limit_ts = None
        if tier == "crypto_short_full":
            slug_ts = parse_slug_timestamp(token)
            if slug_ts:
                event_start_ts = max(created_at_ts or int(since_ts), slug_ts - int(crypto_window_seconds))
                event_end_limit_ts = slug_ts + int(crypto_after_seconds)
        if tier == "longtail_event_window" and end_date_ts:
            event_start_ts = max(created_at_ts or int(since_ts), end_date_ts - int(event_window_days) * 86400)
        base_start_ts = requested_from_ts or event_start_ts or created_at_ts or since_ts
        anchor_ts = last_ts or base_start_ts
        if requested_from_ts and (last_ts is None or last_ts < requested_from_ts):
            anchor_ts = requested_from_ts
        if event_start_ts and (last_ts is None or last_ts < event_start_ts):
            anchor_ts = event_start_ts
        if created_at_ts and (last_ts is None or last_ts < created_at_ts):
            anchor_ts = max(anchor_ts, created_at_ts)
        start_ts = max(int(since_ts), int(anchor_ts - overlap_seconds))
        token_end_ts = min(int(end_ts), start_ts + int(window_seconds) - 1)
        if requested_to_ts:
            token_end_ts = min(token_end_ts, requested_to_ts)
        if event_end_limit_ts:
            token_end_ts = min(token_end_ts, event_end_limit_ts)
        if end_date_ts and bool(token.get("closed")):
            token_end_ts = min(token_end_ts, end_date_ts + 86400)
        if token_end_ts <= start_ts:
            continue
        touched += 1
        try:
            points, token_failures = client.fetch_segmented_prices_history(
                str(token["token_id"]),
                start_ts=start_ts,
                end_ts=token_end_ts,
                fidelity_minutes=fidelity_minutes,
                pause_seconds=request_pause_seconds,
            )
            written = insert_frontend_points(
                conn,
                market_id=int(token["market_id"]),
                market_slug=token.get("market_slug"),
                token_id=str(token["token_id"]),
                token_side=str(token["token_side"]),
                points=points,
                fidelity_minutes=fidelity_minutes,
            )
            rows_written += written
            failures += len(token_failures)
            update_frontend_state(
                conn,
                token=token,
                status="error" if token_failures else "complete",
                last_complete_ts=token_end_ts,
                rows_written=written,
                last_error="; ".join(f.error for f in token_failures) if token_failures else None,
            )
            print(
                json.dumps(
                    {
                        "source": FRONTEND_SOURCE,
                        "token_id": token["token_id"],
                        "market_slug": token.get("market_slug"),
                        "start_ts": start_ts,
                        "end_ts": token_end_ts,
                        "frontend_tier": tier,
                        "rows_written": written,
                        "failures": len(token_failures),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            if request_pause_seconds > 0:
                time.sleep(float(request_pause_seconds))
        except Exception as exc:  # noqa: BLE001
            failures += 1
            update_frontend_state(conn, token=token, status="error", last_complete_ts=last_ts or since_ts, rows_written=0, last_error=repr(exc))
    return {"tokens": touched + skipped, "rows_written": rows_written, "failures": failures, "skipped": skipped}


def run_frontend_incremental(
    conn: Any,
    client: FrontendPriceClient,
    *,
    since_ts: int,
    end_ts: int,
    token_limit: int,
    window_seconds: int,
    overlap_seconds: int,
    fidelity_minutes: int,
    min_orderfilled_trades: int,
    include_active_without_trades: bool,
    targets_only: bool,
    all_markets: bool,
    recent_days: int,
    event_window_days: int,
    backtest_min_orderfilled_trades: int,
    defer_low_value: bool,
    crypto_window_seconds: int,
    crypto_after_seconds: int,
    request_pause_seconds: float,
    shard_index: int,
    shard_count: int,
) -> dict[str, int]:
    bulk_skipped = 0
    if all_markets:
        bulk_skipped = mark_frontend_skipped_no_orderfilled(
            conn,
            since_ts=since_ts,
            end_ts=end_ts,
            limit=max(int(token_limit) * 20, 1000),
            shard_index=shard_index,
            shard_count=shard_count,
        )
        if bulk_skipped:
            conn.commit()
            print(
                json.dumps(
                    {
                        "event": "frontend_bulk_skipped_no_orderfilled",
                        "source": FRONTEND_SOURCE,
                        "skipped": bulk_skipped,
                        "shard_index": shard_index,
                        "shard_count": shard_count,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
    bulk_deferred = 0
    if all_markets and defer_low_value:
        bulk_deferred = mark_frontend_deferred_low_value(
            conn,
            since_ts=since_ts,
            end_ts=end_ts,
            recent_days=recent_days,
            min_orderfilled_trades=backtest_min_orderfilled_trades,
            limit=max(int(token_limit) * 20, 1000),
            shard_index=shard_index,
            shard_count=shard_count,
        )
        if bulk_deferred:
            conn.commit()
            print(
                json.dumps(
                    {
                        "event": "frontend_bulk_deferred_low_value",
                        "source": FRONTEND_SOURCE,
                        "deferred": bulk_deferred,
                        "shard_index": shard_index,
                        "shard_count": shard_count,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
    tokens = fetch_frontend_build_candidates(
        conn,
        since_ts=since_ts,
        end_ts=end_ts,
        limit=token_limit,
        min_orderfilled_trades=min_orderfilled_trades,
        include_active_without_trades=include_active_without_trades,
        targets_only=targets_only,
        all_markets=all_markets,
        shard_index=shard_index,
        shard_count=shard_count,
    )
    result = run_frontend_for_tokens(
        conn,
        client,
        tokens,
        since_ts=since_ts,
        end_ts=end_ts,
        window_seconds=window_seconds,
        overlap_seconds=overlap_seconds,
        fidelity_minutes=fidelity_minutes,
        min_orderfilled_trades=min_orderfilled_trades,
        include_active_without_trades=include_active_without_trades,
        all_markets=all_markets,
        recent_days=recent_days,
        event_window_days=event_window_days,
        backtest_min_orderfilled_trades=backtest_min_orderfilled_trades,
        defer_low_value=defer_low_value,
        crypto_window_seconds=crypto_window_seconds,
        crypto_after_seconds=crypto_after_seconds,
        request_pause_seconds=request_pause_seconds,
    )
    result["tokens"] = int(result.get("tokens") or 0) + int(bulk_skipped) + int(bulk_deferred)
    result["skipped"] = int(result.get("skipped") or 0) + int(bulk_skipped) + int(bulk_deferred)
    return result


def api_trades_target_end_ts(token: dict[str, Any], end_ts: int) -> int:
    token_end_ts = int(end_ts)
    end_date_ts = maybe_datetime_to_ts(token.get("end_date"))
    if end_date_ts and bool(token.get("closed")):
        token_end_ts = min(token_end_ts, end_date_ts + 86400)
    requested_to_ts = maybe_datetime_to_ts(token.get("requested_to_ts"))
    if requested_to_ts:
        token_end_ts = min(token_end_ts, requested_to_ts)
    return token_end_ts


def run_api_trades_for_tokens(
    conn: Any,
    ch: ClickHouseClient,
    tokens: list[dict[str, Any]],
    *,
    since_ts: int,
    end_ts: int,
    window_seconds: int,
    overlap_seconds: int,
    min_orderfilled_trades: int,
    include_active_without_trades: bool,
) -> dict[str, int]:
    rows_written = 0
    failures = 0
    touched = 0
    no_rows = 0
    for token in tokens:
        is_target = bool(token.get("target_priority"))
        is_trade_eligible = bool(token.get("eligible")) and int(token.get("orderfilled_trade_count") or 0) >= int(min_orderfilled_trades)
        is_live_fill = bool(include_active_without_trades) and bool(token.get("active")) and not bool(token.get("closed"))
        if not (is_target or is_trade_eligible or is_live_fill):
            continue
        last_ts = maybe_datetime_to_ts(token.get("last_complete_ts"))
        requested_from_ts = maybe_datetime_to_ts(token.get("requested_from_ts"))
        created_at_ts = maybe_datetime_to_ts(token.get("created_at"))
        base_start_ts = requested_from_ts or created_at_ts or since_ts
        anchor_ts = last_ts or base_start_ts
        if requested_from_ts and (last_ts is None or last_ts < requested_from_ts):
            anchor_ts = requested_from_ts
        if created_at_ts and (last_ts is None or last_ts < created_at_ts):
            anchor_ts = max(anchor_ts, created_at_ts)
        start_ts = max(int(since_ts), int(anchor_ts - overlap_seconds))
        token_end_cap = api_trades_target_end_ts(token, end_ts)
        token_end_ts = min(int(token_end_cap), start_ts + int(window_seconds) - 1)
        if token_end_ts <= start_ts:
            continue
        touched += 1
        try:
            sql = api_trades_sql(
                start_ts=start_ts,
                end_ts=token_end_ts,
                market_ids=[int(token["market_id"])],
                token_ids=[str(token["token_id"])],
            )
            rows = ch.query_json_rows(sql)
            written = insert_api_trade_rows(conn, {str(token["token_id"]): token}, rows)
            rows_written += written
            state_complete_ts = token_end_ts
            if not rows:
                no_rows += 1
                next_trade_ts = find_next_api_trade_ts(
                    ch,
                    token=token,
                    start_ts=token_end_ts + 1,
                    end_ts=token_end_cap,
                )
                if next_trade_ts is None:
                    state_complete_ts = token_end_cap
                else:
                    state_complete_ts = max(token_end_ts, min(int(next_trade_ts), token_end_cap))
            update_api_trades_state(
                conn,
                token=token,
                status="complete",
                last_complete_ts=state_complete_ts,
                rows_written=written,
            )
            conn.commit()
            print(
                json.dumps(
                    {
                        "source": API_TRADES_SOURCE,
                        "token_id": token["token_id"],
                        "market_slug": token.get("market_slug"),
                        "start_ts": start_ts,
                        "end_ts": token_end_ts,
                        "state_complete_ts": state_complete_ts,
                        "rows_read": len(rows),
                        "rows_written": written,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            failures += 1
            update_api_trades_state(
                conn,
                token=token,
                status="error",
                last_complete_ts=last_ts or since_ts,
                rows_written=0,
                last_error=repr(exc),
            )
            conn.commit()
    return {"tokens": touched, "rows_written": rows_written, "failures": failures, "no_rows": no_rows}


def find_next_api_trade_ts(
    ch: ClickHouseClient,
    *,
    token: dict[str, Any],
    start_ts: int,
    end_ts: int,
) -> int | None:
    if int(end_ts) < int(start_ts):
        return None
    probe_sql = f"""
        SELECT min(trade_timestamp)
        FROM pg_api_trades_main
        WHERE trade_timestamp >= {int(start_ts)}
          AND trade_timestamp <= {int(end_ts)}
          AND trade_time >= toDateTime({int(start_ts)}, 'UTC')
          AND trade_time <= toDateTime({int(end_ts)}, 'UTC')
          AND market_id = {int(token["market_id"])}
          AND asset = {quote_clickhouse_string(str(token["token_id"]))}
          AND price >= 0
          AND price <= 1
          AND size > 0
    """
    raw = ch.query_scalar(probe_sql)
    text = str(raw or "").strip()
    if not text or text == "0" or text.upper() in {"NULL", "\\N", "NAN"}:
        return None
    return int(float(text))


def run_api_trades_incremental(
    conn: Any,
    ch: ClickHouseClient,
    *,
    since_ts: int,
    end_ts: int,
    token_limit: int,
    window_seconds: int,
    overlap_seconds: int,
    min_orderfilled_trades: int,
    include_active_without_trades: bool,
    shard_index: int,
    shard_count: int,
) -> dict[str, int]:
    tokens = fetch_api_trades_build_candidates(
        conn,
        since_ts=since_ts,
        end_ts=end_ts,
        limit=token_limit,
        min_orderfilled_trades=min_orderfilled_trades,
        include_active_without_trades=include_active_without_trades,
        shard_index=shard_index,
        shard_count=shard_count,
    )
    return run_api_trades_for_tokens(
        conn,
        ch,
        tokens,
        since_ts=since_ts,
        end_ts=end_ts,
        window_seconds=window_seconds,
        overlap_seconds=overlap_seconds,
        min_orderfilled_trades=min_orderfilled_trades,
        include_active_without_trades=include_active_without_trades,
    )


def run_block_close_for_tokens(
    conn: Any,
    ch: ClickHouseClient,
    tokens: list[dict[str, Any]],
    *,
    block_window: int,
    block_overlap: int,
    min_orderfilled_trades: int,
) -> dict[str, int]:
    rows_written = 0
    failures = 0
    table = ch.settings.orderfilled_table
    planned_tokens = []
    for token in tokens:
        if not bool(token.get("eligible")) and not token.get("target_priority"):
            continue
        if int(token.get("orderfilled_trade_count") or 0) < int(min_orderfilled_trades) and not token.get("target_priority"):
            continue
        first_block = int(token.get("first_orderfilled_block") or 0)
        last_block = int(token.get("last_orderfilled_block") or 0)
        last_complete = int(token.get("last_complete_block") or 0)
        requested_from_block = int(token.get("requested_from_block") or 0)
        requested_to_block = int(token.get("requested_to_block") or 0)
        if first_block <= 0 or last_block <= 0:
            continue
        has_block_rows = bool(token.get("has_block_rows"))
        if not has_block_rows and token.get("build_status") != "empty":
            last_complete = 0
        anchor_block = last_complete or requested_from_block or first_block
        if requested_from_block and (not last_complete or last_complete < requested_from_block):
            anchor_block = requested_from_block
        start_block = max(first_block, anchor_block - int(block_overlap) + 1 if last_complete else anchor_block)
        to_block = min(last_block, start_block + int(block_window) - 1)
        if requested_to_block:
            to_block = min(to_block, requested_to_block)
        if to_block < start_block:
            continue
        planned_token = dict(token)
        planned_token["planned_from_block"] = start_block
        planned_token["planned_to_block"] = to_block
        planned_token["clickhouse_token_id"] = str(token["token_id_hex"]).lower()
        planned_tokens.append(planned_token)

    touched = len(planned_tokens)
    market_groups: list[list[dict[str, Any]]] = []
    current_market_id: int | None = None
    current_group: list[dict[str, Any]] = []
    for token in planned_tokens:
        market_id = int(token["market_id"])
        if current_group and market_id != current_market_id:
            market_groups.append(current_group)
            current_group = []
        current_market_id = market_id
        current_group.append(token)
    if current_group:
        market_groups.append(current_group)

    chunked_market_groups: list[list[dict[str, Any]]] = []
    current_chunk: list[dict[str, Any]] = []
    for market_group in market_groups:
        if current_chunk and len(current_chunk) + len(market_group) > int(BLOCK_CLOSE_QUERY_TOKEN_BATCH_SIZE):
            chunked_market_groups.append(current_chunk)
            current_chunk = []
        current_chunk.extend(market_group)
    if current_chunk:
        chunked_market_groups.append(current_chunk)

    for chunk_tokens in chunked_market_groups:
        from_block = min(int(token["planned_from_block"]) for token in chunk_tokens)
        to_block = max(int(token["planned_to_block"]) for token in chunk_tokens)
        market_ids = sorted({int(token["market_id"]) for token in chunk_tokens})
        token_ids = [str(token["clickhouse_token_id"]) for token in chunk_tokens]
        tokens_by_clickhouse_id = {str(token["clickhouse_token_id"]): token for token in chunk_tokens}
        planned_ranges = {
            str(token["clickhouse_token_id"]): (int(token["planned_from_block"]), int(token["planned_to_block"]))
            for token in chunk_tokens
        }
        try:
            sql = orderfilled_block_close_sql(
                table=table,
                from_block=from_block,
                to_block=to_block,
                # CLOB token ids are globally unique, while old live indexer rows
                # may carry placeholder market ids until Gamma discovery catches up.
                # Query by token only and write rows under current canonical metadata.
                market_ids=None,
                token_ids=token_ids,
            )
            rows = ch.query_json_rows(sql)
            written = insert_market_block_close_rows(
                conn,
                tokens_by_clickhouse_id=tokens_by_clickhouse_id,
                planned_ranges=planned_ranges,
                rows=rows,
            )
            rows_written += written
            covered_rows_by_token = count_block_close_rows_for_tokens(conn, chunk_tokens)
            missing_tokens = []
            for token in chunk_tokens:
                covered_rows = int(covered_rows_by_token.get(str(token["token_id"]), 0))
                if covered_rows > 0:
                    update_block_state(conn, token=token, status="complete", last_complete_block=int(token["planned_to_block"]))
                else:
                    missing_tokens.append(token)
                    update_block_state(
                        conn,
                        token=token,
                        status="empty",
                        last_complete_block=int(token["planned_to_block"]),
                        last_error=(
                            "no block close rows in planned range "
                            f"{int(token['planned_from_block'])}-{int(token['planned_to_block'])}"
                        ),
                    )
            conn.commit()
            print(
                json.dumps(
                    {
                        "source": BLOCK_CLOSE_SOURCE,
                        "market_count": len(market_ids),
                        "first_market_id": market_ids[0] if market_ids else None,
                        "last_market_id": market_ids[-1] if market_ids else None,
                        "token_count": len(chunk_tokens),
                        "from_block": from_block,
                        "to_block": to_block,
                        "rows_written": written,
                        "tokens_without_block_rows": len(missing_tokens),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            failures += len(chunk_tokens)
            for token in chunk_tokens:
                update_block_state(
                    conn,
                    token=token,
                    status="error",
                    last_complete_block=int(token.get("last_complete_block") or token.get("first_orderfilled_block") or 0),
                    last_error=repr(exc),
                )
            conn.commit()
    return {"tokens": touched, "rows_written": rows_written, "failures": failures}


def run_block_close_incremental(
    conn: Any,
    ch: ClickHouseClient,
    *,
    since_ts: int,
    created_before_ts: int | None,
    token_limit: int,
    block_window: int,
    block_overlap: int,
    min_orderfilled_trades: int,
    live_only: bool,
    live_lookback_days: int,
    shard_index: int,
    shard_count: int,
) -> dict[str, int]:
    tokens = fetch_block_close_build_candidates(
        conn,
        since_ts=since_ts,
        created_before_ts=created_before_ts,
        limit=token_limit,
        min_orderfilled_trades=min_orderfilled_trades,
        live_only=live_only,
        live_lookback_days=live_lookback_days,
        shard_index=shard_index,
        shard_count=shard_count,
    )
    if tokens:
        market_ids = sorted({int(token["market_id"]) for token in tokens})
        expanded_tokens = fetch_block_close_market_sibling_candidates(
            conn,
            market_ids=market_ids,
            since_ts=since_ts,
            created_before_ts=created_before_ts,
            limit=max(int(token_limit), int(token_limit) * 3),
            min_orderfilled_trades=min_orderfilled_trades,
        )
        tokens_by_id = {str(token["token_id"]): token for token in tokens}
        for token in expanded_tokens:
            tokens_by_id[str(token["token_id"])] = token
        tokens = list(tokens_by_id.values())
    result = run_block_close_for_tokens(
        conn,
        ch,
        tokens,
        block_window=block_window,
        block_overlap=block_overlap,
        min_orderfilled_trades=min_orderfilled_trades,
    )
    market_ids = sorted({int(token["market_id"]) for token in tokens})
    update_market_progress(conn, market_ids=market_ids, end_ts=utc_now_ts())
    conn.commit()
    result["markets"] = len(market_ids)
    return result


def run_market_price_incremental(
    conn: Any,
    ch: ClickHouseClient,
    client: FrontendPriceClient,
    *,
    since_ts: int,
    end_ts: int,
    market_limit: int,
    block_window: int,
    block_overlap: int,
    block_min_orderfilled_trades: int,
    frontend_window_seconds: int,
    frontend_overlap_seconds: int,
    frontend_min_orderfilled_trades: int,
    frontend_include_active_without_trades: bool,
    frontend_all_markets: bool,
    frontend_recent_days: int,
    frontend_event_window_days: int,
    frontend_backtest_min_orderfilled_trades: int,
    frontend_defer_low_value: bool,
    frontend_crypto_window_seconds: int,
    frontend_crypto_after_seconds: int,
    frontend_request_pause_seconds: float,
    fidelity_minutes: int,
    shard_index: int,
    shard_count: int,
) -> dict[str, int]:
    market_ids = fetch_price_market_chronological_candidates(
        conn,
        since_ts=since_ts,
        end_ts=end_ts,
        limit=market_limit,
        frontend_min_orderfilled_trades=frontend_min_orderfilled_trades,
        frontend_include_active_without_trades=frontend_include_active_without_trades,
        shard_index=shard_index,
        shard_count=shard_count,
    )
    tokens = fetch_market_tokens_for_block_close(conn, market_ids=market_ids)
    tokens = refresh_market_token_eligibility(conn, ch, tokens)
    block_result = run_block_close_for_tokens(
        conn,
        ch,
        tokens,
        block_window=block_window,
        block_overlap=block_overlap,
        min_orderfilled_trades=block_min_orderfilled_trades,
    )
    frontend_result = run_frontend_for_tokens(
        conn,
        client,
        tokens,
        since_ts=since_ts,
        end_ts=end_ts,
        window_seconds=frontend_window_seconds,
        overlap_seconds=frontend_overlap_seconds,
        fidelity_minutes=fidelity_minutes,
        min_orderfilled_trades=frontend_min_orderfilled_trades,
        include_active_without_trades=frontend_include_active_without_trades,
        all_markets=frontend_all_markets,
        recent_days=frontend_recent_days,
        event_window_days=frontend_event_window_days,
        backtest_min_orderfilled_trades=frontend_backtest_min_orderfilled_trades,
        defer_low_value=frontend_defer_low_value,
        crypto_window_seconds=frontend_crypto_window_seconds,
        crypto_after_seconds=frontend_crypto_after_seconds,
        request_pause_seconds=frontend_request_pause_seconds,
    )
    update_market_progress(conn, market_ids=market_ids, end_ts=end_ts)
    return {
        "markets": len(market_ids),
        "tokens": len(tokens),
        "block_tokens": block_result["tokens"],
        "block_rows_written": block_result["rows_written"],
        "block_failures": block_result["failures"],
        "frontend_tokens": frontend_result["tokens"],
        "frontend_rows_written": frontend_result["rows_written"],
        "frontend_failures": frontend_result["failures"],
        "rows_written": block_result["rows_written"] + frontend_result["rows_written"],
        "failures": block_result["failures"] + frontend_result["failures"],
    }


def maintenance_refresh_since_ts(
    conn: Any,
    *,
    table: str,
    baseline_since_ts: int,
    lookback_seconds: int,
) -> int:
    allowed_tables = {
        "quant.market_token_metadata",
        "quant.market_event_members",
    }
    if table not in allowed_tables:
        raise ValueError(f"unsupported maintenance table: {table}")
    with conn.cursor() as cur:
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1) AS has_rows")
        has_rows = bool(cur.fetchone()["has_rows"])
    if not has_rows or int(lookback_seconds) <= 0:
        return int(baseline_since_ts)
    return max(int(baseline_since_ts), utc_now_ts() - int(lookback_seconds))


def run_daemon(args: argparse.Namespace) -> None:
    metadata_next = 0.0
    event_membership_next = 0.0
    eligibility_next = 0.0
    progress_backfill_next = 0.0
    orderfilled_stats_next: float | None = None
    schema_ready = bool(args.skip_schema)
    while True:
        cycle_started = time.time()
        with postgres_connection(PostgresSettings()) as conn:
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off")
            if not schema_ready:
                create_schema(conn)
                conn.commit()
                schema_ready = True
            if orderfilled_stats_next is None:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT EXISTS (SELECT 1 FROM quant.market_orderfilled_market_stats LIMIT 1) AS has_rows"
                    )
                    stats_ready = bool(cur.fetchone()["has_rows"])
                orderfilled_stats_next = (
                    cycle_started + float(args.orderfilled_market_stats_interval_seconds)
                    if stats_ready
                    else 0.0
                )
            if (
                not args.skip_orderfilled_market_stats_refresh
                and args.source_mode in {"all", "both", BLOCK_CLOSE_SOURCE, "maintenance"}
                and cycle_started >= orderfilled_stats_next
            ):
                count = refresh_orderfilled_market_stats(conn, ClickHouseClient(), commit_batches=True)
                conn.commit()
                orderfilled_stats_next = time.time() + float(args.orderfilled_market_stats_interval_seconds)
                print(json.dumps({"event": "orderfilled_market_stats_refreshed", "count": count}, sort_keys=True), flush=True)
            if not args.skip_maintenance and cycle_started >= metadata_next:
                metadata_since_ts = maintenance_refresh_since_ts(
                    conn,
                    table="quant.market_token_metadata",
                    baseline_since_ts=args.since_ts,
                    lookback_seconds=args.maintenance_lookback_seconds,
                )
                count = refresh_market_token_metadata(conn, since_ts=metadata_since_ts)
                conn.commit()
                missing_count = refresh_missing_market_token_metadata(
                    conn,
                    limit=args.metadata_missing_limit,
                )
                conn.commit()
                metadata_next = time.time() + float(args.metadata_interval_seconds)
                print(
                    json.dumps(
                        {
                            "event": "metadata_refreshed",
                            "count": count,
                            "missing_backfilled": missing_count,
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                gc.collect()
            if not args.skip_maintenance and cycle_started >= eligibility_next:
                # Eligibility is a recurring freshness task, not a historical
                # full-table rebuild. Passing the September baseline here
                # made PostgreSQL rank almost four million metadata rows every
                # two minutes, exceed temp_file_limit, and restart the same
                # worker that keeps new market/token metadata current.
                eligibility_since_ts = max(
                    int(args.since_ts),
                    int(cycle_started)
                    - max(1, int(args.maintenance_lookback_seconds)),
                )
                count = refresh_since_eligibility(
                    conn,
                    ClickHouseClient(),
                    since_ts=eligibility_since_ts,
                    shard_index=args.shard_index,
                    shard_count=args.shard_count,
                )
                conn.commit()
                eligibility_next = time.time() + float(args.eligibility_interval_seconds)
                print(json.dumps({"event": "eligibility_refreshed", "count": count}, sort_keys=True), flush=True)
            if not args.skip_maintenance and cycle_started >= progress_backfill_next:
                count = backfill_missing_market_progress(
                    conn,
                    since_ts=args.since_ts,
                    end_ts=utc_now_ts() - int(args.frontend_lag_seconds),
                    limit=max(1000, int(args.token_limit)),
                    shard_index=args.shard_index,
                    shard_count=args.shard_count,
                )
                conn.commit()
                progress_backfill_next = time.time() + max(
                    60.0,
                    min(
                        float(args.event_membership_interval_seconds),
                        float(args.eligibility_interval_seconds),
                    ),
                )
                print(json.dumps({"event": "market_progress_backfilled", "count": count}, sort_keys=True), flush=True)
                gc.collect()
            if not args.skip_maintenance and cycle_started >= event_membership_next:
                # Keep the live membership read model current even when an
                # older deployment left a historical backfill gap.  Using the
                # stale table watermark here made the daemon aggregate weeks
                # of token rows, exceed temp_file_limit, restart, and never
                # reach newly discovered markets. Historical repair is a
                # separate bounded job; live maintenance is a rolling window.
                event_since_ts = max(
                    int(args.since_ts),
                    int(cycle_started)
                    - max(1, int(args.maintenance_lookback_seconds)),
                )
                event_result = refresh_market_event_memberships(conn, since_ts=event_since_ts)
                conn.commit()
                event_membership_next = time.time() + float(args.event_membership_interval_seconds)
                print(json.dumps({"event": "event_memberships_refreshed", **event_result}, sort_keys=True), flush=True)
                gc.collect()

            if args.source_mode in {"both", "all"}:
                end_ts = utc_now_ts() - int(args.frontend_lag_seconds)
                pipeline_run_id = start_run(
                    conn,
                    source="market_price_pipeline",
                    mode="daemon",
                    meta={
                        "since_ts": args.since_ts,
                        "end_ts": end_ts,
                        "market_limit": args.token_limit,
                        "block_window": args.block_window,
                        "block_min_orderfilled_trades": args.block_min_orderfilled_trades,
                        "frontend_min_orderfilled_trades": args.frontend_min_orderfilled_trades,
                        "frontend_include_active_without_trades": args.frontend_include_active_without_trades,
                        "frontend_all_markets": args.frontend_all_markets,
                        "frontend_recent_days": args.frontend_recent_days,
                        "frontend_event_window_days": args.frontend_event_window_days,
                        "frontend_backtest_min_orderfilled_trades": args.frontend_backtest_min_orderfilled_trades,
                        "frontend_defer_low_value": args.frontend_defer_low_value,
                        "frontend_crypto_window_seconds": args.frontend_crypto_window_seconds,
                        "frontend_crypto_after_seconds": args.frontend_crypto_after_seconds,
                        "frontend_request_pause_seconds": args.frontend_request_pause_seconds,
                        "shard_index": args.shard_index,
                        "shard_count": args.shard_count,
                    },
                )
                pipeline_result = run_market_price_incremental(
                    conn,
                    ClickHouseClient(),
                    FrontendPriceClient(),
                    since_ts=args.since_ts,
                    end_ts=end_ts,
                    market_limit=args.token_limit,
                    block_window=args.block_window,
                    block_overlap=args.block_overlap,
                    block_min_orderfilled_trades=args.block_min_orderfilled_trades,
                    frontend_window_seconds=args.frontend_window_seconds,
                    frontend_overlap_seconds=args.frontend_overlap_seconds,
                    frontend_min_orderfilled_trades=args.frontend_min_orderfilled_trades,
                    frontend_include_active_without_trades=args.frontend_include_active_without_trades,
                    frontend_all_markets=args.frontend_all_markets,
                    frontend_recent_days=args.frontend_recent_days,
                    frontend_event_window_days=args.frontend_event_window_days,
                    frontend_backtest_min_orderfilled_trades=args.frontend_backtest_min_orderfilled_trades,
                    frontend_defer_low_value=args.frontend_defer_low_value,
                    frontend_crypto_window_seconds=args.frontend_crypto_window_seconds,
                    frontend_crypto_after_seconds=args.frontend_crypto_after_seconds,
                    frontend_request_pause_seconds=args.frontend_request_pause_seconds,
                    fidelity_minutes=args.fidelity_minutes,
                    shard_index=args.shard_index,
                    shard_count=args.shard_count,
                )
                finish_run(
                    conn,
                    run_id=pipeline_run_id,
                    status="complete" if pipeline_result["failures"] == 0 else "error",
                    rows_written=pipeline_result["rows_written"],
                    markets_complete=pipeline_result["markets"],
                    error_count=pipeline_result["failures"],
                )
                print(json.dumps({"event": "market_price_pipeline", **pipeline_result}, sort_keys=True), flush=True)

            if args.source_mode in {API_TRADES_SOURCE, "all"}:
                end_ts = utc_now_ts() - int(args.api_trades_lag_seconds)
                api_run_id = start_run(
                    conn,
                    source=API_TRADES_SOURCE,
                    mode="daemon",
                    meta={
                        "since_ts": args.since_ts,
                        "end_ts": end_ts,
                        "token_limit": args.api_trades_token_limit,
                        "window_seconds": args.api_trades_window_seconds,
                        "overlap_seconds": args.api_trades_overlap_seconds,
                        "min_orderfilled_trades": args.api_trades_min_orderfilled_trades,
                        "include_active_without_trades": args.api_trades_include_active_without_trades,
                        "shard_index": args.shard_index,
                        "shard_count": args.shard_count,
                    },
                )
                api_result = run_api_trades_incremental(
                    conn,
                    ClickHouseClient(),
                    since_ts=args.since_ts,
                    end_ts=end_ts,
                    token_limit=args.api_trades_token_limit,
                    window_seconds=args.api_trades_window_seconds,
                    overlap_seconds=args.api_trades_overlap_seconds,
                    min_orderfilled_trades=args.api_trades_min_orderfilled_trades,
                    include_active_without_trades=args.api_trades_include_active_without_trades,
                    shard_index=args.shard_index,
                    shard_count=args.shard_count,
                )
                finish_run(
                    conn,
                    run_id=api_run_id,
                    status="complete" if api_result["failures"] == 0 else "error",
                    rows_written=api_result["rows_written"],
                    markets_complete=api_result["tokens"],
                    error_count=api_result["failures"],
                )
                print(json.dumps({"event": "api_trades_incremental", **api_result}, sort_keys=True), flush=True)

            if args.source_mode == FRONTEND_SOURCE:
                end_ts = utc_now_ts() - int(args.frontend_lag_seconds)
                frontend_run_id = start_run(
                    conn,
                    source=FRONTEND_SOURCE,
                    mode="daemon",
                    meta={
                        "since_ts": args.since_ts,
                        "end_ts": end_ts,
                        "token_limit": args.frontend_token_limit,
                        "min_orderfilled_trades": args.frontend_min_orderfilled_trades,
                        "include_active_without_trades": args.frontend_include_active_without_trades,
                        "all_markets": args.frontend_all_markets,
                        "recent_days": args.frontend_recent_days,
                        "event_window_days": args.frontend_event_window_days,
                        "backtest_min_orderfilled_trades": args.frontend_backtest_min_orderfilled_trades,
                        "defer_low_value": args.frontend_defer_low_value,
                        "crypto_window_seconds": args.frontend_crypto_window_seconds,
                        "crypto_after_seconds": args.frontend_crypto_after_seconds,
                        "request_pause_seconds": args.frontend_request_pause_seconds,
                        "targets_only": args.frontend_targets_only,
                        "shard_index": args.shard_index,
                        "shard_count": args.shard_count,
                    },
                )
                frontend_result = run_frontend_incremental(
                    conn,
                    FrontendPriceClient(),
                    since_ts=args.since_ts,
                    end_ts=end_ts,
                    token_limit=args.frontend_token_limit,
                    window_seconds=args.frontend_window_seconds,
                    overlap_seconds=args.frontend_overlap_seconds,
                    fidelity_minutes=args.fidelity_minutes,
                    min_orderfilled_trades=args.frontend_min_orderfilled_trades,
                    include_active_without_trades=args.frontend_include_active_without_trades,
                    targets_only=args.frontend_targets_only,
                    all_markets=args.frontend_all_markets,
                    recent_days=args.frontend_recent_days,
                    event_window_days=args.frontend_event_window_days,
                    backtest_min_orderfilled_trades=args.frontend_backtest_min_orderfilled_trades,
                    defer_low_value=args.frontend_defer_low_value,
                    crypto_window_seconds=args.frontend_crypto_window_seconds,
                    crypto_after_seconds=args.frontend_crypto_after_seconds,
                    request_pause_seconds=args.frontend_request_pause_seconds,
                    shard_index=args.shard_index,
                    shard_count=args.shard_count,
                )
                finish_run(
                    conn,
                    run_id=frontend_run_id,
                    status="complete" if frontend_result["failures"] == 0 else "error",
                    rows_written=frontend_result["rows_written"],
                    markets_complete=frontend_result["tokens"],
                    error_count=frontend_result["failures"],
                )

            if args.source_mode == BLOCK_CLOSE_SOURCE:
                block_run_id = start_run(
                    conn,
                    source=BLOCK_CLOSE_SOURCE,
                    mode="daemon",
                    meta={
                        "since_ts": args.since_ts,
                        "created_before_ts": args.created_before_ts,
                        "token_limit": args.block_token_limit,
                        "block_window": args.block_window,
                        "min_orderfilled_trades": args.block_min_orderfilled_trades,
                        "live_only": args.block_live_only,
                        "live_lookback_days": args.block_live_lookback_days,
                        "shard_index": args.shard_index,
                        "shard_count": args.shard_count,
                    },
                )
                block_result = run_block_close_incremental(
                    conn,
                    ClickHouseClient(),
                    since_ts=args.since_ts,
                    created_before_ts=args.created_before_ts,
                    token_limit=args.block_token_limit,
                    block_window=args.block_window,
                    block_overlap=args.block_overlap,
                    min_orderfilled_trades=args.block_min_orderfilled_trades,
                    live_only=args.block_live_only,
                    live_lookback_days=args.block_live_lookback_days,
                    shard_index=args.shard_index,
                    shard_count=args.shard_count,
                )
                finish_run(
                    conn,
                    run_id=block_run_id,
                    status="complete" if block_result["failures"] == 0 else "error",
                    rows_written=block_result["rows_written"],
                    markets_complete=block_result["tokens"],
                    error_count=block_result["failures"],
                )

        elapsed = time.time() - cycle_started
        sleep_seconds = max(0.0, float(args.sleep_seconds) - elapsed)
        if sleep_seconds:
            time.sleep(sleep_seconds)


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    with postgres_connection(PostgresSettings()) as conn:
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit = off")
        if not args.skip_schema:
            create_schema(conn)
        if args.refresh_metadata:
            refresh_market_token_metadata(conn)
        if args.refresh_eligibility:
            refresh_eligibility(conn, ClickHouseClient())
        if args.refresh_metadata or args.refresh_eligibility:
            backfill_missing_market_progress(conn, since_ts=args.since_ts, end_ts=args.end_ts or utc_now_ts())
            refresh_market_event_memberships(conn, since_ts=args.since_ts)

        meta = vars(args).copy()
        run_id = start_run(conn, source=args.source, mode="once", meta=meta)
        try:
            if args.source == "frontend":
                result = backfill_frontend_prices(
                    conn,
                    FrontendPriceClient(),
                    start_ts=args.start_ts,
                    end_ts=args.end_ts,
                    fidelity_minutes=args.fidelity_minutes,
                    limit=args.limit,
                )
            elif args.source == "orderfilled_block_close":
                result = backfill_block_close_prices(
                    conn,
                    ClickHouseClient(),
                    from_block=args.from_block,
                    to_block=args.to_block,
                    limit=args.limit,
                )
            elif args.source == API_TRADES_SOURCE:
                result = backfill_api_trades(
                    conn,
                    ClickHouseClient(),
                    start_ts=args.start_ts,
                    end_ts=args.end_ts,
                    market_slug=args.market_slug,
                    limit=args.limit,
                )
            else:
                raise ValueError(f"unsupported source: {args.source}")
            finish_run(
                conn,
                run_id=run_id,
                status="complete",
                rows_written=int(result.get("rows_written") or 0),
                markets_complete=int(result.get("tokens") or 0),
                error_count=int(result.get("failures") or 0),
            )
            result["run_id"] = run_id
            return result
        except Exception as exc:
            finish_run(conn, run_id=run_id, status="error", rows_written=0, markets_complete=0, error_count=1, last_error=repr(exc))
            conn.commit()
            raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a one-shot quant price build.")
    parser.add_argument("--source", choices=("frontend", "orderfilled_block_close", "api_trades"), default="frontend")
    parser.add_argument("--refresh-metadata", action="store_true")
    parser.add_argument("--refresh-eligibility", action="store_true")
    parser.add_argument("--skip-schema", action="store_true", help="Skip schema creation for one-shot jobs when tables already exist.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--start-ts", type=int)
    parser.add_argument("--end-ts", type=int)
    parser.add_argument("--market-slug")
    parser.add_argument("--fidelity-minutes", type=int, default=1)
    parser.add_argument("--from-block", type=int)
    parser.add_argument("--to-block", type=int)
    parser.add_argument("--daemon", action="store_true")
    parser.add_argument("--source-mode", choices=("all", "both", "frontend", "orderfilled_block_close", "api_trades", "maintenance"), default="both")
    parser.add_argument("--since-ts", type=int, default=SEPTEMBER_2025_TS)
    parser.add_argument("--created-before-ts", type=int)
    parser.add_argument("--token-limit", type=int, default=20)
    parser.add_argument("--frontend-token-limit", type=int)
    parser.add_argument("--block-token-limit", type=int)
    parser.add_argument("--sleep-seconds", type=float, default=30.0)
    parser.add_argument("--skip-maintenance", action="store_true")
    parser.add_argument("--maintenance-lookback-seconds", type=int, default=7200)
    parser.add_argument("--metadata-missing-limit", type=int, default=100000)
    parser.add_argument("--metadata-interval-seconds", type=float, default=3600.0)
    parser.add_argument("--event-membership-interval-seconds", type=float, default=3600.0)
    parser.add_argument("--eligibility-interval-seconds", type=float, default=900.0)
    parser.add_argument("--skip-orderfilled-market-stats-refresh", action="store_true")
    parser.add_argument("--orderfilled-market-stats-interval-seconds", type=float, default=3600.0)
    parser.add_argument("--frontend-min-orderfilled-trades", type=int, default=1000)
    parser.add_argument("--frontend-include-active-without-trades", action="store_true")
    parser.add_argument("--frontend-all-markets", action="store_true")
    parser.add_argument("--frontend-targets-only", action="store_true")
    parser.add_argument("--frontend-window-seconds", type=int, default=7 * 86400)
    parser.add_argument("--frontend-overlap-seconds", type=int, default=6 * 3600)
    parser.add_argument("--frontend-lag-seconds", type=int, default=120)
    parser.add_argument("--frontend-recent-days", type=int, default=30)
    parser.add_argument("--frontend-event-window-days", type=int, default=7)
    parser.add_argument("--frontend-backtest-min-orderfilled-trades", type=int, default=100)
    parser.add_argument("--frontend-defer-low-value", action="store_true")
    parser.add_argument("--frontend-crypto-window-seconds", type=int, default=6 * 3600)
    parser.add_argument("--frontend-crypto-after-seconds", type=int, default=2 * 3600)
    parser.add_argument("--frontend-request-pause-seconds", type=float, default=0.0)
    parser.add_argument("--api-trades-token-limit", type=int)
    parser.add_argument("--api-trades-window-seconds", type=int, default=3600)
    parser.add_argument("--api-trades-overlap-seconds", type=int, default=120)
    parser.add_argument("--api-trades-lag-seconds", type=int, default=120)
    parser.add_argument("--api-trades-min-orderfilled-trades", type=int, default=1)
    parser.add_argument("--api-trades-include-active-without-trades", action="store_true")
    parser.add_argument("--block-min-orderfilled-trades", type=int, default=1)
    parser.add_argument("--block-window", type=int, default=50000)
    parser.add_argument("--block-overlap", type=int, default=2000)
    parser.add_argument("--block-live-only", action="store_true")
    parser.add_argument("--block-live-lookback-days", type=int, default=7)
    parser.add_argument(
        "--repair-block-close-state",
        action="store_true",
        help="Reset orderfilled_block_close states marked complete even though no block close rows exist.",
    )
    parser.add_argument("--repair-limit", type=int, default=10000, help="Max broken block-close states to reset in one run.")
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    args.frontend_token_limit = args.frontend_token_limit or args.token_limit
    args.block_token_limit = args.block_token_limit or args.token_limit
    args.api_trades_token_limit = args.api_trades_token_limit or args.token_limit
    args.shard_count = max(1, int(args.shard_count))
    args.shard_index = max(0, min(int(args.shard_index), args.shard_count - 1))
    if args.repair_block_close_state:
        with postgres_connection(PostgresSettings()) as conn:
            repaired = repair_block_close_complete_without_rows(conn, limit=args.repair_limit)
        print({"repaired": repaired})
        return
    if args.daemon:
        run_daemon(args)
        return
    if args.source == "frontend" and (args.start_ts is None or args.end_ts is None):
        raise SystemExit("--start-ts and --end-ts are required for frontend")
    if args.source == "orderfilled_block_close" and (args.from_block is None or args.to_block is None):
        raise SystemExit("--from-block and --to-block are required for orderfilled sources")
    if args.source == API_TRADES_SOURCE and (args.start_ts is None or args.end_ts is None):
        raise SystemExit("--start-ts and --end-ts are required for api_trades")
    print(run_once(args))


if __name__ == "__main__":
    main()
