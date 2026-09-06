#!/usr/bin/env python3
"""Sync materialized ClickHouse OrderFilled block-close rows into Postgres.

This is a targeted repair tool for quant research UI/API data.  ClickHouse is
the historical source of truth for `market_token_block_close`; Postgres is the
serving store used by the current quant API.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.core.db import ClickHouseClient, postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402
from quant.backtest.materialized_execution_summary import (  # noqa: E402
    refresh_execution_summaries_for_tokens,
)
from quant.prices.block_close_algorithm import quote_clickhouse_string  # noqa: E402
from quant.prices.block_close_writer_fence import (  # noqa: E402
    BLOCK_CLOSE_CANONICAL_TABLE,
    acquire_block_close_writer_shared,
)


DEFAULT_BLOCK_WINDOW = 50_000
DEFAULT_INSERT_BATCH_SIZE = 25_000
SOURCE = "clickhouse_market_token_block_close"


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NULL", "\\N", "NONE", "NAN"}:
        return None
    return Decimal(text)


def timestamp_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NULL", "\\N", "NONE"}:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def yes_probability(token_side: str | None, token_price: Decimal | None) -> Decimal | None:
    if token_price is None:
        return None
    side = str(token_side or "").upper()
    if side == "YES":
        return token_price
    if side == "NO":
        return Decimal("1") - token_price
    return None


def normalize_hex_token(value: str) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    return text


def fetch_tokens(
    *,
    event_slug: str | None,
    market_slug: str | None,
    market_id: int | None,
    token_ids: list[str],
) -> list[dict[str, Any]]:
    filters = ["m.token_id IS NOT NULL", "m.token_id_hex IS NOT NULL"]
    params: list[Any] = []
    join_event = ""
    if event_slug:
        join_event = "JOIN quant.market_event_members e ON e.market_id = m.market_id"
        filters.append("e.event_slug = %s")
        params.append(event_slug)
    if market_slug:
        filters.append("m.market_slug = %s")
        params.append(market_slug)
    if market_id is not None:
        filters.append("m.market_id = %s")
        params.append(int(market_id))
    if token_ids:
        filters.append("m.token_id = ANY(%s::text[])")
        params.append([str(token_id) for token_id in token_ids])

    with postgres_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT
                    m.token_id,
                    lower(m.token_id_hex) AS token_id_hex,
                    m.market_id,
                    m.market_slug,
                    upper(m.token_side) AS token_side
                FROM quant.market_token_metadata m
                {join_event}
                WHERE {" AND ".join(filters)}
                ORDER BY market_id, token_side, token_id
                """,
                params,
            )
            return [dict(row) for row in cur.fetchall()]


def clickhouse_coverage(ch: ClickHouseClient, token_hex_ids: Iterable[str]) -> dict[str, Any]:
    token_list = sorted({normalize_hex_token(token) for token in token_hex_ids if str(token or "").strip()})
    if not token_list:
        return {"rows": 0, "tokens": 0, "min_block": None, "max_block": None}
    quoted = ",".join(quote_clickhouse_string(token) for token in token_list)
    rows = ch.query_json_rows(
        f"""
        SELECT
            count() AS rows,
            uniqExact(token_id) AS tokens,
            min(block_number) AS min_block,
            max(block_number) AS max_block
        FROM market_token_block_close
        WHERE token_id IN ({quoted})
        """,
        timeout_seconds=300,
    )
    return rows[0] if rows else {"rows": 0, "tokens": 0, "min_block": None, "max_block": None}


def fetch_clickhouse_rows(
    ch: ClickHouseClient,
    *,
    token_hex_ids: Iterable[str],
    from_block: int,
    to_block: int,
) -> list[dict[str, Any]]:
    token_list = sorted({normalize_hex_token(token) for token in token_hex_ids if str(token or "").strip()})
    if not token_list:
        return []
    quoted = ",".join(quote_clickhouse_string(token) for token in token_list)
    return ch.query_json_rows(
        f"""
        SELECT
            c.token_id,
            c.market_id,
            c.block_number,
            formatDateTime(bt.block_time, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS block_timestamp,
            toString(c.close_price) AS close_price,
            toString(c.vwap_price) AS vwap_price,
            toString(c.raw_price_close) AS close_raw_price,
            c.last_tx_hash AS close_tx_hash,
            c.last_log_index AS close_log_index,
            c.trade_count AS trade_count,
            c.volume_shares AS volume
        FROM market_token_block_close c
        LEFT JOIN block_timestamps bt ON bt.block_number = c.block_number
        WHERE c.token_id IN ({quoted})
          AND c.block_number >= {int(from_block)}
          AND c.block_number <= {int(to_block)}
        ORDER BY c.token_id ASC, c.block_number ASC
        """,
        timeout_seconds=600,
    )


def make_insert_value(token: dict[str, Any], row: dict[str, Any]) -> tuple[Any, ...]:
    close_price = decimal_or_none(row.get("close_price"))
    vwap_price = decimal_or_none(row.get("vwap_price"))
    token_side = str(token["token_side"]).upper()
    yes_close = yes_probability(token_side, close_price)
    yes_vwap = yes_probability(token_side, vwap_price)
    trade_count = int(row.get("trade_count") or 0)
    volume = decimal_or_none(row.get("volume")) or Decimal("0")
    return (
        str(token["token_id"]),
        int(token["market_id"]),
        token.get("market_slug"),
        token_side,
        int(row["block_number"]),
        timestamp_or_none(row.get("block_timestamp")),
        close_price,
        yes_close,
        vwap_price,
        yes_vwap,
        decimal_or_none(row.get("close_raw_price")),
        SOURCE,
        row.get("close_tx_hash") or None,
        int(row["close_log_index"]) if row.get("close_log_index") not in (None, "") else None,
        trade_count,
        trade_count,
        volume,
        SOURCE,
        json.dumps([], sort_keys=True),
    )


def write_postgres_rows(
    rows: list[dict[str, Any]],
    *,
    tokens_by_hex: dict[str, dict[str, Any]],
    batch_size: int,
    update_existing_timestamps: bool,
) -> int:
    if not rows:
        return 0
    values = []
    for row in rows:
        token = tokens_by_hex.get(normalize_hex_token(str(row.get("token_id") or "")))
        if token:
            values.append(make_insert_value(token, row))
    if not values:
        return 0

    columns = (
        "token_id",
        "market_id",
        "market_slug",
        "token_side",
        "block_number",
        "block_timestamp",
        "close_price",
        "yes_probability_close",
        "vwap_price",
        "yes_probability_vwap",
        "close_raw_price",
        "close_price_source",
        "close_tx_hash",
        "close_log_index",
        "trade_count",
        "raw_trade_count",
        "volume",
        "source",
        "anomaly_flags",
    )
    column_sql = ", ".join(columns)
    if update_existing_timestamps:
        conflict_sql = """
        ON CONFLICT (token_id, block_number) DO UPDATE SET
            block_timestamp = COALESCE(
                quant.market_token_block_close.block_timestamp,
                EXCLUDED.block_timestamp
            )
        WHERE (
            quant.market_token_block_close.block_timestamp IS NULL
            AND EXCLUDED.block_timestamp IS NOT NULL
        )
        """
    else:
        conflict_sql = "ON CONFLICT (token_id, block_number) DO NOTHING"

    affected = 0
    with postgres_connection() as conn:
        for start in range(0, len(values), int(batch_size)):
            batch = values[start : start + int(batch_size)]
            acquire_block_close_writer_shared(conn)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TEMP TABLE tmp_sync_market_token_block_close
                    ON COMMIT DROP
                    AS SELECT {column_sql}
                    FROM {BLOCK_CLOSE_CANONICAL_TABLE}
                    WHERE FALSE
                    """
                )
                with cur.copy(f"COPY tmp_sync_market_token_block_close ({column_sql}) FROM STDIN") as copy:
                    for value in batch:
                        copy.write_row(value)
                cur.execute(
                    f"""
                    INSERT INTO {BLOCK_CLOSE_CANONICAL_TABLE} ({column_sql})
                    SELECT {column_sql}
                    FROM tmp_sync_market_token_block_close
                    {conflict_sql}
                    """
                )
                affected += int(cur.rowcount or 0)
            conn.commit()
    return affected


def update_build_state(tokens: list[dict[str, Any]], *, complete_block: int) -> None:
    if not tokens:
        return
    with postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO quant.market_price_build_market_state (
                    source, token_id, market_id, market_slug, token_side,
                    status, last_complete_block, attempt_count, last_error, updated_at
                ) VALUES (
                    'orderfilled_block_close', %s, %s, %s, %s,
                    'complete', %s, 1, NULL, now()
                )
                ON CONFLICT (source, token_id) DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    market_slug = EXCLUDED.market_slug,
                    token_side = EXCLUDED.token_side,
                    status = EXCLUDED.status,
                    last_complete_block = GREATEST(
                        COALESCE(quant.market_price_build_market_state.last_complete_block, 0),
                        EXCLUDED.last_complete_block
                    ),
                    last_error = NULL,
                    updated_at = now()
                """,
                [
                    (
                        str(token["token_id"]),
                        int(token["market_id"]),
                        token.get("market_slug"),
                        str(token["token_side"]).upper(),
                        int(complete_block),
                    )
                    for token in tokens
                ],
            )


def sync_block_windows(
    ch: ClickHouseClient,
    *,
    tokens_by_hex: dict[str, dict[str, Any]],
    from_block: int,
    to_block: int,
    block_window: int,
    insert_batch_size: int,
    max_windows: int | None,
    update_existing_timestamps: bool,
) -> dict[str, int | None]:
    """Sync bounded windows and return only successfully processed highwater."""

    affected_total = 0
    windows_run = 0
    completed_through_block: int | None = None
    current = int(from_block)
    while current <= int(to_block):
        if max_windows is not None and windows_run >= int(max_windows):
            break
        window_to = min(current + int(block_window) - 1, int(to_block))
        rows = fetch_clickhouse_rows(
            ch,
            token_hex_ids=tokens_by_hex.keys(),
            from_block=current,
            to_block=window_to,
        )
        affected = write_postgres_rows(
            rows,
            tokens_by_hex=tokens_by_hex,
            batch_size=insert_batch_size,
            update_existing_timestamps=update_existing_timestamps,
        )
        affected_total += affected
        windows_run += 1
        completed_through_block = window_to
        print(
            {
                "window": [current, window_to],
                "clickhouse_rows": len(rows),
                "postgres_rows_inserted_or_timestamp_updated": affected,
            },
            flush=True,
        )
        current = window_to + 1
    return {
        "postgres_rows_inserted_or_timestamp_updated": affected_total,
        "windows_run": windows_run,
        "completed_through_block": completed_through_block,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync ClickHouse materialized OrderFilled block-close rows into Postgres.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--event-slug")
    target.add_argument("--market-slug")
    target.add_argument("--market-id", type=int)
    target.add_argument("--token-id", action="append", default=[])
    parser.add_argument("--from-block", type=int)
    parser.add_argument("--to-block", type=int)
    parser.add_argument("--block-window", type=int, default=DEFAULT_BLOCK_WINDOW)
    parser.add_argument("--insert-batch-size", type=int, default=DEFAULT_INSERT_BATCH_SIZE)
    parser.add_argument("--max-windows", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--no-update-existing-timestamps",
        action="store_true",
        help="Only insert missing rows; do not fill block_timestamp on existing rows.",
    )
    parser.add_argument("--no-update-build-state", action="store_true")
    parser.add_argument(
        "--no-refresh-execution-summary",
        action="store_true",
        help="Do not refresh token/market/event materialized execution summaries after syncing block-close rows.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.block_window <= 0:
        raise SystemExit("--block-window must be positive")
    if args.insert_batch_size <= 0:
        raise SystemExit("--insert-batch-size must be positive")
    if args.max_windows is not None and args.max_windows <= 0:
        raise SystemExit("--max-windows must be positive when provided")
    tokens = fetch_tokens(
        event_slug=args.event_slug,
        market_slug=args.market_slug,
        market_id=args.market_id,
        token_ids=args.token_id,
    )
    if not tokens:
        raise SystemExit("No matching tokens found in quant.market_token_metadata.")
    tokens_by_hex = {normalize_hex_token(str(token["token_id_hex"])): token for token in tokens}
    ch = ClickHouseClient()
    coverage = clickhouse_coverage(ch, tokens_by_hex.keys())
    if not coverage.get("rows"):
        print({"tokens": len(tokens), "clickhouse_rows": 0, "status": "no_clickhouse_rows"}, flush=True)
        return

    min_block = int(args.from_block or coverage["min_block"])
    max_block = int(args.to_block or coverage["max_block"])
    if min_block > max_block:
        raise SystemExit("--from-block must be less than or equal to --to-block")
    print(
        {
            "tokens": len(tokens),
            "clickhouse_coverage": coverage,
            "sync_window": [min_block, max_block],
            "dry_run": bool(args.dry_run),
            "update_existing_timestamps": not bool(args.no_update_existing_timestamps),
        },
        flush=True,
    )
    if args.dry_run:
        return

    sync_result = sync_block_windows(
        ch,
        tokens_by_hex=tokens_by_hex,
        from_block=min_block,
        to_block=max_block,
        block_window=args.block_window,
        insert_batch_size=args.insert_batch_size,
        max_windows=args.max_windows,
        update_existing_timestamps=not bool(args.no_update_existing_timestamps),
    )
    affected_total = int(sync_result["postgres_rows_inserted_or_timestamp_updated"] or 0)
    windows_run = int(sync_result["windows_run"] or 0)
    completed_through_block = sync_result["completed_through_block"]

    if not args.no_update_build_state and completed_through_block is not None:
        update_build_state(tokens, complete_block=completed_through_block)
    summary_refresh: dict[str, Any] | None = None
    if not args.no_refresh_execution_summary:
        with postgres_connection() as conn:
            create_schema(conn)
            summary_refresh = refresh_execution_summaries_for_tokens(conn, [str(token["token_id"]) for token in tokens])
            conn.commit()
    fully_complete = completed_through_block == max_block
    print(
        {
            "status": "complete" if fully_complete else "partial",
            "windows_run": windows_run,
            "postgres_rows_inserted_or_timestamp_updated": affected_total,
            "requested_to_block": max_block,
            "last_complete_block": completed_through_block,
            "build_state_updated": bool(
                not args.no_update_build_state and completed_through_block is not None
            ),
            "execution_summary_refresh": summary_refresh or {"status": "skipped"},
        },
        flush=True,
    )


if __name__ == "__main__":
    main()
