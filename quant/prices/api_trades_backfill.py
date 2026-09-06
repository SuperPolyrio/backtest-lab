"""Backfill Polymarket data-api trades into timestamp-axis price rows."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .api_trades_algorithm import api_trades_sql
from ..core.db import ClickHouseClient, PostgresSettings, postgres_connection
from ..core.metadata import refresh_market_token_metadata


SOURCE = "api_trades"


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NULL", "\\N", "NONE", "NAN"}:
        return None
    return Decimal(text)


def _timestamp_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _yes_probability(token_side: str | None, token_price: Decimal | None) -> Decimal | None:
    if token_price is None:
        return None
    side = str(token_side or "").upper()
    if side == "YES":
        return token_price
    if side == "NO":
        return Decimal("1") - token_price
    return token_price


def fetch_api_trade_tokens(
    conn: Any,
    *,
    market_slug: str | None = None,
    limit: int | None = None,
) -> dict[str, dict[str, Any]]:
    params: list[Any] = []
    filters = ["m.token_id IS NOT NULL"]
    if market_slug:
        filters.append("m.market_slug = %s")
        params.append(str(market_slug))
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT %s"
        params.append(int(limit))
    where_sql = " AND ".join(filters)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT m.token_id, m.market_id, m.market_slug, m.token_side
            FROM quant.market_token_metadata m
            WHERE {where_sql}
            ORDER BY m.market_id ASC, m.outcome_index ASC, m.token_id ASC
            {limit_sql}
            """,
            params,
        )
        return {str(row["token_id"]): dict(row) for row in cur.fetchall()}


def _trade_insert_value(meta: dict[str, Any], row: dict[str, Any]) -> tuple[Any, ...] | None:
    price = _decimal_or_none(row.get("price"))
    trade_time = _timestamp_or_none(row.get("trade_time"))
    if price is None or trade_time is None:
        return None
    token_side = str(meta["token_side"])
    timestamp = int(row.get("timestamp") or trade_time.timestamp())
    return (
        str(meta["token_id"]),
        int(meta["market_id"]),
        meta.get("market_slug") or row.get("market_slug") or row.get("slug"),
        token_side,
        str(row["trade_key"]),
        row.get("transaction_hash"),
        timestamp,
        trade_time,
        price,
        _yes_probability(token_side, price),
        _decimal_or_none(row.get("size")) or Decimal("0"),
        _decimal_or_none(row.get("notional")) or Decimal("0"),
        row.get("side"),
        row.get("proxy_wallet"),
        row.get("condition_id"),
        row.get("market_filter_condition_id"),
        row.get("outcome"),
        int(row["outcome_index"]) if row.get("outcome_index") is not None else None,
        row.get("source_endpoint"),
        bool(row.get("taker_only")),
    )


def insert_api_trade_rows(conn: Any, metadata_by_token: dict[str, dict[str, Any]], rows: list[dict[str, Any]]) -> int:
    values = []
    for row in rows:
        token_id = str(row.get("token_id") or "").strip()
        meta = metadata_by_token.get(token_id)
        if not meta:
            continue
        value = _trade_insert_value(meta, row)
        if value is not None:
            values.append(value)
    if not values:
        return 0
    columns = (
        "token_id", "market_id", "market_slug", "token_side",
        "trade_key", "transaction_hash", "timestamp", "trade_time",
        "price", "yes_probability", "size", "notional",
        "side", "proxy_wallet", "condition_id", "market_filter_condition_id",
        "outcome", "outcome_index", "source_endpoint", "taker_only",
    )
    column_sql = ", ".join(columns)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TEMP TABLE tmp_market_token_api_trades
            ON COMMIT DROP
            AS SELECT {column_sql}
            FROM quant.market_token_api_trades
            WHERE FALSE
            """
        )
        with cur.copy(f"COPY tmp_market_token_api_trades ({column_sql}) FROM STDIN") as copy:
            for value in values:
                copy.write_row(value)
        cur.execute(
            f"""
            INSERT INTO quant.market_token_api_trades ({column_sql})
            SELECT {column_sql}
            FROM tmp_market_token_api_trades
            ON CONFLICT (token_id, trade_key) DO NOTHING
            """
        )
    return len(values)


def backfill_api_trades(
    conn: Any,
    ch: ClickHouseClient,
    *,
    start_ts: int,
    end_ts: int,
    market_slug: str | None = None,
    limit: int | None = None,
) -> dict[str, int]:
    metadata_by_token = fetch_api_trade_tokens(conn, market_slug=market_slug, limit=limit)
    if not metadata_by_token:
        return {"tokens": 0, "rows_written": 0}
    sql = api_trades_sql(
        table="pg_api_trades_main",
        start_ts=start_ts,
        end_ts=end_ts,
        market_ids={int(meta["market_id"]) for meta in metadata_by_token.values()},
        token_ids=metadata_by_token.keys(),
    )
    rows = ch.query_json_rows(sql)
    rows_written = insert_api_trade_rows(conn, metadata_by_token, rows)
    return {"tokens": len(metadata_by_token), "rows_written": rows_written}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill timestamp-axis Polymarket data-api trades.")
    parser.add_argument("--start-ts", type=int, required=True)
    parser.add_argument("--end-ts", type=int, required=True)
    parser.add_argument("--market-slug")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--refresh-metadata", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    with postgres_connection(PostgresSettings()) as conn:
        if args.refresh_metadata:
            refresh_market_token_metadata(conn)
        result = backfill_api_trades(
            conn,
            ClickHouseClient(),
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            market_slug=args.market_slug,
            limit=args.limit,
        )
    print(result)


if __name__ == "__main__":
    main()
