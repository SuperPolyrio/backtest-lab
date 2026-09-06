"""Backfill block-number based OrderFilled close prices."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any

from .block_close_algorithm import orderfilled_block_close_sql
from ..core.db import ClickHouseClient, PostgresSettings, postgres_connection
from ..core.metadata import refresh_market_token_metadata
from ..core.schema import create_schema
from .block_close_writer_fence import (
    BLOCK_CLOSE_CANONICAL_TABLE,
    acquire_block_close_writer_shared,
    validate_block_close_target_table,
)


SOURCE = "orderfilled_block_close"
BLOCK_CLOSE_ROW_SOURCE = "clean_orderfilled_fact"

BLOCK_CLOSE_COLUMNS = (
    "token_id", "market_id", "market_slug", "token_side", "block_number",
    "block_timestamp", "open_price", "high_price", "low_price",
    "close_price", "yes_probability_close", "vwap_price", "yes_probability_vwap",
    "close_raw_price", "first_tx_hash", "close_price_source", "close_tx_hash",
    "last_tx_hash", "first_log_index", "last_log_index", "close_log_index",
    "close_maker_amount", "close_taker_amount", "trade_count", "raw_trade_count",
    "internal_filtered_count", "invalid_size_count", "invalid_price_count",
    "amount_ratio_count", "raw_price_fallback_count", "extreme_trade_count",
    "anomaly_flags", "source", "volume", "buy_volume", "sell_volume",
)


@dataclass(frozen=True, slots=True)
class PreparedBlockCloseRows:
    source_rows: int
    values: tuple[tuple[Any, ...], ...]
    unknown_token_ids: tuple[str, ...]
    checksum: str


@dataclass(frozen=True, slots=True)
class BlockCloseWriteResult:
    source_rows: int
    mapped_rows: int
    inserted_rows: int
    identical_conflicts: int
    checksum: str


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
    return None


def _anomaly_flags(row: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if int(row.get("internal_filtered_count") or 0) > 0:
        flags.append("internal_counterparty_filtered")
    if int(row.get("invalid_size_count") or 0) > 0:
        flags.append("invalid_size_filtered")
    if int(row.get("invalid_price_count") or 0) > 0:
        flags.append("invalid_price_filtered")
    if int(row.get("extreme_trade_count") or 0) > 0:
        flags.append("extreme_price_trade_present")
    if int(row.get("raw_price_fallback_count") or 0) > 0:
        flags.append("raw_price_fallback_used")
    return flags


def _checksum_json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return normalized.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def block_close_values_checksum(values: tuple[tuple[Any, ...], ...]) -> str:
    rows = [
        [_checksum_json_value(value) for value in row]
        for row in sorted(values, key=lambda item: (str(item[0]), int(item[4])))
    ]
    payload = json.dumps(rows, sort_keys=False, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def prepare_block_close_rows(
    metadata_by_token: dict[str, dict[str, Any]],
    rows: list[dict[str, Any]],
) -> PreparedBlockCloseRows:
    values: list[tuple[Any, ...]] = []
    unknown_token_ids: set[str] = set()
    seen_keys: set[tuple[str, int]] = set()
    for row in rows:
        clickhouse_token_id = str(row.get("token_id") or "").lower()
        meta = metadata_by_token.get(clickhouse_token_id)
        if not meta:
            unknown_token_ids.add(clickhouse_token_id or "<missing>")
            continue
        token_id = str(meta["token_id"])
        block_number = int(row["block_number"])
        key = (token_id, block_number)
        if key in seen_keys:
            raise ValueError(
                "duplicate normalized block-close key in one source chunk: "
                f"token_id={token_id!r}, block_number={block_number}"
            )
        seen_keys.add(key)
        open_price = _decimal_or_none(row.get("open_price"))
        high_price = _decimal_or_none(row.get("high_price"))
        low_price = _decimal_or_none(row.get("low_price"))
        close_price = _decimal_or_none(row.get("close_price"))
        if close_price is None:
            raise ValueError(
                f"close_price is required for token_id={token_id!r}, block_number={block_number}"
            )
        vwap_price = _decimal_or_none(row.get("vwap_price"))
        close_raw_price = _decimal_or_none(row.get("close_raw_price"))
        close_maker_amount = _decimal_or_none(row.get("close_maker_amount"))
        close_taker_amount = _decimal_or_none(row.get("close_taker_amount"))
        token_side = meta.get("token_side")
        values.append(
            (
                token_id,
                int(meta["market_id"]),
                meta.get("market_slug"),
                token_side,
                block_number,
                _timestamp_or_none(row.get("block_timestamp")),
                open_price,
                high_price,
                low_price,
                close_price,
                _yes_probability(str(token_side), close_price),
                vwap_price,
                _yes_probability(str(token_side), vwap_price),
                close_raw_price,
                row.get("first_tx_hash"),
                row.get("close_price_source") or "unknown",
                row.get("close_tx_hash"),
                row.get("last_tx_hash") or row.get("close_tx_hash"),
                int(row["first_log_index"]) if row.get("first_log_index") is not None else None,
                int(row["last_log_index"]) if row.get("last_log_index") is not None else None,
                int(row["close_log_index"]) if row.get("close_log_index") is not None else None,
                close_maker_amount,
                close_taker_amount,
                int(row.get("clean_trade_count") or 0),
                int(row.get("raw_trade_count") or 0),
                int(row.get("internal_filtered_count") or 0),
                int(row.get("invalid_size_count") or 0),
                int(row.get("invalid_price_count") or 0),
                int(row.get("amount_ratio_count") or 0),
                int(row.get("raw_price_fallback_count") or 0),
                int(row.get("extreme_trade_count") or 0),
                json.dumps(_anomaly_flags(row), sort_keys=True),
                BLOCK_CLOSE_ROW_SOURCE,
                _decimal_or_none(row.get("volume")) or Decimal("0"),
                _decimal_or_none(row.get("buy_volume")) or Decimal("0"),
                _decimal_or_none(row.get("sell_volume")) or Decimal("0"),
            )
        )
    frozen_values = tuple(values)
    return PreparedBlockCloseRows(
        source_rows=len(rows),
        values=frozen_values,
        unknown_token_ids=tuple(sorted(unknown_token_ids)),
        checksum=block_close_values_checksum(frozen_values),
    )


def fetch_eligible_block_tokens(conn: Any, *, limit: int | None = None) -> dict[str, dict[str, Any]]:
    params: list[Any] = []
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT %s"
        params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT m.token_id, m.token_id_hex, m.market_id, m.market_slug, m.token_side
            FROM quant.market_token_metadata m
            JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            WHERE e.eligible = TRUE
              AND m.token_id_hex IS NOT NULL
            ORDER BY m.market_id ASC, m.outcome_index ASC, m.token_id ASC
            {limit_sql}
            """,
            params,
        )
        return {str(row["token_id_hex"]).lower(): dict(row) for row in cur.fetchall()}


def _read_count_row(row: Any, key: str) -> int:
    if row is None:
        return 0
    if isinstance(row, dict):
        return int(row.get(key) or 0)
    return int(row[0] or 0)


def insert_block_close_rows_detailed(
    conn: Any,
    metadata_by_token: dict[str, dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    target_table: str,
    fail_on_unknown: bool = True,
) -> BlockCloseWriteResult:
    """Insert an immutable rebuild chunk and audit every conflict.

    The insert and the caller's chunk checkpoint must share the same database
    transaction. Any non-identical conflict rolls this savepoint back before
    raising, leaving no partially accepted chunk rows behind.
    """

    target_table = validate_block_close_target_table(target_table)
    prepared = prepare_block_close_rows(metadata_by_token, rows)
    if prepared.unknown_token_ids and fail_on_unknown:
        raise ValueError(
            "source chunk contains token ids outside the frozen manifest: "
            + ", ".join(prepared.unknown_token_ids[:20])
        )
    if not prepared.values:
        return BlockCloseWriteResult(
            source_rows=prepared.source_rows,
            mapped_rows=0,
            inserted_rows=0,
            identical_conflicts=0,
            checksum=prepared.checksum,
        )

    acquire_block_close_writer_shared(conn)
    column_sql = ", ".join(BLOCK_CLOSE_COLUMNS)
    target_row = ", ".join(f"target.{column}" for column in BLOCK_CLOSE_COLUMNS)
    incoming_row = ", ".join(f"incoming.{column}" for column in BLOCK_CLOSE_COLUMNS)
    with conn.cursor() as cur:
        cur.execute("SAVEPOINT block_close_chunk_write")
        try:
            cur.execute("DROP TABLE IF EXISTS pg_temp.tmp_market_token_block_close")
            cur.execute(
                f"""
                CREATE TEMP TABLE tmp_market_token_block_close
                ON COMMIT DROP
                AS SELECT {column_sql}
                FROM {target_table}
                WHERE FALSE
                """
            )
            with cur.copy(
                f"COPY tmp_market_token_block_close ({column_sql}) FROM STDIN"
            ) as copy:
                for value in prepared.values:
                    copy.write_row(value)
            cur.execute(
                f"""
                WITH inserted AS (
                    INSERT INTO {target_table} ({column_sql})
                    SELECT {column_sql}
                    FROM tmp_market_token_block_close
                    ON CONFLICT (token_id, block_number) DO NOTHING
                    RETURNING 1
                )
                SELECT COUNT(*) AS inserted_rows FROM inserted
                """
            )
            inserted_rows = _read_count_row(cur.fetchone(), "inserted_rows")
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS matched_rows,
                    COUNT(*) FILTER (
                        WHERE ROW({target_row}) IS DISTINCT FROM ROW({incoming_row})
                    ) AS nonidentical_rows
                FROM tmp_market_token_block_close incoming
                JOIN {target_table} target
                  ON target.token_id = incoming.token_id
                 AND target.block_number = incoming.block_number
                """
            )
            comparison = cur.fetchone()
            if isinstance(comparison, dict):
                matched_rows = int(comparison.get("matched_rows") or 0)
                nonidentical_rows = int(comparison.get("nonidentical_rows") or 0)
            else:
                matched_rows = int(comparison[0] or 0)
                nonidentical_rows = int(comparison[1] or 0)
            if matched_rows != len(prepared.values):
                raise RuntimeError(
                    "shadow insert parity failed: "
                    f"mapped={len(prepared.values)}, matched={matched_rows}"
                )
            if nonidentical_rows:
                raise RuntimeError(
                    "non-identical block-close conflict detected: "
                    f"rows={nonidentical_rows}"
                )
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT block_close_chunk_write")
            cur.execute("RELEASE SAVEPOINT block_close_chunk_write")
            raise
        cur.execute("RELEASE SAVEPOINT block_close_chunk_write")

    return BlockCloseWriteResult(
        source_rows=prepared.source_rows,
        mapped_rows=len(prepared.values),
        inserted_rows=inserted_rows,
        identical_conflicts=len(prepared.values) - inserted_rows,
        checksum=prepared.checksum,
    )


def insert_block_close_rows(
    conn: Any,
    metadata_by_token: dict[str, dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    target_table: str = BLOCK_CLOSE_CANONICAL_TABLE,
) -> int:
    """Legacy upsert API for the canonical tape, with a fenced safe target.

    Rebuild shadows are immutable: targeting one automatically switches to the
    detailed insert/conflict path and returns only newly inserted rows.
    """

    target_table = validate_block_close_target_table(target_table)
    if target_table != BLOCK_CLOSE_CANONICAL_TABLE:
        result = insert_block_close_rows_detailed(
            conn,
            metadata_by_token,
            rows,
            target_table=target_table,
            fail_on_unknown=True,
        )
        return result.inserted_rows

    prepared = prepare_block_close_rows(metadata_by_token, rows)
    if not prepared.values:
        return 0
    acquire_block_close_writer_shared(conn)
    column_sql = ", ".join(BLOCK_CLOSE_COLUMNS)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS pg_temp.tmp_market_token_block_close")
        cur.execute(
            f"""
            CREATE TEMP TABLE tmp_market_token_block_close
            ON COMMIT DROP
            AS SELECT {column_sql}
            FROM {target_table}
            WHERE FALSE
            """
        )
        with cur.copy(
            f"COPY tmp_market_token_block_close ({column_sql}) FROM STDIN"
        ) as copy:
            for value in prepared.values:
                copy.write_row(value)
        cur.execute(
            f"""
            INSERT INTO {target_table} ({column_sql})
            SELECT {column_sql}
            FROM tmp_market_token_block_close
            ON CONFLICT (token_id, block_number) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                block_timestamp = EXCLUDED.block_timestamp,
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                yes_probability_close = EXCLUDED.yes_probability_close,
                vwap_price = EXCLUDED.vwap_price,
                yes_probability_vwap = EXCLUDED.yes_probability_vwap,
                close_raw_price = EXCLUDED.close_raw_price,
                first_tx_hash = EXCLUDED.first_tx_hash,
                close_price_source = EXCLUDED.close_price_source,
                close_tx_hash = EXCLUDED.close_tx_hash,
                last_tx_hash = EXCLUDED.last_tx_hash,
                first_log_index = EXCLUDED.first_log_index,
                last_log_index = EXCLUDED.last_log_index,
                close_log_index = EXCLUDED.close_log_index,
                close_maker_amount = EXCLUDED.close_maker_amount,
                close_taker_amount = EXCLUDED.close_taker_amount,
                trade_count = EXCLUDED.trade_count,
                raw_trade_count = EXCLUDED.raw_trade_count,
                internal_filtered_count = EXCLUDED.internal_filtered_count,
                invalid_size_count = EXCLUDED.invalid_size_count,
                invalid_price_count = EXCLUDED.invalid_price_count,
                amount_ratio_count = EXCLUDED.amount_ratio_count,
                raw_price_fallback_count = EXCLUDED.raw_price_fallback_count,
                extreme_trade_count = EXCLUDED.extreme_trade_count,
                anomaly_flags = EXCLUDED.anomaly_flags,
                source = EXCLUDED.source,
                volume = EXCLUDED.volume,
                buy_volume = EXCLUDED.buy_volume,
                sell_volume = EXCLUDED.sell_volume,
                built_at = now()
            """
        )
    return len(prepared.values)


def count_block_close_rows_by_token(
    conn: Any,
    metadata_by_token: dict[str, dict[str, Any]],
    *,
    from_block: int,
    to_block: int,
) -> dict[str, int]:
    if not metadata_by_token:
        return {}
    token_ids = [str(meta["token_id"]) for meta in metadata_by_token.values()]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT token_id, COUNT(*) AS rows
            FROM quant.market_token_block_close
            WHERE token_id = ANY(%s::text[])
              AND block_number >= %s
              AND block_number <= %s
            GROUP BY token_id
            """,
            (token_ids, int(from_block), int(to_block)),
        )
        return {str(row["token_id"]): int(row["rows"] or 0) for row in cur.fetchall()}


def backfill_block_close_prices(
    conn: Any,
    ch: ClickHouseClient,
    *,
    from_block: int,
    to_block: int,
    limit: int | None = None,
) -> dict[str, int]:
    metadata_by_token = fetch_eligible_block_tokens(conn, limit=limit)
    if not metadata_by_token:
        return {"tokens": 0, "rows_written": 0}
    sql = orderfilled_block_close_sql(
        table=ch.settings.orderfilled_table,
        from_block=from_block,
        to_block=to_block,
        token_ids=metadata_by_token.keys(),
    )
    rows = ch.query_json_rows(sql)
    rows_written = insert_block_close_rows(conn, metadata_by_token, rows)
    covered_rows_by_token = count_block_close_rows_by_token(
        conn,
        metadata_by_token,
        from_block=from_block,
        to_block=to_block,
    )
    completed_metadata = [
        meta
        for meta in metadata_by_token.values()
        if int(covered_rows_by_token.get(str(meta["token_id"]), 0)) > 0
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO quant.market_price_build_market_state (
                source, token_id, market_id, market_slug, token_side,
                status, last_complete_block, attempt_count, updated_at
            ) VALUES (%s, %s, %s, %s, %s, 'complete', %s, 1, now())
            ON CONFLICT (source, token_id) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                status = EXCLUDED.status,
                last_complete_block = GREATEST(
                    COALESCE(quant.market_price_build_market_state.last_complete_block, 0),
                    EXCLUDED.last_complete_block
                ),
                attempt_count = quant.market_price_build_market_state.attempt_count + 1,
                last_error = NULL,
                updated_at = now()
            """,
            [
                (
                    SOURCE,
                    meta["token_id"],
                    meta["market_id"],
                    meta.get("market_slug"),
                    meta["token_side"],
                    int(to_block),
                )
                for meta in completed_metadata
            ],
        )
    return {
        "tokens": len(metadata_by_token),
        "completed_tokens": len(completed_metadata),
        "tokens_without_rows": len(metadata_by_token) - len(completed_metadata),
        "rows_written": rows_written,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill quant OrderFilled block close prices once.")
    parser.add_argument("--from-block", type=int, required=True)
    parser.add_argument("--to-block", type=int, required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--refresh-metadata", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    with postgres_connection(PostgresSettings()) as conn:
        create_schema(conn)
        if args.refresh_metadata:
            refresh_market_token_metadata(conn)
        result = backfill_block_close_prices(
            conn,
            ClickHouseClient(),
            from_block=args.from_block,
            to_block=args.to_block,
            limit=args.limit,
        )
    print(result)


if __name__ == "__main__":
    main()
