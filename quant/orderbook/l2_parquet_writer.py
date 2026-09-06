"""Short-lived DuckDB worker for one atomic L2 Parquet part."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path

import duckdb


SORT_KEY = "market, asset_id, timestamp_received, collector_seq, sequence_in_message"

# Keep raw JSON ingestion deterministic. DuckDB's sampling can otherwise infer a
# numeric hash as INT128 and fail later in the same file when a 0x-prefixed hash
# arrives.
JSON_COLUMNS = {
    "timestamp_received": "VARCHAR",
    "timestamp_normalized": "VARCHAR",
    "timestamp": "VARCHAR",
    "raw_event_timestamp": "VARCHAR",
    "clock_invalid_reason": "VARCHAR",
    "rest_request_start_ns": "VARCHAR",
    "rest_response_end_ns": "VARCHAR",
    "market": "VARCHAR",
    "event_type": "VARCHAR",
    "asset_id": "VARCHAR",
    "collector_seq": "VARCHAR",
    "sequence_in_message": "VARCHAR",
    "bids": "VARCHAR",
    "asks": "VARCHAR",
    "price": "VARCHAR",
    "size": "VARCHAR",
    "side": "VARCHAR",
    "best_bid": "VARCHAR",
    "best_ask": "VARCHAR",
    "fee_rate_bps": "VARCHAR",
    "transaction_hash": "VARCHAR",
    "old_tick_size": "VARCHAR",
    "new_tick_size": "VARCHAR",
    "book_hash": "VARCHAR",
    "payload_hash": "VARCHAR",
    "source": "VARCHAR",
    "ws_url": "VARCHAR",
    "shard_id": "VARCHAR",
    "shard_count": "VARCHAR",
    "raw_connection_id": "VARCHAR",
    "raw_connection_generation": "VARCHAR",
    "raw_frame_seq": "VARCHAR",
    "raw_received_wall_ns": "VARCHAR",
    "message_index": "VARCHAR",
    "change_index": "VARCHAR",
    "group_id": "VARCHAR",
    "is_last_in_group": "VARCHAR",
    "raw_frame_complete": "VARCHAR",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--input-format",
        choices=("newline_delimited", "array"),
        default="newline_delimited",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stats-output", type=Path)
    parser.add_argument("--compression", default="zstd")
    parser.add_argument("--compression-level", type=int, default=9)
    parser.add_argument("--row-group-size", type=int, default=1_048_576)
    parser.add_argument("--threads", type=int, default=4)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:", config={"threads": str(max(1, int(args.threads)))})
    try:
        columns_sql = "{" + ",".join(
            f"'{name}':'{column_type}'" for name, column_type in JSON_COLUMNS.items()
        ) + "}"
        con.execute(
            f"""
            COPY (
                SELECT
                    CAST(timestamp_received AS TIMESTAMPTZ) AS timestamp_received,
                    CAST(timestamp_normalized AS TIMESTAMPTZ)
                        AS timestamp_normalized,
                    CAST(timestamp AS TIMESTAMPTZ) AS timestamp,
                    CAST(raw_event_timestamp AS VARCHAR)
                        AS raw_event_timestamp,
                    CAST(clock_invalid_reason AS VARCHAR)
                        AS clock_invalid_reason,
                    TRY_CAST(rest_request_start_ns AS UBIGINT)
                        AS rest_request_start_ns,
                    TRY_CAST(rest_response_end_ns AS UBIGINT)
                        AS rest_response_end_ns,
                    CAST(market AS VARCHAR) AS market,
                    CAST(event_type AS VARCHAR) AS event_type,
                    CAST(asset_id AS VARCHAR) AS asset_id,
                    CAST(collector_seq AS BIGINT) AS collector_seq,
                    CAST(sequence_in_message AS INTEGER) AS sequence_in_message,
                    CAST(bids AS VARCHAR) AS bids,
                    CAST(asks AS VARCHAR) AS asks,
                    TRY_CAST(price AS DECIMAL(20, 10)) AS price,
                    TRY_CAST(size AS DECIMAL(38, 10)) AS size,
                    CAST(side AS VARCHAR) AS side,
                    TRY_CAST(best_bid AS DECIMAL(20, 10)) AS best_bid,
                    TRY_CAST(best_ask AS DECIMAL(20, 10)) AS best_ask,
                    TRY_CAST(fee_rate_bps AS USMALLINT) AS fee_rate_bps,
                    CAST(transaction_hash AS VARCHAR) AS transaction_hash,
                    TRY_CAST(old_tick_size AS DECIMAL(20, 10)) AS old_tick_size,
                    TRY_CAST(new_tick_size AS DECIMAL(20, 10)) AS new_tick_size,
                    CAST(book_hash AS VARCHAR) AS book_hash,
                    CAST(payload_hash AS VARCHAR) AS payload_hash,
                    CAST(source AS VARCHAR) AS source,
                    CAST(ws_url AS VARCHAR) AS ws_url,
                    TRY_CAST(shard_id AS INTEGER) AS shard_id,
                    TRY_CAST(shard_count AS INTEGER) AS shard_count,
                    CAST(raw_connection_id AS VARCHAR) AS raw_connection_id,
                    TRY_CAST(raw_connection_generation AS BIGINT)
                        AS raw_connection_generation,
                    TRY_CAST(raw_frame_seq AS BIGINT) AS raw_frame_seq,
                    TRY_CAST(raw_received_wall_ns AS UBIGINT)
                        AS raw_received_wall_ns,
                    TRY_CAST(message_index AS INTEGER) AS message_index,
                    TRY_CAST(change_index AS INTEGER) AS change_index,
                    CAST(group_id AS VARCHAR) AS group_id,
                    TRY_CAST(is_last_in_group AS BOOLEAN) AS is_last_in_group,
                    TRY_CAST(raw_frame_complete AS BOOLEAN)
                        AS raw_frame_complete
                FROM read_json(
                    ?,
                    format='{args.input_format}',
                    maximum_object_size=67108864,
                    columns={columns_sql}
                )
                ORDER BY {SORT_KEY}
            )
            TO ?
            (FORMAT PARQUET, COMPRESSION {str(args.compression).upper()},
             COMPRESSION_LEVEL {int(args.compression_level)}, ROW_GROUP_SIZE {int(args.row_group_size)})
            """,
            [str(args.output), str(args.input)],
        )
        if args.stats_output is not None:
            (
                event_count,
                asset_count,
                market_count,
                first_event_ts,
                last_event_ts,
                first_received_at,
                last_received_at,
                first_local_seq,
                last_local_seq,
            ) = con.execute(
                """
                SELECT
                    count(*)::BIGINT,
                    count(DISTINCT asset_id) FILTER (
                        WHERE asset_id IS NOT NULL AND asset_id != ''
                    )::BIGINT,
                    count(DISTINCT market) FILTER (
                        WHERE market IS NOT NULL AND market != ''
                    )::BIGINT,
                    min(timestamp),
                    max(timestamp),
                    min(timestamp_received),
                    max(timestamp_received),
                    min(collector_seq),
                    max(collector_seq)
                FROM read_parquet(?)
                """,
                [str(args.output)],
            ).fetchone()
            event_type_counts = {
                str(event_type or "unknown"): int(count)
                for event_type, count in con.execute(
                    """
                    SELECT event_type, count(*)::BIGINT
                    FROM read_parquet(?)
                    GROUP BY event_type
                    """,
                    [str(args.output)],
                ).fetchall()
            }
            raw_frame_watermarks = {
                str(int(shard_id)): int(frame_seq)
                for shard_id, frame_seq in con.execute(
                    """
                    SELECT shard_id, max(raw_frame_seq)::BIGINT
                    FROM read_parquet(?)
                    WHERE raw_frame_complete
                      AND shard_id IS NOT NULL
                      AND raw_frame_seq IS NOT NULL
                    GROUP BY shard_id
                    """,
                    [str(args.output)],
                ).fetchall()
            }
            payload = {
                "event_count": int(event_count),
                "asset_count": int(asset_count),
                "market_count": int(market_count),
                "first_event_ts": _iso(first_event_ts),
                "last_event_ts": _iso(last_event_ts),
                "first_received_at": _iso(first_received_at),
                "last_received_at": _iso(last_received_at),
                "first_local_seq": (
                    int(first_local_seq)
                    if first_local_seq is not None
                    else None
                ),
                "last_local_seq": (
                    int(last_local_seq)
                    if last_local_seq is not None
                    else None
                ),
                "event_type_counts": event_type_counts,
                "raw_frame_watermarks": raw_frame_watermarks,
            }
            args.stats_output.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
            temporary = args.stats_output.with_name(
                f".{args.stats_output.name}.{os.getpid()}.tmp"
            )
            temporary.write_text(
                json.dumps(payload, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary, args.stats_output)
    finally:
        con.close()
    return 0


def _iso(value: object) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


if __name__ == "__main__":
    raise SystemExit(main())
