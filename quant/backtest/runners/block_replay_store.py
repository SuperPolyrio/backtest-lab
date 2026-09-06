"""ClickHouse block-level OrderFilled replay store.

The raw `orderfilled_fact` table is the accurate replay source, but scanning and
grouping it for every strategy sweep is too expensive. This module owns the
block-level replay table used by fast screening runs.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import time
from typing import Any, Iterable

from ...core.db import ClickHouseClient, safe_identifier


DEFAULT_BLOCK_REPLAY_TABLE = "orderfilled_block_replay"
DEFAULT_BLOCK_REPLAY_COVERAGE_TABLE = "orderfilled_block_replay_coverage"


@dataclass(frozen=True)
class BlockReplayBackfillResult:
    table: str
    market_count: int
    from_block: int
    to_block: int
    before_rows: int
    inserted_rows: int
    after_rows: int
    elapsed_sec: float
    coverage_rows: int = 0


def ensure_orderfilled_block_replay_table(
    client: ClickHouseClient | None = None,
    *,
    table: str = DEFAULT_BLOCK_REPLAY_TABLE,
) -> None:
    """Create the formal block replay table if it does not exist."""

    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    ch.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            market_id UInt64 CODEC(Delta, ZSTD(3)),
            token_id String CODEC(ZSTD(3)),
            block_number UInt64 CODEC(Delta, ZSTD(3)),
            outcome_code UInt8 CODEC(ZSTD(3)),
            block_time DateTime CODEC(Delta, ZSTD(3)),
            open_price Decimal(20, 10) CODEC(ZSTD(3)),
            high_price Decimal(20, 10) CODEC(ZSTD(3)),
            low_price Decimal(20, 10) CODEC(ZSTD(3)),
            close_price Decimal(20, 10) CODEC(ZSTD(3)),
            vwap_price Decimal(20, 10) CODEC(ZSTD(3)),
            volume Decimal(30, 10) CODEC(ZSTD(3)),
            trade_count UInt64 CODEC(ZSTD(3)),
            buy_volume Decimal(30, 10) CODEC(ZSTD(3)),
            sell_volume Decimal(30, 10) CODEC(ZSTD(3)),
            first_log_index UInt32 CODEC(Delta, ZSTD(3)),
            last_log_index UInt32 CODEC(Delta, ZSTD(3)),
            first_tx_hash String CODEC(ZSTD(3)),
            last_tx_hash String CODEC(ZSTD(3)),
            source_table LowCardinality(String) DEFAULT 'orderfilled_fact' CODEC(ZSTD(3)),
            build_tag LowCardinality(String) DEFAULT 'manual_backfill' CODEC(ZSTD(3)),
            ingested_at DateTime DEFAULT now() CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY intDiv(block_number, 1000000)
        ORDER BY (market_id, token_id, block_number)
        SETTINGS index_granularity = 8192
        """,
        timeout_seconds=60,
    )
    ch.execute(
        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS vwap_price Decimal(20, 10) CODEC(ZSTD(3)) AFTER close_price",
        timeout_seconds=60,
    )
    ensure_orderfilled_block_replay_coverage_table(ch)


def ensure_orderfilled_block_replay_coverage_table(
    client: ClickHouseClient | None = None,
    *,
    table: str = DEFAULT_BLOCK_REPLAY_COVERAGE_TABLE,
) -> None:
    """Create the persistent coverage manifest for block replay ranges."""

    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    ch.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            market_id UInt64 CODEC(Delta, ZSTD(3)),
            token_id String CODEC(ZSTD(3)),
            from_block UInt64 CODEC(Delta, ZSTD(3)),
            to_block UInt64 CODEC(Delta, ZSTD(3)),
            row_count UInt64 CODEC(ZSTD(3)),
            first_block UInt64 CODEC(Delta, ZSTD(3)),
            last_block UInt64 CODEC(Delta, ZSTD(3)),
            data_version String CODEC(ZSTD(3)),
            build_tag LowCardinality(String) CODEC(ZSTD(3)),
            updated_at DateTime DEFAULT now() CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (market_id, token_id, from_block, to_block, data_version)
        SETTINGS index_granularity = 8192
        """,
        timeout_seconds=60,
    )


def backfill_orderfilled_block_replay(
    market_ids: Iterable[int],
    *,
    from_block: int,
    to_block: int,
    table: str = DEFAULT_BLOCK_REPLAY_TABLE,
    client: ClickHouseClient | None = None,
    force: bool = False,
    build_tag: str = "manual_backfill",
) -> BlockReplayBackfillResult:
    """Materialize block replay rows for a bounded market/block range.

    The table uses ReplacingMergeTree, so a forced rerun is safe for query
    correctness when readers use FINAL. For normal repeated runs we skip the
    insert if the requested range already has rows, which avoids unnecessary
    table growth during interactive research.
    """

    ids = sorted({int(market_id) for market_id in market_ids})
    if not ids:
        return BlockReplayBackfillResult(
            table=table,
            market_count=0,
            from_block=int(from_block),
            to_block=int(to_block),
            before_rows=0,
            inserted_rows=0,
            after_rows=0,
            elapsed_sec=0.0,
            coverage_rows=0,
        )
    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    ensure_orderfilled_block_replay_table(ch, table=table_name)
    ids_sql = ",".join(str(market_id) for market_id in ids)
    start = time.perf_counter()
    before_rows = _count_replay_rows(ch, table=table_name, ids_sql=ids_sql, from_block=from_block, to_block=to_block)
    coverage = _replay_coverage(ch, table=table_name, ids_sql=ids_sql, from_block=from_block, to_block=to_block)
    target_ids = ids
    if not force:
        existing_ids = _covered_replay_market_ids(
            ch,
            ids_sql=ids_sql,
            from_block=from_block,
            to_block=to_block,
            data_version="orderfilled_block_replay_v1",
        )
        if not existing_ids:
            existing_ids = _existing_replay_market_ids(ch, table=table_name, ids_sql=ids_sql, from_block=from_block, to_block=to_block)
        target_ids = [market_id for market_id in ids if market_id not in existing_ids]
        if target_ids:
            target_ids = _raw_available_market_ids(ch, market_ids=target_ids, from_block=from_block, to_block=to_block)
        if not target_ids:
            coverage_rows = _count_coverage_rows(ch, ids_sql=ids_sql, from_block=from_block, to_block=to_block)
            replay_token_pairs = _count_replay_token_pairs(ch, table=table_name, ids_sql=ids_sql, from_block=from_block, to_block=to_block)
            if before_rows > 0 and coverage_rows < replay_token_pairs:
                coverage_rows = refresh_orderfilled_block_replay_coverage(
                    ids,
                    from_block=from_block,
                    to_block=to_block,
                    replay_table=table_name,
                    client=ch,
                    build_tag=build_tag,
                    data_version="orderfilled_block_replay_v1",
                )
            return BlockReplayBackfillResult(
                table=table_name,
                market_count=len(ids),
                from_block=int(from_block),
                to_block=int(to_block),
                before_rows=before_rows,
                inserted_rows=0,
                after_rows=before_rows,
                elapsed_sec=round(time.perf_counter() - start, 6),
                coverage_rows=coverage_rows,
            )
    target_ids_sql = ",".join(str(market_id) for market_id in target_ids)
    if _coverage_complete(coverage, market_count=len(ids), from_block=from_block, to_block=to_block) and not force:
        coverage_rows = _count_coverage_rows(ch, ids_sql=ids_sql, from_block=from_block, to_block=to_block)
        replay_token_pairs = _count_replay_token_pairs(ch, table=table_name, ids_sql=ids_sql, from_block=from_block, to_block=to_block)
        if before_rows > 0 and coverage_rows < replay_token_pairs:
            coverage_rows = refresh_orderfilled_block_replay_coverage(
                ids,
                from_block=from_block,
                to_block=to_block,
                replay_table=table_name,
                client=ch,
                build_tag=build_tag,
                data_version="orderfilled_block_replay_v1",
            )
        return BlockReplayBackfillResult(
            table=table_name,
            market_count=len(ids),
            from_block=int(from_block),
            to_block=int(to_block),
            before_rows=before_rows,
            inserted_rows=0,
            after_rows=before_rows,
            elapsed_sec=round(time.perf_counter() - start, 6),
            coverage_rows=coverage_rows,
        )

    tag = _quote_clickhouse_string(build_tag)
    ch.execute(
        f"""
        INSERT INTO {table_name}
        SELECT
            f.market_id,
            f.token_id,
            f.block_number,
            any(f.outcome_code) AS outcome_code,
            any(bt.block_time) AS block_time,
            argMin(f.price, (f.log_index, f.tx_hash)) AS open_price,
            max(f.price) AS high_price,
            min(f.price) AS low_price,
            argMax(f.price, (f.log_index, f.tx_hash)) AS close_price,
            CAST(if(sum(f.size) > 0, sum(f.price * f.size) / sum(f.size), 0), 'Decimal(20, 10)') AS vwap_price,
            sum(f.size) AS volume,
            count() AS trade_count,
            sumIf(f.size, f.side_code = 1) AS buy_volume,
            sumIf(f.size, f.side_code = 2) AS sell_volume,
            min(f.log_index) AS first_log_index,
            max(f.log_index) AS last_log_index,
            argMin(f.tx_hash, (f.log_index, f.tx_hash)) AS first_tx_hash,
            argMax(f.tx_hash, (f.log_index, f.tx_hash)) AS last_tx_hash,
            'orderfilled_fact' AS source_table,
            {tag} AS build_tag,
            now() AS ingested_at
        FROM (
            SELECT
                market_id,
                token_id,
                block_number,
                outcome_code,
                price,
                size,
                side_code,
                log_index,
                tx_hash
            FROM orderfilled_fact
            PREWHERE market_id IN ({target_ids_sql})
              AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        ) f
        INNER JOIN block_timestamps bt ON bt.block_number = f.block_number
        GROUP BY f.market_id, f.token_id, f.block_number
        """,
        timeout_seconds=600,
    )
    after_rows = _count_replay_rows(ch, table=table_name, ids_sql=ids_sql, from_block=from_block, to_block=to_block)
    coverage_rows = refresh_orderfilled_block_replay_coverage(
        ids,
        from_block=from_block,
        to_block=to_block,
        replay_table=table_name,
        client=ch,
        build_tag=build_tag,
        data_version="orderfilled_block_replay_v1",
    )
    return BlockReplayBackfillResult(
        table=table_name,
        market_count=len(ids),
        from_block=int(from_block),
        to_block=int(to_block),
        before_rows=before_rows,
        inserted_rows=max(0, after_rows - before_rows),
        after_rows=after_rows,
        elapsed_sec=round(time.perf_counter() - start, 6),
        coverage_rows=coverage_rows,
    )


def refresh_orderfilled_block_replay_coverage(
    market_ids: Iterable[int],
    *,
    from_block: int,
    to_block: int,
    replay_table: str = DEFAULT_BLOCK_REPLAY_TABLE,
    coverage_table: str = DEFAULT_BLOCK_REPLAY_COVERAGE_TABLE,
    client: ClickHouseClient | None = None,
    build_tag: str = "manual_backfill",
    data_version: str = "orderfilled_block_replay_v1",
) -> int:
    """Persist coverage rows for a bounded replay build."""

    ids = sorted({int(market_id) for market_id in market_ids})
    if not ids:
        return 0
    ch = client or ClickHouseClient()
    replay_name = safe_identifier(replay_table)
    coverage_name = safe_identifier(coverage_table)
    ensure_orderfilled_block_replay_coverage_table(ch, table=coverage_name)
    ids_sql = ",".join(str(market_id) for market_id in ids)
    tag = _quote_clickhouse_string(build_tag)
    version = _quote_clickhouse_string(data_version)
    before = _count_coverage_rows(ch, ids_sql=ids_sql, from_block=from_block, to_block=to_block, table=coverage_name)
    ch.execute(
        f"""
        INSERT INTO {coverage_name}
        SELECT
            market_id,
            token_id,
            {int(from_block)} AS from_block,
            {int(to_block)} AS to_block,
            count() AS row_count,
            min(block_number) AS first_block,
            max(block_number) AS last_block,
            {version} AS data_version,
            {tag} AS build_tag,
            now() AS updated_at
        FROM {replay_name}
        PREWHERE market_id IN ({ids_sql})
          AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        GROUP BY market_id, token_id
        """,
        timeout_seconds=180,
    )
    after = _count_coverage_rows(ch, ids_sql=ids_sql, from_block=from_block, to_block=to_block, table=coverage_name)
    return max(0, after - before)


def load_orderfilled_block_replay_coverage(
    market_ids: Iterable[int],
    *,
    from_block: int,
    to_block: int,
    coverage_table: str = DEFAULT_BLOCK_REPLAY_COVERAGE_TABLE,
    client: ClickHouseClient | None = None,
    data_version: str = "orderfilled_block_replay_v1",
) -> list[dict[str, Any]]:
    ids = sorted({int(market_id) for market_id in market_ids})
    if not ids:
        return []
    ch = client or ClickHouseClient()
    table_name = safe_identifier(coverage_table)
    ensure_orderfilled_block_replay_coverage_table(ch, table=table_name)
    ids_sql = ",".join(str(market_id) for market_id in ids)
    version = _quote_clickhouse_string(data_version)
    return ch.query_json_rows(
        f"""
        SELECT
            market_id,
            token_id,
            min(from_block) AS from_block,
            max(to_block) AS to_block,
            sum(row_count) AS row_count,
            min(first_block) AS first_block,
            max(last_block) AS last_block,
            any(data_version) AS data_version,
            max(updated_at) AS updated_at
        FROM {table_name}
        PREWHERE market_id IN ({ids_sql})
        WHERE data_version = {version}
          AND from_block <= {int(from_block)}
          AND to_block >= {int(to_block)}
        GROUP BY market_id, token_id
        ORDER BY market_id ASC, token_id ASC
        """,
        timeout_seconds=120,
    )


def load_orderfilled_block_replay_rows(
    market_ids: Iterable[int],
    *,
    from_block: int,
    to_block: int,
    table: str = DEFAULT_BLOCK_REPLAY_TABLE,
    client: ClickHouseClient | None = None,
) -> list[dict[str, Any]]:
    """Load materialized block replay rows for a bounded range."""

    ids = sorted({int(market_id) for market_id in market_ids})
    if not ids:
        return []
    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    ids_sql = ",".join(str(market_id) for market_id in ids)
    return ch.query_json_rows(
        f"""
        SELECT
            market_id,
            outcome_code,
            token_id,
            block_number,
            block_time,
            open_price,
            high_price,
            low_price,
            close_price,
            vwap_price,
            volume,
            trade_count,
            buy_volume,
            sell_volume,
            first_log_index,
            last_log_index,
            lower(last_tx_hash) AS tx_hash,
            low_price AS buy_cross_price,
            high_price AS sell_cross_price,
            close_price AS price,
            volume AS size,
            'orderfilled_block_replay' AS replay_source
        FROM {table_name}
        PREWHERE market_id IN ({ids_sql})
          AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        ORDER BY market_id ASC, outcome_code ASC, block_number ASC
        """,
        timeout_seconds=240,
    )


def load_orderfilled_block_replay_rows_for_ranges(
    market_block_ranges: dict[int, tuple[int, int]],
    *,
    table: str = DEFAULT_BLOCK_REPLAY_TABLE,
    client: ClickHouseClient | None = None,
) -> list[dict[str, Any]]:
    """Load block replay rows using per-market block windows."""

    ranges = {
        int(market_id): (int(bounds[0]), int(bounds[1]))
        for market_id, bounds in market_block_ranges.items()
        if bounds[0] is not None and bounds[1] is not None and int(bounds[0]) <= int(bounds[1])
    }
    if not ranges:
        return []
    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    ids_sql = ",".join(str(market_id) for market_id in sorted(ranges))
    min_block = min(bounds[0] for bounds in ranges.values())
    max_block = max(bounds[1] for bounds in ranges.values())
    range_sql = _market_block_range_condition(ranges)
    return ch.query_json_rows(
        f"""
        SELECT
            market_id,
            outcome_code,
            token_id,
            block_number,
            block_time,
            open_price,
            high_price,
            low_price,
            close_price,
            vwap_price,
            volume,
            trade_count,
            buy_volume,
            sell_volume,
            first_log_index,
            last_log_index,
            lower(last_tx_hash) AS tx_hash,
            low_price AS buy_cross_price,
            high_price AS sell_cross_price,
            close_price AS price,
            volume AS size,
            'orderfilled_block_replay' AS replay_source
        FROM {table_name}
        PREWHERE market_id IN ({ids_sql})
          AND block_number BETWEEN {min_block} AND {max_block}
        WHERE {range_sql}
        ORDER BY market_id ASC, outcome_code ASC, block_number ASC
        """,
        timeout_seconds=240,
    )


def _count_replay_rows(
    client: ClickHouseClient,
    *,
    table: str,
    ids_sql: str,
    from_block: int,
    to_block: int,
) -> int:
    value = client.query_scalar(
        f"""
        SELECT count()
        FROM {table}
        PREWHERE market_id IN ({ids_sql})
          AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        """,
        timeout_seconds=120,
    )
    return int(value or 0)


def _count_replay_token_pairs(
    client: ClickHouseClient,
    *,
    table: str,
    ids_sql: str,
    from_block: int,
    to_block: int,
) -> int:
    value = client.query_scalar(
        f"""
        SELECT uniqExact(tuple(market_id, token_id))
        FROM {table}
        PREWHERE market_id IN ({ids_sql})
          AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        """,
        timeout_seconds=120,
    )
    return int(value or 0)


def _replay_coverage(
    client: ClickHouseClient,
    *,
    table: str,
    ids_sql: str,
    from_block: int,
    to_block: int,
) -> dict[str, int]:
    rows = client.query_json_rows(
        f"""
        SELECT
            countDistinct(market_id) AS market_count,
            min(block_number) AS min_block,
            max(block_number) AS max_block
        FROM {table}
        PREWHERE market_id IN ({ids_sql})
          AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        """,
        timeout_seconds=120,
    )
    if not rows:
        return {"market_count": 0, "min_block": 0, "max_block": 0}
    row = rows[0]
    return {
        "market_count": int(row.get("market_count") or 0),
        "min_block": int(row.get("min_block") or 0),
        "max_block": int(row.get("max_block") or 0),
    }


def _existing_replay_market_ids(
    client: ClickHouseClient,
    *,
    table: str,
    ids_sql: str,
    from_block: int,
    to_block: int,
) -> set[int]:
    rows = client.query_json_rows(
        f"""
        SELECT DISTINCT market_id
        FROM {table}
        PREWHERE market_id IN ({ids_sql})
          AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        """,
        timeout_seconds=120,
    )
    return {int(row["market_id"]) for row in rows}


def _covered_replay_market_ids(
    client: ClickHouseClient,
    *,
    ids_sql: str,
    from_block: int,
    to_block: int,
    data_version: str,
    table: str = DEFAULT_BLOCK_REPLAY_COVERAGE_TABLE,
) -> set[int]:
    table_name = safe_identifier(table)
    ensure_orderfilled_block_replay_coverage_table(client, table=table_name)
    version = _quote_clickhouse_string(data_version)
    rows = client.query_json_rows(
        f"""
        SELECT DISTINCT market_id
        FROM {table_name}
        PREWHERE market_id IN ({ids_sql})
        WHERE data_version = {version}
          AND from_block <= {int(from_block)}
          AND to_block >= {int(to_block)}
          AND row_count > 0
        """,
        timeout_seconds=120,
    )
    return {int(row["market_id"]) for row in rows}


def _count_coverage_rows(
    client: ClickHouseClient,
    *,
    ids_sql: str,
    from_block: int,
    to_block: int,
    table: str = DEFAULT_BLOCK_REPLAY_COVERAGE_TABLE,
) -> int:
    table_name = safe_identifier(table)
    ensure_orderfilled_block_replay_coverage_table(client, table=table_name)
    value = client.query_scalar(
        f"""
        SELECT uniqExact(tuple(market_id, token_id))
        FROM {table_name}
        PREWHERE market_id IN ({ids_sql})
        WHERE from_block <= {int(to_block)}
          AND to_block >= {int(from_block)}
        """,
        timeout_seconds=120,
    )
    return int(value or 0)


def _raw_available_market_ids(
    client: ClickHouseClient,
    *,
    market_ids: list[int],
    from_block: int,
    to_block: int,
) -> list[int]:
    if not market_ids:
        return []
    ids_sql = ",".join(str(market_id) for market_id in market_ids)
    rows = client.query_json_rows(
        f"""
        SELECT DISTINCT market_id
        FROM orderfilled_fact
        PREWHERE market_id IN ({ids_sql})
          AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        """,
        timeout_seconds=180,
    )
    return sorted({int(row["market_id"]) for row in rows})


def _coverage_complete(
    coverage: dict[str, int],
    *,
    market_count: int,
    from_block: int,
    to_block: int,
) -> bool:
    return (
        int(coverage.get("market_count") or 0) >= int(market_count)
        and int(coverage.get("min_block") or 0) <= int(from_block)
        and int(coverage.get("max_block") or 0) >= int(to_block)
    )




def _quote_clickhouse_string(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _market_block_range_condition(ranges: dict[int, tuple[int, int]]) -> str:
    return " OR ".join(
        f"(market_id = {int(market_id)} AND block_number BETWEEN {int(bounds[0])} AND {int(bounds[1])})"
        for market_id, bounds in sorted(ranges.items())
    )


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ensure", action="store_true")
    parser.add_argument("--table", default=DEFAULT_BLOCK_REPLAY_TABLE)
    parser.add_argument("--market-ids", default="", help="Comma-separated market ids for bounded backfill.")
    parser.add_argument("--from-block", type=int, default=0)
    parser.add_argument("--to-block", type=int, default=0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    ch = ClickHouseClient()
    ensure_orderfilled_block_replay_table(ch, table=args.table)
    if args.ensure and not args.market_ids:
        payload = {"table": args.table, "ensured": True}
    else:
        if not args.market_ids or not args.from_block or not args.to_block:
            raise SystemExit("--market-ids, --from-block, and --to-block are required unless only --ensure is used")
        result = backfill_orderfilled_block_replay(
            [int(item) for item in args.market_ids.split(",") if item.strip()],
            from_block=args.from_block,
            to_block=args.to_block,
            table=args.table,
            client=ch,
            force=args.force,
            build_tag="cli_backfill",
        )
        payload = asdict(result)
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
