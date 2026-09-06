"""ClickHouse tick-level OrderFilled replay store.

The block replay table is useful for fast screening, but strict fill-first
execution needs the original OrderFilled ticks in deterministic order.  This
module materializes bounded `(market_id, token_id, block_number)` slices into a
TradeTick-like cache while preserving canonical fill keys and attribution
fields used by the event stream.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import time
from typing import Any, Iterable

from ...core.db import ClickHouseClient, safe_identifier


DEFAULT_TRADE_REPLAY_TABLE = "orderfilled_trade_replay"
DEFAULT_TRADE_REPLAY_COVERAGE_TABLE = "orderfilled_trade_replay_coverage"
TRADE_REPLAY_DATA_VERSION = "orderfilled_trade_replay_v1"


@dataclass(frozen=True)
class TradeReplayBackfillResult:
    table: str
    token_pair_count: int
    from_block: int
    to_block: int
    before_rows: int
    inserted_rows: int
    after_rows: int
    elapsed_sec: float
    coverage_rows: int = 0

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def ensure_orderfilled_trade_replay_table(
    client: ClickHouseClient | None = None,
    *,
    table: str = DEFAULT_TRADE_REPLAY_TABLE,
) -> None:
    """Create the tick-level replay table if needed."""

    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    ch.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            market_id UInt64 CODEC(Delta, ZSTD(3)),
            condition_id String CODEC(ZSTD(3)),
            token_id String CODEC(ZSTD(3)),
            block_number UInt64 CODEC(Delta, ZSTD(3)),
            transaction_index UInt32 CODEC(Delta, ZSTD(3)),
            log_index UInt32 CODEC(Delta, ZSTD(3)),
            tx_hash String CODEC(ZSTD(3)),
            outcome_code UInt8 CODEC(ZSTD(3)),
            trade_price Decimal(20, 10) CODEC(ZSTD(3)),
            size Decimal(30, 10) CODEC(ZSTD(3)),
            side LowCardinality(String) CODEC(ZSTD(3)),
            maker String CODEC(ZSTD(3)),
            taker String CODEC(ZSTD(3)),
            canonical_fill_key String CODEC(ZSTD(3)),
            canonical_fill_key_kind LowCardinality(String) CODEC(ZSTD(3)),
            block_trade_index UInt32 CODEC(Delta, ZSTD(3)),
            source_table LowCardinality(String) DEFAULT 'orderfilled_fact' CODEC(ZSTD(3)),
            build_tag LowCardinality(String) DEFAULT 'manual_backfill' CODEC(ZSTD(3)),
            ingested_at DateTime DEFAULT now() CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY intDiv(block_number, 1000000)
        ORDER BY (market_id, condition_id, token_id, block_number, transaction_index, log_index, tx_hash, canonical_fill_key)
        SETTINGS index_granularity = 8192
        """,
        timeout_seconds=60,
    )
    ch.execute(
        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS condition_id String AFTER market_id",
        timeout_seconds=60,
    )
    ensure_orderfilled_trade_replay_coverage_table(ch)


def ensure_orderfilled_trade_replay_coverage_table(
    client: ClickHouseClient | None = None,
    *,
    table: str = DEFAULT_TRADE_REPLAY_COVERAGE_TABLE,
) -> None:
    """Create coverage rows for materialized tick replay windows."""

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
            canonical_event_count UInt64 CODEC(ZSTD(3)),
            fallback_event_count UInt64 CODEC(ZSTD(3)),
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


def backfill_orderfilled_trade_replay(
    token_pairs: Iterable[tuple[int, str]],
    *,
    from_block: int,
    to_block: int,
    table: str = DEFAULT_TRADE_REPLAY_TABLE,
    client: ClickHouseClient | None = None,
    force: bool = False,
    build_tag: str = "manual_backfill",
) -> TradeReplayBackfillResult:
    """Materialize raw OrderFilled ticks for bounded market/token windows."""

    pairs = _normalized_pairs(token_pairs)
    if not pairs:
        return TradeReplayBackfillResult(
            table=table,
            token_pair_count=0,
            from_block=int(from_block),
            to_block=int(to_block),
            before_rows=0,
            inserted_rows=0,
            after_rows=0,
            elapsed_sec=0.0,
        )
    start_block, end_block = _validated_block_window(from_block, to_block)
    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    ensure_orderfilled_trade_replay_table(ch, table=table_name)
    pairs_sql = _tuple_list(pairs)
    start = time.perf_counter()
    before_rows = _count_replay_rows(ch, table=table_name, pairs_sql=pairs_sql, from_block=start_block, to_block=end_block)
    if before_rows and not force:
        coverage_rows = refresh_orderfilled_trade_replay_coverage(
            pairs,
            from_block=start_block,
            to_block=end_block,
            replay_table=table_name,
            client=ch,
            build_tag=build_tag,
        )
        return TradeReplayBackfillResult(
            table=table_name,
            token_pair_count=len(pairs),
            from_block=start_block,
            to_block=end_block,
            before_rows=before_rows,
            inserted_rows=0,
            after_rows=before_rows,
            elapsed_sec=round(time.perf_counter() - start, 6),
            coverage_rows=coverage_rows,
        )

    expressions = _orderfilled_column_expressions(ch)
    source_table = safe_identifier(ch.settings.orderfilled_table)
    source_table_literal = _quote_clickhouse_string(source_table)
    tag = _quote_clickhouse_string(build_tag)
    ch.execute(
        f"""
        INSERT INTO {table_name}
        SELECT
            market_id,
            condition_id,
            token_id,
            block_number,
            transaction_index,
            log_index,
            tx_hash,
            outcome_code,
            trade_price,
            size,
            side,
            maker,
            taker,
            canonical_fill_key,
            canonical_fill_key_kind,
            row_number() OVER (
                PARTITION BY market_id, condition_id, token_id, block_number
                ORDER BY transaction_index ASC, log_index ASC, tx_hash ASC, canonical_fill_key ASC
            ) AS block_trade_index,
            {source_table_literal} AS source_table,
            {tag} AS build_tag,
            now() AS ingested_at
        FROM (
            SELECT
                market_id,
                {expressions['condition_id']} AS condition_id,
                lower(token_id) AS token_id,
                block_number,
                {expressions['transaction_index']} AS transaction_index,
                log_index,
                lower(tx_hash) AS tx_hash,
                outcome_code,
                price AS trade_price,
                size,
                {expressions['side']} AS side,
                {expressions['maker']} AS maker,
                {expressions['taker']} AS taker,
                if(
                    length(tx_hash) > 0
                    AND length(toString(log_index)) > 0
                    AND length(toString(market_id)) > 0
                    AND length(token_id) > 0
                    AND length({expressions['maker']}) > 0
                    AND length({expressions['taker']}) > 0
                    AND length({expressions['side']}) > 0,
                    if(
                        length({expressions['condition_id']}) > 0,
                        concat(lower(tx_hash), '|', toString(log_index), '|', toString(market_id), '|', {expressions['condition_id']}, '|', lower(token_id), '|', {expressions['maker']}, '|', {expressions['taker']}, '|', {expressions['side']}),
                        concat(lower(tx_hash), '|', toString(log_index), '|', toString(market_id), '|', lower(token_id), '|', {expressions['maker']}, '|', {expressions['taker']}, '|', {expressions['side']})
                    ),
                    if(
                        length({expressions['condition_id']}) > 0,
                        concat(toString(block_number), '|', toString({expressions['transaction_index']}), '|', toString(log_index), '|', lower(tx_hash), '|', toString(market_id), '|', {expressions['condition_id']}, '|', lower(token_id), '|', toString(price), '|', toString(size)),
                        concat(toString(block_number), '|', toString({expressions['transaction_index']}), '|', toString(log_index), '|', lower(tx_hash), '|', toString(market_id), '|', lower(token_id), '|', toString(price), '|', toString(size))
                    )
                ) AS canonical_fill_key,
                if(
                    length(tx_hash) > 0
                    AND length(toString(log_index)) > 0
                    AND length(toString(market_id)) > 0
                    AND length(token_id) > 0
                    AND length({expressions['maker']}) > 0
                    AND length({expressions['taker']}) > 0
                    AND length({expressions['side']}) > 0,
                    'canonical',
                    'fallback'
                ) AS canonical_fill_key_kind
            FROM {source_table}
            PREWHERE (market_id, token_id) IN ({pairs_sql})
              AND block_number BETWEEN {start_block} AND {end_block}
        )
        ORDER BY market_id ASC, condition_id ASC, token_id ASC, block_number ASC, transaction_index ASC, log_index ASC, tx_hash ASC
        """,
        timeout_seconds=600,
    )
    after_rows = _count_replay_rows(ch, table=table_name, pairs_sql=pairs_sql, from_block=start_block, to_block=end_block)
    coverage_rows = refresh_orderfilled_trade_replay_coverage(
        pairs,
        from_block=start_block,
        to_block=end_block,
        replay_table=table_name,
        client=ch,
        build_tag=build_tag,
    )
    return TradeReplayBackfillResult(
        table=table_name,
        token_pair_count=len(pairs),
        from_block=start_block,
        to_block=end_block,
        before_rows=before_rows,
        inserted_rows=max(0, after_rows - before_rows),
        after_rows=after_rows,
        elapsed_sec=round(time.perf_counter() - start, 6),
        coverage_rows=coverage_rows,
    )


def refresh_orderfilled_trade_replay_coverage(
    token_pairs: Iterable[tuple[int, str]],
    *,
    from_block: int,
    to_block: int,
    replay_table: str = DEFAULT_TRADE_REPLAY_TABLE,
    coverage_table: str = DEFAULT_TRADE_REPLAY_COVERAGE_TABLE,
    client: ClickHouseClient | None = None,
    build_tag: str = "manual_backfill",
    data_version: str = TRADE_REPLAY_DATA_VERSION,
) -> int:
    pairs = _normalized_pairs(token_pairs)
    if not pairs:
        return 0
    start_block, end_block = _validated_block_window(from_block, to_block)
    ch = client or ClickHouseClient()
    replay_name = safe_identifier(replay_table)
    coverage_name = safe_identifier(coverage_table)
    ensure_orderfilled_trade_replay_coverage_table(ch, table=coverage_name)
    pairs_sql = _tuple_list(pairs)
    tag = _quote_clickhouse_string(build_tag)
    version = _quote_clickhouse_string(data_version)
    before = _count_coverage_rows(ch, pairs_sql=pairs_sql, from_block=start_block, to_block=end_block, table=coverage_name)
    ch.execute(
        f"""
        INSERT INTO {coverage_name}
        SELECT
            market_id,
            token_id,
            {start_block} AS from_block,
            {end_block} AS to_block,
            count() AS row_count,
            min(block_number) AS first_block,
            max(block_number) AS last_block,
            countIf(canonical_fill_key_kind = 'canonical') AS canonical_event_count,
            countIf(canonical_fill_key_kind != 'canonical') AS fallback_event_count,
            {version} AS data_version,
            {tag} AS build_tag,
            now() AS updated_at
        FROM {replay_name}
        PREWHERE (market_id, token_id) IN ({pairs_sql})
          AND block_number BETWEEN {start_block} AND {end_block}
        GROUP BY market_id, token_id
        """,
        timeout_seconds=180,
    )
    after = _count_coverage_rows(ch, pairs_sql=pairs_sql, from_block=start_block, to_block=end_block, table=coverage_name)
    return max(0, after - before)


def load_orderfilled_trade_replay_coverage(
    token_pairs: Iterable[tuple[int, str]],
    *,
    from_block: int,
    to_block: int,
    coverage_table: str = DEFAULT_TRADE_REPLAY_COVERAGE_TABLE,
    client: ClickHouseClient | None = None,
    data_version: str = TRADE_REPLAY_DATA_VERSION,
) -> list[dict[str, Any]]:
    """Load coverage rows proving a tick replay window is materialized."""

    pairs = _normalized_pairs(token_pairs)
    if not pairs:
        return []
    start_block, end_block = _validated_block_window(from_block, to_block)
    ch = client or ClickHouseClient()
    table_name = safe_identifier(coverage_table)
    pairs_sql = _tuple_list(pairs)
    version = _quote_clickhouse_string(data_version)
    return ch.query_json_rows(
        f"""
        SELECT
            market_id,
            token_id,
            argMax(from_block, updated_at) AS from_block,
            argMax(to_block, updated_at) AS to_block,
            argMax(row_count, updated_at) AS row_count,
            argMax(first_block, updated_at) AS first_block,
            argMax(last_block, updated_at) AS last_block,
            argMax(canonical_event_count, updated_at) AS canonical_event_count,
            argMax(fallback_event_count, updated_at) AS fallback_event_count,
            argMax(data_version, updated_at) AS loaded_data_version,
            max(updated_at) AS latest_updated_at
        FROM (
            SELECT *
            FROM {table_name}
            PREWHERE (market_id, token_id) IN ({pairs_sql})
            WHERE data_version = {version}
              AND from_block <= {start_block}
              AND to_block >= {end_block}
        )
        GROUP BY market_id, token_id
        ORDER BY market_id ASC, token_id ASC
        """,
        timeout_seconds=120,
    )


def load_orderfilled_trade_replay_rows(
    token_pairs: Iterable[tuple[int, str]],
    *,
    from_block: int,
    to_block: int,
    table: str = DEFAULT_TRADE_REPLAY_TABLE,
    client: ClickHouseClient | None = None,
    limit: int = 10_000_000,
) -> list[dict[str, Any]]:
    """Load materialized raw ticks in deterministic replay order."""

    pairs = _normalized_pairs(token_pairs)
    if not pairs:
        return []
    start_block, end_block = _validated_block_window(from_block, to_block)
    row_limit = _validated_limit(limit)
    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    pairs_sql = _tuple_list(pairs)
    return ch.query_json_rows(
        f"""
        SELECT
            market_id,
            token_id,
            block_number,
            transaction_index,
            log_index,
            lower(tx_hash) AS tx_hash,
            outcome_code,
            trade_price,
            trade_price AS price,
            size,
            side AS side_code,
            maker,
            taker,
            canonical_fill_key,
            canonical_fill_key_kind,
            block_trade_index,
            'orderfilled_trade_replay' AS replay_source,
            'orderfilled_fact' AS source
        FROM {table_name}
        PREWHERE (market_id, token_id) IN ({pairs_sql})
          AND block_number BETWEEN {start_block} AND {end_block}
        ORDER BY market_id ASC, condition_id ASC, token_id ASC, block_number ASC, transaction_index ASC, log_index ASC, tx_hash ASC, canonical_fill_key ASC
        LIMIT {row_limit}
        """,
        timeout_seconds=240,
    )


def build_trade_replay_sql_contract_report() -> dict[str, Any]:
    """DB-free contract check for the tick replay materialized path."""

    pairs = [(11, "token-a"), (12, "token-b")]
    create_client = _SqlCaptureClickHouse()
    ensure_orderfilled_trade_replay_table(create_client, table="orderfilled_trade_replay_contract")
    load_client = _SqlCaptureClickHouse()
    load_orderfilled_trade_replay_rows(pairs, from_block=100, to_block=200, client=load_client, table="orderfilled_trade_replay_contract")
    ddl = "\n".join(create_client.executed)
    load_sql = "\n".join(load_client.queries)
    checks = {
        "table_ordered_by_token_block_tick": "ORDER BY (market_id, condition_id, token_id, block_number, transaction_index, log_index, tx_hash, canonical_fill_key)" in ddl,
        "canonical_fill_key_persisted": "canonical_fill_key String" in ddl and "canonical_fill_key_kind" in ddl,
        "condition_id_persisted": "condition_id String" in ddl,
        "maker_taker_side_persisted": "maker String" in ddl and "taker String" in ddl and "side LowCardinality(String)" in ddl,
        "block_trade_index_persisted": "block_trade_index UInt32" in ddl,
        "loader_pair_prewhere": "PREWHERE (market_id, token_id) IN ((11, 'token-a'),(12, 'token-b'))" in load_sql,
        "loader_block_range": "block_number BETWEEN 100 AND 200" in load_sql,
        "loader_tick_ordered": "ORDER BY market_id ASC, condition_id ASC, token_id ASC, block_number ASC, transaction_index ASC, log_index ASC, tx_hash ASC, canonical_fill_key ASC" in load_sql,
        "loader_limited": "LIMIT 10000000" in load_sql,
    }
    missing = [name for name, ok in checks.items() if not ok]
    status = "ready" if not missing else "missing"
    return {
        "status": status,
        "contract_version": "trade_replay_sql_contract_v1",
        "reason": "tick-level replay cache preserves deterministic OrderFilled TradeTick inputs" if status == "ready" else "tick-level replay cache contract is incomplete",
        "checks": checks,
        "missing": missing,
        "required_access": [
            "configurable source orderfilled table",
            "PREWHERE (market_id, token_id)",
            "block_number BETWEEN from_block AND to_block",
            "ORDER BY market_id, token_id, block_number, transaction_index, log_index, tx_hash, canonical_fill_key",
            "canonical fill key, condition_id, and key kind",
            "maker/taker/side attribution",
            "block_trade_index",
        ],
    }


def _orderfilled_column_expressions(client: ClickHouseClient) -> dict[str, str]:
    database = safe_identifier(client.settings.database)
    table = safe_identifier(client.settings.orderfilled_table)
    has_transaction_index = _has_column(client, database, table, "transaction_index")
    has_condition_id = _has_column(client, database, table, "condition_id")
    has_maker = _has_column(client, database, table, "maker")
    has_taker = _has_column(client, database, table, "taker")
    has_side = _has_column(client, database, table, "side")
    has_side_code = _has_column(client, database, table, "side_code")
    return {
        "transaction_index": "toUInt32(transaction_index)" if has_transaction_index else "toUInt32(0)",
        "condition_id": "lower(toString(condition_id))" if has_condition_id else "''",
        "maker": "lower(toString(maker))" if has_maker else "''",
        "taker": "lower(toString(taker))" if has_taker else "''",
        "side": _side_expression(has_side=has_side, has_side_code=has_side_code),
    }


def _side_expression(*, has_side: bool, has_side_code: bool) -> str:
    if has_side:
        return "upper(toString(side))"
    if has_side_code:
        return "multiIf(side_code = 1, 'BUY', side_code = 2, 'SELL', upper(toString(side_code)))"
    return "''"


def _has_column(client: ClickHouseClient, database: str, table: str, column: str) -> bool:
    value = client.query_scalar(
        f"""
        SELECT count()
        FROM system.columns
        WHERE database = '{_ch_escape(database)}'
          AND table = '{_ch_escape(table)}'
          AND name = '{_ch_escape(column)}'
        """,
        timeout_seconds=30,
    )
    return str(value).strip() not in {"", "0"}


def _count_replay_rows(
    client: ClickHouseClient,
    *,
    table: str,
    pairs_sql: str,
    from_block: int,
    to_block: int,
) -> int:
    value = client.query_scalar(
        f"""
        SELECT count()
        FROM {table}
        PREWHERE (market_id, token_id) IN ({pairs_sql})
          AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        """,
        timeout_seconds=120,
    )
    return int(value or 0)


def _count_coverage_rows(
    client: ClickHouseClient,
    *,
    pairs_sql: str,
    from_block: int,
    to_block: int,
    table: str = DEFAULT_TRADE_REPLAY_COVERAGE_TABLE,
) -> int:
    table_name = safe_identifier(table)
    value = client.query_scalar(
        f"""
        SELECT count()
        FROM {table_name}
        PREWHERE (market_id, token_id) IN ({pairs_sql})
          AND from_block <= {int(from_block)}
          AND to_block >= {int(to_block)}
        """,
        timeout_seconds=120,
    )
    return int(value or 0)


def _normalized_pairs(token_pairs: Iterable[tuple[int, str]]) -> list[tuple[int, str]]:
    pairs = []
    seen: set[tuple[int, str]] = set()
    for market_id, token_id in token_pairs:
        pair = (int(market_id), str(token_id or "").strip().lower())
        if pair[0] <= 0 or not pair[1] or pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return sorted(pairs)


def _validated_block_window(from_block: int, to_block: int) -> tuple[int, int]:
    start = int(from_block)
    end = int(to_block)
    if start < 0 or end < 0:
        raise ValueError("OrderFilled replay block bounds must be non-negative")
    if end < start:
        raise ValueError("OrderFilled replay requires to_block >= from_block")
    return start, end


def _validated_limit(limit: int) -> int:
    row_limit = int(limit)
    if row_limit <= 0:
        raise ValueError("OrderFilled replay load limit must be > 0")
    return row_limit


def _tuple_list(pairs: list[tuple[int, str]]) -> str:
    return ",".join(f"({int(market_id)}, {_quote_clickhouse_string(token_id)})" for market_id, token_id in pairs)


def _quote_clickhouse_string(value: str) -> str:
    return "'" + _ch_escape(str(value).lower()) + "'"


def _ch_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


class _SqlCaptureClickHouse:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.queries: list[str] = []

    def execute(self, sql: str, timeout_seconds: int | None = None) -> None:
        self.executed.append(str(sql))

    def query_json_rows(self, sql: str, timeout_seconds: int | None = None) -> list[dict[str, Any]]:
        self.queries.append(str(sql))
        return []
