"""Build OrderFilled-only V2 data layers in ClickHouse.

This script materializes the data contract described in
``quantV2/docs/orderfilled数据怎么处理.md`` for a bounded block/date window.

The current source table, ``orderfilled_fact``, is already a normalized fact
table rather than a verbatim chain log.  Where raw asset IDs are absent, the
builder derives maker/taker asset IDs from the stored aggressor side and records
that derivation in explicit source columns.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.core.db import (  # noqa: E402
    ClickHouseClient,
    postgres_connection,
    safe_identifier,
)


DEFAULT_CHAIN_ID = 137
DEFAULT_FROM_DATE = "2026-02-01 00:00:00"
DEFAULT_CHUNK_BLOCKS = 100_000
DEFAULT_TABLES = (
    "raw_orderfilled",
    "maker_fill_ticks",
    "orderfilled_quarantine",
    "trade_prints_one_sided",
    "block_trade_bars_sparse",
)
TIME_BAR_INTERVALS = (1, 5)
TRUSTED_BLOCK_TIME_SOURCE_SQL = (
    "(startsWith(lower(source), 'rpc') OR startsWith(lower(source), 'polygon_rpc'))"
)
# Validation queries serialize Decimal sums with toString, so conservation is exact.
CONSERVATION_ABS_TOLERANCE = Decimal("0")


def builder_identity() -> dict[str, str]:
    """Return the exact materializer/validator bytes used by this process."""

    script_path = Path(__file__).resolve()
    return {
        "canonical_identity_contract": "ORDERFILLED_SEVEN_FIELD_V1",
        "script_path": str(script_path),
        "script_sha256": hashlib.sha256(script_path.read_bytes()).hexdigest(),
    }


def _fill_identity_group_by(prefix: str = "") -> str:
    """Return the existing seven-field canonical OrderFilled identity."""

    return ", ".join(
        (
            f"lower({prefix}tx_hash)",
            f"{prefix}log_index",
            f"{prefix}market_id",
            f"lower({prefix}token_id)",
            f"lower({prefix}maker)",
            f"lower({prefix}taker)",
            f"{prefix}side_code",
        )
    )


def _immutable_payload_tuple(prefix: str = "") -> str:
    """Return normalized immutable payload fields for conflict detection."""

    return f"""tuple(
        lower({prefix}tx_hash),
        toUInt32({prefix}log_index),
        toUInt64({prefix}market_id),
        lower({prefix}condition_id),
        lower({prefix}token_id),
        toUInt8({prefix}outcome_code),
        lower({prefix}maker),
        lower({prefix}taker),
        toUInt8({prefix}side_code),
        {prefix}price,
        {prefix}size,
        toUInt64({prefix}block_number),
        lower({prefix}order_hash),
        lower(toString({prefix}contract)),
        {prefix}maker_amount,
        {prefix}taker_amount,
        {prefix}fee
    )"""


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid decimal validation value: {value!r}") from exc


def _conserved(left: Any, right: Any) -> bool:
    return abs(_decimal(left) - _decimal(right)) <= CONSERVATION_ABS_TOLERANCE


@dataclass(frozen=True)
class ChunkResult:
    start_block: int
    end_block: int
    table_results: dict[str, dict[str, Any]]
    elapsed_sec: float


def quote_ch(value: Any) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def table_exists(client: ClickHouseClient, table: str) -> bool:
    table_name = safe_identifier(table)
    value = client.query_scalar(
        f"""
        SELECT count()
        FROM system.tables
        WHERE database = currentDatabase()
          AND name = {quote_ch(table_name)}
        """,
        timeout_seconds=30,
    )
    return str(value).strip() not in {"", "0"}


def scalar_int(
    client: ClickHouseClient, query: str, *, timeout_seconds: int = 120
) -> int:
    value = client.query_scalar(query, timeout_seconds=timeout_seconds)
    text = str(value).strip()
    return int(text or "0")


def query_one(
    client: ClickHouseClient, query: str, *, timeout_seconds: int = 120
) -> dict[str, Any]:
    rows = client.query_json_rows(query, timeout_seconds=timeout_seconds)
    return rows[0] if rows else {}


def validate_trusted_block_time_coverage(
    client: ClickHouseClient,
    start_block: int,
    end_block: int,
) -> dict[str, Any]:
    """Require one conflict-free RPC header timestamp for every source block."""

    coverage = query_one(
        client,
        f"""
        WITH
            source_blocks AS
            (
                SELECT DISTINCT block_number
                FROM orderfilled_fact
                WHERE block_number BETWEEN {start_block} AND {end_block}
            ),
            trusted AS
            (
                SELECT
                    block_number,
                    argMax(block_time, tuple(ingested_at, source)) AS block_time,
                    argMax(block_hash, tuple(ingested_at, source)) AS block_hash
                FROM block_timestamps
                WHERE block_number BETWEEN {start_block} AND {end_block}
                  AND {TRUSTED_BLOCK_TIME_SOURCE_SQL}
                GROUP BY block_number
            )
        SELECT
            (SELECT count() FROM source_blocks) AS source_blocks,
            (SELECT count() FROM trusted) AS trusted_timestamp_blocks,
            (SELECT count() FROM source_blocks LEFT ANTI JOIN trusted USING block_number) AS missing_timestamp_blocks,
            (SELECT countIf(block_time <= toDateTime64('2000-01-01 00:00:00', 0, 'UTC')) FROM trusted) AS invalid_timestamp_blocks,
            (SELECT countIf(length(block_hash) = 0) FROM trusted) AS missing_hash_blocks
        """,
        timeout_seconds=600,
    )
    conflicts = scalar_int(
        client,
        f"""
        SELECT count()
        FROM
        (
            SELECT block_number
            FROM block_timestamps
            WHERE block_number BETWEEN {start_block} AND {end_block}
              AND {TRUSTED_BLOCK_TIME_SOURCE_SQL}
            GROUP BY block_number
            HAVING uniqExact(block_time) > 1 OR uniqExact(block_hash) > 1
        )
        """,
        timeout_seconds=600,
    )
    result = {
        "source_blocks": int(coverage.get("source_blocks") or 0),
        "trusted_timestamp_blocks": int(coverage.get("trusted_timestamp_blocks") or 0),
        "missing_timestamp_blocks": int(coverage.get("missing_timestamp_blocks") or 0),
        "invalid_timestamp_blocks": int(coverage.get("invalid_timestamp_blocks") or 0),
        "missing_hash_blocks": int(coverage.get("missing_hash_blocks") or 0),
        "conflicting_timestamp_blocks": conflicts,
    }
    result["status"] = (
        "ready"
        if result["source_blocks"] > 0
        and result["missing_timestamp_blocks"] == 0
        and result["invalid_timestamp_blocks"] == 0
        and result["missing_hash_blocks"] == 0
        and result["conflicting_timestamp_blocks"] == 0
        else "incomplete"
    )
    return result


def validate_source_identity_quality(
    client: ClickHouseClient,
    start_block: int,
    end_block: int,
) -> dict[str, Any]:
    """Audit physical duplicates before canonicalizing the shared source.

    Exact physical replays are permitted and collapsed.  A seven-field fill
    identity carrying more than one immutable payload, or one tx/log identity
    expanding to multiple fill identities, is a hard conflict.
    """

    identity = _fill_identity_group_by()
    payload = _immutable_payload_tuple()
    summary = query_one(
        client,
        f"""
        SELECT
            sum(physical_rows) AS physical_source_rows,
            count() AS canonical_source_rows,
            sum(physical_rows - 1) AS duplicate_extra_rows,
            countIf(immutable_payload_versions > 1) AS immutable_payload_conflict_identities
        FROM
        (
            SELECT
                count() AS physical_rows,
                uniqExact({payload}) AS immutable_payload_versions
            FROM orderfilled_fact
            WHERE block_number BETWEEN {start_block} AND {end_block}
            GROUP BY {identity}
        )
        """,
        timeout_seconds=900,
    )
    tx_log_conflicts = scalar_int(
        client,
        f"""
        SELECT count()
        FROM
        (
            SELECT lower(tx_hash), log_index
            FROM orderfilled_fact
            WHERE block_number BETWEEN {start_block} AND {end_block}
            GROUP BY lower(tx_hash), log_index
            HAVING uniqExact(tuple(
                market_id,
                lower(token_id),
                lower(maker),
                lower(taker),
                side_code
            )) > 1
        )
        """,
        timeout_seconds=900,
    )
    result = {
        "physical_source_rows": int(summary.get("physical_source_rows") or 0),
        "canonical_source_rows": int(summary.get("canonical_source_rows") or 0),
        "duplicate_extra_rows": int(summary.get("duplicate_extra_rows") or 0),
        "immutable_payload_conflict_identities": int(
            summary.get("immutable_payload_conflict_identities") or 0
        ),
        "tx_log_identity_conflicts": tx_log_conflicts,
    }
    result["status"] = (
        "ready"
        if result["canonical_source_rows"] > 0
        and result["immutable_payload_conflict_identities"] == 0
        and result["tx_log_identity_conflicts"] == 0
        else "conflict"
    )
    return result


def resolve_start_block(client: ClickHouseClient, from_date: str) -> int:
    return scalar_int(
        client,
        f"""
        SELECT toUInt64(min(block_number))
        FROM block_timestamps
        WHERE block_time >= toDateTime64({quote_ch(from_date)}, 0, 'UTC')
        """,
        timeout_seconds=120,
    )


def resolve_end_block(client: ClickHouseClient) -> int:
    return scalar_int(
        client,
        "SELECT toUInt64(max(block_number)) FROM orderfilled_fact",
        timeout_seconds=120,
    )


def ensure_tables(client: ClickHouseClient) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS market_asset_map
        (
            chain_id UInt64,
            market_id UInt64,
            condition_id String,
            asset_id String,
            outcome LowCardinality(String),
            canonical_side LowCardinality(String),
            token_decimals UInt8 DEFAULT 6,
            usdc_decimals UInt8 DEFAULT 6,
            tick_size Nullable(Decimal(20, 10)),
            min_order_size Nullable(Decimal(30, 10)),
            market_slug String DEFAULT '',
            market_title String DEFAULT '',
            category String DEFAULT '',
            start_time Nullable(DateTime64(0, 'UTC')),
            close_time Nullable(DateTime64(0, 'UTC')),
            resolution_time Nullable(DateTime64(0, 'UTC')),
            winner_asset_id String DEFAULT '',
            active Nullable(Bool),
            closed Nullable(Bool),
            metadata_source LowCardinality(String),
            first_block UInt64,
            last_block UInt64,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (chain_id, asset_id)
        SETTINGS index_granularity = 8192
        """,
        """
        CREATE TABLE IF NOT EXISTS raw_orderfilled
        (
            chain_id UInt64,
            contract_address String,
            block_number UInt64,
            block_time DateTime64(0, 'UTC'),
            block_time_source LowCardinality(String),
            tx_hash String,
            tx_index UInt32,
            tx_index_source LowCardinality(String),
            log_index UInt32,
            order_hash String,
            maker String,
            taker String,
            maker_asset_id String,
            taker_asset_id String,
            asset_id_source LowCardinality(String),
            maker_amount_filled_raw Decimal(38, 0),
            taker_amount_filled_raw Decimal(38, 0),
            fee_raw Decimal(38, 0),
            source_price Decimal(20, 10),
            source_size Decimal(30, 10),
            source_side_code UInt8,
            market_id UInt64,
            condition_id String,
            outcome_code UInt8,
            raw_json String DEFAULT '',
            inserted_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(block_time)
        ORDER BY (chain_id, block_number, tx_hash, log_index)
        SETTINGS index_granularity = 8192
        """,
        """
        CREATE TABLE IF NOT EXISTS maker_fill_ticks
        (
            fill_id String,
            chain_id UInt64,
            block_number UInt64,
            block_time DateTime64(0, 'UTC'),
            block_time_source LowCardinality(String),
            tx_hash String,
            tx_index UInt32,
            tx_index_source LowCardinality(String),
            log_index UInt32,
            order_hash String,
            market_id UInt64,
            condition_id String,
            asset_id String,
            outcome LowCardinality(String),
            price Decimal(20, 10),
            size_shares Decimal(30, 10),
            notional_usdc Decimal(38, 10),
            passive_side LowCardinality(String),
            aggressor_side LowCardinality(String),
            maker String,
            taker String,
            fee_usdc Decimal(38, 10) DEFAULT 0,
            source LowCardinality(String) DEFAULT 'orderfilled_fact',
            confidence LowCardinality(String) DEFAULT 'high',
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(block_time)
        ORDER BY (chain_id, market_id, asset_id, block_number, tx_index, log_index, tx_hash, fill_id)
        SETTINGS index_granularity = 8192
        """,
        """
        CREATE TABLE IF NOT EXISTS orderfilled_quarantine
        (
            chain_id UInt64,
            block_number UInt64,
            block_time Nullable(DateTime64(0, 'UTC')),
            tx_hash String,
            log_index UInt32,
            order_hash String,
            market_id UInt64,
            condition_id String,
            asset_id String,
            reason LowCardinality(String),
            raw_json String DEFAULT '',
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        PARTITION BY intDiv(block_number, 1000000)
        ORDER BY (chain_id, block_number, tx_hash, log_index, reason)
        SETTINGS index_granularity = 8192
        """,
        """
        CREATE TABLE IF NOT EXISTS trade_prints_one_sided
        (
            trade_id String,
            chain_id UInt64,
            block_number UInt64,
            block_time DateTime64(0, 'UTC'),
            tx_hash String,
            tx_index UInt32,
            tx_index_source LowCardinality(String),
            trade_group_id String,
            market_id UInt64,
            condition_id String,
            asset_id String,
            outcome LowCardinality(String),
            price Decimal(20, 10),
            size_shares Decimal(38, 10),
            notional_usdc Decimal(38, 10),
            aggressor_side LowCardinality(String),
            passive_side LowCardinality(String),
            source_fill_ids Array(String),
            source_order_hashes Array(String),
            source_log_indexes Array(UInt32),
            source_fill_count UInt32,
            confidence LowCardinality(String) DEFAULT 'high',
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(block_time)
        ORDER BY (chain_id, market_id, asset_id, block_number, tx_index, tx_hash, price, aggressor_side, trade_id)
        SETTINGS index_granularity = 8192
        """,
        """
        CREATE TABLE IF NOT EXISTS block_trade_bars_sparse
        (
            chain_id UInt64,
            market_id UInt64,
            condition_id String,
            asset_id String,
            outcome LowCardinality(String),
            block_number UInt64,
            block_time_start DateTime64(0, 'UTC'),
            block_time_end DateTime64(0, 'UTC'),
            open_price Decimal(20, 10),
            high_price Decimal(20, 10),
            low_price Decimal(20, 10),
            close_price Decimal(20, 10),
            vwap_price Decimal(20, 10),
            volume_shares Decimal(38, 10),
            notional_usdc Decimal(38, 10),
            trade_count UInt64,
            buy_volume_shares Decimal(38, 10),
            sell_volume_shares Decimal(38, 10),
            buy_notional_usdc Decimal(38, 10),
            sell_notional_usdc Decimal(38, 10),
            buy_count UInt64,
            sell_count UInt64,
            first_trade_id String,
            last_trade_id String,
            source LowCardinality(String) DEFAULT 'orderfilled_one_sided',
            is_sparse Bool DEFAULT true,
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(block_time_start)
        ORDER BY (chain_id, market_id, asset_id, block_number)
        SETTINGS index_granularity = 8192
        """,
        """
        CREATE TABLE IF NOT EXISTS time_bars
        (
            chain_id UInt64,
            interval_minutes UInt16,
            market_id UInt64,
            condition_id String,
            asset_id String,
            outcome LowCardinality(String),
            window_start DateTime64(0, 'UTC'),
            window_end DateTime64(0, 'UTC'),
            open_price Decimal(20, 10),
            high_price Decimal(20, 10),
            low_price Decimal(20, 10),
            close_price Decimal(20, 10),
            vwap_price Decimal(20, 10),
            volume_shares Decimal(38, 10),
            notional_usdc Decimal(38, 10),
            trade_count UInt64,
            buy_volume_shares Decimal(38, 10),
            sell_volume_shares Decimal(38, 10),
            buy_notional_usdc Decimal(38, 10),
            sell_notional_usdc Decimal(38, 10),
            buy_count UInt64,
            sell_count UInt64,
            first_trade_id String,
            last_trade_id String,
            source LowCardinality(String) DEFAULT 'orderfilled_one_sided',
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(window_start)
        ORDER BY (chain_id, interval_minutes, market_id, asset_id, window_start)
        SETTINGS index_granularity = 8192
        """,
        """
        CREATE TABLE IF NOT EXISTS orderfilled_v2_build_chunks
        (
            table_name LowCardinality(String),
            from_block UInt64,
            to_block UInt64,
            source_rows UInt64,
            output_rows UInt64,
            status LowCardinality(String),
            elapsed_sec Float64,
            build_tag String,
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY (table_name, from_block, to_block, created_at)
        SETTINGS index_granularity = 8192
        """,
    ]
    for statement in statements:
        client.execute(statement, timeout_seconds=120)


def insert_market_asset_map(
    client: ClickHouseClient,
    start_block: int,
    end_block: int,
    *,
    chain_id: int,
    force: bool = False,
) -> dict[str, Any]:
    if force:
        client.execute(
            f"""
            ALTER TABLE market_asset_map DELETE
            WHERE chain_id = toUInt64({chain_id})
              AND (
                  first_block BETWEEN {start_block} AND {end_block}
                  OR last_block BETWEEN {start_block} AND {end_block}
              )
            SETTINGS mutations_sync = 1
            """,
            timeout_seconds=1800,
        )
    before = count_rows(
        client, "market_asset_map", start_block, end_block, block_column="last_block"
    )
    start = time.perf_counter()
    client.execute(
        f"""
        INSERT INTO market_asset_map
        SELECT
            toUInt64({chain_id}) AS chain_id,
            any(market_id) AS market_id,
            if(
                countIf(length(condition_id) > 0) > 0,
                argMaxIf(condition_id, block_number, length(condition_id) > 0),
                ''
            ) AS condition_id,
            lower(token_id) AS asset_id,
            multiIf(any(outcome_code) = 1, 'YES', any(outcome_code) = 2, 'NO', concat('OUTCOME_', toString(any(outcome_code)))) AS outcome,
            outcome AS canonical_side,
            toUInt8(6) AS token_decimals,
            toUInt8(6) AS usdc_decimals,
            CAST(NULL, 'Nullable(Decimal(20, 10))') AS tick_size,
            CAST(NULL, 'Nullable(Decimal(30, 10))') AS min_order_size,
            '' AS market_slug,
            '' AS market_title,
            '' AS category,
            CAST(NULL, 'Nullable(DateTime64(0, \\'UTC\\'))') AS start_time,
            CAST(NULL, 'Nullable(DateTime64(0, \\'UTC\\'))') AS close_time,
            CAST(NULL, 'Nullable(DateTime64(0, \\'UTC\\'))') AS resolution_time,
            '' AS winner_asset_id,
            CAST(NULL, 'Nullable(Bool)') AS active,
            CAST(NULL, 'Nullable(Bool)') AS closed,
            'orderfilled_fact_distinct_asset' AS metadata_source,
            min(block_number) AS first_block,
            max(block_number) AS last_block,
            now() AS updated_at
        FROM orderfilled_fact
        WHERE block_number BETWEEN {start_block} AND {end_block}
          AND length(token_id) > 0
          AND lower(token_id) NOT IN
          (
              SELECT asset_id
              FROM market_asset_map
              WHERE chain_id = toUInt64({chain_id})
          )
        GROUP BY lower(token_id)
        """,
        timeout_seconds=1800,
    )
    after = count_rows(
        client, "market_asset_map", start_block, end_block, block_column="last_block"
    )
    return {
        "before_rows": before,
        "after_rows": after,
        "inserted_estimate": max(0, after - before),
        "elapsed_sec": round(time.perf_counter() - start, 3),
    }


def import_postgres_market_asset_map(
    client: ClickHouseClient,
    *,
    chain_id: int,
    batch_size: int = 50_000,
) -> dict[str, Any]:
    """Import authoritative token metadata from Postgres into ClickHouse.

    ``orderfilled_fact`` is the execution source, but it can have blank
    condition IDs.  The Postgres metadata table carries the token-to-market
    mapping needed by the document's ``market_asset_map`` contract.
    """

    inserted = 0
    started = time.perf_counter()
    columns = (
        "chain_id",
        "market_id",
        "condition_id",
        "asset_id",
        "outcome",
        "canonical_side",
        "token_decimals",
        "usdc_decimals",
        "tick_size",
        "min_order_size",
        "market_slug",
        "market_title",
        "category",
        "start_time",
        "close_time",
        "resolution_time",
        "winner_asset_id",
        "active",
        "closed",
        "metadata_source",
        "first_block",
        "last_block",
    )
    with postgres_connection(readonly=True) as conn:
        with conn.cursor(name="orderfilled_v2_market_asset_map") as cur:
            cur.itersize = batch_size
            cur.execute(
                """
                SELECT
                    market_id,
                    COALESCE(condition_id, '') AS condition_id,
                    lower(COALESCE(token_id_hex, '')) AS asset_id,
                    upper(COALESCE(token_side, '')) AS outcome,
                    COALESCE(market_slug, '') AS market_slug,
                    COALESCE(market_title, '') AS market_title,
                    active,
                    closed
                FROM quant.market_token_metadata
                WHERE token_id_hex IS NOT NULL
                  AND token_id_hex <> ''
                """
            )
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                payload_lines = []
                for row in rows:
                    outcome = str(row["outcome"] or "").upper()
                    if outcome not in {"YES", "NO"}:
                        outcome = "OUTCOME_UNKNOWN"
                    payload = {
                        "chain_id": chain_id,
                        "market_id": int(row["market_id"] or 0),
                        "condition_id": str(row["condition_id"] or ""),
                        "asset_id": str(row["asset_id"] or "").lower(),
                        "outcome": outcome,
                        "canonical_side": outcome,
                        "token_decimals": 6,
                        "usdc_decimals": 6,
                        "tick_size": None,
                        "min_order_size": None,
                        "market_slug": str(row["market_slug"] or ""),
                        "market_title": str(row["market_title"] or ""),
                        "category": "",
                        "start_time": None,
                        "close_time": None,
                        "resolution_time": None,
                        "winner_asset_id": "",
                        "active": row["active"],
                        "closed": row["closed"],
                        "metadata_source": "postgres_quant_market_token_metadata",
                        "first_block": 0,
                        "last_block": 0,
                    }
                    payload_lines.append(
                        json.dumps(payload, ensure_ascii=False, default=str)
                    )
                client.execute(
                    "INSERT INTO market_asset_map ("
                    + ", ".join(columns)
                    + ") FORMAT JSONEachRow",
                    stdin="\n".join(payload_lines) + "\n",
                    timeout_seconds=300,
                )
                inserted += len(rows)
    return {
        "status": "inserted",
        "rows": inserted,
        "elapsed_sec": round(time.perf_counter() - started, 3),
    }


def source_with_time_cte(start_block: int, end_block: int) -> str:
    return f"""
        WITH canonical_source AS
        (
            SELECT
                tupleElement(selected, 1) AS tx_hash,
                tupleElement(selected, 2) AS log_index,
                tupleElement(selected, 3) AS market_id,
                tupleElement(selected, 4) AS condition_id,
                tupleElement(selected, 5) AS token_id,
                tupleElement(selected, 6) AS outcome_code,
                tupleElement(selected, 7) AS maker,
                tupleElement(selected, 8) AS taker,
                tupleElement(selected, 9) AS side_code,
                tupleElement(selected, 10) AS price,
                tupleElement(selected, 11) AS size,
                tupleElement(selected, 12) AS block_number,
                tupleElement(selected, 13) AS order_hash,
                tupleElement(selected, 14) AS contract,
                tupleElement(selected, 15) AS maker_amount,
                tupleElement(selected, 16) AS taker_amount,
                tupleElement(selected, 17) AS fee,
                tupleElement(selected, 18) AS ingested_at
            FROM
            (
                SELECT
                    argMax(
                        tuple(
                            tx_hash,
                            log_index,
                            market_id,
                            condition_id,
                            token_id,
                            outcome_code,
                            maker,
                            taker,
                            side_code,
                            price,
                            size,
                            block_number,
                            order_hash,
                            contract,
                            maker_amount,
                            taker_amount,
                            fee,
                            ingested_at
                        ),
                        ingested_at
                    ) AS selected
                FROM orderfilled_fact
                WHERE block_number BETWEEN {start_block} AND {end_block}
                GROUP BY {_fill_identity_group_by()}
            )
        )
        SELECT
            f.*,
            bt.block_time AS v2_block_time,
            concat('block_timestamps:', bt.trusted_source) AS v2_block_time_source
        FROM canonical_source AS f
        INNER JOIN
        (
            SELECT
                block_number,
                argMax(block_time, tuple(ingested_at, source)) AS block_time,
                argMax(source, tuple(ingested_at, source)) AS trusted_source
            FROM block_timestamps
            WHERE block_number BETWEEN {start_block} AND {end_block}
              AND {TRUSTED_BLOCK_TIME_SOURCE_SQL}
            GROUP BY block_number
        ) AS bt ON bt.block_number = f.block_number
    """


def insert_raw_orderfilled(
    client: ClickHouseClient, start_block: int, end_block: int, *, chain_id: int
) -> None:
    client.execute(
        f"""
        INSERT INTO raw_orderfilled
        SELECT
            toUInt64({chain_id}) AS chain_id,
            lower(contract) AS contract_address,
            block_number,
            v2_block_time AS block_time,
            v2_block_time_source AS block_time_source,
            lower(tx_hash) AS tx_hash,
            toUInt32(0) AS tx_index,
            'missing_in_orderfilled_fact' AS tx_index_source,
            toUInt32(log_index) AS log_index,
            lower(order_hash) AS order_hash,
            lower(maker) AS maker,
            lower(taker) AS taker,
            multiIf(side_code = 1, lower(token_id), side_code = 2, '0', '') AS maker_asset_id,
            multiIf(side_code = 1, '0', side_code = 2, lower(token_id), '') AS taker_asset_id,
            'derived_from_aggressor_side_code' AS asset_id_source,
            toDecimal128(ifNull(maker_amount, 0), 0) AS maker_amount_filled_raw,
            toDecimal128(ifNull(taker_amount, 0), 0) AS taker_amount_filled_raw,
            toDecimal128(ifNull(fee, 0), 0) AS fee_raw,
            price AS source_price,
            size AS source_size,
            toUInt8(side_code) AS source_side_code,
            market_id,
            condition_id,
            outcome_code,
            '' AS raw_json,
            now() AS inserted_at
        FROM ({source_with_time_cte(start_block, end_block)})
        """,
        timeout_seconds=1800,
    )


def insert_maker_fill_ticks(
    client: ClickHouseClient, start_block: int, end_block: int, *, chain_id: int
) -> None:
    client.execute(
        f"""
        INSERT INTO maker_fill_ticks
        SELECT
            lower(hex(SHA256(concat('fill|', lower(f.tx_hash), '|', toString(f.log_index), '|', toString(f.market_id), '|', lower(f.token_id), '|', lower(f.maker), '|', lower(f.taker), '|', toString(f.side_code))))) AS fill_id,
            toUInt64({chain_id}) AS chain_id,
            f.block_number AS block_number,
            f.v2_block_time AS block_time,
            f.v2_block_time_source AS block_time_source,
            lower(f.tx_hash) AS tx_hash,
            toUInt32(0) AS tx_index,
            'missing_in_orderfilled_fact' AS tx_index_source,
            toUInt32(f.log_index) AS log_index,
            lower(f.order_hash) AS order_hash,
            f.market_id AS market_id,
            if(length(f.condition_id) > 0, f.condition_id, m.condition_id) AS condition_id,
            lower(f.token_id) AS asset_id,
            m.outcome AS outcome,
            f.price AS price,
            f.size AS size_shares,
            toDecimal128(toFloat64(f.price) * toFloat64(f.size), 10) AS notional_usdc,
            multiIf(f.side_code = 1, 'SELL', f.side_code = 2, 'BUY', '') AS passive_side,
            multiIf(f.side_code = 1, 'BUY', f.side_code = 2, 'SELL', '') AS aggressor_side,
            lower(f.maker) AS maker,
            lower(f.taker) AS taker,
            toDecimal128(toFloat64(ifNull(f.fee, 0)) / 1000000.0, 10) AS fee_usdc,
            'orderfilled_fact' AS source,
            'high' AS confidence,
            now() AS created_at
        FROM ({source_with_time_cte(start_block, end_block)}) AS f
        INNER JOIN
        (
            SELECT
                chain_id,
                asset_id,
                if(
                    countIf(length(condition_id) > 0) > 0,
                    argMaxIf(condition_id, updated_at, length(condition_id) > 0),
                    ''
                ) AS condition_id,
                if(
                    countIf(outcome IN ('YES', 'NO')) > 0,
                    argMaxIf(outcome, updated_at, outcome IN ('YES', 'NO')),
                    any(outcome)
                ) AS outcome
            FROM market_asset_map
            WHERE chain_id = toUInt64({chain_id})
            GROUP BY chain_id, asset_id
        ) AS m
            ON m.chain_id = toUInt64({chain_id})
           AND m.asset_id = lower(f.token_id)
        WHERE f.price > 0
          AND f.price <= 1
          AND f.size > 0
          AND f.side_code IN (1, 2)
          AND length(if(length(f.condition_id) > 0, f.condition_id, m.condition_id)) > 0
          AND length(f.tx_hash) > 0
          AND length(f.order_hash) > 0
          AND length(f.token_id) > 0
          AND length(f.maker) > 0
          AND length(f.taker) > 0
        """,
        timeout_seconds=1800,
    )


def insert_orderfilled_quarantine(
    client: ClickHouseClient, start_block: int, end_block: int, *, chain_id: int
) -> None:
    client.execute(
        f"""
        INSERT INTO orderfilled_quarantine
        SELECT
            toUInt64({chain_id}) AS chain_id,
            f.block_number AS block_number,
            f.v2_block_time AS block_time,
            lower(f.tx_hash) AS tx_hash,
            toUInt32(f.log_index) AS log_index,
            lower(f.order_hash) AS order_hash,
            f.market_id AS market_id,
            f.condition_id AS condition_id,
            lower(f.token_id) AS asset_id,
            multiIf(
                length(f.tx_hash) = 0 OR length(f.order_hash) = 0, 'missing_tx_or_order_hash',
                length(f.token_id) = 0, 'unknown_asset_id',
                isNull(m.asset_id), 'unknown_asset_id',
                length(if(length(f.condition_id) > 0, f.condition_id, ifNull(m.condition_id, ''))) = 0, 'unknown_condition_id',
                f.side_code NOT IN (1, 2), 'unsupported_side_code',
                f.size <= 0, 'zero_size',
                f.price <= 0 OR f.price > 1, 'price_out_of_range',
                length(f.maker) = 0 OR length(f.taker) = 0, 'missing_counterparty',
                'unknown'
            ) AS reason,
            '' AS raw_json,
            now() AS created_at
        FROM ({source_with_time_cte(start_block, end_block)}) AS f
        LEFT JOIN
        (
            SELECT
                chain_id,
                asset_id,
                if(
                    countIf(length(condition_id) > 0) > 0,
                    argMaxIf(condition_id, updated_at, length(condition_id) > 0),
                    ''
                ) AS condition_id
            FROM market_asset_map
            WHERE chain_id = toUInt64({chain_id})
            GROUP BY chain_id, asset_id
        ) AS m
            ON m.chain_id = toUInt64({chain_id})
           AND m.asset_id = lower(f.token_id)
        WHERE f.price <= 0
           OR f.price > 1
           OR f.size <= 0
           OR f.side_code NOT IN (1, 2)
           OR length(f.tx_hash) = 0
           OR length(f.order_hash) = 0
           OR length(f.token_id) = 0
           OR length(if(length(f.condition_id) > 0, f.condition_id, ifNull(m.condition_id, ''))) = 0
           OR length(f.maker) = 0
           OR length(f.taker) = 0
           OR isNull(m.asset_id)
        """,
        timeout_seconds=1800,
    )


def insert_trade_prints_one_sided(
    client: ClickHouseClient, start_block: int, end_block: int, *, chain_id: int
) -> None:
    client.execute(
        f"""
        INSERT INTO trade_prints_one_sided
        SELECT
            lower(hex(SHA256(concat('trade|', toString(chain_id), '|', toString(block_number), '|', lower(tx_hash), '|', asset_id, '|', aggressor_side, '|', passive_side, '|', toString(price))))) AS trade_id,
            chain_id,
            block_number,
            any(block_time) AS block_time,
            tx_hash,
            any(tx_index) AS tx_index,
            any(tx_index_source) AS tx_index_source,
            lower(tx_hash) AS trade_group_id,
            any(market_id) AS market_id,
            any(condition_id) AS condition_id,
            asset_id,
            any(outcome) AS outcome,
            price,
            toDecimal128(sum(size_shares), 10) AS size_shares,
            toDecimal128(sum(notional_usdc), 10) AS notional_usdc,
            aggressor_side,
            passive_side,
            arraySort(groupArray(fill_id)) AS source_fill_ids,
            arraySort(groupUniqArray(order_hash)) AS source_order_hashes,
            arraySort(groupArray(log_index)) AS source_log_indexes,
            toUInt32(count()) AS source_fill_count,
            'high' AS confidence,
            now() AS created_at
        FROM maker_fill_ticks
        WHERE chain_id = toUInt64({chain_id})
          AND block_number BETWEEN {start_block} AND {end_block}
        GROUP BY
            chain_id,
            block_number,
            tx_hash,
            asset_id,
            aggressor_side,
            passive_side,
            price
        """,
        timeout_seconds=1800,
    )


def insert_block_trade_bars_sparse(
    client: ClickHouseClient, start_block: int, end_block: int, *, chain_id: int
) -> None:
    client.execute(
        f"""
        INSERT INTO block_trade_bars_sparse
        (
            chain_id,
            market_id,
            condition_id,
            asset_id,
            outcome,
            block_number,
            block_time_start,
            block_time_end,
            open_price,
            high_price,
            low_price,
            close_price,
            vwap_price,
            volume_shares,
            notional_usdc,
            trade_count,
            buy_volume_shares,
            sell_volume_shares,
            buy_notional_usdc,
            sell_notional_usdc,
            buy_count,
            sell_count,
            first_trade_id,
            last_trade_id,
            source,
            is_sparse,
            created_at
        )
        SELECT
            chain_id,
            market_id,
            condition_id,
            asset_id,
            outcome,
            block_number,
            block_time_start,
            block_time_end,
            open_price,
            high_price,
            low_price,
            close_price,
            toDecimal128(sum_notional_usdc / nullIf(sum_size_shares, 0), 10) AS vwap_price,
            toDecimal128(sum_size_shares, 10) AS volume_shares,
            toDecimal128(sum_notional_usdc, 10) AS notional_usdc,
            trade_count,
            toDecimal128(buy_size_shares, 10) AS buy_volume_shares,
            toDecimal128(sell_size_shares, 10) AS sell_volume_shares,
            toDecimal128(buy_notional, 10) AS buy_notional_usdc,
            toDecimal128(sell_notional, 10) AS sell_notional_usdc,
            buy_count,
            sell_count,
            first_trade_id,
            last_trade_id,
            'orderfilled_one_sided' AS source,
            CAST(1, 'Bool') AS is_sparse,
            now() AS created_at
        FROM
        (
            SELECT
                chain_id,
                market_id,
                any(condition_id) AS condition_id,
                asset_id,
                any(outcome) AS outcome,
                block_number,
                min(block_time) AS block_time_start,
                max(block_time) AS block_time_end,
                argMin(price, tuple(tx_index, tx_hash, arrayMin(source_log_indexes), trade_id)) AS open_price,
                max(price) AS high_price,
                min(price) AS low_price,
                argMax(price, tuple(tx_index, tx_hash, arrayMax(source_log_indexes), trade_id)) AS close_price,
                sum(size_shares) AS sum_size_shares,
                sum(notional_usdc) AS sum_notional_usdc,
                count() AS trade_count,
                sumIf(size_shares, aggressor_side = 'BUY') AS buy_size_shares,
                sumIf(size_shares, aggressor_side = 'SELL') AS sell_size_shares,
                sumIf(notional_usdc, aggressor_side = 'BUY') AS buy_notional,
                sumIf(notional_usdc, aggressor_side = 'SELL') AS sell_notional,
                countIf(aggressor_side = 'BUY') AS buy_count,
                countIf(aggressor_side = 'SELL') AS sell_count,
                argMin(trade_id, tuple(tx_index, tx_hash, arrayMin(source_log_indexes), trade_id)) AS first_trade_id,
                argMax(trade_id, tuple(tx_index, tx_hash, arrayMax(source_log_indexes), trade_id)) AS last_trade_id
            FROM trade_prints_one_sided
            WHERE chain_id = toUInt64({chain_id})
              AND block_number BETWEEN {start_block} AND {end_block}
            GROUP BY chain_id, market_id, asset_id, block_number
        )
        """,
        timeout_seconds=1800,
    )


def insert_time_bars(
    client: ClickHouseClient,
    start_block: int,
    end_block: int,
    *,
    chain_id: int,
    interval_minutes: int,
) -> None:
    client.execute(
        f"""
        INSERT INTO time_bars
        (
            chain_id,
            interval_minutes,
            market_id,
            condition_id,
            asset_id,
            outcome,
            window_start,
            window_end,
            open_price,
            high_price,
            low_price,
            close_price,
            vwap_price,
            volume_shares,
            notional_usdc,
            trade_count,
            buy_volume_shares,
            sell_volume_shares,
            buy_notional_usdc,
            sell_notional_usdc,
            buy_count,
            sell_count,
            first_trade_id,
            last_trade_id,
            source,
            created_at
        )
        SELECT
            chain_id,
            toUInt16({interval_minutes}) AS interval_minutes,
            market_id,
            condition_id,
            asset_id,
            outcome,
            window_start,
            window_start + toIntervalMinute({interval_minutes}) AS window_end,
            open_price,
            high_price,
            low_price,
            close_price,
            toDecimal128(sum_notional_usdc / nullIf(sum_size_shares, 0), 10) AS vwap_price,
            toDecimal128(sum_size_shares, 10) AS volume_shares,
            toDecimal128(sum_notional_usdc, 10) AS notional_usdc,
            trade_count,
            toDecimal128(buy_size_shares, 10) AS buy_volume_shares,
            toDecimal128(sell_size_shares, 10) AS sell_volume_shares,
            toDecimal128(buy_notional, 10) AS buy_notional_usdc,
            toDecimal128(sell_notional, 10) AS sell_notional_usdc,
            buy_count,
            sell_count,
            first_trade_id,
            last_trade_id,
            'orderfilled_one_sided' AS source,
            now() AS created_at
        FROM
        (
            SELECT
                chain_id,
                market_id,
                any(condition_id) AS condition_id,
                asset_id,
                any(outcome) AS outcome,
                toStartOfInterval(block_time, toIntervalMinute({interval_minutes}), 'UTC') AS window_start,
                argMin(price, tuple(block_number, tx_index, tx_hash, arrayMin(source_log_indexes), trade_id)) AS open_price,
                max(price) AS high_price,
                min(price) AS low_price,
                argMax(price, tuple(block_number, tx_index, tx_hash, arrayMax(source_log_indexes), trade_id)) AS close_price,
                sum(size_shares) AS sum_size_shares,
                sum(notional_usdc) AS sum_notional_usdc,
                count() AS trade_count,
                sumIf(size_shares, aggressor_side = 'BUY') AS buy_size_shares,
                sumIf(size_shares, aggressor_side = 'SELL') AS sell_size_shares,
                sumIf(notional_usdc, aggressor_side = 'BUY') AS buy_notional,
                sumIf(notional_usdc, aggressor_side = 'SELL') AS sell_notional,
                countIf(aggressor_side = 'BUY') AS buy_count,
                countIf(aggressor_side = 'SELL') AS sell_count,
                argMin(trade_id, tuple(block_number, tx_index, tx_hash, arrayMin(source_log_indexes), trade_id)) AS first_trade_id,
                argMax(trade_id, tuple(block_number, tx_index, tx_hash, arrayMax(source_log_indexes), trade_id)) AS last_trade_id
            FROM trade_prints_one_sided
            WHERE chain_id = toUInt64({chain_id})
              AND block_number BETWEEN {start_block} AND {end_block}
            GROUP BY chain_id, market_id, asset_id, window_start
        )
        """,
        timeout_seconds=2400,
    )


INSERT_BY_TABLE = {
    "raw_orderfilled": insert_raw_orderfilled,
    "maker_fill_ticks": insert_maker_fill_ticks,
    "orderfilled_quarantine": insert_orderfilled_quarantine,
    "trade_prints_one_sided": insert_trade_prints_one_sided,
    "block_trade_bars_sparse": insert_block_trade_bars_sparse,
}


def count_rows(
    client: ClickHouseClient,
    table: str,
    start_block: int,
    end_block: int,
    *,
    block_column: str = "block_number",
) -> int:
    if not table_exists(client, table):
        return 0
    return scalar_int(
        client,
        f"""
        SELECT count()
        FROM {safe_identifier(table)}
        WHERE {safe_identifier(block_column)} BETWEEN {start_block} AND {end_block}
        """,
        timeout_seconds=120,
    )


def chunk_record_exists(
    client: ClickHouseClient,
    table_name: str,
    start_block: int,
    end_block: int,
    *,
    build_tag: str,
) -> bool:
    if not table_exists(client, "orderfilled_v2_build_chunks"):
        return False
    value = client.query_scalar(
        f"""
        SELECT count()
        FROM orderfilled_v2_build_chunks
        WHERE table_name = {quote_ch(table_name)}
          AND from_block = toUInt64({start_block})
          AND to_block = toUInt64({end_block})
          AND status = 'inserted'
          AND build_tag = {quote_ch(build_tag)}
        """,
        timeout_seconds=60,
    )
    return str(value).strip() not in {"", "0"}


def delete_chunk_record(
    client: ClickHouseClient,
    table_name: str,
    start_block: int,
    end_block: int,
    *,
    build_tag: str,
) -> None:
    if not table_exists(client, "orderfilled_v2_build_chunks"):
        return
    client.execute(
        f"""
        ALTER TABLE orderfilled_v2_build_chunks DELETE
        WHERE table_name = {quote_ch(table_name)}
          AND from_block = toUInt64({start_block})
          AND to_block = toUInt64({end_block})
          AND build_tag = {quote_ch(build_tag)}
        SETTINGS mutations_sync = 1
        """,
        timeout_seconds=300,
    )


def delete_all_chunk_records(
    client: ClickHouseClient,
    table_name: str,
    start_block: int,
    end_block: int,
) -> None:
    """Revoke every overlapping receipt before replacing derived rows.

    API readiness merges receipt intervals without a builder-version filter, so
    leaving a larger legacy receipt would keep a rebuilt subrange advertised.
    """

    if not table_exists(client, "orderfilled_v2_build_chunks"):
        return
    client.execute(
        f"""
        ALTER TABLE orderfilled_v2_build_chunks DELETE
        WHERE table_name = {quote_ch(table_name)}
          AND NOT (
              to_block < toUInt64({start_block})
              OR from_block > toUInt64({end_block})
          )
        SETTINGS mutations_sync = 1
        """,
        timeout_seconds=300,
    )


def record_chunk(
    client: ClickHouseClient,
    *,
    table_name: str,
    start_block: int,
    end_block: int,
    source_rows: int,
    output_rows: int,
    status: str,
    elapsed_sec: float,
    build_tag: str,
) -> None:
    client.execute(
        f"""
        INSERT INTO orderfilled_v2_build_chunks
        SELECT
            {quote_ch(table_name)} AS table_name,
            toUInt64({start_block}) AS from_block,
            toUInt64({end_block}) AS to_block,
            toUInt64({source_rows}) AS source_rows,
            toUInt64({output_rows}) AS output_rows,
            {quote_ch(status)} AS status,
            toFloat64({elapsed_sec:.6f}) AS elapsed_sec,
            {quote_ch(build_tag)} AS build_tag,
            now() AS created_at
        """,
        timeout_seconds=60,
    )


def backfill_chunk(
    client: ClickHouseClient,
    *,
    start_block: int,
    end_block: int,
    tables: Iterable[str],
    chain_id: int,
    force: bool,
    build_tag: str,
) -> ChunkResult:
    chunk_start = time.perf_counter()
    table_tuple = tuple(tables)
    current_receipts = {
        safe_identifier(table): chunk_record_exists(
            client,
            safe_identifier(table),
            start_block,
            end_block,
            build_tag=build_tag,
        )
        for table in table_tuple
    }
    if any(current_receipts.values()):
        existing_validation = validate_layers(
            client, start_block, end_block, chain_id=chain_id
        )
        if (
            not force
            and all(current_receipts.values())
            and existing_validation["status"] == "ready"
        ):
            return ChunkResult(
                start_block=start_block,
                end_block=end_block,
                table_results={
                    table: {
                        "status": "skipped_existing",
                        "before_rows": count_rows(
                            client, table, start_block, end_block
                        ),
                        "after_rows": count_rows(client, table, start_block, end_block),
                        "completion_record": True,
                    }
                    for table in current_receipts
                },
                elapsed_sec=round(time.perf_counter() - chunk_start, 3),
            )
        for table_name in current_receipts:
            delete_all_chunk_records(client, table_name, start_block, end_block)

    results: dict[str, dict[str, Any]] = {}
    for table in table_tuple:
        table_name = safe_identifier(table)
        before = count_rows(client, table_name, start_block, end_block)
        has_complete_record = chunk_record_exists(
            client, table_name, start_block, end_block, build_tag=build_tag
        )
        if force:
            delete_chunk_record(
                client, table_name, start_block, end_block, build_tag=build_tag
            )
            has_complete_record = False
        if has_complete_record and not force:
            results[table_name] = {
                "status": "skipped_existing",
                "before_rows": before,
                "after_rows": before,
                "completion_record": True,
            }
            continue
        if not has_complete_record:
            delete_all_chunk_records(client, table_name, start_block, end_block)
        if before and (force or not has_complete_record):
            client.execute(
                f"ALTER TABLE {table_name} DELETE WHERE block_number BETWEEN {start_block} AND {end_block} SETTINGS mutations_sync = 1",
                timeout_seconds=1800,
            )
        insert_fn = INSERT_BY_TABLE[table_name]
        start = time.perf_counter()
        insert_fn(client, start_block, end_block, chain_id=chain_id)
        elapsed = round(time.perf_counter() - start, 3)
        after = count_rows(client, table_name, start_block, end_block)
        inserted = max(0, after - (0 if force else before))
        results[table_name] = {
            "status": "inserted",
            "before_rows": before,
            "after_rows": after,
            "inserted_estimate": inserted,
            "elapsed_sec": elapsed,
        }
    return ChunkResult(
        start_block=start_block,
        end_block=end_block,
        table_results=results,
        elapsed_sec=round(time.perf_counter() - chunk_start, 3),
    )


def iter_chunks(
    start_block: int, end_block: int, chunk_blocks: int
) -> Iterable[tuple[int, int]]:
    current = start_block
    while current <= end_block:
        chunk_end = min(end_block, current + chunk_blocks - 1)
        yield current, chunk_end
        current = chunk_end + 1


def validate_layers(
    client: ClickHouseClient, start_block: int, end_block: int, *, chain_id: int
) -> dict[str, Any]:
    source_identity = validate_source_identity_quality(client, start_block, end_block)
    counts = {
        "source_orderfilled_fact": scalar_int(
            client,
            f"SELECT count() FROM orderfilled_fact WHERE block_number BETWEEN {start_block} AND {end_block}",
            timeout_seconds=600,
        ),
        "source_orderfilled_fact_canonical": int(
            source_identity["canonical_source_rows"]
        ),
        "source_duplicate_extra_rows": int(source_identity["duplicate_extra_rows"]),
        "raw_orderfilled": count_rows(
            client, "raw_orderfilled", start_block, end_block
        ),
        "maker_fill_ticks": count_rows(
            client, "maker_fill_ticks", start_block, end_block
        ),
        "orderfilled_quarantine": count_rows(
            client, "orderfilled_quarantine", start_block, end_block
        ),
        "trade_prints_one_sided": count_rows(
            client, "trade_prints_one_sided", start_block, end_block
        ),
        "block_trade_bars_sparse": count_rows(
            client, "block_trade_bars_sparse", start_block, end_block
        ),
    }
    detail = {
        "maker_quality": query_one(
            client,
            f"""
            SELECT
                countIf(price <= 0 OR price > 1) AS bad_price_rows,
                countIf(size_shares <= 0) AS bad_size_rows,
                countIf(notional_usdc <= 0) AS bad_notional_rows,
                countIf(passive_side NOT IN ('BUY', 'SELL')) AS bad_passive_side_rows,
                countIf(aggressor_side NOT IN ('BUY', 'SELL')) AS bad_aggressor_side_rows,
                countIf(length(asset_id) = 0 OR length(condition_id) = 0) AS missing_market_mapping_rows,
                countIf(tx_index_source != 'missing_in_orderfilled_fact') AS exact_tx_index_rows,
                countIf(NOT startsWith(block_time_source, 'block_timestamps:')) AS untrusted_block_time_rows,
                uniqExact(fill_id) AS unique_fill_ids,
                toString(sum(size_shares)) AS size_shares_sum,
                toString(sum(notional_usdc)) AS notional_usdc_sum
            FROM maker_fill_ticks
            WHERE chain_id = toUInt64({chain_id})
              AND block_number BETWEEN {start_block} AND {end_block}
            """,
            timeout_seconds=600,
        ),
        "trade_print_quality": query_one(
            client,
            f"""
            SELECT
                countIf(price <= 0 OR price > 1) AS bad_price_rows,
                countIf(size_shares <= 0) AS bad_size_rows,
                countIf(notional_usdc <= 0) AS bad_notional_rows,
                countIf(passive_side NOT IN ('BUY', 'SELL')) AS bad_passive_side_rows,
                countIf(aggressor_side NOT IN ('BUY', 'SELL')) AS bad_aggressor_side_rows,
                countIf(source_fill_count = 0 OR length(source_fill_ids) = 0) AS missing_source_fill_rows,
                countIf(
                    source_fill_count != length(source_fill_ids)
                    OR length(source_fill_ids) != arrayUniq(source_fill_ids)
                ) AS non_unique_source_fill_rows,
                uniqExact(trade_id) AS unique_trade_ids,
                sum(source_fill_count) AS source_fill_count_sum,
                toString(sum(size_shares)) AS size_shares_sum,
                toString(sum(notional_usdc)) AS notional_usdc_sum
            FROM trade_prints_one_sided
            WHERE chain_id = toUInt64({chain_id})
              AND block_number BETWEEN {start_block} AND {end_block}
            """,
            timeout_seconds=600,
        ),
        "trade_source_fill_quality": query_one(
            client,
            f"""
            SELECT
                count() AS flattened_source_fill_count,
                uniqExact(fill_id) AS unique_source_fill_ids
            FROM
            (
                SELECT arrayJoin(source_fill_ids) AS fill_id
                FROM trade_prints_one_sided
                WHERE chain_id = toUInt64({chain_id})
                  AND block_number BETWEEN {start_block} AND {end_block}
            )
            """,
            timeout_seconds=600,
        ),
        "maker_trade_reference_set_quality": query_one(
            client,
            f"""
            WITH
                maker_ids AS
                (
                    SELECT DISTINCT fill_id
                    FROM maker_fill_ticks
                    WHERE chain_id = toUInt64({chain_id})
                      AND block_number BETWEEN {start_block} AND {end_block}
                ),
                trade_refs AS
                (
                    SELECT DISTINCT arrayJoin(source_fill_ids) AS fill_id
                    FROM trade_prints_one_sided
                    WHERE chain_id = toUInt64({chain_id})
                      AND block_number BETWEEN {start_block} AND {end_block}
                )
            SELECT
                (SELECT count() FROM maker_ids LEFT ANTI JOIN trade_refs USING fill_id)
                    AS maker_ids_missing_from_trade,
                (SELECT count() FROM trade_refs LEFT ANTI JOIN maker_ids USING fill_id)
                    AS unknown_trade_source_fill_ids
            """,
            timeout_seconds=600,
        ),
        "trade_bar_key_quality": query_one(
            client,
            f"""
            WITH
                trade_keys AS
                (
                    SELECT DISTINCT chain_id, market_id, asset_id, block_number
                    FROM trade_prints_one_sided
                    WHERE chain_id = toUInt64({chain_id})
                      AND block_number BETWEEN {start_block} AND {end_block}
                ),
                bar_keys AS
                (
                    SELECT DISTINCT chain_id, market_id, asset_id, block_number
                    FROM block_trade_bars_sparse
                    WHERE chain_id = toUInt64({chain_id})
                      AND block_number BETWEEN {start_block} AND {end_block}
                )
            SELECT
                (
                    SELECT count()
                    FROM trade_keys
                    LEFT ANTI JOIN bar_keys USING (chain_id, market_id, asset_id, block_number)
                ) AS trade_keys_missing_from_bars,
                (
                    SELECT count()
                    FROM bar_keys
                    LEFT ANTI JOIN trade_keys USING (chain_id, market_id, asset_id, block_number)
                ) AS unknown_bar_keys
            """,
            timeout_seconds=600,
        ),
        "bar_quality": query_one(
            client,
            f"""
            SELECT
                countIf(volume_shares <= 0) AS non_positive_volume_rows,
                countIf(trade_count <= 0) AS non_positive_trade_count_rows,
                countIf(volume_shares != buy_volume_shares + sell_volume_shares)
                    AS side_volume_mismatch_rows,
                countIf(notional_usdc != buy_notional_usdc + sell_notional_usdc)
                    AS side_notional_mismatch_rows,
                countIf(trade_count != buy_count + sell_count) AS side_count_mismatch_rows,
                uniqExact(tuple(chain_id, market_id, asset_id, block_number)) AS unique_bar_keys,
                sum(trade_count) AS trade_count_sum,
                toString(sum(volume_shares)) AS volume_shares_sum,
                toString(sum(notional_usdc)) AS notional_usdc_sum
            FROM block_trade_bars_sparse
            WHERE chain_id = toUInt64({chain_id})
              AND block_number BETWEEN {start_block} AND {end_block}
            """,
            timeout_seconds=600,
        ),
        "source_identity": source_identity,
    }
    maker_quality = detail["maker_quality"]
    trade_quality = detail["trade_print_quality"]
    trade_source_fill_quality = detail["trade_source_fill_quality"]
    maker_trade_reference_set_quality = detail["maker_trade_reference_set_quality"]
    trade_bar_key_quality = detail["trade_bar_key_quality"]
    bar_quality = detail["bar_quality"]
    checks = {
        "source_identity_payload_conflicts_absent": int(
            source_identity["immutable_payload_conflict_identities"]
        )
        == 0,
        "source_tx_log_identity_conflicts_absent": int(
            source_identity["tx_log_identity_conflicts"]
        )
        == 0,
        "raw_matches_source": counts["raw_orderfilled"]
        == counts["source_orderfilled_fact_canonical"],
        "maker_plus_quarantine_matches_raw": counts["maker_fill_ticks"]
        + counts["orderfilled_quarantine"]
        == counts["raw_orderfilled"],
        "maker_fill_ids_unique": int(maker_quality.get("unique_fill_ids") or 0)
        == counts["maker_fill_ticks"],
        "trade_prints_not_more_than_maker_ticks": counts["trade_prints_one_sided"]
        <= counts["maker_fill_ticks"],
        "trade_ids_unique": int(trade_quality.get("unique_trade_ids") or 0)
        == counts["trade_prints_one_sided"],
        "trade_source_fill_ids_unique_within_rows": str(
            trade_quality.get("non_unique_source_fill_rows", "0")
        )
        == "0",
        "trade_source_fill_ids_unique_globally": int(
            trade_source_fill_quality.get("unique_source_fill_ids") or 0
        )
        == counts["maker_fill_ticks"],
        "trade_source_fill_flattened_count_matches_maker": int(
            trade_source_fill_quality.get("flattened_source_fill_count") or 0
        )
        == counts["maker_fill_ticks"],
        "maker_trade_reference_sets_equal": str(
            maker_trade_reference_set_quality.get("maker_ids_missing_from_trade", "0")
        )
        == "0"
        and str(
            maker_trade_reference_set_quality.get("unknown_trade_source_fill_ids", "0")
        )
        == "0",
        "bars_not_more_than_trade_prints": counts["block_trade_bars_sparse"]
        <= counts["trade_prints_one_sided"],
        "bar_keys_unique": int(bar_quality.get("unique_bar_keys") or 0)
        == counts["block_trade_bars_sparse"],
        "trade_bar_key_sets_equal": str(
            trade_bar_key_quality.get("trade_keys_missing_from_bars", "0")
        )
        == "0"
        and str(trade_bar_key_quality.get("unknown_bar_keys", "0")) == "0",
        "trade_print_source_fill_sum_matches_maker": str(
            trade_quality.get("source_fill_count_sum", "0")
        )
        == str(counts["maker_fill_ticks"]),
        "maker_to_trade_size_conserved": _conserved(
            maker_quality.get("size_shares_sum"), trade_quality.get("size_shares_sum")
        ),
        "maker_to_trade_notional_conserved": _conserved(
            maker_quality.get("notional_usdc_sum"),
            trade_quality.get("notional_usdc_sum"),
        ),
        "trade_to_bar_size_conserved": _conserved(
            trade_quality.get("size_shares_sum"), bar_quality.get("volume_shares_sum")
        ),
        "trade_to_bar_notional_conserved": _conserved(
            trade_quality.get("notional_usdc_sum"), bar_quality.get("notional_usdc_sum")
        ),
        "trade_to_bar_count_conserved": str(bar_quality.get("trade_count_sum", "0"))
        == str(counts["trade_prints_one_sided"]),
        "maker_rows_use_trusted_block_time": str(
            maker_quality.get("untrusted_block_time_rows", "0")
        )
        == "0",
        "maker_quality_rows_clean": all(
            str(maker_quality.get(field, "0")) == "0"
            for field in (
                "bad_price_rows",
                "bad_size_rows",
                "bad_notional_rows",
                "bad_passive_side_rows",
                "bad_aggressor_side_rows",
                "missing_market_mapping_rows",
            )
        ),
        "trade_quality_rows_clean": all(
            str(trade_quality.get(field, "0")) == "0"
            for field in (
                "bad_price_rows",
                "bad_size_rows",
                "bad_notional_rows",
                "bad_passive_side_rows",
                "bad_aggressor_side_rows",
                "missing_source_fill_rows",
                "non_unique_source_fill_rows",
            )
        ),
        "bar_quality_rows_clean": all(
            str(bar_quality.get(field, "0")) == "0"
            for field in (
                "non_positive_volume_rows",
                "non_positive_trade_count_rows",
                "side_volume_mismatch_rows",
                "side_notional_mismatch_rows",
                "side_count_mismatch_rows",
            )
        ),
    }
    passed = all(checks.values())
    return {
        "status": "ready" if passed else "incomplete",
        "from_block": start_block,
        "to_block": end_block,
        "counts": counts,
        "checks": checks,
        "detail": detail,
    }


def recover_validated_existing_chunk(
    client: ClickHouseClient,
    *,
    start_block: int,
    end_block: int,
    tables: Iterable[str],
    chain_id: int,
) -> tuple[ChunkResult, dict[str, Any]] | None:
    """Recover a fully written chunk whose completion receipts were not published."""

    table_results: dict[str, dict[str, Any]] = {}
    has_existing_rows = False
    for table in tables:
        table_name = safe_identifier(table)
        rows = count_rows(client, table_name, start_block, end_block)
        has_existing_rows = has_existing_rows or rows > 0
        table_results[table_name] = {
            "status": "recovered_existing",
            "before_rows": rows,
            "after_rows": rows,
            "elapsed_sec": 0.0,
        }
    if not has_existing_rows:
        return None

    validation = validate_layers(client, start_block, end_block, chain_id=chain_id)
    if validation.get("status") != "ready":
        return None
    return (
        ChunkResult(
            start_block=start_block,
            end_block=end_block,
            table_results=table_results,
            elapsed_sec=0.0,
        ),
        validation,
    )


def publish_validated_chunk_records(
    client: ClickHouseClient,
    *,
    result: ChunkResult,
    validation: dict[str, Any],
    build_tag: str,
) -> list[str]:
    """Publish per-table receipts only after the entire chunk is valid.

    The trade-tape receipt is deliberately inserted last because API coverage
    is derived from that table's receipt.  A receipt write failure before that
    point therefore cannot publish a partially validated interval.
    """

    if validation.get("status") != "ready":
        return []
    source_rows = int(validation["counts"]["source_orderfilled_fact_canonical"])
    publication_order = (
        "raw_orderfilled",
        "maker_fill_ticks",
        "orderfilled_quarantine",
        "block_trade_bars_sparse",
        "trade_prints_one_sided",
    )
    published: list[str] = []
    for table_name in publication_order:
        table_result = result.table_results.get(table_name)
        if not table_result or table_result.get("status") not in {
            "inserted",
            "recovered_existing",
        }:
            continue
        if chunk_record_exists(
            client,
            table_name,
            result.start_block,
            result.end_block,
            build_tag=build_tag,
        ):
            continue
        delete_all_chunk_records(
            client,
            table_name,
            result.start_block,
            result.end_block,
        )
        record_chunk(
            client,
            table_name=table_name,
            start_block=result.start_block,
            end_block=result.end_block,
            source_rows=source_rows,
            output_rows=int(table_result.get("after_rows") or 0),
            status="inserted",
            elapsed_sec=float(table_result.get("elapsed_sec") or 0.0),
            build_tag=build_tag,
        )
        published.append(table_name)
    return published


def backfill_time_bars(
    client: ClickHouseClient,
    start_block: int,
    end_block: int,
    *,
    chain_id: int,
    force: bool,
    build_tag: str,
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for interval in TIME_BAR_INTERVALS:
        before = scalar_int(
            client,
            f"""
            SELECT count()
            FROM time_bars
            WHERE chain_id = toUInt64({chain_id})
              AND interval_minutes = toUInt16({interval})
              AND window_start >= (
                  SELECT min(block_time)
                  FROM trade_prints_one_sided
                  WHERE block_number BETWEEN {start_block} AND {end_block}
              )
              AND window_start <= (
                  SELECT max(block_time)
                  FROM trade_prints_one_sided
                  WHERE block_number BETWEEN {start_block} AND {end_block}
              )
            """,
            timeout_seconds=300,
        )
        if before and not force:
            results[str(interval)] = {
                "status": "skipped_existing",
                "before_rows": before,
                "after_rows": before,
            }
            continue
        if before and force:
            client.execute(
                f"""
                ALTER TABLE time_bars DELETE
                WHERE chain_id = toUInt64({chain_id})
                  AND interval_minutes = toUInt16({interval})
                  AND window_start >= (
                      SELECT min(block_time)
                      FROM trade_prints_one_sided
                      WHERE block_number BETWEEN {start_block} AND {end_block}
                  )
                  AND window_start <= (
                      SELECT max(block_time)
                      FROM trade_prints_one_sided
                      WHERE block_number BETWEEN {start_block} AND {end_block}
                  )
                SETTINGS mutations_sync = 1
                """,
                timeout_seconds=1800,
            )
        start = time.perf_counter()
        insert_time_bars(
            client, start_block, end_block, chain_id=chain_id, interval_minutes=interval
        )
        after = scalar_int(
            client,
            f"SELECT count() FROM time_bars WHERE chain_id = toUInt64({chain_id}) AND interval_minutes = toUInt16({interval})",
            timeout_seconds=300,
        )
        results[str(interval)] = {
            "status": "inserted",
            "before_rows": before,
            "total_rows_after": after,
            "elapsed_sec": round(time.perf_counter() - start, 3),
            "build_tag": build_tag,
        }
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build OrderFilled-only V2 data layers from ClickHouse orderfilled_fact."
    )
    parser.add_argument(
        "--from-date",
        default=DEFAULT_FROM_DATE,
        help="UTC lower bound used to resolve the first block.",
    )
    parser.add_argument("--from-block", type=int, help="Override lower bound block.")
    parser.add_argument(
        "--to-block",
        type=int,
        help="Override upper bound block. Defaults to max(orderfilled_fact.block_number).",
    )
    parser.add_argument("--chunk-blocks", type=int, default=DEFAULT_CHUNK_BLOCKS)
    parser.add_argument(
        "--max-chunks", type=int, help="Process only the first N chunks."
    )
    parser.add_argument("--chain-id", type=int, default=DEFAULT_CHAIN_ID)
    parser.add_argument("--build-tag", default="orderfilled_v2_2026_02_plus")
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=sorted(INSERT_BY_TABLE),
        default=list(DEFAULT_TABLES),
    )
    parser.add_argument("--ensure-only", action="store_true")
    parser.add_argument("--market-map-only", action="store_true")
    parser.add_argument(
        "--import-postgres-asset-map",
        action="store_true",
        help="Import quant.market_token_metadata into ClickHouse market_asset_map.",
    )
    parser.add_argument(
        "--time-bars",
        action="store_true",
        help="Build 1m and 5m time bars after trade prints exist.",
    )
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--block-time-preflight-only",
        action="store_true",
        help="Check exact RPC block-time completeness without reading or writing V2 derived layers.",
    )
    parser.add_argument(
        "--source-identity-preflight-only",
        action="store_true",
        help="Check canonical fill identity and immutable payload conflicts without writing V2 layers.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete target rows for each chunk before re-inserting.",
    )
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.chunk_blocks <= 0:
        raise SystemExit("--chunk-blocks must be positive")
    if args.chain_id != DEFAULT_CHAIN_ID:
        raise SystemExit(
            "orderfilled_fact has no chain_id column; this builder is restricted to Polygon chain 137"
        )
    client = ClickHouseClient()
    ensure_tables(client)
    start_block = int(args.from_block or resolve_start_block(client, args.from_date))
    end_block = int(args.to_block or resolve_end_block(client))
    if end_block < start_block:
        raise SystemExit("to-block must be >= from-block")
    report: dict[str, Any] = {
        "from_date": args.from_date,
        "from_block": start_block,
        "to_block": end_block,
        "chain_id": args.chain_id,
        "build_tag": args.build_tag,
        "builder_identity": builder_identity(),
    }
    if args.ensure_only:
        report["status"] = "ensured"
        print_report(report, json_mode=args.json)
        return 0
    if args.block_time_preflight_only:
        report["block_time_preflight"] = validate_trusted_block_time_coverage(
            client, start_block, end_block
        )
        print_report(report, json_mode=args.json)
        return 0 if report["block_time_preflight"]["status"] == "ready" else 3
    if args.source_identity_preflight_only:
        report["source_identity_preflight"] = validate_source_identity_quality(
            client, start_block, end_block
        )
        print_report(report, json_mode=args.json)
        return 0 if report["source_identity_preflight"]["status"] == "ready" else 4
    if args.validate_only:
        report["block_time_preflight"] = validate_trusted_block_time_coverage(
            client, start_block, end_block
        )
        report["source_identity_preflight"] = validate_source_identity_quality(
            client, start_block, end_block
        )
        report["validation"] = validate_layers(
            client, start_block, end_block, chain_id=args.chain_id
        )
        print_report(report, json_mode=args.json)
        return (
            0
            if report["block_time_preflight"]["status"] == "ready"
            and report["source_identity_preflight"]["status"] == "ready"
            and report["validation"]["status"] == "ready"
            else 2
        )

    report["block_time_preflight"] = validate_trusted_block_time_coverage(
        client, start_block, end_block
    )
    if report["block_time_preflight"]["status"] != "ready":
        print_report(report, json_mode=args.json)
        return 3
    report["source_identity_preflight"] = validate_source_identity_quality(
        client, start_block, end_block
    )
    if report["source_identity_preflight"]["status"] != "ready":
        print_report(report, json_mode=args.json)
        return 4

    report["market_asset_map"] = insert_market_asset_map(
        client, start_block, end_block, chain_id=args.chain_id, force=args.force
    )
    if args.import_postgres_asset_map:
        report["postgres_market_asset_map"] = import_postgres_market_asset_map(
            client, chain_id=args.chain_id
        )
    if args.market_map_only:
        print_report(report, json_mode=args.json)
        return 0

    chunk_results: list[dict[str, Any]] = []
    for index, (chunk_start, chunk_end) in enumerate(
        iter_chunks(start_block, end_block, args.chunk_blocks), start=1
    ):
        if args.max_chunks and index > args.max_chunks:
            break
        recovered = None
        if not args.force:
            recovered = recover_validated_existing_chunk(
                client,
                start_block=chunk_start,
                end_block=chunk_end,
                tables=args.tables,
                chain_id=args.chain_id,
            )
        if recovered is None:
            result = backfill_chunk(
                client,
                start_block=chunk_start,
                end_block=chunk_end,
                tables=args.tables,
                chain_id=args.chain_id,
                force=args.force,
                build_tag=args.build_tag,
            )
            chunk_validation = validate_layers(
                client, chunk_start, chunk_end, chain_id=args.chain_id
            )
        else:
            result, chunk_validation = recovered
        published_receipts = publish_validated_chunk_records(
            client,
            result=result,
            validation=chunk_validation,
            build_tag=args.build_tag,
        )
        chunk_payload = asdict(result)
        chunk_payload["validation"] = chunk_validation
        chunk_payload["published_receipts"] = published_receipts
        chunk_results.append(chunk_payload)
        print_report({"chunk": chunk_payload}, json_mode=True)
        sys.stdout.flush()
        if chunk_validation["status"] != "ready":
            report["chunks"] = chunk_results
            report["validation"] = chunk_validation
            print_report(report, json_mode=args.json)
            return 2
    report["chunks"] = chunk_results
    if args.time_bars:
        report["time_bars"] = backfill_time_bars(
            client,
            start_block,
            end_block,
            chain_id=args.chain_id,
            force=args.force,
            build_tag=args.build_tag,
        )
    report["validation"] = validate_layers(
        client, start_block, end_block, chain_id=args.chain_id
    )
    print_report(report, json_mode=args.json)
    return 0 if report["validation"]["status"] == "ready" else 2


def print_report(report: dict[str, Any], *, json_mode: bool) -> None:
    if json_mode:
        print(json.dumps(report, ensure_ascii=False, default=str, sort_keys=True))
        return
    print(json.dumps(report, ensure_ascii=False, default=str, indent=2, sort_keys=True))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
