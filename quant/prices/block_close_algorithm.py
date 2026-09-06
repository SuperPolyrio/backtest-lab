"""ClickHouse SQL for clean OrderFilled block close prices.

The production OrderFilled price tape keeps two ideas separate:

* close_price: the traded price of that token in that block.
* yes_probability_close: the same close converted to the market YES
  probability by the Postgres writer, using token_side metadata.

This query only builds the clean token-level tape. It filters known internal
exchange/settlement counterparties, derives trade price from maker/taker
amounts when those fields are available, and keeps enough diagnostics to audit
why a block close looks strange later.
"""

from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Iterable


INTERNAL_COUNTERPARTIES = (
    # NegRisk CTF Exchange / adapter-style internal settlement legs observed in
    # OrderFilled logs. Stored without 0x because orderfilled_fact normalizes
    # address-like fields that way.
    "c5d563a36ae78145c45a50134d48a1215220f80a",
    "e111180000d2663c0091e4f400237545b87b996b",
)


_CLICKHOUSE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_clickhouse_string(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _clickhouse_identifier(value: str) -> str:
    text = str(value or "").strip()
    if not _CLICKHOUSE_IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"unsafe ClickHouse identifier: {value!r}")
    return text


def _utc_iso(value: datetime | str) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def orderfilled_block_close_sql(
    *,
    table: str = "orderfilled_fact",
    from_block: int,
    to_block: int,
    market_ids: Iterable[int] | None = None,
    token_ids: Iterable[str] | None = None,
    source_cutoff: datetime | str | None = None,
    source_highwater_block: int | None = None,
    timestamp_anchor_block: int | None = None,
    timestamp_anchor_time: datetime | str | None = None,
    strict_snapshot: bool = False,
) -> str:
    table = _clickhouse_identifier(table)
    if int(from_block) > int(to_block):
        raise ValueError("from_block must be <= to_block")
    if strict_snapshot and source_cutoff is None:
        raise ValueError("strict_snapshot requires source_cutoff")
    if (timestamp_anchor_block is None) != (timestamp_anchor_time is None):
        raise ValueError("timestamp anchor block and time must be supplied together")
    filters = [f"f.block_number BETWEEN {int(from_block)} AND {int(to_block)}"]
    if source_highwater_block is not None:
        filters.append(f"f.block_number <= {int(source_highwater_block)}")
    cutoff_expression: str | None = None
    if source_cutoff is not None:
        cutoff_expression = (
            "toDateTime64("
            + quote_clickhouse_string(_utc_iso(source_cutoff))
            + ", 6, 'UTC')"
        )
        filters.append(f"f.ingested_at < {cutoff_expression}")
    market_list = sorted({int(market_id) for market_id in market_ids or [] if int(market_id or 0) > 0})
    if market_list:
        filters.append(f"f.market_id IN ({','.join(str(market_id) for market_id in market_list)})")
    token_list = [str(token).lower() for token in token_ids or [] if str(token or "").strip()]
    if token_list:
        quoted = ",".join(quote_clickhouse_string(token) for token in token_list)
        filters.append(f"lower(f.token_id) IN ({quoted})")
    where_sql = " AND ".join(filters)
    internal_addresses = ",".join(quote_clickhouse_string(address) for address in INTERNAL_COUNTERPARTIES)

    if source_cutoff is None:
        anchor_sql = f"""
            (SELECT ifNull(max(block_number), 0) FROM {table}) AS max_fact_block,
            (SELECT ifNull(max(block_number), 0) FROM block_timestamps) AS max_ts_block,
            (SELECT ifNull(max(block_time), toDateTime(0, 'UTC')) FROM block_timestamps) AS max_ts_time,
            if(max_ts_block > 0 AND max_ts_time > toDateTime('2000-01-01 00:00:00', 'UTC') AND max_fact_block - max_ts_block <= 7200, max_ts_block, max_fact_block) AS anchor_block,
            if(max_ts_block > 0 AND max_ts_time > toDateTime('2000-01-01 00:00:00', 'UTC') AND max_fact_block - max_ts_block <= 7200, max_ts_time, now('UTC')) AS anchor_time,
        """
        timestamp_join_sql = """
                LEFT JOIN (
                    SELECT block_number, argMax(block_time, ingested_at) AS block_time
                    FROM block_timestamps
                    GROUP BY block_number
                ) bt ON bt.block_number = f.block_number
        """
        missing_timestamp_sql = (
            "addSeconds(anchor_time, "
            "(toInt64(f.block_number) - toInt64(anchor_block)) * 2)"
        )
    else:
        if timestamp_anchor_block is None:
            anchor_sql = ""
            missing_timestamp_sql = "CAST(NULL AS Nullable(DateTime64(6, 'UTC')))"
        else:
            anchor_time_literal = quote_clickhouse_string(_utc_iso(timestamp_anchor_time))
            anchor_sql = f"""
            toInt64({int(timestamp_anchor_block)}) AS anchor_block,
            toDateTime64({anchor_time_literal}, 6, 'UTC') AS anchor_time,
            """
            missing_timestamp_sql = (
                "addSeconds(anchor_time, "
                "(toInt64(f.block_number) - toInt64(anchor_block)) * 2)"
            )
        timestamp_join_sql = f"""
                LEFT JOIN (
                    SELECT block_number, argMax(block_time, ingested_at) AS block_time
                    FROM block_timestamps
                    WHERE ingested_at < {cutoff_expression}
                    GROUP BY block_number
                ) bt ON bt.block_number = f.block_number
        """
    return f"""
        WITH
            {anchor_sql}
            raw AS (
                SELECT
                    f.market_id,
                    f.condition_id,
                    lower(f.token_id) AS token_id,
                    f.outcome_code,
                    f.block_number,
                    if(
                        isNull(bt.block_time) OR bt.block_time <= toDateTime('2000-01-01 00:00:00', 'UTC'),
                        {missing_timestamp_sql},
                        bt.block_time
                    ) AS block_timestamp,
                    lower(f.tx_hash) AS tx_hash,
                    f.log_index,
                    lower(replaceRegexpOne(f.maker, '^0x', '')) AS maker,
                    lower(replaceRegexpOne(f.taker, '^0x', '')) AS taker,
                    f.price AS raw_price,
                    f.size,
                    f.side_code,
                    f.maker_amount,
                    f.taker_amount,
                    (
                        maker_amount IS NOT NULL
                        AND taker_amount IS NOT NULL
                        AND maker_amount > 0
                        AND taker_amount > 0
                        AND maker_amount != taker_amount
                    ) AS can_derive_amount_price,
                    if(
                        can_derive_amount_price,
                        toDecimal128(least(assumeNotNull(maker_amount), assumeNotNull(taker_amount)), 10)
                            / toDecimal128(greatest(assumeNotNull(maker_amount), assumeNotNull(taker_amount)), 10),
                        raw_price
                    ) AS trade_price,
                    (maker IN ({internal_addresses}) OR taker IN ({internal_addresses})) AS is_internal_counterparty,
                    (size <= 0) AS is_invalid_size,
                    (trade_price < 0 OR trade_price > 1) AS is_invalid_price,
                    (trade_price <= 0.01 OR trade_price >= 0.99) AS is_extreme_price
                FROM {table} f
                {timestamp_join_sql}
                WHERE {where_sql}
            ),
            raw_stats AS (
                SELECT
                    token_id,
                    block_number,
                    count() AS raw_trade_count,
                    countIf(is_internal_counterparty) AS internal_filtered_count,
                    countIf(is_invalid_size) AS invalid_size_count,
                    countIf(is_invalid_price) AS invalid_price_count,
                    countIf(can_derive_amount_price) AS amount_ratio_count,
                    countIf(NOT can_derive_amount_price) AS raw_price_fallback_count,
                    countIf(is_extreme_price) AS extreme_trade_count
                FROM raw
                GROUP BY token_id, block_number
            ),
            clean AS (
                SELECT *
                FROM raw
                WHERE
                    NOT is_internal_counterparty
                    AND NOT is_invalid_size
                    AND NOT is_invalid_price
            )
        SELECT
            any(clean.market_id) AS market_id,
            any(clean.condition_id) AS condition_id,
            clean.token_id AS token_id,
            any(clean.outcome_code) AS outcome_code,
            clean.block_number AS block_number,
            formatDateTime(any(clean.block_timestamp), '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS block_timestamp,
            toString(argMin(clean.trade_price, tuple(clean.log_index, clean.tx_hash))) AS open_price,
            toString(max(clean.trade_price)) AS high_price,
            toString(min(clean.trade_price)) AS low_price,
            toString(tupleElement(
                argMax(tuple(clean.trade_price), tuple(clean.log_index, clean.tx_hash)),
                1
            )) AS close_price,
            toString(tupleElement(
                argMax(tuple(clean.raw_price), tuple(clean.log_index, clean.tx_hash)),
                1
            )) AS close_raw_price,
            lower(argMin(tx_hash, tuple(log_index, tx_hash))) AS first_tx_hash,
            lower(argMax(tx_hash, tuple(log_index, tx_hash))) AS close_tx_hash,
            lower(argMax(tx_hash, tuple(log_index, tx_hash))) AS last_tx_hash,
            toUInt32(min(clean.log_index)) AS first_log_index,
            toUInt32(max(clean.log_index)) AS last_log_index,
            toUInt32(argMax(clean.log_index, tuple(clean.log_index, clean.tx_hash))) AS close_log_index,
            toString(tupleElement(
                argMax(tuple(clean.maker_amount), tuple(clean.log_index, clean.tx_hash)),
                1
            )) AS close_maker_amount,
            toString(tupleElement(
                argMax(tuple(clean.taker_amount), tuple(clean.log_index, clean.tx_hash)),
                1
            )) AS close_taker_amount,
            if(
                argMax(clean.can_derive_amount_price, tuple(clean.log_index, clean.tx_hash)),
                'maker_taker_amount_ratio',
                'raw_orderfilled_price'
            ) AS close_price_source,
            count() AS clean_trade_count,
            raw_stats.raw_trade_count AS raw_trade_count,
            raw_stats.internal_filtered_count AS internal_filtered_count,
            raw_stats.invalid_size_count AS invalid_size_count,
            raw_stats.invalid_price_count AS invalid_price_count,
            raw_stats.amount_ratio_count AS amount_ratio_count,
            raw_stats.raw_price_fallback_count AS raw_price_fallback_count,
            raw_stats.extreme_trade_count AS extreme_trade_count,
            toString(sum(clean.size)) AS volume,
            toString(sumIf(clean.size, clean.side_code = 1)) AS buy_volume,
            toString(sumIf(clean.size, clean.side_code = 2)) AS sell_volume,
            toString(sum(clean.trade_price * clean.size) / nullIf(sum(clean.size), 0)) AS vwap_price
        FROM clean
        INNER JOIN raw_stats
            ON raw_stats.token_id = clean.token_id
            AND raw_stats.block_number = clean.block_number
        GROUP BY
            clean.token_id,
            clean.block_number,
            raw_stats.raw_trade_count,
            raw_stats.internal_filtered_count,
            raw_stats.invalid_size_count,
            raw_stats.invalid_price_count,
            raw_stats.amount_ratio_count,
            raw_stats.raw_price_fallback_count,
            raw_stats.extreme_trade_count
        ORDER BY clean.token_id ASC, clean.block_number ASC
    """
