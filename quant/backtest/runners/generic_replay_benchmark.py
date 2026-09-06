"""Generic multi-market replay access benchmark.

This runner validates the common replay architecture without depending on an
NBA-specific selector. It samples recent `(market_id, token_id)` pairs directly
from ClickHouse, then compares market-only raw reads, token-pair raw reads, and
block-level replay aggregation.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import time
from typing import Any

from ...core.db import ClickHouseClient


READY = "ready"
REVIEW = "review"
MISSING = "missing"


@dataclass(frozen=True)
class GenericReplayBenchmarkResult:
    token_pairs: int
    market_count: int
    from_block: int
    to_block: int
    index_sec: float
    market_only_count_sec: float
    market_only_count: int
    token_pair_count_sec: float
    token_pair_count: int
    block_replay_count_sec: float
    block_replay_count: int
    market_only_rows_sec: float | None = None
    market_only_rows: int | None = None
    token_pair_rows_sec: float | None = None
    token_pair_rows: int | None = None
    block_replay_rows_sec: float | None = None
    block_replay_rows: int | None = None


def build_generic_replay_sql_contract_report() -> dict[str, Any]:
    """Validate the static SQL shape used by large replay benchmarks.

    This is intentionally DB-free.  The goal is to keep the historical replay
    fast path honest: raw detail reads must be bounded by token pair and block
    range, and block replay aggregation must stay materialized/summary-shaped
    instead of degenerating into an online all-market raw scan.
    """

    pairs_sql = "(11, 'token-a'),(12, 'token-b')"
    raw_pair_sql = _raw_rows_sql(pairs_sql=pairs_sql, from_block=100, to_block=200)
    block_sql = _block_replay_rows_sql(pairs_sql=pairs_sql, from_block=100, to_block=200)
    checks = {
        "raw_pair_prewhere": "PREWHERE (market_id, token_id) IN" in raw_pair_sql,
        "raw_pair_block_range": "block_number BETWEEN 100 AND 200" in raw_pair_sql,
        "raw_pair_ordered": "ORDER BY market_id ASC, token_id ASC, block_number ASC, log_index ASC, tx_hash ASC" in raw_pair_sql,
        "raw_pair_limited": "LIMIT 10000000" in raw_pair_sql,
        "block_replay_prewhere": "PREWHERE (market_id, token_id) IN" in block_sql,
        "block_replay_block_range": "block_number BETWEEN 100 AND 200" in block_sql,
        "block_replay_grouped": "GROUP BY market_id, token_id, block_number" in block_sql,
        "block_replay_ordered": "ORDER BY market_id ASC, token_id ASC, block_number ASC" in block_sql,
        "block_replay_limited": "LIMIT 10000000" in block_sql,
        "block_replay_vwap": "AS vwap_price" in block_sql,
        "block_replay_side_volume": "AS buy_volume" in block_sql and "AS sell_volume" in block_sql,
        "block_replay_ohlc": "AS low_price" in block_sql and "AS high_price" in block_sql and "AS close_price" in block_sql,
    }
    missing = [name for name, ok in checks.items() if not ok]
    status = READY if not missing else MISSING
    return {
        "status": status,
        "contract_version": "generic_replay_sql_contract_v1",
        "reason": "generic replay SQL is bounded by token pair and block range" if status == READY else "generic replay SQL contract is incomplete",
        "checks": checks,
        "missing": missing,
        "required_access": [
            "PREWHERE (market_id, token_id)",
            "block_number BETWEEN from_block AND to_block",
            "ORDER BY market_id, token_id, block_number, log_index, tx_hash for raw ticks",
            "LIMIT for raw tick reads",
            "block replay aggregation with OHLCV/VWAP/side volume",
        ],
    }


def run_generic_replay_benchmark(
    *,
    token_pairs: int = 300,
    lookback_blocks: int = 500000,
    include_rows: bool = False,
) -> GenericReplayBenchmarkResult:
    client = ClickHouseClient()
    to_block = int(client.query_scalar("SELECT max(block_number) FROM orderfilled_fact", timeout_seconds=30))
    from_block = to_block - int(lookback_blocks)
    index_start = time.perf_counter()
    index_rows = client.query_json_rows(
        f"""
        SELECT market_id, token_id, count() AS rows
        FROM orderfilled_fact
        PREWHERE block_number BETWEEN {from_block} AND {to_block}
        GROUP BY market_id, token_id
        ORDER BY rows DESC
        LIMIT {int(token_pairs)}
        """,
        timeout_seconds=120,
    )
    index_sec = time.perf_counter() - index_start
    pairs = [(int(row["market_id"]), str(row["token_id"])) for row in index_rows]
    market_ids = sorted({market_id for market_id, _ in pairs})
    ids_sql = ",".join(str(market_id) for market_id in market_ids)
    pairs_sql = _tuple_list(pairs)

    market_only_count_sec, market_only_count = _time_scalar(
        client,
        f"""
        SELECT count()
        FROM orderfilled_fact
        PREWHERE market_id IN ({ids_sql})
          AND block_number BETWEEN {from_block} AND {to_block}
        """,
    )
    token_pair_count_sec, token_pair_count = _time_scalar(
        client,
        f"""
        SELECT count()
        FROM orderfilled_fact
        PREWHERE (market_id, token_id) IN ({pairs_sql})
          AND block_number BETWEEN {from_block} AND {to_block}
        """,
    )
    block_replay_count_sec, block_replay_count = _time_scalar(
        client,
        f"""
        SELECT count()
        FROM (
            SELECT market_id, token_id, block_number
            FROM orderfilled_fact
            PREWHERE (market_id, token_id) IN ({pairs_sql})
              AND block_number BETWEEN {from_block} AND {to_block}
            GROUP BY market_id, token_id, block_number
        )
        """,
    )

    row_metrics: dict[str, Any] = {}
    if include_rows:
        row_metrics["market_only_rows_sec"], row_metrics["market_only_rows"] = _time_rows(
            client,
            _raw_rows_sql(ids_sql=ids_sql, from_block=from_block, to_block=to_block),
        )
        row_metrics["token_pair_rows_sec"], row_metrics["token_pair_rows"] = _time_rows(
            client,
            _raw_rows_sql(pairs_sql=pairs_sql, from_block=from_block, to_block=to_block),
        )
        row_metrics["block_replay_rows_sec"], row_metrics["block_replay_rows"] = _time_rows(
            client,
            _block_replay_rows_sql(pairs_sql=pairs_sql, from_block=from_block, to_block=to_block),
        )

    return GenericReplayBenchmarkResult(
        token_pairs=len(pairs),
        market_count=len(market_ids),
        from_block=from_block,
        to_block=to_block,
        index_sec=round(index_sec, 6),
        market_only_count_sec=round(market_only_count_sec, 6),
        market_only_count=market_only_count,
        token_pair_count_sec=round(token_pair_count_sec, 6),
        token_pair_count=token_pair_count,
        block_replay_count_sec=round(block_replay_count_sec, 6),
        block_replay_count=block_replay_count,
        **row_metrics,
    )


def _time_scalar(client: ClickHouseClient, sql: str) -> tuple[float, int]:
    start = time.perf_counter()
    value = int(client.query_scalar(sql, timeout_seconds=180))
    return time.perf_counter() - start, value


def _time_rows(client: ClickHouseClient, sql: str) -> tuple[float, int]:
    start = time.perf_counter()
    rows = client.query_json_rows(sql, timeout_seconds=240)
    return time.perf_counter() - start, len(rows)


def _raw_rows_sql(
    *,
    from_block: int,
    to_block: int,
    ids_sql: str | None = None,
    pairs_sql: str | None = None,
) -> str:
    if pairs_sql:
        prewhere = f"(market_id, token_id) IN ({pairs_sql})"
    else:
        prewhere = f"market_id IN ({ids_sql})"
    return f"""
    SELECT market_id, outcome_code, token_id, block_number, log_index, lower(tx_hash) AS tx_hash, price, size
    FROM orderfilled_fact
    PREWHERE {prewhere}
      AND block_number BETWEEN {from_block} AND {to_block}
    ORDER BY market_id ASC, token_id ASC, block_number ASC, log_index ASC, tx_hash ASC
    LIMIT 10000000
    """


def _block_replay_rows_sql(*, pairs_sql: str, from_block: int, to_block: int) -> str:
    return f"""
    SELECT
        market_id,
        token_id,
        block_number,
        argMax(price, (log_index, tx_hash)) AS close_price,
        CAST(if(sum(size) > 0, sum(price * size) / sum(size), 0), 'Decimal(20, 10)') AS vwap_price,
        sum(size) AS volume,
        count() AS trade_count,
        sumIf(size, side_code = 1) AS buy_volume,
        sumIf(size, side_code = 2) AS sell_volume,
        min(price) AS low_price,
        max(price) AS high_price,
        min(log_index) AS first_log_index,
        max(log_index) AS last_log_index
    FROM orderfilled_fact
    PREWHERE (market_id, token_id) IN ({pairs_sql})
      AND block_number BETWEEN {from_block} AND {to_block}
    GROUP BY market_id, token_id, block_number
    ORDER BY market_id ASC, token_id ASC, block_number ASC
    LIMIT 10000000
    """


def _tuple_list(pairs: list[tuple[int, str]]) -> str:
    return ",".join(f"({int(market_id)}, {_quote_clickhouse_string(token_id)})" for market_id, token_id in pairs)


def _quote_clickhouse_string(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'").lower() + "'"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-pairs", type=int, default=300)
    parser.add_argument("--lookback-blocks", type=int, default=500000)
    parser.add_argument("--include-rows", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = run_generic_replay_benchmark(
        token_pairs=args.token_pairs,
        lookback_blocks=args.lookback_blocks,
        include_rows=args.include_rows,
    )
    payload = asdict(result)
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(
            f"generic_replay_benchmark: pairs={result.token_pairs} markets={result.market_count} "
            f"blocks={result.from_block}->{result.to_block}"
        )
        print(
            f"counts: market_only={result.market_only_count} in {result.market_only_count_sec}s, "
            f"token_pair={result.token_pair_count} in {result.token_pair_count_sec}s, "
            f"block_replay={result.block_replay_count} in {result.block_replay_count_sec}s"
        )
        if result.market_only_rows_sec is not None:
            print(
                f"rows: market_only={result.market_only_rows} in {result.market_only_rows_sec}s, "
                f"token_pair={result.token_pair_rows} in {result.token_pair_rows_sec}s, "
                f"block_replay={result.block_replay_rows} in {result.block_replay_rows_sec}s"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
