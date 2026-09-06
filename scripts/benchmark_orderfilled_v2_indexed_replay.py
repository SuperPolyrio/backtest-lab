#!/usr/bin/env python3
"""Benchmark OrderFilled V2 indexed taker replay."""

from __future__ import annotations

import argparse
import json
import random
import resource
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.benchmark_persistence import complete_benchmark_run, create_benchmark_run, fail_benchmark_run
from quant.backtest.orderfilled_v2_compare import compare_replay_results
from quant.backtest.orderfilled_v2_replay import (
    V2TakerOrder,
    V2TradePrint,
    load_v2_trade_slices_for_orders,
    replay_v2_taker_orders_reference,
    replay_v2_taker_orders_with_diagnostics,
)
from quant.core.db import ClickHouseClient, postgres_connection


OUT_DIR = PROJECT_ROOT / "runtime_outputs" / "orderfilled_v2_benchmarks"
Q = Decimal("0.0000000001")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("synthetic", "historical"), default="synthetic")
    parser.add_argument("--trades", type=int, default=50000)
    parser.add_argument("--orders", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--market-count", type=int, default=5)
    parser.add_argument("--start-block", type=int, default=None)
    parser.add_argument("--end-block", type=int, default=None)
    parser.add_argument("--order-size", default="1")
    parser.add_argument("--participation-rate", default="0.025")
    parser.add_argument("--reference", action="store_true", help="Run slow reference and compare; recommended only for small/medium synthetic sizes.")
    parser.add_argument("--persist-postgres", action="store_true")
    parser.add_argument("--output-prefix", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    t0 = perf_counter()
    benchmark_id: int | None = None
    try:
        if args.persist_postgres:
            with postgres_connection(readonly=False) as conn:
                benchmark_id = create_benchmark_run(
                    conn,
                    universe_type=args.dataset,
                    universe_name=f"orderfilled_v2_{args.dataset}",
                    market_count=int(args.market_count if args.dataset == "historical" else 1),
                    strategy_name="orderfilled_v2_indexed_replay_benchmark",
                    parameters=vars(args),
                    profiles={"indexed": True, "reference": bool(args.reference)},
                )
                conn.commit()

        if args.dataset == "synthetic":
            orders, trades, load_summary = _build_synthetic(args)
        else:
            orders, trades, load_summary = _load_historical(args)

        indexed_start = perf_counter()
        indexed_results, indexed_ledger, diagnostics = replay_v2_taker_orders_with_diagnostics(orders, trades)
        indexed_runtime_sec = _elapsed(indexed_start)
        reference_runtime_sec: Decimal | None = None
        comparison: dict[str, Any] | None = None
        if args.reference:
            reference_start = perf_counter()
            reference_results, reference_ledger = replay_v2_taker_orders_reference(orders, trades)
            reference_runtime_sec = _elapsed(reference_start)
            comparison = compare_replay_results(reference_results, reference_ledger, indexed_results, indexed_ledger)

        total_runtime_sec = _elapsed(t0)
        summary = _summary(
            args=args,
            total_runtime_sec=total_runtime_sec,
            indexed_runtime_sec=indexed_runtime_sec,
            reference_runtime_sec=reference_runtime_sec,
            load_summary=load_summary,
            diagnostics=diagnostics.as_dict(),
            fills_count=sum(len(result.fills) for result in indexed_results),
            capacity_updates_count=len(indexed_ledger.as_dict()),
            comparison=comparison,
        )
        artifacts = {"summary": summary, "comparison": comparison or {}, "load_summary": load_summary, "diagnostics": diagnostics.as_dict()}
        if benchmark_id is not None:
            with postgres_connection(readonly=False) as conn:
                complete_benchmark_run(
                    conn,
                    benchmark_id=benchmark_id,
                    summary=summary,
                    rows=[_benchmark_row(summary, benchmark_id=benchmark_id)],
                    artifacts=artifacts,
                    data_version="orderfilled_v2_indexed_v1",
                )
                conn.commit()
            summary["benchmark_id"] = benchmark_id

        prefix = args.output_prefix or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        json_path = OUT_DIR / f"{prefix}_indexed_benchmark.json"
        md_path = OUT_DIR / f"{prefix}_indexed_benchmark.md"
        json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        md_path.write_text(_markdown(summary), encoding="utf-8")
        print(json.dumps({"status": "ready", "json": str(json_path), "markdown": str(md_path), "benchmark_id": benchmark_id}, indent=2))
        return 0
    except Exception as exc:
        if benchmark_id is not None:
            with postgres_connection(readonly=False) as conn:
                fail_benchmark_run(conn, benchmark_id=benchmark_id, error=str(exc))
                conn.commit()
        raise


def _build_synthetic(args: argparse.Namespace) -> tuple[list[V2TakerOrder], list[V2TradePrint], dict[str, Any]]:
    random.seed(int(args.seed))
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    trades: list[V2TradePrint] = []
    for index in range(max(0, int(args.trades))):
        side = "BUY" if index % 2 == 0 else "SELL"
        price = Decimal("0.50") + Decimal(random.randint(-5, 5)) / Decimal("100")
        size = Decimal(random.randint(1, 200))
        trades.append(
            V2TradePrint(
                trade_id=f"synthetic-{index}",
                market_id=1,
                condition_id="synthetic-condition",
                asset_id="synthetic-token",
                outcome="YES",
                block_number=index + 1,
                block_time=base + timedelta(seconds=index),
                tx_hash=f"0x{index:064x}",
                tx_index=index,
                tx_index_source="synthetic",
                price=price,
                size=size,
                notional=(price * size).quantize(Q),
                aggressor_side=side,  # type: ignore[arg-type]
                passive_side="SELL" if side == "BUY" else "BUY",  # type: ignore[arg-type]
                source_log_indexes=(index,),
                source_fill_count=1,
            )
        )
    orders: list[V2TakerOrder] = []
    step = max(1, max(1, int(args.trades)) // max(1, int(args.orders)))
    for index in range(max(0, int(args.orders))):
        block = index * step + 1
        side = "BUY" if index % 2 == 0 else "SELL"
        orders.append(
            V2TakerOrder(
                order_id=f"synthetic-order-{index}",
                market_id=1,
                asset_id="synthetic-token",
                side=side,  # type: ignore[arg-type]
                limit_price=Decimal("0.60") if side == "BUY" else Decimal("0.40"),
                size=Decimal(str(args.order_size)),
                signal_block=max(0, block - 1),
                signal_ts=base + timedelta(seconds=max(0, block - 2)),
                latency=timedelta(0),
                horizon=timedelta(seconds=30),
                horizon_blocks=30,
                participation_rate=Decimal(str(args.participation_rate)),
            )
        )
    return orders, trades, {"source": "synthetic", "load_sec": Decimal("0"), "rows_loaded": len(trades), "db_query_count": 0}


def _load_historical(args: argparse.Namespace) -> tuple[list[V2TakerOrder], list[V2TradePrint], dict[str, Any]]:
    if args.start_block is None or args.end_block is None:
        raise SystemExit("historical benchmark requires --start-block and --end-block")
    client = ClickHouseClient()
    pairs = client.query_json_rows(
        f"""
        SELECT
            market_id,
            asset_id,
            any(outcome) AS outcome,
            min(block_number) AS min_block,
            max(block_number) AS max_block,
            count() AS rows
        FROM trade_prints_one_sided
        PREWHERE block_number BETWEEN {int(args.start_block)} AND {int(args.end_block)}
        GROUP BY market_id, asset_id
        ORDER BY rows DESC
        LIMIT {max(1, int(args.market_count))}
        """,
        timeout_seconds=120,
    )
    orders: list[V2TakerOrder] = []
    for index, row in enumerate(pairs):
        min_block = int(row["min_block"])
        max_block = int(row["max_block"])
        for side, limit in (("BUY", Decimal("1")), ("SELL", Decimal("0"))):
            orders.append(
                V2TakerOrder(
                    order_id=f"historical-{index}-{side.lower()}",
                    market_id=int(row["market_id"]),
                    asset_id=str(row["asset_id"]).lower(),
                    side=side,  # type: ignore[arg-type]
                    limit_price=limit,
                    size=Decimal(str(args.order_size)),
                    signal_block=max(0, min_block - 1),
                    signal_ts=None,
                    latency=timedelta(0),
                    horizon=None,
                    horizon_blocks=max(1, max_block - min_block),
                    participation_rate=Decimal(str(args.participation_rate)),
                )
            )
    load = load_v2_trade_slices_for_orders(orders, client=client)
    return orders, list(load.trades), {**load.as_dict(), "source": "clickhouse", "selected_pairs": len(pairs)}


def _summary(**kwargs: Any) -> dict[str, Any]:
    args = kwargs["args"]
    diagnostics = kwargs["diagnostics"]
    total = kwargs["total_runtime_sec"]
    orders = int(diagnostics["orders_count"])
    trades = int(diagnostics["trade_rows_indexed"])
    indexed_runtime = kwargs["indexed_runtime_sec"]
    reference_runtime = kwargs["reference_runtime_sec"]
    speedup = Decimal("0")
    if reference_runtime is not None and indexed_runtime > 0:
        speedup = (reference_runtime / indexed_runtime).quantize(Q)
    load_summary = kwargs["load_summary"]
    return {
        "status": "ready",
        "dataset": args.dataset,
        "runtime_total_sec": total,
        "load_sec": Decimal(str(load_summary.get("load_sec", "0"))).quantize(Q),
        "index_build_sec": diagnostics["index_build_sec"],
        "matching_sec": diagnostics["matching_sec"],
        "indexed_runtime_sec": indexed_runtime,
        "reference_runtime_sec": reference_runtime,
        "speedup_vs_reference": speedup,
        "orders_count": orders,
        "trades_loaded": trades,
        "trade_groups_count": diagnostics["trade_groups"],
        "candidate_trades_scanned": diagnostics["candidate_rows_scanned"],
        "naive_rows_scanned": diagnostics["naive_rows_scanned"],
        "scan_reduction_ratio": diagnostics["scan_reduction_ratio"],
        "candidate_rows_per_order_p50": diagnostics["candidate_rows_per_order_p50"],
        "candidate_rows_per_order_p95": diagnostics["candidate_rows_per_order_p95"],
        "candidate_rows_per_order_p99": diagnostics["candidate_rows_per_order_p99"],
        "fills_count": kwargs["fills_count"],
        "capacity_updates_count": kwargs["capacity_updates_count"],
        "orders_per_sec": (Decimal(orders) / indexed_runtime).quantize(Q) if indexed_runtime > 0 else Decimal("0"),
        "trades_loaded_per_sec": (Decimal(trades) / Decimal(str(load_summary.get("load_sec", "0")))).quantize(Q) if Decimal(str(load_summary.get("load_sec", "0"))) > 0 else Decimal("0"),
        "peak_memory_mb": Decimal(str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)).quantize(Q),
        "load_summary": load_summary,
        "comparison": kwargs["comparison"] or {},
    }


def _benchmark_row(summary: dict[str, Any], *, benchmark_id: int) -> dict[str, Any]:
    return {
        "market_id": None,
        "market_slug": f"orderfilled-v2-indexed-{summary['dataset']}",
        "title": "OrderFilled V2 indexed benchmark",
        "outcome": "YES",
        "fast_status": "ready",
        "accurate_status": summary.get("comparison", {}).get("status") or "not_run",
        "data_quality": "ready",
        "payload": summary,
        "benchmark_id": benchmark_id,
    }


def _markdown(summary: dict[str, Any]) -> str:
    return f"""# OrderFilled V2 Indexed Benchmark

Status: {summary['status']}

- dataset: `{summary['dataset']}`
- runtime_total_sec: `{summary['runtime_total_sec']}`
- load_sec: `{summary['load_sec']}`
- index_build_sec: `{summary['index_build_sec']}`
- matching_sec: `{summary['matching_sec']}`
- indexed_runtime_sec: `{summary['indexed_runtime_sec']}`
- reference_runtime_sec: `{summary['reference_runtime_sec']}`
- speedup_vs_reference: `{summary['speedup_vs_reference']}`
- orders_count: `{summary['orders_count']}`
- trades_loaded: `{summary['trades_loaded']}`
- trade_groups_count: `{summary['trade_groups_count']}`
- candidate_trades_scanned: `{summary['candidate_trades_scanned']}`
- naive_rows_scanned: `{summary['naive_rows_scanned']}`
- scan_reduction_ratio: `{summary['scan_reduction_ratio']}`
- candidate_rows_per_order_p50: `{summary['candidate_rows_per_order_p50']}`
- candidate_rows_per_order_p95: `{summary['candidate_rows_per_order_p95']}`
- candidate_rows_per_order_p99: `{summary['candidate_rows_per_order_p99']}`
- fills_count: `{summary['fills_count']}`
- capacity_updates_count: `{summary['capacity_updates_count']}`
- orders_per_sec: `{summary['orders_per_sec']}`
- trades_loaded_per_sec: `{summary['trades_loaded_per_sec']}`
- peak_memory_mb: `{summary['peak_memory_mb']}`

## Comparison

```json
{json.dumps(summary.get('comparison') or {}, ensure_ascii=False, indent=2, default=str)}
```
"""


def _elapsed(start: float) -> Decimal:
    return Decimal(str(perf_counter() - start)).quantize(Q)


if __name__ == "__main__":
    raise SystemExit(main())
