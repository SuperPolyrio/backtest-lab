#!/usr/bin/env python3
"""Differential wall-clock benchmark for the Python and Rust V2 matchers."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.orderfilled_v2_replay import (  # noqa: E402
    CapacityLedger,
    V2OrderResult,
    V2ReplayDiagnostics,
    V2TakerOrder,
    V2TradePrint,
    prepare_v2_trade_tape,
    replay_v2_taker_orders_with_diagnostics,
)
from quant.backtest.rust_kernel import rust_kernel_available  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trades", type=int, default=50_000)
    parser.add_argument("--orders", type=int, default=100)
    parser.add_argument("--samples", type=int, default=9)
    parser.add_argument("--minimum-speedup", type=float, default=3.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not rust_kernel_available():
        raise SystemExit("_fill_only_rust is not installed in this Python environment")
    trades, orders = _fixture(max(1, args.trades), max(1, args.orders))
    prepared = prepare_v2_trade_tape(trades)

    cold_started = perf_counter()
    replay_v2_taker_orders_with_diagnostics(
        orders[:1],
        prepared_tape=prepared,
        backend="rust",
    )
    native_materialization_seconds = perf_counter() - cold_started

    rows: dict[str, Any] = {}
    for backend in ("python", "rust"):
        samples: list[float] = []
        final_results: list[V2OrderResult] = []
        final_ledger = CapacityLedger()
        final_diagnostics: V2ReplayDiagnostics | None = None
        for _ in range(max(1, args.samples)):
            started = perf_counter()
            final_results, final_ledger, final_diagnostics = (
                replay_v2_taker_orders_with_diagnostics(
                    orders,
                    prepared_tape=prepared,
                    ledger=CapacityLedger(),
                    backend=backend,  # type: ignore[arg-type]
                )
            )
            samples.append(perf_counter() - started)
        result_hash = _sha256([row.as_dict() for row in final_results])
        ledger_hash = _sha256(final_ledger.snapshot())
        if final_diagnostics is None:
            raise RuntimeError("matcher benchmark produced no diagnostics")
        rows[backend] = {
            "minimum_seconds": min(samples),
            "median_seconds": statistics.median(samples),
            "maximum_seconds": max(samples),
            "result_sha256": result_hash,
            "ledger_sha256": ledger_hash,
            "diagnostics": final_diagnostics.as_dict(),
        }
    result_equal = rows["python"]["result_sha256"] == rows["rust"]["result_sha256"]
    ledger_equal = rows["python"]["ledger_sha256"] == rows["rust"]["ledger_sha256"]
    speedup = rows["python"]["median_seconds"] / rows["rust"]["median_seconds"]
    payload = {
        "schema_version": "FillOnlyRustBenchmarkV1",
        "trades": len(trades),
        "orders": len(orders),
        "samples": max(1, args.samples),
        "native_materialization_seconds": native_materialization_seconds,
        "minimum_speedup": args.minimum_speedup,
        "median_speedup": speedup,
        "result_equal": result_equal,
        "ledger_equal": ledger_equal,
        "passed": result_equal and ledger_equal and speedup >= args.minimum_speedup,
        "backends": rows,
    }
    encoded = json.dumps(payload, default=str, indent=2, sort_keys=True)
    print(encoded)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(encoded + "\n", encoding="utf-8")
        temporary.replace(args.output)
    return 0 if payload["passed"] else 1


def _fixture(trade_count: int, order_count: int) -> tuple[list[V2TradePrint], list[V2TakerOrder]]:
    base = datetime(2026, 7, 1, tzinfo=timezone.utc)
    trades = [
        V2TradePrint(
            trade_id=f"trade-{index}",
            market_id=1,
            condition_id="condition",
            asset_id="asset",
            outcome="YES",
            block_number=100_000 + index,
            block_time=base + timedelta(seconds=index),
            tx_hash=f"0x{index:064x}",
            tx_index=index,
            tx_index_source="benchmark",
            price=Decimal("0.55"),
            size=Decimal("100"),
            notional=Decimal("55"),
            aggressor_side="BUY" if index % 2 == 0 else "SELL",
            passive_side="SELL" if index % 2 == 0 else "BUY",
            source_log_indexes=(index,),
            source_fill_count=1,
            trade_group_id=f"group-{index}",
        )
        for index in range(trade_count)
    ]
    anchor = max(0, trade_count // 2 - order_count // 2)
    orders = [
        V2TakerOrder(
            order_id=f"order-{index}",
            market_id=1,
            asset_id="asset",
            side="BUY",
            limit_price=Decimal("0.60"),
            size=Decimal("10"),
            signal_block=100_000 + anchor + index,
            signal_ts=base + timedelta(seconds=anchor + index),
            latency_blocks=1,
            latency=timedelta(seconds=1),
            horizon_blocks=100,
            horizon=timedelta(seconds=30),
            participation_rate=Decimal("0.025"),
            price_buffer=Decimal("0.005"),
        )
        for index in range(order_count)
    ]
    return trades, orders


def _sha256(value: Any) -> str:
    payload = json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
