#!/usr/bin/env python3
"""Benchmark shared trade indexes and incremental capacity-ledger deltas."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import resource
import sys
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_trade_validation import synthetic_orders_and_trades  # noqa: E402
from quant.backtest.orderfilled_v2_replay import (  # noqa: E402
    CapacityLedger,
    V2OrderResult,
    prepare_v2_trade_tape,
    replay_v2_taker_orders_with_diagnostics,
)

DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "runtime_outputs"
    / "orderfilled_v2_benchmarks"
    / "runtime_acceleration.json"
)
ZERO = Decimal("0")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trades", type=int, default=1_000_000)
    parser.add_argument("--orders", type=int, default=10_000)
    parser.add_argument("--profiles", type=int, default=6)
    parser.add_argument("--seed", type=int, default=20260826)
    parser.add_argument("--ledger-days", type=int, default=173)
    parser.add_argument("--allocations-per-day", type=int, default=2_000)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _seconds(start: float) -> float:
    return perf_counter() - start


def _result_digest(results: list[V2OrderResult], ledger: CapacityLedger) -> str:
    digest = hashlib.sha256()
    for result in results:
        digest.update(
            json.dumps(
                result.as_dict(),
                default=str,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )
        digest.update(b"\n")
    digest.update(
        json.dumps(
            ledger.as_dict(),
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    digest.update(
        json.dumps(
            ledger.market_window_as_dict(),
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )
    return digest.hexdigest()


def _run_matcher_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    build_started = perf_counter()
    orders, trades = synthetic_orders_and_trades(
        trades_count=args.trades,
        orders_count=args.orders,
        seed=args.seed,
    )
    input_build_sec = _seconds(build_started)

    legacy_digests: list[str] = []
    legacy_index_build_sec = ZERO
    legacy_matching_sec = ZERO
    legacy_started = perf_counter()
    for _ in range(args.profiles):
        results, ledger, diagnostics = replay_v2_taker_orders_with_diagnostics(
            orders,
            trades,
        )
        legacy_index_build_sec += diagnostics.index_build_sec
        legacy_matching_sec += diagnostics.matching_sec
        legacy_digests.append(_result_digest(results, ledger))
        del results, ledger, diagnostics
        gc.collect()
    legacy_total_sec = _seconds(legacy_started)

    prepared_started = perf_counter()
    prepared_tape = prepare_v2_trade_tape(trades)
    prepared_build_wall_sec = _seconds(prepared_started)
    prepared_digests: list[str] = []
    prepared_matching_sec = ZERO
    prepared_replay_started = perf_counter()
    reused_flags: list[bool] = []
    for _ in range(args.profiles):
        results, ledger, diagnostics = replay_v2_taker_orders_with_diagnostics(
            orders,
            prepared_tape=prepared_tape,
        )
        reused_flags.append(diagnostics.index_reused)
        prepared_matching_sec += diagnostics.matching_sec
        prepared_digests.append(_result_digest(results, ledger))
        del results, ledger, diagnostics
        gc.collect()
    prepared_replay_sec = _seconds(prepared_replay_started)
    prepared_total_sec = prepared_build_wall_sec + prepared_replay_sec

    equality = legacy_digests == prepared_digests
    if not equality:
        raise RuntimeError("prepared replay changed result or capacity-ledger digest")
    return {
        "trades": len(trades),
        "orders": len(orders),
        "profiles": args.profiles,
        "input_build_sec": input_build_sec,
        "legacy_total_sec": legacy_total_sec,
        "legacy_index_build_sec": float(legacy_index_build_sec),
        "legacy_matching_sec": float(legacy_matching_sec),
        "prepared_total_sec": prepared_total_sec,
        "prepared_index_build_sec": prepared_build_wall_sec,
        "prepared_replay_sec": prepared_replay_sec,
        "prepared_matching_sec": float(prepared_matching_sec),
        "speedup": legacy_total_sec / prepared_total_sec if prepared_total_sec else None,
        "index_builds_before": args.profiles,
        "index_builds_after": 1,
        "index_builds_avoided": max(0, args.profiles - 1),
        "all_replays_reported_index_reused": all(reused_flags),
        "result_and_capacity_digest_equal": equality,
        "result_digest": prepared_digests[0] if prepared_digests else None,
    }


def _run_ledger_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    days = args.ledger_days
    per_day = args.allocations_per_day

    legacy = CapacityLedger()
    previous: dict[str, str] = {}
    legacy_delta_rows = 0
    legacy_started = perf_counter()
    for day in range(days):
        for offset in range(per_day):
            legacy.consume(f"trade-{day:04d}-{offset:08d}", Decimal("0.025"))
        current = legacy.as_dict()
        daily_delta = {
            key: value for key, value in current.items() if previous.get(key) != value
        }
        legacy_delta_rows += len(daily_delta)
        previous = current
    legacy_sec = _seconds(legacy_started)

    incremental = CapacityLedger()
    incremental_delta_rows = 0
    incremental_started = perf_counter()
    for day in range(days):
        incremental.begin_delta_tracking()
        for offset in range(per_day):
            incremental.consume(f"trade-{day:04d}-{offset:08d}", Decimal("0.025"))
        incremental_delta_rows += len(incremental.drain_delta().source_trade_consumed)
    incremental_sec = _seconds(incremental_started)

    final_equal = legacy.as_dict() == incremental.as_dict()
    if not final_equal or legacy_delta_rows != incremental_delta_rows:
        raise RuntimeError("incremental ledger delta changed cumulative capacity state")
    return {
        "days": days,
        "allocations_per_day": per_day,
        "total_allocations": days * per_day,
        "legacy_snapshot_diff_sec": legacy_sec,
        "incremental_mutation_delta_sec": incremental_sec,
        "speedup": legacy_sec / incremental_sec if incremental_sec else None,
        "daily_delta_rows_equal": legacy_delta_rows == incremental_delta_rows,
        "final_capacity_ledger_equal": final_equal,
    }


def main() -> int:
    args = parse_args()
    started = perf_counter()
    matcher = _run_matcher_benchmark(args)
    ledger = _run_ledger_benchmark(args)
    payload = {
        "schema_version": "OrderFilledV2RuntimeAccelerationBenchmarkV1",
        "status": "PASS",
        "matcher": matcher,
        "capacity_ledger": ledger,
        "wall_sec": _seconds(started),
        "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
    }
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
