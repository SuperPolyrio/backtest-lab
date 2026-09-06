#!/usr/bin/env python3
"""Benchmark indexed fill-trade replay and emit scalability diagnostics."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_trade_validation import VALIDATION_OUT_DIR, benchmark_fill_trade_replay, write_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trades", type=int, default=100_000)
    parser.add_argument("--orders", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--reference", action="store_true", help="Also run slow reference matcher; use on small/medium sizes.")
    parser.add_argument("--output-json", type=Path, default=VALIDATION_OUT_DIR / "benchmark_fill_trade_replay.json")
    parser.add_argument("--output-md", type=Path, default=VALIDATION_OUT_DIR / "benchmark_fill_trade_replay.md")
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = benchmark_fill_trade_replay(
        trades_count=args.trades,
        orders_count=args.orders,
        seed=args.seed,
        reference=args.reference,
    )
    write_report(payload, output_json=args.output_json, output_md=args.output_md)
    print(f"status={payload['status']} json={args.output_json} md={args.output_md}")
    return 1 if args.strict and payload["status"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
