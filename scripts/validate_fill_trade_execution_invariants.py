#!/usr/bin/env python3
"""Validate fill-trade execution invariants for synthetic or persisted V2 runs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_trade_validation import (
    VALIDATION_OUT_DIR,
    synthetic_orders_and_trades,
    validate_execution_invariants,
    validate_latest_v2_run_from_postgres,
    write_report,
)
from quant.backtest.orderfilled_v2_replay import replay_v2_taker_orders_with_diagnostics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("synthetic", "postgres-run"), default="synthetic")
    parser.add_argument("--run-id", type=int, default=None)
    parser.add_argument("--trades", type=int, default=1000)
    parser.add_argument("--orders", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-json", type=Path, default=VALIDATION_OUT_DIR / "execution_invariants.json")
    parser.add_argument("--output-md", type=Path, default=VALIDATION_OUT_DIR / "execution_invariants.md")
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.source == "postgres-run":
        payload = validate_latest_v2_run_from_postgres(run_id=args.run_id)
    else:
        orders, trades = synthetic_orders_and_trades(trades_count=args.trades, orders_count=args.orders, seed=args.seed)
        results, ledger, _ = replay_v2_taker_orders_with_diagnostics(orders, trades)
        payload = validate_execution_invariants(orders=orders, results=results, ledger=ledger, trades=trades)
    write_report(payload, output_json=args.output_json, output_md=args.output_md)
    print(f"status={payload['status']} json={args.output_json} md={args.output_md}")
    return 1 if args.strict and payload["status"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
