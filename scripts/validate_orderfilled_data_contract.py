#!/usr/bin/env python3
"""Validate raw -> maker fill ticks -> one-sided trade prints data contract."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_trade_validation import VALIDATION_OUT_DIR, validate_orderfilled_data_contract, write_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--chain-id", type=int, default=137)
    parser.add_argument("--output-json", type=Path, default=VALIDATION_OUT_DIR / "orderfilled_data_contract.json")
    parser.add_argument("--output-md", type=Path, default=VALIDATION_OUT_DIR / "orderfilled_data_contract.md")
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = validate_orderfilled_data_contract(from_block=args.from_block, to_block=args.to_block, chain_id=args.chain_id)
    write_report(payload, output_json=args.output_json, output_md=args.output_md)
    print(f"status={payload['status']} json={args.output_json} md={args.output_md}")
    return 1 if args.strict and payload["status"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
