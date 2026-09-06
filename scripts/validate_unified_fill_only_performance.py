#!/usr/bin/env python3
"""Validate frozen V2/V3 streaming and Rust performance artifacts."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.unified_performance_acceptance import (  # noqa: E402
    PerformanceAcceptanceInputs,
    build_performance_acceptance_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check unified Fill-only long-run performance gates"
    )
    for name in (
        "primary-15",
        "prefix-30",
        "dense-30",
        "days-60",
        "primary-173",
        "six-warm",
        "six-cold",
        "rust-benchmark",
        "v3-reference",
    ):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    inputs = PerformanceAcceptanceInputs(
        primary_15=args.primary_15,
        prefix_30=args.prefix_30,
        dense_30=args.dense_30,
        days_60=args.days_60,
        primary_173=args.primary_173,
        six_warm=args.six_warm,
        six_cold=args.six_cold,
        rust_benchmark=args.rust_benchmark,
        v3_reference=args.v3_reference,
    )
    report = build_performance_acceptance_report(inputs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, args.output)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
