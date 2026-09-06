#!/usr/bin/env python3
"""Check run-level external evidence coverage for fill-first backtests."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_source_run_coverage import (  # noqa: E402
    READY,
    build_external_source_run_coverage_report,
    external_source_run_coverage_to_markdown,
    load_external_source_run_coverage_inputs,
)
from quant.backtest.run_artifacts import load_latest_fill_first_backtest_run_id  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, default=None, help="Backtest run id. Defaults to latest fill-first run.")
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero for review reports.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=True) as conn:
        run_id = args.run_id if args.run_id is not None else load_latest_fill_first_backtest_run_id(conn)
        inputs = load_external_source_run_coverage_inputs(conn, run_id=run_id) if run_id is not None else None
    report = build_external_source_run_coverage_report(inputs, run_id=run_id)
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(external_source_run_coverage_to_markdown(report))
    if report["status"] == READY:
        return 0
    return 1 if args.strict_review else 0


if __name__ == "__main__":
    raise SystemExit(main())
