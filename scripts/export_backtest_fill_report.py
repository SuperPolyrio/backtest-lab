#!/usr/bin/env python3
"""Export a focused fill/no-fill report for a backtest run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_report import build_backtest_fill_report, backtest_fill_report_to_markdown  # noqa: E402
from quant.backtest.run_artifacts import load_backtest_run_artifact_inputs, load_latest_fill_first_backtest_run_id  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, help="Backtest run id. Defaults to the latest fill-first run.")
    parser.add_argument("--max-orders", type=int, default=50, help="Maximum missed/no-fill order rows to include.")
    parser.add_argument("--output", type=Path, help="Optional output file. Defaults to stdout.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=True) as conn:
        run_id = args.run_id if args.run_id is not None else load_latest_fill_first_backtest_run_id(conn)
        inputs = load_backtest_run_artifact_inputs(conn, run_id=int(run_id)) if run_id is not None else None
    report = build_backtest_fill_report(inputs, run_id=run_id, max_orders=args.max_orders)
    if args.format == "json":
        content = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    else:
        content = backtest_fill_report_to_markdown(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(content + "\n", encoding="utf-8")
    else:
        print(content)
    return 0 if report["status"] in {"ready", "review"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
