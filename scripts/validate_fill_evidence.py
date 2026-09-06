#!/usr/bin/env python3
"""Validate fill evidence distribution for a persisted backtest run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_evidence import (  # noqa: E402
    build_fill_evidence_validation_report,
    fill_evidence_validation_report_to_markdown,
)
from quant.backtest.run_artifacts import load_backtest_run_artifact_inputs, load_latest_fill_first_backtest_run_id  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, default=None, help="Backtest run id. Defaults to the latest fill-first run.")
    parser.add_argument("--max-orders", type=int, default=50, help="Maximum order evidence rows to print.")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero unless the report status is ready.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=True) as conn:
        run_id = args.run_id if args.run_id is not None else load_latest_fill_first_backtest_run_id(conn)
        inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id) if run_id is not None else None
    orders = list((inputs or {}).get("orders") or [])
    run_meta = ((inputs or {}).get("run") or {}).get("meta") or {}
    data_quality = run_meta.get("actual_data_quality") if isinstance(run_meta, dict) else {}
    fill_quality = data_quality.get("fill_quality") if isinstance(data_quality, dict) else None
    report = build_fill_evidence_validation_report(orders, fill_quality=fill_quality, max_orders=args.max_orders)
    report["run_id"] = run_id
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(fill_evidence_validation_report_to_markdown(report), end="")
    return 1 if args.strict and report.get("status") != "ready" else 0


if __name__ == "__main__":
    raise SystemExit(main())
