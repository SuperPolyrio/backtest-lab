#!/usr/bin/env python3
"""Run the historical backtest-core quality gate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.backtest_core_gate import (  # noqa: E402
    FAIL,
    READY,
    build_backtest_core_gate_report,
    core_gate_to_markdown,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--check-db", action="store_true", help="Audit latest persisted fill-first run core artifacts.")
    parser.add_argument("--include-latest-run-artifact", action="store_true", help="Include latest run artifact core checks.")
    parser.add_argument("--include-fill-evidence-validation", action="store_true", help="Validate latest run fill evidence.")
    parser.add_argument("--include-pytest", action="store_true", help="Run the focused backtest-core pytest subset.")
    parser.add_argument(
        "--stage-check",
        action="store_true",
        help="Run the normal core verification preset: latest run artifact/fill evidence when --check-db is set plus focused pytest.",
    )
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero for review gates.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    include_latest = bool(args.include_latest_run_artifact or (args.stage_check and args.check_db))
    include_fill = bool(args.include_fill_evidence_validation or (args.stage_check and args.check_db))
    include_pytest = bool(args.include_pytest or args.stage_check)
    if args.check_db:
        with postgres_connection(readonly=True) as conn:
            report = build_backtest_core_gate_report(
                args.project_root,
                conn=conn,
                check_db=True,
                include_latest_run_artifact=include_latest,
                include_fill_evidence_validation=include_fill,
                include_pytest=include_pytest,
            )
    else:
        report = build_backtest_core_gate_report(
            args.project_root,
            conn=None,
            check_db=False,
            include_latest_run_artifact=include_latest,
            include_fill_evidence_validation=include_fill,
            include_pytest=include_pytest,
        )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(core_gate_to_markdown(report))
    if report["status"] == READY:
        return 0
    if report["status"] == FAIL:
        return 2
    return 1 if args.strict_review else 0


if __name__ == "__main__":
    raise SystemExit(main())
