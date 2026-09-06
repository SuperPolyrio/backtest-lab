#!/usr/bin/env python3
"""Audit persisted artifacts for a fill-first backtest run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.run_artifacts import (  # noqa: E402
    MISSING,
    READY,
    UNKNOWN,
    backtest_run_artifact_report_to_markdown,
    build_backtest_run_artifact_report,
    load_backtest_run_artifact_inputs,
    load_latest_fill_first_backtest_run_id,
    repair_backtest_run_fill_evidence_artifacts,
    repair_backtest_run_fill_quality_artifacts,
)
from quant.backtest.external_source_missing_evidence import (  # noqa: E402
    write_external_source_missing_evidence_task_pack,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, default=None, help="Backtest run id to audit. Defaults to the latest fill-first run.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--repair-fill-quality", action="store_true", help="Rebuild missing fill_quality artifact and metrics from stored orders.")
    parser.add_argument("--repair-fill-evidence", action="store_true", help="Persist order evidence fields from order meta and rebuild fill_quality metrics.")
    parser.add_argument("--no-rebuild-fill-quality", action="store_true", help="With --repair-fill-evidence, update order evidence fields only.")
    parser.add_argument(
        "--export-missing-evidence-dir",
        type=Path,
        default=None,
        help="Write a missing external evidence task pack for this run into the given directory.",
    )
    parser.add_argument("--strict", action="store_true", help="Exit non-zero for review/unknown reports.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repair_requested = args.repair_fill_quality or args.repair_fill_evidence
    with postgres_connection(readonly=not repair_requested) as conn:
        run_id = args.run_id if args.run_id is not None else load_latest_fill_first_backtest_run_id(conn)
        if args.repair_fill_evidence and run_id is not None:
            repair = repair_backtest_run_fill_evidence_artifacts(
                conn,
                run_id=run_id,
                rebuild_fill_quality=not args.no_rebuild_fill_quality,
            )
            report = repair["after"]
            report["repair"] = {key: value for key, value in repair.items() if key not in {"before", "after"}}
        elif args.repair_fill_quality and run_id is not None:
            repair = repair_backtest_run_fill_quality_artifacts(conn, run_id=run_id)
            report = repair["after"]
            report["repair"] = {key: value for key, value in repair.items() if key not in {"before", "after"}}
        else:
            inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id) if run_id is not None else None
            report = build_backtest_run_artifact_report(inputs, run_id=run_id)
    if args.export_missing_evidence_dir:
        plan = report.get("external_source_missing_evidence_plan")
        if isinstance(plan, dict):
            report["missing_evidence_task_pack"] = write_external_source_missing_evidence_task_pack(
                plan,
                args.export_missing_evidence_dir,
                run_id=run_id,
            )
        else:
            report["missing_evidence_task_pack"] = {
                "status": "missing",
                "run_id": run_id,
                "output_dir": str(args.export_missing_evidence_dir),
                "reason": "artifact_report_has_no_external_source_missing_evidence_plan",
            }
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(backtest_run_artifact_report_to_markdown(report))
    if report["status"] == READY:
        return 0
    if report["status"] in {MISSING, UNKNOWN}:
        return 2
    return 1 if args.strict else 0


if __name__ == "__main__":
    raise SystemExit(main())
