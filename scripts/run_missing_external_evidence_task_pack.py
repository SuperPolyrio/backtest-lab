#!/usr/bin/env python3
"""Validate/import a missing external evidence task pack and refresh coverage."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.missing_evidence_pipeline import (  # noqa: E402
    missing_external_evidence_pipeline_to_markdown,
    run_missing_external_evidence_task_pack_pipeline,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task-pack-dir", required=True, type=Path, help="Directory created by audit_backtest_run_artifacts.py --export-missing-evidence-dir.")
    parser.add_argument("--events", type=Path, default=None, help="Optional filled event JSON/JSONL path. Defaults to the task-pack event_templates JSONL.")
    parser.add_argument("--run-id", type=int, default=None, help="Override run_id. Defaults to the task-pack plan run_id.")
    parser.add_argument("--source", default=None, help="Override real_order_state_events.source.")
    parser.add_argument("--state-key", default=None, help="Persist import freshness under this key when --write is set.")
    parser.add_argument("--write", action="store_true", help="Write events and calibration samples. Default is validation dry-run only.")
    parser.add_argument("--require-cost-fields", action="store_true", help="Treat missing fee/rebate/cash/position fields as validation errors for filled events.")
    parser.add_argument("--include-open-events", action="store_true", help="Allow open/accepted events to build calibration-like samples.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not run schema initialization before writing.")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.write:
        with postgres_connection() as conn:
            report = run_missing_external_evidence_task_pack_pipeline(
                task_pack_dir=args.task_pack_dir,
                conn=conn,
                events_path=args.events,
                run_id=args.run_id,
                source=args.source,
                state_key=args.state_key,
                write=True,
                require_cost_fields=args.require_cost_fields,
                include_open_events=args.include_open_events,
                skip_init_schema=args.skip_init_schema,
            )
    else:
        report = run_missing_external_evidence_task_pack_pipeline(
            task_pack_dir=args.task_pack_dir,
            events_path=args.events,
            run_id=args.run_id,
            source=args.source,
            write=False,
            require_cost_fields=args.require_cost_fields,
            include_open_events=args.include_open_events,
            skip_init_schema=args.skip_init_schema,
        )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(missing_external_evidence_pipeline_to_markdown(report))
    if report["status"] == "fail":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
