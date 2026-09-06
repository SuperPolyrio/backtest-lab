#!/usr/bin/env python3
"""Plan the fill-first production launch sequence."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.production_launch_checklist import (  # noqa: E402
    READY,
    build_fill_first_production_launch_checklist,
    production_launch_checklist_to_markdown,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-mode", choices=("paper", "live"), default="paper")
    parser.add_argument("--env-file", type=Path, action="append", default=[], help="Legacy: load private KEY=VALUE settings as external source env. Repeatable.")
    parser.add_argument("--external-env-file", type=Path, action="append", default=[], help="Load external evidence source KEY=VALUE settings. Repeatable.")
    parser.add_argument("--order-execution-env-file", type=Path, action="append", default=[], help="Load ORDER_EXECUTION_* KEY=VALUE settings. Repeatable.")
    parser.add_argument("--check-db", action="store_true", help="Include external source freshness and run evidence DB checks.")
    parser.add_argument("--run-id", type=int, default=None, help="Backtest run id that must have external evidence before launch.")
    parser.add_argument(
        "--run-artifact-json",
        type=Path,
        default=None,
        help="Use a local run artifact JSON for paper/live evidence gate verification.",
    )
    parser.add_argument("--max-stale-seconds", type=int, default=86400)
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero when launch checklist is not ready.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def load_run_artifact_report(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"run artifact JSON must contain an object: {path}")
    return data


def main() -> int:
    args = parse_args()
    try:
        run_artifact_report = load_run_artifact_report(args.run_artifact_json)
    except Exception as exc:
        print(f"failed to load --run-artifact-json: {exc}", file=sys.stderr)
        return 2
    run_id = args.run_id
    if run_id is None and isinstance(run_artifact_report, dict) and run_artifact_report.get("run_id") is not None:
        run_id = int(run_artifact_report["run_id"])
    if args.check_db:
        with postgres_connection(readonly=True) as conn:
            report = build_fill_first_production_launch_checklist(
                target_mode=args.target_mode,
                env_files=args.env_file,
                external_source_env_files=args.external_env_file or None,
                order_execution_env_files=args.order_execution_env_file,
                project_root=PROJECT_ROOT,
                conn=conn,
                check_db=True,
                run_id=run_id,
                run_artifact_report=run_artifact_report,
                max_stale_seconds=args.max_stale_seconds,
            )
    else:
        report = build_fill_first_production_launch_checklist(
            target_mode=args.target_mode,
            env_files=args.env_file,
            external_source_env_files=args.external_env_file or None,
            order_execution_env_files=args.order_execution_env_file,
            project_root=PROJECT_ROOT,
            check_db=False,
            run_id=run_id,
            run_artifact_report=run_artifact_report,
            max_stale_seconds=args.max_stale_seconds,
        )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(production_launch_checklist_to_markdown(report))
    return 1 if args.strict_review and report.get("status") != READY else 0


if __name__ == "__main__":
    raise SystemExit(main())
