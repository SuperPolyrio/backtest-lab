#!/usr/bin/env python3
"""Check production readiness for fill-first paper/live execution."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_first_production_readiness import (  # noqa: E402
    READY,
    build_fill_first_production_readiness_report,
    fill_first_production_readiness_to_markdown,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-mode", choices=("paper", "live"), default="paper")
    parser.add_argument("--env-file", type=Path, action="append", default=[], help="Load KEY=VALUE settings before checking.")
    parser.add_argument("--check-db", action="store_true", help="Check external source import freshness in Postgres.")
    parser.add_argument("--run-id", type=int, default=None, help="Require run-level external evidence coverage for this backtest run when --check-db is set.")
    parser.add_argument(
        "--run-artifact-json",
        type=Path,
        default=None,
        help="Load a local run artifact report JSON and use its paper_live_evidence_gate_report for offline preflight verification.",
    )
    parser.add_argument("--max-stale-seconds", type=int, default=86400, help="Block when the last external import success is older than this.")
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero for review/blocker findings.")
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
            report = build_fill_first_production_readiness_report(
                target_mode=args.target_mode,
                env_files=args.env_file,
                project_root=PROJECT_ROOT,
                conn=conn,
                check_db=True,
                run_id=run_id,
                run_artifact_report=run_artifact_report,
                max_stale_seconds=args.max_stale_seconds,
            )
    else:
        report = build_fill_first_production_readiness_report(
            target_mode=args.target_mode,
            env_files=args.env_file,
            project_root=PROJECT_ROOT,
            check_db=False,
            run_id=run_id,
            run_artifact_report=run_artifact_report,
            max_stale_seconds=args.max_stale_seconds,
        )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(fill_first_production_readiness_to_markdown(report))
    if report["status"] == READY:
        return 0
    return 1 if args.strict_review else 0


if __name__ == "__main__":
    raise SystemExit(main())
