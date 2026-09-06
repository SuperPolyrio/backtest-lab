#!/usr/bin/env python3
"""Run the fill-first backtest project quality gate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_first_quality_gate import (  # noqa: E402
    FAIL,
    READY,
    build_fill_first_quality_gate_report,
    quality_gate_to_markdown,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--check-db", action="store_true", help="Check DB-backed tables and external import freshness.")
    parser.add_argument("--root", type=Path, action="append", default=[], help="Extra/local source directory to scan. Repeatable.")
    parser.add_argument(
        "--external-env-file",
        type=Path,
        action="append",
        default=[],
        help="Load external source KEY=VALUE settings for configured import dry-run. Repeatable.",
    )
    parser.add_argument(
        "--order-execution-env-file",
        type=Path,
        action="append",
        default=[],
        help="Load ORDER_EXECUTION_* KEY=VALUE settings for order execution audit and production preflight. Repeatable.",
    )
    parser.add_argument("--max-stale-seconds", type=int, default=86400)
    parser.add_argument("--no-allow-empty-external", action="store_true", help="Treat empty external source state as a failing/unknown gate.")
    parser.add_argument("--skip-smoke", action="store_true", help="Skip fixture smoke run.")
    parser.add_argument("--include-db-smoke", action="store_true", help="Run smoke_check with DB/ClickHouse sample checks.")
    parser.add_argument("--include-run-artifact-audit", action="store_true", help="Audit the latest persisted fill-first backtest run artifacts.")
    parser.add_argument("--include-fill-evidence-validation", action="store_true", help="Validate latest persisted fill-first run fill evidence types.")
    parser.add_argument("--include-external-run-coverage", action="store_true", help="Check latest fill-first run external evidence and calibration coverage.")
    parser.add_argument(
        "--include-external-fixture-db-smoke",
        action="store_true",
        help="Run rollback-smoke against Postgres external evidence tables.",
    )
    parser.add_argument("--include-pytest", action="store_true", help="Run quant/backtest pytest suite.")
    parser.add_argument("--include-frontend-smoke", action="store_true", help="Check the static HTML frontend entrypoint.")
    parser.add_argument(
        "--stage-check",
        action="store_true",
        help=(
            "Run the normal per-stage verification preset: fixture smoke, current schema run artifact fixture, "
            "external source onboarding fixture, production readiness CLI artifact gate fixture using "
            "--run-artifact-json, quant/backtest pytest, and static frontend smoke. With --check-db it also runs latest fill-first run artifact audit and external "
            "source rollback DB smoke."
        ),
    )
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero for review gates.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def quality_gate_options(args: argparse.Namespace) -> dict[str, bool]:
    return {
        "include_run_artifact_audit": bool(args.include_run_artifact_audit or (args.stage_check and args.check_db)),
        "include_fill_evidence_validation": bool(args.include_fill_evidence_validation or (args.stage_check and args.check_db)),
        "include_external_run_coverage": bool(args.include_external_run_coverage or (args.stage_check and args.check_db)),
        "include_external_fixture_db_smoke": bool(args.include_external_fixture_db_smoke or (args.stage_check and args.check_db)),
        "include_pytest": bool(args.include_pytest or args.stage_check),
        "include_frontend_smoke": bool(args.include_frontend_smoke or args.stage_check),
    }


def main() -> int:
    args = parse_args()
    options = quality_gate_options(args)
    conn_cm = postgres_connection(readonly=not options["include_external_fixture_db_smoke"]) if (args.check_db or options["include_external_fixture_db_smoke"]) else None
    if conn_cm is not None:
        with conn_cm as conn:
            report = build_fill_first_quality_gate_report(
                args.project_root,
                conn=conn,
                check_db=True,
                discovery_roots=args.root or None,
                max_stale_seconds=args.max_stale_seconds,
                allow_empty_external=not args.no_allow_empty_external,
                run_fixture_smoke=not args.skip_smoke,
                include_db_smoke=args.include_db_smoke,
                include_run_artifact_audit=options["include_run_artifact_audit"],
                include_fill_evidence_validation=options["include_fill_evidence_validation"],
                include_external_run_coverage=options["include_external_run_coverage"],
                include_external_fixture_db_smoke=options["include_external_fixture_db_smoke"],
                include_pytest=options["include_pytest"],
                include_frontend_smoke=options["include_frontend_smoke"],
                external_source_env_files=args.external_env_file,
                order_execution_env_files=args.order_execution_env_file,
            )
    else:
        report = build_fill_first_quality_gate_report(
            args.project_root,
            conn=None,
            check_db=False,
            discovery_roots=args.root or None,
            max_stale_seconds=args.max_stale_seconds,
            allow_empty_external=not args.no_allow_empty_external,
            run_fixture_smoke=not args.skip_smoke,
            include_db_smoke=args.include_db_smoke,
            include_run_artifact_audit=options["include_run_artifact_audit"],
            include_fill_evidence_validation=options["include_fill_evidence_validation"],
            include_external_run_coverage=options["include_external_run_coverage"],
            include_external_fixture_db_smoke=options["include_external_fixture_db_smoke"],
            include_pytest=options["include_pytest"],
            include_frontend_smoke=options["include_frontend_smoke"],
            external_source_env_files=args.external_env_file,
            order_execution_env_files=args.order_execution_env_file,
        )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(quality_gate_to_markdown(report))
    if report["status"] == READY:
        return 0
    if report["status"] == FAIL:
        return 2
    return 1 if args.strict_review else 0


if __name__ == "__main__":
    raise SystemExit(main())
