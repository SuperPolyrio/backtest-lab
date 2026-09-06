#!/usr/bin/env python3
"""Run or preview a fill-first parameter search batch."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.parameter_search_runner import (  # noqa: E402
    READY,
    load_parameter_search_plan_or_build,
    parameter_search_batch_to_markdown,
    run_parameter_search_plan,
)
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan-json", type=Path, help="Existing parameter search plan JSON.")
    parser.add_argument("--base-payload-json", type=Path, help="Base payload for generated plan when --plan-json is omitted.")
    parser.add_argument("--grid-json", type=Path, help="Grid JSON for generated plan when --plan-json is omitted.")
    parser.add_argument("--universe-name", default="fill_first_parameter_search")
    parser.add_argument("--max-runs", type=int, default=250)
    parser.add_argument("--evidence-mode", action="append", default=[], help="Evidence mode to include. Repeatable.")
    parser.add_argument("--execute", action="store_true", help="Actually create and execute backtest runs. Default is dry-run preview.")
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--stage-parameters", action="store_true", help="Build a production parameter staging preview from ready results.")
    parser.add_argument("--write-staging", action="store_true", help="Write staging row to quant.production_parameter_staging.")
    parser.add_argument("--staging-status", choices=("pending", "approved", "rejected", "archived"), default="pending")
    parser.add_argument("--staging-source", default="parameter-search-batch-runner")
    parser.add_argument("--strategy-name", default="unknown")
    parser.add_argument("--strategy-version", default="unknown")
    parser.add_argument("--approved-by", default=None)
    parser.add_argument("--force-review-staging", action="store_true", help="Allow blocked/review reports to be written as explicit review records.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict-review", action="store_true", help="Return non-zero unless executed result coverage is ready.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.write_staging and not args.stage_parameters:
        raise SystemExit("--write-staging requires --stage-parameters")
    if args.write_staging and not args.execute:
        raise SystemExit("--write-staging requires --execute")
    if args.staging_status == "approved" and not args.approved_by:
        raise SystemExit("--approved-by is required with --staging-status approved")

    plan = load_parameter_search_plan_or_build(
        plan_json=args.plan_json,
        base_payload_json=args.base_payload_json,
        grid_json=args.grid_json,
        evidence_modes=args.evidence_mode or None,
        max_runs=args.max_runs,
        universe_name=args.universe_name,
    )
    dry_run = not args.execute
    if dry_run:
        report = run_parameter_search_plan(
            plan,
            dry_run=True,
            max_runs=args.max_runs,
            stage_parameters=args.stage_parameters,
            staging_status=args.staging_status,
            staging_source=args.staging_source,
            approved_by=args.approved_by,
            force_review_staging=args.force_review_staging,
            strategy_name=args.strategy_name,
            strategy_version=args.strategy_version,
            universe_name=args.universe_name,
        )
    else:
        with postgres_connection(readonly=False) as conn:
            create_schema(conn)
            report = run_parameter_search_plan(
                plan,
                conn=conn,
                dry_run=False,
                max_runs=args.max_runs,
                stop_on_error=args.stop_on_error,
                stage_parameters=args.stage_parameters,
                write_staging=args.write_staging,
                staging_status=args.staging_status,
                staging_source=args.staging_source,
                approved_by=args.approved_by,
                force_review_staging=args.force_review_staging,
                strategy_name=args.strategy_name,
                strategy_version=args.strategy_version,
                universe_name=args.universe_name,
            )
            conn.commit()

    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default))
    else:
        print(parameter_search_batch_to_markdown(report))
    return 2 if args.strict_review and report.get("status") != READY else 0


def _json_default(value: Any) -> str:
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
