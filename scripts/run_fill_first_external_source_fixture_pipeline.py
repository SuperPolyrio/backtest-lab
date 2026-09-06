#!/usr/bin/env python3
"""Run the fill-first external evidence fixture pipeline.

Default mode is dry-run. Use --write to import the generated fixture files into
Postgres, --check-db to verify table counts plus external source health, or
--rollback-smoke to call create_schema and prove the DB schema accepts fixture evidence without leaving rows behind.
Run-specific mode also calls build_external_source_run_coverage_report and emits run_coverage
with order_state_coverage_pct and calibration_coverage_pct.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_source_fixture_pipeline import (  # noqa: E402
    FAIL,
    READY,
    fill_first_external_source_fixture_pipeline_to_markdown,
    run_fill_first_external_source_fixture_pipeline,
)
from quant.backtest.run_artifacts import load_latest_fill_first_backtest_run_id  # noqa: E402
from quant.backtest.shadow_live_plan import build_shadow_live_order_plan, load_shadow_live_plan_inputs  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "runtime_outputs" / "fill_first_external_source_fixture_pipeline",
        help="Directory to write fixture files.",
    )
    parser.add_argument("--run-id", type=int, default=9001, help="Run id embedded in fixture rows. For --write, pass an existing run id.")
    parser.add_argument("--use-latest-run-id", action="store_true", help="Use latest fill-first quant_backtest_runs.run_id instead of --run-id.")
    parser.add_argument("--from-run-orders", action="store_true", help="Build fixture rows from persisted quant_backtest_orders for the selected run.")
    parser.add_argument("--limit", type=int, default=5000, help="Maximum persisted orders to load when --from-run-orders is set.")
    parser.add_argument("--max-orders", type=int, default=None, help="Maximum persisted orders to include when --from-run-orders is set.")
    parser.add_argument("--source-prefix", default="fixture-pipeline", help="Source prefix for generated rows and state keys.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    parser.add_argument("--write", action="store_true", help="Import generated fixture files into Postgres.")
    parser.add_argument("--check-db", action="store_true", help="Verify imported fixture rows and external source state in Postgres.")
    parser.add_argument("--rollback-smoke", action="store_true", help="Call create_schema, insert fixture evidence in one transaction, and roll it back.")
    parser.add_argument("--build-calibration-smoke", action="store_true", help="During --rollback-smoke, also build and upsert calibration samples before rollback.")
    parser.add_argument("--build-calibration", action="store_true", help="After --write, build persisted fill/cost calibration samples from imported fixture evidence.")
    parser.add_argument("--timeout", type=int, default=300, help="Per-import command timeout seconds.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = args.run_id
    if args.use_latest_run_id:
        with postgres_connection(readonly=True) as conn:
            latest = load_latest_fill_first_backtest_run_id(conn)
        if latest is None:
            raise SystemExit("--use-latest-run-id requested, but no fill-first quant_backtest_runs rows exist")
        run_id = int(latest)
    fixture_plan = None
    if args.from_run_orders:
        with postgres_connection(readonly=True) as conn:
            inputs = load_shadow_live_plan_inputs(conn, run_id=run_id, limit=args.limit)
        fixture_plan = build_shadow_live_order_plan(
            inputs,
            source=f"{args.source_prefix}-order-state",
            max_orders=args.max_orders,
        )

    conn = None
    if args.check_db or args.rollback_smoke or args.build_calibration:
        conn_context = postgres_connection(readonly=not (args.rollback_smoke or (args.build_calibration and args.write)))
        conn = conn_context.__enter__()
    else:
        conn_context = None
    try:
        report = run_fill_first_external_source_fixture_pipeline(
            args.output_dir,
            project_root=PROJECT_ROOT,
            run_id=run_id,
            source_prefix=args.source_prefix,
            overwrite=args.overwrite,
            write=args.write,
            check_db=args.check_db,
            conn=conn,
            command_timeout_seconds=max(1, int(args.timeout)),
            rollback_smoke=args.rollback_smoke,
            fixture_plan=fixture_plan,
            calibration_smoke=bool(args.build_calibration_smoke),
            build_calibration=bool(args.build_calibration),
        )
    finally:
        if conn_context is not None:
            conn_context.__exit__(None, None, None)

    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(fill_first_external_source_fixture_pipeline_to_markdown(report))
    if report["status"] == READY:
        return 0
    if report["status"] == FAIL:
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
