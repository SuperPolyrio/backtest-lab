#!/usr/bin/env python3
"""Write a local external evidence fixture bundle for fill-first validation.

The generated report prints a run_configured_external_source_imports.py
--env-file dry-run command for validating the bundle before any database write.
The bundle includes order-state, cost, incident, and external_signals JSONL files.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_source_fixture import (  # noqa: E402
    build_fill_first_external_source_fixture,
    build_fill_first_external_source_fixture_from_plan,
    fill_first_external_source_fixture_to_markdown,
)
from quant.backtest.run_artifacts import load_latest_fill_first_backtest_run_id  # noqa: E402
from quant.backtest.shadow_live_plan import build_shadow_live_order_plan, load_shadow_live_plan_inputs  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "runtime_outputs" / "fill_first_external_source_fixture",
        help="Directory to write JSONL and env-file outputs.",
    )
    parser.add_argument("--run-id", type=int, default=9001, help="Synthetic run_id embedded in fixture rows.")
    parser.add_argument(
        "--from-run-id",
        type=int,
        default=None,
        help="Build a run-specific fixture from persisted quant_backtest_orders for this run_id.",
    )
    parser.add_argument(
        "--use-latest-fill-first-run",
        action="store_true",
        help="Build a run-specific fixture from the latest persisted fill-first run.",
    )
    parser.add_argument("--limit", type=int, default=5000, help="Maximum persisted orders to load for run-specific fixture.")
    parser.add_argument("--max-orders", type=int, default=None, help="Maximum persisted orders to write into run-specific fixture.")
    parser.add_argument("--source-prefix", default="fixture", help="Source prefix for generated rows.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.from_run_id is not None or args.use_latest_fill_first_run:
        with postgres_connection(readonly=True) as conn:
            run_id = args.from_run_id
            if run_id is None:
                run_id = load_latest_fill_first_backtest_run_id(conn)
            inputs = load_shadow_live_plan_inputs(conn, run_id=run_id, limit=args.limit) if run_id is not None else None
        plan = build_shadow_live_order_plan(inputs, source=f"{args.source_prefix}-order-state", max_orders=args.max_orders)
        report = build_fill_first_external_source_fixture_from_plan(
            args.output_dir,
            plan,
            source_prefix=args.source_prefix,
            overwrite=args.overwrite,
        )
    else:
        report = build_fill_first_external_source_fixture(
            args.output_dir,
            run_id=args.run_id,
            source_prefix=args.source_prefix,
            overwrite=args.overwrite,
        )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(fill_first_external_source_fixture_to_markdown(report))
    return 0 if report["status"] == "ready" else 2


if __name__ == "__main__":
    raise SystemExit(main())
