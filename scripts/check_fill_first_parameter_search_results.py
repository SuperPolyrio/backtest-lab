#!/usr/bin/env python3
"""Check fill-first parameter search result coverage before staging parameters."""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.parameter_search_plan import build_parameter_search_plan, load_json_mapping  # noqa: E402
from quant.backtest.parameter_search_results import (  # noqa: E402
    READY,
    build_parameter_search_results_report,
    load_json_report,
    load_json_rows,
    parameter_search_results_to_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan-json", type=Path, help="Parameter search plan JSON. Defaults to a generated dry-run plan.")
    parser.add_argument("--results-json", required=True, type=Path, help="JSON list or object containing parameter result rows.")
    parser.add_argument("--base-payload-json", type=Path, help="Base payload for generated default plan when --plan-json is omitted.")
    parser.add_argument("--grid-json", type=Path, help="Grid JSON for generated default plan when --plan-json is omitted.")
    parser.add_argument("--max-runs", type=int, default=250)
    parser.add_argument("--min-result-coverage-pct", default="100")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict-review", action="store_true", help="Return non-zero unless result coverage and robustness are ready.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.plan_json:
        plan = load_json_report(args.plan_json)
    else:
        base_payload = load_json_mapping(args.base_payload_json) if args.base_payload_json else {}
        grid = load_json_mapping(args.grid_json) if args.grid_json else None
        plan = build_parameter_search_plan(base_payload=base_payload, grid=grid, max_runs=args.max_runs)
    rows = load_json_rows(args.results_json)
    report = build_parameter_search_results_report(
        plan,
        rows,
        min_result_coverage_pct=Decimal(str(args.min_result_coverage_pct)),
    )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(parameter_search_results_to_markdown(report))
    return 2 if args.strict_review and report.get("status") != READY else 0


if __name__ == "__main__":
    raise SystemExit(main())
