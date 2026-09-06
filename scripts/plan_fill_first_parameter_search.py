#!/usr/bin/env python3
"""Plan a fill-first parameter search before running robustness analysis."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.parameter_search_plan import (  # noqa: E402
    READY,
    build_parameter_search_plan,
    load_json_mapping,
    parameter_search_plan_to_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-payload-json", type=Path, help="JSON object merged into every planned request payload.")
    parser.add_argument("--grid-json", type=Path, help="JSON object mapping parameter fields to value lists.")
    parser.add_argument("--universe-name", default="fill_first_parameter_search")
    parser.add_argument("--max-runs", type=int, default=250)
    parser.add_argument("--evidence-mode", action="append", default=[], help="Evidence mode to include. Repeatable.")
    parser.add_argument("--entry-threshold", action="append", default=[])
    parser.add_argument("--exit-threshold", action="append", default=[])
    parser.add_argument("--execution-profile", action="append", default=[])
    parser.add_argument("--liquidity-cap-pct", action="append", default=[])
    parser.add_argument("--latency-blocks", action="append", default=[])
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict-review", action="store_true", help="Return non-zero when the plan is not ready.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_payload = load_json_mapping(args.base_payload_json) if args.base_payload_json else {}
    grid = load_json_mapping(args.grid_json) if args.grid_json else None
    cli_grid = _grid_from_args(args)
    if cli_grid:
        grid = {**(grid or {}), **cli_grid}
    report = build_parameter_search_plan(
        base_payload=base_payload,
        grid=grid,
        evidence_modes=args.evidence_mode or None or ("train", "test", "walk_forward"),
        max_runs=args.max_runs,
        universe_name=args.universe_name,
    )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(parameter_search_plan_to_markdown(report))
    return 2 if args.strict_review and report.get("status") != READY else 0


def _grid_from_args(args: argparse.Namespace) -> dict[str, list[Any]]:
    mapping = {
        "entry_threshold": args.entry_threshold,
        "exit_threshold": args.exit_threshold,
        "execution_profile": args.execution_profile,
        "liquidity_cap_pct": args.liquidity_cap_pct,
        "latency_blocks": args.latency_blocks,
    }
    return {key: values for key, values in mapping.items() if values}


if __name__ == "__main__":
    raise SystemExit(main())
