#!/usr/bin/env python3
"""Build a guarded paper/live runner plan from enabled strategy state."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.strategy_activation import load_strategy_enable_state  # noqa: E402
from quant.backtest.strategy_runner_guard import (  # noqa: E402
    build_strategy_runner_plan,
    strategy_runner_plan_to_markdown,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-mode", choices=("paper", "live"), default="paper")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--include-blocked", action="store_true", help="Include non-runnable rows in the plan items.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when no runnable rows are available.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=True) as conn:
        rows = load_strategy_enable_state(
            conn,
            target_mode=args.target_mode,
            enabled_only=not args.include_blocked,
            limit=args.limit,
        )
    plan = build_strategy_runner_plan(
        rows,
        target_mode=args.target_mode,
        include_blocked=args.include_blocked,
        limit=args.limit,
    )
    if args.format == "json":
        print(json.dumps(plan, ensure_ascii=False, indent=2, default=str))
    else:
        print(strategy_runner_plan_to_markdown(plan))
    if args.strict and plan.get("runnable_count", 0) <= 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
