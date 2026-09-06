#!/usr/bin/env python3
"""Build or record guarded paper/live strategy executor intents."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.guarded_executor import (  # noqa: E402
    build_guarded_executor_report,
    guarded_executor_report_to_markdown,
    record_guarded_execution_intents,
)
from quant.backtest.strategy_activation import load_strategy_enable_state  # noqa: E402
from quant.backtest.strategy_runner_guard import build_strategy_runner_plan  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-mode", choices=("paper", "live"), default="paper")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--include-blocked", action="store_true", help="Include non-runnable rows in the runner plan.")
    parser.add_argument("--record-intent", action="store_true", help="Write intent templates to quant.real_order_state_events.")
    parser.add_argument("--source", default="", help="Override order-state event source.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when no runnable intents are available.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=not args.record_intent) as conn:
        if args.record_intent:
            create_schema(conn)
        rows = load_strategy_enable_state(
            conn,
            target_mode=args.target_mode,
            enabled_only=not args.include_blocked,
            limit=args.limit,
        )
        runner_plan = build_strategy_runner_plan(
            rows,
            target_mode=args.target_mode,
            include_blocked=args.include_blocked,
            limit=args.limit,
        )
        report = build_guarded_executor_report(
            runner_plan,
            record_intent=args.record_intent,
            event_source=args.source.strip() or None,
        )
        if args.record_intent:
            report["recorded_event_count"] = record_guarded_execution_intents(conn, report)
            report["executor_status"] = "intent_recorded" if report["recorded_event_count"] else report["executor_status"]
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(guarded_executor_report_to_markdown(report))
    if args.strict and report.get("planned_action_count", 0) <= 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
