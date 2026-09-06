#!/usr/bin/env python3
"""Export a shadow/live collection plan from a persisted backtest run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.run_artifacts import load_latest_fill_first_backtest_run_id  # noqa: E402
from quant.backtest.shadow_live_plan import (  # noqa: E402
    build_shadow_live_order_plan,
    load_shadow_live_plan_inputs,
    shadow_live_event_templates_jsonl,
    shadow_live_order_plan_to_markdown,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, default=None, help="Backtest run id. Defaults to latest fill-first run.")
    parser.add_argument("--source", default="live-shadow", help="Source label used in event templates.")
    parser.add_argument("--limit", type=int, default=5000, help="Maximum orders to load from DB.")
    parser.add_argument("--max-orders", type=int, default=None, help="Maximum orders to include in the exported plan.")
    parser.add_argument("--exclude-rejected", action="store_true", help="Skip simulated rejected orders in the plan.")
    parser.add_argument("--output", type=Path, default=None, help="Optional output file. Defaults to stdout.")
    parser.add_argument("--format", choices=("markdown", "json", "event-jsonl"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=True) as conn:
        run_id = args.run_id if args.run_id is not None else load_latest_fill_first_backtest_run_id(conn)
        inputs = load_shadow_live_plan_inputs(conn, run_id=run_id, limit=args.limit) if run_id is not None else None
    plan = build_shadow_live_order_plan(
        inputs,
        source=args.source,
        include_rejected=not args.exclude_rejected,
        max_orders=args.max_orders,
    )
    if args.format == "json":
        text = json.dumps(plan, ensure_ascii=False, indent=2, default=str)
    elif args.format == "event-jsonl":
        text = shadow_live_event_templates_jsonl(plan)
    else:
        text = shadow_live_order_plan_to_markdown(plan)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + ("\n" if text and not text.endswith("\n") else ""), encoding="utf-8")
    else:
        print(text)
    return 0 if plan.get("status") == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
