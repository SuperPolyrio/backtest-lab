#!/usr/bin/env python3
"""Export missing external evidence work orders for a fill-first backtest run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_source_missing_evidence import (  # noqa: E402
    build_external_source_missing_evidence_plan,
    external_source_missing_evidence_event_templates_jsonl,
    external_source_missing_evidence_plan_to_markdown,
)
from quant.backtest.run_artifacts import (  # noqa: E402
    load_backtest_run_artifact_inputs,
    load_latest_fill_first_backtest_run_id,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, default=None, help="Backtest run id. Defaults to latest fill-first run.")
    parser.add_argument("--source", default="live-shadow", help="Source label to put in exported order-state templates.")
    parser.add_argument("--max-orders", type=int, default=None, help="Maximum missing orders to include in each section.")
    parser.add_argument("--output", type=Path, default=None, help="Optional output path. Defaults to stdout.")
    parser.add_argument("--format", choices=("markdown", "json", "event-jsonl"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=True) as conn:
        run_id = args.run_id if args.run_id is not None else load_latest_fill_first_backtest_run_id(conn)
        inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id) if run_id is not None else None
    plan = build_external_source_missing_evidence_plan(
        inputs,
        run_id=run_id,
        source=args.source,
        max_orders=args.max_orders,
    )
    if args.format == "json":
        text = json.dumps(plan, ensure_ascii=False, indent=2, default=str)
    elif args.format == "event-jsonl":
        text = external_source_missing_evidence_event_templates_jsonl(plan)
    else:
        text = external_source_missing_evidence_plan_to_markdown(plan)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + ("\n" if text and not text.endswith("\n") else ""), encoding="utf-8")
    else:
        print(text)
    return 0 if plan.get("status") in {"ready", "review"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
