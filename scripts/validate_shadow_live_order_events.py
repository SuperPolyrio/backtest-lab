#!/usr/bin/env python3
"""Validate filled shadow/live order-state events before import."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.shadow_live_validation import (  # noqa: E402
    load_shadow_live_event_rows,
    shadow_live_validation_to_markdown,
    validate_shadow_live_order_events,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="JSON or JSONL order-state event file.")
    parser.add_argument("--require-cost-fields", action="store_true", help="Treat missing live fee/rebate/cash/position fields as errors for filled events.")
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero when status is review.")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = load_shadow_live_event_rows(args.input)
    report = validate_shadow_live_order_events(rows, require_cost_fields=args.require_cost_fields)
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(shadow_live_validation_to_markdown(report))
    if report["status"] == "ready":
        return 0
    if report["status"] == "fail":
        return 2
    return 1 if args.strict_review else 0


if __name__ == "__main__":
    raise SystemExit(main())
