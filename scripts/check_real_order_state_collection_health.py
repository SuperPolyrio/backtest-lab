#!/usr/bin/env python3
"""Check real order state collection freshness for fill-first calibration."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.order_state_collection import load_real_order_state_collection_states  # noqa: E402
from quant.backtest.order_state_collection_health import (  # noqa: E402
    READY,
    UNKNOWN,
    collection_health_to_markdown,
    evaluate_collection_state_health,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=None, help="Only check this collection source.")
    parser.add_argument("--state-key", default=None, help="Only check this collection state key.")
    parser.add_argument("--limit", type=int, default=100, help="Maximum state rows to inspect.")
    parser.add_argument("--max-stale-seconds", type=int, default=900, help="Review when last success is older than this.")
    parser.add_argument("--min-events-written", type=int, default=0, help="Review when the last poll wrote fewer events.")
    parser.add_argument("--no-require-success", action="store_true", help="Allow rows without last_success_at.")
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument("--allow-empty", action="store_true", help="Exit 0 when no collection state rows exist.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=True) as conn:
        states = load_real_order_state_collection_states(
            conn,
            source=args.source,
            state_key=args.state_key,
            limit=args.limit,
        )
    report = evaluate_collection_state_health(
        states,
        max_stale_seconds=args.max_stale_seconds,
        min_events_written=args.min_events_written,
        require_success=not args.no_require_success,
    )
    if args.format == "markdown":
        print(collection_health_to_markdown(report))
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))

    if report["status"] == READY or (args.allow_empty and report["status"] == UNKNOWN and not states):
        return 0
    return 2 if report["status"] == UNKNOWN else 1


if __name__ == "__main__":
    raise SystemExit(main())
