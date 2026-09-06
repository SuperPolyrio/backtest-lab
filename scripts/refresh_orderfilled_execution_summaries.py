#!/usr/bin/env python3
"""Refresh execution summaries from materialized block-close rows.

Target tables:
- quant.market_token_execution_summary
- quant.market_execution_summary
- quant.event_execution_summary
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.materialized_execution_summary import (  # noqa: E402
    ExecutionSummaryFilters,
    aggregate_event_execution_summary_rows,
    aggregate_execution_summary_rows,
    aggregate_market_execution_summary_rows,
    upsert_event_execution_summaries,
    upsert_execution_summaries,
    upsert_market_execution_summaries,
)
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-id", type=int)
    parser.add_argument("--market-slug")
    parser.add_argument("--token-id")
    parser.add_argument("--token-side", choices=("YES", "NO", "yes", "no"))
    parser.add_argument("--from-block", type=int)
    parser.add_argument("--to-block", type=int)
    parser.add_argument("--summary-level", choices=("token", "market", "event", "all"), default="token")
    parser.add_argument("--write", action="store_true", help="Persist summaries into quant execution summary tables.")
    parser.add_argument("--limit-preview", type=int, default=5, help="Number of summary rows to include in stdout.")
    return parser.parse_args()


def filters_from_args(args: argparse.Namespace) -> ExecutionSummaryFilters:
    return ExecutionSummaryFilters(
        market_id=args.market_id,
        market_slug=args.market_slug,
        token_id=args.token_id,
        token_side=args.token_side.upper() if args.token_side else None,
        from_block=args.from_block,
        to_block=args.to_block,
    )


def main() -> int:
    args = parse_args()
    filters = filters_from_args(args)
    readonly = not bool(args.write)
    with postgres_connection(readonly=readonly) as conn:
        if args.write:
            create_schema(conn)
        token_rows = aggregate_execution_summary_rows(conn, filters) if args.summary_level in {"token", "all"} else []
        token_written = upsert_execution_summaries(conn, token_rows) if args.write and token_rows else 0
        market_rows = aggregate_market_execution_summary_rows(conn, filters) if args.summary_level in {"market", "all"} else []
        market_written = upsert_market_execution_summaries(conn, market_rows) if args.write and market_rows else 0
        event_rows = aggregate_event_execution_summary_rows(conn, filters) if args.summary_level in {"event", "all"} else []
        event_written = upsert_event_execution_summaries(conn, event_rows) if args.write and event_rows else 0
        payload = {
            "status": "ready",
            "write": bool(args.write),
            "summary_level": args.summary_level,
            "summary_rows": len(token_rows) + len(market_rows) + len(event_rows),
            "token_rows": len(token_rows),
            "market_rows": len(market_rows),
            "event_rows": len(event_rows),
            "written_rows": token_written + market_written + event_written,
            "token_written": token_written,
            "market_written": market_written,
            "event_written": event_written,
            "filters": {
                "market_id": filters.market_id,
                "market_slug": filters.market_slug,
                "token_id": filters.token_id,
                "token_side": filters.token_side,
                "from_block": filters.from_block,
                "to_block": filters.to_block,
            },
            "preview": {
                "token": token_rows[: max(0, int(args.limit_preview))],
                "market": market_rows[: max(0, int(args.limit_preview))],
                "event": event_rows[: max(0, int(args.limit_preview))],
            },
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
