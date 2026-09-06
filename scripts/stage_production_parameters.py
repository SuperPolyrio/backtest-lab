#!/usr/bin/env python3
"""Stage reviewed fill-first strategy parameters from parameter search results."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.production_parameter_staging import (  # noqa: E402
    extract_parameter_search_results_reports,
    normalize_production_parameter_staging,
    upsert_production_parameter_staging,
)
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="parameter_search_results JSON, benchmark detail JSON, artifact JSON, or list.")
    parser.add_argument("--source", default="parameter-search-results")
    parser.add_argument("--strategy-name", default="unknown")
    parser.add_argument("--strategy-version", default="unknown")
    parser.add_argument("--universe-name", default="")
    parser.add_argument("--benchmark-id", type=int)
    parser.add_argument("--run-id", type=int)
    parser.add_argument("--approve", action="store_true", help="Stage as approved instead of pending.")
    parser.add_argument("--approved-by", default=None, help="Reviewer/operator id. Required with --approve.")
    parser.add_argument("--force-review", action="store_true", help="Allow writing review/blocked reports as explicit review records.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print normalized staging rows without writing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.approve and not args.approved_by:
        raise SystemExit("--approved-by is required when --approve is used")
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    reports = extract_parameter_search_results_reports(payload)
    if not reports:
        raise SystemExit("input does not contain a parameter_search_results report")
    status = "approved" if args.approve else "pending"
    normalized = [
        normalize_production_parameter_staging(
            report,
            status=status,
            source=args.source,
            approved_by=args.approved_by,
            force_review=args.force_review,
            benchmark_id=args.benchmark_id,
            run_id=args.run_id,
            strategy_name=args.strategy_name,
            strategy_version=args.strategy_version,
            universe_name=args.universe_name,
        )
        for report in reports
    ]
    if args.dry_run:
        print(json.dumps({"rows": normalized, "count": len(normalized), "status": status}, ensure_ascii=False, indent=2, default=str))
        return 0
    with postgres_connection(readonly=False) as conn:
        create_schema(conn)
        written = upsert_production_parameter_staging(
            conn,
            [
                {
                    **report,
                    "benchmark_id": args.benchmark_id,
                    "run_id": args.run_id,
                    "strategy_name": args.strategy_name,
                    "strategy_version": args.strategy_version,
                    "universe_name": args.universe_name,
                }
                for report in reports
            ],
            status=status,
            source=args.source,
            approved_by=args.approved_by,
            force_review=args.force_review,
        )
        conn.commit()
    print(json.dumps({"written": written, "status": status}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
