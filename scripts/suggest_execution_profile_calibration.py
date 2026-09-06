#!/usr/bin/env python3
"""Suggest builtin execution profile parameters from fill calibration samples."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.calibration_report import DEFAULT_BUCKET_FIELDS, build_periodic_calibration_report  # noqa: E402
from quant.backtest.execution_profile_calibration import (  # noqa: E402
    execution_profile_suggestions_from_report,
    execution_profile_suggestions_to_json,
    execution_profile_suggestions_to_markdown,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, default=None, help="Filter calibration samples by backtest run_id.")
    parser.add_argument("--source", default=None, help="Filter calibration samples by source, for example live-shadow.")
    parser.add_argument("--market-slug", default=None, help="Filter calibration samples by market slug.")
    parser.add_argument("--since", default=None, help="Filter observed_at >= this timestamp.")
    parser.add_argument("--until", default=None, help="Filter observed_at < this timestamp.")
    parser.add_argument("--limit", type=int, default=50000, help="Maximum samples to read.")
    parser.add_argument("--min-bucket-samples", type=int, default=3, help="Minimum rows required to suggest a bucket.")
    parser.add_argument("--bucket-field", action="append", default=None, help="Bucket field to include; repeatable.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown", help="Output format.")
    parser.add_argument("--output", type=Path, default=None, help="Write suggestions to this file instead of stdout.")
    return parser.parse_args()


def fetch_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    with postgres_connection(readonly=True) as conn:
        if not _table_exists(conn):
            return []
        filters: list[str] = []
        params: list[Any] = []
        if args.run_id is not None:
            filters.append("run_id = %s")
            params.append(int(args.run_id))
        if args.source:
            filters.append("source = %s")
            params.append(args.source)
        if args.market_slug:
            filters.append("market_slug = %s")
            params.append(args.market_slug)
        if args.since:
            filters.append("observed_at >= %s")
            params.append(args.since)
        if args.until:
            filters.append("observed_at < %s")
            params.append(args.until)
        where_sql = "WHERE " + " AND ".join(filters) if filters else ""
        params.append(max(1, int(args.limit)))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                FROM quant.quant_backtest_calibration_orders
                {where_sql}
                ORDER BY observed_at DESC NULLS LAST, calibration_id DESC
                LIMIT %s
                """,
                params,
            )
            return [dict(row) for row in cur.fetchall()]


def main() -> int:
    args = parse_args()
    rows = fetch_rows(args)
    bucket_fields = tuple(args.bucket_field or DEFAULT_BUCKET_FIELDS)
    report = build_periodic_calibration_report(
        rows,
        bucket_fields=bucket_fields,
        min_bucket_samples=args.min_bucket_samples,
    )
    suggestions = execution_profile_suggestions_from_report(report, min_samples=args.min_bucket_samples)
    if args.format == "json":
        text = execution_profile_suggestions_to_json(suggestions)
    else:
        text = execution_profile_suggestions_to_markdown(suggestions)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quant.quant_backtest_calibration_orders') IS NOT NULL AS exists")
        row = cur.fetchone()
    if isinstance(row, dict):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


if __name__ == "__main__":
    raise SystemExit(main())
