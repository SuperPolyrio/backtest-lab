#!/usr/bin/env python3
"""Validate, import, and build calibration samples from shadow/live order events."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.shadow_live_pipeline import (  # noqa: E402
    ShadowLivePipelineOptions,
    run_shadow_live_calibration_pipeline,
    shadow_live_pipeline_report_to_json,
    shadow_live_pipeline_report_to_markdown,
)
from quant.backtest.shadow_live_validation import load_shadow_live_event_rows  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="Filled shadow/live JSON or JSONL order-state events.")
    parser.add_argument("--run-id", required=True, type=int, help="Backtest run id whose simulated orders are being calibrated.")
    parser.add_argument("--source", default="live-shadow", help="Source label written to quant.real_order_state_events.")
    parser.add_argument("--calibration-source", default="live-shadow-calibration", help="Source label written to quant.quant_backtest_calibration_orders.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and build samples without writing database rows.")
    parser.add_argument("--include-open-events", action="store_true", help="Also build samples from non-terminal open/accepted events.")
    parser.add_argument("--no-require-cost-fields", action="store_true", help="Allow filled events missing live cost/cash/position fields.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not run schema initialization before writing.")
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero when pipeline status is review.")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    parser.add_argument("--output", type=Path, default=None, help="Write report to file instead of stdout.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_events = load_shadow_live_event_rows(args.input)
    options = ShadowLivePipelineOptions(
        source=args.source,
        calibration_source=args.calibration_source,
        run_id=args.run_id,
        require_cost_fields=not args.no_require_cost_fields,
        include_open_events=bool(args.include_open_events),
        dry_run=bool(args.dry_run),
    )
    with postgres_connection(readonly=bool(args.dry_run)) as conn:
        if not args.dry_run and not args.skip_init_schema:
            create_schema(conn)
        orders = _fetch_orders(conn, args.run_id)
        report = run_shadow_live_calibration_pipeline(
            orders=orders,
            raw_events=raw_events,
            options=options,
            conn=None if args.dry_run else conn,
        )
    text = shadow_live_pipeline_report_to_json(report) if args.format == "json" else shadow_live_pipeline_report_to_markdown(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    if report["status"] == "ready":
        return 0
    if report["status"] == "fail":
        return 2
    return 1 if args.strict_review else 0


def _fetch_orders(conn: Any, run_id: int) -> list[dict[str, Any]]:
    if not _table_exists(conn, "quant.quant_backtest_orders"):
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                o.*,
                r.market_slug AS run_market_slug,
                r.token_side AS run_token_side,
                r.price_source AS run_price_source,
                r.backtest_engine AS run_backtest_engine
            FROM quant.quant_backtest_orders o
            JOIN quant.quant_backtest_runs r ON r.run_id = o.run_id
            WHERE o.run_id = %s
            ORDER BY o.signal_index ASC, o.order_id ASC
            """,
            (int(run_id),),
        )
        return [dict(row) for row in cur.fetchall()]


def _table_exists(conn: Any, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (table_name,))
        row = cur.fetchone()
    if isinstance(row, dict):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


if __name__ == "__main__":
    raise SystemExit(main())
