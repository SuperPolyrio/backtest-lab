#!/usr/bin/env python3
"""Build calibration samples from simulated orders and real order state events."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.calibration import build_calibration_report, upsert_calibration_orders  # noqa: E402
from quant.backtest.calibration_samples import build_calibration_samples_from_order_events  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, default=None, help="Build samples for one backtest run_id.")
    parser.add_argument("--event-source", default=None, help="Filter real_order_state_events.source.")
    parser.add_argument("--source", default="auto-real-order-state", help="Source written to calibration samples.")
    parser.add_argument("--since", default=None, help="Filter real event_time >= this timestamp.")
    parser.add_argument("--until", default=None, help="Filter real event_time < this timestamp.")
    parser.add_argument("--run-limit", type=int, default=50, help="Max run ids to process when --run-id is omitted.")
    parser.add_argument("--dry-run", action="store_true", help="Print report only; do not write calibration table.")
    parser.add_argument("--include-open-events", action="store_true", help="Also build samples from non-terminal open/accepted events.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not run schema initialization before writing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=bool(args.dry_run)) as conn:
        if not args.dry_run and not args.skip_init_schema:
            create_schema(conn)
        run_ids = [args.run_id] if args.run_id is not None else _candidate_run_ids(conn, args)
        orders = _fetch_orders(conn, run_ids)
        events = _fetch_events(conn, run_ids, args)
        samples = build_calibration_samples_from_order_events(
            orders,
            events,
            source=args.source,
            include_open_events=bool(args.include_open_events),
        )
        written = 0
        if not args.dry_run:
            written = upsert_calibration_orders(conn, samples)
    report = build_calibration_report(samples)
    print(
        json.dumps(
            {
                "run_ids": run_ids,
                "orders_read": len(orders),
                "events_read": len(events),
                "samples_built": len(samples),
                "samples_written": written,
                "dry_run": bool(args.dry_run),
                "report": report,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )
    return 0


def _candidate_run_ids(conn: Any, args: argparse.Namespace) -> list[int]:
    if not _table_exists(conn, "quant.real_order_state_events"):
        return []
    filters = ["run_id IS NOT NULL"]
    params: list[Any] = []
    if args.event_source:
        filters.append("source = %s")
        params.append(args.event_source)
    if args.since:
        filters.append("event_time >= %s")
        params.append(args.since)
    if args.until:
        filters.append("event_time < %s")
        params.append(args.until)
    params.append(max(1, int(args.run_limit)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT run_id
            FROM quant.real_order_state_events
            WHERE {" AND ".join(filters)}
            GROUP BY run_id
            ORDER BY max(COALESCE(event_time, created_at)) DESC, run_id DESC
            LIMIT %s
            """,
            params,
        )
        return [int(row["run_id"]) for row in cur.fetchall()]


def _fetch_orders(conn: Any, run_ids: list[int]) -> list[dict[str, Any]]:
    if not run_ids or not _table_exists(conn, "quant.quant_backtest_orders"):
        return []
    placeholders = ", ".join(["%s"] * len(run_ids))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                o.*,
                r.market_slug AS run_market_slug,
                r.token_side AS run_token_side,
                r.price_source AS run_price_source,
                r.backtest_engine AS run_backtest_engine
            FROM quant.quant_backtest_orders o
            JOIN quant.quant_backtest_runs r ON r.run_id = o.run_id
            WHERE o.run_id IN ({placeholders})
            ORDER BY o.run_id DESC, o.signal_index ASC, o.order_id ASC
            """,
            run_ids,
        )
        return [dict(row) for row in cur.fetchall()]


def _fetch_events(conn: Any, run_ids: list[int], args: argparse.Namespace) -> list[dict[str, Any]]:
    if not run_ids or not _table_exists(conn, "quant.real_order_state_events"):
        return []
    filters = [f"run_id IN ({', '.join(['%s'] * len(run_ids))})"]
    params: list[Any] = list(run_ids)
    if args.event_source:
        filters.append("source = %s")
        params.append(args.event_source)
    if args.since:
        filters.append("event_time >= %s")
        params.append(args.since)
    if args.until:
        filters.append("event_time < %s")
        params.append(args.until)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.real_order_state_events
            WHERE {" AND ".join(filters)}
            ORDER BY run_id DESC, COALESCE(event_time, created_at), event_id
            """,
            params,
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
