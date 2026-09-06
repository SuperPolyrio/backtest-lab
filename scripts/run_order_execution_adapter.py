#!/usr/bin/env python3
"""Dry-run or execute guarded strategy intents through an external order adapter."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.guarded_executor import build_guarded_executor_report  # noqa: E402
from quant.backtest.order_execution_calibration import (  # noqa: E402
    build_order_execution_calibration_report,
    order_execution_calibration_report_to_summary,
    record_order_execution_calibration_samples,
)
from quant.backtest.order_execution_adapter import (  # noqa: E402
    build_order_execution_adapter_report,
    order_execution_adapter_report_to_markdown,
    record_order_execution_adapter_events,
)
from quant.backtest.order_execution_safety import (  # noqa: E402
    READY,
    build_order_execution_run_safety_report,
    order_execution_run_safety_to_markdown,
)
from quant.backtest.strategy_activation import load_strategy_enable_state  # noqa: E402
from quant.backtest.strategy_runner_guard import build_strategy_runner_plan  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-mode", choices=("paper", "live"), default=os.environ.get("ORDER_EXECUTION_TARGET_MODE", "paper"))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("ORDER_EXECUTION_LIMIT", "50")))
    parser.add_argument("--include-blocked", action="store_true", help="Include non-runnable enabled-state rows in diagnostics.")
    parser.add_argument("--submit-url", default=os.environ.get("ORDER_EXECUTION_SUBMIT_URL", ""))
    parser.add_argument("--cancel-url", default=os.environ.get("ORDER_EXECUTION_CANCEL_URL", ""))
    parser.add_argument("--header", action="append", default=_env_headers(), help="HTTP header as Key=Value; repeatable.")
    parser.add_argument("--source", default=os.environ.get("ORDER_EXECUTION_SOURCE", "external-order-adapter"))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("ORDER_EXECUTION_TIMEOUT", "15")))
    parser.add_argument("--execute", action="store_true", help="Call the external submit URL. Default is dry-run.")
    parser.add_argument("--record-events", action="store_true", help="Write adapter response events to quant.real_order_state_events.")
    parser.add_argument("--build-calibration", action="store_true", help="Build fill calibration samples from adapter response events.")
    parser.add_argument("--calibration-source", default=os.environ.get("ORDER_EXECUTION_CALIBRATION_SOURCE", "external-order-adapter-calibration"))
    parser.add_argument("--include-open-events", action="store_true", help="Also build calibration rows from accepted/open response events.")
    parser.add_argument("--live-confirm", default=os.environ.get("ORDER_EXECUTION_LIVE_CONFIRM", ""), help="Required confirmation token for live --execute.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero unless adapter status is ready/submitted.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = _kv_pairs(args.header)
    dry_run = not bool(args.execute)
    safety_report = build_order_execution_run_safety_report(
        target_mode=args.target_mode,
        execute=bool(args.execute),
        record_events=bool(args.record_events),
        submit_url=args.submit_url.strip() or None,
        headers=headers,
        live_confirm=args.live_confirm.strip() or None,
    )
    if args.execute and safety_report["status"] != READY:
        if args.format == "json":
            print(json.dumps({"status": "blocked", "safety": safety_report}, ensure_ascii=False, indent=2, default=str))
        else:
            print(order_execution_run_safety_to_markdown(safety_report))
        return 2
    with postgres_connection(readonly=not args.record_events) as conn:
        if args.record_events:
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
        guarded_report = build_guarded_executor_report(runner_plan)
        report = build_order_execution_adapter_report(
            guarded_report,
            submit_url=args.submit_url.strip() or None,
            cancel_url=args.cancel_url.strip() or None,
            headers=headers,
            source=args.source,
            dry_run=dry_run,
            timeout=args.timeout,
        )
        report["safety"] = safety_report
        if args.record_events:
            report["recorded_event_count"] = record_order_execution_adapter_events(conn, report)
        if args.build_calibration:
            orders = _fetch_orders_for_events(conn, report.get("response_events") or [])
            calibration = build_order_execution_calibration_report(
                orders=orders,
                response_events=report.get("response_events") or [],
                source=args.calibration_source,
                include_open_events=bool(args.include_open_events),
            )
            if args.record_events:
                calibration["samples_written"] = record_order_execution_calibration_samples(conn, calibration)
            report["calibration"] = order_execution_calibration_report_to_summary(calibration)
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(order_execution_adapter_report_to_markdown(report))
    if args.strict and report.get("status") != "ready":
        return 1
    return 0


def _env_headers() -> list[str]:
    values = []
    for name in ("ORDER_EXECUTION_AUTH_HEADER", "ORDER_EXECUTION_HEADER"):
        raw = os.environ.get(name)
        if raw:
            values.append(raw)
    return values


def _kv_pairs(items: list[str]) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"expected key=value, got {item!r}")
        key, value = item.split("=", 1)
        pairs[key.strip()] = value.strip()
    return {key: value for key, value in pairs.items() if key}


def _fetch_orders_for_events(conn: Any, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    run_ids = sorted({int(event["run_id"]) for event in events if event.get("run_id") not in (None, "")})
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


def _table_exists(conn: Any, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (table_name,))
        row = cur.fetchone()
    if isinstance(row, dict):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


if __name__ == "__main__":
    raise SystemExit(main())
