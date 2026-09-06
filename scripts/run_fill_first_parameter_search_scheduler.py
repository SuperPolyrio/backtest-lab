#!/usr/bin/env python3
"""Create, run, and inspect persistent fill-first parameter search batches."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.parameter_search_runner import load_parameter_search_plan_or_build  # noqa: E402
from quant.backtest.parameter_search_scheduler import (  # noqa: E402
    cancel_parameter_search_batch,
    create_parameter_search_batch,
    get_parameter_search_batch,
    get_parameter_search_batch_items,
    list_parameter_search_batches,
    parameter_search_progress_to_markdown,
    refresh_parameter_search_batch_status,
    requeue_parameter_search_items,
    run_parameter_search_batch_worker,
)
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--create-batch", action="store_true", help="Persist a plan as queued parameter search items.")
    action.add_argument("--run-worker", action="store_true", help="Claim and execute queued/retryable items.")
    action.add_argument("--progress", action="store_true", help="Refresh and print batch progress.")
    action.add_argument("--list", action="store_true", help="List recent parameter search batches.")
    action.add_argument("--cancel-batch", action="store_true", help="Cancel queued/running/retryable work for a batch.")
    action.add_argument("--requeue-items", action="store_true", help="Requeue failed/canceled/running items for another worker pass.")
    action.add_argument("--worker-loop", action="store_true", help="Run a persistent worker loop for systemd or a terminal session.")
    parser.add_argument("--batch-id", type=int)
    parser.add_argument("--plan-json", type=Path)
    parser.add_argument("--base-payload-json", type=Path)
    parser.add_argument("--grid-json", type=Path)
    parser.add_argument("--universe-name", default="fill_first_parameter_search")
    parser.add_argument("--strategy-name", default="unknown")
    parser.add_argument("--strategy-version", default="unknown")
    parser.add_argument("--source", default="parameter-search-scheduler")
    parser.add_argument("--created-by")
    parser.add_argument("--max-runs", type=int, default=250)
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument("--max-items", type=int, default=1)
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    parser.add_argument("--max-polls", type=int, default=1, help="Worker-loop polls. 0 means run forever.")
    parser.add_argument("--idle-exit", action="store_true", help="Exit worker loop after the first idle poll.")
    parser.add_argument("--stream-json", action="store_true", help="Emit one JSON log line per worker-loop poll.")
    parser.add_argument("--worker-id")
    parser.add_argument("--reason")
    parser.add_argument("--canceled-by")
    parser.add_argument("--requeue-status", action="append", default=[], help="Item status to requeue. Repeatable; defaults to failed.")
    parser.add_argument("--reset-attempts", action="store_true")
    parser.add_argument("--clear-results", action="store_true")
    parser.add_argument("--no-retry-failed", action="store_true")
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--status", default=None, help="Status filter for --list.")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=False) as conn:
        create_schema(conn)
        if args.create_batch:
            plan = load_parameter_search_plan_or_build(
                plan_json=args.plan_json,
                base_payload_json=args.base_payload_json,
                grid_json=args.grid_json,
                max_runs=args.max_runs,
                universe_name=args.universe_name,
            )
            batch_id = create_parameter_search_batch(
                conn,
                plan,
                source=args.source,
                strategy_name=args.strategy_name,
                strategy_version=args.strategy_version,
                universe_name=args.universe_name,
                max_attempts=args.max_attempts,
                created_by=args.created_by,
            )
            conn.commit()
            report = refresh_parameter_search_batch_status(conn, batch_id=batch_id)
            payload: dict[str, Any] = {"batch_id": batch_id, "progress": report}
        elif args.run_worker:
            report = run_parameter_search_batch_worker(
                conn,
                batch_id=args.batch_id,
                max_items=args.max_items,
                worker_id=args.worker_id,
                retry_failed=not args.no_retry_failed,
                stop_on_error=args.stop_on_error,
            )
            conn.commit()
            payload = report
        elif args.progress:
            if args.batch_id is None:
                raise SystemExit("--batch-id is required with --progress")
            report = refresh_parameter_search_batch_status(conn, batch_id=args.batch_id)
            conn.commit()
            payload = {"batch_id": args.batch_id, "progress": report}
        elif args.cancel_batch:
            if args.batch_id is None:
                raise SystemExit("--batch-id is required with --cancel-batch")
            payload = cancel_parameter_search_batch(
                conn,
                batch_id=args.batch_id,
                reason=args.reason,
                canceled_by=args.canceled_by,
            )
            conn.commit()
        elif args.requeue_items:
            if args.batch_id is None:
                raise SystemExit("--batch-id is required with --requeue-items")
            payload = requeue_parameter_search_items(
                conn,
                batch_id=args.batch_id,
                statuses=args.requeue_status or ["failed"],
                reset_attempts=args.reset_attempts,
                clear_results=args.clear_results,
            )
            conn.commit()
        elif args.worker_loop:
            payload = _run_worker_loop(args, conn)
        else:
            rows = list_parameter_search_batches(conn, status=args.status, limit=args.limit)
            payload = {"items": rows, "count": len(rows)}
    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
    else:
        print(_to_markdown(payload))
    return 0


def _run_worker_loop(args: argparse.Namespace, conn: Any) -> dict[str, Any]:
    max_polls = int(args.max_polls)
    poll_index = 0
    reports: list[dict[str, Any]] = []
    processed_total = 0
    error_total = 0
    while max_polls <= 0 or poll_index < max_polls:
        poll_index += 1
        report = run_parameter_search_batch_worker(
            conn,
            batch_id=args.batch_id,
            max_items=args.max_items,
            worker_id=args.worker_id,
            retry_failed=not args.no_retry_failed,
            stop_on_error=args.stop_on_error,
        )
        conn.commit()
        reports.append(report)
        processed_total += int(report.get("processed_count") or 0)
        error_total += int(report.get("error_count") or 0)
        if args.stream_json:
            print(json.dumps({"event": "parameter_search_worker_poll", "poll": poll_index, **report}, ensure_ascii=False, default=_json_default), flush=True)
        if int(report.get("processed_count") or 0) == 0 and args.idle_exit:
            break
        if max_polls > 0 and poll_index >= max_polls:
            break
        if float(args.poll_seconds) > 0:
            time.sleep(float(args.poll_seconds))
    return {
        "event": "parameter_search_worker_loop_complete",
        "poll_count": poll_index,
        "processed_count": processed_total,
        "error_count": error_total,
        "reports": reports[-10:],
    }


def _to_markdown(payload: dict[str, Any]) -> str:
    progress = payload.get("progress")
    if isinstance(progress, dict):
        return parameter_search_progress_to_markdown(progress)
    items = payload.get("items")
    if isinstance(items, list):
        lines = ["# Fill-first Parameter Search Batches", "", "| Batch | Status | Universe | Planned | Succeeded | Failed | Updated |", "| --- | --- | --- | ---: | ---: | ---: | --- |"]
        for item in items:
            if not isinstance(item, dict):
                continue
            lines.append(
                "| {batch} | {status} | {universe} | {planned} | {succeeded} | {failed} | {updated} |".format(
                    batch=item.get("batch_id") or "",
                    status=item.get("status") or "",
                    universe=item.get("universe_name") or "",
                    planned=item.get("planned_run_count") or 0,
                    succeeded=item.get("succeeded_count") or 0,
                    failed=item.get("failed_count") or 0,
                    updated=item.get("updated_at") or "",
                )
            )
        return "\n".join(lines)
    return json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default)


def _json_default(value: Any) -> str:
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
