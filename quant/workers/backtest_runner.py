"""Backtest job runner for queued quant runs."""

from __future__ import annotations

import argparse
import time
from collections.abc import Mapping
from typing import Any

from ..backtest.backtest_engine import (
    BacktestRunCancelled,
    claim_backtest_run,
    is_backtest_run_cancelled,
    execute_backtest_run,
    get_backtest_run_for_update_free,
    list_queued_backtest_run_ids,
    mark_run_failed,
    mark_run_cancelled,
    requeue_running_backtest_runs,
    update_backtest_run_progress,
)
from ..core.db import PostgresSettings, postgres_connection


REQUIRED_BACKTEST_TABLES = (
    "quant.quant_backtest_runs",
    "quant.quant_backtest_run_progress",
    "quant.quant_backtest_parameters",
    "quant.quant_backtest_metrics",
    "quant.quant_backtest_equity",
    "quant.quant_backtest_orders",
    "quant.quant_backtest_ledger",
    "quant.quant_backtest_trades",
    "quant.quant_backtest_events",
)


def _assert_schema_ready(conn: Any) -> None:
    """Fail fast without letting a runtime worker execute global DDL."""

    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, to_regclass(name) FROM unnest(%s::text[]) AS required(name)",
            (list(REQUIRED_BACKTEST_TABLES),),
        )
        rows = cur.fetchall()
        missing = []
        for row in rows:
            name, relation = (row["name"], row["to_regclass"]) if isinstance(row, Mapping) else row
            if relation is None:
                missing.append(name)
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(
            f"Backtest schema is not initialized ({names}). "
            "Start once with POLYDATA_SKIP_INIT_SCHEMA=0 before running the worker."
        )


def _progress_writer(settings: PostgresSettings, run_id: int, worker_id: str | None):
    started_at = time.monotonic()

    def write(update: dict[str, Any]) -> None:
        progress = max(0, min(100, int(update.get("progress") or 0)))
        elapsed = max(0.0, time.monotonic() - started_at)
        eta_seconds = None
        if 0 < progress < 100:
            eta_seconds = elapsed * (100 - progress) / progress
        with postgres_connection(settings, readonly=False) as progress_conn:
            if is_backtest_run_cancelled(progress_conn, run_id):
                raise BacktestRunCancelled(f"Backtest run #{run_id} cancelled by user")
            update_backtest_run_progress(
                progress_conn,
                run_id,
                phase=str(update.get("phase") or "running backtest"),
                progress=progress,
                status=str(update.get("status") or "running"),
                current_x=update.get("current_x"),
                x_axis=update.get("x_axis"),
                rows_processed=update.get("rows_processed"),
                total_rows=update.get("total_rows"),
                eta_seconds=eta_seconds,
                worker_id=worker_id,
            )
            progress_conn.commit()

    return write


def run_backtest_job(
    run_id: int,
    *,
    settings: PostgresSettings | None = None,
    ensure_schema: bool = True,
    worker_id: str | None = None,
) -> dict[str, Any] | None:
    """Claim and execute one queued run.

    Returns the final run row. If another worker already claimed or finished the
    run, the current row is returned without duplicate execution.
    """

    resolved_settings = settings or PostgresSettings()
    with postgres_connection(resolved_settings, readonly=False) as conn:
        if ensure_schema:
            _assert_schema_ready(conn)
        claimed = claim_backtest_run(conn, run_id, worker_id=worker_id)
        conn.commit()
        if not claimed:
            return get_backtest_run_for_update_free(conn, run_id)
        try:
            execute_backtest_run(conn, run_id, progress_callback=_progress_writer(resolved_settings, run_id, worker_id))
            conn.commit()
        except BacktestRunCancelled as exc:
            conn.rollback()
            mark_run_cancelled(conn, run_id, str(exc))
            conn.commit()
        except Exception as exc:
            conn.rollback()
            mark_run_failed(conn, run_id, str(exc))
            conn.commit()
        return get_backtest_run_for_update_free(conn, run_id)


def run_queued_backtests_once(
    *,
    settings: PostgresSettings | None = None,
    limit: int = 5,
    ensure_schema: bool = True,
    worker_id: str | None = None,
) -> int:
    resolved_settings = settings or PostgresSettings()
    with postgres_connection(resolved_settings, readonly=False) as conn:
        if ensure_schema:
            _assert_schema_ready(conn)
        run_ids = list_queued_backtest_run_ids(conn, limit=limit)
    completed = 0
    for run_id in run_ids:
        row = run_backtest_job(run_id, settings=resolved_settings, ensure_schema=False, worker_id=worker_id)
        if row and row.get("status") in {"succeeded", "failed", "cancelled"}:
            completed += 1
    return completed


def run_daemon(
    *,
    settings: PostgresSettings | None = None,
    limit: int = 5,
    sleep_seconds: float = 2.0,
    recover_running_on_startup: bool = False,
    worker_id: str | None = None,
) -> None:
    resolved_settings = settings or PostgresSettings()
    with postgres_connection(resolved_settings, readonly=False) as conn:
        _assert_schema_ready(conn)
        if recover_running_on_startup and not worker_id:
            raise ValueError("worker_id is required for safe running-job recovery")
        recovered = requeue_running_backtest_runs(conn, worker_id=worker_id) if recover_running_on_startup else []
        conn.commit()
    if recovered:
        print({"recoveredRunIds": recovered}, flush=True)
    while True:
        completed = run_queued_backtests_once(
            settings=resolved_settings,
            limit=limit,
            ensure_schema=False,
            worker_id=worker_id,
        )
        if completed == 0:
            time.sleep(sleep_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run queued quant backtests.")
    parser.add_argument("--run-id", type=int, help="Execute one queued backtest run id.")
    parser.add_argument("--once", action="store_true", help="Process currently queued runs once and exit.")
    parser.add_argument("--daemon", action="store_true", help="Continuously poll queued runs.")
    parser.add_argument("--limit", type=int, default=5, help="Max queued runs to pick per pass.")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Daemon idle sleep interval.")
    parser.add_argument(
        "--recover-running-on-startup",
        action="store_true",
        help="Requeue running jobs after a full dev-stack restart.",
    )
    parser.add_argument("--worker-id", help="Stable worker owner id used for safe restart recovery.")
    args = parser.parse_args()

    if args.run_id:
        row = run_backtest_job(args.run_id, worker_id=args.worker_id)
        print(row)
        return
    if args.daemon:
        run_daemon(
            limit=args.limit,
            sleep_seconds=args.sleep_seconds,
            recover_running_on_startup=args.recover_running_on_startup,
            worker_id=args.worker_id,
        )
        return
    completed = run_queued_backtests_once(limit=args.limit, worker_id=args.worker_id)
    print({"completed": completed})


if __name__ == "__main__":
    main()
