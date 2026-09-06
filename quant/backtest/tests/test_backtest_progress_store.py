from __future__ import annotations

import json

import pytest

from quant.api.read_api import get_backtest_run_progress
from quant.backtest.backtest_engine import (
    BacktestRunCancelled,
    _emit_run_progress,
    _upsert_backtest_run_progress,
    _compact_data_quality_for_run_meta,
    cancel_backtest_run,
    mark_run_failed,
    requeue_running_backtest_runs,
    update_backtest_run_progress,
)
from quant.workers.backtest_runner import REQUIRED_BACKTEST_TABLES, _assert_schema_ready


class RecordingCursor:
    def __init__(self, *, rows=None, row=None) -> None:
        self.rows = list(rows or [])
        self.row = row
        self.statements: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self):
        return self

    def __exit__(self, *args) -> bool:
        return False

    def execute(self, sql: str, params=()) -> None:
        self.statements.append((" ".join(sql.split()), tuple(params)))

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.row


class RecordingConnection:
    def __init__(self, cursor: RecordingCursor) -> None:
        self.cursor_obj = cursor

    def cursor(self) -> RecordingCursor:
        return self.cursor_obj


class SequenceCursor(RecordingCursor):
    def __init__(self, rows: list[object]) -> None:
        super().__init__()
        self.fetchone_rows = list(rows)

    def fetchone(self):
        return self.fetchone_rows.pop(0) if self.fetchone_rows else None


def test_progress_updates_never_rewrite_run_meta() -> None:
    cursor = RecordingCursor()
    conn = RecordingConnection(cursor)

    update_backtest_run_progress(
        conn,
        17,
        phase="simulating strategy and fills",
        progress=61,
        current_x=88_700_009,
        x_axis="block_number",
        rows_processed=500,
        total_rows=1000,
        eta_seconds=2.4,
    )

    sql = " ".join(statement for statement, _ in cursor.statements).lower()
    assert "quant.quant_backtest_run_progress" in sql
    assert "jsonb_set" not in sql
    assert " meta " not in f" {sql} "
    assert "%s::bigint is null" in sql


def test_progress_can_persist_bounded_artifact_summary() -> None:
    cursor = RecordingCursor()
    conn = RecordingConnection(cursor)

    _upsert_backtest_run_progress(
        conn,
        17,
        status="succeeded",
        phase="backtest artifacts ready",
        progress=100,
        artifact_summary={"run_id": 17, "summary_status": "ready"},
    )

    sql, params = cursor.statements[0]
    assert "artifact_summary" in sql.lower()
    assert '{"run_id": 17, "summary_status": "ready"}' in params


def test_cancelled_progress_is_terminal_against_late_worker_updates() -> None:
    cursor = RecordingCursor()
    conn = RecordingConnection(cursor)

    _upsert_backtest_run_progress(
        conn,
        17,
        status="running",
        phase="late worker update",
        progress=92,
        rows_processed=920,
    )

    sql = cursor.statements[0][0].lower()
    assert "when quant.quant_backtest_run_progress.status = 'cancelled' then 'cancelled'" in sql
    assert "when quant.quant_backtest_run_progress.status = 'cancelled' then quant.quant_backtest_run_progress.phase" in sql
    assert "when quant.quant_backtest_run_progress.status = 'cancelled' then quant.quant_backtest_run_progress.finished_at" in sql


def test_progress_callback_propagates_cooperative_cancellation() -> None:
    def cancel(_payload) -> None:
        raise BacktestRunCancelled("cancelled by test")

    with pytest.raises(BacktestRunCancelled, match="cancelled by test"):
        _emit_run_progress(cancel, phase="simulating", progress=50)


def test_cancel_backtest_run_preserves_progress_and_writes_terminal_state() -> None:
    cursor = SequenceCursor([
        {"run_id": 17, "status": "cancelled", "rows_processed": 500},
        {
            "progress": 61,
            "current_x": 88_700_009,
            "x_axis": "block_number",
            "rows_processed": 500,
            "total_rows": 1000,
        },
    ])
    conn = RecordingConnection(cursor)

    item = cancel_backtest_run(conn, 17, "cancelled from test")

    assert item is not None
    assert item["status"] == "cancelled"
    update_sql = cursor.statements[0][0].lower()
    assert "status in ('queued', 'running')" in update_sql
    progress_sql, progress_params = cursor.statements[-1]
    assert "quant.quant_backtest_run_progress" in progress_sql.lower()
    assert "cancelled" in progress_params
    assert "cancelled from test" in progress_params
    assert 88_700_009 in progress_params


def test_mark_run_failed_does_not_overwrite_cancelled_run() -> None:
    cursor = RecordingCursor()

    mark_run_failed(RecordingConnection(cursor), 17, "late worker failure")

    assert "status <> 'cancelled'" in cursor.statements[0][0].lower()
    assert "when quant.quant_backtest_run_progress.status = 'cancelled'" in cursor.statements[1][0].lower()


def test_run_meta_data_quality_is_bounded_without_stringifying_deep_arrays() -> None:
    report = {
        "status": "ready",
        "fill_quality": {
            **{f"metric_{index}": index for index in range(100)},
            "no_fill_reasons": {},
            "environment_flags": [],
            "order_anomaly_flags": [],
            "avg_markout_after_1_bars": "0",
            "raw_trade_tick_report": {
                "blocks": [
                    {"block": index, "order_sequence": [{"trade": trade} for trade in range(200)]}
                    for index in range(1_000)
                ]
            }
        },
    }

    compact = _compact_data_quality_for_run_meta(report)
    encoded = json.dumps(compact)

    assert compact["status"] == "ready"
    assert compact["fill_quality"]["avg_markout_after_1_bars"] == "0"
    assert compact["fill_quality"]["metric_99"] == 99
    assert len(compact["fill_quality"]["raw_trade_tick_report"]["blocks"]) == 64
    assert len(compact["fill_quality"]["raw_trade_tick_report"]["blocks"][0]["order_sequence"]) == 64
    assert len(encoded) < 200_000


def test_progress_reader_uses_small_progress_table() -> None:
    cursor = RecordingCursor(
        row={
            "run_id": 17,
            "status": "running",
            "rows_processed": 500,
            "error": None,
            "created_at": None,
            "started_at": None,
            "finished_at": None,
            "phase": "simulating strategy and fills",
            "progress": 61,
            "current_x": 88_700_009,
            "x_axis": "block_number",
            "total_rows": 1000,
            "eta_seconds": 2.4,
            "updated_at": "now",
        }
    )
    conn = RecordingConnection(cursor)

    item = get_backtest_run_progress(conn, run_id=17)

    assert item is not None
    assert item["progress"] == 61
    assert item["rows_processed"] == 500
    assert item["updated_at"] == "now"
    sql = cursor.statements[0][0].lower()
    assert "quant.quant_backtest_run_progress" in sql
    assert "quant.quant_backtest_runs" not in sql
    assert "meta" not in sql


def test_dev_restart_recovery_resets_interrupted_progress() -> None:
    cursor = RecordingCursor(rows=[{"run_id": 17}])
    conn = RecordingConnection(cursor)

    assert requeue_running_backtest_runs(conn, worker_id="dev-stack:test") == [17]

    assert "r.status = 'running'" in cursor.statements[0][0].lower()
    assert "p.worker_id = %s" in cursor.statements[0][0].lower()
    assert cursor.statements[0][1] == ("dev-stack:test",)
    recovery_sql = cursor.statements[1][0].lower()
    assert "on conflict (run_id) do update" in recovery_sql
    assert "rows_processed = 0" in recovery_sql
    assert "current_x = null" in recovery_sql


def test_schema_probe_handles_dict_rows_from_postgres_connection() -> None:
    cursor = RecordingCursor(rows=[{"name": name, "to_regclass": name} for name in REQUIRED_BACKTEST_TABLES])

    _assert_schema_ready(RecordingConnection(cursor))

    assert "quant.quant_backtest_run_progress" in REQUIRED_BACKTEST_TABLES
