from pathlib import Path
from typing import Any

import quant.backtest.external_source_fixture_pipeline as pipeline
from quant.backtest.external_source_fixture_pipeline import (
    READY,
    FAIL,
    fill_first_external_source_fixture_pipeline_to_markdown,
    run_fill_first_external_source_fixture_pipeline,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


class FakeCursor:
    def __init__(self) -> None:
        self._one: dict[str, Any] | None = None
        self._many: list[dict[str, Any]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def execute(self, query: str, params: tuple[Any, ...] | list[Any] | None = None) -> None:
        if "pg_advisory_xact_lock" in query:
            self._one = None
            self._many = []
        elif "to_regclass('quant.external_source_import_state')" in query:
            self._one = {"exists": True}
            self._many = []
        elif "INSERT INTO quant.quant_backtest_runs" in query:
            self._one = {"run_id": 777}
            self._many = []
        elif query.strip().upper() == "ROLLBACK":
            self._one = None
            self._many = []
        elif "COUNT(*)" in query and "real_order_state_events" in query:
            self._one = {"count": 3}
            self._many = []
        elif "COUNT(*)" in query and "real_backtest_cost_events" in query:
            self._one = {"count": 2}
            self._many = []
        elif "COUNT(*)" in query and "platform_incidents" in query:
            self._one = {"count": 1}
            self._many = []
        elif "COUNT(*)" in query and "external_signal_events" in query:
            self._one = {"count": 2}
            self._many = []
        elif "FROM quant.external_source_import_state" in query:
            self._one = None
            self._many = [
                {
                    "state_key": "fixture-pipeline-order-state-events",
                    "source_type": "real_order_state_events",
                    "source": "fixture-pipeline-order-state",
                    "last_success_at": "2999-01-01T00:00:00Z",
                    "updated_at": "2999-01-01T00:00:00Z",
                    "last_rows_written": 3,
                },
                {
                    "state_key": "fixture-pipeline-real-cost-events",
                    "source_type": "real_cost_events",
                    "source": "fixture-pipeline-wallet",
                    "last_success_at": "2999-01-01T00:00:00Z",
                    "updated_at": "2999-01-01T00:00:00Z",
                    "last_rows_written": 2,
                },
                {
                    "state_key": "fixture-pipeline-platform-incidents",
                    "source_type": "platform_incidents",
                    "source": "fixture-pipeline-ops",
                    "last_success_at": "2999-01-01T00:00:00Z",
                    "updated_at": "2999-01-01T00:00:00Z",
                    "last_rows_written": 1,
                },
                {
                    "state_key": "fixture-pipeline-external-signals",
                    "source_type": "external_signal_events",
                    "source": "fixture-pipeline-signals",
                    "last_success_at": "2999-01-01T00:00:00Z",
                    "updated_at": "2999-01-01T00:00:00Z",
                    "last_rows_written": 2,
                },
            ]
        else:  # pragma: no cover - assertion guard
            raise AssertionError(f"unexpected query: {query}")

    def fetchone(self) -> dict[str, Any] | None:
        return self._one

    def fetchall(self) -> list[dict[str, Any]]:
        return self._many


class FakeConn:
    def cursor(self) -> FakeCursor:
        return FakeCursor()


def test_external_source_fixture_pipeline_dry_run_is_ready(tmp_path: Path) -> None:
    report = run_fill_first_external_source_fixture_pipeline(
        tmp_path,
        project_root=PROJECT_ROOT,
        overwrite=True,
        write=False,
        check_db=False,
    )

    assert report["status"] == READY
    assert report["imports"]["dry_run"] is True
    assert report["fixture"]["status"] == READY
    assert report["db"]["reason"] == "db_check_not_requested"


def test_external_source_fixture_pipeline_can_verify_db_counts(tmp_path: Path) -> None:
    calls: list[tuple[str, ...]] = []

    def runner(command: tuple[str, ...]) -> dict[str, Any]:
        calls.append(command)
        return {"returncode": 0, "output_tail": "ok"}

    report = run_fill_first_external_source_fixture_pipeline(
        tmp_path,
        project_root=PROJECT_ROOT,
        overwrite=True,
        write=True,
        check_db=True,
        conn=FakeConn(),
        command_runner=runner,
    )

    assert report["status"] == READY
    assert report["imports"]["dry_run"] is False
    assert len(calls) == 4
    assert report["db"]["status"] == READY
    assert report["db"]["table_counts"]["quant.real_order_state_events"] == 3
    assert report["db"]["table_counts"]["quant.external_signal_events"] == 2
    assert report["db"]["health"]["status"] == READY
    assert report["db"]["health"]["state_count"] == 4


def test_external_source_fixture_pipeline_fails_when_import_fails(tmp_path: Path) -> None:
    report = run_fill_first_external_source_fixture_pipeline(
        tmp_path,
        project_root=PROJECT_ROOT,
        overwrite=True,
        write=True,
        command_runner=lambda command: {"returncode": 2, "output_tail": "bad"},
    )

    assert report["status"] == FAIL
    assert report["imports"]["status"] == FAIL


def test_external_source_fixture_pipeline_markdown_contains_db_summary(tmp_path: Path) -> None:
    report = run_fill_first_external_source_fixture_pipeline(
        tmp_path,
        project_root=PROJECT_ROOT,
        overwrite=True,
        check_db=True,
        conn=FakeConn(),
    )
    markdown = fill_first_external_source_fixture_pipeline_to_markdown(report)

    assert "Fill-first External Source Fixture Pipeline" in markdown
    assert "DB Counts" in markdown
    assert "quant.real_order_state_events" in markdown


def test_external_source_fixture_pipeline_rollback_smoke_rolls_back(tmp_path: Path, monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(pipeline, "_acquire_schema_lock", lambda conn: calls.append("lock"))
    monkeypatch.setattr(pipeline, "create_schema", lambda conn: calls.append("schema"))
    monkeypatch.setattr(pipeline, "_missing_required_tables", lambda conn, include_calibration=False: [])
    monkeypatch.setattr(pipeline, "upsert_real_order_state_events", lambda conn, rows: len(list(rows)))
    monkeypatch.setattr(pipeline, "upsert_real_cost_events", lambda conn, rows: len(list(rows)))
    monkeypatch.setattr(pipeline, "upsert_platform_incidents", lambda conn, rows: len(list(rows)))
    monkeypatch.setattr(pipeline, "upsert_external_signal_events", lambda conn, rows: len(list(rows)))

    report = run_fill_first_external_source_fixture_pipeline(
        tmp_path,
        project_root=PROJECT_ROOT,
        overwrite=True,
        rollback_smoke=True,
        conn=FakeConn(),
    )

    assert report["status"] == READY
    assert report["rollback_smoke"] is True
    assert report["db"]["status"] == READY
    assert report["db"]["rolled_back"] is True
    assert report["db"]["smoke_run_id"] == 777
    assert calls == ["lock", "schema"]


def test_run_specific_fixture_pipeline_can_build_calibration_smoke(tmp_path: Path, monkeypatch) -> None:
    plan = {
        "status": "ready",
        "run": {
            "run_id": 321,
            "market_slug": "fixture-world-cup-winner",
            "token_side": "YES",
            "from_block": 88000000,
            "to_block": 88000010,
        },
        "orders": [
            {
                "run_id": 321,
                "order_id": "bt-321-001",
                "market_slug": "fixture-world-cup-winner",
                "token_id": "fixture-token-france",
                "token_side": "YES",
                "side": "BUY",
                "role": "TAKER",
                "simulated_status": "FILLED",
                "status": "FILLED",
                "signal_x": 88000001,
                "submit_x": 88000002,
                "requested_price": "0.20",
                "requested_size": "10",
                "filled_size": "10",
                "filled_notional": "2.05",
                "avg_fill_price": "0.205",
                "fee_cost": "0.01",
                "rebate_cost": "0",
                "slippage_cost": "0.05",
                "latency_seconds": "3",
                "event_template": {
                    "run_id": 321,
                    "order_id": "bt-321-001",
                    "market_slug": "fixture-world-cup-winner",
                    "token_id": "fixture-token-france",
                    "token_side": "YES",
                    "payload": {},
                },
            },
            {
                "run_id": 321,
                "order_id": "bt-321-002",
                "market_slug": "fixture-world-cup-winner",
                "token_id": "fixture-token-spain",
                "token_side": "YES",
                "side": "BUY",
                "role": "MAKER",
                "simulated_status": "NO_FILL",
                "status": "NO_FILL",
                "signal_x": 88000003,
                "submit_x": 88000004,
                "requested_price": "0.14",
                "requested_size": "8",
                "filled_size": "0",
                "filled_notional": "0",
                "no_fill_reason": "no_trade_through",
                "event_template": {
                    "run_id": 321,
                    "order_id": "bt-321-002",
                    "market_slug": "fixture-world-cup-winner",
                    "token_id": "fixture-token-spain",
                    "token_side": "YES",
                    "payload": {},
                },
            },
        ],
    }

    monkeypatch.setattr(pipeline, "create_schema", lambda conn: None)
    monkeypatch.setattr(pipeline, "_missing_required_tables", lambda conn, include_calibration=False: [])
    monkeypatch.setattr(pipeline, "upsert_real_order_state_events", lambda conn, rows: len(list(rows)))
    monkeypatch.setattr(pipeline, "upsert_real_cost_events", lambda conn, rows: len(list(rows)))
    monkeypatch.setattr(pipeline, "upsert_platform_incidents", lambda conn, rows: len(list(rows)))
    monkeypatch.setattr(pipeline, "upsert_external_signal_events", lambda conn, rows: len(list(rows)))
    monkeypatch.setattr(pipeline, "_fetch_orders_for_calibration", lambda conn, run_id: plan["orders"])
    monkeypatch.setattr(pipeline, "upsert_calibration_orders", lambda conn, rows: len(list(rows)))

    report = run_fill_first_external_source_fixture_pipeline(
        tmp_path,
        project_root=PROJECT_ROOT,
        overwrite=True,
        rollback_smoke=True,
        conn=FakeConn(),
        fixture_plan=plan,
        calibration_smoke=True,
    )

    assert report["status"] == READY
    assert report["run_specific"] is True
    assert report["db"]["smoke_run_id"] == 321
    assert report["db"]["table_counts"]["quant.real_order_state_events"] == 2
    assert report["db"]["table_counts"]["quant.real_backtest_cost_events"] == 1
    assert report["db"]["table_counts"]["quant.external_signal_events"] == 2
    assert report["db"]["calibration"]["status"] == READY
    assert report["db"]["calibration"]["samples_built"] == 2
    assert report["db"]["calibration"]["samples_written"] == 2
    assert report["run_coverage"]["status"] == READY
    assert report["run_coverage"]["run_specific"] is True
    assert report["run_coverage"]["external_order_candidate_count"] == 2
    assert report["run_coverage"]["order_state_coverage_pct"] == "100"
    assert report["run_coverage"]["calibration_coverage_pct"] == "100"


def test_external_source_fixture_pipeline_can_build_persisted_calibration_after_write(tmp_path: Path, monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []

    def runner(command: tuple[str, ...]) -> dict[str, Any]:
        calls.append(command)
        return {"returncode": 0, "output_tail": "ok"}

    monkeypatch.setattr(
        pipeline,
        "_persisted_calibration_report",
        lambda conn, run_id, source_prefix, enabled: {
            "status": READY,
            "reason": "calibration_written",
            "orders_read": 2,
            "events_read": 2,
            "samples_built": 2,
            "samples_written": 2,
            "cost_events_read": 1,
            "cost_samples_built": 1,
            "cost_samples_written": 1,
            "report": {"trust_status": "ready"},
            "cost_report": {"sample_count": 1},
        },
    )

    report = run_fill_first_external_source_fixture_pipeline(
        tmp_path,
        project_root=PROJECT_ROOT,
        overwrite=True,
        write=True,
        check_db=True,
        build_calibration=True,
        conn=FakeConn(),
        command_runner=runner,
    )

    assert report["status"] == READY
    assert len(calls) == 4
    assert report["build_calibration"] is True
    assert report["calibration"]["status"] == READY
    assert report["calibration"]["samples_written"] == 2
    assert report["calibration"]["cost_samples_written"] == 1


def test_external_source_fixture_pipeline_marks_non_run_specific_coverage_as_review(tmp_path: Path) -> None:
    report = run_fill_first_external_source_fixture_pipeline(
        tmp_path,
        project_root=PROJECT_ROOT,
        overwrite=True,
        write=False,
        check_db=False,
    )

    assert report["status"] == READY
    assert report["run_coverage"]["status"] == "review"
    assert report["run_coverage"]["run_specific"] is False
    assert report["run_coverage"]["reason"] == "not_run_specific_fixture"


def test_external_source_fixture_pipeline_markdown_contains_run_coverage(tmp_path: Path) -> None:
    plan = {
        "status": "ready",
        "run": {"run_id": 654, "market_slug": "fixture-market", "token_side": "YES"},
        "orders": [
            {
                "run_id": 654,
                "order_id": "bt-654-001",
                "market_slug": "fixture-market",
                "token_id": "fixture-token",
                "token_side": "YES",
                "side": "BUY",
                "role": "TAKER",
                "simulated_status": "FILLED",
                "status": "FILLED",
                "requested_price": "0.20",
                "requested_size": "10",
                "filled_size": "10",
                "filled_notional": "2",
                "event_template": {"run_id": 654, "order_id": "bt-654-001", "payload": {}},
            }
        ],
    }

    report = run_fill_first_external_source_fixture_pipeline(
        tmp_path,
        project_root=PROJECT_ROOT,
        overwrite=True,
        fixture_plan=plan,
    )
    markdown = fill_first_external_source_fixture_pipeline_to_markdown(report)

    assert report["run_coverage"]["status"] == READY
    assert "Run Coverage" in markdown
    assert "order_state_coverage_pct: 100" in markdown
