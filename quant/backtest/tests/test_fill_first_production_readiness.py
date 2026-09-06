import quant.backtest.fill_first_production_readiness as production_readiness
from quant.backtest.fill_first_production_readiness import (
    build_fill_first_production_readiness_report,
    fill_first_production_readiness_to_markdown,
)
from quant.backtest.order_execution_safety import LIVE_CONFIRM_TOKEN


def ready_env() -> dict[str, str]:
    return {
        "ORDER_EXECUTION_TARGET_MODE": "paper",
        "ORDER_EXECUTION_SUBMIT_URL": "https://orders.internal.company/submit",
        "ORDER_EXECUTION_AUTH_HEADER": "Authorization=test",
        "ORDER_STATE_API_URL": "https://orders.internal.company/state",
        "ORDER_STATE_AUTH_HEADER": "Authorization=test",
        "ORDER_STATE_KEY": "order-state-live",
        "COST_EVENTS_URL": "https://costs.internal.company/events",
        "COST_EVENTS_AUTH_HEADER": "Authorization=test",
        "COST_EVENTS_STATE_KEY": "cost-events-live",
        "PLATFORM_INCIDENTS_URL": "https://incidents.internal.company/events",
        "PLATFORM_INCIDENTS_AUTH_HEADER": "Authorization=test",
        "PLATFORM_INCIDENTS_STATE_KEY": "incidents-live",
        "EXTERNAL_SIGNAL_URL": "https://signals.internal.company/events",
        "EXTERNAL_SIGNAL_AUTH_HEADER": "Authorization=test",
        "EXTERNAL_SIGNAL_STATE_KEY": "signals-live",
    }


def patch_ready_artifact_gate(monkeypatch, *, paper_allowed: bool = True, live_allowed: bool = False, status: str = "ready") -> None:
    monkeypatch.setattr(
        production_readiness,
        "load_backtest_run_artifact_inputs",
        lambda conn, run_id: {"run": {"run_id": run_id}},
    )
    monkeypatch.setattr(
        production_readiness,
        "build_backtest_run_artifact_report",
        lambda inputs, run_id=None: {
            "status": "ready",
            "paper_live_evidence_gate_report": {
                "status": status,
                "paper_allowed": paper_allowed,
                "live_allowed": live_allowed,
                "run_evidence_ready": status == "ready",
                "missing_evidence_ready": status == "ready",
                "blocked_reasons": [] if status == "ready" else ["fixture gate blocked"],
                "review_reasons": [],
            },
        },
    )


def test_production_readiness_blocks_unconfigured_paper() -> None:
    report = build_fill_first_production_readiness_report(env={})

    assert report["status"] == "blocked"
    assert report["launch_allowed"] is False
    assert any("ORDER_EXECUTION_SUBMIT_URL" in reason for reason in report["blocked_reasons"])


def test_production_readiness_allows_ready_paper_even_if_order_state_collection_is_review() -> None:
    env = ready_env()
    env.pop("ORDER_STATE_API_URL")
    env.pop("ORDER_STATE_AUTH_HEADER")
    env.pop("ORDER_STATE_KEY")
    report = build_fill_first_production_readiness_report(env=env, target_mode="paper")

    assert report["status"] == "review"
    assert report["launch_allowed"] is False
    assert report["blocked_reasons"] == []
    assert any("real_order_state_events" in reason for reason in report["review_reasons"])


def test_production_readiness_keeps_external_signal_optional_for_paper() -> None:
    env = ready_env()
    env.pop("EXTERNAL_SIGNAL_URL")
    env.pop("EXTERNAL_SIGNAL_AUTH_HEADER")
    env.pop("EXTERNAL_SIGNAL_STATE_KEY")
    report = build_fill_first_production_readiness_report(env=env, target_mode="paper")

    assert report["status"] == "review"
    assert report["launch_allowed"] is False
    assert report["blocked_reasons"] == []
    assert any("external_signal_events" in reason for reason in report["review_reasons"])
    assert any(check["name"] == "external_signal_events" for check in report["checks"])


def test_production_readiness_ready_when_paper_sources_are_configured() -> None:
    report = build_fill_first_production_readiness_report(env=ready_env(), target_mode="paper")

    assert report["status"] == "ready"
    assert report["launch_allowed"] is True
    assert report["blocked_reasons"] == []
    assert report["review_reasons"] == []
    assert report["external_source_import_health"]["checked"] is False


def test_production_readiness_check_db_blocks_without_connection() -> None:
    report = build_fill_first_production_readiness_report(
        env=ready_env(),
        target_mode="paper",
        check_db=True,
        conn=None,
    )

    assert report["status"] == "blocked"
    assert report["launch_allowed"] is False
    assert any("database connection required" in reason for reason in report["blocked_reasons"])


def test_production_readiness_check_db_allows_healthy_external_imports(monkeypatch) -> None:
    state_keys = ["order-state-live", "cost-events-live", "incidents-live", "signals-live"]
    monkeypatch.setattr(
        production_readiness,
        "load_external_source_import_states",
        lambda conn, limit=100: [{"state_key": key} for key in state_keys],
    )
    monkeypatch.setattr(
        production_readiness,
        "evaluate_external_source_import_health",
        lambda states, max_stale_seconds=86400: {
            "status": "ready",
            "reason": "all_ready",
            "state_count": len(states),
            "items": [
                {"state_key": state["state_key"], "status": "ready", "reason": "ready", "last_rows_written": 1}
                for state in states
            ],
        },
    )

    report = build_fill_first_production_readiness_report(
        env=ready_env(),
        target_mode="paper",
        check_db=True,
        conn=object(),
    )

    assert report["status"] == "ready"
    assert report["launch_allowed"] is True
    assert report["external_source_import_health"]["checked"] is True
    assert report["external_source_import_health"]["missing_required_count"] == 0
    assert any(check["name"] == "external source import health" for check in report["checks"])


def test_production_readiness_check_db_blocks_stale_external_imports(monkeypatch) -> None:
    state_keys = ["order-state-live", "cost-events-live", "incidents-live", "signals-live"]
    monkeypatch.setattr(
        production_readiness,
        "load_external_source_import_states",
        lambda conn, limit=100: [{"state_key": key} for key in state_keys],
    )
    monkeypatch.setattr(
        production_readiness,
        "evaluate_external_source_import_health",
        lambda states, max_stale_seconds=86400: {
            "status": "review",
            "reason": "stale_success",
            "state_count": len(states),
            "items": [
                {"state_key": state["state_key"], "status": "review", "reason": "stale_success", "last_rows_written": 1}
                for state in states
            ],
        },
    )

    report = build_fill_first_production_readiness_report(
        env=ready_env(),
        target_mode="paper",
        check_db=True,
        conn=object(),
    )

    assert report["status"] == "blocked"
    assert report["launch_allowed"] is False
    assert any("stale_success" in reason for reason in report["blocked_reasons"])


def test_production_readiness_check_db_blocks_missing_expected_state_key(monkeypatch) -> None:
    monkeypatch.setattr(
        production_readiness,
        "load_external_source_import_states",
        lambda conn, limit=100: [{"state_key": "cost-events-live"}],
    )
    monkeypatch.setattr(
        production_readiness,
        "evaluate_external_source_import_health",
        lambda states, max_stale_seconds=86400: {
            "status": "ready",
            "reason": "all_ready",
            "state_count": len(states),
            "items": [
                {"state_key": state["state_key"], "status": "ready", "reason": "ready", "last_rows_written": 1}
                for state in states
            ],
        },
    )

    report = build_fill_first_production_readiness_report(
        env=ready_env(),
        target_mode="paper",
        check_db=True,
        conn=object(),
    )

    assert report["status"] == "blocked"
    assert report["external_source_import_health"]["missing_required_count"] == 3
    assert any("required_missing=3" in reason for reason in report["blocked_reasons"])


def test_production_readiness_blocks_run_with_incomplete_external_coverage(monkeypatch) -> None:
    state_keys = ["order-state-live", "cost-events-live", "incidents-live", "signals-live"]
    monkeypatch.setattr(
        production_readiness,
        "load_external_source_import_states",
        lambda conn, limit=100: [{"state_key": key} for key in state_keys],
    )
    monkeypatch.setattr(
        production_readiness,
        "evaluate_external_source_import_health",
        lambda states, max_stale_seconds=86400: {
            "status": "ready",
            "reason": "all_ready",
            "state_count": len(states),
            "items": [
                {"state_key": state["state_key"], "status": "ready", "reason": "ready", "last_rows_written": 1}
                for state in states
            ],
        },
    )
    monkeypatch.setattr(production_readiness, "load_external_source_run_coverage_inputs", lambda conn, run_id: {"run": {"run_id": run_id}})
    patch_ready_artifact_gate(monkeypatch)
    monkeypatch.setattr(
        production_readiness,
        "build_external_source_run_coverage_report",
        lambda inputs, run_id=None: {
            "status": "review",
            "run_id": run_id,
            "reason": "missing calibration",
            "order_state_coverage_pct": "100",
            "calibration_coverage_pct": "0",
        },
    )

    report = build_fill_first_production_readiness_report(
        env=ready_env(),
        target_mode="paper",
        check_db=True,
        conn=object(),
        run_id=77,
    )

    assert report["status"] == "blocked"
    assert report["launch_allowed"] is False
    assert report["external_source_run_coverage"]["checked"] is True
    assert any("run_id=77" in reason and "calibration=0%" in reason for reason in report["blocked_reasons"])


def test_production_readiness_allows_run_with_complete_external_coverage(monkeypatch) -> None:
    state_keys = ["order-state-live", "cost-events-live", "incidents-live", "signals-live"]
    monkeypatch.setattr(
        production_readiness,
        "load_external_source_import_states",
        lambda conn, limit=100: [{"state_key": key} for key in state_keys],
    )
    monkeypatch.setattr(
        production_readiness,
        "evaluate_external_source_import_health",
        lambda states, max_stale_seconds=86400: {
            "status": "ready",
            "reason": "all_ready",
            "state_count": len(states),
            "items": [
                {"state_key": state["state_key"], "status": "ready", "reason": "ready", "last_rows_written": 1}
                for state in states
            ],
        },
    )
    monkeypatch.setattr(production_readiness, "load_external_source_run_coverage_inputs", lambda conn, run_id: {"run": {"run_id": run_id}})
    patch_ready_artifact_gate(monkeypatch)
    monkeypatch.setattr(
        production_readiness,
        "build_external_source_run_coverage_report",
        lambda inputs, run_id=None: {
            "status": "ready",
            "run_id": run_id,
            "reason": "external evidence covers this run",
            "order_state_coverage_pct": "100",
            "calibration_coverage_pct": "100",
        },
    )

    report = build_fill_first_production_readiness_report(
        env=ready_env(),
        target_mode="paper",
        check_db=True,
        conn=object(),
        run_id=77,
    )

    assert report["status"] == "ready"
    assert report["launch_allowed"] is True
    assert any(check["name"] == "run-level external evidence coverage" for check in report["checks"])
    assert any(check["name"] == "paper/live evidence gate" for check in report["checks"])


def test_production_readiness_blocks_run_when_paper_live_gate_blocks_paper(monkeypatch) -> None:
    state_keys = ["order-state-live", "cost-events-live", "incidents-live", "signals-live"]
    monkeypatch.setattr(
        production_readiness,
        "load_external_source_import_states",
        lambda conn, limit=100: [{"state_key": key} for key in state_keys],
    )
    monkeypatch.setattr(
        production_readiness,
        "evaluate_external_source_import_health",
        lambda states, max_stale_seconds=86400: {
            "status": "ready",
            "reason": "all_ready",
            "state_count": len(states),
            "items": [
                {"state_key": state["state_key"], "status": "ready", "reason": "ready", "last_rows_written": 1}
                for state in states
            ],
        },
    )
    monkeypatch.setattr(production_readiness, "load_external_source_run_coverage_inputs", lambda conn, run_id: {"run": {"run_id": run_id}})
    monkeypatch.setattr(
        production_readiness,
        "build_external_source_run_coverage_report",
        lambda inputs, run_id=None: {
            "status": "ready",
            "run_id": run_id,
            "reason": "external evidence covers this run",
            "order_state_coverage_pct": "100",
            "calibration_coverage_pct": "100",
        },
    )
    patch_ready_artifact_gate(monkeypatch, paper_allowed=False, status="blocked")

    report = build_fill_first_production_readiness_report(
        env=ready_env(),
        target_mode="paper",
        check_db=True,
        conn=object(),
        run_id=77,
    )

    assert report["status"] == "blocked"
    assert report["launch_allowed"] is False
    assert report["paper_live_evidence_gate"]["checked"] is True
    assert any("paper/live evidence gate blocked" in reason for reason in report["blocked_reasons"])


def test_production_readiness_accepts_local_run_artifact_gate_for_fixture() -> None:
    artifact_report = {
        "status": "ready",
        "paper_live_evidence_gate_report": {
            "status": "ready",
            "paper_allowed": True,
            "live_allowed": False,
            "run_evidence_ready": True,
            "missing_evidence_ready": True,
            "blocked_reasons": [],
            "review_reasons": ["fixture paper-only gate"],
        },
    }

    report = build_fill_first_production_readiness_report(
        env=ready_env(),
        target_mode="paper",
        check_db=False,
        run_id=77,
        run_artifact_report=artifact_report,
    )

    assert report["status"] == "ready"
    assert report["launch_allowed"] is True
    assert report["paper_live_evidence_gate"]["checked"] is True
    assert report["paper_live_evidence_gate"]["paper_allowed"] is True
    assert any(check["name"] == "paper/live evidence gate" for check in report["checks"])


def test_production_readiness_blocks_live_without_confirmation() -> None:
    env = ready_env()
    env["ORDER_EXECUTION_TARGET_MODE"] = "live"
    report = build_fill_first_production_readiness_report(env=env, target_mode="live")

    assert report["status"] == "blocked"
    assert report["launch_allowed"] is False
    assert any("ORDER_EXECUTION_LIVE_CONFIRM" in reason for reason in report["blocked_reasons"])


def test_production_readiness_allows_confirmed_live() -> None:
    env = ready_env()
    env["ORDER_EXECUTION_TARGET_MODE"] = "live"
    env["ORDER_EXECUTION_LIVE_CONFIRM"] = LIVE_CONFIRM_TOKEN
    report = build_fill_first_production_readiness_report(env=env, target_mode="live")

    assert report["status"] == "ready"
    assert report["launch_allowed"] is True


def test_production_readiness_markdown_lists_next_actions() -> None:
    markdown = fill_first_production_readiness_to_markdown(
        build_fill_first_production_readiness_report(env={})
    )

    assert "Fill-first Production Readiness" in markdown
    assert "Next Actions" in markdown
    assert "order execution endpoint" in markdown
