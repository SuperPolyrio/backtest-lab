from pathlib import Path
from types import SimpleNamespace
from typing import Any, Sequence
import importlib.util

import quant.backtest.fill_first_quality_gate as quality_gate
import quant.backtest.fill_first_production_readiness as production_readiness
from quant.backtest.backtest_engine import BACKTEST_ARTIFACT_SCHEMA_VERSION
from quant.backtest.external_source_fixture import build_fill_first_external_source_fixture
from quant.backtest.fill_first_quality_gate import (
    FAIL,
    MISSING,
    READY,
    REVIEW,
    aggregate_quality_gate_status,
    build_fill_first_quality_gate_report,
    quality_gate_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _load_quality_gate_cli_module() -> Any:
    script = PROJECT_ROOT / "scripts" / "run_fill_first_quality_gate.py"
    spec = importlib.util.spec_from_file_location("run_fill_first_quality_gate_cli", script)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_quality_gate_status_prioritizes_fail_then_review() -> None:
    assert aggregate_quality_gate_status([READY, READY]) == READY
    assert aggregate_quality_gate_status([READY, REVIEW]) == REVIEW
    assert aggregate_quality_gate_status([READY, FAIL, REVIEW]) == FAIL


def test_quality_gate_cli_stage_check_enables_local_verification_without_db() -> None:
    module = _load_quality_gate_cli_module()
    options = module.quality_gate_options(
        SimpleNamespace(
            stage_check=True,
            check_db=False,
            include_run_artifact_audit=False,
            include_fill_evidence_validation=False,
            include_external_run_coverage=False,
            include_external_fixture_db_smoke=False,
            include_pytest=False,
            include_frontend_smoke=False,
        )
    )

    assert options["include_pytest"] is True
    assert options["include_frontend_smoke"] is True
    assert options["include_run_artifact_audit"] is False
    assert options["include_fill_evidence_validation"] is False
    assert options["include_external_run_coverage"] is False
    assert options["include_external_fixture_db_smoke"] is False


def test_quality_gate_cli_stage_check_enables_db_verification_with_check_db() -> None:
    module = _load_quality_gate_cli_module()
    options = module.quality_gate_options(
        SimpleNamespace(
            stage_check=True,
            check_db=True,
            include_run_artifact_audit=False,
            include_fill_evidence_validation=False,
            include_external_run_coverage=False,
            include_external_fixture_db_smoke=False,
            include_pytest=False,
            include_frontend_smoke=False,
        )
    )

    assert options["include_pytest"] is True
    assert options["include_frontend_smoke"] is True
    assert options["include_run_artifact_audit"] is True
    assert options["include_fill_evidence_validation"] is True
    assert options["include_external_run_coverage"] is True
    assert options["include_external_fixture_db_smoke"] is True


def test_quality_gate_builds_review_report_without_db() -> None:
    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        check_db=False,
        run_fixture_smoke=False,
    )

    assert report["status"] == REVIEW
    assert report["fail_count"] == 0
    assert any(check["name"] == "structural readiness" for check in report["checks"])
    assert any(check["name"] == "phase1 doc alignment" and check["status"] == READY for check in report["checks"])
    onboarding = next(check for check in report["checks"] if check["name"] == "external source onboarding")
    assert onboarding["status"] == REVIEW
    assert onboarding["payload"]["review_count"] == 4
    assert onboarding["payload"]["production_blockers"]
    assert any(check["name"] == "external source health" and check["status"] == REVIEW for check in report["checks"])


def test_quality_gate_uses_discovery_roots(tmp_path: Path) -> None:
    source_dir = tmp_path / "exports"
    source_dir.mkdir()
    (source_dir / "wallet_cost_events.jsonl").write_text(
        '{"cost_id":"c1","event_type":"gas","amount":"0.12"}\n',
        encoding="utf-8",
    )

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        discovery_roots=[source_dir],
        run_fixture_smoke=False,
    )
    discovery = next(check for check in report["checks"] if check["name"] == "external source discovery")

    assert discovery["status"] == READY
    assert discovery["payload"]["file_count"] == 1
    assert discovery["payload"]["records_read"] == 1


def test_quality_gate_reports_configured_external_sources(monkeypatch) -> None:
    monkeypatch.setenv("COST_EVENTS_INPUT", "/tmp/real_cost_events.jsonl")
    monkeypatch.delenv("ORDER_STATE_API_URL", raising=False)
    monkeypatch.delenv("ORDER_STATE_INPUT", raising=False)
    monkeypatch.delenv("PLATFORM_INCIDENTS_INPUT", raising=False)
    monkeypatch.delenv("PLATFORM_INCIDENTS_URL", raising=False)
    monkeypatch.delenv("EXTERNAL_SIGNAL_INPUT", raising=False)
    monkeypatch.delenv("EXTERNAL_SIGNAL_URL", raising=False)

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        run_fixture_smoke=False,
    )
    configured = next(check for check in report["checks"] if check["name"] == "configured external source imports")

    assert configured["status"] == READY
    assert configured["payload"]["configured_count"] == 1


def test_quality_gate_accepts_external_source_env() -> None:
    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        run_fixture_smoke=False,
        external_source_env={
            "ORDER_STATE_API_URL": "https://orders.internal.local/orders",
            "ORDER_STATE_AUTH_HEADER": "Authorization=test",
            "ORDER_STATE_KEY": "orders-live",
            "COST_EVENTS_URL": "https://costs.internal.local/events",
            "COST_EVENTS_AUTH_HEADER": "Authorization=test",
            "COST_EVENTS_STATE_KEY": "costs-live",
            "PLATFORM_INCIDENTS_URL": "https://incidents.internal.local/events",
            "PLATFORM_INCIDENTS_AUTH_HEADER": "Authorization=test",
            "PLATFORM_INCIDENTS_STATE_KEY": "incidents-live",
            "EXTERNAL_SIGNAL_URL": "https://signals.internal.local/events",
            "EXTERNAL_SIGNAL_AUTH_HEADER": "Authorization=test",
            "EXTERNAL_SIGNAL_STATE_KEY": "signals-live",
            "ORDER_EXECUTION_TARGET_MODE": "paper",
            "ORDER_EXECUTION_SUBMIT_URL": "https://orders.internal.local/submit",
            "ORDER_EXECUTION_AUTH_HEADER": "Authorization=test",
        },
    )
    audit = next(check for check in report["checks"] if check["name"] == "external source env audit")
    order_execution = next(check for check in report["checks"] if check["name"] == "order execution env audit")
    configured = next(check for check in report["checks"] if check["name"] == "configured external source imports")
    onboarding = next(check for check in report["checks"] if check["name"] == "external source onboarding")

    assert audit["status"] == READY
    assert onboarding["status"] == READY
    assert onboarding["payload"]["ready_count"] == 4
    assert order_execution["status"] == READY
    assert configured["status"] == READY
    assert configured["payload"]["configured_count"] == 4


def test_quality_gate_check_db_feeds_production_readiness(monkeypatch) -> None:
    def fake_states(conn, limit=100):
        return [{"state_key": key} for key in ("orders-live", "costs-live", "incidents-live", "signals-live")]

    def fake_health(states, max_stale_seconds=86400):
        return {
            "status": READY,
            "reason": "all_ready",
            "state_count": len(states),
            "items": [
                {"state_key": state["state_key"], "status": READY, "reason": "ready", "last_rows_written": 1}
                for state in states
            ],
        }

    monkeypatch.setattr(
        quality_gate,
        "load_external_source_import_states",
        fake_states,
    )
    monkeypatch.setattr(
        quality_gate,
        "evaluate_external_source_import_health",
        fake_health,
    )
    monkeypatch.setattr(production_readiness, "load_external_source_import_states", fake_states)
    monkeypatch.setattr(production_readiness, "evaluate_external_source_import_health", fake_health)

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        conn=object(),
        check_db=True,
        run_fixture_smoke=False,
        external_source_env={
            "ORDER_STATE_API_URL": "https://orders.internal.local/orders",
            "ORDER_STATE_AUTH_HEADER": "Authorization=test",
            "ORDER_STATE_KEY": "orders-live",
            "COST_EVENTS_URL": "https://costs.internal.local/events",
            "COST_EVENTS_AUTH_HEADER": "Authorization=test",
            "COST_EVENTS_STATE_KEY": "costs-live",
            "PLATFORM_INCIDENTS_URL": "https://incidents.internal.local/events",
            "PLATFORM_INCIDENTS_AUTH_HEADER": "Authorization=test",
            "PLATFORM_INCIDENTS_STATE_KEY": "incidents-live",
            "EXTERNAL_SIGNAL_URL": "https://signals.internal.local/events",
            "EXTERNAL_SIGNAL_AUTH_HEADER": "Authorization=test",
            "EXTERNAL_SIGNAL_STATE_KEY": "signals-live",
            "ORDER_EXECUTION_TARGET_MODE": "paper",
            "ORDER_EXECUTION_SUBMIT_URL": "https://orders.internal.local/submit",
            "ORDER_EXECUTION_AUTH_HEADER": "Authorization=test",
        },
    )
    production = next(check for check in report["checks"] if check["name"] == "fill-first production readiness")

    assert production["payload"]["check_db"] is True
    assert production["payload"]["external_source_import_health"]["checked"] is True
    assert production["payload"]["launch_allowed"] is True


def test_quality_gate_can_include_external_run_coverage(monkeypatch) -> None:
    monkeypatch.setattr(quality_gate, "load_latest_fill_first_backtest_run_id", lambda conn: 7)
    monkeypatch.setattr(quality_gate, "load_external_source_import_states", lambda conn, limit=100: [])
    monkeypatch.setattr(
        quality_gate,
        "evaluate_external_source_import_health",
        lambda states, max_stale_seconds=86400: {
            "status": "unknown",
            "reason": "no_import_state_rows",
            "state_count": 0,
            "items": [],
        },
    )
    monkeypatch.setattr(
        quality_gate,
        "load_external_source_run_coverage_inputs",
        lambda conn, run_id: {
            "run": {"run_id": run_id, "market_slug": "market-a", "token_side": "YES"},
            "orders": [{"order_id": "o1", "status": "FILLED"}],
            "real_order_events": [{"order_id": "o1", "payload": {"live_status": "FILLED"}}],
            "calibration_rows": [{"simulated_order_id": "o1", "live_status": "FILLED"}],
            "real_cost_events": [],
            "cost_calibration_rows": [],
            "platform_incidents": [{"incident_key": "i1"}],
            "external_states": [{"state_key": "orders-live"}],
        },
    )

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        conn=object(),
        check_db=True,
        run_fixture_smoke=False,
        include_external_run_coverage=True,
    )
    coverage = next(check for check in report["checks"] if check["name"] == "external source run coverage")

    assert coverage["status"] == READY
    assert coverage["payload"]["order_state_coverage_pct"] == "100"
    assert coverage["payload"]["calibration_coverage_pct"] == "100"


def test_quality_gate_can_include_fill_evidence_validation(monkeypatch) -> None:
    monkeypatch.setattr(quality_gate, "load_latest_fill_first_backtest_run_id", lambda conn: 72)
    monkeypatch.setattr(quality_gate, "load_external_source_import_states", lambda conn, limit=100: [])
    monkeypatch.setattr(
        quality_gate,
        "load_backtest_run_artifact_inputs",
        lambda conn, run_id: {
            "run": {
                "run_id": run_id,
                "meta": {
                    "actual_data_quality": {
                        "fill_quality": {
                            "submitted_count": 1,
                            "filled_count": 1,
                            "no_fill_count": 0,
                            "raw_orderfilled_fill_count": 1,
                            "block_bar_synthetic_fill_count": 0,
                            "raw_replay_coverage_pct": "100",
                            "block_bar_fallback_pct": "0",
                            "execution_evidence_counts": {"raw_orderfilled": 1},
                            "fill_evidence_counts": {"raw_orderfilled": 1},
                        }
                    }
                },
            },
            "orders": [
                {
                    "order_id": "O-1",
                    "status": "FILLED",
                    "execution_evidence_type": "raw_orderfilled",
                    "raw_candidate_event_count": 2,
                    "raw_consumed_event_count": 1,
                }
            ],
        },
    )

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        conn=object(),
        run_fixture_smoke=False,
        include_fill_evidence_validation=True,
    )
    validation = next(check for check in report["checks"] if check["name"] == "latest fill evidence validation")

    assert validation["status"] == READY
    assert validation["payload"]["run_id"] == 72
    assert validation["payload"]["raw_replay_coverage_pct"] == "100"


def test_quality_gate_fails_filled_orders_without_auditable_evidence(monkeypatch) -> None:
    monkeypatch.setattr(quality_gate, "load_latest_fill_first_backtest_run_id", lambda conn: 73)
    monkeypatch.setattr(quality_gate, "load_external_source_import_states", lambda conn, limit=100: [])
    monkeypatch.setattr(
        quality_gate,
        "load_backtest_run_artifact_inputs",
        lambda conn, run_id: {
            "run": {
                "run_id": run_id,
                "meta": {
                    "actual_data_quality": {
                        "fill_quality": {
                            "submitted_count": 1,
                            "filled_count": 1,
                            "no_fill_count": 0,
                            "raw_orderfilled_fill_count": 0,
                            "block_bar_synthetic_fill_count": 0,
                            "raw_replay_coverage_pct": "0",
                            "block_bar_fallback_pct": "0",
                            "execution_evidence_counts": {"none": 1},
                            "fill_evidence_counts": {"none": 1},
                        }
                    }
                },
            },
            "orders": [
                {
                    "order_id": "O-1",
                    "status": "FILLED",
                    "execution_evidence_type": "none",
                    "raw_candidate_event_count": 0,
                    "raw_consumed_event_count": 0,
                }
            ],
        },
    )

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        conn=object(),
        run_fixture_smoke=False,
        include_fill_evidence_validation=True,
    )
    validation = next(check for check in report["checks"] if check["name"] == "latest fill evidence validation")

    assert report["status"] == FAIL
    assert validation["status"] == FAIL
    assert validation["payload"]["unsupported_filled_evidence_count"] == 1


def test_quality_gate_accepts_external_source_env_file(tmp_path: Path) -> None:
    fixture = build_fill_first_external_source_fixture(tmp_path / "fixture")

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        run_fixture_smoke=False,
        external_source_env_files=[Path(fixture["files"]["env_file"])],
    )
    audit = next(check for check in report["checks"] if check["name"] == "external source env audit")
    configured = next(check for check in report["checks"] if check["name"] == "configured external source imports")
    onboarding = next(check for check in report["checks"] if check["name"] == "external source onboarding")

    assert audit["status"] == READY
    assert audit["payload"]["loaded_env_file_count"] == 1
    assert onboarding["status"] == READY
    assert onboarding["payload"]["ready_count"] == 4
    assert configured["status"] == READY
    assert configured["payload"]["configured_count"] == 4


def test_quality_gate_accepts_order_execution_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "order-execution.env"
    env_file.write_text(
        "\n".join(
            (
                "ORDER_EXECUTION_TARGET_MODE=paper",
                "ORDER_EXECUTION_SUBMIT_URL=http://127.0.0.1:9/fill-first-paper-submit",
                "ORDER_EXECUTION_AUTH_HEADER=",
                "ORDER_EXECUTION_SOURCE=paper-dry-run-local",
                "",
            )
        ),
        encoding="utf-8",
    )

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        run_fixture_smoke=False,
        order_execution_env_files=[env_file],
    )
    external_audit = next(check for check in report["checks"] if check["name"] == "external source env audit")
    order_execution = next(check for check in report["checks"] if check["name"] == "order execution env audit")

    assert external_audit["status"] == REVIEW
    assert order_execution["status"] == READY
    assert order_execution["payload"]["configured"] is True
    assert order_execution["payload"]["submit_url"] == "http://127.0.0.1:9/fill-first-paper-submit"
    assert order_execution["payload"]["target_mode"] == "paper"


def test_quality_gate_merges_external_source_and_order_execution_env_files(tmp_path: Path) -> None:
    fixture = build_fill_first_external_source_fixture(tmp_path / "fixture")
    order_env_file = tmp_path / "order-execution.env"
    order_env_file.write_text(
        "\n".join(
            (
                "ORDER_EXECUTION_TARGET_MODE=paper",
                "ORDER_EXECUTION_SUBMIT_URL=http://127.0.0.1:9/fill-first-paper-submit",
                "ORDER_EXECUTION_AUTH_HEADER=",
                "ORDER_EXECUTION_SOURCE=paper-dry-run-local",
                "",
            )
        ),
        encoding="utf-8",
    )

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        run_fixture_smoke=False,
        external_source_env_files=[Path(fixture["files"]["env_file"])],
        order_execution_env_files=[order_env_file],
    )
    external_audit = next(check for check in report["checks"] if check["name"] == "external source env audit")
    configured = next(check for check in report["checks"] if check["name"] == "configured external source imports")
    order_execution = next(check for check in report["checks"] if check["name"] == "order execution env audit")
    production = next(check for check in report["checks"] if check["name"] == "fill-first production readiness")

    assert external_audit["status"] == READY
    assert configured["status"] == READY
    assert configured["payload"]["configured_count"] == 4
    assert order_execution["status"] == READY
    assert production["payload"]["order_execution_env_audit"]["configured"] is True
    assert production["payload"]["launch_allowed"] is True
    assert "ORDER_EXECUTION_SUBMIT_URL is empty" not in production["payload"]["blocked_reasons"]


def test_quality_gate_reviews_placeholder_external_source_env() -> None:
    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        run_fixture_smoke=False,
        external_source_env={
            "ORDER_STATE_API_URL": "https://replace-with-private-order-api.example/orders",
            "ORDER_STATE_AUTH_HEADER": "Authorization=REPLACE_WITH_PRIVATE_AUTH_VALUE",
        },
    )
    audit = next(check for check in report["checks"] if check["name"] == "external source env audit")
    onboarding = next(check for check in report["checks"] if check["name"] == "external source onboarding")

    assert audit["status"] == REVIEW
    assert onboarding["status"] == REVIEW
    assert onboarding["payload"]["production_blockers"]
    assert any("placeholder values" in issue for issue in audit["payload"]["issues"])


def test_quality_gate_fails_bad_external_source_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "bad.env"
    env_file.write_text("NOT_A_KEY_VALUE_LINE\n", encoding="utf-8")

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        run_fixture_smoke=False,
        external_source_env_files=[env_file],
    )
    audit = next(check for check in report["checks"] if check["name"] == "external source env audit")
    onboarding = next(check for check in report["checks"] if check["name"] == "external source onboarding")

    assert report["status"] == FAIL
    assert audit["status"] == FAIL
    assert onboarding["status"] == FAIL
    assert "env file load failed" in audit["payload"]["issues"][0]


def test_quality_gate_can_include_command_checks() -> None:
    def fake_runner(command: Sequence[str], cwd: Path) -> dict[str, Any]:
        return {"command": list(command), "cwd": str(cwd), "returncode": 0, "output_tail": "ok"}

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        run_fixture_smoke=False,
        include_pytest=True,
        include_frontend_smoke=True,
        command_runner=fake_runner,
    )

    assert any(check["name"] == "pytest backtest suite" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "static frontend smoke" and check["status"] == REVIEW
               and check["evidence"] == "platform-website" for check in report["checks"])


def test_quality_gate_runs_external_source_fixture() -> None:
    report = build_fill_first_quality_gate_report(PROJECT_ROOT, run_fixture_smoke=True)
    fixture = next(check for check in report["checks"] if check["name"] == "external source fixture")
    artifact_fixture = next(check for check in report["checks"] if check["name"] == "current schema run artifact fixture")
    cli_gate = next(check for check in report["checks"] if check["name"] == "production readiness CLI artifact gate fixture")
    bootstrap = next(check for check in report["checks"] if check["name"] == "external source env bootstrap fixture")
    order_execution_bootstrap = next(check for check in report["checks"] if check["name"] == "order execution env bootstrap fixture")
    onboarding = next(check for check in report["checks"] if check["name"] == "external source onboarding fixture")
    parameter_plan = next(check for check in report["checks"] if check["name"] == "parameter search plan fixture")
    parameter_results = next(check for check in report["checks"] if check["name"] == "parameter search results fixture")
    parameter_batch = next(check for check in report["checks"] if check["name"] == "parameter search batch fixture")
    parameter_scheduler = next(check for check in report["checks"] if check["name"] == "parameter search scheduler fixture")
    parameter_staging = next(check for check in report["checks"] if check["name"] == "production parameter staging fixture")
    parameter_staging_review = next(check for check in report["checks"] if check["name"] == "production parameter staging review fixture")

    assert fixture["status"] == READY
    assert fixture["payload"]["status"] == READY
    assert fixture["payload"]["event_counts"]["order_state"] == 3
    assert fixture["payload"]["event_counts"]["external_signals"] == 2
    assert fixture["payload"]["configured_imports"]["configured_count"] == 4
    assert bootstrap["status"] == READY
    assert bootstrap["payload"]["audit_status"] == READY
    assert bootstrap["payload"]["onboarding_status"] == READY
    assert bootstrap["payload"]["contains_real_evidence"] is False
    assert order_execution_bootstrap["status"] == READY
    assert order_execution_bootstrap["payload"]["audit_status"] == READY
    assert order_execution_bootstrap["payload"]["contains_live_execution"] is False
    assert order_execution_bootstrap["payload"]["contains_secret"] is False
    assert onboarding["status"] == READY
    assert onboarding["payload"]["ready_count"] == 4
    assert all(item["dry_run_command"] and item["write_command"] for item in onboarding["payload"]["sources"])
    assert parameter_plan["status"] == READY
    assert {"train", "test", "walk_forward"} <= set(parameter_plan["payload"]["evidence_modes"])
    assert {"realistic", "conservative"} <= set(parameter_plan["payload"]["execution_profiles"])
    assert parameter_results["status"] == READY
    assert parameter_results["payload"]["coverage_pct"] == "100"
    assert parameter_results["payload"]["robustness_report"]["robustness_verdict"] == READY
    assert parameter_results["payload"]["production_parameter_staging"]["staging_allowed"] is True
    assert parameter_batch["status"] == READY
    assert parameter_batch["payload"]["executed_run_count"] == parameter_batch["payload"]["planned_run_count"]
    assert parameter_batch["payload"]["parameter_search_results"]["status"] == READY
    assert parameter_batch["payload"]["staging_preview"]["staging_allowed"] is True
    assert parameter_scheduler["status"] == READY
    assert parameter_scheduler["payload"]["ready"]["status"] == READY
    assert parameter_scheduler["payload"]["retryable"]["retryable_count"] == 1
    assert parameter_scheduler["payload"]["canceled"]["status"] == "canceled"
    assert parameter_staging["status"] == READY
    assert parameter_staging["payload"]["staging_allowed"] is True
    assert parameter_staging["payload"]["status"] == "pending"
    assert parameter_staging_review["status"] == READY
    assert parameter_staging_review["payload"]["approved"]["approved"] is True
    assert parameter_staging_review["payload"]["rejected"]["status"] == "rejected"
    assert cli_gate["status"] == READY
    assert cli_gate["payload"]["report"]["paper_live_evidence_gate"]["checked"] is True
    assert cli_gate["payload"]["report"]["paper_live_evidence_gate"]["paper_allowed"] is True
    assert artifact_fixture["status"] == READY
    assert artifact_fixture["payload"]["artifacts"]["artifact_schema_version"] == BACKTEST_ARTIFACT_SCHEMA_VERSION
    assert artifact_fixture["payload"]["reproducibility_report"]["reproducibility_verdict"] == READY


def test_quality_gate_can_include_external_fixture_db_smoke(monkeypatch) -> None:
    def fake_pipeline(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {
            "status": READY,
            "db": {
                "status": READY,
                "rolled_back": True,
                "table_counts": {
                    "quant.real_order_state_events": 3,
                    "quant.real_backtest_cost_events": 2,
                    "quant.platform_incidents": 1,
                },
            },
        }

    monkeypatch.setattr(quality_gate, "run_fill_first_external_source_fixture_pipeline", fake_pipeline)
    monkeypatch.setattr(quality_gate, "load_external_source_import_states", lambda conn, limit=100: [])

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        conn=object(),
        include_external_fixture_db_smoke=True,
        run_fixture_smoke=False,
    )
    smoke = next(check for check in report["checks"] if check["name"] == "external source fixture DB smoke")

    assert smoke["status"] == READY
    assert "rolled_back=True" in smoke["detail"]


def test_quality_gate_reviews_when_no_fill_first_latest_run(monkeypatch) -> None:
    monkeypatch.setattr(quality_gate, "load_latest_fill_first_backtest_run_id", lambda conn: None)
    monkeypatch.setattr(quality_gate, "load_backtest_run_artifact_inputs", lambda conn, run_id: {"run": {"run_id": run_id}})
    monkeypatch.setattr(
        quality_gate,
        "build_backtest_run_artifact_report",
        lambda inputs, run_id=None: {
            "status": MISSING,
            "run_id": run_id,
            "reason": "no_fill_first_backtest_runs",
            "checks": [],
            "artifacts": {},
        },
    )
    monkeypatch.setattr(quality_gate, "load_external_source_import_states", lambda conn, limit=100: [])

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        conn=object(),
        check_db=True,
        run_fixture_smoke=False,
        include_run_artifact_audit=True,
        allow_empty_external=True,
    )
    audit = next(check for check in report["checks"] if check["name"] == "latest fill-first run artifact audit")

    assert audit["status"] == REVIEW
    assert "no fill-first run found" in audit["detail"]


def test_quality_gate_fails_missing_artifact_for_fill_first_latest_run(monkeypatch) -> None:
    monkeypatch.setattr(quality_gate, "load_latest_fill_first_backtest_run_id", lambda conn: 67)
    monkeypatch.setattr(quality_gate, "load_backtest_run_artifact_inputs", lambda conn, run_id: {"run": {"run_id": run_id}})
    monkeypatch.setattr(
        quality_gate,
        "build_backtest_run_artifact_report",
        lambda inputs, run_id=None: {
            "status": MISSING,
            "run_id": run_id,
            "reproducibility_report": {
                "artifact_schema_version": BACKTEST_ARTIFACT_SCHEMA_VERSION,
                "parameter_snapshot": {
                    "parameters": {"execution_price_mode": "ORDERFILLED_CROSS"},
                }
            },
        },
    )
    monkeypatch.setattr(quality_gate, "load_external_source_import_states", lambda conn, limit=100: [])

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        conn=object(),
        check_db=True,
        run_fixture_smoke=False,
        include_run_artifact_audit=True,
        allow_empty_external=True,
    )
    audit = next(check for check in report["checks"] if check["name"] == "latest fill-first run artifact audit")

    assert report["status"] == FAIL
    assert audit["status"] == FAIL


def test_quality_gate_reviews_missing_artifact_for_old_fill_first_schema(monkeypatch) -> None:
    monkeypatch.setattr(quality_gate, "load_latest_fill_first_backtest_run_id", lambda conn: 68)
    monkeypatch.setattr(quality_gate, "load_backtest_run_artifact_inputs", lambda conn, run_id: {"run": {"run_id": run_id}})
    monkeypatch.setattr(
        quality_gate,
        "build_backtest_run_artifact_report",
        lambda inputs, run_id=None: {
            "status": MISSING,
            "run_id": run_id,
            "reproducibility_report": {
                "artifact_schema_version": None,
                "parameter_snapshot": {
                    "parameters": {"execution_price_mode": "ORDERFILLED_CROSS"},
                },
            },
        },
    )
    monkeypatch.setattr(quality_gate, "load_external_source_import_states", lambda conn, limit=100: [])

    report = build_fill_first_quality_gate_report(
        PROJECT_ROOT,
        conn=object(),
        check_db=True,
        run_fixture_smoke=False,
        include_run_artifact_audit=True,
        allow_empty_external=True,
    )
    audit = next(check for check in report["checks"] if check["name"] == "latest fill-first run artifact audit")

    assert audit["status"] == REVIEW
    assert "not current fill-first schema" in audit["detail"]


def test_quality_gate_markdown_contains_next_actions() -> None:
    report = build_fill_first_quality_gate_report(PROJECT_ROOT, run_fixture_smoke=False)
    markdown = quality_gate_to_markdown(report)

    assert "Fill-first Quality Gate" in markdown
    assert "Next Actions" in markdown
    assert "structural readiness" in markdown
    assert "phase1 doc alignment" in markdown
