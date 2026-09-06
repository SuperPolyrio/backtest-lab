import subprocess
import sys
from pathlib import Path

from quant.backtest.external_source_fixture import build_fill_first_external_source_fixture
from quant.backtest.production_launch_checklist import (
    READY,
    build_fill_first_production_launch_checklist,
    production_launch_checklist_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "scripts" / "plan_fill_first_production_launch.py"


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


def test_production_launch_checklist_blocks_unconfigured_env() -> None:
    report = build_fill_first_production_launch_checklist(env={}, target_mode="paper")

    assert report["status"] == "blocked"
    assert report["launch_allowed"] is False
    assert any(phase["name"] == "production readiness" and phase["status"] == "blocked" for phase in report["phases"])
    assert "1_external_env_audit" in report["command_plan"]
    assert "2_order_execution_env_audit" in report["command_plan"]
    assert "LOB/DEPTH intentionally excluded" in report["scope"]


def test_production_launch_checklist_ready_for_configured_paper_without_db_run_gate() -> None:
    report = build_fill_first_production_launch_checklist(env=ready_env(), target_mode="paper")

    assert report["status"] == "review"
    assert report["production_readiness"]["status"] == READY
    assert report["production_readiness"]["launch_allowed"] is True
    assert any(check["name"] == "external_signal_events" for check in report["production_readiness"]["checks"])
    assert any(phase["name"] == "run external evidence" and phase["status"] == "review" for phase in report["phases"])
    assert "check_fill_first_production_readiness.py" in report["command_plan"]["6_production_readiness"]
    assert "check_external_source_import_health.py --check-db" not in report["command_plan"]["5_import_health"]
    assert "quant-configured-external-sources-import.timer" in report["command_plan"]["8_install_configured_import_timer"]
    assert "quant-external-source-health.timer" in report["command_plan"]["9_install_external_health_timer"]


def test_production_launch_checklist_accepts_separate_source_and_order_env_files(tmp_path: Path) -> None:
    fixture = build_fill_first_external_source_fixture(tmp_path / "sources")
    order_env = tmp_path / "order-execution.env"
    order_env.write_text(
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

    report = build_fill_first_production_launch_checklist(
        external_source_env_files=[Path(fixture["files"]["env_file"])],
        order_execution_env_files=[order_env],
        target_mode="paper",
    )

    assert report["production_readiness"]["status"] == READY
    assert report["production_readiness"]["launch_allowed"] is True
    assert report["external_source_onboarding"]["status"] == READY
    assert report["production_readiness"]["order_execution_env_audit"]["configured"] is True
    assert str(order_env) in report["order_execution_env_files"]
    assert "--env-file" in report["command_plan"]["2_order_execution_env_audit"]
    assert "--external-env-file" in report["command_plan"]["7_quality_gate"]
    assert "--order-execution-env-file" in report["command_plan"]["7_quality_gate"]


def test_production_launch_checklist_markdown_lists_command_plan() -> None:
    markdown = production_launch_checklist_to_markdown(
        build_fill_first_production_launch_checklist(env=ready_env(), target_mode="paper")
    )

    assert "Fill-first Production Launch Checklist" in markdown
    assert "Command Plan" in markdown
    assert "audit_external_source_env.py" in markdown
    assert "audit_order_execution_env.py" in markdown
    assert "run_configured_external_source_imports.py" in markdown
    assert "quant-configured-external-sources-import.timer" in markdown


def test_plan_fill_first_production_launch_cli_outputs_json() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json"],
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    assert completed.returncode == 0
    assert '"schema_version": "fill_first_production_launch_checklist_v1"' in completed.stdout
    assert '"command_plan"' in completed.stdout
