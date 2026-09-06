import json
import subprocess
import sys
from pathlib import Path

from quant.backtest.external_source_fixture import build_fill_first_external_source_fixture
from quant.backtest.external_source_onboarding import (
    FAIL,
    READY,
    REVIEW,
    build_external_source_onboarding_plan,
    external_source_onboarding_plan_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "scripts" / "plan_external_source_onboarding.py"


def test_external_source_onboarding_reports_actionable_review_for_empty_env() -> None:
    report = build_external_source_onboarding_plan(env={}, project_root=PROJECT_ROOT)

    assert report["status"] == REVIEW
    assert report["source_count"] == 4
    assert report["ready_count"] == 0
    assert any("真实订单状态" in blocker for blocker in report["production_blockers"])
    assert any("外部信号输入" in blocker for blocker in report["production_blockers"])
    order_state = next(item for item in report["sources"] if item["kind"] == "real_order_state_events")
    assert order_state["configured"] is False
    assert order_state["dry_run_command"] == []
    assert order_state["write_command"] == []
    assert "source is still unconfigured" in order_state["completion_criteria"][-1]
    assert "scripts/audit_external_source_env.py" in "\n".join(report["next_actions"])
    assert "scripts/run_configured_external_source_imports.py" in "\n".join(report["next_actions"])
    assert "check_external_source_import_health.py --check-db" not in "\n".join(report["next_actions"])


def test_external_source_onboarding_accepts_fixture_env_file(tmp_path: Path) -> None:
    fixture = build_fill_first_external_source_fixture(tmp_path / "fixture", run_id=321)

    report = build_external_source_onboarding_plan(
        [Path(fixture["files"]["env_file"])],
        env={},
        project_root=PROJECT_ROOT,
    )

    assert report["status"] == READY
    assert report["ready_count"] == 4
    assert report["production_blockers"] == []
    for item in report["sources"]:
        assert item["configured"] is True
        assert item["dry_run_command"]
        assert item["write_command"]
        assert any("env audit status is ready" in criterion for criterion in item["completion_criteria"])


def test_external_source_onboarding_markdown_contains_completion_commands(tmp_path: Path) -> None:
    fixture = build_fill_first_external_source_fixture(tmp_path / "fixture", run_id=322)
    report = build_external_source_onboarding_plan([fixture["files"]["env_file"]], env={}, project_root=PROJECT_ROOT)

    markdown = external_source_onboarding_plan_to_markdown(report)

    assert "Completion criteria" in markdown
    assert "Dry-run command" in markdown
    assert "Write command" in markdown
    assert "外部信号输入" in markdown
    assert "scripts/check_external_source_run_coverage.py" in markdown
    assert "check_external_source_import_health.py --check-db" not in markdown


def test_external_source_onboarding_reports_fail_for_invalid_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / "bad.env"
    env_file.write_text("NOT_A_KEY_VALUE_LINE\n", encoding="utf-8")

    report = build_external_source_onboarding_plan([env_file], env={}, project_root=PROJECT_ROOT)

    assert report["status"] == FAIL
    assert report["fail_count"] == 1
    assert any("env file load failed" in blocker for blocker in report["production_blockers"])


def test_plan_external_source_onboarding_script_outputs_json(tmp_path: Path) -> None:
    fixture = build_fill_first_external_source_fixture(tmp_path / "fixture", run_id=323)

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--env-file",
            str(fixture["files"]["env_file"]),
            "--format",
            "json",
            "--strict-review",
        ],
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout
    report = json.loads(completed.stdout)
    assert report["status"] == READY
    assert report["ready_count"] == 4


def test_plan_external_source_onboarding_script_blocks_review() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--format", "json", "--strict-review"],
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    assert completed.returncode == 2
    report = json.loads(completed.stdout)
    assert report["status"] == REVIEW
