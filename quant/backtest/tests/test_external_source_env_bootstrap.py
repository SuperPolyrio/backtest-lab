import json
import subprocess
import sys
from pathlib import Path

from quant.backtest.external_source_env_bootstrap import (
    READY,
    bootstrap_external_source_env,
    external_source_env_bootstrap_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "scripts" / "bootstrap_fill_first_external_sources.py"


def test_bootstrap_external_source_env_creates_ready_file_mode_env(tmp_path: Path) -> None:
    report = bootstrap_external_source_env(tmp_path / "sources", project_root=PROJECT_ROOT)

    assert report["status"] == READY
    assert report["env_written"] is True
    assert report["contains_real_evidence"] is False
    assert report["audit_status"] == READY
    assert report["onboarding_status"] == READY
    assert len(report["created_files"]) == 4
    env_text = Path(report["env_file"]).read_text(encoding="utf-8")
    assert "ORDER_STATE_INPUT=" in env_text
    assert "COST_EVENTS_STATE_KEY=real-cost-events-local-file" in env_text
    assert "EXTERNAL_SIGNAL_INPUT=" in env_text
    assert "REPLACE_WITH" not in env_text
    assert "Authorization=" not in env_text


def test_bootstrap_external_source_env_refuses_to_overwrite_without_flag(tmp_path: Path) -> None:
    first = bootstrap_external_source_env(tmp_path / "sources", project_root=PROJECT_ROOT)
    second = bootstrap_external_source_env(tmp_path / "sources", project_root=PROJECT_ROOT)

    assert first["status"] == READY
    assert second["status"] == "review"
    assert second["env_written"] is False
    assert "already exists" in second["reason"]


def test_bootstrap_external_source_env_markdown_lists_commands(tmp_path: Path) -> None:
    markdown = external_source_env_bootstrap_to_markdown(
        bootstrap_external_source_env(tmp_path / "sources", project_root=PROJECT_ROOT)
    )

    assert "Fill-first External Source Env Bootstrap" in markdown
    assert "contains_real_evidence: False" in markdown
    assert "audit_external_source_env.py" in markdown
    assert "run_configured_external_source_imports.py" in markdown


def test_bootstrap_fill_first_external_sources_cli_outputs_json(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--target-dir",
            str(tmp_path / "sources"),
            "--format",
            "json",
            "--strict",
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
    assert report["audit_status"] == READY
    assert report["onboarding_status"] == READY
