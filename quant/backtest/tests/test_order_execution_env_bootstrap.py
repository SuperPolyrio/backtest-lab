import json
import subprocess
import sys
from pathlib import Path

from quant.backtest.order_execution_env_bootstrap import (
    READY,
    bootstrap_order_execution_env,
    order_execution_env_bootstrap_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "scripts" / "bootstrap_fill_first_order_execution.py"


def test_bootstrap_order_execution_env_creates_ready_paper_only_env(tmp_path: Path) -> None:
    report = bootstrap_order_execution_env(tmp_path / "orders")

    assert report["status"] == READY
    assert report["env_written"] is True
    assert report["contains_live_execution"] is False
    assert report["contains_secret"] is False
    assert report["audit_status"] == READY
    env_text = Path(report["env_file"]).read_text(encoding="utf-8")
    assert "ORDER_EXECUTION_TARGET_MODE=paper" in env_text
    assert "ORDER_EXECUTION_SUBMIT_URL=http://127.0.0.1:9/fill-first-paper-submit" in env_text
    assert "ORDER_EXECUTION_AUTH_HEADER=" in env_text
    assert "I_UNDERSTAND_LIVE_ORDER_RISK" not in env_text


def test_bootstrap_order_execution_env_refuses_to_overwrite_without_flag(tmp_path: Path) -> None:
    first = bootstrap_order_execution_env(tmp_path / "orders")
    second = bootstrap_order_execution_env(tmp_path / "orders")

    assert first["status"] == READY
    assert second["status"] == "review"
    assert second["env_written"] is False
    assert "already exists" in second["reason"]


def test_order_execution_env_bootstrap_markdown_lists_dry_run_commands(tmp_path: Path) -> None:
    markdown = order_execution_env_bootstrap_to_markdown(
        bootstrap_order_execution_env(tmp_path / "orders")
    )

    assert "Fill-first Order Execution Env Bootstrap" in markdown
    assert "contains_live_execution: False" in markdown
    assert "audit_order_execution_env.py" in markdown
    assert "run_order_execution_adapter.py" in markdown
    assert "--execute" not in markdown


def test_bootstrap_fill_first_order_execution_cli_outputs_json(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--target-dir",
            str(tmp_path / "orders"),
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
    assert report["contains_live_execution"] is False
