from pathlib import Path
import subprocess
import sys

from quant.backtest.fill_first_readiness import READY
from quant.backtest.phase1_doc_alignment import (
    build_phase1_doc_alignment_report,
    phase1_doc_alignment_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_phase1_doc_alignment_report_is_ready() -> None:
    report = build_phase1_doc_alignment_report(PROJECT_ROOT)

    assert report["status"] == READY
    assert report["missing_count"] == 0
    names = {check["name"] for check in report["checks"]}
    assert "orders table lifecycle fields" in names
    assert "ledger cashflow table" in names
    assert "buy and sell crossing semantics tests" in names
    assert "frontend fill quality display" in names
    assert "sample backtest smoke command" in names


def test_phase1_doc_alignment_markdown_lists_acceptance_items() -> None:
    markdown = phase1_doc_alignment_to_markdown(build_phase1_doc_alignment_report(PROJECT_ROOT))

    assert "Phase 1 Fill-first Doc Alignment" in markdown
    assert "orders table lifecycle fields" in markdown
    assert "Strategy Tester displays Fill Quality" in markdown


def test_phase1_doc_alignment_cli_outputs_markdown() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "check_phase1_doc_alignment.py"),
            "--format",
            "markdown",
            "--strict",
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Phase 1 Fill-first Doc Alignment: ready" in completed.stdout
