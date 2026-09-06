from pathlib import Path

from quant.backtest.fill_first_readiness import READY
from quant.backtest.l2_execution_doc_alignment import (
    build_l2_execution_doc_alignment_report,
    l2_execution_doc_alignment_to_markdown,
)


def test_l2_execution_doc_alignment_is_ready_for_current_code() -> None:
    report = build_l2_execution_doc_alignment_report(Path(__file__).resolve().parents[3])

    assert report["status"] == READY
    assert report["missing_count"] == 0
    assert report["ready_count"] >= 10
    assert any(check["name"] == "matching engine timeline" for check in report["checks"])
    assert any(check["name"] == "legacy execution removed" for check in report["checks"])


def test_l2_execution_doc_alignment_markdown_lists_scope_and_checks() -> None:
    report = build_l2_execution_doc_alignment_report(Path(__file__).resolve().parents[3])

    markdown = l2_execution_doc_alignment_to_markdown(report)

    assert "# L2 + OrderFilled Execution Doc Alignment: ready" in markdown
    assert "polymarket_execution_model_guidance_for_codex.md" in markdown
    assert "matching engine timeline" in markdown
