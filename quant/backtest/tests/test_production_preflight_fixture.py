from pathlib import Path

from quant.backtest.production_preflight_fixture import (
    READY,
    production_preflight_fixture_to_markdown,
    run_fill_first_production_preflight_fixture,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_production_preflight_fixture_is_ready_without_external_api_calls(tmp_path: Path) -> None:
    report = run_fill_first_production_preflight_fixture(
        tmp_path / "preflight",
        project_root=PROJECT_ROOT,
        overwrite=True,
    )

    assert report["status"] == READY
    assert report["external_source_env_audit"]["status"] == READY
    assert report["production_readiness"]["status"] == READY
    assert report["production_readiness"]["launch_allowed"] is True
    assert report["production_readiness"]["paper_live_evidence_gate"]["checked"] is True
    assert report["production_readiness"]["paper_live_evidence_gate"]["status"] == READY
    assert report["production_readiness"]["paper_live_evidence_gate"]["paper_allowed"] is True
    assert report["run_artifact_report"]["paper_live_evidence_gate_report"]["paper_allowed"] is True
    assert report["order_execution_safety"]["safety_status"] == "dry_run_safe"
    assert report["order_execution_safety"]["execute"] is False
    assert "127.0.0.1" in report["paper_dry_run_submit_url"]


def test_production_preflight_fixture_markdown_summarizes_preflight(tmp_path: Path) -> None:
    report = run_fill_first_production_preflight_fixture(
        tmp_path / "preflight",
        project_root=PROJECT_ROOT,
        overwrite=True,
    )
    markdown = production_preflight_fixture_to_markdown(report)

    assert "Fill-first Production Preflight Fixture" in markdown
    assert "external source env audit" in markdown
    assert "production readiness" in markdown
    assert "paper/live evidence gate" in markdown
    assert "dry-run" in markdown
