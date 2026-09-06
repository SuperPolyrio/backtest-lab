from pathlib import Path

from quant.backtest.backtest_core_gate import (
    READY,
    build_backtest_core_gate_report,
    core_gate_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_backtest_core_gate_excludes_paper_live_and_external_domains() -> None:
    report = build_backtest_core_gate_report(PROJECT_ROOT)

    assert report["status"] == READY
    assert "paper/live/production excluded" in report["scope"]
    check_names = [check["name"].lower() for check in report["checks"]]
    assert check_names
    forbidden = ("paper", "live", "external", "production", "guarded", "order execution")
    assert not any(token in name for name in check_names for token in forbidden)
    artifact_payload = next(check["payload"] for check in report["checks"] if check["name"] == "current schema core artifact fixture")
    assert "paper/live evidence gate" in artifact_payload["excluded_check_names"]
    assert "external source run coverage" in artifact_payload["excluded_check_names"]
    assert "performance score report" in artifact_payload["included_check_names"]
    assert "event stream contract" in artifact_payload["included_check_names"]
    assert "raw orderfilled replay contract" in artifact_payload["included_check_names"]
    assert artifact_payload["data_access_contract"]["status"] == READY
    assert artifact_payload["data_access_contract"]["price_access_path_allowed"] is True
    assert artifact_payload["data_access_contract"]["raw_detail_has_limit"] is True
    pmxt_check = next(check for check in report["checks"] if check["name"] == "pmxt l2 replay fixture")
    assert pmxt_check["status"] == READY
    assert pmxt_check["payload"]["upsert_delta_count"] == 1
    sql_contract = next(check for check in report["checks"] if check["name"] == "generic replay sql contract")
    assert sql_contract["status"] == READY
    assert sql_contract["payload"]["checks"]["raw_pair_prewhere"] is True
    assert sql_contract["payload"]["checks"]["block_replay_vwap"] is True
    trade_contract = next(check for check in report["checks"] if check["name"] == "trade replay sql contract")
    assert trade_contract["status"] == READY
    assert trade_contract["payload"]["checks"]["loader_pair_prewhere"] is True
    assert trade_contract["payload"]["checks"]["loader_tick_ordered"] is True
    assert trade_contract["payload"]["checks"]["canonical_fill_key_persisted"] is True


def test_backtest_core_gate_markdown_declares_excluded_domains() -> None:
    report = build_backtest_core_gate_report(PROJECT_ROOT)
    markdown = core_gate_to_markdown(report)

    assert "# Backtest Core Gate: ready" in markdown
    assert "paper/live order-state coverage" in markdown
    assert "real order API adapter readiness" in markdown


def test_backtest_core_gate_can_include_focused_pytest_check_with_runner() -> None:
    def command_runner(cmd, cwd):
        assert "pytest" in cmd
        assert cwd == PROJECT_ROOT
        return {"returncode": 0, "stdout": "1 passed", "stderr": ""}

    report = build_backtest_core_gate_report(PROJECT_ROOT, include_pytest=True, command_runner=command_runner)

    assert report["status"] == READY
    pytest_check = next(check for check in report["checks"] if check["name"] == "pytest backtest-core subset")
    assert pytest_check["status"] == READY
    assert pytest_check["payload"]["output_tail"] == "1 passed"
