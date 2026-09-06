from pathlib import Path

from quant.backtest.configured_external_sources import load_external_source_env_files, build_configured_external_source_report
from quant.backtest.external_source_discovery import discover_external_source_files, preview_external_source_import
from quant.backtest.external_source_fixture import (
    build_fill_first_external_source_fixture,
    build_fill_first_external_source_fixture_from_plan,
    fill_first_external_source_fixture_to_markdown,
)
from quant.backtest.shadow_live_validation import load_shadow_live_event_rows, validate_shadow_live_order_events


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_external_source_fixture_writes_ready_bundle(tmp_path: Path) -> None:
    report = build_fill_first_external_source_fixture(tmp_path, run_id=123, source_prefix="test-fixture")

    assert report["status"] == "ready"
    assert report["event_counts"] == {
        "order_state": 3,
        "cost_events": 2,
        "platform_incidents": 1,
        "external_signals": 2,
    }
    for path in report["files"].values():
        assert Path(path).exists()
    assert report["validation"]["status"] == "ready"
    assert report["validation"]["calibration_ready_count"] == 3
    assert report["discovery"]["status"] == "ready"
    assert report["configured_imports"]["status"] == "ready"


def test_external_source_fixture_is_discoverable_and_previewable(tmp_path: Path) -> None:
    build_fill_first_external_source_fixture(tmp_path, run_id=124)

    candidates = discover_external_source_files([tmp_path])
    report = preview_external_source_import(candidates, base=tmp_path)

    assert {candidate.kind for candidate in candidates} == {
        "real_order_state_events",
        "real_cost_events",
        "platform_incidents",
        "external_signal_events",
    }
    assert report["status"] == "ready"
    assert report["file_count"] == 4
    assert report["records_read"] == 8


def test_external_source_fixture_env_file_builds_configured_import_plan(tmp_path: Path) -> None:
    fixture = build_fill_first_external_source_fixture(tmp_path, run_id=125)
    env = load_external_source_env_files([fixture["files"]["env_file"]], base_env={})

    report = build_configured_external_source_report(env, project_root=PROJECT_ROOT, dry_run=True)

    assert env["ORDER_STATE_KEY"].endswith("-order-state-events")
    assert env["EXTERNAL_SIGNAL_STATE_KEY"].endswith("-external-signals")
    assert report["status"] == "ready"
    assert report["configured_count"] == 4
    assert all(item["status"] == "ready" for item in report["items"])
    order_state = next(item for item in report["items"] if item["kind"] == "real_order_state_events")
    assert order_state["state_key"] == env["ORDER_STATE_KEY"]
    external_signal = next(item for item in report["items"] if item["kind"] == "external_signal_events")
    assert external_signal["state_key"] == env["EXTERNAL_SIGNAL_STATE_KEY"]


def test_external_source_fixture_order_state_passes_strict_validation(tmp_path: Path) -> None:
    fixture = build_fill_first_external_source_fixture(tmp_path, run_id=126)
    rows = load_shadow_live_event_rows(Path(fixture["files"]["order_state"]))

    report = validate_shadow_live_order_events(rows, require_cost_fields=True)

    assert report["status"] == "ready"
    assert report["filled_count"] == 2
    assert report["no_fill_count"] == 1
    assert report["error_count"] == 0


def test_external_source_fixture_markdown_contains_dry_run_command(tmp_path: Path) -> None:
    report = build_fill_first_external_source_fixture(tmp_path, run_id=127)
    markdown = fill_first_external_source_fixture_to_markdown(report)

    assert "Fill-first External Source Fixture: ready" in markdown
    assert "scripts/run_configured_external_source_imports.py" in markdown
    assert "--env-file" in markdown


def test_run_specific_external_source_fixture_uses_plan_order_identity(tmp_path: Path) -> None:
    plan = {
        "status": "ready",
        "run": {
            "run_id": 321,
            "market_slug": "fixture-world-cup-winner",
            "token_side": "YES",
            "from_block": 88000000,
            "to_block": 88000010,
        },
        "orders": [
            {
                "order_id": "bt-321-001",
                "market_slug": "fixture-world-cup-winner",
                "token_id": "fixture-token-france",
                "token_side": "YES",
                "side": "BUY",
                "role": "TAKER",
                "simulated_status": "FILLED",
                "signal_x": 88000001,
                "submit_x": 88000002,
                "requested_price": "0.20",
                "requested_size": "10",
                "actual_fill_size": "10",
                "actual_fill_notional": "2.05",
                "avg_fill_price": "0.205",
                "fee_cost": "0.01",
                "rebate_cost": "0",
                "slippage_cost": "0.05",
                "latency_seconds": "3",
                "event_template": {
                    "run_id": 321,
                    "order_id": "bt-321-001",
                    "market_slug": "fixture-world-cup-winner",
                    "token_id": "fixture-token-france",
                    "token_side": "YES",
                    "payload": {},
                },
            },
            {
                "order_id": "bt-321-002",
                "market_slug": "fixture-world-cup-winner",
                "token_id": "fixture-token-spain",
                "token_side": "YES",
                "side": "BUY",
                "role": "MAKER",
                "simulated_status": "REJECTED",
                "signal_x": 88000003,
                "submit_x": 88000004,
                "requested_price": "0.14",
                "requested_size": "8",
                "filled_size": "0",
                "filled_notional": "0",
                "no_fill_reason": "no_trade_through",
                "event_template": {
                    "run_id": 321,
                    "order_id": "bt-321-002",
                    "market_slug": "fixture-world-cup-winner",
                    "token_id": "fixture-token-spain",
                    "token_side": "YES",
                    "payload": {},
                },
            },
            {
                "order_id": "bt-321-003",
                "market_slug": "fixture-world-cup-winner",
                "token_id": "fixture-token-france",
                "token_side": "YES",
                "side": "BUY",
                "role": "SETTLEMENT",
                "order_type": "SETTLEMENT",
                "execution_source": "settlement_payoff",
                "simulated_status": "FILLED",
                "requested_price": "1",
                "requested_size": "10",
                "actual_fill_size": "10",
                "actual_fill_notional": "10",
                "event_template": {
                    "run_id": 321,
                    "order_id": "bt-321-003",
                    "market_slug": "fixture-world-cup-winner",
                    "token_id": "fixture-token-france",
                    "token_side": "YES",
                    "payload": {},
                },
            },
        ],
    }

    report = build_fill_first_external_source_fixture_from_plan(tmp_path, plan, source_prefix="run-fixture")

    assert report["status"] == "ready"
    assert report["schema_version"] == "fill_first_external_source_run_plan_fixture_v1"
    assert report["run_id"] == 321
    assert report["event_counts"]["order_state"] == 2
    assert report["event_counts"]["cost_events"] == 1
    assert report["event_counts"]["external_signals"] == 2
    rows = load_shadow_live_event_rows(Path(report["files"]["order_state"]))
    assert [row["order_id"] for row in rows] == ["bt-321-001", "bt-321-002"]
    assert rows[0]["payload"]["live_fill_price"] == "0.205"
    assert rows[0]["payload"]["live_cash_delta"] == "-2.06"
    assert rows[0]["payload"]["live_slippage"] == "0.05"
    assert rows[1]["payload"]["live_status"] == "REJECTED"
    assert report["validation"]["status"] == "ready"
    assert report["validation"]["calibration_ready_count"] == 2
