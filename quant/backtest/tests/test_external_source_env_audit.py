from pathlib import Path

from quant.backtest.external_source_env_audit import (
    FAIL,
    READY,
    REVIEW,
    build_external_source_env_audit,
    external_source_env_audit_to_markdown,
)
from quant.backtest.external_source_fixture import build_fill_first_external_source_fixture


def test_external_source_env_audit_reports_review_for_empty_env() -> None:
    report = build_external_source_env_audit(env={})

    assert report["status"] == REVIEW
    assert report["ready_count"] == 0
    assert report["review_count"] == 4
    assert any("ORDER_STATE_INPUT" in issue for issue in report["issues"])
    assert report["sources"][0]["mode"] == "unconfigured"
    assert report["sources"][0]["missing_keys"] == ["ORDER_STATE_INPUT or ORDER_STATE_API_URL"]
    assert "Set ORDER_STATE_INPUT" in report["sources"][0]["next_action"]


def test_external_source_env_audit_accepts_fixture_env_file(tmp_path: Path) -> None:
    fixture = build_fill_first_external_source_fixture(tmp_path / "fixture")

    report = build_external_source_env_audit([Path(fixture["files"]["env_file"])], env={})

    assert report["status"] == READY
    assert report["ready_count"] == 4
    assert report["configured_imports"]["configured_count"] == 4
    assert {source["mode"] for source in report["sources"]} == {"file"}
    assert all(source["next_action"].startswith("Run configured import dry-run") for source in report["sources"])
    cost_source = next(source for source in report["sources"] if source["kind"] == "real_cost_events")
    assert cost_source["health_tracked"] is True


def test_external_source_env_audit_flags_placeholders_and_missing_auth() -> None:
    report = build_external_source_env_audit(
        env={
            "ORDER_STATE_API_URL": "https://replace-with-private-order-api.example/orders",
            "ORDER_STATE_AUTH_HEADER": "Authorization=REPLACE_WITH_PRIVATE_AUTH_VALUE",
            "COST_EVENTS_URL": "https://costs.internal.local/costs",
            "PLATFORM_INCIDENTS_INPUT": "/tmp/does-not-exist/platform_incidents.jsonl",
        }
    )

    assert report["status"] == REVIEW
    assert any("placeholder values" in issue for issue in report["issues"])
    assert any("COST_EVENTS_AUTH_HEADER is empty" in issue for issue in report["issues"])
    assert any("input file does not exist: PLATFORM_INCIDENTS_INPUT" in issue for issue in report["issues"])
    cost_source = next(source for source in report["sources"] if source["kind"] == "real_cost_events")
    assert cost_source["mode"] == "url"
    assert "COST_EVENTS_AUTH_HEADER" in cost_source["missing_keys"]
    assert "COST_EVENTS_STATE_KEY" in cost_source["missing_keys"]
    assert "Set COST_EVENTS_AUTH_HEADER" in cost_source["next_action"]


def test_external_source_env_audit_reports_mixed_mode_before_import(tmp_path: Path) -> None:
    input_path = tmp_path / "orders.jsonl"
    input_path.write_text("{}\n", encoding="utf-8")

    report = build_external_source_env_audit(
        env={
            "ORDER_STATE_INPUT": str(input_path),
            "ORDER_STATE_API_URL": "https://orders.internal.local/orders",
            "ORDER_STATE_AUTH_HEADER": "Authorization=secret",
            "ORDER_STATE_KEY": "orders-live",
        }
    )

    order_source = next(source for source in report["sources"] if source["kind"] == "real_order_state_events")
    assert order_source["status"] == REVIEW
    assert order_source["mode"] == "mixed"
    assert "Choose one input mode" in order_source["next_action"]


def test_external_source_env_audit_invalid_env_file_is_fail(tmp_path: Path) -> None:
    env_file = tmp_path / "bad.env"
    env_file.write_text("NOT_A_KEY_VALUE_LINE\n", encoding="utf-8")

    report = build_external_source_env_audit([env_file], env={})

    assert report["status"] == FAIL
    assert report["fail_count"] == 1
    assert "env file load failed" in report["issues"][0]


def test_external_source_env_audit_markdown_masks_auth_values() -> None:
    report = build_external_source_env_audit(
        env={
            "ORDER_STATE_API_URL": "https://orders.internal.local/orders",
            "ORDER_STATE_AUTH_HEADER": "Authorization=super-secret-token",
            "ORDER_STATE_KEY": "order-live",
            "COST_EVENTS_URL": "https://costs.internal.local/costs",
            "COST_EVENTS_AUTH_HEADER": "Authorization=cost-secret",
            "COST_EVENTS_STATE_KEY": "cost-live",
            "PLATFORM_INCIDENTS_URL": "https://incidents.internal.local/incidents",
            "PLATFORM_INCIDENTS_AUTH_HEADER": "Authorization=incident-secret",
            "PLATFORM_INCIDENTS_STATE_KEY": "incident-live",
            "EXTERNAL_SIGNAL_URL": "https://signals.internal.local/events",
            "EXTERNAL_SIGNAL_AUTH_HEADER": "Authorization=signal-secret",
            "EXTERNAL_SIGNAL_STATE_KEY": "signal-live",
        }
    )

    markdown = external_source_env_audit_to_markdown(report)

    assert report["status"] == READY
    assert "Mode" in markdown
    assert "Next action" in markdown
    assert "super-secret-token" not in markdown
    assert "cost-secret" not in markdown
    assert "incident-secret" not in markdown
    assert "signal-secret" not in markdown
    assert "order-live" in markdown
