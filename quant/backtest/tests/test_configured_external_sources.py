from pathlib import Path

from quant.backtest.configured_external_sources import (
    FAIL,
    READY,
    REVIEW,
    build_configured_external_source_plan,
    build_configured_external_source_report,
    configured_external_source_report_to_markdown,
    load_external_source_env_files,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_configured_external_source_plan_reports_review_without_sources() -> None:
    report = build_configured_external_source_report({}, project_root=PROJECT_ROOT, dry_run=True)

    assert report["status"] == REVIEW
    assert report["configured_count"] == 0
    assert {item["kind"] for item in report["items"]} == {
        "real_order_state_events",
        "real_cost_events",
        "platform_incidents",
        "external_signal_events",
    }


def test_configured_external_source_plan_uses_env_and_masks_headers() -> None:
    env = {
        "ORDER_STATE_API_URL": "https://private.example/orders",
        "ORDER_STATE_AUTH_HEADER": "Authorization=Bearer secret-value",
        "ORDER_STATE_SOURCE": "private-order-api",
        "ORDER_STATE_KEY": "order-api-live",
        "ORDER_STATE_SINCE_PARAM": "updated_after",
        "ORDER_STATE_CURSOR_PARAM": "cursor",
        "COST_EVENTS_INPUT": "/tmp/real_cost_events.jsonl",
        "COST_EVENTS_SOURCE": "wallet-ledger",
        "COST_EVENTS_STATE_KEY": "real-cost-events-live",
        "PLATFORM_INCIDENTS_URL": "https://private.example/incidents",
        "PLATFORM_INCIDENTS_AUTH_HEADER": "X-Api-Key=secret-value",
        "PLATFORM_INCIDENTS_SOURCE": "ops-notes",
        "PLATFORM_INCIDENTS_STATE_KEY": "platform-incidents-live",
        "EXTERNAL_SIGNAL_URL": "https://private.example/signals",
        "EXTERNAL_SIGNAL_AUTH_HEADER": "Authorization=Bearer signal-secret",
        "EXTERNAL_SIGNAL_SOURCE": "news-signals",
        "EXTERNAL_SIGNAL_STATE_KEY": "external-signals-live",
    }

    plan = build_configured_external_source_plan(env, project_root=PROJECT_ROOT, python_executable="python")
    report = build_configured_external_source_report(env, project_root=PROJECT_ROOT, dry_run=True)
    markdown = configured_external_source_report_to_markdown(report)

    assert report["status"] == READY
    assert report["configured_count"] == 4
    order_state = next(item for item in plan if item.kind == "real_order_state_events")
    assert "--state-key" in order_state.command
    assert "order-api-live" in order_state.command
    assert order_state.state_key == "order-api-live"
    assert "Bearer secret-value" not in markdown
    assert "Bearer signal-secret" not in markdown
    assert "X-Api-Key=secret-value" not in markdown
    assert "Authorization=***" in markdown
    assert "X-Api-Key=***" in markdown


def test_configured_order_state_file_source_uses_state_key() -> None:
    env = {
        "ORDER_STATE_INPUT": "/tmp/order_state.jsonl",
        "ORDER_STATE_SOURCE": "manual-order-state",
        "ORDER_STATE_RUN_ID": "123",
        "ORDER_STATE_KEY": "order-state-file-live",
    }

    plan = build_configured_external_source_plan(env, project_root=PROJECT_ROOT, python_executable="python")
    item = next(item for item in plan if item.kind == "real_order_state_events")

    assert item.configured is True
    assert item.script == "scripts/import_real_order_state_events.py"
    assert "--state-key" in item.command
    assert "order-state-file-live" in item.command
    assert item.state_key == "order-state-file-live"


def test_load_external_source_env_files_supports_dotenv_and_export(tmp_path: Path) -> None:
    env_file = tmp_path / "fill-first.env"
    env_file.write_text(
        "\n".join(
            [
                "# real order state",
                "ORDER_STATE_API_URL=https://private.example/orders",
                "export ORDER_STATE_AUTH_HEADER='Authorization=Bearer secret-value'",
                'ORDER_STATE_SOURCE="private-order-api"',
                "COST_EVENTS_INPUT=/tmp/real_cost_events.jsonl",
            ]
        ),
        encoding="utf-8",
    )

    env = load_external_source_env_files([env_file], base_env={})
    report = build_configured_external_source_report(env, project_root=PROJECT_ROOT, dry_run=True)
    markdown = configured_external_source_report_to_markdown(report)

    assert env["ORDER_STATE_AUTH_HEADER"] == "Authorization=Bearer secret-value"
    assert env["ORDER_STATE_SOURCE"] == "private-order-api"
    assert report["status"] == READY
    assert report["configured_count"] == 2
    assert "Bearer secret-value" not in markdown
    assert "Authorization=***" in markdown


def test_load_external_source_env_files_later_files_override(tmp_path: Path) -> None:
    first = tmp_path / "base.env"
    second = tmp_path / "override.env"
    first.write_text("COST_EVENTS_SOURCE=base\nCOST_EVENTS_INPUT=/tmp/base.jsonl\n", encoding="utf-8")
    second.write_text("COST_EVENTS_SOURCE=override\n", encoding="utf-8")

    env = load_external_source_env_files([first, second], base_env={"KEEP": "1"})

    assert env["KEEP"] == "1"
    assert env["COST_EVENTS_INPUT"] == "/tmp/base.jsonl"
    assert env["COST_EVENTS_SOURCE"] == "override"


def test_load_external_source_env_files_rejects_invalid_lines(tmp_path: Path) -> None:
    env_file = tmp_path / "bad.env"
    env_file.write_text("ORDER_STATE_API_URL\n", encoding="utf-8")

    try:
        load_external_source_env_files([env_file], base_env={})
    except ValueError as exc:
        assert "expected KEY=VALUE" in str(exc)
    else:  # pragma: no cover - assertion guard
        raise AssertionError("invalid env file line should fail")


def test_configured_external_source_report_executes_configured_commands() -> None:
    env = {
        "COST_EVENTS_INPUT": "/tmp/real_cost_events.jsonl",
        "COST_EVENTS_SOURCE": "wallet-ledger",
    }
    calls: list[tuple[str, ...]] = []

    def runner(command: tuple[str, ...]) -> dict:
        calls.append(command)
        return {"returncode": 0, "output_tail": "ok"}

    report = build_configured_external_source_report(
        env,
        project_root=PROJECT_ROOT,
        dry_run=False,
        command_runner=runner,
    )

    assert report["status"] == READY
    assert len(calls) == 1
    assert "scripts/import_real_backtest_cost_events.py" in calls[0][1]


def test_configured_external_signal_file_source_uses_state_key() -> None:
    env = {
        "EXTERNAL_SIGNAL_INPUT": "/tmp/external_signal_events.jsonl",
        "EXTERNAL_SIGNAL_SOURCE": "manual-signals",
        "EXTERNAL_SIGNAL_RUN_ID": "456",
        "EXTERNAL_SIGNAL_STATE_KEY": "external-signal-file-live",
    }

    plan = build_configured_external_source_plan(env, project_root=PROJECT_ROOT, python_executable="python")
    item = next(item for item in plan if item.kind == "external_signal_events")

    assert item.configured is True
    assert item.script == "scripts/import_external_signal_events.py"
    assert "--state-key" in item.command
    assert "external-signal-file-live" in item.command
    assert item.state_key == "external-signal-file-live"


def test_configured_external_source_report_fails_on_command_error() -> None:
    env = {"PLATFORM_INCIDENTS_INPUT": "/tmp/incidents.jsonl"}

    report = build_configured_external_source_report(
        env,
        project_root=PROJECT_ROOT,
        dry_run=False,
        command_runner=lambda command: {"returncode": 2, "output_tail": "bad input"},
    )

    assert report["status"] == FAIL
    item = next(item for item in report["items"] if item["configured"])
    assert item["status"] == FAIL
    assert item["output_tail"] == "bad input"
