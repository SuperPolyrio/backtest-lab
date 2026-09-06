from quant.backtest.order_execution_safety import (
    LIVE_CONFIRM_TOKEN,
    build_order_execution_env_audit,
    build_order_execution_run_safety_report,
    order_execution_env_audit_to_markdown,
)


def test_order_execution_env_audit_reviews_unconfigured_env() -> None:
    report = build_order_execution_env_audit({})

    assert report["status"] == "review"
    assert report["configured"] is False
    assert "ORDER_EXECUTION_SUBMIT_URL" in report["missing_keys"]


def test_order_execution_env_audit_requires_auth_for_non_local_url() -> None:
    report = build_order_execution_env_audit({
        "ORDER_EXECUTION_TARGET_MODE": "paper",
        "ORDER_EXECUTION_SUBMIT_URL": "https://orders.example/submit",
    })

    assert report["status"] == "review"
    assert any("AUTH_HEADER" in issue for issue in report["issues"])


def test_order_execution_env_audit_accepts_local_paper_without_auth() -> None:
    report = build_order_execution_env_audit({
        "ORDER_EXECUTION_TARGET_MODE": "paper",
        "ORDER_EXECUTION_SUBMIT_URL": "http://127.0.0.1:9999/orders",
    })

    assert report["status"] == "ready"
    assert report["auth"] == "empty"


def test_order_execution_env_audit_requires_live_confirmation() -> None:
    report = build_order_execution_env_audit({
        "ORDER_EXECUTION_TARGET_MODE": "live",
        "ORDER_EXECUTION_SUBMIT_URL": "https://orders.example/submit",
        "ORDER_EXECUTION_AUTH_HEADER": "Authorization=secret",
    })

    assert report["status"] == "review"
    assert "ORDER_EXECUTION_LIVE_CONFIRM" in report["missing_keys"]


def test_order_execution_env_audit_accepts_live_confirmation() -> None:
    report = build_order_execution_env_audit({
        "ORDER_EXECUTION_TARGET_MODE": "live",
        "ORDER_EXECUTION_SUBMIT_URL": "https://orders.internal.company/submit",
        "ORDER_EXECUTION_AUTH_HEADER": "Authorization=secret",
        "ORDER_EXECUTION_LIVE_CONFIRM": LIVE_CONFIRM_TOKEN,
    })

    assert report["status"] == "ready"
    assert report["live_confirmed"] is True


def test_order_execution_run_safety_blocks_execute_without_record_events() -> None:
    report = build_order_execution_run_safety_report(
        target_mode="paper",
        execute=True,
        record_events=False,
        submit_url="http://127.0.0.1:9999/orders",
    )

    assert report["status"] == "blocked"
    assert any("--record-events" in issue for issue in report["issues"])


def test_order_execution_run_safety_blocks_live_without_confirmation() -> None:
    report = build_order_execution_run_safety_report(
        target_mode="live",
        execute=True,
        record_events=True,
        submit_url="https://orders.example/submit",
        headers={"Authorization": "secret"},
    )

    assert report["status"] == "blocked"
    assert any("--live-confirm" in issue for issue in report["issues"])


def test_order_execution_run_safety_allows_confirmed_live_execution() -> None:
    report = build_order_execution_run_safety_report(
        target_mode="live",
        execute=True,
        record_events=True,
        submit_url="https://orders.internal.company/submit",
        headers={"Authorization": "secret"},
        live_confirm=LIVE_CONFIRM_TOKEN,
    )

    assert report["status"] == "ready"
    assert report["safety_status"] == "execute_allowed"


def test_order_execution_env_audit_markdown_redacts_sensitive_url() -> None:
    report = build_order_execution_env_audit({
        "ORDER_EXECUTION_SUBMIT_URL": "https://user:pass@orders.example/submit",
        "ORDER_EXECUTION_AUTH_HEADER": "Authorization=secret",
    })
    markdown = order_execution_env_audit_to_markdown(report)

    assert "user:pass" not in markdown
    assert "***@orders.example" in markdown
