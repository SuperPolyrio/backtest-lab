from datetime import datetime, timezone

from quant.backtest.guarded_executor import build_guarded_executor_report
from quant.backtest.order_execution_adapter import (
    build_order_execution_adapter_report,
    build_submit_request_template,
    order_execution_adapter_report_to_markdown,
)
from quant.backtest.strategy_runner_guard import build_strategy_runner_plan


def enabled_row(**overrides):
    row = {
        "enable_id": 1,
        "decision_id": 10,
        "run_id": 20,
        "target_mode": "paper",
        "strategy_name": "fill_first_demo",
        "strategy_version": "v1",
        "market_slug": "demo-market",
        "token_side": "YES",
        "price_source": "orderfilled_block_close",
        "actual_execution_engine": "builtin",
        "enabled": True,
        "activation_allowed": True,
        "decision_verdict": "ready",
        "promotion_verdict": "ready",
        "paper_live_evidence_gate_status": "ready",
        "paper_live_paper_allowed": True,
        "paper_live_live_allowed": False,
        "activation_decision": {
            "decision_id": 10,
            "paper_live_evidence_gate_report": {
                "status": "ready",
                "paper_allowed": True,
                "live_allowed": False,
            },
        },
    }
    row.update(overrides)
    return row


def guarded_report():
    runner_plan = build_strategy_runner_plan([enabled_row()], target_mode="paper")
    return build_guarded_executor_report(
        runner_plan,
        now=datetime(2026, 6, 22, 1, 2, 3, tzinfo=timezone.utc),
    )


def test_build_submit_request_template_preserves_fill_first_contract() -> None:
    intent = guarded_report()["event_templates"][0]
    template = build_submit_request_template(
        intent,
        source="unit-adapter",
        submit_url="https://orders.example/submit",
        generated_at=datetime(2026, 6, 22, 1, 3, tzinfo=timezone.utc),
    )

    body = template["body"]
    assert body["client_order_id"] == "guarded-paper-run-20-decision-10-enable-1"
    assert body["run_id"] == 20
    assert body["decision_id"] == 10
    assert body["strategy_name"] == "fill_first_demo"
    assert body["market_slug"] == "demo-market"
    assert body["token_side"] == "YES"
    assert body["fill_first"] is True
    assert body["paper_live_evidence_gate_status"] == "ready"
    assert body["paper_live_paper_allowed"] is True
    assert body["paper_live_live_allowed"] is False
    assert body["paper_live_evidence_gate_report"]["paper_allowed"] is True
    assert body["lob_required"] is False


def test_order_execution_adapter_dry_run_does_not_call_transport() -> None:
    called = False

    def transport(url, body, headers, timeout):
        nonlocal called
        called = True
        return {"id": "live-1", "status": "accepted"}

    report = build_order_execution_adapter_report(
        guarded_report(),
        submit_url="https://orders.example/submit",
        dry_run=True,
        transport=transport,
    )

    assert called is False
    assert report["status"] == "review"
    assert report["adapter_status"] == "dry_run"
    assert report["request_count"] == 1
    assert report["response_event_count"] == 0
    assert report["adapter_contract"]["lob_required"] is False
    assert report["adapter_contract"]["requires_paper_live_evidence_gate"] is True
    assert report["gate_issue_count"] == 0


def test_order_execution_adapter_submits_with_fake_transport_and_normalizes_response() -> None:
    requests = []

    def transport(url, body, headers, timeout):
        requests.append((url, body, headers, timeout))
        return {
            "id": "live-1",
            "clientOrderId": body["client_order_id"],
            "marketSlug": body["market_slug"],
            "tokenSide": body["token_side"],
            "status": "accepted",
            "updatedAt": "2026-06-22T01:03:01Z",
        }

    report = build_order_execution_adapter_report(
        guarded_report(),
        submit_url="https://orders.example/submit",
        headers={"Authorization": "Bearer test"},
        source="unit-order-adapter",
        dry_run=False,
        transport=transport,
    )

    assert report["status"] == "ready"
    assert report["adapter_status"] == "submitted"
    assert len(requests) == 1
    assert requests[0][0] == "https://orders.example/submit"
    assert requests[0][2]["Authorization"] == "Bearer test"
    event = report["response_events"][0]
    assert event["source"] == "unit-order-adapter"
    assert event["event_type"] == "submit"
    assert event["order_id"] == "guarded-paper-run-20-decision-10-enable-1"
    assert event["external_order_id"] == "live-1"
    assert event["api_order_status"] == "ACCEPTED"
    assert event["payload"]["adapter_request"]["fill_first"] is True
    assert event["payload"]["adapter_request"]["paper_live_evidence_gate_status"] == "ready"


def test_order_execution_adapter_blocks_intent_without_paper_live_gate() -> None:
    called = False

    def transport(url, body, headers, timeout):
        nonlocal called
        called = True
        return {"id": "live-1", "status": "accepted"}

    report = guarded_report()
    event = dict(report["event_templates"][0])
    payload = dict(event["payload"])
    payload.pop("paper_live_evidence_gate_status")
    payload.pop("paper_live_paper_allowed")
    payload.pop("paper_live_live_allowed")
    payload.pop("paper_live_evidence_gate_report")
    item = dict(payload["runner_plan_item"])
    item.pop("paper_live_evidence_gate_status")
    item.pop("paper_live_paper_allowed")
    item.pop("paper_live_live_allowed")
    item.pop("paper_live_evidence_gate_report")
    payload["runner_plan_item"] = item
    event["payload"] = payload
    report["event_templates"] = [event]

    adapter_report = build_order_execution_adapter_report(
        report,
        submit_url="https://orders.example/submit",
        dry_run=False,
        transport=transport,
    )

    assert called is False
    assert adapter_report["status"] == "blocked"
    assert adapter_report["adapter_status"] == "fill_first_gate_blocked"
    assert adapter_report["gate_issue_count"] == 1
    assert "missing paper_live_evidence_gate_report" in adapter_report["gate_issues"][0]["reasons"]


def test_order_execution_adapter_requires_submit_url_when_not_dry_run() -> None:
    report = build_order_execution_adapter_report(
        guarded_report(),
        dry_run=False,
    )

    assert report["status"] == "blocked"
    assert report["adapter_status"] == "missing_submit_url"
    assert report["response_events"] == []


def test_order_execution_adapter_error_becomes_order_state_event() -> None:
    def transport(url, body, headers, timeout):
        raise RuntimeError("boom")

    report = build_order_execution_adapter_report(
        guarded_report(),
        submit_url="https://orders.example/submit",
        source="unit-order-adapter",
        dry_run=False,
        transport=transport,
    )

    assert report["status"] == "blocked"
    assert report["adapter_status"] == "submit_error"
    assert report["error_count"] == 1
    event = report["response_events"][0]
    assert event["event_type"] == "submit_error"
    assert event["api_order_status"] == "ADAPTER_ERROR"
    assert event["payload"]["error"] == "boom"


def test_order_execution_adapter_markdown_lists_response_events() -> None:
    def transport(url, body, headers, timeout):
        return {"id": "live-1", "clientOrderId": body["client_order_id"], "status": "accepted"}

    report = build_order_execution_adapter_report(
        guarded_report(),
        submit_url="https://orders.example/submit",
        dry_run=False,
        transport=transport,
    )
    markdown = order_execution_adapter_report_to_markdown(report)

    assert "Order Execution Adapter" in markdown
    assert "guarded-paper-run-20-decision-10-enable-1" in markdown
    assert "ACCEPTED" in markdown
