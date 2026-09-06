"""Deterministic fixture for guarded order execution and calibration."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from quant.backtest.guarded_executor import build_guarded_executor_report
from quant.backtest.order_execution_adapter import build_order_execution_adapter_report
from quant.backtest.order_execution_calibration import (
    build_order_execution_calibration_report,
    order_execution_calibration_report_to_summary,
)
from quant.backtest.strategy_runner_guard import build_strategy_runner_plan


READY = "ready"
REVIEW = "review"


def run_order_execution_fixture_pipeline() -> dict[str, Any]:
    """Exercise enabled state -> guarded intent -> adapter response -> calibration."""

    enable_rows = [_enabled_state_row()]
    runner_plan = build_strategy_runner_plan(enable_rows, target_mode="paper")
    guarded_report = build_guarded_executor_report(
        runner_plan,
        now=datetime(2026, 6, 25, 12, 0, 0, tzinfo=timezone.utc),
    )
    transport_calls: list[dict[str, Any]] = []

    def fake_transport(url: str, body: Mapping[str, Any], headers: Mapping[str, str], timeout: float) -> dict[str, Any]:
        transport_calls.append({"url": url, "body": dict(body), "headers": dict(headers), "timeout": timeout})
        return {
            "id": "fixture-live-order-1",
            "clientOrderId": body.get("client_order_id"),
            "marketSlug": body.get("market_slug"),
            "tokenSide": body.get("token_side"),
            "status": "filled",
            "updatedAt": "2026-06-25T12:00:02Z",
            "createdAt": "2026-06-25T12:00:00Z",
            "live_status": "FILLED",
            "live_fill_price": "0.5300000000",
            "live_fill_size": "10.0000000000",
            "live_slippage": "0.0000000000",
            "live_fee": "0.0100000000",
            "live_rebate": "0",
            "live_cash_delta": "-5.3100000000",
            "live_position_delta": "10.0000000000",
            "live_latency_seconds": "2",
        }

    adapter_report = build_order_execution_adapter_report(
        guarded_report,
        submit_url="http://127.0.0.1:9999/fixture-submit",
        source="fixture-order-execution-adapter",
        dry_run=False,
        transport=fake_transport,
        now=datetime(2026, 6, 25, 12, 0, 1, tzinfo=timezone.utc),
    )
    calibration = build_order_execution_calibration_report(
        orders=[_simulated_order()],
        response_events=adapter_report.get("response_events") or [],
        source="fixture-order-execution-calibration",
    )
    status = _pipeline_status(runner_plan, guarded_report, adapter_report, calibration)
    return {
        "schema_version": "fill_first_order_execution_fixture_v1",
        "status": status,
        "runner_status": runner_plan.get("runner_status"),
        "guarded_executor_status": guarded_report.get("executor_status"),
        "adapter_status": adapter_report.get("adapter_status"),
        "calibration_status": calibration.get("status"),
        "runnable_count": runner_plan.get("runnable_count"),
        "intent_count": guarded_report.get("planned_action_count"),
        "request_count": adapter_report.get("request_count"),
        "response_event_count": adapter_report.get("response_event_count"),
        "samples_built": calibration.get("samples_built"),
        "calibration_summary": order_execution_calibration_report_to_summary(calibration),
        "transport_call_count": len(transport_calls),
        "transport_calls": transport_calls,
    }


def order_execution_fixture_pipeline_to_markdown(report: Mapping[str, Any]) -> str:
    summary = report.get("calibration_summary") if isinstance(report.get("calibration_summary"), Mapping) else {}
    lines = [
        f"# Order Execution Fixture Pipeline: {report.get('status')}",
        "",
        f"- schema: {report.get('schema_version')}",
        f"- runner_status: {report.get('runner_status')}",
        f"- guarded_executor_status: {report.get('guarded_executor_status')}",
        f"- adapter_status: {report.get('adapter_status')}",
        f"- calibration_status: {report.get('calibration_status')}",
        f"- runnable_count: {report.get('runnable_count')}",
        f"- intent_count: {report.get('intent_count')}",
        f"- request_count: {report.get('request_count')}",
        f"- response_event_count: {report.get('response_event_count')}",
        f"- samples_built: {report.get('samples_built')}",
        f"- calibration_trust: {summary.get('trust_status')} - {summary.get('trust_reason')}",
    ]
    return "\n".join(lines)


def _pipeline_status(
    runner_plan: Mapping[str, Any],
    guarded_report: Mapping[str, Any],
    adapter_report: Mapping[str, Any],
    calibration: Mapping[str, Any],
) -> str:
    ready = (
        runner_plan.get("runner_status") == READY
        and guarded_report.get("status") == READY
        and adapter_report.get("status") == READY
        and calibration.get("status") == READY
        and int(calibration.get("samples_built") or 0) >= 1
    )
    return READY if ready else REVIEW


def _enabled_state_row() -> dict[str, Any]:
    return {
        "enable_id": 501,
        "decision_id": 101,
        "run_id": 990010,
        "target_mode": "paper",
        "strategy_name": "fixture_fill_first",
        "strategy_version": "v1",
        "market_slug": "fixture-market",
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
            "decision_id": 101,
            "paper_live_evidence_gate_report": {
                "status": "ready",
                "paper_allowed": True,
                "live_allowed": False,
            },
        },
    }


def _simulated_order() -> dict[str, Any]:
    return {
        "run_id": 990010,
        "order_id": "guarded-paper-run-990010-decision-101-enable-501",
        "run_market_slug": "fixture-market",
        "run_token_side": "YES",
        "side": "BUY_YES",
        "role": "taker",
        "order_type": "marketable_limit",
        "requested_price": "0.5300000000",
        "requested_size": "10.0000000000",
        "status": "FILLED",
        "avg_fill_price": "0.5300000000",
        "filled_size": "10.0000000000",
        "filled_notional": "5.3000000000",
        "fee_cost": "0.0100000000",
        "rebate_cost": "0",
        "slippage_cost": "0",
        "latency_seconds": "2",
    }
