"""Deterministic shadow/live calibration fixture for the fill-first quality gate."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from quant.backtest.calibration import build_calibration_report
from quant.backtest.calibration_samples import build_calibration_samples_from_order_events
from quant.backtest.shadow_live_plan import build_shadow_live_order_plan
from quant.backtest.shadow_live_validation import validate_shadow_live_order_events


def run_shadow_live_calibration_fixture() -> dict[str, Any]:
    """Exercise the export -> validate -> sample -> report path without external services."""

    inputs = _fixture_inputs()
    plan = build_shadow_live_order_plan(inputs, source="live-shadow-fixture")
    live_events = _filled_fixture_events(plan["event_templates"])
    validation = validate_shadow_live_order_events(live_events, require_cost_fields=True)
    samples = build_calibration_samples_from_order_events(
        inputs["orders"],
        live_events,
        source="fixture-shadow-live",
    )
    calibration_report = build_calibration_report(samples)
    passed = (
        plan.get("status") == "ready"
        and validation.get("status") == "ready"
        and calibration_report.get("sample_count") == 2
        and calibration_report.get("status_error_count") == 0
        and calibration_report.get("trust_status") == "ready"
    )
    return {
        "passed": passed,
        "plan_status": plan.get("status"),
        "plan_order_count": plan.get("order_count"),
        "validation_status": validation.get("status"),
        "validation_errors": validation.get("error_count"),
        "validation_warnings": validation.get("warning_count"),
        "calibration_sample_count": calibration_report.get("sample_count"),
        "calibration_status_error_count": calibration_report.get("status_error_count"),
        "calibration_trust_status": calibration_report.get("trust_status"),
        "calibration_report": calibration_report,
    }


def _fixture_inputs() -> dict[str, Any]:
    return {
        "run": {
            "run_id": 990001,
            "market_slug": "fixture-market",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "backtest_engine": "builtin",
            "from_block": 100,
            "to_block": 120,
            "meta": {"token_id": "token-yes", "parameter_fingerprint": "fixture-fp"},
        },
        "parameters": {
            "execution_price_mode": "ORDERFILLED_LIMIT_REPLAY",
            "execution_profile": "realistic",
            "order_role": "taker",
            "latency_seconds": "1.2",
            "position_size": "10",
        },
        "orders": [
            {
                "run_id": 990001,
                "order_id": "FX-1",
                "signal_index": 1,
                "signal_x": 101,
                "submit_x": 102,
                "decision_price": "0.5200000000",
                "requested_price": "0.5300000000",
                "requested_size": "10.0000000000",
                "requested_notional": "5.3000000000",
                "filled_size": "10.0000000000",
                "filled_notional": "5.3000000000",
                "avg_fill_price": "0.5300000000",
                "fee_cost": "0.0100000000",
                "rebate_cost": "0",
                "slippage_cost": "0.0000000000",
                "latency_seconds": "1.2",
                "side": "BUY_YES",
                "role": "taker",
                "order_type": "marketable_limit",
                "status": "FILLED",
                "execution_source": "orderfilled_limit_replay",
                "meta": {"external_order_id": "LIVE-FX-1", "token_id": "token-yes"},
            },
            {
                "run_id": 990001,
                "order_id": "FX-2",
                "signal_index": 2,
                "signal_x": 110,
                "submit_x": 111,
                "decision_price": "0.4900000000",
                "requested_price": "0.4900000000",
                "requested_size": "10.0000000000",
                "requested_notional": "4.9000000000",
                "filled_size": "0",
                "filled_notional": "0",
                "avg_fill_price": None,
                "fee_cost": "0",
                "rebate_cost": "0",
                "slippage_cost": "0",
                "latency_seconds": "1.2",
                "side": "BUY_YES",
                "role": "maker",
                "order_type": "post_only_limit",
                "status": "REJECTED",
                "no_fill_reason": "post_only_rejected",
                "execution_source": "orderfilled_limit_replay",
                "meta": {"external_order_id": "LIVE-FX-2", "token_id": "token-yes"},
            },
        ],
    }


def _filled_fixture_events(templates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events = [deepcopy(template) for template in templates]
    by_order = {event.get("order_id"): event for event in events}

    filled = by_order["FX-1"]
    filled.update(
        {
            "external_order_id": "LIVE-FX-1",
            "event_time": "2026-06-25T12:00:02Z",
            "api_order_status": "filled",
            "accepted_at": "2026-06-25T12:00:02Z",
            "submit_at": "2026-06-25T12:00:00.800000Z",
        }
    )
    filled["payload"].update(
        {
            "live_status": "FILLED",
            "live_fill_price": "0.5300000000",
            "live_fill_size": "10.0000000000",
            "live_slippage": "0.0000000000",
            "live_fee": "0.0100000000",
            "live_rebate": "0",
            "live_cash_delta": "-5.3100000000",
            "live_position_delta": "10.0000000000",
            "live_latency_seconds": "1.2",
        }
    )

    rejected = by_order["FX-2"]
    rejected.update(
        {
            "external_order_id": "LIVE-FX-2",
            "event_time": "2026-06-25T12:01:02Z",
            "api_order_status": "rejected",
            "accepted_at": "2026-06-25T12:01:02Z",
            "submit_at": "2026-06-25T12:01:00.800000Z",
        }
    )
    rejected["payload"].update(
        {
            "live_status": "REJECTED",
            "live_fee": "0",
            "live_rebate": "0",
            "live_cash_delta": "0",
            "live_position_delta": "0",
            "live_latency_seconds": "1.2",
        }
    )
    return events
