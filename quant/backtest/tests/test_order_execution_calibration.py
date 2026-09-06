from quant.backtest.order_execution_calibration import (
    build_order_execution_calibration_report,
    order_execution_calibration_report_to_summary,
)


def sample_order() -> dict:
    return {
        "run_id": 42,
        "order_id": "guarded-paper-run-42-decision-10-enable-1",
        "run_market_slug": "demo-market",
        "run_token_side": "YES",
        "side": "BUY_YES",
        "role": "taker",
        "order_type": "marketable_limit",
        "requested_price": "0.53",
        "requested_size": "10",
        "status": "FILLED",
        "avg_fill_price": "0.53",
        "filled_size": "10",
        "filled_notional": "5.30",
        "fee_cost": "0.01",
        "rebate_cost": "0",
        "slippage_cost": "0",
        "latency_seconds": "1.2",
    }


def sample_fill_event() -> dict:
    return {
        "run_id": 42,
        "order_id": "guarded-paper-run-42-decision-10-enable-1",
        "external_order_id": "live-1",
        "market_slug": "demo-market",
        "token_side": "YES",
        "event_time": "2026-06-25T12:00:02Z",
        "event_type": "fill",
        "source": "external-order-adapter",
        "api_order_status": "FILLED",
        "payload": {
            "live_status": "FILLED",
            "live_fill_price": "0.53",
            "live_fill_size": "10",
            "live_slippage": "0",
            "live_fee": "0.01",
            "live_rebate": "0",
            "live_cash_delta": "-5.31",
            "live_position_delta": "10",
            "live_latency_seconds": "1.2",
        },
    }


def test_order_execution_calibration_builds_samples_from_adapter_response_events() -> None:
    report = build_order_execution_calibration_report(
        orders=[sample_order()],
        response_events=[sample_fill_event()],
    )

    assert report["status"] == "ready"
    assert report["orders_read"] == 1
    assert report["events_read"] == 1
    assert report["samples_built"] == 1
    assert report["calibration_report"]["trust_status"] == "ready"
    summary = order_execution_calibration_report_to_summary(report)
    assert summary["samples_built"] == 1
    assert summary["trust_status"] == "ready"


def test_order_execution_calibration_skips_open_events_by_default() -> None:
    event = sample_fill_event()
    event["event_type"] = "submit"
    event["api_order_status"] = "ACCEPTED"
    event["payload"] = {"live_status": "ACCEPTED"}

    report = build_order_execution_calibration_report(
        orders=[sample_order()],
        response_events=[event],
    )
    with_open = build_order_execution_calibration_report(
        orders=[sample_order()],
        response_events=[event],
        include_open_events=True,
    )

    assert report["status"] == "review"
    assert report["samples_built"] == 0
    assert with_open["samples_built"] == 1
