import json

from quant.backtest.shadow_live_validation import (
    shadow_live_validation_to_markdown,
    validate_shadow_live_order_events,
)


def filled_event() -> dict:
    return {
        "run_id": 42,
        "order_id": "O-1",
        "external_order_id": "live-1",
        "event_time": "2026-06-25T12:00:00Z",
        "source": "live-shadow",
        "api_order_status": "filled",
        "payload": {
            "live_fill_price": "0.531",
            "live_fill_size": "6",
            "live_fee": "0.01",
            "live_rebate": "0",
            "live_cash_delta": "-3.196",
            "live_position_delta": "6",
            "live_latency_seconds": "1.2",
        },
    }


def test_validate_ready_filled_event() -> None:
    report = validate_shadow_live_order_events([filled_event()])

    assert report["status"] == "ready"
    assert report["calibration_ready_count"] == 1
    assert report["filled_count"] == 1
    assert report["error_count"] == 0


def test_validate_empty_template_fails() -> None:
    event = filled_event()
    event["event_time"] = ""
    event["api_order_status"] = ""
    event["payload"] = {"simulated_status": "FILLED"}

    report = validate_shadow_live_order_events([event])

    assert report["status"] == "fail"
    assert "missing_event_time" in report["checks"][0]["errors"]
    assert "missing_live_status" in report["checks"][0]["errors"]


def test_validate_no_fill_event_is_calibration_ready() -> None:
    event = filled_event()
    event["external_order_id"] = ""
    event["api_order_status"] = "rejected"
    event["payload"] = {"live_status": "rejected"}

    report = validate_shadow_live_order_events([event])

    assert report["status"] == "ready"
    assert report["no_fill_count"] == 1
    assert report["calibration_ready_count"] == 1


def test_validate_filled_event_without_costs_is_review_or_fail_when_required() -> None:
    event = filled_event()
    event["payload"] = {"live_fill_price": "0.531", "live_fill_size": "6"}

    soft = validate_shadow_live_order_events([event])
    strict = validate_shadow_live_order_events([event], require_cost_fields=True)

    assert soft["status"] == "review"
    assert "missing_live_fee" in soft["checks"][0]["warnings"]
    assert strict["status"] == "fail"
    assert "missing_live_fee" in strict["checks"][0]["errors"]


def test_validation_markdown_and_json_are_serializable() -> None:
    report = validate_shadow_live_order_events([filled_event()])
    markdown = shadow_live_validation_to_markdown(report)

    assert "Shadow/Live Event Validation" in markdown
    assert "live-1" in markdown
    json.dumps(report, ensure_ascii=False)
