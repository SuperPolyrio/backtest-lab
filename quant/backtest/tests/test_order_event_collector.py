from __future__ import annotations

from quant.backtest.order_event_collector import events_from_order_payload, iter_order_payloads


def test_iter_order_payloads_accepts_common_api_shapes() -> None:
    assert len(iter_order_payloads({"orders": [{"id": "a"}, {"id": "b"}]})) == 2
    assert len(iter_order_payloads({"data": [{"id": "a"}]})) == 1
    assert len(iter_order_payloads([{"id": "a"}])) == 1
    assert len(iter_order_payloads({"id": "single"})) == 1


def test_events_from_order_payload_normalizes_filled_order() -> None:
    events = events_from_order_payload(
        {
            "orders": [
                {
                    "id": "live-1",
                    "clientOrderId": "sim-1",
                    "marketSlug": "market-a",
                    "assetId": "123",
                    "status": "filled",
                    "updatedAt": "2026-06-25T01:02:00Z",
                    "createdAt": "2026-06-25T01:00:00Z",
                    "avgFillPrice": "0.51",
                    "filledSize": "10",
                }
            ]
        },
        source="unit-api",
        run_id=7,
    )

    assert len(events) == 1
    event = events[0]
    assert event["run_id"] == 7
    assert event["source"] == "unit-api"
    assert event["event_type"] == "fill"
    assert event["order_id"] == "sim-1"
    assert event["external_order_id"] == "live-1"
    assert event["market_slug"] == "market-a"
    assert event["token_id"] == "123"
    assert event["api_order_status"] == "FILLED"
    assert event["submit_status"] == "accepted"
    assert event["accepted_status"] == "accepted"
    assert event["payload"]["avgFillPrice"] == "0.51"


def test_events_from_order_payload_normalizes_cancelled_order() -> None:
    event = events_from_order_payload(
        {"id": "live-2", "orderId": "sim-2", "state": "cancelled", "time": "2026-06-25T01:03:00Z"},
        source="unit-api",
    )[0]

    assert event["event_type"] == "cancel"
    assert event["api_order_status"] == "CANCELED"
    assert event["cancel_status"] == "accepted"
