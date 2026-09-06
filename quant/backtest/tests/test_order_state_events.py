from __future__ import annotations

from quant.backtest.backtest_engine import build_fill_quality_report
from quant.backtest.order_state import attach_real_order_state_events, normalize_order_state_event


def test_normalize_order_state_event_accepts_camel_case_aliases() -> None:
    event = normalize_order_state_event(
        {
            "runId": 7,
            "orderId": "order-1",
            "externalOrderId": "clob-1",
            "eventTime": "2026-06-22T12:00:01Z",
            "submitStatus": "accepted",
            "acceptedAt": "2026-06-22T12:00:02Z",
        },
        source="clob-api",
    )

    assert event["run_id"] == 7
    assert event["order_id"] == "order-1"
    assert event["external_order_id"] == "clob-1"
    assert event["event_time"] == "2026-06-22T12:00:01Z"
    assert event["source"] == "clob-api"
    assert event["submit_status"] == "accepted"
    assert event["accepted_at"] == "2026-06-22T12:00:02Z"
    assert event["payload"]["externalOrderId"] == "clob-1"


def test_attach_real_order_state_events_updates_order_meta_and_fill_quality() -> None:
    orders = [
        {
            "order_id": "order-1",
            "signal_index": 1,
            "status": "FILLED",
            "side": "BUY",
            "role": "maker",
            "order_type": "post_only_limit",
            "requested_size": "100",
            "requested_notional": "19.7",
            "filled_size": "100",
            "filled_notional": "19.7",
            "unfilled_size": "0",
            "avg_fill_price": "0.197",
            "fill_pct": "100",
            "fee_cost": "0",
            "rebate": "0",
            "slippage_cost": "0",
            "execution_cost": "0",
            "latency_blocks": 1,
            "latency_seconds": "1",
            "execution_source": "orderfilled_limit_replay_raw",
            "meta": {"external_order_id": "clob-1"},
        },
        {
            "order_id": "order-2",
            "signal_index": 2,
            "status": "NO_FILL",
            "side": "BUY",
            "role": "maker",
            "order_type": "post_only_limit",
            "requested_size": "100",
            "requested_notional": "20",
            "filled_size": "0",
            "filled_notional": "0",
            "unfilled_size": "100",
            "avg_fill_price": None,
            "fill_pct": "0",
            "fee_cost": "0",
            "rebate": "0",
            "slippage_cost": "0",
            "execution_cost": "0",
            "latency_blocks": 1,
            "latency_seconds": "1",
            "execution_source": "orderfilled_limit_replay_raw",
            "no_fill_reason": "cancel_race",
            "meta": {"external_order_id": "clob-2"},
        },
    ]
    events = [
        {
            "external_order_id": "clob-1",
            "event_time": "2026-06-22T12:00:01Z",
            "event_type": "submit",
            "source": "clob-api",
            "submit_status": "accepted",
            "api_order_status": "open",
            "submit_at": "2026-06-22T12:00:00Z",
            "accepted_at": "2026-06-22T12:00:01.500Z",
        },
        {
            "external_order_id": "clob-1",
            "event_time": "2026-06-22T12:00:03Z",
            "event_type": "chain",
            "source": "order-stream",
            "chain_order_status": "filled",
        },
        {
            "external_order_id": "clob-2",
            "event_time": "2026-06-22T12:10:02Z",
            "event_type": "cancel",
            "source": "clob-api",
            "cancel_status": "failed",
            "submit_status": "rejected",
            "cancel_submitted_at": "2026-06-22T12:10:00Z",
            "cancel_accepted_at": "2026-06-22T12:10:04Z",
            "payload": {"error": "cancel race"},
        },
    ]

    attached = attach_real_order_state_events(orders, events)
    report = build_fill_quality_report(
        orders,
        data_quality_report={"real_order_state": {"event_count": 4, "attached_order_count": attached}},
    )

    assert attached == 2
    assert len(orders[0]["meta"]["real_order_state_events"]) == 2
    assert orders[0]["meta"]["submit_status"] == "accepted"
    assert orders[0]["meta"]["chain_order_status"] == "filled"
    assert orders[1]["meta"]["cancel_status"] == "failed"
    assert report["real_order_state_observed_count"] == 2
    assert report["real_order_state_counts"]["submit:accepted"] == 1
    assert report["real_order_state_counts"]["submit:rejected"] == 1
    assert report["real_order_state_counts"]["cancel:failed"] == 1
    assert report["real_order_state_flags"]["api_status_lagging_after_chain_fill"] == 1
    assert report["real_order_state_flags"]["cancel_failed"] == 1
    assert report["avg_order_submit_accept_latency_seconds"] == "1.5"
    assert report["avg_cancel_accept_latency_seconds"] == "4"
    assert report["environment_flags"]["unmatched_real_order_state_events"] == 2
