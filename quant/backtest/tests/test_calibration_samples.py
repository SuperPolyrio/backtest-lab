from __future__ import annotations

from quant.backtest.calibration_samples import build_calibration_sample, build_calibration_samples_from_order_events
from quant.backtest.calibration_report import build_periodic_calibration_report


def test_build_calibration_sample_from_sim_order_and_live_fill_event() -> None:
    order = {
        "run_id": 42,
        "order_id": "sim-1",
        "run_market_slug": "market-a",
        "run_token_side": "YES",
        "side": "BUY",
        "role": "maker",
        "order_type": "post_only_limit",
        "requested_price": "0.200",
        "requested_size": "100",
        "status": "FILLED",
        "avg_fill_price": "0.200",
        "filled_size": "100",
        "filled_notional": "20",
        "fee_cost": "0.01",
        "rebate_cost": "0.02",
        "slippage_cost": "0.001",
        "latency_seconds": "1",
        "meta": {
            "external_order_id": "live-1",
            "liquidity_bucket": "high",
            "market_category": "sports",
            "time_to_expiry_seconds": "3600",
        },
    }
    event = {
        "event_id": 9,
        "run_id": 42,
        "order_id": "sim-1",
        "external_order_id": "live-1",
        "event_time": "2026-06-25T01:00:02Z",
        "event_type": "fill",
        "source": "order-stream",
        "chain_order_status": "filled",
        "submit_at": "2026-06-25T01:00:00Z",
        "accepted_at": "2026-06-25T01:00:02Z",
        "payload": {
            "avgFillPrice": "0.205",
            "filledSize": "80",
            "cashDelta": "-16.4",
            "positionDelta": "80",
            "fee": "0.02",
            "rebate": "0.01",
            "slippage": "0.003",
            "volatilityBucket": "medium",
        },
    }

    sample = build_calibration_sample(order, event, source="auto-test")

    assert sample["run_id"] == 42
    assert sample["sample_id"] == "42|sim-1|live-1|9|fill|2026-06-25T01:00:02Z"
    assert sample["source"] == "auto-test"
    assert sample["market_slug"] == "market-a"
    assert sample["token_side"] == "YES"
    assert sample["simulated_status"] == "FILLED"
    assert sample["live_status"] == "FILLED"
    assert str(sample["price_error"]) == "0.0050000000"
    assert str(sample["size_error"]) == "20.0000000000"
    assert str(sample["fee_error"]) == "0.0100000000"
    assert str(sample["rebate_error"]) == "0.0100000000"
    assert str(sample["cash_error"]) == "3.5900000000"
    assert str(sample["position_error"]) == "20.0000000000"
    assert str(sample["latency_error_seconds"]) == "1.0000000000"
    assert sample["liquidity_bucket"] == "high"
    assert sample["volatility_bucket"] == "medium"
    assert sample["time_to_expiry_bucket"] == "lt_1d"
    assert sample["payload"]["context"]["market_category"] == "sports"

    report = build_periodic_calibration_report([sample], bucket_fields=("market_category", "liquidity_bucket", "volatility_bucket", "time_to_expiry_bucket"))
    assert report["buckets"]["market_category"][0]["bucket"] == "sports"
    assert report["buckets"]["time_to_expiry_bucket"][0]["bucket"] == "lt_1d"


def test_build_calibration_sample_derives_missing_context_buckets() -> None:
    order = {
        "run_id": 43,
        "order_id": "sim-2",
        "run_market_slug": "market-b",
        "run_token_side": "YES",
        "side": "BUY",
        "role": "taker",
        "order_type": "marketable_limit",
        "requested_price": "0.300",
        "requested_size": "50",
        "status": "FILLED",
        "avg_fill_price": "0.300",
        "filled_size": "50",
        "filled_notional": "15",
        "available_notional": "1500",
        "snapshot_drift": "0.025",
        "time_to_expiry_days": "10",
        "latency_seconds": "0.5",
        "meta": {"external_order_id": "live-2"},
    }
    event = {
        "event_id": 10,
        "run_id": 43,
        "order_id": "sim-2",
        "external_order_id": "live-2",
        "event_type": "fill",
        "event_time": "2026-06-25T01:00:04Z",
        "payload": {
            "live_fill_price": "0.301",
            "live_fill_size": "50",
            "live_latency_seconds": "0.75",
        },
    }

    sample = build_calibration_sample(order, event)

    assert sample["liquidity_bucket"] == "medium"
    assert sample["volatility_bucket"] == "medium"
    assert sample["time_to_expiry_bucket"] == "lt_30d"
    assert sample["payload"]["context"]["liquidity_bucket"] == "medium"


def test_build_calibration_samples_skips_open_events_by_default() -> None:
    order = {
        "run_id": 1,
        "order_id": "sim-open",
        "status": "NO_FILL",
        "side": "BUY",
        "requested_size": "10",
        "filled_size": "0",
        "filled_notional": "0",
        "meta": {"external_order_id": "live-open"},
    }
    event = {
        "run_id": 1,
        "order_id": "sim-open",
        "external_order_id": "live-open",
        "event_type": "submit",
        "submit_status": "accepted",
        "event_time": "2026-06-25T01:00:00Z",
    }

    assert build_calibration_samples_from_order_events([order], [event]) == []
    assert len(build_calibration_samples_from_order_events([order], [event], include_open_events=True)) == 1
