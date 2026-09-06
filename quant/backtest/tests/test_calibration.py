from __future__ import annotations

from quant.backtest.calibration import build_calibration_report, calibration_trust, empty_calibration_report, normalize_calibration_order


def test_calibration_report_detects_status_and_latency_errors() -> None:
    rows = [
        normalize_calibration_order(
            {
                "sample_id": "matched",
                "role": "maker",
                "side": "BUY",
                "simulated_status": "FILLED",
                "live_status": "FILLED",
                "simulated_fill_price": "0.198",
                "live_fill_price": "0.199",
                "simulated_fill_size": "100",
                "live_fill_size": "100",
                "simulated_slippage": "0.001",
                "live_slippage": "0.002",
                "simulated_fee": "0",
                "live_fee": "0",
                "simulated_rebate": "0.001",
                "live_rebate": "0.001",
                "simulated_cash_delta": "-19.8",
                "live_cash_delta": "-19.8",
                "simulated_position_delta": "100",
                "live_position_delta": "100",
                "simulated_pnl": "1.25",
                "live_pnl": "1.20",
                "simulated_latency_seconds": "1",
                "live_latency_seconds": "1.5",
                "liquidity_bucket": "medium",
            }
        ),
        normalize_calibration_order(
            {
                "sample_id": "status-mismatch",
                "role": "taker",
                "side": "BUY",
                "simulated_status": "FILLED",
                "live_status": "NO_FILL",
                "simulated_fill_price": "0.60",
                "live_fill_price": None,
                "simulated_fill_size": "10",
                "live_fill_size": "0",
                "simulated_latency_seconds": "0.25",
                "live_latency_seconds": "4.5",
                "volatility_bucket": "high",
            }
        ),
    ]

    report = build_calibration_report(rows)

    assert report["sample_count"] == 2
    assert report["status_error_count"] == 1
    assert report["status_error_rate"] == "50"
    assert report["verdict_counts"]["matched"] == 1
    assert report["verdict_counts"]["status_mismatch"] == 1
    assert report["role_counts"]["maker"] == 1
    assert report["role_counts"]["taker"] == 1
    assert report["liquidity_bucket_counts"]["medium"] == 1
    assert report["volatility_bucket_counts"]["high"] == 1
    assert report["avg_pnl_error"] == "0.025"
    assert report["max_pnl_error"] == "0.05"
    assert report["requires_recalibration"] is True
    assert report["trust_status"] == "review"
    assert "status error" in report["trust_reason"]


def test_normalize_calibration_order_accepts_nested_sim_live_payload() -> None:
    row = normalize_calibration_order(
        {
            "sampleId": "nested-1",
            "marketSlug": "market-a",
            "observedBlock": 123,
            "simulated": {
                "orderId": "sim-1",
                "status": "FILLED",
                "avgFillPrice": "0.42",
                "filledSize": "5",
                "cashDelta": "-2.10",
                "positionDelta": "5",
                "pnl": "0.40",
                "latencySeconds": "1.25",
            },
            "live": {
                "orderId": "live-1",
                "status": "FILLED",
                "avgFillPrice": "0.44",
                "filledSize": "4",
                "cashDelta": "-1.76",
                "positionDelta": "4",
                "pnl": "-0.80",
                "latencySeconds": "3.25",
            },
        },
        source="live-shadow",
        run_id=77,
    )

    assert row["run_id"] == 77
    assert row["source"] == "live-shadow"
    assert row["sample_id"] == "nested-1"
    assert row["simulated_order_id"] == "sim-1"
    assert row["live_order_id"] == "live-1"
    assert str(row["price_error"]) == "0.0200000000"
    assert str(row["size_error"]) == "1.0000000000"
    assert str(row["cash_error"]) == "0.3400000000"
    assert str(row["pnl_error"]) == "1.2000000000"
    assert str(row["latency_error_seconds"]) == "2.0000000000"


def test_calibration_trust_states() -> None:
    assert empty_calibration_report()["trust_status"] == "unknown"
    assert calibration_trust({
        "sample_count": 4,
        "requires_recalibration": False,
        "status_error_rate": "0",
        "avg_price_error": "0.001",
        "avg_slippage_error": "0.001",
        "avg_latency_error_seconds": "0.5",
    })["trust_status"] == "ready"
    review = calibration_trust({
        "sample_count": 4,
        "requires_recalibration": True,
        "status_error_rate": "0",
        "avg_price_error": "0.02",
        "avg_slippage_error": "0.001",
        "avg_latency_error_seconds": "0.5",
    })
    assert review["trust_status"] == "review"
    assert "avg price error" in review["trust_reason"]


def test_calibration_report_flags_pnl_drift() -> None:
    row = normalize_calibration_order(
        {
            "sample_id": "pnl-drift",
            "simulated_status": "FILLED",
            "live_status": "FILLED",
            "simulated_fill_price": "0.40",
            "live_fill_price": "0.40",
            "simulated_fill_size": "10",
            "live_fill_size": "10",
            "simulated_pnl": "3.50",
            "live_pnl": "1.20",
            "simulated_latency_seconds": "1",
            "live_latency_seconds": "1",
        }
    )

    report = build_calibration_report([row])

    assert row["verdict"] == "pnl_mismatch"
    assert report["verdict_counts"]["pnl_mismatch"] == 1
    assert report["avg_pnl_error"] == "2.3"
    assert report["requires_recalibration"] is True
    assert "avg pnl error" in report["trust_reason"]
