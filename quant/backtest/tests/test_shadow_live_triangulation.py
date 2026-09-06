from quant.backtest.shadow_live_triangulation import build_shadow_live_triangulation_report


def clean_fill_sample() -> dict:
    return {
        "simulated_status": "FILLED",
        "live_status": "FILLED",
        "simulated_fill_price": "0.20",
        "live_fill_price": "0.20",
        "simulated_fill_size": "10",
        "live_fill_size": "10",
        "simulated_slippage": "0",
        "live_slippage": "0",
        "simulated_fee": "0.01",
        "live_fee": "0.01",
        "simulated_rebate": "0",
        "live_rebate": "0",
        "simulated_cash_delta": "-2.01",
        "live_cash_delta": "-2.01",
        "simulated_position_delta": "10",
        "live_position_delta": "10",
        "simulated_pnl": "0.40",
        "live_pnl": "0.40",
        "simulated_latency_seconds": "1",
        "live_latency_seconds": "1",
        "role": "taker",
        "side": "BUY_YES",
        "liquidity_bucket": "medium",
        "volatility_bucket": "low",
        "time_to_expiry_bucket": "lt_7d",
    }


def clean_cost_sample() -> dict:
    return {
        "event_type": "FEE",
        "simulated_amount": "0.01",
        "live_amount": "0.01",
        "amount_error": "0",
        "simulated_count": 1,
        "live_count": 1,
        "verdict": "matched",
    }


def test_shadow_live_triangulation_reviews_missing_evidence() -> None:
    report = build_shadow_live_triangulation_report([], [])

    assert report["status"] == "review"
    assert report["triangulation_verdict"] == "review"
    assert report["fill_model_suspect"] is True
    assert report["shadow_live_sample_count"] == 0
    assert "no shadow/live terminal evidence" in report["reason"]


def test_shadow_live_triangulation_ready_with_clean_fill_and_cost_samples() -> None:
    report = build_shadow_live_triangulation_report(
        [clean_fill_sample()],
        [clean_cost_sample()],
        real_order_state_event_count=1,
        external_source_state_count=1,
    )

    assert report["status"] == "ready"
    assert report["triangulation_verdict"] == "ready"
    assert report["fill_model_suspect"] is False
    assert report["shadow_live_sample_count"] == 1
    assert report["cost_sample_count"] == 1
    assert report["drift_summary"]["avg_price_error"] == "0"
    assert report["drift_summary"]["avg_pnl_error"] == "0"
    assert report["drift_summary"]["total_cost_amount_error"] == "0"


def test_shadow_live_triangulation_flags_fill_or_cost_drift() -> None:
    fill = clean_fill_sample()
    fill["live_status"] = "NO_FILL"
    fill["live_fill_price"] = "0.25"
    fill["live_latency_seconds"] = "8"
    cost = clean_cost_sample()
    cost["live_amount"] = "0.03"
    cost["amount_error"] = "0.02"

    report = build_shadow_live_triangulation_report([fill], [cost])

    assert report["status"] == "ready"
    assert report["triangulation_verdict"] == "review"
    assert report["fill_model_suspect"] is True
    assert "status error rate" in report["reason"]
    assert "avg price error" in report["reason"]
    assert report["drift_summary"]["total_cost_amount_error"] == "0.02"


def test_shadow_live_triangulation_flags_pnl_drift() -> None:
    fill = clean_fill_sample()
    fill["simulated_pnl"] = "4.20"
    fill["live_pnl"] = "0.80"

    report = build_shadow_live_triangulation_report([fill], [clean_cost_sample()])

    assert report["triangulation_verdict"] == "review"
    assert report["fill_model_suspect"] is True
    assert "avg pnl error" in report["reason"]
    assert report["drift_summary"]["avg_pnl_error"] == "3.4"
