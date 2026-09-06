from quant.backtest.regime_coverage import READY, REVIEW, build_regime_coverage_report


def _row(
    index: int,
    *,
    category: str,
    liquidity: str,
    time_bucket: str,
    volatility: str = "low",
    final_minute: str = "not_final_minute",
    outcome_bucket: str = "binary",
    pnl: str = "1",
) -> dict:
    return {
        "order_id": f"o{index}",
        "market_category": category,
        "liquidity_bucket": liquidity,
        "time_to_expiry_bucket": time_bucket,
        "volatility_bucket": volatility,
        "final_minute": final_minute,
        "event_outcome_count_bucket": outcome_bucket,
        "status": "FILLED",
        "actual_fill_size": "10",
        "sample_count": 3,
        "pnl": pnl,
    }


def test_regime_coverage_marks_single_regime_as_specific() -> None:
    report = build_regime_coverage_report([
        _row(1, category="sports", liquidity="active", time_bucket="lt_1d"),
        _row(2, category="sports", liquidity="active", time_bucket="lt_1d"),
    ])

    assert report["status"] == READY
    assert report["coverage_verdict"] == REVIEW
    assert report["strategy_scope"] == "regime_specific"
    assert report["regime_specific"] is True
    assert "market_category" in report["narrow_dimensions"]
    assert "liquidity_bucket" in report["narrow_dimensions"]


def test_regime_coverage_allows_generalizable_candidate_with_core_buckets() -> None:
    rows = [
        _row(1, category="sports", liquidity="active", time_bucket="lt_1d", volatility="low", final_minute="not_final_minute", outcome_bucket="binary"),
        _row(2, category="sports", liquidity="active", time_bucket="lt_1d", volatility="low", final_minute="not_final_minute", outcome_bucket="binary"),
        _row(3, category="crypto", liquidity="thin", time_bucket="gte_7d", volatility="high", final_minute="final_minute", outcome_bucket="large_multi_21_plus", pnl="-1"),
        _row(4, category="crypto", liquidity="thin", time_bucket="gte_7d", volatility="high", final_minute="final_minute", outcome_bucket="large_multi_21_plus", pnl="-2"),
    ]

    report = build_regime_coverage_report(rows)

    assert report["status"] == READY
    assert report["coverage_verdict"] == READY
    assert report["strategy_scope"] == "generalizable_candidate"
    assert report["regime_specific"] is False
    assert report["ready_dimension_count"] == 6
    assert report["dimensions"]["market_category"]["known_bucket_count"] == 2
    assert report["dimensions"]["liquidity_bucket"]["ready_bucket_count"] == 2
