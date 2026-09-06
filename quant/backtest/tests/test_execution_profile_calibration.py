from quant.backtest.calibration import normalize_calibration_order
from quant.backtest.calibration_report import build_periodic_calibration_report
from quant.backtest.execution_profile_calibration import (
    execution_profile_suggestions_from_report,
    execution_profile_suggestions_to_markdown,
)


def test_execution_profile_suggestions_flag_stress_bucket() -> None:
    rows = [
        normalize_calibration_order(
            {
                "sample_id": f"sample-{idx}",
                "market_slug": "world-cup",
                "role": "taker",
                "side": "BUY",
                "liquidity_bucket": "thin",
                "simulated_status": "FILLED",
                "live_status": "NO_FILL",
                "simulated_fill_price": "0.40",
                "live_fill_price": "0.45",
                "simulated_slippage": "0.005",
                "live_slippage": "0.045",
                "simulated_latency_seconds": "0.5",
                "live_latency_seconds": "8.0",
            },
            source="test",
        )
        for idx in range(4)
    ]
    report = build_periodic_calibration_report(rows, bucket_fields=("liquidity_bucket",), min_bucket_samples=3)

    suggestions = execution_profile_suggestions_from_report(report, min_samples=3)

    assert suggestions
    overall = suggestions[0]
    assert overall["scope"] == "overall"
    assert overall["recommended"]["execution_profile"] == "stress"
    assert overall["recommended"]["latency_blocks_floor"] == 1
    assert overall["recommended"]["adverse_slippage_price_floor"] == "0.05"
    assert overall["recommended"]["fill_probability_haircut_pct_floor"] == "80"
    assert any(item.get("bucket") == "thin" for item in suggestions)


def test_execution_profile_suggestions_ignore_low_sample_buckets() -> None:
    rows = [
        normalize_calibration_order(
            {
                "sample_id": "one-sample",
                "liquidity_bucket": "thin",
                "simulated_status": "FILLED",
                "live_status": "NO_FILL",
            },
            source="test",
        )
    ]
    report = build_periodic_calibration_report(rows, bucket_fields=("liquidity_bucket",), min_bucket_samples=1)

    suggestions = execution_profile_suggestions_from_report(report, min_samples=3)

    assert suggestions == []


def test_execution_profile_suggestions_markdown_empty_state() -> None:
    markdown = execution_profile_suggestions_to_markdown([])

    assert "# Execution Profile Calibration Suggestions" in markdown
    assert "No execution profile changes suggested" in markdown
