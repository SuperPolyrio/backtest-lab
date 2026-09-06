from __future__ import annotations

from quant.backtest.calibration import normalize_calibration_order
from quant.backtest.calibration_report import build_periodic_calibration_report, calibration_report_to_markdown


def test_periodic_calibration_report_groups_review_buckets() -> None:
    rows = [
        normalize_calibration_order(
            {
                "sample_id": "maker-ok",
                "market_slug": "market-a",
                "payload": {"category": "sports"},
                "role": "maker",
                "side": "BUY",
                "liquidity_bucket": "high",
                "simulated_status": "FILLED",
                "live_status": "FILLED",
                "simulated_fill_price": "0.200",
                "live_fill_price": "0.201",
                "simulated_slippage": "0.001",
                "live_slippage": "0.001",
                "simulated_pnl": "0.30",
                "live_pnl": "0.30",
                "simulated_latency_seconds": "1.0",
                "live_latency_seconds": "1.5",
            }
        ),
        normalize_calibration_order(
            {
                "sample_id": "taker-review",
                "market_slug": "market-b",
                "payload": {"category": "sports"},
                "role": "taker",
                "side": "BUY",
                "liquidity_bucket": "low",
                "simulated_status": "FILLED",
                "live_status": "NO_FILL",
                "simulated_fill_price": "0.600",
                "live_fill_price": "",
                "simulated_slippage": "0.001",
                "live_slippage": "0.040",
                "simulated_pnl": "2.50",
                "live_pnl": "0.25",
                "simulated_latency_seconds": "0.5",
                "live_latency_seconds": "5.0",
            }
        ),
    ]

    report = build_periodic_calibration_report(rows, bucket_fields=("market_category", "role", "liquidity_bucket"), generated_at="2026-06-25T00:00:00+00:00")

    assert report["sample_count"] == 2
    assert report["overall"]["trust_status"] == "review"
    assert report["buckets"]["role"][0]["bucket"] == "taker"
    assert report["buckets"]["role"][0]["trust_status"] == "review"
    assert report["buckets"]["market_category"][0]["bucket"] == "sports"
    assert report["buckets"]["liquidity_bucket"][0]["bucket"] == "low"
    assert report["overall"]["avg_pnl_error"] == "1.125"
    assert any("Segment-specific recalibration" in item for item in report["recommendations"])


def test_periodic_calibration_report_embeds_execution_profile_suggestions() -> None:
    rows = [
        normalize_calibration_order(
            {
                "sample_id": f"taker-review-{idx}",
                "market_slug": "market-b",
                "role": "taker",
                "side": "BUY",
                "liquidity_bucket": "low",
                "simulated_status": "FILLED",
                "live_status": "NO_FILL",
                "simulated_fill_price": "0.600",
                "live_fill_price": "",
                "simulated_slippage": "0.001",
                "live_slippage": "0.040",
                "simulated_latency_seconds": "0.5",
                "live_latency_seconds": "5.0",
            }
        )
        for idx in range(3)
    ]

    report = build_periodic_calibration_report(rows, bucket_fields=("liquidity_bucket",), min_bucket_samples=3)
    suggestions = report["execution_profile_suggestions"]

    assert suggestions[0]["scope"] == "overall"
    assert suggestions[0]["recommended"]["execution_profile"] == "stress"
    assert any(item.get("bucket_field") == "liquidity_bucket" and item.get("bucket") == "low" for item in suggestions)

    markdown = calibration_report_to_markdown(report)
    assert "## Execution Profile Suggestions" in markdown
    assert "liquidity_bucket=low" in markdown
    assert "stress" in markdown


def test_calibration_report_markdown_contains_core_sections() -> None:
    report = build_periodic_calibration_report([], generated_at="2026-06-25T00:00:00+00:00")
    markdown = calibration_report_to_markdown(report)

    assert "# Fill Calibration Report" in markdown
    assert "## Overall" in markdown
    assert "## Recommendations" in markdown
    assert "avg_pnl_error" in markdown
    assert "no calibration samples" in markdown
