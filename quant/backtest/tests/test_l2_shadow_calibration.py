from __future__ import annotations

from quant.backtest.l2_shadow_calibration import (
    build_l2_shadow_calibration_report,
    l2_shadow_calibration_to_markdown,
)


def test_l2_shadow_calibration_ready_with_clean_taker_and_maker_samples() -> None:
    report = build_l2_shadow_calibration_report(
        [
            {
                "role": "taker",
                "simulated_status": "FILLED",
                "live_status": "FILLED",
                "simulated_fill_price": "0.520",
                "live_fill_price": "0.521",
                "simulated_fill_size": "10",
                "live_fill_size": "10",
            },
            {
                "role": "maker",
                "simulated_status": "FILLED",
                "live_status": "FILLED",
                "fill_probability": "0.90",
                "simulated_time_to_fill_seconds": "4",
                "live_time_to_fill_seconds": "5",
            },
            {
                "role": "maker",
                "simulated_status": "NO_FILL",
                "live_status": "NO_FILL",
                "fill_probability": "0.10",
            },
        ],
        generated_at="2026-07-01T00:00:00+00:00",
    )

    assert report["status"] == "ready"
    assert report["paired_sample_count"] == 3
    assert report["taker"]["classification_accuracy_pct"] == "100"
    assert report["taker"]["avg_fill_price_error"] == "0.001"
    assert report["maker"]["brier_score"] == "0.01"
    assert report["maker"]["false_positive_fill_rate_pct"] == "0"
    assert report["calibration_suggestions"] == []


def test_l2_shadow_calibration_reviews_maker_false_positive_fills() -> None:
    report = build_l2_shadow_calibration_report(
        [
            {
                "role": "maker",
                "simulated_status": "FILLED",
                "live_status": "NO_FILL",
                "fill_probability": "0.90",
                "simulated_time_to_fill_seconds": "3",
                "live_time_to_fill_seconds": "20",
            },
            {
                "role": "maker",
                "simulated_status": "FILLED",
                "live_status": "CANCELED",
                "fill_probability": "0.85",
            },
        ]
    )

    assert report["status"] == "review"
    assert "maker brier score" in report["reason"]
    assert report["maker"]["false_positive_fill_rate_pct"] == "100"
    assert any(item["parameter"] == "queue_ahead_fraction" and item["direction"] == "increase" for item in report["calibration_suggestions"])
    assert any(item["parameter"] == "cancel_ahead_fraction" and item["direction"] == "decrease" for item in report["calibration_suggestions"])
    assert any(item["parameter"] == "book_ttl_ms" and item["direction"] == "decrease" for item in report["calibration_suggestions"])


def test_l2_shadow_calibration_missing_without_actual_order_state() -> None:
    report = build_l2_shadow_calibration_report(
        [
            {
                "role": "taker",
                "simulated_status": "FILLED",
                "simulated_fill_price": "0.52",
                "simulated_fill_size": "10",
            }
        ]
    )

    assert report["status"] == "missing"
    assert report["paired_sample_count"] == 0
    assert "actual paper/live order state" in report["reason"]


def test_l2_shadow_calibration_markdown_contains_metrics_and_suggestions() -> None:
    report = build_l2_shadow_calibration_report(
        [
            {
                "role": "taker",
                "simulated_status": "FILLED",
                "live_status": "NO_FILL",
                "simulated_fill_price": "0.50",
                "live_fill_price": "0.55",
                "simulated_fill_size": "20",
                "live_fill_size": "0",
            }
        ]
    )
    markdown = l2_shadow_calibration_to_markdown(report)

    assert "# L2 Shadow/Live Calibration: review" in markdown
    assert "taker_avg_fill_price_error" not in markdown
    assert "avg_fill_price_error" in markdown
    assert "depth_haircut" in markdown
    assert "impact_bps" in markdown
