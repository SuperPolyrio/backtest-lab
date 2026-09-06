from __future__ import annotations

from quant.backtest.fill_only_lob_validity import build_execution_stability_report
from quant.backtest.fill_trade_validation import (
    _lob_holdout_plan_from_rows,
    _lob_holdout_quality_gate_checks,
    _summarize_holdout_comparisons,
    benchmark_fill_trade_replay,
    build_parameter_sensitivity_report,
    compare_reference_vs_indexed,
    run_golden_fixtures,
    synthetic_orders_and_trades,
    validate_execution_invariants,
)
from quant.backtest.orderfilled_v2_replay import replay_v2_taker_orders_with_diagnostics
from scripts.calibrate_fill_only_lob_validity import _split_holdout_metrics


def test_fill_trade_golden_fixtures_pass() -> None:
    report = run_golden_fixtures()

    assert report["status"] == "pass"
    assert report["check_summary"]["PASS"] >= 9


def test_fill_trade_reference_and_indexed_match() -> None:
    report = compare_reference_vs_indexed(trades_count=100, orders_count=20, seed=7)

    assert report["status"] == "pass"
    assert report["summary"]["comparison"]["diff_count"] == 0


def test_fill_trade_invariants_pass_on_synthetic_replay() -> None:
    orders, trades = synthetic_orders_and_trades(trades_count=250, orders_count=25, seed=8)
    results, ledger, _ = replay_v2_taker_orders_with_diagnostics(orders, trades)

    report = validate_execution_invariants(orders=orders, results=results, ledger=ledger, trades=trades)

    assert report["status"] == "pass"


def test_fill_trade_benchmark_and_sensitivity_reports_are_complete() -> None:
    benchmark = benchmark_fill_trade_replay(trades_count=250, orders_count=25, seed=9, reference=True)
    sensitivity = build_parameter_sensitivity_report(trades_count=250, orders_count=25, seed=9)

    assert benchmark["status"] == "pass"
    assert benchmark["summary"]["comparison"]["diff_count"] == 0
    assert sensitivity["status"] == "pass"
    assert len(sensitivity["summary"]["capacity_curve"]) == 5
    assert len(sensitivity["summary"]["latency_curve"]) == 5
    assert len(sensitivity["summary"]["horizon_curve"]) == 5


def test_lob_holdout_summary_reports_quality_gate_metrics() -> None:
    summary = _summarize_holdout_comparisons(
        [
            {
                "verdict": "both_filled_same",
                "fill_only": {"filled_size": "10"},
                "depth": {"filled_size": "10"},
                "size_delta": "0",
                "price_delta": "0",
            },
            {
                "verdict": "fill_only_only",
                "fill_only": {"filled_size": "5"},
                "depth": {"filled_size": "0"},
                "size_delta": "-5",
                "price_delta": "0",
            },
            {
                "verdict": "depth_only",
                "fill_only": {"filled_size": "0"},
                "depth": {"filled_size": "3"},
                "size_delta": "3",
                "price_delta": "0",
            },
        ]
    )

    assert str(summary["false_positive_rate"]) == "0.3333333333"
    assert str(summary["false_negative_rate"]) == "0.3333333333"
    assert str(summary["precision"]) == "0.5000000000"
    assert str(summary["recall"]) == "0.5000000000"
    assert str(summary["avg_abs_size_error"]) == "0E-10"
    assert str(summary["avg_abs_price_error"]) == "0E-10"
    assert str(summary["overfill_rate"]) == "0E-10"
    assert str(summary["underfill_rate"]) == "0E-10"
    assert str(summary["adverse_price_error"]) == "0E-10"


def test_lob_holdout_summary_counts_overfill_underfill_and_price_advantage() -> None:
    summary = _summarize_holdout_comparisons(
        [
            {
                "verdict": "both_filled_size_delta",
                "order": {"side": "BUY"},
                "fill_only": {"filled_size": "12"},
                "depth": {"filled_size": "10"},
                "size_delta": "-2",
                "price_delta": "0.01",
            },
            {
                "verdict": "both_filled_size_delta",
                "order": {"side": "SELL"},
                "fill_only": {"filled_size": "8"},
                "depth": {"filled_size": "10"},
                "size_delta": "2",
                "price_delta": "-0.02",
            },
        ]
    )

    assert str(summary["overfill_rate"]) == "0.5000000000"
    assert str(summary["underfill_rate"]) == "0.5000000000"
    assert str(summary["adverse_price_error"]) == "0.0150000000"
    assert str(summary["avg_abs_size_error"]) == "2.0000000000"
    assert str(summary["avg_abs_price_error"]) == "0.0150000000"


def test_lob_holdout_quality_gate_checks_fail_on_loose_fill_only() -> None:
    checks = _lob_holdout_quality_gate_checks(
        {
            "false_positive_rate": "0.10",
            "precision": "0.75",
            "overfill_rate": "0.20",
            "adverse_price_error": "0.010",
        }
    )

    assert [item.status for item in checks] == ["FAIL", "FAIL", "FAIL", "FAIL"]


def test_execution_stability_report_downgrades_tiny_holdout_samples() -> None:
    report = build_execution_stability_report(
        {
            "calibration": {"samples": 6, "false_positive_rate": "0", "false_negative_rate": "0", "overfill_rate": "0", "underfill_rate": "0", "adverse_price_error": "0"},
            "validation": {"samples": 2, "false_positive_rate": "0", "false_negative_rate": "0", "overfill_rate": "0", "underfill_rate": "0", "adverse_price_error": "0"},
            "holdout": {"samples": 1, "false_positive_rate": "0", "false_negative_rate": "0", "overfill_rate": "0", "underfill_rate": "0", "adverse_price_error": "0"},
        }
    )

    assert report["grade"] != "READY"
    assert "insufficient_total_holdout_samples" in report["blockers"]
    assert "insufficient_holdout_samples" in report["blockers"]


def test_execution_stability_report_marks_large_clean_holdout_ready() -> None:
    report = build_execution_stability_report(
        {
            "calibration": {"samples": 700, "false_positive_rate": "0.01", "false_negative_rate": "0.02", "overfill_rate": "0", "underfill_rate": "0.01", "adverse_price_error": "0"},
            "validation": {"samples": 200, "false_positive_rate": "0.01", "false_negative_rate": "0.02", "overfill_rate": "0", "underfill_rate": "0.01", "adverse_price_error": "0"},
            "holdout": {"samples": 150, "false_positive_rate": "0.01", "false_negative_rate": "0.02", "overfill_rate": "0", "underfill_rate": "0.01", "adverse_price_error": "0"},
        }
    )

    assert report["grade"] == "READY"
    assert report["blockers"] == []


def test_lob_holdout_calibration_plan_recommends_per_market_sample_count() -> None:
    plan = _lob_holdout_plan_from_rows(
        [
            {"market_slug": "a", "market_id": 1, "l2_rows": 100, "orderfilled_rows": 30},
            {"market_slug": "b", "market_id": 2, "l2_rows": 100, "orderfilled_rows": 80},
            {"market_slug": "empty", "market_id": 3, "l2_rows": 100, "orderfilled_rows": 0},
        ],
        target_samples=100,
    )

    assert plan["market_count"] == 2
    assert plan["recommended_per_market"] == 50
    assert plan["expected_samples_at_recommended_per_market"] == 80
    assert plan["sample_shortfall"] == 20
    assert plan["status"] == "limited_lob_holdout_scope"


def test_calibration_split_defaults_to_market_level_oos() -> None:
    rows = [
        {"sample": {"market_id": 1, "market_slug": "same", "sample_id": "a"}, "verdict": "both_no_fill"},
        {"sample": {"market_id": 1, "market_slug": "same", "sample_id": "b"}, "verdict": "both_no_fill"},
    ]
    split = _split_holdout_metrics(rows, split_key="market")

    assert sorted(bucket["samples"] for bucket in split.values()) == [0, 0, 2]
