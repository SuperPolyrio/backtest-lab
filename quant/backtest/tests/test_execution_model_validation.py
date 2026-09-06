from __future__ import annotations

from quant.backtest.execution_model_validation import (
    READY,
    REVIEW,
    build_execution_model_validation_report,
    execution_model_validation_to_markdown,
)


def _row(
    model: str,
    profile: str,
    *,
    pnl: str,
    filled: int,
    submitted: int = 10,
    partial: int = 1,
    unfilled: int = 0,
    slippage: str = "0.01",
    queue_wait: str = "1",
    capacity_ratio: str = "0.25",
    category: str = "sports",
    liquidity: str = "active",
    time_bucket: str = "lt_1d",
    volatility: str = "low",
    final_minute: str = "not_final_minute",
    outcome_bucket: str = "binary",
    strategy: str = "demo",
) -> dict:
    return {
        "execution_model": model,
        "execution_profile": profile,
        "strategy_name": strategy,
        "net_pnl": pnl,
        "submitted_count": submitted,
        "filled_count": filled,
        "partial_fill_count": partial,
        "unfilled_cancelled_count": unfilled,
        "avg_slippage": slippage,
        "queue_wait_seconds": queue_wait,
        "capacity_ratio": capacity_ratio,
        "market_category": category,
        "liquidity_bucket": liquidity,
        "time_to_expiry_bucket": time_bucket,
        "volatility_bucket": volatility,
        "final_minute": final_minute,
        "event_outcome_count_bucket": outcome_bucket,
    }


def test_execution_model_validation_ready_for_baselines_l2_profiles_capacity_and_regimes() -> None:
    rows = [
        _row("ohlcv_close", "realistic", pnl="20", filled=10, slippage="0.00", capacity_ratio="0.10"),
        _row("formula_slippage", "realistic", pnl="16", filled=9, slippage="0.01", capacity_ratio="0.20"),
        _row("l2_orderfilled", "conservative", pnl="3", filled=5, unfilled=5, slippage="0.02", capacity_ratio="0.20"),
        _row("l2_orderfilled", "realistic", pnl="5", filled=6, unfilled=4, slippage="0.025", capacity_ratio="0.45"),
        _row("l2_orderfilled", "optimistic", pnl="7", filled=7, unfilled=3, slippage="0.03", capacity_ratio="1.20"),
        _row(
            "l2_orderfilled",
            "conservative",
            pnl="-1",
            filled=1,
            unfilled=9,
            slippage="0.05",
            capacity_ratio="1.50",
            category="crypto",
            liquidity="thin",
            time_bucket="gte_7d",
            volatility="high",
            final_minute="final_minute",
            outcome_bucket="large_multi_21_plus",
        ),
    ]

    report = build_execution_model_validation_report(rows)

    assert report["status"] == READY
    assert report["validation_verdict"] == READY
    assert report["missing_models"] == []
    assert report["model_summaries"]["l2_orderfilled"]["fill_rate"] == "0.475"
    assert report["model_comparison"]["l2_fill_rate_delta_vs_max_baseline"] == "-0.525"
    assert report["l2_profile_sensitivity"]["monotonic_fill_rate_ok"] is True
    assert report["l2_profile_sensitivity"]["execution_sensitivity_flag"] is False
    assert {row["capacity_bucket"] for row in report["capacity_curve"]} >= {"lte_25pct", "lte_50pct", "gt_100pct"}
    assert report["regime_stress"]["status"] == READY
    assert report["strategy_attribution"][0]["strategy_name"] == "demo"


def test_execution_model_validation_reviews_missing_baseline_and_l2_profile() -> None:
    report = build_execution_model_validation_report(
        [
            _row("ohlcv_close", "realistic", pnl="20", filled=10),
            _row("l2_orderfilled", "optimistic", pnl="12", filled=8),
        ]
    )

    assert report["validation_verdict"] == REVIEW
    assert "formula_slippage" in report["missing_models"]
    assert "conservative" in report["l2_profile_sensitivity"]["missing_profiles"]
    assert "realistic" in report["l2_profile_sensitivity"]["missing_profiles"]


def test_execution_model_validation_flags_l2_too_optimistic_and_profile_monotonicity() -> None:
    rows = [
        _row("ohlcv_close", "realistic", pnl="10", filled=4),
        _row("formula_slippage", "realistic", pnl="9", filled=5),
        _row("l2_orderfilled", "conservative", pnl="7", filled=8),
        _row("l2_orderfilled", "realistic", pnl="8", filled=6),
        _row("l2_orderfilled", "optimistic", pnl="20", filled=7),
        _row(
            "l2_orderfilled",
            "optimistic",
            pnl="1",
            filled=1,
            category="crypto",
            liquidity="thin",
            time_bucket="gte_7d",
            volatility="high",
            final_minute="final_minute",
            outcome_bucket="large_multi_21_plus",
        ),
    ]

    report = build_execution_model_validation_report(rows)

    assert report["validation_verdict"] == REVIEW
    assert report["model_comparison"]["l2_more_optimistic_than_baseline"] is True
    assert report["l2_profile_sensitivity"]["monotonic_fill_rate_ok"] is False
    assert "L2 fill rate exceeds coarse baselines" in report["reason"]
    assert "monotonicity failed" in report["reason"]


def test_execution_model_validation_markdown_contains_core_sections() -> None:
    report = build_execution_model_validation_report(
        [
            _row("ohlcv_close", "realistic", pnl="20", filled=10),
            _row("formula_slippage", "realistic", pnl="16", filled=9),
            _row("l2_orderfilled", "conservative", pnl="8", filled=5),
            _row("l2_orderfilled", "realistic", pnl="10", filled=6),
            _row("l2_orderfilled", "optimistic", pnl="12", filled=7),
        ],
        min_regime_buckets=1,
    )
    markdown = execution_model_validation_to_markdown(report)

    assert "# Execution Model Validation" in markdown
    assert "## Model Comparison" in markdown
    assert "## L2 Profile Sensitivity" in markdown
    assert "## Capacity Curve" in markdown
