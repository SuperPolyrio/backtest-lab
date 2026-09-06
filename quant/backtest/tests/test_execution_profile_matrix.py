from quant.backtest.execution_profile_matrix import READY, REVIEW, build_execution_profile_matrix_report


def _row(profile: str, pnl: str, *, fill_rate: str = "0.8") -> dict:
    return {
        "execution_profile": profile,
        "total_pnl": pnl,
        "fill_rate": fill_rate,
        "signal_count": 10,
        "trades": 8,
        "no_fills": 2,
    }


def test_execution_profile_matrix_requires_realistic_and_conservative() -> None:
    report = build_execution_profile_matrix_report([
        _row("optimistic", "15"),
        _row("realistic", "10"),
    ])

    assert report["status"] == READY
    assert report["coverage_verdict"] == REVIEW
    assert report["missing_profiles"] == ["conservative"]
    assert report["production_policy"]["profile_promotion_allowed"] is False
    assert "Run missing execution profiles: conservative" in report["next_actions"]


def test_execution_profile_matrix_compares_conservative_degradation() -> None:
    report = build_execution_profile_matrix_report([
        _row("realistic", "10", fill_rate="0.8"),
        _row("conservative", "6", fill_rate="0.5"),
        _row("stress", "3", fill_rate="0.3"),
    ])

    assert report["coverage_verdict"] == READY
    assert report["missing_profiles"] == []
    assert report["profiles"]["realistic"]["avg_net_pnl"] == "10"
    assert report["comparisons"]["conservative"]["pnl_delta_vs_baseline"] == "-4"
    assert report["comparisons"]["conservative"]["pnl_degradation_pct_vs_baseline"] == "40"
    assert report["production_policy"]["profile_promotion_allowed"] is True


def test_execution_profile_matrix_reviews_negative_conservative_result() -> None:
    report = build_execution_profile_matrix_report([
        _row("realistic", "10"),
        _row("conservative", "-1"),
    ])

    assert report["coverage_verdict"] == REVIEW
    assert "conservative profile turns profitable realistic result negative" in report["reason"]
