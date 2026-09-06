from quant.backtest.parameter_robustness import READY, REVIEW, build_parameter_robustness_report


def _row(index: int, *, threshold: str, profile: str, pnl: str, regime: str, mode: str) -> dict:
    return {
        "run_id": index,
        "parameter_fingerprint": f"{threshold}-{profile}",
        "parameters": {
            "entry_threshold": threshold,
            "execution_profile": profile,
        },
        "market_category": "sports",
        "liquidity_bucket": regime,
        "mode": mode,
        "net_pnl": pnl,
        "max_drawdown": "1",
        "fill_rate": "0.80",
        "sample_count": 20,
    }


def test_parameter_robustness_report_blocks_best_only_runs() -> None:
    report = build_parameter_robustness_report([
        _row(1, threshold="0.60", profile="realistic", pnl="12", regime="active", mode="single")
    ])

    assert report["status"] == READY
    assert report["robustness_verdict"] == REVIEW
    assert report["best_run"]["net_pnl"] == "12"
    assert report["production_parameter_policy"]["best_parameter_promotion_allowed"] is False
    assert report["production_parameter_policy"]["default_action"] == "do_not_promote_best_only"
    assert "only 1 scored runs" in report["reason"]


def test_parameter_robustness_prefers_performance_score_over_raw_pnl() -> None:
    high_pnl_low_score = _row(1, threshold="0.60", profile="realistic", pnl="100", regime="active", mode="train")
    high_pnl_low_score["performance_score"] = "1"
    lower_pnl_better_score = _row(2, threshold="0.62", profile="realistic", pnl="20", regime="active", mode="test")
    lower_pnl_better_score["performance_score"] = "8"

    report = build_parameter_robustness_report([high_pnl_low_score, lower_pnl_better_score])

    assert report["best_run"]["run_id"] == "2"
    assert report["best_run"]["score"] == "8"


def test_parameter_robustness_report_summarizes_scan_and_regimes() -> None:
    rows = [
        _row(1, threshold="0.55", profile="optimistic", pnl="3", regime="thin", mode="train"),
        _row(2, threshold="0.55", profile="realistic", pnl="2", regime="active", mode="test"),
        _row(3, threshold="0.60", profile="optimistic", pnl="7", regime="thin", mode="train"),
        _row(4, threshold="0.60", profile="realistic", pnl="5", regime="active", mode="test"),
        _row(5, threshold="0.65", profile="optimistic", pnl="-2", regime="thin", mode="train"),
        _row(6, threshold="0.65", profile="realistic", pnl="-1", regime="active", mode="test"),
        _row(7, threshold="0.70", profile="optimistic", pnl="1", regime="thin", mode="walk_forward"),
        _row(8, threshold="0.70", profile="realistic", pnl="0", regime="active", mode="walk_forward"),
        _row(9, threshold="0.75", profile="optimistic", pnl="-4", regime="thin", mode="test"),
        _row(10, threshold="0.75", profile="realistic", pnl="-3", regime="active", mode="test"),
    ]

    report = build_parameter_robustness_report(rows)

    assert report["status"] == READY
    assert report["robustness_verdict"] == READY
    assert report["best_run"]["parameters"]["entry_threshold"] == "0.60"
    assert report["median_run"]["run_id"] in {"2", "7"}
    assert len(report["worst_decile"]) == 1
    assert report["worst_decile"][0]["net_pnl"] == "-4"
    assert {row["parameter"] for row in report["parameter_sensitivity"]} == {"entry_threshold", "execution_profile"}
    assert "liquidity_bucket" in report["regime_split_performance"]
    assert report["production_parameter_policy"]["best_parameter_promotion_allowed"] is True
    assert report["train_test_present"] is True
    assert report["walk_forward_present"] is True
