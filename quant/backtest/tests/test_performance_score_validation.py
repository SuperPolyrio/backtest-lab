from quant.backtest.performance_score_validation import READY, REVIEW, build_performance_score_validation_report


def test_performance_score_validation_flags_small_sample_top_rank() -> None:
    report = build_performance_score_validation_report(
        [
            {"run_id": 1, "performance_score": "50", "closed_trade_count": 1, "net_pnl": "60"},
            {"run_id": 2, "performance_score": "12", "closed_trade_count": 8, "net_pnl": "16"},
            {"run_id": 3, "performance_score": "10", "closed_trade_count": 7, "net_pnl": "12"},
            {"run_id": 4, "performance_score": "8", "closed_trade_count": 6, "net_pnl": "9"},
        ],
        top_n=3,
    )

    assert report["status"] == READY
    assert report["score_bias_verdict"] == REVIEW
    assert report["top_small_sample_count"] == 1
    assert report["small_sample_score_premium"] == "38"
    assert "best small-sample score exceeds" in report["reason"]


def test_performance_score_validation_ready_for_reliable_top_rows() -> None:
    report = build_performance_score_validation_report(
        [
            {"run_id": 1, "performance_score": "15", "closed_trade_count": 8, "net_pnl": "20"},
            {"run_id": 2, "performance_score": "12", "closed_trade_count": 5, "net_pnl": "14"},
            {"run_id": 3, "performance_score": "9", "closed_trade_count": 4, "net_pnl": "10"},
            {"run_id": 4, "performance_score": "4", "closed_trade_count": 1, "net_pnl": "30"},
        ],
        top_n=3,
    )

    assert report["status"] == READY
    assert report["score_bias_verdict"] == READY
    assert report["top_small_sample_share_pct"] == "0"
    assert report["best_reliable_sample"]["run_id"] == 1
