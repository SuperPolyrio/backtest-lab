from quant.backtest.performance_score import READY, REVIEW, build_performance_score_report


def test_performance_score_combines_pnl_risk_fill_coverage_and_prediction() -> None:
    report = build_performance_score_report(
        trades=[
            {"trade_id": "t1", "pnl": "10"},
            {"trade_id": "t2", "pnl": "-4"},
            {"trade_id": "t3", "pnl": "3"},
            {"trade_id": "t4", "pnl": "2"},
        ],
        ledger=[
            {"x_value": 1, "cash_after": "1000"},
            {"x_value": 2, "cash_after": "1010"},
            {"x_value": 3, "cash_after": "1006"},
            {"x_value": 4, "cash_after": "1011"},
        ],
        fill_quality_report={"fill_rate": "80", "no_fill_rate": "20"},
        data_quality_report={"quality_verdict": READY, "span_coverage_pct": "90"},
        prediction_quality_report={"prediction_verdict": READY, "brier_advantage": "0.02"},
    )

    assert report["status"] == READY
    assert report["ranking_verdict"] == REVIEW
    assert report["net_pnl"] == "11"
    assert report["max_drawdown"] == "4"
    assert report["penalties"]["coverage_penalty"] == "0.1"
    assert report["penalties"]["low_fill_penalty"] == "0.2"
    assert report["components"]["brier_component"] == "2"
    assert report["performance_score"] == "8.7"
    assert report["rolling_return"]["count"] == 2
    assert "coverage below 100%" in report["reason"]


def test_performance_score_ready_when_inputs_are_complete() -> None:
    report = build_performance_score_report(
        trades=[
            {"trade_id": "t1", "pnl": "2"},
            {"trade_id": "t2", "pnl": "3"},
            {"trade_id": "t3", "pnl": "4"},
        ],
        ledger=[],
        fill_quality_report={"fill_rate": "100"},
        data_quality_report={"quality_verdict": READY, "span_coverage_pct": "100"},
        prediction_quality_report={"prediction_verdict": READY, "brier_advantage": "0"},
    )

    assert report["status"] == READY
    assert report["ranking_verdict"] == READY
    assert report["performance_score"] == "9"
    assert report["calmar"] == "0"
