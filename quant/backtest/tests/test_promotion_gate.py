from quant.backtest.promotion_gate import build_fill_first_promotion_gate_report


def ready_inputs() -> dict:
    return {
        "run_credibility": {"status": "ready"},
        "data_quality_report": {"quality_verdict": "ready"},
        "reproducibility_report": {"reproducibility_verdict": "ready"},
        "materialized_cache_report": {"cache_verdict": "ready"},
        "shadow_live_triangulation_report": {"triangulation_verdict": "ready", "fill_model_suspect": False},
        "regime_coverage_report": {"coverage_verdict": "ready", "strategy_scope": "generalizable"},
        "prediction_quality_report": {"prediction_verdict": "ready"},
        "settlement_compatibility_report": {"compatibility_verdict": "ready"},
        "external_source_run_coverage_report": {
            "status": "ready",
            "order_state_coverage_pct": "100",
            "calibration_coverage_pct": "100",
        },
        "external_source_missing_evidence_plan": {
            "status": "ready",
            "missing_order_state_count": 0,
            "missing_calibration_count": 0,
        },
    }


def test_promotion_gate_allows_live_when_core_reports_are_ready() -> None:
    report = build_fill_first_promotion_gate_report(**ready_inputs())

    assert report["status"] == "ready"
    assert report["promotion_verdict"] == "ready"
    assert report["paper_promotion_allowed"] is True
    assert report["production_promotion_allowed"] is True
    assert report["allowed_next_modes"] == ["backtest", "paper", "live"]


def test_promotion_gate_blocks_fill_model_suspect() -> None:
    inputs = ready_inputs()
    inputs["shadow_live_triangulation_report"] = {
        "triangulation_verdict": "review",
        "fill_model_suspect": True,
    }

    report = build_fill_first_promotion_gate_report(**inputs)

    assert report["promotion_verdict"] == "blocked"
    assert report["paper_promotion_allowed"] is False
    assert report["production_promotion_allowed"] is False
    assert "fill_model_suspect=true" in report["blocked_reasons"]
    assert "shadow/live triangulation verdict=review" in report["blocked_reasons"]


def test_promotion_gate_allows_paper_but_not_live_for_regime_specific_strategy() -> None:
    inputs = ready_inputs()
    inputs["regime_coverage_report"] = {
        "coverage_verdict": "review",
        "strategy_scope": "regime_specific",
    }

    report = build_fill_first_promotion_gate_report(**inputs)

    assert report["promotion_verdict"] == "review"
    assert report["paper_promotion_allowed"] is True
    assert report["production_promotion_allowed"] is False
    assert report["allowed_next_modes"] == ["backtest", "paper"]
    assert "strategy_scope=regime_specific" in report["review_reasons"]


def test_promotion_gate_blocks_missing_external_evidence() -> None:
    inputs = ready_inputs()
    inputs["external_source_run_coverage_report"] = {
        "status": "review",
        "order_state_coverage_pct": "50",
        "calibration_coverage_pct": "0",
    }
    inputs["external_source_missing_evidence_plan"] = {
        "status": "review",
        "missing_order_state_count": 1,
        "missing_calibration_count": 2,
    }

    report = build_fill_first_promotion_gate_report(**inputs)

    assert report["promotion_verdict"] == "blocked"
    assert report["paper_promotion_allowed"] is False
    assert report["production_promotion_allowed"] is False
    assert "external source run coverage=review" in report["blocked_reasons"]
    assert "missing_order_state_count=1" in report["blocked_reasons"]
    assert "missing_calibration_count=2" in report["blocked_reasons"]
