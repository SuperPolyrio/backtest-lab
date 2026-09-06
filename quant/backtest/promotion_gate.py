"""Promotion/staging gate for fill-first backtest runs."""

from __future__ import annotations

from typing import Any, Mapping


READY = "ready"
REVIEW = "review"
MISSING = "missing"


def build_fill_first_promotion_gate_report(
    *,
    run_credibility: Mapping[str, Any] | None = None,
    data_quality_report: Mapping[str, Any] | None = None,
    reproducibility_report: Mapping[str, Any] | None = None,
    materialized_cache_report: Mapping[str, Any] | None = None,
    shadow_live_triangulation_report: Mapping[str, Any] | None = None,
    regime_coverage_report: Mapping[str, Any] | None = None,
    prediction_quality_report: Mapping[str, Any] | None = None,
    settlement_compatibility_report: Mapping[str, Any] | None = None,
    external_source_run_coverage_report: Mapping[str, Any] | None = None,
    external_source_missing_evidence_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Decide whether a backtest result can advance toward paper/live.

    The gate is intentionally conservative: an artifact report may be structurally
    ready while still being blocked from production promotion.
    """

    credibility = dict(run_credibility or {})
    data_quality = dict(data_quality_report or {})
    reproducibility = dict(reproducibility_report or {})
    cache = dict(materialized_cache_report or {})
    shadow_live = dict(shadow_live_triangulation_report or {})
    regime = dict(regime_coverage_report or {})
    prediction = dict(prediction_quality_report or {})
    settlement = dict(settlement_compatibility_report or {})
    external_coverage = dict(external_source_run_coverage_report or {})
    missing_evidence = dict(external_source_missing_evidence_plan or {})

    blocked_reasons: list[str] = []
    review_reasons: list[str] = []
    missing_reasons: list[str] = []

    _require_ready("run credibility", credibility.get("status"), blocked_reasons, missing_reasons)
    _require_ready("data quality", data_quality.get("quality_verdict"), blocked_reasons, missing_reasons)
    _require_ready("reproducibility", reproducibility.get("reproducibility_verdict"), blocked_reasons, missing_reasons)
    _require_ready("materialized replay cache", cache.get("cache_verdict"), blocked_reasons, missing_reasons)
    _require_ready("settlement/source compatibility", settlement.get("compatibility_verdict"), blocked_reasons, missing_reasons)

    if not shadow_live:
        missing_reasons.append("missing shadow/live triangulation report")
    else:
        triangulation = str(shadow_live.get("triangulation_verdict") or MISSING)
        suspect = bool(shadow_live.get("fill_model_suspect"))
        if suspect:
            blocked_reasons.append("fill_model_suspect=true")
        if triangulation != READY:
            blocked_reasons.append(f"shadow/live triangulation verdict={triangulation}")

    if not regime:
        missing_reasons.append("missing regime coverage report")
    else:
        coverage_verdict = str(regime.get("coverage_verdict") or MISSING)
        strategy_scope = str(regime.get("strategy_scope") or "")
        if coverage_verdict == MISSING:
            missing_reasons.append("missing regime coverage verdict")
        elif coverage_verdict != READY:
            review_reasons.append(f"regime coverage verdict={coverage_verdict}")
        if strategy_scope == "regime_specific":
            review_reasons.append("strategy_scope=regime_specific")

    if prediction:
        prediction_verdict = str(prediction.get("prediction_verdict") or MISSING)
        if prediction_verdict != READY:
            review_reasons.append(f"prediction quality verdict={prediction_verdict}")

    if not external_coverage:
        missing_reasons.append("missing external source run coverage report")
    else:
        coverage_status = str(external_coverage.get("status") or MISSING)
        if coverage_status == MISSING:
            missing_reasons.append("external source run coverage=missing")
        elif coverage_status != READY:
            blocked_reasons.append(f"external source run coverage={coverage_status}")
        if str(external_coverage.get("order_state_coverage_pct") or "0") != "100":
            blocked_reasons.append(f"order_state_coverage_pct={external_coverage.get('order_state_coverage_pct', 0)}")
        if str(external_coverage.get("calibration_coverage_pct") or "0") != "100":
            blocked_reasons.append(f"calibration_coverage_pct={external_coverage.get('calibration_coverage_pct', 0)}")

    if not missing_evidence:
        missing_reasons.append("missing external evidence work-order plan")
    else:
        missing_status = str(missing_evidence.get("status") or MISSING)
        missing_order_state = _to_int(missing_evidence.get("missing_order_state_count"))
        missing_calibration = _to_int(missing_evidence.get("missing_calibration_count"))
        if missing_status == MISSING:
            missing_reasons.append("external missing evidence plan=missing")
        elif missing_status != READY:
            blocked_reasons.append(f"external missing evidence plan={missing_status}")
        if missing_order_state:
            blocked_reasons.append(f"missing_order_state_count={missing_order_state}")
        if missing_calibration:
            blocked_reasons.append(f"missing_calibration_count={missing_calibration}")

    promotion_allowed = not blocked_reasons and not missing_reasons
    live_allowed = promotion_allowed and not review_reasons
    allowed_next_modes = ["backtest"]
    if promotion_allowed:
        allowed_next_modes.append("paper")
    if live_allowed:
        allowed_next_modes.append("live")

    if missing_reasons:
        verdict = MISSING
    elif blocked_reasons:
        verdict = "blocked"
    elif review_reasons:
        verdict = REVIEW
    else:
        verdict = READY

    return {
        "status": READY,
        "promotion_verdict": verdict,
        "production_promotion_allowed": live_allowed,
        "paper_promotion_allowed": promotion_allowed,
        "allowed_next_modes": allowed_next_modes,
        "blocked_reasons": blocked_reasons,
        "review_reasons": review_reasons,
        "missing_reasons": missing_reasons,
        "policy": {
            "name": "fill_first_shadow_live_gate_v1",
            "do_not_promote_when_fill_model_suspect": True,
            "require_shadow_live_triangulation": True,
            "require_reproducible_materialized_input": True,
            "regime_specific_requires_review_before_live": True,
            "require_run_level_external_evidence_coverage": True,
            "require_no_missing_external_evidence": True,
        },
    }


def _require_ready(name: str, status: Any, blocked_reasons: list[str], missing_reasons: list[str]) -> None:
    value = str(status or MISSING)
    if value == READY:
        return
    if value == MISSING:
        missing_reasons.append(f"{name}=missing")
        return
    blocked_reasons.append(f"{name}={value}")


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
