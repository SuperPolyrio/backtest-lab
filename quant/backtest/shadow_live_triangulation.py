"""Backtest-vs-shadow/live triangulation reports for fill-first execution."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping, Sequence

from quant.backtest.calibration import build_calibration_report
from quant.backtest.cost_calibration import build_cost_calibration_report


READY = "ready"
REVIEW = "review"
MISSING = "missing"


def build_shadow_live_triangulation_report(
    calibration_rows: Sequence[Mapping[str, Any]],
    cost_calibration_rows: Sequence[Mapping[str, Any]] | None = None,
    *,
    real_order_state_event_count: int = 0,
    external_source_state_count: int = 0,
) -> dict[str, Any]:
    fill_report = build_calibration_report(calibration_rows)
    cost_report = build_cost_calibration_report(cost_calibration_rows or [])
    fill_samples = int(_decimal(fill_report.get("sample_count")))
    cost_samples = int(_decimal(cost_report.get("sample_count")))
    evidence_count = fill_samples + cost_samples + int(real_order_state_event_count or 0)
    if evidence_count <= 0:
        return {
            "status": REVIEW,
            "triangulation_verdict": REVIEW,
            "fill_model_suspect": True,
            "reason": "no shadow/live terminal evidence attached yet",
            "backtest_sample_count": 0,
            "shadow_live_sample_count": 0,
            "cost_sample_count": 0,
            "real_order_state_event_count": int(real_order_state_event_count or 0),
            "external_source_state_count": int(external_source_state_count or 0),
            "fill_calibration": fill_report,
            "cost_calibration": cost_report,
            "drift_summary": _drift_summary(fill_report, cost_report),
            "next_actions": ["Attach shadow/live order-state events and cost events before trusting the fill model."],
        }

    drift = _drift_summary(fill_report, cost_report)
    suspect_reasons = []
    if fill_samples <= 0:
        suspect_reasons.append("no fill calibration samples")
    if cost_samples <= 0:
        suspect_reasons.append("no cost calibration samples")
    if _decimal(fill_report.get("status_error_rate")) > Decimal("0"):
        suspect_reasons.append(f"status error rate {fill_report.get('status_error_rate')}%")
    if _decimal(fill_report.get("avg_price_error")) > Decimal("0.01"):
        suspect_reasons.append(f"avg price error {fill_report.get('avg_price_error')}")
    if _decimal(fill_report.get("avg_slippage_error")) > Decimal("0.01"):
        suspect_reasons.append(f"avg slippage error {fill_report.get('avg_slippage_error')}")
    if _decimal(fill_report.get("avg_pnl_error")) > Decimal("1"):
        suspect_reasons.append(f"avg pnl error {fill_report.get('avg_pnl_error')}")
    if _decimal(fill_report.get("avg_latency_error_seconds")) > Decimal("2"):
        suspect_reasons.append(f"avg latency error {fill_report.get('avg_latency_error_seconds')}s")
    if bool(cost_report.get("requires_recalibration")):
        suspect_reasons.append("cost calibration requires recalibration")
    fill_model_suspect = bool(suspect_reasons)
    verdict = REVIEW if fill_model_suspect else READY
    return {
        "status": READY,
        "triangulation_verdict": verdict,
        "fill_model_suspect": fill_model_suspect,
        "reason": "; ".join(suspect_reasons) if suspect_reasons else "backtest fill model is within available shadow/live thresholds",
        "backtest_sample_count": fill_samples,
        "shadow_live_sample_count": fill_samples,
        "cost_sample_count": cost_samples,
        "real_order_state_event_count": int(real_order_state_event_count or 0),
        "external_source_state_count": int(external_source_state_count or 0),
        "fill_calibration": fill_report,
        "cost_calibration": cost_report,
        "drift_summary": drift,
        "next_actions": _next_actions(fill_model_suspect, fill_samples, cost_samples),
    }


def _drift_summary(fill_report: Mapping[str, Any], cost_report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status_error_rate": str(fill_report.get("status_error_rate") or "0"),
        "avg_price_error": str(fill_report.get("avg_price_error") or "0"),
        "max_price_error": str(fill_report.get("max_price_error") or "0"),
        "avg_size_error": str(fill_report.get("avg_size_error") or "0"),
        "avg_slippage_error": str(fill_report.get("avg_slippage_error") or "0"),
        "avg_fee_error": str(fill_report.get("avg_fee_error") or "0"),
        "avg_rebate_error": str(fill_report.get("avg_rebate_error") or "0"),
        "avg_cash_error": str(fill_report.get("avg_cash_error") or "0"),
        "avg_position_error": str(fill_report.get("avg_position_error") or "0"),
        "avg_pnl_error": str(fill_report.get("avg_pnl_error") or "0"),
        "max_pnl_error": str(fill_report.get("max_pnl_error") or "0"),
        "avg_latency_error_seconds": str(fill_report.get("avg_latency_error_seconds") or "0"),
        "total_cost_amount_error": str(cost_report.get("total_amount_error") or "0"),
        "avg_cost_amount_error": str(cost_report.get("avg_amount_error") or "0"),
        "missing_live_cost_count": int(_decimal(cost_report.get("missing_live_count"))),
        "missing_simulated_cost_count": int(_decimal(cost_report.get("missing_simulated_count"))),
    }


def _next_actions(fill_model_suspect: bool, fill_samples: int, cost_samples: int) -> list[str]:
    actions: list[str] = []
    if fill_samples <= 0:
        actions.append("Collect terminal shadow/live order-state events and build calibration samples.")
    if cost_samples <= 0:
        actions.append("Import real fee/rebate/gas/redeem/capital cost events for the same run.")
    if fill_model_suspect:
        actions.append("Review fill probability, no-fill, slippage, latency, fee, rebate, and PnL attribution assumptions before promoting the strategy.")
    if not actions:
        actions.append("Keep monitoring shadow/live drift for this strategy and execution profile.")
    return actions


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")
