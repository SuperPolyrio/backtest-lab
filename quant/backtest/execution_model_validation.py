"""Validation reports for execution-model comparison and L2 sensitivity."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"

MODEL_OHLCV = "ohlcv_close"
MODEL_FORMULA = "formula_slippage"
MODEL_L2 = "l2_orderfilled"

REQUIRED_MODELS = (MODEL_OHLCV, MODEL_FORMULA, MODEL_L2)
REQUIRED_L2_PROFILES = ("conservative", "realistic", "optimistic")
REGIME_DIMENSIONS = (
    "market_category",
    "liquidity_bucket",
    "time_to_expiry_bucket",
    "volatility_bucket",
    "final_minute",
    "event_outcome_count_bucket",
)


def build_execution_model_validation_report(
    rows: Sequence[Any],
    *,
    required_models: Sequence[str] = REQUIRED_MODELS,
    required_l2_profiles: Sequence[str] = REQUIRED_L2_PROFILES,
    pnl_outperformance_review_threshold: Decimal | str = Decimal("0"),
    fill_rate_review_tolerance: Decimal | str = Decimal("0.02"),
    min_regime_buckets: int = 2,
) -> dict[str, Any]:
    """Build a DB-free report for guidance sections 13.7, 13.8 and Phase 3."""
    normalized = [_normalize_row(row) for row in rows]
    normalized = [row for row in normalized if row["model"]]
    if not normalized:
        return {
            "status": MISSING,
            "validation_verdict": MISSING,
            "reason": "no execution model rows available",
            "required_models": list(required_models),
            "present_models": [],
            "missing_models": list(required_models),
            "model_summaries": {},
            "model_comparison": {},
            "l2_profile_sensitivity": {},
            "capacity_curve": [],
            "strategy_attribution": [],
            "regime_stress": {},
            "next_actions": ["Run the same strategy through OHLCV, formula slippage, and L2 + OrderFilled execution models."],
        }

    grouped_models: dict[str, list[dict[str, Any]]] = {}
    for row in normalized:
        grouped_models.setdefault(row["model"], []).append(row)
    model_summaries = {model: _summary(model, bucket) for model, bucket in sorted(grouped_models.items())}
    required = [_canonical_model(model) for model in required_models]
    present = sorted(grouped_models)
    missing = [model for model in required if model not in grouped_models]
    model_comparison = _model_comparison(model_summaries)
    l2_rows = grouped_models.get(MODEL_L2, [])
    l2_profile_sensitivity = _l2_profile_sensitivity(l2_rows, required_l2_profiles=required_l2_profiles)
    capacity_curve = _capacity_curve(l2_rows)
    strategy_attribution = _strategy_attribution(normalized)
    regime_stress = _regime_stress(normalized, min_regime_buckets=max(1, int(min_regime_buckets)))
    review_reasons = _review_reasons(
        missing=missing,
        model_comparison=model_comparison,
        l2_profile_sensitivity=l2_profile_sensitivity,
        regime_stress=regime_stress,
        pnl_outperformance_review_threshold=_decimal(pnl_outperformance_review_threshold),
        fill_rate_review_tolerance=_decimal(fill_rate_review_tolerance),
    )
    verdict = READY if not review_reasons else REVIEW
    return {
        "status": READY,
        "validation_verdict": verdict,
        "reason": "execution model validation covers baselines, L2 profiles, capacity and regimes" if not review_reasons else "; ".join(review_reasons),
        "required_models": required,
        "present_models": present,
        "missing_models": missing,
        "model_summaries": model_summaries,
        "model_comparison": model_comparison,
        "l2_profile_sensitivity": l2_profile_sensitivity,
        "capacity_curve": capacity_curve,
        "strategy_attribution": strategy_attribution,
        "regime_stress": regime_stress,
        "next_actions": _next_actions(verdict, missing, l2_profile_sensitivity, regime_stress),
    }


def execution_model_validation_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Execution Model Validation: {report.get('validation_verdict') or report.get('status')}",
        "",
        f"- reason: {report.get('reason', '-')}",
        f"- present_models: {', '.join(report.get('present_models') or []) or '-'}",
        f"- missing_models: {', '.join(report.get('missing_models') or []) or '-'}",
        "",
        "## Model Comparison",
        "",
        "| Model | Rows | Submitted | Fill Rate | Partial Rate | Avg Slippage | Avg Queue Wait | Net PnL |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    summaries = report.get("model_summaries") if isinstance(report.get("model_summaries"), Mapping) else {}
    for model, summary in summaries.items():
        if not isinstance(summary, Mapping):
            continue
        lines.append(
            "| "
            + " | ".join(
                [
                    _text(model),
                    _text(summary.get("row_count")),
                    _text(summary.get("submitted_count")),
                    _text(summary.get("fill_rate")),
                    _text(summary.get("partial_fill_rate")),
                    _text(summary.get("avg_slippage")),
                    _text(summary.get("avg_queue_wait_seconds")),
                    _text(summary.get("net_pnl")),
                ]
            )
            + " |"
        )
    lines.extend(["", "## L2 Profile Sensitivity", ""])
    sensitivity = report.get("l2_profile_sensitivity") if isinstance(report.get("l2_profile_sensitivity"), Mapping) else {}
    lines.append(f"- monotonic_fill_rate_ok: {sensitivity.get('monotonic_fill_rate_ok', '-')}")
    lines.append(f"- missing_profiles: {', '.join(sensitivity.get('missing_profiles') or []) or '-'}")
    lines.append(f"- execution_sensitivity_flag: {sensitivity.get('execution_sensitivity_flag', '-')}")
    lines.extend(["", "## Capacity Curve", "", "| Capacity Bucket | Samples | Fill Rate | Avg Slippage | Net PnL |", "| --- | ---: | ---: | ---: | ---: |"])
    for bucket in report.get("capacity_curve") or []:
        if not isinstance(bucket, Mapping):
            continue
        lines.append(
            "| "
            + " | ".join(
                [
                    _text(bucket.get("capacity_bucket")),
                    _text(bucket.get("sample_count")),
                    _text(bucket.get("fill_rate")),
                    _text(bucket.get("avg_slippage")),
                    _text(bucket.get("net_pnl")),
                ]
            )
            + " |"
        )
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def _summary(name: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    submitted = sum((_decimal(row.get("submitted_count")) for row in rows), Decimal("0"))
    filled = sum((_decimal(row.get("filled_count")) for row in rows), Decimal("0"))
    partial = sum((_decimal(row.get("partial_fill_count")) for row in rows), Decimal("0"))
    unfilled_cancelled = sum((_decimal(row.get("unfilled_cancelled_count")) for row in rows), Decimal("0"))
    slippages = [_decimal(row.get("avg_slippage")) for row in rows]
    queue_waits = [_decimal(row.get("queue_wait_seconds")) for row in rows]
    pnl = sum((_decimal(row.get("net_pnl")) for row in rows), Decimal("0"))
    return {
        "name": name,
        "row_count": len(rows),
        "submitted_count": _decimal_text(submitted),
        "filled_count": _decimal_text(filled),
        "partial_fill_count": _decimal_text(partial),
        "unfilled_cancelled_count": _decimal_text(unfilled_cancelled),
        "fill_rate": _decimal_text(_ratio(filled, submitted)),
        "partial_fill_rate": _decimal_text(_ratio(partial, submitted)),
        "unfilled_cancelled_ratio": _decimal_text(_ratio(unfilled_cancelled, submitted)),
        "avg_slippage": _decimal_text(_avg(slippages)),
        "avg_queue_wait_seconds": _decimal_text(_avg(queue_waits)),
        "net_pnl": _decimal_text(pnl),
    }


def _model_comparison(summaries: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    l2 = summaries.get(MODEL_L2)
    if not l2:
        return {"status": MISSING, "reason": "missing l2_orderfilled model"}
    baselines = [summary for model, summary in summaries.items() if model in {MODEL_OHLCV, MODEL_FORMULA}]
    if not baselines:
        return {"status": REVIEW, "reason": "missing coarse baseline models"}
    max_baseline_fill = max(_decimal(summary.get("fill_rate")) for summary in baselines)
    max_baseline_pnl = max(_decimal(summary.get("net_pnl")) for summary in baselines)
    l2_fill = _decimal(l2.get("fill_rate"))
    l2_pnl = _decimal(l2.get("net_pnl"))
    return {
        "status": READY,
        "l2_fill_rate": _decimal_text(l2_fill),
        "max_baseline_fill_rate": _decimal_text(max_baseline_fill),
        "l2_fill_rate_delta_vs_max_baseline": _decimal_text(l2_fill - max_baseline_fill),
        "l2_net_pnl": _decimal_text(l2_pnl),
        "max_baseline_net_pnl": _decimal_text(max_baseline_pnl),
        "l2_pnl_delta_vs_max_baseline": _decimal_text(l2_pnl - max_baseline_pnl),
        "l2_more_optimistic_than_baseline": l2_fill > max_baseline_fill or l2_pnl > max_baseline_pnl,
    }


def _l2_profile_sensitivity(rows: Sequence[Mapping[str, Any]], *, required_l2_profiles: Sequence[str]) -> dict[str, Any]:
    by_profile: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        profile = str(row.get("execution_profile") or "").lower()
        if profile:
            by_profile.setdefault(profile, []).append(row)
    required = [str(profile).lower() for profile in required_l2_profiles]
    missing = [profile for profile in required if profile not in by_profile]
    summaries = {profile: _summary(profile, bucket) for profile, bucket in sorted(by_profile.items())}
    ordered = [summaries.get(profile) for profile in required if profile in summaries]
    fill_rates = [_decimal(summary.get("fill_rate")) for summary in ordered if summary]
    monotonic = len(fill_rates) < 2 or all(left <= right for left, right in zip(fill_rates, fill_rates[1:]))
    realistic = summaries.get("realistic")
    optimistic = summaries.get("optimistic")
    conservative = summaries.get("conservative")
    sensitivity_flag = False
    if optimistic and conservative:
        sensitivity_flag = _decimal(optimistic.get("net_pnl")) > 0 and _decimal(conservative.get("net_pnl")) < 0
    if realistic and conservative:
        sensitivity_flag = sensitivity_flag or (_decimal(realistic.get("net_pnl")) > 0 and _decimal(conservative.get("net_pnl")) < 0)
    return {
        "status": READY if not missing else REVIEW,
        "required_profiles": required,
        "present_profiles": sorted(by_profile),
        "missing_profiles": missing,
        "profiles": summaries,
        "monotonic_fill_rate_ok": monotonic,
        "execution_sensitivity_flag": sensitivity_flag,
    }


def _capacity_curve(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        bucket = _capacity_bucket(row)
        grouped.setdefault(bucket, []).append(row)
    order = ("lte_10pct", "lte_25pct", "lte_50pct", "lte_100pct", "gt_100pct", "unknown")
    output: list[dict[str, Any]] = []
    for bucket in order:
        items = grouped.get(bucket, [])
        if not items:
            continue
        summary = _summary(bucket, items)
        output.append(
            {
                "capacity_bucket": bucket,
                "sample_count": len(items),
                "fill_rate": summary["fill_rate"],
                "partial_fill_rate": summary["partial_fill_rate"],
                "avg_slippage": summary["avg_slippage"],
                "avg_queue_wait_seconds": summary["avg_queue_wait_seconds"],
                "net_pnl": summary["net_pnl"],
            }
        )
    return output


def _strategy_attribution(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        key = str(row.get("strategy_name") or "unknown")
        grouped.setdefault(key, []).append(row)
    output: list[dict[str, Any]] = []
    for strategy, items in grouped.items():
        summary = _summary(strategy, items)
        l2_items = [row for row in items if row.get("model") == MODEL_L2]
        baseline_items = [row for row in items if row.get("model") in {MODEL_OHLCV, MODEL_FORMULA}]
        l2_pnl = sum((_decimal(row.get("net_pnl")) for row in l2_items), Decimal("0"))
        baseline_pnl = sum((_decimal(row.get("net_pnl")) for row in baseline_items), Decimal("0"))
        output.append(
            {
                "strategy_name": strategy,
                "sample_count": len(items),
                "fill_rate": summary["fill_rate"],
                "net_pnl": summary["net_pnl"],
                "l2_pnl_delta_vs_baselines": _decimal_text(l2_pnl - baseline_pnl),
                "execution_attribution": "execution_sensitive" if l2_items and baseline_items and l2_pnl < baseline_pnl else "baseline_or_unclassified",
            }
        )
    return sorted(output, key=lambda row: (-int(row["sample_count"]), row["strategy_name"]))


def _regime_stress(rows: Sequence[Mapping[str, Any]], *, min_regime_buckets: int) -> dict[str, Any]:
    dimensions: dict[str, dict[str, Any]] = {}
    narrow: list[str] = []
    for dimension in REGIME_DIMENSIONS:
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            bucket = str(row.get(dimension) or "unknown")
            grouped.setdefault(bucket, []).append(row)
        bucket_rows = []
        known = 0
        for bucket, items in grouped.items():
            if bucket != "unknown":
                known += 1
            summary = _summary(bucket, items)
            bucket_rows.append(
                {
                    "bucket": bucket,
                    "sample_count": len(items),
                    "fill_rate": summary["fill_rate"],
                    "partial_fill_rate": summary["partial_fill_rate"],
                    "avg_slippage": summary["avg_slippage"],
                    "avg_queue_wait_seconds": summary["avg_queue_wait_seconds"],
                    "unfilled_cancelled_ratio": summary["unfilled_cancelled_ratio"],
                    "net_pnl": summary["net_pnl"],
                }
            )
        if known < min_regime_buckets:
            narrow.append(dimension)
        dimensions[dimension] = {
            "known_bucket_count": known,
            "status": READY if known >= min_regime_buckets else REVIEW,
            "buckets": sorted(bucket_rows, key=lambda row: (-int(row["sample_count"]), str(row["bucket"]))),
        }
    return {
        "status": READY if not narrow else REVIEW,
        "narrow_dimensions": narrow,
        "dimensions": dimensions,
    }


def _review_reasons(
    *,
    missing: Sequence[str],
    model_comparison: Mapping[str, Any],
    l2_profile_sensitivity: Mapping[str, Any],
    regime_stress: Mapping[str, Any],
    pnl_outperformance_review_threshold: Decimal,
    fill_rate_review_tolerance: Decimal,
) -> list[str]:
    reasons: list[str] = []
    if missing:
        reasons.append("missing execution models: " + ", ".join(missing))
    if model_comparison.get("status") != READY:
        reasons.append(str(model_comparison.get("reason") or "model comparison missing"))
    l2_delta_fill = _decimal(model_comparison.get("l2_fill_rate_delta_vs_max_baseline"))
    l2_delta_pnl = _decimal(model_comparison.get("l2_pnl_delta_vs_max_baseline"))
    if l2_delta_fill > fill_rate_review_tolerance:
        reasons.append(f"L2 fill rate exceeds coarse baselines by {l2_delta_fill}")
    if l2_delta_pnl > pnl_outperformance_review_threshold:
        reasons.append(f"L2 PnL exceeds coarse baselines by {l2_delta_pnl}; check look-ahead or optimistic queue assumptions")
    if l2_profile_sensitivity.get("missing_profiles"):
        reasons.append("missing L2 profiles: " + ", ".join(l2_profile_sensitivity.get("missing_profiles") or []))
    if l2_profile_sensitivity.get("monotonic_fill_rate_ok") is False:
        reasons.append("L2 conservative <= realistic <= optimistic fill-rate monotonicity failed")
    if l2_profile_sensitivity.get("execution_sensitivity_flag"):
        reasons.append("strategy only works under less conservative execution assumptions")
    if regime_stress.get("status") != READY:
        reasons.append("regime stress coverage too narrow: " + ", ".join(regime_stress.get("narrow_dimensions") or []))
    return reasons


def _next_actions(
    verdict: str,
    missing: Sequence[str],
    l2_profile_sensitivity: Mapping[str, Any],
    regime_stress: Mapping[str, Any],
) -> list[str]:
    actions: list[str] = []
    if missing:
        actions.append("Run missing execution baselines: " + ", ".join(missing))
    if l2_profile_sensitivity.get("missing_profiles"):
        actions.append("Run L2 execution profiles: " + ", ".join(l2_profile_sensitivity.get("missing_profiles") or []))
    if l2_profile_sensitivity.get("monotonic_fill_rate_ok") is False:
        actions.append("Review L2 profile configs; conservative fill rate should not exceed optimistic.")
    if regime_stress.get("narrow_dimensions"):
        actions.append("Expand stress coverage across regimes: " + ", ".join(regime_stress.get("narrow_dimensions") or []))
    if not actions and verdict == READY:
        actions.append("Keep A/B/C execution-model comparison attached to promotion artifacts.")
    return actions or ["Review execution validation warnings before promoting the strategy."]


def _normalize_row(row: Any) -> dict[str, Any]:
    plain = _plain(row)
    payload = _mapping(plain.get("payload"))
    params = _mapping(plain.get("parameters")) or _mapping(payload.get("parameters"))
    context = _mapping(plain.get("context")) or _mapping(payload.get("context"))
    combined = {**payload, **params, **context, **plain}
    submitted = _decimal(_first(combined, "submitted_count", "submittedCount", "signal_count", "signalCount", "orders", "trades"))
    filled = _decimal(_first(combined, "filled_count", "filledCount", "filled_orders", "filledOrders", "trades"))
    partial = _decimal(_first(combined, "partial_fill_count", "partialFillCount", "partial_count", "partialCount"))
    unfilled = _decimal(_first(combined, "unfilled_cancelled_count", "unfilledCancelledCount", "no_fill_count", "noFillCount", "cancelled_count", "cancelledCount"))
    if submitted <= 0:
        submitted = max(filled + unfilled, Decimal("1"))
    visible_depth = _decimal(_first(combined, "visible_depth", "visibleDepth", "available_depth", "availableDepth", "available_notional", "availableNotional"))
    requested_size = _decimal(_first(combined, "requested_size", "requestedSize", "order_size", "orderSize", "size", "notional"))
    capacity_ratio = _optional_decimal(_first(combined, "capacity_ratio", "capacityRatio"))
    if capacity_ratio is None and visible_depth > 0 and requested_size > 0:
        capacity_ratio = requested_size / visible_depth
    return {
        "model": _canonical_model(_first(combined, "execution_model", "executionModel", "execution_mode_family", "executionModeFamily", "model", "engine")),
        "execution_profile": str(_first(combined, "execution_profile", "executionProfile", "profile") or "").strip().lower(),
        "strategy_name": str(_first(combined, "strategy_name", "strategyName", "strategy") or "unknown"),
        "submitted_count": submitted,
        "filled_count": filled,
        "partial_fill_count": partial,
        "unfilled_cancelled_count": unfilled,
        "avg_slippage": _decimal(_first(combined, "avg_slippage", "avgSlippage", "slippage", "slippage_cost", "slippageCost")),
        "queue_wait_seconds": _decimal(_first(combined, "queue_wait_seconds", "queueWaitSeconds", "avg_queue_wait_seconds", "avgQueueWaitSeconds")),
        "net_pnl": _decimal(_first(combined, "net_pnl", "netPnl", "total_pnl", "totalPnl", "pnl", "netProfit")),
        "requested_size": requested_size,
        "visible_depth": visible_depth,
        "capacity_ratio": capacity_ratio,
        "market_category": str(_first(combined, "market_category", "marketCategory", "category", "event_category", "eventCategory") or "unknown"),
        "liquidity_bucket": str(_first(combined, "liquidity_bucket", "liquidityBucket") or "unknown"),
        "time_to_expiry_bucket": str(_first(combined, "time_to_expiry_bucket", "timeToExpiryBucket") or "unknown"),
        "volatility_bucket": str(_first(combined, "volatility_bucket", "volatilityBucket") or "unknown"),
        "final_minute": str(_first(combined, "final_minute", "finalMinute") or "unknown"),
        "event_outcome_count_bucket": str(_first(combined, "event_outcome_count_bucket", "eventOutcomeCountBucket") or "unknown"),
    }


def _canonical_model(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if not text:
        return ""
    if text in {"ohlcv", "close", "close_price", "block_close", "ohlcv_close", "block_bar"}:
        return MODEL_OHLCV
    if text in {"formula", "formula_slippage", "price_history", "price_history_slippage", "frontend_price_history"}:
        return MODEL_FORMULA
    if text in {"l2", "depth", "lob", "orderfilled_lob", "l2_orderfilled", "orderfilled_depth", "built_in", "builtin"}:
        return MODEL_L2
    return text


def _capacity_bucket(row: Mapping[str, Any]) -> str:
    ratio = row.get("capacity_ratio")
    if ratio is None:
        return "unknown"
    value = _decimal(ratio)
    if value <= Decimal("0.10"):
        return "lte_10pct"
    if value <= Decimal("0.25"):
        return "lte_25pct"
    if value <= Decimal("0.50"):
        return "lte_50pct"
    if value <= Decimal("1"):
        return "lte_100pct"
    return "gt_100pct"


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _plain(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, Mapping):
        return {str(key): _plain_value(inner) for key, inner in value.items()}
    attrs = {
        key: getattr(value, key)
        for key in dir(value)
        if not key.startswith("_") and not callable(getattr(value, key, None))
    }
    return {str(key): _plain_value(inner) for key, inner in attrs.items()}


def _plain_value(value: Any) -> Any:
    if is_dataclass(value):
        return _plain_value(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _plain_value(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_value(item) for item in value]
    return value


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value)


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _avg(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return numerator / denominator


def _decimal_text(value: Any) -> str:
    decimal = _decimal(value).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    text = format(decimal.normalize(), "f")
    return text if text != "-0" else "0"


def _text(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|")
