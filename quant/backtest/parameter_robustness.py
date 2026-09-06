"""Parameter robustness and overfit guard reports for fill-first backtests."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal, ROUND_CEILING
from typing import Any, Iterable, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"

DEFAULT_PARAMETER_FIELDS = (
    "entry_threshold",
    "exit_threshold",
    "position_size",
    "execution_profile",
    "order_role",
    "latency_blocks",
    "adverse_slippage_cents",
    "fill_probability_haircut_pct",
    "liquidity_cap_pct",
    "final_valuation_mode",
)

DEFAULT_REGIME_FIELDS = (
    "market_category",
    "liquidity_bucket",
    "volatility_bucket",
    "time_to_expiry_bucket",
    "event_outcome_count_bucket",
    "data_quality",
)


def build_parameter_robustness_report(
    rows: Sequence[Any],
    *,
    parameter_fields: Sequence[str] = DEFAULT_PARAMETER_FIELDS,
    regime_fields: Sequence[str] = DEFAULT_REGIME_FIELDS,
    min_runs: int = 5,
    min_parameter_sets: int = 3,
) -> dict[str, Any]:
    """Build a report that blocks best-only parameter promotion.

    The input is intentionally generic so it can consume benchmark rows, parameter
    search exports, split/walk-forward rows, or persisted run summaries.
    """

    normalized = [_normalize_row(row, index=index) for index, row in enumerate(rows, start=1)]
    normalized = [row for row in normalized if row["has_score"]]
    if not normalized:
        return {
            "status": MISSING,
            "robustness_verdict": MISSING,
            "reason": "no scored parameter runs available",
            "run_count": 0,
            "parameter_set_count": 0,
            "best_run": None,
            "median_run": None,
            "worst_decile": [],
            "parameter_sensitivity": [],
            "regime_split_performance": {},
            "production_parameter_policy": _production_policy(False, False, False, "no scored parameter runs available"),
            "next_actions": ["Run a parameter scan, split test, or walk-forward batch before selecting parameters."],
        }

    sorted_rows = sorted(normalized, key=lambda item: (item["score"], item["net_pnl"]), reverse=True)
    score_sorted_asc = list(reversed(sorted_rows))
    parameter_set_count = len({row["parameter_fingerprint"] for row in normalized})
    train_test_present = _has_train_test_evidence(normalized)
    walk_forward_present = any(row["evidence_mode"] == "walk_forward" for row in normalized)
    sensitivity = _build_parameter_sensitivity(normalized, parameter_fields)
    regime = _build_regime_split(normalized, regime_fields)
    enough_runs = len(normalized) >= int(min_runs)
    enough_parameters = parameter_set_count >= int(min_parameter_sets)
    has_sensitivity = bool(sensitivity)
    has_regime = any(regime.values())
    can_promote = enough_runs and enough_parameters and has_sensitivity and has_regime and train_test_present

    review_reasons: list[str] = []
    if not enough_runs:
        review_reasons.append(f"only {len(normalized)} scored runs; need at least {min_runs}")
    if not enough_parameters:
        review_reasons.append(f"only {parameter_set_count} parameter sets; need at least {min_parameter_sets}")
    if not has_sensitivity:
        review_reasons.append("parameter sensitivity could not be measured")
    if not has_regime:
        review_reasons.append("regime split performance could not be measured")
    if not train_test_present:
        review_reasons.append("train/test or walk-forward evidence is missing")

    verdict = READY if can_promote else REVIEW
    reason = "parameter scan has enough cross-parameter and split evidence" if can_promote else "; ".join(review_reasons)
    return {
        "status": READY,
        "robustness_verdict": verdict,
        "reason": reason,
        "run_count": len(normalized),
        "parameter_set_count": parameter_set_count,
        "train_test_present": train_test_present,
        "walk_forward_present": walk_forward_present,
        "best_run": _public_row(sorted_rows[0]),
        "median_run": _public_row(_percentile_row(score_sorted_asc, Decimal("0.50"))),
        "worst_decile": [_public_row(row) for row in score_sorted_asc[:_tail_count(score_sorted_asc, Decimal("0.10"))]],
        "parameter_sensitivity": sensitivity,
        "regime_split_performance": regime,
        "production_parameter_policy": _production_policy(
            can_promote,
            train_test_present,
            walk_forward_present,
            "best parameter may be promoted only after robustness review" if can_promote else "best-only parameter promotion is blocked",
        ),
        "next_actions": _next_actions(verdict, train_test_present, walk_forward_present),
    }


def _normalize_row(row: Any, *, index: int) -> dict[str, Any]:
    plain = _as_plain(row)
    payload = _as_mapping(plain.get("payload"))
    parameters = _first_mapping(plain, payload, "parameters", "parameter_snapshot", "parameterSnapshot", "strategy_parameters", "strategyParameters")
    context = _first_mapping(plain, payload, "context", "meta", "execution_context", "executionContext")
    combined = {**payload, **context, **parameters, **plain}
    net_pnl = _first_decimal(
        combined,
        "net_pnl",
        "netPnl",
        "total_pnl",
        "totalPnl",
        "accurate_pnl",
        "accuratePnl",
        "pnl",
        "netProfit",
        "settlement_pnl",
        "settlementPnl",
    )
    max_drawdown = abs(_first_decimal(combined, "max_drawdown", "maxDrawdown", "drawdown", "drawdownPnl"))
    score = _first_optional_decimal(combined, "performance_score", "performanceScore", "score", "objective", "robustness_score")
    if score is None:
        score = net_pnl - max_drawdown
    run_id = _first_text(combined, "run_id", "runId", "benchmark_id", "benchmarkId", "key") or f"row-{index}"
    fingerprint = (
        _first_text(combined, "parameter_fingerprint", "parameterFingerprint")
        or _fingerprint_subset(combined, DEFAULT_PARAMETER_FIELDS)
        or f"row-{index}"
    )
    filled = _first_decimal(combined, "filled_count", "filledCount", "trades")
    submitted = _first_decimal(combined, "submitted_count", "submittedCount", "signal_count", "signalCount")
    fill_rate = _first_optional_decimal(combined, "fill_rate", "fillRate")
    if fill_rate is None:
        fill_rate = filled / submitted if submitted > 0 else Decimal("0")
    evidence_mode = _evidence_mode(combined)
    return {
        "source": plain,
        "run_id": run_id,
        "market_slug": _first_text(combined, "market_slug", "marketSlug"),
        "title": _first_text(combined, "title"),
        "parameter_fingerprint": fingerprint,
        "parameters": {field: combined.get(field) for field in DEFAULT_PARAMETER_FIELDS if not _is_blank(combined.get(field))},
        "regime": {field: combined.get(field) for field in DEFAULT_REGIME_FIELDS if not _is_blank(combined.get(field))},
        "net_pnl": net_pnl,
        "max_drawdown": max_drawdown,
        "score": score,
        "fill_rate": fill_rate,
        "sample_count": int(_first_decimal(combined, "sample_count", "sampleCount", "trades", "signal_count", "signalCount")),
        "evidence_mode": evidence_mode,
        "has_score": not _is_blank(_first_present(combined, "performance_score", "performanceScore", "net_pnl", "netPnl", "total_pnl", "totalPnl", "accurate_pnl", "accuratePnl", "pnl", "netProfit", "score", "objective")),
    }


def _build_parameter_sensitivity(rows: Sequence[Mapping[str, Any]], parameter_fields: Sequence[str]) -> list[dict[str, Any]]:
    sensitivity: list[dict[str, Any]] = []
    for field in parameter_fields:
        buckets: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            value = _as_mapping(row.get("parameters")).get(field)
            if _is_blank(value):
                continue
            buckets.setdefault(str(value), []).append(row)
        if len(buckets) < 2:
            continue
        bucket_rows = [_bucket_summary(value, bucket) for value, bucket in sorted(buckets.items())]
        scores = [_decimal(item["avg_score"]) for item in bucket_rows]
        pnls = [_decimal(item["avg_net_pnl"]) for item in bucket_rows]
        sensitivity.append(
            {
                "parameter": field,
                "value_count": len(bucket_rows),
                "best_value": max(bucket_rows, key=lambda item: _decimal(item["avg_score"]))["value"],
                "worst_value": min(bucket_rows, key=lambda item: _decimal(item["avg_score"]))["value"],
                "score_range": _decimal_text(max(scores) - min(scores)),
                "net_pnl_range": _decimal_text(max(pnls) - min(pnls)),
                "buckets": bucket_rows,
            }
        )
    return sensitivity


def _build_regime_split(rows: Sequence[Mapping[str, Any]], regime_fields: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for field in regime_fields:
        buckets: dict[str, list[Mapping[str, Any]]] = {}
        for row in rows:
            value = _as_mapping(row.get("regime")).get(field)
            if _is_blank(value):
                continue
            buckets.setdefault(str(value), []).append(row)
        if buckets:
            output[field] = [_bucket_summary(value, bucket) for value, bucket in sorted(buckets.items())]
    return output


def _bucket_summary(value: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    scores = [_decimal(row.get("score")) for row in rows]
    pnls = [_decimal(row.get("net_pnl")) for row in rows]
    fill_rates = [_decimal(row.get("fill_rate")) for row in rows]
    return {
        "value": value,
        "count": len(rows),
        "avg_score": _decimal_text(sum(scores, Decimal("0")) / Decimal(len(scores))),
        "avg_net_pnl": _decimal_text(sum(pnls, Decimal("0")) / Decimal(len(pnls))),
        "median_net_pnl": _decimal_text(_percentile(sorted(pnls), Decimal("0.50"))),
        "min_net_pnl": _decimal_text(min(pnls)),
        "max_net_pnl": _decimal_text(max(pnls)),
        "avg_fill_rate": _decimal_text(sum(fill_rates, Decimal("0")) / Decimal(len(fill_rates))),
    }


def _public_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "run_id": row.get("run_id"),
        "market_slug": row.get("market_slug"),
        "title": row.get("title"),
        "parameter_fingerprint": row.get("parameter_fingerprint"),
        "parameters": row.get("parameters") or {},
        "regime": row.get("regime") or {},
        "net_pnl": _decimal_text(_decimal(row.get("net_pnl"))),
        "max_drawdown": _decimal_text(_decimal(row.get("max_drawdown"))),
        "score": _decimal_text(_decimal(row.get("score"))),
        "fill_rate": _decimal_text(_decimal(row.get("fill_rate"))),
        "sample_count": int(row.get("sample_count") or 0),
        "evidence_mode": row.get("evidence_mode"),
    }


def _production_policy(can_promote: bool, train_test_present: bool, walk_forward_present: bool, reason: str) -> dict[str, Any]:
    return {
        "best_parameter_promotion_allowed": bool(can_promote),
        "default_action": "review_and_stage" if can_promote else "do_not_promote_best_only",
        "requires_train_test_or_walk_forward": True,
        "train_test_present": bool(train_test_present),
        "walk_forward_present": bool(walk_forward_present),
        "reason": reason,
    }


def _next_actions(verdict: str, train_test_present: bool, walk_forward_present: bool) -> list[str]:
    if verdict == READY:
        return ["Review the best, median, worst-decile, sensitivity, and regime split before staging parameters."]
    actions = ["Do not promote the best parameter set directly to production."]
    if not train_test_present:
        actions.append("Run a train/test split or include split labels in the parameter scan.")
    if not walk_forward_present:
        actions.append("Run a walk-forward batch before treating the parameter edge as stable.")
    actions.append("Expand the scan across more markets, parameter fingerprints, and regimes.")
    return actions


def _has_train_test_evidence(rows: Sequence[Mapping[str, Any]]) -> bool:
    modes = {str(row.get("evidence_mode") or "") for row in rows}
    return "walk_forward" in modes or ("train" in modes and "test" in modes) or "split" in modes


def _evidence_mode(row: Mapping[str, Any]) -> str:
    raw = str(_first_present(row, "evidence_mode", "evidenceMode", "split", "sample", "phase", "mode", "run_type", "runType") or "").lower()
    if "walk" in raw or raw in {"wf", "walk_forward"}:
        return "walk_forward"
    if "train" in raw:
        return "train"
    if "test" in raw or "holdout" in raw:
        return "test"
    if "split" in raw:
        return "split"
    return "unsplit"


def _percentile_row(sorted_rows_asc: Sequence[Mapping[str, Any]], q: Decimal) -> Mapping[str, Any]:
    if not sorted_rows_asc:
        return {}
    index = int((q * Decimal(len(sorted_rows_asc) - 1)).to_integral_value(rounding=ROUND_CEILING))
    return sorted_rows_asc[max(0, min(index, len(sorted_rows_asc) - 1))]


def _tail_count(rows: Sequence[Any], fraction: Decimal) -> int:
    if not rows:
        return 0
    return max(1, int((Decimal(len(rows)) * fraction).to_integral_value(rounding=ROUND_CEILING)))


def _percentile(sorted_values: Sequence[Decimal], q: Decimal) -> Decimal:
    if not sorted_values:
        return Decimal("0")
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = q * Decimal(len(sorted_values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - Decimal(lower)
    return sorted_values[lower] * (Decimal("1") - weight) + sorted_values[upper] * weight


def _first_mapping(*items: Any) -> dict[str, Any]:
    sources = [item for item in items if isinstance(item, Mapping)]
    keys = [str(item) for item in items if not isinstance(item, Mapping)]
    for source in sources:
        for key in keys:
            value = source.get(key)
            mapping = _as_mapping(value)
            if mapping:
                return mapping
    return {}


def _as_plain(value: Any) -> dict[str, Any]:
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
    if isinstance(value, Decimal):
        return str(value)
    return value


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and not _is_blank(row.get(key)):
            return row.get(key)
    return None


def _first_text(row: Mapping[str, Any], *keys: str) -> str | None:
    value = _first_present(row, *keys)
    return None if _is_blank(value) else str(value)


def _first_decimal(row: Mapping[str, Any], *keys: str) -> Decimal:
    value = _first_present(row, *keys)
    return _decimal(value)


def _first_optional_decimal(row: Mapping[str, Any], *keys: str) -> Decimal | None:
    value = _first_present(row, *keys)
    if _is_blank(value):
        return None
    return _decimal(value)


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _decimal_text(value: Decimal | int | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else _decimal(value)
    return format(decimal_value.normalize(), "f")


def _fingerprint_subset(row: Mapping[str, Any], fields: Iterable[str]) -> str:
    parts = [f"{field}={row.get(field)}" for field in fields if not _is_blank(row.get(field))]
    return "|".join(parts)


def _is_blank(value: Any) -> bool:
    return value is None or value == "" or value == {}
