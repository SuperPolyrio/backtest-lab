"""Execution profile comparison report for fill-first backtests."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"

DEFAULT_REQUIRED_PROFILES = ("realistic", "conservative")


def build_execution_profile_matrix_report(
    rows: Sequence[Any],
    *,
    required_profiles: Sequence[str] = DEFAULT_REQUIRED_PROFILES,
    baseline_profile: str = "realistic",
    max_conservative_degradation_pct: Decimal = Decimal("50"),
) -> dict[str, Any]:
    """Compare strategy results across execution profiles.

    This report is intentionally generic so benchmark profile rows, exported
    run summaries, or parameter scan rows can all prove whether the strategy was
    evaluated beyond an optimistic assumption.
    """

    normalized = [_normalize_row(row) for row in rows]
    normalized = [row for row in normalized if row["execution_profile"]]
    if not normalized:
        return {
            "status": MISSING,
            "coverage_verdict": MISSING,
            "reason": "no execution profile rows available",
            "required_profiles": list(required_profiles),
            "present_profiles": [],
            "missing_profiles": list(required_profiles),
            "profiles": {},
            "comparisons": {},
            "production_policy": _policy(False, "profile coverage missing"),
            "next_actions": ["Run the same strategy under realistic and conservative execution profiles."],
        }

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in normalized:
        grouped.setdefault(row["execution_profile"], []).append(row)
    profile_summaries = {profile: _profile_summary(profile, bucket) for profile, bucket in sorted(grouped.items())}
    present = sorted(grouped)
    required = [str(item).lower() for item in required_profiles]
    missing = [profile for profile in required if profile not in grouped]
    comparisons = _comparisons(profile_summaries, baseline_profile=str(baseline_profile).lower())
    review_reasons: list[str] = []
    if missing:
        review_reasons.append("missing required execution profiles: " + ", ".join(missing))

    realistic = profile_summaries.get("realistic")
    conservative = profile_summaries.get("conservative")
    if realistic and conservative:
        real_pnl = _decimal(realistic["avg_net_pnl"])
        cons_pnl = _decimal(conservative["avg_net_pnl"])
        degradation = _degradation_pct(real_pnl, cons_pnl)
        if real_pnl > 0 and cons_pnl < 0:
            review_reasons.append("conservative profile turns profitable realistic result negative")
        elif degradation > max_conservative_degradation_pct:
            review_reasons.append(f"conservative pnl degradation {degradation}% exceeds {max_conservative_degradation_pct}%")

    coverage_verdict = READY if not review_reasons else REVIEW
    return {
        "status": READY,
        "coverage_verdict": coverage_verdict,
        "reason": "execution profile matrix covers required profiles" if not review_reasons else "; ".join(review_reasons),
        "required_profiles": required,
        "present_profiles": present,
        "missing_profiles": missing,
        "baseline_profile": str(baseline_profile).lower(),
        "profiles": profile_summaries,
        "comparisons": comparisons,
        "production_policy": _policy(coverage_verdict == READY, "review conservative degradation before promotion" if coverage_verdict == READY else "do not promote without conservative coverage"),
        "next_actions": _next_actions(coverage_verdict, missing),
    }


def _normalize_row(row: Any) -> dict[str, Any]:
    plain = _plain(row)
    payload = _mapping(plain.get("payload"))
    params = _first_mapping(plain, payload, "parameters", "parameter_snapshot", "parameterSnapshot")
    combined = {**payload, **params, **plain}
    profile = str(_first(combined, "execution_profile", "executionProfile", "profile") or "").strip().lower()
    submitted = _decimal(_first(combined, "submitted_count", "submittedCount", "signal_count", "signalCount", "trades"))
    filled = _decimal(_first(combined, "filled_count", "filledCount", "trades"))
    no_fills = _decimal(_first(combined, "no_fills", "noFills", "no_fill_count", "noFillCount"))
    fill_rate = _optional_decimal(_first(combined, "fill_rate", "fillRate"))
    if fill_rate is None:
        fill_rate = filled / submitted if submitted > 0 else Decimal("0")
    return {
        "execution_profile": profile,
        "run_id": _first(combined, "run_id", "runId", "key"),
        "replay_mode": _first(combined, "replay_mode", "replayMode"),
        "net_pnl": _decimal(_first(combined, "net_pnl", "netPnl", "total_pnl", "totalPnl", "pnl", "netProfit")),
        "settlement_pnl": _decimal(_first(combined, "settlement_pnl", "settlementPnl")),
        "trade_exit_pnl": _decimal(_first(combined, "trade_exit_pnl", "tradeExitPnl")),
        "submitted_count": submitted,
        "filled_count": filled,
        "no_fill_count": no_fills,
        "fill_rate": fill_rate,
        "fee_total": _decimal(_first(combined, "fee_total", "feeTotal")),
        "slippage_total": _decimal(_first(combined, "slippage_total", "slippageTotal")),
    }


def _profile_summary(profile: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnls = [_decimal(row.get("net_pnl")) for row in rows]
    fill_rates = [_decimal(row.get("fill_rate")) for row in rows]
    submitted = sum((_decimal(row.get("submitted_count")) for row in rows), Decimal("0"))
    filled = sum((_decimal(row.get("filled_count")) for row in rows), Decimal("0"))
    no_fill = sum((_decimal(row.get("no_fill_count")) for row in rows), Decimal("0"))
    return {
        "execution_profile": profile,
        "run_count": len(rows),
        "avg_net_pnl": _text(sum(pnls, Decimal("0")) / Decimal(len(pnls))),
        "total_net_pnl": _text(sum(pnls, Decimal("0"))),
        "min_net_pnl": _text(min(pnls)),
        "max_net_pnl": _text(max(pnls)),
        "avg_fill_rate": _text(sum(fill_rates, Decimal("0")) / Decimal(len(fill_rates))),
        "submitted_count": _text(submitted),
        "filled_count": _text(filled),
        "no_fill_count": _text(no_fill),
        "fee_total": _text(sum((_decimal(row.get("fee_total")) for row in rows), Decimal("0"))),
        "slippage_total": _text(sum((_decimal(row.get("slippage_total")) for row in rows), Decimal("0"))),
    }


def _comparisons(profile_summaries: Mapping[str, Mapping[str, Any]], *, baseline_profile: str) -> dict[str, Any]:
    baseline = profile_summaries.get(baseline_profile)
    if not baseline:
        return {}
    baseline_pnl = _decimal(baseline.get("avg_net_pnl"))
    baseline_fill = _decimal(baseline.get("avg_fill_rate"))
    output: dict[str, Any] = {}
    for profile, summary in profile_summaries.items():
        pnl = _decimal(summary.get("avg_net_pnl"))
        fill = _decimal(summary.get("avg_fill_rate"))
        output[profile] = {
            "pnl_delta_vs_baseline": _text(pnl - baseline_pnl),
            "pnl_degradation_pct_vs_baseline": _text(_degradation_pct(baseline_pnl, pnl)),
            "fill_rate_delta_vs_baseline": _text(fill - baseline_fill),
        }
    return output


def _degradation_pct(baseline: Decimal, candidate: Decimal) -> Decimal:
    if baseline <= 0:
        return Decimal("0")
    return max(Decimal("0"), ((baseline - candidate) / abs(baseline)) * Decimal("100")).quantize(Decimal("0.0000000001"))


def _policy(allowed: bool, reason: str) -> dict[str, Any]:
    return {
        "profile_promotion_allowed": bool(allowed),
        "requires_realistic": True,
        "requires_conservative": True,
        "default_action": "review_and_stage" if allowed else "do_not_promote_profile_until_conservative_tested",
        "reason": reason,
    }


def _next_actions(verdict: str, missing: Sequence[str]) -> list[str]:
    if verdict == READY:
        return ["Compare realistic vs conservative degradation before promoting the strategy."]
    actions: list[str] = []
    if missing:
        actions.append("Run missing execution profiles: " + ", ".join(missing))
    actions.append("Do not promote results that only prove an optimistic or single-profile run.")
    return actions


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _first_mapping(*items: Any) -> dict[str, Any]:
    sources = [item for item in items if isinstance(item, Mapping)]
    keys = [str(item) for item in items if not isinstance(item, Mapping)]
    for source in sources:
        for key in keys:
            value = _mapping(source.get(key))
            if value:
                return value
    return {}


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


def _text(value: Decimal | str | int | float) -> str:
    return format(_decimal(value).normalize(), "f")
