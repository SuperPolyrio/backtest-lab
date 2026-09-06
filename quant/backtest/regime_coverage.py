"""Regime coverage reports for fill-first backtest research."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"

REGIME_COVERAGE_DIMENSIONS = (
    "market_category",
    "time_to_expiry_bucket",
    "liquidity_bucket",
    "volatility_bucket",
    "final_minute",
    "event_outcome_count_bucket",
)

GENERALIZATION_REQUIRED_DIMENSIONS = (
    "market_category",
    "liquidity_bucket",
    "time_to_expiry_bucket",
)


def build_regime_coverage_report(
    rows: Sequence[Any],
    *,
    universe: Mapping[str, Any] | None = None,
    min_bucket_count: int = 2,
    min_samples_per_bucket: int = 2,
) -> dict[str, Any]:
    normalized = [_normalize_row(row, universe=universe) for row in rows]
    normalized = [row for row in normalized if row["sample_count"] > 0]
    if not normalized:
        return {
            "status": MISSING,
            "coverage_verdict": MISSING,
            "strategy_scope": "unknown",
            "reason": "no rows available for regime coverage",
            "sample_count": 0,
            "dimension_count": 0,
            "ready_dimension_count": 0,
            "regime_specific": True,
            "narrow_dimensions": list(REGIME_COVERAGE_DIMENSIONS),
            "dimensions": {},
            "next_actions": ["Run the strategy across multiple market categories, liquidity buckets, and time-to-expiry regimes."],
        }

    dimensions = {
        dimension: _dimension_summary(normalized, dimension, min_samples_per_bucket=min_samples_per_bucket)
        for dimension in REGIME_COVERAGE_DIMENSIONS
    }
    ready_dimensions = [
        dimension
        for dimension, summary in dimensions.items()
        if summary["known_bucket_count"] >= min_bucket_count and summary["ready_bucket_count"] >= min_bucket_count
    ]
    narrow_dimensions = [
        dimension
        for dimension in REGIME_COVERAGE_DIMENSIONS
        if dimension not in ready_dimensions
    ]
    missing_generalization = [dimension for dimension in GENERALIZATION_REQUIRED_DIMENSIONS if dimension not in ready_dimensions]
    regime_specific = bool(missing_generalization or narrow_dimensions)
    if missing_generalization:
        verdict = REVIEW
        scope = "regime_specific"
        reason = "coverage too narrow for general strategy claim: " + ", ".join(missing_generalization)
    elif narrow_dimensions:
        verdict = REVIEW
        scope = "partially_generalizable"
        reason = "some regime dimensions are still narrow: " + ", ".join(narrow_dimensions)
    else:
        verdict = READY
        scope = "generalizable_candidate"
        reason = "core regime dimensions have multiple populated buckets"
    return {
        "status": READY,
        "coverage_verdict": verdict,
        "strategy_scope": scope,
        "reason": reason,
        "sample_count": sum(int(row["sample_count"]) for row in normalized),
        "row_count": len(normalized),
        "dimension_count": len(dimensions),
        "ready_dimension_count": len(ready_dimensions),
        "regime_specific": regime_specific,
        "narrow_dimensions": narrow_dimensions,
        "required_generalization_dimensions": list(GENERALIZATION_REQUIRED_DIMENSIONS),
        "dimensions": dimensions,
        "next_actions": _next_actions(scope, narrow_dimensions),
    }


def _normalize_row(row: Any, *, universe: Mapping[str, Any] | None) -> dict[str, Any]:
    plain = _as_plain(row)
    payload = _as_mapping(plain.get("payload"))
    meta = _as_mapping(plain.get("meta"))
    context = _as_mapping(meta.get("context")) or _as_mapping(payload.get("context")) or _as_mapping(plain.get("context"))
    combined = {**payload, **meta, **context, **plain}
    universe_map = dict(universe or {})
    raw_rows = _to_int(_first_present(combined, "raw_rows_for_outcome", "rawRowsForOutcome", "raw_rows", "rawRows"))
    status = str(_first_present(combined, "status", "order_status", "orderStatus", "fast_status", "accurate_status") or "").upper()
    filled_size = _decimal(_first_present(combined, "filled_size", "filledSize", "actual_fill_size", "actualFillSize"))
    sample_count = max(1, _to_int(_first_present(combined, "sample_count", "sampleCount", "submitted_count", "signal_count", "trades", "count") or 1))
    return {
        "market_category": _first_text(combined, "market_category", "marketCategory", "category", "event_category", "eventCategory")
        or _first_text(universe_map, "category")
        or "unknown",
        "time_to_expiry_bucket": _first_text(combined, "time_to_expiry_bucket", "timeToExpiryBucket") or _time_to_expiry_bucket(combined),
        "liquidity_bucket": _first_text(combined, "liquidity_bucket", "liquidityBucket") or _liquidity_bucket(raw_rows),
        "volatility_bucket": _first_text(combined, "volatility_bucket", "volatilityBucket") or "unknown",
        "final_minute": _first_text(combined, "final_minute", "finalMinute") or _final_minute_bucket(combined),
        "event_outcome_count_bucket": _first_text(combined, "event_outcome_count_bucket", "eventOutcomeCountBucket") or _outcome_count_bucket(combined),
        "sample_count": sample_count,
        "filled_count": sample_count if status in {"FILLED", "PARTIAL_FILLED"} or filled_size > 0 else 0,
        "win_count": sample_count if _decimal(_first_present(combined, "pnl", "net_pnl", "total_pnl", "accurate_pnl")) > 0 else 0,
        "pnl": _decimal(_first_present(combined, "pnl", "net_pnl", "total_pnl", "accurate_pnl", "netProfit")),
    }


def _dimension_summary(rows: Sequence[Mapping[str, Any]], dimension: str, *, min_samples_per_bucket: int) -> dict[str, Any]:
    buckets: dict[str, dict[str, Any]] = {}
    for row in rows:
        bucket = str(row.get(dimension) or "unknown")
        state = buckets.setdefault(bucket, {"bucket": bucket, "row_count": 0, "sample_count": 0, "filled_count": 0, "win_count": 0, "pnl": Decimal("0")})
        state["row_count"] += 1
        state["sample_count"] += int(row.get("sample_count") or 0)
        state["filled_count"] += int(row.get("filled_count") or 0)
        state["win_count"] += int(row.get("win_count") or 0)
        state["pnl"] += _decimal(row.get("pnl"))
    bucket_rows = []
    for state in buckets.values():
        sample_count = max(1, int(state["sample_count"]))
        bucket_rows.append(
            {
                "bucket": state["bucket"],
                "row_count": int(state["row_count"]),
                "sample_count": int(state["sample_count"]),
                "filled_count": int(state["filled_count"]),
                "fill_rate": _decimal_text(Decimal(int(state["filled_count"])) / Decimal(sample_count)),
                "win_rate": _decimal_text(Decimal(int(state["win_count"])) / Decimal(sample_count)),
                "total_pnl": _decimal_text(Decimal(state["pnl"])),
                "ready": state["bucket"] != "unknown" and int(state["sample_count"]) >= min_samples_per_bucket,
            }
        )
    known_bucket_count = sum(1 for row in bucket_rows if row["bucket"] != "unknown")
    ready_bucket_count = sum(1 for row in bucket_rows if row["ready"])
    return {
        "dimension": dimension,
        "bucket_count": len(bucket_rows),
        "known_bucket_count": known_bucket_count,
        "ready_bucket_count": ready_bucket_count,
        "status": READY if ready_bucket_count >= 2 else REVIEW,
        "buckets": sorted(bucket_rows, key=lambda item: (-int(item["sample_count"]), str(item["bucket"]))),
    }


def _next_actions(scope: str, narrow_dimensions: Sequence[str]) -> list[str]:
    if scope == "generalizable_candidate":
        return ["Keep reporting performance by regime; do not collapse results into a single average PnL."]
    actions = ["Mark the strategy as regime-specific until coverage improves."]
    if narrow_dimensions:
        actions.append("Expand backtests across: " + ", ".join(narrow_dimensions))
    return actions


def _time_to_expiry_bucket(row: Mapping[str, Any]) -> str:
    seconds = _first_present(row, "time_to_expiry_seconds", "timeToExpirySeconds")
    if seconds is None:
        days = _first_present(row, "time_to_expiry_days", "timeToExpiryDays")
        if days is not None:
            seconds = _decimal(days) * Decimal("86400")
    if seconds is None:
        seconds = _seconds_between(_first_present(row, "signal_time", "signalTime"), _first_present(row, "end_date", "endDate"))
    if seconds is None:
        return "unknown"
    value = _decimal(seconds)
    if value <= Decimal("60"):
        return "final_minute"
    if value <= Decimal("3600"):
        return "lt_1h"
    if value <= Decimal("86400"):
        return "lt_1d"
    if value <= Decimal("604800"):
        return "lt_7d"
    return "gte_7d"


def _final_minute_bucket(row: Mapping[str, Any]) -> str:
    value = _first_present(row, "time_to_expiry_seconds", "timeToExpirySeconds")
    if value is None:
        value = _seconds_between(_first_present(row, "signal_time", "signalTime"), _first_present(row, "end_date", "endDate"))
    if value is None:
        return "unknown"
    return "final_minute" if _decimal(value) <= Decimal("60") else "not_final_minute"


def _outcome_count_bucket(row: Mapping[str, Any]) -> str:
    count = _to_int(_first_present(row, "event_outcome_count", "eventOutcomeCount", "outcome_count", "outcomeCount"))
    if count <= 0:
        return "unknown"
    if count == 1:
        return "single_outcome"
    if count == 2:
        return "binary"
    if count <= 5:
        return "small_multi_3_5"
    if count <= 20:
        return "medium_multi_6_20"
    return "large_multi_21_plus"


def _liquidity_bucket(raw_rows: int) -> str:
    if raw_rows <= 0:
        return "unknown"
    if raw_rows < 10:
        return "thin"
    if raw_rows < 100:
        return "medium"
    return "active"


def _seconds_between(start: Any, end: Any) -> Decimal | None:
    try:
        if not start or not end:
            return None
        start_dt = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
        return Decimal(str(max(0.0, (end_dt - start_dt).total_seconds())))
    except Exception:
        return None


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
        if key in row and row.get(key) not in (None, "", {}):
            return row.get(key)
    return None


def _first_text(row: Mapping[str, Any], *keys: str) -> str | None:
    value = _first_present(row, *keys)
    return None if value in (None, "", {}) else str(value)


def _to_int(value: Any) -> int:
    try:
        return int(Decimal(str(value or "0")))
    except Exception:
        return 0


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _decimal_text(value: Decimal | int | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else _decimal(value)
    return format(decimal_value.normalize(), "f")
