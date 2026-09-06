"""Turn live-vs-sim fill calibration drift into execution profile suggestions."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Any, Mapping


PRICE_QUANT = Decimal("0.0001")
PCT_QUANT = Decimal("0.1")
SECONDS_QUANT = Decimal("0.1")
MIN_ADVERSE_SLIP_PRICE = Decimal("0.0025")
MAX_FILL_HAIRCUT_PCT = Decimal("80")


def execution_profile_suggestions_from_report(
    report: Mapping[str, Any],
    *,
    min_samples: int = 3,
) -> list[dict[str, Any]]:
    """Build conservative, read-only profile suggestions from a calibration report."""
    minimum = max(1, int(min_samples))
    suggestions: list[dict[str, Any]] = []
    overall = report.get("overall") if isinstance(report.get("overall"), Mapping) else {}
    overall_suggestion = _suggestion_for_summary(overall, scope="overall", min_samples=minimum)
    if overall_suggestion:
        suggestions.append(overall_suggestion)

    buckets = report.get("buckets") if isinstance(report.get("buckets"), Mapping) else {}
    for field, items in buckets.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, Mapping):
                continue
            suggestion = _suggestion_for_summary(
                item,
                scope="bucket",
                bucket_field=str(field),
                bucket=str(item.get("bucket") or "unknown"),
                min_samples=minimum,
            )
            if suggestion:
                suggestions.append(suggestion)

    return sorted(
        suggestions,
        key=lambda row: (
            0 if row.get("scope") == "overall" else 1,
            -int(row.get("sample_count") or 0),
            str(row.get("bucket_field") or ""),
            str(row.get("bucket") or ""),
        ),
    )


def execution_profile_suggestions_to_markdown(suggestions: list[Mapping[str, Any]]) -> str:
    lines = [
        "# Execution Profile Calibration Suggestions",
        "",
        "These are read-only suggestions derived from live-vs-sim fill calibration. Review them before changing default profiles.",
        "",
    ]
    if not suggestions:
        lines.append("No execution profile changes suggested for the selected samples.")
        return "\n".join(lines).rstrip() + "\n"
    lines.extend([
        "| Scope | Bucket | Samples | Suggested Profile | Latency Floor | Adverse Slip Floor | Fill Haircut Floor | Reason |",
        "| --- | --- | ---: | --- | ---: | ---: | ---: | --- |",
    ])
    for suggestion in suggestions:
        recommended = suggestion.get("recommended") if isinstance(suggestion.get("recommended"), Mapping) else {}
        bucket = "overall" if suggestion.get("scope") == "overall" else f"{suggestion.get('bucket_field')}={suggestion.get('bucket')}"
        lines.append(
            "| "
            + " | ".join([
                _text(suggestion.get("scope")),
                _text(bucket),
                _text(suggestion.get("sample_count")),
                _text(recommended.get("execution_profile")),
                _text(recommended.get("latency_seconds_floor")),
                _text(recommended.get("adverse_slippage_price_floor")),
                _text(recommended.get("fill_probability_haircut_pct_floor")),
                _text(suggestion.get("trust_reason")),
            ])
            + " |"
        )
    return "\n".join(lines).rstrip() + "\n"


def execution_profile_suggestions_to_json(suggestions: list[Mapping[str, Any]]) -> str:
    return json.dumps(suggestions, ensure_ascii=False, indent=2, default=str) + "\n"


def _suggestion_for_summary(
    summary: Mapping[str, Any],
    *,
    scope: str,
    min_samples: int,
    bucket_field: str | None = None,
    bucket: str | None = None,
) -> dict[str, Any] | None:
    sample_count = int(_decimal(summary.get("sample_count")))
    if sample_count < min_samples:
        return None
    if str(summary.get("trust_status") or "").lower() != "review":
        return None

    status_error_rate = _decimal(summary.get("status_error_rate"))
    avg_price_error = _decimal(summary.get("avg_price_error"))
    avg_slippage_error = _decimal(summary.get("avg_slippage_error"))
    avg_latency_error = _decimal(summary.get("avg_latency_error_seconds"))
    adverse_slippage_floor = _adverse_slippage_floor(avg_price_error, avg_slippage_error)
    fill_haircut_floor = _fill_haircut_floor(status_error_rate)
    latency_floor = _latency_floor(avg_latency_error)
    profile = _profile_for_errors(status_error_rate, avg_price_error, avg_slippage_error, avg_latency_error)
    recommended = {
        "execution_profile": profile,
        "latency_seconds_floor": _decimal_text(latency_floor, SECONDS_QUANT),
        "latency_blocks_floor": 1 if latency_floor > 0 else 0,
        "adverse_slippage_price_floor": _decimal_text(adverse_slippage_floor, PRICE_QUANT),
        "adverse_slippage_cents_floor": _decimal_text(adverse_slippage_floor * Decimal("100"), PCT_QUANT),
        "fill_probability_haircut_pct_floor": _decimal_text(fill_haircut_floor, PCT_QUANT),
    }
    suggestion = {
        "scope": scope,
        "sample_count": sample_count,
        "trust_status": summary.get("trust_status"),
        "trust_reason": summary.get("trust_reason"),
        "recommended": recommended,
        "evidence": {
            "status_error_rate": _decimal_text(status_error_rate, PCT_QUANT),
            "avg_price_error": _decimal_text(avg_price_error, PRICE_QUANT),
            "avg_slippage_error": _decimal_text(avg_slippage_error, PRICE_QUANT),
            "avg_latency_error_seconds": _decimal_text(avg_latency_error, SECONDS_QUANT),
            "requires_recalibration": bool(summary.get("requires_recalibration")),
        },
    }
    if bucket_field is not None:
        suggestion["bucket_field"] = bucket_field
    if bucket is not None:
        suggestion["bucket"] = bucket
    return suggestion


def _profile_for_errors(
    status_error_rate: Decimal,
    avg_price_error: Decimal,
    avg_slippage_error: Decimal,
    avg_latency_error: Decimal,
) -> str:
    if (
        status_error_rate >= Decimal("20")
        or avg_price_error >= Decimal("0.03")
        or avg_slippage_error >= Decimal("0.03")
        or avg_latency_error >= Decimal("5")
    ):
        return "stress"
    return "conservative"


def _latency_floor(avg_latency_error: Decimal) -> Decimal:
    if avg_latency_error <= 0:
        return Decimal("0")
    return avg_latency_error.quantize(SECONDS_QUANT, rounding=ROUND_HALF_UP)


def _adverse_slippage_floor(avg_price_error: Decimal, avg_slippage_error: Decimal) -> Decimal:
    floor = max(avg_price_error, avg_slippage_error)
    if floor <= Decimal("0.01"):
        return Decimal("0")
    return max(MIN_ADVERSE_SLIP_PRICE, floor).quantize(PRICE_QUANT, rounding=ROUND_HALF_UP)


def _fill_haircut_floor(status_error_rate: Decimal) -> Decimal:
    if status_error_rate <= 0:
        return Decimal("0")
    haircut = status_error_rate * Decimal("1.5")
    return min(MAX_FILL_HAIRCUT_PCT, haircut).quantize(PCT_QUANT, rounding=ROUND_HALF_UP)


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _decimal_text(value: Decimal, quant: Decimal) -> str:
    text = format(value.quantize(quant, rounding=ROUND_HALF_UP), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _text(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|")
