"""Shadow/live calibration metrics for the L2 + OrderFilled execution model."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping


READY = "ready"
REVIEW = "review"
MISSING = "missing"

PRICE_QUANT = Decimal("0.0001")
SIZE_QUANT = Decimal("0.0001")
SECONDS_QUANT = Decimal("0.1")
PCT_QUANT = Decimal("0.1")
SCORE_QUANT = Decimal("0.0001")

FILLED_CLASSES = {"full", "partial"}


def build_l2_shadow_calibration_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    fill_probability_threshold: Decimal | str = Decimal("0.5"),
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Compare simulated L2/OrderFilled execution against paper/live order outcomes."""
    samples = [_normalize_sample(row) for row in rows]
    sample_count = len(samples)
    paired = [row for row in samples if row["has_actual_state"]]
    taker = [row for row in paired if row["role"] == "taker"]
    maker = [row for row in paired if row["role"] == "maker"]
    threshold = _decimal(fill_probability_threshold)
    taker_report = _taker_report(taker)
    maker_report = _maker_report(maker, threshold=threshold)
    suggestions = _calibration_suggestions(taker_report, maker_report, paired)
    status, reason = _status_reason(sample_count, paired, taker_report, maker_report)
    return {
        "schema_version": "l2_shadow_live_calibration_v1",
        "generated_at": generated_at,
        "status": status,
        "reason": reason,
        "sample_count": sample_count,
        "paired_sample_count": len(paired),
        "unpaired_sample_count": sample_count - len(paired),
        "taker_sample_count": len(taker),
        "maker_sample_count": len(maker),
        "fill_probability_threshold": _decimal_text(threshold, SCORE_QUANT),
        "taker": taker_report,
        "maker": maker_report,
        "calibration_suggestions": suggestions,
        "next_actions": _next_actions(status, suggestions),
    }


def l2_shadow_calibration_to_markdown(report: Mapping[str, Any]) -> str:
    taker = report.get("taker") if isinstance(report.get("taker"), Mapping) else {}
    maker = report.get("maker") if isinstance(report.get("maker"), Mapping) else {}
    lines = [
        f"# L2 Shadow/Live Calibration: {report.get('status')}",
        "",
        f"- reason: {report.get('reason', '-')}",
        f"- samples: {report.get('sample_count', 0)}",
        f"- paired_samples: {report.get('paired_sample_count', 0)}",
        f"- taker_samples: {report.get('taker_sample_count', 0)}",
        f"- maker_samples: {report.get('maker_sample_count', 0)}",
        "",
        "## Taker",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
    ]
    for key in (
        "classification_accuracy_pct",
        "avg_fill_price_error",
        "avg_fill_size_error",
        "reject_partial_full_accuracy_pct",
        "false_positive_fill_rate_pct",
        "false_negative_fill_rate_pct",
    ):
        lines.append(f"| {key} | {_text(taker.get(key))} |")
    lines.extend(["", "## Maker", "", "| Metric | Value |", "| --- | ---: |"])
    for key in (
        "brier_score",
        "predicted_fill_rate_pct",
        "actual_fill_rate_pct",
        "false_positive_fill_rate_pct",
        "false_negative_fill_rate_pct",
        "avg_time_to_fill_error_seconds",
    ):
        lines.append(f"| {key} | {_text(maker.get(key))} |")
    lines.extend(["", "## Calibration Suggestions", ""])
    suggestions = report.get("calibration_suggestions") or []
    if not suggestions:
        lines.append("No execution parameter changes suggested for the selected samples.")
    else:
        lines.extend([
            "| Parameter | Direction | Reason |",
            "| --- | --- | --- |",
        ])
        for suggestion in suggestions:
            if not isinstance(suggestion, Mapping):
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        _text(suggestion.get("parameter")),
                        _text(suggestion.get("direction")),
                        _text(suggestion.get("reason")),
                    ]
                )
                + " |"
            )
    return "\n".join(lines).rstrip() + "\n"


def _taker_report(samples: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(samples)
    if count <= 0:
        return _empty_taker_report()
    class_matches = [row for row in samples if row["predicted_class"] == row["actual_class"]]
    predicted_filled = [row for row in samples if row["predicted_class"] in FILLED_CLASSES]
    actual_filled = [row for row in samples if row["actual_class"] in FILLED_CLASSES]
    false_positive = [row for row in samples if row["predicted_class"] in FILLED_CLASSES and row["actual_class"] not in FILLED_CLASSES]
    false_negative = [row for row in samples if row["predicted_class"] not in FILLED_CLASSES and row["actual_class"] in FILLED_CLASSES]
    price_errors = [_abs_diff(row["predicted_fill_price"], row["actual_fill_price"]) for row in samples if row["predicted_fill_price"] is not None and row["actual_fill_price"] is not None]
    size_errors = [_abs_diff(row["predicted_fill_size"], row["actual_fill_size"]) for row in samples if row["predicted_fill_size"] is not None and row["actual_fill_size"] is not None]
    return {
        "sample_count": count,
        "classification_accuracy_pct": _pct_text(_ratio(len(class_matches), count)),
        "reject_partial_full_accuracy_pct": _pct_text(_ratio(len(class_matches), count)),
        "avg_fill_price_error": _decimal_text(_avg(price_errors), PRICE_QUANT),
        "max_fill_price_error": _decimal_text(max(price_errors) if price_errors else Decimal("0"), PRICE_QUANT),
        "avg_fill_size_error": _decimal_text(_avg(size_errors), SIZE_QUANT),
        "max_fill_size_error": _decimal_text(max(size_errors) if size_errors else Decimal("0"), SIZE_QUANT),
        "predicted_fill_count": len(predicted_filled),
        "actual_fill_count": len(actual_filled),
        "false_positive_fill_count": len(false_positive),
        "false_negative_fill_count": len(false_negative),
        "false_positive_fill_rate_pct": _pct_text(_ratio(len(false_positive), len(predicted_filled))),
        "false_negative_fill_rate_pct": _pct_text(_ratio(len(false_negative), len(actual_filled))),
    }


def _maker_report(samples: list[dict[str, Any]], *, threshold: Decimal) -> dict[str, Any]:
    count = len(samples)
    if count <= 0:
        return _empty_maker_report()
    actual_filled = [row for row in samples if row["actual_class"] in FILLED_CLASSES]
    predicted_positive = [row for row in samples if row["predicted_probability"] >= threshold or row["predicted_class"] in FILLED_CLASSES]
    false_positive = [row for row in samples if (row["predicted_probability"] >= threshold or row["predicted_class"] in FILLED_CLASSES) and row["actual_class"] not in FILLED_CLASSES]
    false_negative = [row for row in samples if row["predicted_probability"] < threshold and row["predicted_class"] not in FILLED_CLASSES and row["actual_class"] in FILLED_CLASSES]
    brier_terms = []
    for row in samples:
        actual = Decimal("1") if row["actual_class"] in FILLED_CLASSES else Decimal("0")
        predicted = _clamp_probability(row["predicted_probability"])
        brier_terms.append((predicted - actual) * (predicted - actual))
    ttf_errors = [
        _abs_diff(row["predicted_time_to_fill_seconds"], row["actual_time_to_fill_seconds"])
        for row in samples
        if row["predicted_time_to_fill_seconds"] is not None and row["actual_time_to_fill_seconds"] is not None
    ]
    return {
        "sample_count": count,
        "brier_score": _decimal_text(_avg(brier_terms), SCORE_QUANT),
        "predicted_fill_rate_pct": _pct_text(_ratio(len(predicted_positive), count)),
        "actual_fill_rate_pct": _pct_text(_ratio(len(actual_filled), count)),
        "false_positive_fill_count": len(false_positive),
        "false_negative_fill_count": len(false_negative),
        "false_positive_fill_rate_pct": _pct_text(_ratio(len(false_positive), len(predicted_positive))),
        "false_negative_fill_rate_pct": _pct_text(_ratio(len(false_negative), len(actual_filled))),
        "avg_time_to_fill_error_seconds": _decimal_text(_avg(ttf_errors), SECONDS_QUANT),
        "max_time_to_fill_error_seconds": _decimal_text(max(ttf_errors) if ttf_errors else Decimal("0"), SECONDS_QUANT),
        "calibration_curve": _calibration_curve(samples),
    }


def _calibration_curve(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets = [
        ("0.00-0.25", Decimal("0"), Decimal("0.25")),
        ("0.25-0.50", Decimal("0.25"), Decimal("0.50")),
        ("0.50-0.75", Decimal("0.50"), Decimal("0.75")),
        ("0.75-1.00", Decimal("0.75"), Decimal("1.0000001")),
    ]
    rows: list[dict[str, Any]] = []
    for name, lower, upper in buckets:
        bucket = [row for row in samples if lower <= _clamp_probability(row["predicted_probability"]) < upper]
        if not bucket:
            rows.append({"bucket": name, "sample_count": 0, "avg_predicted_probability": "0", "actual_fill_rate_pct": "0"})
            continue
        avg_pred = _avg([_clamp_probability(row["predicted_probability"]) for row in bucket])
        fills = sum(1 for row in bucket if row["actual_class"] in FILLED_CLASSES)
        rows.append(
            {
                "bucket": name,
                "sample_count": len(bucket),
                "avg_predicted_probability": _decimal_text(avg_pred, SCORE_QUANT),
                "actual_fill_rate_pct": _pct_text(_ratio(fills, len(bucket))),
            }
        )
    return rows


def _calibration_suggestions(
    taker: Mapping[str, Any],
    maker: Mapping[str, Any],
    paired: list[dict[str, Any]],
) -> list[dict[str, str]]:
    suggestions: list[dict[str, str]] = []
    taker_count = int(taker.get("sample_count") or 0)
    maker_count = int(maker.get("sample_count") or 0)
    if taker_count:
        if _decimal(taker.get("false_positive_fill_rate_pct")) >= Decimal("10") or _decimal(taker.get("avg_fill_size_error")) > Decimal("5"):
            suggestions.append(
                {
                    "parameter": "depth_haircut",
                    "direction": "decrease",
                    "reason": "taker predicted more executable depth than live orders received",
                }
            )
        if _decimal(taker.get("avg_fill_price_error")) > Decimal("0.01"):
            suggestions.append(
                {
                    "parameter": "impact_bps",
                    "direction": "increase",
                    "reason": "taker live fill price drift is larger than current impact/slippage assumption",
                }
            )
    if maker_count:
        if _decimal(maker.get("false_positive_fill_rate_pct")) >= Decimal("10") or _decimal(maker.get("brier_score")) > Decimal("0.20"):
            suggestions.append(
                {
                    "parameter": "queue_ahead_fraction",
                    "direction": "increase",
                    "reason": "maker model over-predicted fills versus live outcomes",
                }
            )
            suggestions.append(
                {
                    "parameter": "cancel_ahead_fraction",
                    "direction": "decrease",
                    "reason": "maker queue should trust OrderFilled evidence more than unexplained LOB disappearance",
                }
            )
        if _decimal(maker.get("false_negative_fill_rate_pct")) >= Decimal("20"):
            suggestions.append(
                {
                    "parameter": "queue_ahead_fraction",
                    "direction": "decrease",
                    "reason": "maker model under-predicted live fills",
                }
            )
        if _decimal(maker.get("avg_time_to_fill_error_seconds")) > Decimal("5"):
            suggestions.append(
                {
                    "parameter": "book_ttl_ms",
                    "direction": "decrease",
                    "reason": "time-to-fill drift suggests stale book windows are too permissive",
                }
            )
    latency_errors = [
        _abs_diff(row["predicted_latency_seconds"], row["actual_latency_seconds"])
        for row in paired
        if row["predicted_latency_seconds"] is not None and row["actual_latency_seconds"] is not None
    ]
    if latency_errors and _avg(latency_errors) > Decimal("2"):
        suggestions.append(
            {
                "parameter": "latency_model",
                "direction": "recalibrate",
                "reason": "paper/live order ack or fill latency differs from simulated latency",
            }
        )
    return _dedupe_suggestions(suggestions)


def _status_reason(
    sample_count: int,
    paired: list[dict[str, Any]],
    taker: Mapping[str, Any],
    maker: Mapping[str, Any],
) -> tuple[str, str]:
    if sample_count <= 0:
        return MISSING, "no shadow/live calibration samples"
    if not paired:
        return MISSING, "no samples contain actual paper/live order state"
    reasons: list[str] = []
    if int(taker.get("sample_count") or 0):
        if _decimal(taker.get("classification_accuracy_pct")) < Decimal("80"):
            reasons.append(f"taker classification accuracy {taker.get('classification_accuracy_pct')}%")
        if _decimal(taker.get("avg_fill_price_error")) > Decimal("0.01"):
            reasons.append(f"taker avg fill price error {taker.get('avg_fill_price_error')}")
    if int(maker.get("sample_count") or 0):
        if _decimal(maker.get("brier_score")) > Decimal("0.20"):
            reasons.append(f"maker brier score {maker.get('brier_score')}")
        if _decimal(maker.get("false_positive_fill_rate_pct")) >= Decimal("10"):
            reasons.append(f"maker false positive fill rate {maker.get('false_positive_fill_rate_pct')}%")
    if reasons:
        return REVIEW, "; ".join(reasons)
    return READY, "shadow/live execution calibration within current L2 thresholds"


def _next_actions(status: str, suggestions: list[Mapping[str, str]]) -> list[str]:
    if status == MISSING:
        return ["Attach terminal paper/live order-state events before trusting L2 execution calibration."]
    if suggestions:
        params = ", ".join(str(item.get("parameter")) for item in suggestions[:5])
        return [f"Review L2 execution profile parameters: {params}."]
    return ["Keep collecting paired shadow/live samples across maker/taker regimes."]


def _normalize_sample(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
    sim = row.get("simulated") if isinstance(row.get("simulated"), Mapping) else {}
    live = row.get("live") if isinstance(row.get("live"), Mapping) else {}
    role = str(_first(row, payload, sim, live, "role") or "").strip().lower()
    if role not in {"maker", "taker"}:
        role = "maker" if _bool(_first(row, payload, sim, live, "post_only", "postOnly")) else "taker"
    predicted_status = _canonical_status(_first(row, sim, payload, "simulated_status", "predicted_status", "status"))
    actual_status = _canonical_status(_first(row, live, payload, "actual_status", "live_status", "api_order_status", "chain_order_status", "clob_order_status"))
    return {
        "role": role,
        "side": str(_first(row, payload, sim, live, "side") or "").strip().upper(),
        "predicted_class": _status_class(predicted_status, _decimal_or_none(_first(row, sim, payload, "simulated_fill_size", "predicted_fill_size", "filled_size"))),
        "actual_class": _status_class(actual_status, _decimal_or_none(_first(row, live, payload, "actual_fill_size", "live_fill_size", "filled_size"))),
        "predicted_fill_price": _decimal_or_none(_first(row, sim, payload, "simulated_fill_price", "predicted_fill_price", "avg_fill_price")),
        "actual_fill_price": _decimal_or_none(_first(row, live, payload, "actual_fill_price", "live_fill_price", "avg_fill_price")),
        "predicted_fill_size": _decimal_or_none(_first(row, sim, payload, "simulated_fill_size", "predicted_fill_size", "filled_size")),
        "actual_fill_size": _decimal_or_none(_first(row, live, payload, "actual_fill_size", "live_fill_size", "filled_size")),
        "predicted_probability": _clamp_probability(_decimal(_first(row, sim, payload, "predicted_fill_probability", "simulated_fill_probability", "fill_probability"))),
        "predicted_time_to_fill_seconds": _decimal_or_none(_first(row, sim, payload, "predicted_time_to_fill_seconds", "simulated_time_to_fill_seconds")),
        "actual_time_to_fill_seconds": _decimal_or_none(_first(row, live, payload, "actual_time_to_fill_seconds", "live_time_to_fill_seconds")),
        "predicted_latency_seconds": _decimal_or_none(_first(row, sim, payload, "predicted_latency_seconds", "simulated_latency_seconds")),
        "actual_latency_seconds": _decimal_or_none(_first(row, live, payload, "actual_latency_seconds", "live_latency_seconds")),
        "has_actual_state": actual_status != "UNKNOWN" or _first(row, live, payload, "actual_fill_size", "live_fill_size", "actual_fill_price", "live_fill_price") not in (None, ""),
    }


def _status_class(status: str, fill_size: Decimal | None) -> str:
    if "PARTIAL" in status:
        return "partial"
    if status in {"FILLED", "MATCHED"} or (fill_size is not None and fill_size > 0):
        return "full"
    if status in {"REJECTED", "FAILED"}:
        return "reject"
    if status in {"NO_FILL", "CANCELED", "CANCELLED", "EXPIRED", "OPEN", "UNKNOWN"}:
        return "none"
    return "none"


def _canonical_status(value: Any) -> str:
    text = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    if not text:
        return "UNKNOWN"
    if text in {"NO_FILL", "UNFILLED", "NOFILL"}:
        return "NO_FILL"
    if "PARTIAL" in text:
        return "PARTIAL_FILLED"
    if "NO_FILL" in text or "UNFILLED" in text:
        return "NO_FILL"
    if "CANCEL" in text:
        return "CANCELED"
    if "REJECT" in text:
        return "REJECTED"
    if "EXPIRE" in text:
        return "EXPIRED"
    if "FILL" in text or "MATCH" in text:
        return "FILLED"
    if text in {"PENDING", "SUBMITTED", "ACCEPTED", "OPEN"}:
        return "OPEN"
    return text


def _first(*sources_and_keys: Any) -> Any:
    sources: list[Mapping[str, Any]] = []
    keys: list[str] = []
    reading_keys = False
    for item in sources_and_keys:
        if not reading_keys and isinstance(item, Mapping):
            sources.append(item)
            continue
        reading_keys = True
        if isinstance(item, tuple):
            keys.extend(str(key) for key in item)
        else:
            keys.append(str(item))
    expanded_keys: list[str] = []
    for key in keys:
        expanded_keys.append(str(key))
        expanded_keys.append(_camel(str(key)))
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in expanded_keys:
            value = source.get(key)
            if value not in (None, ""):
                return value
    return None


def _camel(key: str) -> str:
    parts = key.split("_")
    return parts[0] + "".join(part[:1].upper() + part[1:] for part in parts[1:])


def _decimal(value: Any) -> Decimal:
    parsed = _decimal_or_none(value)
    return parsed if parsed is not None else Decimal("0")


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _clamp_probability(value: Decimal | None) -> Decimal:
    if value is None:
        return Decimal("0")
    if value < 0:
        return Decimal("0")
    if value > 1:
        return Decimal("1")
    return value


def _abs_diff(left: Decimal | None, right: Decimal | None) -> Decimal:
    if left is None or right is None:
        return Decimal("0")
    return abs(left - right)


def _ratio(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return Decimal(numerator) / Decimal(denominator)


def _avg(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _pct_text(value: Decimal) -> str:
    return _decimal_text(value * Decimal("100"), PCT_QUANT)


def _decimal_text(value: Decimal, quant: Decimal) -> str:
    text = format(value.quantize(quant, rounding=ROUND_HALF_UP), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _dedupe_suggestions(suggestions: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    unique: list[dict[str, str]] = []
    for item in suggestions:
        key = (item["parameter"], item["direction"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _empty_taker_report() -> dict[str, Any]:
    return {
        "sample_count": 0,
        "classification_accuracy_pct": "0",
        "reject_partial_full_accuracy_pct": "0",
        "avg_fill_price_error": "0",
        "max_fill_price_error": "0",
        "avg_fill_size_error": "0",
        "max_fill_size_error": "0",
        "predicted_fill_count": 0,
        "actual_fill_count": 0,
        "false_positive_fill_count": 0,
        "false_negative_fill_count": 0,
        "false_positive_fill_rate_pct": "0",
        "false_negative_fill_rate_pct": "0",
    }


def _empty_maker_report() -> dict[str, Any]:
    return {
        "sample_count": 0,
        "brier_score": "0",
        "predicted_fill_rate_pct": "0",
        "actual_fill_rate_pct": "0",
        "false_positive_fill_count": 0,
        "false_negative_fill_count": 0,
        "false_positive_fill_rate_pct": "0",
        "false_negative_fill_rate_pct": "0",
        "avg_time_to_fill_error_seconds": "0",
        "max_time_to_fill_error_seconds": "0",
        "calibration_curve": [],
    }


def _bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _text(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|")
