"""Calibration metrics with finite-sample uncertainty."""

from __future__ import annotations

import math
from collections import Counter
from decimal import Decimal
from typing import Any, Iterable, Mapping


def quantile(values: Iterable[Any], q: float) -> Decimal | None:
    valid: list[Decimal] = []
    for value in values:
        try:
            parsed = Decimal(str(value))
        except Exception:
            continue
        if parsed.is_finite():
            valid.append(parsed)
    valid.sort()
    if not valid:
        return None
    index = int(round((len(valid) - 1) * min(1.0, max(0.0, q))))
    return valid[index]


def wilson_interval(successes: int, total: int, *, z: float = 1.959963984540054) -> dict[str, Any]:
    if total <= 0:
        return {"point": None, "lower": None, "upper": None, "successes": successes, "total": total}
    p = successes / total
    denominator = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denominator
    radius = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denominator
    return {
        "point": p,
        "lower": max(0.0, center - radius),
        "upper": min(1.0, center + radius),
        "successes": successes,
        "total": total,
        "method": "wilson_95",
        "rule_of_three_upper": min(1.0, 3 / total) if successes == 0 else None,
    }


def confusion(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(
        (
            str(row.get("predicted_class") or "UNKNOWN_DATA"),
            str(row.get("actual_class") or "UNKNOWN_DATA"),
        )
        for row in rows
    )
    return [
        {"predicted_class": predicted, "actual_class": actual, "count": count}
        for (predicted, actual), count in sorted(counts.items())
    ]


def holdout_metrics(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    samples = [dict(row) for row in rows]
    fill_predictions = [
        row
        for row in samples
        if str(row.get("predicted_class")) in {"FULL", "PARTIAL"}
    ]
    false_positive = sum(
        str(row.get("actual_class")) in {"NO_FILL", "REJECT"}
        for row in fill_predictions
    )
    fok_full = [
        row
        for row in samples
        if str(row.get("order_type")) == "FOK"
        and str(row.get("predicted_class")) == "FULL"
    ]
    fok_false_full = sum(str(row.get("actual_class")) != "FULL" for row in fok_full)
    return {
        "sample_count": len(samples),
        "confusion_matrix": confusion(samples),
        "false_positive_fill": wilson_interval(false_positive, len(fill_predictions)),
        "fok_false_positive_full": wilson_interval(fok_false_full, len(fok_full)),
        "vwap_error_median_ticks": _format(quantile((row.get("price_error_ticks") for row in samples), 0.50)),
        "vwap_error_p95_ticks": _format(quantile((row.get("price_error_ticks") for row in samples), 0.95)),
        "filled_size_overprediction_p95": _format(
            quantile(
                (
                    max(Decimal("0"), _decimal(row.get("filled_size_relative_error")))
                    for row in samples
                ),
                0.95,
            )
        ),
        "fee_error_max": _format(
            max((_decimal(row.get("fee_error")) for row in samples), default=Decimal("0"))
        ),
    }


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _format(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None
