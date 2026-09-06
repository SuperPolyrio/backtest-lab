"""Replay-consistency reports for L2 book state and OrderFilled evidence.

The execution guidance requires a data-level check before DEPTH execution is
trusted: historical OrderFilled rows should be explainable by nearby L2 book
state, and LOB size decreases should not be blindly treated as trades.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"


def build_l2_replay_consistency_report(
    alignment: Mapping[str, Any] | None,
    *,
    book_decrease_rows: Sequence[Mapping[str, Any]] | None = None,
    max_ready_unexplained_orderfilled_pct: Decimal = Decimal("5"),
    max_ready_unexplained_book_decrease_pct: Decimal = Decimal("25"),
) -> dict[str, Any]:
    """Summarize whether PMXT/L2 replay can explain OrderFilled evidence.

    ``alignment`` is intentionally compatible with
    ``orderfilled_l2_alignment_report_payload`` from ``scripts/validate_pmxt_l2_raw.py``.
    ``book_decrease_rows`` is optional and lets callers provide already-computed
    LOB decrease rows: each row should contain ``old_size``, ``new_size`` and
    ``orderfilled_size``.
    """

    payload = dict(alignment or {})
    orderfilled_rows = _int(payload.get("orderfilled_rows_matched") or payload.get("orderfilled_rows_seen"))
    aligned = _int(payload.get("aligned_count"))
    price_outside = _int(payload.get("price_outside_spread_count"))
    missing_timestamp = _int(payload.get("missing_timestamp_count"))
    missing_l2 = _int(payload.get("missing_l2_before_fill_count"))
    stale_l2 = _int(payload.get("stale_l2_count"))
    depth_checked = _int(payload.get("depth_checked_count"))
    depth_sufficient = _int(payload.get("depth_sufficient_count"))
    crossable_depth_sufficient = _int(payload.get("crossable_depth_sufficient_count"))
    unexplained_orderfilled = max(0, orderfilled_rows - aligned) + price_outside
    book_decrease = _book_decrease_summary(book_decrease_rows or ())
    book_age = _book_age_distribution(payload.get("sample_rows") if isinstance(payload.get("sample_rows"), list) else [])
    coverage_ratio = _ratio(aligned, orderfilled_rows)
    price_compatible_ratio = _ratio(max(0, aligned - price_outside), orderfilled_rows)
    depth_sufficient_ratio = _ratio(depth_sufficient, depth_checked)
    crossable_depth_sufficient_ratio = _ratio(crossable_depth_sufficient, depth_checked)
    unexplained_orderfilled_ratio = _ratio(unexplained_orderfilled, orderfilled_rows)
    reasons: list[str] = []
    if orderfilled_rows <= 0:
        reasons.append("no_orderfilled_rows")
    if aligned <= 0:
        reasons.append("no_aligned_l2_book_state")
    if missing_timestamp:
        reasons.append("missing_fill_timestamps")
    if missing_l2:
        reasons.append("missing_l2_before_fill")
    if stale_l2:
        reasons.append("stale_l2_before_fill")
    if price_outside:
        reasons.append("orderfilled_price_outside_spread")
    if _decimal(unexplained_orderfilled_ratio) > max_ready_unexplained_orderfilled_pct:
        reasons.append("unexplained_orderfilled_ratio_high")
    if (
        book_decrease["decrease_count"] > 0
        and _decimal(book_decrease["unexplained_book_decrease_ratio"]) > max_ready_unexplained_book_decrease_pct
    ):
        reasons.append("unexplained_book_decrease_ratio_high")
    status = MISSING if orderfilled_rows <= 0 or aligned <= 0 else REVIEW if reasons else READY
    return {
        "schema_version": "l2_replay_consistency_v1",
        "status": status,
        "coverage_ratio": coverage_ratio,
        "orderfilled_matched_to_book_ratio": coverage_ratio,
        "price_compatible_ratio": price_compatible_ratio,
        "depth_sufficient_ratio": depth_sufficient_ratio,
        "crossable_depth_sufficient_ratio": crossable_depth_sufficient_ratio,
        "unexplained_orderfilled_ratio": unexplained_orderfilled_ratio,
        "book_age_distribution": book_age,
        "unexplained_book_decrease_ratio": book_decrease["unexplained_book_decrease_ratio"],
        "book_decrease_summary": book_decrease,
        "counts": {
            "orderfilled_rows": orderfilled_rows,
            "aligned_count": aligned,
            "price_outside_spread_count": price_outside,
            "missing_timestamp_count": missing_timestamp,
            "missing_l2_before_fill_count": missing_l2,
            "stale_l2_count": stale_l2,
            "depth_checked_count": depth_checked,
            "depth_sufficient_count": depth_sufficient,
            "crossable_depth_sufficient_count": crossable_depth_sufficient,
            "unexplained_orderfilled_count": unexplained_orderfilled,
        },
        "alignment_status": str(payload.get("status") or ""),
        "reason": "; ".join(reasons) if reasons else "L2 replay and OrderFilled evidence are mutually explainable within configured thresholds.",
        "review_reasons": reasons,
        "sample_rows": list(payload.get("sample_rows") or [])[:20],
    }


def l2_replay_consistency_to_markdown(report: Mapping[str, Any]) -> str:
    counts = report.get("counts") if isinstance(report.get("counts"), Mapping) else {}
    book_age = report.get("book_age_distribution") if isinstance(report.get("book_age_distribution"), Mapping) else {}
    lines = [
        f"# L2 Replay Consistency: {report.get('status')}",
        "",
        f"- coverage_ratio: {report.get('coverage_ratio')}",
        f"- orderfilled_matched_to_book_ratio: {report.get('orderfilled_matched_to_book_ratio')}",
        f"- price_compatible_ratio: {report.get('price_compatible_ratio')}",
        f"- depth_sufficient_ratio: {report.get('depth_sufficient_ratio')}",
        f"- unexplained_orderfilled_ratio: {report.get('unexplained_orderfilled_ratio')}",
        f"- unexplained_book_decrease_ratio: {report.get('unexplained_book_decrease_ratio')}",
        f"- book_age_ms_p50: {book_age.get('p50_ms')}",
        f"- book_age_ms_p95: {book_age.get('p95_ms')}",
        "",
        "## Counts",
    ]
    for key in (
        "orderfilled_rows",
        "aligned_count",
        "price_outside_spread_count",
        "missing_l2_before_fill_count",
        "stale_l2_count",
        "unexplained_orderfilled_count",
    ):
        lines.append(f"- {key}: {counts.get(key, 0)}")
    lines.extend(["", "## Reason", str(report.get("reason") or "")])
    return "\n".join(lines)


def _book_decrease_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    decrease_count = 0
    total_decrease = Decimal("0")
    explained_by_orderfilled = Decimal("0")
    unexplained = Decimal("0")
    for row in rows:
        old_size = max(Decimal("0"), _decimal(row.get("old_size")))
        new_size = max(Decimal("0"), _decimal(row.get("new_size")))
        decrease = max(Decimal("0"), old_size - new_size)
        if decrease <= 0:
            continue
        orderfilled_size = max(Decimal("0"), _decimal(row.get("orderfilled_size")))
        explained = min(decrease, orderfilled_size)
        decrease_count += 1
        total_decrease += decrease
        explained_by_orderfilled += explained
        unexplained += max(Decimal("0"), decrease - explained)
    return {
        "decrease_count": decrease_count,
        "total_decrease_size": _decimal_text(total_decrease),
        "orderfilled_explained_decrease_size": _decimal_text(explained_by_orderfilled),
        "unexplained_decrease_size": _decimal_text(unexplained),
        "unexplained_book_decrease_ratio": _ratio_decimal(unexplained, total_decrease),
    }


def _book_age_distribution(sample_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lags = sorted(_int(row.get("lag_ms")) for row in sample_rows if row.get("lag_ms") is not None and _int(row.get("lag_ms")) >= 0)
    if not lags:
        return {"sample_count": 0, "min_ms": None, "p50_ms": None, "p95_ms": None, "max_ms": None}
    return {
        "sample_count": len(lags),
        "min_ms": lags[0],
        "p50_ms": _percentile(lags, Decimal("0.50")),
        "p95_ms": _percentile(lags, Decimal("0.95")),
        "max_ms": lags[-1],
    }


def _percentile(values: Sequence[int], q: Decimal) -> int:
    if not values:
        return 0
    idx = int((Decimal(len(values) - 1) * q).to_integral_value(rounding=ROUND_HALF_UP))
    return int(values[max(0, min(len(values) - 1, idx))])


def _ratio(numerator: int, denominator: int) -> str:
    return _ratio_decimal(Decimal(max(0, numerator)), Decimal(max(0, denominator)))


def _ratio_decimal(numerator: Decimal, denominator: Decimal) -> str:
    if denominator <= 0:
        return "0"
    return _decimal_text((numerator / denominator * Decimal("100")).quantize(Decimal("0.0001")))


def _decimal_text(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.0000000001")), "f").rstrip("0").rstrip(".") or "0"


def _decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")
    return parsed if parsed.is_finite() else Decimal("0")


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0
