"""Periodic fill calibration reports for the builtin backtest engine."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Iterable, Mapping, Sequence

from quant.backtest.calibration import build_calibration_report, empty_calibration_report
from quant.backtest.execution_profile_calibration import execution_profile_suggestions_from_report


DEFAULT_BUCKET_FIELDS = (
    "market_category",
    "market_slug",
    "role",
    "side",
    "liquidity_bucket",
    "volatility_bucket",
    "time_to_expiry_bucket",
)

BUCKET_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "market_category": ("market_category", "marketCategory", "category", "event_category", "eventCategory"),
}


def build_periodic_calibration_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    bucket_fields: Sequence[str] = DEFAULT_BUCKET_FIELDS,
    min_bucket_samples: int = 1,
    min_suggestion_samples: int | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    samples = [dict(row) for row in rows]
    overall = build_calibration_report(samples) if samples else empty_calibration_report()
    buckets = {
        field: _bucket_reports(samples, field, min_bucket_samples=max(1, int(min_bucket_samples)))
            for field in bucket_fields
    }
    report = {
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "sample_count": len(samples),
        "overall": overall,
        "buckets": buckets,
        "recommendations": calibration_recommendations(overall, buckets),
    }
    suggestion_minimum = max(1, int(min_suggestion_samples if min_suggestion_samples is not None else min_bucket_samples))
    report["execution_profile_suggestions"] = execution_profile_suggestions_from_report(report, min_samples=suggestion_minimum)
    return report


def calibration_recommendations(overall: Mapping[str, Any], buckets: Mapping[str, Sequence[Mapping[str, Any]]]) -> list[str]:
    recommendations: list[str] = []
    if overall.get("trust_status") == "unknown":
        recommendations.append("Collect live/shadow calibration samples before trusting fill-level backtests.")
    if overall.get("trust_status") == "review":
        recommendations.append(f"Review execution profile thresholds: {overall.get('trust_reason') or 'calibration drift detected'}.")
    review_buckets = [
        f"{field}={bucket.get('bucket')}"
        for field, items in buckets.items()
        for bucket in items
        if bucket.get("trust_status") == "review"
    ]
    if review_buckets:
        preview = ", ".join(review_buckets[:8])
        suffix = "" if len(review_buckets) <= 8 else f", +{len(review_buckets) - 8} more"
        recommendations.append(f"Segment-specific recalibration needed for {preview}{suffix}.")
    if not recommendations:
        recommendations.append("Calibration is within current fill-first thresholds.")
    return recommendations


def calibration_report_to_markdown(report: Mapping[str, Any], *, max_bucket_rows: int = 12) -> str:
    overall = report.get("overall") if isinstance(report.get("overall"), Mapping) else {}
    lines = [
        "# Fill Calibration Report",
        "",
        f"- Generated: {report.get('generated_at', '-')}",
        f"- Samples: {report.get('sample_count', 0)}",
        f"- Trust: {overall.get('trust_status', 'unknown')} - {overall.get('trust_reason', '-')}",
        "",
        "## Overall",
        "",
        "| Metric | Value |",
        "| --- | --- |",
    ]
    for key in (
        "sample_count",
        "status_error_count",
        "status_error_rate",
        "avg_price_error",
        "max_price_error",
        "avg_size_error",
        "avg_slippage_error",
        "avg_fee_error",
        "avg_rebate_error",
        "avg_cash_error",
        "avg_position_error",
        "avg_pnl_error",
        "max_pnl_error",
        "avg_latency_error_seconds",
        "requires_recalibration",
    ):
        lines.append(f"| {key} | {_text(overall.get(key))} |")

    lines.extend(["", "## Recommendations", ""])
    for item in report.get("recommendations") or []:
        lines.append(f"- {item}")

    suggestions = report.get("execution_profile_suggestions")
    lines.extend(["", "## Execution Profile Suggestions", ""])
    if not suggestions:
        lines.append("No execution profile changes suggested for the selected samples.")
    else:
        lines.extend([
            "| Scope | Bucket | Samples | Profile | Latency Sec | Latency Blocks | Adverse Slip | Fill Haircut | Reason |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | --- |",
        ])
        for suggestion in suggestions:
            if not isinstance(suggestion, Mapping):
                continue
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
                    _text(recommended.get("latency_blocks_floor")),
                    _text(recommended.get("adverse_slippage_price_floor")),
                    _text(recommended.get("fill_probability_haircut_pct_floor")),
                    _text(suggestion.get("trust_reason")),
                ])
                + " |"
            )

    buckets = report.get("buckets") if isinstance(report.get("buckets"), Mapping) else {}
    lines.extend(["", "## Buckets", ""])
    for field, items in buckets.items():
        lines.extend([
            f"### {field}",
            "",
            "| Bucket | Trust | Samples | Status Err % | Avg Price Err | Avg Slip Err | Avg PnL Err | Avg Latency Err | Reason |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ])
        for bucket in list(items)[:max(1, int(max_bucket_rows))]:
            lines.append(
                "| "
                + " | ".join([
                    _text(bucket.get("bucket")),
                    _text(bucket.get("trust_status")),
                    _text(bucket.get("sample_count")),
                    _text(bucket.get("status_error_rate")),
                    _text(bucket.get("avg_price_error")),
                    _text(bucket.get("avg_slippage_error")),
                    _text(bucket.get("avg_pnl_error")),
                    _text(bucket.get("avg_latency_error_seconds")),
                    _text(bucket.get("trust_reason")),
                ])
                + " |"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def calibration_report_to_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n"


def _bucket_reports(rows: list[dict[str, Any]], field: str, *, min_bucket_samples: int) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        bucket = _bucket_value(_row_value(row, field))
        grouped.setdefault(bucket, []).append(row)
    reports: list[dict[str, Any]] = []
    for bucket, bucket_rows in grouped.items():
        if len(bucket_rows) < min_bucket_samples:
            continue
        summary = build_calibration_report(bucket_rows)
        summary["bucket"] = bucket
        summary["bucket_field"] = field
        reports.append(summary)
    trust_rank = {"review": 0, "unknown": 1, "ready": 2}
    return sorted(
        reports,
        key=lambda row: (
            trust_rank.get(str(row.get("trust_status")), 9),
            -int(row.get("sample_count") or 0),
            str(row.get("bucket") or ""),
        ),
    )


def _bucket_value(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "unknown"


def _row_value(row: Mapping[str, Any], field: str) -> Any:
    aliases = BUCKET_FIELD_ALIASES.get(field, (field,))
    for key in aliases:
        value = row.get(key)
        if value not in (None, ""):
            return value
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        for key in aliases:
            value = payload.get(key)
            if value not in (None, ""):
                return value
        context = payload.get("context")
        if isinstance(context, Mapping):
            for key in aliases:
                value = context.get(key)
                if value not in (None, ""):
                    return value
    return None


def _text(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|")
