"""End-to-end shadow/live calibration pipeline for fill-first backtests."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Iterable, Mapping

from quant.backtest.calibration import build_calibration_report, upsert_calibration_orders
from quant.backtest.calibration_report import build_periodic_calibration_report
from quant.backtest.calibration_samples import build_calibration_samples_from_order_events
from quant.backtest.l2_shadow_calibration import build_l2_shadow_calibration_report
from quant.backtest.order_state import normalize_order_state_event, upsert_real_order_state_events
from quant.backtest.shadow_live_validation import validate_shadow_live_order_events


PIPELINE_SCHEMA_VERSION = "fill_first_shadow_live_pipeline_v1"


@dataclass(frozen=True)
class ShadowLivePipelineOptions:
    source: str = "live-shadow"
    calibration_source: str = "live-shadow-calibration"
    run_id: int | None = None
    require_cost_fields: bool = True
    include_open_events: bool = False
    dry_run: bool = True


def run_shadow_live_calibration_pipeline(
    *,
    orders: Iterable[Mapping[str, Any]],
    raw_events: Iterable[Mapping[str, Any]],
    options: ShadowLivePipelineOptions | None = None,
    conn: Any | None = None,
) -> dict[str, Any]:
    opts = options or ShadowLivePipelineOptions()
    normalized_events = [
        normalize_order_state_event(event, source=opts.source, run_id=opts.run_id)
        for event in raw_events
    ]
    validation = validate_shadow_live_order_events(
        normalized_events,
        require_cost_fields=opts.require_cost_fields,
    )
    if validation["status"] == "fail":
        return _pipeline_report(
            status="fail",
            reason="shadow/live validation failed",
            options=opts,
            orders=[],
            events=normalized_events,
            validation=validation,
            samples=[],
            events_written=0,
            samples_written=0,
        )

    order_rows = [dict(order) for order in orders]
    samples = build_calibration_samples_from_order_events(
        order_rows,
        normalized_events,
        source=opts.calibration_source,
        include_open_events=opts.include_open_events,
    )
    events_written = 0
    samples_written = 0
    if not opts.dry_run:
        if conn is None:
            raise ValueError("conn is required when dry_run=False")
        events_written = upsert_real_order_state_events(conn, normalized_events)
        samples_written = upsert_calibration_orders(conn, samples)

    status = "ready"
    reason = "calibration samples built"
    if validation["status"] == "review":
        status = "review"
        reason = "shadow/live validation has warnings"
    if not samples:
        status = "review"
        reason = "no calibration samples built"
    return _pipeline_report(
        status=status,
        reason=reason,
        options=opts,
        orders=order_rows,
        events=normalized_events,
        validation=validation,
        samples=samples,
        events_written=events_written,
        samples_written=samples_written,
    )


def shadow_live_pipeline_report_to_markdown(report: Mapping[str, Any]) -> str:
    calibration = report.get("calibration_report") if isinstance(report.get("calibration_report"), Mapping) else {}
    l2_calibration = report.get("l2_shadow_calibration_report") if isinstance(report.get("l2_shadow_calibration_report"), Mapping) else {}
    validation = report.get("validation") if isinstance(report.get("validation"), Mapping) else {}
    lines = [
        f"# Shadow/Live Calibration Pipeline: {report.get('status')}",
        "",
        f"- schema: {report.get('schema_version')}",
        f"- reason: {report.get('reason')}",
        f"- run_id: {report.get('run_id')}",
        f"- dry_run: {report.get('dry_run')}",
        f"- orders_read: {report.get('orders_read', 0)}",
        f"- events_read: {report.get('events_read', 0)}",
        f"- events_written: {report.get('events_written', 0)}",
        f"- samples_built: {report.get('samples_built', 0)}",
        f"- samples_written: {report.get('samples_written', 0)}",
        f"- validation: {validation.get('status')} ({validation.get('error_count', 0)} errors, {validation.get('warning_count', 0)} warnings)",
        f"- calibration_trust: {calibration.get('trust_status', 'unknown')} - {calibration.get('trust_reason', '-')}",
        f"- l2_shadow_calibration: {l2_calibration.get('status', 'missing')} - {l2_calibration.get('reason', '-')}",
        "",
        "## Calibration Summary",
        "",
        "| Metric | Value |",
        "| --- | --- |",
    ]
    for key in (
        "sample_count",
        "status_error_count",
        "status_error_rate",
        "avg_price_error",
        "avg_size_error",
        "avg_slippage_error",
        "avg_fee_error",
        "avg_rebate_error",
        "avg_cash_error",
        "avg_position_error",
        "avg_latency_error_seconds",
        "requires_recalibration",
    ):
        lines.append(f"| {key} | {_text(calibration.get(key))} |")
    lines.extend([
        "",
        "## L2 Shadow Calibration",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| taker_classification_accuracy_pct | {_text((l2_calibration.get('taker') or {}).get('classification_accuracy_pct') if isinstance(l2_calibration.get('taker'), Mapping) else None)} |",
        f"| taker_avg_fill_price_error | {_text((l2_calibration.get('taker') or {}).get('avg_fill_price_error') if isinstance(l2_calibration.get('taker'), Mapping) else None)} |",
        f"| maker_brier_score | {_text((l2_calibration.get('maker') or {}).get('brier_score') if isinstance(l2_calibration.get('maker'), Mapping) else None)} |",
        f"| maker_false_positive_fill_rate_pct | {_text((l2_calibration.get('maker') or {}).get('false_positive_fill_rate_pct') if isinstance(l2_calibration.get('maker'), Mapping) else None)} |",
    ])
    return "\n".join(lines) + "\n"


def shadow_live_pipeline_report_to_json(report: Mapping[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n"


def _pipeline_report(
    *,
    status: str,
    reason: str,
    options: ShadowLivePipelineOptions,
    orders: list[dict[str, Any]],
    events: list[dict[str, Any]],
    validation: Mapping[str, Any],
    samples: list[dict[str, Any]],
    events_written: int,
    samples_written: int,
) -> dict[str, Any]:
    calibration_report = build_calibration_report(samples)
    periodic_report = build_periodic_calibration_report(samples)
    l2_shadow_calibration_report = build_l2_shadow_calibration_report(samples)
    return {
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "status": status,
        "reason": reason,
        "run_id": options.run_id,
        "source": options.source,
        "calibration_source": options.calibration_source,
        "dry_run": options.dry_run,
        "orders_read": len(orders),
        "events_read": len(events),
        "events_written": events_written,
        "samples_built": len(samples),
        "samples_written": samples_written,
        "validation": dict(validation),
        "calibration_report": calibration_report,
        "periodic_report": periodic_report,
        "l2_shadow_calibration_report": l2_shadow_calibration_report,
    }


def _text(value: Any) -> str:
    if value is None:
        return "-"
    return str(value).replace("|", "\\|")
