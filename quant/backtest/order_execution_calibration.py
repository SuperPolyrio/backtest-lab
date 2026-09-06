"""Build calibration samples from external order adapter response events."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from quant.backtest.calibration import build_calibration_report, upsert_calibration_orders
from quant.backtest.calibration_report import build_periodic_calibration_report
from quant.backtest.calibration_samples import build_calibration_samples_from_order_events


READY = "ready"
REVIEW = "review"


def build_order_execution_calibration_report(
    *,
    orders: Iterable[Mapping[str, Any]],
    response_events: Iterable[Mapping[str, Any]],
    source: str = "external-order-adapter-calibration",
    include_open_events: bool = False,
) -> dict[str, Any]:
    """Convert adapter response events into fill calibration samples."""

    order_rows = [dict(order) for order in orders]
    event_rows = [dict(event) for event in response_events]
    samples = build_calibration_samples_from_order_events(
        order_rows,
        event_rows,
        source=source,
        include_open_events=include_open_events,
    )
    calibration_report = build_calibration_report(samples)
    periodic_report = build_periodic_calibration_report(samples)
    status = READY if samples else REVIEW
    reason = "adapter response calibration samples built" if samples else "no calibration samples built from adapter response events"
    return {
        "status": status,
        "reason": reason,
        "source": source,
        "include_open_events": bool(include_open_events),
        "orders_read": len(order_rows),
        "events_read": len(event_rows),
        "samples_built": len(samples),
        "samples_written": 0,
        "samples": samples,
        "calibration_report": calibration_report,
        "periodic_report": periodic_report,
    }


def record_order_execution_calibration_samples(conn: Any, report: Mapping[str, Any]) -> int:
    """Persist adapter calibration samples to quant.quant_backtest_calibration_orders."""

    samples = [dict(sample) for sample in (report.get("samples") or []) if isinstance(sample, Mapping)]
    return upsert_calibration_orders(conn, samples)


def order_execution_calibration_report_to_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    """Return a compact JSON-safe summary suitable for adapter reports."""

    calibration = report.get("calibration_report") if isinstance(report.get("calibration_report"), Mapping) else {}
    periodic = report.get("periodic_report") if isinstance(report.get("periodic_report"), Mapping) else {}
    return {
        "status": report.get("status"),
        "reason": report.get("reason"),
        "source": report.get("source"),
        "include_open_events": report.get("include_open_events"),
        "orders_read": report.get("orders_read", 0),
        "events_read": report.get("events_read", 0),
        "samples_built": report.get("samples_built", 0),
        "samples_written": report.get("samples_written", 0),
        "trust_status": calibration.get("trust_status", "unknown"),
        "trust_reason": calibration.get("trust_reason", "no calibration samples"),
        "requires_recalibration": bool(calibration.get("requires_recalibration")),
        "periodic_bucket_count": len(periodic.get("buckets") or {}) if isinstance(periodic.get("buckets"), Mapping) else 0,
    }
