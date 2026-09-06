"""Validate shadow/live order-state events before importing calibration evidence."""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from quant.backtest.order_state import normalize_order_state_event


FILLED_STATUSES = {"FILLED", "PARTIAL_FILLED"}
NO_FILL_STATUSES = {"CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "NO_FILL", "FAILED"}
OPEN_STATUSES = {"OPEN", "ACCEPTED", "SUBMITTED", "PENDING", "UNKNOWN"}
VALIDATION_SCHEMA_VERSION = "fill_first_shadow_live_validation_v1"


def load_shadow_live_event_rows(path: Path) -> list[dict[str, Any]]:
    """Load JSON/JSONL shadow-live order-state rows from disk."""

    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if path.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    parsed = json.loads(text)
    if isinstance(parsed, list):
        return [dict(item) for item in parsed if isinstance(item, Mapping)]
    if isinstance(parsed, Mapping) and isinstance(parsed.get("events"), list):
        return [dict(item) for item in parsed["events"] if isinstance(item, Mapping)]
    if isinstance(parsed, Mapping) and isinstance(parsed.get("event_templates"), list):
        return [dict(item) for item in parsed["event_templates"] if isinstance(item, Mapping)]
    if isinstance(parsed, Mapping):
        return [dict(parsed)]
    raise ValueError(f"unsupported input JSON shape in {path}")


def validate_shadow_live_order_events(
    rows: list[Mapping[str, Any]],
    *,
    require_cost_fields: bool = False,
) -> dict[str, Any]:
    """Return a pre-import validation report for shadow/live evidence rows."""

    checks = [_validate_row(index, row, require_cost_fields=require_cost_fields) for index, row in enumerate(rows, start=1)]
    error_count = sum(len(check["errors"]) for check in checks)
    warning_count = sum(len(check["warnings"]) for check in checks)
    calibration_ready_count = sum(1 for check in checks if check["calibration_ready"])
    filled_count = sum(1 for check in checks if check["live_status"] in FILLED_STATUSES)
    no_fill_count = sum(1 for check in checks if check["live_status"] in NO_FILL_STATUSES)
    open_count = sum(1 for check in checks if check["live_status"] in OPEN_STATUSES)
    status = "ready"
    reason = "valid calibration evidence"
    if not rows:
        status = "fail"
        reason = "no events"
    elif error_count:
        status = "fail"
        reason = "validation errors"
    elif calibration_ready_count == 0:
        status = "review"
        reason = "no terminal calibration-ready events"
    elif warning_count:
        status = "review"
        reason = "valid with warnings"
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "status": status,
        "reason": reason,
        "event_count": len(rows),
        "calibration_ready_count": calibration_ready_count,
        "filled_count": filled_count,
        "no_fill_count": no_fill_count,
        "open_count": open_count,
        "error_count": error_count,
        "warning_count": warning_count,
        "checks": checks,
    }


def shadow_live_validation_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Shadow/Live Event Validation: {report.get('status')}",
        "",
        f"- schema: {report.get('schema_version')}",
        f"- reason: {report.get('reason')}",
        f"- events: {report.get('event_count', 0)}",
        f"- calibration_ready: {report.get('calibration_ready_count', 0)}",
        f"- filled: {report.get('filled_count', 0)}",
        f"- no_fill: {report.get('no_fill_count', 0)}",
        f"- open: {report.get('open_count', 0)}",
        f"- errors: {report.get('error_count', 0)}",
        f"- warnings: {report.get('warning_count', 0)}",
        "",
        "| row | order_id | external_order_id | live_status | calibration_ready | errors | warnings |",
        "| ---: | --- | --- | --- | --- | --- | --- |",
    ]
    for check in report.get("checks") or []:
        lines.append(
            "| {row} | {order_id} | {external_order_id} | {live_status} | {ready} | {errors} | {warnings} |".format(
                row=check.get("row_number"),
                order_id=check.get("order_id") or "",
                external_order_id=check.get("external_order_id") or "",
                live_status=check.get("live_status") or "",
                ready="yes" if check.get("calibration_ready") else "no",
                errors=", ".join(check.get("errors") or []) or "-",
                warnings=", ".join(check.get("warnings") or []) or "-",
            )
        )
    return "\n".join(lines)


def _validate_row(index: int, row: Mapping[str, Any], *, require_cost_fields: bool) -> dict[str, Any]:
    normalized = normalize_order_state_event(row)
    payload = _payload(normalized.get("payload"))
    live_status = _live_status(normalized, payload)
    errors: list[str] = []
    warnings: list[str] = []

    if not normalized.get("run_id"):
        errors.append("missing_run_id")
    if not normalized.get("order_id"):
        errors.append("missing_simulated_order_id")
    if not normalized.get("event_time"):
        errors.append("missing_event_time")
    if not normalized.get("source"):
        errors.append("missing_source")
    if live_status == "UNKNOWN":
        errors.append("missing_live_status")

    external_order_id = _text(normalized.get("external_order_id"))
    if live_status in FILLED_STATUSES:
        if not external_order_id:
            errors.append("missing_external_order_id")
        _require_decimal(payload, "live_fill_price", errors)
        _require_decimal(payload, "live_fill_size", errors)
        for field in ("live_fee", "live_rebate", "live_cash_delta", "live_position_delta", "live_latency_seconds"):
            if field not in payload or payload.get(field) in (None, ""):
                target = errors if require_cost_fields and field in {"live_fee", "live_rebate", "live_cash_delta", "live_position_delta"} else warnings
                target.append(f"missing_{field}")
            elif not _is_decimal(payload.get(field)):
                errors.append(f"invalid_{field}")
    elif live_status in NO_FILL_STATUSES:
        if _has_any(payload, ("live_fill_price", "live_fill_size")):
            warnings.append("no_fill_has_fill_fields")
        if not external_order_id and live_status not in {"REJECTED", "FAILED"}:
            warnings.append("missing_external_order_id")
    elif live_status in OPEN_STATUSES:
        warnings.append("non_terminal_event")

    calibration_ready = not errors and live_status in FILLED_STATUSES | NO_FILL_STATUSES
    return {
        "row_number": index,
        "order_id": _text(normalized.get("order_id")),
        "external_order_id": external_order_id,
        "live_status": live_status,
        "calibration_ready": calibration_ready,
        "errors": errors,
        "warnings": warnings,
    }


def _payload(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _live_status(event: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    explicit = _first(payload, "live_status", "liveStatus", "fill_status", "fillStatus", "status")
    if explicit not in (None, ""):
        return _canonical_status(explicit)
    for field in (
        "api_order_status",
        "chain_order_status",
        "clob_order_status",
        "cancel_status",
        "accepted_status",
        "submit_status",
    ):
        status = _canonical_status(event.get(field))
        if status != "UNKNOWN":
            return status
    return "UNKNOWN"


def _canonical_status(value: Any) -> str:
    text = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    if not text:
        return "UNKNOWN"
    if text in {"NO_FILL", "UNFILLED"}:
        return "NO_FILL"
    if "PARTIAL" in text:
        return "PARTIAL_FILLED"
    if "FILL" in text or "MATCH" in text:
        return "FILLED"
    if "CANCEL" in text:
        return "CANCELED"
    if "REJECT" in text:
        return "REJECTED"
    if "EXPIRE" in text:
        return "EXPIRED"
    if "FAIL" in text:
        return "FAILED"
    if text in {"OPEN", "ACCEPTED", "SUBMITTED", "PENDING"}:
        return text
    return text


def _require_decimal(payload: Mapping[str, Any], field: str, errors: list[str]) -> None:
    if field not in payload or payload.get(field) in (None, ""):
        errors.append(f"missing_{field}")
    elif not _is_decimal(payload.get(field)):
        errors.append(f"invalid_{field}")


def _is_decimal(value: Any) -> bool:
    try:
        Decimal(str(value))
    except (InvalidOperation, ValueError):
        return False
    return True


def _has_any(payload: Mapping[str, Any], fields: tuple[str, ...]) -> bool:
    return any(payload.get(field) not in (None, "") for field in fields)


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
