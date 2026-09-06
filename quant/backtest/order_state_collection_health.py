"""Health checks for long-running real order state collection."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
UNKNOWN = "unknown"


def evaluate_collection_state_health(
    states: Sequence[Mapping[str, Any]],
    *,
    now: Any = None,
    max_stale_seconds: int = 900,
    min_events_written: int = 0,
    require_success: bool = True,
) -> dict[str, Any]:
    checked_at = _parse_datetime(now) or datetime.now(timezone.utc)
    items = [
        evaluate_collection_state_item(
            state,
            now=checked_at,
            max_stale_seconds=max_stale_seconds,
            min_events_written=min_events_written,
            require_success=require_success,
        )
        for state in states
    ]
    if not items:
        return {
            "status": UNKNOWN,
            "reason": "no_collection_state_rows",
            "checked_at": checked_at.isoformat(),
            "state_count": 0,
            "ready_count": 0,
            "review_count": 0,
            "unknown_count": 0,
            "stale_count": 0,
            "error_count": 0,
            "items": [],
        }

    review_count = sum(1 for item in items if item["status"] == REVIEW)
    unknown_count = sum(1 for item in items if item["status"] == UNKNOWN)
    ready_count = sum(1 for item in items if item["status"] == READY)
    status = REVIEW if review_count else UNKNOWN if unknown_count else READY
    return {
        "status": status,
        "reason": _summary_reason(items, status),
        "checked_at": checked_at.isoformat(),
        "state_count": len(items),
        "ready_count": ready_count,
        "review_count": review_count,
        "unknown_count": unknown_count,
        "stale_count": sum(1 for item in items if item["reason"] == "stale_success"),
        "error_count": sum(1 for item in items if item["reason"] == "last_error"),
        "items": items,
    }


def evaluate_collection_state_item(
    state: Mapping[str, Any],
    *,
    now: datetime,
    max_stale_seconds: int,
    min_events_written: int,
    require_success: bool,
) -> dict[str, Any]:
    last_success_at = _parse_datetime(state.get("last_success_at"))
    updated_at = _parse_datetime(state.get("updated_at"))
    age_anchor = last_success_at if last_success_at is not None else updated_at
    age_seconds = None if age_anchor is None else max(0.0, (now - age_anchor).total_seconds())
    last_error = _text_or_none(state.get("last_error"))
    last_events_written = _to_int(state.get("last_events_written"), default=0)

    status = READY
    reason = "ready"
    if last_error:
        status = REVIEW
        reason = "last_error"
    elif require_success and last_success_at is None:
        status = UNKNOWN
        reason = "missing_success"
    elif age_seconds is None:
        status = UNKNOWN
        reason = "missing_timestamp"
    elif max_stale_seconds >= 0 and age_seconds > max_stale_seconds:
        status = REVIEW
        reason = "stale_success"
    elif last_events_written < max(0, int(min_events_written or 0)):
        status = REVIEW
        reason = "below_min_events_written"

    return {
        "state_key": state.get("state_key"),
        "source": state.get("source"),
        "endpoint": state.get("endpoint"),
        "status": status,
        "reason": reason,
        "age_seconds": age_seconds,
        "last_event_time": _iso_or_none(state.get("last_event_time")),
        "last_cursor": state.get("last_cursor"),
        "last_payload_count": _to_int(state.get("last_payload_count"), default=0),
        "last_events_written": last_events_written,
        "last_error": last_error,
        "last_success_at": _iso_or_none(last_success_at),
        "updated_at": _iso_or_none(updated_at),
    }


def collection_health_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"status: {report.get('status')}",
        f"reason: {report.get('reason')}",
        f"checked_at: {report.get('checked_at')}",
        f"states: {report.get('state_count', 0)} ready={report.get('ready_count', 0)} review={report.get('review_count', 0)} unknown={report.get('unknown_count', 0)}",
        "",
        "| state_key | source | status | reason | age_seconds | events_written | last_success_at | last_error |",
        "| --- | --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for item in report.get("items", []):
        lines.append(
            "| {state_key} | {source} | {status} | {reason} | {age_seconds} | {events_written} | {last_success_at} | {last_error} |".format(
                state_key=_md(item.get("state_key")),
                source=_md(item.get("source")),
                status=_md(item.get("status")),
                reason=_md(item.get("reason")),
                age_seconds="" if item.get("age_seconds") is None else f"{float(item['age_seconds']):.0f}",
                events_written=item.get("last_events_written", 0),
                last_success_at=_md(item.get("last_success_at")),
                last_error=_md(item.get("last_error")),
            )
        )
    return "\n".join(lines)


def _summary_reason(items: Sequence[Mapping[str, Any]], status: str) -> str:
    if status == READY:
        return "all_ready"
    for item in items:
        if item.get("status") == status:
            return str(item.get("reason") or status)
    return status


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value in (None, ""):
        return None
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_or_none(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed is not None else None


def _text_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _to_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _md(value: Any) -> str:
    text = str(value if value is not None else "")
    return text.replace("|", "\\|")
