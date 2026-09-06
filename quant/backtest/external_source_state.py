"""State and health checks for fill-first external source imports."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Mapping, Sequence


IMPORT_STATE_TABLE = "quant.external_source_import_state"
READY = "ready"
REVIEW = "review"
UNKNOWN = "unknown"


def upsert_external_source_import_state(
    conn: Any,
    *,
    state_key: str,
    source_type: str,
    source: str,
    endpoint: str | None = None,
    params: Mapping[str, Any] | None = None,
    last_payload_count: int = 0,
    last_rows_written: int = 0,
    last_error: str | None = None,
) -> None:
    if not _table_exists(conn):
        raise RuntimeError(f"{IMPORT_STATE_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.external_source_import_state (
                state_key, source_type, source, endpoint, params,
                last_payload_count, last_rows_written, last_error,
                last_success_at, updated_at
            )
            VALUES (
                %(state_key)s, %(source_type)s, %(source)s, %(endpoint)s, %(params)s::jsonb,
                %(last_payload_count)s, %(last_rows_written)s, %(last_error)s,
                CASE WHEN %(last_error)s IS NULL THEN now() ELSE NULL END, now()
            )
            ON CONFLICT (state_key)
            DO UPDATE SET
                source_type = EXCLUDED.source_type,
                source = EXCLUDED.source,
                endpoint = COALESCE(EXCLUDED.endpoint, quant.external_source_import_state.endpoint),
                params = EXCLUDED.params,
                last_payload_count = EXCLUDED.last_payload_count,
                last_rows_written = EXCLUDED.last_rows_written,
                last_error = EXCLUDED.last_error,
                last_success_at = CASE
                    WHEN EXCLUDED.last_error IS NULL THEN now()
                    ELSE quant.external_source_import_state.last_success_at
                END,
                updated_at = now()
            """,
            {
                "state_key": state_key,
                "source_type": source_type,
                "source": source,
                "endpoint": endpoint,
                "params": json.dumps(dict(params or {}), ensure_ascii=False, default=str),
                "last_payload_count": max(0, int(last_payload_count or 0)),
                "last_rows_written": max(0, int(last_rows_written or 0)),
                "last_error": last_error,
            },
        )


def load_external_source_import_states(
    conn: Any,
    *,
    source_type: str | None = None,
    state_key: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if not _table_exists(conn):
        return []
    filters: list[str] = []
    params: list[Any] = []
    if source_type:
        filters.append("source_type = %s")
        params.append(source_type)
    if state_key:
        filters.append("state_key = %s")
        params.append(state_key)
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(max(1, int(limit or 100)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.external_source_import_state
            {where_sql}
            ORDER BY updated_at DESC, state_key ASC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def evaluate_external_source_import_health(
    states: Sequence[Mapping[str, Any]],
    *,
    now: Any = None,
    max_stale_seconds: int = 86400,
    min_rows_written: int = 0,
    require_success: bool = True,
) -> dict[str, Any]:
    checked_at = _parse_datetime(now) or datetime.now(timezone.utc)
    items = [
        evaluate_external_source_import_item(
            state,
            now=checked_at,
            max_stale_seconds=max_stale_seconds,
            min_rows_written=min_rows_written,
            require_success=require_success,
        )
        for state in states
    ]
    if not items:
        return {
            "status": UNKNOWN,
            "reason": "no_import_state_rows",
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


def evaluate_external_source_import_item(
    state: Mapping[str, Any],
    *,
    now: datetime,
    max_stale_seconds: int,
    min_rows_written: int,
    require_success: bool,
) -> dict[str, Any]:
    last_success_at = _parse_datetime(state.get("last_success_at"))
    updated_at = _parse_datetime(state.get("updated_at"))
    age_anchor = last_success_at if last_success_at is not None else updated_at
    age_seconds = None if age_anchor is None else max(0.0, (now - age_anchor).total_seconds())
    last_error = _text_or_none(state.get("last_error"))
    last_rows_written = _to_int(state.get("last_rows_written"), default=0)
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
    elif last_rows_written < max(0, int(min_rows_written or 0)):
        status = REVIEW
        reason = "below_min_rows_written"
    return {
        "state_key": state.get("state_key"),
        "source_type": state.get("source_type"),
        "source": state.get("source"),
        "endpoint": state.get("endpoint"),
        "status": status,
        "reason": reason,
        "age_seconds": age_seconds,
        "last_payload_count": _to_int(state.get("last_payload_count"), default=0),
        "last_rows_written": last_rows_written,
        "last_error": last_error,
        "last_success_at": _iso_or_none(last_success_at),
        "updated_at": _iso_or_none(updated_at),
    }


def external_source_import_health_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"status: {report.get('status')}",
        f"reason: {report.get('reason')}",
        f"checked_at: {report.get('checked_at')}",
        f"states: {report.get('state_count', 0)} ready={report.get('ready_count', 0)} review={report.get('review_count', 0)} unknown={report.get('unknown_count', 0)}",
        "",
        "| state_key | type | source | status | reason | age_seconds | rows_written | last_success_at | last_error |",
        "| --- | --- | --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for item in report.get("items", []):
        lines.append(
            "| {state_key} | {source_type} | {source} | {status} | {reason} | {age_seconds} | {rows_written} | {last_success_at} | {last_error} |".format(
                state_key=_md(item.get("state_key")),
                source_type=_md(item.get("source_type")),
                source=_md(item.get("source")),
                status=_md(item.get("status")),
                reason=_md(item.get("reason")),
                age_seconds="" if item.get("age_seconds") is None else f"{float(item['age_seconds']):.0f}",
                rows_written=item.get("last_rows_written", 0),
                last_success_at=_md(item.get("last_success_at")),
                last_error=_md(item.get("last_error")),
            )
        )
    return "\n".join(lines)


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quant.external_source_import_state') IS NOT NULL AS exists")
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


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
    text = str(value or "")
    return text.replace("|", "\\|")
