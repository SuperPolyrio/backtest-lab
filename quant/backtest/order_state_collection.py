"""Collection state helpers for long-running real order state ingestion."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Iterable, Mapping, Sequence


COLLECTION_STATE_TABLE = "quant.real_order_state_collection_state"
DEFAULT_CURSOR_KEYS = ("next_cursor", "nextCursor", "next", "cursor", "nextPageToken", "pageToken")


def build_collection_request_params(
    base_params: Mapping[str, Any],
    state: Mapping[str, Any] | None,
    *,
    since_param: str | None = None,
    cursor_param: str | None = None,
    initial_since: str | None = None,
) -> dict[str, str]:
    params = {str(key): str(value) for key, value in base_params.items() if value not in (None, "")}
    state = state or {}
    cursor = state.get("last_cursor")
    watermark = state.get("last_event_time")
    if cursor_param and cursor not in (None, ""):
        params[cursor_param] = str(cursor)
    elif since_param:
        since_value = watermark or initial_since
        if since_value not in (None, ""):
            params[since_param] = _iso_text(since_value)
    return params


def extract_response_cursor(payload: Any, keys: Sequence[str] = DEFAULT_CURSOR_KEYS) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    pagination = payload.get("pagination")
    if isinstance(pagination, Mapping):
        for key in keys:
            value = pagination.get(key)
            if value not in (None, ""):
                return str(value)
    meta = payload.get("meta")
    if isinstance(meta, Mapping):
        for key in keys:
            value = meta.get(key)
            if value not in (None, ""):
                return str(value)
    return None


def event_time_watermark(events: Iterable[Mapping[str, Any]]) -> str | None:
    max_time: datetime | None = None
    for event in events:
        parsed = _parse_datetime(event.get("event_time"))
        if parsed is None:
            continue
        if max_time is None or parsed > max_time:
            max_time = parsed
    return max_time.isoformat() if max_time is not None else None


def load_real_order_state_collection_state(conn: Any, state_key: str) -> dict[str, Any] | None:
    if not _table_exists(conn):
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.real_order_state_collection_state
            WHERE state_key = %s
            """,
            (state_key,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def load_real_order_state_collection_states(
    conn: Any,
    *,
    source: str | None = None,
    state_key: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if not _table_exists(conn):
        return []
    filters: list[str] = []
    params: list[Any] = []
    if source:
        filters.append("source = %s")
        params.append(source)
    if state_key:
        filters.append("state_key = %s")
        params.append(state_key)
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(max(1, int(limit or 100)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.real_order_state_collection_state
            {where_sql}
            ORDER BY updated_at DESC, state_key ASC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def upsert_real_order_state_collection_state(
    conn: Any,
    *,
    state_key: str,
    source: str,
    endpoint: str | None = None,
    params: Mapping[str, Any] | None = None,
    last_event_time: Any = None,
    last_cursor: str | None = None,
    last_payload_count: int = 0,
    last_events_written: int = 0,
    last_error: str | None = None,
) -> None:
    if not _table_exists(conn):
        raise RuntimeError(f"{COLLECTION_STATE_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.real_order_state_collection_state (
                state_key, source, endpoint, params, last_event_time, last_cursor,
                last_payload_count, last_events_written, last_error, last_success_at, updated_at
            )
            VALUES (
                %(state_key)s, %(source)s, %(endpoint)s, %(params)s::jsonb, %(last_event_time)s,
                %(last_cursor)s, %(last_payload_count)s, %(last_events_written)s, %(last_error)s,
                CASE WHEN %(last_error)s IS NULL THEN now() ELSE NULL END, now()
            )
            ON CONFLICT (state_key)
            DO UPDATE SET
                source = EXCLUDED.source,
                endpoint = COALESCE(EXCLUDED.endpoint, quant.real_order_state_collection_state.endpoint),
                params = EXCLUDED.params,
                last_event_time = COALESCE(EXCLUDED.last_event_time, quant.real_order_state_collection_state.last_event_time),
                last_cursor = COALESCE(EXCLUDED.last_cursor, quant.real_order_state_collection_state.last_cursor),
                last_payload_count = EXCLUDED.last_payload_count,
                last_events_written = EXCLUDED.last_events_written,
                last_error = EXCLUDED.last_error,
                last_success_at = CASE
                    WHEN EXCLUDED.last_error IS NULL THEN now()
                    ELSE quant.real_order_state_collection_state.last_success_at
                END,
                updated_at = now()
            """,
            {
                "state_key": state_key,
                "source": source,
                "endpoint": endpoint,
                "params": json.dumps(dict(params or {}), ensure_ascii=False, default=str),
                "last_event_time": last_event_time,
                "last_cursor": last_cursor,
                "last_payload_count": max(0, int(last_payload_count or 0)),
                "last_events_written": max(0, int(last_events_written or 0)),
                "last_error": last_error,
            },
        )


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quant.real_order_state_collection_state') IS NOT NULL AS exists")
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


def _iso_text(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed is not None else str(value)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value in (None, ""):
        return None
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
