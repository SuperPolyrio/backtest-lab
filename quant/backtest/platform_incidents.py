"""External platform incident timeline for fill-first backtest diagnostics."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Iterable, Mapping, Sequence


INCIDENT_TABLE = "quant.platform_incidents"

ALIASES: dict[str, tuple[str, ...]] = {
    "incident_key": ("incident_key", "incidentKey", "id", "key"),
    "source": ("source",),
    "severity": ("severity", "level"),
    "component": ("component", "service", "system"),
    "title": ("title", "name", "summary"),
    "description": ("description", "details", "body"),
    "market_slug": ("market_slug", "marketSlug"),
    "token_id": ("token_id", "tokenId", "asset_id", "assetId"),
    "token_side": ("token_side", "tokenSide"),
    "start_ts": ("start_ts", "startTs", "start_time", "startTime", "start", "from_ts", "fromTs"),
    "end_ts": ("end_ts", "endTs", "end_time", "endTime", "end", "to_ts", "toTs"),
    "start_block": ("start_block", "startBlock", "from_block", "fromBlock"),
    "end_block": ("end_block", "endBlock", "to_block", "toBlock"),
    "payload": ("payload", "raw", "raw_payload", "rawPayload"),
}


def normalize_platform_incident(row: Mapping[str, Any], *, source: str | None = None) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field, aliases in ALIASES.items():
        value = _first_value(row, aliases)
        if value not in (None, ""):
            normalized[field] = value
    if source:
        normalized["source"] = source
    normalized.setdefault("source", "manual")
    normalized["severity"] = _flag_key(normalized.get("severity") or "info")
    normalized["component"] = _flag_key(normalized.get("component") or "platform")
    normalized.setdefault("title", str(normalized.get("incident_key") or "platform incident"))
    normalized.setdefault("incident_key", _default_incident_key(normalized))
    payload = normalized.get("payload")
    normalized["payload"] = payload if isinstance(payload, dict) else dict(row)
    return normalized


def build_platform_incident_report(incidents: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [normalize_platform_incident(row) for row in incidents]
    severity_counts: dict[str, int] = {}
    component_counts: dict[str, int] = {}
    flags: dict[str, int] = {}
    compact: list[dict[str, Any]] = []
    for row in rows:
        severity = str(row.get("severity") or "info")
        component = str(row.get("component") or "platform")
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        component_counts[component] = component_counts.get(component, 0) + 1
        flags[f"platform_incident_{component}"] = flags.get(f"platform_incident_{component}", 0) + 1
        if severity in {"warning", "error", "critical"}:
            flag = f"platform_incident_{component}_{severity}"
            flags[flag] = flags.get(flag, 0) + 1
        compact.append({
            "incident_key": row.get("incident_key"),
            "source": row.get("source"),
            "severity": severity,
            "component": component,
            "title": row.get("title"),
            "market_slug": row.get("market_slug"),
            "token_side": row.get("token_side"),
            "start_ts": _iso_or_none(row.get("start_ts")),
            "end_ts": _iso_or_none(row.get("end_ts")),
            "start_block": _int_or_none(row.get("start_block")),
            "end_block": _int_or_none(row.get("end_block")),
        })
    return {
        "source": INCIDENT_TABLE,
        "incident_count": len(rows),
        "severity_counts": dict(sorted(severity_counts.items())),
        "component_counts": dict(sorted(component_counts.items())),
        "environment_flags": dict(sorted(flags.items())),
        "incidents": compact[:50],
    }


def upsert_platform_incidents(conn: Any, incidents: Iterable[Mapping[str, Any]]) -> int:
    rows = [normalize_platform_incident(row) for row in incidents]
    if not rows:
        return 0
    if not _table_exists(conn):
        raise RuntimeError(f"{INCIDENT_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO quant.platform_incidents (
                    incident_key, source, severity, component, title, description,
                    market_slug, token_id, token_side, start_ts, end_ts, start_block, end_block, payload
                )
                VALUES (
                    %(incident_key)s, %(source)s, %(severity)s, %(component)s, %(title)s, %(description)s,
                    %(market_slug)s, %(token_id)s, %(token_side)s, %(start_ts)s, %(end_ts)s,
                    %(start_block)s, %(end_block)s, %(payload)s::jsonb
                )
                ON CONFLICT (source, incident_key)
                DO UPDATE SET
                    severity = EXCLUDED.severity,
                    component = EXCLUDED.component,
                    title = EXCLUDED.title,
                    description = COALESCE(EXCLUDED.description, quant.platform_incidents.description),
                    market_slug = COALESCE(EXCLUDED.market_slug, quant.platform_incidents.market_slug),
                    token_id = COALESCE(EXCLUDED.token_id, quant.platform_incidents.token_id),
                    token_side = COALESCE(EXCLUDED.token_side, quant.platform_incidents.token_side),
                    start_ts = COALESCE(EXCLUDED.start_ts, quant.platform_incidents.start_ts),
                    end_ts = COALESCE(EXCLUDED.end_ts, quant.platform_incidents.end_ts),
                    start_block = COALESCE(EXCLUDED.start_block, quant.platform_incidents.start_block),
                    end_block = COALESCE(EXCLUDED.end_block, quant.platform_incidents.end_block),
                    payload = EXCLUDED.payload
                """,
                _db_row(row),
            )
    return len(rows)


def load_platform_incidents_for_run(conn: Any, run: Mapping[str, Any], points: Sequence[Any] | None = None) -> list[dict[str, Any]]:
    if not _table_exists(conn):
        return []
    from_block, to_block = _run_block_window(run, points)
    from_ts, to_ts = _run_time_window(run, points)
    market_slug = str(run.get("market_slug") or "").strip()
    token_side = str(run.get("token_side") or "").strip()
    meta = run.get("meta") if isinstance(run.get("meta"), Mapping) else {}
    token_id = str(meta.get("token_id") or "").strip()

    filters = [
        "(market_slug IS NULL OR market_slug = %s)",
        "(token_side IS NULL OR token_side = %s)",
        "(token_id IS NULL OR token_id = %s)",
    ]
    params: list[Any] = [market_slug, token_side, token_id]
    overlap_parts: list[str] = []
    if from_block is not None and to_block is not None:
        overlap_parts.append("((start_block IS NULL OR start_block <= %s) AND (end_block IS NULL OR end_block >= %s) AND (start_block IS NOT NULL OR end_block IS NOT NULL))")
        params.extend([to_block, from_block])
    if from_ts is not None and to_ts is not None:
        overlap_parts.append("((start_ts IS NULL OR start_ts <= %s) AND (end_ts IS NULL OR end_ts >= %s) AND (start_ts IS NOT NULL OR end_ts IS NOT NULL))")
        params.extend([to_ts, from_ts])
    if not overlap_parts:
        return []
    filters.append("(" + " OR ".join(overlap_parts) + ")")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.platform_incidents
            WHERE {" AND ".join(filters)}
            ORDER BY COALESCE(start_ts, to_timestamp(start_block)), incident_id
            LIMIT 500
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def _run_block_window(run: Mapping[str, Any], points: Sequence[Any] | None) -> tuple[int | None, int | None]:
    from_block = _int_or_none(run.get("from_block"))
    to_block = _int_or_none(run.get("to_block"))
    if (from_block is None or to_block is None) and points:
        values = [_int_or_none(getattr(point, "x_value", None)) for point in points]
        values = [value for value in values if value is not None]
        if values:
            from_block = min(values) if from_block is None else from_block
            to_block = max(values) if to_block is None else to_block
    return from_block, to_block


def _run_time_window(run: Mapping[str, Any], points: Sequence[Any] | None) -> tuple[datetime | None, datetime | None]:
    from_ts = _timestamp_or_none(run.get("from_ts"))
    to_ts = _timestamp_or_none(run.get("to_ts"))
    if (from_ts is None or to_ts is None) and points:
        values = [_parse_datetime(getattr(point, "timestamp", None)) for point in points]
        values = [value for value in values if value is not None]
        if values:
            from_ts = min(values) if from_ts is None else from_ts
            to_ts = max(values) if to_ts is None else to_ts
    return from_ts, to_ts


def _db_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result = {field: row.get(field) for field in (
        "incident_key",
        "source",
        "severity",
        "component",
        "title",
        "description",
        "market_slug",
        "token_id",
        "token_side",
        "start_ts",
        "end_ts",
        "start_block",
        "end_block",
    )}
    result["payload"] = json.dumps(row.get("payload") or {}, default=str)
    return result


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quant.platform_incidents') IS NOT NULL AS exists")
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


def _first_value(row: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _default_incident_key(row: Mapping[str, Any]) -> str:
    parts = [row.get("component"), row.get("severity"), row.get("start_ts"), row.get("start_block"), row.get("title")]
    return "|".join(str(part or "") for part in parts).strip("|") or "manual-platform-incident"


def _flag_key(value: Any) -> str:
    text = str(value or "unknown").strip().lower()
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in text)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "unknown"


def _timestamp_or_none(value: Any) -> datetime | None:
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().isdigit()):
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    return _parse_datetime(value)


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


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
