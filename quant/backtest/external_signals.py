"""Canonical external signal events for fill-first backtest inputs."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, Mapping


EXTERNAL_SIGNAL_TABLE = "quant.external_signal_events"

ALIASES: dict[str, tuple[str, ...]] = {
    "signal_id": ("signal_id", "signalId", "event_id", "eventId", "id"),
    "source": ("source", "provider", "origin"),
    "event_type": ("event_type", "eventType", "type"),
    "market_slug": ("market_slug", "marketSlug"),
    "token_id": ("token_id", "tokenId", "asset_id", "assetId"),
    "token_side": ("token_side", "tokenSide"),
    "observed_at": ("observed_at", "observedAt", "timestamp", "time"),
    "observed_block": ("observed_block", "observedBlock", "block_number", "blockNumber"),
    "latency_seconds": ("latency_seconds", "latencySeconds", "latency"),
    "payload_hash": ("payload_hash", "payloadHash"),
    "resolution_source": ("resolution_source", "resolutionSource"),
    "settlement_rule": ("settlement_rule", "settlementRule", "resolution_rule", "resolutionRule"),
    "price_to_beat_source": ("price_to_beat_source", "priceToBeatSource"),
    "oracle_source": ("oracle_source", "oracleSource", "oracle"),
    "payload": ("payload", "raw", "raw_payload", "rawPayload"),
}


def normalize_external_signal_event(
    row: Mapping[str, Any],
    *,
    source: str | None = None,
    run_id: int | None = None,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field, aliases in ALIASES.items():
        value = _first_value(row, aliases)
        if value not in (None, ""):
            normalized[field] = value
    if source:
        normalized["source"] = source
    if run_id is not None:
        normalized["run_id"] = int(run_id)
    elif row.get("run_id") not in (None, ""):
        normalized["run_id"] = _int_or_none(row.get("run_id"))
    normalized.setdefault("source", "manual")
    normalized.setdefault("event_type", "external_signal")
    payload = _payload_object(normalized.get("payload"), row)
    payload_hash = str(normalized.get("payload_hash") or "").strip()
    if not payload_hash:
        payload_hash = _payload_hash(payload)
    normalized["payload_hash"] = payload_hash
    normalized["payload"] = payload
    normalized.setdefault("signal_id", _default_signal_id(normalized))
    return normalized


def build_external_signal_import_report(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [normalize_external_signal_event(row) for row in events]
    source_counts: dict[str, int] = {}
    missing_counts: dict[str, int] = {}
    required = ("observed_at", "source", "latency_seconds", "payload_hash")
    for row in rows:
        source = str(row.get("source") or "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1
        for field in required:
            if row.get(field) in (None, ""):
                missing_counts[field] = missing_counts.get(field, 0) + 1
    return {
        "schema_version": "fill_first_external_signal_import_v1",
        "status": "ready" if not missing_counts else "review",
        "event_count": len(rows),
        "source_counts": dict(sorted(source_counts.items())),
        "missing_required_field_count": sum(missing_counts.values()),
        "missing_field_counts": dict(sorted(missing_counts.items())),
        "preview": rows[:20],
    }


def upsert_external_signal_events(conn: Any, events: Iterable[Mapping[str, Any]]) -> int:
    rows = [normalize_external_signal_event(event) for event in events]
    if not rows:
        return 0
    if not _table_exists(conn):
        raise RuntimeError(f"{EXTERNAL_SIGNAL_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO quant.external_signal_events (
                    run_id, signal_id, source, event_type, market_slug, token_id, token_side,
                    observed_at, observed_block, latency_seconds, payload_hash,
                    resolution_source, settlement_rule, price_to_beat_source, oracle_source, payload
                )
                VALUES (
                    %(run_id)s, %(signal_id)s, %(source)s, %(event_type)s, %(market_slug)s, %(token_id)s,
                    %(token_side)s, %(observed_at)s, %(observed_block)s, %(latency_seconds)s, %(payload_hash)s,
                    %(resolution_source)s, %(settlement_rule)s, %(price_to_beat_source)s, %(oracle_source)s,
                    %(payload)s::jsonb
                )
                ON CONFLICT (source, signal_id)
                DO UPDATE SET
                    run_id = COALESCE(EXCLUDED.run_id, quant.external_signal_events.run_id),
                    event_type = EXCLUDED.event_type,
                    market_slug = COALESCE(EXCLUDED.market_slug, quant.external_signal_events.market_slug),
                    token_id = COALESCE(EXCLUDED.token_id, quant.external_signal_events.token_id),
                    token_side = COALESCE(EXCLUDED.token_side, quant.external_signal_events.token_side),
                    observed_at = COALESCE(EXCLUDED.observed_at, quant.external_signal_events.observed_at),
                    observed_block = COALESCE(EXCLUDED.observed_block, quant.external_signal_events.observed_block),
                    latency_seconds = COALESCE(EXCLUDED.latency_seconds, quant.external_signal_events.latency_seconds),
                    payload_hash = EXCLUDED.payload_hash,
                    resolution_source = COALESCE(EXCLUDED.resolution_source, quant.external_signal_events.resolution_source),
                    settlement_rule = COALESCE(EXCLUDED.settlement_rule, quant.external_signal_events.settlement_rule),
                    price_to_beat_source = COALESCE(EXCLUDED.price_to_beat_source, quant.external_signal_events.price_to_beat_source),
                    oracle_source = COALESCE(EXCLUDED.oracle_source, quant.external_signal_events.oracle_source),
                    payload = EXCLUDED.payload
                """,
                _db_row(row),
            )
    return len(rows)


def load_external_signal_events_for_run(conn: Any, run_id: int) -> list[dict[str, Any]]:
    if not _table_exists(conn):
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.external_signal_events
            WHERE run_id = %s
            ORDER BY observed_at NULLS LAST, observed_block NULLS LAST, signal_event_id
            LIMIT 5000
            """,
            (int(run_id),),
        )
        return [dict(row) for row in cur.fetchall()]


def _db_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        field: row.get(field)
        for field in (
            "run_id",
            "signal_id",
            "source",
            "event_type",
            "market_slug",
            "token_id",
            "token_side",
            "observed_at",
            "observed_block",
            "latency_seconds",
            "payload_hash",
            "resolution_source",
            "settlement_rule",
            "price_to_beat_source",
            "oracle_source",
        )
    }
    result["payload"] = json.dumps(row.get("payload") or {}, default=str)
    return result


def _payload_object(value: Any, row: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except ValueError:
            parsed = {"raw": value}
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    if isinstance(value, Mapping):
        return dict(value)
    return dict(row)


def _payload_hash(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _default_signal_id(row: Mapping[str, Any]) -> str:
    parts = [
        row.get("source"),
        row.get("event_type"),
        row.get("market_slug"),
        row.get("token_id"),
        row.get("token_side"),
        row.get("observed_at"),
        row.get("observed_block"),
        row.get("payload_hash"),
    ]
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quant.external_signal_events') IS NOT NULL AS exists")
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


def _first_value(row: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)
