"""Real order state event persistence and attachment helpers.

These helpers keep live/order-stream evidence separate from the simulator while
letting the builtin fill report audit submit, accept, cancel, and status races.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
from typing import Any, Iterable, Mapping


STATE_TABLE = "quant.real_order_state_events"

STATE_FIELDS = (
    "submit_status",
    "accepted_status",
    "cancel_status",
    "api_order_status",
    "chain_order_status",
    "clob_order_status",
    "submit_at",
    "accepted_at",
    "cancel_submitted_at",
    "cancel_accepted_at",
)

COMPACT_EVENT_FIELDS = (
    "event_time",
    "event_type",
    "source",
    "submit_status",
    "accepted_status",
    "cancel_status",
    "api_order_status",
    "chain_order_status",
    "clob_order_status",
    "submit_at",
    "accepted_at",
    "cancel_submitted_at",
    "cancel_accepted_at",
    "payload",
)

ALIASES: dict[str, tuple[str, ...]] = {
    "run_id": ("run_id", "runId"),
    "order_id": ("order_id", "orderId", "client_order_id", "clientOrderId"),
    "external_order_id": (
        "external_order_id",
        "externalOrderId",
        "clob_order_id",
        "clobOrderId",
        "order_hash",
        "orderHash",
        "id",
    ),
    "market_slug": ("market_slug", "marketSlug"),
    "token_id": ("token_id", "tokenId", "asset_id", "assetId"),
    "token_side": ("token_side", "tokenSide", "side_token", "sideToken"),
    "event_time": ("event_time", "eventTime", "timestamp", "time", "created_at", "createdAt"),
    "event_type": ("event_type", "eventType", "type"),
    "source": ("source",),
    "submit_status": ("submit_status", "submitStatus", "submission_status", "submissionStatus"),
    "accepted_status": ("accepted_status", "acceptedStatus", "order_accepted_status", "orderAcceptedStatus"),
    "cancel_status": ("cancel_status", "cancelStatus", "order_cancel_status", "orderCancelStatus", "cancel_result", "cancelResult"),
    "api_order_status": ("api_order_status", "apiOrderStatus", "api_status", "apiStatus", "get_order_status", "getOrderStatus"),
    "chain_order_status": ("chain_order_status", "chainOrderStatus", "chain_status", "chainStatus", "onchain_status", "onchainStatus"),
    "clob_order_status": ("clob_order_status", "clobOrderStatus", "clob_status", "clobStatus", "matching_status", "matchingStatus"),
    "submit_at": ("submit_at", "submitAt", "submitted_at", "submittedAt", "submit_timestamp", "submitTimestamp"),
    "accepted_at": ("accepted_at", "acceptedAt", "order_accepted_at", "orderAcceptedAt", "accepted_timestamp", "acceptedTimestamp"),
    "cancel_submitted_at": ("cancel_submitted_at", "cancelSubmittedAt", "cancel_submit_at", "cancelSubmitAt", "cancel_requested_at", "cancelRequestedAt"),
    "cancel_accepted_at": ("cancel_accepted_at", "cancelAcceptedAt", "cancelled_at", "cancelledAt", "canceled_at", "canceledAt"),
    "payload": ("payload", "raw", "raw_payload", "rawPayload"),
}


def normalize_order_state_event(row: Mapping[str, Any], *, source: str | None = None, run_id: int | None = None) -> dict[str, Any]:
    """Normalize a flexible order state event into the DB column shape."""

    normalized: dict[str, Any] = {}
    for field, aliases in ALIASES.items():
        value = _first_value(row, aliases)
        if value not in (None, ""):
            normalized[field] = value
    if source:
        normalized["source"] = source
    normalized.setdefault("source", "manual")
    if run_id is not None:
        normalized["run_id"] = run_id
    normalized.setdefault("event_type", "order_state")
    payload = normalized.get("payload")
    normalized["payload"] = payload if isinstance(payload, dict) else dict(row)
    return normalized


def upsert_real_order_state_events(conn: Any, events: Iterable[Mapping[str, Any]]) -> int:
    rows = [normalize_order_state_event(event) for event in events]
    if not rows:
        return 0
    if not _table_exists(conn):
        raise RuntimeError(f"{STATE_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO quant.real_order_state_events (
                    run_id, order_id, external_order_id, market_slug, token_id, token_side,
                    event_time, event_type, source,
                    submit_status, accepted_status, cancel_status,
                    api_order_status, chain_order_status, clob_order_status,
                    submit_at, accepted_at, cancel_submitted_at, cancel_accepted_at, payload
                )
                VALUES (
                    %(run_id)s, %(order_id)s, %(external_order_id)s, %(market_slug)s, %(token_id)s, %(token_side)s,
                    %(event_time)s, %(event_type)s, %(source)s,
                    %(submit_status)s, %(accepted_status)s, %(cancel_status)s,
                    %(api_order_status)s, %(chain_order_status)s, %(clob_order_status)s,
                    %(submit_at)s, %(accepted_at)s, %(cancel_submitted_at)s, %(cancel_accepted_at)s, %(payload)s::jsonb
                )
                ON CONFLICT (source, external_order_id, event_type, event_time)
                DO UPDATE SET
                    run_id = COALESCE(EXCLUDED.run_id, quant.real_order_state_events.run_id),
                    order_id = COALESCE(EXCLUDED.order_id, quant.real_order_state_events.order_id),
                    market_slug = COALESCE(EXCLUDED.market_slug, quant.real_order_state_events.market_slug),
                    token_id = COALESCE(EXCLUDED.token_id, quant.real_order_state_events.token_id),
                    token_side = COALESCE(EXCLUDED.token_side, quant.real_order_state_events.token_side),
                    submit_status = COALESCE(EXCLUDED.submit_status, quant.real_order_state_events.submit_status),
                    accepted_status = COALESCE(EXCLUDED.accepted_status, quant.real_order_state_events.accepted_status),
                    cancel_status = COALESCE(EXCLUDED.cancel_status, quant.real_order_state_events.cancel_status),
                    api_order_status = COALESCE(EXCLUDED.api_order_status, quant.real_order_state_events.api_order_status),
                    chain_order_status = COALESCE(EXCLUDED.chain_order_status, quant.real_order_state_events.chain_order_status),
                    clob_order_status = COALESCE(EXCLUDED.clob_order_status, quant.real_order_state_events.clob_order_status),
                    submit_at = COALESCE(EXCLUDED.submit_at, quant.real_order_state_events.submit_at),
                    accepted_at = COALESCE(EXCLUDED.accepted_at, quant.real_order_state_events.accepted_at),
                    cancel_submitted_at = COALESCE(EXCLUDED.cancel_submitted_at, quant.real_order_state_events.cancel_submitted_at),
                    cancel_accepted_at = COALESCE(EXCLUDED.cancel_accepted_at, quant.real_order_state_events.cancel_accepted_at),
                    payload = EXCLUDED.payload
                """,
                _db_row(row),
            )
    return len(rows)


def load_real_order_state_events_for_run(conn: Any, run_id: int) -> list[dict[str, Any]]:
    if not _table_exists(conn):
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                run_id, order_id, external_order_id, market_slug, token_id, token_side,
                event_time, event_type, source,
                submit_status, accepted_status, cancel_status,
                api_order_status, chain_order_status, clob_order_status,
                submit_at, accepted_at, cancel_submitted_at, cancel_accepted_at,
                payload, created_at
            FROM quant.real_order_state_events
            WHERE run_id = %s
            ORDER BY COALESCE(event_time, created_at), event_id
            """,
            (run_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def attach_real_order_state_events(orders: list[dict[str, Any]], events: Iterable[Mapping[str, Any]]) -> int:
    """Attach matching real state events to order meta and return updated order count."""

    grouped = _group_events(events)
    if not grouped:
        return 0
    attached = 0
    for order in orders:
        keys = _order_match_keys(order)
        matches: list[dict[str, Any]] = []
        seen: set[str] = set()
        for key in keys:
            for event in grouped.get(key, []):
                event_key = _event_identity(event)
                if event_key in seen:
                    continue
                seen.add(event_key)
                matches.append(event)
        if not matches:
            continue
        matches.sort(key=_event_sort_key)
        meta = order.get("meta") if isinstance(order.get("meta"), dict) else {}
        order["meta"] = meta
        compact = [_compact_event(event) for event in matches]
        meta["real_order_state_events"] = compact
        for event in matches:
            if event.get("external_order_id"):
                meta["external_order_id"] = _meta_value(event.get("external_order_id"))
            for field in STATE_FIELDS:
                value = event.get(field)
                if value not in (None, ""):
                    meta[field] = _meta_value(value)
        attached += 1
    return attached


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quant.real_order_state_events') IS NOT NULL AS exists")
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


def _first_value(row: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _db_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "run_id": row.get("run_id"),
        "order_id": _text_or_none(row.get("order_id")),
        "external_order_id": _text_or_none(row.get("external_order_id")),
        "market_slug": _text_or_none(row.get("market_slug")),
        "token_id": _text_or_none(row.get("token_id")),
        "token_side": _text_or_none(row.get("token_side")),
        "event_time": row.get("event_time"),
        "event_type": _text_or_none(row.get("event_type")) or "order_state",
        "source": _text_or_none(row.get("source")) or "manual",
        "submit_status": _text_or_none(row.get("submit_status")),
        "accepted_status": _text_or_none(row.get("accepted_status")),
        "cancel_status": _text_or_none(row.get("cancel_status")),
        "api_order_status": _text_or_none(row.get("api_order_status")),
        "chain_order_status": _text_or_none(row.get("chain_order_status")),
        "clob_order_status": _text_or_none(row.get("clob_order_status")),
        "submit_at": row.get("submit_at"),
        "accepted_at": row.get("accepted_at"),
        "cancel_submitted_at": row.get("cancel_submitted_at"),
        "cancel_accepted_at": row.get("cancel_accepted_at"),
        "payload": json.dumps(row.get("payload") or {}, default=str),
    }


def _group_events(events: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw_event in events:
        event = normalize_order_state_event(raw_event)
        for key in _event_match_keys(event):
            grouped.setdefault(key, []).append(event)
    return grouped


def _order_match_keys(order: Mapping[str, Any]) -> set[str]:
    keys: set[str] = set()
    for value in (
        order.get("order_id"),
        order.get("id"),
        order.get("external_order_id"),
    ):
        _add_key(keys, value)
    meta = order.get("meta") if isinstance(order.get("meta"), Mapping) else {}
    for value in (
        meta.get("order_id"),
        meta.get("external_order_id"),
        meta.get("clob_order_id"),
        meta.get("order_hash"),
    ):
        _add_key(keys, value)
    return keys


def _event_match_keys(event: Mapping[str, Any]) -> set[str]:
    keys: set[str] = set()
    for value in (event.get("order_id"), event.get("external_order_id")):
        _add_key(keys, value)
    return keys


def _add_key(keys: set[str], value: Any) -> None:
    text = _text_or_none(value)
    if text:
        keys.add(text)


def _text_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _event_sort_key(event: Mapping[str, Any]) -> tuple[str, str]:
    return (_sort_text(event.get("event_time")), _sort_text(event.get("created_at")))


def _sort_text(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value or "")


def _event_identity(event: Mapping[str, Any]) -> str:
    parts = (
        event.get("source"),
        event.get("external_order_id"),
        event.get("order_id"),
        event.get("event_type"),
        event.get("event_time"),
    )
    return "|".join(str(part or "") for part in parts)


def _compact_event(event: Mapping[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for field in COMPACT_EVENT_FIELDS:
        value = event.get(field)
        if value not in (None, ""):
            compact[field] = _meta_value(value)
    return compact


def _meta_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
        return value
    return str(value)
