"""Normalize external order API payloads into real order state events."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from quant.backtest.order_state import normalize_order_state_event


ORDER_LIST_KEYS = ("orders", "items", "data", "results", "events")


def events_from_order_payload(
    payload: Any,
    *,
    source: str = "order-api",
    run_id: int | None = None,
    observed_at: str | None = None,
) -> list[dict[str, Any]]:
    return [
        normalize_order_state_event(event, source=source, run_id=run_id)
        for event in (_event_from_order(item, source=source, run_id=run_id, observed_at=observed_at) for item in iter_order_payloads(payload))
    ]


def iter_order_payloads(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, Mapping)]
    if not isinstance(payload, Mapping):
        return []
    for key in ORDER_LIST_KEYS:
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(item) for item in value if isinstance(item, Mapping)]
    return [dict(payload)]


def _event_from_order(order: Mapping[str, Any], *, source: str, run_id: int | None, observed_at: str | None) -> dict[str, Any]:
    status = _status(order)
    event = {
        "run_id": run_id or _first(order, "run_id", "runId"),
        "order_id": _first(order, "order_id", "orderId", "client_order_id", "clientOrderId", "client_id", "clientId"),
        "external_order_id": _first(order, "external_order_id", "externalOrderId", "clob_order_id", "clobOrderId", "order_hash", "orderHash", "hash", "id"),
        "market_slug": _first(order, "market_slug", "marketSlug", "slug"),
        "token_id": _first(order, "token_id", "tokenId", "asset_id", "assetId", "token"),
        "token_side": _first(order, "token_side", "tokenSide", "outcome_side", "outcomeSide"),
        "event_time": _first(order, "event_time", "eventTime", "updated_at", "updatedAt", "created_at", "createdAt", "timestamp", "time") or observed_at or _now_iso(),
        "event_type": _event_type(status, order),
        "source": source,
        "submit_status": _submit_status(status),
        "accepted_status": _accepted_status(status),
        "cancel_status": _cancel_status(status),
        "api_order_status": status or None,
        "chain_order_status": _first(order, "chain_order_status", "chainOrderStatus", "onchain_status", "onchainStatus"),
        "clob_order_status": _first(order, "clob_order_status", "clobOrderStatus", "matching_status", "matchingStatus", "status", "state", "orderStatus"),
        "submit_at": _first(order, "submit_at", "submitAt", "submitted_at", "submittedAt", "created_at", "createdAt"),
        "accepted_at": _first(order, "accepted_at", "acceptedAt", "updated_at", "updatedAt"),
        "cancel_submitted_at": _first(order, "cancel_submitted_at", "cancelSubmittedAt", "cancel_requested_at", "cancelRequestedAt"),
        "cancel_accepted_at": _first(order, "cancel_accepted_at", "cancelAcceptedAt", "cancelled_at", "cancelledAt", "canceled_at", "canceledAt"),
        "payload": dict(order),
    }
    return {key: value for key, value in event.items() if value not in (None, "")}


def _event_type(status: str, order: Mapping[str, Any]) -> str:
    explicit = _first(order, "event_type", "eventType", "type")
    if explicit not in (None, ""):
        return str(explicit)
    if status in {"FILLED", "PARTIAL_FILLED"}:
        return "fill"
    if status in {"CANCELED", "CANCELLED"}:
        return "cancel"
    if status in {"REJECTED", "FAILED"}:
        return "reject"
    if status == "EXPIRED":
        return "expire"
    if status in {"OPEN", "ACCEPTED", "SUBMITTED", "PENDING"}:
        return "submit"
    return "order_state"


def _status(order: Mapping[str, Any]) -> str:
    raw = _first(order, "status", "state", "order_status", "orderStatus", "api_order_status", "apiOrderStatus")
    text = str(raw or "").strip().upper().replace("-", "_").replace(" ", "_")
    if not text:
        return "UNKNOWN"
    if text in {"NO_FILL", "UNFILLED"}:
        return "NO_FILL"
    if "PARTIAL" in text:
        return "PARTIAL_FILLED"
    if "FILL" in text or "MATCH" in text or "EXECUT" in text:
        return "FILLED"
    if "CANCEL" in text:
        return "CANCELED"
    if "REJECT" in text:
        return "REJECTED"
    if "EXPIRE" in text:
        return "EXPIRED"
    if text in {"OPEN", "LIVE", "ACTIVE"}:
        return "OPEN"
    if text in {"ACCEPT", "ACCEPTED"}:
        return "ACCEPTED"
    if text in {"SUBMIT", "SUBMITTED"}:
        return "SUBMITTED"
    return text


def _submit_status(status: str) -> str | None:
    if status in {"OPEN", "ACCEPTED", "SUBMITTED", "PENDING", "FILLED", "PARTIAL_FILLED", "CANCELED"}:
        return "accepted"
    if status in {"REJECTED", "FAILED"}:
        return "rejected"
    return None


def _accepted_status(status: str) -> str | None:
    if status in {"OPEN", "ACCEPTED", "FILLED", "PARTIAL_FILLED", "CANCELED"}:
        return "accepted"
    return None


def _cancel_status(status: str) -> str | None:
    if status in {"CANCELED", "CANCELLED"}:
        return "accepted"
    return None


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
