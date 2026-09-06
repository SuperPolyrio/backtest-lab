"""Normalize Polymarket CLOB messages into order book state-machine inputs."""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from math import isfinite
from typing import Any, Literal

from .local_book import BookSide


@dataclass(frozen=True)
class NormalizedBookSnapshot:
    token_id: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    event_ts_ms: int
    source_hash: str | None = None
    raw: dict[str, Any] | None = None
    received_ts_ms: int | None = None
    source: str | None = None
    connection_id: str | None = None
    connection_generation: int | None = None
    raw_frame_seq: int | None = None
    message_index: int | None = None
    group_id: str | None = None
    sequence_in_message: int = 0
    event_clock: Literal["exchange", "receive"] = "exchange"


@dataclass(frozen=True)
class NormalizedBookDelta:
    token_id: str
    side: BookSide
    price: Decimal
    size: Decimal
    event_ts_ms: int
    source_hash: str | None = None
    raw: dict[str, Any] | None = None
    received_ts_ms: int | None = None
    source: str | None = None
    connection_id: str | None = None
    connection_generation: int | None = None
    raw_frame_seq: int | None = None
    message_index: int | None = None
    group_id: str | None = None
    sequence_in_message: int = 0
    event_clock: Literal["exchange", "receive"] = "exchange"
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None


@dataclass(frozen=True)
class NormalizedTradeEvent:
    token_id: str
    price: Decimal
    size: Decimal
    aggressor_side: str
    event_ts_ms: int
    transaction_hash: str | None = None
    raw: dict[str, Any] | None = None


NormalizedBookEvent = NormalizedBookSnapshot | NormalizedBookDelta


def normalize_polymarket_event(
    event: dict[str, Any],
    *,
    received_ts_ms: int | None = None,
    strict: bool = False,
) -> list[NormalizedBookEvent]:
    """Normalize one source message while retaining its replay provenance.

    ``strict=True`` is intended for the live state machine.  It rejects the
    complete source frame before any state mutation when one constituent level
    or delta is malformed.  The default remains permissive for older offline
    readers which intentionally count malformed rows instead of raising.
    """

    event_type = str(event.get("event_type") or "").strip()
    provenance = _provenance(event, received_ts_ms=received_ts_ms, strict=strict)
    if event_type == "book":
        token_id = _token_id(event.get("asset_id"))
        if not token_id:
            if strict:
                raise ValueError("book snapshot is missing asset_id")
            return []
        return [
            NormalizedBookSnapshot(
                token_id=token_id,
                bids=_levels(event.get("bids"), strict=strict),
                asks=_levels(event.get("asks"), strict=strict),
                event_ts_ms=_timestamp_ms(event.get("timestamp"), strict=strict),
                source_hash=str(event.get("hash") or "") or None,
                raw=event,
                **provenance,
            )
        ]
    if event_type == "price_change":
        event_ts_ms = _timestamp_ms(event.get("timestamp"), strict=strict)
        normalized: list[NormalizedBookEvent] = []
        raw_changes = event.get("price_changes")
        if not isinstance(raw_changes, list):
            if strict:
                raise ValueError("price_change frame must contain a price_changes list")
            return []
        for sequence_in_message, item in enumerate(raw_changes):
            if not isinstance(item, dict):
                if strict:
                    raise ValueError(
                        f"price_change item {sequence_in_message} is not an object"
                    )
                continue
            token_id = _token_id(item.get("asset_id"))
            side = _side_from_polymarket(item.get("side"))
            price = _optional_decimal(item.get("price"))
            size = _optional_decimal(item.get("size"))
            if not token_id or side is None or price is None or size is None:
                if strict:
                    raise ValueError(
                        f"price_change item {sequence_in_message} is malformed"
                    )
                continue
            if strict and not (Decimal(0) < price < Decimal(1)):
                raise ValueError(
                    f"price_change item {sequence_in_message} has invalid Polymarket price {price}"
                )
            if strict and size < 0:
                raise ValueError(
                    f"price_change item {sequence_in_message} has negative size {size}"
                )
            best_bid = _optional_decimal(item.get("best_bid"))
            best_ask = _optional_decimal(item.get("best_ask"))
            if strict and (item.get("best_bid") is None) != (item.get("best_ask") is None):
                raise ValueError(
                    f"price_change item {sequence_in_message} has incomplete top hint"
                )
            if strict and item.get("best_bid") is not None and (best_bid is None or best_ask is None):
                raise ValueError(
                    f"price_change item {sequence_in_message} has malformed top hint"
                )
            normalized.append(
                NormalizedBookDelta(
                    token_id=token_id,
                    side=side,
                    price=price,
                    size=size,
                    event_ts_ms=event_ts_ms,
                    source_hash=str(item.get("hash") or event.get("hash") or "") or None,
                    raw={"event_type": "price_change", "market": event.get("market"), "timestamp": event.get("timestamp"), "price_change": item},
                    sequence_in_message=sequence_in_message,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    **provenance,
                )
            )
        return normalized
    return []


def normalize_rest_book(
    token_id: str,
    payload: dict[str, Any],
    *,
    event_ts_ms: int | None = None,
    received_ts_ms: int | None = None,
) -> NormalizedBookSnapshot:
    received = int(received_ts_ms if received_ts_ms is not None else time.time() * 1000)
    state_ts = int(event_ts_ms if event_ts_ms is not None else received)
    return NormalizedBookSnapshot(
        token_id=_token_id(token_id),
        bids=_levels(payload.get("bids"), strict=True),
        asks=_levels(payload.get("asks") or payload.get("offers"), strict=True),
        event_ts_ms=state_ts,
        source_hash=str(payload.get("hash") or "") or None,
        raw=payload,
        received_ts_ms=received,
        source=str(payload.get("source") or "polymarket_clob_rest"),
        event_clock="receive",
    )


def normalize_polymarket_trade(event: dict[str, Any]) -> NormalizedTradeEvent | None:
    if str(event.get("event_type") or "").strip() != "last_trade_price":
        return None
    token_id = _token_id(event.get("asset_id"))
    price = _optional_decimal(event.get("price"))
    size = _optional_decimal(event.get("size"))
    side = str(event.get("side") or "").strip().upper()
    if (
        not token_id
        or price is None
        or size is None
        or price <= 0
        or price >= 1
        or size <= 0
        or side not in {"BUY", "SELL"}
    ):
        return None
    return NormalizedTradeEvent(
        token_id=token_id,
        price=price,
        size=size,
        aggressor_side=side,
        event_ts_ms=_timestamp_ms(event.get("timestamp")),
        transaction_hash=str(event.get("transaction_hash") or "") or None,
        raw=event,
    )


def _levels(rows: Any, *, strict: bool = False) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(rows, list):
        if strict and rows is not None:
            raise ValueError("order book levels must be a list")
        return ()
    parsed: list[tuple[Decimal, Decimal]] = []
    for row in rows:
        if isinstance(row, dict):
            price = _optional_decimal(row.get("price"))
            size = _optional_decimal(row.get("size"))
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            price = _optional_decimal(row[0])
            size = _optional_decimal(row[1])
        else:
            if strict:
                raise ValueError("order book level is not a price/size pair")
            continue
        if price is None or size is None or price <= 0 or size <= 0:
            if strict:
                raise ValueError(f"invalid order book level: {row!r}")
            continue
        if strict and price >= 1:
            raise ValueError(f"invalid Polymarket order book price: {price}")
        parsed.append((price, size))
    return tuple(parsed)


def _side_from_polymarket(value: Any) -> BookSide | None:
    text = str(value or "").strip().upper()
    if text in {"BUY", "BID", "BIDS"}:
        return "bid"
    if text in {"SELL", "ASK", "ASKS"}:
        return "ask"
    return None


def _token_id(value: Any) -> str:
    return str(value or "").strip()


def _timestamp_ms(value: Any, *, strict: bool = False) -> int:
    if value is None or str(value).strip() == "":
        if strict:
            raise ValueError("order book event timestamp is missing")
        return int(time.time() * 1000)
    try:
        parsed = float(str(value))
    except ValueError:
        if strict:
            raise ValueError(f"invalid order book event timestamp: {value!r}") from None
        return int(time.time() * 1000)
    if not isfinite(parsed):
        if strict:
            raise ValueError(f"non-finite order book event timestamp: {value!r}")
        return int(time.time() * 1000)
    if parsed > 10**17:
        return int(parsed // 1_000_000)
    if parsed > 10**14:
        return int(parsed // 1_000)
    if parsed < 10**11:
        return int(parsed * 1000)
    return int(parsed)


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def _provenance(
    event: dict[str, Any],
    *,
    received_ts_ms: int | None,
    strict: bool,
) -> dict[str, Any]:
    raw_wall_ns = _optional_int(event.get("_raw_received_wall_ns"), strict=strict)
    received = (
        int(received_ts_ms)
        if received_ts_ms is not None
        else int(raw_wall_ns // 1_000_000)
        if raw_wall_ns is not None
        else None
    )
    return {
        "received_ts_ms": received,
        "source": str(event.get("source") or "polymarket_market_ws"),
        "connection_id": str(event.get("_raw_connection_id") or "") or None,
        "connection_generation": _optional_int(
            event.get("_raw_connection_generation"), strict=strict
        ),
        "raw_frame_seq": _optional_int(event.get("_raw_frame_seq"), strict=strict),
        "message_index": _optional_int(event.get("_raw_message_index"), strict=strict),
        "group_id": str(event.get("_raw_group_id") or "") or None,
        "event_clock": "exchange",
    }


def _optional_int(value: Any, *, strict: bool) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        if strict:
            raise ValueError(f"invalid integer provenance value: {value!r}") from None
        return None
