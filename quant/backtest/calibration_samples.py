"""Build fill calibration samples from simulated orders and real order events."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping

from quant.backtest.calibration import normalize_calibration_order


TERMINAL_NO_FILL_STATUSES = {"CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "NO_FILL", "FAILED"}


def build_calibration_samples_from_order_events(
    orders: Iterable[Mapping[str, Any]],
    events: Iterable[Mapping[str, Any]],
    *,
    source: str = "auto-real-order-state",
    include_open_events: bool = False,
) -> list[dict[str, Any]]:
    grouped = _group_events(events)
    samples: list[dict[str, Any]] = []
    for order in orders:
        matches = _matching_events(order, grouped)
        for event in matches:
            if not include_open_events and not _is_calibration_event(event):
                continue
            samples.append(build_calibration_sample(order, event, source=source))
    return samples


def build_calibration_sample(order: Mapping[str, Any], event: Mapping[str, Any], *, source: str = "auto-real-order-state") -> dict[str, Any]:
    payload = _event_payload(event)
    meta = _payload(order.get("meta"))
    run_id = _first(order, "run_id", "runId")
    order_id = _text(_first(order, "order_id", "orderId"))
    external_order_id = _text(_first(event, "external_order_id", "externalOrderId") or meta.get("external_order_id") or meta.get("externalOrderId"))
    context = {
        "market_category": _context_value(order, event, payload, meta, "market_category", "marketCategory", "category", "event_category", "eventCategory"),
        "liquidity_bucket": _context_value(order, event, payload, meta, "liquidity_bucket", "liquidityBucket") or _derived_liquidity_bucket(order, payload, meta),
        "volatility_bucket": _context_value(order, event, payload, meta, "volatility_bucket", "volatilityBucket") or _derived_volatility_bucket(order, payload, meta),
        "time_to_expiry_bucket": _context_value(order, event, payload, meta, "time_to_expiry_bucket", "timeToExpiryBucket") or _derived_time_to_expiry_bucket(order, payload, meta),
    }
    sample = {
        "run_id": run_id,
        "sample_id": _sample_id(run_id, order_id, event, external_order_id),
        "source": source,
        "market_slug": _first(event, "market_slug", "marketSlug") or _first(order, "market_slug", "marketSlug") or _first(order, "run_market_slug", "runMarketSlug"),
        "token_id": _first(event, "token_id", "tokenId") or _first(order, "token_id", "tokenId") or meta.get("token_id") or meta.get("tokenId"),
        "token_side": _first(event, "token_side", "tokenSide") or _first(order, "token_side", "tokenSide") or _first(order, "run_token_side", "runTokenSide"),
        "side": _first(order, "side"),
        "role": _first(order, "role"),
        "order_type": _first(order, "order_type", "orderType"),
        "observed_at": _first(event, "event_time", "eventTime", "accepted_at", "acceptedAt", "created_at", "createdAt"),
        "observed_block": _first(payload, "observed_block", "observedBlock", "block_number", "blockNumber"),
        "simulated_order_id": order_id,
        "live_order_id": external_order_id,
        "requested_price": _first(order, "requested_price", "requestedPrice", "decision_price", "decisionPrice"),
        "requested_size": _first(order, "requested_size", "requestedSize"),
        "simulated_status": _first(order, "status"),
        "live_status": _live_status(event, payload),
        "simulated_fill_price": _first(order, "avg_fill_price", "avgFillPrice"),
        "live_fill_price": _first(payload, "live_fill_price", "liveFillPrice", "avg_fill_price", "avgFillPrice", "fill_price", "fillPrice", "price"),
        "simulated_fill_size": _first(order, "filled_size", "filledSize"),
        "live_fill_size": _first(payload, "live_fill_size", "liveFillSize", "filled_size", "filledSize", "matched_size", "matchedSize", "size"),
        "simulated_slippage": _first(order, "slippage_cost", "slippageCost"),
        "live_slippage": _first(payload, "live_slippage", "liveSlippage", "slippage", "slippage_cost", "slippageCost"),
        "simulated_fee": _first(order, "fee_cost", "feeCost"),
        "live_fee": _first(payload, "live_fee", "liveFee", "fee", "fee_cost", "feeCost"),
        "simulated_rebate": _first(order, "rebate_cost", "rebateCost"),
        "live_rebate": _first(payload, "live_rebate", "liveRebate", "rebate", "rebate_cost", "rebateCost"),
        "simulated_cash_delta": _simulated_cash_delta(order),
        "live_cash_delta": _first(payload, "live_cash_delta", "liveCashDelta", "cash_delta", "cashDelta"),
        "simulated_position_delta": _simulated_position_delta(order),
        "live_position_delta": _first(payload, "live_position_delta", "livePositionDelta", "position_delta", "positionDelta"),
        "simulated_latency_seconds": _first(order, "latency_seconds", "latencySeconds"),
        "live_latency_seconds": _live_latency_seconds(event, payload),
        **context,
        "payload": {"simulated_order": dict(order), "real_event": dict(event), "context": context},
    }
    return normalize_calibration_order(sample, source=source, run_id=int(run_id) if run_id not in (None, "") else None)


def _group_events(events: Iterable[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        normalized = dict(event)
        for key in _event_keys(normalized):
            grouped.setdefault(key, []).append(normalized)
    return grouped


def _matching_events(order: Mapping[str, Any], grouped: Mapping[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    seen: set[str] = set()
    for key in _order_keys(order):
        for event in grouped.get(key, []):
            identity = _sample_id(_first(order, "run_id", "runId"), _text(_first(order, "order_id", "orderId")), event, _text(_first(event, "external_order_id", "externalOrderId")))
            if identity in seen:
                continue
            seen.add(identity)
            matches.append(event)
    return sorted(matches, key=lambda event: str(_first(event, "event_time", "eventTime", "created_at", "createdAt") or ""))


def _order_keys(order: Mapping[str, Any]) -> set[str]:
    keys: set[str] = set()
    meta = _payload(order.get("meta"))
    for value in (
        _first(order, "order_id", "orderId"),
        _first(order, "external_order_id", "externalOrderId"),
        meta.get("external_order_id"),
        meta.get("externalOrderId"),
        meta.get("clob_order_id"),
        meta.get("clobOrderId"),
        meta.get("order_hash"),
        meta.get("orderHash"),
    ):
        if _text(value):
            keys.add(_text(value))
    return keys


def _event_keys(event: Mapping[str, Any]) -> set[str]:
    keys: set[str] = set()
    for value in (_first(event, "order_id", "orderId"), _first(event, "external_order_id", "externalOrderId")):
        if _text(value):
            keys.add(_text(value))
    return keys


def _is_calibration_event(event: Mapping[str, Any]) -> bool:
    payload = _event_payload(event)
    if _first(payload, "live_fill_price", "liveFillPrice", "filled_size", "filledSize", "fill_price", "fillPrice", "price") not in (None, ""):
        return True
    status = _live_status(event, payload)
    return status in TERMINAL_NO_FILL_STATUSES or status in {"FILLED", "PARTIAL_FILLED"}


def _live_status(event: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    explicit = _first(payload, "live_status", "liveStatus", "fill_status", "fillStatus", "order_status", "orderStatus", "status")
    if explicit not in (None, ""):
        return _canonical_status(explicit)
    for field in ("chain_order_status", "chainOrderStatus", "clob_order_status", "clobOrderStatus", "api_order_status", "apiOrderStatus", "cancel_status", "cancelStatus", "submit_status", "submitStatus"):
        status = _canonical_status(_first(event, field))
        if status in {"FILLED", "PARTIAL_FILLED"} or status in TERMINAL_NO_FILL_STATUSES:
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
    if text in {"OPEN", "ACCEPTED", "SUBMITTED", "PENDING"}:
        return "OPEN"
    return text


def _live_latency_seconds(event: Mapping[str, Any], payload: Mapping[str, Any]) -> Any:
    explicit = _first(payload, "live_latency_seconds", "liveLatencySeconds", "latency_seconds", "latencySeconds")
    if explicit not in (None, ""):
        return explicit
    submit_at = _parse_time(_first(event, "submit_at", "submitAt"))
    accepted_at = _parse_time(_first(event, "accepted_at", "acceptedAt"))
    if submit_at and accepted_at:
        return str(max(0, (accepted_at - submit_at).total_seconds()))
    return None


def _context_value(
    order: Mapping[str, Any],
    event: Mapping[str, Any],
    payload: Mapping[str, Any],
    meta: Mapping[str, Any],
    *keys: str,
) -> Any:
    for row in (payload, meta, event, order):
        value = _first(row, *keys)
        if value not in (None, ""):
            return value
    return None


def _derived_liquidity_bucket(order: Mapping[str, Any], payload: Mapping[str, Any], meta: Mapping[str, Any]) -> str | None:
    value = _first(
        meta,
        "available_notional",
        "availableNotional",
        "block_volume",
        "blockVolume",
        "volume",
        "liquidity",
    )
    if value in (None, ""):
        value = _first(
            order,
            "available_notional",
            "availableNotional",
            "block_volume",
            "blockVolume",
            "volume",
            "filled_notional",
            "filledNotional",
        )
    if value in (None, ""):
        value = _first(payload, "available_notional", "availableNotional", "block_volume", "blockVolume", "volume")
    if value in (None, ""):
        return None
    amount = _decimal(value)
    if amount >= Decimal("10000"):
        return "high"
    if amount >= Decimal("1000"):
        return "medium"
    if amount > Decimal("0"):
        return "low"
    return "empty"


def _derived_volatility_bucket(order: Mapping[str, Any], payload: Mapping[str, Any], meta: Mapping[str, Any]) -> str | None:
    value = _first(
        meta,
        "volatility",
        "volatility_abs",
        "volatilityAbs",
        "price_move",
        "priceMove",
        "snapshot_drift",
        "snapshotDrift",
    )
    if value in (None, ""):
        value = _first(order, "volatility", "price_move", "priceMove", "snapshot_drift", "snapshotDrift")
    if value in (None, ""):
        value = _first(payload, "volatility", "price_move", "priceMove", "snapshot_drift", "snapshotDrift")
    if value in (None, ""):
        return None
    amount = abs(_decimal(value))
    if amount >= Decimal("0.03"):
        return "high"
    if amount >= Decimal("0.01"):
        return "medium"
    return "low"


def _derived_time_to_expiry_bucket(order: Mapping[str, Any], payload: Mapping[str, Any], meta: Mapping[str, Any]) -> str | None:
    seconds_value = _first(
        meta,
        "time_to_expiry_seconds",
        "timeToExpirySeconds",
        "seconds_to_expiry",
        "secondsToExpiry",
    )
    if seconds_value in (None, ""):
        seconds_value = _first(order, "time_to_expiry_seconds", "timeToExpirySeconds", "seconds_to_expiry", "secondsToExpiry")
    if seconds_value in (None, ""):
        seconds_value = _first(payload, "time_to_expiry_seconds", "timeToExpirySeconds", "seconds_to_expiry", "secondsToExpiry")
    if seconds_value not in (None, ""):
        return _time_to_expiry_bucket(_decimal(seconds_value) / Decimal("86400"))

    days_value = _first(
        meta,
        "time_to_expiry_days",
        "timeToExpiryDays",
        "days_to_expiry",
        "daysToExpiry",
    )
    if days_value in (None, ""):
        days_value = _first(order, "time_to_expiry_days", "timeToExpiryDays", "days_to_expiry", "daysToExpiry")
    if days_value in (None, ""):
        days_value = _first(payload, "time_to_expiry_days", "timeToExpiryDays", "days_to_expiry", "daysToExpiry")
    if days_value in (None, ""):
        return None
    return _time_to_expiry_bucket(_decimal(days_value))


def _time_to_expiry_bucket(days: Decimal) -> str:
    if days <= Decimal("1"):
        return "lt_1d"
    if days <= Decimal("7"):
        return "lt_7d"
    if days <= Decimal("30"):
        return "lt_30d"
    return "gt_30d"


def _simulated_cash_delta(order: Mapping[str, Any]) -> str:
    meta = _payload(order.get("meta"))
    explicit = _first(meta, "cash_delta", "cashDelta")
    if explicit not in (None, ""):
        return str(explicit)
    filled_notional = _decimal(_first(order, "filled_notional", "filledNotional"))
    fee = _decimal(_first(order, "fee_cost", "feeCost"))
    rebate = _decimal(_first(order, "rebate_cost", "rebateCost"))
    side = str(_first(order, "side") or "").upper()
    cash = filled_notional - fee + rebate if side.startswith("SELL") else -(filled_notional + fee - rebate)
    return _decimal_text(cash)


def _simulated_position_delta(order: Mapping[str, Any]) -> str:
    meta = _payload(order.get("meta"))
    explicit = _first(meta, "position_delta", "positionDelta")
    if explicit not in (None, ""):
        return str(explicit)
    filled_size = _decimal(_first(order, "filled_size", "filledSize"))
    side = str(_first(order, "side") or "").upper()
    position = -filled_size if side.startswith("SELL") else filled_size
    return _decimal_text(position)


def _sample_id(run_id: Any, order_id: str | None, event: Mapping[str, Any], external_order_id: str | None) -> str:
    parts = (
        run_id,
        order_id,
        external_order_id,
        _first(event, "event_id", "eventId"),
        _first(event, "event_type", "eventType"),
        _first(event, "event_time", "eventTime"),
    )
    return "|".join(str(part or "") for part in parts).strip("|") or "auto-calibration-sample"


def _payload(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _event_payload(event: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = _payload(event.get("payload"))
    if not nested:
        return event
    merged = dict(event)
    merged.update(nested)
    return merged


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _parse_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _decimal_text(value: Decimal) -> str:
    text = format(value.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP), "f")
    text = text.rstrip("0").rstrip(".") if "." in text else text
    return text or "0"
