"""Canonical replay rows derived from persisted backtest artifacts."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


REPLAY_SCHEMA_VERSION = "quant-replay-v1"
TRADING_LEDGER_EVENTS = {"BUY", "SELL", "SETTLEMENT"}


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _meta(row: Mapping[str, Any]) -> dict[str, Any]:
    value = row.get("meta")
    return dict(value) if isinstance(value, Mapping) else {}


def _side(value: Any) -> str:
    text = str(value or "").upper()
    if "SELL" in text or text == "SETTLEMENT":
        return "SELL"
    if "BUY" in text:
        return "BUY"
    return text or "UNKNOWN"


def _timestamp(row: Mapping[str, Any], meta: Mapping[str, Any]) -> Any:
    return (
        meta.get("fill_timestamp")
        or meta.get("source_timestamp")
        or meta.get("submit_timestamp")
        or meta.get("signal_timestamp")
        or meta.get("timestamp")
        or row.get("timestamp")
        or row.get("created_at")
    )


def _size_usd(price: Decimal | None, size: Decimal | None, cash_delta: Decimal | None) -> Decimal | None:
    if price is not None and size is not None:
        return abs(price * size)
    return abs(cash_delta) if cash_delta is not None else None


def _outcome_label(run: Mapping[str, Any], row: Mapping[str, Any], meta: Mapping[str, Any]) -> str:
    run_meta = _meta(run)
    return str(
        row.get("outcome_label")
        or meta.get("outcome_label")
        or run.get("outcome_label")
        or run_meta.get("outcome_label")
        or run.get("market_slug")
        or "Outcome"
    )


def _signal_id(order: Mapping[str, Any], meta: Mapping[str, Any]) -> Any:
    order_meta = _meta(order)
    return meta.get("signal_id") or order_meta.get("signal_id") or order_meta.get("signalId")


def build_backtest_replay(
    run: Mapping[str, Any],
    *,
    orders: list[dict[str, Any]],
    ledger: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build one auditable replay stream without merging semantic duplicates."""

    run_id = int(run.get("run_id") or 0)
    order_by_id = {str(row.get("order_id")): row for row in orders if row.get("order_id")}
    trade_by_id = {str(row.get("trade_id")): row for row in trades if row.get("trade_id")}
    ledger_order_ids: set[str] = set()
    items: list[dict[str, Any]] = []

    for row in ledger:
        event_type = str(row.get("event_type") or "").upper()
        row_meta = _meta(row)
        order_id = str(row.get("order_id") or "") or None
        trade_id = str(row.get("trade_id") or "") or None
        order = order_by_id.get(order_id or "", {})
        trade = trade_by_id.get(trade_id or "", {})
        shares_delta = _decimal(row.get("shares_delta"))
        cash_delta = _decimal(row.get("cash_delta"))
        price = _decimal(row.get("price")) or _decimal(order.get("avg_fill_price"))
        is_fill = event_type in TRADING_LEDGER_EVENTS and shares_delta not in (None, Decimal("0"))
        if is_fill and order_id:
            ledger_order_ids.add(order_id)
        item_type = "FILL" if is_fill else "CASHFLOW"
        size = abs(shares_delta) if shares_delta is not None else None
        items.append({
            "schema_version": REPLAY_SCHEMA_VERSION,
            "run_id": run_id,
            "replay_id": f"ledger:{row.get('ledger_id')}",
            "event_type": item_type,
            "lifecycle_type": event_type,
            "source_table": "quant.quant_backtest_ledger",
            "evidence_level": "fill_ledger" if is_fill else "account_ledger",
            "x_axis": row.get("x_axis"),
            "x_value": row.get("x_value"),
            "block_number": row.get("x_value") if row.get("x_axis") == "block_number" else row_meta.get("block_number"),
            "timestamp": _timestamp(row, row_meta),
            "market_slug": row.get("market_slug") or run.get("market_slug"),
            "outcome_label": _outcome_label(run, row, row_meta),
            "token_side": row.get("token_side") or run.get("token_side"),
            "side": _side(event_type),
            "status": "FILLED" if is_fill else event_type,
            "fill_price": price,
            "probability": price,
            "filled_size": size if is_fill else None,
            "size_usd": _size_usd(price, size, cash_delta) if is_fill else abs(cash_delta) if cash_delta is not None else None,
            "fee": row.get("fee"),
            "rebate": row.get("rebate"),
            "slippage_cost": row.get("slippage_cost"),
            "execution_cost": row.get("execution_cost"),
            "realized_pnl": row.get("realized_pnl"),
            "pnl_pct": trade.get("pnl_pct"),
            "reason": trade.get("exit_reason") or order.get("execution_source") or row.get("source") or event_type,
            "confidence": row_meta.get("confidence"),
            "signal_id": _signal_id(order, row_meta),
            "order_id": order_id,
            "fill_id": row_meta.get("fill_id") or (f"{row.get('ledger_id')}:fill" if is_fill else None),
            "trade_id": trade_id,
            "ledger_id": row.get("ledger_id"),
            "position_after": row.get("position_after"),
            "cash_after": row.get("cash_after"),
            "provenance": {
                "ledger_id": row.get("ledger_id"),
                "order_id": order_id,
                "trade_id": trade_id,
                "source_event_ids": list(row_meta.get("source_event_ids") or []),
                "fill_index": row_meta.get("fill_index"),
            },
        })

    for row in orders:
        order_id = str(row.get("order_id") or "")
        if order_id in ledger_order_ids:
            continue
        row_meta = _meta(row)
        filled_size = (
            _decimal(row.get("actual_fill_size"))
            or _decimal(row.get("filled_size"))
            or _decimal(row_meta.get("actual_fill_size"))
            or _decimal(row_meta.get("filledSize"))
            or _decimal(row_meta.get("filled_size"))
            or Decimal("0")
        )
        is_fill = filled_size > 0
        fill_price = (
            _decimal(row.get("avg_fill_price"))
            or _decimal(row.get("decision_price"))
            or _decimal(row_meta.get("avgFillPrice"))
            or _decimal(row_meta.get("avg_fill_price"))
        )
        x_value = row.get("submit_x") or row.get("signal_x")
        items.append({
            "schema_version": REPLAY_SCHEMA_VERSION,
            "run_id": run_id,
            "replay_id": f"order:{order_id}",
            "event_type": "FILL" if is_fill else "ORDER",
            "lifecycle_type": str(row.get("status") or "ORDER").upper(),
            "source_table": "quant.quant_backtest_orders",
            "evidence_level": "order_only" if is_fill else "order_state",
            "x_axis": row.get("x_axis"),
            "x_value": x_value,
            "block_number": x_value if row.get("x_axis") == "block_number" else row_meta.get("block_number"),
            "timestamp": _timestamp(row, row_meta),
            "market_slug": run.get("market_slug"),
            "outcome_label": _outcome_label(run, row, row_meta),
            "token_side": run.get("token_side"),
            "side": _side(row.get("side")),
            "status": row.get("status"),
            "fill_price": fill_price if is_fill else None,
            "probability": fill_price,
            "filled_size": filled_size if is_fill else None,
            "size_usd": (
                row.get("actual_fill_notional")
                or row.get("filled_notional")
                or row_meta.get("actual_fill_notional")
                or row_meta.get("filledNotional")
                or row_meta.get("filled_notional")
            ) if is_fill else None,
            "fee": row.get("fee_cost"),
            "rebate": row.get("rebate_cost"),
            "slippage_cost": row.get("slippage_cost"),
            "execution_cost": row.get("execution_cost"),
            "realized_pnl": None,
            "pnl_pct": None,
            "reason": row.get("no_fill_reason") or row.get("execution_source") or row.get("status"),
            "confidence": row_meta.get("confidence"),
            "signal_id": _signal_id(row, row_meta),
            "order_id": order_id,
            "fill_id": f"{order_id}:order-fill" if is_fill else None,
            "trade_id": row.get("trade_id"),
            "ledger_id": None,
            "position_after": None,
            "cash_after": None,
            "provenance": {
                "order_id": order_id,
                "execution_evidence_type": row.get("execution_evidence_type"),
                "raw_consumed_event_count": row.get("raw_consumed_event_count"),
            },
        })

    for row in events:
        row_meta = _meta(row)
        x_value = row.get("x_value")
        if x_value in (None, "") or int(x_value) <= 0:
            # Framework/audit metadata belongs to the run artifact, not the
            # time-ordered replay cursor stream.
            continue
        items.append({
            "schema_version": REPLAY_SCHEMA_VERSION,
            "run_id": run_id,
            "replay_id": f"event:{row.get('event_index')}",
            "event_type": "SIGNAL",
            "lifecycle_type": str(row.get("event_type") or "SIGNAL").upper(),
            "source_table": "quant.quant_backtest_events",
            "evidence_level": "strategy_event",
            "x_axis": row.get("x_axis"),
            "x_value": x_value,
            "block_number": x_value if row.get("x_axis") == "block_number" else row_meta.get("block_number"),
            "timestamp": _timestamp(row, row_meta),
            "market_slug": run.get("market_slug"),
            "outcome_label": _outcome_label(run, row, row_meta),
            "token_side": run.get("token_side"),
            "side": _side(row.get("event_type")),
            "status": row.get("event_type"),
            "fill_price": None,
            "probability": row.get("price"),
            "filled_size": None,
            "size_usd": None,
            "fee": None,
            "rebate": None,
            "slippage_cost": None,
            "execution_cost": None,
            "realized_pnl": None,
            "pnl_pct": None,
            "reason": row.get("message") or row.get("event_type"),
            "confidence": row_meta.get("confidence"),
            "signal_id": row_meta.get("signal_id") or f"event-{row.get('event_index')}",
            "order_id": row_meta.get("order_id"),
            "fill_id": None,
            "trade_id": row.get("trade_id"),
            "ledger_id": None,
            "position_after": None,
            "cash_after": None,
            "provenance": {"event_index": row.get("event_index")},
        })

    precedence = {"SIGNAL": 0, "ORDER": 1, "FILL": 2, "CASHFLOW": 3}
    items.sort(key=lambda item: (int(item.get("x_value") or 0), precedence.get(str(item.get("event_type")), 9), str(item.get("replay_id"))))
    for sequence, item in enumerate(items, start=1):
        item["sequence"] = sequence

    summary = {
        "item_count": len(items),
        "fill_count": sum(item["event_type"] == "FILL" for item in items),
        "order_count": sum(item["event_type"] == "ORDER" for item in items),
        "signal_count": sum(item["event_type"] == "SIGNAL" for item in items),
        "cashflow_count": sum(item["event_type"] == "CASHFLOW" for item in items),
        "ledger_fill_count": sum(item["evidence_level"] == "fill_ledger" for item in items),
        "order_only_fill_count": sum(item["evidence_level"] == "order_only" for item in items),
    }
    return {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "run_id": run_id,
        "status": run.get("status"),
        "items": items,
        "summary": summary,
    }
