"""Order lifecycle rows for quant backtests."""

from __future__ import annotations

from decimal import Decimal
from typing import Any


def next_order_id(index: int) -> str:
    return f"O-{int(index):04d}"


def order_status_from_fill(fill: dict[str, Any]) -> str:
    status = str(fill.get("fill_status") or "").upper()
    if status == "FILLED":
        return "FILLED"
    if status in {"PARTIAL", "PARTIAL_FILLED"}:
        return "PARTIAL_FILLED"
    if status in {"CANCELED", "CANCELLED"}:
        return "CANCELED"
    if status == "CANCEL_FAILED":
        return "CANCEL_FAILED"
    if status == "EXPIRED":
        return "EXPIRED"
    if status == "NO_FILL":
        return "NO_FILL"
    if status in {"MODELED_EXPECTATION", "MODELED_DISTRIBUTION", "UNOBSERVABLE"}:
        return status
    if fill.get("rejected"):
        notes = fill.get("notes") or []
        if isinstance(notes, list) and any(
            item in notes
            for item in (
                "no_orderfilled_volume",
                "buy_limit_not_crossed",
                "sell_limit_not_crossed",
                "limit_not_crossed",
                "terminal_price_limit_not_fillable",
                "unresolved_without_settlement_value",
                "orderfilled_no_fill",
                "lob_no_book",
                "lob_stale_book",
                "lob_insufficient_depth",
                "orderfilled_lob_no_overlap",
                "l2_no_executable_depth",
                "book_stale_or_missing",
                "price_limit_or_empty_depth",
            )
        ):
            return "NO_FILL"
        return "REJECTED"
    if Decimal(str(fill.get("filled_size") or fill.get("size") or 0)) <= 0:
        return "NO_FILL"
    return status or "SUBMITTED"


def no_fill_reason(fill: dict[str, Any]) -> str | None:
    notes = fill.get("notes")
    if isinstance(notes, list) and notes:
        return ",".join(str(item) for item in notes)
    if isinstance(notes, str) and notes:
        return notes
    status = order_status_from_fill(fill)
    if status in {"NO_FILL", "REJECTED", "CANCELED", "CANCEL_FAILED"}:
        return status.lower()
    return None


def execution_evidence_type(order_or_fill: dict[str, Any]) -> str:
    """Classify what evidence supports a simulated order fill."""

    raw_meta = order_or_fill.get("meta")
    meta: dict[str, Any] = raw_meta if isinstance(raw_meta, dict) else order_or_fill
    status = str(order_or_fill.get("status") or meta.get("fill_status") or "").upper()
    try:
        filled_size = Decimal(str(order_or_fill.get("filled_size") or meta.get("filled_size") or meta.get("size") or 0))
    except Exception:
        filled_size = Decimal("0")
    if filled_size <= 0 or status in {"NO_FILL", "REJECTED", "EXPIRED", "CANCELED", "CANCEL_FAILED"}:
        return "none"

    explicit = str(
        order_or_fill.get("execution_evidence_type")
        or meta.get("execution_evidence_type")
        or ""
    ).strip().lower()
    if explicit and explicit not in {"unknown", "none"}:
        return explicit

    source = str(order_or_fill.get("execution_source") or meta.get("execution_source") or "").lower()
    consumed_events = meta.get("consumed_events") if isinstance(meta, dict) else None
    if bool(meta.get("block_bar_used_for_fill")) or "synthetic" in source:
        return "block_bar_ohlcv_fallback"
    if isinstance(consumed_events, list) and consumed_events:
        return "raw_orderfilled"
    if "orderfilled_limit_replay_raw" in source:
        return "raw_orderfilled"
    if meta.get("book_snapshot_id") is not None or "lob" in source or "clob" in source:
        return "lob_depth"
    if "settlement" in source:
        return "settlement"
    return "model"


def _event_count(meta: dict[str, Any], key: str) -> int:
    value = meta.get(key)
    return len(value) if isinstance(value, list) else 0


def enrich_order_evidence_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Populate evidence fields for persisted rows, including legacy meta-only rows."""

    raw_meta = row.get("meta")
    meta: dict[str, Any] = raw_meta if isinstance(raw_meta, dict) else {}
    evidence_type = row.get("execution_evidence_type") or meta.get("execution_evidence_type")
    if evidence_type in (None, "", "unknown"):
        evidence_type = execution_evidence_type(row)
    candidate_events = meta.get("candidate_events") if isinstance(meta, dict) else None
    consumed_events = meta.get("consumed_events") if isinstance(meta, dict) else None
    row["execution_evidence_type"] = evidence_type
    if int(row.get("raw_candidate_event_count") or 0) <= 0 and isinstance(candidate_events, list):
        row["raw_candidate_event_count"] = len(candidate_events)
    if int(row.get("raw_consumed_event_count") or 0) <= 0 and isinstance(consumed_events, list):
        row["raw_consumed_event_count"] = len(consumed_events)
    if row.get("block_bar_cross_field") in (None, ""):
        row["block_bar_cross_field"] = meta.get("block_bar_cross_field")
    if row.get("block_bar_cross_price") in (None, ""):
        row["block_bar_cross_price"] = meta.get("block_bar_cross_price")
    if not bool(row.get("block_bar_crossed")) and meta.get("block_bar_crossed") is not None:
        row["block_bar_crossed"] = bool(meta.get("block_bar_crossed"))
    return row


def order_from_fill(
    *,
    order_id: str,
    signal_index: int,
    x_axis: str,
    x_value: int,
    side: str,
    role: str,
    order_type: str,
    decision_price: Decimal,
    fill: dict[str, Any],
    trade_id: str | None = None,
    latency_seconds: Decimal = Decimal("0"),
    latency_blocks: int = 0,
    submit_x_override: int | None = None,
) -> dict[str, Any]:
    meta = dict(fill)
    avg_fill_price = fill.get("avg_fill_price") or fill.get("entry_price") or fill.get("exit_price")
    status = order_status_from_fill(fill)
    requested_size = Decimal(str(fill.get("requested_size") or fill.get("size") or 0))
    requested_notional = Decimal(str(fill.get("requested_notional") or 0))
    if status in {"NO_FILL", "REJECTED", "CANCELED", "CANCEL_FAILED"}:
        filled_size = Decimal("0")
        filled_notional = Decimal("0")
        unfilled_size = requested_size
    else:
        filled_size = Decimal(str(fill.get("filled_size") or fill.get("size") or 0))
        filled_notional = Decimal(str(fill.get("filled_notional") or 0))
        unfilled_size = Decimal(str(fill.get("unfilled_size") or 0))
    expected_fill_size = Decimal(
        str(fill["expected_fill_size"] if "expected_fill_size" in fill else filled_size)
    )
    actual_fill_size = Decimal(
        str(fill["actual_fill_size"] if "actual_fill_size" in fill else filled_size)
    )
    expected_fill_notional = Decimal(
        str(
            fill["expected_fill_notional"]
            if "expected_fill_notional" in fill
            else filled_notional
        )
    )
    actual_fill_notional = Decimal(
        str(
            fill["actual_fill_notional"]
            if "actual_fill_notional" in fill
            else filled_notional
        )
    )
    participation_rate = Decimal(str(fill.get("participation_rate") or 0))
    if participation_rate <= 0:
        block_volume = Decimal(str(fill.get("block_volume") or 0))
        if block_volume > 0 and requested_size > 0:
            participation_rate = requested_size * Decimal("100") / block_volume
    evidence_type = execution_evidence_type(
        {
            **meta,
            "status": status,
            "filled_size": filled_size,
            "execution_source": str(fill.get("execution_source") or "unknown"),
        }
    )
    raw_candidate_event_count = _event_count(meta, "candidate_events")
    raw_consumed_event_count = _event_count(meta, "consumed_events")
    meta["execution_evidence_type"] = evidence_type
    meta["raw_candidate_event_count"] = raw_candidate_event_count
    meta["raw_consumed_event_count"] = raw_consumed_event_count
    return {
        "order_id": order_id,
        "signal_index": int(signal_index),
        "trade_id": trade_id,
        "x_axis": x_axis,
        "signal_x": int(x_value),
        "submit_x": int(submit_x_override) if submit_x_override is not None else (int(x_value) + int(latency_blocks or 0) if x_axis == "block_number" else int(x_value)),
        "decision_price": decision_price,
        "requested_price": avg_fill_price or decision_price,
        "side": side,
        "role": role,
        "order_type": order_type,
        "status": status,
        "requested_size": requested_size,
        "requested_notional": requested_notional,
        "expected_fill_size": expected_fill_size,
        "expected_fill_notional": expected_fill_notional,
        "actual_fill_size": actual_fill_size,
        "actual_fill_notional": actual_fill_notional,
        "filled_size": filled_size,
        "filled_notional": filled_notional,
        "unfilled_size": unfilled_size,
        "avg_fill_price": avg_fill_price,
        "fill_probability": Decimal(str(fill.get("fill_probability") or 0)),
        "fill_pct": Decimal(str(fill.get("fill_pct") or 0)),
        "block_volume": Decimal(str(fill.get("block_volume") or 0)),
        "trade_count": int(fill.get("trade_count") or 0),
        "participation_rate": participation_rate,
        "available_notional": Decimal(str(fill.get("available_notional") or 0)),
        "fee_cost": Decimal(str(fill.get("fee_cost") or 0)),
        "rebate": Decimal(str(fill.get("rebate") or fill.get("rebate_cost") or 0)),
        "rebate_cost": Decimal(str(fill.get("rebate") or fill.get("rebate_cost") or 0)),
        "slippage_cost": Decimal(str(fill.get("slippage_cost") or 0)),
        "execution_cost": Decimal(str(fill.get("execution_cost") or 0)),
        "latency_blocks": int(latency_blocks or 0),
        "latency_seconds": Decimal(str(latency_seconds or 0)),
        "no_fill_reason": no_fill_reason(fill),
        "execution_source": str(fill.get("execution_source") or "unknown"),
        "execution_evidence_type": evidence_type,
        "raw_candidate_event_count": raw_candidate_event_count,
        "raw_consumed_event_count": raw_consumed_event_count,
        "block_bar_crossed": bool(meta.get("block_bar_crossed")),
        "block_bar_cross_field": meta.get("block_bar_cross_field"),
        "block_bar_cross_price": meta.get("block_bar_cross_price"),
        "meta": meta,
    }


def summarize_orders(orders: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "signal_count": len(orders),
        "submitted_count": len(orders),
        "filled_count": 0,
        "partial_fill_count": 0,
        "no_fill_count": 0,
        "rejected_count": 0,
        "expired_count": 0,
    }
    for order in orders:
        status = str(order.get("status") or "").upper()
        if status == "FILLED":
            counts["filled_count"] += 1
        elif status == "PARTIAL_FILLED":
            counts["partial_fill_count"] += 1
        elif status == "NO_FILL":
            counts["no_fill_count"] += 1
        elif status == "REJECTED":
            counts["rejected_count"] += 1
        elif status == "EXPIRED":
            counts["expired_count"] += 1
    return counts
