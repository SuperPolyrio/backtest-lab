"""Fill evidence summaries for fill-first backtest runs."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence

from quant.backtest.backtest_engine import build_fill_quality_report
from quant.backtest.orders import execution_evidence_type


def build_fill_evidence_validation_report(
    orders: Sequence[Mapping[str, Any]],
    *,
    fill_quality: Mapping[str, Any] | None = None,
    max_orders: int = 50,
) -> dict[str, Any]:
    rows = [dict(row) for row in orders]
    provided_quality = dict(fill_quality or {})
    if provided_quality and "execution_evidence_counts" in provided_quality:
        quality = provided_quality
    else:
        quality = build_fill_quality_report(rows)
    submitted = int(quality.get("submitted_count") or len(rows))
    filled = int(quality.get("filled_count") or 0)
    raw_fills = int(quality.get("raw_orderfilled_fill_count") or 0)
    block_fallback_fills = int(quality.get("block_bar_synthetic_fill_count") or 0)
    no_fill = int(quality.get("no_fill_count") or 0)
    order_rows = [_order_evidence_row(row) for row in rows[: max(0, int(max_orders))]]
    status = "ready"
    reasons: list[str] = []
    if submitted <= 0:
        status = "missing"
        reasons.append("no submitted orders")
    elif filled > 0 and raw_fills <= 0:
        status = "review"
        reasons.append("filled orders have no raw OrderFilled evidence")
    if block_fallback_fills > 0:
        status = "review" if status == "ready" else status
        reasons.append(f"{block_fallback_fills} filled orders used block bar fallback")
    return {
        "status": status,
        "reason": "; ".join(reasons) or "fill evidence is auditable",
        "submitted_count": submitted,
        "filled_count": filled,
        "no_fill_count": no_fill,
        "raw_orderfilled_fill_count": raw_fills,
        "block_bar_synthetic_fill_count": block_fallback_fills,
        "raw_replay_coverage_pct": str(quality.get("raw_replay_coverage_pct") or _pct(raw_fills, filled)),
        "block_bar_fallback_pct": str(quality.get("block_bar_fallback_pct") or _pct(block_fallback_fills, filled)),
        "execution_evidence_counts": dict(quality.get("execution_evidence_counts") or {}),
        "fill_evidence_counts": dict(quality.get("fill_evidence_counts") or {}),
        "orders_returned": len(order_rows),
        "orders": order_rows,
    }


def fill_evidence_validation_report_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Fill Evidence Validation: {report.get('status')}",
        "",
        f"- reason: {report.get('reason') or '-'}",
        f"- submitted_count: {report.get('submitted_count', 0)}",
        f"- filled_count: {report.get('filled_count', 0)}",
        f"- no_fill_count: {report.get('no_fill_count', 0)}",
        f"- raw_orderfilled_fill_count: {report.get('raw_orderfilled_fill_count', 0)}",
        f"- block_bar_synthetic_fill_count: {report.get('block_bar_synthetic_fill_count', 0)}",
        f"- raw_replay_coverage_pct: {report.get('raw_replay_coverage_pct', '0')}%",
        f"- block_bar_fallback_pct: {report.get('block_bar_fallback_pct', '0')}%",
        "",
        "## Orders",
        "",
        "| order_id | status | evidence | raw candidates | raw consumed | ticks | fillable | side discounted | block BUY/SELL/UNK | block field | block price | no_fill_reason |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | --- |",
    ]
    for row in report.get("orders") or []:
        if not isinstance(row, Mapping):
            continue
        lines.append(
            "| {order_id} | {status} | {evidence} | {raw_candidate_event_count} | "
            "{raw_consumed_event_count} | {fill_schedule_tick_count} | {fill_schedule_fillable_size} | "
            "{side_discounted_tick_count} | {block_buy_volume}/{block_sell_volume}/{block_unknown_side_volume} | "
            "{block_bar_cross_field} | {block_bar_cross_price} | {no_fill_reason} |".format(
                order_id=row.get("order_id") or "-",
                status=row.get("status") or "-",
                evidence=row.get("execution_evidence_type") or "-",
                raw_candidate_event_count=row.get("raw_candidate_event_count", 0),
                raw_consumed_event_count=row.get("raw_consumed_event_count", 0),
                fill_schedule_tick_count=row.get("fill_schedule_tick_count", 0),
                fill_schedule_fillable_size=row.get("fill_schedule_fillable_size") or "0",
                side_discounted_tick_count=row.get("side_discounted_tick_count", 0),
                block_buy_volume=row.get("block_buy_volume") or "0",
                block_sell_volume=row.get("block_sell_volume") or "0",
                block_unknown_side_volume=row.get("block_unknown_side_volume") or "0",
                block_bar_cross_field=row.get("block_bar_cross_field") or "-",
                block_bar_cross_price=row.get("block_bar_cross_price") or "-",
                no_fill_reason=row.get("no_fill_reason") or "-",
            )
        )
    return "\n".join(lines) + "\n"


def _order_evidence_row(order: Mapping[str, Any]) -> dict[str, Any]:
    meta = order.get("meta") if isinstance(order.get("meta"), Mapping) else {}
    candidate_events = meta.get("candidate_events") if isinstance(meta, Mapping) else None
    consumed_events = meta.get("consumed_events") if isinstance(meta, Mapping) else None
    schedule_summary = _fill_schedule_summary(meta.get("fill_schedule") if isinstance(meta, Mapping) else None)
    return {
        "order_id": order.get("order_id"),
        "status": order.get("status"),
        "side": order.get("side"),
        "execution_source": order.get("execution_source"),
        "execution_evidence_type": order.get("execution_evidence_type") or meta.get("execution_evidence_type") or execution_evidence_type(dict(order)),
        "raw_candidate_event_count": order.get("raw_candidate_event_count") if order.get("raw_candidate_event_count") is not None else (len(candidate_events) if isinstance(candidate_events, list) else 0),
        "raw_consumed_event_count": order.get("raw_consumed_event_count") if order.get("raw_consumed_event_count") is not None else (len(consumed_events) if isinstance(consumed_events, list) else 0),
        "block_bar_crossed": order.get("block_bar_crossed") if order.get("block_bar_crossed") is not None else meta.get("block_bar_crossed"),
        "block_bar_cross_field": order.get("block_bar_cross_field") or meta.get("block_bar_cross_field"),
        "block_bar_cross_price": str(order.get("block_bar_cross_price") or meta.get("block_bar_cross_price") or ""),
        "no_fill_reason": order.get("no_fill_reason"),
        **schedule_summary,
    }


def _fill_schedule_summary(schedule: Any) -> dict[str, Any]:
    rows = schedule if isinstance(schedule, Sequence) and not isinstance(schedule, (str, bytes, bytearray)) else []
    tick_count = 0
    fillable_size = Decimal("0")
    side_counts: dict[str, int] = {}
    side_discounted = 0
    block_contexts: dict[tuple[str, str, str], dict[str, Decimal | str]] = {}
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        tick_count += 1
        fillable_size += _decimal(item.get("fillable_size"))
        side = str(item.get("side_compatibility") or "unknown")
        side_counts[side] = side_counts.get(side, 0) + 1
        factor = _decimal(item.get("side_compatibility_factor"))
        if "incompatible" in side or (factor > 0 and factor < Decimal("1")):
            side_discounted += 1
        block_key = (
            str(item.get("market_id") or ""),
            str(item.get("token_id") or ""),
            str(item.get("block_number") or ""),
        )
        if block_key not in block_contexts:
            block_contexts[block_key] = {
                "market_id": block_key[0],
                "token_id": block_key[1],
                "block_number": block_key[2],
                "buy_volume": _decimal(item.get("block_buy_volume")),
                "sell_volume": _decimal(item.get("block_sell_volume")),
                "unknown_side_volume": _decimal(item.get("block_unknown_side_volume")),
                "buy_notional": _decimal(item.get("block_buy_notional")),
                "sell_notional": _decimal(item.get("block_sell_notional")),
                "unknown_side_notional": _decimal(item.get("block_unknown_side_notional")),
                "vwap": str(item.get("block_vwap_price") or ""),
            }
    buy_volume = sum((_decimal(row.get("buy_volume")) for row in block_contexts.values()), Decimal("0"))
    sell_volume = sum((_decimal(row.get("sell_volume")) for row in block_contexts.values()), Decimal("0"))
    unknown_volume = sum((_decimal(row.get("unknown_side_volume")) for row in block_contexts.values()), Decimal("0"))
    return {
        "fill_schedule_tick_count": tick_count,
        "fill_schedule_fillable_size": _decimal_text(fillable_size),
        "side_compatibility_counts": dict(sorted(side_counts.items())),
        "side_discounted_tick_count": side_discounted,
        "block_side_context_count": len(block_contexts),
        "block_buy_volume": _decimal_text(buy_volume),
        "block_sell_volume": _decimal_text(sell_volume),
        "block_unknown_side_volume": _decimal_text(unknown_volume),
        "block_side_contexts": [
            {
                **{key: value for key, value in row.items() if key in {"market_id", "token_id", "block_number", "vwap"}},
                "buy_volume": _decimal_text(_decimal(row.get("buy_volume"))),
                "sell_volume": _decimal_text(_decimal(row.get("sell_volume"))),
                "unknown_side_volume": _decimal_text(_decimal(row.get("unknown_side_volume"))),
                "buy_notional": _decimal_text(_decimal(row.get("buy_notional"))),
                "sell_notional": _decimal_text(_decimal(row.get("sell_notional"))),
                "unknown_side_notional": _decimal_text(_decimal(row.get("unknown_side_notional"))),
            }
            for row in list(block_contexts.values())[:10]
        ],
    }


def _pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0"
    value = (Decimal(str(numerator)) / Decimal(str(denominator)) * Decimal("100")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return format(value.normalize(), "f")


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP).normalize(), "f")
