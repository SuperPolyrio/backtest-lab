"""Backtest ledger helpers.

The ledger is account-view: cash deltas, share deltas, and running account
state are derived from simulated fills. It intentionally stays separate from
the trade table so later SPLIT/MERGE/REDEEM/REBATE events can be added without
rewriting trade history.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence

from .polymarket_cashflow import normalize_polymarket_activity_event


Q = Decimal("0.0000000001")
POLYMARKET_CREDIT_EVENTS = {"SELL", "SETTLEMENT", "REDEEM", "MERGE", "REBATE", "MAKER_REBATE", "REFUND"}
POLYMARKET_DEBIT_EVENTS = {"BUY", "SPLIT"}
POLYMARKET_SPECIAL_EVENTS = {"SPLIT", "MERGE", "REDEEM", "REBATE", "MAKER_REBATE", "REFUND"}
NON_TRADING_EVENTS = {"REWARD", "REFERRAL", "REFERRAL_REWARD", "CONVERSION"}


def build_ledger_rows(
    trades: list[dict[str, Any]],
    initial_capital: Decimal,
    *,
    gas_cost_per_order: Decimal = Decimal("0"),
    settlement_cost: Decimal = Decimal("0"),
    redeem_cost: Decimal = Decimal("0"),
    capital_cost_bps: Decimal = Decimal("0"),
    cashflow_events: Sequence[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    cash = Decimal(str(initial_capital))
    position = Decimal("0")
    rows: list[dict[str, Any]] = []
    ledger_index = 1
    emitted_entries: set[str] = set()
    for trade in trades:
        entry_order_id = str(trade.get("entry_order_id") or trade.get("trade_id") or "")
        exit_order_id = str(trade.get("exit_order_id") or "")
        entry_price = Decimal(str(trade.get("entry_price") or 0))
        entry_fill_slices = trade.get("entry_fill_slices") if isinstance(trade.get("entry_fill_slices"), list) else []
        if entry_order_id not in emitted_entries and entry_fill_slices:
            for fill_index, fill_slice in enumerate(entry_fill_slices, start=1):
                fill_size = Decimal(str(fill_slice.get("size") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
                fill_price = Decimal(str(fill_slice.get("price") or entry_price)).quantize(Q, rounding=ROUND_HALF_UP)
                fill_fee = Decimal(str(fill_slice.get("fee") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
                fill_rebate = Decimal(str(fill_slice.get("rebate") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
                fill_slippage = Decimal(str(fill_slice.get("slippage_cost") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
                fill_notional = (fill_price * fill_size).quantize(Q, rounding=ROUND_HALF_UP)
                cash_delta = -(fill_notional + fill_fee - fill_rebate).quantize(Q, rounding=ROUND_HALF_UP)
                cash += cash_delta
                position += fill_size
                ledger_row = _row(
                    ledger_index=ledger_index,
                    trade=trade,
                    order_id=entry_order_id or None,
                    event_type="BUY",
                    x_value=int(fill_slice.get("x_value") or trade["entry_x"]),
                    shares_delta=fill_size,
                    cash_delta=cash_delta,
                    fee=fill_fee,
                    rebate=fill_rebate,
                    slippage_cost=fill_slippage,
                    execution_cost=fill_fee + fill_slippage - fill_rebate,
                    realized_pnl=Decimal("0"),
                    position_after=position,
                    cash_after=cash,
                    price=fill_price,
                )
                ledger_row["meta"].update({
                    "fill_index": fill_index,
                    "fill_timestamp": fill_slice.get("fill_timestamp"),
                    "source_event_ids": list(fill_slice.get("source_event_ids") or []),
                    "fill_level": True,
                })
                rows.append(ledger_row)
                ledger_index += 1
            ledger_index, cash = _append_cost_row(
                rows,
                ledger_index=ledger_index,
                trade=trade,
                order_id=entry_order_id or None,
                event_type="GAS_COST",
                x_value=int(entry_fill_slices[0].get("x_value") or trade["entry_x"]),
                amount=gas_cost_per_order,
                cash_after=cash,
                position_after=position,
                reason="entry_order",
            )
            emitted_entries.add(entry_order_id)
        if entry_order_id not in emitted_entries:
            entry_trades = [
                item for item in trades
                if str(item.get("entry_order_id") or item.get("trade_id") or "") == entry_order_id
            ]
            entry_size = sum((Decimal(str(item.get("size") or 0)) for item in entry_trades), Decimal("0"))
            entry_fee = sum(
                (
                    Decimal(str(item.get("entry_fee_cost")))
                    if item.get("entry_fee_cost") is not None
                    else Decimal(str(item.get("fee_cost") or 0)) / Decimal("2")
                    for item in entry_trades
                ),
                Decimal("0"),
            ).quantize(Q, rounding=ROUND_HALF_UP)
            entry_slippage = sum(
                (
                    Decimal(str(item.get("entry_slippage_cost")))
                    if item.get("entry_slippage_cost") is not None
                    else Decimal(str(item.get("slippage_cost") or 0)) / Decimal("2")
                    for item in entry_trades
                ),
                Decimal("0"),
            ).quantize(Q, rounding=ROUND_HALF_UP)
            entry_rebate = sum(
                (
                    Decimal(str(item.get("entry_rebate")))
                    if item.get("entry_rebate") is not None
                    else Decimal(str(item.get("rebate") or 0)) / Decimal("2")
                    for item in entry_trades
                ),
                Decimal("0"),
            ).quantize(Q, rounding=ROUND_HALF_UP)
            entry_notional = (entry_price * entry_size).quantize(Q, rounding=ROUND_HALF_UP)
            # Slippage is already embedded in the simulated execution price.
            # Keep it as attribution, but do not subtract it twice from cash.
            cash_delta = -(entry_notional + entry_fee - entry_rebate).quantize(Q, rounding=ROUND_HALF_UP)
            cash += cash_delta
            position += entry_size
            rows.append(_row(
                ledger_index=ledger_index,
                trade=trade,
                order_id=entry_order_id or None,
                event_type="BUY",
                x_value=int(trade["entry_x"]),
                shares_delta=entry_size,
                cash_delta=cash_delta,
                fee=entry_fee,
                rebate=entry_rebate,
                slippage_cost=entry_slippage,
                execution_cost=entry_fee + entry_slippage - entry_rebate,
                realized_pnl=Decimal("0"),
                position_after=position,
                cash_after=cash,
                price=entry_price,
            ))
            ledger_index += 1
            ledger_index, cash = _append_cost_row(
                rows,
                ledger_index=ledger_index,
                trade=trade,
                order_id=entry_order_id or None,
                event_type="GAS_COST",
                x_value=int(trade["entry_x"]),
                amount=gas_cost_per_order,
                cash_after=cash,
                position_after=position,
                reason="entry_order",
            )
            emitted_entries.add(entry_order_id)

        size = Decimal(str(trade.get("size") or 0))
        exit_price = Decimal(str(trade.get("exit_price") or 0))
        exit_notional = (exit_price * size).quantize(Q, rounding=ROUND_HALF_UP)
        fee = Decimal(str(trade.get("fee_cost") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
        slippage = Decimal(str(trade.get("slippage_cost") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
        buy_fee = (
            Decimal(str(trade.get("entry_fee_cost"))).quantize(Q, rounding=ROUND_HALF_UP)
            if trade.get("entry_fee_cost") is not None
            else (fee / Decimal("2")).quantize(Q, rounding=ROUND_HALF_UP)
        )
        sell_fee = (
            Decimal(str(trade.get("exit_fee_cost"))).quantize(Q, rounding=ROUND_HALF_UP)
            if trade.get("exit_fee_cost") is not None
            else (fee - buy_fee).quantize(Q, rounding=ROUND_HALF_UP)
        )
        buy_slippage = (
            Decimal(str(trade.get("entry_slippage_cost"))).quantize(Q, rounding=ROUND_HALF_UP)
            if trade.get("entry_slippage_cost") is not None
            else (slippage / Decimal("2")).quantize(Q, rounding=ROUND_HALF_UP)
        )
        sell_slippage = (
            Decimal(str(trade.get("exit_slippage_cost"))).quantize(Q, rounding=ROUND_HALF_UP)
            if trade.get("exit_slippage_cost") is not None
                else (slippage - buy_slippage).quantize(Q, rounding=ROUND_HALF_UP)
        )
        rebate = Decimal(str(trade.get("rebate") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
        buy_rebate = (
            Decimal(str(trade.get("entry_rebate"))).quantize(Q, rounding=ROUND_HALF_UP)
            if trade.get("entry_rebate") is not None
            else (rebate / Decimal("2")).quantize(Q, rounding=ROUND_HALF_UP)
        )
        sell_rebate = (
            Decimal(str(trade.get("exit_rebate"))).quantize(Q, rounding=ROUND_HALF_UP)
            if trade.get("exit_rebate") is not None
            else (rebate - buy_rebate).quantize(Q, rounding=ROUND_HALF_UP)
        )
        cash_delta = (exit_notional - sell_fee + sell_rebate).quantize(Q, rounding=ROUND_HALF_UP)
        realized_pnl = Decimal(str(trade.get("pnl") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
        cash += cash_delta
        position -= size
        rows.append(_row(
            ledger_index=ledger_index,
            trade=trade,
            order_id=exit_order_id or None,
            event_type="SETTLEMENT" if str(trade.get("exit_reason") or "").lower() == "settlement" else "SELL",
            x_value=int(trade["exit_x"]),
            shares_delta=-size,
            cash_delta=cash_delta,
            fee=sell_fee,
            rebate=sell_rebate,
            slippage_cost=sell_slippage,
            execution_cost=sell_fee + sell_slippage - sell_rebate,
            realized_pnl=realized_pnl,
            position_after=position,
            cash_after=cash,
            price=exit_price,
        ))
        ledger_index += 1
        ledger_index, cash = _append_cost_row(
            rows,
            ledger_index=ledger_index,
            trade=trade,
            order_id=exit_order_id or None,
            event_type="GAS_COST",
            x_value=int(trade["exit_x"]),
            amount=gas_cost_per_order,
            cash_after=cash,
            position_after=position,
            reason="exit_order",
        )
        if str(trade.get("exit_reason") or "").lower() == "settlement":
            ledger_index, cash = _append_cost_row(
                rows,
                ledger_index=ledger_index,
                trade=trade,
                order_id=exit_order_id or None,
                event_type="SETTLEMENT_COST",
                x_value=int(trade["exit_x"]),
                amount=settlement_cost,
                cash_after=cash,
                position_after=position,
                reason="settlement",
            )
            ledger_index, cash = _append_cost_row(
                rows,
                ledger_index=ledger_index,
                trade=trade,
                order_id=exit_order_id or None,
                event_type="REDEEM_COST",
                x_value=int(trade["exit_x"]),
                amount=redeem_cost,
                cash_after=cash,
                position_after=position,
                reason="redeem",
            )
        capital_cost = _capital_cost(trade, capital_cost_bps)
        ledger_index, cash = _append_cost_row(
            rows,
            ledger_index=ledger_index,
            trade=trade,
            order_id=exit_order_id or None,
            event_type="CAPITAL_COST",
            x_value=int(trade["exit_x"]),
            amount=capital_cost,
            cash_after=cash,
            position_after=position,
            reason="capital_occupied",
        )
    if cashflow_events:
        for event in cashflow_events:
            row = _cashflow_event_row(ledger_index, event)
            if row is None:
                continue
            rows.append(row)
            ledger_index += 1
        rows = _rebase_running_account(rows, initial_capital)
    return rows


def build_source_evidenced_order_ledger_rows(
    orders: Sequence[Mapping[str, Any]],
    initial_capital: Decimal,
    *,
    market_slug: str,
    token_side: str,
    token_id: str | None = None,
    gas_cost_per_order: Decimal = Decimal("0"),
    settlement_cost: Decimal = Decimal("0"),
    redeem_cost: Decimal = Decimal("0"),
    cashflow_events: Sequence[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Build the canonical cash ledger from source-evidenced order quantities.

    Fill-only V3 may carry a larger modeled expectation on the same order. Only
    ``actual_fill_size`` is allowed to change cash or inventory here.
    """

    cash = Decimal(str(initial_capital)).quantize(Q, rounding=ROUND_HALF_UP)
    position = Decimal("0").quantize(Q)
    average_cost = Decimal("0").quantize(Q)
    rows: list[dict[str, Any]] = []
    ledger_index = 1

    ordered = sorted(
        orders,
        key=lambda row: (
            _optional_int(row.get("submit_x") or row.get("signal_x")),
            str(row.get("order_id") or ""),
        ),
    )
    for order in ordered:
        raw_meta = order.get("meta")
        meta: Mapping[str, Any] = raw_meta if isinstance(raw_meta, Mapping) else {}
        actual_size = _mapping_decimal(
            order,
            meta,
            "actual_fill_size",
            fallback_key="filled_size",
        )
        if actual_size <= 0:
            continue

        side = str(order.get("side") or "").upper()
        is_buy = side.startswith("BUY")
        is_sell = side.startswith("SELL")
        if not (is_buy or is_sell):
            continue

        applied_size = actual_size if is_buy else min(actual_size, position)
        if applied_size <= 0:
            continue
        scale = applied_size / actual_size
        actual_notional = _mapping_decimal(
            order,
            meta,
            "actual_fill_notional",
            fallback_key="filled_notional",
        )
        raw_price = _mapping_decimal(
            order,
            meta,
            "avg_fill_price",
            fallback_key="requested_price",
        )
        price = (
            actual_notional / actual_size
            if actual_notional > 0 and actual_size > 0
            else raw_price
        ).quantize(Q, rounding=ROUND_HALF_UP)
        notional = (price * applied_size).quantize(Q, rounding=ROUND_HALF_UP)
        fee = (
            _mapping_decimal(order, meta, "actual_fee_cost", fallback_key="fee_cost")
            * scale
        ).quantize(Q, rounding=ROUND_HALF_UP)
        rebate = (
            _mapping_decimal(order, meta, "actual_rebate", fallback_key="rebate")
            * scale
        ).quantize(Q, rounding=ROUND_HALF_UP)
        slippage = (
            _mapping_decimal(order, meta, "actual_slippage_cost", fallback_key="slippage_cost")
            * scale
        ).quantize(Q, rounding=ROUND_HALF_UP)

        if is_buy:
            previous_cost = average_cost * position
            cash_delta = -(notional + fee - rebate).quantize(Q, rounding=ROUND_HALF_UP)
            position += applied_size
            average_cost = (
                (previous_cost + notional) / position
                if position > 0
                else Decimal("0")
            ).quantize(Q, rounding=ROUND_HALF_UP)
            event_type = "BUY"
            realized_pnl = Decimal("0").quantize(Q)
        else:
            cash_delta = (notional - fee + rebate).quantize(Q, rounding=ROUND_HALF_UP)
            realized_pnl = (
                (price - average_cost) * applied_size - fee + rebate
            ).quantize(Q, rounding=ROUND_HALF_UP)
            position -= applied_size
            if position <= 0:
                position = Decimal("0").quantize(Q)
                average_cost = Decimal("0").quantize(Q)
            event_type = (
                "SETTLEMENT"
                if str(order.get("role") or "").lower() == "settlement"
                or str(order.get("order_type") or "").lower() == "settlement"
                else "SELL"
            )

        cash += cash_delta
        trade = {
            "trade_id": order.get("trade_id"),
            "market_slug": market_slug,
            "token_side": token_side,
            "token_id": token_id,
            "x_axis": order.get("x_axis") or "block_number",
            "exit_reason": "settlement" if event_type == "SETTLEMENT" else None,
        }
        row = _row(
            ledger_index=ledger_index,
            trade=trade,
            order_id=str(order.get("order_id") or "") or None,
            event_type=event_type,
            x_value=_optional_int(order.get("submit_x") or order.get("signal_x")),
            shares_delta=applied_size if is_buy else -applied_size,
            cash_delta=cash_delta,
            fee=fee,
            rebate=rebate,
            slippage_cost=slippage,
            execution_cost=fee + slippage - rebate,
            realized_pnl=realized_pnl,
            position_after=position,
            cash_after=cash,
            price=price,
        )
        consumed = meta.get("consumed_events")
        source_ids = [
            str(item.get("trade_id"))
            for item in consumed
            if isinstance(item, Mapping) and item.get("trade_id")
        ] if isinstance(consumed, list) else []
        row["source"] = "source_evidenced_order_fill"
        row["meta"].update(
            {
                "execution_evidence_type": meta.get("execution_evidence_type"),
                "expected_fill_size": str(order.get("expected_fill_size") or "0"),
                "actual_fill_size": str(actual_size),
                "applied_actual_fill_size": str(applied_size),
                "source_event_ids": source_ids,
                "inventory_capped": applied_size < actual_size,
            }
        )
        rows.append(row)
        ledger_index += 1
        ledger_index, cash = _append_cost_row(
            rows,
            ledger_index=ledger_index,
            trade=trade,
            order_id=str(order.get("order_id") or "") or None,
            event_type="GAS_COST",
            x_value=_optional_int(order.get("submit_x") or order.get("signal_x")),
            amount=gas_cost_per_order,
            cash_after=cash,
            position_after=position,
            reason="source_evidenced_order",
        )
        if event_type == "SETTLEMENT":
            for cost_type, amount in (
                ("SETTLEMENT_COST", settlement_cost),
                ("REDEEM_COST", redeem_cost),
            ):
                ledger_index, cash = _append_cost_row(
                    rows,
                    ledger_index=ledger_index,
                    trade=trade,
                    order_id=str(order.get("order_id") or "") or None,
                    event_type=cost_type,
                    x_value=_optional_int(order.get("submit_x") or order.get("signal_x")),
                    amount=amount,
                    cash_after=cash,
                    position_after=position,
                    reason="settlement",
                )

    if cashflow_events:
        for event in cashflow_events:
            cashflow_row = _cashflow_event_row(ledger_index, event)
            if cashflow_row is None:
                continue
            rows.append(cashflow_row)
            ledger_index += 1
    return _rebase_running_account(rows, initial_capital)


def ledger_summary(rows: list[dict[str, Any]], initial_capital: Decimal) -> dict[str, Decimal]:
    cash = Decimal(str(initial_capital))
    position = Decimal("0")
    realized = Decimal("0")
    trade_exit = Decimal("0")
    settlement = Decimal("0")
    fees = Decimal("0")
    slippage = Decimal("0")
    rebate = Decimal("0")
    external_cost = Decimal("0")
    gas_cost = Decimal("0")
    settlement_cost = Decimal("0")
    redeem_cost = Decimal("0")
    capital_cost = Decimal("0")
    redeem_total = Decimal("0")
    merge_total = Decimal("0")
    split_total = Decimal("0")
    special_rebate_total = Decimal("0")
    refund_total = Decimal("0")
    for row in rows:
        cash = Decimal(str(row.get("cash_after", cash)))
        position = Decimal(str(row.get("position_after", position)))
        realized += Decimal(str(row.get("realized_pnl") or 0))
        event_type = str(row.get("event_type") or "").upper()
        if event_type == "SETTLEMENT":
            settlement += Decimal(str(row.get("realized_pnl") or 0))
        elif event_type == "SELL":
            trade_exit += Decimal(str(row.get("realized_pnl") or 0))
        fees += Decimal(str(row.get("fee") or 0))
        slippage += Decimal(str(row.get("slippage_cost") or 0))
        rebate += Decimal(str(row.get("rebate") or 0))
        cost = max(Decimal("0"), -Decimal(str(row.get("cash_delta") or 0)))
        if event_type in {"GAS_COST", "SETTLEMENT_COST", "REDEEM_COST", "CAPITAL_COST"}:
            external_cost += cost
        if event_type == "GAS_COST":
            gas_cost += cost
        elif event_type == "SETTLEMENT_COST":
            settlement_cost += cost
        elif event_type == "REDEEM_COST":
            redeem_cost += cost
        elif event_type == "CAPITAL_COST":
            capital_cost += cost
        cash_delta = Decimal(str(row.get("cash_delta") or 0))
        if event_type == "REDEEM":
            redeem_total += cash_delta
        elif event_type == "MERGE":
            merge_total += cash_delta
        elif event_type == "SPLIT":
            split_total += -cash_delta
        elif event_type in {"REBATE", "MAKER_REBATE"}:
            special_rebate_total += cash_delta
        elif event_type == "REFUND":
            refund_total += cash_delta
    account_pnl = (cash - Decimal(str(initial_capital))).quantize(Q, rounding=ROUND_HALF_UP)
    realized_for_summary = account_pnl if rows and position.quantize(Q, rounding=ROUND_HALF_UP) == 0 else realized.quantize(Q, rounding=ROUND_HALF_UP)
    return {
        "cash_balance": cash.quantize(Q, rounding=ROUND_HALF_UP),
        "position_after": position.quantize(Q, rounding=ROUND_HALF_UP),
        "realized_pnl": realized_for_summary,
        "ledger_cash_pnl": account_pnl,
        "trade_exit_pnl": trade_exit.quantize(Q, rounding=ROUND_HALF_UP),
        "settlement_pnl": settlement.quantize(Q, rounding=ROUND_HALF_UP),
        "fee_total": fees.quantize(Q, rounding=ROUND_HALF_UP),
        "slippage_total": slippage.quantize(Q, rounding=ROUND_HALF_UP),
        "rebate_total": rebate.quantize(Q, rounding=ROUND_HALF_UP),
        "redeem_total": redeem_total.quantize(Q, rounding=ROUND_HALF_UP),
        "merge_total": merge_total.quantize(Q, rounding=ROUND_HALF_UP),
        "split_total": split_total.quantize(Q, rounding=ROUND_HALF_UP),
        "special_rebate_total": special_rebate_total.quantize(Q, rounding=ROUND_HALF_UP),
        "refund_total": refund_total.quantize(Q, rounding=ROUND_HALF_UP),
        "external_cost_total": external_cost.quantize(Q, rounding=ROUND_HALF_UP),
        "gas_cost_total": gas_cost.quantize(Q, rounding=ROUND_HALF_UP),
        "settlement_cost_total": settlement_cost.quantize(Q, rounding=ROUND_HALF_UP),
        "redeem_cost_total": redeem_cost.quantize(Q, rounding=ROUND_HALF_UP),
        "capital_cost_total": capital_cost.quantize(Q, rounding=ROUND_HALF_UP),
        "ledger_rows": Decimal(len(rows)),
    }


def _capital_cost(trade: dict[str, Any], capital_cost_bps: Decimal) -> Decimal:
    bps = max(Decimal("0"), Decimal(str(capital_cost_bps or 0)))
    if bps <= 0:
        return Decimal("0")
    basis = Decimal(str(trade.get("notional") or trade.get("filled_notional") or 0))
    holding_bars = Decimal(str(max(0, int(trade.get("holding_bars") or 0))))
    return (basis * holding_bars * bps / Decimal("10000")).quantize(Q, rounding=ROUND_HALF_UP)


def _append_cost_row(
    rows: list[dict[str, Any]],
    *,
    ledger_index: int,
    trade: dict[str, Any],
    order_id: str | None,
    event_type: str,
    x_value: int,
    amount: Decimal,
    cash_after: Decimal,
    position_after: Decimal,
    reason: str,
) -> tuple[int, Decimal]:
    cost = Decimal(str(amount or 0)).quantize(Q, rounding=ROUND_HALF_UP)
    if cost <= 0:
        return ledger_index, cash_after
    cash_delta = -cost
    next_cash = (cash_after + cash_delta).quantize(Q, rounding=ROUND_HALF_UP)
    row = _row(
        ledger_index=ledger_index,
        trade=trade,
        order_id=order_id,
        event_type=event_type,
        x_value=x_value,
        shares_delta=Decimal("0"),
        cash_delta=cash_delta,
        fee=Decimal("0"),
        rebate=Decimal("0"),
        slippage_cost=Decimal("0"),
        execution_cost=cost,
        realized_pnl=cash_delta,
        position_after=position_after,
        cash_after=next_cash,
        price=Decimal("0"),
    )
    row["source"] = "simulated_cost"
    row["meta"] = {"trade_id": trade.get("trade_id"), "cost_type": event_type.lower(), "reason": reason}
    rows.append(row)
    return ledger_index + 1, next_cash


def _row(
    *,
    ledger_index: int,
    trade: dict[str, Any],
    order_id: str | None,
    event_type: str,
    x_value: int,
    shares_delta: Decimal,
    cash_delta: Decimal,
    fee: Decimal,
    rebate: Decimal,
    slippage_cost: Decimal,
    execution_cost: Decimal,
    realized_pnl: Decimal,
    position_after: Decimal,
    cash_after: Decimal,
    price: Decimal,
) -> dict[str, Any]:
    return {
        "ledger_id": f"L-{ledger_index:04d}",
        "order_id": order_id,
        "trade_id": trade.get("trade_id"),
        "event_type": event_type,
        "x_axis": trade.get("x_axis", "block_number"),
        "x_value": x_value,
        "market_slug": trade.get("market_slug"),
        "event_slug": _trade_value(trade, "event_slug", "eventSlug"),
        "token_id": _trade_value(trade, "token_id", "tokenId"),
        "token_side": trade.get("token_side"),
        "shares_delta": shares_delta.quantize(Q, rounding=ROUND_HALF_UP),
        "cash_delta": cash_delta.quantize(Q, rounding=ROUND_HALF_UP),
        "fee": fee.quantize(Q, rounding=ROUND_HALF_UP),
        "rebate": rebate.quantize(Q, rounding=ROUND_HALF_UP),
        "slippage_cost": slippage_cost.quantize(Q, rounding=ROUND_HALF_UP),
        "execution_cost": execution_cost.quantize(Q, rounding=ROUND_HALF_UP),
        "realized_pnl": realized_pnl.quantize(Q, rounding=ROUND_HALF_UP),
        "position_after": position_after.quantize(Q, rounding=ROUND_HALF_UP),
        "cash_after": cash_after.quantize(Q, rounding=ROUND_HALF_UP),
        "price": price,
        "source": "simulated_trade",
        "meta": {"trade_id": trade.get("trade_id"), "exit_reason": trade.get("exit_reason")},
    }


def _cashflow_event_row(ledger_index: int, event: Mapping[str, Any]) -> dict[str, Any] | None:
    row = normalize_polymarket_activity_event(event)
    event_type = row["event_type"]
    if event_type in NON_TRADING_EVENTS:
        return None
    if event_type not in POLYMARKET_CREDIT_EVENTS | POLYMARKET_DEBIT_EVENTS:
        return None
    amount = Decimal(str(row["amount"])).quantize(Q, rounding=ROUND_HALF_UP)
    shares_delta = Decimal(str(row["shares_delta"])).quantize(Q, rounding=ROUND_HALF_UP)
    cash_delta = _cash_delta_for_event(event_type, amount)
    realized_pnl = _event_realized_pnl(event, event_type, cash_delta)
    position_legs = _cashflow_position_legs(event)
    return {
        "ledger_id": f"L-{ledger_index:04d}",
        "order_id": event.get("order_id") or event.get("orderId"),
        "trade_id": event.get("trade_id") or event.get("tradeId") or row["event_id"] or None,
        "event_type": event_type,
        "x_axis": event.get("x_axis") or event.get("xAxis") or "block_number",
        "x_value": _optional_int(row["x_value"]),
        "market_slug": row["market_slug"],
        "event_slug": row["event_slug"],
        "token_id": row["token_id"],
        "token_side": row["token_side"],
        "shares_delta": shares_delta,
        "cash_delta": cash_delta,
        "fee": Decimal("0").quantize(Q),
        "rebate": cash_delta if event_type in {"REBATE", "MAKER_REBATE"} else Decimal("0").quantize(Q),
        "slippage_cost": Decimal("0").quantize(Q),
        "execution_cost": Decimal("0").quantize(Q),
        "realized_pnl": realized_pnl,
        "position_after": Decimal("0").quantize(Q),
        "cash_after": Decimal("0").quantize(Q),
        "price": _optional_decimal(event.get("price") or event.get("avg_price") or event.get("avgPrice")),
        "position_legs": position_legs,
        "source": str(event.get("source") or row["source"] or "polymarket_cashflow"),
        "meta": {
            "event_id": row["event_id"],
            "raw_event_type": row["raw_event_type"],
            "amount_source": row.get("amount_source"),
            "position_key": row["position_key"],
            "position_legs": position_legs,
        },
    }


def _cash_delta_for_event(event_type: str, amount: Decimal) -> Decimal:
    if event_type in POLYMARKET_DEBIT_EVENTS:
        return -amount
    if event_type in POLYMARKET_CREDIT_EVENTS:
        return amount
    return Decimal("0").quantize(Q)


def _cashflow_position_legs(event: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    value = _first_mapping_value(event, "position_legs", "positionLegs", "legs", "tokens", "outcome_tokens", "outcomeTokens")
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, Mapping)]
    yes_token = _first_mapping_value(event, "yes_token_id", "yesTokenId", "yes_asset_id", "yesAssetId")
    no_token = _first_mapping_value(event, "no_token_id", "noTokenId", "no_asset_id", "noAssetId")
    if yes_token and no_token:
        return [
            {"token_id": str(yes_token), "token_side": "YES"},
            {"token_id": str(no_token), "token_side": "NO"},
        ]
    complement = _first_mapping_value(event, "complement_token_id", "complementTokenId", "complementary_token_id", "complementaryTokenId")
    token_id = _first_mapping_value(event, "token_id", "tokenId", "asset_id", "assetId")
    token_side = _first_mapping_value(event, "token_side", "tokenSide", "outcome", "side")
    if token_id and complement:
        normalized_side = str(token_side or "").upper()
        complement_side = "NO" if normalized_side == "YES" else "YES" if normalized_side == "NO" else ""
        return [
            {"token_id": str(token_id), "token_side": str(token_side or "")},
            {"token_id": str(complement), "token_side": complement_side},
        ]
    return []


def _event_realized_pnl(event: Mapping[str, Any], event_type: str, cash_delta: Decimal) -> Decimal:
    explicit = _first_mapping_value(event, "realized_pnl", "realizedPnl", "pnl")
    if explicit not in (None, ""):
        return Decimal(str(explicit)).quantize(Q, rounding=ROUND_HALF_UP)
    if event_type in {"REBATE", "MAKER_REBATE", "SPLIT"}:
        return cash_delta.quantize(Q, rounding=ROUND_HALF_UP)
    # REDEEM/MERGE/REFUND close or convert position.  Without per-position cost
    # basis on the raw activity row, account-level PnL is reconstructed from
    # cash_after - initial_capital in ledger_summary instead of guessed here.
    return Decimal("0").quantize(Q)


def _rebase_running_account(rows: list[dict[str, Any]], initial_capital: Decimal) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=_ledger_sort_key)
    cash = Decimal(str(initial_capital)).quantize(Q, rounding=ROUND_HALF_UP)
    position = Decimal("0").quantize(Q, rounding=ROUND_HALF_UP)
    rebased: list[dict[str, Any]] = []
    for index, row in enumerate(ordered, start=1):
        item = dict(row)
        item["ledger_id"] = f"L-{index:04d}"
        cash += Decimal(str(item.get("cash_delta") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
        position += Decimal(str(item.get("shares_delta") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
        item["cash_after"] = cash.quantize(Q, rounding=ROUND_HALF_UP)
        item["position_after"] = position.quantize(Q, rounding=ROUND_HALF_UP)
        rebased.append(item)
    return rebased


def _ledger_sort_key(row: Mapping[str, Any]) -> tuple[int, int, str]:
    event_type = str(row.get("event_type") or "").upper()
    priority = {
        "BUY": 10,
        "SPLIT": 15,
        "GAS_COST": 20,
        "SELL": 30,
        "SETTLEMENT": 30,
        "REDEEM": 35,
        "MERGE": 35,
        "REFUND": 35,
        "REBATE": 40,
        "MAKER_REBATE": 40,
        "SETTLEMENT_COST": 50,
        "REDEEM_COST": 55,
        "CAPITAL_COST": 60,
    }.get(event_type, 90)
    return (_optional_int(row.get("x_value")), priority, str(row.get("ledger_id") or ""))


def _optional_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _optional_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0").quantize(Q)
    return Decimal(str(value)).quantize(Q, rounding=ROUND_HALF_UP)


def _mapping_decimal(
    row: Mapping[str, Any],
    meta: Mapping[str, Any],
    key: str,
    *,
    fallback_key: str,
) -> Decimal:
    if key in row:
        return _optional_decimal(row.get(key))
    if key in meta:
        return _optional_decimal(meta.get(key))
    if fallback_key in row:
        return _optional_decimal(row.get(fallback_key))
    return _optional_decimal(meta.get(fallback_key))


def _first_mapping_value(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _trade_value(trade: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = trade.get(key)
        if value not in (None, ""):
            return value
    raw_meta = trade.get("meta")
    meta: Mapping[str, Any] = raw_meta if isinstance(raw_meta, dict) else {}
    for key in keys:
        value = meta.get(key)
        if value not in (None, ""):
            return value
    return None
