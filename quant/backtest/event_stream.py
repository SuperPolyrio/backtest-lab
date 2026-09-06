"""Typed event stream helpers for fill-first backtests.

The execution model can stay OrderFilled-first while still exposing a single
ordered stream for later multi-outcome and paper/live replay.  The helpers here
normalize persisted strategy events, order lifecycle rows, fill evidence, and
ledger cashflows into one block/timestamp ordered contract.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping, Sequence


EVENT_STREAM_SCHEMA_VERSION = "fill_first_event_stream_v1"

EVENT_PRIORITIES = {
    "MARKET": 0,
    "PRICE_BLOCK": 10,
    "SIGNAL": 20,
    "ORDER": 30,
    "RAW_TRADE": 35,
    "FILL": 40,
    "LEDGER": 50,
    "SETTLEMENT": 60,
}

CANONICAL_FILL_KEY_FIELDS = ("tx_hash", "log_index", "market_id", "condition_id", "token_id", "maker", "taker", "side")
FALLBACK_FILL_KEY_FIELDS = ("block_number", "transaction_index", "log_index", "tx_hash", "market_id", "condition_id", "token_id", "price", "size")


@dataclass(frozen=True)
class BacktestEvent:
    event_type: str
    x_axis: str
    x_value: int
    priority: int
    source: str
    market_slug: str = ""
    token_side: str = ""
    order_id: str = ""
    trade_id: str = ""
    price: str | None = None
    size: str | None = None
    cash_delta: str | None = None
    message: str = ""
    meta: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["meta"] = dict(self.meta or {})
        return row


class MarketEvent(BacktestEvent):
    pass


class PriceBlockEvent(BacktestEvent):
    pass


class SignalEvent(BacktestEvent):
    pass


class OrderEvent(BacktestEvent):
    pass


class FillEvent(BacktestEvent):
    pass


class RawTradeEvent(BacktestEvent):
    pass


class LedgerEvent(BacktestEvent):
    pass


class SettlementEvent(BacktestEvent):
    pass


def build_backtest_event_stream(
    *,
    price_points: Sequence[Any] | None = None,
    strategy_events: Sequence[Mapping[str, Any]] | None = None,
    orders: Sequence[Mapping[str, Any]] | None = None,
    raw_orderfilled_events: Sequence[Mapping[str, Any]] | None = None,
    ledger: Sequence[Mapping[str, Any]] | None = None,
    market_events: Sequence[Mapping[str, Any]] | None = None,
    market_slug: str = "",
    token_side: str = "",
) -> list[BacktestEvent]:
    events: list[BacktestEvent] = []
    for row in market_events or []:
        events.append(_market_event(row, market_slug=market_slug, token_side=token_side))
    for point in price_points or []:
        events.append(_price_block_event(point, market_slug=market_slug, token_side=token_side))
    for row in strategy_events or []:
        events.append(_signal_event(row, market_slug=market_slug, token_side=token_side))
    for row in orders or []:
        order_event = _order_event(row, market_slug=market_slug, token_side=token_side)
        events.append(order_event)
        events.extend(_raw_trade_events_from_order(row, market_slug=order_event.market_slug, token_side=order_event.token_side))
        fill_event = _fill_event(row, market_slug=order_event.market_slug, token_side=order_event.token_side)
        if fill_event is not None:
            events.append(fill_event)
    for event in raw_orderfilled_events or []:
        raw_event = _raw_trade_event(event, market_slug=market_slug, token_side=token_side)
        if raw_event is not None:
            events.append(raw_event)
    events = _enrich_raw_trade_block_context(_dedupe_raw_trade_events(events))
    for row in ledger or []:
        events.append(_ledger_event(row, market_slug=market_slug, token_side=token_side))
    return sorted(events, key=event_sort_key)


def build_event_stream_contract_report(
    *,
    price_points: Sequence[Any] | None = None,
    strategy_events: Sequence[Mapping[str, Any]] | None = None,
    orders: Sequence[Mapping[str, Any]] | None = None,
    raw_orderfilled_events: Sequence[Mapping[str, Any]] | None = None,
    ledger: Sequence[Mapping[str, Any]] | None = None,
    market_events: Sequence[Mapping[str, Any]] | None = None,
    market_slug: str = "",
    token_side: str = "",
) -> dict[str, Any]:
    events = build_backtest_event_stream(
        price_points=price_points,
        strategy_events=strategy_events,
        orders=orders,
        raw_orderfilled_events=raw_orderfilled_events,
        ledger=ledger,
        market_events=market_events,
        market_slug=market_slug,
        token_side=token_side,
    )
    type_counts: dict[str, int] = {}
    x_axes: set[str] = set()
    for event in events:
        type_counts[event.event_type] = type_counts.get(event.event_type, 0) + 1
        if event.x_axis:
            x_axes.add(event.x_axis)
    missing_contract: list[str] = []
    if orders and not type_counts.get("ORDER"):
        missing_contract.append("ORDER")
    if orders and any(_decimal(row.get("filled_size")) > 0 for row in orders) and not type_counts.get("FILL"):
        missing_contract.append("FILL")
    if ledger and not (type_counts.get("LEDGER") or type_counts.get("SETTLEMENT")):
        missing_contract.append("LEDGER")
    sorted_ok = all(event_sort_key(left) <= event_sort_key(right) for left, right in zip(events, events[1:]))
    raw_trade_tick_report = build_raw_trade_tick_report(events)
    status = "ready" if events and sorted_ok and not missing_contract else "review" if events else "missing"
    return {
        "status": status,
        "schema_version": EVENT_STREAM_SCHEMA_VERSION,
        "reason": "typed event stream can be replayed in x-order" if status == "ready" else "event stream contract incomplete",
        "event_count": len(events),
        "type_counts": type_counts,
        "x_axes": sorted(x_axes),
        "sorted": sorted_ok,
        "missing_contract": missing_contract,
        "raw_trade_tick_report": raw_trade_tick_report,
        "required_event_classes": [
            "MarketEvent",
            "PriceBlockEvent",
            "SignalEvent",
            "OrderEvent",
            "RawTradeEvent",
            "FillEvent",
            "LedgerEvent",
            "SettlementEvent",
        ],
    }


def build_joint_replay_plan_report(outcome_inputs: Sequence[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    """Build an auditable multi-outcome replay plan from typed event streams.

    This does not require LOB data.  It proves whether a batch/event run can be
    driven by one globally ordered fill-first event stream instead of by
    post-hoc summing individually completed outcome runs.
    """
    outcomes = list(outcome_inputs or [])
    if not outcomes:
        return {
            "status": "missing",
            "schema_version": EVENT_STREAM_SCHEMA_VERSION,
            "joint_replay_verdict": "missing_outcomes",
            "reason": "no outcome streams supplied",
            "outcome_count": 0,
            "event_count": 0,
            "type_counts": {},
            "x_axes": [],
            "sorted": False,
            "posthoc_sum_risk": "unknown",
            "replay_mode": "missing",
            "outcomes": [],
            "next_actions": ["Provide per-outcome price/order/fill/ledger streams before joint replay."],
        }

    merged: list[BacktestEvent] = []
    outcome_reports: list[dict[str, Any]] = []
    for index, outcome in enumerate(outcomes, start=1):
        market_slug = str(outcome.get("market_slug") or outcome.get("marketSlug") or "")
        token_side = str(outcome.get("token_side") or outcome.get("tokenSide") or "")
        outcome_key = str(outcome.get("outcome_key") or outcome.get("outcomeKey") or outcome.get("token_id") or outcome.get("tokenId") or f"outcome-{index}")
        stream = build_backtest_event_stream(
            price_points=_sequence(outcome.get("price_points") or outcome.get("pricePoints")),
            strategy_events=_sequence(outcome.get("strategy_events") or outcome.get("strategyEvents") or outcome.get("events")),
            orders=_sequence(outcome.get("orders")),
            raw_orderfilled_events=_sequence(outcome.get("raw_orderfilled_events") or outcome.get("rawOrderfilledEvents") or outcome.get("trade_ticks") or outcome.get("tradeTicks")),
            ledger=_sequence(outcome.get("ledger")),
            market_events=_sequence(outcome.get("market_events") or outcome.get("marketEvents")),
            market_slug=market_slug,
            token_side=token_side,
        )
        merged.extend(stream)
        type_counts: dict[str, int] = {}
        for event in stream:
            type_counts[event.event_type] = type_counts.get(event.event_type, 0) + 1
        outcome_reports.append(
            {
                "outcome_key": outcome_key,
                "market_slug": market_slug,
                "token_side": token_side,
                "event_count": len(stream),
                "first_x": min((event.x_value for event in stream), default=None),
                "last_x": max((event.x_value for event in stream), default=None),
                "type_counts": type_counts,
            }
        )

    ordered = sorted(merged, key=event_sort_key)
    type_counts: dict[str, int] = {}
    x_axes: set[str] = set()
    for event in ordered:
        type_counts[event.event_type] = type_counts.get(event.event_type, 0) + 1
        if event.x_axis:
            x_axes.add(event.x_axis)
    raw_trade_tick_report = build_raw_trade_tick_report(ordered)
    sorted_ok = all(event_sort_key(left) <= event_sort_key(right) for left, right in zip(ordered, ordered[1:]))
    unique_outcomes = {row["outcome_key"] for row in outcome_reports if row.get("outcome_key")}
    unique_markets = {row["market_slug"] for row in outcome_reports if row.get("market_slug")}
    has_order_contract = bool(type_counts.get("ORDER"))
    has_cashflow_contract = bool(type_counts.get("LEDGER") or type_counts.get("SETTLEMENT"))
    has_multi_outcome = len(unique_outcomes) > 1
    if has_multi_outcome and sorted_ok and has_order_contract and has_cashflow_contract:
        verdict = "joint_replay_ready"
        status = "ready"
        reason = "multiple outcome streams can be replayed in one global x-order"
        risk = "controlled"
        mode = "joint_event_stream"
        next_actions: list[str] = []
    elif sorted_ok and ordered:
        verdict = "single_outcome_only"
        status = "ready"
        reason = "single outcome stream is ordered; multi-outcome runner still needs more outcome streams"
        risk = "review_multi_outcome_batches"
        mode = "single_outcome_event_stream"
        next_actions = ["Use this contract for batch/event runners so multiple outcomes are replayed in one ordered stream."]
    else:
        verdict = "contract_incomplete"
        status = "review" if ordered else "missing"
        reason = "event stream is empty or cannot prove order/ledger contract"
        risk = "high"
        mode = "posthoc_or_incomplete"
        next_actions = ["Add ORDER plus LEDGER/SETTLEMENT events for each outcome before trusting joint replay."]

    return {
        "status": status,
        "schema_version": EVENT_STREAM_SCHEMA_VERSION,
        "joint_replay_verdict": verdict,
        "reason": reason,
        "outcome_count": len(unique_outcomes),
        "market_count": len(unique_markets),
        "event_count": len(ordered),
        "type_counts": type_counts,
        "x_axes": sorted(x_axes),
        "sorted": sorted_ok,
        "raw_trade_tick_count": raw_trade_tick_report["trade_tick_count"],
        "raw_trade_block_count": raw_trade_tick_report["block_count"],
        "raw_canonical_fill_key_coverage_pct": raw_trade_tick_report["canonical_fill_key_coverage_pct"],
        "raw_block_context_coverage_pct": raw_trade_tick_report["block_context_coverage_pct"],
        "raw_trade_tick_report": raw_trade_tick_report,
        "posthoc_sum_risk": risk,
        "replay_mode": mode,
        "outcomes": outcome_reports,
        "next_actions": next_actions,
    }


def build_joint_replay_execution_report(outcome_inputs: Sequence[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    """Replay one or more outcome streams in global x-order.

    This is still fill-first and does not require LOB.  It consumes the typed
    event stream contract directly and produces a portfolio-level state trace,
    which is the execution primitive batch/event runners should use instead of
    post-hoc summing individually completed outcome runs.
    """
    plan = build_joint_replay_plan_report(outcome_inputs)
    outcomes = list(outcome_inputs or [])
    if not outcomes:
        return {
            "status": "missing",
            "schema_version": EVENT_STREAM_SCHEMA_VERSION,
            "execution_verdict": "missing_outcomes",
            "reason": "no outcome streams supplied",
            "replay_mode": "missing",
            "plan": plan,
            "event_count": 0,
            "submitted_orders": 0,
            "fill_events": 0,
            "ledger_events": 0,
            "cash_balance": "0",
            "max_cash_at_risk": "0",
            "marked_position_value": "0",
            "portfolio_equity": "0",
            "equity_curve": [],
            "equity_curve_points": 0,
            "max_drawdown": "0",
            "max_drawdown_pct": "0",
            "max_drawdown_start_sequence": None,
            "max_drawdown_end_sequence": None,
            "raw_trade_tick_count": 0,
            "raw_trade_price_update_count": 0,
            "raw_trade_block_count": 0,
            "raw_canonical_fill_key_coverage_pct": "0",
            "raw_block_context_coverage_pct": "0",
            "raw_trade_tick_report": build_raw_trade_tick_report([]),
            "positions": [],
            "timeline": [],
            "next_actions": ["Provide per-outcome streams before joint execution replay."],
        }

    keyed_events: list[tuple[str, BacktestEvent]] = []
    outcome_labels: dict[str, dict[str, str]] = {}
    for index, outcome in enumerate(outcomes, start=1):
        outcome_key = str(outcome.get("outcome_key") or outcome.get("outcomeKey") or outcome.get("token_id") or outcome.get("tokenId") or f"outcome-{index}")
        market_slug = str(outcome.get("market_slug") or outcome.get("marketSlug") or "")
        token_side = str(outcome.get("token_side") or outcome.get("tokenSide") or "")
        event_slug = str(outcome.get("event_slug") or outcome.get("eventSlug") or "")
        stream = build_backtest_event_stream(
            price_points=_sequence(outcome.get("price_points") or outcome.get("pricePoints")),
            strategy_events=_sequence(outcome.get("strategy_events") or outcome.get("strategyEvents") or outcome.get("events")),
            orders=_sequence(outcome.get("orders")),
            raw_orderfilled_events=_sequence(outcome.get("raw_orderfilled_events") or outcome.get("rawOrderfilledEvents") or outcome.get("trade_ticks") or outcome.get("tradeTicks")),
            ledger=_sequence(outcome.get("ledger")),
            market_events=_sequence(outcome.get("market_events") or outcome.get("marketEvents")),
            market_slug=market_slug,
            token_side=token_side,
        )
        keyed_events.extend((outcome_key, event) for event in stream)
        outcome_labels[outcome_key] = {"market_slug": market_slug, "token_side": token_side, "event_slug": event_slug}

    ordered = sorted(keyed_events, key=lambda item: event_sort_key(item[1]))
    raw_trade_tick_report = build_raw_trade_tick_report([event for _, event in ordered])
    cash = Decimal("0")
    max_cash_at_risk = Decimal("0")
    positions: dict[str, Decimal] = {}
    latest_prices: dict[str, Decimal] = {}
    outcome_cashflows: dict[str, Decimal] = {}
    order_to_outcome: dict[str, str] = {}
    submitted_orders = 0
    fill_events = 0
    ledger_events = 0
    raw_trade_price_updates = 0
    timeline: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    equity_peak: Decimal | None = None
    equity_peak_sequence: int | None = None
    max_drawdown = Decimal("0")
    max_drawdown_pct = Decimal("0")
    max_drawdown_start_sequence: int | None = None
    max_drawdown_end_sequence: int | None = None

    def append_portfolio_point(sequence: int, outcome_key: str, event: BacktestEvent, *, record_equity_curve: bool = True) -> None:
        nonlocal equity_peak, equity_peak_sequence
        nonlocal max_drawdown, max_drawdown_pct, max_drawdown_start_sequence, max_drawdown_end_sequence

        step_marked_value = sum(
            positions.get(key, Decimal("0")) * latest_prices.get(key, Decimal("0"))
            for key in positions
        )
        step_equity = cash + step_marked_value
        if record_equity_curve:
            if equity_peak is None or step_equity > equity_peak:
                equity_peak = step_equity
                equity_peak_sequence = sequence

            peak = equity_peak if equity_peak is not None else step_equity
            drawdown = max(Decimal("0"), peak - step_equity)
            drawdown_pct = Decimal("0") if peak == 0 else drawdown / abs(peak) * Decimal("100")
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                max_drawdown_pct = drawdown_pct
                max_drawdown_start_sequence = equity_peak_sequence
                max_drawdown_end_sequence = sequence
        else:
            peak = equity_peak if equity_peak is not None else step_equity
            drawdown = max(Decimal("0"), peak - step_equity)
            drawdown_pct = Decimal("0") if peak == 0 else drawdown / abs(peak) * Decimal("100")

        row = {
            "sequence": sequence,
            "x_value": event.x_value,
            "event_type": event.event_type,
            "outcome_key": outcome_key,
            "order_id": event.order_id,
            "trade_id": event.trade_id,
            "cash_delta": event.cash_delta,
            "cash_balance": _decimal_string(cash),
            "position": _decimal_string(positions.get(outcome_key, Decimal("0"))),
            "marked_position_value": _decimal_string(step_marked_value),
            "portfolio_equity": _decimal_string(step_equity),
            "equity_peak": _decimal_string(peak),
            "drawdown": _decimal_string(drawdown),
            "drawdown_pct": _decimal_string(drawdown_pct),
            "max_cash_at_risk": _decimal_string(max_cash_at_risk),
        }
        timeline.append(row)
        if not record_equity_curve:
            return
        equity_curve.append(
            {
                "point_index": len(equity_curve) + 1,
                "sequence": sequence,
                "x_axis": event.x_axis,
                "x_value": event.x_value,
                "event_type": event.event_type,
                "outcome_key": outcome_key,
                "cash_balance": row["cash_balance"],
                "marked_position_value": row["marked_position_value"],
                "portfolio_equity": row["portfolio_equity"],
                "equity_peak": row["equity_peak"],
                "drawdown": row["drawdown"],
                "drawdown_pct": row["drawdown_pct"],
            }
        )

    for sequence, (outcome_key, event) in enumerate(ordered, start=1):
        if event.event_type == "PRICE_BLOCK" and event.price is not None:
            latest_prices[outcome_key] = _decimal(event.price)
        elif event.event_type == "RAW_TRADE" and event.price is not None:
            latest_prices[outcome_key] = _decimal(event.price)
            raw_trade_price_updates += 1
        elif event.event_type == "ORDER":
            submitted_orders += 1
            if event.order_id:
                order_to_outcome[event.order_id] = outcome_key
        elif event.event_type == "FILL":
            fill_events += 1
            target_key = order_to_outcome.get(event.order_id, outcome_key)
            positions[target_key] = positions.get(target_key, Decimal("0")) + _decimal(event.size)
        elif event.event_type in {"LEDGER", "SETTLEMENT"}:
            ledger_events += 1
            cash += _decimal(event.cash_delta)
            outcome_cashflows[outcome_key] = outcome_cashflows.get(outcome_key, Decimal("0")) + _decimal(event.cash_delta)
            if event.event_type == "SETTLEMENT" and event.size is not None:
                target_key = order_to_outcome.get(event.order_id, outcome_key)
                positions[target_key] = positions.get(target_key, Decimal("0")) + _decimal(event.size)
            if cash < 0:
                max_cash_at_risk = max(max_cash_at_risk, -cash)

        if event.event_type in {"ORDER", "FILL"}:
            append_portfolio_point(sequence, outcome_key, event, record_equity_curve=False)
        elif event.event_type in {"LEDGER", "SETTLEMENT"}:
            append_portfolio_point(sequence, outcome_key, event)
        elif event.event_type in {"PRICE_BLOCK", "RAW_TRADE"} and positions:
            append_portfolio_point(sequence, outcome_key, event)

    marked_position_value = Decimal("0")
    position_rows: list[dict[str, Any]] = []
    for outcome_key in sorted(set(outcome_labels) | set(positions)):
        size = positions.get(outcome_key, Decimal("0"))
        mark_price = latest_prices.get(outcome_key, Decimal("0"))
        mark_value = size * mark_price
        marked_position_value += mark_value
        labels = outcome_labels.get(outcome_key, {})
        position_rows.append(
            {
                "outcome_key": outcome_key,
                "market_slug": labels.get("market_slug", ""),
                "token_side": labels.get("token_side", ""),
                "position_size": _decimal_string(size),
                "mark_price": _decimal_string(mark_price),
                "marked_value": _decimal_string(mark_value),
            }
        )

    portfolio_equity = cash + marked_position_value
    event_probability_report = _joint_event_probability_report(outcome_labels, latest_prices)
    yes_no_complement_report = _joint_yes_no_complement_report(outcome_labels, latest_prices)
    event_exposure_report = _joint_event_exposure_report(outcome_labels, positions, latest_prices, outcome_cashflows)
    if plan["joint_replay_verdict"] == "joint_replay_ready":
        verdict = "joint_execution_ready"
        mode = "joint_event_stream_execution"
        status = "ready"
        reason = "portfolio state was replayed by one global x-order event stream"
        next_actions: list[str] = []
    elif plan["status"] == "ready":
        verdict = "single_outcome_execution_ready"
        mode = "single_outcome_event_stream_execution"
        status = "ready"
        reason = "single outcome state was replayed by the same event-stream execution primitive"
        next_actions = ["Route batch/event runners through this primitive with multiple outcome streams."]
    else:
        verdict = "contract_incomplete"
        mode = "posthoc_or_incomplete"
        status = "review" if ordered else "missing"
        reason = "joint execution replay cannot be trusted until the event stream contract is complete"
        next_actions = ["Provide ordered ORDER/FILL/LEDGER events before executing joint replay."]

    return {
        "status": status,
        "schema_version": EVENT_STREAM_SCHEMA_VERSION,
        "execution_verdict": verdict,
        "reason": reason,
        "replay_mode": mode,
        "plan": plan,
        "event_count": len(ordered),
        "submitted_orders": submitted_orders,
        "fill_events": fill_events,
        "ledger_events": ledger_events,
        "raw_trade_price_update_count": raw_trade_price_updates,
        "cash_balance": _decimal_string(cash),
        "max_cash_at_risk": _decimal_string(max_cash_at_risk),
        "marked_position_value": _decimal_string(marked_position_value),
        "portfolio_equity": _decimal_string(portfolio_equity),
        "equity_curve": equity_curve,
        "equity_curve_points": len(equity_curve),
        "max_drawdown": _decimal_string(max_drawdown),
        "max_drawdown_pct": _decimal_string(max_drawdown_pct),
        "max_drawdown_start_sequence": max_drawdown_start_sequence,
        "max_drawdown_end_sequence": max_drawdown_end_sequence,
        "raw_trade_tick_count": raw_trade_tick_report["trade_tick_count"],
        "raw_trade_block_count": raw_trade_tick_report["block_count"],
        "raw_canonical_fill_key_coverage_pct": raw_trade_tick_report["canonical_fill_key_coverage_pct"],
        "raw_block_context_coverage_pct": raw_trade_tick_report["block_context_coverage_pct"],
        "raw_trade_tick_report": raw_trade_tick_report,
        "event_probability_report": event_probability_report,
        "yes_no_complement_report": yes_no_complement_report,
        "event_exposure_report": event_exposure_report,
        "positions": position_rows,
        "timeline": timeline,
        "timeline_truncated": False,
        "posthoc_sum_replaced": mode in {"joint_event_stream_execution", "single_outcome_event_stream_execution"},
        "next_actions": next_actions,
    }


def _joint_event_probability_report(outcome_labels: Mapping[str, Mapping[str, str]], latest_prices: Mapping[str, Decimal]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    probability_sum = Decimal("0")
    missing_count = 0
    for outcome_key in sorted(outcome_labels):
        label = outcome_labels.get(outcome_key, {})
        price = latest_prices.get(outcome_key)
        if price is None:
            missing_count += 1
            price_text = None
        else:
            probability_sum += price
            price_text = _decimal_string(price.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP))
        rows.append(
            {
                "outcome_key": outcome_key,
                "market_slug": label.get("market_slug", ""),
                "token_side": label.get("token_side", ""),
                "latest_probability": price_text,
            }
        )
    probability_gap = probability_sum - Decimal("1")
    has_multi_outcome = len(outcome_labels) > 1
    within_tolerance = abs(probability_gap) <= Decimal("0.05")
    status = "ready" if has_multi_outcome and missing_count == 0 and within_tolerance else "review"
    if not has_multi_outcome:
        reason = "single outcome cannot prove event-level probability sum"
    elif missing_count:
        reason = "missing latest probability for one or more outcomes"
    elif within_tolerance:
        reason = "latest outcome probabilities sum within tolerance"
    else:
        reason = "latest outcome probabilities do not sum near one"
    return {
        "status": status,
        "reason": reason,
        "outcome_count": len(outcome_labels),
        "missing_count": missing_count,
        "probability_sum": _decimal_string(probability_sum.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "probability_gap": _decimal_string(probability_gap.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "tolerance": "0.05",
        "rows": rows,
    }


def _joint_yes_no_complement_report(outcome_labels: Mapping[str, Mapping[str, str]], latest_prices: Mapping[str, Decimal]) -> dict[str, Any]:
    grouped: dict[str, dict[str, tuple[str, Decimal]]] = {}
    for outcome_key, label in outcome_labels.items():
        market_slug = str(label.get("market_slug") or "")
        token_side = str(label.get("token_side") or "").upper()
        price = latest_prices.get(outcome_key)
        if not market_slug or token_side not in {"YES", "NO"} or price is None:
            continue
        grouped.setdefault(market_slug, {})[token_side] = (outcome_key, price)

    rows: list[dict[str, Any]] = []
    max_deviation = Decimal("0")
    for market_slug in sorted(grouped):
        pair = grouped[market_slug]
        if "YES" not in pair or "NO" not in pair:
            continue
        yes_key, yes_price = pair["YES"]
        no_key, no_price = pair["NO"]
        total = yes_price + no_price
        deviation = abs(total - Decimal("1"))
        max_deviation = max(max_deviation, deviation)
        rows.append(
            {
                "market_slug": market_slug,
                "yes_outcome_key": yes_key,
                "no_outcome_key": no_key,
                "yes_probability": _decimal_string(yes_price.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "no_probability": _decimal_string(no_price.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "sum": _decimal_string(total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "deviation": _decimal_string(deviation.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
            }
        )
    status = "ready" if rows and max_deviation <= Decimal("0.02") else "review"
    if not rows:
        reason = "no YES/NO complement pairs available in joint replay"
    elif status == "ready":
        reason = "YES/NO complement pairs are within tolerance"
    else:
        reason = "YES/NO complement deviation exceeds tolerance"
    return {
        "status": status,
        "reason": reason,
        "checked_count": len(rows),
        "bad_count": sum(1 for row in rows if _decimal(row.get("deviation")) > Decimal("0.02")),
        "max_deviation": _decimal_string(max_deviation.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "tolerance": "0.02",
        "rows": rows,
    }


def _joint_event_exposure_report(
    outcome_labels: Mapping[str, Mapping[str, str]],
    positions: Mapping[str, Decimal],
    latest_prices: Mapping[str, Decimal],
    outcome_cashflows: Mapping[str, Decimal],
) -> dict[str, Any]:
    """Summarize mutually-exclusive event exposure from current outcome state."""

    grouped: dict[str, dict[str, Any]] = {}
    for outcome_key in sorted(set(outcome_labels) | set(positions) | set(outcome_cashflows)):
        label = outcome_labels.get(outcome_key, {})
        event_key = str(label.get("event_slug") or "").strip()
        if not event_key:
            event_key = "__joint_event__" if len(outcome_labels) > 1 else str(label.get("market_slug") or outcome_key)
        position = _decimal(positions.get(outcome_key))
        mark_price = _decimal(latest_prices.get(outcome_key))
        marked_value = (position * mark_price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        net_cashflow = _decimal(outcome_cashflows.get(outcome_key)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        cash_at_risk = max(Decimal("0"), -net_cashflow).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        state = grouped.setdefault(
            event_key,
            {
                "event_key": event_key,
                "outcomes": [],
                "gross_position_size": Decimal("0"),
                "gross_marked_exposure": Decimal("0"),
                "gross_cash_at_risk": Decimal("0"),
                "net_cashflow": Decimal("0"),
                "max_single_winner_marked_value": Decimal("0"),
            },
        )
        state["gross_position_size"] += abs(position)
        state["gross_marked_exposure"] += abs(marked_value)
        state["gross_cash_at_risk"] += cash_at_risk
        state["net_cashflow"] += net_cashflow
        state["max_single_winner_marked_value"] = max(state["max_single_winner_marked_value"], marked_value)
        state["outcomes"].append(
            {
                "outcome_key": outcome_key,
                "market_slug": label.get("market_slug", ""),
                "token_side": label.get("token_side", ""),
                "position_size": _decimal_string(position.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "mark_price": _decimal_string(mark_price.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "marked_value": _decimal_string(marked_value),
                "net_cashflow": _decimal_string(net_cashflow),
                "cash_at_risk": _decimal_string(cash_at_risk),
            }
        )

    rows: list[dict[str, Any]] = []
    total_gross_cash_at_risk = Decimal("0")
    worst_case_cash_loss = Decimal("0")
    for event_key in sorted(grouped):
        state = grouped[event_key]
        gross_cash_at_risk = state["gross_cash_at_risk"].quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        max_winner_value = state["max_single_winner_marked_value"].quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        event_worst_loss = max(Decimal("0"), gross_cash_at_risk - max_winner_value).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        total_gross_cash_at_risk += gross_cash_at_risk
        worst_case_cash_loss = max(worst_case_cash_loss, event_worst_loss)
        rows.append(
            {
                "event_key": event_key,
                "outcome_count": len(state["outcomes"]),
                "gross_position_size": _decimal_string(state["gross_position_size"].quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "gross_marked_exposure": _decimal_string(state["gross_marked_exposure"].quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "gross_cash_at_risk": _decimal_string(gross_cash_at_risk),
                "net_cashflow": _decimal_string(state["net_cashflow"].quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "max_single_winner_marked_value": _decimal_string(max_winner_value),
                "worst_case_cash_loss": _decimal_string(event_worst_loss),
                "outcomes": state["outcomes"],
            }
        )

    has_multi_outcome_event = any(int(row["outcome_count"]) > 1 for row in rows)
    status = "ready" if rows and has_multi_outcome_event else "review" if rows else "missing"
    reason = (
        "multi-outcome event exposure is available"
        if status == "ready"
        else "only single-outcome exposure is available" if rows else "no positions or cashflows available for event exposure"
    )
    return {
        "status": status,
        "reason": reason,
        "event_count": len(rows),
        "multi_outcome_event_count": sum(1 for row in rows if int(row["outcome_count"]) > 1),
        "total_gross_cash_at_risk": _decimal_string(total_gross_cash_at_risk.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "worst_case_cash_loss": _decimal_string(worst_case_cash_loss.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "rows": rows,
    }


def event_sort_key(event: BacktestEvent) -> tuple[int, int, int, int, str, str, str, str, str, str]:
    meta = dict(event.meta or {})
    return (
        int(event.x_value),
        int(event.priority),
        _sort_int(meta.get("transaction_index") or meta.get("transactionIndex")),
        _sort_int(meta.get("log_index") or meta.get("logIndex")),
        str(meta.get("tx_hash") or meta.get("txHash") or ""),
        str(meta.get("canonical_fill_key") or meta.get("canonicalFillKey") or ""),
        str(event.market_slug),
        str(event.order_id),
        str(event.trade_id),
        str(event.event_type),
    )


def event_stream_to_dicts(events: Iterable[BacktestEvent]) -> list[dict[str, Any]]:
    return [event.as_dict() for event in events]


def build_raw_trade_tick_report(events: Sequence[BacktestEvent], *, max_order_sequence: int = 200) -> dict[str, Any]:
    raw_events = sorted([event for event in events if event.event_type == "RAW_TRADE"], key=event_sort_key)
    if not raw_events:
        return {
            "status": "missing",
            "reason": "no raw OrderFilled trade ticks available",
            "trade_tick_count": 0,
            "block_count": 0,
            "sorted": True,
            "canonical_fill_key_count": 0,
            "fallback_fill_key_count": 0,
            "canonical_fill_key_coverage_pct": "0",
            "maker_taker_side_attributed_count": 0,
            "maker_taker_side_missing_count": 0,
            "maker_taker_side_coverage_pct": "0",
            "block_context_event_count": 0,
            "block_context_coverage_pct": "0",
            "canonical_key_kind_counts": {},
            "block_identity_fields": ["market_slug", "token_side", "market_id", "condition_id", "token_id", "block_number"],
            "blocks": [],
        }

    sorted_ok = all(event_sort_key(left) <= event_sort_key(right) for left, right in zip(raw_events, raw_events[1:]))
    attributed = [
        event
        for event in raw_events
        if (event.meta or {}).get("maker") and (event.meta or {}).get("taker") and (event.meta or {}).get("side")
    ]
    key_kind_counts: dict[str, int] = {}
    for event in raw_events:
        kind = str((event.meta or {}).get("canonical_fill_key_kind") or "unknown")
        key_kind_counts[kind] = key_kind_counts.get(kind, 0) + 1
    block_context_events = [
        event
        for event in raw_events
        if (event.meta or {}).get("block_trade_index") and (event.meta or {}).get("block_vwap_price") is not None
    ]
    canonical_count = key_kind_counts.get("canonical", 0)
    fallback_count = key_kind_counts.get("fallback", 0)
    blocks: dict[tuple[str, str, str, str, str, int], list[BacktestEvent]] = {}
    for event in raw_events:
        blocks.setdefault(_raw_trade_block_report_key(event), []).append(event)

    block_rows: list[dict[str, Any]] = []
    for block_key in sorted(blocks, key=_raw_trade_block_report_sort_key):
        market_slug, token_side, market_id, condition_id, token_id, block_number = block_key
        block_events = sorted(blocks[block_key], key=event_sort_key)
        priced = [(event, _decimal(event.price), _decimal(event.size)) for event in block_events if event.price is not None]
        volume = sum((max(Decimal("0"), size) for _, _, size in priced), Decimal("0"))
        notional = sum((max(Decimal("0"), price) * max(Decimal("0"), size) for _, price, size in priced), Decimal("0"))
        prices = [price for _, price, _ in priced]
        vwap = (notional / volume).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if volume > 0 else Decimal("0")
        buy_volume = Decimal("0")
        sell_volume = Decimal("0")
        unknown_side_volume = Decimal("0")
        buy_notional = Decimal("0")
        sell_notional = Decimal("0")
        unknown_side_notional = Decimal("0")
        for event, price, size in priced:
            safe_size = max(Decimal("0"), size)
            safe_notional = max(Decimal("0"), price) * safe_size
            side = str((event.meta or {}).get("side") or "").upper()
            if side == "BUY":
                buy_volume += safe_size
                buy_notional += safe_notional
            elif side == "SELL":
                sell_volume += safe_size
                sell_notional += safe_notional
            else:
                unknown_side_volume += safe_size
                unknown_side_notional += safe_notional
        sequence_rows: list[dict[str, Any]] = []
        for sequence, event in enumerate(block_events[:max_order_sequence], start=1):
            meta = dict(event.meta or {})
            sequence_rows.append(
                {
                    "sequence": sequence,
                    "canonical_fill_key": meta.get("canonical_fill_key") or "",
                    "transaction_index": _int(meta.get("transaction_index")),
                    "log_index": _int(meta.get("log_index")),
                    "tx_hash": meta.get("tx_hash") or "",
                    "market_slug": event.market_slug,
                    "token_side": event.token_side,
                    "condition_id": meta.get("condition_id"),
                    "token_id": meta.get("token_id"),
                    "order_id": event.order_id,
                    "trade_id": event.trade_id,
                    "price": event.price,
                    "size": event.size,
                    "maker": meta.get("maker"),
                    "taker": meta.get("taker"),
                    "side": meta.get("side"),
                    "evidence_role": meta.get("evidence_role"),
                }
            )
        block_rows.append(
            {
                "market_slug": market_slug,
                "token_side": token_side,
                "market_id": market_id,
                "condition_id": condition_id,
                "token_id": token_id,
                "block_number": block_number,
                "trade_tick_count": len(block_events),
                "volume": _decimal_string(volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "notional": _decimal_string(notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "buy_volume": _decimal_string(buy_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "sell_volume": _decimal_string(sell_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "unknown_side_volume": _decimal_string(unknown_side_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "buy_notional": _decimal_string(buy_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "sell_notional": _decimal_string(sell_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "unknown_side_notional": _decimal_string(unknown_side_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "open": _decimal_string(prices[0].quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)) if prices else None,
                "high": _decimal_string(max(prices).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)) if prices else None,
                "low": _decimal_string(min(prices).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)) if prices else None,
                "close": _decimal_string(prices[-1].quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)) if prices else None,
                "vwap": _decimal_string(vwap),
                "first_sequence": _event_sequence_dict(block_events[0]),
                "last_sequence": _event_sequence_dict(block_events[-1]),
                "order_sequence": sequence_rows,
                "order_sequence_truncated": len(block_events) > max_order_sequence,
            }
        )

    coverage_pct = (_decimal(len(attributed)) * Decimal("100") / _decimal(len(raw_events))).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    canonical_coverage_pct = (_decimal(canonical_count) * Decimal("100") / _decimal(len(raw_events))).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    block_context_coverage_pct = (_decimal(len(block_context_events)) * Decimal("100") / _decimal(len(raw_events))).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    attribution_missing_count = len(raw_events) - len(attributed)
    return {
        "status": "ready" if sorted_ok else "review",
        "reason": "raw OrderFilled trade ticks are block ordered with OHLCV/VWAP summaries" if sorted_ok else "raw trade ticks are not sorted",
        "trade_tick_count": len(raw_events),
        "block_count": len(block_rows),
        "sorted": sorted_ok,
        "canonical_key_fields": list(CANONICAL_FILL_KEY_FIELDS),
        "fallback_key_fields": list(FALLBACK_FILL_KEY_FIELDS),
        "block_identity_fields": ["market_slug", "token_side", "market_id", "condition_id", "token_id", "block_number"],
        "canonical_key_kind_counts": key_kind_counts,
        "canonical_fill_key_count": canonical_count,
        "fallback_fill_key_count": fallback_count,
        "canonical_fill_key_coverage_pct": _decimal_string(canonical_coverage_pct),
        "maker_taker_side_attributed_count": len(attributed),
        "maker_taker_side_missing_count": attribution_missing_count,
        "maker_taker_side_coverage_pct": _decimal_string(coverage_pct),
        "block_context_event_count": len(block_context_events),
        "block_context_coverage_pct": _decimal_string(block_context_coverage_pct),
        "blocks": block_rows,
    }


def _market_event(row: Mapping[str, Any], *, market_slug: str, token_side: str) -> MarketEvent:
    return MarketEvent(
        event_type="MARKET",
        x_axis=str(row.get("x_axis") or row.get("xAxis") or "block_number"),
        x_value=_int(row.get("x_value") or row.get("xValue") or row.get("block_number") or row.get("blockNumber")),
        priority=EVENT_PRIORITIES["MARKET"],
        source=str(row.get("source") or "market_event"),
        market_slug=str(row.get("market_slug") or row.get("marketSlug") or market_slug),
        token_side=str(row.get("token_side") or row.get("tokenSide") or token_side),
        message=str(row.get("message") or row.get("event_type") or row.get("eventType") or "market event"),
        meta=dict(row.get("meta") or {}),
    )


def _price_block_event(point: Any, *, market_slug: str, token_side: str) -> PriceBlockEvent:
    x_value = _value(point, "x_value", "xValue", "block_number", "blockNumber", "timestamp")
    return PriceBlockEvent(
        event_type="PRICE_BLOCK",
        x_axis=str(_value(point, "x_axis", "xAxis") or "block_number"),
        x_value=_int(x_value),
        priority=EVENT_PRIORITIES["PRICE_BLOCK"],
        source=str(_value(point, "source") or "market_token_block_close"),
        market_slug=str(_value(point, "market_slug", "marketSlug") or market_slug),
        token_side=str(_value(point, "token_side", "tokenSide") or token_side),
        price=_optional_string(_value(point, "price", "close", "yes_probability_close", "yesProbabilityClose")),
        size=_optional_string(_value(point, "volume")),
        message="block-level price event",
        meta={},
    )


def _signal_event(row: Mapping[str, Any], *, market_slug: str, token_side: str) -> SignalEvent:
    return SignalEvent(
        event_type="SIGNAL",
        x_axis=str(row.get("x_axis") or row.get("xAxis") or "block_number"),
        x_value=_int(row.get("x_value") or row.get("xValue")),
        priority=EVENT_PRIORITIES["SIGNAL"],
        source="strategy_event",
        market_slug=str(row.get("market_slug") or row.get("marketSlug") or market_slug),
        token_side=str(row.get("token_side") or row.get("tokenSide") or token_side),
        trade_id=str(row.get("trade_id") or row.get("tradeId") or ""),
        price=_optional_string(row.get("price")),
        message=str(row.get("message") or row.get("event_type") or row.get("eventType") or "strategy signal"),
        meta=dict(row.get("meta") or {}),
    )


def _order_event(row: Mapping[str, Any], *, market_slug: str, token_side: str) -> OrderEvent:
    meta = dict(row.get("meta") or {})
    return OrderEvent(
        event_type="ORDER",
        x_axis=str(row.get("x_axis") or row.get("xAxis") or "block_number"),
        x_value=_int(row.get("submit_x") or row.get("submitX") or row.get("signal_x") or row.get("signalX")),
        priority=EVENT_PRIORITIES["ORDER"],
        source=str(row.get("execution_source") or row.get("executionSource") or "order_lifecycle"),
        market_slug=str(row.get("market_slug") or row.get("marketSlug") or meta.get("market_slug") or meta.get("marketSlug") or market_slug),
        token_side=str(row.get("token_side") or row.get("tokenSide") or meta.get("token_side") or meta.get("tokenSide") or token_side),
        order_id=str(row.get("order_id") or row.get("orderId") or ""),
        trade_id=str(row.get("trade_id") or row.get("tradeId") or ""),
        price=_optional_string(row.get("requested_price") or row.get("requestedPrice") or row.get("decision_price") or row.get("decisionPrice")),
        size=_optional_string(row.get("requested_size") or row.get("requestedSize")),
        message=str(row.get("status") or "order submitted"),
        meta={**meta, "status": row.get("status"), "role": row.get("role"), "side": row.get("side")},
    )


def _fill_event(row: Mapping[str, Any], *, market_slug: str, token_side: str) -> FillEvent | None:
    filled_size = _decimal(row.get("filled_size") or row.get("filledSize"))
    if filled_size <= 0:
        return None
    meta = dict(row.get("meta") or {})
    return FillEvent(
        event_type="FILL",
        x_axis=str(row.get("x_axis") or row.get("xAxis") or "block_number"),
        x_value=_int(row.get("submit_x") or row.get("submitX") or row.get("signal_x") or row.get("signalX")),
        priority=EVENT_PRIORITIES["FILL"],
        source=str(row.get("execution_source") or row.get("executionSource") or "orderfilled_fact"),
        market_slug=str(row.get("market_slug") or row.get("marketSlug") or meta.get("market_slug") or meta.get("marketSlug") or market_slug),
        token_side=str(row.get("token_side") or row.get("tokenSide") or meta.get("token_side") or meta.get("tokenSide") or token_side),
        order_id=str(row.get("order_id") or row.get("orderId") or ""),
        trade_id=str(row.get("trade_id") or row.get("tradeId") or ""),
        price=_optional_string(row.get("avg_fill_price") or row.get("avgFillPrice") or row.get("requested_price") or row.get("requestedPrice")),
        size=_optional_string(filled_size),
        message="order filled from OrderFilled-calibrated evidence",
        meta=meta,
    )


def _raw_trade_events_from_order(row: Mapping[str, Any], *, market_slug: str, token_side: str) -> list[RawTradeEvent]:
    meta = dict(row.get("meta") or {})
    rows: list[RawTradeEvent] = []
    order_id = str(row.get("order_id") or row.get("orderId") or "")
    trade_id = str(row.get("trade_id") or row.get("tradeId") or "")
    for evidence_role, key in (("consumed", "consumed_events"), ("candidate", "candidate_events")):
        events = meta.get(key)
        if not isinstance(events, Sequence) or isinstance(events, (str, bytes, bytearray)):
            continue
        for event in events:
            if not isinstance(event, Mapping):
                continue
            raw_event = _raw_trade_event(
                {**dict(event), "evidence_role": evidence_role, "order_id": order_id, "trade_id": trade_id},
                market_slug=market_slug,
                token_side=token_side,
            )
            if raw_event is not None:
                rows.append(raw_event)
    return rows


def _raw_trade_event(row: Mapping[str, Any], *, market_slug: str, token_side: str) -> RawTradeEvent | None:
    block_number = _value(row, "block_number", "blockNumber", "x_value", "xValue")
    if block_number in (None, ""):
        return None
    maker = _normalize_address(_value(row, "maker", "maker_address", "makerAddress"))
    taker = _normalize_address(_value(row, "taker", "taker_address", "takerAddress"))
    side = _normalize_side(_value(row, "side_code", "sideCode", "side", "taker_side", "takerSide"))
    key, key_kind = canonical_fill_key_parts(row)
    meta = {
        "canonical_fill_key": key,
        "canonical_fill_key_kind": key_kind,
        "market_id": _optional_string(_value(row, "market_id", "marketId")),
        "condition_id": _optional_string(_value(row, "condition_id", "conditionId")),
        "token_id": _optional_string(_value(row, "token_id", "tokenId")),
        "tx_hash": _normalize_tx_hash(_value(row, "tx_hash", "txHash")),
        "transaction_index": _int(_value(row, "transaction_index", "transactionIndex")),
        "log_index": _int(_value(row, "log_index", "logIndex")),
        "maker": maker,
        "taker": taker,
        "side": side,
        "maker_side": _maker_side_from_taker_side(side),
        "taker_side": side,
        "maker_amount": _optional_string(_value(row, "maker_amount", "makerAmount")),
        "taker_amount": _optional_string(_value(row, "taker_amount", "takerAmount")),
        "evidence_role": _optional_string(_value(row, "evidence_role", "evidenceRole")) or "raw",
    }
    return RawTradeEvent(
        event_type="RAW_TRADE",
        x_axis=str(_value(row, "x_axis", "xAxis") or "block_number"),
        x_value=_int(block_number),
        priority=EVENT_PRIORITIES["RAW_TRADE"],
        source=str(_value(row, "source") or "orderfilled_fact"),
        market_slug=str(_value(row, "market_slug", "marketSlug") or market_slug),
        token_side=str(_value(row, "token_side", "tokenSide") or token_side),
        order_id=str(_value(row, "order_id", "orderId") or ""),
        trade_id=str(_value(row, "trade_id", "tradeId") or ""),
        price=_optional_string(_value(row, "trade_price", "tradePrice", "price", "avg_fill_price", "avgFillPrice")),
        size=_optional_string(_value(row, "size", "amount", "matched_size", "matchedSize")),
        message="raw OrderFilled trade tick",
        meta=meta,
    )


def _dedupe_raw_trade_events(events: Sequence[BacktestEvent]) -> list[BacktestEvent]:
    seen: set[str] = set()
    deduped: list[BacktestEvent] = []
    for event in sorted(events, key=event_sort_key):
        if event.event_type != "RAW_TRADE":
            deduped.append(event)
            continue
        key = str((event.meta or {}).get("canonical_fill_key") or "")
        key_kind = str((event.meta or {}).get("canonical_fill_key_kind") or "")
        if key_kind != "canonical":
            deduped.append(event)
            continue
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        deduped.append(event)
    return deduped


def _enrich_raw_trade_block_context(events: Sequence[BacktestEvent]) -> list[BacktestEvent]:
    raw_events = [event for event in events if event.event_type == "RAW_TRADE"]
    if not raw_events:
        return list(events)

    block_groups: dict[tuple[str, str, str, str, str, int], list[BacktestEvent]] = {}
    for event in raw_events:
        meta = dict(event.meta or {})
        group_key = (
            str(event.market_slug),
            str(event.token_side),
            str(meta.get("market_id") or ""),
            str(meta.get("condition_id") or ""),
            str(meta.get("token_id") or ""),
            int(event.x_value),
        )
        block_groups.setdefault(group_key, []).append(event)

    enriched_by_identity: dict[tuple[Any, ...], BacktestEvent] = {}
    for group_events in block_groups.values():
        ordered = sorted(group_events, key=event_sort_key)
        priced = [(event, _decimal(event.price), _decimal(event.size)) for event in ordered if event.price is not None]
        volume = sum((max(Decimal("0"), size) for _, _, size in priced), Decimal("0"))
        notional = sum((max(Decimal("0"), price) * max(Decimal("0"), size) for _, price, size in priced), Decimal("0"))
        prices = [price for _, price, _ in priced]
        vwap = (notional / volume).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if volume > 0 else Decimal("0")
        buy_volume = Decimal("0")
        sell_volume = Decimal("0")
        unknown_side_volume = Decimal("0")
        buy_notional = Decimal("0")
        sell_notional = Decimal("0")
        unknown_side_notional = Decimal("0")
        for event, price, size in priced:
            safe_size = max(Decimal("0"), size)
            safe_notional = max(Decimal("0"), price) * safe_size
            side = str((event.meta or {}).get("side") or "").upper()
            if side == "BUY":
                buy_volume += safe_size
                buy_notional += safe_notional
            elif side == "SELL":
                sell_volume += safe_size
                sell_notional += safe_notional
            else:
                unknown_side_volume += safe_size
                unknown_side_notional += safe_notional
        open_price = prices[0].quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if prices else None
        high_price = max(prices).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if prices else None
        low_price = min(prices).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if prices else None
        close_price = prices[-1].quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if prices else None
        first_sequence = _event_sequence_dict(ordered[0])
        last_sequence = _event_sequence_dict(ordered[-1])
        for block_trade_index, event in enumerate(ordered, start=1):
            meta = dict(event.meta or {})
            enriched_meta = {
                **meta,
                "block_trade_index": block_trade_index,
                "block_trade_count": len(ordered),
                "block_open_price": _decimal_string(open_price) if open_price is not None else None,
                "block_high_price": _decimal_string(high_price) if high_price is not None else None,
                "block_low_price": _decimal_string(low_price) if low_price is not None else None,
                "block_close_price": _decimal_string(close_price) if close_price is not None else None,
                "block_vwap_price": _decimal_string(vwap),
                "block_volume": _decimal_string(volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "block_notional": _decimal_string(notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "block_buy_volume": _decimal_string(buy_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "block_sell_volume": _decimal_string(sell_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "block_unknown_side_volume": _decimal_string(unknown_side_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "block_buy_notional": _decimal_string(buy_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "block_sell_notional": _decimal_string(sell_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "block_unknown_side_notional": _decimal_string(unknown_side_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
                "block_first_sequence": first_sequence,
                "block_last_sequence": last_sequence,
            }
            enriched_by_identity[_event_identity(event)] = replace(event, meta=enriched_meta)

    return [enriched_by_identity.get(_event_identity(event), event) for event in events]


def _event_identity(event: BacktestEvent) -> tuple[Any, ...]:
    meta = dict(event.meta or {})
    return (
        event.event_type,
        event.x_axis,
        int(event.x_value),
        event.priority,
        event.source,
        event.market_slug,
        event.token_side,
        event.order_id,
        event.trade_id,
        event.price,
        event.size,
        meta.get("canonical_fill_key"),
        meta.get("transaction_index"),
        meta.get("log_index"),
        meta.get("tx_hash"),
        meta.get("evidence_role"),
    )


def _event_sequence_dict(event: BacktestEvent) -> dict[str, Any]:
    meta = dict(event.meta or {})
    return {
        "block_number": int(event.x_value),
        "transaction_index": _int(meta.get("transaction_index")),
        "log_index": _int(meta.get("log_index")),
        "tx_hash": meta.get("tx_hash") or "",
        "canonical_fill_key": meta.get("canonical_fill_key") or "",
        "condition_id": meta.get("condition_id") or "",
        "token_id": meta.get("token_id") or "",
    }


def _raw_trade_block_report_key(event: BacktestEvent) -> tuple[str, str, str, str, str, int]:
    meta = dict(event.meta or {})
    return (
        str(event.market_slug or ""),
        str(event.token_side or ""),
        str(meta.get("market_id") or ""),
        str(meta.get("condition_id") or ""),
        str(meta.get("token_id") or ""),
        int(event.x_value),
    )


def _raw_trade_block_report_sort_key(key: tuple[str, str, str, str, str, int]) -> tuple[int, str, str, str, str, str]:
    market_slug, token_side, market_id, condition_id, token_id, block_number = key
    return int(block_number), str(market_slug), str(token_side), str(market_id), str(condition_id), str(token_id)


def canonical_fill_key(row: Mapping[str, Any]) -> str:
    return canonical_fill_key_parts(row)[0]


def canonical_fill_key_parts(row: Mapping[str, Any]) -> tuple[str, str]:
    tx_hash = _normalize_tx_hash(_value(row, "tx_hash", "txHash"))
    log_index = _optional_string(_value(row, "log_index", "logIndex"))
    market_id = _optional_string(_value(row, "market_id", "marketId"))
    condition_id = _optional_string(_value(row, "condition_id", "conditionId"))
    token_id = _optional_string(_value(row, "token_id", "tokenId"))
    maker = _normalize_address(_value(row, "maker", "maker_address", "makerAddress"))
    taker = _normalize_address(_value(row, "taker", "taker_address", "takerAddress"))
    side = _normalize_side(_value(row, "side_code", "sideCode", "side", "taker_side", "takerSide"))
    strong_parts = [tx_hash, log_index, market_id, token_id, maker, taker, side]
    has_strong_identity = all(str(part or "").strip() for part in strong_parts)
    if has_strong_identity:
        canonical_parts = [tx_hash, log_index, market_id]
        if condition_id:
            canonical_parts.append(condition_id)
        canonical_parts.extend([token_id, maker, taker, side])
        return _join_key(canonical_parts), "canonical"

    fallback_parts = [
        _optional_string(_value(row, "block_number", "blockNumber", "x_value", "xValue")),
        _optional_string(_value(row, "transaction_index", "transactionIndex")),
        log_index,
        tx_hash,
        market_id,
        condition_id,
        token_id,
        _optional_string(_value(row, "trade_price", "tradePrice", "price", "avg_fill_price", "avgFillPrice")),
        _optional_string(_value(row, "size", "amount", "matched_size", "matchedSize")),
    ]
    return _join_key(fallback_parts), "fallback"


def _join_key(parts: Sequence[Any]) -> str:
    return "|".join(str(part) for part in parts if part not in (None, ""))


def _normalize_tx_hash(value: Any) -> str | None:
    text = _optional_string(value)
    return text.lower() if text else None


def _normalize_address(value: Any) -> str | None:
    text = _optional_string(value)
    return text.lower() if text else None


def _normalize_side(value: Any) -> str | None:
    text = _optional_string(value)
    if text is None:
        return None
    normalized = text.strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "BUY_YES": "BUY",
        "YES_BUY": "BUY",
        "TAKER_BUY": "BUY",
        "SELL_YES": "SELL",
        "YES_SELL": "SELL",
        "TAKER_SELL": "SELL",
    }
    return aliases.get(normalized, normalized)


def _maker_side_from_taker_side(side: str | None) -> str | None:
    if side == "BUY":
        return "SELL"
    if side == "SELL":
        return "BUY"
    return None


def _ledger_event(row: Mapping[str, Any], *, market_slug: str, token_side: str) -> BacktestEvent:
    event_type = str(row.get("event_type") or row.get("eventType") or "LEDGER").upper()
    cls = SettlementEvent if event_type in {"SETTLEMENT", "REFUND", "REDEEM"} else LedgerEvent
    public_type = "SETTLEMENT" if cls is SettlementEvent else "LEDGER"
    return cls(
        event_type=public_type,
        x_axis=str(row.get("x_axis") or row.get("xAxis") or "block_number"),
        x_value=_int(row.get("x_value") or row.get("xValue")),
        priority=EVENT_PRIORITIES[public_type],
        source=str(row.get("source") or "backtest_ledger"),
        market_slug=str(row.get("market_slug") or row.get("marketSlug") or market_slug),
        token_side=str(row.get("token_side") or row.get("tokenSide") or token_side),
        order_id=str(row.get("order_id") or row.get("orderId") or ""),
        trade_id=str(row.get("trade_id") or row.get("tradeId") or ""),
        price=_optional_string(row.get("price")),
        size=_optional_string(row.get("shares_delta") or row.get("sharesDelta")),
        cash_delta=_optional_string(row.get("cash_delta") or row.get("cashDelta")),
        message=event_type,
        meta={"ledger_event_type": event_type, "realized_pnl": _optional_string(row.get("realized_pnl") or row.get("realizedPnl"))},
    )


def _value(row: Any, *keys: str) -> Any:
    if isinstance(row, Mapping):
        for key in keys:
            if row.get(key) not in (None, ""):
                return row.get(key)
        return None
    for key in keys:
        value = getattr(row, key, None)
        if value not in (None, ""):
            return value
    return None


def _int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _sort_int(value: Any) -> int:
    if value in (None, ""):
        return 2_147_483_647
    try:
        return int(value)
    except Exception:
        return 2_147_483_647


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except Exception:
        return Decimal("0")


def _optional_string(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _decimal_string(value: Decimal) -> str:
    return format(value.normalize(), "f") if value else "0"


def _sequence(value: Any) -> Sequence[Any]:
    if value is None:
        return []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return []
