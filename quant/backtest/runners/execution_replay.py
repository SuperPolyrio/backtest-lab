"""OrderFilled-first execution replay helpers.

The helpers model Polymarket orders as resting limit orders over immutable
historical fills. Price crossing makes a fill candidate; available historical
volume and the liquidity cap decide actual fill size.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Literal

from ..event_stream import canonical_fill_key_parts
from ..execution_profiles import normalize_execution_profile, normalize_order_role


Q = Decimal("0.0000000001")
TimeInForce = Literal["GTC", "GTD", "FOK", "FAK"]
OrderStatus = Literal["FILLED", "PARTIAL_FILLED", "NO_FILL", "REJECTED", "EXPIRED", "CANCELED", "CANCEL_FAILED"]
OrderSide = Literal["BUY_YES", "SELL_YES"]
ExecutionProfileName = Literal["optimistic", "neutral", "realistic", "conservative", "stress"]
OrderRole = Literal["maker", "taker", "auto"]
SequenceKey = tuple[int, int, int, str, str]


def sequence_key(
    block_number: int,
    transaction_index: int | None = None,
    log_index: int | None = None,
    tx_hash: str | None = None,
    canonical_fill_key: str | None = None,
) -> SequenceKey:
    return (
        int(block_number),
        int(transaction_index or 0),
        int(log_index or 0),
        str(tx_hash or ""),
        str(canonical_fill_key or ""),
    )


@dataclass(frozen=True)
class ReplayTradeEvent:
    market_id: int
    token_id: str
    block_number: int
    log_index: int
    tx_hash: str
    trade_price: Decimal
    size: Decimal
    transaction_index: int = 0
    condition_id: str | None = None
    maker: str | None = None
    taker: str | None = None
    side_code: str | None = None

    @property
    def event_sequence(self) -> SequenceKey:
        return sequence_key(
            self.block_number,
            self.transaction_index,
            self.log_index,
            self.tx_hash,
            self.canonical_fill_key,
        )

    @property
    def canonical_fill_key_parts(self) -> tuple[str, str]:
        return canonical_fill_key_parts(replay_trade_event_dict(self))

    @property
    def canonical_fill_key(self) -> str:
        return self.canonical_fill_key_parts[0]

    @property
    def canonical_fill_key_kind(self) -> str:
        return self.canonical_fill_key_parts[1]


@dataclass(frozen=True)
class OrderIntent:
    side: OrderSide
    limit_price: Decimal
    size: Decimal
    time_in_force: TimeInForce = "GTC"
    submit_sequence: SequenceKey = field(default_factory=lambda: sequence_key(0))
    expire_sequence: SequenceKey | None = None
    liquidity_cap_pct: Decimal = Decimal("100")
    fee_bps: Decimal = Decimal("0")
    rebate_bps: Decimal = Decimal("0")
    order_id: str = "O-0001"
    order_role: OrderRole = "maker"
    execution_profile: ExecutionProfileName = "optimistic"
    cancel_sequence: SequenceKey | None = None
    cancel_ack_sequence: SequenceKey | None = None
    cancel_fail_sequence: SequenceKey | None = None


@dataclass
class OrderReplayResult:
    order_id: str
    status: OrderStatus
    filled_size: Decimal
    unfilled_size: Decimal
    avg_fill_price: Decimal
    cash_delta: Decimal
    fee: Decimal
    rebate: Decimal
    position_delta: Decimal
    candidate_events: list[ReplayTradeEvent]
    consumed_events: list[ReplayTradeEvent]
    requested_size: Decimal
    requested_notional: Decimal
    limit_price: Decimal
    filled_notional: Decimal
    fill_pct: Decimal
    fill_probability: Decimal
    expected_fill_size: Decimal
    expected_fill_notional: Decimal
    participation_rate: Decimal
    available_size: Decimal
    fill_profile: str
    order_role: str
    fill_probability_model: str
    execution_model_evidence: dict[str, Any] = field(default_factory=dict)
    fill_schedule: list[dict[str, Any]] = field(default_factory=list)
    no_fill_reason: str = ""

    def to_fill_dict(self) -> dict[str, Any]:
        price_key = "entry_price" if self.position_delta > 0 else "exit_price"
        notes = [self.no_fill_reason] if self.no_fill_reason else []
        candidate_volume = sum((event.size for event in self.candidate_events), Decimal("0")).quantize(Q, rounding=ROUND_HALF_UP)
        candidate_notional = sum(
            (event.size * Decimal(str(event.trade_price)) for event in self.candidate_events),
            Decimal("0"),
        ).quantize(Q, rounding=ROUND_HALF_UP)
        available_notional = sum(
            (
                Decimal(str(row.get("fillable_size") or 0))
                * Decimal(str(row.get("trade_price") or 0))
                for row in self.fill_schedule
            ),
            Decimal("0"),
        ).quantize(Q, rounding=ROUND_HALF_UP)
        return {
            "requested_notional": self.requested_notional,
            "filled_notional": self.filled_notional,
            "expected_fill_notional": self.expected_fill_notional,
            "actual_fill_notional": self.filled_notional,
            "fill_pct": self.fill_pct,
            "fill_probability": self.fill_probability,
            "fill_probability_model": self.fill_probability_model,
            "size": self.filled_size,
            price_key: self.avg_fill_price,
            "avg_fill_price": self.avg_fill_price,
            "liquidity_cap_pct": Decimal("0") if not self.candidate_events else None,
            "partial_fill": self.status == "PARTIAL_FILLED",
            "rejected": self.status in {"NO_FILL", "REJECTED", "EXPIRED", "CANCELED", "CANCEL_FAILED"},
            "fill_status": "PARTIAL" if self.status == "PARTIAL_FILLED" else self.status,
            "requested_size": self.requested_size,
            "expected_fill_size": self.expected_fill_size,
            "actual_fill_size": self.filled_size,
            "filled_size": self.filled_size,
            "unfilled_size": self.unfilled_size,
            "block_volume": candidate_volume,
            "candidate_notional": candidate_notional,
            "trade_count": len(self.candidate_events),
            "participation_rate": self.participation_rate,
            "available_notional": available_notional,
            "available_size": self.available_size,
            "fee_cost": self.fee,
            "rebate": self.rebate,
            "rebate_cost": self.rebate,
            "slippage_cost": Decimal("0"),
            "execution_cost": self.fee - self.rebate,
            "execution_source": "orderfilled_limit_replay",
            "execution_profile": self.fill_profile,
            "fill_profile": self.fill_profile,
            "order_role": self.order_role,
            "execution_model_evidence": self.execution_model_evidence,
            "limit_price": self.limit_price,
            "notes": notes,
            "fill_schedule": self.fill_schedule,
            "candidate_events": [
                replay_trade_event_dict(event)
                for event in self.candidate_events
            ],
            "consumed_events": [
                replay_trade_event_dict(event)
                for event in self.consumed_events
            ],
        }


def replay_limit_order(order: OrderIntent, events: list[ReplayTradeEvent]) -> OrderReplayResult:
    ordered_events = dedupe_replay_events(events)
    profile = normalize_execution_profile(order.execution_profile)
    role = normalize_order_role(order.order_role)
    candidates = [
        event
        for event in ordered_events
        if _is_candidate(order, event)
    ]
    cancel_failed = order.cancel_fail_sequence is not None
    cancel_cutoff = order.cancel_ack_sequence or order.cancel_sequence
    if cancel_cutoff is not None and not cancel_failed:
        candidates = [event for event in candidates if event.event_sequence <= cancel_cutoff]
    if order.time_in_force in {"FOK", "FAK"}:
        candidates = [event for event in candidates if event.event_sequence[0] == order.submit_sequence[0]]
    requested_size = max(Decimal("0"), Decimal(str(order.size))).quantize(Q, rounding=ROUND_HALF_UP)
    limit_price = max(Decimal("0.0000000001"), Decimal(str(order.limit_price))).quantize(Q, rounding=ROUND_HALF_UP)
    requested_notional = (requested_size * limit_price).quantize(Q, rounding=ROUND_HALF_UP)
    if requested_size <= 0:
        return _empty_result(order, "REJECTED", "invalid_order_size", requested_size, limit_price, candidates, Decimal("0"), profile, role, [], Decimal("0"))
    fill_schedule = _build_fill_schedule(
        candidates,
        order.liquidity_cap_pct,
        profile,
        role,
        order.side,
        context_events=ordered_events,
        requested_size=requested_size,
        cancel_sequence=order.cancel_sequence,
        cancel_ack_sequence=order.cancel_ack_sequence,
        cancel_fail_sequence=order.cancel_fail_sequence,
    )
    fillable = _schedule_available_size(fill_schedule)
    fill_probability = _fill_probability(requested_size, fillable, candidates, profile, role)
    fill_probability_model = _fill_probability_model(profile, role)

    if order.time_in_force == "FOK" and fillable < requested_size:
        return _empty_result(order, "REJECTED", "fok_insufficient_volume", requested_size, limit_price, candidates, fillable, profile, role, fill_schedule, fill_probability)
    if fillable <= 0:
        if order.cancel_sequence is not None and cancel_failed:
            return _empty_result(order, "CANCEL_FAILED", "cancel_failed_no_fill_evidence", requested_size, limit_price, candidates, fillable, profile, role, fill_schedule, fill_probability)
        if order.cancel_sequence is not None:
            return _empty_result(order, "CANCELED", "cancelled_before_fill", requested_size, limit_price, candidates, fillable, profile, role, fill_schedule, fill_probability)
        if order.time_in_force == "GTD" and order.expire_sequence is not None:
            return _empty_result(order, "EXPIRED", "gtd_expired", requested_size, limit_price, candidates, fillable, profile, role, fill_schedule, fill_probability)
        return _empty_result(order, "NO_FILL", "limit_not_crossed", requested_size, limit_price, candidates, fillable, profile, role, fill_schedule, fill_probability)

    filled_size = min(requested_size, fillable).quantize(Q, rounding=ROUND_HALF_UP)
    if filled_size < requested_size and order.time_in_force == "GTC":
        status: OrderStatus = "PARTIAL_FILLED"
    elif filled_size < requested_size and order.time_in_force in {"GTD", "FAK"}:
        status = "PARTIAL_FILLED"
    else:
        status = "FILLED"
    partial_residual_reason = _partial_residual_reason(order, filled_size, requested_size, cancel_failed)
    return _filled_result(
        order,
        status,
        requested_size,
        filled_size,
        limit_price,
        candidates,
        fillable,
        profile,
        role,
        fill_schedule,
        fill_probability,
        fill_probability_model,
        partial_residual_reason,
    )


def replay_trade_event_dict(event: ReplayTradeEvent) -> dict[str, Any]:
    key, key_kind = canonical_fill_key_parts(
        {
            "market_id": event.market_id,
            "token_id": event.token_id,
            "condition_id": event.condition_id,
            "block_number": event.block_number,
            "transaction_index": event.transaction_index,
            "log_index": event.log_index,
            "tx_hash": event.tx_hash,
            "trade_price": event.trade_price,
            "size": event.size,
            "maker": event.maker,
            "taker": event.taker,
            "side_code": event.side_code,
        }
    )
    return {
        "market_id": event.market_id,
        "token_id": event.token_id,
        "condition_id": event.condition_id,
        "block_number": event.block_number,
        "transaction_index": event.transaction_index,
        "log_index": event.log_index,
        "tx_hash": event.tx_hash,
        "trade_price": event.trade_price,
        "size": event.size,
        "maker": event.maker,
        "taker": event.taker,
        "side_code": event.side_code,
        "canonical_fill_key": key,
        "canonical_fill_key_kind": key_kind,
    }


def dedupe_replay_events(events: list[ReplayTradeEvent]) -> list[ReplayTradeEvent]:
    seen: set[str] = set()
    deduped: list[ReplayTradeEvent] = []
    for event in sorted(events, key=lambda item: item.event_sequence):
        key, key_kind = event.canonical_fill_key_parts
        if key_kind == "canonical" and key:
            if key in seen:
                continue
            seen.add(key)
        deduped.append(event)
    return deduped


def _is_candidate(order: OrderIntent, event: ReplayTradeEvent) -> bool:
    if event.event_sequence <= order.submit_sequence:
        return False
    if order.expire_sequence is not None and event.event_sequence > order.expire_sequence:
        return False
    price = Decimal(str(event.trade_price))
    limit = Decimal(str(order.limit_price))
    if order.side == "BUY_YES":
        return price <= limit
    return price >= limit


def _fillable_size(
    events: list[ReplayTradeEvent],
    liquidity_cap_pct: Decimal,
    execution_profile: str = "optimistic",
    order_role: str = "maker",
    order_side: str = "",
) -> Decimal:
    return _schedule_available_size(_build_fill_schedule(events, liquidity_cap_pct, execution_profile, order_role, order_side))


def _build_fill_schedule(
    events: list[ReplayTradeEvent],
    liquidity_cap_pct: Decimal,
    execution_profile: str = "optimistic",
    order_role: str = "maker",
    order_side: str = "",
    context_events: list[ReplayTradeEvent] | None = None,
    requested_size: Decimal | None = None,
    cancel_sequence: SequenceKey | None = None,
    cancel_ack_sequence: SequenceKey | None = None,
    cancel_fail_sequence: SequenceKey | None = None,
) -> list[dict[str, Any]]:
    requested_cap = min(Decimal("1"), max(Decimal("0"), Decimal(str(liquidity_cap_pct)) / Decimal("100")))
    if requested_cap <= 0:
        return []
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    requested = max(Decimal("0"), Decimal(str(requested_size or 0)))
    base_factor = _profile_role_fill_factor(profile, role)
    effective_cap = _effective_liquidity_cap(requested_cap, profile, role)
    ordered_events = sorted(events, key=lambda item: item.event_sequence)
    context_ordered_events = sorted(context_events if context_events is not None else events, key=lambda item: item.event_sequence)
    block_counts: dict[tuple[int, int, str], int] = {}
    candidate_block_seen: dict[tuple[int, int, str], int] = {}
    block_events: dict[tuple[int, int, str], list[ReplayTradeEvent]] = {}
    block_stats: dict[tuple[int, int, str], dict[str, Any]] = {}
    block_positions: dict[tuple[Any, ...], int] = {}
    for event in context_ordered_events:
        key = _block_liquidity_key(event)
        block_counts[key] = block_counts.get(key, 0) + 1
        block_positions[_replay_event_identity(event)] = block_counts[key]
        block_events.setdefault(key, []).append(event)
    for key, group_events in block_events.items():
        block_stats[key] = _block_price_context(group_events)
    schedule: list[dict[str, Any]] = []
    remaining_requested = requested
    for index, event in enumerate(ordered_events):
        event_size = max(Decimal("0"), Decimal(str(event.size)))
        if event_size <= 0:
            continue
        block_key = _block_liquidity_key(event)
        candidate_block_seen[block_key] = candidate_block_seen.get(block_key, 0) + 1
        candidate_block_trade_index = candidate_block_seen[block_key]
        block_trade_index = block_positions.get(_replay_event_identity(event), 0)
        block_trade_count = block_counts[block_key]
        context = block_stats[block_key]
        sequence_decay_factor = _sequence_decay_factor(index, block_trade_index, profile, role)
        block_context_factor, block_context_reason, block_range_pct, vwap_dislocation_pct = _block_context_fill_factor(
            context,
            event.trade_price,
            profile,
            role,
        )
        tick_quality_factor, tick_quality_reason, tick_volume_share_pct = _tick_quality_fill_factor(
            context,
            event_size,
            vwap_dislocation_pct,
            profile,
            role,
        )
        remaining_before_tick = remaining_requested.quantize(Q, rounding=ROUND_HALF_UP)
        block_participation_factor, block_participation_reason, requested_block_participation_pct = _block_participation_fill_factor(
            context,
            remaining_before_tick,
            profile,
            role,
        )
        event_factor = base_factor * sequence_decay_factor * block_context_factor * tick_quality_factor
        side_factor, side_compatibility, expected_event_side, observed_event_side = _side_compatibility_factor(
            order_side,
            event.side_code,
            role,
            profile,
        )
        maker_queue_factor, maker_queue_reason, maker_queue_ahead_size = _maker_queue_fill_factor(
            event,
            block_events.get(block_key, []),
            remaining_before_tick,
            profile,
            role,
        )
        cancel_pending_factor, cancel_pending_reason = _cancel_pending_fill_factor(
            event.event_sequence,
            cancel_sequence,
            cancel_ack_sequence,
            cancel_fail_sequence,
            profile,
            role,
        )
        event_factor *= side_factor * block_participation_factor * maker_queue_factor * cancel_pending_factor
        fill_factor = min(Decimal("1"), max(Decimal("0"), event_factor))
        fillable_size = (event_size * effective_cap * fill_factor).quantize(Q, rounding=ROUND_HALF_UP)
        remaining_after_tick = max(Decimal("0"), remaining_before_tick - fillable_size).quantize(Q, rounding=ROUND_HALF_UP)
        schedule.append(
            {
                "event": event,
                "sequence": index + 1,
                "block_trade_index": block_trade_index,
                "block_trade_count": block_trade_count,
                "candidate_block_trade_index": candidate_block_trade_index,
                "block_open_price": context["open"],
                "block_high_price": context["high"],
                "block_low_price": context["low"],
                "block_close_price": context["close"],
                "block_vwap_price": context["vwap"],
                "block_volume": context["volume"],
                "block_notional": context["notional"],
                "block_buy_volume": context["buy_volume"],
                "block_sell_volume": context["sell_volume"],
                "block_unknown_side_volume": context["unknown_side_volume"],
                "block_buy_notional": context["buy_notional"],
                "block_sell_notional": context["sell_notional"],
                "block_unknown_side_notional": context["unknown_side_notional"],
                "block_first_sequence": context["first_sequence"],
                "block_last_sequence": context["last_sequence"],
                "raw_size": event_size.quantize(Q, rounding=ROUND_HALF_UP),
                "liquidity_cap_pct": (requested_cap * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP),
                "effective_liquidity_cap_pct": (effective_cap * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP),
                "liquidity_curve": _liquidity_curve_name(profile, role),
                "profile_factor": fill_factor.quantize(Q, rounding=ROUND_HALF_UP),
                "base_profile_factor": base_factor.quantize(Q, rounding=ROUND_HALF_UP),
                "sequence_decay_factor": sequence_decay_factor.quantize(Q, rounding=ROUND_HALF_UP),
                "block_context_factor": block_context_factor.quantize(Q, rounding=ROUND_HALF_UP),
                "block_context_reason": block_context_reason,
                "block_range_pct": block_range_pct.quantize(Q, rounding=ROUND_HALF_UP),
                "vwap_dislocation_pct": vwap_dislocation_pct.quantize(Q, rounding=ROUND_HALF_UP),
                "tick_quality_factor": tick_quality_factor.quantize(Q, rounding=ROUND_HALF_UP),
                "tick_quality_reason": tick_quality_reason,
                "tick_volume_share_pct": tick_volume_share_pct.quantize(Q, rounding=ROUND_HALF_UP),
                "remaining_order_size_before_tick": remaining_before_tick,
                "remaining_order_size_after_tick": remaining_after_tick,
                "requested_block_participation_pct": requested_block_participation_pct.quantize(Q, rounding=ROUND_HALF_UP),
                "block_participation_factor": block_participation_factor.quantize(Q, rounding=ROUND_HALF_UP),
                "block_participation_reason": block_participation_reason,
                "maker_queue_factor": maker_queue_factor.quantize(Q, rounding=ROUND_HALF_UP),
                "maker_queue_reason": maker_queue_reason,
                "maker_queue_ahead_size": maker_queue_ahead_size.quantize(Q, rounding=ROUND_HALF_UP),
                "cancel_pending_factor": cancel_pending_factor.quantize(Q, rounding=ROUND_HALF_UP),
                "cancel_pending_reason": cancel_pending_reason,
                "side_compatibility": side_compatibility,
                "side_compatibility_factor": side_factor.quantize(Q, rounding=ROUND_HALF_UP),
                "expected_event_side": expected_event_side,
                "observed_event_side": observed_event_side,
                "fillable_size": fillable_size,
                "fill_probability_model": _fill_probability_model(profile, role),
            }
        )
        remaining_requested = remaining_after_tick
    return schedule


def _block_price_context(events: list[ReplayTradeEvent]) -> dict[str, Any]:
    ordered = sorted(events, key=lambda item: item.event_sequence)
    prices = [max(Decimal("0"), Decimal(str(event.trade_price))) for event in ordered]
    sizes = [max(Decimal("0"), Decimal(str(event.size))) for event in ordered]
    volume = sum(sizes, Decimal("0")).quantize(Q, rounding=ROUND_HALF_UP)
    notional = sum((price * size for price, size in zip(prices, sizes)), Decimal("0")).quantize(Q, rounding=ROUND_HALF_UP)
    vwap = (notional / volume).quantize(Q, rounding=ROUND_HALF_UP) if volume > 0 else Decimal("0").quantize(Q)
    buy_volume = Decimal("0")
    sell_volume = Decimal("0")
    unknown_side_volume = Decimal("0")
    buy_notional = Decimal("0")
    sell_notional = Decimal("0")
    unknown_side_notional = Decimal("0")
    for event, price, size in zip(ordered, prices, sizes):
        side = _normalize_trade_side(event.side_code)
        event_notional = (price * size).quantize(Q, rounding=ROUND_HALF_UP)
        if side == "BUY":
            buy_volume += size
            buy_notional += event_notional
        elif side == "SELL":
            sell_volume += size
            sell_notional += event_notional
        else:
            unknown_side_volume += size
            unknown_side_notional += event_notional
    return {
        "open": prices[0].quantize(Q, rounding=ROUND_HALF_UP) if prices else Decimal("0").quantize(Q),
        "high": max(prices).quantize(Q, rounding=ROUND_HALF_UP) if prices else Decimal("0").quantize(Q),
        "low": min(prices).quantize(Q, rounding=ROUND_HALF_UP) if prices else Decimal("0").quantize(Q),
        "close": prices[-1].quantize(Q, rounding=ROUND_HALF_UP) if prices else Decimal("0").quantize(Q),
        "vwap": vwap,
        "volume": volume,
        "notional": notional,
        "buy_volume": buy_volume.quantize(Q, rounding=ROUND_HALF_UP),
        "sell_volume": sell_volume.quantize(Q, rounding=ROUND_HALF_UP),
        "unknown_side_volume": unknown_side_volume.quantize(Q, rounding=ROUND_HALF_UP),
        "buy_notional": buy_notional.quantize(Q, rounding=ROUND_HALF_UP),
        "sell_notional": sell_notional.quantize(Q, rounding=ROUND_HALF_UP),
        "unknown_side_notional": unknown_side_notional.quantize(Q, rounding=ROUND_HALF_UP),
        "first_sequence": _event_sequence_dict(ordered[0]) if ordered else {},
        "last_sequence": _event_sequence_dict(ordered[-1]) if ordered else {},
    }


def _block_context_fill_factor(
    context: dict[str, Any],
    trade_price: Decimal,
    execution_profile: str,
    order_role: str,
) -> tuple[Decimal, str, Decimal, Decimal]:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    vwap = max(Decimal("0"), Decimal(str(context.get("vwap") or 0)))
    high = max(Decimal("0"), Decimal(str(context.get("high") or 0)))
    low = max(Decimal("0"), Decimal(str(context.get("low") or 0)))
    price = max(Decimal("0"), Decimal(str(trade_price or 0)))
    if profile == "optimistic" or vwap <= 0:
        return Decimal("1"), "optimistic_or_missing_block_context", Decimal("0"), Decimal("0")

    block_range_pct = ((high - low).copy_abs() / vwap).quantize(Q, rounding=ROUND_HALF_UP)
    vwap_dislocation_pct = ((price - vwap).copy_abs() / vwap).quantize(Q, rounding=ROUND_HALF_UP)
    range_excess = max(Decimal("0"), block_range_pct - Decimal("0.05"))
    dislocation_excess = max(Decimal("0"), vwap_dislocation_pct - Decimal("0.025"))
    if range_excess <= 0 and dislocation_excess <= 0:
        return Decimal("1"), "stable_block_context", block_range_pct, vwap_dislocation_pct

    profile_weight = {
        "neutral": Decimal("0.25"),
        "realistic": Decimal("0.45"),
        "conservative": Decimal("0.70"),
        "stress": Decimal("1.10"),
    }[profile]
    role_weight = {
        "taker": Decimal("0.35"),
        "auto": Decimal("0.70"),
        "maker": Decimal("1.00"),
    }[role]
    risk = (range_excess + dislocation_excess) * profile_weight * role_weight
    factor = (Decimal("1") / (Decimal("1") + risk)).quantize(Q, rounding=ROUND_HALF_UP)
    return factor, "block_volatility_vwap_discount", block_range_pct, vwap_dislocation_pct


def _tick_quality_fill_factor(
    context: dict[str, Any],
    event_size: Decimal,
    vwap_dislocation_pct: Decimal,
    execution_profile: str,
    order_role: str,
) -> tuple[Decimal, str, Decimal]:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    volume = max(Decimal("0"), Decimal(str(context.get("volume") or 0)))
    size = max(Decimal("0"), Decimal(str(event_size or 0)))
    if profile == "optimistic" or volume <= 0 or size <= 0:
        return Decimal("1"), "optimistic_or_missing_tick_quality", Decimal("0")
    share = (size / volume).quantize(Q, rounding=ROUND_HALF_UP)
    share_pct = (share * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP)
    small_share_excess = max(Decimal("0"), Decimal("0.01") - share) / Decimal("0.01")
    dislocation_excess = max(Decimal("0"), Decimal(str(vwap_dislocation_pct)) - Decimal("0.05")) / Decimal("0.05")
    if small_share_excess <= 0 or dislocation_excess <= 0:
        return Decimal("1"), "normal_tick_quality", share_pct

    profile_weight = {
        "neutral": Decimal("0.35"),
        "realistic": Decimal("0.60"),
        "conservative": Decimal("0.95"),
        "stress": Decimal("1.40"),
    }[profile]
    role_weight = {
        "taker": Decimal("0.50"),
        "auto": Decimal("0.75"),
        "maker": Decimal("1.00"),
    }[role]
    risk = small_share_excess * dislocation_excess * profile_weight * role_weight
    factor = (Decimal("1") / (Decimal("1") + risk)).quantize(Q, rounding=ROUND_HALF_UP)
    return factor, "small_dislocated_tick_discount", share_pct


def _block_participation_fill_factor(
    context: dict[str, Any],
    requested_size: Decimal,
    execution_profile: str,
    order_role: str,
) -> tuple[Decimal, str, Decimal]:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    requested = max(Decimal("0"), Decimal(str(requested_size or 0)))
    volume = max(Decimal("0"), Decimal(str(context.get("volume") or 0)))
    if profile == "optimistic" or requested <= 0 or volume <= 0:
        if requested <= 0:
            return Decimal("1"), "residual_order_already_fully_fillable", Decimal("0")
        return Decimal("1"), "optimistic_or_missing_block_participation", Decimal("0")

    participation = (requested / volume).quantize(Q, rounding=ROUND_HALF_UP)
    participation_pct = (participation * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP)
    soft_cap = {
        "neutral": Decimal("5.0") if role == "taker" else Decimal("3.0"),
        "realistic": Decimal("4.0") if role == "taker" else Decimal("2.5"),
        "conservative": Decimal("3.0") if role == "taker" else Decimal("2.0"),
        "stress": Decimal("2.0") if role == "taker" else Decimal("1.5"),
    }[profile]
    if participation <= soft_cap:
        return Decimal("1"), "normal_block_participation", participation_pct

    excess = (participation - soft_cap) / soft_cap
    profile_weight = {
        "neutral": Decimal("0.35"),
        "realistic": Decimal("0.55"),
        "conservative": Decimal("0.80"),
        "stress": Decimal("1.20"),
    }[profile]
    role_weight = {
        "taker": Decimal("0.65"),
        "auto": Decimal("0.85"),
        "maker": Decimal("1.00"),
    }[role]
    risk = excess * profile_weight * role_weight
    factor = (Decimal("1") / (Decimal("1") + risk)).quantize(Q, rounding=ROUND_HALF_UP)
    return factor, "large_order_block_participation_discount", participation_pct


def _maker_queue_fill_factor(
    event: ReplayTradeEvent,
    block_events: list[ReplayTradeEvent],
    requested_size: Decimal,
    execution_profile: str,
    order_role: str,
) -> tuple[Decimal, str, Decimal]:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    requested = max(Decimal("0"), Decimal(str(requested_size or 0)))
    if profile == "optimistic" or role != "maker" or requested <= 0:
        return Decimal("1"), "optimistic_taker_or_missing_queue_context", Decimal("0")

    price = Decimal(str(event.trade_price or 0)).quantize(Q, rounding=ROUND_HALF_UP)
    ahead_size = Decimal("0")
    for other in block_events:
        if other.event_sequence >= event.event_sequence:
            continue
        other_price = Decimal(str(other.trade_price or 0)).quantize(Q, rounding=ROUND_HALF_UP)
        if other_price != price:
            continue
        ahead_size += max(Decimal("0"), Decimal(str(other.size or 0)))
    ahead_size = ahead_size.quantize(Q, rounding=ROUND_HALF_UP)
    if ahead_size <= 0:
        return Decimal("1"), "no_same_price_orderfilled_queue_ahead", ahead_size

    profile_weight = {
        "neutral": Decimal("0.25"),
        "realistic": Decimal("0.50"),
        "conservative": Decimal("0.75"),
        "stress": Decimal("1.00"),
    }[profile]
    effective_ahead = ahead_size * profile_weight
    factor = (requested / (requested + effective_ahead)).quantize(Q, rounding=ROUND_HALF_UP)
    return factor, "same_price_orderfilled_queue_ahead_discount", ahead_size


def _cancel_pending_fill_factor(
    event_sequence: SequenceKey,
    cancel_sequence: SequenceKey | None,
    cancel_ack_sequence: SequenceKey | None,
    cancel_fail_sequence: SequenceKey | None,
    execution_profile: str,
    order_role: str,
) -> tuple[Decimal, str]:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    if cancel_sequence is None or event_sequence <= cancel_sequence:
        return Decimal("1"), "no_cancel_pending"
    terminal_sequence = cancel_ack_sequence or cancel_fail_sequence
    if terminal_sequence is not None and event_sequence > terminal_sequence:
        return Decimal("1"), "cancel_pending_window_closed"
    if profile == "optimistic":
        return Decimal("1"), "cancel_pending_ignored_optimistic"
    penalty = {
        "neutral": Decimal("0.85") if role == "taker" else Decimal("0.70"),
        "realistic": Decimal("0.70") if role == "taker" else Decimal("0.50"),
        "conservative": Decimal("0.55") if role == "taker" else Decimal("0.35"),
        "stress": Decimal("0.35") if role == "taker" else Decimal("0.20"),
    }[profile]
    if cancel_fail_sequence is not None and (cancel_ack_sequence is None or cancel_fail_sequence <= cancel_ack_sequence):
        penalty = min(Decimal("1"), penalty + Decimal("0.10"))
        return penalty, "cancel_pending_before_failed_cancel_discount"
    return penalty, "cancel_pending_before_ack_discount"


def _event_sequence_dict(event: ReplayTradeEvent) -> dict[str, Any]:
    return {
        "market_id": int(event.market_id),
        "token_id": str(event.token_id),
        "condition_id": event.condition_id,
        "block_number": int(event.block_number),
        "transaction_index": int(event.transaction_index),
        "log_index": int(event.log_index),
        "tx_hash": str(event.tx_hash),
        "canonical_fill_key": event.canonical_fill_key,
    }


def _schedule_available_size(schedule: list[dict[str, Any]]) -> Decimal:
    return sum((row["fillable_size"] for row in schedule), Decimal("0")).quantize(Q, rounding=ROUND_HALF_UP)


def _fill_probability(
    requested_size: Decimal,
    available_size: Decimal,
    candidates: list[ReplayTradeEvent],
    execution_profile: str,
    order_role: str,
) -> Decimal:
    if requested_size <= 0:
        return Decimal("0")
    if not candidates:
        return Decimal("0")
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    coverage = min(Decimal("1"), max(Decimal("0"), available_size / requested_size))
    uncertainty = {
        "optimistic": Decimal("1.00"),
        "neutral": Decimal("0.95") if role == "taker" else Decimal("0.85"),
        "realistic": Decimal("0.85") if role == "taker" else Decimal("0.70"),
        "conservative": Decimal("0.70") if role == "taker" else Decimal("0.50"),
        "stress": Decimal("0.45") if role == "taker" else Decimal("0.30"),
    }[profile]
    sensitivity = {
        "optimistic": Decimal("0"),
        "neutral": Decimal("0.10") if role == "taker" else Decimal("0.25"),
        "realistic": Decimal("0.25") if role == "taker" else Decimal("0.45"),
        "conservative": Decimal("0.45") if role == "taker" else Decimal("0.75"),
        "stress": Decimal("0.80") if role == "taker" else Decimal("1.25"),
    }[profile]
    shaped = coverage * uncertainty / (Decimal("1") + (Decimal("1") - coverage) * sensitivity)
    return (min(Decimal("1"), max(Decimal("0"), shaped)) * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP)


def _fill_probability_model(execution_profile: str, order_role: str) -> str:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    if profile in {"neutral", "conservative", "stress"}:
        return f"raw_tick_sequence_{profile}_{role}"
    if profile == "optimistic":
        return f"raw_tick_sequence_full_cross_{role}"
    return f"raw_tick_sequence_realistic_{role}"


def _profile_role_fill_factor(execution_profile: str, order_role: str) -> Decimal:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    if profile == "optimistic":
        return Decimal("1")
    profile_factor = {
        "neutral": Decimal("0.90"),
        "realistic": Decimal("0.75"),
        "conservative": Decimal("0.55"),
        "stress": Decimal("0.30"),
    }[profile]
    role_factor = {
        "taker": {
            "neutral": Decimal("1.00"),
            "realistic": Decimal("0.95"),
            "conservative": Decimal("0.85"),
            "stress": Decimal("0.70"),
        },
        "maker": {
            "neutral": Decimal("0.70"),
            "realistic": Decimal("0.55"),
            "conservative": Decimal("0.40"),
            "stress": Decimal("0.25"),
        },
        "auto": {
            "neutral": Decimal("0.85"),
            "realistic": Decimal("0.75"),
            "conservative": Decimal("0.60"),
            "stress": Decimal("0.45"),
        },
    }[role][profile]
    return profile_factor * role_factor


def _effective_liquidity_cap(requested_cap: Decimal, execution_profile: str, order_role: str) -> Decimal:
    cap = min(Decimal("1"), max(Decimal("0"), Decimal(str(requested_cap))))
    if cap <= 0:
        return Decimal("0")
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    penalty = {
        "optimistic": Decimal("0"),
        "neutral": Decimal("0.10") if role == "taker" else Decimal("0.35"),
        "realistic": Decimal("0.25") if role == "taker" else Decimal("0.60"),
        "conservative": Decimal("0.55") if role == "taker" else Decimal("1.00"),
        "stress": Decimal("1.25") if role == "taker" else Decimal("2.00"),
    }[profile]
    return (cap / (Decimal("1") + (Decimal("1") - cap) * penalty)).quantize(Q, rounding=ROUND_HALF_UP)


def _sequence_decay_factor(global_index: int, block_trade_index: int, execution_profile: str, order_role: str) -> Decimal:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    if profile == "optimistic":
        return Decimal("1")
    global_decay = Decimal(global_index) * Decimal("0.10")
    block_decay_base = {
        "neutral": Decimal("0.03") if role == "taker" else Decimal("0.08"),
        "realistic": Decimal("0.06") if role == "taker" else Decimal("0.14"),
        "conservative": Decimal("0.10") if role == "taker" else Decimal("0.22"),
        "stress": Decimal("0.18") if role == "taker" else Decimal("0.35"),
    }[profile]
    block_decay = max(Decimal("0"), Decimal(block_trade_index - 1)) * block_decay_base
    return (Decimal("1") / (Decimal("1") + global_decay + block_decay)).quantize(Q, rounding=ROUND_HALF_UP)


def _liquidity_curve_name(execution_profile: str, order_role: str) -> str:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    if profile == "optimistic":
        return f"linear_cap_{role}"
    return f"nonlinear_cap_sequence_decay_{profile}_{role}"


def _block_liquidity_key(event: ReplayTradeEvent) -> tuple[int, str, str, int]:
    return int(event.market_id), str(event.condition_id or ""), str(event.token_id), int(event.block_number)


def _replay_event_identity(event: ReplayTradeEvent) -> tuple[Any, ...]:
    return (
        int(event.market_id),
        str(event.condition_id or "").lower(),
        str(event.token_id),
        int(event.block_number),
        int(event.transaction_index),
        int(event.log_index),
        str(event.tx_hash).lower(),
        str(event.maker or "").lower(),
        str(event.taker or "").lower(),
        str(event.side_code or "").upper(),
    )


def _side_compatibility_factor(
    order_side: str,
    event_side: str | None,
    order_role: str,
    execution_profile: str,
) -> tuple[Decimal, str, str, str]:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    observed = _normalize_trade_side(event_side)
    expected = _expected_event_side_for_order(order_side, role)
    if not expected:
        return Decimal("1"), "not_applicable", expected, observed
    if not observed:
        return Decimal("1"), "unknown", expected, observed
    if observed == expected:
        return Decimal("1"), "compatible", expected, observed
    if profile == "optimistic":
        return Decimal("1"), "incompatible_ignored_optimistic", expected, observed
    penalty = {
        "neutral": Decimal("0.75"),
        "realistic": Decimal("0.60"),
        "conservative": Decimal("0.40"),
        "stress": Decimal("0.25"),
    }[profile]
    return penalty, "incompatible_discounted", expected, observed


def _expected_event_side_for_order(order_side: str, order_role: str) -> str:
    side = str(order_side or "").upper()
    role = normalize_order_role(order_role)
    if side == "BUY_YES":
        return "SELL" if role == "maker" else "BUY"
    if side == "SELL_YES":
        return "BUY" if role == "maker" else "SELL"
    return ""


def _normalize_trade_side(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"1", "BUY", "BUY_YES", "YES_BUY"}:
        return "BUY"
    if text in {"2", "SELL", "SELL_YES", "YES_SELL"}:
        return "SELL"
    return ""


def _filled_result(
    order: OrderIntent,
    status: OrderStatus,
    requested_size: Decimal,
    filled_size: Decimal,
    limit_price: Decimal,
    candidates: list[ReplayTradeEvent],
    fillable_size: Decimal,
    fill_profile: str,
    order_role: str,
    fill_schedule: list[dict[str, Any]],
    fill_probability: Decimal,
    fill_probability_model: str,
    no_fill_reason: str = "",
) -> OrderReplayResult:
    consumed_events, filled_notional, consumed_schedule = _consume_schedule_details(fill_schedule, filled_size)
    if filled_size > 0 and filled_notional > 0:
        price = (filled_notional / filled_size).quantize(Q, rounding=ROUND_HALF_UP)
    else:
        price = limit_price
    fee = (filled_notional * max(Decimal("0"), Decimal(str(order.fee_bps))) / Decimal("10000")).quantize(Q, rounding=ROUND_HALF_UP)
    rebate = (filled_notional * max(Decimal("0"), Decimal(str(order.rebate_bps))) / Decimal("10000")).quantize(Q, rounding=ROUND_HALF_UP)
    sign = Decimal("1") if order.side == "BUY_YES" else Decimal("-1")
    cash_delta = (-(filled_notional + fee - rebate) if order.side == "BUY_YES" else (filled_notional - fee + rebate)).quantize(Q, rounding=ROUND_HALF_UP)
    expected_fill_size = min(requested_size, max(Decimal("0"), fillable_size)).quantize(Q, rounding=ROUND_HALF_UP)
    expected_fill_notional = filled_notional if expected_fill_size == filled_size else (expected_fill_size * price).quantize(Q, rounding=ROUND_HALF_UP)
    candidate_volume = sum((max(Decimal("0"), Decimal(str(event.size))) for event in candidates), Decimal("0")).quantize(Q, rounding=ROUND_HALF_UP)
    execution_evidence = _build_execution_model_evidence(
        requested_size=requested_size,
        available_size=fillable_size,
        candidates=candidates,
        fill_schedule=consumed_schedule,
        liquidity_cap_pct=order.liquidity_cap_pct,
        execution_profile=fill_profile,
        order_role=order_role,
        fill_probability=fill_probability,
        fill_probability_model=fill_probability_model,
    )
    unfilled_size = max(Decimal("0"), requested_size - filled_size).quantize(Q, rounding=ROUND_HALF_UP)
    if no_fill_reason:
        execution_evidence["partial_residual_reason"] = no_fill_reason
        execution_evidence["partial_residual_size"] = unfilled_size
        execution_evidence["partial_residual_lifecycle"] = _partial_residual_lifecycle(no_fill_reason)
    return OrderReplayResult(
        order_id=order.order_id,
        status=status,
        filled_size=filled_size,
        unfilled_size=unfilled_size,
        avg_fill_price=price,
        cash_delta=cash_delta,
        fee=fee,
        rebate=rebate,
        position_delta=(filled_size * sign).quantize(Q, rounding=ROUND_HALF_UP),
        candidate_events=candidates,
        consumed_events=consumed_events,
        requested_size=requested_size,
        requested_notional=(requested_size * limit_price).quantize(Q, rounding=ROUND_HALF_UP),
        limit_price=limit_price,
        filled_notional=filled_notional,
        fill_pct=(filled_size * Decimal("100") / requested_size).quantize(Q, rounding=ROUND_HALF_UP) if requested_size else Decimal("0"),
        fill_probability=fill_probability,
        expected_fill_size=expected_fill_size,
        expected_fill_notional=expected_fill_notional,
        participation_rate=(requested_size * Decimal("100") / candidate_volume).quantize(Q, rounding=ROUND_HALF_UP) if candidate_volume else Decimal("0"),
        available_size=fillable_size,
        fill_profile=fill_profile,
        order_role=order_role,
        fill_probability_model=fill_probability_model,
        execution_model_evidence=execution_evidence,
        fill_schedule=_serialize_fill_schedule(consumed_schedule),
        no_fill_reason=no_fill_reason,
    )


def _empty_result(
    order: OrderIntent,
    status: OrderStatus,
    reason: str,
    requested_size: Decimal,
    price: Decimal,
    candidates: list[ReplayTradeEvent],
    fillable_size: Decimal | None = None,
    fill_profile: str | None = None,
    order_role: str | None = None,
    fill_schedule: list[dict[str, Any]] | None = None,
    fill_probability: Decimal | None = None,
) -> OrderReplayResult:
    candidate_volume = sum((max(Decimal("0"), Decimal(str(event.size))) for event in candidates), Decimal("0")).quantize(Q, rounding=ROUND_HALF_UP)
    profile = normalize_execution_profile(fill_profile or order.execution_profile)
    role = normalize_order_role(order_role or order.order_role)
    available_size = (
        _schedule_available_size(
            fill_schedule
            or _build_fill_schedule(
                candidates,
                order.liquidity_cap_pct,
                profile,
                role,
                order.side,
                requested_size=requested_size,
            )
        )
        if fillable_size is None
        else max(Decimal("0"), Decimal(str(fillable_size)))
    ).quantize(Q, rounding=ROUND_HALF_UP)
    expected_fill_size = min(requested_size, available_size).quantize(Q, rounding=ROUND_HALF_UP)
    probability = (
        _fill_probability(requested_size, available_size, candidates, profile, role)
        if fill_probability is None
        else Decimal(str(fill_probability)).quantize(Q, rounding=ROUND_HALF_UP)
    )
    return OrderReplayResult(
        order_id=order.order_id,
        status=status,
        filled_size=Decimal("0"),
        unfilled_size=requested_size,
        avg_fill_price=price,
        cash_delta=Decimal("0"),
        fee=Decimal("0"),
        rebate=Decimal("0"),
        position_delta=Decimal("0"),
        candidate_events=candidates,
        consumed_events=[],
        requested_size=requested_size,
        requested_notional=(requested_size * price).quantize(Q, rounding=ROUND_HALF_UP),
        limit_price=price,
        filled_notional=Decimal("0"),
        fill_pct=Decimal("0"),
        fill_probability=probability,
        expected_fill_size=expected_fill_size,
        expected_fill_notional=(expected_fill_size * price).quantize(Q, rounding=ROUND_HALF_UP),
        participation_rate=(requested_size * Decimal("100") / candidate_volume).quantize(Q, rounding=ROUND_HALF_UP) if candidate_volume else Decimal("0"),
        available_size=available_size,
        fill_profile=profile,
        order_role=role,
        fill_probability_model=_fill_probability_model(profile, role),
        execution_model_evidence=_build_execution_model_evidence(
            requested_size=requested_size,
            available_size=available_size,
            candidates=candidates,
            fill_schedule=fill_schedule
            or _build_fill_schedule(
                candidates,
                order.liquidity_cap_pct,
                profile,
                role,
                order.side,
                requested_size=requested_size,
            ),
            liquidity_cap_pct=order.liquidity_cap_pct,
            execution_profile=profile,
            order_role=role,
            fill_probability=probability,
            fill_probability_model=_fill_probability_model(profile, role),
        ),
        fill_schedule=_serialize_fill_schedule(
            fill_schedule
            or _build_fill_schedule(
                candidates,
                order.liquidity_cap_pct,
                profile,
                role,
                order.side,
                requested_size=requested_size,
            )
        ),
        no_fill_reason=reason,
    )


def _partial_residual_reason(
    order: OrderIntent,
    filled_size: Decimal,
    requested_size: Decimal,
    cancel_failed: bool,
) -> str:
    if filled_size <= 0 or filled_size >= requested_size:
        return ""
    if order.time_in_force == "FAK":
        return "fak_residual_cancelled"
    if order.cancel_sequence is not None and not cancel_failed:
        return "residual_cancelled_after_partial_fill"
    if order.time_in_force == "GTD" and order.expire_sequence is not None:
        return "gtd_residual_expired_after_partial_fill"
    return "residual_resting_after_partial_fill"


def _partial_residual_lifecycle(reason: str) -> str:
    if "cancel" in reason:
        return "cancelled"
    if "expired" in reason:
        return "expired"
    if "resting" in reason:
        return "resting"
    return "unfilled"


def _consume_events(events: list[ReplayTradeEvent], target_size: Decimal) -> tuple[list[ReplayTradeEvent], Decimal]:
    return _consume_schedule(
        [
            {
                "event": event,
                "fillable_size": max(Decimal("0"), Decimal(str(event.size))).quantize(Q, rounding=ROUND_HALF_UP),
            }
            for event in events
        ],
        target_size,
    )


def _consume_schedule(schedule: list[dict[str, Any]], target_size: Decimal) -> tuple[list[ReplayTradeEvent], Decimal]:
    consumed, filled_notional, _ = _consume_schedule_details(schedule, target_size)
    return consumed, filled_notional


def _consume_schedule_details(schedule: list[dict[str, Any]], target_size: Decimal) -> tuple[list[ReplayTradeEvent], Decimal, list[dict[str, Any]]]:
    remaining = max(Decimal("0"), Decimal(str(target_size)))
    consumed: list[ReplayTradeEvent] = []
    filled_notional = Decimal("0")
    annotated: list[dict[str, Any]] = []
    for row in schedule:
        row_copy = dict(row)
        event = row["event"]
        event_size = max(Decimal("0"), Decimal(str(row.get("fillable_size", event.size))))
        take_size = min(remaining, event_size).quantize(Q, rounding=ROUND_HALF_UP)
        consumed_notional = (take_size * Decimal(str(event.trade_price))).quantize(Q, rounding=ROUND_HALF_UP)
        row_copy["consumed_size"] = take_size
        row_copy["consumed_notional"] = consumed_notional
        row_copy["unconsumed_fillable_size"] = max(Decimal("0"), event_size - take_size).quantize(Q, rounding=ROUND_HALF_UP)
        row_copy["consumption_status"] = _schedule_consumption_status(event_size, take_size, remaining)
        annotated.append(row_copy)
        if take_size <= 0:
            continue
        consumed.append(
            ReplayTradeEvent(
                market_id=event.market_id,
                token_id=event.token_id,
                condition_id=event.condition_id,
                block_number=event.block_number,
                transaction_index=event.transaction_index,
                log_index=event.log_index,
                tx_hash=event.tx_hash,
                trade_price=event.trade_price,
                size=take_size,
                maker=event.maker,
                taker=event.taker,
                side_code=event.side_code,
            )
        )
        filled_notional += consumed_notional
        remaining -= take_size
    return consumed, filled_notional.quantize(Q, rounding=ROUND_HALF_UP), annotated


def _schedule_consumption_status(fillable_size: Decimal, consumed_size: Decimal, remaining_before_tick: Decimal) -> str:
    if fillable_size <= 0:
        return "zero_fillable"
    if remaining_before_tick <= 0 or consumed_size <= 0:
        return "unconsumed_after_order_full"
    if consumed_size >= fillable_size:
        return "fully_consumed"
    return "partially_consumed"


def _serialize_fill_schedule(schedule: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cumulative = Decimal("0")
    for row in schedule:
        event = row["event"]
        fillable_size = Decimal(str(row.get("fillable_size") or 0)).quantize(Q, rounding=ROUND_HALF_UP)
        cumulative = (cumulative + fillable_size).quantize(Q, rounding=ROUND_HALF_UP)
        rows.append(
            {
                "sequence": int(row.get("sequence") or len(rows) + 1),
                "market_id": event.market_id,
                "token_id": event.token_id,
                "condition_id": event.condition_id,
                "block_number": event.block_number,
                "transaction_index": event.transaction_index,
                "log_index": event.log_index,
                "tx_hash": event.tx_hash,
                "trade_price": event.trade_price,
                "raw_size": Decimal(str(row.get("raw_size") or event.size)).quantize(Q, rounding=ROUND_HALF_UP),
                "canonical_fill_key": event.canonical_fill_key,
                "canonical_fill_key_kind": event.canonical_fill_key_kind,
                "block_trade_index": int(row.get("block_trade_index") or 0),
                "block_trade_count": int(row.get("block_trade_count") or 0),
                "candidate_block_trade_index": int(row.get("candidate_block_trade_index") or row.get("block_trade_index") or 0),
                "block_open_price": Decimal(str(row.get("block_open_price") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_high_price": Decimal(str(row.get("block_high_price") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_low_price": Decimal(str(row.get("block_low_price") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_close_price": Decimal(str(row.get("block_close_price") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_vwap_price": Decimal(str(row.get("block_vwap_price") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_volume": Decimal(str(row.get("block_volume") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_notional": Decimal(str(row.get("block_notional") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_buy_volume": Decimal(str(row.get("block_buy_volume") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_sell_volume": Decimal(str(row.get("block_sell_volume") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_unknown_side_volume": Decimal(str(row.get("block_unknown_side_volume") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_buy_notional": Decimal(str(row.get("block_buy_notional") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_sell_notional": Decimal(str(row.get("block_sell_notional") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_unknown_side_notional": Decimal(str(row.get("block_unknown_side_notional") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_first_sequence": dict(row.get("block_first_sequence") or {}),
                "block_last_sequence": dict(row.get("block_last_sequence") or {}),
                "liquidity_cap_pct": Decimal(str(row.get("liquidity_cap_pct") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "effective_liquidity_cap_pct": Decimal(str(row.get("effective_liquidity_cap_pct") or row.get("liquidity_cap_pct") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "liquidity_curve": str(row.get("liquidity_curve") or ""),
                "profile_factor": Decimal(str(row.get("profile_factor") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "base_profile_factor": Decimal(str(row.get("base_profile_factor") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "sequence_decay_factor": Decimal(str(row.get("sequence_decay_factor") or 1)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_context_factor": Decimal(str(row.get("block_context_factor") or 1)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_context_reason": str(row.get("block_context_reason") or ""),
                "block_range_pct": Decimal(str(row.get("block_range_pct") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "vwap_dislocation_pct": Decimal(str(row.get("vwap_dislocation_pct") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "tick_quality_factor": Decimal(str(row.get("tick_quality_factor") or 1)).quantize(Q, rounding=ROUND_HALF_UP),
                "tick_quality_reason": str(row.get("tick_quality_reason") or ""),
                "tick_volume_share_pct": Decimal(str(row.get("tick_volume_share_pct") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "remaining_order_size_before_tick": Decimal(str(row.get("remaining_order_size_before_tick") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "remaining_order_size_after_tick": Decimal(str(row.get("remaining_order_size_after_tick") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "requested_block_participation_pct": Decimal(str(row.get("requested_block_participation_pct") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_participation_factor": Decimal(str(row.get("block_participation_factor") or 1)).quantize(Q, rounding=ROUND_HALF_UP),
                "block_participation_reason": str(row.get("block_participation_reason") or ""),
                "maker_queue_factor": Decimal(str(row.get("maker_queue_factor") or 1)).quantize(Q, rounding=ROUND_HALF_UP),
                "maker_queue_reason": str(row.get("maker_queue_reason") or ""),
                "maker_queue_ahead_size": Decimal(str(row.get("maker_queue_ahead_size") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "cancel_pending_factor": Decimal(str(row.get("cancel_pending_factor") or 1)).quantize(Q, rounding=ROUND_HALF_UP),
                "cancel_pending_reason": str(row.get("cancel_pending_reason") or ""),
                "side_compatibility": str(row.get("side_compatibility") or ""),
                "side_compatibility_factor": Decimal(str(row.get("side_compatibility_factor") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "expected_event_side": str(row.get("expected_event_side") or ""),
                "observed_event_side": str(row.get("observed_event_side") or ""),
                "fillable_size": fillable_size,
                "consumed_size": Decimal(str(row.get("consumed_size") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "consumed_notional": Decimal(str(row.get("consumed_notional") or 0)).quantize(Q, rounding=ROUND_HALF_UP),
                "unconsumed_fillable_size": Decimal(
                    str(row.get("unconsumed_fillable_size") if row.get("unconsumed_fillable_size") is not None else fillable_size)
                ).quantize(Q, rounding=ROUND_HALF_UP),
                "consumption_status": str(row.get("consumption_status") or ""),
                "cumulative_fillable_size": cumulative,
                "fill_probability_model": str(row.get("fill_probability_model") or ""),
            }
        )
    return rows


def _build_execution_model_evidence(
    *,
    requested_size: Decimal,
    available_size: Decimal,
    candidates: list[ReplayTradeEvent],
    fill_schedule: list[dict[str, Any]],
    liquidity_cap_pct: Decimal,
    execution_profile: str,
    order_role: str,
    fill_probability: Decimal,
    fill_probability_model: str,
) -> dict[str, Any]:
    """Summarize the actual fill algorithm used for one simulated order.

    The tick schedule is intentionally detailed, but reports and frontends also
    need a stable order-level contract: which profile was used, how raw
    OrderFilled volume was shaped, and whether side/sequence evidence affected
    fills.  This is execution evidence, not a paper/live order-state check.
    """

    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    requested = max(Decimal("0"), Decimal(str(requested_size))).quantize(Q, rounding=ROUND_HALF_UP)
    available = max(Decimal("0"), Decimal(str(available_size))).quantize(Q, rounding=ROUND_HALF_UP)
    raw_candidate_size = sum((max(Decimal("0"), Decimal(str(event.size))) for event in candidates), Decimal("0")).quantize(Q, rounding=ROUND_HALF_UP)
    raw_candidate_notional = sum(
        (max(Decimal("0"), Decimal(str(event.size))) * max(Decimal("0"), Decimal(str(event.trade_price))) for event in candidates),
        Decimal("0"),
    ).quantize(Q, rounding=ROUND_HALF_UP)
    requested_cap = min(Decimal("1"), max(Decimal("0"), Decimal(str(liquidity_cap_pct)) / Decimal("100")))
    effective_cap = _effective_liquidity_cap(requested_cap, profile, role)
    coverage = (available / requested).quantize(Q, rounding=ROUND_HALF_UP) if requested > 0 else Decimal("0").quantize(Q)
    raw_coverage = (raw_candidate_size / requested).quantize(Q, rounding=ROUND_HALF_UP) if requested > 0 else Decimal("0").quantize(Q)
    side_counts: dict[str, int] = {}
    incompatible_ticks = 0
    sequence_adjusted_ticks = 0
    context_discount_ticks = 0
    quality_discount_ticks = 0
    participation_discount_ticks = 0
    maker_queue_adjusted_ticks = 0
    cancel_pending_adjusted_ticks = 0
    fillable_ticks = 0
    consumed_ticks = 0
    partially_consumed_ticks = 0
    unconsumed_fillable_size = Decimal("0")
    consumed_notional = Decimal("0")
    min_profile_factor = Decimal("1")
    min_sequence_factor = Decimal("1")
    min_participation_factor = Decimal("1")
    min_maker_queue_factor = Decimal("1")
    max_maker_queue_ahead_size = Decimal("0")
    max_block_range_pct = Decimal("0")
    max_vwap_dislocation_pct = Decimal("0")
    max_requested_block_participation_pct = Decimal("0")
    dynamic_remaining_pressure_ticks = 0
    min_remaining_order_size_before_tick = requested
    for row in fill_schedule:
        compatibility = str(row.get("side_compatibility") or "")
        if compatibility:
            side_counts[compatibility] = side_counts.get(compatibility, 0) + 1
        if compatibility == "incompatible_discounted":
            incompatible_ticks += 1
        sequence_factor = Decimal(str(row.get("sequence_decay_factor") or 1))
        profile_factor = Decimal(str(row.get("profile_factor") or 1))
        context_factor = Decimal(str(row.get("block_context_factor") or 1))
        quality_factor = Decimal(str(row.get("tick_quality_factor") or 1))
        participation_factor = Decimal(str(row.get("block_participation_factor") or 1))
        maker_queue_factor = Decimal(str(row.get("maker_queue_factor") or 1))
        maker_queue_ahead_size = Decimal(str(row.get("maker_queue_ahead_size") or 0))
        cancel_pending_factor = Decimal(str(row.get("cancel_pending_factor") or 1))
        if sequence_factor < Decimal("1"):
            sequence_adjusted_ticks += 1
        if context_factor < Decimal("1"):
            context_discount_ticks += 1
        if quality_factor < Decimal("1"):
            quality_discount_ticks += 1
        if participation_factor < Decimal("1"):
            participation_discount_ticks += 1
        remaining_before_tick = Decimal(str(row.get("remaining_order_size_before_tick") or 0))
        if remaining_before_tick < requested:
            dynamic_remaining_pressure_ticks += 1
        if maker_queue_factor < Decimal("1") or maker_queue_ahead_size > 0:
            maker_queue_adjusted_ticks += 1
        if cancel_pending_factor < Decimal("1"):
            cancel_pending_adjusted_ticks += 1
        if Decimal(str(row.get("fillable_size") or 0)) > 0:
            fillable_ticks += 1
        consumed_size = Decimal(str(row.get("consumed_size") or 0))
        if consumed_size > 0:
            consumed_ticks += 1
            consumed_notional += Decimal(str(row.get("consumed_notional") or 0))
        if str(row.get("consumption_status") or "") == "partially_consumed":
            partially_consumed_ticks += 1
        unconsumed_fillable_size += Decimal(str(row.get("unconsumed_fillable_size") or 0))
        min_profile_factor = min(min_profile_factor, profile_factor)
        min_sequence_factor = min(min_sequence_factor, sequence_factor)
        min_participation_factor = min(min_participation_factor, participation_factor)
        min_maker_queue_factor = min(min_maker_queue_factor, maker_queue_factor)
        max_maker_queue_ahead_size = max(max_maker_queue_ahead_size, maker_queue_ahead_size)
        max_block_range_pct = max(max_block_range_pct, Decimal(str(row.get("block_range_pct") or 0)))
        max_vwap_dislocation_pct = max(max_vwap_dislocation_pct, Decimal(str(row.get("vwap_dislocation_pct") or 0)))
        max_requested_block_participation_pct = max(
            max_requested_block_participation_pct,
            Decimal(str(row.get("requested_block_participation_pct") or 0)),
        )
        min_remaining_order_size_before_tick = min(min_remaining_order_size_before_tick, remaining_before_tick)
    return {
        "model_version": "orderfilled_tick_execution_v4",
        "execution_source": "orderfilled_limit_replay",
        "execution_profile": profile,
        "order_role": role,
        "fill_probability_model": fill_probability_model,
        "fill_probability_algorithm": _fill_probability_algorithm_name(profile, role),
        "fill_probability": Decimal(str(fill_probability)).quantize(Q, rounding=ROUND_HALF_UP),
        "requested_size": requested,
        "raw_candidate_size": raw_candidate_size,
        "raw_candidate_notional": raw_candidate_notional,
        "available_size_after_model": available,
        "coverage_ratio": coverage,
        "raw_coverage_ratio": raw_coverage,
        "requested_liquidity_cap_pct": (requested_cap * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP),
        "effective_liquidity_cap_pct": (effective_cap * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP),
        "liquidity_curve": _liquidity_curve_name(profile, role),
        "base_profile_factor": _profile_role_fill_factor(profile, role).quantize(Q, rounding=ROUND_HALF_UP),
        "candidate_tick_count": len(candidates),
        "fillable_tick_count": fillable_ticks,
        "consumed_tick_count": consumed_ticks,
        "partially_consumed_tick_count": partially_consumed_ticks,
        "unconsumed_fillable_size": unconsumed_fillable_size.quantize(Q, rounding=ROUND_HALF_UP),
        "consumed_notional": consumed_notional.quantize(Q, rounding=ROUND_HALF_UP),
        "sequence_adjusted_tick_count": sequence_adjusted_ticks,
        "block_context_discount_tick_count": context_discount_ticks,
        "tick_quality_discount_tick_count": quality_discount_ticks,
        "block_participation_discount_tick_count": participation_discount_ticks,
        "maker_queue_adjusted_tick_count": maker_queue_adjusted_ticks,
        "cancel_pending_adjusted_tick_count": cancel_pending_adjusted_ticks,
        "incompatible_side_tick_count": incompatible_ticks,
        "side_compatibility_counts": dict(sorted(side_counts.items())),
        "min_profile_factor": min_profile_factor.quantize(Q, rounding=ROUND_HALF_UP),
        "min_sequence_decay_factor": min_sequence_factor.quantize(Q, rounding=ROUND_HALF_UP),
        "min_block_participation_factor": min_participation_factor.quantize(Q, rounding=ROUND_HALF_UP),
        "min_maker_queue_factor": min_maker_queue_factor.quantize(Q, rounding=ROUND_HALF_UP),
        "max_maker_queue_ahead_size": max_maker_queue_ahead_size.quantize(Q, rounding=ROUND_HALF_UP),
        "max_block_range_pct": max_block_range_pct.quantize(Q, rounding=ROUND_HALF_UP),
        "max_vwap_dislocation_pct": max_vwap_dislocation_pct.quantize(Q, rounding=ROUND_HALF_UP),
        "max_requested_block_participation_pct": max_requested_block_participation_pct.quantize(Q, rounding=ROUND_HALF_UP),
        "min_remaining_order_size_before_tick": min_remaining_order_size_before_tick.quantize(Q, rounding=ROUND_HALF_UP),
        "dynamic_remaining_pressure_tick_count": dynamic_remaining_pressure_ticks,
        "uses_canonical_trade_ticks": any(event.canonical_fill_key_kind == "canonical" for event in candidates),
        "uses_block_ohlcv_vwap": bool(fill_schedule),
        "uses_same_block_order_sequence": any(int(row.get("block_trade_count") or 0) > 1 for row in fill_schedule),
        "uses_block_participation_pressure": participation_discount_ticks > 0,
        "uses_dynamic_remaining_order_pressure": dynamic_remaining_pressure_ticks > 0,
        "uses_orderfilled_maker_queue_proxy": maker_queue_adjusted_ticks > 0,
        "uses_cancel_pending_race_discount": cancel_pending_adjusted_ticks > 0,
        "uses_side_attribution": any(str(row.get("observed_event_side") or "") for row in fill_schedule),
    }


def _fill_probability_algorithm_name(execution_profile: str, order_role: str) -> str:
    profile = normalize_execution_profile(execution_profile)
    role = normalize_order_role(order_role)
    if profile == "optimistic":
        return f"full_cross_linear_volume_{role}"
    if profile == "neutral":
        return f"balanced_orderfilled_tick_coverage_{role}"
    if profile == "realistic":
        return f"realistic_orderfilled_tick_haircut_{role}"
    if profile == "conservative":
        return f"conservative_orderfilled_tick_haircut_{role}"
    return f"stress_tail_orderfilled_tick_haircut_{role}"
