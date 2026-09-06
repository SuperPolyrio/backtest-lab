"""Economic-level maker FIFO shared by YES/NO mirror orders."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from .book import (
    EconomicLevelKey,
    canonical_action,
    canonical_yes_price,
    consumed_side,
    resting_side,
    token_price,
)
from .contracts import (
    EconomicAction,
    EconomicBookSide,
    Pml2OrderIntent,
    TradeEvent,
    qty,
)
from .profiles import Pml2Profile


class MakerTemporalConflict(RuntimeError):
    """Late source evidence cannot be applied to the current maker queue safely."""


@dataclass
class MakerOrderState:
    order: Pml2OrderIntent
    key: EconomicLevelKey
    accepted_ts: datetime
    arrival_sequence: int
    remaining_size: Decimal
    queue_ahead: Decimal
    status: str = "WORKING"
    filled_size: Decimal = Decimal(0)


@dataclass
class MakerLevelQueue:
    key: EconomicLevelKey
    external_queue_ahead: Decimal
    orders: list[MakerOrderState] = field(default_factory=list)


@dataclass(frozen=True)
class MakerFillAllocation:
    order: Pml2OrderIntent
    size: Decimal
    raw_price: Decimal
    canonical_yes_price: Decimal
    queue_ahead_before: Decimal
    queue_ahead_after: Decimal
    source_event_id: str
    exchange_ts: datetime


class EconomicMakerQueueLedger:
    def __init__(self, profile: Pml2Profile) -> None:
        self.profile = profile
        self.queues: dict[EconomicLevelKey, MakerLevelQueue] = {}
        self.orders: dict[str, MakerOrderState] = {}
        self._arrival_sequence = 0

    def admit(
        self,
        order: Pml2OrderIntent,
        *,
        accepted_ts: datetime,
        book_epoch: int,
        displayed_external_size: Decimal,
        remaining_size: Decimal | None = None,
    ) -> MakerOrderState:
        action = canonical_action(order.outcome, order.side)
        key = EconomicLevelKey(
            condition_id=order.condition_id.lower(),
            book_epoch=book_epoch,
            side=resting_side(action),
            canonical_yes_price=canonical_yes_price(
                order.outcome, order.limit_price
            ),
        )
        queue = self.queues.get(key)
        if queue is None:
            queue = MakerLevelQueue(
                key=key,
                external_queue_ahead=qty(
                    displayed_external_size * self.profile.queue_ahead_fraction
                ),
            )
            self.queues[key] = queue
        self._arrival_sequence += 1
        own_ahead = sum(
            (
                item.remaining_size
                for item in queue.orders
                if item.status in {"WORKING", "PARTIAL"}
            ),
            Decimal(0),
        )
        state = MakerOrderState(
            order=order,
            key=key,
            accepted_ts=accepted_ts,
            arrival_sequence=self._arrival_sequence,
            remaining_size=qty(
                order.effective_requested_amount
                if remaining_size is None
                else remaining_size
            ),
            queue_ahead=qty(queue.external_queue_ahead + own_ahead),
        )
        queue.orders.append(state)
        self.orders[order.order_id] = state
        self._refresh_queue_ahead(queue)
        return state

    def cancel(self, order_id: str) -> bool:
        state = self.orders.get(order_id)
        if state is None or state.status not in {"WORKING", "PARTIAL"}:
            return False
        state.status = "CANCELLED"
        state.remaining_size = Decimal(0)
        self._refresh_queue_ahead(self.queues[state.key])
        return True

    def expire(self, order_id: str) -> bool:
        state = self.orders.get(order_id)
        if state is None or state.status not in {"WORKING", "PARTIAL"}:
            return False
        state.status = "EXPIRED"
        state.remaining_size = Decimal(0)
        self._refresh_queue_ahead(self.queues[state.key])
        return True

    def process_trade(self, event: TradeEvent) -> tuple[MakerFillAllocation, ...]:
        action = canonical_action(event.outcome, event.aggressor_side)
        passive = consumed_side(action)
        trade_price = canonical_yes_price(event.outcome, event.price)
        keys = [
            key
            for key in self.queues
            if key.condition_id == event.condition_id.lower()
            and key.book_epoch == event.book_epoch
            and key.side == passive
            and self._trade_reaches(action, trade_price, key.canonical_yes_price)
        ]
        keys.sort(
            key=lambda key: key.canonical_yes_price,
            reverse=passive == EconomicBookSide.BID,
        )
        flow = event.size
        fills: list[MakerFillAllocation] = []
        for key in keys:
            if flow <= 0:
                break
            queue = self.queues[key]
            eligible = [
                state
                for state in queue.orders
                if state.status in {"WORKING", "PARTIAL"}
                and state.accepted_ts <= event.exchange_ts
            ]
            future = [
                state
                for state in queue.orders
                if state.status in {"WORKING", "PARTIAL"}
                and state.accepted_ts > event.exchange_ts
            ]
            if not eligible:
                continue
            if future:
                raise MakerTemporalConflict(
                    "late trade predates part of the current maker queue: "
                    f"event_id={event.event_id}"
                )
            external = min(queue.external_queue_ahead, flow)
            queue.external_queue_ahead = qty(queue.external_queue_ahead - external)
            flow = qty(flow - external)
            for state in queue.orders:
                if flow <= 0:
                    break
                if state.status not in {"WORKING", "PARTIAL"}:
                    continue
                if state.accepted_ts > event.exchange_ts:
                    continue
                before = state.queue_ahead
                take = qty(min(state.remaining_size, flow))
                if take <= 0:
                    continue
                state.remaining_size = qty(state.remaining_size - take)
                state.filled_size = qty(state.filled_size + take)
                flow = qty(flow - take)
                state.status = "FILLED" if state.remaining_size <= 0 else "PARTIAL"
                fills.append(
                    MakerFillAllocation(
                        order=state.order,
                        size=take,
                        raw_price=token_price(
                            state.order.outcome, key.canonical_yes_price
                        ),
                        canonical_yes_price=key.canonical_yes_price,
                        queue_ahead_before=before,
                        queue_ahead_after=Decimal(0),
                        source_event_id=event.event_id,
                        exchange_ts=event.exchange_ts,
                    )
                )
            self._refresh_queue_ahead(queue)
        return tuple(fills)

    def on_unmatched_book_decrease(
        self,
        key: EconomicLevelKey,
        decrease: Decimal,
        *,
        event_ts: datetime | None = None,
    ) -> Decimal:
        queue = self.queues.get(key)
        if queue is None:
            return Decimal(0)
        if event_ts is not None:
            eligible = [
                state
                for state in queue.orders
                if state.status in {"WORKING", "PARTIAL"}
                and state.accepted_ts <= event_ts
            ]
            future = [
                state
                for state in queue.orders
                if state.status in {"WORKING", "PARTIAL"}
                and state.accepted_ts > event_ts
            ]
            if not eligible:
                return Decimal(0)
            if future:
                raise MakerTemporalConflict(
                    "late book decrease predates part of the current maker queue"
                )
        allocated = qty(decrease * self.profile.cancel_ahead_fraction)
        actual = min(queue.external_queue_ahead, allocated)
        queue.external_queue_ahead = qty(queue.external_queue_ahead - actual)
        self._refresh_queue_ahead(queue)
        return actual

    def queue_ahead(self, order_id: str) -> Decimal | None:
        state = self.orders.get(order_id)
        return state.queue_ahead if state is not None else None

    @staticmethod
    def _trade_reaches(
        action: EconomicAction,
        trade_price: Decimal,
        resting_price: Decimal,
    ) -> bool:
        if action == EconomicAction.SELL:
            return resting_price >= trade_price
        return resting_price <= trade_price

    @staticmethod
    def _refresh_queue_ahead(queue: MakerLevelQueue) -> None:
        ahead = max(Decimal(0), queue.external_queue_ahead)
        for state in sorted(queue.orders, key=lambda item: item.arrival_sequence):
            if state.status not in {"WORKING", "PARTIAL"}:
                continue
            state.queue_ahead = qty(max(Decimal(0), ahead))
            ahead += state.remaining_size
