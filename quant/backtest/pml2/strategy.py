"""Causal strategy API for dynamic PML2 replay."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from .book import EconomicResidualBook
from .contracts import (
    BookDeltaEvent,
    BookFrameBatchEvent,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    EconomicBookSide,
    EventEnvelope,
    EventType,
    ExecutionMatch,
    MarketLifecycleEvent,
    Pml2OrderIntent,
    Pml2OrderResult,
    TradeEvent,
)
from .session import ReplayExecutionSession


@dataclass(frozen=True)
class ObservedLevel:
    side: EconomicBookSide
    canonical_yes_price: Decimal
    available_size: Decimal
    displayed_size: Decimal
    book_epoch: int
    snapshot_id: str
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class ObservedCondition:
    condition_id: str
    sync_state: str
    trading_mode: str
    book_epoch: int
    last_exchange_ts: datetime | None
    snapshot_id: str
    depth_truncated: bool
    depth_scope: str


class ObservedBookView:
    """Immutable projections of the locally delivered book state."""

    def __init__(self, book: EconomicResidualBook) -> None:
        self._book = book

    def condition(self, condition_id: str) -> ObservedCondition:
        state = self._book.condition(condition_id)
        return ObservedCondition(
            condition_id=state.condition_id,
            sync_state=state.sync_state.value,
            trading_mode=state.trading_mode.value,
            book_epoch=state.book_epoch,
            last_exchange_ts=state.last_exchange_ts,
            snapshot_id=state.last_snapshot_id,
            depth_truncated=state.depth_truncated,
            depth_scope=state.depth_scope,
        )

    def levels(
        self, condition_id: str, side: EconomicBookSide
    ) -> tuple[ObservedLevel, ...]:
        normalized = str(condition_id).lower()
        state = self._book.condition(normalized)
        rows = [
            ObservedLevel(
                side=key.side,
                canonical_yes_price=key.canonical_yes_price,
                available_size=level.available_size,
                displayed_size=level.displayed_size,
                book_epoch=key.book_epoch,
                snapshot_id=level.snapshot_id,
                source_event_ids=level.source_event_ids,
            )
            for key, level in self._book.levels.items()
            if key.condition_id == normalized
            and key.book_epoch == state.book_epoch
            and key.side == side
            and level.available_size > 0
            and level.mirror_valid
        ]
        rows.sort(
            key=lambda item: item.canonical_yes_price,
            reverse=side == EconomicBookSide.BID,
        )
        return tuple(rows)

    def best(
        self, condition_id: str, side: EconomicBookSide
    ) -> ObservedLevel | None:
        rows = self.levels(condition_id, side)
        return rows[0] if rows else None


class PredictionStrategy(Protocol):
    strategy_id: str

    def on_book(
        self,
        ctx: ObservedStrategyContext,
        event: (
            BookSnapshotEvent
            | BookFrameBatchEvent
            | BookLevelBatchEvent
            | BookDeltaEvent
        ),
    ) -> None: ...

    def on_trade(
        self, ctx: ObservedStrategyContext, event: TradeEvent
    ) -> None: ...

    def on_external_event(
        self, ctx: ObservedStrategyContext, event: EventEnvelope
    ) -> None: ...

    def on_order_update(
        self, ctx: ObservedStrategyContext, update: ExecutionMatch
    ) -> None: ...

    def on_market_status(
        self, ctx: ObservedStrategyContext, event: MarketLifecycleEvent
    ) -> None: ...


class ObservedStrategyContext:
    """Strategy-facing context that never exposes ExchangeBookState."""

    def __init__(
        self,
        *,
        session: ReplayExecutionSession,
        strategy_id: str,
        now: datetime,
        observed_orders: dict[str, Pml2OrderResult],
    ) -> None:
        self._session = session
        self.strategy_id = str(strategy_id)
        self.now = now
        self.book = ObservedBookView(session.observed_book)
        self._observed_orders = observed_orders

    @property
    def run_id(self) -> str:
        return self._session.run_id

    def information_available(self, event_id: str) -> bool:
        return self._session.information_available(event_id, self.now)

    def order(self, order_id: str) -> Pml2OrderResult | None:
        return self._observed_orders.get(str(order_id))

    def submit_order(self, order: Pml2OrderIntent) -> None:
        if order.strategy_id != self.strategy_id:
            raise ValueError("strategy may submit only its own orders")
        if order.run_id != self.run_id:
            raise ValueError("strategy order run_id does not match replay")
        if order.signal_ts > self.now or order.observed_ts > self.now:
            raise ValueError("strategy order depends on information not yet observed")
        if order.submit_ts < self.now:
            raise ValueError("strategy cannot submit an order retroactively")
        self._session.submit_order(order)

    def cancel_order(
        self,
        order_id: str,
        *,
        cancel_latency_ms: int | None = None,
    ) -> None:
        state = self._session.orders.get(str(order_id))
        if state is None or state.order.strategy_id != self.strategy_id:
            raise ValueError("strategy may cancel only its own known orders")
        self._session.submit_cancel(
            str(order_id),
            signal_ts=self.now,
            cancel_latency_ms=cancel_latency_ms,
        )


class DynamicPredictionReplay:
    """Dispatch locally delivered events to strategies inside one session."""

    def __init__(
        self,
        *,
        session: ReplayExecutionSession,
        strategies: tuple[PredictionStrategy, ...],
    ) -> None:
        self.session = session
        self.strategies = tuple(strategies)
        ids = [str(item.strategy_id) for item in self.strategies]
        if not ids or any(not item for item in ids):
            raise ValueError("at least one identified strategy is required")
        if len(ids) != len(set(ids)):
            raise ValueError("strategy_id values must be unique")
        self._observed_orders: dict[str, dict[str, Pml2OrderResult]] = {
            strategy_id: {} for strategy_id in ids
        }
        session.add_local_event_listener(self._on_local_event)

    def run(self, *, until: datetime | None = None) -> ReplayExecutionSession:
        self.session.run(until=until)
        return self.session

    def _on_local_event(self, envelope: EventEnvelope, at: datetime) -> None:
        for strategy in self.strategies:
            ctx = ObservedStrategyContext(
                session=self.session,
                strategy_id=strategy.strategy_id,
                now=at,
                observed_orders=self._observed_orders[strategy.strategy_id],
            )
            self._dispatch(strategy, ctx, envelope)

    def _dispatch(
        self,
        strategy: PredictionStrategy,
        ctx: ObservedStrategyContext,
        envelope: EventEnvelope,
    ) -> None:
        payload: Any = envelope.payload
        if envelope.event_type in {
            EventType.BOOK_SNAPSHOT,
            EventType.BOOK_FRAME_BATCH,
            EventType.BOOK_LEVEL_BATCH,
            EventType.BOOK_DELTA,
        }:
            self._call(strategy, "on_book", ctx, payload)
        elif envelope.event_type == EventType.TRADE:
            self._call(strategy, "on_trade", ctx, payload)
        elif envelope.event_type == EventType.EXTERNAL_SIGNAL:
            self._call(strategy, "on_external_event", ctx, envelope)
        elif envelope.event_type == EventType.MARKET_LIFECYCLE:
            self._call(strategy, "on_market_status", ctx, payload)
        elif envelope.event_type == EventType.RESPONSE:
            fill_id = str(payload["fill_id"])
            match = next(
                (item for item in self.session.matches if item.fill_id == fill_id),
                None,
            )
            if match is None or match.order_id not in self.session.orders:
                raise RuntimeError("venue response references an unknown fill")
            order_state = self.session.orders[match.order_id]
            if order_state.order.strategy_id != strategy.strategy_id:
                return
            self._observed_orders[strategy.strategy_id][match.order_id] = (
                self.session.result(match.order_id)
            )
            self._call(strategy, "on_order_update", ctx, match)

    @staticmethod
    def _call(strategy: PredictionStrategy, name: str, *args: Any) -> None:
        callback = getattr(strategy, name, None)
        if callback is not None:
            callback(*args)
