"""Streaming state shared by frozen-order and dynamic Fill-only replay."""

from __future__ import annotations

import base64
import hashlib
import heapq
import json
import pickle
import random
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal, Protocol, TypeAlias, cast

from quant.backtest.orderfilled_v2_replay import (
    CapacityLedger,
    EventKey,
    V2Fill,
    V2OrderResult,
    V2TakerOrder,
    V2TradePrint,
    replay_v2_taker_orders_with_diagnostics,
)
from quant.backtest.prepared_trade_tape import PreparedTradeTape, prepare_trade_tape
from quant.backtest.trade_only_v3.engine import (
    RunLiquidityLedger,
    replay_trade_only_orders_with_diagnostics,
)
from quant.backtest.trade_only_v3.models import (
    TradeOnlyFill,
    TradeOnlyOrder,
    TradeOnlyOrderResult,
)

ExecutionFamily = Literal["V2", "V3"]
ReplayOrder: TypeAlias = V2TakerOrder | TradeOnlyOrder
ReplayOrderResult: TypeAlias = V2OrderResult | TradeOnlyOrderResult


class DynamicTradeStrategy(Protocol):
    def on_trade(
        self, trade: V2TradePrint, context: ReplayStrategyContext
    ) -> Iterable[ReplayOrder]: ...


class ReplayTradeCatalog(Protocol):
    source_pin: str
    clickhouse_query_count: int

    @property
    def row_count(self) -> int: ...

    def prepare_for_orders(
        self, orders: Sequence[ReplayOrder]
    ) -> PreparedTradeTape[V2TradePrint]: ...

    def iter_aligned_chunks(
        self, *, target_rows: int
    ) -> Iterable[tuple[V2TradePrint, ...]]: ...


@dataclass
class ReplayLedgerBook:
    cash_delta: Decimal = Decimal(0)
    positions: dict[str, Decimal] = field(default_factory=dict)
    fill_count: int = 0
    filled_notional: Decimal = Decimal(0)

    def apply_fill(
        self, *, asset_id: str, side: str, size: Decimal, price: Decimal
    ) -> None:
        quantity = Decimal(size)
        notional = quantity * Decimal(price)
        direction = Decimal(1) if str(side).upper() == "BUY" else Decimal(-1)
        self.positions[asset_id.lower()] = (
            self.positions.get(asset_id.lower(), Decimal(0)) + direction * quantity
        )
        self.cash_delta += -direction * notional
        self.fill_count += 1
        self.filled_notional += notional

    def as_dict(self) -> dict[str, Any]:
        return {
            "cash_delta": str(self.cash_delta),
            "positions": {
                key: str(value) for key, value in sorted(self.positions.items())
            },
            "fill_count": self.fill_count,
            "filled_notional": str(self.filled_notional),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ReplayLedgerBook:
        return cls(
            cash_delta=Decimal(str(payload.get("cash_delta") or "0")),
            positions={
                str(key): Decimal(str(value))
                for key, value in dict(payload.get("positions") or {}).items()
            },
            fill_count=int(payload.get("fill_count") or 0),
            filled_notional=Decimal(str(payload.get("filled_notional") or "0")),
        )


@dataclass
class ReplayAccountingState:
    source_confirmed: ReplayLedgerBook = field(default_factory=ReplayLedgerBook)
    inferred: ReplayLedgerBook = field(default_factory=ReplayLedgerBook)
    modeled: ReplayLedgerBook = field(default_factory=ReplayLedgerBook)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_confirmed": self.source_confirmed.as_dict(),
            "inferred": self.inferred.as_dict(),
            "modeled": self.modeled.as_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ReplayAccountingState:
        return cls(
            source_confirmed=ReplayLedgerBook.from_dict(
                payload.get("source_confirmed") or {}
            ),
            inferred=ReplayLedgerBook.from_dict(payload.get("inferred") or {}),
            modeled=ReplayLedgerBook.from_dict(payload.get("modeled") or {}),
        )


@dataclass(frozen=True)
class ReplayStrategyContext:
    now: datetime
    trade: V2TradePrint | None
    accounting: ReplayAccountingState
    strategy_state: dict[str, Any]
    settlement_state: dict[str, Any]
    rng: random.Random
    high_watermark: EventKey | None


@dataclass(frozen=True)
class ReplayRunReceipt:
    schema_version: str
    run_id: str
    replay_mode: str
    execution_family: ExecutionFamily
    profile: str | None
    chunks_processed: int
    events_processed: int
    orders_generated: int
    results_count: int
    catalog_rows: int
    high_watermark: dict[str, Any] | None
    accounting: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class InMemoryTradeCatalog:
    """Read-only catalog used by tests and bounded local studies."""

    def __init__(
        self,
        trades: Iterable[V2TradePrint],
        *,
        source_pin: str = "in-memory",
    ) -> None:
        self.source_pin = source_pin
        self.prepared_tape = prepare_trade_tape(trades)
        self.clickhouse_query_count = 0

    @property
    def ordered_trades(self) -> Sequence[V2TradePrint]:
        return self.prepared_tape.ordered_trades

    @property
    def row_count(self) -> int:
        return self.prepared_tape.trade_rows_indexed

    def prepare_for_orders(
        self, orders: Sequence[ReplayOrder]
    ) -> PreparedTradeTape[V2TradePrint]:
        del orders
        return self.prepared_tape

    def iter_aligned_chunks(
        self, *, target_rows: int
    ) -> Iterable[tuple[V2TradePrint, ...]]:
        return timestamp_aligned_chunks(self.ordered_trades, target_rows=target_rows)


class FrozenOrderStrategy:
    """Emit already frozen orders when their signal trade is replayed."""

    def __init__(self, orders: Iterable[ReplayOrder]) -> None:
        self._by_trade_id: dict[str, list[ReplayOrder]] = {}
        self._by_event: dict[tuple[int, datetime, int, str], list[ReplayOrder]] = {}
        for order in orders:
            source_id = getattr(order, "signal_source_trade_id", None)
            if source_id:
                self._by_trade_id.setdefault(str(source_id), []).append(order)
                continue
            signal_block = getattr(order, "signal_block", None)
            signal_ts = getattr(order, "signal_ts", None)
            if signal_block is None or signal_ts is None:
                raise ValueError(
                    "frozen dynamic order needs signal source or block/time"
                )
            key = (
                int(signal_block),
                _utc(signal_ts),
                int(order.market_id),
                str(order.asset_id).lower(),
            )
            self._by_event.setdefault(key, []).append(order)

    def on_trade(
        self, trade: V2TradePrint, context: ReplayStrategyContext
    ) -> Iterable[ReplayOrder]:
        del context
        direct = self._by_trade_id.get(trade.trade_id, ())
        key = (
            trade.block_number,
            _utc(trade.block_time),
            trade.market_id,
            trade.asset_id.lower(),
        )
        return (*direct, *self._by_event.get(key, ()))


class ReplaySession:
    """Keep strategy and execution state alive while market chunks are replaced."""

    def __init__(
        self,
        *,
        run_id: str,
        execution_family: ExecutionFamily,
        catalog: ReplayTradeCatalog,
        profile: str | None = None,
        random_seed: int = 0,
    ) -> None:
        if execution_family == "V3" and not profile:
            raise ValueError("V3 ReplaySession requires a profile")
        self.run_id = str(run_id)
        self.execution_family = execution_family
        self.catalog = catalog
        self.profile = profile
        self.random_seed = int(random_seed)
        self.current_chunk: tuple[V2TradePrint, ...] = ()
        self.current_tape: PreparedTradeTape[V2TradePrint] | None = None
        self.strategy: DynamicTradeStrategy | None = None
        self.reset_run()

    def reset_run(self) -> None:
        """Reset mutable run state while retaining the immutable catalog/index."""

        self.v2_ledger = CapacityLedger()
        self.v3_ledger = RunLiquidityLedger(f"{self.run_id}:v3")
        self.accounting = ReplayAccountingState()
        self.strategy_state: dict[str, Any] = {}
        self.settlement_state: dict[str, Any] = {}
        self.rng = random.Random(self.random_seed)
        self.high_watermark: EventKey | None = None
        self.active_orders: dict[str, ReplayOrder] = {}
        self.orders: list[ReplayOrder] = []
        self.results: list[ReplayOrderResult] = []
        self._order_by_id: dict[str, ReplayOrder] = {}
        self._scheduled: list[
            tuple[datetime, tuple[int, int, str, int, str], int, str, Any]
        ] = []
        self._schedule_serial = 0
        self._source_sequences: dict[str, tuple[int, int, str, int, str]] = {}
        self._chunks_processed = 0
        self._events_processed = 0
        self.clear_market_data()
        reset = getattr(self.strategy, "reset", None)
        if callable(reset):
            reset()

    def load_market_data(self, trades: Sequence[V2TradePrint]) -> None:
        self.current_chunk = tuple(trades)
        self.current_tape = prepare_trade_tape(self.current_chunk)

    def clear_market_data(self) -> None:
        self.current_chunk = ()
        self.current_tape = None

    def snapshot(self) -> dict[str, Any]:
        """Create a checksummed chunk-boundary checkpoint, including RNG state."""

        if self.current_chunk:
            raise RuntimeError(
                "ReplaySession snapshots are only valid at chunk boundaries"
            )
        strategy_snapshot = None
        snapshot_method = getattr(self.strategy, "snapshot_state", None)
        if callable(snapshot_method):
            strategy_snapshot = snapshot_method()
        opaque_state = {
            "strategy_state": self.strategy_state,
            "settlement_state": self.settlement_state,
            "rng_state": self.rng.getstate(),
            "active_orders": self.active_orders,
            "orders": self.orders,
            "results": self.results,
            "order_by_id": self._order_by_id,
            "scheduled": self._scheduled,
            "source_sequences": self._source_sequences,
            "strategy_snapshot": strategy_snapshot,
        }
        encoded = base64.b64encode(
            pickle.dumps(opaque_state, protocol=pickle.HIGHEST_PROTOCOL)
        ).decode("ascii")
        draft = {
            "schema_version": "UnifiedFillOnlyReplaySessionSnapshotV1",
            "run_id": self.run_id,
            "execution_family": self.execution_family,
            "profile": self.profile,
            "source_pin": self.catalog.source_pin,
            "random_seed": self.random_seed,
            "high_watermark": asdict(self.high_watermark)
            if self.high_watermark is not None
            else None,
            "chunks_processed": self._chunks_processed,
            "events_processed": self._events_processed,
            "schedule_serial": self._schedule_serial,
            "accounting": self.accounting.as_dict(),
            "v2_ledger": self.v2_ledger.snapshot(),
            "v3_ledger": self.v3_ledger.snapshot(),
            "opaque_state_b64": encoded,
        }
        return {**draft, "snapshot_sha256": _canonical_sha256(draft)}

    def restore_snapshot(
        self,
        payload: Mapping[str, Any],
        *,
        strategy: DynamicTradeStrategy | None = None,
    ) -> None:
        """Restore a trusted local checkpoint into the existing catalog session."""

        snapshot = dict(payload)
        digest = str(snapshot.pop("snapshot_sha256", ""))
        if not digest or digest != _canonical_sha256(snapshot):
            raise ValueError("ReplaySession snapshot checksum mismatch")
        for key, expected in (
            ("schema_version", "UnifiedFillOnlyReplaySessionSnapshotV1"),
            ("run_id", self.run_id),
            ("execution_family", self.execution_family),
            ("profile", self.profile),
            ("source_pin", self.catalog.source_pin),
        ):
            if snapshot.get(key) != expected:
                raise ValueError(f"ReplaySession snapshot {key} mismatch")
        opaque = pickle.loads(base64.b64decode(snapshot["opaque_state_b64"]))
        self.strategy = strategy or self.strategy
        self.strategy_state = dict(opaque["strategy_state"])
        self.settlement_state = dict(opaque["settlement_state"])
        self.rng = random.Random()
        self.rng.setstate(opaque["rng_state"])
        self.active_orders = dict(opaque["active_orders"])
        self.orders = list(opaque["orders"])
        self.results = list(opaque["results"])
        self._order_by_id = dict(opaque["order_by_id"])
        self._scheduled = list(opaque["scheduled"])
        heapq.heapify(self._scheduled)
        self._source_sequences = dict(opaque["source_sequences"])
        self._schedule_serial = int(snapshot["schedule_serial"])
        self._chunks_processed = int(snapshot["chunks_processed"])
        self._events_processed = int(snapshot["events_processed"])
        high = snapshot.get("high_watermark")
        self.high_watermark = EventKey(**high) if high is not None else None
        self.accounting = ReplayAccountingState.from_dict(snapshot["accounting"])
        self.v2_ledger = CapacityLedger.from_snapshot(snapshot["v2_ledger"])
        self.v3_ledger = RunLiquidityLedger.from_snapshot(snapshot["v3_ledger"])
        restore_method = getattr(self.strategy, "restore_state", None)
        if callable(restore_method) and opaque.get("strategy_snapshot") is not None:
            restore_method(opaque["strategy_snapshot"])
        self.clear_market_data()

    def replay_frozen_orders(
        self, orders: Iterable[ReplayOrder]
    ) -> tuple[list[ReplayOrderResult], ReplayRunReceipt]:
        order_rows = list(orders)
        results = self._execute_orders(order_rows)
        for order, result in zip(order_rows, results, strict=True):
            self.orders.append(order)
            self._order_by_id[order.order_id] = order
            self.results.append(result)
            self._apply_result_now(order, result)
        return results, self._receipt("FrozenOrderReplay")

    def replay_dynamic(
        self,
        strategy: DynamicTradeStrategy,
        *,
        chunk_size: int,
        max_chunks: int | None = None,
    ) -> tuple[list[ReplayOrderResult], ReplayRunReceipt]:
        self.strategy = strategy
        chunks_this_call = 0
        stopped_early = False
        for chunk in self.catalog.iter_aligned_chunks(target_rows=chunk_size):
            pending_chunk = tuple(
                trade
                for trade in chunk
                if self.high_watermark is None
                or trade.sequence > _event_key_sequence(self.high_watermark)
            )
            if not pending_chunk:
                continue
            self.load_market_data(pending_chunk)
            self._chunks_processed += 1
            chunks_this_call += 1
            for trade in pending_chunk:
                self._source_sequences[trade.trade_id] = trade.sequence
                self._flush_scheduled(trade.block_time, trade.sequence)
                context = self._strategy_context(trade.block_time, trade)
                generated = tuple(strategy.on_trade(trade, context) or ())
                for raw_order in generated:
                    order = self._bind_signal_source(raw_order, trade)
                    result = self._execute_orders([order])[0]
                    self.orders.append(order)
                    self.results.append(result)
                    self._order_by_id[order.order_id] = order
                    self.active_orders[order.order_id] = order
                    self._schedule_result(order, result)
                self.high_watermark = trade.event_key
                self._events_processed += 1
            self.clear_market_data()
            if max_chunks is not None and chunks_this_call >= max(1, int(max_chunks)):
                stopped_early = True
                break
        if not stopped_early:
            self._flush_scheduled(
                datetime.max.replace(tzinfo=timezone.utc),
                (2**63 - 1, 2**31 - 1, "~", 2**31 - 1, "~"),
            )
        return list(self.results), self._receipt("DynamicStrategyReplay")

    def _execute_orders(self, orders: Sequence[ReplayOrder]) -> list[ReplayOrderResult]:
        if not orders:
            return []
        prepared = self.catalog.prepare_for_orders(orders)
        self._source_sequences.update(
            {trade.trade_id: trade.sequence for trade in prepared.ordered_trades}
        )
        if self.execution_family == "V2":
            if not all(isinstance(order, V2TakerOrder) for order in orders):
                raise TypeError("V2 ReplaySession accepts V2TakerOrder only")
            v2_results, _, _ = replay_v2_taker_orders_with_diagnostics(
                orders,  # type: ignore[arg-type]
                ledger=self.v2_ledger,
                prepared_tape=prepared,
            )
            return list(v2_results)
        if not all(isinstance(order, TradeOnlyOrder) for order in orders):
            raise TypeError("V3 ReplaySession accepts TradeOnlyOrder only")
        v3_results, _, _ = replay_trade_only_orders_with_diagnostics(
            orders,  # type: ignore[arg-type]
            None,
            str(self.profile),
            ledger=self.v3_ledger,
            prepared_tape=prepared,
        )
        return list(v3_results)

    def _bind_signal_source(
        self, order: ReplayOrder, trade: V2TradePrint
    ) -> ReplayOrder:
        if isinstance(order, V2TakerOrder):
            return replace(
                order,
                signal_block=trade.block_number,
                signal_ts=trade.block_time,
                signal_source_trade_id=trade.trade_id,
                signal_source_tx_hash=trade.tx_hash,
                signal_source_log_indexes=trade.source_log_indexes,
                exclude_signal_source_trade=True,
            )
        return replace(
            order,
            signal_block=trade.block_number,
            signal_ts=trade.block_time,
            signal_source_trade_id=trade.trade_id,
        )

    def _schedule_result(self, order: ReplayOrder, result: ReplayOrderResult) -> None:
        for fill in result.fills:
            source_ids = tuple(getattr(fill, "source_trade_ids", ()))
            if not source_ids and hasattr(fill, "source_trade_id"):
                source_ids = (str(fill.source_trade_id),)
            sequence = max(
                (
                    self._source_sequences.get(item, _minimum_sequence())
                    for item in source_ids
                ),
                default=_minimum_sequence(),
            )
            self._push_schedule(fill.fill_ts, sequence, "fill", (order, result, fill))
        terminal_ts = _order_terminal_ts(order, result)
        terminal_sequence = max(
            (
                self._source_sequences.get(source_id, _minimum_sequence())
                for fill in result.fills
                for source_id in _fill_source_ids(fill)
            ),
            default=_minimum_sequence(),
        )
        self._push_schedule(terminal_ts, terminal_sequence, "result", (order, result))

    def _push_schedule(
        self,
        when: datetime,
        sequence: tuple[int, int, str, int, str],
        kind: str,
        payload: Any,
    ) -> None:
        self._schedule_serial += 1
        heapq.heappush(
            self._scheduled,
            (_utc(when), sequence, self._schedule_serial, kind, payload),
        )

    def _flush_scheduled(
        self, now: datetime, sequence: tuple[int, int, str, int, str]
    ) -> None:
        boundary = (_utc(now), sequence)
        while self._scheduled and self._scheduled[0][:2] <= boundary:
            _, _, _, kind, payload = heapq.heappop(self._scheduled)
            if kind == "fill":
                order, result, fill = payload
                self._apply_fill(order, result, fill)
                continue
            order, result = payload
            self.active_orders.pop(order.order_id, None)
            callback = getattr(self.strategy, "on_order_result", None)
            if callable(callback):
                callback(result, self._strategy_context(_utc(now), None))

    def _apply_result_now(self, order: ReplayOrder, result: ReplayOrderResult) -> None:
        for fill in result.fills:
            self._apply_fill(order, result, fill)

    def _apply_fill(
        self, order: ReplayOrder, result: ReplayOrderResult, fill: Any
    ) -> None:
        evidence = str(getattr(result, "evidence_tier", "A_SOURCE_CONFIRMED"))
        if self.execution_family == "V2" or evidence == "A_SOURCE_CONFIRMED":
            book = self.accounting.source_confirmed
        elif evidence.startswith(("B_", "C_")):
            book = self.accounting.inferred
        else:
            book = self.accounting.modeled
        book.apply_fill(
            asset_id=order.asset_id,
            side=order.side,
            size=fill.filled_size,
            price=fill.exec_price,
        )

    def _strategy_context(
        self, now: datetime, trade: V2TradePrint | None
    ) -> ReplayStrategyContext:
        return ReplayStrategyContext(
            now=_utc(now),
            trade=trade,
            accounting=self.accounting,
            strategy_state=self.strategy_state,
            settlement_state=self.settlement_state,
            rng=self.rng,
            high_watermark=self.high_watermark,
        )

    def _receipt(self, mode: str) -> ReplayRunReceipt:
        return ReplayRunReceipt(
            schema_version="UnifiedFillOnlyReplayReceiptV1",
            run_id=self.run_id,
            replay_mode=mode,
            execution_family=self.execution_family,
            profile=self.profile,
            chunks_processed=self._chunks_processed,
            events_processed=self._events_processed,
            orders_generated=len(self.orders),
            results_count=len(self.results),
            catalog_rows=self.catalog.row_count,
            high_watermark=asdict(self.high_watermark)
            if self.high_watermark is not None
            else None,
            accounting=self.accounting.as_dict(),
        )


def timestamp_aligned_chunks(
    trades: Iterable[V2TradePrint], *, target_rows: int
) -> tuple[tuple[V2TradePrint, ...], ...]:
    """Chunk an ordered tape without splitting timestamp, tx, or trade group."""

    if int(target_rows) <= 0:
        raise ValueError("target_rows must be positive")
    rows = tuple(sorted(trades, key=lambda item: item.sequence))
    chunks: list[tuple[V2TradePrint, ...]] = []
    start = 0
    while start < len(rows):
        end = min(len(rows), start + int(target_rows))
        while end < len(rows) and _same_atomic_boundary(rows[end - 1], rows[end]):
            end += 1
        chunks.append(rows[start:end])
        start = end
    return tuple(chunks)


def _same_atomic_boundary(left: V2TradePrint, right: V2TradePrint) -> bool:
    if _utc(left.block_time) == _utc(right.block_time):
        return True
    if left.tx_hash and left.tx_hash.lower() == right.tx_hash.lower():
        return True
    left_group = left.trade_group_id or ""
    right_group = right.trade_group_id or ""
    return bool(left_group and left_group == right_group)


def _fill_source_ids(fill: Any) -> tuple[str, ...]:
    source_ids = tuple(getattr(fill, "source_trade_ids", ()))
    if source_ids:
        return source_ids
    source_id = getattr(fill, "source_trade_id", None)
    return (str(source_id),) if source_id else ()


def _minimum_sequence() -> tuple[int, int, str, int, str]:
    return (-1, -1, "", -1, "")


def _event_key_sequence(key: EventKey) -> tuple[int, int, str, int, str]:
    return (key.block_number, key.tx_index, key.tx_hash, key.log_index, key.trade_id)


def _order_terminal_ts(order: ReplayOrder, result: ReplayOrderResult) -> datetime:
    fills = cast(tuple[V2Fill | TradeOnlyFill, ...], tuple(result.fills))
    if result.status == "FILLED" and fills:
        return max(_utc(fill.fill_ts) for fill in fills)
    deadline = getattr(order, "deadline_ts", None)
    if deadline is not None:
        return _utc(deadline)
    if fills:
        return max(_utc(fill.fill_ts) for fill in fills)
    signal = getattr(order, "signal_ts", None)
    if signal is None:
        raise ValueError("order has no timestamp for terminal scheduling")
    return _utc(signal)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(payload).hexdigest()
