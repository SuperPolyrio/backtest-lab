"""L2 book plus OrderFilled execution primitives.

This module implements the conservative execution core described in
``docs/量化/成交模型/polymarket_execution_model_guidance_for_codex.md``.
It is intentionally L2/MBP based: book depth is visible by price level, while
maker fills require OrderFilled-derived trade evidence to consume queue ahead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
from typing import Any, Literal, Mapping


Q = Decimal("0.0000000001")
BookSide = Literal["BUY", "SELL"]
OrderSide = Literal["BUY", "SELL"]
OrderType = Literal["LIMIT", "MARKETABLE_LIMIT"]
TimeInForce = Literal["GTC", "GTD", "FOK", "FAK", "IOC"]
OrderState = Literal["WORKING", "PARTIAL", "FILLED", "CANCELLED", "REJECTED"]
QueueMode = Literal["trade_only", "reconciled", "optimistic"]
BookConfidence = Literal["HIGH", "MEDIUM", "LOW", "INVALID"]


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class BookSnapshot:
    ts: datetime
    market_id: str
    asset_id: str
    sequence: int | None
    source: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    hash: str | None = None
    min_order_size: Decimal | None = None
    tick_size: Decimal | None = None
    is_full_depth: bool = False
    observed_depth_levels: int | None = None

    @property
    def event_id(self) -> str:
        digest = hashlib.sha256()
        digest.update(f"snapshot|{self.market_id}|{self.asset_id}|{_ts(self.ts)}|{self.sequence}".encode("utf-8"))
        for levels in (self.bids, self.asks):
            for level in levels:
                digest.update(f"|{_q(level.price)}:{_q(level.size)}".encode("ascii"))
        return digest.hexdigest()[:24]


@dataclass(frozen=True)
class BookDelta:
    ts: datetime
    market_id: str
    asset_id: str
    side: BookSide
    price: Decimal
    new_size: Decimal
    sequence: int | None
    source: str
    hash: str | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None

    @property
    def event_id(self) -> str:
        payload = (
            f"delta|{self.market_id}|{self.asset_id}|{self.side}|{_q(self.price)}|{_q(self.new_size)}|"
            f"{_ts(self.ts)}|{self.sequence}|{self.best_bid}|{self.best_ask}"
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class FillTick:
    ts: datetime
    block_number: int | None
    tx_hash: str
    log_index: int
    order_hash: str
    market_id: str
    asset_id: str
    price: Decimal
    size: Decimal
    passive_side: BookSide
    aggressor_side: OrderSide
    maker: str
    taker: str
    fee: Decimal
    source: Literal["orderfilled", "trade_api", "reconciled"] = "orderfilled"

    @property
    def event_id(self) -> str:
        payload = f"fill|{self.tx_hash.lower()}|{self.log_index}|{self.order_hash.lower()}|{self.market_id}|{self.asset_id}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class OrderFilledNormalization:
    fill_tick: FillTick | None
    status: Literal["ready", "quarantine"]
    reason: str
    canonical_fill_key: str


def normalize_orderfilled_event(
    row: Mapping[str, Any],
    *,
    market_id: str | None = None,
    token_decimals: int = 6,
    collateral_decimals: int = 6,
) -> OrderFilledNormalization:
    """Normalize a raw Polymarket OrderFilled row into a maker-level FillTick.

    ``makerAssetId == 0`` means the resting maker order was buying outcome
    tokens with collateral. ``takerAssetId == 0`` means the resting maker order
    was selling outcome tokens for collateral.
    """

    maker_asset_id = _asset(row, "makerAssetId", "maker_asset_id", "maker_asset_id_decimal")
    taker_asset_id = _asset(row, "takerAssetId", "taker_asset_id", "taker_asset_id_decimal")
    maker_amount = _decimal_raw(_get(row, "makerAmountFilled", "maker_amount_filled", "maker_amount"))
    taker_amount = _decimal_raw(_get(row, "takerAmountFilled", "taker_amount_filled", "taker_amount"))
    tx_hash = str(_get(row, "transactionHash", "tx_hash", "transaction_hash") or "")
    log_index = int(_decimal_raw(_get(row, "logIndex", "log_index") or 0))
    order_hash = str(_get(row, "orderHash", "order_hash") or "")
    key = _canonical_orderfilled_key(tx_hash, log_index, order_hash, maker_asset_id, taker_asset_id)

    if not tx_hash or not order_hash:
        return OrderFilledNormalization(None, "quarantine", "missing_tx_or_order_hash", key)
    if maker_asset_id == "0" and taker_asset_id != "0":
        passive_side: BookSide = "BUY"
        aggressor_side: OrderSide = "SELL"
        asset_id = taker_asset_id
        size = _normalize_amount(taker_amount, token_decimals)
        collateral = _normalize_amount(maker_amount, collateral_decimals)
    elif taker_asset_id == "0" and maker_asset_id != "0":
        passive_side = "SELL"
        aggressor_side = "BUY"
        asset_id = maker_asset_id
        size = _normalize_amount(maker_amount, token_decimals)
        collateral = _normalize_amount(taker_amount, collateral_decimals)
    else:
        return OrderFilledNormalization(None, "quarantine", "unsupported_asset_path", key)

    if size <= 0 or collateral <= 0:
        return OrderFilledNormalization(None, "quarantine", "non_positive_size_or_collateral", key)
    price = _q(collateral / size)
    if price < 0 or price > 1:
        return OrderFilledNormalization(None, "quarantine", "price_out_of_bounds", key)

    ts = _coerce_datetime(_get(row, "match_time", "matchTime", "block_timestamp", "timestamp", "ts")) or datetime.fromtimestamp(0, tz=timezone.utc)
    tick = FillTick(
        ts=ts,
        block_number=int(_decimal_raw(_get(row, "blockNumber", "block_number") or 0)) or None,
        tx_hash=tx_hash,
        log_index=log_index,
        order_hash=order_hash,
        market_id=str(market_id or _get(row, "market_id", "marketId", "condition_id", "conditionId") or ""),
        asset_id=asset_id,
        price=price,
        size=_q(size),
        passive_side=passive_side,
        aggressor_side=aggressor_side,
        maker=str(_get(row, "maker") or ""),
        taker=str(_get(row, "taker") or ""),
        fee=_normalize_amount(_decimal_raw(_get(row, "fee", "fee_amount") or 0), collateral_decimals),
        source="orderfilled",
    )
    return OrderFilledNormalization(tick, "ready", "normalized", key)


@dataclass
class StrategyOrderIntent:
    client_order_id: str
    signal_ts: datetime
    market_id: str
    asset_id: str
    side: OrderSide
    order_type: OrderType
    limit_price: Decimal
    size: Decimal
    tif: TimeInForce
    post_only: bool = False
    expires_at: datetime | None = None

    @property
    def event_id(self) -> str:
        payload = f"intent|{self.client_order_id}|{self.market_id}|{self.asset_id}|{_ts(self.signal_ts)}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


@dataclass
class StrategyCancelIntent:
    client_order_id: str
    signal_ts: datetime
    market_id: str
    asset_id: str
    cancel_latency_ms: int | None = None

    @property
    def event_id(self) -> str:
        payload = f"cancel|{self.client_order_id}|{self.market_id}|{self.asset_id}|{_ts(self.signal_ts)}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True)
class ExecutionFill:
    order_id: str
    ts: datetime
    asset_id: str
    side: OrderSide
    liquidity_flag: Literal["MAKER", "TAKER"]
    price: Decimal
    size: Decimal
    fee: Decimal
    source_event_ids: tuple[str, ...]
    reason: str
    book_ts: datetime | None
    queue_ahead_before: Decimal | None = None
    queue_ahead_after: Decimal | None = None

    @property
    def notional(self) -> Decimal:
        return _q(self.price * self.size)


@dataclass
class RestingOrder:
    order_id: str
    asset_id: str
    side: OrderSide
    price: Decimal
    original_size: Decimal
    remaining_size: Decimal
    accepted_ts: datetime
    queue_ahead: Decimal
    queue_ahead_at_admit: Decimal | None = None
    submitted_at: datetime | None = None
    state: OrderState = "WORKING"
    fills: list[ExecutionFill] = field(default_factory=list)


@dataclass(frozen=True)
class BookQuality:
    book_age_ms: int
    has_snapshot_anchor: bool
    gap_since_last_snapshot: bool
    depth_levels_observed: int
    is_full_depth: bool
    source: str
    confidence: BookConfidence


@dataclass(frozen=True)
class L2ExecutionConfig:
    mode: Literal["conservative", "realistic", "optimistic"] = "conservative"
    queue_mode: QueueMode = "trade_only"
    queue_ahead_fraction: Decimal = Decimal("1.0")
    cancel_ahead_fraction: Decimal = Decimal("0")
    depth_haircut: Decimal = Decimal("0.7")
    require_fresh_book: bool = True
    book_ttl_ms: int = 5_000
    submit_latency_ms: int = 100
    cancel_latency_ms: int = 50
    replace_latency_ms: int = 100
    fee_bps: Decimal = Decimal("0")
    impact_strength_bps: Decimal = Decimal("0")
    allow_cross_gap_execution: bool = False
    use_orderfilled_for_maker_queue: bool = True
    use_lob_decrease_for_queue: bool = False


@dataclass(frozen=True)
class OrderExecutionResult:
    order_id: str
    state: OrderState
    fills: tuple[ExecutionFill, ...]
    remaining_size: Decimal
    reject_reason: str = ""
    resting_order: RestingOrder | None = None
    submitted_at: datetime | None = None
    venue_received_at: datetime | None = None
    book_snapshot_id: str | None = None
    queue_ahead_at_admit: Decimal | None = None
    book_quality: BookQuality | None = None
    mode: str = ""

    @property
    def filled_size(self) -> Decimal:
        return _q(sum((fill.size for fill in self.fills), Decimal("0")))

    @property
    def avg_fill_price(self) -> Decimal:
        if self.filled_size <= 0:
            return Decimal("0")
        return _q(sum((fill.notional for fill in self.fills), Decimal("0")) / self.filled_size)

    def audit_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "l2_orderfilled_execution_audit_v1",
            "order_id": self.order_id,
            "state": self.state,
            "submitted_at": _ts(self.submitted_at) if self.submitted_at else None,
            "venue_received_at": _ts(self.venue_received_at) if self.venue_received_at else None,
            "filled_size": self.filled_size,
            "remaining_size": self.remaining_size,
            "avg_fill_price": self.avg_fill_price,
            "reject_reason": self.reject_reason,
            "book_snapshot_id": self.book_snapshot_id,
            "queue_ahead_at_admit": self.queue_ahead_at_admit,
            "book_quality": book_quality_to_audit_dict(self.book_quality),
            "mode": self.mode,
            "invariant_violations": l2_order_result_invariant_violations(self),
            "fill_count": len(self.fills),
            "source_event_ids": sorted({event_id for fill in self.fills for event_id in fill.source_event_ids}),
            "fills": [execution_fill_to_audit_dict(fill) for fill in self.fills],
            "resting_order": resting_order_to_audit_dict(self.resting_order) if self.resting_order is not None else None,
        }


@dataclass(frozen=True)
class ExecutionTimelineResult:
    fills: tuple[ExecutionFill, ...]
    order_results: dict[str, OrderExecutionResult]
    audit_events: tuple[dict[str, Any], ...]
    corrections: tuple[str, ...] = ()

    def audit_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "l2_orderfilled_timeline_audit_v1",
            "fill_count": len(self.fills),
            "order_count": len(self.order_results),
            "corrections": list(self.corrections),
            "fills": [execution_fill_to_audit_dict(fill) for fill in self.fills],
            "orders": {order_id: result.audit_dict() for order_id, result in self.order_results.items()},
            "events": list(self.audit_events),
            "invariant_violations": {
                order_id: violations
                for order_id, result in self.order_results.items()
                if (violations := l2_order_result_invariant_violations(result))
            },
        }


class BookState:
    """L2 market-by-price book with residual consumption tracking."""

    def __init__(self, *, stale_after_ms: int = 5_000) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.market_id = ""
        self.asset_id = ""
        self.source = ""
        self.last_snapshot_ts: datetime | None = None
        self.last_snapshot_event_id: str | None = None
        self.last_update_ts: datetime | None = None
        self.last_update_event_id: str | None = None
        self.last_sequence: int | None = None
        self.has_snapshot_anchor = False
        self.gap_since_last_snapshot = False
        self.is_full_depth = False
        self.stale_after_ms = stale_after_ms
        self._residual: dict[tuple[BookSide, Decimal], Decimal] = {}

    def apply_snapshot(self, snapshot: BookSnapshot) -> None:
        self.market_id = snapshot.market_id
        self.asset_id = snapshot.asset_id
        self.source = snapshot.source
        self.bids = _levels_to_map(snapshot.bids)
        self.asks = _levels_to_map(snapshot.asks)
        self.last_snapshot_ts = snapshot.ts
        self.last_snapshot_event_id = snapshot.hash or snapshot.event_id
        self.last_update_ts = snapshot.ts
        self.last_update_event_id = snapshot.hash or snapshot.event_id
        self.last_sequence = snapshot.sequence
        self.has_snapshot_anchor = True
        self.gap_since_last_snapshot = False
        self.is_full_depth = snapshot.is_full_depth
        self._residual.clear()

    def apply_delta(self, delta: BookDelta) -> bool:
        if not self.has_snapshot_anchor:
            self.gap_since_last_snapshot = True
            return False
        if delta.sequence is not None and self.last_sequence is not None and delta.sequence < self.last_sequence:
            self.gap_since_last_snapshot = True
            return False
        if delta.market_id != self.market_id or delta.asset_id != self.asset_id:
            self.gap_since_last_snapshot = True
            return False
        book = self.bids if delta.side == "BUY" else self.asks
        price = _q(delta.price)
        size = _q(delta.new_size)
        if size <= 0:
            book.pop(price, None)
            self._residual.pop((delta.side, price), None)
        else:
            book[price] = size
            consumed = self._residual.get((delta.side, price), Decimal("0"))
            if consumed > size:
                self._residual[(delta.side, price)] = size
        # PMXT price_change messages include authoritative top-of-book fields.
        # Some streams omit explicit zero-size events when the inside moves, so
        # retaining levels beyond those bounds creates a false crossed book.
        if delta.best_bid is not None:
            best_bid = _q(delta.best_bid)
            for stale_price in [item for item in self.bids if item > best_bid]:
                self.bids.pop(stale_price, None)
                self._residual.pop(("BUY", stale_price), None)
        if delta.best_ask is not None:
            best_ask = _q(delta.best_ask)
            for stale_price in [item for item in self.asks if item < best_ask]:
                self.asks.pop(stale_price, None)
                self._residual.pop(("SELL", stale_price), None)
        self.last_update_ts = delta.ts
        self.last_update_event_id = delta.hash or delta.event_id
        self.last_sequence = delta.sequence if delta.sequence is not None else self.last_sequence
        return True

    @property
    def best_bid(self) -> Decimal | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return min(self.asks) if self.asks else None

    @property
    def spread(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return _q(self.best_ask - self.best_bid)

    @property
    def mid(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return _q((self.best_bid + self.best_ask) / Decimal("2"))

    def remaining_bids(self) -> tuple[BookLevel, ...]:
        return tuple(
            BookLevel(price, size)
            for price, size in sorted(self._remaining("BUY").items(), key=lambda item: item[0], reverse=True)
            if size > 0
        )

    def remaining_asks(self) -> tuple[BookLevel, ...]:
        return tuple(
            BookLevel(price, size)
            for price, size in sorted(self._remaining("SELL").items(), key=lambda item: item[0])
            if size > 0
        )

    def same_price_remaining(self, side: BookSide, price: Decimal) -> Decimal:
        return self._remaining(side).get(_q(price), Decimal("0"))

    def residual_consume(self, *, book_side: BookSide, price: Decimal, size: Decimal) -> Decimal:
        price_q = _q(price)
        visible = (self.bids if book_side == "BUY" else self.asks).get(price_q, Decimal("0"))
        consumed = self._residual.get((book_side, price_q), Decimal("0"))
        take = min(max(Decimal("0"), _q(size)), max(Decimal("0"), visible - consumed))
        self._residual[(book_side, price_q)] = _q(consumed + take)
        return take

    def quality_at(self, ts: datetime, *, ttl_ms: int | None = None) -> BookQuality:
        ttl = self.stale_after_ms if ttl_ms is None else ttl_ms
        if not self.has_snapshot_anchor or self.last_update_ts is None:
            return BookQuality(0, False, self.gap_since_last_snapshot, 0, self.is_full_depth, self.source, "INVALID")
        age_ms = max(0, int((ts - self.last_update_ts).total_seconds() * 1000))
        depth_levels = len(self.bids) + len(self.asks)
        if self.gap_since_last_snapshot:
            confidence: BookConfidence = "INVALID"
        elif self.best_bid is not None and self.best_ask is not None and self.best_bid >= self.best_ask:
            confidence = "INVALID"
        elif age_ms > ttl:
            confidence = "LOW"
        elif not self.bids or not self.asks:
            confidence = "LOW"
        elif self.is_full_depth:
            confidence = "HIGH"
        else:
            confidence = "MEDIUM"
        return BookQuality(age_ms, True, self.gap_since_last_snapshot, depth_levels, self.is_full_depth, self.source, confidence)

    def _remaining(self, side: BookSide) -> dict[Decimal, Decimal]:
        source = self.bids if side == "BUY" else self.asks
        return {
            price: max(Decimal("0"), size - self._residual.get((side, price), Decimal("0"))).quantize(Q)
            for price, size in source.items()
        }


class LevelQueue:
    """FIFO queue at one L2 price level."""

    def __init__(self, *, asset_id: str, side: BookSide, price: Decimal, env_ahead: Decimal) -> None:
        self.asset_id = asset_id
        self.side = side
        self.price = _q(price)
        self.env_ahead = _q(max(Decimal("0"), env_ahead))
        self.agent_orders: list[RestingOrder] = []

    def place_order(self, order: RestingOrder) -> RestingOrder:
        order.queue_ahead = _q(self.env_ahead + sum((item.remaining_size for item in self.agent_orders), Decimal("0")))
        order.queue_ahead_at_admit = order.queue_ahead
        order.state = "WORKING"
        self.agent_orders.append(order)
        return order

    def cancel_order(self, order_id: str, *, ts: datetime) -> bool:
        for order in self.agent_orders:
            if order.order_id != order_id or order.state in {"FILLED", "CANCELLED", "REJECTED"}:
                continue
            order.state = "CANCELLED"
            order.remaining_size = Decimal("0")
            return True
        return False

    def process_fill_tick(self, tick: FillTick) -> tuple[ExecutionFill, ...]:
        if tick.asset_id != self.asset_id or tick.passive_side != self.side or _q(tick.price) != self.price:
            return ()
        remaining_flow = _q(max(Decimal("0"), tick.size))
        if remaining_flow <= 0:
            return ()
        env_consumed = min(self.env_ahead, remaining_flow)
        self.env_ahead = _q(self.env_ahead - env_consumed)
        remaining_flow = _q(remaining_flow - env_consumed)
        fills: list[ExecutionFill] = []
        while remaining_flow > 0 and self.agent_orders:
            order = self.agent_orders[0]
            if order.state in {"FILLED", "CANCELLED", "REJECTED"} or order.remaining_size <= 0:
                self.agent_orders.pop(0)
                continue
            before = _q(order.queue_ahead)
            size = min(order.remaining_size, remaining_flow)
            fill = ExecutionFill(
                order_id=order.order_id,
                ts=tick.ts,
                asset_id=order.asset_id,
                side=order.side,
                liquidity_flag="MAKER",
                price=order.price,
                size=_q(size),
                fee=Decimal("0"),
                source_event_ids=(tick.event_id,),
                reason="maker_queue_consumed_by_orderfilled",
                book_ts=None,
                queue_ahead_before=before,
                queue_ahead_after=Decimal("0"),
            )
            order.fills.append(fill)
            fills.append(fill)
            order.remaining_size = _q(order.remaining_size - size)
            order.queue_ahead = Decimal("0")
            remaining_flow = _q(remaining_flow - size)
            order.state = "FILLED" if order.remaining_size <= 0 else "PARTIAL"
            if order.state == "FILLED":
                self.agent_orders.pop(0)
        for order in self.agent_orders:
            if order.state == "WORKING":
                order.queue_ahead = self.env_ahead
                break
        return tuple(fills)

    def update_env_from_lob_change(
        self,
        *,
        old_size: Decimal,
        new_size: Decimal,
        orderfilled_size: Decimal,
        mode: QueueMode = "trade_only",
        cancel_ahead_fraction: Decimal = Decimal("0"),
    ) -> Decimal:
        old_q = _q(max(Decimal("0"), old_size))
        new_q = _q(max(Decimal("0"), new_size))
        trade_q = _q(max(Decimal("0"), orderfilled_size))
        decrease = max(Decimal("0"), old_q - new_q)
        unexplained = max(Decimal("0"), decrease - trade_q)
        fraction = Decimal("0")
        if mode == "reconciled":
            fraction = max(Decimal("0"), min(Decimal("1"), cancel_ahead_fraction))
        elif mode == "optimistic":
            fraction = Decimal("1")
        self.env_ahead = _q(max(Decimal("0"), self.env_ahead - trade_q - unexplained * fraction))
        for order in self.agent_orders:
            if order.state in {"WORKING", "PARTIAL"}:
                order.queue_ahead = min(order.queue_ahead, self.env_ahead)
                break
        return self.env_ahead


class L2OrderFilledExecutionModel:
    """Small deterministic matching engine for L2 taker and maker replay."""

    def __init__(self, config: L2ExecutionConfig | None = None) -> None:
        self.config = config or L2ExecutionConfig()
        self.book = BookState(stale_after_ms=self.config.book_ttl_ms)
        self.queues: dict[tuple[str, BookSide, Decimal], LevelQueue] = {}
        self.order_results: dict[str, OrderExecutionResult] = {}
        self.ledger: list[ExecutionFill] = []
        self.audit_events: list[dict[str, Any]] = []

    def apply_snapshot(self, snapshot: BookSnapshot) -> None:
        old_sizes = self._queue_visible_sizes()
        self.book.apply_snapshot(snapshot)
        if self.config.use_lob_decrease_for_queue:
            self._reconcile_queue_visible_sizes(old_sizes)

    def apply_delta(self, delta: BookDelta) -> bool:
        old_sizes = self._queue_visible_sizes()
        applied = self.book.apply_delta(delta)
        if applied and self.config.use_lob_decrease_for_queue:
            self._reconcile_queue_visible_sizes(old_sizes, only_key=(delta.asset_id, delta.side, _q(delta.price)))
        return applied

    def execute_taker(self, intent: StrategyOrderIntent) -> OrderExecutionResult:
        received_at = intent.signal_ts + timedelta(milliseconds=self.config.submit_latency_ms)
        quality = self.book.quality_at(received_at, ttl_ms=self.config.book_ttl_ms)
        if quality.confidence == "INVALID" or (self.config.require_fresh_book and quality.confidence == "LOW" and not self.config.allow_cross_gap_execution):
            if self.book.best_bid is not None and self.book.best_ask is not None and self.book.best_bid >= self.book.best_ask:
                reason = "crossed_book"
            else:
                reason = "book_stale_or_missing"
            result = self._order_result(
                intent,
                "REJECTED",
                (),
                _q(intent.size),
                reason,
                received_at=received_at,
                quality=quality,
            )
            self._record_order_result(result)
            return result
        if intent.post_only and self._would_cross(intent):
            result = self._order_result(
                intent,
                "REJECTED",
                (),
                _q(intent.size),
                "post_only_crosses_book",
                received_at=received_at,
                quality=quality,
            )
            self._record_order_result(result)
            return result

        fills: list[ExecutionFill] = []
        remaining = _q(intent.size)
        candidate: list[tuple[BookSide, BookLevel, Decimal]] = []
        book_side: BookSide = "SELL" if intent.side == "BUY" else "BUY"
        levels = self.book.remaining_asks() if intent.side == "BUY" else self.book.remaining_bids()
        for level in levels:
            if intent.side == "BUY" and level.price > intent.limit_price:
                break
            if intent.side == "SELL" and level.price < intent.limit_price:
                break
            capacity = _q(level.size * self.config.depth_haircut)
            take = min(remaining, capacity)
            if take <= 0:
                continue
            candidate.append((book_side, level, take))
            remaining = _q(remaining - take)
            if remaining <= 0:
                break

        if intent.tif == "FOK" and remaining > 0:
            result = self._order_result(
                intent,
                "REJECTED",
                (),
                _q(intent.size),
                "fok_insufficient_liquidity",
                received_at=received_at,
                quality=quality,
            )
            self._record_order_result(result)
            return result

        visible_depth = _q(sum((take for _, _, take in candidate), Decimal("0")))
        for side, level, take in candidate:
            committed = self.book.residual_consume(book_side=side, price=level.price, size=take)
            if committed <= 0:
                continue
            fill_price = _apply_adverse_impact(level.price, intent.side, committed, visible_depth, self.config.impact_strength_bps)
            fills.append(
                ExecutionFill(
                    order_id=intent.client_order_id,
                    ts=received_at,
                    asset_id=intent.asset_id,
                    side=intent.side,
                    liquidity_flag="TAKER",
                    price=fill_price,
                    size=committed,
                    fee=_q(committed * level.price * self.config.fee_bps / Decimal("10000")),
                    source_event_ids=(f"book:{self.book.market_id}:{self.book.asset_id}:{_ts(self.book.last_update_ts)}",),
                    reason="taker_walk_visible_book",
                    book_ts=self.book.last_update_ts,
                )
            )

        filled = _q(sum((fill.size for fill in fills), Decimal("0")))
        remaining_after = _q(max(Decimal("0"), intent.size - filled))
        if remaining_after > 0 and intent.tif in {"GTC", "GTD"}:
            resting = self.admit_maker_order(intent, remaining_size=remaining_after, accepted_ts=received_at)
            state: OrderState = "PARTIAL" if fills else resting.state
            result = self._order_result(
                intent,
                state,
                tuple(fills),
                remaining_after,
                resting_order=resting,
                received_at=received_at,
                quality=quality,
                queue_ahead_at_admit=resting.queue_ahead_at_admit,
            )
            self._record_order_result(result)
            return result
        if remaining_after > 0 and intent.tif in {"FAK", "IOC"}:
            result = self._order_result(
                intent,
                "PARTIAL" if fills else "CANCELLED",
                tuple(fills),
                remaining_after,
                "unfilled_remainder_cancelled",
                received_at=received_at,
                quality=quality,
            )
            self._record_order_result(result)
            return result
        result = self._order_result(
            intent,
            "FILLED" if fills else "REJECTED",
            tuple(fills),
            remaining_after,
            "" if fills else "price_limit_or_empty_depth",
            received_at=received_at,
            quality=quality,
        )
        self._record_order_result(result)
        return result

    def admit_maker_order(
        self,
        intent: StrategyOrderIntent,
        *,
        remaining_size: Decimal | None = None,
        accepted_ts: datetime | None = None,
    ) -> RestingOrder:
        if intent.post_only and self._would_cross(intent):
            return RestingOrder(
                order_id=intent.client_order_id,
                asset_id=intent.asset_id,
                side=intent.side,
                price=_q(intent.limit_price),
                original_size=_q(remaining_size or intent.size),
                remaining_size=Decimal("0"),
                accepted_ts=accepted_ts or intent.signal_ts,
                queue_ahead=Decimal("0"),
                submitted_at=intent.signal_ts,
                state="REJECTED",
            )
        book_side: BookSide = "BUY" if intent.side == "BUY" else "SELL"
        queue = self._queue_for(intent.asset_id, book_side, intent.limit_price)
        if not queue.agent_orders:
            visible = self.book.same_price_remaining(book_side, intent.limit_price)
            queue.env_ahead = _q(visible * self.config.queue_ahead_fraction)
        order = RestingOrder(
            order_id=intent.client_order_id,
            asset_id=intent.asset_id,
            side=intent.side,
            price=_q(intent.limit_price),
            original_size=_q(remaining_size or intent.size),
            remaining_size=_q(remaining_size or intent.size),
            accepted_ts=accepted_ts or intent.signal_ts,
            queue_ahead=Decimal("0"),
            submitted_at=intent.signal_ts,
        )
        return queue.place_order(order)

    def process_fill_tick(self, tick: FillTick) -> tuple[ExecutionFill, ...]:
        if not self.config.use_orderfilled_for_maker_queue:
            return ()
        queue = self.queues.get((tick.asset_id, tick.passive_side, _q(tick.price)))
        if queue is None:
            return ()
        fills = queue.process_fill_tick(tick)
        for fill in fills:
            self.ledger.append(fill)
            resting = self._find_resting_order(fill.order_id)
            state: OrderState = "FILLED" if resting is None or resting.remaining_size <= 0 else "PARTIAL"
            remaining = Decimal("0") if resting is None else resting.remaining_size
            existing = self.order_results.get(fill.order_id)
            previous_fills = tuple(existing.fills) if existing is not None else ()
            result = OrderExecutionResult(
                fill.order_id,
                state,
                previous_fills + (fill,),
                remaining,
                resting_order=resting,
                submitted_at=resting.submitted_at if resting is not None else None,
                venue_received_at=resting.accepted_ts if resting is not None else None,
                book_snapshot_id=self.book.last_snapshot_event_id,
                queue_ahead_at_admit=resting.queue_ahead_at_admit if resting is not None else None,
                book_quality=self.book.quality_at(tick.ts, ttl_ms=self.config.book_ttl_ms),
                mode=self.config.mode,
            )
            self.order_results[fill.order_id] = result
        return fills

    def cancel_order(self, cancel: StrategyCancelIntent, *, received_at: datetime | None = None) -> OrderExecutionResult:
        effective_ts = received_at or cancel.signal_ts + timedelta(
            milliseconds=self.config.cancel_latency_ms if cancel.cancel_latency_ms is None else cancel.cancel_latency_ms
        )
        cancelled_order: RestingOrder | None = None
        for queue in self.queues.values():
            for order in queue.agent_orders:
                if order.order_id == cancel.client_order_id and order.state in {"WORKING", "PARTIAL"}:
                    queue.cancel_order(cancel.client_order_id, ts=effective_ts)
                    cancelled_order = order
                    break
            if cancelled_order is not None:
                break
        if cancelled_order is None:
            result = OrderExecutionResult(
                cancel.client_order_id,
                "REJECTED",
                (),
                Decimal("0"),
                "cancel_order_not_working",
                submitted_at=cancel.signal_ts,
                venue_received_at=effective_ts,
                book_snapshot_id=self.book.last_snapshot_event_id,
                book_quality=self.book.quality_at(effective_ts, ttl_ms=self.config.book_ttl_ms),
                mode=self.config.mode,
            )
        else:
            existing = self.order_results.get(cancel.client_order_id)
            result = OrderExecutionResult(
                cancel.client_order_id,
                "CANCELLED",
                tuple(existing.fills) if existing is not None else tuple(cancelled_order.fills),
                cancelled_order.remaining_size,
                "cancelled_by_strategy",
                resting_order=cancelled_order,
                submitted_at=cancel.signal_ts,
                venue_received_at=effective_ts,
                book_snapshot_id=self.book.last_snapshot_event_id,
                queue_ahead_at_admit=cancelled_order.queue_ahead_at_admit,
                book_quality=self.book.quality_at(effective_ts, ttl_ms=self.config.book_ttl_ms),
                mode=self.config.mode,
            )
        self.order_results[cancel.client_order_id] = result
        return result

    def process_event(
        self,
        event: BookSnapshot | BookDelta | FillTick | StrategyOrderIntent | StrategyCancelIntent,
    ) -> tuple[ExecutionFill, ...]:
        if isinstance(event, BookSnapshot):
            self.apply_snapshot(event)
            self.audit_events.append({"type": "book_snapshot", "event_id": event.event_id, "ts": _ts(event.ts)})
            return ()
        if isinstance(event, BookDelta):
            applied = self.apply_delta(event)
            self.audit_events.append({"type": "book_delta", "event_id": event.event_id, "ts": _ts(event.ts), "applied": applied})
            return ()
        if isinstance(event, FillTick):
            fills = self.process_fill_tick(event)
            self.audit_events.append({"type": "fill_tick", "event_id": event.event_id, "ts": _ts(event.ts), "fills": len(fills)})
            return fills
        if isinstance(event, StrategyOrderIntent):
            result = self.execute_taker(event)
            self.audit_events.append(
                {
                    "type": "strategy_order",
                    "event_id": event.event_id,
                    "order_id": event.client_order_id,
                    "submitted_at": _ts(event.signal_ts),
                    "venue_received_at": _ts(event.signal_ts + timedelta(milliseconds=self.config.submit_latency_ms)),
                    "state": result.state,
                    "fill_count": len(result.fills),
                    "reject_reason": result.reject_reason,
                }
            )
            return result.fills
        result = self.cancel_order(event)
        self.audit_events.append(
            {
                "type": "strategy_cancel",
                "event_id": event.event_id,
                "order_id": event.client_order_id,
                "submitted_at": _ts(event.signal_ts),
                    "venue_received_at": _ts(
                        event.signal_ts
                    + timedelta(milliseconds=self.config.cancel_latency_ms if event.cancel_latency_ms is None else event.cancel_latency_ms)
                ),
                "state": result.state,
                "reject_reason": result.reject_reason,
            }
        )
        return ()

    def run_event_timeline(
        self,
        events: list[BookSnapshot | BookDelta | FillTick | StrategyOrderIntent | StrategyCancelIntent],
        *,
        sort_non_monotonic: bool = True,
    ) -> ExecutionTimelineResult:
        corrections: list[str] = []
        decorated = [(index, _event_effective_ts(event, self.config), event) for index, event in enumerate(events)]
        if any(decorated[i][1] > decorated[i + 1][1] for i in range(len(decorated) - 1)):
            if not sort_non_monotonic:
                raise ValueError("event timeline is not monotonic")
            corrections.append("non_monotonic_events_sorted")
        for _, _, event in sorted(decorated, key=lambda item: (item[1], _event_priority(item[2]), item[0])):
            self.process_event(event)
        return ExecutionTimelineResult(tuple(self.ledger), dict(self.order_results), tuple(self.audit_events), tuple(corrections))

    def reconcile_lob_change(
        self,
        *,
        asset_id: str,
        side: BookSide,
        price: Decimal,
        old_size: Decimal,
        new_size: Decimal,
        orderfilled_size: Decimal,
    ) -> Decimal:
        queue = self._queue_for(asset_id, side, price)
        return queue.update_env_from_lob_change(
            old_size=old_size,
            new_size=new_size,
            orderfilled_size=orderfilled_size,
            mode=self.config.queue_mode,
            cancel_ahead_fraction=self.config.cancel_ahead_fraction,
        )

    def _queue_for(self, asset_id: str, side: BookSide, price: Decimal) -> LevelQueue:
        key = (asset_id, side, _q(price))
        if key not in self.queues:
            self.queues[key] = LevelQueue(asset_id=asset_id, side=side, price=price, env_ahead=Decimal("0"))
        return self.queues[key]

    def _would_cross(self, intent: StrategyOrderIntent) -> bool:
        if intent.side == "BUY":
            return self.book.best_ask is not None and _q(intent.limit_price) >= self.book.best_ask
        return self.book.best_bid is not None and _q(intent.limit_price) <= self.book.best_bid

    def _order_result(
        self,
        intent: StrategyOrderIntent,
        state: OrderState,
        fills: tuple[ExecutionFill, ...],
        remaining_size: Decimal,
        reject_reason: str = "",
        *,
        resting_order: RestingOrder | None = None,
        received_at: datetime,
        quality: BookQuality,
        queue_ahead_at_admit: Decimal | None = None,
    ) -> OrderExecutionResult:
        return OrderExecutionResult(
            intent.client_order_id,
            state,
            fills,
            remaining_size,
            reject_reason,
            resting_order=resting_order,
            submitted_at=intent.signal_ts,
            venue_received_at=received_at,
            book_snapshot_id=self.book.last_snapshot_event_id,
            queue_ahead_at_admit=queue_ahead_at_admit,
            book_quality=quality,
            mode=self.config.mode,
        )

    def _record_order_result(self, result: OrderExecutionResult) -> None:
        self.order_results[result.order_id] = result
        self.ledger.extend(result.fills)

    def _find_resting_order(self, order_id: str) -> RestingOrder | None:
        for queue in self.queues.values():
            for order in queue.agent_orders:
                if order.order_id == order_id:
                    return order
        existing = self.order_results.get(order_id)
        return existing.resting_order if existing is not None else None

    def _queue_visible_sizes(self) -> dict[tuple[str, BookSide, Decimal], Decimal]:
        return {key: self.book.same_price_remaining(key[1], key[2]) for key in self.queues}

    def _reconcile_queue_visible_sizes(
        self,
        old_sizes: Mapping[tuple[str, BookSide, Decimal], Decimal],
        *,
        only_key: tuple[str, BookSide, Decimal] | None = None,
    ) -> None:
        keys = [only_key] if only_key is not None else list(old_sizes)
        for key in keys:
            if key is None or key not in self.queues:
                continue
            old_size = old_sizes.get(key, Decimal("0"))
            new_size = self.book.same_price_remaining(key[1], key[2])
            self.queues[key].update_env_from_lob_change(
                old_size=old_size,
                new_size=new_size,
                orderfilled_size=Decimal("0"),
                mode=self.config.queue_mode,
                cancel_ahead_fraction=self.config.cancel_ahead_fraction,
            )


def execution_fill_to_audit_dict(fill: ExecutionFill) -> dict[str, Any]:
    return {
        "order_id": fill.order_id,
        "ts": _ts(fill.ts),
        "asset_id": fill.asset_id,
        "side": fill.side,
        "liquidity_flag": fill.liquidity_flag,
        "price": fill.price,
        "size": fill.size,
        "notional": fill.notional,
        "fee": fill.fee,
        "source_event_ids": list(fill.source_event_ids),
        "reason": fill.reason,
        "book_ts": _ts(fill.book_ts) if fill.book_ts else None,
        "queue_ahead_before": fill.queue_ahead_before,
        "queue_ahead_after": fill.queue_ahead_after,
    }


def book_quality_to_audit_dict(quality: BookQuality | None) -> dict[str, Any] | None:
    if quality is None:
        return None
    return {
        "book_age_ms": quality.book_age_ms,
        "has_snapshot_anchor": quality.has_snapshot_anchor,
        "gap_since_last_snapshot": quality.gap_since_last_snapshot,
        "depth_levels_observed": quality.depth_levels_observed,
        "is_full_depth": quality.is_full_depth,
        "source": quality.source,
        "confidence": quality.confidence,
    }


def l2_order_result_invariant_violations(result: OrderExecutionResult) -> list[str]:
    violations: list[str] = []
    filled = result.filled_size
    if result.remaining_size < 0:
        violations.append("remaining_size_negative")
    if result.resting_order is not None:
        if result.resting_order.queue_ahead < 0:
            violations.append("queue_ahead_negative")
        original = _q(result.resting_order.original_size)
        if filled > original:
            violations.append("filled_size_exceeds_original_size")
    elif filled > 0 and result.remaining_size < 0:
        violations.append("filled_size_exceeds_available_size")
    for fill in result.fills:
        if fill.size <= 0:
            violations.append(f"non_positive_fill_size:{fill.order_id}")
        if fill.liquidity_flag == "MAKER" and result.resting_order is not None and fill.price != result.resting_order.price:
            violations.append(f"maker_fill_price_not_limit:{fill.order_id}")
        if not fill.source_event_ids and not result.book_snapshot_id:
            violations.append(f"fill_missing_source_evidence:{fill.order_id}")
    if result.state == "REJECTED" and not result.reject_reason:
        violations.append("rejected_without_reason")
    return violations


def resting_order_to_audit_dict(order: RestingOrder | None) -> dict[str, Any] | None:
    if order is None:
        return None
    return {
        "order_id": order.order_id,
        "asset_id": order.asset_id,
        "side": order.side,
        "price": order.price,
        "original_size": order.original_size,
        "remaining_size": order.remaining_size,
        "submitted_at": _ts(order.submitted_at) if order.submitted_at else None,
        "accepted_ts": _ts(order.accepted_ts),
        "queue_ahead": order.queue_ahead,
        "queue_ahead_at_admit": order.queue_ahead_at_admit,
        "state": order.state,
        "fill_count": len(order.fills),
    }


def l2_config_from_params(params: Any) -> L2ExecutionConfig:
    profile = str(getattr(params, "execution_profile", "realistic") or "realistic").strip().lower()
    if profile == "optimistic":
        queue_mode: QueueMode = "optimistic"
        queue_ahead_fraction = Decimal("0.5")
        cancel_ahead_fraction = Decimal("1.0")
        depth_haircut = Decimal("1.0")
        book_ttl_ms = 10_000
        submit_latency_ms = 0
        cancel_latency_ms = 0
        impact_strength_bps = Decimal("0")
    elif profile in {"conservative", "stress"}:
        queue_mode = "trade_only"
        queue_ahead_fraction = Decimal("1.0")
        cancel_ahead_fraction = Decimal("0")
        depth_haircut = Decimal("0.7")
        book_ttl_ms = 5_000
        submit_latency_ms = 100
        cancel_latency_ms = 50
        impact_strength_bps = Decimal("0")
    else:
        queue_mode = "reconciled"
        queue_ahead_fraction = Decimal("0.8")
        cancel_ahead_fraction = Decimal("0.5")
        depth_haircut = Decimal("0.85")
        book_ttl_ms = 5_000
        submit_latency_ms = 100
        cancel_latency_ms = 50
        impact_strength_bps = Decimal("10")
    configured_book_ttl_ms = getattr(params, "book_ttl_ms", None)
    configured_book_ttl_seconds = getattr(params, "max_book_staleness_seconds", None)
    configured_submit_latency_ms = getattr(params, "submit_latency_ms", None)
    configured_latency_seconds = getattr(params, "latency_seconds", None)
    configured_cancel_latency_ms = getattr(params, "cancel_latency_ms", None)
    configured_impact_strength = getattr(params, "impact_strength_bps", None)
    return L2ExecutionConfig(
        mode="optimistic" if profile == "optimistic" else "conservative" if profile in {"conservative", "stress"} else "realistic",
        queue_mode=queue_mode,
        queue_ahead_fraction=_decimal_raw(getattr(params, "queue_ahead_fraction", queue_ahead_fraction) or queue_ahead_fraction),
        cancel_ahead_fraction=_decimal_raw(getattr(params, "cancel_ahead_fraction", cancel_ahead_fraction) or cancel_ahead_fraction),
        depth_haircut=_decimal_raw(getattr(params, "depth_haircut", depth_haircut) or depth_haircut),
        book_ttl_ms=(
            int(_decimal_raw(configured_book_ttl_ms))
            if configured_book_ttl_ms is not None
            else int(_decimal_raw(configured_book_ttl_seconds) * Decimal("1000"))
            if configured_book_ttl_seconds is not None
            else book_ttl_ms
        ),
        submit_latency_ms=(
            int(_decimal_raw(configured_submit_latency_ms))
            if configured_submit_latency_ms is not None
            else int(_decimal_raw(configured_latency_seconds) * Decimal("1000"))
            if configured_latency_seconds is not None
            else submit_latency_ms
        ),
        cancel_latency_ms=int(_decimal_raw(configured_cancel_latency_ms)) if configured_cancel_latency_ms is not None else cancel_latency_ms,
        fee_bps=_decimal_raw(getattr(params, "fee_bps", Decimal("0")) or 0),
        impact_strength_bps=_decimal_raw(configured_impact_strength) if configured_impact_strength is not None else impact_strength_bps,
        allow_cross_gap_execution=not bool(getattr(params, "reject_on_stale_book", True)),
        use_lob_decrease_for_queue=queue_mode != "trade_only",
    )


def l2_execution_config_matrix(params: Any) -> dict[str, dict[str, Any]]:
    return {
        profile: l2_execution_config_to_audit_dict(_l2_config_for_profile(params, profile))
        for profile in ("conservative", "realistic", "optimistic")
    }


def l2_execution_config_to_audit_dict(config: L2ExecutionConfig) -> dict[str, Any]:
    return {
        "mode": config.mode,
        "queue_mode": config.queue_mode,
        "queue_ahead_fraction": config.queue_ahead_fraction,
        "cancel_ahead_fraction": config.cancel_ahead_fraction,
        "depth_haircut": config.depth_haircut,
        "require_fresh_book": config.require_fresh_book,
        "book_ttl_ms": config.book_ttl_ms,
        "submit_latency_ms": config.submit_latency_ms,
        "cancel_latency_ms": config.cancel_latency_ms,
        "replace_latency_ms": config.replace_latency_ms,
        "fee_bps": config.fee_bps,
        "impact_strength_bps": config.impact_strength_bps,
        "allow_cross_gap_execution": config.allow_cross_gap_execution,
        "use_orderfilled_for_maker_queue": config.use_orderfilled_for_maker_queue,
        "use_lob_decrease_for_queue": config.use_lob_decrease_for_queue,
    }


def simulate_l2_depth_execution(
    *,
    snapshots: list[Any],
    decision_block: int | None,
    decision_timestamp: datetime | None,
    side: str,
    target_size: Decimal,
    signal_price: Decimal,
    params: Any,
    market_id: str = "",
    asset_id: str = "",
) -> dict[str, Any]:
    config = l2_config_from_params(params)
    execution_ts = (decision_timestamp or datetime.fromtimestamp(0, tz=timezone.utc)) + timedelta(milliseconds=config.submit_latency_ms)
    snapshot = _select_snapshot_for_execution(snapshots, decision_block=decision_block, execution_ts=execution_ts)
    requested_size = _q(max(Decimal("0"), target_size))
    intent_side: OrderSide = "BUY" if str(side).upper().startswith("BUY") else "SELL"
    intent = StrategyOrderIntent(
        client_order_id=f"sim-{intent_side.lower()}-{decision_block or int(execution_ts.timestamp())}",
        signal_ts=decision_timestamp or execution_ts - timedelta(milliseconds=config.submit_latency_ms),
        market_id=market_id or str(_snapshot_attr(snapshot, "market_id", "") or ""),
        asset_id=asset_id or str(_snapshot_attr(snapshot, "token_id", "") or _snapshot_attr(snapshot, "asset_id", "") or ""),
        side=intent_side,
        order_type="LIMIT",
        limit_price=_limit_price_for_side(params, signal_price, intent_side),
        size=requested_size,
        tif="FAK" if getattr(params, "allow_partial_fill", True) else "FOK",
        post_only=False,
    )
    if snapshot is None:
        return _empty_depth_dict(intent, requested_size, signal_price, "NO_BOOK", "no historical book snapshot", params, side)
    book_snapshot = _book_snapshot_from_any(snapshot, market_id=intent.market_id, asset_id=intent.asset_id, fallback_ts=execution_ts)
    model = L2OrderFilledExecutionModel(config)
    model.apply_snapshot(book_snapshot)
    result = model.execute_taker(intent)
    return _depth_result_to_dict(result, book_snapshot, requested_size, signal_price, params, side, config)


def combine_orderfilled_l2_execution(
    orderfilled_fill: Mapping[str, Any],
    l2_fill: Mapping[str, Any],
    *,
    signal_price: Decimal,
    side: str,
) -> dict[str, Any]:
    requested_size = _decimal_raw(orderfilled_fill.get("requested_size") or orderfilled_fill.get("size"))
    if requested_size <= 0:
        requested_size = _decimal_raw(l2_fill.get("requested_size"))
    requested_notional = _decimal_raw(orderfilled_fill.get("requested_notional"))
    if requested_notional <= 0:
        requested_notional = _q(requested_size * max(_decimal_raw(signal_price), Decimal("0.0000000001")))
    orderfilled_size = _decimal_raw(orderfilled_fill.get("filled_size") or orderfilled_fill.get("size"))
    l2_size = _decimal_raw(l2_fill.get("filled_size") or l2_fill.get("size"))
    notes = list(orderfilled_fill.get("notes") if isinstance(orderfilled_fill.get("notes"), list) else [])
    evidence = {
        "fill_model": "l2_orderfilled_execution_v1",
        "execution_source": "l2_orderfilled",
        "execution_model": "L2OrderFilledExecutionModel",
        "execution_model_version": "l2_orderfilled_execution_v1",
        "execution_audit": l2_fill.get("execution_audit"),
        "orderfilled_execution_source": str(orderfilled_fill.get("execution_source") or "orderfilled_volume"),
        "l2_execution_source": str(l2_fill.get("execution_source") or "l2_orderfilled_depth"),
        "orderfilled_filled_size": orderfilled_size,
        "l2_filled_size": l2_size,
        "book_snapshot_id": l2_fill.get("book_snapshot_id"),
        "snapshot_version": l2_fill.get("snapshot_version"),
        "book_quality": l2_fill.get("book_quality"),
        "residual_book_model": l2_fill.get("residual_book_model"),
        "levels_consumed": l2_fill.get("levels_consumed"),
        "consumed_levels": l2_fill.get("consumed_levels"),
        "best_bid": l2_fill.get("best_bid"),
        "best_ask": l2_fill.get("best_ask"),
        "spread": l2_fill.get("spread"),
        "mid": l2_fill.get("mid"),
        "queue_model": "l2_level_queue_v1",
    }
    if orderfilled_size <= 0 or bool(orderfilled_fill.get("rejected")):
        notes.append("orderfilled_no_fill")
        return _empty_combined(orderfilled_fill, evidence, requested_size, requested_notional, notes)
    if l2_size <= 0 or bool(l2_fill.get("rejected")):
        notes.append("l2_no_executable_depth")
        return _empty_combined(orderfilled_fill, evidence, requested_size, requested_notional, notes)
    filled_size = _q(min(orderfilled_size, l2_size))
    avg_price = _decimal_raw(l2_fill.get("avg_fill_price") or orderfilled_fill.get("avg_fill_price") or signal_price)
    filled_notional = _q(filled_size * avg_price)
    fill_pct = _pct(filled_size, requested_size)
    fee = _decimal_raw(l2_fill.get("fee_cost"))
    slippage = _decimal_raw(l2_fill.get("slippage_cost"))
    rebate = _decimal_raw(orderfilled_fill.get("rebate") or orderfilled_fill.get("rebate_cost"))
    if orderfilled_size > 0:
        rebate = _q(rebate * filled_size / orderfilled_size)
    notes.append("l2_orderfilled_intersection")
    price_key = "entry_price" if str(side).upper().startswith("BUY") else "exit_price"
    return {
        **dict(orderfilled_fill),
        **evidence,
        "requested_notional": requested_notional,
        "filled_notional": filled_notional,
        "expected_fill_notional": filled_notional,
        "actual_fill_notional": filled_notional,
        "fill_pct": fill_pct,
        "fill_probability": min(_decimal_raw(orderfilled_fill.get("fill_probability")), _decimal_raw(l2_fill.get("fill_probability") or l2_fill.get("fill_pct"))),
        "size": filled_size,
        price_key: avg_price,
        "avg_fill_price": avg_price,
        "partial_fill": filled_size < requested_size,
        "rejected": False,
        "fill_status": "FILLED" if filled_size >= requested_size else "PARTIAL",
        "requested_size": requested_size,
        "expected_fill_size": filled_size,
        "actual_fill_size": filled_size,
        "filled_size": filled_size,
        "unfilled_size": _q(max(Decimal("0"), requested_size - filled_size)),
        "available_notional": min(_decimal_raw(orderfilled_fill.get("available_notional")), _decimal_raw(l2_fill.get("available_notional") or l2_fill.get("filled_notional"))),
        "fee_cost": fee,
        "rebate": rebate,
        "rebate_cost": rebate,
        "slippage_cost": slippage,
        "execution_cost": _q(fee + slippage - rebate),
        "notes": notes,
    }


def _levels_to_map(levels: tuple[BookLevel, ...]) -> dict[Decimal, Decimal]:
    result: dict[Decimal, Decimal] = {}
    for level in levels:
        price = _q(level.price)
        size = _q(level.size)
        if price > 0 and size > 0:
            result[price] = result.get(price, Decimal("0")) + size
    return result


def _q(value: Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Q, rounding=ROUND_HALF_UP)


def _normalize_amount(value: Decimal, decimals: int) -> Decimal:
    scale = Decimal(10) ** max(0, int(decimals))
    return Decimal(str(value)) / scale


def _decimal_raw(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")
    return parsed if parsed.is_finite() else Decimal("0")


def _asset(row: Mapping[str, Any], *keys: str) -> str:
    value = _get(row, *keys)
    if value is None or value == "":
        return ""
    text = str(value).strip()
    if text.lower() in {"0x0000000000000000000000000000000000000000", "0x0"}:
        return "0"
    return text.lower() if text.startswith("0x") else text


def _get(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _canonical_orderfilled_key(
    tx_hash: str,
    log_index: int,
    order_hash: str,
    maker_asset_id: str,
    taker_asset_id: str,
) -> str:
    return "|".join(
        [
            str(tx_hash).lower(),
            str(int(log_index)),
            str(order_hash).lower(),
            str(maker_asset_id).lower(),
            str(taker_asset_id).lower(),
        ]
    )


def _pct(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return (numerator * Decimal("100") / denominator).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _l2_config_for_profile(params: Any, profile: str) -> L2ExecutionConfig:
    class ProfileParams:
        def __getattr__(self, name: str) -> Any:
            if name == "execution_profile":
                return profile
            return getattr(params, name)

    return l2_config_from_params(ProfileParams())


def _apply_adverse_impact(
    price: Decimal,
    side: OrderSide,
    order_size: Decimal,
    visible_depth: Decimal,
    strength_bps: Decimal,
) -> Decimal:
    strength = _decimal_raw(strength_bps)
    if strength <= 0 or order_size <= 0 or visible_depth <= 0:
        return _q(price)
    ratio = max(Decimal("0"), _decimal_raw(order_size) / _decimal_raw(visible_depth))
    impact_bps = strength * ratio.sqrt()
    multiplier = Decimal("1") + impact_bps / Decimal("10000") if side == "BUY" else Decimal("1") - impact_bps / Decimal("10000")
    return _q(min(Decimal("1"), max(Decimal("0"), _decimal_raw(price) * multiplier)))


def _select_snapshot_for_execution(
    snapshots: list[Any],
    *,
    decision_block: int | None,
    execution_ts: datetime,
) -> Any | None:
    candidates: list[Any] = []
    for snapshot in snapshots:
        ts = _coerce_datetime(_snapshot_attr(snapshot, "timestamp", None) or _snapshot_attr(snapshot, "ts", None))
        block = _snapshot_attr(snapshot, "block_number", None)
        if ts is not None and ts <= execution_ts:
            candidates.append(snapshot)
            continue
        if decision_block is not None and block is not None and int(block) <= int(decision_block):
            candidates.append(snapshot)
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            _coerce_datetime(_snapshot_attr(item, "timestamp", None) or _snapshot_attr(item, "ts", None)) or datetime.min.replace(tzinfo=timezone.utc),
            int(_snapshot_attr(item, "block_number", -1) or -1),
            int(_snapshot_attr(item, "snapshot_id", 0) or 0),
        ),
        reverse=True,
    )
    return candidates[0]


def _book_snapshot_from_any(snapshot: Any, *, market_id: str = "", asset_id: str = "", fallback_ts: datetime | None = None) -> BookSnapshot:
    ts = _coerce_datetime(_snapshot_attr(snapshot, "timestamp", None) or _snapshot_attr(snapshot, "ts", None)) or fallback_ts or datetime.fromtimestamp(0, tz=timezone.utc)
    return BookSnapshot(
        ts=ts,
        market_id=market_id or str(_snapshot_attr(snapshot, "market_id", "") or ""),
        asset_id=asset_id or str(_snapshot_attr(snapshot, "asset_id", "") or _snapshot_attr(snapshot, "token_id", "") or ""),
        sequence=int(_snapshot_attr(snapshot, "snapshot_id", 0) or 0),
        source=str(_snapshot_attr(snapshot, "source", "clob_orderbook_snapshots") or "clob_orderbook_snapshots"),
        bids=_coerce_book_levels(_snapshot_attr(snapshot, "bids", ()) or ()),
        asks=_coerce_book_levels(_snapshot_attr(snapshot, "asks", ()) or ()),
        hash=str(_snapshot_attr(snapshot, "snapshot_version", "") or "") or None,
        is_full_depth=bool(_snapshot_attr(snapshot, "is_full_depth", False)),
    )


def _coerce_book_levels(rows: Any) -> tuple[BookLevel, ...]:
    levels: list[BookLevel] = []
    for row in rows or ():
        if isinstance(row, BookLevel):
            levels.append(row)
            continue
        if isinstance(row, Mapping):
            levels.append(BookLevel(_decimal_raw(row.get("price")), _decimal_raw(row.get("size"))))
            continue
        try:
            price, size = row
        except Exception:
            continue
        levels.append(BookLevel(_decimal_raw(price), _decimal_raw(size)))
    return tuple(levels)


def _snapshot_attr(snapshot: Any, name: str, default: Any = None) -> Any:
    if snapshot is None:
        return default
    if isinstance(snapshot, Mapping):
        return snapshot.get(name, default)
    return getattr(snapshot, name, default)


def _limit_price_for_side(params: Any, signal_price: Decimal, side: OrderSide) -> Decimal:
    if side == "BUY":
        value = getattr(params, "buy_limit_price", None) or getattr(params, "max_entry_price", None) or signal_price
    else:
        value = getattr(params, "sell_limit_price", None) or getattr(params, "min_exit_price", None) or signal_price
    return min(Decimal("1"), max(Decimal("0"), _decimal_raw(value)))


def _depth_result_to_dict(
    result: OrderExecutionResult,
    snapshot: BookSnapshot,
    requested_size: Decimal,
    signal_price: Decimal,
    params: Any,
    side: str,
    config: L2ExecutionConfig,
) -> dict[str, Any]:
    filled_size = result.filled_size
    avg_price = result.avg_fill_price
    filled_notional = _q(sum((fill.notional for fill in result.fills), Decimal("0")))
    requested_notional = _q(requested_size * max(_decimal_raw(signal_price), Decimal("0.0000000001")))
    fill_pct = _pct(filled_size, requested_size)
    price_key = "entry_price" if str(side).upper().startswith("BUY") else "exit_price"
    best_bid = max((level.price for level in snapshot.bids), default=None)
    best_ask = min((level.price for level in snapshot.asks), default=None)
    fee = _q(sum((fill.fee for fill in result.fills), Decimal("0")))
    side_upper = str(side).upper()
    depth_side = snapshot.asks if side_upper.startswith("BUY") else snapshot.bids
    visible_depth_size = _q(sum((level.size for level in depth_side), Decimal("0")))
    visible_depth_notional = _q(sum((level.price * level.size for level in depth_side), Decimal("0")))
    order_size_to_visible_depth = _pct(requested_size, visible_depth_size) if visible_depth_size > 0 else Decimal("0")
    slippage = _q(abs(avg_price - _decimal_raw(signal_price)) * filled_size) if filled_size > 0 and avg_price > 0 else Decimal("0")
    return {
        "requested_notional": requested_notional,
        "filled_notional": filled_notional,
        "expected_fill_notional": filled_notional,
        "actual_fill_notional": filled_notional,
        "fill_pct": fill_pct,
        "fill_probability": fill_pct,
        "size": filled_size,
        price_key: avg_price if avg_price > 0 else None,
        "avg_fill_price": avg_price,
        "liquidity_cap_pct": max(Decimal("0"), _decimal_raw(getattr(params, "liquidity_cap_pct", "100"))),
        "min_fill_pct": max(Decimal("0"), _decimal_raw(getattr(params, "min_fill_pct", "0"))),
        "partial_fill": result.state == "PARTIAL",
        "rejected": result.state not in {"FILLED", "PARTIAL"},
        "fill_status": result.state,
        "book_snapshot_id": snapshot.sequence,
        "snapshot_version": snapshot.hash or snapshot.event_id,
        "staleness_seconds": None,
        "staleness_blocks": None,
        "requested_size": requested_size,
        "filled_size": filled_size,
        "actual_fill_size": filled_size,
        "expected_fill_size": filled_size,
        "unfilled_size": result.remaining_size,
        "fee_cost": fee,
        "rebate": Decimal("0"),
        "rebate_cost": Decimal("0"),
        "slippage_cost": slippage,
        "execution_cost": _q(fee + slippage),
        "execution_source": "l2_orderfilled_depth",
        "execution_model": "L2OrderFilledExecutionModel",
        "execution_model_version": "l2_orderfilled_execution_v1",
        "execution_audit": result.audit_dict(),
        "execution_config": l2_execution_config_to_audit_dict(config),
        "execution_profile_config_matrix": l2_execution_config_matrix(params),
        "residual_book_model": "visible_depth_residual_v1",
        "queue_model": "l2_level_queue_v1",
        "book_quality": book_quality_to_audit_dict(result.book_quality) or ("HIGH" if snapshot.is_full_depth else "MEDIUM"),
        "block_volume": Decimal("0"),
        "trade_count": 0,
        "available_notional": filled_notional,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": _q(best_ask - best_bid) if best_bid is not None and best_ask is not None else None,
        "mid": _q((best_bid + best_ask) / Decimal("2")) if best_bid is not None and best_ask is not None else None,
        "levels_consumed": len(result.fills),
        "bid_size_depth": _q(sum((level.size for level in snapshot.bids), Decimal("0"))),
        "ask_size_depth": _q(sum((level.size for level in snapshot.asks), Decimal("0"))),
        "visible_depth_size": visible_depth_size,
        "visible_depth_notional": visible_depth_notional,
        "order_size_to_visible_depth_pct": order_size_to_visible_depth,
        "simulated_taker_volume": filled_size,
        "impact_strength_bps": config.impact_strength_bps,
        "consumed_levels": [{"price": fill.price, "size": fill.size, "consumed_size": fill.size} for fill in result.fills],
        "notes": ["l2_orderfilled_execution", result.reject_reason] if result.reject_reason else ["l2_orderfilled_execution"],
        "depth_haircut": config.depth_haircut,
        "submit_latency_ms": config.submit_latency_ms,
    }


def _empty_depth_dict(
    intent: StrategyOrderIntent,
    requested_size: Decimal,
    signal_price: Decimal,
    status: str,
    reason: str,
    params: Any,
    side: str,
) -> dict[str, Any]:
    requested_notional = _q(requested_size * max(_decimal_raw(signal_price), Decimal("0.0000000001")))
    price_key = "entry_price" if str(side).upper().startswith("BUY") else "exit_price"
    config = l2_config_from_params(params)
    received_at = intent.signal_ts + timedelta(milliseconds=config.submit_latency_ms)
    return {
        "requested_notional": requested_notional,
        "filled_notional": Decimal("0"),
        "expected_fill_notional": Decimal("0"),
        "actual_fill_notional": Decimal("0"),
        "fill_pct": Decimal("0"),
        "fill_probability": Decimal("0"),
        "size": Decimal("0"),
        price_key: None,
        "avg_fill_price": Decimal("0"),
        "liquidity_cap_pct": max(Decimal("0"), _decimal_raw(getattr(params, "liquidity_cap_pct", "100"))),
        "min_fill_pct": max(Decimal("0"), _decimal_raw(getattr(params, "min_fill_pct", "0"))),
        "partial_fill": False,
        "rejected": True,
        "fill_status": status,
        "requested_size": requested_size,
        "filled_size": Decimal("0"),
        "actual_fill_size": Decimal("0"),
        "expected_fill_size": Decimal("0"),
        "unfilled_size": requested_size,
        "fee_cost": Decimal("0"),
        "rebate": Decimal("0"),
        "rebate_cost": Decimal("0"),
        "slippage_cost": Decimal("0"),
        "execution_cost": Decimal("0"),
        "execution_source": "l2_orderfilled_depth",
        "execution_model": "L2OrderFilledExecutionModel",
        "execution_model_version": "l2_orderfilled_execution_v1",
        "execution_audit": {
            "schema_version": "l2_orderfilled_execution_audit_v1",
            "order_id": intent.client_order_id,
            "state": "REJECTED",
            "submitted_at": _ts(intent.signal_ts),
            "venue_received_at": _ts(received_at),
            "filled_size": Decimal("0"),
            "remaining_size": requested_size,
            "avg_fill_price": Decimal("0"),
            "reject_reason": reason,
            "book_snapshot_id": None,
            "queue_ahead_at_admit": None,
            "book_quality": None,
            "mode": config.mode,
            "fill_count": 0,
            "source_event_ids": [],
            "fills": [],
            "resting_order": None,
        },
        "execution_config": l2_execution_config_to_audit_dict(config),
        "execution_profile_config_matrix": l2_execution_config_matrix(params),
        "residual_book_model": "visible_depth_residual_v1",
        "queue_model": "l2_level_queue_v1",
        "book_snapshot_id": None,
        "snapshot_version": None,
        "available_notional": Decimal("0"),
        "visible_depth_size": Decimal("0"),
        "visible_depth_notional": Decimal("0"),
        "order_size_to_visible_depth_pct": Decimal("0"),
        "simulated_taker_volume": Decimal("0"),
        "impact_strength_bps": config.impact_strength_bps,
        "notes": ["l2_orderfilled_execution", reason],
    }


def _empty_combined(
    orderfilled_fill: Mapping[str, Any],
    evidence: dict[str, Any],
    requested_size: Decimal,
    requested_notional: Decimal,
    notes: list[Any],
) -> dict[str, Any]:
    return {
        **dict(orderfilled_fill),
        **evidence,
        "requested_notional": requested_notional,
        "filled_notional": Decimal("0"),
        "expected_fill_notional": Decimal("0"),
        "actual_fill_notional": Decimal("0"),
        "fill_pct": Decimal("0"),
        "fill_probability": Decimal("0"),
        "size": Decimal("0"),
        "partial_fill": False,
        "rejected": True,
        "fill_status": "REJECTED",
        "requested_size": requested_size,
        "expected_fill_size": Decimal("0"),
        "actual_fill_size": Decimal("0"),
        "filled_size": Decimal("0"),
        "unfilled_size": requested_size,
        "fee_cost": Decimal("0"),
        "rebate": Decimal("0"),
        "rebate_cost": Decimal("0"),
        "slippage_cost": Decimal("0"),
        "execution_cost": Decimal("0"),
        "notes": notes,
    }


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _ts(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _event_effective_ts(
    event: BookSnapshot | BookDelta | FillTick | StrategyOrderIntent | StrategyCancelIntent,
    config: L2ExecutionConfig,
) -> datetime:
    if isinstance(event, StrategyOrderIntent):
        return event.signal_ts + timedelta(milliseconds=config.submit_latency_ms)
    if isinstance(event, StrategyCancelIntent):
        latency = config.submit_latency_ms if event.cancel_latency_ms is None else event.cancel_latency_ms
        return event.signal_ts + timedelta(milliseconds=latency)
    return event.ts


def _event_priority(event: BookSnapshot | BookDelta | FillTick | StrategyOrderIntent | StrategyCancelIntent) -> int:
    if isinstance(event, BookSnapshot):
        return 0
    if isinstance(event, BookDelta):
        return 1
    if isinstance(event, FillTick):
        return 2
    if isinstance(event, StrategyCancelIntent):
        return 3
    return 4
