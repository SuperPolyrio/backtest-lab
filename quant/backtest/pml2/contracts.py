"""Canonical contracts for prediction-market L2 replay.

The contracts keep raw token coordinates and canonical YES-economic
coordinates together.  This prevents YES and NO mirror books from becoming
two independent sources of liquidity.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from enum import Enum
from hashlib import sha256
from typing import Any

QTY_QUANTUM = Decimal("0.0000000001")
PRICE_QUANTUM = Decimal("0.0000000001")
ORDER_AMOUNT_TOLERANCE = Decimal("0.000001")


class BookSyncState(str, Enum):
    UNINITIALIZED = "UNINITIALIZED"
    SYNCING = "SYNCING"
    SYNCED = "SYNCED"
    GAP = "GAP"
    STALE = "STALE"
    HANDOVER_PENDING = "HANDOVER_PENDING"
    WAITING_FOR_SNAPSHOT = "WAITING_FOR_SNAPSHOT"
    CLOSED = "CLOSED"


class BookValidityMode(str, Enum):
    """Controls whether elapsed wall time can invalidate a reconstructed book."""

    EVENT_DRIVEN = "EVENT_DRIVEN"
    MAX_AGE = "MAX_AGE"


class EventType(str, Enum):
    MARKET_LIFECYCLE = "MARKET_LIFECYCLE"
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    BOOK_FRAME_BATCH = "BOOK_FRAME_BATCH"
    BOOK_LEVEL_BATCH = "BOOK_LEVEL_BATCH"
    BOOK_DELTA = "BOOK_DELTA"
    TRADE = "TRADE"
    ORDER_ARRIVAL = "ORDER_ARRIVAL"
    ORDER_GROUP_ARRIVAL = "ORDER_GROUP_ARRIVAL"
    VENUE_EXECUTE = "VENUE_EXECUTE"
    CANCEL_ARRIVAL = "CANCEL_ARRIVAL"
    RECONCILE_TIMEOUT = "RECONCILE_TIMEOUT"
    EXPIRE = "EXPIRE"
    STRATEGY_ACTION = "STRATEGY_ACTION"
    EXTERNAL_SIGNAL = "EXTERNAL_SIGNAL"
    LOCAL_DELIVERY = "LOCAL_DELIVERY"
    RESPONSE = "RESPONSE"
    DATA_WAIT_TIMEOUT = "DATA_WAIT_TIMEOUT"


class Outcome(str, Enum):
    YES = "YES"
    NO = "NO"


class RawOrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderAmountUnit(str, Enum):
    SHARES = "SHARES"
    QUOTE = "QUOTE"


class VenueAdmissionStatus(str, Enum):
    UNKNOWN = "UNKNOWN"
    ACCEPTED = "ACCEPTED"
    REJECTED_MIN_ORDER_SIZE = "REJECTED_MIN_ORDER_SIZE"


class EconomicAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class EconomicBookSide(str, Enum):
    BID = "BID"
    ASK = "ASK"


class TimeInForce(str, Enum):
    FOK = "FOK"
    FAK = "FAK"
    # Polymarket/CLOB clients commonly call this IOC.  PML2 keeps the
    # submitted label for audit purposes; the execution contract is the same
    # as FAK: consume immediately available marketable depth, then cancel the
    # remainder instead of resting it.
    IOC = "IOC"
    GTC = "GTC"
    GTD = "GTD"


class SubmissionOnStaleBook(str, Enum):
    FAIL_CLOSED = "FAIL_CLOSED"
    WAIT_FOR_FRESH = "WAIT_FOR_FRESH"


class ModeledExecutionMode(str, Enum):
    OFF = "OFF"
    EXPECTED = "EXPECTED"
    MONTE_CARLO = "MONTE_CARLO"


class TransportCoverageState(str, Enum):
    FRESH = "FRESH"
    QUIET_BUT_COVERED = "QUIET_BUT_COVERED"
    COVERAGE_PROVEN = "COVERAGE_PROVEN"
    TRANSPORT_BACKLOG = "TRANSPORT_BACKLOG"
    COVERAGE_GAP = "COVERAGE_GAP"
    MISSING = "MISSING"


class FillEvidenceTier(str, Enum):
    OBSERVED_L2 = "OBSERVED_L2"
    MODELED_EXPECTED = "MODELED_EXPECTED"
    MODELED_MONTE_CARLO = "MODELED_MONTE_CARLO"


class ReplayValidationMode(str, Enum):
    RESEARCH = "RESEARCH"
    FORMAL = "FORMAL"


class ReplayAuditMode(str, Enum):
    FULL = "FULL"
    CHAIN_ONLY = "CHAIN_ONLY"


class OrderGroupPolicy(str, Enum):
    SEQUENTIAL = "SEQUENTIAL"
    SIMULTANEOUS_BEST_EFFORT = "SIMULTANEOUS_BEST_EFFORT"
    ALL_LEGS_FOK = "ALL_LEGS_FOK"
    HEDGE_ON_LEG_FAILURE = "HEDGE_ON_LEG_FAILURE"


class TradingMode(str, Enum):
    LIVE = "LIVE"
    POST_ONLY = "POST_ONLY"
    CANCEL_ONLY = "CANCEL_ONLY"
    PAUSED = "PAUSED"
    CLOSED = "CLOSED"


@dataclass(frozen=True)
class BinaryMarketIdentity:
    """Authoritative coordinates for one binary prediction market.

    This mapping is intentionally separate from event and order payloads.  A
    caller opting into formal validation must supply an independently frozen
    mapping so that mutually consistent but wrong order/event identifiers do
    not silently pass replay.
    """

    condition_id: str
    market_id: str
    yes_asset_id: str
    no_asset_id: str

    def __post_init__(self) -> None:
        values = (
            self.condition_id,
            self.market_id,
            self.yes_asset_id,
            self.no_asset_id,
        )
        if any(not str(value).strip() for value in values):
            raise ValueError("binary market identity fields are required")
        if self.yes_asset_id.casefold() == self.no_asset_id.casefold():
            raise ValueError("YES and NO asset ids must differ")

    def asset_id_for(self, outcome: Outcome) -> str:
        return self.yes_asset_id if outcome == Outcome.YES else self.no_asset_id

    def validate_coordinates(
        self,
        *,
        condition_id: str,
        market_id: str,
        asset_id: str | None = None,
        outcome: Outcome | None = None,
    ) -> None:
        if condition_id.casefold() != self.condition_id.casefold():
            raise ValueError(
                "condition_id does not match frozen binary market identity"
            )
        if market_id.casefold() != self.market_id.casefold():
            raise ValueError("market_id does not match frozen condition mapping")
        if (asset_id is None) != (outcome is None):
            raise ValueError("asset_id and outcome must be validated together")
        if asset_id is not None and outcome is not None:
            expected = self.asset_id_for(outcome)
            if asset_id.casefold() != expected.casefold():
                raise ValueError(
                    f"asset_id does not match frozen {outcome.value} token mapping"
                )


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    WAITING_FOR_DATA = "WAITING_FOR_DATA"
    WORKING = "WORKING"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    DATA_NOT_READY = "DATA_NOT_READY"


class MatchFinalityState(str, Enum):
    MATCHED = "MATCHED"
    PENDING_SETTLEMENT = "PENDING_SETTLEMENT"
    MINED = "MINED"
    RETRYING = "RETRYING"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"


class CtfMatchType(str, Enum):
    COMPLEMENTARY = "COMPLEMENTARY"
    MINT = "MINT"
    MERGE = "MERGE"
    UNKNOWN_L2 = "UNKNOWN_L2"


EVENT_PRIORITY: dict[EventType, int] = {
    EventType.MARKET_LIFECYCLE: 10,
    EventType.EXPIRE: 11,
    # Historical market data observed at the exact exchange timestamp must be
    # applied before a simulated order arrives at that timestamp.  Otherwise
    # replay can consume stale pre-event depth while claiming a
    # market-data-first tie policy.
    EventType.BOOK_SNAPSHOT: 15,
    EventType.BOOK_FRAME_BATCH: 15,
    EventType.BOOK_LEVEL_BATCH: 15,
    EventType.BOOK_DELTA: 15,
    EventType.TRADE: 15,
    EventType.ORDER_ARRIVAL: 20,
    EventType.ORDER_GROUP_ARRIVAL: 20,
    EventType.VENUE_EXECUTE: 21,
    EventType.CANCEL_ARRIVAL: 22,
    EventType.RECONCILE_TIMEOUT: 35,
    EventType.STRATEGY_ACTION: 40,
    EventType.EXTERNAL_SIGNAL: 41,
    EventType.LOCAL_DELIVERY: 50,
    EventType.RESPONSE: 51,
    # A fresh local delivery at the exact deadline gets one final chance to
    # release a waiting order before its data-wait timeout is evaluated.
    EventType.DATA_WAIT_TIMEOUT: 52,
}


def utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _validate_source_received_clock(value: Any) -> None:
    received = getattr(value, "source_received_ts", None)
    if received is None:
        return
    normalized = utc(received, field_name="source_received_ts")
    object.__setattr__(value, "source_received_ts", normalized)
    if normalized < value.exchange_ts:
        raise ValueError("source_received_ts cannot precede exchange_ts")
    if normalized > value.local_ts:
        raise ValueError("source_received_ts cannot exceed local_ts")


def price(value: Decimal | str | float) -> Decimal:
    parsed = Decimal(str(value)).quantize(PRICE_QUANTUM, rounding=ROUND_HALF_UP)
    if not Decimal(0) <= parsed <= Decimal(1):
        raise ValueError("price must be within [0, 1]")
    return parsed


def qty(value: Decimal | str | float) -> Decimal:
    parsed = Decimal(str(value)).quantize(QTY_QUANTUM, rounding=ROUND_HALF_UP)
    if parsed < 0:
        raise ValueError("quantity cannot be negative")
    return parsed


def qty_down(value: Decimal | str | float) -> Decimal:
    parsed = Decimal(str(value)).quantize(QTY_QUANTUM, rounding=ROUND_DOWN)
    if parsed < 0:
        raise ValueError("quantity cannot be negative")
    return parsed


def canonical_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return utc(value, field_name="timestamp").isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {
            str(key): canonical_value(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        return [canonical_value(item) for item in value]
    return value


def canonical_hash(value: Any) -> str:
    payload = canonical_value(
        asdict(value) if hasattr(value, "__dataclass_fields__") else value
    )
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class SubmissionPolicy:
    """Local data gate applied before an order is submitted to the venue."""

    on_stale_book: SubmissionOnStaleBook = SubmissionOnStaleBook.FAIL_CLOSED
    max_data_wait_ms: int = 0
    on_timeout: OrderStatus = OrderStatus.DATA_NOT_READY

    def __post_init__(self) -> None:
        if self.max_data_wait_ms < 0:
            raise ValueError("max_data_wait_ms cannot be negative")
        if self.on_timeout != OrderStatus.DATA_NOT_READY:
            raise ValueError("submission timeout must fail as DATA_NOT_READY")
        if (
            self.on_stale_book == SubmissionOnStaleBook.WAIT_FOR_FRESH
            and self.max_data_wait_ms <= 0
        ):
            raise ValueError("WAIT_FOR_FRESH requires max_data_wait_ms > 0")

    def as_dict(self) -> dict[str, Any]:
        return canonical_value(asdict(self))


@dataclass(frozen=True)
class ModeledExecutionPolicy:
    """Controls estimates which are deliberately separate from observed fills."""

    maker: ModeledExecutionMode = ModeledExecutionMode.OFF
    gap: ModeledExecutionMode = ModeledExecutionMode.OFF
    random_seed: int = 0

    def as_dict(self) -> dict[str, Any]:
        return canonical_value(asdict(self))


@dataclass(frozen=True)
class TransportCoverageWindow:
    """Offline proof that an L2 state is usable over a half-open interval."""

    proof_id: str
    condition_id: str
    market_id: str
    start_ts: datetime
    end_ts: datetime
    allowed: bool
    state: TransportCoverageState
    reason: str
    asset_ids: tuple[str, ...]
    source: str

    def __post_init__(self) -> None:
        if not self.proof_id or not self.condition_id or not self.source:
            raise ValueError("transport coverage identity is required")
        object.__setattr__(
            self,
            "start_ts",
            utc(self.start_ts, field_name="coverage start_ts"),
        )
        object.__setattr__(
            self,
            "end_ts",
            utc(self.end_ts, field_name="coverage end_ts"),
        )
        if self.end_ts <= self.start_ts:
            raise ValueError("transport coverage end_ts must follow start_ts")
        normalized_assets = tuple(
            sorted({str(item) for item in self.asset_ids if str(item)})
        )
        if not normalized_assets:
            raise ValueError("transport coverage requires asset_ids")
        object.__setattr__(self, "asset_ids", normalized_assets)

    def contains(self, at: datetime) -> bool:
        observed = utc(at, field_name="coverage lookup timestamp")
        return self.start_ts <= observed < self.end_ts


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    size: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "price", price(self.price))
        object.__setattr__(self, "size", qty(self.size))
        if self.size <= 0:
            raise ValueError("book level size must be positive")


@dataclass(frozen=True)
class EventEnvelope:
    event_id: str
    event_type: EventType
    exchange_ts: datetime
    local_ts: datetime
    source: str
    source_sequence: int = 0
    source_batch_sequence: int = 0
    ingest_id: str = ""
    payload: Any = field(default=None, compare=False)

    def __post_init__(self) -> None:
        if not self.event_id or not self.source:
            raise ValueError("event_id and source are required")
        object.__setattr__(
            self, "exchange_ts", utc(self.exchange_ts, field_name="exchange_ts")
        )
        object.__setattr__(self, "local_ts", utc(self.local_ts, field_name="local_ts"))
        if self.local_ts < self.exchange_ts:
            raise ValueError("local_ts cannot precede exchange_ts")

    @property
    def exchange_sort_key(self) -> tuple[datetime, int, int, int, str, str]:
        return (
            self.exchange_ts,
            EVENT_PRIORITY[self.event_type],
            int(self.source_sequence),
            int(self.source_batch_sequence),
            self.ingest_id,
            self.event_id,
        )


@dataclass(frozen=True)
class BookSnapshotEvent:
    snapshot_id: str
    condition_id: str
    market_id: str
    asset_id: str
    outcome: Outcome
    exchange_ts: datetime
    local_ts: datetime
    book_epoch: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    source: str
    sequence: int | None = None
    is_full_depth: bool = True
    is_truncated: bool = False
    depth_scope: str = "FULL"
    tick_size: Decimal | None = None
    min_order_size: Decimal | None = None
    book_hash: str = ""
    source_received_ts: datetime | None = None

    def __post_init__(self) -> None:
        if not self.snapshot_id or not self.condition_id or not self.asset_id:
            raise ValueError("snapshot identity is required")
        object.__setattr__(
            self, "exchange_ts", utc(self.exchange_ts, field_name="exchange_ts")
        )
        object.__setattr__(self, "local_ts", utc(self.local_ts, field_name="local_ts"))
        if self.local_ts < self.exchange_ts:
            raise ValueError("snapshot local_ts cannot precede exchange_ts")
        _validate_source_received_clock(self)
        if self.book_epoch < 0:
            raise ValueError("book_epoch cannot be negative")
        if self.tick_size is not None:
            object.__setattr__(self, "tick_size", price(self.tick_size))
            if self.tick_size <= 0:
                raise ValueError("tick_size must be positive")
        if self.min_order_size is not None:
            object.__setattr__(self, "min_order_size", qty(self.min_order_size))
            if self.min_order_size <= 0:
                raise ValueError("min_order_size must be positive")
        if self.is_full_depth and self.is_truncated:
            raise ValueError("full-depth snapshot cannot also be truncated")


@dataclass(frozen=True)
class BookDeltaEvent:
    event_id: str
    condition_id: str
    market_id: str
    asset_id: str
    outcome: Outcome
    exchange_ts: datetime
    local_ts: datetime
    book_epoch: int
    side: EconomicBookSide
    price: Decimal
    new_size: Decimal
    source: str
    sequence: int | None = None
    linked_trade_event_ids: tuple[str, ...] = ()
    source_received_ts: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "exchange_ts", utc(self.exchange_ts, field_name="exchange_ts")
        )
        object.__setattr__(self, "local_ts", utc(self.local_ts, field_name="local_ts"))
        _validate_source_received_clock(self)
        object.__setattr__(self, "price", price(self.price))
        object.__setattr__(self, "new_size", qty(self.new_size))


@dataclass(frozen=True)
class BookLevelBatchEvent:
    """One source message containing atomic absolute level updates."""

    event_id: str
    condition_id: str
    market_id: str
    asset_id: str
    outcome: Outcome
    exchange_ts: datetime
    local_ts: datetime
    book_epoch: int
    updates: tuple[BookDeltaEvent, ...]
    source: str
    source_sequence: int = 0
    source_batch_sequence: int = 0
    source_received_ts: datetime | None = None
    authoritative_best_bid: Decimal | None = None
    authoritative_best_ask: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.event_id or not self.updates:
            raise ValueError("book level batch identity and updates are required")
        object.__setattr__(
            self, "exchange_ts", utc(self.exchange_ts, field_name="exchange_ts")
        )
        object.__setattr__(self, "local_ts", utc(self.local_ts, field_name="local_ts"))
        if self.local_ts < self.exchange_ts:
            raise ValueError("batch local_ts cannot precede exchange_ts")
        _validate_source_received_clock(self)
        if (self.authoritative_best_bid is None) != (
            self.authoritative_best_ask is None
        ):
            raise ValueError("authoritative top must provide both bid and ask")
        if self.authoritative_best_bid is not None:
            assert self.authoritative_best_ask is not None
            normalized_bid = price(self.authoritative_best_bid)
            normalized_ask = price(self.authoritative_best_ask)
            object.__setattr__(
                self,
                "authoritative_best_bid",
                normalized_bid,
            )
            object.__setattr__(
                self,
                "authoritative_best_ask",
                normalized_ask,
            )
            if normalized_bid >= normalized_ask:
                raise ValueError("authoritative best bid must be below best ask")
        for update in self.updates:
            if (
                update.condition_id != self.condition_id
                or update.market_id != self.market_id
                or update.asset_id != self.asset_id
                or update.outcome != self.outcome
                or update.book_epoch != self.book_epoch
                or update.exchange_ts != self.exchange_ts
                or update.source_received_ts != self.source_received_ts
                or update.local_ts != self.local_ts
                or update.source != self.source
            ):
                raise ValueError("all level updates must share the batch envelope")


@dataclass(frozen=True)
class BookFrameBatchEvent:
    """One globally atomic raw frame containing one or more outcome legs."""

    event_id: str
    condition_id: str
    market_id: str
    exchange_ts: datetime
    source_received_ts: datetime
    local_ts: datetime
    book_epoch: int
    batches: tuple[BookLevelBatchEvent, ...]
    source: str
    source_sequence: int = 0
    source_batch_sequence: int = 0

    def __post_init__(self) -> None:
        if not self.event_id or not self.batches:
            raise ValueError("book frame identity and leg batches are required")
        object.__setattr__(
            self, "exchange_ts", utc(self.exchange_ts, field_name="exchange_ts")
        )
        object.__setattr__(
            self,
            "source_received_ts",
            utc(self.source_received_ts, field_name="source_received_ts"),
        )
        object.__setattr__(self, "local_ts", utc(self.local_ts, field_name="local_ts"))
        _validate_source_received_clock(self)
        legs: set[tuple[str, Outcome]] = set()
        for batch in self.batches:
            if (
                batch.condition_id != self.condition_id
                or batch.market_id != self.market_id
                or batch.exchange_ts != self.exchange_ts
                or batch.source_received_ts != self.source_received_ts
                or batch.local_ts != self.local_ts
                or batch.book_epoch != self.book_epoch
                or batch.source != self.source
            ):
                raise ValueError("all leg batches must share the raw frame envelope")
            if (
                batch.authoritative_best_bid is None
                or batch.authoritative_best_ask is None
            ):
                raise ValueError(
                    "raw frame leg requires a complete authoritative top pair"
                )
            leg = (batch.asset_id, batch.outcome)
            if leg in legs:
                raise ValueError("raw frame cannot repeat an asset/outcome leg")
            legs.add(leg)


@dataclass(frozen=True)
class TradeEvent:
    event_id: str
    condition_id: str
    market_id: str
    asset_id: str
    outcome: Outcome
    exchange_ts: datetime
    local_ts: datetime
    book_epoch: int
    price: Decimal
    size: Decimal
    aggressor_side: RawOrderSide
    source: str
    source_sequence: int = 0
    event_group_id: str = ""
    evidence_link_id: str = ""
    evidence_kind: str = "TRADE_PRINT"
    source_event_ids: tuple[str, ...] = ()
    source_received_ts: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "exchange_ts", utc(self.exchange_ts, field_name="exchange_ts")
        )
        object.__setattr__(self, "local_ts", utc(self.local_ts, field_name="local_ts"))
        _validate_source_received_clock(self)
        object.__setattr__(self, "price", price(self.price))
        object.__setattr__(self, "size", qty(self.size))
        if self.size <= 0:
            raise ValueError("trade size must be positive")


@dataclass(frozen=True)
class MarketLifecycleEvent:
    event_id: str
    condition_id: str
    market_id: str
    exchange_ts: datetime
    local_ts: datetime
    trading_mode: TradingMode
    source: str
    source_sequence: int = 0
    requires_fresh_snapshot: bool = False

    def __post_init__(self) -> None:
        if not self.event_id or not self.condition_id or not self.source:
            raise ValueError("market lifecycle identity is required")
        object.__setattr__(
            self, "exchange_ts", utc(self.exchange_ts, field_name="exchange_ts")
        )
        object.__setattr__(self, "local_ts", utc(self.local_ts, field_name="local_ts"))
        if self.local_ts < self.exchange_ts:
            raise ValueError("lifecycle local_ts cannot precede exchange_ts")


@dataclass(frozen=True)
class Pml2OrderIntent:
    run_id: str
    order_id: str
    strategy_id: str
    condition_id: str
    market_id: str
    asset_id: str
    outcome: Outcome
    side: RawOrderSide
    size: Decimal
    limit_price: Decimal
    tif: TimeInForce
    signal_ts: datetime
    observed_ts: datetime
    submit_ts: datetime
    amount_unit: OrderAmountUnit = OrderAmountUnit.SHARES
    signed_maker_amount: Decimal | None = None
    signed_taker_amount: Decimal | None = None
    venue_admission: VenueAdmissionStatus = VenueAdmissionStatus.UNKNOWN
    venue_admission_evidence_id: str = ""
    post_only: bool = False
    expires_at: datetime | None = None
    entry_latency_ms: int | None = None
    cancel_latency_ms: int | None = None
    response_latency_ms: int | None = None
    venue_delay_ms: int | None = None
    fee_rate: Decimal = Decimal(0)
    fee_exponent: Decimal = Decimal(1)
    match_type_hint: CtfMatchType = CtfMatchType.UNKNOWN_L2
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not all(
            (
                self.run_id,
                self.order_id,
                self.strategy_id,
                self.condition_id,
                self.asset_id,
            )
        ):
            raise ValueError("order identity fields are required")
        object.__setattr__(self, "size", qty(self.size))
        object.__setattr__(self, "limit_price", price(self.limit_price))
        if not isinstance(self.amount_unit, OrderAmountUnit):
            object.__setattr__(self, "amount_unit", OrderAmountUnit(self.amount_unit))
        if not isinstance(self.venue_admission, VenueAdmissionStatus):
            object.__setattr__(
                self,
                "venue_admission",
                VenueAdmissionStatus(self.venue_admission),
            )
        signed_values = (self.signed_maker_amount, self.signed_taker_amount)
        if (signed_values[0] is None) != (signed_values[1] is None):
            raise ValueError(
                "signed_maker_amount and signed_taker_amount must appear together"
            )
        if signed_values[0] is not None and signed_values[1] is not None:
            maker_amount = qty(signed_values[0])
            taker_amount = qty(signed_values[1])
            if maker_amount <= 0 or taker_amount <= 0:
                raise ValueError("signed order amounts must be positive")
            object.__setattr__(self, "signed_maker_amount", maker_amount)
            object.__setattr__(self, "signed_taker_amount", taker_amount)
        object.__setattr__(self, "fee_rate", max(Decimal(0), Decimal(self.fee_rate)))
        object.__setattr__(
            self, "fee_exponent", max(Decimal(0), Decimal(self.fee_exponent))
        )
        if self.size <= 0 or not Decimal(0) < self.limit_price < Decimal(1):
            raise ValueError("order size and limit price must be positive")
        if self.amount_unit == OrderAmountUnit.QUOTE:
            if self.side != RawOrderSide.BUY:
                raise ValueError("QUOTE amount is supported only for BUY orders")
            if self.tif not in {TimeInForce.FOK, TimeInForce.FAK, TimeInForce.IOC}:
                raise ValueError("QUOTE BUY is supported only for immediate TIF orders")
            if self.post_only:
                raise ValueError("QUOTE BUY cannot be post_only")
        if (
            self.venue_admission != VenueAdmissionStatus.UNKNOWN
            and not self.venue_admission_evidence_id.strip()
        ):
            raise ValueError("observed venue admission requires an evidence id")
        for field_name in ("signal_ts", "observed_ts", "submit_ts"):
            object.__setattr__(
                self, field_name, utc(getattr(self, field_name), field_name=field_name)
            )
        if not self.signal_ts <= self.observed_ts <= self.submit_ts:
            raise ValueError("signal_ts <= observed_ts <= submit_ts is required")
        if self.expires_at is not None:
            object.__setattr__(
                self, "expires_at", utc(self.expires_at, field_name="expires_at")
            )
        if self.tif == TimeInForce.GTD and self.expires_at is None:
            raise ValueError("GTD requires expires_at")

    @property
    def signed_quote_amount(self) -> Decimal | None:
        if self.signed_maker_amount is None or self.signed_taker_amount is None:
            return None
        return (
            self.signed_maker_amount
            if self.side == RawOrderSide.BUY
            else self.signed_taker_amount
        )

    @property
    def signed_share_amount(self) -> Decimal | None:
        if self.signed_maker_amount is None or self.signed_taker_amount is None:
            return None
        return (
            self.signed_taker_amount
            if self.side == RawOrderSide.BUY
            else self.signed_maker_amount
        )

    @property
    def effective_requested_amount(self) -> Decimal:
        if self.amount_unit == OrderAmountUnit.QUOTE:
            return qty(self.signed_quote_amount or self.size)
        return qty(self.signed_share_amount or self.size)

    @property
    def requested_share_size(self) -> Decimal:
        signed = self.signed_share_amount
        if signed is not None:
            return qty(signed)
        if self.amount_unit == OrderAmountUnit.QUOTE:
            return qty(self.effective_requested_amount / self.limit_price)
        return qty(self.effective_requested_amount)

    def amount_for_fill(self, *, fill_size: Decimal, fill_price: Decimal) -> Decimal:
        return qty(
            fill_size * fill_price
            if self.amount_unit == OrderAmountUnit.QUOTE
            else fill_size
        )


@dataclass(frozen=True)
class OrderGroupIntent:
    run_id: str
    group_id: str
    strategy_id: str
    policy: OrderGroupPolicy
    legs: tuple[Pml2OrderIntent, ...]
    hedge_legs: tuple[Pml2OrderIntent, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.run_id or not self.group_id or not self.strategy_id:
            raise ValueError("order group identity fields are required")
        if not self.legs:
            raise ValueError("order group requires at least one primary leg")
        if self.policy != OrderGroupPolicy.HEDGE_ON_LEG_FAILURE and len(self.legs) < 2:
            raise ValueError("multi-leg order group requires at least two primary legs")
        if self.policy == OrderGroupPolicy.HEDGE_ON_LEG_FAILURE and not self.hedge_legs:
            raise ValueError("HEDGE_ON_LEG_FAILURE requires hedge_legs")
        if self.policy != OrderGroupPolicy.HEDGE_ON_LEG_FAILURE and self.hedge_legs:
            raise ValueError("hedge_legs are only valid for HEDGE_ON_LEG_FAILURE")
        all_legs = (*self.legs, *self.hedge_legs)
        order_ids = [item.order_id for item in all_legs]
        if len(order_ids) != len(set(order_ids)):
            raise ValueError("order group leg order_id values must be unique")
        for leg in all_legs:
            if leg.run_id != self.run_id or leg.strategy_id != self.strategy_id:
                raise ValueError("order group legs must share run_id and strategy_id")
        if self.policy == OrderGroupPolicy.ALL_LEGS_FOK and any(
            item.tif != TimeInForce.FOK for item in self.legs
        ):
            raise ValueError("ALL_LEGS_FOK requires FOK on every primary leg")


@dataclass(frozen=True)
class OrderGroupResult:
    group_id: str
    policy: OrderGroupPolicy
    status: str
    reason: str
    primary_order_ids: tuple[str, ...]
    hedge_order_ids: tuple[str, ...]
    executed_hedge_order_ids: tuple[str, ...]
    leg_statuses: Mapping[str, str]
    audit_hash: str = ""

    def __post_init__(self) -> None:
        if not self.audit_hash:
            payload = asdict(self)
            payload["audit_hash"] = ""
            object.__setattr__(self, "audit_hash", canonical_hash(payload))

    def as_dict(self) -> dict[str, Any]:
        return canonical_value(asdict(self))


@dataclass(frozen=True)
class ExecutionChildFill:
    fill_id: str
    order_id: str
    condition_id: str
    market_id: str
    asset_id: str
    outcome: Outcome
    raw_side: RawOrderSide
    canonical_side: EconomicAction
    qty: Decimal
    raw_price: Decimal
    canonical_yes_price: Decimal
    liquidity_role: str
    fill_exchange_ts: datetime
    fill_receive_ts: datetime
    book_epoch: int
    snapshot_id: str
    source_event_ids: tuple[str, ...]
    residual_before: Decimal | None
    residual_after: Decimal | None
    queue_ahead_before: Decimal | None = None
    queue_ahead_after: Decimal | None = None
    fee: Decimal = Decimal(0)
    rebate_accrual: Decimal = Decimal(0)
    evidence_kind: str = "L2_VISIBLE_DEPTH"

    def __post_init__(self) -> None:
        object.__setattr__(self, "qty", qty(self.qty))
        object.__setattr__(self, "raw_price", price(self.raw_price))
        object.__setattr__(self, "canonical_yes_price", price(self.canonical_yes_price))
        object.__setattr__(
            self,
            "fill_exchange_ts",
            utc(self.fill_exchange_ts, field_name="fill_exchange_ts"),
        )
        object.__setattr__(
            self,
            "fill_receive_ts",
            utc(self.fill_receive_ts, field_name="fill_receive_ts"),
        )
        if self.fill_receive_ts < self.fill_exchange_ts:
            raise ValueError("fill_receive_ts cannot precede fill_exchange_ts")


@dataclass(frozen=True)
class ExecutionMatch:
    run_id: str
    order_id: str
    fill_id: str
    execution_model: str
    profile: str
    event_group_id: str
    condition_id: str
    market_id: str
    token_id: str
    outcome: Outcome
    raw_side: RawOrderSide
    canonical_side: EconomicAction
    qty: Decimal
    raw_price: Decimal
    canonical_yes_price: Decimal
    notional: Decimal
    liquidity_role: str
    tif: TimeInForce
    match_type_hint: CtfMatchType
    signal_ts: datetime
    observed_ts: datetime
    submit_ts: datetime
    exchange_arrival_ts: datetime
    fill_exchange_ts: datetime
    fill_receive_ts: datetime
    book_epoch: int
    snapshot_id: str
    source_event_ids: tuple[str, ...]
    queue_ahead_before: Decimal | None
    queue_ahead_after: Decimal | None
    residual_before: Decimal | None
    residual_after: Decimal | None
    fee: Decimal
    rebate_accrual: Decimal
    evidence_kind: str
    fee_schedule_id: str = "ORDER_INTENT_FEE_FIELDS"
    fee_model_version: str = "pml2-fee-v1"
    fee_source: str = "ORDER_INTENT_POINT_IN_TIME"
    finality_state: MatchFinalityState = MatchFinalityState.MATCHED
    fill_block: int | None = None
    audit_hash: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "qty", qty(self.qty))
        object.__setattr__(self, "raw_price", price(self.raw_price))
        object.__setattr__(self, "canonical_yes_price", price(self.canonical_yes_price))
        object.__setattr__(self, "notional", qty(self.notional))
        for field_name in (
            "signal_ts",
            "observed_ts",
            "submit_ts",
            "exchange_arrival_ts",
            "fill_exchange_ts",
            "fill_receive_ts",
        ):
            object.__setattr__(
                self, field_name, utc(getattr(self, field_name), field_name=field_name)
            )
        if self.qty <= 0:
            raise ValueError("execution match quantity must be positive")
        if self.raw_side == RawOrderSide.BUY and self.raw_price < 0:
            raise ValueError("invalid BUY fill price")
        if not self.audit_hash:
            payload = asdict(self)
            payload["audit_hash"] = ""
            object.__setattr__(self, "audit_hash", canonical_hash(payload))

    def as_dict(self) -> dict[str, Any]:
        return canonical_value(asdict(self))


@dataclass(frozen=True)
class ModeledFillEstimate:
    """A probability estimate, never an observed execution or capacity debit."""

    estimate_id: str
    run_id: str
    order_id: str
    execution_model: str
    profile: str
    evidence_tier: FillEvidenceTier
    model_kind: str
    requested_size: Decimal
    p_any_fill: Decimal
    conditional_expected_size: Decimal
    expected_size: Decimal
    expected_price: Decimal | None
    horizon_seconds: int
    decision_ts: datetime
    domain_status: str
    model_version: str
    artifact_hash: str
    source_event_ids: tuple[str, ...] = ()
    random_seed: int | None = None
    sampled_fill_size: Decimal | None = None
    audit_hash: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "requested_size", qty(self.requested_size))
        object.__setattr__(
            self,
            "conditional_expected_size",
            qty(self.conditional_expected_size),
        )
        object.__setattr__(self, "expected_size", qty(self.expected_size))
        if self.expected_price is not None:
            object.__setattr__(self, "expected_price", price(self.expected_price))
        object.__setattr__(
            self,
            "decision_ts",
            utc(self.decision_ts, field_name="decision_ts"),
        )
        probability = Decimal(str(self.p_any_fill))
        if not Decimal(0) <= probability <= Decimal(1):
            raise ValueError("p_any_fill must be within [0, 1]")
        object.__setattr__(self, "p_any_fill", probability)
        if self.horizon_seconds <= 0:
            raise ValueError("horizon_seconds must be positive")
        if self.expected_size > self.requested_size:
            raise ValueError("expected_size cannot exceed requested_size")
        if self.conditional_expected_size > self.requested_size:
            raise ValueError("conditional_expected_size cannot exceed requested_size")
        if self.sampled_fill_size is not None:
            sampled = qty(self.sampled_fill_size)
            if sampled > self.requested_size:
                raise ValueError("sampled_fill_size cannot exceed requested_size")
            object.__setattr__(self, "sampled_fill_size", sampled)
        if self.evidence_tier == FillEvidenceTier.OBSERVED_L2:
            raise ValueError("ModeledFillEstimate cannot use OBSERVED_L2 evidence")
        if not self.audit_hash:
            payload = asdict(self)
            payload["audit_hash"] = ""
            object.__setattr__(self, "audit_hash", canonical_hash(payload))

    def as_dict(self) -> dict[str, Any]:
        return canonical_value(
            {
                "schema_version": "pml2_modeled_fill_estimate_v1",
                **asdict(self),
            }
        )


@dataclass(frozen=True)
class Pml2OrderResult:
    order: Pml2OrderIntent
    status: OrderStatus
    reason: str
    exchange_arrival_ts: datetime
    fills: tuple[ExecutionMatch, ...]
    remaining_size: Decimal
    remaining_amount: Decimal | None = None
    admission_diagnostics: tuple[str, ...] = ()
    queue_ahead: Decimal | None = None
    maker_survival_forecast: Mapping[str, Any] | None = None
    counterfactual_impact: Mapping[str, Any] | None = None
    audit_hash: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "exchange_arrival_ts",
            utc(self.exchange_arrival_ts, field_name="exchange_arrival_ts"),
        )
        object.__setattr__(self, "remaining_size", qty(self.remaining_size))
        filled_size = sum((item.qty for item in self.fills), Decimal(0))
        filled_notional = sum((item.notional for item in self.fills), Decimal(0))
        filled_amount = qty(
            filled_notional
            if self.order.amount_unit == OrderAmountUnit.QUOTE
            else filled_size
        )
        remaining_amount = self.remaining_amount
        if remaining_amount is None:
            remaining_amount = qty(
                max(
                    Decimal(0),
                    self.order.effective_requested_amount - filled_amount,
                )
            )
        else:
            remaining_amount = qty(remaining_amount)
        object.__setattr__(self, "remaining_amount", remaining_amount)
        object.__setattr__(
            self,
            "admission_diagnostics",
            tuple(dict.fromkeys(str(item) for item in self.admission_diagnostics)),
        )
        filled = sum((item.qty for item in self.fills), Decimal(0))
        if (
            self.order.amount_unit == OrderAmountUnit.SHARES
            and filled + self.remaining_size != self.order.effective_requested_amount
        ):
            raise ValueError("filled plus remaining must equal requested size")
        amount_total_error = abs(
            filled_amount + remaining_amount - self.order.effective_requested_amount
        )
        if amount_total_error > ORDER_AMOUNT_TOLERANCE:
            raise ValueError("filled plus remaining must equal requested amount")
        if not self.audit_hash:
            payload = {
                "order_id": self.order.order_id,
                "amount_unit": self.order.amount_unit,
                "requested_amount": self.order.effective_requested_amount,
                "venue_admission": self.order.venue_admission,
                "venue_admission_evidence_id": (
                    self.order.venue_admission_evidence_id
                ),
                "status": self.status,
                "reason": self.reason,
                "fills": [fill.audit_hash for fill in self.fills],
                "remaining_size": self.remaining_size,
                "remaining_amount": self.remaining_amount,
                "admission_diagnostics": self.admission_diagnostics,
                "maker_survival_forecast": self.maker_survival_forecast,
                "counterfactual_impact": self.counterfactual_impact,
            }
            object.__setattr__(self, "audit_hash", canonical_hash(payload))

    @property
    def filled_size(self) -> Decimal:
        return qty(sum((item.qty for item in self.fills), Decimal(0)))

    @property
    def filled_amount(self) -> Decimal:
        return qty(
            sum(
                (
                    item.notional
                    if self.order.amount_unit == OrderAmountUnit.QUOTE
                    else item.qty
                    for item in self.fills
                ),
                Decimal(0),
            )
        )

    @property
    def avg_fill_price(self) -> Decimal | None:
        if self.filled_size <= 0:
            return None
        return price(
            sum((item.raw_price * item.qty for item in self.fills), Decimal(0))
            / self.filled_size
        )

    def as_dict(self) -> dict[str, Any]:
        return canonical_value(
            {
                "schema_version": "pml2_order_result_v2",
                "order": asdict(self.order),
                "status": self.status,
                "reason": self.reason,
                "exchange_arrival_ts": self.exchange_arrival_ts,
                "fills": [item.as_dict() for item in self.fills],
                "filled_size": self.filled_size,
                "remaining_size": self.remaining_size,
                "requested_amount": self.order.effective_requested_amount,
                "filled_amount": self.filled_amount,
                "remaining_amount": self.remaining_amount,
                "amount_unit": self.order.amount_unit,
                "admission_diagnostics": self.admission_diagnostics,
                "avg_fill_price": self.avg_fill_price,
                "queue_ahead": self.queue_ahead,
                "maker_survival_forecast": self.maker_survival_forecast,
                "counterfactual_impact": self.counterfactual_impact,
                "audit_hash": self.audit_hash,
            }
        )


@dataclass(frozen=True)
class SettlementReceipt:
    receipt_id: str
    fill_id: str
    state: MatchFinalityState
    observed_at: datetime
    tx_hash: str | None = None
    block_number: int | None = None
    reason: str = ""
    source_event_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "observed_at", utc(self.observed_at, field_name="observed_at")
        )


@dataclass(frozen=True)
class CoverageManifest:
    run_id: str
    source_ids: tuple[str, ...]
    start_ts: datetime | None
    end_ts: datetime | None
    full_snapshot_ids: tuple[str, ...]
    gap_event_ids: tuple[str, ...]
    source_handover_verified: bool
    cold_restore_used: bool
    event_order_policy: str = "pml2_historical_events_win_ties_v1"
    event_count: int = 0
    snapshot_count: int = 0
    delta_count: int = 0
    trade_count: int = 0
    duplicate_event_count: int = 0
    mirror_mismatch_count: int = 0
    stale_order_count: int = 0
    truncated_snapshot_count: int = 0
    transport_coverage_window_count: int = 0
    transport_coverage_proof_ids: tuple[str, ...] = ()
    data_quality_status: str = "VALID"

    @property
    def manifest_hash(self) -> str:
        return canonical_hash(self)
