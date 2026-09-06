"""OrderFilled V2 trade-tape execution replay.

This is the small, strict runner for the V2 tables built from
``trade_prints_one_sided``.  It intentionally models taker participation only:
no L2 depth, no L3 queue, no OHLCV-touched fills.
"""

from __future__ import annotations

import json
import os
from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from heapq import merge
from pathlib import Path
from time import perf_counter
from typing import Any, Iterable, Literal, Mapping

from quant.backtest.fill_only_lob_validity import (
    FillOnlyLobValidityModel,
    default_lob_holdout_validity_rule,
    fill_only_validity_rule_from_mapping,
    load_lob_holdout_validity_rule,
)
from quant.backtest.orderfilled_probability import (
    OrderFilledProbabilityModel,
    default_orderfilled_probability_profile,
    load_orderfilled_probability_profile,
)
from quant.backtest.prepared_trade_tape import (
    PreparedTradeTape,
    TradeGroupIndex,
    prepare_trade_tape,
)
from quant.core.db import ClickHouseClient

Q = Decimal("0.0000000001")
OrderSide = Literal["BUY", "SELL"]
TradeSideEvidenceMode = Literal["same_side", "any_order_side"]
TimeInForce = Literal["GTC", "GTD", "IOC", "FOK", "FAK"]
ExecutionGrade = Literal[
    "observed_fill_replay",
    "trade_tape_participation",
    "formula_slippage_baseline",
    "unsupported",
]
MakerMode = Literal["strict_no_fill", "phantom_queue"]


@dataclass(frozen=True)
class EventKey:
    block_number: int
    tx_index: int
    tx_hash: str
    log_index: int
    trade_id: str


@dataclass(frozen=True, slots=True)
class V2TradePrint:
    trade_id: str
    market_id: int
    condition_id: str
    asset_id: str
    outcome: str
    block_number: int
    block_time: datetime
    tx_hash: str
    tx_index: int
    tx_index_source: str
    price: Decimal
    size: Decimal
    notional: Decimal
    aggressor_side: OrderSide
    passive_side: OrderSide
    source_log_indexes: tuple[int, ...]
    source_fill_count: int
    trade_group_id: str | None = None

    @property
    def sequence(self) -> tuple[int, int, str, int, str]:
        first_log = min(self.source_log_indexes) if self.source_log_indexes else 0
        return (
            self.block_number,
            self.tx_index,
            self.tx_hash,
            first_log,
            self.trade_id,
        )

    @property
    def event_key(self) -> EventKey:
        first_log = min(self.source_log_indexes) if self.source_log_indexes else 0
        return EventKey(
            self.block_number, self.tx_index, self.tx_hash, first_log, self.trade_id
        )


@dataclass(frozen=True)
class V2TakerOrder:
    order_id: str
    market_id: int
    asset_id: str
    side: OrderSide
    limit_price: Decimal
    size: Decimal
    signal_block: int | None = None
    signal_ts: datetime | None = None
    latency_blocks: int = 0
    latency: timedelta = timedelta(0)
    horizon_blocks: int | None = None
    horizon: timedelta | None = None
    trade_slice_lookback_blocks: int = 0
    trade_slice_lookback: timedelta = timedelta(0)
    participation_rate: Decimal = Decimal("0.025")
    price_buffer: Decimal = Decimal("0")
    tif: TimeInForce = "GTC"
    allow_partial_fill: bool = True
    signal_source_trade_id: str | None = None
    signal_source_tx_hash: str | None = None
    signal_source_log_indexes: tuple[int, ...] = field(default_factory=tuple)
    exclude_signal_source_trade: bool = False
    require_pre_arrival_quote_proxy: bool = False
    quote_proxy_ttl: timedelta | None = None
    min_trailing_same_side_trade_count: int = 0
    min_trailing_same_side_volume: Decimal = Decimal("0")
    trailing_volume_multiplier: Decimal = Decimal("0")
    trailing_participation_rate: Decimal | None = None
    max_fill_size_per_order: Decimal | None = None
    market_window_cap: Decimal | None = None
    market_window_blocks: int | None = None
    min_future_eligible_trade_count: int = 0
    min_future_eligible_volume: Decimal = Decimal("0")
    lob_validity_rule: dict[str, Any] | None = None
    orderfilled_probability_profile: dict[str, Any] | None = None
    execution_profile_name: str | None = None
    execution_profile_activation: str | None = None
    execution_stability_grade: str | None = None
    trade_side_evidence_mode: TradeSideEvidenceMode = "same_side"

    @property
    def arrival_block(self) -> int | None:
        if self.signal_block is None:
            return None
        return int(self.signal_block) + max(0, int(self.latency_blocks))

    @property
    def arrival_ts(self) -> datetime | None:
        if self.signal_ts is None:
            return None
        return _utc(self.signal_ts) + self.latency

    @property
    def deadline_block(self) -> int | None:
        if self.arrival_block is None:
            return None
        if _is_immediate_tif(self.tif):
            horizon = (
                1
                if self.horizon_blocks is None
                else min(max(0, int(self.horizon_blocks)), 1)
            )
            return self.arrival_block + horizon
        if self.horizon_blocks is None:
            return None
        return self.arrival_block + max(0, int(self.horizon_blocks))

    @property
    def deadline_ts(self) -> datetime | None:
        if self.arrival_ts is None:
            return None
        if _is_immediate_tif(self.tif):
            horizon = (
                timedelta(seconds=1)
                if self.horizon is None
                else min(max(self.horizon, timedelta(0)), timedelta(seconds=1))
            )
            return self.arrival_ts + horizon
        if self.horizon is None:
            return None
        return self.arrival_ts + self.horizon


@dataclass(frozen=True)
class V2MakerOrder:
    order_id: str
    market_id: int
    asset_id: str
    side: OrderSide
    limit_price: Decimal
    size: Decimal
    signal_block: int | None = None
    signal_ts: datetime | None = None
    latency_blocks: int = 0
    latency: timedelta = timedelta(0)
    horizon_blocks: int | None = None
    horizon: timedelta | None = None

    @property
    def arrival_block(self) -> int | None:
        if self.signal_block is None:
            return None
        return int(self.signal_block) + max(0, int(self.latency_blocks))

    @property
    def arrival_ts(self) -> datetime | None:
        if self.signal_ts is None:
            return None
        return _utc(self.signal_ts) + self.latency

    @property
    def deadline_block(self) -> int | None:
        if self.arrival_block is None or self.horizon_blocks is None:
            return None
        return self.arrival_block + max(0, int(self.horizon_blocks))

    @property
    def deadline_ts(self) -> datetime | None:
        if self.arrival_ts is None or self.horizon is None:
            return None
        return self.arrival_ts + self.horizon


@dataclass(frozen=True)
class V2ExecutionProfile:
    name: str
    participation_rate: Decimal
    latency: timedelta
    horizon: timedelta
    price_buffer: Decimal = Decimal("0")
    latency_blocks: int = 0
    horizon_blocks: int | None = None
    maker_mode: MakerMode = "strict_no_fill"
    maker_participation_rate: Decimal = Decimal("0")
    maker_phantom_queue: Decimal = Decimal("0")
    exclude_signal_source_trade: bool = False
    require_pre_arrival_quote_proxy: bool = False
    quote_proxy_ttl: timedelta | None = None
    min_trailing_same_side_trade_count: int = 0
    min_trailing_same_side_volume: Decimal = Decimal("0")
    trailing_volume_multiplier: Decimal = Decimal("0")
    trailing_participation_rate: Decimal | None = None
    max_fill_size_per_order: Decimal | None = None
    market_window_cap: Decimal | None = None
    market_window_cap_fraction: Decimal | None = None
    market_window_blocks: int | None = None
    min_future_eligible_trade_count: int = 0
    min_future_eligible_volume: Decimal = Decimal("0")
    lob_validity_rule: dict[str, Any] | None = None
    orderfilled_probability_profile: dict[str, Any] | None = None
    orderfilled_capacity_variant: str | None = None
    profile_activation: str = "builtin"
    execution_stability_grade: str | None = None
    trade_side_evidence_mode: TradeSideEvidenceMode = "same_side"


V2_EXECUTION_PROFILES: dict[str, V2ExecutionProfile] = {
    "strict_audit": V2ExecutionProfile(
        name="strict_audit",
        participation_rate=Decimal("0.005"),
        latency=timedelta(seconds=5),
        horizon=timedelta(seconds=30),
        price_buffer=Decimal("0.01"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        require_pre_arrival_quote_proxy=True,
        quote_proxy_ttl=timedelta(minutes=5),
        min_trailing_same_side_trade_count=1,
        trailing_volume_multiplier=Decimal("1"),
        trailing_participation_rate=Decimal("0.005"),
        min_future_eligible_trade_count=2,
    ),
    "conservative_trade_tape": V2ExecutionProfile(
        name="conservative_trade_tape",
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        price_buffer=Decimal("0.005"),
        maker_mode="phantom_queue",
        maker_participation_rate=Decimal("0.025"),
        maker_phantom_queue=Decimal("1000"),
        exclude_signal_source_trade=True,
        require_pre_arrival_quote_proxy=True,
        quote_proxy_ttl=timedelta(minutes=30),
        min_trailing_same_side_trade_count=1,
        trailing_volume_multiplier=Decimal("0.5"),
        trailing_participation_rate=Decimal("0.025"),
        min_future_eligible_trade_count=1,
    ),
    "lob_holdout_calibrated_fill_only": V2ExecutionProfile(
        name="lob_holdout_calibrated_fill_only",
        participation_rate=Decimal("0.01"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=30),
        price_buffer=Decimal("0.005"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        require_pre_arrival_quote_proxy=True,
        quote_proxy_ttl=timedelta(minutes=5),
        min_trailing_same_side_trade_count=1,
        trailing_volume_multiplier=Decimal("1"),
        trailing_participation_rate=Decimal("0.01"),
        min_future_eligible_trade_count=1,
        lob_validity_rule=default_lob_holdout_validity_rule().as_dict(),
    ),
    "probabilistic_trade_tape": V2ExecutionProfile(
        name="probabilistic_trade_tape",
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        horizon_blocks=2000,
        price_buffer=Decimal("0.005"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        min_future_eligible_trade_count=1,
        orderfilled_probability_profile=default_orderfilled_probability_profile().as_dict(),
        orderfilled_capacity_variant="expected",
        profile_activation="orderfilled_only_probability",
    ),
    "probabilistic_conservative": V2ExecutionProfile(
        name="probabilistic_conservative",
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        horizon_blocks=2000,
        price_buffer=Decimal("0.005"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        min_future_eligible_trade_count=1,
        orderfilled_probability_profile=default_orderfilled_probability_profile().as_dict(),
        orderfilled_capacity_variant="conservative",
        profile_activation="orderfilled_only_probability",
    ),
    "probabilistic_source_confirmed": V2ExecutionProfile(
        name="probabilistic_source_confirmed",
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        horizon_blocks=2000,
        price_buffer=Decimal("0.005"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        min_future_eligible_trade_count=1,
        orderfilled_probability_profile=default_orderfilled_probability_profile().as_dict(),
        orderfilled_capacity_variant="source_confirmed",
        profile_activation="orderfilled_only_probability_upper_bound",
    ),
    "probabilistic_taker_5s": V2ExecutionProfile(
        name="probabilistic_taker_5s",
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=5),
        price_buffer=Decimal("0.005"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        min_future_eligible_trade_count=1,
        orderfilled_probability_profile=default_orderfilled_probability_profile().as_dict(),
        orderfilled_capacity_variant="expected",
        profile_activation="orderfilled_only_short_arrival_same_side",
    ),
    "probabilistic_taker_30s": V2ExecutionProfile(
        name="probabilistic_taker_30s",
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=30),
        price_buffer=Decimal("0.005"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        min_future_eligible_trade_count=1,
        orderfilled_probability_profile=default_orderfilled_probability_profile().as_dict(),
        orderfilled_capacity_variant="expected",
        profile_activation="orderfilled_only_short_arrival_same_side",
    ),
    "probabilistic_taker_120s": V2ExecutionProfile(
        name="probabilistic_taker_120s",
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=120),
        price_buffer=Decimal("0.005"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        min_future_eligible_trade_count=1,
        orderfilled_probability_profile=default_orderfilled_probability_profile().as_dict(),
        orderfilled_capacity_variant="expected",
        profile_activation="orderfilled_only_short_arrival_same_side_sensitivity",
    ),
    "probabilistic_taker_30s_any_order_side": V2ExecutionProfile(
        name="probabilistic_taker_30s_any_order_side",
        participation_rate=Decimal("0.01"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=30),
        price_buffer=Decimal("0.005"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        min_future_eligible_trade_count=1,
        orderfilled_probability_profile=default_orderfilled_probability_profile().as_dict(),
        orderfilled_capacity_variant="expected",
        profile_activation="orderfilled_only_short_arrival_order_side_unknown_aggressor",
        trade_side_evidence_mode="any_order_side",
    ),
    "probabilistic_taker_120s_any_order_side": V2ExecutionProfile(
        name="probabilistic_taker_120s_any_order_side",
        participation_rate=Decimal("0.01"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=120),
        price_buffer=Decimal("0.005"),
        maker_mode="strict_no_fill",
        exclude_signal_source_trade=True,
        min_future_eligible_trade_count=1,
        orderfilled_probability_profile=default_orderfilled_probability_profile().as_dict(),
        orderfilled_capacity_variant="expected",
        profile_activation="orderfilled_only_short_arrival_order_side_unknown_aggressor_sensitivity",
        trade_side_evidence_mode="any_order_side",
    ),
    "optimistic_sensitivity": V2ExecutionProfile(
        name="optimistic_sensitivity",
        participation_rate=Decimal("0.05"),
        latency=timedelta(0),
        horizon=timedelta(minutes=15),
        price_buffer=Decimal("0"),
        maker_mode="phantom_queue",
        maker_participation_rate=Decimal("0.05"),
        maker_phantom_queue=Decimal("100"),
    ),
}


@dataclass(frozen=True)
class V2Fill:
    order_id: str
    fill_ts: datetime
    fill_block: int
    side: OrderSide
    limit_price: Decimal
    filled_size: Decimal
    exec_price: Decimal
    source_trade_id: str
    source_tx_hash: str
    source_log_indexes: tuple[int, ...]
    historical_price: Decimal
    historical_size: Decimal
    price_buffer_paid: Decimal
    participation_rate: Decimal
    allocated_capacity: Decimal
    tx_index_source: str

    def as_dict(self) -> dict[str, Any]:
        row = {item.name: getattr(self, item.name) for item in fields(self)}
        row["fill_ts"] = self.fill_ts.isoformat()
        row["source_log_indexes"] = list(self.source_log_indexes)
        return row


@dataclass(frozen=True)
class V2OrderResult:
    order_id: str
    status: str
    side: OrderSide
    requested_size: Decimal
    filled_size: Decimal
    unfilled_size: Decimal
    avg_price: Decimal
    limit_price: Decimal
    arrival_block: int | None
    arrival_ts: datetime | None
    eligible_historical_volume: Decimal
    simulated_volume: Decimal
    participation_rate: Decimal
    capacity_utilization: Decimal
    avg_fill_delay_seconds: Decimal
    avg_price_buffer: Decimal
    filled_notional: Decimal
    cash_delta: Decimal
    position_delta: Decimal
    reason_unfilled: str
    p_depth_valid: Decimal | None = None
    fill_only_eligibility: str | None = None
    fill_validity_reason: str = ""
    fill_validity_features: dict[str, Any] | None = None
    fill_validity_rule: dict[str, Any] | None = None
    p_fill: Decimal | None = None
    conditional_capacity_fraction: Decimal | None = None
    fill_capacity_variant: str | None = None
    fill_probability_eligibility: str | None = None
    fill_probability_reason: str = ""
    fill_probability_features: dict[str, Any] | None = None
    fill_probability_profile: dict[str, Any] | None = None
    execution_profile_name: str | None = None
    execution_profile_activation: str | None = None
    execution_stability_grade: str | None = None
    fills: tuple[V2Fill, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        row = {item.name: getattr(self, item.name) for item in fields(self)}
        row["arrival_ts"] = self.arrival_ts.isoformat() if self.arrival_ts else None
        row["fills"] = [fill.as_dict() for fill in self.fills]
        return row


@dataclass(frozen=True)
class V2MakerResult:
    order_id: str
    status: str
    side: OrderSide
    requested_size: Decimal
    filled_size: Decimal
    unfilled_size: Decimal
    limit_price: Decimal
    maker_mode: MakerMode
    initial_phantom_queue: Decimal
    remaining_phantom_queue: Decimal
    candidate_historical_volume: Decimal
    reason_unfilled: str
    fills: tuple[V2Fill, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["fills"] = [fill.as_dict() for fill in self.fills]
        return row


@dataclass(frozen=True)
class V2WalletFillTick:
    fill_id: str
    wallet: str
    wallet_role: Literal["maker", "taker"]
    market_id: int
    condition_id: str
    asset_id: str
    outcome: str
    block_number: int
    block_time: datetime
    tx_hash: str
    log_index: int
    order_hash: str
    price: Decimal
    size: Decimal
    fee: Decimal
    passive_side: OrderSide
    aggressor_side: OrderSide

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["block_time"] = self.block_time.isoformat()
        return row


class CapacityLedger:
    def __init__(self) -> None:
        self._consumed: dict[str, Decimal] = {}
        self._market_window_consumed: dict[tuple[int, str, str, int], Decimal] = {}
        self._delta_tracking = False
        self._delta_source_consumed: dict[str, Decimal] = {}
        self._delta_market_window_consumed: dict[
            tuple[int, str, str, int], Decimal
        ] = {}
        self._mutation_version = 0

    @property
    def mutation_version(self) -> int:
        """Monotonic source-capacity version for native session synchronization."""

        return self._mutation_version

    def remaining(self, trade: V2TradePrint, participation_rate: Decimal) -> Decimal:
        cap = (trade.size * _decimal(participation_rate)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        used = self._consumed.get(trade.trade_id, Decimal("0"))
        return max(Decimal("0"), cap - used).quantize(Q, rounding=ROUND_HALF_UP)

    def consumed_size(self, trade_id: str) -> Decimal:
        """Return source capacity already allocated without copying the ledger."""

        return self._consumed.get(str(trade_id), Decimal("0"))

    def consumed_items(self) -> tuple[tuple[str, Decimal], ...]:
        """Expose the sparse consumed set for native batch adapters."""

        return tuple(self._consumed.items())

    def remaining_for_order(
        self, order: V2TakerOrder, trade: V2TradePrint, participation_rate: Decimal
    ) -> Decimal:
        return min(
            self.remaining(trade, participation_rate),
            self.market_window_remaining(order, trade),
        ).quantize(Q, rounding=ROUND_HALF_UP)

    def market_window_remaining(
        self, order: V2TakerOrder, trade: V2TradePrint
    ) -> Decimal:
        cap = order.market_window_cap
        blocks = order.market_window_blocks
        if cap is None or blocks is None or int(blocks) <= 0:
            return Decimal("Infinity")
        key = self.market_window_key(order, trade)
        if key is None:
            return Decimal("Infinity")
        used = self._market_window_consumed.get(key, Decimal("0"))
        return max(Decimal("0"), _decimal(cap) - used).quantize(
            Q, rounding=ROUND_HALF_UP
        )

    def consume(self, trade_id: str, size: Decimal) -> None:
        qty = _decimal(size)
        self._consumed[trade_id] = (
            self._consumed.get(trade_id, Decimal("0")) + qty
        ).quantize(
            Q,
            rounding=ROUND_HALF_UP,
        )
        self._mutation_version += 1
        if self._delta_tracking:
            self._delta_source_consumed[trade_id] = (
                self._delta_source_consumed.get(trade_id, Decimal("0")) + qty
            ).quantize(Q, rounding=ROUND_HALF_UP)

    def consume_for_order(
        self, order: V2TakerOrder, trade: V2TradePrint, size: Decimal
    ) -> None:
        qty = _decimal(size)
        self.consume(trade.trade_id, qty)
        key = self.market_window_key(order, trade)
        if key is not None:
            self._market_window_consumed[key] = (
                self._market_window_consumed.get(key, Decimal("0")) + qty
            ).quantize(
                Q,
                rounding=ROUND_HALF_UP,
            )
            if self._delta_tracking:
                self._delta_market_window_consumed[key] = (
                    self._delta_market_window_consumed.get(key, Decimal("0")) + qty
                ).quantize(Q, rounding=ROUND_HALF_UP)

    def begin_delta_tracking(self) -> None:
        """Capture only subsequent mutations without scanning cumulative state."""

        if self._delta_tracking:
            raise RuntimeError("capacity ledger delta tracking is already active")
        self._delta_tracking = True
        self._delta_source_consumed.clear()
        self._delta_market_window_consumed.clear()

    def drain_delta(self) -> CapacityLedgerDelta:
        """Return captured mutations and stop tracking them."""

        if not self._delta_tracking:
            raise RuntimeError("capacity ledger delta tracking is not active")
        delta = CapacityLedgerDelta(
            source_trade_consumed=dict(self._delta_source_consumed),
            market_window_consumed=dict(self._delta_market_window_consumed),
        )
        self._delta_tracking = False
        self._delta_source_consumed.clear()
        self._delta_market_window_consumed.clear()
        return delta

    def apply_delta(self, payload: Mapping[str, Any]) -> None:
        if self._delta_tracking:
            raise RuntimeError("cannot apply capacity delta during delta tracking")
        changed = False
        for trade_id, quantity in dict(
            payload.get("source_trade_consumed") or {}
        ).items():
            self._consumed[str(trade_id)] = (
                self._consumed.get(str(trade_id), Decimal("0")) + _decimal(quantity)
            ).quantize(Q, rounding=ROUND_HALF_UP)
            changed = True
        for encoded, quantity in dict(
            payload.get("market_window_consumed") or {}
        ).items():
            market_id, asset_id, side, window = str(encoded).split(":", 3)
            key = (int(market_id), asset_id, _side(side), int(window))
            self._market_window_consumed[key] = (
                self._market_window_consumed.get(key, Decimal("0")) + _decimal(quantity)
            ).quantize(Q, rounding=ROUND_HALF_UP)
            changed = True
        if changed:
            self._mutation_version += 1

    def retain_source_trade_ids(self, trade_ids: Iterable[str]) -> int:
        """Drop consumed source keys that cannot occur in the next trade slice."""

        if self._delta_tracking:
            raise RuntimeError(
                "cannot prune capacity ledger while delta tracking is active"
            )
        previous = self._consumed
        retained: dict[str, Decimal] = {}
        for trade_id in trade_ids:
            if trade_id in previous:
                retained[trade_id] = previous[trade_id]
        self._consumed = retained
        removed = len(previous) - len(retained)
        if removed:
            self._mutation_version += 1
        return removed

    def market_window_key(
        self, order: V2TakerOrder, trade: V2TradePrint
    ) -> tuple[int, str, str, int] | None:
        blocks = order.market_window_blocks
        if order.market_window_cap is None or blocks is None or int(blocks) <= 0:
            return None
        window = int(trade.block_number) // int(blocks)
        return (
            int(order.market_id),
            str(order.asset_id).lower(),
            _side(order.side),
            window,
        )

    def as_dict(self) -> dict[str, str]:
        return {key: str(value) for key, value in sorted(self._consumed.items())}

    def market_window_as_dict(self) -> dict[str, str]:
        return {
            f"{market_id}:{asset_id}:{side}:{window}": str(value)
            for (market_id, asset_id, side, window), value in sorted(
                self._market_window_consumed.items()
            )
        }

    def snapshot(self) -> dict[str, Any]:
        if self._delta_tracking:
            raise RuntimeError("cannot snapshot capacity ledger during delta tracking")
        return {
            "schema_version": "V2CapacityLedgerSnapshotV1",
            "source_consumed": {
                key: str(value) for key, value in sorted(self._consumed.items())
            },
            "market_window_consumed": [
                {
                    "market_id": market_id,
                    "asset_id": asset_id,
                    "side": side,
                    "window": window,
                    "quantity": str(value),
                }
                for (market_id, asset_id, side, window), value in sorted(
                    self._market_window_consumed.items()
                )
            ],
        }

    @classmethod
    def from_snapshot(cls, payload: Mapping[str, Any]) -> "CapacityLedger":
        if payload.get("schema_version") != "V2CapacityLedgerSnapshotV1":
            raise ValueError("unsupported V2 capacity ledger snapshot")
        ledger = cls()
        ledger._consumed = {
            str(key): _decimal(value)
            for key, value in dict(payload.get("source_consumed") or {}).items()
        }
        ledger._market_window_consumed = {
            (
                int(row["market_id"]),
                str(row["asset_id"]),
                _side(row["side"]),
                int(row["window"]),
            ): _decimal(row["quantity"])
            for row in payload.get("market_window_consumed") or []
        }
        return ledger


@dataclass(frozen=True)
class CapacityLedgerDelta:
    source_trade_consumed: dict[str, Decimal]
    market_window_consumed: dict[tuple[int, str, str, int], Decimal]

    def as_dict(self) -> dict[str, dict[str, str]]:
        return {
            "source_trade_consumed": {
                key: str(value)
                for key, value in sorted(self.source_trade_consumed.items())
            },
            "market_window_consumed": {
                f"{market_id}:{asset_id}:{side}:{window}": str(value)
                for (market_id, asset_id, side, window), value in sorted(
                    self.market_window_consumed.items()
                )
            },
        }


PreparedV2TradeTape = PreparedTradeTape


@dataclass(frozen=True)
class RequiredTradeWindow:
    market_id: int
    asset_id: str
    aggressor_side: OrderSide | None
    start_block: int | None
    end_block: int | None
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    source_min_block: int | None = None
    source_max_block: int | None = None

    @property
    def group_key(self) -> tuple[int, str, str]:
        return (
            int(self.market_id),
            self.asset_id.lower(),
            _side(self.aggressor_side) if self.aggressor_side else "*",
        )

    @property
    def axis(self) -> Literal["block", "time"]:
        has_blocks = self.start_block is not None and self.end_block is not None
        has_times = self.start_ts is not None and self.end_ts is not None
        if has_blocks == has_times:
            raise ValueError(
                "required trade window must use exactly one complete block or time axis"
            )
        return "block" if has_blocks else "time"


def _required_block_bounds(window: RequiredTradeWindow) -> tuple[int, int]:
    if window.start_block is None or window.end_block is None:
        raise ValueError("block trade window is missing a bound")
    return int(window.start_block), int(window.end_block)


def _required_time_bounds(window: RequiredTradeWindow) -> tuple[datetime, datetime]:
    if window.start_ts is None or window.end_ts is None:
        raise ValueError("time trade window is missing a bound")
    return _utc(window.start_ts), _utc(window.end_ts)


@dataclass(frozen=True)
class TradeSliceLoadResult:
    trades: tuple[V2TradePrint, ...]
    windows_count: int
    merged_windows_count: int
    db_query_count: int
    rows_loaded: int
    load_sec: Decimal

    def as_dict(self) -> dict[str, Any]:
        return {
            "windows_count": self.windows_count,
            "merged_windows_count": self.merged_windows_count,
            "db_query_count": self.db_query_count,
            "rows_loaded": self.rows_loaded,
            "load_sec": self.load_sec,
        }


class TradeSliceLimitExceeded(ValueError):
    def __init__(self, window: RequiredTradeWindow, limit: int) -> None:
        self.window = window
        self.limit = int(limit)
        bounds = (
            f"from_block={window.start_block} to_block={window.end_block}"
            if window.axis == "block"
            else (
                f"from_ts={window.start_ts.isoformat()} "
                f"to_ts={window.end_ts.isoformat()}"
                if window.start_ts is not None and window.end_ts is not None
                else "invalid_time_bounds"
            )
        )
        super().__init__(
            "trade slice exceeds row limit "
            f"market_id={window.market_id} asset_id={window.asset_id} "
            f"{bounds} limit={self.limit}"
        )


@dataclass(frozen=True)
class V2ReplayDiagnostics:
    orders_count: int
    trade_rows_indexed: int
    trade_groups: int
    index_build_sec: Decimal
    matching_sec: Decimal
    candidate_rows_scanned: int
    naive_rows_scanned: int
    scan_reduction_ratio: Decimal
    candidate_rows_per_order_p50: Decimal
    candidate_rows_per_order_p95: Decimal
    candidate_rows_per_order_p99: Decimal
    candidate_rows_per_order_max: int
    index_reused: bool = False
    matching_backend: str = "python"
    backend_fallback_reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FillEvidenceGateResult:
    reason: str
    trailing_trade_count: int
    trailing_volume: Decimal
    future_eligible_trade_count: int
    future_eligible_volume: Decimal
    order_remaining_cap: Decimal


def replay_v2_taker_orders(
    orders: Iterable[V2TakerOrder],
    trades: Iterable[V2TradePrint],
    *,
    ledger: CapacityLedger | None = None,
) -> tuple[list[V2OrderResult], CapacityLedger]:
    results, capacity, _ = replay_v2_taker_orders_with_diagnostics(
        orders, trades, ledger=ledger
    )
    return results, capacity


def replay_v2_taker_orders_reference(
    orders: Iterable[V2TakerOrder],
    trades: Iterable[V2TradePrint],
    *,
    ledger: CapacityLedger | None = None,
) -> tuple[list[V2OrderResult], CapacityLedger]:
    capacity = ledger or CapacityLedger()
    ordered_trades = sorted(trades, key=lambda item: item.sequence)
    results = [
        replay_v2_taker_order(order, ordered_trades, capacity, trades_are_ordered=True)
        for order in orders
    ]
    return results, capacity


def replay_v2_taker_orders_indexed(
    orders: Iterable[V2TakerOrder],
    trades: Iterable[V2TradePrint],
    *,
    ledger: CapacityLedger | None = None,
) -> tuple[list[V2OrderResult], CapacityLedger]:
    results, capacity, _ = replay_v2_taker_orders_with_diagnostics(
        orders, trades, ledger=ledger
    )
    return results, capacity


def replay_v2_taker_orders_with_diagnostics(
    orders: Iterable[V2TakerOrder],
    trades: Iterable[V2TradePrint] | None = None,
    *,
    ledger: CapacityLedger | None = None,
    prepared_tape: PreparedV2TradeTape | None = None,
    backend: Literal["auto", "python", "rust"] = "auto",
) -> tuple[list[V2OrderResult], CapacityLedger, V2ReplayDiagnostics]:
    capacity = ledger or CapacityLedger()
    order_rows = list(orders)
    if prepared_tape is not None:
        if trades is not None:
            raise ValueError("pass either trades or prepared_tape, not both")
        prepared = prepared_tape
        index = prepared.index
        combined_groups = prepared.combined_groups
        trade_rows_count = prepared.trade_rows_indexed
        index_build_sec = Decimal("0")
        index_reused = True
    else:
        if trades is None:
            raise ValueError("trades or prepared_tape is required")
        prepared = prepare_v2_trade_tape(trades)
        index = prepared.index
        combined_groups = prepared.combined_groups
        index_build_sec = prepared.index_build_sec
        trade_rows_count = prepared.trade_rows_indexed
        index_reused = False
    candidate_counts: list[int] = []
    t1 = perf_counter()
    requested_backend = str(backend).lower()
    if requested_backend not in {"auto", "python", "rust"}:
        raise ValueError(f"unsupported V2 matcher backend: {backend!r}")
    matching_backend = "python"
    backend_fallback_reason = ""
    use_rust = False
    if requested_backend != "python":
        minimum_work = max(0, int(os.getenv("FILL_ONLY_RUST_MIN_WORK", "10000")))
        enough_work = len(order_rows) * trade_rows_count >= minimum_work
        if requested_backend == "auto" and not enough_work:
            backend_fallback_reason = f"batch_work_below_threshold:{minimum_work}"
        else:
            from quant.backtest.rust_kernel import v2_rust_compatibility

            compatibility = v2_rust_compatibility(order_rows, prepared)
        if requested_backend != "auto" or enough_work:
            if compatibility.supported:
                use_rust = True
            elif requested_backend == "rust":
                raise ValueError(
                    f"V2 Rust matcher is incompatible: {compatibility.reason}"
                )
            else:
                backend_fallback_reason = compatibility.reason
    if use_rust:
        from quant.backtest.rust_kernel import replay_v2_taker_orders_rust

        rust_output = replay_v2_taker_orders_rust(order_rows, prepared, capacity)
        results = rust_output.results
        candidate_counts = rust_output.candidate_counts
        matching_backend = "rust"
    else:
        results = [
            replay_v2_taker_order_indexed(
                order,
                index,
                capacity,
                candidate_counts=candidate_counts,
                combined_groups=combined_groups,
            )
            for order in order_rows
        ]
    matching_sec = Decimal(str(perf_counter() - t1)).quantize(Q, rounding=ROUND_HALF_UP)
    candidate_rows_scanned = sum(candidate_counts)
    naive_rows_scanned = len(order_rows) * trade_rows_count
    diagnostics = V2ReplayDiagnostics(
        orders_count=len(order_rows),
        trade_rows_indexed=trade_rows_count,
        trade_groups=len(index),
        index_build_sec=index_build_sec,
        matching_sec=matching_sec,
        candidate_rows_scanned=candidate_rows_scanned,
        naive_rows_scanned=naive_rows_scanned,
        scan_reduction_ratio=(
            Decimal(candidate_rows_scanned) / Decimal(naive_rows_scanned)
        ).quantize(Q, rounding=ROUND_HALF_UP)
        if naive_rows_scanned
        else Decimal("0"),
        candidate_rows_per_order_p50=_int_percentile(candidate_counts, Decimal("0.50")),
        candidate_rows_per_order_p95=_int_percentile(candidate_counts, Decimal("0.95")),
        candidate_rows_per_order_p99=_int_percentile(candidate_counts, Decimal("0.99")),
        candidate_rows_per_order_max=max(candidate_counts) if candidate_counts else 0,
        index_reused=index_reused,
        matching_backend=matching_backend,
        backend_fallback_reason=backend_fallback_reason,
    )
    return results, capacity, diagnostics


def prepare_v2_trade_tape(trades: Iterable[V2TradePrint]) -> PreparedV2TradeTape:
    """Materialize and index one immutable trade partition for repeated replay."""

    return prepare_trade_tape(trades)


def build_v2_trade_group_index(
    trades: Iterable[V2TradePrint],
) -> dict[tuple[int, str, OrderSide], TradeGroupIndex]:
    grouped: dict[tuple[int, str, OrderSide], list[V2TradePrint]] = {}
    for trade in trades:
        key = (
            int(trade.market_id),
            trade.asset_id.lower(),
            _side(trade.aggressor_side),
        )
        grouped.setdefault(key, []).append(trade)

    indexes: dict[tuple[int, str, OrderSide], TradeGroupIndex] = {}
    for key, rows in grouped.items():
        ordered = tuple(sorted(rows, key=lambda item: item.sequence))
        indexes[key] = TradeGroupIndex(
            key=key,
            trades=ordered,
            block_numbers=tuple(row.block_number for row in ordered),
            block_times=tuple(row.block_time for row in ordered),
        )
    return indexes


def build_v2_combined_trade_group_index(
    index: Mapping[tuple[int, str, OrderSide], TradeGroupIndex],
) -> dict[tuple[int, str], TradeGroupIndex]:
    """Merge each market/token's already sorted side streams exactly once."""

    pairs = {(market_id, asset_id) for market_id, asset_id, _ in index}
    combined: dict[tuple[int, str], TradeGroupIndex] = {}
    for market_id, asset_id in pairs:
        sides: tuple[OrderSide, ...] = ("BUY", "SELL")
        streams = [
            group.trades
            for side in sides
            if (group := index.get((market_id, asset_id, side))) is not None
        ]
        ordered = tuple(merge(*streams, key=lambda item: item.sequence))
        combined[(market_id, asset_id)] = TradeGroupIndex(
            key=(market_id, asset_id, "BUY"),
            trades=ordered,
            block_numbers=tuple(row.block_number for row in ordered),
            block_times=tuple(row.block_time for row in ordered),
        )
    return combined


def build_required_trade_windows(
    orders: Iterable[V2TakerOrder],
    *,
    source_min_block: int | None = None,
    source_max_block: int | None = None,
) -> list[RequiredTradeWindow]:
    windows: list[RequiredTradeWindow] = []
    for order in orders:
        probability_profile = _orderfilled_probability_profile(order)
        aggressor_side = (
            None
            if probability_profile is not None
            or order.trade_side_evidence_mode == "any_order_side"
            else _side(order.side)
        )
        if order.arrival_block is not None and order.deadline_block is not None:
            probability_lookback = (
                int(probability_profile.lookback_blocks)
                if probability_profile is not None
                else 0
            )
            lookback_blocks = max(
                0, probability_lookback, int(order.trade_slice_lookback_blocks)
            )
            windows.append(
                RequiredTradeWindow(
                    market_id=int(order.market_id),
                    asset_id=str(order.asset_id).lower(),
                    aggressor_side=aggressor_side,
                    start_block=max(
                        0, int(order.arrival_block) - max(0, lookback_blocks)
                    ),
                    end_block=int(order.deadline_block),
                )
            )
            continue
        if order.arrival_ts is None or order.deadline_ts is None:
            raise ValueError(
                f"order {order.order_id} needs either block or timestamp arrival/deadline bounds for trade-slice loading"
            )
        if source_min_block is None or source_max_block is None:
            raise ValueError(
                f"timestamp-only order {order.order_id} needs pinned source block bounds"
            )
        probability_lookback_seconds = (
            float(probability_profile.lookback_seconds)
            if probability_profile is not None
            else 0.0
        )
        lookback_seconds = max(
            0.0,
            probability_lookback_seconds,
            float(order.trade_slice_lookback.total_seconds()),
        )
        windows.append(
            RequiredTradeWindow(
                market_id=int(order.market_id),
                asset_id=str(order.asset_id).lower(),
                aggressor_side=aggressor_side,
                start_block=None,
                end_block=None,
                start_ts=_utc(order.arrival_ts) - timedelta(seconds=lookback_seconds),
                end_ts=_utc(order.deadline_ts),
                source_min_block=max(0, int(source_min_block)),
                source_max_block=max(0, int(source_max_block)),
            )
        )
    return windows


def merge_required_trade_windows(
    windows: Iterable[RequiredTradeWindow],
    *,
    merge_gap_blocks: int = 0,
    merge_gap_seconds: int = 0,
) -> list[RequiredTradeWindow]:
    by_key: dict[
        tuple[int, str, str, str, int | None, int | None], list[RequiredTradeWindow]
    ] = {}
    for window in windows:
        if window.axis == "block":
            if window.start_block is None or window.end_block is None:
                raise ValueError("block trade window is missing a bound")
            start_block = min(int(window.start_block), int(window.end_block))
            end_block = max(int(window.start_block), int(window.end_block))
            normalized = RequiredTradeWindow(
                market_id=int(window.market_id),
                asset_id=window.asset_id.lower(),
                aggressor_side=_side(window.aggressor_side)
                if window.aggressor_side
                else None,
                start_block=start_block,
                end_block=end_block,
            )
        else:
            if window.start_ts is None or window.end_ts is None:
                raise ValueError("time trade window is missing a bound")
            start_ts = min(_utc(window.start_ts), _utc(window.end_ts))
            end_ts = max(_utc(window.start_ts), _utc(window.end_ts))
            normalized = RequiredTradeWindow(
                market_id=int(window.market_id),
                asset_id=window.asset_id.lower(),
                aggressor_side=_side(window.aggressor_side)
                if window.aggressor_side
                else None,
                start_block=None,
                end_block=None,
                start_ts=start_ts,
                end_ts=end_ts,
                source_min_block=window.source_min_block,
                source_max_block=window.source_max_block,
            )
        key = (
            *normalized.group_key,
            normalized.axis,
            normalized.source_min_block,
            normalized.source_max_block,
        )
        by_key.setdefault(key, []).append(normalized)

    merged: list[RequiredTradeWindow] = []
    block_gap = max(0, int(merge_gap_blocks))
    time_gap = timedelta(seconds=max(0, int(merge_gap_seconds)))
    for rows in by_key.values():
        if not rows:
            continue
        axis = rows[0].axis
        if axis == "block" and any(
            row.start_block is None or row.end_block is None for row in rows
        ):
            raise ValueError("block trade window is missing a bound")
        if axis == "time" and any(
            row.start_ts is None or row.end_ts is None for row in rows
        ):
            raise ValueError("time trade window is missing a bound")
        ordered = sorted(
            rows,
            key=(_required_block_bounds if axis == "block" else _required_time_bounds),
        )
        if not ordered:
            continue
        cur = ordered[0]
        for nxt in ordered[1:]:
            if axis == "block":
                nxt_start, _ = _required_block_bounds(nxt)
                _, cur_end = _required_block_bounds(cur)
                overlaps = nxt_start <= cur_end + block_gap
            else:
                nxt_start_ts, _ = _required_time_bounds(nxt)
                _, cur_end_ts = _required_time_bounds(cur)
                overlaps = nxt_start_ts <= cur_end_ts + time_gap
            if overlaps:
                cur_start_block, cur_end_block = (
                    _required_block_bounds(cur) if axis == "block" else (None, None)
                )
                _, nxt_end_block = (
                    _required_block_bounds(nxt) if axis == "block" else (None, None)
                )
                merged_start_ts, merged_end_ts = (
                    _required_time_bounds(cur) if axis == "time" else (None, None)
                )
                _, merged_next_end_ts = (
                    _required_time_bounds(nxt) if axis == "time" else (None, None)
                )
                cur = RequiredTradeWindow(
                    market_id=cur.market_id,
                    asset_id=cur.asset_id,
                    aggressor_side=cur.aggressor_side,
                    start_block=cur_start_block,
                    end_block=(
                        max(cur_end_block, nxt_end_block)
                        if cur_end_block is not None and nxt_end_block is not None
                        else None
                    ),
                    start_ts=merged_start_ts,
                    end_ts=(
                        max(merged_end_ts, merged_next_end_ts)
                        if merged_end_ts is not None and merged_next_end_ts is not None
                        else None
                    ),
                    source_min_block=cur.source_min_block,
                    source_max_block=cur.source_max_block,
                )
            else:
                merged.append(cur)
                cur = nxt
        merged.append(cur)
    return sorted(
        merged,
        key=lambda item: (
            item.market_id,
            item.asset_id,
            str(item.aggressor_side or ""),
            item.axis,
            int(item.start_block) if item.start_block is not None else 0,
            _utc(item.start_ts).isoformat() if item.start_ts is not None else "",
        ),
    )


def load_v2_trade_slices_for_orders(
    orders: Iterable[V2TakerOrder],
    *,
    client: ClickHouseClient | None = None,
    merge_gap_blocks: int = 0,
    merge_gap_seconds: int = 0,
    limit_per_window: int | None = None,
    source_min_block: int | None = None,
    source_max_block: int | None = None,
) -> TradeSliceLoadResult:
    windows = build_required_trade_windows(
        orders,
        source_min_block=source_min_block,
        source_max_block=source_max_block,
    )
    return load_v2_trade_slices_for_windows(
        windows,
        client=client,
        merge_gap_blocks=merge_gap_blocks,
        merge_gap_seconds=merge_gap_seconds,
        limit_per_window=limit_per_window,
    )


def load_v2_trade_slices_for_windows(
    windows: Iterable[RequiredTradeWindow],
    *,
    client: ClickHouseClient | None = None,
    merge_gap_blocks: int = 0,
    merge_gap_seconds: int = 0,
    limit_per_window: int | None = None,
    reject_truncated_windows: bool = False,
) -> TradeSliceLoadResult:
    window_rows = list(windows)
    merged = merge_required_trade_windows(
        window_rows,
        merge_gap_blocks=merge_gap_blocks,
        merge_gap_seconds=merge_gap_seconds,
    )
    ch = client or ClickHouseClient()
    t0 = perf_counter()
    trades_by_id: dict[str, V2TradePrint] = {}
    for window in merged:
        query_limit = limit_per_window
        if reject_truncated_windows and limit_per_window is not None:
            query_limit = max(1, int(limit_per_window)) + 1
        rows = _load_v2_trade_window(window, ch, limit_per_window=query_limit)
        if (
            reject_truncated_windows
            and limit_per_window is not None
            and len(rows) > int(limit_per_window)
        ):
            raise TradeSliceLimitExceeded(window, int(limit_per_window))
        for trade in rows:
            trades_by_id.setdefault(trade.trade_id, trade)
    elapsed = Decimal(str(perf_counter() - t0)).quantize(Q, rounding=ROUND_HALF_UP)
    trades = tuple(sorted(trades_by_id.values(), key=lambda item: item.sequence))
    return TradeSliceLoadResult(
        trades=trades,
        windows_count=len(window_rows),
        merged_windows_count=len(merged),
        db_query_count=len(merged),
        rows_loaded=len(trades),
        load_sec=elapsed,
    )


def with_v2_execution_profile(
    order: V2TakerOrder, profile: str | V2ExecutionProfile
) -> V2TakerOrder:
    resolved = get_v2_execution_profile(profile)
    if isinstance(profile, V2ExecutionProfile):
        # A profile object is already resolved for this run. Reuse its immutable
        # calibration payload across orders instead of re-reading it per order.
        lob_rule = resolved.lob_validity_rule
        probability_profile = resolved.orderfilled_probability_profile
    else:
        lob_rule = _profile_lob_validity_rule(resolved)
        probability_profile = _profile_orderfilled_probability(resolved)
    return replace(
        order,
        participation_rate=resolved.participation_rate,
        latency=resolved.latency,
        latency_blocks=resolved.latency_blocks,
        horizon=resolved.horizon,
        horizon_blocks=resolved.horizon_blocks,
        price_buffer=resolved.price_buffer,
        exclude_signal_source_trade=resolved.exclude_signal_source_trade,
        require_pre_arrival_quote_proxy=resolved.require_pre_arrival_quote_proxy,
        quote_proxy_ttl=resolved.quote_proxy_ttl,
        min_trailing_same_side_trade_count=resolved.min_trailing_same_side_trade_count,
        min_trailing_same_side_volume=resolved.min_trailing_same_side_volume,
        trailing_volume_multiplier=resolved.trailing_volume_multiplier,
        trailing_participation_rate=resolved.trailing_participation_rate,
        max_fill_size_per_order=resolved.max_fill_size_per_order,
        market_window_cap=_profile_market_window_cap(order, resolved),
        market_window_blocks=resolved.market_window_blocks,
        min_future_eligible_trade_count=resolved.min_future_eligible_trade_count,
        min_future_eligible_volume=resolved.min_future_eligible_volume,
        lob_validity_rule=lob_rule,
        orderfilled_probability_profile=probability_profile,
        execution_profile_name=resolved.name,
        execution_profile_activation=resolved.profile_activation,
        execution_stability_grade=resolved.execution_stability_grade,
        trade_side_evidence_mode=resolved.trade_side_evidence_mode,
    )


def get_v2_execution_profile(profile: str | V2ExecutionProfile) -> V2ExecutionProfile:
    if isinstance(profile, V2ExecutionProfile):
        return profile
    profile = _profile_alias(profile)
    try:
        resolved = V2_EXECUTION_PROFILES[profile]
    except KeyError as exc:
        raise ValueError(f"unknown V2 execution profile: {profile!r}") from exc
    if profile == "lob_holdout_calibrated_fill_only":
        return _configured_lob_holdout_execution_profile(resolved)
    if profile in {
        "probabilistic_trade_tape",
        "probabilistic_conservative",
        "probabilistic_source_confirmed",
        "probabilistic_taker_5s",
        "probabilistic_taker_30s",
        "probabilistic_taker_120s",
        "probabilistic_taker_30s_any_order_side",
        "probabilistic_taker_120s_any_order_side",
    }:
        return _configured_orderfilled_probability_execution_profile(resolved)
    return resolved


def _profile_orderfilled_probability(
    profile: V2ExecutionProfile,
) -> dict[str, Any] | None:
    if not profile.orderfilled_probability_profile:
        return None
    path = os.getenv("POLYDATA_QUANT_ORDERFILLED_PROBABILITY_PROFILE", "").strip()
    if not path:
        default_path = (
            Path(__file__).resolve().parents[2]
            / "config"
            / "execution"
            / "orderfilled_probability_profile.v1.json"
        )
        if default_path.exists():
            path = str(default_path)
    if path:
        payload = load_orderfilled_probability_profile(path).as_dict()
    else:
        payload = dict(
            profile.orderfilled_probability_profile
            or default_orderfilled_probability_profile().as_dict()
        )
    if profile.orderfilled_capacity_variant:
        payload["capacity_variant"] = profile.orderfilled_capacity_variant
    return payload


def _configured_orderfilled_probability_execution_profile(
    base: V2ExecutionProfile,
) -> V2ExecutionProfile:
    probability = load_orderfilled_probability_profile(
        _orderfilled_probability_profile_path()
    )
    payload = probability.as_dict()
    if base.orderfilled_capacity_variant:
        payload["capacity_variant"] = base.orderfilled_capacity_variant
    return replace(
        base,
        orderfilled_probability_profile=payload,
        profile_activation=probability.activation,
    )


def _orderfilled_probability_profile_path() -> str | None:
    path = os.getenv("POLYDATA_QUANT_ORDERFILLED_PROBABILITY_PROFILE", "").strip()
    if path:
        return path
    default_path = (
        Path(__file__).resolve().parents[2]
        / "config"
        / "execution"
        / "orderfilled_probability_profile.v1.json"
    )
    return str(default_path) if default_path.exists() else None


def _profile_lob_validity_rule(profile: V2ExecutionProfile) -> dict[str, Any] | None:
    if (
        profile.name != "lob_holdout_calibrated_fill_only"
        and not profile.lob_validity_rule
    ):
        return None
    path = os.getenv("POLYDATA_QUANT_FILL_ONLY_LOB_VALIDITY_PROFILE", "").strip()
    if not path:
        default_path = (
            Path(__file__).resolve().parents[2]
            / "config"
            / "execution"
            / "fill_only_lob_validity_profile.v1.json"
        )
        if default_path.exists():
            path = str(default_path)
    if path:
        return load_lob_holdout_validity_rule(path).as_dict()
    return dict(
        profile.lob_validity_rule or default_lob_holdout_validity_rule().as_dict()
    )


def _configured_lob_holdout_execution_profile(
    base: V2ExecutionProfile,
) -> V2ExecutionProfile:
    payload = _load_lob_holdout_profile_payload()
    if not payload:
        return base
    params = (
        payload.get("params") if isinstance(payload.get("params"), dict) else payload
    )
    if not isinstance(params, dict):
        return base
    ttl = _profile_timedelta(
        params,
        seconds_key="quote_proxy_ttl_seconds",
        ms_key="quote_proxy_ttl_ms",
        default=base.quote_proxy_ttl,
    )
    horizon = _profile_horizon(params, default=base.horizon)
    return replace(
        base,
        participation_rate=_profile_decimal(
            params, "participation_rate", base.participation_rate
        ),
        horizon=horizon,
        price_buffer=_profile_decimal(
            params,
            "price_buffer_abs",
            _profile_decimal(params, "price_buffer", base.price_buffer),
        ),
        require_pre_arrival_quote_proxy=_profile_bool(
            params,
            "require_pre_arrival_quote_proxy",
            base.require_pre_arrival_quote_proxy,
        ),
        quote_proxy_ttl=ttl,
        min_trailing_same_side_trade_count=_profile_int(
            params,
            "min_trailing_same_side_count",
            base.min_trailing_same_side_trade_count,
        ),
        min_trailing_same_side_volume=_profile_decimal(
            params, "min_trailing_same_side_volume", base.min_trailing_same_side_volume
        ),
        trailing_participation_rate=_profile_optional_decimal(
            params, "trailing_volume_cap_fraction", base.trailing_participation_rate
        ),
        market_window_cap=_profile_optional_decimal(
            params, "market_window_cap", base.market_window_cap
        ),
        market_window_cap_fraction=_profile_optional_decimal(
            params, "market_window_cap_fraction", base.market_window_cap_fraction
        ),
        market_window_blocks=_profile_optional_int(
            params, "market_window_blocks", base.market_window_blocks
        ),
        min_future_eligible_trade_count=_profile_int(
            params,
            "min_future_eligible_trade_count",
            base.min_future_eligible_trade_count,
        ),
        min_future_eligible_volume=_profile_decimal(
            params, "min_future_eligible_volume", base.min_future_eligible_volume
        ),
        lob_validity_rule=fill_only_validity_rule_from_mapping(params).as_dict(),
        profile_activation=_profile_activation(payload),
        execution_stability_grade=_profile_stability_grade(payload),
    )


def _load_lob_holdout_profile_payload() -> dict[str, Any] | None:
    path = os.getenv("POLYDATA_QUANT_FILL_ONLY_LOB_VALIDITY_PROFILE", "").strip()
    if not path:
        default_path = (
            Path(__file__).resolve().parents[2]
            / "config"
            / "execution"
            / "fill_only_lob_validity_profile.v1.json"
        )
        if default_path.exists():
            path = str(default_path)
    if not path:
        return None
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _profile_stability_grade(payload: dict[str, Any]) -> str | None:
    stability = payload.get("execution_stability")
    if not isinstance(stability, dict):
        report = payload.get("calibration_report")
        summary = report.get("summary") if isinstance(report, dict) else None
        stability = (
            summary.get("execution_stability") if isinstance(summary, dict) else None
        )
    if not isinstance(stability, dict):
        return None
    grade = str(stability.get("grade") or "").strip().upper()
    return grade or None


def _profile_activation(payload: dict[str, Any]) -> str:
    grade = _profile_stability_grade(payload)
    if grade == "READY":
        return "primary_ready"
    if grade:
        return "review_only_low_confidence"
    metrics = payload.get("holdout_metrics")
    samples = (
        int((metrics or {}).get("samples") or 0) if isinstance(metrics, dict) else 0
    )
    return "review_only_missing_stability" if samples < 1000 else "review_only_unscored"


def _profile_market_window_cap(
    order: V2TakerOrder, profile: V2ExecutionProfile
) -> Decimal | None:
    if profile.market_window_cap_fraction is not None:
        return (
            _decimal(order.size)
            * max(Decimal("0"), _decimal(profile.market_window_cap_fraction))
        ).quantize(Q, rounding=ROUND_HALF_UP)
    return profile.market_window_cap


def _profile_decimal(params: dict[str, Any], key: str, default: Decimal) -> Decimal:
    value = params.get(key)
    if value is None or value == "":
        return default
    return _decimal(value)


def _profile_optional_decimal(
    params: dict[str, Any], key: str, default: Decimal | None
) -> Decimal | None:
    value = params.get(key)
    if value is None or value == "":
        return default
    return _decimal(value)


def _profile_int(params: dict[str, Any], key: str, default: int) -> int:
    value = params.get(key)
    if value is None or value == "":
        return default
    return int(value)


def _profile_optional_int(
    params: dict[str, Any], key: str, default: int | None
) -> int | None:
    value = params.get(key)
    if value is None or value == "":
        return default
    return int(value)


def _profile_bool(params: dict[str, Any], key: str, default: bool) -> bool:
    value = params.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _profile_timedelta(
    params: dict[str, Any], *, seconds_key: str, ms_key: str, default: timedelta | None
) -> timedelta | None:
    if params.get(ms_key) is not None:
        return timedelta(milliseconds=float(_decimal(params[ms_key])))
    if params.get(seconds_key) is not None:
        return timedelta(seconds=float(_decimal(params[seconds_key])))
    return default


def _profile_horizon(params: dict[str, Any], *, default: timedelta) -> timedelta:
    value = params.get("execution_horizon_seconds")
    if value is not None:
        return timedelta(seconds=float(_decimal(value)))
    by_tif = params.get("execution_horizon_by_tif")
    if isinstance(by_tif, dict):
        raw = str(by_tif.get("GTC") or "").lower()
        if raw.endswith("s") and raw[:-1].strip().isdigit():
            return timedelta(seconds=int(raw[:-1].strip()))
    return default


def _profile_alias(profile: Any) -> str:
    text = str(profile or "")
    aliases = {
        "primary_calibrated": "lob_holdout_calibrated_fill_only",
        "primary_conservative": "lob_holdout_calibrated_fill_only",
        "orderfilled_probability": "probabilistic_trade_tape",
        "probabilistic": "probabilistic_trade_tape",
        "probability": "probabilistic_trade_tape",
        "probabilistic_expected": "probabilistic_trade_tape",
        "probability_expected": "probabilistic_trade_tape",
        "probability_conservative": "probabilistic_conservative",
        "orderfilled_probability_conservative": "probabilistic_conservative",
        "probability_source_confirmed": "probabilistic_source_confirmed",
        "orderfilled_probability_source_confirmed": "probabilistic_source_confirmed",
        "optimistic_upper_bound": "optimistic_sensitivity",
    }
    return aliases.get(text, text)


def replay_v2_taker_order(
    order: V2TakerOrder,
    trades: Iterable[V2TradePrint],
    ledger: CapacityLedger | None = None,
    *,
    trades_are_ordered: bool = False,
) -> V2OrderResult:
    capacity = ledger or CapacityLedger()
    side = _side(order.side)
    remaining = max(Decimal("0"), _decimal(order.size)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    limit = _decimal(order.limit_price).quantize(Q, rounding=ROUND_HALF_UP)
    participation_rate = max(
        Decimal("0"), min(Decimal("1"), _decimal(order.participation_rate))
    )
    price_buffer = max(Decimal("0"), _decimal(order.price_buffer))
    fills: list[V2Fill] = []
    eligible_volume = Decimal("0")
    saw_same_side_limit_trade = False
    saw_buffer_blocked_trade = False
    saw_capacity_exhausted_trade = False
    pending_consumes: list[tuple[V2TradePrint, Decimal]] = []
    pending_by_trade: dict[str, Decimal] = {}
    pending_by_window: dict[tuple[int, str, str, int], Decimal] = {}

    if remaining <= 0:
        return _result(
            order, "REJECTED", remaining, fills, Decimal("0"), "invalid_order_size"
        )
    if participation_rate <= 0:
        return _result(
            order, "NO_FILL", remaining, fills, Decimal("0"), "zero_participation_rate"
        )

    ordered_trades = tuple(
        trades if trades_are_ordered else sorted(trades, key=lambda item: item.sequence)
    )
    validity = _lob_validity_decision(order, ordered_trades, side, limit, price_buffer)
    if validity is not None and not validity.accepted:
        return _result(
            order,
            "NO_FILL",
            remaining,
            fills,
            Decimal("0"),
            validity.reason,
            validity=validity,
        )
    probability = _orderfilled_probability_decision(order, ordered_trades)
    if probability is not None and not probability.accepted:
        return _result(
            order,
            "NO_FILL",
            remaining,
            fills,
            Decimal("0"),
            probability.reason,
            validity=validity,
            probability=probability,
        )
    gate = _fill_evidence_gate(order, ordered_trades, side, limit, price_buffer)
    if gate.reason:
        return _result(
            order,
            "NO_FILL",
            remaining,
            fills,
            Decimal("0"),
            gate.reason,
            validity=validity,
            probability=probability,
        )
    order_remaining_cap = _apply_lob_validity_capacity(
        gate.order_remaining_cap, validity
    )
    order_remaining_cap = _apply_orderfilled_probability_capacity(
        order_remaining_cap, probability
    )
    for trade in ordered_trades:
        if remaining <= 0 or order_remaining_cap <= 0:
            break
        if (
            not _same_market_asset(order, trade)
            or not _after_arrival(order, trade)
            or not _before_deadline(order, trade)
        ):
            continue
        if _is_excluded_source_trade(order, trade):
            continue
        if not _trade_side_is_eligible(order, trade, side):
            continue
        if not _limit_allows(side, trade.price, limit):
            continue

        saw_same_side_limit_trade = True
        eligible_volume += trade.size
        exec_price = _exec_price(side, trade.price, price_buffer)
        if not _limit_allows(side, exec_price, limit):
            saw_buffer_blocked_trade = True
            continue
        trade_capacity = _capacity_remaining_after_pending(
            capacity,
            order,
            trade,
            participation_rate,
            pending_by_trade,
            pending_by_window,
        )
        if trade_capacity <= 0:
            saw_capacity_exhausted_trade = True
            continue
        qty = min(remaining, trade_capacity, order_remaining_cap).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if qty <= 0:
            continue
        pending_consumes.append((trade, qty))
        pending_by_trade[trade.trade_id] = (
            pending_by_trade.get(trade.trade_id, Decimal("0")) + qty
        ).quantize(Q, rounding=ROUND_HALF_UP)
        window_key = capacity.market_window_key(order, trade)
        if window_key is not None:
            pending_by_window[window_key] = (
                pending_by_window.get(window_key, Decimal("0")) + qty
            ).quantize(Q, rounding=ROUND_HALF_UP)
        fills.append(
            V2Fill(
                order_id=order.order_id,
                fill_ts=trade.block_time,
                fill_block=trade.block_number,
                side=side,
                limit_price=limit,
                filled_size=qty,
                exec_price=exec_price,
                source_trade_id=trade.trade_id,
                source_tx_hash=trade.tx_hash,
                source_log_indexes=trade.source_log_indexes,
                historical_price=trade.price,
                historical_size=trade.size,
                price_buffer_paid=abs(exec_price - trade.price).quantize(
                    Q, rounding=ROUND_HALF_UP
                ),
                participation_rate=participation_rate,
                allocated_capacity=trade_capacity,
                tx_index_source=trade.tx_index_source,
            )
        )
        remaining = (remaining - qty).quantize(Q, rounding=ROUND_HALF_UP)
        order_remaining_cap = (order_remaining_cap - qty).quantize(
            Q, rounding=ROUND_HALF_UP
        )

    reason = _unfilled_reason_for_order(
        order,
        remaining,
        saw_same_side_limit_trade,
        saw_buffer_blocked_trade,
        saw_capacity_exhausted_trade,
    )
    status = "FILLED" if remaining <= 0 else "PARTIAL_FILLED" if fills else "NO_FILL"
    if _requires_full_fill(order) and remaining > 0:
        return _result(
            order,
            "NO_FILL",
            max(Decimal("0"), _decimal(order.size)).quantize(Q, rounding=ROUND_HALF_UP),
            [],
            eligible_volume,
            _full_fill_reject_reason(order, reason),
            validity=validity,
            probability=probability,
        )
    for trade, qty in pending_consumes:
        capacity.consume_for_order(order, trade, qty)
    if _is_cancel_remainder_tif(order.tif) and remaining > 0 and fills:
        reason = "unfilled_remainder_cancelled_by_tif"
    return _result(
        order,
        status,
        remaining,
        fills,
        eligible_volume,
        reason,
        validity=validity,
        probability=probability,
    )


def replay_v2_taker_order_indexed(
    order: V2TakerOrder,
    index: Mapping[tuple[int, str, str], TradeGroupIndex[Any]],
    ledger: CapacityLedger | None = None,
    *,
    candidate_counts: list[int] | None = None,
    combined_groups: Mapping[tuple[int, str], TradeGroupIndex] | None = None,
) -> V2OrderResult:
    capacity = ledger or CapacityLedger()
    candidate_scanned = 0
    side = _side(order.side)
    remaining = max(Decimal("0"), _decimal(order.size)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    limit = _decimal(order.limit_price).quantize(Q, rounding=ROUND_HALF_UP)
    participation_rate = max(
        Decimal("0"), min(Decimal("1"), _decimal(order.participation_rate))
    )
    price_buffer = max(Decimal("0"), _decimal(order.price_buffer))
    fills: list[V2Fill] = []
    eligible_volume = Decimal("0")
    saw_same_side_limit_trade = False
    saw_buffer_blocked_trade = False
    saw_capacity_exhausted_trade = False
    pending_consumes: list[tuple[V2TradePrint, Decimal]] = []
    pending_by_trade: dict[str, Decimal] = {}
    pending_by_window: dict[tuple[int, str, str, int], Decimal] = {}

    if remaining <= 0:
        if candidate_counts is not None:
            candidate_counts.append(candidate_scanned)
        return _result(
            order, "REJECTED", remaining, fills, Decimal("0"), "invalid_order_size"
        )
    if participation_rate <= 0:
        if candidate_counts is not None:
            candidate_counts.append(candidate_scanned)
        return _result(
            order, "NO_FILL", remaining, fills, Decimal("0"), "zero_participation_rate"
        )

    group = _execution_trade_group(
        index,
        order,
        side,
        combined_groups=combined_groups,
    )
    probability_context = _orderfilled_probability_context(
        index,
        order,
        combined_groups=combined_groups,
    )
    probability = _orderfilled_probability_decision(
        order, probability_context, presorted_relevant=True
    )
    if probability is not None and not probability.accepted:
        if candidate_counts is not None:
            candidate_counts.append(candidate_scanned)
        return _result(
            order,
            "NO_FILL",
            remaining,
            fills,
            Decimal("0"),
            probability.reason,
            probability=probability,
        )
    if group is None:
        if candidate_counts is not None:
            candidate_counts.append(candidate_scanned)
        validity = _lob_validity_decision(
            order, probability_context, side, limit, price_buffer
        )
        reason = (
            validity.reason
            if validity is not None and not validity.accepted
            else _no_trade_reason(order)
        )
        return _result(
            order,
            "NO_FILL",
            remaining,
            fills,
            Decimal("0"),
            reason,
            validity=validity,
            probability=probability,
        )

    validity_context = (
        probability_context
        if probability_context
        else _order_relevant_trade_slice(group, order)
    )
    validity = _lob_validity_decision(
        order, validity_context, side, limit, price_buffer
    )
    if validity is not None and not validity.accepted:
        if candidate_counts is not None:
            candidate_counts.append(candidate_scanned)
        return _result(
            order,
            "NO_FILL",
            remaining,
            fills,
            Decimal("0"),
            validity.reason,
            validity=validity,
            probability=probability,
        )
    gate = _fill_evidence_gate(order, validity_context, side, limit, price_buffer)
    if gate.reason:
        if candidate_counts is not None:
            candidate_counts.append(candidate_scanned)
        return _result(
            order,
            "NO_FILL",
            remaining,
            fills,
            Decimal("0"),
            gate.reason,
            validity=validity,
            probability=probability,
        )
    order_remaining_cap = _apply_lob_validity_capacity(
        gate.order_remaining_cap, validity
    )
    order_remaining_cap = _apply_orderfilled_probability_capacity(
        order_remaining_cap, probability
    )

    if order.arrival_block is not None:
        start = bisect_left(group.block_numbers, order.arrival_block)
    elif order.arrival_ts is not None:
        start = bisect_left(group.block_times, _utc(order.arrival_ts))
    else:
        start = 0
    if order.deadline_block is not None:
        end = bisect_right(group.block_numbers, order.deadline_block)
    elif order.deadline_ts is not None:
        end = bisect_right(group.block_times, _utc(order.deadline_ts))
    else:
        end = len(group.trades)
    for trade in group.trades[start:end]:
        if remaining <= 0 or order_remaining_cap <= 0:
            break
        candidate_scanned += 1
        if not _after_arrival(order, trade) or not _before_deadline(order, trade):
            continue
        if _is_excluded_source_trade(order, trade):
            continue
        if not _limit_allows(side, trade.price, limit):
            continue

        saw_same_side_limit_trade = True
        eligible_volume += trade.size
        exec_price = _exec_price(side, trade.price, price_buffer)
        if not _limit_allows(side, exec_price, limit):
            saw_buffer_blocked_trade = True
            continue
        trade_capacity = _capacity_remaining_after_pending(
            capacity,
            order,
            trade,
            participation_rate,
            pending_by_trade,
            pending_by_window,
        )
        if trade_capacity <= 0:
            saw_capacity_exhausted_trade = True
            continue
        qty = min(remaining, trade_capacity, order_remaining_cap).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if qty <= 0:
            continue
        pending_consumes.append((trade, qty))
        pending_by_trade[trade.trade_id] = (
            pending_by_trade.get(trade.trade_id, Decimal("0")) + qty
        ).quantize(Q, rounding=ROUND_HALF_UP)
        window_key = capacity.market_window_key(order, trade)
        if window_key is not None:
            pending_by_window[window_key] = (
                pending_by_window.get(window_key, Decimal("0")) + qty
            ).quantize(Q, rounding=ROUND_HALF_UP)
        fills.append(
            V2Fill(
                order_id=order.order_id,
                fill_ts=trade.block_time,
                fill_block=trade.block_number,
                side=side,
                limit_price=limit,
                filled_size=qty,
                exec_price=exec_price,
                source_trade_id=trade.trade_id,
                source_tx_hash=trade.tx_hash,
                source_log_indexes=trade.source_log_indexes,
                historical_price=trade.price,
                historical_size=trade.size,
                price_buffer_paid=abs(exec_price - trade.price).quantize(
                    Q, rounding=ROUND_HALF_UP
                ),
                participation_rate=participation_rate,
                allocated_capacity=trade_capacity,
                tx_index_source=trade.tx_index_source,
            )
        )
        remaining = (remaining - qty).quantize(Q, rounding=ROUND_HALF_UP)
        order_remaining_cap = (order_remaining_cap - qty).quantize(
            Q, rounding=ROUND_HALF_UP
        )

    reason = _unfilled_reason_for_order(
        order,
        remaining,
        saw_same_side_limit_trade,
        saw_buffer_blocked_trade,
        saw_capacity_exhausted_trade,
    )
    status = "FILLED" if remaining <= 0 else "PARTIAL_FILLED" if fills else "NO_FILL"
    if _requires_full_fill(order) and remaining > 0:
        if candidate_counts is not None:
            candidate_counts.append(candidate_scanned)
        return _result(
            order,
            "NO_FILL",
            max(Decimal("0"), _decimal(order.size)).quantize(Q, rounding=ROUND_HALF_UP),
            [],
            eligible_volume,
            _full_fill_reject_reason(order, reason),
            validity=validity,
            probability=probability,
        )
    for trade, qty in pending_consumes:
        capacity.consume_for_order(order, trade, qty)
    if _is_cancel_remainder_tif(order.tif) and remaining > 0 and fills:
        reason = "unfilled_remainder_cancelled_by_tif"
    if candidate_counts is not None:
        candidate_counts.append(candidate_scanned)
    return _result(
        order,
        status,
        remaining,
        fills,
        eligible_volume,
        reason,
        validity=validity,
        probability=probability,
    )


def replay_v2_maker_order(
    order: V2MakerOrder,
    trades: Iterable[V2TradePrint],
    profile: str | V2ExecutionProfile = "strict_audit",
    *,
    ledger: CapacityLedger | None = None,
) -> V2MakerResult:
    resolved = get_v2_execution_profile(profile)
    side = _side(order.side)
    requested = max(Decimal("0"), _decimal(order.size)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    limit = _decimal(order.limit_price).quantize(Q, rounding=ROUND_HALF_UP)
    if requested <= 0:
        return _maker_result(
            order,
            resolved,
            [],
            Decimal("0"),
            Decimal("0"),
            "REJECTED",
            "invalid_order_size",
        )
    if resolved.maker_mode == "strict_no_fill":
        return _maker_result(
            order,
            resolved,
            [],
            Decimal("0"),
            Decimal("0"),
            "WORKING_BUT_NON_EXECUTABLE_IN_ORDERFILLED_ONLY",
            "maker_strict_no_fill",
        )

    capacity = ledger or CapacityLedger()
    remaining = requested
    phantom_queue = max(Decimal("0"), _decimal(resolved.maker_phantom_queue)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    initial_queue = phantom_queue
    participation_rate = max(
        Decimal("0"), min(Decimal("1"), _decimal(resolved.maker_participation_rate))
    )
    fills: list[V2Fill] = []
    candidate_volume = Decimal("0")

    for trade in sorted(trades, key=lambda item: item.sequence):
        if remaining <= 0:
            break
        if (
            not _same_maker_market_asset(order, trade)
            or not _after_maker_arrival(order, trade)
            or not _before_maker_deadline(order, trade)
        ):
            continue
        if not _maker_trade_can_hit(side, trade, limit):
            continue
        candidate_volume += trade.size
        flow = capacity.remaining(trade, participation_rate)
        if flow <= 0:
            continue
        if phantom_queue > 0:
            consumed = min(phantom_queue, flow).quantize(Q, rounding=ROUND_HALF_UP)
            phantom_queue = (phantom_queue - consumed).quantize(
                Q, rounding=ROUND_HALF_UP
            )
            flow = (flow - consumed).quantize(Q, rounding=ROUND_HALF_UP)
        qty = min(remaining, flow).quantize(Q, rounding=ROUND_HALF_UP)
        if qty <= 0:
            continue
        capacity.consume(trade.trade_id, qty)
        fills.append(
            V2Fill(
                order_id=order.order_id,
                fill_ts=trade.block_time,
                fill_block=trade.block_number,
                side=side,
                limit_price=limit,
                filled_size=qty,
                exec_price=limit,
                source_trade_id=trade.trade_id,
                source_tx_hash=trade.tx_hash,
                source_log_indexes=trade.source_log_indexes,
                historical_price=trade.price,
                historical_size=trade.size,
                price_buffer_paid=abs(limit - trade.price).quantize(
                    Q, rounding=ROUND_HALF_UP
                ),
                participation_rate=participation_rate,
                allocated_capacity=flow,
                tx_index_source=trade.tx_index_source,
            )
        )
        remaining = (remaining - qty).quantize(Q, rounding=ROUND_HALF_UP)

    status = "FILLED" if remaining <= 0 else "PARTIAL_FILLED" if fills else "NO_FILL"
    reason = (
        ""
        if status == "FILLED"
        else "phantom_queue_not_cleared"
        if phantom_queue > 0
        else "insufficient_post_queue_trade_capacity"
    )
    return _maker_result(
        order,
        resolved,
        fills,
        candidate_volume,
        phantom_queue,
        status,
        reason,
        initial_queue=initial_queue,
    )


def build_v2_replay_report(
    orders: Iterable[V2TakerOrder],
    trades: Iterable[V2TradePrint],
    *,
    profiles: Iterable[str] = (
        "strict_audit",
        "conservative_trade_tape",
        "probabilistic_conservative",
        "probabilistic_trade_tape",
        "probabilistic_source_confirmed",
        "optimistic_sensitivity",
    ),
    capacity_rates: Iterable[Decimal] = (
        Decimal("0.005"),
        Decimal("0.01"),
        Decimal("0.025"),
        Decimal("0.05"),
        Decimal("0.10"),
    ),
    latency_values: Iterable[timedelta] = (
        timedelta(0),
        timedelta(seconds=1),
        timedelta(seconds=5),
    ),
    horizon_values: Iterable[timedelta] = (
        timedelta(seconds=5),
        timedelta(seconds=30),
        timedelta(minutes=5),
    ),
    settlement_price: Decimal | None = None,
    fee_bps: Decimal = Decimal("0"),
) -> dict[str, Any]:
    order_rows = list(orders)
    trade_rows = list(trades)
    mode_comparison: dict[str, dict[str, Any]] = {}
    for profile_name in profiles:
        profiled_orders = [
            with_v2_execution_profile(order, profile_name) for order in order_rows
        ]
        profile_results, _ = replay_v2_taker_orders(profiled_orders, trade_rows)
        mode_comparison[profile_name] = summarize_v2_results(profile_results)

    base_orders = [
        with_v2_execution_profile(order, "probabilistic_trade_tape")
        for order in order_rows
    ]
    return {
        "model": "orderfilled_v2_trade_tape_taker_participation",
        "execution_grade": "trade_tape_participation",
        "not_l2_depth_or_l3_queue": True,
        "mode_comparison": mode_comparison,
        "capacity_curve": _curve(
            base_orders, trade_rows, "participation_rate", capacity_rates
        ),
        "latency_curve": _curve(base_orders, trade_rows, "latency", latency_values),
        "horizon_curve": _curve(base_orders, trade_rows, "horizon", horizon_values),
        "pnl_assumption": _pnl_assumption(
            mode_comparison.get("probabilistic_trade_tape", {}),
            settlement_price,
            fee_bps,
        ),
        "unfilled_reason_distribution": _reason_distribution(
            replay_v2_taker_orders(base_orders, trade_rows)[0],
        ),
    }


def build_v2_robustness_report(
    orders: Iterable[V2TakerOrder],
    trades: Iterable[V2TradePrint],
    *,
    parameter_grid: Iterable[dict[str, Any]] | None = None,
    category_by_market: dict[int, str] | None = None,
    walk_forward_splits: int = 3,
) -> dict[str, Any]:
    order_rows = list(orders)
    trade_rows = sorted(trades, key=lambda item: item.sequence)
    grid = list(parameter_grid or _default_parameter_grid())
    parameter_rows: list[dict[str, Any]] = []
    for index, params in enumerate(grid):
        adjusted = [_apply_parameter_row(order, params) for order in order_rows]
        results, _ = replay_v2_taker_orders(adjusted, trade_rows)
        parameter_rows.append(
            {
                "index": index,
                "parameters": _json_ready(params),
                "summary": summarize_v2_results(results),
            }
        )

    return {
        "status": "ready",
        "parameter_grid": parameter_rows,
        "walk_forward": _walk_forward_rows(order_rows, trade_rows, walk_forward_splits),
        "category_split": _category_split_rows(
            order_rows, trade_rows, category_by_market or {}
        ),
        "regime_split": _regime_split_rows(order_rows, trade_rows),
        "overfit_warning": _overfit_warning(parameter_rows),
    }


def classify_v2_execution_grade(
    *,
    observed_fill: bool = False,
    trade_tape: bool = True,
    formula_only: bool = False,
    needs_l2_or_queue: bool = False,
) -> ExecutionGrade:
    if observed_fill:
        return "observed_fill_replay"
    if needs_l2_or_queue:
        return "unsupported"
    if formula_only:
        return "formula_slippage_baseline"
    if trade_tape:
        return "trade_tape_participation"
    return "unsupported"


def build_v2_observed_fill_order(
    trade: V2TradePrint,
    *,
    order_id: str | None = None,
    size_fraction: Decimal = Decimal("0.025"),
    allowed_buffer: Decimal = Decimal("0"),
    lead_time: timedelta = timedelta(seconds=1),
) -> V2TakerOrder:
    side = _side(trade.aggressor_side)
    buffer = max(Decimal("0"), _decimal(allowed_buffer))
    limit = _exec_price(side, trade.price, buffer)
    return V2TakerOrder(
        order_id=order_id or f"observed-{trade.trade_id}",
        market_id=trade.market_id,
        asset_id=trade.asset_id,
        side=side,
        limit_price=limit,
        size=(trade.size * max(Decimal("0"), _decimal(size_fraction))).quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        signal_block=max(0, trade.block_number - 1),
        signal_ts=trade.block_time - lead_time,
        latency=timedelta(0),
        horizon=lead_time + timedelta(seconds=1),
        horizon_blocks=2,
        participation_rate=max(Decimal("0"), _decimal(size_fraction)),
        price_buffer=buffer,
    )


def build_v2_observed_fill_replay_report(
    trades: Iterable[V2TradePrint],
    *,
    size_fraction: Decimal = Decimal("0.025"),
    allowed_buffer: Decimal = Decimal("0"),
) -> dict[str, Any]:
    trade_rows = list(trades)
    orders = [
        build_v2_observed_fill_order(
            trade, size_fraction=size_fraction, allowed_buffer=allowed_buffer
        )
        for trade in trade_rows
    ]
    results, ledger = replay_v2_taker_orders(orders, trade_rows)
    return {
        "execution_grade": "observed_fill_replay",
        "orders": [order.as_dict() for order in results],
        "summary": summarize_v2_results(results),
        "capacity_ledger": ledger.as_dict(),
    }


def calibrate_v2_live_fills(
    predicted: Iterable[V2OrderResult], actual_rows: Iterable[dict[str, Any]]
) -> dict[str, Any]:
    predicted_by_id = {row.order_id: row for row in predicted}
    actual_by_id = {str(row.get("order_id")): row for row in actual_rows}
    order_ids = sorted(set(predicted_by_id) | set(actual_by_id))
    comparisons: list[dict[str, Any]] = []
    false_positive = 0
    false_negative = 0
    size_error_abs = Decimal("0")
    price_error_abs = Decimal("0")
    price_error_count = 0
    for order_id in order_ids:
        pred = predicted_by_id.get(order_id)
        actual = actual_by_id.get(order_id, {})
        predicted_size = pred.filled_size if pred else Decimal("0")
        actual_size = _decimal(actual.get("filled_size", "0")).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        predicted_price = pred.avg_price if pred else Decimal("0")
        actual_price = _decimal(actual.get("avg_price", "0")).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if predicted_size > 0 and actual_size <= 0:
            false_positive += 1
        if predicted_size <= 0 and actual_size > 0:
            false_negative += 1
        size_error = abs(predicted_size - actual_size).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        size_error_abs += size_error
        price_error = Decimal("0")
        if predicted_size > 0 and actual_size > 0:
            price_error = abs(predicted_price - actual_price).quantize(
                Q, rounding=ROUND_HALF_UP
            )
            price_error_abs += price_error
            price_error_count += 1
        comparisons.append(
            {
                "order_id": order_id,
                "predicted_size": predicted_size,
                "actual_size": actual_size,
                "predicted_price": predicted_price,
                "actual_price": actual_price,
                "size_error_abs": size_error,
                "price_error_abs": price_error,
            }
        )
    count = len(order_ids)
    return {
        "status": "ready" if count else "empty",
        "compared_orders": count,
        "false_positive_fills": false_positive,
        "false_negative_fills": false_negative,
        "avg_abs_size_error": (size_error_abs / Decimal(count)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if count
        else Decimal("0"),
        "avg_abs_price_error": (price_error_abs / Decimal(price_error_count)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if price_error_count
        else Decimal("0"),
        "comparisons": comparisons,
    }


def load_v2_calibration_actual_rows(
    conn: Any,
    *,
    run_id: int | None = None,
    source: str | None = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if run_id is not None:
        filters.append("run_id = %s")
        params.append(int(run_id))
    if source:
        filters.append("source = %s")
        params.append(source)
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(max(1, int(limit)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                COALESCE(simulated_order_id, live_order_id, sample_id) AS order_id,
                live_fill_size AS filled_size,
                live_fill_price AS avg_price,
                live_status,
                live_latency_seconds,
                sample_id,
                source
            FROM quant.quant_backtest_calibration_orders
            {where_sql}
            ORDER BY observed_at DESC NULLS LAST, calibration_id DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def build_v2_formula_slippage_baseline(
    order: V2TakerOrder,
    *,
    reference_price: Decimal,
    available_volume: Decimal,
    spread: Decimal = Decimal("0"),
    slippage: Decimal = Decimal("0"),
    volume_limit: Decimal = Decimal("0.025"),
) -> dict[str, Any]:
    side = _side(order.side)
    limit = _decimal(order.limit_price).quantize(Q, rounding=ROUND_HALF_UP)
    half_spread = max(Decimal("0"), _decimal(spread)) / Decimal("2")
    buffer = half_spread + max(Decimal("0"), _decimal(slippage))
    exec_price = _exec_price(side, _decimal(reference_price), buffer)
    capacity = (
        max(Decimal("0"), _decimal(available_volume))
        * max(Decimal("0"), _decimal(volume_limit))
    ).quantize(Q, rounding=ROUND_HALF_UP)
    requested = max(Decimal("0"), _decimal(order.size)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    qty = (
        Decimal("0")
        if not _limit_allows(side, exec_price, limit)
        else min(requested, capacity).quantize(Q, rounding=ROUND_HALF_UP)
    )
    status = (
        "FILLED"
        if qty >= requested and requested > 0
        else "PARTIAL_FILLED"
        if qty > 0
        else "NO_FILL"
    )
    reason = (
        ""
        if status == "FILLED"
        else "formula_price_exceeds_limit"
        if qty <= 0 and not _limit_allows(side, exec_price, limit)
        else "formula_volume_cap"
    )
    notional = (qty * exec_price).quantize(Q, rounding=ROUND_HALF_UP)
    return {
        "execution_grade": "formula_slippage_baseline",
        "order_id": order.order_id,
        "status": status,
        "side": side,
        "reference_price": _decimal(reference_price).quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        "exec_price": exec_price,
        "requested_size": requested,
        "filled_size": qty,
        "available_volume": _decimal(available_volume).quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        "volume_limit": _decimal(volume_limit).quantize(Q, rounding=ROUND_HALF_UP),
        "filled_notional": notional,
        "reason_unfilled": reason,
        "audit_warning": "formula baseline only; no OrderFilled source trade audit",
    }


def build_v2_unsupported_report(
    reason: str, *, requested_layer: str = "unsupported"
) -> dict[str, Any]:
    return {
        "execution_grade": "unsupported",
        "requested_layer": requested_layer,
        "status": "unsupported",
        "reason": reason,
        "not_l2_depth_or_l3_queue": True,
    }


def persist_v2_replay_run(
    conn: Any,
    *,
    market_slug: str,
    token_side: str,
    orders: Iterable[V2TakerOrder],
    results: Iterable[V2OrderResult],
    report: dict[str, Any] | None = None,
    execution_profile: str = "conservative_trade_tape",
    price_source: str = "trade_prints_one_sided",
) -> int:
    order_rows = list(orders)
    result_rows = list(results)
    summary = summarize_v2_results(result_rows)
    from_block = min(
        (order.signal_block for order in order_rows if order.signal_block is not None),
        default=None,
    )
    to_block = max(
        (fill.fill_block for result in result_rows for fill in result.fills),
        default=from_block,
    )
    meta = {
        "model": "orderfilled_v2_trade_tape_taker_participation",
        "report": report or {},
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_runs (
                status, market_slug, token_side, price_source, backtest_engine,
                from_block, to_block, rows_processed, meta, started_at, finished_at
            )
            VALUES ('finished', %s, %s, %s, 'orderfilled_v2_replay', %s, %s, %s, %s::jsonb, now(), now())
            RETURNING run_id
            """,
            (
                market_slug,
                token_side,
                price_source,
                from_block,
                to_block,
                len(result_rows),
                _json_dumps(meta),
            ),
        )
        fetched = cur.fetchone()
        run_id = int(fetched["run_id"] if isinstance(fetched, dict) else fetched[0])
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_parameters (
                run_id, entry_threshold, exit_threshold, stop_loss, take_profit, max_holding_bars,
                initial_capital, position_size, fee_bps, slippage_bps, liquidity_cap_pct,
                execution_price_mode, execution_profile, order_role, latency_seconds,
                allow_partial_fill, final_valuation_mode
            )
            VALUES (%s, 0, 0, 0, 0, 0, 0, 0, 0, 0, %s, 'ORDERFILLED_V2_TAPE', %s, 'taker', %s, true, 'SETTLEMENT')
            ON CONFLICT (run_id) DO UPDATE SET
                liquidity_cap_pct = EXCLUDED.liquidity_cap_pct,
                execution_price_mode = EXCLUDED.execution_price_mode,
                execution_profile = EXCLUDED.execution_profile,
                latency_seconds = EXCLUDED.latency_seconds
            """,
            (
                run_id,
                _decimal(summary.get("participation_utilization", "0"))
                * Decimal("100"),
                execution_profile,
                _avg_order_latency_seconds(order_rows),
            ),
        )
        _insert_v2_metrics(cur, run_id, summary)
        _insert_v2_orders(cur, run_id, market_slug, token_side, order_rows, result_rows)
        _insert_v2_ledger(cur, run_id, market_slug, token_side, result_rows)
        _insert_v2_events(cur, run_id, result_rows)
    return run_id


def summarize_v2_results(results: Iterable[V2OrderResult]) -> dict[str, Any]:
    rows = list(results)
    attempted = len(rows)
    filled = sum(1 for row in rows if row.status == "FILLED")
    partial = sum(1 for row in rows if row.status == "PARTIAL_FILLED")
    unfilled = sum(1 for row in rows if row.status == "NO_FILL")
    simulated_volume = sum(
        (row.simulated_volume for row in rows), Decimal("0")
    ).quantize(Q, rounding=ROUND_HALF_UP)
    eligible_volume = sum(
        (row.eligible_historical_volume for row in rows), Decimal("0")
    ).quantize(Q, rounding=ROUND_HALF_UP)
    weighted_notional = sum(
        (fill.filled_size * fill.exec_price for row in rows for fill in row.fills),
        Decimal("0"),
    )
    avg_price = (
        (weighted_notional / simulated_volume).quantize(Q, rounding=ROUND_HALF_UP)
        if simulated_volume
        else Decimal("0")
    )
    delay_notional = sum(
        (row.avg_fill_delay_seconds * row.simulated_volume for row in rows),
        Decimal("0"),
    )
    buffer_notional = sum(
        (row.avg_price_buffer * row.simulated_volume for row in rows), Decimal("0")
    )
    filled_notional = sum((row.filled_notional for row in rows), Decimal("0")).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    cash_delta = sum((row.cash_delta for row in rows), Decimal("0")).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    position_delta = sum((row.position_delta for row in rows), Decimal("0")).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    return {
        "attempted_orders": attempted,
        "filled_orders": filled,
        "partial_orders": partial,
        "unfilled_orders": unfilled,
        "fill_rate": (Decimal(filled) / Decimal(attempted)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if attempted
        else Decimal("0"),
        "partial_fill_rate": (Decimal(partial) / Decimal(attempted)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if attempted
        else Decimal("0"),
        "simulated_volume": simulated_volume,
        "eligible_historical_volume": eligible_volume,
        "participation_utilization": (simulated_volume / eligible_volume).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if eligible_volume
        else Decimal("0"),
        "avg_execution_price": avg_price,
        "avg_fill_delay_seconds": (delay_notional / simulated_volume).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if simulated_volume
        else Decimal("0"),
        "avg_price_buffer": (buffer_notional / simulated_volume).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if simulated_volume
        else Decimal("0"),
        "filled_notional": filled_notional,
        "cash_delta": cash_delta,
        "position_delta": position_delta,
        "unfilled_reason_distribution": _reason_distribution(rows),
    }


def load_v2_trade_prints(
    *,
    market_id: int,
    asset_id: str,
    from_block: int,
    to_block: int,
    client: ClickHouseClient | None = None,
    limit: int = 10000,
) -> list[V2TradePrint]:
    ch = client or ClickHouseClient()
    asset = _quote_ch(asset_id.lower())
    rows = ch.query_json_rows(
        f"""
        SELECT
            trade_id,
            trade_group_id,
            market_id,
            condition_id,
            asset_id,
            outcome,
            block_number,
            block_time,
            tx_hash,
            tx_index,
            tx_index_source,
            price,
            size_shares,
            notional_usdc,
            aggressor_side,
            passive_side,
            source_log_indexes,
            source_fill_count
        FROM trade_prints_one_sided
        PREWHERE market_id = {int(market_id)}
          AND asset_id = {asset}
          AND block_number BETWEEN {int(from_block)} AND {int(to_block)}
        ORDER BY block_number ASC, tx_index ASC, tx_hash ASC, arrayMin(source_log_indexes) ASC, trade_id ASC
        LIMIT {max(1, int(limit))}
        """,
        timeout_seconds=120,
    )
    return [trade_print_from_row(row) for row in rows]


def _load_v2_trade_window(
    window: RequiredTradeWindow,
    client: ClickHouseClient,
    *,
    limit_per_window: int | None = None,
) -> list[V2TradePrint]:
    asset = _quote_ch(window.asset_id.lower())
    side_filter = ""
    if window.aggressor_side is not None:
        side_filter = f"\n          AND aggressor_side = {_quote_ch(_side(window.aggressor_side))}"
    limit_sql = (
        ""
        if limit_per_window is None
        else f"\n        LIMIT {max(1, int(limit_per_window))}"
    )
    if window.axis == "block":
        if window.start_block is None or window.end_block is None:
            raise ValueError("block trade window is missing a bound")
        bounds_filter = f"block_number BETWEEN {int(window.start_block)} AND {int(window.end_block)}"
    else:
        if window.source_min_block is None or window.source_max_block is None:
            raise ValueError("timestamp trade window needs pinned source block bounds")
        if window.start_ts is None or window.end_ts is None:
            raise ValueError("timestamp trade window is missing a time bound")
        start_ts = _quote_ch(_utc(window.start_ts).isoformat())
        end_ts = _quote_ch(_utc(window.end_ts).isoformat())
        bounds_filter = (
            f"block_number BETWEEN {int(window.source_min_block)} AND {int(window.source_max_block)}"
            f"\n          AND block_time BETWEEN parseDateTime64BestEffort({start_ts})"
            f" AND parseDateTime64BestEffort({end_ts})"
        )
    rows = client.query_json_rows(
        f"""
        SELECT
            trade_id,
            trade_group_id,
            market_id,
            condition_id,
            asset_id,
            outcome,
            block_number,
            block_time,
            tx_hash,
            tx_index,
            tx_index_source,
            price,
            size_shares,
            notional_usdc,
            aggressor_side,
            passive_side,
            source_log_indexes,
            source_fill_count
        FROM trade_prints_one_sided
        PREWHERE market_id = {int(window.market_id)}
          AND asset_id = {asset}
          AND {bounds_filter}{side_filter}
        ORDER BY block_number ASC, tx_index ASC, tx_hash ASC, arrayMin(source_log_indexes) ASC, trade_id ASC{limit_sql}
        """,
        timeout_seconds=120,
    )
    return [trade_print_from_row(row) for row in rows]


def load_v2_wallet_fill_ticks(
    *,
    wallet: str,
    client: ClickHouseClient | None = None,
    market_id: int | None = None,
    asset_id: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    limit: int = 1000,
) -> list[V2WalletFillTick]:
    ch = client or ClickHouseClient()
    clauses = [
        f"(maker = {_quote_ch(wallet.lower())} OR taker = {_quote_ch(wallet.lower())})"
    ]
    if market_id is not None:
        clauses.append(f"market_id = toUInt64({int(market_id)})")
    if asset_id is not None:
        clauses.append(f"asset_id = {_quote_ch(asset_id.lower())}")
    if from_block is not None:
        clauses.append(f"block_number >= toUInt64({int(from_block)})")
    if to_block is not None:
        clauses.append(f"block_number <= toUInt64({int(to_block)})")
    rows = ch.query_json_rows(
        f"""
        SELECT
            fill_id,
            multiIf(maker = {_quote_ch(wallet.lower())}, 'maker', taker = {_quote_ch(wallet.lower())}, 'taker', 'unknown') AS wallet_role,
            market_id,
            condition_id,
            asset_id,
            outcome,
            block_number,
            block_time,
            tx_hash,
            log_index,
            order_hash,
            price,
            size_shares,
            fee_usdc,
            passive_side,
            aggressor_side
        FROM maker_fill_ticks
        WHERE {" AND ".join(clauses)}
        ORDER BY block_number ASC, tx_index ASC, tx_hash ASC, log_index ASC, fill_id ASC
        LIMIT {max(1, int(limit))}
        """,
        timeout_seconds=120,
    )
    return [wallet_fill_tick_from_row(row, wallet=wallet) for row in rows]


def wallet_fill_tick_from_row(row: dict[str, Any], *, wallet: str) -> V2WalletFillTick:
    role = str(row.get("wallet_role") or "").lower()
    if role not in {"maker", "taker"}:
        raise ValueError(f"unsupported wallet role: {role!r}")
    return V2WalletFillTick(
        fill_id=str(row["fill_id"]),
        wallet=wallet.lower(),
        wallet_role=role,  # type: ignore[arg-type]
        market_id=int(row["market_id"]),
        condition_id=str(row.get("condition_id") or ""),
        asset_id=str(row["asset_id"]).lower(),
        outcome=str(row.get("outcome") or ""),
        block_number=int(row["block_number"]),
        block_time=_parse_datetime(row["block_time"]),
        tx_hash=str(row["tx_hash"]).lower(),
        log_index=int(row["log_index"]),
        order_hash=str(row["order_hash"]).lower(),
        price=_decimal(row["price"]),
        size=_decimal(row["size_shares"]),
        fee=_decimal(row.get("fee_usdc", "0")),
        passive_side=_side(row["passive_side"]),
        aggressor_side=_side(row["aggressor_side"]),
    )


def wallet_fill_to_observed_order(
    fill: V2WalletFillTick,
    *,
    size_fraction: Decimal = Decimal("0.025"),
    allowed_buffer: Decimal = Decimal("0"),
    lead_time: timedelta = timedelta(seconds=1),
) -> V2TakerOrder:
    side = fill.aggressor_side if fill.wallet_role == "taker" else fill.passive_side
    return V2TakerOrder(
        order_id=f"wallet-{fill.wallet_role}-{fill.fill_id}",
        market_id=fill.market_id,
        asset_id=fill.asset_id,
        side=side,
        limit_price=_exec_price(
            side, fill.price, max(Decimal("0"), _decimal(allowed_buffer))
        ),
        size=(fill.size * max(Decimal("0"), _decimal(size_fraction))).quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        signal_block=max(0, fill.block_number - 1),
        signal_ts=fill.block_time - lead_time,
        latency=timedelta(0),
        horizon=lead_time + timedelta(seconds=1),
        horizon_blocks=2,
        participation_rate=max(Decimal("0"), _decimal(size_fraction)),
        price_buffer=max(Decimal("0"), _decimal(allowed_buffer)),
    )


def trade_print_from_row(row: dict[str, Any]) -> V2TradePrint:
    return V2TradePrint(
        trade_id=str(row["trade_id"]),
        market_id=int(row["market_id"]),
        condition_id=str(row.get("condition_id") or ""),
        asset_id=str(row["asset_id"]).lower(),
        outcome=str(row.get("outcome") or ""),
        block_number=int(row["block_number"]),
        block_time=_parse_datetime(row["block_time"]),
        tx_hash=str(row["tx_hash"]).lower(),
        tx_index=int(row.get("tx_index") or 0),
        tx_index_source=str(row.get("tx_index_source") or ""),
        price=_decimal(row["price"]),
        size=_decimal(row["size_shares"]),
        notional=_decimal(row["notional_usdc"]),
        aggressor_side=_side(row["aggressor_side"]),
        passive_side=_side(row["passive_side"]),
        source_log_indexes=tuple(
            int(value) for value in (row.get("source_log_indexes") or [])
        ),
        source_fill_count=int(row.get("source_fill_count") or 0),
        trade_group_id=str(row.get("trade_group_id") or "") or None,
    )


def _result(
    order: V2TakerOrder,
    status: str,
    remaining: Decimal,
    fills: list[V2Fill],
    eligible_volume: Decimal,
    reason: str,
    *,
    validity: Any | None = None,
    probability: Any | None = None,
) -> V2OrderResult:
    filled = sum((fill.filled_size for fill in fills), Decimal("0")).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    notional = sum((fill.filled_size * fill.exec_price for fill in fills), Decimal("0"))
    avg = (
        (notional / filled).quantize(Q, rounding=ROUND_HALF_UP)
        if filled
        else Decimal("0")
    )
    utilization = (
        (filled / eligible_volume).quantize(Q, rounding=ROUND_HALF_UP)
        if eligible_volume
        else Decimal("0")
    )
    avg_delay = _avg_fill_delay_seconds(order, fills, filled)
    buffer_paid = sum(
        (fill.filled_size * fill.price_buffer_paid for fill in fills), Decimal("0")
    )
    avg_buffer = (
        (buffer_paid / filled).quantize(Q, rounding=ROUND_HALF_UP)
        if filled
        else Decimal("0")
    )
    filled_notional = notional.quantize(Q, rounding=ROUND_HALF_UP)
    cash_sign = Decimal("-1") if _side(order.side) == "BUY" else Decimal("1")
    position_sign = Decimal("1") if _side(order.side) == "BUY" else Decimal("-1")
    return V2OrderResult(
        order_id=order.order_id,
        status=status,
        side=_side(order.side),
        requested_size=_decimal(order.size).quantize(Q, rounding=ROUND_HALF_UP),
        filled_size=filled,
        unfilled_size=max(Decimal("0"), remaining).quantize(Q, rounding=ROUND_HALF_UP),
        avg_price=avg,
        limit_price=_decimal(order.limit_price).quantize(Q, rounding=ROUND_HALF_UP),
        arrival_block=order.arrival_block,
        arrival_ts=order.arrival_ts,
        eligible_historical_volume=eligible_volume.quantize(Q, rounding=ROUND_HALF_UP),
        simulated_volume=filled,
        participation_rate=max(
            Decimal("0"), min(Decimal("1"), _decimal(order.participation_rate))
        ).quantize(Q, rounding=ROUND_HALF_UP),
        capacity_utilization=utilization,
        avg_fill_delay_seconds=avg_delay,
        avg_price_buffer=avg_buffer,
        filled_notional=filled_notional,
        cash_delta=(cash_sign * filled_notional).quantize(Q, rounding=ROUND_HALF_UP),
        position_delta=(position_sign * filled).quantize(Q, rounding=ROUND_HALF_UP),
        reason_unfilled="" if status == "FILLED" else _standard_unfilled_reason(reason),
        p_depth_valid=validity.p_depth_valid if validity is not None else None,
        fill_only_eligibility=validity.eligibility if validity is not None else None,
        fill_validity_reason=validity.reason if validity is not None else "",
        fill_validity_features=validity.features.as_dict()
        if validity is not None
        else None,
        fill_validity_rule=validity.rule.as_dict() if validity is not None else None,
        p_fill=probability.p_fill if probability is not None else None,
        conditional_capacity_fraction=probability.conditional_capacity_fraction
        if probability is not None
        else None,
        fill_capacity_variant=probability.capacity_variant
        if probability is not None
        else None,
        fill_probability_eligibility=probability.eligibility
        if probability is not None
        else None,
        fill_probability_reason=probability.reason if probability is not None else "",
        fill_probability_features=probability.features.as_dict()
        if probability is not None
        else None,
        fill_probability_profile=_cached_probability_profile_payload(
            probability.profile
        )
        if probability is not None
        else None,
        execution_profile_name=order.execution_profile_name,
        execution_profile_activation=order.execution_profile_activation,
        execution_stability_grade=order.execution_stability_grade,
        fills=tuple(fills),
    )


def _avg_fill_delay_seconds(
    order: V2TakerOrder, fills: list[V2Fill], filled: Decimal
) -> Decimal:
    if not fills or filled <= 0:
        return Decimal("0")
    if order.arrival_ts is not None:
        delay = sum(
            (
                fill.filled_size
                * Decimal(
                    str(max(0.0, (fill.fill_ts - order.arrival_ts).total_seconds()))
                )
                for fill in fills
            ),
            Decimal("0"),
        )
        return (delay / filled).quantize(Q, rounding=ROUND_HALF_UP)
    if order.arrival_block is not None:
        delay = sum(
            (
                fill.filled_size
                * Decimal(max(0, fill.fill_block - order.arrival_block))
                for fill in fills
            ),
            Decimal("0"),
        )
        return (delay / filled).quantize(Q, rounding=ROUND_HALF_UP)
    return Decimal("0")


def _insert_v2_metrics(cur: Any, run_id: int, summary: dict[str, Any]) -> None:
    metric_items = [
        (
            "attempted_orders",
            "Attempted orders",
            "execution",
            summary.get("attempted_orders"),
            10,
        ),
        (
            "filled_orders",
            "Filled orders",
            "execution",
            summary.get("filled_orders"),
            20,
        ),
        (
            "partial_orders",
            "Partial orders",
            "execution",
            summary.get("partial_orders"),
            30,
        ),
        (
            "unfilled_orders",
            "Unfilled orders",
            "execution",
            summary.get("unfilled_orders"),
            40,
        ),
        ("fill_rate", "Fill rate", "execution", summary.get("fill_rate"), 50),
        (
            "simulated_volume",
            "Simulated volume",
            "capacity",
            summary.get("simulated_volume"),
            60,
        ),
        (
            "eligible_historical_volume",
            "Eligible historical volume",
            "capacity",
            summary.get("eligible_historical_volume"),
            70,
        ),
        (
            "participation_utilization",
            "Participation utilization",
            "capacity",
            summary.get("participation_utilization"),
            80,
        ),
        (
            "avg_fill_delay_seconds",
            "Average fill delay seconds",
            "latency",
            summary.get("avg_fill_delay_seconds"),
            90,
        ),
        (
            "avg_price_buffer",
            "Average price buffer",
            "price",
            summary.get("avg_price_buffer"),
            100,
        ),
        ("cash_delta", "Cash delta", "pnl", summary.get("cash_delta"), 110),
        ("position_delta", "Position delta", "pnl", summary.get("position_delta"), 120),
    ]
    for key, name, group, value, sort_order in metric_items:
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_metrics (
                run_id, metric_key, metric_name, metric_group, value, formatted_value, status, sort_order
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'neutral', %s)
            ON CONFLICT (run_id, metric_key) DO UPDATE SET
                value = EXCLUDED.value,
                formatted_value = EXCLUDED.formatted_value,
                status = EXCLUDED.status,
                sort_order = EXCLUDED.sort_order
            """,
            (run_id, key, name, group, _decimal(value), str(value), sort_order),
        )


def _insert_v2_orders(
    cur: Any,
    run_id: int,
    market_slug: str,
    token_side: str,
    orders: list[V2TakerOrder],
    results: list[V2OrderResult],
) -> None:
    by_id = {result.order_id: result for result in results}
    for index, order in enumerate(orders):
        result = by_id[order.order_id]
        signal_x = order.signal_block or result.arrival_block or 0
        submit_x = result.arrival_block or signal_x
        meta = {
            "market_id": order.market_id,
            "asset_id": order.asset_id,
            "arrival_ts": result.arrival_ts.isoformat() if result.arrival_ts else None,
            "fills": [fill.as_dict() for fill in result.fills],
        }
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_orders (
                run_id, order_id, signal_index, x_axis, signal_x, submit_x, decision_price,
                requested_price, side, role, order_type, status, requested_size, requested_notional,
                actual_fill_size, actual_fill_notional, filled_size, filled_notional, unfilled_size,
                avg_fill_price, fill_pct, block_volume, trade_count, available_notional,
                participation_rate, latency_blocks, latency_seconds, no_fill_reason,
                execution_source, execution_evidence_type, raw_candidate_event_count,
                raw_consumed_event_count, meta
            )
            VALUES (
                %s, %s, %s, 'block_number', %s, %s, %s,
                %s, %s, 'taker', 'limit', %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                'trade_prints_one_sided', 'orderfilled_v2_trade_print', %s,
                %s, %s::jsonb
            )
            ON CONFLICT (run_id, order_id) DO UPDATE SET
                status = EXCLUDED.status,
                actual_fill_size = EXCLUDED.actual_fill_size,
                actual_fill_notional = EXCLUDED.actual_fill_notional,
                filled_size = EXCLUDED.filled_size,
                filled_notional = EXCLUDED.filled_notional,
                unfilled_size = EXCLUDED.unfilled_size,
                avg_fill_price = EXCLUDED.avg_fill_price,
                fill_pct = EXCLUDED.fill_pct,
                no_fill_reason = EXCLUDED.no_fill_reason,
                meta = EXCLUDED.meta
            """,
            (
                run_id,
                result.order_id,
                index,
                signal_x,
                submit_x,
                order.limit_price,
                order.limit_price,
                result.side,
                result.status,
                result.requested_size,
                result.requested_size * order.limit_price,
                result.filled_size,
                result.filled_notional,
                result.filled_size,
                result.filled_notional,
                result.unfilled_size,
                result.avg_price if result.fills else None,
                (result.filled_size / result.requested_size * Decimal("100")).quantize(
                    Q, rounding=ROUND_HALF_UP
                )
                if result.requested_size
                else Decimal("0"),
                result.eligible_historical_volume,
                len(result.fills),
                result.eligible_historical_volume * result.avg_price,
                result.participation_rate,
                order.latency_blocks,
                Decimal(str(order.latency.total_seconds())).quantize(
                    Q, rounding=ROUND_HALF_UP
                ),
                result.reason_unfilled or None,
                len(result.fills),
                len(result.fills),
                _json_dumps(meta),
            ),
        )


def _insert_v2_ledger(
    cur: Any,
    run_id: int,
    market_slug: str,
    token_side: str,
    results: list[V2OrderResult],
) -> None:
    position = Decimal("0")
    cash = Decimal("0")
    counter = 0
    for result in results:
        for fill in result.fills:
            counter += 1
            position = (
                position
                + (fill.filled_size if fill.side == "BUY" else -fill.filled_size)
            ).quantize(Q, rounding=ROUND_HALF_UP)
            cash = (
                cash
                + (
                    -(fill.filled_size * fill.exec_price)
                    if fill.side == "BUY"
                    else fill.filled_size * fill.exec_price
                )
            ).quantize(Q, rounding=ROUND_HALF_UP)
            ledger_id = f"{result.order_id}:{counter}:{fill.source_trade_id}"
            cur.execute(
                """
                INSERT INTO quant.quant_backtest_ledger (
                    run_id, ledger_id, order_id, event_type, x_axis, x_value, market_slug, token_side,
                    shares_delta, cash_delta, position_after, cash_after, price, source, meta
                )
                VALUES (%s, %s, %s, 'FILL', 'block_number', %s, %s, %s, %s, %s, %s, %s, %s, 'orderfilled_v2_replay', %s::jsonb)
                ON CONFLICT (run_id, ledger_id) DO UPDATE SET
                    shares_delta = EXCLUDED.shares_delta,
                    cash_delta = EXCLUDED.cash_delta,
                    position_after = EXCLUDED.position_after,
                    cash_after = EXCLUDED.cash_after,
                    meta = EXCLUDED.meta
                """,
                (
                    run_id,
                    ledger_id,
                    result.order_id,
                    fill.fill_block,
                    market_slug,
                    token_side,
                    fill.filled_size if fill.side == "BUY" else -fill.filled_size,
                    -(fill.filled_size * fill.exec_price)
                    if fill.side == "BUY"
                    else fill.filled_size * fill.exec_price,
                    position,
                    cash,
                    fill.exec_price,
                    _json_dumps(fill.as_dict()),
                ),
            )


def _insert_v2_events(cur: Any, run_id: int, results: list[V2OrderResult]) -> None:
    event_index = 0
    for result in results:
        x_value = result.arrival_block or 0
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_events (
                run_id, event_index, event_type, x_axis, x_value, trade_id, price, message, meta
            )
            VALUES (%s, %s, 'ORDER_RESULT', 'block_number', %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (run_id, event_index) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                x_value = EXCLUDED.x_value,
                price = EXCLUDED.price,
                message = EXCLUDED.message,
                meta = EXCLUDED.meta
            """,
            (
                run_id,
                event_index,
                x_value,
                result.order_id,
                result.avg_price if result.fills else None,
                result.status,
                _json_dumps(result.as_dict()),
            ),
        )
        event_index += 1


def _avg_order_latency_seconds(orders: list[V2TakerOrder]) -> Decimal:
    if not orders:
        return Decimal("0")
    total = sum(
        (Decimal(str(order.latency.total_seconds())) for order in orders), Decimal("0")
    )
    return (total / Decimal(len(orders))).quantize(Q, rounding=ROUND_HALF_UP)


def _maker_result(
    order: V2MakerOrder,
    profile: V2ExecutionProfile,
    fills: list[V2Fill],
    candidate_volume: Decimal,
    remaining_phantom_queue: Decimal,
    status: str,
    reason: str,
    *,
    initial_queue: Decimal | None = None,
) -> V2MakerResult:
    filled = sum((fill.filled_size for fill in fills), Decimal("0")).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    requested = _decimal(order.size).quantize(Q, rounding=ROUND_HALF_UP)
    queue = (
        _decimal(profile.maker_phantom_queue)
        if initial_queue is None
        else initial_queue
    )
    return V2MakerResult(
        order_id=order.order_id,
        status=status,
        side=_side(order.side),
        requested_size=requested,
        filled_size=filled,
        unfilled_size=max(Decimal("0"), requested - filled).quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        limit_price=_decimal(order.limit_price).quantize(Q, rounding=ROUND_HALF_UP),
        maker_mode=profile.maker_mode,
        initial_phantom_queue=queue.quantize(Q, rounding=ROUND_HALF_UP),
        remaining_phantom_queue=remaining_phantom_queue.quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        candidate_historical_volume=candidate_volume.quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        reason_unfilled="" if status == "FILLED" else reason,
        fills=tuple(fills),
    )


def _curve(
    orders: list[V2TakerOrder],
    trades: list[V2TradePrint],
    field_name: str,
    values: Iterable[Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for value in values:
        adjusted = [replace(order, **{field_name: value}) for order in orders]
        results, _ = replay_v2_taker_orders(adjusted, trades)
        rows.append(
            {"value": _curve_value(value), "summary": summarize_v2_results(results)}
        )
    return rows


def _default_parameter_grid() -> list[dict[str, Any]]:
    return [
        {
            "participation_rate": Decimal("0.005"),
            "latency": timedelta(seconds=5),
            "horizon": timedelta(seconds=30),
            "price_buffer": Decimal("0.01"),
        },
        {
            "participation_rate": Decimal("0.025"),
            "latency": timedelta(seconds=1),
            "horizon": timedelta(minutes=5),
            "price_buffer": Decimal("0.005"),
        },
        {
            "participation_rate": Decimal("0.05"),
            "latency": timedelta(0),
            "horizon": timedelta(minutes=15),
            "price_buffer": Decimal("0"),
        },
    ]


def _apply_parameter_row(order: V2TakerOrder, params: dict[str, Any]) -> V2TakerOrder:
    allowed = {
        "participation_rate",
        "latency",
        "horizon",
        "price_buffer",
        "latency_blocks",
        "horizon_blocks",
    }
    updates = {key: value for key, value in params.items() if key in allowed}
    return replace(order, **updates)


def _walk_forward_rows(
    orders: list[V2TakerOrder], trades: list[V2TradePrint], splits: int
) -> list[dict[str, Any]]:
    if not trades:
        return []
    split_count = max(1, int(splits))
    min_block = min(trade.block_number for trade in trades)
    max_block = max(trade.block_number for trade in trades)
    span = max(1, max_block - min_block + 1)
    width = max(1, (span + split_count - 1) // split_count)
    rows: list[dict[str, Any]] = []
    for index in range(split_count):
        start = min_block + index * width
        end = min(max_block, start + width - 1)
        window_trades = [
            trade for trade in trades if start <= trade.block_number <= end
        ]
        window_orders = [
            order
            for order in orders
            if order.signal_block is None or order.signal_block <= end
        ]
        results, _ = replay_v2_taker_orders(window_orders, window_trades)
        rows.append(
            {
                "index": index,
                "from_block": start,
                "to_block": end,
                "trade_print_rows": len(window_trades),
                "summary": summarize_v2_results(results),
            }
        )
    return rows


def _category_split_rows(
    orders: list[V2TakerOrder],
    trades: list[V2TradePrint],
    category_by_market: dict[int, str],
) -> list[dict[str, Any]]:
    categories = sorted(
        {
            category_by_market.get(int(item.market_id), "uncategorized")
            for item in orders
        }
        | {
            category_by_market.get(int(item.market_id), "uncategorized")
            for item in trades
        }
    )
    rows: list[dict[str, Any]] = []
    for category in categories:
        cat_orders = [
            order
            for order in orders
            if category_by_market.get(int(order.market_id), "uncategorized") == category
        ]
        cat_trades = [
            trade
            for trade in trades
            if category_by_market.get(int(trade.market_id), "uncategorized") == category
        ]
        results, _ = replay_v2_taker_orders(cat_orders, cat_trades)
        rows.append(
            {
                "category": category,
                "orders": len(cat_orders),
                "trade_print_rows": len(cat_trades),
                "summary": summarize_v2_results(results),
            }
        )
    return rows


def _regime_split_rows(
    orders: list[V2TakerOrder], trades: list[V2TradePrint]
) -> list[dict[str, Any]]:
    if not trades:
        return []
    sizes = sorted(trade.size for trade in trades)
    threshold = sizes[len(sizes) // 2]
    rows: list[dict[str, Any]] = []
    for regime, predicate in (
        ("low_trade_size", lambda trade: trade.size < threshold),
        ("high_trade_size", lambda trade: trade.size >= threshold),
    ):
        regime_trades = [trade for trade in trades if predicate(trade)]
        results, _ = replay_v2_taker_orders(orders, regime_trades)
        rows.append(
            {
                "regime": regime,
                "threshold_size": threshold.quantize(Q, rounding=ROUND_HALF_UP),
                "trade_print_rows": len(regime_trades),
                "summary": summarize_v2_results(results),
            }
        )
    return rows


def _overfit_warning(parameter_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not parameter_rows:
        return {"status": "empty", "parameter_count": 0}
    cash_values = [
        _decimal(row["summary"].get("cash_delta", "0")) for row in parameter_rows
    ]
    best = max(cash_values)
    worst = min(cash_values)
    fragile = len(parameter_rows) > 1 and best > 0 and worst <= 0
    dsr = _deflated_sharpe_ratio_heuristic(cash_values)
    return {
        "status": "review" if fragile or dsr["status"] == "review" else "ok",
        "parameter_count": len(parameter_rows),
        "best_cash_delta": best.quantize(Q, rounding=ROUND_HALF_UP),
        "worst_cash_delta": worst.quantize(Q, rounding=ROUND_HALF_UP),
        "deflated_sharpe_ratio_heuristic": dsr,
        "note": "heuristic only; use full returns distribution for production DSR",
    }


def _deflated_sharpe_ratio_heuristic(values: list[Decimal]) -> dict[str, Any]:
    if len(values) < 2:
        return {"status": "insufficient", "value": Decimal("0")}
    floats = [float(value) for value in values]
    mean = sum(floats) / len(floats)
    variance = sum((value - mean) ** 2 for value in floats) / max(1, len(floats) - 1)
    if variance <= 0:
        return {"status": "insufficient", "value": Decimal("0")}
    stdev = variance**0.5
    sharpe = mean / stdev
    penalty = (len(values) ** 0.5 - 1.0) / max(1.0, len(values) ** 0.5)
    adjusted = Decimal(str(sharpe - penalty)).quantize(Q, rounding=ROUND_HALF_UP)
    return {
        "status": "review" if adjusted < 0 else "ok",
        "value": adjusted,
        "raw_sharpe": Decimal(str(sharpe)).quantize(Q, rounding=ROUND_HALF_UP),
        "trial_count": len(values),
    }


def _curve_value(value: Any) -> str:
    if isinstance(value, timedelta):
        return f"{Decimal(str(value.total_seconds())).quantize(Q, rounding=ROUND_HALF_UP)}s"
    return str(value)


def _int_percentile(values: list[int], percentile: Decimal) -> Decimal:
    if not values:
        return Decimal("0")
    ordered = sorted(int(value) for value in values)
    pct = max(Decimal("0"), min(Decimal("1"), _decimal(percentile)))
    index = int(
        (Decimal(len(ordered) - 1) * pct).to_integral_value(rounding=ROUND_HALF_UP)
    )
    return Decimal(ordered[index]).quantize(Q, rounding=ROUND_HALF_UP)


def _pnl_assumption(
    summary: dict[str, Any], settlement_price: Decimal | None, fee_bps: Decimal
) -> dict[str, Any]:
    filled_notional = _decimal(summary.get("filled_notional", "0"))
    cash_delta = _decimal(summary.get("cash_delta", "0"))
    position_delta = _decimal(summary.get("position_delta", "0"))
    fees = (filled_notional * _decimal(fee_bps) / Decimal("10000")).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    if settlement_price is None:
        return {
            "status": "settlement_not_provided",
            "fee_bps": _decimal(fee_bps).quantize(Q, rounding=ROUND_HALF_UP),
            "estimated_fees": fees,
            "cash_delta_after_fees": (cash_delta - fees).quantize(
                Q, rounding=ROUND_HALF_UP
            ),
        }
    settlement_value = (position_delta * _decimal(settlement_price)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    return {
        "status": "estimated",
        "fee_bps": _decimal(fee_bps).quantize(Q, rounding=ROUND_HALF_UP),
        "settlement_price": _decimal(settlement_price).quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        "estimated_fees": fees,
        "settlement_value": settlement_value,
        "pnl_after_fees_and_settlement": (
            cash_delta + settlement_value - fees
        ).quantize(Q, rounding=ROUND_HALF_UP),
    }


def _reason_distribution(results: Iterable[V2OrderResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        reason = result.reason_unfilled or "filled"
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items()))


def _same_maker_market_asset(order: V2MakerOrder, trade: V2TradePrint) -> bool:
    return (
        int(order.market_id) == int(trade.market_id)
        and str(order.asset_id).lower() == trade.asset_id.lower()
    )


def _after_maker_arrival(order: V2MakerOrder, trade: V2TradePrint) -> bool:
    if order.arrival_block is not None and trade.block_number < order.arrival_block:
        return False
    if order.arrival_ts is not None and trade.block_time < order.arrival_ts:
        return False
    return True


def _before_maker_deadline(order: V2MakerOrder, trade: V2TradePrint) -> bool:
    if order.deadline_block is not None and trade.block_number > order.deadline_block:
        return False
    if order.deadline_ts is not None and trade.block_time > order.deadline_ts:
        return False
    return True


def _maker_trade_can_hit(side: OrderSide, trade: V2TradePrint, limit: Decimal) -> bool:
    if side == "BUY":
        return trade.aggressor_side == "SELL" and trade.price <= limit
    return trade.aggressor_side == "BUY" and trade.price >= limit


def _capacity_remaining_after_pending(
    capacity: CapacityLedger,
    order: V2TakerOrder,
    trade: V2TradePrint,
    participation_rate: Decimal,
    pending_by_trade: dict[str, Decimal],
    pending_by_window: dict[tuple[int, str, str, int], Decimal],
) -> Decimal:
    trade_remaining = capacity.remaining(
        trade, participation_rate
    ) - pending_by_trade.get(trade.trade_id, Decimal("0"))
    window_remaining = capacity.market_window_remaining(order, trade)
    window_key = capacity.market_window_key(order, trade)
    if window_key is not None:
        window_remaining -= pending_by_window.get(window_key, Decimal("0"))
    return max(Decimal("0"), min(trade_remaining, window_remaining)).quantize(
        Q, rounding=ROUND_HALF_UP
    )


def _requires_full_fill(order: V2TakerOrder) -> bool:
    return _tif(order.tif) == "FOK" or not bool(order.allow_partial_fill)


def _full_fill_reject_reason(order: V2TakerOrder, base_reason: str) -> str:
    if _tif(order.tif) == "FOK":
        return "fok_insufficient_orderfilled_capacity"
    return base_reason or "full_fill_required_insufficient_orderfilled_capacity"


def _is_cancel_remainder_tif(tif: str) -> bool:
    return _tif(tif) in {"IOC", "FAK"}


def _fill_evidence_gate(
    order: V2TakerOrder,
    trades: Iterable[V2TradePrint],
    side: OrderSide,
    limit: Decimal,
    price_buffer: Decimal,
) -> FillEvidenceGateResult:
    ordered = tuple(trades)
    trailing_count = 0
    trailing_volume = Decimal("0")
    last_quote_price: Decimal | None = None
    future_count = 0
    future_volume = Decimal("0")

    for trade in ordered:
        if (
            not _same_market_asset(order, trade)
            or _is_excluded_source_trade(order, trade)
            or not _trade_side_is_eligible(order, trade, side)
        ):
            continue
        if _before_arrival_strict(order, trade) and _within_quote_ttl(order, trade):
            last_quote_price = trade.price
            if _limit_allows(side, trade.price, limit):
                trailing_count += 1
                trailing_volume += trade.size
        if (
            _after_arrival(order, trade)
            and _before_deadline(order, trade)
            and _limit_allows(side, trade.price, limit)
        ):
            exec_price = _exec_price(side, trade.price, price_buffer)
            if _limit_allows(side, exec_price, limit):
                future_count += 1
                future_volume += trade.size

    trailing_volume = trailing_volume.quantize(Q, rounding=ROUND_HALF_UP)
    future_volume = future_volume.quantize(Q, rounding=ROUND_HALF_UP)
    if order.require_pre_arrival_quote_proxy:
        if last_quote_price is None:
            return _gate_result(
                "missing_pre_arrival_trade_quote_proxy",
                trailing_count,
                trailing_volume,
                future_count,
                future_volume,
                Decimal("0"),
            )
        if not _limit_allows(side, last_quote_price, limit):
            return _gate_result(
                "pre_arrival_trade_quote_outside_limit",
                trailing_count,
                trailing_volume,
                future_count,
                future_volume,
                Decimal("0"),
            )
    min_trailing_count = max(0, int(order.min_trailing_same_side_trade_count or 0))
    if trailing_count < min_trailing_count:
        return _gate_result(
            "insufficient_trailing_same_side_trade_count",
            trailing_count,
            trailing_volume,
            future_count,
            future_volume,
            Decimal("0"),
        )
    min_trailing_volume = max(
        _decimal(order.min_trailing_same_side_volume),
        _decimal(order.size)
        * max(Decimal("0"), _decimal(order.trailing_volume_multiplier)),
    )
    if trailing_volume < min_trailing_volume:
        return _gate_result(
            "insufficient_trailing_same_side_volume",
            trailing_count,
            trailing_volume,
            future_count,
            future_volume,
            Decimal("0"),
        )
    min_future_count = max(0, int(order.min_future_eligible_trade_count or 0))
    if future_count < min_future_count:
        return _gate_result(
            "insufficient_future_eligible_trade_count",
            trailing_count,
            trailing_volume,
            future_count,
            future_volume,
            Decimal("0"),
        )
    min_future_volume = max(Decimal("0"), _decimal(order.min_future_eligible_volume))
    if future_volume < min_future_volume:
        return _gate_result(
            "insufficient_future_eligible_volume",
            trailing_count,
            trailing_volume,
            future_count,
            future_volume,
            Decimal("0"),
        )

    cap = max(Decimal("0"), _decimal(order.size)).quantize(Q, rounding=ROUND_HALF_UP)
    if order.max_fill_size_per_order is not None:
        cap = min(
            cap, max(Decimal("0"), _decimal(order.max_fill_size_per_order))
        ).quantize(Q, rounding=ROUND_HALF_UP)
    if order.trailing_participation_rate is not None:
        trailing_cap = (
            trailing_volume
            * max(
                Decimal("0"),
                min(Decimal("1"), _decimal(order.trailing_participation_rate)),
            )
        ).quantize(Q, rounding=ROUND_HALF_UP)
        cap = min(cap, trailing_cap).quantize(Q, rounding=ROUND_HALF_UP)
    if cap <= 0 and (
        order.max_fill_size_per_order is not None
        or order.trailing_participation_rate is not None
    ):
        return _gate_result(
            "fill_evidence_order_capacity_zero",
            trailing_count,
            trailing_volume,
            future_count,
            future_volume,
            Decimal("0"),
        )
    return _gate_result(
        "", trailing_count, trailing_volume, future_count, future_volume, cap
    )


def _lob_validity_decision(
    order: V2TakerOrder,
    trades: Iterable[V2TradePrint],
    side: OrderSide,
    limit: Decimal,
    price_buffer: Decimal,
) -> Any | None:
    if not order.lob_validity_rule:
        return None
    return FillOnlyLobValidityModel(order.lob_validity_rule).decide(
        order,
        trades,
        side=side,
        limit=limit,
        price_buffer=price_buffer,
    )


def _apply_lob_validity_capacity(capacity: Decimal, decision: Any | None) -> Decimal:
    if decision is None:
        return capacity.quantize(Q, rounding=ROUND_HALF_UP)
    return (capacity * _decimal(decision.capacity_multiplier)).quantize(
        Q, rounding=ROUND_HALF_UP
    )


def _orderfilled_probability_profile(order: V2TakerOrder) -> Any | None:
    if not order.orderfilled_probability_profile:
        return None
    return _cached_orderfilled_probability_model(
        order.orderfilled_probability_profile
    ).profile


def _orderfilled_probability_decision(
    order: V2TakerOrder,
    trades: Iterable[V2TradePrint],
    *,
    presorted_relevant: bool = False,
) -> Any | None:
    if not order.orderfilled_probability_profile:
        return None
    return _cached_orderfilled_probability_model(
        order.orderfilled_probability_profile
    ).decide(order, trades, presorted_relevant=presorted_relevant)


_ORDERFILLED_PROBABILITY_MODEL_CACHE: dict[
    int, tuple[dict[str, Any], OrderFilledProbabilityModel]
] = {}
_ORDERFILLED_PROBABILITY_PROFILE_PAYLOAD_CACHE: dict[
    int, tuple[Any, dict[str, Any]]
] = {}


def _cached_orderfilled_probability_model(
    payload: dict[str, Any],
) -> OrderFilledProbabilityModel:
    key = id(payload)
    cached = _ORDERFILLED_PROBABILITY_MODEL_CACHE.get(key)
    if cached is not None and cached[0] is payload:
        return cached[1]
    if len(_ORDERFILLED_PROBABILITY_MODEL_CACHE) >= 64:
        _ORDERFILLED_PROBABILITY_MODEL_CACHE.clear()
    model = OrderFilledProbabilityModel(payload)
    _ORDERFILLED_PROBABILITY_MODEL_CACHE[key] = (payload, model)
    return model


def _cached_probability_profile_payload(profile: Any) -> dict[str, Any]:
    key = id(profile)
    cached = _ORDERFILLED_PROBABILITY_PROFILE_PAYLOAD_CACHE.get(key)
    if cached is not None and cached[0] is profile:
        return cached[1]
    if len(_ORDERFILLED_PROBABILITY_PROFILE_PAYLOAD_CACHE) >= 64:
        _ORDERFILLED_PROBABILITY_PROFILE_PAYLOAD_CACHE.clear()
    payload = profile.as_dict()
    _ORDERFILLED_PROBABILITY_PROFILE_PAYLOAD_CACHE[key] = (profile, payload)
    return payload


def _apply_orderfilled_probability_capacity(
    capacity: Decimal, decision: Any | None
) -> Decimal:
    if decision is None:
        return capacity.quantize(Q, rounding=ROUND_HALF_UP)
    return (capacity * _decimal(decision.capacity_multiplier)).quantize(
        Q, rounding=ROUND_HALF_UP
    )


def _orderfilled_probability_context(
    index: Mapping[tuple[int, str, str], TradeGroupIndex[Any]],
    order: V2TakerOrder,
    *,
    combined_groups: Mapping[tuple[int, str], TradeGroupIndex] | None = None,
) -> tuple[V2TradePrint, ...]:
    if not order.orderfilled_probability_profile:
        return ()
    market_id = int(order.market_id)
    asset_id = str(order.asset_id).lower()
    if combined_groups is not None:
        group = combined_groups.get((market_id, asset_id))
        return _order_relevant_trade_slice(group, order) if group is not None else ()
    rows: list[V2TradePrint] = []
    for side in ("BUY", "SELL"):
        group = index.get((market_id, asset_id, side))
        if group is not None:
            rows.extend(group.trades)
    return tuple(sorted(rows, key=lambda item: item.sequence))


def _execution_trade_group(
    index: Mapping[tuple[int, str, str], TradeGroupIndex[Any]],
    order: V2TakerOrder,
    side: OrderSide,
    *,
    combined_groups: Mapping[tuple[int, str], TradeGroupIndex] | None = None,
) -> TradeGroupIndex | None:
    market_id = int(order.market_id)
    asset_id = str(order.asset_id).lower()
    if order.trade_side_evidence_mode != "any_order_side":
        return index.get((market_id, asset_id, side))
    if combined_groups is not None:
        return combined_groups.get((market_id, asset_id))
    rows: list[V2TradePrint] = []
    for source_side in ("BUY", "SELL"):
        group = index.get((market_id, asset_id, source_side))
        if group is not None:
            rows.extend(group.trades)
    if not rows:
        return None
    ordered = tuple(sorted(rows, key=lambda item: item.sequence))
    return TradeGroupIndex(
        key=(market_id, asset_id, side),
        trades=ordered,
        block_numbers=tuple(row.block_number for row in ordered),
        block_times=tuple(row.block_time for row in ordered),
    )


def _order_relevant_trade_slice(
    group: TradeGroupIndex,
    order: V2TakerOrder,
) -> tuple[V2TradePrint, ...]:
    """Select only evidence that an order's rules can inspect."""

    needs_pre_arrival = bool(
        order.orderfilled_probability_profile
        or order.lob_validity_rule
        or order.require_pre_arrival_quote_proxy
        or int(order.min_trailing_same_side_trade_count or 0) > 0
        or _decimal(order.min_trailing_same_side_volume) > 0
        or _decimal(order.trailing_volume_multiplier) > 0
        or order.trailing_participation_rate is not None
    )
    lookback_blocks = max(0, int(order.trade_slice_lookback_blocks or 0))
    lookback_seconds = max(0.0, float(order.trade_slice_lookback.total_seconds()))
    probability_profile = _orderfilled_probability_profile(order)
    if probability_profile is not None:
        lookback_blocks = max(lookback_blocks, int(probability_profile.lookback_blocks))
        lookback_seconds = max(
            lookback_seconds,
            float(probability_profile.lookback_seconds),
        )
    if order.quote_proxy_ttl is not None:
        lookback_seconds = max(
            lookback_seconds,
            float(order.quote_proxy_ttl.total_seconds()),
        )
    if order.lob_validity_rule:
        lookback_seconds = max(
            lookback_seconds,
            float(order.lob_validity_rule.get("trailing_window_seconds", 0) or 0),
        )

    start = 0
    end = len(group.trades)
    if order.arrival_block is not None:
        first_block = (
            int(order.arrival_block) - lookback_blocks
            if needs_pre_arrival
            else int(order.arrival_block)
        )
        start = max(start, bisect_left(group.block_numbers, first_block))
    if order.deadline_block is not None:
        end = min(end, bisect_right(group.block_numbers, int(order.deadline_block)))
    if order.arrival_ts is not None:
        first_ts = (
            _utc(order.arrival_ts) - timedelta(seconds=lookback_seconds)
            if needs_pre_arrival
            else _utc(order.arrival_ts)
        )
        start = max(start, bisect_left(group.block_times, first_ts))
    if order.deadline_ts is not None:
        end = min(end, bisect_right(group.block_times, _utc(order.deadline_ts)))
    if end <= start:
        return ()
    return tuple(group.trades[start:end])


def _trade_side_is_eligible(
    order: V2TakerOrder, trade: V2TradePrint, side: OrderSide
) -> bool:
    if order.trade_side_evidence_mode == "any_order_side":
        return True
    return trade.aggressor_side == side


def _no_trade_reason(order: V2TakerOrder) -> str:
    if order.trade_side_evidence_mode == "any_order_side":
        return "no_post_arrival_economic_limit_trade"
    return "no_post_arrival_same_side_limit_trade"


def _gate_result(
    reason: str,
    trailing_count: int,
    trailing_volume: Decimal,
    future_count: int,
    future_volume: Decimal,
    cap: Decimal,
) -> FillEvidenceGateResult:
    return FillEvidenceGateResult(
        reason=reason,
        trailing_trade_count=trailing_count,
        trailing_volume=trailing_volume.quantize(Q, rounding=ROUND_HALF_UP),
        future_eligible_trade_count=future_count,
        future_eligible_volume=future_volume.quantize(Q, rounding=ROUND_HALF_UP),
        order_remaining_cap=cap.quantize(Q, rounding=ROUND_HALF_UP),
    )


def _is_excluded_source_trade(order: V2TakerOrder, trade: V2TradePrint) -> bool:
    if not order.exclude_signal_source_trade:
        return False
    if order.signal_source_trade_id and trade.trade_id == order.signal_source_trade_id:
        return True
    if (
        order.signal_source_tx_hash
        and str(trade.tx_hash or "").lower() == str(order.signal_source_tx_hash).lower()
    ):
        return True
    source_logs = set(int(value) for value in (order.signal_source_log_indexes or ()))
    return bool(
        source_logs
        and source_logs.intersection(
            set(int(value) for value in trade.source_log_indexes)
        )
    )


def _before_arrival_strict(order: V2TakerOrder, trade: V2TradePrint) -> bool:
    if order.arrival_block is not None and trade.block_number >= order.arrival_block:
        return False
    if order.arrival_ts is not None and trade.block_time >= order.arrival_ts:
        return False
    return True


def _within_quote_ttl(order: V2TakerOrder, trade: V2TradePrint) -> bool:
    if order.quote_proxy_ttl is None or order.arrival_ts is None:
        return True
    return trade.block_time >= order.arrival_ts - order.quote_proxy_ttl


def _unfilled_reason(
    remaining: Decimal, saw_trade: bool, buffer_blocked: bool, capacity_exhausted: bool
) -> str:
    if remaining <= 0:
        return ""
    if not saw_trade:
        return "no_post_arrival_same_side_limit_trade"
    if buffer_blocked:
        return "price_buffer_exceeds_limit"
    if capacity_exhausted:
        return "trade_print_capacity_already_consumed"
    return "insufficient_post_arrival_same_side_orderfilled_capacity"


def _unfilled_reason_for_order(
    order: V2TakerOrder,
    remaining: Decimal,
    saw_trade: bool,
    buffer_blocked: bool,
    capacity_exhausted: bool,
) -> str:
    reason = _unfilled_reason(remaining, saw_trade, buffer_blocked, capacity_exhausted)
    if order.trade_side_evidence_mode != "any_order_side":
        return reason
    aliases = {
        "no_post_arrival_same_side_limit_trade": "no_post_arrival_economic_limit_trade",
        "insufficient_post_arrival_same_side_orderfilled_capacity": "insufficient_post_arrival_economic_orderfilled_capacity",
    }
    return aliases.get(reason, reason)


def _standard_unfilled_reason(reason: str) -> str:
    text = str(reason or "")
    aliases = {
        "no_post_arrival_same_side_limit_trade": "no_post_arrival_same_side_trade",
        "no_post_arrival_economic_limit_trade": "no_post_arrival_economic_trade",
        "trade_print_capacity_already_consumed": "insufficient_trade_capacity",
        "insufficient_post_arrival_same_side_orderfilled_capacity": "insufficient_trade_capacity",
        "insufficient_post_arrival_economic_orderfilled_capacity": "insufficient_trade_capacity",
        "fok_insufficient_orderfilled_capacity": "insufficient_trade_capacity",
        "full_fill_required_insufficient_orderfilled_capacity": "insufficient_trade_capacity",
        "insufficient_future_eligible_trade_count": "future_trade_count_too_low",
        "insufficient_trailing_same_side_volume": "insufficient_trailing_volume",
        "insufficient_trailing_same_side_trade_count": "insufficient_trailing_trade_count",
        "lob_holdout_calibrated_missing_quote_proxy": "missing_pre_arrival_quote_proxy",
        "lob_holdout_calibrated_horizon_too_long": "unsupported_long_horizon",
        "lob_holdout_calibrated_low_validity_probability": "low_p_depth_valid",
        "fill_evidence_order_capacity_zero": "insufficient_trade_capacity",
        "maker_strict_no_fill": "unsupported_maker_order",
    }
    return aliases.get(text, text)


def _same_market_asset(order: V2TakerOrder, trade: V2TradePrint) -> bool:
    return (
        int(order.market_id) == int(trade.market_id)
        and str(order.asset_id).lower() == trade.asset_id.lower()
    )


def _after_arrival(order: V2TakerOrder, trade: V2TradePrint) -> bool:
    if order.arrival_block is not None and trade.block_number < order.arrival_block:
        return False
    if order.arrival_ts is not None and trade.block_time < order.arrival_ts:
        return False
    return True


def _before_deadline(order: V2TakerOrder, trade: V2TradePrint) -> bool:
    if order.deadline_block is not None and trade.block_number > order.deadline_block:
        return False
    if order.deadline_ts is not None and trade.block_time > order.deadline_ts:
        return False
    return True


def _limit_allows(side: OrderSide, price: Decimal, limit: Decimal) -> bool:
    return price <= limit if side == "BUY" else price >= limit


def _exec_price(side: OrderSide, historical_price: Decimal, buffer: Decimal) -> Decimal:
    price = historical_price + buffer if side == "BUY" else historical_price - buffer
    return max(Decimal("0"), min(Decimal("1"), price)).quantize(
        Q, rounding=ROUND_HALF_UP
    )


def _side(value: Any) -> OrderSide:
    text = str(value or "").upper().replace("_YES", "").replace("_NO", "")
    if text not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported side: {value!r}")
    return text  # type: ignore[return-value]


def _tif(value: Any) -> TimeInForce:
    text = str(value or "GTC").upper()
    if text not in {"GTC", "GTD", "IOC", "FOK", "FAK"}:
        return "GTC"
    return text  # type: ignore[return-value]


def _is_immediate_tif(value: Any) -> bool:
    return _tif(value) in {"IOC", "FOK", "FAK"}


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or "0"))


def _json_dumps(value: Any) -> str:
    return json.dumps(_json_ready(value), ensure_ascii=False, sort_keys=True)


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value.total_seconds())
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    return value


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _utc(value)
    text = str(value).strip().replace("Z", "+00:00")
    if "+" not in text and "T" not in text:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        return parsed.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(text)
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _quote_ch(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
