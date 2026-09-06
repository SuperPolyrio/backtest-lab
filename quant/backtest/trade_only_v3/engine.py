from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections.abc import Iterable
from dataclasses import asdict, dataclass, replace
from datetime import timedelta
from decimal import ROUND_HALF_UP, Decimal
from functools import lru_cache
from itertools import pairwise
from pathlib import Path
from time import perf_counter
from typing import Any, Literal

from quant.backtest.orderfilled_probability import (
    PROBABILITY_TARGET_FAK_ANY_FILL,
    PROBABILITY_TARGET_FOK_FULL_FILL,
    OrderFilledProbabilityDecision,
    OrderFilledProbabilityFeatures,
    OrderFilledProbabilityModel,
    OrderFilledProbabilityProfile,
    extract_orderfilled_probability_features,
    load_orderfilled_probability_profile,
    probability_profile_domain_violations,
)
from quant.backtest.orderfilled_v2_replay import (
    CapacityLedger as V2CapacityLedger,
)
from quant.backtest.orderfilled_v2_replay import (
    V2TakerOrder,
    V2TradePrint,
    replay_v2_taker_order,
    replay_v2_taker_order_indexed,
    replay_v2_taker_orders_with_diagnostics,
)
from quant.backtest.prepared_trade_tape import (
    PreparedTradeTape,
    prepare_trade_tape,
    slice_trade_group,
)
from quant.backtest.rust_kernel import (
    monte_carlo_fill_distribution_python,
    monte_carlo_fill_distribution_rust,
    rust_kernel_available,
)

from .hierarchical_prior import (
    context_for_order,
    load_hierarchical_prior_profile,
    resolve_hierarchical_prior,
)
from .models import (
    ExecutionMode,
    LiquidityIntent,
    TradeOnlyFill,
    TradeOnlyOrder,
    TradeOnlyOrderResult,
    TradeOnlyProfile,
    get_trade_only_profile,
)
from .price_buffer import resolve_price_buffer

Q = Decimal("0.0000000001")
TICK = Decimal("0.01")


@dataclass(frozen=True)
class V3ReplayDiagnostics:
    orders_count: int
    trade_rows_indexed: int
    trade_groups: int
    index_build_sec: Decimal
    matching_sec: Decimal
    candidate_rows_scanned: int
    naive_rows_scanned: int
    scan_reduction_ratio: Decimal
    window_queries: int
    index_reused: bool
    matching_backend: str = "python"
    backend_fallback_reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class _TradeReplayContext:
    trades: tuple[V2TradePrint, ...]
    prepared: PreparedTradeTape[V2TradePrint] | None
    indexed: bool
    candidate_rows_scanned: int = 0
    window_queries: int = 0

    def future(self, order: TradeOnlyOrder) -> tuple[V2TradePrint, ...]:
        self.window_queries += 1
        if not self.indexed or self.prepared is None:
            self.candidate_rows_scanned += len(self.trades)
            return tuple(
                trade
                for trade in self.trades
                if _same(order, trade)
                and (
                    order.arrival_block is None
                    or trade.block_number >= order.arrival_block
                )
                and (
                    order.deadline_block is None
                    or trade.block_number <= order.deadline_block
                )
                and trade.block_time >= order.arrival_ts
                and trade.block_time <= order.deadline_ts
                and trade.trade_id != order.signal_source_trade_id
            )
        rows = slice_trade_group(
            self.prepared.group(order.market_id, order.asset_id),
            start_block=order.arrival_block,
            end_block=order.deadline_block,
            start_ts=order.arrival_ts,
            end_ts=order.deadline_ts,
        )
        self.candidate_rows_scanned += len(rows)
        return tuple(
            trade for trade in rows if trade.trade_id != order.signal_source_trade_id
        )

    def pre_arrival(self, order: TradeOnlyOrder) -> tuple[V2TradePrint, ...]:
        self.window_queries += 1
        start_ts = order.arrival_ts - order.lookback
        start_block = (
            max(0, order.arrival_block - order.lookback_blocks)
            if order.arrival_block is not None
            else None
        )
        cache: dict[tuple[Any, ...], tuple[V2TradePrint, ...]] | None = None
        cache_key: tuple[Any, ...] | None = None
        if self.indexed and self.prepared is not None:
            cache = self.prepared.backend_payloads.setdefault(
                "v3_pre_arrival_windows_v1", {}
            )
            cache_key = (
                order.market_id,
                order.asset_id.lower(),
                order.arrival_block,
                order.arrival_ts,
                order.lookback,
                order.lookback_blocks,
                order.signal_source_trade_id,
            )
            cached = cache.get(cache_key)
            if cached is not None:
                return cached
        if not self.indexed or self.prepared is None:
            self.candidate_rows_scanned += len(self.trades)
            return tuple(
                trade
                for trade in self.trades
                if _same(order, trade)
                and (start_block is None or trade.block_number >= start_block)
                and (
                    order.arrival_block is None
                    or trade.block_number < order.arrival_block
                )
                and trade.block_time >= start_ts
                and trade.block_time < order.arrival_ts
                and trade.trade_id != order.signal_source_trade_id
            )
        rows = slice_trade_group(
            self.prepared.group(order.market_id, order.asset_id),
            start_block=start_block,
            end_block=order.arrival_block,
            start_ts=start_ts,
            end_ts=order.arrival_ts,
            end_inclusive=False,
        )
        self.candidate_rows_scanned += len(rows)
        result = tuple(
            trade for trade in rows if trade.trade_id != order.signal_source_trade_id
        )
        if cache is not None and cache_key is not None:
            if len(cache) >= 100_000:
                cache.clear()
            cache[cache_key] = result
        return result

    def note_candidates(self, count: int) -> None:
        self.window_queries += 1
        self.candidate_rows_scanned += max(0, int(count))


class RunLiquidityLedger:
    """Shared source and synthetic capacity for one replay run."""

    def __init__(self, ledger_id: str) -> None:
        self.ledger_id = ledger_id
        self.v2 = V2CapacityLedger()
        self._source_consumed: dict[str, Decimal] = {}
        self._synthetic_consumed: dict[tuple[int, str, str, int], Decimal] = {}
        self._delta_tracking = False
        self._delta_source: dict[str, Decimal] = {}
        self._delta_synthetic: dict[tuple[int, str, str, int], Decimal] = {}

    def source_remaining(self, trade: V2TradePrint, rate: Decimal) -> Decimal:
        cap = (_d(trade.size) * _bounded(rate)).quantize(Q, rounding=ROUND_HALF_UP)
        return max(
            Decimal(0), cap - self._source_consumed.get(trade.trade_id, Decimal(0))
        )

    def consume_source(self, trade: V2TradePrint, quantity: Decimal) -> None:
        self._source_consumed[trade.trade_id] = (
            self._source_consumed.get(trade.trade_id, Decimal(0)) + _d(quantity)
        ).quantize(Q, rounding=ROUND_HALF_UP)
        if self._delta_tracking:
            self._delta_source[trade.trade_id] = (
                self._delta_source.get(trade.trade_id, Decimal(0)) + _d(quantity)
            ).quantize(Q, rounding=ROUND_HALF_UP)

    def synthetic_key(self, order: TradeOnlyOrder) -> tuple[int, str, str, int]:
        return (
            order.market_id,
            order.asset_id.lower(),
            order.side,
            int(order.arrival_ts.timestamp()),
        )

    def synthetic_remaining(self, order: TradeOnlyOrder, capacity: Decimal) -> Decimal:
        return max(
            Decimal(0),
            _d(capacity)
            - self._synthetic_consumed.get(self.synthetic_key(order), Decimal(0)),
        )

    def consume_synthetic(self, order: TradeOnlyOrder, quantity: Decimal) -> None:
        key = self.synthetic_key(order)
        self._synthetic_consumed[key] = (
            self._synthetic_consumed.get(key, Decimal(0)) + _d(quantity)
        ).quantize(Q, rounding=ROUND_HALF_UP)
        if self._delta_tracking:
            self._delta_synthetic[key] = (
                self._delta_synthetic.get(key, Decimal(0)) + _d(quantity)
            ).quantize(Q, rounding=ROUND_HALF_UP)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ledger_id": self.ledger_id,
            "v2_source_capacity": self.v2.as_dict(),
            "inferred_source_capacity": {
                key: str(value) for key, value in sorted(self._source_consumed.items())
            },
            "synthetic_arrival_capacity": {
                "|".join(map(str, key)): str(value)
                for key, value in sorted(self._synthetic_consumed.items())
            },
        }

    def begin_delta_tracking(self) -> None:
        if self._delta_tracking:
            raise RuntimeError("V3 liquidity ledger delta tracking is already active")
        self.v2.begin_delta_tracking()
        self._delta_tracking = True
        self._delta_source.clear()
        self._delta_synthetic.clear()

    def drain_delta(self) -> dict[str, Any]:
        if not self._delta_tracking:
            raise RuntimeError("V3 liquidity ledger delta tracking is not active")
        payload = {
            "schema_version": "V3LiquidityLedgerDeltaV1",
            "v2": self.v2.drain_delta().as_dict(),
            "inferred_source_capacity": {
                key: str(value) for key, value in sorted(self._delta_source.items())
            },
            "synthetic_arrival_capacity": {
                "|".join(map(str, key)): str(value)
                for key, value in sorted(self._delta_synthetic.items())
            },
        }
        self._delta_tracking = False
        self._delta_source.clear()
        self._delta_synthetic.clear()
        return payload

    def apply_delta(self, payload: dict[str, Any]) -> None:
        if self._delta_tracking:
            raise RuntimeError("cannot apply V3 ledger delta during delta tracking")
        self.v2.apply_delta(dict(payload.get("v2") or {}))
        for trade_id, quantity in dict(
            payload.get("inferred_source_capacity") or {}
        ).items():
            self._source_consumed[str(trade_id)] = (
                self._source_consumed.get(str(trade_id), Decimal(0)) + _d(quantity)
            ).quantize(Q, rounding=ROUND_HALF_UP)
        for encoded, quantity in dict(
            payload.get("synthetic_arrival_capacity") or {}
        ).items():
            market_id, asset_id, side, epoch = str(encoded).split("|", 3)
            key = (int(market_id), asset_id, side, int(epoch))
            self._synthetic_consumed[key] = (
                self._synthetic_consumed.get(key, Decimal(0)) + _d(quantity)
            ).quantize(Q, rounding=ROUND_HALF_UP)

    def prune(
        self,
        *,
        source_trade_ids: Iterable[str],
        synthetic_not_before_epoch: int | None = None,
    ) -> dict[str, int]:
        if self._delta_tracking:
            raise RuntimeError("cannot prune V3 liquidity ledger during delta tracking")
        retained_ids = {str(value) for value in source_trade_ids}
        v2_dropped = self.v2.retain_source_trade_ids(retained_ids)
        previous_source = len(self._source_consumed)
        self._source_consumed = {
            key: value
            for key, value in self._source_consumed.items()
            if key in retained_ids
        }
        previous_synthetic = len(self._synthetic_consumed)
        if synthetic_not_before_epoch is not None:
            self._synthetic_consumed = {
                key: value
                for key, value in self._synthetic_consumed.items()
                if key[3] >= int(synthetic_not_before_epoch)
            }
        return {
            "v2_source_keys_dropped": v2_dropped,
            "inferred_source_keys_dropped": previous_source
            - len(self._source_consumed),
            "synthetic_keys_dropped": previous_synthetic
            - len(self._synthetic_consumed),
        }

    def snapshot(self) -> dict[str, Any]:
        if self._delta_tracking:
            raise RuntimeError(
                "cannot snapshot V3 liquidity ledger during delta tracking"
            )
        return {
            "schema_version": "V3RunLiquidityLedgerSnapshotV1",
            "ledger_id": self.ledger_id,
            "v2": self.v2.snapshot(),
            "inferred_source_capacity": {
                key: str(value) for key, value in sorted(self._source_consumed.items())
            },
            "synthetic_arrival_capacity": [
                {
                    "market_id": key[0],
                    "asset_id": key[1],
                    "side": key[2],
                    "arrival_epoch": key[3],
                    "quantity": str(value),
                }
                for key, value in sorted(self._synthetic_consumed.items())
            ],
        }

    @classmethod
    def from_snapshot(cls, payload: dict[str, Any]) -> RunLiquidityLedger:
        if payload.get("schema_version") != "V3RunLiquidityLedgerSnapshotV1":
            raise ValueError("unsupported V3 liquidity ledger snapshot")
        ledger = cls(str(payload["ledger_id"]))
        ledger.v2 = V2CapacityLedger.from_snapshot(payload["v2"])
        ledger._source_consumed = {
            str(key): _d(value)
            for key, value in dict(
                payload.get("inferred_source_capacity") or {}
            ).items()
        }
        ledger._synthetic_consumed = {
            (
                int(row["market_id"]),
                str(row["asset_id"]),
                str(row["side"]),
                int(row["arrival_epoch"]),
            ): _d(row["quantity"])
            for row in payload.get("synthetic_arrival_capacity") or []
        }
        return ledger


def replay_trade_only_orders(
    orders: Iterable[TradeOnlyOrder],
    trades: Iterable[V2TradePrint] | None,
    profile: str | TradeOnlyProfile,
    *,
    ledger_id: str = "trade-only-v3-run",
    ledger: RunLiquidityLedger | None = None,
    prepared_tape: PreparedTradeTape[V2TradePrint] | None = None,
    backend: Literal["auto", "python", "rust"] = "auto",
) -> tuple[list[TradeOnlyOrderResult], RunLiquidityLedger]:
    results, capacity, _ = replay_trade_only_orders_with_diagnostics(
        orders,
        trades,
        profile,
        ledger_id=ledger_id,
        ledger=ledger,
        prepared_tape=prepared_tape,
        backend=backend,
    )
    return results, capacity


def replay_trade_only_orders_reference(
    orders: Iterable[TradeOnlyOrder],
    trades: Iterable[V2TradePrint],
    profile: str | TradeOnlyProfile,
    *,
    ledger_id: str = "trade-only-v3-run",
    ledger: RunLiquidityLedger | None = None,
) -> tuple[list[TradeOnlyOrderResult], RunLiquidityLedger]:
    """Preserve the original full-scan implementation as a parity oracle."""

    resolved = get_trade_only_profile(profile) if isinstance(profile, str) else profile
    trade_rows = tuple(sorted(trades, key=lambda row: row.sequence))
    capacity = ledger or RunLiquidityLedger(ledger_id)
    context = _TradeReplayContext(trade_rows, prepared=None, indexed=False)
    results = [_replay_one(order, context, resolved, capacity) for order in orders]
    return results, capacity


def replay_trade_only_orders_with_diagnostics(
    orders: Iterable[TradeOnlyOrder],
    trades: Iterable[V2TradePrint] | None,
    profile: str | TradeOnlyProfile,
    *,
    ledger_id: str = "trade-only-v3-run",
    ledger: RunLiquidityLedger | None = None,
    prepared_tape: PreparedTradeTape[V2TradePrint] | None = None,
    backend: Literal["auto", "python", "rust"] = "auto",
) -> tuple[list[TradeOnlyOrderResult], RunLiquidityLedger, V3ReplayDiagnostics]:
    """Replay V3 orders against one reusable indexed trade partition."""

    resolved = get_trade_only_profile(profile) if isinstance(profile, str) else profile
    order_rows = list(orders)
    if prepared_tape is not None:
        if trades is not None:
            raise ValueError("pass either trades or prepared_tape, not both")
        prepared = prepared_tape
        index_build_sec = Decimal(0)
        index_reused = True
    else:
        if trades is None:
            raise ValueError("trades or prepared_tape is required")
        prepared = prepare_trade_tape(trades)
        index_build_sec = prepared.index_build_sec
        index_reused = False
    capacity = ledger or RunLiquidityLedger(ledger_id)
    requested_backend = str(backend).lower()
    if requested_backend not in {"auto", "python", "rust"}:
        raise ValueError(f"unsupported V3 matcher backend: {backend!r}")
    if resolved.execution_mode == ExecutionMode.SOURCE_CONFIRMED_TAPE and all(
        order.size > 0 and order.liquidity_intent != LiquidityIntent.PASSIVE
        for order in order_rows
    ):
        v2_orders = [
            _source_confirmed_v2_order(order, resolved) for order in order_rows
        ]
        v2_results, _, v2_diagnostics = replay_v2_taker_orders_with_diagnostics(
            v2_orders,
            ledger=capacity.v2,
            prepared_tape=prepared,
            backend=backend,
        )
        results = [
            _source_confirmed_result(order, resolved, capacity, result)
            for order, result in zip(order_rows, v2_results, strict=True)
        ]
        naive_rows = len(order_rows) * prepared.trade_rows_indexed
        diagnostics = V3ReplayDiagnostics(
            orders_count=len(order_rows),
            trade_rows_indexed=prepared.trade_rows_indexed,
            trade_groups=prepared.trade_groups,
            index_build_sec=index_build_sec,
            matching_sec=v2_diagnostics.matching_sec,
            candidate_rows_scanned=v2_diagnostics.candidate_rows_scanned,
            naive_rows_scanned=naive_rows,
            scan_reduction_ratio=(
                Decimal(v2_diagnostics.candidate_rows_scanned) / Decimal(naive_rows)
            ).quantize(Q, rounding=ROUND_HALF_UP)
            if naive_rows
            else Decimal(0),
            window_queries=len(order_rows),
            index_reused=index_reused,
            matching_backend=v2_diagnostics.matching_backend,
            backend_fallback_reason=v2_diagnostics.backend_fallback_reason,
        )
        return results, capacity, diagnostics
    if resolved.execution_mode == ExecutionMode.CENTRAL_ROUTER and all(
        order.size > 0 and order.liquidity_intent == LiquidityIntent.TAKER
        for order in order_rows
    ):
        return _replay_central_taker_batch(
            order_rows,
            prepared,
            resolved,
            capacity,
            backend=backend,
            index_build_sec=index_build_sec,
            index_reused=index_reused,
        )
    modeled_backend = "python"
    fallback_reason = ""
    if resolved.execution_mode == ExecutionMode.GENERATIVE_TAPE_MC:
        if requested_backend in {"auto", "rust"} and rust_kernel_available():
            modeled_backend = "rust"
        elif requested_backend == "rust":
            raise ValueError("V3 Rust Monte Carlo kernel is not installed")
        elif requested_backend == "auto":
            fallback_reason = "rust_extension_not_installed"
    elif requested_backend == "rust":
        raise ValueError(
            "V3 Rust matcher currently supports compatible SOURCE_CONFIRMED_TAPE taker batches only"
        )
    elif requested_backend == "auto":
        fallback_reason = "execution_mode_not_supported_by_rust"
    context = _TradeReplayContext((), prepared=prepared, indexed=True)
    started = perf_counter()
    results = [
        _replay_one(order, context, resolved, capacity, backend=modeled_backend)
        for order in order_rows
    ]
    matching_sec = Decimal(str(perf_counter() - started)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    naive_rows = len(order_rows) * prepared.trade_rows_indexed
    diagnostics = V3ReplayDiagnostics(
        orders_count=len(order_rows),
        trade_rows_indexed=prepared.trade_rows_indexed,
        trade_groups=prepared.trade_groups,
        index_build_sec=index_build_sec,
        matching_sec=matching_sec,
        candidate_rows_scanned=context.candidate_rows_scanned,
        naive_rows_scanned=naive_rows,
        scan_reduction_ratio=(
            Decimal(context.candidate_rows_scanned) / Decimal(naive_rows)
        ).quantize(Q, rounding=ROUND_HALF_UP)
        if naive_rows
        else Decimal(0),
        window_queries=context.window_queries,
        index_reused=index_reused,
        matching_backend=modeled_backend,
        backend_fallback_reason=fallback_reason,
    )
    return results, capacity, diagnostics


def _replay_one(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
    *,
    backend: str = "python",
) -> TradeOnlyOrderResult:
    if order.size <= 0:
        return _empty(order, profile, "REJECTED", "invalid_order_size")
    if profile.execution_mode == ExecutionMode.SOURCE_CONFIRMED_TAPE:
        return _source_confirmed(order, trades, profile, ledger)
    if profile.execution_mode == ExecutionMode.PASSIVE_TRADE_THROUGH_LOWER:
        return _passive_trade_through(order, trades, profile, ledger)
    if profile.execution_mode == ExecutionMode.PASSIVE_TOUCH_SURVIVAL:
        return _passive_touch_survival(order, trades, profile, ledger)
    if profile.execution_mode == ExecutionMode.SYNTHETIC_ARRIVAL_LIQUIDITY:
        return _synthetic_arrival(order, trades, profile, ledger)
    if profile.execution_mode == ExecutionMode.GENERATIVE_TAPE_MC:
        return _generative_mc(order, trades, profile, ledger, backend=backend)
    if profile.execution_mode == ExecutionMode.HIERARCHICAL_EXPECTED_FILL:
        return _hierarchical_expected_fill(order, trades, profile, ledger)
    if profile.execution_mode == ExecutionMode.CENTRAL_ROUTER:
        return _central_router(order, trades, profile, ledger)
    return _auto_bound(order, trades, profile, ledger)


def _source_confirmed(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
) -> TradeOnlyOrderResult:
    if order.liquidity_intent == LiquidityIntent.PASSIVE:
        return _empty(
            order,
            profile,
            "UNSUPPORTED",
            "source_confirmed_taker_requires_taker_intent",
        )
    v2_order = _source_confirmed_v2_order(order, profile)
    if trades.indexed and trades.prepared is not None:
        candidate_counts: list[int] = []
        result = replay_v2_taker_order_indexed(
            v2_order,
            trades.prepared.index,
            ledger.v2,
            candidate_counts=candidate_counts,
            combined_groups=trades.prepared.combined_groups,
        )
        trades.note_candidates(sum(candidate_counts))
    else:
        result = replay_v2_taker_order(
            v2_order, trades.trades, ledger.v2, trades_are_ordered=True
        )
    return _source_confirmed_result(order, profile, ledger, result)


def _source_confirmed_v2_order(
    order: TradeOnlyOrder,
    profile: TradeOnlyProfile,
) -> V2TakerOrder:
    return V2TakerOrder(
        order_id=order.order_id,
        market_id=order.market_id,
        asset_id=order.asset_id,
        side=order.side,
        limit_price=order.limit_price,
        size=order.size,
        signal_block=order.signal_block,
        signal_ts=order.signal_ts,
        latency_blocks=order.latency_blocks,
        latency=order.latency,
        horizon_blocks=order.horizon_blocks,
        horizon=order.horizon,
        participation_rate=profile.participation_rate,
        price_buffer=profile.price_buffer,
        tif=order.tif,
        allow_partial_fill=order.allow_partial_fill,
        signal_source_trade_id=order.signal_source_trade_id,
        exclude_signal_source_trade=True,
        min_future_eligible_trade_count=1,
        execution_profile_name=profile.name,
        execution_profile_activation=profile.calibration_status,
    )


def _source_confirmed_result(
    order: TradeOnlyOrder,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
    result: Any,
) -> TradeOnlyOrderResult:
    fills = tuple(
        TradeOnlyFill(
            order_id=order.order_id,
            execution_mode=profile.execution_mode.value,
            evidence_tier=profile.evidence_tier.value,
            trigger_type="ONE_SECOND_TAPE_PROXY"
            if order.tif in {"IOC", "FAK", "FOK"}
            else "FUTURE_SOURCE_TRADE",
            filled_size=fill.filled_size,
            exec_price=fill.exec_price,
            fill_ts=fill.fill_ts,
            fill_block=fill.fill_block,
            source_trade_ids=(fill.source_trade_id,),
            source_tx_hashes=(fill.source_tx_hash,),
            source_log_indexes=fill.source_log_indexes,
            fill_time_lower=fill.fill_ts,
            fill_time_upper=fill.fill_ts,
            model_version=profile.model_version,
            feature_snapshot_hash=_feature_hash(order, []),
            random_seed=order.random_seed,
            run_liquidity_ledger_id=ledger.ledger_id,
        )
        for fill in result.fills
    )
    return _result(
        order,
        profile,
        fills,
        reason=result.reason_unfilled,
        status=result.status,
    )


def _passive_trade_through(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
) -> TradeOnlyOrderResult:
    if order.liquidity_intent == LiquidityIntent.TAKER:
        return _empty(
            order, profile, "UNSUPPORTED", "trade_through_requires_passive_intent"
        )
    if order.tif in {"IOC", "FAK", "FOK"}:
        return _empty(
            order, profile, "UNOBSERVABLE", "UNOBSERVABLE_IMMEDIATE_LIQUIDITY"
        )
    crosses = [
        trade
        for trade in _future(order, trades)
        if _passive_trigger(order, trade) == "STRICT_CROSS"
    ]
    if not crosses:
        return _empty(order, profile, "NO_FILL", "no_post_arrival_strict_trade_through")
    remaining = order.size
    fills: list[TradeOnlyFill] = []
    for trade in crosses:
        capacity = ledger.source_remaining(trade, profile.participation_rate)
        quantity = min(remaining, capacity).quantize(Q, rounding=ROUND_HALF_UP)
        if quantity <= 0:
            continue
        ledger.consume_source(trade, quantity)
        fills.append(
            _source_inferred_fill(
                order, profile, ledger, trade, quantity, "STRICT_CROSS"
            )
        )
        remaining -= quantity
        if remaining <= 0:
            break
    return _result(
        order, profile, tuple(fills), reason="insufficient_trade_through_capacity"
    )


def _passive_touch_survival(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
) -> TradeOnlyOrderResult:
    if order.liquidity_intent == LiquidityIntent.TAKER:
        return _empty(
            order, profile, "UNSUPPORTED", "touch_survival_requires_passive_intent"
        )
    if order.tif in {"IOC", "FAK", "FOK"}:
        return _empty(
            order, profile, "UNOBSERVABLE", "UNOBSERVABLE_IMMEDIATE_LIQUIDITY"
        )
    future = _future(order, trades)
    touches = [trade for trade in future if _passive_trigger(order, trade) == "TOUCH"]
    crosses = [
        trade for trade in future if _passive_trigger(order, trade) == "STRICT_CROSS"
    ]
    if not touches and not crosses:
        return _empty(order, profile, "NO_FILL", "no_post_arrival_touch_or_cross")
    first_touch = min(touches + crosses, key=lambda row: row.sequence)
    first_cross = min(crosses, key=lambda row: row.sequence) if crosses else None
    probabilities = _touch_probability_bounds(order, trades, first_touch, first_cross)
    variant = profile.touch_probability_variant or "expected"
    probability = probabilities[variant]
    source_rows = crosses if variant == "lower" and crosses else touches + crosses
    source_volume = sum((_d(row.size) for row in source_rows), Decimal(0))
    raw_capacity = source_volume * profile.participation_rate * probability
    quantity = min(order.size, raw_capacity).quantize(Q, rounding=ROUND_HALF_UP)
    if quantity <= 0:
        return _empty(
            order,
            profile,
            "NO_FILL",
            "touch_survival_lower_bound_zero",
            probability_bounds=probabilities,
        )
    # The first trigger owns the deterministic expected allocation; the source
    # ledger prevents another modeled order from reusing the same print.
    allocation_source = (
        first_cross if variant == "lower" and first_cross is not None else first_touch
    )
    available = ledger.source_remaining(allocation_source, profile.participation_rate)
    quantity = min(quantity, available).quantize(Q, rounding=ROUND_HALF_UP)
    if quantity <= 0:
        return _empty(
            order,
            profile,
            "NO_FILL",
            "touch_source_capacity_consumed",
            probability_bounds=probabilities,
        )
    ledger.consume_source(allocation_source, quantity)
    fill = TradeOnlyFill(
        order_id=order.order_id,
        execution_mode=profile.execution_mode.value,
        evidence_tier=profile.evidence_tier.value,
        trigger_type="TOUCH_INTERVAL_SURVIVAL",
        filled_size=quantity,
        exec_price=order.limit_price.quantize(Q, rounding=ROUND_HALF_UP),
        fill_ts=first_cross.block_time
        if first_cross is not None and variant == "lower"
        else first_touch.block_time,
        fill_block=first_cross.block_number
        if first_cross is not None and variant == "lower"
        else first_touch.block_number,
        source_trade_ids=tuple(
            row.trade_id for row in (first_touch, first_cross) if row is not None
        ),
        source_tx_hashes=tuple(
            row.tx_hash for row in (first_touch, first_cross) if row is not None
        ),
        source_log_indexes=tuple(
            index
            for row in (first_touch, first_cross)
            if row is not None
            for index in row.source_log_indexes
        ),
        touch_ts=first_touch.block_time,
        cross_ts=first_cross.block_time if first_cross is not None else None,
        fill_time_lower=first_touch.block_time,
        fill_time_upper=first_cross.block_time
        if first_cross is not None
        else order.deadline_ts,
        p_fill_1s=probabilities["p_1s"],
        p_fill_5s=probabilities["p_5s"],
        p_fill_30s=probabilities["p_30s"],
        p_fill_horizon=probability,
        model_version=profile.model_version,
        feature_snapshot_hash=_feature_hash(order, _pre_arrival(order, trades)),
        random_seed=order.random_seed,
        run_liquidity_ledger_id=ledger.ledger_id,
    )
    return _result(order, profile, (fill,), probability_bounds=probabilities)


def _synthetic_arrival(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
) -> TradeOnlyOrderResult:
    state = _arrival_state(order, trades, profile)
    if state is None:
        return _empty(
            order,
            profile,
            "NO_FILL",
            "insufficient_pre_arrival_tape_for_synthetic_arrival",
        )
    quantile = profile.capacity_quantile or Decimal("0.50")
    capacity = _d(state["capacity_q50"])
    if quantile <= Decimal("0.10"):
        capacity = _d(state["capacity_q10"])
    elif quantile >= Decimal("0.90"):
        capacity = _d(state["capacity_q90"])
    capacity = ledger.synthetic_remaining(order, capacity)
    exec_price = _d(state["exec_price"])
    if not _limit_allows(order, exec_price):
        return _empty(
            order,
            profile,
            "NO_FILL",
            "synthetic_arrival_price_outside_limit",
            capacity_bounds=_capacity_bounds(state),
        )
    if order.tif == "FOK" or not order.allow_partial_fill:
        quantity = order.size if capacity >= order.size else Decimal(0)
    else:
        quantity = min(order.size, capacity)
    quantity = quantity.quantize(Q, rounding=ROUND_HALF_UP)
    if quantity <= 0:
        reason = (
            "fok_synthetic_capacity_insufficient"
            if order.tif == "FOK"
            else "synthetic_arrival_capacity_zero"
        )
        return _empty(
            order, profile, "NO_FILL", reason, capacity_bounds=_capacity_bounds(state)
        )
    ledger.consume_synthetic(order, quantity)
    fill = TradeOnlyFill(
        order_id=order.order_id,
        execution_mode=profile.execution_mode.value,
        evidence_tier=profile.evidence_tier.value,
        trigger_type="MODEL_INFERRED_AT_ARRIVAL",
        filled_size=quantity,
        exec_price=exec_price,
        fill_ts=order.arrival_ts,
        fill_block=order.arrival_block,
        capacity_q10=_d(state["capacity_q10"]),
        capacity_q50=_d(state["capacity_q50"]),
        capacity_q90=_d(state["capacity_q90"]),
        latent_mid=_d(state["latent_mid"]),
        latent_spread=_d(state["latent_spread"]),
        impact_ticks=_d(state["impact_ticks"]),
        model_version=profile.model_version,
        feature_snapshot_hash=str(state["feature_snapshot_hash"]),
        random_seed=order.random_seed,
        run_liquidity_ledger_id=ledger.ledger_id,
    )
    return _result(order, profile, (fill,), capacity_bounds=_capacity_bounds(state))


def _generative_mc(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
    *,
    backend: str,
) -> TradeOnlyOrderResult:
    pre = _pre_arrival(order, trades)
    if len(pre) < 2:
        return _empty(
            order, profile, "NO_FILL", "insufficient_pre_arrival_tape_for_monte_carlo"
        )
    paths = max(
        10, min(5_000, int(order.monte_carlo_paths or profile.monte_carlo_paths or 250))
    )
    window_seconds = max(1.0, order.lookback.total_seconds())
    hitting_side = (
        "SELL" if order.liquidity_intent == LiquidityIntent.PASSIVE else order.side
    )
    hitting = [row for row in pre if row.aggressor_side == hitting_side]
    intensity = max(0.0, len(hitting) / window_seconds)
    prices = [_d(row.price) for row in pre]
    last_price = prices[-1]
    distance_ticks = abs(float(order.limit_price - last_price) / float(TICK))
    intensity *= math.exp(-0.35 * max(0.0, distance_ticks))
    horizon_seconds = max(0.0, (order.deadline_ts - order.arrival_ts).total_seconds())
    sizes = [row.size for row in hitting] or [row.size for row in pre]
    sampler = (
        monte_carlo_fill_distribution_rust
        if backend == "rust"
        else monte_carlo_fill_distribution_python
    )
    distribution = sampler(
        poisson_mean=intensity * horizon_seconds,
        sizes=sizes,
        order_size=order.size,
        participation_rate=profile.participation_rate,
        paths=paths,
        seed=order.random_seed,
        require_full=order.tif == "FOK" or not order.allow_partial_fill,
    )
    expected = distribution.expected_fill
    fill_rate = distribution.fill_probability
    full_rate = distribution.full_fill_probability
    q10, q50, q90 = distribution.q10, distribution.q50, distribution.q90
    state = _arrival_state(
        order, trades, replace(profile, capacity_quantile=Decimal("0.50"))
    )
    exec_price = _d(state["exec_price"]) if state is not None else order.limit_price
    mc = {
        "paths": paths,
        "seed": order.random_seed,
        "arrival_model": "POISSON_EXPONENTIAL_DISTANCE_V2",
        "rng_algorithm": distribution.rng_algorithm,
        "rng_initial_state": distribution.initial_state,
        "rng_final_state": distribution.next_state,
        "intensity_per_second": str(
            Decimal(str(intensity)).quantize(Q, rounding=ROUND_HALF_UP)
        ),
        "fill_probability": str(fill_rate),
        "full_fill_probability": str(full_rate),
        "expected_fill_size": str(expected),
        "fill_size_q10": str(q10),
        "fill_size_q50": str(q50),
        "fill_size_q90": str(q90),
    }
    if expected <= 0 or not _limit_allows(order, exec_price):
        return _empty(
            order,
            profile,
            "MODELED_DISTRIBUTION",
            "monte_carlo_expected_fill_zero",
            monte_carlo=mc,
        )
    fill = TradeOnlyFill(
        order_id=order.order_id,
        execution_mode=profile.execution_mode.value,
        evidence_tier=profile.evidence_tier.value,
        trigger_type="MONTE_CARLO_EXPECTED_VALUE",
        filled_size=expected,
        exec_price=exec_price,
        fill_ts=order.arrival_ts,
        fill_block=None,
        capacity_q10=q10,
        capacity_q50=q50,
        capacity_q90=q90,
        latent_mid=_d(state["latent_mid"]) if state is not None else None,
        latent_spread=_d(state["latent_spread"]) if state is not None else None,
        impact_ticks=_d(state["impact_ticks"]) if state is not None else None,
        model_version=profile.model_version,
        feature_snapshot_hash=_feature_hash(order, pre),
        random_seed=order.random_seed,
        run_liquidity_ledger_id=ledger.ledger_id,
    )
    return _result(
        order, profile, (fill,), status="MODELED_DISTRIBUTION", monte_carlo=mc
    )


def _cached_probability_decision(
    order: TradeOnlyOrder,
    pre: tuple[V2TradePrint, ...],
    profile: OrderFilledProbabilityProfile,
    trades: _TradeReplayContext,
) -> OrderFilledProbabilityDecision:
    prepared = trades.prepared if trades.indexed else None
    feature_key = (
        order.market_id,
        order.asset_id.lower(),
        order.side,
        order.limit_price,
        order.size,
        order.arrival_block,
        order.arrival_ts,
        order.deadline_block,
        order.deadline_ts,
        order.signal_source_trade_id,
        order.market_slug,
        order.market_title,
        order.category,
        order.league,
        order.market_end_ts,
        profile.lookback_seconds,
        profile.tick_size,
    )
    features: OrderFilledProbabilityFeatures | None = None
    feature_cache: dict[tuple[Any, ...], OrderFilledProbabilityFeatures] | None = None
    if prepared is not None:
        feature_cache = prepared.backend_payloads.setdefault(
            "v3_probability_features_v1", {}
        )
        features = feature_cache.get(feature_key)
    if features is None:
        features = extract_orderfilled_probability_features(
            order,
            pre,
            lookback=timedelta(seconds=float(profile.lookback_seconds)),
            tick_size=profile.tick_size,
            presorted=True,
            same_market_asset=True,
        )
        if feature_cache is not None:
            if len(feature_cache) >= 50_000:
                feature_cache.clear()
            feature_cache[feature_key] = features

    profile_key = (
        profile.model_version,
        profile.capacity_variant,
        profile.capacity_mode,
        profile.min_probability,
        profile.hard_reject_below_probability,
        profile.hierarchical_probability_blend_strength,
        profile.hierarchical_probability_scale,
        profile.probability_target,
        profile.supported_sides,
        profile.supported_tifs,
        profile.supported_amount_units,
        profile.minimum_supported_order_size,
        profile.maximum_supported_order_size,
        profile.minimum_supported_log_order_to_tape_ratio,
        profile.maximum_supported_log_order_to_tape_ratio,
        id(profile.coefficients),
        id(profile.feature_means),
        id(profile.feature_scales),
        id(profile.conditional_capacity_models),
        id(profile.hierarchical_probability_cells),
    )
    decision_key = (profile_key, feature_key)
    decision_cache: dict[tuple[Any, ...], OrderFilledProbabilityDecision] | None = None
    if prepared is not None:
        decision_cache = prepared.backend_payloads.setdefault(
            "v3_probability_decisions_v1", {}
        )
        cached = decision_cache.get(decision_key)
        if cached is not None:
            return cached
    decision = OrderFilledProbabilityModel(profile).decide_from_features(
        order, features
    )
    if decision_cache is not None:
        if len(decision_cache) >= 100_000:
            decision_cache.clear()
        decision_cache[decision_key] = decision
    return decision


def _hierarchical_expected_fill(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
    *,
    confirmed_source: TradeOnlyOrderResult | None = None,
) -> TradeOnlyOrderResult:
    if order.liquidity_intent != LiquidityIntent.TAKER:
        return _empty(
            order,
            profile,
            "UNSUPPORTED",
            "hierarchical_expected_fill_requires_taker_intent",
        )
    immediate_tif = order.tif in {"IOC", "FAK", "FOK"}
    requires_full_fill = order.tif == "FOK" or not order.allow_partial_fill
    required_probability_target = (
        PROBABILITY_TARGET_FOK_FULL_FILL
        if requires_full_fill
        else PROBABILITY_TARGET_FAK_ANY_FILL
    )
    if immediate_tif and not profile.allow_immediate_tif_modeling:
        return _empty(
            order,
            profile,
            "UNSUPPORTED",
            "hierarchical_expected_fill_requires_gtc_or_gtd",
        )

    pre = _pre_arrival(order, trades)
    if len(pre) < profile.minimum_pre_arrival_trades and not profile.allow_prior_only:
        return _empty(
            order,
            profile,
            "NO_FILL",
            "insufficient_pre_arrival_tape_for_hierarchical_expected_fill",
            model_diagnostics={
                "minimum_pre_arrival_trades": profile.minimum_pre_arrival_trades,
                "local_pre_arrival_trade_count": len(pre),
                "prior_sampling_contract": "TRADE_ANCHORED_CALIBRATION",
                "expected_fill_is_observed_execution": False,
            },
        )
    execution_horizon_seconds = max(
        1, round((order.deadline_ts - order.arrival_ts).total_seconds())
    )
    model_horizon_seconds = max(
        1,
        int(profile.probability_model_horizon_seconds or execution_horizon_seconds),
    )
    model_order = order
    if model_horizon_seconds != execution_horizon_seconds:
        model_order = replace(
            order,
            tif="GTD",
            horizon=timedelta(seconds=model_horizon_seconds),
        )
    use_full_fill_profile = bool(
        requires_full_fill and profile.full_fill_probability_profile_path
    )
    probability_profile_path = (
        profile.full_fill_probability_profile_path
        if use_full_fill_profile
        else profile.probability_profile_path
    )
    probability_profile_sha256 = (
        profile.full_fill_probability_profile_sha256
        if use_full_fill_profile
        else profile.probability_profile_sha256
    )
    probability_profile = _probability_profile(
        probability_profile_path,
        expected_sha256=probability_profile_sha256,
    )
    evaluate_probability = bool(pre) or profile.evaluate_probability_on_empty_tape
    expected_decision = (
        _cached_probability_decision(
            model_order,
            pre,
            probability_profile,
            trades,
        )
        if evaluate_probability
        else None
    )
    conservative_profile = replace(probability_profile, capacity_variant="conservative")
    conservative_decision = (
        _cached_probability_decision(
            model_order,
            pre,
            conservative_profile,
            trades,
        )
        if evaluate_probability
        else None
    )
    contract_violations = (
        probability_profile_domain_violations(
            probability_profile,
            model_order,
            expected_decision.features,
            required_target=required_probability_target,
        )
        if expected_decision is not None
        else ()
    )
    if profile.enforce_probability_contract and contract_violations:
        return _empty(
            order,
            profile,
            "MODEL_DOMAIN_ABSTAIN",
            "probability_model_contract_violation",
            model_diagnostics={
                "required_probability_target": required_probability_target,
                "artifact_probability_target": probability_profile.probability_target,
                "contract_violations": list(contract_violations),
                "probability_model_version": probability_profile.model_version,
                "probability_artifact_sha256": probability_profile_sha256,
                "runtime_lob_usage": "NONE",
                "expected_fill_is_observed_execution": False,
            },
        )
    if (
        profile.direct_probability_model
        and expected_decision is not None
        and not expected_decision.domain_supported
    ):
        return _empty(
            order,
            profile,
            "MODELED_EXPECTATION",
            "orderfilled_probability_model_out_of_domain",
            model_diagnostics={
                "probability_model_version": probability_profile.model_version,
                "probability_artifact_sha256": probability_profile_sha256,
                "probability_activation": probability_profile.activation,
                "abstain_categories": list(probability_profile.abstain_categories),
                "abstain_category_families": list(
                    probability_profile.abstain_category_families
                ),
                "runtime_lob_usage": "NONE",
                "expected_fill_is_observed_execution": False,
            },
        )

    context = context_for_order(order, pre)
    prior_horizon_seconds = model_horizon_seconds
    prior_cell: dict[str, Any] = {}
    if profile.hierarchical_prior_path:
        try:
            prior_cell = resolve_hierarchical_prior(
                load_hierarchical_prior_profile(profile.hierarchical_prior_path),
                horizon_seconds=prior_horizon_seconds,
                context=context,
                minimum_samples=profile.minimum_prior_samples,
            )
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            prior_cell = {}
    if (
        len(pre) < profile.minimum_pre_arrival_trades
        and profile.allow_prior_only
        and not prior_cell
    ):
        return _empty(
            order,
            profile,
            "NO_FILL",
            "insufficient_supported_prior_for_hierarchical_expected_fill",
            model_diagnostics={
                "minimum_pre_arrival_trades": profile.minimum_pre_arrival_trades,
                "local_pre_arrival_trade_count": len(pre),
                "minimum_prior_samples": profile.minimum_prior_samples,
                "prior_sampling_contract": "TRADE_ANCHORED_CALIBRATION",
                "expected_fill_is_observed_execution": False,
            },
        )
    strength = max(1, profile.probability_prior_strength)
    local_weight = (
        Decimal(1)
        if profile.direct_probability_model and expected_decision is not None
        else min(Decimal(1), Decimal(len(pre)) / Decimal(strength))
        if expected_decision is not None
        else Decimal(0)
    )
    prior_weight = Decimal(1) - local_weight
    prior_p_calibration_horizon = _bounded(
        _d(prior_cell.get("p_fill", profile.probability_prior_30s or Decimal(0)))
    )
    local_p_model_horizon = _bounded(
        expected_decision.p_fill if expected_decision is not None else Decimal(0)
    )
    if execution_horizon_seconds != model_horizon_seconds:
        local_p_execution = _scale_probability(
            local_p_model_horizon,
            execution_horizon_seconds,
            base_seconds=model_horizon_seconds,
        )
    else:
        local_p_execution = local_p_model_horizon
    if prior_horizon_seconds == execution_horizon_seconds:
        prior_p_execution = prior_p_calibration_horizon
    else:
        prior_p_execution = _scale_probability(
            prior_p_calibration_horizon,
            execution_horizon_seconds,
            base_seconds=prior_horizon_seconds,
        )
    if prior_horizon_seconds == model_horizon_seconds:
        prior_p_model_horizon = prior_p_calibration_horizon
    else:
        prior_p_model_horizon = _scale_probability(
            prior_p_calibration_horizon,
            model_horizon_seconds,
            base_seconds=prior_horizon_seconds,
        )
    p_any_fill = _bounded(
        local_weight * local_p_execution + prior_weight * prior_p_execution
    )
    p_model_horizon = _bounded(
        local_weight * local_p_model_horizon + prior_weight * prior_p_model_horizon
    )
    source_lower_size = (
        min(order.size, confirmed_source.filled_size)
        if confirmed_source is not None
        else Decimal(0)
    )
    source_confirms_any_fill = source_lower_size > 0
    if source_confirms_any_fill:
        p_any_fill = Decimal(1)

    prior_fraction = _bounded(
        _d(
            prior_cell.get(
                "conditional_fill_fraction",
                profile.conditional_capacity_prior or Decimal("0.50"),
            )
        )
    )
    expected_fraction = _bounded(
        local_weight
        * (
            expected_decision.conditional_capacity_fraction
            if expected_decision is not None
            else Decimal(0)
        )
        + prior_weight * prior_fraction
    )
    conservative_fraction = _bounded(
        local_weight
        * (
            conservative_decision.conditional_capacity_fraction
            if conservative_decision is not None
            else Decimal(0)
        )
        + prior_weight * min(prior_fraction, Decimal("0.6381675966"))
    )
    if requires_full_fill:
        direct_full_fill_target = (
            probability_profile.probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
        )
        p_fill = (
            p_any_fill
            if direct_full_fill_target
            else _bounded(p_any_fill * expected_fraction)
        )
        conservative_p_fill = (
            p_any_fill
            if direct_full_fill_target
            else _bounded(p_any_fill * conservative_fraction)
        )
        reported_conditional_fraction = Decimal(1)
        conditional_size = order.size.quantize(Q, rounding=ROUND_HALF_UP)
        expected_size = (p_fill * order.size).quantize(Q, rounding=ROUND_HALF_UP)
        conservative_expected_size = (conservative_p_fill * order.size).quantize(
            Q, rounding=ROUND_HALF_UP
        )
    else:
        p_fill = p_any_fill
        conservative_p_fill = p_any_fill
        reported_conditional_fraction = expected_fraction
        conditional_size = (order.size * expected_fraction).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        expected_size = (p_fill * conditional_size).quantize(Q, rounding=ROUND_HALF_UP)
        conservative_expected_size = (
            conservative_p_fill * order.size * conservative_fraction
        ).quantize(Q, rounding=ROUND_HALF_UP)

    available_pool = ledger.synthetic_remaining(order, conditional_size)
    modeled_total = min(order.size, expected_size, available_pool).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    quantity = max(source_lower_size, modeled_total).quantize(Q, rounding=ROUND_HALF_UP)
    modeled_residual = max(Decimal(0), quantity - source_lower_size).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    probabilities = {
        "model_horizon": local_p_execution.quantize(Q, rounding=ROUND_HALF_UP),
        "hierarchical_prior_horizon": prior_p_execution.quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        "blended_horizon": p_fill.quantize(Q, rounding=ROUND_HALF_UP),
        "model_target_horizon": local_p_model_horizon.quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        "hierarchical_prior_target_horizon": prior_p_model_horizon.quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        "blended_target_horizon": p_model_horizon.quantize(Q, rounding=ROUND_HALF_UP),
        "any_fill_execution_horizon": p_any_fill.quantize(Q, rounding=ROUND_HALF_UP),
        "horizon": p_fill.quantize(Q, rounding=ROUND_HALF_UP),
    }
    if requires_full_fill:
        key = (
            "full_fill_probability"
            if probability_profile.probability_target
            == PROBABILITY_TARGET_FOK_FULL_FILL
            else "full_fill_proxy"
        )
        probabilities[key] = p_fill.quantize(Q, rounding=ROUND_HALF_UP)
    curve_probability = (
        p_model_horizon
        if requires_full_fill
        and probability_profile.probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
        else _bounded(p_model_horizon * expected_fraction)
        if requires_full_fill
        else p_model_horizon
    )
    capacities = {
        "conditional_conservative": (order.size * conservative_fraction).quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        "conditional_expected": conditional_size,
        "unconditional_conservative": conservative_expected_size,
        "unconditional_expected": expected_size,
    }
    admission_threshold = (
        probability_profile.min_probability
        if profile.use_probability_profile_threshold
        else profile.minimum_modeled_probability
    )
    diagnostics = {
        "probability_contract": (
            "DIRECT_P_FULL_FILL"
            if requires_full_fill
            and probability_profile.probability_target
            == PROBABILITY_TARGET_FOK_FULL_FILL
            else "P_ANY_FILL_X_CONDITIONAL_FULL_CAPACITY_PROXY"
            if requires_full_fill
            else "P_FILL_BY_HORIZON_X_CONDITIONAL_FILL_FRACTION"
        ),
        "required_probability_target": required_probability_target,
        "artifact_probability_target": probability_profile.probability_target,
        "probability_contract_enforced": profile.enforce_probability_contract,
        "probability_contract_violations": list(contract_violations),
        "price_rule": "ORDER_LIMIT_WORST_PRICE",
        "source_trade_required": False,
        "source_trade_ids_are_empty": not source_confirms_any_fill,
        "local_pre_arrival_trade_count": len(pre),
        "local_weight": str(local_weight.quantize(Q, rounding=ROUND_HALF_UP)),
        "prior_weight": str(prior_weight.quantize(Q, rounding=ROUND_HALF_UP)),
        "probability_model_version": probability_profile.model_version,
        "probability_artifact_sha256": probability_profile_sha256,
        "probability_training_rows": probability_profile.training_rows,
        "probability_activation": probability_profile.activation,
        "base_orderfilled_probability": (
            str(expected_decision.base_probability)
            if expected_decision is not None
            else None
        ),
        "hierarchical_probability": (
            str(expected_decision.hierarchical_probability)
            if expected_decision is not None
            and expected_decision.hierarchical_probability is not None
            else None
        ),
        "hierarchical_probability_cell": (
            expected_decision.hierarchical_probability_cell
            if expected_decision is not None
            else None
        ),
        "hierarchical_probability_samples": (
            expected_decision.hierarchical_probability_samples
            if expected_decision is not None
            else 0
        ),
        "prior_sampling_contract": "TRADE_ANCHORED_CALIBRATION",
        "prior_only_used": expected_decision is None,
        "minimum_prior_samples": profile.minimum_prior_samples,
        "hierarchical_prior_horizon_seconds": str(prior_horizon_seconds),
        "base_probability_horizon_seconds": str(model_horizon_seconds),
        "requested_horizon_seconds": str(execution_horizon_seconds),
        "independently_trained_horizon": (
            model_horizon_seconds == execution_horizon_seconds
        ),
        "time_scaling": (
            "CONSTANT_HAZARD_FROM_TRAINED_HORIZON"
            if model_horizon_seconds != execution_horizon_seconds
            else "NONE"
        ),
        "tif": order.tif,
        "tif_execution_contract": (
            "ATOMIC_ZERO_OR_FULL_EXPECTATION"
            if requires_full_fill
            else "PARTIAL_EXPECTATION_ALLOWED"
        ),
        "direction_evidence": (
            {
                "same_flow_share": str(
                    expected_decision.features.vector["same_flow_share"]
                ),
                "same_trade_count": expected_decision.features.trailing_same_count,
                "opposite_trade_count": expected_decision.features.trailing_opposite_count,
                "source": "PRE_ARRIVAL_ORDERFILLED_FEATURES",
            }
            if expected_decision is not None
            else {
                "source": "HIERARCHICAL_PRIOR_ONLY",
                "side_specific": "side"
                in str(prior_cell.get("level") or "").split("+"),
            }
        ),
        "hierarchical_prior_level": prior_cell.get("level", "profile_fallback"),
        "hierarchical_prior_cell_key": prior_cell.get("cell_key", "fallback"),
        "hierarchical_prior_samples": int(prior_cell.get("samples") or 0),
        "hierarchical_context": context.as_dict(),
        "expected_fill_is_observed_execution": False,
        "source_confirmed_lower_bound_size": str(
            source_lower_size.quantize(Q, rounding=ROUND_HALF_UP)
        ),
        "modeled_residual_size": str(modeled_residual),
        "source_residual_contract": (
            "MAX_SOURCE_LOWER_BOUND_AND_CONDITIONAL_EXPECTED_TOTAL"
            if source_confirms_any_fill
            else "NO_SOURCE_LOWER_BOUND"
        ),
        "minimum_modeled_probability": str(admission_threshold),
        "probability_threshold_source": (
            "PROBABILITY_ARTIFACT"
            if profile.use_probability_profile_threshold
            else "TRADE_ONLY_PROFILE"
        ),
        "probability_blend_contract": (
            "DIRECT_ORDERFILLED_MODEL"
            if profile.direct_probability_model
            else "LOCAL_HIERARCHICAL_SHRINKAGE"
        ),
        "probability_admission_passed": (
            source_confirms_any_fill or p_model_horizon >= admission_threshold
        ),
    }
    if not source_confirms_any_fill and p_model_horizon < admission_threshold:
        return _empty(
            order,
            profile,
            "MODELED_EXPECTATION",
            "modeled_probability_below_profile_threshold",
            probability_bounds=probabilities,
            capacity_bounds=capacities,
            model_diagnostics=diagnostics,
        )
    if quantity <= 0:
        return _empty(
            order,
            profile,
            "MODELED_EXPECTATION",
            "hierarchical_expected_fill_zero",
            probability_bounds=probabilities,
            capacity_bounds=capacities,
            model_diagnostics=diagnostics,
        )

    if confirmed_source is not None and modeled_residual <= 0:
        return confirmed_source
    ledger.consume_synthetic(
        order,
        modeled_total if source_confirms_any_fill else quantity,
    )
    fill = TradeOnlyFill(
        order_id=order.order_id,
        execution_mode=profile.execution_mode.value,
        evidence_tier=profile.evidence_tier.value,
        trigger_type=(
            "MODELED_EXPECTED_IMMEDIATE_FILL"
            if immediate_tif
            else "MODELED_EXPECTED_FILL_BY_HORIZON"
        ),
        filled_size=modeled_residual if source_confirms_any_fill else quantity,
        exec_price=order.limit_price.quantize(Q, rounding=ROUND_HALF_UP),
        fill_ts=order.arrival_ts if immediate_tif else order.deadline_ts,
        fill_block=None,
        source_trade_ids=(),
        source_tx_hashes=(),
        source_log_indexes=(),
        fill_time_lower=order.arrival_ts,
        fill_time_upper=order.deadline_ts,
        p_fill_1s=_scale_probability(
            curve_probability, 1, base_seconds=model_horizon_seconds
        ).quantize(Q, rounding=ROUND_HALF_UP),
        p_fill_5s=_scale_probability(
            curve_probability, 5, base_seconds=model_horizon_seconds
        ).quantize(Q, rounding=ROUND_HALF_UP),
        p_fill_30s=_scale_probability(
            curve_probability, 30, base_seconds=model_horizon_seconds
        ).quantize(Q, rounding=ROUND_HALF_UP),
        p_fill_horizon=p_fill.quantize(Q, rounding=ROUND_HALF_UP),
        conditional_fill_fraction=reported_conditional_fraction.quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        conditional_fill_size=conditional_size,
        unconditional_expected_fill_size=(
            modeled_residual if source_confirms_any_fill else expected_size
        ),
        participation_rate=profile.participation_rate,
        model_version=profile.model_version,
        feature_snapshot_hash=_feature_hash(order, pre),
        random_seed=order.random_seed,
        run_liquidity_ledger_id=ledger.ledger_id,
    )
    fills = (
        (*confirmed_source.fills, fill)
        if source_confirms_any_fill and confirmed_source is not None
        else (fill,)
    )
    result = _result(
        order,
        profile,
        fills,
        status="MODELED_EXPECTATION",
        reason=(
            "source_confirmed_lower_bound_plus_modeled_residual_expectation"
            if source_confirms_any_fill
            else "model_inferred_expected_fill_not_observed_execution"
        ),
        probability_bounds=probabilities,
        capacity_bounds=capacities,
        model_diagnostics=diagnostics,
    )
    if source_confirms_any_fill:
        result = replace(
            result,
            evidence_tier="MIXED_A_SOURCE_CONFIRMED_D_MODELED_EXPECTATION",
        )
    return result


def _central_router(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
) -> TradeOnlyOrderResult:
    if order.liquidity_intent == LiquidityIntent.AUTO_BOUND:
        result = _auto_bound(
            order, trades, get_trade_only_profile("auto_bound"), ledger
        )
        return _router_result(result, "auto_bound", None)
    if order.liquidity_intent == LiquidityIntent.PASSIVE:
        lower = _passive_trade_through(
            order, trades, get_trade_only_profile("maker_trade_through_lower"), ledger
        )
        if lower.filled_size > 0:
            return _router_result(lower, "passive_trade_through", None)
        expected = _passive_touch_survival(
            order,
            trades,
            get_trade_only_profile("maker_touch_survival_expected"),
            ledger,
        )
        return _router_result(expected, "passive_touch_survival", lower)

    pre = _pre_arrival(order, trades)
    context = context_for_order(order, pre)
    buffer_resolution = resolve_price_buffer(
        profile.price_buffer_profile_path,
        context=context,
        fallback=profile.price_buffer,
    )
    source_profile = replace(
        get_trade_only_profile("taker_source_confirmed"),
        horizon=order.horizon,
        default_horizon_blocks=order.horizon_blocks,
        price_buffer=_d(buffer_resolution["buffer"]),
    )
    source = _source_confirmed(order, trades, source_profile, ledger)
    return _central_router_after_source(
        order,
        trades,
        profile,
        ledger,
        source,
        buffer_resolution,
    )


def _central_router_after_source(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
    source: TradeOnlyOrderResult,
    buffer_resolution: dict[str, Any],
) -> TradeOnlyOrderResult:
    if source.filled_size > 0:
        if (
            profile.augment_source_with_modeled_residual
            and source.filled_size < order.size
        ):
            expected_profile = replace(
                profile,
                execution_mode=ExecutionMode.HIERARCHICAL_EXPECTED_FILL,
                evidence_tier=get_trade_only_profile(
                    "taker_hierarchical_expected_30s"
                ).evidence_tier,
            )
            expected = _hierarchical_expected_fill(
                order,
                trades,
                expected_profile,
                ledger,
                confirmed_source=source,
            )
            if expected.filled_size > source.filled_size:
                return _router_result(
                    expected,
                    "taker_source_confirmed_plus_modeled_residual",
                    source,
                    price_buffer=buffer_resolution,
                )
        return _router_result(
            source,
            "taker_source_confirmed",
            None,
            price_buffer=buffer_resolution,
        )
    expected_profile = replace(
        profile,
        execution_mode=ExecutionMode.HIERARCHICAL_EXPECTED_FILL,
        evidence_tier=get_trade_only_profile(
            "taker_hierarchical_expected_30s"
        ).evidence_tier,
    )
    expected = _hierarchical_expected_fill(order, trades, expected_profile, ledger)
    return _router_result(
        expected,
        "taker_hierarchical_expected",
        source,
        price_buffer=buffer_resolution,
    )


def _replay_central_taker_batch(
    orders: list[TradeOnlyOrder],
    prepared: PreparedTradeTape[V2TradePrint],
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
    *,
    backend: Literal["auto", "python", "rust"],
    index_build_sec: Decimal,
    index_reused: bool,
) -> tuple[list[TradeOnlyOrderResult], RunLiquidityLedger, V3ReplayDiagnostics]:
    context = _TradeReplayContext((), prepared=prepared, indexed=True)
    started = perf_counter()
    buffer_resolutions: list[dict[str, Any]] = []
    source_profiles: list[TradeOnlyProfile] = []
    for order in orders:
        pre = _pre_arrival(order, context)
        buffer_resolution = resolve_price_buffer(
            profile.price_buffer_profile_path,
            context=context_for_order(order, pre),
            fallback=profile.price_buffer,
        )
        buffer_resolutions.append(buffer_resolution)
        source_profiles.append(
            replace(
                get_trade_only_profile("taker_source_confirmed"),
                horizon=order.horizon,
                default_horizon_blocks=order.horizon_blocks,
                price_buffer=_d(buffer_resolution["buffer"]),
            )
        )
    v2_orders = [
        _source_confirmed_v2_order(order, source_profile)
        for order, source_profile in zip(orders, source_profiles, strict=True)
    ]
    v2_results, _, v2_diagnostics = replay_v2_taker_orders_with_diagnostics(
        v2_orders,
        ledger=ledger.v2,
        prepared_tape=prepared,
        backend=backend,
    )
    source_results = [
        _source_confirmed_result(order, source_profile, ledger, result)
        for order, source_profile, result in zip(
            orders, source_profiles, v2_results, strict=True
        )
    ]
    results = [
        _central_router_after_source(
            order,
            context,
            profile,
            ledger,
            source,
            buffer_resolution,
        )
        for order, source, buffer_resolution in zip(
            orders, source_results, buffer_resolutions, strict=True
        )
    ]
    matching_sec = Decimal(str(perf_counter() - started)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    naive_rows = len(orders) * prepared.trade_rows_indexed
    scanned = v2_diagnostics.candidate_rows_scanned + context.candidate_rows_scanned
    return (
        results,
        ledger,
        V3ReplayDiagnostics(
            orders_count=len(orders),
            trade_rows_indexed=prepared.trade_rows_indexed,
            trade_groups=prepared.trade_groups,
            index_build_sec=index_build_sec,
            matching_sec=matching_sec,
            candidate_rows_scanned=scanned,
            naive_rows_scanned=naive_rows,
            scan_reduction_ratio=(Decimal(scanned) / Decimal(naive_rows)).quantize(
                Q, rounding=ROUND_HALF_UP
            )
            if naive_rows
            else Decimal(0),
            window_queries=len(orders) + context.window_queries,
            index_reused=index_reused,
            matching_backend=(
                "hybrid_rust_python"
                if v2_diagnostics.matching_backend == "rust"
                else "python"
            ),
            backend_fallback_reason=v2_diagnostics.backend_fallback_reason,
        ),
    )


def _router_result(
    result: TradeOnlyOrderResult,
    route: str,
    prior_attempt: TradeOnlyOrderResult | None,
    *,
    price_buffer: dict[str, Any] | None = None,
) -> TradeOnlyOrderResult:
    diagnostics = dict(result.model_diagnostics)
    diagnostics.update(
        {
            "central_router_version": "trade_only_central_router_v1",
            "selected_route": route,
            "prior_attempt_status": prior_attempt.status if prior_attempt else None,
            "prior_attempt_reason": prior_attempt.reason if prior_attempt else None,
        }
    )
    if price_buffer is not None:
        diagnostics["price_buffer_resolution"] = price_buffer
    return replace(result, model_diagnostics=diagnostics)


def _auto_bound(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
) -> TradeOnlyOrderResult:
    if order.liquidity_intent == LiquidityIntent.PASSIVE:
        lower_profile = get_trade_only_profile("maker_trade_through_lower")
        expected_profile = get_trade_only_profile("maker_touch_survival_expected")
    else:
        lower_profile = get_trade_only_profile("taker_source_confirmed")
        expected_profile = get_trade_only_profile("taker_synthetic_q50")
    upper_profile = get_trade_only_profile("taker_synthetic_q90")
    lower = _replay_one(
        order, trades, lower_profile, RunLiquidityLedger(f"{ledger.ledger_id}:lower")
    )
    expected = _replay_one(
        order,
        trades,
        expected_profile,
        RunLiquidityLedger(f"{ledger.ledger_id}:expected"),
    )
    upper_order = replace(order, liquidity_intent=LiquidityIntent.TAKER)
    upper = _replay_one(
        upper_order,
        trades,
        upper_profile,
        RunLiquidityLedger(f"{ledger.ledger_id}:upper"),
    )
    scenarios = {
        "strict_lower": lower.as_dict(),
        "expected_modeled": expected.as_dict(),
        "synthetic_upper": upper.as_dict(),
    }
    ordering_valid = lower.filled_size <= expected.filled_size <= upper.filled_size
    scenarios["bound_diagnostics"] = {
        "ordering_valid": ordering_valid,
        "lower_filled_size": str(lower.filled_size),
        "expected_filled_size": str(expected.filled_size),
        "upper_filled_size": str(upper.filled_size),
    }
    return TradeOnlyOrderResult(
        order_id=order.order_id,
        status="BOUND_SET" if ordering_valid else "BOUND_ORDERING_VIOLATION",
        requested_size=order.size,
        filled_size=Decimal(0),
        unfilled_size=order.size,
        avg_price=Decimal(0),
        reason=(
            "scenario_results_must_be_reported_separately"
            if ordering_valid
            else "uncalibrated_scenarios_do_not_form_ordered_bounds"
        ),
        execution_mode=profile.execution_mode.value,
        evidence_tier="MIXED_EVIDENCE",
        result_role=profile.result_role,
        calibration_status=profile.calibration_status,
        scenarios=scenarios,
    )


def _source_inferred_fill(
    order: TradeOnlyOrder,
    profile: TradeOnlyProfile,
    ledger: RunLiquidityLedger,
    trade: V2TradePrint,
    quantity: Decimal,
    trigger: str,
) -> TradeOnlyFill:
    return TradeOnlyFill(
        order_id=order.order_id,
        execution_mode=profile.execution_mode.value,
        evidence_tier=profile.evidence_tier.value,
        trigger_type=trigger,
        filled_size=quantity,
        exec_price=order.limit_price.quantize(Q, rounding=ROUND_HALF_UP),
        fill_ts=trade.block_time,
        fill_block=trade.block_number,
        source_trade_ids=(trade.trade_id,),
        source_tx_hashes=(trade.tx_hash,),
        source_log_indexes=trade.source_log_indexes,
        cross_ts=trade.block_time,
        fill_time_lower=trade.block_time,
        fill_time_upper=trade.block_time,
        model_version=profile.model_version,
        feature_snapshot_hash=_feature_hash(order, []),
        random_seed=order.random_seed,
        run_liquidity_ledger_id=ledger.ledger_id,
    )


def _passive_trigger(order: TradeOnlyOrder, trade: V2TradePrint) -> str | None:
    if order.side == "BUY":
        if trade.aggressor_side != "SELL":
            return None
        if trade.price < order.limit_price:
            return "STRICT_CROSS"
        if trade.price == order.limit_price:
            return "TOUCH"
        return None
    if trade.aggressor_side != "BUY":
        return None
    if trade.price > order.limit_price:
        return "STRICT_CROSS"
    if trade.price == order.limit_price:
        return "TOUCH"
    return None


def _future(
    order: TradeOnlyOrder, trades: _TradeReplayContext
) -> tuple[V2TradePrint, ...]:
    return trades.future(order)


def _pre_arrival(
    order: TradeOnlyOrder, trades: _TradeReplayContext
) -> tuple[V2TradePrint, ...]:
    return trades.pre_arrival(order)


def _touch_probability_bounds(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    first_touch: V2TradePrint,
    first_cross: V2TradePrint | None,
) -> dict[str, Decimal]:
    pre = _pre_arrival(order, trades)
    hitting_side = "SELL" if order.side == "BUY" else "BUY"
    hit_count = sum(1 for row in pre if row.aggressor_side == hitting_side)
    rate = Decimal(hit_count) / Decimal(str(max(1.0, order.lookback.total_seconds())))
    touch_size = max(Decimal("0.0000000001"), _d(first_touch.size))
    queue_ratio = order.size / touch_size
    queue_discount = Decimal(str(math.exp(-min(20.0, float(queue_ratio)))))
    hazard = max(Decimal("0.000001"), rate * (Decimal("0.5") + queue_discount))

    def cumulative(seconds: float) -> Decimal:
        return _bounded(
            Decimal(str(1.0 - math.exp(-float(hazard) * max(0.0, seconds))))
        )

    horizon_seconds = max(
        0.0, (order.deadline_ts - first_touch.block_time).total_seconds()
    )
    p_expected = cumulative(horizon_seconds)
    lower = Decimal(1) if first_cross is not None else Decimal(0)
    upper = Decimal(1)
    return {
        "lower": lower.quantize(Q, rounding=ROUND_HALF_UP),
        "expected": p_expected.quantize(Q, rounding=ROUND_HALF_UP),
        "upper": upper.quantize(Q, rounding=ROUND_HALF_UP),
        "p_1s": cumulative(1).quantize(Q, rounding=ROUND_HALF_UP),
        "p_5s": cumulative(5).quantize(Q, rounding=ROUND_HALF_UP),
        "p_30s": cumulative(30).quantize(Q, rounding=ROUND_HALF_UP),
    }


def _arrival_state(
    order: TradeOnlyOrder,
    trades: _TradeReplayContext,
    profile: TradeOnlyProfile,
) -> dict[str, Decimal | str] | None:
    pre = _pre_arrival(order, trades)
    if len(pre) < 2:
        return None
    buy = [row for row in pre if row.aggressor_side == "BUY"]
    sell = [row for row in pre if row.aggressor_side == "SELL"]
    prices = [_d(row.price) for row in pre]
    ewma = prices[0]
    for price in prices[1:]:
        ewma = Decimal("0.35") * price + Decimal("0.65") * ewma
    roll_spread = _roll_spread(prices)
    if buy and sell:
        buy_price = _d(buy[-1].price)
        sell_price = _d(sell[-1].price)
        latent_mid = (buy_price + sell_price) / Decimal(2)
        latent_spread = max(TICK, abs(buy_price - sell_price))
    else:
        latent_mid = ewma
        latent_spread = max(TICK, roll_spread)
    sizes = sorted(_d(row.size) for row in pre)
    median_size = sizes[len(sizes) // 2]
    total_volume = sum(sizes, Decimal(0))
    rate_capacity = total_volume / Decimal(
        str(max(1.0, order.lookback.total_seconds()))
    )
    base = max(rate_capacity, median_size) * profile.participation_rate
    q10 = (base * Decimal("0.5")).quantize(Q, rounding=ROUND_HALF_UP)
    q50 = base.quantize(Q, rounding=ROUND_HALF_UP)
    q90 = (base * Decimal(2)).quantize(Q, rounding=ROUND_HALF_UP)
    selected = q50
    if profile.capacity_quantile is not None and profile.capacity_quantile <= Decimal(
        "0.10"
    ):
        selected = q10
    elif profile.capacity_quantile is not None and profile.capacity_quantile >= Decimal(
        "0.90"
    ):
        selected = q90
    volume_share = selected / max(Decimal("0.0000000001"), total_volume)
    impact_ticks = max(Decimal(1), Decimal(100) * volume_share * volume_share)
    half_spread = latent_spread / Decimal(2)
    if order.side == "BUY":
        exec_price = latent_mid + half_spread + impact_ticks * TICK
    else:
        exec_price = latent_mid - half_spread - impact_ticks * TICK
    exec_price = min(Decimal(1), max(Decimal(0), exec_price)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    return {
        "latent_mid": latent_mid.quantize(Q, rounding=ROUND_HALF_UP),
        "latent_spread": latent_spread.quantize(Q, rounding=ROUND_HALF_UP),
        "capacity_q10": q10,
        "capacity_q50": q50,
        "capacity_q90": q90,
        "impact_ticks": impact_ticks.quantize(Q, rounding=ROUND_HALF_UP),
        "exec_price": exec_price,
        "feature_snapshot_hash": _feature_hash(order, pre),
    }


def _roll_spread(prices: list[Decimal]) -> Decimal:
    if len(prices) < 3:
        return TICK
    changes = [float(right - left) for left, right in pairwise(prices)]
    if len(changes) < 2:
        return TICK
    left = changes[:-1]
    right = changes[1:]
    mean_left = statistics.fmean(left)
    mean_right = statistics.fmean(right)
    covariance = sum(
        (a - mean_left) * (b - mean_right) for a, b in zip(left, right)
    ) / len(left)
    return Decimal(str(2.0 * math.sqrt(max(0.0, -covariance))))


def _capacity_bounds(state: dict[str, Decimal | str]) -> dict[str, Decimal]:
    return {
        "q10": _d(state["capacity_q10"]),
        "q50": _d(state["capacity_q50"]),
        "q90": _d(state["capacity_q90"]),
    }


def _result(
    order: TradeOnlyOrder,
    profile: TradeOnlyProfile,
    fills: tuple[TradeOnlyFill, ...],
    *,
    reason: str = "",
    status: str | None = None,
    probability_bounds: dict[str, Decimal] | None = None,
    capacity_bounds: dict[str, Decimal] | None = None,
    monte_carlo: dict[str, Any] | None = None,
    model_diagnostics: dict[str, Any] | None = None,
) -> TradeOnlyOrderResult:
    filled = sum((fill.filled_size for fill in fills), Decimal(0)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    notional = sum((fill.filled_size * fill.exec_price for fill in fills), Decimal(0))
    average = (
        (notional / filled).quantize(Q, rounding=ROUND_HALF_UP)
        if filled
        else Decimal(0)
    )
    if status is None:
        status = (
            "FILLED"
            if filled >= order.size
            else "PARTIAL_FILLED"
            if filled > 0
            else "NO_FILL"
        )
    if status == "FILLED":
        reason = ""
    return TradeOnlyOrderResult(
        order_id=order.order_id,
        status=status,
        requested_size=order.size.quantize(Q, rounding=ROUND_HALF_UP),
        filled_size=filled,
        unfilled_size=max(Decimal(0), order.size - filled).quantize(
            Q, rounding=ROUND_HALF_UP
        ),
        avg_price=average,
        reason=reason,
        execution_mode=profile.execution_mode.value,
        evidence_tier=profile.evidence_tier.value,
        result_role=profile.result_role,
        calibration_status=profile.calibration_status,
        fills=fills,
        probability_bounds=probability_bounds or {},
        capacity_bounds=capacity_bounds or {},
        monte_carlo=monte_carlo,
        model_diagnostics=model_diagnostics or {},
    )


def _empty(
    order: TradeOnlyOrder,
    profile: TradeOnlyProfile,
    status: str,
    reason: str,
    *,
    probability_bounds: dict[str, Decimal] | None = None,
    capacity_bounds: dict[str, Decimal] | None = None,
    monte_carlo: dict[str, Any] | None = None,
    model_diagnostics: dict[str, Any] | None = None,
) -> TradeOnlyOrderResult:
    return _result(
        order,
        profile,
        (),
        reason=reason,
        status=status,
        probability_bounds=probability_bounds,
        capacity_bounds=capacity_bounds,
        monte_carlo=monte_carlo,
        model_diagnostics=model_diagnostics,
    )


def _same(order: TradeOnlyOrder, trade: V2TradePrint) -> bool:
    return (
        order.market_id == trade.market_id
        and order.asset_id.lower() == trade.asset_id.lower()
    )


def _limit_allows(order: TradeOnlyOrder, price: Decimal) -> bool:
    return (
        price <= order.limit_price
        if order.side == "BUY"
        else price >= order.limit_price
    )


def _feature_hash(order: TradeOnlyOrder, trades: Iterable[V2TradePrint]) -> str:
    payload = {
        "order": {
            "market_id": order.market_id,
            "asset_id": order.asset_id.lower(),
            "side": order.side,
            "limit_price": str(order.limit_price),
            "size": str(order.size),
            "arrival_block": order.arrival_block,
            "arrival_ts": order.arrival_ts.isoformat(),
        },
        "trades": [
            [
                row.trade_id,
                row.block_number,
                row.block_time.isoformat(),
                str(row.price),
                str(row.size),
                row.aggressor_side,
            ]
            for row in trades
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


@lru_cache(maxsize=8)
def _probability_profile_cached(
    resolved_path: str,
    mtime_ns: int,
    expected_sha256: str | None,
) -> OrderFilledProbabilityProfile:
    del mtime_ns
    if expected_sha256 is not None:
        actual_sha256 = hashlib.sha256(Path(resolved_path).read_bytes()).hexdigest()
        if actual_sha256 != expected_sha256:
            raise ValueError(
                "probability artifact checksum mismatch: "
                f"expected {expected_sha256}, got {actual_sha256}"
            )
    return load_orderfilled_probability_profile(resolved_path)


def _probability_profile(
    path: str | None,
    *,
    expected_sha256: str | None = None,
) -> OrderFilledProbabilityProfile:
    if path is None:
        if expected_sha256 is not None:
            raise ValueError("probability artifact checksum requires a profile path")
        return load_orderfilled_probability_profile(None)
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = Path(__file__).resolve().parents[3] / resolved
    return _probability_profile_cached(
        str(resolved),
        resolved.stat().st_mtime_ns,
        expected_sha256,
    )


def _scale_probability(
    probability: Decimal,
    seconds: float,
    *,
    base_seconds: float,
) -> Decimal:
    p = float(_bounded(probability))
    if p <= 0 or seconds <= 0:
        return Decimal(0)
    if p >= 1:
        return Decimal(1)
    scaled = 1.0 - math.pow(1.0 - p, float(seconds) / max(1e-9, base_seconds))
    return _bounded(Decimal(str(scaled)))


def _bounded(value: Decimal) -> Decimal:
    return max(Decimal(0), min(Decimal(1), _d(value)))


def _d(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))
