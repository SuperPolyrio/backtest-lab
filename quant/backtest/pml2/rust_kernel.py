"""Bulk native book-walk for independent PML2 snapshot taker orders."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from time import perf_counter
from typing import Literal

import numpy as np

from quant.backtest.rust_kernel import SCALE, rust_kernel_available
from quant.simulator.economics import LiquidityRole

from .book import canonical_action, canonical_yes_price
from .contracts import (
    BookLevel,
    BookSnapshotEvent,
    CtfMatchType,
    ExecutionMatch,
    MatchFinalityState,
    ORDER_AMOUNT_TOLERANCE,
    OrderAmountUnit,
    OrderStatus,
    Pml2OrderIntent,
    Pml2OrderResult,
    RawOrderSide,
    TimeInForce,
    VenueAdmissionStatus,
    canonical_value,
    qty,
    qty_down,
)
from .profiles import Pml2Profile, get_pml2_profile

AUTO_RUST_MIN_ORDERS = 2_000
AUTO_RUST_MAX_LEVELS_PER_ORDER = 4


@dataclass(frozen=True, slots=True)
class IndependentSnapshotCase:
    order: Pml2OrderIntent
    snapshot: BookSnapshotEvent
    visible_depth_within_limit: Decimal | None = None


@dataclass(frozen=True, slots=True)
class Pml2NativeDiagnostics:
    backend: str
    orders: int
    levels: int
    materialized_levels: int
    matching_seconds: float
    fallback_reason: str = ""


def replay_independent_snapshot_takers(
    cases: list[IndependentSnapshotCase],
    *,
    profile: str | Pml2Profile = "realistic",
    backend: Literal["auto", "python", "rust"] = "auto",
) -> tuple[list[Pml2OrderResult], Pml2NativeDiagnostics]:
    """Replay independent FAK/FOK/IOC cases without rebuilding a session per order.

    Each case owns its snapshot capacity. This helper must not be used when orders
    share one economic residual book, wait for deltas, or enter a maker queue.
    """

    resolved = get_pml2_profile(profile) if isinstance(profile, str) else profile
    requested = backend.strip().lower()
    if requested not in {"auto", "python", "rust"}:
        raise ValueError(f"unsupported PML2 matcher backend: {backend!r}")
    normalized = [_normalize_case(case, resolved) for case in cases]
    has_quote_orders = any(
        order.amount_unit == OrderAmountUnit.QUOTE for order, _, _ in normalized
    )
    level_count = sum(len(levels) for _, _, levels in normalized)
    if requested == "rust" and not rust_kernel_available():
        raise ValueError("PML2 Rust snapshot matcher is not installed")
    if requested == "rust" and has_quote_orders:
        raise ValueError("PML2 Rust snapshot matcher does not support QUOTE orders")
    allocation_cases = normalized
    use_rust = requested == "rust"
    fallback_reason = ""
    if requested == "rust":
        allocation_cases = _trim_native_levels(normalized, resolved)
    elif requested == "auto":
        if has_quote_orders:
            fallback_reason = "quote_amount_unit_requires_python"
        elif not rust_kernel_available():
            fallback_reason = "rust_extension_not_installed"
        elif len(cases) < AUTO_RUST_MIN_ORDERS:
            fallback_reason = f"order_count_below_threshold:{AUTO_RUST_MIN_ORDERS}"
        else:
            allocation_cases = _trim_native_levels(normalized, resolved)
            use_rust = sum(len(levels) for _, _, levels in allocation_cases) <= (
                len(cases) * AUTO_RUST_MAX_LEVELS_PER_ORDER
            )
            if not use_rust:
                fallback_reason = (
                    "marketable_levels_per_order_above_threshold:"
                    f"{AUTO_RUST_MAX_LEVELS_PER_ORDER}"
                )
    materialized_level_count = sum(len(levels) for _, _, levels in allocation_cases)
    started = perf_counter()
    allocations = (
        _rust_allocations(allocation_cases, resolved)
        if use_rust
        else _python_allocations(allocation_cases, resolved)
    )
    results = [
        _build_result(case, resolved, rows)
        for case, rows in zip(cases, allocations, strict=True)
    ]
    return results, Pml2NativeDiagnostics(
        backend="rust" if use_rust else "python",
        orders=len(cases),
        levels=level_count,
        materialized_levels=materialized_level_count,
        matching_seconds=perf_counter() - started,
        fallback_reason=fallback_reason,
    )


def _normalize_case(
    case: IndependentSnapshotCase,
    profile: Pml2Profile,
) -> tuple[Pml2OrderIntent, BookSnapshotEvent, tuple[BookLevel, ...]]:
    order = case.order
    snapshot = case.snapshot
    if order.tif not in {TimeInForce.FAK, TimeInForce.FOK, TimeInForce.IOC}:
        raise ValueError("native snapshot replay supports FAK/FOK/IOC only")
    if order.post_only:
        raise ValueError("native snapshot replay does not support post-only orders")
    if (
        order.condition_id.casefold() != snapshot.condition_id.casefold()
        or order.market_id.casefold() != snapshot.market_id.casefold()
        or order.asset_id.casefold() != snapshot.asset_id.casefold()
        or order.outcome != snapshot.outcome
    ):
        raise ValueError("order and snapshot coordinates do not match")
    arrival = order.submit_ts + timedelta(
        milliseconds=(
            profile.entry_latency_ms
            if order.entry_latency_ms is None
            else max(0, order.entry_latency_ms)
        )
    )
    if snapshot.local_ts > arrival:
        raise ValueError("snapshot local_ts cannot be after order arrival")
    if (
        order.venue_admission == VenueAdmissionStatus.REJECTED_MIN_ORDER_SIZE
        or (
            snapshot.min_order_size is not None
            and order.requested_share_size < snapshot.min_order_size
            and order.venue_admission != VenueAdmissionStatus.ACCEPTED
        )
    ):
        raise ValueError("order is below snapshot min_order_size")
    if snapshot.tick_size is not None and order.limit_price % snapshot.tick_size != 0:
        raise ValueError("order limit is not tick aligned")
    levels = snapshot.asks if order.side == RawOrderSide.BUY else snapshot.bids
    ordered = tuple(
        sorted(
            levels,
            key=lambda level: level.price,
            reverse=order.side == RawOrderSide.SELL,
        )
    )
    return order, snapshot, ordered


def _trim_native_levels(
    cases: list[tuple[Pml2OrderIntent, BookSnapshotEvent, tuple[BookLevel, ...]]],
    profile: Pml2Profile,
) -> list[tuple[Pml2OrderIntent, BookSnapshotEvent, tuple[BookLevel, ...]]]:
    """Keep only the marketable prefix Rust can possibly consume."""

    trimmed = []
    for order, snapshot, levels in cases:
        selected: list[BookLevel] = []
        available = Decimal(0)
        for level in levels:
            if not _limit_allows(order, level.price):
                break
            effective = qty(level.size * profile.depth_haircut)
            if effective <= 0:
                continue
            selected.append(level)
            available = qty(available + effective)
            if available >= order.effective_requested_amount:
                break
        trimmed.append((order, snapshot, tuple(selected)))
    return trimmed


def _rust_allocations(
    cases: list[tuple[Pml2OrderIntent, BookSnapshotEvent, tuple[BookLevel, ...]]],
    profile: Pml2Profile,
) -> list[list[tuple[int, Decimal, Decimal, Decimal, Decimal]]]:
    import _fill_only_rust  # type: ignore[import-not-found]

    offsets = [0]
    prices: list[int] = []
    sizes: list[int] = []
    for _, _, levels in cases:
        prices.extend(_scaled(level.price) for level in levels)
        sizes.extend(_scaled(level.size) for level in levels)
        offsets.append(len(prices))
    output = _fill_only_rust.match_l2_snapshot_batch(
        np.asarray(offsets, dtype=np.int64),
        np.asarray(prices, dtype=np.int64),
        np.asarray(sizes, dtype=np.int64),
        np.asarray(
            [0 if order.side == RawOrderSide.BUY else 1 for order, _, _ in cases],
            dtype=np.uint8,
        ),
        np.asarray(
            [_scaled(order.limit_price) for order, _, _ in cases], dtype=np.int64
        ),
        np.asarray(
            [_scaled(order.effective_requested_amount) for order, _, _ in cases],
            dtype=np.int64,
        ),
        np.asarray([_scaled(profile.depth_haircut)] * len(cases), dtype=np.int64),
        np.asarray(
            [int(order.tif == TimeInForce.FOK) for order, _, _ in cases],
            dtype=np.uint8,
        ),
    )
    rows: list[list[tuple[int, Decimal, Decimal, Decimal, Decimal]]] = [
        [] for _ in cases
    ]
    for order_index, level_index, quantity, raw_price, before, after in zip(
        output["fill_order_index"].tolist(),
        output["fill_level_index"].tolist(),
        output["fill_quantity"].tolist(),
        output["fill_price"].tolist(),
        output["residual_before"].tolist(),
        output["residual_after"].tolist(),
        strict=True,
    ):
        rows[int(order_index)].append(
            (
                int(level_index) - offsets[int(order_index)],
                _unscaled(quantity),
                _unscaled(raw_price),
                _unscaled(before),
                _unscaled(after),
            )
        )
    return rows


def _python_allocations(
    cases: list[tuple[Pml2OrderIntent, BookSnapshotEvent, tuple[BookLevel, ...]]],
    profile: Pml2Profile,
) -> list[list[tuple[int, Decimal, Decimal, Decimal, Decimal]]]:
    output: list[list[tuple[int, Decimal, Decimal, Decimal, Decimal]]] = []
    for order, _, levels in cases:
        remaining = order.effective_requested_amount
        rows: list[tuple[int, Decimal, Decimal, Decimal, Decimal]] = []
        for index, level in enumerate(levels):
            if remaining <= 0:
                break
            if not _limit_allows(order, level.price):
                continue
            available = qty(level.size * profile.depth_haircut)
            quantity = (
                qty_down(min(available, remaining / level.price))
                if order.amount_unit == OrderAmountUnit.QUOTE
                else min(remaining, available)
            )
            if quantity <= 0:
                continue
            rows.append((index, quantity, level.price, available, available - quantity))
            consumed = (
                quantity * level.price
                if order.amount_unit == OrderAmountUnit.QUOTE
                else quantity
            )
            remaining = qty(max(Decimal(0), remaining - consumed))
        if order.tif == TimeInForce.FOK and remaining > ORDER_AMOUNT_TOLERANCE:
            rows = []
        output.append(rows)
    return output


def _build_result(
    case: IndependentSnapshotCase,
    profile: Pml2Profile,
    rows: list[tuple[int, Decimal, Decimal, Decimal, Decimal]],
) -> Pml2OrderResult:
    order = case.order
    snapshot = case.snapshot
    arrival = order.submit_ts + timedelta(
        milliseconds=(
            profile.entry_latency_ms
            if order.entry_latency_ms is None
            else max(0, order.entry_latency_ms)
        )
    )
    receive = arrival + timedelta(
        milliseconds=(
            profile.response_latency_ms
            if order.response_latency_ms is None
            else max(0, order.response_latency_ms)
        )
    )
    matches: list[ExecutionMatch] = []
    for sequence, (_, size, raw_price, before, after) in enumerate(rows, start=1):
        fill_id = f"{order.run_id}:{order.order_id}:taker:{sequence}"
        fee = qty(
            size
            * order.fee_rate
            * ((raw_price * (Decimal(1) - raw_price)) ** order.fee_exponent)
        )
        matches.append(
            ExecutionMatch(
                run_id=order.run_id,
                order_id=order.order_id,
                fill_id=fill_id,
                execution_model="PREDICTION_L2_REPLAY_V1",
                profile=profile.name,
                event_group_id=snapshot.snapshot_id,
                condition_id=order.condition_id,
                market_id=order.market_id,
                token_id=order.asset_id,
                outcome=order.outcome,
                raw_side=order.side,
                canonical_side=canonical_action(order.outcome, order.side),
                qty=size,
                raw_price=raw_price,
                canonical_yes_price=canonical_yes_price(order.outcome, raw_price),
                notional=qty(size * raw_price),
                liquidity_role=LiquidityRole.TAKER.value,
                tif=order.tif,
                match_type_hint=CtfMatchType.UNKNOWN_L2,
                signal_ts=order.signal_ts,
                observed_ts=order.observed_ts,
                submit_ts=order.submit_ts,
                exchange_arrival_ts=arrival,
                fill_exchange_ts=arrival,
                fill_receive_ts=receive,
                book_epoch=snapshot.book_epoch,
                snapshot_id=snapshot.snapshot_id,
                source_event_ids=(snapshot.snapshot_id,),
                queue_ahead_before=None,
                queue_ahead_after=None,
                residual_before=before,
                residual_after=after,
                fee=fee,
                rebate_accrual=Decimal(0),
                evidence_kind="L2_VISIBLE_DEPTH_AT_VENUE_EXECUTION",
                finality_state=MatchFinalityState.MATCHED,
            )
        )
    filled = qty(sum((match.qty for match in matches), Decimal(0)))
    filled_notional = qty(sum((match.notional for match in matches), Decimal(0)))
    filled_amount = (
        filled_notional
        if order.amount_unit == OrderAmountUnit.QUOTE
        else filled
    )
    remaining_amount = qty(
        max(Decimal(0), order.effective_requested_amount - filled_amount)
    )
    remaining = qty(max(Decimal(0), order.requested_share_size - filled))
    if remaining_amount <= ORDER_AMOUNT_TOLERANCE:
        remaining_amount = Decimal(0)
        remaining = Decimal(0)
        status = OrderStatus.FILLED
        reason = "arrival_book_walk_complete"
    elif filled > 0:
        status = OrderStatus.PARTIAL
        reason = (
            "ioc_remainder_cancelled"
            if order.tif == TimeInForce.IOC
            else "fak_remainder_cancelled"
        )
    elif order.tif == TimeInForce.FOK:
        status = OrderStatus.REJECTED
        reason = (
            "fok_truncated_depth_unverifiable"
            if snapshot.is_truncated
            else "fok_insufficient_economic_residual"
        )
    else:
        status = OrderStatus.CANCELLED
        reason = "no_marketable_exchange_depth"
    visible = (
        qty(case.visible_depth_within_limit)
        if case.visible_depth_within_limit is not None
        else qty(
            sum(
                (
                    level.size
                    for level in (
                        snapshot.asks
                        if order.side == RawOrderSide.BUY
                        else snapshot.bids
                    )
                    if _limit_allows(order, level.price)
                ),
                Decimal(0),
            )
        )
    )
    visible_amount = (
        qty(
            sum(
                (
                    level.size * level.price
                    for level in snapshot.asks
                    if _limit_allows(order, level.price)
                ),
                Decimal(0),
            )
        )
        if order.amount_unit == OrderAmountUnit.QUOTE
        else visible
    )
    ratio = (
        Decimal("Infinity")
        if visible_amount <= 0
        else order.effective_requested_amount / visible_amount
    )
    counterfactual_payload: dict[str, object] = {
        "gate_version": "pml2-visible-depth-impact-v2",
        "requested_size": order.requested_share_size,
        "visible_depth_within_limit": visible,
        "order_to_visible_depth_ratio": ratio,
        "warning_ratio": profile.max_order_to_visible_depth_ratio,
        "decision": (
            "VISIBLE_DEPTH_ONLY_REMAINDER_UNMODELED"
            if ratio > profile.max_order_to_visible_depth_ratio
            else "PASS"
        ),
        "impact_policy": "EXECUTE_VISIBLE_DEPTH_ONLY",
    }
    if order.amount_unit == OrderAmountUnit.QUOTE:
        counterfactual_payload.update(
            requested_amount=order.effective_requested_amount,
            amount_unit=order.amount_unit,
            visible_amount_within_limit=visible_amount,
        )
    counterfactual = canonical_value(counterfactual_payload)
    return Pml2OrderResult(
        order=order,
        status=status,
        reason=reason,
        exchange_arrival_ts=arrival,
        fills=tuple(matches),
        remaining_size=remaining,
        remaining_amount=remaining_amount,
        admission_diagnostics=(
            ("MIN_ORDER_SIZE_CONTRACT_CONFLICT",)
            if snapshot.min_order_size is not None
            and order.requested_share_size < snapshot.min_order_size
            and order.venue_admission == VenueAdmissionStatus.ACCEPTED
            else ()
        ),
        counterfactual_impact=counterfactual if visible > 0 else None,
    )


def _limit_allows(order: Pml2OrderIntent, raw_price: Decimal) -> bool:
    return (
        raw_price <= order.limit_price
        if order.side == RawOrderSide.BUY
        else raw_price >= order.limit_price
    )


def _scaled(value: Decimal) -> int:
    return int((Decimal(value) * SCALE).to_integral_value())


def _unscaled(value: int) -> Decimal:
    return (Decimal(int(value)) / SCALE).quantize(Decimal("0.0000000001"))


__all__ = [
    "IndependentSnapshotCase",
    "Pml2NativeDiagnostics",
    "replay_independent_snapshot_takers",
]
