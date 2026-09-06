"""Optional Rust hot loop for source-auditable Fill-only taker replay."""

from __future__ import annotations

import math
import weakref
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from quant.backtest.orderfilled_v2_replay import (
        CapacityLedger,
        PreparedV2TradeTape,
        V2OrderResult,
        V2TakerOrder,
    )


Q = Decimal("0.0000000001")
SCALE = 10_000_000_000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class RustReplayCompatibility:
    supported: bool
    reason: str = ""


@dataclass(frozen=True)
class RustV2ReplayOutput:
    results: list[V2OrderResult]
    candidate_counts: list[int]


@dataclass(frozen=True)
class RustBernoulliOutput:
    draws: tuple[bool, ...]
    next_states: tuple[int, ...]


@dataclass(frozen=True)
class MonteCarloFillDistribution:
    expected_fill: Decimal
    fill_probability: Decimal
    full_fill_probability: Decimal
    q10: Decimal
    q50: Decimal
    q90: Decimal
    initial_state: int
    next_state: int
    rng_algorithm: str = "XORSHIFT64_POISSON_CHUNK_V1"


@dataclass(frozen=True)
class _RustPreparedArrays:
    trade_market: np.ndarray[Any, np.dtype[np.int64]]
    trade_asset: np.ndarray[Any, np.dtype[np.int64]]
    trade_side: np.ndarray[Any, np.dtype[np.uint8]]
    trade_block: np.ndarray[Any, np.dtype[np.int64]]
    trade_time_us: np.ndarray[Any, np.dtype[np.int64]]
    trade_price: np.ndarray[Any, np.dtype[np.int64]]
    trade_size: np.ndarray[Any, np.dtype[np.int64]]
    asset_codes: dict[str, int]
    trade_index_by_id: dict[str, int]


@dataclass
class _RustReplaySessionPayload:
    ledger_ref: Any
    native: Any
    ledger_version: int


def rust_kernel_available() -> bool:
    try:
        import _fill_only_rust  # type: ignore[import-not-found]  # noqa: F401
    except ImportError:
        return False
    return True


def rust_bernoulli_stateful(
    probabilities: Iterable[Decimal], states: Iterable[int]
) -> RustBernoulliOutput:
    """Draw one Bernoulli sample per stream and return resumable RNG states."""

    import _fill_only_rust  # type: ignore[import-not-found]

    probability_rows = tuple(probabilities)
    state_rows = tuple(int(value) for value in states)
    if len(probability_rows) != len(state_rows):
        raise ValueError("probability/state length mismatch")
    if any(value < 0 or value > 2**64 - 1 for value in state_rows):
        raise ValueError("Rust RNG state must fit uint64")
    draws, next_states = _fill_only_rust.bernoulli_stateful(
        np.asarray(
            [_scaled(_clamp_rate(value)) for value in probability_rows],
            dtype=np.int64,
        ),
        np.asarray(state_rows, dtype=np.uint64),
    )
    return RustBernoulliOutput(
        draws=tuple(bool(value) for value in draws.tolist()),
        next_states=tuple(int(value) for value in next_states.tolist()),
    )


def monte_carlo_fill_distribution_rust(
    *,
    poisson_mean: float,
    sizes: Iterable[Decimal],
    order_size: Decimal,
    participation_rate: Decimal,
    paths: int,
    seed: int,
    require_full: bool,
) -> MonteCarloFillDistribution:
    """Sample one modeled fill distribution in Rust and return resumable state."""

    import _fill_only_rust  # type: ignore[import-not-found]

    size_rows = tuple(sizes)
    chunks, threshold = _poisson_chunk_contract(poisson_mean)
    initial_state = _u64_seed(seed)
    output = _fill_only_rust.monte_carlo_fill_distribution(
        chunks,
        threshold,
        np.asarray([_scaled(value) for value in size_rows], dtype=np.int64),
        _scaled(order_size),
        _scaled(_clamp_rate(participation_rate)),
        max(1, int(paths)),
        initial_state,
        bool(require_full),
    )
    return _mc_output(output, initial_state=initial_state)


def monte_carlo_fill_distribution_python(
    *,
    poisson_mean: float,
    sizes: Iterable[Decimal],
    order_size: Decimal,
    participation_rate: Decimal,
    paths: int,
    seed: int,
    require_full: bool,
) -> MonteCarloFillDistribution:
    """Exact Python oracle for the versioned Rust Monte Carlo RNG contract."""

    size_rows = tuple(_scaled(value) for value in sizes)
    chunks, threshold = _poisson_chunk_contract(poisson_mean)
    if chunks > 0 and not size_rows:
        raise ValueError("monte carlo sizes cannot be empty when intensity is positive")
    path_count = max(1, int(paths))
    state = _u64_seed(seed)
    initial_state = state
    scaled_order_size = _scaled(order_size)
    participation = _scaled(_clamp_rate(participation_rate))
    fills: list[int] = []
    for _ in range(path_count):
        arrivals = 0
        for _ in range(chunks):
            product = 2**64 - 1
            count = 0
            while product > threshold:
                count += 1
                if count > 1_000_000:
                    raise ValueError("poisson sampler iteration limit exceeded")
                state = _xorshift64(state)
                product = (product * state) >> 64
            arrivals += max(0, count - 1)
        generated = 0
        for _ in range(arrivals):
            state = _xorshift64(state)
            index = (state * len(size_rows)) >> 64
            generated += size_rows[min(index, len(size_rows) - 1)]
        capacity = (generated * participation + SCALE // 2) // SCALE
        if require_full:
            fill = scaled_order_size if capacity >= scaled_order_size else 0
        else:
            fill = min(scaled_order_size, capacity)
        fills.append(fill)
    fills.sort()
    total = sum(fills)

    def probability(count: int) -> int:
        return (count * SCALE + path_count // 2) // path_count

    raw = {
        "expected_fill": (total + path_count // 2) // path_count,
        "fill_probability": probability(sum(value > 0 for value in fills)),
        "full_fill_probability": probability(
            sum(value >= scaled_order_size for value in fills)
        ),
        "q10": fills[((path_count - 1) * 1) // 10],
        "q50": fills[((path_count - 1) * 1) // 2],
        "q90": fills[((path_count - 1) * 9) // 10],
        "next_state": state,
    }
    return _mc_output(raw, initial_state=initial_state)


def _poisson_chunk_contract(mean: float) -> tuple[int, int]:
    if not math.isfinite(mean) or mean < 0:
        raise ValueError("poisson_mean must be finite and non-negative")
    if mean <= 0:
        return 0, 2**64 - 1
    chunks = max(1, math.ceil(mean / 20.0))
    chunk_mean = mean / chunks
    threshold = int(math.exp(-chunk_mean) * (2**64 - 1))
    return chunks, max(0, min(2**64 - 1, threshold))


def _u64_seed(seed: int) -> int:
    value = int(seed)
    if value < 0 or value > 2**64 - 1:
        raise ValueError("Monte Carlo seed must fit uint64")
    return value


def _xorshift64(state: int) -> int:
    mask = 2**64 - 1
    if state == 0:
        state = 0x9E37_79B9_7F4A_7C15
    state ^= (state << 13) & mask
    state ^= state >> 7
    state ^= (state << 17) & mask
    return state & mask


def _mc_output(output: Any, *, initial_state: int) -> MonteCarloFillDistribution:
    return MonteCarloFillDistribution(
        expected_fill=_from_scaled(int(output["expected_fill"])),
        fill_probability=_from_scaled(int(output["fill_probability"])),
        full_fill_probability=_from_scaled(int(output["full_fill_probability"])),
        q10=_from_scaled(int(output["q10"])),
        q50=_from_scaled(int(output["q50"])),
        q90=_from_scaled(int(output["q90"])),
        initial_state=initial_state,
        next_state=int(output["next_state"]),
    )


def v2_rust_compatibility(
    orders: Iterable[V2TakerOrder],
    prepared_tape: PreparedV2TradeTape,
) -> RustReplayCompatibility:
    order_rows = list(orders)
    if not order_rows:
        return RustReplayCompatibility(False, "empty_order_batch")
    if not prepared_tape.ordered_trades:
        return RustReplayCompatibility(False, "empty_trade_tape")
    for order in order_rows:
        reason = _unsupported_order_reason(order)
        if reason:
            return RustReplayCompatibility(False, f"{order.order_id}:{reason}")
        try:
            _scaled(order.limit_price)
            _scaled(order.size)
            _scaled(order.participation_rate)
            _scaled(order.price_buffer)
            _scaled(order.min_future_eligible_volume)
        except (OverflowError, ValueError) as exc:
            return RustReplayCompatibility(False, f"{order.order_id}:{exc}")
    if not rust_kernel_available():
        return RustReplayCompatibility(False, "rust_extension_not_installed")
    try:
        _prepared_arrays(prepared_tape)
    except (OverflowError, ValueError) as exc:
        return RustReplayCompatibility(False, f"trade_tape:{exc}")
    return RustReplayCompatibility(True)


def replay_v2_taker_orders_rust(
    orders: Iterable[V2TakerOrder],
    prepared_tape: PreparedV2TradeTape,
    ledger: CapacityLedger,
) -> RustV2ReplayOutput:
    """Run one compatible ordered batch and reconstruct canonical V2 results."""

    from quant.backtest.orderfilled_v2_replay import (
        V2Fill,
        _apply_orderfilled_probability_capacity,
        _full_fill_reject_reason,
        _is_cancel_remainder_tif,
        _no_trade_reason,
        _orderfilled_probability_context,
        _orderfilled_probability_decision,
        _result,
        _side,
        _unfilled_reason_for_order,
    )

    order_rows = list(orders)
    compatibility = v2_rust_compatibility(order_rows, prepared_tape)
    if not compatibility.supported:
        raise ValueError(f"V2 batch is not Rust-compatible: {compatibility.reason}")
    trade_rows = list(prepared_tape.ordered_trades)
    static = _prepared_arrays(prepared_tape)
    asset_codes = dict(static.asset_codes)
    for order in order_rows:
        asset_id = str(order.asset_id).lower()
        if asset_id not in asset_codes:
            asset_codes[asset_id] = len(asset_codes)
    native_session = _native_replay_session(prepared_tape, static, ledger)

    probability_decisions = [
        _orderfilled_probability_decision(
            order,
            _orderfilled_probability_context(
                prepared_tape.index,
                order,
                combined_groups=prepared_tape.combined_groups,
            ),
            presorted_relevant=True,
        )
        for order in order_rows
    ]
    fill_caps = [
        _apply_orderfilled_probability_capacity(
            max(Decimal(0), Decimal(order.size)).quantize(Q, rounding=ROUND_HALF_UP),
            decision,
        )
        for order, decision in zip(order_rows, probability_decisions, strict=True)
    ]

    output = native_session.native.match_taker_batch(
        _i64(order.market_id for order in order_rows),
        _i64(asset_codes[str(order.asset_id).lower()] for order in order_rows),
        _u8(_side_code(order.side) for order in order_rows),
        _i64(_optional_bound(order.arrival_block, lower=True) for order in order_rows),
        _i64(
            _optional_bound(order.deadline_block, lower=False) for order in order_rows
        ),
        _i64(
            _optional_time_bound(order.arrival_ts, lower=True) for order in order_rows
        ),
        _i64(
            _optional_time_bound(order.deadline_ts, lower=False) for order in order_rows
        ),
        _i64(_scaled(order.limit_price) for order in order_rows),
        _i64(_scaled(order.size) for order in order_rows),
        _i64(_scaled(value) for value in fill_caps),
        _u8(
            int(decision is not None and not decision.accepted)
            for decision in probability_decisions
        ),
        _i64(_scaled(_clamp_rate(order.participation_rate)) for order in order_rows),
        _i64(
            _scaled(max(Decimal(0), Decimal(order.price_buffer)))
            for order in order_rows
        ),
        _i64(
            _excluded_trade_index(order, static.trade_index_by_id)
            for order in order_rows
        ),
        _u8(_requires_full(order) for order in order_rows),
        _i64(
            max(0, int(order.min_future_eligible_trade_count)) for order in order_rows
        ),
        _i64(
            _scaled(max(Decimal(0), Decimal(order.min_future_eligible_volume)))
            for order in order_rows
        ),
    )

    fill_rows_by_order: dict[int, list[tuple[int, int, int, int]]] = {}
    fill_order_indexes = output["fill_order_index"].tolist()
    fill_trade_indexes = output["fill_trade_index"].tolist()
    fill_quantities = output["fill_quantity"].tolist()
    fill_prices = output["fill_price"].tolist()
    fill_capacities = output["fill_allocated_capacity"].tolist()
    for output_index, order_index in enumerate(fill_order_indexes):
        fill_rows_by_order.setdefault(int(order_index), []).append(
            (
                int(fill_trade_indexes[output_index]),
                int(fill_quantities[output_index]),
                int(fill_prices[output_index]),
                int(fill_capacities[output_index]),
            )
        )

    statuses = output["status"].tolist()
    remaining_rows = output["remaining"].tolist()
    eligible_rows = output["eligible_volume"].tolist()
    reason_rows = output["reason"].tolist()
    candidate_counts = [int(value) for value in output["candidate_count"].tolist()]
    results: list[V2OrderResult] = []
    for order_index, order in enumerate(order_rows):
        side = _side(order.side)
        limit = _decimal_q(order.limit_price)
        participation_rate = _clamp_rate(order.participation_rate)
        fills: list[V2Fill] = []
        for (
            trade_index,
            quantity_scaled,
            price_scaled,
            allocated_scaled,
        ) in fill_rows_by_order.get(order_index, []):
            trade = trade_rows[trade_index]
            quantity = _from_scaled(quantity_scaled)
            price = _from_scaled(price_scaled)
            allocated = _from_scaled(allocated_scaled)
            fills.append(
                V2Fill(
                    order_id=order.order_id,
                    fill_ts=trade.block_time,
                    fill_block=trade.block_number,
                    side=side,
                    limit_price=limit,
                    filled_size=quantity,
                    exec_price=price,
                    source_trade_id=trade.trade_id,
                    source_tx_hash=trade.tx_hash,
                    source_log_indexes=trade.source_log_indexes,
                    historical_price=trade.price,
                    historical_size=trade.size,
                    price_buffer_paid=abs(price - trade.price).quantize(
                        Q, rounding=ROUND_HALF_UP
                    ),
                    participation_rate=participation_rate,
                    allocated_capacity=allocated,
                    tx_index_source=trade.tx_index_source,
                )
            )
            ledger.consume_for_order(order, trade, quantity)
        status = {0: "NO_FILL", 1: "PARTIAL_FILLED", 2: "FILLED"}[
            int(statuses[order_index])
        ]
        probability = probability_decisions[order_index]
        reason = _reason_for_code(
            int(reason_rows[order_index]),
            order=order,
            remaining=_from_scaled(int(remaining_rows[order_index])),
            no_trade_reason=_no_trade_reason(order),
            unfilled_reason_for_order=_unfilled_reason_for_order,
            full_fill_reject_reason=_full_fill_reject_reason,
            probability=probability,
        )
        if status == "PARTIAL_FILLED" and _is_cancel_remainder_tif(order.tif):
            reason = "unfilled_remainder_cancelled_by_tif"
        results.append(
            _result(
                order,
                status,
                _from_scaled(int(remaining_rows[order_index])),
                fills,
                _from_scaled(int(eligible_rows[order_index])),
                reason,
                probability=probability,
            )
        )
    native_session.ledger_version = ledger.mutation_version
    return RustV2ReplayOutput(results=results, candidate_counts=candidate_counts)


def _unsupported_order_reason(order: V2TakerOrder) -> str:
    if Decimal(order.size) <= 0:
        return "invalid_order_size_requires_python"
    if _clamp_rate(order.participation_rate) <= 0:
        return "zero_participation_requires_python"
    if order.trade_side_evidence_mode != "same_side":
        return "any_side_evidence_not_supported"
    if order.lob_validity_rule:
        return "lob_validity_rule_requires_python"
    if order.require_pre_arrival_quote_proxy:
        return "pre_arrival_quote_gate_requires_python"
    if int(order.min_trailing_same_side_trade_count or 0) > 0:
        return "trailing_count_gate_requires_python"
    if Decimal(order.min_trailing_same_side_volume) > 0:
        return "trailing_volume_gate_requires_python"
    if Decimal(order.trailing_volume_multiplier) > 0:
        return "trailing_multiplier_requires_python"
    if order.trailing_participation_rate is not None:
        return "trailing_participation_requires_python"
    if order.max_fill_size_per_order is not None:
        return "per_order_capacity_cap_requires_python"
    if order.market_window_cap is not None or order.market_window_blocks is not None:
        return "market_window_cap_requires_python"
    if order.exclude_signal_source_trade and (
        order.signal_source_tx_hash or order.signal_source_log_indexes
    ):
        return "multi_key_source_exclusion_requires_python"
    return ""


def _prepared_arrays(prepared_tape: PreparedV2TradeTape) -> _RustPreparedArrays:
    cache_key = "fill_only_rust_arrays_v2"
    cached = prepared_tape.backend_payloads.get(cache_key)
    if isinstance(cached, _RustPreparedArrays):
        return cached
    trade_rows = prepared_tape.ordered_trades
    asset_codes = {
        asset_id: index
        for index, asset_id in enumerate(
            sorted({str(trade.asset_id).lower() for trade in trade_rows})
        )
    }
    payload = _RustPreparedArrays(
        trade_market=_readonly_i64(trade.market_id for trade in trade_rows),
        trade_asset=_readonly_i64(
            asset_codes[str(trade.asset_id).lower()] for trade in trade_rows
        ),
        trade_side=_readonly_u8(
            _side_code(trade.aggressor_side) for trade in trade_rows
        ),
        trade_block=_readonly_i64(trade.block_number for trade in trade_rows),
        trade_time_us=_readonly_i64(
            _datetime_us(trade.block_time) for trade in trade_rows
        ),
        trade_price=_readonly_i64(_scaled(trade.price) for trade in trade_rows),
        trade_size=_readonly_i64(_scaled(trade.size) for trade in trade_rows),
        asset_codes=asset_codes,
        trade_index_by_id={
            trade.trade_id: index for index, trade in enumerate(trade_rows)
        },
    )
    prepared_tape.backend_payloads[cache_key] = payload
    return payload


def _native_prepared_tape(
    prepared_tape: PreparedV2TradeTape,
    arrays: _RustPreparedArrays,
) -> Any:
    import _fill_only_rust  # type: ignore[import-not-found]

    cache_key = "fill_only_rust_native_tape_v1"
    cached = prepared_tape.backend_payloads.get(cache_key)
    if cached is not None:
        return cached
    native = _fill_only_rust.PreparedTradeTape(
        arrays.trade_market,
        arrays.trade_asset,
        arrays.trade_side,
        arrays.trade_block,
        arrays.trade_time_us,
        arrays.trade_price,
        arrays.trade_size,
    )
    prepared_tape.backend_payloads[cache_key] = native
    return native


def _native_replay_session(
    prepared_tape: PreparedV2TradeTape,
    arrays: _RustPreparedArrays,
    ledger: CapacityLedger,
) -> _RustReplaySessionPayload:
    sessions_key = "fill_only_rust_replay_sessions_v1"
    sessions = prepared_tape.backend_payloads.setdefault(sessions_key, {})
    for key, payload in tuple(sessions.items()):
        if payload.ledger_ref() is None:
            sessions.pop(key, None)
    key = id(ledger)
    payload = sessions.get(key)
    if (
        isinstance(payload, _RustReplaySessionPayload)
        and payload.ledger_ref() is ledger
        and payload.ledger_version == ledger.mutation_version
    ):
        return payload

    consumed = np.zeros(prepared_tape.trade_rows_indexed, dtype=np.int64)
    for trade_id, quantity in ledger.consumed_items():
        trade_index = arrays.trade_index_by_id.get(trade_id)
        if trade_index is not None:
            consumed[trade_index] = _scaled(quantity)
    native = _native_prepared_tape(prepared_tape, arrays).replay_session(consumed)
    payload = _RustReplaySessionPayload(
        ledger_ref=weakref.ref(ledger),
        native=native,
        ledger_version=ledger.mutation_version,
    )
    sessions[key] = payload
    return payload


def install_columnar_prepared_arrays(
    prepared_tape: PreparedV2TradeTape,
    *,
    trade_market: np.ndarray[Any, Any],
    trade_asset: np.ndarray[Any, Any],
    trade_side: np.ndarray[Any, Any],
    trade_block: np.ndarray[Any, Any],
    trade_time_us: np.ndarray[Any, Any],
    trade_price: np.ndarray[Any, Any],
    trade_size: np.ndarray[Any, Any],
    asset_codes: dict[str, int],
    trade_ids: Iterable[str],
) -> None:
    """Attach Arrow-derived arrays using the same cache contract as Python rows."""

    arrays = _RustPreparedArrays(
        trade_market=_readonly_array(trade_market, np.int64),
        trade_asset=_readonly_array(trade_asset, np.int64),
        trade_side=_readonly_array(trade_side, np.uint8),
        trade_block=_readonly_array(trade_block, np.int64),
        trade_time_us=_readonly_array(trade_time_us, np.int64),
        trade_price=_readonly_array(trade_price, np.int64),
        trade_size=_readonly_array(trade_size, np.int64),
        asset_codes=dict(asset_codes),
        trade_index_by_id={
            str(trade_id): index for index, trade_id in enumerate(trade_ids)
        },
    )
    lengths = {
        len(arrays.trade_market),
        len(arrays.trade_asset),
        len(arrays.trade_side),
        len(arrays.trade_block),
        len(arrays.trade_time_us),
        len(arrays.trade_price),
        len(arrays.trade_size),
        len(arrays.trade_index_by_id),
    }
    if lengths != {prepared_tape.trade_rows_indexed}:
        raise ValueError("columnar Rust arrays do not match the prepared tape")
    prepared_tape.backend_payloads["fill_only_rust_arrays_v2"] = arrays


def _readonly_array(values: np.ndarray[Any, Any], dtype: Any) -> np.ndarray[Any, Any]:
    array = np.ascontiguousarray(values, dtype=dtype)
    array.setflags(write=False)
    return array


def _reason_for_code(
    code: int,
    *,
    order: V2TakerOrder,
    remaining: Decimal,
    no_trade_reason: str,
    unfilled_reason_for_order: Any,
    full_fill_reject_reason: Any,
    probability: Any | None,
) -> str:
    if code == 0:
        return ""
    if code == 1:
        return no_trade_reason
    if code == 2:
        return "price_buffer_exceeds_limit"
    if code == 3:
        return "trade_print_capacity_already_consumed"
    if code == 4:
        return full_fill_reject_reason(order, "")
    if code == 5:
        return unfilled_reason_for_order(order, remaining, True, False, False)
    if code == 7:
        return "insufficient_future_eligible_trade_count"
    if code == 8:
        return "insufficient_future_eligible_volume"
    if code == 9:
        return (
            probability.reason
            if probability is not None
            else "low_orderfilled_fill_probability"
        )
    raise ValueError(f"unknown Rust matcher reason code: {code}")


def _execution_price(side: str, historical: Decimal, buffer: Decimal) -> Decimal:
    value = (
        historical + max(Decimal(0), Decimal(buffer))
        if side == "BUY"
        else historical - max(Decimal(0), Decimal(buffer))
    )
    return max(Decimal(0), min(Decimal(1), value)).quantize(Q, rounding=ROUND_HALF_UP)


def _requires_full(order: V2TakerOrder) -> int:
    return int(str(order.tif).upper() == "FOK" or not order.allow_partial_fill)


def _excluded_trade_index(order: V2TakerOrder, indexes: dict[str, int]) -> int:
    if not order.exclude_signal_source_trade or not order.signal_source_trade_id:
        return -1
    return indexes.get(order.signal_source_trade_id, -1)


def _clamp_rate(value: Decimal) -> Decimal:
    return max(Decimal(0), min(Decimal(1), Decimal(value)))


def _decimal_q(value: Decimal) -> Decimal:
    return Decimal(value).quantize(Q, rounding=ROUND_HALF_UP)


def _scaled(value: Decimal) -> int:
    scaled = int(
        (Decimal(value).quantize(Q, rounding=ROUND_HALF_UP) * SCALE).to_integral_exact()
    )
    if scaled < -(2**63) or scaled > 2**63 - 1:
        raise OverflowError("fixed-point value exceeds i64")
    return scaled


def _from_scaled(value: int) -> Decimal:
    return (Decimal(value) / Decimal(SCALE)).quantize(Q, rounding=ROUND_HALF_UP)


def _side_code(value: str) -> int:
    text = str(value).upper()
    if text == "BUY":
        return 0
    if text == "SELL":
        return 1
    raise ValueError(f"unsupported side: {value!r}")


def _datetime_us(value: datetime) -> int:
    resolved = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    delta = resolved.astimezone(timezone.utc) - _EPOCH
    result = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    if result < -(2**63) or result > 2**63 - 1:
        raise OverflowError("timestamp exceeds i64 microseconds")
    return result


def _optional_bound(value: int | None, *, lower: bool) -> int:
    return (-(2**63) if lower else 2**63 - 1) if value is None else int(value)


def _optional_time_bound(value: datetime | None, *, lower: bool) -> int:
    return _optional_bound(None, lower=lower) if value is None else _datetime_us(value)


def _i64(values: Iterable[int]) -> np.ndarray[Any, np.dtype[np.int64]]:
    return np.ascontiguousarray(list(values), dtype=np.int64)


def _u8(values: Iterable[int]) -> np.ndarray[Any, np.dtype[np.uint8]]:
    return np.ascontiguousarray(list(values), dtype=np.uint8)


def _readonly_i64(values: Iterable[int]) -> np.ndarray[Any, np.dtype[np.int64]]:
    result = _i64(values)
    result.flags.writeable = False
    return result


def _readonly_u8(values: Iterable[int]) -> np.ndarray[Any, np.dtype[np.uint8]]:
    result = _u8(values)
    result.flags.writeable = False
    return result
