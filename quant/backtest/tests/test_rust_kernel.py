from __future__ import annotations

import random
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from quant.backtest.orderfilled_probability import (
    default_orderfilled_probability_profile,
)
from quant.backtest.orderfilled_v2_replay import (
    CapacityLedger,
    V2TakerOrder,
    V2TradePrint,
    prepare_v2_trade_tape,
    replay_v2_taker_orders_with_diagnostics,
)
from quant.backtest.rust_kernel import (
    monte_carlo_fill_distribution_python,
    monte_carlo_fill_distribution_rust,
    rust_bernoulli_stateful,
    rust_kernel_available,
)

pytestmark = pytest.mark.backtest_validation
BASE_TS = datetime(2026, 7, 1, tzinfo=timezone.utc)


def _trade(
    trade_id: str,
    block: int,
    side: str,
    price: str,
    size: str,
    *,
    second: int,
    asset_id: str = "asset-a",
) -> V2TradePrint:
    return V2TradePrint(
        trade_id=trade_id,
        market_id=11,
        condition_id="condition",
        asset_id=asset_id,
        outcome="YES",
        block_number=block,
        block_time=BASE_TS + timedelta(seconds=second),
        tx_hash=f"0x{trade_id}",
        tx_index=block,
        tx_index_source="fixture",
        price=Decimal(price),
        size=Decimal(size),
        notional=Decimal(price) * Decimal(size),
        aggressor_side=side,  # type: ignore[arg-type]
        passive_side="SELL" if side == "BUY" else "BUY",  # type: ignore[arg-type]
        source_log_indexes=(block,),
        source_fill_count=1,
        trade_group_id=f"group-{trade_id}",
    )


def _order(order_id: str, side: str, limit: str, size: str = "10") -> V2TakerOrder:
    return V2TakerOrder(
        order_id=order_id,
        market_id=11,
        asset_id="asset-a",
        side=side,  # type: ignore[arg-type]
        limit_price=Decimal(limit),
        size=Decimal(size),
        signal_block=99,
        signal_ts=BASE_TS,
        latency_blocks=1,
        latency=timedelta(seconds=1),
        horizon_blocks=20,
        horizon=timedelta(seconds=20),
        participation_rate=Decimal("0.025"),
    )


def _assert_python_rust_equal(
    orders: list[V2TakerOrder],
    trades: list[V2TradePrint],
    *,
    initial_consumed: tuple[str, Decimal] | None = None,
) -> None:
    tape = prepare_v2_trade_tape(trades)
    python_ledger = CapacityLedger()
    rust_ledger = CapacityLedger()
    if initial_consumed is not None:
        python_ledger.consume(*initial_consumed)
        rust_ledger.consume(*initial_consumed)
    python_results, _, python_diag = replay_v2_taker_orders_with_diagnostics(
        orders,
        ledger=python_ledger,
        prepared_tape=tape,
        backend="python",
    )
    rust_results, _, rust_diag = replay_v2_taker_orders_with_diagnostics(
        orders,
        ledger=rust_ledger,
        prepared_tape=tape,
        backend="rust",
    )
    assert [row.as_dict() for row in rust_results] == [
        row.as_dict() for row in python_results
    ]
    assert rust_ledger.snapshot() == python_ledger.snapshot()
    assert rust_diag.candidate_rows_scanned <= python_diag.candidate_rows_scanned
    assert rust_diag.matching_backend == "rust"


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_rust_matches_python_for_buy_sell_buffer_and_shared_capacity() -> None:
    trades = [
        _trade("buy-touch", 100, "BUY", "0.52", "100", second=1),
        _trade("sell-touch", 101, "SELL", "0.49", "80", second=2),
        _trade("buy-later", 102, "BUY", "0.53", "200", second=3),
        _trade("buy-expensive", 103, "BUY", "0.56", "100", second=4),
    ]
    orders = [
        replace(_order("buy-1", "BUY", "0.53"), price_buffer=Decimal("0.005")),
        replace(_order("buy-2", "BUY", "0.54"), price_buffer=Decimal("0.005")),
        replace(_order("sell-1", "SELL", "0.48"), price_buffer=Decimal("0.005")),
    ]
    _assert_python_rust_equal(orders, trades)


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_rust_matches_python_for_fok_fak_source_exclusion_and_existing_ledger() -> None:
    trades = [
        _trade("signal", 100, "BUY", "0.50", "100", second=1),
        _trade("capacity-a", 101, "BUY", "0.50", "100", second=2),
        _trade("capacity-b", 102, "BUY", "0.50", "100", second=3),
    ]
    base = replace(
        _order("base", "BUY", "0.51"),
        exclude_signal_source_trade=True,
        signal_source_trade_id="signal",
    )
    orders = [
        replace(base, order_id="fok", tif="FOK", size=Decimal(4)),
        replace(base, order_id="fak", tif="FAK", size=Decimal(10)),
        replace(
            base, order_id="partial-disabled", allow_partial_fill=False, size=Decimal(4)
        ),
    ]
    _assert_python_rust_equal(
        orders,
        trades,
        initial_consumed=("capacity-a", Decimal("1.5")),
    )


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_rust_matches_python_for_future_evidence_gates_and_time_bounds() -> None:
    trades = [
        _trade("before", 99, "BUY", "0.50", "100", second=0),
        _trade("eligible-a", 100, "BUY", "0.50", "10", second=1),
        _trade("eligible-b", 101, "BUY", "0.50", "20", second=2),
        _trade("late", 110, "BUY", "0.50", "100", second=30),
    ]
    base = replace(
        _order("gate-pass", "BUY", "0.51"),
        min_future_eligible_trade_count=2,
        min_future_eligible_volume=Decimal(30),
        horizon=timedelta(seconds=3),
        horizon_blocks=3,
    )
    orders = [
        base,
        replace(base, order_id="count-fail", min_future_eligible_trade_count=3),
        replace(base, order_id="volume-fail", min_future_eligible_volume=Decimal(31)),
        replace(base, order_id="missing-asset", asset_id="asset-missing"),
    ]
    _assert_python_rust_equal(orders, trades)


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_rust_matches_python_for_probability_capacity_and_rejection() -> None:
    trades = [
        _trade("before", 99, "BUY", "0.50", "200", second=0),
        _trade("eligible-a", 100, "BUY", "0.50", "200", second=1),
        _trade("eligible-b", 101, "BUY", "0.50", "200", second=2),
    ]
    profile = default_orderfilled_probability_profile().as_dict()
    rejected_profile = dict(profile)
    rejected_profile["hard_reject_below_probability"] = True
    rejected_profile["min_probability"] = "1"
    orders = [
        replace(
            _order("probability-cap", "BUY", "0.51"),
            orderfilled_probability_profile=profile,
            min_future_eligible_trade_count=1,
        ),
        replace(
            _order("probability-reject", "BUY", "0.51"),
            orderfilled_probability_profile=rejected_profile,
            min_future_eligible_trade_count=1,
        ),
    ]

    _assert_python_rust_equal(orders, trades)


def test_auto_backend_avoids_native_materialization_for_small_batches() -> None:
    trade = _trade("trade", 100, "BUY", "0.50", "100", second=1)
    order = replace(
        _order("window-cap", "BUY", "0.51"),
        market_window_cap=Decimal(1),
        market_window_blocks=100,
    )
    _, _, diagnostics = replay_v2_taker_orders_with_diagnostics(
        [order],
        [trade],
        backend="auto",
    )
    assert diagnostics.matching_backend == "python"
    assert diagnostics.backend_fallback_reason == "batch_work_below_threshold:10000"
    with pytest.raises(ValueError, match="market_window_cap_requires_python"):
        replay_v2_taker_orders_with_diagnostics(
            [order],
            [trade],
            backend="rust",
        )


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_rust_matches_python_on_deterministic_randomized_batch() -> None:
    rng = random.Random(730_2026)
    trades = [
        _trade(
            f"random-{index}",
            100 + index,
            rng.choice(["BUY", "SELL"]),
            f"{rng.randrange(35, 66) / 100:.2f}",
            str(rng.randrange(1, 101)),
            second=index,
            asset_id=rng.choice(["asset-a", "asset-b"]),
        )
        for index in range(200)
    ]
    orders: list[V2TakerOrder] = []
    for index in range(80):
        signal_offset = rng.randrange(0, 180)
        tif = rng.choice(["GTC", "GTD", "IOC", "FOK", "FAK"])
        source = trades[signal_offset] if rng.random() < 0.25 else None
        orders.append(
            V2TakerOrder(
                order_id=f"random-order-{index}",
                market_id=11,
                asset_id=rng.choice(["asset-a", "asset-b", "asset-missing"]),
                side=rng.choice(["BUY", "SELL"]),
                limit_price=Decimal(rng.randrange(38, 63)) / Decimal(100),
                size=Decimal(rng.randrange(1, 21)),
                signal_block=99 + signal_offset,
                signal_ts=BASE_TS + timedelta(seconds=max(0, signal_offset - 1)),
                latency_blocks=rng.randrange(0, 3),
                latency=timedelta(seconds=rng.randrange(0, 3)),
                horizon_blocks=rng.randrange(1, 21),
                horizon=timedelta(seconds=rng.randrange(1, 21)),
                participation_rate=Decimal(rng.choice([5, 10, 25, 50])) / Decimal(1000),
                price_buffer=Decimal(rng.choice([0, 5, 10])) / Decimal(1000),
                tif=tif,  # type: ignore[arg-type]
                allow_partial_fill=rng.random() >= 0.1,
                exclude_signal_source_trade=source is not None,
                signal_source_trade_id=source.trade_id if source is not None else None,
                min_future_eligible_trade_count=rng.choice([0, 0, 0, 1, 2]),
                min_future_eligible_volume=Decimal(rng.choice([0, 0, 10, 25])),
            )
        )
    _assert_python_rust_equal(orders, trades)


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_persistent_rust_session_matches_one_shot_and_resynchronizes_ledger() -> None:
    trades = [
        _trade("capacity-a", 100, "BUY", "0.50", "400", second=1),
        _trade("capacity-b", 101, "BUY", "0.50", "400", second=2),
    ]
    orders = [
        _order("batch-a", "BUY", "0.51", size="6"),
        _order("batch-b", "BUY", "0.51", size="6"),
        _order("batch-c", "BUY", "0.51", size="6"),
    ]
    tape = prepare_v2_trade_tape(trades)

    python_ledger = CapacityLedger()
    python_results, _, _ = replay_v2_taker_orders_with_diagnostics(
        orders,
        ledger=python_ledger,
        prepared_tape=tape,
        backend="python",
    )

    rust_ledger = CapacityLedger()
    first, _, _ = replay_v2_taker_orders_with_diagnostics(
        orders[:1],
        ledger=rust_ledger,
        prepared_tape=tape,
        backend="rust",
    )
    second, _, _ = replay_v2_taker_orders_with_diagnostics(
        orders[1:2],
        ledger=rust_ledger,
        prepared_tape=tape,
        backend="rust",
    )
    # Simulate a checkpoint restore or an allocation performed by another route.
    rust_ledger.consume("capacity-b", Decimal("1.25"))
    python_ledger_after_external = CapacityLedger()
    replay_v2_taker_orders_with_diagnostics(
        orders[:2],
        ledger=python_ledger_after_external,
        prepared_tape=tape,
        backend="python",
    )
    python_ledger_after_external.consume("capacity-b", Decimal("1.25"))
    expected_third, _, _ = replay_v2_taker_orders_with_diagnostics(
        orders[2:],
        ledger=python_ledger_after_external,
        prepared_tape=tape,
        backend="python",
    )
    third, _, _ = replay_v2_taker_orders_with_diagnostics(
        orders[2:],
        ledger=rust_ledger,
        prepared_tape=tape,
        backend="rust",
    )

    assert [row.as_dict() for row in first + second] == [
        row.as_dict() for row in python_results[:2]
    ]
    assert [row.as_dict() for row in third] == [row.as_dict() for row in expected_third]
    assert rust_ledger.snapshot() == python_ledger_after_external.snapshot()
    assert "fill_only_rust_native_tape_v1" in tape.backend_payloads
    assert len(tape.backend_payloads["fill_only_rust_replay_sessions_v1"]) == 1


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_rust_probability_rng_state_resumes_exactly() -> None:
    probabilities = [Decimal("0.10"), Decimal("0.50"), Decimal("0.90")]
    initial_states = [0, 73, 2**64 - 1]

    first = rust_bernoulli_stateful(probabilities, initial_states)
    resumed = rust_bernoulli_stateful(probabilities, first.next_states)
    continuous_first = rust_bernoulli_stateful(probabilities, initial_states)
    continuous_second = rust_bernoulli_stateful(
        probabilities, continuous_first.next_states
    )

    assert first == continuous_first
    assert resumed == continuous_second
    assert first.next_states != tuple(initial_states)


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
@pytest.mark.parametrize("poisson_mean", [0.0, 0.25, 8.5, 41.0, 123.5])
def test_rust_monte_carlo_distribution_matches_python_reference(
    poisson_mean: float,
) -> None:
    kwargs = {
        "poisson_mean": poisson_mean,
        "sizes": [Decimal("0.5"), Decimal(2), Decimal("7.25")],
        "order_size": Decimal(10),
        "participation_rate": Decimal("0.025"),
        "paths": 257,
        "seed": 73,
        "require_full": False,
    }

    python = monte_carlo_fill_distribution_python(**kwargs)
    rust = monte_carlo_fill_distribution_rust(**kwargs)

    assert rust == python
    resumed = monte_carlo_fill_distribution_rust(**{**kwargs, "seed": rust.next_state})
    assert resumed.initial_state == rust.next_state
