from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from quant.backtest.pml2.contracts import (
    BookLevel,
    BookSnapshotEvent,
    OrderAmountUnit,
    Outcome,
    Pml2OrderIntent,
    RawOrderSide,
    TimeInForce,
)
from quant.backtest.pml2.rust_kernel import (
    IndependentSnapshotCase,
    replay_independent_snapshot_takers,
)
from quant.backtest.pml2.session import ReplayExecutionSession
from quant.backtest.rust_kernel import rust_kernel_available

pytestmark = pytest.mark.backtest_validation
T0 = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)


def _snapshot(
    case_id: str,
    *,
    outcome: Outcome = Outcome.YES,
    bids: tuple[tuple[str, str], ...] = (("0.49", "6"),),
    asks: tuple[tuple[str, str], ...] = (("0.50", "6"),),
) -> BookSnapshotEvent:
    return BookSnapshotEvent(
        snapshot_id=f"snapshot-{case_id}",
        condition_id=f"condition-{case_id}",
        market_id=f"market-{case_id}",
        asset_id=f"asset-{case_id}",
        outcome=outcome,
        exchange_ts=T0,
        local_ts=T0,
        book_epoch=0,
        bids=tuple(BookLevel(Decimal(price), Decimal(size)) for price, size in bids),
        asks=tuple(BookLevel(Decimal(price), Decimal(size)) for price, size in asks),
        source="fixture",
        sequence=0,
        is_full_depth=True,
        is_truncated=False,
        depth_scope="FULL",
        tick_size=Decimal("0.01"),
        min_order_size=Decimal(1),
    )


def _order(
    case_id: str,
    *,
    outcome: Outcome = Outcome.YES,
    side: RawOrderSide = RawOrderSide.BUY,
    size: str = "10",
    limit: str = "0.52",
    tif: TimeInForce = TimeInForce.FAK,
    amount_unit: OrderAmountUnit = OrderAmountUnit.SHARES,
) -> Pml2OrderIntent:
    at = T0 + timedelta(seconds=1)
    return Pml2OrderIntent(
        run_id="native-parity",
        order_id=f"order-{case_id}",
        strategy_id="strategy",
        condition_id=f"condition-{case_id}",
        market_id=f"market-{case_id}",
        asset_id=f"asset-{case_id}",
        outcome=outcome,
        side=side,
        size=Decimal(size),
        limit_price=Decimal(limit),
        tif=tif,
        signal_ts=at,
        observed_ts=at,
        submit_ts=at,
        amount_unit=amount_unit,
        entry_latency_ms=0,
        cancel_latency_ms=0,
        response_latency_ms=0,
        venue_delay_ms=0,
        fee_rate=Decimal("0.02"),
        fee_exponent=2,
    )


def _session_result(case: IndependentSnapshotCase):
    session = ReplayExecutionSession(run_id=case.order.run_id, profile="realistic")
    session.ingest_snapshot(case.snapshot)
    session.submit_order(case.order)
    session.run()
    return session.result(case.order.order_id)


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_native_snapshot_matcher_matches_python_and_session_contract() -> None:
    cases = [
        IndependentSnapshotCase(
            _order("buy-fak", size="10", limit="0.52"),
            _snapshot("buy-fak", asks=(("0.50", "4"), ("0.51", "5"))),
        ),
        IndependentSnapshotCase(
            _order(
                "sell-ioc",
                side=RawOrderSide.SELL,
                size="7",
                limit="0.47",
                tif=TimeInForce.IOC,
            ),
            _snapshot("sell-ioc", bids=(("0.49", "3"), ("0.48", "5"))),
        ),
        IndependentSnapshotCase(
            _order(
                "no-buy",
                outcome=Outcome.NO,
                size="5",
                limit="0.42",
            ),
            _snapshot(
                "no-buy",
                outcome=Outcome.NO,
                asks=(("0.40", "2"), ("0.41", "6")),
            ),
        ),
        IndependentSnapshotCase(
            _order("fok-reject", size="10", limit="0.52", tif=TimeInForce.FOK),
            _snapshot("fok-reject", asks=(("0.50", "3"),)),
        ),
    ]

    python, python_diagnostics = replay_independent_snapshot_takers(
        cases, profile="realistic", backend="python"
    )
    rust, rust_diagnostics = replay_independent_snapshot_takers(
        cases, profile="realistic", backend="rust"
    )
    session = [_session_result(case) for case in cases]

    assert [row.as_dict() for row in rust] == [row.as_dict() for row in python]
    assert [row.as_dict() for row in rust] == [row.as_dict() for row in session]
    assert python_diagnostics.backend == "python"
    assert rust_diagnostics.backend == "rust"


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_native_snapshot_matcher_randomized_python_rust_differential() -> None:
    rng = random.Random(73_2026)
    cases: list[IndependentSnapshotCase] = []
    for index in range(1_000):
        outcome = rng.choice([Outcome.YES, Outcome.NO])
        side = rng.choice([RawOrderSide.BUY, RawOrderSide.SELL])
        levels = tuple(
            (
                f"{price / 100:.2f}",
                f"{rng.randrange(1, 101) / 10:.1f}",
            )
            for price in (
                sorted(rng.sample(range(35, 66), rng.randrange(1, 6)))
                if side == RawOrderSide.BUY
                else sorted(
                    rng.sample(range(35, 66), rng.randrange(1, 6)), reverse=True
                )
            )
        )
        case_id = f"random-{index}"
        snapshot = _snapshot(
            case_id,
            outcome=outcome,
            asks=levels if side == RawOrderSide.BUY else (("0.70", "1"),),
            bids=levels if side == RawOrderSide.SELL else (("0.30", "1"),),
        )
        cases.append(
            IndependentSnapshotCase(
                _order(
                    case_id,
                    outcome=outcome,
                    side=side,
                    size=str(rng.randrange(10, 201) / 10),
                    limit=f"{rng.randrange(38, 63) / 100:.2f}",
                    tif=rng.choice([TimeInForce.FAK, TimeInForce.FOK, TimeInForce.IOC]),
                ),
                snapshot,
            )
        )

    python, _ = replay_independent_snapshot_takers(
        cases, profile="realistic", backend="python"
    )
    rust, _ = replay_independent_snapshot_takers(
        cases, profile="realistic", backend="rust"
    )

    assert [row.as_dict() for row in rust] == [row.as_dict() for row in python]


def test_quote_budget_snapshot_matcher_matches_session_via_python() -> None:
    case = IndependentSnapshotCase(
        _order(
            "quote-buy",
            size="1",
            limit="0.50",
            amount_unit=OrderAmountUnit.QUOTE,
        ),
        _snapshot("quote-buy", asks=(("0.40", "1"), ("0.50", "2"))),
    )

    results, diagnostics = replay_independent_snapshot_takers(
        [case], profile="realistic", backend="python"
    )

    assert results[0].as_dict() == _session_result(case).as_dict()
    assert results[0].filled_amount == Decimal("1.0000000000")
    assert results[0].filled_size == Decimal("2.2000000000")
    assert diagnostics.backend == "python"
    with pytest.raises(ValueError, match="does not support QUOTE"):
        replay_independent_snapshot_takers(
            [case], profile="realistic", backend="rust"
        )


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_native_snapshot_auto_routes_by_measured_batch_shape() -> None:
    case = IndependentSnapshotCase(_order("auto", size="1"), _snapshot("auto"))

    small, small_diagnostics = replay_independent_snapshot_takers(
        [case] * 100, profile="realistic", backend="auto"
    )
    large, large_diagnostics = replay_independent_snapshot_takers(
        [case] * 2_000, profile="realistic", backend="auto"
    )
    explicit, _ = replay_independent_snapshot_takers(
        [case] * 2_000, profile="realistic", backend="python"
    )

    assert small_diagnostics.backend == "python"
    assert small_diagnostics.fallback_reason == "order_count_below_threshold:2000"
    assert large_diagnostics.backend == "rust"
    assert [row.as_dict() for row in large] == [row.as_dict() for row in explicit]
    assert len(small) == 100
