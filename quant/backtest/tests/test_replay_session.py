from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from quant.backtest.orderfilled_v2_replay import (
    V2OrderResult,
    V2TakerOrder,
    V2TradePrint,
)
from quant.backtest.replay_session import (
    FrozenOrderStrategy,
    InMemoryTradeCatalog,
    ReplaySession,
    timestamp_aligned_chunks,
)
from quant.backtest.trade_only_v3 import LiquidityIntent, TradeOnlyOrder

BASE_TS = datetime(2026, 7, 1, tzinfo=timezone.utc)


def _trade(
    trade_id: str,
    *,
    seconds: int,
    block: int,
    side: str = "BUY",
    price: str = "0.50",
    tx_hash: str | None = None,
    trade_group_id: str | None = None,
) -> V2TradePrint:
    return V2TradePrint(
        trade_id=trade_id,
        market_id=7,
        condition_id="condition",
        asset_id="asset",
        outcome="YES",
        block_number=block,
        block_time=BASE_TS + timedelta(seconds=seconds),
        tx_hash=tx_hash or f"0x{trade_id}",
        tx_index=block,
        tx_index_source="receipt",
        price=Decimal(price),
        size=Decimal(100),
        notional=Decimal(price) * Decimal(100),
        aggressor_side=side,  # type: ignore[arg-type]
        passive_side="SELL" if side == "BUY" else "BUY",
        source_log_indexes=(block,),
        source_fill_count=1,
        trade_group_id=trade_group_id,
    )


def _v2_order(signal: V2TradePrint) -> V2TakerOrder:
    return V2TakerOrder(
        order_id="order-v2",
        market_id=signal.market_id,
        asset_id=signal.asset_id,
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(2),
        signal_block=signal.block_number,
        signal_ts=signal.block_time,
        latency=timedelta(0),
        latency_blocks=0,
        horizon=timedelta(seconds=10),
        horizon_blocks=10,
        participation_rate=Decimal("0.025"),
        signal_source_trade_id=signal.trade_id,
        signal_source_tx_hash=signal.tx_hash,
        signal_source_log_indexes=signal.source_log_indexes,
        exclude_signal_source_trade=True,
    )


def test_timestamp_chunks_do_not_split_timestamp_tx_or_trade_group() -> None:
    trades = [
        _trade("a", seconds=0, block=1, tx_hash="0xa"),
        _trade("b", seconds=0, block=1, tx_hash="0xb"),
        _trade("c", seconds=1, block=2, tx_hash="0xc", trade_group_id="group"),
        _trade("d", seconds=2, block=3, tx_hash="0xd", trade_group_id="group"),
        _trade("e", seconds=3, block=4, tx_hash="0xe"),
        _trade("f", seconds=4, block=5, tx_hash="0xe"),
    ]

    chunks = timestamp_aligned_chunks(trades, target_rows=1)

    assert [[row.trade_id for row in chunk] for chunk in chunks] == [
        ["a", "b"],
        ["c", "d"],
        ["e", "f"],
    ]


def test_v2_dynamic_replay_equals_frozen_and_excludes_signal_trade() -> None:
    signal = _trade("signal", seconds=0, block=100)
    future = _trade("future", seconds=2, block=102)
    catalog = InMemoryTradeCatalog([signal, future])
    order = _v2_order(signal)

    frozen = ReplaySession(
        run_id="frozen", execution_family="V2", catalog=catalog
    )
    frozen_results, _ = frozen.replay_frozen_orders([order])
    dynamic = ReplaySession(
        run_id="dynamic", execution_family="V2", catalog=catalog
    )
    dynamic_results, receipt = dynamic.replay_dynamic(
        FrozenOrderStrategy([order]), chunk_size=1
    )

    assert [row.as_dict() for row in dynamic_results] == [
        row.as_dict() for row in frozen_results
    ]
    assert dynamic.v2_ledger.as_dict() == frozen.v2_ledger.as_dict()
    assert dynamic.accounting.as_dict() == frozen.accounting.as_dict()
    assert isinstance(dynamic_results[0], V2OrderResult)
    assert dynamic_results[0].fills[0].source_trade_id == "future"
    assert receipt.chunks_processed == 2
    assert receipt.events_processed == 2


@pytest.mark.parametrize("chunk_size", [1, 1_000, 100_000])
def test_dynamic_chunk_sizes_and_reset_match_fresh_process(chunk_size: int) -> None:
    trades = [_trade(f"t{index}", seconds=index, block=100 + index) for index in range(8)]
    order = _v2_order(trades[1])
    catalog = InMemoryTradeCatalog(trades)

    small = ReplaySession(run_id="same", execution_family="V2", catalog=catalog)
    small_results, _ = small.replay_dynamic(FrozenOrderStrategy([order]), chunk_size=1)
    catalog_identity = id(catalog.prepared_tape)
    small.reset_run()
    reset_results, _ = small.replay_dynamic(FrozenOrderStrategy([order]), chunk_size=1_000)
    fresh = ReplaySession(run_id="same", execution_family="V2", catalog=catalog)
    fresh_results, _ = fresh.replay_dynamic(
        FrozenOrderStrategy([order]), chunk_size=chunk_size
    )

    expected = [row.as_dict() for row in small_results]
    assert [row.as_dict() for row in reset_results] == expected
    assert [row.as_dict() for row in fresh_results] == expected
    assert id(catalog.prepared_tape) == catalog_identity
    assert small.v2_ledger.as_dict() == fresh.v2_ledger.as_dict()


def test_dynamic_order_cannot_use_current_signal_print_as_fill() -> None:
    signal = _trade("only", seconds=0, block=100)
    catalog = InMemoryTradeCatalog([signal])
    session = ReplaySession(run_id="self", execution_family="V2", catalog=catalog)

    results, _ = session.replay_dynamic(
        FrozenOrderStrategy([_v2_order(signal)]), chunk_size=1
    )

    assert results[0].status == "NO_FILL"
    assert results[0].filled_size == 0
    assert session.accounting.source_confirmed.fill_count == 0


def test_dynamic_signal_can_target_another_token_without_self_fill() -> None:
    signal = _trade("signal", seconds=0, block=100)
    target = replace(
        _trade("target", seconds=2, block=102),
        asset_id="asset-b",
    )
    order = replace(
        _v2_order(signal),
        asset_id="asset-b",
        signal_source_trade_id=signal.trade_id,
    )
    catalog = InMemoryTradeCatalog([signal, target])
    session = ReplaySession(run_id="cross-token", execution_family="V2", catalog=catalog)

    results, _ = session.replay_dynamic(
        FrozenOrderStrategy([order]), chunk_size=1
    )

    assert results[0].status == "FILLED"
    assert results[0].fills[0].source_trade_id == "target"
    assert results[0].fills[0].source_trade_id != signal.trade_id


def test_v3_modeled_fills_are_accounted_separately_from_source_fills() -> None:
    pre_one = _trade("pre-one", seconds=0, block=100, side="BUY", price="0.55")
    pre_two = _trade("pre-two", seconds=1, block=101, side="SELL", price="0.53")
    signal = _trade("signal", seconds=2, block=102, side="BUY", price="0.55")
    catalog = InMemoryTradeCatalog([pre_one, pre_two, signal])
    order = TradeOnlyOrder(
        order_id="order-v3",
        market_id=7,
        asset_id="asset",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(10),
        signal_block=signal.block_number,
        signal_ts=signal.block_time,
        tif="GTD",
        liquidity_intent=LiquidityIntent.TAKER,
        latency=timedelta(0),
        latency_blocks=0,
        horizon=timedelta(seconds=30),
        horizon_blocks=30,
        lookback=timedelta(minutes=5),
        lookback_blocks=100,
        signal_source_trade_id=signal.trade_id,
    )
    session = ReplaySession(
        run_id="modeled",
        execution_family="V3",
        profile="taker_synthetic_q50",
        catalog=catalog,
    )

    results, _ = session.replay_dynamic(FrozenOrderStrategy([order]), chunk_size=1)

    assert results[0].filled_size > 0
    assert session.accounting.modeled.fill_count == 1
    assert session.accounting.source_confirmed.fill_count == 0
    assert session.accounting.inferred.fill_count == 0


def test_v3_dynamic_source_replay_equals_frozen() -> None:
    pre = _trade("pre", seconds=0, block=100, side="BUY", price="0.55")
    signal = _trade("signal", seconds=1, block=101, side="BUY", price="0.55")
    future = _trade("future", seconds=2, block=102, side="BUY", price="0.55")
    catalog = InMemoryTradeCatalog([pre, signal, future])
    base = TradeOnlyOrder(
        order_id="order-v3-source",
        market_id=7,
        asset_id="asset",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(2),
        signal_block=signal.block_number,
        signal_ts=signal.block_time,
        tif="GTD",
        liquidity_intent=LiquidityIntent.TAKER,
        latency=timedelta(0),
        latency_blocks=0,
        horizon=timedelta(seconds=10),
        horizon_blocks=10,
        signal_source_trade_id=signal.trade_id,
    )
    frozen = ReplaySession(
        run_id="v3", execution_family="V3", profile="taker_source_confirmed", catalog=catalog
    )
    dynamic = ReplaySession(
        run_id="v3", execution_family="V3", profile="taker_source_confirmed", catalog=catalog
    )

    frozen_results, _ = frozen.replay_frozen_orders([base])
    dynamic_results, _ = dynamic.replay_dynamic(
        FrozenOrderStrategy([replace(base)]), chunk_size=1
    )

    assert [row.as_dict() for row in dynamic_results] == [
        row.as_dict() for row in frozen_results
    ]
    assert dynamic.v3_ledger.as_dict() == frozen.v3_ledger.as_dict()


def test_crash_snapshot_resume_equals_continuous_dynamic_replay() -> None:
    trades = [_trade(f"t{index}", seconds=index, block=100 + index) for index in range(6)]
    order = replace(
        _v2_order(trades[1]),
        signal_source_trade_id=trades[1].trade_id,
        signal_source_tx_hash=trades[1].tx_hash,
        signal_source_log_indexes=trades[1].source_log_indexes,
    )
    catalog = InMemoryTradeCatalog(trades, source_pin="pinned")
    continuous = ReplaySession(
        run_id="resume", execution_family="V2", catalog=catalog, random_seed=73
    )
    continuous_results, _ = continuous.replay_dynamic(
        FrozenOrderStrategy([order]), chunk_size=1
    )

    interrupted = ReplaySession(
        run_id="resume", execution_family="V2", catalog=catalog, random_seed=73
    )
    interrupted.replay_dynamic(
        FrozenOrderStrategy([order]), chunk_size=1, max_chunks=2
    )
    first_rng_value = interrupted.rng.random()
    snapshot = interrupted.snapshot()
    expected_next_rng_value = interrupted.rng.random()

    resumed = ReplaySession(
        run_id="resume", execution_family="V2", catalog=catalog, random_seed=73
    )
    resumed.restore_snapshot(snapshot, strategy=FrozenOrderStrategy([order]))
    assert first_rng_value != expected_next_rng_value
    assert resumed.rng.random() == expected_next_rng_value
    resumed_results, _ = resumed.replay_dynamic(
        FrozenOrderStrategy([order]), chunk_size=1
    )

    assert [row.as_dict() for row in resumed_results] == [
        row.as_dict() for row in continuous_results
    ]
    assert resumed.v2_ledger.snapshot() == continuous.v2_ledger.snapshot()
    assert resumed.accounting.as_dict() == continuous.accounting.as_dict()
    assert resumed.high_watermark == continuous.high_watermark
