from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from quant.backtest.orderfilled_probability import (
    PROBABILITY_TARGET_FAK_ANY_FILL,
    PROBABILITY_TARGET_FOK_FULL_FILL,
    default_orderfilled_probability_profile,
)
from quant.backtest.orderfilled_v2_replay import V2TradePrint
from quant.backtest.prepared_trade_tape import prepare_trade_tape
from quant.backtest.rust_kernel import rust_kernel_available
from quant.backtest.trade_only_v3 import (
    LiquidityIntent,
    RunLiquidityLedger,
    TradeOnlyOrder,
    get_trade_only_profile,
    replay_trade_only_orders,
    replay_trade_only_orders_reference,
    replay_trade_only_orders_with_diagnostics,
)
from quant.backtest.trade_only_v3 import engine as v3_engine

BASE_TS = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def _trade(
    trade_id: str,
    *,
    seconds: int,
    block: int,
    side: str,
    price: str,
    size: str = "100",
) -> V2TradePrint:
    return V2TradePrint(
        trade_id=trade_id,
        market_id=42,
        condition_id="0xcondition",
        asset_id="asset-yes",
        outcome="YES",
        block_number=block,
        block_time=BASE_TS + timedelta(seconds=seconds),
        tx_hash=f"0x{trade_id}",
        tx_index=1,
        tx_index_source="receipt",
        price=Decimal(price),
        size=Decimal(size),
        notional=Decimal(price) * Decimal(size),
        aggressor_side=side,  # type: ignore[arg-type]
        passive_side="SELL" if side == "BUY" else "BUY",
        source_log_indexes=(1,),
        source_fill_count=1,
    )


def _order(**overrides) -> TradeOnlyOrder:
    values = {
        "order_id": "order-1",
        "market_id": 42,
        "asset_id": "asset-yes",
        "side": "BUY",
        "limit_price": Decimal("0.57"),
        "size": Decimal(10),
        "signal_block": 100,
        "signal_ts": BASE_TS,
        "latency": timedelta(seconds=1),
        "horizon": timedelta(seconds=30),
        "horizon_blocks": 30,
        "lookback": timedelta(minutes=5),
        "lookback_blocks": 100,
        "liquidity_intent": LiquidityIntent.TAKER,
    }
    values.update(overrides)
    return TradeOnlyOrder(**values)


def test_source_confirmed_adapter_preserves_real_trade_evidence() -> None:
    trades = [
        _trade("before", seconds=-2, block=99, side="BUY", price="0.55"),
        _trade("after", seconds=2, block=102, side="BUY", price="0.55"),
    ]

    results, _ = replay_trade_only_orders([_order()], trades, "taker_source_confirmed")

    result = results[0]
    assert result.status == "PARTIAL_FILLED"
    assert result.evidence_tier == "A_SOURCE_CONFIRMED"
    assert result.fills[0].source_trade_ids == ("after",)
    assert result.fills[0].exec_price == Decimal("0.5550000000")


def test_timestamp_native_order_without_block_matches_full_scan_and_index() -> None:
    trades = [
        _trade("before", seconds=-2, block=99, side="BUY", price="0.55"),
        _trade("after", seconds=2, block=102, side="BUY", price="0.55"),
    ]
    order = _order(signal_block=None)

    reference, reference_ledger = replay_trade_only_orders_reference(
        [order], trades, "taker_source_confirmed"
    )
    indexed, indexed_ledger = replay_trade_only_orders(
        [order], trades, "taker_source_confirmed"
    )

    assert [row.as_dict() for row in indexed] == [row.as_dict() for row in reference]
    assert indexed_ledger.as_dict() == reference_ledger.as_dict()
    assert indexed[0].fills[0].source_trade_ids == ("after",)


def test_passive_trade_through_uses_opposite_aggressor_and_limit_price() -> None:
    order = _order(liquidity_intent=LiquidityIntent.PASSIVE)
    trades = [
        _trade("wrong-side", seconds=2, block=102, side="BUY", price="0.56"),
        _trade("touch", seconds=3, block=103, side="SELL", price="0.57"),
        _trade("cross", seconds=4, block=104, side="SELL", price="0.56"),
    ]

    results, _ = replay_trade_only_orders([order], trades, "maker_trade_through_lower")

    fill = results[0].fills[0]
    assert fill.trigger_type == "STRICT_CROSS"
    assert fill.source_trade_ids == ("cross",)
    assert fill.exec_price == Decimal("0.5700000000")
    assert results[0].evidence_tier == "B_TRADE_THROUGH_INFERRED"


def test_passive_immediate_tif_is_unobservable_without_synthetic_arrival() -> None:
    order = _order(liquidity_intent=LiquidityIntent.PASSIVE, tif="IOC")

    results, _ = replay_trade_only_orders([order], [], "maker_trade_through_lower")

    assert results[0].status == "UNOBSERVABLE"
    assert results[0].reason == "UNOBSERVABLE_IMMEDIATE_LIQUIDITY"


def test_touch_survival_reports_interval_and_probability_bounds() -> None:
    order = _order(liquidity_intent=LiquidityIntent.PASSIVE)
    trades = [
        _trade("pre-hit", seconds=-2, block=99, side="SELL", price="0.58"),
        _trade("touch", seconds=2, block=102, side="SELL", price="0.57"),
    ]

    results, _ = replay_trade_only_orders(
        [order], trades, "maker_touch_survival_expected"
    )

    result = results[0]
    assert result.status == "PARTIAL_FILLED"
    assert result.probability_bounds["lower"] == Decimal("0E-10")
    assert result.probability_bounds["upper"] == Decimal("1.0000000000")
    assert result.fills[0].touch_ts == BASE_TS + timedelta(seconds=2)
    assert result.fills[0].cross_ts is None


def test_synthetic_arrival_can_model_fill_without_future_source_trade() -> None:
    trades = [
        _trade("pre-buy", seconds=-3, block=98, side="BUY", price="0.55"),
        _trade("pre-sell", seconds=-2, block=99, side="SELL", price="0.53"),
    ]
    order = _order(limit_price=Decimal("0.60"))

    results, _ = replay_trade_only_orders([order], trades, "taker_synthetic_q50")

    fill = results[0].fills[0]
    assert fill.evidence_tier == "D_SYNTHETIC_ARRIVAL"
    assert fill.trigger_type == "MODEL_INFERRED_AT_ARRIVAL"
    assert fill.source_trade_ids == ()
    assert fill.source_tx_hashes == ()
    assert fill.feature_snapshot_hash
    assert fill.capacity_q10 < fill.capacity_q50 < fill.capacity_q90


def test_synthetic_fok_is_atomic() -> None:
    trades = [
        _trade("pre-buy", seconds=-3, block=98, side="BUY", price="0.55"),
        _trade("pre-sell", seconds=-2, block=99, side="SELL", price="0.53"),
    ]
    small = _order(
        order_id="small", limit_price=Decimal("0.60"), size=Decimal(1), tif="FOK"
    )
    large = _order(
        order_id="large", limit_price=Decimal("0.60"), size=Decimal(10), tif="FOK"
    )

    results, _ = replay_trade_only_orders([small, large], trades, "taker_synthetic_q50")

    assert results[0].status == "FILLED"
    assert results[0].filled_size == Decimal("1.0000000000")
    assert results[1].status == "NO_FILL"
    assert results[1].filled_size == 0


def test_monte_carlo_is_repeatable_and_reports_distribution() -> None:
    trades = [
        _trade("pre-1", seconds=-5, block=96, side="BUY", price="0.55", size="20"),
        _trade("pre-2", seconds=-4, block=97, side="BUY", price="0.55", size="30"),
        _trade("pre-3", seconds=-3, block=98, side="SELL", price="0.54", size="20"),
        _trade("pre-4", seconds=-2, block=99, side="BUY", price="0.55", size="40"),
    ]
    order = _order(limit_price=Decimal("0.60"), random_seed=73, monte_carlo_paths=100)

    first, _ = replay_trade_only_orders([order], trades, "generative_tape_mc")
    second, _ = replay_trade_only_orders([order], trades, "generative_tape_mc")

    assert first[0].monte_carlo == second[0].monte_carlo
    assert first[0].monte_carlo["paths"] == 100
    assert first[0].status == "MODELED_DISTRIBUTION"
    assert first[0].monte_carlo["rng_algorithm"] == "XORSHIFT64_POISSON_CHUNK_V1"


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_monte_carlo_python_and_rust_backends_are_exact() -> None:
    trades = [
        _trade("pre-1", seconds=-5, block=96, side="BUY", price="0.55", size="20"),
        _trade("pre-2", seconds=-4, block=97, side="BUY", price="0.55", size="30"),
        _trade("pre-3", seconds=-3, block=98, side="SELL", price="0.54", size="20"),
        _trade("pre-4", seconds=-2, block=99, side="BUY", price="0.55", size="40"),
    ]
    order = _order(limit_price=Decimal("0.60"), random_seed=73, monte_carlo_paths=257)
    prepared = prepare_trade_tape(trades)

    python, python_ledger, python_diagnostics = (
        replay_trade_only_orders_with_diagnostics(
            [order],
            None,
            "generative_tape_mc",
            prepared_tape=prepared,
            backend="python",
        )
    )
    rust, rust_ledger, rust_diagnostics = replay_trade_only_orders_with_diagnostics(
        [order],
        None,
        "generative_tape_mc",
        prepared_tape=prepared,
        backend="rust",
    )

    assert [row.as_dict() for row in rust] == [row.as_dict() for row in python]
    assert rust_ledger.as_dict() == python_ledger.as_dict()
    assert python_diagnostics.matching_backend == "python"
    assert rust_diagnostics.matching_backend == "rust"


def test_hierarchical_expected_fill_separates_probability_and_capacity() -> None:
    trades = [
        _trade("pre-buy", seconds=-10, block=99, side="BUY", price="0.55"),
    ]
    order = _order(tif="GTD", limit_price=Decimal("0.57"))

    results, _ = replay_trade_only_orders(
        [order], trades, "taker_hierarchical_expected_30s"
    )

    result = results[0]
    fill = result.fills[0]
    assert result.status == "MODELED_EXPECTATION"
    assert fill.source_trade_ids == ()
    assert fill.exec_price == order.limit_price
    assert fill.unconditional_expected_fill_size == fill.filled_size
    assert fill.conditional_fill_size > fill.unconditional_expected_fill_size
    assert fill.p_fill_horizon < 1
    assert result.model_diagnostics["probability_training_rows"] == 23_982
    assert result.model_diagnostics["expected_fill_is_observed_execution"] is False


def test_hierarchical_expected_fill_horizons_use_independent_models() -> None:
    trades = [
        _trade("pre-buy", seconds=-10, block=99, side="BUY", price="0.55"),
    ]
    results = []
    for seconds in (30, 120, 300):
        order = _order(
            order_id=f"order-{seconds}",
            tif="GTD",
            horizon=timedelta(seconds=seconds),
            horizon_blocks=1_000,
        )
        replayed, _ = replay_trade_only_orders(
            [order], trades, f"taker_hierarchical_expected_{seconds}s"
        )
        results.append(replayed[0])

    assert [
        result.model_diagnostics["base_probability_horizon_seconds"]
        for result in results
    ] == ["30", "120", "300"]
    assert all(
        result.model_diagnostics["independently_trained_horizon"] is True
        for result in results
    )
    assert all("model_30s" not in result.probability_bounds for result in results)
    assert (
        results[2].calibration_status == "ORDERFILLED_PROXY_REVIEW_TRANSFER_UNVALIDATED"
    )


def test_central_router_prefers_source_then_falls_back_to_hierarchy() -> None:
    pre = _trade("pre", seconds=-2, block=99, side="BUY", price="0.55")
    future = _trade("future", seconds=2, block=102, side="BUY", price="0.55")
    order = _order(tif="GTD")

    source, _ = replay_trade_only_orders(
        [order], [pre, future], "central_trade_only_30s"
    )
    modeled, _ = replay_trade_only_orders([order], [pre], "central_trade_only_30s")

    assert source[0].evidence_tier == "A_SOURCE_CONFIRMED"
    assert source[0].model_diagnostics["selected_route"] == "taker_source_confirmed"
    assert modeled[0].status == "MODELED_EXPECTATION"
    assert (
        modeled[0].model_diagnostics["selected_route"] == "taker_hierarchical_expected"
    )
    assert modeled[0].model_diagnostics["prior_attempt_status"] == "NO_FILL"
    assert (
        modeled[0].model_diagnostics["price_buffer_resolution"]["fallback_used"] is True
    )


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_central_router_batch_matches_legacy_order_loop_and_rust_source_route() -> None:
    trades = [
        _trade("pre-a", seconds=-5, block=95, side="BUY", price="0.54", size="40"),
        _trade("pre-b", seconds=-2, block=98, side="SELL", price="0.55", size="30"),
        _trade("future-a", seconds=2, block=102, side="BUY", price="0.55", size="200"),
        _trade("future-b", seconds=5, block=105, side="BUY", price="0.56", size="200"),
    ]
    orders = [
        _order(order_id=f"central-{index}", tif="GTD", size=Decimal(3))
        for index in range(6)
    ]
    profile = get_trade_only_profile("central_trade_only_30s")
    prepared = prepare_trade_tape(trades)

    legacy_ledger = RunLiquidityLedger("central-parity")
    context = v3_engine._TradeReplayContext((), prepared=prepared, indexed=True)
    legacy = [
        v3_engine._central_router(order, context, profile, legacy_ledger)
        for order in orders
    ]
    python, python_ledger, python_diagnostics = (
        replay_trade_only_orders_with_diagnostics(
            orders,
            None,
            profile,
            ledger_id="central-parity",
            prepared_tape=prepared,
            backend="python",
        )
    )
    rust, rust_ledger, rust_diagnostics = replay_trade_only_orders_with_diagnostics(
        orders,
        None,
        profile,
        ledger_id="central-parity",
        prepared_tape=prepared,
        backend="rust",
    )

    assert [row.as_dict() for row in python] == [row.as_dict() for row in legacy]
    assert python_ledger.snapshot() == legacy_ledger.snapshot()
    assert [row.as_dict() for row in rust] == [row.as_dict() for row in python]
    assert rust_ledger.snapshot() == python_ledger.snapshot()
    assert python_diagnostics.matching_backend == "python"
    assert rust_diagnostics.matching_backend == "hybrid_rust_python"


def test_indexed_v3_matches_full_scan_for_every_profile() -> None:
    trades = [
        _trade("pre-buy", seconds=-5, block=96, side="BUY", price="0.55", size="20"),
        _trade("pre-sell", seconds=-4, block=97, side="SELL", price="0.54", size="30"),
        _trade("touch", seconds=2, block=102, side="SELL", price="0.57", size="40"),
        _trade("cross", seconds=3, block=103, side="SELL", price="0.56", size="50"),
        _trade("future-buy", seconds=4, block=104, side="BUY", price="0.55", size="60"),
    ]
    taker = _order(tif="GTD", random_seed=73, monte_carlo_paths=50)
    passive = _order(
        tif="GTD",
        liquidity_intent=LiquidityIntent.PASSIVE,
        random_seed=73,
        monte_carlo_paths=50,
    )
    profiles = {
        "taker_source_confirmed": taker,
        "maker_trade_through_lower": passive,
        "maker_touch_survival_conservative": passive,
        "maker_touch_survival_expected": passive,
        "maker_touch_survival_upper": passive,
        "taker_synthetic_q10": taker,
        "taker_synthetic_q50": taker,
        "taker_synthetic_q90": taker,
        "generative_tape_mc": taker,
        "taker_hierarchical_expected_30s": taker,
        "taker_hierarchical_expected_120s": replace(
            taker, horizon=timedelta(seconds=120), horizon_blocks=120
        ),
        "taker_hierarchical_expected_300s": replace(
            taker, horizon=timedelta(seconds=300), horizon_blocks=300
        ),
        "central_trade_only_30s": taker,
        "central_trade_only_120s": replace(
            taker, horizon=timedelta(seconds=120), horizon_blocks=120
        ),
        "central_trade_only_300s": replace(
            taker, horizon=timedelta(seconds=300), horizon_blocks=300
        ),
        "taker_tif_aware_expected_5s": replace(
            taker,
            tif="FAK",
            horizon=timedelta(seconds=5),
            horizon_blocks=15,
            category="sports",
            league="nba",
        ),
        "central_trade_only_tif_aware_5s": replace(
            taker,
            tif="FAK",
            horizon=timedelta(seconds=5),
            horizon_blocks=15,
            category="sports",
            league="nba",
        ),
        "central_trade_only_tif_aware_5s_recall": replace(
            taker,
            tif="FAK",
            horizon=timedelta(seconds=5),
            horizon_blocks=15,
            category="sports",
            league="nba",
        ),
        "auto_bound": replace(taker, liquidity_intent=LiquidityIntent.AUTO_BOUND),
    }

    for profile, order in profiles.items():
        reference, reference_ledger = replay_trade_only_orders_reference(
            [order], trades, profile
        )
        indexed, indexed_ledger = replay_trade_only_orders([order], trades, profile)

        assert [row.as_dict() for row in indexed] == [
            row.as_dict() for row in reference
        ], profile
        assert indexed_ledger.as_dict() == reference_ledger.as_dict(), profile


def test_indexed_v3_reuses_prepared_tape_and_skips_unrelated_rows() -> None:
    related = [
        _trade("pre", seconds=-2, block=99, side="BUY", price="0.55"),
        _trade("future", seconds=2, block=102, side="BUY", price="0.55"),
    ]
    unrelated = [
        replace(
            _trade(
                f"other-{index}",
                seconds=index,
                block=200 + index,
                side="BUY",
                price="0.55",
            ),
            market_id=9000 + index,
            asset_id=f"other-{index}",
        )
        for index in range(100)
    ]
    prepared = prepare_trade_tape([*related, *unrelated])

    results, _, diagnostics = replay_trade_only_orders_with_diagnostics(
        [_order(tif="GTD")],
        None,
        "taker_source_confirmed",
        prepared_tape=prepared,
    )

    assert results[0].filled_size > 0
    assert diagnostics.index_reused is True
    assert diagnostics.index_build_sec == 0
    assert diagnostics.candidate_rows_scanned == 1
    assert diagnostics.naive_rows_scanned == 102
    assert diagnostics.scan_reduction_ratio < Decimal("0.02")


@pytest.mark.skipif(
    not rust_kernel_available(), reason="Rust extension is not installed"
)
def test_v3_source_confirmed_python_rust_batch_parity() -> None:
    trades = [
        _trade(
            f"source-{index}",
            seconds=index,
            block=100 + index,
            side="BUY" if index % 2 == 0 else "SELL",
            price="0.55",
            size="100",
        )
        for index in range(120)
    ]
    orders = [
        _order(
            order_id=f"order-{index}",
            signal_block=100 + index,
            signal_ts=BASE_TS + timedelta(seconds=index),
            tif="GTD",
            horizon=timedelta(seconds=20),
            horizon_blocks=20,
            signal_source_trade_id=f"source-{index}",
        )
        for index in range(60)
    ]
    prepared = prepare_trade_tape(trades)

    python_results, python_ledger, python_diagnostics = (
        replay_trade_only_orders_with_diagnostics(
            orders,
            None,
            "taker_source_confirmed",
            prepared_tape=prepared,
            backend="python",
        )
    )
    rust_results, rust_ledger, rust_diagnostics = (
        replay_trade_only_orders_with_diagnostics(
            orders,
            None,
            "taker_source_confirmed",
            prepared_tape=prepared,
            backend="rust",
        )
    )

    assert [result.as_dict() for result in rust_results] == [
        result.as_dict() for result in python_results
    ]
    assert rust_ledger.snapshot() == python_ledger.snapshot()
    assert rust_diagnostics.matching_backend == "rust"
    assert (
        rust_diagnostics.candidate_rows_scanned
        <= python_diagnostics.candidate_rows_scanned
    )


def test_hierarchical_expected_fill_requires_active_pre_arrival_tape() -> None:
    results, _ = replay_trade_only_orders(
        [_order(tif="GTD")], [], "taker_hierarchical_expected_30s"
    )

    assert results[0].status == "NO_FILL"
    assert (
        results[0].reason
        == "insufficient_pre_arrival_tape_for_hierarchical_expected_fill"
    )


def test_v3_liquidity_ledger_delta_snapshot_and_pruning() -> None:
    trade = _trade("source", seconds=2, block=102, side="BUY", price="0.55")
    order = _order(tif="GTD")
    ledger = RunLiquidityLedger("ledger")
    ledger.begin_delta_tracking()
    ledger.consume_source(trade, Decimal("1.25"))
    ledger.consume_synthetic(order, Decimal("0.50"))

    delta = ledger.drain_delta()
    restored = RunLiquidityLedger.from_snapshot(ledger.snapshot())

    assert delta["inferred_source_capacity"] == {"source": "1.2500000000"}
    assert len(delta["synthetic_arrival_capacity"]) == 1
    assert restored.as_dict() == ledger.as_dict()
    pruning = restored.prune(
        source_trade_ids=(),
        synthetic_not_before_epoch=int(order.arrival_ts.timestamp()) + 1,
    )
    assert pruning["inferred_source_keys_dropped"] == 1
    assert pruning["synthetic_keys_dropped"] == 1


def test_hierarchical_expected_fill_rejects_immediate_tif() -> None:
    trades = [
        _trade("pre-buy", seconds=-10, block=99, side="BUY", price="0.55"),
    ]

    results, _ = replay_trade_only_orders(
        [_order(tif="FOK")], trades, "taker_hierarchical_expected_30s"
    )

    assert results[0].status == "UNSUPPORTED"
    assert results[0].reason == "hierarchical_expected_fill_requires_gtc_or_gtd"


def test_tif_aware_expected_supports_immediate_fak_and_atomic_fok() -> None:
    fak, _ = replay_trade_only_orders(
        [_order(tif="FAK", category="sports", league="nba")],
        [],
        "taker_tif_aware_expected_5s",
    )
    fok, _ = replay_trade_only_orders(
        [_order(tif="FOK", category="sports", league="nba")],
        [],
        "taker_tif_aware_expected_5s",
    )

    fak_result = fak[0]
    fok_result = fok[0]
    assert fak_result.status == "MODELED_EXPECTATION"
    assert fak_result.filled_size > 0
    assert fak_result.fills[0].trigger_type == "MODELED_EXPECTED_IMMEDIATE_FILL"
    assert fak_result.fills[0].fill_ts == _order().arrival_ts
    assert fak_result.model_diagnostics["prior_only_used"] is True
    assert fak_result.model_diagnostics["time_scaling"] == (
        "CONSTANT_HAZARD_FROM_TRAINED_HORIZON"
    )
    assert fak_result.model_diagnostics["expected_fill_is_observed_execution"] is False
    assert fok_result.status == "MODELED_EXPECTATION"
    assert fok_result.probability_bounds["full_fill_proxy"] > 0
    assert fok_result.model_diagnostics["tif_execution_contract"] == (
        "ATOMIC_ZERO_OR_FULL_EXPECTATION"
    )
    assert fok_result.fills[0].conditional_fill_size == Decimal("10.0000000000")
    assert fok_result.fills[0].conditional_fill_fraction == Decimal("1.0000000000")


def _contract_artifact(target: str, *, intercept: str) -> dict:
    artifact = default_orderfilled_probability_profile().as_dict()
    artifact.update(
        {
            "intercept": intercept,
            "coefficients": {name: "0" for name in artifact["coefficients"]},
            "calibration_x": [],
            "calibration_y": [],
            "model_contract": {
                "probability_target": target,
                "supported_sides": ["BUY", "SELL"],
                "supported_tifs": [
                    "FOK" if target == PROBABILITY_TARGET_FOK_FULL_FILL else "FAK"
                ],
                "supported_amount_units": ["SHARES"],
                "minimum_order_size": "1",
                "maximum_order_size": "20",
                "minimum_log_order_to_tape_ratio": "0",
                "maximum_log_order_to_tape_ratio": "30",
            },
        }
    )
    return artifact


def test_contract_aware_fok_uses_direct_full_fill_probability(tmp_path) -> None:
    fak_path = tmp_path / "fak.json"
    fok_path = tmp_path / "fok.json"
    fak_path.write_text(
        json.dumps(_contract_artifact(PROBABILITY_TARGET_FAK_ANY_FILL, intercept="0")),
        encoding="utf-8",
    )
    fok_path.write_text(
        json.dumps(
            _contract_artifact(
                PROBABILITY_TARGET_FOK_FULL_FILL,
                intercept="1.3862943611198906",
            )
        ),
        encoding="utf-8",
    )
    profile = replace(
        get_trade_only_profile("central_trade_only_contract_aware"),
        probability_profile_path=str(fak_path),
        full_fill_probability_profile_path=str(fok_path),
        price_buffer_profile_path=None,
    )

    results, _ = replay_trade_only_orders([_order(tif="FOK")], [], profile)

    result = results[0]
    assert result.status == "MODELED_EXPECTATION"
    assert result.probability_bounds["full_fill_probability"] == Decimal("0.8000000000")
    assert "full_fill_proxy" not in result.probability_bounds
    assert result.filled_size == Decimal("8.0000000000")
    assert result.model_diagnostics["probability_contract"] == "DIRECT_P_FULL_FILL"


def test_contract_aware_profile_abstains_outside_target_and_size_domain(
    tmp_path,
) -> None:
    wrong_path = tmp_path / "wrong.json"
    wrong_path.write_text(
        json.dumps(_contract_artifact(PROBABILITY_TARGET_FAK_ANY_FILL, intercept="0")),
        encoding="utf-8",
    )
    profile = replace(
        get_trade_only_profile("central_trade_only_contract_aware"),
        probability_profile_path=str(wrong_path),
        full_fill_probability_profile_path=str(wrong_path),
        price_buffer_profile_path=None,
    )

    target_mismatch, _ = replay_trade_only_orders([_order(tif="FOK")], [], profile)
    size_mismatch, _ = replay_trade_only_orders(
        [_order(tif="FAK", size=Decimal(25))], [], profile
    )

    assert target_mismatch[0].status == "MODEL_DOMAIN_ABSTAIN"
    assert "probability_target" in " ".join(
        target_mismatch[0].model_diagnostics["contract_violations"]
    )
    assert size_mismatch[0].status == "MODEL_DOMAIN_ABSTAIN"
    assert size_mismatch[0].model_diagnostics["contract_violations"] == [
        "order_size_above_training_domain"
    ]


def test_probability_artifact_checksum_mismatch_fails_closed(tmp_path) -> None:
    artifact_path = tmp_path / "fak.json"
    artifact_path.write_text(
        json.dumps(_contract_artifact(PROBABILITY_TARGET_FAK_ANY_FILL, intercept="0")),
        encoding="utf-8",
    )
    profile = replace(
        get_trade_only_profile("central_trade_only_contract_aware"),
        probability_profile_path=str(artifact_path),
        probability_profile_sha256="0" * 64,
        price_buffer_profile_path=None,
    )

    with pytest.raises(ValueError, match="probability artifact checksum mismatch"):
        replay_trade_only_orders([_order(tif="FAK")], [], profile)


def test_tif_aware_prior_only_fails_closed_without_supported_artifact() -> None:
    profile = replace(
        get_trade_only_profile("taker_tif_aware_expected_5s"),
        hierarchical_prior_path="config/execution/does-not-exist.json",
    )

    results, _ = replay_trade_only_orders([_order(tif="FAK")], [], profile)

    assert results[0].status == "NO_FILL"
    assert results[0].reason == (
        "insufficient_supported_prior_for_hierarchical_expected_fill"
    )


def test_tif_aware_probability_threshold_rejects_weak_modeled_opportunity() -> None:
    profile = replace(
        get_trade_only_profile("taker_tif_aware_expected_5s"),
        minimum_modeled_probability=Decimal("0.10"),
    )

    results, _ = replay_trade_only_orders(
        [_order(tif="FAK", category="sports", league="nba")], [], profile
    )

    assert results[0].status == "MODELED_EXPECTATION"
    assert results[0].filled_size == 0
    assert results[0].reason == "modeled_probability_below_profile_threshold"
    assert results[0].model_diagnostics["probability_admission_passed"] is False


def test_l2_reference_profile_uses_artifact_threshold_and_empty_tape(
    tmp_path,
) -> None:
    artifact = default_orderfilled_probability_profile().as_dict()
    artifact.update(
        {
            "min_probability": "0.60",
            "intercept": "0",
            "coefficients": {name: "0" for name in artifact["coefficients"]},
            "calibration_x": [],
            "calibration_y": [],
        }
    )
    path = tmp_path / "probability.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    profile = replace(
        get_trade_only_profile("central_trade_only_l2_reference_fak"),
        probability_profile_path=str(path),
        price_buffer_profile_path=None,
    )

    rejected, _ = replay_trade_only_orders([_order(tif="FAK")], [], profile)
    admitted_profile = replace(profile, use_probability_profile_threshold=False)
    admitted, _ = replay_trade_only_orders([_order(tif="FAK")], [], admitted_profile)

    assert rejected[0].filled_size == 0
    assert rejected[0].model_diagnostics["minimum_modeled_probability"] == "0.60"
    assert rejected[0].model_diagnostics["probability_threshold_source"] == (
        "PROBABILITY_ARTIFACT"
    )
    assert rejected[0].model_diagnostics["probability_blend_contract"] == (
        "DIRECT_ORDERFILLED_MODEL"
    )
    assert admitted[0].filled_size > 0


def test_l2_reference_profile_reloads_when_artifact_changes(tmp_path) -> None:
    artifact = default_orderfilled_probability_profile().as_dict()
    artifact["model_version"] = "reload-v1"
    path = tmp_path / "probability.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    profile = replace(
        get_trade_only_profile("central_trade_only_l2_reference_expected_fak"),
        probability_profile_path=str(path),
        price_buffer_profile_path=None,
    )

    first, _ = replay_trade_only_orders([_order(tif="FAK")], [], profile)
    first_mtime = path.stat().st_mtime_ns
    artifact["model_version"] = "reload-v2"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    os.utime(path, ns=(first_mtime + 1, first_mtime + 1))
    second, _ = replay_trade_only_orders([_order(tif="FAK")], [], profile)

    assert first[0].model_diagnostics["probability_model_version"] == "reload-v1"
    assert second[0].model_diagnostics["probability_model_version"] == "reload-v2"


def test_l2_reference_profile_abstains_out_of_domain(tmp_path) -> None:
    artifact = default_orderfilled_probability_profile().as_dict()
    artifact.update(
        {
            "min_probability": "0",
            "intercept": "10",
            "coefficients": {name: "0" for name in artifact["coefficients"]},
            "calibration_x": [],
            "calibration_y": [],
            "domain_gate": {"abstain_category_families": ["esports"]},
        }
    )
    path = tmp_path / "probability.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    profile = replace(
        get_trade_only_profile("central_trade_only_l2_reference_fak"),
        probability_profile_path=str(path),
        price_buffer_profile_path=None,
    )

    results, _ = replay_trade_only_orders(
        [
            _order(
                tif="FAK",
                category="league-of-legends",
                league="lol",
            )
        ],
        [],
        profile,
    )

    assert results[0].status == "MODELED_EXPECTATION"
    assert results[0].filled_size == 0
    assert results[0].reason == "orderfilled_probability_model_out_of_domain"
    assert results[0].model_diagnostics["abstain_category_families"] == ["esports"]


def test_l2_reference_profile_abstains_for_exact_category(tmp_path) -> None:
    artifact = default_orderfilled_probability_profile().as_dict()
    artifact.update(
        {
            "min_probability": "0",
            "intercept": "10",
            "coefficients": {name: "0" for name in artifact["coefficients"]},
            "calibration_x": [],
            "calibration_y": [],
            "domain_gate": {"abstain_categories": ["french-election"]},
        }
    )
    path = tmp_path / "probability.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    profile = replace(
        get_trade_only_profile("central_trade_only_l2_reference_fak"),
        probability_profile_path=str(path),
        price_buffer_profile_path=None,
    )

    results, _ = replay_trade_only_orders(
        [_order(tif="FAK", category="French Election")], [], profile
    )

    assert results[0].status == "MODELED_EXPECTATION"
    assert results[0].filled_size == 0
    assert results[0].reason == "orderfilled_probability_model_out_of_domain"
    assert results[0].model_diagnostics["abstain_categories"] == ["french-election"]


def test_l2_reference_expected_profile_does_not_binary_threshold() -> None:
    thresholded, _ = replay_trade_only_orders(
        [_order(tif="FAK", category="politics")],
        [],
        "central_trade_only_l2_reference_fak",
    )
    expected, _ = replay_trade_only_orders(
        [_order(tif="FAK", category="politics")],
        [],
        "central_trade_only_l2_reference_expected_fak",
    )

    assert thresholded[0].status == "MODELED_EXPECTATION"
    assert expected[0].status == "MODELED_EXPECTATION"
    assert expected[0].model_diagnostics["minimum_modeled_probability"] == "0"
    assert expected[0].model_diagnostics["probability_threshold_source"] == (
        "TRADE_ONLY_PROFILE"
    )
    assert expected[0].filled_size > 0


def test_l2_reference_expected_adds_separate_modeled_residual_to_source(
    tmp_path,
) -> None:
    artifact = default_orderfilled_probability_profile().as_dict()
    artifact.update(
        {
            "intercept": "0",
            "coefficients": {name: "0" for name in artifact["coefficients"]},
            "calibration_x": [],
            "calibration_y": [],
            "conditional_capacity": {
                "mode": "conditional_source_capacity",
                "default_variant": "expected",
                "models": {
                    variant: {
                        "name": variant,
                        "target": "test",
                        "floor": "0",
                        "intercept": value,
                        "coefficients": {
                            name: "0" for name in artifact["coefficients"]
                        },
                        "training_rows": 100,
                    }
                    for variant, value in (
                        ("expected", "0.9"),
                        ("conservative", "0.8"),
                    )
                },
            },
        }
    )
    path = tmp_path / "probability.json"
    path.write_text(json.dumps(artifact), encoding="utf-8")
    profile = replace(
        get_trade_only_profile("central_trade_only_l2_reference_expected_fak"),
        probability_profile_path=str(path),
        price_buffer_profile_path=None,
    )
    order = _order(tif="FAK")
    trades = [_trade("source", seconds=2, block=101, side="BUY", price="0.55")]

    source, _ = replay_trade_only_orders([order], trades, "taker_source_confirmed")
    expected, _ = replay_trade_only_orders([order], trades, profile)

    assert source[0].filled_size == Decimal("2.5000000000")
    assert expected[0].status == "MODELED_EXPECTATION"
    assert expected[0].filled_size == Decimal("9.0000000000")
    assert expected[0].evidence_tier == (
        "MIXED_A_SOURCE_CONFIRMED_D_MODELED_EXPECTATION"
    )
    assert [fill.evidence_tier for fill in expected[0].fills] == [
        "A_SOURCE_CONFIRMED",
        "D_SYNTHETIC_ARRIVAL",
    ]
    assert [fill.filled_size for fill in expected[0].fills] == [
        Decimal("2.5000000000"),
        Decimal("6.5000000000"),
    ]
    assert expected[0].model_diagnostics["source_residual_contract"] == (
        "MAX_SOURCE_LOWER_BOUND_AND_CONDITIONAL_EXPECTED_TOTAL"
    )


def test_tif_aware_orders_share_modeled_capacity_at_same_arrival() -> None:
    orders = [
        _order(
            order_id=f"modeled-{index}",
            tif="FAK",
            category="sports",
            league="nba",
        )
        for index in range(150)
    ]

    results, _ = replay_trade_only_orders(orders, [], "taker_tif_aware_expected_5s")

    total = sum((result.filled_size for result in results), Decimal(0))
    first_pool = results[0].capacity_bounds["conditional_expected"]
    assert total <= first_pool
    assert any(result.filled_size == 0 for result in results)


def test_auto_bound_keeps_scenarios_separate() -> None:
    trades = [
        _trade("pre-buy", seconds=-3, block=98, side="BUY", price="0.55"),
        _trade("pre-sell", seconds=-2, block=99, side="SELL", price="0.53"),
    ]

    results, _ = replay_trade_only_orders(
        [_order(limit_price=Decimal("0.60"))], trades, "auto_bound"
    )

    result = results[0]
    assert result.status == "BOUND_SET"
    assert set(result.scenarios) == {
        "strict_lower",
        "expected_modeled",
        "synthetic_upper",
        "bound_diagnostics",
    }
    assert result.filled_size == 0
    assert result.scenarios["bound_diagnostics"]["ordering_valid"] is True
    assert result.reason == "scenario_results_must_be_reported_separately"


def test_auto_bound_rejects_unordered_uncalibrated_scenarios() -> None:
    trades = [
        _trade("pre-buy", seconds=-3, block=98, side="BUY", price="0.55"),
        _trade("pre-sell", seconds=-2, block=99, side="SELL", price="0.53"),
        _trade(
            "future-buy", seconds=2, block=102, side="BUY", price="0.55", size="1000"
        ),
    ]

    results, _ = replay_trade_only_orders(
        [_order(limit_price=Decimal("0.60"))], trades, "auto_bound"
    )

    result = results[0]
    assert result.status == "BOUND_ORDERING_VIOLATION"
    assert result.filled_size == 0
    assert result.scenarios["bound_diagnostics"]["ordering_valid"] is False
