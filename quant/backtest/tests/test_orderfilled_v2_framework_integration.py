from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from quant.backtest import backtest_engine as engine
from quant.backtest.backtest_engine import BacktestParameters, PricePoint
from quant.backtest.orders import order_status_from_fill
from quant.backtest.orderfilled_v2_replay import V2Fill, V2OrderResult
from quant.backtest.pml2.financial import default_execution_adapter_registry
from quant.backtest.trade_only_v3 import TradeOnlyFill, TradeOnlyOrderResult


def _point(block: int, price: str) -> PricePoint:
    return PricePoint(
        x_value=block,
        price=Decimal(price),
        volume=Decimal("100"),
        trade_count=1,
        timestamp=datetime(2026, 1, 1, 0, 0, block, tzinfo=timezone.utc),
    )


def _result_for_order(order) -> V2OrderResult:
    price = Decimal("0.60") if order.side == "BUY" else Decimal("0.40")
    fill = V2Fill(
        order_id=order.order_id,
        fill_ts=order.signal_ts,
        fill_block=order.arrival_block or order.signal_block or 0,
        side=order.side,
        limit_price=order.limit_price,
        filled_size=order.size,
        exec_price=price,
        source_trade_id=f"trade-{order.order_id}",
        source_tx_hash="0xabc",
        source_log_indexes=(1,),
        historical_price=price,
        historical_size=Decimal("100"),
        price_buffer_paid=Decimal("0"),
        participation_rate=order.participation_rate,
        allocated_capacity=order.size,
        tx_index_source="test",
    )
    return V2OrderResult(
        order_id=order.order_id,
        status="FILLED",
        side=order.side,
        requested_size=order.size,
        filled_size=order.size,
        unfilled_size=Decimal("0"),
        avg_price=price,
        limit_price=order.limit_price,
        arrival_block=order.arrival_block,
        arrival_ts=order.arrival_ts,
        eligible_historical_volume=Decimal("100"),
        simulated_volume=order.size,
        participation_rate=order.participation_rate,
        capacity_utilization=Decimal("0.1"),
        avg_fill_delay_seconds=Decimal("0"),
        avg_price_buffer=Decimal("0"),
        filled_notional=order.size * price,
        cash_delta=-(order.size * price) if order.side == "BUY" else order.size * price,
        position_delta=order.size if order.side == "BUY" else -order.size,
        reason_unfilled="",
        fills=(fill,),
    )


def test_orderfilled_v2_tape_mode_runs_through_builtin_strategy(monkeypatch) -> None:
    monkeypatch.setattr(
        engine,
        "load_v2_trade_slices_for_orders",
        lambda orders, **_: SimpleNamespace(trades=(), as_dict=lambda: {"rows_loaded": 1}),
    )
    monkeypatch.setattr(
        engine,
        "replay_v2_taker_orders_with_diagnostics",
        lambda orders, trades, ledger=None: (
            [_result_for_order(list(orders)[0])],
            ledger,
            SimpleNamespace(as_dict=lambda: {"orders_count": 1, "trade_rows_indexed": 1}),
        ),
    )

    params = BacktestParameters(
        entry_threshold=Decimal("0.58"),
        exit_threshold=Decimal("0.44"),
        position_size=Decimal("10"),
        execution_price_mode="ORDERFILLED_V2_TAPE",
        execution_profile="realistic",
        buy_limit_price=Decimal("1"),
        sell_limit_price=Decimal("0"),
        latency_blocks=1,
        cancel_after_blocks=10,
    )
    run = {
        "market_slug": "demo-market",
        "token_side": "YES",
        "price_source": "orderfilled_block_close",
        "_orderfilled_v2_token_context": {
            "market_id": 1,
            "token_id_hex": "0xtoken",
        },
    }

    result = engine.simulate_strategy([_point(1, "0.60"), _point(2, "0.70"), _point(3, "0.40")], run, params)

    assert len(result["orders"]) == 2
    assert len(result["trades"]) == 1
    assert result["orders"][0]["execution_source"] == "orderfilled_v2_trade_tape"
    assert result["orders"][0]["execution_evidence_type"] == "raw_orderfilled"
    assert result["orderfilled_v2"]["summary"]["attempted_orders"] == 2


def test_orderfilled_only_alias_normalizes_to_v2_tape_mode() -> None:
    assert engine.normalize_execution_price_mode("orderfilled-only-trade-tape") == "ORDERFILLED_V2_TAPE"
    assert engine.normalize_execution_price_mode("ORDERFILLED_ONLY") == "ORDERFILLED_V2_TAPE"


def test_v2_research_profiles_route_to_orderfilled_probability() -> None:
    for profile in ("realistic", "primary_calibrated", "probabilistic_trade_tape", "orderfilled_probability"):
        params = engine.BacktestParameters(execution_profile=profile)
        assert engine._v2_profile_name(params) == "probabilistic_trade_tape"
    for profile in ("primary_conservative", "probabilistic_conservative", "probability_conservative"):
        params = engine.BacktestParameters(execution_profile=profile)
        assert engine._v2_profile_name(params) == "probabilistic_conservative"
    params = engine.BacktestParameters(execution_profile="probability_source_confirmed")
    assert engine._v2_profile_name(params) == "probabilistic_source_confirmed"


def test_v2_no_fill_status_stays_no_fill_not_rejected() -> None:
    assert order_status_from_fill({"fill_status": "NO_FILL", "rejected": True, "notes": ["insufficient_post_arrival_same_side_orderfilled_capacity"]}) == "NO_FILL"


def _v3_result_for_order(order) -> TradeOnlyOrderResult:
    price = Decimal("0.60") if order.side == "BUY" else Decimal("0.40")
    source_size = Decimal("4") if order.side == "BUY" else order.size
    source = TradeOnlyFill(
        order_id=order.order_id,
        execution_mode="CENTRAL_ROUTER",
        evidence_tier="A_SOURCE_CONFIRMED",
        trigger_type="SOURCE_CONFIRMED",
        filled_size=source_size,
        exec_price=price,
        fill_ts=order.arrival_ts,
        fill_block=order.arrival_block,
        source_trade_ids=(f"source-{order.order_id}",),
        source_tx_hashes=("0xabc",),
        source_log_indexes=(1,),
        participation_rate=Decimal("0.025"),
    )
    fills = [source]
    status = "FILLED"
    reason = "source_confirmed"
    evidence_tier = "A_SOURCE_CONFIRMED"
    if order.side == "BUY":
        fills.append(
            TradeOnlyFill(
                order_id=order.order_id,
                execution_mode="CENTRAL_ROUTER",
                evidence_tier="D_SYNTHETIC_ARRIVAL",
                trigger_type="MODELED_EXPECTED_IMMEDIATE_FILL",
                filled_size=order.size - source_size,
                exec_price=price,
                fill_ts=order.arrival_ts,
                fill_block=None,
                p_fill_1s=Decimal("0.6"),
                p_fill_horizon=Decimal("0.6"),
                participation_rate=Decimal("0.025"),
            )
        )
        status = "MODELED_EXPECTATION"
        reason = "source_confirmed_lower_bound_plus_modeled_residual_expectation"
        evidence_tier = "MIXED_A_SOURCE_CONFIRMED_D_MODELED_EXPECTATION"
    return TradeOnlyOrderResult(
        order_id=order.order_id,
        status=status,
        requested_size=order.size,
        filled_size=order.size,
        unfilled_size=Decimal("0"),
        avg_price=price,
        reason=reason,
        execution_mode="CENTRAL_ROUTER",
        evidence_tier=evidence_tier,
        result_role="CENTRAL_EXPECTED_LOCAL_RESEARCH",
        calibration_status="TEST",
        fills=tuple(fills),
    )


def test_fill_only_v3_mode_runs_through_main_strategy_with_separate_actual_and_expected(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        engine,
        "_execute_v3_order",
        lambda order, capacity, context: (
            context["results"].append(_v3_result_for_order(order))
            or context["results"][-1]
        ),
    )
    params = BacktestParameters(
        entry_threshold=Decimal("0.58"),
        exit_threshold=Decimal("0.44"),
        position_size=Decimal("10"),
        execution_price_mode="ORDERFILLED_V3_TRADE",
        execution_profile="realistic",
        buy_limit_price=Decimal("1"),
        sell_limit_price=Decimal("0"),
        latency_blocks=1,
    )
    run = {
        "run_id": 99,
        "market_slug": "demo-market",
        "token_side": "YES",
        "price_source": "orderfilled_block_close",
        "_orderfilled_v2_token_context": {
            "market_id": 1,
            "token_id_hex": "0xtoken",
            "market_slug": "demo-market",
            "category": "sports",
        },
    }

    result = engine.simulate_strategy(
        [_point(1, "0.60"), _point(2, "0.70"), _point(3, "0.40")],
        run,
        params,
    )

    entry = result["orders"][0]
    assert entry["status"] == "MODELED_EXPECTATION"
    assert entry["expected_fill_size"] == Decimal("10.0000000000")
    assert entry["actual_fill_size"] == Decimal("4.0000000000")
    assert entry["execution_evidence_type"] == "mixed_orderfilled_and_model"
    assert entry["meta"]["time_in_force"] == "FAK"
    assert entry["meta"]["fill_probability_haircut_pct"] == Decimal("0")
    assert entry["meta"]["effective_liquidity_cap_pct"] == Decimal("2.5000000000")
    assert result["fill_only_v3"]["summary"]["mixed_orders"] == 1
    assert result["fill_only_v3"]["summary"]["expected_fill_size"] == Decimal("20.0000000000")
    assert result["fill_only_v3"]["summary"]["actual_fill_size"] == Decimal("14.0000000000")
    assert [row["event_type"] for row in result["ledger"]] == ["BUY", "SELL"]
    assert [row["shares_delta"] for row in result["ledger"]] == [
        Decimal("4.0000000000"),
        Decimal("-4.0000000000"),
    ]
    assert result["ledger"][-1]["meta"]["inventory_capped"] is True
    evidence = default_execution_adapter_registry().extract(entry["meta"])
    assert [item.source_fill_id for item in evidence] == ["source-O-0001"]


def test_fill_only_v3_alias_and_realistic_profile_are_explicit() -> None:
    assert engine.normalize_execution_price_mode("fill-only-v3") == "ORDERFILLED_V3_TRADE"
    assert engine._v3_profile_name("realistic") == "central_trade_only_l2_reference_expected_fak"
