from decimal import Decimal

import pytest

from quant.backtest.backtest_engine import BacktestParameters, PricePoint
from quant.backtest.backtest_engine import _fill_decision as engine_fill_decision
from quant.backtest.frameworks import _fill_decision as framework_fill_decision
from quant.backtest.orderfilled_execution import (
    execution_price,
    fee_rebate_for_notional,
    orderfilled_fill_decision,
    role_fee_bps,
    role_rebate_bps,
    target_notional,
)


pytestmark = pytest.mark.backtest_validation


def test_orderfilled_execution_helpers_are_shared_for_engine_math() -> None:
    params = BacktestParameters(
        position_size=Decimal("25"),
        max_position_notional=Decimal("20"),
        slippage_bps=Decimal("100"),
        fee_bps=Decimal("5"),
        maker_fee_bps=Decimal("2"),
        taker_fee_bps=Decimal("8"),
        maker_rebate_bps=Decimal("3"),
    )

    assert target_notional(params) == Decimal("20")
    assert execution_price(Decimal("0.50"), params, "entry") == Decimal("0.5050000000")
    assert execution_price(Decimal("0.50"), params, "exit") == Decimal("0.4950000000")
    assert role_fee_bps(params, "maker") == Decimal("2")
    assert role_fee_bps(params, "taker") == Decimal("8")
    assert role_rebate_bps(params, "maker") == Decimal("3")
    assert role_rebate_bps(params, "taker") == Decimal("0")
    assert fee_rebate_for_notional(params, Decimal("20"), "maker") == (
        Decimal("0.0040000000"),
        Decimal("0.0060000000"),
    )


@pytest.mark.parametrize("side", ["BUY_YES", "SELL_YES"])
def test_orderfilled_fill_decision_matches_builtin_and_framework_adapters(side: str) -> None:
    point = PricePoint(
        x_value=123,
        price=Decimal("0.42"),
        volume=Decimal("12"),
        trade_count=7,
    )
    params = BacktestParameters(
        position_size=Decimal("10"),
        liquidity_cap_pct=Decimal("75"),
        min_fill_pct=Decimal("10"),
        min_fill_size=Decimal("0.1"),
        slippage_bps=Decimal("25"),
        execution_profile="conservative",
        order_role="maker",
        maker_fee_bps=Decimal("2"),
        taker_fee_bps=Decimal("8"),
        maker_rebate_bps=Decimal("1"),
        adverse_slippage_cents=Decimal("0.01"),
        fill_probability_haircut_pct=Decimal("40"),
    )
    run = {"price_source": "orderfilled_block_close"}

    direct = orderfilled_fill_decision(params, point, side)
    builtin = engine_fill_decision(params, point, run, side)
    adapter = framework_fill_decision(params, point, run, side)

    keys = [
        "requested_notional",
        "filled_notional",
        "expected_fill_notional",
        "actual_fill_notional",
        "fill_pct",
        "fill_probability",
        "raw_fill_probability",
        "filled_size",
        "expected_fill_size",
        "actual_fill_size",
        "unfilled_size",
        "avg_fill_price",
        "fee_cost",
        "rebate",
        "slippage_cost",
        "execution_cost",
        "execution_profile",
        "order_role",
        "execution_source",
    ]
    for key in keys:
        assert builtin[key] == direct[key]
        assert adapter[key] == direct[key]


def test_orderfilled_fill_decision_uses_side_volume_basis_for_maker_vs_taker() -> None:
    point = PricePoint(
        x_value=123,
        price=Decimal("0.42"),
        volume=Decimal("20"),
        trade_count=4,
        buy_volume=Decimal("15"),
        sell_volume=Decimal("5"),
    )
    params = BacktestParameters(
        position_size=Decimal("10"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="realistic",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    maker = orderfilled_fill_decision(
        BacktestParameters(**{**params.__dict__, "order_role": "maker"}),
        point,
        "BUY_YES",
    )
    taker = orderfilled_fill_decision(
        BacktestParameters(**{**params.__dict__, "order_role": "taker"}),
        point,
        "BUY_YES",
    )

    assert maker["block_volume_basis"] == "side_volume"
    assert taker["block_volume_basis"] == "side_volume"
    assert maker["effective_block_volume"] == Decimal("5.0000000000")
    assert taker["effective_block_volume"] == Decimal("15.0000000000")
    assert maker["available_notional"] == Decimal("5.0000000000")
    assert taker["available_notional"] == Decimal("15.0000000000")
    assert maker["filled_notional"] < taker["filled_notional"]


def test_orderfilled_fill_decision_discounts_volatile_dislocated_block_context() -> None:
    stable_point = PricePoint(
        x_value=123,
        price=Decimal("0.45"),
        volume=Decimal("20"),
        trade_count=4,
        high_price=Decimal("0.46"),
        low_price=Decimal("0.44"),
        vwap_price=Decimal("0.45"),
    )
    volatile_point = PricePoint(
        x_value=123,
        price=Decimal("0.60"),
        volume=Decimal("20"),
        trade_count=4,
        high_price=Decimal("0.80"),
        low_price=Decimal("0.20"),
        vwap_price=Decimal("0.45"),
    )
    params = BacktestParameters(
        position_size=Decimal("10"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="conservative",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    stable = orderfilled_fill_decision(params, stable_point, "BUY_YES")
    volatile = orderfilled_fill_decision(params, volatile_point, "BUY_YES")

    assert stable["block_context_factor"] == Decimal("1")
    assert volatile["block_context_factor"] < Decimal("1")
    assert volatile["block_context_reason"] == "volatile_block_context_discount"
    assert volatile["modeled_fill_probability_pre_haircut"] < stable["modeled_fill_probability_pre_haircut"]
    assert volatile["filled_notional"] < stable["filled_notional"]


def test_orderfilled_fill_decision_discounts_single_trade_block_for_maker() -> None:
    thin_point = PricePoint(
        x_value=123,
        price=Decimal("0.42"),
        volume=Decimal("20"),
        trade_count=1,
    )
    liquid_point = PricePoint(
        x_value=123,
        price=Decimal("0.42"),
        volume=Decimal("20"),
        trade_count=6,
    )
    params = BacktestParameters(
        position_size=Decimal("10"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="realistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    thin = orderfilled_fill_decision(params, thin_point, "BUY_YES")
    liquid = orderfilled_fill_decision(params, liquid_point, "BUY_YES")

    assert thin["trade_count_factor"] < Decimal("1")
    assert thin["trade_count_reason"] == "single_trade_block_discount"
    assert liquid["trade_count_factor"] == Decimal("1")
    assert thin["modeled_fill_probability_pre_haircut"] < liquid["modeled_fill_probability_pre_haircut"]
    assert thin["filled_notional"] < liquid["filled_notional"]
