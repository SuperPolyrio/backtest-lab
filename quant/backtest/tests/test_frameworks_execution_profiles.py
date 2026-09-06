from decimal import Decimal

import pytest

from quant.backtest.backtest_engine import BacktestParameters, PricePoint
from quant.backtest.frameworks import AdapterPosition, _close_trade, _fill_decision


pytestmark = pytest.mark.backtest_validation


def test_framework_orderfilled_fill_decision_uses_execution_profile_haircut() -> None:
    point = PricePoint(x_value=100, price=Decimal("0.50"), volume=Decimal("100"), trade_count=5)
    run = {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"}
    optimistic = BacktestParameters(
        position_size=Decimal("100"),
        execution_profile="optimistic",
        fill_probability_haircut_pct=Decimal("0"),
        adverse_slippage_cents=Decimal("0"),
        order_role="taker",
    )
    conservative = BacktestParameters(
        position_size=Decimal("100"),
        execution_profile="conservative",
        fill_probability_haircut_pct=Decimal("50"),
        adverse_slippage_cents=Decimal("0.01"),
        order_role="maker",
        maker_rebate_bps=Decimal("10"),
    )

    optimistic_fill = _fill_decision(optimistic, point, run, "BUY_YES")
    conservative_fill = _fill_decision(conservative, point, run, "BUY_YES")

    assert optimistic_fill["raw_fill_probability"] == Decimal("100.0000000000")
    assert optimistic_fill["fill_probability"] == Decimal("100")
    assert optimistic_fill["filled_notional"] == Decimal("100.0000000000")
    assert conservative_fill["raw_fill_probability"] == Decimal("100.0000000000")
    assert conservative_fill["fill_probability"] == Decimal("50.0")
    assert conservative_fill["filled_notional"] == Decimal("50.0000000000")
    assert conservative_fill["filled_size"] < optimistic_fill["filled_size"]
    assert conservative_fill["order_role"] == "maker"
    assert conservative_fill["execution_profile"] == "conservative"
    assert conservative_fill["rebate"] == Decimal("0.0500000000")
    assert conservative_fill["execution_cost"] < conservative_fill["slippage_cost"]


def test_framework_close_trade_uses_entry_and_exit_fill_costs_and_rebates() -> None:
    position = AdapterPosition(
        trade_index=1,
        entry_index=0,
        entry_x=100,
        entry_price=Decimal("0.40"),
        size=Decimal("10"),
        requested_notional=Decimal("4"),
        filled_notional=Decimal("4"),
        fill_pct=Decimal("100"),
        entry_fee_cost=Decimal("0.02"),
        entry_rebate=Decimal("0.01"),
        entry_slippage_cost=Decimal("0.03"),
    )
    exit_fill = {
        "size": Decimal("10"),
        "exit_price": Decimal("0.60"),
        "fill_status": "FILLED",
        "fee_cost": Decimal("0.04"),
        "rebate": Decimal("0.02"),
        "slippage_cost": Decimal("0.05"),
        "fill_probability": Decimal("100"),
        "block_volume": Decimal("100"),
        "trade_count": 3,
        "available_notional": Decimal("6"),
        "execution_source": "orderfilled_volume",
    }

    trade = _close_trade(
        {"market_slug": "demo", "token_side": "YES"},
        "block_number",
        position,
        110,
        Decimal("0.60"),
        2,
        "exit_threshold",
        BacktestParameters(fee_bps=Decimal("99")),
        exit_fill=exit_fill,
    )

    assert trade["fee_cost"] == Decimal("0.0600000000")
    assert trade["rebate"] == Decimal("0.0300000000")
    assert trade["slippage_cost"] == Decimal("0.0800000000")
    assert trade["execution_cost"] == Decimal("0.1100000000")
    assert trade["pnl"] == Decimal("1.9700000000")
