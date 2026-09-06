from decimal import Decimal

import pytest

from quant.backtest.backtest_engine import BacktestParameters, PricePoint, parse_parameters, simulate_strategy
from quant.backtest.runners.execution_replay import sequence_key
from quant.backtest.strategy_intents import build_threshold_limit_intent


pytestmark = pytest.mark.backtest_validation


def test_threshold_limit_intent_preserves_strategy_signal_and_replay_mapping() -> None:
    intent = build_threshold_limit_intent(
        strategy_name="fixed_threshold",
        strategy_version="fixed_threshold_v1",
        signal_index=7,
        signal_x=88000001,
        submit_x=88000003,
        side="BUY_YES",
        signal_price=Decimal("0.581"),
        limit_price=Decimal("0.58"),
        target_notional=Decimal("11.6"),
        order_id="O-0007",
        reason="entry_limit",
        time_in_force="GTC",
        liquidity_cap_pct=Decimal("25"),
        role="maker",
        order_type="post_only_limit",
        execution_profile="conservative",
    )

    replay_intent = intent.to_replay_order_intent()
    payload = intent.as_dict()

    assert replay_intent.side == "BUY_YES"
    assert replay_intent.limit_price == Decimal("0.5800000000")
    assert replay_intent.size == Decimal("20.0000000000")
    assert replay_intent.submit_sequence == sequence_key(88000003, 0, 0, "O-0007-submit")
    assert replay_intent.liquidity_cap_pct == Decimal("25")
    assert replay_intent.order_role == "maker"
    assert replay_intent.execution_profile == "conservative"
    assert payload["signal"]["signal_index"] == 7
    assert payload["signal"]["reason"] == "entry_limit"
    assert payload["requested_notional"] == "11.6"
    assert payload["execution_profile"] == "conservative"


def test_builtin_limit_replay_orders_include_strategy_intent_contract() -> None:
    points = [
        PricePoint(100, Decimal("0.57"), Decimal("100"), trade_count=2),
        PricePoint(101, Decimal("0.60"), Decimal("100"), trade_count=2),
    ]
    params = BacktestParameters(
        entry_threshold=Decimal("0.58"),
        sell_limit_price=Decimal("0.60"),
        position_size=Decimal("11.6"),
        execution_price_mode="ORDERFILLED_LIMIT_REPLAY",
        liquidity_cap_pct=Decimal("100"),
    )

    result = simulate_strategy(
        points,
        {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"},
        params,
    )

    buy_order = result["orders"][0]
    strategy_intent = buy_order["meta"]["strategy_intent"]
    assert strategy_intent["signal"]["strategy_name"] == "fixed_threshold"
    assert strategy_intent["signal"]["signal_x"] == 100
    assert strategy_intent["submit_x"] == buy_order["submit_x"]
    assert strategy_intent["side"] == buy_order["side"]
    assert strategy_intent["execution_model"] in {
        "orderfilled_limit_replay_raw",
        "orderfilled_limit_replay_synthetic",
    }


def test_parse_parameters_accepts_bar_aware_signal_fields() -> None:
    params = parse_parameters(
        {
            "entrySignalPriceField": "vwap",
            "exitSignalPriceField": "close",
            "entryUseBlockRange": True,
            "exitUseBlockRange": True,
            "signalMinTradeCount": 3,
            "signalMinBlockVolume": "25",
        }
    )

    assert params.entry_signal_price_field == "vwap_price"
    assert params.exit_signal_price_field == "close_price"
    assert params.entry_use_block_range is True
    assert params.exit_use_block_range is True
    assert params.signal_min_trade_count == 3
    assert params.signal_min_block_volume == Decimal("25")


def test_builtin_strategy_can_gate_on_vwap_trade_count_and_block_volume() -> None:
    points = [
        PricePoint(
            100,
            Decimal("0.61"),
            Decimal("15"),
            trade_count=1,
            vwap_price=Decimal("0.57"),
            close_price=Decimal("0.61"),
        ),
        PricePoint(
            101,
            Decimal("0.62"),
            Decimal("40"),
            trade_count=3,
            vwap_price=Decimal("0.59"),
            close_price=Decimal("0.62"),
        ),
    ]
    params = BacktestParameters(
        entry_threshold=Decimal("0.58"),
        position_size=Decimal("10"),
        execution_price_mode="ORDERFILLED",
        entry_signal_price_field="vwap_price",
        signal_min_trade_count=2,
        signal_min_block_volume=Decimal("20"),
        max_holding_bars=1,
    )

    result = simulate_strategy(
        points,
        {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"},
        params,
    )

    first_order = result["orders"][0]
    assert first_order["signal_x"] == 101
    assert first_order["decision_price"] == Decimal("0.5900000000")
    assert first_order["meta"]["signal_price_field"] == "vwap_price"
    assert first_order["meta"]["signal_min_trade_count"] == 2
    assert first_order["meta"]["signal_min_block_volume"] == Decimal("20.0000000000")


def test_builtin_strategy_can_use_block_range_for_entry_and_exit() -> None:
    points = [
        PricePoint(
            100,
            Decimal("0.55"),
            Decimal("100"),
            trade_count=4,
            high_price=Decimal("0.61"),
            low_price=Decimal("0.54"),
            close_price=Decimal("0.55"),
        ),
        PricePoint(
            101,
            Decimal("0.56"),
            Decimal("120"),
            trade_count=4,
            high_price=Decimal("0.70"),
            low_price=Decimal("0.56"),
            close_price=Decimal("0.56"),
        ),
    ]
    params = BacktestParameters(
        entry_threshold=Decimal("0.58"),
        take_profit=Decimal("0.10"),
        position_size=Decimal("10"),
        execution_price_mode="ORDERFILLED",
        entry_use_block_range=True,
        exit_use_block_range=True,
    )

    result = simulate_strategy(
        points,
        {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"},
        params,
    )

    assert result["orders"][0]["decision_price"] == Decimal("0.6100000000")
    assert result["orders"][0]["meta"]["signal_trigger_field"] == "high_price"
    assert result["orders"][0]["meta"]["signal_block_range_used"] is True
    assert result["trades"][0]["exit_reason"] == "take_profit"
    assert result["orders"][1]["decision_price"] == Decimal("0.7000000000")
    assert result["orders"][1]["meta"]["signal_trigger_field"] == "high_price"
