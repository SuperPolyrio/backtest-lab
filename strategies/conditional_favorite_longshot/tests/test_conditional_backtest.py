from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from quant.backtest.orderfilled_v2_replay import V2OrderResult, V2TradePrint, replay_v2_taker_order
from strategies.conditional_favorite_longshot.calibration import (
    CalibrationEstimate,
    estimate_calibration,
)
from strategies.conditional_favorite_longshot.conditional_backtest import (
    ConditionalConfig,
    MarketObservation,
    _alpha_only_summary,
    _bias_direction,
    _signal_from_estimate,
    result_to_trade,
    signal_to_order,
)


NOW = datetime(2026, 6, 1, tzinfo=timezone.utc)


def test_four_model_calibration_detects_stable_favorite_underpricing() -> None:
    rows = []
    market_id = 0
    for month in range(12):
        for index in range(90):
            market_id += 1
            probability = (0.65, 0.75, 0.85)[index % 3]
            calibrated = min(0.97, probability + 0.10)
            won = ((index * 37 + month * 11) % 100) < int(calibrated * 100)
            rows.append(
                SimpleNamespace(
                    market_id=market_id,
                    signal_time=datetime(2025, month + 1, 15, tzinfo=timezone.utc),
                    probability_yes=probability,
                    yes_won=won,
                )
            )

    estimate = estimate_calibration(
        rows,
        probability=0.75,
        min_samples=100,
        min_validation_samples=90,
        bootstrap_samples=20,
        min_stability_periods=6,
        min_period_samples=20,
        require_validation_improvement=False,
        seed_key="stable-favorite",
    )

    assert set(estimate.model_predictions) == {"power", "platt", "isotonic", "local_linear"}
    assert estimate.probability > 0.75
    assert estimate.lower_bound > 0.75
    assert estimate.stable


def test_signal_can_buy_yes_or_complementary_no() -> None:
    observation = _observation(probability_yes=0.70)
    yes_estimate = _estimate(probability=0.80, lower=0.77, upper=0.83)
    no_estimate = _estimate(probability=0.58, lower=0.54, upper=0.61)
    config = ConditionalConfig(edge_threshold=Decimal("0.01"), execution_buffer=Decimal("0.01"))

    yes_signal = _signal_from_estimate(observation, yes_estimate, "category_tte", ("sports",), config)
    no_signal = _signal_from_estimate(observation, no_estimate, "category_tte", ("sports",), config)

    assert yes_signal is not None
    assert yes_signal.buy_side == "YES"
    assert yes_signal.asset_id == "token-yes"
    assert no_signal is not None
    assert no_signal.buy_side == "NO"
    assert no_signal.asset_id == "token-no"
    assert _bias_direction(Decimal("0.70"), Decimal("0.80")) == "classic_favorite_longshot"
    assert _bias_direction(Decimal("0.70"), Decimal("0.58")) == "reverse_favorite_longshot"


def test_trade_price_range_filters_selected_side_and_reports_buckets() -> None:
    estimate = _estimate(probability=0.98, lower=0.97, upper=0.99)
    mid_config = ConditionalConfig(
        min_trade_price=Decimal("0.60"),
        max_trade_price=Decimal("0.80"),
        edge_threshold=Decimal("0"),
        execution_buffer=Decimal("0"),
    )
    tail_config = ConditionalConfig(
        min_trade_price=Decimal("0.90"),
        max_trade_price=Decimal("0.99"),
        edge_threshold=Decimal("0"),
        execution_buffer=Decimal("0"),
    )

    assert _signal_from_estimate(
        _observation(probability_yes=0.95),
        estimate,
        "category_tte",
        ("sports",),
        mid_config,
    ) is None
    tail_signal = _signal_from_estimate(
        _observation(probability_yes=0.95),
        estimate,
        "category_tte",
        ("sports",),
        tail_config,
    )

    assert tail_signal is not None
    summary = _alpha_only_summary([tail_signal], config=tail_config)
    assert summary["signals"] == 1
    assert summary["by_price_bucket"]["0.90-0.99"]["signals"] == 1


def test_order_excludes_signal_trade_and_needs_future_orderfilled_source() -> None:
    signal = _signal_from_estimate(
        _observation(probability_yes=0.70),
        _estimate(probability=0.82, lower=0.79, upper=0.85),
        "category_tte",
        ("sports",),
        ConditionalConfig(),
    )
    assert signal is not None
    order = signal_to_order(signal, config=ConditionalConfig(), profile="probabilistic_source_confirmed")
    any_side_order = signal_to_order(
        signal,
        config=ConditionalConfig(),
        profile="probabilistic_taker_120s_any_order_side",
    )
    source = _trade("signal", block=100, seconds=0, price="0.70")
    future = _trade("future", block=101, seconds=2, price="0.70")

    no_fill = replay_v2_taker_order(order, [source])
    filled = replay_v2_taker_order(order, [source, future])

    assert order.exclude_signal_source_trade
    assert any_side_order.deadline_block is not None
    assert any_side_order.deadline_ts == any_side_order.arrival_ts + timedelta(seconds=120)
    assert order.asset_id == "token-yes"
    assert no_fill.status == "NO_FILL"
    assert filled.filled_size > 0
    assert filled.fills[0].source_trade_id == "future"


def test_result_uses_bought_token_outcome_and_capital_cost() -> None:
    observation = _observation(probability_yes=0.70, yes_won=False)
    signal = _signal_from_estimate(
        observation,
        _estimate(probability=0.55, lower=0.52, upper=0.58),
        "category_tte",
        ("sports",),
        ConditionalConfig(edge_threshold=Decimal("0"), execution_buffer=Decimal("0")),
    )
    assert signal is not None and signal.buy_side == "NO"
    config = ConditionalConfig(
        edge_threshold=Decimal("0"),
        execution_buffer=Decimal("0"),
        capital_cost_annual_rate=Decimal("0.10"),
    )
    order = signal_to_order(signal, config=config, profile="probabilistic_source_confirmed")
    result = V2OrderResult(
        order_id=order.order_id,
        status="FILLED",
        side="BUY",
        requested_size=Decimal("10"),
        filled_size=Decimal("10"),
        unfilled_size=Decimal("0"),
        avg_price=Decimal("0.30"),
        limit_price=Decimal("0.30"),
        arrival_block=100,
        arrival_ts=NOW + timedelta(seconds=1),
        eligible_historical_volume=Decimal("100"),
        simulated_volume=Decimal("10"),
        participation_rate=Decimal("0.10"),
        capacity_utilization=Decimal("0.10"),
        avg_fill_delay_seconds=Decimal("2"),
        avg_price_buffer=Decimal("0"),
        filled_notional=Decimal("3"),
        cash_delta=Decimal("-3"),
        position_delta=Decimal("10"),
        reason_unfilled="",
    )

    row = result_to_trade(signal, order, result, config=config, profile="probabilistic_source_confirmed")

    assert Decimal(row.settlement_value) == Decimal("10.0000000000")
    assert Decimal(row.capital_cost) > 0
    assert Decimal(row.pnl) < Decimal("7")


def _observation(*, probability_yes: float, yes_won: bool = True) -> MarketObservation:
    return MarketObservation(
        market_id=1,
        market_slug="nba-a-b-2026-06-01",
        title="A vs B",
        category="sports",
        league="nba",
        product_type="moneyline",
        close_time=NOW + timedelta(hours=2),
        close_time_source="gamma_closed_time",
        signal_time=NOW,
        label_available_at=NOW + timedelta(hours=3),
        tte_bucket="90-240m",
        true_tte_minutes=120.0,
        liquidity_regime="liquid",
        bucket_trade_count=100,
        bucket_volume=Decimal("1000"),
        probability_yes=probability_yes,
        yes_won=yes_won,
        signal_trade_id="signal",
        signal_tx_hash="0xsignal",
        signal_log_indexes=(1,),
        signal_block=100,
        signal_asset_id="token-yes",
        signal_outcome="YES",
        yes_asset_id="token-yes",
        no_asset_id="token-no",
    )


def _estimate(*, probability: float, lower: float, upper: float) -> CalibrationEstimate:
    return CalibrationEstimate(
        selected_model="platt",
        probability=probability,
        lower_bound=lower,
        upper_bound=upper,
        sample_count=100,
        market_count=100,
        validation_count=20,
        validation_brier=0.18,
        validation_baseline_brier=0.20,
        model_predictions={"power": probability, "platt": probability, "isotonic": probability, "local_linear": probability},
        model_briers={"power": 0.19, "platt": 0.18, "isotonic": 0.20, "local_linear": 0.21},
        model_sign_agreement=1.0,
        stable_periods=6,
        stable_period_sign_ratio=1.0,
        stable_seasons=2,
        stable_season_sign_ratio=1.0,
        stable=True,
        rejection_reasons=(),
    )


def _trade(trade_id: str, *, block: int, seconds: int, price: str) -> V2TradePrint:
    return V2TradePrint(
        trade_id=trade_id,
        market_id=1,
        condition_id="condition",
        asset_id="token-yes",
        outcome="YES",
        block_number=block,
        block_time=NOW + timedelta(seconds=seconds),
        tx_hash=f"0x{trade_id}",
        tx_index=0,
        tx_index_source="fixture",
        price=Decimal(price),
        size=Decimal("100"),
        notional=Decimal(price) * Decimal("100"),
        aggressor_side="BUY",
        passive_side="SELL",
        source_log_indexes=(1 if trade_id == "signal" else 2,),
        source_fill_count=1,
    )
