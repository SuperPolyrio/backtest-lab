from decimal import Decimal

import pytest

from quant.backtest.backtest_engine import BacktestParameters, PricePoint, simulate_strategy
from quant.backtest.ledger import (
    build_ledger_rows,
    build_source_evidenced_order_ledger_rows,
    ledger_summary,
)
from quant.backtest.ledger_validation import build_ledger_cashflow_validation_report


pytestmark = pytest.mark.backtest_validation


def test_source_evidenced_order_ledger_excludes_modeled_size_and_caps_inventory() -> None:
    rows = build_source_evidenced_order_ledger_rows(
        [
            {
                "order_id": "buy-1",
                "side": "BUY_YES",
                "submit_x": 10,
                "expected_fill_size": "10",
                "actual_fill_size": "4",
                "filled_size": "10",
                "avg_fill_price": "0.60",
                "meta": {
                    "actual_fill_notional": "2.40",
                    "execution_evidence_type": "mixed_orderfilled_and_model",
                    "consumed_events": [{"trade_id": "source-buy"}],
                },
            },
            {
                "order_id": "sell-1",
                "side": "SELL_YES",
                "submit_x": 20,
                "expected_fill_size": "10",
                "actual_fill_size": "10",
                "filled_size": "10",
                "avg_fill_price": "0.70",
                "meta": {
                    "actual_fill_notional": "7.00",
                    "execution_evidence_type": "raw_orderfilled",
                    "consumed_events": [{"trade_id": "source-sell"}],
                },
            },
            {
                "order_id": "modeled-only",
                "side": "BUY_YES",
                "submit_x": 30,
                "expected_fill_size": "8",
                "actual_fill_size": "0",
                "filled_size": "8",
                "avg_fill_price": "0.50",
                "meta": {"execution_evidence_type": "modeled_expectation"},
            },
        ],
        Decimal("100"),
        market_slug="demo",
        token_side="YES",
        token_id="yes-token",
    )

    assert [row["event_type"] for row in rows] == ["BUY", "SELL"]
    assert [row["shares_delta"] for row in rows] == [Decimal("4.0000000000"), Decimal("-4.0000000000")]
    assert rows[0]["meta"]["source_event_ids"] == ["source-buy"]
    assert rows[1]["meta"]["inventory_capped"] is True
    assert rows[-1]["position_after"] == Decimal("0E-10")
    assert rows[-1]["cash_after"] == Decimal("100.4000000000")


def test_unfilled_exit_holds_to_resolution_settlement_one():
    points = [
        PricePoint(x_value=1, price=Decimal("0.60"), volume=Decimal("30"), trade_count=1),
        PricePoint(x_value=2, price=Decimal("0.40"), volume=Decimal("30"), trade_count=1),
        PricePoint(x_value=3, price=Decimal("0.97"), volume=Decimal("30"), trade_count=1),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("10"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.99"),
        settlement_value=Decimal("1"),
    )

    result = simulate_strategy(points, {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"}, params)

    assert result["trades"][0]["exit_reason"] == "settlement"
    assert result["trades"][0]["exit_price"] == Decimal("1")
    assert [row["event_type"] for row in result["ledger"]] == ["BUY", "SETTLEMENT"]
    assert next(metric for metric in result["metrics"] if metric["metric_key"] == "trade_exit_pnl")["value"] == Decimal("0E-10")
    assert next(metric for metric in result["metrics"] if metric["metric_key"] == "settlement_pnl")["value"] == result["trades"][0]["pnl"]


def test_unfilled_exit_settlement_zero_does_not_force_close_at_last_price():
    points = [
        PricePoint(x_value=10, price=Decimal("0.60"), volume=Decimal("30"), trade_count=1),
        PricePoint(x_value=11, price=Decimal("0.40"), volume=Decimal("30"), trade_count=1),
        PricePoint(x_value=12, price=Decimal("0.97"), volume=Decimal("30"), trade_count=1),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("10"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.99"),
        settlement_value=Decimal("0"),
    )

    result = simulate_strategy(points, {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"}, params)

    trade = result["trades"][0]
    assert trade["exit_reason"] == "settlement"
    assert trade["exit_price"] == Decimal("0")
    assert trade["exit_price"] != Decimal("0.97")
    assert result["ledger"][-1]["event_type"] == "SETTLEMENT"


def test_settlement_ledger_records_external_cost_events_and_net_profit():
    points = [
        PricePoint(x_value=1, price=Decimal("0.60"), volume=Decimal("30"), trade_count=1),
        PricePoint(x_value=2, price=Decimal("0.40"), volume=Decimal("30"), trade_count=1),
        PricePoint(x_value=3, price=Decimal("0.97"), volume=Decimal("30"), trade_count=1),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("10"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.99"),
        settlement_value=Decimal("1"),
        gas_cost_per_order=Decimal("0.10"),
        settlement_cost=Decimal("0.30"),
        redeem_cost=Decimal("0.20"),
        capital_cost_bps=Decimal("1"),
    )

    result = simulate_strategy(points, {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"}, params)
    event_types = [row["event_type"] for row in result["ledger"]]
    metrics = {row["metric_key"]: row["value"] for row in result["metrics"]}

    assert event_types == ["BUY", "GAS_COST", "SETTLEMENT", "GAS_COST", "SETTLEMENT_COST", "REDEEM_COST", "CAPITAL_COST"]
    assert metrics["ledger_external_cost_total"] == Decimal("-0.7003579911")
    assert metrics["ledger_capital_cost_total"] == Decimal("-0.0003579911")
    assert metrics["net_profit"] == Decimal("4.5590170090")
    assert result["ledger"][-1]["cash_after"] == Decimal("104.5590170090")


def test_simulated_ledger_rows_preserve_event_and_token_identity() -> None:
    rows = build_ledger_rows(
        [
            {
                "trade_id": "t1",
                "entry_order_id": "o-entry",
                "exit_order_id": "o-exit",
                "market_slug": "france",
                "token_side": "YES",
                "event_slug": "world-cup",
                "meta": {"token_id": "france-yes"},
                "entry_x": 100,
                "exit_x": 110,
                "entry_price": "0.40",
                "exit_price": "0.60",
                "size": "10",
                "pnl": "2",
            }
        ],
        Decimal("100"),
    )

    assert rows[0]["event_slug"] == "world-cup"
    assert rows[0]["token_id"] == "france-yes"
    assert rows[1]["event_slug"] == "world-cup"
    assert rows[1]["token_id"] == "france-yes"


def test_ledger_rows_replay_polymarket_special_cashflows_in_block_order() -> None:
    rows = build_ledger_rows(
        [],
        Decimal("100"),
        cashflow_events=[
            {
                "id": "redeem-1",
                "type": "REDEEM",
                "market_slug": "france",
                "event_slug": "world-cup",
                "token_id": "france-yes",
                "token_side": "YES",
                "amount": "10",
                "size": "10",
                "block_number": 30,
            },
            {
                "id": "buy-1",
                "type": "BUY",
                "market_slug": "france",
                "event_slug": "world-cup",
                "token_id": "france-yes",
                "token_side": "YES",
                "amount": "4",
                "size": "10",
                "block_number": 10,
            },
            {
                "id": "rebate-1",
                "type": "MAKER_REBATE",
                "market_slug": "france",
                "event_slug": "world-cup",
                "token_id": "france-yes",
                "token_side": "YES",
                "amount": "0.05",
                "block_number": 20,
            },
            {
                "id": "reward-1",
                "type": "REWARD",
                "market_slug": "france",
                "event_slug": "world-cup",
                "token_id": "france-yes",
                "token_side": "YES",
                "amount": "99",
                "block_number": 25,
            },
        ],
    )
    summary = ledger_summary(rows, Decimal("100"))

    assert [row["event_type"] for row in rows] == ["BUY", "MAKER_REBATE", "REDEEM"]
    assert [row["x_value"] for row in rows] == [10, 20, 30]
    assert rows[-1]["cash_after"] == Decimal("106.0500000000")
    assert rows[-1]["position_after"] == Decimal("0E-10")
    assert summary["ledger_cash_pnl"] == Decimal("6.0500000000")
    assert summary["realized_pnl"] == Decimal("6.0500000000")
    assert summary["redeem_total"] == Decimal("10.0000000000")
    assert summary["special_rebate_total"] == Decimal("0.0500000000")


def test_ledger_rows_derive_redeem_cashflow_amount_from_size_when_amount_missing() -> None:
    rows = build_ledger_rows(
        [],
        Decimal("100"),
        cashflow_events=[
            {
                "id": "buy-1",
                "type": "BUY",
                "market_slug": "france",
                "event_slug": "world-cup",
                "token_id": "france-yes",
                "token_side": "YES",
                "amount": "1",
                "size": "5",
                "block_number": 10,
            },
            {
                "id": "redeem-1",
                "type": "REDEEM",
                "market_slug": "france",
                "event_slug": "world-cup",
                "token_id": "france-yes",
                "token_side": "YES",
                "size": "2",
                "payout": "0.90",
                "block_number": 20,
            },
        ],
    )
    summary = ledger_summary(rows, Decimal("100"))

    assert [row["event_type"] for row in rows] == ["BUY", "REDEEM"]
    assert rows[-1]["cash_delta"] == Decimal("1.8000000000")
    assert rows[-1]["cash_after"] == Decimal("100.8000000000")
    assert rows[-1]["meta"]["amount_source"] == "shares_x_unit_price"
    assert summary["ledger_cash_pnl"] == Decimal("0.8000000000")
    assert summary["redeem_total"] == Decimal("1.8000000000")


def test_ledger_rows_interleave_special_cashflows_with_simulated_trade_rows() -> None:
    rows = build_ledger_rows(
        [
            {
                "trade_id": "t1",
                "entry_order_id": "o-entry",
                "exit_order_id": "o-exit",
                "market_slug": "france",
                "token_side": "YES",
                "event_slug": "world-cup",
                "token_id": "france-yes",
                "entry_x": 10,
                "exit_x": 40,
                "entry_price": "0.40",
                "exit_price": "0.60",
                "size": "10",
                "pnl": "2",
            }
        ],
        Decimal("100"),
        cashflow_events=[
            {
                "id": "split-1",
                "type": "SPLIT",
                "market_slug": "spain",
                "event_slug": "world-cup",
                "token_id": "spain-yes",
                "token_side": "YES",
                "amount": "2",
                "size": "2",
                "block_number": 20,
            },
            {
                "id": "merge-1",
                "type": "MERGE",
                "market_slug": "spain",
                "event_slug": "world-cup",
                "token_id": "spain-yes",
                "token_side": "YES",
                "amount": "1",
                "size": "1",
                "block_number": 30,
            },
        ],
    )
    summary = ledger_summary(rows, Decimal("100"))

    assert [row["event_type"] for row in rows] == ["BUY", "SPLIT", "MERGE", "SELL"]
    assert [row["cash_after"] for row in rows] == [
        Decimal("96.0000000000"),
        Decimal("94.0000000000"),
        Decimal("95.0000000000"),
        Decimal("101.0000000000"),
    ]
    assert summary["split_total"] == Decimal("2.0000000000")
    assert summary["merge_total"] == Decimal("1.0000000000")
    assert summary["position_after"] == Decimal("1.0000000000")


def test_ledger_rows_preserve_complete_set_legs_for_split_merge_validation() -> None:
    rows = build_ledger_rows(
        [],
        Decimal("100"),
        cashflow_events=[
            {
                "id": "split-set",
                "type": "SPLIT",
                "market_slug": "france",
                "event_slug": "world-cup",
                "yes_token_id": "france-yes",
                "no_token_id": "france-no",
                "amount": "2",
                "size": "2",
                "block_number": 20,
            },
            {
                "id": "merge-set",
                "type": "MERGE",
                "market_slug": "france",
                "event_slug": "world-cup",
                "yes_token_id": "france-yes",
                "no_token_id": "france-no",
                "amount": "1",
                "size": "1",
                "block_number": 30,
            },
        ],
    )
    report = build_ledger_cashflow_validation_report(
        [],
        rows,
        {
            "initial_capital": "100",
            "mark_prices": {
                "france|world-cup|france-yes|YES": "0.30",
                "france|world-cup|france-no|NO": "0.70",
            },
        },
    )

    assert [row["event_type"] for row in rows] == ["SPLIT", "MERGE"]
    assert rows[0]["position_legs"] == [
        {"token_id": "france-yes", "token_side": "YES"},
        {"token_id": "france-no", "token_side": "NO"},
    ]
    assert rows[0]["meta"]["position_legs"] == rows[0]["position_legs"]
    assert report["polymarket_residual_position_count"] == 2
    assert report["polymarket_unrealized_position_value"] == "1"
    assert report["polymarket_portfolio_cash_at_risk"] == "1"
    assert report["polymarket_event_cash_at_risk"] == {"world-cup": "1"}


def test_simulate_strategy_replays_run_cashflow_events_into_ledger_and_metrics() -> None:
    points = [
        PricePoint(x_value=10, price=Decimal("0.10"), volume=Decimal("0"), trade_count=0),
        PricePoint(x_value=20, price=Decimal("0.11"), volume=Decimal("0"), trade_count=0),
        PricePoint(x_value=30, price=Decimal("0.12"), volume=Decimal("0"), trade_count=0),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("10"),
        entry_threshold=Decimal("0.90"),
        execution_price_mode="ORDERFILLED_CROSS",
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "france",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "cashflowEvents": [
                {
                    "id": "buy-1",
                    "type": "BUY",
                    "market_slug": "france",
                    "event_slug": "world-cup",
                    "token_id": "france-yes",
                    "token_side": "YES",
                    "amount": "4",
                    "size": "10",
                    "block_number": 10,
                },
                {
                    "id": "rebate-1",
                    "type": "MAKER_REBATE",
                    "market_slug": "france",
                    "event_slug": "world-cup",
                    "token_id": "france-yes",
                    "token_side": "YES",
                    "amount": "0.05",
                    "block_number": 20,
                },
                {
                    "id": "redeem-1",
                    "type": "REDEEM",
                    "market_slug": "france",
                    "event_slug": "world-cup",
                    "token_id": "france-yes",
                    "token_side": "YES",
                    "amount": "10",
                    "size": "10",
                    "block_number": 30,
                },
            ],
        },
        params,
    )
    metrics = {row["metric_key"]: row["value"] for row in result["metrics"]}

    assert result["trades"] == []
    assert [row["event_type"] for row in result["ledger"]] == ["BUY", "MAKER_REBATE", "REDEEM"]
    assert result["ledger"][-1]["cash_after"] == Decimal("106.0500000000")
    assert result["ledger"][-1]["position_after"] == Decimal("0E-10")
    assert metrics["net_profit"] == Decimal("6.0500000000")
    assert metrics["ledger_realized_pnl"] == Decimal("6.0500000000")
