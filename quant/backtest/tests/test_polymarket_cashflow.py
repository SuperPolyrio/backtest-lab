from decimal import Decimal

import pytest

from quant.backtest.polymarket_cashflow import normalize_polymarket_activity_event, replay_polymarket_cashflows


pytestmark = pytest.mark.backtest_validation


def test_normalize_polymarket_activity_maps_trade_side_and_special_events() -> None:
    buy = normalize_polymarket_activity_event(
        {
            "id": "a1",
            "type": "TRADE",
            "side": "BUY",
            "marketSlug": "demo",
            "eventSlug": "event-demo",
            "tokenId": "yes-token",
            "outcome": "YES",
            "amount": "4.20",
            "size": "10",
        }
    )
    rebate = normalize_polymarket_activity_event({"type": "MAKER_REBATE", "amount": "0.02"})

    assert buy["event_type"] == "BUY"
    assert buy["amount"] == Decimal("4.2000000000")
    assert buy["shares_delta"] == Decimal("10.0000000000")
    assert buy["position_key"] == "demo|event-demo|yes-token|YES"
    assert rebate["event_type"] == "MAKER_REBATE"
    assert rebate["shares_delta"] == Decimal("0E-10")


def test_normalize_polymarket_activity_derives_special_cashflow_amount_from_size_and_unit_price() -> None:
    redeem = normalize_polymarket_activity_event(
        {
            "type": "REDEEM",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "size": "3",
            "payout": "0.80",
        }
    )
    merge = normalize_polymarket_activity_event(
        {
            "type": "MERGE",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "size": "2",
        }
    )

    assert redeem["amount"] == Decimal("2.4000000000")
    assert redeem["amount_source"] == "shares_x_unit_price"
    assert redeem["shares_delta"] == Decimal("-3.0000000000")
    assert merge["amount"] == Decimal("2.0000000000")
    assert merge["amount_source"] == "shares_x_unit_price"
    assert merge["shares_delta"] == Decimal("-2.0000000000")


def test_normalize_polymarket_activity_keeps_trade_side_out_of_position_key() -> None:
    row = normalize_polymarket_activity_event(
        {
            "type": "TRADE",
            "side": "BUY",
            "market_slug": "demo",
            "event_slug": "event-demo",
            "token_id": "yes-token",
            "amount": "1.25",
            "size": "5",
        }
    )

    assert row["event_type"] == "BUY"
    assert row["token_side"] == ""
    assert row["position_key"] == "demo|event-demo|yes-token|"


def test_normalize_polymarket_activity_accepts_yes_no_side_as_outcome_when_not_trade_side() -> None:
    row = normalize_polymarket_activity_event(
        {
            "type": "BUY",
            "side": "YES",
            "market_slug": "demo",
            "event_slug": "event-demo",
            "token_id": "yes-token",
            "amount": "1.25",
            "size": "5",
        }
    )

    assert row["event_type"] == "BUY"
    assert row["token_side"] == "YES"
    assert row["position_key"] == "demo|event-demo|yes-token|YES"


def test_replay_polymarket_cashflows_uses_activity_formula_and_excludes_rewards() -> None:
    events = [
        {
            "id": "buy-1",
            "type": "TRADE",
            "side": "BUY",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "amount": "4.00",
            "size": "10",
            "block_number": 100,
        },
        {
            "id": "sell-1",
            "type": "TRADE",
            "side": "SELL",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "amount": "1.50",
            "size": "3",
            "block_number": 101,
        },
        {
            "id": "rebate-1",
            "type": "MAKER_REBATE",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "amount": "0.05",
            "block_number": 102,
        },
        {
            "id": "split-1",
            "type": "SPLIT",
            "market_slug": "spain",
            "event_slug": "world-cup",
            "token_id": "spain-yes",
            "token_side": "YES",
            "amount": "2.00",
            "size": "2",
            "block_number": 103,
        },
        {
            "id": "merge-1",
            "type": "MERGE",
            "market_slug": "spain",
            "event_slug": "world-cup",
            "token_id": "spain-yes",
            "token_side": "YES",
            "amount": "1.00",
            "size": "1",
            "block_number": 104,
        },
        {
            "id": "redeem-1",
            "type": "REDEEM",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "amount": "2.00",
            "size": "2",
            "block_number": 105,
        },
        {
            "id": "reward-1",
            "type": "REWARD",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "amount": "99.00",
            "block_number": 106,
        },
    ]

    report = replay_polymarket_cashflows(
        events,
        initial_capital=Decimal("100"),
        mark_prices={
            "france|world-cup|france-yes|YES": "0.60",
            "spain|world-cup|spain-yes|YES": "0.40",
        },
    )
    summary = report["summary"]

    # SELL + REDEEM + MERGE + REBATE - BUY - SPLIT + residual marked value.
    assert summary["cashflow_total"] == Decimal("-1.4500000000")
    assert summary["unrealized_position_value"] == Decimal("3.4000000000")
    assert summary["net_trading_pnl"] == Decimal("1.9500000000")
    assert summary["excluded_total"] == Decimal("99.0000000000")
    assert summary["cash_balance"] == Decimal("98.5500000000")
    assert summary["residual_position_count"] == 2
    assert summary["portfolio_cash_at_risk"] == Decimal("3.0000000000")
    assert summary["event_cash_at_risk"] == {"world-cup": Decimal("3.0000000000")}
    assert report["ledger"][-1]["excluded_from_trading_pnl"] is True


def test_replay_polymarket_cashflows_uses_derived_redeem_amount_when_amount_missing() -> None:
    report = replay_polymarket_cashflows(
        [
            {
                "id": "buy-1",
                "type": "BUY",
                "market_slug": "france",
                "event_slug": "world-cup",
                "token_id": "france-yes",
                "token_side": "YES",
                "amount": "1",
                "size": "5",
                "block_number": 100,
            },
            {
                "id": "redeem-1",
                "type": "REDEEM",
                "market_slug": "france",
                "event_slug": "world-cup",
                "token_id": "france-yes",
                "token_side": "YES",
                "size": "2",
                "payout": "1",
                "block_number": 101,
            },
        ],
        mark_prices={"france|world-cup|france-yes|YES": "0.20"},
    )

    summary = report["summary"]
    assert summary["cashflow_total"] == Decimal("1.0000000000")
    assert summary["unrealized_position_value"] == Decimal("0.6000000000")
    assert summary["net_trading_pnl"] == Decimal("1.6000000000")
    assert summary["portfolio_cash_at_risk"] == Decimal("0.6000000000")
    assert report["ledger"][-1]["amount"] == Decimal("2.0000000000")
    assert report["ledger"][-1]["meta"]["amount_source"] == "shares_x_unit_price"
    assert report["residual_positions"][0]["quantity"] == Decimal("3.0000000000")


def test_replay_polymarket_cashflows_residual_position_value_uses_position_key_marks() -> None:
    report = replay_polymarket_cashflows(
        [
            {
                "type": "BUY",
                "market_slug": "argentina",
                "event_slug": "world-cup",
                "token_id": "arg-yes",
                "token_side": "YES",
                "amount": "3",
                "size": "12",
            }
        ],
        mark_prices={"argentina|world-cup|arg-yes|YES": "0.30"},
    )

    assert report["residual_positions"] == [
        {
            "position_key": "argentina|world-cup|arg-yes|YES",
            "market_slug": "argentina",
            "event_slug": "world-cup",
            "token_id": "arg-yes",
            "token_side": "YES",
            "quantity": Decimal("12.0000000000"),
            "cost_basis": Decimal("3.0000000000"),
            "mark_price": Decimal("0.3000000000"),
            "mark_value": Decimal("3.6000000000"),
        }
    ]
    assert report["summary"]["net_trading_pnl"] == Decimal("0.6000000000")


def test_replay_polymarket_cashflows_tracks_split_merge_complete_set_legs() -> None:
    report = replay_polymarket_cashflows(
        [
            {
                "id": "split-set",
                "type": "SPLIT",
                "market_slug": "france",
                "event_slug": "world-cup",
                "yes_token_id": "france-yes",
                "no_token_id": "france-no",
                "amount": "2",
                "size": "2",
                "block_number": 100,
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
                "block_number": 101,
            },
        ],
        mark_prices={
            "france|world-cup|france-yes|YES": "0.30",
            "france|world-cup|france-no|NO": "0.70",
        },
    )

    summary = report["summary"]
    assert summary["cashflow_total"] == Decimal("-1.0000000000")
    assert summary["unrealized_position_value"] == Decimal("1.0000000000")
    assert summary["net_trading_pnl"] == Decimal("0E-10")
    assert summary["portfolio_cash_at_risk"] == Decimal("1.0000000000")
    assert summary["event_cash_at_risk"] == {"world-cup": Decimal("1.0000000000")}
    assert report["ledger"][0]["meta"]["position_legs"] == [
        {
            "position_key": "france|world-cup|france-yes|YES",
            "token_id": "france-yes",
            "token_side": "YES",
            "shares_delta": Decimal("2.0000000000"),
            "cost_basis_delta": Decimal("1.0000000000"),
        },
        {
            "position_key": "france|world-cup|france-no|NO",
            "token_id": "france-no",
            "token_side": "NO",
            "shares_delta": Decimal("2.0000000000"),
            "cost_basis_delta": Decimal("1.0000000000"),
        },
    ]
    assert report["ledger"][1]["meta"]["position_legs"][0]["shares_delta"] == Decimal("-1.0000000000")
    assert report["residual_positions"] == [
        {
            "position_key": "france|world-cup|france-no|NO",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-no",
            "token_side": "NO",
            "quantity": Decimal("1.0000000000"),
            "cost_basis": Decimal("0.5000000000"),
            "mark_price": Decimal("0.7000000000"),
            "mark_value": Decimal("0.7000000000"),
        },
        {
            "position_key": "france|world-cup|france-yes|YES",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "quantity": Decimal("1.0000000000"),
            "cost_basis": Decimal("0.5000000000"),
            "mark_price": Decimal("0.3000000000"),
            "mark_value": Decimal("0.3000000000"),
        },
    ]
