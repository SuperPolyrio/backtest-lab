from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from quant.backtest.runners.account_timeline import apply_bankroll_constraints, build_account_timeline
from quant.backtest.runners.analysis import build_probability_bucket_rows
from quant.backtest.runners.nba_pregame_hold import (
    NbaFavoriteReplayDataset,
    _build_favorite_trades,
    _build_trades,
    _market_event_time,
    _normalize_orderfilled_rows,
    run_nba_pregame_favorite_hold_from_dataset,
    run_nba_pregame_favorite_hold_sweep_from_dataset,
)
from quant.backtest.runners.selectors import ResolvedMarketCandidate
from quant.backtest.runners.strategy_lab import FavoriteHoldStrategySpec, mark_skipped_status, select_trade_candidates


pytestmark = pytest.mark.backtest_validation


def test_pregame_limit_fill_uses_only_future_crossing_inside_window():
    market = _market(settlement_code=1)
    rows = [
        _row(price="0.59", block=10, log=1, hour=17, minute=10),  # outside 6h-4h window
        _row(price="0.59", block=11, log=1, hour=17, minute=31),  # signal
        _row(price="0.57", block=11, log=0, hour=17, minute=31),  # before signal crossing, must not fill
        _row(price="0.61", block=12, log=1, hour=17, minute=40),  # not crossing for BUY limit 0.59
        _row(price="0.58", block=13, log=1, hour=18, minute=0),  # valid fill
        _row(price="0.50", block=14, log=1, hour=19, minute=45),  # after window, ignored
    ]

    trades = _build_trades(
        [market],
        {market.market_id: rows},
        lower=Decimal("0.58"),
        upper=Decimal("0.62"),
        target=Decimal("0.60"),
        yes_only=False,
        window_start_hours=Decimal("6"),
        window_end_hours=Decimal("4"),
        order_size=Decimal("1"),
        liquidity_cap_pct=Decimal("100"),
    )

    assert len(trades) == 1
    trade = trades[0]
    assert trade.order_status == "FILLED"
    assert trade.signal_block == 11
    assert trade.signal_log_index == 1
    assert trade.limit_price == "0.59"
    assert trade.crossing_trade_price == "0.58"
    assert trade.fill_block == 13
    assert trade.buy_price == "0.58"
    assert trade.payoff == "1"
    assert trade.pnl_per_share == "0.42"


def test_pregame_limit_order_expires_when_no_future_crossing():
    market = _market(settlement_code=2)
    rows = [
        _row(price="0.60", block=20, log=1, hour=17, minute=35),
        _row(price="0.61", block=21, log=1, hour=18, minute=0),
        _row(price="0.62", block=22, log=1, hour=19, minute=0),
    ]

    trades = _build_trades(
        [market],
        {market.market_id: rows},
        lower=Decimal("0.58"),
        upper=Decimal("0.62"),
        target=Decimal("0.60"),
        yes_only=True,
        window_start_hours=Decimal("6"),
        window_end_hours=Decimal("4"),
        order_size=Decimal("1"),
        liquidity_cap_pct=Decimal("100"),
    )

    assert len(trades) == 1
    trade = trades[0]
    assert trade.order_status == "EXPIRED"
    assert trade.filled_size == "0"
    assert trade.payoff == "0"
    assert trade.pnl_per_share == "0"
    assert trade.fill_block == 0


def test_pregame_default_strategy_ignores_no_side_signals():
    market = _market(settlement_code=2)
    rows = [
        _row(price="0.60", block=30, log=1, hour=17, minute=35, outcome_code=2, token_id="token-no"),
        _row(price="0.59", block=31, log=1, hour=17, minute=45, outcome_code=2, token_id="token-no"),
        _row(price="0.60", block=32, log=1, hour=18, minute=0, outcome_code=1, token_id="token-yes"),
        _row(price="0.59", block=33, log=1, hour=18, minute=5, outcome_code=1, token_id="token-yes"),
    ]

    trades = _build_trades(
        [market],
        {market.market_id: rows},
        lower=Decimal("0.58"),
        upper=Decimal("0.62"),
        target=Decimal("0.60"),
        yes_only=True,
        window_start_hours=Decimal("6"),
        window_end_hours=Decimal("4"),
        order_size=Decimal("1"),
        liquidity_cap_pct=Decimal("100"),
    )

    assert len(trades) == 1
    trade = trades[0]
    assert trade.buy_outcome_label == "YES"
    assert trade.signal_block == 32
    assert trade.fill_block == 33


def test_favorite_strategy_buys_high_probability_side_with_fixed_stake():
    market = _market(settlement_code=2)
    rows = [
        _row(price="0.38", block=40, log=1, hour=20, minute=1, outcome_code=1, token_id="token-yes"),
        _row(price="0.62", block=41, log=1, hour=20, minute=5, outcome_code=2, token_id="token-no", size="30"),
        _row(price="0.61", block=42, log=1, hour=20, minute=10, outcome_code=2, token_id="token-no", size="30"),
    ]

    trades = _build_favorite_trades(
        [market],
        {market.market_id: rows},
        spec=FavoriteHoldStrategySpec(
            min_probability=Decimal("0.60"),
            snapshot_hours_before_start=None,
            window_start_hours=Decimal("4"),
            window_end_hours=Decimal("0"),
            stake=Decimal("10"),
            liquidity_cap_pct=Decimal("100"),
            yes_only=False,
        ),
    )

    assert len(trades) == 1
    trade = trades[0]
    assert trade.buy_outcome_label == "NO"
    assert trade.signal_source_outcome_code == 1
    assert trade.signal_source_price == "0.38"
    assert trade.signal_probability == "0.62"
    assert trade.order_status == "FILLED"
    assert Decimal(trade.requested_shares).quantize(Decimal("0.0001")) == Decimal("16.1290")
    assert Decimal(trade.buy_cost).quantize(Decimal("0.01")) == Decimal("10.08")
    assert Decimal(trade.settlement_value).quantize(Decimal("0.01")) == Decimal("16.13")
    assert Decimal(trade.pnl).quantize(Decimal("0.01")) == Decimal("6.05")


def test_strategy_spec_daily_cap_keeps_highest_probability_signals():
    rows = [
        _favorite_trade(probability="0.61", signal_hour=18, market_id=10, slug="game-a"),
        _favorite_trade(probability="0.79", signal_hour=19, market_id=11, slug="game-b"),
        _favorite_trade(probability="0.72", signal_hour=20, market_id=12, slug="game-c"),
    ]
    limited = select_trade_candidates(
        rows,
        max_daily_trades=2,
        sort_by="probability_desc",
        probability_getter=lambda row: Decimal(row.signal_probability),
        signal_time_getter=lambda row: row.signal_time,
        rows_getter=lambda row: row.raw_rows_for_outcome,
        mark_skipped=lambda row: mark_skipped_status(row),
        min_probability=Decimal("0.60"),
    )

    kept = [row.market_slug for row in limited if row.order_status == "FILLED"]
    skipped = [row.market_slug for row in limited if row.order_status == "SKIPPED_MAX_DAILY_TRADES"]
    assert set(kept) == {"game-b", "game-c"}
    assert skipped == ["game-a"]


def test_probability_desc_now_prefers_close_line_edge():
    rows = [
        _favorite_trade(probability="0.79", signal_hour=18, market_id=40, slug="expensive-favorite", close_line_edge="+0.01"),
        _favorite_trade(probability="0.67", signal_hour=19, market_id=41, slug="better-edge", close_line_edge="+0.08"),
    ]
    limited = select_trade_candidates(
        rows,
        max_daily_trades=1,
        sort_by="probability_desc",
        probability_getter=lambda row: Decimal(row.signal_probability),
        signal_time_getter=lambda row: row.signal_time,
        rows_getter=lambda row: row.raw_rows_for_outcome,
        mark_skipped=lambda row: mark_skipped_status(row),
        min_probability=Decimal("0.60"),
    )

    kept = [row.market_slug for row in limited if row.order_status == "FILLED"]
    skipped = [row.market_slug for row in limited if row.order_status == "SKIPPED_MAX_DAILY_TRADES"]
    assert kept == ["better-edge"]
    assert skipped == ["expensive-favorite"]


def test_probability_buckets_and_account_timeline_are_reportable():
    rows = [
        _favorite_trade(probability="0.61", signal_hour=18, market_id=20, slug="game-1", pnl="2", settlement_value="12"),
        _favorite_trade(probability="0.67", signal_hour=19, market_id=21, slug="game-2", pnl="-10", settlement_value="0"),
    ]

    buckets = build_probability_bucket_rows(rows)
    timeline = build_account_timeline(rows, initial_capital=Decimal("100"))

    assert [bucket.bucket_label for bucket in buckets] == ["0.6-0.65", "0.65-0.7"]
    assert buckets[0].trade_count == 1
    assert buckets[1].total_pnl == "-10"
    assert [row.event_type for row in timeline] == ["BUY", "BUY", "SETTLEMENT", "SETTLEMENT"]
    assert timeline[1].locked_cost_after == "20"
    assert timeline[-1].cash_after == "92"


def test_bankroll_constraints_enforce_daily_cost_and_position_caps():
    rows = [
        _build_favorite_trade(
            market_id=30,
            market_slug="game-1",
            signal_time=datetime(2024, 10, 23, 18, 0, tzinfo=timezone.utc).isoformat(),
            end_date=datetime(2024, 10, 24, 1, 0, tzinfo=timezone.utc).isoformat(),
            max_daily_cost=20,
            max_concurrent_positions_limit=1,
        ),
        _build_favorite_trade(
            market_id=31,
            market_slug="game-2",
            signal_time=datetime(2024, 10, 23, 19, 0, tzinfo=timezone.utc).isoformat(),
            end_date=datetime(2024, 10, 24, 2, 0, tzinfo=timezone.utc).isoformat(),
            max_daily_cost=20,
            max_concurrent_positions_limit=1,
        ),
        _build_favorite_trade(
            market_id=32,
            market_slug="game-3",
            signal_time=datetime(2024, 10, 23, 20, 0, tzinfo=timezone.utc).isoformat(),
            end_date=datetime(2024, 10, 24, 3, 0, tzinfo=timezone.utc).isoformat(),
            max_daily_cost=10,
            max_concurrent_positions_limit=None,
        ),
    ]

    constrained = apply_bankroll_constraints(rows, initial_capital=Decimal("100"))

    assert constrained[0].order_status == "FILLED"
    assert constrained[1].order_status == "SKIPPED_MAX_CONCURRENT_POSITIONS"
    assert constrained[2].order_status == "SKIPPED_MAX_DAILY_COST"


def test_favorite_dataset_can_be_reused_for_strategy_sweep():
    market = _market(settlement_code=1)
    rows = _normalize_orderfilled_rows(
        [
            _row(price="0.62", block=50, log=1, hour=20, minute=0, size="30"),
            _row(price="0.61", block=51, log=1, hour=20, minute=10, size="30"),
            _row(price="0.72", block=52, log=1, hour=20, minute=20, size="30"),
            _row(price="0.71", block=53, log=1, hour=20, minute=30, size="30"),
        ]
    )
    dataset = NbaFavoriteReplayDataset(
        markets=[market],
        raw_rows=rows,
        by_market={market.market_id: rows},
        load_window_start_hours="4",
        load_window_end_hours="0",
        db_query_sec=1.23,
        raw_market_count=1,
        raw_row_count=len(rows),
    )
    low_spec = FavoriteHoldStrategySpec(
        min_probability=Decimal("0.60"),
        max_probability=Decimal("0.65"),
        snapshot_hours_before_start=None,
        window_start_hours=Decimal("4"),
        window_end_hours=Decimal("0"),
        stake=Decimal("10"),
    )
    high_spec = FavoriteHoldStrategySpec(
        min_probability=Decimal("0.70"),
        max_probability=Decimal("0.75"),
        snapshot_hours_before_start=None,
        window_start_hours=Decimal("4"),
        window_end_hours=Decimal("0"),
        stake=Decimal("10"),
    )

    low_report = run_nba_pregame_favorite_hold_from_dataset(dataset, spec=low_spec)
    high_report = run_nba_pregame_favorite_hold_from_dataset(dataset, spec=high_spec)
    sweep = run_nba_pregame_favorite_hold_sweep_from_dataset(
        dataset,
        specs=[low_spec, high_spec],
        db_query_sec=dataset.db_query_sec,
    )

    assert low_report.trades == 1
    assert high_report.trades == 1
    assert low_report.trade_rows[0].signal_probability == "0.62"
    assert high_report.trade_rows[0].signal_probability == "0.72"
    assert sweep.strategy_count == 2
    assert sweep.db_query_sec == 1.23
    assert [report.trades for report in sweep.reports] == [low_report.trades, high_report.trades]


def test_favorite_report_pnl_is_sourced_from_settlement_ledger():
    market = _market(settlement_code=1)
    rows = _normalize_orderfilled_rows(
        [
            _row(price="0.62", block=60, log=1, hour=20, minute=0, size="30"),
            _row(price="0.61", block=61, log=1, hour=20, minute=10, size="30"),
        ]
    )
    dataset = NbaFavoriteReplayDataset(
        markets=[market],
        raw_rows=rows,
        by_market={market.market_id: rows},
        load_window_start_hours="4",
        load_window_end_hours="0",
        db_query_sec=0,
        raw_market_count=1,
        raw_row_count=len(rows),
    )
    report = run_nba_pregame_favorite_hold_from_dataset(
        dataset,
        spec=FavoriteHoldStrategySpec(
            min_probability=Decimal("0.60"),
            max_probability=Decimal("0.65"),
            snapshot_hours_before_start=None,
            window_start_hours=Decimal("4"),
            window_end_hours=Decimal("0"),
            stake=Decimal("10"),
        ),
    )

    assert report.trades == 1
    assert report.trade_exit_pnl == "0"
    assert report.settlement_pnl == report.total_pnl
    assert Decimal(report.ending_capital) == Decimal(report.initial_capital) + Decimal(report.total_pnl)


def test_execution_profile_stress_reduces_fill_and_pnl():
    market = _market(settlement_code=1)
    rows = _normalize_orderfilled_rows(
        [
            _row(price="0.60", block=70, log=1, hour=20, minute=0, size="10"),
            _row(price="0.59", block=71, log=1, hour=20, minute=10, size="10"),
        ]
    )
    optimistic = _build_favorite_trades(
        [market],
        {market.market_id: rows},
        spec=FavoriteHoldStrategySpec(
            min_probability=Decimal("0.60"),
            max_probability=Decimal("0.80"),
            snapshot_hours_before_start=None,
            window_start_hours=Decimal("4"),
            window_end_hours=Decimal("0"),
            stake=Decimal("10"),
            liquidity_cap_pct=Decimal("100"),
            execution_profile="optimistic",
        ),
    )[0]
    stress = _build_favorite_trades(
        [market],
        {market.market_id: rows},
        spec=FavoriteHoldStrategySpec(
            min_probability=Decimal("0.60"),
            max_probability=Decimal("0.80"),
            snapshot_hours_before_start=None,
            window_start_hours=Decimal("4"),
            window_end_hours=Decimal("0"),
            stake=Decimal("10"),
            liquidity_cap_pct=Decimal("100"),
            execution_profile="stress",
        ),
    )[0]

    assert Decimal(stress.filled_size) < Decimal(optimistic.filled_size)
    assert Decimal(stress.buy_cost) < Decimal(optimistic.buy_cost)
    assert Decimal(stress.roi) < Decimal(optimistic.roi)


def test_market_event_time_uses_slug_date_when_db_end_date_is_shifted():
    shifted = ResolvedMarketCandidate(
        market_id=2,
        market_slug="nba-was-hou-2024-11-11",
        title="Wizards vs Rockets",
        end_date=datetime(2024, 11, 19, 1, 0, tzinfo=timezone.utc),
        settlement_code=1,
        settlement_outcome="YES",
    )

    assert _market_event_time(shifted) == datetime(2024, 11, 11, 1, 0, tzinfo=timezone.utc)


def _market(*, settlement_code: int) -> ResolvedMarketCandidate:
    return ResolvedMarketCandidate(
        market_id=1,
        market_slug="nba-test-2024-10-23",
        title="Test vs Test",
        end_date=datetime(2024, 10, 23, 23, 30, tzinfo=timezone.utc),
        settlement_code=settlement_code,
        settlement_outcome="YES" if settlement_code == 1 else "NO",
    )


def _row(
    *,
    price: str,
    block: int,
    log: int,
    hour: int,
    minute: int,
    outcome_code: int = 1,
    token_id: str = "token-yes",
    size: str = "5",
) -> dict[str, object]:
    return {
        "market_id": 1,
        "outcome_code": outcome_code,
        "token_id": token_id,
        "block_number": block,
        "log_index": log,
        "tx_hash": f"0x{block:04x}{log:04x}",
        "price": price,
        "size": size,
        "block_time": datetime(2024, 10, 23, hour, minute, tzinfo=timezone.utc),
    }


def _favorite_trade(
    *,
    probability: str,
    signal_hour: int,
    market_id: int,
    slug: str,
    pnl: str = "5",
    settlement_value: str = "15",
    **overrides,
):
    base = dict(
        market_id=market_id,
        market_slug=slug,
        signal_probability=probability,
        signal_time=datetime(2024, 10, 23, signal_hour, 0, tzinfo=timezone.utc).isoformat(),
        buy_cost="10",
        settlement_value=settlement_value,
        pnl=pnl,
    )
    base.update(overrides)
    return _build_favorite_trade(**base)


def _build_favorite_trade(**overrides):
    from quant.backtest.runners.nba_pregame_hold import NbaFavoriteHoldTrade

    base = dict(
        market_id=1,
        market_slug="game",
        title="Game",
        end_date=datetime(2024, 10, 23, 23, 0, tzinfo=timezone.utc).isoformat(),
        buy_outcome_code=1,
        buy_outcome_label="YES",
        settlement_code=1,
        settlement_outcome="YES",
        window_start=datetime(2024, 10, 23, 19, 0, tzinfo=timezone.utc).isoformat(),
        window_end=datetime(2024, 10, 23, 23, 0, tzinfo=timezone.utc).isoformat(),
        signal_time=datetime(2024, 10, 23, 22, 0, tzinfo=timezone.utc).isoformat(),
        signal_source_outcome_code=1,
        signal_source_price="0.61",
        signal_probability="0.61",
        close_line_probability="0.63",
        close_line_trade_price="0.63",
        snapshot_drift="+0.02",
        close_line_edge="+0.02",
        limit_price="0.61",
        stake="10",
        max_daily_cost=None,
        max_concurrent_positions_limit=None,
        requested_shares="16.393442623",
        filled_size="16.393442623",
        buy_cost="10",
        crossing_trade_price="0.61",
        order_status="FILLED",
        payoff_per_share="1",
        settlement_value="15",
        pnl="5",
        roi="0.5",
        signal_block=1,
        signal_log_index=1,
        fill_block=2,
        fill_log_index=1,
        token_id="token-yes",
        raw_rows_for_outcome=100,
    )
    base.update(overrides)
    return NbaFavoriteHoldTrade(**base)
