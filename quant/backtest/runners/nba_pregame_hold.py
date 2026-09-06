"""NBA 2024/25 pregame 60/40 hold-to-settlement validation runner."""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import sys
import time
from typing import Any

from ..execution_profiles import apply_adverse_slippage, profile_defaults
from ..ledger import build_ledger_rows, ledger_summary
from ...core.db import ClickHouseClient
from .account_timeline import AccountTimelineRow, apply_bankroll_constraints, build_account_timeline, max_concurrent_exposure
from .analysis import DailyTradeRow, DriftBucketRow, ProbabilityBucketRow, build_daily_trade_rows, build_drift_bucket_rows, build_probability_bucket_rows
from .base import MemorySampler, Timer
from .block_replay_store import (
    BlockReplayBackfillResult,
    backfill_orderfilled_block_replay,
    load_orderfilled_block_replay_rows_for_ranges,
)
from .execution_replay import OrderIntent, ReplayTradeEvent, replay_limit_order, sequence_key
from .fast_accurate import build_fast_accurate_rows as build_generic_fast_accurate_rows
from .fast_accurate import summarize_fast_accurate_rows
from .selectors import ResolvedMarketCandidate, select_nba_2024_25_moneyline_markets
from .strategy_lab import FavoriteHoldStrategySpec, mark_skipped_status, select_trade_candidates


@dataclass(frozen=True)
class NbaPregameHoldTrade:
    market_id: int
    market_slug: str
    title: str
    end_date: str | None
    buy_outcome_code: int
    buy_outcome_label: str
    settlement_code: int
    settlement_outcome: str
    window_start: str
    window_end: str
    signal_price: str
    limit_price: str
    buy_price: str
    crossing_trade_price: str
    order_status: str
    filled_size: str
    payoff: str
    pnl_per_share: str
    roi: str
    signal_block: int
    signal_log_index: int
    fill_block: int
    fill_log_index: int
    token_id: str
    raw_rows_for_outcome: int


@dataclass(frozen=True)
class NbaPregameHoldReport:
    run_id: str
    market_count: int
    raw_market_count: int
    signal_count: int
    trades: int
    no_fills: int
    skipped_without_60_40: int
    wins: int
    losses: int
    win_rate: str
    total_cost: str
    total_payoff: str
    total_pnl: str
    total_roi: str
    db_query_sec: float
    engine_sec: float
    total_runtime_sec: float
    peak_memory_mb: float
    trade_rows: list[NbaPregameHoldTrade]


@dataclass(frozen=True)
class NbaFavoriteHoldTrade:
    market_id: int
    market_slug: str
    title: str
    end_date: str | None
    buy_outcome_code: int
    buy_outcome_label: str
    settlement_code: int
    settlement_outcome: str
    window_start: str
    window_end: str
    signal_time: str
    signal_source_outcome_code: int
    signal_source_price: str
    signal_probability: str
    close_line_probability: str
    close_line_trade_price: str
    snapshot_drift: str
    close_line_edge: str
    limit_price: str
    stake: str
    max_daily_cost: str | None
    max_concurrent_positions_limit: int | None
    requested_shares: str
    filled_size: str
    buy_cost: str
    crossing_trade_price: str
    order_status: str
    payoff_per_share: str
    settlement_value: str
    pnl: str
    roi: str
    signal_block: int
    signal_log_index: int
    fill_block: int
    fill_log_index: int
    token_id: str
    raw_rows_for_outcome: int


@dataclass(frozen=True)
class NbaFavoriteHoldReport:
    run_id: str
    initial_capital: str
    stake: str
    min_probability: str
    max_probability: str
    snapshot_hours_before_start: str
    enforce_bankroll: bool
    max_daily_trades: int | None
    max_daily_cost: str | None
    max_concurrent_positions_limit: int | None
    sort_by: str
    execution_profile: str
    market_count: int
    raw_market_count: int
    signal_count: int
    trades: int
    no_fills: int
    skipped_without_signal: int
    skipped_insufficient_cash: int
    skipped_daily_trade_cap: int
    skipped_daily_cost_cap: int
    skipped_concurrent_positions_cap: int
    wins: int
    losses: int
    win_rate: str
    total_staked: str
    total_cost: str
    total_payoff: str
    total_pnl: str
    trade_exit_pnl: str
    settlement_pnl: str
    fee_total: str
    slippage_total: str
    rebate_total: str
    ending_capital: str
    total_roi_on_cost: str
    total_return_on_initial_capital: str
    avg_pnl: str
    profit_factor: str
    max_drawdown: str
    max_realized_pnl_drawdown: str
    max_concurrent_cost: str
    max_concurrent_positions: int
    db_query_sec: float
    engine_sec: float
    total_runtime_sec: float
    peak_memory_mb: float
    bucket_rows: list[ProbabilityBucketRow]
    drift_bucket_rows: list[DriftBucketRow]
    daily_rows: list[DailyTradeRow]
    timeline_rows: list[AccountTimelineRow]
    trade_rows: list[NbaFavoriteHoldTrade]


@dataclass(frozen=True)
class NbaFavoriteReplayDataset:
    markets: list[ResolvedMarketCandidate]
    raw_rows: list[dict[str, Any]]
    by_market: dict[int, list[dict[str, Any]]]
    load_window_start_hours: str
    load_window_end_hours: str
    db_query_sec: float
    raw_market_count: int
    raw_row_count: int
    replay_mode: str = "accurate"
    replay_table: str | None = None
    backfill_result: BlockReplayBackfillResult | None = None


@dataclass(frozen=True)
class NbaFavoriteHoldSweepReport:
    run_id: str
    strategy_count: int
    market_count: int
    raw_market_count: int
    raw_row_count: int
    db_query_sec: float
    engine_sec: float
    total_runtime_sec: float
    peak_memory_mb: float
    reports: list[NbaFavoriteHoldReport]


@dataclass(frozen=True)
class NbaFastAccurateComparisonRow:
    market_id: int
    market_slug: str
    title: str
    event_time: str | None
    buy_outcome_label: str
    signal_time: str
    fast_status: str
    accurate_status: str
    fast_buy_price: str
    accurate_buy_price: str
    fast_signal_probability: str
    accurate_signal_probability: str
    fast_buy_cost: str
    accurate_buy_cost: str
    fast_filled_size: str
    accurate_filled_size: str
    fast_pnl: str
    accurate_pnl: str
    pnl_diff: str
    fast_signal_block: int
    accurate_signal_block: int
    fast_fill_block: int
    accurate_fill_block: int
    token_id: str
    settlement_outcome: str
    data_quality: str


@dataclass(frozen=True)
class NbaFastAccurateComparisonReport:
    run_id: str
    strategy: dict[str, str | int | bool | None]
    market_count: int
    fast_raw_rows: int
    accurate_raw_rows: int
    fast_trades: int
    accurate_trades: int
    matched_markets: int
    status_mismatches: int
    pnl_diff_abs_total: str
    fast_total_pnl: str
    accurate_total_pnl: str
    fast_db_query_sec: float
    accurate_db_query_sec: float
    fast_engine_sec: float
    accurate_engine_sec: float
    block_replay_backfill_sec: float
    total_runtime_sec: float
    rows: list[NbaFastAccurateComparisonRow]


def run_nba_pregame_hold(
    *,
    limit: int = 200,
    lower: Decimal = Decimal("0.58"),
    upper: Decimal = Decimal("0.62"),
    target: Decimal = Decimal("0.60"),
    yes_only: bool = True,
    window_start_hours: Decimal = Decimal("6"),
    window_end_hours: Decimal = Decimal("4"),
    order_size: Decimal = Decimal("1"),
    liquidity_cap_pct: Decimal = Decimal("100"),
) -> NbaPregameHoldReport:
    timer = Timer()
    start = time.perf_counter()
    memory = MemorySampler.peak_mb()
    with timer.track("db_query"):
        markets = select_nba_2024_25_moneyline_markets(limit=limit)
        raw_rows = _load_window_orderfilled_rows(
            markets,
            window_start_hours=window_start_hours,
            window_end_hours=window_end_hours,
        )
        raw_rows = _normalize_orderfilled_rows(raw_rows)
    with timer.track("engine"):
        by_market = _group_rows_by_market(raw_rows)
        trades = _build_trades(
            markets,
            by_market,
            lower=lower,
            upper=upper,
            target=target,
            yes_only=yes_only,
            window_start_hours=window_start_hours,
            window_end_hours=window_end_hours,
            order_size=order_size,
            liquidity_cap_pct=liquidity_cap_pct,
        )
    filled_trades = [trade for trade in trades if trade.order_status in {"FILLED", "PARTIAL_FILLED"}]
    wins = sum(1 for trade in filled_trades if Decimal(trade.payoff) > 0)
    total_cost = sum((Decimal(trade.buy_price) * Decimal(trade.filled_size) for trade in filled_trades), Decimal("0"))
    total_payoff = sum((Decimal(trade.payoff) * Decimal(trade.filled_size) for trade in filled_trades), Decimal("0"))
    total_pnl = total_payoff - total_cost
    total_roi = total_pnl / total_cost if total_cost else Decimal("0")
    raw_market_count = len({_row_market_id(row) for row in raw_rows})
    no_fills = sum(1 for trade in trades if trade.order_status not in {"FILLED", "PARTIAL_FILLED"})
    return NbaPregameHoldReport(
        run_id="nba_pregame_60_40_hold_yes_only" if yes_only else "nba_pregame_60_40_hold",
        market_count=len(markets),
        raw_market_count=raw_market_count,
        signal_count=len(trades),
        trades=len(filled_trades),
        no_fills=no_fills,
        skipped_without_60_40=len(markets) - len(trades),
        wins=wins,
        losses=len(filled_trades) - wins,
        win_rate=_decimal_text(Decimal(wins) / Decimal(len(filled_trades)) if filled_trades else Decimal("0")),
        total_cost=_decimal_text(total_cost),
        total_payoff=_decimal_text(total_payoff),
        total_pnl=_decimal_text(total_pnl),
        total_roi=_decimal_text(total_roi),
        db_query_sec=round(timer.elapsed.get("db_query", 0.0), 6),
        engine_sec=round(timer.elapsed.get("engine", 0.0), 6),
        total_runtime_sec=round(time.perf_counter() - start, 6),
        peak_memory_mb=memory,
        trade_rows=trades,
    )


def run_nba_pregame_favorite_hold(
    *,
    limit: int = 2000,
    min_probability: Decimal = Decimal("0.60"),
    max_probability: Decimal = Decimal("0.80"),
    snapshot_hours_before_start: Decimal | None = Decimal("1"),
    signal_lookback_hours: Decimal = Decimal("24"),
    window_start_hours: Decimal = Decimal("4"),
    window_end_hours: Decimal = Decimal("0"),
    initial_capital: Decimal = Decimal("1000"),
    stake: Decimal = Decimal("10"),
    liquidity_cap_pct: Decimal = Decimal("100"),
    enforce_bankroll: bool = True,
    max_daily_trades: int | None = None,
    max_daily_cost: Decimal | None = None,
    max_concurrent_positions: int | None = None,
    sort_by: str = "probability_desc",
    replay_mode: str = "accurate",
    force_block_replay_backfill: bool = False,
    yes_only: bool = True,
    execution_profile: str = "realistic",
) -> NbaFavoriteHoldReport:
    spec = FavoriteHoldStrategySpec(
        min_probability=min_probability,
        max_probability=max_probability,
        snapshot_hours_before_start=snapshot_hours_before_start,
        signal_lookback_hours=signal_lookback_hours,
        window_start_hours=window_start_hours,
        window_end_hours=window_end_hours,
        initial_capital=initial_capital,
        stake=stake,
        liquidity_cap_pct=liquidity_cap_pct,
        enforce_bankroll=enforce_bankroll,
        max_daily_trades=max_daily_trades,
        max_daily_cost=max_daily_cost,
        max_concurrent_positions=max_concurrent_positions,
        sort_by=sort_by,
        yes_only=yes_only,
        execution_profile=execution_profile,
    )
    start = time.perf_counter()
    memory = MemorySampler.peak_mb()
    dataset = load_nba_favorite_replay_dataset(
        limit=limit,
        specs=[spec],
        replay_mode=replay_mode,
        force_block_replay_backfill=force_block_replay_backfill,
    )
    return run_nba_pregame_favorite_hold_from_dataset(
        dataset,
        spec=spec,
        db_query_sec=dataset.db_query_sec,
        total_start=start,
        peak_memory_mb=memory,
    )


def load_nba_favorite_replay_dataset(
    *,
    limit: int = 2000,
    specs: list[FavoriteHoldStrategySpec] | None = None,
    window_start_hours: Decimal | None = None,
    window_end_hours: Decimal | None = None,
    snapshot_hours_before_start: Decimal | None = None,
    signal_lookback_hours: Decimal | None = None,
    replay_mode: str = "accurate",
    force_block_replay_backfill: bool = False,
) -> NbaFavoriteReplayDataset:
    """Load NBA replay rows once so many strategy specs can reuse them."""

    markets = select_nba_2024_25_moneyline_markets(limit=limit)
    return load_favorite_replay_dataset_for_markets(
        markets,
        specs=specs,
        window_start_hours=window_start_hours,
        window_end_hours=window_end_hours,
        snapshot_hours_before_start=snapshot_hours_before_start,
        signal_lookback_hours=signal_lookback_hours,
        replay_mode=replay_mode,
        force_block_replay_backfill=force_block_replay_backfill,
        build_tag="nba_fast_replay",
    )


def load_favorite_replay_dataset_for_markets(
    markets: list[ResolvedMarketCandidate],
    *,
    specs: list[FavoriteHoldStrategySpec] | None = None,
    window_start_hours: Decimal | None = None,
    window_end_hours: Decimal | None = None,
    snapshot_hours_before_start: Decimal | None = None,
    signal_lookback_hours: Decimal | None = None,
    replay_mode: str = "accurate",
    force_block_replay_backfill: bool = False,
    build_tag: str = "generic_fast_replay",
) -> NbaFavoriteReplayDataset:
    """Load replay rows for an already-resolved universe of markets."""

    effective_specs = specs or [
        FavoriteHoldStrategySpec(
            window_start_hours=window_start_hours or Decimal("4"),
            window_end_hours=window_end_hours or Decimal("0"),
            snapshot_hours_before_start=Decimal("1") if snapshot_hours_before_start is None else snapshot_hours_before_start,
            signal_lookback_hours=signal_lookback_hours or Decimal("24"),
        )
    ]
    timer = Timer()
    backfill_result: BlockReplayBackfillResult | None = None
    with timer.track("db_query"):
        load_start_hours = max(_load_start_hours_for_spec(spec) for spec in effective_specs)
        load_end_hours = min(spec.window_end_hours for spec in effective_specs)
        if replay_mode == "fast":
            raw_rows, backfill_result = _load_window_orderfilled_block_replay_rows(
                markets,
                window_start_hours=load_start_hours,
                window_end_hours=load_end_hours,
                force_backfill=force_block_replay_backfill,
                build_tag=build_tag,
            )
        elif replay_mode == "accurate":
            raw_rows = _load_window_orderfilled_rows(
                markets,
                window_start_hours=load_start_hours,
                window_end_hours=load_end_hours,
            )
        else:
            raise ValueError("replay_mode must be 'accurate' or 'fast'")
        raw_rows = _normalize_orderfilled_rows(raw_rows)
        by_market = _group_rows_by_market(raw_rows)
    return NbaFavoriteReplayDataset(
        markets=markets,
        raw_rows=raw_rows,
        by_market=by_market,
        load_window_start_hours=_decimal_text(load_start_hours),
        load_window_end_hours=_decimal_text(load_end_hours),
        db_query_sec=round(timer.elapsed.get("db_query", 0.0), 6),
        raw_market_count=len({int(row["market_id"]) for row in raw_rows}),
        raw_row_count=len(raw_rows),
        replay_mode=replay_mode,
        replay_table="orderfilled_block_replay" if replay_mode == "fast" else "orderfilled_fact",
        backfill_result=backfill_result,
    )


def run_nba_pregame_favorite_hold_from_dataset(
    dataset: NbaFavoriteReplayDataset,
    *,
    spec: FavoriteHoldStrategySpec,
    db_query_sec: float = 0.0,
    total_start: float | None = None,
    peak_memory_mb: float | None = None,
) -> NbaFavoriteHoldReport:
    start = total_start if total_start is not None else time.perf_counter()
    memory = MemorySampler.peak_mb() if peak_memory_mb is None else peak_memory_mb
    timer = Timer()
    with timer.track("engine"):
        trade_rows = _build_favorite_trades(dataset.markets, dataset.by_market, spec=spec)
        trade_rows = _apply_favorite_strategy_constraints(trade_rows, spec)
    filled_trades = [row for row in trade_rows if Decimal(row.filled_size) > 0]
    wins = sum(1 for row in filled_trades if Decimal(row.pnl) > 0)
    losses = sum(1 for row in filled_trades if Decimal(row.pnl) < 0)
    ledger_rows = _favorite_trades_to_ledger_rows(filled_trades, initial_capital=spec.initial_capital)
    ledger_account = ledger_summary(ledger_rows, spec.initial_capital)
    pnls = [Decimal(row.pnl) for row in filled_trades]
    positive_pnl = sum((pnl for pnl in pnls if pnl > 0), Decimal("0"))
    negative_pnl = sum((pnl for pnl in pnls if pnl < 0), Decimal("0"))
    total_staked = spec.stake * Decimal(len(filled_trades))
    total_cost = sum((Decimal(row.buy_cost) for row in filled_trades), Decimal("0"))
    total_payoff = sum((Decimal(row.settlement_value) for row in filled_trades), Decimal("0"))
    total_pnl = ledger_account["realized_pnl"]
    ending_capital = ledger_account["cash_balance"]
    no_fills = sum(1 for row in trade_rows if Decimal(row.filled_size) <= 0)
    skipped_insufficient_cash = sum(1 for row in trade_rows if row.order_status == "SKIPPED_INSUFFICIENT_CASH")
    skipped_daily_trade_cap = sum(1 for row in trade_rows if row.order_status == "SKIPPED_MAX_DAILY_TRADES")
    skipped_daily_cost_cap = sum(1 for row in trade_rows if row.order_status == "SKIPPED_MAX_DAILY_COST")
    skipped_concurrent_positions_cap = sum(1 for row in trade_rows if row.order_status == "SKIPPED_MAX_CONCURRENT_POSITIONS")
    realized_pnl_drawdown = _max_drawdown(pnls)
    timeline_rows = build_account_timeline(filled_trades, initial_capital=spec.initial_capital)
    max_concurrent_cost, max_concurrent_positions = max_concurrent_exposure(filled_trades)
    bucket_rows = build_probability_bucket_rows(filled_trades)
    drift_bucket_rows = build_drift_bucket_rows(filled_trades)
    daily_rows = build_daily_trade_rows(trade_rows)
    return NbaFavoriteHoldReport(
        run_id="nba_pregame_favorite_snapshot_hold",
        initial_capital=_decimal_text(spec.initial_capital),
        stake=_decimal_text(spec.stake),
        min_probability=_decimal_text(spec.min_probability),
        max_probability=_decimal_text(spec.max_probability),
        snapshot_hours_before_start=_decimal_text(spec.snapshot_hours_before_start) if spec.snapshot_hours_before_start is not None else "",
        enforce_bankroll=spec.enforce_bankroll,
        max_daily_trades=spec.max_daily_trades,
        max_daily_cost=_optional_decimal_text(spec.max_daily_cost),
        max_concurrent_positions_limit=spec.max_concurrent_positions,
        sort_by=spec.sort_by,
        execution_profile=spec.execution_profile,
        market_count=len(dataset.markets),
        raw_market_count=dataset.raw_market_count,
        signal_count=len(trade_rows),
        trades=len(filled_trades),
        no_fills=no_fills,
        skipped_without_signal=len(dataset.markets) - len(trade_rows),
        skipped_insufficient_cash=skipped_insufficient_cash,
        skipped_daily_trade_cap=skipped_daily_trade_cap,
        skipped_daily_cost_cap=skipped_daily_cost_cap,
        skipped_concurrent_positions_cap=skipped_concurrent_positions_cap,
        wins=wins,
        losses=losses,
        win_rate=_decimal_text(Decimal(wins) / Decimal(len(filled_trades)) if filled_trades else Decimal("0")),
        total_staked=_decimal_text(total_staked),
        total_cost=_decimal_text(total_cost),
        total_payoff=_decimal_text(total_payoff),
        total_pnl=_decimal_text(total_pnl),
        trade_exit_pnl=_decimal_text(ledger_account["trade_exit_pnl"]),
        settlement_pnl=_decimal_text(ledger_account["settlement_pnl"]),
        fee_total=_decimal_text(ledger_account["fee_total"]),
        slippage_total=_decimal_text(ledger_account["slippage_total"]),
        rebate_total=_decimal_text(ledger_account["rebate_total"]),
        ending_capital=_decimal_text(ending_capital),
        total_roi_on_cost=_decimal_text(total_pnl / total_cost if total_cost else Decimal("0")),
        total_return_on_initial_capital=_decimal_text(total_pnl / spec.initial_capital if spec.initial_capital else Decimal("0")),
        avg_pnl=_decimal_text(total_pnl / Decimal(len(filled_trades)) if filled_trades else Decimal("0")),
        profit_factor=_decimal_text(positive_pnl / abs(negative_pnl) if negative_pnl else Decimal("0")),
        max_drawdown=_decimal_text(realized_pnl_drawdown),
        max_realized_pnl_drawdown=_decimal_text(realized_pnl_drawdown),
        max_concurrent_cost=_decimal_text(max_concurrent_cost),
        max_concurrent_positions=max_concurrent_positions,
        db_query_sec=round(db_query_sec, 6),
        engine_sec=round(timer.elapsed.get("engine", 0.0), 6),
        total_runtime_sec=round(time.perf_counter() - start, 6),
        peak_memory_mb=memory,
        bucket_rows=bucket_rows,
        drift_bucket_rows=drift_bucket_rows,
        daily_rows=daily_rows,
        timeline_rows=timeline_rows,
        trade_rows=trade_rows,
    )


def run_nba_pregame_favorite_hold_sweep(
    *,
    specs: list[FavoriteHoldStrategySpec],
    limit: int = 2000,
) -> NbaFavoriteHoldSweepReport:
    if not specs:
        raise ValueError("at least one strategy spec is required")
    start = time.perf_counter()
    memory = MemorySampler.peak_mb()
    dataset = load_nba_favorite_replay_dataset(limit=limit, specs=specs)
    return run_nba_pregame_favorite_hold_sweep_from_dataset(
        dataset,
        specs=specs,
        db_query_sec=dataset.db_query_sec,
        total_start=start,
        peak_memory_mb=memory,
    )


def run_nba_pregame_favorite_hold_sweep_from_dataset(
    dataset: NbaFavoriteReplayDataset,
    *,
    specs: list[FavoriteHoldStrategySpec],
    db_query_sec: float = 0.0,
    total_start: float | None = None,
    peak_memory_mb: float | None = None,
) -> NbaFavoriteHoldSweepReport:
    if not specs:
        raise ValueError("at least one strategy spec is required")
    start = total_start if total_start is not None else time.perf_counter()
    memory = MemorySampler.peak_mb() if peak_memory_mb is None else peak_memory_mb
    engine_start = time.perf_counter()
    reports = [
        run_nba_pregame_favorite_hold_from_dataset(
            dataset,
            spec=spec,
            db_query_sec=0.0,
            total_start=time.perf_counter(),
            peak_memory_mb=memory,
        )
        for spec in specs
    ]
    engine_sec = time.perf_counter() - engine_start
    return NbaFavoriteHoldSweepReport(
        run_id="nba_pregame_favorite_sweep",
        strategy_count=len(specs),
        market_count=len(dataset.markets),
        raw_market_count=dataset.raw_market_count,
        raw_row_count=dataset.raw_row_count,
        db_query_sec=round(db_query_sec, 6),
        engine_sec=round(engine_sec, 6),
        total_runtime_sec=round(time.perf_counter() - start, 6),
        peak_memory_mb=memory,
        reports=reports,
    )


def run_nba_fast_accurate_comparison(
    *,
    limit: int = 500,
    min_probability: Decimal = Decimal("0.60"),
    max_probability: Decimal = Decimal("0.80"),
    snapshot_hours_before_start: Decimal | None = Decimal("1"),
    signal_lookback_hours: Decimal = Decimal("24"),
    window_start_hours: Decimal = Decimal("1"),
    window_end_hours: Decimal = Decimal("0"),
    initial_capital: Decimal = Decimal("1000"),
    stake: Decimal = Decimal("10"),
    liquidity_cap_pct: Decimal = Decimal("100"),
    max_daily_trades: int | None = None,
    max_daily_cost: Decimal | None = None,
    max_concurrent_positions: int | None = None,
    sort_by: str = "probability_desc",
    force_block_replay_backfill: bool = False,
    yes_only: bool = True,
    execution_profile: str = "realistic",
) -> NbaFastAccurateComparisonReport:
    """Run the same NBA strategy through fast block replay and accurate raw replay."""

    start = time.perf_counter()
    spec = FavoriteHoldStrategySpec(
        min_probability=min_probability,
        max_probability=max_probability,
        snapshot_hours_before_start=snapshot_hours_before_start,
        signal_lookback_hours=signal_lookback_hours,
        window_start_hours=window_start_hours,
        window_end_hours=window_end_hours,
        initial_capital=initial_capital,
        stake=stake,
        liquidity_cap_pct=liquidity_cap_pct,
        enforce_bankroll=True,
        max_daily_trades=max_daily_trades,
        max_daily_cost=max_daily_cost,
        max_concurrent_positions=max_concurrent_positions,
        sort_by=sort_by,
        yes_only=yes_only,
        execution_profile=execution_profile,
    )
    fast_dataset = load_nba_favorite_replay_dataset(
        limit=limit,
        specs=[spec],
        replay_mode="fast",
        force_block_replay_backfill=force_block_replay_backfill,
    )
    fast_report = run_nba_pregame_favorite_hold_from_dataset(
        fast_dataset,
        spec=spec,
        db_query_sec=fast_dataset.db_query_sec,
        total_start=time.perf_counter(),
    )
    accurate_dataset = load_nba_favorite_replay_dataset(
        limit=limit,
        specs=[spec],
        replay_mode="accurate",
    )
    accurate_report = run_nba_pregame_favorite_hold_from_dataset(
        accurate_dataset,
        spec=spec,
        db_query_sec=accurate_dataset.db_query_sec,
        total_start=time.perf_counter(),
    )
    rows = [
        NbaFastAccurateComparisonRow(**row.__dict__)
        for row in build_generic_fast_accurate_rows(fast_report.trade_rows, accurate_report.trade_rows)
    ]
    summary = summarize_fast_accurate_rows(rows)
    backfill_sec = fast_dataset.backfill_result.elapsed_sec if fast_dataset.backfill_result else 0.0
    return NbaFastAccurateComparisonReport(
        run_id="nba_favorite_fast_vs_accurate",
        strategy={
            "min_probability": _decimal_text(min_probability),
            "max_probability": _decimal_text(max_probability),
            "snapshot_hours_before_start": _decimal_text(snapshot_hours_before_start) if snapshot_hours_before_start is not None else None,
            "signal_lookback_hours": _decimal_text(signal_lookback_hours),
            "window_start_hours": _decimal_text(window_start_hours),
            "window_end_hours": _decimal_text(window_end_hours),
            "initial_capital": _decimal_text(initial_capital),
            "stake": _decimal_text(stake),
            "liquidity_cap_pct": _decimal_text(liquidity_cap_pct),
            "max_daily_trades": max_daily_trades,
            "max_daily_cost": _optional_decimal_text(max_daily_cost),
            "max_concurrent_positions": max_concurrent_positions,
            "sort_by": sort_by,
            "yes_only": yes_only,
            "execution_profile": execution_profile,
        },
        market_count=limit,
        fast_raw_rows=fast_dataset.raw_row_count,
        accurate_raw_rows=accurate_dataset.raw_row_count,
        fast_trades=fast_report.trades,
        accurate_trades=accurate_report.trades,
        matched_markets=summary.matched_markets,
        status_mismatches=summary.status_mismatches,
        pnl_diff_abs_total=summary.pnl_diff_abs_total,
        fast_total_pnl=summary.fast_total_pnl,
        accurate_total_pnl=summary.accurate_total_pnl,
        fast_db_query_sec=fast_dataset.db_query_sec,
        accurate_db_query_sec=accurate_dataset.db_query_sec,
        fast_engine_sec=fast_report.engine_sec,
        accurate_engine_sec=accurate_report.engine_sec,
        block_replay_backfill_sec=round(backfill_sec, 6),
        total_runtime_sec=round(time.perf_counter() - start, 6),
        rows=rows,
    )


def _build_fast_accurate_rows(
    fast_report: NbaFavoriteHoldReport,
    accurate_report: NbaFavoriteHoldReport,
) -> list[NbaFastAccurateComparisonRow]:
    fast_by_market = {row.market_id: row for row in fast_report.trade_rows}
    accurate_by_market = {row.market_id: row for row in accurate_report.trade_rows}
    rows: list[NbaFastAccurateComparisonRow] = []
    for market_id in sorted(set(fast_by_market) | set(accurate_by_market)):
        fast = fast_by_market.get(market_id)
        accurate = accurate_by_market.get(market_id)
        template = fast or accurate
        if template is None:
            continue
        fast_pnl = Decimal(fast.pnl) if fast else Decimal("0")
        accurate_pnl = Decimal(accurate.pnl) if accurate else Decimal("0")
        rows.append(
            NbaFastAccurateComparisonRow(
                market_id=market_id,
                market_slug=template.market_slug,
                title=template.title,
                event_time=template.end_date,
                buy_outcome_label=template.buy_outcome_label,
                signal_time=template.signal_time,
                fast_status=fast.order_status if fast else "NO_SIGNAL",
                accurate_status=accurate.order_status if accurate else "NO_SIGNAL",
                fast_buy_price=fast.limit_price if fast else "",
                accurate_buy_price=accurate.limit_price if accurate else "",
                fast_signal_probability=fast.signal_probability if fast else "",
                accurate_signal_probability=accurate.signal_probability if accurate else "",
                fast_buy_cost=fast.buy_cost if fast else "0",
                accurate_buy_cost=accurate.buy_cost if accurate else "0",
                fast_filled_size=fast.filled_size if fast else "0",
                accurate_filled_size=accurate.filled_size if accurate else "0",
                fast_pnl=_decimal_text(fast_pnl),
                accurate_pnl=_decimal_text(accurate_pnl),
                pnl_diff=_signed_decimal_text(fast_pnl - accurate_pnl),
                fast_signal_block=fast.signal_block if fast else 0,
                accurate_signal_block=accurate.signal_block if accurate else 0,
                fast_fill_block=fast.fill_block if fast else 0,
                accurate_fill_block=accurate.fill_block if accurate else 0,
                token_id=template.token_id,
                settlement_outcome=template.settlement_outcome,
                data_quality=_comparison_quality(fast, accurate),
            )
        )
    return rows


def _comparison_quality(
    fast: NbaFavoriteHoldTrade | None,
    accurate: NbaFavoriteHoldTrade | None,
) -> str:
    if fast is None:
        return "accurate_only_signal"
    if accurate is None:
        return "fast_only_signal"
    if fast.order_status != accurate.order_status:
        return "status_mismatch"
    if Decimal(fast.pnl) != Decimal(accurate.pnl):
        return "pnl_drift"
    return "matched"


def _load_start_hours_for_spec(spec: FavoriteHoldStrategySpec) -> Decimal:
    if spec.snapshot_hours_before_start is None:
        return spec.window_start_hours
    return max(spec.window_start_hours, spec.snapshot_hours_before_start + spec.signal_lookback_hours)


def _apply_favorite_strategy_constraints(
    trade_rows: list[NbaFavoriteHoldTrade],
    spec: FavoriteHoldStrategySpec,
) -> list[NbaFavoriteHoldTrade]:
    trade_rows = select_trade_candidates(
        trade_rows,
        max_daily_trades=spec.max_daily_trades,
        sort_by=spec.sort_by,
        probability_getter=lambda row: Decimal(row.signal_probability),
        signal_time_getter=lambda row: row.signal_time,
        rows_getter=lambda row: row.raw_rows_for_outcome,
        mark_skipped=lambda row: mark_skipped_status(row),
        min_probability=spec.min_probability,
    )
    if spec.enforce_bankroll:
        trade_rows = apply_bankroll_constraints(trade_rows, initial_capital=spec.initial_capital)
    return trade_rows


def _load_window_orderfilled_rows(
    markets: list[ResolvedMarketCandidate],
    *,
    window_start_hours: Decimal,
    window_end_hours: Decimal,
) -> list[dict[str, Any]]:
    if not markets:
        return []
    dated_markets = [market for market in markets if market.end_date is not None]
    if not dated_markets:
        return []
    ids = ",".join(str(market.market_id) for market in markets)
    starts = [
        _market_event_time(market) - timedelta(hours=float(window_start_hours))
        for market in dated_markets
        if market.end_date is not None
    ]
    ends = [
        _market_event_time(market) - timedelta(hours=float(window_end_hours))
        for market in dated_markets
        if market.end_date is not None
    ]
    global_start = min(starts).strftime("%Y-%m-%d %H:%M:%S")
    global_end = max(ends).strftime("%Y-%m-%d %H:%M:%S")
    client = ClickHouseClient()
    block_min, block_max = _block_range_for_time_window(client, global_start=global_start, global_end=global_end)
    market_block_ranges = _block_ranges_for_market_windows(
        client,
        dated_markets,
        window_start_hours=window_start_hours,
        window_end_hours=window_end_hours,
    )
    prewhere_filters = [f"market_id IN ({ids})"]
    if block_min is not None and block_max is not None:
        prewhere_filters.append(f"block_number BETWEEN {block_min} AND {block_max}")
    prewhere_sql = " AND ".join(prewhere_filters)
    range_sql = _market_block_range_condition(market_block_ranges)
    if range_sql == "":
        return []
    return client.query_json_rows(
        f"""
        SELECT
            f.market_id,
            f.outcome_code,
            f.token_id,
            f.block_number,
            f.log_index,
            lower(f.tx_hash) AS tx_hash,
            f.price,
            f.size,
            bt.block_time
        FROM (
            SELECT
                market_id,
                outcome_code,
                token_id,
                block_number,
                log_index,
                tx_hash,
                price,
                size
            FROM orderfilled_fact
            PREWHERE {prewhere_sql}
        ) f
        INNER JOIN block_timestamps bt ON bt.block_number = f.block_number
        WHERE ({range_sql})
        ORDER BY f.market_id ASC, f.outcome_code ASC, f.block_number ASC, f.log_index ASC, f.tx_hash ASC
        """
    )


def _load_window_orderfilled_block_replay_rows(
    markets: list[ResolvedMarketCandidate],
    *,
    window_start_hours: Decimal,
    window_end_hours: Decimal,
    force_backfill: bool = False,
    build_tag: str = "nba_fast_replay",
) -> tuple[list[dict[str, Any]], BlockReplayBackfillResult | None]:
    if not markets:
        return [], None
    dated_markets = [market for market in markets if market.end_date is not None]
    if not dated_markets:
        return [], None
    global_start, global_end = _global_time_window(
        dated_markets,
        window_start_hours=window_start_hours,
        window_end_hours=window_end_hours,
    )
    client = ClickHouseClient()
    block_min, block_max = _block_range_for_time_window(client, global_start=global_start, global_end=global_end)
    if block_min is None or block_max is None:
        return [], None
    market_block_ranges = _block_ranges_for_market_windows(
        client,
        dated_markets,
        window_start_hours=window_start_hours,
        window_end_hours=window_end_hours,
    )
    if not market_block_ranges:
        return [], None
    market_ids = [market.market_id for market in markets]
    backfill = backfill_orderfilled_block_replay(
        market_ids,
        from_block=block_min,
        to_block=block_max,
        client=client,
        force=force_backfill,
        build_tag=build_tag,
    )
    rows = load_orderfilled_block_replay_rows_for_ranges(
        market_block_ranges,
        client=client,
    )
    return rows, backfill


def _global_time_window(
    markets: list[ResolvedMarketCandidate],
    *,
    window_start_hours: Decimal,
    window_end_hours: Decimal,
) -> tuple[str, str]:
    starts = [
        _market_event_time(market) - timedelta(hours=float(window_start_hours))
        for market in markets
        if market.end_date is not None
    ]
    ends = [
        _market_event_time(market) - timedelta(hours=float(window_end_hours))
        for market in markets
        if market.end_date is not None
    ]
    return min(starts).strftime("%Y-%m-%d %H:%M:%S"), max(ends).strftime("%Y-%m-%d %H:%M:%S")


def _block_ranges_for_market_windows(
    client: ClickHouseClient,
    markets: list[ResolvedMarketCandidate],
    *,
    window_start_hours: Decimal,
    window_end_hours: Decimal,
) -> dict[int, tuple[int, int]]:
    windows: list[tuple[int, datetime, datetime]] = []
    for market in markets:
        if market.end_date is None:
            continue
        event_time = _market_event_time(market)
        start_time = event_time - timedelta(hours=float(window_start_hours))
        end_time = event_time - timedelta(hours=float(window_end_hours))
        windows.append((int(market.market_id), start_time, end_time))
    if not windows:
        return {}
    global_start = min(start for _, start, _ in windows) - timedelta(days=1)
    global_end = max(end for _, _, end in windows) + timedelta(days=1)
    anchors = _load_daily_block_anchors(client, global_start=global_start, global_end=global_end)
    if not anchors:
        return {}
    padding_blocks = 3000
    min_known = min(anchor["min_block"] for anchor in anchors.values())
    max_known = max(anchor["max_block"] for anchor in anchors.values())
    ranges: dict[int, tuple[int, int]] = {}
    for market_id, start_time, end_time in windows:
        start_block = _estimate_block_from_anchors(anchors, start_time)
        end_block = _estimate_block_from_anchors(anchors, end_time)
        if start_block is None or end_block is None:
            continue
        ranges[market_id] = (
            max(min_known, min(start_block, end_block) - padding_blocks),
            min(max_known, max(start_block, end_block) + padding_blocks),
        )
    return ranges


def _load_daily_block_anchors(
    client: ClickHouseClient,
    *,
    global_start: datetime,
    global_end: datetime,
) -> dict[str, dict[str, Any]]:
    start_text = global_start.strftime("%Y-%m-%d %H:%M:%S")
    end_text = global_end.strftime("%Y-%m-%d %H:%M:%S")
    rows = client.query_json_rows(
        f"""
        SELECT
            toString(toDate(block_time)) AS day,
            min(block_number) AS min_block,
            max(block_number) AS max_block,
            min(block_time) AS min_time,
            max(block_time) AS max_time
        FROM block_timestamps
        WHERE block_time >= toDateTime('{start_text}', 'UTC')
          AND block_time <= toDateTime('{end_text}', 'UTC')
        GROUP BY day
        ORDER BY day
        """,
        timeout_seconds=120,
    )
    anchors: dict[str, dict[str, Any]] = {}
    for row in rows:
        day = str(row.get("day") or "")
        if not day:
            continue
        anchors[day] = {
            "min_block": int(row["min_block"]),
            "max_block": int(row["max_block"]),
            "min_time": _as_utc(_parse_block_time(row["min_time"])),
            "max_time": _as_utc(_parse_block_time(row["max_time"])),
        }
    return anchors


def _estimate_block_from_anchors(anchors: dict[str, dict[str, Any]], timestamp: datetime) -> int | None:
    day = timestamp.astimezone(timezone.utc).date().isoformat()
    anchor = anchors.get(day)
    if anchor is None:
        return None
    min_time = anchor["min_time"]
    max_time = anchor["max_time"]
    min_block = int(anchor["min_block"])
    max_block = int(anchor["max_block"])
    total_seconds = max(Decimal("1"), Decimal(str((max_time - min_time).total_seconds())))
    offset_seconds = Decimal(str((timestamp.astimezone(timezone.utc) - min_time).total_seconds()))
    ratio = max(Decimal("0"), min(Decimal("1"), offset_seconds / total_seconds))
    return int(Decimal(min_block) + (Decimal(max_block - min_block) * ratio))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _market_block_range_condition(ranges: dict[int, tuple[int, int]]) -> str:
    if not ranges:
        return ""
    return " OR ".join(
        f"(f.market_id = {int(market_id)} AND f.block_number BETWEEN {int(bounds[0])} AND {int(bounds[1])})"
        for market_id, bounds in sorted(ranges.items())
    )


def _block_range_for_time_window(
    client: ClickHouseClient,
    *,
    global_start: str,
    global_end: str,
) -> tuple[int | None, int | None]:
    rows = client.query_json_rows(
        f"""
        SELECT
            min(block_number) AS min_block,
            max(block_number) AS max_block
        FROM block_timestamps
        WHERE block_time >= toDateTime('{global_start}', 'UTC')
          AND block_time <= toDateTime('{global_end}', 'UTC')
        """,
        timeout_seconds=30,
    )
    if not rows:
        return None, None
    row = rows[0]
    min_block = row.get("min_block")
    max_block = row.get("max_block")
    if min_block in (None, "") or max_block in (None, ""):
        return None, None
    return int(min_block), int(max_block)


def _normalize_orderfilled_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        block_time = _parse_block_time(row.get("block_time"))
        item = dict(row)
        item["_market_id"] = int(row["market_id"])
        item["_outcome_code"] = int(row["outcome_code"])
        item["_token_id"] = str(row["token_id"])
        item["_block_number"] = int(row["block_number"])
        item["_log_index"] = int(row.get("log_index", row.get("last_log_index", 0)))
        item["_tx_hash"] = str(row.get("tx_hash", row.get("last_tx_hash", "")))
        item["_block_time"] = block_time
        item["_price"] = Decimal(str(row.get("price", row.get("close_price"))))
        item["_size"] = Decimal(str(row.get("size", row.get("volume", "0"))))
        if "low_price" in row:
            item["_low_price"] = Decimal(str(row["low_price"]))
        if "high_price" in row:
            item["_high_price"] = Decimal(str(row["high_price"]))
        if "close_price" in row:
            item["_close_price"] = Decimal(str(row["close_price"]))
        item["_replay_source"] = str(row.get("replay_source", "orderfilled_fact"))
        normalized.append(item)
    return normalized


def _group_rows_by_market(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_row_market_id(row), []).append(row)
    for market_rows in grouped.values():
        market_rows.sort(key=lambda row: (_row_block_number(row), _row_log_index(row), _row_tx_hash(row)))
    return grouped


def _build_trades(
    markets: list[ResolvedMarketCandidate],
    by_market: dict[int, list[dict[str, Any]]],
    *,
    lower: Decimal,
    upper: Decimal,
    target: Decimal,
    yes_only: bool,
    window_start_hours: Decimal,
    window_end_hours: Decimal,
    order_size: Decimal,
    liquidity_cap_pct: Decimal,
) -> list[NbaPregameHoldTrade]:
    trades: list[NbaPregameHoldTrade] = []
    for market in markets:
        if market.end_date is None:
            continue
        event_time = _market_event_time(market)
        window_start = event_time - timedelta(hours=float(window_start_hours))
        window_end = event_time - timedelta(hours=float(window_end_hours))
        market_rows = [
            row for row in by_market.get(market.market_id, [])
            if window_start <= _row_block_time(row, event_time) <= window_end
        ]
        raw_rows_by_outcome = _count_by_outcome(market_rows)
        signals: list[tuple[tuple[int, int, str], Decimal, dict[str, Any]]] = []
        for row in market_rows:
            outcome_code = _row_outcome_code(row)
            if yes_only and outcome_code != 1:
                continue
            price = _row_price(row)
            if lower <= price <= upper:
                signals.append((
                    (_row_block_number(row), _row_log_index(row), _row_tx_hash(row)),
                    abs(price - target),
                    row,
                ))
        if not signals:
            continue
        _, _, row = sorted(signals, key=lambda item: (item[0], item[1]))[0]
        limit_price = Decimal(str(row["price"]))
        buy_code = int(row["outcome_code"])
        signal_sequence = _after_signal_sequence(row)
        replay_events = [
            ReplayTradeEvent(
                market_id=market.market_id,
                token_id=_row_token_id(event),
                block_number=_row_block_number(event),
                log_index=_row_log_index(event),
                tx_hash=_row_tx_hash(event),
                trade_price=_row_buy_cross_price(event),
                size=_row_size(event),
            )
            for event in market_rows
            if _row_outcome_code(event) == buy_code and _row_token_id(event) == _row_token_id(row)
        ]
        fill = replay_limit_order(
            OrderIntent(
                side="BUY_YES",
                limit_price=limit_price,
                size=order_size,
                time_in_force="GTD",
                submit_sequence=signal_sequence,
                expire_sequence=_last_sequence(replay_events) or signal_sequence,
                liquidity_cap_pct=liquidity_cap_pct,
                order_id=f"NBA-{market.market_id}-{buy_code}",
            ),
            replay_events,
        )
        payoff = Decimal("1") if buy_code == market.settlement_code else Decimal("0")
        buy_price = fill.avg_fill_price if fill.filled_size > 0 else limit_price
        pnl = (payoff - buy_price) if fill.filled_size > 0 else Decimal("0")
        roi = pnl / buy_price if buy_price and fill.filled_size > 0 else Decimal("0")
        crossing = fill.candidate_events[0] if fill.candidate_events else None
        trades.append(
            NbaPregameHoldTrade(
                market_id=market.market_id,
                market_slug=market.market_slug,
                title=market.title,
                end_date=event_time.isoformat(),
                buy_outcome_code=buy_code,
                buy_outcome_label="YES" if buy_code == 1 else "NO",
                settlement_code=market.settlement_code,
                settlement_outcome=market.settlement_outcome,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
                signal_price=_decimal_text(limit_price),
                limit_price=_decimal_text(limit_price),
                buy_price=_decimal_text(buy_price),
                crossing_trade_price=_decimal_text(crossing.trade_price) if crossing else "",
                order_status=fill.status,
                filled_size=_decimal_text(fill.filled_size),
                payoff=_decimal_text(payoff),
                pnl_per_share=_decimal_text(pnl),
                roi=_decimal_text(roi),
                signal_block=_row_block_number(row),
                signal_log_index=_row_log_index(row),
                fill_block=int(crossing.block_number) if crossing else 0,
                fill_log_index=int(crossing.log_index) if crossing else 0,
                token_id=_row_token_id(row),
                raw_rows_for_outcome=raw_rows_by_outcome.get(buy_code, 0),
            )
        )
    return trades


def _build_favorite_trades(
    markets: list[ResolvedMarketCandidate],
    by_market: dict[int, list[dict[str, Any]]],
    *,
    spec: FavoriteHoldStrategySpec,
) -> list[NbaFavoriteHoldTrade]:
    trades: list[NbaFavoriteHoldTrade] = []
    execution_profile = profile_defaults(spec.execution_profile)
    effective_liquidity_cap_pct = (
        spec.liquidity_cap_pct
        * (Decimal("1") - execution_profile.fill_probability_haircut_pct / Decimal("100"))
    )
    for market in markets:
        if market.end_date is None:
            continue
        event_time = _market_event_time(market)
        signal_lookback_start = (
            event_time - timedelta(hours=float(spec.snapshot_hours_before_start + spec.signal_lookback_hours))
            if spec.snapshot_hours_before_start is not None
            else event_time - timedelta(hours=float(spec.window_start_hours))
        )
        window_start = event_time - timedelta(hours=float(spec.window_start_hours))
        window_end = event_time - timedelta(hours=float(spec.window_end_hours))
        market_rows = [
            row for row in by_market.get(market.market_id, [])
            if signal_lookback_start <= _row_block_time(row, event_time) <= window_end
        ]
        if not market_rows:
            continue
        token_by_outcome = _token_by_outcome(market_rows)
        raw_rows_by_outcome = _count_by_outcome(market_rows)
        close_line = _close_line_probability(market_rows, buy_code=None, event_time=event_time, window_end=window_end)
        if spec.snapshot_hours_before_start is not None:
            signal_time = event_time - timedelta(hours=float(spec.snapshot_hours_before_start))
            signal = _favorite_signal_at_snapshot(
                market_rows,
                token_by_outcome,
                spec.min_probability,
                spec.max_probability,
                signal_time,
                event_time,
                yes_only=spec.yes_only,
            )
        else:
            signal_time = window_start
            signal = _first_favorite_signal(
                market_rows,
                token_by_outcome,
                spec.min_probability,
                spec.max_probability,
                yes_only=spec.yes_only,
            )
        if signal is None:
            continue
        signal_row, buy_code, probability, token_id = signal
        close_line = _close_line_probability(market_rows, buy_code=buy_code, event_time=event_time, window_end=window_end)
        close_line_probability = close_line[0] if close_line else probability
        close_line_trade_price = close_line[1] if close_line else probability
        snapshot_drift = close_line_probability - probability
        if int(market.settlement_code or 0) not in {1, 2}:
            trades.append(
                NbaFavoriteHoldTrade(
                    market_id=market.market_id,
                    market_slug=market.market_slug,
                    title=market.title,
                    end_date=event_time.isoformat(),
                    buy_outcome_code=buy_code,
                    buy_outcome_label="YES" if buy_code == 1 else "NO",
                    settlement_code=int(market.settlement_code or 0),
                    settlement_outcome=market.settlement_outcome or "UNRESOLVED",
                    window_start=window_start.isoformat(),
                    window_end=window_end.isoformat(),
                    signal_time=signal_time.isoformat(),
                    signal_source_outcome_code=_row_outcome_code(signal_row),
                    signal_source_price=_decimal_text(_row_price(signal_row)),
                    signal_probability=_decimal_text(probability),
                    close_line_probability=_decimal_text(close_line_probability),
                    close_line_trade_price=_decimal_text(close_line_trade_price),
                    snapshot_drift=_signed_decimal_text(snapshot_drift),
                    close_line_edge=_signed_decimal_text(snapshot_drift),
                    limit_price=_decimal_text(probability),
                    stake=_decimal_text(spec.stake),
                    max_daily_cost=_optional_decimal_text(spec.max_daily_cost),
                    max_concurrent_positions_limit=spec.max_concurrent_positions,
                    requested_shares=_decimal_text((spec.stake / probability) if probability > 0 else Decimal("0")),
                    filled_size="0",
                    buy_cost="0",
                    crossing_trade_price="",
                    order_status="UNRESOLVED",
                    payoff_per_share="0",
                    settlement_value="0",
                    pnl="0",
                    roi="0",
                    signal_block=_row_block_number(signal_row),
                    signal_log_index=_row_log_index(signal_row),
                    fill_block=0,
                    fill_log_index=0,
                    token_id=token_id,
                    raw_rows_for_outcome=raw_rows_by_outcome.get(buy_code, 0),
                )
            )
            continue
        signal_sequence = _after_signal_sequence(signal_row, latency_blocks=execution_profile.latency_blocks)
        replay_events = [
            ReplayTradeEvent(
                market_id=market.market_id,
                token_id=_row_token_id(row),
                block_number=_row_block_number(row),
                log_index=_row_log_index(row),
                tx_hash=_row_tx_hash(row),
                trade_price=_row_buy_cross_price(row),
                size=_row_size(row),
            )
            for row in market_rows
            if _row_outcome_code(row) == buy_code and _row_token_id(row) == token_id
            and _row_block_time(row, event_time) >= signal_time
            and _row_block_time(row, event_time) <= window_end
        ]
        requested_shares = (spec.stake / probability) if probability > 0 else Decimal("0")
        fill = replay_limit_order(
            OrderIntent(
                side="BUY_YES",
                limit_price=probability,
                size=requested_shares,
                time_in_force="GTD",
                submit_sequence=signal_sequence,
                expire_sequence=_last_sequence(replay_events) or signal_sequence,
                liquidity_cap_pct=effective_liquidity_cap_pct,
                order_id=f"NBA-FAV-{market.market_id}-{buy_code}",
            ),
            replay_events,
        )
        crossing = fill.candidate_events[0] if fill.candidate_events else None
        payoff_per_share = Decimal("1") if buy_code == market.settlement_code else Decimal("0")
        execution_price = apply_adverse_slippage(fill.avg_fill_price if fill.filled_size > 0 else probability, execution_profile, "BUY")
        buy_cost = (fill.filled_size * execution_price)
        settlement_value = fill.filled_size * payoff_per_share
        pnl = settlement_value - buy_cost
        trades.append(
            NbaFavoriteHoldTrade(
                market_id=market.market_id,
                market_slug=market.market_slug,
                title=market.title,
                end_date=event_time.isoformat(),
                buy_outcome_code=buy_code,
                buy_outcome_label="YES" if buy_code == 1 else "NO",
                settlement_code=market.settlement_code,
                settlement_outcome=market.settlement_outcome,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
                signal_time=signal_time.isoformat(),
                signal_source_outcome_code=_row_outcome_code(signal_row),
                signal_source_price=_decimal_text(_row_price(signal_row)),
                signal_probability=_decimal_text(probability),
                close_line_probability=_decimal_text(close_line_probability),
                close_line_trade_price=_decimal_text(close_line_trade_price),
                snapshot_drift=_signed_decimal_text(snapshot_drift),
                close_line_edge=_signed_decimal_text(snapshot_drift),
                limit_price=_decimal_text(probability),
                stake=_decimal_text(spec.stake),
                max_daily_cost=_optional_decimal_text(spec.max_daily_cost),
                max_concurrent_positions_limit=spec.max_concurrent_positions,
                requested_shares=_decimal_text(requested_shares),
                filled_size=_decimal_text(fill.filled_size),
                buy_cost=_decimal_text(buy_cost),
                crossing_trade_price=_decimal_text(crossing.trade_price) if crossing else "",
                order_status=fill.status,
                payoff_per_share=_decimal_text(payoff_per_share),
                settlement_value=_decimal_text(settlement_value),
                pnl=_decimal_text(pnl),
                roi=_decimal_text(pnl / buy_cost if buy_cost else Decimal("0")),
                signal_block=_row_block_number(signal_row),
                signal_log_index=_row_log_index(signal_row),
                fill_block=int(crossing.block_number) if crossing else 0,
                fill_log_index=int(crossing.log_index) if crossing else 0,
                token_id=token_id,
                raw_rows_for_outcome=raw_rows_by_outcome.get(buy_code, 0),
            )
        )
    return trades


def _favorite_trades_to_ledger_rows(
    trades: list[NbaFavoriteHoldTrade],
    *,
    initial_capital: Decimal,
) -> list[dict[str, Any]]:
    trade_dicts: list[dict[str, Any]] = []
    for index, trade in enumerate(trades, start=1):
        size = Decimal(trade.filled_size)
        if size <= 0:
            continue
        buy_cost = Decimal(trade.buy_cost)
        entry_price = buy_cost / size if size else Decimal("0")
        exit_price = Decimal(trade.payoff_per_share)
        signal_block = int(trade.signal_block or trade.fill_block or index)
        exit_block = max(signal_block + 1, int(trade.fill_block or signal_block) + 1)
        trade_dicts.append(
            {
                "trade_id": f"{trade.market_id}-{trade.buy_outcome_code}-{index}",
                "entry_order_id": f"BUY-{trade.market_id}-{trade.buy_outcome_code}-{index}",
                "exit_order_id": f"SETTLE-{trade.market_id}-{trade.buy_outcome_code}-{index}",
                "market_slug": trade.market_slug,
                "token_side": trade.buy_outcome_label,
                "x_axis": "block_number",
                "entry_x": signal_block,
                "exit_x": exit_block,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "size": size,
                "pnl": Decimal(trade.pnl),
                "fee_cost": Decimal("0"),
                "slippage_cost": Decimal("0"),
                "entry_fee_cost": Decimal("0"),
                "exit_fee_cost": Decimal("0"),
                "entry_slippage_cost": Decimal("0"),
                "exit_slippage_cost": Decimal("0"),
                "exit_reason": "settlement",
            }
        )
    return build_ledger_rows(trade_dicts, initial_capital)


def _first_favorite_signal(
    rows: list[dict[str, Any]],
    token_by_outcome: dict[int, str],
    min_probability: Decimal,
    max_probability: Decimal,
    *,
    yes_only: bool,
) -> tuple[dict[str, Any], int, Decimal, str] | None:
    for row in rows:
        favorite = _favorite_from_row(row, token_by_outcome, yes_only=yes_only)
        if favorite is None:
            continue
        buy_code, probability, token_id = favorite
        if min_probability <= probability <= max_probability:
            return row, buy_code, probability, token_id
    return None


def _favorite_signal_at_snapshot(
    rows: list[dict[str, Any]],
    token_by_outcome: dict[int, str],
    min_probability: Decimal,
    max_probability: Decimal,
    signal_time: datetime,
    event_time: datetime,
    *,
    yes_only: bool,
) -> tuple[dict[str, Any], int, Decimal, str] | None:
    candidates = [row for row in rows if _row_block_time(row, event_time) <= signal_time]
    for row in reversed(candidates):
        favorite = _favorite_from_row(row, token_by_outcome, yes_only=yes_only)
        if favorite is None:
            continue
        buy_code, probability, token_id = favorite
        if min_probability <= probability <= max_probability:
            return row, buy_code, probability, token_id
        return None
    return None


def _favorite_from_row(
    row: dict[str, Any],
    token_by_outcome: dict[int, str],
    *,
    yes_only: bool,
) -> tuple[int, Decimal, str] | None:
    source_code = _row_outcome_code(row)
    source_price = _row_price(row)
    if yes_only:
        token_id = token_by_outcome.get(1)
        if not token_id:
            return None
        probability = source_price if source_code == 1 else Decimal("1") - source_price
        return 1, probability, token_id
    same_probability = source_price
    opposite_probability = Decimal("1") - source_price
    if same_probability >= opposite_probability:
        buy_code = source_code
        probability = same_probability
    else:
        buy_code = 2 if source_code == 1 else 1
        probability = opposite_probability
    token_id = token_by_outcome.get(buy_code)
    if not token_id:
        return None
    return buy_code, probability, token_id


def _close_line_probability(
    rows: list[dict[str, Any]],
    *,
    buy_code: int | None,
    event_time: datetime,
    window_end: datetime,
) -> tuple[Decimal, Decimal] | None:
    candidates = [row for row in rows if _row_block_time(row, event_time) <= window_end]
    for row in reversed(candidates):
        if buy_code is None:
            continue
        row_code = _row_outcome_code(row)
        row_price = _row_price(row)
        if row_code == buy_code:
            return row_price, row_price
        return Decimal("1") - row_price, row_price
    return None
def _token_by_outcome(rows: list[dict[str, Any]]) -> dict[int, str]:
    tokens: dict[int, str] = {}
    for row in rows:
        code = _row_outcome_code(row)
        tokens.setdefault(code, _row_token_id(row))
    return tokens


def _last_sequence(events: list[ReplayTradeEvent]) -> tuple[int, int, int, str, str] | None:
    if not events:
        return None
    return max((event.event_sequence for event in events))


def _after_signal_sequence(row: dict[str, Any], *, latency_blocks: int = 0) -> tuple[int, int, int, str, str]:
    return sequence_key(
        _row_block_number(row) + max(0, int(latency_blocks)),
        0,
        _row_log_index(row),
        _row_tx_hash(row),
        "~",
    )


def _count_by_outcome(rows: list[dict[str, Any]]) -> dict[int, int]:
    counts: dict[int, int] = {}
    for row in rows:
        code = _row_outcome_code(row)
        counts[code] = counts.get(code, 0) + 1
    return counts


def _row_block_time(row: dict[str, Any], end_date: datetime) -> datetime:
    raw = row.get("_block_time", row["block_time"])
    if isinstance(raw, datetime):
        value = raw
    else:
        value = _parse_block_time(raw)
    if value.tzinfo is None:
        return value.replace(tzinfo=end_date.tzinfo or timezone.utc)
    return value.astimezone(end_date.tzinfo or timezone.utc)


def _parse_block_time(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw
    return datetime.fromisoformat(str(raw).replace(" ", "T"))


def _row_market_id(row: dict[str, Any]) -> int:
    return int(row["_market_id"] if "_market_id" in row else row["market_id"])


def _row_outcome_code(row: dict[str, Any]) -> int:
    return int(row["_outcome_code"] if "_outcome_code" in row else row["outcome_code"])


def _row_token_id(row: dict[str, Any]) -> str:
    return str(row["_token_id"] if "_token_id" in row else row["token_id"])


def _row_block_number(row: dict[str, Any]) -> int:
    return int(row["_block_number"] if "_block_number" in row else row["block_number"])


def _row_log_index(row: dict[str, Any]) -> int:
    if "_log_index" in row:
        return int(row["_log_index"])
    return int(row.get("log_index", row.get("last_log_index", 0)))


def _row_tx_hash(row: dict[str, Any]) -> str:
    if "_tx_hash" in row:
        return str(row["_tx_hash"])
    return str(row.get("tx_hash", row.get("last_tx_hash", "")))


def _row_price(row: dict[str, Any]) -> Decimal:
    value = row.get("_price")
    return value if isinstance(value, Decimal) else Decimal(str(row["price"]))


def _row_buy_cross_price(row: dict[str, Any]) -> Decimal:
    value = row.get("_low_price")
    if isinstance(value, Decimal):
        return value
    if "low_price" in row:
        return Decimal(str(row["low_price"]))
    return _row_price(row)


def _row_size(row: dict[str, Any]) -> Decimal:
    value = row.get("_size")
    return value if isinstance(value, Decimal) else Decimal(str(row["size"]))


def _market_event_time(market: ResolvedMarketCandidate) -> datetime:
    if market.end_date is None:
        raise ValueError(f"market {market.market_slug} has no end_date")
    parts = market.market_slug.split("-")
    try:
        year, month, day = map(int, parts[-3:])
        slug_date = datetime(year, month, day, tzinfo=market.end_date.tzinfo or timezone.utc).date()
    except (TypeError, ValueError):
        return market.end_date
    if abs((market.end_date.date() - slug_date).days) <= 1:
        return market.end_date
    return datetime(
        year,
        month,
        day,
        market.end_date.hour,
        market.end_date.minute,
        market.end_date.second,
        tzinfo=market.end_date.tzinfo or timezone.utc,
    )


def _max_drawdown(pnls: list[Decimal]) -> Decimal:
    peak = Decimal("0")
    equity = Decimal("0")
    max_drawdown = Decimal("0")
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _decimal_text(value: Decimal | int | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(decimal_value.normalize(), "f")


def _optional_decimal_text(value: Decimal | int | str | None) -> str | None:
    return _decimal_text(value) if value is not None else None


def _signed_decimal_text(value: Decimal) -> str:
    text = _decimal_text(value)
    if value > 0:
        return f"+{text}"
    return text


def _report_dict(report: NbaPregameHoldReport) -> dict[str, Any]:
    payload = asdict(report)
    payload["trade_rows"] = [asdict(row) for row in report.trade_rows]
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--lower", default="0.58")
    parser.add_argument("--upper", default="0.62")
    parser.add_argument("--target", default="0.60")
    parser.add_argument("--yes-only", action="store_true", help="Deprecated compatibility flag; YES-only is now the default.")
    parser.add_argument("--allow-no", action="store_true", help="Also allow NO-side 60/40 signals for comparison runs.")
    parser.add_argument("--window-start-hours", default="6")
    parser.add_argument("--window-end-hours", default="4")
    parser.add_argument("--order-size", default="1")
    parser.add_argument("--liquidity-cap-pct", default="100")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--csv", default="")
    args = parser.parse_args(argv)
    report = run_nba_pregame_hold(
        limit=args.limit,
        lower=Decimal(str(args.lower)),
        upper=Decimal(str(args.upper)),
        target=Decimal(str(args.target)),
        yes_only=not args.allow_no,
        window_start_hours=Decimal(str(args.window_start_hours)),
        window_end_hours=Decimal(str(args.window_end_hours)),
        order_size=Decimal(str(args.order_size)),
        liquidity_cap_pct=Decimal(str(args.liquidity_cap_pct)),
    )
    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(asdict(report.trade_rows[0]).keys()) if report.trade_rows else [])
            if report.trade_rows:
                writer.writeheader()
                for row in report.trade_rows:
                    writer.writerow(asdict(row))
    if args.json:
        print(json.dumps(_report_dict(report), ensure_ascii=False, sort_keys=True))
    else:
        print(
            f"{report.run_id}: markets={report.market_count} raw_markets={report.raw_market_count} "
            f"signals={report.signal_count} fills={report.trades} no_fills={report.no_fills} "
            f"wins={report.wins} losses={report.losses} "
            f"pnl={report.total_pnl} roi={report.total_roi}"
        )
        for row in report.trade_rows:
            print(
                f"{row.market_slug} | {row.title} | signal={row.buy_outcome_label} {row.signal_price} "
                f"status={row.order_status} buy={row.buy_price} settle={row.settlement_outcome} "
                f"payoff={row.payoff} pnl={row.pnl_per_share} roi={row.roi}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
