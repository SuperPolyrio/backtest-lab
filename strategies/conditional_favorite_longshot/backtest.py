"""Walk-forward NBA favorite-longshot backtest on the V2 OrderFilled tape."""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable

from quant.backtest.orderfilled_v2_replay import (
    CapacityLedger,
    V2OrderResult,
    V2TakerOrder,
    V2TradePrint,
    replay_v2_taker_orders_with_diagnostics,
    trade_print_from_row,
    with_v2_execution_profile,
)
from quant.backtest.runners.nba_pregame_hold import _market_event_time
from quant.backtest.runners.selectors import ResolvedMarketCandidate, select_nba_2024_25_moneyline_markets
from quant.core.db import ClickHouseClient
from quant.core.metadata import derive_clickhouse_token_id_hex
from quant.prices.block_close_algorithm import INTERNAL_COUNTERPARTIES, quote_clickhouse_string


Q = Decimal("0.0000000001")
DEFAULT_PROFILES = (
    "probabilistic_taker_5s",
    "probabilistic_taker_30s",
    "probabilistic_taker_120s",
    "probabilistic_taker_30s_any_order_side",
    "probabilistic_taker_120s_any_order_side",
)


@dataclass(frozen=True)
class BacktestConfig:
    market_limit: int = 500
    snapshot_minutes_before_start: int = 60
    max_execution_window_seconds: int = 120
    signal_lookback_hours: int = 24
    min_probability: Decimal = Decimal("0.60")
    max_probability: Decimal = Decimal("0.80")
    min_trades: int = 20
    min_samples: int = 20
    prior_strength: Decimal = Decimal("20")
    calibration_price_tolerance: Decimal = Decimal("0.025")
    confidence_z: Decimal = Decimal("1.2815515655")
    edge_threshold: Decimal = Decimal("0.015")
    execution_buffer: Decimal = Decimal("0.010")
    fee_bps: Decimal = Decimal("0")
    stake: Decimal = Decimal("10")
    initial_capital: Decimal = Decimal("1000")
    max_daily_cost: Decimal = Decimal("20")
    max_concurrent_positions: int = 2
    settlement_lag_hours: int = 6
    apply_portfolio_constraints: bool = False
    profiles: tuple[str, ...] = DEFAULT_PROFILES


@dataclass(frozen=True)
class Candidate:
    market_id: int
    market_slug: str
    title: str
    event_time: datetime
    signal_time: datetime
    label_available_at: datetime
    settlement_code: int
    asset_id: str
    outcome_code: int
    favorite_side: str
    signal_price: Decimal
    signal_trade_id: str
    signal_tx_hash: str
    signal_log_indexes: tuple[int, ...]
    signal_block: int
    trailing_trade_count: int
    trailing_volume: Decimal
    price_bucket: str
    liquidity_bucket: str
    won: bool
    winning_asset_id: str = ""
    yes_asset_id: str = ""
    no_asset_id: str = ""
    signal_price_age_seconds: Decimal = Decimal("0")
    pair_timestamp_skew_seconds: Decimal | None = None
    pair_sum_error: Decimal | None = None
    price_source: str = "latest_global_implied_complement"
    won_by_code: bool | None = None


@dataclass(frozen=True)
class Signal:
    candidate: Candidate
    calibration_level: str
    calibration_samples: int
    calibration_wins: int
    prior_win_rate: Decimal
    posterior_win_rate: Decimal
    gross_edge: Decimal
    net_edge: Decimal
    historical_mean_price: Decimal = Decimal("0")
    historical_bias: Decimal = Decimal("0")
    shrunk_bias: Decimal = Decimal("0")
    posterior_lower_bound: Decimal = Decimal("0")


@dataclass(frozen=True)
class TradeResult:
    profile: str
    market_id: int
    market_slug: str
    favorite_side: str
    signal_time: str
    event_time: str
    signal_price: str
    posterior_win_rate: str
    net_edge: str
    calibration_level: str
    calibration_samples: int
    trailing_trade_count: int
    requested_size: str
    status: str
    filled_size: str
    avg_price: str
    fill_probability: str
    conditional_capacity_fraction: str
    capacity_variant: str
    source_trade_ids: tuple[str, ...]
    source_tx_hashes: tuple[str, ...]
    buy_cost: str
    fee: str
    settlement_value: str
    pnl: str
    reason_unfilled: str
    won: bool = False
    winning_asset_id: str = ""
    limit_price: str = "0"
    avg_fill_delay_seconds: str = "0"


@dataclass(frozen=True)
class ProfileSummary:
    profile: str
    signals: int
    filled_orders: int
    partial_orders: int
    no_fill_orders: int
    total_filled_size: str
    total_cost: str
    total_settlement_value: str
    total_fees: str
    total_pnl: str
    roi_on_cost: str
    ending_capital: str
    wins: int
    losses: int
    source_invariant_errors: int


@dataclass(frozen=True)
class BacktestReport:
    strategy: str
    data_source: str
    market_count: int
    tape_rows: int
    raw_candidates: int
    calibrated_signals: int
    selected_signals: int
    skipped_min_samples: int
    skipped_edge: int
    skipped_portfolio_limits: int
    config: dict[str, Any]
    profile_summaries: tuple[ProfileSummary, ...]
    trades: tuple[TradeResult, ...]
    diagnostics: dict[str, Any] = field(default_factory=dict)


def run_nba_backtest(
    config: BacktestConfig = BacktestConfig(),
    *,
    client: ClickHouseClient | None = None,
    markets: list[ResolvedMarketCandidate] | None = None,
    trades: Iterable[V2TradePrint] | None = None,
) -> BacktestReport:
    market_rows = markets or select_nba_2024_25_moneyline_markets(limit=config.market_limit)
    raw_tape = list(trades) if trades is not None else load_orderfilled_tape(market_rows, config=config, client=client)
    tape = _economic_trade_rows(raw_tape)
    candidates = build_candidates(market_rows, tape, config=config)
    signals, skipped_min_samples, skipped_edge = build_walk_forward_signals(candidates, config=config)
    if config.apply_portfolio_constraints:
        selected, skipped_portfolio = apply_portfolio_limits(signals, config=config)
    else:
        selected, skipped_portfolio = list(signals), 0

    all_trade_rows: list[TradeResult] = []
    summaries: list[ProfileSummary] = []
    diagnostics: dict[str, Any] = {
        "candidate_distribution": _candidate_distribution(candidates),
        "economic_tape_normalization": {
            "input_rows": len(raw_tape),
            "one_sided_rows": len(tape),
            "removed_duplicate_side_rows": len(raw_tape) - len(tape),
            "capacity_rule": "max_size_per_market_asset_block_tx_price",
        },
        "settlement_mapping_audit": _settlement_mapping_audit(market_rows, candidates),
        "raw_candidate_source_confirmed": _candidate_execution_coverage(candidates, tape, config=config),
        "alpha_only": _alpha_only_decomposition(signals, config=config),
    }
    for profile in config.profiles:
        orders = [signal_to_order(signal, config=config, profile=profile) for signal in selected]
        results, _, replay_diagnostics = replay_v2_taker_orders_with_diagnostics(
            orders,
            tape,
            ledger=CapacityLedger(),
        )
        rows = [
            result_to_trade(signal, order, result, config=config, profile=profile)
            for signal, order, result in zip(selected, orders, results)
        ]
        all_trade_rows.extend(rows)
        summaries.append(summarize_profile(profile, rows, config=config))
        diagnostics[profile] = replay_diagnostics.as_dict()
        diagnostics[f"{profile}_cohorts"] = _cohort_decomposition(selected, rows, config=config)
        diagnostics[f"{profile}_execution_decomposition"] = _execution_decomposition(selected, rows, config=config)

    return BacktestReport(
        strategy="conditional_favorite_longshot_nba_v2",
        data_source="orderfilled_fact_order_side_proxy_then_one_sided_economic_dedup",
        market_count=len(market_rows),
        tape_rows=len(tape),
        raw_candidates=len(candidates),
        calibrated_signals=len(signals),
        selected_signals=len(selected),
        skipped_min_samples=skipped_min_samples,
        skipped_edge=skipped_edge,
        skipped_portfolio_limits=skipped_portfolio,
        config=_json_ready(asdict(config)),
        profile_summaries=tuple(summaries),
        trades=tuple(all_trade_rows),
        diagnostics=_json_ready(diagnostics),
    )


def load_orderfilled_tape(
    markets: list[ResolvedMarketCandidate],
    *,
    config: BacktestConfig,
    client: ClickHouseClient | None = None,
    batch_size: int = 25,
) -> list[V2TradePrint]:
    """Normalize bounded legacy NBA OrderFilled rows with the V2 one-sided SQL."""

    ch = client or ClickHouseClient()
    rows: list[V2TradePrint] = []
    internal_addresses = ",".join(quote_clickhouse_string(address) for address in INTERNAL_COUNTERPARTIES)
    for offset in range(0, len(markets), max(1, batch_size)):
        batch = [market for market in markets[offset : offset + batch_size] if market.end_date is not None]
        if not batch:
            continue
        ids = ",".join(str(int(market.market_id)) for market in batch)
        conditions = []
        for market in batch:
            event_time = _market_event_time(market)
            start = event_time - timedelta(
                hours=config.signal_lookback_hours,
                minutes=config.snapshot_minutes_before_start,
            )
            end = event_time - timedelta(minutes=config.snapshot_minutes_before_start) + timedelta(
                seconds=config.max_execution_window_seconds + 60
            )
            conditions.append(
                f"(market_id = {int(market.market_id)} "
                f"AND block_time >= toDateTime('{_sql_time(start)}', 'UTC') "
                f"AND block_time <= toDateTime('{_sql_time(end)}', 'UTC'))"
            )
        query_rows = ch.query_json_rows(
            f"""
            SELECT
                lower(hex(SHA256(concat(
                    'trade|137|', toString(block_number), '|', tx_hash, '|',
                    asset_id, '|', aggressor_side, '|', passive_side, '|',
                    toString(trade_price)
                )))) AS trade_id,
                market_id,
                any(condition_id) AS condition_id,
                asset_id,
                outcome,
                block_number,
                block_time,
                tx_hash,
                toUInt32(0) AS tx_index,
                'missing_in_orderfilled_fact' AS tx_index_source,
                trade_price AS price,
                sum(size) AS size_shares,
                toDecimal128(toFloat64(trade_price) * sum(toFloat64(size)), 10) AS notional_usdc,
                aggressor_side,
                passive_side,
                arraySort(groupArray(toUInt32(log_index))) AS source_log_indexes,
                toUInt32(count()) AS source_fill_count
            FROM
            (
                SELECT
                    f.market_id,
                    f.condition_id,
                    lower(f.token_id) AS asset_id,
                    if(f.outcome_code = 1, 'YES', 'NO') AS outcome,
                    f.block_number,
                    bt.block_time,
                    lower(f.tx_hash) AS tx_hash,
                    f.log_index,
                    f.size,
                    multiIf(f.side_code = 1, 'BUY', f.side_code = 2, 'SELL', '') AS aggressor_side,
                    multiIf(f.side_code = 1, 'SELL', f.side_code = 2, 'BUY', '') AS passive_side,
                    if(
                        f.maker_amount IS NOT NULL
                        AND f.taker_amount IS NOT NULL
                        AND f.maker_amount > 0
                        AND f.taker_amount > 0
                        AND f.maker_amount != f.taker_amount,
                        toDecimal128(least(assumeNotNull(f.maker_amount), assumeNotNull(f.taker_amount)), 10)
                            / toDecimal128(greatest(assumeNotNull(f.maker_amount), assumeNotNull(f.taker_amount)), 10),
                        f.price
                    ) AS trade_price
                FROM orderfilled_fact AS f
                INNER JOIN block_timestamps AS bt ON bt.block_number = f.block_number
                PREWHERE f.market_id IN ({ids})
                WHERE f.side_code IN (1, 2)
                  AND f.size > 0
                  AND lower(replaceRegexpOne(f.maker, '^0x', '')) NOT IN ({internal_addresses})
                  AND lower(replaceRegexpOne(f.taker, '^0x', '')) NOT IN ({internal_addresses})
            )
            WHERE ({' OR '.join(conditions)})
              AND trade_price > 0
              AND trade_price <= 1
            GROUP BY
                market_id, asset_id, outcome, block_number, block_time, tx_hash,
                trade_price, aggressor_side, passive_side
            ORDER BY
                market_id, block_number, tx_hash, arrayMin(source_log_indexes), trade_id
            """,
            timeout_seconds=300,
        )
        rows.extend(trade_print_from_row(row) for row in query_rows)
    return sorted(rows, key=lambda trade: trade.sequence)


def build_candidates(
    markets: list[ResolvedMarketCandidate],
    trades: Iterable[V2TradePrint],
    *,
    config: BacktestConfig,
) -> list[Candidate]:
    by_market: dict[int, list[V2TradePrint]] = {}
    for trade in trades:
        by_market.setdefault(int(trade.market_id), []).append(trade)
    candidates: list[Candidate] = []
    for market in markets:
        if market.end_date is None or market.settlement_code not in {1, 2}:
            continue
        event_time = _market_event_time(market)
        signal_time = event_time - timedelta(minutes=config.snapshot_minutes_before_start)
        lookback_start = signal_time - timedelta(hours=config.signal_lookback_hours)
        prior = [
            trade
            for trade in by_market.get(market.market_id, [])
            if lookback_start <= _utc(trade.block_time) <= signal_time
        ]
        state = _synchronized_favorite_state(market, prior, signal_time)
        if state is None:
            continue
        signal_trade, outcome_code, price, asset_id, diagnostics = state
        if price < config.min_probability or price > config.max_probability:
            continue
        same_asset = [trade for trade in prior if trade.asset_id.lower() == asset_id.lower()]
        economic = _economic_trade_rows(same_asset)
        if len(economic) < config.min_trades:
            continue
        volume = sum((trade.size for trade in economic), Decimal("0"))
        yes_asset_id = _market_asset_id(market.token_yes_id, prior, "YES")
        no_asset_id = _market_asset_id(market.token_no_id, prior, "NO")
        winning_asset_id = yes_asset_id if market.settlement_code == 1 else no_asset_id
        won_by_token = bool(winning_asset_id and asset_id.lower() == winning_asset_id.lower())
        won_by_code = outcome_code == market.settlement_code
        candidates.append(
            Candidate(
                market_id=market.market_id,
                market_slug=market.market_slug,
                title=market.title,
                event_time=event_time,
                signal_time=signal_time,
                label_available_at=event_time + timedelta(hours=config.settlement_lag_hours),
                settlement_code=market.settlement_code,
                asset_id=asset_id,
                outcome_code=outcome_code,
                favorite_side="YES" if outcome_code == 1 else "NO",
                signal_price=price,
                signal_trade_id=signal_trade.trade_id,
                signal_tx_hash=signal_trade.tx_hash,
                signal_log_indexes=signal_trade.source_log_indexes,
                signal_block=signal_trade.block_number,
                trailing_trade_count=len(economic),
                trailing_volume=volume,
                price_bucket=_price_bucket(price),
                liquidity_bucket=_liquidity_bucket(len(economic)),
                won=won_by_token,
                winning_asset_id=winning_asset_id,
                yes_asset_id=yes_asset_id,
                no_asset_id=no_asset_id,
                signal_price_age_seconds=diagnostics["signal_price_age_seconds"],
                pair_timestamp_skew_seconds=diagnostics["pair_timestamp_skew_seconds"],
                pair_sum_error=diagnostics["pair_sum_error"],
                won_by_code=won_by_code,
            )
        )
    return sorted(candidates, key=lambda row: (row.signal_time, row.market_id))


def build_walk_forward_signals(
    candidates: Iterable[Candidate],
    *,
    config: BacktestConfig,
) -> tuple[list[Signal], int, int]:
    ordered = sorted(candidates, key=lambda row: (row.signal_time, row.market_id))
    signals: list[Signal] = []
    skipped_min_samples = 0
    skipped_edge = 0
    for current in ordered:
        history = [row for row in ordered if row.label_available_at < current.signal_time]
        matched, level = _calibration_history(current, history, config)
        if len(matched) < config.min_samples:
            skipped_min_samples += 1
            continue
        wins = sum(1 for row in matched if row.won)
        historical_mean_price = sum((row.signal_price for row in matched), Decimal("0")) / Decimal(len(matched))
        historical_bias = sum(
            ((Decimal("1") if row.won else Decimal("0")) - row.signal_price for row in matched),
            Decimal("0"),
        ) / Decimal(len(matched))
        shrinkage = Decimal(len(matched)) / (Decimal(len(matched)) + config.prior_strength)
        shrunk_bias = historical_bias * shrinkage
        posterior = min(Decimal("1"), max(Decimal("0"), current.signal_price + shrunk_bias))
        effective_n = Decimal(len(matched)) + config.prior_strength
        standard_error = (
            posterior * (Decimal("1") - posterior) / max(Decimal("1"), effective_n + Decimal("1"))
        ).sqrt()
        lower_bound = max(Decimal("0"), posterior - config.confidence_z * standard_error)
        gross_edge = posterior - current.signal_price
        fee = current.signal_price * config.fee_bps / Decimal("10000")
        net_edge = lower_bound - current.signal_price - config.execution_buffer - fee
        if net_edge <= config.edge_threshold:
            skipped_edge += 1
            continue
        signals.append(
            Signal(
                candidate=current,
                calibration_level=level,
                calibration_samples=len(matched),
                calibration_wins=wins,
                prior_win_rate=current.signal_price,
                posterior_win_rate=posterior,
                gross_edge=gross_edge,
                net_edge=net_edge,
                historical_mean_price=historical_mean_price,
                historical_bias=historical_bias,
                shrunk_bias=shrunk_bias,
                posterior_lower_bound=lower_bound,
            )
        )
    return signals, skipped_min_samples, skipped_edge


def apply_portfolio_limits(
    signals: Iterable[Signal],
    *,
    config: BacktestConfig,
) -> tuple[list[Signal], int]:
    selected: list[Signal] = []
    daily_cost: dict[str, Decimal] = {}
    active_until: list[datetime] = []
    skipped = 0
    for signal in sorted(signals, key=lambda row: (row.candidate.signal_time, -row.net_edge)):
        now = signal.candidate.signal_time
        active_until = [value for value in active_until if value > now]
        day = now.date().isoformat()
        if daily_cost.get(day, Decimal("0")) + config.stake > config.max_daily_cost:
            skipped += 1
            continue
        if len(active_until) >= config.max_concurrent_positions:
            skipped += 1
            continue
        selected.append(signal)
        daily_cost[day] = daily_cost.get(day, Decimal("0")) + config.stake
        active_until.append(signal.candidate.label_available_at)
    return selected, skipped


def signal_to_order(signal: Signal, *, config: BacktestConfig, profile: str) -> V2TakerOrder:
    candidate = signal.candidate
    limit_price = min(Decimal("0.999"), candidate.signal_price + config.execution_buffer)
    base = V2TakerOrder(
        order_id=f"CFL-{profile}-{candidate.market_id}-{candidate.outcome_code}",
        market_id=candidate.market_id,
        asset_id=candidate.asset_id,
        side="BUY",
        limit_price=limit_price,
        size=(config.stake / limit_price).quantize(Q, rounding=ROUND_HALF_UP),
        # Snapshot time is authoritative; the last signal trade may be hours old.
        signal_block=None,
        signal_ts=candidate.signal_time,
        tif="GTD",
        allow_partial_fill=True,
        signal_source_trade_id=candidate.signal_trade_id,
        signal_source_tx_hash=candidate.signal_tx_hash,
        signal_source_log_indexes=candidate.signal_log_indexes,
        exclude_signal_source_trade=True,
    )
    return replace(with_v2_execution_profile(base, profile), horizon_blocks=None)


def result_to_trade(
    signal: Signal,
    order: V2TakerOrder,
    result: V2OrderResult,
    *,
    config: BacktestConfig,
    profile: str,
) -> TradeResult:
    candidate = signal.candidate
    fee = result.filled_notional * config.fee_bps / Decimal("10000")
    settlement_value = result.filled_size if candidate.won else Decimal("0")
    pnl = settlement_value - result.filled_notional - fee
    return TradeResult(
        profile=profile,
        market_id=candidate.market_id,
        market_slug=candidate.market_slug,
        favorite_side=candidate.favorite_side,
        signal_time=candidate.signal_time.isoformat(),
        event_time=candidate.event_time.isoformat(),
        signal_price=_decimal_text(candidate.signal_price),
        posterior_win_rate=_decimal_text(signal.posterior_win_rate),
        net_edge=_decimal_text(signal.net_edge),
        calibration_level=signal.calibration_level,
        calibration_samples=signal.calibration_samples,
        trailing_trade_count=candidate.trailing_trade_count,
        requested_size=_decimal_text(order.size),
        status=result.status,
        filled_size=_decimal_text(result.filled_size),
        avg_price=_decimal_text(result.avg_price),
        fill_probability=_decimal_text(result.p_fill or Decimal("0")),
        conditional_capacity_fraction=_decimal_text(result.conditional_capacity_fraction or Decimal("0")),
        capacity_variant=result.fill_capacity_variant or "",
        source_trade_ids=tuple(fill.source_trade_id for fill in result.fills),
        source_tx_hashes=tuple(fill.source_tx_hash for fill in result.fills),
        buy_cost=_decimal_text(result.filled_notional),
        fee=_decimal_text(fee),
        settlement_value=_decimal_text(settlement_value),
        pnl=_decimal_text(pnl),
        reason_unfilled=result.reason_unfilled,
        won=candidate.won,
        winning_asset_id=candidate.winning_asset_id,
        limit_price=_decimal_text(order.limit_price),
        avg_fill_delay_seconds=_decimal_text(result.avg_fill_delay_seconds),
    )


def summarize_profile(
    profile: str,
    rows: Iterable[TradeResult],
    *,
    config: BacktestConfig,
) -> ProfileSummary:
    values = list(rows)
    total_size = sum((Decimal(row.filled_size) for row in values), Decimal("0"))
    total_cost = sum((Decimal(row.buy_cost) for row in values), Decimal("0"))
    total_settlement = sum((Decimal(row.settlement_value) for row in values), Decimal("0"))
    total_fees = sum((Decimal(row.fee) for row in values), Decimal("0"))
    total_pnl = sum((Decimal(row.pnl) for row in values), Decimal("0"))
    invariant_errors = sum(
        1
        for row in values
        if Decimal(row.filled_size) > 0 and (not row.source_trade_ids or not row.source_tx_hashes)
    )
    return ProfileSummary(
        profile=profile,
        signals=len(values),
        filled_orders=sum(row.status == "FILLED" for row in values),
        partial_orders=sum(row.status == "PARTIAL_FILLED" for row in values),
        no_fill_orders=sum(row.status == "NO_FILL" for row in values),
        total_filled_size=_decimal_text(total_size),
        total_cost=_decimal_text(total_cost),
        total_settlement_value=_decimal_text(total_settlement),
        total_fees=_decimal_text(total_fees),
        total_pnl=_decimal_text(total_pnl),
        roi_on_cost=_decimal_text(total_pnl / total_cost if total_cost else Decimal("0")),
        ending_capital=_decimal_text(config.initial_capital + total_pnl),
        wins=sum(Decimal(row.pnl) > 0 for row in values),
        losses=sum(Decimal(row.pnl) < 0 for row in values),
        source_invariant_errors=invariant_errors,
    )


def _candidate_execution_coverage(
    candidates: list[Candidate],
    trades: list[V2TradePrint],
    *,
    config: BacktestConfig,
) -> dict[str, Any]:
    profile = "probabilistic_source_confirmed"
    signals = [
        Signal(
            candidate=candidate,
            calibration_level="coverage_only",
            calibration_samples=0,
            calibration_wins=0,
            prior_win_rate=Decimal("0"),
            posterior_win_rate=candidate.signal_price,
            gross_edge=Decimal("0"),
            net_edge=Decimal("0"),
        )
        for candidate in candidates
    ]
    orders = [signal_to_order(signal, config=config, profile=profile) for signal in signals]
    results, _, _ = replay_v2_taker_orders_with_diagnostics(orders, trades, ledger=CapacityLedger())
    reasons: dict[str, int] = {}
    for result in results:
        reason = result.reason_unfilled or "filled"
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "orders": len(results),
        "filled": sum(result.status == "FILLED" for result in results),
        "partial": sum(result.status == "PARTIAL_FILLED" for result in results),
        "no_fill": sum(result.status == "NO_FILL" for result in results),
        "reason_distribution": dict(sorted(reasons.items())),
    }


def _candidate_distribution(candidates: list[Candidate]) -> dict[str, Any]:
    sides: dict[str, int] = {}
    price_buckets: dict[str, int] = {}
    for candidate in candidates:
        sides[candidate.favorite_side] = sides.get(candidate.favorite_side, 0) + 1
        price_buckets[candidate.price_bucket] = price_buckets.get(candidate.price_bucket, 0) + 1
    return {
        "candidates": len(candidates),
        "yes_favorite": sides.get("YES", 0),
        "no_favorite": sides.get("NO", 0),
        "wins": sum(candidate.won for candidate in candidates),
        "losses": sum(not candidate.won for candidate in candidates),
        "price_buckets": dict(sorted(price_buckets.items())),
        "mean_signal_price_age_seconds": _mean_decimal(
            [candidate.signal_price_age_seconds for candidate in candidates]
        ),
    }


def _settlement_mapping_audit(
    markets: list[ResolvedMarketCandidate],
    candidates: list[Candidate],
) -> dict[str, Any]:
    missing_tokens = [
        market.market_id
        for market in markets
        if not derive_clickhouse_token_id_hex(market.token_yes_id)
        or not derive_clickhouse_token_id_hex(market.token_no_id)
    ]
    duplicate_tokens = [
        market.market_id
        for market in markets
        if derive_clickhouse_token_id_hex(market.token_yes_id)
        and derive_clickhouse_token_id_hex(market.token_yes_id)
        == derive_clickhouse_token_id_hex(market.token_no_id)
    ]
    mismatches = [
        candidate.market_id
        for candidate in candidates
        if candidate.won_by_code is not None and candidate.won_by_code != candidate.won
    ]
    return {
        "markets": len(markets),
        "candidates": len(candidates),
        "missing_or_invalid_token_pairs": len(missing_tokens),
        "missing_or_invalid_token_market_ids": missing_tokens,
        "duplicate_token_pairs": len(duplicate_tokens),
        "duplicate_token_market_ids": duplicate_tokens,
        "bought_winner_code_token_mismatches": len(mismatches),
        "mismatch_market_ids": mismatches,
        "passed": not missing_tokens and not duplicate_tokens and not mismatches,
        "pnl_label": "bought_token_id_equals_winning_token_id",
    }


def _alpha_only_decomposition(signals: list[Signal], *, config: BacktestConfig) -> dict[str, Any]:
    return _cohort_stats(signals, [], config=config)


def _cohort_decomposition(
    signals: list[Signal],
    rows: list[TradeResult],
    *,
    config: BacktestConfig,
) -> dict[str, Any]:
    row_by_market = {row.market_id: row for row in rows}
    filled = [
        signal
        for signal in signals
        if Decimal(row_by_market[signal.candidate.market_id].filled_size) > 0
    ]
    no_fill = [
        signal
        for signal in signals
        if Decimal(row_by_market[signal.candidate.market_id].filled_size) <= 0
    ]
    return {
        "all_signals": _cohort_stats(signals, rows, config=config),
        "filled": _cohort_stats(
            filled,
            [row_by_market[signal.candidate.market_id] for signal in filled],
            config=config,
        ),
        "no_fill": _cohort_stats(
            no_fill,
            [row_by_market[signal.candidate.market_id] for signal in no_fill],
            config=config,
        ),
    }


def _cohort_stats(
    signals: list[Signal],
    rows: list[TradeResult],
    *,
    config: BacktestConfig,
) -> dict[str, Any]:
    count = len(signals)
    wins = sum(signal.candidate.won for signal in signals)
    theoretical_equal_share = sum(
        (Decimal("1") if signal.candidate.won else Decimal("0")) - signal.candidate.signal_price
        for signal in signals
    )
    fixed_notional_pnl = Decimal("0")
    for signal in signals:
        execution_price = min(Decimal("0.999"), signal.candidate.signal_price + config.execution_buffer)
        shares = config.stake / execution_price
        fixed_notional_pnl += (shares if signal.candidate.won else Decimal("0")) - config.stake
    total_size = sum((Decimal(row.filled_size) for row in rows), Decimal("0"))
    total_cost = sum((Decimal(row.buy_cost) for row in rows), Decimal("0"))
    total_settlement = sum((Decimal(row.settlement_value) for row in rows), Decimal("0"))
    expected_settlement = sum(
        Decimal(row.filled_size)
        * next(
            signal.posterior_win_rate
            for signal in signals
            if signal.candidate.market_id == row.market_id
        )
        for row in rows
        if Decimal(row.filled_size) > 0
    )
    weighted_posterior = expected_settlement / total_size if total_size else Decimal("0")
    weighted_outcome = total_settlement / total_size if total_size else Decimal("0")
    return {
        "signal_count": count,
        "win_count": wins,
        "loss_count": count - wins,
        "equal_weight_win_rate": Decimal(wins) / Decimal(count) if count else Decimal("0"),
        "mean_signal_price": _mean_decimal([signal.candidate.signal_price for signal in signals]),
        "mean_posterior": _mean_decimal([signal.posterior_win_rate for signal in signals]),
        "mean_posterior_lower_bound": _mean_decimal([signal.posterior_lower_bound for signal in signals]),
        "mean_limit_price": _mean_decimal(
            [min(Decimal("0.999"), signal.candidate.signal_price + config.execution_buffer) for signal in signals]
        ),
        "mean_fill_delay_seconds": _mean_decimal(
            [Decimal(row.avg_fill_delay_seconds) for row in rows if Decimal(row.filled_size) > 0]
        ),
        "theoretical_equal_share_pnl": theoretical_equal_share,
        "theoretical_fixed_10_usdc_pnl": fixed_notional_pnl,
        "actual_filled_shares": total_size,
        "actual_cost": total_cost,
        "actual_settlement": total_settlement,
        "actual_pnl": total_settlement - total_cost - sum((Decimal(row.fee) for row in rows), Decimal("0")),
        "posterior_weighted_by_filled_shares": weighted_posterior,
        "outcome_weighted_by_filled_shares": weighted_outcome,
        "expected_settlement_from_posterior": expected_settlement,
        "actual_minus_expected_settlement": total_settlement - expected_settlement,
    }


def _execution_decomposition(
    signals: list[Signal],
    rows: list[TradeResult],
    *,
    config: BacktestConfig,
) -> dict[str, Any]:
    row_by_market = {row.market_id: row for row in rows}
    e0 = sum(
        (Decimal("1") if signal.candidate.won else Decimal("0")) - signal.candidate.signal_price
        for signal in signals
    )
    e1 = Decimal("0")
    e2 = Decimal("0")
    for signal in signals:
        limit = min(Decimal("0.999"), signal.candidate.signal_price + config.execution_buffer)
        e1 += ((config.stake / limit) if signal.candidate.won else Decimal("0")) - config.stake
        row = row_by_market[signal.candidate.market_id]
        if Decimal(row.filled_size) > 0:
            e2 += (Decimal("1") if signal.candidate.won else Decimal("0")) - Decimal(row.avg_price)
    e3 = sum((Decimal(row.pnl) for row in rows), Decimal("0"))
    return {
        "E0_all_signals_fixed_one_share_pnl": e0,
        "E1_all_signals_fixed_10_usdc_at_limit_pnl": e1,
        "E2_filled_selection_fixed_one_share_at_avg_fill_pnl": e2,
        "E3_capacity_weighted_actual_pnl": e3,
        "execution_selection_effect_E2_minus_E0": e2 - e0,
    }


def _mean_decimal(values: Iterable[Decimal]) -> Decimal:
    rows = list(values)
    return sum(rows, Decimal("0")) / Decimal(len(rows)) if rows else Decimal("0")


def _favorite_at_snapshot(trades: list[V2TradePrint]) -> tuple[V2TradePrint, int] | None:
    if not trades:
        return None
    latest = max(trades, key=lambda row: row.sequence)
    latest_outcome = latest.outcome.upper()
    if latest_outcome == "YES":
        return (latest, 1) if latest.price >= Decimal("0.5") else (_latest_outcome_trade(trades, "NO") or latest, 2)
    if latest_outcome == "NO":
        return (latest, 2) if latest.price >= Decimal("0.5") else (_latest_outcome_trade(trades, "YES") or latest, 1)
    return None


def _synchronized_favorite_state(
    market: ResolvedMarketCandidate,
    trades: list[V2TradePrint],
    signal_time: datetime,
) -> tuple[V2TradePrint, int, Decimal, str, dict[str, Decimal | None]] | None:
    economic = _economic_trade_rows(trades)
    if not economic:
        return None
    latest = max(economic, key=lambda row: row.sequence)
    outcome = latest.outcome.upper()
    if outcome not in {"YES", "NO"}:
        return None
    yes_price = latest.price if outcome == "YES" else Decimal("1") - latest.price
    no_price = latest.price if outcome == "NO" else Decimal("1") - latest.price
    favorite_outcome = "YES" if yes_price >= no_price else "NO"
    outcome_code = 1 if favorite_outcome == "YES" else 2
    price = yes_price if favorite_outcome == "YES" else no_price
    asset_id = _market_asset_id(
        market.token_yes_id if favorite_outcome == "YES" else market.token_no_id,
        economic,
        favorite_outcome,
    )
    if not asset_id:
        return None
    yes_last = _latest_outcome_trade(economic, "YES")
    no_last = _latest_outcome_trade(economic, "NO")
    pair_skew = (
        Decimal(str(abs((_utc(yes_last.block_time) - _utc(no_last.block_time)).total_seconds())))
        if yes_last is not None and no_last is not None
        else None
    )
    pair_sum_error = (
        abs(yes_last.price + no_last.price - Decimal("1"))
        if yes_last is not None and no_last is not None
        else None
    )
    age = max(0.0, (_utc(signal_time) - _utc(latest.block_time)).total_seconds())
    return (
        latest,
        outcome_code,
        price.quantize(Q, rounding=ROUND_HALF_UP),
        asset_id,
        {
            "signal_price_age_seconds": Decimal(str(age)).quantize(Q, rounding=ROUND_HALF_UP),
            "pair_timestamp_skew_seconds": pair_skew,
            "pair_sum_error": pair_sum_error,
        },
    )


def _latest_outcome_trade(trades: Iterable[V2TradePrint], outcome: str) -> V2TradePrint | None:
    matching = [trade for trade in trades if trade.outcome.upper() == outcome.upper()]
    return max(matching, key=lambda row: row.sequence) if matching else None


def _market_asset_id(
    metadata_token_id: str | None,
    trades: Iterable[V2TradePrint],
    outcome: str,
) -> str:
    canonical = derive_clickhouse_token_id_hex(metadata_token_id)
    if canonical:
        return canonical
    latest = _latest_outcome_trade(trades, outcome)
    return latest.asset_id.lower() if latest is not None else ""


def _economic_trade_rows(trades: Iterable[V2TradePrint]) -> list[V2TradePrint]:
    rows: dict[tuple[int, int, str, str, Decimal], V2TradePrint] = {}
    for trade in trades:
        key = (trade.market_id, trade.block_number, trade.tx_hash, trade.asset_id, trade.price)
        current = rows.get(key)
        if current is None or (trade.size, trade.trade_id) > (current.size, current.trade_id):
            rows[key] = trade
    return sorted(rows.values(), key=lambda row: row.sequence)


def _calibration_history(
    current: Candidate,
    history: list[Candidate],
    config: BacktestConfig,
) -> tuple[list[Candidate], str]:
    tolerance = config.calibration_price_tolerance
    levels = (
        (
            "league_time_continuous_price_liquidity_side",
            lambda row: (
                abs(row.signal_price - current.signal_price) <= tolerance
                and row.liquidity_bucket == current.liquidity_bucket
                and row.favorite_side == current.favorite_side
            ),
        ),
        (
            "league_time_continuous_price_side",
            lambda row: (
                abs(row.signal_price - current.signal_price) <= tolerance
                and row.favorite_side == current.favorite_side
            ),
        ),
        (
            "league_time_continuous_price",
            lambda row: abs(row.signal_price - current.signal_price) <= tolerance * Decimal("2"),
        ),
    )
    for name, predicate in levels:
        matched = [row for row in history if predicate(row)]
        if len(matched) >= config.min_samples:
            return matched, name
    return [], "insufficient_history"


def _price_bucket(price: Decimal) -> str:
    bucket = math.floor(float(price / Decimal("0.05"))) * 5
    low = Decimal(bucket) / Decimal("100")
    return f"{low:.2f}-{low + Decimal('0.05'):.2f}"


def _liquidity_bucket(trades: int) -> str:
    if trades < 50:
        return "20-49"
    if trades < 200:
        return "50-199"
    return "200+"


def _decimal_text(value: Decimal | Any) -> str:
    return str(Decimal(str(value or "0")).quantize(Q, rounding=ROUND_HALF_UP))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sql_time(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    return value


def _report_payload(report: BacktestReport) -> dict[str, Any]:
    return _json_ready(asdict(report))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--min-samples", type=int, default=20)
    parser.add_argument("--min-samples-grid", default="")
    parser.add_argument("--min-trades", type=int, default=20)
    parser.add_argument("--edge-threshold", default="0.015")
    parser.add_argument("--confidence-z", default="1.2815515655")
    parser.add_argument("--confidence-z-grid", default="")
    parser.add_argument("--max-execution-window-seconds", type=int, default=120)
    parser.add_argument("--profiles", default=",".join(DEFAULT_PROFILES))
    parser.add_argument("--portfolio-constraints", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("runtime_outputs/conditional_favorite_longshot/nba_v2.json"))
    args = parser.parse_args(argv)
    config = BacktestConfig(
        market_limit=max(1, args.limit),
        min_samples=max(1, args.min_samples),
        min_trades=max(1, args.min_trades),
        edge_threshold=Decimal(str(args.edge_threshold)),
        confidence_z=Decimal(str(args.confidence_z)),
        max_execution_window_seconds=max(1, args.max_execution_window_seconds),
        apply_portfolio_constraints=bool(args.portfolio_constraints),
        profiles=tuple(profile.strip() for profile in args.profiles.split(",") if profile.strip()),
    )
    grid = tuple(
        sorted({max(1, int(value.strip())) for value in args.min_samples_grid.split(",") if value.strip()})
    )
    z_grid = tuple(
        sorted({Decimal(value.strip()) for value in args.confidence_z_grid.split(",") if value.strip()})
    )
    if z_grid:
        markets = select_nba_2024_25_moneyline_markets(limit=config.market_limit)
        tape = load_orderfilled_tape(markets, config=config)
        reports = [
            run_nba_backtest(
                BacktestConfig(**{**asdict(config), "confidence_z": value}),
                markets=markets,
                trades=tape,
            )
            for value in z_grid
        ]
        payload = {
            "schema_version": "conditional_favorite_longshot_sensitivity_v2",
            "parameter": "confidence_z",
            "values": [str(value) for value in z_grid],
            "reports": [_report_payload(report) for report in reports],
        }
        console = {
            "strategy": "conditional_favorite_longshot_nba_v2",
            "markets": len(markets),
            "tape_rows": reports[0].tape_rows if reports else 0,
            "sensitivity": [
                {
                    "confidence_z": report.config["confidence_z"],
                    "raw_candidates": report.raw_candidates,
                    "signals": report.selected_signals,
                    "profiles": [asdict(row) for row in report.profile_summaries],
                }
                for report in reports
            ],
            "output": str(args.output),
        }
    elif grid:
        markets = select_nba_2024_25_moneyline_markets(limit=config.market_limit)
        tape = load_orderfilled_tape(markets, config=config)
        reports = [
            run_nba_backtest(
                BacktestConfig(**{**asdict(config), "min_samples": value}),
                markets=markets,
                trades=tape,
            )
            for value in grid
        ]
        payload = {
            "schema_version": "conditional_favorite_longshot_sensitivity_v2",
            "parameter": "min_samples",
            "values": list(grid),
            "reports": [_report_payload(report) for report in reports],
        }
        console = {
            "strategy": "conditional_favorite_longshot_nba_v2",
            "markets": len(markets),
            "tape_rows": reports[0].tape_rows if reports else 0,
            "sensitivity": [
                {
                    "min_samples": report.config["min_samples"],
                    "raw_candidates": report.raw_candidates,
                    "signals": report.selected_signals,
                    "profiles": [asdict(row) for row in report.profile_summaries],
                }
                for report in reports
            ],
            "output": str(args.output),
        }
    else:
        report = run_nba_backtest(config)
        payload = _report_payload(report)
        console = {
            "strategy": report.strategy,
            "markets": report.market_count,
            "tape_rows": report.tape_rows,
            "raw_candidates": report.raw_candidates,
            "signals": report.selected_signals,
            "profiles": [asdict(row) for row in report.profile_summaries],
            "output": str(args.output),
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(console, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
