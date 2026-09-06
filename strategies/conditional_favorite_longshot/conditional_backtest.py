"""Conditional favorite-longshot discovery and OrderFilled-only backtest."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable

from quant.backtest.orderfilled_v2_replay import (
    CapacityLedger,
    TradeSliceLoadResult,
    V2OrderResult,
    V2TakerOrder,
    V2TradePrint,
    load_v2_trade_slices_for_orders,
    replay_v2_taker_orders_with_diagnostics,
    with_v2_execution_profile,
)
from quant.core.db import ClickHouseClient, postgres_connection
from quant.core.metadata import derive_clickhouse_token_id_hex
from strategies.conditional_favorite_longshot.calibration import (
    CalibrationEstimate,
    estimate_calibration,
)


Q = Decimal("0.0000000001")
DEFAULT_PROFILES = (
    "probabilistic_conservative",
    "probabilistic_trade_tape",
    "probabilistic_source_confirmed",
)
TTE_BUCKETS = ("0-10m", "10-30m", "30-90m", "90-240m", "240m+")
SPORT_LEAGUES = (
    "nba",
    "nfl",
    "mlb",
    "nhl",
    "wnba",
    "ncaab",
    "ncaaf",
    "soccer",
    "tennis",
    "esports",
    "ufc",
    "golf",
    "cricket",
    "formula1",
)


@dataclass(frozen=True)
class ConditionalConfig:
    market_limit: int = 5000
    query_candidate_limit: int = 50000
    start_date: str = "2024-01-01"
    end_date: str | None = None
    categories: tuple[str, ...] = ("sports", "crypto", "politics", "weather", "pop_culture")
    close_time_sources: tuple[str, ...] = ("gamma_closed_time",)
    max_tte_days: int = 30
    min_bucket_trades: int = 5
    min_samples: int = 80
    min_validation_samples: int = 20
    min_stability_periods: int = 3
    min_period_samples: int = 8
    min_period_sign_ratio: float = 0.67
    min_stability_seasons: int = 2
    min_season_samples: int = 30
    min_season_sign_ratio: float = 0.67
    min_model_sign_agreement: float = 0.75
    bootstrap_samples: int = 100
    confidence_level: float = 0.90
    edge_threshold: Decimal = Decimal("0.01")
    execution_buffer: Decimal = Decimal("0.01")
    min_trade_price: Decimal = Decimal("0.01")
    max_trade_price: Decimal = Decimal("0.99")
    fee_bps: Decimal = Decimal("0")
    capital_cost_annual_rate: Decimal = Decimal("0")
    stake: Decimal = Decimal("10")
    initial_capital: Decimal = Decimal("1000")
    profiles: tuple[str, ...] = DEFAULT_PROFILES
    apply_portfolio_constraints: bool = False
    max_daily_cost: Decimal = Decimal("100")
    max_concurrent_positions: int = 20


@dataclass(frozen=True)
class ConditionalMarket:
    market_id: int
    market_slug: str
    title: str
    category: str
    league: str
    product_type: str
    close_time: datetime
    close_time_source: str
    label_available_at: datetime
    settlement_code: int
    settlement_outcome: str
    yes_asset_id: str
    no_asset_id: str


@dataclass(frozen=True)
class MarketObservation:
    market_id: int
    market_slug: str
    title: str
    category: str
    league: str
    product_type: str
    close_time: datetime
    close_time_source: str
    signal_time: datetime
    label_available_at: datetime
    tte_bucket: str
    true_tte_minutes: float
    liquidity_regime: str
    bucket_trade_count: int
    bucket_volume: Decimal
    probability_yes: float
    yes_won: bool
    signal_trade_id: str
    signal_tx_hash: str
    signal_log_indexes: tuple[int, ...]
    signal_block: int
    signal_asset_id: str
    signal_outcome: str
    yes_asset_id: str
    no_asset_id: str


@dataclass(frozen=True)
class ConditionalSignal:
    observation: MarketObservation
    buy_side: str
    asset_id: str
    market_price: Decimal
    calibrated_side_probability: Decimal
    confidence_lower_bound: Decimal
    net_edge: Decimal
    condition_level: str
    condition_key: tuple[str, ...]
    bias_direction: str
    estimate: CalibrationEstimate


@dataclass(frozen=True)
class ConditionalTrade:
    profile: str
    market_id: int
    market_slug: str
    category: str
    league: str
    product_type: str
    tte_bucket: str
    close_time_source: str
    buy_side: str
    bias_direction: str
    signal_time: str
    market_price: str
    calibrated_probability: str
    confidence_lower_bound: str
    net_edge: str
    selected_model: str
    condition_level: str
    calibration_samples: int
    stable_periods: int
    stable_seasons: int
    status: str
    requested_size: str
    filled_size: str
    avg_price: str
    buy_cost: str
    fees: str
    capital_cost: str
    settlement_value: str
    pnl: str
    reason_unfilled: str
    source_trade_ids: tuple[str, ...]
    source_tx_hashes: tuple[str, ...]


@dataclass(frozen=True)
class ConditionalProfileSummary:
    profile: str
    signals: int
    filled_orders: int
    partial_orders: int
    no_fill_orders: int
    wins: int
    losses: int
    total_cost: str
    total_fees: str
    total_capital_cost: str
    total_settlement: str
    total_pnl: str
    roi_on_cost: str
    ending_capital: str
    source_invariant_errors: int


@dataclass(frozen=True)
class ConditionalBacktestReport:
    strategy: str
    market_count: int
    observation_count: int
    stable_signals: int
    selected_signals: int
    trades: tuple[ConditionalTrade, ...]
    profile_summaries: tuple[ConditionalProfileSummary, ...]
    config: dict[str, Any]
    diagnostics: dict[str, Any] = field(default_factory=dict)


def run_conditional_backtest(
    config: ConditionalConfig = ConditionalConfig(),
    *,
    markets: list[ConditionalMarket] | None = None,
    observations: list[MarketObservation] | None = None,
    execution_trades: Iterable[V2TradePrint] | None = None,
    postgres_conn: Any | None = None,
    clickhouse_client: ClickHouseClient | None = None,
) -> ConditionalBacktestReport:
    market_rows = markets or select_conditional_markets(
        config,
        conn=postgres_conn,
        clickhouse_client=clickhouse_client,
    )
    observation_rows = observations or load_market_observations(
        market_rows,
        config=config,
        client=clickhouse_client,
    )
    signals, signal_diagnostics = build_conditional_signals(observation_rows, config=config)
    selected, portfolio_skipped = (
        apply_portfolio_limits(signals, config=config)
        if config.apply_portfolio_constraints
        else (signals, 0)
    )

    profiled_orders = {
        profile: [signal_to_order(signal, config=config, profile=profile) for signal in selected]
        for profile in config.profiles
    }
    if execution_trades is None:
        loaded = _load_execution_trade_union(profiled_orders, client=clickhouse_client)
        execution_rows = list(loaded.trades)
        slice_diagnostics = loaded.as_dict()
    else:
        execution_rows = list(execution_trades)
        slice_diagnostics = {
            "windows_count": 0,
            "merged_windows_count": 0,
            "db_query_count": 0,
            "rows_loaded": len(execution_rows),
            "load_sec": Decimal("0"),
            "source": "provided",
        }

    all_trades: list[ConditionalTrade] = []
    summaries: list[ConditionalProfileSummary] = []
    replay_diagnostics: dict[str, Any] = {}
    for profile, orders in profiled_orders.items():
        results, _, diagnostics = replay_v2_taker_orders_with_diagnostics(
            orders,
            execution_rows,
            ledger=CapacityLedger(),
        )
        rows = [
            result_to_trade(signal, order, result, config=config, profile=profile)
            for signal, order, result in zip(selected, orders, results)
        ]
        all_trades.extend(rows)
        summaries.append(summarize_profile(profile, rows, config=config))
        replay_diagnostics[profile] = diagnostics.as_dict()

    diagnostics = {
        "universe": _market_distribution(market_rows),
        "observations": _observation_distribution(observation_rows),
        "signal_discovery": signal_diagnostics,
        "portfolio_skipped": portfolio_skipped,
        "execution_trade_slices": slice_diagnostics,
        "execution_replay": replay_diagnostics,
        "alpha_only": _alpha_only_summary(selected, config=config),
        "fill_only_by_price_bucket": {
            profile: {
                bucket: _json_ready(asdict(summarize_profile(profile, bucket_rows, config=config)))
                for bucket, bucket_rows in _trades_by_price_bucket(rows).items()
            }
            for profile, rows in (
                (profile, [row for row in all_trades if row.profile == profile])
                for profile in profiled_orders
            )
        },
        "methodology": {
            "close_time_policy": list(config.close_time_sources),
            "price_sampling": "last one-sided economic trade in each true-TTE bucket",
            "training": "monthly expanding walk-forward; labels must be available before test month",
            "calibration_models": ["power", "platt", "isotonic", "local_linear"],
            "confidence": "market-cluster bootstrap",
            "execution": "OrderFilled-only V2; no LOB",
        },
    }
    return ConditionalBacktestReport(
        strategy="conditional_favorite_longshot_v3",
        market_count=len(market_rows),
        observation_count=len(observation_rows),
        stable_signals=len(signals),
        selected_signals=len(selected),
        trades=tuple(all_trades),
        profile_summaries=tuple(summaries),
        config=_json_ready(asdict(config)),
        diagnostics=_json_ready(diagnostics),
    )


def select_conditional_markets(
    config: ConditionalConfig,
    *,
    conn: Any | None = None,
    clickhouse_client: ClickHouseClient | None = None,
) -> list[ConditionalMarket]:
    covered_market_ids = _covered_market_ids(
        max(config.query_candidate_limit, config.market_limit),
        client=clickhouse_client,
    )
    if not covered_market_ids:
        return []
    owns_connection = conn is None
    manager = postgres_connection(readonly=True) if owns_connection else None
    connection = manager.__enter__() if manager is not None else conn
    try:
        with connection.cursor() as cur:
            close_sql = _close_sql_expression(config.close_time_sources)
            end_filter = f"AND {close_sql} < %s::timestamptz" if config.end_date else ""
            params: list[Any] = [covered_market_ids, config.start_date]
            if config.end_date:
                params.append(config.end_date)
            params.append(max(config.market_limit, config.query_candidate_limit))
            cur.execute(
                f"""
                SELECT
                    m.id AS market_id,
                    m.slug AS market_slug,
                    COALESCE(m.title, m.slug) AS title,
                    COALESCE(m.category, '') AS category,
                    m.tags,
                    m.end_date,
                    s.gamma_closed_time,
                    s.completion_time,
                    s.settlement_event_time,
                    s.updated_at AS status_updated_at,
                    s.settlement_code,
                    s.settlement_outcome,
                    m.yes_token_id,
                    m.no_token_id
                FROM core.markets m
                JOIN core.market_status_snapshot s ON s.market_id = m.id
                WHERE s.is_resolved IS TRUE
                  AND s.settlement_code IN (1, 2)
                  AND m.id = ANY(%s)
                  AND m.yes_token_id IS NOT NULL
                  AND m.no_token_id IS NOT NULL
                  AND {close_sql} >= %s::timestamptz
                  {end_filter}
                ORDER BY {close_sql}, m.id
                LIMIT %s
                """,
                params,
            )
            raw_rows = list(cur.fetchall())
    finally:
        if manager is not None:
            manager.__exit__(None, None, None)

    grouped: dict[tuple[str, str, str, str], list[ConditionalMarket]] = defaultdict(list)
    allowed_categories = {value.strip().lower() for value in config.categories}
    for row in raw_rows:
        category = _domain(row.get("category"), row.get("tags"), row.get("market_slug"), row.get("title"))
        if allowed_categories and category not in allowed_categories:
            continue
        close_time, close_source = _close_time(row, config.close_time_sources)
        if close_time is None:
            continue
        yes_asset = derive_clickhouse_token_id_hex(row.get("yes_token_id"))
        no_asset = derive_clickhouse_token_id_hex(row.get("no_token_id"))
        if not yes_asset or not no_asset or yes_asset == no_asset:
            continue
        label_available = _utc(
            row.get("settlement_event_time")
            or row.get("completion_time")
            or row.get("status_updated_at")
            or close_time
        )
        league = _league(row.get("tags"), row.get("market_slug"), row.get("title"), category)
        product_type = _product_type(row.get("market_slug"), row.get("title"), category)
        market = ConditionalMarket(
            market_id=int(row["market_id"]),
            market_slug=str(row.get("market_slug") or ""),
            title=str(row.get("title") or ""),
            category=category,
            league=league,
            product_type=product_type,
            close_time=_utc(close_time),
            close_time_source=close_source,
            label_available_at=max(label_available, _utc(close_time)),
            settlement_code=int(row["settlement_code"]),
            settlement_outcome=str(row.get("settlement_outcome") or ""),
            yes_asset_id=yes_asset,
            no_asset_id=no_asset,
        )
        grouped[(category, league, product_type, market.close_time.strftime("%Y-%m"))].append(market)

    selected: list[ConditionalMarket] = []
    ordered_groups = [grouped[key] for key in sorted(grouped)]
    index = 0
    while len(selected) < config.market_limit:
        progressed = False
        for rows in ordered_groups:
            if index < len(rows):
                selected.append(rows[index])
                progressed = True
                if len(selected) >= config.market_limit:
                    break
        if not progressed:
            break
        index += 1
    return sorted(selected, key=lambda row: (row.close_time, row.market_id))


def _covered_market_ids(limit: int, *, client: ClickHouseClient | None = None) -> list[int]:
    ch = client or ClickHouseClient()
    rows = ch.query_json_rows(
        f"""
        SELECT market_id
        FROM orderfilled_trade_replay_coverage
        GROUP BY market_id
        HAVING sum(row_count) > 0
        ORDER BY cityHash64(market_id)
        LIMIT {max(1, int(limit))}
        """,
        timeout_seconds=300,
    )
    return [int(row["market_id"]) for row in rows]


def load_market_observations(
    markets: list[ConditionalMarket],
    *,
    config: ConditionalConfig,
    client: ClickHouseClient | None = None,
    batch_size: int = 100,
) -> list[MarketObservation]:
    ch = client or ClickHouseClient()
    market_by_id = {market.market_id: market for market in markets}
    observations: list[MarketObservation] = []
    for offset in range(0, len(markets), max(1, batch_size)):
        batch = markets[offset : offset + max(1, batch_size)]
        if not batch:
            continue
        ids = ",".join(str(market.market_id) for market in batch)
        close_expr = "multiIf(" + ",".join(
            f"market_id={market.market_id},toDateTime64('{_sql_time(market.close_time)}',0,'UTC')"
            for market in batch
        ) + ",toDateTime64(0,0,'UTC'))"
        rows = ch.query_json_rows(
            f"""
            SELECT
                market_id,
                tte_bucket,
                argMax(trade_id, sequence_key) AS trade_id,
                argMax(asset_id, sequence_key) AS asset_id,
                argMax(outcome, sequence_key) AS outcome,
                argMax(price, sequence_key) AS price,
                argMax(block_number, sequence_key) AS block_number,
                argMax(block_time, sequence_key) AS block_time,
                argMax(tx_hash, sequence_key) AS tx_hash,
                argMax(source_log_indexes, sequence_key) AS source_log_indexes,
                count() AS bucket_trade_count,
                sum(size_shares) AS bucket_volume
            FROM
            (
                SELECT
                    market_id,
                    trade_id,
                    asset_id,
                    outcome,
                    price,
                    size_shares,
                    block_number,
                    block_time,
                    tx_hash,
                    source_log_indexes,
                    tuple(block_time, block_number, tx_index, tx_hash, arrayMin(source_log_indexes), trade_id) AS sequence_key,
                    multiIf(
                        age_seconds >= 0 AND age_seconds < 600, '0-10m',
                        age_seconds >= 600 AND age_seconds < 1800, '10-30m',
                        age_seconds >= 1800 AND age_seconds < 5400, '30-90m',
                        age_seconds >= 5400 AND age_seconds < 14400, '90-240m',
                        age_seconds >= 14400 AND age_seconds <= {int(config.max_tte_days) * 86400}, '240m+',
                        ''
                    ) AS tte_bucket
                FROM
                (
                    SELECT
                        *,
                        dateDiff('second', block_time, close_time) AS age_seconds
                    FROM
                    (
                        SELECT
                            trade_id,
                            market_id,
                            asset_id,
                            outcome,
                            price,
                            size_shares,
                            block_number,
                            block_time,
                            tx_hash,
                            tx_index,
                            source_log_indexes,
                            {close_expr} AS close_time
                        FROM trade_prints_one_sided
                        PREWHERE market_id IN ({ids})
                        WHERE confidence = 'high'
                          AND price >= 0.01
                          AND price <= 0.99
                    )
                    WHERE block_time <= close_time
                      AND block_time >= close_time - INTERVAL {int(config.max_tte_days)} DAY
                )
            )
            WHERE tte_bucket != ''
            GROUP BY market_id, tte_bucket
            ORDER BY market_id, tte_bucket
            """,
            timeout_seconds=300,
        )
        for row in rows:
            market = market_by_id.get(int(row["market_id"]))
            if market is None or int(row.get("bucket_trade_count") or 0) < config.min_bucket_trades:
                continue
            outcome = str(row.get("outcome") or "").upper()
            price = Decimal(str(row["price"]))
            if outcome == "YES":
                probability_yes = price
            elif outcome == "NO":
                probability_yes = Decimal("1") - price
            else:
                continue
            signal_time = _utc(_datetime(row["block_time"]))
            tte_minutes = max(0.0, (market.close_time - signal_time).total_seconds() / 60.0)
            trade_count = int(row["bucket_trade_count"])
            observations.append(
                MarketObservation(
                    market_id=market.market_id,
                    market_slug=market.market_slug,
                    title=market.title,
                    category=market.category,
                    league=market.league,
                    product_type=market.product_type,
                    close_time=market.close_time,
                    close_time_source=market.close_time_source,
                    signal_time=signal_time,
                    label_available_at=market.label_available_at,
                    tte_bucket=str(row["tte_bucket"]),
                    true_tte_minutes=tte_minutes,
                    liquidity_regime=_liquidity_regime(trade_count, Decimal(str(row["bucket_volume"]))),
                    bucket_trade_count=trade_count,
                    bucket_volume=Decimal(str(row["bucket_volume"])),
                    probability_yes=float(probability_yes),
                    yes_won=market.settlement_code == 1,
                    signal_trade_id=str(row["trade_id"]),
                    signal_tx_hash=str(row["tx_hash"]),
                    signal_log_indexes=tuple(int(value) for value in row.get("source_log_indexes") or ()),
                    signal_block=int(row["block_number"]),
                    signal_asset_id=str(row["asset_id"]).lower(),
                    signal_outcome=outcome,
                    yes_asset_id=market.yes_asset_id,
                    no_asset_id=market.no_asset_id,
                )
            )
    return sorted(observations, key=lambda row: (row.signal_time, row.market_id, row.tte_bucket))


def build_conditional_signals(
    observations: Iterable[MarketObservation],
    *,
    config: ConditionalConfig,
) -> tuple[list[ConditionalSignal], dict[str, Any]]:
    rows = sorted(observations, key=lambda row: (row.signal_time, row.market_id, row.tte_bucket))
    signals: list[ConditionalSignal] = []
    rejections: Counter[str] = Counter()
    selected_models: Counter[str] = Counter()
    condition_levels: Counter[str] = Counter()
    estimate_cache: dict[tuple[Any, ...], CalibrationEstimate] = {}
    near_misses: list[dict[str, Any]] = []

    for current in rows:
        p_yes = Decimal(str(current.probability_yes))
        if not any(
            config.min_trade_price <= price <= config.max_trade_price
            for price in (p_yes, Decimal("1") - p_yes)
        ):
            rejections["price_outside_trade_range"] += 1
            continue
        month_start = current.signal_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        available_history = [row for row in rows if row.label_available_at < month_start]
        signal: ConditionalSignal | None = None
        best_reasons: tuple[str, ...] = ("insufficient_history",)
        for level, key in _condition_keys(current):
            matched = [row for row in available_history if _condition_key(row, level) == key]
            evaluation_price = min(0.99, max(0.01, round(current.probability_yes, 4)))
            cache_key = (month_start.isoformat(), level, key, evaluation_price)
            estimate = estimate_cache.get(cache_key)
            if estimate is None:
                estimate = estimate_calibration(
                    matched,
                    probability=evaluation_price,
                    min_samples=config.min_samples,
                    min_validation_samples=config.min_validation_samples,
                    bootstrap_samples=config.bootstrap_samples,
                    confidence_level=config.confidence_level,
                    min_stability_periods=config.min_stability_periods,
                    min_period_samples=config.min_period_samples,
                    min_period_sign_ratio=config.min_period_sign_ratio,
                    min_stability_seasons=config.min_stability_seasons,
                    min_season_samples=config.min_season_samples,
                    min_season_sign_ratio=config.min_season_sign_ratio,
                    min_model_sign_agreement=config.min_model_sign_agreement,
                    seed_key="|".join((month_start.isoformat(), level, *key, f"{evaluation_price:.4f}")),
                )
                estimate_cache[cache_key] = estimate
            if not estimate.stable:
                best_reasons = estimate.rejection_reasons
                continue
            signal = _signal_from_estimate(current, estimate, level, key, config)
            if signal is not None:
                break
            near_misses.append(_near_miss(current, estimate, level, config))
            best_reasons = ("edge_below_cost_threshold",)
        if signal is None:
            rejections.update(best_reasons)
            continue
        signals.append(signal)
        selected_models[signal.estimate.selected_model] += 1
        condition_levels[signal.condition_level] += 1

    diagnostics = {
        "evaluated_observations": len(rows),
        "stable_signals": len(signals),
        "rejection_reasons": dict(sorted(rejections.items())),
        "selected_models": dict(sorted(selected_models.items())),
        "condition_levels": dict(sorted(condition_levels.items())),
        "buy_sides": dict(sorted(Counter(signal.buy_side for signal in signals).items())),
        "bias_directions": dict(sorted(Counter(signal.bias_direction for signal in signals).items())),
        "top_near_misses": sorted(
            near_misses,
            key=lambda row: (Decimal(str(row["best_net_edge"])), row["signal_time"]),
            reverse=True,
        )[:20],
    }
    return signals, diagnostics


def _near_miss(
    observation: MarketObservation,
    estimate: CalibrationEstimate,
    level: str,
    config: ConditionalConfig,
) -> dict[str, Any]:
    p_yes = Decimal(str(observation.probability_yes))
    p_no = Decimal("1") - p_yes
    yes_edge = (
        Decimal(str(estimate.lower_bound))
        - p_yes
        - config.execution_buffer
        - p_yes * config.fee_bps / Decimal("10000")
    )
    no_edge = (
        Decimal("1")
        - Decimal(str(estimate.upper_bound))
        - p_no
        - config.execution_buffer
        - p_no * config.fee_bps / Decimal("10000")
    )
    return {
        "market_id": observation.market_id,
        "market_slug": observation.market_slug,
        "signal_time": observation.signal_time.isoformat(),
        "category": observation.category,
        "league": observation.league,
        "product_type": observation.product_type,
        "tte_bucket": observation.tte_bucket,
        "market_probability_yes": observation.probability_yes,
        "calibrated_probability_yes": estimate.probability,
        "lower_bound_yes": estimate.lower_bound,
        "upper_bound_yes": estimate.upper_bound,
        "best_side": "YES" if yes_edge >= no_edge else "NO",
        "best_net_edge": max(yes_edge, no_edge),
        "required_edge": config.edge_threshold,
        "selected_model": estimate.selected_model,
        "condition_level": level,
        "samples": estimate.sample_count,
        "stable_periods": estimate.stable_periods,
        "stable_seasons": estimate.stable_seasons,
    }


def _signal_from_estimate(
    observation: MarketObservation,
    estimate: CalibrationEstimate,
    level: str,
    key: tuple[str, ...],
    config: ConditionalConfig,
) -> ConditionalSignal | None:
    p_yes = Decimal(str(observation.probability_yes))
    p_no = Decimal("1") - p_yes
    q_yes = Decimal(str(estimate.probability))
    yes_lower = Decimal(str(estimate.lower_bound))
    no_lower = Decimal("1") - Decimal(str(estimate.upper_bound))
    yes_fee = p_yes * config.fee_bps / Decimal("10000")
    no_fee = p_no * config.fee_bps / Decimal("10000")
    yes_edge = yes_lower - p_yes - config.execution_buffer - yes_fee
    no_edge = no_lower - p_no - config.execution_buffer - no_fee
    threshold = config.edge_threshold
    candidates = [
        row
        for row in (
            ("YES", p_yes, q_yes, yes_lower, yes_edge, observation.yes_asset_id),
            ("NO", p_no, Decimal("1") - q_yes, no_lower, no_edge, observation.no_asset_id),
        )
        if config.min_trade_price <= row[1] <= config.max_trade_price and row[4] > threshold
    ]
    if not candidates:
        return None
    buy_side, market_price, calibrated_side, lower, net_edge, asset_id = max(
        candidates,
        key=lambda row: row[4],
    )
    return ConditionalSignal(
        observation=observation,
        buy_side=buy_side,
        asset_id=asset_id,
        market_price=market_price,
        calibrated_side_probability=calibrated_side,
        confidence_lower_bound=lower,
        net_edge=net_edge,
        condition_level=level,
        condition_key=key,
        bias_direction=_bias_direction(p_yes, q_yes),
        estimate=estimate,
    )


def signal_to_order(
    signal: ConditionalSignal,
    *,
    config: ConditionalConfig,
    profile: str,
) -> V2TakerOrder:
    limit_price = min(Decimal("0.999"), signal.market_price + config.execution_buffer)
    base = V2TakerOrder(
        order_id=f"CFL3-{profile}-{signal.observation.market_id}-{signal.observation.tte_bucket}-{signal.buy_side}",
        market_id=signal.observation.market_id,
        asset_id=signal.asset_id,
        side="BUY",
        limit_price=limit_price,
        size=(config.stake / limit_price).quantize(Q, rounding=ROUND_HALF_UP),
        signal_block=signal.observation.signal_block,
        signal_ts=signal.observation.signal_time,
        tif="GTD",
        allow_partial_fill=True,
        signal_source_trade_id=signal.observation.signal_trade_id,
        signal_source_tx_hash=signal.observation.signal_tx_hash,
        signal_source_log_indexes=signal.observation.signal_log_indexes,
        exclude_signal_source_trade=True,
    )
    profiled = with_v2_execution_profile(base, profile)
    if profiled.deadline_block is None and profiled.signal_block is not None:
        # Loading may over-read blocks; the replay still enforces the exact time deadline.
        profiled = replace(profiled, horizon_blocks=2000)
    return profiled


def result_to_trade(
    signal: ConditionalSignal,
    order: V2TakerOrder,
    result: V2OrderResult,
    *,
    config: ConditionalConfig,
    profile: str,
) -> ConditionalTrade:
    won = signal.observation.yes_won if signal.buy_side == "YES" else not signal.observation.yes_won
    fees = result.filled_notional * config.fee_bps / Decimal("10000")
    holding_seconds = max(
        0.0,
        (signal.observation.label_available_at - (result.arrival_ts or signal.observation.signal_time)).total_seconds(),
    )
    capital_cost = (
        result.filled_notional
        * config.capital_cost_annual_rate
        * Decimal(str(holding_seconds))
        / Decimal(str(365 * 86400))
    )
    settlement = result.filled_size if won else Decimal("0")
    pnl = settlement - result.filled_notional - fees - capital_cost
    return ConditionalTrade(
        profile=profile,
        market_id=signal.observation.market_id,
        market_slug=signal.observation.market_slug,
        category=signal.observation.category,
        league=signal.observation.league,
        product_type=signal.observation.product_type,
        tte_bucket=signal.observation.tte_bucket,
        close_time_source=signal.observation.close_time_source,
        buy_side=signal.buy_side,
        bias_direction=signal.bias_direction,
        signal_time=signal.observation.signal_time.isoformat(),
        market_price=_decimal_text(signal.market_price),
        calibrated_probability=_decimal_text(signal.calibrated_side_probability),
        confidence_lower_bound=_decimal_text(signal.confidence_lower_bound),
        net_edge=_decimal_text(signal.net_edge),
        selected_model=signal.estimate.selected_model,
        condition_level=signal.condition_level,
        calibration_samples=signal.estimate.sample_count,
        stable_periods=signal.estimate.stable_periods,
        stable_seasons=signal.estimate.stable_seasons,
        status=result.status,
        requested_size=_decimal_text(order.size),
        filled_size=_decimal_text(result.filled_size),
        avg_price=_decimal_text(result.avg_price),
        buy_cost=_decimal_text(result.filled_notional),
        fees=_decimal_text(fees),
        capital_cost=_decimal_text(capital_cost),
        settlement_value=_decimal_text(settlement),
        pnl=_decimal_text(pnl),
        reason_unfilled=result.reason_unfilled,
        source_trade_ids=tuple(fill.source_trade_id for fill in result.fills),
        source_tx_hashes=tuple(fill.source_tx_hash for fill in result.fills),
    )


def summarize_profile(
    profile: str,
    trades: Iterable[ConditionalTrade],
    *,
    config: ConditionalConfig,
) -> ConditionalProfileSummary:
    rows = list(trades)
    total_cost = sum((Decimal(row.buy_cost) for row in rows), Decimal("0"))
    total_fees = sum((Decimal(row.fees) for row in rows), Decimal("0"))
    total_capital = sum((Decimal(row.capital_cost) for row in rows), Decimal("0"))
    total_settlement = sum((Decimal(row.settlement_value) for row in rows), Decimal("0"))
    total_pnl = sum((Decimal(row.pnl) for row in rows), Decimal("0"))
    return ConditionalProfileSummary(
        profile=profile,
        signals=len(rows),
        filled_orders=sum(row.status == "FILLED" for row in rows),
        partial_orders=sum(row.status == "PARTIAL_FILLED" for row in rows),
        no_fill_orders=sum(row.status == "NO_FILL" for row in rows),
        wins=sum(Decimal(row.pnl) > 0 for row in rows),
        losses=sum(Decimal(row.pnl) < 0 for row in rows),
        total_cost=_decimal_text(total_cost),
        total_fees=_decimal_text(total_fees),
        total_capital_cost=_decimal_text(total_capital),
        total_settlement=_decimal_text(total_settlement),
        total_pnl=_decimal_text(total_pnl),
        roi_on_cost=_decimal_text(total_pnl / total_cost if total_cost else Decimal("0")),
        ending_capital=_decimal_text(config.initial_capital + total_pnl),
        source_invariant_errors=sum(
            Decimal(row.filled_size) > 0 and (not row.source_trade_ids or not row.source_tx_hashes)
            for row in rows
        ),
    )


def apply_portfolio_limits(
    signals: Iterable[ConditionalSignal],
    *,
    config: ConditionalConfig,
) -> tuple[list[ConditionalSignal], int]:
    selected: list[ConditionalSignal] = []
    daily_cost: dict[str, Decimal] = {}
    active_until: list[datetime] = []
    skipped = 0
    for signal in sorted(signals, key=lambda row: (row.observation.signal_time, -row.net_edge)):
        now = signal.observation.signal_time
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
        active_until.append(signal.observation.label_available_at)
    return selected, skipped


def _load_execution_trade_union(
    orders_by_profile: dict[str, list[V2TakerOrder]],
    *,
    client: ClickHouseClient | None,
) -> TradeSliceLoadResult:
    all_orders = [order for orders in orders_by_profile.values() for order in orders]
    if not all_orders:
        return TradeSliceLoadResult((), 0, 0, 0, 0, Decimal("0"))
    return load_v2_trade_slices_for_orders(all_orders, client=client, merge_gap_blocks=50)


def _condition_keys(row: MarketObservation) -> tuple[tuple[str, tuple[str, ...]], ...]:
    return (
        ("category_league_product_tte_liquidity", _condition_key(row, "category_league_product_tte_liquidity")),
        ("category_league_product_tte", _condition_key(row, "category_league_product_tte")),
        ("category_product_tte", _condition_key(row, "category_product_tte")),
        ("category_tte", _condition_key(row, "category_tte")),
    )


def _condition_key(row: MarketObservation, level: str) -> tuple[str, ...]:
    base = (row.category,)
    if level == "category_league_product_tte_liquidity":
        return (*base, row.league, row.product_type, row.tte_bucket, row.liquidity_regime, row.close_time_source)
    if level == "category_league_product_tte":
        return (*base, row.league, row.product_type, row.tte_bucket, row.close_time_source)
    if level == "category_product_tte":
        return (*base, row.product_type, row.tte_bucket, row.close_time_source)
    if level == "category_tte":
        return (*base, row.tte_bucket, row.close_time_source)
    raise ValueError(f"unknown condition level: {level}")


def _alpha_only_summary(signals: list[ConditionalSignal], *, config: ConditionalConfig) -> dict[str, Any]:
    summary = _alpha_only_stats(signals, config=config)
    summary["by_price_bucket"] = {
        bucket: _alpha_only_stats(rows, config=config)
        for bucket, rows in _signals_by_price_bucket(signals).items()
    }
    return summary


def _alpha_only_stats(signals: list[ConditionalSignal], *, config: ConditionalConfig) -> dict[str, Any]:
    pnl = Decimal("0")
    wins = 0
    for signal in signals:
        won = signal.observation.yes_won if signal.buy_side == "YES" else not signal.observation.yes_won
        wins += int(won)
        limit = min(Decimal("0.999"), signal.market_price + config.execution_buffer)
        shares = config.stake / limit
        pnl += (shares if won else Decimal("0")) - config.stake
    return {
        "signals": len(signals),
        "wins": wins,
        "losses": len(signals) - wins,
        "win_rate": Decimal(wins) / Decimal(len(signals)) if signals else Decimal("0"),
        "fixed_stake_pnl_at_limit_before_fill_selection": pnl,
    }


def _price_bucket(price: Decimal) -> str:
    for lower, upper in (
        (Decimal("0.01"), Decimal("0.10")),
        (Decimal("0.10"), Decimal("0.20")),
        (Decimal("0.20"), Decimal("0.30")),
        (Decimal("0.30"), Decimal("0.40")),
        (Decimal("0.40"), Decimal("0.50")),
        (Decimal("0.50"), Decimal("0.60")),
        (Decimal("0.60"), Decimal("0.70")),
        (Decimal("0.70"), Decimal("0.80")),
        (Decimal("0.80"), Decimal("0.90")),
        (Decimal("0.90"), Decimal("0.99")),
    ):
        if lower <= price < upper or (upper == Decimal("0.99") and price == upper):
            return f"{lower:.2f}-{upper:.2f}"
    return "outside"


def _signals_by_price_bucket(
    signals: Iterable[ConditionalSignal],
) -> dict[str, list[ConditionalSignal]]:
    grouped: dict[str, list[ConditionalSignal]] = {}
    for signal in signals:
        grouped.setdefault(_price_bucket(signal.market_price), []).append(signal)
    return dict(sorted(grouped.items()))


def _trades_by_price_bucket(
    trades: Iterable[ConditionalTrade],
) -> dict[str, list[ConditionalTrade]]:
    grouped: dict[str, list[ConditionalTrade]] = {}
    for trade in trades:
        grouped.setdefault(_price_bucket(Decimal(trade.market_price)), []).append(trade)
    return dict(sorted(grouped.items()))


def _market_distribution(markets: list[ConditionalMarket]) -> dict[str, Any]:
    return {
        "markets": len(markets),
        "categories": dict(sorted(Counter(row.category for row in markets).items())),
        "leagues": dict(sorted(Counter(row.league for row in markets).items())),
        "product_types": dict(sorted(Counter(row.product_type for row in markets).items())),
        "close_time_sources": dict(sorted(Counter(row.close_time_source for row in markets).items())),
    }


def _observation_distribution(rows: list[MarketObservation]) -> dict[str, Any]:
    return {
        "observations": len(rows),
        "markets": len({row.market_id for row in rows}),
        "tte_buckets": dict(sorted(Counter(row.tte_bucket for row in rows).items())),
        "liquidity_regimes": dict(sorted(Counter(row.liquidity_regime for row in rows).items())),
        "yes_wins": sum(row.yes_won for row in rows),
    }


def _close_time(row: dict[str, Any], allowed: tuple[str, ...]) -> tuple[datetime | None, str]:
    for source in allowed:
        if source == "gamma_closed_time" and row.get("gamma_closed_time") is not None:
            return _datetime(row["gamma_closed_time"]), source
        if source == "completion_time" and row.get("completion_time") is not None:
            return _datetime(row["completion_time"]), source
        if source == "scheduled_end" and row.get("end_date") is not None:
            return _datetime(row["end_date"]), source
    return None, ""


def _close_sql_expression(allowed: tuple[str, ...]) -> str:
    columns = {
        "gamma_closed_time": "s.gamma_closed_time",
        "completion_time": "s.completion_time",
        "scheduled_end": "m.end_date",
    }
    selected = [columns[source] for source in allowed if source in columns]
    if not selected:
        raise ValueError("at least one supported close_time_source is required")
    return selected[0] if len(selected) == 1 else f"COALESCE({','.join(selected)})"


def _domain(category: Any, tags: Any, slug: Any, title: Any) -> str:
    text = " ".join((str(category or ""), str(tags or ""), str(slug or ""), str(title or ""))).lower()
    if any(value in text for value in ("sports", "nba", "nfl", "mlb", "nhl", "soccer", "tennis", "esports", "basketball")):
        return "sports"
    if any(value in text for value in ("crypto", "bitcoin", "ethereum", "solana", "xrp", "up-or-down")):
        return "crypto"
    if any(value in text for value in ("politics", "election", "president", "congress", "senate")):
        return "politics"
    if any(value in text for value in ("weather", "temperature", "rainfall", "snowfall")):
        return "weather"
    if any(value in text for value in ("pop-culture", "movies", "music", "awards", "entertainment")):
        return "pop_culture"
    if any(value in text for value in ("finance", "fed", "interest-rate", "stock")):
        return "finance"
    return str(category or "other").strip().lower().replace("-", "_") or "other"


def _league(tags: Any, slug: Any, title: Any, category: str) -> str:
    text = " ".join((str(tags or ""), str(slug or ""), str(title or ""))).lower()
    for league in SPORT_LEAGUES:
        if re.search(rf"(^|[^a-z0-9]){re.escape(league)}([^a-z0-9]|$)", text):
            return league
    if category == "crypto":
        for asset in ("bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp"):
            if re.search(rf"(^|[^a-z0-9]){asset}([^a-z0-9]|$)", text):
                return asset
    return category


def _product_type(slug: Any, title: Any, category: str) -> str:
    text = f"{slug or ''} {title or ''}".lower()
    if category == "sports":
        if re.search(r"(nba|nfl|mlb|nhl|wnba)-[a-z0-9]+-[a-z0-9]+-20\\d\\d", text):
            return "moneyline"
        if any(value in text for value in ("spread", "handicap", "margin")):
            return "spread"
        if any(value in text for value in ("total", "over", "under")):
            return "total"
        if any(value in text for value in ("win", "winner", "vs", "match")):
            return "match_winner"
        return "sports_other"
    if category == "crypto":
        if any(value in text for value in ("up-or-down", "up or down")):
            return "up_down"
        if any(value in text for value in ("above", "below", "reach", "price")):
            return "price_threshold"
    if category == "politics" and any(value in text for value in ("win", "winner", "elected")):
        return "election_winner"
    if any(value in text for value in ("mention", "say ", "says ")):
        return "mention"
    return "binary_question"


def _liquidity_regime(trades: int, volume: Decimal) -> str:
    if trades < 20 or volume < Decimal("100"):
        return "thin"
    if trades < 100 or volume < Decimal("1000"):
        return "medium"
    return "liquid"


def _bias_direction(p_yes: Decimal, q_yes: Decimal) -> str:
    if (p_yes >= Decimal("0.5") and q_yes > p_yes) or (p_yes < Decimal("0.5") and q_yes < p_yes):
        return "classic_favorite_longshot"
    if q_yes != p_yes:
        return "reverse_favorite_longshot"
    return "none"


def _datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sql_time(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _decimal_text(value: Any) -> str:
    return str(Decimal(str(value or "0")).quantize(Q, rounding=ROUND_HALF_UP))


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--candidate-limit", type=int, default=50000)
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--categories", default="sports,crypto,politics,weather,pop_culture")
    parser.add_argument("--close-time-sources", default="gamma_closed_time")
    parser.add_argument("--min-bucket-trades", type=int, default=5)
    parser.add_argument("--min-samples", type=int, default=80)
    parser.add_argument("--bootstrap-samples", type=int, default=100)
    parser.add_argument("--confidence-level", type=float, default=0.90)
    parser.add_argument("--min-stability-periods", type=int, default=3)
    parser.add_argument("--min-period-samples", type=int, default=8)
    parser.add_argument("--min-stability-seasons", type=int, default=2)
    parser.add_argument("--min-season-samples", type=int, default=30)
    parser.add_argument("--edge-threshold", default="0.01")
    parser.add_argument("--execution-buffer", default="0.01")
    parser.add_argument("--min-trade-price", default="0.01")
    parser.add_argument("--max-trade-price", default="0.99")
    parser.add_argument("--fee-bps", default="0")
    parser.add_argument("--capital-cost-annual-rate", default="0")
    parser.add_argument("--profiles", default=",".join(DEFAULT_PROFILES))
    parser.add_argument("--portfolio-constraints", action="store_true")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime_outputs/conditional_favorite_longshot/conditional_v3.json"),
    )
    args = parser.parse_args(argv)
    min_trade_price = Decimal(args.min_trade_price)
    max_trade_price = Decimal(args.max_trade_price)
    if not Decimal("0.01") <= min_trade_price <= max_trade_price <= Decimal("0.99"):
        parser.error("trade price range must satisfy 0.01 <= min <= max <= 0.99")
    config = ConditionalConfig(
        market_limit=max(1, args.limit),
        query_candidate_limit=max(args.limit, args.candidate_limit),
        start_date=args.start_date,
        end_date=args.end_date or None,
        categories=tuple(value.strip().lower() for value in args.categories.split(",") if value.strip()),
        close_time_sources=tuple(value.strip() for value in args.close_time_sources.split(",") if value.strip()),
        min_bucket_trades=max(1, args.min_bucket_trades),
        min_samples=max(10, args.min_samples),
        bootstrap_samples=max(0, args.bootstrap_samples),
        confidence_level=max(0.50, min(0.999, args.confidence_level)),
        min_stability_periods=max(1, args.min_stability_periods),
        min_period_samples=max(1, args.min_period_samples),
        min_stability_seasons=max(1, args.min_stability_seasons),
        min_season_samples=max(1, args.min_season_samples),
        edge_threshold=Decimal(args.edge_threshold),
        execution_buffer=Decimal(args.execution_buffer),
        min_trade_price=min_trade_price,
        max_trade_price=max_trade_price,
        fee_bps=Decimal(args.fee_bps),
        capital_cost_annual_rate=Decimal(args.capital_cost_annual_rate),
        profiles=tuple(value.strip() for value in args.profiles.split(",") if value.strip()),
        apply_portfolio_constraints=bool(args.portfolio_constraints),
    )
    report = run_conditional_backtest(config)
    payload = _json_ready(asdict(report))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "strategy": report.strategy,
        "markets": report.market_count,
        "observations": report.observation_count,
        "signals": report.selected_signals,
        "profiles": [asdict(row) for row in report.profile_summaries],
        "output": str(args.output),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
