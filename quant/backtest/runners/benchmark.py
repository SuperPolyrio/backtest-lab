"""Persistent multi-market backtest benchmarks."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
import json
import time
from typing import Any

from ..benchmark_persistence import (
    complete_benchmark_run,
    create_benchmark_run,
    fail_benchmark_run,
)
from .fast_accurate import build_fast_accurate_rows, summarize_fast_accurate_rows
from .nba_pregame_hold import (
    NbaFavoriteHoldReport,
    load_favorite_replay_dataset_for_markets,
    run_nba_pregame_favorite_hold_from_dataset,
)
from .report_artifacts import build_benchmark_artifacts
from .selectors import UniverseSpec, select_replay_universe
from .strategy_lab import FavoriteHoldStrategySpec


DEFAULT_PROFILES = ("fast:optimistic", "fast:realistic", "fast:conservative", "fast:stress", "accurate:realistic")


@dataclass(frozen=True)
class BenchmarkProfileResult:
    key: str
    replay_mode: str
    execution_profile: str
    raw_rows: int
    trades: int
    no_fills: int
    fill_rate: str
    partial_fill_rate: str
    total_pnl: str
    settlement_pnl: str
    trade_exit_pnl: str
    fee_total: str
    slippage_total: str
    db_query_sec: float
    engine_sec: float
    total_runtime_sec: float


@dataclass(frozen=True)
class BacktestBenchmarkResult:
    benchmark_id: int | None
    status: str
    universe_type: str
    universe_name: str
    market_count: int
    strategy_name: str
    parameters: dict[str, Any]
    profiles: dict[str, Any]
    summary: dict[str, Any]
    data_version: str
    rows: list[Any]
    profile_results: list[BenchmarkProfileResult]


def run_nba_fast_accurate_benchmark(
    *,
    limit: int = 50,
    persist_conn: Any | None = None,
    force_block_replay_backfill: bool = False,
    min_probability: Decimal = Decimal("0.60"),
    max_probability: Decimal = Decimal("0.80"),
    stake: Decimal = Decimal("10"),
    initial_capital: Decimal = Decimal("1000"),
    max_daily_cost: Decimal | None = Decimal("20"),
    max_concurrent_positions: int | None = 2,
    max_daily_trades: int | None = None,
    profile_keys: tuple[str, ...] = DEFAULT_PROFILES,
    benchmark_id: int | None = None,
) -> BacktestBenchmarkResult:
    """Run the NBA benchmark bundle and optionally persist it."""
    return run_orderfilled_fast_accurate_benchmark(
        universe_spec=UniverseSpec(universe_name="nba_2024_25_moneyline", universe_type="preset", limit=limit, category="sports"),
        persist_conn=persist_conn,
        force_block_replay_backfill=force_block_replay_backfill,
        min_probability=min_probability,
        max_probability=max_probability,
        stake=stake,
        initial_capital=initial_capital,
        max_daily_cost=max_daily_cost,
        max_concurrent_positions=max_concurrent_positions,
        max_daily_trades=max_daily_trades,
        profile_keys=profile_keys,
    )


def run_orderfilled_fast_accurate_benchmark(
    *,
    universe_spec: UniverseSpec,
    persist_conn: Any | None = None,
    force_block_replay_backfill: bool = False,
    min_probability: Decimal = Decimal("0.60"),
    max_probability: Decimal = Decimal("0.80"),
    stake: Decimal = Decimal("10"),
    initial_capital: Decimal = Decimal("1000"),
    max_daily_cost: Decimal | None = Decimal("20"),
    max_concurrent_positions: int | None = 2,
    max_daily_trades: int | None = None,
    profile_keys: tuple[str, ...] = DEFAULT_PROFILES,
    benchmark_id: int | None = None,
) -> BacktestBenchmarkResult:
    """Run a generic OrderFilled-first fast/accurate benchmark bundle."""

    started = time.perf_counter()
    strategy_name = "favorite_hold_v1"
    universe_name = universe_spec.universe_name
    parameters = {
        "limit": int(universe_spec.limit),
        "universe": _universe_payload(universe_spec),
        "min_probability": str(min_probability),
        "max_probability": str(max_probability),
        "snapshot_hours_before_start": "1",
        "signal_lookback_hours": "24",
        "window_start_hours": "1",
        "window_end_hours": "0",
        "initial_capital": str(initial_capital),
        "stake": str(stake),
        "max_daily_cost": str(max_daily_cost) if max_daily_cost is not None else None,
        "max_concurrent_positions": max_concurrent_positions,
        "max_daily_trades": max_daily_trades,
        "yes_only": True,
        "sort_by": "probability_desc",
    }
    profiles = {"requested": list(profile_keys)}
    if persist_conn is not None and benchmark_id is None:
        benchmark_id = create_benchmark_run(
            persist_conn,
            universe_type=universe_spec.universe_type,
            universe_name=universe_name,
            market_count=universe_spec.limit,
            strategy_name=strategy_name,
            parameters=parameters,
            profiles=profiles,
        )
        persist_conn.commit()

    try:
        markets = select_replay_universe(universe_spec)
        base_spec = FavoriteHoldStrategySpec(
            min_probability=min_probability,
            max_probability=max_probability,
            snapshot_hours_before_start=Decimal("1"),
            signal_lookback_hours=Decimal("24"),
            window_start_hours=Decimal("1"),
            window_end_hours=Decimal("0"),
            initial_capital=initial_capital,
            stake=stake,
            liquidity_cap_pct=Decimal("100"),
            enforce_bankroll=True,
            max_daily_trades=max_daily_trades,
            max_daily_cost=max_daily_cost,
            max_concurrent_positions=max_concurrent_positions,
            sort_by="probability_desc",
            yes_only=True,
            execution_profile="realistic",
        )
        needs_fast = any(_parse_profile_key(key)[0] == "fast" for key in profile_keys)
        needs_accurate = any(_parse_profile_key(key)[0] == "accurate" for key in profile_keys)
        if not needs_fast and needs_accurate:
            # Accurate reports still use block replay coverage as their baseline,
            # but do not execute every fast profile unless explicitly requested.
            needs_fast = True
        fast_dataset = (
            load_favorite_replay_dataset_for_markets(
                markets,
                specs=[base_spec],
                replay_mode="fast",
                force_block_replay_backfill=force_block_replay_backfill,
                build_tag=f"{universe_name}_fast_replay",
            )
            if needs_fast
            else None
        )
        accurate_dataset = (
            load_favorite_replay_dataset_for_markets(
                markets,
                specs=[base_spec],
                replay_mode="accurate",
            )
            if needs_accurate
            else None
        )
        reports: dict[str, NbaFavoriteHoldReport] = {}
        for key in profile_keys:
            replay_mode, execution_profile = _parse_profile_key(key)
            dataset = fast_dataset if replay_mode == "fast" else accurate_dataset
            if dataset is None:
                continue
            spec = FavoriteHoldStrategySpec(**{**base_spec.__dict__, "execution_profile": execution_profile})
            reports[key] = run_nba_pregame_favorite_hold_from_dataset(
                dataset,
                spec=spec,
                db_query_sec=dataset.db_query_sec,
                total_start=time.perf_counter(),
            )
        fast_realistic = reports.get("fast:realistic")
        accurate_realistic = reports.get("accurate:realistic")
        comparison_rows = (
            build_fast_accurate_rows(fast_realistic.trade_rows, accurate_realistic.trade_rows)
            if fast_realistic and accurate_realistic
            else []
        )
        comparison = summarize_fast_accurate_rows(comparison_rows)
        profile_results = [
            _profile_result(
                key,
                report,
                raw_rows=(
                    (fast_dataset.raw_row_count if fast_dataset is not None else 0)
                    if key.startswith("fast:")
                    else (accurate_dataset.raw_row_count if accurate_dataset is not None else 0)
                ),
            )
            for key, report in reports.items()
        ]
        backfill = fast_dataset.backfill_result if fast_dataset is not None else None
        coverage_summary = {
            "table": backfill.table if backfill else "orderfilled_block_replay",
            "market_count": backfill.market_count if backfill else 0,
            "from_block": backfill.from_block if backfill else 0,
            "to_block": backfill.to_block if backfill else 0,
            "before_rows": backfill.before_rows if backfill else 0,
            "inserted_rows": backfill.inserted_rows if backfill else 0,
            "after_rows": backfill.after_rows if backfill else 0,
            "coverage_rows": backfill.coverage_rows if backfill else 0,
            "elapsed_sec": backfill.elapsed_sec if backfill else 0,
        }
        universe_payload = {
            **_universe_payload(universe_spec),
            "selected_market_count": len(markets),
            "fast_raw_market_count": fast_dataset.raw_market_count if fast_dataset is not None else 0,
            "accurate_raw_market_count": accurate_dataset.raw_market_count if accurate_dataset is not None else 0,
        }
        extra_artifacts = build_benchmark_artifacts(
            reports=reports,
            comparison_rows=comparison_rows,
            coverage=coverage_summary,
            universe=universe_payload,
        )
        summary = {
            "market_count": len(markets),
            "universe": universe_payload,
            "fast_raw_rows": fast_dataset.raw_row_count if fast_dataset is not None else 0,
            "accurate_raw_rows": accurate_dataset.raw_row_count if accurate_dataset is not None else 0,
            "status_mismatches": comparison.status_mismatches,
            "pnl_diff_abs_total": comparison.pnl_diff_abs_total,
            "total_pnl_diff": comparison.pnl_diff_abs_total,
            "fast_total_pnl": comparison.fast_total_pnl,
            "accurate_total_pnl": comparison.accurate_total_pnl,
            "coverage": coverage_summary,
            "fill_quality": extra_artifacts["fill_quality"],
            "data_quality": extra_artifacts["data_quality"],
            "regime_coverage": extra_artifacts["regime_coverage"],
            "prediction_quality": extra_artifacts["prediction_quality"],
            "performance_score": extra_artifacts["performance_score"],
            "execution_profile_matrix": extra_artifacts["execution_profile_matrix"],
            "execution_model_rows": extra_artifacts["execution_model_rows"],
            "execution_model_validation": extra_artifacts["execution_model_validation"],
            "parameter_robustness": extra_artifacts["parameter_robustness"],
            "parameter_search_plan": extra_artifacts["parameter_search_plan"],
            "parameter_search_results": extra_artifacts["parameter_search_results"],
            "profiles": [asdict(row) for row in profile_results],
            "timing": {
                "fast_db_query_sec": fast_dataset.db_query_sec if fast_dataset is not None else 0,
                "accurate_db_query_sec": accurate_dataset.db_query_sec if accurate_dataset is not None else 0,
                "total_runtime_sec": round(time.perf_counter() - started, 6),
            },
        }
        data_version = _fingerprint({"parameters": parameters, "summary": summary})
        result = BacktestBenchmarkResult(
            benchmark_id=benchmark_id,
            status="completed",
            universe_type=universe_spec.universe_type,
            universe_name=universe_name,
            market_count=len(markets),
            strategy_name=strategy_name,
            parameters=parameters,
            profiles=profiles,
            summary=summary,
            data_version=data_version,
            rows=comparison_rows,
            profile_results=profile_results,
        )
        if persist_conn is not None and benchmark_id is not None:
            complete_benchmark_run(
                persist_conn,
                benchmark_id=benchmark_id,
                summary=summary,
                rows=comparison_rows,
                artifacts={
                    "summary": summary,
                    **extra_artifacts,
                },
                data_version=data_version,
            )
            persist_conn.commit()
        return result
    except Exception as exc:
        if persist_conn is not None and benchmark_id is not None:
            fail_benchmark_run(persist_conn, benchmark_id=benchmark_id, error=str(exc))
            persist_conn.commit()
        raise


def _parse_profile_key(key: str) -> tuple[str, str]:
    replay_mode, _, execution_profile = str(key).partition(":")
    replay_mode = replay_mode if replay_mode in {"fast", "accurate"} else "fast"
    execution_profile = execution_profile or "realistic"
    return replay_mode, execution_profile


def _universe_payload(spec: UniverseSpec) -> dict[str, Any]:
    return {
        "universe_name": spec.universe_name,
        "universe_type": spec.universe_type,
        "limit": spec.limit,
        "market_ids": list(spec.market_ids),
        "market_slugs": list(spec.market_slugs),
        "event_slug": spec.event_slug,
        "category": spec.category,
        "start_date": spec.start_date,
        "end_date": spec.end_date,
        "require_resolved": spec.require_resolved,
        "require_orderfilled_rows": spec.require_orderfilled_rows,
    }


def _profile_result(key: str, report: NbaFavoriteHoldReport, *, raw_rows: int) -> BenchmarkProfileResult:
    partial = sum(1 for row in report.trade_rows if row.order_status == "PARTIAL_FILLED")
    signal_count = max(1, int(report.signal_count))
    return BenchmarkProfileResult(
        key=key,
        replay_mode=key.split(":", 1)[0],
        execution_profile=key.split(":", 1)[1] if ":" in key else "realistic",
        raw_rows=int(raw_rows),
        trades=int(report.trades),
        no_fills=int(report.no_fills),
        fill_rate=str(Decimal(report.trades) / Decimal(signal_count)),
        partial_fill_rate=str(Decimal(partial) / Decimal(signal_count)),
        total_pnl=report.total_pnl,
        settlement_pnl=report.settlement_pnl,
        trade_exit_pnl=report.trade_exit_pnl,
        fee_total=report.fee_total,
        slippage_total=report.slippage_total,
        db_query_sec=report.db_query_sec,
        engine_sec=report.engine_sec,
        total_runtime_sec=report.total_runtime_sec,
    )


def _fingerprint(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe", default="nba_2024_25_moneyline")
    parser.add_argument("--universe-type", default="")
    parser.add_argument("--category", default="")
    parser.add_argument("--event-slug", default="")
    parser.add_argument("--market-slug", action="append", default=[])
    parser.add_argument("--market-id", action="append", type=int, default=[])
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--allow-unresolved", action="store_true")
    parser.add_argument("--allow-empty-orderfilled", action="store_true")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--force-block-replay-backfill", action="store_true")
    parser.add_argument("--profiles", default=",".join(DEFAULT_PROFILES))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    universe_spec = UniverseSpec(
        universe_name=args.universe,
        universe_type=args.universe_type or ("event" if args.event_slug else "preset"),
        limit=args.limit,
        market_ids=tuple(args.market_id or ()),
        market_slugs=tuple(args.market_slug or ()),
        event_slug=args.event_slug or None,
        category=args.category or None,
        start_date=args.start_date or None,
        end_date=args.end_date or None,
        require_resolved=not args.allow_unresolved,
        require_orderfilled_rows=not args.allow_empty_orderfilled,
    )
    profile_keys = tuple(item.strip() for item in str(args.profiles).split(",") if item.strip()) or DEFAULT_PROFILES
    result = run_orderfilled_fast_accurate_benchmark(
        universe_spec=universe_spec,
        force_block_replay_backfill=args.force_block_replay_backfill,
        profile_keys=profile_keys,
    )
    payload = asdict(result)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
    else:
        print(
            f"benchmark {result.universe_name}: markets={result.market_count} "
            f"fast_rows={result.summary.get('fast_raw_rows')} accurate_rows={result.summary.get('accurate_raw_rows')} "
            f"status_mismatches={result.summary.get('status_mismatches')} "
            f"pnl_diff_abs={result.summary.get('pnl_diff_abs_total')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
