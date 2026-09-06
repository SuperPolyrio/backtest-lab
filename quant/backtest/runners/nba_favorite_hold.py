"""CLI for the NBA pregame favorite hold-to-settlement strategy."""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from decimal import Decimal
import json
import sys

from .nba_pregame_hold import run_nba_fast_accurate_comparison, run_nba_pregame_favorite_hold, run_nba_pregame_favorite_hold_sweep
from .strategy_lab import FavoriteHoldStrategySpec


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--min-probability", default="0.60")
    parser.add_argument("--max-probability", default="0.80")
    parser.add_argument("--snapshot-hours-before-start", default="1")
    parser.add_argument("--signal-lookback-hours", default="24")
    parser.add_argument("--window-start-hours", default="4")
    parser.add_argument("--window-end-hours", default="0")
    parser.add_argument("--initial-capital", default="1000")
    parser.add_argument("--stake", default="10")
    parser.add_argument("--liquidity-cap-pct", default="100")
    parser.add_argument("--no-bankroll", action="store_true")
    parser.add_argument("--max-daily-trades", type=int, default=0)
    parser.add_argument("--max-daily-cost", default="")
    parser.add_argument("--max-concurrent-positions", type=int, default=0)
    parser.add_argument(
        "--sort-by",
        default="probability_desc",
        choices=(
            "probability_desc",
            "snapshot_probability_desc",
            "probability_asc",
            "closest_to_min_probability",
            "rows_desc",
            "signal_time_asc",
            "close_line_edge_desc",
            "close_line_edge_asc",
        ),
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--csv", default="")
    parser.add_argument("--sweep", action="store_true", help="Run a small multi-strategy sweep with one shared NBA replay dataset.")
    parser.add_argument("--replay-mode", choices=("accurate", "fast"), default="accurate")
    parser.add_argument("--compare-fast-accurate", action="store_true", help="Run fast block replay and accurate raw replay on the same NBA strategy.")
    parser.add_argument("--force-block-replay-backfill", action="store_true")
    parser.add_argument("--buy-favorite-side", action="store_true", help="Allow buying NO when NO is the high-probability side. Default validation strategy buys YES only.")
    parser.add_argument("--execution-profile", choices=("optimistic", "realistic", "conservative", "stress"), default="realistic")
    args = parser.parse_args(argv)
    if args.compare_fast_accurate:
        report = run_nba_fast_accurate_comparison(
            limit=args.limit,
            min_probability=Decimal(str(args.min_probability)),
            max_probability=Decimal(str(args.max_probability)),
            snapshot_hours_before_start=Decimal(str(args.snapshot_hours_before_start)),
            signal_lookback_hours=Decimal(str(args.signal_lookback_hours)),
            window_start_hours=Decimal(str(args.window_start_hours)),
            window_end_hours=Decimal(str(args.window_end_hours)),
            initial_capital=Decimal(str(args.initial_capital)),
            stake=Decimal(str(args.stake)),
            liquidity_cap_pct=Decimal(str(args.liquidity_cap_pct)),
            max_daily_trades=args.max_daily_trades or None,
            max_daily_cost=Decimal(str(args.max_daily_cost)) if str(args.max_daily_cost).strip() else None,
            max_concurrent_positions=args.max_concurrent_positions or None,
            sort_by=args.sort_by,
            force_block_replay_backfill=args.force_block_replay_backfill,
            yes_only=not args.buy_favorite_side,
            execution_profile=args.execution_profile,
        )
        payload = asdict(report)
        payload["rows"] = [asdict(row) for row in report.rows]
        if args.csv and report.rows:
            with open(args.csv, "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=list(asdict(report.rows[0]).keys()))
                writer.writeheader()
                for row in report.rows:
                    writer.writerow(asdict(row))
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(
                f"{report.run_id}: markets={report.market_count} fast_rows={report.fast_raw_rows} "
                f"accurate_rows={report.accurate_raw_rows} fast_fills={report.fast_trades} "
                f"accurate_fills={report.accurate_trades} status_mismatches={report.status_mismatches} "
                f"fast_pnl={report.fast_total_pnl} accurate_pnl={report.accurate_total_pnl} "
                f"pnl_diff_abs={report.pnl_diff_abs_total} total_sec={report.total_runtime_sec}"
            )
            print(
                f"timing: fast_db={report.fast_db_query_sec}s accurate_db={report.accurate_db_query_sec}s "
                f"fast_engine={report.fast_engine_sec}s accurate_engine={report.accurate_engine_sec}s "
                f"backfill={report.block_replay_backfill_sec}s"
            )
            for row in report.rows[:50]:
                print(
                    f"{row.market_slug} | fast={row.fast_status} {row.fast_pnl} "
                    f"accurate={row.accurate_status} {row.accurate_pnl} diff={row.pnl_diff} "
                    f"quality={row.data_quality}"
                )
        return 0
    if args.sweep:
        common = dict(
            initial_capital=Decimal(str(args.initial_capital)),
            stake=Decimal(str(args.stake)),
            liquidity_cap_pct=Decimal(str(args.liquidity_cap_pct)),
            enforce_bankroll=not args.no_bankroll,
            max_daily_trades=args.max_daily_trades or None,
            max_daily_cost=Decimal(str(args.max_daily_cost)) if str(args.max_daily_cost).strip() else None,
            max_concurrent_positions=args.max_concurrent_positions or None,
            sort_by=args.sort_by,
            yes_only=not args.buy_favorite_side,
            execution_profile=args.execution_profile,
        )
        specs = [
            FavoriteHoldStrategySpec(
                min_probability=Decimal(str(args.min_probability)),
                max_probability=Decimal(str(args.max_probability)),
                snapshot_hours_before_start=Decimal(str(args.snapshot_hours_before_start)),
                signal_lookback_hours=Decimal(str(args.signal_lookback_hours)),
                window_start_hours=Decimal(str(args.window_start_hours)),
                window_end_hours=Decimal(str(args.window_end_hours)),
                **common,
            ),
            FavoriteHoldStrategySpec(
                min_probability=Decimal("0.65"),
                max_probability=Decimal("0.75"),
                snapshot_hours_before_start=Decimal(str(args.snapshot_hours_before_start)),
                signal_lookback_hours=Decimal(str(args.signal_lookback_hours)),
                window_start_hours=Decimal(str(args.window_start_hours)),
                window_end_hours=Decimal(str(args.window_end_hours)),
                **common,
            ),
            FavoriteHoldStrategySpec(
                min_probability=Decimal(str(args.min_probability)),
                max_probability=Decimal("0.70"),
                snapshot_hours_before_start=Decimal("0.5"),
                signal_lookback_hours=Decimal(str(args.signal_lookback_hours)),
                window_start_hours=Decimal(str(args.window_start_hours)),
                window_end_hours=Decimal(str(args.window_end_hours)),
                **common,
            ),
        ]
        sweep = run_nba_pregame_favorite_hold_sweep(specs=specs, limit=args.limit)
        payload = asdict(sweep)
        payload["reports"] = [asdict(report) for report in sweep.reports]
        if args.json:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(
                f"{sweep.run_id}: strategies={sweep.strategy_count} markets={sweep.market_count} "
                f"raw_rows={sweep.raw_row_count} db_query_sec={sweep.db_query_sec} "
                f"engine_sec={sweep.engine_sec} total_sec={sweep.total_runtime_sec}"
            )
            for index, report in enumerate(sweep.reports, start=1):
                print(
                    f"strategy#{index}: prob={report.min_probability}-{report.max_probability} "
                    f"signals={report.signal_count} fills={report.trades} pnl={report.total_pnl} "
                    f"engine_sec={report.engine_sec}"
                )
        return 0
    report = run_nba_pregame_favorite_hold(
        limit=args.limit,
        min_probability=Decimal(str(args.min_probability)),
        max_probability=Decimal(str(args.max_probability)),
        snapshot_hours_before_start=Decimal(str(args.snapshot_hours_before_start)),
        signal_lookback_hours=Decimal(str(args.signal_lookback_hours)),
        window_start_hours=Decimal(str(args.window_start_hours)),
        window_end_hours=Decimal(str(args.window_end_hours)),
        initial_capital=Decimal(str(args.initial_capital)),
        stake=Decimal(str(args.stake)),
        liquidity_cap_pct=Decimal(str(args.liquidity_cap_pct)),
        enforce_bankroll=not args.no_bankroll,
        max_daily_trades=args.max_daily_trades or None,
        max_daily_cost=Decimal(str(args.max_daily_cost)) if str(args.max_daily_cost).strip() else None,
        max_concurrent_positions=args.max_concurrent_positions or None,
        sort_by=args.sort_by,
        replay_mode=args.replay_mode,
        force_block_replay_backfill=args.force_block_replay_backfill,
        yes_only=not args.buy_favorite_side,
        execution_profile=args.execution_profile,
    )
    if args.csv and report.trade_rows:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(asdict(report.trade_rows[0]).keys()))
            writer.writeheader()
            for row in report.trade_rows:
                writer.writerow(asdict(row))
    payload = asdict(report)
    payload["trade_rows"] = [asdict(row) for row in report.trade_rows]
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    else:
        print(
            f"{report.run_id}: markets={report.market_count} raw_markets={report.raw_market_count} "
            f"signals={report.signal_count} fills={report.trades} no_fills={report.no_fills} "
            f"skipped_daily_cap={report.skipped_daily_trade_cap} skipped_daily_cost={report.skipped_daily_cost_cap} "
            f"skipped_pos_cap={report.skipped_concurrent_positions_cap} skipped_cash={report.skipped_insufficient_cash} "
            f"wins={report.wins} losses={report.losses} pnl={report.total_pnl} "
            f"ending_capital={report.ending_capital} roi_on_cost={report.total_roi_on_cost}"
        )
        for row in report.trade_rows:
            print(
                f"{row.market_slug} | {row.title} | buy={row.buy_outcome_label} "
                f"prob={row.signal_probability} status={row.order_status} cost={row.buy_cost} "
                f"settle={row.settlement_outcome} pnl={row.pnl}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
