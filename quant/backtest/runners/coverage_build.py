"""Build or refresh block replay coverage for a benchmark universe."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal
import json

from .nba_pregame_hold import load_nba_favorite_replay_dataset
from .strategy_lab import FavoriteHoldStrategySpec


def build_replay_coverage(
    *,
    universe: str = "nba_2024_25_moneyline",
    limit: int = 500,
    window_start_hours: Decimal = Decimal("25"),
    window_end_hours: Decimal = Decimal("0"),
    force: bool = False,
) -> dict:
    if universe != "nba_2024_25_moneyline":
        raise ValueError("only nba_2024_25_moneyline coverage is supported in this version")
    spec = FavoriteHoldStrategySpec(
        window_start_hours=window_start_hours,
        window_end_hours=window_end_hours,
        snapshot_hours_before_start=Decimal("1"),
        signal_lookback_hours=Decimal("24"),
    )
    dataset = load_nba_favorite_replay_dataset(
        limit=limit,
        specs=[spec],
        replay_mode="fast",
        force_block_replay_backfill=force,
    )
    backfill = dataset.backfill_result
    return {
        "universe": universe,
        "limit": int(limit),
        "raw_market_count": dataset.raw_market_count,
        "raw_row_count": dataset.raw_row_count,
        "db_query_sec": dataset.db_query_sec,
        "coverage": asdict(backfill) if backfill else None,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe", default="nba_2024_25_moneyline")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--window-start-hours", default="25")
    parser.add_argument("--window-end-hours", default="0")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    result = build_replay_coverage(
        universe=args.universe,
        limit=args.limit,
        window_start_hours=Decimal(str(args.window_start_hours)),
        window_end_hours=Decimal(str(args.window_end_hours)),
        force=args.force,
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    else:
        coverage = result.get("coverage") or {}
        print(
            f"coverage {result['universe']}: markets={result['raw_market_count']} rows={result['raw_row_count']} "
            f"inserted={coverage.get('inserted_rows', 0)} coverage_rows={coverage.get('coverage_rows', 0)} "
            f"db={result['db_query_sec']}s"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
