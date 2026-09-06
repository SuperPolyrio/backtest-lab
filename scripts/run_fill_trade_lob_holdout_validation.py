#!/usr/bin/env python3
"""Compare the same real orders through fill-only and L2-depth holdout replay."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_trade_validation import (
    VALIDATION_OUT_DIR,
    run_lob_holdout_run_summary_validation,
    run_lob_holdout_validation,
    write_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v2-run-id", type=int, default=None)
    parser.add_argument("--l2-run-id", type=int, default=None)
    parser.add_argument("--run-summary", action="store_true", help="Use the old persisted-run fill-rate comparison.")
    parser.add_argument("--market-slug", action="append", default=[])
    parser.add_argument("--build-tag", default="pmxt_v2_team_vs_team_main_lifecycle")
    parser.add_argument("--per-market", type=int, default=1)
    parser.add_argument("--discover-market-limit", type=int, default=None, help="Auto-discover depth+fill markets for the build tag when market slugs are not provided.")
    parser.add_argument("--lookback-hours", type=int, default=6)
    parser.add_argument("--book-ttl-ms", type=int, default=300_000)
    parser.add_argument("--depth-haircut", default="1.0")
    parser.add_argument("--price-tolerance", default="0.000001")
    parser.add_argument("--max-false-positive-rate", default="0.05")
    parser.add_argument("--max-overfill-rate", default="0.05")
    parser.add_argument("--max-adverse-price-error", default="0.005")
    parser.add_argument("--min-precision", default="0.90")
    parser.add_argument(
        "--order-side",
        default="SAMPLE",
        choices=("SAMPLE", "BUY", "SELL"),
        help="SAMPLE replays each sample's historical side; BUY/SELL forces the same side for every sample.",
    )
    parser.add_argument(
        "--fill-only-profile",
        default="conservative_trade_tape",
        choices=("plumbing_replay", "strict_audit", "conservative_trade_tape", "lob_holdout_calibrated_fill_only", "optimistic_sensitivity"),
        help="plumbing_replay reproduces source fills; named profiles run stricter counterfactual fill-only replay without LOB.",
    )
    parser.add_argument("--output-json", type=Path, default=VALIDATION_OUT_DIR / "lob_holdout_validation.json")
    parser.add_argument("--output-md", type=Path, default=VALIDATION_OUT_DIR / "lob_holdout_validation.md")
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.run_summary:
        payload = run_lob_holdout_run_summary_validation(v2_run_id=args.v2_run_id, l2_run_id=args.l2_run_id)
    else:
        payload = run_lob_holdout_validation(
            market_slugs=args.market_slug or None,
            build_tag=str(args.build_tag),
            per_market=max(1, int(args.per_market)),
            lookback_hours=max(1, int(args.lookback_hours)),
            book_ttl_ms=max(1, int(args.book_ttl_ms)),
            depth_haircut=args.depth_haircut,
            price_tolerance=args.price_tolerance,
            order_side=args.order_side,
            fill_only_profile=args.fill_only_profile,
            max_false_positive_rate=args.max_false_positive_rate,
            max_overfill_rate=args.max_overfill_rate,
            max_adverse_price_error=args.max_adverse_price_error,
            min_precision=args.min_precision,
            discover_market_limit=args.discover_market_limit,
        )
    write_report(payload, output_json=args.output_json, output_md=args.output_md)
    print(f"status={payload['status']} json={args.output_json} md={args.output_md}")
    return 1 if args.strict and payload["status"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())
