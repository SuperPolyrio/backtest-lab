#!/usr/bin/env python3
"""Build a LOB-labeled calibration dataset for fill-only execution."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_trade_validation import (  # noqa: E402
    VALIDATION_OUT_DIR,
    build_lob_holdout_calibration_plan,
    run_lob_holdout_validation,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-slug", action="append", default=[])
    parser.add_argument("--discover-market-limit", type=int, default=50)
    parser.add_argument("--build-tag", default="pmxt_v2_team_vs_team_main_lifecycle")
    parser.add_argument("--target-samples", type=int, default=1000)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--per-market", type=int, default=25)
    parser.add_argument("--lookback-hours", type=int, default=6)
    parser.add_argument("--book-ttl-ms", type=int, default=300_000)
    parser.add_argument("--depth-haircut", default="1.0")
    parser.add_argument("--price-tolerance", default="0.000001")
    parser.add_argument("--order-side", default="SAMPLE", choices=("SAMPLE", "BUY", "SELL"))
    parser.add_argument("--fill-only-profile", default="lob_holdout_calibrated_fill_only")
    parser.add_argument("--output-json", type=Path, default=VALIDATION_OUT_DIR / "fill_only_lob_calibration_dataset.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    plan = build_lob_holdout_calibration_plan(
        build_tag=str(args.build_tag),
        target_samples=max(1, int(args.target_samples)),
        discover_market_limit=args.discover_market_limit,
    )
    if args.plan_only:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        print(
            "status={status} target={target} markets={markets} recommended_per_market={per_market} expected_samples={expected} shortfall={shortfall} json={path}".format(
                status=plan["status"],
                target=plan["target_samples"],
                markets=plan["market_count"],
                per_market=plan["recommended_per_market"],
                expected=plan["expected_samples_at_recommended_per_market"],
                shortfall=plan["sample_shortfall"],
                path=args.output_json,
            )
        )
        return 0 if plan["market_count"] else 1
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
        discover_market_limit=args.discover_market_limit,
    )
    summary = dict(payload.get("summary") or {})
    comparisons = list(summary.get("comparisons") or [])
    dataset = {
        "schema_version": "fill_only_lob_labeled_dataset_v1",
        "status": payload.get("status"),
        "parameters": {
            "build_tag": args.build_tag,
            "target_samples": args.target_samples,
            "market_slug": args.market_slug,
            "discover_market_limit": args.discover_market_limit,
            "per_market": args.per_market,
            "order_side": args.order_side,
            "fill_only_profile": args.fill_only_profile,
        },
        "plan": plan,
        "metrics": {key: summary.get(key) for key in (
            "samples",
            "verdict_counts",
            "false_positive_rate",
            "false_negative_rate",
            "precision",
            "recall",
            "overfill_rate",
            "underfill_rate",
            "adverse_price_error",
            "avg_abs_size_error",
            "avg_abs_price_error",
        )},
        "effective_market_slugs": summary.get("effective_market_slugs"),
        "samples": [row.get("calibration_sample") for row in comparisons if row.get("calibration_sample")],
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(dataset, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(f"status={dataset['status']} samples={len(dataset['samples'])} json={args.output_json}")
    return 0 if dataset["samples"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
