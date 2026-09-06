#!/usr/bin/env python3
"""Generate the aggregate fill-trade-only validation report."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_trade_validation import (
    VALIDATION_OUT_DIR,
    benchmark_fill_trade_replay,
    build_parameter_sensitivity_report,
    compare_reference_vs_indexed,
    run_golden_fixtures,
    run_lob_holdout_validation,
    run_trade_print_plumbing_validation,
    status_from_checks,
    validate_execution_invariants,
    validate_latest_v2_run_from_postgres,
    validate_orderfilled_data_contract,
    write_report,
)
from quant.backtest.orderfilled_v2_replay import replay_v2_taker_orders_with_diagnostics
from quant.backtest.fill_trade_validation import ValidationCheck, markdown_report, synthetic_orders_and_trades


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-db", action="store_true", help="Run ClickHouse/Postgres backed validations too.")
    parser.add_argument("--include-lob-holdout", action="store_true")
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--trades", type=int, default=5000)
    parser.add_argument("--orders", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-json", type=Path, default=VALIDATION_OUT_DIR / "fill_trade_validation_report.json")
    parser.add_argument("--output-md", type=Path, default=VALIDATION_OUT_DIR / "fill_trade_validation_report.md")
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sections = []
    sections.append(run_golden_fixtures())
    sections.append(compare_reference_vs_indexed(trades_count=100, orders_count=20, seed=args.seed))

    orders, trades = synthetic_orders_and_trades(trades_count=args.trades, orders_count=args.orders, seed=args.seed)
    results, ledger, _ = replay_v2_taker_orders_with_diagnostics(orders, trades)
    sections.append(validate_execution_invariants(orders=orders, results=results, ledger=ledger, trades=trades))
    sections.append(benchmark_fill_trade_replay(trades_count=args.trades, orders_count=args.orders, seed=args.seed, reference=False))
    sections.append(build_parameter_sensitivity_report(trades_count=min(args.trades, 5000), orders_count=min(args.orders, 500), seed=args.seed))

    if args.include_db:
        sections.append(validate_orderfilled_data_contract(from_block=args.from_block, to_block=args.to_block))
        sections.append(run_trade_print_plumbing_validation(from_block=args.from_block, to_block=args.to_block))
        sections.append(validate_latest_v2_run_from_postgres())
    if args.include_lob_holdout:
        sections.append(run_lob_holdout_validation())

    checks = [
        ValidationCheck(
            name=f"section {section['title']}",
            status="PASS" if section["status"] == "pass" else "REVIEW" if section["status"] == "review" else "FAIL",
            detail=section.get("check_summary"),
        )
        for section in sections
    ]
    payload = {
        "title": "Fill Trade Validation Report",
        "status": status_from_checks(checks, strict_review=args.strict),
        "summary": {
            "sections": len(sections),
            "include_db": args.include_db,
            "include_lob_holdout": args.include_lob_holdout,
        },
        "checks": [row.as_dict() for row in checks],
        "sections": sections,
    }
    write_report(payload, output_json=args.output_json, output_md=args.output_md)
    args.output_md.write_text(_aggregate_markdown(payload), encoding="utf-8")
    print(f"status={payload['status']} json={args.output_json} md={args.output_md}")
    return 1 if args.strict and payload["status"] != "pass" else 0


def _aggregate_markdown(payload: dict) -> str:
    lines = [markdown_report(payload), "", "## Section Details", ""]
    for section in payload.get("sections", []):
        lines.extend(["", markdown_report(section)])
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
