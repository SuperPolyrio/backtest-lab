#!/usr/bin/env python3
"""Create a bounded current-schema fill-first sample backtest run."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.sample_run import READY, run_fill_first_sample_backtest  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed-run-id", type=int, default=None, help="Clone market/window/params from an existing run.")
    parser.add_argument("--market-slug", default=None, help="Explicit market slug. Defaults to latest fill-first run.")
    parser.add_argument("--token-id", default=None, help="Explicit token id; keeps the sample query index-bounded.")
    parser.add_argument("--token-side", default="YES", choices=("YES", "NO"))
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--window-rows", type=int, default=1000, help="Recent block-close rows to use when a window is not provided.")
    parser.add_argument("--min-orderfilled-rows", type=int, default=25, help="Fallback candidate minimum materialized orderfilled rows.")
    parser.add_argument("--entry-threshold", default=None)
    parser.add_argument("--exit-threshold", default=None)
    parser.add_argument("--take-profit", default=None)
    parser.add_argument("--position-size", default=None)
    parser.add_argument("--initial-capital", default=None)
    parser.add_argument("--order-role", choices=("maker", "taker"), default=None)
    parser.add_argument("--buy-limit-price", default=None)
    parser.add_argument("--sell-limit-price", default=None)
    parser.add_argument("--settlement-value", default=None)
    parser.add_argument("--liquidity-cap-pct", default=None)
    parser.add_argument("--fill-probability-haircut-pct", default=None)
    parser.add_argument(
        "--execution-price-mode",
        default=None,
        help="Execution mode for the sample run, e.g. ORDERFILLED_CROSS or DEPTH.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Build payload only; do not insert a run.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero unless the sample report is ready.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with postgres_connection(readonly=bool(args.dry_run)) as conn:
        if not args.dry_run:
            create_schema(conn)
        report = run_fill_first_sample_backtest(
            conn,
            seed_run_id=args.seed_run_id,
            market_slug=args.market_slug,
            token_id=args.token_id,
            token_side=args.token_side,
            from_block=args.from_block,
            to_block=args.to_block,
            window_rows=max(2, int(args.window_rows)),
            min_orderfilled_rows=max(1, int(args.min_orderfilled_rows)),
            entry_threshold=args.entry_threshold,
            exit_threshold=args.exit_threshold,
            take_profit=args.take_profit,
            position_size=args.position_size,
            initial_capital=args.initial_capital,
            order_role=args.order_role,
            buy_limit_price=args.buy_limit_price,
            sell_limit_price=args.sell_limit_price,
            settlement_value=args.settlement_value,
            liquidity_cap_pct=args.liquidity_cap_pct,
            fill_probability_haircut_pct=args.fill_probability_haircut_pct,
            execution_price_mode=args.execution_price_mode,
            dry_run=args.dry_run,
        )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(_to_markdown(report))
    if report.get("status") == READY:
        return 0
    return 2 if args.strict else 0


def _to_markdown(report: dict) -> str:
    candidate = report.get("candidate") or {}
    payload = report.get("payload") or {}
    run = report.get("run") or {}
    artifact = report.get("artifact_report") or {}
    lines = [
        f"# Fill-first Sample Backtest: {report.get('status')}",
        "",
        f"- dry_run: {report.get('dry_run')}",
        f"- run_id: {report.get('run_id') or run.get('run_id') or '-'}",
        f"- artifact_status: {report.get('artifact_status') or '-'}",
        f"- artifact_schema_version: {report.get('artifact_schema_version') or '-'}",
        f"- market_slug: {candidate.get('market_slug') or payload.get('market_slug')}",
        f"- token_side: {candidate.get('token_side') or payload.get('token_side')}",
        f"- token_id: {candidate.get('token_id') or payload.get('token_id') or '-'}",
        f"- block_window: {candidate.get('from_block')} -> {candidate.get('to_block')}",
        f"- candidate_source: {candidate.get('source')}",
        f"- seed_run_id: {candidate.get('seed_run_id') or '-'}",
        f"- execution_price_mode: {payload.get('execution_price_mode')}",
        f"- order_role: {payload.get('order_role')}",
        f"- buy_limit_price: {payload.get('buy_limit_price') or '-'}",
        f"- sell_limit_price: {payload.get('sell_limit_price') or '-'}",
        f"- settlement_value: {payload.get('settlement_value') or '-'}",
        f"- rows_processed: {run.get('rows_processed') or '-'}",
    ]
    checks = artifact.get("checks") if isinstance(artifact, dict) else None
    if checks:
        lines.extend(["", "| check | status | detail |", "| --- | --- | --- |"])
        for check in checks:
            detail = str(check.get("detail") or "").replace("|", "\\|")
            lines.append(
                f"| {check.get('name')} | {check.get('status')} | {detail} |"
            )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
