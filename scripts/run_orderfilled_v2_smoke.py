#!/usr/bin/env python3
"""Run a tiny real-data smoke check for the OrderFilled V2 replay runner."""

from __future__ import annotations

import json
import sys
import argparse
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.orderfilled_v2_replay import (
    V2MakerOrder,
    V2TakerOrder,
    build_v2_observed_fill_replay_report,
    build_v2_replay_report,
    build_v2_robustness_report,
    calibrate_v2_live_fills,
    load_v2_trade_prints,
    load_v2_trade_slices_for_orders,
    load_v2_wallet_fill_ticks,
    load_v2_calibration_actual_rows,
    persist_v2_replay_run,
    replay_v2_maker_order,
    replay_v2_taker_orders,
    replay_v2_taker_orders_with_diagnostics,
    summarize_v2_results,
    wallet_fill_to_observed_order,
)
from quant.core.db import ClickHouseClient, postgres_connection
from quant.core.schema import create_schema


OUT_DIR = PROJECT_ROOT / "runtime_outputs" / "orderfilled_v2_backtest"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--persist-postgres", action="store_true", help="Write this smoke replay into quant_backtest_* tables.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not run schema initialization before --persist-postgres.")
    parser.add_argument("--market-slug", default="orderfilled-v2-smoke", help="market_slug used when persisting the smoke run.")
    parser.add_argument("--token-side", default="YES", help="token_side used when persisting the smoke run.")
    parser.add_argument("--calibration-run-id", type=int, default=None, help="Read actual live calibration rows for this run id.")
    parser.add_argument("--calibration-source", default=None, help="Optional source filter for calibration rows.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    client = ClickHouseClient()
    candidate = _select_candidate(client)
    if not candidate:
        raise SystemExit("no trade_prints_one_sided candidate found")
    trades = load_v2_trade_prints(
        market_id=int(candidate["market_id"]),
        asset_id=str(candidate["asset_id"]),
        from_block=int(candidate["min_block"]),
        to_block=int(candidate["min_block"]) + 100000,
        client=client,
        limit=200,
    )
    buy_trade = next((trade for trade in trades if trade.aggressor_side == "BUY"), None)
    sell_trade = next((trade for trade in trades if trade.aggressor_side == "SELL"), None)
    if buy_trade is None or sell_trade is None:
        raise SystemExit("candidate window did not include both BUY and SELL prints")
    orders = [
        V2TakerOrder(
            order_id="smoke-buy-1",
            market_id=buy_trade.market_id,
            asset_id=buy_trade.asset_id,
            side="BUY",
            limit_price=Decimal("1"),
            size=Decimal("10"),
            signal_block=buy_trade.block_number - 1,
            signal_ts=buy_trade.block_time - timedelta(seconds=2),
            latency=timedelta(seconds=1),
            horizon=timedelta(hours=1),
            horizon_blocks=20000,
            participation_rate=Decimal("0.025"),
        ),
        V2TakerOrder(
            order_id="smoke-sell-1",
            market_id=sell_trade.market_id,
            asset_id=sell_trade.asset_id,
            side="SELL",
            limit_price=Decimal("0"),
            size=Decimal("10"),
            signal_block=sell_trade.block_number - 1,
            signal_ts=sell_trade.block_time - timedelta(seconds=2),
            latency=timedelta(seconds=1),
            horizon=timedelta(hours=1),
            horizon_blocks=20000,
            participation_rate=Decimal("0.025"),
        ),
        V2TakerOrder(
            order_id="smoke-buy-no-fill",
            market_id=buy_trade.market_id,
            asset_id=buy_trade.asset_id,
            side="BUY",
            limit_price=Decimal("0.01"),
            size=Decimal("1"),
            signal_block=buy_trade.block_number - 1,
            signal_ts=buy_trade.block_time - timedelta(seconds=2),
            latency=timedelta(seconds=1),
            horizon=timedelta(hours=1),
            horizon_blocks=20000,
            participation_rate=Decimal("0.025"),
        ),
    ]
    execution_slice = load_v2_trade_slices_for_orders(
        orders,
        client=client,
        merge_gap_blocks=0,
        limit_per_window=5000,
    )
    execution_trades = list(execution_slice.trades)
    wallet_replay = _wallet_replay_sample(client, candidate, execution_trades)

    results, ledger, match_diagnostics = replay_v2_taker_orders_with_diagnostics(orders, execution_trades)
    scenario_report = build_v2_replay_report(orders, execution_trades)
    robustness_report = build_v2_robustness_report(
        orders,
        execution_trades,
        category_by_market={int(candidate["market_id"]): "sample_candidate"},
        walk_forward_splits=3,
    )
    observed_replay = build_v2_observed_fill_replay_report(execution_trades[:5])
    calibration_actual_rows = _load_calibration_rows(args)
    calibration_report = calibrate_v2_live_fills(
        results,
        calibration_actual_rows
        or [
            {
                "order_id": result.order_id,
                "filled_size": result.filled_size,
                "avg_price": result.avg_price,
            }
            for result in results
        ],
    )
    maker_orders = [
        V2MakerOrder(
            order_id="smoke-maker-sell-strict",
            market_id=buy_trade.market_id,
            asset_id=buy_trade.asset_id,
            side="SELL",
            limit_price=buy_trade.price,
            size=Decimal("1"),
            signal_block=buy_trade.block_number - 1,
            signal_ts=buy_trade.block_time - timedelta(seconds=2),
            latency=timedelta(seconds=1),
            horizon=timedelta(hours=1),
            horizon_blocks=20000,
        ),
        V2MakerOrder(
            order_id="smoke-maker-buy-phantom",
            market_id=sell_trade.market_id,
            asset_id=sell_trade.asset_id,
            side="BUY",
            limit_price=sell_trade.price,
            size=Decimal("1"),
            signal_block=sell_trade.block_number - 1,
            signal_ts=sell_trade.block_time - timedelta(seconds=2),
            latency=timedelta(seconds=1),
            horizon=timedelta(hours=1),
            horizon_blocks=20000,
        ),
    ]
    maker_sensitivity = [
        replay_v2_maker_order(maker_orders[0], execution_trades, "strict_audit").as_dict(),
        replay_v2_maker_order(maker_orders[1], execution_trades, "optimistic_sensitivity").as_dict(),
    ]
    has_fill = any(result.fills for result in results)
    has_unfilled_reason = any(result.reason_unfilled for result in results)
    report = {
        "status": "ready" if has_fill and has_unfilled_reason else "review",
        "model": "orderfilled_v2_trade_tape_taker_participation",
        "not_l2_depth_or_l3_queue": True,
        "candidate": candidate,
        "bootstrap_trade_print_rows_loaded": len(trades),
        "trade_print_rows_loaded": len(execution_trades),
        "trade_slice_load": execution_slice.as_dict(),
        "match_diagnostics": match_diagnostics.as_dict(),
        "summary": summarize_v2_results(results),
        "scenario_report": scenario_report,
        "robustness_report": robustness_report,
        "observed_replay": observed_replay,
        "wallet_replay": wallet_replay,
        "calibration_report": calibration_report,
        "maker_sensitivity": maker_sensitivity,
        "orders": [result.as_dict() for result in results],
        "capacity_ledger": ledger.as_dict(),
    }
    persisted_run_id = _persist_if_requested(args, orders, results, report)
    report["persistence"] = {
        "requested": bool(args.persist_postgres),
        "run_id": persisted_run_id,
        "market_slug": args.market_slug,
        "token_side": args.token_side,
    }
    json_path = OUT_DIR / "smoke_report.json"
    md_path = OUT_DIR / "smoke_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    md_path.write_text(_markdown(report), encoding="utf-8")
    print(json.dumps({"status": report["status"], "json": str(json_path), "markdown": str(md_path)}, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "ready" else 1


def _select_candidate(client: ClickHouseClient) -> dict[str, Any] | None:
    rows = client.query_json_rows(
        """
        SELECT market_id, condition_id, asset_id, outcome, count() AS rows,
               min(block_number) AS min_block, max(block_number) AS max_block,
               countIf(aggressor_side='BUY') AS buy_rows,
               countIf(aggressor_side='SELL') AS sell_rows
        FROM trade_prints_one_sided
        WHERE price > 0 AND price <= 1 AND size_shares > 0
        GROUP BY market_id, condition_id, asset_id, outcome
        HAVING buy_rows >= 5 AND sell_rows >= 1
        ORDER BY rows DESC
        LIMIT 1
        """,
        timeout_seconds=120,
    )
    return rows[0] if rows else None


def _load_calibration_rows(args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.calibration_run_id is None and not args.calibration_source:
        return []
    with postgres_connection(readonly=True) as conn:
        return load_v2_calibration_actual_rows(
            conn,
            run_id=args.calibration_run_id,
            source=args.calibration_source,
            limit=1000,
        )


def _persist_if_requested(
    args: argparse.Namespace,
    orders: list[V2TakerOrder],
    results,
    report: dict[str, Any],
) -> int | None:
    if not args.persist_postgres:
        return None
    with postgres_connection(readonly=False) as conn:
        if not args.skip_init_schema:
            create_schema(conn)
        return persist_v2_replay_run(
            conn,
            market_slug=args.market_slug,
            token_side=args.token_side,
            orders=orders,
            results=results,
            report=report,
        )


def _wallet_replay_sample(client: ClickHouseClient, candidate: dict[str, Any], trades) -> dict[str, Any]:
    asset_id = str(candidate["asset_id"]).lower()
    from_block = int(candidate["min_block"])
    to_block = from_block + 100000
    rows = client.query_json_rows(
        f"""
        SELECT wallet, count() AS rows
        FROM
        (
            SELECT maker AS wallet
            FROM maker_fill_ticks
            WHERE market_id = toUInt64({int(candidate["market_id"])})
              AND asset_id = '{asset_id}'
              AND block_number BETWEEN toUInt64({from_block}) AND toUInt64({to_block})
              AND length(maker) > 0
            UNION ALL
            SELECT taker AS wallet
            FROM maker_fill_ticks
            WHERE market_id = toUInt64({int(candidate["market_id"])})
              AND asset_id = '{asset_id}'
              AND block_number BETWEEN toUInt64({from_block}) AND toUInt64({to_block})
              AND length(taker) > 0
        )
        GROUP BY wallet
        HAVING rows >= 1
        ORDER BY rows DESC
        LIMIT 1
        """,
        timeout_seconds=120,
    )
    if not rows:
        return {"status": "missing", "note": "no maker_fill_ticks wallet sample for candidate"}
    wallet = str(rows[0].get("wallet") or "").lower()
    fills = load_v2_wallet_fill_ticks(
        wallet=wallet,
        client=client,
        market_id=int(candidate["market_id"]),
        asset_id=asset_id,
        from_block=from_block,
        to_block=to_block,
        limit=5,
    )
    if not fills:
        return {"status": "missing", "wallet": wallet, "wallet_fill_rows": 0, "note": "wallet selected but no fill rows loaded"}
    orders = [wallet_fill_to_observed_order(fill) for fill in fills]
    results, ledger = replay_v2_taker_orders(orders, trades)
    return {
        "status": "ready",
        "wallet": wallet,
        "wallet_fill_rows": len(fills),
        "summary": summarize_v2_results(results),
        "orders": [result.as_dict() for result in results],
        "capacity_ledger": ledger.as_dict(),
        "note": "wallet replay uses maker_fill_ticks-derived observed orders against trade_prints_one_sided",
    }


def _markdown(report: dict[str, Any]) -> str:
    summary = report["summary"]
    scenario = report["scenario_report"]
    orders = "\n".join(
        f"- {row['order_id']}: {row['status']}, filled={row['filled_size']}, reason={row['reason_unfilled'] or 'filled'}"
        for row in report["orders"]
    )
    modes = "\n".join(
        f"- {name}: filled={row['filled_orders']}, partial={row['partial_orders']}, unfilled={row['unfilled_orders']}, simulated_volume={row['simulated_volume']}, cash_delta={row['cash_delta']}"
        for name, row in scenario["mode_comparison"].items()
    )
    capacity_curve = "\n".join(
        f"- {row['value']}: volume={row['summary']['simulated_volume']}, utilization={row['summary']['participation_utilization']}"
        for row in scenario["capacity_curve"]
    )
    latency_curve = "\n".join(
        f"- {row['value']}: filled={row['summary']['filled_orders']}, avg_delay={row['summary']['avg_fill_delay_seconds']}"
        for row in scenario["latency_curve"]
    )
    horizon_curve = "\n".join(
        f"- {row['value']}: filled={row['summary']['filled_orders']}, volume={row['summary']['simulated_volume']}"
        for row in scenario["horizon_curve"]
    )
    makers = "\n".join(
        f"- {row['order_id']}: mode={row['maker_mode']}, status={row['status']}, filled={row['filled_size']}, queue_left={row['remaining_phantom_queue']}, reason={row['reason_unfilled'] or 'filled'}"
        for row in report["maker_sensitivity"]
    )
    robustness = report["robustness_report"]
    parameter_grid = "\n".join(
        f"- {row['index']}: simulated_volume={row['summary']['simulated_volume']}, cash_delta={row['summary']['cash_delta']}, parameters={row['parameters']}"
        for row in robustness["parameter_grid"]
    )
    walk_forward = "\n".join(
        f"- {row['index']}: blocks={row['from_block']}..{row['to_block']}, rows={row['trade_print_rows']}, volume={row['summary']['simulated_volume']}"
        for row in robustness["walk_forward"]
    )
    regime_split = "\n".join(
        f"- {row['regime']}: rows={row['trade_print_rows']}, volume={row['summary']['simulated_volume']}"
        for row in robustness["regime_split"]
    )
    overfit_warning = json.dumps(robustness["overfit_warning"], ensure_ascii=False, default=str)
    wallet = report["wallet_replay"]
    persistence = report.get("persistence", {})
    return f"""# OrderFilled V2 Smoke Report

Status: {report['status']}

Model: `orderfilled_v2_trade_tape_taker_participation`

This is execution-evidence constrained and is not L2 depth or L3 queue replay.

## Candidate

- market_id: {report['candidate']['market_id']}
- asset_id: {report['candidate']['asset_id']}
- bootstrap trade prints: {report.get('bootstrap_trade_print_rows_loaded', report['trade_print_rows_loaded'])}
- execution trade prints: {report['trade_print_rows_loaded']}
- trade slice load: {report.get('trade_slice_load', {})}
- match diagnostics: {report.get('match_diagnostics', {})}

## Summary

- attempted_orders: {summary['attempted_orders']}
- filled_orders: {summary['filled_orders']}
- partial_orders: {summary['partial_orders']}
- unfilled_orders: {summary['unfilled_orders']}
- simulated_volume: {summary['simulated_volume']}
- eligible_historical_volume: {summary['eligible_historical_volume']}
- participation_utilization: {summary['participation_utilization']}
- avg_fill_delay_seconds: {summary['avg_fill_delay_seconds']}
- avg_price_buffer: {summary['avg_price_buffer']}
- filled_notional: {summary['filled_notional']}
- cash_delta: {summary['cash_delta']}
- position_delta: {summary['position_delta']}
- unfilled_reason_distribution: {summary['unfilled_reason_distribution']}

## Mode Comparison

{modes}

## Capacity Curve

{capacity_curve}

## Latency Curve

{latency_curve}

## Horizon Curve

{horizon_curve}

## PnL Assumption

- status: {scenario['pnl_assumption']['status']}
- estimated_fees: {scenario['pnl_assumption']['estimated_fees']}
- cash_delta_after_fees: {scenario['pnl_assumption'].get('cash_delta_after_fees', 'n/a')}

## Maker Sensitivity

{makers}

## Observed Fill Replay

- execution_grade: {report['observed_replay']['execution_grade']}
- attempted_orders: {report['observed_replay']['summary']['attempted_orders']}
- filled_orders: {report['observed_replay']['summary']['filled_orders']}
- source_trades_consumed: {len(report['observed_replay']['capacity_ledger'])}

## Wallet Replay Sample

- status: {wallet['status']}
- wallet: {wallet.get('wallet', 'n/a')}
- wallet_fill_rows: {wallet.get('wallet_fill_rows', 0)}
- attempted_orders: {wallet.get('summary', {}).get('attempted_orders', 0)}
- filled_orders: {wallet.get('summary', {}).get('filled_orders', 0)}
- note: {wallet.get('note', '')}

## Paper/Live Calibration Skeleton

- status: {report['calibration_report']['status']}
- compared_orders: {report['calibration_report']['compared_orders']}
- false_positive_fills: {report['calibration_report']['false_positive_fills']}
- false_negative_fills: {report['calibration_report']['false_negative_fills']}
- avg_abs_size_error: {report['calibration_report']['avg_abs_size_error']}
- avg_abs_price_error: {report['calibration_report']['avg_abs_price_error']}

## Robustness

- status: {robustness['status']}
- overfit_warning: {overfit_warning}

Parameter grid:

{parameter_grid}

Walk-forward:

{walk_forward}

Regime split:

{regime_split}

## Persistence

- adapter: `persist_v2_replay_run`
- target_tables: `quant_backtest_runs`, `quant_backtest_parameters`, `quant_backtest_metrics`, `quant_backtest_orders`, `quant_backtest_ledger`, `quant_backtest_events`
- requested: {persistence.get('requested', False)}
- run_id: {persistence.get('run_id', 'n/a')}
- market_slug: {persistence.get('market_slug', 'n/a')}
- token_side: {persistence.get('token_side', 'n/a')}

## Orders

{orders}
"""


if __name__ == "__main__":
    raise SystemExit(main())
