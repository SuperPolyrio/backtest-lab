#!/usr/bin/env python3
"""Forward validation for the OrderFilled-only probability execution profile."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.orderfilled_v2_replay import (  # noqa: E402
    CapacityLedger,
    V2TakerOrder,
    replay_v2_taker_orders,
    summarize_v2_results,
    trade_print_from_row,
    with_v2_execution_profile,
)
from quant.core.db import ClickHouseClient  # noqa: E402


DEFAULT_PROFILE = PROJECT_ROOT / "config" / "execution" / "orderfilled_probability_profile.v1.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "runtime_outputs" / "fill_trade_validation" / "orderfilled_probability_forward_validation.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rows-per-market", type=int, default=3000)
    parser.add_argument("--orders-per-market", type=int, default=250)
    parser.add_argument("--sample-stride", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = json.loads(args.profile.read_text(encoding="utf-8"))
    markets = payload.get("trained_markets") or []
    if not markets:
        raise RuntimeError("profile has no trained_markets for forward validation")
    client = ClickHouseClient()
    rows: list[dict[str, Any]] = []
    all_probability_orders: list[V2TakerOrder] = []
    all_probability_conservative_orders: list[V2TakerOrder] = []
    all_source_confirmed_orders: list[V2TakerOrder] = []
    all_conservative_orders: list[V2TakerOrder] = []
    all_trades = []
    for market in markets:
        trades = _load_forward_rows(client, market, args.rows_per_market)
        orders = _build_orders(
            trades,
            training_last_block=int(market["training_last_block"]),
            limit=max(1, int(args.orders_per_market)),
            stride=max(1, int(args.sample_stride)),
        )
        probability_orders = [with_v2_execution_profile(order, "probabilistic_trade_tape") for order in orders]
        probability_conservative_orders = [
            with_v2_execution_profile(order, "probabilistic_conservative")
            for order in orders
        ]
        source_confirmed_orders = [
            with_v2_execution_profile(order, "probabilistic_source_confirmed")
            for order in orders
        ]
        conservative_orders = [with_v2_execution_profile(order, "conservative_trade_tape") for order in orders]
        probability_results, _ = replay_v2_taker_orders(probability_orders, trades, ledger=CapacityLedger())
        probability_conservative_results, _ = replay_v2_taker_orders(
            probability_conservative_orders,
            trades,
            ledger=CapacityLedger(),
        )
        source_confirmed_results, _ = replay_v2_taker_orders(
            source_confirmed_orders,
            trades,
            ledger=CapacityLedger(),
        )
        conservative_results, _ = replay_v2_taker_orders(conservative_orders, trades, ledger=CapacityLedger())
        invariant_errors = _source_invariant_errors(probability_orders, probability_results, trades)
        invariant_errors.extend(
            _source_invariant_errors(
                probability_conservative_orders,
                probability_conservative_results,
                trades,
            )
        )
        invariant_errors.extend(
            _source_invariant_errors(
                source_confirmed_orders,
                source_confirmed_results,
                trades,
            )
        )
        rows.append(
            {
                "market_id": int(market["market_id"]),
                "asset_id": str(market["asset_id"]),
                "training_last_block": int(market["training_last_block"]),
                "forward_trade_rows": len(trades),
                "orders": len(orders),
                "probabilistic": summarize_v2_results(probability_results),
                "probabilistic_conservative": summarize_v2_results(probability_conservative_results),
                "probabilistic_source_confirmed": summarize_v2_results(source_confirmed_results),
                "conservative": summarize_v2_results(conservative_results),
                "probabilistic_source_invariant_errors": invariant_errors,
            }
        )
        all_probability_orders.extend(probability_orders)
        all_probability_conservative_orders.extend(probability_conservative_orders)
        all_source_confirmed_orders.extend(source_confirmed_orders)
        all_conservative_orders.extend(conservative_orders)
        all_trades.extend(trades)
    probability_results, _ = replay_v2_taker_orders(all_probability_orders, all_trades, ledger=CapacityLedger())
    probability_conservative_results, _ = replay_v2_taker_orders(
        all_probability_conservative_orders,
        all_trades,
        ledger=CapacityLedger(),
    )
    source_confirmed_results, _ = replay_v2_taker_orders(
        all_source_confirmed_orders,
        all_trades,
        ledger=CapacityLedger(),
    )
    conservative_results, _ = replay_v2_taker_orders(all_conservative_orders, all_trades, ledger=CapacityLedger())
    errors = _source_invariant_errors(all_probability_orders, probability_results, all_trades)
    errors.extend(
        _source_invariant_errors(
            all_probability_conservative_orders,
            probability_conservative_results,
            all_trades,
        )
    )
    errors.extend(
        _source_invariant_errors(
            all_source_confirmed_orders,
            source_confirmed_results,
            all_trades,
        )
    )
    probability_summary = summarize_v2_results(probability_results)
    probability_conservative_summary = summarize_v2_results(probability_conservative_results)
    source_confirmed_summary = summarize_v2_results(source_confirmed_results)
    conservative_summary = summarize_v2_results(conservative_results)
    probability_any_fill_rate = _any_fill_rate(probability_summary)
    probability_conservative_any_fill_rate = _any_fill_rate(probability_conservative_summary)
    source_confirmed_any_fill_rate = _any_fill_rate(source_confirmed_summary)
    conservative_any_fill_rate = _any_fill_rate(conservative_summary)
    report = {
        "schema_version": "orderfilled_probability_forward_validation_v2",
        "status": "pass" if not errors else "fail",
        "data_source": "trade_prints_one_sided",
        "profile": str(args.profile),
        "markets": rows,
        "summary": {
            "probabilistic": probability_summary,
            "probabilistic_conservative": probability_conservative_summary,
            "probabilistic_source_confirmed": source_confirmed_summary,
            "conservative": conservative_summary,
            "probabilistic_any_fill_rate": str(probability_any_fill_rate),
            "probabilistic_conservative_any_fill_rate": str(probability_conservative_any_fill_rate),
            "probabilistic_source_confirmed_any_fill_rate": str(source_confirmed_any_fill_rate),
            "conservative_any_fill_rate": str(conservative_any_fill_rate),
            "any_fill_rate_delta": str(probability_any_fill_rate - conservative_any_fill_rate),
            "source_invariant_errors": errors,
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": report["status"],
                "orders": probability_summary["attempted_orders"],
                "probabilistic_any_fill_rate": str(probability_any_fill_rate),
                "probabilistic_conservative_any_fill_rate": str(probability_conservative_any_fill_rate),
                "probabilistic_source_confirmed_any_fill_rate": str(source_confirmed_any_fill_rate),
                "conservative_any_fill_rate": str(conservative_any_fill_rate),
                "any_fill_rate_delta": report["summary"]["any_fill_rate_delta"],
                "source_invariant_errors": len(errors),
                "output": str(args.output),
            },
            ensure_ascii=False,
        )
    )
    return 0 if report["status"] == "pass" else 1


def _load_forward_rows(client: ClickHouseClient, market: dict[str, Any], limit: int):
    asset = str(market["asset_id"]).replace("\\", "\\\\").replace("'", "\\'")
    last_block = int(market["training_last_block"])
    rows = client.query_json_rows(
        f"""
        SELECT
            trade_id, market_id, condition_id, asset_id, outcome, block_number,
            block_time, tx_hash, tx_index, tx_index_source, price, size_shares,
            notional_usdc, aggressor_side, passive_side, source_log_indexes,
            source_fill_count
        FROM trade_prints_one_sided
        PREWHERE market_id = {int(market["market_id"])}
          AND asset_id = '{asset}'
          AND block_number >= {max(0, last_block - 300)}
        WHERE price > 0 AND price <= 1 AND size_shares > 0
        ORDER BY block_number ASC, tx_index ASC, tx_hash ASC, arrayMin(source_log_indexes) ASC
        LIMIT {max(100, int(limit))}
        """,
        timeout_seconds=180,
    )
    return [trade_print_from_row(row) for row in rows]


def _build_orders(trades, *, training_last_block: int, limit: int, stride: int) -> list[V2TakerOrder]:
    orders: list[V2TakerOrder] = []
    for index, anchor in enumerate(trades):
        if anchor.block_number <= training_last_block or index % stride:
            continue
        side = anchor.aggressor_side
        limit_price = anchor.price + Decimal("0.01") if side == "BUY" else anchor.price - Decimal("0.01")
        limit_price = max(Decimal("0.001"), min(Decimal("0.999"), limit_price))
        orders.append(
            V2TakerOrder(
                order_id=f"forward-{anchor.market_id}-{anchor.trade_id}",
                market_id=anchor.market_id,
                asset_id=anchor.asset_id,
                side=side,
                limit_price=limit_price,
                size=max(Decimal("0.1"), anchor.size * Decimal("0.01")),
                signal_block=anchor.block_number,
                signal_ts=anchor.block_time,
                latency_blocks=1,
                latency=timedelta(microseconds=1),
                horizon_blocks=100,
                horizon=timedelta(seconds=30),
                participation_rate=Decimal("0.025"),
                price_buffer=Decimal("0.005"),
                tif="GTC",
            )
        )
        if len(orders) >= limit:
            break
    return orders


def _source_invariant_errors(orders, results, trades) -> list[dict[str, Any]]:
    trade_by_id = {trade.trade_id: trade for trade in trades}
    order_by_id = {order.order_id: order for order in orders}
    errors: list[dict[str, Any]] = []
    for result in results:
        order = order_by_id[result.order_id]
        for fill in result.fills:
            source = trade_by_id.get(fill.source_trade_id)
            reasons: list[str] = []
            if source is None:
                reasons.append("missing_source_trade")
            else:
                if source.aggressor_side != order.side:
                    reasons.append("wrong_source_side")
                if fill.fill_block < int(order.arrival_block or 0):
                    reasons.append("pre_arrival_source")
                if order.side == "BUY" and fill.exec_price > order.limit_price:
                    reasons.append("buy_price_above_limit")
                if order.side == "SELL" and fill.exec_price < order.limit_price:
                    reasons.append("sell_price_below_limit")
            if reasons:
                errors.append({"order_id": result.order_id, "trade_id": fill.source_trade_id, "reasons": reasons})
    return errors


def _any_fill_rate(summary: dict[str, Any]) -> Decimal:
    attempted = Decimal(int(summary.get("attempted_orders") or 0))
    if attempted <= 0:
        return Decimal("0")
    filled = Decimal(int(summary.get("filled_orders") or 0) + int(summary.get("partial_orders") or 0))
    return filled / attempted


if __name__ == "__main__":
    raise SystemExit(main())
