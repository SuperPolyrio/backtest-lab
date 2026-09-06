#!/usr/bin/env python3
"""Compare fixed Fill-only V3 TIF-aware profiles on frozen real cohorts."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.orderfilled_v2_replay import V2TradePrint  # noqa: E402
from quant.backtest.trade_only_v3 import (  # noqa: E402
    LiquidityIntent,
    TradeOnlyOrder,
    replay_trade_only_orders,
)

PROFILES = (
    "taker_source_confirmed",
    "central_trade_only_tif_aware_5s",
    "central_trade_only_tif_aware_5s_recall",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    loaded = [(path, json.loads(path.read_text(encoding="utf-8"))) for path in args.cohort]
    titles = {
        int(raw["market_id"]): str(raw["title"])
        for _, raw in loaded
        if raw.get("title")
    }
    for _, raw in loaded:
        raw["title"] = raw.get("title") or titles.get(int(raw["market_id"]), "")
    cohorts = [_run_cohort(path, raw) for path, raw in loaded]
    payload = {
        "schema_version": "fill-only-v3-tif-aware-validation-v1",
        "aggregate": _aggregate(cohorts),
        "cohorts": cohorts,
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


def _run_cohort(path: Path, raw: dict[str, Any]) -> dict[str, Any]:
    trades = _trade_tape(raw)
    orders = [_order(raw, row, index) for index, row in enumerate(raw["orders"])]
    results = {
        profile: replay_trade_only_orders(orders, trades, profile)[0]
        for profile in PROFILES
    }
    source_positive = {
        row.order_id
        for row in results["taker_source_confirmed"]
        if row.filled_size > 0
    }
    central_modeled = {
        row.order_id
        for row in results["central_trade_only_tif_aware_5s"]
        if row.filled_size > 0 and row.status == "MODELED_EXPECTATION"
    }
    support_5s = _independent_source_support(orders, trades, seconds=5)
    support_30s = _independent_source_support(orders, trades, seconds=30)
    return {
        "cohort": str(path),
        "market_id": raw["market_id"],
        "asset_id": raw["asset_id"],
        "title": raw.get("title", ""),
        "orders": len(orders),
        "trade_rows": len(trades),
        "profiles": {
            profile: _summary(rows) for profile, rows in results.items()
        },
        "central_increment": {
            "source_positive_orders": len(source_positive),
            "modeled_positive_orders": len(central_modeled),
            "modeled_with_independent_source_support_5s": len(
                central_modeled & support_5s
            ),
            "modeled_with_independent_source_support_30s": len(
                central_modeled & support_30s
            ),
            "support_5s_precision": _ratio(
                len(central_modeled & support_5s), len(central_modeled)
            ),
            "support_30s_precision": _ratio(
                len(central_modeled & support_30s), len(central_modeled)
            ),
            "retrospective_support_is_not_observed_execution": True,
        },
    }


def _trade_tape(raw: dict[str, Any]) -> list[V2TradePrint]:
    by_id: dict[str, dict[str, Any]] = {}
    for order in raw["orders"]:
        for row in order.get("tape", []):
            by_id.setdefault(str(row["trade_id"]), row)
    rows = sorted(
        by_id.values(),
        key=lambda row: (
            int(row["block_number"]),
            datetime.fromisoformat(str(row["ts"])),
            str(row["trade_id"]),
        ),
    )
    output = []
    for index, row in enumerate(rows):
        side = str(row["aggressor_side"]).upper()
        price = Decimal(str(row["price"]))
        size = Decimal(str(row["size"]))
        trade_id = str(row["trade_id"])
        output.append(
            V2TradePrint(
                trade_id=trade_id,
                market_id=int(raw["market_id"]),
                condition_id=f"cohort:{raw['market_id']}",
                asset_id=str(raw["asset_id"]),
                outcome="UNKNOWN",
                block_number=int(row["block_number"]),
                block_time=datetime.fromisoformat(str(row["ts"])),
                tx_hash=f"cohort:{trade_id}",
                tx_index=index,
                tx_index_source="frozen_cohort",
                price=price,
                size=size,
                notional=price * size,
                aggressor_side=side,  # type: ignore[arg-type]
                passive_side="SELL" if side == "BUY" else "BUY",
                source_log_indexes=(index,),
                source_fill_count=1,
            )
        )
    return output


def _order(raw: dict[str, Any], row: dict[str, Any], index: int) -> TradeOnlyOrder:
    return TradeOnlyOrder(
        order_id=f"{Path(str(raw.get('title') or raw['market_id'])).stem}:{row['order_id']}",
        market_id=int(raw["market_id"]),
        asset_id=str(raw["asset_id"]),
        side=str(row.get("side", "BUY")).upper(),  # type: ignore[arg-type]
        limit_price=Decimal(str(row["limit_price"])),
        size=Decimal(str(row["size"])),
        signal_block=int(row["signal_block"]),
        signal_ts=datetime.fromisoformat(str(row["signal_ts"])),
        tif="FAK",
        liquidity_intent=LiquidityIntent.TAKER,
        latency=timedelta(seconds=int(raw.get("latency_seconds", 1))),
        latency_blocks=1,
        horizon=timedelta(seconds=5),
        horizon_blocks=15,
        lookback=timedelta(minutes=5),
        lookback_blocks=300,
        signal_source_trade_id=str(row["signal_trade_id"]),
        random_seed=73 + index,
        market_title=str(raw.get("title") or ""),
    )


def _independent_source_support(
    orders: list[TradeOnlyOrder], trades: list[V2TradePrint], *, seconds: int
) -> set[str]:
    supported = set()
    for order in orders:
        probe = replace(
            order,
            tif="GTD",
            horizon=timedelta(seconds=seconds),
            horizon_blocks=max(15, seconds * 3),
        )
        result = replay_trade_only_orders(
            [probe], trades, "taker_source_confirmed"
        )[0][0]
        if result.filled_size > 0:
            supported.add(order.order_id)
    return supported


def _summary(rows: list[Any]) -> dict[str, Any]:
    positive = [row for row in rows if row.filled_size > 0]
    return {
        "orders": len(rows),
        "positive_orders": len(positive),
        "observed_positive_orders": sum(
            bool(row.fills and row.fills[0].source_trade_ids) for row in positive
        ),
        "modeled_positive_orders": sum(
            row.status == "MODELED_EXPECTATION" for row in positive
        ),
        "total_observed_or_expected_size": str(
            sum((row.filled_size for row in rows), Decimal(0))
        ),
        "statuses": dict(Counter(str(row.status) for row in rows)),
    }


def _aggregate(cohorts: list[dict[str, Any]]) -> dict[str, Any]:
    profiles: dict[str, dict[str, Any]] = {}
    for profile in PROFILES:
        rows = [cohort["profiles"][profile] for cohort in cohorts]
        profiles[profile] = {
            "orders": sum(int(row["orders"]) for row in rows),
            "positive_orders": sum(int(row["positive_orders"]) for row in rows),
            "observed_positive_orders": sum(
                int(row["observed_positive_orders"]) for row in rows
            ),
            "modeled_positive_orders": sum(
                int(row["modeled_positive_orders"]) for row in rows
            ),
            "total_observed_or_expected_size": str(
                sum(
                    (Decimal(str(row["total_observed_or_expected_size"])) for row in rows),
                    Decimal(0),
                )
            ),
        }
    modeled = sum(
        int(cohort["central_increment"]["modeled_positive_orders"])
        for cohort in cohorts
    )
    support_5s = sum(
        int(cohort["central_increment"]["modeled_with_independent_source_support_5s"])
        for cohort in cohorts
    )
    support_30s = sum(
        int(cohort["central_increment"]["modeled_with_independent_source_support_30s"])
        for cohort in cohorts
    )
    return {
        "cohorts": len(cohorts),
        "markets": len({int(cohort["market_id"]) for cohort in cohorts}),
        "orders": sum(int(cohort["orders"]) for cohort in cohorts),
        "profiles": profiles,
        "central_increment": {
            "modeled_positive_orders": modeled,
            "modeled_with_independent_source_support_5s": support_5s,
            "modeled_with_independent_source_support_30s": support_30s,
            "support_5s_precision": _ratio(support_5s, modeled),
            "support_30s_precision": _ratio(support_30s, modeled),
            "retrospective_support_is_not_observed_execution": True,
        },
    }


def _ratio(numerator: int, denominator: int) -> str | None:
    if denominator == 0:
        return None
    return str((Decimal(numerator) / Decimal(denominator)).quantize(Decimal("0.0001")))


if __name__ == "__main__":
    raise SystemExit(main())
