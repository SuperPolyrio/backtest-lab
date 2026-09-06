#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.orderfilled_probability import future_same_side_fill_label  # noqa: E402
from quant.backtest.orderfilled_v2_replay import V2TakerOrder  # noqa: E402
from quant.backtest.trade_only_v3.hierarchical_prior import (  # noqa: E402
    HierarchicalContext,
    infer_market_taxonomy,
    liquidity_regime,
    price_bucket,
    tte_bucket,
)
from quant.core.db import ClickHouseClient  # noqa: E402
from scripts.calibrate_orderfilled_probability import (  # noqa: E402
    _load_market_trades,
    _resolve_candidates,
)

LEVELS = (
    ("category",),
    ("category", "side"),
    ("category", "league", "side"),
    ("category", "league", "price_bucket", "side"),
    ("category", "league", "price_bucket", "tte_bucket", "side"),
    (
        "category",
        "league",
        "price_bucket",
        "tte_bucket",
        "liquidity_regime",
        "side",
    ),
    (
        "market_id",
        "category",
        "league",
        "price_bucket",
        "tte_bucket",
        "liquidity_regime",
        "side",
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-id", action="append", type=int, default=[])
    parser.add_argument("--auto-markets", type=int, default=18)
    parser.add_argument("--rows-per-market", type=int, default=5000)
    parser.add_argument("--sample-stride", type=int, default=5)
    parser.add_argument("--lookback-seconds", type=int, default=300)
    parser.add_argument("--lookback-blocks", type=int, default=300)
    parser.add_argument("--horizons", default="1,5,30,120,300")
    parser.add_argument("--prior-strength", type=Decimal, default=Decimal("30"))
    parser.add_argument("--min-cell-samples", type=int, default=20)
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT
        / "config/execution/trade_only_hierarchical_priors.v1.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    horizons = tuple(
        sorted({int(value) for value in args.horizons.split(",") if value.strip()})
    )
    client = ClickHouseClient()
    candidates = _resolve_candidates(client, args)
    metadata = _load_metadata(client, [int(row["market_id"]) for row in candidates])
    samples: dict[int, list[dict[str, Any]]] = {value: [] for value in horizons}
    market_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        market_id = int(candidate["market_id"])
        trades = _load_market_trades(client, candidate, args.rows_per_market)
        meta = metadata.get(market_id, {})
        category, league = infer_market_taxonomy(
            meta.get("category"), meta.get("slug"), meta.get("title")
        )
        built = _build_samples(
            trades,
            market_id=market_id,
            category=category,
            league=league,
            end_time=_datetime(meta.get("end_time")),
            horizons=horizons,
            lookback_seconds=args.lookback_seconds,
            lookback_blocks=args.lookback_blocks,
            stride=args.sample_stride,
        )
        for horizon, rows in built.items():
            samples[horizon].extend(rows)
        market_rows.append(
            {
                "market_id": market_id,
                "category": category,
                "league": league,
                "trade_rows": len(trades),
                "end_time": meta.get("end_time"),
            }
        )
    payload = {
        "schema_version": "trade_only_hierarchical_prior_v2",
        "data_source": "trade_prints_one_sided+market_metadata",
        "label": "future_same_side_limit_eligible_orderfilled",
        "prior_strength": str(args.prior_strength),
        "min_cell_samples": args.min_cell_samples,
        "levels": [list(fields) for fields in LEVELS],
        "markets": market_rows,
        "horizons": {
            str(horizon): _fit_horizon(
                rows,
                prior_strength=args.prior_strength,
                min_samples=args.min_cell_samples,
            )
            for horizon, rows in samples.items()
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "status": "pass",
                "output": str(args.output),
                "horizon_samples": {
                    str(key): len(value) for key, value in samples.items()
                },
                "markets": len(market_rows),
            }
        )
    )
    return 0


def _build_samples(
    trades: list[Any],
    *,
    market_id: int,
    category: str,
    league: str,
    end_time: datetime | None,
    horizons: tuple[int, ...],
    lookback_seconds: int,
    lookback_blocks: int,
    stride: int,
) -> dict[int, list[dict[str, Any]]]:
    output: dict[int, list[dict[str, Any]]] = {value: [] for value in horizons}
    if len(trades) < 20:
        return output
    for index in range(5, len(trades) - 1, max(1, stride)):
        anchor = trades[index]
        start_time = anchor.block_time - timedelta(seconds=lookback_seconds)
        pre = [
            row
            for row in trades[: index + 1]
            if row.block_time >= start_time
            and row.block_number >= anchor.block_number - lookback_blocks
        ]
        max_deadline = anchor.block_time + timedelta(seconds=max(horizons))
        future_window = []
        for row in trades[index + 1 :]:
            if row.block_time > max_deadline:
                break
            future_window.append(row)
        for side in ("BUY", "SELL"):
            limit = (
                anchor.price + Decimal("0.01")
                if side == "BUY"
                else anchor.price - Decimal("0.01")
            )
            limit = max(Decimal("0.001"), min(Decimal("0.999"), limit))
            size = max(Decimal("0.1"), anchor.size * Decimal("0.025"))
            context = HierarchicalContext(
                market_id=str(market_id),
                category=category,
                league=league,
                price_bucket=price_bucket(limit),
                tte_bucket=tte_bucket(anchor.block_time, end_time),
                liquidity_regime=liquidity_regime(pre),
                side=side.lower(),
            )
            for horizon in horizons:
                order = V2TakerOrder(
                    order_id=f"hier-{anchor.trade_id}-{side}-{horizon}",
                    market_id=market_id,
                    asset_id=anchor.asset_id,
                    side=side,
                    limit_price=limit,
                    size=size,
                    signal_block=anchor.block_number,
                    signal_ts=anchor.block_time,
                    latency_blocks=1,
                    latency=timedelta(microseconds=1),
                    horizon_blocks=max(100, horizon * 3),
                    horizon=timedelta(seconds=horizon),
                    participation_rate=Decimal("0.025"),
                    price_buffer=Decimal("0.005"),
                )
                deadline = anchor.block_time + timedelta(seconds=horizon)
                future = [row for row in future_window if row.block_time <= deadline]
                label, volume = future_same_side_fill_label(
                    order, future, price_buffer=Decimal("0.005")
                )
                fraction = (
                    min(Decimal(1), volume * Decimal("0.025") / size)
                    if label
                    else Decimal(0)
                )
                output[horizon].append(
                    {
                        "context": context,
                        "label": int(label),
                        "conditional_fraction": fraction,
                    }
                )
    return output


def _fit_horizon(
    samples: list[dict[str, Any]],
    *,
    prior_strength: Decimal,
    min_samples: int,
) -> dict[str, Any]:
    total = len(samples)
    positive = sum(int(row["label"]) for row in samples)
    fraction_sum = sum(
        (Decimal(str(row["conditional_fraction"])) for row in samples if row["label"]),
        Decimal(0),
    )
    global_p = Decimal(positive) / Decimal(max(1, total))
    global_capacity = fraction_sum / Decimal(max(1, positive))
    global_cell = _cell(
        total, positive, fraction_sum, global_p, global_capacity, Decimal(0)
    )
    fitted_levels: list[dict[str, Any]] = []
    parent_cells: dict[str, dict[str, Any]] = {"global": global_cell}
    parent_fields: tuple[str, ...] = ()
    for fields in LEVELS:
        grouped: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"samples": 0, "positive": 0, "fraction_sum": Decimal(0)}
        )
        for row in samples:
            context = row["context"]
            key = _key(fields, context)
            grouped[key]["samples"] += 1
            grouped[key]["positive"] += int(row["label"])
            if row["label"]:
                grouped[key]["fraction_sum"] += Decimal(
                    str(row["conditional_fraction"])
                )
        cells: dict[str, Any] = {}
        for key, stats in grouped.items():
            if int(stats["samples"]) < min_samples:
                continue
            sample_context = next(
                row["context"] for row in samples if _key(fields, row["context"]) == key
            )
            parent_key = (
                _key(parent_fields, sample_context) if parent_fields else "global"
            )
            parent = parent_cells.get(parent_key, global_cell)
            cells[key] = _cell(
                int(stats["samples"]),
                int(stats["positive"]),
                Decimal(stats["fraction_sum"]),
                Decimal(str(parent["p_fill"])),
                Decimal(str(parent["conditional_fill_fraction"])),
                prior_strength,
            )
        fitted_levels.append({"fields": list(fields), "cells": cells})
        parent_cells = cells
        parent_fields = fields
    return {"global": global_cell, "levels": fitted_levels, "samples": total}


def _cell(
    samples: int,
    positive: int,
    fraction_sum: Decimal,
    parent_p: Decimal,
    parent_capacity: Decimal,
    strength: Decimal,
) -> dict[str, Any]:
    p_fill = (Decimal(positive) + strength * parent_p) / (
        Decimal(max(1, samples)) + strength
    )
    capacity = (fraction_sum + strength * parent_capacity) / (
        Decimal(max(1, positive)) + strength
    )
    return {
        "samples": samples,
        "positive_samples": positive,
        "p_fill": str(p_fill.quantize(Decimal("0.0000000001"))),
        "conditional_fill_fraction": str(
            max(Decimal(0), min(Decimal(1), capacity)).quantize(Decimal("0.0000000001"))
        ),
    }


def _key(fields: tuple[str, ...], context: HierarchicalContext) -> str:
    return "|".join(context.value(field) for field in fields)


def _load_metadata(
    client: ClickHouseClient, market_ids: list[int]
) -> dict[int, dict[str, Any]]:
    if not market_ids:
        return {}
    joined = ",".join(str(value) for value in sorted(set(market_ids)))
    rows = client.query_json_rows(
        f"""
        SELECT market_id, argMax(slug, updated_at) slug, argMax(title, updated_at) title,
               argMax(category, updated_at) category, argMax(end_time, updated_at) end_time
        FROM pnl_market_condition_metadata_full
        WHERE market_id IN ({joined})
        GROUP BY market_id
        """,
        timeout_seconds=120,
    )
    return {int(row["market_id"]): dict(row) for row in rows}


def _datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed


if __name__ == "__main__":
    raise SystemExit(main())
