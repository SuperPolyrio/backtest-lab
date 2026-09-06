#!/usr/bin/env python3
"""Validate PML2 static-FAK book walking against an independent control."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = (
    ROOT
    / "backtest_framework"
    / "nautilus_trader_comparison"
    / "pml2_order_contract_v2_cross_validation"
)
TOLERANCE = Decimal("0.000001")


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _fill_class(row: Mapping[str, Any], requested: Decimal) -> str:
    filled = _decimal(row.get("filled_size"))
    if filled <= TOLERANCE:
        return "NO_FILL"
    if filled + TOLERANCE < requested:
        return "PARTIAL"
    return "FILLED"


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def validate(args: argparse.Namespace) -> dict[str, Any]:
    summaries = sorted(args.input_root.glob("window_*/summary.json"))
    counts: Counter[str] = Counter()
    categories: Counter[str] = Counter()
    depth_regimes: Counter[str] = Counter()
    activity_regimes: Counter[str] = Counter()
    spread_regimes: Counter[str] = Counter()
    dates: set[str] = set()
    markets: set[str] = set()
    market_days: set[tuple[str, str]] = set()
    price_min: Decimal | None = None
    price_max: Decimal | None = None
    pml2_quantity = Decimal(0)
    control_quantity = Decimal(0)
    quantity_error = Decimal(0)
    price_error = Decimal(0)
    mismatches: list[dict[str, Any]] = []
    included_windows: list[str] = []

    for summary_path in summaries:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        orders_path = summary_path.with_name("orders.jsonl")
        if not orders_path.exists():
            continue
        cohort = summary.get("cohort", {})
        start = datetime.fromisoformat(str(cohort["start"]))
        date_value = start.date().isoformat()
        dates.add(date_value)
        included_windows.append(str(summary_path.parent))
        with orders_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                row = json.loads(line)
                models = row.get("models", {})
                pml2 = models.get("pml2_fak")
                control = models.get("nautilus_fak")
                if not isinstance(pml2, Mapping) or not isinstance(control, Mapping):
                    raise ValueError(
                        f"missing paired models in {orders_path}:{line_number}"
                    )
                requested = _decimal(row["size"])
                pml2_filled = _decimal(pml2.get("filled_size"))
                control_filled = _decimal(control.get("filled_size"))
                pml2_class = _fill_class(pml2, requested)
                control_class = _fill_class(control, requested)
                pml2_price = (
                    None if pml2.get("avg_price") is None else _decimal(pml2["avg_price"])
                )
                control_price = (
                    None
                    if control.get("avg_price") is None
                    else _decimal(control["avg_price"])
                )
                row_quantity_error = abs(pml2_filled - control_filled)
                row_price_error = (
                    Decimal(0)
                    if pml2_price is None and control_price is None
                    else (
                        Decimal("Infinity")
                        if pml2_price is None or control_price is None
                        else abs(pml2_price - control_price)
                    )
                )

                counts["orders"] += 1
                counts[f"pml2_{pml2_class.lower()}"] += 1
                counts[f"control_{control_class.lower()}"] += 1
                pml2_quantity += pml2_filled
                control_quantity += control_filled
                quantity_error += row_quantity_error
                price_error += row_price_error
                if pml2_class != control_class:
                    counts["status_mismatches"] += 1
                if row_quantity_error > TOLERANCE:
                    counts["quantity_mismatches"] += 1
                if row_price_error > TOLERANCE:
                    counts["price_mismatches"] += 1
                if (
                    pml2_class != control_class
                    or row_quantity_error > TOLERANCE
                    or row_price_error > TOLERANCE
                ) and len(mismatches) < args.maximum_mismatch_examples:
                    mismatches.append(
                        {
                            "window": str(summary_path.parent.name),
                            "order_id": row.get("order_id"),
                            "market_id": row.get("market_id"),
                            "pml2_class": pml2_class,
                            "control_class": control_class,
                            "pml2_filled_size": str(pml2_filled),
                            "control_filled_size": str(control_filled),
                            "pml2_avg_price": (
                                None if pml2_price is None else str(pml2_price)
                            ),
                            "control_avg_price": (
                                None if control_price is None else str(control_price)
                            ),
                        }
                    )

                market_id = str(row["market_id"])
                markets.add(market_id)
                market_days.add((market_id, date_value))
                categories[str(row.get("category") or "unknown")] += 1
                depth_regimes[str(row.get("depth_regime") or "unknown")] += 1
                activity_regimes[str(row.get("activity_regime") or "unknown")] += 1
                spread_regimes[str(row.get("spread_regime") or "unknown")] += 1
                limit_price = _decimal(row["limit_price"])
                price_min = limit_price if price_min is None else min(price_min, limit_price)
                price_max = limit_price if price_max is None else max(price_max, limit_price)

    gates = {
        "orders_exceed_minimum": counts["orders"] > args.minimum_orders,
        "minimum_windows": len(included_windows) >= args.minimum_windows,
        "minimum_dates": len(dates) >= args.minimum_dates,
        "minimum_markets": len(markets) >= args.minimum_markets,
        "minimum_categories": len(categories) >= args.minimum_categories,
        "depth_regime_coverage": {
            "SHALLOW_LT_1X",
            "MEDIUM_1X_TO_10X",
            "DEEP_GE_10X",
        }.issubset(depth_regimes),
        "activity_regime_coverage": {
            "SPARSE_LE_5",
            "MEDIUM_6_TO_50",
            "ACTIVE_GT_50",
        }.issubset(activity_regimes),
        "spread_regime_coverage": {
            "TIGHT_LE_0_002",
            "NORMAL_0_002_TO_0_02",
            "WIDE_GT_0_02",
        }.issubset(spread_regimes),
        "zero_status_mismatches": counts["status_mismatches"] == 0,
        "zero_quantity_mismatches": counts["quantity_mismatches"] == 0,
        "zero_price_mismatches": counts["price_mismatches"] == 0,
    }
    result = {
        "schema_version": "pml2_order_contract_cross_validation_v1",
        "status": "PASS" if all(gates.values()) else "FAIL",
        "comparison_scope": (
            "STATIC_ARRIVAL_L2_FAK_BOOK_WALK; independent orders; "
            "dynamic queue and shared-strategy capacity are tested separately"
        ),
        "sample": {
            "orders": counts["orders"],
            "windows": len(included_windows),
            "dates": sorted(dates),
            "unique_markets": len(markets),
            "market_days": len(market_days),
            "category_counts": dict(sorted(categories.items())),
            "price_min": None if price_min is None else str(price_min),
            "price_max": None if price_max is None else str(price_max),
            "depth_regimes": dict(sorted(depth_regimes.items())),
            "activity_regimes": dict(sorted(activity_regimes.items())),
            "spread_regimes": dict(sorted(spread_regimes.items())),
        },
        "pml2": {
            "positive_orders": counts["pml2_filled"] + counts["pml2_partial"],
            "filled_orders": counts["pml2_filled"],
            "partial_orders": counts["pml2_partial"],
            "no_fill_orders": counts["pml2_no_fill"],
            "quantity": str(pml2_quantity),
        },
        "nautilus_control": {
            "positive_orders": (
                counts["control_filled"] + counts["control_partial"]
            ),
            "filled_orders": counts["control_filled"],
            "partial_orders": counts["control_partial"],
            "no_fill_orders": counts["control_no_fill"],
            "quantity": str(control_quantity),
        },
        "differences": {
            "status_mismatches": counts["status_mismatches"],
            "quantity_mismatches": counts["quantity_mismatches"],
            "price_mismatches": counts["price_mismatches"],
            "absolute_quantity_error_sum": str(quantity_error),
            "absolute_price_error_sum": str(price_error),
            "examples": mismatches,
        },
        "gates": gates,
        "included_windows": included_windows,
    }
    _write_json(args.output, result)
    return result


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_INPUT / "result.json")
    parser.add_argument("--minimum-orders", type=int, default=10_000)
    parser.add_argument("--minimum-windows", type=int, default=10)
    parser.add_argument("--minimum-dates", type=int, default=3)
    parser.add_argument("--minimum-markets", type=int, default=100)
    parser.add_argument("--minimum-categories", type=int, default=5)
    parser.add_argument("--maximum-mismatch-examples", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = _args()
    result = validate(args)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
