#!/usr/bin/env python3
"""Expand a large Fill-only V3 validation result into per-order explanations."""

from __future__ import annotations

import argparse
import csv
import html
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULT = (
    PROJECT_ROOT
    / "backtest_framework"
    / "nautilus_trader_comparison"
    / "fill_only_v3_large_cross_validation_independent_v8"
    / "result.json"
)
ZERO = Decimal(0)
ONE = Decimal(1)


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or 0))


def _expected_probability(model: Mapping[str, Any]) -> Decimal:
    bounds = model.get("probability_bounds")
    if isinstance(bounds, Mapping):
        for key in (
            "any_fill_execution_horizon",
            "model_target_horizon",
            "horizon",
        ):
            if bounds.get(key) is not None:
                return min(ONE, max(ZERO, _decimal(bounds[key])))
    return ONE if _decimal(model.get("filled_size")) > 0 else ZERO


def _optional_decimal(value: Any) -> str:
    return "" if value is None else str(value)


def _first_fill(model: Mapping[str, Any]) -> Mapping[str, Any]:
    fills = model.get("fills") or []
    return fills[0] if fills else {}


def _reference_label(model: Mapping[str, Any]) -> Decimal:
    return ONE if _decimal(model.get("filled_size")) > 0 else ZERO


def _classification(probability: Decimal, label: Decimal) -> str:
    if label == ONE and probability >= Decimal("0.5"):
        return "REFERENCE_FILL_PROBABILITY_GE_50PCT"
    if label == ONE:
        return "REFERENCE_FILL_PROBABILITY_LT_50PCT"
    if probability >= Decimal("0.5"):
        return "REFERENCE_NO_FILL_PROBABILITY_GE_50PCT"
    return "REFERENCE_NO_FILL_PROBABILITY_LT_50PCT"


def _explanation_text(row: Mapping[str, Any]) -> str:
    probability = _decimal(row["v3_probability"])
    conditional = _decimal(row["v3_conditional_fill_size"])
    expected = _decimal(row["v3_expected_size"])
    pml2_quantity = _decimal(row["pml2_filled_size"])
    nautilus_quantity = _decimal(row["nautilus_filled_size"])
    return (
        f"订单在 {row['decision_ts']} 产生，1 秒后到达；买入 {row['outcome']} "
        f"{row['order_size']} 股，限价 {row['limit_price']}。到达前 15 分钟有 "
        f"{row['trailing_trade_count']} 笔 OrderFilled、成交量 {row['trailing_volume']}。"
        f"V3 估计至少成交一点的概率为 {probability:.6%}，条件成交量为 "
        f"{conditional} 股，所以该笔期望成交量为 {expected} 股。"
        f"PML2 给出的标签是 {int(_decimal(row['pml2_label']))}、成交 "
        f"{pml2_quantity} 股；该笔对 PML2 订单等价量分子贡献 {probability}，"
        f"分母贡献 {row['pml2_label']}，数量差 {row['pml2_quantity_delta']}，"
        f"Brier 贡献 {row['pml2_brier_contribution']}。"
        f"Nautilus 给出的标签是 {int(_decimal(row['nautilus_label']))}、成交 "
        f"{nautilus_quantity} 股；该笔对 Nautilus 订单等价量分子同样贡献 "
        f"{probability}，分母贡献 {row['nautilus_label']}，数量差 "
        f"{row['nautilus_quantity_delta']}，Brier 贡献 "
        f"{row['nautilus_brier_contribution']}。"
    )


def explain_order(raw: Mapping[str, Any], *, window: str) -> dict[str, str]:
    models = raw["models"]
    expected_model = models["v3_l2_expected"]
    source_model = models["v3_source_fak"]
    pml2 = models["pml2_fak"]
    nautilus = models["nautilus_fak"]
    probability = _expected_probability(expected_model)
    expected_size = _decimal(expected_model.get("filled_size"))
    first_fill = _first_fill(expected_model)
    conditional_size = _decimal(
        first_fill.get("conditional_fill_size")
        or expected_model.get("capacity_bounds", {}).get("conditional_expected")
        or (expected_size / probability if probability > 0 else 0)
    )
    pml2_label = _reference_label(pml2)
    nautilus_label = _reference_label(nautilus)
    pml2_size = _decimal(pml2.get("filled_size"))
    nautilus_size = _decimal(nautilus.get("filled_size"))
    signal = raw.get("signal_trade") or {}
    diagnostics = expected_model.get("model_diagnostics") or {}
    result = {
        "window": window,
        "market_id": str(raw["market_id"]),
        "condition_id": str(raw.get("condition_id") or ""),
        "asset_id": str(raw.get("asset_id") or ""),
        "title": str(raw.get("title") or ""),
        "slug": str(raw.get("slug") or ""),
        "category": str(raw.get("category") or "unknown"),
        "outcome": str(raw.get("outcome") or ""),
        "order_id": str(raw["order_id"]),
        "decision_ts": str(raw["decision_ts"]),
        "arrival_ts": str(raw["arrival_ts"]),
        "side": str(raw.get("side") or ""),
        "order_size": str(raw.get("size") or 0),
        "limit_price": str(raw.get("limit_price") or 0),
        "signal_trade_id": str(signal.get("trade_id") or ""),
        "signal_trade_ts": str(signal.get("block_time") or ""),
        "signal_trade_side": str(signal.get("aggressor_side") or ""),
        "signal_trade_price": str(signal.get("price") or ""),
        "signal_trade_size": str(signal.get("size") or ""),
        "trailing_trade_count": str(raw.get("trailing_trade_count") or 0),
        "trailing_volume": str(raw.get("trailing_volume") or 0),
        "best_bid": _optional_decimal(raw.get("best_bid")),
        "best_ask": _optional_decimal(raw.get("best_ask")),
        "spread": _optional_decimal(raw.get("spread")),
        "raw_visible_ask_depth": str(raw.get("raw_visible_ask_depth") or 0),
        "book_age_ms": str(raw.get("book_age_ms") or 0),
        "book_evidence_status": str(raw.get("book_evidence_status") or ""),
        "depth_regime": str(raw.get("depth_regime") or "unknown"),
        "activity_regime": str(raw.get("activity_regime") or "unknown"),
        "spread_regime": str(raw.get("spread_regime") or "unknown"),
        "source_status": str(source_model.get("status") or ""),
        "source_reason": str(source_model.get("reason") or ""),
        "source_filled_size": str(source_model.get("filled_size") or 0),
        "v3_status": str(expected_model.get("status") or ""),
        "v3_reason": str(expected_model.get("reason") or ""),
        "v3_route": str(diagnostics.get("selected_route") or "source_confirmed"),
        "v3_probability": str(probability),
        "v3_conditional_fill_size": str(conditional_size),
        "v3_expected_size": str(expected_size),
        "v3_avg_price": _optional_decimal(expected_model.get("avg_price")),
        "v3_probability_cell": str(
            diagnostics.get("hierarchical_probability_cell") or ""
        ),
        "v3_probability_cell_samples": str(
            diagnostics.get("hierarchical_probability_samples") or 0
        ),
        "pml2_status": str(pml2.get("status") or ""),
        "pml2_reason": str(pml2.get("reason") or ""),
        "pml2_label": str(pml2_label),
        "pml2_filled_size": str(pml2_size),
        "pml2_avg_price": _optional_decimal(pml2.get("avg_price")),
        "pml2_order_equivalent_delta": str(probability - pml2_label),
        "pml2_quantity_delta": str(expected_size - pml2_size),
        "pml2_brier_contribution": str((probability - pml2_label) ** 2),
        "pml2_classification": _classification(probability, pml2_label),
        "nautilus_status": str(nautilus.get("status") or ""),
        "nautilus_reason": str(nautilus.get("reason") or ""),
        "nautilus_label": str(nautilus_label),
        "nautilus_filled_size": str(nautilus_size),
        "nautilus_avg_price": _optional_decimal(nautilus.get("avg_price")),
        "nautilus_order_equivalent_delta": str(probability - nautilus_label),
        "nautilus_quantity_delta": str(expected_size - nautilus_size),
        "nautilus_brier_contribution": str((probability - nautilus_label) ** 2),
        "nautilus_classification": _classification(probability, nautilus_label),
    }
    result["explanation_zh"] = _explanation_text(result)
    return result


@dataclass
class Totals:
    orders: int = 0
    windows: set[str] = field(default_factory=set)
    outcomes: set[str] = field(default_factory=set)
    assets: set[str] = field(default_factory=set)
    expected_orders: Decimal = ZERO
    expected_quantity: Decimal = ZERO
    pml2_orders: Decimal = ZERO
    pml2_quantity: Decimal = ZERO
    pml2_brier: Decimal = ZERO
    nautilus_orders: Decimal = ZERO
    nautilus_quantity: Decimal = ZERO
    nautilus_brier: Decimal = ZERO

    def add(self, row: Mapping[str, str]) -> None:
        self.orders += 1
        self.windows.add(row["window"])
        self.outcomes.add(row["outcome"])
        self.assets.add(row["asset_id"])
        self.expected_orders += _decimal(row["v3_probability"])
        self.expected_quantity += _decimal(row["v3_expected_size"])
        self.pml2_orders += _decimal(row["pml2_label"])
        self.pml2_quantity += _decimal(row["pml2_filled_size"])
        self.pml2_brier += _decimal(row["pml2_brier_contribution"])
        self.nautilus_orders += _decimal(row["nautilus_label"])
        self.nautilus_quantity += _decimal(row["nautilus_filled_size"])
        self.nautilus_brier += _decimal(row["nautilus_brier_contribution"])

    def as_dict(self) -> dict[str, str | int]:
        return {
            "orders": self.orders,
            "windows": len(self.windows),
            "outcomes": ",".join(sorted(self.outcomes)),
            "assets": len(self.assets),
            "expected_order_equivalents": str(self.expected_orders),
            "expected_quantity": str(self.expected_quantity),
            "pml2_positive_orders": str(self.pml2_orders),
            "pml2_quantity": str(self.pml2_quantity),
            "pml2_order_ratio": _ratio(self.expected_orders, self.pml2_orders),
            "pml2_quantity_ratio": _ratio(
                self.expected_quantity, self.pml2_quantity
            ),
            "pml2_brier": str(self.pml2_brier / self.orders),
            "nautilus_positive_orders": str(self.nautilus_orders),
            "nautilus_quantity": str(self.nautilus_quantity),
            "nautilus_order_ratio": _ratio(
                self.expected_orders, self.nautilus_orders
            ),
            "nautilus_quantity_ratio": _ratio(
                self.expected_quantity, self.nautilus_quantity
            ),
            "nautilus_brier": str(self.nautilus_brier / self.orders),
        }


def _ratio(numerator: Decimal, denominator: Decimal) -> str:
    return "" if denominator == 0 else str(numerator / denominator)


def summarize(rows: Iterable[Mapping[str, str]]) -> Totals:
    totals = Totals()
    for row in rows:
        totals.add(row)
    return totals


def _load_rows(result_path: Path) -> tuple[dict[str, Any], list[dict[str, str]]]:
    result = json.loads(result_path.read_text(encoding="utf-8"))
    rows: list[dict[str, str]] = []
    for summary_name in result["successful_summaries"]:
        summary_path = Path(summary_name)
        if not summary_path.is_absolute():
            summary_path = PROJECT_ROOT / summary_path
        orders_path = summary_path.with_name("orders.jsonl")
        window = summary_path.parent.name.removeprefix("window_")
        with orders_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if line.strip():
                    row = explain_order(json.loads(line), window=window)
                    row["source_orders_file"] = str(
                        orders_path.relative_to(PROJECT_ROOT)
                    )
                    row["source_line"] = str(line_number)
                    rows.append(row)
    rows.sort(key=lambda row: (row["market_id"], row["decision_ts"], row["order_id"]))
    return result, rows


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"cannot write empty CSV: {path}")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _market_rows(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["market_id"]].append(row)
    result = []
    for market_id, values in sorted(grouped.items(), key=lambda item: int(item[0])):
        summary = summarize(values).as_dict()
        for reference in ("pml2", "nautilus"):
            summary[f"{reference}_order_ratio_gate"] = _ratio_gate(
                str(summary[f"{reference}_order_ratio"])
            )
            summary[f"{reference}_quantity_ratio_gate"] = _ratio_gate(
                str(summary[f"{reference}_quantity_ratio"])
            )
        result.append(
            {
                "market_id": market_id,
                "title": values[0]["title"],
                "categories": ",".join(sorted({row["category"] for row in values})),
                **summary,
            }
        )
    return result


def _market_asset_rows(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row["market_id"], row["asset_id"], row["outcome"])].append(row)
    result = []
    for (market_id, asset_id, outcome), values in sorted(
        grouped.items(), key=lambda item: (int(item[0][0]), item[0][2], item[0][1])
    ):
        result.append(
            {
                "market_id": market_id,
                "asset_id": asset_id,
                "outcome": outcome,
                "title": values[0]["title"],
                "category": values[0]["category"],
                **summarize(values).as_dict(),
            }
        )
    return result


def _assert_close(name: str, actual: Decimal, expected: Decimal) -> None:
    if abs(actual - expected) > Decimal("1e-12"):
        raise AssertionError(f"{name}: actual={actual} expected={expected}")


def _ratio_gate(value: str) -> str:
    if not value:
        return "NO_REFERENCE_POSITIVE_ORDERS"
    ratio = Decimal(value)
    if Decimal("0.85") <= ratio <= Decimal("1.15"):
        return "PASS_85_TO_115_PERCENT"
    return "OUTSIDE_85_TO_115_PERCENT"


def _diagnostics(
    rows: list[dict[str, str]], markets: list[dict[str, Any]]
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for reference in ("pml2", "nautilus"):
        matrix = Counter(row[f"{reference}_classification"] for row in rows)
        positive = sum(
            (_decimal(row[f"{reference}_label"]) for row in rows), ZERO
        )
        positive_rate = positive / len(rows)
        climatology_brier = positive_rate * (ONE - positive_rate)
        actual_brier = sum(
            (
                (
                    _decimal(row["v3_probability"])
                    - _decimal(row[f"{reference}_label"])
                )
                ** 2
                for row in rows
            ),
            ZERO,
        ) / len(rows)
        ratio_values = [
            str(market[f"{reference}_order_ratio"]) for market in markets
        ]
        result[reference] = {
            "threshold_50pct_confusion": dict(sorted(matrix.items())),
            "reference_positive_rate": str(positive_rate),
            "climatology_brier": str(climatology_brier),
            "model_brier": str(actual_brier),
            "brier_skill_vs_constant_reference_rate": str(
                ONE - actual_brier / climatology_brier
            ),
            "markets": len(markets),
            "markets_with_reference_positive_orders": sum(
                bool(value) for value in ratio_values
            ),
            "markets_without_reference_positive_orders": sum(
                not value for value in ratio_values
            ),
            "markets_inside_85_to_115_percent": sum(
                _ratio_gate(value) == "PASS_85_TO_115_PERCENT"
                for value in ratio_values
            ),
            "markets_outside_85_to_115_percent": sum(
                _ratio_gate(value) == "OUTSIDE_85_TO_115_PERCENT"
                for value in ratio_values
            ),
        }
    overlap = Counter(
        f"PML2_{row['pml2_label']}_NAUTILUS_{row['nautilus_label']}" for row in rows
    )
    result["reference_overlap"] = dict(sorted(overlap.items()))
    result["v3_positive_expected_size_orders"] = sum(
        _decimal(row["v3_expected_size"]) > 0 for row in rows
    )
    result["source_confirmed_positive_orders"] = sum(
        _decimal(row["source_filled_size"]) > 0 for row in rows
    )
    return result


def _reconcile(result: Mapping[str, Any], totals: Totals) -> dict[str, Any]:
    aggregate = result["quality"]["aggregate"]
    breadth_models = result["breadth"]["models"]
    checks = {
        "orders": (Decimal(totals.orders), _decimal(result["successful_orders"])),
        "v3_expected_quantity": (
            totals.expected_quantity,
            _decimal(breadth_models["v3_l2_expected"]["quantity"]),
        ),
        "pml2_positive_orders": (
            totals.pml2_orders,
            _decimal(breadth_models["pml2_fak"]["positive_quantity_orders"]),
        ),
        "pml2_quantity": (
            totals.pml2_quantity,
            _decimal(breadth_models["pml2_fak"]["quantity"]),
        ),
        "pml2_order_ratio": (
            totals.expected_orders / totals.pml2_orders,
            _decimal(aggregate["pml2_fak"]["expected_positive_order_ratio"]),
        ),
        "pml2_quantity_ratio": (
            totals.expected_quantity / totals.pml2_quantity,
            _decimal(aggregate["pml2_fak"]["expected_quantity_ratio"]),
        ),
        "pml2_brier": (
            totals.pml2_brier / totals.orders,
            _decimal(aggregate["pml2_fak"]["probability_brier_score"]),
        ),
        "nautilus_positive_orders": (
            totals.nautilus_orders,
            _decimal(breadth_models["nautilus_fak"]["positive_quantity_orders"]),
        ),
        "nautilus_quantity": (
            totals.nautilus_quantity,
            _decimal(breadth_models["nautilus_fak"]["quantity"]),
        ),
        "nautilus_order_ratio": (
            totals.expected_orders / totals.nautilus_orders,
            _decimal(aggregate["nautilus_fak"]["expected_positive_order_ratio"]),
        ),
        "nautilus_quantity_ratio": (
            totals.expected_quantity / totals.nautilus_quantity,
            _decimal(aggregate["nautilus_fak"]["expected_quantity_ratio"]),
        ),
        "nautilus_brier": (
            totals.nautilus_brier / totals.orders,
            _decimal(aggregate["nautilus_fak"]["probability_brier_score"]),
        ),
    }
    for name, (actual, expected) in checks.items():
        _assert_close(name, actual, expected)
    return {
        "status": "PASS",
        "formula": {
            "order_equivalents": "sum(v3_probability)",
            "order_ratio": "sum(v3_probability) / reference_positive_orders",
            "quantity_ratio": "sum(v3_expected_size) / sum(reference_filled_size)",
            "brier": "mean((v3_probability - reference_label)^2)",
        },
        "totals": totals.as_dict(),
        "checks": {
            name: {"actual": str(actual), "expected": str(expected), "match": True}
            for name, (actual, expected) in checks.items()
        },
    }


def _html_page(title: str, body: str, *, prefix: str = "") -> str:
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)}</title><link rel="stylesheet" href="{prefix}style.css"></head>
<body><main><header><a href="{prefix}index.html">Fill-only V3 逐单解释</a><h1>{html.escape(title)}</h1></header>{body}</main></body></html>"""


def _summary_table(values: Mapping[str, Any]) -> str:
    labels = {
        "orders": "订单数",
        "windows": "窗口数",
        "outcomes": "Outcome",
        "expected_order_equivalents": "V3 订单等价量",
        "expected_quantity": "V3 期望量",
        "pml2_positive_orders": "PML2 正成交订单",
        "pml2_quantity": "PML2 成交量",
        "pml2_order_ratio": "对 PML2 订单比",
        "pml2_quantity_ratio": "对 PML2 数量比",
        "pml2_brier": "对 PML2 Brier",
        "nautilus_positive_orders": "Nautilus 正成交订单",
        "nautilus_quantity": "Nautilus 成交量",
        "nautilus_order_ratio": "对 Nautilus 订单比",
        "nautilus_quantity_ratio": "对 Nautilus 数量比",
        "nautilus_brier": "对 Nautilus Brier",
        "pml2_order_ratio_gate": "PML2 market 订单比门",
        "pml2_quantity_ratio_gate": "PML2 market 数量比门",
        "nautilus_order_ratio_gate": "Nautilus market 订单比门",
        "nautilus_quantity_ratio_gate": "Nautilus market 数量比门",
    }
    cells = "".join(
        f"<tr><th>{html.escape(label)}</th><td>{html.escape(str(values[key]))}</td></tr>"
        for key, label in labels.items()
        if key in values
    )
    return f'<table class="summary">{cells}</table>'


def _write_html(
    output: Path,
    result: Mapping[str, Any],
    rows: list[dict[str, str]],
    markets: list[dict[str, Any]],
    reconciliation: Mapping[str, Any],
) -> None:
    markets_dir = output / "markets"
    markets_dir.mkdir(exist_ok=True)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["market_id"]].append(row)

    market_lines = []
    for market in markets:
        market_id = str(market["market_id"])
        market_lines.append(
            "<tr>"
            f'<td><a href="markets/{market_id}.html">{market_id}</a></td>'
            f"<td>{html.escape(str(market['title']))}</td>"
            f"<td>{market['orders']}</td>"
            f"<td>{market['expected_order_equivalents']}</td>"
            f"<td>{market['pml2_positive_orders']}</td>"
            f"<td>{market['nautilus_positive_orders']}</td>"
            f"<td>{market['pml2_brier']}</td>"
            f"<td>{market['nautilus_brier']}</td></tr>"
        )
        order_sections = []
        for row in grouped[market_id]:
            detail_rows = "".join(
                f"<tr><th>{html.escape(key)}</th><td>{html.escape(str(value))}</td></tr>"
                for key, value in row.items()
                if key != "explanation_zh"
            )
            order_sections.append(
                f'<details class="order"><summary>{html.escape(row["order_id"])} | '
                f'{html.escape(row["decision_ts"])} | {html.escape(row["outcome"])}</summary>'
                f'<p class="explanation">{html.escape(row["explanation_zh"])}</p>'
                f'<table class="detail">{detail_rows}</table></details>'
            )
        market_body = (
            f"<p>{html.escape(str(market['title']))}</p>"
            + _summary_table(market)
            + "".join(order_sections)
        )
        (markets_dir / f"{market_id}.html").write_text(
            _html_page(f"Market {market_id}", market_body, prefix="../"),
            encoding="utf-8",
        )

    totals = reconciliation["totals"]
    diagnostics = reconciliation["diagnostics"]
    failed = int(result.get("failed_or_unavailable_windows") or 0)
    pml2 = diagnostics["pml2"]
    nautilus = diagnostics["nautilus"]
    body = (
        f"<p>共 {totals['orders']} 笔订单、{len(markets)} 个 market；另有 {failed} 个"
        "数据不足窗口未生成订单，未进入下表。</p>"
        "<h2>先读懂两个百分比</h2>"
        "<p><strong>订单等价量比不是逐单命中率。</strong>分子是每笔 V3 成交概率之和，"
        "分母是参考模型实际产生正成交量的订单数。成交量比则是 V3 期望成交量之和除以"
        "参考模型成交量之和。不同订单上的高估和低估可以在总和中互相抵消。</p>"
        "<pre>订单等价量比 = sum(P_V3(any fill)) / count(reference size &gt; 0)\n"
        "成交量比 = sum(V3 expected size) / sum(reference filled size)\n"
        "Brier = mean((P_V3(any fill) - reference label)^2)</pre>"
        "<h2>逐单与逐 market 诊断</h2>"
        f"<p>PML2：Brier {pml2['model_brier']}，相对恒定成交率基线的 skill 仅 "
        f"{pml2['brier_skill_vs_constant_reference_rate']}；有参考成交的 "
        f"{pml2['markets_with_reference_positive_orders']} 个 market 中，只有 "
        f"{pml2['markets_inside_85_to_115_percent']} 个 market 的订单比位于 85%–115%。"
        f"另有 {pml2['markets_without_reference_positive_orders']} 个 market 没有 PML2 正成交。</p>"
        f"<p>Nautilus：Brier {nautilus['model_brier']}，skill "
        f"{nautilus['brier_skill_vs_constant_reference_rate']}；有参考成交的 "
        f"{nautilus['markets_with_reference_positive_orders']} 个 market 中，只有 "
        f"{nautilus['markets_inside_85_to_115_percent']} 个位于 85%–115%。"
        f"另有 {nautilus['markets_without_reference_positive_orders']} 个 market 没有正成交。</p>"
        + _summary_table(totals)
        + '<p><a href="orders.csv">逐单 CSV</a> · <a href="orders.jsonl">逐单 JSONL</a> · '
        '<a href="markets.csv">逐 market CSV</a> · '
        '<a href="market_assets.csv">逐 market/token CSV</a> · '
        '<a href="reconciliation.json">总额对账</a></p>'
        + "<table><thead><tr><th>Market</th><th>标题</th><th>订单</th>"
        "<th>V3 等价量</th><th>PML2</th><th>Nautilus</th><th>PML2 Brier</th>"
        "<th>Nautilus Brier</th></tr></thead><tbody>"
        + "".join(market_lines)
        + "</tbody></table>"
    )
    (output / "index.html").write_text(
        _html_page("11,000 笔订单逐单解释", body), encoding="utf-8"
    )
    (output / "style.css").write_text(
        """body{margin:0;background:#f7f7f5;color:#20211f;font:14px/1.5 system-ui,sans-serif}main{max-width:1500px;margin:auto;padding:24px}header a{color:#1264a3;text-decoration:none}h1{font-size:28px}table{width:100%;border-collapse:collapse;background:#fff;margin:16px 0}th,td{padding:8px 10px;border:1px solid #d8d9d5;text-align:left;vertical-align:top}.summary{max-width:760px}.summary th{width:260px}.order{background:#fff;border:1px solid #d8d9d5;margin:10px 0}.order summary{cursor:pointer;font-weight:650;padding:12px}.explanation{padding:0 14px 12px}.detail{margin:0;border-left:0;border-right:0;border-bottom:0}.detail th{width:280px;background:#f3f4f1}a{color:#1264a3}@media(max-width:800px){main{padding:12px}table{display:block;overflow:auto}.detail th{width:180px}}""",
        encoding="utf-8",
    )


def run(result_path: Path, output: Path) -> dict[str, Any]:
    result, rows = _load_rows(result_path)
    totals = summarize(rows)
    reconciliation = _reconcile(result, totals)
    markets = _market_rows(rows)
    market_assets = _market_asset_rows(rows)
    reconciliation["diagnostics"] = _diagnostics(rows, markets)
    output.mkdir(parents=True, exist_ok=True)
    _write_csv(output / "orders.csv", rows)
    _write_jsonl(output / "orders.jsonl", rows)
    _write_csv(output / "markets.csv", markets)
    _write_csv(output / "market_assets.csv", market_assets)
    (output / "reconciliation.json").write_text(
        json.dumps(reconciliation, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    _write_html(output, result, rows, markets, reconciliation)
    return {
        "status": "PASS",
        "output": str(output),
        "orders": len(rows),
        "markets": len(markets),
        "market_assets": len(market_assets),
        "totals": reconciliation["totals"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--result", type=Path, default=DEFAULT_RESULT)
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result_path = args.result.resolve()
    output = (
        args.output.resolve()
        if args.output is not None
        else result_path.parent / "order_level_explanations"
    )
    print(json.dumps(run(result_path, output), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
