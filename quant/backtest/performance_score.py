"""Risk-adjusted performance scoring for fill-first backtest research."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"


def build_performance_score_report(
    *,
    trades: Sequence[Mapping[str, Any]] | None = None,
    ledger: Sequence[Mapping[str, Any]] | None = None,
    fill_quality_report: Mapping[str, Any] | None = None,
    data_quality_report: Mapping[str, Any] | None = None,
    prediction_quality_report: Mapping[str, Any] | None = None,
    min_closed_trades: int = 3,
) -> dict[str, Any]:
    """Build a prediction-market-specific performance/ranking report.

    This intentionally does not try to replace fill quality or prediction quality.
    It combines them into a conservative research ranking score so strategy lists
    are not sorted by raw PnL alone.
    """

    trade_rows = [_as_mapping(row) for row in trades or []]
    ledger_rows = [_as_mapping(row) for row in ledger or []]
    fill_quality = dict(fill_quality_report or {})
    data_quality = dict(data_quality_report or {})
    prediction_quality = dict(prediction_quality_report or {})

    pnl_values = _trade_pnls(trade_rows)
    equity_curve = _equity_curve(ledger_rows, pnl_values)
    closed_trade_count = len(pnl_values)
    net_pnl = sum(pnl_values, Decimal("0"))
    max_drawdown = _max_drawdown(equity_curve)
    sharpe = _sharpe(pnl_values)
    sortino = _sortino(pnl_values)
    calmar = _ratio(net_pnl, max_drawdown)
    rolling_returns = _rolling_returns(pnl_values)

    fill_rate_pct = _rate_pct(_first_present(fill_quality, "fill_rate", "liquidity_fill_rate"))
    no_fill_rate_pct = _rate_pct(_first_present(fill_quality, "no_fill_rate"))
    if no_fill_rate_pct is None and fill_rate_pct is not None:
        no_fill_rate_pct = max(Decimal("0"), Decimal("100") - fill_rate_pct)
    coverage_pct = _rate_pct(_first_present(data_quality, "span_coverage_pct", "coverage_pct"))
    if coverage_pct is None:
        coverage_pct = Decimal("0")
    low_fill_penalty = _penalty(max(Decimal("0"), Decimal("100") - (fill_rate_pct or Decimal("0"))))
    coverage_penalty = _penalty(max(Decimal("0"), Decimal("100") - coverage_pct))
    drawdown_penalty = max_drawdown
    brier_advantage = _decimal(_first_present(prediction_quality, "brier_advantage", "prediction_brier_advantage"))
    brier_component = brier_advantage * Decimal("100")
    performance_score = net_pnl - drawdown_penalty - coverage_penalty - low_fill_penalty + brier_component

    review_reasons: list[str] = []
    if closed_trade_count < int(min_closed_trades):
        review_reasons.append(f"only {closed_trade_count} closed trades; need at least {min_closed_trades}")
    if data_quality and data_quality.get("quality_verdict") not in {None, READY}:
        review_reasons.append(f"data quality verdict={data_quality.get('quality_verdict')}")
    if coverage_pct < Decimal("100"):
        review_reasons.append(f"coverage below 100%: {coverage_pct}%")
    if fill_rate_pct is not None and fill_rate_pct < Decimal("50"):
        review_reasons.append(f"low fill rate: {fill_rate_pct}%")
    if prediction_quality and prediction_quality.get("prediction_verdict") == REVIEW:
        review_reasons.append("prediction quality requires review")

    status = MISSING if not trade_rows and not ledger_rows else READY
    ranking_verdict = READY if status == READY and not review_reasons else REVIEW if status == READY else MISSING
    return {
        "schema_version": "fill_first_performance_score_v1",
        "status": status,
        "ranking_verdict": ranking_verdict,
        "reason": "performance score is usable for research ranking" if ranking_verdict == READY else "; ".join(review_reasons) or "no closed trade or ledger evidence",
        "closed_trade_count": closed_trade_count,
        "net_pnl": _decimal_text(net_pnl),
        "max_drawdown": _decimal_text(max_drawdown),
        "sharpe": _decimal_text(sharpe),
        "sortino": _decimal_text(sortino),
        "calmar": _decimal_text(calmar),
        "rolling_return": rolling_returns,
        "fill_rate_pct": _decimal_text(fill_rate_pct or Decimal("0")),
        "no_fill_rate_pct": _decimal_text(no_fill_rate_pct or Decimal("0")),
        "coverage_pct": _decimal_text(coverage_pct),
        "brier_advantage": _decimal_text(brier_advantage),
        "penalties": {
            "drawdown_penalty": _decimal_text(drawdown_penalty),
            "coverage_penalty": _decimal_text(coverage_penalty),
            "low_fill_penalty": _decimal_text(low_fill_penalty),
        },
        "components": {
            "pnl": _decimal_text(net_pnl),
            "brier_component": _decimal_text(brier_component),
            "penalty_total": _decimal_text(drawdown_penalty + coverage_penalty + low_fill_penalty),
        },
        "performance_score": _decimal_text(performance_score),
        "score_formula": "net_pnl - max_drawdown - coverage_penalty - low_fill_penalty + 100*brier_advantage",
        "next_actions": _next_actions(ranking_verdict, review_reasons),
    }


def _trade_pnls(rows: Sequence[Mapping[str, Any]]) -> list[Decimal]:
    values = [_decimal(_first_present(row, "pnl", "net_pnl", "realized_pnl", "total_pnl")) for row in rows]
    return [value for value in values if value != Decimal("0") or rows]


def _equity_curve(ledger_rows: Sequence[Mapping[str, Any]], pnl_values: Sequence[Decimal]) -> list[Decimal]:
    points: list[tuple[Decimal, Decimal]] = []
    for index, row in enumerate(ledger_rows, start=1):
        value = _first_present(row, "portfolio_equity", "equity", "cash_after")
        if value is None:
            continue
        points.append((_decimal(_first_present(row, "x_value", "sequence", "ledger_id") or index), _decimal(value)))
    if points:
        ordered = [value for _, value in sorted(points, key=lambda item: item[0])]
        first = ordered[0]
        return [value - first for value in ordered]
    cumulative: list[Decimal] = []
    total = Decimal("0")
    for pnl in pnl_values:
        total += pnl
        cumulative.append(total)
    return cumulative


def _max_drawdown(equity: Sequence[Decimal]) -> Decimal:
    if not equity:
        return Decimal("0")
    peak = equity[0]
    drawdown = Decimal("0")
    for value in equity:
        peak = max(peak, value)
        drawdown = max(drawdown, peak - value)
    return drawdown


def _sharpe(values: Sequence[Decimal]) -> Decimal:
    if len(values) < 2:
        return Decimal("0")
    avg = sum(values, Decimal("0")) / Decimal(len(values))
    std = _stddev(values, avg, downside_only=False)
    return _ratio(avg, std)


def _sortino(values: Sequence[Decimal]) -> Decimal:
    if len(values) < 2:
        return Decimal("0")
    avg = sum(values, Decimal("0")) / Decimal(len(values))
    downside = _stddev(values, Decimal("0"), downside_only=True)
    return _ratio(avg, downside)


def _stddev(values: Sequence[Decimal], center: Decimal, *, downside_only: bool) -> Decimal:
    selected = [value for value in values if not downside_only or value < center]
    if not selected:
        return Decimal("0")
    variance = sum((value - center) ** 2 for value in selected) / Decimal(len(selected))
    return _sqrt(variance)


def _sqrt(value: Decimal) -> Decimal:
    if value <= 0:
        return Decimal("0")
    return Decimal(str(float(value) ** 0.5))


def _rolling_returns(values: Sequence[Decimal], *, window: int = 3) -> dict[str, Any]:
    if not values:
        return {"window": window, "count": 0, "best": "0", "worst": "0", "latest": "0"}
    if len(values) < window:
        total = sum(values, Decimal("0"))
        return {"window": len(values), "count": 1, "best": _decimal_text(total), "worst": _decimal_text(total), "latest": _decimal_text(total)}
    returns = [sum(values[index : index + window], Decimal("0")) for index in range(0, len(values) - window + 1)]
    return {
        "window": window,
        "count": len(returns),
        "best": _decimal_text(max(returns)),
        "worst": _decimal_text(min(returns)),
        "latest": _decimal_text(returns[-1]),
    }


def _penalty(value: Decimal) -> Decimal:
    return max(Decimal("0"), value) / Decimal("100")


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def _rate_pct(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    number = _decimal(value)
    if Decimal("0") <= number <= Decimal("1"):
        return number * Decimal("100")
    return number


def _first_present(source: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in source and source.get(key) not in (None, ""):
            return source.get(key)
    return None


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "__dict__"):
        return {str(key): inner for key, inner in vars(value).items()}
    return {}


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    normalized = value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP).normalize()
    return format(normalized, "f")


def _next_actions(verdict: str, review_reasons: Sequence[str]) -> list[str]:
    if verdict == READY:
        return ["Use performance_score for ranking, but keep fill quality and prediction quality visible next to PnL."]
    if verdict == MISSING:
        return ["Run a fill-first backtest with closed trades or ledger cashflows before ranking strategies."]
    return [f"Review performance score input: {reason}" for reason in review_reasons[:5]]
