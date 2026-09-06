"""Cashflow validation for fill-first backtest ledgers."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from .polymarket_cashflow import normalize_polymarket_activity_event, replay_polymarket_cashflows


READY = "ready"
REVIEW = "review"
MISSING = "missing"

Q = Decimal("0.0000000001")
DEFAULT_TOLERANCE = Decimal("0.0000001")
POLYMARKET_SPECIAL_EVENTS = {"SPLIT", "MERGE", "REDEEM", "REBATE", "MAKER_REBATE", "REFUND"}


def build_ledger_cashflow_validation_report(
    trades: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
    parameters: Mapping[str, Any] | None = None,
    *,
    tolerance: Decimal = DEFAULT_TOLERANCE,
) -> dict[str, Any]:
    """Validate that net PnL can be reconstructed from cashflow ledger rows."""

    trade_rows = [dict(row) for row in trades]
    ledger_rows = [dict(row) for row in ledger]
    initial_capital = _decimal((parameters or {}).get("initial_capital"))
    trade_pnl_total = sum((_decimal(row.get("pnl")) for row in trade_rows), Decimal("0"))
    ledger_realized_pnl = sum((_decimal(row.get("realized_pnl")) for row in ledger_rows), Decimal("0"))
    cash_delta_total = sum((_decimal(row.get("cash_delta")) for row in ledger_rows), Decimal("0"))
    reconstructed_cash = initial_capital + cash_delta_total
    reported_cash_after = _last_cash_after(ledger_rows, initial_capital)
    cash_after_diff = (reconstructed_cash - reported_cash_after).copy_abs()
    ledger_diff = (ledger_realized_pnl - trade_pnl_total)
    ledger_trade_ids = {str(row.get("trade_id")) for row in ledger_rows if not _blank(row.get("trade_id"))}
    trade_ids = [str(row.get("trade_id")) for row in trade_rows if not _blank(row.get("trade_id"))]
    missing_trade_ids = [trade_id for trade_id in trade_ids if trade_id not in ledger_trade_ids]
    missing_cash_delta = sum(1 for row in ledger_rows if _blank(row.get("cash_delta")))
    missing_realized_pnl = sum(1 for row in ledger_rows if _blank(row.get("realized_pnl")))
    residual_position = _decimal(ledger_rows[-1].get("position_after")) if ledger_rows else Decimal("0")
    event_counts: dict[str, int] = {}
    for row in ledger_rows:
        event_type = str(row.get("event_type") or "UNKNOWN").upper()
        event_counts[event_type] = event_counts.get(event_type, 0) + 1
    mark_prices = _mark_prices_for_cashflow(ledger_rows, parameters or {})
    polymarket_cashflow = replay_polymarket_cashflows(
        ledger_rows,
        initial_capital=Decimal("0"),
        mark_prices=mark_prices,
    )
    polymarket_summary = polymarket_cashflow["summary"]
    polymarket_residual_positions = [_serialize_residual_position(row) for row in polymarket_cashflow["residual_positions"][:20]]
    special_event_counts = {
        event_type: count
        for event_type, count in sorted(event_counts.items())
        if event_type in POLYMARKET_SPECIAL_EVENTS
    }

    reasons: list[str] = []
    verdict = READY
    if trade_rows and not ledger_rows:
        verdict = MISSING
        reasons.append("closed trades exist but ledger rows are missing")
    if not trade_rows and not ledger_rows:
        verdict = REVIEW
        reasons.append("no trades or ledger rows to validate")
    if missing_cash_delta:
        verdict = MISSING
        reasons.append(f"ledger rows missing cash_delta={missing_cash_delta}")
    if missing_realized_pnl:
        verdict = MISSING
        reasons.append(f"ledger rows missing realized_pnl={missing_realized_pnl}")
    if missing_trade_ids:
        verdict = REVIEW if verdict == READY else verdict
        reasons.append(f"closed trades without ledger rows={len(missing_trade_ids)}")
    if ledger_rows and cash_after_diff > tolerance:
        verdict = REVIEW if verdict == READY else verdict
        reasons.append(f"cash_after does not match cumulative cash_delta by {cash_after_diff}")
    if trade_rows and not missing_trade_ids and ledger_diff.copy_abs() > tolerance:
        verdict = REVIEW if verdict == READY else verdict
        reasons.append(f"ledger realized pnl differs from trade pnl by {ledger_diff}")
    if residual_position.copy_abs() > tolerance:
        verdict = REVIEW if verdict == READY else verdict
        reasons.append("ledger has residual open position; realized pnl is not full account equity")

    return {
        "status": READY,
        "cashflow_verdict": verdict,
        "reason": "; ".join(reasons) if reasons else "cashflow ledger reconstructs realized net pnl within tolerance",
        "initial_capital": _text(initial_capital),
        "trade_count": len(trade_rows),
        "ledger_event_count": len(ledger_rows),
        "event_counts": event_counts,
        "trade_pnl_total": _text(trade_pnl_total),
        "net_profit_trade": _text(trade_pnl_total),
        "net_profit_ledger": _text(ledger_realized_pnl),
        "ledger_realized_pnl": _text(ledger_realized_pnl),
        "ledger_diff": _text(ledger_diff),
        "cash_delta_total": _text(cash_delta_total),
        "reconstructed_cash_after": _text(reconstructed_cash),
        "reported_cash_after": _text(reported_cash_after),
        "cash_after_diff": _text(cash_after_diff),
        "residual_position": _text(residual_position),
        "missing_trade_ledger_count": len(missing_trade_ids),
        "missing_trade_ids": missing_trade_ids[:20],
        "missing_cash_delta_count": missing_cash_delta,
        "missing_realized_pnl_count": missing_realized_pnl,
        "tolerance": _text(tolerance),
        "cashflow_formula": "SELL + REDEEM + MERGE + REBATE - BUY - SPLIT + unrealized position value",
        "polymarket_special_event_counts": special_event_counts,
        "polymarket_credit_total": _text(_decimal(polymarket_summary.get("credit_total"))),
        "polymarket_debit_total": _text(_decimal(polymarket_summary.get("debit_total"))),
        "polymarket_cashflow_total": _text(_decimal(polymarket_summary.get("cashflow_total"))),
        "polymarket_excluded_total": _text(_decimal(polymarket_summary.get("excluded_total"))),
        "polymarket_unrealized_position_value": _text(_decimal(polymarket_summary.get("unrealized_position_value"))),
        "polymarket_residual_position_count": int(polymarket_summary.get("residual_position_count") or 0),
        "polymarket_residual_positions": polymarket_residual_positions,
        "polymarket_portfolio_cash_at_risk": _text(_decimal(polymarket_summary.get("portfolio_cash_at_risk"))),
        "polymarket_event_cash_at_risk": {
            str(key): _text(_decimal(value))
            for key, value in dict(polymarket_summary.get("event_cash_at_risk") or {}).items()
        },
        "polymarket_net_trading_pnl": _text(_decimal(polymarket_summary.get("net_trading_pnl"))),
        "polymarket_mark_price_count": len(mark_prices),
    }


def _last_cash_after(rows: Sequence[Mapping[str, Any]], initial_capital: Decimal) -> Decimal:
    for row in reversed(rows):
        if not _blank(row.get("cash_after")):
            return _decimal(row.get("cash_after"))
    return initial_capital


def _mark_prices_for_cashflow(
    rows: Sequence[Mapping[str, Any]],
    parameters: Mapping[str, Any],
) -> dict[str, Decimal]:
    mark_prices: dict[str, Decimal] = {}
    parameter_marks = parameters.get("mark_prices") or parameters.get("final_mark_prices") or parameters.get("close_prices")
    if isinstance(parameter_marks, Mapping):
        for key, value in parameter_marks.items():
            if not _blank(value):
                mark_prices[str(key)] = _decimal(value)

    for row in rows:
        mark_value = _first(
            row,
            "mark_price",
            "final_mark_price",
            "close_price",
            "latest_price",
            "settlement_price",
        )
        if _blank(mark_value):
            continue
        key = normalize_polymarket_activity_event(row)["position_key"]
        if key.strip("|"):
            mark_prices[key] = _decimal(mark_value)
    return mark_prices


def _serialize_residual_position(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "position_key": str(row.get("position_key") or ""),
        "market_slug": str(row.get("market_slug") or ""),
        "event_slug": str(row.get("event_slug") or ""),
        "token_id": str(row.get("token_id") or ""),
        "token_side": str(row.get("token_side") or ""),
        "quantity": _text(_decimal(row.get("quantity"))),
        "cost_basis": _text(_decimal(row.get("cost_basis"))),
        "mark_price": _text(_decimal(row.get("mark_price"))),
        "mark_value": _text(_decimal(row.get("mark_value"))),
    }


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and not _blank(row[key]):
            return row[key]
    return None


def _decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _blank(value: Any) -> bool:
    return value is None or value == ""


def _text(value: Decimal) -> str:
    return format(value.quantize(Q), "f").rstrip("0").rstrip(".") or "0"
