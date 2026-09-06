"""Generic fast-vs-accurate replay comparison helpers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class FastAccurateComparisonRow:
    market_id: int
    market_slug: str
    title: str
    event_time: str | None
    buy_outcome_label: str
    signal_time: str
    fast_status: str
    accurate_status: str
    fast_buy_price: str
    accurate_buy_price: str
    fast_signal_probability: str
    accurate_signal_probability: str
    fast_buy_cost: str
    accurate_buy_cost: str
    fast_filled_size: str
    accurate_filled_size: str
    fast_pnl: str
    accurate_pnl: str
    pnl_diff: str
    fast_signal_block: int
    accurate_signal_block: int
    fast_fill_block: int
    accurate_fill_block: int
    token_id: str
    settlement_outcome: str
    data_quality: str


@dataclass(frozen=True)
class FastAccurateComparisonSummary:
    matched_markets: int
    status_mismatches: int
    pnl_diff_abs_total: str
    fast_total_pnl: str
    accurate_total_pnl: str


class ComparableTrade(Protocol):
    market_id: int
    market_slug: str
    title: str
    end_date: str | None
    buy_outcome_label: str
    signal_time: str
    order_status: str
    limit_price: str
    signal_probability: str
    buy_cost: str
    filled_size: str
    pnl: str
    signal_block: int
    fill_block: int
    token_id: str
    settlement_outcome: str


def build_fast_accurate_rows(
    fast_trades: list[ComparableTrade],
    accurate_trades: list[ComparableTrade],
) -> list[FastAccurateComparisonRow]:
    fast_by_market = {row.market_id: row for row in fast_trades}
    accurate_by_market = {row.market_id: row for row in accurate_trades}
    rows: list[FastAccurateComparisonRow] = []
    for market_id in sorted(set(fast_by_market) | set(accurate_by_market)):
        fast = fast_by_market.get(market_id)
        accurate = accurate_by_market.get(market_id)
        template = fast or accurate
        if template is None:
            continue
        fast_pnl = Decimal(fast.pnl) if fast else Decimal("0")
        accurate_pnl = Decimal(accurate.pnl) if accurate else Decimal("0")
        rows.append(
            FastAccurateComparisonRow(
                market_id=market_id,
                market_slug=template.market_slug,
                title=template.title,
                event_time=template.end_date,
                buy_outcome_label=template.buy_outcome_label,
                signal_time=template.signal_time,
                fast_status=fast.order_status if fast else "NO_SIGNAL",
                accurate_status=accurate.order_status if accurate else "NO_SIGNAL",
                fast_buy_price=fast.limit_price if fast else "",
                accurate_buy_price=accurate.limit_price if accurate else "",
                fast_signal_probability=fast.signal_probability if fast else "",
                accurate_signal_probability=accurate.signal_probability if accurate else "",
                fast_buy_cost=fast.buy_cost if fast else "0",
                accurate_buy_cost=accurate.buy_cost if accurate else "0",
                fast_filled_size=fast.filled_size if fast else "0",
                accurate_filled_size=accurate.filled_size if accurate else "0",
                fast_pnl=_decimal_text(fast_pnl),
                accurate_pnl=_decimal_text(accurate_pnl),
                pnl_diff=_signed_decimal_text(fast_pnl - accurate_pnl),
                fast_signal_block=fast.signal_block if fast else 0,
                accurate_signal_block=accurate.signal_block if accurate else 0,
                fast_fill_block=fast.fill_block if fast else 0,
                accurate_fill_block=accurate.fill_block if accurate else 0,
                token_id=template.token_id,
                settlement_outcome=template.settlement_outcome,
                data_quality=_comparison_quality(fast, accurate),
            )
        )
    return rows


def summarize_fast_accurate_rows(rows: list[FastAccurateComparisonRow]) -> FastAccurateComparisonSummary:
    status_mismatches = sum(1 for row in rows if row.fast_status != row.accurate_status)
    pnl_diff_abs_total = sum((abs(Decimal(row.pnl_diff)) for row in rows), Decimal("0"))
    fast_total_pnl = sum((Decimal(row.fast_pnl) for row in rows), Decimal("0"))
    accurate_total_pnl = sum((Decimal(row.accurate_pnl) for row in rows), Decimal("0"))
    return FastAccurateComparisonSummary(
        matched_markets=len(rows),
        status_mismatches=status_mismatches,
        pnl_diff_abs_total=_decimal_text(pnl_diff_abs_total),
        fast_total_pnl=_decimal_text(fast_total_pnl),
        accurate_total_pnl=_decimal_text(accurate_total_pnl),
    )


def _comparison_quality(fast: ComparableTrade | None, accurate: ComparableTrade | None) -> str:
    if fast is None:
        return "accurate_only_signal"
    if accurate is None:
        return "fast_only_signal"
    if fast.order_status != accurate.order_status:
        return "status_mismatch"
    if Decimal(fast.pnl) != Decimal(accurate.pnl):
        return "pnl_drift"
    if fast.fill_block != accurate.fill_block:
        return "fill_block_drift"
    if Decimal(fast.filled_size) != Decimal(accurate.filled_size):
        return "fill_size_drift"
    return "matched"


def _decimal_text(value: Decimal | int | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(decimal_value.normalize(), "f")


def _signed_decimal_text(value: Decimal) -> str:
    text = _decimal_text(value)
    if value > 0:
        return f"+{text}"
    return text
