"""Strategy-spec helpers for OrderFilled-first strategy lab runners."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from typing import Literal


SortMode = Literal[
    "probability_desc",
    "snapshot_probability_desc",
    "probability_asc",
    "closest_to_min_probability",
    "rows_desc",
    "signal_time_asc",
    "close_line_edge_desc",
    "close_line_edge_asc",
]


@dataclass(frozen=True)
class FavoriteHoldStrategySpec:
    min_probability: Decimal = Decimal("0.60")
    max_probability: Decimal = Decimal("0.80")
    snapshot_hours_before_start: Decimal | None = Decimal("1")
    signal_lookback_hours: Decimal = Decimal("24")
    window_start_hours: Decimal = Decimal("4")
    window_end_hours: Decimal = Decimal("0")
    initial_capital: Decimal = Decimal("1000")
    stake: Decimal = Decimal("10")
    liquidity_cap_pct: Decimal = Decimal("100")
    enforce_bankroll: bool = True
    max_daily_trades: int | None = None
    max_daily_cost: Decimal | None = None
    max_concurrent_positions: int | None = None
    sort_by: SortMode = "probability_desc"
    yes_only: bool = True
    execution_profile: str = "realistic"


def trade_calendar_day(signal_time: str) -> date:
    return datetime.fromisoformat(signal_time).date()


def select_trade_candidates(
    rows: list,
    *,
    max_daily_trades: int | None,
    sort_by: SortMode,
    probability_getter,
    signal_time_getter,
    rows_getter,
    mark_skipped,
    min_probability: Decimal,
) -> list:
    if max_daily_trades is None or max_daily_trades <= 0:
        return rows
    grouped: dict[date, list] = {}
    for row in rows:
        grouped.setdefault(trade_calendar_day(signal_time_getter(row)), []).append(row)
    selected: list = []
    for day in sorted(grouped):
        ranked = sorted(
            grouped[day],
            key=lambda row: _selection_sort_key(
                row,
                sort_by=sort_by,
                probability_getter=probability_getter,
                signal_time_getter=signal_time_getter,
                rows_getter=rows_getter,
                min_probability=min_probability,
            ),
        )
        allowed = ranked[:max_daily_trades]
        overflow = ranked[max_daily_trades:]
        selected.extend(allowed)
        selected.extend(mark_skipped(row) for row in overflow)
    return sorted(selected, key=lambda row: (signal_time_getter(row), probability_getter(row)))


def _selection_sort_key(
    row,
    *,
    sort_by: SortMode,
    probability_getter,
    signal_time_getter,
    rows_getter,
    min_probability: Decimal,
) -> tuple:
    probability = probability_getter(row)
    signal_time = signal_time_getter(row)
    rows = rows_getter(row)
    close_line_edge = Decimal(str(getattr(row, "close_line_edge", probability)))
    if sort_by == "probability_desc":
        return (-close_line_edge, -probability, signal_time)
    if sort_by == "snapshot_probability_desc":
        return (-probability, signal_time)
    if sort_by == "probability_asc":
        return (probability, signal_time)
    if sort_by == "closest_to_min_probability":
        return (abs(probability - min_probability), signal_time)
    if sort_by == "rows_desc":
        return (-rows, signal_time)
    if sort_by == "close_line_edge_desc":
        return (-close_line_edge, -probability, signal_time)
    if sort_by == "close_line_edge_asc":
        return (close_line_edge, probability, signal_time)
    return (signal_time, -probability)


def mark_skipped_status(row, *, status_field: str = "order_status", status_value: str = "SKIPPED_MAX_DAILY_TRADES"):
    return replace(row, **{status_field: status_value, "filled_size": "0", "buy_cost": "0", "settlement_value": "0", "pnl": "0", "roi": "0"})
