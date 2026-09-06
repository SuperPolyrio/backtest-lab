"""Account timeline helpers for strategy lab reporting."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class AccountTimelineRow:
    timestamp: str
    event_type: str
    market_slug: str
    cash_delta: str
    cash_after: str
    locked_cost_after: str
    open_positions_after: int
    realized_pnl_after: str


def apply_bankroll_constraints(rows, *, initial_capital: Decimal):
    cash = initial_capital
    pending_settlements: list[tuple[datetime, Decimal]] = []
    constrained = []
    daily_costs: dict[str, Decimal] = {}
    for row in sorted(rows, key=lambda item: (item.signal_time, item.signal_block, item.signal_log_index, item.market_id)):
        signal_time = datetime.fromisoformat(row.signal_time)
        matured = [item for item in pending_settlements if item[0] <= signal_time]
        if matured:
            cash += sum((value for _, value in matured), Decimal("0"))
            pending_settlements = [item for item in pending_settlements if item[0] > signal_time]
        buy_cost = Decimal(row.buy_cost)
        trade_day = signal_time.date().isoformat()
        if Decimal(row.filled_size) > 0 and buy_cost > cash:
            constrained.append(
                replace(
                    row,
                    order_status="SKIPPED_INSUFFICIENT_CASH",
                    filled_size="0",
                    buy_cost="0",
                    settlement_value="0",
                    pnl="0",
                    roi="0",
                )
            )
            continue
        max_daily_cost = getattr(row, "max_daily_cost", None)
        if Decimal(row.filled_size) > 0 and max_daily_cost not in (None, "", "0"):
            cost_limit = Decimal(str(max_daily_cost))
            if daily_costs.get(trade_day, Decimal("0")) + buy_cost > cost_limit:
                constrained.append(
                    replace(
                        row,
                        order_status="SKIPPED_MAX_DAILY_COST",
                        filled_size="0",
                        buy_cost="0",
                        settlement_value="0",
                        pnl="0",
                        roi="0",
                    )
                )
                continue
        max_positions = getattr(row, "max_concurrent_positions_limit", None)
        if Decimal(row.filled_size) > 0 and max_positions not in (None, "", "0"):
            active_positions = sum(1 for settle_at, _ in pending_settlements if settle_at > signal_time)
            if active_positions >= int(max_positions):
                constrained.append(
                    replace(
                        row,
                        order_status="SKIPPED_MAX_CONCURRENT_POSITIONS",
                        filled_size="0",
                        buy_cost="0",
                        settlement_value="0",
                        pnl="0",
                        roi="0",
                    )
                )
                continue
        if buy_cost > 0:
            cash -= buy_cost
            pending_settlements.append((datetime.fromisoformat(row.end_date), Decimal(row.settlement_value)))
            daily_costs[trade_day] = daily_costs.get(trade_day, Decimal("0")) + buy_cost
        constrained.append(row)
    return constrained


def build_account_timeline(rows, *, initial_capital: Decimal):
    events: list[tuple[datetime, int, str, str, Decimal, Decimal]] = []
    for row in rows:
        cost = Decimal(row.buy_cost)
        pnl = Decimal(row.pnl)
        size = Decimal(row.filled_size)
        if cost <= 0 or size <= 0:
            continue
        start = datetime.fromisoformat(row.signal_time)
        end = datetime.fromisoformat(row.end_date) if row.end_date else start
        events.append((start, 0, "BUY", row.market_slug, -cost, Decimal("0")))
        events.append((end, 1, "SETTLEMENT", row.market_slug, Decimal(row.settlement_value), pnl))
    timeline: list[AccountTimelineRow] = []
    cash = initial_capital
    locked = Decimal("0")
    realized = Decimal("0")
    open_positions = 0
    for timestamp, order_key, event_type, market_slug, cash_delta, realized_delta in sorted(events, key=lambda item: (item[0], order_key_of(item))):
        cash += cash_delta
        if event_type == "BUY":
            locked += -cash_delta
            open_positions += 1
        else:
            locked -= cash_delta - realized_delta
            open_positions -= 1
            realized += realized_delta
        timeline.append(
            AccountTimelineRow(
                timestamp=timestamp.isoformat(),
                event_type=event_type,
                market_slug=market_slug,
                cash_delta=_decimal_text(cash_delta),
                cash_after=_decimal_text(cash),
                locked_cost_after=_decimal_text(max(locked, Decimal("0"))),
                open_positions_after=max(open_positions, 0),
                realized_pnl_after=_decimal_text(realized),
            )
        )
    return timeline


def max_concurrent_exposure(rows) -> tuple[Decimal, int]:
    max_cost = Decimal("0")
    max_positions = 0
    for row in build_account_timeline(rows, initial_capital=Decimal("0")):
        locked = Decimal(row.locked_cost_after)
        max_cost = max(max_cost, locked)
        max_positions = max(max_positions, row.open_positions_after)
    return max_cost, max_positions


def order_key_of(item: tuple[datetime, int, str, str, Decimal, Decimal]) -> int:
    return int(item[1])


def _decimal_text(value: Decimal | int | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(decimal_value.normalize(), "f")
