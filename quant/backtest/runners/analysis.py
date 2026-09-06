"""Analysis helpers for strategy lab runners."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_FLOOR


@dataclass(frozen=True)
class ProbabilityBucketRow:
    bucket_label: str
    trade_count: int
    wins: int
    losses: int
    total_cost: str
    total_payoff: str
    total_pnl: str
    win_rate: str
    roi_on_cost: str


@dataclass(frozen=True)
class DailyTradeRow:
    trade_day: str
    signal_count: int
    filled_count: int
    skipped_count: int
    wins: int
    losses: int
    total_cost: str
    total_payoff: str
    total_pnl: str


@dataclass(frozen=True)
class DriftBucketRow:
    bucket_label: str
    trade_count: int
    avg_snapshot_probability: str
    avg_close_line_probability: str
    avg_snapshot_drift: str
    total_pnl: str
    roi_on_cost: str


def build_probability_bucket_rows(rows, *, bucket_size: Decimal = Decimal("0.05")) -> list[ProbabilityBucketRow]:
    buckets: dict[str, list] = {}
    for row in rows:
        if Decimal(row.filled_size) <= 0:
            continue
        probability = Decimal(row.signal_probability)
        floor = (probability / bucket_size).to_integral_value(rounding=ROUND_FLOOR) * bucket_size
        ceiling = floor + bucket_size
        label = f"{_decimal_text(floor)}-{_decimal_text(ceiling)}"
        buckets.setdefault(label, []).append(row)
    bucket_rows: list[ProbabilityBucketRow] = []
    for label in sorted(buckets):
        bucket = buckets[label]
        costs = sum((Decimal(row.buy_cost) for row in bucket), Decimal("0"))
        payoffs = sum((Decimal(row.settlement_value) for row in bucket), Decimal("0"))
        pnl = payoffs - costs
        wins = sum(1 for row in bucket if Decimal(row.pnl) > 0)
        losses = sum(1 for row in bucket if Decimal(row.pnl) < 0)
        bucket_rows.append(
            ProbabilityBucketRow(
                bucket_label=label,
                trade_count=len(bucket),
                wins=wins,
                losses=losses,
                total_cost=_decimal_text(costs),
                total_payoff=_decimal_text(payoffs),
                total_pnl=_decimal_text(pnl),
                win_rate=_decimal_text(Decimal(wins) / Decimal(len(bucket)) if bucket else Decimal("0")),
                roi_on_cost=_decimal_text(pnl / costs if costs else Decimal("0")),
            )
        )
    return bucket_rows


def build_daily_trade_rows(rows) -> list[DailyTradeRow]:
    grouped: dict[str, list] = {}
    for row in rows:
        day = datetime.fromisoformat(row.signal_time).date().isoformat()
        grouped.setdefault(day, []).append(row)
    daily_rows: list[DailyTradeRow] = []
    for day in sorted(grouped):
        group = grouped[day]
        filled = [row for row in group if Decimal(row.filled_size) > 0]
        costs = sum((Decimal(row.buy_cost) for row in filled), Decimal("0"))
        payoffs = sum((Decimal(row.settlement_value) for row in filled), Decimal("0"))
        pnl = payoffs - costs
        daily_rows.append(
            DailyTradeRow(
                trade_day=day,
                signal_count=len(group),
                filled_count=len(filled),
                skipped_count=sum(1 for row in group if Decimal(row.filled_size) <= 0),
                wins=sum(1 for row in filled if Decimal(row.pnl) > 0),
                losses=sum(1 for row in filled if Decimal(row.pnl) < 0),
                total_cost=_decimal_text(costs),
                total_payoff=_decimal_text(payoffs),
                total_pnl=_decimal_text(pnl),
            )
        )
    return daily_rows


def build_drift_bucket_rows(rows, *, bucket_size: Decimal = Decimal("0.02")) -> list[DriftBucketRow]:
    buckets: dict[str, list] = {}
    for row in rows:
        if Decimal(row.filled_size) <= 0:
            continue
        drift = Decimal(row.snapshot_drift)
        floor = (drift / bucket_size).to_integral_value(rounding=ROUND_FLOOR) * bucket_size
        ceiling = floor + bucket_size
        label = f"{_signed_decimal_text(floor)}-{_signed_decimal_text(ceiling)}"
        buckets.setdefault(label, []).append(row)
    drift_rows: list[DriftBucketRow] = []
    for label in sorted(buckets):
        bucket = buckets[label]
        snapshot_probs = [Decimal(row.signal_probability) for row in bucket]
        close_probs = [Decimal(row.close_line_probability) for row in bucket]
        drifts = [Decimal(row.snapshot_drift) for row in bucket]
        costs = sum((Decimal(row.buy_cost) for row in bucket), Decimal("0"))
        pnl = sum((Decimal(row.pnl) for row in bucket), Decimal("0"))
        drift_rows.append(
            DriftBucketRow(
                bucket_label=label,
                trade_count=len(bucket),
                avg_snapshot_probability=_decimal_text(sum(snapshot_probs, Decimal("0")) / Decimal(len(snapshot_probs))),
                avg_close_line_probability=_decimal_text(sum(close_probs, Decimal("0")) / Decimal(len(close_probs))),
                avg_snapshot_drift=_signed_decimal_text(sum(drifts, Decimal("0")) / Decimal(len(drifts))),
                total_pnl=_decimal_text(pnl),
                roi_on_cost=_decimal_text(pnl / costs if costs else Decimal("0")),
            )
        )
    return drift_rows


def _decimal_text(value: Decimal | int | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(decimal_value.normalize(), "f")


def _signed_decimal_text(value: Decimal) -> str:
    text = _decimal_text(value)
    if value > 0 and not text.startswith("+"):
        return f"+{text}"
    return text
