"""Builder fee calculation, separate from platform fees and rebates."""

from __future__ import annotations

from decimal import Decimal

from .fee_rounding import round_fee
from .fee_schedule_registry import FeeSchedule


def builder_fee_bps(schedule: FeeSchedule, liquidity_role: str) -> int:
    role = getattr(liquidity_role, "value", liquidity_role)
    role = str(role).upper()
    if role == "TAKER":
        return schedule.builder_taker_fee_bps
    if role == "MAKER":
        return schedule.builder_maker_fee_bps
    raise ValueError(f"unsupported liquidity role: {liquidity_role}")


def calculate_builder_fee(
    *,
    schedule: FeeSchedule,
    liquidity_role: str,
    price: Decimal,
    shares: Decimal,
) -> tuple[int, Decimal]:
    bps = builder_fee_bps(schedule, liquidity_role)
    raw = max(Decimal(0), shares) * max(Decimal(0), price)
    raw *= Decimal(bps) / Decimal(10_000)
    return bps, round_fee(raw, unit=schedule.rounding_unit)
