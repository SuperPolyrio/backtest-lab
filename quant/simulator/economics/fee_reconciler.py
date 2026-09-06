"""Reconcile modeled per-fill fees against official fill evidence."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .fee_engine import FeeCharge
from .fee_rounding import FEE_ROUNDING_UNIT


@dataclass(frozen=True)
class FeeReconciliation:
    status: str
    modeled_platform_fee: Decimal
    official_platform_fee: Decimal | None
    platform_fee_delta: Decimal | None
    modeled_builder_fee: Decimal
    official_builder_fee: Decimal | None
    builder_fee_delta: Decimal | None
    modeled_total_fee: Decimal
    official_total_fee: Decimal | None
    total_fee_delta: Decimal | None
    tolerance: Decimal


def reconcile_fee_charge(
    charge: FeeCharge,
    *,
    official_platform_fee: Decimal | None = None,
    official_builder_fee: Decimal | None = None,
    official_total_fee: Decimal | None = None,
    tolerance: Decimal = FEE_ROUNDING_UNIT,
) -> FeeReconciliation:
    if all(
        value is None
        for value in (official_platform_fee, official_builder_fee, official_total_fee)
    ):
        raise ValueError("at least one official fee amount is required")
    platform_delta = _delta(charge.platform_fee, official_platform_fee)
    builder_delta = _delta(charge.builder_fee, official_builder_fee)
    total_delta = _delta(charge.total_fee, official_total_fee)
    compared = tuple(
        item
        for item in (platform_delta, builder_delta, total_delta)
        if item is not None
    )
    status = "PASS" if all(abs(item) <= tolerance for item in compared) else "MISMATCH"
    return FeeReconciliation(
        status=status,
        modeled_platform_fee=charge.platform_fee,
        official_platform_fee=official_platform_fee,
        platform_fee_delta=platform_delta,
        modeled_builder_fee=charge.builder_fee,
        official_builder_fee=official_builder_fee,
        builder_fee_delta=builder_delta,
        modeled_total_fee=charge.total_fee,
        official_total_fee=official_total_fee,
        total_fee_delta=total_delta,
        tolerance=tolerance,
    )


def _delta(modeled: Decimal, official: Decimal | None) -> Decimal | None:
    return None if official is None else modeled - Decimal(official)
