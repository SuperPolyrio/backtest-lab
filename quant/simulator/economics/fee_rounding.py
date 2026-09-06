"""Polymarket fee precision and minimum-charge semantics."""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal

FEE_ROUNDING_UNIT = Decimal("0.00001")
FEE_ROUNDING_POLICY = "POLYMARKET_V2_TRUNCATE_5DP_MIN_0.00001"


def round_fee(
    amount: Decimal,
    *,
    unit: Decimal = FEE_ROUNDING_UNIT,
) -> Decimal:
    """Round one fee component; sub-unit charges become zero."""

    value = max(Decimal(0), Decimal(amount))
    if unit <= 0:
        raise ValueError("fee rounding unit must be positive")
    # V2 OrderFilled receipts are authoritative. Observed half-unit cases are
    # truncated by the operator before the integer fee reaches the contract.
    rounded = value.quantize(unit, rounding=ROUND_DOWN)
    return rounded if rounded >= unit else Decimal(0).quantize(unit)
