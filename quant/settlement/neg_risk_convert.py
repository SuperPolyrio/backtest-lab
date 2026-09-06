"""Inventory-safe negative-risk conversion planning."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class NegRiskConversion:
    source_no_asset_id: str
    quantity: Decimal
    collateral_delta: Decimal
    yes_deltas: Mapping[str, Decimal]
    status: str


def plan_no_to_other_yes(
    *,
    source_no_asset_id: str,
    event_yes_asset_ids: tuple[str, ...],
    quantity: Decimal,
    augmented_neg_risk: bool,
    source_yes_asset_id: str | None = None,
) -> NegRiskConversion:
    if quantity <= 0:
        raise ValueError("conversion quantity must be positive")
    if not source_yes_asset_id:
        raise ValueError("conversion requires the source outcome YES asset")
    targets = tuple(
        asset for asset in event_yes_asset_ids if asset != source_yes_asset_id
    )
    if augmented_neg_risk:
        if not targets:
            raise ValueError("augmented conversion requires other active YES assets")
        return NegRiskConversion(
            source_no_asset_id=source_no_asset_id,
            quantity=quantity,
            collateral_delta=Decimal(0),
            yes_deltas={asset: quantity for asset in targets},
            status="PLANNED_AUGMENTED",
        )
    if not targets:
        raise ValueError("neg-risk conversion requires other YES assets")
    return NegRiskConversion(
        source_no_asset_id=source_no_asset_id,
        quantity=quantity,
        collateral_delta=Decimal(0),
        yes_deltas={asset: quantity for asset in targets},
        status="PLANNED",
    )
