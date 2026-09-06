"""Valuation views for resolution/dispute capital that is not yet redeemable cash."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .oracle_state import OracleResolutionState, ResolutionPhase
from .payout_vector import PayoutVector


@dataclass(frozen=True)
class ResolutionExposure:
    asset_id: str
    quantity: Decimal
    current_liquidation_mark: Decimal | None
    payout_vector: PayoutVector


@dataclass(frozen=True)
class ResolutionValuation:
    optimistic_resolution_value: Decimal
    liquidation_value: Decimal | None
    stressed_dispute_value: Decimal | None
    confirmed_redeemable_payout: Decimal
    capital_locked: bool


def value_resolution_exposure(
    state: OracleResolutionState,
    exposure: ResolutionExposure,
    *,
    dispute_haircut: Decimal = Decimal("0.5"),
) -> ResolutionValuation:
    quantity = Decimal(exposure.quantity)
    if quantity < 0:
        raise ValueError("resolution exposure cannot be negative")
    payout = exposure.payout_vector.payout_for(exposure.asset_id)
    optimistic = quantity * payout
    liquidation = (
        None
        if exposure.current_liquidation_mark is None
        else quantity * Decimal(exposure.current_liquidation_mark)
    )
    haircut = min(Decimal(1), max(Decimal(0), Decimal(dispute_haircut)))
    stressed = liquidation
    if state.phase in {
        ResolutionPhase.DISPUTED_ROUND_1,
        ResolutionPhase.DISPUTED_ROUND_2,
        ResolutionPhase.DISPUTED,
        ResolutionPhase.DVM_VOTING,
    }:
        stressed = (
            min(optimistic * haircut, liquidation)
            if liquidation is not None
            else optimistic * haircut
        )
    redeemable = (
        optimistic
        if state.phase
        in {
            ResolutionPhase.REDEEMABLE,
            ResolutionPhase.REDEEMING,
            ResolutionPhase.REDEEMED,
        }
        else Decimal(0)
    )
    return ResolutionValuation(
        optimistic_resolution_value=optimistic,
        liquidation_value=liquidation,
        stressed_dispute_value=stressed,
        confirmed_redeemable_payout=redeemable,
        capital_locked=state.is_capital_locked,
    )
