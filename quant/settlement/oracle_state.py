"""Resolution phases, proposal timing, redeem retries, and capital-lock accounting."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum


class ResolutionPhase(str, Enum):
    TRADING_STOPPED = "TRADING_STOPPED"
    RESOLUTION_ELIGIBLE = "RESOLUTION_ELIGIBLE"
    PROPOSAL_SUBMITTED = "PROPOSAL_SUBMITTED"
    CHALLENGE_WINDOW = "CHALLENGE_WINDOW"
    DISPUTED_ROUND_1 = "DISPUTED_ROUND_1"
    SECOND_PROPOSAL = "SECOND_PROPOSAL"
    DISPUTED_ROUND_2 = "DISPUTED_ROUND_2"
    DVM_VOTING = "DVM_VOTING"
    RESOLUTION_FINAL = "RESOLUTION_FINAL"
    REDEEMABLE = "REDEEMABLE"
    REDEEMING = "REDEEMING"
    REDEEMED = "REDEEMED"

    # Kept as distinct legacy values for existing persisted paper rows.
    PROPOSAL_PENDING = "PROPOSAL_PENDING"
    DISPUTED = "DISPUTED"
    FINALIZED = "FINALIZED"


_ALLOWED = {
    ResolutionPhase.TRADING_STOPPED: {
        ResolutionPhase.RESOLUTION_ELIGIBLE,
        ResolutionPhase.PROPOSAL_PENDING,
        ResolutionPhase.FINALIZED,
    },
    ResolutionPhase.RESOLUTION_ELIGIBLE: {
        ResolutionPhase.PROPOSAL_SUBMITTED,
        ResolutionPhase.RESOLUTION_FINAL,
    },
    ResolutionPhase.PROPOSAL_SUBMITTED: {
        ResolutionPhase.CHALLENGE_WINDOW,
        ResolutionPhase.DISPUTED_ROUND_1,
    },
    ResolutionPhase.CHALLENGE_WINDOW: {
        ResolutionPhase.DISPUTED_ROUND_1,
        ResolutionPhase.RESOLUTION_FINAL,
    },
    ResolutionPhase.DISPUTED_ROUND_1: {
        ResolutionPhase.SECOND_PROPOSAL,
        ResolutionPhase.DVM_VOTING,
    },
    ResolutionPhase.SECOND_PROPOSAL: {
        ResolutionPhase.CHALLENGE_WINDOW,
        ResolutionPhase.DISPUTED_ROUND_2,
    },
    ResolutionPhase.DISPUTED_ROUND_2: {ResolutionPhase.DVM_VOTING},
    ResolutionPhase.DVM_VOTING: {
        ResolutionPhase.RESOLUTION_FINAL,
        ResolutionPhase.FINALIZED,
    },
    ResolutionPhase.RESOLUTION_FINAL: {ResolutionPhase.REDEEMABLE},
    ResolutionPhase.REDEEMABLE: {ResolutionPhase.REDEEMING},
    ResolutionPhase.REDEEMING: {ResolutionPhase.REDEEMABLE, ResolutionPhase.REDEEMED},
    ResolutionPhase.PROPOSAL_PENDING: {
        ResolutionPhase.CHALLENGE_WINDOW,
        ResolutionPhase.DISPUTED,
        ResolutionPhase.FINALIZED,
    },
    ResolutionPhase.DISPUTED: {ResolutionPhase.DVM_VOTING},
    ResolutionPhase.FINALIZED: {ResolutionPhase.REDEEMABLE},
}


@dataclass(frozen=True)
class OracleResolutionState:
    condition_id: str
    phase: ResolutionPhase
    trading_stopped_at: datetime
    expected_resolution_at: datetime | None = None
    actual_finalized_at: datetime | None = None
    redeemed_at: datetime | None = None
    redeem_started_at: datetime | None = None
    proposal_count: int = 0
    dispute_round: int = 0

    def transition(
        self, target: ResolutionPhase, *, at: datetime
    ) -> OracleResolutionState:
        if target == self.phase:
            return self
        if target not in _ALLOWED.get(self.phase, set()):
            raise ValueError(
                f"invalid resolution transition {self.phase.value} -> {target.value}"
            )
        if (
            target is ResolutionPhase.PROPOSAL_SUBMITTED
            and self.expected_resolution_at
            and at < self.expected_resolution_at
        ):
            raise ValueError(
                "proposal cannot be submitted before expected resolution time"
            )
        return replace(
            self,
            phase=target,
            actual_finalized_at=(
                at
                if target
                in {ResolutionPhase.RESOLUTION_FINAL, ResolutionPhase.FINALIZED}
                else self.actual_finalized_at
            ),
            redeemed_at=at if target is ResolutionPhase.REDEEMED else self.redeemed_at,
            redeem_started_at=at
            if target is ResolutionPhase.REDEEMING
            else self.redeem_started_at,
            proposal_count=self.proposal_count
            + int(
                target
                in {ResolutionPhase.PROPOSAL_SUBMITTED, ResolutionPhase.SECOND_PROPOSAL}
            ),
            dispute_round=max(
                self.dispute_round,
                1
                if target
                in {ResolutionPhase.DISPUTED_ROUND_1, ResolutionPhase.DISPUTED}
                else 2
                if target is ResolutionPhase.DISPUTED_ROUND_2
                else 0,
            ),
        )

    @property
    def is_capital_locked(self) -> bool:
        return self.phase is not ResolutionPhase.REDEEMED

    @property
    def is_final(self) -> bool:
        return self.phase in {
            ResolutionPhase.RESOLUTION_FINAL,
            ResolutionPhase.FINALIZED,
            ResolutionPhase.REDEEMABLE,
            ResolutionPhase.REDEEMING,
            ResolutionPhase.REDEEMED,
        }

    def capital_locked_seconds(self, *, now: datetime) -> int:
        end = self.redeemed_at or now
        return max(0, int((end - self.trading_stopped_at).total_seconds()))
