"""Single admission authority for orders and account mutations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from .client import GeoblockUnavailable
from .domain import (
    AdmissionDecision,
    AdmissionRequest,
    AdmissionStatus,
    ExposureEffect,
    GeoblockSnapshot,
    JurisdictionMode,
    stable_hash,
)
from .policy import JurisdictionPolicy


class GeoblockProvider(Protocol):
    def snapshot(self, *, now: datetime | None = None) -> GeoblockSnapshot: ...


class AdmissionStore(Protocol):
    def record_policy(self, policy: JurisdictionPolicy) -> None: ...

    def decision(self, request_id: str) -> AdmissionDecision | None: ...

    def record_decision(self, decision: AdmissionDecision) -> AdmissionDecision: ...


class UnifiedAdmissionService:
    def __init__(
        self,
        *,
        store: AdmissionStore,
        policy: JurisdictionPolicy | None = None,
        geoblock_provider: GeoblockProvider | None = None,
    ) -> None:
        self.store = store
        self.policy = policy or JurisdictionPolicy()
        self.geoblock_provider = geoblock_provider

    def decide(
        self,
        request: AdmissionRequest,
        *,
        geoblock_snapshot: GeoblockSnapshot | None = None,
    ) -> AdmissionDecision:
        current = self.store.decision(request.request_id)
        if current is not None:
            if current.request.request_hash != request.request_hash:
                raise ValueError("admission request id conflicts with prior payload")
            return current
        self.store.record_policy(self.policy)
        decided_at = datetime.now(timezone.utc)
        snapshot = geoblock_snapshot
        provider_error: str | None = None
        if request.operation.requires_order_geoblock and snapshot is None:
            if self.geoblock_provider is None:
                provider_error = "geoblock_provider_missing"
            else:
                try:
                    snapshot = self.geoblock_provider.snapshot(now=request.observed_at)
                except GeoblockUnavailable:
                    provider_error = "geoblock_api_unavailable"
                except Exception:
                    provider_error = "geoblock_provider_error"

        status, mode, reasons = self._evaluate(
            request,
            snapshot=snapshot,
            provider_error=provider_error,
        )
        decision_id = stable_hash(
            {
                "request_hash": request.request_hash,
                "status": status.value,
                "mode": mode.value,
                "policy_version": self.policy.version,
                "reasons": reasons,
                "snapshot_id": snapshot.snapshot_id if snapshot else None,
            },
            prefix="admission-",
        )
        return self.store.record_decision(
            AdmissionDecision(
                decision_id=decision_id,
                request=request,
                status=status,
                jurisdiction_mode=mode,
                policy_version=self.policy.version,
                reason_codes=tuple(reasons),
                decided_at=decided_at,
                geoblock_snapshot=snapshot,
            )
        )

    def _evaluate(
        self,
        request: AdmissionRequest,
        *,
        snapshot: GeoblockSnapshot | None,
        provider_error: str | None,
    ) -> tuple[AdmissionStatus, JurisdictionMode, list[str]]:
        if request.operation.always_available:
            return (
                AdmissionStatus.ALLOWED,
                JurisdictionMode.NOT_APPLICABLE,
                ["risk_reducing_control_plane_action"],
            )
        if not request.operation.requires_order_geoblock:
            return (
                AdmissionStatus.ALLOWED,
                JurisdictionMode.NOT_APPLICABLE,
                ["order_geoblock_not_applicable"],
            )
        if snapshot is None:
            return (
                AdmissionStatus.FAIL_CLOSED,
                JurisdictionMode.UNKNOWN,
                [provider_error or "geoblock_snapshot_missing"],
            )
        if not snapshot.is_fresh(request.observed_at):
            return (
                AdmissionStatus.FAIL_CLOSED,
                JurisdictionMode.UNKNOWN,
                ["geoblock_snapshot_stale"],
            )
        mode = self.policy.classify(snapshot)
        if mode is JurisdictionMode.UNRESTRICTED:
            return AdmissionStatus.ALLOWED, mode, ["jurisdiction_unrestricted"]
        if mode is JurisdictionMode.CLOSE_ONLY:
            if request.exposure_effect is ExposureEffect.REDUCE:
                if (
                    request.exposure_before is None
                    or request.exposure_after is None
                ):
                    return (
                        AdmissionStatus.FAIL_CLOSED,
                        mode,
                        ["close_only_exposure_unverified"],
                    )
                if not (
                    request.exposure_before > request.exposure_after
                    and request.exposure_after >= 0
                ):
                    return (
                        AdmissionStatus.DENIED,
                        mode,
                        ["close_only_reverse_or_nonreducing_order_denied"],
                    )
                return AdmissionStatus.ALLOWED, mode, ["close_only_risk_reduction"]
            return AdmissionStatus.DENIED, mode, ["close_only_exposure_increase_denied"]
        if mode is JurisdictionMode.BLOCK_COMPLETELY:
            return AdmissionStatus.DENIED, mode, ["jurisdiction_blocked_completely"]
        return (
            AdmissionStatus.FAIL_CLOSED,
            JurisdictionMode.UNKNOWN,
            ["blocked_jurisdiction_unclassified"],
        )


def order_exposure_effect(side: str) -> ExposureEffect:
    selected = str(side).strip().upper()
    if selected == "BUY":
        return ExposureEffect.INCREASE
    if selected == "SELL":
        return ExposureEffect.REDUCE
    return ExposureEffect.UNKNOWN


def multileg_exposure_effect(legs: Any) -> ExposureEffect:
    sides = {str(getattr(leg, "side", "")).upper() for leg in legs}
    if sides == {"BUY"}:
        return ExposureEffect.INCREASE
    if sides == {"SELL"}:
        return ExposureEffect.REDUCE
    if sides <= {"BUY", "SELL"} and sides:
        return ExposureEffect.MIXED
    return ExposureEffect.UNKNOWN
