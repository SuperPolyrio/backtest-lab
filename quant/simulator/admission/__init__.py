"""Unified eligibility and geoblock admission."""

from .client import GeoblockUnavailable, PolymarketGeoblockClient
from .domain import (
    AdmissionDecision,
    AdmissionOperation,
    AdmissionRequest,
    AdmissionStatus,
    ExposureEffect,
    GeoblockSnapshot,
    JurisdictionMode,
)
from .policy import DEFAULT_POLICY_VERSION, JurisdictionPolicy
from .runtime import AdmissionRuntime, build_admission_runtime
from .service import (
    UnifiedAdmissionService,
    multileg_exposure_effect,
    order_exposure_effect,
)
from .store import MemoryAdmissionStore, PostgresAdmissionStore

__all__ = [
    "AdmissionDecision",
    "AdmissionOperation",
    "AdmissionRequest",
    "AdmissionRuntime",
    "AdmissionStatus",
    "DEFAULT_POLICY_VERSION",
    "ExposureEffect",
    "GeoblockSnapshot",
    "GeoblockUnavailable",
    "JurisdictionMode",
    "JurisdictionPolicy",
    "MemoryAdmissionStore",
    "PolymarketGeoblockClient",
    "PostgresAdmissionStore",
    "UnifiedAdmissionService",
    "build_admission_runtime",
    "multileg_exposure_effect",
    "order_exposure_effect",
]
