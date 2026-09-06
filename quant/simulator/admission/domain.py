"""Domain contracts for unified Polymarket eligibility admission."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


class AdmissionOperation(str, Enum):
    ORDER = "ORDER"
    ORDER_CANCEL = "ORDER_CANCEL"
    COMBO_RFQ = "COMBO_RFQ"
    SPLIT = "SPLIT"
    MERGE = "MERGE"
    REDEEM = "REDEEM"
    NEG_RISK_CONVERT = "NEG_RISK_CONVERT"
    BRIDGE_DEPOSIT = "BRIDGE_DEPOSIT"
    BRIDGE_WITHDRAWAL = "BRIDGE_WITHDRAWAL"
    SPONSOR = "SPONSOR"
    DISPUTE = "DISPUTE"
    READ = "READ"
    RECONCILIATION = "RECONCILIATION"

    @property
    def requires_order_geoblock(self) -> bool:
        return self in {AdmissionOperation.ORDER, AdmissionOperation.COMBO_RFQ}

    @property
    def always_available(self) -> bool:
        return self in {
            AdmissionOperation.ORDER_CANCEL,
            AdmissionOperation.READ,
            AdmissionOperation.RECONCILIATION,
        }


class ExposureEffect(str, Enum):
    INCREASE = "INCREASE"
    REDUCE = "REDUCE"
    NEUTRAL = "NEUTRAL"
    MIXED = "MIXED"
    UNKNOWN = "UNKNOWN"


class JurisdictionMode(str, Enum):
    UNRESTRICTED = "UNRESTRICTED"
    CLOSE_ONLY = "CLOSE_ONLY"
    BLOCK_COMPLETELY = "BLOCK_COMPLETELY"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNKNOWN = "UNKNOWN"


class AdmissionStatus(str, Enum):
    ALLOWED = "ALLOWED"
    DENIED = "DENIED"
    FAIL_CLOSED = "FAIL_CLOSED"


@dataclass(frozen=True)
class GeoblockSnapshot:
    blocked: bool
    country: str
    region: str
    detected_ip: str
    observed_at: datetime
    expires_at: datetime
    raw_payload_hash: str
    source: str = "polymarket_geoblock_api"
    proxy_url: str | None = None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.expires_at.tzinfo is None:
            raise ValueError("geoblock timestamps must be timezone-aware")
        if self.expires_at <= self.observed_at:
            raise ValueError("geoblock snapshot must expire after it is observed")
        if len(str(self.raw_payload_hash)) != 64:
            raise ValueError("geoblock payload hash must be SHA256")
        object.__setattr__(self, "country", str(self.country).strip().upper())
        object.__setattr__(self, "region", str(self.region).strip().upper())

    def is_fresh(self, at: datetime) -> bool:
        return self.observed_at <= at < self.expires_at

    @property
    def snapshot_id(self) -> str:
        return stable_hash(
            {
                "blocked": self.blocked,
                "country": self.country,
                "region": self.region,
                "detected_ip": self.detected_ip,
                "observed_at": self.observed_at,
                "expires_at": self.expires_at,
                "raw_payload_hash": self.raw_payload_hash,
                "source": self.source,
                "proxy_url": self.proxy_url,
            },
            prefix="geo-",
        )

    def as_dict(self) -> dict[str, Any]:
        return _jsonable(asdict(self))


@dataclass(frozen=True)
class AdmissionRequest:
    request_id: str
    operation: AdmissionOperation
    account_id: str
    strategy_id: str | None
    exposure_effect: ExposureEffect
    observed_at: datetime
    asset_id: str | None = None
    condition_id: str | None = None
    market_id: str | None = None
    exposure_before: Decimal | None = None
    exposure_after: Decimal | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not str(self.request_id).strip() or not str(self.account_id).strip():
            raise ValueError("admission request_id and account_id are required")
        if self.observed_at.tzinfo is None:
            raise ValueError("admission observed_at must be timezone-aware")
        if self.exposure_before is not None:
            object.__setattr__(self, "exposure_before", Decimal(self.exposure_before))
        if self.exposure_after is not None:
            object.__setattr__(self, "exposure_after", Decimal(self.exposure_after))

    @property
    def request_hash(self) -> str:
        return stable_hash(self.as_dict(), prefix="request-")

    def as_dict(self) -> dict[str, Any]:
        return _jsonable(
            {
                "request_id": self.request_id,
                "operation": self.operation.value,
                "account_id": self.account_id,
                "strategy_id": self.strategy_id,
                "asset_id": self.asset_id,
                "condition_id": self.condition_id,
                "market_id": self.market_id,
                "exposure_effect": self.exposure_effect.value,
                "exposure_before": self.exposure_before,
                "exposure_after": self.exposure_after,
                "observed_at": self.observed_at,
                "metadata": dict(self.metadata),
            }
        )


@dataclass(frozen=True)
class AdmissionDecision:
    decision_id: str
    request: AdmissionRequest
    status: AdmissionStatus
    jurisdiction_mode: JurisdictionMode
    policy_version: str
    reason_codes: tuple[str, ...]
    decided_at: datetime
    geoblock_snapshot: GeoblockSnapshot | None = None

    @property
    def allowed(self) -> bool:
        return self.status is AdmissionStatus.ALLOWED

    def as_dict(self) -> dict[str, Any]:
        return _jsonable(
            {
                "decision_id": self.decision_id,
                "request": self.request.as_dict(),
                "status": self.status.value,
                "allowed": self.allowed,
                "jurisdiction_mode": self.jurisdiction_mode.value,
                "policy_version": self.policy_version,
                "reason_codes": list(self.reason_codes),
                "decided_at": self.decided_at,
                "geoblock_snapshot": (
                    self.geoblock_snapshot.as_dict()
                    if self.geoblock_snapshot is not None
                    else None
                ),
            }
        )


def stable_hash(value: Any, *, prefix: str = "") -> str:
    payload = json.dumps(
        _jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return prefix + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value
