"""Point-in-time L2 gap availability forecasts for modeled execution only."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .contracts import canonical_value, qty

SCHEMA_VERSION = "pml2_gap_availability_v1"
DEFAULT_ARTIFACT_PATH = (
    Path(__file__).resolve().parents[3]
    / "config"
    / "execution"
    / "pml2_gap_availability.v1.json"
)


@dataclass(frozen=True)
class GapAvailabilityForecast:
    gap_ms: int
    p_executable: Decimal
    conditional_available_fraction: Decimal
    expected_available_size: Decimal
    expected_executable_size: Decimal
    price_error_quantiles: Mapping[str, Decimal]
    stratum_key: str
    sample_count: int
    executable_count: int
    maximum_supported_gap_ms: int
    model_version: str
    artifact_hash: str
    evidence_scope: str
    domain_status: str
    promotion_allowed: bool

    @property
    def abstained(self) -> bool:
        return self.domain_status.startswith("ABSTAIN")

    def as_dict(self) -> dict[str, Any]:
        return canonical_value(self.__dict__)


@dataclass(frozen=True)
class GapAvailabilityArtifact:
    payload: Mapping[str, Any]

    @classmethod
    def load(cls, path: Path | str = DEFAULT_ARTIFACT_PATH) -> GapAvailabilityArtifact:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise TypeError("gap availability artifact must be an object")
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("unsupported gap availability artifact schema")
        expected = str(payload.get("artifact_hash") or "")
        body = dict(payload)
        body.pop("artifact_hash", None)
        if not expected or expected != payload_hash(body):
            raise ValueError("gap availability artifact hash mismatch")
        walk_forward = payload.get("walk_forward")
        if not isinstance(walk_forward, Mapping):
            raise TypeError("gap availability walk-forward evidence is missing")
        if walk_forward.get("status") != "PASS":
            raise ValueError("gap availability walk-forward evidence is not PASS")
        if int(walk_forward.get("event_leakage_count") or 0):
            raise ValueError("gap availability artifact contains event leakage")
        strata = payload.get("strata")
        if not isinstance(strata, Mapping) or "GLOBAL" not in strata:
            raise ValueError("gap availability artifact has no GLOBAL stratum")
        return cls(payload=dict(payload))

    @property
    def artifact_hash(self) -> str:
        return str(self.payload["artifact_hash"])

    @property
    def training_end(self) -> datetime:
        return _datetime(self.payload["training_end"])

    @property
    def maximum_supported_gap_ms(self) -> int:
        method = self.payload.get("method")
        method = method if isinstance(method, Mapping) else {}
        return int(method.get("maximum_supported_gap_ms") or 0)

    def forecast(
        self,
        *,
        decision_ts: datetime,
        gap_ms: int,
        requested_size: Decimal | str | float,
        category: str = "GLOBAL",
        price_value: Decimal | str | float = Decimal("0.5"),
        tte_bucket: str = "UNKNOWN",
        liquidity_regime: str = "UNKNOWN",
        minimum_stratum_samples: int = 50,
    ) -> GapAvailabilityForecast:
        decision = _datetime(decision_ts)
        gap = max(0, int(gap_ms))
        requested = qty(requested_size)
        if decision <= self.training_end:
            return self._abstain(
                gap,
                requested,
                "ABSTAIN_CALIBRATION_NOT_POINT_IN_TIME_AVAILABLE",
            )
        if gap <= 0 or gap > self.maximum_supported_gap_ms:
            return self._abstain(
                gap,
                requested,
                "ABSTAIN_GAP_OUTSIDE_TRAINED_SUPPORT",
            )
        price_bucket = _price_bucket(Decimal(str(price_value)))
        category_text = str(category or "GLOBAL").strip().upper()
        tte_text = str(tte_bucket or "UNKNOWN").strip().upper()
        liquidity_text = str(liquidity_regime or "UNKNOWN").strip().upper()
        keys = (
            f"CATEGORY:{category_text}|PRICE:{price_bucket}|TTE:{tte_text}|LIQUIDITY:{liquidity_text}",
            f"CATEGORY:{category_text}|PRICE:{price_bucket}|LIQUIDITY:{liquidity_text}",
            f"PRICE:{price_bucket}|LIQUIDITY:{liquidity_text}",
            f"LIQUIDITY:{liquidity_text}",
            "GLOBAL",
        )
        strata = self.payload["strata"]
        selected_key = "GLOBAL"
        selected: Mapping[str, Any] = strata["GLOBAL"]
        for key in keys:
            candidate = strata.get(key)
            if not isinstance(candidate, Mapping):
                continue
            if key != "GLOBAL" and int(candidate.get("sample_count") or 0) < max(
                1, int(minimum_stratum_samples)
            ):
                continue
            selected_key = key
            selected = candidate
            break
        point = _point_for_gap(selected, gap)
        if point is None:
            return self._abstain(
                gap,
                requested,
                "ABSTAIN_STRATUM_GAP_UNSUPPORTED",
            )
        samples = int(point.get("sample_count") or 0)
        if samples < max(1, int(minimum_stratum_samples)):
            return self._abstain(
                gap,
                requested,
                "ABSTAIN_INSUFFICIENT_DOMAIN_SAMPLES",
            )
        probability = _probability(point.get("p_executable") or 0)
        conditional_fraction = _probability(
            point.get("conditional_available_fraction") or 0
        )
        conditional_size = qty(requested * conditional_fraction)
        expected_size = qty(probability * conditional_size)
        quantiles = point.get("price_error_quantiles")
        quantiles = quantiles if isinstance(quantiles, Mapping) else {}
        promotion = bool(self.payload.get("promotion_allowed"))
        return GapAvailabilityForecast(
            gap_ms=gap,
            p_executable=probability,
            conditional_available_fraction=conditional_fraction,
            expected_available_size=conditional_size,
            expected_executable_size=expected_size,
            price_error_quantiles={
                str(key): Decimal(str(value)) for key, value in quantiles.items()
            },
            stratum_key=selected_key,
            sample_count=samples,
            executable_count=int(point.get("executable_count") or 0),
            maximum_supported_gap_ms=self.maximum_supported_gap_ms,
            model_version=str(self.payload["model_version"]),
            artifact_hash=self.artifact_hash,
            evidence_scope=str(self.payload["evidence_scope"]),
            domain_status=(
                "CALIBRATED_PROMOTED"
                if promotion
                else "ARCHIVE_MASKING_PROXY_TRANSFER_UNVALIDATED"
            ),
            promotion_allowed=promotion,
        )

    def _abstain(
        self,
        gap_ms: int,
        requested_size: Decimal,
        reason: str,
    ) -> GapAvailabilityForecast:
        return GapAvailabilityForecast(
            gap_ms=gap_ms,
            p_executable=Decimal(0),
            conditional_available_fraction=Decimal(0),
            expected_available_size=Decimal(0),
            expected_executable_size=Decimal(0),
            price_error_quantiles={},
            stratum_key="NONE",
            sample_count=0,
            executable_count=0,
            maximum_supported_gap_ms=self.maximum_supported_gap_ms,
            model_version=str(self.payload["model_version"]),
            artifact_hash=self.artifact_hash,
            evidence_scope=str(self.payload["evidence_scope"]),
            domain_status=reason,
            promotion_allowed=bool(self.payload.get("promotion_allowed")),
        )

    def readiness(self) -> dict[str, Any]:
        walk_forward = self.payload.get("walk_forward")
        walk_forward = walk_forward if isinstance(walk_forward, Mapping) else {}
        return {
            "available": True,
            "model_version": self.payload["model_version"],
            "artifact_hash": self.artifact_hash,
            "training_end": self.payload["training_end"],
            "evidence_scope": self.payload["evidence_scope"],
            "maximum_supported_gap_ms": self.maximum_supported_gap_ms,
            "masking_horizons_ms": list(
                (self.payload.get("method") or {}).get("masking_horizons_ms") or []
            ),
            "walk_forward_status": walk_forward.get("status"),
            "fold_count": int(walk_forward.get("fold_count") or 0),
            "event_leakage_count": int(walk_forward.get("event_leakage_count") or 0),
            "promotion_allowed": bool(self.payload.get("promotion_allowed")),
        }


def load_default_gap_availability_artifact() -> GapAvailabilityArtifact | None:
    try:
        return GapAvailabilityArtifact.load(DEFAULT_ARTIFACT_PATH)
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return None


def _point_for_gap(stratum: Mapping[str, Any], gap_ms: int) -> Mapping[str, Any] | None:
    points = stratum.get("points")
    if not isinstance(points, list):
        return None
    normalized = sorted(
        (
            (int(point.get("gap_ms") or 0), point)
            for point in points
            if isinstance(point, Mapping) and int(point.get("gap_ms") or 0) > 0
        ),
        key=lambda item: item[0],
    )
    for supported_gap, point in normalized:
        if gap_ms <= supported_gap:
            return point
    return None


def _price_bucket(value: Decimal) -> str:
    if value < Decimal("0.1"):
        return "P00_10"
    if value < Decimal("0.3"):
        return "P10_30"
    if value < Decimal("0.7"):
        return "P30_70"
    if value < Decimal("0.9"):
        return "P70_90"
    return "P90_100"


def _probability(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    return min(Decimal(1), max(Decimal(0), parsed))


def _datetime(value: datetime | str) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("gap availability timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def payload_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()
