"""Versioned Maker time-to-fill forecasts for PML2 research replay."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any

from .contracts import RawOrderSide, canonical_value

SCHEMA_VERSION = "pml2_maker_survival_v1"
DEFAULT_ARTIFACT_PATH = (
    Path(__file__).resolve().parents[3]
    / "config"
    / "execution"
    / "pml2_maker_survival.v1.json"
)


@dataclass(frozen=True)
class MakerSurvivalForecast:
    horizon_seconds: int
    fill_probability: Decimal
    survival_probability: Decimal
    conditional_fill_fraction: Decimal
    stratum_key: str
    sample_count: int
    event_count: int
    model_version: str
    artifact_hash: str
    evidence_scope: str
    domain_status: str
    authenticated_trial_count: int
    authenticated_promotion_allowed: bool
    raw_stratum_probability: Decimal
    global_prior_probability: Decimal
    stratum_weight: Decimal

    def as_dict(self) -> dict[str, Any]:
        return canonical_value(self.__dict__)


@dataclass(frozen=True)
class MakerSurvivalArtifact:
    payload: Mapping[str, Any]

    @classmethod
    def load(cls, path: Path | str = DEFAULT_ARTIFACT_PATH) -> MakerSurvivalArtifact:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise TypeError("Maker survival artifact must be an object")
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("unsupported Maker survival artifact schema")
        expected = str(payload.get("artifact_hash") or "")
        body = dict(payload)
        body.pop("artifact_hash", None)
        actual = _payload_hash(body)
        if not expected or expected != actual:
            raise ValueError("Maker survival artifact hash mismatch")
        split = payload.get("walk_forward")
        if not isinstance(split, Mapping) or split.get("status") != "PASS":
            raise ValueError("Maker survival walk-forward evidence is not PASS")
        if int(split.get("event_leakage_count") or 0) != 0:
            raise ValueError("Maker survival artifact contains event leakage")
        curves = payload.get("curves")
        if not isinstance(curves, Mapping) or "GLOBAL" not in curves:
            raise ValueError("Maker survival artifact has no global curve")
        return cls(payload=dict(payload))

    @property
    def artifact_hash(self) -> str:
        return str(self.payload["artifact_hash"])

    @property
    def training_end(self) -> datetime:
        return _datetime(self.payload["training_end"])

    def forecast(
        self,
        *,
        decision_ts: datetime,
        horizon_seconds: int,
        category: str,
        side: RawOrderSide | str,
        quote_position: str,
        queue_bucket: str,
        minimum_stratum_samples: int = 20,
    ) -> MakerSurvivalForecast:
        decision = _datetime(decision_ts)
        horizon = max(1, int(horizon_seconds))
        authenticated = self.payload.get("authenticated_holdout")
        authenticated = authenticated if isinstance(authenticated, Mapping) else {}
        if decision <= self.training_end:
            return MakerSurvivalForecast(
                horizon_seconds=horizon,
                fill_probability=Decimal(0),
                survival_probability=Decimal(1),
                conditional_fill_fraction=Decimal(0),
                stratum_key="NONE",
                sample_count=0,
                event_count=0,
                model_version=str(self.payload["model_version"]),
                artifact_hash=self.artifact_hash,
                evidence_scope=str(self.payload["evidence_scope"]),
                domain_status="CALIBRATION_NOT_POINT_IN_TIME_AVAILABLE",
                authenticated_trial_count=int(authenticated.get("sample_count") or 0),
                authenticated_promotion_allowed=bool(
                    authenticated.get("promotion_allowed")
                ),
                raw_stratum_probability=Decimal(0),
                global_prior_probability=Decimal(0),
                stratum_weight=Decimal(0),
            )
        side_text = side.value if isinstance(side, RawOrderSide) else str(side).upper()
        category_text = str(category or "GLOBAL").strip().upper()
        position_text = str(quote_position or "AT_BEST").strip().upper()
        queue_text = str(queue_bucket or "UNKNOWN").strip().upper()
        candidate_keys = (
            f"CATEGORY:{category_text}|SIDE:{side_text}|POSITION:{position_text}|QUEUE:{queue_text}",
            f"CATEGORY:{category_text}|SIDE:{side_text}|POSITION:{position_text}",
            f"SIDE:{side_text}|POSITION:{position_text}",
            "GLOBAL",
        )
        curves = self.payload["curves"]
        selected_key = "GLOBAL"
        selected: Mapping[str, Any] = curves["GLOBAL"]
        for key in candidate_keys:
            value = curves.get(key)
            if not isinstance(value, Mapping):
                continue
            if (
                int(value.get("sample_count") or 0)
                < max(1, int(minimum_stratum_samples))
                and key != "GLOBAL"
            ):
                continue
            selected_key = key
            selected = value
            break
        points = selected.get("points")
        if not isinstance(points, list) or not points:
            raise ValueError(f"Maker survival curve {selected_key} has no points")
        raw_probability, sample_count, event_count = _interpolate_curve(points, horizon)
        global_probability, _, _ = _interpolate_curve(
            curves["GLOBAL"]["points"], horizon
        )
        if selected_key == "GLOBAL":
            weight = Decimal(1)
        else:
            method = self.payload.get("method")
            method = method if isinstance(method, Mapping) else {}
            prior_strength = max(
                Decimal(1),
                Decimal(str(method.get("hierarchical_prior_strength") or 50)),
            )
            weight = Decimal(sample_count) / (Decimal(sample_count) + prior_strength)
        probability = _probability(
            weight * raw_probability + (Decimal(1) - weight) * global_probability
        )
        raw_fraction = _interpolate_fraction(points, horizon)
        global_fraction = _interpolate_fraction(curves["GLOBAL"]["points"], horizon)
        conditional_fraction = (
            Decimal(0)
            if probability <= 0
            else _probability(
                (
                    weight * raw_probability * raw_fraction
                    + (Decimal(1) - weight) * global_probability * global_fraction
                )
                / probability
            )
        )
        return MakerSurvivalForecast(
            horizon_seconds=horizon,
            fill_probability=probability,
            survival_probability=Decimal(1) - probability,
            conditional_fill_fraction=conditional_fraction,
            stratum_key=selected_key,
            sample_count=sample_count,
            event_count=event_count,
            model_version=str(self.payload["model_version"]),
            artifact_hash=self.artifact_hash,
            evidence_scope=str(self.payload["evidence_scope"]),
            domain_status=(
                "AUTHENTICATED_CALIBRATED"
                if bool(authenticated.get("promotion_allowed"))
                else "ORDERFILLED_PROXY_CALIBRATED_TRANSFER_UNVALIDATED"
            ),
            authenticated_trial_count=int(authenticated.get("sample_count") or 0),
            authenticated_promotion_allowed=bool(
                authenticated.get("promotion_allowed")
            ),
            raw_stratum_probability=raw_probability,
            global_prior_probability=global_probability,
            stratum_weight=weight,
        )

    def readiness(self) -> dict[str, Any]:
        authenticated = self.payload.get("authenticated_holdout")
        authenticated = authenticated if isinstance(authenticated, Mapping) else {}
        walk_forward = self.payload.get("walk_forward")
        walk_forward = walk_forward if isinstance(walk_forward, Mapping) else {}
        return {
            "available": True,
            "model_version": self.payload["model_version"],
            "artifact_hash": self.artifact_hash,
            "training_end": self.payload["training_end"],
            "evidence_scope": self.payload["evidence_scope"],
            "walk_forward_status": walk_forward.get("status"),
            "fold_count": int(walk_forward.get("fold_count") or 0),
            "event_leakage_count": int(walk_forward.get("event_leakage_count") or 0),
            "authenticated_trial_count": int(authenticated.get("sample_count") or 0),
            "authenticated_outcomes": dict(authenticated.get("outcome_counts") or {}),
            "authenticated_promotion_allowed": bool(
                authenticated.get("promotion_allowed")
            ),
            "authenticated_conclusion": authenticated.get("calibration_conclusion"),
        }

    def conditional_fill_fraction(
        self,
        *,
        decision_ts: datetime,
        horizon_seconds: int,
        category: str,
        side: RawOrderSide | str,
        quote_position: str,
        queue_bucket: str,
        minimum_stratum_samples: int = 20,
    ) -> Decimal:
        """Return E[filled fraction | any fill] for a modeled Maker estimate."""

        forecast = self.forecast(
            decision_ts=decision_ts,
            horizon_seconds=horizon_seconds,
            category=category,
            side=side,
            quote_position=quote_position,
            queue_bucket=queue_bucket,
            minimum_stratum_samples=minimum_stratum_samples,
        )
        return forecast.conditional_fill_fraction


def load_default_maker_survival_artifact() -> MakerSurvivalArtifact | None:
    try:
        return MakerSurvivalArtifact.load(DEFAULT_ARTIFACT_PATH)
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return None


def _interpolate_curve(
    points: list[Any], horizon_seconds: int
) -> tuple[Decimal, int, int]:
    normalized = sorted(
        (
            (
                int(point["horizon_seconds"]),
                Decimal(str(point["fill_probability"])),
                int(point.get("sample_count") or 0),
                int(point.get("event_count") or 0),
            )
            for point in points
            if isinstance(point, Mapping)
        ),
        key=lambda item: item[0],
    )
    if not normalized:
        raise ValueError("Maker survival curve contains no valid points")
    target = int(horizon_seconds)
    if target <= normalized[0][0]:
        first_horizon, first_probability, samples, events = normalized[0]
        scaled = first_probability * Decimal(target) / Decimal(first_horizon)
        return _probability(scaled), samples, events
    for left, right in pairwise(normalized):
        if target > right[0]:
            continue
        span = Decimal(right[0] - left[0])
        weight = Decimal(target - left[0]) / span
        value = left[1] + (right[1] - left[1]) * weight
        return _probability(value), right[2], right[3]
    final = normalized[-1]
    return _probability(final[1]), final[2], final[3]


def _probability(value: Decimal) -> Decimal:
    return min(Decimal(1), max(Decimal(0), value))


def _interpolate_fraction(points: list[Any], horizon_seconds: int) -> Decimal:
    normalized = sorted(
        (
            (
                int(point["horizon_seconds"]),
                Decimal(str(point.get("conditional_fill_fraction") or 0)),
            )
            for point in points
            if isinstance(point, Mapping)
        ),
        key=lambda item: item[0],
    )
    if not normalized:
        return Decimal(0)
    target = int(horizon_seconds)
    if target <= normalized[0][0]:
        return _probability(normalized[0][1])
    for left, right in pairwise(normalized):
        if target > right[0]:
            continue
        span = Decimal(right[0] - left[0])
        weight = Decimal(target - left[0]) / span
        return _probability(left[1] + (right[1] - left[1]) * weight)
    return _probability(normalized[-1][1])


def _datetime(value: datetime | str) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Maker survival timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _payload_hash(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()
