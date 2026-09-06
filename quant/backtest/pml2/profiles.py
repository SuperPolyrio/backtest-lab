"""Effective-dated profile registry for PML2 replay."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any

from .contracts import BookValidityMode, canonical_hash, canonical_value

REGISTRY_PATH = (
    Path(__file__).resolve().parents[3]
    / "config"
    / "execution"
    / "pml2_profiles.v1.json"
)


@dataclass(frozen=True)
class Pml2Profile:
    name: str
    schema_version: str
    effective_from: datetime
    calibration_source: str
    training_start: datetime
    training_end: datetime
    depth_haircut: Decimal
    positive_replenishment_fraction: Decimal
    queue_ahead_fraction: Decimal
    cancel_ahead_fraction: Decimal
    book_validity_mode: BookValidityMode
    max_book_age_ms: int
    feed_latency_ms: int
    entry_latency_ms: int
    cancel_latency_ms: int
    response_latency_ms: int
    venue_delay_ms: int
    trade_delta_match_window_ms: int
    mirror_size_tolerance: Decimal
    mirror_time_tolerance_ms: int
    mirror_policy: str
    source_priority: tuple[str, ...]
    maker_queue_model: str
    max_order_to_visible_depth_ratio: Decimal

    @property
    def profile_hash(self) -> str:
        return canonical_hash(self)

    def as_dict(self) -> dict[str, Any]:
        return canonical_value(
            {
                **asdict(self),
                "profile_hash": self.profile_hash,
                "uses_lob_data": True,
                "requires_future_trade": False,
                "residual_scope": "CONDITION_ECONOMIC_LEVEL",
            }
        )

    def validate_calibration_for(self, decision_ts: datetime) -> None:
        observed = _datetime(decision_ts, "decision_ts")
        if self.training_end >= observed:
            raise ValueError(
                "profile calibration training_end must precede decision_ts"
            )


def _datetime(value: datetime | str, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _fraction(value: Any, field_name: str) -> Decimal:
    parsed = Decimal(str(value))
    if not Decimal(0) <= parsed <= Decimal(1):
        raise ValueError(f"{field_name} must be within [0, 1]")
    return parsed


def _nonnegative_int(value: Any, field_name: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{field_name} cannot be negative")
    return parsed


@lru_cache(maxsize=1)
def load_pml2_profile_registry() -> dict[str, Pml2Profile]:
    payload = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    schema_version = str(payload["schema_version"])
    effective_from = _datetime(payload["effective_from"], "effective_from")
    calibration = payload.get("calibration")
    if not isinstance(calibration, Mapping):
        raise TypeError("pml2 calibration registry is missing")
    profiles = payload.get("profiles")
    if not isinstance(profiles, Mapping):
        raise TypeError("pml2 profile registry is missing profiles")
    result: dict[str, Pml2Profile] = {}
    for raw_name, raw in profiles.items():
        if not isinstance(raw, Mapping):
            raise TypeError(f"invalid PML2 profile {raw_name}")
        name = str(raw_name).lower()
        result[name] = Pml2Profile(
            name=name,
            schema_version=schema_version,
            effective_from=effective_from,
            calibration_source=str(calibration["source"]),
            training_start=_datetime(calibration["training_start"], "training_start"),
            training_end=_datetime(calibration["training_end"], "training_end"),
            depth_haircut=_fraction(raw["depth_haircut"], "depth_haircut"),
            positive_replenishment_fraction=_fraction(
                raw["positive_replenishment_fraction"],
                "positive_replenishment_fraction",
            ),
            queue_ahead_fraction=_fraction(
                raw["queue_ahead_fraction"], "queue_ahead_fraction"
            ),
            cancel_ahead_fraction=_fraction(
                raw["cancel_ahead_fraction"], "cancel_ahead_fraction"
            ),
            book_validity_mode=BookValidityMode(
                str(raw.get("book_validity_mode", BookValidityMode.MAX_AGE.value))
                .strip()
                .upper()
            ),
            max_book_age_ms=_nonnegative_int(
                raw["max_book_age_ms"], "max_book_age_ms"
            ),
            feed_latency_ms=_nonnegative_int(
                raw["feed_latency_ms"], "feed_latency_ms"
            ),
            entry_latency_ms=_nonnegative_int(
                raw["entry_latency_ms"], "entry_latency_ms"
            ),
            cancel_latency_ms=_nonnegative_int(
                raw["cancel_latency_ms"], "cancel_latency_ms"
            ),
            response_latency_ms=_nonnegative_int(
                raw["response_latency_ms"], "response_latency_ms"
            ),
            venue_delay_ms=_nonnegative_int(
                raw["venue_delay_ms"], "venue_delay_ms"
            ),
            trade_delta_match_window_ms=_nonnegative_int(
                raw["trade_delta_match_window_ms"],
                "trade_delta_match_window_ms",
            ),
            mirror_size_tolerance=_fraction(
                raw["mirror_size_tolerance"], "mirror_size_tolerance"
            ),
            mirror_time_tolerance_ms=_nonnegative_int(
                raw["mirror_time_tolerance_ms"], "mirror_time_tolerance_ms"
            ),
            mirror_policy=str(raw["mirror_policy"]),
            source_priority=tuple(str(item) for item in raw["source_priority"]),
            maker_queue_model=str(raw["maker_queue_model"]),
            max_order_to_visible_depth_ratio=_positive_decimal(
                raw["max_order_to_visible_depth_ratio"],
                "max_order_to_visible_depth_ratio",
            ),
        )
    required = {"strict", "realistic", "optimistic"}
    if set(result) != required:
        raise ValueError(f"PML2 profile registry must contain {sorted(required)}")
    return result


def _positive_decimal(value: Any, field_name: str) -> Decimal:
    parsed = Decimal(str(value))
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed


def get_pml2_profile(name: str | None = None) -> Pml2Profile:
    key = str(name or "realistic").strip().lower()
    try:
        return load_pml2_profile_registry()[key]
    except KeyError as exc:
        raise ValueError(f"unknown PML2 profile: {name!r}") from exc


def list_pml2_profiles() -> list[dict[str, Any]]:
    return [
        profile.as_dict()
        for profile in load_pml2_profile_registry().values()
    ]
