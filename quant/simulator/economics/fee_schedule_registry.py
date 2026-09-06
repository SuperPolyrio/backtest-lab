"""Effective-dated fee schedule contract shared by live paper and replay."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from .fee_rounding import FEE_ROUNDING_POLICY, FEE_ROUNDING_UNIT


@dataclass(frozen=True)
class FeeSchedule:
    schedule_id: str
    asset_id: str
    condition_id: str
    effective_from: datetime
    effective_until: datetime | None
    platform_fee_rate: Decimal
    platform_fee_exponent: Decimal = Decimal(1)
    platform_taker_only: bool = True
    builder_code: str | None = None
    builder_taker_fee_bps: int = 0
    builder_maker_fee_bps: int = 0
    rounding_unit: Decimal = FEE_ROUNDING_UNIT
    rounding_policy: str = FEE_ROUNDING_POLICY
    economics_regime_id: str | None = None
    source: str = "UNSPECIFIED"

    def __post_init__(self) -> None:
        if not self.schedule_id:
            raise ValueError("fee schedule id is required")
        if not self.asset_id or not self.condition_id:
            raise ValueError("fee schedule asset and condition are required")
        if self.effective_from.tzinfo is None:
            raise ValueError("fee schedule effective_from must be timezone-aware")
        if (
            self.effective_until is not None
            and self.effective_until <= self.effective_from
        ):
            raise ValueError("fee schedule effective_until must follow effective_from")
        if self.platform_fee_rate < 0:
            raise ValueError("platform fee rate must be non-negative")
        if self.platform_fee_rate > 0 and self.platform_fee_exponent < 1:
            raise ValueError("fee-enabled schedules require exponent >= 1")
        if self.builder_taker_fee_bps < 0 or self.builder_taker_fee_bps > 100:
            raise ValueError("builder taker fee must be within 0..100 bps")
        if self.builder_maker_fee_bps < 0 or self.builder_maker_fee_bps > 50:
            raise ValueError("builder maker fee must be within 0..50 bps")
        if self.rounding_unit <= 0:
            raise ValueError("fee rounding unit must be positive")

    def applies_at(self, at: datetime) -> bool:
        return self.effective_from <= at and (
            self.effective_until is None or at < self.effective_until
        )

    @property
    def regime_id(self) -> str:
        return self.economics_regime_id or self.schedule_id


class FeeScheduleRegistry:
    """Idempotent effective-dated schedule registry for deterministic replay."""

    def __init__(self, schedules: Iterable[FeeSchedule] = ()) -> None:
        self._by_id: dict[str, FeeSchedule] = {}
        self._by_asset: dict[str, list[FeeSchedule]] = {}
        for schedule in schedules:
            self.register(schedule)

    def register(self, schedule: FeeSchedule) -> FeeSchedule:
        existing = self._by_id.get(schedule.schedule_id)
        if existing is not None:
            if existing != schedule:
                raise ValueError(f"fee schedule id collision: {schedule.schedule_id}")
            return existing
        history = self._by_asset.setdefault(schedule.asset_id, [])
        history.append(schedule)
        history.sort(key=lambda item: (item.effective_from, item.schedule_id))
        self._by_id[schedule.schedule_id] = schedule
        return schedule

    def resolve(self, asset_id: str, *, at: datetime) -> FeeSchedule:
        matches = [
            item
            for item in self._by_asset.get(str(asset_id), ())
            if item.applies_at(at)
        ]
        if not matches:
            raise LookupError(f"no fee schedule for {asset_id} at {at.isoformat()}")
        return matches[-1]

    def history(self, asset_id: str) -> tuple[FeeSchedule, ...]:
        return tuple(self._by_asset.get(str(asset_id), ()))


def fee_schedule_id(
    *,
    asset_id: str,
    condition_id: str,
    effective_from: datetime,
    platform_fee_rate: Decimal,
    platform_fee_exponent: Decimal,
    platform_taker_only: bool,
    source: str,
) -> str:
    normalized = effective_from.astimezone(timezone.utc).isoformat()
    payload = {
        "asset_id": str(asset_id),
        "condition_id": str(condition_id),
        "effective_from": normalized,
        "platform_fee_exponent": format(Decimal(platform_fee_exponent), "f"),
        "platform_fee_rate": format(Decimal(platform_fee_rate), "f"),
        "platform_taker_only": bool(platform_taker_only),
        "source": str(source),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return f"fee-schedule:{digest}"
