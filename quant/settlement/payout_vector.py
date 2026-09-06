"""Generalized payout vectors including Unknown/50-50 outcomes."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class PayoutVector:
    condition_id: str
    payouts: Mapping[str, Decimal]
    resolution_source: str
    oracle_finalized_at: str

    def __post_init__(self) -> None:
        if not self.condition_id or not self.payouts:
            raise ValueError("condition_id and payouts are required")
        values = [Decimal(value) for value in self.payouts.values()]
        if any(value < 0 or value > 1 for value in values):
            raise ValueError("payout_per_share must be within [0, 1]")
        if sum(values, Decimal(0)) != Decimal(1):
            raise ValueError(
                "a complete mutually-exclusive payout vector must sum to 1"
            )

    def payout_for(self, asset_id: str) -> Decimal:
        return Decimal(self.payouts.get(str(asset_id), Decimal(0)))

    @property
    def truth_hash(self) -> str:
        payload = {
            "condition_id": self.condition_id,
            "payouts": {
                str(key): format(Decimal(value), "f")
                for key, value in sorted(self.payouts.items())
            },
            "resolution_source": self.resolution_source,
            "oracle_finalized_at": self.oracle_finalized_at,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
