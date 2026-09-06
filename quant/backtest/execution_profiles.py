"""Execution assumption profiles for OrderFilled-first backtests."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


PROFILE_NAMES = {"optimistic", "neutral", "realistic", "conservative", "stress"}
ROLE_NAMES = {"taker", "maker", "auto"}


@dataclass(frozen=True)
class ExecutionProfile:
    name: str
    order_role: str
    latency_blocks: int
    adverse_slippage_cents: Decimal
    fill_probability_haircut_pct: Decimal


def normalize_execution_profile(value: Any) -> str:
    name = str(value or "realistic").strip().lower()
    return name if name in PROFILE_NAMES else "realistic"


def normalize_order_role(value: Any) -> str:
    role = str(value or "taker").strip().lower()
    return role if role in ROLE_NAMES else "taker"


def profile_defaults(name: str) -> ExecutionProfile:
    normalized = normalize_execution_profile(name)
    if normalized == "optimistic":
        return ExecutionProfile(normalized, "taker", 0, Decimal("0"), Decimal("0"))
    if normalized == "neutral":
        return ExecutionProfile(normalized, "taker", 0, Decimal("0.0025"), Decimal("10"))
    if normalized == "conservative":
        return ExecutionProfile(normalized, "taker", 1, Decimal("0.015"), Decimal("50"))
    if normalized == "stress":
        return ExecutionProfile(normalized, "taker", 1, Decimal("0.02"), Decimal("50"))
    return ExecutionProfile("realistic", "taker", 0, Decimal("0.005"), Decimal("20"))


def normalize_adverse_slippage(value: Any, default: Decimal) -> Decimal:
    raw = max(Decimal("0"), Decimal(str(value if value not in {None, ""} else default)))
    # Public/API parameters are named "cents"; accept 1/2 as 1c/2c while
    # preserving existing normalized probability-unit values such as 0.005.
    if raw >= Decimal("1"):
        return (raw / Decimal("100")).quantize(Decimal("0.0000000001"))
    return raw


def effective_execution_profile(params: Any) -> ExecutionProfile:
    base = profile_defaults(getattr(params, "execution_profile", "realistic"))
    latency_value = getattr(params, "latency_blocks", None)
    haircut_value = getattr(params, "fill_probability_haircut_pct", None)
    return ExecutionProfile(
        name=base.name,
        order_role=normalize_order_role(getattr(params, "order_role", base.order_role)),
        latency_blocks=max(0, int(base.latency_blocks if latency_value in (None, "") else latency_value)),
        adverse_slippage_cents=normalize_adverse_slippage(
            getattr(params, "adverse_slippage_cents", base.adverse_slippage_cents),
            base.adverse_slippage_cents,
        ),
        fill_probability_haircut_pct=min(
            Decimal("100"),
            max(
                Decimal("0"),
                Decimal(str(base.fill_probability_haircut_pct if haircut_value in (None, "") else haircut_value)),
            ),
        ),
    )


def apply_probability_haircut(fill_probability: Decimal, profile: ExecutionProfile) -> Decimal:
    haircut = Decimal("1") - (profile.fill_probability_haircut_pct / Decimal("100"))
    return max(Decimal("0"), min(Decimal("100"), fill_probability * haircut))


def apply_adverse_slippage(price: Decimal, profile: ExecutionProfile, side: str) -> Decimal:
    slip = max(Decimal("0"), profile.adverse_slippage_cents)
    if slip <= 0:
        return price
    if str(side).upper().startswith("BUY"):
        return min(Decimal("0.9999999999"), price + slip)
    return max(Decimal("0"), price - slip)
