"""Single authoritative per-fill platform and builder fee engine."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

from .builder_fee_engine import calculate_builder_fee
from .fee_rounding import round_fee
from .fee_schedule_registry import FeeSchedule


class LiquidityRole(str, Enum):
    TAKER = "TAKER"
    MAKER = "MAKER"


@dataclass(frozen=True)
class FeeCharge:
    fee_charge_id: str
    fill_id: str
    asset_id: str
    condition_id: str
    liquidity_role: str
    price: Decimal
    shares: Decimal
    platform_fee_rate: Decimal
    platform_fee_exponent: Decimal
    platform_fee: Decimal
    builder_code: str | None
    builder_fee_rate_bps: int
    builder_fee: Decimal
    total_fee: Decimal
    rounding_policy: str
    fee_schedule_id: str
    economics_regime_id: str
    source: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class FeeEngine:
    """Calculate and identify one immutable fee charge per fill."""

    @staticmethod
    def calculate(
        *,
        fill_id: str,
        schedule: FeeSchedule,
        liquidity_role: LiquidityRole | str,
        price: Decimal,
        shares: Decimal,
    ) -> FeeCharge:
        role = _liquidity_role(liquidity_role)
        price = Decimal(price)
        shares = Decimal(shares)
        if not fill_id:
            raise ValueError("fill id is required for fee calculation")
        if price < 0 or price > 1:
            raise ValueError("fill price must be within [0, 1]")
        if shares < 0:
            raise ValueError("fill shares must be non-negative")

        platform_rate = schedule.platform_fee_rate
        if role is LiquidityRole.MAKER and schedule.platform_taker_only:
            platform_rate = Decimal(0)
        base = price * (Decimal(1) - price)
        raw_platform = shares * platform_rate * (base**schedule.platform_fee_exponent)
        platform_fee = round_fee(
            raw_platform,
            unit=schedule.rounding_unit,
        )
        builder_bps, builder_fee = calculate_builder_fee(
            schedule=schedule,
            liquidity_role=role.value,
            price=price,
            shares=shares,
        )
        total_fee = platform_fee + builder_fee
        payload = {
            "builder_fee": format(builder_fee, "f"),
            "fill_id": fill_id,
            "platform_fee": format(platform_fee, "f"),
            "role": role.value,
            "schedule_id": schedule.schedule_id,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        return FeeCharge(
            fee_charge_id=f"fee-charge:{digest}",
            fill_id=fill_id,
            asset_id=schedule.asset_id,
            condition_id=schedule.condition_id,
            liquidity_role=role.value,
            price=price,
            shares=shares,
            platform_fee_rate=platform_rate,
            platform_fee_exponent=schedule.platform_fee_exponent,
            platform_fee=platform_fee,
            builder_code=schedule.builder_code,
            builder_fee_rate_bps=builder_bps,
            builder_fee=builder_fee,
            total_fee=total_fee,
            rounding_policy=schedule.rounding_policy,
            fee_schedule_id=schedule.schedule_id,
            economics_regime_id=schedule.regime_id,
            source=schedule.source,
        )


def maximum_order_fees(
    *,
    schedule: FeeSchedule,
    liquidity_role: LiquidityRole | str,
    limit_price: Decimal,
    shares: Decimal | None = None,
    quote_notional: Decimal | None = None,
) -> FeeCharge:
    """Return a conservative all-in reservation fee bound for an order."""

    if (shares is None) == (quote_notional is None):
        raise ValueError("provide exactly one of shares or quote_notional")
    limit_price = Decimal(limit_price)
    if limit_price <= 0 or limit_price > 1:
        raise ValueError("limit price must be within (0, 1]")
    if quote_notional is not None:
        notional = max(Decimal(0), Decimal(quote_notional))
        # For official exponents >= 1, quote * rate is a conservative upper bound.
        platform = (
            notional * schedule.platform_fee_rate
            if _liquidity_role(liquidity_role) is LiquidityRole.TAKER
            or not schedule.platform_taker_only
            else Decimal(0)
        )
        platform = round_fee(platform, unit=schedule.rounding_unit)
        role = _liquidity_role(liquidity_role)
        builder_bps = (
            schedule.builder_taker_fee_bps
            if role is LiquidityRole.TAKER
            else schedule.builder_maker_fee_bps
        )
        builder = round_fee(
            notional * Decimal(builder_bps) / Decimal(10_000),
            unit=schedule.rounding_unit,
        )
        synthetic_shares = notional / limit_price
        return _reservation_charge(
            schedule=schedule,
            role=role,
            price=limit_price,
            shares=synthetic_shares,
            platform=platform,
            builder_bps=builder_bps,
            builder=builder,
        )

    assert shares is not None
    role = _liquidity_role(liquidity_role)
    shares = max(Decimal(0), Decimal(shares))
    fee_price = min(limit_price, Decimal("0.5"))
    platform_charge = FeeEngine.calculate(
        fill_id="ORDER_RESERVATION_MAXIMUM",
        schedule=schedule,
        liquidity_role=role,
        price=fee_price,
        shares=shares,
    )
    builder_bps = (
        schedule.builder_taker_fee_bps
        if role is LiquidityRole.TAKER
        else schedule.builder_maker_fee_bps
    )
    builder = round_fee(
        shares * limit_price * Decimal(builder_bps) / Decimal(10_000),
        unit=schedule.rounding_unit,
    )
    return _reservation_charge(
        schedule=schedule,
        role=role,
        price=limit_price,
        shares=shares,
        platform=platform_charge.platform_fee,
        builder_bps=builder_bps,
        builder=builder,
    )


def _reservation_charge(
    *,
    schedule: FeeSchedule,
    role: LiquidityRole,
    price: Decimal,
    shares: Decimal,
    platform: Decimal,
    builder_bps: int,
    builder: Decimal,
) -> FeeCharge:
    total = platform + builder
    digest = hashlib.sha256(
        f"reservation|{schedule.schedule_id}|{role.value}|{total}".encode()
    ).hexdigest()
    return FeeCharge(
        fee_charge_id=f"fee-reservation:{digest}",
        fill_id="ORDER_RESERVATION_MAXIMUM",
        asset_id=schedule.asset_id,
        condition_id=schedule.condition_id,
        liquidity_role=role.value,
        price=price,
        shares=shares,
        platform_fee_rate=schedule.platform_fee_rate,
        platform_fee_exponent=schedule.platform_fee_exponent,
        platform_fee=platform,
        builder_code=schedule.builder_code,
        builder_fee_rate_bps=builder_bps,
        builder_fee=builder,
        total_fee=total,
        rounding_policy=schedule.rounding_policy,
        fee_schedule_id=schedule.schedule_id,
        economics_regime_id=schedule.regime_id,
        source=schedule.source,
    )


def _liquidity_role(value: LiquidityRole | str) -> LiquidityRole:
    normalized = getattr(value, "value", value)
    return LiquidityRole(str(normalized).upper())
