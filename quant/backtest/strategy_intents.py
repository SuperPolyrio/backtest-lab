"""Strategy intent contracts for fill-first backtests.

Strategies should emit auditable order intents. Execution models consume those
intents and decide whether historical OrderFilled evidence can satisfy them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Literal, Mapping

from .runners.execution_replay import OrderIntent as ReplayOrderIntent
from .runners.execution_replay import TimeInForce, sequence_key


Q = Decimal("0.0000000001")
StrategyOrderSide = Literal["BUY_YES", "SELL_YES"]


@dataclass(frozen=True)
class StrategySignal:
    strategy_name: str
    strategy_version: str
    signal_index: int
    signal_x: int
    side: StrategyOrderSide
    signal_price: Decimal
    reason: str
    confidence: Decimal = Decimal("1")
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "strategy_version": self.strategy_version,
            "signal_index": int(self.signal_index),
            "signal_x": int(self.signal_x),
            "side": self.side,
            "signal_price": _decimal_text(self.signal_price),
            "reason": self.reason,
            "confidence": _decimal_text(self.confidence),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class StrategyOrderIntent:
    signal: StrategySignal
    order_id: str
    limit_price: Decimal
    size: Decimal
    submit_x: int
    time_in_force: TimeInForce = "GTC"
    expire_x: int | None = None
    cancel_x: int | None = None
    cancel_ack_x: int | None = None
    cancel_fail_x: int | None = None
    liquidity_cap_pct: Decimal = Decimal("100")
    fee_bps: Decimal = Decimal("0")
    rebate_bps: Decimal = Decimal("0")
    role: str = "maker"
    order_type: str = "post_only_limit"
    execution_model: str = "orderfilled_limit_replay"
    execution_profile: str = "optimistic"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def side(self) -> StrategyOrderSide:
        return self.signal.side

    @property
    def requested_notional(self) -> Decimal:
        return (self.size * self.limit_price).quantize(Q, rounding=ROUND_HALF_UP)

    def to_replay_order_intent(self) -> ReplayOrderIntent:
        return ReplayOrderIntent(
            side=self.side,
            limit_price=self.limit_price,
            size=self.size,
            time_in_force=self.time_in_force,
            submit_sequence=sequence_key(self.submit_x, 0, 0, f"{self.order_id}-submit"),
            expire_sequence=sequence_key(self.expire_x, 0, 0, f"{self.order_id}-expire") if self.expire_x is not None else None,
            cancel_sequence=sequence_key(self.cancel_x, 0, 0, f"{self.order_id}-cancel") if self.cancel_x is not None else None,
            cancel_ack_sequence=sequence_key(self.cancel_ack_x, 0, 0, f"{self.order_id}-cancel-ack") if self.cancel_ack_x is not None else None,
            cancel_fail_sequence=sequence_key(self.cancel_fail_x, 0, 0, f"{self.order_id}-cancel-fail") if self.cancel_fail_x is not None else None,
            liquidity_cap_pct=self.liquidity_cap_pct,
            fee_bps=self.fee_bps,
            rebate_bps=self.rebate_bps,
            order_id=self.order_id,
            order_role=self.role,
            execution_profile=self.execution_profile,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "side": self.side,
            "limit_price": _decimal_text(self.limit_price),
            "size": _decimal_text(self.size),
            "requested_notional": _decimal_text(self.requested_notional),
            "submit_x": int(self.submit_x),
            "time_in_force": self.time_in_force,
            "expire_x": int(self.expire_x) if self.expire_x is not None else None,
            "cancel_x": int(self.cancel_x) if self.cancel_x is not None else None,
            "cancel_ack_x": int(self.cancel_ack_x) if self.cancel_ack_x is not None else None,
            "cancel_fail_x": int(self.cancel_fail_x) if self.cancel_fail_x is not None else None,
            "liquidity_cap_pct": _decimal_text(self.liquidity_cap_pct),
            "fee_bps": _decimal_text(self.fee_bps),
            "rebate_bps": _decimal_text(self.rebate_bps),
            "role": self.role,
            "order_type": self.order_type,
            "execution_model": self.execution_model,
            "execution_profile": self.execution_profile,
            "signal": self.signal.as_dict(),
            "metadata": dict(self.metadata),
        }


def build_threshold_limit_intent(
    *,
    strategy_name: str,
    strategy_version: str,
    signal_index: int,
    signal_x: int,
    submit_x: int,
    side: StrategyOrderSide,
    signal_price: Decimal,
    limit_price: Decimal,
    target_notional: Decimal,
    order_id: str,
    reason: str,
    time_in_force: TimeInForce,
    liquidity_cap_pct: Decimal,
    role: str,
    order_type: str,
    fee_bps: Decimal = Decimal("0"),
    rebate_bps: Decimal = Decimal("0"),
    target_size: Decimal | None = None,
    expire_x: int | None = None,
    cancel_x: int | None = None,
    cancel_ack_x: int | None = None,
    cancel_fail_x: int | None = None,
    execution_model: str = "orderfilled_limit_replay",
    execution_profile: str = "optimistic",
    metadata: Mapping[str, Any] | None = None,
) -> StrategyOrderIntent:
    price = max(Decimal("0.0000000001"), Decimal(str(limit_price))).quantize(Q, rounding=ROUND_HALF_UP)
    if target_size is None:
        size = (max(Decimal("0"), Decimal(str(target_notional))) / price).quantize(Q, rounding=ROUND_HALF_UP)
    else:
        size = max(Decimal("0"), Decimal(str(target_size))).quantize(Q, rounding=ROUND_HALF_UP)
    signal = StrategySignal(
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        signal_index=int(signal_index),
        signal_x=int(signal_x),
        side=side,
        signal_price=Decimal(str(signal_price)).quantize(Q, rounding=ROUND_HALF_UP),
        reason=reason,
        metadata=metadata or {},
    )
    return StrategyOrderIntent(
        signal=signal,
        order_id=order_id,
        limit_price=price,
        size=size,
        submit_x=int(submit_x),
        time_in_force=time_in_force,
        expire_x=int(expire_x) if expire_x is not None else None,
        cancel_x=int(cancel_x) if cancel_x is not None else None,
        cancel_ack_x=int(cancel_ack_x) if cancel_ack_x is not None else None,
        cancel_fail_x=int(cancel_fail_x) if cancel_fail_x is not None else None,
        liquidity_cap_pct=max(Decimal("0"), Decimal(str(liquidity_cap_pct))),
        fee_bps=max(Decimal("0"), Decimal(str(fee_bps))),
        rebate_bps=max(Decimal("0"), Decimal(str(rebate_bps))),
        role=role,
        order_type=order_type,
        execution_model=execution_model,
        execution_profile=str(execution_profile or "optimistic").lower(),
        metadata=metadata or {},
    )


def _decimal_text(value: Decimal) -> str:
    return format(Decimal(str(value)).normalize(), "f")
