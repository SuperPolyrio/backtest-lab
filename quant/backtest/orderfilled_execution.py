"""Shared OrderFilled execution helpers.

This module owns the block-volume based OrderFilled execution approximation used
by the builtin engine and framework adapters.  Keeping the math in one place
prevents adapter paths from drifting away from the main backtest engine.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from .execution_profiles import apply_adverse_slippage, apply_probability_haircut, effective_execution_profile


Q = Decimal("0.0000000001")


def pct(numerator: Decimal, denominator: Decimal) -> Decimal:
    if not denominator:
        return Decimal("0")
    return (numerator / denominator * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP)


def bps_fraction(value: Any) -> Decimal:
    return max(Decimal("0"), Decimal(str(value or "0"))) / Decimal("10000")


def role_fee_bps(params: Any, role: str) -> Decimal:
    normalized = str(role or "").lower()
    if normalized == "maker" and getattr(params, "maker_fee_bps", None) is not None:
        return max(Decimal("0"), Decimal(str(getattr(params, "maker_fee_bps"))))
    if normalized == "taker" and getattr(params, "taker_fee_bps", None) is not None:
        return max(Decimal("0"), Decimal(str(getattr(params, "taker_fee_bps"))))
    return max(Decimal("0"), Decimal(str(getattr(params, "fee_bps", Decimal("0")) or 0)))


def role_rebate_bps(params: Any, role: str) -> Decimal:
    if str(role or "").lower() != "maker":
        return Decimal("0")
    return max(Decimal("0"), Decimal(str(getattr(params, "maker_rebate_bps", Decimal("0")) or 0)))


def fee_rebate_for_notional(params: Any, notional: Decimal, role: str) -> tuple[Decimal, Decimal]:
    safe_notional = max(Decimal("0"), Decimal(str(notional)))
    fee = (safe_notional * bps_fraction(role_fee_bps(params, role))).quantize(Q, rounding=ROUND_HALF_UP)
    rebate = (safe_notional * bps_fraction(role_rebate_bps(params, role))).quantize(Q, rounding=ROUND_HALF_UP)
    return fee, rebate


def execution_price(price: Decimal, params: Any, side: str) -> Decimal:
    fraction = bps_fraction(getattr(params, "slippage_bps", Decimal("0")))
    if side == "raw_entry":
        if fraction <= 0:
            return price
        return (price / (Decimal("1") + fraction)).quantize(Q, rounding=ROUND_HALF_UP)
    if side == "entry":
        return min(Decimal("0.9999999999"), price * (Decimal("1") + fraction)).quantize(Q, rounding=ROUND_HALF_UP)
    return max(Decimal("0"), price * (Decimal("1") - fraction)).quantize(Q, rounding=ROUND_HALF_UP)


def target_notional(params: Any) -> Decimal:
    target = max(Decimal("0"), Decimal(str(getattr(params, "position_size", "0"))))
    max_position = max(Decimal("0"), Decimal(str(getattr(params, "max_position_notional", "0"))))
    if max_position > 0:
        target = min(target, max_position)
    return target


def orderfilled_fill_decision(
    params: Any,
    point: Any,
    side: str,
    *,
    target_size: Decimal | None = None,
) -> dict[str, Any]:
    price = Decimal(str(point.price))
    profile = effective_execution_profile(params)
    role = profile.order_role
    profile_name = str(profile.name or "realistic").lower()
    exec_price = execution_price(price, params, "entry" if side.startswith("BUY") else "exit")
    exec_price = apply_adverse_slippage(exec_price, profile, side)
    requested_notional = target_notional(params)
    if target_size is not None:
        requested_size = max(Decimal("0"), Decimal(str(target_size)))
        requested_notional = (requested_size * max(exec_price, Q)).quantize(Q, rounding=ROUND_HALF_UP)
    else:
        requested_size = (requested_notional / max(exec_price, Q)).quantize(Q, rounding=ROUND_HALF_UP)

    cap_pct = max(Decimal("0"), Decimal(str(getattr(params, "liquidity_cap_pct", "100"))))
    min_fill_pct = max(Decimal("0"), Decimal(str(getattr(params, "min_fill_pct", "0"))))
    block_volume = max(Decimal("0"), Decimal(str(getattr(point, "volume", "0") or "0")))
    side_volume = _side_volume_for_order(point, side, role).quantize(Q, rounding=ROUND_HALF_UP)
    effective_volume = side_volume if side_volume > 0 else block_volume
    volume_basis = "side_volume" if side_volume > 0 else "block_volume"
    available_notional = (effective_volume * cap_pct / Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP)
    raw_fill_probability = min(Decimal("100"), pct(available_notional, requested_notional)) if requested_notional else Decimal("0")
    block_context_factor, block_context_reason, block_range_pct, vwap_dislocation_pct = _block_context_fill_factor(
        point,
        exec_price,
        profile_name,
        role,
    )
    trade_count_factor, trade_count_reason = _trade_count_fill_factor(
        int(getattr(point, "trade_count", 0) or 0),
        profile_name,
        role,
    )
    participation_pressure_factor, participation_pressure_reason = _participation_pressure_fill_factor(
        requested_notional,
        available_notional,
        profile_name,
        role,
    )
    modeled_fill_probability_pre_haircut = (
        raw_fill_probability
        * block_context_factor
        * trade_count_factor
        * participation_pressure_factor
    ).quantize(Q, rounding=ROUND_HALF_UP)
    fill_probability = apply_probability_haircut(raw_fill_probability, profile)
    fill_probability = min(fill_probability, modeled_fill_probability_pre_haircut)
    effective_available_notional = min(
        available_notional,
        (requested_notional * fill_probability / Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP),
    )
    expected_fill_notional = min(requested_notional, effective_available_notional).quantize(Q, rounding=ROUND_HALF_UP)
    expected_fill_size = (expected_fill_notional / exec_price).quantize(Q, rounding=ROUND_HALF_UP) if exec_price > 0 else Decimal("0")
    participation_rate = pct(requested_notional, available_notional) if available_notional else Decimal("0")
    empty = {
        "requested_notional": requested_notional,
        "filled_notional": Decimal("0"),
        "expected_fill_notional": expected_fill_notional,
        "actual_fill_notional": Decimal("0"),
        "fill_pct": Decimal("0"),
        "fill_probability": fill_probability,
        "size": Decimal("0"),
        "liquidity_cap_pct": cap_pct,
        "min_fill_pct": min_fill_pct,
        "partial_fill": False,
        "rejected": True,
        "fill_status": "REJECTED",
        "requested_size": requested_size,
        "expected_fill_size": expected_fill_size,
        "actual_fill_size": Decimal("0"),
        "filled_size": Decimal("0"),
        "unfilled_size": requested_size,
        "block_volume": block_volume,
        "trade_count": int(getattr(point, "trade_count", 0) or 0),
        "available_notional": available_notional,
        "block_volume_basis": volume_basis,
        "effective_block_volume": effective_volume,
        "side_volume": side_volume,
        "participation_rate": participation_rate,
        "execution_source": "orderfilled_volume",
        "execution_profile": profile.name,
        "order_role": role,
        "fill_probability_model": f"orderfilled_volume_context_{profile.name}_{role}",
        "fee_bps": role_fee_bps(params, role),
        "rebate_bps": role_rebate_bps(params, role),
        "fee_cost": Decimal("0"),
        "rebate": Decimal("0"),
        "rebate_cost": Decimal("0"),
        "slippage_cost": Decimal("0"),
        "execution_cost": Decimal("0"),
        "latency_blocks": profile.latency_blocks,
        "adverse_slippage_cents": profile.adverse_slippage_cents,
        "fill_probability_haircut_pct": profile.fill_probability_haircut_pct,
        "raw_fill_probability": raw_fill_probability,
        "modeled_fill_probability_pre_haircut": modeled_fill_probability_pre_haircut,
        "block_context_factor": block_context_factor,
        "block_context_reason": block_context_reason,
        "block_range_pct": block_range_pct,
        "vwap_dislocation_pct": vwap_dislocation_pct,
        "trade_count_factor": trade_count_factor,
        "trade_count_reason": trade_count_reason,
        "participation_pressure_factor": participation_pressure_factor,
        "participation_pressure_reason": participation_pressure_reason,
        "notes": ["no_orderfilled_volume"] if block_volume <= 0 else [],
    }
    if price <= 0 or exec_price <= 0 or requested_notional <= 0 or cap_pct <= 0:
        return empty
    if available_notional <= 0:
        return empty

    if getattr(params, "allow_partial_fill", True):
        filled_notional = min(requested_notional, effective_available_notional)
    else:
        if effective_available_notional < requested_notional:
            return {**empty, "notes": ["insufficient_orderfilled_volume_for_full_fill"]}
        filled_notional = requested_notional
    filled_notional = filled_notional.quantize(Q, rounding=ROUND_HALF_UP)
    filled_size = (filled_notional / exec_price).quantize(Q, rounding=ROUND_HALF_UP)
    min_size_from_pct = requested_size * min_fill_pct / Decimal("100")
    min_required_size = max(Decimal(str(getattr(params, "min_fill_size", Decimal("0")))), min_size_from_pct).quantize(Q, rounding=ROUND_HALF_UP)
    fill_pct = pct(filled_notional, requested_notional) if requested_notional else Decimal("0")
    if filled_size <= 0 or filled_size < min_required_size:
        return {
            **empty,
            "filled_notional": filled_notional,
            "actual_fill_notional": filled_notional,
            "fill_pct": fill_pct,
            "filled_size": filled_size,
            "actual_fill_size": filled_size,
            "unfilled_size": max(Decimal("0"), requested_size - filled_size),
            "notes": ["below_min_fill_threshold"],
        }
    fill_status = "FILLED" if filled_notional >= requested_notional else "PARTIAL"
    price_key = "entry_price" if side.startswith("BUY") else "exit_price"
    slippage_cost = ((exec_price - price) * filled_size).copy_abs().quantize(Q, rounding=ROUND_HALF_UP)
    fee_cost, rebate = fee_rebate_for_notional(params, filled_notional, role)
    return {
        "requested_notional": requested_notional,
        "filled_notional": filled_notional,
        "expected_fill_notional": expected_fill_notional,
        "actual_fill_notional": filled_notional,
        "fill_pct": fill_pct,
        "fill_probability": fill_probability,
        "size": filled_size,
        price_key: exec_price,
        "avg_fill_price": exec_price,
        "liquidity_cap_pct": cap_pct,
        "min_fill_pct": min_fill_pct,
        "partial_fill": fill_status == "PARTIAL",
        "rejected": False,
        "fill_status": fill_status,
        "requested_size": requested_size,
        "expected_fill_size": expected_fill_size,
        "actual_fill_size": filled_size,
        "filled_size": filled_size,
        "unfilled_size": max(Decimal("0"), requested_size - filled_size).quantize(Q, rounding=ROUND_HALF_UP),
        "block_volume": block_volume,
        "trade_count": int(getattr(point, "trade_count", 0) or 0),
        "available_notional": available_notional,
        "block_volume_basis": volume_basis,
        "effective_block_volume": effective_volume,
        "side_volume": side_volume,
        "participation_rate": participation_rate,
        "execution_source": "orderfilled_volume",
        "execution_profile": profile.name,
        "order_role": role,
        "fill_probability_model": f"orderfilled_volume_context_{profile.name}_{role}",
        "fee_bps": role_fee_bps(params, role),
        "rebate_bps": role_rebate_bps(params, role),
        "latency_blocks": profile.latency_blocks,
        "adverse_slippage_cents": profile.adverse_slippage_cents,
        "fill_probability_haircut_pct": profile.fill_probability_haircut_pct,
        "raw_fill_probability": raw_fill_probability,
        "modeled_fill_probability_pre_haircut": modeled_fill_probability_pre_haircut,
        "block_context_factor": block_context_factor,
        "block_context_reason": block_context_reason,
        "block_range_pct": block_range_pct,
        "vwap_dislocation_pct": vwap_dislocation_pct,
        "trade_count_factor": trade_count_factor,
        "trade_count_reason": trade_count_reason,
        "participation_pressure_factor": participation_pressure_factor,
        "participation_pressure_reason": participation_pressure_reason,
        "fee_cost": fee_cost,
        "rebate": rebate,
        "rebate_cost": rebate,
        "slippage_cost": slippage_cost,
        "execution_cost": fee_cost + slippage_cost - rebate,
        "notes": ["expected_fill_from_historical_orderfilled_volume"],
    }


def _side_volume_for_order(point: Any, side: str, role: str) -> Decimal:
    buy_volume = max(Decimal("0"), Decimal(str(getattr(point, "buy_volume", Decimal("0")) or 0)))
    sell_volume = max(Decimal("0"), Decimal(str(getattr(point, "sell_volume", Decimal("0")) or 0)))
    side_text = str(side or "").upper()
    normalized_role = str(role or "").lower()
    if side_text.startswith("BUY"):
        return sell_volume if normalized_role == "maker" else buy_volume
    return buy_volume if normalized_role == "maker" else sell_volume


def _block_context_fill_factor(
    point: Any,
    execution_price_value: Decimal,
    profile_name: str,
    role: str,
) -> tuple[Decimal, str, Decimal, Decimal]:
    if profile_name == "optimistic":
        return Decimal("1"), "optimistic_block_context", Decimal("0"), Decimal("0")
    vwap_value = getattr(point, "vwap_price", None)
    high_value = getattr(point, "high_price", None)
    low_value = getattr(point, "low_price", None)
    if vwap_value in (None, "") or high_value in (None, "") or low_value in (None, ""):
        return Decimal("1"), "missing_block_context", Decimal("0"), Decimal("0")
    vwap = max(Decimal("0"), Decimal(str(vwap_value)))
    high = max(Decimal("0"), Decimal(str(high_value)))
    low = max(Decimal("0"), Decimal(str(low_value)))
    exec_price = max(Decimal("0"), Decimal(str(execution_price_value or 0)))
    if vwap <= 0:
        return Decimal("1"), "missing_block_context", Decimal("0"), Decimal("0")
    block_range_pct = ((high - low).copy_abs() / vwap * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP)
    vwap_dislocation_pct = ((exec_price - vwap).copy_abs() / vwap * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP)
    range_threshold = Decimal("8")
    dislocation_threshold = Decimal("3")
    range_excess = max(Decimal("0"), block_range_pct - range_threshold) / range_threshold
    dislocation_excess = max(Decimal("0"), vwap_dislocation_pct - dislocation_threshold) / dislocation_threshold
    if range_excess <= 0 and dislocation_excess <= 0:
        return Decimal("1"), "stable_block_context", block_range_pct, vwap_dislocation_pct
    profile_weight = {
        "neutral": Decimal("0.20"),
        "realistic": Decimal("0.35"),
        "conservative": Decimal("0.55"),
        "stress": Decimal("0.85"),
    }.get(profile_name, Decimal("0.35"))
    role_weight = Decimal("1.00") if str(role or "").lower() == "maker" else Decimal("0.75")
    risk = (range_excess + dislocation_excess) * profile_weight * role_weight
    factor = (Decimal("1") / (Decimal("1") + risk)).quantize(Q, rounding=ROUND_HALF_UP)
    return factor, "volatile_block_context_discount", block_range_pct, vwap_dislocation_pct


def _trade_count_fill_factor(trade_count: int, profile_name: str, role: str) -> tuple[Decimal, str]:
    if profile_name == "optimistic" or trade_count <= 0:
        return Decimal("1"), "optimistic_or_missing_trade_count"
    normalized_role = str(role or "").lower()
    if trade_count >= 3:
        return Decimal("1"), "normal_trade_count"
    if trade_count == 2:
        factors = {
            "neutral": Decimal("0.95") if normalized_role == "taker" else Decimal("0.90"),
            "realistic": Decimal("0.90") if normalized_role == "taker" else Decimal("0.80"),
            "conservative": Decimal("0.85") if normalized_role == "taker" else Decimal("0.70"),
            "stress": Decimal("0.75") if normalized_role == "taker" else Decimal("0.55"),
        }
        return factors.get(profile_name, Decimal("0.85")), "thin_trade_count_discount"
    factors = {
        "neutral": Decimal("0.90") if normalized_role == "taker" else Decimal("0.80"),
        "realistic": Decimal("0.82") if normalized_role == "taker" else Decimal("0.65"),
        "conservative": Decimal("0.72") if normalized_role == "taker" else Decimal("0.50"),
        "stress": Decimal("0.60") if normalized_role == "taker" else Decimal("0.35"),
    }
    return factors.get(profile_name, Decimal("0.72")), "single_trade_block_discount"


def _participation_pressure_fill_factor(
    requested_notional: Decimal,
    available_notional: Decimal,
    profile_name: str,
    role: str,
) -> tuple[Decimal, str]:
    if profile_name == "optimistic":
        return Decimal("1"), "optimistic_participation"
    requested = max(Decimal("0"), Decimal(str(requested_notional or 0)))
    available = max(Decimal("0"), Decimal(str(available_notional or 0)))
    if requested <= 0 or available <= 0:
        return Decimal("1"), "missing_participation_context"
    participation = (requested / available * Decimal("100")).quantize(Q, rounding=ROUND_HALF_UP)
    soft_cap = {
        "neutral": Decimal("100") if str(role or "").lower() == "taker" else Decimal("85"),
        "realistic": Decimal("90") if str(role or "").lower() == "taker" else Decimal("75"),
        "conservative": Decimal("80") if str(role or "").lower() == "taker" else Decimal("65"),
        "stress": Decimal("70") if str(role or "").lower() == "taker" else Decimal("55"),
    }.get(profile_name, Decimal("90"))
    if participation <= soft_cap:
        return Decimal("1"), "normal_participation_pressure"
    excess = (participation - soft_cap) / soft_cap
    profile_weight = {
        "neutral": Decimal("0.20"),
        "realistic": Decimal("0.35"),
        "conservative": Decimal("0.55"),
        "stress": Decimal("0.80"),
    }.get(profile_name, Decimal("0.35"))
    factor = (Decimal("1") / (Decimal("1") + excess * profile_weight)).quantize(Q, rounding=ROUND_HALF_UP)
    return factor, "high_participation_pressure_discount"
