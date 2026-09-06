"""Probability-weighted execution eligibility from OrderFilled trade tape only."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any

Q = Decimal("0.0000000001")
ARRIVAL_FEATURE_CONTRACT = "ARRIVAL_BLOCK_AND_TIME_SIGNAL_EXCLUDED_V2"
HIERARCHY_KEY_SCHEME_LEGACY = "legacy_v1"
HIERARCHY_KEY_SCHEME_VALIDATION_ALIGNED = "validation_aligned_v2"
PROBABILITY_TARGET_LEGACY = "LEGACY_UNSPECIFIED"
PROBABILITY_TARGET_FAK_ANY_FILL = "FAK_ANY_FILL"
PROBABILITY_TARGET_FOK_FULL_FILL = "FOK_FULL_FILL"
FEATURE_NAMES = (
    "log_same_count",
    "log_opposite_count",
    "log_same_volume",
    "log_opposite_volume",
    "same_flow_share",
    "same_recency",
    "opposite_recency",
    "limit_aggressiveness_ticks",
    "absolute_return_ticks",
    "log_order_to_tape_ratio",
    "log_order_size",
    "log_horizon_seconds",
    "buy_side",
    "price_extremity",
    "local_trade_present",
    "two_sided_trade_present",
    "log_total_trade_count",
    "limit_price",
    "category_sports",
    "category_esports",
    "category_crypto",
    "category_economics",
    "category_politics",
    "category_other",
    "aggressiveness_nonnegative",
    "aggressiveness_ge_2",
    "aggressiveness_ge_5",
    "aggressiveness_ge_10",
    "positive_aggressiveness_ticks",
    "negative_aggressiveness_ticks",
    "any_recent_30s",
    "any_recent_120s",
    "any_recent_300s",
    "price_bucket_00_05",
    "price_bucket_05_20",
    "price_bucket_20_50",
    "price_bucket_50_80",
    "price_bucket_80_95",
    "price_bucket_95_100",
    "trades_ge_2",
    "trades_ge_6",
    "trades_ge_20",
    "trades_ge_50",
    "flow_imbalance_abs",
)


@dataclass(frozen=True)
class OrderFilledProbabilityFeatures:
    side: str
    limit_price: Decimal
    trailing_same_count: int
    trailing_opposite_count: int
    trailing_same_volume: Decimal
    trailing_opposite_volume: Decimal
    last_same_age_seconds: Decimal | None
    last_opposite_age_seconds: Decimal | None
    last_trade_price: Decimal | None
    first_trade_price: Decimal | None
    horizon_seconds: Decimal
    order_size: Decimal
    vector: dict[str, Decimal]

    def as_dict(self) -> dict[str, Any]:
        return _json_ready(_dataclass_field_mapping(self))


@dataclass(frozen=True)
class ConditionalCapacityProfile:
    name: str
    target: str
    quantile: Decimal | None
    floor: Decimal
    intercept: Decimal
    coefficients: dict[str, Decimal]
    training_rows: int

    def as_dict(self) -> dict[str, Any]:
        return _json_ready(_dataclass_field_mapping(self))


@dataclass(frozen=True)
class OrderFilledProbabilityProfile:
    name: str
    model_version: str
    min_probability: Decimal
    medium_probability: Decimal
    high_probability: Decimal
    capacity_floor: Decimal
    hard_reject_below_probability: bool
    lookback_seconds: Decimal
    lookback_blocks: int
    tick_size: Decimal
    intercept: Decimal
    coefficients: dict[str, Decimal]
    feature_means: dict[str, Decimal]
    feature_scales: dict[str, Decimal]
    calibration_x: tuple[Decimal, ...]
    calibration_y: tuple[Decimal, ...]
    calibration_by_category_family: dict[
        str, tuple[tuple[Decimal, ...], tuple[Decimal, ...]]
    ]
    capacity_mode: str
    capacity_variant: str
    conditional_capacity_models: dict[str, ConditionalCapacityProfile]
    training_rows: int
    activation: str
    abstain_categories: tuple[str, ...]
    abstain_category_families: tuple[str, ...]
    abstain_activity_regimes: tuple[str, ...]
    hierarchical_probability_cells: dict[str, dict[str, Any]]
    hierarchical_probability_min_samples: int
    hierarchical_probability_blend_strength: Decimal
    hierarchical_probability_scale: Decimal
    hierarchical_key_scheme: str
    hierarchical_supported_categories: tuple[str, ...]
    hierarchical_supported_category_families: tuple[str, ...]
    hierarchical_fallback_categories: tuple[str, ...]
    hierarchical_fallback_category_families: tuple[str, ...]
    probability_target: str = PROBABILITY_TARGET_LEGACY
    supported_sides: tuple[str, ...] = ()
    supported_tifs: tuple[str, ...] = ()
    supported_amount_units: tuple[str, ...] = ()
    minimum_supported_order_size: Decimal | None = None
    maximum_supported_order_size: Decimal | None = None
    minimum_supported_log_order_to_tape_ratio: Decimal | None = None
    maximum_supported_log_order_to_tape_ratio: Decimal | None = None
    training_period_start: str | None = None
    training_period_end: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return _json_ready(_dataclass_field_mapping(self))


@dataclass(frozen=True)
class OrderFilledProbabilityDecision:
    p_fill: Decimal
    raw_probability: Decimal
    capacity_multiplier: Decimal
    conditional_capacity_fraction: Decimal
    capacity_variant: str
    eligibility: str
    accepted: bool
    reason: str
    domain_supported: bool
    base_probability: Decimal
    hierarchical_probability: Decimal | None
    hierarchical_probability_cell: str | None
    hierarchical_probability_samples: int
    features: OrderFilledProbabilityFeatures
    profile: OrderFilledProbabilityProfile
    domain_violations: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return _json_ready(_dataclass_field_mapping(self))


class OrderFilledProbabilityModel:
    """Estimate future same-side fill evidence from pre-arrival tape state."""

    def __init__(
        self, profile: OrderFilledProbabilityProfile | Mapping[str, Any] | None = None
    ) -> None:
        if profile is None:
            self.profile = default_orderfilled_probability_profile()
        elif isinstance(profile, OrderFilledProbabilityProfile):
            self.profile = profile
        else:
            self.profile = orderfilled_probability_profile_from_mapping(profile)

    def decide(
        self,
        order: Any,
        trades: Iterable[Any],
        *,
        presorted_relevant: bool = False,
    ) -> OrderFilledProbabilityDecision:
        features = extract_orderfilled_probability_features(
            order,
            trades,
            lookback=timedelta(seconds=float(self.profile.lookback_seconds)),
            tick_size=self.profile.tick_size,
            presorted=presorted_relevant,
            same_market_asset=presorted_relevant,
        )
        return self.decide_from_features(order, features)

    def decide_from_features(
        self,
        order: Any,
        features: OrderFilledProbabilityFeatures,
    ) -> OrderFilledProbabilityDecision:
        """Evaluate an already extracted point-in-time feature row."""

        raw = _linear_probability(features.vector, self.profile)
        category_family = _category_family(order)
        family_calibration = self.profile.calibration_by_category_family.get(
            category_family
        )
        calibration_x, calibration_y = family_calibration or (
            self.profile.calibration_x,
            self.profile.calibration_y,
        )
        base_probability = _calibrate(raw, calibration_x, calibration_y)
        prior_key, prior_probability, prior_samples = _resolve_hierarchical_probability(
            order, self.profile, features.vector
        )
        calibrated = base_probability
        if prior_probability is not None:
            strength = max(
                Decimal(0), self.profile.hierarchical_probability_blend_strength
            )
            prior_weight = (
                Decimal(1)
                if strength == 0
                else Decimal(prior_samples) / (Decimal(prior_samples) + strength)
            )
            calibrated = _bounded(
                prior_weight * prior_probability
                + (Decimal(1) - prior_weight) * base_probability
            )
        calibrated = _bounded(
            calibrated * max(Decimal(0), self.profile.hierarchical_probability_scale)
        )
        category = _normalized_category(order)
        activity_regime = _activity_regime(features.vector)
        contract_violations = probability_profile_domain_violations(
            self.profile,
            order,
            features,
        )
        domain_supported = (
            category not in self.profile.abstain_categories
            and category_family not in self.profile.abstain_category_families
            and activity_regime not in self.profile.abstain_activity_regimes
            and not contract_violations
        )
        accepted = domain_supported and (
            not self.profile.hard_reject_below_probability
            or calibrated >= self.profile.min_probability
        )
        conditional_capacity = _conditional_capacity(features.vector, self.profile)
        if prior_key is not None and prior_samples > 0:
            cell = self.profile.hierarchical_probability_cells.get(prior_key, {})
            prior_conditional = cell.get("conditional_fill_fraction")
            if prior_conditional is not None:
                strength = max(
                    Decimal(0), self.profile.hierarchical_probability_blend_strength
                )
                positive_samples = max(0, int(cell.get("positive_samples") or 0))
                conditional_weight = (
                    Decimal(1)
                    if strength == 0
                    else Decimal(positive_samples)
                    / (Decimal(positive_samples) + strength)
                )
                conditional_capacity = _bounded(
                    conditional_weight * _bounded(_decimal(prior_conditional))
                    + (Decimal(1) - conditional_weight) * conditional_capacity
                )
        if self.profile.capacity_mode == "conditional_source_capacity":
            multiplier = conditional_capacity
        else:
            floor = _bounded(self.profile.capacity_floor)
            multiplier = floor + (Decimal(1) - floor) * calibrated
        if not accepted:
            multiplier = Decimal(0)
        eligibility = _eligibility(calibrated, self.profile)
        return OrderFilledProbabilityDecision(
            p_fill=calibrated.quantize(Q, rounding=ROUND_HALF_UP),
            raw_probability=raw.quantize(Q, rounding=ROUND_HALF_UP),
            capacity_multiplier=multiplier.quantize(Q, rounding=ROUND_HALF_UP),
            conditional_capacity_fraction=conditional_capacity.quantize(
                Q, rounding=ROUND_HALF_UP
            ),
            capacity_variant=self.profile.capacity_variant,
            eligibility=eligibility,
            accepted=accepted,
            reason=(
                ""
                if accepted
                else "orderfilled_probability_model_out_of_domain"
                if not domain_supported
                else "low_orderfilled_fill_probability"
            ),
            domain_supported=domain_supported,
            base_probability=base_probability.quantize(Q, rounding=ROUND_HALF_UP),
            hierarchical_probability=(
                prior_probability.quantize(Q, rounding=ROUND_HALF_UP)
                if prior_probability is not None
                else None
            ),
            hierarchical_probability_cell=prior_key,
            hierarchical_probability_samples=prior_samples,
            features=features,
            profile=self.profile,
            domain_violations=contract_violations,
        )


def default_orderfilled_probability_profile() -> OrderFilledProbabilityProfile:
    coefficients = {
        "log_same_count": Decimal("0.55"),
        "log_opposite_count": Decimal("0.15"),
        "log_same_volume": Decimal("0.20"),
        "log_opposite_volume": Decimal("0.05"),
        "same_flow_share": Decimal("0.75"),
        "same_recency": Decimal("0.80"),
        "opposite_recency": Decimal("0.10"),
        "limit_aggressiveness_ticks": Decimal("0.12"),
        "absolute_return_ticks": Decimal("-0.08"),
        "log_order_to_tape_ratio": Decimal("-0.30"),
        "log_horizon_seconds": Decimal("0.25"),
        "buy_side": Decimal(0),
        "price_extremity": Decimal("-0.15"),
        "local_trade_present": Decimal(0),
        "two_sided_trade_present": Decimal(0),
        "log_total_trade_count": Decimal(0),
        "limit_price": Decimal(0),
        "category_sports": Decimal(0),
        "category_esports": Decimal(0),
        "category_crypto": Decimal(0),
        "category_economics": Decimal(0),
        "category_politics": Decimal(0),
        "category_other": Decimal(0),
    }
    full_capacity = ConditionalCapacityProfile(
        name="expected",
        target="conditional_fill_fraction",
        quantile=None,
        floor=Decimal(0),
        intercept=Decimal(1),
        coefficients={name: Decimal(0) for name in FEATURE_NAMES},
        training_rows=0,
    )
    return OrderFilledProbabilityProfile(
        name="orderfilled_probability_trade_tape",
        model_version="orderfilled_probability_builtin_v1",
        min_probability=Decimal("0.45"),
        medium_probability=Decimal("0.45"),
        high_probability=Decimal("0.75"),
        capacity_floor=Decimal("0.25"),
        hard_reject_below_probability=False,
        lookback_seconds=Decimal(300),
        lookback_blocks=300,
        tick_size=Decimal("0.01"),
        intercept=Decimal("-1.25"),
        coefficients=coefficients,
        feature_means={name: Decimal(0) for name in FEATURE_NAMES},
        feature_scales={name: Decimal(1) for name in FEATURE_NAMES},
        calibration_x=(),
        calibration_y=(),
        calibration_by_category_family={},
        capacity_mode="probability_weighted",
        capacity_variant="expected",
        conditional_capacity_models={"expected": full_capacity},
        training_rows=0,
        activation="builtin_untrained",
        abstain_categories=(),
        abstain_category_families=(),
        abstain_activity_regimes=(),
        hierarchical_probability_cells={},
        hierarchical_probability_min_samples=0,
        hierarchical_probability_blend_strength=Decimal(0),
        hierarchical_probability_scale=Decimal(1),
        hierarchical_key_scheme=HIERARCHY_KEY_SCHEME_LEGACY,
        hierarchical_supported_categories=(),
        hierarchical_supported_category_families=(),
        hierarchical_fallback_categories=(),
        hierarchical_fallback_category_families=(),
    )


def load_orderfilled_probability_profile(
    path: str | Path | None,
) -> OrderFilledProbabilityProfile:
    if not path:
        return default_orderfilled_probability_profile()
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return orderfilled_probability_profile_from_mapping(payload)


def orderfilled_probability_profile_from_mapping(
    row: Mapping[str, Any],
) -> OrderFilledProbabilityProfile:
    params_raw = row.get("params")
    params: Mapping[str, Any] = params_raw if isinstance(params_raw, Mapping) else row
    model_raw = row.get("model")
    if not isinstance(model_raw, Mapping):
        model_raw = params.get("model")
    model: Mapping[str, Any] = model_raw if isinstance(model_raw, Mapping) else {}
    if not isinstance(model, Mapping) or not model:
        model = params
    calibration_raw = row.get("calibration")
    if not isinstance(calibration_raw, Mapping):
        calibration_raw = params.get("calibration")
    calibration: Mapping[str, Any] = (
        calibration_raw if isinstance(calibration_raw, Mapping) else {}
    )
    family_calibration_raw = calibration.get("by_category_family")
    family_calibrations = {
        str(family).strip().lower(): (
            tuple(_decimal(value) for value in values.get("x", ())),
            tuple(_decimal(value) for value in values.get("y", ())),
        )
        for family, values in (
            family_calibration_raw.items()
            if isinstance(family_calibration_raw, Mapping)
            else ()
        )
        if str(family).strip() and isinstance(values, Mapping)
    }
    defaults = default_orderfilled_probability_profile()
    coefficients = _decimal_mapping(model.get("coefficients"), defaults.coefficients)
    means = _decimal_mapping(model.get("feature_means"), defaults.feature_means)
    scales = {
        key: max(Decimal("0.0000000001"), value)
        for key, value in _decimal_mapping(
            model.get("feature_scales"), defaults.feature_scales
        ).items()
    }
    capacity_raw = row.get("conditional_capacity")
    capacity: Mapping[str, Any] = (
        capacity_raw if isinstance(capacity_raw, Mapping) else {}
    )
    capacity_models = _capacity_models_from_mapping(
        capacity.get("models") or row.get("conditional_capacity_models"),
        defaults.conditional_capacity_models,
    )
    domain_raw = row.get("domain_gate")
    domain = domain_raw if isinstance(domain_raw, Mapping) else {}
    hierarchical_raw = row.get("hierarchical_probability")
    hierarchical = hierarchical_raw if isinstance(hierarchical_raw, Mapping) else {}
    contract_raw = row.get("model_contract")
    contract: Mapping[str, Any] = (
        contract_raw if isinstance(contract_raw, Mapping) else {}
    )
    raw_cells = hierarchical.get("cells")
    cells = {
        str(key): dict(value)
        for key, value in (raw_cells.items() if isinstance(raw_cells, Mapping) else ())
        if isinstance(value, Mapping)
    }
    return OrderFilledProbabilityProfile(
        name=str(row.get("profile_name") or params.get("name") or defaults.name),
        model_version=str(
            row.get("model_version") or model.get("version") or defaults.model_version
        ),
        min_probability=_decimal(
            params.get("min_probability", defaults.min_probability)
        ),
        medium_probability=_decimal(
            params.get("medium_probability", defaults.medium_probability)
        ),
        high_probability=_decimal(
            params.get("high_probability", defaults.high_probability)
        ),
        capacity_floor=_decimal(params.get("capacity_floor", defaults.capacity_floor)),
        hard_reject_below_probability=_bool(
            params.get(
                "hard_reject_below_probability", defaults.hard_reject_below_probability
            )
        ),
        lookback_seconds=_decimal(
            params.get("lookback_seconds", defaults.lookback_seconds)
        ),
        lookback_blocks=max(
            0, int(params.get("lookback_blocks", defaults.lookback_blocks))
        ),
        tick_size=max(
            Decimal("0.0000000001"),
            _decimal(params.get("tick_size", defaults.tick_size)),
        ),
        intercept=_decimal(model.get("intercept", defaults.intercept)),
        coefficients=coefficients,
        feature_means=means,
        feature_scales=scales,
        calibration_x=tuple(
            _decimal(value)
            for value in calibration.get("x", params.get("calibration_x", ()))
        ),
        calibration_y=tuple(
            _decimal(value)
            for value in calibration.get("y", params.get("calibration_y", ()))
        ),
        calibration_by_category_family=family_calibrations,
        capacity_mode=str(
            params.get("capacity_mode")
            or capacity.get("mode")
            or defaults.capacity_mode
        ),
        capacity_variant=str(
            params.get("capacity_variant")
            or capacity.get("default_variant")
            or defaults.capacity_variant
        ),
        conditional_capacity_models=capacity_models,
        training_rows=max(
            0,
            int(
                row.get(
                    "training_rows", row.get("trained_on_rows", defaults.training_rows)
                )
                or 0
            ),
        ),
        activation=str(row.get("activation") or "trained_orderfilled_only"),
        abstain_categories=tuple(
            sorted(
                {
                    str(value).strip().lower()
                    for value in domain.get("abstain_categories", ())
                    if str(value).strip()
                }
            )
        ),
        abstain_category_families=tuple(
            sorted(
                {
                    str(value).strip().lower()
                    for value in domain.get("abstain_category_families", ())
                    if str(value).strip()
                }
            )
        ),
        abstain_activity_regimes=tuple(
            sorted(
                {
                    str(value).strip().upper()
                    for value in domain.get("abstain_activity_regimes", ())
                    if str(value).strip()
                }
            )
        ),
        hierarchical_probability_cells=cells,
        hierarchical_probability_min_samples=max(
            0, int(hierarchical.get("minimum_samples") or 0)
        ),
        hierarchical_probability_blend_strength=max(
            Decimal(0), _decimal(hierarchical.get("blend_strength") or 0)
        ),
        hierarchical_probability_scale=max(
            Decimal(0), _decimal(hierarchical.get("probability_scale") or 1)
        ),
        hierarchical_key_scheme=str(
            hierarchical.get("key_scheme") or HIERARCHY_KEY_SCHEME_LEGACY
        ),
        hierarchical_supported_categories=tuple(
            sorted(
                {
                    str(value).strip().lower()
                    for value in hierarchical.get("supported_categories", ())
                    if str(value).strip()
                }
            )
        ),
        hierarchical_supported_category_families=tuple(
            sorted(
                {
                    str(value).strip().lower()
                    for value in hierarchical.get("supported_category_families", ())
                    if str(value).strip()
                }
            )
        ),
        hierarchical_fallback_categories=tuple(
            sorted(
                {
                    str(value).strip().lower()
                    for value in hierarchical.get("fallback_categories", ())
                    if str(value).strip()
                }
            )
        ),
        hierarchical_fallback_category_families=tuple(
            sorted(
                {
                    str(value).strip().lower()
                    for value in hierarchical.get("fallback_category_families", ())
                    if str(value).strip()
                }
            )
        ),
        probability_target=str(
            contract.get("probability_target") or PROBABILITY_TARGET_LEGACY
        )
        .strip()
        .upper(),
        supported_sides=_normalized_contract_values(contract.get("supported_sides")),
        supported_tifs=_normalized_contract_values(contract.get("supported_tifs")),
        supported_amount_units=_normalized_contract_values(
            contract.get("supported_amount_units")
        ),
        minimum_supported_order_size=_optional_decimal(
            contract.get("minimum_order_size")
        ),
        maximum_supported_order_size=_optional_decimal(
            contract.get("maximum_order_size")
        ),
        minimum_supported_log_order_to_tape_ratio=_optional_decimal(
            contract.get("minimum_log_order_to_tape_ratio")
        ),
        maximum_supported_log_order_to_tape_ratio=_optional_decimal(
            contract.get("maximum_log_order_to_tape_ratio")
        ),
        training_period_start=_optional_text(contract.get("training_period_start")),
        training_period_end=_optional_text(contract.get("training_period_end")),
    )


def probability_profile_domain_violations(
    profile: OrderFilledProbabilityProfile,
    order: Any,
    features: OrderFilledProbabilityFeatures,
    *,
    required_target: str | None = None,
) -> tuple[str, ...]:
    """Return explicit contract violations without silently extrapolating."""

    violations: list[str] = []
    target = str(profile.probability_target or PROBABILITY_TARGET_LEGACY).upper()
    requested_target = str(required_target or "").strip().upper()
    if (
        requested_target
        and target != PROBABILITY_TARGET_LEGACY
        and target != requested_target
    ):
        violations.append(f"probability_target:{target}!={requested_target}")

    side = _side(getattr(order, "side", ""))
    if profile.supported_sides and side not in profile.supported_sides:
        violations.append(f"side:{side}")
    tif = str(getattr(order, "tif", "")).strip().upper()
    if profile.supported_tifs and tif not in profile.supported_tifs:
        violations.append(f"tif:{tif or 'UNKNOWN'}")
    amount_unit_raw = getattr(order, "amount_unit", "SHARES")
    amount_unit = str(getattr(amount_unit_raw, "value", amount_unit_raw)).upper()
    if (
        profile.supported_amount_units
        and amount_unit not in profile.supported_amount_units
    ):
        violations.append(f"amount_unit:{amount_unit}")

    size = max(Decimal(0), _decimal(getattr(order, "size", 0)))
    if (
        profile.minimum_supported_order_size is not None
        and size < profile.minimum_supported_order_size
    ):
        violations.append("order_size_below_training_domain")
    if (
        profile.maximum_supported_order_size is not None
        and size > profile.maximum_supported_order_size
    ):
        violations.append("order_size_above_training_domain")

    log_ratio = features.vector.get("log_order_to_tape_ratio", Decimal(0))
    if (
        profile.minimum_supported_log_order_to_tape_ratio is not None
        and log_ratio < profile.minimum_supported_log_order_to_tape_ratio
    ):
        violations.append("order_to_tape_ratio_below_training_domain")
    if (
        profile.maximum_supported_log_order_to_tape_ratio is not None
        and log_ratio > profile.maximum_supported_log_order_to_tape_ratio
    ):
        violations.append("order_to_tape_ratio_above_training_domain")
    return tuple(violations)


def extract_orderfilled_probability_features(
    order: Any,
    trades: Iterable[Any],
    *,
    lookback: timedelta = timedelta(minutes=5),
    tick_size: Decimal = Decimal("0.01"),
    presorted: bool = False,
    same_market_asset: bool = False,
) -> OrderFilledProbabilityFeatures:
    side = _side(getattr(order, "side", ""))
    arrival_ts = getattr(order, "arrival_ts", None)
    arrival_block = getattr(order, "arrival_block", None)
    start_ts = arrival_ts - lookback if isinstance(arrival_ts, datetime) else None
    lookback_blocks = max(1, int(max(1.0, lookback.total_seconds())))
    start_block = (
        int(arrival_block) - lookback_blocks if arrival_block is not None else None
    )
    source_rows = (
        trades
        if presorted
        else sorted(trades, key=lambda item: getattr(item, "sequence", ()))
    )
    rows = [
        trade
        for trade in source_rows
        if (same_market_asset or _same_market_asset(order, trade))
        and _strictly_before_arrival(arrival_ts, arrival_block, trade)
        and _inside_lookback(start_ts, start_block, trade)
    ]
    same = [
        trade for trade in rows if _side(getattr(trade, "aggressor_side", "")) == side
    ]
    opposite = [
        trade for trade in rows if _side(getattr(trade, "aggressor_side", "")) != side
    ]
    same_volume = sum(
        (_decimal(getattr(trade, "size", 0)) for trade in same), Decimal(0)
    )
    opposite_volume = sum(
        (_decimal(getattr(trade, "size", 0)) for trade in opposite), Decimal(0)
    )
    total_volume = same_volume + opposite_volume
    last_same = same[-1] if same else None
    last_opposite = opposite[-1] if opposite else None
    last_trade = rows[-1] if rows else None
    first_trade = rows[0] if rows else None
    last_price = _optional_decimal(getattr(last_trade, "price", None))
    first_price = _optional_decimal(getattr(first_trade, "price", None))
    limit = _decimal(getattr(order, "limit_price", 0))
    size = max(Decimal(0), _decimal(getattr(order, "size", 0)))
    horizon_seconds = _horizon_seconds(order)
    tick = max(Decimal("0.0000000001"), _decimal(tick_size))
    aggressiveness = Decimal(0)
    absolute_return = Decimal(0)
    if last_price is not None:
        aggressiveness = (
            (limit - last_price) / tick
            if side == "BUY"
            else (last_price - limit) / tick
        )
    if first_price is not None and last_price is not None:
        absolute_return = abs(last_price - first_price) / tick
    same_share = same_volume / total_volume if total_volume > 0 else Decimal("0.5")
    same_age = _age_seconds(arrival_ts, last_same)
    opposite_age = _age_seconds(arrival_ts, last_opposite)
    window_seconds = max(Decimal(1), Decimal(str(lookback.total_seconds())))
    category_family = _category_family(order)
    vector = augment_orderfilled_probability_vector(
        {
            "log_same_count": _log1p(len(same)),
            "log_opposite_count": _log1p(len(opposite)),
            "log_same_volume": _log1p(same_volume),
            "log_opposite_volume": _log1p(opposite_volume),
            "same_flow_share": same_share,
            "same_recency": _recency_score(same_age, window_seconds),
            "opposite_recency": _recency_score(opposite_age, window_seconds),
            "limit_aggressiveness_ticks": _clamp(
                aggressiveness, Decimal(-20), Decimal(20)
            ),
            "absolute_return_ticks": _clamp(absolute_return, Decimal(0), Decimal(20)),
            "log_order_to_tape_ratio": _log1p(
                size / max(Decimal("0.0000000001"), total_volume)
            ),
            "log_order_size": _log1p(size),
            "log_horizon_seconds": _log1p(horizon_seconds),
            "buy_side": Decimal(1) if side == "BUY" else Decimal(0),
            "price_extremity": abs(limit - Decimal("0.5")) * Decimal(2),
            "local_trade_present": Decimal(1) if rows else Decimal(0),
            "two_sided_trade_present": Decimal(1) if same and opposite else Decimal(0),
            "log_total_trade_count": _log1p(len(rows)),
            "limit_price": _clamp(limit, Decimal(0), Decimal(1)),
            "category_sports": Decimal(1)
            if category_family == "sports"
            else Decimal(0),
            "category_esports": Decimal(1)
            if category_family == "esports"
            else Decimal(0),
            "category_crypto": Decimal(1)
            if category_family == "crypto"
            else Decimal(0),
            "category_economics": Decimal(1)
            if category_family == "economics"
            else Decimal(0),
            "category_politics": Decimal(1)
            if category_family == "politics"
            else Decimal(0),
            "category_other": Decimal(1) if category_family == "other" else Decimal(0),
        },
        last_same_age_seconds=same_age,
        last_opposite_age_seconds=opposite_age,
    )
    return OrderFilledProbabilityFeatures(
        side=side,
        limit_price=limit.quantize(Q, rounding=ROUND_HALF_UP),
        trailing_same_count=len(same),
        trailing_opposite_count=len(opposite),
        trailing_same_volume=same_volume.quantize(Q, rounding=ROUND_HALF_UP),
        trailing_opposite_volume=opposite_volume.quantize(Q, rounding=ROUND_HALF_UP),
        last_same_age_seconds=same_age,
        last_opposite_age_seconds=opposite_age,
        last_trade_price=last_price.quantize(Q, rounding=ROUND_HALF_UP)
        if last_price is not None
        else None,
        first_trade_price=first_price.quantize(Q, rounding=ROUND_HALF_UP)
        if first_price is not None
        else None,
        horizon_seconds=horizon_seconds.quantize(Q, rounding=ROUND_HALF_UP),
        order_size=size.quantize(Q, rounding=ROUND_HALF_UP),
        vector={
            key: value.quantize(Q, rounding=ROUND_HALF_UP)
            for key, value in vector.items()
        },
    )


def augment_orderfilled_probability_vector(
    vector: Mapping[str, Any],
    *,
    last_same_age_seconds: Any | None = None,
    last_opposite_age_seconds: Any | None = None,
    order_size: Any | None = None,
) -> dict[str, Decimal]:
    """Add deterministic nonlinear terms derived from pre-arrival tape state."""

    result = {str(name): _decimal(value) for name, value in vector.items()}
    if order_size is not None:
        result.setdefault("log_order_size", _log1p(max(Decimal(0), _decimal(order_size))))
    aggressiveness = _decimal(result.get("limit_aggressiveness_ticks", 0))
    price = _bounded(_decimal(result.get("limit_price", 0)))
    total_count = max(0.0, math.expm1(float(result.get("log_total_trade_count", 0))))
    count_epsilon = 1e-6
    same_share = _bounded(_decimal(result.get("same_flow_share", "0.5")))
    ages = [
        _decimal(value)
        for value in (last_same_age_seconds, last_opposite_age_seconds)
        if value is not None
    ]
    most_recent_age = min(ages) if ages else None
    result.update(
        {
            "aggressiveness_nonnegative": Decimal(1)
            if aggressiveness >= 0
            else Decimal(0),
            "aggressiveness_ge_2": Decimal(1) if aggressiveness >= 2 else Decimal(0),
            "aggressiveness_ge_5": Decimal(1) if aggressiveness >= 5 else Decimal(0),
            "aggressiveness_ge_10": Decimal(1) if aggressiveness >= 10 else Decimal(0),
            "positive_aggressiveness_ticks": _clamp(
                aggressiveness, Decimal(0), Decimal(20)
            ),
            "negative_aggressiveness_ticks": _clamp(
                -aggressiveness, Decimal(0), Decimal(20)
            ),
            "any_recent_30s": Decimal(1)
            if most_recent_age is not None and most_recent_age <= 30
            else Decimal(0),
            "any_recent_120s": Decimal(1)
            if most_recent_age is not None and most_recent_age <= 120
            else Decimal(0),
            "any_recent_300s": Decimal(1)
            if most_recent_age is not None and most_recent_age <= 300
            else Decimal(0),
            "price_bucket_00_05": Decimal(1) if price < Decimal("0.05") else Decimal(0),
            "price_bucket_05_20": Decimal(1)
            if Decimal("0.05") <= price < Decimal("0.20")
            else Decimal(0),
            "price_bucket_20_50": Decimal(1)
            if Decimal("0.20") <= price < Decimal("0.50")
            else Decimal(0),
            "price_bucket_50_80": Decimal(1)
            if Decimal("0.50") <= price < Decimal("0.80")
            else Decimal(0),
            "price_bucket_80_95": Decimal(1)
            if Decimal("0.80") <= price < Decimal("0.95")
            else Decimal(0),
            "price_bucket_95_100": Decimal(1)
            if price >= Decimal("0.95")
            else Decimal(0),
            "trades_ge_2": Decimal(1)
            if total_count + count_epsilon >= 2
            else Decimal(0),
            "trades_ge_6": Decimal(1)
            if total_count + count_epsilon >= 6
            else Decimal(0),
            "trades_ge_20": Decimal(1)
            if total_count + count_epsilon >= 20
            else Decimal(0),
            "trades_ge_50": Decimal(1)
            if total_count + count_epsilon >= 50
            else Decimal(0),
            "flow_imbalance_abs": abs(same_share - Decimal("0.5")) * Decimal(2),
        }
    )
    return result


def future_same_side_fill_label(
    order: Any, trades: Iterable[Any], *, price_buffer: Decimal = Decimal(0)
) -> tuple[int, Decimal]:
    side = _side(getattr(order, "side", ""))
    limit = _decimal(getattr(order, "limit_price", 0))
    volume = Decimal(0)
    for trade in trades:
        if (
            not _same_market_asset(order, trade)
            or not _at_or_after_arrival(order, trade)
            or not _at_or_before_deadline(order, trade)
        ):
            continue
        if _side(getattr(trade, "aggressor_side", "")) != side:
            continue
        price = _decimal(getattr(trade, "price", 0))
        exec_price = price + price_buffer if side == "BUY" else price - price_buffer
        if not _limit_allows(side, price, limit) or not _limit_allows(
            side, exec_price, limit
        ):
            continue
        volume += max(Decimal(0), _decimal(getattr(trade, "size", 0)))
    return (1 if volume > 0 else 0), volume.quantize(Q, rounding=ROUND_HALF_UP)


def _linear_probability(
    vector: Mapping[str, Decimal], profile: OrderFilledProbabilityProfile
) -> Decimal:
    score = profile.intercept
    for name in FEATURE_NAMES:
        value = _decimal(vector.get(name, 0))
        mean = _decimal(profile.feature_means.get(name, 0))
        scale = max(
            Decimal("0.0000000001"), _decimal(profile.feature_scales.get(name, 1))
        )
        score += _decimal(profile.coefficients.get(name, 0)) * ((value - mean) / scale)
    numeric = max(-40.0, min(40.0, float(score)))
    return Decimal(str(1.0 / (1.0 + math.exp(-numeric))))


def _conditional_capacity(
    vector: Mapping[str, Decimal],
    profile: OrderFilledProbabilityProfile,
) -> Decimal:
    model = profile.conditional_capacity_models.get(profile.capacity_variant)
    if model is None:
        model = profile.conditional_capacity_models.get("expected")
    if model is None:
        return Decimal(1)
    value = model.intercept
    for name in FEATURE_NAMES:
        raw = _decimal(vector.get(name, 0))
        mean = _decimal(profile.feature_means.get(name, 0))
        scale = max(
            Decimal("0.0000000001"), _decimal(profile.feature_scales.get(name, 1))
        )
        value += _decimal(model.coefficients.get(name, 0)) * ((raw - mean) / scale)
    return max(_bounded(model.floor), _bounded(value))


def _calibrate(
    probability: Decimal, xs: tuple[Decimal, ...], ys: tuple[Decimal, ...]
) -> Decimal:
    p = _bounded(probability)
    if len(xs) < 2 or len(xs) != len(ys):
        return p
    if p <= xs[0]:
        return _bounded(ys[0])
    if p >= xs[-1]:
        return _bounded(ys[-1])
    for index in range(1, len(xs)):
        if p <= xs[index]:
            left_x, right_x = xs[index - 1], xs[index]
            left_y, right_y = ys[index - 1], ys[index]
            if right_x <= left_x:
                return _bounded(right_y)
            weight = (p - left_x) / (right_x - left_x)
            return _bounded(left_y + weight * (right_y - left_y))
    return p


def _eligibility(probability: Decimal, profile: OrderFilledProbabilityProfile) -> str:
    if probability >= profile.high_probability:
        return "ELIGIBLE_HIGH"
    if probability >= profile.medium_probability:
        return "ELIGIBLE_MEDIUM"
    if probability >= profile.min_probability:
        return "ELIGIBLE_LOW"
    return "UNSUPPORTED"


def _same_market_asset(order: Any, trade: Any) -> bool:
    return (
        int(order.market_id) == int(trade.market_id)
        and str(order.asset_id).lower() == str(trade.asset_id).lower()
    )


def _strictly_before_arrival(arrival_ts: Any, arrival_block: Any, trade: Any) -> bool:
    if arrival_block is not None and int(trade.block_number) >= int(arrival_block):
        return False
    return not (isinstance(arrival_ts, datetime) and trade.block_time >= arrival_ts)


def _inside_lookback(
    start_ts: datetime | None, start_block: int | None, trade: Any
) -> bool:
    if start_block is not None and int(trade.block_number) < start_block:
        return False
    return not (start_ts is not None and trade.block_time < start_ts)


def _at_or_after_arrival(order: Any, trade: Any) -> bool:
    arrival_block = getattr(order, "arrival_block", None)
    arrival_ts = getattr(order, "arrival_ts", None)
    if arrival_block is not None and int(trade.block_number) < int(arrival_block):
        return False
    return not (isinstance(arrival_ts, datetime) and trade.block_time < arrival_ts)


def _at_or_before_deadline(order: Any, trade: Any) -> bool:
    deadline_block = getattr(order, "deadline_block", None)
    deadline_ts = getattr(order, "deadline_ts", None)
    if deadline_block is not None and int(trade.block_number) > int(deadline_block):
        return False
    return not (isinstance(deadline_ts, datetime) and trade.block_time > deadline_ts)


def _limit_allows(side: str, price: Decimal, limit: Decimal) -> bool:
    return price <= limit if side == "BUY" else price >= limit


def _horizon_seconds(order: Any) -> Decimal:
    arrival_ts = getattr(order, "arrival_ts", None)
    deadline_ts = getattr(order, "deadline_ts", None)
    if isinstance(arrival_ts, datetime) and isinstance(deadline_ts, datetime):
        return Decimal(str(max(0.0, (deadline_ts - arrival_ts).total_seconds())))
    arrival_block = getattr(order, "arrival_block", None)
    deadline_block = getattr(order, "deadline_block", None)
    if arrival_block is not None and deadline_block is not None:
        return Decimal(max(0, int(deadline_block) - int(arrival_block)))
    return Decimal(0)


def _age_seconds(arrival_ts: Any, trade: Any | None) -> Decimal | None:
    if trade is None or not isinstance(arrival_ts, datetime):
        return None
    return Decimal(
        str(max(0.0, (arrival_ts - trade.block_time).total_seconds()))
    ).quantize(Q, rounding=ROUND_HALF_UP)


def _recency_score(age: Decimal | None, window: Decimal) -> Decimal:
    if age is None:
        return Decimal(0)
    return Decimal(str(math.exp(-float(age / max(Decimal(1), window)))))


def _log1p(value: Any) -> Decimal:
    return Decimal(str(math.log1p(max(0.0, float(_decimal(value))))))


def _side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported side: {value!r}")
    return side


def _category_family(order: Any) -> str:
    text = " ".join(
        str(getattr(order, name, "") or "").strip().lower().replace("_", "-")
        for name in ("category", "market_slug", "market_title", "league")
    )
    groups = (
        (
            "esports",
            (
                "league-of-legends",
                " lol ",
                "lol",
                "esports",
                "counter-strike",
                "valorant",
                "dota",
            ),
        ),
        (
            "sports",
            (
                "sports",
                "nba",
                "nfl",
                "mlb",
                "nhl",
                "soccer",
                "football",
                "basketball",
                "baseball",
                "hockey",
                "ufc",
                "tennis",
                "atp",
                "wta",
                "cfb",
                "ncaa",
                "golf",
                "pga",
                "cricket",
                "formula-1",
            ),
        ),
        (
            "crypto",
            (
                "crypto",
                "bitcoin",
                "ethereum",
                "solana",
                "xrp",
                "dogecoin",
            ),
        ),
        (
            "economics",
            (
                "economy",
                "finance",
                "fed-rate",
                "interest-rate",
                "inflation",
                "gdp",
                "recession",
            ),
        ),
        (
            "politics",
            (
                "politic",
                "democratic",
                "republican",
                "congress",
                "senate",
                "election",
                "president",
                "prime-minister",
                "geopolit",
                "war",
                "ceasefire",
                "iran",
                "israel",
                "ukraine",
                "russia",
                "china",
                "japan",
                "cuba",
                "fedorov",
            ),
        ),
    )
    for family, markers in groups:
        if any(marker in text for marker in markers):
            return family
    return "other"


def _normalized_category(order: Any) -> str:
    raw = str(getattr(order, "category", "") or "unknown").strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    return normalized or "unknown"


def _price_bucket(price: Any, key_scheme: str) -> str:
    value = _bounded(_decimal(price))
    if key_scheme == HIERARCHY_KEY_SCHEME_VALIDATION_ALIGNED:
        if value < Decimal("0.10"):
            return "00_10"
        if value < Decimal("0.25"):
            return "10_25"
        if value < Decimal("0.40"):
            return "25_40"
        if value < Decimal("0.60"):
            return "40_60"
        if value < Decimal("0.75"):
            return "60_75"
        if value < Decimal("0.90"):
            return "75_90"
        return "90_100"
    if value < Decimal("0.05"):
        return "00_05"
    if value < Decimal("0.20"):
        return "05_20"
    if value < Decimal("0.50"):
        return "20_50"
    if value < Decimal("0.80"):
        return "50_80"
    if value < Decimal("0.95"):
        return "80_95"
    return "95_100"


def _activity_bucket(vector: Mapping[str, Any] | None, key_scheme: str) -> str:
    if not vector:
        return "unknown"
    total_count = max(
        0.0,
        math.expm1(float(_decimal(vector.get("log_total_trade_count", 0)))),
    )
    if key_scheme == HIERARCHY_KEY_SCHEME_VALIDATION_ALIGNED:
        if total_count < 6:
            return "sparse"
        if total_count < 51:
            return "medium"
        return "active"
    if total_count < 1:
        return "zero"
    if total_count < 3:
        return "sparse"
    if total_count < 6:
        return "thin"
    if total_count < 20:
        return "medium"
    return "active"


def _activity_regime(vector: Mapping[str, Any] | None) -> str:
    total_count = max(
        0.0,
        math.expm1(float(_decimal((vector or {}).get("log_total_trade_count", 0)))),
    )
    if total_count < 6:
        return "SPARSE_LE_5"
    if total_count < 51:
        return "MEDIUM_6_TO_50"
    return "ACTIVE_GT_50"


def _order_size_bucket(order: Any) -> str:
    size = max(Decimal(0), _decimal(getattr(order, "size", 0)))
    for boundary, name in (
        (Decimal(1), "le_1"),
        (Decimal(5), "le_5"),
        (Decimal(10), "le_10"),
        (Decimal(25), "le_25"),
        (Decimal(100), "le_100"),
    ):
        if size <= boundary:
            return name
    return "gt_100"


def _utc_hour_bucket(order: Any) -> str:
    value = getattr(order, "signal_ts", None)
    if not isinstance(value, datetime):
        return "unknown"
    lower = (value.astimezone(timezone.utc).hour // 4) * 4
    return f"{lower:02d}_{lower + 4:02d}"


def _utc_hour_exact(order: Any) -> str:
    value = getattr(order, "signal_ts", None)
    if not isinstance(value, datetime):
        return "unknown"
    return f"{value.astimezone(timezone.utc).hour:02d}"


def _tte_bucket(order: Any) -> str | None:
    signal_ts = getattr(order, "signal_ts", None)
    market_end_ts = getattr(order, "market_end_ts", None)
    if not isinstance(signal_ts, datetime) or not isinstance(market_end_ts, datetime):
        return None
    seconds = max(0.0, (market_end_ts - signal_ts).total_seconds())
    if seconds <= 10 * 60:
        return "00_10m"
    if seconds <= 30 * 60:
        return "10_30m"
    if seconds <= 90 * 60:
        return "30_90m"
    if seconds <= 240 * 60:
        return "90_240m"
    return "240m_plus"


def _product_bucket(order: Any) -> str | None:
    text = " ".join(
        str(getattr(order, name, "") or "").strip().lower().replace("_", "-")
        for name in ("slug", "title", "market_slug", "market_title")
    )
    if not text:
        return None
    if "-5m-" in text or " 5 minute" in text or " five minute" in text:
        return "crypto_updown_5m" if "updown" in text or "up or down" in text else "5m"
    if "-15m-" in text or " 15 minute" in text or " fifteen minute" in text:
        return (
            "crypto_updown_15m" if "updown" in text or "up or down" in text else "15m"
        )
    if "updown" in text or "up or down" in text:
        return "crypto_updown_other"
    return None


def hierarchical_probability_keys(
    order: Any,
    vector: Mapping[str, Any] | None = None,
    *,
    key_scheme: str = HIERARCHY_KEY_SCHEME_LEGACY,
) -> tuple[str, ...]:
    market_id = str(getattr(order, "market_id", "unknown"))
    asset_id = str(getattr(order, "asset_id", "unknown")).strip().lower()
    category = _normalized_category(order)
    family = _category_family(order)
    bucket = _price_bucket(getattr(order, "limit_price", 0), key_scheme)
    activity = _activity_bucket(vector, key_scheme)
    hour = _utc_hour_bucket(order)
    exact_hour = _utc_hour_exact(order)
    tte = _tte_bucket(order)
    product = _product_bucket(order)
    keys: list[str] = []
    if key_scheme == HIERARCHY_KEY_SCHEME_VALIDATION_ALIGNED:
        side = str(getattr(order, "side", "unknown")).strip().lower()
        tif = str(getattr(order, "tif", "unknown")).strip().lower()
        size = _order_size_bucket(order)
        contract = f"{side}|{tif}|{size}"
        keys.extend(
            (
                f"market_asset_contract_price_activity|{market_id}|{asset_id}|{contract}|{bucket}|{activity}",
            )
        )
        if product is not None:
            keys.append(
                f"product_contract_price_activity|{product}|{contract}|{bucket}|{activity}"
            )
            keys.append(f"product_contract_price|{product}|{contract}|{bucket}")
            keys.append(f"product_price_activity|{product}|{bucket}|{activity}")
            keys.append(f"product_price|{product}|{bucket}")
            keys.append(f"product_contract|{product}|{contract}")
            keys.append(f"product|{product}")
        keys.append(
            f"category_contract_price_activity|{category}|{contract}|{bucket}|{activity}"
        )
        if family not in {"other", "unknown"}:
            keys.append(
                f"family_contract_price_activity|{family}|{contract}|{bucket}|{activity}"
            )
        keys.extend(
            (
                f"contract_price_activity|{contract}|{bucket}|{activity}",
                f"category_contract_price|{category}|{contract}|{bucket}",
            )
        )
        if family not in {"other", "unknown"}:
            keys.append(f"family_contract_price|{family}|{contract}|{bucket}")
        keys.extend(
            (
                f"contract_price|{contract}|{bucket}",
                f"category_contract|{category}|{contract}",
            )
        )
        if family not in {"other", "unknown"}:
            keys.append(f"family_contract|{family}|{contract}")
        keys.append(f"contract|{contract}")
    keys.extend(
        [
            f"market_asset_price_activity_hour|{market_id}|{asset_id}|{bucket}|{activity}|{hour}",
            f"market_asset_price_activity|{market_id}|{asset_id}|{bucket}|{activity}",
            f"market_asset_price|{market_id}|{asset_id}|{bucket}",
            f"market_asset|{market_id}|{asset_id}",
            f"market_price_activity_hour|{market_id}|{bucket}|{activity}|{hour}",
            f"market_price_activity|{market_id}|{bucket}|{activity}",
            f"market_price|{market_id}|{bucket}",
            f"market|{market_id}",
            f"category_price_activity_exact_hour|{category}|{bucket}|{activity}|{exact_hour}",
            f"category_price_activity_hour|{category}|{bucket}|{activity}|{hour}",
            f"category_price_activity|{category}|{bucket}|{activity}",
            f"category_activity|{category}|{activity}",
        ]
    )
    if tte is not None:
        keys.append(f"category_price_tte|{category}|{bucket}|{tte}")
    keys.extend(
        (
            f"category_price_hour|{category}|{bucket}|{hour}",
            f"category_price|{category}|{bucket}",
            f"category|{category}",
        )
    )
    # `other` is a catch-all, not a meaningful parent population.
    if family not in {"other", "unknown"}:
        keys.extend(
            (
                f"family_price_activity_exact_hour|{family}|{bucket}|{activity}|{exact_hour}",
                f"family_price_activity_hour|{family}|{bucket}|{activity}|{hour}",
                f"family_price_activity|{family}|{bucket}|{activity}",
                f"family_activity|{family}|{activity}",
            )
        )
        if tte is not None:
            keys.append(f"family_price_tte|{family}|{bucket}|{tte}")
        keys.extend(
            (
                f"family_price_hour|{family}|{bucket}|{hour}",
                f"family_price|{family}|{bucket}",
                f"family|{family}",
            )
        )
    keys.extend(
        (
            f"price_activity_exact_hour|{bucket}|{activity}|{exact_hour}",
            f"price_activity_hour|{bucket}|{activity}|{hour}",
            f"price_activity|{bucket}|{activity}",
            f"activity|{activity}",
            f"price_hour|{bucket}|{hour}",
            f"price|{bucket}",
            "global",
        )
    )
    return tuple(keys)


def _resolve_hierarchical_probability(
    order: Any,
    profile: OrderFilledProbabilityProfile,
    vector: Mapping[str, Any] | None = None,
) -> tuple[str | None, Decimal | None, int]:
    category = _normalized_category(order)
    family = _category_family(order)
    skip_category = (
        category in profile.hierarchical_fallback_categories
        or bool(profile.hierarchical_supported_categories)
        and category not in profile.hierarchical_supported_categories
    )
    skip_family = (
        family in profile.hierarchical_fallback_category_families
        or bool(profile.hierarchical_supported_category_families)
        and family not in profile.hierarchical_supported_category_families
    )
    minimum = max(0, int(profile.hierarchical_probability_min_samples))
    for key in hierarchical_probability_keys(
        order, vector, key_scheme=profile.hierarchical_key_scheme
    ):
        level = key.split("|", 1)[0]
        if skip_category and level.startswith("category"):
            continue
        if skip_family and level.startswith("family"):
            continue
        if (skip_category or skip_family) and key == "global":
            continue
        cell = profile.hierarchical_probability_cells.get(key)
        if not isinstance(cell, Mapping):
            continue
        samples = max(0, int(cell.get("samples") or 0))
        if samples < minimum:
            continue
        probability = cell.get("probability")
        if probability is None:
            continue
        return key, _bounded(_decimal(probability)), samples
    return None, None, 0


def _decimal_mapping(value: Any, defaults: Mapping[str, Decimal]) -> dict[str, Decimal]:
    source = value if isinstance(value, Mapping) else {}
    return {
        name: _decimal(source.get(name, defaults.get(name, 0)))
        for name in FEATURE_NAMES
    }


def _capacity_models_from_mapping(
    value: Any,
    defaults: Mapping[str, ConditionalCapacityProfile],
) -> dict[str, ConditionalCapacityProfile]:
    source = value if isinstance(value, Mapping) else {}
    models: dict[str, ConditionalCapacityProfile] = {}
    for name, raw in source.items():
        if not isinstance(raw, Mapping):
            continue
        models[str(name)] = ConditionalCapacityProfile(
            name=str(raw.get("name") or name),
            target=str(raw.get("target") or "conditional_fill_fraction"),
            quantile=_optional_decimal(raw.get("quantile")),
            floor=_bounded(_decimal(raw.get("floor", 0))),
            intercept=_decimal(raw.get("intercept", 1)),
            coefficients=_decimal_mapping(raw.get("coefficients"), {}),
            training_rows=max(0, int(raw.get("training_rows") or 0)),
        )
    return models or dict(defaults)


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return _decimal(value)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalized_contract_values(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple, set, frozenset)):
        return ()
    return tuple(
        sorted({str(item).strip().upper() for item in value if str(item).strip()})
    )


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None or value == "":
        return Decimal(0)
    return Decimal(str(value))


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _bounded(value: Decimal) -> Decimal:
    return max(Decimal(0), min(Decimal(1), _decimal(value)))


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    return max(low, min(high, value))


def _json_ready(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(_dataclass_field_mapping(value))
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    return value


def _dataclass_field_mapping(value: Any) -> dict[str, Any]:
    return {item.name: getattr(value, item.name) for item in fields(value)}
