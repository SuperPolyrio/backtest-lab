from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Literal


class LiquidityIntent(str, Enum):
    TAKER = "TAKER"
    PASSIVE = "PASSIVE"
    AUTO_BOUND = "AUTO_BOUND"


class ExecutionMode(str, Enum):
    SOURCE_CONFIRMED_TAPE = "SOURCE_CONFIRMED_TAPE"
    PASSIVE_TRADE_THROUGH_LOWER = "PASSIVE_TRADE_THROUGH_LOWER"
    PASSIVE_TOUCH_SURVIVAL = "PASSIVE_TOUCH_SURVIVAL"
    SYNTHETIC_ARRIVAL_LIQUIDITY = "SYNTHETIC_ARRIVAL_LIQUIDITY"
    GENERATIVE_TAPE_MC = "GENERATIVE_TAPE_MC"
    HIERARCHICAL_EXPECTED_FILL = "HIERARCHICAL_EXPECTED_FILL"
    CENTRAL_ROUTER = "CENTRAL_ROUTER"
    AUTO_BOUND = "AUTO_BOUND"


class EvidenceTier(str, Enum):
    A_SOURCE_CONFIRMED = "A_SOURCE_CONFIRMED"
    B_TRADE_THROUGH_INFERRED = "B_TRADE_THROUGH_INFERRED"
    C_TOUCH_SURVIVAL = "C_TOUCH_SURVIVAL"
    D_SYNTHETIC_ARRIVAL = "D_SYNTHETIC_ARRIVAL"
    E_GENERATIVE_TAPE_MC = "E_GENERATIVE_TAPE_MC"


TimeInForce = Literal["GTC", "GTD", "IOC", "FOK", "FAK"]
Side = Literal["BUY", "SELL"]


@dataclass(frozen=True)
class TradeOnlyProfile:
    name: str
    execution_mode: ExecutionMode
    evidence_tier: EvidenceTier
    default_liquidity_intent: LiquidityIntent
    participation_rate: Decimal
    latency: timedelta
    horizon: timedelta
    price_buffer: Decimal = Decimal(0)
    capacity_quantile: Decimal | None = None
    touch_probability_variant: str | None = None
    monte_carlo_paths: int = 0
    default_tif: TimeInForce = "GTC"
    default_lookback_blocks: int = 300
    default_horizon_blocks: int = 2_000
    probability_profile_path: str | None = None
    probability_profile_sha256: str | None = None
    full_fill_probability_profile_path: str | None = None
    full_fill_probability_profile_sha256: str | None = None
    enforce_probability_contract: bool = False
    hierarchical_prior_path: str | None = None
    price_buffer_profile_path: str | None = None
    probability_prior_30s: Decimal | None = None
    probability_prior_strength: int = 0
    conditional_capacity_prior: Decimal | None = None
    minimum_pre_arrival_trades: int = 0
    allow_immediate_tif_modeling: bool = False
    allow_prior_only: bool = False
    minimum_prior_samples: int = 0
    probability_model_horizon_seconds: int | None = None
    minimum_modeled_probability: Decimal = Decimal(0)
    use_probability_profile_threshold: bool = False
    direct_probability_model: bool = False
    evaluate_probability_on_empty_tape: bool = False
    augment_source_with_modeled_residual: bool = False
    calibration_status: str = "UNTRAINED_HEURISTIC"
    result_role: str = "SENSITIVITY_ONLY"
    model_version: str = "trade_only_v3_rules_v1"

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "execution_mode": self.execution_mode.value,
            "evidence_tier": self.evidence_tier.value,
            "default_liquidity_intent": self.default_liquidity_intent.value,
            "participation_rate": str(self.participation_rate),
            "latency_seconds": str(self.latency.total_seconds()),
            "horizon_seconds": str(self.horizon.total_seconds()),
            "price_buffer": str(self.price_buffer),
            "capacity_quantile": str(self.capacity_quantile)
            if self.capacity_quantile is not None
            else None,
            "touch_probability_variant": self.touch_probability_variant,
            "monte_carlo_paths": self.monte_carlo_paths,
            "default_tif": self.default_tif,
            "default_lookback_blocks": self.default_lookback_blocks,
            "default_horizon_blocks": self.default_horizon_blocks,
            "probability_profile_path": self.probability_profile_path,
            "probability_profile_sha256": self.probability_profile_sha256,
            "full_fill_probability_profile_path": (
                self.full_fill_probability_profile_path
            ),
            "full_fill_probability_profile_sha256": (
                self.full_fill_probability_profile_sha256
            ),
            "enforce_probability_contract": self.enforce_probability_contract,
            "hierarchical_prior_path": self.hierarchical_prior_path,
            "price_buffer_profile_path": self.price_buffer_profile_path,
            "probability_prior_30s": str(self.probability_prior_30s)
            if self.probability_prior_30s is not None
            else None,
            "probability_prior_strength": self.probability_prior_strength,
            "conditional_capacity_prior": str(self.conditional_capacity_prior)
            if self.conditional_capacity_prior is not None
            else None,
            "minimum_pre_arrival_trades": self.minimum_pre_arrival_trades,
            "allow_immediate_tif_modeling": self.allow_immediate_tif_modeling,
            "allow_prior_only": self.allow_prior_only,
            "minimum_prior_samples": self.minimum_prior_samples,
            "probability_model_horizon_seconds": self.probability_model_horizon_seconds,
            "minimum_modeled_probability": str(self.minimum_modeled_probability),
            "use_probability_profile_threshold": self.use_probability_profile_threshold,
            "direct_probability_model": self.direct_probability_model,
            "evaluate_probability_on_empty_tape": self.evaluate_probability_on_empty_tape,
            "augment_source_with_modeled_residual": (
                self.augment_source_with_modeled_residual
            ),
            "calibration_status": self.calibration_status,
            "result_role": self.result_role,
            "model_version": self.model_version,
            "uses_lob_data": False,
        }


TRADE_ONLY_PROFILES: dict[str, TradeOnlyProfile] = {
    "taker_source_confirmed": TradeOnlyProfile(
        name="taker_source_confirmed",
        execution_mode=ExecutionMode.SOURCE_CONFIRMED_TAPE,
        evidence_tier=EvidenceTier.A_SOURCE_CONFIRMED,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        price_buffer=Decimal("0.005"),
        calibration_status="RULE_BASED_AUDIT",
        result_role="AUDIT_LOWER_BOUND",
        model_version="orderfilled_v2_source_adapter_v1",
    ),
    "maker_trade_through_lower": TradeOnlyProfile(
        name="maker_trade_through_lower",
        execution_mode=ExecutionMode.PASSIVE_TRADE_THROUGH_LOWER,
        evidence_tier=EvidenceTier.B_TRADE_THROUGH_INFERRED,
        default_liquidity_intent=LiquidityIntent.PASSIVE,
        participation_rate=Decimal("0.01"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        calibration_status="RULE_BASED_LOWER_BOUND",
        result_role="INFERRED_LOWER_BOUND",
    ),
    "maker_touch_survival_conservative": TradeOnlyProfile(
        name="maker_touch_survival_conservative",
        execution_mode=ExecutionMode.PASSIVE_TOUCH_SURVIVAL,
        evidence_tier=EvidenceTier.C_TOUCH_SURVIVAL,
        default_liquidity_intent=LiquidityIntent.PASSIVE,
        participation_rate=Decimal("0.01"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        touch_probability_variant="lower",
        calibration_status="INTERVAL_HEURISTIC_UNCALIBRATED",
    ),
    "maker_touch_survival_expected": TradeOnlyProfile(
        name="maker_touch_survival_expected",
        execution_mode=ExecutionMode.PASSIVE_TOUCH_SURVIVAL,
        evidence_tier=EvidenceTier.C_TOUCH_SURVIVAL,
        default_liquidity_intent=LiquidityIntent.PASSIVE,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        touch_probability_variant="expected",
        calibration_status="INTERVAL_HEURISTIC_UNCALIBRATED",
    ),
    "maker_touch_survival_upper": TradeOnlyProfile(
        name="maker_touch_survival_upper",
        execution_mode=ExecutionMode.PASSIVE_TOUCH_SURVIVAL,
        evidence_tier=EvidenceTier.C_TOUCH_SURVIVAL,
        default_liquidity_intent=LiquidityIntent.PASSIVE,
        participation_rate=Decimal("0.05"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        touch_probability_variant="upper",
        calibration_status="INTERVAL_HEURISTIC_UNCALIBRATED",
        result_role="SYNTHETIC_UPPER_BOUND",
    ),
    "taker_synthetic_q10": TradeOnlyProfile(
        name="taker_synthetic_q10",
        execution_mode=ExecutionMode.SYNTHETIC_ARRIVAL_LIQUIDITY,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.005"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=1),
        capacity_quantile=Decimal("0.10"),
        calibration_status="TAPE_PROXY_UNCALIBRATED",
    ),
    "taker_synthetic_q50": TradeOnlyProfile(
        name="taker_synthetic_q50",
        execution_mode=ExecutionMode.SYNTHETIC_ARRIVAL_LIQUIDITY,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=1),
        capacity_quantile=Decimal("0.50"),
        calibration_status="TAPE_PROXY_UNCALIBRATED",
    ),
    "taker_synthetic_q90": TradeOnlyProfile(
        name="taker_synthetic_q90",
        execution_mode=ExecutionMode.SYNTHETIC_ARRIVAL_LIQUIDITY,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.05"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=1),
        capacity_quantile=Decimal("0.90"),
        calibration_status="TAPE_PROXY_UNCALIBRATED",
        result_role="SYNTHETIC_UPPER_BOUND",
    ),
    "generative_tape_mc": TradeOnlyProfile(
        name="generative_tape_mc",
        execution_mode=ExecutionMode.GENERATIVE_TAPE_MC,
        evidence_tier=EvidenceTier.E_GENERATIVE_TAPE_MC,
        default_liquidity_intent=LiquidityIntent.AUTO_BOUND,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=30),
        monte_carlo_paths=250,
        calibration_status="POISSON_PROXY_UNCALIBRATED",
    ),
    "taker_hierarchical_expected_30s": TradeOnlyProfile(
        name="taker_hierarchical_expected_30s",
        execution_mode=ExecutionMode.HIERARCHICAL_EXPECTED_FILL,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=30),
        default_tif="GTD",
        default_horizon_blocks=100,
        probability_profile_path="config/execution/orderfilled_probability_profile.v1.json",
        hierarchical_prior_path="config/execution/trade_only_hierarchical_priors.v1.json",
        probability_prior_30s=Decimal("0.2221666250"),
        probability_prior_strength=8,
        conditional_capacity_prior=Decimal("0.7836923437"),
        minimum_pre_arrival_trades=1,
        calibration_status="ORDERFILLED_PROXY_CALIBRATED_TRANSFER_UNVALIDATED",
        result_role="EXPECTED_MODELED_SENSITIVITY",
        model_version="orderfilled_hierarchical_expected_v2",
    ),
    "taker_hierarchical_expected_120s": TradeOnlyProfile(
        name="taker_hierarchical_expected_120s",
        execution_mode=ExecutionMode.HIERARCHICAL_EXPECTED_FILL,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=120),
        default_tif="GTD",
        default_horizon_blocks=300,
        probability_profile_path="config/execution/orderfilled_probability_profile.120s.v1.json",
        hierarchical_prior_path="config/execution/trade_only_hierarchical_priors.v1.json",
        probability_prior_30s=Decimal("0.2221666250"),
        probability_prior_strength=8,
        conditional_capacity_prior=Decimal("0.7836923437"),
        minimum_pre_arrival_trades=1,
        calibration_status="ORDERFILLED_PROXY_CALIBRATED_TRANSFER_UNVALIDATED",
        result_role="EXPECTED_MODELED_SENSITIVITY",
        model_version="orderfilled_hierarchical_expected_v2",
    ),
    "taker_hierarchical_expected_300s": TradeOnlyProfile(
        name="taker_hierarchical_expected_300s",
        execution_mode=ExecutionMode.HIERARCHICAL_EXPECTED_FILL,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=300),
        default_tif="GTD",
        default_horizon_blocks=1_000,
        probability_profile_path="config/execution/orderfilled_probability_profile.300s.v1.json",
        hierarchical_prior_path="config/execution/trade_only_hierarchical_priors.v1.json",
        probability_prior_30s=Decimal("0.2221666250"),
        probability_prior_strength=8,
        conditional_capacity_prior=Decimal("0.7836923437"),
        minimum_pre_arrival_trades=1,
        calibration_status="ORDERFILLED_PROXY_REVIEW_TRANSFER_UNVALIDATED",
        result_role="EXPECTED_MODELED_SENSITIVITY",
        model_version="orderfilled_hierarchical_expected_v2",
    ),
    "central_trade_only_30s": TradeOnlyProfile(
        name="central_trade_only_30s",
        execution_mode=ExecutionMode.CENTRAL_ROUTER,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=30),
        price_buffer=Decimal("0.005"),
        default_tif="GTD",
        default_horizon_blocks=100,
        probability_profile_path="config/execution/orderfilled_probability_profile.v1.json",
        hierarchical_prior_path="config/execution/trade_only_hierarchical_priors.v1.json",
        price_buffer_profile_path="config/execution/trade_only_price_buffer.v1.json",
        minimum_pre_arrival_trades=1,
        calibration_status="ORDERFILLED_PROXY_CALIBRATED_TRANSFER_UNVALIDATED",
        result_role="CENTRAL_LOCAL_RESEARCH",
        model_version="trade_only_central_router_v1",
    ),
    "central_trade_only_120s": TradeOnlyProfile(
        name="central_trade_only_120s",
        execution_mode=ExecutionMode.CENTRAL_ROUTER,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=120),
        price_buffer=Decimal("0.005"),
        default_tif="GTD",
        default_horizon_blocks=300,
        probability_profile_path="config/execution/orderfilled_probability_profile.120s.v1.json",
        hierarchical_prior_path="config/execution/trade_only_hierarchical_priors.v1.json",
        price_buffer_profile_path="config/execution/trade_only_price_buffer.v1.json",
        minimum_pre_arrival_trades=1,
        calibration_status="ORDERFILLED_PROXY_CALIBRATED_TRANSFER_UNVALIDATED",
        result_role="SENSITIVITY_ONLY",
        model_version="trade_only_central_router_v1",
    ),
    "central_trade_only_300s": TradeOnlyProfile(
        name="central_trade_only_300s",
        execution_mode=ExecutionMode.CENTRAL_ROUTER,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=300),
        price_buffer=Decimal("0.005"),
        default_tif="GTD",
        default_horizon_blocks=1_000,
        probability_profile_path="config/execution/orderfilled_probability_profile.300s.v1.json",
        hierarchical_prior_path="config/execution/trade_only_hierarchical_priors.v1.json",
        price_buffer_profile_path="config/execution/trade_only_price_buffer.v1.json",
        minimum_pre_arrival_trades=1,
        calibration_status="ORDERFILLED_PROXY_REVIEW_TRANSFER_UNVALIDATED",
        result_role="SENSITIVITY_ONLY",
        model_version="trade_only_central_router_v1",
    ),
    "taker_tif_aware_expected_5s": TradeOnlyProfile(
        name="taker_tif_aware_expected_5s",
        execution_mode=ExecutionMode.HIERARCHICAL_EXPECTED_FILL,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=5),
        default_tif="FAK",
        default_horizon_blocks=15,
        probability_profile_path="config/execution/orderfilled_probability_profile.5s.v1.json",
        hierarchical_prior_path="config/execution/trade_only_hierarchical_priors.tif_aware.v1.json",
        probability_prior_30s=Decimal("0.05"),
        probability_prior_strength=8,
        conditional_capacity_prior=Decimal("0.7836923437"),
        minimum_pre_arrival_trades=1,
        allow_immediate_tif_modeling=True,
        allow_prior_only=True,
        minimum_prior_samples=100,
        probability_model_horizon_seconds=5,
        minimum_modeled_probability=Decimal("0.05"),
        calibration_status="ORDERFILLED_5S_PROXY_CALIBRATED_TIF_TRANSFER_UNVALIDATED",
        result_role="EXPECTED_MODELED_SENSITIVITY",
        model_version="orderfilled_tif_aware_expected_v1",
    ),
    "central_trade_only_tif_aware_5s": TradeOnlyProfile(
        name="central_trade_only_tif_aware_5s",
        execution_mode=ExecutionMode.CENTRAL_ROUTER,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=5),
        price_buffer=Decimal("0.005"),
        default_tif="FAK",
        default_horizon_blocks=15,
        probability_profile_path="config/execution/orderfilled_probability_profile.5s.v1.json",
        hierarchical_prior_path="config/execution/trade_only_hierarchical_priors.tif_aware.v1.json",
        price_buffer_profile_path="config/execution/trade_only_price_buffer.v1.json",
        probability_prior_30s=Decimal("0.05"),
        probability_prior_strength=8,
        conditional_capacity_prior=Decimal("0.7836923437"),
        minimum_pre_arrival_trades=1,
        allow_immediate_tif_modeling=True,
        allow_prior_only=True,
        minimum_prior_samples=100,
        probability_model_horizon_seconds=5,
        minimum_modeled_probability=Decimal("0.10"),
        calibration_status="ORDERFILLED_5S_PROXY_CALIBRATED_TIF_TRANSFER_UNVALIDATED",
        result_role="CENTRAL_LOCAL_RESEARCH",
        model_version="trade_only_tif_aware_central_router_v1",
    ),
    "central_trade_only_l2_reference_fak": TradeOnlyProfile(
        name="central_trade_only_l2_reference_fak",
        execution_mode=ExecutionMode.CENTRAL_ROUTER,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=1),
        price_buffer=Decimal("0.005"),
        default_tif="FAK",
        default_horizon_blocks=1,
        probability_profile_path=(
            "config/execution/fill_only_v3_l2_reference_probability.v1.json"
        ),
        price_buffer_profile_path="config/execution/trade_only_price_buffer.v1.json",
        probability_prior_strength=1,
        minimum_pre_arrival_trades=0,
        allow_immediate_tif_modeling=True,
        probability_model_horizon_seconds=1,
        use_probability_profile_threshold=True,
        direct_probability_model=True,
        evaluate_probability_on_empty_tape=True,
        calibration_status=(
            "PML2_OFFLINE_EXECUTABILITY_LABEL_RUNTIME_ORDERFILLED_ONLY"
        ),
        result_role="CENTRAL_LOCAL_RESEARCH",
        model_version="trade_only_l2_reference_fak_v1",
    ),
    "central_trade_only_l2_reference_expected_fak": TradeOnlyProfile(
        name="central_trade_only_l2_reference_expected_fak",
        execution_mode=ExecutionMode.CENTRAL_ROUTER,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=1),
        price_buffer=Decimal("0.005"),
        default_tif="FAK",
        default_horizon_blocks=1,
        probability_profile_path=(
            "config/execution/fill_only_v3_l2_reference_probability.v1.json"
        ),
        price_buffer_profile_path="config/execution/trade_only_price_buffer.v1.json",
        probability_prior_strength=1,
        minimum_pre_arrival_trades=0,
        allow_immediate_tif_modeling=True,
        probability_model_horizon_seconds=1,
        minimum_modeled_probability=Decimal(0),
        use_probability_profile_threshold=False,
        direct_probability_model=True,
        evaluate_probability_on_empty_tape=True,
        augment_source_with_modeled_residual=True,
        calibration_status=(
            "PML2_OFFLINE_EXECUTABILITY_LABEL_RUNTIME_ORDERFILLED_ONLY"
        ),
        result_role="CENTRAL_EXPECTED_LOCAL_RESEARCH",
        model_version="trade_only_l2_reference_expected_fak_v1",
    ),
    "central_trade_only_contract_aware": TradeOnlyProfile(
        name="central_trade_only_contract_aware",
        execution_mode=ExecutionMode.CENTRAL_ROUTER,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.TAKER,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(seconds=1),
        price_buffer=Decimal("0.005"),
        default_tif="FAK",
        default_horizon_blocks=1,
        probability_profile_path=(
            "config/execution/fill_only_v3_contract_fak_probability.v1.json"
        ),
        full_fill_probability_profile_path=(
            "config/execution/fill_only_v3_contract_fok_probability.v1.json"
        ),
        price_buffer_profile_path="config/execution/trade_only_price_buffer.v1.json",
        probability_prior_strength=1,
        minimum_pre_arrival_trades=0,
        allow_immediate_tif_modeling=True,
        probability_model_horizon_seconds=1,
        minimum_modeled_probability=Decimal(0),
        use_probability_profile_threshold=False,
        direct_probability_model=True,
        evaluate_probability_on_empty_tape=True,
        augment_source_with_modeled_residual=True,
        enforce_probability_contract=True,
        calibration_status="PML2_CONTRACT_SPECIFIC_OFFLINE_LABEL_RESEARCH",
        result_role="CENTRAL_EXPECTED_LOCAL_RESEARCH",
        model_version="trade_only_contract_aware_v1",
    ),
    "auto_bound": TradeOnlyProfile(
        name="auto_bound",
        execution_mode=ExecutionMode.AUTO_BOUND,
        evidence_tier=EvidenceTier.D_SYNTHETIC_ARRIVAL,
        default_liquidity_intent=LiquidityIntent.AUTO_BOUND,
        participation_rate=Decimal("0.025"),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
        capacity_quantile=Decimal("0.50"),
        calibration_status="MIXED_EVIDENCE_UNCALIBRATED",
        result_role="BOUND_SET",
    ),
}

TRADE_ONLY_PROFILES["central_trade_only_tif_aware_5s_recall"] = replace(
    TRADE_ONLY_PROFILES["central_trade_only_tif_aware_5s"],
    name="central_trade_only_tif_aware_5s_recall",
    minimum_modeled_probability=Decimal("0.05"),
    result_role="SENSITIVITY_ONLY",
    model_version="trade_only_tif_aware_central_router_recall_v1",
)

TRADE_ONLY_PROFILES["central_trade_only_contract_aware_adaptive"] = replace(
    TRADE_ONLY_PROFILES["central_trade_only_contract_aware"],
    name="central_trade_only_contract_aware_adaptive",
    probability_profile_path=(
        "config/execution/fill_only_v3_contract_fak_probability.adaptive_v22.json"
    ),
    probability_profile_sha256=(
        "2d4c6b3881f02351da06dec0cb046112726cbb4efbe8435927f77c84956e3b2c"
    ),
    full_fill_probability_profile_path=(
        "config/execution/fill_only_v3_contract_fok_probability.adaptive_v14.json"
    ),
    full_fill_probability_profile_sha256=(
        "0c158c60d91c995972f71a8574c517ce725fdd438f7591fcefb4f4dc9758a085"
    ),
    calibration_status="PML2_CONTRACT_SPECIFIC_ADAPTIVE_OFFLINE_VALIDATED",
    model_version="trade_only_contract_aware_adaptive_v1",
)

# Calibration must score the probability available when the real order reached
# the venue.  It must not let that order's later OrderFilled event promote the
# prediction through the source-confirmed branch of the central router.
TRADE_ONLY_PROFILES["taker_arrival_probability_only"] = replace(
    TRADE_ONLY_PROFILES["central_trade_only_l2_reference_expected_fak"],
    name="taker_arrival_probability_only",
    execution_mode=ExecutionMode.HIERARCHICAL_EXPECTED_FILL,
    augment_source_with_modeled_residual=False,
    calibration_status="LIVE_ORDER_LABEL_CALIBRATION_CANDIDATE",
    result_role="LIVE_LABEL_CALIBRATION_PREDICTION",
    model_version="trade_only_arrival_probability_only_v1",
)

TRADE_ONLY_PROFILES["taker_arrival_contract_probability_only"] = replace(
    TRADE_ONLY_PROFILES["central_trade_only_contract_aware"],
    name="taker_arrival_contract_probability_only",
    execution_mode=ExecutionMode.HIERARCHICAL_EXPECTED_FILL,
    augment_source_with_modeled_residual=False,
    calibration_status="CONTRACT_AWARE_LIVE_LABEL_CALIBRATION_CANDIDATE",
    result_role="LIVE_LABEL_CALIBRATION_PREDICTION",
    model_version="trade_only_arrival_contract_probability_only_v1",
)

TRADE_ONLY_PROFILES["taker_arrival_contract_probability_only_adaptive"] = replace(
    TRADE_ONLY_PROFILES["central_trade_only_contract_aware_adaptive"],
    name="taker_arrival_contract_probability_only_adaptive",
    execution_mode=ExecutionMode.HIERARCHICAL_EXPECTED_FILL,
    augment_source_with_modeled_residual=False,
    calibration_status="ADAPTIVE_CONTRACT_AWARE_LIVE_LABEL_CALIBRATION_CANDIDATE",
    result_role="LIVE_LABEL_CALIBRATION_PREDICTION",
    model_version="trade_only_arrival_contract_probability_only_adaptive_v1",
)


def get_trade_only_profile(name: str) -> TradeOnlyProfile:
    try:
        return TRADE_ONLY_PROFILES[str(name).strip().lower()]
    except KeyError as exc:
        raise ValueError(f"unknown trade-only profile: {name!r}") from exc


def list_trade_only_profiles() -> list[dict[str, Any]]:
    return [profile.as_dict() for profile in TRADE_ONLY_PROFILES.values()]


@dataclass(frozen=True)
class TradeOnlyOrder:
    order_id: str
    market_id: int
    asset_id: str
    side: Side
    limit_price: Decimal
    size: Decimal
    signal_block: int | None
    signal_ts: datetime
    tif: TimeInForce = "GTC"
    liquidity_intent: LiquidityIntent = LiquidityIntent.TAKER
    latency: timedelta = timedelta(seconds=1)
    latency_blocks: int = 0
    horizon: timedelta = timedelta(minutes=5)
    horizon_blocks: int = 2_000
    lookback: timedelta = timedelta(minutes=5)
    lookback_blocks: int = 300
    allow_partial_fill: bool = True
    signal_source_trade_id: str | None = None
    random_seed: int = 0
    monte_carlo_paths: int | None = None
    market_slug: str | None = None
    market_title: str | None = None
    category: str | None = None
    league: str | None = None
    market_end_ts: datetime | None = None

    @property
    def arrival_ts(self) -> datetime:
        return self.signal_ts + self.latency

    @property
    def arrival_block(self) -> int | None:
        if self.signal_block is None:
            return None
        return self.signal_block + max(0, self.latency_blocks)

    @property
    def deadline_ts(self) -> datetime:
        if self.tif in {"IOC", "FAK", "FOK"}:
            return self.arrival_ts + timedelta(seconds=1)
        return self.arrival_ts + self.horizon

    @property
    def deadline_block(self) -> int | None:
        if self.arrival_block is None:
            return None
        if self.tif in {"IOC", "FAK", "FOK"}:
            return self.arrival_block + min(1, max(0, self.horizon_blocks))
        return self.arrival_block + max(0, self.horizon_blocks)


def with_trade_only_profile(
    order: TradeOnlyOrder, profile: str | TradeOnlyProfile
) -> TradeOnlyOrder:
    """Bind execution-window defaults while preserving strategy order intent."""

    resolved = get_trade_only_profile(profile) if isinstance(profile, str) else profile
    return replace(
        order,
        latency=resolved.latency,
        horizon=resolved.horizon,
        horizon_blocks=resolved.default_horizon_blocks,
        lookback_blocks=resolved.default_lookback_blocks,
        monte_carlo_paths=(
            order.monte_carlo_paths
            if order.monte_carlo_paths is not None
            else resolved.monte_carlo_paths or None
        ),
    )


@dataclass(frozen=True)
class TradeOnlyFill:
    order_id: str
    execution_mode: str
    evidence_tier: str
    trigger_type: str
    filled_size: Decimal
    exec_price: Decimal
    fill_ts: datetime
    fill_block: int | None
    source_trade_ids: tuple[str, ...] = ()
    source_tx_hashes: tuple[str, ...] = ()
    source_log_indexes: tuple[int, ...] = ()
    touch_ts: datetime | None = None
    cross_ts: datetime | None = None
    fill_time_lower: datetime | None = None
    fill_time_upper: datetime | None = None
    p_fill_1s: Decimal | None = None
    p_fill_5s: Decimal | None = None
    p_fill_30s: Decimal | None = None
    p_fill_horizon: Decimal | None = None
    conditional_fill_fraction: Decimal | None = None
    conditional_fill_size: Decimal | None = None
    unconditional_expected_fill_size: Decimal | None = None
    participation_rate: Decimal | None = None
    capacity_q10: Decimal | None = None
    capacity_q50: Decimal | None = None
    capacity_q90: Decimal | None = None
    latent_mid: Decimal | None = None
    latent_spread: Decimal | None = None
    impact_ticks: Decimal | None = None
    model_version: str = ""
    feature_snapshot_hash: str = ""
    random_seed: int | None = None
    run_liquidity_ledger_id: str = ""

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        for key in (
            "fill_ts",
            "touch_ts",
            "cross_ts",
            "fill_time_lower",
            "fill_time_upper",
        ):
            value = row[key]
            row[key] = value.isoformat() if value is not None else None
        row["source_trade_ids"] = list(self.source_trade_ids)
        row["source_tx_hashes"] = list(self.source_tx_hashes)
        row["source_log_indexes"] = list(self.source_log_indexes)
        return _json_ready(row)


@dataclass(frozen=True)
class TradeOnlyOrderResult:
    order_id: str
    status: str
    requested_size: Decimal
    filled_size: Decimal
    unfilled_size: Decimal
    avg_price: Decimal
    reason: str
    execution_mode: str
    evidence_tier: str
    result_role: str
    calibration_status: str
    fills: tuple[TradeOnlyFill, ...] = ()
    probability_bounds: dict[str, Decimal] = field(default_factory=dict)
    capacity_bounds: dict[str, Decimal] = field(default_factory=dict)
    monte_carlo: dict[str, Any] | None = None
    scenarios: dict[str, Any] | None = None
    model_diagnostics: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["fills"] = [fill.as_dict() for fill in self.fills]
        return _json_ready(row)


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value
