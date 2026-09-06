"""Evidence-tiered HTTP-facing service for Prediction L2 Replay V2."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from quant.backtest.rust_kernel import rust_kernel_available

from .availability import load_default_gap_availability_artifact
from .contracts import (
    FillEvidenceTier,
    ModeledExecutionMode,
    ModeledExecutionPolicy,
    ModeledFillEstimate,
    OrderStatus,
    Pml2OrderIntent,
    RawOrderSide,
    ReplayAuditMode,
    SubmissionOnStaleBook,
    SubmissionPolicy,
    canonical_value,
    qty,
)
from .profiles import list_pml2_profiles
from .rust_kernel import (
    AUTO_RUST_MAX_LEVELS_PER_ORDER,
    AUTO_RUST_MIN_ORDERS,
)
from .service import (
    REQUEST_FIELDS,
    Pml2DataNotReadyError,
    Pml2RequestError,
    build_pml2_readiness,
    run_pml2_replay,
)
from .survival import load_default_maker_survival_artifact

SCHEMA_VERSION = "prediction-l2-replay-api-v2"
MODEL_NAME = "PREDICTION_L2_REPLAY_V2"
DEFAULT_PROFILE = "realistic"

V2_REQUEST_FIELDS = REQUEST_FIELDS | {
    "submission_policy",
    "submissionPolicy",
    "modeled_execution",
    "modeledExecution",
    "audit_mode",
    "auditMode",
}
SUBMISSION_POLICY_FIELDS = frozenset(
    {
        "on_stale_book",
        "onStaleBook",
        "max_data_wait_ms",
        "maxDataWaitMs",
        "on_timeout",
        "onTimeout",
    }
)
MODELED_EXECUTION_FIELDS = frozenset({"maker", "gap", "random_seed", "randomSeed"})
MAKER_FORECAST_FIELDS = frozenset(
    {
        "decision_ts",
        "decisionTs",
        "horizon_seconds",
        "horizonSeconds",
        "category",
        "side",
        "quote_position",
        "quotePosition",
        "queue_bucket",
        "queueBucket",
        "minimum_stratum_samples",
        "minimumStratumSamples",
        "order_size",
        "orderSize",
    }
)
GAP_FORECAST_FIELDS = frozenset(
    {
        "decision_ts",
        "decisionTs",
        "gap_ms",
        "gapMs",
        "requested_size",
        "requestedSize",
        "category",
        "price",
        "tte_bucket",
        "tteBucket",
        "liquidity_regime",
        "liquidityRegime",
        "minimum_stratum_samples",
        "minimumStratumSamples",
    }
)
MATRIX_FIELDS = V2_REQUEST_FIELDS | {"variants"}

EXECUTION_VARIANTS: dict[str, dict[str, Any]] = {
    "strict_fok": {
        "profile": "strict",
        "tif": "FOK",
        "submission": {"onStaleBook": "FAIL_CLOSED", "maxDataWaitMs": 0},
    },
    "strict_fak_control": {
        "profile": "strict",
        "tif": "FAK",
        "submission": {"onStaleBook": "FAIL_CLOSED", "maxDataWaitMs": 0},
    },
    "realistic_fak": {
        "profile": "realistic",
        "tif": "FAK",
        "submission": {"onStaleBook": "FAIL_CLOSED", "maxDataWaitMs": 0},
    },
    "wait30_fak": {
        "profile": "realistic",
        "tif": "FAK",
        "submission": {"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 30_000},
    },
    "wait30_gtd30": {
        "profile": "realistic",
        "tif": "GTD",
        "gtd_duration_ms": 30_000,
        "submission": {"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 30_000},
    },
    "wait30_gtd30_maker_expected": {
        "profile": "realistic",
        "tif": "GTD",
        "gtd_duration_ms": 30_000,
        "submission": {"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 30_000},
        "modeled": {"maker": "EXPECTED", "gap": "OFF"},
    },
    "wait30_fak_gap_expected": {
        "profile": "realistic",
        "tif": "FAK",
        "submission": {"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 30_000},
        "modeled": {"maker": "OFF", "gap": "EXPECTED"},
    },
    "wait120_fak": {
        "profile": "realistic",
        "tif": "FAK",
        "submission": {"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 120_000},
    },
    "optimistic_fak": {
        "profile": "optimistic",
        "tif": "FAK",
        "submission": {"onStaleBook": "FAIL_CLOSED", "maxDataWaitMs": 0},
    },
}


def list_prediction_l2_v2_profiles() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "default_profile": DEFAULT_PROFILE,
        "base_profiles": list_pml2_profiles(),
        "execution_variants": [
            {"name": name, **canonical_value(value)}
            for name, value in EXECUTION_VARIANTS.items()
        ],
        "evidence_tiers": [item.value for item in FillEvidenceTier],
        "audit_modes": [item.value for item in ReplayAuditMode],
    }


def build_prediction_l2_v2_readiness() -> dict[str, Any]:
    base = build_pml2_readiness()
    maker = load_default_maker_survival_artifact()
    gap = load_default_gap_availability_artifact()
    maker_readiness = (
        maker.readiness()
        if maker is not None
        else {"available": False, "reason": "ARTIFACT_MISSING_OR_INVALID"}
    )
    gap_readiness = (
        gap.readiness()
        if gap is not None
        else {"available": False, "reason": "ARTIFACT_MISSING_OR_INVALID"}
    )
    rust_available = rust_kernel_available()
    return {
        "schema_version": SCHEMA_VERSION,
        "ready": bool(base.get("ready")),
        "ready_for_observed_replay": bool(base.get("ready_for_contract_replay")),
        "ready_for_modeled_estimates": maker is not None and gap is not None,
        "production_ready": False,
        "default_profile": DEFAULT_PROFILE,
        "hard_boundaries": {
            "stale_l2_observed_fill_allowed": False,
            "elapsed_age_alone_invalidates_synced_book": False,
            "known_transport_gap_requires_later_snapshot": True,
            "tif_semantics_preserved": True,
            "modeled_estimates_are_execution_matches": False,
            "quiet_but_covered_is_not_stale": True,
            "known_transport_gap_is_fillable": False,
        },
        "capabilities": {
            **dict(base.get("capabilities") or {}),
            "wait_for_fresh_local_data_gate": True,
            "modeled_fill_estimate_ledger": True,
            "execution_increment_matrix": True,
            "gap_artificial_masking_forecast": gap is not None,
            "maker_conditional_size_forecast": maker is not None,
            "transport_coverage_proof_on_observed_fill": True,
            "event_driven_book_validity": True,
        },
        "maker_survival": maker_readiness,
        "gap_availability": gap_readiness,
        "snapshot_batch_backend": {
            "rust_kernel_available": rust_available,
            "supported_tif": ["FAK", "FOK", "IOC"],
            "scope": "INDEPENDENT_ARRIVAL_SNAPSHOT_TAKER_BATCHES",
            "auto_rust_min_orders": AUTO_RUST_MIN_ORDERS,
            "auto_rust_max_materialized_levels_per_order": (
                AUTO_RUST_MAX_LEVELS_PER_ORDER
            ),
            "dynamic_delta_queue_gtd_backend": "PYTHON",
            "dynamic_delta_queue_gtd_profile_status": "PROFILED",
            "dynamic_rust_migration_status": "NOT_JUSTIFIED_BY_CURRENT_PROFILE",
            "dynamic_recommended_audit_mode": ReplayAuditMode.CHAIN_ONLY.value,
            "dynamic_benchmark": (
                "backtest_framework/nautilus_trader_comparison/"
                "pml2_dynamic_profile/large_11000_final.json"
            ),
        },
        "audit_modes": {
            "default": ReplayAuditMode.FULL.value,
            "formal": ReplayAuditMode.FULL.value,
            "large_local_research": ReplayAuditMode.CHAIN_ONLY.value,
        },
        "v1_readiness": base,
    }


def run_prediction_l2_v2_replay(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise Pml2RequestError("request body must be a JSON object")
    _reject_unknown(payload, V2_REQUEST_FIELDS, "V2 request")
    submission = _submission_policy(
        _value(payload, "submission_policy", "submissionPolicy")
    )
    modeled = _modeled_policy(_value(payload, "modeled_execution", "modeledExecution"))
    audit_mode = _enum(
        ReplayAuditMode,
        _value(payload, "audit_mode", "auditMode", default="FULL"),
        "audit mode",
    )
    _validate_wait_order_groups(payload, submission)
    v1_payload = {
        key: deepcopy(value) for key, value in payload.items() if key in REQUEST_FIELDS
    }
    base, session, orders = run_pml2_replay(
        v1_payload,
        _session_options={
            "submission_policy": submission,
            "execution_model_name": MODEL_NAME,
            "audit_mode": audit_mode,
        },
        _return_context=True,
    )
    estimates = _modeled_estimates(
        session=session,
        orders=orders,
        policy=modeled,
    )
    matches = []
    for match in session.matches:
        if not match.source_event_ids:
            raise Pml2DataNotReadyError(
                f"observed L2 match {match.fill_id} has no source_event_ids"
            )
        matches.append(
            {**match.as_dict(), "evidence_tier": FillEvidenceTier.OBSERVED_L2.value}
        )
    observed_size = sum((match.qty for match in session.matches), Decimal(0))
    expected_modeled_size = sum(
        (estimate.expected_size for estimate in estimates), Decimal(0)
    )
    sampled_modeled_size = sum(
        (estimate.sampled_fill_size or Decimal(0) for estimate in estimates),
        Decimal(0),
    )
    return {
        **base,
        "schema_version": SCHEMA_VERSION,
        "execution_model": MODEL_NAME,
        "submission_policy": submission.as_dict(),
        "modeled_execution_policy": modeled.as_dict(),
        "audit_mode": audit_mode.value,
        "execution_matches": matches,
        "submission_audit": [
            session.submission_audit(order.order_id) for order in orders
        ],
        "modeled_fill_estimates": [item.as_dict() for item in estimates],
        "observed_execution": {
            "fill_count": len(matches),
            "filled_size": format(observed_size, "f"),
            "pnl_eligibility": "OBSERVED_PNL_ELIGIBLE",
        },
        "modeled_execution": {
            "estimate_count": len(estimates),
            "expected_size": format(expected_modeled_size, "f"),
            "sampled_size": format(sampled_modeled_size, "f"),
            "pnl_eligibility": "EXPECTED_OR_SCENARIO_PNL_ONLY",
        },
    }


def run_prediction_l2_v2_execution_matrix(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise Pml2RequestError("request body must be a JSON object")
    _reject_unknown(payload, MATRIX_FIELDS, "V2 execution matrix request")
    requested = payload.get("variants") or list(EXECUTION_VARIANTS)
    if not isinstance(requested, list) or not requested:
        raise Pml2RequestError("variants must be a non-empty list")
    names = [str(item) for item in requested]
    unknown = sorted(set(names) - set(EXECUTION_VARIANTS))
    if unknown:
        raise Pml2RequestError(f"unknown V2 execution variants: {unknown}")
    if len(names) != len(set(names)):
        raise Pml2RequestError("variants must not contain duplicates")
    root_run_id = str(_value(payload, "run_id", "runId", default="pml2-v2-matrix"))
    common = {
        key: deepcopy(value) for key, value in payload.items() if key != "variants"
    }
    common.pop("run_id", None)
    common.pop("runId", None)
    results: dict[str, dict[str, Any]] = {}
    for name in names:
        variant = EXECUTION_VARIANTS[name]
        request = _variant_payload(common, variant)
        request["run_id"] = f"{root_run_id}:{name}"
        results[name] = run_prediction_l2_v2_replay(request)
    strict_observed = Decimal(
        str(
            results.get("strict_fok", {})
            .get("observed_execution", {})
            .get("filled_size", 0)
        )
    )
    comparison = {}
    for name, result in results.items():
        observed = Decimal(str(result["observed_execution"]["filled_size"]))
        modeled_size = Decimal(str(result["modeled_execution"]["expected_size"]))
        comparison[name] = {
            "observed_filled_size": format(observed, "f"),
            "modeled_expected_size": format(modeled_size, "f"),
            "delta_observed_vs_strict": format(observed - strict_observed, "f"),
            "status_counts": result["status_counts"],
        }
    return {
        "schema_version": "prediction-l2-execution-matrix-v2",
        "run_id": root_run_id,
        "variants": names,
        "comparison": comparison,
        "results": results,
    }


def run_prediction_l2_v2_maker_forecast(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise Pml2RequestError("request body must be a JSON object")
    _reject_unknown(payload, MAKER_FORECAST_FIELDS, "V2 maker forecast")
    artifact = load_default_maker_survival_artifact()
    if artifact is None:
        raise Pml2DataNotReadyError("Maker survival artifact is missing or invalid")
    decision = _datetime(_value(payload, "decision_ts", "decisionTs"), "decision_ts")
    horizon = _integer(
        _value(payload, "horizon_seconds", "horizonSeconds"),
        "horizon_seconds",
        minimum=1,
    )
    category = str(_value(payload, "category", default="GLOBAL"))
    side = _enum(RawOrderSide, _value(payload, "side"), "side")
    quote_position = str(
        _value(payload, "quote_position", "quotePosition", default="AT_BEST")
    )
    queue_bucket = str(
        _value(payload, "queue_bucket", "queueBucket", default="UNKNOWN")
    )
    minimum = _integer(
        _value(
            payload,
            "minimum_stratum_samples",
            "minimumStratumSamples",
            default=20,
        ),
        "minimum_stratum_samples",
        minimum=1,
    )
    order_size = qty(_value(payload, "order_size", "orderSize", default="1"))
    forecast = artifact.forecast(
        decision_ts=decision,
        horizon_seconds=horizon,
        category=category,
        side=side,
        quote_position=quote_position,
        queue_bucket=queue_bucket,
        minimum_stratum_samples=minimum,
    )
    conditional_fraction = artifact.conditional_fill_fraction(
        decision_ts=decision,
        horizon_seconds=horizon,
        category=category,
        side=side,
        quote_position=quote_position,
        queue_bucket=queue_bucket,
        minimum_stratum_samples=minimum,
    )
    conditional_size = qty(order_size * conditional_fraction)
    return {
        "schema_version": "prediction-l2-maker-forecast-api-v2",
        "forecast": {
            **forecast.as_dict(),
            "p_any_fill": forecast.as_dict()["fill_probability"],
            "conditional_fill_fraction": format(conditional_fraction, "f"),
            "conditional_expected_size": format(conditional_size, "f"),
            "expected_size": format(
                qty(forecast.fill_probability * conditional_size), "f"
            ),
        },
        "model_readiness": artifact.readiness(),
    }


def run_prediction_l2_v2_gap_forecast(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise Pml2RequestError("request body must be a JSON object")
    _reject_unknown(payload, GAP_FORECAST_FIELDS, "V2 gap forecast")
    artifact = load_default_gap_availability_artifact()
    if artifact is None:
        raise Pml2DataNotReadyError("gap availability artifact is missing or invalid")
    forecast = artifact.forecast(
        decision_ts=_datetime(
            _value(payload, "decision_ts", "decisionTs"), "decision_ts"
        ),
        gap_ms=_integer(_value(payload, "gap_ms", "gapMs"), "gap_ms", minimum=1),
        requested_size=_value(payload, "requested_size", "requestedSize"),
        category=str(_value(payload, "category", default="GLOBAL")),
        price_value=_value(payload, "price", default="0.5"),
        tte_bucket=str(_value(payload, "tte_bucket", "tteBucket", default="UNKNOWN")),
        liquidity_regime=str(
            _value(payload, "liquidity_regime", "liquidityRegime", default="UNKNOWN")
        ),
        minimum_stratum_samples=_integer(
            _value(
                payload,
                "minimum_stratum_samples",
                "minimumStratumSamples",
                default=50,
            ),
            "minimum_stratum_samples",
            minimum=1,
        ),
    )
    return {
        "schema_version": "prediction-l2-gap-forecast-api-v1",
        "forecast": forecast.as_dict(),
        "model_readiness": artifact.readiness(),
    }


def _modeled_estimates(
    *,
    session: Any,
    orders: tuple[Pml2OrderIntent, ...],
    policy: ModeledExecutionPolicy,
) -> tuple[ModeledFillEstimate, ...]:
    gap_artifact = load_default_gap_availability_artifact()
    estimates: list[ModeledFillEstimate] = []
    for order in orders:
        state = session.orders[order.order_id]
        remaining = state.remaining_size
        if remaining <= 0:
            continue
        if policy.maker != ModeledExecutionMode.OFF and state.maker_survival_forecast:
            forecast = state.maker_survival_forecast
            horizon = int(forecast["horizon_seconds"])
            conditional_fraction = Decimal(
                str(forecast.get("conditional_fill_fraction") or 0)
            )
            estimates.append(
                _estimate(
                    order=order,
                    model_kind="MAKER_SURVIVAL",
                    mode=policy.maker,
                    probability=Decimal(str(forecast["fill_probability"])),
                    conditional_size=qty(remaining * conditional_fraction),
                    expected_price=order.limit_price,
                    horizon_seconds=horizon,
                    domain_status=str(forecast["domain_status"]),
                    model_version=str(forecast["model_version"]),
                    artifact_hash=str(forecast["artifact_hash"]),
                    seed=policy.random_seed,
                    profile=session.profile.name,
                )
            )
        if (
            policy.gap != ModeledExecutionMode.OFF
            and gap_artifact is not None
            and state.status == OrderStatus.DATA_NOT_READY
        ):
            gap_ms = (
                session.submission_policy.max_data_wait_ms
                if state.data_wait_deadline is not None
                else session.profile.max_book_age_ms
            )
            forecast = gap_artifact.forecast(
                decision_ts=order.signal_ts,
                gap_ms=max(1, gap_ms),
                requested_size=remaining,
                category=str(order.metadata.get("category") or "GLOBAL"),
                price_value=order.limit_price,
                tte_bucket=str(order.metadata.get("tte_bucket") or "UNKNOWN"),
                liquidity_regime=str(
                    order.metadata.get("liquidity_regime") or "UNKNOWN"
                ),
            )
            estimates.append(
                _estimate(
                    order=order,
                    model_kind="L2_GAP_AVAILABILITY",
                    mode=policy.gap,
                    probability=forecast.p_executable,
                    conditional_size=forecast.expected_available_size,
                    expected_price=order.limit_price,
                    horizon_seconds=max(1, int(gap_ms / 1000)),
                    domain_status=forecast.domain_status,
                    model_version=forecast.model_version,
                    artifact_hash=forecast.artifact_hash,
                    seed=policy.random_seed,
                    profile=session.profile.name,
                )
            )
    return tuple(estimates)


def _estimate(
    *,
    order: Pml2OrderIntent,
    model_kind: str,
    mode: ModeledExecutionMode,
    probability: Decimal,
    conditional_size: Decimal,
    expected_price: Decimal,
    horizon_seconds: int,
    domain_status: str,
    model_version: str,
    artifact_hash: str,
    seed: int,
    profile: str,
) -> ModeledFillEstimate:
    expected = qty(probability * conditional_size)
    sampled: Decimal | None = None
    tier = FillEvidenceTier.MODELED_EXPECTED
    random_seed: int | None = None
    if mode == ModeledExecutionMode.MONTE_CARLO:
        tier = FillEvidenceTier.MODELED_MONTE_CARLO
        random_seed = _order_seed(seed, order.order_id, model_kind)
        draw = Decimal(random_seed) / Decimal(2**64 - 1)
        sampled = conditional_size if draw < probability else Decimal(0)
    return ModeledFillEstimate(
        estimate_id=f"{order.run_id}:{order.order_id}:{model_kind.lower()}",
        run_id=order.run_id,
        order_id=order.order_id,
        execution_model=MODEL_NAME,
        profile=profile,
        evidence_tier=tier,
        model_kind=model_kind,
        requested_size=order.requested_share_size,
        p_any_fill=probability,
        conditional_expected_size=conditional_size,
        expected_size=expected,
        expected_price=expected_price,
        horizon_seconds=horizon_seconds,
        decision_ts=order.signal_ts,
        domain_status=domain_status,
        model_version=model_version,
        artifact_hash=artifact_hash,
        random_seed=random_seed,
        sampled_fill_size=sampled,
    )


def _variant_payload(
    common: Mapping[str, Any], variant: Mapping[str, Any]
) -> dict[str, Any]:
    result = deepcopy(dict(common))
    result["profile"] = variant["profile"]
    result["submissionPolicy"] = deepcopy(variant["submission"])
    result["modeledExecution"] = deepcopy(
        variant.get("modeled") or {"maker": "OFF", "gap": "OFF"}
    )
    result["orders"] = [
        _override_order(row, variant) for row in result.get("orders", [])
    ]
    for group in result.get("orderGroups", result.get("order_groups", [])):
        for field in ("legs", "hedgeLegs", "hedge_legs"):
            if field in group:
                group[field] = [_override_order(row, variant) for row in group[field]]
    return result


def _override_order(
    row: Mapping[str, Any], variant: Mapping[str, Any]
) -> dict[str, Any]:
    result = deepcopy(dict(row))
    tif = str(variant["tif"])
    result["tif"] = tif
    result.pop("expires_at", None)
    result.pop("expiresAt", None)
    if tif == "GTD":
        raw_submit = _value(
            result,
            "submit_ts",
            "submitTs",
            "observed_ts",
            "observedTs",
            "signal_ts",
            "signalTs",
        )
        submit = _datetime(raw_submit, "submit_ts")
        expires = submit + timedelta(milliseconds=int(variant["gtd_duration_ms"]))
        result["expiresAt"] = expires.isoformat()
    return result


def _submission_policy(value: Any) -> SubmissionPolicy:
    if value is None:
        return SubmissionPolicy()
    if not isinstance(value, Mapping):
        raise Pml2RequestError("submissionPolicy must be a JSON object")
    _reject_unknown(value, SUBMISSION_POLICY_FIELDS, "submission policy")
    on_stale = _enum(
        SubmissionOnStaleBook,
        _value(value, "on_stale_book", "onStaleBook", default="FAIL_CLOSED"),
        "on_stale_book",
    )
    wait_ms = _integer(
        _value(value, "max_data_wait_ms", "maxDataWaitMs", default=0),
        "max_data_wait_ms",
        minimum=0,
    )
    timeout = _enum(
        OrderStatus,
        _value(value, "on_timeout", "onTimeout", default="DATA_NOT_READY"),
        "on_timeout",
    )
    try:
        return SubmissionPolicy(on_stale, wait_ms, timeout)
    except ValueError as exc:
        raise Pml2RequestError(str(exc)) from exc


def _modeled_policy(value: Any) -> ModeledExecutionPolicy:
    if value is None:
        return ModeledExecutionPolicy()
    if not isinstance(value, Mapping):
        raise Pml2RequestError("modeledExecution must be a JSON object")
    _reject_unknown(value, MODELED_EXECUTION_FIELDS, "modeled execution")
    return ModeledExecutionPolicy(
        maker=_enum(
            ModeledExecutionMode,
            _value(value, "maker", default="OFF"),
            "maker",
        ),
        gap=_enum(
            ModeledExecutionMode,
            _value(value, "gap", default="OFF"),
            "gap",
        ),
        random_seed=_integer(
            _value(value, "random_seed", "randomSeed", default=0),
            "random_seed",
            minimum=0,
        ),
    )


def _validate_wait_order_groups(
    payload: Mapping[str, Any], policy: SubmissionPolicy
) -> None:
    if policy.on_stale_book != SubmissionOnStaleBook.WAIT_FOR_FRESH:
        return
    groups = _value(payload, "order_groups", "orderGroups", default=[])
    for group in groups:
        if str(group.get("policy") or "").upper() != "SEQUENTIAL":
            raise Pml2RequestError(
                "WAIT_FOR_FRESH currently requires SEQUENTIAL order groups"
            )


def _order_seed(seed: int, order_id: str, model_kind: str) -> int:
    digest = hashlib.sha256(f"{seed}|{order_id}|{model_kind}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _reject_unknown(
    value: Mapping[str, Any], allowed: frozenset[str] | set[str], label: str
) -> None:
    unknown = set(value) - set(allowed)
    if unknown:
        raise Pml2RequestError(f"unknown {label} fields: {sorted(unknown)}")


def _value(value: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in value:
            return value[key]
    return default


def _integer(value: Any, field_name: str, *, minimum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise Pml2RequestError(f"{field_name} must be an integer") from exc
    if parsed < minimum:
        raise Pml2RequestError(f"{field_name} must be >= {minimum}")
    return parsed


def _enum(enum_type: Any, value: Any, field_name: str) -> Any:
    try:
        return enum_type(str(value).upper())
    except ValueError as exc:
        choices = ", ".join(item.value for item in enum_type)
        raise Pml2RequestError(f"{field_name} must be one of: {choices}") from exc


def _datetime(value: Any, field_name: str) -> datetime:
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        )
    except (TypeError, ValueError) as exc:
        raise Pml2RequestError(f"{field_name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Pml2RequestError(f"{field_name} must be timezone-aware")
    return parsed
