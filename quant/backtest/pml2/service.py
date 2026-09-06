"""Strict JSON/HTTP-facing service for Prediction L2 Replay V1."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from quant.simulator.economics import FeeSchedule, FeeScheduleRegistry

from .adapters import (
    DEFAULT_ARCHIVE_CANDIDATES,
    Pml2ArchiveSnapshotLoader,
    default_l2_archive_dir,
)
from .contracts import (
    BinaryMarketIdentity,
    BookDeltaEvent,
    BookFrameBatchEvent,
    BookLevel,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    CtfMatchType,
    EconomicBookSide,
    MarketLifecycleEvent,
    OrderAmountUnit,
    OrderGroupIntent,
    OrderGroupPolicy,
    Outcome,
    Pml2OrderIntent,
    RawOrderSide,
    ReplayValidationMode,
    TimeInForce,
    TradeEvent,
    TradingMode,
    VenueAdmissionStatus,
    canonical_hash,
)
from .profiles import REGISTRY_PATH, get_pml2_profile, list_pml2_profiles
from .session import ReplayExecutionSession
from .survival import load_default_maker_survival_artifact

SCHEMA_VERSION = "prediction-l2-replay-api-v1"
DEFAULT_PROFILE = "realistic"
MAX_EVENTS = 50_000
MAX_ORDERS = 2_000
PROFILE_MATRIX = ("strict", "realistic", "optimistic")

REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "requestId",
        "run_id",
        "runId",
        "profile",
        "cold_restore_used",
        "coldRestoreUsed",
        "events",
        "orders",
        "order_groups",
        "orderGroups",
        "cancels",
        "fee_schedules",
        "feeSchedules",
        "archive_restore",
        "archiveRestore",
        "contract_validation",
        "contractValidation",
    }
)
EVENT_FIELDS = frozenset(
    {
        "type",
        "event_id",
        "eventId",
        "snapshot_id",
        "snapshotId",
        "condition_id",
        "conditionId",
        "market_id",
        "marketId",
        "asset_id",
        "assetId",
        "outcome",
        "exchange_ts",
        "exchangeTs",
        "source_received_ts",
        "sourceReceivedTs",
        "local_ts",
        "localTs",
        "book_epoch",
        "bookEpoch",
        "source",
        "sequence",
        "is_full_depth",
        "isFullDepth",
        "is_truncated",
        "isTruncated",
        "depth_scope",
        "depthScope",
        "tick_size",
        "tickSize",
        "min_order_size",
        "minOrderSize",
        "book_hash",
        "bookHash",
        "bids",
        "asks",
        "side",
        "price",
        "new_size",
        "newSize",
        "size",
        "aggressor_side",
        "aggressorSide",
        "event_group_id",
        "eventGroupId",
        "evidence_link_id",
        "evidenceLinkId",
        "evidence_kind",
        "evidenceKind",
        "source_event_ids",
        "sourceEventIds",
        "linked_trade_event_ids",
        "linkedTradeEventIds",
        "trading_mode",
        "tradingMode",
        "requires_fresh_snapshot",
        "requiresFreshSnapshot",
        "updates",
    }
)
BATCH_UPDATE_FIELDS = frozenset(
    {
        "event_id",
        "eventId",
        "sequence",
        "side",
        "price",
        "new_size",
        "newSize",
        "linked_trade_event_ids",
        "linkedTradeEventIds",
    }
)
ORDER_FIELDS = frozenset(
    {
        "order_id",
        "orderId",
        "strategy_id",
        "strategyId",
        "condition_id",
        "conditionId",
        "market_id",
        "marketId",
        "asset_id",
        "assetId",
        "outcome",
        "side",
        "size",
        "amount_unit",
        "amountUnit",
        "signed_maker_amount",
        "signedMakerAmount",
        "signed_taker_amount",
        "signedTakerAmount",
        "venue_admission",
        "venueAdmission",
        "venue_admission_evidence_id",
        "venueAdmissionEvidenceId",
        "limit_price",
        "limitPrice",
        "tif",
        "signal_ts",
        "signalTs",
        "observed_ts",
        "observedTs",
        "submit_ts",
        "submitTs",
        "post_only",
        "postOnly",
        "expires_at",
        "expiresAt",
        "entry_latency_ms",
        "entryLatencyMs",
        "cancel_latency_ms",
        "cancelLatencyMs",
        "response_latency_ms",
        "responseLatencyMs",
        "venue_delay_ms",
        "venueDelayMs",
        "fee_rate",
        "feeRate",
        "fee_exponent",
        "feeExponent",
        "match_type_hint",
        "matchTypeHint",
        "fill_block",
        "fillBlock",
        "metadata",
    }
)
CANCEL_FIELDS = frozenset(
    {
        "order_id",
        "orderId",
        "signal_ts",
        "signalTs",
        "cancel_latency_ms",
        "cancelLatencyMs",
    }
)
MATRIX_REQUEST_FIELDS = REQUEST_FIELDS | {"profiles"}
FEE_SCHEDULE_FIELDS = frozenset(
    {
        "schedule_id",
        "scheduleId",
        "asset_id",
        "assetId",
        "condition_id",
        "conditionId",
        "effective_from",
        "effectiveFrom",
        "effective_until",
        "effectiveUntil",
        "platform_fee_rate",
        "platformFeeRate",
        "platform_fee_exponent",
        "platformFeeExponent",
        "platform_taker_only",
        "platformTakerOnly",
        "builder_code",
        "builderCode",
        "builder_taker_fee_bps",
        "builderTakerFeeBps",
        "builder_maker_fee_bps",
        "builderMakerFeeBps",
        "rounding_unit",
        "roundingUnit",
        "economics_regime_id",
        "economicsRegimeId",
        "source",
    }
)
ARCHIVE_RESTORE_FIELDS = frozenset(
    {
        "condition_id",
        "conditionId",
        "market_id",
        "marketId",
        "yes_asset_id",
        "yesAssetId",
        "no_asset_id",
        "noAssetId",
        "start_time",
        "startTime",
        "end_time",
        "endTime",
        "maker_horizon_seconds",
        "makerHorizonSeconds",
        "max_events",
        "maxEvents",
        "book_epoch",
        "bookEpoch",
        "source_files",
        "sourceFiles",
    }
)
ARCHIVE_SOURCE_FILE_FIELDS = frozenset({"path", "sha256"})
MAX_ARCHIVE_SOURCE_FILES = 256
ORDER_GROUP_FIELDS = frozenset(
    {
        "group_id",
        "groupId",
        "strategy_id",
        "strategyId",
        "policy",
        "legs",
        "hedge_legs",
        "hedgeLegs",
        "metadata",
    }
)
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
    }
)
CONTRACT_VALIDATION_FIELDS = frozenset(
    {
        "mode",
        "identity_mappings",
        "identityMappings",
        "require_binary_pair",
        "requireBinaryPair",
        "allow_research_zero_fee",
        "allowResearchZeroFee",
        "sequence_step_by_source",
        "sequenceStepBySource",
    }
)
IDENTITY_MAPPING_FIELDS = frozenset(
    {
        "condition_id",
        "conditionId",
        "market_id",
        "marketId",
        "yes_asset_id",
        "yesAssetId",
        "no_asset_id",
        "noAssetId",
    }
)


@dataclass(frozen=True)
class _ReplayContractValidation:
    explicit: bool
    mode: ReplayValidationMode
    identity_mappings: tuple[BinaryMarketIdentity, ...]
    require_binary_pair: bool
    allow_research_zero_fee: bool
    sequence_step_by_source: Mapping[str, int]


class Pml2ServiceError(RuntimeError):
    status_code = 500
    error_code = "PML2_ERROR"

    def __init__(
        self,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "error": str(self),
            "error_code": self.error_code,
        }
        if self.details:
            result["details"] = self.details
        return result


class Pml2RequestError(Pml2ServiceError):
    status_code = 400
    error_code = "INVALID_PML2_REQUEST"


class Pml2LimitError(Pml2ServiceError):
    status_code = 413
    error_code = "PML2_REQUEST_LIMIT_EXCEEDED"


class Pml2DataNotReadyError(Pml2ServiceError):
    status_code = 409
    error_code = "PML2_HISTORICAL_DATA_NOT_READY"


def build_pml2_readiness() -> dict[str, Any]:
    profiles = list_pml2_profiles()
    survival = load_default_maker_survival_artifact()
    survival_readiness = (
        survival.readiness()
        if survival is not None
        else {"available": False, "reason": "ARTIFACT_MISSING_OR_INVALID"}
    )
    existing: list[str] = []
    for path in DEFAULT_ARCHIVE_CANDIDATES:
        try:
            if path.exists() and next(path.rglob("*.parquet"), None) is not None:
                existing.append(str(path.resolve()))
        except OSError:
            continue
    return {
        "schema_version": SCHEMA_VERSION,
        "ready": bool(profiles),
        "ready_for_contract_replay": True,
        "maturity": "RESEARCH_GRADE",
        "production_ready": False,
        "production_blockers": [
            "AUTHENTICATED_MAKER_FILL_OUTCOME_DIVERSITY_INSUFFICIENT",
        ],
        "historical_source_authority": "XUE_NATIVE_L2_BOUNDED_WINDOW_RESTORE_ONLY",
        "historical_coverage_scope": "PER_REQUEST_INTERVAL_ONLY",
        "global_historical_coverage_proven": False,
        "historical_clock_policy": (
            "BASELINE_AND_RAW_EVENT_DUAL_CLOCK_WITH_FRAME_EVIDENCE"
        ),
        "market_impact_policy": "VISIBLE_DEPTH_ONLY_WITH_UNMODELED_REMAINDER",
        "historical_source_status": "READY" if existing else "NOT_PROBED_OR_MISSING",
        "default_profile": DEFAULT_PROFILE,
        "uses_lob_data": True,
        "uses_orderfilled_as_taker_gate": False,
        "profile_registry": str(REGISTRY_PATH),
        "selected_archive_root": str(default_l2_archive_dir()),
        "archive_roots": existing,
        "capabilities": {
            "run_scoped_session": True,
            "dual_clock": True,
            "yes_no_shared_capacity": True,
            "maker_economic_queue": True,
            "trade_delta_reconciliation": True,
            "cold_archive_point_in_time_restore": True,
            "execution_match_finalizer_adapter": True,
            "effective_dated_fee_schedule": True,
            "dynamic_strategy_python_api": True,
            "atomic_level_batches": True,
            "api_cold_archive_restore": True,
            "multi_leg_atomic_coordinator": True,
            "maker_archive_trade_delta_restore": True,
            "counterfactual_large_order_gate": True,
            "maker_survival_forecast": survival is not None,
            "native_ioc_taker": True,
            "native_quote_budget_market_buy": True,
            "signed_maker_taker_amount_replay": True,
            "venue_admission_evidence_audit": True,
            "formal_identity_mapping_validation": True,
            "formal_fee_fail_closed": True,
            "binary_pair_contract_validation": True,
            "event_driven_book_validity": True,
            "elapsed_age_alone_invalidates_synced_book": False,
            "known_transport_gap_requires_later_snapshot": True,
            "receive_and_local_clock_provenance": True,
            "transport_continuity_aware_freshness": True,
            "receive_and_local_clock_freshness": True,
            "ttl_fallback_without_coverage_proof": False,
        },
        "maker_survival": survival_readiness,
    }


def run_pml2_maker_forecast(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return the versioned Maker time-to-fill forecast used by replay."""

    if not isinstance(payload, Mapping):
        raise Pml2RequestError("request body must be a JSON object")
    _reject_unknown(payload, MAKER_FORECAST_FIELDS, "maker forecast")
    artifact = load_default_maker_survival_artifact()
    if artifact is None:
        raise Pml2DataNotReadyError("Maker survival artifact is missing or invalid")
    forecast = artifact.forecast(
        decision_ts=_datetime(
            _value(payload, "decision_ts", "decisionTs"), "decision_ts"
        ),
        horizon_seconds=_integer(
            _value(payload, "horizon_seconds", "horizonSeconds"),
            "horizon_seconds",
            minimum=1,
        ),
        category=str(_value(payload, "category", default="GLOBAL")),
        side=_enum(RawOrderSide, _value(payload, "side"), "side"),
        quote_position=str(
            _value(payload, "quote_position", "quotePosition", default="AT_BEST")
        ),
        queue_bucket=str(
            _value(payload, "queue_bucket", "queueBucket", default="UNKNOWN")
        ),
        minimum_stratum_samples=_integer(
            _value(
                payload,
                "minimum_stratum_samples",
                "minimumStratumSamples",
                default=20,
            ),
            "minimum_stratum_samples",
            minimum=1,
        ),
    )
    return {
        "schema_version": "prediction-l2-maker-forecast-api-v1",
        "forecast": forecast.as_dict(),
        "model_readiness": artifact.readiness(),
    }


def run_pml2_replay(
    payload: Mapping[str, Any],
    *,
    _session_options: Mapping[str, Any] | None = None,
    _return_context: bool = False,
) -> Any:
    if not isinstance(payload, Mapping):
        raise Pml2RequestError("request body must be a JSON object")
    _reject_unknown(payload, REQUEST_FIELDS, "request")
    run_id = str(_value(payload, "run_id", "runId", default="pml2-api-run"))
    validation = _contract_validation(
        _value(payload, "contract_validation", "contractValidation")
    )
    profile_name = str(_value(payload, "profile", default=DEFAULT_PROFILE)).lower()
    try:
        profile = get_pml2_profile(profile_name)
    except (ValueError, OSError) as exc:
        raise Pml2RequestError(str(exc)) from exc
    raw_events = _value(payload, "events", default=[])
    raw_orders = _value(payload, "orders", default=[])
    raw_order_groups = _value(payload, "order_groups", "orderGroups", default=[])
    raw_cancels = _value(payload, "cancels", default=[])
    raw_fee_schedules = _value(
        payload, "fee_schedules", "feeSchedules", default=[]
    )
    raw_archive_restore = _value(
        payload, "archive_restore", "archiveRestore", default=[]
    )
    if not isinstance(raw_events, list):
        raise Pml2RequestError("events must be a list")
    if not isinstance(raw_orders, list):
        raise Pml2RequestError("orders must be a list")
    if not isinstance(raw_order_groups, list):
        raise Pml2RequestError("order_groups must be a list")
    if not raw_orders and not raw_order_groups:
        raise Pml2RequestError("orders or order_groups must be non-empty")
    if not isinstance(raw_cancels, list):
        raise Pml2RequestError("cancels must be a list")
    if not isinstance(raw_fee_schedules, list):
        raise Pml2RequestError("fee_schedules must be a list")
    if not isinstance(raw_archive_restore, list):
        raise Pml2RequestError("archive_restore must be a list")
    if len(raw_events) > MAX_EVENTS:
        raise Pml2LimitError(f"events exceeds {MAX_EVENTS}")
    parsed_events = tuple(_event(row, profile.feed_latency_ms) for row in raw_events)
    parsed_orders = tuple(_order(row, run_id) for row in raw_orders)
    parsed_groups = tuple(_order_group(row, run_id) for row in raw_order_groups)
    group_orders = tuple(
        leg
        for group in parsed_groups
        for leg in (*group.legs, *group.hedge_legs)
    )
    all_orders = (*parsed_orders, *group_orders)
    if len(all_orders) > MAX_ORDERS:
        raise Pml2LimitError(f"total orders exceeds {MAX_ORDERS}")
    order_ids = [item.order_id for item in all_orders]
    if len(order_ids) != len(set(order_ids)):
        raise Pml2RequestError("order_id values must be unique across request")
    for order in all_orders:
        profile.validate_calibration_for(order.signal_ts)
    fee_schedules = _fee_schedule_registry(raw_fee_schedules)
    identity_by_condition = _validate_contract_identities(
        validation=validation,
        events=parsed_events,
        orders=all_orders,
        archive_rows=raw_archive_restore,
        fee_schedule_rows=raw_fee_schedules,
    )
    fee_policy = _validate_fee_policy(
        validation=validation,
        orders=all_orders,
        fee_schedules=fee_schedules,
        profile=profile,
    )
    use_cold_restore = bool(raw_archive_restore) or _boolean(
        _value(payload, "cold_restore_used", "coldRestoreUsed", default=False),
        "cold_restore_used",
    )
    session = ReplayExecutionSession(
        run_id=run_id,
        profile=profile,
        cold_restore_used=use_cold_restore,
        sequence_step_by_source=validation.sequence_step_by_source,
        fee_schedules=fee_schedules,
        **dict(_session_options or {}),
    )
    source_plan = _restore_archive_events(
        session=session,
        rows=raw_archive_restore,
        orders=all_orders,
        feed_latency_ms=profile.feed_latency_ms,
        identity_by_condition=identity_by_condition,
    )
    pair_coverage = _binary_pair_coverage(
        validation=validation,
        orders=all_orders,
        events=parsed_events,
        source_plan=source_plan,
        profile=profile,
    )
    if validation.require_binary_pair and pair_coverage["reasons"]:
        raise Pml2DataNotReadyError(
            "; ".join(pair_coverage["reasons"]),
            details={"contract_coverage": pair_coverage},
        )
    for event in parsed_events:
        if isinstance(event, BookSnapshotEvent):
            session.ingest_snapshot(event)
        elif isinstance(event, BookFrameBatchEvent):
            session.ingest_frame_batch(event)
        elif isinstance(event, BookLevelBatchEvent):
            session.ingest_level_batch(event)
        elif isinstance(event, BookDeltaEvent):
            session.ingest_delta(event)
        elif isinstance(event, TradeEvent):
            session.ingest_trade(event)
        else:
            session.ingest_lifecycle(event)
    for order in parsed_orders:
        session.submit_order(order)
    for group in parsed_groups:
        session.submit_order_group(group)
    for row in raw_cancels:
        if not isinstance(row, Mapping):
            raise Pml2RequestError("each cancel must be a JSON object")
        _reject_unknown(row, CANCEL_FIELDS, "cancel")
        session.submit_cancel(
            _required_text(row, "order_id", "orderId"),
            signal_ts=_datetime(_value(row, "signal_ts", "signalTs"), "signal_ts"),
            cancel_latency_ms=_optional_int(
                _value(row, "cancel_latency_ms", "cancelLatencyMs"),
                "cancel_latency_ms",
            ),
        )
    try:
        session.run()
    except LookupError as exc:
        raise Pml2DataNotReadyError(str(exc)) from exc
    except ValueError as exc:
        if "fee schedule condition does not match" in str(exc):
            raise Pml2RequestError(str(exc)) from exc
        raise
    report = session.report()
    contract_coverage = _contract_coverage_report(
        validation=validation,
        pair_coverage=pair_coverage,
        fee_policy=fee_policy,
        checked_record_count=(
            len(parsed_events)
            + len(all_orders)
            + len(raw_archive_restore)
            + len(raw_fee_schedules)
        ),
    )
    result = {
        **report,
        "request_id": str(_value(payload, "request_id", "requestId", default="")),
        "execution_matches": [item.as_dict() for item in session.matches],
        "settlement_receipts": [],
        "data_source_plan": source_plan,
        "contract_coverage": contract_coverage,
    }
    if _return_context:
        return result, session, all_orders
    return result


def run_pml2_profile_matrix(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise Pml2RequestError("request body must be a JSON object")
    _reject_unknown(payload, MATRIX_REQUEST_FIELDS, "request")
    requested = payload.get("profiles", PROFILE_MATRIX)
    if not isinstance(requested, list | tuple) or not requested:
        raise Pml2RequestError("profiles must be a non-empty list")
    profiles = tuple(str(item).strip().lower() for item in requested)
    if len(set(profiles)) != len(profiles):
        raise Pml2RequestError("profiles must not contain duplicates")
    unknown = sorted(set(profiles) - set(PROFILE_MATRIX))
    if unknown:
        raise Pml2RequestError(f"unknown PML2 profiles: {unknown}")
    root_run_id = str(_value(payload, "run_id", "runId", default="pml2-matrix"))
    common = dict(payload)
    common.pop("profiles", None)
    common.pop("profile", None)
    common.pop("run_id", None)
    common.pop("runId", None)
    results: dict[str, dict[str, Any]] = {}
    for profile in profiles:
        results[profile] = run_pml2_replay(
            {
                **common,
                "run_id": f"{root_run_id}:{profile}",
                "profile": profile,
            }
        )
    strict_size = Decimal(str(results.get("strict", {}).get("filled_size", "0")))
    comparison = {
        profile: {
            "filled_size": result["filled_size"],
            "fill_ratio": result["fill_ratio"],
            "match_count": result["match_count"],
            "status_counts": result["status_counts"],
            "delta_filled_size_vs_strict": format(
                Decimal(str(result["filled_size"])) - strict_size,
                "f",
            ),
            "data_quality_status": result["coverage_manifest"][
                "data_quality_status"
            ],
        }
        for profile, result in results.items()
    }
    return {
        "schema_version": "prediction-l2-replay-profile-matrix-v1",
        "run_id": root_run_id,
        "profiles": list(profiles),
        "comparison": comparison,
        "results": results,
    }


def _contract_validation(value: Any) -> _ReplayContractValidation:
    """Parse the opt-in replay validation contract.

    Omission intentionally preserves the historical research API.  Supplying
    the object opts the caller into explicit fee semantics; FORMAL additionally
    requires an independently frozen binary market mapping and, by default,
    both outcome books.
    """

    if value is None:
        return _ReplayContractValidation(
            explicit=False,
            mode=ReplayValidationMode.RESEARCH,
            identity_mappings=(),
            require_binary_pair=False,
            allow_research_zero_fee=True,
            sequence_step_by_source={},
        )
    if not isinstance(value, Mapping):
        raise Pml2RequestError("contract_validation must be a JSON object")
    _reject_unknown(value, CONTRACT_VALIDATION_FIELDS, "contract validation")
    mode = _enum(
        ReplayValidationMode,
        _value(value, "mode", default="RESEARCH"),
        "contract validation mode",
    )
    raw_mappings = _value(
        value,
        "identity_mappings",
        "identityMappings",
        default=[],
    )
    if not isinstance(raw_mappings, list):
        raise Pml2RequestError("identity_mappings must be a list")
    mappings: list[BinaryMarketIdentity] = []
    for row in raw_mappings:
        if not isinstance(row, Mapping):
            raise Pml2RequestError("each identity mapping must be a JSON object")
        _reject_unknown(row, IDENTITY_MAPPING_FIELDS, "identity mapping")
        try:
            mappings.append(
                BinaryMarketIdentity(
                    condition_id=_required_text(
                        row, "condition_id", "conditionId"
                    ),
                    market_id=_required_text(row, "market_id", "marketId"),
                    yes_asset_id=_required_text(
                        row, "yes_asset_id", "yesAssetId"
                    ),
                    no_asset_id=_required_text(
                        row, "no_asset_id", "noAssetId"
                    ),
                )
            )
        except ValueError as exc:
            raise Pml2RequestError(f"invalid identity mapping: {exc}") from exc
    _ensure_unique_identity_mappings(tuple(mappings))
    require_pair_raw = _value(
        value, "require_binary_pair", "requireBinaryPair"
    )
    require_binary_pair = (
        mode == ReplayValidationMode.FORMAL
        if require_pair_raw is None
        else _boolean(require_pair_raw, "require_binary_pair")
    )
    allow_research_zero_fee = _boolean(
        _value(
            value,
            "allow_research_zero_fee",
            "allowResearchZeroFee",
            default=False,
        ),
        "allow_research_zero_fee",
    )
    if mode == ReplayValidationMode.FORMAL and not mappings:
        raise Pml2RequestError(
            "FORMAL contract validation requires identity_mappings"
        )
    if mode == ReplayValidationMode.FORMAL and allow_research_zero_fee:
        raise Pml2RequestError(
            "allow_research_zero_fee is only valid in RESEARCH mode"
        )
    raw_sequence_steps = _value(
        value,
        "sequence_step_by_source",
        "sequenceStepBySource",
        default={},
    )
    if not isinstance(raw_sequence_steps, Mapping):
        raise Pml2RequestError("sequence_step_by_source must be a JSON object")
    sequence_step_by_source: dict[str, int] = {}
    for raw_source, raw_step in raw_sequence_steps.items():
        source = str(raw_source).strip()
        if not source:
            raise Pml2RequestError(
                "sequence_step_by_source keys must be non-empty source ids"
            )
        sequence_step_by_source[source] = _integer(
            raw_step,
            f"sequence_step_by_source[{source}]",
            minimum=1,
        )
    return _ReplayContractValidation(
        explicit=True,
        mode=mode,
        identity_mappings=tuple(mappings),
        require_binary_pair=require_binary_pair,
        allow_research_zero_fee=allow_research_zero_fee,
        sequence_step_by_source=sequence_step_by_source,
    )


def _ensure_unique_identity_mappings(
    mappings: tuple[BinaryMarketIdentity, ...],
) -> None:
    seen_condition: set[str] = set()
    seen_market: set[str] = set()
    seen_asset: set[str] = set()
    for mapping in mappings:
        condition = mapping.condition_id.casefold()
        market = mapping.market_id.casefold()
        assets = {
            mapping.yes_asset_id.casefold(),
            mapping.no_asset_id.casefold(),
        }
        if condition in seen_condition:
            raise Pml2RequestError(
                f"duplicate identity mapping condition_id: {mapping.condition_id}"
            )
        if market in seen_market:
            raise Pml2RequestError(
                f"duplicate identity mapping market_id: {mapping.market_id}"
            )
        overlap = assets & seen_asset
        if overlap:
            raise Pml2RequestError(
                f"asset_id appears in multiple identity mappings: {sorted(overlap)}"
            )
        seen_condition.add(condition)
        seen_market.add(market)
        seen_asset.update(assets)


def _validate_contract_identities(
    *,
    validation: _ReplayContractValidation,
    events: tuple[
        BookSnapshotEvent
        | BookFrameBatchEvent
        | BookLevelBatchEvent
        | BookDeltaEvent
        | TradeEvent
        | MarketLifecycleEvent,
        ...,
    ],
    orders: tuple[Pml2OrderIntent, ...],
    archive_rows: list[Any],
    fee_schedule_rows: list[Any],
) -> dict[str, BinaryMarketIdentity]:
    index = {
        item.condition_id.casefold(): item
        for item in validation.identity_mappings
    }
    if not index:
        return index
    for event in events:
        if isinstance(event, MarketLifecycleEvent):
            _validate_identity_record(
                index,
                condition_id=event.condition_id,
                market_id=event.market_id,
                label=f"event {event.event_id}",
            )
        else:
            event_id = (
                event.snapshot_id
                if isinstance(event, BookSnapshotEvent)
                else event.event_id
            )
            identity_events = (
                event.batches
                if isinstance(event, BookFrameBatchEvent)
                else (event,)
            )
            for identity_event in identity_events:
                _validate_identity_record(
                    index,
                    condition_id=identity_event.condition_id,
                    market_id=identity_event.market_id,
                    asset_id=identity_event.asset_id,
                    outcome=identity_event.outcome,
                    label=f"event {event_id}",
                )
    for order in orders:
        _validate_identity_record(
            index,
            condition_id=order.condition_id,
            market_id=order.market_id,
            asset_id=order.asset_id,
            outcome=order.outcome,
            label=f"order {order.order_id}",
        )
    for row in archive_rows:
        if not isinstance(row, Mapping):
            raise Pml2RequestError("each archive restore must be a JSON object")
        condition_id = _required_text(row, "condition_id", "conditionId")
        market_id = _required_text(row, "market_id", "marketId")
        _validate_identity_record(
            index,
            condition_id=condition_id,
            market_id=market_id,
            asset_id=_required_text(row, "yes_asset_id", "yesAssetId"),
            outcome=Outcome.YES,
            label="archive restore YES",
        )
        _validate_identity_record(
            index,
            condition_id=condition_id,
            market_id=market_id,
            asset_id=_required_text(row, "no_asset_id", "noAssetId"),
            outcome=Outcome.NO,
            label="archive restore NO",
        )
    for row in fee_schedule_rows:
        if not isinstance(row, Mapping):
            raise Pml2RequestError("each fee schedule must be a JSON object")
        condition_id = _required_text(row, "condition_id", "conditionId")
        asset_id = _required_text(row, "asset_id", "assetId")
        mapping = index.get(condition_id.casefold())
        if mapping is None:
            raise Pml2RequestError(
                "fee schedule has no frozen identity mapping for "
                f"condition_id={condition_id}"
            )
        if asset_id.casefold() == mapping.yes_asset_id.casefold():
            outcome = Outcome.YES
        elif asset_id.casefold() == mapping.no_asset_id.casefold():
            outcome = Outcome.NO
        else:
            raise Pml2RequestError(
                "fee schedule asset_id does not match frozen token mapping"
            )
        try:
            mapping.validate_coordinates(
                condition_id=condition_id,
                market_id=mapping.market_id,
                asset_id=asset_id,
                outcome=outcome,
            )
        except ValueError as exc:
            raise Pml2RequestError(
                f"fee schedule identity mismatch: {exc}"
            ) from exc
    return index


def _validate_identity_record(
    index: Mapping[str, BinaryMarketIdentity],
    *,
    condition_id: str,
    market_id: str,
    label: str,
    asset_id: str | None = None,
    outcome: Outcome | None = None,
) -> None:
    mapping = index.get(condition_id.casefold())
    if mapping is None:
        raise Pml2RequestError(
            f"{label} has no frozen identity mapping for condition_id={condition_id}"
        )
    try:
        mapping.validate_coordinates(
            condition_id=condition_id,
            market_id=market_id,
            asset_id=asset_id,
            outcome=outcome,
        )
    except ValueError as exc:
        raise Pml2RequestError(f"{label} identity mismatch: {exc}") from exc


def _validate_fee_policy(
    *,
    validation: _ReplayContractValidation,
    orders: tuple[Pml2OrderIntent, ...],
    fee_schedules: FeeScheduleRegistry | None,
    profile: Any,
) -> str:
    if not validation.explicit:
        return "LEGACY_ORDER_INTENT_FIELDS_UNVERIFIED"
    if validation.mode == ReplayValidationMode.RESEARCH:
        if fee_schedules is not None:
            return "EFFECTIVE_DATED_SCHEDULE"
        if all(order.fee_rate > 0 for order in orders):
            return "EXPLICIT_ORDER_INTENT_FEES"
        if validation.allow_research_zero_fee:
            return "EXPLICIT_RESEARCH_ZERO_FEE"
        raise Pml2RequestError(
            "RESEARCH replay with zero/default order fees requires "
            "allow_research_zero_fee=true"
        )
    if fee_schedules is None:
        raise Pml2RequestError(
            "FORMAL replay requires effective-dated fee_schedules; "
            "order fee defaults are not accepted"
        )
    for order in orders:
        entry_latency_ms = (
            profile.entry_latency_ms
            if order.entry_latency_ms is None
            else max(0, order.entry_latency_ms)
        )
        venue_delay_ms = (
            profile.venue_delay_ms
            if order.venue_delay_ms is None
            else max(0, order.venue_delay_ms)
        )
        earliest_fill = order.submit_ts + timedelta(
            milliseconds=entry_latency_ms + venue_delay_ms
        )
        try:
            schedule = fee_schedules.resolve(order.asset_id, at=earliest_fill)
        except LookupError as exc:
            reason = (
                "FEE_SCHEDULE_MISSING "
                f"order_id={order.order_id} asset_id={order.asset_id} "
                f"at={earliest_fill.isoformat()}"
            )
            raise Pml2DataNotReadyError(
                reason,
                details={"contract_coverage": {"status": "REJECTED", "reasons": [reason]}},
            ) from exc
        if schedule.condition_id.casefold() != order.condition_id.casefold():
            raise Pml2RequestError(
                f"fee schedule condition does not match order {order.order_id}"
            )
    return "EFFECTIVE_DATED_SCHEDULE_REQUIRED"


def _binary_pair_coverage(
    *,
    validation: _ReplayContractValidation,
    orders: tuple[Pml2OrderIntent, ...],
    events: tuple[
        BookSnapshotEvent
        | BookFrameBatchEvent
        | BookLevelBatchEvent
        | BookDeltaEvent
        | TradeEvent
        | MarketLifecycleEvent,
        ...,
    ],
    source_plan: Mapping[str, Any],
    profile: Any,
) -> dict[str, Any]:
    snapshots: dict[str, dict[str, list[datetime]]] = {}
    for event in events:
        if isinstance(event, BookSnapshotEvent):
            snapshots.setdefault(event.condition_id.casefold(), {}).setdefault(
                event.outcome.value, []
            ).append(event.source_received_ts or event.exchange_ts)
    restores = source_plan.get("restores", [])
    if isinstance(restores, list):
        for restore in restores:
            if not isinstance(restore, Mapping):
                continue
            condition_id = str(restore.get("condition_id", "")).casefold()
            snapshot_available_ts = restore.get(
                "snapshot_received_ts",
                restore.get("snapshot_exchange_ts", {}),
            )
            if condition_id and isinstance(snapshot_available_ts, Mapping):
                for outcome, raw_times in snapshot_available_ts.items():
                    if not isinstance(raw_times, list):
                        continue
                    for raw_time in raw_times:
                        try:
                            parsed = _datetime(
                                raw_time, "restored snapshot available_ts"
                            )
                        except Pml2RequestError:
                            continue
                        snapshots.setdefault(condition_id, {}).setdefault(
                            str(outcome).upper(), []
                        ).append(parsed)
    requested_conditions: dict[str, tuple[str, datetime]] = {}
    for order in orders:
        entry_latency_ms = (
            profile.entry_latency_ms
            if order.entry_latency_ms is None
            else max(0, order.entry_latency_ms)
        )
        arrival = order.submit_ts + timedelta(milliseconds=entry_latency_ms)
        normalized = order.condition_id.casefold()
        existing = requested_conditions.get(normalized)
        if existing is None or arrival < existing[1]:
            requested_conditions[normalized] = (order.condition_id, arrival)
    conditions: list[dict[str, Any]] = []
    reasons: list[str] = []
    expected = {Outcome.YES.value, Outcome.NO.value}
    for normalized, (condition_id, earliest_arrival) in sorted(
        requested_conditions.items()
    ):
        observed = {
            outcome
            for outcome, timestamps in snapshots.get(normalized, {}).items()
            if any(timestamp <= earliest_arrival for timestamp in timestamps)
        }
        missing = sorted(expected - observed)
        status = "COMPLETE" if not missing else "INCOMPLETE"
        conditions.append(
            {
                "condition_id": condition_id,
                "earliest_exchange_arrival_ts": earliest_arrival.isoformat(),
                "snapshot_outcomes_before_arrival": sorted(observed),
                "missing_snapshot_outcomes": missing,
                "status": status,
            }
        )
        if missing:
            reasons.append(
                "BINARY_PAIR_INCOMPLETE "
                f"condition_id={condition_id} "
                f"missing_snapshot_outcomes={','.join(missing)}"
            )
    return {
        "required": validation.require_binary_pair,
        "status": "COMPLETE" if not reasons else "INCOMPLETE",
        "conditions": conditions,
        "reasons": reasons,
    }


def _contract_coverage_report(
    *,
    validation: _ReplayContractValidation,
    pair_coverage: Mapping[str, Any],
    fee_policy: str,
    checked_record_count: int,
) -> dict[str, Any]:
    reasons = list(pair_coverage.get("reasons", []))
    if not validation.explicit:
        reasons.append("LEGACY_RESEARCH_REQUEST_NO_EXPLICIT_VALIDATION")
    elif not validation.identity_mappings:
        reasons.append("IDENTITY_MAPPING_NOT_PROVIDED")
    if fee_policy == "LEGACY_ORDER_INTENT_FIELDS_UNVERIFIED":
        reasons.append("FEE_POLICY_UNVERIFIED_LEGACY_DEFAULT")
    elif fee_policy == "EXPLICIT_RESEARCH_ZERO_FEE":
        reasons.append("RESEARCH_ZERO_FEE_EXPLICIT")
    status = (
        "VALID"
        if validation.mode == ReplayValidationMode.FORMAL and not reasons
        else "RESEARCH_UNVERIFIED"
    )
    return {
        "mode": validation.mode.value,
        "explicit": validation.explicit,
        "status": status,
        "reasons": reasons,
        "identity_mapping_count": len(validation.identity_mappings),
        "identity_checked_record_count": (
            checked_record_count if validation.identity_mappings else 0
        ),
        "fee_policy": fee_policy,
        "sequence_step_by_source": dict(validation.sequence_step_by_source),
        "binary_pair": dict(pair_coverage),
    }


def _event(
    row: Any, feed_latency_ms: int
) -> (
    BookSnapshotEvent
    | BookLevelBatchEvent
    | BookDeltaEvent
    | TradeEvent
    | MarketLifecycleEvent
):
    if not isinstance(row, Mapping):
        raise Pml2RequestError("each event must be a JSON object")
    _reject_unknown(row, EVENT_FIELDS, "event")
    kind = _required_text(row, "type").upper()
    exchange_ts = _datetime(
        _value(row, "exchange_ts", "exchangeTs"), "exchange_ts"
    )
    source_received_raw = _value(
        row,
        "source_received_ts",
        "sourceReceivedTs",
    )
    source_received_ts = (
        None
        if source_received_raw is None
        else _datetime(source_received_raw, "source_received_ts")
    )
    local_raw = _value(row, "local_ts", "localTs")
    local_ts = (
        (source_received_ts or exchange_ts)
        + timedelta(milliseconds=max(0, feed_latency_ms))
        if local_raw is None
        else _datetime(local_raw, "local_ts")
    )
    if kind == "LIFECYCLE":
        return MarketLifecycleEvent(
            event_id=_required_text(row, "event_id", "eventId"),
            condition_id=_required_text(row, "condition_id", "conditionId"),
            market_id=_required_text(row, "market_id", "marketId"),
            exchange_ts=exchange_ts,
            local_ts=local_ts,
            trading_mode=_enum(
                TradingMode,
                _value(row, "trading_mode", "tradingMode"),
                "trading_mode",
            ),
            source=_required_text(row, "source"),
            source_sequence=_optional_int(_value(row, "sequence"), "sequence") or 0,
            requires_fresh_snapshot=_boolean(
                _value(
                    row,
                    "requires_fresh_snapshot",
                    "requiresFreshSnapshot",
                    default=False,
                ),
                "requires_fresh_snapshot",
            ),
        )
    common = {
        "condition_id": _required_text(row, "condition_id", "conditionId"),
        "market_id": _required_text(row, "market_id", "marketId"),
        "asset_id": _required_text(row, "asset_id", "assetId"),
        "outcome": _enum(Outcome, _value(row, "outcome"), "outcome"),
        "exchange_ts": exchange_ts,
        "source_received_ts": source_received_ts,
        "local_ts": local_ts,
        "book_epoch": _integer(
            _value(row, "book_epoch", "bookEpoch", default=0),
            "book_epoch",
            minimum=0,
        ),
        "source": _required_text(row, "source"),
    }
    sequence = _optional_int(_value(row, "sequence"), "sequence")
    if kind in {"LEVEL_BATCH", "BOOK_LEVEL_BATCH"}:
        raw_updates = _value(row, "updates")
        if not isinstance(raw_updates, list) or not raw_updates:
            raise Pml2RequestError("updates must be a non-empty list")
        batch_id = _required_text(row, "event_id", "eventId")
        updates: list[BookDeltaEvent] = []
        for index, update in enumerate(raw_updates):
            if not isinstance(update, Mapping):
                raise Pml2RequestError("each batch update must be a JSON object")
            _reject_unknown(update, BATCH_UPDATE_FIELDS, "batch update")
            linked = _value(
                update,
                "linked_trade_event_ids",
                "linkedTradeEventIds",
                default=[],
            )
            if not isinstance(linked, list):
                raise Pml2RequestError(
                    "batch linked_trade_event_ids must be a list"
                )
            updates.append(
                BookDeltaEvent(
                    event_id=str(
                        _value(
                            update,
                            "event_id",
                            "eventId",
                            default=f"{batch_id}:{index}",
                        )
                    ),
                    sequence=_optional_int(
                        _value(update, "sequence"), "batch update sequence"
                    ),
                    side=_enum(
                        EconomicBookSide, _value(update, "side"), "side"
                    ),
                    price=_decimal(_value(update, "price"), "price"),
                    new_size=_decimal(
                        _value(update, "new_size", "newSize"), "new_size"
                    ),
                    linked_trade_event_ids=tuple(str(item) for item in linked),
                    **common,
                )
            )
        return BookLevelBatchEvent(
            event_id=batch_id,
            updates=tuple(updates),
            source_sequence=sequence or 0,
            **common,
        )
    if kind == "SNAPSHOT":
        return BookSnapshotEvent(
            snapshot_id=_required_text(row, "snapshot_id", "snapshotId", "event_id", "eventId"),
            sequence=sequence,
            bids=_levels(_value(row, "bids", default=[]), "bids"),
            asks=_levels(_value(row, "asks", default=[]), "asks"),
            is_full_depth=_boolean(
                _value(row, "is_full_depth", "isFullDepth", default=True),
                "is_full_depth",
            ),
            is_truncated=_boolean(
                _value(row, "is_truncated", "isTruncated", default=False),
                "is_truncated",
            ),
            depth_scope=str(
                _value(row, "depth_scope", "depthScope", default="FULL")
            ),
            tick_size=_optional_decimal(
                _value(row, "tick_size", "tickSize"), "tick_size"
            ),
            min_order_size=_optional_decimal(
                _value(row, "min_order_size", "minOrderSize"),
                "min_order_size",
            ),
            book_hash=str(
                _value(row, "book_hash", "bookHash", default="")
            ),
            **common,
        )
    if kind == "DELTA":
        linked = _value(
            row,
            "linked_trade_event_ids",
            "linkedTradeEventIds",
            default=[],
        )
        if not isinstance(linked, list):
            raise Pml2RequestError("linked_trade_event_ids must be a list")
        return BookDeltaEvent(
            event_id=_required_text(row, "event_id", "eventId"),
            sequence=sequence,
            side=_enum(EconomicBookSide, _value(row, "side"), "side"),
            price=_decimal(_value(row, "price"), "price"),
            new_size=_decimal(_value(row, "new_size", "newSize"), "new_size"),
            linked_trade_event_ids=tuple(str(item) for item in linked),
            **common,
        )
    if kind == "TRADE":
        source_event_ids = _value(
            row, "source_event_ids", "sourceEventIds", default=[]
        )
        if not isinstance(source_event_ids, list):
            raise Pml2RequestError("source_event_ids must be a list")
        return TradeEvent(
            event_id=_required_text(row, "event_id", "eventId"),
            source_sequence=sequence or 0,
            price=_decimal(_value(row, "price"), "price"),
            size=_decimal(_value(row, "size"), "size"),
            aggressor_side=_enum(
                RawOrderSide,
                _value(row, "aggressor_side", "aggressorSide"),
                "aggressor_side",
            ),
            event_group_id=str(
                _value(row, "event_group_id", "eventGroupId", default="")
            ),
            evidence_link_id=str(
                _value(row, "evidence_link_id", "evidenceLinkId", default="")
            ),
            evidence_kind=str(
                _value(row, "evidence_kind", "evidenceKind", default="TRADE_PRINT")
            ),
            source_event_ids=tuple(str(item) for item in source_event_ids),
            **common,
        )
    raise Pml2RequestError(
        "event type must be SNAPSHOT, LEVEL_BATCH, DELTA, TRADE, or LIFECYCLE"
    )


def _order(row: Any, run_id: str) -> Pml2OrderIntent:
    if not isinstance(row, Mapping):
        raise Pml2RequestError("each order must be a JSON object")
    _reject_unknown(row, ORDER_FIELDS, "order")
    signal_ts = _datetime(_value(row, "signal_ts", "signalTs"), "signal_ts")
    observed_ts = _datetime(
        _value(row, "observed_ts", "observedTs", default=signal_ts),
        "observed_ts",
    )
    submit_ts = _datetime(
        _value(row, "submit_ts", "submitTs", default=observed_ts),
        "submit_ts",
    )
    fill_block = _optional_int(
        _value(row, "fill_block", "fillBlock"), "fill_block"
    )
    raw_metadata = _value(row, "metadata", default={})
    if not isinstance(raw_metadata, Mapping):
        raise Pml2RequestError("order metadata must be a JSON object")
    metadata = dict(raw_metadata)
    if fill_block is not None:
        metadata["fill_block"] = fill_block
    return Pml2OrderIntent(
        run_id=run_id,
        order_id=_required_text(row, "order_id", "orderId"),
        strategy_id=_required_text(row, "strategy_id", "strategyId"),
        condition_id=_required_text(row, "condition_id", "conditionId"),
        market_id=_required_text(row, "market_id", "marketId"),
        asset_id=_required_text(row, "asset_id", "assetId"),
        outcome=_enum(Outcome, _value(row, "outcome"), "outcome"),
        side=_enum(RawOrderSide, _value(row, "side"), "side"),
        size=_decimal(_value(row, "size"), "size"),
        limit_price=_decimal(
            _value(row, "limit_price", "limitPrice"), "limit_price"
        ),
        tif=_enum(TimeInForce, _value(row, "tif", default="FAK"), "tif"),
        signal_ts=signal_ts,
        observed_ts=observed_ts,
        submit_ts=submit_ts,
        amount_unit=_enum(
            OrderAmountUnit,
            _value(row, "amount_unit", "amountUnit", default="SHARES"),
            "amount_unit",
        ),
        signed_maker_amount=_optional_decimal(
            _value(row, "signed_maker_amount", "signedMakerAmount"),
            "signed_maker_amount",
        ),
        signed_taker_amount=_optional_decimal(
            _value(row, "signed_taker_amount", "signedTakerAmount"),
            "signed_taker_amount",
        ),
        venue_admission=_enum(
            VenueAdmissionStatus,
            _value(row, "venue_admission", "venueAdmission", default="UNKNOWN"),
            "venue_admission",
        ),
        venue_admission_evidence_id=str(
            _value(
                row,
                "venue_admission_evidence_id",
                "venueAdmissionEvidenceId",
                default="",
            )
        ),
        post_only=_boolean(
            _value(row, "post_only", "postOnly", default=False), "post_only"
        ),
        expires_at=(
            None
            if _value(row, "expires_at", "expiresAt") is None
            else _datetime(_value(row, "expires_at", "expiresAt"), "expires_at")
        ),
        entry_latency_ms=_optional_int(
            _value(row, "entry_latency_ms", "entryLatencyMs"),
            "entry_latency_ms",
        ),
        cancel_latency_ms=_optional_int(
            _value(row, "cancel_latency_ms", "cancelLatencyMs"),
            "cancel_latency_ms",
        ),
        response_latency_ms=_optional_int(
            _value(row, "response_latency_ms", "responseLatencyMs"),
            "response_latency_ms",
        ),
        venue_delay_ms=_optional_int(
            _value(row, "venue_delay_ms", "venueDelayMs"), "venue_delay_ms"
        ),
        fee_rate=_decimal(
            _value(row, "fee_rate", "feeRate", default="0"), "fee_rate"
        ),
        fee_exponent=_decimal(
            _value(row, "fee_exponent", "feeExponent", default="1"),
            "fee_exponent",
        ),
        match_type_hint=_enum(
            CtfMatchType,
            _value(
                row,
                "match_type_hint",
                "matchTypeHint",
                default="UNKNOWN_L2",
            ),
            "match_type_hint",
        ),
        metadata=metadata,
    )


def _order_group(row: Any, run_id: str) -> OrderGroupIntent:
    if not isinstance(row, Mapping):
        raise Pml2RequestError("each order group must be a JSON object")
    _reject_unknown(row, ORDER_GROUP_FIELDS, "order group")
    strategy_id = _required_text(row, "strategy_id", "strategyId")
    raw_legs = _value(row, "legs")
    raw_hedges = _value(row, "hedge_legs", "hedgeLegs", default=[])
    if not isinstance(raw_legs, list) or not raw_legs:
        raise Pml2RequestError("order group legs must be a non-empty list")
    if not isinstance(raw_hedges, list):
        raise Pml2RequestError("order group hedge_legs must be a list")
    metadata = _value(row, "metadata", default={})
    if not isinstance(metadata, Mapping):
        raise Pml2RequestError("order group metadata must be a JSON object")

    def parse_leg(value: Any) -> Pml2OrderIntent:
        if not isinstance(value, Mapping):
            raise Pml2RequestError("each order group leg must be a JSON object")
        payload = dict(value)
        payload.setdefault("strategy_id", strategy_id)
        return _order(payload, run_id)

    try:
        return OrderGroupIntent(
            run_id=run_id,
            group_id=_required_text(row, "group_id", "groupId"),
            strategy_id=strategy_id,
            policy=_enum(
                OrderGroupPolicy, _value(row, "policy"), "order group policy"
            ),
            legs=tuple(parse_leg(value) for value in raw_legs),
            hedge_legs=tuple(parse_leg(value) for value in raw_hedges),
            metadata=dict(metadata),
        )
    except ValueError as exc:
        raise Pml2RequestError(f"invalid order group: {exc}") from exc


def _levels(value: Any, field_name: str) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        raise Pml2RequestError(f"{field_name} must be a list")
    levels: list[BookLevel] = []
    for row in value:
        if not isinstance(row, Mapping):
            raise Pml2RequestError(f"each {field_name} level must be an object")
        unknown = set(row) - {"price", "size"}
        if unknown:
            raise Pml2RequestError(
                f"unknown {field_name} level fields: {sorted(unknown)}"
            )
        levels.append(
            BookLevel(
                _decimal(row.get("price"), "price"),
                _decimal(row.get("size"), "size"),
            )
        )
    return tuple(levels)


def _fee_schedule_registry(
    rows: list[Any],
) -> FeeScheduleRegistry | None:
    if not rows:
        return None
    schedules: list[FeeSchedule] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise Pml2RequestError("each fee schedule must be a JSON object")
        _reject_unknown(row, FEE_SCHEDULE_FIELDS, "fee schedule")
        effective_until_raw = _value(
            row, "effective_until", "effectiveUntil"
        )
        try:
            schedule = FeeSchedule(
                schedule_id=_required_text(row, "schedule_id", "scheduleId"),
                asset_id=_required_text(row, "asset_id", "assetId"),
                condition_id=_required_text(
                    row, "condition_id", "conditionId"
                ),
                effective_from=_datetime(
                    _value(row, "effective_from", "effectiveFrom"),
                    "effective_from",
                ),
                effective_until=(
                    None
                    if effective_until_raw is None
                    else _datetime(effective_until_raw, "effective_until")
                ),
                platform_fee_rate=_decimal(
                    _value(row, "platform_fee_rate", "platformFeeRate"),
                    "platform_fee_rate",
                ),
                platform_fee_exponent=_decimal(
                    _value(
                        row,
                        "platform_fee_exponent",
                        "platformFeeExponent",
                        default="1",
                    ),
                    "platform_fee_exponent",
                ),
                platform_taker_only=_boolean(
                    _value(
                        row,
                        "platform_taker_only",
                        "platformTakerOnly",
                        default=True,
                    ),
                    "platform_taker_only",
                ),
                builder_code=(
                    None
                    if _value(row, "builder_code", "builderCode") in (None, "")
                    else str(_value(row, "builder_code", "builderCode"))
                ),
                builder_taker_fee_bps=_integer(
                    _value(
                        row,
                        "builder_taker_fee_bps",
                        "builderTakerFeeBps",
                        default=0,
                    ),
                    "builder_taker_fee_bps",
                ),
                builder_maker_fee_bps=_integer(
                    _value(
                        row,
                        "builder_maker_fee_bps",
                        "builderMakerFeeBps",
                        default=0,
                    ),
                    "builder_maker_fee_bps",
                ),
                rounding_unit=_decimal(
                    _value(
                        row,
                        "rounding_unit",
                        "roundingUnit",
                        default="0.00001",
                    ),
                    "rounding_unit",
                ),
                economics_regime_id=(
                    None
                    if _value(
                        row, "economics_regime_id", "economicsRegimeId"
                    )
                    in (None, "")
                    else str(
                        _value(
                            row,
                            "economics_regime_id",
                            "economicsRegimeId",
                        )
                    )
                ),
                source=str(_value(row, "source", default="PML2_API")),
            )
        except ValueError as exc:
            raise Pml2RequestError(f"invalid fee schedule: {exc}") from exc
        schedules.append(schedule)
    try:
        return FeeScheduleRegistry(schedules)
    except ValueError as exc:
        raise Pml2RequestError(f"invalid fee schedule registry: {exc}") from exc


def _restore_archive_events(
    *,
    session: ReplayExecutionSession,
    rows: list[Any],
    orders: tuple[Pml2OrderIntent, ...],
    feed_latency_ms: int,
    identity_by_condition: Mapping[str, BinaryMarketIdentity],
) -> dict[str, Any]:
    if not rows:
        return {
            "cold_restore_requested": False,
            "archive_root": None,
            "restores": [],
            "coverage_scope": "NO_ARCHIVE_RESTORE_REQUESTED",
            "all_restores_clock_verified": False,
            "source_manifest_hash": None,
        }
    loader = Pml2ArchiveSnapshotLoader()
    restored_rows: list[dict[str, Any]] = []
    all_restores_clock_verified = True
    baseline_clock_count = 0
    raw_event_clock_count = 0
    frame_evidence_count = 0
    restore_manifest_hashes: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise Pml2RequestError("each archive restore must be a JSON object")
        _reject_unknown(row, ARCHIVE_RESTORE_FIELDS, "archive restore")
        start_time = _datetime(
            _value(row, "start_time", "startTime"), "archive start_time"
        )
        condition_id = _required_text(row, "condition_id", "conditionId")
        condition_orders = tuple(
            order
            for order in orders
            if order.condition_id.lower() == condition_id.lower()
        )
        if not condition_orders:
            raise Pml2RequestError(
                "archive restore condition has no matching request orders"
            )
        arrivals: set[datetime] = set()
        for order in condition_orders:
            entry_latency = (
                session.profile.entry_latency_ms
                if order.entry_latency_ms is None
                else max(0, order.entry_latency_ms)
            )
            arrival = order.submit_ts + timedelta(milliseconds=entry_latency)
            arrivals.add(arrival)
            venue_delay = (
                session.profile.venue_delay_ms
                if order.venue_delay_ms is None
                else max(0, order.venue_delay_ms)
            )
            if venue_delay > 0:
                arrivals.add(arrival + timedelta(milliseconds=venue_delay))
        earliest_arrival = min(arrivals)
        if start_time >= earliest_arrival:
            raise Pml2RequestError(
                "archive start_time must precede the earliest exchange arrival"
            )
        explicit_end = _value(row, "end_time", "endTime")
        raw_maker_horizon = _value(
            row,
            "maker_horizon_seconds",
            "makerHorizonSeconds",
            default=None,
        )
        maker_horizon_seconds = (
            900
            if raw_maker_horizon is None
            else _integer(
                raw_maker_horizon,
                "maker_horizon_seconds",
                minimum=1,
            )
        )
        if explicit_end is not None:
            end_time = _datetime(explicit_end, "archive end_time")
        else:
            end_candidates = set(arrivals)
            for order in condition_orders:
                if order.expires_at is not None:
                    end_candidates.add(order.expires_at)
                elif order.tif in {TimeInForce.GTC, TimeInForce.GTD}:
                    end_candidates.add(
                        max(arrivals)
                        + timedelta(seconds=maker_horizon_seconds)
                    )
            end_time = max(end_candidates)
        if end_time < earliest_arrival:
            raise Pml2RequestError(
                "archive end_time must include the earliest exchange arrival"
            )
        raw_max_events = _value(
            row,
            "max_events",
            "maxEvents",
            default=None,
        )
        max_events = (
            MAX_EVENTS
            if raw_max_events is None
            else _integer(raw_max_events, "max_events", minimum=1)
        )
        if max_events > MAX_EVENTS:
            raise Pml2LimitError(f"archive max_events exceeds {MAX_EVENTS}")
        hash_bound_source_files = _archive_source_files(row)
        result = loader.restore_condition_events(
            condition_id=condition_id,
            market_id=_required_text(row, "market_id", "marketId"),
            yes_asset_id=_required_text(
                row, "yes_asset_id", "yesAssetId"
            ),
            no_asset_id=_required_text(row, "no_asset_id", "noAssetId"),
            start_time=start_time,
            end_time=end_time,
            book_epoch=_integer(
                _value(row, "book_epoch", "bookEpoch", default=0),
                "book_epoch",
            ),
            feed_latency_ms=feed_latency_ms,
            max_events=max_events,
            source_files=hash_bound_source_files,
        )
        if not result.restored:
            raise Pml2DataNotReadyError(result.reason)
        for coverage_window in result.transport_coverage_windows:
            session.register_transport_coverage(coverage_window)
        restore_clock_verified = bool(
            result.clock_verified
            and result.baseline_clock_count == 2
            and result.raw_event_clock_count >= 0
            and result.frame_evidence_verified
            and result.source_manifest_hash
        )
        all_restores_clock_verified = (
            all_restores_clock_verified and restore_clock_verified
        )
        baseline_clock_count += result.baseline_clock_count
        raw_event_clock_count += result.raw_event_clock_count
        frame_evidence_count += int(result.frame_evidence_verified)
        restore_manifest_hashes.append(result.source_manifest_hash)
        snapshot_outcomes: set[str] = set()
        snapshot_exchange_ts: dict[str, list[str]] = {}
        snapshot_received_ts: dict[str, list[str]] = {}
        for event in result.events:
            if identity_by_condition:
                event_id = (
                    event.snapshot_id
                    if isinstance(event, BookSnapshotEvent)
                    else event.event_id
                )
                identity_events = (
                    event.batches
                    if isinstance(event, BookFrameBatchEvent)
                    else (event,)
                )
                for identity_event in identity_events:
                    _validate_identity_record(
                        identity_by_condition,
                        condition_id=identity_event.condition_id,
                        market_id=identity_event.market_id,
                        asset_id=identity_event.asset_id,
                        outcome=identity_event.outcome,
                        label=f"restored event {event_id}",
                    )
            if isinstance(event, BookSnapshotEvent):
                snapshot_outcomes.add(event.outcome.value)
                snapshot_exchange_ts.setdefault(event.outcome.value, []).append(
                    event.exchange_ts.isoformat()
                )
                snapshot_received_ts.setdefault(event.outcome.value, []).append(
                    (event.source_received_ts or event.exchange_ts).isoformat()
                )
                session.ingest_snapshot(event)
            elif isinstance(event, BookFrameBatchEvent):
                session.ingest_frame_batch(event)
            elif isinstance(event, BookLevelBatchEvent):
                session.ingest_level_batch(event)
            else:
                session.ingest_trade(event)
        restored_rows.append(
            {
                "condition_id": _required_text(
                    row, "condition_id", "conditionId"
                ),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "source": result.source,
                "source_files": list(result.source_files),
                "source_manifest_hash": result.source_manifest_hash,
                "row_count": result.row_count,
                "event_count": len(result.events),
                "snapshot_count": result.snapshot_count,
                "delta_count": result.delta_count,
                "trade_count": result.trade_count,
                "snapshot_outcomes": sorted(snapshot_outcomes),
                "snapshot_exchange_ts": {
                    outcome: sorted(timestamps)
                    for outcome, timestamps in sorted(
                        snapshot_exchange_ts.items()
                    )
                },
                "snapshot_received_ts": {
                    outcome: sorted(timestamps)
                    for outcome, timestamps in sorted(
                        snapshot_received_ts.items()
                    )
                },
                "authority_scope": (
                    "XUE_NATIVE_L2_BOUNDED_WINDOW_RESTORED_CLOCK_VERIFIED"
                    if restore_clock_verified
                    else "XUE_NATIVE_L2_BOUNDED_WINDOW_RESTORED_CLOCK_UNVERIFIED"
                ),
                "coverage_scope": "REQUESTED_INTERVAL_ONLY",
                "clock_verified": restore_clock_verified,
                "clock_evidence": result.clock_evidence,
                "receipt_validation": (
                    "VERIFIED"
                    if restore_clock_verified
                    else "REJECTED_OR_INCOMPLETE"
                ),
                "baseline_clock_count": result.baseline_clock_count,
                "raw_event_clock_count": result.raw_event_clock_count,
                "frame_evidence_verified": result.frame_evidence_verified,
                "source_binding": result.source_binding,
                "hash_bound_file_count": result.hash_bound_file_count,
                "source_file_sha256": [
                    {"path": path, "sha256": sha256}
                    for path, sha256 in result.source_file_sha256
                ],
                "transport_coverage": {
                    "status": (
                        "PROVEN"
                        if result.transport_coverage_windows
                        else "UNAVAILABLE_TTL_FALLBACK"
                    ),
                    "window_count": len(result.transport_coverage_windows),
                    "allowed_window_count": sum(
                        item.allowed for item in result.transport_coverage_windows
                    ),
                    "blocked_window_count": sum(
                        not item.allowed
                        for item in result.transport_coverage_windows
                    ),
                    "proof_ids": [
                        item.proof_id
                        for item in result.transport_coverage_windows
                    ],
                },
                "reason": result.reason,
            }
        )
    aggregate_manifest_hash = canonical_hash(
        {
            "restore_count": len(restored_rows),
            "source_manifest_hashes": sorted(restore_manifest_hashes),
        }
    )
    if all_restores_clock_verified:
        try:
            session.register_verified_cold_restore_clock_evidence(
                restore_count=len(restored_rows),
                baseline_clock_count=baseline_clock_count,
                raw_event_clock_count=raw_event_clock_count,
                frame_evidence_count=frame_evidence_count,
                source_manifest_hash=aggregate_manifest_hash,
            )
        except ValueError as exc:
            raise Pml2DataNotReadyError(
                f"cold restore clock evidence invalid: {exc}"
            ) from exc
    return {
        "cold_restore_requested": True,
        "archive_root": str(loader.archive_dir.resolve()),
        "restores": restored_rows,
        "coverage_scope": "REQUESTED_INTERVALS_ONLY",
        "all_restores_clock_verified": all_restores_clock_verified,
        "source_manifest_hash": aggregate_manifest_hash,
    }


def _reject_unknown(value: Mapping[str, Any], allowed: frozenset[str], label: str) -> None:
    unknown = set(value) - allowed
    if unknown:
        raise Pml2RequestError(f"unknown {label} fields: {sorted(unknown)}")


def _archive_source_files(
    value: Mapping[str, Any],
) -> tuple[dict[str, str], ...] | None:
    raw = _value(value, "source_files", "sourceFiles")
    if raw is None:
        return None
    if not isinstance(raw, list) or not raw:
        raise Pml2RequestError(
            "archive source_files must be a non-empty JSON array"
        )
    if len(raw) > MAX_ARCHIVE_SOURCE_FILES:
        raise Pml2LimitError(
            f"archive source_files exceeds {MAX_ARCHIVE_SOURCE_FILES}"
        )
    result: list[dict[str, str]] = []
    seen_paths: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            raise Pml2RequestError(
                "each archive source_files entry must be a JSON object"
            )
        _reject_unknown(
            item,
            ARCHIVE_SOURCE_FILE_FIELDS,
            "archive source_files entry",
        )
        path = _required_text(item, "path")
        sha256 = _required_text(item, "sha256").lower()
        if len(sha256) != 64 or any(
            char not in "0123456789abcdef" for char in sha256
        ):
            raise Pml2RequestError(
                "archive source_files sha256 must be 64 hexadecimal characters"
            )
        if path in seen_paths:
            raise Pml2RequestError(
                "archive source_files contains duplicate paths"
            )
        seen_paths.add(path)
        result.append({"path": path, "sha256": sha256})
    return tuple(result)


def _value(value: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in value:
            return value[key]
    return default


def _required_text(value: Mapping[str, Any], *keys: str) -> str:
    result = str(_value(value, *keys, default="")).strip()
    if not result:
        raise Pml2RequestError(f"{keys[0]} is required")
    return result


def _datetime(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError as exc:
            raise Pml2RequestError(f"{field_name} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Pml2RequestError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _decimal(value: Any, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise Pml2RequestError(f"{field_name} must be decimal") from exc
    if not parsed.is_finite():
        raise Pml2RequestError(f"{field_name} must be finite")
    return parsed


def _optional_decimal(value: Any, field_name: str) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value, field_name)


def _integer(value: Any, field_name: str, *, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise Pml2RequestError(f"{field_name} must be an integer") from exc
    if parsed < minimum:
        raise Pml2RequestError(f"{field_name} must be >= {minimum}")
    return parsed


def _optional_int(value: Any, field_name: str) -> int | None:
    if value in (None, ""):
        return None
    return _integer(value, field_name)


def _boolean(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if str(value).lower() in {"true", "1"}:
        return True
    if str(value).lower() in {"false", "0"}:
        return False
    raise Pml2RequestError(f"{field_name} must be boolean")


def _enum(enum_type: Any, value: Any, field_name: str) -> Any:
    try:
        return enum_type(str(value).upper())
    except ValueError as exc:
        choices = ", ".join(item.value for item in enum_type)
        raise Pml2RequestError(
            f"{field_name} must be one of: {choices}"
        ) from exc
