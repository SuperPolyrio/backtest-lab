"""Domain rules shared by the taker calibration control plane."""

from __future__ import annotations

from enum import Enum
import hashlib
import json
from typing import Any, Iterable, Mapping


class ModelState(str, Enum):
    DRAFT = "DRAFT"
    CALIBRATING = "CALIBRATING"
    CANDIDATE = "CANDIDATE"
    HOLDOUT_PASSED = "HOLDOUT_PASSED"
    SHADOW_PROMOTED = "SHADOW_PROMOTED"
    CALIBRATED_ACTIVE = "CALIBRATED_ACTIVE"
    CALIBRATED_STALE = "CALIBRATED_STALE"
    REJECTED = "REJECTED"
    DEPRECATED = "DEPRECATED"
    # Compatibility value retained for historical rows written before v3.
    VALIDATED = "VALIDATED"


class ProbeState(str, Enum):
    PLANNED = "PLANNED"
    PREFLIGHT_OK = "PREFLIGHT_OK"
    PREDICTION_FROZEN = "PREDICTION_FROZEN"
    SIGNED = "SIGNED"
    SUBMITTING = "SUBMITTING"
    HTTP_REJECTED = "HTTP_REJECTED"
    SUBMIT_OUTCOME_UNKNOWN = "SUBMIT_OUTCOME_UNKNOWN"
    ACKED = "ACKED"
    TRADE_ID_ASSIGNED = "TRADE_ID_ASSIGNED"
    MATCHED_NOT_BROADCASTED = "MATCHED_NOT_BROADCASTED"
    LIVE = "LIVE"
    DELAYED = "DELAYED"
    UNMATCHED = "UNMATCHED"
    MATCHED = "MATCHED"
    MINED = "MINED"
    RETRYING = "RETRYING"
    FAILED = "FAILED"
    CONFIRMED = "CONFIRMED"
    RECONCILED = "RECONCILED"
    CALIBRATABLE = "CALIBRATABLE"
    DATA_INCOMPLETE = "DATA_INCOMPLETE"
    RISK_ABORTED = "RISK_ABORTED"
    MARKET_DATA_UNSAFE = "MARKET_DATA_UNSAFE"
    ACCOUNTING_MISMATCH = "ACCOUNTING_MISMATCH"
    SELF_TRADE_CONTAMINATED = "SELF_TRADE_CONTAMINATED"
    MANUAL_INTERVENTION = "MANUAL_INTERVENTION"
    VENUE_MAINTENANCE = "VENUE_MAINTENANCE"


ARTIFACT_NAMES = (
    "INTENT_PRESENT",
    "PREDICTION_PRESENT",
    "DECISION_BOOK_PRESENT",
    "ARRIVAL_BOOK_PRESENT",
    "MODEL_MANIFEST_PRESENT",
    "RISK_CONFIG_PRESENT",
    "MARKET_METADATA_PRESENT",
    "SIGNED_ORDER_PRESENT",
    "ORDER_HASH_PRESENT",
    "HTTP_REQUEST_PRESENT",
    "HTTP_RESPONSE_PRESENT",
    "ORDER_ID_PRESENT",
    "USER_ORDER_EVENT_PRESENT",
    "USER_TRADE_EVENT_PRESENT",
    "REST_ORDER_RECONCILED",
    "REST_TRADE_RECONCILED",
    "FINAL_TRADE_STATUS_PRESENT",
    "SELF_TRADE_FILTERED",
    "ACCOUNTING_RECONCILED",
)

NO_SUBMIT_REQUIRED_ARTIFACTS = frozenset(ARTIFACT_NAMES[:9])
LIVE_REQUIRED_ARTIFACTS = frozenset(ARTIFACT_NAMES)
LIVE_REJECTION_REQUIRED_ARTIFACTS = frozenset(
    {
        *NO_SUBMIT_REQUIRED_ARTIFACTS,
        "HTTP_REQUEST_PRESENT",
        "HTTP_RESPONSE_PRESENT",
        "ACCOUNTING_RECONCILED",
    }
)

NON_CALIBRATABLE_STATES = frozenset(
    {
        ProbeState.DATA_INCOMPLETE,
        ProbeState.RISK_ABORTED,
        ProbeState.MARKET_DATA_UNSAFE,
        ProbeState.SUBMIT_OUTCOME_UNKNOWN,
        ProbeState.ACCOUNTING_MISMATCH,
        ProbeState.SELF_TRADE_CONTAMINATED,
        ProbeState.MANUAL_INTERVENTION,
        ProbeState.VENUE_MAINTENANCE,
    }
)

_ALLOWED_TRANSITIONS: dict[ProbeState, frozenset[ProbeState]] = {
    ProbeState.PLANNED: frozenset(
        {ProbeState.PREFLIGHT_OK, ProbeState.RISK_ABORTED, ProbeState.MARKET_DATA_UNSAFE}
    ),
    ProbeState.PREFLIGHT_OK: frozenset(
        {ProbeState.PREDICTION_FROZEN, ProbeState.DATA_INCOMPLETE, ProbeState.MARKET_DATA_UNSAFE}
    ),
    ProbeState.PREDICTION_FROZEN: frozenset(
        {ProbeState.SIGNED, ProbeState.DATA_INCOMPLETE, ProbeState.RISK_ABORTED}
    ),
    ProbeState.SIGNED: frozenset(
        {ProbeState.SUBMITTING, ProbeState.RECONCILED, ProbeState.MANUAL_INTERVENTION}
    ),
    ProbeState.SUBMITTING: frozenset(
        {ProbeState.HTTP_REJECTED, ProbeState.SUBMIT_OUTCOME_UNKNOWN, ProbeState.ACKED}
    ),
    ProbeState.ACKED: frozenset(
        {
            ProbeState.TRADE_ID_ASSIGNED,
            ProbeState.LIVE,
            ProbeState.DELAYED,
            ProbeState.UNMATCHED,
            ProbeState.MATCHED,
        }
    ),
    ProbeState.TRADE_ID_ASSIGNED: frozenset(
        {ProbeState.MATCHED_NOT_BROADCASTED, ProbeState.MATCHED, ProbeState.FAILED}
    ),
    ProbeState.MATCHED_NOT_BROADCASTED: frozenset(
        {ProbeState.MATCHED, ProbeState.RETRYING, ProbeState.FAILED}
    ),
    ProbeState.LIVE: frozenset(
        {ProbeState.MATCHED, ProbeState.RECONCILED, ProbeState.MANUAL_INTERVENTION}
    ),
    ProbeState.DELAYED: frozenset(
        {ProbeState.MATCHED, ProbeState.UNMATCHED, ProbeState.SUBMIT_OUTCOME_UNKNOWN}
    ),
    ProbeState.UNMATCHED: frozenset({ProbeState.RECONCILED}),
    ProbeState.MATCHED: frozenset(
        {ProbeState.MINED, ProbeState.RETRYING, ProbeState.FAILED}
    ),
    ProbeState.MINED: frozenset(
        {ProbeState.CONFIRMED, ProbeState.RETRYING, ProbeState.FAILED}
    ),
    ProbeState.RETRYING: frozenset(
        {ProbeState.MINED, ProbeState.CONFIRMED, ProbeState.FAILED}
    ),
    ProbeState.CONFIRMED: frozenset({ProbeState.RECONCILED}),
    ProbeState.FAILED: frozenset({ProbeState.RECONCILED}),
    ProbeState.RECONCILED: frozenset(
        {
            ProbeState.CALIBRATABLE,
            ProbeState.DATA_INCOMPLETE,
            ProbeState.ACCOUNTING_MISMATCH,
            ProbeState.SELF_TRADE_CONTAMINATED,
        }
    ),
}


def transition_probe(current: ProbeState | str, target: ProbeState | str) -> ProbeState:
    current_state = current if isinstance(current, ProbeState) else ProbeState(str(current))
    target_state = target if isinstance(target, ProbeState) else ProbeState(str(target))
    if current_state == target_state:
        return target_state
    if target_state in NON_CALIBRATABLE_STATES:
        return target_state
    if target_state not in _ALLOWED_TRANSITIONS.get(current_state, frozenset()):
        raise ValueError(f"invalid calibration probe transition: {current_state.value} -> {target_state.value}")
    return target_state


def artifact_bitmap(
    present: Iterable[str],
    *,
    live: bool,
    expected_rejection: bool = False,
) -> dict[str, Any]:
    present_set = {str(item) for item in present}
    unknown = sorted(present_set - set(ARTIFACT_NAMES))
    if unknown:
        raise ValueError(f"unknown calibration artifacts: {', '.join(unknown)}")
    required = (
        LIVE_REJECTION_REQUIRED_ARTIFACTS
        if live and expected_rejection
        else LIVE_REQUIRED_ARTIFACTS
        if live
        else NO_SUBMIT_REQUIRED_ARTIFACTS
    )
    mask = 0
    for index, name in enumerate(ARTIFACT_NAMES):
        if name in present_set:
            mask |= 1 << index
    missing = sorted(required - present_set)
    return {
        "schema_version": "calibration_artifact_bitmap_v1",
        "mask": str(mask),
        "present": sorted(present_set),
        "required": sorted(required),
        "missing": missing,
        "complete": not missing,
        "live": bool(live),
        "expected_rejection": bool(expected_rejection),
    }


def canonical_json(value: Any) -> str:
    return json.dumps(_json_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def payload_hash(value: Any, *, prefix: str = "") -> str:
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return f"{prefix}{digest[:24]}"


def redact_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    secret_fragments = ("private", "secret", "passphrase", "password", "api_key", "apikey")
    redacted: dict[str, Any] = {}
    for key, item in value.items():
        key_text = str(key)
        if any(fragment in key_text.lower() for fragment in secret_fragments):
            redacted[key_text] = "<redacted>" if item not in (None, "") else None
        elif isinstance(item, Mapping):
            redacted[key_text] = redact_mapping(item)
        elif isinstance(item, (list, tuple)):
            redacted[key_text] = [redact_mapping(entry) if isinstance(entry, Mapping) else entry for entry in item]
        else:
            redacted[key_text] = item
    return redacted


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "as_dict"):
        return _json_value(value.as_dict())
    if hasattr(value, "__dataclass_fields__"):
        return {name: _json_value(getattr(value, name)) for name in value.__dataclass_fields__}
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_value(item) for item in value]
    return str(value) if value.__class__.__module__ == "decimal" else value
