"""Auditable execution profile overrides derived from fill calibration."""

from __future__ import annotations

from decimal import Decimal
import json
from typing import Any, Iterable, Mapping

from quant.backtest.execution_profiles import normalize_execution_profile, normalize_order_role


OVERRIDE_TABLE = "quant.execution_profile_overrides"
APPROVED_STATUSES = {"approved", "active"}
WRITABLE_STATUSES = {"pending", "approved", "rejected", "archived"}
BUCKET_OVERRIDE_FIELDS = (
    "market_slug",
    "market_category",
    "liquidity_bucket",
    "volatility_bucket",
    "time_to_expiry_bucket",
    "role",
    "side",
)


def normalize_execution_profile_override(
    row: Mapping[str, Any],
    *,
    status: str = "pending",
    source: str | None = None,
    approved_by: str | None = None,
) -> dict[str, Any]:
    recommended = row.get("recommended") if isinstance(row.get("recommended"), Mapping) else row
    normalized_status = _normalize_status(row.get("status", status))
    approved = normalized_status in APPROVED_STATUSES
    override = {
        "status": normalized_status,
        "scope": _scope(row.get("scope")),
        "bucket_field": _blank_to_none(row.get("bucket_field", row.get("bucketField"))),
        "bucket_value": _blank_to_none(row.get("bucket", row.get("bucket_value", row.get("bucketValue")))),
        "execution_profile": normalize_execution_profile(
            recommended.get("execution_profile", recommended.get("executionProfile", row.get("execution_profile", row.get("executionProfile", "realistic"))))
        ),
        "order_role": _optional_role(recommended.get("order_role", recommended.get("orderRole", row.get("order_role", row.get("orderRole"))))),
        "latency_blocks": max(0, int(_decimal(recommended.get("latency_blocks_floor", recommended.get("latencyBlocksFloor", recommended.get("latency_blocks", 0)))))),
        "adverse_slippage_cents": _decimal(
            recommended.get(
                "adverse_slippage_price_floor",
                recommended.get("adverseSlippagePriceFloor", recommended.get("adverse_slippage_cents", recommended.get("adverseSlippageCents", 0))),
            )
        ),
        "fill_probability_haircut_pct": min(
            Decimal("100"),
            max(
                Decimal("0"),
                _decimal(
                    recommended.get(
                        "fill_probability_haircut_pct_floor",
                        recommended.get("fillProbabilityHaircutPctFloor", recommended.get("fill_probability_haircut_pct", 0)),
                    )
                ),
            ),
        ),
        "source": str(source or row.get("source") or "calibration-suggestion"),
        "calibration_sample_count": max(0, int(_decimal(row.get("sample_count", row.get("sampleCount", 0))))),
        "calibration_window_start": _blank_to_none(row.get("calibration_window_start", row.get("calibrationWindowStart"))),
        "calibration_window_end": _blank_to_none(row.get("calibration_window_end", row.get("calibrationWindowEnd"))),
        "reason": _blank_to_none(row.get("trust_reason", row.get("reason", row.get("trustReason")))),
        "evidence": _json_object(row.get("evidence") if isinstance(row.get("evidence"), Mapping) else dict(row)),
        "approved_by": approved_by or _blank_to_none(row.get("approved_by", row.get("approvedBy"))),
        "approved": approved,
    }
    if override["scope"] == "overall":
        override["bucket_field"] = None
        override["bucket_value"] = None
    return override


def upsert_execution_profile_overrides(
    conn: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    status: str = "pending",
    source: str | None = None,
    approved_by: str | None = None,
) -> int:
    normalized = [
        normalize_execution_profile_override(row, status=status, source=source, approved_by=approved_by)
        for row in rows
    ]
    if not normalized:
        return 0
    if not _table_exists(conn):
        raise RuntimeError(f"{OVERRIDE_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        for row in normalized:
            cur.execute(
                """
                INSERT INTO quant.execution_profile_overrides (
                    status, scope, bucket_field, bucket_value, execution_profile, order_role,
                    latency_blocks, adverse_slippage_cents, fill_probability_haircut_pct,
                    source, calibration_sample_count, calibration_window_start, calibration_window_end,
                    reason, evidence, approved_by, approved_at
                )
                VALUES (
                    %(status)s, %(scope)s, %(bucket_field)s, %(bucket_value)s, %(execution_profile)s, %(order_role)s,
                    %(latency_blocks)s, %(adverse_slippage_cents)s, %(fill_probability_haircut_pct)s,
                    %(source)s, %(calibration_sample_count)s, %(calibration_window_start)s, %(calibration_window_end)s,
                    %(reason)s, %(evidence)s::jsonb, %(approved_by)s,
                    CASE WHEN %(approved)s THEN now() ELSE NULL END
                )
                """,
                {**row, "evidence": json.dumps(row["evidence"], ensure_ascii=False, default=str)},
            )
    return len(normalized)


def load_execution_profile_override(
    conn: Any,
    *,
    override_id: int | None = None,
    scope: str = "overall",
    bucket_field: str | None = None,
    bucket_value: str | None = None,
    status: str = "approved",
) -> dict[str, Any] | None:
    if not _table_exists(conn):
        return None
    filters = ["status = %s"]
    params: list[Any] = [status]
    if override_id is not None:
        filters.append("override_id = %s")
        params.append(int(override_id))
    else:
        normalized_scope = _scope(scope)
        filters.append("scope = %s")
        params.append(normalized_scope)
        if normalized_scope != "overall":
            filters.append("bucket_field = %s")
            filters.append("bucket_value = %s")
            params.extend([bucket_field, bucket_value])
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.execution_profile_overrides
            WHERE {' AND '.join(filters)}
            ORDER BY updated_at DESC, override_id DESC
            LIMIT 1
            """,
            params,
        )
        row = cur.fetchone()
    return dict(row) if row else None


def apply_execution_profile_override_to_payload(payload: Mapping[str, Any], override: Mapping[str, Any] | None) -> dict[str, Any]:
    result = dict(payload)
    if not override:
        return result
    result["execution_profile"] = override.get("execution_profile") or result.get("execution_profile") or result.get("executionProfile") or "realistic"
    if override.get("order_role"):
        result["order_role"] = override["order_role"]
    result["latency_blocks"] = max(
        _int_payload(result.get("latency_blocks", result.get("latencyBlocks")), 0),
        _int_payload(override.get("latency_blocks"), 0),
    )
    result["adverse_slippage_cents"] = max(
        _decimal(result.get("adverse_slippage_cents", result.get("adverseSlippageCents"))),
        _decimal(override.get("adverse_slippage_cents")),
    )
    result["fill_probability_haircut_pct"] = max(
        _decimal(result.get("fill_probability_haircut_pct", result.get("fillProbabilityHaircutPct"))),
        _decimal(override.get("fill_probability_haircut_pct")),
    )
    context = result.get("execution_context", result.get("executionContext"))
    if not isinstance(context, dict):
        context = {}
    context = dict(context)
    context["execution_profile_override"] = {
        "override_id": override.get("override_id"),
        "source": override.get("source"),
        "scope": override.get("scope"),
        "bucket_field": override.get("bucket_field"),
        "bucket_value": override.get("bucket_value"),
        "status": override.get("status"),
        "calibration_sample_count": override.get("calibration_sample_count"),
    }
    result["execution_context"] = context
    return result


def maybe_apply_approved_execution_profile_override(conn: Any, payload: Mapping[str, Any]) -> dict[str, Any]:
    use_calibrated = _bool_payload(payload.get("use_calibrated_execution_profile", payload.get("useCalibratedExecutionProfile")))
    override_id = payload.get("execution_profile_override_id", payload.get("executionProfileOverrideId"))
    if override_id in (None, "") and not use_calibrated:
        return dict(payload)
    override = select_approved_execution_profile_override(conn, payload)
    return apply_execution_profile_override_to_payload(payload, override)


def select_approved_execution_profile_override(conn: Any, payload: Mapping[str, Any]) -> dict[str, Any] | None:
    """Select the approved override that should govern this payload.

    Explicit override ids win. Otherwise, calibrated mode tries bucket-specific
    overrides from payload/context fields before falling back to an overall
    approved override.
    """

    override_id = payload.get("execution_profile_override_id", payload.get("executionProfileOverrideId"))
    if override_id not in (None, ""):
        return load_execution_profile_override(conn, override_id=int(override_id), status="approved")
    if not _bool_payload(payload.get("use_calibrated_execution_profile", payload.get("useCalibratedExecutionProfile"))):
        return None
    for bucket_field, bucket_value in _bucket_override_candidates(payload):
        override = load_execution_profile_override(
            conn,
            scope="bucket",
            bucket_field=bucket_field,
            bucket_value=bucket_value,
            status="approved",
        )
        if override:
            return override
    override = load_execution_profile_override(
        conn,
        scope="overall",
        status="approved",
    )
    return override


def _bucket_override_candidates(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    for field in BUCKET_OVERRIDE_FIELDS:
        value = _contextual_payload_value(payload, field)
        if value in (None, "") and field == "role":
            value = _contextual_payload_value(payload, "order_role")
        if value in (None, ""):
            continue
        candidates.append((field, str(value)))
    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in candidates:
        key = (item[0], item[1])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _contextual_payload_value(payload: Mapping[str, Any], field: str) -> Any:
    aliases = _field_aliases(field)
    for row in _payload_contexts(payload):
        for key in aliases:
            value = row.get(key)
            if value not in (None, ""):
                return value
    return None


def _payload_contexts(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    contexts: list[Mapping[str, Any]] = [payload]
    for key in ("execution_context", "executionContext", "calibration_context", "calibrationContext", "context"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            contexts.append(value)
            nested = value.get("context")
            if isinstance(nested, Mapping):
                contexts.append(nested)
            calibration = value.get("calibration_context") or value.get("calibrationContext")
            if isinstance(calibration, Mapping):
                contexts.append(calibration)
    return contexts


def _field_aliases(field: str) -> tuple[str, ...]:
    camel = "".join([part if idx == 0 else part.capitalize() for idx, part in enumerate(field.split("_"))])
    aliases = [field, camel]
    if field == "market_category":
        aliases.extend(["category", "event_category", "eventCategory"])
    if field == "role":
        aliases.extend(["order_role", "orderRole"])
    return tuple(aliases)


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quant.execution_profile_overrides') IS NOT NULL AS exists")
        row = cur.fetchone()
    if isinstance(row, dict):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


def _normalize_status(value: Any) -> str:
    status = str(value or "pending").strip().lower()
    return status if status in WRITABLE_STATUSES else "pending"


def _scope(value: Any) -> str:
    scope = str(value or "overall").strip().lower()
    return scope if scope in {"overall", "bucket"} else "overall"


def _optional_role(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return normalize_order_role(value)


def _blank_to_none(value: Any) -> Any:
    return None if value in (None, "") else value


def _json_object(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _int_payload(value: Any, default: int) -> int:
    try:
        return max(0, int(str(value)))
    except Exception:
        return default


def _bool_payload(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}
