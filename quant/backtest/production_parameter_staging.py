"""Auditable staging for production strategy parameters."""

from __future__ import annotations

from decimal import Decimal
import json
from typing import Any, Iterable, Mapping


STAGING_TABLE = "quant.production_parameter_staging"
APPROVED_STATUSES = {"approved", "active"}
WRITABLE_STATUSES = {"pending", "approved", "rejected", "archived"}
REVIEWED_STATUSES = {"approved", "rejected", "archived"}


def normalize_production_parameter_staging(
    report: Mapping[str, Any],
    *,
    status: str = "pending",
    source: str | None = None,
    approved_by: str | None = None,
    force_review: bool = False,
    benchmark_id: int | None = None,
    run_id: int | None = None,
    strategy_name: str = "unknown",
    strategy_version: str = "unknown",
    universe_name: str = "",
) -> dict[str, Any]:
    """Convert a parameter_search_results report into a staging row."""

    normalized_status = _normalize_status(status)
    if normalized_status in APPROVED_STATUSES and not approved_by:
        raise ValueError("approved_by is required when staging parameters as approved")
    staging = _mapping(report.get("production_parameter_staging"))
    if not staging:
        raise ValueError("parameter_search_results report is missing production_parameter_staging")
    staging_allowed = bool(staging.get("staging_allowed"))
    if not staging_allowed and not force_review:
        raise ValueError("parameter report is not staging_allowed; use force_review only for explicit review records")
    parameters = _mapping(staging.get("best_parameters"))
    fingerprint = _text(staging.get("best_parameter_fingerprint"))
    if staging_allowed and (not parameters or not fingerprint):
        raise ValueError("staging_allowed report must include best_parameters and best_parameter_fingerprint")
    regime_coverage = _mapping(report.get("regime_coverage_report")) or _mapping(staging.get("regime_coverage_report"))
    regime_coverage_verdict = (
        _text(staging.get("regime_coverage_verdict"))
        or _text(regime_coverage.get("coverage_verdict"))
        or "missing"
    )
    score_validation = _mapping(report.get("performance_score_validation_report")) or _mapping(staging.get("performance_score_validation_report"))
    score_bias_verdict = (
        _text(staging.get("score_bias_verdict"))
        or _text(score_validation.get("score_bias_verdict"))
        or "missing"
    )
    return {
        "status": normalized_status,
        "source": str(source or "parameter-search-results"),
        "benchmark_id": int(benchmark_id) if benchmark_id is not None else None,
        "run_id": int(run_id) if run_id is not None else None,
        "strategy_name": str(strategy_name or "unknown"),
        "strategy_version": str(strategy_version or "unknown"),
        "universe_name": str(universe_name or report.get("universe_name") or ""),
        "parameter_fingerprint": fingerprint,
        "parameters": parameters,
        "staging_allowed": staging_allowed,
        "coverage_pct": _decimal(report.get("coverage_pct")),
        "robustness_verdict": _text(_mapping(report.get("robustness_report")).get("robustness_verdict")) or "missing",
        "regime_coverage_verdict": regime_coverage_verdict,
        "score_bias_verdict": score_bias_verdict,
        "strategy_scope": _text(staging.get("strategy_scope")) or _text(regime_coverage.get("strategy_scope")) or "unknown",
        "regime_specific": bool(staging.get("regime_specific") or regime_coverage.get("regime_specific")),
        "default_action": _text(staging.get("default_action")) or "do_not_stage",
        "blocked_reasons": list(staging.get("blocked_reasons") or []),
        "reason": _text(report.get("reason")) or _text(_mapping(staging.get("robustness_policy")).get("reason")),
        "evidence": {
            "schema_version": report.get("schema_version"),
            "planned_run_count": report.get("planned_run_count"),
            "result_row_count": report.get("result_row_count"),
            "covered_run_count": report.get("covered_run_count"),
            "missing_run_count": report.get("missing_run_count"),
            "duplicate_result_count": report.get("duplicate_result_count"),
            "unplanned_result_count": report.get("unplanned_result_count"),
            "required_field_report": report.get("required_field_report"),
            "regime_coverage_report": regime_coverage,
            "performance_score_validation_report": score_validation,
            "production_parameter_staging": staging,
        },
        "parameter_search_results": dict(report),
        "approved_by": approved_by,
        "approved": normalized_status in APPROVED_STATUSES,
    }


def upsert_production_parameter_staging(
    conn: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    status: str = "pending",
    source: str | None = None,
    approved_by: str | None = None,
    force_review: bool = False,
) -> int:
    normalized = [
        normalize_production_parameter_staging(
            row,
            status=status,
            source=source,
            approved_by=approved_by,
            force_review=force_review,
            benchmark_id=_optional_int(row.get("benchmark_id", row.get("benchmarkId"))),
            run_id=_optional_int(row.get("run_id", row.get("runId"))),
            strategy_name=str(row.get("strategy_name", row.get("strategyName", "unknown"))),
            strategy_version=str(row.get("strategy_version", row.get("strategyVersion", "unknown"))),
            universe_name=str(row.get("universe_name", row.get("universeName", ""))),
        )
        for row in rows
    ]
    if not normalized:
        return 0
    if not _table_exists(conn):
        raise RuntimeError(f"{STAGING_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        for row in normalized:
            cur.execute(
                """
                INSERT INTO quant.production_parameter_staging (
                    status, source, benchmark_id, run_id, strategy_name, strategy_version, universe_name,
                    parameter_fingerprint, parameters, staging_allowed, coverage_pct, robustness_verdict,
                    default_action, blocked_reasons, reason, evidence, parameter_search_results,
                    approved_by, approved_at
                )
                VALUES (
                    %(status)s, %(source)s, %(benchmark_id)s, %(run_id)s, %(strategy_name)s, %(strategy_version)s, %(universe_name)s,
                    %(parameter_fingerprint)s, %(parameters)s::jsonb, %(staging_allowed)s, %(coverage_pct)s, %(robustness_verdict)s,
                    %(default_action)s, %(blocked_reasons)s::jsonb, %(reason)s, %(evidence)s::jsonb, %(parameter_search_results)s::jsonb,
                    %(approved_by)s, CASE WHEN %(approved)s THEN now() ELSE NULL END
                )
                """,
                {
                    **row,
                    "parameters": json.dumps(row["parameters"], ensure_ascii=False, default=str),
                    "blocked_reasons": json.dumps(row["blocked_reasons"], ensure_ascii=False, default=str),
                    "evidence": json.dumps(row["evidence"], ensure_ascii=False, default=str),
                    "parameter_search_results": json.dumps(row["parameter_search_results"], ensure_ascii=False, default=str),
                },
            )
    return len(normalized)


def load_production_parameter_staging(
    conn: Any,
    *,
    status: str | None = "approved",
    limit: int = 50,
) -> list[dict[str, Any]]:
    if not _table_exists(conn):
        return []
    filters: list[str] = []
    params: list[Any] = []
    if status:
        filters.append("status = %s")
        params.append(_normalize_status(status))
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(max(1, int(limit)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.production_parameter_staging
            {where_sql}
            ORDER BY updated_at DESC, staging_id DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def validate_production_parameter_staging_status_update(
    row: Mapping[str, Any],
    *,
    status: str,
    reviewed_by: str | None = None,
    review_note: str | None = None,
) -> dict[str, Any]:
    """Validate an auditable status transition for a staged parameter row."""

    normalized_status = _normalize_status(status)
    reviewer = _text(reviewed_by).strip()
    if normalized_status in REVIEWED_STATUSES and not reviewer:
        raise ValueError("reviewed_by is required when approving, rejecting, or archiving staged parameters")
    staging_allowed = bool(row.get("staging_allowed"))
    robustness = _text(row.get("robustness_verdict")) or "missing"
    regime_coverage = _regime_coverage_verdict(row)
    score_bias = _score_bias_verdict(row)
    if normalized_status == "approved":
        if not staging_allowed:
            raise ValueError("cannot approve a parameter row unless staging_allowed is true")
        if robustness != "ready":
            raise ValueError("cannot approve a parameter row unless robustness_verdict is ready")
        if regime_coverage != "ready":
            raise ValueError("cannot approve a parameter row unless regime_coverage_verdict is ready")
        if score_bias != "ready":
            raise ValueError("cannot approve a parameter row unless score_bias_verdict is ready")
        if not _text(row.get("parameter_fingerprint")):
            raise ValueError("cannot approve a parameter row without parameter_fingerprint")
        if not _mapping(row.get("parameters")):
            raise ValueError("cannot approve a parameter row without parameters")
    return {
        "status": normalized_status,
        "reviewed_by": reviewer or None,
        "review_note": _text(review_note) or None,
        "approved_by": reviewer if normalized_status == "approved" else None,
        "approved": normalized_status == "approved",
    }


def update_production_parameter_staging_status(
    conn: Any,
    *,
    staging_id: int,
    status: str,
    reviewed_by: str | None = None,
    review_note: str | None = None,
) -> dict[str, Any]:
    """Update a staged parameter row status after human review."""

    if not _table_exists(conn):
        raise RuntimeError(f"{STAGING_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.production_parameter_staging
            WHERE staging_id = %s
            FOR UPDATE
            """,
            (int(staging_id),),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"production parameter staging row not found: {staging_id}")
        current = dict(row)
        update = validate_production_parameter_staging_status_update(
            current,
            status=status,
            reviewed_by=reviewed_by,
            review_note=review_note,
        )
        cur.execute(
            """
            UPDATE quant.production_parameter_staging
            SET status = %(status)s,
                reviewed_by = %(reviewed_by)s,
                reviewed_at = CASE WHEN %(reviewed_by)s IS NULL THEN reviewed_at ELSE now() END,
                review_note = %(review_note)s,
                approved_by = %(approved_by)s,
                approved_at = CASE WHEN %(approved)s THEN now() ELSE NULL END,
                updated_at = now()
            WHERE staging_id = %(staging_id)s
            RETURNING *
            """,
            {**update, "staging_id": int(staging_id)},
        )
        updated = cur.fetchone()
    return dict(updated)


def extract_parameter_search_results_reports(payload: Any) -> list[dict[str, Any]]:
    """Load reports from direct report JSON, artifact payloads, or wrapper objects."""

    if isinstance(payload, list):
        reports: list[dict[str, Any]] = []
        for item in payload:
            reports.extend(extract_parameter_search_results_reports(item))
        return reports
    if not isinstance(payload, Mapping):
        return []
    if payload.get("schema_version") == "fill_first_parameter_search_results_v1":
        return [dict(payload)]
    for key in ("parameter_search_results", "parameterSearchResults", "report"):
        nested = payload.get(key)
        reports = extract_parameter_search_results_reports(nested)
        if reports:
            return reports
    artifacts = payload.get("artifacts")
    if isinstance(artifacts, list):
        reports = []
        for artifact in artifacts:
            if not isinstance(artifact, Mapping):
                continue
            artifact_key = artifact.get("artifact_key", artifact.get("artifactKey"))
            if artifact_key == "parameter_search_results":
                reports.extend(extract_parameter_search_results_reports(artifact.get("payload")))
        return reports
    return []


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quant.production_parameter_staging') IS NOT NULL AS exists")
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


def _normalize_status(value: Any) -> str:
    status = str(value or "pending").strip().lower()
    return status if status in WRITABLE_STATUSES else "pending"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return "" if value in (None, "") else str(value)


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _regime_coverage_verdict(row: Mapping[str, Any]) -> str:
    direct = _text(row.get("regime_coverage_verdict"))
    if direct:
        return direct
    evidence = _mapping(row.get("evidence"))
    report = _mapping(row.get("parameter_search_results"))
    staging = _mapping(report.get("production_parameter_staging")) or _mapping(evidence.get("production_parameter_staging"))
    regime = (
        _mapping(report.get("regime_coverage_report"))
        or _mapping(evidence.get("regime_coverage_report"))
        or _mapping(staging.get("regime_coverage_report"))
    )
    return _text(staging.get("regime_coverage_verdict")) or _text(regime.get("coverage_verdict")) or "missing"


def _score_bias_verdict(row: Mapping[str, Any]) -> str:
    direct = _text(row.get("score_bias_verdict"))
    if direct:
        return direct
    evidence = _mapping(row.get("evidence"))
    report = _mapping(row.get("parameter_search_results"))
    staging = _mapping(report.get("production_parameter_staging")) or _mapping(evidence.get("production_parameter_staging"))
    validation = (
        _mapping(report.get("performance_score_validation_report"))
        or _mapping(evidence.get("performance_score_validation_report"))
        or _mapping(staging.get("performance_score_validation_report"))
    )
    return _text(staging.get("score_bias_verdict")) or _text(validation.get("score_bias_verdict")) or "missing"
