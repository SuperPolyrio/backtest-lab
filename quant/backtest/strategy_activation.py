"""Strategy activation decisions guarded by fill-first promotion reports."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"
BLOCKED = "blocked"

TARGET_MODES = ("backtest", "paper", "live")
ACTIVATION_TABLE = "quant.strategy_activation_decisions"
ENABLE_STATE_TABLE = "quant.strategy_enable_state"


def build_strategy_activation_decision(
    artifact_report: Mapping[str, Any] | None,
    *,
    target_mode: str,
    requested_by: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    """Build an auditable decision for advancing a run to backtest/paper/live.

    ``artifact_report`` is the output of ``build_backtest_run_artifact_report``.
    This function deliberately does not execute orders; it only decides whether a
    persisted run is allowed to be used by the next execution mode.
    """

    normalized_mode = _target_mode(target_mode)
    report = dict(artifact_report or {})
    gate = _mapping(report.get("promotion_gate_report"))
    paper_live_gate = _mapping(report.get("paper_live_evidence_gate_report"))
    artifacts = _mapping(report.get("artifacts"))
    reproducibility = _mapping(report.get("reproducibility_report"))
    run_id = _int_or_none(report.get("run_id"))
    promotion_verdict = str(gate.get("promotion_verdict") or MISSING)
    paper_live_gate_status = str(paper_live_gate.get("status") or MISSING)
    paper_live_paper_allowed = bool(paper_live_gate.get("paper_allowed"))
    paper_live_live_allowed = bool(paper_live_gate.get("live_allowed"))
    allowed_modes = _string_list(gate.get("allowed_next_modes"))
    blocked_reasons = _string_list(gate.get("blocked_reasons"))
    review_reasons = _string_list(gate.get("review_reasons"))
    missing_reasons = _string_list(gate.get("missing_reasons"))
    blocked_reasons.extend(_string_list(paper_live_gate.get("blocked_reasons")))
    review_reasons.extend(_string_list(paper_live_gate.get("review_reasons")))

    reasons: list[str] = []
    if not run_id:
        reasons.append("missing run_id")
    if not gate:
        reasons.append("missing promotion_gate_report")
    if normalized_mode != "backtest" and not paper_live_gate:
        missing_reasons.append("missing paper_live_evidence_gate_report")

    if normalized_mode == "backtest":
        mode_allowed = bool(run_id and report and not reasons)
        if not mode_allowed and "backtest" not in allowed_modes:
            reasons.append("backtest mode requires an audited persisted run")
    elif normalized_mode == "paper":
        mode_allowed = bool(gate.get("paper_promotion_allowed") and paper_live_paper_allowed)
        if gate.get("paper_promotion_allowed") and not paper_live_paper_allowed:
            reasons.append("paper is not allowed by paper/live evidence gate")
        if not mode_allowed:
            reasons.append("paper promotion is not allowed by fill-first gate")
    else:
        mode_allowed = bool(gate.get("production_promotion_allowed") and paper_live_live_allowed)
        if gate.get("production_promotion_allowed") and not paper_live_live_allowed:
            reasons.append("live is not allowed by paper/live evidence gate")
        if not mode_allowed:
            reasons.append("live promotion is not allowed by fill-first gate")

    if normalized_mode not in allowed_modes and gate:
        reasons.append(f"{normalized_mode} not in allowed_next_modes")

    activation_allowed = bool(mode_allowed and not (normalized_mode != "backtest" and missing_reasons))
    decision_verdict = _decision_verdict(
        activation_allowed=activation_allowed,
        target_mode=normalized_mode,
        promotion_verdict=promotion_verdict,
        blocked_reasons=blocked_reasons,
        missing_reasons=missing_reasons,
        review_reasons=review_reasons,
        reasons=reasons,
    )

    strategy_name = str(
        artifacts.get("strategy_name")
        or reproducibility.get("strategy_name")
        or report.get("strategy_name")
        or "unknown"
    )
    strategy_version = str(
        artifacts.get("strategy_version")
        or reproducibility.get("strategy_version")
        or report.get("strategy_version")
        or "unknown"
    )
    created_at = datetime.now(timezone.utc).isoformat()
    return {
        "status": READY,
        "decision_verdict": decision_verdict,
        "activation_allowed": activation_allowed,
        "target_mode": normalized_mode,
        "run_id": run_id,
        "strategy_name": strategy_name,
        "strategy_version": strategy_version,
        "market_slug": report.get("market_slug"),
        "token_side": report.get("token_side"),
        "price_source": report.get("price_source"),
        "actual_execution_engine": report.get("backtest_engine") or artifacts.get("backtest_engine") or "unknown",
        "promotion_verdict": promotion_verdict,
        "production_promotion_allowed": bool(gate.get("production_promotion_allowed")),
        "paper_promotion_allowed": bool(gate.get("paper_promotion_allowed")),
        "paper_live_evidence_gate_status": paper_live_gate_status,
        "paper_live_paper_allowed": paper_live_paper_allowed,
        "paper_live_live_allowed": paper_live_live_allowed,
        "allowed_next_modes": allowed_modes,
        "blocked_reasons": blocked_reasons,
        "review_reasons": review_reasons,
        "missing_reasons": missing_reasons,
        "decision_reasons": reasons,
        "requested_by": requested_by or "",
        "notes": notes or "",
        "created_at": created_at,
        "promotion_gate_report": gate,
        "paper_live_evidence_gate_report": paper_live_gate,
        "artifact_status": report.get("status"),
        "artifact_summary": {
            "run_credibility_status": artifacts.get("run_credibility_status"),
            "data_quality_verdict": artifacts.get("data_quality_verdict"),
            "reproducibility_verdict": artifacts.get("reproducibility_verdict"),
            "materialized_cache_verdict": artifacts.get("materialized_cache_verdict"),
            "shadow_live_triangulation_verdict": artifacts.get("shadow_live_triangulation_verdict"),
            "fill_model_suspect": artifacts.get("fill_model_suspect"),
            "settlement_compatibility_verdict": artifacts.get("settlement_compatibility_verdict"),
            "paper_live_evidence_gate_status": paper_live_gate_status,
            "paper_live_paper_allowed": paper_live_paper_allowed,
            "paper_live_live_allowed": paper_live_live_allowed,
            "paper_live_evidence_gate_report": paper_live_gate,
        },
    }


def insert_strategy_activation_decision(conn: Any, decision: Mapping[str, Any]) -> dict[str, Any]:
    """Persist an activation decision and return the stored row."""

    if not _table_exists(conn):
        raise RuntimeError(f"{ACTIVATION_TABLE} does not exist; run quant.core.schema.create_schema first")
    row = dict(decision)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.strategy_activation_decisions (
                run_id, target_mode, activation_allowed, decision_verdict,
                strategy_name, strategy_version, market_slug, token_side, price_source,
                actual_execution_engine, promotion_verdict, paper_promotion_allowed,
                production_promotion_allowed, paper_live_evidence_gate_status,
                paper_live_paper_allowed, paper_live_live_allowed, allowed_next_modes, blocked_reasons,
                review_reasons, missing_reasons, decision_reasons, requested_by,
                notes, promotion_gate_report, paper_live_evidence_gate_report, artifact_summary
            )
            VALUES (
                %(run_id)s, %(target_mode)s, %(activation_allowed)s, %(decision_verdict)s,
                %(strategy_name)s, %(strategy_version)s, %(market_slug)s, %(token_side)s, %(price_source)s,
                %(actual_execution_engine)s, %(promotion_verdict)s, %(paper_promotion_allowed)s,
                %(production_promotion_allowed)s, %(paper_live_evidence_gate_status)s,
                %(paper_live_paper_allowed)s, %(paper_live_live_allowed)s, %(allowed_next_modes)s::jsonb, %(blocked_reasons)s::jsonb,
                %(review_reasons)s::jsonb, %(missing_reasons)s::jsonb, %(decision_reasons)s::jsonb, %(requested_by)s,
                %(notes)s, %(promotion_gate_report)s::jsonb, %(paper_live_evidence_gate_report)s::jsonb, %(artifact_summary)s::jsonb
            )
            RETURNING *
            """,
            {
                **row,
                "allowed_next_modes": json.dumps(row.get("allowed_next_modes") or [], ensure_ascii=True, default=str),
                "blocked_reasons": json.dumps(row.get("blocked_reasons") or [], ensure_ascii=True, default=str),
                "review_reasons": json.dumps(row.get("review_reasons") or [], ensure_ascii=True, default=str),
                "missing_reasons": json.dumps(row.get("missing_reasons") or [], ensure_ascii=True, default=str),
                "decision_reasons": json.dumps(row.get("decision_reasons") or [], ensure_ascii=True, default=str),
                "promotion_gate_report": json.dumps(row.get("promotion_gate_report") or {}, ensure_ascii=True, default=str),
                "paper_live_evidence_gate_report": json.dumps(row.get("paper_live_evidence_gate_report") or {}, ensure_ascii=True, default=str),
                "artifact_summary": json.dumps(row.get("artifact_summary") or {}, ensure_ascii=True, default=str),
            },
        )
        stored = cur.fetchone()
    return dict(stored) if stored else row


def load_strategy_activation_decisions(
    conn: Any,
    *,
    run_id: int | None = None,
    target_mode: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Load recent activation decisions for audit/UI display."""

    if not _table_exists(conn):
        return []
    filters: list[str] = []
    params: list[Any] = []
    if run_id is not None:
        filters.append("run_id = %s")
        params.append(int(run_id))
    if target_mode:
        filters.append("target_mode = %s")
        params.append(_target_mode(target_mode))
    where = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(max(1, min(int(limit), 500)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.strategy_activation_decisions
            {where}
            ORDER BY created_at DESC, decision_id DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def build_strategy_enable_state(
    decision: Mapping[str, Any],
    *,
    enable: bool,
    requested_by: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Build the DB-managed strategy enable state from an activation decision.

    Enabling requires an already allowed activation decision. Disabling is always
    permitted so a strategy can be shut off even when the latest gate is blocked.
    """

    row = dict(decision or {})
    decision_id = _int_or_none(row.get("decision_id"))
    target_mode = _target_mode(str(row.get("target_mode") or "paper"))
    activation_allowed = bool(row.get("activation_allowed"))
    decision_verdict = str(row.get("decision_verdict") or MISSING)
    requested_enable = bool(enable)
    enabled = bool(requested_enable and activation_allowed and decision_verdict == READY)
    blocked_reasons = _string_list(row.get("blocked_reasons"))
    missing_reasons = _string_list(row.get("missing_reasons"))
    review_reasons = _string_list(row.get("review_reasons"))
    enable_reasons = _string_list(row.get("decision_reasons"))
    paper_live_gate = _paper_live_gate_from_decision(row)
    paper_live_gate_status = str(row.get("paper_live_evidence_gate_status") or paper_live_gate.get("status") or MISSING)
    paper_live_paper_allowed = bool(row.get("paper_live_paper_allowed") or paper_live_gate.get("paper_allowed"))
    paper_live_live_allowed = bool(row.get("paper_live_live_allowed") or paper_live_gate.get("live_allowed"))
    paper_live_mode_allowed = paper_live_live_allowed if target_mode == "live" else paper_live_paper_allowed
    if requested_enable and not decision_id:
        enable_reasons.append("missing persisted activation decision_id")
    if requested_enable and not activation_allowed:
        enable_reasons.append("activation_allowed=false")
    if requested_enable and decision_verdict != READY:
        enable_reasons.append(f"decision_verdict={decision_verdict}")
    if requested_enable and target_mode != "backtest":
        if not paper_live_gate:
            enable_reasons.append("missing paper_live_evidence_gate_report")
        if paper_live_gate_status == MISSING:
            missing_reasons.append("paper_live_evidence_gate_status=missing")
        if not paper_live_mode_allowed:
            enable_reasons.append(f"paper_live_{target_mode}_allowed=false")

    enabled = bool(enabled and (target_mode == "backtest" or paper_live_mode_allowed))

    if enabled:
        enable_status = "enabled"
    elif requested_enable:
        enable_status = MISSING if missing_reasons or not decision_id or decision_verdict == MISSING else BLOCKED
    else:
        enable_status = "disabled"

    return {
        "status": READY,
        "enable_status": enable_status,
        "enabled": enabled,
        "requested_enable": requested_enable,
        "decision_id": decision_id,
        "run_id": _int_or_none(row.get("run_id")),
        "target_mode": target_mode,
        "strategy_name": str(row.get("strategy_name") or "unknown"),
        "strategy_version": str(row.get("strategy_version") or "unknown"),
        "market_slug": str(row.get("market_slug") or ""),
        "token_side": str(row.get("token_side") or ""),
        "price_source": str(row.get("price_source") or ""),
        "actual_execution_engine": row.get("actual_execution_engine") or "unknown",
        "activation_allowed": activation_allowed,
        "decision_verdict": decision_verdict,
        "promotion_verdict": row.get("promotion_verdict") or MISSING,
        "paper_live_evidence_gate_status": paper_live_gate_status,
        "paper_live_paper_allowed": paper_live_paper_allowed,
        "paper_live_live_allowed": paper_live_live_allowed,
        "blocked_reasons": blocked_reasons,
        "review_reasons": review_reasons,
        "missing_reasons": missing_reasons,
        "enable_reasons": enable_reasons,
        "requested_by": requested_by or row.get("requested_by") or "",
        "reason": reason or "",
        "activation_decision": row,
    }


def upsert_strategy_enable_state(conn: Any, state: Mapping[str, Any]) -> dict[str, Any]:
    """Persist the current enable state keyed by strategy/mode/market/outcome."""

    if not _enable_state_table_exists(conn):
        raise RuntimeError(f"{ENABLE_STATE_TABLE} does not exist; run quant.core.schema.create_schema first")
    row = dict(state)
    if row.get("requested_enable") and not row.get("enabled"):
        raise ValueError("cannot enable strategy unless activation decision is persisted, allowed, and ready")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.strategy_enable_state (
                decision_id, run_id, target_mode, strategy_name, strategy_version,
                market_slug, token_side, price_source, actual_execution_engine,
                enabled, enable_status, activation_allowed, decision_verdict,
                promotion_verdict, paper_live_evidence_gate_status,
                paper_live_paper_allowed, paper_live_live_allowed,
                blocked_reasons, review_reasons, missing_reasons,
                enable_reasons, requested_by, reason, activation_decision
            )
            VALUES (
                %(decision_id)s, %(run_id)s, %(target_mode)s, %(strategy_name)s, %(strategy_version)s,
                %(market_slug)s, %(token_side)s, %(price_source)s, %(actual_execution_engine)s,
                %(enabled)s, %(enable_status)s, %(activation_allowed)s, %(decision_verdict)s,
                %(promotion_verdict)s, %(paper_live_evidence_gate_status)s,
                %(paper_live_paper_allowed)s, %(paper_live_live_allowed)s,
                %(blocked_reasons)s::jsonb, %(review_reasons)s::jsonb, %(missing_reasons)s::jsonb,
                %(enable_reasons)s::jsonb, %(requested_by)s, %(reason)s, %(activation_decision)s::jsonb
            )
            ON CONFLICT (strategy_name, strategy_version, target_mode, market_slug, token_side)
            DO UPDATE SET
                decision_id = EXCLUDED.decision_id,
                run_id = EXCLUDED.run_id,
                price_source = EXCLUDED.price_source,
                actual_execution_engine = EXCLUDED.actual_execution_engine,
                enabled = EXCLUDED.enabled,
                enable_status = EXCLUDED.enable_status,
                activation_allowed = EXCLUDED.activation_allowed,
                decision_verdict = EXCLUDED.decision_verdict,
                promotion_verdict = EXCLUDED.promotion_verdict,
                paper_live_evidence_gate_status = EXCLUDED.paper_live_evidence_gate_status,
                paper_live_paper_allowed = EXCLUDED.paper_live_paper_allowed,
                paper_live_live_allowed = EXCLUDED.paper_live_live_allowed,
                blocked_reasons = EXCLUDED.blocked_reasons,
                review_reasons = EXCLUDED.review_reasons,
                missing_reasons = EXCLUDED.missing_reasons,
                enable_reasons = EXCLUDED.enable_reasons,
                requested_by = EXCLUDED.requested_by,
                reason = EXCLUDED.reason,
                activation_decision = EXCLUDED.activation_decision,
                updated_at = now()
            RETURNING *
            """,
            {
                **row,
                "blocked_reasons": json.dumps(row.get("blocked_reasons") or [], ensure_ascii=True, default=str),
                "review_reasons": json.dumps(row.get("review_reasons") or [], ensure_ascii=True, default=str),
                "missing_reasons": json.dumps(row.get("missing_reasons") or [], ensure_ascii=True, default=str),
                "enable_reasons": json.dumps(row.get("enable_reasons") or [], ensure_ascii=True, default=str),
                "activation_decision": json.dumps(row.get("activation_decision") or {}, ensure_ascii=True, default=str),
            },
        )
        stored = cur.fetchone()
    return dict(stored) if stored else row


def load_strategy_enable_state(
    conn: Any,
    *,
    target_mode: str | None = None,
    enabled_only: bool = False,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Load strategy enable state rows for runner/API consumption."""

    if not _enable_state_table_exists(conn):
        return []
    filters: list[str] = []
    params: list[Any] = []
    if target_mode:
        filters.append("target_mode = %s")
        params.append(_target_mode(target_mode))
    if enabled_only:
        filters.append("enabled = TRUE")
    where = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(max(1, min(int(limit), 500)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.strategy_enable_state
            {where}
            ORDER BY updated_at DESC, enable_id DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def strategy_activation_decision_to_markdown(decision: Mapping[str, Any]) -> str:
    """Render a concise human-readable activation decision."""

    return "\n".join(
        [
            f"# Strategy Activation Decision: {decision.get('decision_verdict')}",
            "",
            f"- run_id: {decision.get('run_id')}",
            f"- target_mode: {decision.get('target_mode')}",
            f"- activation_allowed: {decision.get('activation_allowed')}",
            f"- strategy: {decision.get('strategy_name')}@{decision.get('strategy_version')}",
            f"- market: {decision.get('market_slug') or '-'} / {decision.get('token_side') or '-'}",
            f"- actual_execution_engine: {decision.get('actual_execution_engine')}",
            f"- promotion_verdict: {decision.get('promotion_verdict')}",
            f"- paper_promotion_allowed: {decision.get('paper_promotion_allowed')}",
            f"- production_promotion_allowed: {decision.get('production_promotion_allowed')}",
            f"- paper_live_evidence_gate_status: {decision.get('paper_live_evidence_gate_status')}",
            f"- paper_live_paper_allowed: {decision.get('paper_live_paper_allowed')}",
            f"- paper_live_live_allowed: {decision.get('paper_live_live_allowed')}",
            f"- allowed_next_modes: {', '.join(_string_list(decision.get('allowed_next_modes'))) or '-'}",
            f"- blocked_reasons: {', '.join(_string_list(decision.get('blocked_reasons'))) or '-'}",
            f"- review_reasons: {', '.join(_string_list(decision.get('review_reasons'))) or '-'}",
            f"- missing_reasons: {', '.join(_string_list(decision.get('missing_reasons'))) or '-'}",
            f"- decision_reasons: {', '.join(_string_list(decision.get('decision_reasons'))) or '-'}",
        ]
    )


def strategy_enable_state_to_markdown(state: Mapping[str, Any]) -> str:
    """Render a concise strategy enable-state report."""

    return "\n".join(
        [
            f"# Strategy Enable State: {state.get('enable_status')}",
            "",
            f"- enabled: {state.get('enabled')}",
            f"- target_mode: {state.get('target_mode')}",
            f"- decision_id: {state.get('decision_id')}",
            f"- run_id: {state.get('run_id')}",
            f"- strategy: {state.get('strategy_name')}@{state.get('strategy_version')}",
            f"- market: {state.get('market_slug') or '-'} / {state.get('token_side') or '-'}",
            f"- actual_execution_engine: {state.get('actual_execution_engine')}",
            f"- activation_allowed: {state.get('activation_allowed')}",
            f"- decision_verdict: {state.get('decision_verdict')}",
            f"- promotion_verdict: {state.get('promotion_verdict')}",
            f"- paper_live_evidence_gate_status: {state.get('paper_live_evidence_gate_status')}",
            f"- paper_live_paper_allowed: {state.get('paper_live_paper_allowed')}",
            f"- paper_live_live_allowed: {state.get('paper_live_live_allowed')}",
            f"- enable_reasons: {', '.join(_string_list(state.get('enable_reasons'))) or '-'}",
        ]
    )


def _decision_verdict(
    *,
    activation_allowed: bool,
    target_mode: str,
    promotion_verdict: str,
    blocked_reasons: Sequence[str],
    missing_reasons: Sequence[str],
    review_reasons: Sequence[str],
    reasons: Sequence[str],
) -> str:
    if activation_allowed:
        if target_mode == "live" and (review_reasons or promotion_verdict != READY):
            return REVIEW
        return READY
    if missing_reasons or promotion_verdict == MISSING:
        return MISSING
    if blocked_reasons or reasons:
        return BLOCKED
    return REVIEW


def _target_mode(value: str) -> str:
    mode = str(value or "").strip().lower().replace("_", "-")
    aliases = {
        "dryrun": "backtest",
        "dry-run": "backtest",
        "simulation": "backtest",
        "paper-trading": "paper",
        "prod": "live",
        "production": "live",
    }
    normalized = aliases.get(mode, mode)
    if normalized not in TARGET_MODES:
        raise ValueError(f"target_mode must be one of {', '.join(TARGET_MODES)}")
    return normalized


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _paper_live_gate_from_decision(row: Mapping[str, Any]) -> dict[str, Any]:
    gate = _mapping(row.get("paper_live_evidence_gate_report"))
    if gate:
        return gate
    artifact_summary = _mapping(row.get("artifact_summary"))
    return _mapping(artifact_summary.get("paper_live_evidence_gate_report"))


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Sequence):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _int_or_none(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (ACTIVATION_TABLE,))
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row and row[0])


def _enable_state_table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (ENABLE_STATE_TABLE,))
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row and row[0])
