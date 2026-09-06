"""Production preflight for fill-first paper/live execution."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from quant.backtest.configured_external_sources import load_external_source_env_files
from quant.backtest.external_source_env_audit import build_external_source_env_audit
from quant.backtest.external_source_run_coverage import (
    build_external_source_run_coverage_report,
    load_external_source_run_coverage_inputs,
)
from quant.backtest.external_source_state import (
    READY as HEALTH_READY,
    evaluate_external_source_import_health,
    load_external_source_import_states,
)
from quant.backtest.order_execution_safety import build_order_execution_env_audit
from quant.backtest.run_artifacts import build_backtest_run_artifact_report, load_backtest_run_artifact_inputs


READY = "ready"
REVIEW = "review"
BLOCKED = "blocked"


def build_fill_first_production_readiness_report(
    *,
    target_mode: str = "paper",
    env: Mapping[str, str] | None = None,
    env_files: Sequence[Path | str] = (),
    project_root: Path | None = None,
    conn: Any | None = None,
    check_db: bool = False,
    run_id: int | None = None,
    run_artifact_report: Mapping[str, Any] | None = None,
    max_stale_seconds: int = 86400,
) -> dict[str, Any]:
    """Return a single preflight report for paper/live fill-first execution."""

    try:
        values = _load_env(env=env, env_files=env_files)
    except Exception as exc:
        return {
            "status": BLOCKED,
            "target_mode": _target_mode(target_mode),
            "launch_allowed": False,
            "scope": "fill-first paper/live execution; LOB/DEPTH intentionally excluded",
            "env_files": [str(Path(path).expanduser()) for path in env_files],
            "external_source_env_audit": {"status": BLOCKED, "issues": [f"env file load failed: {exc}"]},
            "order_execution_env_audit": {"status": BLOCKED, "issues": []},
            "checks": [
                {
                    "name": "env files",
                    "status": BLOCKED,
                    "reason": f"env file load failed: {exc}",
                    "evidence": "scripts/check_fill_first_production_readiness.py",
                }
            ],
            "blocked_reasons": [f"env file load failed: {exc}"],
            "review_reasons": [],
            "next_actions": [f"Fix blocker: env files - env file load failed: {exc}"],
        }
    mode = _target_mode(target_mode or values.get("ORDER_EXECUTION_TARGET_MODE") or "paper")
    external = build_external_source_env_audit(env_files, env=values, project_root=project_root)
    execution = build_order_execution_env_audit(values)
    external_health = _build_external_source_health_report(
        mode=mode,
        external=external,
        conn=conn,
        check_db=check_db,
        max_stale_seconds=max_stale_seconds,
    )
    run_coverage = _build_run_coverage_report(conn=conn, check_db=check_db, run_id=run_id)
    paper_live_gate = _build_paper_live_gate_report(
        conn=conn,
        check_db=check_db,
        run_id=run_id,
        mode=mode,
        run_artifact_report=run_artifact_report,
    )
    checks = _build_checks(
        mode=mode,
        external=external,
        execution=execution,
        external_health=external_health,
        run_coverage=run_coverage,
        paper_live_gate=paper_live_gate,
    )
    status = _aggregate(check["status"] for check in checks)
    return {
        "status": status,
        "target_mode": mode,
        "launch_allowed": status == READY,
        "scope": "fill-first paper/live execution; LOB/DEPTH intentionally excluded",
        "check_db": bool(check_db),
        "run_id": run_id,
        "max_stale_seconds": int(max_stale_seconds),
        "env_files": [str(Path(path).expanduser()) for path in env_files],
        "external_source_env_audit": external,
        "external_source_import_health": external_health,
        "external_source_run_coverage": run_coverage,
        "paper_live_evidence_gate": paper_live_gate,
        "order_execution_env_audit": execution,
        "checks": checks,
        "blocked_reasons": [check["reason"] for check in checks if check["status"] == BLOCKED],
        "review_reasons": [check["reason"] for check in checks if check["status"] == REVIEW],
        "next_actions": _next_actions(mode=mode, checks=checks),
    }


def fill_first_production_readiness_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Fill-first Production Readiness: {report.get('status')}",
        "",
        f"- target_mode: {report.get('target_mode')}",
        f"- launch_allowed: {report.get('launch_allowed')}",
        f"- run_id: {report.get('run_id') or '-'}",
        f"- scope: {report.get('scope')}",
        "",
        "| Check | Status | Reason | Evidence |",
        "| --- | --- | --- | --- |",
    ]
    for check in report.get("checks") or []:
        if not isinstance(check, Mapping):
            continue
        lines.append(
            "| {name} | {status} | {reason} | `{evidence}` |".format(
                name=check.get("name", ""),
                status=check.get("status", ""),
                reason=str(check.get("reason") or "").replace("|", "\\|"),
                evidence=check.get("evidence", ""),
            )
        )
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions") or [])
    return "\n".join(lines)


def _build_checks(
    *,
    mode: str,
    external: Mapping[str, Any],
    execution: Mapping[str, Any],
    external_health: Mapping[str, Any],
    run_coverage: Mapping[str, Any],
    paper_live_gate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    checks = [
        _check_execution_config(execution),
        _check_external_source(external, "real_order_state_events", required=False),
        _check_external_source(external, "real_cost_events", required=True),
        _check_external_source(external, "platform_incidents", required=True),
        _check_external_source(external, "external_signal_events", required=False),
    ]
    if external_health.get("checked"):
        checks.append(_check_external_source_health(external_health))
    if run_coverage.get("checked"):
        checks.append(_check_run_coverage(run_coverage))
    if paper_live_gate.get("checked"):
        checks.append(_check_paper_live_gate(paper_live_gate, mode=mode))
    if mode == "live":
        checks.append(_check_live_confirmation(execution))
        checks.append(_check_all_external_sources_ready(external))
    return checks


def _check_execution_config(execution: Mapping[str, Any]) -> dict[str, Any]:
    if execution.get("status") == READY:
        return {
            "name": "order execution endpoint",
            "status": READY,
            "reason": "ORDER_EXECUTION endpoint and auth settings are ready",
            "evidence": "scripts/audit_order_execution_env.py",
        }
    issues = execution.get("issues") if isinstance(execution.get("issues"), list) else []
    return {
        "name": "order execution endpoint",
        "status": BLOCKED,
        "reason": "; ".join(str(issue) for issue in issues) or "ORDER_EXECUTION endpoint is not ready",
        "evidence": "scripts/audit_order_execution_env.py",
    }


def _check_external_source(external: Mapping[str, Any], kind: str, *, required: bool) -> dict[str, Any]:
    source = _source_report(external, kind)
    if source and source.get("status") == READY:
        return {
            "name": kind,
            "status": READY,
            "reason": f"{kind} source configured",
            "evidence": "scripts/audit_external_source_env.py",
        }
    status = BLOCKED if required else REVIEW
    reason = f"{kind}: source is not configured"
    if source:
        issues = source.get("issues") if isinstance(source.get("issues"), list) else []
        detail = "; ".join(str(issue) for issue in issues)
        reason = f"{kind}: {detail}" if detail else reason
    return {
        "name": kind,
        "status": status,
        "reason": reason,
        "evidence": "scripts/audit_external_source_env.py",
    }


def _check_live_confirmation(execution: Mapping[str, Any]) -> dict[str, Any]:
    if execution.get("live_confirmed"):
        return {
            "name": "live confirmation",
            "status": READY,
            "reason": "live execution confirmation token is set",
            "evidence": "ORDER_EXECUTION_LIVE_CONFIRM",
        }
    return {
        "name": "live confirmation",
        "status": BLOCKED,
        "reason": f"live execution requires ORDER_EXECUTION_LIVE_CONFIRM={execution.get('required_live_confirm')}",
        "evidence": "ORDER_EXECUTION_LIVE_CONFIRM",
    }


def _check_all_external_sources_ready(external: Mapping[str, Any]) -> dict[str, Any]:
    if external.get("status") == READY:
        return {
            "name": "live external source completeness",
            "status": READY,
            "reason": "all external source env checks are ready",
            "evidence": "scripts/audit_external_source_env.py",
        }
    return {
        "name": "live external source completeness",
        "status": BLOCKED,
        "reason": "live execution requires ready order-state, cost, incident, and external signal source configuration",
        "evidence": "scripts/audit_external_source_env.py",
    }


def _build_external_source_health_report(
    *,
    mode: str,
    external: Mapping[str, Any],
    conn: Any | None,
    check_db: bool,
    max_stale_seconds: int,
) -> dict[str, Any]:
    if not check_db:
        return {
            "checked": False,
            "status": REVIEW,
            "reason": "database not checked; pass --check-db to inspect import freshness rows",
            "state_count": None,
        }
    if conn is None:
        return {
            "checked": True,
            "status": BLOCKED,
            "reason": "database connection required for --check-db external source health",
            "state_count": None,
            "items": [],
        }
    try:
        states = load_external_source_import_states(conn, limit=100)
        report = evaluate_external_source_import_health(states, max_stale_seconds=max_stale_seconds)
    except Exception as exc:  # pragma: no cover - defensive live DB path
        return {
            "checked": True,
            "status": BLOCKED,
            "reason": f"external source health check failed: {exc}",
            "state_count": None,
            "items": [],
        }
    expected_sources = _expected_external_health_sources(external=external, mode=mode, states=states, report=report)
    return {
        "checked": True,
        **report,
        "expected_sources": expected_sources,
        "missing_required_count": sum(
            1
            for item in expected_sources
            if item.get("required") and item.get("reason") in {"missing_import_state", "missing_state_key"}
        ),
        "unhealthy_required_count": sum(
            1
            for item in expected_sources
            if item.get("required") and item.get("status") not in {HEALTH_READY}
        ),
    }


def _build_run_coverage_report(*, conn: Any | None, check_db: bool, run_id: int | None) -> dict[str, Any]:
    if run_id is None:
        return {
            "checked": False,
            "status": REVIEW,
            "reason": "run_id not provided; pass --run-id with --check-db to require run-level external evidence coverage",
            "run_id": None,
        }
    if not check_db:
        return {
            "checked": False,
            "status": REVIEW,
            "reason": "database not checked; pass --check-db with --run-id to inspect run-level external evidence coverage",
            "run_id": int(run_id),
        }
    if conn is None:
        return {
            "checked": True,
            "status": BLOCKED,
            "reason": "database connection required for run-level external evidence coverage",
            "run_id": int(run_id),
        }
    try:
        inputs = load_external_source_run_coverage_inputs(conn, run_id=int(run_id))
        report = build_external_source_run_coverage_report(inputs, run_id=int(run_id))
    except Exception as exc:  # pragma: no cover - defensive live DB path
        return {
            "checked": True,
            "status": BLOCKED,
            "reason": f"run-level external evidence coverage check failed: {exc}",
            "run_id": int(run_id),
        }
    return {"checked": True, **report}


def _build_paper_live_gate_report(
    *,
    conn: Any | None,
    check_db: bool,
    run_id: int | None,
    mode: str,
    run_artifact_report: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if run_artifact_report is not None:
        gate = (
            run_artifact_report.get("paper_live_evidence_gate_report")
            if isinstance(run_artifact_report.get("paper_live_evidence_gate_report"), Mapping)
            else {}
        )
        return _paper_live_gate_from_artifact(
            gate=gate,
            run_id=run_id,
            mode=mode,
            reason="local run artifact report provided",
        )
    if run_id is None:
        return {
            "checked": False,
            "status": REVIEW,
            "reason": "run_id not provided; pass --run-id with --check-db to require paper/live evidence gate",
            "run_id": None,
            "target_mode": mode,
        }
    if not check_db:
        return {
            "checked": False,
            "status": REVIEW,
            "reason": "database not checked; pass --check-db with --run-id to inspect paper/live evidence gate",
            "run_id": int(run_id),
            "target_mode": mode,
        }
    if conn is None:
        return {
            "checked": True,
            "status": BLOCKED,
            "reason": "database connection required for paper/live evidence gate",
            "run_id": int(run_id),
            "target_mode": mode,
        }
    try:
        inputs = load_backtest_run_artifact_inputs(conn, run_id=int(run_id))
        artifact = build_backtest_run_artifact_report(inputs, run_id=int(run_id))
        gate = artifact.get("paper_live_evidence_gate_report") if isinstance(artifact.get("paper_live_evidence_gate_report"), Mapping) else {}
    except Exception as exc:  # pragma: no cover - defensive live DB path
        return {
            "checked": True,
            "status": BLOCKED,
            "reason": f"paper/live evidence gate check failed: {exc}",
            "run_id": int(run_id),
            "target_mode": mode,
        }
    return _paper_live_gate_from_artifact(
        gate=gate,
        run_id=run_id,
        mode=mode,
        reason=str(artifact.get("status") or "artifact audited"),
    )


def _paper_live_gate_from_artifact(
    *,
    gate: Mapping[str, Any],
    run_id: int | None,
    mode: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "checked": True,
        "run_id": int(run_id) if run_id is not None else None,
        "target_mode": mode,
        "status": str(gate.get("status") or BLOCKED),
        "paper_allowed": bool(gate.get("paper_allowed")),
        "live_allowed": bool(gate.get("live_allowed")),
        "run_evidence_ready": bool(gate.get("run_evidence_ready")),
        "missing_evidence_ready": bool(gate.get("missing_evidence_ready")),
        "reason": reason,
        "blocked_reasons": list(gate.get("blocked_reasons") or []),
        "review_reasons": list(gate.get("review_reasons") or []),
        "gate": dict(gate),
    }


def _check_external_source_health(health: Mapping[str, Any]) -> dict[str, Any]:
    status = READY if (
        health.get("status") == HEALTH_READY
        and int(health.get("missing_required_count") or 0) == 0
        and int(health.get("unhealthy_required_count") or 0) == 0
    ) else BLOCKED
    state_count = health.get("state_count")
    reason = f"external source import health {health.get('status')}: {health.get('reason')}"
    if state_count is not None:
        reason += f" (states={state_count})"
    if health.get("missing_required_count") or health.get("unhealthy_required_count"):
        reason += (
            f"; required_missing={int(health.get('missing_required_count') or 0)}"
            f" required_unhealthy={int(health.get('unhealthy_required_count') or 0)}"
        )
    return {
        "name": "external source import health",
        "status": status,
        "reason": reason,
        "evidence": "quant.external_source_import_state",
    }


def _check_run_coverage(run_coverage: Mapping[str, Any]) -> dict[str, Any]:
    status = READY if (
        run_coverage.get("status") == READY
        and str(run_coverage.get("order_state_coverage_pct")) == "100"
        and str(run_coverage.get("calibration_coverage_pct")) == "100"
    ) else BLOCKED
    reason = (
        f"run_id={run_coverage.get('run_id')} external evidence coverage {run_coverage.get('status')}: "
        f"order_state={run_coverage.get('order_state_coverage_pct')}% "
        f"calibration={run_coverage.get('calibration_coverage_pct')}%"
    )
    if run_coverage.get("reason"):
        reason += f"; {run_coverage.get('reason')}"
    return {
        "name": "run-level external evidence coverage",
        "status": status,
        "reason": reason,
        "evidence": "scripts/check_external_source_run_coverage.py",
    }


def _check_paper_live_gate(gate: Mapping[str, Any], *, mode: str) -> dict[str, Any]:
    mode_allowed = bool(gate.get("live_allowed")) if mode == "live" else bool(gate.get("paper_allowed"))
    status = READY if gate.get("status") == READY and mode_allowed else BLOCKED
    reason = (
        f"run_id={gate.get('run_id')} paper/live evidence gate {gate.get('status')}: "
        f"paper_allowed={gate.get('paper_allowed')} live_allowed={gate.get('live_allowed')} "
        f"run_evidence_ready={gate.get('run_evidence_ready')} "
        f"missing_evidence_ready={gate.get('missing_evidence_ready')}"
    )
    reasons = [str(item) for item in (gate.get("blocked_reasons") or [])]
    reasons.extend(str(item) for item in (gate.get("review_reasons") or []) if mode == "live")
    if reasons:
        reason += "; " + "; ".join(reasons)
    return {
        "name": "paper/live evidence gate",
        "status": status,
        "reason": reason,
        "evidence": "quant.backtest.run_artifacts.paper_live_evidence_gate_report",
    }


def _expected_external_health_sources(
    *,
    external: Mapping[str, Any],
    mode: str,
    states: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
) -> list[dict[str, Any]]:
    item_by_key = {
        str(item.get("state_key")): item
        for item in report.get("items") or []
        if isinstance(item, Mapping) and item.get("state_key")
    }
    state_keys = {
        str(state.get("state_key"))
        for state in states
        if isinstance(state, Mapping) and state.get("state_key")
    }
    expected: list[dict[str, Any]] = []
    for source in external.get("sources") or []:
        if not isinstance(source, Mapping):
            continue
        kind = str(source.get("kind") or "")
        state_key = str(source.get("state_key") or "")
        configured = source.get("status") == READY and bool(state_key)
        required = kind in {"real_cost_events", "platform_incidents"} or mode == "live" or configured
        if not required:
            continue
        if not state_key:
            expected.append(
                {
                    "kind": kind,
                    "state_key": state_key,
                    "required": True,
                    "status": BLOCKED,
                    "reason": "missing_state_key",
                }
            )
            continue
        if state_key not in state_keys:
            expected.append(
                {
                    "kind": kind,
                    "state_key": state_key,
                    "required": True,
                    "status": BLOCKED,
                    "reason": "missing_import_state",
                }
            )
            continue
        item = item_by_key.get(state_key, {})
        expected.append(
            {
                "kind": kind,
                "state_key": state_key,
                "required": True,
                "status": item.get("status") or BLOCKED,
                "reason": item.get("reason") or "missing_health_item",
                "age_seconds": item.get("age_seconds"),
                "last_rows_written": item.get("last_rows_written"),
                "last_success_at": item.get("last_success_at"),
            }
        )
    return expected


def _source_report(external: Mapping[str, Any], kind: str) -> Mapping[str, Any] | None:
    for source in external.get("sources") or []:
        if isinstance(source, Mapping) and source.get("kind") == kind:
            return source
    return None


def _aggregate(statuses: Sequence[str] | Any) -> str:
    values = set(statuses)
    if BLOCKED in values:
        return BLOCKED
    if REVIEW in values:
        return REVIEW
    return READY


def _next_actions(*, mode: str, checks: Sequence[Mapping[str, Any]]) -> list[str]:
    actions: list[str] = []
    for check in checks:
        if check.get("status") == BLOCKED:
            actions.append(f"Fix blocker: {check.get('name')} - {check.get('reason')}")
        elif check.get("status") == REVIEW:
            actions.append(f"Review: {check.get('name')} - {check.get('reason')}")
    if not actions:
        actions.append(f"{mode} fill-first preflight is ready; run order execution adapter dry-run before --execute.")
    return actions


def _load_env(*, env: Mapping[str, str] | None, env_files: Sequence[Path | str]) -> dict[str, str]:
    if env_files:
        return load_external_source_env_files(env_files, base_env=dict(env or {}))
    return dict(env or {})


def _target_mode(value: str) -> str:
    mode = str(value or "paper").strip().lower().replace("_", "-")
    return "live" if mode in {"live", "prod", "production"} else "paper"
