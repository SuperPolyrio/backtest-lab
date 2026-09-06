"""Launch checklist for fill-first paper/live operation."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from quant.backtest.configured_external_sources import load_external_source_env_files
from quant.backtest.external_source_onboarding import build_external_source_onboarding_plan
from quant.backtest.fill_first_production_readiness import build_fill_first_production_readiness_report


READY = "ready"
REVIEW = "review"
BLOCKED = "blocked"
FAIL = "fail"


def build_fill_first_production_launch_checklist(
    *,
    target_mode: str = "paper",
    env: Mapping[str, str] | None = None,
    env_files: Sequence[Path | str] = (),
    external_source_env_files: Sequence[Path | str] | None = None,
    order_execution_env: Mapping[str, str] | None = None,
    order_execution_env_files: Sequence[Path | str] | None = None,
    project_root: Path | None = None,
    conn: Any | None = None,
    check_db: bool = False,
    run_id: int | None = None,
    run_artifact_report: Mapping[str, Any] | None = None,
    max_stale_seconds: int = 86400,
) -> dict[str, Any]:
    """Combine external-source onboarding and production readiness into one launch checklist."""

    source_env_files = tuple(external_source_env_files if external_source_env_files is not None else env_files)
    order_env_files = tuple(order_execution_env_files or ())
    all_env_files = tuple(source_env_files) + tuple(order_env_files)
    try:
        source_values = _load_env(env=env, env_files=source_env_files)
        order_values = _load_env(env=order_execution_env or env, env_files=order_env_files) if order_env_files or order_execution_env else source_values
        values = _merge_env_values(source_values, order_values)
    except Exception as exc:
        env_paths = [str(Path(path).expanduser()) for path in all_env_files]
        return {
            "schema_version": "fill_first_production_launch_checklist_v1",
            "status": BLOCKED,
            "target_mode": _target_mode(target_mode),
            "launch_allowed": False,
            "scope": "fill-first/orderfilled-calibrated execution; LOB/DEPTH intentionally excluded",
            "env_files": env_paths,
            "external_source_env_files": [str(Path(path).expanduser()) for path in source_env_files],
            "order_execution_env_files": [str(Path(path).expanduser()) for path in order_env_files],
            "run_id": run_id,
            "phases": [
                {
                    "name": "env files",
                    "status": BLOCKED,
                    "reason": f"env file load failed: {exc}",
                    "evidence": "env-file",
                }
            ],
            "blocked_reasons": [f"env file load failed: {exc}"],
            "review_reasons": [],
            "command_plan": _command_plan(
                external_source_env_files=source_env_files,
                order_execution_env_files=order_env_files,
                target_mode=target_mode,
                run_id=run_id,
                check_db=check_db,
            ),
            "next_actions": [f"Fix env-file syntax or path: {exc}"],
        }

    onboarding = build_external_source_onboarding_plan(
        source_env_files,
        env=source_values,
        project_root=project_root,
    )
    readiness = build_fill_first_production_readiness_report(
        target_mode=target_mode,
        env=values,
        env_files=all_env_files,
        project_root=project_root,
        conn=conn,
        check_db=check_db,
        run_id=run_id,
        run_artifact_report=run_artifact_report,
        max_stale_seconds=max_stale_seconds,
    )
    phases = _phases(onboarding=onboarding, readiness=readiness, check_db=check_db, run_id=run_id)
    status = _aggregate_phase_status(phases)
    launch_allowed = status == READY and bool(readiness.get("launch_allowed"))
    blocked_reasons = [str(phase["reason"]) for phase in phases if phase["status"] in {BLOCKED, FAIL}]
    review_reasons = [str(phase["reason"]) for phase in phases if phase["status"] == REVIEW]
    return {
        "schema_version": "fill_first_production_launch_checklist_v1",
        "status": status,
        "target_mode": _target_mode(target_mode),
        "launch_allowed": launch_allowed,
        "scope": "fill-first/orderfilled-calibrated execution; LOB/DEPTH intentionally excluded",
        "check_db": bool(check_db),
        "run_id": run_id,
        "max_stale_seconds": int(max_stale_seconds),
        "env_files": [str(Path(path).expanduser()) for path in all_env_files],
        "external_source_env_files": [str(Path(path).expanduser()) for path in source_env_files],
        "order_execution_env_files": [str(Path(path).expanduser()) for path in order_env_files],
        "phases": phases,
        "blocked_reasons": blocked_reasons,
        "review_reasons": review_reasons,
        "command_plan": _command_plan(
            external_source_env_files=source_env_files,
            order_execution_env_files=order_env_files,
            target_mode=target_mode,
            run_id=run_id,
            check_db=check_db,
        ),
        "next_actions": _next_actions(
            status=status,
            phases=phases,
            command_plan=_command_plan(
                external_source_env_files=source_env_files,
                order_execution_env_files=order_env_files,
                target_mode=target_mode,
                run_id=run_id,
                check_db=check_db,
            ),
        ),
        "external_source_onboarding": onboarding,
        "production_readiness": readiness,
    }


def production_launch_checklist_to_markdown(report: Mapping[str, Any]) -> str:
    """Render a concise launch checklist."""

    lines = [
        f"# Fill-first Production Launch Checklist: {report.get('status')}",
        "",
        f"- target_mode: {report.get('target_mode')}",
        f"- launch_allowed: {report.get('launch_allowed')}",
        f"- run_id: {report.get('run_id') or '-'}",
        f"- check_db: {report.get('check_db')}",
        f"- scope: {report.get('scope')}",
        "",
        "| Phase | Status | Reason | Evidence |",
        "| --- | --- | --- | --- |",
    ]
    for phase in report.get("phases") or []:
        if not isinstance(phase, Mapping):
            continue
        lines.append(
            "| {name} | {status} | {reason} | `{evidence}` |".format(
                name=phase.get("name", ""),
                status=phase.get("status", ""),
                reason=str(phase.get("reason") or "").replace("|", "\\|"),
                evidence=phase.get("evidence", ""),
            )
        )
    commands = report.get("command_plan") if isinstance(report.get("command_plan"), Mapping) else {}
    if commands:
        lines.extend(["", "## Command Plan"])
        for key, command in commands.items():
            lines.append(f"- {key}: `{command}`")
    blockers = [str(item) for item in report.get("blocked_reasons") or []]
    if blockers:
        lines.extend(["", "## Blocked Reasons"])
        lines.extend(f"- {reason}" for reason in blockers)
    reviews = [str(item) for item in report.get("review_reasons") or []]
    if reviews:
        lines.extend(["", "## Review Reasons"])
        lines.extend(f"- {reason}" for reason in reviews)
    lines.extend(["", "## Next Actions"])
    actions = [str(item) for item in report.get("next_actions") or []]
    lines.extend(f"- {action}" for action in actions) if actions else lines.append("- none")
    return "\n".join(lines)


def _load_env(*, env: Mapping[str, str] | None, env_files: Sequence[Path | str]) -> dict[str, str]:
    base = dict(os.environ if env is None else env)
    if env_files:
        return load_external_source_env_files(env_files, base_env=base)
    return base


def _merge_env_values(*envs: Mapping[str, str] | None) -> dict[str, str]:
    merged: dict[str, str] = {}
    for env in envs:
        if env:
            merged.update({str(key): str(value) for key, value in env.items()})
    return merged


def _phases(*, onboarding: Mapping[str, Any], readiness: Mapping[str, Any], check_db: bool, run_id: int | None) -> list[dict[str, Any]]:
    dry_run = onboarding.get("configured_import_dry_run") if isinstance(onboarding.get("configured_import_dry_run"), Mapping) else {}
    write_preview = onboarding.get("configured_import_write_preview") if isinstance(onboarding.get("configured_import_write_preview"), Mapping) else {}
    health = readiness.get("external_source_import_health") if isinstance(readiness.get("external_source_import_health"), Mapping) else {}
    run_coverage = readiness.get("external_source_run_coverage") if isinstance(readiness.get("external_source_run_coverage"), Mapping) else {}
    paper_live_gate = readiness.get("paper_live_evidence_gate") if isinstance(readiness.get("paper_live_evidence_gate"), Mapping) else {}
    return [
        {
            "name": "external source onboarding",
            "status": _phase_status(onboarding.get("status")),
            "reason": _source_count_reason(onboarding),
            "evidence": "scripts/plan_external_source_onboarding.py",
        },
        {
            "name": "configured import dry-run",
            "status": _phase_status(dry_run.get("status")),
            "reason": f"configured={dry_run.get('configured_count', 0)} skipped={dry_run.get('skipped_count', 0)}",
            "evidence": "scripts/run_configured_external_source_imports.py",
        },
        {
            "name": "configured import write preview",
            "status": _phase_status(write_preview.get("status")),
            "reason": f"configured={write_preview.get('configured_count', 0)} skipped={write_preview.get('skipped_count', 0)} preview-only",
            "evidence": "scripts/run_configured_external_source_imports.py --write",
        },
        {
            "name": "external source health",
            "status": _health_status(health, check_db=check_db),
            "reason": str(health.get("reason") or ("check-db enabled" if check_db else "database not checked")),
            "evidence": "scripts/check_external_source_import_health.py",
        },
        {
            "name": "run external evidence",
            "status": _run_evidence_status(run_coverage, paper_live_gate, run_id=run_id, check_db=check_db),
            "reason": _run_evidence_reason(run_coverage, paper_live_gate, run_id=run_id, check_db=check_db),
            "evidence": "scripts/check_external_source_run_coverage.py",
        },
        {
            "name": "production readiness",
            "status": _phase_status(readiness.get("status")),
            "reason": f"launch_allowed={readiness.get('launch_allowed')}",
            "evidence": "scripts/check_fill_first_production_readiness.py",
        },
    ]


def _command_plan(
    *,
    external_source_env_files: Sequence[Path | str],
    order_execution_env_files: Sequence[Path | str],
    target_mode: str,
    run_id: int | None,
    check_db: bool,
) -> dict[str, str]:
    source_env_args = _generic_env_args(external_source_env_files)
    order_env_args = _generic_env_args(order_execution_env_files)
    all_env_files = tuple(external_source_env_files) + tuple(order_execution_env_files)
    all_env_args = _generic_env_args(all_env_files)
    run_arg = f" --run-id {int(run_id)}" if run_id is not None else ""
    db_arg = " --check-db" if check_db else ""
    mode_arg = f"--target-mode {_target_mode(target_mode)}"
    source_suffix = f" {source_env_args}" if source_env_args else ""
    order_suffix = f" {order_env_args}" if order_env_args else ""
    all_suffix = f" {all_env_args}" if all_env_args else ""
    return {
        "1_external_env_audit": f"conda run -n polyBacktest python scripts/audit_external_source_env.py{source_suffix} --format markdown",
        "2_order_execution_env_audit": f"conda run -n polyBacktest python scripts/audit_order_execution_env.py{order_suffix} --format markdown",
        "3_import_dry_run": f"conda run -n polyBacktest python scripts/run_configured_external_source_imports.py{source_suffix} --format markdown",
        "4_import_write": f"conda run -n polyBacktest python scripts/run_configured_external_source_imports.py{source_suffix} --write --format markdown",
        "5_import_health": f"conda run -n polyBacktest python scripts/check_external_source_import_health.py --format markdown",
        "6_production_readiness": f"conda run -n polyBacktest python scripts/check_fill_first_production_readiness.py {mode_arg}{all_suffix}{db_arg}{run_arg} --format markdown",
        "7_quality_gate": f"conda run -n polyBacktest python scripts/run_fill_first_quality_gate.py{_quality_gate_env_args(external_source_env_files, order_execution_env_files)}{db_arg} --format markdown",
        "8_install_configured_import_timer": (
            "sudo cp deploy/systemd/quant-configured-external-sources-import.service.example "
            "/etc/systemd/system/quant-configured-external-sources-import.service && "
            "sudo cp deploy/systemd/quant-configured-external-sources-import.timer.example "
            "/etc/systemd/system/quant-configured-external-sources-import.timer && "
            "sudo systemctl daemon-reload && "
            "sudo systemctl enable --now quant-configured-external-sources-import.timer"
        ),
        "9_install_external_health_timer": (
            "sudo cp deploy/systemd/quant-external-source-health.service.example "
            "/etc/systemd/system/quant-external-source-health.service && "
            "sudo cp deploy/systemd/quant-external-source-health.timer.example "
            "/etc/systemd/system/quant-external-source-health.timer && "
            "sudo systemctl daemon-reload && "
            "sudo systemctl enable --now quant-external-source-health.timer"
        ),
    }


def _next_actions(*, status: str, phases: Sequence[Mapping[str, Any]], command_plan: Mapping[str, str]) -> list[str]:
    if status == READY:
        return [
            "Run a paper/shadow validation window before any live execution.",
            "Keep ORDER_EXECUTION live confirmation unset unless intentionally doing live execution.",
        ]
    actions = []
    for phase in phases:
        if phase.get("status") != READY:
            actions.append(f"Fix {phase.get('name')}: {phase.get('reason')}")
    actions.extend(
        [
            f"Start with external env audit: {command_plan.get('1_external_env_audit')}",
            f"Then audit order execution env: {command_plan.get('2_order_execution_env_audit')}",
            f"Then dry-run imports: {command_plan.get('3_import_dry_run')}",
        ]
    )
    return actions


def _aggregate_phase_status(phases: Sequence[Mapping[str, Any]]) -> str:
    statuses = {str(phase.get("status")) for phase in phases}
    if statuses & {BLOCKED, FAIL}:
        return BLOCKED
    if REVIEW in statuses:
        return REVIEW
    return READY


def _phase_status(value: Any) -> str:
    status = str(value or REVIEW).lower()
    if status == FAIL:
        return BLOCKED
    if status in {READY, REVIEW, BLOCKED}:
        return status
    return REVIEW


def _health_status(health: Mapping[str, Any], *, check_db: bool) -> str:
    if not check_db or not health.get("checked"):
        return REVIEW
    return _phase_status(health.get("status"))


def _run_evidence_status(
    run_coverage: Mapping[str, Any],
    paper_live_gate: Mapping[str, Any],
    *,
    run_id: int | None,
    check_db: bool,
) -> str:
    if run_id is None or not check_db:
        return REVIEW
    if run_coverage.get("status") == READY and paper_live_gate.get("status") == READY:
        return READY
    return BLOCKED


def _run_evidence_reason(
    run_coverage: Mapping[str, Any],
    paper_live_gate: Mapping[str, Any],
    *,
    run_id: int | None,
    check_db: bool,
) -> str:
    if run_id is None:
        return "run_id not provided; run-level evidence gate not checked"
    if not check_db:
        return "database not checked; pass --check-db with --run-id"
    return (
        f"run_id={run_id} coverage={run_coverage.get('status')} "
        f"paper_live_gate={paper_live_gate.get('status')}"
    )


def _source_count_reason(report: Mapping[str, Any]) -> str:
    return (
        f"ready={report.get('ready_count', 0)} "
        f"review={report.get('review_count', 0)} "
        f"fail={report.get('fail_count', 0)}"
    )


def _generic_env_args(env_files: Sequence[Path | str]) -> str:
    return " ".join(f"--env-file {Path(path).expanduser()}" for path in env_files)


def _quality_gate_env_args(
    external_source_env_files: Sequence[Path | str],
    order_execution_env_files: Sequence[Path | str],
) -> str:
    args = []
    args.extend(f"--external-env-file {Path(path).expanduser()}" for path in external_source_env_files)
    args.extend(f"--order-execution-env-file {Path(path).expanduser()}" for path in order_execution_env_files)
    return (" " + " ".join(args)) if args else ""


def _target_mode(value: str | None) -> str:
    mode = str(value or "paper").strip().lower().replace("_", "-")
    return mode if mode in {"paper", "live"} else "paper"
