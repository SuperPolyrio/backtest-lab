"""Production onboarding plan for fill-first external evidence sources."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from quant.backtest.configured_external_sources import (
    FAIL,
    READY,
    REVIEW,
    build_configured_external_source_report,
    load_external_source_env_files,
)
from quant.backtest.external_source_env_audit import build_external_source_env_audit


SOURCE_LABELS = {
    "real_order_state_events": "真实订单状态",
    "real_cost_events": "真实成本/钱包流水",
    "platform_incidents": "平台异常/维护窗口",
    "external_signal_events": "外部信号输入",
}


def build_external_source_onboarding_plan(
    env_files: Sequence[Path | str] = (),
    *,
    env: Mapping[str, str] | None = None,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Build a source-by-source checklist before paper/live fill-first use."""

    try:
        values = _load_values(env_files, env=env)
    except Exception as exc:
        env_audit = build_external_source_env_audit(env_files, env=env, project_root=project_root)
        return {
            "schema_version": "fill_first_external_source_onboarding_v1",
            "status": FAIL,
            "env_files": [str(Path(path).expanduser()) for path in env_files],
            "source_count": 0,
            "ready_count": 0,
            "review_count": 0,
            "fail_count": 1,
            "production_blockers": [f"env file load failed: {exc}"],
            "sources": [],
            "next_actions": [
                "Fix the env-file syntax or path before planning external source onboarding.",
                *[str(issue) for issue in env_audit.get("issues", [])],
            ],
            "env_audit": env_audit,
            "configured_import_dry_run": {"status": FAIL, "items": []},
            "configured_import_write_preview": {"status": FAIL, "items": []},
        }
    env_audit = build_external_source_env_audit(env_files, env=values, project_root=project_root)
    dry_run_report = build_configured_external_source_report(
        values,
        project_root=project_root,
        dry_run=True,
        skip_init_schema=True,
    )
    write_report = build_configured_external_source_report(
        values,
        project_root=project_root,
        dry_run=False,
        skip_init_schema=True,
        command_runner=_command_preview_runner,
    )

    dry_items = _items_by_kind(dry_run_report)
    write_items = _items_by_kind(write_report)
    source_items = [
        _build_source_item(source, dry_items.get(str(source.get("kind"))), write_items.get(str(source.get("kind"))))
        for source in env_audit.get("sources", [])
    ]
    production_blockers = [
        blocker
        for item in source_items
        if item["status"] != READY
        for blocker in item["production_blockers"]
    ]
    status = _aggregate_status([str(item["status"]) for item in source_items], env_audit.get("status"))
    return {
        "schema_version": "fill_first_external_source_onboarding_v1",
        "status": status,
        "env_files": env_audit.get("env_files", []),
        "source_count": len(source_items),
        "ready_count": sum(1 for item in source_items if item["status"] == READY),
        "review_count": sum(1 for item in source_items if item["status"] == REVIEW),
        "fail_count": sum(1 for item in source_items if item["status"] == FAIL),
        "production_blockers": production_blockers,
        "sources": source_items,
        "next_actions": _next_actions(source_items, env_files=env_files),
        "env_audit": env_audit,
        "configured_import_dry_run": dry_run_report,
        "configured_import_write_preview": write_report,
    }


def external_source_onboarding_plan_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Fill-first External Source Onboarding: {report.get('status')}",
        "",
        f"- ready: {report.get('ready_count', 0)}",
        f"- review: {report.get('review_count', 0)}",
        f"- fail: {report.get('fail_count', 0)}",
        "",
        "| Source | Status | Mode | Required | Missing | Completion criteria | Dry-run command | Write command |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in report.get("sources", []):
        criteria = "<br>".join(str(value) for value in item.get("completion_criteria", []))
        dry_command = " ".join(str(part) for part in item.get("dry_run_command", []))
        write_command = " ".join(str(part) for part in item.get("write_command", []))
        lines.append(
            "| {source} | {status} | {mode} | {required} | {missing} | {criteria} | `{dry}` | `{write}` |".format(
                source=item.get("label") or item.get("kind") or "",
                status=item.get("status") or "",
                mode=item.get("mode") or "",
                required=", ".join(str(key) for key in item.get("required_keys", [])) or "none",
                missing=", ".join(str(key) for key in item.get("missing_keys", [])) or "none",
                criteria=criteria.replace("|", "\\|"),
                dry=dry_command.replace("|", "\\|"),
                write=write_command.replace("|", "\\|"),
            )
        )
    blockers = [str(value) for value in report.get("production_blockers", [])]
    if blockers:
        lines.extend(["", "## Production Blockers"])
        lines.extend(f"- {blocker}" for blocker in blockers)
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions", []))
    return "\n".join(lines)


def _load_values(env_files: Sequence[Path | str], *, env: Mapping[str, str] | None) -> dict[str, str]:
    if env_files:
        return load_external_source_env_files(env_files, base_env=env or {})
    return dict(env or {})


def _command_preview_runner(command: Sequence[str]) -> dict[str, Any]:
    return {"returncode": 0, "output_tail": "preview only; command not executed"}


def _items_by_kind(report: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {str(item.get("kind")): item for item in report.get("items", []) if isinstance(item, Mapping)}


def _build_source_item(
    source: Mapping[str, Any],
    dry_run_item: Mapping[str, Any] | None,
    write_item: Mapping[str, Any] | None,
) -> dict[str, Any]:
    kind = str(source.get("kind") or "")
    configured = bool((dry_run_item or {}).get("configured"))
    status = str(source.get("status") or REVIEW)
    issues = [str(issue) for issue in source.get("issues", [])]
    missing_keys = [str(key) for key in source.get("missing_keys", [])]
    production_blockers = _production_blockers(kind, configured=configured, status=status, issues=issues)
    return {
        "kind": kind,
        "label": SOURCE_LABELS.get(kind, kind),
        "status": READY if status == READY and configured else REVIEW if status != FAIL else FAIL,
        "mode": source.get("mode") or "unconfigured",
        "configured": configured,
        "required_keys": list(source.get("required_keys", [])),
        "missing_keys": missing_keys,
        "health_tracked": bool(source.get("health_tracked")),
        "issues": issues,
        "next_action": source.get("next_action") or "",
        "dry_run_command": list((dry_run_item or {}).get("command", [])) if configured else [],
        "write_command": list((write_item or {}).get("command", [])) if configured else [],
        "completion_criteria": _completion_criteria(kind, source=source, configured=configured),
        "production_blockers": production_blockers,
    }


def _completion_criteria(kind: str, *, source: Mapping[str, Any], configured: bool) -> list[str]:
    label = SOURCE_LABELS.get(kind, kind)
    criteria = [
        f"{label} env audit status is ready",
        "configured import dry-run command is present",
        "write command has been reviewed and run with --write only after dry-run passes",
    ]
    if source.get("health_tracked"):
        criteria.append("external_source_import_state has a state_key for freshness checks")
    else:
        criteria.append("set a state_key before production freshness checks")
    if kind == "real_order_state_events":
        criteria.append("current backtest run has 100% order-state and calibration coverage before promotion")
    elif kind == "real_cost_events":
        criteria.append("fee/rebate/gas/settlement/capital-cost events can reconcile against simulated ledger")
    elif kind == "platform_incidents":
        criteria.append("incident windows overlap no-fill/latency diagnostics and are included in run artifact audit")
    elif kind == "external_signal_events":
        criteria.append("strategy signal events have observed time/block, source, latency and payload hash before run audit")
    if not configured:
        criteria.append("source is still unconfigured; configure file or URL input first")
    return criteria


def _production_blockers(kind: str, *, configured: bool, status: str, issues: Sequence[str]) -> list[str]:
    if status == READY and configured:
        return []
    label = SOURCE_LABELS.get(kind, kind)
    blockers = [f"{label}: source is not production-ready"]
    blockers.extend(f"{label}: {issue}" for issue in issues)
    if not configured:
        blockers.append(f"{label}: no configured import command")
    return blockers


def _next_actions(source_items: Sequence[Mapping[str, Any]], *, env_files: Sequence[Path | str]) -> list[str]:
    env_part = " ".join(f"--env-file {Path(path).expanduser()}" for path in env_files)
    audit_cmd = "conda run -n polyBacktest python scripts/audit_external_source_env.py"
    import_cmd = "conda run -n polyBacktest python scripts/run_configured_external_source_imports.py"
    health_cmd = "conda run -n polyBacktest python scripts/check_external_source_import_health.py"
    if env_part:
        audit_cmd = f"{audit_cmd} {env_part}"
        import_cmd = f"{import_cmd} {env_part}"
    actions = [
        "Fill or remove placeholder values in the private env-file; do not commit secrets.",
        f"Run env audit: {audit_cmd} --strict-review",
        f"Run configured import dry-run: {import_cmd}",
        f"After dry-run review, run configured import write: {import_cmd} --write",
        f"Check import freshness: {health_cmd}",
        "Before paper/live promotion, run scripts/check_external_source_run_coverage.py for the target run.",
    ]
    for item in source_items:
        if item.get("status") != READY:
            actions.append(f"{item.get('label')}: {item.get('next_action')}")
    return actions


def _aggregate_status(statuses: Sequence[str], env_status: Any) -> str:
    if env_status == FAIL or FAIL in statuses:
        return FAIL
    if REVIEW in statuses:
        return REVIEW
    return READY
