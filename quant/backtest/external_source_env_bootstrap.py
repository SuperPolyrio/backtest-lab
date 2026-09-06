"""Bootstrap a local file-mode env for fill-first external evidence sources."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from quant.backtest.external_source_env_audit import build_external_source_env_audit
from quant.backtest.external_source_onboarding import build_external_source_onboarding_plan


READY = "ready"
REVIEW = "review"

SOURCE_FILES = {
    "ORDER_STATE_INPUT": "real_order_state_events.jsonl",
    "COST_EVENTS_INPUT": "real_cost_events.jsonl",
    "PLATFORM_INCIDENTS_INPUT": "platform_incidents.jsonl",
    "EXTERNAL_SIGNAL_INPUT": "external_signal_events.jsonl",
}


def bootstrap_external_source_env(
    target_dir: Path,
    *,
    env_file: Path | None = None,
    project_root: Path | None = None,
    conda_exe: str = "/opt/anaconda3/bin/conda",
    conda_env: str = "polyBacktest",
    overwrite: bool = False,
    create_files: bool = True,
) -> dict[str, Any]:
    """Create local JSONL placeholders and an env-file for file-mode imports."""

    root = Path(project_root or Path(__file__).resolve().parents[2]).expanduser()
    data_dir = Path(target_dir).expanduser()
    output_env = Path(env_file).expanduser() if env_file else data_dir / "fill-first-external-sources.env"
    created_files: list[str] = []
    existing_files: list[str] = []
    skipped_files: list[str] = []
    data_dir.mkdir(parents=True, exist_ok=True)
    if create_files:
        for filename in SOURCE_FILES.values():
            path = data_dir / filename
            if path.exists():
                existing_files.append(str(path))
            else:
                path.touch()
                created_files.append(str(path))
    else:
        skipped_files = [str(data_dir / filename) for filename in SOURCE_FILES.values()]
    env_exists = output_env.exists()
    env_written = False
    if env_exists and not overwrite:
        status = REVIEW
        reason = f"env file already exists: {output_env}"
    else:
        output_env.parent.mkdir(parents=True, exist_ok=True)
        output_env.write_text(
            _env_text(data_dir, project_root=root, conda_exe=conda_exe, conda_env=conda_env),
            encoding="utf-8",
        )
        env_written = True
        status = READY
        reason = "local file-mode env created"
    audit = build_external_source_env_audit([output_env], env={}, project_root=root) if output_env.exists() else {}
    onboarding = build_external_source_onboarding_plan([output_env], env={}, project_root=root) if output_env.exists() else {}
    commands = _commands(output_env)
    if status == READY and audit.get("status") != READY:
        status = REVIEW
        reason = f"env created but audit status is {audit.get('status')}"
    return {
        "schema_version": "fill_first_external_source_env_bootstrap_v1",
        "status": status,
        "reason": reason,
        "target_dir": str(data_dir),
        "env_file": str(output_env),
        "env_written": env_written,
        "created_files": created_files,
        "existing_files": existing_files,
        "skipped_files": skipped_files,
        "contains_real_evidence": False,
        "audit_status": audit.get("status"),
        "onboarding_status": onboarding.get("status"),
        "audit": audit,
        "onboarding": onboarding,
        "commands": commands,
        "next_actions": _next_actions(commands),
    }


def external_source_env_bootstrap_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Fill-first External Source Env Bootstrap: {report.get('status')}",
        "",
        f"- reason: {report.get('reason')}",
        f"- env_file: `{report.get('env_file')}`",
        f"- target_dir: `{report.get('target_dir')}`",
        f"- env_written: {report.get('env_written')}",
        f"- contains_real_evidence: {report.get('contains_real_evidence')}",
        f"- audit_status: {report.get('audit_status')}",
        f"- onboarding_status: {report.get('onboarding_status')}",
        "",
        "## Files",
    ]
    for path in report.get("created_files", []):
        lines.append(f"- created `{path}`")
    for path in report.get("existing_files", []):
        lines.append(f"- existing `{path}`")
    for path in report.get("skipped_files", []):
        lines.append(f"- not-created `{path}`")
    lines.extend(["", "## Commands"])
    commands = report.get("commands") if isinstance(report.get("commands"), Mapping) else {}
    for name, command in commands.items():
        lines.append(f"- {name}: `{command}`")
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions", []))
    return "\n".join(lines)


def _env_text(data_dir: Path, *, project_root: Path, conda_exe: str, conda_env: str) -> str:
    values = {
        "PROJECT_ROOT": str(project_root),
        "CONDA_EXE": conda_exe,
        "CONDA_ENV": conda_env,
        "ORDER_STATE_INPUT": str(data_dir / SOURCE_FILES["ORDER_STATE_INPUT"]),
        "ORDER_STATE_API_URL": "",
        "ORDER_STATE_AUTH_HEADER": "",
        "ORDER_STATE_SOURCE": "order-state-file",
        "ORDER_STATE_RUN_ID": "",
        "ORDER_STATE_KEY": "order-state-local-file",
        "ORDER_STATE_SINCE_PARAM": "updated_after",
        "ORDER_STATE_CURSOR_PARAM": "cursor",
        "ORDER_STATE_INITIAL_SINCE": "",
        "ORDER_STATE_POLL_SECONDS": "0",
        "ORDER_STATE_MAX_POLLS": "1",
        "COST_EVENTS_INPUT": str(data_dir / SOURCE_FILES["COST_EVENTS_INPUT"]),
        "COST_EVENTS_URL": "",
        "COST_EVENTS_AUTH_HEADER": "",
        "COST_EVENTS_SOURCE": "wallet-ledger-file",
        "COST_EVENTS_RUN_ID": "",
        "COST_EVENTS_STATE_KEY": "real-cost-events-local-file",
        "COST_EVENTS_BUILD_CALIBRATION": "0",
        "PLATFORM_INCIDENTS_INPUT": str(data_dir / SOURCE_FILES["PLATFORM_INCIDENTS_INPUT"]),
        "PLATFORM_INCIDENTS_URL": "",
        "PLATFORM_INCIDENTS_AUTH_HEADER": "",
        "PLATFORM_INCIDENTS_SOURCE": "ops-notes-file",
        "PLATFORM_INCIDENTS_STATE_KEY": "platform-incidents-local-file",
        "EXTERNAL_SIGNAL_INPUT": str(data_dir / SOURCE_FILES["EXTERNAL_SIGNAL_INPUT"]),
        "EXTERNAL_SIGNAL_URL": "",
        "EXTERNAL_SIGNAL_AUTH_HEADER": "",
        "EXTERNAL_SIGNAL_SOURCE": "external-signal-file",
        "EXTERNAL_SIGNAL_RUN_ID": "",
        "EXTERNAL_SIGNAL_STATE_KEY": "external-signals-local-file",
        "EXTERNAL_SOURCE_HEALTH_MAX_STALE_SECONDS": "86400",
    }
    lines = [
        "# Local file-mode fill-first external source env.",
        "# Fill the JSONL files with real exporter data before treating this as production evidence.",
        "# Do not commit private env files or copied live evidence.",
    ]
    lines.extend(f"{key}={value}" for key, value in values.items())
    lines.append("")
    return "\n".join(lines)


def _commands(env_file: Path) -> dict[str, str]:
    env = str(env_file)
    return {
        "audit": f"conda run -n polyBacktest python scripts/audit_external_source_env.py --env-file {env} --strict-review",
        "onboarding": f"conda run -n polyBacktest python scripts/plan_external_source_onboarding.py --env-file {env} --strict-review",
        "dry_run_import": f"conda run -n polyBacktest python scripts/run_configured_external_source_imports.py --env-file {env}",
        "write_import": f"conda run -n polyBacktest python scripts/run_configured_external_source_imports.py --env-file {env} --write",
        "quality_gate": f"conda run -n polyBacktest python scripts/run_fill_first_quality_gate.py --external-env-file {env} --stage-check --format markdown",
    }


def _next_actions(commands: Mapping[str, str]) -> list[str]:
    return [
        "Replace the empty JSONL placeholders with real order-state, cost, incident and signal exports, or switch selected sources to private API URLs.",
        f"Run env audit: {commands['audit']}",
        f"Preview imports: {commands['dry_run_import']}",
        "Only after reviewing real data, run the write import command and then check external source freshness.",
        "Run run-level coverage on the target backtest before paper/live promotion.",
    ]
