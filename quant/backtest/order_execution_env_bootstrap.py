"""Bootstrap a local paper-only ORDER_EXECUTION env for fill-first dry-runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from quant.backtest.order_execution_safety import build_order_execution_env_audit


READY = "ready"
REVIEW = "review"


def bootstrap_order_execution_env(
    target_dir: Path,
    *,
    env_file: Path | None = None,
    submit_url: str = "http://127.0.0.1:9/fill-first-paper-submit",
    cancel_url: str = "",
    source: str = "paper-dry-run-local",
    overwrite: bool = False,
) -> dict[str, Any]:
    """Create a local paper-only order execution env-file.

    The generated env is intended for request-template and safety validation.
    It does not enable network execution by itself because the adapter CLI
    remains in dry-run mode by default.
    """

    output_dir = Path(target_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_env = Path(env_file).expanduser() if env_file else output_dir / "order-execution-paper.env"
    env_exists = output_env.exists()
    env_written = False
    if env_exists and not overwrite:
        status = REVIEW
        reason = f"env file already exists: {output_env}"
    else:
        output_env.parent.mkdir(parents=True, exist_ok=True)
        output_env.write_text(_env_text(submit_url=submit_url, cancel_url=cancel_url, source=source), encoding="utf-8")
        env_written = True
        status = READY
        reason = "paper dry-run order execution env created"
    env_values = _env_values(submit_url=submit_url, cancel_url=cancel_url, source=source)
    audit = build_order_execution_env_audit(env_values)
    if status == READY and audit.get("status") != READY:
        status = REVIEW
        reason = f"env created but audit status is {audit.get('status')}"
    commands = _commands(output_env)
    return {
        "schema_version": "fill_first_order_execution_env_bootstrap_v1",
        "status": status,
        "reason": reason,
        "target_dir": str(output_dir),
        "env_file": str(output_env),
        "env_written": env_written,
        "contains_live_execution": False,
        "contains_secret": False,
        "audit_status": audit.get("status"),
        "audit": audit,
        "commands": commands,
        "next_actions": _next_actions(commands),
    }


def order_execution_env_bootstrap_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Fill-first Order Execution Env Bootstrap: {report.get('status')}",
        "",
        f"- reason: {report.get('reason')}",
        f"- env_file: `{report.get('env_file')}`",
        f"- target_dir: `{report.get('target_dir')}`",
        f"- env_written: {report.get('env_written')}",
        f"- contains_live_execution: {report.get('contains_live_execution')}",
        f"- contains_secret: {report.get('contains_secret')}",
        f"- audit_status: {report.get('audit_status')}",
        "",
        "## Commands",
    ]
    commands = report.get("commands") if isinstance(report.get("commands"), Mapping) else {}
    for name, command in commands.items():
        lines.append(f"- {name}: `{command}`")
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions", []))
    return "\n".join(lines)


def _env_text(*, submit_url: str, cancel_url: str, source: str) -> str:
    values = _env_values(submit_url=submit_url, cancel_url=cancel_url, source=source)
    lines = [
        "# Local paper-only order execution env.",
        "# This file is for dry-run request-template validation. It does not enable live execution.",
        "# Do not add live credentials here. Use a separate private env-file for real paper/live adapters.",
    ]
    lines.extend(f"{key}={value}" for key, value in values.items())
    lines.append("")
    return "\n".join(lines)


def _env_values(*, submit_url: str, cancel_url: str, source: str) -> dict[str, str]:
    return {
        "ORDER_EXECUTION_TARGET_MODE": "paper",
        "ORDER_EXECUTION_SUBMIT_URL": submit_url,
        "ORDER_EXECUTION_CANCEL_URL": cancel_url,
        "ORDER_EXECUTION_AUTH_HEADER": "",
        "ORDER_EXECUTION_SOURCE": source,
        "ORDER_EXECUTION_TIMEOUT": "15",
        "ORDER_EXECUTION_LIMIT": "50",
        "ORDER_EXECUTION_LIVE_CONFIRM": "",
    }


def _commands(env_file: Path) -> dict[str, str]:
    env = str(env_file)
    return {
        "audit": f"set -a && . {env} && set +a && conda run -n polyBacktest python scripts/audit_order_execution_env.py --strict-review",
        "dry_run_adapter": f"set -a && . {env} && set +a && conda run -n polyBacktest python scripts/run_order_execution_adapter.py --format markdown",
        "quality_gate": f"set -a && . {env} && set +a && conda run -n polyBacktest python scripts/run_fill_first_quality_gate.py --stage-check --format markdown",
    }


def _next_actions(commands: Mapping[str, str]) -> list[str]:
    return [
        "Run the audit command and confirm the env is paper-only.",
        "Run the adapter in dry-run mode to inspect request templates; this must not call an external order API.",
        "Use a separate private env-file with real paper/live credentials only after promotion and paper/live evidence gates pass.",
        f"Audit: {commands['audit']}",
        f"Dry-run adapter: {commands['dry_run_adapter']}",
    ]
