"""Environment-driven import plan for fill-first external evidence sources."""

from __future__ import annotations

from dataclasses import dataclass
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
FAIL = "fail"


@dataclass(frozen=True)
class ConfiguredExternalSource:
    kind: str
    script: str
    command: tuple[str, ...]
    display_command: tuple[str, ...]
    endpoint: str | None
    source: str | None
    state_key: str | None
    configured: bool
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "script": self.script,
            "command": list(self.display_command),
            "endpoint": self.endpoint,
            "source": self.source,
            "state_key": self.state_key,
            "configured": self.configured,
            "reason": self.reason,
        }


def build_configured_external_source_plan(
    env: Mapping[str, str] | None = None,
    *,
    project_root: Path | None = None,
    python_executable: str | None = None,
    skip_init_schema: bool = False,
) -> list[ConfiguredExternalSource]:
    values = env or os.environ
    root = Path(project_root or Path(__file__).resolve().parents[2])
    py = python_executable or sys.executable
    return [
        _order_state_source(values, root, py, skip_init_schema=skip_init_schema),
        _cost_source(values, root, py, skip_init_schema=skip_init_schema),
        _incident_source(values, root, py, skip_init_schema=skip_init_schema),
        _external_signal_source(values, root, py, skip_init_schema=skip_init_schema),
    ]


def build_configured_external_source_report(
    env: Mapping[str, str] | None = None,
    *,
    project_root: Path | None = None,
    dry_run: bool = True,
    skip_init_schema: bool = False,
    command_timeout_seconds: int = 300,
    command_runner: Any | None = None,
) -> dict[str, Any]:
    plan = build_configured_external_source_plan(
        env,
        project_root=project_root,
        skip_init_schema=skip_init_schema,
    )
    items: list[dict[str, Any]] = []
    for source in plan:
        item = source.as_dict()
        if not source.configured:
            item["status"] = REVIEW
            item["returncode"] = None
            items.append(item)
            continue
        if dry_run:
            item["status"] = READY
            item["returncode"] = 0
            item["dry_run"] = True
            items.append(item)
            continue
        result = _run_source(source, timeout=command_timeout_seconds, command_runner=command_runner)
        item.update(result)
        returncode = result.get("returncode")
        item["status"] = READY if returncode is not None and int(returncode) == 0 else FAIL
        items.append(item)
    configured_count = sum(1 for item in items if item["configured"])
    fail_count = sum(1 for item in items if item["status"] == FAIL)
    status = FAIL if fail_count else READY if configured_count else REVIEW
    return {
        "status": status,
        "dry_run": bool(dry_run),
        "configured_count": configured_count,
        "skipped_count": len(items) - configured_count,
        "items": items,
    }


def load_external_source_env_files(
    paths: Sequence[Path | str],
    *,
    base_env: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Load dotenv/systemd-style key-value files for external source imports."""
    env = dict(base_env or os.environ)
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        values = _parse_env_file(path)
        env.update(values)
    return env


def configured_external_source_report_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"status: {report.get('status')}",
        f"dry_run: {report.get('dry_run')}",
        f"configured: {report.get('configured_count', 0)}",
        f"skipped: {report.get('skipped_count', 0)}",
        "",
        "| kind | status | configured | endpoint | source | state_key | reason | command |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in report.get("items", []):
        command = " ".join(str(part) for part in item.get("command", []))
        lines.append(
            "| {kind} | {status} | {configured} | `{endpoint}` | {source} | {state_key} | {reason} | `{command}` |".format(
                kind=item.get("kind") or "",
                status=item.get("status") or "",
                configured=item.get("configured"),
                endpoint=item.get("endpoint") or "",
                source=item.get("source") or "",
                state_key=item.get("state_key") or "",
                reason=str(item.get("reason") or "").replace("|", "\\|"),
                command=command.replace("|", "\\|"),
            )
        )
    return "\n".join(lines)


def _order_state_source(
    env: Mapping[str, str],
    root: Path,
    py: str,
    *,
    skip_init_schema: bool,
) -> ConfiguredExternalSource:
    input_path = _clean(env.get("ORDER_STATE_INPUT"))
    url = _clean(env.get("ORDER_STATE_API_URL"))
    source = _clean(env.get("ORDER_STATE_SOURCE")) or "order-api"
    run_id = _clean(env.get("ORDER_STATE_RUN_ID"))
    script = "scripts/import_real_order_state_events.py" if input_path else "scripts/collect_real_order_state_events.py"
    if input_path:
        command = [py, str(root / script), "--input", input_path, "--source", source]
        if run_id:
            command.extend(["--run-id", run_id])
        state_key = _clean(env.get("ORDER_STATE_KEY"))
        if state_key:
            command.extend(["--state-key", state_key])
        if skip_init_schema:
            command.append("--skip-init-schema")
        return _source("real_order_state_events", script, command, input_path, source, state_key, True, "ORDER_STATE_INPUT")
    if url:
        command = [py, str(root / script), "--url", url, "--source", source]
        header = _clean(env.get("ORDER_STATE_AUTH_HEADER"))
        if header:
            command.extend(["--header", header])
        if run_id:
            command.extend(["--run-id", run_id])
        for env_key, flag in (
            ("ORDER_STATE_KEY", "--state-key"),
            ("ORDER_STATE_SINCE_PARAM", "--since-param"),
            ("ORDER_STATE_CURSOR_PARAM", "--cursor-param"),
            ("ORDER_STATE_INITIAL_SINCE", "--initial-since"),
            ("ORDER_STATE_POLL_SECONDS", "--poll-seconds"),
            ("ORDER_STATE_MAX_POLLS", "--max-polls"),
        ):
            value = _clean(env.get(env_key))
            if value:
                command.extend([flag, value])
        if skip_init_schema:
            command.append("--skip-init-schema")
        return _source("real_order_state_events", script, command, url, source, _clean(env.get("ORDER_STATE_KEY")), True, "ORDER_STATE_API_URL")
    return _source("real_order_state_events", script, [py, str(root / script)], None, source, _clean(env.get("ORDER_STATE_KEY")), False, "set ORDER_STATE_INPUT or ORDER_STATE_API_URL")


def _cost_source(
    env: Mapping[str, str],
    root: Path,
    py: str,
    *,
    skip_init_schema: bool,
) -> ConfiguredExternalSource:
    input_path = _clean(env.get("COST_EVENTS_INPUT"))
    url = _clean(env.get("COST_EVENTS_URL"))
    source = _clean(env.get("COST_EVENTS_SOURCE")) or "wallet-ledger"
    endpoint = url or input_path
    script = "scripts/import_real_backtest_cost_events.py"
    command = [py, str(root / script)]
    if url:
        command.extend(["--url", url])
    elif input_path:
        command.extend(["--input", input_path])
    else:
        return _source("real_cost_events", script, command, None, source, _clean(env.get("COST_EVENTS_STATE_KEY")), False, "set COST_EVENTS_INPUT or COST_EVENTS_URL")
    header = _clean(env.get("COST_EVENTS_AUTH_HEADER"))
    if header:
        command.extend(["--header", header])
    command.extend(["--source", source])
    for env_key, flag in (("COST_EVENTS_RUN_ID", "--run-id"), ("COST_EVENTS_STATE_KEY", "--state-key")):
        value = _clean(env.get(env_key))
        if value:
            command.extend([flag, value])
    if _truthy(env.get("COST_EVENTS_BUILD_CALIBRATION")):
        command.append("--build-calibration")
    if skip_init_schema:
        command.append("--skip-init-schema")
    return _source("real_cost_events", script, command, endpoint, source, _clean(env.get("COST_EVENTS_STATE_KEY")), True, "COST_EVENTS_INPUT/COST_EVENTS_URL")


def _incident_source(
    env: Mapping[str, str],
    root: Path,
    py: str,
    *,
    skip_init_schema: bool,
) -> ConfiguredExternalSource:
    input_path = _clean(env.get("PLATFORM_INCIDENTS_INPUT"))
    url = _clean(env.get("PLATFORM_INCIDENTS_URL"))
    source = _clean(env.get("PLATFORM_INCIDENTS_SOURCE")) or "ops-notes"
    endpoint = url or input_path
    script = "scripts/import_platform_incidents.py"
    command = [py, str(root / script)]
    if url:
        command.extend(["--url", url])
    elif input_path:
        command.extend(["--input", input_path])
    else:
        return _source("platform_incidents", script, command, None, source, _clean(env.get("PLATFORM_INCIDENTS_STATE_KEY")), False, "set PLATFORM_INCIDENTS_INPUT or PLATFORM_INCIDENTS_URL")
    header = _clean(env.get("PLATFORM_INCIDENTS_AUTH_HEADER"))
    if header:
        command.extend(["--header", header])
    command.extend(["--source", source])
    state_key = _clean(env.get("PLATFORM_INCIDENTS_STATE_KEY"))
    if state_key:
        command.extend(["--state-key", state_key])
    if skip_init_schema:
        command.append("--skip-init-schema")
    return _source("platform_incidents", script, command, endpoint, source, state_key, True, "PLATFORM_INCIDENTS_INPUT/PLATFORM_INCIDENTS_URL")


def _external_signal_source(
    env: Mapping[str, str],
    root: Path,
    py: str,
    *,
    skip_init_schema: bool,
) -> ConfiguredExternalSource:
    input_path = _clean(env.get("EXTERNAL_SIGNAL_INPUT"))
    url = _clean(env.get("EXTERNAL_SIGNAL_URL"))
    source = _clean(env.get("EXTERNAL_SIGNAL_SOURCE")) or "external-signal"
    endpoint = url or input_path
    script = "scripts/import_external_signal_events.py"
    command = [py, str(root / script)]
    if url:
        command.extend(["--url", url])
    elif input_path:
        command.extend(["--input", input_path])
    else:
        return _source(
            "external_signal_events",
            script,
            command,
            None,
            source,
            _clean(env.get("EXTERNAL_SIGNAL_STATE_KEY")),
            False,
            "set EXTERNAL_SIGNAL_INPUT or EXTERNAL_SIGNAL_URL",
        )
    header = _clean(env.get("EXTERNAL_SIGNAL_AUTH_HEADER"))
    if header:
        command.extend(["--header", header])
    command.extend(["--source", source])
    for env_key, flag in (("EXTERNAL_SIGNAL_RUN_ID", "--run-id"), ("EXTERNAL_SIGNAL_STATE_KEY", "--state-key")):
        value = _clean(env.get(env_key))
        if value:
            command.extend([flag, value])
    if skip_init_schema:
        command.append("--skip-init-schema")
    return _source("external_signal_events", script, command, endpoint, source, _clean(env.get("EXTERNAL_SIGNAL_STATE_KEY")), True, "EXTERNAL_SIGNAL_INPUT/EXTERNAL_SIGNAL_URL")


def _source(
    kind: str,
    script: str,
    command: Sequence[str],
    endpoint: str | None,
    source: str | None,
    state_key: str | None,
    configured: bool,
    reason: str,
) -> ConfiguredExternalSource:
    display = tuple(_masked_command(command))
    return ConfiguredExternalSource(
        kind=kind,
        script=script,
        command=tuple(command),
        display_command=display,
        endpoint=endpoint,
        source=source,
        state_key=state_key,
        configured=configured,
        reason=reason,
    )


def _run_source(source: ConfiguredExternalSource, *, timeout: int, command_runner: Any | None) -> dict[str, Any]:
    if command_runner is not None:
        return dict(command_runner(source.command))
    completed = subprocess.run(
        list(source.command),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
        timeout=timeout,
    )
    output = completed.stdout or ""
    return {
        "returncode": completed.returncode,
        "output_tail": output[-4000:],
    }


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(f"env file not found: {path}")
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            raise ValueError(f"invalid env file line {path}:{line_number}: expected KEY=VALUE")
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"invalid env file line {path}:{line_number}: empty key")
        values[key] = _strip_env_value(value)
    return values


def _strip_env_value(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _masked_command(command: Sequence[str]) -> list[str]:
    masked: list[str] = []
    mask_next = False
    for part in command:
        if mask_next:
            masked.append(_mask_secret_arg(part))
            mask_next = False
            continue
        masked.append(str(part))
        if part == "--header":
            mask_next = True
    return masked


def _mask_secret_arg(value: str) -> str:
    key = value.split("=", 1)[0] if "=" in value else "header"
    lowered = key.lower()
    if "auth" in lowered or "key" in lowered or "token" in lowered:
        return f"{key}=***"
    return value


def _clean(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "replace-with" in lowered or lowered in {"none", "null", "todo"}:
        return ""
    return text


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}
