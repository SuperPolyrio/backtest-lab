"""Audit fill-first external source env-file configuration before import."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from quant.backtest.configured_external_sources import (
    FAIL,
    READY,
    REVIEW,
    build_configured_external_source_report,
    load_external_source_env_files,
)


PLACEHOLDER_MARKERS = (
    "replace-with",
    "replace_with",
    "todo",
    "<",
    ">",
    "example",
)

SECRET_KEY_PARTS = ("auth", "authorization", "token", "secret", "password")


@dataclass(frozen=True)
class ExternalSourceEnvSpec:
    kind: str
    input_key: str
    url_key: str
    auth_key: str | None
    source_key: str
    state_key: str | None
    run_id_key: str | None = None


SPECS: tuple[ExternalSourceEnvSpec, ...] = (
    ExternalSourceEnvSpec(
        kind="real_order_state_events",
        input_key="ORDER_STATE_INPUT",
        url_key="ORDER_STATE_API_URL",
        auth_key="ORDER_STATE_AUTH_HEADER",
        source_key="ORDER_STATE_SOURCE",
        state_key="ORDER_STATE_KEY",
        run_id_key="ORDER_STATE_RUN_ID",
    ),
    ExternalSourceEnvSpec(
        kind="real_cost_events",
        input_key="COST_EVENTS_INPUT",
        url_key="COST_EVENTS_URL",
        auth_key="COST_EVENTS_AUTH_HEADER",
        source_key="COST_EVENTS_SOURCE",
        state_key="COST_EVENTS_STATE_KEY",
        run_id_key="COST_EVENTS_RUN_ID",
    ),
    ExternalSourceEnvSpec(
        kind="platform_incidents",
        input_key="PLATFORM_INCIDENTS_INPUT",
        url_key="PLATFORM_INCIDENTS_URL",
        auth_key="PLATFORM_INCIDENTS_AUTH_HEADER",
        source_key="PLATFORM_INCIDENTS_SOURCE",
        state_key="PLATFORM_INCIDENTS_STATE_KEY",
    ),
    ExternalSourceEnvSpec(
        kind="external_signal_events",
        input_key="EXTERNAL_SIGNAL_INPUT",
        url_key="EXTERNAL_SIGNAL_URL",
        auth_key="EXTERNAL_SIGNAL_AUTH_HEADER",
        source_key="EXTERNAL_SIGNAL_SOURCE",
        state_key="EXTERNAL_SIGNAL_STATE_KEY",
        run_id_key="EXTERNAL_SIGNAL_RUN_ID",
    ),
)


def build_external_source_env_audit(
    env_files: Sequence[Path | str] = (),
    *,
    env: Mapping[str, str] | None = None,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Return a safe audit report for configured fill-first external sources."""

    try:
        values = _load_values(env_files, env=env)
    except Exception as exc:
        return {
            "status": FAIL,
            "env_files": [str(Path(path).expanduser()) for path in env_files],
            "loaded_env_file_count": 0,
            "source_count": len(SPECS),
            "ready_count": 0,
            "review_count": 0,
            "fail_count": 1,
            "issues": [f"env file load failed: {exc}"],
            "sources": [],
            "configured_imports": {"status": FAIL, "items": []},
        }

    source_reports = [_audit_spec(spec, values) for spec in SPECS]
    configured_report = build_configured_external_source_report(
        values,
        project_root=project_root,
        dry_run=True,
        skip_init_schema=True,
    )
    statuses = [str(item["status"]) for item in source_reports]
    status = _aggregate_status(statuses)
    issues = [issue for item in source_reports for issue in item["issues"]]
    return {
        "status": status,
        "env_files": [str(Path(path).expanduser()) for path in env_files],
        "loaded_env_file_count": len(env_files),
        "source_count": len(source_reports),
        "ready_count": statuses.count(READY),
        "review_count": statuses.count(REVIEW),
        "fail_count": statuses.count(FAIL),
        "issues": issues,
        "sources": source_reports,
        "configured_imports": configured_report,
    }


def external_source_env_audit_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# External Source Env Audit: {report.get('status')}",
        "",
        f"- env_files: {report.get('loaded_env_file_count', 0)}",
        f"- ready: {report.get('ready_count', 0)}",
        f"- review: {report.get('review_count', 0)}",
        f"- fail: {report.get('fail_count', 0)}",
        "",
        "| Source | Status | Mode | Input | URL | Auth | State | Health | Missing | Issues | Next action |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in report.get("sources", []):
        issues = "; ".join(str(issue) for issue in item.get("issues", [])) or "ok"
        missing = ", ".join(str(key) for key in item.get("missing_keys", [])) or "none"
        health = "tracked" if item.get("health_tracked") else "not tracked"
        lines.append(
            "| {kind} | {status} | {mode} | `{input}` | `{url}` | {auth} | {state} | {health} | {missing} | {issues} | {next_action} |".format(
                kind=item.get("kind") or "",
                status=item.get("status") or "",
                mode=item.get("mode") or "",
                input=item.get("input") or "",
                url=item.get("url") or "",
                auth=item.get("auth") or "",
                state=item.get("state_key") or "",
                health=health,
                missing=missing.replace("|", "\\|"),
                issues=issues.replace("|", "\\|"),
                next_action=str(item.get("next_action") or "").replace("|", "\\|"),
            )
        )
    configured = report.get("configured_imports")
    if isinstance(configured, Mapping):
        lines.extend(
            [
                "",
                "## Configured Import Plan",
                "",
                f"- status: {configured.get('status')}",
                f"- configured: {configured.get('configured_count', 0)}",
                f"- skipped: {configured.get('skipped_count', 0)}",
            ]
        )
    return "\n".join(lines)


def _load_values(env_files: Sequence[Path | str], *, env: Mapping[str, str] | None) -> dict[str, str]:
    base = dict(os.environ if env is None else env)
    if not env_files:
        return base
    return load_external_source_env_files(env_files, base_env=base)


def _audit_spec(spec: ExternalSourceEnvSpec, env: Mapping[str, str]) -> dict[str, Any]:
    input_value = _clean(env.get(spec.input_key))
    url_value = _clean(env.get(spec.url_key))
    auth_value = _clean(env.get(spec.auth_key)) if spec.auth_key else ""
    state_key = _clean(env.get(spec.state_key)) if spec.state_key else ""
    mode = _source_mode(input_value=input_value, url_value=url_value)
    relevant_keys = [
        spec.input_key,
        spec.url_key,
        spec.source_key,
        *(key for key in (spec.auth_key, spec.state_key, spec.run_id_key) if key),
    ]
    issues: list[str] = []
    missing_keys: list[str] = []
    placeholder_keys = [key for key in relevant_keys if _has_placeholder(env.get(key))]
    if placeholder_keys:
        issues.append("placeholder values: " + ", ".join(placeholder_keys))
    if not input_value and not url_value:
        issues.append(f"set {spec.input_key} or {spec.url_key}")
        missing_keys.append(f"{spec.input_key} or {spec.url_key}")
    if input_value and url_value:
        issues.append(f"both {spec.input_key} and {spec.url_key} are set; input file will be preferred only for order-state")
    if input_value and not Path(input_value).expanduser().exists():
        issues.append(f"input file does not exist: {spec.input_key}")
        missing_keys.append(spec.input_key)
    if url_value and spec.auth_key and not auth_value:
        issues.append(f"{spec.auth_key} is empty for URL import")
        missing_keys.append(spec.auth_key)
    if spec.state_key and (url_value or spec.kind != "real_order_state_events") and not state_key:
        issues.append(f"{spec.state_key} is empty; import health cannot track this source")
        missing_keys.append(spec.state_key)
    status = REVIEW if issues else READY
    required_keys = _required_keys_for_spec(spec, mode)
    return {
        "kind": spec.kind,
        "status": status,
        "mode": mode,
        "required_keys": required_keys,
        "missing_keys": missing_keys,
        "input": _safe_value(input_value, spec.input_key),
        "url": _safe_value(url_value, spec.url_key),
        "auth": "set" if auth_value else "empty",
        "source": _safe_value(_clean(env.get(spec.source_key)), spec.source_key),
        "state_key": _safe_value(state_key, spec.state_key or ""),
        "health_tracked": bool(state_key),
        "issues": issues,
        "next_action": _next_action_for_spec(
            spec,
            mode=mode,
            issues=issues,
            placeholder_keys=placeholder_keys,
            missing_keys=missing_keys,
        ),
    }


def _source_mode(*, input_value: str, url_value: str) -> str:
    if input_value and url_value:
        return "mixed"
    if input_value:
        return "file"
    if url_value:
        return "url"
    return "unconfigured"


def _required_keys_for_spec(spec: ExternalSourceEnvSpec, mode: str) -> list[str]:
    if mode == "unconfigured":
        return [f"{spec.input_key} or {spec.url_key}"]
    if mode == "file":
        keys = [spec.input_key]
        if spec.state_key and spec.kind != "real_order_state_events":
            keys.append(spec.state_key)
        return keys
    if mode == "url":
        keys = [spec.url_key]
        if spec.auth_key:
            keys.append(spec.auth_key)
        if spec.state_key:
            keys.append(spec.state_key)
        return keys
    keys = [spec.input_key, spec.url_key]
    if spec.auth_key:
        keys.append(spec.auth_key)
    if spec.state_key:
        keys.append(spec.state_key)
    return keys


def _next_action_for_spec(
    spec: ExternalSourceEnvSpec,
    *,
    mode: str,
    issues: Sequence[str],
    placeholder_keys: Sequence[str],
    missing_keys: Sequence[str],
) -> str:
    if not issues:
        return "Run configured import dry-run, review the masked command, then rerun with --write."
    if placeholder_keys:
        return "Replace placeholder env values before enabling this source."
    if mode == "unconfigured":
        return f"Set {spec.input_key} for file import or {spec.url_key} for URL import."
    if mode == "mixed":
        return "Choose one input mode before production; use file for batch replay or URL for incremental collection."
    if spec.input_key in missing_keys:
        return f"Write the exporter JSONL file or point {spec.input_key} to an existing file."
    if spec.auth_key and spec.auth_key in missing_keys:
        return f"Set {spec.auth_key} so URL imports can authenticate."
    if spec.state_key and spec.state_key in missing_keys:
        return f"Set {spec.state_key} so freshness and cursor health can be tracked."
    return "Fix the reported env issue, then rerun audit_external_source_env.py."


def _aggregate_status(statuses: Sequence[str]) -> str:
    if FAIL in statuses:
        return FAIL
    if REVIEW in statuses:
        return REVIEW
    return READY


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _has_placeholder(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered in {"none", "null", "todo", "tbd", "changeme", "change-me"}:
        return True
    return any(marker in lowered for marker in PLACEHOLDER_MARKERS)


def _safe_value(value: str, key: str) -> str:
    if not value:
        return ""
    lowered = key.lower()
    if any(part in lowered for part in SECRET_KEY_PARTS):
        return "***"
    return value
