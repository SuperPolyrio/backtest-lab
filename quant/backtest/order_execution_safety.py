"""Safety checks for guarded external order execution."""

from __future__ import annotations

import os
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse


READY = "ready"
REVIEW = "review"
BLOCKED = "blocked"
FAIL = "fail"

LIVE_CONFIRM_TOKEN = "I_UNDERSTAND_LIVE_ORDER_RISK"

PLACEHOLDER_MARKERS = (
    "replace-with",
    "replace_with",
    "todo",
    "<",
    ">",
    "example",
)

SENSITIVE_HEADER_PARTS = ("authorization", "auth", "token", "secret", "password", "api-key", "apikey")


def build_order_execution_env_audit(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Audit ORDER_EXECUTION_* configuration without exposing secret values."""

    values = dict(os.environ if env is None else env)
    target_mode = _target_mode(values.get("ORDER_EXECUTION_TARGET_MODE") or "paper")
    submit_url = _clean(values.get("ORDER_EXECUTION_SUBMIT_URL"))
    cancel_url = _clean(values.get("ORDER_EXECUTION_CANCEL_URL"))
    auth_header = _clean(values.get("ORDER_EXECUTION_AUTH_HEADER") or values.get("ORDER_EXECUTION_HEADER"))
    source = _clean(values.get("ORDER_EXECUTION_SOURCE") or "external-order-adapter")
    live_confirm = _clean(values.get("ORDER_EXECUTION_LIVE_CONFIRM"))

    issues: list[str] = []
    missing_keys: list[str] = []
    placeholder_keys = [
        key
        for key in (
            "ORDER_EXECUTION_SUBMIT_URL",
            "ORDER_EXECUTION_CANCEL_URL",
            "ORDER_EXECUTION_AUTH_HEADER",
            "ORDER_EXECUTION_HEADER",
            "ORDER_EXECUTION_SOURCE",
            "ORDER_EXECUTION_LIVE_CONFIRM",
        )
        if _has_placeholder(values.get(key))
    ]
    if placeholder_keys:
        issues.append("placeholder values: " + ", ".join(placeholder_keys))
    if target_mode not in {"paper", "live"}:
        issues.append("ORDER_EXECUTION_TARGET_MODE must be paper or live")
        missing_keys.append("ORDER_EXECUTION_TARGET_MODE")
    if not submit_url:
        issues.append("ORDER_EXECUTION_SUBMIT_URL is empty")
        missing_keys.append("ORDER_EXECUTION_SUBMIT_URL")
    if submit_url and not _is_local_url(submit_url) and not auth_header:
        issues.append("ORDER_EXECUTION_AUTH_HEADER is empty for non-local submit URL")
        missing_keys.append("ORDER_EXECUTION_AUTH_HEADER")
    if target_mode == "live" and live_confirm != LIVE_CONFIRM_TOKEN:
        issues.append(f"live execution requires ORDER_EXECUTION_LIVE_CONFIRM={LIVE_CONFIRM_TOKEN}")
        missing_keys.append("ORDER_EXECUTION_LIVE_CONFIRM")

    configured = bool(submit_url)
    status = READY if configured and not issues else REVIEW
    return {
        "status": status,
        "target_mode": target_mode,
        "configured": configured,
        "submit_url": _safe_url(submit_url),
        "cancel_url": _safe_url(cancel_url),
        "auth": "set" if auth_header else "empty",
        "source": source,
        "live_confirmed": live_confirm == LIVE_CONFIRM_TOKEN,
        "required_live_confirm": LIVE_CONFIRM_TOKEN,
        "missing_keys": missing_keys,
        "placeholder_keys": placeholder_keys,
        "issues": issues,
        "next_action": _env_next_action(configured=configured, issues=issues, target_mode=target_mode),
    }


def build_order_execution_run_safety_report(
    *,
    target_mode: str,
    execute: bool,
    record_events: bool,
    submit_url: str | None,
    headers: Mapping[str, str] | None = None,
    live_confirm: str | None = None,
) -> dict[str, Any]:
    """Validate one order execution adapter invocation."""

    mode = _target_mode(target_mode)
    header_values = dict(headers or {})
    issues: list[str] = []
    if mode not in {"paper", "live"}:
        issues.append("target_mode must be paper or live")
    if _has_placeholder(submit_url):
        issues.append("submit_url contains a placeholder")
    if any(_has_placeholder(value) for value in header_values.values()):
        issues.append("headers contain placeholder values")
    if execute and not record_events:
        issues.append("--execute requires --record-events so submit/cancel/fill evidence is persisted")
    if execute and not submit_url:
        issues.append("--execute requires ORDER_EXECUTION_SUBMIT_URL or --submit-url")
    if execute and submit_url and not _is_local_url(submit_url) and not _has_auth_header(header_values):
        issues.append("--execute against a non-local URL requires an auth header")
    if execute and mode == "live" and live_confirm != LIVE_CONFIRM_TOKEN:
        issues.append(f"live --execute requires --live-confirm {LIVE_CONFIRM_TOKEN}")

    if issues:
        status = BLOCKED
        safety_status = BLOCKED
        reason = "; ".join(issues)
    elif execute:
        status = READY
        safety_status = "execute_allowed"
        reason = "external order execution safety checks passed"
    else:
        status = REVIEW
        safety_status = "dry_run_safe"
        reason = "dry-run only; external order API will not be called"

    return {
        "status": status,
        "safety_status": safety_status,
        "target_mode": mode,
        "execute": bool(execute),
        "record_events": bool(record_events),
        "submit_url_configured": bool(submit_url),
        "auth_header_configured": _has_auth_header(header_values),
        "live_confirmed": live_confirm == LIVE_CONFIRM_TOKEN,
        "required_live_confirm": LIVE_CONFIRM_TOKEN,
        "issues": issues,
        "reason": reason,
    }


def order_execution_env_audit_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Order Execution Env Audit: {report.get('status')}",
        "",
        f"- target_mode: {report.get('target_mode')}",
        f"- configured: {report.get('configured')}",
        f"- submit_url: `{report.get('submit_url') or ''}`",
        f"- cancel_url: `{report.get('cancel_url') or ''}`",
        f"- auth: {report.get('auth')}",
        f"- live_confirmed: {report.get('live_confirmed')}",
        f"- issues: {'; '.join(str(issue) for issue in report.get('issues', [])) or 'ok'}",
        f"- next_action: {report.get('next_action')}",
    ]
    return "\n".join(lines)


def order_execution_run_safety_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Order Execution Safety: {report.get('safety_status')}",
        "",
        f"- target_mode: {report.get('target_mode')}",
        f"- execute: {report.get('execute')}",
        f"- record_events: {report.get('record_events')}",
        f"- submit_url_configured: {report.get('submit_url_configured')}",
        f"- auth_header_configured: {report.get('auth_header_configured')}",
        f"- live_confirmed: {report.get('live_confirmed')}",
        f"- reason: {report.get('reason')}",
    ]
    return "\n".join(lines)


def _target_mode(value: str | None) -> str:
    return str(value or "paper").strip().lower().replace("_", "-")


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _has_placeholder(value: Any) -> bool:
    text = _clean(value).lower()
    if not text:
        return False
    return any(marker in text for marker in PLACEHOLDER_MARKERS)


def _is_local_url(value: str | None) -> bool:
    parsed = urlparse(str(value or ""))
    host = (parsed.hostname or "").lower()
    return host in {"localhost", "127.0.0.1", "::1"} or host.endswith(".local")


def _has_auth_header(headers: Mapping[str, str]) -> bool:
    for key, value in headers.items():
        if not value:
            continue
        name = str(key).strip().lower()
        if any(part in name for part in SENSITIVE_HEADER_PARTS):
            return True
    return False


def _safe_url(value: str) -> str:
    if not value:
        return ""
    parsed = urlparse(value)
    if not parsed.netloc:
        return value
    userinfo = ""
    if parsed.username or parsed.password:
        userinfo = "***@"
    host = parsed.hostname or ""
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return parsed._replace(netloc=f"{userinfo}{host}").geturl()


def _env_next_action(*, configured: bool, issues: Sequence[str], target_mode: str) -> str:
    if not configured:
        return "configure ORDER_EXECUTION_SUBMIT_URL for a tested paper endpoint"
    if issues:
        return "fix order execution env issues before running with --execute"
    if target_mode == "live":
        return "run a paper/shadow validation window before live --execute"
    return "review dry-run request templates before enabling --execute"
