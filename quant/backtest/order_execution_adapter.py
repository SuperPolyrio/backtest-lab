"""External order execution adapter contract for guarded strategy intents.

The adapter bridges guarded executor intent templates to an external paper/live
order API. It is deliberately conservative: dry-run is the default, a submit
URL is required for network sends, and all responses are normalized back into
quant.real_order_state_events for fill-first calibration.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any, Callable, Mapping
from urllib.request import Request, urlopen

from quant.backtest.order_event_collector import events_from_order_payload
from quant.backtest.order_state import normalize_order_state_event, upsert_real_order_state_events


READY = "ready"
REVIEW = "review"
BLOCKED = "blocked"
MISSING = "missing"

Transport = Callable[[str, Mapping[str, Any], Mapping[str, str], float], Any]


def build_order_execution_adapter_report(
    guarded_executor_report: Mapping[str, Any],
    *,
    submit_url: str | None = None,
    cancel_url: str | None = None,
    headers: Mapping[str, str] | None = None,
    source: str = "external-order-adapter",
    dry_run: bool = True,
    timeout: float = 15.0,
    transport: Transport | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Execute or dry-run guarded intent templates against an external API."""

    generated_at = _utc_now(now)
    intents = [
        dict(event)
        for event in (guarded_executor_report.get("event_templates") or [])
        if isinstance(event, Mapping)
    ]
    request_templates = [
        build_submit_request_template(intent, source=source, submit_url=submit_url, generated_at=generated_at)
        for intent in intents
    ]
    gate_issues = _intent_gate_issues(intents)
    response_events: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    should_send = bool(submit_url) and not dry_run and bool(intents) and not gate_issues
    if should_send:
        sender = transport or _post_json
        for request_template in request_templates:
            try:
                response = sender(str(submit_url), request_template["body"], dict(headers or {}), float(timeout))
                response_events.extend(
                    _response_events(
                        response,
                        request_template=request_template,
                        source=source,
                        observed_at=generated_at,
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive around live adapters
                errors.append(
                    {
                        "order_id": request_template.get("order_id"),
                        "external_order_id": request_template.get("external_order_id"),
                        "error": str(exc),
                    }
                )
                response_events.append(_error_event(request_template, source=source, error=str(exc), generated_at=generated_at))

    if gate_issues:
        status = BLOCKED
        adapter_status = "fill_first_gate_blocked"
        reason = "guarded intents failed paper/live evidence gate checks"
    elif errors:
        status = BLOCKED
        adapter_status = "submit_error"
        reason = "external order adapter returned errors"
    elif not intents:
        status = REVIEW
        adapter_status = "idle"
        reason = str(guarded_executor_report.get("reason") or "no guarded intent templates")
    elif dry_run:
        status = REVIEW
        adapter_status = "dry_run"
        reason = "dry-run only; external order API was not called"
    elif not submit_url:
        status = BLOCKED
        adapter_status = "missing_submit_url"
        reason = "submit_url is required when dry_run=false"
    else:
        status = READY
        adapter_status = "submitted"
        reason = "external order API responses normalized"

    return {
        "status": status,
        "adapter_status": adapter_status,
        "target_mode": guarded_executor_report.get("target_mode"),
        "dry_run": bool(dry_run),
        "source": source,
        "submit_url_configured": bool(submit_url),
        "cancel_url_configured": bool(cancel_url),
        "intent_count": len(intents),
        "request_count": len(request_templates),
        "response_event_count": len(response_events),
        "recorded_event_count": 0,
        "error_count": len(errors),
        "gate_issue_count": len(gate_issues),
        "reason": reason,
        "generated_at": generated_at,
        "request_templates": request_templates,
        "response_events": response_events,
        "errors": errors,
        "gate_issues": gate_issues,
        "adapter_contract": {
            "input": "guarded_executor.event_templates",
            "network_submit_requires": "dry_run=false and submit_url",
            "requires_paper_live_evidence_gate": True,
            "response_event_sink": "quant.real_order_state_events",
            "calibration_sink": "quant.quant_backtest_calibration_orders",
            "lob_required": False,
            "supports_submit": True,
            "supports_cancel": bool(cancel_url),
            "default_mode": "dry_run",
        },
        "next_actions": _next_actions(
            dry_run=bool(dry_run),
            submit_url_configured=bool(submit_url),
            event_count=len(response_events),
            error_count=len(errors),
            gate_issue_count=len(gate_issues),
        ),
    }


def build_submit_request_template(
    intent_event: Mapping[str, Any],
    *,
    source: str,
    submit_url: str | None,
    generated_at: datetime,
) -> dict[str, Any]:
    """Build the external submit payload from a guarded intent event."""

    payload = intent_event.get("payload") if isinstance(intent_event.get("payload"), Mapping) else {}
    item = payload.get("runner_plan_item") if isinstance(payload.get("runner_plan_item"), Mapping) else {}
    body = {
        "client_order_id": intent_event.get("external_order_id") or intent_event.get("order_id"),
        "run_id": intent_event.get("run_id"),
        "decision_id": payload.get("decision_id"),
        "enable_id": payload.get("enable_id"),
        "target_mode": payload.get("target_mode"),
        "planned_action": payload.get("planned_action"),
        "strategy_name": item.get("strategy_name"),
        "strategy_version": item.get("strategy_version"),
        "market_slug": intent_event.get("market_slug") or item.get("market_slug"),
        "token_id": intent_event.get("token_id") or item.get("token_id"),
        "token_side": intent_event.get("token_side") or item.get("token_side"),
        "price_source": item.get("price_source"),
        "actual_execution_engine": item.get("actual_execution_engine"),
        "submitted_at": generated_at.isoformat(),
        "fill_first": True,
        "paper_live_evidence_gate_status": payload.get("paper_live_evidence_gate_status") or item.get("paper_live_evidence_gate_status"),
        "paper_live_paper_allowed": payload.get("paper_live_paper_allowed") if payload.get("paper_live_paper_allowed") is not None else item.get("paper_live_paper_allowed"),
        "paper_live_live_allowed": payload.get("paper_live_live_allowed") if payload.get("paper_live_live_allowed") is not None else item.get("paper_live_live_allowed"),
        "paper_live_evidence_gate_report": payload.get("paper_live_evidence_gate_report") if isinstance(payload.get("paper_live_evidence_gate_report"), Mapping) else item.get("paper_live_evidence_gate_report"),
        "lob_required": False,
    }
    return {
        "source": source,
        "submit_url": submit_url,
        "order_id": intent_event.get("order_id"),
        "external_order_id": intent_event.get("external_order_id"),
        "run_id": intent_event.get("run_id"),
        "market_slug": body.get("market_slug"),
        "token_side": body.get("token_side"),
        "body": {key: value for key, value in body.items() if value not in (None, "")},
    }


def record_order_execution_adapter_events(conn: Any, report: Mapping[str, Any]) -> int:
    """Persist external adapter response events to quant.real_order_state_events."""

    events = [dict(event) for event in (report.get("response_events") or []) if isinstance(event, Mapping)]
    return upsert_real_order_state_events(conn, events)


def order_execution_adapter_report_to_markdown(report: Mapping[str, Any]) -> str:
    """Render a concise adapter report."""

    lines = [
        f"# Order Execution Adapter: {report.get('adapter_status')}",
        "",
        f"- target_mode: {report.get('target_mode')}",
        f"- dry_run: {report.get('dry_run')}",
        f"- submit_url_configured: {report.get('submit_url_configured')}",
        f"- intent_count: {report.get('intent_count')}",
        f"- response_event_count: {report.get('response_event_count')}",
        f"- recorded_event_count: {report.get('recorded_event_count')}",
        f"- error_count: {report.get('error_count')}",
        f"- reason: {report.get('reason')}",
        "",
        "| Client order | Market | Side | Status | External order |",
        "| --- | --- | --- | --- | --- |",
    ]
    for event in report.get("response_events") or []:
        if not isinstance(event, Mapping):
            continue
        lines.append(
            "| {order_id} | {market} | {side} | {status} | {external} |".format(
                order_id=event.get("order_id") or "-",
                market=event.get("market_slug") or "-",
                side=event.get("token_side") or "-",
                status=event.get("api_order_status") or event.get("submit_status") or "-",
                external=event.get("external_order_id") or "-",
            )
        )
    return "\n".join(lines)


def _response_events(
    response: Any,
    *,
    request_template: Mapping[str, Any],
    source: str,
    observed_at: datetime,
) -> list[dict[str, Any]]:
    events = events_from_order_payload(response, source=source, run_id=_int_or_none(request_template.get("run_id")), observed_at=observed_at.isoformat())
    if events:
        enriched = []
        for event in events:
            row = dict(event)
            row.setdefault("order_id", request_template.get("order_id"))
            row.setdefault("market_slug", request_template.get("market_slug"))
            row.setdefault("token_side", request_template.get("token_side"))
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            payload.setdefault("adapter_request", request_template.get("body"))
            row["payload"] = payload
            enriched.append(normalize_order_state_event(row, source=source, run_id=_int_or_none(request_template.get("run_id"))))
        return enriched
    return [
        normalize_order_state_event(
            {
                "run_id": request_template.get("run_id"),
                "order_id": request_template.get("order_id"),
                "external_order_id": request_template.get("external_order_id"),
                "market_slug": request_template.get("market_slug"),
                "token_side": request_template.get("token_side"),
                "event_time": observed_at,
                "event_type": "submit",
                "source": source,
                "submit_status": "accepted",
                "api_order_status": "ACCEPTED",
                "payload": {
                    "adapter_request": request_template.get("body"),
                    "adapter_response": response,
                },
            },
            source=source,
            run_id=_int_or_none(request_template.get("run_id")),
        )
    ]


def _error_event(
    request_template: Mapping[str, Any],
    *,
    source: str,
    error: str,
    generated_at: datetime,
) -> dict[str, Any]:
    return normalize_order_state_event(
        {
            "run_id": request_template.get("run_id"),
            "order_id": request_template.get("order_id"),
            "external_order_id": request_template.get("external_order_id"),
            "market_slug": request_template.get("market_slug"),
            "token_side": request_template.get("token_side"),
            "event_time": generated_at,
            "event_type": "submit_error",
            "source": source,
            "submit_status": "rejected",
            "api_order_status": "ADAPTER_ERROR",
            "payload": {
                "adapter_request": request_template.get("body"),
                "error": error,
            },
        },
        source=source,
        run_id=_int_or_none(request_template.get("run_id")),
    )


def _post_json(url: str, body: Mapping[str, Any], headers: Mapping[str, str], timeout: float) -> Any:
    request = Request(
        url,
        data=json.dumps(dict(body), ensure_ascii=False, default=str).encode("utf-8"),
        headers={"Accept": "application/json", "Content-Type": "application/json", **dict(headers)},
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
    return json.loads(raw) if raw.strip() else {}


def _intent_gate_issues(intents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for index, intent in enumerate(intents):
        payload = intent.get("payload") if isinstance(intent.get("payload"), Mapping) else {}
        item = payload.get("runner_plan_item") if isinstance(payload.get("runner_plan_item"), Mapping) else {}
        target_mode = str(payload.get("target_mode") or item.get("target_mode") or "paper").strip().lower()
        gate_status = str(payload.get("paper_live_evidence_gate_status") or item.get("paper_live_evidence_gate_status") or "")
        paper_allowed = _boolish(payload.get("paper_live_paper_allowed"), item.get("paper_live_paper_allowed"))
        live_allowed = _boolish(payload.get("paper_live_live_allowed"), item.get("paper_live_live_allowed"))
        gate_report = payload.get("paper_live_evidence_gate_report") if isinstance(payload.get("paper_live_evidence_gate_report"), Mapping) else item.get("paper_live_evidence_gate_report")
        reasons: list[str] = []
        if not isinstance(gate_report, Mapping) or not gate_report:
            reasons.append("missing paper_live_evidence_gate_report")
        if gate_status != READY:
            reasons.append(f"paper_live_evidence_gate_status={gate_status or MISSING}")
        if target_mode == "live" and not live_allowed:
            reasons.append("paper_live_live_allowed=false")
        if target_mode != "live" and not paper_allowed:
            reasons.append("paper_live_paper_allowed=false")
        if reasons:
            issues.append(
                {
                    "index": index,
                    "order_id": intent.get("order_id"),
                    "external_order_id": intent.get("external_order_id"),
                    "target_mode": target_mode,
                    "reasons": reasons,
                }
            )
    return issues


def _boolish(primary: Any, fallback: Any = None) -> bool:
    value = primary if primary is not None else fallback
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _next_actions(*, dry_run: bool, submit_url_configured: bool, event_count: int, error_count: int, gate_issue_count: int) -> list[str]:
    if gate_issue_count:
        return ["rebuild guarded executor intents from a runner plan that passed paper/live evidence gate"]
    if error_count:
        return ["inspect adapter errors before recording events or calibrating fill model"]
    if dry_run:
        actions = ["review submit request templates"]
        if not submit_url_configured:
            actions.append("configure ORDER_EXECUTION_SUBMIT_URL before disabling dry-run")
        actions.append("rerun with dry_run=false only for a tested paper/live endpoint")
        return actions
    if not submit_url_configured:
        return ["configure submit_url before external order execution"]
    if event_count <= 0:
        return ["verify external API response shape maps to order-state events"]
    return ["record response events and run fill-first calibration samples"]


def _utc_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
