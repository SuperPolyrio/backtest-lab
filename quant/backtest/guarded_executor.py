"""Guarded paper/live executor adapter for fill-first strategy activation.

This module intentionally does not submit real orders. It converts a guarded
runner plan into deterministic order-intent evidence that can be inspected,
dry-run, or explicitly written to quant.real_order_state_events before a real
external order adapter is wired in.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from quant.backtest.order_state import normalize_order_state_event, upsert_real_order_state_events


READY = "ready"
REVIEW = "review"
BLOCKED = "blocked"

EXECUTOR_SOURCES = {
    "paper": "guarded-paper-executor",
    "live": "guarded-live-executor",
}


def build_guarded_executor_report(
    runner_plan: Mapping[str, Any],
    *,
    record_intent: bool = False,
    event_source: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a guarded executor report from a strategy runner plan.

    The report is dry-run by default. Passing record_intent=True only marks the
    report as intended for persistence; callers must explicitly call
    record_guarded_execution_intents to write to the order-state evidence table.
    """

    target_mode = str(runner_plan.get("target_mode") or "paper").strip().lower()
    if target_mode not in EXECUTOR_SOURCES:
        raise ValueError("target_mode must be one of paper, live")
    source = event_source or EXECUTOR_SOURCES[target_mode]
    generated_at = _utc_now(now)
    runnable_items = [
        dict(item)
        for item in (runner_plan.get("items") or [])
        if isinstance(item, Mapping) and bool(item.get("runnable"))
    ]
    blocked_items = [
        dict(item)
        for item in (runner_plan.get("blocked_items") or [])
        if isinstance(item, Mapping)
    ]
    events = [
        _intent_event(item, source=source, target_mode=target_mode, generated_at=generated_at)
        for item in runnable_items
    ]

    if events:
        status = READY
        executor_status = "intent_ready" if record_intent else "dry_run_ready"
        reason = "guarded runnable strategy intents built"
    elif runner_plan.get("runner_status") == BLOCKED or blocked_items:
        status = BLOCKED
        executor_status = BLOCKED
        reason = "runner plan has no runnable items"
    else:
        status = REVIEW
        executor_status = "idle"
        reason = str(runner_plan.get("reason") or "no runnable strategy intents")

    return {
        "status": status,
        "executor_status": executor_status,
        "target_mode": target_mode,
        "input_runner_status": runner_plan.get("runner_status"),
        "record_intent": bool(record_intent),
        "dry_run": not bool(record_intent),
        "event_source": source,
        "planned_action_count": len(runnable_items),
        "recorded_event_count": 0,
        "blocked_count": int(runner_plan.get("blocked_count") or len(blocked_items)),
        "reason": reason,
        "generated_at": generated_at,
        "event_templates": events,
        "runner_contract": dict(runner_plan.get("runner_contract") or {}),
        "executor_contract": {
            "input_source": "strategy_runner_plan",
            "enabled_state_source": "quant.strategy_enable_state",
            "requires_paper_live_evidence_gate": True,
            "order_state_sink": "quant.real_order_state_events",
            "calibration_sink": "quant.quant_backtest_calibration_orders",
            "lob_required": False,
            "real_order_submission": False,
            "requires_external_order_adapter": True,
            "default_mode": "dry_run",
        },
        "next_actions": _next_actions(target_mode=target_mode, event_count=len(events), record_intent=record_intent),
    }


def record_guarded_execution_intents(conn: Any, report: Mapping[str, Any]) -> int:
    """Persist report event templates into quant.real_order_state_events."""

    events = [dict(event) for event in (report.get("event_templates") or []) if isinstance(event, Mapping)]
    return upsert_real_order_state_events(conn, events)


def guarded_executor_report_to_markdown(report: Mapping[str, Any]) -> str:
    """Render a concise guarded executor report."""

    lines = [
        f"# Guarded Executor: {report.get('executor_status')}",
        "",
        f"- target_mode: {report.get('target_mode')}",
        f"- record_intent: {report.get('record_intent')}",
        f"- planned_action_count: {report.get('planned_action_count')}",
        f"- recorded_event_count: {report.get('recorded_event_count')}",
        f"- event_source: {report.get('event_source')}",
        f"- reason: {report.get('reason')}",
        "",
        "| Action | Run | Decision | Strategy | Market | Side | Submit status | External order id |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for event in report.get("event_templates") or []:
        if not isinstance(event, Mapping):
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), Mapping) else {}
        item = payload.get("runner_plan_item") if isinstance(payload.get("runner_plan_item"), Mapping) else {}
        lines.append(
            "| {action} | {run_id} | {decision_id} | {strategy}@{version} | {market} | {side} | {status} | {external_order_id} |".format(
                action=payload.get("planned_action") or "-",
                run_id=event.get("run_id") or "-",
                decision_id=payload.get("decision_id") or "-",
                strategy=item.get("strategy_name") or "-",
                version=item.get("strategy_version") or "-",
                market=event.get("market_slug") or "-",
                side=event.get("token_side") or "-",
                status=event.get("submit_status") or "-",
                external_order_id=event.get("external_order_id") or "-",
            )
        )
    return "\n".join(lines)


def _intent_event(
    item: Mapping[str, Any],
    *,
    source: str,
    target_mode: str,
    generated_at: datetime,
) -> dict[str, Any]:
    decision_id = _int_or_none(item.get("decision_id"))
    run_id = _int_or_none(item.get("run_id"))
    enable_id = _int_or_none(item.get("enable_id"))
    external_order_id = "guarded-{mode}-run-{run}-decision-{decision}-enable-{enable}".format(
        mode=target_mode,
        run=run_id or "none",
        decision=decision_id or "none",
        enable=enable_id or "none",
    )
    event = {
        "run_id": run_id,
        "order_id": external_order_id,
        "external_order_id": external_order_id,
        "market_slug": item.get("market_slug") or None,
        "token_id": item.get("token_id") or None,
        "token_side": item.get("token_side") or None,
        "event_time": generated_at,
        "event_type": "submit_intent",
        "source": source,
        "submit_status": "INTENT_RECORDED",
        "api_order_status": "DRY_RUN",
        "clob_order_status": "NOT_SUBMITTED",
        "submit_at": generated_at,
        "payload": {
            "target_mode": target_mode,
            "planned_action": item.get("planned_action"),
            "decision_id": decision_id,
            "enable_id": enable_id,
            "run_id": run_id,
            "dry_run": True,
            "real_order_submission": False,
            "requires_external_order_adapter": True,
            "lob_required": False,
            "order_state_sink": item.get("order_state_sink") or "quant.real_order_state_events",
            "calibration_sink": item.get("calibration_sink") or "quant.quant_backtest_calibration_orders",
            "paper_live_evidence_gate_status": item.get("paper_live_evidence_gate_status"),
            "paper_live_paper_allowed": item.get("paper_live_paper_allowed"),
            "paper_live_live_allowed": item.get("paper_live_live_allowed"),
            "paper_live_evidence_gate_report": item.get("paper_live_evidence_gate_report") if isinstance(item.get("paper_live_evidence_gate_report"), Mapping) else {},
            "runner_plan_item": dict(item),
        },
    }
    return normalize_order_state_event(event)


def _next_actions(*, target_mode: str, event_count: int, record_intent: bool) -> list[str]:
    if event_count <= 0:
        return ["enable a ready activation decision before running paper/live executor"]
    actions = []
    if not record_intent:
        actions.append("review dry-run intent templates")
        actions.append("rerun with record_intent=true only when the intent should enter order-state evidence")
    actions.append("wire external order adapter before real submit/cancel/fill")
    actions.append(f"compare {target_mode} order-state evidence against fill-first calibration")
    return actions


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
