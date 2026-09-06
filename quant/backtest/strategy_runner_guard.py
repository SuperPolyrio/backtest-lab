"""Guarded paper/live runner plans built only from enabled strategy state."""

from __future__ import annotations

from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"
BLOCKED = "blocked"

RUNNER_MODES = ("paper", "live")


def build_strategy_runner_plan(
    enable_rows: Sequence[Mapping[str, Any]] | None,
    *,
    target_mode: str,
    include_blocked: bool = False,
    limit: int = 50,
) -> dict[str, Any]:
    """Build a runner input plan from DB-managed strategy enable state.

    This function is intentionally order-submission agnostic. It defines the
    only safe source a future paper/live executor should read from and the sink
    where observed order states must be written for fill-first calibration.
    """

    mode = _runner_mode(target_mode)
    rows = [dict(row) for row in (enable_rows or [])]
    items: list[dict[str, Any]] = []
    blocked_items: list[dict[str, Any]] = []
    for row in rows[: max(1, int(limit))]:
        item = _plan_item(row, target_mode=mode)
        if item["runnable"]:
            items.append(item)
        else:
            blocked_items.append(item)
            if include_blocked:
                items.append(item)

    if not rows:
        status = REVIEW
        runner_status = "idle"
        reason = "no enabled strategy state rows"
    elif any(item["runnable"] for item in items):
        status = READY
        runner_status = "ready"
        reason = "enabled strategies loaded from quant.strategy_enable_state"
    elif blocked_items:
        status = BLOCKED
        runner_status = BLOCKED
        reason = "enabled state rows exist but none are runnable"
    else:
        status = REVIEW
        runner_status = "idle"
        reason = "no runnable strategies"

    return {
        "status": status,
        "runner_status": runner_status,
        "target_mode": mode,
        "runnable_count": sum(1 for item in items if item["runnable"]),
        "blocked_count": len(blocked_items),
        "item_count": len(items),
        "items": items,
        "blocked_items": blocked_items,
        "reason": reason,
        "runner_contract": {
            "read_source": "quant.strategy_enable_state",
            "required_filter": "enabled=true and target_mode=paper/live",
            "requires_activation_decision": True,
            "requires_activation_allowed": True,
            "requires_decision_verdict": READY,
            "requires_paper_live_evidence_gate": True,
            "order_state_sink": "quant.real_order_state_events",
            "calibration_sink": "quant.quant_backtest_calibration_orders",
            "cost_sink": "quant.real_backtest_cost_events",
            "lob_required": False,
        },
    }


def strategy_runner_plan_to_markdown(plan: Mapping[str, Any]) -> str:
    """Render a concise guarded runner plan."""

    lines = [
        f"# Strategy Runner Plan: {plan.get('runner_status')}",
        "",
        f"- target_mode: {plan.get('target_mode')}",
        f"- runnable_count: {plan.get('runnable_count')}",
        f"- blocked_count: {plan.get('blocked_count')}",
        f"- reason: {plan.get('reason')}",
        "",
        "| Runnable | Strategy | Mode | Run | Decision | Market | Engine | Reason |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in plan.get("items") or []:
        lines.append(
            "| {runnable} | {strategy}@{version} | {mode} | {run_id} | {decision_id} | {market}/{side} | {engine} | {reason} |".format(
                runnable="yes" if item.get("runnable") else "no",
                strategy=item.get("strategy_name") or "-",
                version=item.get("strategy_version") or "-",
                mode=item.get("target_mode") or "-",
                run_id=item.get("run_id") or "-",
                decision_id=item.get("decision_id") or "-",
                market=item.get("market_slug") or "-",
                side=item.get("token_side") or "-",
                engine=item.get("actual_execution_engine") or "-",
                reason=str(item.get("reason") or "-").replace("|", "\\|"),
            )
        )
    return "\n".join(lines)


def _plan_item(row: Mapping[str, Any], *, target_mode: str) -> dict[str, Any]:
    enabled = bool(row.get("enabled"))
    activation_allowed = bool(row.get("activation_allowed"))
    decision_verdict = str(row.get("decision_verdict") or MISSING)
    row_mode = _runner_mode(str(row.get("target_mode") or target_mode))
    paper_live_gate = _paper_live_gate_from_row(row)
    paper_live_gate_status = str(row.get("paper_live_evidence_gate_status") or paper_live_gate.get("status") or MISSING)
    paper_live_paper_allowed = bool(row.get("paper_live_paper_allowed") or paper_live_gate.get("paper_allowed"))
    paper_live_live_allowed = bool(row.get("paper_live_live_allowed") or paper_live_gate.get("live_allowed"))
    paper_live_mode_allowed = paper_live_live_allowed if target_mode == "live" else paper_live_paper_allowed
    reasons: list[str] = []
    if row_mode != target_mode:
        reasons.append(f"target_mode mismatch: {row_mode}")
    if not enabled:
        reasons.append("enabled=false")
    if not activation_allowed:
        reasons.append("activation_allowed=false")
    if decision_verdict != READY:
        reasons.append(f"decision_verdict={decision_verdict}")
    if not paper_live_gate:
        reasons.append("missing paper_live_evidence_gate_report")
    if paper_live_gate_status == MISSING:
        reasons.append("paper_live_evidence_gate_status=missing")
    if not paper_live_mode_allowed:
        reasons.append(f"paper_live_{target_mode}_allowed=false")
    if not row.get("decision_id"):
        reasons.append("missing decision_id")
    if not row.get("run_id"):
        reasons.append("missing run_id")
    runnable = not reasons
    planned_action = "paper_shadow_submit" if target_mode == "paper" else "live_guarded_submit"
    return {
        "runnable": runnable,
        "reason": "; ".join(reasons) if reasons else "ready",
        "planned_action": planned_action,
        "target_mode": target_mode,
        "enable_id": _int_or_none(row.get("enable_id")),
        "decision_id": _int_or_none(row.get("decision_id")),
        "run_id": _int_or_none(row.get("run_id")),
        "strategy_name": str(row.get("strategy_name") or "unknown"),
        "strategy_version": str(row.get("strategy_version") or "unknown"),
        "market_slug": str(row.get("market_slug") or ""),
        "token_side": str(row.get("token_side") or ""),
        "price_source": str(row.get("price_source") or ""),
        "actual_execution_engine": str(row.get("actual_execution_engine") or "unknown"),
        "paper_live_evidence_gate_status": paper_live_gate_status,
        "paper_live_paper_allowed": paper_live_paper_allowed,
        "paper_live_live_allowed": paper_live_live_allowed,
        "paper_live_evidence_gate_report": paper_live_gate,
        "order_state_sink": "quant.real_order_state_events",
        "calibration_sink": "quant.quant_backtest_calibration_orders",
        "activation_decision": row.get("activation_decision") if isinstance(row.get("activation_decision"), Mapping) else {},
    }


def _runner_mode(value: str) -> str:
    mode = str(value or "").strip().lower().replace("_", "-")
    aliases = {"paper-trading": "paper", "prod": "live", "production": "live"}
    normalized = aliases.get(mode, mode)
    if normalized not in RUNNER_MODES:
        raise ValueError(f"target_mode must be one of {', '.join(RUNNER_MODES)}")
    return normalized


def _paper_live_gate_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    direct = row.get("paper_live_evidence_gate_report")
    if isinstance(direct, Mapping):
        return dict(direct)
    decision = row.get("activation_decision")
    if isinstance(decision, Mapping):
        gate = decision.get("paper_live_evidence_gate_report")
        if isinstance(gate, Mapping):
            return dict(gate)
        artifact_summary = decision.get("artifact_summary")
        if isinstance(artifact_summary, Mapping) and isinstance(artifact_summary.get("paper_live_evidence_gate_report"), Mapping):
            return dict(artifact_summary["paper_live_evidence_gate_report"])
    return {}


def _int_or_none(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
