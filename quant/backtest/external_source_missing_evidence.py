"""Build actionable work orders for missing external fill evidence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from quant.backtest.shadow_live_plan import (
    build_shadow_live_order_plan,
    shadow_live_event_templates_jsonl,
)


PLAN_SCHEMA_VERSION = "fill_first_missing_external_evidence_plan_v1"


def build_external_source_missing_evidence_plan(
    inputs: Mapping[str, Any] | None,
    *,
    run_id: int | None = None,
    source: str = "live-shadow",
    max_orders: int | None = None,
) -> dict[str, Any]:
    """Return the exact run orders that still need real order-state/calibration evidence."""

    if inputs is None:
        return {
            "schema_version": PLAN_SCHEMA_VERSION,
            "status": "missing",
            "run_id": run_id,
            "reason": "run_not_found",
            "orders_requiring_order_state": [],
            "orders_requiring_calibration": [],
            "event_templates": [],
            "next_actions": ["Create or select a fill-first backtest run before exporting missing evidence work orders."],
        }

    run = dict(inputs.get("run") or {})
    parameters = dict(inputs.get("parameters") or {})
    orders = [dict(row) for row in inputs.get("orders") or []]
    real_order_events = [dict(row) for row in _first_sequence(inputs, ("real_order_events", "real_order_state_rows"))]
    calibration_rows = [dict(row) for row in inputs.get("calibration_rows") or []]

    candidates = [order for order in orders if _is_external_order_candidate(order)]
    candidate_by_id = {_order_id(order): order for order in candidates if _order_id(order)}
    candidate_ids = set(candidate_by_id)
    order_state_ids = _event_order_ids(real_order_events)
    calibration_ids = _calibration_order_ids(calibration_rows)

    missing_order_state_ids = sorted(candidate_ids - order_state_ids)
    missing_calibration_ids = sorted(candidate_ids - calibration_ids)
    if max_orders is not None:
        max_count = max(0, int(max_orders))
        missing_order_state_ids = missing_order_state_ids[:max_count]
        missing_calibration_ids = missing_calibration_ids[:max_count]

    missing_order_state_orders = [candidate_by_id[order_id] for order_id in missing_order_state_ids]
    order_state_plan = build_shadow_live_order_plan(
        {
            "run": run,
            "parameters": parameters,
            "orders": missing_order_state_orders,
        },
        source=source,
        include_rejected=True,
    )

    orders_requiring_calibration = [
        _compact_order(candidate_by_id[order_id], reason="missing_calibration_sample")
        for order_id in missing_calibration_ids
    ]
    orders_requiring_order_state = [
        _compact_order(candidate_by_id[order_id], reason="missing_order_state_event")
        for order_id in missing_order_state_ids
    ]
    status = "ready" if not missing_order_state_ids and not missing_calibration_ids else "review"
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "status": status,
        "run_id": int(run.get("run_id") or run_id or 0) or None,
        "market_slug": run.get("market_slug"),
        "token_side": run.get("token_side"),
        "source": source,
        "reason": _reason(missing_order_state_ids, missing_calibration_ids),
        "external_order_candidate_count": len(candidate_ids),
        "missing_order_state_count": len(missing_order_state_ids),
        "missing_calibration_count": len(missing_calibration_ids),
        "orders_requiring_order_state": orders_requiring_order_state,
        "orders_requiring_calibration": orders_requiring_calibration,
        "event_templates": list(order_state_plan.get("event_templates") or []),
        "commands": _commands(int(run.get("run_id") or run_id or 0) or None, source),
        "next_actions": _next_actions(missing_order_state_ids, missing_calibration_ids),
    }


def external_source_missing_evidence_event_templates_jsonl(plan: Mapping[str, Any]) -> str:
    """Return JSONL templates for only the orders missing order-state evidence."""

    return shadow_live_event_templates_jsonl({"event_templates": list(plan.get("event_templates") or [])})


def write_external_source_missing_evidence_task_pack(
    plan: Mapping[str, Any],
    output_dir: str | Path,
    *,
    run_id: int | None = None,
    prefix: str | None = None,
) -> dict[str, Any]:
    """Write a self-contained task pack for collecting missing external evidence."""

    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    resolved_run_id = plan.get("run_id") or run_id
    run_text = str(resolved_run_id or "unknown")
    base_name = prefix or f"missing_external_evidence_{run_text}"

    json_path = target_dir / f"{base_name}.json"
    markdown_path = target_dir / f"{base_name}.md"
    event_jsonl_path = target_dir / f"{base_name}.event_templates.jsonl"
    commands_path = target_dir / f"{base_name}.commands.sh"

    json_path.write_text(json.dumps(dict(plan), ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    markdown_path.write_text(external_source_missing_evidence_plan_to_markdown(plan) + "\n", encoding="utf-8")
    event_jsonl = external_source_missing_evidence_event_templates_jsonl(plan)
    event_jsonl_path.write_text(event_jsonl + ("\n" if event_jsonl and not event_jsonl.endswith("\n") else ""), encoding="utf-8")
    commands_path.write_text(_commands_shell(plan), encoding="utf-8")

    return {
        "status": plan.get("status") or "unknown",
        "run_id": resolved_run_id,
        "output_dir": str(target_dir),
        "missing_order_state_count": int(plan.get("missing_order_state_count") or 0),
        "missing_calibration_count": int(plan.get("missing_calibration_count") or 0),
        "event_template_count": len(list(plan.get("event_templates") or [])),
        "files": {
            "plan_json": str(json_path),
            "plan_markdown": str(markdown_path),
            "event_templates_jsonl": str(event_jsonl_path),
            "commands": str(commands_path),
        },
    }


def external_source_missing_evidence_plan_to_markdown(plan: Mapping[str, Any]) -> str:
    lines = [
        f"# Missing External Evidence Plan: {plan.get('status')}",
        "",
        f"- schema: {plan.get('schema_version')}",
        f"- run_id: {plan.get('run_id')}",
        f"- market: {plan.get('market_slug') or '-'}",
        f"- token_side: {plan.get('token_side') or '-'}",
        f"- source: {plan.get('source') or '-'}",
        f"- reason: {plan.get('reason')}",
        f"- external_order_candidate_count: {plan.get('external_order_candidate_count', 0)}",
        f"- missing_order_state_count: {plan.get('missing_order_state_count', 0)}",
        f"- missing_calibration_count: {plan.get('missing_calibration_count', 0)}",
        "",
        "## Orders Requiring Order-State Evidence",
        "",
        "| order_id | status | side | role | requested_price | requested_size | submit_x | reason |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for order in list(plan.get("orders_requiring_order_state") or [])[:50]:
        lines.append(_order_markdown_row(order))
    if not plan.get("orders_requiring_order_state"):
        lines.append("| - | - | - | - | - | - | - | none |")
    lines.extend(
        [
            "",
            "## Orders Requiring Calibration Samples",
            "",
            "| order_id | status | side | role | requested_price | requested_size | submit_x | reason |",
            "| --- | --- | --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for order in list(plan.get("orders_requiring_calibration") or [])[:50]:
        lines.append(_order_markdown_row(order))
    if not plan.get("orders_requiring_calibration"):
        lines.append("| - | - | - | - | - | - | - | none |")
    commands = plan.get("commands") if isinstance(plan.get("commands"), Mapping) else {}
    if commands:
        lines.extend(["", "## Commands"])
        for name, command in commands.items():
            lines.append(f"- {name}: `{command}`")
    actions = list(plan.get("next_actions") or [])
    if actions:
        lines.extend(["", "## Next Actions"])
        lines.extend(f"- {action}" for action in actions)
    return "\n".join(lines)


def _commands_shell(plan: Mapping[str, Any]) -> str:
    commands = plan.get("commands") if isinstance(plan.get("commands"), Mapping) else {}
    lines = [
        "#!/usr/bin/env bash",
        "set -euo pipefail",
        "",
        "# Fill these event templates with real external order-state evidence first.",
    ]
    for name, command in commands.items():
        lines.extend(["", f"# {name}", str(command)])
    return "\n".join(lines) + "\n"


def _order_markdown_row(order: Mapping[str, Any]) -> str:
    return "| {order_id} | {status} | {side} | {role} | {price} | {size} | {submit_x} | {reason} |".format(
        order_id=order.get("order_id") or "",
        status=order.get("simulated_status") or "",
        side=order.get("side") or "",
        role=order.get("role") or "",
        price=order.get("requested_price") or "",
        size=order.get("requested_size") or "",
        submit_x=order.get("submit_x") or "",
        reason=order.get("reason") or "",
    )


def _compact_order(order: Mapping[str, Any], *, reason: str) -> dict[str, Any]:
    return {
        "order_id": order.get("order_id"),
        "reason": reason,
        "simulated_status": str(order.get("status") or "UNKNOWN").upper(),
        "market_slug": order.get("market_slug"),
        "side": order.get("side"),
        "role": order.get("role"),
        "order_type": order.get("order_type"),
        "signal_x": order.get("signal_x"),
        "submit_x": order.get("submit_x"),
        "requested_price": _text(order.get("requested_price") or order.get("decision_price")),
        "requested_size": _text(order.get("requested_size")),
        "requested_notional": _text(order.get("requested_notional")),
        "filled_size": _text(order.get("filled_size") or order.get("actual_fill_size")),
        "filled_notional": _text(order.get("filled_notional") or order.get("actual_fill_notional")),
        "no_fill_reason": order.get("no_fill_reason"),
        "execution_source": order.get("execution_source"),
    }


def _commands(run_id: int | None, source: str) -> dict[str, str]:
    run_text = str(run_id or "<run_id>")
    return {
        "export_missing_event_templates": (
            "conda run -n polyBacktest python scripts/export_external_source_missing_evidence_plan.py "
            f"--run-id {run_text} --format event-jsonl --output runtime_outputs/missing_external_evidence_{run_text}.jsonl"
        ),
        "validate_filled_events": (
            "conda run -n polyBacktest python scripts/validate_shadow_live_order_events.py "
            f"--input runtime_outputs/missing_external_evidence_{run_text}.jsonl"
        ),
        "import_filled_events": (
            "conda run -n polyBacktest python scripts/import_real_order_state_events.py "
            f"--input runtime_outputs/missing_external_evidence_{run_text}.jsonl --source {source} --run-id {run_text}"
        ),
        "build_calibration_samples": (
            "conda run -n polyBacktest python scripts/build_backtest_calibration_samples.py "
            f"--run-id {run_text} --event-source {source}"
        ),
        "check_run_coverage": (
            "conda run -n polyBacktest python scripts/check_external_source_run_coverage.py "
            f"--run-id {run_text} --format markdown"
        ),
    }


def _next_actions(missing_order_state_ids: Sequence[str], missing_calibration_ids: Sequence[str]) -> list[str]:
    actions: list[str] = []
    if missing_order_state_ids:
        actions.append("Fill the exported event_templates with real API/order-stream terminal status, fill price, size, fee/rebate, cash/position delta, and observed latency.")
        actions.append("Validate and import the filled event template JSONL before building calibration samples.")
    if missing_calibration_ids:
        actions.append("Build or refresh calibration samples after order-state events are imported.")
    if not actions:
        actions.append("No missing external order-state or calibration evidence for this run.")
    return actions


def _reason(missing_order_state_ids: Sequence[str], missing_calibration_ids: Sequence[str]) -> str:
    if not missing_order_state_ids and not missing_calibration_ids:
        return "external order-state and calibration evidence are complete for candidate orders"
    parts: list[str] = []
    if missing_order_state_ids:
        parts.append(f"{len(missing_order_state_ids)} orders missing order-state evidence")
    if missing_calibration_ids:
        parts.append(f"{len(missing_calibration_ids)} orders missing calibration samples")
    return "; ".join(parts)


def _first_sequence(inputs: Mapping[str, Any], keys: Sequence[str]) -> Sequence[Any]:
    for key in keys:
        value = inputs.get(key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            return value
    return []


def _event_order_ids(events: list[Mapping[str, Any]]) -> set[str]:
    ids: set[str] = set()
    for event in events:
        payload = _payload(event.get("payload"))
        for value in (
            event.get("order_id"),
            event.get("external_order_id"),
            payload.get("simulated_order_id"),
            payload.get("simulatedOrderId"),
        ):
            text = _text(value)
            if text:
                ids.add(text)
    return ids


def _calibration_order_ids(rows: list[Mapping[str, Any]]) -> set[str]:
    ids: set[str] = set()
    for row in rows:
        for value in (row.get("simulated_order_id"), row.get("live_order_id")):
            text = _text(value)
            if text:
                ids.add(text)
    return ids


def _is_external_order_candidate(order: Mapping[str, Any]) -> bool:
    execution_source = str(order.get("execution_source") or "").lower()
    order_type = str(order.get("order_type") or "").upper()
    if execution_source in {"settlement_payoff", "force_close", "settlement"}:
        return False
    if order_type in {"SETTLEMENT", "REDEEM", "PAYOUT"}:
        return False
    return bool(_order_id(order))


def _order_id(order: Mapping[str, Any]) -> str:
    return _text(order.get("order_id")) or ""


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
