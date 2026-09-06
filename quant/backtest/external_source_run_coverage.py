"""Run-level coverage checks for fill-first external evidence."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from quant.backtest.platform_incidents import load_platform_incidents_for_run


READY = "ready"
REVIEW = "review"
MISSING = "missing"


def load_external_source_run_coverage_inputs(conn: Any, *, run_id: int) -> dict[str, Any] | None:
    """Load persisted run evidence needed for a fill-first external coverage report."""

    run = _fetch_one(conn, "SELECT * FROM quant.quant_backtest_runs WHERE run_id = %s", (int(run_id),))
    if not run:
        return None
    orders = _fetch_all(conn, "quant.quant_backtest_orders", "run_id = %s ORDER BY signal_index ASC, order_id ASC", (int(run_id),))
    real_order_events = _fetch_all(
        conn,
        "quant.real_order_state_events",
        "run_id = %s ORDER BY COALESCE(event_time, created_at), event_id",
        (int(run_id),),
    )
    calibration_rows = _fetch_all(
        conn,
        "quant.quant_backtest_calibration_orders",
        "run_id = %s ORDER BY observed_at NULLS LAST, calibration_id",
        (int(run_id),),
    )
    real_cost_events = _fetch_all(
        conn,
        "quant.real_backtest_cost_events",
        "run_id = %s ORDER BY observed_at NULLS LAST, cost_id",
        (int(run_id),),
    )
    cost_calibration_rows = _fetch_all(
        conn,
        "quant.quant_backtest_cost_calibration",
        "run_id = %s ORDER BY observed_at NULLS LAST, cost_calibration_id",
        (int(run_id),),
    )
    external_states = _fetch_all(conn, "quant.external_source_import_state", "TRUE ORDER BY updated_at DESC, state_key ASC LIMIT 100", ())
    try:
        incidents = load_platform_incidents_for_run(conn, run)
    except Exception:
        incidents = []
    return {
        "run": run,
        "orders": orders,
        "real_order_events": real_order_events,
        "calibration_rows": calibration_rows,
        "real_cost_events": real_cost_events,
        "cost_calibration_rows": cost_calibration_rows,
        "platform_incidents": incidents,
        "external_states": external_states,
    }


def build_external_source_run_coverage_report(inputs: Mapping[str, Any] | None, *, run_id: int | None = None) -> dict[str, Any]:
    """Summarize whether one run is covered by external fill-first evidence."""

    if inputs is None:
        return {
            "status": MISSING,
            "run_id": run_id,
            "reason": "run_not_found",
            "checks": [],
            "next_actions": ["Create or select a fill-first backtest run before checking external evidence coverage."],
        }
    run = dict(inputs.get("run") or {})
    orders = [dict(row) for row in inputs.get("orders") or []]
    real_order_events = [dict(row) for row in inputs.get("real_order_events") or []]
    calibration_rows = [dict(row) for row in inputs.get("calibration_rows") or []]
    real_cost_events = [dict(row) for row in inputs.get("real_cost_events") or []]
    cost_calibration_rows = [dict(row) for row in inputs.get("cost_calibration_rows") or []]
    platform_incidents = [dict(row) for row in inputs.get("platform_incidents") or []]
    external_states = [dict(row) for row in inputs.get("external_states") or []]

    candidates = [order for order in orders if _is_external_order_candidate(order)]
    candidate_ids = {_text(order.get("order_id")) for order in candidates if _text(order.get("order_id"))}
    event_order_ids = _event_order_ids(real_order_events)
    calibration_order_ids = _calibration_order_ids(calibration_rows)
    matched_event_ids = candidate_ids & event_order_ids
    matched_calibration_ids = candidate_ids & calibration_order_ids
    expected_cost_count = _expected_cost_count(orders)

    order_coverage = _coverage(len(matched_event_ids), len(candidate_ids))
    calibration_coverage = _coverage(len(matched_calibration_ids), len(candidate_ids))
    checks = [
        _order_state_check(len(candidate_ids), len(real_order_events), len(matched_event_ids), order_coverage),
        _calibration_check(len(candidate_ids), len(calibration_rows), len(matched_calibration_ids), calibration_coverage),
        _cost_check(expected_cost_count, len(real_cost_events), len(cost_calibration_rows)),
        _incident_check(len(platform_incidents)),
        _external_state_check(len(external_states)),
    ]
    status = _aggregate(check["status"] for check in checks)
    return {
        "status": status,
        "run_id": int(run.get("run_id") or run_id or 0) or None,
        "market_slug": run.get("market_slug"),
        "token_side": run.get("token_side"),
        "reason": _reason(checks, status),
        "external_order_candidate_count": len(candidate_ids),
        "real_order_state_event_count": len(real_order_events),
        "matched_order_state_count": len(matched_event_ids),
        "order_state_coverage_pct": order_coverage,
        "calibration_sample_count": len(calibration_rows),
        "matched_calibration_order_count": len(matched_calibration_ids),
        "calibration_coverage_pct": calibration_coverage,
        "expected_cost_order_count": expected_cost_count,
        "real_cost_event_count": len(real_cost_events),
        "cost_calibration_sample_count": len(cost_calibration_rows),
        "platform_incident_count": len(platform_incidents),
        "external_source_state_count": len(external_states),
        "missing_order_state_order_ids": sorted(candidate_ids - event_order_ids)[:50],
        "missing_calibration_order_ids": sorted(candidate_ids - calibration_order_ids)[:50],
        "checks": checks,
        "next_actions": _next_actions(checks),
    }


def external_source_run_coverage_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# External Source Run Coverage: {report.get('status')}",
        "",
        f"- run_id: {report.get('run_id')}",
        f"- market: {report.get('market_slug') or '-'}",
        f"- token_side: {report.get('token_side') or '-'}",
        f"- reason: {report.get('reason')}",
        f"- order_state_coverage_pct: {report.get('order_state_coverage_pct', 0)}",
        f"- calibration_coverage_pct: {report.get('calibration_coverage_pct', 0)}",
        "",
        "| Check | Status | Detail |",
        "| --- | --- | --- |",
    ]
    for check in report.get("checks") or []:
        lines.append(
            "| {name} | {status} | {detail} |".format(
                name=check.get("name", ""),
                status=check.get("status", ""),
                detail=str(check.get("detail") or "").replace("|", "\\|"),
            )
        )
    actions = list(report.get("next_actions") or [])
    if actions:
        lines.extend(["", "## Next Actions"])
        lines.extend(f"- {action}" for action in actions)
    return "\n".join(lines)


def _order_state_check(candidate_count: int, event_count: int, matched_count: int, coverage: str) -> dict[str, str]:
    if candidate_count <= 0:
        return _check("order-state coverage", REVIEW, "no external order candidates found for this run")
    if matched_count >= candidate_count:
        return _check("order-state coverage", READY, f"matched={matched_count}/{candidate_count} events={event_count} coverage={coverage}%")
    return _check("order-state coverage", REVIEW, f"matched={matched_count}/{candidate_count} events={event_count} coverage={coverage}%")


def _calibration_check(candidate_count: int, sample_count: int, matched_count: int, coverage: str) -> dict[str, str]:
    if candidate_count <= 0:
        return _check("fill calibration coverage", REVIEW, "no external order candidates available for calibration")
    if matched_count >= candidate_count:
        return _check("fill calibration coverage", READY, f"matched={matched_count}/{candidate_count} samples={sample_count} coverage={coverage}%")
    return _check("fill calibration coverage", REVIEW, f"matched={matched_count}/{candidate_count} samples={sample_count} coverage={coverage}%")


def _cost_check(expected_cost_count: int, real_cost_count: int, cost_sample_count: int) -> dict[str, str]:
    if expected_cost_count <= 0:
        return _check("cost evidence coverage", READY, f"no simulated fee/rebate costs requiring wallet calibration; real_cost_events={real_cost_count}")
    if real_cost_count > 0 and cost_sample_count > 0:
        return _check("cost evidence coverage", READY, f"expected_cost_orders={expected_cost_count} real_cost_events={real_cost_count} cost_samples={cost_sample_count}")
    return _check("cost evidence coverage", REVIEW, f"expected_cost_orders={expected_cost_count} real_cost_events={real_cost_count} cost_samples={cost_sample_count}")


def _incident_check(incident_count: int) -> dict[str, str]:
    if incident_count > 0:
        return _check("platform incident coverage", READY, f"overlapping incidents={incident_count}")
    return _check("platform incident coverage", REVIEW, "no overlapping platform incident rows; this may be normal, but source coverage is unproven")


def _external_state_check(state_count: int) -> dict[str, str]:
    if state_count > 0:
        return _check("external source state", READY, f"import state rows={state_count}")
    return _check("external source state", REVIEW, "no external_source_import_state rows")


def _check(name: str, status: str, detail: str) -> dict[str, str]:
    return {"name": name, "status": status, "detail": detail}


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
    return bool(_text(order.get("order_id")))


def _expected_cost_count(orders: list[Mapping[str, Any]]) -> int:
    return sum(
        1
        for order in orders
        if any(_decimal(order.get(field)) != 0 for field in ("fee_cost", "rebate_cost"))
    )


def _coverage(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "0"
    value = Decimal(numerator) * Decimal("100") / Decimal(denominator)
    return _decimal_text(value)


def _aggregate(statuses: Any) -> str:
    values = set(statuses)
    if MISSING in values:
        return MISSING
    if REVIEW in values:
        return REVIEW
    return READY


def _reason(checks: list[Mapping[str, Any]], status: str) -> str:
    if status == READY:
        return "external evidence covers this run"
    for check in checks:
        if check.get("status") == status:
            return str(check.get("detail") or status)
    return status


def _next_actions(checks: list[Mapping[str, Any]]) -> list[str]:
    actions = [f"Review {check.get('name')}: {check.get('detail')}" for check in checks if check.get("status") != READY]
    return actions or ["Run-level external evidence coverage is ready for fill-first audit."]


def _fetch_one(conn: Any, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    return dict(row) if row else None


def _fetch_all(conn: Any, table: str, where_sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    if not _table_exists(conn, table):
        return []
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table} WHERE {where_sql}", params)
        return [dict(row) for row in cur.fetchall()]


def _table_exists(conn: Any, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (table,))
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


def _payload(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    text = format(value.quantize(Decimal("0.0001")), "f")
    text = text.rstrip("0").rstrip(".") if "." in text else text
    return text or "0"
