"""Focused fill/no-fill report for a persisted backtest run."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import json
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"


def build_backtest_fill_report(
    inputs: Mapping[str, Any] | None,
    *,
    run_id: int | None = None,
    max_orders: int = 50,
) -> dict[str, Any]:
    """Build a compact report centered on fill quality and no-fill evidence."""

    if inputs is None:
        return {
            "status": MISSING,
            "run_id": run_id,
            "reason": "run_not_found" if run_id is not None else "no_backtest_run_selected",
            "summary": {},
            "fill_quality": {},
            "no_fill": {},
            "evidence": {},
            "diagnostics": {},
            "missed_orders": [],
            "next_actions": ["Run or select a backtest run before exporting the fill report."],
        }

    run = dict(inputs.get("run") or {})
    meta = _json_dict(run.get("meta"))
    data_quality = _json_dict(meta.get("actual_data_quality"))
    fill_quality = _json_dict(data_quality.get("fill_quality")) or _json_dict(meta.get("fill_quality"))
    orders = [dict(order) for order in inputs.get("orders") or []]
    missed_orders = _missed_orders(orders, limit=max_orders)
    diagnostics = _diagnostics(fill_quality, data_quality, inputs, missed_orders)
    status = _report_status(fill_quality, diagnostics)

    return {
        "status": status,
        "run_id": _to_int(run.get("run_id")) or run_id,
        "summary": {
            "market_slug": run.get("market_slug"),
            "token_side": run.get("token_side"),
            "price_source": run.get("price_source"),
            "requested_engine": meta.get("requested_backtest_engine") or meta.get("backtest_engine") or run.get("backtest_engine"),
            "actual_engine": meta.get("actual_backtest_engine") or run.get("backtest_engine"),
            "rows_processed": _to_int(run.get("rows_processed")),
            "from_block": run.get("from_block"),
            "to_block": run.get("to_block"),
            "from_ts": run.get("from_ts"),
            "to_ts": run.get("to_ts"),
        },
        "fill_quality": _fill_quality_summary(fill_quality),
        "no_fill": _no_fill_summary(fill_quality),
        "evidence": _evidence_summary(fill_quality, data_quality, inputs),
        "diagnostics": diagnostics,
        "missed_orders": missed_orders,
        "next_actions": _next_actions(status, diagnostics),
    }


def backtest_fill_report_to_markdown(report: Mapping[str, Any]) -> str:
    """Render a fill report in a small human-readable Markdown form."""

    summary = _json_dict(report.get("summary"))
    fill = _json_dict(report.get("fill_quality"))
    no_fill = _json_dict(report.get("no_fill"))
    evidence = _json_dict(report.get("evidence"))
    diagnostics = _json_dict(report.get("diagnostics"))
    lines = [
        f"# Fill Report: {report.get('status')}",
        "",
        f"- run_id: {report.get('run_id') or '-'}",
        f"- market: {summary.get('market_slug') or '-'} / {summary.get('token_side') or '-'}",
        f"- source: {summary.get('price_source') or '-'}",
        f"- engine: requested={summary.get('requested_engine') or '-'} actual={summary.get('actual_engine') or '-'}",
        f"- window: block {summary.get('from_block') or '-'} -> {summary.get('to_block') or '-'}",
        "",
        "## Fill Quality",
        "",
        f"- submitted: {fill.get('submitted_count', 0)}",
        f"- filled: {fill.get('filled_count', 0)}",
        f"- partial: {fill.get('partial_fill_count', 0)}",
        f"- no_fill: {fill.get('no_fill_count', 0)}",
        f"- fill_rate: {fill.get('fill_rate') or '-'}",
        f"- expected_notional: {fill.get('expected_fill_notional') or '0'}",
        f"- actual_notional: {fill.get('actual_fill_notional') or '0'}",
        f"- avg_participation_rate: {fill.get('avg_participation_rate') or '-'}",
        "",
        "## No Fill",
        "",
        f"- missed_opportunity_count: {no_fill.get('missed_opportunity_count', 0)}",
        f"- missed_opportunity_notional_total: {no_fill.get('missed_opportunity_notional_total') or '0'}",
        f"- avg_missed_opportunity_price_move: {no_fill.get('avg_missed_opportunity_price_move') or '-'}",
    ]
    reasons = _json_dict(no_fill.get("reasons"))
    if reasons:
        lines.extend(["", "| Reason | Count | Missed Notional |", "| --- | ---: | ---: |"])
        missed_by_reason = _json_dict(no_fill.get("missed_opportunity_by_reason"))
        for reason, count in reasons.items():
            lines.append(f"| {reason} | {count} | {missed_by_reason.get(reason, '-')} |")

    lines.extend(
        [
            "",
            "## Evidence",
            "",
            f"- raw_event_count: {evidence.get('raw_event_count', 0)}",
            f"- candidate_event_count: {evidence.get('candidate_event_count', 0)}",
            f"- consumed_event_count: {evidence.get('consumed_event_count', 0)}",
            f"- raw_orderfilled_fill_count: {evidence.get('raw_orderfilled_fill_count', 0)}",
            f"- block_bar_synthetic_fill_count: {evidence.get('block_bar_synthetic_fill_count', 0)}",
            f"- raw_replay_fallback_suppressed_count: {evidence.get('raw_replay_fallback_suppressed_count', 0)}",
            f"- block_participation_discount_tick_count: {evidence.get('block_participation_discount_tick_count', 0)}",
            f"- max_requested_block_participation_pct: {evidence.get('max_requested_block_participation_pct', '0')}%",
            f"- min_block_participation_factor: {evidence.get('min_block_participation_factor', '1')}",
            f"- raw_replay_coverage_pct: {evidence.get('raw_replay_coverage_pct', '0')}%",
            f"- block_bar_fallback_pct: {evidence.get('block_bar_fallback_pct', '0')}%",
            f"- source_table: {evidence.get('source_table') or '-'}",
            f"- access_path: {evidence.get('access_path') or '-'}",
            f"- data_quality_status: {evidence.get('data_quality_status') or '-'}",
            f"- calibration_samples: {evidence.get('calibration_samples', 0)}",
            f"- cost_calibration_samples: {evidence.get('cost_calibration_samples', 0)}",
            f"- real_order_state_events: {evidence.get('real_order_state_events', 0)}",
            "",
            "## Diagnostics",
            "",
            f"- verdict: {diagnostics.get('verdict') or '-'}",
            f"- reasons: {', '.join(str(item) for item in diagnostics.get('reasons') or []) or '-'}",
        ]
    )

    missed_orders = list(report.get("missed_orders") or [])
    if missed_orders:
        lines.extend(["", "## Missed Orders", "", "| Order | Side | Role | Submit | Reason | Requested | Missed | Ticks | Side Disc. | Block Part. Disc. | Block BUY/SELL/UNK |", "| --- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |"])
        for order in missed_orders[:20]:
            lines.append(
                "| {order_id} | {side} | {role} | {submit_x} | {reason} | {requested_notional} | {missed_notional} | "
                "{ticks} | {side_discounted} | {participation_discounted} | {buy}/{sell}/{unknown} |".format(
                    order_id=order.get("order_id") or "-",
                    side=order.get("side") or "-",
                    role=order.get("role") or "-",
                    submit_x=order.get("submit_x") or "-",
                    reason=order.get("no_fill_reason") or "-",
                    requested_notional=order.get("requested_notional") or "0",
                    missed_notional=order.get("missed_notional") or "0",
                    ticks=order.get("fill_schedule_tick_count", 0),
                    side_discounted=order.get("side_discounted_tick_count", 0),
                    participation_discounted=order.get("block_participation_discount_tick_count", 0),
                    buy=order.get("block_buy_volume") or "0",
                    sell=order.get("block_sell_volume") or "0",
                    unknown=order.get("block_unknown_side_volume") or "0",
                )
            )

    next_actions = list(report.get("next_actions") or [])
    if next_actions:
        lines.extend(["", "## Next Actions"])
        lines.extend(f"- {action}" for action in next_actions)
    return "\n".join(lines)


def _fill_quality_summary(fill_quality: Mapping[str, Any]) -> dict[str, Any]:
    submitted = _to_decimal(fill_quality.get("submitted_count"))
    filled = _to_decimal(fill_quality.get("filled_count"))
    fill_rate = None
    if submitted > 0:
        fill_rate = _decimal_text((filled / submitted) * Decimal("100")) + "%"
    return {
        "signal_count": _to_int(fill_quality.get("signal_count")),
        "submitted_count": _to_int(fill_quality.get("submitted_count")),
        "filled_count": _to_int(fill_quality.get("filled_count")),
        "partial_fill_count": _to_int(fill_quality.get("partial_fill_count")),
        "no_fill_count": _to_int(fill_quality.get("no_fill_count")),
        "fill_rate": fill_rate,
        "expected_fill_size": str(fill_quality.get("expected_fill_size") or "0"),
        "actual_fill_size": str(fill_quality.get("actual_fill_size") or "0"),
        "expected_fill_notional": str(fill_quality.get("expected_fill_notional") or "0"),
        "actual_fill_notional": str(fill_quality.get("actual_fill_notional") or "0"),
        "avg_participation_rate": str(fill_quality.get("avg_participation_rate") or ""),
        "avg_markout_after_1_bars": str(fill_quality.get("avg_markout_after_1_bars") or ""),
    }


def _no_fill_summary(fill_quality: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "reasons": _json_dict(fill_quality.get("no_fill_reasons")),
        "missed_opportunity_count": _to_int(fill_quality.get("missed_opportunity_count")),
        "missed_opportunity_notional_total": str(fill_quality.get("missed_opportunity_notional_total") or "0"),
        "avg_missed_opportunity_notional": str(fill_quality.get("avg_missed_opportunity_notional") or ""),
        "max_missed_opportunity_notional": str(fill_quality.get("max_missed_opportunity_notional") or ""),
        "avg_missed_opportunity_price_move": str(fill_quality.get("avg_missed_opportunity_price_move") or ""),
        "missed_opportunity_by_reason": _json_dict(fill_quality.get("missed_opportunity_by_reason")),
        "missed_opportunity_buckets": _json_dict(fill_quality.get("missed_opportunity_buckets")),
        "missed_opportunity_notional_by_bucket": _json_dict(fill_quality.get("missed_opportunity_notional_by_bucket")),
    }


def _evidence_summary(
    fill_quality: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    raw_summary = _json_dict(fill_quality.get("raw_evidence_summary"))
    replay = _json_dict(data_quality.get("orderfilled_replay"))
    return {
        "raw_event_count": _to_int(fill_quality.get("raw_event_count")),
        "candidate_event_count": _to_int(fill_quality.get("candidate_event_count")),
        "consumed_event_count": _to_int(fill_quality.get("consumed_event_count")),
        "execution_evidence_counts": _json_dict(fill_quality.get("execution_evidence_counts")),
        "fill_evidence_counts": _json_dict(fill_quality.get("fill_evidence_counts")),
        "raw_orderfilled_fill_count": _to_int(fill_quality.get("raw_orderfilled_fill_count")),
        "block_bar_synthetic_fill_count": _to_int(fill_quality.get("block_bar_synthetic_fill_count")),
        "raw_replay_fallback_suppressed_count": _to_int(fill_quality.get("raw_replay_fallback_suppressed_count")),
        "block_participation_discount_tick_count": _to_int(fill_quality.get("block_participation_discount_tick_count")),
        "max_requested_block_participation_pct": str(fill_quality.get("max_requested_block_participation_pct") or "0"),
        "min_block_participation_factor": str(fill_quality.get("min_block_participation_factor") or "1"),
        "raw_replay_coverage_pct": str(fill_quality.get("raw_replay_coverage_pct") or "0"),
        "block_bar_fallback_pct": str(fill_quality.get("block_bar_fallback_pct") or "0"),
        "candidate_event_unique_count": _to_int(raw_summary.get("candidate_event_unique_count") or fill_quality.get("candidate_event_unique_count")),
        "candidate_event_duplicate_count": _to_int(raw_summary.get("candidate_event_duplicate_count") or fill_quality.get("candidate_event_duplicate_count")),
        "candidate_unique_notional": str(raw_summary.get("candidate_unique_notional") or "0"),
        "consumed_unique_notional": str(raw_summary.get("consumed_unique_notional") or "0"),
        "replay_source": replay.get("source"),
        "replay_fallback": replay.get("fallback"),
        "source_table": data_quality.get("source_table"),
        "access_path": data_quality.get("access_path"),
        "data_quality_status": data_quality.get("status"),
        "gap_count": _to_int(data_quality.get("gap_count")),
        "fallback_count": _to_int(data_quality.get("fallback_count")),
        "duplicate_count": _to_int(data_quality.get("duplicate_count")),
        "calibration_samples": _to_int(inputs.get("calibration_count")),
        "cost_calibration_samples": _to_int(inputs.get("cost_calibration_count")),
        "real_order_state_events": _to_int(inputs.get("real_order_state_event_count")),
        "external_source_states": _to_int(inputs.get("external_source_state_count")),
    }


def _diagnostics(
    fill_quality: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    inputs: Mapping[str, Any],
    missed_orders: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reasons: list[str] = []
    if not fill_quality:
        reasons.append("missing fill_quality artifact")
    if _to_int(fill_quality.get("submitted_count")) <= 0:
        reasons.append("no submitted orders")
    if _to_int(fill_quality.get("raw_event_count")) <= 0:
        reasons.append("no raw OrderFilled replay events")
    if _to_int(fill_quality.get("candidate_event_count")) <= 0 and _to_int(fill_quality.get("submitted_count")) > 0:
        reasons.append("submitted orders saw no candidate fill evidence")
    if _json_dict(fill_quality.get("environment_flags")):
        reasons.append("environment flags present")
    if _json_dict(fill_quality.get("order_anomaly_flags")):
        reasons.append("order anomaly flags present")
    if _to_int(inputs.get("calibration_count")) <= 0:
        reasons.append("missing live-vs-sim calibration samples")
    if _to_int(inputs.get("cost_calibration_count")) <= 0:
        reasons.append("missing real cost calibration samples")
    if str(data_quality.get("status") or "").lower() not in {"ready", "ok"}:
        reasons.append(f"data quality status is {data_quality.get('status') or 'unknown'}")
    if missed_orders and not _json_dict(fill_quality.get("no_fill_reasons")):
        reasons.append("missed orders exist but no_fill_reasons is empty")
    return {
        "verdict": READY if not reasons else REVIEW,
        "reasons": reasons,
        "environment_flags": _json_dict(fill_quality.get("environment_flags")),
        "order_anomaly_flags": _json_dict(fill_quality.get("order_anomaly_flags")),
    }


def _report_status(fill_quality: Mapping[str, Any], diagnostics: Mapping[str, Any]) -> str:
    if not fill_quality:
        return MISSING
    return READY if diagnostics.get("verdict") == READY else REVIEW


def _missed_orders(orders: Sequence[Mapping[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for order in orders:
        status = str(order.get("status") or "").upper()
        no_fill_reason = order.get("no_fill_reason") or order.get("noFillReason")
        if status not in {"NO_FILL", "REJECTED", "EXPIRED", "CANCELLED"} and not no_fill_reason:
            continue
        requested = _to_decimal(order.get("requested_notional"))
        actual = _to_decimal(order.get("actual_fill_notional") or order.get("filled_notional"))
        missed = requested - actual
        if missed < 0:
            missed = Decimal("0")
        meta = order.get("meta") if isinstance(order.get("meta"), Mapping) else {}
        rows.append(
            {
                "order_id": order.get("order_id"),
                "status": order.get("status"),
                "side": order.get("side"),
                "role": order.get("role"),
                "order_type": order.get("order_type"),
                "signal_x": order.get("signal_x"),
                "submit_x": order.get("submit_x"),
                "requested_price": str(order.get("requested_price") or ""),
                "requested_notional": _decimal_text(requested),
                "actual_fill_notional": _decimal_text(actual),
                "missed_notional": _decimal_text(missed),
                "no_fill_reason": no_fill_reason,
                "latency_blocks": order.get("latency_blocks"),
                "latency_seconds": str(order.get("latency_seconds") or ""),
                **_tick_evidence_summary(meta.get("fill_schedule")),
            }
        )
    rows.sort(key=lambda item: _to_decimal(item.get("missed_notional")), reverse=True)
    return rows[: max(0, int(limit))]


def _tick_evidence_summary(schedule: Any) -> dict[str, Any]:
    rows = schedule if isinstance(schedule, Sequence) and not isinstance(schedule, (str, bytes, bytearray)) else []
    tick_count = 0
    side_discounted = 0
    participation_discounted = 0
    max_requested_participation_pct = Decimal("0")
    min_participation_factor = Decimal("1")
    fillable_size = Decimal("0")
    block_contexts: dict[tuple[str, str, str], dict[str, Decimal]] = {}
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        tick_count += 1
        fillable_size += _to_decimal(item.get("fillable_size"))
        side = str(item.get("side_compatibility") or "")
        factor = _to_decimal(item.get("side_compatibility_factor"))
        if "incompatible" in side or (factor > 0 and factor < Decimal("1")):
            side_discounted += 1
        participation_factor = _to_decimal(item.get("block_participation_factor"))
        if participation_factor > 0 and participation_factor < Decimal("1"):
            participation_discounted += 1
            min_participation_factor = min(min_participation_factor, participation_factor)
            max_requested_participation_pct = max(max_requested_participation_pct, _to_decimal(item.get("requested_block_participation_pct")))
        block_key = (
            str(item.get("market_id") or ""),
            str(item.get("token_id") or ""),
            str(item.get("block_number") or ""),
        )
        block_contexts.setdefault(
            block_key,
            {
                "buy_volume": _to_decimal(item.get("block_buy_volume")),
                "sell_volume": _to_decimal(item.get("block_sell_volume")),
                "unknown_side_volume": _to_decimal(item.get("block_unknown_side_volume")),
            },
        )
    return {
        "fill_schedule_tick_count": tick_count,
        "fill_schedule_fillable_size": _decimal_text(fillable_size),
        "side_discounted_tick_count": side_discounted,
        "block_participation_discount_tick_count": participation_discounted,
        "max_requested_block_participation_pct": _decimal_text(max_requested_participation_pct),
        "min_block_participation_factor": _decimal_text(min_participation_factor if participation_discounted else Decimal("1")),
        "block_buy_volume": _decimal_text(sum((row["buy_volume"] for row in block_contexts.values()), Decimal("0"))),
        "block_sell_volume": _decimal_text(sum((row["sell_volume"] for row in block_contexts.values()), Decimal("0"))),
        "block_unknown_side_volume": _decimal_text(sum((row["unknown_side_volume"] for row in block_contexts.values()), Decimal("0"))),
    }


def _next_actions(status: str, diagnostics: Mapping[str, Any]) -> list[str]:
    if status == MISSING:
        return ["Rebuild or repair fill_quality for this run before using it in research."]
    reasons = [str(item) for item in diagnostics.get("reasons") or []]
    actions: list[str] = []
    if "missing live-vs-sim calibration samples" in reasons:
        actions.append("Import real order state events and run calibration sample builder.")
    if "missing real cost calibration samples" in reasons:
        actions.append("Import real cost events and rebuild cost calibration.")
    if "submitted orders saw no candidate fill evidence" in reasons:
        actions.append("Inspect token/block window and raw OrderFilled replay coverage.")
    if "environment flags present" in reasons:
        actions.append("Check overlapping platform incident timeline before tuning strategy parameters.")
    if not actions:
        actions.append("Fill report is structurally ready; use no-fill and markout buckets to tune execution assumptions.")
    return actions


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(loaded) if isinstance(loaded, Mapping) else {}
    return {}


def _to_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except Exception:
        try:
            return int(Decimal(str(value)))
        except Exception:
            return 0


def _to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")
