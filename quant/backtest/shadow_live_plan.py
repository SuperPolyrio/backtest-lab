"""Build shadow/live collection plans from persisted fill-first backtest orders."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
from typing import Any, Mapping, Sequence


PLAN_SCHEMA_VERSION = "fill_first_shadow_live_plan_v1"


def load_shadow_live_plan_inputs(conn: Any, *, run_id: int, limit: int = 5000) -> dict[str, Any] | None:
    run = _fetch_one(conn, "SELECT * FROM quant.quant_backtest_runs WHERE run_id = %s", (int(run_id),))
    if not run:
        return None
    parameters = _fetch_one(conn, "SELECT * FROM quant.quant_backtest_parameters WHERE run_id = %s", (int(run_id),))
    orders = _fetch_all(
        conn,
        """
        SELECT *
        FROM quant.quant_backtest_orders
        WHERE run_id = %s
        ORDER BY signal_index ASC, order_id ASC
        LIMIT %s
        """,
        (int(run_id), max(1, int(limit))),
    )
    return {"run": run, "parameters": parameters, "orders": orders}


def build_shadow_live_order_plan(
    inputs: Mapping[str, Any] | None,
    *,
    source: str = "live-shadow",
    include_rejected: bool = True,
    max_orders: int | None = None,
) -> dict[str, Any]:
    if inputs is None:
        return {
            "schema_version": PLAN_SCHEMA_VERSION,
            "status": "missing",
            "reason": "run_not_found",
            "order_count": 0,
            "orders": [],
            "event_templates": [],
        }
    run = dict(inputs.get("run") or {})
    parameters = dict(inputs.get("parameters") or {})
    raw_orders = list(inputs.get("orders") or [])
    orders = [
        _plan_order(row, run=run, parameters=parameters, source=source)
        for row in raw_orders
        if include_rejected or str(row.get("status") or "").upper() != "REJECTED"
    ]
    if max_orders is not None:
        orders = orders[: max(0, int(max_orders))]
    event_templates = [order["event_template"] for order in orders]
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "status": "ready" if orders else "review",
        "reason": "orders_exported" if orders else "no_orders_to_export",
        "run": {
            "run_id": run.get("run_id"),
            "market_slug": run.get("market_slug"),
            "token_side": run.get("token_side"),
            "price_source": run.get("price_source"),
            "backtest_engine": run.get("backtest_engine"),
            "from_block": run.get("from_block"),
            "to_block": run.get("to_block"),
            "from_ts": run.get("from_ts"),
            "to_ts": run.get("to_ts"),
            "parameter_fingerprint": _run_meta(run).get("parameter_fingerprint"),
        },
        "parameters": _compact_parameters(parameters),
        "source": source,
        "order_count": len(orders),
        "orders": orders,
        "event_templates": event_templates,
        "commands": {
            "import_real_order_state_events": "conda run -n polyBacktest python scripts/import_real_order_state_events.py --input <filled_event_templates.jsonl> --source "
            + source
            + " --run-id "
            + str(run.get("run_id") or "<run_id>"),
            "build_calibration_samples": "conda run -n polyBacktest python scripts/build_backtest_calibration_samples.py --run-id "
            + str(run.get("run_id") or "<run_id>")
            + " --event-source "
            + source,
        },
    }


def shadow_live_order_plan_to_markdown(plan: Mapping[str, Any]) -> str:
    run = plan.get("run") if isinstance(plan.get("run"), Mapping) else {}
    lines = [
        f"# Shadow/Live Collection Plan: {plan.get('status')}",
        "",
        f"- schema: {plan.get('schema_version')}",
        f"- run_id: {run.get('run_id')}",
        f"- market: {run.get('market_slug') or '-'}",
        f"- token_side: {run.get('token_side') or '-'}",
        f"- source: {plan.get('source') or '-'}",
        f"- orders: {plan.get('order_count', 0)}",
        "",
        "| order_id | status | side | role | requested_price | requested_size | submit_x | template_status |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for order in list(plan.get("orders") or [])[:50]:
        template = order.get("event_template") if isinstance(order.get("event_template"), Mapping) else {}
        lines.append(
            "| {order_id} | {status} | {side} | {role} | {price} | {size} | {submit_x} | {template_status} |".format(
                order_id=order.get("order_id") or "",
                status=order.get("simulated_status") or "",
                side=order.get("side") or "",
                role=order.get("role") or "",
                price=order.get("requested_price") or "",
                size=order.get("requested_size") or "",
                submit_x=order.get("submit_x") or "",
                template_status=template.get("api_order_status") or "",
            )
        )
    commands = plan.get("commands") if isinstance(plan.get("commands"), Mapping) else {}
    if commands:
        lines.extend(["", "## Commands"])
        for name, command in commands.items():
            lines.append(f"- {name}: `{command}`")
    return "\n".join(lines)


def shadow_live_event_templates_jsonl(plan: Mapping[str, Any]) -> str:
    return "\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str) for row in plan.get("event_templates") or [])


def _plan_order(order: Mapping[str, Any], *, run: Mapping[str, Any], parameters: Mapping[str, Any], source: str) -> dict[str, Any]:
    meta = _json_dict(order.get("meta"))
    token_id = meta.get("token_id") or meta.get("tokenId") or _run_meta(run).get("token_id")
    simulated_status = str(order.get("status") or "UNKNOWN").upper()
    client_order_id = _client_order_id(order, run=run)
    signal_time = _signal_time(order, meta=meta)
    signal_block = _block_axis_value(order.get("signal_block"), order.get("signal_x"), run=run)
    submit_block = _block_axis_value(order.get("submit_block"), order.get("submit_x"), run=run)
    intended_price = _text(order.get("requested_price") or order.get("decision_price"))
    intended_size = _text(order.get("requested_size"))
    decision_context = _decision_context(
        order,
        run=run,
        meta=meta,
        parameters=parameters,
        token_id=token_id,
        client_order_id=client_order_id,
        signal_time=signal_time,
        signal_block=signal_block,
        submit_block=submit_block,
        intended_price=intended_price,
        intended_size=intended_size,
    )
    event_template = {
        "run_id": run.get("run_id"),
        "order_id": order.get("order_id"),
        "client_order_id": client_order_id,
        "external_order_id": meta.get("external_order_id") or "",
        "market_slug": run.get("market_slug"),
        "token_id": token_id,
        "token_side": run.get("token_side"),
        "event_time": "",
        "event_type": "order_state",
        "source": source,
        "submit_status": "",
        "accepted_status": "",
        "cancel_status": "",
        "api_order_status": "",
        "chain_order_status": "",
        "clob_order_status": "",
        "submit_at": "",
        "accepted_at": "",
        "cancel_submitted_at": "",
        "cancel_accepted_at": "",
        "payload": {
            "live_status": "",
            "live_fill_price": "",
            "live_fill_size": "",
            "live_slippage": "",
            "live_fee": "",
            "live_rebate": "",
            "live_cash_delta": "",
            "live_position_delta": "",
            "live_latency_seconds": "",
            "simulated_order_id": order.get("order_id"),
            "client_order_id": client_order_id,
            "simulated_status": simulated_status,
            "simulated_requested_price": _text(order.get("requested_price") or order.get("decision_price")),
            "simulated_requested_size": _text(order.get("requested_size")),
            "simulated_submit_x": order.get("submit_x"),
            "signal_time": signal_time,
            "signal_block": signal_block,
            "submit_block": submit_block,
            "intended_price": intended_price,
            "intended_size": intended_size,
            "decision_context": decision_context,
        },
    }
    return {
        "order_id": order.get("order_id"),
        "client_order_id": client_order_id,
        "market_slug": run.get("market_slug"),
        "token_id": token_id,
        "token_side": run.get("token_side"),
        "side": order.get("side"),
        "role": order.get("role"),
        "order_type": order.get("order_type"),
        "simulated_status": simulated_status,
        "signal_time": signal_time,
        "signal_block": signal_block,
        "signal_x": order.get("signal_x"),
        "submit_block": submit_block,
        "submit_x": order.get("submit_x"),
        "decision_price": _text(order.get("decision_price")),
        "requested_price": _text(order.get("requested_price") or order.get("decision_price")),
        "requested_size": _text(order.get("requested_size")),
        "intended_price": intended_price,
        "intended_size": intended_size,
        "decision_context": decision_context,
        "requested_notional": _text(order.get("requested_notional")),
        "expected_fill_size": _text(order.get("expected_fill_size")),
        "expected_fill_notional": _text(order.get("expected_fill_notional")),
        "actual_fill_size": _text(order.get("actual_fill_size")),
        "actual_fill_notional": _text(order.get("actual_fill_notional")),
        "filled_size": _text(order.get("filled_size")),
        "filled_notional": _text(order.get("filled_notional")),
        "avg_fill_price": _text(order.get("avg_fill_price")),
        "fill_probability": _text(order.get("fill_probability")),
        "fill_pct": _text(order.get("fill_pct")),
        "participation_rate": _text(order.get("participation_rate")),
        "fee_cost": _text(order.get("fee_cost")),
        "rebate_cost": _text(order.get("rebate_cost")),
        "slippage_cost": _text(order.get("slippage_cost")),
        "execution_cost": _text(order.get("execution_cost")),
        "latency_seconds": _text(order.get("latency_seconds")),
        "no_fill_reason": order.get("no_fill_reason"),
        "execution_source": order.get("execution_source"),
        "event_template": event_template,
    }


def _client_order_id(order: Mapping[str, Any], *, run: Mapping[str, Any]) -> str:
    meta = _json_dict(order.get("meta"))
    for value in (
        order.get("client_order_id"),
        order.get("clientOrderId"),
        meta.get("client_order_id"),
        meta.get("clientOrderId"),
        order.get("order_id"),
    ):
        if value not in (None, ""):
            return str(value)
    return f"run-{run.get('run_id', 'unknown')}-order-unknown"


def _signal_time(order: Mapping[str, Any], *, meta: Mapping[str, Any]) -> str | None:
    strategy_intent = _json_dict(meta.get("strategy_intent"))
    signal = _json_dict(strategy_intent.get("signal"))
    signal_metadata = _json_dict(signal.get("metadata"))
    for value in (
        order.get("signal_time"),
        order.get("signal_ts"),
        order.get("signal_timestamp"),
        signal_metadata.get("signal_time"),
        signal_metadata.get("observed_at"),
        signal_metadata.get("timestamp"),
    ):
        if value not in (None, ""):
            return _text(_json_value(value))
    return None


def _block_axis_value(*values: Any, run: Mapping[str, Any]) -> int | None:
    if str(run.get("price_source") or "").strip() != "orderfilled_block_close":
        return None
    for value in values:
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _decision_context(
    order: Mapping[str, Any],
    *,
    run: Mapping[str, Any],
    meta: Mapping[str, Any],
    parameters: Mapping[str, Any],
    token_id: Any,
    client_order_id: str,
    signal_time: str | None,
    signal_block: int | None,
    submit_block: int | None,
    intended_price: str | None,
    intended_size: str | None,
) -> dict[str, Any]:
    run_meta = _run_meta(run)
    strategy_intent = _json_dict(meta.get("strategy_intent"))
    return {
        "client_order_id": client_order_id,
        "run_id": _json_value(run.get("run_id")),
        "market_slug": run.get("market_slug"),
        "token_id": token_id,
        "token_side": run.get("token_side"),
        "price_source": run.get("price_source"),
        "backtest_engine": run.get("backtest_engine"),
        "parameter_fingerprint": run_meta.get("parameter_fingerprint"),
        "signal_time": signal_time,
        "signal_x": _json_value(order.get("signal_x")),
        "signal_block": signal_block,
        "submit_x": _json_value(order.get("submit_x")),
        "submit_block": submit_block,
        "side": order.get("side"),
        "role": order.get("role"),
        "order_type": order.get("order_type"),
        "time_in_force": order.get("time_in_force"),
        "execution_source": order.get("execution_source"),
        "execution_price_mode": parameters.get("execution_price_mode"),
        "execution_profile": parameters.get("execution_profile"),
        "order_role": parameters.get("order_role"),
        "latency_blocks": _json_value(parameters.get("latency_blocks")),
        "latency_seconds": _json_value(parameters.get("latency_seconds")),
        "liquidity_cap_pct": _json_value(parameters.get("liquidity_cap_pct")),
        "fill_probability_haircut_pct": _json_value(parameters.get("fill_probability_haircut_pct")),
        "adverse_slippage_cents": _json_value(parameters.get("adverse_slippage_cents")),
        "fee_bps": _json_value(parameters.get("fee_bps")),
        "maker_fee_bps": _json_value(parameters.get("maker_fee_bps")),
        "taker_fee_bps": _json_value(parameters.get("taker_fee_bps")),
        "maker_rebate_bps": _json_value(parameters.get("maker_rebate_bps")),
        "slippage_bps": _json_value(parameters.get("slippage_bps")),
        "intended_price": intended_price,
        "intended_size": intended_size,
        "decision_price": _text(order.get("decision_price")),
        "requested_notional": _text(order.get("requested_notional")),
        "fill_probability": _text(order.get("fill_probability")),
        "participation_rate": _text(order.get("participation_rate")),
        "no_fill_reason": order.get("no_fill_reason"),
        "strategy_intent": strategy_intent or None,
    }


def _compact_parameters(parameters: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "execution_price_mode",
        "execution_profile",
        "order_role",
        "latency_blocks",
        "latency_seconds",
        "allow_partial_fill",
        "min_fill_size",
        "fee_bps",
        "maker_fee_bps",
        "taker_fee_bps",
        "maker_rebate_bps",
        "slippage_bps",
        "liquidity_cap_pct",
        "fill_probability_haircut_pct",
        "adverse_slippage_cents",
        "position_size",
        "initial_capital",
    )
    return {key: _json_value(parameters.get(key)) for key in keys if key in parameters}


def _fetch_one(conn: Any, query: str, params: Sequence[Any]) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    return dict(row) if row else None


def _fetch_all(conn: Any, query: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def _run_meta(run: Mapping[str, Any]) -> dict[str, Any]:
    return _json_dict(run.get("meta"))


def _json_dict(value: Any) -> dict[str, Any]:
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


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return value
