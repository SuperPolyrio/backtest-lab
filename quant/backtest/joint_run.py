"""Persistent joint-event fill-first backtest runs.

This module turns multiple already simulated outcome streams into one persisted
run by replaying their ORDER/FILL/LEDGER events in global x-order.  Native
joint runs stay OrderFilled-first by default, but can also consume historical
CLOB snapshots per outcome for DEPTH / ORDERFILLED_LOB execution.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal
import json
from typing import Any, Mapping, Sequence

from quant.backtest.backtest_engine import (
    BACKTEST_ARTIFACT_SCHEMA_VERSION,
    BACKTEST_STRATEGY_NAME,
    BACKTEST_STRATEGY_VERSION,
    OpenPosition,
    PricePoint,
    _code_provenance,
    _close_trade,
    _execution_context_from_payload,
    _exit_reason,
    _fill_decision,
    _filter_replay_events_after,
    _force_close_fill,
    _last_consumed_event_sequence,
    _mark_resting_limit_fill,
    _merge_resting_limit_fills,
    _settlement_fill,
    _settlement_value,
    _should_continue_resting_entry,
    _should_continue_resting_exit,
    backtest_parameter_fingerprint,
    backtest_parameter_snapshot,
    build_fill_quality_report,
    fill_quality_metrics,
    is_orderfilled_cross_mode,
    is_orderfilled_lob_mode,
    normalize_execution_price_mode,
    normalize_price_source,
    parse_parameters,
    replace_backtest_results,
    _limit_replay_fill,
    _replay_event_from_any,
)
from quant.backtest.event_stream import build_joint_replay_execution_report
from quant.backtest.frameworks import normalize_backtest_engine
from quant.backtest.ledger import build_ledger_rows
from quant.backtest.orders import next_order_id, order_from_fill
from quant.backtest.runners.execution_replay import replay_trade_event_dict
from quant.backtest.execution import BookSnapshot, snapshot_from_any


def create_and_execute_joint_backtest(conn: Any, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Create a persisted joint run from multiple outcome streams."""
    payload_dict = dict(payload or {})
    outcomes = _outcomes(payload_dict)
    if not outcomes:
        raise ValueError("outcomes is required for joint backtest run")

    run_id = create_joint_backtest_run(conn, payload_dict, outcomes)
    result = build_joint_backtest_result(payload_dict, outcomes)
    replace_backtest_results(conn, run_id, result)
    report = result["joint_execution_report"]
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_runs
            SET status = 'succeeded',
                rows_processed = %s,
                meta = meta || %s::jsonb,
                finished_at = now(),
                error = NULL
            WHERE run_id = %s
            RETURNING *
            """,
            (
                int(report.get("event_count") or 0),
                json.dumps(
                    {
                        "actual_data_quality": result["data_quality"],
                        "joint_execution_report": report,
                        "joint_replay_report": report.get("plan"),
                        "requested_backtest_engine": "builtin",
                        "actual_backtest_engine": "builtin",
                    },
                    default=str,
                ),
                run_id,
            ),
        )
        row = cur.fetchone()
    return dict(row) if row else {"run_id": run_id, "status": "succeeded"}


def create_joint_backtest_run(conn: Any, payload: Mapping[str, Any], outcomes: Sequence[Mapping[str, Any]]) -> int:
    params = parse_parameters(dict(payload))
    price_source = normalize_price_source(_get(payload, "price_source", "priceSource", default="orderfilled_block_close"))
    backtest_engine = normalize_backtest_engine(_get(payload, "backtest_engine", "backtestEngine", "engine", default="builtin"))
    run_mode = "native_joint_event_stream" if _native_joint_requested(payload, outcomes) else "joint_event_stream"
    market_slug = _joint_market_slug(payload, outcomes)
    token_side = "YES"
    from_block, to_block = _block_window(outcomes)
    execution_context = _execution_context_from_payload(
        {
            **dict(payload),
            "executionContext": {
                **(_get(payload, "execution_context", "executionContext", default={}) or {}),
                "run_mode": run_mode,
                "joint_outcome_count": len(outcomes),
                "native_joint_runner": run_mode == "native_joint_event_stream",
            },
        }
    )
    code = _code_provenance()
    snapshot = backtest_parameter_snapshot(
        market_slug=market_slug,
        token_side=token_side,
        token_id=None,
        outcome_label=str(_get(payload, "event_title", "eventTitle", "outcome_label", "outcomeLabel", default="Joint event run") or "Joint event run"),
        price_source=price_source,
        backtest_engine=backtest_engine,
        from_ts=None,
        to_ts=None,
        from_block=from_block,
        to_block=to_block,
        params=params,
        execution_context=execution_context,
    )
    fingerprint = backtest_parameter_fingerprint(snapshot)
    model_versions = {
        "strategy_version": BACKTEST_STRATEGY_VERSION,
        "fill_model": execution_context.get("fill_model"),
        "fill_model_version": execution_context.get("fill_model_version"),
        "fee_model_version": execution_context.get("fee_model_version"),
        "slippage_model_version": execution_context.get("slippage_model_version"),
        "execution_price_mode": params.execution_price_mode,
        "execution_profile": params.execution_profile,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_runs (
                status, market_slug, token_side, price_source, backtest_engine,
                from_ts, to_ts, from_block, to_block, meta, started_at
            )
            VALUES ('running', %s, %s, %s, %s, NULL, NULL, %s, %s, %s::jsonb, now())
            RETURNING run_id
            """,
            (
                market_slug,
                token_side,
                price_source,
                backtest_engine,
                from_block,
                to_block,
                json.dumps(
                    {
                        "strategy": BACKTEST_STRATEGY_VERSION,
                        "strategy_name": BACKTEST_STRATEGY_NAME,
                        "strategy_version": BACKTEST_STRATEGY_VERSION,
                        "backtest_engine": backtest_engine,
                        "run_mode": run_mode,
                        "native_joint_runner": run_mode == "native_joint_event_stream",
                        "artifact_schema_version": BACKTEST_ARTIFACT_SCHEMA_VERSION,
                        "code_commit": code.get("code_commit"),
                        "code_dirty": code.get("code_dirty"),
                        "code_source": code.get("code_source"),
                        "model_versions": model_versions,
                        "event_slug": _get(payload, "event_slug", "eventSlug"),
                        "event_title": _get(payload, "event_title", "eventTitle"),
                        "event_outcome_count": len(outcomes),
                        "outcome_label": _get(payload, "event_title", "eventTitle", default="Joint event run"),
                        "execution_context": execution_context,
                        "parameter_fingerprint": fingerprint,
                        "parameter_snapshot": snapshot,
                    },
                    default=str,
                ),
            ),
        )
        run_id = int(cur.fetchone()["run_id"])
        _insert_parameters(cur, run_id, params)
    return run_id


def build_joint_backtest_result(payload: Mapping[str, Any], outcomes: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    native_joint = _native_joint_requested(payload, outcomes)
    source_outcomes = build_native_joint_threshold_outcomes(payload, outcomes) if native_joint else list(outcomes)
    normalized_outcomes = [_normalize_outcome(outcome, index) for index, outcome in enumerate(source_outcomes, start=1)]
    event_slug = str(_get(payload, "event_slug", "eventSlug", default="") or "")
    if event_slug:
        for outcome in normalized_outcomes:
            if not outcome.get("event_slug"):
                outcome["event_slug"] = event_slug
    cashflow_ledger_rows = _cashflow_ledger_rows(_payload_cashflow_events(payload))
    report = build_joint_replay_execution_report(_outcomes_with_cashflow_ledger(normalized_outcomes, cashflow_ledger_rows))
    event_probability = _joint_event_probability_report(normalized_outcomes)
    report["event_probability_report"] = event_probability
    ledger = _joint_ledger_rows(
        normalized_outcomes,
        extra_rows=cashflow_ledger_rows,
    )
    orders = [order for outcome in normalized_outcomes for order in outcome["orders"]]
    trades = [trade for outcome in normalized_outcomes for trade in outcome["trades"]]
    equity = _equity_rows(report)
    metrics = _joint_metrics(report, orders, ledger)
    events = _event_rows(report)
    params = parse_parameters(dict(payload or {}))
    data_quality = {
        "status": report["status"],
        "source_table": "native_joint_event_stream" if native_joint else "joint_event_stream",
        "access_path": "order_fill_ledger_stream",
        "x_axis": "block_number",
        "rows": report["event_count"],
        "first_x": min((int(row["x_value"]) for row in report.get("timeline", []) or []), default=None),
        "last_x": max((int(row["x_value"]) for row in report.get("timeline", []) or []), default=None),
        "gap_count": 0,
        "jump_count": 0,
        "warning_level": "OK" if report["status"] == "ready" else "REVIEW",
        "data_version": f"joint:{report['schema_version']}:{report['event_count']}",
        "joint_execution_report": report,
        "joint_replay_report": report.get("plan"),
        "native_joint_runner": native_joint,
        "requested_execution_price_mode": params.execution_price_mode,
        "actual_execution_price_mode": _native_actual_execution_price_mode(params.execution_price_mode) if native_joint else params.execution_price_mode,
        "event_outcome_count": event_probability["outcome_count"],
        "event_probability_report": event_probability,
        "event_exposure_report": report.get("event_exposure_report"),
        "event_outcomes": event_probability["outcomes"],
    }
    fill_quality = build_fill_quality_report(
        orders,
        replay_context={
            "source": data_quality["source_table"],
            "native_joint_runner": native_joint,
            "outcome_count": len(normalized_outcomes),
        },
        data_quality_report=data_quality,
    )
    data_quality["fill_quality"] = fill_quality
    metrics.extend(fill_quality_metrics(fill_quality))
    return {
        "metrics": metrics,
        "equity": equity,
        "trades": trades,
        "orders": orders,
        "ledger": ledger,
        "events": events,
        "data_quality": data_quality,
        "fill_quality": fill_quality,
        "joint_execution_report": report,
    }


def build_native_joint_threshold_outcomes(payload: Mapping[str, Any], outcomes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Run a native multi-outcome threshold strategy on one global event stream.

    This stays OrderFilled-first by default.  When a native outcome also carries
    historical CLOB snapshots, DEPTH / ORDERFILLED_LOB execution can be applied
    per outcome without leaving the global joint replay path.
    """

    params = parse_parameters(dict(payload or {}))
    execution_params = _orderfilled_native_params(params)
    states: list[dict[str, Any]] = []
    global_points: list[tuple[int, int, int, PricePoint]] = []
    for outcome_index, outcome in enumerate(outcomes, start=1):
        outcome_key = str(_get(outcome, "outcome_key", "outcomeKey", "token_id", "tokenId", default=f"outcome-{outcome_index}"))
        market_slug = str(_get(outcome, "market_slug", "marketSlug", default=outcome_key) or outcome_key)
        token_side = str(_get(outcome, "token_side", "tokenSide", default="YES") or "YES").upper()
        points = _native_price_points(outcome)
        replay_events = _native_replay_events(outcome)
        clob_snapshots = _native_clob_snapshots(outcome)
        state = {
            "outcome_key": outcome_key,
            "market_slug": market_slug,
            "token_side": token_side,
            "points": points,
            "replay_events": replay_events,
            "clob_snapshots": clob_snapshots,
            "orders": [],
            "trades": [],
            "position": None,
            "pending_entry": None,
            "pending_exit": None,
            "order_index": 0,
        }
        states.append(state)
        for point_index, point in enumerate(points):
            global_points.append((int(point.x_value), outcome_index, point_index, point))

    global_points.sort(key=lambda item: (item[0], item[1], item[2]))
    for _, outcome_index, point_index, point in global_points:
        state = states[outcome_index - 1]
        if _native_continue_pending_entry(execution_params, state, point, point_index):
            continue
        if _native_continue_pending_exit(execution_params, state, point, point_index):
            continue
        position = state["position"]
        if position is None:
            if point.price < params.entry_threshold or point.price > params.max_entry_price:
                continue
            fill = _native_fill_decision(execution_params, state, point, "BUY_YES", signal_index=point_index + 1)
            if _should_continue_resting_entry(fill, execution_params, str(fill.get("order_role") or "maker")):
                state["pending_entry"] = {
                    "point": point,
                    "point_index": point_index,
                    "fill": _mark_resting_limit_fill(fill, x_value=int(point.x_value)),
                    "last_x": int(point.x_value),
                    "last_sequence": _last_consumed_event_sequence(fill),
                    "trade_id": f"T-{len(state['trades']) + 1:04d}",
                }
                continue
            order = _native_order_from_fill(state, point, point_index, fill, "BUY_YES", "native_joint_entry")
            state["orders"].append(order)
            if _decimal(fill.get("size")) <= 0:
                continue
            state["position"] = OpenPosition(
                trade_index=len(state["trades"]) + 1,
                entry_index=point_index,
                entry_x=point.x_value,
                entry_price=_decimal(fill.get("entry_price") or fill.get("avg_fill_price") or point.price),
                size=_decimal(fill.get("size")),
                requested_notional=_decimal(fill.get("requested_notional")),
                filled_notional=_decimal(fill.get("filled_notional")),
                fill_pct=_decimal(fill.get("fill_pct")),
                fill_status=str(fill.get("fill_status") or "FILLED"),
                book_snapshot_id=_optional_int(fill.get("book_snapshot_id")),
                snapshot_version=str(fill.get("snapshot_version") or "") or None,
                staleness_seconds=_optional_decimal(fill.get("staleness_seconds")),
                staleness_blocks=_optional_int(fill.get("staleness_blocks")),
                avg_fill_price=_optional_decimal(fill.get("avg_fill_price")),
                fill_probability=_decimal(fill.get("fill_probability")),
                block_volume=_decimal(fill.get("block_volume")),
                trade_count=int(_decimal(fill.get("trade_count"))),
                available_notional=_decimal(fill.get("available_notional")),
                entry_order_id=order["order_id"],
                entry_fee_cost=_decimal(fill.get("fee_cost")),
                entry_rebate=_decimal(fill.get("rebate") or fill.get("rebate_cost")),
                entry_slippage_cost=_decimal(fill.get("slippage_cost")),
            )
            continue

        exit_reason = _exit_reason(point.price, position.entry_price, point_index - position.entry_index, params)
        if not exit_reason or point.price < params.min_exit_price:
            continue
        fill = _native_fill_decision(execution_params, state, point, "SELL_YES", target_size=position.size, signal_index=point_index + 1)
        if _should_continue_resting_exit(fill, execution_params, str(fill.get("order_role") or "maker")):
            state["pending_exit"] = {
                "point": point,
                "point_index": point_index,
                "fill": _mark_resting_limit_fill(fill, x_value=int(point.x_value)),
                "last_x": int(point.x_value),
                "last_sequence": _last_consumed_event_sequence(fill),
                "trade_id": f"T-{position.trade_index:04d}",
                "exit_reason": exit_reason,
            }
            continue
        order = _native_order_from_fill(state, point, point_index, fill, "SELL_YES", "native_joint_exit", trade_id=f"T-{position.trade_index:04d}")
        state["orders"].append(order)
        if _decimal(fill.get("size")) <= 0:
            continue
        trade = _close_trade(
            _state_run(state),
            "block_number",
            position,
            point,
            point_index,
            exit_reason,
            execution_params,
            exit_fill=fill,
            exit_order_id=order["order_id"],
        )
        state["trades"].append(trade)
        remaining_size = max(Decimal("0"), position.size - _decimal(fill.get("size")))
        if remaining_size <= 0:
            state["position"] = None
        else:
            position.size = remaining_size
            position.trade_index = len(state["trades"]) + 1

    for state in states:
        _native_flush_pending_entry(execution_params, state)
        _native_flush_pending_exit(execution_params, state)
        position = state["position"]
        points = state["points"]
        if position is None or not points:
            continue
        last = points[-1]
        settlement_value = _settlement_value(params, points)
        if params.final_valuation_mode == "SETTLEMENT" and settlement_value is not None:
            fill = _settlement_fill(execution_params, last, position.size, settlement_value)
            reason = "settlement"
            order_type = "native_joint_settlement"
        else:
            fill = _force_close_fill(execution_params, last, position.size)
            reason = "end_of_data"
            order_type = "native_joint_force_close"
        order = _native_order_from_fill(state, last, len(points) - 1, fill, "SELL_YES", order_type, trade_id=f"T-{position.trade_index:04d}")
        state["orders"].append(order)
        if _decimal(fill.get("size")) > 0:
            state["trades"].append(
                _close_trade(
                    _state_run(state),
                    "block_number",
                    position,
                    last,
                    len(points) - 1,
                    reason,
                    execution_params,
                    exit_fill=fill,
                    exit_order_id=order["order_id"],
                )
            )
        state["position"] = None

    result: list[dict[str, Any]] = []
    for state in states:
        ledger = build_ledger_rows(
            state["trades"],
            Decimal("0"),
            gas_cost_per_order=params.gas_cost_per_order,
            settlement_cost=params.settlement_cost,
            redeem_cost=params.redeem_cost,
            capital_cost_bps=params.capital_cost_bps,
        )
        result.append(
            {
                "outcomeKey": state["outcome_key"],
                "marketSlug": state["market_slug"],
                "tokenSide": state["token_side"],
                "pricePoints": [
                    {
                        "x_axis": "block_number",
                        "x_value": point.x_value,
                        "price": point.price,
                        "volume": point.volume,
                        "trade_count": point.trade_count,
                    }
                    for point in state["points"]
                ],
                "orders": state["orders"],
                "trades": state["trades"],
                "ledger": ledger,
                "rawOrderfilledEvents": [
                    replay_trade_event_dict(event)
                    for event in state.get("replay_events", [])
                ],
                "nativeJointRun": True,
            }
        )
    return result


def _normalize_outcome(outcome: Mapping[str, Any], index: int) -> dict[str, Any]:
    outcome_key = str(_get(outcome, "outcome_key", "outcomeKey", "token_id", "tokenId", default=f"outcome-{index}"))
    market_slug = str(_get(outcome, "market_slug", "marketSlug", default="") or "")
    token_side = str(_get(outcome, "token_side", "tokenSide", default="YES") or "YES").upper()
    return {
        "outcome_key": outcome_key,
        "event_slug": str(_get(outcome, "event_slug", "eventSlug", default="") or ""),
        "market_slug": market_slug,
        "token_side": token_side,
        "price_points": list(_get(outcome, "price_points", "pricePoints", default=[]) or []),
        "orders": [_normalize_order(row, outcome_key, market_slug, token_side, offset) for offset, row in enumerate(_get(outcome, "orders", default=[]) or [], start=1)],
        "trades": [_normalize_trade(row, outcome_key, market_slug, token_side) for row in _get(outcome, "trades", default=[]) or []],
        "ledger": [_normalize_ledger(row, outcome_key, market_slug, token_side, offset) for offset, row in enumerate(_get(outcome, "ledger", default=[]) or [], start=1)],
        "raw_orderfilled_events": [
            _normalize_raw_orderfilled_event(row, outcome_key, market_slug, token_side)
            for row in _get(outcome, "raw_orderfilled_events", "rawOrderfilledEvents", "trade_ticks", "tradeTicks", default=[]) or []
            if isinstance(row, Mapping)
        ],
    }


def _normalize_order(row: Mapping[str, Any], outcome_key: str, market_slug: str, token_side: str, offset: int) -> dict[str, Any]:
    raw_order_id = str(_get(row, "order_id", "orderId", default=f"order-{offset}") or f"order-{offset}")
    raw_trade_id = str(_get(row, "trade_id", "tradeId", default="") or "")
    order_id = f"{outcome_key}:{raw_order_id}"
    trade_id = f"{outcome_key}:{raw_trade_id}" if raw_trade_id else None
    requested_size = _decimal(_get(row, "requested_size", "requestedSize"))
    filled_size = _decimal(_get(row, "filled_size", "filledSize"))
    requested_notional = _decimal(_get(row, "requested_notional", "requestedNotional"))
    filled_notional = _decimal(_get(row, "filled_notional", "filledNotional"))
    meta = {
        **(_get(row, "meta", default={}) or {}),
        "outcome_key": outcome_key,
        "market_slug": market_slug,
        "token_side": token_side,
        "source_order_id": raw_order_id,
    }
    for key in ("candidate_events", "candidateEvents", "consumed_events", "consumedEvents"):
        value = _get(row, key)
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            normalized_key = "candidate_events" if "candidate" in key.lower() else "consumed_events"
            meta[normalized_key] = [
                _normalize_raw_orderfilled_event(event, outcome_key, market_slug, token_side, order_id=raw_order_id, trade_id=raw_trade_id)
                for event in value
                if isinstance(event, Mapping)
            ]
    return {
        "order_id": order_id,
        "signal_index": int(_decimal(_get(row, "signal_index", "signalIndex", default=offset))),
        "trade_id": trade_id,
        "x_axis": str(_get(row, "x_axis", "xAxis", default="block_number") or "block_number"),
        "signal_x": int(_decimal(_get(row, "signal_x", "signalX", "submit_x", "submitX", default=0))),
        "submit_x": int(_decimal(_get(row, "submit_x", "submitX", "signal_x", "signalX", default=0))),
        "decision_price": _decimal(_get(row, "decision_price", "decisionPrice", "requested_price", "requestedPrice")),
        "requested_price": _optional_decimal(_get(row, "requested_price", "requestedPrice", "decision_price", "decisionPrice")),
        "side": str(_get(row, "side", default="BUY_YES") or "BUY_YES"),
        "role": str(_get(row, "role", default="maker") or "maker"),
        "order_type": str(_get(row, "order_type", "orderType", default="LIMIT") or "LIMIT"),
        "status": str(_get(row, "status", default="UNKNOWN") or "UNKNOWN"),
        "requested_size": requested_size,
        "requested_notional": requested_notional,
        "filled_size": filled_size,
        "filled_notional": filled_notional,
        "unfilled_size": _decimal(_get(row, "unfilled_size", "unfilledSize", default=max(Decimal("0"), requested_size - filled_size))),
        "avg_fill_price": _optional_decimal(_get(row, "avg_fill_price", "avgFillPrice", "requested_price", "requestedPrice")),
        "fill_probability": _decimal(_get(row, "fill_probability", "fillProbability")),
        "fill_pct": _decimal(_get(row, "fill_pct", "fillPct")),
        "block_volume": _decimal(_get(row, "block_volume", "blockVolume")),
        "trade_count": int(_decimal(_get(row, "trade_count", "tradeCount"))),
        "available_notional": _decimal(_get(row, "available_notional", "availableNotional")),
        "fee_cost": _decimal(_get(row, "fee_cost", "feeCost")),
        "rebate_cost": _decimal(_get(row, "rebate_cost", "rebateCost", "rebate")),
        "slippage_cost": _decimal(_get(row, "slippage_cost", "slippageCost")),
        "execution_cost": _decimal(_get(row, "execution_cost", "executionCost")),
        "latency_blocks": int(_decimal(_get(row, "latency_blocks", "latencyBlocks"))),
        "latency_seconds": _decimal(_get(row, "latency_seconds", "latencySeconds")),
        "no_fill_reason": _get(row, "no_fill_reason", "noFillReason"),
        "execution_source": str(_get(row, "execution_source", "executionSource", default="joint_event_stream") or "joint_event_stream"),
        "meta": meta,
    }


def _normalize_raw_orderfilled_event(
    row: Mapping[str, Any],
    outcome_key: str,
    market_slug: str,
    token_side: str,
    *,
    order_id: str | None = None,
    trade_id: str | None = None,
) -> dict[str, Any]:
    raw_order_id = _get(row, "order_id", "orderId", default=order_id)
    raw_trade_id = _get(row, "trade_id", "tradeId", default=trade_id)
    return {
        **dict(row),
        "market_slug": str(_get(row, "market_slug", "marketSlug", default=market_slug) or market_slug),
        "token_side": str(_get(row, "token_side", "tokenSide", default=token_side) or token_side),
        "order_id": _prefixed_id(outcome_key, raw_order_id),
        "trade_id": _prefixed_id(outcome_key, raw_trade_id),
        "source": str(_get(row, "source", default="orderfilled_fact") or "orderfilled_fact"),
    }


def _prefixed_id(outcome_key: str, value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    prefix = f"{outcome_key}:"
    return text if text.startswith(prefix) else f"{prefix}{text}"


def _normalize_trade(row: Mapping[str, Any], outcome_key: str, market_slug: str, token_side: str) -> dict[str, Any]:
    raw_trade_id = str(_get(row, "trade_id", "tradeId", default="trade") or "trade")
    raw_entry_order_id = _get(row, "entry_order_id", "entryOrderId")
    raw_exit_order_id = _get(row, "exit_order_id", "exitOrderId")
    return {
        "trade_id": f"{outcome_key}:{raw_trade_id}",
        "entry_order_id": f"{outcome_key}:{raw_entry_order_id}" if raw_entry_order_id else None,
        "exit_order_id": f"{outcome_key}:{raw_exit_order_id}" if raw_exit_order_id else None,
        "market_slug": market_slug or str(_get(row, "market_slug", "marketSlug", default="") or ""),
        "token_side": token_side or str(_get(row, "token_side", "tokenSide", default="YES") or "YES").upper(),
        "side": str(_get(row, "side", default="LONG") or "LONG"),
        "x_axis": str(_get(row, "x_axis", "xAxis", default="block_number") or "block_number"),
        "entry_x": int(_decimal(_get(row, "entry_x", "entryX"))),
        "exit_x": int(_decimal(_get(row, "exit_x", "exitX"))),
        "entry_price": _decimal(_get(row, "entry_price", "entryPrice")),
        "exit_price": _decimal(_get(row, "exit_price", "exitPrice")),
        "size": _decimal(_get(row, "size")),
        "notional": _decimal(_get(row, "notional")),
        "requested_notional": _decimal(_get(row, "requested_notional", "requestedNotional", "notional")),
        "filled_notional": _decimal(_get(row, "filled_notional", "filledNotional", "notional")),
        "fill_pct": _decimal(_get(row, "fill_pct", "fillPct", default=100)),
        "requested_size": _decimal(_get(row, "requested_size", "requestedSize", "size")),
        "filled_size": _decimal(_get(row, "filled_size", "filledSize", "size")),
        "unfilled_size": _decimal(_get(row, "unfilled_size", "unfilledSize")),
        "fill_status": str(_get(row, "fill_status", "fillStatus", default="FILLED") or "FILLED"),
        "book_snapshot_id": _get(row, "book_snapshot_id", "bookSnapshotId"),
        "snapshot_version": _get(row, "snapshot_version", "snapshotVersion"),
        "staleness_seconds": _optional_decimal(_get(row, "staleness_seconds", "stalenessSeconds")),
        "staleness_blocks": _get(row, "staleness_blocks", "stalenessBlocks"),
        "avg_fill_price": _optional_decimal(_get(row, "avg_fill_price", "avgFillPrice", "exit_price", "exitPrice")),
        "fill_probability": _decimal(_get(row, "fill_probability", "fillProbability")),
        "block_volume": _decimal(_get(row, "block_volume", "blockVolume")),
        "trade_count": int(_decimal(_get(row, "trade_count", "tradeCount"))),
        "available_notional": _decimal(_get(row, "available_notional", "availableNotional")),
        "execution_source": str(_get(row, "execution_source", "executionSource", default="native_joint_event_stream") or "native_joint_event_stream"),
        "fee_cost": _decimal(_get(row, "fee_cost", "feeCost")),
        "rebate": _decimal(_get(row, "rebate", "rebate_cost", "rebateCost")),
        "slippage_cost": _decimal(_get(row, "slippage_cost", "slippageCost")),
        "execution_cost": _decimal(_get(row, "execution_cost", "executionCost")),
        "pnl": _decimal(_get(row, "pnl")),
        "pnl_pct": _decimal(_get(row, "pnl_pct", "pnlPct")),
        "holding_bars": int(_decimal(_get(row, "holding_bars", "holdingBars", default=1))),
        "exit_reason": str(_get(row, "exit_reason", "exitReason", default="unknown") or "unknown"),
    }


def _normalize_ledger(row: Mapping[str, Any], outcome_key: str, market_slug: str, token_side: str, offset: int) -> dict[str, Any]:
    raw_id = str(_get(row, "ledger_id", "ledgerId", default=f"ledger-{offset}") or f"ledger-{offset}")
    raw_order_id = _get(row, "order_id", "orderId")
    raw_trade_id = _get(row, "trade_id", "tradeId")
    return {
        "ledger_id": f"{outcome_key}:{raw_id}",
        "order_id": f"{outcome_key}:{raw_order_id}" if raw_order_id else None,
        "trade_id": f"{outcome_key}:{raw_trade_id}" if raw_trade_id else None,
        "event_type": str(_get(row, "event_type", "eventType", default="LEDGER") or "LEDGER").upper(),
        "x_axis": str(_get(row, "x_axis", "xAxis", default="block_number") or "block_number"),
        "x_value": int(_decimal(_get(row, "x_value", "xValue", default=0))),
        "market_slug": market_slug,
        "token_side": token_side,
        "shares_delta": _decimal(_get(row, "shares_delta", "sharesDelta")),
        "cash_delta": _decimal(_get(row, "cash_delta", "cashDelta")),
        "fee": _decimal(_get(row, "fee")),
        "rebate": _decimal(_get(row, "rebate")),
        "slippage_cost": _decimal(_get(row, "slippage_cost", "slippageCost")),
        "execution_cost": _decimal(_get(row, "execution_cost", "executionCost")),
        "realized_pnl": _decimal(_get(row, "realized_pnl", "realizedPnl")),
        "position_after": _decimal(_get(row, "position_after", "positionAfter")),
        "cash_after": _decimal(_get(row, "cash_after", "cashAfter")),
        "price": _optional_decimal(_get(row, "price")),
        "source": str(_get(row, "source", default="joint_event_stream") or "joint_event_stream"),
        "meta": {
            **(_get(row, "meta", default={}) or {}),
            "outcome_key": outcome_key,
            "source_ledger_id": raw_id,
        },
    }


def _native_joint_requested(payload: Mapping[str, Any], outcomes: Sequence[Mapping[str, Any]]) -> bool:
    mode = str(_get(payload, "run_mode", "runMode", "execution_mode", "executionMode", default="") or "").lower()
    if mode in {"native_joint_event_stream", "native_joint", "joint_native"}:
        return True
    native = _get(payload, "native_joint_run", "nativeJointRun")
    if isinstance(native, bool):
        return native
    if str(native or "").lower() in {"1", "true", "yes"}:
        return True
    if outcomes and all(not (_get(outcome, "orders", default=[]) or _get(outcome, "ledger", default=[])) for outcome in outcomes):
        return True
    return False


def _orderfilled_native_params(params: Any) -> Any:
    mode = normalize_execution_price_mode(getattr(params, "execution_price_mode", "ORDERFILLED"), "ORDERFILLED")
    if mode in {"ORDERFILLED", "DEPTH"} or is_orderfilled_cross_mode(mode) or is_orderfilled_lob_mode(mode):
        return params
    return replace(params, execution_price_mode="ORDERFILLED")


def _native_actual_execution_price_mode(mode: Any) -> str:
    normalized = normalize_execution_price_mode(mode, "ORDERFILLED")
    return normalized


def _native_price_points(outcome: Mapping[str, Any]) -> list[PricePoint]:
    points: list[PricePoint] = []
    for row in _get(outcome, "price_points", "pricePoints", default=[]) or []:
        if not isinstance(row, Mapping):
            continue
        x_value = _get(row, "x_value", "xValue", "block_number", "blockNumber", "timestamp")
        price = _get(row, "price", "close", "yes_probability_close", "yesProbabilityClose", "latest_price", "latestPrice")
        if x_value in (None, "") or price in (None, ""):
            continue
        points.append(
            PricePoint(
                x_value=int(_decimal(x_value)),
                price=_decimal(price),
                volume=_decimal(_get(row, "volume", "block_volume", "blockVolume", "notional", default=0)),
                trade_count=int(_decimal(_get(row, "trade_count", "tradeCount", default=0))),
                timestamp=_optional_datetime(_get(row, "timestamp", "ts", "time", "datetime")),
                open_price=_optional_decimal(_get(row, "open_price", "openPrice", "open")),
                high_price=_optional_decimal(_get(row, "high_price", "highPrice", "high")),
                low_price=_optional_decimal(_get(row, "low_price", "lowPrice", "low")),
                close_price=_optional_decimal(_get(row, "close_price", "closePrice", "close")),
                vwap_price=_optional_decimal(_get(row, "vwap_price", "vwapPrice", "vwap")),
                buy_volume=_decimal(_get(row, "buy_volume", "buyVolume", default=0)),
                sell_volume=_decimal(_get(row, "sell_volume", "sellVolume", default=0)),
                first_log_index=_optional_int(_get(row, "first_log_index", "firstLogIndex")),
                last_log_index=_optional_int(_get(row, "last_log_index", "lastLogIndex")),
            )
        )
    points.sort(key=lambda point: int(point.x_value))
    return points


def _native_clob_snapshots(outcome: Mapping[str, Any]) -> list[BookSnapshot]:
    rows = _get(
        outcome,
        "clob_snapshots",
        "clobSnapshots",
        "book_snapshots",
        "bookSnapshots",
        "lob_snapshots",
        "lobSnapshots",
        default=[],
    ) or []
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return []
    snapshots: list[BookSnapshot] = []
    for row in rows:
        snapshot = snapshot_from_any(dict(row) if isinstance(row, Mapping) else row)
        if snapshot is not None:
            snapshots.append(snapshot)
    snapshots.sort(
        key=lambda item: (
            item.timestamp.isoformat() if item.timestamp is not None else "",
            int(item.block_number or -1),
            int(item.snapshot_id),
        )
    )
    return snapshots


def _native_replay_events(outcome: Mapping[str, Any]) -> list[Any]:
    rows = _get(
        outcome,
        "raw_orderfilled_events",
        "rawOrderfilledEvents",
        "trade_ticks",
        "tradeTicks",
        default=[],
    ) or []
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return []
    events = []
    for row in rows:
        event = _replay_event_from_any(dict(row) if isinstance(row, Mapping) else row)
        if event is not None:
            events.append(event)
    events.sort(key=lambda event: event.event_sequence)
    return events


def _native_fill_decision(
    params: Any,
    state: Mapping[str, Any],
    point: PricePoint,
    side: str,
    *,
    target_size: Decimal | None = None,
    signal_index: int | None = None,
) -> dict[str, Any]:
    mode = normalize_execution_price_mode(getattr(params, "execution_price_mode", "ORDERFILLED"), "ORDERFILLED")
    if is_orderfilled_cross_mode(mode):
        fill = _limit_replay_fill(
            params,
            point,
            side,
            limit_price=point.price,
            decision_x=int(point.x_value),
            signal_index=signal_index,
            submit_x=int(point.x_value),
            target_size=target_size,
            x_axis="block_number",
            replay_events=list(state.get("replay_events") or []),
        )
        fill["execution_source"] = f"native_joint_event_stream:{fill.get('execution_source') or 'orderfilled_limit_replay'}"
        fill["native_joint_runner"] = True
        fill["outcome_key"] = state["outcome_key"]
        return fill
    fill = _fill_decision(params, point, _state_run(state), side, target_size=target_size)
    meta = dict(fill)
    meta["execution_source"] = f"native_joint_event_stream:{fill.get('execution_source') or 'orderfilled_volume'}"
    meta["native_joint_runner"] = True
    meta["outcome_key"] = state["outcome_key"]
    return meta


def _native_continue_pending_entry(params: Any, state: dict[str, Any], point: PricePoint, point_index: int) -> bool:
    pending = state.get("pending_entry")
    if not pending:
        return False
    if int(point.x_value) <= int(pending.get("last_x") or 0):
        return True
    pending_fill = dict(pending["fill"])
    remaining_size = max(Decimal("0"), _decimal(pending_fill.get("requested_size")) - _decimal(pending_fill.get("size")))
    if remaining_size > 0:
        replay_events = list(state.get("replay_events") or [])
        continuation = _native_fill_decision(
            params,
            {**state, "replay_events": _filter_replay_events_after(replay_events, pending.get("last_sequence"))},
            point,
            "BUY_YES",
            target_size=remaining_size,
            signal_index=int(pending.get("point_index") or 0) + 1,
        )
        if _decimal(continuation.get("size")) > 0:
            pending_fill = _merge_resting_limit_fills(pending_fill, continuation, x_value=int(point.x_value))
            pending["fill"] = pending_fill
            pending["last_x"] = int(point.x_value)
            pending["last_sequence"] = _last_consumed_event_sequence(continuation) or pending.get("last_sequence")
    if _decimal(pending_fill.get("unfilled_size")) > Decimal("0.0000000001"):
        return True
    original_point = pending["point"]
    original_index = int(pending.get("point_index") or point_index)
    order = _native_order_from_fill(state, original_point, original_index, pending_fill, "BUY_YES", "native_joint_entry", trade_id=pending.get("trade_id"))
    state["orders"].append(order)
    state["position"] = _native_open_position_from_fill(
        state,
        pending_fill,
        point=point,
        point_index=point_index,
        order_id=order["order_id"],
    )
    state["pending_entry"] = None
    return True


def _native_continue_pending_exit(params: Any, state: dict[str, Any], point: PricePoint, point_index: int) -> bool:
    pending = state.get("pending_exit")
    position = state.get("position")
    if not pending or position is None:
        return False
    if int(point.x_value) <= int(pending.get("last_x") or 0):
        return True
    pending_fill = dict(pending["fill"])
    remaining_size = max(Decimal("0"), _decimal(pending_fill.get("requested_size")) - _decimal(pending_fill.get("size")))
    if remaining_size > 0:
        replay_events = list(state.get("replay_events") or [])
        continuation = _native_fill_decision(
            params,
            {**state, "replay_events": _filter_replay_events_after(replay_events, pending.get("last_sequence"))},
            point,
            "SELL_YES",
            target_size=remaining_size,
            signal_index=int(pending.get("point_index") or point_index) + 1,
        )
        if _decimal(continuation.get("size")) > 0:
            pending_fill = _merge_resting_limit_fills(pending_fill, continuation, x_value=int(point.x_value))
            pending["fill"] = pending_fill
            pending["last_x"] = int(point.x_value)
            pending["last_sequence"] = _last_consumed_event_sequence(continuation) or pending.get("last_sequence")
    if _decimal(pending_fill.get("unfilled_size")) > Decimal("0.0000000001"):
        return True
    original_point = pending["point"]
    original_index = int(pending.get("point_index") or point_index)
    order = _native_order_from_fill(state, original_point, original_index, pending_fill, "SELL_YES", "native_joint_exit", trade_id=pending.get("trade_id"))
    state["orders"].append(order)
    trade = _close_trade(
        _state_run(state),
        "block_number",
        position,
        point,
        point_index,
        str(pending.get("exit_reason") or "limit_exit"),
        params,
        exit_fill=pending_fill,
        exit_order_id=order["order_id"],
    )
    state["trades"].append(trade)
    state["position"] = None
    state["pending_exit"] = None
    return True


def _native_flush_pending_entry(params: Any, state: dict[str, Any]) -> None:
    pending = state.get("pending_entry")
    if not pending:
        return
    pending_fill = dict(pending["fill"])
    if _decimal(pending_fill.get("size")) <= 0:
        state["pending_entry"] = None
        return
    point = pending["point"]
    point_index = int(pending.get("point_index") or 0)
    order = _native_order_from_fill(state, point, point_index, pending_fill, "BUY_YES", "native_joint_entry", trade_id=pending.get("trade_id"))
    state["orders"].append(order)
    state["position"] = _native_open_position_from_fill(
        state,
        pending_fill,
        point=point,
        point_index=point_index,
        order_id=order["order_id"],
    )
    state["pending_entry"] = None


def _native_flush_pending_exit(params: Any, state: dict[str, Any]) -> None:
    pending = state.get("pending_exit")
    position = state.get("position")
    if not pending or position is None:
        state["pending_exit"] = None
        return
    pending_fill = dict(pending["fill"])
    if _decimal(pending_fill.get("size")) <= 0:
        state["pending_exit"] = None
        return
    point = pending["point"]
    point_index = int(pending.get("point_index") or 0)
    order = _native_order_from_fill(state, point, point_index, pending_fill, "SELL_YES", "native_joint_exit", trade_id=pending.get("trade_id"))
    state["orders"].append(order)
    trade = _close_trade(
        _state_run(state),
        "block_number",
        position,
        point,
        point_index,
        str(pending.get("exit_reason") or "limit_exit"),
        params,
        exit_fill=pending_fill,
        exit_order_id=order["order_id"],
    )
    state["trades"].append(trade)
    remaining_size = max(Decimal("0"), position.size - _decimal(pending_fill.get("size")))
    state["position"] = position if remaining_size > Decimal("0.0000000001") else None
    if state["position"] is not None:
        state["position"].size = remaining_size
    state["pending_exit"] = None


def _native_open_position_from_fill(
    state: dict[str, Any],
    fill: Mapping[str, Any],
    *,
    point: PricePoint,
    point_index: int,
    order_id: str,
) -> OpenPosition:
    return OpenPosition(
        trade_index=len(state["trades"]) + 1,
        entry_index=point_index,
        entry_x=point.x_value,
        entry_price=_decimal(fill.get("entry_price") or fill.get("avg_fill_price") or point.price),
        size=_decimal(fill.get("size")),
        requested_notional=_decimal(fill.get("requested_notional")),
        filled_notional=_decimal(fill.get("filled_notional")),
        fill_pct=_decimal(fill.get("fill_pct")),
        fill_status=str(fill.get("fill_status") or "FILLED"),
        book_snapshot_id=_optional_int(fill.get("book_snapshot_id")),
        snapshot_version=str(fill.get("snapshot_version") or "") or None,
        staleness_seconds=_optional_decimal(fill.get("staleness_seconds")),
        staleness_blocks=_optional_int(fill.get("staleness_blocks")),
        avg_fill_price=_optional_decimal(fill.get("avg_fill_price")),
        fill_probability=_decimal(fill.get("fill_probability")),
        block_volume=_decimal(fill.get("block_volume")),
        trade_count=int(_decimal(fill.get("trade_count"))),
        available_notional=_decimal(fill.get("available_notional")),
        entry_order_id=order_id,
        entry_fee_cost=_decimal(fill.get("fee_cost")),
        entry_rebate=_decimal(fill.get("rebate") or fill.get("rebate_cost")),
        entry_slippage_cost=_decimal(fill.get("slippage_cost")),
    )


def _native_order_from_fill(
    state: dict[str, Any],
    point: PricePoint,
    point_index: int,
    fill: Mapping[str, Any],
    side: str,
    order_type: str,
    *,
    trade_id: str | None = None,
) -> dict[str, Any]:
    state["order_index"] = int(state.get("order_index") or 0) + 1
    return order_from_fill(
        order_id=next_order_id(state["order_index"]),
        signal_index=point_index + 1,
        x_axis="block_number",
        x_value=point.x_value,
        side=side,
        role=str(fill.get("order_role") or "maker"),
        order_type=order_type,
        decision_price=point.price,
        fill=dict(fill),
        trade_id=trade_id if _decimal(fill.get("size")) > 0 else None,
        latency_seconds=_decimal(fill.get("latency_seconds")),
        latency_blocks=int(_decimal(fill.get("latency_blocks"))),
    )


def _state_run(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "market_slug": str(state.get("market_slug") or ""),
        "token_side": str(state.get("token_side") or "YES"),
        "price_source": "orderfilled_block_close",
        "_orderfilled_replay_events": list(state.get("replay_events") or []),
        "_clob_snapshots": [snapshot for snapshot in state.get("clob_snapshots") or [] if snapshot is not None],
    }


def _joint_ledger_rows(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    extra_rows: Sequence[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    rows = [dict(row) for outcome in outcomes for row in outcome["ledger"]]
    rows.extend(dict(row) for row in extra_rows or [])
    rows.sort(key=lambda row: (int(row.get("x_value") or 0), str(row.get("ledger_id") or "")))
    cash = Decimal("0")
    positions: dict[str, Decimal] = {}
    for row in rows:
        key = str(row.get("meta", {}).get("outcome_key") or row.get("market_slug") or "outcome")
        cash += _decimal(row.get("cash_delta"))
        positions[key] = positions.get(key, Decimal("0")) + _decimal(row.get("shares_delta"))
        row["cash_after"] = cash
        row["position_after"] = positions[key]
    return rows


def _payload_cashflow_events(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = (
        _get(payload, "cashflow_events", "cashflowEvents")
        or _get(payload, "polymarket_activity", "polymarketActivity")
        or _get(payload, "activity_events", "activityEvents")
        or []
    )
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _cashflow_ledger_rows(cashflow_events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not cashflow_events:
        return []
    return build_ledger_rows([], Decimal("0"), cashflow_events=cashflow_events)


def _outcomes_with_cashflow_ledger(
    outcomes: Sequence[Mapping[str, Any]],
    cashflow_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not cashflow_rows:
        return [dict(outcome) for outcome in outcomes]

    merged: list[dict[str, Any]] = []
    unmatched = [dict(row) for row in cashflow_rows]
    for outcome in outcomes:
        row = dict(outcome)
        market_slug = str(row.get("market_slug") or row.get("marketSlug") or "")
        token_side = str(row.get("token_side") or row.get("tokenSide") or "")
        matched: list[dict[str, Any]] = []
        remaining: list[dict[str, Any]] = []
        for cashflow in unmatched:
            same_market = not cashflow.get("market_slug") or str(cashflow.get("market_slug")) == market_slug
            same_token_side = not cashflow.get("token_side") or str(cashflow.get("token_side")) == token_side
            if same_market and same_token_side:
                matched.append(cashflow)
            else:
                remaining.append(cashflow)
        unmatched = remaining
        if matched:
            row["ledger"] = list(row.get("ledger") or []) + matched
        merged.append(row)

    if unmatched:
        merged.append(
            {
                "outcome_key": "__account_cashflow__",
                "market_slug": "",
                "token_side": "",
                "price_points": [],
                "orders": [],
                "trades": [],
                "ledger": unmatched,
                "raw_orderfilled_events": [],
            }
        )
    return merged


def _equity_rows(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    timeline = list(report.get("equity_curve") or report.get("timeline") or [])
    if not timeline:
        return [
            {
                "point_index": 1,
                "x_axis": "block_number",
                "x_value": 0,
                "equity": _decimal(report.get("portfolio_equity")),
                "drawdown": Decimal("0"),
                "drawdown_pct": Decimal("0"),
                "cumulative_return": Decimal("0"),
            }
        ]
    peak = Decimal("-999999999999999")
    initial = _decimal(timeline[0].get("portfolio_equity"))
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(timeline, start=1):
        equity = _decimal(item.get("portfolio_equity"))
        peak = max(peak, equity)
        drawdown = max(Decimal("0"), peak - equity)
        drawdown_pct = Decimal("0") if peak == 0 else drawdown / abs(peak) * Decimal("100")
        cumulative = Decimal("0") if initial == 0 else (equity - initial) / abs(initial) * Decimal("100")
        rows.append(
            {
                "point_index": index,
                "x_axis": "block_number",
                "x_value": int(_decimal(item.get("x_value"))),
                "equity": equity,
                "drawdown": drawdown,
                "drawdown_pct": drawdown_pct,
                "cumulative_return": cumulative,
            }
        )
    return rows


def _event_rows(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "event_type": str(row.get("event_type") or "JOINT_EVENT"),
            "x_axis": "block_number",
            "x_value": int(_decimal(row.get("x_value"))),
            "trade_id": row.get("trade_id") or None,
            "price": Decimal("0"),
            "message": f"joint {row.get('event_type') or 'event'}",
            "meta": dict(row),
        }
        for row in report.get("timeline", []) or []
    ]


def _joint_metrics(report: Mapping[str, Any], orders: Sequence[Mapping[str, Any]], ledger: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    filled = sum(1 for row in orders if _decimal(row.get("filled_size")) > 0)
    submitted = len(orders)
    fill_rate = Decimal("0") if submitted == 0 else Decimal(filled) / Decimal(submitted) * Decimal("100")
    probability = report.get("event_probability_report") if isinstance(report.get("event_probability_report"), Mapping) else {}
    exposure = report.get("event_exposure_report") if isinstance(report.get("event_exposure_report"), Mapping) else {}
    probability_sum = _decimal(probability.get("probability_sum"))
    probability_gap = _decimal(probability.get("probability_gap"))
    return [
        _metric("joint_execution_status", "Joint execution status", 0, str(report.get("status") or "unknown"), "neutral", 1),
        _metric("joint_execution_event_count", "Joint events", _decimal(report.get("event_count")), str(report.get("event_count") or 0), "neutral", 2),
        _metric("joint_submitted_orders", "Joint submitted orders", Decimal(submitted), str(submitted), "neutral", 3),
        _metric("joint_fill_events", "Joint fill events", _decimal(report.get("fill_events")), str(report.get("fill_events") or 0), "neutral", 4),
        _metric("joint_ledger_events", "Joint ledger events", Decimal(len(ledger)), str(len(ledger)), "neutral", 5),
        _metric("joint_fill_rate", "Joint fill rate", fill_rate, f"{fill_rate:.2f}%", "positive" if fill_rate > 0 else "neutral", 6),
        _metric("joint_max_cash_at_risk", "Joint max cash at risk", _decimal(report.get("max_cash_at_risk")), str(report.get("max_cash_at_risk") or "0"), "negative", 7),
        _metric("joint_portfolio_equity", "Joint portfolio equity", _decimal(report.get("portfolio_equity")), str(report.get("portfolio_equity") or "0"), "positive", 8),
        _metric("joint_probability_sum", "Joint probability sum", probability_sum, f"{probability_sum:.4f}", "positive" if abs(probability_sum - Decimal("1")) <= Decimal("0.05") else "neutral", 9),
        _metric("joint_probability_gap", "Joint probability gap", probability_gap, f"{probability_gap:.4f}", "positive" if abs(probability_gap) <= Decimal("0.05") else "neutral", 10),
        _metric("joint_event_gross_cash_at_risk", "Joint event gross cash at risk", _decimal(exposure.get("total_gross_cash_at_risk")), str(exposure.get("total_gross_cash_at_risk") or "0"), "negative", 11),
        _metric("joint_event_worst_case_cash_loss", "Joint event worst-case cash loss", _decimal(exposure.get("worst_case_cash_loss")), str(exposure.get("worst_case_cash_loss") or "0"), "negative", 12),
    ]


def _joint_event_probability_report(outcomes: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    probability_sum = Decimal("0")
    missing = 0
    for index, outcome in enumerate(outcomes, start=1):
        outcome_key = str(_get(outcome, "outcome_key", "outcomeKey", "token_id", "tokenId", default=f"outcome-{index}"))
        latest = _latest_price_point(outcome)
        probability = _optional_decimal(_get(latest, "price", "yes_probability", "yesProbability", "close_price", "closePrice")) if latest else None
        if probability is None:
            missing += 1
            probability_text = None
        else:
            probability_sum += probability
            probability_text = _decimal_text(probability)
        rows.append(
            {
                "outcome_key": outcome_key,
                "market_slug": str(_get(outcome, "market_slug", "marketSlug", default=outcome_key) or outcome_key),
                "token_side": str(_get(outcome, "token_side", "tokenSide", default="YES") or "YES"),
                "yes_probability": probability_text,
                "latest_x": int(_decimal(_get(latest or {}, "x_value", "xValue", "block_number", "blockNumber"))) if latest else None,
            }
        )
    probability_gap = (Decimal("1") - probability_sum).quantize(Decimal("0.0000000001"))
    status = "ready" if rows and missing == 0 else "review" if rows else "missing"
    return {
        "status": status,
        "outcome_count": len(rows),
        "missing_probability_count": missing,
        "probability_sum": _decimal_text(probability_sum),
        "probability_gap": _decimal_text(probability_gap),
        "sum_status": "ready" if abs(probability_gap) <= Decimal("0.05") and missing == 0 else "review",
        "outcomes": rows,
    }


def _latest_price_point(outcome: Mapping[str, Any]) -> Mapping[str, Any] | None:
    points = _get(outcome, "price_points", "pricePoints", default=[]) or []
    if not isinstance(points, Sequence) or isinstance(points, (str, bytes, bytearray)):
        return None
    rows = [row for row in points if isinstance(row, Mapping)]
    if not rows:
        return None
    return max(rows, key=lambda row: int(_decimal(_get(row, "x_value", "xValue", "block_number", "blockNumber", "timestamp"))))


def _metric(key: str, name: str, value: Decimal, formatted: str, status: str, sort_order: int) -> dict[str, Any]:
    return {
        "metric_key": key,
        "metric_name": name,
        "metric_group": "joint_execution",
        "value": value,
        "formatted_value": formatted,
        "delta": "",
        "status": status,
        "tooltip": "Joint fill-first event-stream metric",
        "sort_order": sort_order,
    }


def _insert_parameters(cur: Any, run_id: int, params: Any) -> None:
    cur.execute(
        """
        INSERT INTO quant.quant_backtest_parameters (
            run_id, entry_threshold, exit_threshold, stop_loss, take_profit,
            max_holding_bars, initial_capital, position_size,
            fee_bps, maker_fee_bps, taker_fee_bps, maker_rebate_bps,
            slippage_bps, liquidity_cap_pct,
            max_position_notional, min_fill_pct,
            execution_price_mode, execution_profile, order_role,
            latency_blocks, adverse_slippage_cents, fill_probability_haircut_pct,
            latency_seconds, max_book_staleness_seconds,
            allow_partial_fill, min_fill_size, reject_on_stale_book,
            final_valuation_mode, max_entry_price, min_exit_price,
            buy_limit_price, sell_limit_price, settlement_value,
            gas_cost_per_order, settlement_cost, redeem_cost, capital_cost_bps
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            params.entry_threshold,
            params.exit_threshold,
            params.stop_loss,
            params.take_profit,
            params.max_holding_bars,
            params.initial_capital,
            params.position_size,
            params.fee_bps,
            params.maker_fee_bps,
            params.taker_fee_bps,
            params.maker_rebate_bps,
            params.slippage_bps,
            params.liquidity_cap_pct,
            params.max_position_notional,
            params.min_fill_pct,
            params.execution_price_mode,
            params.execution_profile,
            params.order_role,
            params.latency_blocks,
            params.adverse_slippage_cents,
            params.fill_probability_haircut_pct,
            params.latency_seconds,
            params.max_book_staleness_seconds,
            params.allow_partial_fill,
            params.min_fill_size,
            params.reject_on_stale_book,
            params.final_valuation_mode,
            params.max_entry_price,
            params.min_exit_price,
            params.buy_limit_price,
            params.sell_limit_price,
            params.settlement_value,
            params.gas_cost_per_order,
            params.settlement_cost,
            params.redeem_cost,
            params.capital_cost_bps,
        ),
    )


def _outcomes(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    value = _get(payload, "outcomes", "outcomeInputs", default=[])
    return list(value) if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) else []


def _joint_market_slug(payload: Mapping[str, Any], outcomes: Sequence[Mapping[str, Any]]) -> str:
    explicit = _get(payload, "market_slug", "marketSlug", "event_slug", "eventSlug")
    if explicit:
        return f"joint:{explicit}"
    first = outcomes[0] if outcomes else {}
    return f"joint:{_get(first, 'market_slug', 'marketSlug', 'outcome_key', 'outcomeKey', default='event')}"


def _block_window(outcomes: Sequence[Mapping[str, Any]]) -> tuple[int | None, int | None]:
    values: list[int] = []
    for outcome in outcomes:
        for point in _get(outcome, "price_points", "pricePoints", default=[]) or []:
            value = _get(point, "x_value", "xValue", "block_number", "blockNumber", "timestamp")
            if value not in (None, ""):
                values.append(int(_decimal(value)))
        for row in _get(outcome, "orders", default=[]) or []:
            value = _get(row, "submit_x", "submitX", "signal_x", "signalX")
            if value not in (None, ""):
                values.append(int(_decimal(value)))
            _append_raw_event_blocks(
                values,
                _get(row, "candidate_events", "candidateEvents", "consumed_events", "consumedEvents", default=[]),
            )
            meta = _get(row, "meta", default={}) or {}
            if isinstance(meta, Mapping):
                _append_raw_event_blocks(values, _get(meta, "candidate_events", "candidateEvents", default=[]))
                _append_raw_event_blocks(values, _get(meta, "consumed_events", "consumedEvents", default=[]))
        _append_raw_event_blocks(
            values,
            _get(
                outcome,
                "raw_orderfilled_events",
                "rawOrderfilledEvents",
                "trade_ticks",
                "tradeTicks",
                default=[],
            ),
        )
    return (min(values), max(values)) if values else (None, None)


def _append_raw_event_blocks(values: list[int], rows: Any) -> None:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        value = _get(row, "block_number", "blockNumber", "x_value", "xValue", "timestamp")
        if value not in (None, ""):
            values.append(int(_decimal(value)))


def _get(row: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if isinstance(row, Mapping) and row.get(key) not in (None, ""):
            return row.get(key)
    return default


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except Exception:
        return Decimal("0")


def _decimal_text(value: Any) -> str:
    decimal = _decimal(value).quantize(Decimal("0.0000000001"))
    return format(decimal, "f").rstrip("0").rstrip(".") or "0"


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return _decimal(value)


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except Exception:
        return None


def _optional_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
