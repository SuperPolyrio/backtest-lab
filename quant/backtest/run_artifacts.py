"""Audit and export artifacts for fill-first backtest runs."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
import hashlib
import json
from typing import Any, Iterable, Mapping, Sequence

from quant.backtest.event_stream import (
    build_event_stream_contract_report,
    build_joint_replay_execution_report,
    build_joint_replay_plan_report,
)
from quant.backtest.backtest_engine import build_fill_quality_report, fill_quality_metrics
from quant.backtest.execution_model_validation import build_execution_model_validation_report
from quant.backtest.external_source_missing_evidence import build_external_source_missing_evidence_plan
from quant.backtest.external_source_run_coverage import build_external_source_run_coverage_report
from quant.backtest.external_signals import load_external_signal_events_for_run
from quant.backtest.ledger_validation import build_ledger_cashflow_validation_report
from quant.backtest.orders import enrich_order_evidence_fields
from quant.backtest.performance_score import build_performance_score_report
from quant.backtest.platform_incidents import build_platform_incident_report, load_platform_incidents_for_run
from quant.backtest.promotion_gate import build_fill_first_promotion_gate_report
from quant.backtest.regime_coverage import build_regime_coverage_report
from quant.backtest.shadow_live_triangulation import build_shadow_live_triangulation_report


READY = "ready"
REVIEW = "review"
MISSING = "missing"
UNKNOWN = "unknown"


REQUIRED_PARAMETER_FIELDS = (
    "entry_threshold",
    "exit_threshold",
    "initial_capital",
    "position_size",
    "execution_price_mode",
    "execution_profile",
    "order_role",
    "latency_blocks",
    "latency_seconds",
    "allow_partial_fill",
    "final_valuation_mode",
)

REQUIRED_DATA_QUALITY_FIELDS = (
    "data_version",
    "source_table",
    "access_path",
    "x_axis",
    "rows",
    "first_x",
    "last_x",
    "requested_from",
    "requested_to",
    "span_coverage_pct",
)

REQUIRED_FILL_QUALITY_FIELDS = (
    "signal_count",
    "submitted_count",
    "filled_count",
    "partial_fill_count",
    "no_fill_count",
    "no_fill_reasons",
    "expected_fill_size",
    "actual_fill_size",
    "expected_fill_notional",
    "actual_fill_notional",
    "avg_participation_rate",
    "raw_event_count",
    "candidate_event_count",
    "consumed_event_count",
    "raw_evidence_summary",
    "environment_flags",
    "order_anomaly_flags",
    "avg_markout_after_1_bars",
)

REGIME_DIMENSIONS = (
    "market_category",
    "role",
    "side",
    "liquidity_bucket",
    "volatility_bucket",
    "time_to_expiry_bucket",
    "final_minute",
    "event_outcome_count_bucket",
    "no_fill_reason",
)

SETTLEMENT_SOURCE_FIELDS = (
    "resolution_source",
    "settlement_rule",
    "price_to_beat_source",
    "oracle_source",
)

MARKET_LIFECYCLE_FIELDS = (
    "market_lifecycle_status",
    "resolved_outcome",
    "end_date",
)

PARITY_ORDER_FIELDS = (
    "order_id",
    "signal_index",
    "x_axis",
    "signal_x",
    "submit_x",
    "side",
    "role",
    "order_type",
    "status",
    "decision_price",
    "requested_price",
    "requested_size",
    "requested_notional",
    "expected_fill_size",
    "expected_fill_notional",
    "actual_fill_size",
    "actual_fill_notional",
    "filled_size",
    "filled_notional",
    "unfilled_size",
    "fill_probability",
    "fill_pct",
    "participation_rate",
    "latency_blocks",
    "latency_seconds",
    "execution_source",
)

PARITY_LEDGER_FIELDS = (
    "ledger_id",
    "event_type",
    "x_axis",
    "x_value",
    "shares_delta",
    "cash_delta",
    "fee",
    "rebate",
    "slippage_cost",
    "execution_cost",
    "realized_pnl",
    "position_after",
    "cash_after",
    "price",
    "source",
)

PARITY_LIVE_EVENT_FIELDS = (
    "run_id",
    "order_id",
    "external_order_id",
    "event_time",
    "event_type",
    "source",
    "live_status",
    "live_fill_price",
    "live_fill_size",
    "live_fee",
    "live_rebate",
    "live_cash_delta",
    "live_position_delta",
    "live_latency_seconds",
)

EVENT_PROBABILITY_SUM_MIN = Decimal("0.50")
EVENT_PROBABILITY_SUM_MAX = Decimal("1.50")
EVENT_COMPLEMENT_TOLERANCE = Decimal("0.05")

CREDIBILITY_WEIGHTS = {
    "run_succeeded": 15,
    "data_quality_ready": 15,
    "fill_quality_present": 20,
    "raw_evidence_present": 15,
    "low_runtime_flags": 10,
    "live_calibration_present": 10,
    "cost_calibration_present": 10,
    "external_source_state_present": 5,
}

TAIL_RISK_STRESS_LENGTHS = (1, 3, 5, 10)

MATERIALIZED_PRICE_INPUT_TABLES = (
    "quant.market_token_block_close",
    "market_token_block_close",
    "quant.market_token_frontend_price_1m",
    "market_token_frontend_price_1m",
    "quant.orderfilled_block_replay",
    "orderfilled_block_replay",
)

MATERIALIZED_ACCESS_PATH_HINTS = (
    "token_id",
    "market_id",
    "block_range",
    "block_number",
    "primary_key",
    "indexed",
)

PRICE_ACCESS_CONTRACT_PATHS = (
    "token_id_block_number_range",
    "token_id_block_range",
    "token_id_timestamp_range",
    "market_slug_token_side_block_number_range",
    "market_slug_token_side_block_range",
    "market_slug_token_side_timestamp_range",
    "market_id_token_side_block_number_range",
    "market_id_token_side_block_range",
)

REQUIRED_CACHE_SNAPSHOT_FIELDS = (
    "data_version",
    "source_table",
    "access_path",
    "x_axis",
    "first_x",
    "last_x",
    "requested_from",
    "requested_to",
    "span_coverage_pct",
)

RAW_REPLAY_DETAIL_SOURCES = (
    "orderfilled_fact",
    "quant.orderfilled_fact",
    "orderfilled_block_replay",
    "quant.orderfilled_block_replay",
)


def load_latest_backtest_run_id(conn: Any) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM quant.quant_backtest_runs
            ORDER BY created_at DESC, run_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        return None
    return int(row["run_id"] if isinstance(row, Mapping) else row[0])


def load_latest_fill_first_backtest_run_id(conn: Any) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.run_id
            FROM quant.quant_backtest_runs r
            LEFT JOIN quant.quant_backtest_parameters p ON p.run_id = r.run_id
            WHERE upper(replace(coalesce(p.execution_price_mode, ''), '-', '_')) IN (
                'ORDERFILLED_CROSS',
                'ORDERFILLED_LIMIT_REPLAY',
                'LIMIT_REPLAY',
                'ORDERFILLED',
                'ORDERFILLED_V2_TAPE',
                'ORDERFILLED_V3_TRADE',
                'PREDICTION_L2_REPLAY_V1'
            )
            ORDER BY r.created_at DESC, r.run_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        return None
    return int(row["run_id"] if isinstance(row, Mapping) else row[0])


def load_backtest_run_artifact_inputs(conn: Any, *, run_id: int) -> dict[str, Any] | None:
    run = _fetch_one(conn, "SELECT * FROM quant.quant_backtest_runs WHERE run_id = %s", (int(run_id),))
    if not run:
        return None
    parameters = _fetch_one(conn, "SELECT * FROM quant.quant_backtest_parameters WHERE run_id = %s", (int(run_id),))
    benchmark_id = _benchmark_id_from_context({"run": run, "parameters": parameters or {}})
    benchmark_artifact_rows = _load_benchmark_artifact_rows(conn, benchmark_id=benchmark_id)
    benchmark_rows = _load_benchmark_rows(conn, benchmark_id=benchmark_id)
    execution_model_rows = _execution_model_rows_from_benchmark_artifacts(benchmark_artifact_rows, benchmark_rows)
    metrics = _fetch_all(conn, "SELECT * FROM quant.quant_backtest_metrics WHERE run_id = %s", (int(run_id),))
    orders = [
        enrich_order_evidence_fields(dict(row))
        for row in _fetch_all(conn, "SELECT * FROM quant.quant_backtest_orders WHERE run_id = %s", (int(run_id),))
    ]
    trades = _fetch_all(conn, "SELECT * FROM quant.quant_backtest_trades WHERE run_id = %s", (int(run_id),))
    ledger = _fetch_all(conn, "SELECT * FROM quant.quant_backtest_ledger WHERE run_id = %s", (int(run_id),))
    events = _fetch_all(conn, "SELECT * FROM quant.quant_backtest_events WHERE run_id = %s ORDER BY event_index", (int(run_id),))
    calibration = _fetch_count(conn, "quant.quant_backtest_calibration_orders", "run_id = %s", (int(run_id),))
    cost_calibration = _fetch_count(conn, "quant.quant_backtest_cost_calibration", "run_id = %s", (int(run_id),))
    calibration_rows = _fetch_all(conn, "SELECT * FROM quant.quant_backtest_calibration_orders WHERE run_id = %s ORDER BY observed_at NULLS LAST, calibration_id", (int(run_id),))
    cost_calibration_rows = _fetch_all(conn, "SELECT * FROM quant.quant_backtest_cost_calibration WHERE run_id = %s ORDER BY observed_at NULLS LAST, cost_calibration_id", (int(run_id),))
    real_order_events = _fetch_count(conn, "quant.real_order_state_events", "run_id = %s", (int(run_id),))
    real_order_state_rows = _fetch_all_if_table_exists(
        conn,
        "quant.real_order_state_events",
        "SELECT * FROM quant.real_order_state_events WHERE run_id = %s ORDER BY COALESCE(event_time, created_at), event_id",
        (int(run_id),),
    )
    real_cost_event_rows = _fetch_all_if_table_exists(
        conn,
        "quant.real_backtest_cost_events",
        "SELECT * FROM quant.real_backtest_cost_events WHERE run_id = %s ORDER BY observed_at NULLS LAST, cost_id",
        (int(run_id),),
    )
    external_source_states = _fetch_count(conn, "quant.external_source_import_state", None, ())
    external_source_state_rows = _fetch_all_if_table_exists(
        conn,
        "quant.external_source_import_state",
        "SELECT * FROM quant.external_source_import_state ORDER BY updated_at DESC, state_key ASC LIMIT 100",
        (),
    )
    try:
        platform_incidents = load_platform_incidents_for_run(conn, run)
    except Exception:
        platform_incidents = []
    try:
        external_signal_events = load_external_signal_events_for_run(conn, int(run_id))
    except Exception:
        external_signal_events = []
    return {
        "run": run,
        "parameters": parameters,
        "benchmark_id": benchmark_id,
        "benchmark_artifacts": benchmark_artifact_rows,
        "benchmark_rows": benchmark_rows,
        "execution_model_rows": execution_model_rows,
        "metrics": metrics,
        "orders": orders,
        "trades": trades,
        "ledger": ledger,
        "events": events,
        "calibration_count": calibration,
        "cost_calibration_count": cost_calibration,
        "calibration_rows": calibration_rows,
        "cost_calibration_rows": cost_calibration_rows,
        "real_order_state_event_count": real_order_events,
        "real_order_state_rows": real_order_state_rows,
        "real_cost_event_rows": real_cost_event_rows,
        "external_source_state_count": external_source_states,
        "external_source_state_rows": external_source_state_rows,
        "platform_incidents": platform_incidents,
        "external_signal_events": external_signal_events,
    }


def load_backtest_run_artifact_summary(conn: Any, *, run_id: int) -> dict[str, Any] | None:
    """Load a bounded artifact summary without detoasting the run audit payload.

    Full artifact audits intentionally load every persisted order and event. That is
    appropriate for an explicit quality gate, but too expensive for workbench first
    paint. This query only reads indexed counts and compact run/parameter columns.
    """

    cached = _fetch_one(
        conn,
        """
        SELECT artifact_summary
        FROM quant.quant_backtest_run_progress
        WHERE run_id = %s
        """,
        (int(run_id),),
    )
    if cached:
        summary = cached.get("artifact_summary")
        if isinstance(summary, str):
            try:
                summary = json.loads(summary)
            except json.JSONDecodeError:
                summary = None
        if isinstance(summary, Mapping) and summary:
            return dict(summary)

    row = _fetch_one(
        conn,
        """
        WITH target AS (
            SELECT %s::bigint AS run_id
        ),
        metric_counts AS (
            SELECT count(*)::bigint AS metric_count
            FROM quant.quant_backtest_metrics
            WHERE run_id = (SELECT run_id FROM target)
        ),
        equity_counts AS (
            SELECT count(*)::bigint AS equity_count
            FROM quant.quant_backtest_equity
            WHERE run_id = (SELECT run_id FROM target)
        ),
        trade_counts AS (
            SELECT count(*)::bigint AS trade_count
            FROM quant.quant_backtest_trades
            WHERE run_id = (SELECT run_id FROM target)
        ),
        order_counts AS (
            SELECT count(*)::bigint AS order_count
            FROM quant.quant_backtest_orders
            WHERE run_id = (SELECT run_id FROM target)
        ),
        ledger_counts AS (
            SELECT count(*)::bigint AS ledger_count
            FROM quant.quant_backtest_ledger
            WHERE run_id = (SELECT run_id FROM target)
        ),
        event_counts AS (
            SELECT count(*)::bigint AS event_count
            FROM quant.quant_backtest_events
            WHERE run_id = (SELECT run_id FROM target)
        )
        SELECT
            r.run_id,
            r.status,
            r.market_slug,
            r.token_side,
            r.price_source,
            r.backtest_engine,
            r.from_ts,
            r.to_ts,
            r.from_block,
            r.to_block,
            r.rows_processed,
            r.error,
            r.created_at,
            r.started_at,
            r.finished_at,
            p.execution_price_mode,
            p.execution_profile,
            p.order_role,
            p.final_valuation_mode,
            metric_counts.metric_count,
            equity_counts.equity_count,
            trade_counts.trade_count,
            order_counts.order_count,
            ledger_counts.ledger_count,
            event_counts.event_count
        FROM quant.quant_backtest_runs r
        LEFT JOIN quant.quant_backtest_parameters p ON p.run_id = r.run_id
        CROSS JOIN metric_counts
        CROSS JOIN equity_counts
        CROSS JOIN trade_counts
        CROSS JOIN order_counts
        CROSS JOIN ledger_counts
        CROSS JOIN event_counts
        WHERE r.run_id = (SELECT run_id FROM target)
        """,
        (int(run_id),),
    )
    return build_backtest_run_artifact_summary(row, run_id=run_id) if row else None


def build_backtest_run_artifact_summary(
    row: Mapping[str, Any] | None,
    *,
    run_id: int | None = None,
) -> dict[str, Any]:
    """Build an honest first-paint summary; deep quality checks remain unevaluated."""

    if row is None:
        return {
            "status": UNKNOWN,
            "summary_status": UNKNOWN,
            "deep_audit_status": UNKNOWN,
            "run_id": run_id,
            "reason": "run_not_found" if run_id is not None else "no_backtest_runs",
            "audit_mode": "summary",
            "checks": [],
            "artifacts": {},
            "next_actions": ["Run or select a backtest run before auditing run artifacts."],
        }

    item = dict(row)
    resolved_run_id = _to_int(item.get("run_id")) or run_id
    run_status = str(item.get("status") or UNKNOWN).lower()
    metric_count = _to_int(item.get("metric_count"))
    equity_count = _to_int(item.get("equity_count"))
    trade_count = _to_int(item.get("trade_count"))
    order_count = _to_int(item.get("order_count"))
    ledger_count = _to_int(item.get("ledger_count"))
    event_count = _to_int(item.get("event_count"))
    timestamps_evaluated = all(
        item.get(key) is not None
        for key in ("timestamped_order_count", "timestamped_ledger_count", "timestamped_event_count")
    )
    timestamped_order_count = _to_int(item.get("timestamped_order_count")) if timestamps_evaluated else 0
    timestamped_ledger_count = _to_int(item.get("timestamped_ledger_count")) if timestamps_evaluated else 0
    timestamped_event_count = _to_int(item.get("timestamped_event_count")) if timestamps_evaluated else 0
    replayable_count = order_count + ledger_count + event_count
    timestamped_count = timestamped_order_count + timestamped_ledger_count + timestamped_event_count

    run_check_status = READY if run_status == "succeeded" else REVIEW
    persisted_check_status = READY if metric_count > 0 and equity_count > 0 else REVIEW
    execution_check_status = READY if event_count > 0 and (order_count > 0 or ledger_count > 0) else REVIEW
    if not timestamps_evaluated:
        timestamp_check_status = UNKNOWN
    elif replayable_count == 0:
        timestamp_check_status = REVIEW
    elif timestamped_count == replayable_count:
        timestamp_check_status = READY
    else:
        timestamp_check_status = MISSING

    checks = [
        _check(
            "run status",
            run_check_status,
            f"status={run_status} rows={_to_int(item.get('rows_processed'))}",
            "quant.quant_backtest_runs",
        ),
        _check(
            "persisted result series",
            persisted_check_status,
            f"metrics={metric_count} equity={equity_count}",
            "quant.quant_backtest_metrics + quant.quant_backtest_equity",
        ),
        _check(
            "execution artifacts",
            execution_check_status,
            f"events={event_count} orders={order_count} ledger={ledger_count} trades={trade_count}",
            "quant.quant_backtest_events + orders + ledger + trades",
        ),
        _check(
            "source timestamps",
            timestamp_check_status,
            (
                f"timestamped={timestamped_count}/{replayable_count}"
                if timestamps_evaluated
                else "not evaluated in bounded summary; use canonical replay or full audit"
            ),
            "quant.quant_backtest_events/orders/ledger.meta",
        ),
    ]
    summary_status = _aggregate(check["status"] for check in checks)
    return {
        # A summary can prove persistence and timestamp closure, but it cannot
        # substitute for the explicit full quality gate.
        "status": REVIEW,
        "summary_status": summary_status,
        "deep_audit_status": UNKNOWN,
        "run_id": resolved_run_id,
        "reason": "bounded_database_summary",
        "audit_mode": "summary",
        "checks": checks,
        "artifacts": {
            "metrics": metric_count,
            "equity_points": equity_count,
            "trades": trade_count,
            "orders": order_count,
            "ledger_events": ledger_count,
            "events": event_count,
            "replayable_rows": replayable_count,
            "timestamped_rows": timestamped_count if timestamps_evaluated else None,
            "source_timestamp_coverage_pct": (
                str((Decimal(timestamped_count) * Decimal(100) / Decimal(replayable_count)).quantize(Decimal("0.01")))
                if timestamps_evaluated and replayable_count
                else None
            ),
            "execution_price_mode": item.get("execution_price_mode"),
            "execution_profile": item.get("execution_profile"),
            "order_role": item.get("order_role"),
            "final_valuation_mode": item.get("final_valuation_mode"),
        },
        "run": {
            key: item.get(key)
            for key in (
                "run_id",
                "status",
                "market_slug",
                "token_side",
                "price_source",
                "backtest_engine",
                "from_ts",
                "to_ts",
                "from_block",
                "to_block",
                "rows_processed",
                "error",
                "created_at",
                "started_at",
                "finished_at",
            )
        },
        "next_actions": [
            "Request this endpoint without mode=summary to run the explicit full artifact quality audit."
        ],
    }


def build_backtest_run_artifact_report(inputs: Mapping[str, Any] | None, *, run_id: int | None = None) -> dict[str, Any]:
    if inputs is None:
        return {
            "status": UNKNOWN,
            "run_id": run_id,
            "reason": "run_not_found" if run_id is not None else "no_backtest_runs",
            "checks": [],
            "artifacts": {},
            "next_actions": ["Run or select a backtest run before auditing run artifacts."],
        }
    run = dict(inputs.get("run") or {})
    meta = _json_dict(run.get("meta"))
    data_quality = _json_dict(meta.get("actual_data_quality"))
    fill_quality = _json_dict(data_quality.get("fill_quality")) or _json_dict(meta.get("fill_quality"))
    metrics = list(inputs.get("metrics") or [])
    orders = list(inputs.get("orders") or [])
    trades = list(inputs.get("trades") or [])
    ledger = list(inputs.get("ledger") or [])
    events = list(inputs.get("events") or [])
    raw_orderfilled_events = _raw_orderfilled_events_for_artifact(data_quality, fill_quality)
    credibility = _build_run_credibility_assessment(run, data_quality, fill_quality, inputs)
    data_quality_report = build_run_data_quality_report(data_quality, fill_quality)
    materialized_cache_report = build_materialized_cache_report(run, data_quality, data_quality_report, fill_quality)
    raw_orderfilled_replay_contract_report = build_raw_orderfilled_replay_contract_report(data_quality, fill_quality)
    historical_l2_alignment_report = build_historical_l2_alignment_report(meta, data_quality, fill_quality)
    environment_incident_report = build_environment_incident_report(list(inputs.get("platform_incidents") or []), fill_quality)
    external_signal_events = _external_signal_events_from_inputs(inputs, meta)
    external_signal_contract_report = build_external_signal_contract_report(run, inputs.get("parameters"), external_signal_events)
    execution_semantics_report = build_execution_semantics_report(orders, fill_quality)
    fill_probability_evidence_report = build_fill_probability_evidence_report(orders, fill_quality)
    maker_taker_execution_report = build_maker_taker_execution_report(orders)
    maker_queue_uncertainty_report = build_maker_queue_uncertainty_report(orders)
    latency_profile_report = build_latency_profile_report(orders, fill_quality, inputs.get("parameters") or {})
    slippage_regime_report = build_slippage_regime_report(orders, fill_quality, inputs.get("parameters") or {})
    regime_report = build_execution_regime_report(orders)
    regime_coverage_report = build_regime_coverage_report(orders)
    execution_model_validation_report = build_execution_model_validation_report(list(inputs.get("execution_model_rows") or []))
    shadow_live_report = build_shadow_live_triangulation_report(
        list(inputs.get("calibration_rows") or []),
        list(inputs.get("cost_calibration_rows") or []),
        real_order_state_event_count=_to_int(inputs.get("real_order_state_event_count")),
        external_source_state_count=_to_int(inputs.get("external_source_state_count")),
    )
    external_run_coverage_report = build_external_source_run_coverage_report(
        {
            "run": run,
            "orders": orders,
            "real_order_events": list(inputs.get("real_order_state_rows") or []),
            "calibration_rows": list(inputs.get("calibration_rows") or []),
            "real_cost_events": list(inputs.get("real_cost_event_rows") or []),
            "cost_calibration_rows": list(inputs.get("cost_calibration_rows") or []),
            "platform_incidents": list(inputs.get("platform_incidents") or []),
            "external_states": list(inputs.get("external_source_state_rows") or []),
        },
        run_id=int(run.get("run_id") or run_id or 0) or None,
    )
    external_missing_evidence_plan = build_external_source_missing_evidence_plan(
        {
            "run": run,
            "parameters": inputs.get("parameters") or {},
            "orders": orders,
            "real_order_events": list(inputs.get("real_order_state_rows") or []),
            "calibration_rows": list(inputs.get("calibration_rows") or []),
        },
        run_id=int(run.get("run_id") or run_id or 0) or None,
    )
    tail_risk_report = build_tail_risk_report(trades, inputs.get("parameters"))
    prediction_quality_report = build_prediction_quality_report(run, inputs.get("parameters"), orders, trades)
    performance_score_report = build_performance_score_report(
        trades=trades,
        ledger=ledger,
        fill_quality_report=fill_quality,
        data_quality_report=data_quality_report,
        prediction_quality_report=prediction_quality_report,
    )
    lifecycle_report = build_market_lifecycle_report(run, inputs.get("parameters"), trades, ledger)
    settlement_report = build_settlement_compatibility_report(run, inputs.get("parameters"), trades, ledger)
    event_risk_report = build_event_level_risk_report(run, inputs.get("parameters"), orders, trades, ledger, inputs)
    event_stream_report = build_event_stream_contract_report(
        strategy_events=events,
        orders=orders,
        raw_orderfilled_events=raw_orderfilled_events,
        ledger=ledger,
        market_slug=str(run.get("market_slug") or ""),
        token_side=str(run.get("token_side") or ""),
    )
    joint_replay_report = build_joint_replay_plan_report(
        [
            {
                "outcome_key": str(meta.get("outcome_label") or run.get("token_side") or "selected"),
                "market_slug": str(run.get("market_slug") or ""),
                "token_side": str(run.get("token_side") or ""),
                "strategy_events": events,
                "orders": orders,
                "raw_orderfilled_events": raw_orderfilled_events,
                "ledger": ledger,
            }
        ]
    )
    joint_execution_report = build_joint_replay_execution_report(
        [
            {
                "outcome_key": str(meta.get("outcome_label") or run.get("token_side") or "selected"),
                "market_slug": str(run.get("market_slug") or ""),
                "token_side": str(run.get("token_side") or ""),
                "strategy_events": events,
                "orders": orders,
                "raw_orderfilled_events": raw_orderfilled_events,
                "ledger": ledger,
            }
        ]
    )
    reproducibility_report = build_reproducibility_report(run, inputs.get("parameters"), data_quality_report, fill_quality, inputs)
    parity_report = build_execution_ledger_parity_report(orders, ledger, inputs)
    ledger_cashflow_report = build_ledger_cashflow_validation_report(trades, ledger, inputs.get("parameters") or {})
    promotion_gate_report = build_fill_first_promotion_gate_report(
        run_credibility=credibility,
        data_quality_report=data_quality_report,
        reproducibility_report=reproducibility_report,
        materialized_cache_report=materialized_cache_report,
        shadow_live_triangulation_report=shadow_live_report,
        regime_coverage_report=regime_coverage_report,
        prediction_quality_report=prediction_quality_report,
        settlement_compatibility_report=settlement_report,
        external_source_run_coverage_report=external_run_coverage_report,
        external_source_missing_evidence_plan=external_missing_evidence_plan,
    )
    paper_live_evidence_gate_report = _build_paper_live_evidence_gate_report(
        external_run_coverage_report,
        external_missing_evidence_plan,
        promotion_gate_report,
    )
    checks = [
        _check_run_status(run),
        _check_parameter_snapshot(run, inputs.get("parameters")),
        _check_reproducibility_report(reproducibility_report),
        _check_materialized_cache_report(materialized_cache_report),
        _check_raw_orderfilled_replay_contract_report(raw_orderfilled_replay_contract_report),
        _check_historical_l2_alignment_report(historical_l2_alignment_report),
        _check_environment_incident_report(environment_incident_report),
        _check_external_signal_contract_report(external_signal_contract_report),
        _check_block_window(run),
        _check_data_quality(data_quality),
        _check_fill_quality(fill_quality, metrics),
        _check_orders(orders, fill_quality),
        _check_execution_semantics_report(execution_semantics_report),
        _check_fill_probability_evidence_report(fill_probability_evidence_report),
        _check_maker_taker_execution_report(maker_taker_execution_report),
        _check_maker_queue_uncertainty_report(maker_queue_uncertainty_report),
        _check_latency_profile_report(latency_profile_report),
        _check_slippage_regime_report(slippage_regime_report),
        _check_ledger(ledger),
        _check_execution_ledger_parity_report(parity_report),
        _check_ledger_cashflow_validation_report(ledger_cashflow_report),
        _check_promotion_gate_report(promotion_gate_report),
        _check_regime_report(orders),
        _check_regime_coverage_report(regime_coverage_report),
        _check_execution_model_validation_report(execution_model_validation_report),
        _check_shadow_live_triangulation_report(shadow_live_report),
        _check_external_source_run_coverage_report(external_run_coverage_report),
        _check_external_source_missing_evidence_plan(external_missing_evidence_plan),
        _check_paper_live_evidence_gate_report(paper_live_evidence_gate_report),
        _check_tail_risk_report(trades, inputs.get("parameters")),
        _check_prediction_quality_report(prediction_quality_report),
        _check_performance_score_report(performance_score_report),
        _check_market_lifecycle_report(run, inputs.get("parameters"), trades, ledger),
        _check_settlement_compatibility_report(run, inputs.get("parameters"), trades, ledger),
        _check_event_level_risk_report(event_risk_report),
        _check_event_stream_contract_report(event_stream_report),
        _check_joint_replay_plan_report(joint_replay_report),
        _check_joint_replay_execution_report(joint_execution_report),
        _check_quality_metrics(metrics),
        _check_calibration_sources(inputs),
    ]
    status = _aggregate([*(check["status"] for check in checks), str(credibility["status"])])
    return {
        "status": status,
        "run_id": int(run.get("run_id") or run_id or 0) or None,
        "market_slug": run.get("market_slug"),
        "token_side": run.get("token_side"),
        "price_source": run.get("price_source"),
        "backtest_engine": run.get("backtest_engine"),
        "rows_processed": _to_int(run.get("rows_processed")),
        "artifacts": {
            "parameter_fingerprint": meta.get("parameter_fingerprint"),
            "data_version": data_quality.get("data_version") or data_quality.get("checksum"),
            "data_quality_status": data_quality.get("status"),
            "data_quality_verdict": data_quality_report["quality_verdict"],
            "data_quality_warning_level": data_quality_report["warning_level"],
            "data_quality_max_gap": data_quality_report["max_gap"],
            "data_quality_stale_count": data_quality_report["stale_count"],
            "data_quality_fallback_count": data_quality_report["fallback_count"],
            "data_quality_duplicate_count": data_quality_report["duplicate_count"],
            "reproducibility_status": reproducibility_report["status"],
            "reproducibility_verdict": reproducibility_report["reproducibility_verdict"],
            "materialized_cache_status": materialized_cache_report["status"],
            "materialized_cache_verdict": materialized_cache_report["cache_verdict"],
            "materialized_cache_source_table": materialized_cache_report["source_table"],
            "materialized_cache_access_path": materialized_cache_report["access_path"],
            "materialized_cache_data_version": materialized_cache_report["data_version"],
            "materialized_cache_key": materialized_cache_report["cache_key"],
            "raw_orderfilled_replay_contract_verdict": raw_orderfilled_replay_contract_report["contract_verdict"],
            "raw_orderfilled_loaded_event_count": raw_orderfilled_replay_contract_report["loaded_event_count"],
            "raw_orderfilled_deduped_event_count": raw_orderfilled_replay_contract_report["deduped_event_count"],
            "raw_orderfilled_duplicate_event_count": raw_orderfilled_replay_contract_report["duplicate_event_count"],
            "raw_orderfilled_exact_duplicate_event_count": raw_orderfilled_replay_contract_report["exact_duplicate_event_count"],
            "raw_orderfilled_conflicting_duplicate_event_count": raw_orderfilled_replay_contract_report["conflicting_duplicate_event_count"],
            "raw_orderfilled_duplicate_group_count": raw_orderfilled_replay_contract_report["duplicate_group_count"],
            "raw_orderfilled_conflicting_duplicate_group_count": raw_orderfilled_replay_contract_report["conflicting_duplicate_group_count"],
            "raw_orderfilled_canonical_fill_key_coverage_pct": raw_orderfilled_replay_contract_report["canonical_fill_key_coverage_pct"],
            "raw_orderfilled_maker_taker_side_coverage_pct": raw_orderfilled_replay_contract_report["maker_taker_side_coverage_pct"],
            "raw_orderfilled_block_context_coverage_pct": raw_orderfilled_replay_contract_report["block_context_coverage_pct"],
            "raw_orderfilled_block_vwap_available_pct": raw_orderfilled_replay_contract_report["block_vwap_available_pct"],
            "raw_orderfilled_block_high_low_available_pct": raw_orderfilled_replay_contract_report["block_high_low_available_pct"],
            "raw_orderfilled_block_order_sequence_coverage_pct": raw_orderfilled_replay_contract_report["block_order_sequence_coverage_pct"],
            "raw_orderfilled_multi_trade_sequence_coverage_pct": raw_orderfilled_replay_contract_report["multi_trade_sequence_coverage_pct"],
            "historical_l2_alignment_status": historical_l2_alignment_report["status"],
            "historical_l2_alignment_verdict": historical_l2_alignment_report["alignment_verdict"],
            "historical_l2_aligned_count": historical_l2_alignment_report["aligned_count"],
            "historical_l2_orderfilled_rows_matched": historical_l2_alignment_report["orderfilled_rows_matched"],
            "historical_l2_alignment_pct": historical_l2_alignment_report["alignment_pct"],
            "historical_l2_price_outside_spread_count": historical_l2_alignment_report["price_outside_spread_count"],
            "historical_l2_stale_count": historical_l2_alignment_report["stale_l2_count"],
            "historical_l2_missing_before_fill_count": historical_l2_alignment_report["missing_l2_before_fill_count"],
            "historical_l2_depth_checked_count": historical_l2_alignment_report["depth_checked_count"],
            "historical_l2_depth_sufficient_count": historical_l2_alignment_report["depth_sufficient_count"],
            "historical_l2_depth_insufficient_count": historical_l2_alignment_report["depth_insufficient_count"],
            "historical_l2_crossable_depth_sufficient_count": historical_l2_alignment_report["crossable_depth_sufficient_count"],
            "historical_l2_depth_sufficient_pct": historical_l2_alignment_report["depth_sufficient_pct"],
            "historical_l2_crossable_depth_sufficient_pct": historical_l2_alignment_report["crossable_depth_sufficient_pct"],
            "environment_incident_status": environment_incident_report["status"],
            "environment_incident_verdict": environment_incident_report["incident_verdict"],
            "environment_incident_count": environment_incident_report["incident_count"],
            "environment_incident_flag_count": environment_incident_report["environment_flag_count"],
            "external_signal_contract_status": external_signal_contract_report["status"],
            "external_signal_contract_verdict": external_signal_contract_report["signal_verdict"],
            "external_signal_event_count": external_signal_contract_report["event_count"],
            "external_signal_missing_field_count": external_signal_contract_report["missing_required_field_count"],
            "execution_semantics_status": execution_semantics_report["status"],
            "execution_semantics_verdict": execution_semantics_report["semantics_verdict"],
            "execution_semantics_role_counts": execution_semantics_report["role_counts"],
            "execution_semantics_order_type_counts": execution_semantics_report["order_type_counts"],
            "execution_semantics_time_in_force_counts": execution_semantics_report["time_in_force_counts"],
            "fill_probability_evidence_status": fill_probability_evidence_report["status"],
            "fill_probability_evidence_verdict": fill_probability_evidence_report["evidence_verdict"],
            "fill_probability_ready_order_count": fill_probability_evidence_report["ready_order_count"],
            "fill_probability_missing_field_counts": fill_probability_evidence_report["missing_field_counts"],
            "maker_taker_execution_status": maker_taker_execution_report["status"],
            "maker_taker_execution_verdict": maker_taker_execution_report["maker_taker_verdict"],
            "maker_taker_execution_scope": maker_taker_execution_report["execution_scope"],
            "maker_order_count": maker_taker_execution_report["role_counts"].get("maker", 0),
            "taker_order_count": maker_taker_execution_report["role_counts"].get("taker", 0),
            "maker_queue_uncertainty_status": maker_queue_uncertainty_report["status"],
            "maker_queue_uncertainty_verdict": maker_queue_uncertainty_report["queue_uncertainty_verdict"],
            "maker_queue_risk_order_count": maker_queue_uncertainty_report["risk_order_count"],
            "maker_queue_high_participation_count": maker_queue_uncertainty_report["high_participation_order_count"],
            "latency_profile_status": latency_profile_report["status"],
            "latency_profile_verdict": latency_profile_report["latency_verdict"],
            "latency_profile_name": latency_profile_report["latency_profile"],
            "latency_stale_price_risk_count": latency_profile_report["stale_price_risk_count"],
            "slippage_regime_status": slippage_regime_report["status"],
            "slippage_regime_verdict": slippage_regime_report["slippage_regime_verdict"],
            "slippage_risk_order_count": slippage_regime_report["risk_order_count"],
            "slippage_risk_regime_count": slippage_regime_report["risk_regime_count"],
            "code_commit": reproducibility_report["code_commit"],
            "code_dirty": reproducibility_report["code_dirty"],
            "strategy_name": reproducibility_report["strategy_name"],
            "strategy_version": reproducibility_report["strategy_version"],
            "artifact_schema_version": reproducibility_report["artifact_schema_version"],
            "fill_signal_count": fill_quality.get("signal_count"),
            "fill_submitted_count": fill_quality.get("submitted_count"),
            "raw_replay_fallback_suppressed_count": _to_int(fill_quality.get("raw_replay_fallback_suppressed_count")),
            "raw_replay_synthetic_cross_no_fill_count": _to_int(fill_quality.get("raw_replay_synthetic_cross_no_fill_count")),
            "block_participation_discount_tick_count": _to_int(fill_quality.get("block_participation_discount_tick_count")),
            "max_requested_block_participation_pct": fill_quality.get("max_requested_block_participation_pct"),
            "min_block_participation_factor": fill_quality.get("min_block_participation_factor"),
            "orders": len(orders),
            "trades": len(trades),
            "ledger_events": len(ledger),
            "metrics": len(metrics),
            "calibration_samples": _to_int(inputs.get("calibration_count")),
            "cost_calibration_samples": _to_int(inputs.get("cost_calibration_count")),
            "real_order_state_events": _to_int(inputs.get("real_order_state_event_count")),
            "external_source_states": _to_int(inputs.get("external_source_state_count")),
            "run_credibility_status": credibility["status"],
            "run_credibility_score": credibility["score"],
            "run_credibility_reason": credibility["reason"],
            "regime_report_status": regime_report["status"],
            "regime_report_dimensions": len(regime_report["dimensions"]),
            "regime_coverage_status": regime_coverage_report["status"],
            "regime_coverage_verdict": regime_coverage_report["coverage_verdict"],
            "strategy_scope": regime_coverage_report["strategy_scope"],
            "regime_specific": regime_coverage_report["regime_specific"],
            "execution_model_validation_status": execution_model_validation_report["status"],
            "execution_model_validation_verdict": execution_model_validation_report["validation_verdict"],
            "execution_model_missing_models": execution_model_validation_report["missing_models"],
            "execution_model_present_models": execution_model_validation_report["present_models"],
            "execution_model_l2_fill_rate": _json_dict(execution_model_validation_report.get("model_comparison")).get("l2_fill_rate"),
            "execution_model_l2_pnl_delta_vs_baseline": _json_dict(execution_model_validation_report.get("model_comparison")).get("l2_pnl_delta_vs_max_baseline"),
            "execution_model_l2_monotonic_fill_rate_ok": _json_dict(execution_model_validation_report.get("l2_profile_sensitivity")).get("monotonic_fill_rate_ok"),
            "shadow_live_triangulation_status": shadow_live_report["status"],
            "shadow_live_triangulation_verdict": shadow_live_report["triangulation_verdict"],
            "fill_model_suspect": shadow_live_report["fill_model_suspect"],
            "shadow_live_sample_count": shadow_live_report["shadow_live_sample_count"],
            "external_run_coverage_status": external_run_coverage_report["status"],
            "external_run_order_state_coverage_pct": external_run_coverage_report["order_state_coverage_pct"],
            "external_run_calibration_coverage_pct": external_run_coverage_report["calibration_coverage_pct"],
            "external_run_candidate_orders": external_run_coverage_report["external_order_candidate_count"],
            "external_run_matched_order_state_count": external_run_coverage_report["matched_order_state_count"],
            "external_run_calibration_samples": external_run_coverage_report["calibration_sample_count"],
            "external_run_cost_calibration_samples": external_run_coverage_report["cost_calibration_sample_count"],
            "external_run_platform_incidents": external_run_coverage_report["platform_incident_count"],
            "external_missing_evidence_status": external_missing_evidence_plan["status"],
            "external_missing_order_state_count": external_missing_evidence_plan["missing_order_state_count"],
            "external_missing_calibration_count": external_missing_evidence_plan["missing_calibration_count"],
            "external_missing_event_template_count": len(external_missing_evidence_plan.get("event_templates") or []),
            "tail_risk_status": tail_risk_report["status"],
            "tail_risk_verdict": tail_risk_report["risk_verdict"],
            "tail_risk_trade_count": tail_risk_report["trade_count"],
            "max_single_loss": tail_risk_report["max_single_loss"],
            "consecutive_loss_streak": tail_risk_report["consecutive_loss_streak"],
            "prediction_quality_status": prediction_quality_report["status"],
            "prediction_quality_verdict": prediction_quality_report["prediction_verdict"],
            "prediction_quality_sample_count": prediction_quality_report["sample_count"],
            "prediction_brier_score": prediction_quality_report["brier_score"],
            "prediction_brier_advantage": prediction_quality_report["brier_advantage"],
            "performance_score_status": performance_score_report["status"],
            "performance_ranking_verdict": performance_score_report["ranking_verdict"],
            "performance_score": performance_score_report["performance_score"],
            "performance_sharpe": performance_score_report["sharpe"],
            "performance_sortino": performance_score_report["sortino"],
            "performance_calmar": performance_score_report["calmar"],
            "performance_coverage_penalty": performance_score_report["penalties"]["coverage_penalty"],
            "performance_low_fill_penalty": performance_score_report["penalties"]["low_fill_penalty"],
            "settlement_compatibility_status": settlement_report["status"],
            "settlement_compatibility_verdict": settlement_report["compatibility_verdict"],
            "settlement_trade_count": settlement_report["settlement_trade_count"],
            "settlement_lifecycle_status": settlement_report["market_lifecycle_status"],
            "market_lifecycle_status": lifecycle_report["status"],
            "market_lifecycle_verdict": lifecycle_report["lifecycle_verdict"],
            "market_resolved_outcome": lifecycle_report["resolved_outcome"],
            "event_level_risk_status": event_risk_report["status"],
            "event_level_risk_verdict": event_risk_report["risk_verdict"],
            "event_outcome_count": event_risk_report["observed_outcome_count"],
            "event_probability_sum": event_risk_report["probability_sum"]["value"],
            "portfolio_cash_at_risk": event_risk_report["portfolio_risk"]["portfolio_cash_at_risk"],
            "event_stream_status": event_stream_report["status"],
            "event_stream_schema_version": event_stream_report["schema_version"],
            "event_stream_event_count": event_stream_report["event_count"],
            "event_stream_raw_trade_tick_count": _to_int(_json_dict(event_stream_report.get("raw_trade_tick_report")).get("trade_tick_count")),
            "event_stream_raw_trade_block_count": _to_int(_json_dict(event_stream_report.get("raw_trade_tick_report")).get("block_count")),
            "joint_replay_status": joint_replay_report["status"],
            "joint_replay_verdict": joint_replay_report["joint_replay_verdict"],
            "joint_replay_mode": joint_replay_report["replay_mode"],
            "joint_replay_event_count": joint_replay_report["event_count"],
            "joint_replay_raw_trade_tick_count": _to_int(joint_replay_report.get("raw_trade_tick_count")),
            "joint_replay_raw_trade_block_count": _to_int(joint_replay_report.get("raw_trade_block_count")),
            "joint_replay_raw_canonical_fill_key_coverage_pct": joint_replay_report.get("raw_canonical_fill_key_coverage_pct"),
            "joint_replay_raw_block_context_coverage_pct": joint_replay_report.get("raw_block_context_coverage_pct"),
            "joint_execution_status": joint_execution_report["status"],
            "joint_execution_verdict": joint_execution_report["execution_verdict"],
            "joint_execution_mode": joint_execution_report["replay_mode"],
            "joint_execution_event_count": joint_execution_report["event_count"],
            "joint_execution_raw_trade_tick_count": _to_int(joint_execution_report.get("raw_trade_tick_count")),
            "joint_execution_raw_trade_block_count": _to_int(joint_execution_report.get("raw_trade_block_count")),
            "joint_execution_raw_canonical_fill_key_coverage_pct": joint_execution_report.get("raw_canonical_fill_key_coverage_pct"),
            "joint_execution_raw_block_context_coverage_pct": joint_execution_report.get("raw_block_context_coverage_pct"),
            "joint_execution_max_cash_at_risk": joint_execution_report["max_cash_at_risk"],
            "joint_execution_portfolio_equity": joint_execution_report["portfolio_equity"],
            "joint_execution_equity_curve_points": joint_execution_report.get("equity_curve_points", 0),
            "joint_execution_max_drawdown": joint_execution_report.get("max_drawdown", "0"),
            "joint_execution_max_drawdown_pct": joint_execution_report.get("max_drawdown_pct", "0"),
            "execution_ledger_parity_status": parity_report["status"],
            "execution_ledger_parity_verdict": parity_report["parity_verdict"],
            "parity_order_missing_field_count": parity_report["order_schema"]["missing_field_count"],
            "parity_ledger_missing_field_count": parity_report["ledger_schema"]["missing_field_count"],
            "ledger_cashflow_status": ledger_cashflow_report["status"],
            "ledger_cashflow_verdict": ledger_cashflow_report["cashflow_verdict"],
            "net_profit_trade": ledger_cashflow_report["net_profit_trade"],
            "net_profit_ledger": ledger_cashflow_report["net_profit_ledger"],
            "ledger_diff": ledger_cashflow_report["ledger_diff"],
            "ledger_missing_trade_count": ledger_cashflow_report["missing_trade_ledger_count"],
            "polymarket_cashflow_total": ledger_cashflow_report["polymarket_cashflow_total"],
            "polymarket_unrealized_position_value": ledger_cashflow_report["polymarket_unrealized_position_value"],
            "polymarket_net_trading_pnl": ledger_cashflow_report["polymarket_net_trading_pnl"],
            "polymarket_residual_position_count": ledger_cashflow_report["polymarket_residual_position_count"],
            "polymarket_portfolio_cash_at_risk": ledger_cashflow_report["polymarket_portfolio_cash_at_risk"],
            "polymarket_mark_price_count": ledger_cashflow_report["polymarket_mark_price_count"],
            "promotion_gate_status": promotion_gate_report["status"],
            "promotion_verdict": promotion_gate_report["promotion_verdict"],
            "production_promotion_allowed": promotion_gate_report["production_promotion_allowed"],
            "paper_promotion_allowed": promotion_gate_report["paper_promotion_allowed"],
            "allowed_next_modes": promotion_gate_report["allowed_next_modes"],
            "paper_live_evidence_gate_status": paper_live_evidence_gate_report["status"],
            "paper_live_evidence_gate_paper_allowed": paper_live_evidence_gate_report["paper_allowed"],
            "paper_live_evidence_gate_live_allowed": paper_live_evidence_gate_report["live_allowed"],
            "paper_live_evidence_gate_run_evidence_ready": paper_live_evidence_gate_report["run_evidence_ready"],
            "paper_live_evidence_gate_missing_evidence_ready": paper_live_evidence_gate_report["missing_evidence_ready"],
        },
        "data_quality_report": data_quality_report,
        "reproducibility_report": reproducibility_report,
        "materialized_cache_report": materialized_cache_report,
        "raw_orderfilled_replay_contract_report": raw_orderfilled_replay_contract_report,
        "historical_l2_alignment_report": historical_l2_alignment_report,
        "environment_incident_report": environment_incident_report,
        "external_signal_contract_report": external_signal_contract_report,
        "execution_semantics_report": execution_semantics_report,
        "fill_probability_evidence_report": fill_probability_evidence_report,
        "maker_taker_execution_report": maker_taker_execution_report,
        "maker_queue_uncertainty_report": maker_queue_uncertainty_report,
        "latency_profile_report": latency_profile_report,
        "slippage_regime_report": slippage_regime_report,
        "credibility": credibility,
        "regime_report": regime_report,
        "regime_coverage_report": regime_coverage_report,
        "execution_model_validation_report": execution_model_validation_report,
        "shadow_live_triangulation_report": shadow_live_report,
        "external_source_run_coverage_report": external_run_coverage_report,
        "external_source_missing_evidence_plan": external_missing_evidence_plan,
        "tail_risk_report": tail_risk_report,
        "prediction_quality_report": prediction_quality_report,
        "performance_score_report": performance_score_report,
        "market_lifecycle_report": lifecycle_report,
        "settlement_compatibility_report": settlement_report,
        "event_level_risk_report": event_risk_report,
        "event_stream_report": event_stream_report,
        "joint_replay_report": joint_replay_report,
        "joint_execution_report": joint_execution_report,
        "execution_ledger_parity_report": parity_report,
        "ledger_cashflow_validation_report": ledger_cashflow_report,
        "promotion_gate_report": promotion_gate_report,
        "paper_live_evidence_gate_report": paper_live_evidence_gate_report,
        "checks": checks,
        "next_actions": _next_actions(checks, credibility),
    }


def repair_backtest_run_fill_quality_artifacts(conn: Any, *, run_id: int, force: bool = False) -> dict[str, Any]:
    inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
    before = build_backtest_run_artifact_report(inputs, run_id=run_id)
    if inputs is None:
        return {"status": UNKNOWN, "run_id": run_id, "updated": False, "reason": "run_not_found", "before": before, "after": before}
    run = dict(inputs.get("run") or {})
    meta = _json_dict(run.get("meta"))
    data_quality = _json_dict(meta.get("actual_data_quality"))
    existing_fill_quality = _json_dict(data_quality.get("fill_quality")) or _json_dict(meta.get("fill_quality"))
    missing_existing_fields = [field for field in REQUIRED_FILL_QUALITY_FIELDS if field not in existing_fill_quality]
    if existing_fill_quality and not force and not missing_existing_fields:
        return {
            "status": READY,
            "run_id": run_id,
            "updated": False,
            "reason": "fill_quality_already_present",
            "before": before,
            "after": before,
        }
    if not data_quality:
        return {
            "status": MISSING,
            "run_id": run_id,
            "updated": False,
            "reason": "missing_actual_data_quality",
            "before": before,
            "after": before,
        }
    orders = list(inputs.get("orders") or [])
    replay_context = _json_dict(data_quality.get("orderfilled_replay"))
    fill_quality = build_fill_quality_report(orders, replay_context=replay_context, data_quality_report=data_quality)
    data_quality["fill_quality"] = fill_quality
    meta["actual_data_quality"] = data_quality
    metrics = fill_quality_metrics(fill_quality)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_runs
            SET meta = %s::jsonb
            WHERE run_id = %s
            """,
            (json.dumps(meta, ensure_ascii=True, default=str), int(run_id)),
        )
        cur.executemany(
            """
            INSERT INTO quant.quant_backtest_metrics (
                run_id, metric_key, metric_name, metric_group, value,
                formatted_value, delta, status, tooltip, sort_order
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, metric_key)
            DO UPDATE SET
                metric_name = EXCLUDED.metric_name,
                metric_group = EXCLUDED.metric_group,
                value = EXCLUDED.value,
                formatted_value = EXCLUDED.formatted_value,
                delta = EXCLUDED.delta,
                status = EXCLUDED.status,
                tooltip = EXCLUDED.tooltip,
                sort_order = EXCLUDED.sort_order
            """,
            [
                (
                    int(run_id),
                    row["metric_key"],
                    row["metric_name"],
                    row["metric_group"],
                    row["value"],
                    row["formatted_value"],
                    row["delta"],
                    row["status"],
                    row["tooltip"],
                    row["sort_order"],
                )
                for row in metrics
            ],
        )
    after_inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
    after = build_backtest_run_artifact_report(after_inputs, run_id=run_id)
    return {
        "status": after["status"],
        "run_id": run_id,
        "updated": True,
        "reason": "fill_quality_rebuilt_from_orders" if not missing_existing_fields else "fill_quality_rebuilt_missing_fields",
        "missing_existing_fields": missing_existing_fields,
        "metrics_written": len(metrics),
        "before": before,
        "after": after,
    }


def repair_backtest_run_fill_evidence_artifacts(conn: Any, *, run_id: int, rebuild_fill_quality: bool = True) -> dict[str, Any]:
    inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
    before = build_backtest_run_artifact_report(inputs, run_id=run_id)
    if inputs is None:
        return {"status": UNKNOWN, "run_id": run_id, "updated": False, "reason": "run_not_found", "before": before, "after": before}
    orders = [enrich_order_evidence_fields(dict(row)) for row in inputs.get("orders") or []]
    if not orders:
        return {"status": MISSING, "run_id": run_id, "updated": False, "reason": "no_orders", "before": before, "after": before}
    with conn.cursor() as cur:
        cur.executemany(
            """
            UPDATE quant.quant_backtest_orders
            SET execution_evidence_type = %s,
                raw_candidate_event_count = %s,
                raw_consumed_event_count = %s,
                block_bar_crossed = %s,
                block_bar_cross_field = %s,
                block_bar_cross_price = %s,
                meta = meta || %s::jsonb
            WHERE run_id = %s AND order_id = %s
            """,
            [
                (
                    row.get("execution_evidence_type") or "unknown",
                    int(row.get("raw_candidate_event_count") or 0),
                    int(row.get("raw_consumed_event_count") or 0),
                    bool(row.get("block_bar_crossed") or False),
                    row.get("block_bar_cross_field"),
                    row.get("block_bar_cross_price"),
                    json.dumps(
                        {
                            "execution_evidence_type": row.get("execution_evidence_type") or "unknown",
                            "raw_candidate_event_count": int(row.get("raw_candidate_event_count") or 0),
                            "raw_consumed_event_count": int(row.get("raw_consumed_event_count") or 0),
                            "block_bar_crossed": bool(row.get("block_bar_crossed") or False),
                            "block_bar_cross_field": row.get("block_bar_cross_field"),
                            "block_bar_cross_price": row.get("block_bar_cross_price"),
                        },
                        ensure_ascii=True,
                        default=str,
                    ),
                    int(run_id),
                    row.get("order_id"),
                )
                for row in orders
            ],
        )
    fill_quality_repair: dict[str, Any] | None = None
    if rebuild_fill_quality:
        fill_quality_repair = repair_backtest_run_fill_quality_artifacts(conn, run_id=run_id, force=True)
    after_inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
    after = build_backtest_run_artifact_report(after_inputs, run_id=run_id)
    evidence_counts: dict[str, int] = {}
    for row in orders:
        key = str(row.get("execution_evidence_type") or "unknown")
        evidence_counts[key] = evidence_counts.get(key, 0) + 1
    return {
        "status": after["status"],
        "run_id": run_id,
        "updated": True,
        "reason": "fill_evidence_repaired_from_order_meta",
        "orders_updated": len(orders),
        "execution_evidence_counts": dict(sorted(evidence_counts.items())),
        "fill_quality_repair": {
            key: value
            for key, value in (fill_quality_repair or {}).items()
            if key not in {"before", "after"}
        } if fill_quality_repair else None,
        "before": before,
        "after": after,
    }


def backtest_run_artifact_report_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Backtest Run Artifact Audit: {report.get('status')}",
        "",
        f"- run_id: {report.get('run_id')}",
        f"- market: {report.get('market_slug') or '-'}",
        f"- token_side: {report.get('token_side') or '-'}",
        f"- price_source: {report.get('price_source') or '-'}",
        f"- engine: {report.get('backtest_engine') or '-'}",
        f"- rows_processed: {report.get('rows_processed') or 0}",
        "",
        "| Check | Status | Detail | Evidence |",
        "| --- | --- | --- | --- |",
    ]
    for check in report.get("checks", []):
        lines.append(
            "| {name} | {status} | {detail} | `{evidence}` |".format(
                name=check.get("name", ""),
                status=check.get("status", ""),
                detail=str(check.get("detail", "")).replace("|", "\\|"),
                evidence=check.get("evidence", ""),
            )
        )
    lines.extend(["", "## Artifacts"])
    for key, value in dict(report.get("artifacts") or {}).items():
        lines.append(f"- {key}: {value}")
    credibility = report.get("credibility") if isinstance(report.get("credibility"), Mapping) else {}
    if credibility:
        lines.extend(
            [
                "",
                "## Run Credibility",
                f"- status: {credibility.get('status')}",
                f"- score: {credibility.get('score')}",
                f"- reason: {credibility.get('reason')}",
            ]
        )
        evidence = credibility.get("evidence")
        if isinstance(evidence, Mapping):
            for key, value in evidence.items():
                lines.append(f"- {key}: {value}")
    data_quality_report = report.get("data_quality_report") if isinstance(report.get("data_quality_report"), Mapping) else {}
    if data_quality_report:
        lines.extend(
            [
                "",
                "## Data Quality Report",
                f"- status: {data_quality_report.get('status')}",
                f"- quality_verdict: {data_quality_report.get('quality_verdict')}",
                f"- warning_level: {data_quality_report.get('warning_level')}",
                f"- reason: {data_quality_report.get('reason')}",
                f"- block_range: {_json_dict(data_quality_report.get('block_range')).get('from')} -> {_json_dict(data_quality_report.get('block_range')).get('to')}",
                f"- row_count: {data_quality_report.get('row_count')}",
                f"- gap_count: {data_quality_report.get('gap_count')}",
                f"- max_gap: {data_quality_report.get('max_gap')}",
                f"- stale_count: {data_quality_report.get('stale_count')}",
                f"- fallback_count: {data_quality_report.get('fallback_count')}",
                f"- duplicate_count: {data_quality_report.get('duplicate_count')}",
            ]
        )
    reproducibility_report = report.get("reproducibility_report") if isinstance(report.get("reproducibility_report"), Mapping) else {}
    if reproducibility_report:
        lines.extend(
            [
                "",
                "## Reproducibility Report",
                f"- status: {reproducibility_report.get('status')}",
                f"- reproducibility_verdict: {reproducibility_report.get('reproducibility_verdict')}",
                f"- reason: {reproducibility_report.get('reason')}",
                f"- code_commit: {reproducibility_report.get('code_commit')}",
                f"- strategy: {reproducibility_report.get('strategy_name')}@{reproducibility_report.get('strategy_version')}",
                f"- parameter_fingerprint: {reproducibility_report.get('parameter_fingerprint')}",
                f"- data_version: {reproducibility_report.get('data_version')}",
                f"- block_range: {_json_dict(reproducibility_report.get('block_range')).get('from')} -> {_json_dict(reproducibility_report.get('block_range')).get('to')}",
            ]
        )
    materialized_cache_report = report.get("materialized_cache_report") if isinstance(report.get("materialized_cache_report"), Mapping) else {}
    if materialized_cache_report:
        raw_window = _json_dict(materialized_cache_report.get("raw_detail_window"))
        contract = _json_dict(materialized_cache_report.get("data_access_contract"))
        lines.extend(
            [
                "",
                "## Materialized Replay Cache Report",
                f"- status: {materialized_cache_report.get('status')}",
                f"- cache_verdict: {materialized_cache_report.get('cache_verdict')}",
                f"- reason: {materialized_cache_report.get('reason')}",
                f"- source_table: {materialized_cache_report.get('source_table')}",
                f"- access_path: {materialized_cache_report.get('access_path')}",
                f"- data_version: {materialized_cache_report.get('data_version')}",
                f"- cache_key: {materialized_cache_report.get('cache_key')}",
                f"- materialized_price_input: {materialized_cache_report.get('materialized_price_input')}",
                f"- keyed_access_path: {materialized_cache_report.get('keyed_access_path')}",
                f"- strict_keyed_access_path: {materialized_cache_report.get('strict_keyed_access_path')}",
                f"- bounded_raw_detail: {materialized_cache_report.get('bounded_raw_detail')}",
                f"- data_access_contract: {contract.get('status') or '-'} {contract.get('contract_version') or '-'}",
                f"- raw_detail_has_limit: {contract.get('raw_detail_has_limit') if contract else '-'}",
                f"- raw_detail_window: {raw_window.get('source')} {raw_window.get('from_block')} -> {raw_window.get('to_block')}",
                f"- missing_snapshot_fields: {', '.join(str(item) for item in materialized_cache_report.get('missing_snapshot_fields') or []) or '-'}",
            ]
        )
    raw_replay_contract = report.get("raw_orderfilled_replay_contract_report") if isinstance(report.get("raw_orderfilled_replay_contract_report"), Mapping) else {}
    if raw_replay_contract:
        raw_window = _json_dict(raw_replay_contract.get("loaded_block_window"))
        lines.extend(
            [
                "",
                "## Raw OrderFilled Replay Contract",
                f"- status: {raw_replay_contract.get('status')}",
                f"- contract_verdict: {raw_replay_contract.get('contract_verdict')}",
                f"- reason: {raw_replay_contract.get('reason')}",
                f"- source: {raw_replay_contract.get('source')}",
                f"- fallback: {raw_replay_contract.get('fallback')}",
                f"- loaded_event_count: {raw_replay_contract.get('loaded_event_count')}",
                f"- deduped_event_count: {raw_replay_contract.get('deduped_event_count')}",
                f"- duplicate_event_count: {raw_replay_contract.get('duplicate_event_count')}",
                f"- exact_duplicate_event_count: {raw_replay_contract.get('exact_duplicate_event_count')}",
                f"- conflicting_duplicate_event_count: {raw_replay_contract.get('conflicting_duplicate_event_count')}",
                f"- raw_trade_tick_count: {raw_replay_contract.get('raw_trade_tick_count')}",
                f"- raw_block_count: {raw_replay_contract.get('raw_block_count')}",
                f"- canonical_fill_key_coverage_pct: {raw_replay_contract.get('canonical_fill_key_coverage_pct')}",
                f"- maker_taker_side_coverage_pct: {raw_replay_contract.get('maker_taker_side_coverage_pct')}",
                f"- block_context_coverage_pct: {raw_replay_contract.get('block_context_coverage_pct')}",
                f"- block_vwap_available_pct: {raw_replay_contract.get('block_vwap_available_pct')}",
                f"- block_high_low_available_pct: {raw_replay_contract.get('block_high_low_available_pct')}",
                f"- block_order_sequence_coverage_pct: {raw_replay_contract.get('block_order_sequence_coverage_pct')}",
                f"- multi_trade_sequence_coverage_pct: {raw_replay_contract.get('multi_trade_sequence_coverage_pct')}",
                f"- loaded_block_window: {raw_window.get('from_block')} -> {raw_window.get('to_block')}",
            ]
        )
    historical_l2 = report.get("historical_l2_alignment_report") if isinstance(report.get("historical_l2_alignment_report"), Mapping) else {}
    if historical_l2:
        lines.extend(
            [
                "",
                "## Historical L2 Alignment Report",
                f"- status: {historical_l2.get('status')}",
                f"- alignment_verdict: {historical_l2.get('alignment_verdict')}",
                f"- reason: {historical_l2.get('reason')}",
                f"- source: {historical_l2.get('source')}",
                f"- aligned: {historical_l2.get('aligned_count')} / {historical_l2.get('orderfilled_rows_matched')}",
                f"- alignment_pct: {historical_l2.get('alignment_pct')}",
                f"- pmxt_rows_seen: {historical_l2.get('pmxt_rows_seen')}",
                f"- stale_l2_count: {historical_l2.get('stale_l2_count')}",
                f"- missing_l2_before_fill_count: {historical_l2.get('missing_l2_before_fill_count')}",
                f"- missing_timestamp_count: {historical_l2.get('missing_timestamp_count')}",
                f"- price_outside_spread_count: {historical_l2.get('price_outside_spread_count')}",
                f"- depth_checked_count: {historical_l2.get('depth_checked_count')}",
                f"- depth_sufficient_count: {historical_l2.get('depth_sufficient_count')}",
                f"- depth_insufficient_count: {historical_l2.get('depth_insufficient_count')}",
                f"- crossable_depth_sufficient_count: {historical_l2.get('crossable_depth_sufficient_count')}",
                f"- depth_sufficient_pct: {historical_l2.get('depth_sufficient_pct')}",
                f"- crossable_depth_sufficient_pct: {historical_l2.get('crossable_depth_sufficient_pct')}",
                f"- max_lag_ms: {historical_l2.get('max_lag_ms')}",
            ]
        )
    execution_semantics = report.get("execution_semantics_report") if isinstance(report.get("execution_semantics_report"), Mapping) else {}
    if execution_semantics:
        latency = _json_dict(execution_semantics.get("latency"))
        lines.extend(
            [
                "",
                "## Execution Semantics Report",
                f"- status: {execution_semantics.get('status')}",
                f"- semantics_verdict: {execution_semantics.get('semantics_verdict')}",
                f"- reason: {execution_semantics.get('reason')}",
                f"- order_count: {execution_semantics.get('order_count')}",
                f"- role_counts: {_json_dict(execution_semantics.get('role_counts'))}",
                f"- order_type_counts: {_json_dict(execution_semantics.get('order_type_counts'))}",
                f"- time_in_force_counts: {_json_dict(execution_semantics.get('time_in_force_counts'))}",
                f"- no_fill_reason_counts: {_json_dict(execution_semantics.get('no_fill_reason_counts'))}",
                f"- avg_latency_blocks: {latency.get('avg_latency_blocks')}",
                f"- avg_latency_seconds: {latency.get('avg_latency_seconds')}",
                f"- caveats: {', '.join(str(item) for item in execution_semantics.get('caveats') or []) or '-'}",
            ]
        )
    fill_probability_evidence = report.get("fill_probability_evidence_report") if isinstance(report.get("fill_probability_evidence_report"), Mapping) else {}
    if fill_probability_evidence:
        lines.extend(
            [
                "",
                "## Fill Probability Evidence Report",
                f"- status: {fill_probability_evidence.get('status')}",
                f"- evidence_verdict: {fill_probability_evidence.get('evidence_verdict')}",
                f"- reason: {fill_probability_evidence.get('reason')}",
                f"- ready_order_count: {fill_probability_evidence.get('ready_order_count')} / {fill_probability_evidence.get('order_count')}",
                f"- orderfilled_order_count: {fill_probability_evidence.get('orderfilled_order_count')}",
                f"- source_counts: {_json_dict(fill_probability_evidence.get('source_counts'))}",
                f"- fill_probability_buckets: {_json_dict(fill_probability_evidence.get('fill_probability_buckets'))}",
                f"- participation_buckets: {_json_dict(fill_probability_evidence.get('participation_buckets'))}",
                f"- missing_field_counts: {_json_dict(fill_probability_evidence.get('missing_field_counts'))}",
                f"- totals: {_json_dict(fill_probability_evidence.get('totals'))}",
                f"- averages: {_json_dict(fill_probability_evidence.get('averages'))}",
            ]
        )
    maker_taker = report.get("maker_taker_execution_report") if isinstance(report.get("maker_taker_execution_report"), Mapping) else {}
    if maker_taker:
        lines.extend(
            [
                "",
                "## Maker/Taker Execution Report",
                f"- status: {maker_taker.get('status')}",
                f"- maker_taker_verdict: {maker_taker.get('maker_taker_verdict')}",
                f"- execution_scope: {maker_taker.get('execution_scope')}",
                f"- reason: {maker_taker.get('reason')}",
                f"- role_counts: {_json_dict(maker_taker.get('role_counts'))}",
                f"- missing_roles: {', '.join(str(item) for item in maker_taker.get('missing_roles') or []) or '-'}",
            ]
        )
        summaries = maker_taker.get("role_summaries")
        if isinstance(summaries, Mapping):
            for role, summary in summaries.items():
                if isinstance(summary, Mapping):
                    lines.append(
                        "- {role}: orders={orders} fill_rate={fill_rate}% no_fill={no_fill} avg_slip={slip} fee={fee} rebate={rebate} markout_1={markout}".format(
                            role=role,
                            orders=summary.get("submitted_count", 0),
                            fill_rate=summary.get("fill_rate", "0"),
                            no_fill=summary.get("no_fill_count", 0),
                            slip=summary.get("avg_slippage_cost", "0"),
                            fee=summary.get("fee_cost", "0"),
                            rebate=summary.get("rebate", "0"),
                            markout=summary.get("avg_markout_after_1_bars") or "-",
                        )
                    )
    maker_queue = report.get("maker_queue_uncertainty_report") if isinstance(report.get("maker_queue_uncertainty_report"), Mapping) else {}
    if maker_queue:
        lines.extend(
            [
                "",
                "## Maker Queue Uncertainty Report",
                f"- status: {maker_queue.get('status')}",
                f"- queue_uncertainty_verdict: {maker_queue.get('queue_uncertainty_verdict')}",
                f"- execution_scope: {maker_queue.get('execution_scope')}",
                f"- reason: {maker_queue.get('reason')}",
                f"- maker_order_count: {maker_queue.get('maker_order_count')}",
                f"- maker_filled_count: {maker_queue.get('maker_filled_count')}",
                f"- risk_order_count: {maker_queue.get('risk_order_count')}",
                f"- high_participation_order_count: {maker_queue.get('high_participation_order_count')}",
                f"- thin_evidence_order_count: {maker_queue.get('thin_evidence_order_count')}",
                f"- missing_queue_evidence_count: {maker_queue.get('missing_queue_evidence_count')}",
                f"- suggested_fill_haircut_pct: {maker_queue.get('suggested_fill_haircut_pct')}",
            ]
        )
        risk_rows = maker_queue.get("risk_orders")
        if isinstance(risk_rows, Sequence) and not isinstance(risk_rows, (str, bytes, bytearray)):
            for row in risk_rows[:10]:
                if isinstance(row, Mapping):
                    lines.append(
                        "- {order_id}: status={status} participation={participation}% trade_count={trade_count} risk={risk_level} reasons={reasons}".format(
                            order_id=row.get("order_id") or "-",
                            status=row.get("status") or "-",
                            participation=row.get("participation_rate") or "0",
                            trade_count=row.get("trade_count") or 0,
                            risk_level=row.get("risk_level") or "-",
                            reasons=", ".join(str(item) for item in row.get("risk_reasons") or []) or "-",
                        )
                    )
    latency_profile = report.get("latency_profile_report") if isinstance(report.get("latency_profile_report"), Mapping) else {}
    if latency_profile:
        lines.extend(
            [
                "",
                "## Latency Profile Report",
                f"- status: {latency_profile.get('status')}",
                f"- latency_verdict: {latency_profile.get('latency_verdict')}",
                f"- latency_profile: {latency_profile.get('latency_profile')}",
                f"- reason: {latency_profile.get('reason')}",
                f"- configured_latency_seconds: {latency_profile.get('configured_latency_seconds')}",
                f"- configured_latency_blocks: {latency_profile.get('configured_latency_blocks')}",
                f"- avg_order_latency_seconds: {latency_profile.get('avg_order_latency_seconds')}",
                f"- avg_cancel_latency_seconds: {latency_profile.get('avg_cancel_latency_seconds')}",
                f"- avg_effective_latency_x_span: {latency_profile.get('avg_effective_latency_x_span')}",
                f"- fak_fok_order_count: {latency_profile.get('fak_fok_order_count')}",
                f"- cancel_sensitive_order_count: {latency_profile.get('cancel_sensitive_order_count')}",
                f"- stale_price_risk_count: {latency_profile.get('stale_price_risk_count')}",
                f"- missing_latency_count: {latency_profile.get('missing_latency_count')}",
                f"- review_reasons: {', '.join(str(item) for item in latency_profile.get('review_reasons') or []) or '-'}",
            ]
        )
    environment_incident = report.get("environment_incident_report") if isinstance(report.get("environment_incident_report"), Mapping) else {}
    if environment_incident:
        lines.extend(
            [
                "",
                "## Environment Incident Report",
                f"- status: {environment_incident.get('status')}",
                f"- incident_verdict: {environment_incident.get('incident_verdict')}",
                f"- reason: {environment_incident.get('reason')}",
                f"- incident_count: {environment_incident.get('incident_count')}",
                f"- severity_counts: {_json_dict(environment_incident.get('severity_counts'))}",
                f"- component_counts: {_json_dict(environment_incident.get('component_counts'))}",
                f"- environment_flag_count: {environment_incident.get('environment_flag_count')}",
                f"- order_anomaly_count: {environment_incident.get('order_anomaly_count')}",
                f"- review_reasons: {', '.join(str(item) for item in environment_incident.get('review_reasons') or []) or '-'}",
            ]
        )
        incidents = environment_incident.get("incidents")
        if isinstance(incidents, Sequence) and not isinstance(incidents, (str, bytes, bytearray)):
            for row in incidents[:10]:
                if isinstance(row, Mapping):
                    lines.append(
                        "- {key}: severity={severity} component={component} title={title} blocks={start_block}->{end_block}".format(
                            key=row.get("incident_key") or "-",
                            severity=row.get("severity") or "-",
                            component=row.get("component") or "-",
                            title=row.get("title") or "-",
                            start_block=row.get("start_block") or "-",
                            end_block=row.get("end_block") or "-",
                        )
                    )
    external_signal_contract = report.get("external_signal_contract_report") if isinstance(report.get("external_signal_contract_report"), Mapping) else {}
    if external_signal_contract:
        lines.extend(
            [
                "",
                "## External Signal Contract Report",
                f"- status: {external_signal_contract.get('status')}",
                f"- signal_verdict: {external_signal_contract.get('signal_verdict')}",
                f"- reason: {external_signal_contract.get('reason')}",
                f"- event_count: {external_signal_contract.get('event_count')}",
                f"- missing_required_field_count: {external_signal_contract.get('missing_required_field_count')}",
                f"- timestamp_alignment_status: {external_signal_contract.get('timestamp_alignment_status')}",
                f"- resolution_compatibility_status: {external_signal_contract.get('resolution_compatibility_status')}",
                f"- source_counts: {_json_dict(external_signal_contract.get('source_counts'))}",
                f"- review_reasons: {', '.join(str(item) for item in external_signal_contract.get('review_reasons') or []) or '-'}",
            ]
        )
    slippage_regime = report.get("slippage_regime_report") if isinstance(report.get("slippage_regime_report"), Mapping) else {}
    if slippage_regime:
        lines.extend(
            [
                "",
                "## Slippage Regime Report",
                f"- status: {slippage_regime.get('status')}",
                f"- slippage_regime_verdict: {slippage_regime.get('slippage_regime_verdict')}",
                f"- reason: {slippage_regime.get('reason')}",
                f"- configured_slippage_bps: {slippage_regime.get('configured_slippage_bps')}",
                f"- configured_adverse_slippage_cents: {slippage_regime.get('configured_adverse_slippage_cents')}",
                f"- avg_slippage_cost: {slippage_regime.get('avg_slippage_cost')}",
                f"- avg_slippage_bps: {slippage_regime.get('avg_slippage_bps')}",
                f"- risk_order_count: {slippage_regime.get('risk_order_count')}",
                f"- risk_regime_count: {slippage_regime.get('risk_regime_count')}",
                f"- review_reasons: {', '.join(str(item) for item in slippage_regime.get('review_reasons') or []) or '-'}",
            ]
        )
        risk_rows = slippage_regime.get("risk_orders")
        if isinstance(risk_rows, Sequence) and not isinstance(risk_rows, (str, bytes, bytearray)):
            for row in risk_rows[:10]:
                if isinstance(row, Mapping):
                    lines.append(
                        "- {order_id}: price={price} participation={participation}% slip_bps={slip_bps} regimes={regimes} reasons={reasons}".format(
                            order_id=row.get("order_id") or "-",
                            price=row.get("price") or "0",
                            participation=row.get("participation_rate") or "0",
                            slip_bps=row.get("slippage_bps") or "0",
                            regimes=", ".join(str(item) for item in row.get("risk_regimes") or []) or "-",
                            reasons=", ".join(str(item) for item in row.get("risk_reasons") or []) or "-",
                        )
                    )
    regime_report = report.get("regime_report") if isinstance(report.get("regime_report"), Mapping) else {}
    if regime_report:
        lines.extend(
            [
                "",
                "## Execution Regime Report",
                f"- status: {regime_report.get('status')}",
                f"- reason: {regime_report.get('reason')}",
            ]
        )
        dimensions = regime_report.get("dimensions")
        if isinstance(dimensions, Mapping):
            for dimension, rows in dimensions.items():
                if not isinstance(rows, Sequence):
                    continue
                lines.append(f"- {dimension}: {len(rows)} buckets")
    regime_coverage_report = report.get("regime_coverage_report") if isinstance(report.get("regime_coverage_report"), Mapping) else {}
    if regime_coverage_report:
        lines.extend(
            [
                "",
                "## Regime Coverage Report",
                f"- status: {regime_coverage_report.get('status')}",
                f"- coverage_verdict: {regime_coverage_report.get('coverage_verdict')}",
                f"- strategy_scope: {regime_coverage_report.get('strategy_scope')}",
                f"- regime_specific: {regime_coverage_report.get('regime_specific')}",
                f"- reason: {regime_coverage_report.get('reason')}",
                f"- narrow_dimensions: {', '.join(str(item) for item in regime_coverage_report.get('narrow_dimensions') or []) or '-'}",
            ]
        )
    execution_model_report = report.get("execution_model_validation_report") if isinstance(report.get("execution_model_validation_report"), Mapping) else {}
    if execution_model_report:
        model_comparison = _json_dict(execution_model_report.get("model_comparison"))
        sensitivity = _json_dict(execution_model_report.get("l2_profile_sensitivity"))
        lines.extend(
            [
                "",
                "## Execution Model Validation Report",
                f"- status: {execution_model_report.get('status')}",
                f"- validation_verdict: {execution_model_report.get('validation_verdict')}",
                f"- reason: {execution_model_report.get('reason')}",
                f"- present_models: {', '.join(str(item) for item in execution_model_report.get('present_models') or []) or '-'}",
                f"- missing_models: {', '.join(str(item) for item in execution_model_report.get('missing_models') or []) or '-'}",
                f"- l2_fill_rate: {model_comparison.get('l2_fill_rate')}",
                f"- l2_pnl_delta_vs_max_baseline: {model_comparison.get('l2_pnl_delta_vs_max_baseline')}",
                f"- l2_profile_monotonic_fill_rate_ok: {sensitivity.get('monotonic_fill_rate_ok')}",
            ]
        )
    shadow_live_report = report.get("shadow_live_triangulation_report") if isinstance(report.get("shadow_live_triangulation_report"), Mapping) else {}
    if shadow_live_report:
        drift = _json_dict(shadow_live_report.get("drift_summary"))
        lines.extend(
            [
                "",
                "## Shadow/Live Triangulation Report",
                f"- status: {shadow_live_report.get('status')}",
                f"- triangulation_verdict: {shadow_live_report.get('triangulation_verdict')}",
                f"- fill_model_suspect: {shadow_live_report.get('fill_model_suspect')}",
                f"- reason: {shadow_live_report.get('reason')}",
                f"- shadow_live_sample_count: {shadow_live_report.get('shadow_live_sample_count')}",
                f"- cost_sample_count: {shadow_live_report.get('cost_sample_count')}",
                f"- status_error_rate: {drift.get('status_error_rate')}",
                f"- avg_price_error: {drift.get('avg_price_error')}",
                f"- avg_latency_error_seconds: {drift.get('avg_latency_error_seconds')}",
                f"- total_cost_amount_error: {drift.get('total_cost_amount_error')}",
            ]
        )
    external_run_coverage = report.get("external_source_run_coverage_report") if isinstance(report.get("external_source_run_coverage_report"), Mapping) else {}
    if external_run_coverage:
        lines.extend(
            [
                "",
                "## External Source Run Coverage",
                f"- status: {external_run_coverage.get('status')}",
                f"- reason: {external_run_coverage.get('reason')}",
                f"- external_order_candidate_count: {external_run_coverage.get('external_order_candidate_count')}",
                f"- order_state_coverage_pct: {external_run_coverage.get('order_state_coverage_pct')}",
                f"- calibration_sample_count: {external_run_coverage.get('calibration_sample_count')}",
                f"- calibration_coverage_pct: {external_run_coverage.get('calibration_coverage_pct')}",
                f"- cost_calibration_sample_count: {external_run_coverage.get('cost_calibration_sample_count')}",
                f"- platform_incident_count: {external_run_coverage.get('platform_incident_count')}",
                f"- external_source_state_count: {external_run_coverage.get('external_source_state_count')}",
            ]
        )
    missing_plan = report.get("external_source_missing_evidence_plan") if isinstance(report.get("external_source_missing_evidence_plan"), Mapping) else {}
    if missing_plan:
        lines.extend(
            [
                "",
                "## Missing External Evidence Plan",
                f"- status: {missing_plan.get('status')}",
                f"- reason: {missing_plan.get('reason')}",
                f"- external_order_candidate_count: {missing_plan.get('external_order_candidate_count')}",
                f"- missing_order_state_count: {missing_plan.get('missing_order_state_count')}",
                f"- missing_calibration_count: {missing_plan.get('missing_calibration_count')}",
                f"- event_template_count: {len(missing_plan.get('event_templates') or [])}",
            ]
        )
        commands = missing_plan.get("commands") if isinstance(missing_plan.get("commands"), Mapping) else {}
        if commands:
            lines.append(f"- export_missing_event_templates: `{commands.get('export_missing_event_templates')}`")
    task_pack = report.get("missing_evidence_task_pack") if isinstance(report.get("missing_evidence_task_pack"), Mapping) else {}
    if task_pack:
        files = task_pack.get("files") if isinstance(task_pack.get("files"), Mapping) else {}
        lines.extend(
            [
                "",
                "## Missing Evidence Task Pack",
                f"- status: {task_pack.get('status')}",
                f"- output_dir: {task_pack.get('output_dir')}",
                f"- missing_order_state_count: {task_pack.get('missing_order_state_count')}",
                f"- missing_calibration_count: {task_pack.get('missing_calibration_count')}",
                f"- event_template_count: {task_pack.get('event_template_count')}",
            ]
        )
        for name, path in files.items():
            lines.append(f"- {name}: `{path}`")
    promotion_gate = report.get("promotion_gate_report") if isinstance(report.get("promotion_gate_report"), Mapping) else {}
    if promotion_gate:
        lines.extend(
            [
                "",
                "## Fill-First Promotion Gate",
                f"- status: {promotion_gate.get('status')}",
                f"- promotion_verdict: {promotion_gate.get('promotion_verdict')}",
                f"- production_promotion_allowed: {promotion_gate.get('production_promotion_allowed')}",
                f"- paper_promotion_allowed: {promotion_gate.get('paper_promotion_allowed')}",
                f"- allowed_next_modes: {', '.join(str(item) for item in promotion_gate.get('allowed_next_modes') or []) or '-'}",
                f"- blocked_reasons: {', '.join(str(item) for item in promotion_gate.get('blocked_reasons') or []) or '-'}",
                f"- review_reasons: {', '.join(str(item) for item in promotion_gate.get('review_reasons') or []) or '-'}",
                f"- missing_reasons: {', '.join(str(item) for item in promotion_gate.get('missing_reasons') or []) or '-'}",
            ]
        )
    paper_live_gate = report.get("paper_live_evidence_gate_report") if isinstance(report.get("paper_live_evidence_gate_report"), Mapping) else {}
    if paper_live_gate:
        lines.extend(
            [
                "",
                "## Paper/Live Evidence Gate",
                f"- status: {paper_live_gate.get('status')}",
                f"- paper_allowed: {paper_live_gate.get('paper_allowed')}",
                f"- live_allowed: {paper_live_gate.get('live_allowed')}",
                f"- run_evidence_ready: {paper_live_gate.get('run_evidence_ready')}",
                f"- missing_evidence_ready: {paper_live_gate.get('missing_evidence_ready')}",
                f"- promotion_ready: {paper_live_gate.get('promotion_ready')}",
                f"- order_state_coverage_pct: {paper_live_gate.get('order_state_coverage_pct')}",
                f"- calibration_coverage_pct: {paper_live_gate.get('calibration_coverage_pct')}",
                f"- missing_order_state_count: {paper_live_gate.get('missing_order_state_count')}",
                f"- missing_calibration_count: {paper_live_gate.get('missing_calibration_count')}",
                f"- blocked_reasons: {', '.join(str(item) for item in paper_live_gate.get('blocked_reasons') or []) or '-'}",
                f"- review_reasons: {', '.join(str(item) for item in paper_live_gate.get('review_reasons') or []) or '-'}",
            ]
        )
    tail_risk_report = report.get("tail_risk_report") if isinstance(report.get("tail_risk_report"), Mapping) else {}
    if tail_risk_report:
        lines.extend(
            [
                "",
                "## Tail Risk Report",
                f"- status: {tail_risk_report.get('status')}",
                f"- risk_verdict: {tail_risk_report.get('risk_verdict')}",
                f"- reason: {tail_risk_report.get('reason')}",
                f"- max_single_loss: {tail_risk_report.get('max_single_loss')}",
                f"- consecutive_loss_streak: {tail_risk_report.get('consecutive_loss_streak')}",
                f"- losses_to_ruin: {_json_dict(tail_risk_report.get('ruin_risk')).get('losses_to_ruin')}",
                f"- max_trade_notional_pct_of_capital: {_json_dict(tail_risk_report.get('position_concentration')).get('max_trade_notional_pct_of_capital')}",
            ]
        )
    prediction_quality_report = report.get("prediction_quality_report") if isinstance(report.get("prediction_quality_report"), Mapping) else {}
    if prediction_quality_report:
        lines.extend(
            [
                "",
                "## Prediction Quality Report",
                f"- status: {prediction_quality_report.get('status')}",
                f"- prediction_verdict: {prediction_quality_report.get('prediction_verdict')}",
                f"- reason: {prediction_quality_report.get('reason')}",
                f"- sample_count: {prediction_quality_report.get('sample_count')}",
                f"- brier_score: {prediction_quality_report.get('brier_score')}",
                f"- market_brier_score: {prediction_quality_report.get('market_brier_score')}",
                f"- brier_advantage: {prediction_quality_report.get('brier_advantage')}",
                f"- baseline_fallback_count: {prediction_quality_report.get('baseline_fallback_count')}",
            ]
        )
    performance_score_report = report.get("performance_score_report") if isinstance(report.get("performance_score_report"), Mapping) else {}
    if performance_score_report:
        penalties = _json_dict(performance_score_report.get("penalties"))
        lines.extend(
            [
                "",
                "## Performance Score",
                f"- status: {performance_score_report.get('status')}",
                f"- ranking_verdict: {performance_score_report.get('ranking_verdict')}",
                f"- reason: {performance_score_report.get('reason')}",
                f"- performance_score: {performance_score_report.get('performance_score')}",
                f"- net_pnl: {performance_score_report.get('net_pnl')}",
                f"- max_drawdown: {performance_score_report.get('max_drawdown')}",
                f"- sharpe: {performance_score_report.get('sharpe')}",
                f"- sortino: {performance_score_report.get('sortino')}",
                f"- calmar: {performance_score_report.get('calmar')}",
                f"- coverage_penalty: {penalties.get('coverage_penalty')}",
                f"- low_fill_penalty: {penalties.get('low_fill_penalty')}",
            ]
        )
    lifecycle_report = report.get("market_lifecycle_report") if isinstance(report.get("market_lifecycle_report"), Mapping) else {}
    if lifecycle_report:
        lines.extend(
            [
                "",
                "## Market Lifecycle Report",
                f"- status: {lifecycle_report.get('status')}",
                f"- lifecycle_verdict: {lifecycle_report.get('lifecycle_verdict')}",
                f"- reason: {lifecycle_report.get('reason')}",
                f"- market_lifecycle_status: {lifecycle_report.get('market_lifecycle_status')}",
                f"- resolved_outcome: {lifecycle_report.get('resolved_outcome')}",
                f"- end_date: {lifecycle_report.get('end_date')}",
            ]
        )
    settlement_report = report.get("settlement_compatibility_report") if isinstance(report.get("settlement_compatibility_report"), Mapping) else {}
    if settlement_report:
        lines.extend(
            [
                "",
                "## Settlement Compatibility Report",
                f"- status: {settlement_report.get('status')}",
                f"- compatibility_verdict: {settlement_report.get('compatibility_verdict')}",
                f"- reason: {settlement_report.get('reason')}",
                f"- final_valuation_mode: {settlement_report.get('final_valuation_mode')}",
                f"- settlement_value: {settlement_report.get('settlement_value')}",
                f"- resolution_source: {settlement_report.get('resolution_source')}",
                f"- settlement_rule: {settlement_report.get('settlement_rule')}",
                f"- price_to_beat_source: {settlement_report.get('price_to_beat_source')}",
                f"- oracle_source: {settlement_report.get('oracle_source')}",
            ]
        )
    event_risk_report = report.get("event_level_risk_report") if isinstance(report.get("event_level_risk_report"), Mapping) else {}
    if event_risk_report:
        probability_sum = _json_dict(event_risk_report.get("probability_sum"))
        portfolio_risk = _json_dict(event_risk_report.get("portfolio_risk"))
        lines.extend(
            [
                "",
                "## Event-Level Risk Report",
                f"- status: {event_risk_report.get('status')}",
                f"- risk_verdict: {event_risk_report.get('risk_verdict')}",
                f"- reason: {event_risk_report.get('reason')}",
                f"- event_slug: {event_risk_report.get('event_slug')}",
                f"- observed_outcome_count: {event_risk_report.get('observed_outcome_count')}",
                f"- expected_outcome_count: {event_risk_report.get('expected_outcome_count')}",
                f"- probability_sum: {probability_sum.get('value')}",
                f"- complement_status: {_json_dict(event_risk_report.get('yes_no_complement')).get('status')}",
                f"- correlation_status: {_json_dict(event_risk_report.get('outcome_correlation')).get('status')}",
                f"- portfolio_cash_at_risk: {portfolio_risk.get('portfolio_cash_at_risk')}",
            ]
        )
    event_stream_report = report.get("event_stream_report") if isinstance(report.get("event_stream_report"), Mapping) else {}
    if event_stream_report:
        lines.extend(
            [
                "",
                "## Event Stream Contract Report",
                f"- status: {event_stream_report.get('status')}",
                f"- schema_version: {event_stream_report.get('schema_version')}",
                f"- reason: {event_stream_report.get('reason')}",
                f"- event_count: {event_stream_report.get('event_count')}",
                f"- x_axes: {', '.join(str(item) for item in event_stream_report.get('x_axes') or [])}",
                f"- sorted: {event_stream_report.get('sorted')}",
                f"- missing_contract: {', '.join(str(item) for item in event_stream_report.get('missing_contract') or []) or '-'}",
            ]
        )
    joint_replay_report = report.get("joint_replay_report") if isinstance(report.get("joint_replay_report"), Mapping) else {}
    if joint_replay_report:
        lines.extend(
            [
                "",
                "## Joint Replay Plan Report",
                f"- status: {joint_replay_report.get('status')}",
                f"- joint_replay_verdict: {joint_replay_report.get('joint_replay_verdict')}",
                f"- replay_mode: {joint_replay_report.get('replay_mode')}",
                f"- reason: {joint_replay_report.get('reason')}",
                f"- outcome_count: {joint_replay_report.get('outcome_count')}",
                f"- market_count: {joint_replay_report.get('market_count')}",
                f"- event_count: {joint_replay_report.get('event_count')}",
                f"- posthoc_sum_risk: {joint_replay_report.get('posthoc_sum_risk')}",
            ]
        )
    joint_execution_report = report.get("joint_execution_report") if isinstance(report.get("joint_execution_report"), Mapping) else {}
    if joint_execution_report:
        lines.extend(
            [
                "",
                "## Joint Replay Execution Report",
                f"- status: {joint_execution_report.get('status')}",
                f"- execution_verdict: {joint_execution_report.get('execution_verdict')}",
                f"- replay_mode: {joint_execution_report.get('replay_mode')}",
                f"- reason: {joint_execution_report.get('reason')}",
                f"- submitted_orders: {joint_execution_report.get('submitted_orders')}",
                f"- fill_events: {joint_execution_report.get('fill_events')}",
                f"- ledger_events: {joint_execution_report.get('ledger_events')}",
                f"- max_cash_at_risk: {joint_execution_report.get('max_cash_at_risk')}",
                f"- portfolio_equity: {joint_execution_report.get('portfolio_equity')}",
                f"- equity_curve_points: {joint_execution_report.get('equity_curve_points')}",
                f"- max_drawdown: {joint_execution_report.get('max_drawdown')}",
                f"- max_drawdown_pct: {joint_execution_report.get('max_drawdown_pct')}",
            ]
        )
    parity_report = report.get("execution_ledger_parity_report") if isinstance(report.get("execution_ledger_parity_report"), Mapping) else {}
    if parity_report:
        lines.extend(
            [
                "",
                "## Execution/Ledger Parity Report",
                f"- status: {parity_report.get('status')}",
                f"- parity_verdict: {parity_report.get('parity_verdict')}",
                f"- reason: {parity_report.get('reason')}",
                f"- order_rows: {_json_dict(parity_report.get('order_schema')).get('row_count')}",
                f"- ledger_rows: {_json_dict(parity_report.get('ledger_schema')).get('row_count')}",
                f"- live_evidence_status: {_json_dict(parity_report.get('live_evidence')).get('status')}",
            ]
        )
    ledger_cashflow_report = report.get("ledger_cashflow_validation_report") if isinstance(report.get("ledger_cashflow_validation_report"), Mapping) else {}
    if ledger_cashflow_report:
        lines.extend(
            [
                "",
                "## Ledger Cashflow Validation Report",
                f"- status: {ledger_cashflow_report.get('status')}",
                f"- cashflow_verdict: {ledger_cashflow_report.get('cashflow_verdict')}",
                f"- reason: {ledger_cashflow_report.get('reason')}",
                f"- net_profit_trade: {ledger_cashflow_report.get('net_profit_trade')}",
                f"- net_profit_ledger: {ledger_cashflow_report.get('net_profit_ledger')}",
                f"- ledger_diff: {ledger_cashflow_report.get('ledger_diff')}",
                f"- missing_trade_ledger_count: {ledger_cashflow_report.get('missing_trade_ledger_count')}",
                f"- residual_position: {ledger_cashflow_report.get('residual_position')}",
                f"- cashflow_formula: {ledger_cashflow_report.get('cashflow_formula')}",
                f"- polymarket_cashflow_total: {ledger_cashflow_report.get('polymarket_cashflow_total')}",
                f"- polymarket_unrealized_position_value: {ledger_cashflow_report.get('polymarket_unrealized_position_value')}",
                f"- polymarket_net_trading_pnl: {ledger_cashflow_report.get('polymarket_net_trading_pnl')}",
                f"- polymarket_residual_position_count: {ledger_cashflow_report.get('polymarket_residual_position_count')}",
                f"- polymarket_portfolio_cash_at_risk: {ledger_cashflow_report.get('polymarket_portfolio_cash_at_risk')}",
                f"- polymarket_event_cash_at_risk: {_json_dict(ledger_cashflow_report.get('polymarket_event_cash_at_risk'))}",
            ]
        )
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions", []))
    return "\n".join(lines)


def _check_run_status(run: Mapping[str, Any]) -> dict[str, str]:
    status = str(run.get("status") or "unknown")
    if status == "succeeded":
        return _check("run status", READY, "run succeeded", "quant.quant_backtest_runs.status")
    if status in {"queued", "running"}:
        return _check("run status", REVIEW, f"run status is {status}", "quant.quant_backtest_runs.status")
    return _check("run status", MISSING, f"run status is {status}", "quant.quant_backtest_runs.status")


def _check_parameter_snapshot(run: Mapping[str, Any], parameters: Any) -> dict[str, str]:
    meta = _json_dict(run.get("meta"))
    snapshot = _parameter_snapshot_fields(meta)
    missing_columns = [field for field in REQUIRED_PARAMETER_FIELDS if parameters is None or _is_blank(dict(parameters).get(field))]
    missing_snapshot = [field for field in REQUIRED_PARAMETER_FIELDS if _is_blank(snapshot.get(field))]
    if _is_blank(meta.get("parameter_fingerprint")):
        return _check("parameter snapshot", MISSING, "missing parameter_fingerprint", "quant.quant_backtest_runs.meta")
    if missing_columns:
        return _check("parameter snapshot", MISSING, "missing parameter columns: " + ", ".join(missing_columns), "quant.quant_backtest_parameters")
    if missing_snapshot:
        return _check("parameter snapshot", REVIEW, "snapshot missing fields: " + ", ".join(missing_snapshot), "quant.quant_backtest_runs.meta.parameter_snapshot")
    return _check("parameter snapshot", READY, "fingerprint and required parameters saved", "quant.quant_backtest_runs.meta")


def _parameter_snapshot_fields(meta: Mapping[str, Any]) -> dict[str, Any]:
    snapshot = _json_dict(meta.get("parameter_snapshot"))
    nested = _json_dict(snapshot.get("parameters"))
    return {**snapshot, **nested}


def _check_block_window(run: Mapping[str, Any]) -> dict[str, str]:
    price_source = str(run.get("price_source") or "")
    if price_source == "orderfilled_block_close":
        if run.get("from_block") is not None and run.get("to_block") is not None:
            return _check("block/window", READY, f"block {run.get('from_block')} -> {run.get('to_block')}", "quant.quant_backtest_runs.from_block/to_block")
        return _check("block/window", MISSING, "orderfilled run missing from_block/to_block", "quant.quant_backtest_runs")
    if run.get("from_ts") is not None and run.get("to_ts") is not None:
        return _check("block/window", READY, f"timestamp {run.get('from_ts')} -> {run.get('to_ts')}", "quant.quant_backtest_runs.from_ts/to_ts")
    return _check("block/window", REVIEW, "run window is not fully bounded", "quant.quant_backtest_runs")


def _check_data_quality(data_quality: Mapping[str, Any]) -> dict[str, str]:
    if not data_quality:
        return _check("data quality artifact", MISSING, "missing actual_data_quality", "quant.quant_backtest_runs.meta.actual_data_quality")
    missing = [field for field in REQUIRED_DATA_QUALITY_FIELDS if _is_blank(data_quality.get(field))]
    if missing:
        return _check("data quality artifact", MISSING, "missing fields: " + ", ".join(missing), "quant.quant_backtest_runs.meta.actual_data_quality")
    status = READY if data_quality.get("status") == "ready" else REVIEW
    detail = f"status={data_quality.get('status')} rows={data_quality.get('rows')} version={data_quality.get('data_version') or data_quality.get('checksum')}"
    return _check("data quality artifact", status, detail, "quant.quant_backtest_runs.meta.actual_data_quality")


def _check_fill_quality(fill_quality: Mapping[str, Any], metrics: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    if not fill_quality:
        return _check("fill quality artifact", MISSING, "missing fill_quality", "quant.quant_backtest_runs.meta.actual_data_quality.fill_quality")
    missing = [field for field in REQUIRED_FILL_QUALITY_FIELDS if field not in fill_quality]
    metric_keys = {str(row.get("metric_key") or "") for row in metrics}
    if "fill_quality_fill_rate" not in metric_keys:
        missing.append("metric:fill_quality_fill_rate")
    if missing:
        return _check("fill quality artifact", MISSING, "missing fields: " + ", ".join(missing), "fill_quality + quant.quant_backtest_metrics")
    detail = f"submitted={fill_quality.get('submitted_count')} filled={fill_quality.get('filled_count')} no_fill={fill_quality.get('no_fill_count')}"
    return _check("fill quality artifact", READY, detail, "quant.quant_backtest_runs.meta.actual_data_quality.fill_quality")


def _check_orders(orders: Sequence[Mapping[str, Any]], fill_quality: Mapping[str, Any]) -> dict[str, str]:
    submitted = _to_int(fill_quality.get("submitted_count"))
    if submitted > 0 and not orders:
        return _check("order lifecycle", MISSING, "fill_quality has submitted orders but quant_backtest_orders is empty", "quant.quant_backtest_orders")
    statuses = sorted({str(row.get("status") or "unknown") for row in orders})
    if orders:
        return _check("order lifecycle", READY, f"orders={len(orders)} statuses={','.join(statuses)}", "quant.quant_backtest_orders")
    return _check("order lifecycle", REVIEW, "no submitted orders in this run", "quant.quant_backtest_orders")


def _check_execution_semantics_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check("execution semantics report", MISSING, "missing execution semantics report", "quant.backtest.run_artifacts.execution_semantics_report")
    verdict = str(report.get("semantics_verdict") or UNKNOWN)
    missing = _json_dict(report.get("missing_counts"))
    role_counts = _json_dict(report.get("role_counts"))
    order_type_counts = _json_dict(report.get("order_type_counts"))
    time_in_force_counts = _json_dict(report.get("time_in_force_counts"))
    detail = (
        f"verdict={verdict} orders={_to_int(report.get('order_count'))} "
        f"roles={','.join(role_counts) or '-'} "
        f"types={','.join(order_type_counts) or '-'} "
        f"tif={','.join(time_in_force_counts) or '-'} "
        f"missing={sum(_to_int(value) for value in missing.values())}"
    )
    return _check(
        "execution semantics report",
        READY if verdict == READY else MISSING if verdict == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.execution_semantics_report",
    )


def _check_fill_probability_evidence_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check(
            "fill probability evidence report",
            MISSING,
            "missing fill probability evidence report",
            "quant.backtest.run_artifacts.fill_probability_evidence_report",
        )
    verdict = str(report.get("evidence_verdict") or UNKNOWN)
    missing = _json_dict(report.get("missing_field_counts"))
    detail = (
        f"verdict={verdict} "
        f"ready_orders={_to_int(report.get('ready_order_count'))}/{_to_int(report.get('order_count'))} "
        f"orderfilled={_to_int(report.get('orderfilled_order_count'))} "
        f"missing={sum(_to_int(value) for value in missing.values())}"
    )
    return _check(
        "fill probability evidence report",
        READY if verdict == READY else MISSING if verdict == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.fill_probability_evidence_report",
    )


def _check_maker_taker_execution_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check(
            "maker/taker execution report",
            MISSING,
            "missing maker/taker execution report",
            "quant.backtest.run_artifacts.maker_taker_execution_report",
        )
    verdict = str(report.get("maker_taker_verdict") or UNKNOWN)
    role_counts = _json_dict(report.get("role_counts"))
    missing = report.get("missing_roles") or []
    detail = (
        f"verdict={verdict} scope={report.get('execution_scope') or UNKNOWN} "
        f"maker={_to_int(role_counts.get('maker'))} taker={_to_int(role_counts.get('taker'))} "
        f"missing={','.join(str(item) for item in missing) if isinstance(missing, Sequence) and not isinstance(missing, (str, bytes, bytearray)) else missing}"
    )
    return _check(
        "maker/taker execution report",
        MISSING if verdict == MISSING else READY if verdict == READY else REVIEW,
        detail,
        "quant.backtest.run_artifacts.maker_taker_execution_report",
    )


def _check_maker_queue_uncertainty_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check(
            "maker queue uncertainty report",
            MISSING,
            "missing maker queue uncertainty report",
            "quant.backtest.run_artifacts.maker_queue_uncertainty_report",
        )
    verdict = str(report.get("queue_uncertainty_verdict") or UNKNOWN)
    detail = (
        f"verdict={verdict} maker={_to_int(report.get('maker_order_count'))} "
        f"risk={_to_int(report.get('risk_order_count'))} "
        f"high_participation={_to_int(report.get('high_participation_order_count'))} "
        f"missing_queue={_to_int(report.get('missing_queue_evidence_count'))}"
    )
    return _check(
        "maker queue uncertainty report",
        READY if report.get("status") == READY else MISSING if report.get("status") == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.maker_queue_uncertainty_report",
    )


def _check_latency_profile_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check(
            "latency profile report",
            MISSING,
            "missing latency profile report",
            "quant.backtest.run_artifacts.latency_profile_report",
        )
    detail = (
        f"verdict={report.get('latency_verdict') or UNKNOWN} "
        f"profile={report.get('latency_profile') or UNKNOWN} "
        f"seconds={report.get('configured_latency_seconds')} blocks={report.get('configured_latency_blocks')} "
        f"stale_risk={_to_int(report.get('stale_price_risk_count'))}"
    )
    return _check(
        "latency profile report",
        READY if report.get("status") == READY else MISSING if report.get("status") == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.latency_profile_report",
    )


def _check_slippage_regime_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check(
            "slippage regime report",
            MISSING,
            "missing slippage regime report",
            "quant.backtest.run_artifacts.slippage_regime_report",
        )
    detail = (
        f"verdict={report.get('slippage_regime_verdict') or UNKNOWN} "
        f"risk_orders={_to_int(report.get('risk_order_count'))} "
        f"risk_regimes={_to_int(report.get('risk_regime_count'))} "
        f"avg_slippage_bps={report.get('avg_slippage_bps')}"
    )
    return _check(
        "slippage regime report",
        READY if report.get("status") == READY else MISSING if report.get("status") == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.slippage_regime_report",
    )


def _check_ledger(ledger: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    if not ledger:
        return _check("cashflow ledger", REVIEW, "no ledger events; possible no-trade run", "quant.quant_backtest_ledger")
    event_types = sorted({str(row.get("event_type") or "unknown") for row in ledger})
    if not any(event in event_types for event in ("BUY", "SELL", "SETTLEMENT", "FEE", "REBATE", "GAS_COST", "REDEEM_COST")):
        return _check("cashflow ledger", REVIEW, "ledger lacks recognized cashflow event types", "quant.quant_backtest_ledger")
    return _check("cashflow ledger", READY, f"events={len(ledger)} types={','.join(event_types)}", "quant.quant_backtest_ledger")


def _check_regime_report(orders: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    if not orders:
        return _check("execution regime report", REVIEW, "no orders available for regime split", "quant.quant_backtest_orders")
    report = build_execution_regime_report(orders)
    if report["status"] == READY:
        return _check(
            "execution regime report",
            READY,
            f"dimensions={len(report['dimensions'])} orders={report['order_count']}",
            "quant.backtest.run_artifacts.regime_report",
        )
    return _check("execution regime report", REVIEW, str(report.get("reason") or "review regime report"), "quant.backtest.run_artifacts.regime_report")


def _check_regime_coverage_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check("regime coverage report", MISSING, "missing regime coverage report", "quant.backtest.regime_coverage")
    verdict = str(report.get("coverage_verdict") or UNKNOWN)
    scope = str(report.get("strategy_scope") or UNKNOWN)
    narrow = report.get("narrow_dimensions") or []
    return _check(
        "regime coverage report",
        MISSING if verdict == MISSING else READY,
        f"verdict={verdict} scope={scope} narrow={','.join(str(item) for item in narrow) if isinstance(narrow, Sequence) and not isinstance(narrow, (str, bytes, bytearray)) else narrow}",
        "quant.backtest.regime_coverage",
    )


def _check_execution_model_validation_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check("execution model validation report", REVIEW, "missing execution model validation report", "quant.backtest.execution_model_validation")
    verdict = str(report.get("validation_verdict") or UNKNOWN)
    missing = report.get("missing_models") or []
    sensitivity = _json_dict(report.get("l2_profile_sensitivity"))
    detail = (
        f"verdict={verdict} "
        f"models={','.join(str(item) for item in report.get('present_models') or []) or '-'} "
        f"missing={','.join(str(item) for item in missing) if isinstance(missing, Sequence) and not isinstance(missing, (str, bytes, bytearray)) else missing} "
        f"monotonic={sensitivity.get('monotonic_fill_rate_ok')}"
    )
    return _check(
        "execution model validation report",
        READY if verdict == READY else REVIEW,
        detail,
        "quant.backtest.execution_model_validation",
    )


def _check_shadow_live_triangulation_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check("shadow/live triangulation report", MISSING, "missing shadow/live triangulation report", "quant.backtest.shadow_live_triangulation")
    verdict = str(report.get("triangulation_verdict") or UNKNOWN)
    suspect = bool(report.get("fill_model_suspect"))
    samples = _to_int(report.get("shadow_live_sample_count"))
    cost_samples = _to_int(report.get("cost_sample_count"))
    return _check(
        "shadow/live triangulation report",
        MISSING if verdict == MISSING else READY,
        f"verdict={verdict} fill_model_suspect={suspect} samples={samples} cost_samples={cost_samples}",
        "quant.backtest.shadow_live_triangulation",
    )


def _check_external_source_run_coverage_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check(
            "external source run coverage",
            MISSING,
            "missing external source run coverage report",
            "quant.backtest.external_source_run_coverage",
        )
    status = str(report.get("status") or UNKNOWN)
    check_status = MISSING if status == MISSING else READY if status == READY else REVIEW
    detail = (
        f"status={status} "
        f"order_coverage={report.get('order_state_coverage_pct', 0)}% "
        f"calibration_coverage={report.get('calibration_coverage_pct', 0)}% "
        f"candidates={_to_int(report.get('external_order_candidate_count'))} "
        f"samples={_to_int(report.get('calibration_sample_count'))}"
    )
    return _check("external source run coverage", check_status, detail, "quant.backtest.external_source_run_coverage")


def _check_external_source_missing_evidence_plan(plan: Mapping[str, Any]) -> dict[str, str]:
    if not plan:
        return _check(
            "external source missing evidence plan",
            MISSING,
            "missing external source missing evidence plan",
            "quant.backtest.external_source_missing_evidence",
        )
    status = str(plan.get("status") or UNKNOWN)
    check_status = MISSING if status == MISSING else READY if status == READY else REVIEW
    detail = (
        f"status={status} "
        f"missing_order_state={_to_int(plan.get('missing_order_state_count'))} "
        f"missing_calibration={_to_int(plan.get('missing_calibration_count'))} "
        f"event_templates={len(plan.get('event_templates') or [])}"
    )
    return _check("external source missing evidence plan", check_status, detail, "quant.backtest.external_source_missing_evidence")


def _check_promotion_gate_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check("fill-first promotion gate", MISSING, "missing promotion gate report", "quant.backtest.promotion_gate")
    verdict = str(report.get("promotion_verdict") or UNKNOWN)
    production_allowed = bool(report.get("production_promotion_allowed"))
    paper_allowed = bool(report.get("paper_promotion_allowed"))
    return _check(
        "fill-first promotion gate",
        MISSING if verdict == MISSING else READY,
        f"verdict={verdict} production_allowed={production_allowed} paper_allowed={paper_allowed}",
        "quant.backtest.promotion_gate",
    )


def _build_paper_live_evidence_gate_report(
    external_run_coverage: Mapping[str, Any],
    missing_evidence: Mapping[str, Any],
    promotion_gate: Mapping[str, Any],
) -> dict[str, Any]:
    run_evidence_ready = (
        str(external_run_coverage.get("status") or UNKNOWN) == READY
        and _pct_is_100(external_run_coverage.get("order_state_coverage_pct"))
        and _pct_is_100(external_run_coverage.get("calibration_coverage_pct"))
    )
    missing_order_state = _to_int(missing_evidence.get("missing_order_state_count"))
    missing_calibration = _to_int(missing_evidence.get("missing_calibration_count"))
    missing_evidence_ready = (
        str(missing_evidence.get("status") or UNKNOWN) == READY
        and missing_order_state == 0
        and missing_calibration == 0
    )
    promotion_ready = bool(promotion_gate.get("paper_promotion_allowed") or promotion_gate.get("production_promotion_allowed"))
    paper_allowed = run_evidence_ready and missing_evidence_ready and bool(promotion_gate.get("paper_promotion_allowed"))
    live_allowed = run_evidence_ready and missing_evidence_ready and bool(promotion_gate.get("production_promotion_allowed"))

    blocked_reasons: list[str] = []
    if not run_evidence_ready:
        blocked_reasons.extend(
            [
                f"run evidence status={external_run_coverage.get('status', UNKNOWN)}",
                f"order_state_coverage_pct={external_run_coverage.get('order_state_coverage_pct', 0)}",
                f"calibration_coverage_pct={external_run_coverage.get('calibration_coverage_pct', 0)}",
            ]
        )
    if not missing_evidence_ready:
        blocked_reasons.extend(
            [
                f"missing evidence status={missing_evidence.get('status', UNKNOWN)}",
                f"missing_order_state_count={missing_order_state}",
                f"missing_calibration_count={missing_calibration}",
            ]
        )
    blocked_reasons.extend(str(item) for item in promotion_gate.get("blocked_reasons") or [])
    blocked_reasons.extend(str(item) for item in promotion_gate.get("missing_reasons") or [])
    blocked_reasons = _unique_text(blocked_reasons)
    review_reasons = _unique_text(str(item) for item in promotion_gate.get("review_reasons") or [])
    status = READY if paper_allowed else "blocked" if blocked_reasons else REVIEW if review_reasons else MISSING
    next_actions = _unique_text(
        [
            *(str(item) for item in external_run_coverage.get("next_actions") or [] if not run_evidence_ready),
            *(str(item) for item in missing_evidence.get("next_actions") or [] if not missing_evidence_ready),
            *(f"resolve {reason}" for reason in blocked_reasons),
            *(f"review {reason}" for reason in review_reasons),
        ]
    )

    return {
        "status": status,
        "paper_allowed": paper_allowed,
        "live_allowed": live_allowed,
        "run_evidence_ready": run_evidence_ready,
        "missing_evidence_ready": missing_evidence_ready,
        "promotion_ready": promotion_ready,
        "order_state_coverage_pct": str(external_run_coverage.get("order_state_coverage_pct", "0")),
        "calibration_coverage_pct": str(external_run_coverage.get("calibration_coverage_pct", "0")),
        "missing_order_state_count": missing_order_state,
        "missing_calibration_count": missing_calibration,
        "blocked_reasons": blocked_reasons,
        "review_reasons": review_reasons,
        "next_actions": next_actions,
    }


def _check_paper_live_evidence_gate_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check("paper/live evidence gate", MISSING, "missing paper/live evidence gate", "quant.backtest.run_artifacts.paper_live_evidence_gate_report")
    status = str(report.get("status") or UNKNOWN)
    detail = (
        f"status={status} "
        f"paper_allowed={bool(report.get('paper_allowed'))} "
        f"live_allowed={bool(report.get('live_allowed'))} "
        f"run_evidence_ready={bool(report.get('run_evidence_ready'))} "
        f"missing_evidence_ready={bool(report.get('missing_evidence_ready'))}"
    )
    return _check(
        "paper/live evidence gate",
        READY if status == READY else REVIEW,
        detail,
        "quant.backtest.run_artifacts.paper_live_evidence_gate_report",
    )


def _pct_is_100(value: Any) -> bool:
    try:
        return Decimal(str(value or "0")) == Decimal("100")
    except Exception:
        return False


def _unique_text(items: Iterable[Any]) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "")
        if not text or text in seen:
            continue
        values.append(text)
        seen.add(text)
    return values


def _check_tail_risk_report(trades: Sequence[Mapping[str, Any]], params: Any) -> dict[str, str]:
    if not trades:
        return _check("tail risk report", REVIEW, "no closed trades available for tail-risk stress", "quant.quant_backtest_trades")
    report = build_tail_risk_report(trades, params)
    return _check(
        "tail risk report",
        READY if report["status"] == READY else REVIEW,
        f"trades={report['trade_count']} verdict={report['risk_verdict']} max_loss={report['max_single_loss']}",
        "quant.backtest.run_artifacts.tail_risk_report",
    )


def _check_prediction_quality_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check("prediction quality report", MISSING, "missing prediction quality report", "quant.backtest.run_artifacts.prediction_quality_report")
    status = READY if report.get("status") == READY else REVIEW
    detail = (
        f"samples={report.get('sample_count')} "
        f"verdict={report.get('prediction_verdict')} "
        f"brier={report.get('brier_score')}"
    )
    return _check("prediction quality report", status, detail, "quant.backtest.run_artifacts.prediction_quality_report")


def _check_performance_score_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check("performance score report", MISSING, "missing performance score report", "quant.backtest.performance_score")
    status = READY if report.get("status") == READY else REVIEW
    detail = (
        f"verdict={report.get('ranking_verdict')} "
        f"score={report.get('performance_score')} "
        f"sharpe={report.get('sharpe')} "
        f"calmar={report.get('calmar')}"
    )
    return _check("performance score report", status, detail, "quant.backtest.performance_score")


def _check_market_lifecycle_report(
    run: Mapping[str, Any],
    parameters: Any,
    trades: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    report = build_market_lifecycle_report(run, parameters, trades, ledger)
    return _check(
        "market lifecycle report",
        READY if report["status"] == READY else REVIEW,
        f"verdict={report['lifecycle_verdict']} status={report['market_lifecycle_status']} resolved={report['resolved_outcome']}",
        "quant.backtest.run_artifacts.market_lifecycle_report",
    )


def _check_settlement_compatibility_report(
    run: Mapping[str, Any],
    parameters: Any,
    trades: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    report = build_settlement_compatibility_report(run, parameters, trades, ledger)
    return _check(
        "settlement compatibility report",
        READY if report["status"] == READY else REVIEW,
        f"verdict={report['compatibility_verdict']} mode={report['final_valuation_mode']} settlement_trades={report['settlement_trade_count']}",
        "quant.backtest.run_artifacts.settlement_compatibility_report",
    )


def _check_quality_metrics(metrics: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    keys = {str(row.get("metric_key") or "") for row in metrics}
    required = {"data_quality_status", "gap_count", "data_version", "fill_quality_fill_rate", "fill_quality_no_fill_rate"}
    missing = sorted(required - keys)
    if missing:
        return _check("quality metrics", MISSING, "missing metrics: " + ", ".join(missing), "quant.quant_backtest_metrics")
    return _check("quality metrics", READY, f"metrics={len(metrics)}", "quant.quant_backtest_metrics")


def _check_reproducibility_report(report: Mapping[str, Any]) -> dict[str, str]:
    verdict = str(report.get("reproducibility_verdict") or UNKNOWN)
    missing = report.get("missing_fields")
    detail = f"verdict={verdict}"
    if isinstance(missing, Sequence) and not isinstance(missing, (str, bytes, bytearray)) and missing:
        detail += " missing=" + ",".join(str(item) for item in missing)
    return _check(
        "reproducibility report",
        READY if verdict == READY else MISSING if verdict == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.reproducibility_report",
    )


def _check_materialized_cache_report(report: Mapping[str, Any]) -> dict[str, str]:
    verdict = str(report.get("cache_verdict") or UNKNOWN)
    source_table = str(report.get("source_table") or "-")
    access_path = str(report.get("access_path") or "-")
    bounded_raw = bool(report.get("bounded_raw_detail"))
    return _check(
        "materialized replay cache",
        READY if verdict == READY else MISSING if verdict == MISSING else REVIEW,
        f"verdict={verdict} source={source_table} access={access_path} raw_detail_bounded={bounded_raw}",
        "quant.backtest.run_artifacts.materialized_cache_report",
    )


def _check_raw_orderfilled_replay_contract_report(report: Mapping[str, Any]) -> dict[str, str]:
    verdict = str(report.get("contract_verdict") or UNKNOWN)
    window = _json_dict(report.get("loaded_block_window"))
    detail = (
        f"verdict={verdict} loaded={_to_int(report.get('loaded_event_count'))} "
        f"deduped={_to_int(report.get('deduped_event_count'))} "
        f"raw_ticks={_to_int(report.get('raw_trade_tick_count'))} "
        f"duplicates={_to_int(report.get('duplicate_event_count'))} "
        f"exact_duplicates={_to_int(report.get('exact_duplicate_event_count'))} "
        f"conflicting_duplicates={_to_int(report.get('conflicting_duplicate_event_count'))} "
        f"canonical={report.get('canonical_fill_key_coverage_pct') or '0'}% "
        f"attribution={report.get('maker_taker_side_coverage_pct') or '0'}% "
        f"block_context={report.get('block_context_coverage_pct') or '0'}% "
        f"vwap_blocks={_to_int(report.get('block_vwap_available_count'))}/{_to_int(report.get('raw_block_count'))} "
        f"sequence_blocks={_to_int(report.get('block_order_sequence_available_count'))}/{_to_int(report.get('raw_block_count'))} "
        f"window={window.get('from_block') or '-'}->{window.get('to_block') or '-'}"
    )
    missing = report.get("missing_contract_fields")
    if isinstance(missing, Sequence) and not isinstance(missing, (str, bytes, bytearray)) and missing:
        detail += " missing=" + ",".join(str(item) for item in missing)
    return _check(
        "raw orderfilled replay contract",
        READY if verdict == READY else MISSING if verdict == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.raw_orderfilled_replay_contract_report",
    )


def _check_historical_l2_alignment_report(report: Mapping[str, Any]) -> dict[str, str]:
    verdict = str(report.get("alignment_verdict") or UNKNOWN)
    detail = (
        f"verdict={verdict} aligned={_to_int(report.get('aligned_count'))}/{_to_int(report.get('orderfilled_rows_matched'))} "
        f"outside={_to_int(report.get('price_outside_spread_count'))} "
        f"stale={_to_int(report.get('stale_l2_count'))} "
        f"missing_l2={_to_int(report.get('missing_l2_before_fill_count'))} "
        f"missing_ts={_to_int(report.get('missing_timestamp_count'))} "
        f"depth={_to_int(report.get('depth_sufficient_count'))}/{_to_int(report.get('depth_checked_count'))} "
        f"crossable={_to_int(report.get('crossable_depth_sufficient_count'))}/{_to_int(report.get('depth_checked_count'))} "
        f"source={report.get('source') or '-'}"
    )
    return _check(
        "historical l2 alignment report",
        READY if verdict == READY else MISSING if verdict == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.historical_l2_alignment_report",
    )


def _check_environment_incident_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check(
            "environment incident report",
            MISSING,
            "missing environment incident report",
            "quant.backtest.run_artifacts.environment_incident_report",
        )
    verdict = str(report.get("incident_verdict") or UNKNOWN)
    detail = (
        f"verdict={verdict} incidents={_to_int(report.get('incident_count'))} "
        f"env_flags={_to_int(report.get('environment_flag_count'))} "
        f"order_anomalies={_to_int(report.get('order_anomaly_count'))}"
    )
    return _check(
        "environment incident report",
        READY if report.get("status") == READY else MISSING if report.get("status") == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.environment_incident_report",
    )


def _check_external_signal_contract_report(report: Mapping[str, Any]) -> dict[str, str]:
    if not report:
        return _check(
            "external signal contract report",
            MISSING,
            "missing external signal contract report",
            "quant.backtest.run_artifacts.external_signal_contract_report",
        )
    verdict = str(report.get("signal_verdict") or UNKNOWN)
    detail = (
        f"verdict={verdict} events={_to_int(report.get('event_count'))} "
        f"missing_fields={_to_int(report.get('missing_required_field_count'))} "
        f"alignment={report.get('timestamp_alignment_status') or UNKNOWN} "
        f"resolution={report.get('resolution_compatibility_status') or UNKNOWN}"
    )
    return _check(
        "external signal contract report",
        READY if report.get("status") == READY else MISSING if report.get("status") == MISSING else REVIEW,
        detail,
        "quant.backtest.run_artifacts.external_signal_contract_report",
    )


def _check_execution_ledger_parity_report(report: Mapping[str, Any]) -> dict[str, str]:
    verdict = str(report.get("parity_verdict") or UNKNOWN)
    order_missing = _to_int(_json_dict(report.get("order_schema")).get("missing_field_count"))
    ledger_missing = _to_int(_json_dict(report.get("ledger_schema")).get("missing_field_count"))
    return _check(
        "execution/ledger parity report",
        READY if verdict == READY else MISSING if verdict == MISSING else REVIEW,
        f"verdict={verdict} order_missing={order_missing} ledger_missing={ledger_missing}",
        "quant.backtest.run_artifacts.execution_ledger_parity_report",
    )


def _check_ledger_cashflow_validation_report(report: Mapping[str, Any]) -> dict[str, str]:
    verdict = str(report.get("cashflow_verdict") or UNKNOWN)
    missing = _to_int(report.get("missing_trade_ledger_count"))
    diff = str(report.get("ledger_diff") or "0")
    return _check(
        "ledger cashflow validation",
        READY if verdict == READY else MISSING if verdict == MISSING else REVIEW,
        f"verdict={verdict} net_trade={report.get('net_profit_trade')} net_ledger={report.get('net_profit_ledger')} diff={diff} missing_trades={missing}",
        "quant.backtest.ledger_validation",
    )


def _check_event_level_risk_report(report: Mapping[str, Any]) -> dict[str, str]:
    verdict = str(report.get("risk_verdict") or UNKNOWN)
    observed = _to_int(report.get("observed_outcome_count"))
    probability_status = str(_json_dict(report.get("probability_sum")).get("status") or UNKNOWN)
    complement_status = str(_json_dict(report.get("yes_no_complement")).get("status") or UNKNOWN)
    return _check(
        "event-level risk report",
        READY if verdict == READY else MISSING if verdict == MISSING else REVIEW,
        f"verdict={verdict} outcomes={observed} probability_sum={probability_status} complement={complement_status}",
        "quant.backtest.run_artifacts.event_level_risk_report",
    )


def _check_event_stream_contract_report(report: Mapping[str, Any]) -> dict[str, str]:
    status = str(report.get("status") or UNKNOWN)
    missing = report.get("missing_contract")
    raw_ticks = _json_dict(report.get("raw_trade_tick_report"))
    detail = (
        f"status={status} events={_to_int(report.get('event_count'))} "
        f"raw_ticks={_to_int(raw_ticks.get('trade_tick_count'))} "
        f"raw_blocks={_to_int(raw_ticks.get('block_count'))} "
        f"canonical={raw_ticks.get('canonical_fill_key_coverage_pct') or '0'}% "
        f"attribution={raw_ticks.get('maker_taker_side_coverage_pct') or '0'}% "
        f"schema={report.get('schema_version') or '-'}"
    )
    if isinstance(missing, Sequence) and not isinstance(missing, (str, bytes, bytearray)) and missing:
        detail += " missing=" + ",".join(str(item) for item in missing)
    return _check(
        "event stream contract",
        READY if status == READY else MISSING if status == MISSING else REVIEW,
        detail,
        "quant.backtest.event_stream",
    )


def _check_joint_replay_plan_report(report: Mapping[str, Any]) -> dict[str, str]:
    status = str(report.get("status") or UNKNOWN)
    verdict = str(report.get("joint_replay_verdict") or UNKNOWN)
    detail = f"status={status} verdict={verdict} mode={report.get('replay_mode') or '-'} outcomes={_to_int(report.get('outcome_count'))} events={_to_int(report.get('event_count'))}"
    return _check(
        "joint replay plan",
        READY if status == READY else MISSING if status == MISSING else REVIEW,
        detail,
        "quant.backtest.event_stream.joint_replay_plan",
    )


def _check_joint_replay_execution_report(report: Mapping[str, Any]) -> dict[str, str]:
    status = str(report.get("status") or UNKNOWN)
    verdict = str(report.get("execution_verdict") or UNKNOWN)
    detail = (
        f"status={status} verdict={verdict} mode={report.get('replay_mode') or '-'} "
        f"events={_to_int(report.get('event_count'))} fills={_to_int(report.get('fill_events'))} "
        f"ledger={_to_int(report.get('ledger_events'))} equity_points={_to_int(report.get('equity_curve_points'))} "
        f"max_dd={report.get('max_drawdown') or '0'}"
    )
    return _check(
        "joint replay execution",
        READY if status == READY else MISSING if status == MISSING else REVIEW,
        detail,
        "quant.backtest.event_stream.joint_replay_execution",
    )


def build_run_data_quality_report(
    data_quality: Mapping[str, Any],
    fill_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    fill = dict(fill_quality or {})
    replay = _json_dict(data_quality.get("orderfilled_replay"))
    raw_summary = _json_dict(fill.get("raw_evidence_summary"))
    largest_gaps = _list_of_dicts(data_quality.get("largest_gaps"))
    max_gap = max((_to_int(row.get("span")) for row in largest_gaps), default=0)
    fallback = replay.get("fallback") or fill.get("raw_fallback")
    fallback_count = 0
    if not _is_blank(fallback):
        fallback_count += 1
    fallback_count += _to_int(fill.get("fallback_candidate_orders"))
    duplicate_count = (
        _to_int(raw_summary.get("candidate_event_duplicate_count") or fill.get("candidate_event_duplicate_count"))
        + _to_int(raw_summary.get("consumed_event_duplicate_count") or fill.get("consumed_event_duplicate_count"))
    )
    source_mix = {
        "price_source": data_quality.get("price_source"),
        "source_table": data_quality.get("source_table"),
        "access_path": data_quality.get("access_path"),
        "raw_replay_source": replay.get("source"),
        "raw_replay_fallback": None if _is_blank(fallback) else str(fallback),
        "execution_sources": _json_dict(fill.get("source_counts")),
        "execution_evidence": _json_dict(fill.get("execution_evidence_counts")),
        "fill_evidence": _json_dict(fill.get("fill_evidence_counts")),
    }
    stale_count = _to_int(data_quality.get("stale_count") or data_quality.get("stale_price_count"))
    if data_quality.get("stale_status") not in (None, "", "ready", "fresh", "ok", "OK"):
        stale_count = max(1, stale_count)
    gap_count = _to_int(data_quality.get("gap_count"))
    jump_count = _to_int(data_quality.get("jump_count"))
    row_count = _to_int(data_quality.get("rows"))
    span_coverage = _decimal(data_quality.get("span_coverage_pct"))
    warning_level = str(data_quality.get("warning_level") or "UNKNOWN").upper()
    caveats = [str(item) for item in data_quality.get("caveats") or []]
    if fallback_count:
        caveats.append(f"{fallback_count} fallback indicators")
    if duplicate_count:
        caveats.append(f"{duplicate_count} duplicate raw evidence rows")
    if stale_count:
        caveats.append(f"{stale_count} stale rows/windows")
    invalid_reasons: list[str] = []
    review_reasons: list[str] = []
    if not data_quality:
        invalid_reasons.append("missing data_quality artifact")
    if row_count <= 0:
        invalid_reasons.append("row_count is zero")
    if warning_level == "BAD":
        invalid_reasons.append("data warning level is BAD")
    if span_coverage and span_coverage < Decimal("50"):
        invalid_reasons.append("span coverage below 50%")
    if gap_count:
        review_reasons.append(f"{gap_count} x-axis gaps")
    if jump_count:
        review_reasons.append(f"{jump_count} price jumps")
    if stale_count:
        review_reasons.append(f"{stale_count} stale rows/windows")
    if fallback_count:
        review_reasons.append("fallback was used")
    if duplicate_count:
        review_reasons.append(f"{duplicate_count} duplicate raw evidence rows")
    if str(data_quality.get("status") or "").lower() not in {"ready", "ok"}:
        review_reasons.append(f"data status is {data_quality.get('status') or 'unknown'}")
    if invalid_reasons:
        quality_verdict = "invalid"
    elif review_reasons or warning_level not in {"OK", "UNKNOWN"}:
        quality_verdict = REVIEW
    else:
        quality_verdict = READY
    block_range = {
        "from": data_quality.get("first_x"),
        "to": data_quality.get("last_x"),
        "requested_from": data_quality.get("requested_from"),
        "requested_to": data_quality.get("requested_to"),
        "x_axis": data_quality.get("x_axis"),
    }
    dedupe_stats = {
        "canonical_key_fields": raw_summary.get("canonical_key_fields") or [],
        "candidate_event_unique_count": _to_int(raw_summary.get("candidate_event_unique_count") or fill.get("candidate_event_unique_count")),
        "candidate_event_duplicate_count": _to_int(raw_summary.get("candidate_event_duplicate_count") or fill.get("candidate_event_duplicate_count")),
        "consumed_event_unique_count": _to_int(raw_summary.get("consumed_event_unique_count") or fill.get("consumed_event_unique_count")),
        "consumed_event_duplicate_count": _to_int(raw_summary.get("consumed_event_duplicate_count") or fill.get("consumed_event_duplicate_count")),
    }
    return {
        "status": READY,
        "quality_verdict": quality_verdict,
        "warning_level": warning_level,
        "reason": "; ".join(invalid_reasons or review_reasons) or "data quality is sufficient for fill-first research reporting",
        "block_range": block_range,
        "row_count": row_count,
        "gap_count": gap_count,
        "max_gap": max_gap,
        "stale_count": stale_count,
        "source_mix": source_mix,
        "fallback_count": fallback_count,
        "dedupe_stats": dedupe_stats,
        "duplicate_count": duplicate_count,
        "jump_count": jump_count,
        "span_coverage_pct": None if _is_blank(data_quality.get("span_coverage_pct")) else str(data_quality.get("span_coverage_pct")),
        "caveats": caveats,
    }


def build_execution_semantics_report(
    orders: Sequence[Mapping[str, Any]],
    fill_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize whether persisted orders preserve fill-first execution semantics."""

    rows = [dict(row) for row in orders]
    if not rows:
        return {
            "status": MISSING,
            "semantics_verdict": MISSING,
            "reason": "no persisted orders available to audit maker/taker, order type, time-in-force, or no-fill semantics",
            "order_count": 0,
            "status_counts": {},
            "role_counts": {},
            "order_type_counts": {},
            "time_in_force_counts": {},
            "no_fill_reason_counts": {},
            "execution_source_counts": {},
            "latency": {},
            "semantics_present": {},
            "caveats": ["orders are required to prove fill-first execution semantics"],
            "next_actions": ["Create a fill-first run that persists quant_backtest_orders."],
        }

    status_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    order_type_counts: dict[str, int] = {}
    time_in_force_counts: dict[str, int] = {}
    no_fill_reason_counts: dict[str, int] = {}
    execution_source_counts: dict[str, int] = {}
    missing_counts = {
        "role": 0,
        "order_type": 0,
        "time_in_force": 0,
        "execution_source": 0,
        "latency": 0,
    }
    latency_blocks: list[Decimal] = []
    latency_seconds: list[Decimal] = []
    effective_latency_spans: list[Decimal] = []
    strategy_intent_count = 0
    has_orderfilled_evidence = False

    for row in rows:
        meta = _json_dict(row.get("meta"))
        intent = _json_dict(meta.get("strategy_intent")) or _json_dict(meta.get("strategyIntent"))
        if intent:
            strategy_intent_count += 1
        contexts = [row, intent, meta]

        status = _normalized_text(row.get("status"), fallback="unknown").upper()
        role = _normalized_text(_first_context_value(contexts, ("role", "order_role", "orderRole"))).lower()
        order_type = _normalized_text(_first_context_value(contexts, ("order_type", "orderType"))).lower()
        time_in_force = _normalized_text(
            _first_context_value(contexts, ("time_in_force", "timeInForce", "tif")),
        ).upper()
        no_fill_reason = _normalized_text(_first_context_value(contexts, ("no_fill_reason", "noFillReason"))).upper()
        execution_source = _normalized_text(_first_context_value(contexts, ("execution_source", "executionSource"))).lower()
        execution_evidence = _normalized_text(
            _first_context_value(
                contexts,
                ("execution_evidence_type", "executionEvidenceType"),
            )
        ).lower()
        has_orderfilled_evidence = has_orderfilled_evidence or "orderfilled" in execution_evidence

        _bump(status_counts, status)
        if role:
            _bump(role_counts, role)
        else:
            missing_counts["role"] += 1
        if order_type:
            _bump(order_type_counts, order_type)
        else:
            missing_counts["order_type"] += 1
        if time_in_force:
            _bump(time_in_force_counts, time_in_force)
        else:
            missing_counts["time_in_force"] += 1
        if no_fill_reason:
            _bump(no_fill_reason_counts, no_fill_reason)
        if execution_source:
            _bump(execution_source_counts, execution_source)
        else:
            missing_counts["execution_source"] += 1

        block_latency_value = _first_context_value(contexts, ("latency_blocks", "latencyBlocks"))
        second_latency_value = _first_context_value(contexts, ("latency_seconds", "latencySeconds"))
        span_latency_value = _first_context_value(contexts, ("effective_latency_x_span", "effectiveLatencyXSpan"))
        if any(not _is_blank(value) for value in (block_latency_value, second_latency_value, span_latency_value)):
            if not _is_blank(block_latency_value):
                latency_blocks.append(_decimal(block_latency_value))
            if not _is_blank(second_latency_value):
                latency_seconds.append(_decimal(second_latency_value))
            if not _is_blank(span_latency_value):
                effective_latency_spans.append(_decimal(span_latency_value))
        else:
            missing_counts["latency"] += 1

    caveats: list[str] = []
    if missing_counts["role"]:
        caveats.append(f"{missing_counts['role']} orders missing role")
    if missing_counts["order_type"]:
        caveats.append(f"{missing_counts['order_type']} orders missing order_type")
    if missing_counts["time_in_force"]:
        caveats.append(f"{missing_counts['time_in_force']} orders missing time_in_force")
    if missing_counts["execution_source"]:
        caveats.append(f"{missing_counts['execution_source']} orders missing execution_source")
    if missing_counts["latency"]:
        caveats.append(f"{missing_counts['latency']} orders missing latency")

    no_fill_count = status_counts.get("NO_FILL", 0) + status_counts.get("PARTIAL", 0)
    if no_fill_count and not no_fill_reason_counts:
        caveats.append("NO_FILL/PARTIAL orders exist but no no_fill_reason was persisted")
    has_orderfilled_source = has_orderfilled_evidence or any(
        "orderfilled" in key for key in execution_source_counts
    )
    if not has_orderfilled_source:
        caveats.append("orders do not show an OrderFilled execution source")

    fill_counts = _json_dict(fill_quality or {})
    expected_tif_counts = _json_dict(fill_counts.get("time_in_force_counts") or fill_counts.get("timeInForceCounts"))
    expected_order_type_counts = _json_dict(fill_counts.get("order_type_counts") or fill_counts.get("orderTypeCounts"))
    expected_role_counts = _json_dict(fill_counts.get("role_counts") or fill_counts.get("roleCounts"))
    if expected_tif_counts and not time_in_force_counts:
        caveats.append("fill_quality has time-in-force counts but orders do not")
    if expected_order_type_counts and not order_type_counts:
        caveats.append("fill_quality has order-type counts but orders do not")
    if expected_role_counts and not role_counts:
        caveats.append("fill_quality has role counts but orders do not")

    status = READY if not caveats else REVIEW
    verdict = READY if status == READY else REVIEW
    reason = (
        "orders preserve maker/taker, order type, time-in-force, no-fill and latency semantics"
        if status == READY
        else "; ".join(caveats)
    )
    return {
        "status": status,
        "semantics_verdict": verdict,
        "reason": reason,
        "order_count": len(rows),
        "status_counts": status_counts,
        "role_counts": role_counts,
        "order_type_counts": order_type_counts,
        "time_in_force_counts": time_in_force_counts,
        "no_fill_reason_counts": no_fill_reason_counts,
        "execution_source_counts": execution_source_counts,
        "maker_count": role_counts.get("maker", 0),
        "taker_count": role_counts.get("taker", 0),
        "post_only_count": sum(count for key, count in order_type_counts.items() if "post" in key),
        "marketable_count": sum(count for key, count in order_type_counts.items() if "marketable" in key),
        "fok_count": time_in_force_counts.get("FOK", 0),
        "fak_count": time_in_force_counts.get("FAK", 0),
        "gtc_count": time_in_force_counts.get("GTC", 0),
        "gtd_count": time_in_force_counts.get("GTD", 0),
        "strategy_intent_count": strategy_intent_count,
        "missing_counts": missing_counts,
        "latency": {
            "avg_latency_blocks": _average_decimal_string(latency_blocks),
            "avg_latency_seconds": _average_decimal_string(latency_seconds),
            "avg_effective_latency_x_span": _average_decimal_string(effective_latency_spans),
            "max_effective_latency_x_span": _decimal_string(max(effective_latency_spans)) if effective_latency_spans else None,
            "latency_sample_count": len(latency_blocks) + len(latency_seconds) + len(effective_latency_spans),
        },
        "semantics_present": {
            "role": bool(role_counts),
            "order_type": bool(order_type_counts),
            "time_in_force": bool(time_in_force_counts),
            "no_fill_reason": bool(no_fill_reason_counts) or no_fill_count == 0,
            "latency": missing_counts["latency"] < len(rows),
            "orderfilled_execution_source": has_orderfilled_source,
            "strategy_intent": strategy_intent_count > 0,
        },
        "fill_quality_counts": {
            "role_counts": expected_role_counts,
            "order_type_counts": expected_order_type_counts,
            "time_in_force_counts": expected_tif_counts,
        },
        "caveats": caveats,
        "next_actions": [] if status == READY else ["Persist role/order_type/time_in_force/latency/no_fill_reason on each simulated order."],
    }


def build_fill_probability_evidence_report(
    orders: Sequence[Mapping[str, Any]],
    fill_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Audit whether order fills preserve the evidence used to estimate fill probability."""

    rows = [dict(row) for row in orders]
    if not rows:
        return {
            "status": MISSING,
            "evidence_verdict": MISSING,
            "reason": "no persisted orders available to audit fill probability evidence",
            "order_count": 0,
            "ready_order_count": 0,
            "missing_field_counts": {},
            "source_counts": {},
            "fill_probability_buckets": {},
            "participation_buckets": {},
            "totals": {},
            "caveats": ["orders are required to prove block-volume/trade-count fill probability evidence"],
            "next_actions": ["Create a fill-first run that persists quant_backtest_orders."],
        }

    required_order_fields = (
        "fill_probability",
        "block_volume",
        "trade_count",
        "available_notional",
        "participation_rate",
        "expected_fill_size",
        "expected_fill_notional",
        "actual_fill_size",
        "actual_fill_notional",
    )
    required_meta_fields = (
        "effective_liquidity_cap_pct",
        "fill_probability_haircut_pct",
    )
    missing_field_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    fill_probability_buckets: dict[str, int] = {}
    participation_buckets: dict[str, int] = {}
    ready_order_count = 0
    orderfilled_order_count = 0
    requested_notional = Decimal("0")
    expected_notional = Decimal("0")
    actual_notional = Decimal("0")
    available_notional = Decimal("0")
    block_volume = Decimal("0")
    trade_count = 0
    raw_probability_samples: list[Decimal] = []
    effective_probability_samples: list[Decimal] = []
    haircut_samples: list[Decimal] = []
    effective_cap_samples: list[Decimal] = []

    for row in rows:
        meta = _json_dict(row.get("meta"))
        source = _normalized_text(row.get("execution_source"), fallback="unknown")
        _bump(source_counts, source)
        evidence_type = _normalized_text(
            _first_non_blank(
                row.get("execution_evidence_type"),
                meta.get("execution_evidence_type"),
            ),
            fallback="unknown",
        ).lower()
        if "orderfilled" in source.lower() or "orderfilled" in evidence_type:
            orderfilled_order_count += 1
        order_missing: list[str] = []
        for field in required_order_fields:
            if field not in row or _is_blank(row.get(field)):
                order_missing.append(field)
        for field in required_meta_fields:
            if field not in meta or _is_blank(meta.get(field)):
                order_missing.append(f"meta.{field}")
        for field in order_missing:
            _bump(missing_field_counts, field)
        if not order_missing:
            ready_order_count += 1

        fill_probability = _decimal(row.get("fill_probability"))
        participation = _decimal(row.get("participation_rate"))
        _bump(fill_probability_buckets, _pct_bucket(fill_probability))
        _bump(participation_buckets, _pct_bucket(participation))
        requested_notional += _decimal(row.get("requested_notional"))
        expected_notional += _decimal(row.get("expected_fill_notional"))
        actual_notional += _decimal(row.get("actual_fill_notional"))
        available_notional += _decimal(row.get("available_notional"))
        block_volume += _decimal(row.get("block_volume"))
        trade_count += _to_int(row.get("trade_count"))
        if not _is_blank(meta.get("raw_fill_probability")):
            raw_probability_samples.append(_decimal(meta.get("raw_fill_probability")))
        if not _is_blank(meta.get("effective_fill_probability")):
            effective_probability_samples.append(_decimal(meta.get("effective_fill_probability")))
        elif not _is_blank(row.get("fill_probability")):
            effective_probability_samples.append(fill_probability)
        if not _is_blank(meta.get("fill_probability_haircut_pct")):
            haircut_samples.append(_decimal(meta.get("fill_probability_haircut_pct")))
        if not _is_blank(meta.get("effective_liquidity_cap_pct")):
            effective_cap_samples.append(_decimal(meta.get("effective_liquidity_cap_pct")))

    caveats: list[str] = []
    if missing_field_counts:
        missing_text = ", ".join(f"{field}={count}" for field, count in sorted(missing_field_counts.items()))
        caveats.append(f"missing evidence fields: {missing_text}")
    if orderfilled_order_count <= 0:
        caveats.append("no OrderFilled-backed orders in this run")
    fill = _json_dict(fill_quality or {})
    if _to_int(fill.get("candidate_event_count")) > 0 and _to_int(fill.get("consumed_event_count")) <= 0 and actual_notional > 0:
        caveats.append("filled orders exist but consumed raw event count is zero")
    if expected_notional < actual_notional:
        caveats.append("actual fill notional exceeds expected fill notional")

    status = READY if not caveats else REVIEW
    return {
        "status": status,
        "evidence_verdict": READY if status == READY else REVIEW,
        "reason": "fill probability evidence is persisted on every order" if status == READY else "; ".join(caveats),
        "order_count": len(rows),
        "ready_order_count": ready_order_count,
        "orderfilled_order_count": orderfilled_order_count,
        "missing_field_counts": missing_field_counts,
        "source_counts": source_counts,
        "fill_probability_buckets": fill_probability_buckets,
        "participation_buckets": participation_buckets,
        "totals": {
            "requested_notional": _decimal_string(requested_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
            "expected_fill_notional": _decimal_string(expected_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
            "actual_fill_notional": _decimal_string(actual_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
            "available_notional": _decimal_string(available_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
            "block_volume": _decimal_string(block_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
            "trade_count": trade_count,
        },
        "averages": {
            "raw_fill_probability": _average_decimal_string(raw_probability_samples),
            "effective_fill_probability": _average_decimal_string(effective_probability_samples),
            "fill_probability_haircut_pct": _average_decimal_string(haircut_samples),
            "effective_liquidity_cap_pct": _average_decimal_string(effective_cap_samples),
        },
        "fill_quality_crosscheck": {
            "candidate_event_count": _to_int(fill.get("candidate_event_count")),
            "consumed_event_count": _to_int(fill.get("consumed_event_count")),
            "avg_participation_rate": fill.get("avg_participation_rate"),
            "avg_fill_probability_haircut_pct": fill.get("avg_fill_probability_haircut_pct"),
        },
        "caveats": caveats,
        "next_actions": [] if status == READY else ["Persist fill probability evidence fields on each order and rebuild the run artifact audit."],
    }


def build_reproducibility_report(
    run: Mapping[str, Any],
    parameters: Mapping[str, Any] | None,
    data_quality_report: Mapping[str, Any],
    fill_quality: Mapping[str, Any],
    inputs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    meta = _json_dict(run.get("meta"))
    snapshot = _json_dict(meta.get("parameter_snapshot"))
    snapshot_params = _json_dict(snapshot.get("parameters"))
    execution_context = _json_dict(meta.get("execution_context"))
    snapshot_context = _json_dict(snapshot.get("execution_context"))
    model_versions = _json_dict(meta.get("model_versions"))
    data_quality = _json_dict(meta.get("actual_data_quality"))
    params = dict(parameters or {})
    contexts = [
        model_versions,
        execution_context,
        snapshot_context,
        snapshot,
        params,
        meta,
        run,
    ]
    run_id = _to_int(run.get("run_id"))
    parameter_fingerprint = meta.get("parameter_fingerprint")
    data_version = data_quality_report.get("data_version") or data_quality.get("data_version") or data_quality.get("checksum")
    block_range = {
        "from": run.get("from_block") if run.get("from_block") is not None else _json_dict(data_quality_report.get("block_range")).get("from"),
        "to": run.get("to_block") if run.get("to_block") is not None else _json_dict(data_quality_report.get("block_range")).get("to"),
        "requested_from": _json_dict(data_quality_report.get("block_range")).get("requested_from"),
        "requested_to": _json_dict(data_quality_report.get("block_range")).get("requested_to"),
        "x_axis": run.get("price_source") == "orderfilled_block_close" and "block_number" or _json_dict(data_quality_report.get("block_range")).get("x_axis"),
    }
    source_quality = {
        "status": data_quality.get("status"),
        "verdict": data_quality_report.get("quality_verdict"),
        "warning_level": data_quality_report.get("warning_level"),
        "source_mix": data_quality_report.get("source_mix") or {},
    }
    gap_report = {
        "gap_count": data_quality_report.get("gap_count"),
        "max_gap": data_quality_report.get("max_gap"),
        "stale_count": data_quality_report.get("stale_count"),
        "fallback_count": data_quality_report.get("fallback_count"),
        "duplicate_count": data_quality_report.get("duplicate_count"),
    }
    artifact_manifest = _artifact_manifest(inputs or {}, meta, data_quality_report, fill_quality)
    report = {
        "status": READY,
        "run_id": run_id or None,
        "code_commit": meta.get("code_commit"),
        "code_dirty": meta.get("code_dirty"),
        "code_source": meta.get("code_source"),
        "strategy_name": _first_context_value(contexts, ("strategy_name", "strategyName")) or meta.get("strategy") or snapshot.get("strategy"),
        "strategy_version": _first_context_value(contexts, ("strategy_version", "strategyVersion", "strategy")) or snapshot.get("strategy"),
        "artifact_schema_version": meta.get("artifact_schema_version"),
        "parameter_fingerprint": parameter_fingerprint,
        "parameter_snapshot_status": READY if snapshot and snapshot_params else REVIEW if snapshot else MISSING,
        "parameter_snapshot": snapshot,
        "data_version": data_version,
        "block_range": block_range,
        "source_quality": source_quality,
        "gap_report": gap_report,
        "fill_model": _first_context_value(contexts, ("fill_model", "fillModel")),
        "fill_model_version": _first_context_value(contexts, ("fill_model_version", "fillModelVersion")),
        "fee_model_version": _first_context_value(contexts, ("fee_model_version", "feeModelVersion")),
        "slippage_model_version": _first_context_value(contexts, ("slippage_model_version", "slippageModelVersion")),
        "artifact_manifest": artifact_manifest,
        "replay_command": f"conda run -n polyBacktest python scripts/audit_backtest_run_artifacts.py --run-id {run_id} --format markdown" if run_id else None,
    }
    required = {
        "run_id": report["run_id"],
        "code_commit": report["code_commit"],
        "strategy_name": report["strategy_name"],
        "strategy_version": report["strategy_version"],
        "parameter_fingerprint": report["parameter_fingerprint"],
        "parameter_snapshot": report["parameter_snapshot"] if report["parameter_snapshot_status"] != MISSING else None,
        "data_version": report["data_version"],
        "block_range.from": block_range.get("from"),
        "block_range.to": block_range.get("to"),
        "source_quality.verdict": source_quality.get("verdict"),
        "gap_report.gap_count": gap_report.get("gap_count"),
        "fill_model_version": report["fill_model_version"],
        "fee_model_version": report["fee_model_version"],
        "slippage_model_version": report["slippage_model_version"],
        "artifact_schema_version": report["artifact_schema_version"],
    }
    missing = [key for key, value in required.items() if _is_blank(value) or value == {}]
    manifest_missing = sorted(key for key, value in artifact_manifest.items() if value is False)
    if missing:
        verdict = MISSING
        reason = "missing reproducibility fields: " + ", ".join(missing)
    elif manifest_missing:
        verdict = REVIEW
        reason = "artifact manifest incomplete: " + ", ".join(manifest_missing)
    elif report["code_dirty"] is True:
        verdict = REVIEW
        reason = "run was created from a dirty working tree"
    else:
        verdict = READY
        reason = "run has enough provenance to replay the same parameters and data window"
    report["reproducibility_verdict"] = verdict
    report["reason"] = reason
    report["missing_fields"] = missing
    report["manifest_missing"] = manifest_missing
    return report


def build_materialized_cache_report(
    run: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
    fill_quality: Mapping[str, Any],
) -> dict[str, Any]:
    replay = _json_dict(data_quality.get("orderfilled_replay"))
    loaded_window = _json_dict(fill_quality.get("loaded_block_window")) or _json_dict(replay.get("loaded_block_window"))
    source_table = _text_or_none(data_quality.get("source_table"))
    access_path = _text_or_none(data_quality.get("access_path"))
    data_version = _text_or_none(data_quality.get("data_version") or data_quality.get("checksum"))
    x_axis = _text_or_none(data_quality.get("x_axis"))
    raw_source = _text_or_none(replay.get("source") or fill_quality.get("raw_replay_source"))
    input_snapshot = {
        "price_source": _text_or_none(data_quality.get("price_source") or run.get("price_source")),
        "source_table": source_table,
        "access_path": access_path,
        "data_version": data_version,
        "x_axis": x_axis,
        "first_x": data_quality.get("first_x"),
        "last_x": data_quality.get("last_x"),
        "requested_from": data_quality.get("requested_from"),
        "requested_to": data_quality.get("requested_to"),
        "span_coverage_pct": None if _is_blank(data_quality.get("span_coverage_pct")) else str(data_quality.get("span_coverage_pct")),
        "quality_verdict": data_quality_report.get("quality_verdict"),
        "warning_level": data_quality_report.get("warning_level"),
    }
    block_range = {
        "from": run.get("from_block") if run.get("from_block") is not None else data_quality.get("first_x"),
        "to": run.get("to_block") if run.get("to_block") is not None else data_quality.get("last_x"),
        "requested_from": data_quality.get("requested_from"),
        "requested_to": data_quality.get("requested_to"),
        "x_axis": x_axis,
    }
    raw_detail_window = {
        "source": raw_source,
        "fallback": None if _is_blank(replay.get("fallback")) else str(replay.get("fallback")),
        "from_block": _first_non_blank(
            loaded_window.get("from_block"),
            loaded_window.get("from"),
            replay.get("from_block"),
            replay.get("from"),
            run.get("from_block"),
            data_quality.get("requested_from"),
            data_quality.get("first_x"),
        ),
        "to_block": _first_non_blank(
            loaded_window.get("to_block"),
            loaded_window.get("to"),
            replay.get("to_block"),
            replay.get("to"),
            run.get("to_block"),
            data_quality.get("requested_to"),
            data_quality.get("last_x"),
        ),
        "market_id": _first_non_blank(replay.get("market_id"), replay.get("marketId"), data_quality.get("market_id"), data_quality.get("marketId")),
        "token_id": _first_non_blank(replay.get("token_id"), replay.get("tokenId"), data_quality.get("token_id"), data_quality.get("tokenId")),
        "token_id_hex": _first_non_blank(replay.get("token_id_hex"), replay.get("tokenIdHex"), data_quality.get("token_id_hex"), data_quality.get("tokenIdHex")),
        "limit": _first_non_blank(replay.get("limit"), loaded_window.get("limit"), fill_quality.get("raw_event_limit")),
    }
    source_normalized = str(source_table or "").lower()
    access_normalized = str(access_path or "").lower()
    materialized_price_input = source_normalized in MATERIALIZED_PRICE_INPUT_TABLES or any(
        table in source_normalized for table in MATERIALIZED_PRICE_INPUT_TABLES
    )
    keyed_access_path = any(hint in access_normalized for hint in MATERIALIZED_ACCESS_PATH_HINTS)
    price_access_contract = build_data_access_contract_report(
        source_table=source_table,
        access_path=access_path,
        raw_source=raw_source,
        raw_detail_window=raw_detail_window,
    )
    strict_keyed_access_path = price_access_contract["price_access_path_allowed"]
    bounded_raw_detail = bool(price_access_contract["raw_detail_bounded"])
    missing_snapshot_fields = [field for field in REQUIRED_CACHE_SNAPSHOT_FIELDS if _is_blank(input_snapshot.get(field))]
    cache_key = _text_or_none(data_quality.get("cache_key") or data_quality.get("cacheKey"))
    if _is_blank(cache_key):
        cache_key = "|".join(
            str(part)
            for part in (
                input_snapshot.get("price_source") or "-",
                source_table or "-",
                access_path or "-",
                data_version or "-",
                x_axis or "-",
                input_snapshot.get("requested_from") or input_snapshot.get("first_x") or "-",
                input_snapshot.get("requested_to") or input_snapshot.get("last_x") or "-",
            )
        )
    reasons: list[str] = []
    if not materialized_price_input:
        reasons.append("price input is not a known materialized/pre-aggregated table")
    if not strict_keyed_access_path:
        reasons.append("access path is not an allowed keyed token/market range contract")
    if missing_snapshot_fields:
        reasons.append("input snapshot missing fields: " + ", ".join(missing_snapshot_fields))
    if not bounded_raw_detail:
        reasons.append("raw OrderFilled detail source is not bounded by market/token and block window")
    if _is_blank(cache_key):
        reasons.append("missing cache key")
    if not source_table or not data_version:
        verdict = MISSING
    elif reasons:
        verdict = REVIEW
    else:
        verdict = READY
    return {
        "status": READY,
        "cache_verdict": verdict,
        "reason": "; ".join(reasons) if reasons else "run used a materialized price input, bounded OrderFilled detail window, and reproducible input snapshot/cache key",
        "source_table": source_table,
        "access_path": access_path,
        "data_version": data_version,
        "cache_key": cache_key,
        "materialized_price_input": materialized_price_input,
        "keyed_access_path": keyed_access_path,
        "strict_keyed_access_path": strict_keyed_access_path,
        "bounded_raw_detail": bounded_raw_detail,
        "data_access_contract": price_access_contract,
        "raw_detail_window": raw_detail_window,
        "input_snapshot": input_snapshot,
        "block_range": block_range,
        "missing_snapshot_fields": missing_snapshot_fields,
    }


def build_raw_orderfilled_replay_contract_report(
    data_quality: Mapping[str, Any],
    fill_quality: Mapping[str, Any],
) -> dict[str, Any]:
    """Report whether the raw OrderFilled detail stream is replayable.

    This sits below execution-model checks: it verifies the historical facts
    entering the backtest are loaded from a bounded orderfilled_fact window,
    deduped by canonical fill keys, and convertible into raw trade ticks.
    """

    v3_report = _json_dict(data_quality.get("fill_only_v3"))
    if v3_report:
        return _build_v3_trade_slice_contract_report(v3_report)

    replay = _json_dict(data_quality.get("orderfilled_replay"))
    raw_summary = _json_dict(fill_quality.get("raw_evidence_summary"))
    loaded_window = _json_dict(fill_quality.get("loaded_block_window")) or _json_dict(replay.get("loaded_block_window"))
    tick_report = (
        _json_dict(fill_quality.get("raw_trade_tick_report"))
        or _json_dict(raw_summary.get("raw_trade_tick_report"))
        or _json_dict(replay.get("raw_trade_tick_report"))
    )
    source = _text_or_none(replay.get("source") or fill_quality.get("raw_replay_source"))
    fallback = _text_or_none(replay.get("fallback") or fill_quality.get("raw_fallback"))
    raw_event_count = _to_int(fill_quality.get("raw_event_count") or replay.get("event_count"))
    loaded_event_count = _to_int(
        _first_non_blank(
            fill_quality.get("loaded_raw_event_count"),
            replay.get("loaded_event_count"),
            loaded_window.get("loaded_event_count"),
            raw_event_count,
        )
    )
    deduped_event_count = _to_int(
        _first_non_blank(
            fill_quality.get("deduped_raw_event_count"),
            replay.get("deduped_event_count"),
            loaded_window.get("replay_event_count"),
            raw_event_count,
        )
    )
    duplicate_event_count = _to_int(
        _first_non_blank(fill_quality.get("raw_duplicate_event_count"), replay.get("duplicate_event_count"), loaded_window.get("duplicate_event_count"))
    )
    duplicate_classified = any(
        "raw_exact_duplicate_event_count" in source
        or "raw_conflicting_duplicate_event_count" in source
        or "exact_duplicate_event_count" in source
        or "conflicting_duplicate_event_count" in source
        for source in (fill_quality, raw_summary, replay, loaded_window)
    )
    exact_duplicate_event_count = _to_int(
        _first_non_blank(
            fill_quality.get("raw_exact_duplicate_event_count"),
            raw_summary.get("raw_exact_duplicate_event_count"),
            replay.get("exact_duplicate_event_count"),
            loaded_window.get("exact_duplicate_event_count"),
            0,
        )
    )
    conflicting_duplicate_event_count = (
        _to_int(
            _first_non_blank(
                fill_quality.get("raw_conflicting_duplicate_event_count"),
                raw_summary.get("raw_conflicting_duplicate_event_count"),
                replay.get("conflicting_duplicate_event_count"),
                loaded_window.get("conflicting_duplicate_event_count"),
                0,
            )
        )
        if duplicate_classified
        else duplicate_event_count
    )
    duplicate_group_count = _to_int(
        _first_non_blank(
            fill_quality.get("raw_duplicate_group_count"),
            raw_summary.get("raw_duplicate_group_count"),
            replay.get("duplicate_group_count"),
            loaded_window.get("duplicate_group_count"),
            0,
        )
    )
    conflicting_duplicate_group_count = _to_int(
        _first_non_blank(
            fill_quality.get("raw_conflicting_duplicate_group_count"),
            raw_summary.get("raw_conflicting_duplicate_group_count"),
            replay.get("conflicting_duplicate_group_count"),
            loaded_window.get("conflicting_duplicate_group_count"),
            0,
        )
    )
    canonical_event_count = _to_int(
        _first_non_blank(fill_quality.get("raw_canonical_event_count"), replay.get("canonical_event_count"), tick_report.get("canonical_fill_key_count"))
    )
    fallback_key_event_count = _to_int(
        _first_non_blank(fill_quality.get("raw_fallback_key_event_count"), replay.get("fallback_key_event_count"), tick_report.get("fallback_fill_key_count"))
    )
    unknown_key_event_count = _to_int(
        _first_non_blank(fill_quality.get("raw_unknown_key_event_count"), replay.get("unknown_key_event_count"), tick_report.get("unknown_fill_key_count"))
    )
    raw_trade_tick_count = _to_int(_first_non_blank(fill_quality.get("raw_trade_tick_count"), replay.get("raw_trade_tick_count"), tick_report.get("trade_tick_count")))
    raw_block_count = _to_int(_first_non_blank(fill_quality.get("raw_block_count"), replay.get("raw_block_count"), tick_report.get("block_count")))
    canonical_pct = _coverage_text(
        _first_non_blank(
            fill_quality.get("raw_canonical_fill_key_coverage_pct"),
            raw_summary.get("raw_canonical_fill_key_coverage_pct"),
            replay.get("raw_canonical_fill_key_coverage_pct"),
            tick_report.get("canonical_fill_key_coverage_pct"),
            "",
        )
    )
    attribution_pct = _coverage_text(
        _first_non_blank(
            fill_quality.get("raw_maker_taker_side_coverage_pct"),
            raw_summary.get("raw_maker_taker_side_coverage_pct"),
            replay.get("raw_maker_taker_side_coverage_pct"),
            tick_report.get("maker_taker_side_coverage_pct"),
            "",
        )
    )
    block_context_pct = _coverage_text(
        _first_non_blank(
            fill_quality.get("raw_block_context_coverage_pct"),
            raw_summary.get("raw_block_context_coverage_pct"),
            replay.get("raw_block_context_coverage_pct"),
            tick_report.get("block_context_coverage_pct"),
            "",
        )
    )
    block_rows = tick_report.get("blocks") if isinstance(tick_report.get("blocks"), Sequence) and not isinstance(tick_report.get("blocks"), (str, bytes, bytearray)) else []
    block_vwap_available_count = 0
    block_high_low_available_count = 0
    block_order_sequence_available_count = 0
    multi_trade_block_count = 0
    multi_trade_sequence_block_count = 0
    for row in block_rows:
        if not isinstance(row, Mapping):
            continue
        trade_tick_count = _to_int(row.get("trade_tick_count"))
        if trade_tick_count > 1:
            multi_trade_block_count += 1
        if not _is_blank(row.get("vwap")):
            block_vwap_available_count += 1
        if not _is_blank(row.get("high")) and not _is_blank(row.get("low")):
            block_high_low_available_count += 1
        order_sequence = row.get("order_sequence")
        if isinstance(order_sequence, Sequence) and not isinstance(order_sequence, (str, bytes, bytearray)) and order_sequence:
            block_order_sequence_available_count += 1
            if trade_tick_count > 1:
                multi_trade_sequence_block_count += 1
    block_vwap_available_pct = _coverage_text(_pct_text(block_vwap_available_count, raw_block_count))
    block_high_low_available_pct = _coverage_text(_pct_text(block_high_low_available_count, raw_block_count))
    block_order_sequence_coverage_pct = _coverage_text(_pct_text(block_order_sequence_available_count, raw_block_count))
    multi_trade_sequence_coverage_pct = _coverage_text(_pct_text(multi_trade_sequence_block_count, multi_trade_block_count))
    block_window = {
        "from_block": _first_non_blank(loaded_window.get("from_block"), loaded_window.get("from"), replay.get("from_block"), replay.get("from")),
        "to_block": _first_non_blank(loaded_window.get("to_block"), loaded_window.get("to"), replay.get("to_block"), replay.get("to")),
        "loaded_first_block": _first_non_blank(loaded_window.get("loaded_first_block"), replay.get("loaded_first_block")),
        "loaded_last_block": _first_non_blank(loaded_window.get("loaded_last_block"), replay.get("loaded_last_block")),
        "replay_first_block": _first_non_blank(loaded_window.get("replay_first_block"), replay.get("replay_first_block")),
        "replay_last_block": _first_non_blank(loaded_window.get("replay_last_block"), replay.get("replay_last_block")),
        "market_id": _first_non_blank(loaded_window.get("market_id"), replay.get("market_id"), replay.get("marketId"), data_quality.get("market_id")),
        "token_id": _first_non_blank(loaded_window.get("token_id"), replay.get("token_id"), replay.get("tokenId"), data_quality.get("token_id")),
        "token_id_hex": _first_non_blank(loaded_window.get("token_id_hex"), replay.get("token_id_hex"), replay.get("tokenIdHex"), data_quality.get("token_id_hex")),
        "limit": _first_non_blank(loaded_window.get("limit"), replay.get("limit"), fill_quality.get("raw_event_limit")),
        "hit_limit": bool(_first_non_blank(loaded_window.get("hit_limit"), replay.get("hit_limit"), False)),
        "loaded_event_count": loaded_event_count,
        "deduped_event_count": deduped_event_count,
        "duplicate_event_count": duplicate_event_count,
        "exact_duplicate_event_count": exact_duplicate_event_count,
        "conflicting_duplicate_event_count": conflicting_duplicate_event_count,
        "duplicate_group_count": duplicate_group_count,
        "conflicting_duplicate_group_count": conflicting_duplicate_group_count,
    }
    missing: list[str] = []
    source_normalized = str(source or "").lower()
    raw_source_expected = source_normalized in RAW_REPLAY_DETAIL_SOURCES or "orderfilled_fact" in source_normalized
    if raw_source_expected and _is_blank(block_window["from_block"]):
        missing.append("raw_block_window_from")
    if raw_source_expected and _is_blank(block_window["to_block"]):
        missing.append("raw_block_window_to")
    if raw_source_expected and _is_blank(block_window["market_id"]):
        missing.append("raw_market_id")
    if raw_source_expected and _is_blank(block_window["token_id"]) and _is_blank(block_window["token_id_hex"]):
        missing.append("raw_token_id")
    if raw_source_expected and (_is_blank(block_window["limit"]) or _decimal(block_window["limit"]) <= 0):
        missing.append("raw_explicit_limit")
    if raw_event_count <= 0:
        missing.append("raw_event_count")
    if loaded_event_count < deduped_event_count:
        missing.append("loaded_gte_deduped_count")
    if deduped_event_count != raw_event_count:
        missing.append("deduped_matches_raw_event_count")
    if raw_trade_tick_count != raw_event_count:
        missing.append("raw_trade_tick_count")
    if raw_block_count <= 0 and raw_event_count > 0:
        missing.append("raw_block_count")
    if _is_blank(canonical_pct):
        missing.append("canonical_fill_key_coverage_pct")
    if _is_blank(attribution_pct):
        missing.append("maker_taker_side_coverage_pct")
    if _is_blank(block_context_pct):
        missing.append("block_context_coverage_pct")

    review_reasons: list[str] = []
    if fallback:
        review_reasons.append(f"raw replay used fallback={fallback}")
    if conflicting_duplicate_event_count:
        review_reasons.append(
            f"raw replay found {conflicting_duplicate_event_count} conflicting duplicate events"
        )
    if block_window["hit_limit"]:
        review_reasons.append("raw replay hit the explicit load limit")
    if canonical_pct and _decimal(canonical_pct) < Decimal("100"):
        review_reasons.append(f"canonical fill-key coverage is {canonical_pct}%")
    if attribution_pct and _decimal(attribution_pct) < Decimal("100"):
        review_reasons.append(f"maker/taker/side attribution coverage is {attribution_pct}%")
    if block_context_pct and _decimal(block_context_pct) < Decimal("100"):
        review_reasons.append(f"raw trade tick block context coverage is {block_context_pct}%")
    if fallback_key_event_count or unknown_key_event_count:
        review_reasons.append(
            f"non-canonical fill keys fallback={fallback_key_event_count} unknown={unknown_key_event_count}"
        )

    if missing:
        verdict = MISSING
        reason = "missing raw replay contract fields: " + ", ".join(missing)
    elif review_reasons:
        verdict = REVIEW
        reason = "; ".join(review_reasons)
    else:
        verdict = READY
        reason = "raw OrderFilled detail stream is bounded, deduped, attributed, and convertible to trade ticks"
    return {
        "status": READY,
        "contract_verdict": verdict,
        "reason": reason,
        "source": source,
        "fallback": fallback,
        "raw_event_count": raw_event_count,
        "loaded_event_count": loaded_event_count,
        "deduped_event_count": deduped_event_count,
        "duplicate_event_count": duplicate_event_count,
        "exact_duplicate_event_count": exact_duplicate_event_count,
        "conflicting_duplicate_event_count": conflicting_duplicate_event_count,
        "duplicate_group_count": duplicate_group_count,
        "conflicting_duplicate_group_count": conflicting_duplicate_group_count,
        "duplicate_classification": "classified" if duplicate_classified else "legacy_unclassified",
        "canonical_event_count": canonical_event_count,
        "fallback_key_event_count": fallback_key_event_count,
        "unknown_key_event_count": unknown_key_event_count,
        "canonical_fill_key_coverage_pct": canonical_pct,
        "maker_taker_side_coverage_pct": attribution_pct,
        "block_context_coverage_pct": block_context_pct,
        "raw_trade_tick_count": raw_trade_tick_count,
        "raw_block_count": raw_block_count,
        "multi_trade_block_count": multi_trade_block_count,
        "multi_trade_sequence_block_count": multi_trade_sequence_block_count,
        "block_vwap_available_count": block_vwap_available_count,
        "block_vwap_available_pct": block_vwap_available_pct,
        "block_high_low_available_count": block_high_low_available_count,
        "block_high_low_available_pct": block_high_low_available_pct,
        "block_order_sequence_available_count": block_order_sequence_available_count,
        "block_order_sequence_coverage_pct": block_order_sequence_coverage_pct,
        "multi_trade_sequence_coverage_pct": multi_trade_sequence_coverage_pct,
        "loaded_block_window": block_window,
        "raw_trade_tick_report_status": tick_report.get("status"),
        "missing_contract_fields": missing,
        "review_reasons": review_reasons,
    }


def _build_v3_trade_slice_contract_report(v3_report: Mapping[str, Any]) -> dict[str, Any]:
    loads = [
        dict(row)
        for row in v3_report.get("trade_slice_loads") or []
        if isinstance(row, Mapping)
    ]
    windows = [
        dict(row)
        for row in v3_report.get("required_trade_windows") or []
        if isinstance(row, Mapping)
    ]
    coverage = _json_dict(v3_report.get("source_coverage"))
    intervals = [
        (_to_int(row.get("from_block")), _to_int(row.get("to_block")))
        for row in coverage.get("intervals") or []
        if isinstance(row, Mapping)
    ]
    rows_loaded = sum(_to_int(row.get("rows_loaded")) for row in loads)
    missing: list[str] = []
    if str(coverage.get("source_table") or "") != "trade_prints_one_sided":
        missing.append("trade_prints_one_sided_source")
    if _to_int(coverage.get("receipt_count")) <= 0:
        missing.append("completed_build_receipts")
    if not windows:
        missing.append("required_trade_windows")
    if not loads:
        missing.append("trade_slice_loads")
    if any(
        not any(
            left <= _to_int(window.get("start_block"))
            and _to_int(window.get("end_block")) <= right
            for left, right in intervals
        )
        for window in windows
    ):
        missing.append("coverage_for_every_required_window")

    starts = [_to_int(row.get("start_block")) for row in windows]
    ends = [_to_int(row.get("end_block")) for row in windows]
    loaded_window = {
        "from_block": min(starts) if starts else None,
        "to_block": max(ends) if ends else None,
        "loaded_event_count": rows_loaded,
        "deduped_event_count": rows_loaded,
        "duplicate_event_count": 0,
        "exact_duplicate_event_count": 0,
        "conflicting_duplicate_event_count": 0,
        "duplicate_group_count": 0,
        "conflicting_duplicate_group_count": 0,
    }
    verdict = MISSING if missing else READY
    reason = (
        "missing V3 one-sided trade-slice contract fields: " + ", ".join(missing)
        if missing
        else "Fill-only V3 loaded bounded trade_prints_one_sided slices inside completed build-receipt coverage"
    )
    return {
        "status": READY,
        "contract_verdict": verdict,
        "contract_type": "ONE_SIDED_TRADE_SLICE_V1",
        "reason": reason,
        "source": coverage.get("source_table"),
        "fallback": None,
        "raw_event_count": rows_loaded,
        "loaded_event_count": rows_loaded,
        "deduped_event_count": rows_loaded,
        "duplicate_event_count": 0,
        "exact_duplicate_event_count": 0,
        "conflicting_duplicate_event_count": 0,
        "duplicate_group_count": 0,
        "conflicting_duplicate_group_count": 0,
        "duplicate_classification": "canonical_derived_trade_groups",
        "canonical_event_count": rows_loaded,
        "fallback_key_event_count": 0,
        "unknown_key_event_count": 0,
        "canonical_fill_key_coverage_pct": "100",
        "maker_taker_side_coverage_pct": "100",
        "block_context_coverage_pct": "100",
        "raw_trade_tick_count": rows_loaded,
        "raw_block_count": len(windows),
        "multi_trade_block_count": 0,
        "multi_trade_sequence_block_count": 0,
        "block_vwap_available_count": 0,
        "block_vwap_available_pct": "0",
        "block_high_low_available_count": 0,
        "block_high_low_available_pct": "0",
        "block_order_sequence_available_count": 0,
        "block_order_sequence_coverage_pct": "0",
        "multi_trade_sequence_coverage_pct": "0",
        "loaded_block_window": loaded_window,
        "raw_trade_tick_report_status": "not_applicable_derived_trade_slice",
        "missing_contract_fields": missing,
        "review_reasons": [],
        "source_coverage": coverage,
        "required_trade_windows": windows,
        "trade_slice_loads": loads,
    }


def build_historical_l2_alignment_report(
    meta: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    fill_quality: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize PMXT/L2-vs-OrderFilled alignment evidence for a run.

    The report is historical-only: it proves whether PMXT/L2 book state was
    aligned with raw OrderFilled fill evidence.  It does not depend on live
    order-state or external execution sources.
    """

    source, raw = _historical_l2_alignment_source(meta, data_quality, fill_quality)
    if not raw:
        return {
            "status": REVIEW,
            "alignment_verdict": REVIEW,
            "reason": "no historical L2/PMXT alignment artifact attached to this run",
            "source": None,
            "pmxt_rows_seen": 0,
            "pmxt_matched_events": 0,
            "pmxt_applied_events": 0,
            "orderfilled_rows_seen": 0,
            "orderfilled_rows_matched": 0,
            "aligned_count": 0,
            "alignment_pct": "0",
            "max_lag_ms": None,
            "missing_timestamp_count": 0,
            "missing_l2_before_fill_count": 0,
            "stale_l2_count": 0,
            "price_outside_spread_count": 0,
            "price_unchecked_count": 0,
            "depth_checked_count": 0,
            "depth_sufficient_count": 0,
            "depth_insufficient_count": 0,
            "crossable_depth_sufficient_count": 0,
            "depth_sufficient_pct": "0",
            "crossable_depth_sufficient_pct": "0",
            "sample_rows": [],
        }

    orderfilled_rows_matched = _to_int(
        _first_non_blank(
            raw.get("orderfilled_rows_matched"),
            raw.get("matched_orderfilled_rows"),
            raw.get("orderfilled_matched_count"),
            raw.get("orderfilled_rows_seen"),
            raw.get("orderfilled_fill_count"),
        )
    )
    aligned_count = _to_int(_first_non_blank(raw.get("aligned_count"), raw.get("fresh_l2_fill_count"), raw.get("matched_count")))
    missing_timestamp_count = _to_int(raw.get("missing_timestamp_count"))
    missing_l2_before_fill_count = _to_int(raw.get("missing_l2_before_fill_count"))
    stale_l2_count = _to_int(raw.get("stale_l2_count"))
    price_outside_spread_count = _to_int(raw.get("price_outside_spread_count"))
    price_unchecked_count = _to_int(raw.get("price_unchecked_count"))
    depth_checked_count = _to_int(raw.get("depth_checked_count"))
    depth_sufficient_count = _to_int(raw.get("depth_sufficient_count"))
    depth_insufficient_count = _to_int(raw.get("depth_insufficient_count"))
    crossable_depth_sufficient_count = _to_int(raw.get("crossable_depth_sufficient_count"))
    if _is_blank(raw.get("alignment_pct")):
        alignment_pct = _coverage_text(_pct_text(aligned_count, orderfilled_rows_matched))
    else:
        alignment_pct = _coverage_text(raw.get("alignment_pct"))
    if _is_blank(raw.get("depth_sufficient_pct")):
        depth_sufficient_pct = _coverage_text(_pct_text(depth_sufficient_count, depth_checked_count))
    else:
        depth_sufficient_pct = _coverage_text(raw.get("depth_sufficient_pct"))
    if _is_blank(raw.get("crossable_depth_sufficient_pct")):
        crossable_depth_sufficient_pct = _coverage_text(_pct_text(crossable_depth_sufficient_count, depth_checked_count))
    else:
        crossable_depth_sufficient_pct = _coverage_text(raw.get("crossable_depth_sufficient_pct"))

    missing_reasons: list[str] = []
    review_reasons: list[str] = []
    if orderfilled_rows_matched <= 0:
        missing_reasons.append("no OrderFilled rows matched to L2 alignment artifact")
    if missing_timestamp_count:
        review_reasons.append(f"{missing_timestamp_count} OrderFilled rows missing timestamps")
    if missing_l2_before_fill_count:
        review_reasons.append(f"{missing_l2_before_fill_count} fills had no prior L2 book")
    if stale_l2_count:
        review_reasons.append(f"{stale_l2_count} fills matched stale L2 book state")
    if price_outside_spread_count:
        review_reasons.append(f"{price_outside_spread_count} fills priced outside matched L2 spread")
    if depth_checked_count > 0 and depth_insufficient_count:
        review_reasons.append(f"{depth_insufficient_count} aligned fills lacked sufficient same-side L2 depth")
    if depth_checked_count > 0 and crossable_depth_sufficient_count < depth_checked_count:
        review_reasons.append(
            f"{depth_checked_count - crossable_depth_sufficient_count} aligned fills lacked sufficient crossable L2 depth"
        )

    source_status = str(raw.get("status") or "").lower()
    if source_status in {MISSING, "fail", "failed"}:
        verdict = MISSING
    elif missing_reasons:
        verdict = MISSING
    elif source_status == READY and aligned_count >= orderfilled_rows_matched and not review_reasons:
        verdict = READY
    elif aligned_count > 0 and aligned_count >= orderfilled_rows_matched and not review_reasons:
        verdict = READY
    else:
        verdict = REVIEW

    reason = (
        "historical L2/PMXT book state is aligned with OrderFilled fill evidence"
        if verdict == READY
        else "; ".join(missing_reasons + review_reasons) or str(raw.get("reason") or "historical L2/PMXT alignment needs review")
    )
    sample_rows = raw.get("sample_rows") or raw.get("samples") or []
    if not isinstance(sample_rows, Sequence) or isinstance(sample_rows, (str, bytes, bytearray)):
        sample_rows = []
    return {
        "status": READY if verdict == READY else verdict,
        "alignment_verdict": verdict,
        "reason": reason,
        "source": source,
        "pmxt_rows_seen": _to_int(raw.get("pmxt_rows_seen")),
        "pmxt_matched_events": _to_int(raw.get("pmxt_matched_events")),
        "pmxt_applied_events": _to_int(raw.get("pmxt_applied_events")),
        "orderfilled_rows_seen": _to_int(_first_non_blank(raw.get("orderfilled_rows_seen"), raw.get("orderfilled_fill_count"))),
        "orderfilled_rows_matched": orderfilled_rows_matched,
        "aligned_count": aligned_count,
        "alignment_pct": alignment_pct,
        "max_lag_ms": _first_non_blank(raw.get("max_lag_ms"), raw.get("maxLagMs")),
        "missing_timestamp_count": missing_timestamp_count,
        "missing_l2_before_fill_count": missing_l2_before_fill_count,
        "stale_l2_count": stale_l2_count,
        "price_outside_spread_count": price_outside_spread_count,
        "price_unchecked_count": price_unchecked_count,
        "depth_checked_count": depth_checked_count,
        "depth_sufficient_count": depth_sufficient_count,
        "depth_insufficient_count": depth_insufficient_count,
        "crossable_depth_sufficient_count": crossable_depth_sufficient_count,
        "depth_sufficient_pct": depth_sufficient_pct,
        "crossable_depth_sufficient_pct": crossable_depth_sufficient_pct,
        "review_reasons": review_reasons,
        "missing_reasons": missing_reasons,
        "sample_rows": list(sample_rows)[:5],
    }


def _historical_l2_alignment_source(
    meta: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    fill_quality: Mapping[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    candidates: list[tuple[str, Any]] = [
        ("meta.historical_l2_alignment", meta.get("historical_l2_alignment")),
        ("meta.pmxt_l2_alignment", meta.get("pmxt_l2_alignment")),
        ("meta.orderfilled_l2_alignment", meta.get("orderfilled_l2_alignment")),
        ("data_quality.historical_l2_alignment", data_quality.get("historical_l2_alignment")),
        ("data_quality.pmxt_l2_alignment", data_quality.get("pmxt_l2_alignment")),
        ("data_quality.orderfilled_l2_alignment", data_quality.get("orderfilled_l2_alignment")),
        ("fill_quality.historical_l2_alignment", fill_quality.get("historical_l2_alignment")),
        ("fill_quality.pmxt_l2_alignment", fill_quality.get("pmxt_l2_alignment")),
        ("fill_quality.orderfilled_l2_alignment", fill_quality.get("orderfilled_l2_alignment")),
    ]
    replay = _json_dict(data_quality.get("orderfilled_replay"))
    candidates.extend(
        [
            ("orderfilled_replay.historical_l2_alignment", replay.get("historical_l2_alignment")),
            ("orderfilled_replay.pmxt_l2_alignment", replay.get("pmxt_l2_alignment")),
            ("orderfilled_replay.orderfilled_l2_alignment", replay.get("orderfilled_l2_alignment")),
        ]
    )
    for name, value in candidates:
        row = _json_dict(value)
        if row:
            return name, row
    return None, {}


def _raw_orderfilled_events_for_artifact(
    data_quality: Mapping[str, Any],
    fill_quality: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    replay = _json_dict(data_quality.get("orderfilled_replay"))
    raw_summary = _json_dict(fill_quality.get("raw_evidence_summary"))
    for container in (replay, data_quality, fill_quality, raw_summary):
        for key in ("raw_orderfilled_events", "rawOrderfilledEvents", "trade_ticks", "tradeTicks", "raw_events", "rawEvents"):
            value = container.get(key)
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
                continue
            for row in value:
                if isinstance(row, Mapping):
                    rows.append(row)
    return rows


def build_data_access_contract_report(
    *,
    source_table: Any,
    access_path: Any,
    raw_source: Any,
    raw_detail_window: Mapping[str, Any],
) -> dict[str, Any]:
    source_normalized = str(source_table or "").lower()
    raw_source_normalized = str(raw_source or "").lower()
    access_normalized = _normalize_access_contract_path(access_path)
    price_source_table_allowed = source_normalized in MATERIALIZED_PRICE_INPUT_TABLES or any(
        table in source_normalized for table in MATERIALIZED_PRICE_INPUT_TABLES
    )
    price_access_path_allowed = access_normalized in PRICE_ACCESS_CONTRACT_PATHS
    raw_requires_window = raw_source_normalized in RAW_REPLAY_DETAIL_SOURCES or "orderfilled_fact" in raw_source_normalized
    raw_detail_windowed = not _is_blank(raw_detail_window.get("from_block")) and not _is_blank(raw_detail_window.get("to_block"))
    raw_detail_identified = (
        not _is_blank(raw_detail_window.get("market_id"))
        and (not _is_blank(raw_detail_window.get("token_id")) or not _is_blank(raw_detail_window.get("token_id_hex")))
    )
    raw_detail_has_limit = not _is_blank(raw_detail_window.get("limit")) and _decimal(raw_detail_window.get("limit")) > 0
    raw_detail_bounded = not raw_requires_window or (raw_detail_windowed and raw_detail_identified and raw_detail_has_limit)
    missing: list[str] = []
    if not price_source_table_allowed:
        missing.append("allowed_price_source_table")
    if not price_access_path_allowed:
        missing.append("allowed_price_access_path")
    if raw_requires_window and not raw_detail_windowed:
        missing.append("raw_block_window")
    if raw_requires_window and not raw_detail_identified:
        missing.append("raw_market_token_identity")
    if raw_requires_window and not raw_detail_has_limit:
        missing.append("raw_explicit_limit")
    status = READY if not missing else REVIEW
    return {
        "status": status,
        "contract_version": "backtest_data_access_contract_v1",
        "price_source_table_allowed": price_source_table_allowed,
        "price_access_path_allowed": price_access_path_allowed,
        "normalized_access_path": access_normalized,
        "raw_requires_window": raw_requires_window,
        "raw_detail_windowed": raw_detail_windowed,
        "raw_detail_identified": raw_detail_identified,
        "raw_detail_has_limit": raw_detail_has_limit,
        "raw_detail_bounded": raw_detail_bounded,
        "missing": missing,
    }


def _normalize_access_contract_path(value: Any) -> str:
    text = str(value or "").strip().lower()
    for char in ("+", "-", " ", "/"):
        text = text.replace(char, "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text.strip("_")


def build_environment_incident_report(
    platform_incidents: Sequence[Mapping[str, Any]],
    fill_quality: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Combine platform incidents and runtime environment flags for a run."""

    fill = _json_dict(fill_quality or {})
    incident_report = build_platform_incident_report(platform_incidents)
    severity_counts = _int_dict(incident_report.get("severity_counts"))
    component_counts = _int_dict(incident_report.get("component_counts"))
    platform_flags = _int_dict(incident_report.get("environment_flags"))
    runtime_flags = _int_dict(fill.get("environment_flags"))
    order_anomaly_flags = _int_dict(fill.get("order_anomaly_flags"))
    merged_flags = dict(platform_flags)
    for key, value in runtime_flags.items():
        merged_flags[key] = merged_flags.get(key, 0) + value
    incident_count = _to_int(incident_report.get("incident_count"))
    severe_incident_count = sum(severity_counts.get(key, 0) for key in ("warning", "error", "critical"))
    environment_flag_count = sum(merged_flags.values())
    order_anomaly_count = _to_int(fill.get("order_anomaly_count")) or sum(order_anomaly_flags.values())
    review_reasons: list[str] = []
    if severe_incident_count:
        review_reasons.append(f"{severe_incident_count} warning/error/critical platform incidents overlap this run")
    if runtime_flags:
        review_reasons.append(f"{sum(runtime_flags.values())} runtime environment flags were recorded in fill quality")
    if order_anomaly_count:
        review_reasons.append(f"{order_anomaly_count} order anomaly flags were recorded in fill quality")
    verdict = REVIEW if review_reasons else READY
    return {
        "schema_version": "fill_first_environment_incident_v1",
        "status": READY,
        "incident_verdict": verdict,
        "reason": "; ".join(review_reasons) if review_reasons else "no warning/error platform incidents or runtime environment flags overlap this run",
        "source": incident_report.get("source"),
        "incident_count": incident_count,
        "severe_incident_count": severe_incident_count,
        "severity_counts": severity_counts,
        "component_counts": component_counts,
        "environment_flags": dict(sorted(merged_flags.items())),
        "runtime_environment_flags": dict(sorted(runtime_flags.items())),
        "platform_environment_flags": dict(sorted(platform_flags.items())),
        "environment_flag_count": environment_flag_count,
        "order_anomaly_flags": dict(sorted(order_anomaly_flags.items())),
        "order_anomaly_count": order_anomaly_count,
        "incidents": list(incident_report.get("incidents") or []),
        "review_reasons": review_reasons,
        "next_actions": _environment_incident_next_actions(verdict, review_reasons),
    }


def _environment_incident_next_actions(verdict: str, review_reasons: Sequence[str]) -> list[str]:
    if verdict == READY:
        return ["Use this run as strategy evidence without known platform incident adjustment."]
    actions = [f"Review environment incident: {reason}" for reason in review_reasons[:5]]
    actions.append("Separate platform/Gamma/CLOB/API incidents from strategy parameter or fill-model changes before tuning.")
    return actions


EXTERNAL_SIGNAL_REQUIRED_FIELDS = ("observed_at", "source", "latency_seconds", "payload_hash")
EXTERNAL_SIGNAL_RESOLUTION_FIELDS = ("resolution_source", "settlement_rule", "price_to_beat_source", "oracle_source")


def build_external_signal_contract_report(
    run: Mapping[str, Any],
    parameters: Mapping[str, Any] | None,
    external_signal_events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate external-world signal events before they are trusted by a strategy."""

    contexts = [run, _json_dict(run.get("meta")), _json_dict(parameters)]
    requires_external_signals = _context_bool(
        contexts,
        (
            "requires_external_signal",
            "requiresExternalSignal",
            "uses_external_data",
            "usesExternalData",
            "external_signal_required",
            "externalSignalRequired",
        ),
    )
    rows = [_normalize_external_signal_event(row) for row in external_signal_events]
    if not rows and not requires_external_signals:
        return {
            "schema_version": "fill_first_external_signal_contract_v1",
            "status": READY,
            "signal_verdict": READY,
            "reason": "this run does not use external signal events",
            "external_signal_required": False,
            "event_count": 0,
            "required_fields": list(EXTERNAL_SIGNAL_REQUIRED_FIELDS),
            "missing_required_field_count": 0,
            "missing_field_counts": {},
            "source_counts": {},
            "avg_latency_seconds": None,
            "timestamp_alignment_status": "not_applicable",
            "resolution_compatibility_status": "not_applicable",
            "resolution_mismatch_count": 0,
            "resolution_missing_count": 0,
            "misaligned_event_count": 0,
            "unverifiable_alignment_count": 0,
            "review_reasons": [],
            "events": [],
            "next_actions": [],
        }
    if not rows and requires_external_signals:
        return {
            "schema_version": "fill_first_external_signal_contract_v1",
            "status": MISSING,
            "signal_verdict": MISSING,
            "reason": "strategy declares external signal usage but no external signal events are attached to the run",
            "external_signal_required": True,
            "event_count": 0,
            "required_fields": list(EXTERNAL_SIGNAL_REQUIRED_FIELDS),
            "missing_required_field_count": len(EXTERNAL_SIGNAL_REQUIRED_FIELDS),
            "missing_field_counts": {field: 1 for field in EXTERNAL_SIGNAL_REQUIRED_FIELDS},
            "source_counts": {},
            "avg_latency_seconds": None,
            "timestamp_alignment_status": MISSING,
            "resolution_compatibility_status": MISSING,
            "resolution_mismatch_count": 0,
            "resolution_missing_count": 0,
            "misaligned_event_count": 0,
            "unverifiable_alignment_count": 0,
            "review_reasons": ["external signal events are required but missing"],
            "events": [],
            "next_actions": ["Attach canonical external signal events with observed_at/source/latency/payload_hash before trusting this strategy run."],
        }

    missing_field_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    latencies: list[Decimal] = []
    compact_events: list[dict[str, Any]] = []
    misaligned_count = 0
    unverifiable_alignment_count = 0
    resolution_mismatch_count = 0
    resolution_missing_count = 0
    run_resolution = _external_signal_run_resolution(run, parameters)
    run_from_block = _to_int(run.get("from_block"))
    run_to_block = _to_int(run.get("to_block"))
    run_from_ts = _parse_datetime_utc(_first_non_blank(run.get("from_ts"), run.get("started_at")))
    run_to_ts = _parse_datetime_utc(_first_non_blank(run.get("to_ts"), run.get("finished_at")))

    for row in rows:
        for field in EXTERNAL_SIGNAL_REQUIRED_FIELDS:
            if _is_blank(row.get(field)):
                missing_field_counts[field] = missing_field_counts.get(field, 0) + 1
        source = str(row.get("source") or "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1
        if not _is_blank(row.get("latency_seconds")):
            latencies.append(_decimal(row.get("latency_seconds")))
        alignment_status = _external_signal_alignment_status(row, run_from_block, run_to_block, run_from_ts, run_to_ts)
        if alignment_status == "misaligned":
            misaligned_count += 1
        elif alignment_status == "unverifiable":
            unverifiable_alignment_count += 1
        resolution_status = _external_signal_resolution_status(row, run_resolution)
        if resolution_status == "mismatch":
            resolution_mismatch_count += 1
        elif resolution_status == "missing":
            resolution_missing_count += 1
        compact_events.append(
            {
                "event_id": row.get("event_id"),
                "event_type": row.get("event_type"),
                "observed_at": row.get("observed_at"),
                "observed_block": row.get("observed_block"),
                "source": source,
                "latency_seconds": row.get("latency_seconds"),
                "payload_hash": row.get("payload_hash"),
                "computed_payload_hash": row.get("computed_payload_hash"),
                "timestamp_alignment_status": alignment_status,
                "resolution_compatibility_status": resolution_status,
            }
        )

    review_reasons: list[str] = []
    missing_required_total = sum(missing_field_counts.values())
    if missing_required_total:
        review_reasons.append(f"{missing_required_total} required external signal fields are missing")
    if misaligned_count:
        review_reasons.append(f"{misaligned_count} external signal events fall outside the run block/time window")
    if unverifiable_alignment_count:
        review_reasons.append(f"{unverifiable_alignment_count} external signal events cannot be time-aligned to this run")
    if resolution_mismatch_count:
        review_reasons.append(f"{resolution_mismatch_count} external signal events disagree with run resolution/source fields")
    if resolution_missing_count:
        review_reasons.append(f"{resolution_missing_count} external signal events lack resolution/source compatibility fields")
    timestamp_alignment_status = REVIEW if misaligned_count or unverifiable_alignment_count else READY
    resolution_compatibility_status = REVIEW if resolution_mismatch_count or resolution_missing_count else READY
    verdict = REVIEW if review_reasons else READY
    return {
        "schema_version": "fill_first_external_signal_contract_v1",
        "status": READY,
        "signal_verdict": verdict,
        "reason": "external signal events satisfy canonical schema and run compatibility checks" if verdict == READY else "; ".join(review_reasons),
        "external_signal_required": bool(requires_external_signals),
        "event_count": len(rows),
        "required_fields": list(EXTERNAL_SIGNAL_REQUIRED_FIELDS),
        "missing_required_field_count": missing_required_total,
        "missing_field_counts": dict(sorted(missing_field_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "avg_latency_seconds": _average_decimal_string(latencies),
        "timestamp_alignment_status": timestamp_alignment_status,
        "resolution_compatibility_status": resolution_compatibility_status,
        "resolution_mismatch_count": resolution_mismatch_count,
        "resolution_missing_count": resolution_missing_count,
        "misaligned_event_count": misaligned_count,
        "unverifiable_alignment_count": unverifiable_alignment_count,
        "run_resolution": run_resolution,
        "review_reasons": review_reasons,
        "events": compact_events[:50],
        "next_actions": _external_signal_next_actions(verdict, review_reasons),
    }


def _external_signal_next_actions(verdict: str, review_reasons: Sequence[str]) -> list[str]:
    if verdict == READY:
        return ["External signal events can be used as canonical strategy inputs for this run."]
    actions = [f"Review external signal contract: {reason}" for reason in review_reasons[:5]]
    actions.append("Normalize external events to observed_at/source/latency_seconds/payload_hash and resolution fields before strategy replay.")
    return actions


def _external_signal_events_from_inputs(inputs: Mapping[str, Any], meta: Mapping[str, Any]) -> list[dict[str, Any]]:
    for value in (
        inputs.get("external_signal_events"),
        inputs.get("external_signals"),
        meta.get("external_signal_events"),
        meta.get("externalSignals"),
        meta.get("external_signals"),
    ):
        rows = _list_of_dicts(value)
        if rows:
            return rows
    return []


def _normalize_external_signal_event(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = row.get("payload")
    if isinstance(payload, str) and payload.strip():
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = {"raw": payload}
    payload_obj = payload if isinstance(payload, Mapping) else dict(row)
    computed_hash = hashlib.sha256(json.dumps(payload_obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return {
        "event_id": _first_non_blank(row.get("event_id"), row.get("eventId"), row.get("id")),
        "event_type": _first_non_blank(row.get("event_type"), row.get("eventType"), row.get("type")),
        "observed_at": _first_non_blank(row.get("observed_at"), row.get("observedAt"), row.get("timestamp"), row.get("time")),
        "observed_block": _first_non_blank(row.get("observed_block"), row.get("observedBlock"), row.get("block_number"), row.get("blockNumber")),
        "source": _first_non_blank(row.get("source"), row.get("provider"), row.get("origin")),
        "latency_seconds": _first_non_blank(row.get("latency_seconds"), row.get("latencySeconds"), row.get("latency")),
        "payload_hash": _first_non_blank(row.get("payload_hash"), row.get("payloadHash")),
        "computed_payload_hash": computed_hash,
        "resolution_source": _first_non_blank(row.get("resolution_source"), row.get("resolutionSource")),
        "settlement_rule": _first_non_blank(row.get("settlement_rule"), row.get("settlementRule"), row.get("resolution_rule"), row.get("resolutionRule")),
        "price_to_beat_source": _first_non_blank(row.get("price_to_beat_source"), row.get("priceToBeatSource")),
        "oracle_source": _first_non_blank(row.get("oracle_source"), row.get("oracleSource"), row.get("oracle")),
    }


def _external_signal_run_resolution(run: Mapping[str, Any], parameters: Mapping[str, Any] | None) -> dict[str, str]:
    contexts = _settlement_contexts(run, parameters)
    return {
        "resolution_source": _normalized_text(_first_context_value(contexts, ("resolution_source", "resolutionSource", "resolutionSourceName"))),
        "settlement_rule": _normalized_text(_first_context_value(contexts, ("settlement_rule", "settlementRule", "resolution_rule", "resolutionRule"))),
        "price_to_beat_source": _normalized_text(_first_context_value(contexts, ("price_to_beat_source", "priceToBeatSource", "price_to_beat", "priceToBeat"))),
        "oracle_source": _normalized_text(_first_context_value(contexts, ("oracle_source", "oracleSource", "oracle", "resolution_oracle", "resolutionOracle"))),
    }


def _external_signal_alignment_status(
    row: Mapping[str, Any],
    run_from_block: int,
    run_to_block: int,
    run_from_ts: datetime | None,
    run_to_ts: datetime | None,
) -> str:
    observed_block = _to_int(row.get("observed_block"))
    if observed_block and run_from_block and run_to_block:
        return "ready" if run_from_block <= observed_block <= run_to_block else "misaligned"
    observed_at = _parse_datetime_utc(row.get("observed_at"))
    if observed_at and run_from_ts and run_to_ts:
        return "ready" if run_from_ts <= observed_at <= run_to_ts else "misaligned"
    if observed_block or observed_at:
        return "unverifiable"
    return "unverifiable"


def _external_signal_resolution_status(row: Mapping[str, Any], run_resolution: Mapping[str, str]) -> str:
    missing = 0
    for field in EXTERNAL_SIGNAL_RESOLUTION_FIELDS:
        event_value = _normalized_text(row.get(field)).lower()
        run_value = _normalized_text(run_resolution.get(field)).lower()
        if not event_value:
            missing += 1
            continue
        if run_value and event_value != run_value:
            return "mismatch"
    return "missing" if missing else "ready"


def _parse_datetime_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif _is_blank(value):
        return None
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _artifact_manifest(
    inputs: Mapping[str, Any],
    meta: Mapping[str, Any],
    data_quality_report: Mapping[str, Any],
    fill_quality: Mapping[str, Any],
) -> dict[str, bool]:
    return {
        "run": bool(inputs.get("run")),
        "parameter_snapshot": bool(_json_dict(meta.get("parameter_snapshot"))),
        "parameters_table": bool(inputs.get("parameters")),
        "data_quality_report": bool(data_quality_report),
        "fill_quality": bool(fill_quality),
        "environment_incident_report": True,
        "external_signal_contract_report": True,
        "execution_semantics_report": True,
        "fill_probability_evidence_report": True,
        "metrics": bool(inputs.get("metrics")),
        "orders": bool(inputs.get("orders")) or _to_int(fill_quality.get("submitted_count")) == 0,
        "trades": bool(inputs.get("trades")),
        "ledger": bool(inputs.get("ledger")),
        "run_credibility": True,
        "execution_regime_report": True,
        "external_source_run_coverage_report": True,
        "external_source_missing_evidence_plan": True,
        "tail_risk_report": True,
        "settlement_compatibility_report": True,
        "event_level_risk_report": True,
        "event_stream_report": True,
        "materialized_cache_report": True,
    }


def build_event_level_risk_report(
    run: Mapping[str, Any],
    parameters: Mapping[str, Any] | None,
    orders: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
    inputs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    meta = _json_dict(run.get("meta"))
    data_quality = _json_dict(meta.get("actual_data_quality"))
    event_outcomes = _event_outcome_rows(inputs or {}, meta, data_quality)
    contexts = [
        dict(inputs or {}),
        _json_dict(meta.get("event_context")),
        _json_dict(meta.get("eventContext")),
        _json_dict(meta.get("market_metadata")),
        _json_dict(meta.get("marketMetadata")),
        _parameter_snapshot_fields(meta),
        data_quality,
        meta,
        run,
    ]
    event_slug = _first_context_value(contexts, ("event_slug", "eventSlug", "event_id", "eventId", "group_id", "groupId"))
    expected_outcome_count = _to_int(_first_context_value(contexts, ("event_outcome_count", "eventOutcomeCount", "outcome_count", "outcomeCount")))
    if event_outcomes:
        expected_outcome_count = max(expected_outcome_count, len(event_outcomes))
    outcome_exposures = _event_outcome_exposures(run, orders, trades, ledger, event_outcomes)
    observed_outcome_count = len(outcome_exposures)
    event_snapshot = _event_snapshot_report(
        event_outcomes,
        _json_dict(meta.get("event_context")) or _json_dict(meta.get("eventContext")),
        expected_outcome_count,
    )
    probability_sum = _event_probability_sum(event_outcomes, inputs or {}, meta)
    complement = _event_yes_no_complement(event_outcomes)
    correlation = _event_outcome_correlation(inputs or {}, meta, observed_outcome_count)
    portfolio_risk = _event_portfolio_risk(outcome_exposures, parameters)
    reasons: list[str] = []
    if not outcome_exposures:
        reasons.append("no outcome exposure or event outcome rows")
    if expected_outcome_count > 1 and observed_outcome_count < min(expected_outcome_count, 2):
        reasons.append("run does not include enough outcomes for event-level replay")
    if not event_outcomes:
        reasons.append("missing event outcome snapshot for probability/complement checks")
    if event_snapshot["status"] != READY:
        reasons.append(str(event_snapshot["reason"]))
    if probability_sum["status"] != READY:
        reasons.append(str(probability_sum["reason"]))
    if complement["status"] != READY:
        reasons.append(str(complement["reason"]))
    if correlation["status"] != READY:
        reasons.append(str(correlation["reason"]))
    if portfolio_risk["status"] != READY:
        reasons.append(str(portfolio_risk["reason"]))
    if not outcome_exposures and not event_outcomes:
        verdict = MISSING
    elif reasons:
        verdict = REVIEW
    else:
        verdict = READY
    return {
        "status": READY,
        "risk_verdict": verdict,
        "reason": "; ".join(dict.fromkeys(reasons)) if reasons else "event-level exposure, probability sum, YES/NO complement, correlation, and cash-at-risk checks are available",
        "event_slug": None if _is_blank(event_slug) else str(event_slug),
        "expected_outcome_count": expected_outcome_count,
        "observed_outcome_count": observed_outcome_count,
        "event_snapshot": event_snapshot,
        "probability_sum": probability_sum,
        "yes_no_complement": complement,
        "outcome_correlation": correlation,
        "portfolio_risk": portfolio_risk,
        "outcome_exposures": outcome_exposures,
    }


def _event_outcome_rows(*sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    keys = ("event_outcomes", "eventOutcomes", "outcomes", "outcome_rows", "outcomeRows")
    for source in sources:
        for key in keys:
            rows = _list_of_dicts(source.get(key))
            if rows:
                return rows
    return []


def _event_outcome_exposures(
    run: Mapping[str, Any],
    orders: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
    event_outcomes: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    exposures: dict[str, dict[str, Any]] = {}
    for index, outcome in enumerate(event_outcomes):
        key = _outcome_key(outcome, run, index)
        row = exposures.setdefault(key, _empty_outcome_exposure(key, outcome, run))
        row["yes_probability"] = _optional_decimal_string(_event_probability(outcome, ("yes_probability", "yesProbability", "yes_price", "yesPrice", "latest_yes_price", "latestYesPrice", "probability")))
        row["no_probability"] = _optional_decimal_string(_event_probability(outcome, ("no_probability", "noProbability", "no_price", "noPrice", "latest_no_price", "latestNoPrice")))
    for order in orders:
        key = _outcome_key(order, run)
        row = exposures.setdefault(key, _empty_outcome_exposure(key, order, run))
        row["order_count"] += 1
        row["requested_notional_decimal"] += _decimal(order.get("requested_notional"))
        row["filled_notional_decimal"] += _decimal(order.get("filled_notional"))
        row["unfilled_notional_decimal"] += max(Decimal("0"), _decimal(order.get("requested_notional")) - _decimal(order.get("filled_notional")))
    for trade in trades:
        key = _outcome_key(trade, run)
        row = exposures.setdefault(key, _empty_outcome_exposure(key, trade, run))
        row["trade_count"] += 1
        row["pnl_decimal"] += _decimal(trade.get("pnl"))
        row["trade_notional_decimal"] += _trade_notional(trade)
    for item in ledger:
        key = _outcome_key(item, run)
        row = exposures.setdefault(key, _empty_outcome_exposure(key, item, run))
        event_type = str(item.get("event_type") or "").upper()
        cash_delta = _decimal(item.get("cash_delta"))
        row["ledger_event_count"] += 1
        row["shares_delta_decimal"] += _decimal(item.get("shares_delta"))
        if event_type == "BUY" and cash_delta < 0:
            row["cash_at_risk_decimal"] += abs(cash_delta)
        if event_type in {"SELL", "SETTLEMENT", "REFUND"} and cash_delta > 0:
            row["cash_released_decimal"] += cash_delta
    result: list[dict[str, Any]] = []
    for row in exposures.values():
        cash_at_risk = max(row["filled_notional_decimal"], row["cash_at_risk_decimal"])
        result.append(
            {
                "outcome_key": row["outcome_key"],
                "market_slug": row["market_slug"],
                "outcome_label": row["outcome_label"],
                "token_side": row["token_side"],
                "yes_probability": row["yes_probability"],
                "no_probability": row["no_probability"],
                "order_count": row["order_count"],
                "trade_count": row["trade_count"],
                "ledger_event_count": row["ledger_event_count"],
                "requested_notional": _decimal_string(row["requested_notional_decimal"]),
                "filled_notional": _decimal_string(row["filled_notional_decimal"]),
                "unfilled_notional": _decimal_string(row["unfilled_notional_decimal"]),
                "cash_at_risk": _decimal_string(cash_at_risk),
                "cash_released": _decimal_string(row["cash_released_decimal"]),
                "net_shares_delta": _decimal_string(row["shares_delta_decimal"]),
                "pnl": _decimal_string(row["pnl_decimal"]),
                "trade_notional": _decimal_string(row["trade_notional_decimal"]),
            }
        )
    return sorted(result, key=lambda item: (-_decimal(item["cash_at_risk"]), str(item["outcome_key"])))


def _empty_outcome_exposure(key: str, source: Mapping[str, Any], run: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "outcome_key": key,
        "market_slug": str(_first_mapping_value(source, ("market_slug", "marketSlug")) or run.get("market_slug") or key),
        "outcome_label": str(_first_mapping_value(source, ("outcome_label", "outcomeLabel", "outcome", "title", "market_title", "marketTitle")) or key),
        "token_side": str(_first_mapping_value(source, ("token_side", "tokenSide")) or run.get("token_side") or "YES"),
        "yes_probability": None,
        "no_probability": None,
        "order_count": 0,
        "trade_count": 0,
        "ledger_event_count": 0,
        "requested_notional_decimal": Decimal("0"),
        "filled_notional_decimal": Decimal("0"),
        "unfilled_notional_decimal": Decimal("0"),
        "cash_at_risk_decimal": Decimal("0"),
        "cash_released_decimal": Decimal("0"),
        "shares_delta_decimal": Decimal("0"),
        "pnl_decimal": Decimal("0"),
        "trade_notional_decimal": Decimal("0"),
    }


def _outcome_key(row: Mapping[str, Any], run: Mapping[str, Any], index: int = 0) -> str:
    meta = _json_dict(row.get("meta"))
    context = _json_dict(meta.get("context"))
    candidates = [
        _first_mapping_value(row, ("market_slug", "marketSlug", "outcome_key", "outcomeKey", "condition_id", "conditionId", "token_id", "tokenId")),
        _first_mapping_value(context, ("market_slug", "marketSlug", "outcome_key", "outcomeKey", "outcome_label", "outcomeLabel")),
        run.get("market_slug"),
    ]
    for value in candidates:
        if not _is_blank(value):
            return str(value)
    return f"outcome-{index + 1}"


def _event_probability_sum(event_outcomes: Sequence[Mapping[str, Any]], inputs: Mapping[str, Any], meta: Mapping[str, Any]) -> dict[str, Any]:
    explicit = _first_context_value([inputs, meta], ("probability_sum", "probabilitySum", "event_probability_sum", "eventProbabilitySum"))
    if not _is_blank(explicit):
        value = _decimal(explicit)
        status = READY if EVENT_PROBABILITY_SUM_MIN <= value <= EVENT_PROBABILITY_SUM_MAX else REVIEW
        return {
            "status": status,
            "value": _decimal_string(value),
            "outcome_count": 0,
            "reason": "explicit event probability sum is available" if status == READY else "explicit event probability sum outside configured review band",
        }
    probabilities = [
        probability
        for probability in (_event_probability(row, ("yes_probability", "yesProbability", "yes_price", "yesPrice", "latest_yes_price", "latestYesPrice", "probability")) for row in event_outcomes)
        if probability is not None
    ]
    if not probabilities:
        return {"status": REVIEW, "value": None, "outcome_count": 0, "reason": "no event-level YES probabilities available"}
    value = sum(probabilities, Decimal("0"))
    status = READY if len(probabilities) >= 2 and EVENT_PROBABILITY_SUM_MIN <= value <= EVENT_PROBABILITY_SUM_MAX else REVIEW
    return {
        "status": status,
        "value": _decimal_string(value),
        "outcome_count": len(probabilities),
        "reason": "probability sum built from event outcome snapshot" if status == READY else "probability sum unavailable or outside configured review band",
    }


def _event_snapshot_report(
    event_outcomes: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any],
    expected_outcome_count: int,
) -> dict[str, Any]:
    observed_count = len(event_outcomes)
    expected_count = max(int(expected_outcome_count or 0), observed_count)
    computed_yes_count = sum(
        _event_probability(row, ("yes_probability", "yesProbability", "yes_price", "yesPrice", "latest_yes_price", "latestYesPrice", "probability")) is not None
        for row in event_outcomes
    )
    computed_no_count = sum(
        _event_probability(row, ("no_probability", "noProbability", "no_price", "noPrice", "latest_no_price", "latestNoPrice")) is not None
        for row in event_outcomes
    )
    computed_pair_count = sum(
        _event_probability(row, ("yes_probability", "yesProbability", "yes_price", "yesPrice", "latest_yes_price", "latestYesPrice", "probability")) is not None
        and _event_probability(row, ("no_probability", "noProbability", "no_price", "noPrice", "latest_no_price", "latestNoPrice")) is not None
        for row in event_outcomes
    )
    priced_yes_count = _to_int(context.get("priced_yes_outcome_count")) or computed_yes_count
    priced_no_count = _to_int(context.get("priced_no_outcome_count")) or computed_no_count
    priced_pair_count = _to_int(context.get("priced_pair_count")) or computed_pair_count
    coverage_pct = (
        Decimal(priced_yes_count * 100) / Decimal(expected_count)
        if expected_count > 0
        else Decimal("0")
    )
    complete = (
        expected_count > 0
        and observed_count >= expected_count
        and priced_yes_count >= expected_count
        and priced_no_count >= expected_count
        and priced_pair_count >= expected_count
    )
    if complete:
        reason = "event outcome snapshot has complete YES/NO pricing coverage"
    elif observed_count <= 0:
        reason = "event outcome snapshot is missing"
    else:
        reason = (
            "event snapshot pricing coverage incomplete: "
            f"{priced_yes_count}/{expected_count} YES, "
            f"{priced_no_count}/{expected_count} NO, "
            f"{priced_pair_count}/{expected_count} pairs"
        )
    return {
        "status": READY if complete else REVIEW,
        "reason": reason,
        "schema_version": context.get("schema_version"),
        "source": context.get("source"),
        "snapshot_to_block": context.get("snapshot_to_block"),
        "expected_outcome_count": expected_count,
        "observed_outcome_count": observed_count,
        "priced_yes_outcome_count": priced_yes_count,
        "priced_no_outcome_count": priced_no_count,
        "priced_pair_count": priced_pair_count,
        "price_coverage_pct": _decimal_string(coverage_pct),
    }


def _event_yes_no_complement(event_outcomes: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    checked = 0
    max_deviation = Decimal("0")
    bad_rows: list[dict[str, str]] = []
    for row in event_outcomes:
        yes = _event_probability(row, ("yes_probability", "yesProbability", "yes_price", "yesPrice", "latest_yes_price", "latestYesPrice", "probability"))
        no = _event_probability(row, ("no_probability", "noProbability", "no_price", "noPrice", "latest_no_price", "latestNoPrice"))
        if yes is None or no is None:
            continue
        checked += 1
        total = yes + no
        deviation = abs(total - Decimal("1"))
        max_deviation = max(max_deviation, deviation)
        if deviation > EVENT_COMPLEMENT_TOLERANCE:
            bad_rows.append(
                {
                    "outcome_key": _outcome_key(row, {}),
                    "yes_plus_no": _decimal_string(total),
                    "deviation": _decimal_string(deviation),
                }
            )
    if checked <= 0:
        return {"status": REVIEW, "checked_count": 0, "bad_count": 0, "max_deviation": None, "reason": "no YES/NO complement pairs available"}
    status = READY if not bad_rows else REVIEW
    return {
        "status": status,
        "checked_count": checked,
        "bad_count": len(bad_rows),
        "max_deviation": _decimal_string(max_deviation),
        "bad_rows": bad_rows[:20],
        "reason": "YES/NO complement pairs are within tolerance" if status == READY else "YES/NO complement deviation exceeds tolerance",
    }


def _event_outcome_correlation(inputs: Mapping[str, Any], meta: Mapping[str, Any], observed_outcome_count: int) -> dict[str, Any]:
    raw = _json_dict(inputs.get("outcome_correlation")) or _json_dict(inputs.get("outcomeCorrelation")) or _json_dict(meta.get("outcome_correlation")) or _json_dict(meta.get("outcomeCorrelation"))
    if raw:
        return {
            "status": str(raw.get("status") or READY),
            "reason": str(raw.get("reason") or "outcome correlation summary attached"),
            "method": raw.get("method") or raw.get("source") or "provided",
            "source": raw.get("source"),
            "sample_count": _to_int(raw.get("sample_count") or raw.get("sampleCount")),
            "grid_sample_count": _to_int(raw.get("grid_sample_count") or raw.get("gridSampleCount")),
            "outcome_count": _to_int(raw.get("outcome_count") or raw.get("outcomeCount")),
            "pair_count": _to_int(raw.get("pair_count") or raw.get("pairCount")),
            "from_block": _to_int(raw.get("from_block") or raw.get("fromBlock")) or None,
            "to_block": _to_int(raw.get("to_block") or raw.get("toBlock")) or None,
            "max_abs_correlation": None if _is_blank(raw.get("max_abs_correlation") or raw.get("maxAbsCorrelation")) else str(raw.get("max_abs_correlation") or raw.get("maxAbsCorrelation")),
            "matrix": raw.get("matrix") or {},
        }
    return {
        "status": REVIEW,
        "reason": "no time-aligned outcome correlation summary attached" if observed_outcome_count >= 2 else "not enough outcomes for correlation",
        "method": "not_available",
        "sample_count": 0,
        "max_abs_correlation": None,
        "matrix": {},
    }


def _event_portfolio_risk(outcome_exposures: Sequence[Mapping[str, Any]], parameters: Mapping[str, Any] | None) -> dict[str, Any]:
    initial_capital = _decimal((parameters or {}).get("initial_capital")) or _decimal((parameters or {}).get("initialCapital"))
    cash_at_risk_values = [_decimal(row.get("cash_at_risk")) for row in outcome_exposures]
    filled_values = [_decimal(row.get("filled_notional")) for row in outcome_exposures]
    portfolio_cash_at_risk = sum(cash_at_risk_values, Decimal("0"))
    total_filled = sum(filled_values, Decimal("0"))
    max_outcome = max(cash_at_risk_values, default=Decimal("0"))
    pct_capital = _pct_of_capital(portfolio_cash_at_risk, initial_capital)
    status = READY if outcome_exposures else REVIEW
    reason = "portfolio cash-at-risk built from event outcome exposures" if status == READY else "no outcome exposures available for portfolio cash-at-risk"
    if initial_capital > 0 and pct_capital > Decimal("100"):
        status = REVIEW
        reason = "portfolio cash-at-risk exceeds initial capital"
    return {
        "status": status,
        "reason": reason,
        "portfolio_cash_at_risk": _decimal_string(portfolio_cash_at_risk),
        "portfolio_cash_at_risk_pct_of_capital": _decimal_string(pct_capital),
        "total_filled_notional": _decimal_string(total_filled),
        "max_outcome_cash_at_risk": _decimal_string(max_outcome),
        "outcome_count": len(outcome_exposures),
    }


def _event_probability(row: Mapping[str, Any], keys: Sequence[str]) -> Decimal | None:
    value = _first_mapping_value(row, keys)
    if _is_blank(value):
        return None
    parsed = _decimal(value)
    if parsed < 0:
        return None
    if parsed > 1 and parsed <= 100:
        parsed = parsed / Decimal("100")
    return parsed if Decimal("0") <= parsed <= Decimal("1") else None


def _optional_decimal_string(value: Decimal | None) -> str | None:
    return None if value is None else _decimal_string(value)


def _first_mapping_value(row: Mapping[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = row.get(key)
        if not _is_blank(value):
            return value
    return None


def build_execution_ledger_parity_report(
    orders: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
    inputs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    order_schema = _schema_report(orders, PARITY_ORDER_FIELDS)
    no_fill_without_reason = sum(
        1
        for order in orders
        if str(order.get("status") or "").upper() in {"NO_FILL", "REJECTED", "EXPIRED", "CANCELED", "CANCELLED"}
        and _is_blank(order.get("no_fill_reason"))
    )
    live_event_count = _to_int((inputs or {}).get("real_order_state_event_count"))
    calibration_count = _to_int((inputs or {}).get("calibration_count"))
    cost_calibration_count = _to_int((inputs or {}).get("cost_calibration_count"))
    external_source_state_count = _to_int((inputs or {}).get("external_source_state_count"))
    live_evidence_status = READY if live_event_count or calibration_count else REVIEW
    live_reason = (
        "live/shadow events or calibration samples are attached"
        if live_evidence_status == READY
        else "no live/shadow terminal order events attached yet"
    )
    ledger_schema = _schema_report(ledger, PARITY_LEDGER_FIELDS)
    ledger_event_types = sorted({str(row.get("event_type") or "unknown") for row in ledger})
    reasons: list[str] = []
    if not orders:
        reasons.append("no order lifecycle rows")
    if order_schema["missing_field_count"]:
        reasons.append("order lifecycle rows missing required parity fields")
    if no_fill_without_reason:
        reasons.append(f"terminal no-fill/rejected orders missing reason={no_fill_without_reason}")
    if not ledger:
        reasons.append("no cashflow ledger rows")
    if ledger_schema["missing_field_count"]:
        reasons.append("ledger rows missing required cashflow fields")
    if ledger and not any(event in ledger_event_types for event in ("BUY", "SELL", "SETTLEMENT")):
        reasons.append("ledger lacks BUY/SELL/SETTLEMENT event type")
    if not orders or not ledger or order_schema["missing_field_count"] or ledger_schema["missing_field_count"]:
        verdict = MISSING
    elif no_fill_without_reason:
        verdict = REVIEW
    else:
        verdict = READY
    return {
        "status": READY,
        "parity_verdict": verdict,
        "reason": "; ".join(reasons) if reasons else "backtest execution and ledger rows share the core lifecycle/cashflow schema needed for shadow/live parity",
        "order_schema": {
            **order_schema,
            "required_fields": list(PARITY_ORDER_FIELDS),
            "terminal_no_fill_without_reason": no_fill_without_reason,
        },
        "ledger_schema": {
            **ledger_schema,
            "required_fields": list(PARITY_LEDGER_FIELDS),
            "event_types": ledger_event_types,
        },
        "live_event_contract": {
            "required_fields": list(PARITY_LIVE_EVENT_FIELDS),
            "terminal_statuses": ["FILLED", "PARTIAL_FILLED", "NO_FILL", "CANCELED", "REJECTED", "EXPIRED", "FAILED"],
        },
        "live_evidence": {
            "status": live_evidence_status,
            "reason": live_reason,
            "real_order_state_event_count": live_event_count,
            "calibration_count": calibration_count,
            "cost_calibration_count": cost_calibration_count,
            "external_source_state_count": external_source_state_count,
        },
    }


def _schema_report(rows: Sequence[Mapping[str, Any]], required_fields: Sequence[str]) -> dict[str, Any]:
    missing_by_field = {field: 0 for field in required_fields}
    rows_with_missing = 0
    for row in rows:
        row_missing = False
        for field in required_fields:
            if _is_blank(row.get(field)):
                missing_by_field[field] += 1
                row_missing = True
        if row_missing:
            rows_with_missing += 1
    missing_by_field = {field: count for field, count in missing_by_field.items() if count}
    return {
        "row_count": len(rows),
        "rows_with_missing": rows_with_missing,
        "missing_by_field": missing_by_field,
        "missing_field_count": sum(missing_by_field.values()),
    }


def build_execution_regime_report(orders: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    order_rows = [dict(row) for row in orders]
    if not order_rows:
        return {
            "status": REVIEW,
            "reason": "no orders available for regime split",
            "order_count": 0,
            "dimensions": {},
        }
    dimensions = {dimension: _regime_dimension_rows(order_rows, dimension) for dimension in REGIME_DIMENSIONS}
    known_bucket_count = sum(
        1
        for rows in dimensions.values()
        for row in rows
        if row["bucket"] != "unknown" and _to_int(row["submitted_count"]) > 0
    )
    return {
        "status": READY if known_bucket_count else REVIEW,
        "reason": "regime buckets built from order lifecycle rows" if known_bucket_count else "only unknown regime buckets found",
        "order_count": len(order_rows),
        "dimensions": dimensions,
    }


def build_maker_taker_execution_report(orders: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize maker and taker execution separately for fill-first audits."""

    order_rows = [dict(row) for row in orders]
    if not order_rows:
        return {
            "schema_version": "fill_first_maker_taker_execution_v1",
            "status": MISSING,
            "maker_taker_verdict": MISSING,
            "reason": "no orders available for maker/taker execution split",
            "order_count": 0,
            "required_roles": ["maker", "taker"],
            "present_roles": [],
            "missing_roles": ["maker", "taker"],
            "execution_scope": "missing",
            "role_specific": True,
            "role_counts": {},
            "role_summaries": {},
            "next_actions": ["Generate order lifecycle rows with explicit maker/taker role before trusting execution assumptions."],
        }

    buckets: dict[str, list[dict[str, Any]]] = {}
    for order in order_rows:
        role = _normalized_role(order)
        buckets.setdefault(role, []).append(order)

    required_roles = ["maker", "taker"]
    present_roles = sorted(role for role in buckets if role != "unknown")
    missing_roles = [role for role in required_roles if role not in buckets]
    unknown_count = len(buckets.get("unknown", []))
    role_summaries = {role: _maker_taker_role_summary(role, rows) for role, rows in sorted(buckets.items())}
    review_reasons: list[str] = []
    if unknown_count:
        review_reasons.append(f"{unknown_count} orders are missing explicit maker/taker role")
    if missing_roles:
        review_reasons.append("missing role coverage: " + ", ".join(missing_roles))
    verdict = REVIEW if review_reasons else READY
    execution_scope = "maker_taker_covered" if verdict == READY else "role_specific"
    return {
        "schema_version": "fill_first_maker_taker_execution_v1",
        "status": READY,
        "maker_taker_verdict": verdict,
        "reason": "maker and taker execution are separately covered" if verdict == READY else "; ".join(review_reasons),
        "order_count": len(order_rows),
        "required_roles": required_roles,
        "present_roles": present_roles,
        "missing_roles": missing_roles,
        "execution_scope": execution_scope,
        "role_specific": bool(verdict != READY),
        "role_counts": {role: len(rows) for role, rows in sorted(buckets.items())},
        "role_summaries": role_summaries,
        "next_actions": _maker_taker_next_actions(verdict, review_reasons),
    }


def _maker_taker_role_summary(role: str, orders: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    submitted = len(orders)
    filled = 0
    partial = 0
    no_fill = 0
    rejected = 0
    requested_notional = Decimal("0")
    filled_notional = Decimal("0")
    fill_probability: list[Decimal] = []
    participation: list[Decimal] = []
    slippage_values: list[Decimal] = []
    markout_1: list[Decimal] = []
    fee_cost = Decimal("0")
    rebate = Decimal("0")
    execution_cost = Decimal("0")
    order_types: dict[str, int] = {}
    time_in_force: dict[str, int] = {}
    no_fill_reasons: dict[str, int] = {}
    for order in orders:
        status = str(order.get("status") or "UNKNOWN").upper()
        if status == "FILLED":
            filled += 1
        elif status == "PARTIAL_FILLED":
            partial += 1
        elif status == "NO_FILL":
            no_fill += 1
        elif status in {"REJECTED", "EXPIRED", "CANCELED", "CANCEL_FAILED"}:
            rejected += 1
        requested_notional += _decimal(order.get("requested_notional"))
        filled_notional += _decimal(_first_non_blank(order.get("filled_notional"), order.get("actual_fill_notional")))
        fee_cost += _decimal(_first_non_blank(order.get("fee_cost"), order.get("fee")))
        rebate += _decimal(_first_non_blank(order.get("rebate"), order.get("rebate_cost")))
        execution_cost += _decimal(order.get("execution_cost"))
        if not _is_blank(order.get("fill_probability")):
            fill_probability.append(_decimal(order.get("fill_probability")))
        if not _is_blank(order.get("participation_rate")):
            participation.append(_decimal(order.get("participation_rate")))
        if not _is_blank(order.get("slippage_cost")):
            slippage_values.append(_decimal(order.get("slippage_cost")))
        meta = _json_dict(order.get("meta"))
        markouts = _json_dict(meta.get("markout_after_bars"))
        if not _is_blank(markouts.get("1")):
            markout_1.append(_decimal(markouts.get("1")))
        order_type = str(_first_non_blank(order.get("order_type"), meta.get("order_type"), "unknown"))
        order_types[order_type] = order_types.get(order_type, 0) + 1
        tif = str(_first_non_blank(order.get("time_in_force"), meta.get("time_in_force"), _json_dict(meta.get("strategy_intent")).get("time_in_force"), "unknown"))
        time_in_force[tif] = time_in_force.get(tif, 0) + 1
        reason = str(_first_non_blank(order.get("no_fill_reason"), "unknown"))
        if status == "NO_FILL":
            no_fill_reasons[reason] = no_fill_reasons.get(reason, 0) + 1

    active = Decimal(max(1, submitted))
    return {
        "role": role,
        "submitted_count": submitted,
        "filled_count": filled,
        "partial_fill_count": partial,
        "no_fill_count": no_fill,
        "rejected_count": rejected,
        "fill_rate": _decimal_string((Decimal(filled + partial) / active) * Decimal("100")),
        "no_fill_rate": _decimal_string((Decimal(no_fill) / active) * Decimal("100")),
        "requested_notional": _decimal_string(requested_notional),
        "filled_notional": _decimal_string(filled_notional),
        "filled_notional_rate": _decimal_string((filled_notional / requested_notional * Decimal("100")) if requested_notional else Decimal("0")),
        "avg_fill_probability": _average_decimal_string(fill_probability) or "0",
        "avg_participation_rate": _average_decimal_string(participation) or "0",
        "avg_slippage_cost": _average_decimal_string(slippage_values) or "0",
        "fee_cost": _decimal_string(fee_cost),
        "rebate": _decimal_string(rebate),
        "execution_cost": _decimal_string(execution_cost),
        "avg_markout_after_1_bars": _average_decimal_string(markout_1),
        "markout_sample_count": len(markout_1),
        "order_type_counts": dict(sorted(order_types.items())),
        "time_in_force_counts": dict(sorted(time_in_force.items())),
        "no_fill_reasons": dict(sorted(no_fill_reasons.items())),
    }


def _normalized_role(order: Mapping[str, Any]) -> str:
    role = str(_regime_bucket(order, "role") or "").strip().lower()
    if role in {"maker", "taker"}:
        return role
    meta = _json_dict(order.get("meta"))
    intent = _json_dict(meta.get("strategy_intent"))
    role = str(_first_non_blank(intent.get("role"), meta.get("order_role"), meta.get("role"), "")).strip().lower()
    return role if role in {"maker", "taker"} else "unknown"


def _maker_taker_next_actions(verdict: str, review_reasons: Sequence[str]) -> list[str]:
    if verdict == READY:
        return ["Use maker/taker split metrics when calibrating fill probability, slippage, fee, rebate, and markout assumptions."]
    return [f"Review maker/taker execution split: {reason}" for reason in review_reasons[:5]]


def build_maker_queue_uncertainty_report(orders: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Flag maker fills whose queue position is not proven by OrderFilled-only evidence."""

    order_rows = [dict(row) for row in orders]
    if not order_rows:
        return {
            "schema_version": "fill_first_maker_queue_uncertainty_v1",
            "status": MISSING,
            "queue_uncertainty_verdict": MISSING,
            "reason": "no orders available for maker queue uncertainty audit",
            "execution_scope": "missing",
            "maker_order_count": 0,
            "maker_filled_count": 0,
            "maker_no_fill_count": 0,
            "risk_order_count": 0,
            "high_participation_order_count": 0,
            "thin_evidence_order_count": 0,
            "missing_queue_evidence_count": 0,
            "avg_maker_participation_rate": "0",
            "suggested_fill_haircut_pct": "0",
            "risk_orders": [],
            "next_actions": ["Generate order lifecycle rows before auditing maker queue uncertainty."],
        }

    maker_orders = [row for row in order_rows if _normalized_role(row) == "maker"]
    if not maker_orders:
        return {
            "schema_version": "fill_first_maker_queue_uncertainty_v1",
            "status": READY,
            "queue_uncertainty_verdict": READY,
            "reason": "no maker orders in this run",
            "execution_scope": "not_applicable_no_maker_orders",
            "maker_order_count": 0,
            "maker_filled_count": 0,
            "maker_no_fill_count": 0,
            "risk_order_count": 0,
            "high_participation_order_count": 0,
            "thin_evidence_order_count": 0,
            "missing_queue_evidence_count": 0,
            "avg_maker_participation_rate": "0",
            "suggested_fill_haircut_pct": "0",
            "risk_orders": [],
            "next_actions": [],
        }

    risk_orders: list[dict[str, Any]] = []
    participation_values: list[Decimal] = []
    maker_filled_count = 0
    maker_no_fill_count = 0
    high_participation_count = 0
    thin_evidence_count = 0
    missing_queue_evidence_count = 0
    post_only_reject_count = 0
    cancel_race_count = 0
    adverse_selection_count = 0

    for order in maker_orders:
        status = str(order.get("status") or UNKNOWN).upper()
        if status in {"FILLED", "PARTIAL_FILLED"}:
            maker_filled_count += 1
        if status == "NO_FILL":
            maker_no_fill_count += 1
        meta = _json_dict(order.get("meta"))
        notes = _text_list(meta.get("notes"))
        no_fill_reason = str(order.get("no_fill_reason") or "").lower()
        participation = _maker_participation_rate(order)
        if participation is not None:
            participation_values.append(participation)
        trade_count = _to_int(order.get("trade_count"))
        queue_evidence = _has_queue_evidence(order)
        markout_1 = _maker_markout_after_1(order)
        reasons: list[str] = []
        if not queue_evidence:
            missing_queue_evidence_count += 1
            if status in {"FILLED", "PARTIAL_FILLED"}:
                reasons.append("maker fill is OrderFilled proxy only; no queue position evidence")
        if participation is not None and participation >= Decimal("50"):
            high_participation_count += 1
            reasons.append("maker order consumed at least 50% of observed block participation")
        if status in {"FILLED", "PARTIAL_FILLED"} and trade_count <= 1:
            thin_evidence_count += 1
            reasons.append("maker fill relies on one or fewer raw trades")
        if "post_only" in no_fill_reason or "post_only_rejected" in notes or bool(meta.get("post_only_rejected")):
            post_only_reject_count += 1
            reasons.append("postOnly rejection observed")
        if "cancel_race" in no_fill_reason or "cancel_race" in notes or bool(meta.get("cancel_race")):
            cancel_race_count += 1
            reasons.append("cancel race observed")
        if markout_1 is not None and markout_1 < Decimal("0"):
            adverse_selection_count += 1
            reasons.append("negative 1-bar markout after maker fill")
        if reasons:
            risk_orders.append(
                {
                    "order_id": order.get("order_id"),
                    "status": status,
                    "requested_notional": _decimal_string(_decimal(order.get("requested_notional"))),
                    "filled_notional": _decimal_string(_decimal(_first_non_blank(order.get("filled_notional"), order.get("actual_fill_notional")))),
                    "participation_rate": _decimal_string(participation or Decimal("0")),
                    "trade_count": trade_count,
                    "queue_evidence": queue_evidence,
                    "markout_after_1_bars": _decimal_string(markout_1) if markout_1 is not None else None,
                    "risk_level": _maker_queue_risk_level(reasons),
                    "risk_reasons": reasons,
                }
            )

    verdict = REVIEW if risk_orders else READY
    suggested_haircut = _maker_queue_suggested_haircut_pct(
        risk_order_count=len(risk_orders),
        maker_order_count=len(maker_orders),
        high_participation_count=high_participation_count,
        thin_evidence_count=thin_evidence_count,
        missing_queue_evidence_count=missing_queue_evidence_count,
    )
    if verdict == READY:
        reason = "maker OrderFilled-only queue uncertainty is low for this run"
    else:
        reason = "maker queue position is not proven by OrderFilled-only evidence; treat maker fills as calibrated proxy fills"
    return {
        "schema_version": "fill_first_maker_queue_uncertainty_v1",
        "status": READY,
        "queue_uncertainty_verdict": verdict,
        "reason": reason,
        "execution_scope": "orderfilled_proxy_no_lob",
        "maker_order_count": len(maker_orders),
        "maker_filled_count": maker_filled_count,
        "maker_no_fill_count": maker_no_fill_count,
        "risk_order_count": len(risk_orders),
        "high_participation_order_count": high_participation_count,
        "thin_evidence_order_count": thin_evidence_count,
        "missing_queue_evidence_count": missing_queue_evidence_count,
        "post_only_reject_count": post_only_reject_count,
        "cancel_race_count": cancel_race_count,
        "adverse_selection_count": adverse_selection_count,
        "avg_maker_participation_rate": _average_decimal_string(participation_values) or "0",
        "suggested_fill_haircut_pct": _decimal_string(suggested_haircut),
        "risk_orders": risk_orders[:50],
        "next_actions": _maker_queue_next_actions(verdict, risk_orders),
    }


def _maker_participation_rate(order: Mapping[str, Any]) -> Decimal | None:
    if not _is_blank(order.get("participation_rate")):
        return _decimal(order.get("participation_rate"))
    filled_notional = _decimal(_first_non_blank(order.get("filled_notional"), order.get("actual_fill_notional")))
    available_notional = _decimal(order.get("available_notional"))
    if available_notional > 0:
        return (filled_notional / available_notional * Decimal("100")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    return None


def _has_queue_evidence(order: Mapping[str, Any]) -> bool:
    meta = _json_dict(order.get("meta"))
    queue = _json_dict(_first_non_blank(order.get("queue_evidence"), order.get("queueEvidence"), meta.get("queue_evidence"), meta.get("queueEvidence")))
    if queue:
        return True
    for key in ("queue_position", "queuePosition", "queue_ahead_size", "queueAheadSize", "same_price_queue_ahead", "samePriceQueueAhead"):
        if not _is_blank(order.get(key)) or not _is_blank(meta.get(key)):
            return True
    return False


def _maker_markout_after_1(order: Mapping[str, Any]) -> Decimal | None:
    meta = _json_dict(order.get("meta"))
    markouts = _json_dict(meta.get("markout_after_bars"))
    value = _first_non_blank(order.get("markout_after_1_bars"), markouts.get("1"), markouts.get(1))
    return None if _is_blank(value) else _decimal(value)


def _maker_queue_risk_level(reasons: Sequence[str]) -> str:
    reason_text = " ".join(reasons).lower()
    if "50%" in reason_text or "negative" in reason_text or "postonly" in reason_text or "cancel race" in reason_text:
        return "high"
    if "one or fewer" in reason_text:
        return "medium"
    return "review"


def _maker_queue_suggested_haircut_pct(
    *,
    risk_order_count: int,
    maker_order_count: int,
    high_participation_count: int,
    thin_evidence_count: int,
    missing_queue_evidence_count: int,
) -> Decimal:
    if maker_order_count <= 0 or risk_order_count <= 0:
        return Decimal("0")
    risk_share = Decimal(risk_order_count) / Decimal(maker_order_count)
    haircut = Decimal("10") + risk_share * Decimal("25")
    haircut += Decimal(high_participation_count) * Decimal("5")
    haircut += Decimal(thin_evidence_count) * Decimal("2.5")
    if missing_queue_evidence_count >= maker_order_count:
        haircut += Decimal("10")
    return min(Decimal("75"), haircut).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _maker_queue_next_actions(verdict: str, risk_orders: Sequence[Mapping[str, Any]]) -> list[str]:
    if verdict == READY:
        return ["Maker queue uncertainty audit found no high-risk OrderFilled-only maker fills in this run."]
    return [
        "Treat maker fills as calibrated proxy fills until shadow/live fills or L2 queue evidence confirms queue position.",
        "Use suggested_fill_haircut_pct when stress-testing maker fill probability for this run/profile.",
        f"Review top maker queue risk orders: {', '.join(str(row.get('order_id')) for row in risk_orders[:5] if isinstance(row, Mapping) and row.get('order_id')) or '-'}",
    ]


def _text_list(value: Any) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value]
    if value is None:
        return []
    return [str(value)]


def _order_contexts(order: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    meta = _json_dict(order.get("meta"))
    return [
        order,
        _json_dict(meta.get("strategy_intent")) or _json_dict(meta.get("strategyIntent")),
        meta,
        _json_dict(meta.get("context")),
        _json_dict(meta.get("execution_context")),
        _json_dict(meta.get("executionContext")),
        _json_dict(meta.get("calibration_context")),
        _json_dict(meta.get("calibrationContext")),
    ]


def build_latency_profile_report(
    orders: Sequence[Mapping[str, Any]],
    fill_quality: Mapping[str, Any] | None = None,
    parameters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize configured and observed latency assumptions for fill-first replay."""

    order_rows = [dict(row) for row in orders]
    if not order_rows:
        return {
            "schema_version": "fill_first_latency_profile_v1",
            "status": MISSING,
            "latency_verdict": MISSING,
            "latency_profile": "missing",
            "reason": "no orders available for latency profile audit",
            "order_count": 0,
            "configured_latency_seconds": "0",
            "configured_latency_blocks": 0,
            "avg_order_latency_seconds": None,
            "avg_cancel_latency_seconds": None,
            "avg_effective_latency_x_span": None,
            "max_effective_latency_x_span": None,
            "latency_sample_count": 0,
            "missing_latency_count": 0,
            "fak_fok_order_count": 0,
            "cancel_sensitive_order_count": 0,
            "volatile_order_count": 0,
            "final_minute_order_count": 0,
            "stale_price_risk_count": 0,
            "review_reasons": [],
            "next_actions": ["Generate order lifecycle rows before auditing latency assumptions."],
        }

    fill = _json_dict(fill_quality or {})
    params = _json_dict(parameters or {})
    configured_seconds = _decimal(_first_non_blank(params.get("latency_seconds"), params.get("latencySeconds"), fill.get("avg_latency_seconds")))
    configured_blocks = _to_int(_first_non_blank(params.get("latency_blocks"), params.get("latencyBlocks"), 0))
    observed_seconds: list[Decimal] = []
    observed_blocks: list[Decimal] = []
    effective_spans: list[Decimal] = []
    missing_latency_count = 0
    fak_fok_count = 0
    cancel_sensitive_count = 0
    volatile_count = 0
    final_minute_count = 0
    stale_risk_count = 0
    review_reasons: list[str] = []
    risk_orders: list[dict[str, Any]] = []

    for order in order_rows:
        meta = _json_dict(order.get("meta"))
        contexts = _order_contexts(order)
        tif = str(_first_non_blank(order.get("time_in_force"), meta.get("time_in_force"), _json_dict(meta.get("strategy_intent")).get("time_in_force"), "")).upper()
        if tif in {"FAK", "FOK"}:
            fak_fok_count += 1
        no_fill_reason = str(order.get("no_fill_reason") or "").lower()
        notes = _text_list(meta.get("notes"))
        if "cancel" in no_fill_reason or any("cancel" in item.lower() for item in notes):
            cancel_sensitive_count += 1
        volatility = _regime_bucket(order, "volatility_bucket")
        if volatility in {"high", "volatile", "very_high"}:
            volatile_count += 1
        if _regime_bucket(order, "final_minute") == "final_minute":
            final_minute_count += 1

        block_latency = _decimal(_first_non_blank(order.get("latency_blocks"), _first_context_value(contexts, ("latency_blocks", "latencyBlocks"))))
        second_latency = _decimal(_first_non_blank(order.get("latency_seconds"), _first_context_value(contexts, ("latency_seconds", "latencySeconds"))))
        span_latency = _decimal(_first_context_value(contexts, ("effective_latency_x_span", "effectiveLatencyXSpan")))
        if block_latency:
            observed_blocks.append(block_latency)
        if second_latency:
            observed_seconds.append(second_latency)
        if span_latency:
            effective_spans.append(span_latency)
        if not block_latency and not second_latency and not span_latency:
            missing_latency_count += 1

        order_reasons: list[str] = []
        conservative_enough = bool(block_latency >= 1 or configured_blocks >= 1 or second_latency >= Decimal("1") or configured_seconds >= Decimal("1"))
        cancel_stress_enough = bool(block_latency >= 1 or configured_blocks >= 1 or second_latency >= Decimal("2") or configured_seconds >= Decimal("2"))
        if tif in {"FAK", "FOK"} and not cancel_stress_enough:
            order_reasons.append("FAK/FOK order has less than 2s or 1-block latency stress")
        if ("cancel" in no_fill_reason or any("cancel" in item.lower() for item in notes)) and not cancel_stress_enough:
            order_reasons.append("cancel-sensitive order has less than 2s or 1-block latency stress")
        if volatility in {"high", "volatile", "very_high"} and not conservative_enough:
            order_reasons.append("volatile-regime order has less than 1s or 1-block latency stress")
        if _regime_bucket(order, "final_minute") == "final_minute" and not conservative_enough:
            order_reasons.append("final-minute order has less than 1s or 1-block latency stress")
        if order_reasons:
            stale_risk_count += 1
            risk_orders.append(
                {
                    "order_id": order.get("order_id"),
                    "time_in_force": tif or "unknown",
                    "latency_seconds": _decimal_string(second_latency),
                    "latency_blocks": _decimal_string(block_latency),
                    "volatility_bucket": volatility,
                    "final_minute": _regime_bucket(order, "final_minute"),
                    "risk_reasons": order_reasons,
                }
            )

    if missing_latency_count:
        review_reasons.append(f"{missing_latency_count} orders do not preserve latency fields")
    if stale_risk_count:
        review_reasons.append(f"{stale_risk_count} orders have latency stress below their execution sensitivity")
    if configured_seconds == 0 and configured_blocks == 0 and order_rows:
        review_reasons.append("configured latency is zero; this is optimistic for paper/live parity")
    latency_profile = _latency_profile_name(configured_seconds, configured_blocks, observed_seconds, observed_blocks)
    verdict = REVIEW if review_reasons else READY
    return {
        "schema_version": "fill_first_latency_profile_v1",
        "status": READY,
        "latency_verdict": verdict,
        "latency_profile": latency_profile,
        "reason": "latency assumptions are explicit and conservative enough for this run" if verdict == READY else "; ".join(review_reasons),
        "order_count": len(order_rows),
        "configured_latency_seconds": _decimal_string(configured_seconds),
        "configured_latency_blocks": configured_blocks,
        "avg_order_latency_seconds": _average_decimal_string(observed_seconds) or fill.get("avg_order_submit_accept_latency_seconds"),
        "avg_cancel_latency_seconds": fill.get("avg_cancel_accept_latency_seconds"),
        "avg_effective_latency_x_span": _average_decimal_string(effective_spans) or fill.get("avg_effective_latency_x_span"),
        "max_effective_latency_x_span": _decimal_string(max(effective_spans)) if effective_spans else fill.get("max_effective_latency_x_span"),
        "avg_latency_blocks": _average_decimal_string(observed_blocks) or fill.get("avg_latency_blocks"),
        "latency_sample_count": len(observed_seconds) + len(observed_blocks) + len(effective_spans),
        "missing_latency_count": missing_latency_count,
        "fak_fok_order_count": fak_fok_count,
        "cancel_sensitive_order_count": cancel_sensitive_count,
        "volatile_order_count": volatile_count,
        "final_minute_order_count": final_minute_count,
        "stale_price_risk_count": stale_risk_count,
        "review_reasons": review_reasons,
        "risk_orders": risk_orders[:50],
        "next_actions": _latency_profile_next_actions(verdict, review_reasons),
    }


def _latency_profile_name(
    configured_seconds: Decimal,
    configured_blocks: int,
    observed_seconds: Sequence[Decimal],
    observed_blocks: Sequence[Decimal],
) -> str:
    max_seconds = max([configured_seconds, *observed_seconds], default=Decimal("0"))
    max_blocks = max([Decimal(configured_blocks), *observed_blocks], default=Decimal("0"))
    if max_blocks >= 3 or max_seconds >= Decimal("5"):
        return "stress"
    if max_blocks >= 1 or max_seconds >= Decimal("1"):
        return "conservative"
    if max_seconds > 0:
        return "sub_second"
    return "zero_latency"


def _latency_profile_next_actions(verdict: str, review_reasons: Sequence[str]) -> list[str]:
    if verdict == READY:
        return ["Use latency_profile when comparing realistic/conservative execution profiles and shadow/live drift."]
    actions = [f"Review latency profile: {reason}" for reason in review_reasons[:5]]
    actions.append("For FAK/FOK/cancel-sensitive tests, run a 2-3s or at least 1-block latency stress profile.")
    return actions


def build_slippage_regime_report(
    orders: Sequence[Mapping[str, Any]],
    fill_quality: Mapping[str, Any] | None = None,
    parameters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Audit whether slippage assumptions are explicit across risky fill-first regimes."""

    order_rows = [dict(row) for row in orders]
    if not order_rows:
        return {
            "schema_version": "fill_first_slippage_regime_v1",
            "status": MISSING,
            "slippage_regime_verdict": MISSING,
            "reason": "no orders available for slippage regime audit",
            "order_count": 0,
            "filled_order_count": 0,
            "configured_slippage_bps": "0",
            "configured_adverse_slippage_cents": "0",
            "avg_slippage_cost": "0",
            "avg_slippage_bps": "0",
            "avg_adverse_slippage_cents": "0",
            "risk_order_count": 0,
            "risk_regime_count": 0,
            "regime_summaries": [],
            "review_reasons": [],
            "risk_orders": [],
            "next_actions": ["Generate order lifecycle rows before auditing slippage by regime."],
        }

    fill = _json_dict(fill_quality or {})
    params = _json_dict(parameters or {})
    configured_slippage_bps = _decimal(
        _first_non_blank(
            params.get("slippage_bps"),
            params.get("slippageBps"),
            params.get("adverse_slippage_bps"),
            params.get("adverseSlippageBps"),
            fill.get("avg_slippage_bps"),
            0,
        )
    )
    configured_adverse_cents = _decimal(
        _first_non_blank(
            params.get("adverse_slippage_cents"),
            params.get("adverseSlippageCents"),
            params.get("price_buffer_cents"),
            params.get("priceBufferCents"),
            0,
        )
    )
    slippage_cost_values: list[Decimal] = []
    slippage_bps_values: list[Decimal] = []
    adverse_cents_values: list[Decimal] = []
    risk_orders: list[dict[str, Any]] = []
    review_reasons: list[str] = []
    regime_rows: dict[str, dict[str, Any]] = {}
    filled_count = 0

    for order in order_rows:
        status = str(order.get("status") or UNKNOWN).upper()
        filled = status in {"FILLED", "PARTIAL_FILLED"}
        if filled:
            filled_count += 1
        contexts = _order_contexts(order)
        price = _decimal(_first_non_blank(order.get("requested_price"), order.get("decision_price"), order.get("fill_price"), order.get("price")))
        filled_notional = _decimal(_first_non_blank(order.get("filled_notional"), order.get("actual_fill_notional")))
        slippage_cost = _decimal(_first_non_blank(order.get("slippage_cost"), _first_context_value(contexts, ("slippage_cost", "slippageCost")), 0))
        slippage_bps = Decimal("0")
        if filled_notional > 0:
            slippage_bps = (slippage_cost / filled_notional * Decimal("10000")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
            slippage_cost_values.append(slippage_cost)
            slippage_bps_values.append(slippage_bps)
        adverse_cents = _decimal(
            _first_non_blank(
                _first_context_value(contexts, ("adverse_slippage_cents", "adverseSlippageCents")),
                configured_adverse_cents,
            )
        )
        if adverse_cents:
            adverse_cents_values.append(adverse_cents)
        participation = _maker_participation_rate(order)
        liquidity = _regime_bucket(order, "liquidity_bucket")
        volatility = _regime_bucket(order, "volatility_bucket")
        time_to_expiry = _regime_bucket(order, "time_to_expiry_bucket")
        final_minute = _regime_bucket(order, "final_minute")
        size_bucket = _slippage_size_bucket(participation, filled_notional, _decimal(order.get("requested_notional")))
        price_level = _slippage_price_level(price)
        dimension_buckets = {
            "liquidity": liquidity,
            "volatility": volatility,
            "time_to_expiry": time_to_expiry,
            "final_minute": final_minute,
            "order_size": size_bucket,
            "price_level": price_level,
        }
        risk_regimes = _slippage_risk_regimes(
            liquidity=liquidity,
            volatility=volatility,
            final_minute=final_minute,
            size_bucket=size_bucket,
            price_level=price_level,
        )
        order_reasons = _slippage_order_review_reasons(
            filled=filled,
            risk_regimes=risk_regimes,
            price_level=price_level,
            size_bucket=size_bucket,
            slippage_bps=slippage_bps,
            configured_slippage_bps=configured_slippage_bps,
            adverse_cents=adverse_cents,
        )
        for dimension, bucket in dimension_buckets.items():
            _slippage_update_regime_row(
                regime_rows,
                dimension=dimension,
                bucket=bucket,
                filled=filled,
                slippage_cost=slippage_cost,
                slippage_bps=slippage_bps,
                adverse_cents=adverse_cents,
                risk=bool(order_reasons),
            )
        if order_reasons:
            risk_orders.append(
                {
                    "order_id": order.get("order_id"),
                    "status": status,
                    "price": _decimal_string(price),
                    "filled_notional": _decimal_string(filled_notional),
                    "participation_rate": _decimal_string(participation or Decimal("0")),
                    "liquidity_bucket": liquidity,
                    "volatility_bucket": volatility,
                    "time_to_expiry_bucket": time_to_expiry,
                    "final_minute": final_minute,
                    "size_bucket": size_bucket,
                    "price_level": price_level,
                    "slippage_cost": _decimal_string(slippage_cost),
                    "slippage_bps": _decimal_string(slippage_bps),
                    "adverse_slippage_cents": _decimal_string(adverse_cents),
                    "risk_regimes": risk_regimes,
                    "risk_reasons": order_reasons,
                }
            )

    if risk_orders:
        review_reasons.append(f"{len(risk_orders)} filled orders have optimistic slippage in risky regimes")
    if configured_slippage_bps == 0 and configured_adverse_cents == 0 and filled_count:
        review_reasons.append("configured slippage/adverse price buffer is zero")
    regime_summaries = [_slippage_finalize_regime_row(row) for row in regime_rows.values()]
    risk_regime_count = sum(1 for row in regime_summaries if _to_int(row.get("risk_order_count")) > 0)
    verdict = REVIEW if review_reasons else READY
    return {
        "schema_version": "fill_first_slippage_regime_v1",
        "status": READY,
        "slippage_regime_verdict": verdict,
        "reason": "slippage assumptions are explicit enough across observed regimes" if verdict == READY else "; ".join(review_reasons),
        "order_count": len(order_rows),
        "filled_order_count": filled_count,
        "configured_slippage_bps": _decimal_string(configured_slippage_bps),
        "configured_adverse_slippage_cents": _decimal_string(configured_adverse_cents),
        "avg_slippage_cost": _average_decimal_string(slippage_cost_values) or "0",
        "avg_slippage_bps": _average_decimal_string(slippage_bps_values) or "0",
        "avg_adverse_slippage_cents": _average_decimal_string(adverse_cents_values) or "0",
        "risk_order_count": len(risk_orders),
        "risk_regime_count": risk_regime_count,
        "regime_summaries": sorted(regime_summaries, key=lambda row: (str(row.get("dimension")), str(row.get("bucket")))),
        "review_reasons": review_reasons,
        "risk_orders": risk_orders[:50],
        "next_actions": _slippage_regime_next_actions(verdict, review_reasons),
    }


def _slippage_size_bucket(participation: Decimal | None, filled_notional: Decimal, requested_notional: Decimal) -> str:
    if participation is not None:
        if participation >= Decimal("50"):
            return "large_50pct_plus"
        if participation >= Decimal("20"):
            return "medium_20_50pct"
        if participation > 0:
            return "small_under_20pct"
    notional = max(filled_notional, requested_notional)
    if notional >= Decimal("1000"):
        return "large_notional_1000_plus"
    if notional >= Decimal("100"):
        return "medium_notional_100_1000"
    if notional > 0:
        return "small_notional_under_100"
    return "unknown"


def _slippage_price_level(price: Decimal) -> str:
    if price >= Decimal("0.95"):
        return "high_95_plus"
    if price >= Decimal("0.70"):
        return "high_70_95"
    if price >= Decimal("0.60"):
        return "trend_chase_60_70"
    if price >= Decimal("0.40"):
        return "mid_40_60"
    if price >= Decimal("0.20"):
        return "low_20_40"
    if price > 0:
        return "deep_low_under_20"
    return "unknown"


def _slippage_risk_regimes(
    *,
    liquidity: str,
    volatility: str,
    final_minute: str,
    size_bucket: str,
    price_level: str,
) -> list[str]:
    regimes: list[str] = []
    if liquidity in {"thin", "low", "sparse", "illiquid"}:
        regimes.append("thin_liquidity")
    if volatility in {"high", "volatile", "very_high"}:
        regimes.append("high_volatility")
    if final_minute == "final_minute":
        regimes.append("final_minute")
    if size_bucket in {"large_50pct_plus", "large_notional_1000_plus"}:
        regimes.append("large_order")
    if price_level in {"trend_chase_60_70", "high_70_95", "high_95_plus"}:
        regimes.append(price_level)
    return regimes


def _slippage_order_review_reasons(
    *,
    filled: bool,
    risk_regimes: Sequence[str],
    price_level: str,
    size_bucket: str,
    slippage_bps: Decimal,
    configured_slippage_bps: Decimal,
    adverse_cents: Decimal,
) -> list[str]:
    if not filled or not risk_regimes:
        return []
    reasons: list[str] = []
    has_explicit_stress = configured_slippage_bps > 0 or adverse_cents >= Decimal("0.01") or slippage_bps > 0
    if not has_explicit_stress:
        reasons.append("risky filled order has zero explicit slippage/adverse price stress")
    if price_level == "trend_chase_60_70" and adverse_cents < Decimal("0.01") and slippage_bps < Decimal("5"):
        reasons.append("60-70c trend-chase fill lacks at least 1c or 5bps adverse slippage stress")
    if any(item in risk_regimes for item in ("thin_liquidity", "high_volatility", "final_minute")) and adverse_cents < Decimal("0.02") and slippage_bps < Decimal("10"):
        reasons.append("thin/high-vol/final-minute fill lacks at least 2c or 10bps stress")
    if size_bucket in {"large_50pct_plus", "large_notional_1000_plus"} and slippage_bps < Decimal("5") and adverse_cents < Decimal("0.01"):
        reasons.append("large participation fill has less than 5bps or 1c slippage stress")
    return reasons


def _slippage_update_regime_row(
    rows: dict[str, dict[str, Any]],
    *,
    dimension: str,
    bucket: str,
    filled: bool,
    slippage_cost: Decimal,
    slippage_bps: Decimal,
    adverse_cents: Decimal,
    risk: bool,
) -> None:
    key = f"{dimension}:{bucket}"
    row = rows.setdefault(
        key,
        {
            "dimension": dimension,
            "bucket": bucket,
            "order_count": 0,
            "filled_order_count": 0,
            "risk_order_count": 0,
            "_slippage_cost_values": [],
            "_slippage_bps_values": [],
            "_adverse_cents_values": [],
        },
    )
    row["order_count"] += 1
    if filled:
        row["filled_order_count"] += 1
        row["_slippage_cost_values"].append(slippage_cost)
        row["_slippage_bps_values"].append(slippage_bps)
    if adverse_cents:
        row["_adverse_cents_values"].append(adverse_cents)
    if risk:
        row["risk_order_count"] += 1


def _slippage_finalize_regime_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "dimension": row.get("dimension"),
        "bucket": row.get("bucket"),
        "order_count": _to_int(row.get("order_count")),
        "filled_order_count": _to_int(row.get("filled_order_count")),
        "risk_order_count": _to_int(row.get("risk_order_count")),
        "avg_slippage_cost": _average_decimal_string(row.get("_slippage_cost_values") or []) or "0",
        "avg_slippage_bps": _average_decimal_string(row.get("_slippage_bps_values") or []) or "0",
        "avg_adverse_slippage_cents": _average_decimal_string(row.get("_adverse_cents_values") or []) or "0",
    }


def _slippage_regime_next_actions(verdict: str, review_reasons: Sequence[str]) -> list[str]:
    if verdict == READY:
        return ["Use slippage_regime_report when comparing realistic/stress execution profiles by liquidity, volatility, expiry, size, and price level."]
    actions = [f"Review slippage regime: {reason}" for reason in review_reasons[:5]]
    actions.append("Calibrate adverse_slippage_cents or slippage_bps by fill-first evidence before promoting this run/profile.")
    return actions


def _regime_dimension_rows(orders: Sequence[Mapping[str, Any]], dimension: str) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for order in orders:
        bucket = _regime_bucket(order, dimension)
        row = buckets.setdefault(
            bucket,
            {
                "bucket": bucket,
                "submitted_count": 0,
                "filled_count": 0,
                "partial_fill_count": 0,
                "no_fill_count": 0,
                "rejected_count": 0,
                "requested_notional": Decimal("0"),
                "filled_notional": Decimal("0"),
            },
        )
        status = str(order.get("status") or "UNKNOWN").upper()
        row["submitted_count"] += 1
        if status == "FILLED":
            row["filled_count"] += 1
        elif status == "PARTIAL_FILLED":
            row["partial_fill_count"] += 1
        elif status == "NO_FILL":
            row["no_fill_count"] += 1
        elif status in {"REJECTED", "EXPIRED", "CANCELED", "CANCEL_FAILED"}:
            row["rejected_count"] += 1
        row["requested_notional"] += _decimal(order.get("requested_notional"))
        row["filled_notional"] += _decimal(order.get("filled_notional"))
    result: list[dict[str, Any]] = []
    for row in buckets.values():
        submitted = max(1, int(row["submitted_count"]))
        requested = row["requested_notional"]
        result.append(
            {
                "bucket": row["bucket"],
                "submitted_count": row["submitted_count"],
                "filled_count": row["filled_count"],
                "partial_fill_count": row["partial_fill_count"],
                "no_fill_count": row["no_fill_count"],
                "rejected_count": row["rejected_count"],
                "requested_notional": _decimal_string(requested),
                "filled_notional": _decimal_string(row["filled_notional"]),
                "fill_rate": _decimal_string((Decimal(row["filled_count"] + row["partial_fill_count"]) / Decimal(submitted) * Decimal("100"))),
                "filled_notional_rate": _decimal_string((row["filled_notional"] / requested * Decimal("100")) if requested else Decimal("0")),
            }
        )
    return sorted(result, key=lambda item: (-_to_int(item["submitted_count"]), str(item["bucket"])))


def build_tail_risk_report(
    trades: Sequence[Mapping[str, Any]],
    parameters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    trade_rows = [dict(row) for row in trades]
    if not trade_rows:
        return {
            "status": REVIEW,
            "risk_verdict": "review",
            "reason": "no closed trades available for tail-risk stress",
            "trade_count": 0,
            "win_rate": "0",
            "loss_rate": "0",
            "net_pnl": "0",
            "gross_profit": "0",
            "gross_loss": "0",
            "avg_win": "0",
            "avg_loss": "0",
            "max_single_loss": "0",
            "max_single_win": "0",
            "loss_tail_ratio": "0",
            "profit_buffer_to_max_loss": "0",
            "consecutive_loss_streak": 0,
            "high_price_trade_count": 0,
            "payoff_distribution": {},
            "ruin_risk": {},
            "position_concentration": {},
            "stress": [],
        }
    pnls = [_decimal(row.get("pnl")) for row in trade_rows]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    gross_profit = sum(wins, Decimal("0"))
    gross_loss = sum(losses, Decimal("0"))
    net = sum(pnls, Decimal("0"))
    max_single_loss = min(pnls, default=Decimal("0"))
    max_single_win = max(pnls, default=Decimal("0"))
    avg_win = gross_profit / Decimal(len(wins)) if wins else Decimal("0")
    avg_loss = gross_loss / Decimal(len(losses)) if losses else Decimal("0")
    trade_count = len(trade_rows)
    win_rate = Decimal(len(wins)) / Decimal(trade_count) * Decimal("100")
    loss_rate = Decimal(len(losses)) / Decimal(trade_count) * Decimal("100")
    initial_capital = _decimal((parameters or {}).get("initial_capital")) or _decimal((parameters or {}).get("initialCapital"))
    max_loss_abs = abs(max_single_loss)
    loss_tail_ratio = max_loss_abs / avg_win if avg_win > 0 else Decimal("0")
    profit_buffer = gross_profit / max_loss_abs if max_loss_abs > 0 else Decimal("0")
    streak = _max_consecutive_losses(pnls)
    high_price_trade_count = sum(1 for row in trade_rows if _is_high_price_trade(row))
    notionals = [_trade_notional(row) for row in trade_rows]
    total_notional = sum(notionals, Decimal("0"))
    max_notional = max(notionals, default=Decimal("0"))
    payoff_distribution = _payoff_distribution(pnls)
    ruin_risk = _ruin_risk(max_loss_abs, initial_capital, gross_profit)
    position_concentration = {
        "total_trade_notional": _decimal_string(total_notional),
        "max_trade_notional": _decimal_string(max_notional),
        "max_trade_notional_pct_of_total": _decimal_string((max_notional / total_notional * Decimal("100")) if total_notional > 0 else Decimal("0")),
        "max_trade_notional_pct_of_capital": _decimal_string(_pct_of_capital(max_notional, initial_capital)),
    }
    stress = []
    for length in TAIL_RISK_STRESS_LENGTHS:
        stress_loss = max_loss_abs * Decimal(length)
        stress.append(
            {
                "loss_count": length,
                "loss_amount": _decimal_string(stress_loss),
                "loss_pct_of_capital": _decimal_string(_pct_of_capital(stress_loss, initial_capital)),
                "wipes_gross_profit": bool(stress_loss > gross_profit and gross_profit > 0),
            }
        )
    reasons: list[str] = []
    if win_rate >= Decimal("95") and max_loss_abs > avg_win * Decimal("5") and avg_win > 0:
        reasons.append("high win-rate with outsized single-loss tail")
    if high_price_trade_count:
        reasons.append(f"high-probability price trades={high_price_trade_count}")
    if max_loss_abs > gross_profit and gross_profit > 0:
        reasons.append("one max-loss trade can wipe all gross profit")
    if ruin_risk.get("status") == REVIEW:
        reasons.append(str(ruin_risk.get("reason") or "ruin stress needs review"))
    if _decimal(position_concentration["max_trade_notional_pct_of_capital"]) >= Decimal("25"):
        reasons.append("max trade notional >=25% of capital")
    if _decimal(position_concentration["max_trade_notional_pct_of_total"]) >= Decimal("50") and trade_count > 1:
        reasons.append("trade notional concentration >=50%")
    if streak >= 3:
        reasons.append(f"consecutive loss streak={streak}")
    risk_verdict = REVIEW if reasons else READY
    return {
        "status": READY,
        "risk_verdict": risk_verdict,
        "reason": "; ".join(reasons) if reasons else "tail risk stress built from closed trade PnL",
        "trade_count": trade_count,
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate": _decimal_string(win_rate),
        "loss_rate": _decimal_string(loss_rate),
        "net_pnl": _decimal_string(net),
        "gross_profit": _decimal_string(gross_profit),
        "gross_loss": _decimal_string(gross_loss),
        "avg_win": _decimal_string(avg_win),
        "avg_loss": _decimal_string(avg_loss),
        "max_single_loss": _decimal_string(max_single_loss),
        "max_single_win": _decimal_string(max_single_win),
        "loss_tail_ratio": _decimal_string(loss_tail_ratio),
        "profit_buffer_to_max_loss": _decimal_string(profit_buffer),
        "consecutive_loss_streak": streak,
        "high_price_trade_count": high_price_trade_count,
        "payoff_distribution": payoff_distribution,
        "ruin_risk": ruin_risk,
        "position_concentration": position_concentration,
        "stress": stress,
    }


def build_prediction_quality_report(
    run: Mapping[str, Any],
    parameters: Mapping[str, Any] | None,
    orders: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    order_by_trade_id = {
        str(order.get("trade_id")): order
        for order in orders
        if not _is_blank(order.get("trade_id"))
    }
    parameter_context = _json_dict(parameters)
    scored: list[Decimal] = []
    baseline_scores: list[Decimal] = []
    buckets: dict[str, dict[str, Decimal | int]] = {}
    rows: list[dict[str, Any]] = []
    fallback_baseline_count = 0
    skipped_unresolved = 0

    for trade in trades:
        linked_order = order_by_trade_id.get(str(trade.get("trade_id"))) if not _is_blank(trade.get("trade_id")) else None
        predicted = _prediction_probability(trade, linked_order)
        actual = _realized_outcome_value(trade, run, parameter_context)
        if predicted is None or actual is None:
            skipped_unresolved += 1
            continue
        baseline = _baseline_probability(trade, linked_order)
        if baseline is None:
            baseline = predicted
            fallback_baseline_count += 1
        brier = (predicted - actual) ** 2
        baseline_brier = (baseline - actual) ** 2
        scored.append(brier)
        baseline_scores.append(baseline_brier)
        bucket = _probability_bucket(predicted)
        state = buckets.setdefault(bucket, {"count": 0, "predicted_sum": Decimal("0"), "actual_sum": Decimal("0"), "brier_sum": Decimal("0")})
        state["count"] = int(state["count"]) + 1
        state["predicted_sum"] = Decimal(state["predicted_sum"]) + predicted
        state["actual_sum"] = Decimal(state["actual_sum"]) + actual
        state["brier_sum"] = Decimal(state["brier_sum"]) + brier
        rows.append(
            {
                "trade_id": trade.get("trade_id"),
                "predicted_probability": _decimal_string(predicted),
                "actual_outcome": _decimal_string(actual),
                "market_probability": _decimal_string(baseline),
                "brier_score": _decimal_string(brier),
                "market_brier_score": _decimal_string(baseline_brier),
                "bucket": bucket,
            }
        )

    sample_count = len(scored)
    brier_score = (sum(scored, Decimal("0")) / Decimal(sample_count)) if sample_count else Decimal("0")
    market_brier_score = (sum(baseline_scores, Decimal("0")) / Decimal(sample_count)) if sample_count else Decimal("0")
    brier_advantage = market_brier_score - brier_score
    review_reasons: list[str] = []
    if sample_count <= 0:
        review_reasons.append("no resolved trades with prediction probability")
    if fallback_baseline_count:
        review_reasons.append(f"market close-line baseline missing for {fallback_baseline_count} samples")
    prediction_verdict = REVIEW if review_reasons else READY
    return {
        "status": READY,
        "prediction_verdict": prediction_verdict,
        "reason": "; ".join(review_reasons) if review_reasons else "prediction quality can be compared against realized outcomes and market baseline",
        "sample_count": sample_count,
        "skipped_unresolved_count": skipped_unresolved,
        "baseline_fallback_count": fallback_baseline_count,
        "brier_score": _decimal_string(brier_score),
        "market_brier_score": _decimal_string(market_brier_score),
        "brier_advantage": _decimal_string(brier_advantage),
        "calibration_buckets": [
            {
                "bucket": bucket,
                "count": int(values["count"]),
                "avg_predicted": _decimal_string(Decimal(values["predicted_sum"]) / Decimal(int(values["count"]))),
                "actual_rate": _decimal_string(Decimal(values["actual_sum"]) / Decimal(int(values["count"]))),
                "brier_score": _decimal_string(Decimal(values["brier_sum"]) / Decimal(int(values["count"]))),
            }
            for bucket, values in sorted(buckets.items())
            if int(values["count"]) > 0
        ],
        "rows": rows[:100],
    }


def _prediction_probability(trade: Mapping[str, Any], order: Mapping[str, Any] | None) -> Decimal | None:
    value = _first_row_value(
        (trade, order or {}),
        ("signal_probability", "predicted_probability", "entry_probability", "entry_price", "avg_entry_price", "price"),
    )
    return _probability_or_none(value)


def _baseline_probability(trade: Mapping[str, Any], order: Mapping[str, Any] | None) -> Decimal | None:
    value = _first_row_value(
        (trade, order or {}),
        ("close_line_probability", "market_close_probability", "market_probability", "close_probability", "close_line_price"),
    )
    return _probability_or_none(value)


def _realized_outcome_value(trade: Mapping[str, Any], run: Mapping[str, Any], parameters: Mapping[str, Any]) -> Decimal | None:
    value = _first_row_value(
        (trade, parameters, _json_dict(run.get("meta"))),
        ("realized_outcome", "resolved_value", "payoff_per_share", "settlement_value", "exit_price"),
    )
    return _probability_or_none(value)


def _first_row_value(rows: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> Any:
    for row in rows:
        for key in keys:
            if not _is_blank(row.get(key)):
                return row.get(key)
    return None


def _probability_or_none(value: Any) -> Decimal | None:
    if _is_blank(value):
        return None
    probability = _decimal(value)
    if probability > Decimal("1") and probability <= Decimal("100"):
        probability = probability / Decimal("100")
    if probability < 0:
        return Decimal("0")
    if probability > 1:
        return Decimal("1")
    return probability


def _probability_bucket(value: Decimal) -> str:
    if value < Decimal("0.2"):
        return "0.00-0.20"
    if value < Decimal("0.4"):
        return "0.20-0.40"
    if value < Decimal("0.6"):
        return "0.40-0.60"
    if value < Decimal("0.8"):
        return "0.60-0.80"
    return "0.80-1.00"


def build_market_lifecycle_report(
    run: Mapping[str, Any],
    parameters: Mapping[str, Any] | None,
    trades: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    contexts = _settlement_contexts(run, parameters)
    final_valuation_mode = str(_first_context_value(contexts, ("final_valuation_mode", "finalValuationMode")) or "UNKNOWN").upper()
    settlement_value = _first_context_value(contexts, ("settlement_value", "settlementValue"))
    lifecycle_status = str(
        _first_context_value(
            contexts,
            ("market_lifecycle_status", "marketLifecycleStatus", "market_status", "marketStatus", "resolution_status", "resolutionStatus"),
        )
        or "unknown"
    ).lower()
    resolved_outcome = _first_context_value(
        contexts,
        ("resolved_outcome", "resolvedOutcome", "winning_outcome", "winningOutcome", "settlement_outcome", "settlementOutcome"),
    )
    end_date = _first_context_value(contexts, ("end_date", "endDate", "end_ts", "endTs", "market_end_date", "marketEndDate"))
    resolution_ts = _first_context_value(contexts, ("resolution_ts", "resolutionTs", "resolved_at", "resolvedAt", "resolution_time", "resolutionTime"))
    lifecycle_flags = {
        "closed": _context_bool(contexts, ("market_closed", "marketClosed", "closed")) or lifecycle_status in {"closed", "resolved", "settled", "finalized"},
        "archived": _context_bool(contexts, ("market_archived", "marketArchived", "archived")),
        "refunded": _context_bool(contexts, ("market_refunded", "marketRefunded", "refunded")),
        "invalid": _context_bool(contexts, ("market_invalid", "marketInvalid", "invalid")),
        "cancelled": _context_bool(contexts, ("market_cancelled", "marketCanceled", "marketCancelled", "cancelled", "canceled")),
        "misconfigured": _context_bool(contexts, ("market_misconfigured", "marketMisconfigured", "misconfigured")),
        "resolved": _context_bool(contexts, ("market_resolved", "marketResolved", "resolved")) or not _is_blank(resolved_outcome) or lifecycle_status in {"resolved", "settled", "closed", "finalized"},
    }
    settlement_trade_count = sum(1 for trade in trades if str(trade.get("exit_reason") or "").lower() == "settlement")
    settlement_ledger_count = sum(1 for row in ledger if str(row.get("event_type") or "").upper() == "SETTLEMENT")
    refund_ledger_count = sum(1 for row in ledger if "REFUND" in str(row.get("event_type") or "").upper())
    missing_fields: list[str] = []
    if final_valuation_mode == "SETTLEMENT" and settlement_trade_count > 0:
        if _is_blank(resolved_outcome):
            missing_fields.append("resolved_outcome")
        if _is_blank(end_date):
            missing_fields.append("end_date")
        if lifecycle_status == "unknown":
            missing_fields.append("market_lifecycle_status")
    review_reasons: list[str] = []
    if missing_fields:
        review_reasons.append("missing market lifecycle fields: " + ", ".join(missing_fields))
    if any(lifecycle_flags[key] for key in ("refunded", "invalid", "cancelled", "misconfigured")):
        review_reasons.append("abnormal market lifecycle flag present")
    if refund_ledger_count and not lifecycle_flags["refunded"]:
        review_reasons.append("refund ledger events without refunded lifecycle flag")
    if final_valuation_mode == "SETTLEMENT" and settlement_trade_count > 0 and _is_blank(settlement_value):
        review_reasons.append("settlement trades exist but settlement_value is missing")
    lifecycle_verdict = REVIEW if review_reasons else READY
    return {
        "status": READY,
        "lifecycle_verdict": lifecycle_verdict,
        "reason": "; ".join(review_reasons) if review_reasons else "market lifecycle fields are compatible with settlement/redeem reporting",
        "final_valuation_mode": final_valuation_mode,
        "settlement_value": None if _is_blank(settlement_value) else str(settlement_value),
        "market_lifecycle_status": lifecycle_status,
        "resolved_outcome": None if _is_blank(resolved_outcome) else str(resolved_outcome),
        "end_date": None if _is_blank(end_date) else str(end_date),
        "resolution_ts": None if _is_blank(resolution_ts) else str(resolution_ts),
        "lifecycle_flags": lifecycle_flags,
        "missing_lifecycle_fields": missing_fields,
        "required_fields": list(MARKET_LIFECYCLE_FIELDS),
        "settlement_trade_count": settlement_trade_count,
        "settlement_ledger_count": settlement_ledger_count,
        "refund_ledger_count": refund_ledger_count,
    }


def build_settlement_compatibility_report(
    run: Mapping[str, Any],
    parameters: Mapping[str, Any] | None,
    trades: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    contexts = _settlement_contexts(run, parameters)
    final_valuation_mode = str(_first_context_value(contexts, ("final_valuation_mode", "finalValuationMode")) or "UNKNOWN").upper()
    settlement_value = _first_context_value(contexts, ("settlement_value", "settlementValue"))
    fields = {
        "resolution_source": _first_context_value(contexts, ("resolution_source", "resolutionSource", "resolutionSourceName")),
        "settlement_rule": _first_context_value(contexts, ("settlement_rule", "settlementRule", "resolution_rule", "resolutionRule")),
        "price_to_beat_source": _first_context_value(contexts, ("price_to_beat_source", "priceToBeatSource", "price_to_beat", "priceToBeat")),
        "oracle_source": _first_context_value(contexts, ("oracle_source", "oracleSource", "oracle", "resolution_oracle", "resolutionOracle")),
    }
    lifecycle_report = build_market_lifecycle_report(run, parameters, trades, ledger)
    lifecycle_status = str(lifecycle_report["market_lifecycle_status"])
    lifecycle_flags = dict(lifecycle_report["lifecycle_flags"])
    settlement_trade_count = int(lifecycle_report["settlement_trade_count"])
    settlement_ledger_count = int(lifecycle_report["settlement_ledger_count"])
    refund_ledger_count = int(lifecycle_report["refund_ledger_count"])
    missing_fields = [key for key, value in fields.items() if _is_blank(value)]
    review_reasons: list[str] = []
    if settlement_trade_count > 0 and _is_blank(settlement_value):
        review_reasons.append("settlement trades exist but settlement_value is missing")
    if final_valuation_mode == "SETTLEMENT" and missing_fields:
        review_reasons.append("missing settlement source fields: " + ", ".join(missing_fields))
    if lifecycle_report["lifecycle_verdict"] != READY:
        review_reasons.append("market lifecycle review: " + str(lifecycle_report["reason"]))
    if settlement_trade_count and settlement_ledger_count == 0:
        review_reasons.append("settlement trades exist but ledger lacks SETTLEMENT event")
    compatibility_verdict = REVIEW if review_reasons else READY
    return {
        "status": READY,
        "compatibility_verdict": compatibility_verdict,
        "reason": "; ".join(review_reasons) if review_reasons else "settlement source and lifecycle fields are compatible with recorded payoff events",
        "final_valuation_mode": final_valuation_mode,
        "settlement_value": None if _is_blank(settlement_value) else str(settlement_value),
        "resolution_source": None if _is_blank(fields["resolution_source"]) else str(fields["resolution_source"]),
        "settlement_rule": None if _is_blank(fields["settlement_rule"]) else str(fields["settlement_rule"]),
        "price_to_beat_source": None if _is_blank(fields["price_to_beat_source"]) else str(fields["price_to_beat_source"]),
        "oracle_source": None if _is_blank(fields["oracle_source"]) else str(fields["oracle_source"]),
        "market_lifecycle_status": lifecycle_status,
        "lifecycle_flags": lifecycle_flags,
        "market_lifecycle_report": lifecycle_report,
        "missing_source_fields": missing_fields,
        "settlement_trade_count": settlement_trade_count,
        "settlement_ledger_count": settlement_ledger_count,
        "refund_ledger_count": refund_ledger_count,
    }


def _max_consecutive_losses(pnls: Sequence[Decimal]) -> int:
    longest = 0
    current = 0
    for value in pnls:
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _is_high_price_trade(row: Mapping[str, Any]) -> bool:
    for key in ("entry_price", "exit_price", "avg_fill_price"):
        value = _decimal(row.get(key))
        if value >= Decimal("0.95"):
            return True
    return False


def _trade_notional(row: Mapping[str, Any]) -> Decimal:
    for key in ("filled_notional", "notional", "requested_notional"):
        value = _decimal(row.get(key))
        if value > 0:
            return value
    price = _decimal(row.get("entry_price"))
    size = _decimal(row.get("size") or row.get("filled_size") or row.get("requested_size"))
    return (price * size).copy_abs() if price > 0 and size > 0 else Decimal("0")


def _payoff_distribution(pnls: Sequence[Decimal]) -> dict[str, Any]:
    sorted_values = sorted(pnls)
    count = len(sorted_values)
    if count == 0:
        return {}
    return {
        "min": _decimal_string(sorted_values[0]),
        "p10": _decimal_string(_percentile(sorted_values, Decimal("0.10"))),
        "p25": _decimal_string(_percentile(sorted_values, Decimal("0.25"))),
        "median": _decimal_string(_percentile(sorted_values, Decimal("0.50"))),
        "p75": _decimal_string(_percentile(sorted_values, Decimal("0.75"))),
        "p90": _decimal_string(_percentile(sorted_values, Decimal("0.90"))),
        "max": _decimal_string(sorted_values[-1]),
        "worst_decile_avg": _decimal_string(_tail_average(sorted_values, "left", Decimal("0.10"))),
        "best_decile_avg": _decimal_string(_tail_average(sorted_values, "right", Decimal("0.10"))),
    }


def _percentile(sorted_values: Sequence[Decimal], q: Decimal) -> Decimal:
    if not sorted_values:
        return Decimal("0")
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = q * Decimal(len(sorted_values) - 1)
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - Decimal(lower)
    return sorted_values[lower] * (Decimal("1") - weight) + sorted_values[upper] * weight


def _tail_average(sorted_values: Sequence[Decimal], side: str, fraction: Decimal) -> Decimal:
    if not sorted_values:
        return Decimal("0")
    count = max(1, int((Decimal(len(sorted_values)) * fraction).to_integral_value(rounding=ROUND_CEILING)))
    values = sorted_values[:count] if side == "left" else sorted_values[-count:]
    return sum(values, Decimal("0")) / Decimal(len(values))


def _ruin_risk(max_loss_abs: Decimal, capital: Decimal, gross_profit: Decimal) -> dict[str, Any]:
    if max_loss_abs <= 0:
        return {
            "status": READY,
            "reason": "no losing closed trades",
            "losses_to_ruin": None,
            "max_loss_pct_of_capital": "0",
            "gross_profit_buffer_loss_count": None,
        }
    if capital <= 0:
        return {
            "status": REVIEW,
            "reason": "initial capital missing; cannot compute losses_to_ruin",
            "losses_to_ruin": None,
            "max_loss_pct_of_capital": "0",
            "gross_profit_buffer_loss_count": _ceil_decimal(gross_profit / max_loss_abs) if gross_profit > 0 else 0,
        }
    losses_to_ruin = _ceil_decimal(capital / max_loss_abs)
    gross_profit_buffer_loss_count = _ceil_decimal(gross_profit / max_loss_abs) if gross_profit > 0 else 0
    status = REVIEW if losses_to_ruin <= max(TAIL_RISK_STRESS_LENGTHS) else READY
    reason = (
        f"{losses_to_ruin} max-loss trades can exhaust capital"
        if status == REVIEW
        else "ruin threshold is beyond configured stress lengths"
    )
    return {
        "status": status,
        "reason": reason,
        "losses_to_ruin": losses_to_ruin,
        "max_loss_pct_of_capital": _decimal_string(_pct_of_capital(max_loss_abs, capital)),
        "gross_profit_buffer_loss_count": gross_profit_buffer_loss_count,
    }


def _ceil_decimal(value: Decimal) -> int:
    return int(value.to_integral_value(rounding=ROUND_CEILING))


def _pct_of_capital(value: Decimal, capital: Decimal) -> Decimal:
    return (value / capital * Decimal("100")) if capital > 0 else Decimal("0")


def _regime_bucket(order: Mapping[str, Any], dimension: str) -> str:
    meta = _json_dict(order.get("meta"))
    contexts = [
        order,
        meta,
        _json_dict(meta.get("context")),
        _json_dict(meta.get("execution_context")),
        _json_dict(meta.get("executionContext")),
        _json_dict(meta.get("calibration_context")),
        _json_dict(meta.get("calibrationContext")),
        _json_dict(meta.get("market_metadata")),
        _json_dict(meta.get("marketMetadata")),
    ]
    if dimension == "final_minute":
        return _final_minute_bucket(contexts)
    if dimension == "event_outcome_count_bucket":
        return _event_outcome_count_bucket(contexts)
    aliases = {
        "market_category": ("market_category", "marketCategory", "category", "event_category", "eventCategory", "market_type", "marketType"),
        "role": ("role", "order_role", "orderRole"),
        "side": ("side", "token_side", "tokenSide"),
        "liquidity_bucket": ("liquidity_bucket", "liquidityBucket"),
        "volatility_bucket": ("volatility_bucket", "volatilityBucket"),
        "time_to_expiry_bucket": ("time_to_expiry_bucket", "timeToExpiryBucket"),
        "no_fill_reason": ("no_fill_reason", "noFillReason"),
    }.get(dimension, (dimension,))
    for context in contexts:
        for alias in aliases:
            value = context.get(alias) if isinstance(context, Mapping) else None
            if not _is_blank(value):
                return str(value).strip() or "unknown"
    return "unknown"


def _final_minute_bucket(contexts: Sequence[Mapping[str, Any]]) -> str:
    flag = _first_context_value(
        contexts,
        ("final_minute", "finalMinute", "is_final_minute", "isFinalMinute", "final_minute_flag", "finalMinuteFlag"),
    )
    if isinstance(flag, bool):
        return "final_minute" if flag else "not_final_minute"
    if flag is not None:
        text_flag = str(flag).strip().lower()
        if text_flag in {"1", "true", "yes", "y", "final", "final_minute"}:
            return "final_minute"
        if text_flag in {"0", "false", "no", "n", "not_final_minute"}:
            return "not_final_minute"
    seconds = _first_context_value(contexts, ("time_to_expiry_seconds", "timeToExpirySeconds", "seconds_to_expiry", "secondsToExpiry"))
    if not _is_blank(seconds):
        return "final_minute" if _decimal(seconds) <= Decimal("60") else "not_final_minute"
    days = _first_context_value(contexts, ("time_to_expiry_days", "timeToExpiryDays", "days_to_expiry", "daysToExpiry"))
    if not _is_blank(days):
        return "final_minute" if _decimal(days) * Decimal("86400") <= Decimal("60") else "not_final_minute"
    bucket = _first_context_value(contexts, ("time_to_expiry_bucket", "timeToExpiryBucket"))
    if not _is_blank(bucket):
        text = str(bucket).strip().lower()
        if text in {"final_minute", "lt_1m", "under_1m", "last_minute"}:
            return "final_minute"
        return "not_final_minute"
    return "unknown"


def _event_outcome_count_bucket(contexts: Sequence[Mapping[str, Any]]) -> str:
    value = _first_context_value(
        contexts,
        (
            "event_outcome_count",
            "eventOutcomeCount",
            "outcome_count",
            "outcomeCount",
            "member_count",
            "memberCount",
            "outcomes",
        ),
    )
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        count = len(value)
    else:
        count = _to_int(value)
    if count <= 0:
        return "unknown"
    if count == 1:
        return "single_outcome"
    if count == 2:
        return "binary"
    if count <= 5:
        return "small_multi_3_5"
    if count <= 20:
        return "medium_multi_6_20"
    return "large_multi_21_plus"


def _check_calibration_sources(inputs: Mapping[str, Any]) -> dict[str, str]:
    real_order = _to_int(inputs.get("real_order_state_event_count"))
    calibration = _to_int(inputs.get("calibration_count"))
    cost = _to_int(inputs.get("cost_calibration_count"))
    external = _to_int(inputs.get("external_source_state_count"))
    if real_order or calibration or cost or external:
        return _check(
            "live/shadow calibration evidence",
            READY,
            f"real_order_events={real_order} calibration={calibration} cost={cost} external_states={external}",
            "quant.real_order_state_events + calibration tables",
        )
    return _check(
        "live/shadow calibration evidence",
        REVIEW,
        "no real order/cost/external source evidence attached yet",
        "quant.real_order_state_events + quant.external_source_import_state",
    )


def _fetch_one(conn: Any, query: str, params: Sequence[Any]) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    return dict(row) if row else None


def _fetch_all(conn: Any, query: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def _fetch_all_if_table_exists(conn: Any, table: str, query: str, params: Sequence[Any]) -> list[dict[str, Any]]:
    if not _table_exists(conn, table):
        return []
    return _fetch_all(conn, query, params)


def _load_benchmark_artifact_rows(conn: Any, *, benchmark_id: int | None) -> list[dict[str, Any]]:
    if not benchmark_id:
        return []
    return _fetch_all_if_table_exists(
        conn,
        "quant.quant_backtest_benchmark_artifacts",
        """
        SELECT *
        FROM quant.quant_backtest_benchmark_artifacts
        WHERE benchmark_id = %s
        ORDER BY artifact_key ASC
        """,
        (int(benchmark_id),),
    )


def _load_benchmark_rows(conn: Any, *, benchmark_id: int | None) -> list[dict[str, Any]]:
    if not benchmark_id:
        return []
    return _fetch_all_if_table_exists(
        conn,
        "quant.quant_backtest_benchmark_rows",
        """
        SELECT *
        FROM quant.quant_backtest_benchmark_rows
        WHERE benchmark_id = %s
        ORDER BY row_index ASC
        LIMIT 10000
        """,
        (int(benchmark_id),),
    )


def _fetch_count(conn: Any, table: str, where_sql: str | None, params: Sequence[Any]) -> int:
    if not _table_exists(conn, table):
        return 0
    sql = f"SELECT COUNT(*) AS n FROM {table}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return int(row["n"] if isinstance(row, Mapping) else row[0])


def _table_exists(conn: Any, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (table,))
        row = cur.fetchone()
    return bool(row["exists"] if isinstance(row, Mapping) else row[0])


def _check(name: str, status: str, detail: str, evidence: str) -> dict[str, str]:
    return {"name": name, "status": status, "detail": detail, "evidence": evidence}


def _aggregate(statuses: Iterable[str]) -> str:
    values = set(statuses)
    if MISSING in values:
        return MISSING
    if REVIEW in values or UNKNOWN in values:
        return REVIEW
    return READY


def _build_run_credibility_assessment(
    run: Mapping[str, Any],
    data_quality: Mapping[str, Any],
    fill_quality: Mapping[str, Any],
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    """Score whether a persisted run is trustworthy enough for research reporting.

    This is deliberately stricter than the structural artifact checks. A run can be
    structurally complete but still need review if it lacks live/shadow evidence or
    has replay/data flags.
    """

    evidence = {
        "run_status": str(run.get("status") or "unknown"),
        "data_quality_status": str(data_quality.get("status") or "missing"),
        "raw_event_count": _to_int(fill_quality.get("raw_event_count")),
        "candidate_event_count": _to_int(fill_quality.get("candidate_event_count")),
        "consumed_event_count": _to_int(fill_quality.get("consumed_event_count")),
        "environment_flag_count": _to_int(fill_quality.get("environment_flag_count")),
        "order_anomaly_count": _to_int(fill_quality.get("order_anomaly_count")),
        "calibration_samples": _to_int(inputs.get("calibration_count")),
        "cost_calibration_samples": _to_int(inputs.get("cost_calibration_count")),
        "real_order_state_events": _to_int(inputs.get("real_order_state_event_count")),
        "external_source_states": _to_int(inputs.get("external_source_state_count")),
    }
    score = 0
    reasons: list[str] = []
    if evidence["run_status"] == "succeeded":
        score += CREDIBILITY_WEIGHTS["run_succeeded"]
    else:
        reasons.append(f"run status is {evidence['run_status']}")
    if evidence["data_quality_status"] == "ready":
        score += CREDIBILITY_WEIGHTS["data_quality_ready"]
    else:
        reasons.append(f"data quality is {evidence['data_quality_status']}")
    if fill_quality:
        score += CREDIBILITY_WEIGHTS["fill_quality_present"]
    else:
        reasons.append("missing fill quality artifact")
    if evidence["raw_event_count"] > 0 and evidence["candidate_event_count"] > 0:
        score += CREDIBILITY_WEIGHTS["raw_evidence_present"]
    else:
        reasons.append("raw OrderFilled evidence is empty")
    runtime_flags = evidence["environment_flag_count"] + evidence["order_anomaly_count"]
    if runtime_flags == 0:
        score += CREDIBILITY_WEIGHTS["low_runtime_flags"]
    else:
        reasons.append(f"runtime flags/anomalies={runtime_flags}")
    if evidence["calibration_samples"] > 0 or evidence["real_order_state_events"] > 0:
        score += CREDIBILITY_WEIGHTS["live_calibration_present"]
    else:
        reasons.append("no live/shadow fill calibration evidence")
    if evidence["cost_calibration_samples"] > 0:
        score += CREDIBILITY_WEIGHTS["cost_calibration_present"]
    else:
        reasons.append("no real cost calibration evidence")
    if evidence["external_source_states"] > 0:
        score += CREDIBILITY_WEIGHTS["external_source_state_present"]
    else:
        reasons.append("no external source import state")
    if not data_quality or not fill_quality or evidence["run_status"] not in {"succeeded", "running", "queued"}:
        status = MISSING
    elif score >= 85 and not reasons:
        status = READY
    else:
        status = REVIEW
    return {
        "status": status,
        "score": score,
        "max_score": sum(CREDIBILITY_WEIGHTS.values()),
        "reason": "; ".join(reasons) if reasons else "run has data, raw fill evidence, calibration, cost, and external source state",
        "evidence": evidence,
    }


def _next_actions(checks: Sequence[Mapping[str, str]], credibility: Mapping[str, Any] | None = None) -> list[str]:
    actions: list[str] = []
    for check in checks:
        if check.get("status") == MISSING:
            actions.append(f"Fix missing artifact: {check.get('name')} ({check.get('detail')})")
        elif check.get("status") in {REVIEW, UNKNOWN}:
            actions.append(f"Review artifact: {check.get('name')} ({check.get('detail')})")
    if credibility and credibility.get("status") in {REVIEW, MISSING, UNKNOWN}:
        actions.append(f"Review run credibility: {credibility.get('reason')}")
    if not actions:
        actions.append("Run artifact is complete enough for fill-first research reporting.")
    return actions


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


def _benchmark_id_from_context(inputs: Mapping[str, Any]) -> int | None:
    run = _json_dict(inputs.get("run"))
    parameters = _json_dict(inputs.get("parameters"))
    meta = _json_dict(run.get("meta"))
    contexts = [
        inputs,
        run,
        parameters,
        meta,
        _json_dict(meta.get("execution_context")),
        _json_dict(meta.get("artifact_context")),
        _json_dict(meta.get("benchmark")),
        _json_dict(meta.get("parameter_snapshot")),
    ]
    value = _first_context_value(contexts, ("benchmark_id", "benchmarkId", "source_benchmark_id", "sourceBenchmarkId"))
    parsed = _to_int(value)
    return parsed if parsed > 0 else None


def _execution_model_rows_from_benchmark_artifacts(
    artifact_rows: Sequence[Mapping[str, Any]],
    benchmark_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    payloads: dict[str, Any] = {
        str(row.get("artifact_key") or row.get("artifactKey") or ""): _json_value(row.get("payload"))
        for row in artifact_rows
    }
    for key in ("execution_model_rows", "executionModelRows"):
        rows = _list_of_dicts(payloads.get(key))
        if rows:
            return rows
    for key in ("summary", "execution_model_validation", "executionModelValidation"):
        payload = _json_dict(payloads.get(key))
        rows = _list_of_dicts(payload.get("execution_model_rows") or payload.get("executionModelRows") or payload.get("source_rows") or payload.get("sourceRows"))
        if rows:
            return rows
    profile_rows = _list_of_dicts(payloads.get("profiles"))
    rows = _execution_model_rows_from_profile_artifact(profile_rows)
    if rows:
        return rows
    return _execution_model_rows_from_benchmark_row_payloads(benchmark_rows)


def _execution_model_rows_from_profile_artifact(profile_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in profile_rows:
        model = _execution_model_from_row(row)
        if not model:
            continue
        trades = _to_int(row.get("trades") or row.get("filled_count") or row.get("filledCount"))
        slippage_total = _decimal(row.get("slippage_total") or row.get("slippageTotal"))
        output.append(
            {
                "source": "benchmark_profile_artifact",
                "key": row.get("key"),
                "execution_model": model,
                "replay_mode": row.get("replay_mode") or row.get("replayMode"),
                "execution_profile": row.get("execution_profile") or row.get("executionProfile"),
                "strategy_name": row.get("strategy_name") or row.get("strategyName") or "benchmark",
                "net_pnl": row.get("total_pnl") or row.get("totalPnl") or row.get("net_pnl") or row.get("netPnl"),
                "submitted_count": row.get("signal_count") or row.get("signalCount") or row.get("submitted_count") or row.get("submittedCount"),
                "filled_count": trades,
                "partial_fill_count": row.get("partial_fill_count") or row.get("partialFillCount") or 0,
                "unfilled_cancelled_count": row.get("no_fills") or row.get("noFills") or row.get("no_fill_count") or row.get("noFillCount"),
                "avg_slippage": _decimal_string(slippage_total / Decimal(trades) if trades else Decimal("0")),
                "queue_wait_seconds": row.get("queue_wait_seconds") or row.get("queueWaitSeconds") or 0,
                "market_category": row.get("market_category") or row.get("marketCategory") or "unknown",
                "liquidity_bucket": row.get("liquidity_bucket") or row.get("liquidityBucket") or "benchmark_bundle",
                "volatility_bucket": row.get("volatility_bucket") or row.get("volatilityBucket") or "not_split",
                "time_to_expiry_bucket": row.get("time_to_expiry_bucket") or row.get("timeToExpiryBucket") or "not_split",
                "final_minute": row.get("final_minute") or row.get("finalMinute") or "not_split",
                "event_outcome_count_bucket": row.get("event_outcome_count_bucket") or row.get("eventOutcomeCountBucket") or "benchmark_bundle",
            }
        )
    return output


def _execution_model_rows_from_benchmark_row_payloads(benchmark_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in benchmark_rows:
        payload = _json_dict(row.get("payload"))
        model = _execution_model_from_row(payload) or _execution_model_from_row(row)
        if not model:
            continue
        rows.append({**payload, **dict(row), "execution_model": model, "source": "benchmark_row_payload"})
    return rows


def _execution_model_from_row(row: Mapping[str, Any]) -> str:
    explicit = _first_non_blank(row.get("execution_model"), row.get("executionModel"), row.get("model"))
    if explicit:
        text = str(explicit).strip().lower().replace("-", "_").replace(" ", "_")
        if text in {"ohlcv_close", "formula_slippage", "l2_orderfilled"}:
            return text
        if text in {"ohlcv", "close", "block_close", "block_bar"}:
            return "ohlcv_close"
        if text in {"formula", "price_history", "frontend_price_history"}:
            return "formula_slippage"
        if text in {"l2", "depth", "lob", "orderfilled_lob", "accurate"}:
            return "l2_orderfilled"
    replay_mode = str(_first_non_blank(row.get("replay_mode"), row.get("replayMode")) or "").strip().lower()
    if replay_mode in {"fast", "ohlcv", "close", "block_close", "block_bar"}:
        return "ohlcv_close"
    if replay_mode in {"formula", "formula_slippage", "price_history", "frontend_price_history"}:
        return "formula_slippage"
    if replay_mode in {"accurate", "l2", "depth", "lob", "orderfilled_lob"}:
        return "l2_orderfilled"
    return ""


def _json_value(value: Any) -> Any:
    if isinstance(value, str) and value.strip():
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _int_dict(value: Any) -> dict[str, int]:
    raw = _json_dict(value)
    result: dict[str, int] = {}
    for key, item in raw.items():
        parsed = _to_int(item)
        if parsed:
            result[str(key)] = parsed
    return result


def _normalized_text(value: Any, *, fallback: str = "") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str) and value.strip():
        try:
            value = json.loads(value)
        except ValueError:
            return []
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray, str)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _first_context_value(contexts: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> Any:
    for context in contexts:
        if not isinstance(context, Mapping):
            continue
        for key in keys:
            value = context.get(key)
            if not _is_blank(value):
                return value
    return None


def _settlement_contexts(run: Mapping[str, Any], parameters: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    meta = _json_dict(run.get("meta"))
    snapshot = _parameter_snapshot_fields(meta)
    execution_context = _json_dict(meta.get("execution_context"))
    parameter_context = _json_dict(snapshot.get("execution_context"))
    return [
        execution_context,
        parameter_context,
        snapshot,
        dict(parameters or {}),
        _json_dict(meta.get("market_metadata")),
        _json_dict(meta.get("marketMetadata")),
        meta,
        run,
    ]


def _first_non_blank(*values: Any) -> Any:
    for value in values:
        if not _is_blank(value):
            return value
    return None


def _text_or_none(value: Any) -> str | None:
    if _is_blank(value):
        return None
    return str(value)


def _coverage_text(value: Any) -> str:
    if _is_blank(value):
        return ""
    return str(value)


def _pct_text(numerator: int | Decimal, denominator: int | Decimal) -> str:
    denom = Decimal(str(denominator or 0))
    if denom <= 0:
        return "0"
    value = Decimal(str(numerator or 0)) / denom * Decimal("100")
    return _decimal_string(value)


def _context_bool(contexts: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> bool:
    value = _first_context_value(contexts, keys)
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on", "closed", "archived", "refunded", "invalid", "cancelled", "canceled", "misconfigured"}


def _is_blank(value: Any) -> bool:
    return value is None or value == ""


def _to_int(value: Any) -> int:
    if isinstance(value, Decimal):
        return int(value)
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _decimal_string(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _average_decimal_string(values: Sequence[Decimal]) -> str | None:
    if not values:
        return None
    return _decimal_string(sum(values, Decimal("0")) / Decimal(len(values)))


def _pct_bucket(value: Decimal) -> str:
    pct = max(Decimal("0"), min(Decimal("100"), Decimal(str(value))))
    if pct <= 0:
        return "0"
    if pct < Decimal("25"):
        return "0_25"
    if pct < Decimal("50"):
        return "25_50"
    if pct < Decimal("75"):
        return "50_75"
    if pct < Decimal("100"):
        return "75_100"
    return "100"


def _bump(counter: dict[str, int], key: str) -> None:
    counter[key] = counter.get(key, 0) + 1
