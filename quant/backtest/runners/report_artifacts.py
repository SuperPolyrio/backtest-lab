"""Report artifacts for OrderFilled-first replay benchmarks."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal
from typing import Any

from quant.backtest.execution_model_validation import build_execution_model_validation_report
from quant.backtest.execution_profile_matrix import build_execution_profile_matrix_report
from quant.backtest.parameter_robustness import build_parameter_robustness_report
from quant.backtest.parameter_search_results import build_parameter_search_results_report
from quant.backtest.performance_score import build_performance_score_report
from quant.backtest.regime_coverage import build_regime_coverage_report


def build_benchmark_artifacts(
    *,
    reports: dict[str, Any],
    comparison_rows: list[Any],
    coverage: dict[str, Any],
    universe: dict[str, Any],
) -> dict[str, Any]:
    reference = reports.get("accurate:realistic") or reports.get("fast:realistic") or next(iter(reports.values()), None)
    trade_rows = list(getattr(reference, "trade_rows", []) if reference is not None else [])
    profile_rows = [_plain_profile(key, report) for key, report in reports.items()]
    execution_model_rows = _execution_model_rows_from_profiles(profile_rows, universe=universe)
    execution_model_validation = build_execution_model_validation_report(execution_model_rows)
    parameter_search_plan = build_benchmark_parameter_search_plan(profile_rows, universe=universe)
    parameter_search_results = build_parameter_search_results_report(
        parameter_search_plan,
        _parameter_result_rows_from_profiles(profile_rows, universe=universe),
    )
    fill_quality = build_fill_quality(reference)
    data_quality = build_data_quality(reference, comparison_rows=comparison_rows, coverage=coverage, universe=universe)
    prediction_quality = build_prediction_quality(trade_rows)
    return {
        "profiles": profile_rows,
        "per_market_rows": [_plain_trade(row) for row in trade_rows],
        "fast_accurate_rows": [_as_plain(row) for row in comparison_rows],
        "fill_quality": fill_quality,
        "data_quality": data_quality,
        "regime_buckets": build_regime_buckets(trade_rows),
        "regime_coverage": build_regime_coverage_report(trade_rows, universe=universe),
        "prediction_quality": prediction_quality,
        "performance_score": build_performance_score_report(
            trades=[_plain_trade(row) for row in trade_rows],
            ledger=[],
            fill_quality_report=fill_quality,
            data_quality_report=data_quality,
            prediction_quality_report=prediction_quality,
        ),
        "execution_profile_matrix": build_execution_profile_matrix_report(profile_rows),
        "execution_model_rows": execution_model_rows,
        "execution_model_validation": execution_model_validation,
        "parameter_robustness": build_parameter_robustness_report(
            [
                {
                    **profile,
                    "parameter_fingerprint": profile.get("key"),
                    "parameters": {"execution_profile": profile.get("execution_profile")},
                    "mode": "benchmark_profile",
                }
                for profile in profile_rows
            ],
            min_runs=3,
            min_parameter_sets=3,
        ),
        "parameter_search_plan": parameter_search_plan,
        "parameter_search_results": parameter_search_results,
    }


def _execution_model_rows_from_profiles(profile_rows: list[dict[str, Any]], *, universe: dict[str, Any]) -> list[dict[str, Any]]:
    category = universe.get("category") or universe.get("universe_type") or "unknown"
    output: list[dict[str, Any]] = []
    for row in profile_rows:
        replay_mode = str(row.get("replay_mode") or "").strip().lower()
        execution_model = _execution_model_for_replay_mode(replay_mode)
        if not execution_model:
            continue
        trades = int(row.get("trades") or 0)
        slippage_total = Decimal(str(row.get("slippage_total") or "0"))
        output.append(
            {
                "source": "benchmark_profile",
                "key": row.get("key"),
                "execution_model": execution_model,
                "replay_mode": replay_mode,
                "execution_profile": row.get("execution_profile"),
                "strategy_name": universe.get("strategy_name") or "favorite_hold_v1",
                "net_pnl": row.get("total_pnl"),
                "submitted_count": row.get("signal_count"),
                "filled_count": trades,
                "partial_fill_count": 0,
                "unfilled_cancelled_count": row.get("no_fills"),
                "avg_slippage": _decimal_text(slippage_total / Decimal(trades) if trades else Decimal("0")),
                "queue_wait_seconds": 0,
                "market_category": category,
                "liquidity_bucket": "benchmark_bundle",
                "volatility_bucket": "not_split",
                "time_to_expiry_bucket": "not_split",
                "final_minute": "not_split",
                "event_outcome_count_bucket": "benchmark_bundle",
            }
        )
    return output


def _execution_model_for_replay_mode(replay_mode: str) -> str:
    if replay_mode in {"fast", "ohlcv", "close", "block_close", "block_bar"}:
        return "ohlcv_close"
    if replay_mode in {"formula", "formula_slippage", "price_history", "frontend_price_history"}:
        return "formula_slippage"
    if replay_mode in {"accurate", "l2", "depth", "lob", "orderfilled_lob"}:
        return "l2_orderfilled"
    return ""


def build_benchmark_parameter_search_plan(profile_rows: list[dict[str, Any]], *, universe: dict[str, Any]) -> dict[str, Any]:
    """Represent benchmark profile rows as a coverage-auditable search plan."""

    plan_items = [
        {
            "key": f"benchmark_profile:{row.get('key') or index}",
            "parameter_index": index,
            "evidence_mode": "benchmark_profile",
            "parameter_fingerprint": str(row.get("key") or f"profile-{index}"),
            "parameters": {
                "execution_profile": row.get("execution_profile"),
                "replay_mode": row.get("replay_mode"),
            },
            "request_payload": {
                "universe": universe,
                "execution_profile": row.get("execution_profile"),
                "replay_mode": row.get("replay_mode"),
                "parameter_fingerprint": str(row.get("key") or f"profile-{index}"),
                "evidence_mode": "benchmark_profile",
            },
            "expected_robustness_row_fields": [
                "total_pnl",
                "fill_rate",
                "execution_profile",
            ],
        }
        for index, row in enumerate(profile_rows, start=1)
    ]
    return {
        "schema_version": "fill_first_benchmark_parameter_search_plan_v1",
        "status": "ready" if plan_items else "missing",
        "reason": (
            "benchmark profiles are represented as coverage-auditable parameter rows"
            if plan_items
            else "no benchmark profile rows available"
        ),
        "universe_name": universe.get("universe_name") or universe.get("name") or "benchmark",
        "parameter_fields": ["execution_profile", "replay_mode"],
        "parameter_set_count": len(plan_items),
        "candidate_run_count": len(plan_items),
        "planned_run_count": len(plan_items),
        "evidence_modes": ["benchmark_profile"] if plan_items else [],
        "execution_profiles": sorted({str(row.get("execution_profile")) for row in profile_rows if row.get("execution_profile")}),
        "required_execution_profiles": ["realistic", "conservative"],
        "plan_items": plan_items,
        "robustness_requirements": {
            "min_runs": 5,
            "min_parameter_sets": 3,
            "required_evidence": ["train/test split", "walk-forward batch"],
            "production_policy": "benchmark profile coverage alone cannot stage production parameters",
        },
        "next_actions": [
            "Use this benchmark coverage artifact as input evidence, then run train/test and walk-forward parameter searches before staging parameters.",
        ],
    }


def _parameter_result_rows_from_profiles(profile_rows: list[dict[str, Any]], *, universe: dict[str, Any]) -> list[dict[str, Any]]:
    category = universe.get("category") or universe.get("universe_type") or "unknown"
    rows: list[dict[str, Any]] = []
    for index, row in enumerate(profile_rows, start=1):
        fingerprint = str(row.get("key") or f"profile-{index}")
        rows.append(
            {
                **row,
                "run_id": fingerprint,
                "parameter_fingerprint": fingerprint,
                "parameters": {
                    "execution_profile": row.get("execution_profile"),
                    "replay_mode": row.get("replay_mode"),
                },
                "mode": "benchmark_profile",
                "market_category": category,
                "liquidity_bucket": "benchmark_bundle",
                "volatility_bucket": "not_split",
                "time_to_expiry_bucket": "not_split",
                "net_pnl": row.get("total_pnl"),
                "max_drawdown": "0",
                "fill_rate": row.get("fill_rate"),
                "sample_count": row.get("trades") or row.get("signal_count") or 0,
            }
        )
    return rows


def build_fill_quality(report: Any | None) -> dict[str, Any]:
    if report is None:
        return _empty_quality()
    rows = list(getattr(report, "trade_rows", []))
    signals = int(getattr(report, "signal_count", len(rows)) or len(rows))
    filled = [row for row in rows if Decimal(str(getattr(row, "filled_size", "0") or "0")) > 0]
    partial = [row for row in rows if str(getattr(row, "order_status", "")) == "PARTIAL_FILLED"]
    no_fill = [row for row in rows if Decimal(str(getattr(row, "filled_size", "0") or "0")) <= 0]
    no_fill_reasons: dict[str, int] = {}
    order_anomaly_flags: dict[str, int] = {}
    environment_flags: dict[str, int] = {}
    missed_notionals = [
        Decimal(str(getattr(row, "missed_opportunity_notional", "0") or "0"))
        for row in rows
        if Decimal(str(getattr(row, "missed_opportunity_notional", "0") or "0")) > 0
    ]
    missed_price_moves = [
        Decimal(str(getattr(row, "missed_opportunity_price_move", "0") or "0"))
        for row in rows
        if Decimal(str(getattr(row, "missed_opportunity_price_move", "0") or "0")) > 0
    ]
    for row in no_fill:
        reason = str(getattr(row, "order_status", "NO_FILL") or "NO_FILL")
        no_fill_reasons[reason] = no_fill_reasons.get(reason, 0) + 1
    for row in rows:
        status = str(getattr(row, "order_status", "") or "")
        filled_size = Decimal(str(getattr(row, "filled_size", "0") or "0"))
        raw_rows = int(getattr(row, "raw_rows_for_outcome", 0) or 0)
        if status in {"FILLED", "PARTIAL_FILLED"} and filled_size <= 0:
            order_anomaly_flags["filled_status_zero_size"] = order_anomaly_flags.get("filled_status_zero_size", 0) + 1
        if status in {"NO_FILL", "REJECTED", "EXPIRED", "CANCELED", "CANCEL_FAILED"} and filled_size > 0:
            order_anomaly_flags["non_fill_status_positive_size"] = order_anomaly_flags.get("non_fill_status_positive_size", 0) + 1
        if status in {"FILLED", "PARTIAL_FILLED"} and raw_rows <= 0:
            environment_flags["filled_without_raw_rows"] = environment_flags.get("filled_without_raw_rows", 0) + 1
    avg_fill_price = _avg([_entry_price(row) for row in filled])
    avg_slippage = _avg([abs(Decimal(str(getattr(row, "crossing_trade_price", "") or getattr(row, "limit_price", "0") or "0")) - Decimal(str(getattr(row, "limit_price", "0") or "0"))) for row in filled])
    return {
        "signal_count": signals,
        "submitted_count": len(rows),
        "filled_count": len(filled),
        "partial_fill_count": len(partial),
        "no_fill_count": len(no_fill),
        "fill_rate": _ratio(len(filled), signals),
        "partial_fill_rate": _ratio(len(partial), signals),
        "no_fill_rate": _ratio(len(no_fill), signals),
        "avg_fill_price": _decimal_text(avg_fill_price),
        "avg_slippage": _decimal_text(avg_slippage),
        "avg_latency_blocks": _decimal_text(_avg([Decimal(str(max(0, int(getattr(row, "fill_block", 0) or 0) - int(getattr(row, "signal_block", 0) or 0)))) for row in filled])),
        "avg_effective_latency_x_span": _decimal_text(_avg([Decimal(str(max(0, int(getattr(row, "fill_block", 0) or 0) - int(getattr(row, "signal_block", 0) or 0)))) for row in filled])),
        "max_effective_latency_x_span": _decimal_text(max([Decimal(str(max(0, int(getattr(row, "fill_block", 0) or 0) - int(getattr(row, "signal_block", 0) or 0)))) for row in filled], default=Decimal("0"))),
        "avg_effective_liquidity_cap_pct": "0",
        "avg_adverse_slippage_cents": "0",
        "avg_fill_probability_haircut_pct": "0",
        "avg_markout_after_1_bars": "0",
        "avg_markout_after_5_bars": "0",
        "avg_markout_after_20_bars": "0",
        "markout_sample_count": {},
        "avg_markout_after_60_seconds": "0",
        "avg_markout_after_300_seconds": "0",
        "avg_markout_after_1200_seconds": "0",
        "markout_seconds_sample_count": {},
        "adverse_selection_buckets": {},
        "adverse_selection_count": 0,
        "candidate_event_unique_count": 0,
        "candidate_event_duplicate_count": 0,
        "consumed_event_unique_count": 0,
        "consumed_event_duplicate_count": 0,
        "counterparty_tag_rate": "0",
        "no_fill_reasons": no_fill_reasons,
        "order_anomaly_flags": order_anomaly_flags,
        "order_anomaly_count": sum(order_anomaly_flags.values()),
        "anomaly_order_count": sum(order_anomaly_flags.values()),
        "real_order_state_observed_count": 0,
        "real_order_state_counts": {},
        "real_order_state_flags": {},
        "real_order_state_flag_count": 0,
        "avg_order_submit_accept_latency_seconds": "0",
        "avg_cancel_accept_latency_seconds": "0",
        "environment_flags": environment_flags,
        "environment_flag_count": sum(environment_flags.values()),
        "order_type_counts": {},
        "time_in_force_counts": {},
        "missed_opportunity_count": len(missed_notionals),
        "missed_opportunity_notional_total": _decimal_text(sum(missed_notionals, Decimal("0"))),
        "avg_missed_opportunity_notional": _decimal_text(_avg(missed_notionals)),
        "max_missed_opportunity_notional": _decimal_text(max(missed_notionals) if missed_notionals else Decimal("0")),
        "avg_missed_opportunity_price_move": _decimal_text(_avg(missed_price_moves)),
        "missed_opportunity_by_reason": {},
        "missed_opportunity_buckets": {},
        "missed_opportunity_notional_by_bucket": {},
    }


def build_data_quality(report: Any | None, *, comparison_rows: list[Any], coverage: dict[str, Any], universe: dict[str, Any]) -> dict[str, Any]:
    return {
        "universe": universe,
        "source_table": getattr(report, "replay_table", None) or "orderfilled_fact/orderfilled_block_replay",
        "raw_market_count": int(getattr(report, "raw_market_count", 0) or 0) if report is not None else 0,
        "raw_rows": int(sum(int(getattr(row, "raw_rows_for_outcome", 0) or 0) for row in getattr(report, "trade_rows", []))) if report is not None else 0,
        "coverage": coverage,
        "status_mismatch_count": sum(1 for row in comparison_rows if getattr(row, "fast_status", None) != getattr(row, "accurate_status", None)),
        "pnl_drift_count": sum(1 for row in comparison_rows if Decimal(str(getattr(row, "pnl_diff", "0") or "0")) != 0),
        "gap_status": "not_measured",
        "stale_status": "not_measured",
    }


def build_regime_buckets(rows: list[Any]) -> dict[str, Any]:
    return {
        "price_bucket": _bucket_counts(rows, lambda row: _price_bucket(Decimal(str(getattr(row, "signal_probability", "0") or "0")))),
        "liquidity_bucket": _bucket_counts(rows, lambda row: _liquidity_bucket(int(getattr(row, "raw_rows_for_outcome", 0) or 0))),
        "drift_bucket": _bucket_counts(rows, lambda row: _drift_bucket(Decimal(str(getattr(row, "snapshot_drift", "0") or "0")))),
        "settlement_bucket": _bucket_counts(rows, lambda row: "unresolved" if str(getattr(row, "order_status", "")) == "UNRESOLVED" else ("resolved_win" if Decimal(str(getattr(row, "payoff_per_share", "0") or "0")) > 0 else "resolved_loss")),
    }


def build_prediction_quality(rows: list[Any]) -> dict[str, Any]:
    scored = []
    baseline = []
    buckets: dict[str, dict[str, Decimal | int]] = {}
    for row in rows:
        if str(getattr(row, "order_status", "")) == "UNRESOLVED":
            continue
        probability = Decimal(str(getattr(row, "signal_probability", "0") or "0"))
        actual = Decimal("1") if Decimal(str(getattr(row, "payoff_per_share", "0") or "0")) > 0 else Decimal("0")
        close_line = Decimal(str(getattr(row, "close_line_probability", probability) or probability))
        scored.append((probability - actual) ** 2)
        baseline.append((close_line - actual) ** 2)
        bucket = _price_bucket(probability)
        state = buckets.setdefault(bucket, {"count": 0, "predicted_sum": Decimal("0"), "actual_sum": Decimal("0"), "brier_sum": Decimal("0")})
        state["count"] = int(state["count"]) + 1
        state["predicted_sum"] = Decimal(state["predicted_sum"]) + probability
        state["actual_sum"] = Decimal(state["actual_sum"]) + actual
        state["brier_sum"] = Decimal(state["brier_sum"]) + scored[-1]
    brier = _avg(scored)
    baseline_brier = _avg(baseline)
    return {
        "sample_count": len(scored),
        "brier_score": _decimal_text(brier),
        "market_brier_score": _decimal_text(baseline_brier),
        "brier_advantage": _decimal_text(baseline_brier - brier),
        "calibration_buckets": [
            {
                "bucket": bucket,
                "count": int(values["count"]),
                "avg_predicted": _decimal_text(Decimal(values["predicted_sum"]) / Decimal(int(values["count"]))),
                "actual_rate": _decimal_text(Decimal(values["actual_sum"]) / Decimal(int(values["count"]))),
                "brier_score": _decimal_text(Decimal(values["brier_sum"]) / Decimal(int(values["count"]))),
            }
            for bucket, values in sorted(buckets.items())
            if int(values["count"]) > 0
        ],
        "avg_snapshot_drift": _decimal_text(_avg([Decimal(str(getattr(row, "snapshot_drift", "0") or "0")) for row in rows])),
        "avg_close_line_drift": _decimal_text(_avg([Decimal(str(getattr(row, "close_line_edge", "0") or "0")) for row in rows])),
    }


def _empty_quality() -> dict[str, Any]:
    return {
        "signal_count": 0,
        "submitted_count": 0,
        "filled_count": 0,
        "partial_fill_count": 0,
        "no_fill_count": 0,
        "fill_rate": "0",
        "partial_fill_rate": "0",
        "no_fill_rate": "0",
        "avg_effective_latency_x_span": "0",
        "max_effective_latency_x_span": "0",
        "avg_effective_liquidity_cap_pct": "0",
        "avg_adverse_slippage_cents": "0",
        "avg_fill_probability_haircut_pct": "0",
        "real_order_state_observed_count": 0,
        "real_order_state_counts": {},
        "real_order_state_flags": {},
        "real_order_state_flag_count": 0,
        "avg_order_submit_accept_latency_seconds": "0",
        "avg_cancel_accept_latency_seconds": "0",
        "avg_markout_after_1_bars": "0",
        "avg_markout_after_5_bars": "0",
        "avg_markout_after_20_bars": "0",
        "markout_sample_count": {},
        "avg_markout_after_60_seconds": "0",
        "avg_markout_after_300_seconds": "0",
        "avg_markout_after_1200_seconds": "0",
        "markout_seconds_sample_count": {},
        "adverse_selection_buckets": {},
        "adverse_selection_count": 0,
        "candidate_event_unique_count": 0,
        "candidate_event_duplicate_count": 0,
        "consumed_event_unique_count": 0,
        "consumed_event_duplicate_count": 0,
        "counterparty_tag_rate": "0",
        "order_type_counts": {},
        "time_in_force_counts": {},
        "missed_opportunity_count": 0,
        "missed_opportunity_notional_total": "0",
        "avg_missed_opportunity_notional": "0",
        "max_missed_opportunity_notional": "0",
        "avg_missed_opportunity_price_move": "0",
        "missed_opportunity_by_reason": {},
        "missed_opportunity_buckets": {},
        "missed_opportunity_notional_by_bucket": {},
    }


def _plain_profile(key: str, report: Any) -> dict[str, Any]:
    signal_count = int(getattr(report, "signal_count", 0) or 0)
    trades = int(getattr(report, "trades", 0) or 0)
    replay_mode, _, _profile_from_key = str(key).partition(":")
    return {
        "key": key,
        "replay_mode": replay_mode or "benchmark",
        "execution_profile": getattr(report, "execution_profile", ""),
        "signal_count": signal_count,
        "trades": trades,
        "no_fills": int(getattr(report, "no_fills", 0) or 0),
        "fill_rate": _ratio(trades, signal_count),
        "total_pnl": str(getattr(report, "total_pnl", "0")),
        "settlement_pnl": str(getattr(report, "settlement_pnl", "0")),
        "trade_exit_pnl": str(getattr(report, "trade_exit_pnl", "0")),
        "fee_total": str(getattr(report, "fee_total", "0")),
        "slippage_total": str(getattr(report, "slippage_total", "0")),
        "db_query_sec": float(getattr(report, "db_query_sec", 0.0) or 0.0),
        "engine_sec": float(getattr(report, "engine_sec", 0.0) or 0.0),
    }


def _plain_trade(row: Any) -> dict[str, Any]:
    return _as_plain(row)


def _as_plain(value: Any) -> Any:
    if is_dataclass(value):
        return _as_plain(asdict(value))
    if isinstance(value, dict):
        return {str(key): _as_plain(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_as_plain(item) for item in value]
    if isinstance(value, tuple):
        return [_as_plain(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    return value


def _entry_price(row: Any) -> Decimal:
    filled_size = Decimal(str(getattr(row, "filled_size", "0") or "0"))
    buy_cost = Decimal(str(getattr(row, "buy_cost", "0") or "0"))
    return buy_cost / filled_size if filled_size else Decimal("0")


def _avg(values: list[Decimal]) -> Decimal:
    return sum(values, Decimal("0")) / Decimal(len(values)) if values else Decimal("0")


def _ratio(numerator: int, denominator: int) -> str:
    return _decimal_text(Decimal(numerator) / Decimal(denominator) if denominator else Decimal("0"))


def _decimal_text(value: Decimal | int | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return format(decimal_value.normalize(), "f")


def _bucket_counts(rows: list[Any], bucket_fn) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Decimal | int]] = {}
    for row in rows:
        bucket = str(bucket_fn(row))
        state = buckets.setdefault(bucket, {"count": 0, "pnl": Decimal("0")})
        state["count"] = int(state["count"]) + 1
        state["pnl"] = Decimal(state["pnl"]) + Decimal(str(getattr(row, "pnl", "0") or "0"))
    return [{"bucket": key, "count": int(value["count"]), "pnl": _decimal_text(Decimal(value["pnl"]))} for key, value in sorted(buckets.items())]


def _price_bucket(value: Decimal) -> str:
    if value < Decimal("0.2"):
        return "00_20"
    if value < Decimal("0.4"):
        return "20_40"
    if value < Decimal("0.6"):
        return "40_60"
    if value < Decimal("0.8"):
        return "60_80"
    return "80_100"


def _liquidity_bucket(rows: int) -> str:
    if rows < 10:
        return "thin"
    if rows < 100:
        return "medium"
    return "active"


def _drift_bucket(value: Decimal) -> str:
    if value <= Decimal("-0.05"):
        return "drift_down_5c"
    if value < Decimal("0"):
        return "drift_down"
    if value >= Decimal("0.05"):
        return "drift_up_5c"
    if value > Decimal("0"):
        return "drift_up"
    return "flat"
