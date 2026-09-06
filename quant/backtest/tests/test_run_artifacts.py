from decimal import Decimal

from quant.backtest.run_artifacts import (
    MISSING,
    READY,
    REVIEW,
    backtest_run_artifact_report_to_markdown,
    build_environment_incident_report,
    build_external_signal_contract_report,
    build_event_level_risk_report,
    build_execution_ledger_parity_report,
    build_maker_taker_execution_report,
    build_maker_queue_uncertainty_report,
    build_latency_profile_report,
    build_slippage_regime_report,
    build_execution_regime_report,
    build_execution_semantics_report,
    build_fill_probability_evidence_report,
    build_ledger_cashflow_validation_report,
    build_backtest_run_artifact_report,
    build_backtest_run_artifact_summary,
    load_backtest_run_artifact_summary,
    build_historical_l2_alignment_report,
    build_market_lifecycle_report,
    build_materialized_cache_report,
    build_raw_orderfilled_replay_contract_report,
    build_prediction_quality_report,
    build_reproducibility_report,
    build_run_data_quality_report,
    build_settlement_compatibility_report,
    build_tail_risk_report,
    _benchmark_id_from_context,
    _execution_model_rows_from_benchmark_artifacts,
    load_latest_fill_first_backtest_run_id,
)


class OneRowCursor:
    def __init__(self, row):
        self.row = row
        self.query = ""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        self.query = query

    def fetchone(self):
        return self.row


class OneRowConn:
    def __init__(self, row):
        self.cursor_obj = OneRowCursor(row)

    def cursor(self):
        return self.cursor_obj


def test_load_latest_fill_first_backtest_run_id_filters_execution_modes() -> None:
    conn = OneRowConn({"run_id": 123})

    assert load_latest_fill_first_backtest_run_id(conn) == 123
    assert "execution_price_mode" in conn.cursor_obj.query
    assert "ORDERFILLED_CROSS" in conn.cursor_obj.query
    assert "ORDERFILLED_V2_TAPE" in conn.cursor_obj.query
    assert "ORDERFILLED_V3_TRADE" in conn.cursor_obj.query
    assert "PREDICTION_L2_REPLAY_V1" in conn.cursor_obj.query


def test_load_latest_fill_first_backtest_run_id_returns_none_when_absent() -> None:
    assert load_latest_fill_first_backtest_run_id(OneRowConn(None)) is None


def test_bounded_artifact_summary_reports_persistence_without_claiming_deep_audit() -> None:
    report = build_backtest_run_artifact_summary(
        {
            "run_id": 115,
            "status": "succeeded",
            "rows_processed": 86_586,
            "metric_count": 12,
            "equity_count": 86_586,
            "trade_count": 13,
            "order_count": 66_409,
            "timestamped_order_count": 66_409,
            "ledger_count": 14,
            "timestamped_ledger_count": 14,
            "event_count": 66_410,
            "timestamped_event_count": 66_410,
            "execution_price_mode": "ORDERFILLED",
        }
    )

    assert report["status"] == REVIEW
    assert report["summary_status"] == READY
    assert report["deep_audit_status"] == "unknown"
    assert report["artifacts"]["source_timestamp_coverage_pct"] == "100.00"
    assert report["artifacts"]["orders"] == 66_409
    assert report["reason"] == "bounded_database_summary"


def test_bounded_artifact_summary_does_not_scan_or_infer_timestamp_coverage() -> None:
    report = build_backtest_run_artifact_summary(
        {
            "run_id": 116,
            "status": "succeeded",
            "rows_processed": 100,
            "metric_count": 8,
            "equity_count": 100,
            "trade_count": 1,
            "order_count": 2,
            "ledger_count": 2,
            "event_count": 4,
        }
    )

    assert report["summary_status"] == REVIEW
    assert report["artifacts"]["timestamped_rows"] is None
    assert report["artifacts"]["source_timestamp_coverage_pct"] is None
    assert report["checks"][-1]["status"] == "unknown"


def test_bounded_artifact_summary_prefers_persisted_read_model() -> None:
    cached = {
        "run_id": 120,
        "status": "review",
        "summary_status": "ready",
        "audit_mode": "summary",
    }
    conn = OneRowConn({"artifact_summary": cached})

    assert load_backtest_run_artifact_summary(conn, run_id=120) == cached
    assert "quant_backtest_run_progress" in conn.cursor_obj.query
    assert "metric_counts" not in conn.cursor_obj.query


def test_benchmark_id_from_context_reads_run_meta() -> None:
    assert _benchmark_id_from_context({"run": {"meta": {"benchmark": {"benchmarkId": 17}}}}) == 17
    assert _benchmark_id_from_context({"run": {"meta": {"execution_context": {"source_benchmark_id": "18"}}}}) == 18
    assert _benchmark_id_from_context({"run": {"meta": {}}, "parameters": {"benchmark_id": 19}}) == 19


def test_execution_model_rows_from_benchmark_artifacts_prefers_explicit_rows() -> None:
    rows = _execution_model_rows_from_benchmark_artifacts(
        [
            {
                "artifact_key": "execution_model_rows",
                "payload": [
                    {
                        "execution_model": "formula_slippage",
                        "execution_profile": "realistic",
                        "strategy_name": "fixture",
                        "net_pnl": "1",
                        "submitted_count": 2,
                        "filled_count": 1,
                    }
                ],
            },
            {
                "artifact_key": "profiles",
                "payload": [{"replay_mode": "fast", "execution_profile": "realistic", "trades": 2}],
            },
        ],
        [],
    )

    assert len(rows) == 1
    assert rows[0]["execution_model"] == "formula_slippage"
    assert rows[0]["strategy_name"] == "fixture"


def test_execution_model_rows_from_benchmark_profiles_maps_fast_and_accurate() -> None:
    rows = _execution_model_rows_from_benchmark_artifacts(
        [
            {
                "artifact_key": "profiles",
                "payload": [
                    {
                        "key": "fast:realistic",
                        "replay_mode": "fast",
                        "execution_profile": "realistic",
                        "signal_count": 4,
                        "trades": 4,
                        "no_fills": 0,
                        "total_pnl": "4",
                        "slippage_total": "0",
                        "market_category": "sports",
                    },
                    {
                        "key": "accurate:realistic",
                        "replay_mode": "accurate",
                        "execution_profile": "realistic",
                        "signal_count": 4,
                        "trades": 2,
                        "no_fills": 2,
                        "total_pnl": "1",
                        "slippage_total": "0.04",
                        "market_category": "sports",
                    },
                ],
            }
        ],
        [],
    )

    assert {row["execution_model"] for row in rows} == {"ohlcv_close", "l2_orderfilled"}
    accurate = next(row for row in rows if row["execution_model"] == "l2_orderfilled")
    assert accurate["submitted_count"] == 4
    assert accurate["filled_count"] == 2
    assert accurate["unfilled_cancelled_count"] == 2
    assert accurate["avg_slippage"] == "0.02"


def complete_inputs() -> dict:
    fill_quality = {
        "signal_count": 2,
        "submitted_count": 2,
        "filled_count": 1,
        "partial_fill_count": 0,
        "no_fill_count": 1,
        "no_fill_reasons": {"NO_LIQUIDITY": 1},
        "expected_fill_size": "25",
        "actual_fill_size": "25",
        "expected_fill_notional": "10",
        "actual_fill_notional": "10",
        "avg_participation_rate": "25",
        "raw_event_count": 10,
        "loaded_raw_event_count": 10,
        "deduped_raw_event_count": 10,
        "raw_duplicate_event_count": 0,
        "raw_canonical_event_count": 10,
        "raw_fallback_key_event_count": 0,
        "raw_unknown_key_event_count": 0,
        "raw_canonical_fill_key_coverage_pct": "100",
        "raw_maker_taker_side_coverage_pct": "100",
        "raw_block_context_coverage_pct": "100",
        "raw_trade_tick_count": 10,
        "raw_block_count": 2,
        "candidate_event_count": 3,
        "consumed_event_count": 1,
        "execution_evidence_counts": {"raw_orderfilled": 1, "none": 1},
        "fill_evidence_counts": {"raw_orderfilled": 1},
        "raw_orderfilled_fill_count": 1,
        "block_bar_synthetic_fill_count": 0,
        "raw_replay_fallback_suppressed_count": 1,
        "raw_replay_synthetic_cross_no_fill_count": 1,
        "block_participation_discount_tick_count": 0,
        "max_requested_block_participation_pct": "0",
        "min_block_participation_factor": "1",
        "raw_replay_coverage_pct": "100",
        "block_bar_fallback_pct": "0",
        "raw_evidence_summary": {
            "canonical_key_fields": ["tx_hash", "log_index", "market_id", "condition_id", "token_id", "maker", "taker", "side"],
            "candidate_event_unique_count": 3,
            "candidate_event_duplicate_count": 0,
            "consumed_event_unique_count": 1,
            "consumed_event_duplicate_count": 0,
            "candidate_unique_notional": "1.5",
            "consumed_unique_notional": "0.5",
            "raw_trade_tick_report": {
                "status": "ready",
                "trade_tick_count": 10,
                "block_count": 2,
                "canonical_fill_key_coverage_pct": "100",
                "maker_taker_side_coverage_pct": "100",
                "block_context_coverage_pct": "100",
                "blocks": [
                    {
                        "block_number": 100,
                        "trade_tick_count": 2,
                        "high": "0.61",
                        "low": "0.49",
                        "vwap": "0.55",
                        "order_sequence": [{"tx_hash": "0xraw-buy"}, {"tx_hash": "0xraw-sell"}],
                    },
                    {
                        "block_number": 101,
                        "trade_tick_count": 8,
                        "high": "0.62",
                        "low": "0.5",
                        "vwap": "0.57",
                        "order_sequence": [{"tx_hash": "0xraw-late"}],
                    },
                ],
            },
            "raw_orderfilled_events": [
                {
                    "block_number": 11,
                    "transaction_index": 0,
                    "log_index": 1,
                    "tx_hash": "0xraw-a",
                    "market_id": "123",
                    "token_id": "0xabc",
                    "maker": "0xmaker",
                    "taker": "0xtaker",
                    "side": "BUY",
                    "trade_price": "0.40",
                    "size": "10",
                },
                {
                    "block_number": 12,
                    "transaction_index": 0,
                    "log_index": 2,
                    "tx_hash": "0xraw-b",
                    "market_id": "123",
                    "token_id": "0xabc",
                    "maker": "0xmaker2",
                    "taker": "0xtaker2",
                    "side": "SELL",
                    "trade_price": "0.42",
                    "size": "15",
                },
            ],
        },
        "loaded_block_window": {
            "from_block": 10,
            "to_block": 110,
            "loaded_first_block": 10,
            "loaded_last_block": 110,
            "replay_first_block": 10,
            "replay_last_block": 110,
            "market_id": 123,
            "token_id_hex": "0xabc",
            "limit": 5000,
            "loaded_event_count": 10,
            "replay_event_count": 10,
            "duplicate_event_count": 0,
            "hit_limit": False,
        },
        "environment_flags": {},
        "order_anomaly_flags": {},
        "avg_markout_after_1_bars": "0.01",
    }
    data_quality = {
        "status": "ready",
        "data_version": "abc123",
        "source_table": "quant.market_token_block_close",
        "access_path": "token_id_block_range",
        "x_axis": "block_number",
        "rows": 100,
        "first_x": 10,
        "last_x": 110,
        "median_delta": 1,
        "gap_count": 0,
        "gap_threshold": 4,
        "largest_gaps": [],
        "jump_count": 0,
        "largest_jumps": [],
        "requested_from": 10,
        "requested_to": 110,
        "span_coverage_pct": "100",
        "warning_level": "OK",
        "caveats": [],
        "orderfilled_replay": {
            "source": "orderfilled_fact",
            "fallback": None,
            "from_block": 10,
            "to_block": 110,
            "market_id": 123,
            "token_id": "0xabc",
            "limit": 5000,
            "loaded_event_count": 10,
            "deduped_event_count": 10,
            "duplicate_event_count": 0,
            "canonical_event_count": 10,
            "fallback_key_event_count": 0,
            "unknown_key_event_count": 0,
            "raw_trade_tick_count": 10,
            "raw_block_count": 2,
            "raw_canonical_fill_key_coverage_pct": "100",
            "raw_maker_taker_side_coverage_pct": "100",
            "loaded_block_window": {
                "from_block": 10,
                "to_block": 110,
                "loaded_first_block": 10,
                "loaded_last_block": 110,
                "replay_first_block": 10,
                "replay_last_block": 110,
                "market_id": 123,
                "token_id_hex": "0xabc",
                "limit": 5000,
                "loaded_event_count": 10,
                "replay_event_count": 10,
                "duplicate_event_count": 0,
                "hit_limit": False,
            },
            "raw_trade_tick_report": {
                "status": "ready",
                "trade_tick_count": 10,
                "block_count": 2,
                "canonical_fill_key_coverage_pct": "100",
                "maker_taker_side_coverage_pct": "100",
                "block_context_coverage_pct": "100",
                "blocks": [
                    {
                        "block_number": 100,
                        "trade_tick_count": 2,
                        "high": "0.61",
                        "low": "0.49",
                        "vwap": "0.55",
                        "order_sequence": [{"tx_hash": "0xraw-buy"}, {"tx_hash": "0xraw-sell"}],
                    },
                    {
                        "block_number": 101,
                        "trade_tick_count": 8,
                        "high": "0.62",
                        "low": "0.5",
                        "vwap": "0.57",
                        "order_sequence": [{"tx_hash": "0xraw-late"}],
                    },
                ],
            },
        },
        "pmxt_l2_alignment": {
            "status": "ready",
            "pmxt_rows_seen": 4,
            "pmxt_matched_events": 4,
            "pmxt_applied_events": 4,
            "orderfilled_rows_seen": 2,
            "orderfilled_rows_matched": 2,
            "aligned_count": 2,
            "alignment_pct": "100",
            "max_lag_ms": 60000,
            "missing_timestamp_count": 0,
            "missing_l2_before_fill_count": 0,
            "stale_l2_count": 0,
            "price_outside_spread_count": 0,
            "price_unchecked_count": 0,
            "depth_checked_count": 2,
            "depth_sufficient_count": 2,
            "depth_insufficient_count": 0,
            "crossable_depth_sufficient_count": 2,
            "depth_sufficient_pct": "100",
            "crossable_depth_sufficient_pct": "100",
            "sample_rows": [{"tx_hash": "0xraw-buy", "status": "aligned"}],
        },
        "fill_quality": fill_quality,
    }
    parameter_snapshot = {
        "entry_threshold": "0.6",
        "exit_threshold": "0.5",
        "initial_capital": "1000",
        "position_size": "10",
        "execution_price_mode": "ORDERFILLED_LIMIT_REPLAY",
        "execution_profile": "realistic",
        "order_role": "taker",
        "latency_blocks": 1,
        "latency_seconds": "0",
        "allow_partial_fill": True,
        "final_valuation_mode": "SETTLEMENT",
        "settlement_value": "1",
        "resolution_source": "polymarket",
        "settlement_rule": "official result",
        "price_to_beat_source": "not_applicable",
        "oracle_source": "uma",
        "market_lifecycle_status": "closed",
        "resolved_outcome": "YES",
        "end_date": "2026-06-22T12:00:00Z",
    }
    return {
        "run": {
            "run_id": 7,
            "status": "succeeded",
            "market_slug": "demo-market",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "backtest_engine": "builtin",
            "from_block": 10,
            "to_block": 110,
            "rows_processed": 100,
            "meta": {
                "strategy_name": "fixed_threshold",
                "strategy_version": "fixed_threshold_v1",
                "artifact_schema_version": "fill_first_run_artifact_v1",
                "code_commit": "abcdeffedcba",
                "code_dirty": False,
                "code_source": "git",
                "model_versions": {
                    "fill_model": "orderfilled_limit_cross_then_settlement",
                    "fill_model_version": "orderfilled_limit_cross_v1",
                    "fee_model_version": "maker_taker_rebate_v1",
                    "slippage_model_version": "adverse_slippage_bps_cents_v1",
                },
                "parameter_fingerprint": "fp1",
                "parameter_snapshot": parameter_snapshot,
                "event_slug": "demo-event",
                "event_outcome_count": 3,
                "outcome_correlation": {
                    "status": "ready",
                    "method": "block_return",
                    "sample_count": 100,
                    "max_abs_correlation": "0.42",
                    "matrix": {"demo-market": {"demo-market": 1}},
                },
                "event_outcomes": [
                    {"market_slug": "demo-market", "outcome_label": "Alpha", "yes_probability": "0.40", "no_probability": "0.60"},
                    {"market_slug": "demo-market-b", "outcome_label": "Beta", "yes_probability": "0.35", "no_probability": "0.65"},
                    {"market_slug": "demo-market-c", "outcome_label": "Gamma", "yes_probability": "0.25", "no_probability": "0.75"},
                ],
                "actual_data_quality": data_quality,
            },
        },
        "parameters": parameter_snapshot,
        "metrics": [
            {"metric_key": "data_quality_status"},
            {"metric_key": "gap_count"},
            {"metric_key": "data_version"},
            {"metric_key": "fill_quality_fill_rate"},
            {"metric_key": "fill_quality_no_fill_rate"},
        ],
        "orders": [
            {
                "order_id": "o1",
                "signal_index": 1,
                "status": "FILLED",
                "side": "BUY_YES",
                "role": "maker",
                "order_type": "post_only_limit",
                "x_axis": "block_number",
                "signal_x": 10,
                "submit_x": 11,
                "decision_price": "0.40",
                "requested_price": "0.40",
                "requested_size": "25",
                "requested_notional": "10",
                "expected_fill_size": "25",
                "expected_fill_notional": "10",
                "actual_fill_size": "25",
                "actual_fill_notional": "10",
                "filled_size": "25",
                "filled_notional": "10",
                "unfilled_size": "0",
                "fill_probability": "100",
                "fill_pct": "100",
                "block_volume": "25",
                "trade_count": 3,
                "available_notional": "20",
                "participation_rate": "50",
                "latency_blocks": 1,
                "latency_seconds": "0",
                "execution_source": "orderfilled_limit_replay",
                "meta": {
                    "time_in_force": "GTC",
                    "strategy_intent": {"role": "maker", "order_type": "post_only_limit", "time_in_force": "GTC"},
                    "effective_liquidity_cap_pct": "80",
                    "fill_probability_haircut_pct": "20",
                    "raw_fill_probability": "100",
                    "effective_fill_probability": "80",
                    "context": {"market_category": "sports", "liquidity_bucket": "medium", "volatility_bucket": "low", "time_to_expiry_bucket": "lt_7d", "time_to_expiry_seconds": 3600, "event_outcome_count": 60},
                },
            },
            {
                "order_id": "o2",
                "signal_index": 2,
                "status": "NO_FILL",
                "side": "BUY_YES",
                "role": "taker",
                "order_type": "marketable_limit",
                "x_axis": "block_number",
                "signal_x": 20,
                "submit_x": 21,
                "decision_price": "0.50",
                "requested_price": "0.50",
                "requested_size": "10",
                "requested_notional": "5",
                "expected_fill_size": "0",
                "expected_fill_notional": "0",
                "actual_fill_size": "0",
                "actual_fill_notional": "0",
                "filled_size": "0",
                "filled_notional": "0",
                "unfilled_size": "10",
                "fill_probability": "0",
                "fill_pct": "0",
                "block_volume": "5",
                "trade_count": 1,
                "available_notional": "4",
                "participation_rate": "0",
                "latency_blocks": 1,
                "latency_seconds": "0",
                "no_fill_reason": "NO_LIQUIDITY",
                "execution_source": "orderfilled_limit_replay",
                "meta": {
                    "time_in_force": "FAK",
                    "strategy_intent": {"role": "taker", "order_type": "marketable_limit", "time_in_force": "FAK"},
                    "effective_liquidity_cap_pct": "80",
                    "fill_probability_haircut_pct": "20",
                    "raw_fill_probability": "0",
                    "effective_fill_probability": "0",
                    "context": {"market_category": "sports", "liquidity_bucket": "thin", "volatility_bucket": "high", "time_to_expiry_bucket": "lt_1d", "time_to_expiry_seconds": 45, "outcome_count": 60},
                },
            },
        ],
        "trades": [
            {"trade_id": "t1", "entry_price": "0.40", "close_line_probability": "0.55", "exit_price": "1", "payoff_per_share": "1", "size": "25", "notional": "10", "pnl": "15", "exit_reason": "settlement"},
            {"trade_id": "t2", "entry_price": "0.96", "close_line_probability": "0.80", "exit_price": "0", "payoff_per_share": "0", "size": "10", "notional": "9.6", "pnl": "-9.6"},
            {"trade_id": "t3", "entry_price": "0.30", "close_line_probability": "0.45", "exit_price": "0.38", "payoff_per_share": "0.38", "size": "10", "notional": "3", "pnl": "0.8"},
        ],
        "ledger": [
            {
                "ledger_id": "l1",
                "trade_id": "t1",
                "event_type": "BUY",
                "x_axis": "block_number",
                "x_value": 10,
                "shares_delta": "25",
                "cash_delta": "-10",
                "fee": "0",
                "rebate": "0",
                "slippage_cost": "0",
                "execution_cost": "0",
                "realized_pnl": "0",
                "position_after": "25",
                "cash_after": "990",
                "price": "0.40",
                "source": "simulated_trade",
            },
            {
                "ledger_id": "l2",
                "trade_id": "t1",
                "event_type": "SETTLEMENT",
                "x_axis": "block_number",
                "x_value": 110,
                "shares_delta": "-25",
                "cash_delta": "25",
                "fee": "0",
                "rebate": "0",
                "slippage_cost": "0",
                "execution_cost": "0",
                "realized_pnl": "15",
                "position_after": "0",
                "cash_after": "1015",
                "price": "1",
                "source": "simulated_trade",
            },
            {
                "ledger_id": "l3",
                "trade_id": "t2",
                "event_type": "BUY",
                "x_axis": "block_number",
                "x_value": 20,
                "shares_delta": "10",
                "cash_delta": "-9.6",
                "fee": "0",
                "rebate": "0",
                "slippage_cost": "0",
                "execution_cost": "0",
                "realized_pnl": "0",
                "position_after": "10",
                "cash_after": "1005.4",
                "price": "0.96",
                "source": "simulated_trade",
            },
            {
                "ledger_id": "l4",
                "trade_id": "t2",
                "event_type": "SELL",
                "x_axis": "block_number",
                "x_value": 80,
                "shares_delta": "-10",
                "cash_delta": "0",
                "fee": "0",
                "rebate": "0",
                "slippage_cost": "0",
                "execution_cost": "0",
                "realized_pnl": "-9.6",
                "position_after": "0",
                "cash_after": "1005.4",
                "price": "0",
                "source": "simulated_trade",
            },
            {
                "ledger_id": "l5",
                "trade_id": "t3",
                "event_type": "BUY",
                "x_axis": "block_number",
                "x_value": 30,
                "shares_delta": "10",
                "cash_delta": "-3",
                "fee": "0",
                "rebate": "0",
                "slippage_cost": "0",
                "execution_cost": "0",
                "realized_pnl": "0",
                "position_after": "10",
                "cash_after": "1002.4",
                "price": "0.30",
                "source": "simulated_trade",
            },
            {
                "ledger_id": "l6",
                "trade_id": "t3",
                "event_type": "SELL",
                "x_axis": "block_number",
                "x_value": 90,
                "shares_delta": "-10",
                "cash_delta": "3.8",
                "fee": "0",
                "rebate": "0",
                "slippage_cost": "0",
                "execution_cost": "0",
                "realized_pnl": "0.8",
                "position_after": "0",
                "cash_after": "1006.2",
                "price": "0.38",
                "source": "simulated_trade",
            },
        ],
        "events": [
            {
                "event_type": "open",
                "x_axis": "block_number",
                "x_value": 10,
                "trade_id": "t1",
                "price": "0.40",
                "message": "entry threshold reached",
                "meta": {},
            },
            {
                "event_type": "settlement",
                "x_axis": "block_number",
                "x_value": 110,
                "trade_id": "t1",
                "price": "1",
                "message": "held to settlement payoff",
                "meta": {},
            },
        ],
        "calibration_count": 2,
        "cost_calibration_count": 1,
        "calibration_rows": [
            {
                "simulated_order_id": "o1",
                "live_order_id": "live-o1",
                "simulated_status": "FILLED",
                "live_status": "FILLED",
                "simulated_fill_price": "0.40",
                "live_fill_price": "0.40",
                "simulated_fill_size": "25",
                "live_fill_size": "25",
                "simulated_slippage": "0",
                "live_slippage": "0",
                "simulated_fee": "0",
                "live_fee": "0",
                "simulated_rebate": "0",
                "live_rebate": "0",
                "simulated_cash_delta": "-10",
                "live_cash_delta": "-10",
                "simulated_position_delta": "25",
                "live_position_delta": "25",
                "simulated_latency_seconds": "1",
                "live_latency_seconds": "1",
                "role": "maker",
                "side": "BUY_YES",
                "liquidity_bucket": "medium",
                "volatility_bucket": "low",
                "time_to_expiry_bucket": "lt_7d",
            },
            {
                "simulated_order_id": "o2",
                "live_order_id": "live-o2",
                "simulated_status": "NO_FILL",
                "live_status": "NO_FILL",
                "simulated_fill_price": "0",
                "live_fill_price": "0",
                "simulated_fill_size": "0",
                "live_fill_size": "0",
                "simulated_slippage": "0",
                "live_slippage": "0",
                "simulated_fee": "0",
                "live_fee": "0",
                "simulated_rebate": "0",
                "live_rebate": "0",
                "simulated_cash_delta": "0",
                "live_cash_delta": "0",
                "simulated_position_delta": "0",
                "live_position_delta": "0",
                "simulated_latency_seconds": "1",
                "live_latency_seconds": "1",
                "role": "maker",
                "side": "BUY_YES",
                "liquidity_bucket": "thin",
                "volatility_bucket": "high",
                "time_to_expiry_bucket": "lt_1d",
            }
        ],
        "cost_calibration_rows": [
            {
                "event_type": "FEE",
                "simulated_amount": "0",
                "live_amount": "0",
                "amount_error": "0",
                "simulated_count": 1,
                "live_count": 1,
                "verdict": "matched",
            }
        ],
        "real_order_state_event_count": 2,
        "real_order_state_rows": [
            {"order_id": "o1", "external_order_id": "live-o1", "event_type": "FILLED", "payload": {"simulated_order_id": "o1"}},
            {"order_id": "o2", "external_order_id": "live-o2", "event_type": "NO_FILL", "payload": {"simulated_order_id": "o2"}},
        ],
        "real_cost_event_rows": [],
        "external_source_state_count": 1,
        "external_source_state_rows": [{"state_key": "fixture-external-source", "source_name": "fixture"}],
        "execution_model_rows": [
            {
                "execution_model": "ohlcv_close",
                "execution_profile": "realistic",
                "strategy_name": "fixed-threshold",
                "net_pnl": "20",
                "submitted_count": 10,
                "filled_count": 10,
                "partial_fill_count": 1,
                "avg_slippage": "0",
                "capacity_ratio": "0.10",
                "market_category": "sports",
                "liquidity_bucket": "active",
                "time_to_expiry_bucket": "lt_1d",
                "volatility_bucket": "low",
                "final_minute": "not_final_minute",
                "event_outcome_count_bucket": "binary",
            },
            {
                "execution_model": "formula_slippage",
                "execution_profile": "realistic",
                "strategy_name": "fixed-threshold",
                "net_pnl": "16",
                "submitted_count": 10,
                "filled_count": 9,
                "partial_fill_count": 1,
                "avg_slippage": "0.01",
                "capacity_ratio": "0.20",
                "market_category": "sports",
                "liquidity_bucket": "active",
                "time_to_expiry_bucket": "lt_1d",
                "volatility_bucket": "low",
                "final_minute": "not_final_minute",
                "event_outcome_count_bucket": "binary",
            },
            {
                "execution_model": "l2_orderfilled",
                "execution_profile": "conservative",
                "strategy_name": "fixed-threshold",
                "net_pnl": "3",
                "submitted_count": 10,
                "filled_count": 5,
                "partial_fill_count": 1,
                "unfilled_cancelled_count": 5,
                "avg_slippage": "0.02",
                "queue_wait_seconds": "4",
                "capacity_ratio": "0.20",
                "market_category": "sports",
                "liquidity_bucket": "active",
                "time_to_expiry_bucket": "lt_1d",
                "volatility_bucket": "low",
                "final_minute": "not_final_minute",
                "event_outcome_count_bucket": "binary",
            },
            {
                "execution_model": "l2_orderfilled",
                "execution_profile": "realistic",
                "strategy_name": "fixed-threshold",
                "net_pnl": "5",
                "submitted_count": 10,
                "filled_count": 6,
                "partial_fill_count": 1,
                "unfilled_cancelled_count": 4,
                "avg_slippage": "0.025",
                "queue_wait_seconds": "3",
                "capacity_ratio": "0.45",
                "market_category": "sports",
                "liquidity_bucket": "active",
                "time_to_expiry_bucket": "lt_1d",
                "volatility_bucket": "low",
                "final_minute": "not_final_minute",
                "event_outcome_count_bucket": "binary",
            },
            {
                "execution_model": "l2_orderfilled",
                "execution_profile": "optimistic",
                "strategy_name": "fixed-threshold",
                "net_pnl": "7",
                "submitted_count": 10,
                "filled_count": 7,
                "partial_fill_count": 1,
                "unfilled_cancelled_count": 3,
                "avg_slippage": "0.03",
                "queue_wait_seconds": "2",
                "capacity_ratio": "0.90",
                "market_category": "sports",
                "liquidity_bucket": "active",
                "time_to_expiry_bucket": "lt_1d",
                "volatility_bucket": "low",
                "final_minute": "not_final_minute",
                "event_outcome_count_bucket": "binary",
            },
            {
                "execution_model": "l2_orderfilled",
                "execution_profile": "conservative",
                "strategy_name": "fixed-threshold",
                "net_pnl": "-1",
                "submitted_count": 10,
                "filled_count": 1,
                "partial_fill_count": 0,
                "unfilled_cancelled_count": 9,
                "avg_slippage": "0.05",
                "queue_wait_seconds": "7",
                "capacity_ratio": "1.50",
                "market_category": "crypto",
                "liquidity_bucket": "thin",
                "time_to_expiry_bucket": "gte_7d",
                "volatility_bucket": "high",
                "final_minute": "final_minute",
                "event_outcome_count_bucket": "large_multi_21_plus",
            },
        ],
        "platform_incidents": [{"incident_key": "fixture-ok", "severity": "info", "component": "platform"}],
        "event_outcomes": [
            {"market_slug": "demo-market", "outcome_label": "Alpha", "yes_probability": "0.40", "no_probability": "0.60"},
            {"market_slug": "demo-market-b", "outcome_label": "Beta", "yes_probability": "0.35", "no_probability": "0.65"},
            {"market_slug": "demo-market-c", "outcome_label": "Gamma", "yes_probability": "0.25", "no_probability": "0.75"},
        ],
        "outcome_correlation": {
            "status": "ready",
            "method": "block_return",
            "sample_count": 100,
            "max_abs_correlation": "0.42",
            "matrix": {"demo-market": {"demo-market": 1}},
        },
    }


def test_complete_run_artifact_report_is_ready() -> None:
    inputs = complete_inputs()
    fill_quality = inputs["run"]["meta"]["actual_data_quality"]["fill_quality"]
    fill_quality["block_participation_discount_tick_count"] = 1
    fill_quality["max_requested_block_participation_pct"] = "500"
    fill_quality["min_block_participation_factor"] = "0.5"

    report = build_backtest_run_artifact_report(inputs)

    assert report["status"] == READY
    assert report["artifacts"]["data_version"] == "abc123"
    assert report["data_quality_report"]["quality_verdict"] == READY
    assert report["artifacts"]["data_quality_verdict"] == READY
    assert report["reproducibility_report"]["reproducibility_verdict"] == READY
    assert report["artifacts"]["reproducibility_verdict"] == READY
    assert report["materialized_cache_report"]["cache_verdict"] == READY
    assert report["artifacts"]["materialized_cache_verdict"] == READY
    assert report["artifacts"]["materialized_cache_source_table"] == "quant.market_token_block_close"
    assert report["raw_orderfilled_replay_contract_report"]["contract_verdict"] == READY
    assert report["raw_orderfilled_replay_contract_report"]["raw_trade_tick_count"] == 10
    assert report["raw_orderfilled_replay_contract_report"]["block_vwap_available_pct"] == "100"
    assert report["raw_orderfilled_replay_contract_report"]["block_order_sequence_coverage_pct"] == "100"
    assert report["raw_orderfilled_replay_contract_report"]["multi_trade_sequence_coverage_pct"] == "100"
    assert report["artifacts"]["raw_orderfilled_replay_contract_verdict"] == READY
    assert report["artifacts"]["raw_orderfilled_loaded_event_count"] == 10
    assert report["artifacts"]["raw_orderfilled_block_vwap_available_pct"] == "100"
    assert report["artifacts"]["raw_orderfilled_block_high_low_available_pct"] == "100"
    assert report["artifacts"]["raw_orderfilled_block_order_sequence_coverage_pct"] == "100"
    assert report["artifacts"]["raw_orderfilled_multi_trade_sequence_coverage_pct"] == "100"
    assert report["historical_l2_alignment_report"]["alignment_verdict"] == READY
    assert report["historical_l2_alignment_report"]["aligned_count"] == 2
    assert report["historical_l2_alignment_report"]["depth_checked_count"] == 2
    assert report["historical_l2_alignment_report"]["depth_sufficient_pct"] == "100"
    assert report["artifacts"]["historical_l2_alignment_verdict"] == READY
    assert report["artifacts"]["historical_l2_alignment_pct"] == "100"
    assert report["artifacts"]["historical_l2_depth_sufficient_pct"] == "100"
    assert report["artifacts"]["historical_l2_crossable_depth_sufficient_pct"] == "100"
    l2_check = next(check for check in report["checks"] if check["name"] == "historical l2 alignment report")
    assert l2_check["status"] == READY
    assert "depth=2/2" in l2_check["detail"]
    raw_replay_check = next(check for check in report["checks"] if check["name"] == "raw orderfilled replay contract")
    assert "vwap_blocks=2/2" in raw_replay_check["detail"]
    assert "sequence_blocks=2/2" in raw_replay_check["detail"]
    assert report["execution_semantics_report"]["semantics_verdict"] == READY
    assert report["execution_semantics_report"]["order_type_counts"] == {"marketable_limit": 1, "post_only_limit": 1}
    assert report["execution_semantics_report"]["time_in_force_counts"] == {"FAK": 1, "GTC": 1}
    assert report["artifacts"]["execution_semantics_verdict"] == READY
    assert report["fill_probability_evidence_report"]["evidence_verdict"] == READY
    assert report["fill_probability_evidence_report"]["ready_order_count"] == 2
    assert report["fill_probability_evidence_report"]["fill_probability_buckets"] == {"0": 1, "100": 1}
    assert report["artifacts"]["fill_probability_evidence_verdict"] == READY
    assert report["maker_taker_execution_report"]["maker_taker_verdict"] == READY
    assert report["maker_taker_execution_report"]["role_counts"] == {"maker": 1, "taker": 1}
    assert report["maker_taker_execution_report"]["role_summaries"]["maker"]["fill_rate"] == "100"
    assert report["maker_taker_execution_report"]["role_summaries"]["taker"]["no_fill_count"] == 1
    assert report["artifacts"]["maker_taker_execution_verdict"] == READY
    assert report["artifacts"]["maker_order_count"] == 1
    assert report["artifacts"]["taker_order_count"] == 1
    assert report["maker_queue_uncertainty_report"]["status"] == READY
    assert report["maker_queue_uncertainty_report"]["queue_uncertainty_verdict"] == REVIEW
    assert report["maker_queue_uncertainty_report"]["execution_scope"] == "orderfilled_proxy_no_lob"
    assert report["maker_queue_uncertainty_report"]["maker_order_count"] == 1
    assert report["maker_queue_uncertainty_report"]["risk_order_count"] == 1
    assert report["maker_queue_uncertainty_report"]["high_participation_order_count"] == 1
    assert report["maker_queue_uncertainty_report"]["missing_queue_evidence_count"] == 1
    assert Decimal(report["maker_queue_uncertainty_report"]["suggested_fill_haircut_pct"]) > Decimal("0")
    assert "queue position is not proven" in report["maker_queue_uncertainty_report"]["reason"]
    assert report["artifacts"]["maker_queue_uncertainty_verdict"] == REVIEW
    assert report["artifacts"]["maker_queue_risk_order_count"] == 1
    assert report["latency_profile_report"]["status"] == READY
    assert report["latency_profile_report"]["latency_verdict"] == READY
    assert report["latency_profile_report"]["latency_profile"] == "conservative"
    assert report["latency_profile_report"]["fak_fok_order_count"] == 1
    assert report["latency_profile_report"]["stale_price_risk_count"] == 0
    assert report["artifacts"]["latency_profile_verdict"] == READY
    assert report["environment_incident_report"]["status"] == READY
    assert report["environment_incident_report"]["incident_verdict"] == READY
    assert report["environment_incident_report"]["incident_count"] == 1
    assert report["environment_incident_report"]["severe_incident_count"] == 0
    assert report["artifacts"]["environment_incident_verdict"] == READY
    assert report["external_signal_contract_report"]["status"] == READY
    assert report["external_signal_contract_report"]["signal_verdict"] == READY
    assert report["external_signal_contract_report"]["event_count"] == 0
    assert report["artifacts"]["external_signal_contract_verdict"] == READY
    assert report["slippage_regime_report"]["status"] == READY
    assert report["slippage_regime_report"]["slippage_regime_verdict"] == REVIEW
    assert report["slippage_regime_report"]["risk_order_count"] == 1
    assert report["slippage_regime_report"]["risk_regime_count"] >= 1
    assert report["artifacts"]["slippage_regime_verdict"] == REVIEW
    assert report["artifacts"]["slippage_risk_order_count"] == 1
    assert report["artifacts"]["code_commit"] == "abcdeffedcba"
    assert report["artifacts"]["strategy_version"] == "fixed_threshold_v1"
    assert report["artifacts"]["orders"] == 2
    assert report["credibility"]["status"] == READY
    assert report["artifacts"]["run_credibility_score"] == 100
    assert report["artifacts"]["raw_replay_fallback_suppressed_count"] == 1
    assert report["artifacts"]["raw_replay_synthetic_cross_no_fill_count"] == 1
    assert report["artifacts"]["block_participation_discount_tick_count"] == 1
    assert report["artifacts"]["max_requested_block_participation_pct"] == "500"
    assert report["artifacts"]["min_block_participation_factor"] == "0.5"
    assert report["regime_report"]["status"] == READY
    assert report["artifacts"]["regime_report_status"] == READY
    assert report["regime_coverage_report"]["status"] == READY
    assert report["regime_coverage_report"]["coverage_verdict"] == REVIEW
    assert report["artifacts"]["strategy_scope"] == "regime_specific"
    assert report["execution_model_validation_report"]["status"] == READY
    assert report["execution_model_validation_report"]["validation_verdict"] == READY
    assert report["execution_model_validation_report"]["l2_profile_sensitivity"]["monotonic_fill_rate_ok"] is True
    assert report["artifacts"]["execution_model_validation_verdict"] == READY
    assert report["artifacts"]["execution_model_l2_monotonic_fill_rate_ok"] is True
    assert report["shadow_live_triangulation_report"]["status"] == READY
    assert report["shadow_live_triangulation_report"]["triangulation_verdict"] == READY
    assert report["shadow_live_triangulation_report"]["fill_model_suspect"] is False
    assert report["artifacts"]["shadow_live_triangulation_status"] == READY
    assert report["artifacts"]["shadow_live_triangulation_verdict"] == READY
    assert report["artifacts"]["fill_model_suspect"] is False
    assert report["artifacts"]["shadow_live_sample_count"] == 2
    assert report["external_source_run_coverage_report"]["status"] == READY
    assert report["external_source_run_coverage_report"]["order_state_coverage_pct"] == "100"
    assert report["external_source_run_coverage_report"]["calibration_coverage_pct"] == "100"
    assert report["artifacts"]["external_run_coverage_status"] == READY
    assert report["artifacts"]["external_run_candidate_orders"] == 2
    assert report["external_source_missing_evidence_plan"]["status"] == READY
    assert report["external_source_missing_evidence_plan"]["missing_order_state_count"] == 0
    assert report["external_source_missing_evidence_plan"]["missing_calibration_count"] == 0
    assert report["artifacts"]["external_missing_evidence_status"] == READY
    assert report["artifacts"]["external_missing_order_state_count"] == 0
    assert report["artifacts"]["external_missing_calibration_count"] == 0
    assert report["tail_risk_report"]["status"] == READY
    assert report["artifacts"]["tail_risk_status"] == READY
    assert report["prediction_quality_report"]["status"] == READY
    assert report["prediction_quality_report"]["prediction_verdict"] == READY
    assert report["prediction_quality_report"]["sample_count"] == 3
    assert report["artifacts"]["prediction_quality_status"] == READY
    assert report["artifacts"]["prediction_quality_sample_count"] == 3
    assert report["performance_score_report"]["status"] == READY
    assert report["performance_score_report"]["closed_trade_count"] == 3
    assert report["artifacts"]["performance_score_status"] == READY
    assert report["artifacts"]["performance_score"] == report["performance_score_report"]["performance_score"]
    assert report["artifacts"]["performance_calmar"] == report["performance_score_report"]["calmar"]
    assert report["settlement_compatibility_report"]["status"] == READY
    assert report["settlement_compatibility_report"]["compatibility_verdict"] == READY
    assert report["artifacts"]["settlement_compatibility_status"] == READY
    assert report["event_level_risk_report"]["risk_verdict"] == READY
    assert report["artifacts"]["event_level_risk_verdict"] == READY
    assert report["event_level_risk_report"]["probability_sum"]["value"] == "1"
    assert report["event_stream_report"]["status"] == READY
    assert report["artifacts"]["event_stream_status"] == READY
    assert report["event_stream_report"]["type_counts"]["RAW_TRADE"] == 2
    assert report["event_stream_report"]["raw_trade_tick_report"]["trade_tick_count"] == 2
    assert report["event_stream_report"]["raw_trade_tick_report"]["block_count"] == 2
    assert report["artifacts"]["event_stream_raw_trade_tick_count"] == 2
    assert report["artifacts"]["event_stream_raw_trade_block_count"] == 2
    assert report["joint_replay_report"]["status"] == READY
    assert report["joint_replay_report"]["joint_replay_verdict"] == "single_outcome_only"
    assert report["artifacts"]["joint_replay_mode"] == "single_outcome_event_stream"
    assert report["artifacts"]["joint_replay_raw_trade_tick_count"] == 2
    assert report["artifacts"]["joint_replay_raw_trade_block_count"] == 2
    assert report["artifacts"]["joint_replay_raw_canonical_fill_key_coverage_pct"] == "100"
    assert report["artifacts"]["joint_replay_raw_block_context_coverage_pct"] == "100"
    assert report["joint_execution_report"]["status"] == READY
    assert report["joint_execution_report"]["execution_verdict"] == "single_outcome_execution_ready"
    assert report["artifacts"]["joint_execution_mode"] == "single_outcome_event_stream_execution"
    assert report["artifacts"]["joint_execution_raw_trade_tick_count"] == 2
    assert report["artifacts"]["joint_execution_raw_trade_block_count"] == 2
    assert report["artifacts"]["joint_execution_raw_canonical_fill_key_coverage_pct"] == "100"
    assert report["artifacts"]["joint_execution_raw_block_context_coverage_pct"] == "100"
    assert report["artifacts"]["joint_execution_max_cash_at_risk"] == "22.6"
    assert report["execution_ledger_parity_report"]["parity_verdict"] == READY
    assert report["artifacts"]["execution_ledger_parity_verdict"] == READY
    assert report["ledger_cashflow_validation_report"]["cashflow_verdict"] == READY
    assert report["ledger_cashflow_validation_report"]["net_profit_trade"] == "6.2"
    assert report["ledger_cashflow_validation_report"]["net_profit_ledger"] == "6.2"
    assert report["artifacts"]["ledger_cashflow_verdict"] == READY
    assert report["artifacts"]["ledger_diff"] == "0"
    assert report["promotion_gate_report"]["promotion_verdict"] == REVIEW
    assert report["promotion_gate_report"]["paper_promotion_allowed"] is True
    assert report["promotion_gate_report"]["production_promotion_allowed"] is False
    assert report["artifacts"]["promotion_verdict"] == REVIEW
    assert report["artifacts"]["paper_promotion_allowed"] is True
    assert report["artifacts"]["production_promotion_allowed"] is False
    assert report["paper_live_evidence_gate_report"]["status"] == READY
    assert report["paper_live_evidence_gate_report"]["paper_allowed"] is True
    assert report["paper_live_evidence_gate_report"]["live_allowed"] is False
    assert report["paper_live_evidence_gate_report"]["run_evidence_ready"] is True
    assert report["paper_live_evidence_gate_report"]["missing_evidence_ready"] is True
    assert report["artifacts"]["paper_live_evidence_gate_status"] == READY
    assert report["artifacts"]["paper_live_evidence_gate_paper_allowed"] is True
    assert report["artifacts"]["paper_live_evidence_gate_live_allowed"] is False
    assert any(check["name"] == "fill quality artifact" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "reproducibility report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "materialized replay cache" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "execution semantics report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "fill probability evidence report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "execution regime report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "regime coverage report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "execution model validation report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "shadow/live triangulation report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "external source run coverage" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "tail risk report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "prediction quality report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "settlement compatibility report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "event-level risk report" and check["status"] == READY for check in report["checks"])
    event_stream_check = next(check for check in report["checks"] if check["name"] == "event stream contract")
    assert event_stream_check["status"] == READY
    assert "canonical=" in event_stream_check["detail"]
    assert "attribution=" in event_stream_check["detail"]
    assert any(check["name"] == "joint replay plan" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "joint replay execution" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "execution/ledger parity report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "ledger cashflow validation" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "fill-first promotion gate" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "paper/live evidence gate" and check["status"] == READY for check in report["checks"])

    markdown = backtest_run_artifact_report_to_markdown(report)
    assert "Shadow/Live Triangulation Report" in markdown
    assert "Execution Model Validation Report" in markdown
    assert "External Source Run Coverage" in markdown
    assert "Missing External Evidence Plan" in markdown
    assert "Execution Semantics Report" in markdown
    assert "Fill Probability Evidence Report" in markdown
    assert "fill_model_suspect: False" in markdown
    assert "Fill-First Promotion Gate" in markdown
    assert "Paper/Live Evidence Gate" in markdown
    assert "paper_allowed: True" in markdown
    assert "Ledger Cashflow Validation Report" in markdown
    assert "production_promotion_allowed: False" in markdown


def test_artifact_markdown_shows_missing_evidence_task_pack_paths() -> None:
    report = build_backtest_run_artifact_report(complete_inputs())
    report["missing_evidence_task_pack"] = {
        "status": "review",
        "output_dir": "runtime_outputs/missing_external_evidence_7",
        "missing_order_state_count": 2,
        "missing_calibration_count": 3,
        "event_template_count": 2,
        "files": {
            "plan_json": "runtime_outputs/missing_external_evidence_7/missing_external_evidence_7.json",
            "event_templates_jsonl": "runtime_outputs/missing_external_evidence_7/missing_external_evidence_7.event_templates.jsonl",
        },
    }

    markdown = backtest_run_artifact_report_to_markdown(report)

    assert "Missing Evidence Task Pack" in markdown
    assert "output_dir: runtime_outputs/missing_external_evidence_7" in markdown
    assert "event_templates_jsonl" in markdown


def test_execution_semantics_report_flags_missing_tif() -> None:
    orders = [
        {
            "order_id": "missing-tif",
            "status": "NO_FILL",
            "side": "BUY_YES",
            "role": "maker",
            "order_type": "post_only_limit",
            "no_fill_reason": "NO_LIQUIDITY",
            "execution_source": "orderfilled_limit_replay",
            "latency_blocks": 1,
            "meta": {},
        }
    ]

    report = build_execution_semantics_report(orders)

    assert report["status"] == REVIEW
    assert report["missing_counts"]["time_in_force"] == 1
    assert "missing time_in_force" in report["reason"]


def test_execution_semantics_report_preserves_explicit_zero_latency() -> None:
    report = build_execution_semantics_report(
        [
            {
                "order_id": "zero-latency",
                "status": "FILLED",
                "side": "BUY_YES",
                "role": "maker",
                "order_type": "post_only_limit",
                "execution_source": "orderfilled_limit_replay_raw",
                "latency_blocks": 0,
                "latency_seconds": "0",
                "meta": {"time_in_force": "GTC"},
            }
        ]
    )

    assert report["status"] == READY
    assert report["semantics_verdict"] == READY
    assert report["missing_counts"]["latency"] == 0
    assert report["latency"]["avg_latency_blocks"] == "0"
    assert report["latency"]["avg_latency_seconds"] == "0"
    assert report["latency"]["latency_sample_count"] == 2


def test_fill_probability_evidence_report_flags_missing_haircut_metadata() -> None:
    orders = [
        {
            "order_id": "missing-haircut",
            "status": "PARTIAL_FILLED",
            "side": "BUY_YES",
            "role": "maker",
            "order_type": "post_only_limit",
            "fill_probability": "50",
            "block_volume": "20",
            "trade_count": 2,
            "available_notional": "5",
            "participation_rate": "80",
            "requested_notional": "10",
            "expected_fill_size": "5",
            "expected_fill_notional": "5",
            "actual_fill_size": "5",
            "actual_fill_notional": "5",
            "execution_source": "orderfilled_limit_replay_raw",
            "meta": {"effective_liquidity_cap_pct": "80"},
        }
    ]

    report = build_fill_probability_evidence_report(orders)

    assert report["status"] == REVIEW
    assert report["missing_field_counts"]["meta.fill_probability_haircut_pct"] == 1


def test_run_data_quality_report_flags_fallback_and_dedupe_review() -> None:
    inputs = complete_inputs()
    data_quality = inputs["run"]["meta"]["actual_data_quality"]
    fill_quality = data_quality["fill_quality"]
    data_quality["status"] = "review"
    data_quality["warning_level"] = "WARN"
    data_quality["gap_count"] = 1
    data_quality["largest_gaps"] = [{"from_x": 12, "to_x": 40, "span": 28}]
    data_quality["orderfilled_replay"] = {"source": "orderfilled_fact", "fallback": "synthetic_block_close_events"}
    fill_quality["fallback_candidate_orders"] = 2
    fill_quality["raw_evidence_summary"]["candidate_event_duplicate_count"] = 4

    report = build_run_data_quality_report(data_quality, fill_quality)

    assert report["status"] == READY
    assert report["quality_verdict"] == REVIEW
    assert report["max_gap"] == 28
    assert report["fallback_count"] == 3
    assert report["duplicate_count"] == 4
    assert report["source_mix"]["raw_replay_fallback"] == "synthetic_block_close_events"
    assert report["dedupe_stats"]["candidate_event_duplicate_count"] == 4


def test_materialized_cache_report_requires_materialized_source_and_bounded_raw_detail() -> None:
    inputs = complete_inputs()
    run = inputs["run"]
    data_quality = run["meta"]["actual_data_quality"]
    report = build_materialized_cache_report(
        run,
        data_quality,
        build_run_data_quality_report(data_quality, data_quality["fill_quality"]),
        data_quality["fill_quality"],
    )

    assert report["status"] == READY
    assert report["cache_verdict"] == READY
    assert report["materialized_price_input"] is True
    assert report["keyed_access_path"] is True
    assert report["strict_keyed_access_path"] is True
    assert report["bounded_raw_detail"] is True
    assert report["data_access_contract"]["status"] == READY
    assert report["data_access_contract"]["price_access_path_allowed"] is True
    assert report["data_access_contract"]["raw_detail_has_limit"] is True
    assert report["raw_detail_window"]["from_block"] == 10
    assert report["raw_detail_window"]["market_id"] == 123
    assert report["input_snapshot"]["data_version"] == "abc123"


def test_materialized_cache_report_reviews_raw_main_source_and_missing_snapshot() -> None:
    inputs = complete_inputs()
    run = inputs["run"]
    data_quality = run["meta"]["actual_data_quality"]
    data_quality["source_table"] = "orderfilled_fact"
    data_quality["access_path"] = "global_scan"
    data_quality.pop("data_version")
    data_quality.pop("requested_to")
    data_quality["orderfilled_replay"] = {"source": "orderfilled_fact"}
    data_quality["fill_quality"].pop("loaded_block_window", None)

    report = build_materialized_cache_report(
        run,
        data_quality,
        build_run_data_quality_report(data_quality, data_quality["fill_quality"]),
        data_quality["fill_quality"],
    )

    assert report["cache_verdict"] == MISSING
    assert report["materialized_price_input"] is False
    assert report["keyed_access_path"] is False
    assert report["strict_keyed_access_path"] is False
    assert report["bounded_raw_detail"] is False
    assert "allowed_price_source_table" in report["data_access_contract"]["missing"]
    assert "allowed_price_access_path" in report["data_access_contract"]["missing"]
    assert "raw_explicit_limit" in report["data_access_contract"]["missing"]
    assert "data_version" in report["missing_snapshot_fields"]
    assert "requested_to" in report["missing_snapshot_fields"]


def test_materialized_cache_report_requires_explicit_raw_orderfilled_limit() -> None:
    inputs = complete_inputs()
    run = inputs["run"]
    data_quality = run["meta"]["actual_data_quality"]
    data_quality["orderfilled_replay"].pop("limit")
    data_quality["orderfilled_replay"].pop("loaded_block_window", None)
    data_quality["fill_quality"].pop("loaded_block_window", None)

    report = build_materialized_cache_report(
        run,
        data_quality,
        build_run_data_quality_report(data_quality, data_quality["fill_quality"]),
        data_quality["fill_quality"],
    )

    assert report["cache_verdict"] == REVIEW
    assert report["materialized_price_input"] is True
    assert report["strict_keyed_access_path"] is True
    assert report["bounded_raw_detail"] is False
    assert report["data_access_contract"]["raw_detail_windowed"] is True
    assert report["data_access_contract"]["raw_detail_identified"] is True
    assert report["data_access_contract"]["raw_detail_has_limit"] is False
    assert report["data_access_contract"]["missing"] == ["raw_explicit_limit"]


def test_raw_orderfilled_replay_contract_requires_loaded_tick_and_window_stats() -> None:
    inputs = complete_inputs()
    data_quality = inputs["run"]["meta"]["actual_data_quality"]
    fill_quality = data_quality["fill_quality"]
    for key in (
        "loaded_raw_event_count",
        "deduped_raw_event_count",
        "raw_trade_tick_count",
        "raw_block_count",
        "raw_canonical_fill_key_coverage_pct",
        "raw_maker_taker_side_coverage_pct",
        "raw_block_context_coverage_pct",
        "loaded_block_window",
    ):
        fill_quality.pop(key, None)
    fill_quality["raw_evidence_summary"].pop("raw_trade_tick_report", None)
    fill_quality["raw_evidence_summary"].pop("raw_block_context_coverage_pct", None)
    replay = data_quality["orderfilled_replay"]
    for key in (
        "loaded_event_count",
        "deduped_event_count",
        "raw_trade_tick_count",
        "raw_block_count",
        "raw_canonical_fill_key_coverage_pct",
        "raw_maker_taker_side_coverage_pct",
        "raw_block_context_coverage_pct",
        "loaded_block_window",
        "from_block",
        "to_block",
        "market_id",
        "token_id",
        "limit",
        "raw_trade_tick_report",
    ):
        replay.pop(key, None)

    report = build_raw_orderfilled_replay_contract_report(data_quality, fill_quality)

    assert report["contract_verdict"] == MISSING
    assert "raw_block_window_from" in report["missing_contract_fields"]
    assert "raw_market_id" in report["missing_contract_fields"]
    assert "raw_trade_tick_count" in report["missing_contract_fields"]
    assert "block_context_coverage_pct" in report["missing_contract_fields"]


def test_raw_orderfilled_contract_accepts_v3_bounded_one_sided_trade_slices() -> None:
    report = build_raw_orderfilled_replay_contract_report(
        {
            "fill_only_v3": {
                "source_coverage": {
                    "source_table": "trade_prints_one_sided",
                    "receipt_count": 2,
                    "intervals": [{"from_block": 100, "to_block": 300}],
                },
                "required_trade_windows": [
                    {
                        "market_id": 7,
                        "asset_id": "asset",
                        "start_block": 120,
                        "end_block": 180,
                    }
                ],
                "trade_slice_loads": [{"rows_loaded": 3, "db_query_count": 1}],
            }
        },
        {},
    )

    assert report["contract_verdict"] == READY
    assert report["contract_type"] == "ONE_SIDED_TRADE_SLICE_V1"
    assert report["source"] == "trade_prints_one_sided"
    assert report["loaded_event_count"] == 3
    assert report["loaded_block_window"]["from_block"] == 120
    assert report["loaded_block_window"]["to_block"] == 180


def test_raw_orderfilled_contract_rejects_uncovered_v3_trade_window() -> None:
    report = build_raw_orderfilled_replay_contract_report(
        {
            "fill_only_v3": {
                "source_coverage": {
                    "source_table": "trade_prints_one_sided",
                    "receipt_count": 1,
                    "intervals": [{"from_block": 100, "to_block": 150}],
                },
                "required_trade_windows": [
                    {"start_block": 120, "end_block": 180}
                ],
                "trade_slice_loads": [{"rows_loaded": 0}],
            }
        },
        {},
    )

    assert report["contract_verdict"] == MISSING
    assert "coverage_for_every_required_window" in report["missing_contract_fields"]


def test_fill_probability_evidence_recognizes_v3_raw_orderfilled_evidence() -> None:
    report = build_fill_probability_evidence_report(
        [
            {
                "execution_source": "fill_only_v3_trade_only",
                "execution_evidence_type": "raw_orderfilled",
                "fill_probability": "100",
                "block_volume": "0",
                "trade_count": 1,
                "available_notional": "1",
                "participation_rate": "0.025",
                "requested_notional": "2",
                "expected_fill_size": "1",
                "expected_fill_notional": "1",
                "actual_fill_size": "1",
                "actual_fill_notional": "1",
                "meta": {
                    "execution_evidence_type": "raw_orderfilled",
                    "effective_liquidity_cap_pct": "2.5",
                    "fill_probability_haircut_pct": "0",
                },
            }
        ]
    )

    assert report["evidence_verdict"] == READY
    assert report["orderfilled_order_count"] == 1


def test_execution_semantics_recognizes_v3_raw_orderfilled_evidence() -> None:
    report = build_execution_semantics_report(
        [
            {
                "status": "PARTIAL_FILLED",
                "role": "taker",
                "order_type": "fill_only_v3_trade_only_limit",
                "latency_seconds": "1",
                "execution_source": "fill_only_v3_trade_only",
                "execution_evidence_type": "raw_orderfilled",
                "meta": {"time_in_force": "GTC"},
            }
        ]
    )

    assert report["semantics_verdict"] == READY
    assert report["semantics_present"]["orderfilled_execution_source"] is True


def test_raw_orderfilled_replay_contract_accepts_classified_exact_duplicates() -> None:
    inputs = complete_inputs()
    data_quality = inputs["run"]["meta"]["actual_data_quality"]
    fill_quality = data_quality["fill_quality"]
    replay = data_quality["orderfilled_replay"]
    fill_quality.update(
        {
            "loaded_raw_event_count": 11,
            "deduped_raw_event_count": 10,
            "raw_duplicate_event_count": 1,
            "raw_exact_duplicate_event_count": 1,
            "raw_conflicting_duplicate_event_count": 0,
            "raw_duplicate_group_count": 1,
            "raw_conflicting_duplicate_group_count": 0,
        }
    )
    replay.update(
        {
            "loaded_event_count": 11,
            "deduped_event_count": 10,
            "duplicate_event_count": 1,
            "exact_duplicate_event_count": 1,
            "conflicting_duplicate_event_count": 0,
            "duplicate_group_count": 1,
            "conflicting_duplicate_group_count": 0,
        }
    )

    report = build_raw_orderfilled_replay_contract_report(data_quality, fill_quality)

    assert report["contract_verdict"] == READY
    assert report["duplicate_event_count"] == 1
    assert report["exact_duplicate_event_count"] == 1
    assert report["conflicting_duplicate_event_count"] == 0
    assert report["duplicate_group_count"] == 1
    assert report["conflicting_duplicate_group_count"] == 0
    assert report["loaded_block_window"]["duplicate_group_count"] == 1
    assert report["duplicate_classification"] == "classified"


def test_raw_orderfilled_replay_contract_reviews_conflicting_duplicates() -> None:
    inputs = complete_inputs()
    data_quality = inputs["run"]["meta"]["actual_data_quality"]
    fill_quality = data_quality["fill_quality"]
    replay = data_quality["orderfilled_replay"]
    fill_quality.update(
        {
            "loaded_raw_event_count": 11,
            "deduped_raw_event_count": 10,
            "raw_duplicate_event_count": 1,
            "raw_exact_duplicate_event_count": 0,
            "raw_conflicting_duplicate_event_count": 1,
            "raw_duplicate_group_count": 1,
            "raw_conflicting_duplicate_group_count": 1,
        }
    )
    replay.update(
        {
            "loaded_event_count": 11,
            "deduped_event_count": 10,
            "duplicate_event_count": 1,
            "exact_duplicate_event_count": 0,
            "conflicting_duplicate_event_count": 1,
            "duplicate_group_count": 1,
            "conflicting_duplicate_group_count": 1,
        }
    )

    report = build_raw_orderfilled_replay_contract_report(data_quality, fill_quality)

    assert report["contract_verdict"] == REVIEW
    assert report["conflicting_duplicate_event_count"] == 1
    assert report["duplicate_group_count"] == 1
    assert report["conflicting_duplicate_group_count"] == 1
    assert "conflicting duplicate events" in report["reason"]


def test_historical_l2_alignment_report_reviews_missing_artifact() -> None:
    report = build_historical_l2_alignment_report({}, {}, {})

    assert report["status"] == REVIEW
    assert report["alignment_verdict"] == REVIEW
    assert report["aligned_count"] == 0
    assert "no historical L2/PMXT alignment artifact" in report["reason"]


def test_historical_l2_alignment_report_reviews_stale_or_outside_spread() -> None:
    report = build_historical_l2_alignment_report(
        {},
        {
            "pmxt_l2_alignment": {
                "status": "review",
                "orderfilled_rows_seen": 2,
                "orderfilled_rows_matched": 2,
                "aligned_count": 1,
                "alignment_pct": "50",
                "stale_l2_count": 1,
                "price_outside_spread_count": 1,
            }
        },
        {},
    )

    assert report["status"] == REVIEW
    assert report["alignment_verdict"] == REVIEW
    assert report["aligned_count"] == 1
    assert "stale L2" in report["reason"]
    assert "priced outside" in report["reason"]


def test_historical_l2_alignment_report_reviews_insufficient_depth() -> None:
    report = build_historical_l2_alignment_report(
        {},
        {
            "pmxt_l2_alignment": {
                "status": "ready",
                "orderfilled_rows_seen": 2,
                "orderfilled_rows_matched": 2,
                "aligned_count": 2,
                "alignment_pct": "100",
                "depth_checked_count": 2,
                "depth_sufficient_count": 1,
                "depth_insufficient_count": 1,
                "crossable_depth_sufficient_count": 1,
            }
        },
        {},
    )

    assert report["status"] == REVIEW
    assert report["alignment_verdict"] == REVIEW
    assert report["depth_checked_count"] == 2
    assert report["depth_sufficient_pct"] == "50"
    assert report["crossable_depth_sufficient_pct"] == "50"
    assert "sufficient same-side L2 depth" in report["reason"]
    assert "crossable L2 depth" in report["reason"]


def test_reproducibility_report_requires_code_and_model_versions() -> None:
    inputs = complete_inputs()
    meta = inputs["run"]["meta"]
    for key in ("code_commit", "artifact_schema_version", "model_versions"):
        meta.pop(key)

    report = build_reproducibility_report(
        inputs["run"],
        inputs["parameters"],
        build_run_data_quality_report(meta["actual_data_quality"], meta["actual_data_quality"]["fill_quality"]),
        meta["actual_data_quality"]["fill_quality"],
        inputs,
    )

    assert report["status"] == READY
    assert report["reproducibility_verdict"] == MISSING
    assert "code_commit" in report["missing_fields"]
    assert "fill_model_version" in report["missing_fields"]
    assert "artifact_schema_version" in report["missing_fields"]


def test_execution_ledger_parity_report_requires_shared_schema() -> None:
    inputs = complete_inputs()
    inputs["orders"][0].pop("submit_x")
    inputs["ledger"][0].pop("cash_delta")

    report = build_execution_ledger_parity_report(inputs["orders"], inputs["ledger"], inputs)

    assert report["status"] == READY
    assert report["parity_verdict"] == MISSING
    assert report["order_schema"]["missing_by_field"]["submit_x"] == 1
    assert report["ledger_schema"]["missing_by_field"]["cash_delta"] == 1
    assert report["live_evidence"]["status"] == READY


def test_ledger_cashflow_validation_reviews_unlinked_trade_pnl() -> None:
    inputs = complete_inputs()
    inputs["ledger"] = inputs["ledger"][:2]

    report = build_ledger_cashflow_validation_report(inputs["trades"], inputs["ledger"], inputs["parameters"])

    assert report["status"] == READY
    assert report["cashflow_verdict"] == REVIEW
    assert report["missing_trade_ledger_count"] == 2
    assert report["net_profit_trade"] == "6.2"
    assert report["net_profit_ledger"] == "15"


def test_ledger_cashflow_validation_reports_polymarket_special_events() -> None:
    ledger = [
        {
            "ledger_id": "l-buy",
            "event_type": "BUY",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "shares_delta": "10",
            "cash_delta": "-4",
            "realized_pnl": "0",
            "position_after": "10",
            "cash_after": "96",
        },
        {
            "ledger_id": "l-split",
            "event_type": "SPLIT",
            "market_slug": "spain",
            "event_slug": "world-cup",
            "token_id": "spain-yes",
            "token_side": "YES",
            "shares_delta": "2",
            "cash_delta": "-2",
            "realized_pnl": "0",
            "position_after": "12",
            "cash_after": "94",
        },
        {
            "ledger_id": "l-merge",
            "event_type": "MERGE",
            "market_slug": "spain",
            "event_slug": "world-cup",
            "token_id": "spain-yes",
            "token_side": "YES",
            "shares_delta": "-1",
            "cash_delta": "1",
            "realized_pnl": "0",
            "position_after": "11",
            "cash_after": "95",
        },
        {
            "ledger_id": "l-redeem",
            "event_type": "REDEEM",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "shares_delta": "-10",
            "cash_delta": "10",
            "realized_pnl": "6",
            "position_after": "1",
            "cash_after": "105",
        },
        {
            "ledger_id": "l-rebate",
            "event_type": "REBATE",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "shares_delta": "0",
            "cash_delta": "0.05",
            "realized_pnl": "0.05",
            "position_after": "1",
            "cash_after": "105.05",
        },
    ]

    report = build_ledger_cashflow_validation_report([], ledger, {"initial_capital": "100"})

    assert report["cashflow_formula"] == "SELL + REDEEM + MERGE + REBATE - BUY - SPLIT + unrealized position value"
    assert report["polymarket_special_event_counts"] == {"MERGE": 1, "REBATE": 1, "REDEEM": 1, "SPLIT": 1}
    assert report["polymarket_cashflow_total"] == "5.05"
    assert report["polymarket_portfolio_cash_at_risk"] == "1"
    assert report["polymarket_residual_position_count"] == 1
    assert report["polymarket_event_cash_at_risk"] == {"world-cup": "1"}


def test_ledger_cashflow_validation_marks_residual_positions_and_excludes_rewards() -> None:
    ledger = [
        {
            "ledger_id": "l-buy-france",
            "event_type": "BUY",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "shares_delta": "10",
            "cash_delta": "-4",
            "realized_pnl": "0",
            "position_after": "10",
            "cash_after": "96",
        },
        {
            "ledger_id": "l-sell-france",
            "event_type": "SELL",
            "market_slug": "france",
            "event_slug": "world-cup",
            "token_id": "france-yes",
            "token_side": "YES",
            "shares_delta": "-3",
            "cash_delta": "1.5",
            "realized_pnl": "0.3",
            "position_after": "7",
            "cash_after": "97.5",
        },
        {
            "ledger_id": "l-buy-spain",
            "event_type": "BUY",
            "market_slug": "spain",
            "event_slug": "world-cup",
            "token_id": "spain-yes",
            "token_side": "YES",
            "shares_delta": "5",
            "cash_delta": "-1",
            "realized_pnl": "0",
            "position_after": "12",
            "cash_after": "96.5",
            "mark_price": "0.25",
        },
        {
            "ledger_id": "l-reward",
            "event_type": "REWARD",
            "market_slug": "spain",
            "event_slug": "world-cup",
            "token_id": "spain-yes",
            "token_side": "YES",
            "shares_delta": "0",
            "cash_delta": "99",
            "realized_pnl": "0",
            "position_after": "12",
            "cash_after": "195.5",
        },
    ]

    report = build_ledger_cashflow_validation_report(
        [],
        ledger,
        {
            "initial_capital": "100",
            "mark_prices": {
                "france|world-cup|france-yes|YES": "0.60",
            },
        },
    )

    assert report["polymarket_cashflow_total"] == "-3.5"
    assert report["polymarket_excluded_total"] == "99"
    assert report["polymarket_unrealized_position_value"] == "5.45"
    assert report["polymarket_net_trading_pnl"] == "1.95"
    assert report["polymarket_portfolio_cash_at_risk"] == "3.8"
    assert report["polymarket_event_cash_at_risk"] == {"world-cup": "3.8"}
    assert report["polymarket_mark_price_count"] == 2
    assert report["polymarket_residual_position_count"] == 2
    assert report["polymarket_residual_positions"][0]["mark_value"] == "4.2"


def test_event_level_risk_report_reviews_single_outcome_without_event_snapshot() -> None:
    inputs = complete_inputs()
    inputs.pop("event_outcomes")
    inputs.pop("outcome_correlation")
    inputs["run"]["meta"].pop("event_outcomes")
    inputs["run"]["meta"].pop("outcome_correlation")
    inputs["run"]["meta"]["event_outcome_count"] = 60

    report = build_event_level_risk_report(
        inputs["run"],
        inputs["parameters"],
        inputs["orders"],
        inputs["trades"],
        inputs["ledger"],
        inputs,
    )

    assert report["status"] == READY
    assert report["risk_verdict"] == REVIEW
    assert report["observed_outcome_count"] == 1
    assert report["expected_outcome_count"] == 60
    assert report["probability_sum"]["status"] == REVIEW
    assert report["yes_no_complement"]["status"] == REVIEW
    assert report["outcome_correlation"]["status"] == REVIEW


def test_event_level_risk_report_exposes_incomplete_snapshot_and_observed_complement_deviation() -> None:
    inputs = complete_inputs()
    inputs["event_outcomes"] = [
        {"market_slug": "demo-a", "yes_probability": "0.60", "no_probability": "0.399"},
        {"market_slug": "demo-b", "yes_probability": "0.40", "no_probability": None},
    ]
    inputs["run"]["meta"]["event_outcomes"] = inputs["event_outcomes"]
    inputs["run"]["meta"]["event_context"] = {
        "schema_version": "event_outcome_snapshot_v1",
        "source": "quant.market_event_members+quant.market_token_block_close",
        "snapshot_to_block": 123,
        "event_outcome_count": 2,
        "priced_yes_outcome_count": 2,
        "priced_no_outcome_count": 1,
        "priced_pair_count": 1,
        "price_coverage_pct": "100",
    }

    report = build_event_level_risk_report(
        inputs["run"],
        inputs["parameters"],
        inputs["orders"],
        inputs["trades"],
        inputs["ledger"],
        inputs,
    )

    snapshot = report["event_snapshot"]
    assert report["risk_verdict"] == REVIEW
    assert snapshot["status"] == REVIEW
    assert snapshot["price_coverage_pct"] == "100"
    assert snapshot["priced_pair_count"] == 1
    assert "1/2 pairs" in snapshot["reason"]
    assert report["yes_no_complement"]["status"] == READY
    assert report["yes_no_complement"]["max_deviation"] == "0.001"


def test_run_data_quality_report_marks_bad_coverage_invalid() -> None:
    inputs = complete_inputs()
    data_quality = inputs["run"]["meta"]["actual_data_quality"]
    data_quality["rows"] = 0
    data_quality["warning_level"] = "BAD"
    data_quality["span_coverage_pct"] = "25"

    report = build_run_data_quality_report(data_quality, data_quality["fill_quality"])

    assert report["status"] == READY
    assert report["quality_verdict"] == "invalid"
    assert "row_count is zero" in report["reason"]


def test_nested_parameter_snapshot_is_accepted() -> None:
    inputs = complete_inputs()
    params = inputs["run"]["meta"]["parameter_snapshot"]
    inputs["run"]["meta"]["parameter_snapshot"] = {"parameters": params, "market_slug": "demo-market"}

    report = build_backtest_run_artifact_report(inputs)

    assert any(check["name"] == "parameter snapshot" and check["status"] == READY for check in report["checks"])


def test_missing_data_quality_is_missing() -> None:
    inputs = complete_inputs()
    inputs["run"]["meta"].pop("actual_data_quality")

    report = build_backtest_run_artifact_report(inputs)

    assert report["status"] == MISSING
    assert any(check["name"] == "data quality artifact" and check["status"] == MISSING for check in report["checks"])


def test_no_live_evidence_is_review_not_missing() -> None:
    inputs = complete_inputs()
    inputs["calibration_count"] = 0
    inputs["cost_calibration_count"] = 0
    inputs["calibration_rows"] = []
    inputs["cost_calibration_rows"] = []
    inputs["real_order_state_event_count"] = 0
    inputs["external_source_state_count"] = 0

    report = build_backtest_run_artifact_report(inputs)

    assert report["status"] == REVIEW
    assert report["credibility"]["status"] == REVIEW
    assert report["shadow_live_triangulation_report"]["triangulation_verdict"] == REVIEW
    assert report["shadow_live_triangulation_report"]["fill_model_suspect"] is True
    assert report["promotion_gate_report"]["promotion_verdict"] == "blocked"
    assert report["promotion_gate_report"]["paper_promotion_allowed"] is False
    assert "no live/shadow fill calibration evidence" in report["credibility"]["reason"]
    assert any(check["name"] == "live/shadow calibration evidence" and check["status"] == REVIEW for check in report["checks"])


def test_runtime_flags_reduce_run_credibility() -> None:
    inputs = complete_inputs()
    fill_quality = inputs["run"]["meta"]["actual_data_quality"]["fill_quality"]
    fill_quality["environment_flag_count"] = 1
    fill_quality["environment_flags"] = {"raw_replay_warning": 1}

    report = build_backtest_run_artifact_report(inputs)

    assert report["status"] == REVIEW
    assert report["credibility"]["status"] == REVIEW
    assert "runtime flags/anomalies=1" in report["credibility"]["reason"]
    assert any("Review run credibility" in action for action in report["next_actions"])


def test_execution_regime_report_splits_order_lifecycle_rows() -> None:
    report = build_execution_regime_report(complete_inputs()["orders"])

    assert report["status"] == READY
    assert report["dimensions"]["role"][0]["bucket"] == "maker"
    assert report["dimensions"]["role"][0]["submitted_count"] == 1
    assert report["dimensions"]["role"][0]["filled_count"] == 1
    assert report["dimensions"]["role"][0]["no_fill_count"] == 0
    assert report["dimensions"]["role"][0]["filled_notional_rate"] == "100"
    assert report["dimensions"]["role"][1]["bucket"] == "taker"
    assert report["dimensions"]["role"][1]["no_fill_count"] == 1
    assert report["dimensions"]["market_category"][0]["bucket"] == "sports"
    assert {row["bucket"] for row in report["dimensions"]["liquidity_bucket"]} == {"medium", "thin"}
    assert {row["bucket"] for row in report["dimensions"]["final_minute"]} == {"final_minute", "not_final_minute"}
    assert report["dimensions"]["event_outcome_count_bucket"][0]["bucket"] == "large_multi_21_plus"
    assert report["dimensions"]["no_fill_reason"][0]["bucket"] in {"NO_LIQUIDITY", "unknown"}


def test_maker_taker_execution_report_reviews_missing_taker_role() -> None:
    inputs = complete_inputs()
    for order in inputs["orders"]:
        order["role"] = "maker"
        order["meta"]["strategy_intent"]["role"] = "maker"

    report = build_maker_taker_execution_report(inputs["orders"])

    assert report["status"] == READY
    assert report["maker_taker_verdict"] == REVIEW
    assert report["execution_scope"] == "role_specific"
    assert report["missing_roles"] == ["taker"]
    assert "missing role coverage" in report["reason"]


def test_maker_queue_uncertainty_report_reviews_proxy_maker_fill() -> None:
    inputs = complete_inputs()

    report = build_maker_queue_uncertainty_report(inputs["orders"])

    assert report["status"] == READY
    assert report["queue_uncertainty_verdict"] == REVIEW
    assert report["maker_order_count"] == 1
    assert report["maker_filled_count"] == 1
    assert report["risk_orders"][0]["order_id"] == "o1"
    assert report["risk_orders"][0]["risk_level"] == "high"
    assert "no queue position evidence" in report["risk_orders"][0]["risk_reasons"][0]
    assert "50%" in report["risk_orders"][0]["risk_reasons"][1]


def test_maker_queue_uncertainty_report_ready_without_maker_orders() -> None:
    inputs = complete_inputs()
    for order in inputs["orders"]:
        order["role"] = "taker"
        order["meta"]["strategy_intent"]["role"] = "taker"

    report = build_maker_queue_uncertainty_report(inputs["orders"])

    assert report["status"] == READY
    assert report["queue_uncertainty_verdict"] == READY
    assert report["execution_scope"] == "not_applicable_no_maker_orders"
    assert report["risk_order_count"] == 0


def test_latency_profile_report_reviews_zero_latency_for_fak_cancel_sensitive_orders() -> None:
    inputs = complete_inputs()
    inputs["parameters"]["latency_blocks"] = 0
    inputs["parameters"]["latency_seconds"] = "0"
    for order in inputs["orders"]:
        order["latency_blocks"] = 0
        order["latency_seconds"] = "0"
    inputs["orders"][1]["no_fill_reason"] = "cancel_race"

    report = build_latency_profile_report(inputs["orders"], inputs["run"]["meta"]["actual_data_quality"]["fill_quality"], inputs["parameters"])

    assert report["status"] == READY
    assert report["latency_verdict"] == REVIEW
    assert report["latency_profile"] == "zero_latency"
    assert report["fak_fok_order_count"] == 1
    assert report["cancel_sensitive_order_count"] == 1
    assert report["stale_price_risk_count"] == 1
    assert any("configured latency is zero" in reason for reason in report["review_reasons"])
    assert "FAK/FOK" in report["risk_orders"][0]["risk_reasons"][0]


def test_slippage_regime_report_reviews_risky_zero_slippage() -> None:
    inputs = complete_inputs()
    risky = inputs["orders"][0]
    risky["requested_price"] = "0.65"
    risky["decision_price"] = "0.65"
    risky["slippage_cost"] = "0"
    risky["participation_rate"] = "60"
    risky["meta"]["context"]["liquidity_bucket"] = "thin"
    risky["meta"]["context"]["volatility_bucket"] = "high"
    risky["meta"]["context"]["time_to_expiry_seconds"] = 30
    inputs["parameters"]["latency_blocks"] = 1

    report = build_slippage_regime_report(inputs["orders"], inputs["run"]["meta"]["actual_data_quality"]["fill_quality"], inputs["parameters"])

    assert report["status"] == READY
    assert report["slippage_regime_verdict"] == REVIEW
    assert report["risk_order_count"] == 1
    risk = report["risk_orders"][0]
    assert "thin_liquidity" in risk["risk_regimes"]
    assert "trend_chase_60_70" in risk["risk_regimes"]
    assert "large_order" in risk["risk_regimes"]
    assert any("60-70c" in reason for reason in risk["risk_reasons"])


def test_slippage_regime_report_ready_for_conservative_stress() -> None:
    inputs = complete_inputs()
    orders = [inputs["orders"][0]]
    orders[0]["requested_price"] = "0.45"
    orders[0]["decision_price"] = "0.45"
    orders[0]["filled_notional"] = "50"
    orders[0]["actual_fill_notional"] = "50"
    orders[0]["requested_notional"] = "50"
    orders[0]["participation_rate"] = "10"
    orders[0]["slippage_cost"] = "0.05"
    orders[0]["meta"]["context"]["liquidity_bucket"] = "medium"
    orders[0]["meta"]["context"]["volatility_bucket"] = "low"
    orders[0]["meta"]["context"]["time_to_expiry_seconds"] = 3600
    inputs["parameters"]["slippage_bps"] = "10"
    inputs["parameters"]["adverse_slippage_cents"] = "0.02"

    report = build_slippage_regime_report(orders, inputs["run"]["meta"]["actual_data_quality"]["fill_quality"], inputs["parameters"])

    assert report["status"] == READY
    assert report["slippage_regime_verdict"] == READY
    assert report["risk_order_count"] == 0
    assert Decimal(report["avg_slippage_bps"]) == Decimal("10")


def test_environment_incident_report_reviews_platform_and_runtime_flags() -> None:
    fill_quality = dict(complete_inputs()["run"]["meta"]["actual_data_quality"]["fill_quality"])
    fill_quality["environment_flags"] = {"service_not_ready": 2}
    fill_quality["order_anomaly_flags"] = {"cancel_order_state_conflict": 1}
    fill_quality["order_anomaly_count"] = 1
    incidents = [
        {
            "incident_key": "clob-maintenance",
            "severity": "warning",
            "component": "clob_api",
            "title": "CLOB maintenance",
            "start_block": 10,
            "end_block": 20,
        }
    ]

    report = build_environment_incident_report(incidents, fill_quality)

    assert report["status"] == READY
    assert report["incident_verdict"] == REVIEW
    assert report["incident_count"] == 1
    assert report["severe_incident_count"] == 1
    assert report["environment_flags"]["platform_incident_clob_api_warning"] == 1
    assert report["environment_flags"]["service_not_ready"] == 2
    assert report["order_anomaly_count"] == 1
    assert any("platform incidents" in reason for reason in report["review_reasons"])


def test_external_signal_contract_report_accepts_aligned_canonical_event() -> None:
    inputs = complete_inputs()
    event = {
        "event_id": "weather-1",
        "event_type": "external_score_update",
        "observed_at": "2026-06-22T10:00:00Z",
        "observed_block": 50,
        "source": "manual_fixture",
        "latency_seconds": "2.5",
        "payload_hash": "abc123",
        "payload": {"home": 1, "away": 0},
        "resolution_source": "polymarket",
        "settlement_rule": "official result",
        "price_to_beat_source": "not_applicable",
        "oracle_source": "uma",
    }

    report = build_external_signal_contract_report(inputs["run"], inputs["parameters"], [event])

    assert report["status"] == READY
    assert report["signal_verdict"] == READY
    assert report["event_count"] == 1
    assert report["timestamp_alignment_status"] == READY
    assert report["resolution_compatibility_status"] == READY
    assert report["missing_required_field_count"] == 0
    assert report["source_counts"] == {"manual_fixture": 1}


def test_external_signal_contract_report_requires_declared_external_events() -> None:
    inputs = complete_inputs()
    parameters = dict(inputs["parameters"])
    parameters["requires_external_signal"] = True

    report = build_external_signal_contract_report(inputs["run"], parameters, [])

    assert report["status"] == MISSING
    assert report["signal_verdict"] == MISSING
    assert report["missing_required_field_count"] == 4
    assert "no external signal events" in report["reason"]


def test_tail_risk_report_flags_high_probability_tail_loss() -> None:
    report = build_tail_risk_report(complete_inputs()["trades"], complete_inputs()["parameters"])

    assert report["status"] == READY
    assert report["risk_verdict"] == REVIEW
    assert report["high_price_trade_count"] == 2
    assert report["max_single_loss"] == "-9.6"
    assert report["consecutive_loss_streak"] == 1
    assert report["payoff_distribution"]["min"] == "-9.6"
    assert report["payoff_distribution"]["median"] == "0.8"
    assert report["ruin_risk"]["losses_to_ruin"] == 105
    assert report["ruin_risk"]["gross_profit_buffer_loss_count"] == 2
    assert report["position_concentration"]["max_trade_notional"] == "10"
    assert report["position_concentration"]["max_trade_notional_pct_of_total"] == "44.24778761061946902654867257"
    assert report["stress"][2]["loss_count"] == 5
    assert report["stress"][2]["wipes_gross_profit"] is True


def test_prediction_quality_report_scores_brier_and_reviews_missing_baseline() -> None:
    inputs = complete_inputs()
    report = build_prediction_quality_report(inputs["run"], inputs["parameters"], inputs["orders"], inputs["trades"])

    assert report["status"] == READY
    assert report["prediction_verdict"] == READY
    assert report["sample_count"] == 3
    assert report["baseline_fallback_count"] == 0
    assert len(report["calibration_buckets"]) >= 2

    fallback_inputs = complete_inputs()
    for trade in fallback_inputs["trades"]:
        trade.pop("close_line_probability", None)
    fallback = build_prediction_quality_report(fallback_inputs["run"], fallback_inputs["parameters"], fallback_inputs["orders"], fallback_inputs["trades"])

    assert fallback["status"] == READY
    assert fallback["prediction_verdict"] == REVIEW
    assert fallback["baseline_fallback_count"] == 3
    assert "market close-line baseline missing" in fallback["reason"]


def test_market_lifecycle_report_tracks_resolution_contract() -> None:
    inputs = complete_inputs()

    report = build_market_lifecycle_report(inputs["run"], inputs["parameters"], inputs["trades"], inputs["ledger"])

    assert report["status"] == READY
    assert report["lifecycle_verdict"] == READY
    assert report["market_lifecycle_status"] == "closed"
    assert report["resolved_outcome"] == "YES"
    assert report["end_date"] == "2026-06-22T12:00:00Z"
    assert report["lifecycle_flags"]["closed"] is True
    assert report["lifecycle_flags"]["resolved"] is True


def test_market_lifecycle_report_reviews_missing_resolution_and_abnormal_flags() -> None:
    inputs = complete_inputs()
    params = dict(inputs["parameters"])
    params.pop("resolved_outcome")
    params.pop("end_date")
    params["market_invalid"] = True
    inputs["run"]["meta"]["parameter_snapshot"].pop("resolved_outcome")
    inputs["run"]["meta"]["parameter_snapshot"].pop("end_date")

    report = build_market_lifecycle_report(inputs["run"], params, inputs["trades"], inputs["ledger"])
    settlement = build_settlement_compatibility_report(inputs["run"], params, inputs["trades"], inputs["ledger"])

    assert report["lifecycle_verdict"] == REVIEW
    assert set(report["missing_lifecycle_fields"]) == {"resolved_outcome", "end_date"}
    assert report["lifecycle_flags"]["invalid"] is True
    assert "abnormal market lifecycle flag" in report["reason"]
    assert settlement["compatibility_verdict"] == REVIEW
    assert settlement["market_lifecycle_report"]["lifecycle_verdict"] == REVIEW


def test_settlement_compatibility_report_reviews_missing_sources() -> None:
    inputs = complete_inputs()
    params = dict(inputs["parameters"])
    for key in ("resolution_source", "settlement_rule", "price_to_beat_source", "oracle_source"):
        params.pop(key)
        inputs["run"]["meta"]["parameter_snapshot"].pop(key)

    report = build_settlement_compatibility_report(inputs["run"], params, inputs["trades"], inputs["ledger"])

    assert report["status"] == READY
    assert report["compatibility_verdict"] == REVIEW
    assert report["settlement_trade_count"] == 1
    assert report["settlement_ledger_count"] == 1
    assert set(report["missing_source_fields"]) == {
        "resolution_source",
        "settlement_rule",
        "price_to_beat_source",
        "oracle_source",
    }


def test_missing_raw_evidence_summary_is_missing() -> None:
    inputs = complete_inputs()
    fill_quality = inputs["run"]["meta"]["actual_data_quality"]["fill_quality"]
    fill_quality.pop("raw_evidence_summary")

    report = build_backtest_run_artifact_report(inputs)

    assert report["status"] == MISSING
    assert any(
        check["name"] == "fill quality artifact"
        and check["status"] == MISSING
        and "raw_evidence_summary" in check["detail"]
        for check in report["checks"]
    )


def test_run_artifact_markdown_contains_audit_table() -> None:
    markdown = backtest_run_artifact_report_to_markdown(build_backtest_run_artifact_report(complete_inputs()))

    assert "Backtest Run Artifact Audit" in markdown
    assert "parameter snapshot" in markdown
    assert "data_version: abc123" in markdown
    assert "Run Credibility" in markdown
    assert "Data Quality Report" in markdown
    assert "Reproducibility Report" in markdown
    assert "code_commit: abcdeffedcba" in markdown
    assert "Execution Regime Report" in markdown
    assert "Regime Coverage Report" in markdown
    assert "Tail Risk Report" in markdown
    assert "Prediction Quality Report" in markdown
    assert "Settlement Compatibility Report" in markdown
    assert "Event-Level Risk Report" in markdown
    assert "Event Stream Contract Report" in markdown
    assert "Joint Replay Plan Report" in markdown
    assert "Materialized Replay Cache Report" in markdown
    assert "Execution/Ledger Parity Report" in markdown
