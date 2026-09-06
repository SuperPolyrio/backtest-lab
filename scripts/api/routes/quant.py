# Extracted from PMQ: existing backtest route contracts are unchanged.
from __future__ import annotations

from datetime import datetime, timezone

from decimal import Decimal

import json

import logging

import os

import threading

import time

from typing import Any

from flask import Blueprint, Response, jsonify, request, stream_with_context

from quant.api.read_api import (  # noqa: E402
    get_backtest_calibration_orders,
    get_backtest_calibration_report,
    get_backtest_cost_calibration_report,
    get_backtest_cost_calibration_rows,
    get_backtest_equity,
    get_backtest_events,
    get_backtest_ledger,
    get_backtest_metrics,
    get_backtest_orders,
    get_backtest_position_timeline,
    get_backtest_replay,
    get_backtest_run,
    get_backtest_run_progress,
    get_backtest_runs,
    get_backtest_trades,
    get_execution_profile_overrides,
    get_block_close_prices,
    get_event_price_head,
    get_event_price_tile,
    get_event_execution_summaries,
    get_event_price_series,
    get_frontend_prices,
    get_market_execution_summaries,
    get_market_price_series,
    get_market_token_execution_summaries,
    get_price_build_status,
    get_production_parameter_staging,
    get_quant_event_members,
    get_quant_price_events,
    get_quant_price_markets,
)

from quant.backtest.backtest_engine import (
    cancel_backtest_run,
    create_and_execute_backtest,
    create_backtest_run,
)  # noqa: E402

from quant.backtest.benchmark_persistence import (  # noqa: E402
    create_benchmark_run,
    fail_benchmark_run,
    get_benchmark_artifacts,
    get_benchmark_rows,
    get_benchmark_run,
    list_benchmark_runs,
)

from quant.backtest.event_stream import build_joint_replay_execution_report  # noqa: E402

from quant.backtest.financial_finalization_service import (  # noqa: E402
    FinancialFinalizationServiceError,
    finalize_registered_backtest_run,
    get_backtest_financials,
)

from quant.backtest.market_settlement_service import (  # noqa: E402
    MarketSettlementServiceError,
    get_market_settlement_coverage,
    get_market_settlement_snapshot,
    parse_settlement_cutoff,
    resolve_market_settlements,
)

from quant.backtest.fill_only_v2_service import (  # noqa: E402
    FillOnlyV2Error,
    list_fill_only_v2_profiles,
    load_trade_tape_coverage,
    resolve_fill_only_v2_anchor,
    run_fill_only_v2_replay,
)

from quant.backtest.rust_kernel import rust_kernel_available  # noqa: E402

from quant.backtest.trade_only_v3.service import (  # noqa: E402
    FillOnlyV3Error,
    build_fill_only_v3_readiness,
    list_fill_only_v3_profiles,
    resolve_fill_only_v3_anchor,
    run_fill_only_v3_replay,
)

from quant.backtest.pml2.service import (  # noqa: E402
    Pml2ServiceError,
    build_pml2_readiness,
    run_pml2_maker_forecast,
    run_pml2_profile_matrix,
    run_pml2_replay,
)

from quant.backtest.pml2.profiles import list_pml2_profiles  # noqa: E402

from quant.backtest.pml2.service_v2 import (  # noqa: E402
    build_prediction_l2_v2_readiness,
    list_prediction_l2_v2_profiles,
    run_prediction_l2_v2_execution_matrix,
    run_prediction_l2_v2_gap_forecast,
    run_prediction_l2_v2_maker_forecast,
    run_prediction_l2_v2_replay,
)

from quant.backtest.guarded_executor import (  # noqa: E402
    build_guarded_executor_report,
    record_guarded_execution_intents,
)

from quant.backtest.joint_run import create_and_execute_joint_backtest  # noqa: E402

from quant.backtest.run_artifacts import (  # noqa: E402
    build_backtest_run_artifact_report,
    load_backtest_run_artifact_inputs,
    load_backtest_run_artifact_summary,
)

from quant.backtest.strategy_activation import (  # noqa: E402
    build_strategy_activation_decision,
    build_strategy_enable_state,
    insert_strategy_activation_decision,
    load_strategy_activation_decisions,
    load_strategy_enable_state,
    upsert_strategy_enable_state,
)

from quant.backtest.strategy_runner_guard import build_strategy_runner_plan  # noqa: E402

from quant.backtest.runners.benchmark import run_orderfilled_fast_accurate_benchmark  # noqa: E402

from quant.backtest.runners.coverage_build import build_replay_coverage  # noqa: E402

from quant.backtest.runners.selectors import (
    list_supported_universes,
    universe_spec_from_payload,
)  # noqa: E402

from quant.backtest.parameter_search_plan import build_parameter_search_plan  # noqa: E402

from quant.backtest.parameter_search_scheduler import (  # noqa: E402
    build_parameter_search_progress_report,
    cancel_parameter_search_batch,
    create_parameter_search_batch,
    get_parameter_search_batch,
    get_parameter_search_batch_items,
    list_parameter_search_batches,
    requeue_parameter_search_items,
    run_parameter_search_batch_worker,
)

from quant.backtest.production_parameter_staging import (
    update_production_parameter_staging_status,
)  # noqa: E402

from quant.core.db import PostgresSettings, postgres_connection  # noqa: E402

from quant.core.schema import create_schema  # noqa: E402

LOGGER = logging.getLogger(__name__)

def _parse_int_arg(name: str, default: int | None = None) -> int | None:
    raw = request.args.get(name)
    if raw in (None, ""):
        return default
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return default

def _parse_bool_arg(name: str, default: bool = False) -> bool:
    raw = request.args.get(name)
    if raw in (None, ""):
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

def _compact_backtest_run_row(row: dict[str, Any]) -> dict[str, Any]:
    """Keep run metadata useful for the workbench without shipping audit-sized JSON."""
    compact = dict(row)
    meta = compact.get("meta")
    if not isinstance(meta, dict):
        return compact

    compact_meta = dict(meta)
    data_quality = compact_meta.pop("actual_data_quality", None)
    if isinstance(data_quality, dict):
        scalar_summary = {
            key: value
            for key, value in data_quality.items()
            if value is None or isinstance(value, (str, int, float, bool))
        }
        scalar_summary.update(
            {
                "sections": sorted(str(key) for key in data_quality),
                "details_omitted": True,
            }
        )
        compact_meta["actual_data_quality_summary"] = scalar_summary
    compact["meta"] = compact_meta
    return compact

def _payload_bool(payload: dict[str, Any], *names: str, default: bool = False) -> bool:
    for name in names:
        if name in payload and payload.get(name) not in (None, ""):
            value = payload.get(name)
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}
    return default

def _payload_int(payload: dict[str, Any], name: str, default: int) -> int:
    value = payload.get(name)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _optional_decimal_payload(value: Any) -> Decimal | None:
    if value in (None, "", "null"):
        return None
    return Decimal(str(value))

def _optional_int_payload(value: Any) -> int | None:
    if value in (None, "", "null"):
        return None
    return int(value)

def _benchmark_profile_keys(payload: dict[str, Any]) -> tuple[str, ...]:
    replay_profiles = payload.get("replayProfiles") or payload.get("replay_profiles")
    execution_profiles = payload.get("executionProfiles") or payload.get(
        "execution_profiles"
    )
    if replay_profiles and execution_profiles:
        keys = tuple(
            f"{str(replay)}:{str(profile)}"
            for replay in replay_profiles
            for profile in execution_profiles
            if str(replay) in {"fast", "accurate"}
        )
        return keys or (
            "fast:optimistic",
            "fast:realistic",
            "fast:conservative",
            "fast:stress",
            "accurate:realistic",
        )
    bundle = payload.get("profileBundle") or payload.get("profile_bundle")
    if bundle in (None, "", "fast-vs-accurate"):
        return (
            "fast:optimistic",
            "fast:realistic",
            "fast:conservative",
            "fast:stress",
            "accurate:realistic",
        )
    if isinstance(bundle, list):
        return tuple(str(item) for item in bundle)
    return tuple(
        str(item).strip() for item in str(bundle).split(",") if str(item).strip()
    )

def _benchmark_request_parts(payload: dict[str, Any]) -> dict[str, Any]:
    universe_spec = universe_spec_from_payload(payload)
    strategy_payload = payload.get("strategySpec") or payload.get("strategy_spec") or {}
    if not isinstance(strategy_payload, dict):
        strategy_payload = {}
    profile_keys = _benchmark_profile_keys(payload)
    min_probability = Decimal(
        str(
            strategy_payload.get("minProbability")
            or strategy_payload.get("min_probability")
            or payload.get("minProbability")
            or "0.60"
        )
    )
    max_probability = Decimal(
        str(
            strategy_payload.get("maxProbability")
            or strategy_payload.get("max_probability")
            or payload.get("maxProbability")
            or "0.80"
        )
    )
    stake = Decimal(str(strategy_payload.get("stake") or payload.get("stake") or "10"))
    initial_capital = Decimal(
        str(
            strategy_payload.get("initialCapital")
            or strategy_payload.get("initial_capital")
            or payload.get("initialCapital")
            or "1000"
        )
    )
    max_daily_cost = _optional_decimal_payload(
        strategy_payload.get(
            "maxDailyCost",
            strategy_payload.get(
                "max_daily_cost",
                payload.get("maxDailyCost", payload.get("max_daily_cost", "20")),
            ),
        )
    )
    max_concurrent_positions = _optional_int_payload(
        strategy_payload.get(
            "maxConcurrentPositions",
            strategy_payload.get(
                "max_concurrent_positions",
                payload.get(
                    "maxConcurrentPositions", payload.get("max_concurrent_positions", 2)
                ),
            ),
        )
    )
    max_daily_trades = _optional_int_payload(
        strategy_payload.get(
            "maxDailyTrades",
            strategy_payload.get(
                "max_daily_trades",
                payload.get("maxDailyTrades", payload.get("max_daily_trades")),
            ),
        )
    )
    parameters = {
        "limit": int(universe_spec.limit),
        "universe": {
            "universeName": universe_spec.universe_name,
            "universeType": universe_spec.universe_type,
            "limit": int(universe_spec.limit),
            "marketIds": list(universe_spec.market_ids or []),
            "marketSlugs": list(universe_spec.market_slugs or []),
            "eventSlug": universe_spec.event_slug,
            "category": universe_spec.category,
            "startDate": universe_spec.start_date,
            "endDate": universe_spec.end_date,
            "requireResolved": bool(universe_spec.require_resolved),
            "requireOrderfilledRows": bool(universe_spec.require_orderfilled_rows),
        },
        "min_probability": str(min_probability),
        "max_probability": str(max_probability),
        "snapshot_hours_before_start": "1",
        "signal_lookback_hours": "24",
        "window_start_hours": "1",
        "window_end_hours": "0",
        "initial_capital": str(initial_capital),
        "stake": str(stake),
        "max_daily_cost": str(max_daily_cost) if max_daily_cost is not None else None,
        "max_concurrent_positions": max_concurrent_positions,
        "max_daily_trades": max_daily_trades,
        "yes_only": True,
        "sort_by": "probability_desc",
    }
    return {
        "universe_spec": universe_spec,
        "profile_keys": profile_keys,
        "parameters": parameters,
        "profiles": {"requested": list(profile_keys)},
        "force_block_replay_backfill": bool(
            payload.get("forceBlockReplayBackfill")
            or payload.get("force_block_replay_backfill")
        ),
        "min_probability": min_probability,
        "max_probability": max_probability,
        "stake": stake,
        "initial_capital": initial_capital,
        "max_daily_cost": max_daily_cost,
        "max_concurrent_positions": max_concurrent_positions,
        "max_daily_trades": max_daily_trades,
    }

def _run_benchmark_job(benchmark_id: int, payload: dict[str, Any]) -> None:
    try:
        parts = _benchmark_request_parts(payload)
        with postgres_connection(PostgresSettings(), readonly=False) as conn:
            create_schema(conn)
            run_orderfilled_fast_accurate_benchmark(
                universe_spec=parts["universe_spec"],
                persist_conn=conn,
                benchmark_id=int(benchmark_id),
                force_block_replay_backfill=parts["force_block_replay_backfill"],
                min_probability=parts["min_probability"],
                max_probability=parts["max_probability"],
                stake=parts["stake"],
                initial_capital=parts["initial_capital"],
                max_daily_cost=parts["max_daily_cost"],
                max_concurrent_positions=parts["max_concurrent_positions"],
                max_daily_trades=parts["max_daily_trades"],
                profile_keys=parts["profile_keys"],
            )
    except Exception as exc:  # pragma: no cover - exercised by live API smoke
        LOGGER.exception(
            "quant backtest benchmark background job failed benchmark_id=%s",
            benchmark_id,
        )
        try:
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                fail_benchmark_run(conn, benchmark_id=int(benchmark_id), error=str(exc))
                conn.commit()
        except Exception:
            LOGGER.exception(
                "quant backtest benchmark failed-state write failed benchmark_id=%s",
                benchmark_id,
            )

def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value

def _camel_row(row: dict[str, Any]) -> dict[str, Any]:
    mapping = {
        "workspace_id": "workspaceId",
        "share_id": "shareId",
        "created_at": "createdAt",
        "updated_at": "updatedAt",
        "token_id": "tokenId",
        "market_id": "marketId",
        "market_slug": "marketSlug",
        "token_side": "tokenSide",
        "ts_minute": "tsMinute",
        "block_number": "blockNumber",
        "block_hash": "blockHash",
        "block_time": "blockTime",
        "boundary_sha256": "boundarySha256",
        "at_or_before": "atOrBefore",
        "header_rows_sha256": "headerRowsSha256",
        "close_price": "closePrice",
        "yes_probability_close": "yesProbabilityClose",
        "vwap_price": "vwapPrice",
        "yes_probability_vwap": "yesProbabilityVwap",
        "close_raw_price": "closeRawPrice",
        "close_price_source": "closePriceSource",
        "close_tx_hash": "closeTxHash",
        "close_log_index": "closeLogIndex",
        "trade_count": "tradeCount",
        "raw_trade_count": "rawTradeCount",
        "first_block": "firstBlock",
        "last_block": "lastBlock",
        "block_row_count": "blockRowCount",
        "market_count": "marketCount",
        "classification_counts": "classificationCounts",
        "classification_reason": "classificationReason",
        "completion_status": "completionStatus",
        "cutoff_ts": "cutoffTs",
        "cutoff_block_boundary": "cutoffBlockBoundary",
        "evidence_grade": "evidenceGrade",
        "evidence_policy": "evidencePolicy",
        "payout_by_token": "payoutByToken",
        "protocol_finalized_at": "protocolFinalizedAt",
        "protocol_finalized_block": "protocolFinalizedBlock",
        "record_sha256": "recordSha256",
        "settlement_code": "settlementCode",
        "settlement_event_id": "settlementEventId",
        "settlement_outcome": "settlementOutcome",
        "settlement_source": "settlementSource",
        "settlement_tx_hash": "settlementTxHash",
        "required_settlement_evidence": "requiredSettlementEvidence",
        "actual_settlement_evidence_grade": "actualSettlementEvidenceGrade",
        "source_scope": "sourceScope",
        "token_count": "tokenCount",
        "maker_amount": "makerAmount",
        "taker_amount": "takerAmount",
        "maker_share": "makerShare",
        "taker_share": "takerShare",
        "side_bucket": "sideBucket",
        "liquidity_bucket": "liquidityBucket",
        "side_bucket_counts": "sideBucketCounts",
        "dominant_side_bucket": "dominantSideBucket",
        "dominant_liquidity_bucket": "dominantLiquidityBucket",
        "anomaly_count": "anomalyCount",
        "calibration_id": "calibrationId",
        "cost_calibration_id": "costCalibrationId",
        "sample_id": "sampleId",
        "observed_at": "observedAt",
        "observed_block": "observedBlock",
        "simulated_order_id": "simulatedOrderId",
        "live_order_id": "liveOrderId",
        "simulated_status": "simulatedStatus",
        "live_status": "liveStatus",
        "status_error": "statusError",
        "simulated_fill_price": "simulatedFillPrice",
        "live_fill_price": "liveFillPrice",
        "price_error": "priceError",
        "simulated_fill_size": "simulatedFillSize",
        "live_fill_size": "liveFillSize",
        "size_error": "sizeError",
        "simulated_slippage": "simulatedSlippage",
        "live_slippage": "liveSlippage",
        "slippage_error": "slippageError",
        "simulated_fee": "simulatedFee",
        "live_fee": "liveFee",
        "fee_error": "feeError",
        "simulated_rebate": "simulatedRebate",
        "live_rebate": "liveRebate",
        "rebate_error": "rebateError",
        "simulated_cash_delta": "simulatedCashDelta",
        "live_cash_delta": "liveCashDelta",
        "cash_error": "cashError",
        "simulated_position_delta": "simulatedPositionDelta",
        "live_position_delta": "livePositionDelta",
        "position_error": "positionError",
        "simulated_latency_seconds": "simulatedLatencySeconds",
        "live_latency_seconds": "liveLatencySeconds",
        "latency_error_seconds": "latencyErrorSeconds",
        "liquidity_bucket": "liquidityBucket",
        "volatility_bucket": "volatilityBucket",
        "time_to_expiry_bucket": "timeToExpiryBucket",
        "market_category": "marketCategory",
        "final_minute": "finalMinute",
        "event_outcome_count_bucket": "eventOutcomeCountBucket",
        "submitted_count": "submittedCount",
        "filled_count": "filledCount",
        "partial_fill_count": "partialFillCount",
        "no_fill_count": "noFillCount",
        "rejected_count": "rejectedCount",
        "fill_rate": "fillRate",
        "filled_notional_rate": "filledNotionalRate",
        "sample_count": "sampleCount",
        "status_error_count": "statusErrorCount",
        "status_error_rate": "statusErrorRate",
        "avg_price_error": "avgPriceError",
        "max_price_error": "maxPriceError",
        "avg_size_error": "avgSizeError",
        "avg_slippage_error": "avgSlippageError",
        "avg_fee_error": "avgFeeError",
        "avg_rebate_error": "avgRebateError",
        "avg_cash_error": "avgCashError",
        "avg_position_error": "avgPositionError",
        "avg_latency_error_seconds": "avgLatencyErrorSeconds",
        "simulated_amount": "simulatedAmount",
        "live_amount": "liveAmount",
        "amount_error": "amountError",
        "simulated_count": "simulatedCount",
        "live_count": "liveCount",
        "total_amount_error": "totalAmountError",
        "avg_amount_error": "avgAmountError",
        "missing_live_count": "missingLiveCount",
        "missing_simulated_count": "missingSimulatedCount",
        "event_type_summary": "eventTypeSummary",
        "verdict_counts": "verdictCounts",
        "role_counts": "roleCounts",
        "side_counts": "sideCounts",
        "liquidity_bucket_counts": "liquidityBucketCounts",
        "volatility_bucket_counts": "volatilityBucketCounts",
        "time_to_expiry_bucket_counts": "timeToExpiryBucketCounts",
        "requires_recalibration": "requiresRecalibration",
        "trust_status": "trustStatus",
        "trust_reason": "trustReason",
        "run_id": "runId",
        "started_at": "startedAt",
        "finished_at": "finishedAt",
        "requested_from_ts": "requestedFromTs",
        "requested_to_ts": "requestedToTs",
        "requested_from_block": "requestedFromBlock",
        "requested_to_block": "requestedToBlock",
        "markets_total": "marketsTotal",
        "markets_complete": "marketsComplete",
        "rows_written": "rowsWritten",
        "error_count": "errorCount",
        "last_error": "lastError",
        "price_source": "priceSource",
        "backtest_engine": "backtestEngine",
        "from_ts": "fromTs",
        "to_ts": "toTs",
        "from_block": "fromBlock",
        "to_block": "toBlock",
        "rows_processed": "rowsProcessed",
        "created_at": "createdAt",
        "entry_threshold": "entryThreshold",
        "exit_threshold": "exitThreshold",
        "stop_loss": "stopLoss",
        "take_profit": "takeProfit",
        "max_holding_bars": "maxHoldingBars",
        "initial_capital": "initialCapital",
        "position_size": "positionSize",
        "fee_bps": "feeBps",
        "maker_fee_bps": "makerFeeBps",
        "taker_fee_bps": "takerFeeBps",
        "maker_rebate_bps": "makerRebateBps",
        "slippage_bps": "slippageBps",
        "liquidity_cap_pct": "liquidityCapPct",
        "max_position_notional": "maxPositionNotional",
        "min_fill_pct": "minFillPct",
        "execution_price_mode": "executionPriceMode",
        "execution_profile": "executionProfile",
        "pml2_audit_mode": "pml2AuditMode",
        "order_role": "orderRole",
        "latency_blocks": "latencyBlocks",
        "adverse_slippage_cents": "adverseSlippageCents",
        "fill_probability_haircut_pct": "fillProbabilityHaircutPct",
        "latency_seconds": "latencySeconds",
        "max_book_staleness_seconds": "maxBookStalenessSeconds",
        "allow_partial_fill": "allowPartialFill",
        "min_fill_size": "minFillSize",
        "reject_on_stale_book": "rejectOnStaleBook",
        "final_valuation_mode": "finalValuationMode",
        "max_entry_price": "maxEntryPrice",
        "min_exit_price": "minExitPrice",
        "buy_limit_price": "buyLimitPrice",
        "sell_limit_price": "sellLimitPrice",
        "settlement_value": "settlementValue",
        "gas_cost_per_order": "gasCostPerOrder",
        "settlement_cost": "settlementCost",
        "redeem_cost": "redeemCost",
        "capital_cost_bps": "capitalCostBps",
        "parameter_fingerprint": "parameterFingerprint",
        "parameter_snapshot": "parameterSnapshot",
        "metric_key": "metricKey",
        "metric_name": "metricName",
        "metric_group": "metricGroup",
        "formatted_value": "formattedValue",
        "sort_order": "sortOrder",
        "batch_id": "batchId",
        "item_id": "itemId",
        "item_index": "itemIndex",
        "item_key": "itemKey",
        "planned_run_count": "plannedRunCount",
        "queued_count": "queuedCount",
        "running_count": "runningCount",
        "succeeded_count": "succeededCount",
        "failed_count": "failedCount",
        "retryable_count": "retryableCount",
        "max_attempts": "maxAttempts",
        "attempt_count": "attemptCount",
        "worker_id": "workerId",
        "claimed_at": "claimedAt",
        "result_row": "resultRow",
        "request_payload": "requestPayload",
        "point_index": "pointIndex",
        "x_axis": "xAxis",
        "x_value": "xValue",
        "drawdown_pct": "drawdownPct",
        "cumulative_return": "cumulativeReturn",
        "trade_id": "tradeId",
        "entry_order_id": "entryOrderId",
        "exit_order_id": "exitOrderId",
        "order_id": "orderId",
        "signal_index": "signalIndex",
        "signal_x": "signalX",
        "submit_x": "submitX",
        "decision_price": "decisionPrice",
        "requested_price": "requestedPrice",
        "order_type": "orderType",
        "no_fill_reason": "noFillReason",
        "ledger_id": "ledgerId",
        "event_index": "eventIndex",
        "event_type": "eventType",
        "shares_delta": "sharesDelta",
        "cash_delta": "cashDelta",
        "position_after": "positionAfter",
        "cash_after": "cashAfter",
        "realized_pnl": "realizedPnl",
        "entry_x": "entryX",
        "exit_x": "exitX",
        "entry_price": "entryPrice",
        "exit_price": "exitPrice",
        "requested_notional": "requestedNotional",
        "filled_notional": "filledNotional",
        "requested_size": "requestedSize",
        "filled_size": "filledSize",
        "unfilled_size": "unfilledSize",
        "fill_pct": "fillPct",
        "fill_status": "fillStatus",
        "book_snapshot_id": "bookSnapshotId",
        "snapshot_version": "snapshotVersion",
        "staleness_seconds": "stalenessSeconds",
        "staleness_blocks": "stalenessBlocks",
        "avg_fill_price": "avgFillPrice",
        "fill_probability": "fillProbability",
        "block_volume": "blockVolume",
        "trade_count": "tradeCount",
        "available_notional": "availableNotional",
        "execution_source": "executionSource",
        "fee_cost": "feeCost",
        "rebate_cost": "rebateCost",
        "slippage_cost": "slippageCost",
        "execution_cost": "executionCost",
        "pnl_pct": "pnlPct",
        "holding_bars": "holdingBars",
        "exit_reason": "exitReason",
        "block_rows": "blockRows",
        "frontend_rows": "frontendRows",
        "first_block": "firstBlock",
        "last_block": "lastBlock",
        "latest_block_price": "latestBlockPrice",
        "latest_block_at": "latestBlockAt",
        "first_ts": "firstTs",
        "last_ts": "lastTs",
        "latest_frontend_price": "latestFrontendPrice",
        "latest_frontend_at": "latestFrontendAt",
        "market_title": "marketTitle",
        "item_kind": "itemKind",
        "event_id": "eventId",
        "event_slug": "eventSlug",
        "event_title": "eventTitle",
        "event_category": "eventCategory",
        "event_subcategory": "eventSubcategory",
        "event_image_url": "eventImageUrl",
        "event_icon_url": "eventIconUrl",
        "event": "event",
        "members": "members",
        "outcome_count": "outcomeCount",
        "total_members": "totalMembers",
        "ready_members": "readyMembers",
        "orderfilled_rows": "orderfilledRows",
        "grouping_confidence": "groupingConfidence",
        "coverage_status": "coverageStatus",
        "outcome_key": "outcomeKey",
        "condition_id": "conditionId",
        "end_date": "endDate",
        "outcome_index": "outcomeIndex",
        "outcome_label": "outcomeLabel",
        "buy_yes_token_id": "buyYesTokenId",
        "buy_yes_token_side": "buyYesTokenSide",
        "buy_yes_label": "buyYesLabel",
        "buy_yes_price": "buyYesPrice",
        "buy_no_token_id": "buyNoTokenId",
        "buy_no_token_side": "buyNoTokenSide",
        "buy_no_label": "buyNoLabel",
        "buy_no_price": "buyNoPrice",
        "first_x": "firstX",
        "last_x": "lastX",
        "latest_price": "latestPrice",
        "complement_rows": "complementRows",
        "complement_first_x": "complementFirstX",
        "complement_last_x": "complementLastX",
        "complement_latest_price": "complementLatestPrice",
        "complement_points": "complementPoints",
        "x_axis": "xAxis",
        "yes_probability_close": "yesProbabilityClose",
        "yes_probability_vwap": "yesProbabilityVwap",
        "top_n": "topN",
        "max_points": "maxPoints",
        "source_limit": "sourceLimit",
        "benchmark_id": "benchmarkId",
        "universe_type": "universeType",
        "universe_name": "universeName",
        "strategy_name": "strategyName",
        "data_version": "dataVersion",
        "row_index": "rowIndex",
        "fast_status": "fastStatus",
        "accurate_status": "accurateStatus",
        "fast_pnl": "fastPnl",
        "accurate_pnl": "accuratePnl",
        "pnl_diff": "pnlDiff",
        "fast_fill_block": "fastFillBlock",
        "accurate_fill_block": "accurateFillBlock",
        "data_quality": "dataQuality",
        "actual_data_quality_summary": "actualDataQualitySummary",
        "coverage_pct": "coveragePct",
        "quality_source": "qualitySource",
        "ready_outcomes": "readyOutcomes",
        "expected_outcomes": "expectedOutcomes",
        "visible_observations": "visibleObservations",
        "raw_observations": "rawObservations",
        "expected_observations": "expectedObservations",
        "expected_observations_reason": "expectedObservationsReason",
        "max_gap_width": "maxGapWidth",
        "lagging_outcomes": "laggingOutcomes",
        "duplicate_x_count": "duplicateXCount",
        "out_of_order_count": "outOfOrderCount",
        "latest_timestamp": "latestTimestamp",
        "universe_complete": "universeComplete",
        "normalization_eligibility": "normalizationEligibility",
        "backtest_eligibility": "backtestEligibility",
        "details_omitted": "detailsOmitted",
        "artifact_key": "artifactKey",
        "artifact_kind": "artifactKind",
        "staging_id": "stagingId",
        "staging_allowed": "stagingAllowed",
        "robustness_verdict": "robustnessVerdict",
        "default_action": "defaultAction",
        "parameter_search_results": "parameterSearchResults",
        "batch_id": "batchId",
        "item_id": "itemId",
        "item_index": "itemIndex",
        "item_key": "itemKey",
        "strategy_version": "strategyVersion",
        "planned_run_count": "plannedRunCount",
        "queued_count": "queuedCount",
        "running_count": "runningCount",
        "succeeded_count": "succeededCount",
        "failed_count": "failedCount",
        "canceled_count": "canceledCount",
        "retryable_count": "retryableCount",
        "completion_pct": "completionPct",
        "status_counts": "statusCounts",
        "parameter_fingerprint": "parameterFingerprint",
        "evidence_mode": "evidenceMode",
        "request_payload": "requestPayload",
        "result_row": "resultRow",
        "attempt_count": "attemptCount",
        "max_attempts": "maxAttempts",
        "worker_id": "workerId",
        "claimed_at": "claimedAt",
        "started_at": "startedAt",
        "finished_at": "finishedAt",
        "created_by": "createdBy",
        "processed_count": "processedCount",
        "error_count": "errorCount",
        "processed_items": "processedItems",
        "requeued_item_count": "requeuedItemCount",
        "canceled_item_count": "canceledItemCount",
        "approved_by": "approvedBy",
        "approved_at": "approvedAt",
        "reviewed_by": "reviewedBy",
        "reviewed_at": "reviewedAt",
        "review_note": "reviewNote",
        "updated_at": "updatedAt",
        "next_actions": "nextActions",
        "max_score": "maxScore",
        "run_credibility_status": "runCredibilityStatus",
        "run_credibility_score": "runCredibilityScore",
        "run_credibility_reason": "runCredibilityReason",
        "summary_status": "summaryStatus",
        "deep_audit_status": "deepAuditStatus",
        "audit_mode": "auditMode",
        "equity_points": "equityPoints",
        "ledger_events": "ledgerEvents",
        "replayable_rows": "replayableRows",
        "timestamped_rows": "timestampedRows",
        "source_timestamp_coverage_pct": "sourceTimestampCoveragePct",
        "data_quality_report": "dataQualityReport",
        "reproducibility_report": "reproducibilityReport",
        "regime_coverage_report": "regimeCoverageReport",
        "shadow_live_triangulation_report": "shadowLiveTriangulationReport",
        "promotion_gate_report": "promotionGateReport",
        "paper_live_evidence_gate_report": "paperLiveEvidenceGateReport",
        "paper_live_evidence_gate_status": "paperLiveEvidenceGateStatus",
        "paper_live_paper_allowed": "paperLivePaperAllowed",
        "paper_live_live_allowed": "paperLiveLiveAllowed",
        "promotion_gate_status": "promotionGateStatus",
        "promotion_verdict": "promotionVerdict",
        "production_promotion_allowed": "productionPromotionAllowed",
        "paper_promotion_allowed": "paperPromotionAllowed",
        "allowed_next_modes": "allowedNextModes",
        "blocked_reasons": "blockedReasons",
        "review_reasons": "reviewReasons",
        "missing_reasons": "missingReasons",
        "do_not_promote_when_fill_model_suspect": "doNotPromoteWhenFillModelSuspect",
        "require_shadow_live_triangulation": "requireShadowLiveTriangulation",
        "require_reproducible_materialized_input": "requireReproducibleMaterializedInput",
        "regime_specific_requires_review_before_live": "regimeSpecificRequiresReviewBeforeLive",
        "triangulation_verdict": "triangulationVerdict",
        "fill_model_suspect": "fillModelSuspect",
        "backtest_sample_count": "backtestSampleCount",
        "shadow_live_sample_count": "shadowLiveSampleCount",
        "cost_sample_count": "costSampleCount",
        "real_order_state_event_count": "realOrderStateEventCount",
        "external_source_state_count": "externalSourceStateCount",
        "executor_status": "executorStatus",
        "input_runner_status": "inputRunnerStatus",
        "record_intent": "recordIntent",
        "dry_run": "dryRun",
        "event_source": "eventSource",
        "planned_action_count": "plannedActionCount",
        "recorded_event_count": "recordedEventCount",
        "runner_contract": "runnerContract",
        "executor_contract": "executorContract",
        "event_templates": "eventTemplates",
        "planned_action": "plannedAction",
        "real_order_submission": "realOrderSubmission",
        "requires_external_order_adapter": "requiresExternalOrderAdapter",
        "drift_summary": "driftSummary",
        "total_cost_amount_error": "totalCostAmountError",
        "avg_cost_amount_error": "avgCostAmountError",
        "missing_live_cost_count": "missingLiveCostCount",
        "missing_simulated_cost_count": "missingSimulatedCostCount",
        "execution_ledger_parity_report": "executionLedgerParityReport",
        "event_level_risk_report": "eventLevelRiskReport",
        "event_stream_report": "eventStreamReport",
        "event_stream_status": "eventStreamStatus",
        "event_stream_schema_version": "eventStreamSchemaVersion",
        "event_stream_event_count": "eventStreamEventCount",
        "schema_version": "schemaVersion",
        "type_counts": "typeCounts",
        "x_axes": "xAxes",
        "missing_contract": "missingContract",
        "required_event_classes": "requiredEventClasses",
        "event_level_risk_status": "eventLevelRiskStatus",
        "event_level_risk_verdict": "eventLevelRiskVerdict",
        "event_outcome_count": "eventOutcomeCount",
        "event_probability_sum": "eventProbabilitySum",
        "portfolio_cash_at_risk": "portfolioCashAtRisk",
        "risk_verdict": "riskVerdict",
        "observed_outcome_count": "observedOutcomeCount",
        "expected_outcome_count": "expectedOutcomeCount",
        "probability_sum": "probabilitySum",
        "yes_no_complement": "yesNoComplement",
        "outcome_correlation": "outcomeCorrelation",
        "portfolio_risk": "portfolioRisk",
        "outcome_exposures": "outcomeExposures",
        "cash_at_risk": "cashAtRisk",
        "cash_released": "cashReleased",
        "net_shares_delta": "netSharesDelta",
        "checked_count": "checkedCount",
        "bad_count": "badCount",
        "bad_rows": "badRows",
        "max_deviation": "maxDeviation",
        "sample_count": "sampleCount",
        "max_abs_correlation": "maxAbsCorrelation",
        "portfolio_cash_at_risk_pct_of_capital": "portfolioCashAtRiskPctOfCapital",
        "total_filled_notional": "totalFilledNotional",
        "max_outcome_cash_at_risk": "maxOutcomeCashAtRisk",
        "execution_ledger_parity_status": "executionLedgerParityStatus",
        "execution_ledger_parity_verdict": "executionLedgerParityVerdict",
        "parity_order_missing_field_count": "parityOrderMissingFieldCount",
        "parity_ledger_missing_field_count": "parityLedgerMissingFieldCount",
        "parity_verdict": "parityVerdict",
        "order_schema": "orderSchema",
        "ledger_schema": "ledgerSchema",
        "live_event_contract": "liveEventContract",
        "live_evidence": "liveEvidence",
        "required_fields": "requiredFields",
        "missing_field_count": "missingFieldCount",
        "missing_by_field": "missingByField",
        "rows_with_missing": "rowsWithMissing",
        "terminal_no_fill_without_reason": "terminalNoFillWithoutReason",
        "event_types": "eventTypes",
        "terminal_statuses": "terminalStatuses",
        "reproducibility_status": "reproducibilityStatus",
        "reproducibility_verdict": "reproducibilityVerdict",
        "code_commit": "codeCommit",
        "code_dirty": "codeDirty",
        "code_source": "codeSource",
        "artifact_schema_version": "artifactSchemaVersion",
        "artifact_manifest": "artifactManifest",
        "model_versions": "modelVersions",
        "fill_model": "fillModel",
        "fill_model_version": "fillModelVersion",
        "fee_model_version": "feeModelVersion",
        "slippage_model_version": "slippageModelVersion",
        "parameter_snapshot_status": "parameterSnapshotStatus",
        "gap_report": "gapReport",
        "replay_command": "replayCommand",
        "missing_fields": "missingFields",
        "manifest_missing": "manifestMissing",
        "quality_verdict": "qualityVerdict",
        "warning_level": "warningLevel",
        "block_range": "blockRange",
        "row_count": "rowCount",
        "gap_count": "gapCount",
        "max_gap": "maxGap",
        "stale_count": "staleCount",
        "source_mix": "sourceMix",
        "fallback_count": "fallbackCount",
        "dedupe_stats": "dedupeStats",
        "duplicate_count": "duplicateCount",
        "jump_count": "jumpCount",
        "chart_quality": "chartQuality",
        "quality_source": "qualitySource",
        "gap_intervals": "gapIntervals",
        "jump_points": "jumpPoints",
        "gap_before": "gapBefore",
        "gap_width": "gapWidth",
        "jump_delta": "jumpDelta",
        "jump_direction": "jumpDirection",
        "gap_threshold": "gapThreshold",
        "jump_threshold": "jumpThreshold",
        "gap_detection": "gapDetection",
        "jump_detection": "jumpDetection",
        "raw_rows": "rawRows",
        "rendered_rows": "renderedRows",
        "is_raw": "isRaw",
        "is_imputed": "isImputed",
        "stale_seconds": "staleSeconds",
        "stale_threshold": "staleThreshold",
        "latest_timestamp": "latestTimestamp",
        "probability_snapshot": "probabilitySnapshot",
        "raw_sum": "rawSum",
        "outcome_count": "outcomeCount",
        "scope_complete": "scopeComplete",
        "normalization_applied": "normalizationApplied",
        "overround": "overround",
        "residual": "residual",
        "missing_outcomes": "missingOutcomes",
        "snapshot_method": "snapshotMethod",
        "as_of_timestamp": "asOfTimestamp",
        "lagging_outcomes": "laggingOutcomes",
        "dataset_snapshot": "datasetSnapshot",
        "dataset_fingerprint": "datasetFingerprint",
        "snapshot_version": "snapshotVersion",
        "raw_point_count": "rawPointCount",
        "rendered_point_count": "renderedPointCount",
        "from_x": "fromX",
        "to_x": "toX",
        "span_coverage_pct": "spanCoveragePct",
        "raw_event_count": "rawEventCount",
        "candidate_event_count": "candidateEventCount",
        "consumed_event_count": "consumedEventCount",
        "environment_flag_count": "environmentFlagCount",
        "order_anomaly_count": "orderAnomalyCount",
        "calibration_samples": "calibrationSamples",
        "cost_calibration_samples": "costCalibrationSamples",
        "real_order_state_events": "realOrderStateEvents",
        "external_source_states": "externalSourceStates",
        "schema_version": "schemaVersion",
        "replay_id": "replayId",
        "lifecycle_type": "lifecycleType",
        "source_table": "sourceTable",
        "evidence_level": "evidenceLevel",
        "size_usd": "sizeUsd",
        "fill_id": "fillId",
        "current_x": "currentX",
        "total_rows": "totalRows",
        "eta_seconds": "etaSeconds",
    }
    result = {
        mapping.get(key, key): _camel_value(value, mapping)
        for key, value in row.items()
    }
    meta = result.get("meta")
    if isinstance(meta, dict):
        fingerprint = meta.get("parameterFingerprint") or meta.get(
            "parameter_fingerprint"
        )
        snapshot = meta.get("parameterSnapshot") or meta.get("parameter_snapshot")
        if fingerprint and "parameterFingerprint" not in result:
            result["parameterFingerprint"] = fingerprint
        if snapshot and "parameterSnapshot" not in result:
            result["parameterSnapshot"] = snapshot
    return result

def _camel_value(value: Any, mapping: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {
            mapping.get(key, key): _camel_value(inner, mapping)
            for key, inner in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_camel_value(item, mapping) for item in value]
    return _json_value(value)

def _backtest_idempotency_key(payload: dict[str, Any]) -> str:
    header_value = str(request.headers.get("Idempotency-Key") or "").strip()
    context = payload.get("execution_context") or payload.get("executionContext") or {}
    payload_value = (
        str(
            context.get("idempotency_key") or context.get("idempotencyKey") or ""
        ).strip()
        if isinstance(context, dict)
        else ""
    )
    return (header_value or payload_value)[:160]

def _existing_idempotent_backtest(conn: Any, key: str) -> dict[str, Any] | None:
    if not key:
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_runs
            WHERE meta -> 'execution_context' ->> 'idempotency_key' = %s
            ORDER BY run_id DESC
            LIMIT 1
            """,
            (key,),
        )
        row = cur.fetchone()
    return dict(row) if row else None

def create_quant_blueprint(helpers: dict) -> Blueprint:
    bp = Blueprint("quant_routes", __name__, url_prefix="/quant")
    route_logger = getattr(helpers.get("app"), "logger", LOGGER)
    get_cached_json = helpers.get("get_cached_json")
    set_cached_json = helpers.get("set_cached_json")
    get_snapshot_payload = helpers.get("get_snapshot_payload")





































    @bp.route("/fill-only/v2/profiles", methods=["GET"])
    def api_quant_fill_only_v2_profiles():
        return jsonify(
            {
                "schema_version": "fill-only-v2-replay-api-v1",
                "default_profile": "conservative_trade_tape",
                "uses_lob_data": False,
                "items": list_fill_only_v2_profiles(),
            }
        )

    @bp.route("/fill-only/v2/readiness", methods=["GET"])
    def api_quant_fill_only_v2_readiness():
        try:
            coverage = load_trade_tape_coverage()
        except FillOnlyV2Error as exc:
            return jsonify(exc.as_dict()), exc.status_code
        return jsonify(
            {
                "schema_version": "fill-only-v2-replay-api-v1",
                "ready": True,
                "uses_lob_data": False,
                "rust_kernel_available": rust_kernel_available(),
                "source_confirmed_backend": (
                    "PERSISTENT_RUST_COMPATIBLE_SUBSET_WITH_PYTHON_FALLBACK"
                ),
                "source_coverage": coverage.as_dict(),
            }
        )

    @bp.route("/fill-only/v2/replay", methods=["POST"])
    def api_quant_fill_only_v2_replay():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_FILL_ONLY_REQUEST",
                }
            ), 400
        try:
            result = run_fill_only_v2_replay(payload)
        except FillOnlyV2Error as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("fill-only V2 replay API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "FILL_ONLY_V2_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/fill-only/v2/resolve-anchor", methods=["POST"])
    def api_quant_fill_only_v2_resolve_anchor():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_FILL_ONLY_REQUEST",
                }
            ), 400
        try:
            result = resolve_fill_only_v2_anchor(payload)
        except FillOnlyV2Error as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("fill-only V2 resolve-anchor API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "FILL_ONLY_V2_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/fill-only/v3/profiles", methods=["GET"])
    def api_quant_fill_only_v3_profiles():
        return jsonify(
            {
                "schema_version": "fill-only-v3-trade-only-api-v1",
                "default_profile": "central_trade_only_l2_reference_expected_fak",
                "uses_lob_data": False,
                "items": list_fill_only_v3_profiles(),
            }
        )

    @bp.route("/fill-only/v3/readiness", methods=["GET"])
    def api_quant_fill_only_v3_readiness():
        try:
            readiness = build_fill_only_v3_readiness()
        except FillOnlyV2Error as exc:
            return jsonify(exc.as_dict()), exc.status_code
        return jsonify(readiness)

    @bp.route("/fill-only/v3/replay", methods=["POST"])
    def api_quant_fill_only_v3_replay():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_FILL_ONLY_V3_REQUEST",
                }
            ), 400
        try:
            result = run_fill_only_v3_replay(payload)
        except FillOnlyV3Error as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("fill-only V3 replay API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "FILL_ONLY_V3_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/fill-only/v3/resolve-anchor", methods=["POST"])
    def api_quant_fill_only_v3_resolve_anchor():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_FILL_ONLY_V3_REQUEST",
                }
            ), 400
        try:
            result = resolve_fill_only_v3_anchor(payload)
        except FillOnlyV3Error as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("fill-only V3 resolve-anchor API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "FILL_ONLY_V3_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/prediction-l2/v1/profiles", methods=["GET"])
    def api_quant_prediction_l2_v1_profiles():
        return jsonify(
            {
                "schema_version": "prediction-l2-replay-api-v1",
                "default_profile": "realistic",
                "uses_lob_data": True,
                "uses_orderfilled_as_taker_gate": False,
                "items": list_pml2_profiles(),
            }
        )

    @bp.route("/prediction-l2/v1/readiness", methods=["GET"])
    def api_quant_prediction_l2_v1_readiness():
        try:
            return jsonify(build_pml2_readiness())
        except Exception as exc:
            LOGGER.exception("Prediction L2 V1 readiness failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "PML2_READINESS_ERROR",
                }
            ), 503

    @bp.route("/prediction-l2/v1/replay", methods=["POST"])
    def api_quant_prediction_l2_v1_replay():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_PML2_REQUEST",
                }
            ), 400
        try:
            result = run_pml2_replay(payload)
        except Pml2ServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("Prediction L2 V1 replay API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "PML2_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/prediction-l2/v1/maker-forecast", methods=["POST"])
    def api_quant_prediction_l2_v1_maker_forecast():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_PML2_REQUEST",
                }
            ), 400
        try:
            result = run_pml2_maker_forecast(payload)
        except Pml2ServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("Prediction L2 V1 Maker forecast API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "PML2_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/prediction-l2/v1/replay-matrix", methods=["POST"])
    def api_quant_prediction_l2_v1_replay_matrix():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_PML2_REQUEST",
                }
            ), 400
        try:
            result = run_pml2_profile_matrix(payload)
        except Pml2ServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("Prediction L2 V1 profile matrix API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "PML2_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/prediction-l2/v2/profiles", methods=["GET"])
    def api_quant_prediction_l2_v2_profiles():
        return jsonify(list_prediction_l2_v2_profiles()), 200

    @bp.route("/prediction-l2/v2/readiness", methods=["GET"])
    def api_quant_prediction_l2_v2_readiness():
        try:
            return jsonify(build_prediction_l2_v2_readiness()), 200
        except Exception as exc:
            LOGGER.exception("Prediction L2 V2 readiness failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "PML2_V2_READINESS_ERROR",
                }
            ), 503

    @bp.route("/prediction-l2/v2/replay", methods=["POST"])
    def api_quant_prediction_l2_v2_replay():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_PML2_V2_REQUEST",
                }
            ), 400
        try:
            result = run_prediction_l2_v2_replay(payload)
        except Pml2ServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("Prediction L2 V2 replay API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "PML2_V2_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/prediction-l2/v2/execution-matrix", methods=["POST"])
    def api_quant_prediction_l2_v2_execution_matrix():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_PML2_V2_REQUEST",
                }
            ), 400
        try:
            result = run_prediction_l2_v2_execution_matrix(payload)
        except Pml2ServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("Prediction L2 V2 execution matrix API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "PML2_V2_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/prediction-l2/v2/maker-forecast", methods=["POST"])
    def api_quant_prediction_l2_v2_maker_forecast():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_PML2_V2_REQUEST",
                }
            ), 400
        try:
            result = run_prediction_l2_v2_maker_forecast(payload)
        except Pml2ServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("Prediction L2 V2 Maker forecast API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "PML2_V2_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/prediction-l2/v2/gap-forecast", methods=["POST"])
    def api_quant_prediction_l2_v2_gap_forecast():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_PML2_V2_REQUEST",
                }
            ), 400
        try:
            result = run_prediction_l2_v2_gap_forecast(payload)
        except Pml2ServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("Prediction L2 V2 gap forecast API failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "PML2_V2_INTERNAL_ERROR",
                }
            ), 500
        return jsonify(result), 200

    @bp.route("/backtest-runs", methods=["POST"])
    def api_quant_create_backtest_run():
        payload = request.get_json(silent=True) or {}
        try:
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                existing = _existing_idempotent_backtest(
                    conn, _backtest_idempotency_key(payload)
                )
                if existing:
                    row = existing
                    run_id = int(row["run_id"])
                elif _payload_bool(payload, "async", "asyncExecution", "queued"):
                    run_id = create_backtest_run(conn, payload)
                    conn.commit()
                    row = get_backtest_run(conn, run_id=run_id)
                else:
                    created = create_and_execute_backtest(conn, payload)
                    conn.commit()
                    run_id = int(created["run_id"])
                    row = get_backtest_run(conn, run_id=run_id)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        if not row:
            return jsonify({"error": "backtest run not found"}), 500
        return jsonify(
            {
                "item": _camel_row(row),
                "runId": row.get("run_id"),
                "status": row.get("status"),
                "idempotentReplay": bool(existing),
            }
        ), 200 if existing else 202

    @bp.route("/backtest-joint-replay", methods=["POST"])
    def api_quant_backtest_joint_replay():
        payload = request.get_json(silent=True) or {}
        outcomes = payload.get("outcomes") or payload.get("outcomeInputs") or []
        if not isinstance(outcomes, list):
            return jsonify({"error": "outcomes must be a list"}), 400
        try:
            report = build_joint_replay_execution_report(outcomes)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 400
        return jsonify({"report": _camel_row(report)})

    @bp.route("/backtest-joint-runs", methods=["POST"])
    def api_quant_create_joint_backtest_run():
        payload = request.get_json(silent=True) or {}
        try:
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                existing = _existing_idempotent_backtest(
                    conn, _backtest_idempotency_key(payload)
                )
                if existing:
                    stored = existing
                else:
                    row = create_and_execute_joint_backtest(conn, payload)
                    conn.commit()
                    run_id = int(row["run_id"])
                    stored = get_backtest_run(conn, run_id=run_id)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        if not stored:
            return jsonify({"error": "joint backtest run not found"}), 500
        report = (
            (stored.get("meta") or {}).get("joint_execution_report")
            if isinstance(stored.get("meta"), dict)
            else None
        ) or {}
        return jsonify(
            {
                "item": _camel_row(stored),
                "runId": stored.get("run_id"),
                "status": stored.get("status"),
                "report": _camel_row(report),
                "idempotentReplay": bool(existing),
            }
        ), 200 if existing else 202

    @bp.route("/backtest-runs", methods=["GET"])
    def api_quant_list_backtest_runs():
        limit = min(max(_parse_int_arg("limit", 25) or 25, 1), 100)
        market_slug = (
            request.args.get("market_slug") or request.args.get("marketSlug") or ""
        ).strip() or None
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_backtest_runs(conn, market_slug=market_slug, limit=limit)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/backtest-runs/<int:run_id>", methods=["GET"])
    def api_quant_get_backtest_run(run_id: int):
        compact = _parse_bool_arg("compact")
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            row = get_backtest_run(conn, run_id=run_id, compact=compact)
        if not row:
            return jsonify({"error": "backtest run not found"}), 404
        if compact:
            row = _compact_backtest_run_row(row)
        return jsonify({"item": _camel_row(row)})

    @bp.route("/backtest-runs/<int:run_id>/progress", methods=["GET"])
    def api_quant_get_backtest_run_progress(run_id: int):
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            row = get_backtest_run_progress(conn, run_id=run_id)
        if not row:
            return jsonify({"error": "backtest run not found"}), 404
        return jsonify({"item": _camel_row(row)})

    @bp.route("/fill-only/settlements/resolve", methods=["POST"])
    def api_quant_resolve_fill_only_settlements():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_SETTLEMENT_REQUEST",
                }
            ), 400
        try:
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                result = resolve_market_settlements(
                    conn,
                    payload,
                    polygon_rpc_url=(
                        os.environ.get("POLY_QUANT_SETTLEMENT_RPC_URL")
                        or "http://127.0.0.1:28545"
                    ),
                )
                conn.commit()
        except MarketSettlementServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("market settlement resolution failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "MARKET_SETTLEMENT_INTERNAL_ERROR",
                }
            ), 500
        return jsonify({"item": _camel_row(result)})

    @bp.route("/fill-only/settlements/<int:market_id>", methods=["GET"])
    def api_quant_get_fill_only_settlement(market_id: int):
        try:
            cutoff = parse_settlement_cutoff(
                request.args.get("cutoffTs") or request.args.get("cutoff_ts")
            )
        except MarketSettlementServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            result = get_market_settlement_snapshot(
                conn, market_id=market_id, cutoff_ts=cutoff
            )
        if result is None:
            return jsonify(
                {
                    "error": "settlement snapshot not found",
                    "error_code": "SETTLEMENT_SNAPSHOT_NOT_FOUND",
                }
            ), 404
        return jsonify({"item": _camel_row(result)})

    @bp.route("/fill-only/settlements/coverage", methods=["GET"])
    def api_quant_get_fill_only_settlement_coverage():
        try:
            cutoff = parse_settlement_cutoff(
                request.args.get("cutoffTs") or request.args.get("cutoff_ts")
            )
        except MarketSettlementServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            result = get_market_settlement_coverage(conn, cutoff_ts=cutoff)
        return jsonify({"item": _camel_row(result)})

    @bp.route("/backtest-runs/<int:run_id>/finalize", methods=["POST"])
    def api_quant_finalize_backtest_run(run_id: int):
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(
                {
                    "error": "request body must be a JSON object",
                    "error_code": "INVALID_FINALIZATION_REQUEST",
                }
            ), 400
        try:
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                result = finalize_registered_backtest_run(
                    conn,
                    run_id,
                    payload,
                    polygon_rpc_url=(
                        os.environ.get("POLY_QUANT_SETTLEMENT_RPC_URL")
                        or "http://127.0.0.1:28545"
                    ),
                )
                conn.commit()
        except FinancialFinalizationServiceError as exc:
            return jsonify(exc.as_dict()), exc.status_code
        except Exception as exc:
            LOGGER.exception("backtest financial finalization failed")
            return jsonify(
                {
                    "error": str(exc),
                    "error_code": "FINANCIAL_FINALIZATION_INTERNAL_ERROR",
                }
            ), 500
        return jsonify({"item": _camel_row(result)}), 200

    @bp.route("/backtest-runs/<int:run_id>/financials", methods=["GET"])
    def api_quant_get_backtest_financials(run_id: int):
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            result = get_backtest_financials(conn, run_id)
        if result is None:
            return jsonify(
                {
                    "error": "financial finalization has not been started",
                    "error_code": "FINANCIAL_FINALIZATION_NOT_FOUND",
                }
            ), 404
        return jsonify({"item": _camel_row(result)})

    @bp.route("/backtest-runs/<int:run_id>/finalization-status", methods=["GET"])
    def api_quant_get_backtest_finalization_status(run_id: int):
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            result = get_backtest_financials(conn, run_id)
        if result is None:
            return jsonify(
                {
                    "runId": run_id,
                    "status": "EXECUTION_COMPLETE_SETTLEMENT_NOT_STARTED",
                }
            )
        return jsonify(
            {
                "runId": run_id,
                "status": result.get("status"),
                "cutoffTs": result.get("cutoff_ts"),
                "errorCode": result.get("error_code"),
                "error": result.get("error"),
            }
        )

    @bp.route("/backtest-runs/<int:run_id>/progress-stream", methods=["GET"])
    def api_quant_stream_backtest_run_progress(run_id: int):
        interval = min(max(float(request.args.get("interval") or 0.75), 0.25), 5.0)

        @stream_with_context
        def stream():
            yield "retry: 1000\n\n"
            last_payload = ""
            while True:
                with postgres_connection(PostgresSettings(), readonly=True) as conn:
                    row = get_backtest_run_progress(conn, run_id=run_id)
                if not row:
                    payload = {
                        "runId": run_id,
                        "status": "missing",
                        "error": "backtest run not found",
                    }
                else:
                    payload = _camel_row(row)
                serialized = json.dumps(payload, ensure_ascii=True, default=str)
                if serialized != last_payload:
                    yield f"data: {serialized}\n\n"
                    last_payload = serialized
                status = str(payload.get("status") or "").lower()
                if status in {"succeeded", "failed", "cancelled", "missing"}:
                    return
                time.sleep(interval)

        return Response(
            stream(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    @bp.route("/backtest-runs/<int:run_id>/cancel", methods=["POST"])
    def api_quant_cancel_backtest_run(run_id: int):
        payload = request.get_json(silent=True) or {}
        reason = str(payload.get("reason") or "cancelled by user")
        with postgres_connection(PostgresSettings(), readonly=False) as conn:
            row = cancel_backtest_run(conn, run_id, reason)
            if row is None:
                return jsonify({"error": "backtest run not found"}), 404
            if str(row.get("status") or "").lower() != "cancelled":
                return jsonify(
                    {
                        "error": f"backtest run is already {row.get('status')}",
                        "item": _camel_row(row),
                    }
                ), 409
            conn.commit()
        return jsonify({"item": _camel_row(row), "status": "cancelled"})

    @bp.route("/backtest-runs/<int:run_id>/replay", methods=["GET"])
    def api_quant_get_backtest_replay(run_id: int):
        limit = min(max(_parse_int_arg("limit", 25000) or 25000, 1), 25000)
        view = (request.args.get("view") or "full").strip().lower()
        if view not in {"full", "executions"}:
            return jsonify({"error": "view must be full or executions"}), 400
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            replay = get_backtest_replay(conn, run_id=run_id, limit=limit, view=view)
        if not replay:
            return jsonify({"error": "backtest run not found"}), 404
        return jsonify(_camel_row(replay))

    @bp.route("/backtest-runs/<int:run_id>/trades", methods=["GET"])
    def api_quant_get_backtest_trades(run_id: int):
        limit = min(max(_parse_int_arg("limit", 1000) or 1000, 1), 25000)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_backtest_trades(conn, run_id=run_id, limit=limit)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/backtest-runs/<int:run_id>/orders", methods=["GET"])
    def api_quant_get_backtest_orders(run_id: int):
        limit = min(max(_parse_int_arg("limit", 1000) or 1000, 1), 25000)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_backtest_orders(conn, run_id=run_id, limit=limit)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/backtest-runs/<int:run_id>/ledger", methods=["GET"])
    def api_quant_get_backtest_ledger(run_id: int):
        limit = min(max(_parse_int_arg("limit", 1000) or 1000, 1), 25000)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_backtest_ledger(conn, run_id=run_id, limit=limit)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/backtest-runs/<int:run_id>/events", methods=["GET"])
    def api_quant_get_backtest_events(run_id: int):
        limit = min(max(_parse_int_arg("limit", 1000) or 1000, 1), 25000)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_backtest_events(conn, run_id=run_id, limit=limit)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/backtest-runs/<int:run_id>/positions", methods=["GET"])
    def api_quant_get_backtest_positions(run_id: int):
        limit = min(max(_parse_int_arg("limit", 1000) or 1000, 1), 25000)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_backtest_position_timeline(conn, run_id=run_id, limit=limit)
        return jsonify(
            {
                "items": [_camel_row(row) for row in rows],
                "count": len(rows),
                "projection": "persisted-ledger-position-timeline",
            }
        )

    @bp.route("/backtest-runs/<int:run_id>/equity", methods=["GET"])
    def api_quant_get_backtest_equity(run_id: int):
        limit = min(max(_parse_int_arg("limit", 25000) or 25000, 1), 25000)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_backtest_equity(conn, run_id=run_id, limit=limit)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/backtest-runs/<int:run_id>/metrics", methods=["GET"])
    def api_quant_get_backtest_metrics(run_id: int):
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_backtest_metrics(conn, run_id=run_id)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/backtest-runs/<int:run_id>/calibration", methods=["GET"])
    def api_quant_get_backtest_calibration(run_id: int):
        limit = min(max(_parse_int_arg("limit", 100) or 100, 1), 1000)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            report = get_backtest_calibration_report(conn, run_id=run_id)
            rows = get_backtest_calibration_orders(conn, run_id=run_id, limit=limit)
        return jsonify(
            {
                "report": _camel_row(report),
                "items": [_camel_row(row) for row in rows],
                "count": len(rows),
            }
        )

    @bp.route("/backtest-runs/<int:run_id>/cost-calibration", methods=["GET"])
    def api_quant_get_backtest_cost_calibration(run_id: int):
        limit = min(max(_parse_int_arg("limit", 100) or 100, 1), 1000)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            report = get_backtest_cost_calibration_report(conn, run_id=run_id)
            rows = get_backtest_cost_calibration_rows(conn, run_id=run_id, limit=limit)
        return jsonify(
            {
                "report": _camel_row(report),
                "items": [_camel_row(row) for row in rows],
                "count": len(rows),
            }
        )

    @bp.route("/backtest-runs/<int:run_id>/artifact-audit", methods=["GET"])
    def api_quant_get_backtest_artifact_audit(run_id: int):
        mode = str(request.args.get("mode") or "full").strip().lower()
        if mode not in {"full", "summary"}:
            return jsonify({"error": "mode must be one of full, summary"}), 400
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            if mode == "summary":
                report = load_backtest_run_artifact_summary(conn, run_id=run_id)
            else:
                inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
                report = build_backtest_run_artifact_report(inputs, run_id=run_id)
        if report is None:
            return jsonify({"error": "backtest run not found"}), 404
        if report.get("reason") == "run_not_found":
            return jsonify({"error": "backtest run not found"}), 404
        return jsonify({"report": _camel_row(report)})

    @bp.route("/backtest-runs/<int:run_id>/activation-decision", methods=["POST"])
    def api_quant_strategy_activation_decision(run_id: int):
        payload = request.get_json(silent=True) or {}
        target_mode = str(
            payload.get("targetMode") or payload.get("target_mode") or "paper"
        )
        write = bool(payload.get("write") is True)
        set_enable = (
            str(payload.get("setEnable") or payload.get("set_enable") or "none")
            .strip()
            .lower()
        )
        if set_enable not in {"none", "enabled", "disabled"}:
            return jsonify(
                {"error": "setEnable must be one of none, enabled, disabled"}
            ), 400
        try:
            with postgres_connection(PostgresSettings(), readonly=not write) as conn:
                if write:
                    create_schema(conn)
                inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
                report = build_backtest_run_artifact_report(inputs, run_id=run_id)
                if report.get("reason") == "run_not_found":
                    return jsonify({"error": "backtest run not found"}), 404
                decision = build_strategy_activation_decision(
                    report,
                    target_mode=target_mode,
                    requested_by=str(
                        payload.get("requestedBy") or payload.get("requested_by") or ""
                    ),
                    notes=str(payload.get("notes") or ""),
                )
                if write:
                    decision = insert_strategy_activation_decision(conn, decision)
                enable_state = None
                if set_enable != "none":
                    if not write:
                        return jsonify({"error": "setEnable requires write=true"}), 400
                    enable_state = build_strategy_enable_state(
                        decision,
                        enable=set_enable == "enabled",
                        requested_by=str(
                            payload.get("requestedBy")
                            or payload.get("requested_by")
                            or ""
                        ),
                        reason=str(payload.get("notes") or ""),
                    )
                    enable_state = upsert_strategy_enable_state(conn, enable_state)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant strategy activation decision failed")
            return jsonify({"error": str(exc)}), 500
        response = {"item": _camel_row(decision), "written": write}
        if enable_state is not None:
            response["enableState"] = _camel_row(enable_state)
        return jsonify(response)

    @bp.route("/strategy-activation-decisions", methods=["GET"])
    def api_quant_strategy_activation_decisions():
        limit = min(max(_parse_int_arg("limit", 50) or 50, 1), 200)
        run_id = _parse_int_arg("run_id", 0)
        target_mode = (
            request.args.get("target_mode") or request.args.get("targetMode") or ""
        ).strip() or None
        try:
            with postgres_connection(PostgresSettings(), readonly=True) as conn:
                rows = load_strategy_activation_decisions(
                    conn,
                    run_id=run_id or None,
                    target_mode=target_mode,
                    limit=limit,
                )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant strategy activation decisions fetch failed")
            return jsonify({"error": str(exc)}), 500
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/strategy-enable-state", methods=["GET"])
    def api_quant_strategy_enable_state():
        limit = min(max(_parse_int_arg("limit", 50) or 50, 1), 200)
        target_mode = (
            request.args.get("target_mode") or request.args.get("targetMode") or ""
        ).strip() or None
        enabled_only = _parse_bool_arg("enabled_only") or _parse_bool_arg("enabledOnly")
        try:
            with postgres_connection(PostgresSettings(), readonly=True) as conn:
                rows = load_strategy_enable_state(
                    conn,
                    target_mode=target_mode,
                    enabled_only=enabled_only,
                    limit=limit,
                )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant strategy enable state fetch failed")
            return jsonify({"error": str(exc)}), 500
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/strategy-runner-plan", methods=["GET"])
    def api_quant_strategy_runner_plan():
        limit = min(max(_parse_int_arg("limit", 50) or 50, 1), 200)
        target_mode = (
            request.args.get("target_mode") or request.args.get("targetMode") or "paper"
        ).strip()
        include_blocked = _parse_bool_arg("include_blocked") or _parse_bool_arg(
            "includeBlocked"
        )
        try:
            with postgres_connection(PostgresSettings(), readonly=True) as conn:
                rows = load_strategy_enable_state(
                    conn,
                    target_mode=target_mode,
                    enabled_only=not include_blocked,
                    limit=limit,
                )
            plan = build_strategy_runner_plan(
                rows,
                target_mode=target_mode,
                include_blocked=include_blocked,
                limit=limit,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant strategy runner plan failed")
            return jsonify({"error": str(exc)}), 500
        return jsonify({"plan": _camel_row(plan)})

    @bp.route("/strategy-guarded-executor", methods=["POST"])
    def api_quant_strategy_guarded_executor():
        payload = request.get_json(silent=True) or {}
        limit = min(max(_payload_int(payload, "limit", 50), 1), 200)
        target_mode = str(
            payload.get("targetMode") or payload.get("target_mode") or "paper"
        ).strip()
        include_blocked = _payload_bool(payload, "includeBlocked", "include_blocked")
        record_intent = _payload_bool(payload, "recordIntent", "record_intent")
        event_source = (
            str(payload.get("source") or payload.get("eventSource") or "").strip()
            or None
        )
        try:
            with postgres_connection(
                PostgresSettings(), readonly=not record_intent
            ) as conn:
                if record_intent:
                    create_schema(conn)
                rows = load_strategy_enable_state(
                    conn,
                    target_mode=target_mode,
                    enabled_only=not include_blocked,
                    limit=limit,
                )
                runner_plan = build_strategy_runner_plan(
                    rows,
                    target_mode=target_mode,
                    include_blocked=include_blocked,
                    limit=limit,
                )
                report = build_guarded_executor_report(
                    runner_plan,
                    record_intent=record_intent,
                    event_source=event_source,
                )
                if record_intent:
                    report["recorded_event_count"] = record_guarded_execution_intents(
                        conn, report
                    )
                    if report["recorded_event_count"]:
                        report["executor_status"] = "intent_recorded"
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant guarded executor failed")
            return jsonify({"error": str(exc)}), 500
        return jsonify({"report": _camel_row(report)})

    @bp.route("/execution-profile-overrides", methods=["GET"])
    def api_quant_execution_profile_overrides():
        limit = min(max(_parse_int_arg("limit", 25) or 25, 1), 100)
        status = (request.args.get("status") or "approved").strip().lower()
        if status == "all":
            status = ""
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_execution_profile_overrides(
                conn, status=status or None, limit=limit
            )
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/production-parameter-staging", methods=["GET"])
    def api_quant_production_parameter_staging():
        limit = min(max(_parse_int_arg("limit", 25) or 25, 1), 100)
        status = (request.args.get("status") or "approved").strip().lower()
        if status == "all":
            status = ""
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_production_parameter_staging(
                conn, status=status or None, limit=limit
            )
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/production-parameter-staging/<int:staging_id>/review", methods=["POST"])
    def api_quant_review_production_parameter_staging(staging_id: int):
        payload = request.get_json(silent=True) or {}
        status = str(payload.get("status") or "").strip().lower()
        reviewed_by = str(
            payload.get("reviewed_by", payload.get("reviewedBy", ""))
        ).strip()
        review_note = (
            str(payload.get("review_note", payload.get("reviewNote", ""))).strip()
            or None
        )
        try:
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                create_schema(conn)
                row = update_production_parameter_staging_status(
                    conn,
                    staging_id=staging_id,
                    status=status,
                    reviewed_by=reviewed_by or None,
                    review_note=review_note,
                )
                conn.commit()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("production parameter staging review failed")
            return jsonify({"error": str(exc)}), 500
        return jsonify(
            {
                "item": _camel_row(row),
                "stagingId": row.get("staging_id"),
                "status": row.get("status"),
            }
        )

    @bp.route("/parameter-search-batches", methods=["GET"])
    def api_quant_parameter_search_batches():
        limit = min(max(_parse_int_arg("limit", 25) or 25, 1), 100)
        status = (request.args.get("status") or "").strip().lower() or None
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = list_parameter_search_batches(conn, status=status, limit=limit)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/parameter-search-batches", methods=["POST"])
    def api_quant_create_parameter_search_batch():
        payload = request.get_json(silent=True) or {}
        try:
            plan_payload = (
                payload.get("plan") if isinstance(payload.get("plan"), dict) else None
            )
            plan = plan_payload or build_parameter_search_plan(
                base_payload=payload.get("basePayload")
                or payload.get("base_payload")
                or {},
                grid=payload.get("grid"),
                evidence_modes=payload.get("evidenceModes")
                or payload.get("evidence_modes")
                or ("train", "test", "walk_forward"),
                max_runs=int(payload.get("maxRuns") or payload.get("max_runs") or 250),
                universe_name=str(
                    payload.get("universeName")
                    or payload.get("universe_name")
                    or "fill_first_parameter_search"
                ),
            )
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                create_schema(conn)
                batch_id = create_parameter_search_batch(
                    conn,
                    plan,
                    source=str(payload.get("source") or "parameter-search-api"),
                    strategy_name=str(
                        payload.get("strategyName")
                        or payload.get("strategy_name")
                        or "unknown"
                    ),
                    strategy_version=str(
                        payload.get("strategyVersion")
                        or payload.get("strategy_version")
                        or "unknown"
                    ),
                    universe_name=str(
                        payload.get("universeName")
                        or payload.get("universe_name")
                        or plan.get("universe_name")
                        or ""
                    ),
                    max_attempts=int(
                        payload.get("maxAttempts") or payload.get("max_attempts") or 2
                    ),
                    created_by=str(
                        payload.get("createdBy") or payload.get("created_by") or ""
                    )
                    or None,
                )
                conn.commit()
                row = get_parameter_search_batch(conn, batch_id=batch_id)
                items = get_parameter_search_batch_items(conn, batch_id=batch_id)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant parameter search batch create failed")
            return jsonify({"error": str(exc)}), 500
        progress = build_parameter_search_progress_report(row or {}, items)
        return jsonify(
            {
                "item": _camel_row(row or {}),
                "batchId": batch_id,
                "status": (row or {}).get("status"),
                "progress": _camel_row(progress),
            }
        ), 202

    @bp.route("/parameter-search-batches/<int:batch_id>", methods=["GET"])
    def api_quant_parameter_search_batch(batch_id: int):
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            row = get_parameter_search_batch(conn, batch_id=batch_id)
            if not row:
                return jsonify({"error": "parameter search batch not found"}), 404
            items = get_parameter_search_batch_items(conn, batch_id=batch_id)
        progress = build_parameter_search_progress_report(row, items)
        return jsonify(
            {
                "item": _camel_row(row),
                "items": [_camel_row(item) for item in items],
                "progress": _camel_row(progress),
            }
        )

    @bp.route("/parameter-search-batches/<int:batch_id>/worker", methods=["POST"])
    def api_quant_run_parameter_search_batch_worker(batch_id: int):
        payload = request.get_json(silent=True) or {}
        try:
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                create_schema(conn)
                report = run_parameter_search_batch_worker(
                    conn,
                    batch_id=batch_id,
                    max_items=int(
                        payload.get("maxItems") or payload.get("max_items") or 1
                    ),
                    worker_id=str(
                        payload.get("workerId") or payload.get("worker_id") or ""
                    )
                    or None,
                    retry_failed=bool(
                        payload.get("retryFailed", payload.get("retry_failed", True))
                    ),
                    stop_on_error=bool(
                        payload.get("stopOnError", payload.get("stop_on_error", False))
                    ),
                )
                conn.commit()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant parameter search batch worker failed")
            return jsonify({"error": str(exc)}), 500
        return jsonify(
            {
                "report": _camel_row(report),
                "batchId": batch_id,
                "status": report.get("status"),
            }
        ), 202

    @bp.route("/parameter-search-batches/<int:batch_id>/cancel", methods=["POST"])
    def api_quant_cancel_parameter_search_batch(batch_id: int):
        payload = request.get_json(silent=True) or {}
        try:
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                create_schema(conn)
                report = cancel_parameter_search_batch(
                    conn,
                    batch_id=batch_id,
                    reason=str(payload.get("reason") or "") or None,
                    canceled_by=str(
                        payload.get("canceledBy") or payload.get("canceled_by") or ""
                    )
                    or None,
                )
                conn.commit()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant parameter search batch cancel failed")
            return jsonify({"error": str(exc)}), 500
        progress = (
            report.get("progress") if isinstance(report.get("progress"), dict) else {}
        )
        return jsonify(
            {
                "report": _camel_row(report),
                "batchId": batch_id,
                "status": progress.get("status"),
            }
        ), 202

    @bp.route("/parameter-search-batches/<int:batch_id>/requeue", methods=["POST"])
    def api_quant_requeue_parameter_search_items(batch_id: int):
        payload = request.get_json(silent=True) or {}
        statuses = (
            payload.get("statuses")
            or payload.get("requeueStatuses")
            or payload.get("requeue_statuses")
            or ["failed"]
        )
        if isinstance(statuses, str):
            statuses = [statuses]
        try:
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                create_schema(conn)
                report = requeue_parameter_search_items(
                    conn,
                    batch_id=batch_id,
                    statuses=statuses,
                    reset_attempts=bool(
                        payload.get(
                            "resetAttempts", payload.get("reset_attempts", False)
                        )
                    ),
                    clear_results=bool(
                        payload.get("clearResults", payload.get("clear_results", False))
                    ),
                )
                conn.commit()
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant parameter search batch requeue failed")
            return jsonify({"error": str(exc)}), 500
        progress = (
            report.get("progress") if isinstance(report.get("progress"), dict) else {}
        )
        return jsonify(
            {
                "report": _camel_row(report),
                "batchId": batch_id,
                "status": progress.get("status"),
            }
        ), 202

    @bp.route("/backtest-benchmarks", methods=["POST"])
    def api_quant_create_backtest_benchmark():
        payload = request.get_json(silent=True) or {}
        try:
            parts = _benchmark_request_parts(payload)
            with postgres_connection(PostgresSettings(), readonly=False) as conn:
                create_schema(conn)
                benchmark_id = create_benchmark_run(
                    conn,
                    universe_type=parts["universe_spec"].universe_type,
                    universe_name=parts["universe_spec"].universe_name,
                    market_count=parts["universe_spec"].limit,
                    strategy_name="favorite_hold_v1",
                    parameters=parts["parameters"],
                    profiles=parts["profiles"],
                )
                conn.commit()
                row = get_benchmark_run(conn, benchmark_id=int(benchmark_id))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant backtest benchmark enqueue failed")
            return jsonify({"error": str(exc)}), 500
        if not row:
            return jsonify({"error": "benchmark run not found"}), 500
        worker = threading.Thread(
            target=_run_benchmark_job,
            args=(int(row["benchmark_id"]), dict(payload)),
            name=f"quant-benchmark-{row['benchmark_id']}",
            daemon=True,
        )
        worker.start()
        return jsonify(
            {
                "item": _camel_row(row),
                "benchmarkId": row.get("benchmark_id"),
                "status": row.get("status"),
                "artifacts": [],
            }
        ), 202

    @bp.route("/backtest-universes", methods=["GET"])
    def api_quant_list_backtest_universes():
        return jsonify(
            {
                "items": list_supported_universes(),
                "count": len(list_supported_universes()),
            }
        )

    @bp.route("/backtest-benchmarks", methods=["GET"])
    def api_quant_list_backtest_benchmarks():
        limit = min(max(_parse_int_arg("limit", 25) or 25, 1), 100)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = list_benchmark_runs(conn, limit=limit)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/backtest-benchmarks/<int:benchmark_id>", methods=["GET"])
    def api_quant_get_backtest_benchmark(benchmark_id: int):
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            row = get_benchmark_run(conn, benchmark_id=benchmark_id)
            artifacts = get_benchmark_artifacts(conn, benchmark_id=benchmark_id)
        if not row:
            return jsonify({"error": "benchmark not found"}), 404
        return jsonify(
            {
                "item": _camel_row(row),
                "artifacts": [_camel_row(item) for item in artifacts],
            }
        )

    @bp.route("/backtest-benchmarks/<int:benchmark_id>/rows", methods=["GET"])
    def api_quant_get_backtest_benchmark_rows(benchmark_id: int):
        limit = min(max(_parse_int_arg("limit", 10000) or 10000, 1), 25000)
        with postgres_connection(PostgresSettings(), readonly=True) as conn:
            rows = get_benchmark_rows(conn, benchmark_id=benchmark_id, limit=limit)
        return jsonify({"items": [_camel_row(row) for row in rows], "count": len(rows)})

    @bp.route("/backtest-replay-coverage/build", methods=["POST"])
    def api_quant_build_backtest_replay_coverage():
        payload = request.get_json(silent=True) or {}
        universe = str(payload.get("universe") or "nba_2024_25_moneyline")
        limit = min(max(int(payload.get("limit") or 500), 1), 500)
        try:
            result = build_replay_coverage(
                universe=universe,
                limit=limit,
                window_start_hours=Decimal(
                    str(
                        payload.get("windowStartHours")
                        or payload.get("window_start_hours")
                        or "25"
                    )
                ),
                window_end_hours=Decimal(
                    str(
                        payload.get("windowEndHours")
                        or payload.get("window_end_hours")
                        or "0"
                    )
                ),
                force=bool(payload.get("force")),
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            route_logger.exception("quant replay coverage build failed")
            return jsonify({"error": str(exc)}), 500
        return jsonify(_camel_row(result)), 202

    return bp
