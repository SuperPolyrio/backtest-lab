"""Fixed-template quant backtest engine.

This first production pass intentionally keeps the strategy small: one long
position template, two price sources, and durable run/result tables.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP
import hashlib
import json
import os
import subprocess
from typing import Any, Callable, Mapping

from quant.core.db import ClickHouseClient

from .execution import (
    BookSnapshot,
    parse_book_snapshot,
    snapshot_from_any,
    snapshot_to_dict,
)
from .execution_profiles import apply_adverse_slippage, effective_execution_profile
from .execution_profile_overrides import maybe_apply_approved_execution_profile_override
from .l2_orderfilled_execution import combine_orderfilled_l2_execution, l2_config_from_params, simulate_l2_depth_execution
from .pmxt_compact_execution_store import DEFAULT_BUILD_TAG as PMXT_COMPACT_BUILD_TAG
from .pmxt_compact_execution_store import PmxtCompactBookProvider
from .pml2.archive_execution import XueNativeExecutionProvider
from .pml2.adapters import legacy_snapshot_to_pml2
from .pml2.contracts import (
    Outcome as Pml2Outcome,
    Pml2OrderIntent,
    RawOrderSide as Pml2RawOrderSide,
    ReplayAuditMode,
    TimeInForce as Pml2TimeInForce,
)
from .pml2.session import ReplayExecutionSession
from .frameworks import normalize_backtest_engine, run_framework_backtest
from .ledger import (
    build_ledger_rows,
    build_source_evidenced_order_ledger_rows,
    ledger_summary,
)
from . import orderfilled_execution
from .orderfilled_v2_replay import (
    CapacityLedger,
    RequiredTradeWindow,
    V2OrderResult,
    V2TakerOrder,
    load_v2_trade_slices_for_windows,
    load_v2_trade_slices_for_orders,
    replay_v2_taker_orders_with_diagnostics,
    summarize_v2_results,
    with_v2_execution_profile,
)
from .fill_only_v2_service import load_trade_tape_coverage
from .trade_only_v3 import (
    LiquidityIntent,
    RunLiquidityLedger,
    TradeOnlyOrder,
    TradeOnlyOrderResult,
    get_trade_only_profile,
    replay_trade_only_orders_with_diagnostics,
    with_trade_only_profile,
)
from .orders import execution_evidence_type, next_order_id, order_from_fill, summarize_orders
from .order_state import attach_real_order_state_events, load_real_order_state_events_for_run
from .platform_incidents import build_platform_incident_report, load_platform_incidents_for_run
from .runners.execution_replay import (
    ReplayTradeEvent,
    dedupe_replay_events,
    replay_limit_order,
    replay_trade_event_dict,
    sequence_key,
)
from .strategy_intents import build_threshold_limit_intent
from .event_stream import CANONICAL_FILL_KEY_FIELDS, build_backtest_event_stream, build_raw_trade_tick_report
from ..orderbook.coverage import build_lob_execution_coverage_report
from ..prices.build_targets import target_reason, upsert_price_build_targets_for_market


SUPPORTED_PRICE_SOURCES = {"frontend", "orderfilled_block_close"}
PRICE_QUERY_GUARD_VERSION = "phase1_keyed_price_access_v1"
BACKTEST_STRATEGY_NAME = "fixed_threshold"
BACKTEST_STRATEGY_VERSION = "fixed_threshold_v1"
BACKTEST_ARTIFACT_SCHEMA_VERSION = "fill_first_run_artifact_v1"
BACKTEST_FEE_MODEL_VERSION = "maker_taker_rebate_v1"
BACKTEST_SLIPPAGE_MODEL_VERSION = "adverse_slippage_bps_cents_v1"
BACKTEST_LIMIT_REPLAY_FILL_MODEL_VERSION = "orderfilled_limit_cross_v1"
BACKTEST_PROBABILITY_FILL_MODEL_VERSION = "orderfilled_probability_cap_v1"
EVENT_CORRELATION_MAX_GRID_SAMPLES = 128
EVENT_CORRELATION_MIN_PAIR_SAMPLES = 8
MARKOUT_BAR_HORIZONS = (1, 5, 20)
MARKOUT_SECOND_HORIZONS = (60, 300, 1200)
ORDERFILLED_CROSS_MODE = "ORDERFILLED_CROSS"
ORDERFILLED_LOB_MODE = "ORDERFILLED_LOB"
ORDERFILLED_V2_TAPE_MODE = "ORDERFILLED_V2_TAPE"
ORDERFILLED_V3_TRADE_MODE = "ORDERFILLED_V3_TRADE"
PREDICTION_L2_REPLAY_V1_MODE = "PREDICTION_L2_REPLAY_V1"
PML2_XUE_NATIVE_SOURCE = "XUE_NATIVE"
PML2_LEGACY_PMXT_SOURCE = "LEGACY_PMXT"
LEGACY_ORDERFILLED_LOB_MODE = ORDERFILLED_LOB_MODE
ORDERFILLED_CROSS_ALIASES = {ORDERFILLED_CROSS_MODE, "ORDERFILLED_LIMIT_REPLAY", "LIMIT_REPLAY"}
ORDERFILLED_LOB_ALIASES = {ORDERFILLED_LOB_MODE, "ORDERFILLED_DEPTH", "FILL_LOB", "ORDERFILLED_LOB_CALIBRATED"}
ORDERFILLED_V2_TAPE_ALIASES = {
    ORDERFILLED_V2_TAPE_MODE,
    "ORDERFILLED_V2",
    "ORDERFILLED_ONLY",
    "ORDERFILLED_ONLY_TRADE_TAPE",
    "FILL_EVIDENCE",
    "FILL_EVIDENCE_EXECUTION",
    "TRADE_TAPE_PARTICIPATION",
}
ORDERFILLED_V3_TRADE_ALIASES = {
    ORDERFILLED_V3_TRADE_MODE,
    "ORDERFILLED_V3",
    "FILL_ONLY_V3",
    "TRADE_ONLY_V3",
}
PREDICTION_L2_REPLAY_V1_ALIASES = {
    PREDICTION_L2_REPLAY_V1_MODE,
    "PML2",
    "PML2_V1",
    "PREDICTION_L2",
    "PREDICTION_MARKET_L2",
}

RunProgressCallback = Callable[[dict[str, Any]], None]


class BacktestRunCancelled(RuntimeError):
    """Raised at cooperative checkpoints after a persisted run is cancelled."""


def normalize_execution_price_mode(value: Any, default: str = ORDERFILLED_CROSS_MODE) -> str:
    mode = str(value or default).strip().upper().replace("-", "_")
    if mode == "LEGACY":
        return default
    if mode in ORDERFILLED_CROSS_ALIASES or mode in {"ORDERFILLED_LIMIT_CROSS", "LIMIT_CROSS", "ORDERFILLED_CROSS_REPLAY"}:
        return ORDERFILLED_CROSS_MODE
    if mode in ORDERFILLED_LOB_ALIASES:
        return ORDERFILLED_LOB_MODE
    if mode in ORDERFILLED_V2_TAPE_ALIASES:
        return ORDERFILLED_V2_TAPE_MODE
    if mode in ORDERFILLED_V3_TRADE_ALIASES:
        return ORDERFILLED_V3_TRADE_MODE
    if mode in PREDICTION_L2_REPLAY_V1_ALIASES:
        return PREDICTION_L2_REPLAY_V1_MODE
    return mode or default


def is_orderfilled_cross_mode(value: Any) -> bool:
    return normalize_execution_price_mode(value) in ORDERFILLED_CROSS_ALIASES


def is_orderfilled_lob_mode(value: Any) -> bool:
    return normalize_execution_price_mode(value) == ORDERFILLED_LOB_MODE


def is_orderfilled_v2_tape_mode(value: Any) -> bool:
    return normalize_execution_price_mode(value) == ORDERFILLED_V2_TAPE_MODE


def is_orderfilled_v3_trade_mode(value: Any) -> bool:
    return normalize_execution_price_mode(value) == ORDERFILLED_V3_TRADE_MODE


def is_prediction_l2_replay_v1_mode(value: Any) -> bool:
    return normalize_execution_price_mode(value) == PREDICTION_L2_REPLAY_V1_MODE


def normalize_pml2_l2_source(value: Any) -> str:
    source = str(value or PML2_XUE_NATIVE_SOURCE).strip().upper().replace("-", "_")
    if source in {"XUE", "NATIVE", "NATIVE_XUE", "XUE_NATIVE_L2"}:
        return PML2_XUE_NATIVE_SOURCE
    if source in {"PMXT", "PMXT_COMPACT", "LEGACY", "LEGACY_PMXT_COMPACT"}:
        return PML2_LEGACY_PMXT_SOURCE
    if source in {PML2_XUE_NATIVE_SOURCE, PML2_LEGACY_PMXT_SOURCE}:
        return source
    raise ValueError(
        "pml2_l2_source must be XUE_NATIVE or LEGACY_PMXT"
    )


def normalize_pml2_audit_mode(value: Any) -> str:
    mode = str(value or ReplayAuditMode.CHAIN_ONLY.value).strip().upper()
    try:
        return ReplayAuditMode(mode).value
    except ValueError as exc:
        raise ValueError("pml2_audit_mode must be FULL or CHAIN_ONLY") from exc


def _v3_profile_name(value: Any) -> str:
    profile = str(value or "realistic").strip().lower().replace("-", "_")
    aliases = {
        "realistic": "central_trade_only_l2_reference_expected_fak",
        "central": "central_trade_only_l2_reference_expected_fak",
        "expected": "central_trade_only_l2_reference_expected_fak",
        "strict": "taker_source_confirmed",
        "source_confirmed": "taker_source_confirmed",
        "audit": "taker_source_confirmed",
    }
    resolved = aliases.get(profile, profile)
    get_trade_only_profile(resolved)
    return resolved


DATA_ACCESS_POLICY = {
    "market_search": {
        "allowed_tables": [
            "quant.market_event_metadata",
            "quant.market_event_members",
            "quant.market_price_build_market_progress",
            "quant.market_token_metadata",
        ],
        "forbidden": "Do not discover markets from quant.market_token_block_close with GROUP BY/count or title contains search.",
    },
    "backtest_price_read": {
        "allowed_tables": ["quant.market_token_block_close", "quant.market_token_frontend_price_1m"],
        "required_access": ["token_id range", "market_slug + token_side range", "market_id + token_side range"],
        "forbidden": "Do not scan or group all markets from price detail tables during online backtests.",
    },
    "raw_orderfilled_verification": {
        "allowed_tables": ["ClickHouse poly_orderfilled.orderfilled_fact"],
        "required_access": ["market_id", "token_id", "block_number range", "explicit limit"],
        "forbidden": "Do not run online all-market raw fact aggregation.",
    },
}


@dataclass(frozen=True)
class BacktestParameters:
    entry_threshold: Decimal = Decimal("0.58")
    exit_threshold: Decimal = Decimal("0.44")
    stop_loss: Decimal = Decimal("0.075")
    take_profit: Decimal = Decimal("0.16")
    max_holding_bars: int = 96
    initial_capital: Decimal = Decimal("100000")
    position_size: Decimal = Decimal("100")
    fee_bps: Decimal = Decimal("0")
    maker_fee_bps: Decimal | None = None
    taker_fee_bps: Decimal | None = None
    maker_rebate_bps: Decimal = Decimal("0")
    slippage_bps: Decimal = Decimal("0")
    liquidity_cap_pct: Decimal = Decimal("100")
    max_position_notional: Decimal = Decimal("0")
    min_fill_pct: Decimal = Decimal("0")
    execution_price_mode: str = ORDERFILLED_CROSS_MODE
    execution_profile: str = "realistic"
    pml2_audit_mode: str = ReplayAuditMode.CHAIN_ONLY.value
    order_role: str = "maker"
    latency_blocks: int = 0
    adverse_slippage_cents: Decimal = Decimal("0.005")
    fill_probability_haircut_pct: Decimal = Decimal("20")
    latency_seconds: Decimal = Decimal("0")
    max_book_staleness_seconds: Decimal = Decimal("900")
    allow_partial_fill: bool = True
    min_fill_size: Decimal = Decimal("0")
    reject_on_stale_book: bool = True
    final_valuation_mode: str = "SETTLEMENT"
    max_entry_price: Decimal = Decimal("1")
    min_exit_price: Decimal = Decimal("0")
    buy_limit_price: Decimal | None = None
    sell_limit_price: Decimal | None = None
    settlement_value: Decimal | None = None
    gas_cost_per_order: Decimal = Decimal("0")
    settlement_cost: Decimal = Decimal("0")
    redeem_cost: Decimal = Decimal("0")
    capital_cost_bps: Decimal = Decimal("0")
    cancel_after_blocks: int = 0
    cancel_ack_delay_blocks: int = 0
    cancel_fail: bool = False
    entry_signal_price_field: str = "price"
    exit_signal_price_field: str = "price"
    entry_use_block_range: bool = False
    exit_use_block_range: bool = False
    signal_min_trade_count: int = 0
    signal_min_block_volume: Decimal = Decimal("0")


@dataclass(frozen=True)
class PricePoint:
    x_value: int
    price: Decimal
    volume: Decimal
    trade_count: int = 0
    timestamp: datetime | None = None
    open_price: Decimal | None = None
    high_price: Decimal | None = None
    low_price: Decimal | None = None
    close_price: Decimal | None = None
    vwap_price: Decimal | None = None
    buy_volume: Decimal = Decimal("0")
    sell_volume: Decimal = Decimal("0")
    first_log_index: int | None = None
    last_log_index: int | None = None


@dataclass
class OpenPosition:
    trade_index: int
    entry_index: int
    entry_x: int
    entry_price: Decimal
    size: Decimal
    requested_notional: Decimal
    filled_notional: Decimal
    fill_pct: Decimal
    fill_status: str = "FILLED"
    book_snapshot_id: int | None = None
    snapshot_version: str | None = None
    staleness_seconds: Decimal | None = None
    staleness_blocks: int | None = None
    avg_fill_price: Decimal | None = None
    fill_probability: Decimal = Decimal("0")
    block_volume: Decimal = Decimal("0")
    trade_count: int = 0
    available_notional: Decimal = Decimal("0")
    entry_order_id: str | None = None
    entry_fee_cost: Decimal = Decimal("0")
    entry_rebate: Decimal = Decimal("0")
    entry_slippage_cost: Decimal = Decimal("0")
    entry_fill_slices: list[dict[str, Any]] = field(default_factory=list)


def decimal_or_default(value: Any, default: Decimal) -> Decimal:
    if value in (None, ""):
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


def int_or_default(value: Any, default: int) -> int:
    try:
        parsed = int(str(value))
    except Exception:
        return default
    return parsed if parsed > 0 else default


def bool_or_default(value: Any, default: bool) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed >= 0 else None


def normalize_price_source(value: Any) -> str:
    text = str(value or "frontend").strip().lower()
    aliases = {
        "orderfilled": "orderfilled_block_close",
        "block_close": "orderfilled_block_close",
        "orderfilled_block_close": "orderfilled_block_close",
        "frontend_price_history": "frontend",
        "frontend-price-history": "frontend",
        "frontend": "frontend",
    }
    source = aliases.get(text, text)
    if source not in SUPPORTED_PRICE_SOURCES:
        raise ValueError(f"unsupported price_source: {value!r}")
    return source


def normalize_signal_price_field(value: Any, default: str = "price") -> str:
    text = str(value or default).strip().lower().replace("-", "_")
    aliases = {
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price",
        "vwap": "vwap_price",
    }
    normalized = aliases.get(text, text)
    if normalized in {
        "price",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "vwap_price",
    }:
        return normalized
    return default


def parse_parameters(payload: dict[str, Any]) -> BacktestParameters:
    nested = payload.get("parameters")
    if isinstance(nested, Mapping):
        payload = {**dict(nested), **payload}
    execution_price_mode = normalize_execution_price_mode(payload.get("execution_price_mode", payload.get("executionPriceMode", ORDERFILLED_CROSS_MODE)))
    default_order_role = "maker" if is_orderfilled_cross_mode(execution_price_mode) else "taker"
    execution_profile = str(
        payload.get("execution_profile", payload.get("executionProfile", "realistic"))
        or "realistic"
    ).lower()
    if is_orderfilled_v3_trade_mode(execution_price_mode):
        execution_profile = _v3_profile_name(execution_profile)
    return BacktestParameters(
        entry_threshold=decimal_or_default(payload.get("entry_threshold", payload.get("entryThreshold")), Decimal("0.58")),
        exit_threshold=decimal_or_default(payload.get("exit_threshold", payload.get("exitThreshold")), Decimal("0.44")),
        entry_signal_price_field=normalize_signal_price_field(payload.get("entry_signal_price_field", payload.get("entrySignalPriceField")), "price"),
        exit_signal_price_field=normalize_signal_price_field(payload.get("exit_signal_price_field", payload.get("exitSignalPriceField")), "price"),
        entry_use_block_range=bool_or_default(payload.get("entry_use_block_range", payload.get("entryUseBlockRange")), False),
        exit_use_block_range=bool_or_default(payload.get("exit_use_block_range", payload.get("exitUseBlockRange")), False),
        signal_min_trade_count=max(0, int_or_default(payload.get("signal_min_trade_count", payload.get("signalMinTradeCount")), 0)),
        signal_min_block_volume=max(Decimal("0"), decimal_or_default(payload.get("signal_min_block_volume", payload.get("signalMinBlockVolume")), Decimal("0"))),
        stop_loss=decimal_or_default(payload.get("stop_loss", payload.get("stopLoss")), Decimal("0.075")),
        take_profit=decimal_or_default(payload.get("take_profit", payload.get("takeProfit")), Decimal("0.16")),
        max_holding_bars=int_or_default(payload.get("max_holding_bars", payload.get("maxHoldingBars")), 96),
        initial_capital=decimal_or_default(payload.get("initial_capital", payload.get("initialCapital")), Decimal("100000")),
        position_size=decimal_or_default(payload.get("position_size", payload.get("positionSize")), Decimal("100")),
        fee_bps=decimal_or_default(payload.get("fee_bps", payload.get("feeBps")), Decimal("0")),
        maker_fee_bps=decimal_or_none(payload.get("maker_fee_bps", payload.get("makerFeeBps"))),
        taker_fee_bps=decimal_or_none(payload.get("taker_fee_bps", payload.get("takerFeeBps"))),
        maker_rebate_bps=decimal_or_default(payload.get("maker_rebate_bps", payload.get("makerRebateBps")), Decimal("0")),
        slippage_bps=decimal_or_default(payload.get("slippage_bps", payload.get("slippageBps")), Decimal("0")),
        liquidity_cap_pct=decimal_or_default(payload.get("liquidity_cap_pct", payload.get("liquidityCapPct")), Decimal("100")),
        max_position_notional=decimal_or_default(payload.get("max_position_notional", payload.get("maxPositionNotional")), Decimal("0")),
        min_fill_pct=decimal_or_default(payload.get("min_fill_pct", payload.get("minFillPct")), Decimal("0")),
        execution_price_mode=execution_price_mode,
        execution_profile=execution_profile,
        pml2_audit_mode=normalize_pml2_audit_mode(
            payload.get("pml2_audit_mode", payload.get("pml2AuditMode"))
        ),
        order_role=str(payload.get("order_role", payload.get("orderRole", default_order_role)) or default_order_role).lower(),
        latency_blocks=max(0, int_or_default(payload.get("latency_blocks", payload.get("latencyBlocks")), 0)),
        adverse_slippage_cents=decimal_or_default(payload.get("adverse_slippage_cents", payload.get("adverseSlippageCents")), Decimal("0.005")),
        fill_probability_haircut_pct=decimal_or_default(payload.get("fill_probability_haircut_pct", payload.get("fillProbabilityHaircutPct")), Decimal("20")),
        latency_seconds=decimal_or_default(payload.get("latency_seconds", payload.get("latencySeconds")), Decimal("0")),
        max_book_staleness_seconds=decimal_or_default(payload.get("max_book_staleness_seconds", payload.get("maxBookStalenessSeconds")), Decimal("900")),
        allow_partial_fill=bool_or_default(payload.get("allow_partial_fill", payload.get("allowPartialFill")), True),
        min_fill_size=decimal_or_default(payload.get("min_fill_size", payload.get("minFillSize")), Decimal("0")),
        reject_on_stale_book=bool_or_default(payload.get("reject_on_stale_book", payload.get("rejectOnStaleBook")), True),
        final_valuation_mode=str(payload.get("final_valuation_mode", payload.get("finalValuationMode", "SETTLEMENT")) or "SETTLEMENT").upper(),
        max_entry_price=decimal_or_default(payload.get("max_entry_price", payload.get("maxEntryPrice")), Decimal("1")),
        min_exit_price=decimal_or_default(payload.get("min_exit_price", payload.get("minExitPrice")), Decimal("0")),
        buy_limit_price=decimal_or_none(payload.get("buy_limit_price", payload.get("buyLimitPrice"))),
        sell_limit_price=decimal_or_none(payload.get("sell_limit_price", payload.get("sellLimitPrice"))),
        settlement_value=decimal_or_none(payload.get("settlement_value", payload.get("settlementValue"))),
        gas_cost_per_order=decimal_or_default(payload.get("gas_cost_per_order", payload.get("gasCostPerOrder")), Decimal("0")),
        settlement_cost=decimal_or_default(payload.get("settlement_cost", payload.get("settlementCost")), Decimal("0")),
        redeem_cost=decimal_or_default(payload.get("redeem_cost", payload.get("redeemCost")), Decimal("0")),
        capital_cost_bps=decimal_or_default(payload.get("capital_cost_bps", payload.get("capitalCostBps")), Decimal("0")),
        cancel_after_blocks=int_or_default(payload.get("cancel_after_blocks", payload.get("cancelAfterBlocks")), 0),
        cancel_ack_delay_blocks=int_or_default(payload.get("cancel_ack_delay_blocks", payload.get("cancelAckDelayBlocks")), 0),
        cancel_fail=bool_or_default(payload.get("cancel_fail", payload.get("cancelFail")), False),
    )


def _decimal_text(value: Decimal) -> str:
    return format(Decimal(str(value)).normalize(), "f")


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return _decimal_text(value) if value is not None else None


def backtest_parameter_snapshot(
    *,
    market_slug: str,
    token_side: str,
    token_id: str | None,
    outcome_label: str | None,
    price_source: str,
    backtest_engine: str,
    from_ts: int | None,
    to_ts: int | None,
    from_block: int | None,
    to_block: int | None,
    params: BacktestParameters,
    execution_context: dict[str, Any] | None = None,
    pml2_l2_source: str | None = None,
) -> dict[str, Any]:
    snapshot = {
        "strategy": BACKTEST_STRATEGY_VERSION,
        "strategy_name": BACKTEST_STRATEGY_NAME,
        "strategy_version": BACKTEST_STRATEGY_VERSION,
        "market_slug": market_slug,
        "token_side": token_side,
        "token_id": token_id,
        "outcome_label": outcome_label,
        "price_source": price_source,
        "backtest_engine": backtest_engine,
        "from_ts": from_ts,
        "to_ts": to_ts,
        "from_block": from_block,
        "to_block": to_block,
        "parameters": {
            "entry_threshold": _decimal_text(params.entry_threshold),
            "exit_threshold": _decimal_text(params.exit_threshold),
            "entry_signal_price_field": params.entry_signal_price_field,
            "exit_signal_price_field": params.exit_signal_price_field,
            "entry_use_block_range": bool(params.entry_use_block_range),
            "exit_use_block_range": bool(params.exit_use_block_range),
            "signal_min_trade_count": int(params.signal_min_trade_count),
            "signal_min_block_volume": _decimal_text(params.signal_min_block_volume),
            "stop_loss": _decimal_text(params.stop_loss),
            "take_profit": _decimal_text(params.take_profit),
            "max_holding_bars": int(params.max_holding_bars),
            "initial_capital": _decimal_text(params.initial_capital),
            "position_size": _decimal_text(params.position_size),
            "fee_bps": _decimal_text(params.fee_bps),
            "maker_fee_bps": _optional_decimal_text(params.maker_fee_bps),
            "taker_fee_bps": _optional_decimal_text(params.taker_fee_bps),
            "maker_rebate_bps": _decimal_text(params.maker_rebate_bps),
            "slippage_bps": _decimal_text(params.slippage_bps),
            "liquidity_cap_pct": _decimal_text(params.liquidity_cap_pct),
            "max_position_notional": _decimal_text(params.max_position_notional),
            "min_fill_pct": _decimal_text(params.min_fill_pct),
            "execution_price_mode": params.execution_price_mode,
            "execution_profile": params.execution_profile,
            "pml2_audit_mode": params.pml2_audit_mode,
            "order_role": params.order_role,
            "latency_blocks": int(params.latency_blocks),
            "adverse_slippage_cents": _decimal_text(params.adverse_slippage_cents),
            "fill_probability_haircut_pct": _decimal_text(params.fill_probability_haircut_pct),
            "latency_seconds": _decimal_text(params.latency_seconds),
            "max_book_staleness_seconds": _decimal_text(params.max_book_staleness_seconds),
            "allow_partial_fill": bool(params.allow_partial_fill),
            "min_fill_size": _decimal_text(params.min_fill_size),
            "reject_on_stale_book": bool(params.reject_on_stale_book),
            "final_valuation_mode": params.final_valuation_mode,
            "max_entry_price": _decimal_text(params.max_entry_price),
            "min_exit_price": _decimal_text(params.min_exit_price),
            "buy_limit_price": _optional_decimal_text(params.buy_limit_price),
            "sell_limit_price": _optional_decimal_text(params.sell_limit_price),
            "settlement_value": _optional_decimal_text(params.settlement_value),
            "gas_cost_per_order": _decimal_text(params.gas_cost_per_order),
            "settlement_cost": _decimal_text(params.settlement_cost),
            "redeem_cost": _decimal_text(params.redeem_cost),
            "capital_cost_bps": _decimal_text(params.capital_cost_bps),
            "cancel_after_blocks": int(params.cancel_after_blocks),
            "cancel_ack_delay_blocks": int(params.cancel_ack_delay_blocks),
            "cancel_fail": bool(params.cancel_fail),
        },
    }
    if pml2_l2_source is not None:
        snapshot["pml2_l2_source"] = pml2_l2_source
    if execution_context:
        snapshot["execution_context"] = execution_context
    return snapshot


def backtest_parameter_fingerprint(snapshot: dict[str, Any]) -> str:
    payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _json_safe_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _sanitize_json_value(value: Any, *, depth: int = 0, max_items: int = 24) -> Any:
    if depth > 6:
        if isinstance(value, dict):
            return {"details_omitted": True, "item_count": len(value)}
        if isinstance(value, list):
            return []
        return _json_safe_scalar(value)
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, inner in list(value.items())[:max_items]:
            if not isinstance(key, str):
                key = str(key)
            result[key[:80]] = _sanitize_json_value(inner, depth=depth + 1, max_items=max_items)
        return result
    if isinstance(value, list):
        return [_sanitize_json_value(item, depth=depth + 1, max_items=max_items) for item in value[:max_items]]
    return _json_safe_scalar(value)


def _compact_data_quality_for_run_meta(report: dict[str, Any]) -> dict[str, Any]:
    """Keep run metadata bounded; canonical evidence lives in normalized tables."""

    def compact(value: Any, *, depth: int = 0) -> Any:
        if depth > 8:
            if isinstance(value, dict):
                return {"details_omitted": True, "item_count": len(value)}
            if isinstance(value, list):
                return []
            return _json_safe_scalar(value)
        if isinstance(value, list):
            return [compact(item, depth=depth + 1) for item in value[:64]]
        if isinstance(value, dict):
            items = list(value.items())
            bounded = items[:256]
            result = {
                str(key)[:80]: compact(inner, depth=depth + 1)
                for key, inner in bounded
            }
            if len(items) > len(bounded):
                result["_storage_summary"] = {
                    "details_omitted": True,
                    "item_count": len(items),
                    "persisted_items": len(bounded),
                }
            return result
        return _json_safe_scalar(value)

    return compact(report)


def _signal_price(point: PricePoint, field: str) -> Decimal:
    if field == "price":
        return Decimal(str(point.price))
    value = getattr(point, field, None)
    if value in (None, ""):
        return Decimal(str(point.price))
    return Decimal(str(value))


def _signal_common_context(point: PricePoint, *, field: str, min_trade_count: int, min_block_volume: Decimal) -> dict[str, Any]:
    trade_count = int(point.trade_count or 0)
    block_volume = max(Decimal("0"), Decimal(str(point.volume or 0)))
    eligible = True
    blocked_reason = ""
    if trade_count < int(min_trade_count or 0):
        eligible = False
        blocked_reason = "below_signal_min_trade_count"
    elif block_volume < max(Decimal("0"), Decimal(str(min_block_volume or 0))):
        eligible = False
        blocked_reason = "below_signal_min_block_volume"
    decision_price = _signal_price(point, field)
    return {
        "signal_price": decision_price.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "signal_price_field": field,
        "signal_trade_count": trade_count,
        "signal_block_volume": block_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "signal_min_trade_count": int(min_trade_count or 0),
        "signal_min_block_volume": max(Decimal("0"), Decimal(str(min_block_volume or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "eligible": eligible,
        "blocked_reason": blocked_reason,
    }


def _entry_signal_context(point: PricePoint, params: BacktestParameters) -> dict[str, Any]:
    context = _signal_common_context(
        point,
        field=params.entry_signal_price_field,
        min_trade_count=params.signal_min_trade_count,
        min_block_volume=params.signal_min_block_volume,
    )
    threshold = Decimal(str(params.entry_threshold)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    decision_price = Decimal(str(context["signal_price"]))
    triggered = decision_price >= threshold
    trigger_field = str(context["signal_price_field"])
    block_range_used = False
    if not triggered and bool(params.entry_use_block_range) and point.high_price is not None:
        high_price = Decimal(str(point.high_price)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        if high_price >= threshold:
            triggered = True
            trigger_field = "high_price"
            decision_price = high_price
            block_range_used = True
    return {
        **context,
        "signal_side": "BUY_YES",
        "threshold": threshold,
        "decision_price": decision_price,
        "triggered": bool(context["eligible"] and triggered),
        "crossed": triggered,
        "signal_reason": "entry_threshold" if triggered else str(context["blocked_reason"] or "entry_threshold_not_crossed"),
        "signal_trigger_field": trigger_field,
        "signal_block_range_enabled": bool(params.entry_use_block_range),
        "signal_block_range_used": block_range_used,
    }


def _exit_signal_context(
    point: PricePoint,
    entry_price: Decimal,
    holding_bars: int,
    params: BacktestParameters,
) -> dict[str, Any]:
    context = _signal_common_context(
        point,
        field=params.exit_signal_price_field,
        min_trade_count=params.signal_min_trade_count,
        min_block_volume=params.signal_min_block_volume,
    )
    decision_price = Decimal(str(context["signal_price"]))
    trigger_field = str(context["signal_price_field"])
    block_range_used = False
    exit_threshold = Decimal(str(params.exit_threshold)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    stop_price = (Decimal(str(entry_price)) * (Decimal("1") - Decimal(str(params.stop_loss)))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    take_price = (Decimal(str(entry_price)) * (Decimal("1") + Decimal(str(params.take_profit)))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)

    def _use_range(field_name: str, threshold: Decimal, *, direction: str) -> tuple[bool, Decimal]:
        raw = getattr(point, field_name, None)
        if raw is None:
            return False, decision_price
        value = Decimal(str(raw)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        if direction == "le" and value <= threshold:
            return True, value
        if direction == "ge" and value >= threshold:
            return True, value
        return False, value

    reason: str | None = None
    if decision_price <= exit_threshold:
        reason = "exit_threshold"
    elif bool(params.exit_use_block_range):
        crossed, value = _use_range("low_price", exit_threshold, direction="le")
        if crossed:
            reason = "exit_threshold"
            decision_price = value
            trigger_field = "low_price"
            block_range_used = True

    if reason is None:
        if decision_price <= stop_price:
            reason = "stop_loss"
        elif bool(params.exit_use_block_range):
            crossed, value = _use_range("low_price", stop_price, direction="le")
            if crossed:
                reason = "stop_loss"
                decision_price = value
                trigger_field = "low_price"
                block_range_used = True

    if reason is None:
        if decision_price >= take_price:
            reason = "take_profit"
        elif bool(params.exit_use_block_range):
            crossed, value = _use_range("high_price", take_price, direction="ge")
            if crossed:
                reason = "take_profit"
                decision_price = value
                trigger_field = "high_price"
                block_range_used = True

    if reason is None and holding_bars >= params.max_holding_bars:
        reason = "max_holding_bars"

    return {
        **context,
        "signal_side": "SELL_YES",
        "decision_price": decision_price,
        "reason": reason if context["eligible"] else None,
        "signal_reason": str(reason or context["blocked_reason"] or ""),
        "signal_trigger_field": trigger_field,
        "signal_block_range_enabled": bool(params.exit_use_block_range),
        "signal_block_range_used": block_range_used,
        "exit_threshold": exit_threshold,
        "stop_price": stop_price,
        "take_profit_price": take_price,
    }


def _with_signal_context(fill: dict[str, Any], signal_context: Mapping[str, Any]) -> dict[str, Any]:
    enriched = dict(fill)
    for key in (
        "signal_price",
        "signal_price_field",
        "signal_trade_count",
        "signal_block_volume",
        "signal_min_trade_count",
        "signal_min_block_volume",
        "signal_reason",
        "signal_trigger_field",
        "signal_block_range_enabled",
        "signal_block_range_used",
    ):
        if key in signal_context:
            enriched[key] = signal_context[key]
    if "threshold" in signal_context:
        enriched["signal_threshold"] = signal_context["threshold"]
    if "exit_threshold" in signal_context:
        enriched["signal_exit_threshold"] = signal_context["exit_threshold"]
    if "stop_price" in signal_context:
        enriched["signal_stop_price"] = signal_context["stop_price"]
    if "take_profit_price" in signal_context:
        enriched["signal_take_profit_price"] = signal_context["take_profit_price"]
    return enriched


def _execution_context_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    raw = payload.get("execution_context", payload.get("executionContext"))
    if not isinstance(raw, dict):
        raw = {}
    context = _sanitize_json_value(raw)
    if not isinstance(context, dict):
        context = {}
    mode = normalize_execution_price_mode(payload.get("execution_price_mode", payload.get("executionPriceMode", ORDERFILLED_CROSS_MODE)))
    context.setdefault("model", BACKTEST_STRATEGY_VERSION)
    context.setdefault("strategy_name", BACKTEST_STRATEGY_NAME)
    context.setdefault("strategy_version", BACKTEST_STRATEGY_VERSION)
    if is_orderfilled_v2_tape_mode(mode):
        context.setdefault("fill_model", "orderfilled_v2_trade_tape_taker_participation")
        context.setdefault("fill_model_version", "orderfilled_v2_trade_tape_v1")
    elif is_orderfilled_v3_trade_mode(mode):
        profile = _v3_profile_name(
            payload.get("execution_profile", payload.get("executionProfile", "realistic"))
        )
        context.setdefault("fill_model", "fill_only_v3_trade_only_expected_execution")
        context.setdefault("fill_model_version", get_trade_only_profile(profile).model_version)
        context.setdefault("observed_modeled_accounting", "SEPARATE")
        context.setdefault("uses_lob_data", False)
    else:
        context.setdefault(
            "fill_model",
            "orderfilled_cross_then_settlement" if is_orderfilled_cross_mode(mode) else "orderfilled_probability_with_participation_cap",
        )
        context.setdefault(
            "fill_model_version",
            BACKTEST_LIMIT_REPLAY_FILL_MODEL_VERSION if is_orderfilled_cross_mode(mode) else BACKTEST_PROBABILITY_FILL_MODEL_VERSION,
        )
    context.setdefault("fee_model_version", BACKTEST_FEE_MODEL_VERSION)
    context.setdefault("slippage_model_version", BACKTEST_SLIPPAGE_MODEL_VERSION)
    return context


def _load_event_outcome_snapshot(
    conn: Any,
    *,
    market_slug: str,
    to_block: int | None,
) -> dict[str, Any]:
    """Capture the event universe and its latest prices at the run boundary."""

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH selected_event AS (
                SELECT event_slug
                FROM quant.market_event_members
                WHERE market_slug = %s
                LIMIT 1
            ),
            snapshot_bound AS (
                SELECT %s::bigint AS to_block
            )
            SELECT m.event_slug,
                   m.event_id,
                   em.event_title,
                   em.event_category,
                   m.market_id,
                   m.market_slug,
                   m.question,
                   m.outcome_label,
                   m.outcome_key,
                   m.outcome_order,
                   m.token_yes_id,
                   m.token_no_id,
                   m.status,
                   m.active,
                   m.closed,
                   m.resolved,
                   yes_price.close_price AS yes_probability,
                   yes_price.block_number AS yes_block,
                   yes_price.block_timestamp AS yes_timestamp,
                   no_price.close_price AS no_probability,
                   no_price.block_number AS no_block,
                   no_price.block_timestamp AS no_timestamp
            FROM quant.market_event_members m
            JOIN selected_event selected ON selected.event_slug = m.event_slug
            CROSS JOIN snapshot_bound bound
            LEFT JOIN quant.market_event_metadata em ON em.event_slug = m.event_slug
            LEFT JOIN LATERAL (
                SELECT p.close_price, p.block_number, p.block_timestamp
                FROM quant.market_token_block_close p
                WHERE p.token_id = m.token_yes_id
                  AND (bound.to_block IS NULL OR p.block_number <= bound.to_block)
                ORDER BY p.block_number DESC
                LIMIT 1
            ) yes_price ON TRUE
            LEFT JOIN LATERAL (
                SELECT p.close_price, p.block_number, p.block_timestamp
                FROM quant.market_token_block_close p
                WHERE p.token_id = m.token_no_id
                  AND (bound.to_block IS NULL OR p.block_number <= bound.to_block)
                ORDER BY p.block_number DESC
                LIMIT 1
            ) no_price ON TRUE
            ORDER BY m.outcome_order NULLS LAST, m.market_id
            """,
            (market_slug, to_block),
        )
        source_rows = [dict(row) for row in cur.fetchall()]

    outcomes: list[dict[str, Any]] = []
    for row in source_rows:
        outcomes.append(
            {
                key: _json_safe_scalar(row.get(key))
                for key in (
                    "market_id",
                    "market_slug",
                    "question",
                    "outcome_label",
                    "outcome_key",
                    "outcome_order",
                    "token_yes_id",
                    "token_no_id",
                    "status",
                    "active",
                    "closed",
                    "resolved",
                    "yes_probability",
                    "yes_block",
                    "yes_timestamp",
                    "no_probability",
                    "no_block",
                    "no_timestamp",
                )
            }
        )

    first = source_rows[0] if source_rows else {}
    yes_count = sum(row.get("yes_probability") is not None for row in source_rows)
    no_count = sum(row.get("no_probability") is not None for row in source_rows)
    pair_count = sum(
        row.get("yes_probability") is not None and row.get("no_probability") is not None
        for row in source_rows
    )
    return {
        "schema_version": "event_outcome_snapshot_v1",
        "source": "quant.market_event_members+quant.market_token_block_close",
        "event_slug": _json_safe_scalar(first.get("event_slug")),
        "event_id": _json_safe_scalar(first.get("event_id")),
        "event_title": _json_safe_scalar(first.get("event_title")),
        "market_category": _json_safe_scalar(first.get("event_category")),
        "snapshot_to_block": to_block,
        "event_outcome_count": len(outcomes),
        "priced_yes_outcome_count": yes_count,
        "priced_no_outcome_count": no_count,
        "priced_pair_count": pair_count,
        "price_coverage_pct": _decimal_text(
            Decimal(yes_count * 100) / Decimal(len(outcomes)) if outcomes else Decimal("0")
        ),
        "outcomes": outcomes,
    }


def _load_event_outcome_correlation(
    conn: Any,
    *,
    event_snapshot: Mapping[str, Any],
    from_block: int | None,
    to_block: int | None,
) -> dict[str, Any]:
    """Build a bounded, block-aligned correlation summary for event outcomes."""

    method = "block_grid_locf_probability_change_pearson_v1"
    outcomes = [
        row
        for row in event_snapshot.get("outcomes", [])
        if isinstance(row, Mapping) and _optional_text(row.get("token_yes_id"))
    ]
    if len(outcomes) < 2:
        return _empty_event_correlation(method, "fewer than two event outcomes have YES tokens")
    if from_block is None or to_block is None or int(to_block) <= int(from_block):
        return _empty_event_correlation(method, "explicit increasing run block bounds are required")

    start = int(from_block)
    end = int(to_block)
    span = end - start
    target_samples = min(EVENT_CORRELATION_MAX_GRID_SAMPLES, span + 1)
    step = max(1, (span + max(1, target_samples - 1) - 1) // max(1, target_samples - 1))
    token_ids = [str(row["token_yes_id"]) for row in outcomes]
    outcome_keys = [
        str(row.get("outcome_key") or row.get("market_slug") or row.get("market_id") or f"outcome-{index + 1}")
        for index, row in enumerate(outcomes)
    ]
    outcome_orders = [int(row.get("outcome_order") or index) for index, row in enumerate(outcomes)]

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH tokens AS (
                SELECT *
                FROM unnest(%s::text[], %s::text[], %s::integer[])
                    AS token(token_id, outcome_key, outcome_order)
            ),
            sample_grid AS (
                SELECT generate_series(%s::bigint, %s::bigint, %s::bigint) AS sample_block
                UNION
                SELECT %s::bigint
            )
            SELECT token.outcome_key,
                   token.outcome_order,
                   grid.sample_block,
                   price.close_price
            FROM tokens token
            CROSS JOIN sample_grid grid
            LEFT JOIN LATERAL (
                SELECT p.close_price
                FROM quant.market_token_block_close p
                WHERE p.token_id = token.token_id
                  AND p.block_number >= %s
                  AND p.block_number <= grid.sample_block
                ORDER BY p.block_number DESC
                LIMIT 1
            ) price ON TRUE
            WHERE price.close_price IS NOT NULL
            ORDER BY token.outcome_order, grid.sample_block
            """,
            (token_ids, outcome_keys, outcome_orders, start, end, step, end, start),
        )
        rows = [dict(row) for row in cur.fetchall()]

    series: dict[str, dict[int, Decimal]] = {key: {} for key in outcome_keys}
    for row in rows:
        key = str(row["outcome_key"])
        series.setdefault(key, {})[int(row["sample_block"])] = Decimal(str(row["close_price"]))
    changes = {key: _series_probability_changes(points) for key, points in series.items()}
    matrix: dict[str, dict[str, str | None]] = {
        key: {key: "1" if changes[key] else None}
        for key in outcome_keys
    }
    valid_pair_samples: list[int] = []
    correlations: list[Decimal] = []
    for left_index, left_key in enumerate(outcome_keys):
        for right_key in outcome_keys[left_index + 1 :]:
            shared_blocks = sorted(set(changes[left_key]).intersection(changes[right_key]))
            correlation = None
            if len(shared_blocks) >= EVENT_CORRELATION_MIN_PAIR_SAMPLES:
                correlation = _pearson_decimal(
                    [changes[left_key][block] for block in shared_blocks],
                    [changes[right_key][block] for block in shared_blocks],
                )
            rendered = _rounded_decimal_text(correlation, places=6) if correlation is not None else None
            matrix[left_key][right_key] = rendered
            matrix.setdefault(right_key, {})[left_key] = rendered
            if correlation is not None:
                correlations.append(correlation)
                valid_pair_samples.append(len(shared_blocks))

    grid_sample_count = ((end - start) // step) + 1 + (0 if (end - start) % step == 0 else 1)
    if not correlations:
        return {
            **_empty_event_correlation(method, "no outcome pair has enough non-constant aligned probability changes"),
            "from_block": start,
            "to_block": end,
            "grid_step_blocks": step,
            "grid_sample_count": grid_sample_count,
            "outcome_count": len(outcome_keys),
            "matrix": matrix,
        }
    max_abs = max(abs(value) for value in correlations)
    return {
        "status": "ready",
        "reason": "event outcomes are aligned on a bounded block grid before probability-change correlation",
        "method": method,
        "source": "quant.market_token_block_close",
        "from_block": start,
        "to_block": end,
        "grid_step_blocks": step,
        "grid_sample_count": grid_sample_count,
        "sample_count": min(valid_pair_samples),
        "max_pair_sample_count": max(valid_pair_samples),
        "outcome_count": len(outcome_keys),
        "pair_count": len(correlations),
        "max_abs_correlation": _rounded_decimal_text(max_abs, places=6),
        "matrix": matrix,
    }


def _empty_event_correlation(method: str, reason: str) -> dict[str, Any]:
    return {
        "status": "review",
        "reason": reason,
        "method": method,
        "source": "quant.market_token_block_close",
        "sample_count": 0,
        "outcome_count": 0,
        "pair_count": 0,
        "max_abs_correlation": None,
        "matrix": {},
    }


def _series_probability_changes(points: Mapping[int, Decimal]) -> dict[int, Decimal]:
    changes: dict[int, Decimal] = {}
    previous: Decimal | None = None
    for block in sorted(points):
        value = Decimal(str(points[block]))
        if previous is not None:
            changes[int(block)] = value - previous
        previous = value
    return changes


def _pearson_decimal(left: list[Decimal], right: list[Decimal]) -> Decimal | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    count = Decimal(len(left))
    left_mean = sum(left, Decimal("0")) / count
    right_mean = sum(right, Decimal("0")) / count
    left_centered = [value - left_mean for value in left]
    right_centered = [value - right_mean for value in right]
    left_variance = sum((value * value for value in left_centered), Decimal("0"))
    right_variance = sum((value * value for value in right_centered), Decimal("0"))
    if left_variance == 0 or right_variance == 0:
        return None
    covariance = sum(
        (left_value * right_value for left_value, right_value in zip(left_centered, right_centered, strict=True)),
        Decimal("0"),
    )
    value = covariance / (left_variance * right_variance).sqrt()
    return min(Decimal("1"), max(Decimal("-1"), value))


def _rounded_decimal_text(value: Decimal, *, places: int) -> str:
    quantum = Decimal("1").scaleb(-int(places))
    rounded = value.quantize(quantum, rounding=ROUND_HALF_UP)
    return "0" if rounded == 0 else _decimal_text(rounded)


def _code_provenance() -> dict[str, Any]:
    env_commit = os.environ.get("POLYDATA_CODE_COMMIT") or os.environ.get("GIT_COMMIT")
    if env_commit:
        return {"code_commit": env_commit, "code_dirty": None, "code_source": "env"}
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=os.getcwd(),
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        ).strip()
        dirty = subprocess.check_output(
            ["git", "status", "--short", "--untracked-files=no"],
            cwd=os.getcwd(),
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2,
        ).strip()
        return {"code_commit": commit or None, "code_dirty": bool(dirty), "code_source": "git"}
    except Exception:
        return {"code_commit": None, "code_dirty": None, "code_source": "unknown"}


def create_and_execute_backtest(conn: Any, payload: dict[str, Any]) -> dict[str, Any]:
    run_id = create_backtest_run(conn, payload)
    try:
        execute_backtest_run(conn, run_id)
    except BacktestRunCancelled as exc:
        mark_run_cancelled(conn, run_id, str(exc))
    except Exception as exc:
        mark_run_failed(conn, run_id, str(exc))
    return get_backtest_run_for_update_free(conn, run_id)


def list_queued_backtest_run_ids(conn: Any, *, limit: int = 10) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM quant.quant_backtest_runs
            WHERE status = 'queued'
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        return [int(row["run_id"]) for row in cur.fetchall()]


def _upsert_backtest_run_progress(
    conn: Any,
    run_id: int,
    *,
    status: str,
    phase: str,
    progress: int | float,
    current_x: int | None = None,
    x_axis: str | None = None,
    rows_processed: int | None = None,
    total_rows: int | None = None,
    eta_seconds: float | None = None,
    worker_id: str | None = None,
    error: str | None = None,
    artifact_summary: Mapping[str, Any] | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_run_progress (
                run_id, status, phase, progress, current_x, x_axis,
                rows_processed, total_rows, eta_seconds, worker_id, error,
                artifact_summary, started_at, finished_at, updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, COALESCE(%s, 0), %s, %s, %s, %s,
                COALESCE(%s::jsonb, '{}'::jsonb),
                CASE WHEN %s = 'running' THEN clock_timestamp() ELSE NULL END,
                CASE WHEN %s IN ('succeeded', 'failed', 'cancelled') THEN clock_timestamp() ELSE NULL END,
                clock_timestamp()
            )
            ON CONFLICT (run_id) DO UPDATE SET
                status = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN 'cancelled'
                    ELSE EXCLUDED.status
                END,
                phase = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.phase
                    ELSE EXCLUDED.phase
                END,
                progress = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.progress
                    ELSE EXCLUDED.progress
                END,
                current_x = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.current_x
                    ELSE COALESCE(EXCLUDED.current_x, quant.quant_backtest_run_progress.current_x)
                END,
                x_axis = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.x_axis
                    ELSE COALESCE(EXCLUDED.x_axis, quant.quant_backtest_run_progress.x_axis)
                END,
                rows_processed = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.rows_processed
                    WHEN %s::bigint IS NULL THEN quant.quant_backtest_run_progress.rows_processed
                    ELSE GREATEST(quant.quant_backtest_run_progress.rows_processed, EXCLUDED.rows_processed)
                END,
                total_rows = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.total_rows
                    ELSE COALESCE(EXCLUDED.total_rows, quant.quant_backtest_run_progress.total_rows)
                END,
                eta_seconds = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.eta_seconds
                    ELSE EXCLUDED.eta_seconds
                END,
                worker_id = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.worker_id
                    ELSE COALESCE(EXCLUDED.worker_id, quant.quant_backtest_run_progress.worker_id)
                END,
                error = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.error
                    WHEN EXCLUDED.status = 'failed' THEN COALESCE(EXCLUDED.error, quant.quant_backtest_run_progress.error)
                    ELSE NULL
                END,
                artifact_summary = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.artifact_summary
                    WHEN %s::boolean THEN EXCLUDED.artifact_summary
                    WHEN EXCLUDED.status IN ('queued', 'running', 'failed', 'cancelled') THEN '{}'::jsonb
                    ELSE quant.quant_backtest_run_progress.artifact_summary
                END,
                started_at = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.started_at
                    WHEN EXCLUDED.status = 'queued' THEN NULL
                    WHEN EXCLUDED.status = 'running' THEN COALESCE(quant.quant_backtest_run_progress.started_at, EXCLUDED.started_at)
                    ELSE quant.quant_backtest_run_progress.started_at
                END,
                finished_at = CASE
                    WHEN quant.quant_backtest_run_progress.status = 'cancelled' THEN quant.quant_backtest_run_progress.finished_at
                    WHEN EXCLUDED.status IN ('succeeded', 'failed', 'cancelled') THEN EXCLUDED.finished_at
                    ELSE NULL
                END,
                updated_at = clock_timestamp()
            """,
            (
                int(run_id),
                str(status),
                str(phase),
                max(0, min(100, int(round(float(progress))))),
                int(current_x) if current_x is not None else None,
                str(x_axis) if x_axis else None,
                int(rows_processed) if rows_processed is not None else None,
                int(total_rows) if total_rows is not None else None,
                round(max(0.0, float(eta_seconds)), 1) if eta_seconds is not None else None,
                str(worker_id) if worker_id else None,
                str(error)[:4000] if error else None,
                json.dumps(dict(artifact_summary), default=str) if artifact_summary is not None else None,
                str(status),
                str(status),
                int(rows_processed) if rows_processed is not None else None,
                artifact_summary is not None,
            ),
        )


def claim_backtest_run(conn: Any, run_id: int, *, worker_id: str | None = None) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_runs
            SET status = 'running',
                started_at = CASE WHEN started_at IS NULL THEN clock_timestamp() ELSE started_at END,
                finished_at = NULL,
                error = NULL
            WHERE run_id = %s
              AND status = 'queued'
            RETURNING run_id
            """,
            (int(run_id),),
        )
        claimed = cur.fetchone() is not None
    if claimed:
        _upsert_backtest_run_progress(
            conn,
            run_id,
            status="running",
            phase="claimed by backtest worker",
            progress=2,
            rows_processed=0,
            worker_id=worker_id,
        )
    return claimed


def update_backtest_run_progress(
    conn: Any,
    run_id: int,
    *,
    phase: str,
    progress: int | float,
    status: str = "running",
    current_x: int | None = None,
    x_axis: str | None = None,
    rows_processed: int | None = None,
    total_rows: int | None = None,
    eta_seconds: float | None = None,
    worker_id: str | None = None,
) -> None:
    """Persist observable worker progress without changing backtest semantics."""

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_runs
            SET rows_processed = CASE
                    WHEN %s IS NULL THEN rows_processed
                    ELSE GREATEST(rows_processed, %s)
                END
            WHERE run_id = %s
              AND status <> 'cancelled'
            """,
            (
                rows_processed,
                rows_processed,
                int(run_id),
            ),
        )
    _upsert_backtest_run_progress(
        conn,
        run_id,
        status=status,
        phase=phase,
        progress=progress,
        current_x=current_x,
        x_axis=x_axis,
        rows_processed=rows_processed,
        total_rows=total_rows,
        eta_seconds=eta_seconds,
        worker_id=worker_id,
    )


def is_backtest_run_cancelled(conn: Any, run_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM quant.quant_backtest_runs WHERE run_id = %s", (int(run_id),))
        row = cur.fetchone()
    if not row:
        return False
    status = row.get("status") if isinstance(row, Mapping) else row[0]
    return str(status or "").lower() == "cancelled"


def _raise_if_backtest_run_cancelled(conn: Any, run_id: int) -> None:
    if is_backtest_run_cancelled(conn, run_id):
        raise BacktestRunCancelled(f"Backtest run #{run_id} cancelled by user")


def cancel_backtest_run(conn: Any, run_id: int, reason: str = "cancelled by user") -> dict[str, Any] | None:
    """Cancel a queued/running run without reclassifying terminal artifacts."""

    normalized_reason = str(reason or "cancelled by user").strip()[:500] or "cancelled by user"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_runs
            SET status = 'cancelled',
                error = NULL,
                finished_at = clock_timestamp()
            WHERE run_id = %s
              AND status IN ('queued', 'running')
            RETURNING *
            """,
            (int(run_id),),
        )
        updated = cur.fetchone()
        if updated is None:
            cur.execute("SELECT * FROM quant.quant_backtest_runs WHERE run_id = %s", (int(run_id),))
            current = cur.fetchone()
        else:
            current = updated
        cur.execute(
            """
            SELECT progress, current_x, x_axis, rows_processed, total_rows
            FROM quant.quant_backtest_run_progress
            WHERE run_id = %s
            """,
            (int(run_id),),
        )
        progress_row = cur.fetchone()
    if current is None:
        return None
    row = dict(current)
    if str(row.get("status") or "").lower() != "cancelled":
        return row
    progress = dict(progress_row) if isinstance(progress_row, Mapping) else {}
    _upsert_backtest_run_progress(
        conn,
        run_id,
        status="cancelled",
        phase=normalized_reason,
        progress=progress.get("progress") or 0,
        current_x=progress.get("current_x"),
        x_axis=progress.get("x_axis"),
        rows_processed=progress.get("rows_processed"),
        total_rows=progress.get("total_rows"),
        eta_seconds=0,
    )
    return row


def requeue_running_backtest_runs(conn: Any, *, worker_id: str) -> list[int]:
    """Recover jobs whose owning dev-stack worker exited during execution."""

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_runs r
            SET status = 'queued',
                rows_processed = 0,
                error = NULL,
                started_at = NULL,
                finished_at = NULL
            FROM quant.quant_backtest_run_progress p
            WHERE r.status = 'running'
              AND p.run_id = r.run_id
              AND p.worker_id = %s
            RETURNING r.run_id
            """,
            (str(worker_id),),
        )
        rows = cur.fetchall()
    run_ids = [int(row["run_id"] if isinstance(row, Mapping) else row[0]) for row in rows]
    for recovered_run_id in run_ids:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.quant_backtest_run_progress (
                    run_id, status, phase, progress, rows_processed, worker_id, updated_at
                )
                VALUES (%s, 'queued', 'recovered after worker restart', 0, 0, %s, clock_timestamp())
                ON CONFLICT (run_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    phase = EXCLUDED.phase,
                    progress = 0,
                    current_x = NULL,
                    x_axis = NULL,
                    rows_processed = 0,
                    total_rows = NULL,
                    eta_seconds = NULL,
                    worker_id = EXCLUDED.worker_id,
                    error = NULL,
                    started_at = NULL,
                    finished_at = NULL,
                    updated_at = clock_timestamp()
                """,
                (recovered_run_id, str(worker_id)),
            )
    return run_ids


def _emit_run_progress(callback: RunProgressCallback | None, **payload: Any) -> None:
    if not callable(callback):
        return
    try:
        callback(payload)
    except BacktestRunCancelled:
        raise
    except Exception:
        # Progress is observability only. A transient status-write failure must
        # never invalidate an otherwise deterministic backtest artifact.
        return


def create_backtest_run(conn: Any, payload: dict[str, Any]) -> int:
    payload = maybe_apply_approved_execution_profile_override(conn, payload)
    market_slug = str(payload.get("market_slug", payload.get("marketSlug")) or "").strip()
    if not market_slug:
        raise ValueError("market_slug is required")
    token_id = _optional_text(payload.get("token_id", payload.get("tokenId")))
    outcome_label = _optional_text(payload.get("outcome_label", payload.get("outcomeLabel")))
    token_side = str(payload.get("token_side", payload.get("tokenSide")) or "YES").strip().upper()
    if token_side not in {"YES", "NO"}:
        raise ValueError("token_side must be YES or NO")
    price_source = normalize_price_source(payload.get("price_source", payload.get("priceSource", "frontend")))
    backtest_engine = normalize_backtest_engine(
        payload.get("backtest_engine", payload.get("backtestEngine", payload.get("engine", payload.get("framework", "builtin"))))
    )
    params = parse_parameters(payload)
    pml2_l2_source = (
        normalize_pml2_l2_source(
            payload.get("pml2_l2_source", payload.get("pml2L2Source"))
            or os.environ.get("POLYDATA_QUANT_PML2_L2_SOURCE")
        )
        if is_prediction_l2_replay_v1_mode(params.execution_price_mode)
        else None
    )
    from_ts = _optional_int(payload.get("from_ts", payload.get("from")))
    to_ts = _optional_int(payload.get("to_ts", payload.get("to")))
    from_block = _optional_int(payload.get("from_block", payload.get("fromBlock")))
    to_block = _optional_int(payload.get("to_block", payload.get("toBlock")))
    execution_context = _execution_context_from_payload(payload)
    event_snapshot = _load_event_outcome_snapshot(
        conn,
        market_slug=market_slug,
        to_block=to_block,
    )
    outcome_correlation = _load_event_outcome_correlation(
        conn,
        event_snapshot=event_snapshot,
        from_block=from_block,
        to_block=to_block,
    )
    code_provenance = _code_provenance()
    parameter_snapshot = backtest_parameter_snapshot(
        market_slug=market_slug,
        token_side=token_side,
        token_id=token_id,
        outcome_label=outcome_label,
        price_source=price_source,
        backtest_engine=backtest_engine,
        from_ts=from_ts,
        to_ts=to_ts,
        from_block=from_block,
        to_block=to_block,
        params=params,
        execution_context=execution_context,
        pml2_l2_source=pml2_l2_source,
    )
    parameter_fingerprint = backtest_parameter_fingerprint(parameter_snapshot)
    model_versions = {
        "strategy_version": BACKTEST_STRATEGY_VERSION,
        "fill_model": execution_context.get("fill_model"),
        "fill_model_version": execution_context.get("fill_model_version"),
        "fee_model_version": execution_context.get("fee_model_version"),
        "slippage_model_version": execution_context.get("slippage_model_version"),
        "execution_price_mode": params.execution_price_mode,
        "execution_profile": params.execution_profile,
        "pml2_audit_mode": params.pml2_audit_mode,
        "pml2_l2_source": pml2_l2_source,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_runs (
                status, market_slug, token_side, price_source, backtest_engine,
                from_ts, to_ts, from_block, to_block, meta
            )
            VALUES ('queued', %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING run_id, created_at
            """,
            (
                market_slug,
                token_side,
                price_source,
                backtest_engine,
                from_ts,
                to_ts,
                from_block,
                to_block,
                json.dumps(
                    {
                        "strategy": BACKTEST_STRATEGY_VERSION,
                        "strategy_name": BACKTEST_STRATEGY_NAME,
                        "strategy_version": BACKTEST_STRATEGY_VERSION,
                        "backtest_engine": backtest_engine,
                        "artifact_schema_version": BACKTEST_ARTIFACT_SCHEMA_VERSION,
                        "code_commit": code_provenance.get("code_commit"),
                        "code_dirty": code_provenance.get("code_dirty"),
                        "code_source": code_provenance.get("code_source"),
                        "model_versions": model_versions,
                        "token_id": token_id,
                        "outcome_label": outcome_label,
                        "pml2_l2_source": pml2_l2_source,
                        "cashflow_events": _payload_cashflow_events(payload),
                        "execution_context": execution_context,
                        "event_context": {
                            key: value
                            for key, value in event_snapshot.items()
                            if key != "outcomes"
                        },
                        "event_outcomes": event_snapshot["outcomes"],
                        "outcome_correlation": outcome_correlation,
                        "parameter_fingerprint": parameter_fingerprint,
                        "parameter_snapshot": parameter_snapshot,
                    }
                ),
            ),
        )
        created_run = cur.fetchone()
        run_id = int(created_run["run_id"])
        run_created_at = created_run["created_at"]
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_run_progress (
                run_id, status, phase, progress, rows_processed, run_snapshot, created_at
            )
            VALUES (%s, 'queued', 'waiting for backtest worker', 0, 0, %s::jsonb, %s)
            """,
            (run_id, json.dumps(parameter_snapshot), run_created_at),
        )
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_parameters (
                run_id, entry_threshold, exit_threshold, stop_loss, take_profit,
                max_holding_bars, initial_capital, position_size,
                fee_bps, maker_fee_bps, taker_fee_bps, maker_rebate_bps,
                slippage_bps, liquidity_cap_pct,
                max_position_notional, min_fill_pct,
                execution_price_mode, execution_profile, pml2_audit_mode, order_role,
                latency_blocks, adverse_slippage_cents, fill_probability_haircut_pct,
                latency_seconds, max_book_staleness_seconds,
                allow_partial_fill, min_fill_size, reject_on_stale_book,
                final_valuation_mode, max_entry_price, min_exit_price,
                buy_limit_price, sell_limit_price, settlement_value,
                gas_cost_per_order, settlement_cost, redeem_cost, capital_cost_bps,
                cancel_after_blocks, cancel_ack_delay_blocks, cancel_fail
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                params.pml2_audit_mode,
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
                params.cancel_after_blocks,
                params.cancel_ack_delay_blocks,
                params.cancel_fail,
            ),
        )
    try:
        with conn.transaction():
            upsert_price_build_targets_for_market(
                conn,
                source=price_source,
                market_slug=market_slug,
                token_side=token_side,
                priority=2000,
                reason=target_reason("backtest_requested", {"run_id": run_id}),
                from_ts=from_ts,
                to_ts=to_ts,
                from_block=from_block,
                to_block=to_block,
            )
    except Exception:
        # Registering a builder target is only an acceleration hint.  A locked
        # target table must not invalidate an explicit run that already has a
        # bounded market/token/window.
        pass
    return run_id


def _build_persisted_artifact_summary(
    *,
    run_id: int,
    run: Mapping[str, Any],
    params: BacktestParameters,
    result: Mapping[str, Any],
    rows_processed: int,
) -> dict[str, Any]:
    """Build the first-paint audit from artifacts already held in memory."""

    def rows(name: str) -> list[Any]:
        value = result.get(name)
        return value if isinstance(value, list) else []

    def timestamped(name: str, *keys: str) -> int:
        count = 0
        for item in rows(name):
            if not isinstance(item, Mapping):
                continue
            meta = item.get("meta")
            if isinstance(meta, Mapping) and any(meta.get(key) for key in keys):
                count += 1
        return count

    # Import lazily because run_artifacts also consumes fill-quality helpers from
    # this module. Execution only reaches here after both modules are initialized.
    from quant.backtest.run_artifacts import build_backtest_run_artifact_summary

    summary_row = {
        **{
            key: run.get(key)
            for key in (
                "market_slug",
                "token_side",
                "price_source",
                "from_ts",
                "to_ts",
                "from_block",
                "to_block",
                "created_at",
                "started_at",
            )
        },
        "run_id": int(run_id),
        "status": "succeeded",
        "backtest_engine": str(result.get("actual_backtest_engine") or run.get("backtest_engine") or "builtin"),
        "rows_processed": int(rows_processed),
        "error": None,
        "finished_at": datetime.now(timezone.utc),
        "execution_price_mode": params.execution_price_mode,
        "execution_profile": params.execution_profile,
        "order_role": params.order_role,
        "final_valuation_mode": params.final_valuation_mode,
        "metric_count": len(rows("metrics")),
        "equity_count": len(rows("equity")),
        "trade_count": len(rows("trades")),
        "order_count": len(rows("orders")),
        "ledger_count": len(rows("ledger")),
        "event_count": len(rows("events")),
        "timestamped_order_count": timestamped("orders", "signal_timestamp", "submit_timestamp"),
        "timestamped_ledger_count": timestamped("ledger", "source_timestamp"),
        "timestamped_event_count": timestamped("events", "source_timestamp"),
    }
    return build_backtest_run_artifact_summary(summary_row, run_id=run_id)


def execute_backtest_run(
    conn: Any,
    run_id: int,
    *,
    progress_callback: RunProgressCallback | None = None,
) -> None:
    _raise_if_backtest_run_cancelled(conn, run_id)
    run = _get_run(conn, run_id)
    params = _get_parameters(conn, run_id)
    # A queue worker claims and commits the row before execution so its
    # out-of-transaction progress writer is not blocked by this connection.
    if progress_callback is None:
        _set_run_status(conn, run_id, "running")
    run["_progress_callback"] = progress_callback
    _emit_run_progress(progress_callback, phase="loading price window", progress=6, rows_processed=0)
    points = fetch_price_points(conn, run)
    if len(points) < 2:
        raise RuntimeError("not enough price rows for backtest")
    x_axis = "timestamp" if run["price_source"] == "frontend" else "block_number"
    _emit_run_progress(
        progress_callback,
        phase="price window loaded",
        progress=18,
        current_x=points[0].x_value,
        x_axis=x_axis,
        rows_processed=0,
        total_rows=len(points),
    )
    clob_snapshots: list[BookSnapshot]
    pmxt_provider: PmxtCompactBookProvider | None = None
    pmxt_setup_context: dict[str, Any] = {"status": "disabled", "reason": "execution mode does not require PMXT compact L2"}
    pml2_setup_context: dict[str, Any] = {
        "status": "disabled",
        "reason": "execution mode does not require PML2",
    }
    _emit_run_progress(
        progress_callback,
        phase="loading execution evidence",
        progress=24,
        current_x=points[0].x_value,
        x_axis=x_axis,
        rows_processed=0,
        total_rows=len(points),
    )
    if is_orderfilled_v2_tape_mode(params.execution_price_mode) or is_orderfilled_v3_trade_mode(
        params.execution_price_mode
    ):
        clob_snapshots = []
        run["_orderfilled_v2_token_context"] = _resolve_replay_token_context(conn, run)
    elif is_orderfilled_lob_mode(params.execution_price_mode):
        pmxt_provider, pmxt_setup_context = build_pmxt_compact_book_provider(conn, run)
        if pmxt_provider is not None:
            clob_snapshots = []
            run["_pmxt_compact_book_provider"] = pmxt_provider
        else:
            clob_snapshots = load_clob_execution_snapshots(conn, run, points=points, params=params)
    elif is_prediction_l2_replay_v1_mode(params.execution_price_mode):
        pmxt_provider, clob_snapshots, pml2_setup_context = setup_main_pml2_execution(
            conn,
            run,
            points,
            params,
        )
    else:
        clob_snapshots = load_clob_execution_snapshots(conn, run, points=points, params=params)
    if clob_snapshots:
        run["_clob_snapshots"] = [snapshot_to_dict(snapshot) for snapshot in clob_snapshots]
    data_quality_report = build_data_quality_report(points, run)
    replay_events: list[ReplayTradeEvent]
    if is_prediction_l2_replay_v1_mode(params.execution_price_mode):
        data_quality_report["execution_depth"] = {
            **pml2_setup_context,
            "requested_execution_mode": PREDICTION_L2_REPLAY_V1_MODE,
        }
    else:
        data_quality_report["execution_depth"] = (
            {**pmxt_setup_context, "requested_execution_mode": normalize_execution_price_mode(params.execution_price_mode)}
            if pmxt_provider is not None
            else _public_clob_execution_context(clob_snapshots, run=run, params=params, points=points)
        )
    if is_prediction_l2_replay_v1_mode(params.execution_price_mode):
        replay_events = []
        replay_context = {
            "status": "excluded",
            "reason": "PREDICTION_L2_REPLAY_V1 taker execution has no future OrderFilled gate",
            "execution_model": PREDICTION_L2_REPLAY_V1_MODE,
        }
    elif is_orderfilled_v3_trade_mode(params.execution_price_mode):
        replay_events = []
        replay_context = {
            "status": "excluded",
            "reason": "ORDERFILLED_V3_TRADE loads one-sided trade slices through RequiredTradeWindow",
            "execution_model": ORDERFILLED_V3_TRADE_MODE,
        }
    else:
        replay_events, replay_context = load_orderfilled_limit_replay_events(conn, run, points, params)
    run["_orderfilled_replay_context"] = replay_context
    if replay_events:
        run["_orderfilled_replay_events"] = replay_events
    data_quality_report["orderfilled_replay"] = replay_context
    _emit_run_progress(
        progress_callback,
        phase="simulating strategy and fills",
        progress=38,
        current_x=points[0].x_value,
        x_axis=x_axis,
        rows_processed=0,
        total_rows=len(points),
    )
    result = run_framework_backtest(
        run.get("backtest_engine") or "builtin",
        points,
        run,
        params,
        builtin_simulator=simulate_strategy,
        metrics_builder=build_metrics,
    )
    _emit_run_progress(
        progress_callback,
        phase="validating execution evidence",
        progress=82,
        current_x=points[-1].x_value,
        x_axis=x_axis,
        rows_processed=len(points),
        total_rows=len(points),
    )
    if isinstance(result.get("orderfilled_v2"), dict):
        data_quality_report["orderfilled_v2"] = _sanitize_json_value(result["orderfilled_v2"], max_items=80)
    if isinstance(result.get("fill_only_v3"), dict):
        data_quality_report["fill_only_v3"] = _sanitize_json_value(
            result["fill_only_v3"], max_items=80
        )
    pml2_session = run.get("_pml2_session")
    if isinstance(pml2_session, ReplayExecutionSession):
        pml2_report = pml2_session.report()
        result["pml2_replay_v1"] = pml2_report
        data_quality_report["pml2_replay_v1"] = _sanitize_json_value(
            pml2_report, max_items=80
        )
        native_provider = run.get("_pml2_native_provider")
        if isinstance(native_provider, XueNativeExecutionProvider):
            data_quality_report["execution_depth"] = {
                **native_provider.context(),
                "requested_execution_mode": PREDICTION_L2_REPLAY_V1_MODE,
                "actual_execution_mode": PREDICTION_L2_REPLAY_V1_MODE,
            }
    if pmxt_provider is not None:
        clob_snapshots = pmxt_provider.resolved_snapshots()
        data_quality_report["execution_depth"] = {
            **pmxt_provider.context(),
            "requested_execution_mode": normalize_execution_price_mode(params.execution_price_mode),
            "actual_execution_mode": (
                PREDICTION_L2_REPLAY_V1_MODE
                if is_prediction_l2_replay_v1_mode(params.execution_price_mode)
                else ORDERFILLED_LOB_MODE
            ),
            "source_mode": (
                PML2_LEGACY_PMXT_SOURCE
                if is_prediction_l2_replay_v1_mode(params.execution_price_mode)
                else None
            ),
            "legacy_source_explicitly_requested": bool(
                is_prediction_l2_replay_v1_mode(params.execution_price_mode)
            ),
            "fallback_used": False,
        }
    if is_prediction_l2_replay_v1_mode(params.execution_price_mode):
        coverage_manifest = (
            pml2_session.coverage_manifest()
            if isinstance(pml2_session, ReplayExecutionSession)
            else None
        )
        data_quality_report["lob_execution_coverage"] = {
            "source": data_quality_report["execution_depth"].get("source"),
            "source_mode": data_quality_report["execution_depth"].get("source_mode"),
            "data_quality_status": (
                coverage_manifest.data_quality_status if coverage_manifest else "REJECTED"
            ),
            "transport_coverage_window_count": (
                coverage_manifest.transport_coverage_window_count
                if coverage_manifest
                else 0
            ),
            "transport_coverage_proof_ids": (
                list(coverage_manifest.transport_coverage_proof_ids)
                if coverage_manifest
                else []
            ),
            "fallback_used": bool(
                data_quality_report["execution_depth"].get("fallback_used")
            ),
        }
    else:
        data_quality_report["lob_execution_coverage"] = _build_lob_execution_coverage_context(
            result.get("orders", []),
            clob_snapshots,
            run=run,
            points=points,
            params=params,
        )
    real_order_state_events = load_real_order_state_events_for_run(conn, run_id)
    real_order_state_attached_count = attach_real_order_state_events(result.get("orders", []), real_order_state_events)
    data_quality_report["real_order_state"] = {
        "source": "quant.real_order_state_events",
        "event_count": len(real_order_state_events),
        "attached_order_count": real_order_state_attached_count,
    }
    platform_incidents = load_platform_incidents_for_run(conn, run, points)
    data_quality_report["platform_incidents"] = build_platform_incident_report(platform_incidents)
    fill_quality_report = build_fill_quality_report(
        result.get("orders", []),
        replay_context=replay_context,
        data_quality_report=data_quality_report,
    )
    result["fill_quality"] = fill_quality_report
    data_quality_report["fill_quality"] = fill_quality_report
    result.setdefault("metrics", [])
    result["metrics"].extend(fill_quality_metrics(fill_quality_report))
    result["metrics"].extend(data_quality_metrics(data_quality_report))
    _attach_artifact_source_timestamps(result, points)
    _emit_run_progress(
        progress_callback,
        phase="persisting canonical artifacts",
        progress=92,
        current_x=points[-1].x_value,
        x_axis=x_axis,
        rows_processed=len(points),
        total_rows=len(points),
    )
    _raise_if_backtest_run_cancelled(conn, run_id)
    replace_backtest_results(conn, run_id, result)
    requested_engine = str(result.get("requested_backtest_engine") or run.get("backtest_engine") or "builtin")
    actual_engine = str(result.get("actual_backtest_engine") or requested_engine)
    engine_meta = {
        "actual_data_quality": _compact_data_quality_for_run_meta(data_quality_report),
        "requested_backtest_engine": requested_engine,
        "actual_backtest_engine": actual_engine,
        "engine_routed_to_builtin": bool(result.get("engine_routed_to_builtin")),
    }
    if requested_engine != actual_engine:
        engine_meta["engine_route_note"] = f"{requested_engine} adapter routed to {actual_engine}"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_runs
            SET status = 'succeeded',
                rows_processed = %s,
                meta = meta || %s::jsonb,
                finished_at = clock_timestamp(),
                error = NULL
            WHERE run_id = %s
              AND status <> 'cancelled'
            RETURNING run_id
            """,
            (
                len(points),
                json.dumps(engine_meta),
                run_id,
            ),
        )
        if cur.fetchone() is None:
            raise BacktestRunCancelled(f"Backtest run #{run_id} cancelled by user")
    try:
        artifact_summary = _build_persisted_artifact_summary(
            run_id=run_id,
            run=run,
            params=params,
            result=result,
            rows_processed=len(points),
        )
    except Exception:
        # The read model is an acceleration layer. Persisted canonical artifacts
        # remain authoritative and must not be reclassified as a failed run.
        artifact_summary = None
    _upsert_backtest_run_progress(
        conn,
        run_id,
        status="succeeded",
        phase="backtest artifacts ready",
        progress=100,
        current_x=points[-1].x_value,
        x_axis=x_axis,
        rows_processed=len(points),
        total_rows=len(points),
        eta_seconds=0,
        artifact_summary=artifact_summary,
    )


def mark_run_failed(conn: Any, run_id: int, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_runs
            SET status = 'failed',
                error = %s,
                finished_at = clock_timestamp()
            WHERE run_id = %s
              AND status <> 'cancelled'
            """,
            (
                error[:4000],
                run_id,
            ),
        )
    _upsert_backtest_run_progress(
        conn,
        run_id,
        status="failed",
        phase="backtest failed",
        progress=0,
        error=error,
    )


def mark_run_cancelled(conn: Any, run_id: int, reason: str = "cancelled by user") -> None:
    cancel_backtest_run(conn, run_id, reason)


def _bounded_price_points(rows: list[Any], *, limit: int, run: Mapping[str, Any]) -> list[PricePoint]:
    if len(rows) > limit:
        source = str(run.get("price_source") or "unknown")
        requested_from = run.get("from_block") if source == "orderfilled_block_close" else run.get("from_ts")
        requested_to = run.get("to_block") if source == "orderfilled_block_close" else run.get("to_ts")
        raise RuntimeError(
            "backtest price window exceeds "
            f"POLYDATA_QUANT_BACKTEST_MAX_PRICE_POINTS={limit} "
            f"for {source} range {requested_from}..{requested_to}; narrow the range or raise the explicit limit"
        )
    return [_price_point(row) for row in rows]


def fetch_price_points(conn: Any, run: dict[str, Any], *, limit: int | None = None) -> list[PricePoint]:
    max_points = max(2, int(limit or _env_int("POLYDATA_QUANT_BACKTEST_MAX_PRICE_POINTS", 250_000)))
    query_limit = max_points + 1
    meta = run.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    token_id = str(meta.get("token_id") or "").strip()
    if run["price_source"] == "frontend":
        filters: list[str]
        values: list[Any]
        if token_id:
            filters = ["token_id = %s"]
            values = [token_id]
        else:
            filters = ["market_slug = %s", "token_side = %s"]
            values = [run["market_slug"], run["token_side"]]
        if run.get("from_ts") is not None:
            filters.append("timestamp >= %s")
            values.append(run["from_ts"])
        if run.get("to_ts") is not None:
            filters.append("timestamp <= %s")
            values.append(run["to_ts"])
        values.append(query_limit)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT timestamp AS x_value, price, 0::numeric AS volume, 0::bigint AS trade_count, ts_minute AS block_timestamp
                FROM quant.market_token_frontend_price_1m
                WHERE {" AND ".join(filters)}
                ORDER BY timestamp ASC
                LIMIT %s
                """,
                values,
            )
            rows = list(cur.fetchall())
        return _bounded_price_points(rows, limit=max_points, run=run)

    if token_id:
        filters = ["token_id = %s"]
        values = [token_id]
    else:
        filters = ["market_slug = %s", "token_side = %s"]
        values = [run["market_slug"], run["token_side"]]
    if run.get("from_block") is not None:
        filters.append("block_number >= %s")
        values.append(run["from_block"])
    if run.get("to_block") is not None:
        filters.append("block_number <= %s")
        values.append(run["to_block"])
    values.append(query_limit)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                block_number AS x_value,
                close_price AS price,
                open_price,
                high_price,
                low_price,
                close_price,
                vwap_price,
                volume,
                buy_volume,
                sell_volume,
                trade_count,
                first_log_index,
                last_log_index,
                block_timestamp
            FROM quant.market_token_block_close
            WHERE {" AND ".join(filters)}
            ORDER BY block_number ASC
            LIMIT %s
            """,
            values,
        )
        rows = list(cur.fetchall())
    return _bounded_price_points(rows, limit=max_points, run=run)


def price_access_report(run: dict[str, Any], points: list[PricePoint]) -> dict[str, Any]:
    meta = run.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    token_id = str(meta.get("token_id") or "").strip() or None
    source = normalize_price_source(run.get("price_source"))
    first_x = int(points[0].x_value) if points else None
    last_x = int(points[-1].x_value) if points else None
    if source == "frontend":
        access_path = "token_id+timestamp_range" if token_id else "market_slug+token_side+timestamp_range"
        index_hint = "market_token_frontend_price_1m_pkey" if token_id else "idx_quant_frontend_slug_side_time"
        source_table = "quant.market_token_frontend_price_1m"
    else:
        access_path = "token_id+block_number_range" if token_id else "market_slug+token_side+block_number_range"
        index_hint = "market_token_block_close_pkey" if token_id else "idx_quant_block_close_slug_side_block"
        source_table = "quant.market_token_block_close"
    return {
        "query_guard_version": PRICE_QUERY_GUARD_VERSION,
        "source_table": source_table,
        "access_path": access_path,
        "index_hint": index_hint,
        "token_id": token_id,
        "market_slug": run.get("market_slug"),
        "token_side": run.get("token_side"),
        "requested_from": run.get("from_block") if source == "orderfilled_block_close" else run.get("from_ts"),
        "requested_to": run.get("to_block") if source == "orderfilled_block_close" else run.get("to_ts"),
        "actual_first_x": first_x,
        "actual_last_x": last_x,
        "row_count": len(points),
        "policy": DATA_ACCESS_POLICY["backtest_price_read"],
    }


def load_clob_execution_snapshots(
    conn: Any,
    run: dict[str, Any],
    *,
    points: list[PricePoint] | None = None,
    params: BacktestParameters | None = None,
    limit: int | None = None,
) -> list[BookSnapshot]:
    meta = run.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    token_id = str(meta.get("token_id") or "").strip()
    if not token_id:
        token_id = _resolve_lob_token_id(conn, run)
    if not token_id:
        return []
    side = str(run.get("token_side") or "").strip().upper()
    values: list[Any] = [token_id]
    filters = ["token_id = %s", "book_status = 'ok'", "(level_count_bid > 0 OR level_count_ask > 0)"]
    if side in {"YES", "NO"}:
        filters.append("side = %s")
        values.append(side)
    window_predicates, window_values = _lob_snapshot_window_predicates(run, points or [], params)
    if window_predicates:
        filters.append("(" + " OR ".join(window_predicates) + ")")
        values.extend(window_values)
    row_limit = int(limit or _env_int("POLYDATA_QUANT_LOB_MAX_SNAPSHOTS", 20000))
    values.append(row_limit)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                snapshot_id, token_id, side, source, book_status,
                block_number, snapshot_timestamp, snapshot_version,
                best_bid, best_ask, spread, mid,
                bid_depth, ask_depth, depth_total,
                level_count_bid, level_count_ask, payload, fetched_at, created_at
            FROM quant.clob_orderbook_snapshots
            WHERE {" AND ".join(filters)}
            ORDER BY COALESCE(snapshot_timestamp, fetched_at) ASC, block_number ASC NULLS LAST, snapshot_id ASC
            LIMIT %s
            """,
            tuple(values),
        )
        rows = cur.fetchall()
    return [parse_book_snapshot(dict(row)) for row in rows]


def build_pmxt_compact_book_provider(
    conn: Any,
    run: dict[str, Any],
    *,
    client: Any | None = None,
) -> tuple[PmxtCompactBookProvider | None, dict[str, Any]]:
    """Select the compact PMXT source without silently claiming DEPTH coverage."""

    if str(os.environ.get("POLYDATA_QUANT_PMXT_COMPACT_ENABLED", "1")).strip().lower() in {"0", "false", "no", "off"}:
        return None, {"status": "disabled", "reason": "POLYDATA_QUANT_PMXT_COMPACT_ENABLED is disabled"}
    token = _resolve_replay_token_context(conn, run)
    condition_id = str(token.get("condition_id") or "").strip().lower()
    token_id = str(token.get("token_id") or "").strip()
    market_id = int(token.get("market_id") or run.get("market_id") or 0)
    if not condition_id or not token_id:
        return None, {
            "status": "fallback",
            "reason": "missing condition_id or token_id for compact lookup",
            "condition_id": condition_id or None,
            "token_id": token_id or None,
        }
    provider = PmxtCompactBookProvider(
        condition_id=condition_id,
        token_id=token_id,
        market_id=market_id,
        token_side=str(token.get("token_side") or run.get("token_side") or "YES"),
        orderfilled_token_id=str(token.get("token_id_hex") or token_id).strip().lower(),
        client=client or ClickHouseClient(),
        build_tag=str(os.environ.get("POLYDATA_QUANT_PMXT_COMPACT_BUILD_TAG") or PMXT_COMPACT_BUILD_TAG),
        anchor_lookback_hours=_env_int("POLYDATA_QUANT_PMXT_ANCHOR_LOOKBACK_HOURS", 24),
        max_deltas_per_order=_env_int("POLYDATA_QUANT_PMXT_MAX_DELTAS_PER_ORDER", 100_000),
    )
    try:
        available = provider.available()
    except Exception as exc:
        return None, {
            "status": "fallback",
            "reason": "compact availability query failed",
            "error": f"{type(exc).__name__}: {exc}",
            "condition_id": condition_id,
            "token_id": token_id,
        }
    if not available:
        return None, {
            "status": "fallback",
            "reason": "no compact PMXT rows for market token",
            "condition_id": condition_id,
            "token_id": token_id,
        }
    return provider, {**provider.context(), "status": "configured", "fallback_used": False}


def setup_main_pml2_execution(
    conn: Any,
    run: dict[str, Any],
    points: list[PricePoint],
    params: BacktestParameters,
) -> tuple[PmxtCompactBookProvider | None, list[BookSnapshot], dict[str, Any]]:
    """Configure the main engine's PML2 source without a silent PMXT fallback."""

    source_mode = _pml2_l2_source_for_run(run)
    token = _resolve_replay_token_context(conn, run)
    run["_pml2_token_context"] = token
    run["_pml2_ingested_snapshot_ids"] = set()

    if source_mode == PML2_LEGACY_PMXT_SOURCE:
        session = ReplayExecutionSession(
            run_id=str(run.get("run_id") or "pml2-backtest"),
            profile=params.execution_profile,
            audit_mode=params.pml2_audit_mode,
        )
        run["_pml2_session"] = session
        provider, context = build_pmxt_compact_book_provider(conn, run)
        if provider is not None:
            run["_pmxt_compact_book_provider"] = provider
            snapshots: list[BookSnapshot] = []
        else:
            snapshots = load_clob_execution_snapshots(
                conn,
                run,
                points=points,
                params=params,
            )
        return provider, snapshots, {
            **context,
            "source_mode": PML2_LEGACY_PMXT_SOURCE,
            "legacy_source_explicitly_requested": True,
            "fallback_used": provider is None,
        }

    condition_id = str(token.get("condition_id") or "").strip().lower()
    market_id = str(token.get("market_id") or run.get("market_id") or "").strip()
    yes_asset_id = str(token.get("yes_token_id") or "").strip()
    no_asset_id = str(token.get("no_token_id") or "").strip()
    session = ReplayExecutionSession(
        run_id=str(run.get("run_id") or "pml2-backtest"),
        profile=params.execution_profile,
        audit_mode=params.pml2_audit_mode,
        cold_restore_used=True,
    )
    run["_pml2_session"] = session
    if not all((condition_id, market_id, yes_asset_id, no_asset_id)):
        reason = "XUE Native PML2 requires condition_id and both YES/NO token ids"
        run["_pml2_data_not_ready_reason"] = reason
        return None, [], {
            "schema_version": "pml2_xue_native_execution_context_v1",
            "status": "data_not_ready",
            "source": "xue_native_l2_archive",
            "source_mode": PML2_XUE_NATIVE_SOURCE,
            "reason": reason,
            "condition_id": condition_id or None,
            "market_id": market_id or None,
            "yes_asset_id": yes_asset_id or None,
            "no_asset_id": no_asset_id or None,
            "fallback_used": False,
        }

    provider = XueNativeExecutionProvider(
        session=session,
        condition_id=condition_id,
        market_id=market_id,
        yes_asset_id=yes_asset_id,
        no_asset_id=no_asset_id,
    )
    run["_pml2_native_provider"] = provider
    return None, [], provider.context()


def _resolve_lob_token_id(conn: Any, run: dict[str, Any]) -> str:
    market_slug = str(run.get("market_slug") or "").strip()
    token_side = str(run.get("token_side") or "YES").strip().upper() or "YES"
    if not market_slug:
        return ""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT token_id
            FROM quant.market_token_metadata
            WHERE market_slug = %s
              AND token_side = %s
            ORDER BY outcome_index NULLS LAST, market_id ASC
            LIMIT 1
            """,
            (market_slug, token_side),
        )
        row = cur.fetchone()
    return str((row or {}).get("token_id") or "").strip()


def _lob_snapshot_window_predicates(
    run: dict[str, Any],
    points: list[PricePoint],
    params: BacktestParameters | None,
) -> tuple[list[str], list[Any]]:
    predicates: list[str] = []
    values: list[Any] = []
    timestamps = [point.timestamp for point in points if point.timestamp is not None]
    latency_seconds = Decimal(str(getattr(params, "latency_seconds", Decimal("0")) if params else Decimal("0")))
    max_staleness = Decimal(str(getattr(params, "max_book_staleness_seconds", Decimal("900")) if params else Decimal("900")))
    if timestamps:
        start_ts = min(timestamps) - timedelta(seconds=float(max(Decimal("0"), latency_seconds + max_staleness)))
        end_ts = max(timestamps) + timedelta(seconds=float(max(Decimal("0"), latency_seconds)))
        predicates.append("(COALESCE(snapshot_timestamp, fetched_at) >= %s AND COALESCE(snapshot_timestamp, fetched_at) <= %s)")
        values.extend([start_ts, end_ts])
    x_values = [int(point.x_value) for point in points]
    if run.get("price_source") == "orderfilled_block_close" and x_values:
        latency_blocks = max(0, int(getattr(params, "latency_blocks", 0) if params else 0))
        lookback = max(0, _env_int("POLYDATA_QUANT_LOB_BLOCK_LOOKBACK", 50000))
        from_block = max(0, min(x_values) - lookback)
        to_block = max(x_values) + latency_blocks
        predicates.append("(block_number IS NOT NULL AND block_number >= %s AND block_number <= %s)")
        values.extend([from_block, to_block])
    elif not points:
        if run.get("from_ts") is not None and run.get("to_ts") is not None:
            start_ts = datetime.fromtimestamp(int(run["from_ts"]), tz=timezone.utc) - timedelta(seconds=float(max_staleness))
            end_ts = datetime.fromtimestamp(int(run["to_ts"]), tz=timezone.utc) + timedelta(seconds=float(max(Decimal("0"), latency_seconds)))
            predicates.append("(COALESCE(snapshot_timestamp, fetched_at) >= %s AND COALESCE(snapshot_timestamp, fetched_at) <= %s)")
            values.extend([start_ts, end_ts])
        if run.get("from_block") is not None and run.get("to_block") is not None:
            latency_blocks = max(0, int(getattr(params, "latency_blocks", 0) if params else 0))
            lookback = max(0, _env_int("POLYDATA_QUANT_LOB_BLOCK_LOOKBACK", 50000))
            predicates.append("(block_number IS NOT NULL AND block_number >= %s AND block_number <= %s)")
            values.extend([max(0, int(run["from_block"]) - lookback), int(run["to_block"]) + latency_blocks])
    return predicates, values


def _public_clob_execution_context(
    snapshots: list[BookSnapshot],
    *,
    run: dict[str, Any] | None = None,
    params: BacktestParameters | None = None,
    points: list[PricePoint] | None = None,
) -> dict[str, Any]:
    required = _lob_execution_required(params)
    predicates, values = _lob_snapshot_window_predicates(run or {}, points or [], params)
    window = {"predicate_count": len(predicates), "value_count": len(values)}
    if not snapshots:
        return {
            "source": "clob_orderbook_snapshots",
            "required": required,
            "snapshot_count": 0,
            "snapshot_version": None,
            "load_window": window,
            "warning": "no historical CLOB snapshots loaded",
        }
    first = snapshots[0]
    last = snapshots[-1]
    source_counts: dict[str, int] = {}
    for snapshot in snapshots:
        source = str(snapshot.source or "unknown")
        source_counts[source] = source_counts.get(source, 0) + 1
    version_payload = "|".join(snapshot.snapshot_version for snapshot in snapshots if snapshot.snapshot_version)
    snapshot_version = hashlib.sha256(version_payload.encode("ascii")).hexdigest()[:20] if version_payload else None
    return {
        "source": "clob_orderbook_snapshots",
        "required": required,
        "latest_source": last.source,
        "source_counts": source_counts,
        "pmxt_l2_sampled_count": source_counts.get("pmxt_l2_sampled", 0),
        "uses_pmxt_l2_sampled": source_counts.get("pmxt_l2_sampled", 0) > 0,
        "snapshot_count": len(snapshots),
        "snapshot_version": snapshot_version,
        "load_window": window,
        "first_snapshot_id": first.snapshot_id,
        "last_snapshot_id": last.snapshot_id,
        "first_timestamp": first.timestamp.isoformat() if first.timestamp else None,
        "last_timestamp": last.timestamp.isoformat() if last.timestamp else None,
        "first_block": first.block_number,
        "last_block": last.block_number,
        "latest_best_bid": _decimal_text(last.best_bid) if last.best_bid is not None else None,
        "latest_best_ask": _decimal_text(last.best_ask) if last.best_ask is not None else None,
        "latest_spread": _decimal_text(last.spread) if last.spread is not None else None,
        "latest_ask_depth": _decimal_text(last.ask_depth),
        "latest_bid_depth": _decimal_text(last.bid_depth),
    }


def _lob_execution_required(params: BacktestParameters | None) -> bool:
    if params is None:
        return False
    mode = normalize_execution_price_mode(getattr(params, "execution_price_mode", ""), "ORDERFILLED")
    if is_orderfilled_v2_tape_mode(mode) or is_orderfilled_v3_trade_mode(mode):
        return False
    return mode in {"DEPTH", ORDERFILLED_LOB_MODE, PREDICTION_L2_REPLAY_V1_MODE}


def _build_lob_execution_coverage_context(
    orders: list[dict[str, Any]],
    snapshots: list[BookSnapshot],
    *,
    run: dict[str, Any],
    points: list[PricePoint],
    params: BacktestParameters,
) -> dict[str, Any]:
    required = _lob_execution_required(params)
    if not required:
        return {
            "schema_version": "lob_execution_coverage_v1",
            "status": "disabled",
            "required": False,
            "reason": "execution mode does not require LOB depth evidence",
            "order_count": len(orders),
            "snapshot_count": len(snapshots),
        }
    coverage_orders = _lob_coverage_orders(
        orders,
        run=run,
        points=points,
        token_id_override=snapshots[0].token_id if snapshots else "",
    )
    report = build_lob_execution_coverage_report(
        coverage_orders,
        snapshots,
        max_staleness_seconds=params.max_book_staleness_seconds,
    )
    report["required"] = True
    report["execution_price_mode"] = normalize_execution_price_mode(params.execution_price_mode)
    return report


def _lob_coverage_orders(
    orders: list[dict[str, Any]],
    *,
    run: dict[str, Any],
    points: list[PricePoint],
    token_id_override: str = "",
) -> list[dict[str, Any]]:
    meta = run.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    token_id = str(meta.get("token_id") or token_id_override or "").strip()
    timestamp_by_x = {int(point.x_value): point.timestamp for point in points if point.timestamp is not None}
    rows: list[dict[str, Any]] = []
    for order in orders:
        submit_x = int(order.get("submit_x") or order.get("signal_x") or 0)
        row = {
            "order_id": order.get("order_id"),
            "token_id": token_id,
            "submit_block": submit_x if order.get("x_axis") == "block_number" else None,
            "submit_time": timestamp_by_x.get(submit_x) or _nearest_point_timestamp(points, submit_x),
        }
        if row["submit_time"] is None and order.get("x_axis") != "block_number" and submit_x > 0:
            row["submit_time"] = datetime.fromtimestamp(submit_x, tz=timezone.utc)
        rows.append(row)
    return rows


def _nearest_point_timestamp(points: list[PricePoint], x_value: int) -> datetime | None:
    with_timestamps = [point for point in points if point.timestamp is not None]
    if not with_timestamps:
        return None
    after = [point for point in with_timestamps if int(point.x_value) >= int(x_value)]
    if after:
        after.sort(key=lambda point: int(point.x_value))
        return after[0].timestamp
    return with_timestamps[-1].timestamp


def _attach_artifact_source_timestamps(result: dict[str, Any], points: list[PricePoint]) -> None:
    """Persist the source clock used by the chart on replayable artifacts."""

    timestamp_by_x = {
        int(point.x_value): point.timestamp
        for point in points
        if point.timestamp is not None
    }
    sorted_x = sorted(timestamp_by_x)
    if not sorted_x:
        return

    def timestamp_for(value: Any) -> datetime | None:
        x_value = _int_or_none(value)
        if x_value is None:
            return None
        exact = timestamp_by_x.get(x_value)
        if exact is not None:
            return exact
        index = bisect_left(sorted_x, x_value)
        if index >= len(sorted_x):
            index = len(sorted_x) - 1
        return timestamp_by_x[sorted_x[index]]

    def attach(row: dict[str, Any], x_key: str, meta_key: str) -> None:
        timestamp = timestamp_for(row.get(x_key))
        if timestamp is None:
            return
        current_meta = row.get("meta")
        meta = dict(current_meta) if isinstance(current_meta, Mapping) else {}
        meta.setdefault(meta_key, timestamp.isoformat())
        row["meta"] = meta

    for row in result.get("events", []):
        if isinstance(row, dict):
            attach(row, "x_value", "source_timestamp")
    for row in result.get("ledger", []):
        if isinstance(row, dict):
            attach(row, "x_value", "source_timestamp")
    for row in result.get("orders", []):
        if not isinstance(row, dict):
            continue
        attach(row, "signal_x", "signal_timestamp")
        attach(row, "submit_x", "submit_timestamp")


def load_orderfilled_limit_replay_events(
    conn: Any,
    run: dict[str, Any],
    points: list[PricePoint],
    params: BacktestParameters,
) -> tuple[list[ReplayTradeEvent], dict[str, Any]]:
    context: dict[str, Any] = {
        "source": "ClickHouse orderfilled_fact",
        "enabled": False,
        "event_count": 0,
        "fallback": "not_limit_replay",
    }
    if not _is_limit_replay_mode(params):
        return [], context
    if normalize_price_source(run.get("price_source")) != "orderfilled_block_close":
        context["fallback"] = "price_source_not_orderfilled_block_close"
        return [], context
    if not points:
        context["fallback"] = "no_price_points"
        return [], context
    token_context = _resolve_replay_token_context(conn, run)
    context.update(token_context)
    market_id = token_context.get("market_id")
    token_id_hex = str(token_context.get("token_id_hex") or "").strip().lower()
    if not market_id or not token_id_hex:
        context["fallback"] = "missing_market_id_or_token_id_hex"
        return [], context
    from_block = int(points[0].x_value)
    to_block = int(points[-1].x_value)
    limit = _env_int("POLYDATA_QUANT_LIMIT_REPLAY_MAX_EVENTS", 250_000)
    context.update(
        {
            "enabled": True,
            "from_block": from_block,
            "to_block": to_block,
            "limit": limit,
            "source_table": "orderfilled_fact",
            "access_path": "market_id_token_id_block_number_range",
            "order_by": ["block_number", "transaction_index", "log_index", "tx_hash"],
            "loaded_block_window": {
                "from_block": from_block,
                "to_block": to_block,
                "market_id": int(market_id),
                "token_id_hex": token_id_hex,
                "limit": limit,
            },
        }
    )
    try:
        from .runners.data_sources import ClickHouseOrderFilledStore

        store = ClickHouseOrderFilledStore()
        events = store.load_trade_events(
            market_id=int(market_id),
            token_id=token_id_hex,
            from_block=from_block,
            to_block=to_block,
            limit=limit,
        )
        context.update(
            {
                "source": getattr(store, "last_replay_source", "ClickHouse orderfilled_fact"),
                "source_table": getattr(store, "last_replay_source_table", "orderfilled_fact"),
                "access_path": getattr(store, "last_replay_access_path", "market_id_token_id_block_number_range"),
                "cache_fallback": getattr(store, "last_replay_cache_fallback", None),
            }
        )
        cache_warning = getattr(store, "last_replay_cache_warning", None)
        if cache_warning:
            context["cache_warning"] = cache_warning
    except Exception as exc:
        context.update({"enabled": False, "fallback": "clickhouse_load_failed", "warning": str(exc)[:500]})
        return [], context
    replay_events, context = _orderfilled_replay_context_with_event_stats(events, context, limit=limit)
    context["fallback"] = "synthetic_block_close_events" if not replay_events else None
    if context["loaded_event_count"] >= limit:
        context["warning"] = "raw replay event load hit limit; widen env POLYDATA_QUANT_LIMIT_REPLAY_MAX_EVENTS for fuller replay"
    return replay_events, context


def _orderfilled_replay_context_with_event_stats(
    events: list[ReplayTradeEvent],
    context: dict[str, Any] | None = None,
    *,
    limit: int,
) -> tuple[list[ReplayTradeEvent], dict[str, Any]]:
    loaded_events = sorted(list(events or []), key=lambda event: event.event_sequence)
    replay_events = dedupe_replay_events(loaded_events)
    duplicate_stats = _replay_event_duplicate_stats(loaded_events)
    key_kind_counts: dict[str, int] = {}
    for event in loaded_events:
        key_kind = event.canonical_fill_key_kind or "unknown"
        key_kind_counts[key_kind] = key_kind_counts.get(key_kind, 0) + 1
    raw_tick_events = build_backtest_event_stream(
        raw_orderfilled_events=[replay_trade_event_dict(event) for event in replay_events],
    )
    raw_trade_tick_report = build_raw_trade_tick_report(raw_tick_events)
    loaded_first = loaded_events[0].block_number if loaded_events else None
    loaded_last = loaded_events[-1].block_number if loaded_events else None
    replay_first = replay_events[0].block_number if replay_events else None
    replay_last = replay_events[-1].block_number if replay_events else None
    duplicate_count = max(0, len(loaded_events) - len(replay_events))
    updated = dict(context or {})
    updated.update(
        {
            "event_count": len(replay_events),
            "loaded_event_count": len(loaded_events),
            "deduped_event_count": len(replay_events),
            "duplicate_event_count": duplicate_count,
            "exact_duplicate_event_count": duplicate_stats["exact_duplicate_event_count"],
            "conflicting_duplicate_event_count": duplicate_stats["conflicting_duplicate_event_count"],
            "duplicate_group_count": duplicate_stats["duplicate_group_count"],
            "conflicting_duplicate_group_count": duplicate_stats["conflicting_duplicate_group_count"],
            "dedupe_applied": True,
            "canonical_key_kind_counts": key_kind_counts,
            "canonical_event_count": key_kind_counts.get("canonical", 0),
            "fallback_key_event_count": key_kind_counts.get("fallback", 0),
            "unknown_key_event_count": key_kind_counts.get("unknown", 0),
            "loaded_first_block": loaded_first,
            "loaded_last_block": loaded_last,
            "replay_first_block": replay_first,
            "replay_last_block": replay_last,
            "hit_limit": len(loaded_events) >= int(limit),
            "raw_trade_tick_report": raw_trade_tick_report,
            "raw_trade_tick_count": raw_trade_tick_report.get("trade_tick_count", len(replay_events)),
            "raw_block_count": raw_trade_tick_report.get("block_count", 0),
            "raw_maker_taker_side_coverage_pct": raw_trade_tick_report.get("maker_taker_side_coverage_pct", "0"),
            "raw_canonical_fill_key_coverage_pct": raw_trade_tick_report.get("canonical_fill_key_coverage_pct", "0"),
        }
    )
    loaded_window = dict(updated.get("loaded_block_window") or {})
    loaded_window.update(
        {
            "loaded_first_block": loaded_first,
            "loaded_last_block": loaded_last,
            "replay_first_block": replay_first,
            "replay_last_block": replay_last,
            "loaded_event_count": len(loaded_events),
            "replay_event_count": len(replay_events),
            "duplicate_event_count": duplicate_count,
            "exact_duplicate_event_count": duplicate_stats["exact_duplicate_event_count"],
            "conflicting_duplicate_event_count": duplicate_stats["conflicting_duplicate_event_count"],
            "duplicate_group_count": duplicate_stats["duplicate_group_count"],
            "conflicting_duplicate_group_count": duplicate_stats["conflicting_duplicate_group_count"],
            "hit_limit": len(loaded_events) >= int(limit),
        }
    )
    updated["loaded_block_window"] = loaded_window
    return replay_events, updated


def _replay_event_duplicate_stats(events: list[ReplayTradeEvent]) -> dict[str, int]:
    groups: dict[str, dict[tuple[Any, ...], int]] = {}
    for event in events:
        key, key_kind = event.canonical_fill_key_parts
        if key_kind != "canonical" or not key:
            continue
        fingerprint = (
            int(event.market_id),
            str(event.token_id or "").lower(),
            str(event.condition_id or "").lower(),
            int(event.block_number),
            int(event.transaction_index or 0),
            int(event.log_index),
            str(event.tx_hash or "").lower(),
            Decimal(str(event.trade_price)),
            Decimal(str(event.size)),
            str(event.maker or "").lower(),
            str(event.taker or "").lower(),
            str(event.side_code or "").upper(),
        )
        fingerprints = groups.setdefault(key, {})
        fingerprints[fingerprint] = fingerprints.get(fingerprint, 0) + 1

    exact_duplicate_count = 0
    conflicting_duplicate_count = 0
    duplicate_group_count = 0
    conflicting_duplicate_group_count = 0
    for fingerprints in groups.values():
        row_count = sum(fingerprints.values())
        if row_count <= 1:
            continue
        duplicate_group_count += 1
        exact_duplicate_count += sum(max(0, count - 1) for count in fingerprints.values())
        if len(fingerprints) > 1:
            conflicting_duplicate_group_count += 1
            conflicting_duplicate_count += len(fingerprints) - 1
    return {
        "exact_duplicate_event_count": exact_duplicate_count,
        "conflicting_duplicate_event_count": conflicting_duplicate_count,
        "duplicate_group_count": duplicate_group_count,
        "conflicting_duplicate_group_count": conflicting_duplicate_group_count,
    }


def _resolve_replay_token_context(conn: Any, run: dict[str, Any]) -> dict[str, Any]:
    meta = run.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    token_id = str(meta.get("token_id") or "").strip()
    market_slug = str(run.get("market_slug") or "").strip()
    token_side = str(run.get("token_side") or "YES").strip().upper() or "YES"
    filters: list[str] = []
    values: list[Any] = []
    if token_id:
        filters.append("selected.token_id = %s")
        values.append(token_id)
    else:
        filters.extend(["selected.market_slug = %s", "selected.token_side = %s"])
        values.extend([market_slug, token_side])
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT selected.market_id, selected.condition_id,
                   selected.token_id, selected.token_id_hex,
                   selected.market_slug, selected.market_title,
                   selected.end_date, selected.token_side,
                   (
                       SELECT metadata.event_category
                       FROM quant.market_event_members member
                       JOIN quant.market_event_metadata metadata
                         ON metadata.event_slug = member.event_slug
                       WHERE member.market_id = selected.market_id
                       ORDER BY metadata.updated_at DESC
                       LIMIT 1
                   ) AS category,
                   (
                       SELECT sibling.token_id
                       FROM quant.market_token_metadata sibling
                       WHERE sibling.condition_id = selected.condition_id
                         AND sibling.token_side = 'YES'
                       ORDER BY sibling.outcome_index NULLS LAST, sibling.market_id ASC
                       LIMIT 1
                   ) AS yes_token_id,
                   (
                       SELECT sibling.token_id
                       FROM quant.market_token_metadata sibling
                       WHERE sibling.condition_id = selected.condition_id
                         AND sibling.token_side = 'NO'
                       ORDER BY sibling.outcome_index NULLS LAST, sibling.market_id ASC
                       LIMIT 1
                   ) AS no_token_id
            FROM quant.market_token_metadata selected
            WHERE {" AND ".join(filters)}
            ORDER BY selected.outcome_index NULLS LAST, selected.market_id ASC
            LIMIT 1
            """,
            tuple(values),
        )
        row = cur.fetchone()
    if not row:
        return {"token_id": token_id or None, "market_slug": market_slug, "token_side": token_side}
    selected_side = str(row.get("token_side") or token_side).upper()
    selected_token_id = row.get("token_id")
    return {
        "market_id": int(row["market_id"]) if row.get("market_id") is not None else None,
        "condition_id": str(row.get("condition_id") or "").lower() or None,
        "token_id": selected_token_id,
        "token_id_hex": str(row.get("token_id_hex") or "").lower() or None,
        "market_slug": row.get("market_slug"),
        "market_title": row.get("market_title"),
        "category": row.get("category"),
        "market_end_ts": row.get("end_date"),
        "token_side": row.get("token_side"),
        "yes_token_id": row.get("yes_token_id") or (
            selected_token_id if selected_side == "YES" else None
        ),
        "no_token_id": row.get("no_token_id") or (
            selected_token_id if selected_side == "NO" else None
        ),
    }


def _pml2_l2_source_for_run(run: Mapping[str, Any]) -> str:
    meta = run.get("meta") or {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    configured = (
        meta.get("pml2_l2_source")
        if isinstance(meta, Mapping)
        else None
    )
    return normalize_pml2_l2_source(
        configured or os.environ.get("POLYDATA_QUANT_PML2_L2_SOURCE")
    )


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(str(os.environ.get(name, default)).strip()))
    except Exception:
        return default


def _report_simulation_progress(points: list[PricePoint], run: dict[str, Any], index: int) -> None:
    callback = run.get("_progress_callback")
    if not callable(callback) or not points:
        return
    total = len(points)
    stride = max(1, total // 24)
    processed = index + 1
    if index != 0 and processed != total and processed % stride:
        return
    point = points[index]
    _emit_run_progress(
        callback,
        phase="simulating strategy and fills",
        progress=38 + round((processed / total) * 40),
        current_x=point.x_value,
        x_axis="timestamp" if run.get("price_source") == "frontend" else "block_number",
        rows_processed=processed,
        total_rows=total,
    )


def simulate_strategy(points: list[PricePoint], run: dict[str, Any], params: BacktestParameters) -> dict[str, Any]:
    if is_orderfilled_v2_tape_mode(params.execution_price_mode):
        return simulate_orderfilled_v2_tape_strategy(points, run, params)
    if is_orderfilled_v3_trade_mode(params.execution_price_mode):
        return simulate_orderfilled_v3_trade_strategy(points, run, params)
    if (
        is_orderfilled_lob_mode(params.execution_price_mode)
        and str(params.order_role or "").lower() == "maker"
        and run.get("_pmxt_compact_book_provider") is not None
    ):
        return simulate_orderfilled_lob_maker_strategy(points, run, params)
    if _is_limit_replay_mode(params):
        return simulate_limit_replay_strategy(points, run, params)

    x_axis = "timestamp" if run["price_source"] == "frontend" else "block_number"
    equity = params.initial_capital
    peak = params.initial_capital
    open_position: OpenPosition | None = None
    trades: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    order_index = 0
    profile = effective_execution_profile(params)

    for index, point in enumerate(points):
        _report_simulation_progress(points, run, index)
        entry_signal = _entry_signal_context(point, params)
        if open_position is None and entry_signal["triggered"]:
            order_index += 1
            order_id = next_order_id(order_index)
            fill = _fill_decision(params, point, run, "BUY_YES")
            fill = _with_signal_context(fill, entry_signal)
            trade_id = f"T-{len(trades) + 1:04d}"
            orders.append(order_from_fill(
                order_id=order_id,
                signal_index=index + 1,
                x_axis=x_axis,
                x_value=point.x_value,
                side="BUY_YES",
                role=profile.order_role,
                order_type="market_like_limit",
                decision_price=Decimal(str(entry_signal["decision_price"])),
                fill=fill,
                trade_id=trade_id if fill["size"] > 0 else None,
                latency_seconds=params.latency_seconds,
                latency_blocks=profile.latency_blocks,
            ))
            if fill["size"] <= 0:
                events.append(_event(
                    "fill_rejected",
                    x_axis,
                    point.x_value,
                    None,
                    Decimal(str(entry_signal["decision_price"])),
                    "entry signal rejected by minimum fill or liquidity constraints",
                    meta=fill,
                ))
            else:
                open_position = OpenPosition(
                    trade_index=len(trades) + 1,
                    entry_index=index,
                    entry_x=point.x_value,
                    entry_price=fill.get("entry_price") or _execution_price(point.price, params, "entry"),
                    size=fill["size"],
                    requested_notional=fill["requested_notional"],
                    filled_notional=fill["filled_notional"],
                    fill_pct=fill["fill_pct"],
                    fill_status=fill.get("fill_status", "FILLED"),
                    book_snapshot_id=fill.get("book_snapshot_id"),
                    snapshot_version=fill.get("snapshot_version"),
                    staleness_seconds=fill.get("staleness_seconds"),
                    staleness_blocks=fill.get("staleness_blocks"),
                    avg_fill_price=fill.get("avg_fill_price"),
                    fill_probability=fill.get("fill_probability", Decimal("0")),
                    block_volume=fill.get("block_volume", point.volume),
                    trade_count=int(fill.get("trade_count", point.trade_count) or 0),
                    available_notional=fill.get("available_notional", Decimal("0")),
                    entry_order_id=order_id,
                    entry_fee_cost=Decimal(str(fill.get("fee_cost") or 0)),
                    entry_rebate=Decimal(str(fill.get("rebate") or fill.get("rebate_cost") or 0)),
                    entry_slippage_cost=Decimal(str(fill.get("slippage_cost") or 0)),
                )
                events.append(_event(
                    "open",
                    x_axis,
                    point.x_value,
                    f"T-{open_position.trade_index:04d}",
                    Decimal(str(entry_signal["decision_price"])),
                    "entry threshold reached",
                    meta=fill,
                ))
        elif open_position is not None:
            exit_signal = _exit_signal_context(point, open_position.entry_price, index - open_position.entry_index, params)
            if exit_signal["reason"]:
                order_index += 1
                order_id = next_order_id(order_index)
                exit_fill = _fill_decision(params, point, run, "SELL_YES", target_size=open_position.size)
                exit_fill = _with_signal_context(exit_fill, exit_signal)
                trade_id = f"T-{open_position.trade_index:04d}"
                orders.append(order_from_fill(
                    order_id=order_id,
                    signal_index=index + 1,
                    x_axis=x_axis,
                    x_value=point.x_value,
                    side="SELL_YES",
                    role=profile.order_role,
                    order_type="market_like_limit",
                    decision_price=Decimal(str(exit_signal["decision_price"])),
                    fill=exit_fill,
                    trade_id=trade_id if exit_fill["size"] > 0 else trade_id,
                    latency_seconds=params.latency_seconds,
                    latency_blocks=profile.latency_blocks,
                ))
                if exit_fill["size"] <= 0:
                    events.append(_event(
                        "exit_rejected",
                        x_axis,
                        point.x_value,
                        f"T-{open_position.trade_index:04d}",
                        Decimal(str(exit_signal["decision_price"])),
                        str(exit_signal["reason"]),
                        meta=exit_fill,
                    ))
                    continue
                trade = _close_trade(
                    run,
                    x_axis,
                    open_position,
                    point,
                    index,
                    str(exit_signal["reason"]),
                    params,
                    exit_fill=exit_fill,
                    exit_order_id=order_id,
                )
                trades.append(trade)
                equity += trade["pnl"]
                events.append(
                    _event(
                        "close",
                        x_axis,
                        point.x_value,
                        trade["trade_id"],
                        Decimal(str(exit_signal["decision_price"])),
                        str(exit_signal["reason"]),
                    )
                )
                remaining_size = (open_position.size - exit_fill["size"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
                open_position.size = remaining_size
                if remaining_size <= 0:
                    open_position = None
                else:
                    open_position.trade_index = len(trades) + 1

        mark_equity = equity
        if open_position is not None:
            mark_equity += (_execution_price(point.price, params, "exit") - open_position.entry_price) * open_position.size
        peak = max(peak, mark_equity)
        drawdown = mark_equity - peak
        equity_rows.append(
            {
                "point_index": index + 1,
                "x_axis": x_axis,
                "x_value": point.x_value,
                "equity": mark_equity,
                "drawdown": drawdown,
                "drawdown_pct": _pct(drawdown, peak),
                "cumulative_return": _pct(mark_equity - params.initial_capital, params.initial_capital),
            }
        )

    if open_position is not None:
        last = points[-1]
        order_index += 1
        order_id = next_order_id(order_index)
        if params.final_valuation_mode == "FORCE_CLOSE":
            exit_fill = _force_close_fill(params, last, open_position.size)
        else:
            exit_fill = _fill_decision(params, last, run, "SELL_YES", target_size=open_position.size)
        trade_id = f"T-{open_position.trade_index:04d}"
        orders.append(order_from_fill(
            order_id=order_id,
            signal_index=len(points),
            x_axis=x_axis,
            x_value=last.x_value,
            side="SELL_YES",
            role=profile.order_role,
            order_type="force_close_limit",
            decision_price=last.price,
            fill=exit_fill,
            trade_id=trade_id if exit_fill["size"] > 0 else trade_id,
            latency_seconds=params.latency_seconds,
            latency_blocks=profile.latency_blocks,
        ))
        if exit_fill["size"] > 0:
            trade = _close_trade(run, x_axis, open_position, last, len(points) - 1, "end_of_data", params, exit_fill=exit_fill, exit_order_id=order_id)
            trades.append(trade)
            equity += trade["pnl"]
            events.append(_event("close", x_axis, last.x_value, trade["trade_id"], last.price, "end_of_data"))
        else:
            events.append(_event("force_close_rejected", x_axis, last.x_value, f"T-{open_position.trade_index:04d}", last.price, "end_of_data", meta=exit_fill))

    ledger_rows = build_ledger_rows(
        trades,
        params.initial_capital,
        gas_cost_per_order=params.gas_cost_per_order,
        settlement_cost=params.settlement_cost,
        redeem_cost=params.redeem_cost,
        capital_cost_bps=params.capital_cost_bps,
        cashflow_events=_run_cashflow_events(run),
    )
    metrics = build_metrics(trades, equity_rows, points, params, orders=orders, ledger_rows=ledger_rows)
    return {"trades": trades, "equity": equity_rows, "metrics": metrics, "events": events, "orders": orders, "ledger": ledger_rows}


def simulate_orderfilled_lob_maker_strategy(
    points: list[PricePoint],
    run: dict[str, Any],
    params: BacktestParameters,
) -> dict[str, Any]:
    """Run signal-driven resting orders on the PMXT + OrderFilled timeline."""

    if not points or any(point.timestamp is None for point in points):
        raise RuntimeError("ORDERFILLED_LOB maker replay requires timestamped price points")
    provider = run.get("_pmxt_compact_book_provider")
    if provider is None:
        raise RuntimeError("ORDERFILLED_LOB maker replay requires PMXT compact provider")

    x_axis = "timestamp" if run["price_source"] == "frontend" else "block_number"
    equity = params.initial_capital
    peak = params.initial_capital
    open_position: OpenPosition | None = None
    pending_entry: dict[str, Any] | None = None
    pending_exit: dict[str, Any] | None = None
    trades: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    order_index = 0

    for index, point in enumerate(points):
        _report_simulation_progress(points, run, index)
        if pending_entry is not None:
            while (
                pending_entry["next_fill_index"] < len(pending_entry["timeline_fills"])
                and pending_entry["timeline_fills"][pending_entry["next_fill_index"]]["activation_index"] <= index
            ):
                slice_row = pending_entry["timeline_fills"][pending_entry["next_fill_index"]]
                open_position = _apply_lob_maker_entry_slice(
                    open_position,
                    pending=pending_entry,
                    slice_row=slice_row,
                    point=point,
                    point_index=index,
                    params=params,
                    trades=trades,
                )
                event_type = "open" if pending_entry["next_fill_index"] == 0 else "buy_partial_fill"
                events.append(_event(event_type, x_axis, point.x_value, pending_entry["trade_id"], point.price, "maker entry FillTick applied to position", meta=slice_row["fill"]))
                pending_entry["next_fill_index"] += 1
            if index >= pending_entry["terminal_index"]:
                orders.append(_lob_maker_order_row(pending_entry, x_axis=x_axis, side="BUY_YES", params=params))
                if pending_entry["fill"]["size"] <= 0:
                    events.append(_event("buy_no_fill", x_axis, point.x_value, None, point.price, pending_entry["fill"].get("no_fill_reason") or "maker order not reached", meta=pending_entry["fill"]))
                pending_entry = None

        if pending_exit is not None and open_position is not None:
            while (
                pending_exit["next_fill_index"] < len(pending_exit["timeline_fills"])
                and pending_exit["timeline_fills"][pending_exit["next_fill_index"]]["activation_index"] <= index
                and open_position is not None
            ):
                slice_row = pending_exit["timeline_fills"][pending_exit["next_fill_index"]]
                open_position, trade = _apply_lob_maker_exit_slice(
                    open_position,
                    pending=pending_exit,
                    slice_row=slice_row,
                    point=point,
                    point_index=index,
                    params=params,
                    run=run,
                    x_axis=x_axis,
                )
                trades.append(trade)
                equity += trade["pnl"]
                events.append(_event("close" if open_position is None else "sell_partial_fill", x_axis, point.x_value, trade["trade_id"], point.price, "maker exit FillTick applied to position", meta=slice_row["fill"]))
                pending_exit["next_fill_index"] += 1
            if index >= pending_exit["terminal_index"]:
                orders.append(_lob_maker_order_row(pending_exit, x_axis=x_axis, side="SELL_YES", params=params))
                if pending_exit["fill"]["size"] <= 0:
                    events.append(_event("sell_no_fill", x_axis, point.x_value, pending_exit["trade_id"], point.price, pending_exit["fill"].get("no_fill_reason") or "maker order not reached", meta=pending_exit["fill"]))
                pending_exit = None

        if index < len(points) - 1 and open_position is None and pending_entry is None:
            entry_signal = _entry_signal_context(point, params)
            if entry_signal["triggered"]:
                order_index += 1
                order_id = next_order_id(order_index)
                limit_price = _buy_limit_price(params)
                size = (_target_notional(params) / max(limit_price, Decimal("0.0000000001"))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
                pending_entry = _schedule_lob_maker_order(
                    provider=provider,
                    points=points,
                    point_index=index,
                    order_id=order_id,
                    side="BUY_YES",
                    limit_price=limit_price,
                    size=size,
                    params=params,
                    entry=True,
                    trade_id=f"T-{len(trades) + 1:04d}",
                )
        elif index < len(points) - 1 and open_position is not None and pending_entry is None and pending_exit is None:
            exit_signal = _exit_signal_context(point, open_position.entry_price, index - open_position.entry_index, params)
            if exit_signal["reason"]:
                order_index += 1
                pending_exit = _schedule_lob_maker_order(
                    provider=provider,
                    points=points,
                    point_index=index,
                    order_id=next_order_id(order_index),
                    side="SELL_YES",
                    limit_price=_sell_limit_price(params, open_position.entry_price),
                    size=open_position.size,
                    params=params,
                    entry=False,
                    trade_id=f"T-{open_position.trade_index:04d}",
                )

        mark_equity = equity
        if open_position is not None:
            mark_equity += (point.price - open_position.entry_price) * open_position.size
        peak = max(peak, mark_equity)
        drawdown = mark_equity - peak
        equity_rows.append({
            "point_index": index + 1,
            "x_axis": x_axis,
            "x_value": point.x_value,
            "equity": mark_equity,
            "drawdown": drawdown,
            "drawdown_pct": _pct(drawdown, peak),
            "cumulative_return": _pct(mark_equity - params.initial_capital, params.initial_capital),
        })

    for pending, side in ((pending_entry, "BUY_YES"), (pending_exit, "SELL_YES")):
        if pending is None or any(order.get("order_id") == pending["order_id"] for order in orders):
            continue
        orders.append(order_from_fill(
            order_id=pending["order_id"],
            signal_index=pending["signal_index"],
            x_axis=x_axis,
            x_value=pending["signal_x"],
            side=side,
            role="maker",
            order_type="pmxt_l2_resting_limit",
            decision_price=pending["limit_price"],
            fill=pending["fill"],
            trade_id=pending["trade_id"] if pending["fill"]["size"] > 0 else None,
            latency_seconds=params.latency_seconds,
            latency_blocks=params.latency_blocks,
        ))

    if open_position is not None:
        last = points[-1]
        if params.final_valuation_mode == "FORCE_CLOSE":
            order_index += 1
            order_id = next_order_id(order_index)
            close_fill = _force_close_fill(params, last, open_position.size)
            trade_id = f"T-{open_position.trade_index:04d}"
            orders.append(order_from_fill(
                order_id=order_id,
                signal_index=len(points),
                x_axis=x_axis,
                x_value=last.x_value,
                side="SELL_YES",
                role="taker",
                order_type="force_close_limit",
                decision_price=last.price,
                fill=close_fill,
                trade_id=trade_id,
                latency_seconds=Decimal("0"),
                latency_blocks=0,
            ))
            trade = _close_trade(run, x_axis, open_position, last, len(points) - 1, "end_of_data", params, exit_fill=close_fill, exit_order_id=order_id)
            trades.append(trade)
            equity += trade["pnl"]
            events.append(_event("close", x_axis, last.x_value, trade_id, last.price, "maker-filled position force-closed at end of data", meta=close_fill))
            open_position = None
        else:
            settlement_value = _settlement_value(params, points)
            if settlement_value is not None:
                order_index += 1
                order_id = next_order_id(order_index)
                settlement_fill = _settlement_fill(params, last, open_position.size, settlement_value)
                trade_id = f"T-{open_position.trade_index:04d}"
                orders.append(order_from_fill(
                    order_id=order_id,
                    signal_index=len(points),
                    x_axis=x_axis,
                    x_value=last.x_value,
                    side="SELL_YES",
                    role="settlement",
                    order_type="settlement",
                    decision_price=settlement_value,
                    fill=settlement_fill,
                    trade_id=trade_id,
                    latency_seconds=Decimal("0"),
                    latency_blocks=0,
                ))
                trade = _close_trade(run, x_axis, open_position, last, len(points) - 1, "settlement", params, exit_fill=settlement_fill, exit_order_id=order_id)
                trades.append(trade)
                equity += trade["pnl"]
                events.append(_event("settlement", x_axis, last.x_value, trade_id, settlement_value, "maker-filled position held to settlement", meta=settlement_fill))
                open_position = None
            else:
                events.append(_event("unresolved_open", x_axis, last.x_value, f"T-{open_position.trade_index:04d}", last.price, "maker-filled position has no settlement_value", meta={"position_size": open_position.size}))

    ledger_rows = build_ledger_rows(
        trades,
        params.initial_capital,
        gas_cost_per_order=params.gas_cost_per_order,
        settlement_cost=params.settlement_cost,
        redeem_cost=params.redeem_cost,
        capital_cost_bps=params.capital_cost_bps,
        cashflow_events=_run_cashflow_events(run),
    )
    metrics = build_metrics(trades, equity_rows, points, params, orders=orders, ledger_rows=ledger_rows)
    return {"trades": trades, "equity": equity_rows, "metrics": metrics, "events": events, "orders": orders, "ledger": ledger_rows}


def _schedule_lob_maker_order(
    *,
    provider: PmxtCompactBookProvider,
    points: list[PricePoint],
    point_index: int,
    order_id: str,
    side: str,
    limit_price: Decimal,
    size: Decimal,
    params: BacktestParameters,
    entry: bool,
    trade_id: str,
) -> dict[str, Any]:
    signal_point = points[point_index]
    signal_ts = signal_point.timestamp
    if signal_ts is None:
        raise RuntimeError("maker order signal is missing timestamp")
    cancel_index = len(points) - 1
    cancel_ts = None
    if params.cancel_after_blocks > 0:
        cancel_index = min(len(points) - 1, point_index + params.cancel_after_blocks)
        cancel_ts = points[cancel_index].timestamp
    end_ts = points[cancel_index].timestamp or signal_ts
    result = provider.execute_maker_timeline(
        client_order_id=order_id,
        signal_ts=signal_ts,
        end_ts=end_ts,
        cancel_ts=cancel_ts,
        side=side,
        limit_price=limit_price,
        size=size,
        config=l2_config_from_params(params),
    )
    fill = _lob_maker_result_to_fill(result, params=params, side=side, requested_size=size, limit_price=limit_price, entry=entry)
    result_fills = tuple(result.fills) if result is not None else ()
    timeline_fills = [
        {
            "activation_index": _timestamp_activation_index(points, point_index, item.ts, signal_ts),
            "fill": _lob_maker_execution_fill_slice(item, params=params, entry=entry),
        }
        for item in result_fills
    ]
    result_state = str(result.state if result is not None else "REJECTED").upper()
    terminal_ts = (
        max((item.ts for item in result_fills), default=end_ts)
        if result_state == "FILLED"
        else end_ts
    )
    terminal_index = _timestamp_activation_index(points, point_index, terminal_ts, signal_ts)
    return {
        "order_id": order_id,
        "trade_id": trade_id,
        "signal_index": point_index + 1,
        "signal_x": signal_point.x_value,
        "limit_price": limit_price,
        "terminal_index": terminal_index,
        "timeline_fills": timeline_fills,
        "next_fill_index": 0,
        "fill": fill,
    }


def _timestamp_activation_index(
    points: list[PricePoint],
    start_index: int,
    fill_ts: datetime,
    fallback_ts: datetime,
) -> int:
    return next(
        (
            idx
            for idx in range(start_index, len(points))
            if (points[idx].timestamp or fallback_ts) >= fill_ts
        ),
        len(points) - 1,
    )


def _lob_maker_execution_fill_slice(item: Any, *, params: BacktestParameters, entry: bool) -> dict[str, Any]:
    size = Decimal(str(item.size)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    price = Decimal(str(item.price)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    notional = (size * price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    fee, rebate = _fee_rebate_for_notional(params, notional, "maker")
    price_key = "entry_price" if entry else "exit_price"
    return {
        "requested_notional": notional,
        "filled_notional": notional,
        "expected_fill_notional": notional,
        "actual_fill_notional": notional,
        "fill_pct": Decimal("100"),
        "fill_probability": Decimal("100"),
        "size": size,
        price_key: price,
        "avg_fill_price": price,
        "partial_fill": False,
        "rejected": False,
        "fill_status": "FILLED",
        "requested_size": size,
        "expected_fill_size": size,
        "actual_fill_size": size,
        "filled_size": size,
        "unfilled_size": Decimal("0"),
        "available_notional": notional,
        "block_volume": size,
        "trade_count": 1,
        "fee_cost": fee,
        "rebate": rebate,
        "rebate_cost": rebate,
        "slippage_cost": Decimal("0"),
        "execution_cost": fee - rebate,
        "execution_source": "pmxt_l2_orderfilled_maker_timeline",
        "execution_profile": params.execution_profile,
        "order_role": "maker",
        "execution_model_evidence": {
            "fill_timestamp": item.ts.isoformat(),
            "source_event_ids": list(item.source_event_ids),
            "reason": item.reason,
            "queue_ahead_before": item.queue_ahead_before,
            "queue_ahead_after": item.queue_ahead_after,
            "liquidity_flag": item.liquidity_flag,
        },
        "notes": [],
    }


def _apply_lob_maker_entry_slice(
    position: OpenPosition | None,
    *,
    pending: dict[str, Any],
    slice_row: dict[str, Any],
    point: PricePoint,
    point_index: int,
    params: BacktestParameters,
    trades: list[dict[str, Any]],
) -> OpenPosition:
    fill = slice_row["fill"]
    size = Decimal(str(fill["size"]))
    notional = Decimal(str(fill["filled_notional"]))
    price = Decimal(str(fill["entry_price"]))
    requested_size = Decimal(str(pending["fill"].get("requested_size") or size))
    requested_notional = Decimal(str(pending["fill"].get("requested_notional") or notional))
    if position is None:
        return OpenPosition(
            trade_index=len(trades) + 1,
            entry_index=point_index,
            entry_x=point.x_value,
            entry_price=price,
            size=size,
            requested_notional=requested_notional,
            filled_notional=notional,
            fill_pct=_pct(size, requested_size),
            fill_status="FILLED" if size >= requested_size else "PARTIAL",
            book_snapshot_id=pending["fill"].get("book_snapshot_id"),
            avg_fill_price=price,
            fill_probability=Decimal("100"),
            block_volume=size,
            trade_count=1,
            available_notional=notional,
            entry_order_id=pending["order_id"],
            entry_fee_cost=Decimal(str(fill.get("fee_cost") or 0)),
            entry_rebate=Decimal(str(fill.get("rebate") or 0)),
            entry_slippage_cost=Decimal(str(fill.get("slippage_cost") or 0)),
            entry_fill_slices=[_lob_maker_entry_ledger_slice(slice_row, point)],
        )
    total_size = (position.size + size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    total_notional = (position.entry_price * position.size + notional).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    position.size = total_size
    position.entry_price = (total_notional / total_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    position.avg_fill_price = position.entry_price
    position.filled_notional = (position.filled_notional + notional).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    position.available_notional = position.filled_notional
    position.fill_pct = _pct(total_size, requested_size)
    position.fill_status = "FILLED" if total_size >= requested_size else "PARTIAL"
    position.block_volume += size
    position.trade_count += 1
    position.entry_fee_cost += Decimal(str(fill.get("fee_cost") or 0))
    position.entry_rebate += Decimal(str(fill.get("rebate") or 0))
    position.entry_slippage_cost += Decimal(str(fill.get("slippage_cost") or 0))
    position.entry_fill_slices.append(_lob_maker_entry_ledger_slice(slice_row, point))
    return position


def _lob_maker_entry_ledger_slice(slice_row: dict[str, Any], point: PricePoint) -> dict[str, Any]:
    fill = slice_row["fill"]
    evidence = fill.get("execution_model_evidence") if isinstance(fill.get("execution_model_evidence"), dict) else {}
    return {
        "x_value": point.x_value,
        "fill_timestamp": evidence.get("fill_timestamp"),
        "price": fill["entry_price"],
        "size": fill["size"],
        "fee": fill.get("fee_cost", Decimal("0")),
        "rebate": fill.get("rebate", Decimal("0")),
        "slippage_cost": fill.get("slippage_cost", Decimal("0")),
        "source_event_ids": list(evidence.get("source_event_ids") or []),
    }


def _apply_lob_maker_exit_slice(
    position: OpenPosition,
    *,
    pending: dict[str, Any],
    slice_row: dict[str, Any],
    point: PricePoint,
    point_index: int,
    params: BacktestParameters,
    run: dict[str, Any],
    x_axis: str,
) -> tuple[OpenPosition | None, dict[str, Any]]:
    raw_fill = dict(slice_row["fill"])
    raw_size = Decimal(str(raw_fill["size"]))
    close_size = min(position.size, raw_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    if close_size <= 0:
        raise RuntimeError("maker exit FillTick has no remaining position to close")
    if close_size < raw_size:
        fraction = close_size / raw_size
        raw_fill["size"] = close_size
        raw_fill["filled_size"] = close_size
        raw_fill["actual_fill_size"] = close_size
        raw_fill["expected_fill_size"] = close_size
        for key in ("filled_notional", "actual_fill_notional", "expected_fill_notional", "available_notional", "fee_cost", "rebate", "rebate_cost", "execution_cost"):
            raw_fill[key] = (Decimal(str(raw_fill.get(key) or 0)) * fraction).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    position_size_before = position.size
    allocation = close_size / position_size_before
    entry_fee_share = (position.entry_fee_cost * allocation).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    entry_rebate_share = (position.entry_rebate * allocation).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    entry_slippage_share = (position.entry_slippage_cost * allocation).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    position_for_trade = replace(
        position,
        entry_fee_cost=entry_fee_share,
        entry_rebate=entry_rebate_share,
        entry_slippage_cost=entry_slippage_share,
    )
    trade = _close_trade(
        run,
        x_axis,
        position_for_trade,
        point,
        point_index,
        "maker_limit_exit",
        params,
        exit_fill=raw_fill,
        exit_order_id=pending["order_id"],
    )
    remaining = (position_size_before - close_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    if remaining <= Decimal("0.0000000001"):
        return None, trade
    position.size = remaining
    position.requested_notional = (position.requested_notional * (Decimal("1") - allocation)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    position.filled_notional = (position.entry_price * remaining).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    position.available_notional = position.filled_notional
    position.entry_fee_cost = max(Decimal("0"), position.entry_fee_cost - entry_fee_share)
    position.entry_rebate = max(Decimal("0"), position.entry_rebate - entry_rebate_share)
    position.entry_slippage_cost = max(Decimal("0"), position.entry_slippage_cost - entry_slippage_share)
    position.trade_index += 1
    return position, trade


def _lob_maker_order_row(
    pending: dict[str, Any],
    *,
    x_axis: str,
    side: str,
    params: BacktestParameters,
) -> dict[str, Any]:
    return order_from_fill(
        order_id=pending["order_id"],
        signal_index=pending["signal_index"],
        x_axis=x_axis,
        x_value=pending["signal_x"],
        side=side,
        role="maker",
        order_type="pmxt_l2_resting_limit",
        decision_price=pending["limit_price"],
        fill=pending["fill"],
        trade_id=pending["trade_id"] if pending["fill"]["size"] > 0 else None,
        latency_seconds=params.latency_seconds,
        latency_blocks=params.latency_blocks,
    )


def _lob_maker_result_to_fill(
    result: Any,
    *,
    params: BacktestParameters,
    side: str,
    requested_size: Decimal,
    limit_price: Decimal,
    entry: bool,
) -> dict[str, Any]:
    fills = tuple(result.fills) if result is not None else ()
    filled_size = sum((item.size for item in fills), Decimal("0")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    filled_notional = sum((item.notional for item in fills), Decimal("0")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    avg_price = (filled_notional / filled_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if filled_size > 0 else Decimal("0")
    requested_notional = (requested_size * limit_price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    fill_pct = _pct(filled_size, requested_size)
    fee, rebate = _fee_rebate_for_notional(params, filled_notional, "maker")
    state = str(result.state if result is not None else "REJECTED")
    no_fill_reason = str(result.reject_reason if result is not None else "pmxt_compact_book_missing") or ("maker_order_unfilled_by_window" if filled_size <= 0 else "")
    if filled_size >= requested_size:
        fill_status = "FILLED"
    elif filled_size > 0:
        fill_status = "PARTIAL"
    elif state in {"CANCELLED", "CANCELED"}:
        fill_status = "CANCELED"
    elif state == "REJECTED":
        fill_status = "REJECTED"
    else:
        fill_status = "EXPIRED"
    evidence = result.audit_dict() if result is not None else {"state": "REJECTED", "reject_reason": no_fill_reason}
    evidence["position_activation"] = "each_fill_tick"
    evidence["fill_timestamps"] = [item.ts.isoformat() for item in fills]
    evidence["source_event_ids"] = [event_id for item in fills for event_id in item.source_event_ids]
    price_key = "entry_price" if entry else "exit_price"
    return {
        "requested_notional": requested_notional,
        "filled_notional": filled_notional,
        "expected_fill_notional": filled_notional,
        "actual_fill_notional": filled_notional,
        "fill_pct": fill_pct,
        "fill_probability": Decimal("100") if filled_size > 0 else Decimal("0"),
        "size": filled_size,
        price_key: avg_price,
        "avg_fill_price": avg_price,
        "partial_fill": Decimal("0") < filled_size < requested_size,
        "rejected": state == "REJECTED",
        "fill_status": fill_status,
        "requested_size": requested_size,
        "expected_fill_size": filled_size,
        "actual_fill_size": filled_size,
        "filled_size": filled_size,
        "unfilled_size": max(Decimal("0"), requested_size - filled_size),
        "available_notional": filled_notional,
        "block_volume": sum((item.size for item in fills), Decimal("0")),
        "trade_count": len(fills),
        "fee_cost": fee,
        "rebate": rebate,
        "rebate_cost": rebate,
        "slippage_cost": Decimal("0"),
        "execution_cost": fee - rebate,
        "execution_source": "pmxt_l2_orderfilled_maker_timeline",
        "execution_profile": params.execution_profile,
        "order_role": "maker",
        "book_snapshot_id": result.book_snapshot_id if result is not None else None,
        "execution_model_evidence": evidence,
        "no_fill_reason": no_fill_reason,
        "notes": [no_fill_reason] if no_fill_reason else [],
    }


def simulate_orderfilled_v2_tape_strategy(points: list[PricePoint], run: dict[str, Any], params: BacktestParameters) -> dict[str, Any]:
    return _simulate_orderfilled_trade_strategy(points, run, params, v3=False)


def simulate_orderfilled_v3_trade_strategy(points: list[PricePoint], run: dict[str, Any], params: BacktestParameters) -> dict[str, Any]:
    return _simulate_orderfilled_trade_strategy(points, run, params, v3=True)


def _simulate_orderfilled_trade_strategy(
    points: list[PricePoint],
    run: dict[str, Any],
    params: BacktestParameters,
    *,
    v3: bool,
) -> dict[str, Any]:
    """Run the fixed strategy through the selected OrderFilled trade-tape model."""

    if normalize_price_source(run.get("price_source")) != "orderfilled_block_close":
        raise RuntimeError(
            f"{'ORDERFILLED_V3_TRADE' if v3 else 'ORDERFILLED_V2_TAPE'} "
            "requires price_source=orderfilled_block_close"
        )
    raw_token_context = run.get("_orderfilled_v2_token_context")
    token_context: dict[str, Any] = (
        raw_token_context if isinstance(raw_token_context, dict) else {}
    )
    market_id = token_context.get("market_id")
    asset_id = str(token_context.get("token_id_hex") or token_context.get("token_id") or "").strip().lower()
    if not market_id or not asset_id:
        raise RuntimeError(
            f"{'ORDERFILLED_V3_TRADE' if v3 else 'ORDERFILLED_V2_TAPE'} "
            "requires market_id and token_id_hex from quant.market_token_metadata"
        )

    x_axis = "block_number"
    equity = params.initial_capital
    peak = params.initial_capital
    open_position: OpenPosition | None = None
    trades: list[dict[str, Any]] = []
    orders: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    order_index = 0
    capacity: CapacityLedger | RunLiquidityLedger = (
        RunLiquidityLedger(f"main-backtest-v3:{run.get('run_id') or 'pending'}")
        if v3
        else CapacityLedger()
    )
    execution_context = (
        _new_v3_strategy_context(token_context, params)
        if v3
        else _new_v2_strategy_context(token_context)
    )

    for index, point in enumerate(points):
        _report_simulation_progress(points, run, index)
        entry_signal = _entry_signal_context(point, params)
        if open_position is None and entry_signal["triggered"]:
            order_index += 1
            order_id = next_order_id(order_index)
            trade_id = f"T-{len(trades) + 1:04d}"
            limit_price = _v2_buy_limit_price(params)
            target_size = _v2_target_size(params, point, limit_price)
            if v3:
                tape_order = _v3_order_from_signal(
                    order_id=order_id,
                    market_id=int(market_id),
                    asset_id=asset_id,
                    side="BUY",
                    limit_price=limit_price,
                    size=target_size,
                    signal_point=point,
                    params=params,
                    token_context=token_context,
                )
                tape_result = _execute_v3_order(
                    tape_order, capacity, execution_context  # type: ignore[arg-type]
                )
                fill = _v3_result_to_fill(tape_order, tape_result, point, params, entry=True)
            else:
                tape_order = _v2_order_from_signal(
                    order_id=order_id,
                    market_id=int(market_id),
                    asset_id=asset_id,
                    side="BUY",
                    limit_price=limit_price,
                    size=target_size,
                    signal_point=point,
                    params=params,
                )
                tape_result = _execute_v2_order(
                    tape_order, capacity, execution_context  # type: ignore[arg-type]
                )
                fill = _v2_result_to_fill(tape_order, tape_result, point, params, entry=True)
            fill = _with_signal_context(fill, entry_signal)
            order = order_from_fill(
                order_id=order_id,
                signal_index=index + 1,
                x_axis=x_axis,
                x_value=point.x_value,
                side="BUY_YES",
                role="taker",
                order_type=(
                    "fill_only_v3_trade_only_limit"
                    if v3
                    else "orderfilled_v2_trade_tape_limit"
                ),
                decision_price=Decimal(str(entry_signal["decision_price"])),
                fill=fill,
                trade_id=trade_id if Decimal(str(fill.get("size") or 0)) > 0 else None,
                latency_seconds=params.latency_seconds,
                latency_blocks=params.latency_blocks,
                submit_x_override=tape_order.arrival_block,
            )
            _attach_markout_to_order(order, points, index)
            _attach_missed_opportunity_to_order(order, points, index)
            orders.append(order)
            if fill["size"] <= 0:
                events.append(_event("fill_rejected", x_axis, point.x_value, None, Decimal(str(entry_signal["decision_price"])), _trade_result_reason(tape_result) or ("fill_only_v3_no_fill" if v3 else "orderfilled_v2_no_fill"), meta=fill))
            else:
                open_position = OpenPosition(
                    trade_index=len(trades) + 1,
                    entry_index=index,
                    entry_x=point.x_value,
                    entry_price=fill["entry_price"],
                    size=fill["size"],
                    requested_notional=fill["requested_notional"],
                    filled_notional=fill["filled_notional"],
                    fill_pct=fill["fill_pct"],
                    fill_status=fill.get("fill_status", "FILLED"),
                    avg_fill_price=fill.get("avg_fill_price"),
                    fill_probability=fill.get("fill_probability", Decimal("0")),
                    block_volume=fill.get("block_volume", Decimal("0")),
                    trade_count=int(fill.get("trade_count", 0) or 0),
                    available_notional=fill.get("available_notional", Decimal("0")),
                    entry_order_id=order_id,
                    entry_fee_cost=Decimal(str(fill.get("fee_cost") or 0)),
                    entry_rebate=Decimal(str(fill.get("rebate") or fill.get("rebate_cost") or 0)),
                    entry_slippage_cost=Decimal(str(fill.get("slippage_cost") or 0)),
                )
                events.append(_event("open", x_axis, point.x_value, trade_id, Decimal(str(entry_signal["decision_price"])), f"entry threshold reached; filled by {'fill-only v3' if v3 else 'orderfilled v2 tape'}", meta=fill))
        elif open_position is not None:
            exit_signal = _exit_signal_context(point, open_position.entry_price, index - open_position.entry_index, params)
            if exit_signal["reason"]:
                order_index += 1
                order_id = next_order_id(order_index)
                trade_id = f"T-{open_position.trade_index:04d}"
                limit_price = _v2_sell_limit_price(params)
                if v3:
                    tape_order = _v3_order_from_signal(
                        order_id=order_id,
                        market_id=int(market_id),
                        asset_id=asset_id,
                        side="SELL",
                        limit_price=limit_price,
                        size=open_position.size,
                        signal_point=point,
                        params=params,
                        token_context=token_context,
                    )
                    tape_result = _execute_v3_order(
                        tape_order, capacity, execution_context  # type: ignore[arg-type]
                    )
                    exit_fill = _v3_result_to_fill(tape_order, tape_result, point, params, entry=False)
                else:
                    tape_order = _v2_order_from_signal(
                        order_id=order_id,
                        market_id=int(market_id),
                        asset_id=asset_id,
                        side="SELL",
                        limit_price=limit_price,
                        size=open_position.size,
                        signal_point=point,
                        params=params,
                    )
                    tape_result = _execute_v2_order(
                        tape_order, capacity, execution_context  # type: ignore[arg-type]
                    )
                    exit_fill = _v2_result_to_fill(tape_order, tape_result, point, params, entry=False)
                exit_fill = _with_signal_context(exit_fill, exit_signal)
                order = order_from_fill(
                    order_id=order_id,
                    signal_index=index + 1,
                    x_axis=x_axis,
                    x_value=point.x_value,
                    side="SELL_YES",
                    role="taker",
                    order_type=(
                        "fill_only_v3_trade_only_limit"
                        if v3
                        else "orderfilled_v2_trade_tape_limit"
                    ),
                    decision_price=Decimal(str(exit_signal["decision_price"])),
                    fill=exit_fill,
                    trade_id=trade_id,
                    latency_seconds=params.latency_seconds,
                    latency_blocks=params.latency_blocks,
                    submit_x_override=tape_order.arrival_block,
                )
                _attach_markout_to_order(order, points, index)
                _attach_missed_opportunity_to_order(order, points, index)
                orders.append(order)
                if exit_fill["size"] <= 0:
                    events.append(_event("exit_rejected", x_axis, point.x_value, trade_id, Decimal(str(exit_signal["decision_price"])), _trade_result_reason(tape_result) or str(exit_signal["reason"]), meta=exit_fill))
                    continue
                trade = _close_trade(
                    run,
                    x_axis,
                    open_position,
                    point,
                    index,
                    str(exit_signal["reason"]),
                    params,
                    exit_fill=exit_fill,
                    exit_order_id=order_id,
                )
                trades.append(trade)
                equity += trade["pnl"]
                events.append(_event("close", x_axis, point.x_value, trade_id, Decimal(str(exit_signal["decision_price"])), f"exit signal filled by {'fill-only v3' if v3 else 'orderfilled v2 tape'}", meta=exit_fill))
                remaining_size = (open_position.size - exit_fill["size"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
                open_position.size = remaining_size
                if remaining_size <= Decimal("0.0000000001"):
                    open_position = None
                else:
                    open_position.trade_index = len(trades) + 1

        mark_equity = equity
        if open_position is not None:
            mark_equity += (point.price - open_position.entry_price) * open_position.size
        peak = max(peak, mark_equity)
        drawdown = mark_equity - peak
        equity_rows.append(
            {
                "point_index": index + 1,
                "x_axis": x_axis,
                "x_value": point.x_value,
                "equity": mark_equity,
                "drawdown": drawdown,
                "drawdown_pct": _pct(drawdown, peak),
                "cumulative_return": _pct(mark_equity - params.initial_capital, params.initial_capital),
            }
        )

    if open_position is not None:
        last = points[-1]
        settlement_value = _settlement_value(params, points)
        if settlement_value is not None:
            order_index += 1
            order_id = next_order_id(order_index)
            settlement_fill = _settlement_fill(params, last, open_position.size, settlement_value)
            trade_id = f"T-{open_position.trade_index:04d}"
            orders.append(order_from_fill(
                order_id=order_id,
                signal_index=len(points),
                x_axis=x_axis,
                x_value=last.x_value,
                side="SELL_YES",
                role="settlement",
                order_type="settlement",
                decision_price=settlement_value,
                fill=settlement_fill,
                trade_id=trade_id,
                latency_seconds=Decimal("0"),
                latency_blocks=0,
            ))
            trade = _close_trade(run, x_axis, open_position, last, len(points) - 1, "settlement", params, exit_fill=settlement_fill, exit_order_id=order_id)
            trades.append(trade)
            equity += trade["pnl"]
            events.append(_event("settlement", x_axis, last.x_value, trade_id, settlement_value, "held to settlement payoff", meta=settlement_fill))
        else:
            events.append(_event("unresolved_open", x_axis, last.x_value, f"T-{open_position.trade_index:04d}", last.price, f"open {'V3' if v3 else 'V2'}-filled position has no settlement_value", meta={"position_size": open_position.size}))

    if v3:
        ledger_rows = build_source_evidenced_order_ledger_rows(
            orders,
            params.initial_capital,
            market_slug=str(run.get("market_slug") or ""),
            token_side=str(run.get("token_side") or ""),
            token_id=str(token_context.get("token_id") or asset_id),
            gas_cost_per_order=params.gas_cost_per_order,
            settlement_cost=params.settlement_cost,
            redeem_cost=params.redeem_cost,
            cashflow_events=_run_cashflow_events(run),
        )
    else:
        ledger_rows = build_ledger_rows(
            trades,
            params.initial_capital,
            gas_cost_per_order=params.gas_cost_per_order,
            settlement_cost=params.settlement_cost,
            redeem_cost=params.redeem_cost,
            capital_cost_bps=params.capital_cost_bps,
            cashflow_events=_run_cashflow_events(run),
        )
    metrics = build_metrics(trades, equity_rows, points, params, orders=orders, ledger_rows=ledger_rows)
    if v3:
        execution_summary = _summarize_v3_results(execution_context["results"])
        metrics.extend(_v3_metrics(execution_summary, execution_context))
        execution_report = {
            "summary": execution_summary,
            "profile": execution_context["profile"].as_dict(),
            "trade_slice_loads": execution_context["trade_slice_loads"],
            "required_trade_windows": execution_context["required_trade_windows"],
            "match_diagnostics": execution_context["match_diagnostics"],
            "run_liquidity_ledger": capacity.as_dict(),
            "source_coverage": (
                execution_context["coverage"].as_dict()
                if hasattr(execution_context.get("coverage"), "as_dict")
                else None
            ),
            "token_context": token_context,
            "uses_lob_data": False,
        }
    else:
        execution_summary = summarize_v2_results(execution_context["results"])
        metrics.extend(_v2_metrics(execution_summary, execution_context))
        execution_report = {
            "summary": execution_summary,
            "trade_slice_loads": execution_context["trade_slice_loads"],
            "match_diagnostics": execution_context["match_diagnostics"],
            "capacity_ledger": capacity.as_dict(),
            "token_context": token_context,
        }
    result = {
        "trades": trades,
        "equity": equity_rows,
        "metrics": metrics,
        "events": events,
        "orders": orders,
        "ledger": ledger_rows,
    }
    result["fill_only_v3" if v3 else "orderfilled_v2"] = execution_report
    return result


def _new_v2_strategy_context(token_context: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "token_context": dict(token_context),
        "results": [],
        "trade_slice_loads": [],
        "match_diagnostics": [],
    }


def _new_v3_strategy_context(
    token_context: Mapping[str, Any], params: BacktestParameters
) -> dict[str, Any]:
    return {
        "token_context": dict(token_context),
        "profile": get_trade_only_profile(_v3_profile_name(params.execution_profile)),
        "results": [],
        "required_trade_windows": [],
        "trade_slice_loads": [],
        "match_diagnostics": [],
    }


def _v3_order_from_signal(
    *,
    order_id: str,
    market_id: int,
    asset_id: str,
    side: str,
    limit_price: Decimal,
    size: Decimal,
    signal_point: PricePoint,
    params: BacktestParameters,
    token_context: Mapping[str, Any],
) -> TradeOnlyOrder:
    if signal_point.timestamp is None:
        raise RuntimeError("ORDERFILLED_V3_TRADE requires timestamped block-close rows")
    profile = get_trade_only_profile(_v3_profile_name(params.execution_profile))
    seed_material = f"{market_id}|{asset_id.lower()}|{order_id}|{signal_point.x_value}"
    base = TradeOnlyOrder(
        order_id=order_id,
        market_id=int(market_id),
        asset_id=asset_id.lower(),
        side="BUY" if str(side).upper().startswith("BUY") else "SELL",
        limit_price=min(max(Decimal("0"), Decimal(str(limit_price))), Decimal("1")).quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        ),
        size=max(Decimal("0"), Decimal(str(size))).quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        ),
        signal_block=int(signal_point.x_value),
        signal_ts=signal_point.timestamp,
        tif=(profile.default_tif if params.allow_partial_fill else "FOK"),
        liquidity_intent=LiquidityIntent.TAKER,
        allow_partial_fill=bool(params.allow_partial_fill),
        random_seed=int(hashlib.sha256(seed_material.encode("utf-8")).hexdigest()[:16], 16),
        market_slug=str(token_context.get("market_slug") or "") or None,
        market_title=str(token_context.get("market_title") or "") or None,
        category=str(token_context.get("category") or "") or None,
        market_end_ts=(
            token_context.get("market_end_ts")
            if isinstance(token_context.get("market_end_ts"), datetime)
            else None
        ),
    )
    profiled = with_trade_only_profile(base, profile)
    latency_seconds = Decimal(str(params.latency_seconds or 0))
    horizon_blocks = (
        int(params.cancel_after_blocks)
        if int(params.cancel_after_blocks or 0) > 0
        else profiled.horizon_blocks
    )
    return replace(
        profiled,
        latency=(
            timedelta(seconds=float(max(Decimal("0"), latency_seconds)))
            if latency_seconds > 0
            else profiled.latency
        ),
        latency_blocks=max(0, int(params.latency_blocks or profiled.latency_blocks or 0)),
        horizon_blocks=horizon_blocks,
    )


def _execute_v3_order(
    order: TradeOnlyOrder,
    capacity: RunLiquidityLedger,
    context: dict[str, Any],
) -> TradeOnlyOrderResult:
    arrival_block = order.arrival_block
    deadline_block = order.deadline_block
    if arrival_block is None or deadline_block is None:
        raise RuntimeError("ORDERFILLED_V3_TRADE requires block-native order windows")
    client = context.get("client")
    if client is None:
        client = ClickHouseClient()
        context["client"] = client
    coverage = context.get("coverage")
    if coverage is None:
        coverage = load_trade_tape_coverage(client)
        context["coverage"] = coverage
    window = RequiredTradeWindow(
        market_id=order.market_id,
        asset_id=order.asset_id,
        aggressor_side=None,
        start_block=max(0, arrival_block - order.lookback_blocks),
        end_block=deadline_block,
    )
    if not coverage.contains(window.start_block, window.end_block):
        raise RuntimeError(
            "ORDERFILLED_V3_TRADE coverage gap for "
            f"market_id={order.market_id} asset_id={order.asset_id} "
            f"from_block={window.start_block} to_block={window.end_block}"
        )
    trade_slice = load_v2_trade_slices_for_windows(
        [window],
        client=client,
        merge_gap_blocks=0,
        limit_per_window=_v2_limit_per_window(),
        reject_truncated_windows=True,
    )
    results, _, diagnostics = replay_trade_only_orders_with_diagnostics(
        [order],
        trade_slice.trades,
        context["profile"],
        ledger=capacity,
    )
    result = results[0]
    context["results"].append(result)
    context["required_trade_windows"].append(
        {
            "market_id": window.market_id,
            "asset_id": window.asset_id,
            "start_block": window.start_block,
            "end_block": window.end_block,
        }
    )
    context["trade_slice_loads"].append(trade_slice.as_dict())
    context["match_diagnostics"].append(diagnostics.as_dict())
    return result


def _trade_result_reason(result: V2OrderResult | TradeOnlyOrderResult) -> str:
    return str(
        result.reason
        if isinstance(result, TradeOnlyOrderResult)
        else result.reason_unfilled
        or ""
    )


def _v3_result_to_fill(
    order: TradeOnlyOrder,
    result: TradeOnlyOrderResult,
    point: PricePoint,
    params: BacktestParameters,
    *,
    entry: bool,
) -> dict[str, Any]:
    requested_size = Decimal(str(result.requested_size or order.size or 0)).quantize(
        Decimal("0.0000000001"), rounding=ROUND_HALF_UP
    )
    expected_size = Decimal(str(result.filled_size or 0)).quantize(
        Decimal("0.0000000001"), rounding=ROUND_HALF_UP
    )
    positive_fills = tuple(fill for fill in result.fills if fill.filled_size > 0)
    source_fills = tuple(fill for fill in positive_fills if fill.source_trade_ids)
    modeled_fills = tuple(fill for fill in positive_fills if not fill.source_trade_ids)
    actual_size = sum((fill.filled_size for fill in source_fills), Decimal("0")).quantize(
        Decimal("0.0000000001"), rounding=ROUND_HALF_UP
    )
    expected_notional = sum(
        (fill.filled_size * fill.exec_price for fill in positive_fills), Decimal("0")
    ).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    actual_notional = sum(
        (fill.filled_size * fill.exec_price for fill in source_fills), Decimal("0")
    ).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    avg_price = (
        (expected_notional / expected_size).quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        )
        if expected_size > 0
        else None
    )
    expected_fee, expected_rebate = _fee_rebate_for_notional(
        params, expected_notional, "taker"
    )
    actual_fee, actual_rebate = _fee_rebate_for_notional(
        params, actual_notional, "taker"
    )
    consumed_events = [_v3_fill_event(fill) for fill in source_fills]
    expected_is_observed = expected_size == actual_size and not modeled_fills
    if actual_size > 0 and modeled_fills:
        evidence_type = "mixed_orderfilled_and_model"
    elif actual_size > 0:
        evidence_type = "raw_orderfilled"
    elif expected_size > 0:
        evidence_type = "modeled_expectation"
    else:
        evidence_type = "none"
    modeled_probabilities = [
        value
        for fill in modeled_fills
        for value in (fill.p_fill_horizon, fill.p_fill_1s)
        if value is not None
    ]
    probability = (
        Decimal("1")
        if actual_size > 0
        else max(modeled_probabilities, default=Decimal("0"))
    )
    probability_pct = (probability * Decimal("100")).quantize(
        Decimal("0.0000000001"), rounding=ROUND_HALF_UP
    )
    profile_participation = get_trade_only_profile(
        _v3_profile_name(params.execution_profile)
    ).participation_rate
    context_rate = max(
        profile_participation,
        *(fill.participation_rate or Decimal("0") for fill in positive_fills),
    )
    effective_liquidity_cap_pct = (context_rate * Decimal("100")).quantize(
        Decimal("0.0000000001"), rounding=ROUND_HALF_UP
    )
    probability_models = sorted(
        {fill.model_version for fill in positive_fills if fill.model_version}
    )
    notes = [result.reason] if result.reason else []
    fill: dict[str, Any] = {
        "requested_notional": (requested_size * order.limit_price).quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        ),
        "filled_notional": expected_notional,
        "expected_fill_notional": expected_notional,
        "actual_fill_notional": actual_notional,
        "fill_pct": (
            expected_size / requested_size * Decimal("100")
        ).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        if requested_size > 0
        else Decimal("0"),
        "fill_probability": probability_pct,
        "raw_fill_probability": probability_pct,
        "effective_fill_probability": probability_pct,
        "fill_probability_model": ",".join(probability_models)
        or result.execution_mode,
        "fill_probability_haircut_pct": Decimal("0"),
        "effective_liquidity_cap_pct": effective_liquidity_cap_pct,
        "size": expected_size,
        "avg_fill_price": avg_price,
        "partial_fill": Decimal("0") < expected_size < requested_size,
        "rejected": expected_size <= 0,
        "fill_status": result.status,
        "requested_size": requested_size,
        "expected_fill_size": expected_size,
        "actual_fill_size": actual_size,
        "filled_size": expected_size,
        "unfilled_size": max(Decimal("0"), requested_size - expected_size),
        "block_volume": Decimal("0"),
        "trade_count": len(source_fills),
        "available_notional": expected_notional,
        "participation_rate": context_rate,
        "fee_cost": expected_fee.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "rebate": expected_rebate.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "actual_fee_cost": actual_fee.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "actual_rebate": actual_rebate.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "slippage_cost": Decimal("0"),
        "execution_cost": (expected_fee - expected_rebate).quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        ),
        "execution_source": "fill_only_v3_trade_only",
        "execution_evidence_type": evidence_type,
        "expected_fill_is_observed_execution": expected_is_observed,
        "modeled_fill_size": max(Decimal("0"), expected_size - actual_size),
        "notes": notes,
        "candidate_events": consumed_events,
        "consumed_events": consumed_events,
        "fill_only_v3": result.as_dict(),
        "not_l2_depth_or_l3_queue": True,
        "block_bar_used_for_fill": False,
        "block_bar_execution_model": "disabled_for_fill_only_v3_trade_only",
        "signal_block": int(point.x_value),
        "arrival_block": order.arrival_block,
        "time_in_force": order.tif,
        "lookback_blocks": order.lookback_blocks,
        "horizon_blocks": order.horizon_blocks,
        "result_role": result.result_role,
        "calibration_status": result.calibration_status,
    }
    if entry:
        fill["entry_price"] = avg_price or order.limit_price
    else:
        fill["exit_price"] = avg_price or order.limit_price
    return fill


def _v3_fill_event(fill: Any) -> dict[str, Any]:
    return {
        "trade_id": fill.source_trade_ids[0] if fill.source_trade_ids else None,
        "source_trade_ids": list(fill.source_trade_ids),
        "tx_hash": fill.source_tx_hashes[0] if fill.source_tx_hashes else None,
        "source_tx_hashes": list(fill.source_tx_hashes),
        "block_number": fill.fill_block,
        "fill_ts": fill.fill_ts.isoformat(),
        "log_indexes": list(fill.source_log_indexes),
        "price": fill.exec_price,
        "size": fill.filled_size,
        "execution_mode": fill.execution_mode,
        "evidence_tier": fill.evidence_tier,
        "trigger_type": fill.trigger_type,
    }


def _summarize_v3_results(results: list[TradeOnlyOrderResult]) -> dict[str, Any]:
    expected_size = sum((row.filled_size for row in results), Decimal("0"))
    actual_size = sum(
        (
            fill.filled_size
            for row in results
            for fill in row.fills
            if fill.source_trade_ids
        ),
        Decimal("0"),
    )
    modeled_size = max(Decimal("0"), expected_size - actual_size)
    source_orders = sum(
        any(fill.source_trade_ids and fill.filled_size > 0 for fill in row.fills)
        for row in results
    )
    modeled_orders = sum(
        any(not fill.source_trade_ids and fill.filled_size > 0 for fill in row.fills)
        for row in results
    )
    mixed_orders = sum(
        any(fill.source_trade_ids and fill.filled_size > 0 for fill in row.fills)
        and any(not fill.source_trade_ids and fill.filled_size > 0 for fill in row.fills)
        for row in results
    )
    abstained_orders = sum(
        row.status == "UNOBSERVABLE" or "out_of_domain" in row.reason
        for row in results
    )
    return {
        "attempted_orders": len(results),
        "positive_expected_orders": sum(row.filled_size > 0 for row in results),
        "source_confirmed_orders": source_orders,
        "modeled_orders": modeled_orders,
        "mixed_orders": mixed_orders,
        "no_fill_orders": sum(row.filled_size <= 0 for row in results) - abstained_orders,
        "abstained_orders": abstained_orders,
        "expected_fill_size": expected_size.quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        ),
        "actual_fill_size": actual_size.quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        ),
        "modeled_fill_size": modeled_size.quantize(
            Decimal("0.0000000001"), rounding=ROUND_HALF_UP
        ),
    }


def _v3_metrics(summary: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [
        ("fill_only_v3_attempted_orders", "V3 Attempted Orders", summary.get("attempted_orders"), "orders"),
        ("fill_only_v3_positive_expected_orders", "V3 Positive Expected Orders", summary.get("positive_expected_orders"), "orders"),
        ("fill_only_v3_source_confirmed_orders", "V3 Source-confirmed Orders", summary.get("source_confirmed_orders"), "orders"),
        ("fill_only_v3_modeled_orders", "V3 Modeled Orders", summary.get("modeled_orders"), "orders"),
        ("fill_only_v3_abstained_orders", "V3 Abstained Orders", summary.get("abstained_orders"), "orders"),
        ("fill_only_v3_expected_fill_size", "V3 Expected Fill Size", summary.get("expected_fill_size"), "shares"),
        ("fill_only_v3_actual_fill_size", "V3 Actual Fill Size", summary.get("actual_fill_size"), "shares"),
        ("fill_only_v3_trade_rows_loaded", "V3 Trade Rows Loaded", sum(int(row.get("rows_loaded") or 0) for row in context.get("trade_slice_loads", [])), "trade_prints"),
    ]
    return [
        {
            "metric_key": key,
            "metric_name": name,
            "metric_group": "fill_only_v3",
            "value": Decimal(str(value or 0)),
            "formatted_value": str(value or 0),
            "delta": delta,
            "status": "neutral",
            "tooltip": "Fill-only V3 observed and modeled execution metric",
            "sort_order": 10100 + index,
        }
        for index, (key, name, value, delta) in enumerate(rows)
    ]


def _v2_profile_name(params: BacktestParameters) -> str:
    profile = str(params.execution_profile or "").strip().lower().replace("-", "_")
    if profile in {"strict", "strict_audit", "audit"}:
        return "strict_audit"
    if profile in {"optimistic", "optimistic_sensitivity", "sensitivity"}:
        return "optimistic_sensitivity"
    if profile in {
        "probabilistic_conservative",
        "probability_conservative",
        "orderfilled_probability_conservative",
        "primary_conservative",
    }:
        return "probabilistic_conservative"
    if profile in {
        "probabilistic_source_confirmed",
        "probability_source_confirmed",
        "orderfilled_probability_source_confirmed",
    }:
        return "probabilistic_source_confirmed"
    if profile in {
        "probabilistic",
        "probability",
        "probabilistic_trade_tape",
        "orderfilled_probability",
        "primary_calibrated",
        "realistic",
    }:
        return "probabilistic_trade_tape"
    if profile in {"conservative", "conservative_trade_tape", "trade_tape"}:
        return "conservative_trade_tape"
    return "probabilistic_trade_tape"


def _v2_horizon_blocks(params: BacktestParameters) -> int:
    if int(params.cancel_after_blocks or 0) > 0:
        return int(params.cancel_after_blocks)
    return _env_int("POLYDATA_QUANT_ORDERFILLED_V2_HORIZON_BLOCKS", 20000)


def _v2_time_in_force(params: BacktestParameters) -> str:
    return "FAK" if bool(params.allow_partial_fill) else "FOK"


def _v2_market_window_cap() -> Decimal | None:
    raw = os.environ.get("POLYDATA_QUANT_ORDERFILLED_V2_MARKET_WINDOW_CAP")
    if raw in (None, ""):
        return None
    value = Decimal(str(raw))
    return value if value > 0 else None


def _v2_market_window_blocks() -> int | None:
    value = _env_int("POLYDATA_QUANT_ORDERFILLED_V2_MARKET_WINDOW_BLOCKS", 0)
    return value if value > 0 else None


def _v2_limit_per_window() -> int:
    return _env_int("POLYDATA_QUANT_ORDERFILLED_V2_LIMIT_PER_WINDOW", 250000)


def _v2_order_from_signal(
    *,
    order_id: str,
    market_id: int,
    asset_id: str,
    side: str,
    limit_price: Decimal,
    size: Decimal,
    signal_point: PricePoint,
    params: BacktestParameters,
) -> V2TakerOrder:
    base = V2TakerOrder(
        order_id=order_id,
        market_id=int(market_id),
        asset_id=asset_id.lower(),
        side="BUY" if str(side).upper().startswith("BUY") else "SELL",
        limit_price=min(max(Decimal("0"), Decimal(str(limit_price))), Decimal("1")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        size=max(Decimal("0"), Decimal(str(size))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        signal_block=int(signal_point.x_value),
        signal_ts=signal_point.timestamp,
        tif=_v2_time_in_force(params),  # type: ignore[arg-type]
        allow_partial_fill=bool(params.allow_partial_fill),
        market_window_cap=_v2_market_window_cap(),
        market_window_blocks=_v2_market_window_blocks(),
    )
    profiled = with_v2_execution_profile(base, _v2_profile_name(params))
    latency_seconds = Decimal(str(params.latency_seconds or 0))
    return replace(
        profiled,
        latency_blocks=max(0, int(params.latency_blocks or profiled.latency_blocks or 0)),
        latency=timedelta(seconds=float(max(Decimal("0"), latency_seconds))) if latency_seconds > 0 else profiled.latency,
        horizon_blocks=_v2_horizon_blocks(params),
    )


def _execute_v2_order(
    order: V2TakerOrder,
    capacity: CapacityLedger,
    context: dict[str, Any],
) -> V2OrderResult:
    trade_slice = load_v2_trade_slices_for_orders(
        [order],
        merge_gap_blocks=0,
        limit_per_window=_v2_limit_per_window(),
    )
    results, _, diagnostics = replay_v2_taker_orders_with_diagnostics([order], trade_slice.trades, ledger=capacity)
    result = results[0]
    context["results"].append(result)
    context["trade_slice_loads"].append(trade_slice.as_dict())
    context["match_diagnostics"].append(diagnostics.as_dict())
    return result


def _v2_buy_limit_price(params: BacktestParameters) -> Decimal:
    value = params.buy_limit_price if params.buy_limit_price is not None else params.max_entry_price
    return min(max(Decimal("0"), Decimal(str(value))), Decimal("1")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _v2_sell_limit_price(params: BacktestParameters) -> Decimal:
    value = params.sell_limit_price if params.sell_limit_price is not None else params.min_exit_price
    return min(max(Decimal("0"), Decimal(str(value))), Decimal("1")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _v2_target_size(params: BacktestParameters, point: PricePoint, limit_price: Decimal) -> Decimal:
    reference_price = max(Decimal("0.0000000001"), min(Decimal("1"), Decimal(str(limit_price or point.price or 0))))
    return (_target_notional(params) / reference_price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _v2_result_to_fill(
    order: V2TakerOrder,
    result: V2OrderResult,
    point: PricePoint,
    params: BacktestParameters,
    *,
    entry: bool,
) -> dict[str, Any]:
    filled_size = Decimal(str(result.filled_size or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    requested_size = Decimal(str(result.requested_size or order.size or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    avg_price = Decimal(str(result.avg_price or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if filled_size > 0 else None
    filled_notional = Decimal(str(result.filled_notional or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    fee_cost, rebate = _fee_rebate_for_notional(params, filled_notional, "taker")
    slippage_cost = (Decimal(str(result.avg_price_buffer or 0)) * filled_size).copy_abs().quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    fill_status = "FILLED" if result.status == "FILLED" else "PARTIAL" if result.status == "PARTIAL_FILLED" else "NO_FILL"
    consumed_events = [_v2_fill_event(fill) for fill in result.fills]
    notes = [result.reason_unfilled] if result.reason_unfilled else []
    fill: dict[str, Any] = {
        "requested_notional": (requested_size * order.limit_price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "filled_notional": filled_notional,
        "expected_fill_notional": filled_notional,
        "actual_fill_notional": filled_notional,
        "fill_pct": (filled_size / requested_size * Decimal("100")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if requested_size > 0 else Decimal("0"),
        "fill_probability": (
            Decimal(str(result.p_fill or 0)) * Decimal("100")
            if result.p_fill is not None
            else Decimal("100") if filled_size > 0 else Decimal("0")
        ).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "size": filled_size,
        "avg_fill_price": avg_price,
        "liquidity_cap_pct": (result.participation_rate * Decimal("100")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "min_fill_pct": max(Decimal("0"), Decimal(str(params.min_fill_pct))),
        "partial_fill": result.status == "PARTIAL_FILLED",
        "rejected": filled_size <= 0,
        "fill_status": fill_status,
        "requested_size": requested_size,
        "expected_fill_size": filled_size,
        "actual_fill_size": filled_size,
        "filled_size": filled_size,
        "unfilled_size": Decimal(str(result.unfilled_size or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "block_volume": Decimal(str(result.eligible_historical_volume or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "trade_count": len(result.fills),
        "available_notional": (Decimal(str(result.eligible_historical_volume or 0)) * (avg_price or Decimal("0"))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "participation_rate": Decimal(str(result.participation_rate or 0)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "fee_cost": fee_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "rebate": rebate.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "slippage_cost": slippage_cost,
        "execution_cost": (fee_cost + slippage_cost - rebate).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "execution_source": "orderfilled_v2_trade_tape",
        "execution_evidence_type": "raw_orderfilled",
        "notes": notes,
        "candidate_events": consumed_events,
        "consumed_events": consumed_events,
        "orderfilled_v2": result.as_dict(),
        "not_l2_depth_or_l3_queue": True,
        "block_bar_used_for_fill": False,
        "block_bar_execution_model": "disabled_for_orderfilled_v2_tape",
        "signal_block": int(point.x_value),
        "arrival_block": result.arrival_block,
        "avg_fill_delay_seconds": result.avg_fill_delay_seconds,
        "capacity_utilization": result.capacity_utilization,
    }
    if entry:
        fill["entry_price"] = avg_price or order.limit_price
    else:
        fill["exit_price"] = avg_price or order.limit_price
    return fill


def _v2_fill_event(fill: Any) -> dict[str, Any]:
    return {
        "trade_id": fill.source_trade_id,
        "tx_hash": fill.source_tx_hash,
        "block_number": fill.fill_block,
        "log_indexes": list(fill.source_log_indexes),
        "side": fill.side,
        "price": fill.exec_price,
        "size": fill.filled_size,
        "historical_price": fill.historical_price,
        "historical_size": fill.historical_size,
        "allocated_capacity": fill.allocated_capacity,
        "participation_rate": fill.participation_rate,
        "tx_index_source": fill.tx_index_source,
    }


def _v2_metrics(summary: dict[str, Any], context: dict[str, Any]) -> list[dict[str, Any]]:
    start = 10000
    rows = [
        ("orderfilled_v2_attempted_orders", "V2 Attempted Orders", "orderfilled_v2", summary.get("attempted_orders"), "orders"),
        ("orderfilled_v2_filled_orders", "V2 Filled Orders", "orderfilled_v2", summary.get("filled_orders"), "orders"),
        ("orderfilled_v2_partial_orders", "V2 Partial Orders", "orderfilled_v2", summary.get("partial_orders"), "orders"),
        ("orderfilled_v2_unfilled_orders", "V2 Unfilled Orders", "orderfilled_v2", summary.get("unfilled_orders"), "orders"),
        ("orderfilled_v2_eligible_volume", "V2 Eligible Volume", "orderfilled_v2", summary.get("eligible_historical_volume"), "shares"),
        ("orderfilled_v2_simulated_volume", "V2 Simulated Volume", "orderfilled_v2", summary.get("simulated_volume"), "shares"),
        ("orderfilled_v2_avg_delay", "V2 Avg Fill Delay", "orderfilled_v2", summary.get("avg_fill_delay_seconds"), "blocks/seconds"),
        ("orderfilled_v2_trade_rows_loaded", "V2 Trade Rows Loaded", "orderfilled_v2", sum(int(row.get("rows_loaded") or 0) for row in context.get("trade_slice_loads", [])), "trade_prints"),
    ]
    return [
        {
            "metric_key": key,
            "metric_name": name,
            "metric_group": group,
            "value": Decimal(str(value or 0)),
            "formatted_value": str(value or 0),
            "delta": delta,
            "status": "neutral",
            "tooltip": "OrderFilled-only V2 trade-tape execution metric",
            "sort_order": start + index,
        }
        for index, (key, name, group, value, delta) in enumerate(rows)
    ]


def simulate_limit_replay_strategy(points: list[PricePoint], run: dict[str, Any], params: BacktestParameters) -> dict[str, Any]:
    """Replay a small passive limit-order strategy against historical OrderFilled prices.

    This mode intentionally does not treat `price >= threshold` as a fillable
    opportunity. A buy only fills after a later historical trade/block close is
    at or below the buy limit; a sell only fills after a later price is at or
    above the sell limit. If the sell never crosses, the position is held to
    settlement instead of being force-closed at the final traded price.
    """

    x_axis = "timestamp" if run["price_source"] == "frontend" else "block_number"
    equity = params.initial_capital
    peak = params.initial_capital
    orders: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    order_index = 0
    buy_limit = _buy_limit_price(params)
    first = points[0]
    open_position: OpenPosition | None = None
    buy_order_emitted = False
    sell_order_emitted = False
    sell_order_signal_x: int | None = None
    sell_order_signal_index: int | None = None
    sell_limit: Decimal | None = None
    replay_events = _run_replay_events(run)
    buy_submit_x = _effective_submit_x(first.x_value, points, 0, params, x_axis)
    replay_role = _limit_replay_role(params)
    pending_entry: dict[str, Any] | None = None
    pending_exit: dict[str, Any] | None = None
    entry_raw_replay_suppression: dict[str, Any] | None = None
    exit_raw_replay_suppression: dict[str, Any] | None = None

    for index, point in enumerate(points):
        _report_simulation_progress(points, run, index)
        handled_pending_entry = False
        if pending_entry is not None and open_position is None:
            handled_pending_entry = True
            pending_fill = pending_entry["fill"]
            remaining_size = max(Decimal("0"), Decimal(str(pending_fill.get("requested_size") or 0)) - Decimal(str(pending_fill.get("size") or 0)))
            if remaining_size > 0 and int(point.x_value) > int(pending_entry.get("last_x") or 0):
                continuation_events = _filter_replay_events_after(replay_events, pending_entry.get("last_sequence"))
                continuation = _limit_replay_fill(
                    params,
                    point,
                    "BUY_YES",
                    limit_price=buy_limit,
                    decision_x=first.x_value,
                    signal_index=1,
                    submit_x=buy_submit_x,
                    target_size=remaining_size,
                    x_axis=x_axis,
                    replay_events=continuation_events,
                    allow_block_bar_fallback=not replay_events,
                )
                if Decimal(str(continuation.get("size") or 0)) > 0:
                    pending_fill = _merge_resting_limit_fills(pending_fill, continuation, x_value=int(point.x_value))
                    pending_entry["fill"] = pending_fill
                    pending_entry["last_x"] = int(point.x_value)
                    pending_entry["last_sequence"] = _last_consumed_event_sequence(continuation) or pending_entry.get("last_sequence")
                    events.append(_event("buy_partial_fill", x_axis, point.x_value, pending_entry["trade_id"], point.price, "resting buy order received additional fill", meta=continuation))
            if Decimal(str(pending_fill.get("unfilled_size") or 0)) <= Decimal("0.0000000001"):
                order = order_from_fill(
                    order_id=pending_entry["order_id"],
                    signal_index=1,
                    x_axis=x_axis,
                    x_value=first.x_value,
                    side="BUY_YES",
                    role=replay_role,
                    order_type=_limit_replay_order_type(params),
                    decision_price=buy_limit,
                    fill=pending_fill,
                    trade_id=pending_entry["trade_id"],
                    latency_seconds=params.latency_seconds,
                    latency_blocks=params.latency_blocks,
                    submit_x_override=buy_submit_x,
                )
                _attach_markout_to_order(order, points, index)
                _attach_missed_opportunity_to_order(order, points, index)
                orders.append(order)
                open_position = OpenPosition(
                    trade_index=len(trades) + 1,
                    entry_index=index,
                    entry_x=point.x_value,
                    entry_price=pending_fill.get("entry_price") or buy_limit,
                    size=pending_fill["size"],
                    requested_notional=pending_fill["requested_notional"],
                    filled_notional=pending_fill["filled_notional"],
                    fill_pct=pending_fill["fill_pct"],
                    fill_status=pending_fill.get("fill_status", "FILLED"),
                    book_snapshot_id=pending_fill.get("book_snapshot_id"),
                    snapshot_version=pending_fill.get("snapshot_version"),
                    staleness_seconds=pending_fill.get("staleness_seconds"),
                    staleness_blocks=pending_fill.get("staleness_blocks"),
                    avg_fill_price=pending_fill.get("avg_fill_price"),
                    fill_probability=pending_fill.get("fill_probability", Decimal("100")),
                    block_volume=pending_fill.get("block_volume", point.volume),
                    trade_count=int(pending_fill.get("trade_count", point.trade_count) or 0),
                    available_notional=pending_fill.get("available_notional", pending_fill["filled_notional"]),
                    entry_order_id=pending_entry["order_id"],
                    entry_fee_cost=Decimal(str(pending_fill.get("fee_cost") or 0)),
                    entry_rebate=Decimal(str(pending_fill.get("rebate") or pending_fill.get("rebate_cost") or 0)),
                    entry_slippage_cost=Decimal(str(pending_fill.get("slippage_cost") or 0)),
                )
                sell_order_signal_x = point.x_value
                sell_order_signal_index = index
                sell_limit = _sell_limit_price(params, open_position.entry_price)
                events.append(_event("open", x_axis, point.x_value, pending_entry["trade_id"], point.price, "resting buy order filled", meta=pending_fill))
                pending_entry = None

        if not handled_pending_entry and open_position is None and not buy_order_emitted and int(point.x_value) >= buy_submit_x:
            buy_crossed = _limit_replay_crossed_at(
                replay_events,
                point,
                "BUY_YES",
                buy_limit,
                decision_x=first.x_value,
                submit_x=buy_submit_x,
                params=params,
                x_axis=x_axis,
            )
            if not buy_crossed:
                entry_raw_replay_suppression = _raw_replay_fallback_suppression_context(
                    replay_events,
                    point,
                    "BUY_YES",
                    buy_limit,
                    decision_x=first.x_value,
                    submit_x=buy_submit_x,
                    params=params,
                    x_axis=x_axis,
                ) or entry_raw_replay_suppression
            if _limit_replay_should_attempt(replay_role, point, buy_submit_x, buy_crossed):
                order_index += 1
                order_id = next_order_id(order_index)
                fill = _limit_replay_fill(
                    params,
                    point,
                    "BUY_YES",
                    limit_price=buy_limit,
                    decision_x=first.x_value,
                    signal_index=1,
                    submit_x=buy_submit_x,
                    x_axis=x_axis,
                    replay_events=replay_events,
                )
                trade_id = f"T-{len(trades) + 1:04d}" if not fill.get("rejected") else None
                if _should_continue_resting_entry(fill, params, replay_role):
                    pending_entry = {
                        "order_id": order_id,
                        "trade_id": trade_id,
                        "fill": _mark_resting_limit_fill(fill, x_value=int(point.x_value)),
                        "last_x": int(point.x_value),
                        "last_sequence": _last_consumed_event_sequence(fill),
                    }
                    buy_order_emitted = True
                    events.append(_event("buy_partial_fill", x_axis, point.x_value, trade_id, point.price, "resting buy order partially filled", meta=fill))
                    continue
                order = order_from_fill(
                    order_id=order_id,
                    signal_index=1,
                    x_axis=x_axis,
                    x_value=first.x_value,
                    side="BUY_YES",
                    role=replay_role,
                    order_type=_limit_replay_order_type(params),
                    decision_price=buy_limit,
                    fill=fill,
                    trade_id=trade_id,
                    latency_seconds=params.latency_seconds,
                    latency_blocks=params.latency_blocks,
                    submit_x_override=buy_submit_x,
                )
                _attach_markout_to_order(order, points, index)
                _attach_missed_opportunity_to_order(order, points, index)
                orders.append(order)
                buy_order_emitted = True
                if fill.get("rejected") or Decimal(str(fill.get("size") or 0)) <= 0:
                    events.append(_event("buy_no_fill", x_axis, point.x_value, None, point.price, "buy limit crossed but volume cap prevented fill", meta=fill))
                    continue
                open_position = OpenPosition(
                    trade_index=len(trades) + 1,
                    entry_index=index,
                    entry_x=point.x_value,
                    entry_price=fill.get("entry_price") or buy_limit,
                    size=fill["size"],
                    requested_notional=fill["requested_notional"],
                    filled_notional=fill["filled_notional"],
                    fill_pct=fill["fill_pct"],
                    fill_status=fill.get("fill_status", "FILLED"),
                    book_snapshot_id=fill.get("book_snapshot_id"),
                    snapshot_version=fill.get("snapshot_version"),
                    staleness_seconds=fill.get("staleness_seconds"),
                    staleness_blocks=fill.get("staleness_blocks"),
                    avg_fill_price=fill.get("avg_fill_price"),
                    fill_probability=fill.get("fill_probability", Decimal("100")),
                    block_volume=fill.get("block_volume", point.volume),
                    trade_count=int(fill.get("trade_count", point.trade_count) or 0),
                    available_notional=fill.get("available_notional", fill["filled_notional"]),
                    entry_order_id=order_id,
                    entry_fee_cost=Decimal(str(fill.get("fee_cost") or 0)),
                    entry_rebate=Decimal(str(fill.get("rebate") or fill.get("rebate_cost") or 0)),
                    entry_slippage_cost=Decimal(str(fill.get("slippage_cost") or 0)),
                )
                sell_order_signal_x = point.x_value
                sell_order_signal_index = index
                sell_limit = _sell_limit_price(params, open_position.entry_price)
                events.append(_event("open", x_axis, point.x_value, trade_id, point.price, "buy limit crossed", meta=fill))

        elif open_position is not None:
            if sell_order_signal_x is None:
                sell_order_signal_x = open_position.entry_x
            if sell_order_signal_index is None:
                sell_order_signal_index = open_position.entry_index
            if sell_limit is None:
                sell_limit = _sell_limit_price(params, open_position.entry_price)
            sell_submit_x = _effective_submit_x(sell_order_signal_x, points, sell_order_signal_index, params, x_axis)
            if pending_exit is not None:
                pending_fill = pending_exit["fill"]
                remaining_size = max(Decimal("0"), Decimal(str(pending_fill.get("requested_size") or 0)) - Decimal(str(pending_fill.get("size") or 0)))
                if remaining_size > 0 and int(point.x_value) > int(pending_exit.get("last_x") or 0):
                    continuation_events = _filter_replay_events_after(replay_events, pending_exit.get("last_sequence"))
                    continuation = _limit_replay_fill(
                        params,
                        point,
                        "SELL_YES",
                        limit_price=sell_limit,
                        decision_x=sell_order_signal_x,
                        signal_index=index + 1,
                        submit_x=sell_submit_x,
                        target_size=remaining_size,
                        x_axis=x_axis,
                        replay_events=continuation_events,
                        allow_block_bar_fallback=not replay_events,
                    )
                    if Decimal(str(continuation.get("size") or 0)) > 0:
                        pending_fill = _merge_resting_limit_fills(pending_fill, continuation, x_value=int(point.x_value))
                        pending_exit["fill"] = pending_fill
                        pending_exit["last_x"] = int(point.x_value)
                        pending_exit["last_sequence"] = _last_consumed_event_sequence(continuation) or pending_exit.get("last_sequence")
                        events.append(_event("sell_partial_fill", x_axis, point.x_value, pending_exit["trade_id"], point.price, "resting sell order received additional fill", meta=continuation))
                if Decimal(str(pending_fill.get("unfilled_size") or 0)) <= Decimal("0.0000000001"):
                    order = order_from_fill(
                        order_id=pending_exit["order_id"],
                        signal_index=pending_exit["signal_index"],
                        x_axis=x_axis,
                        x_value=pending_exit["signal_x"],
                        side="SELL_YES",
                        role=replay_role,
                        order_type=_limit_replay_order_type(params),
                        decision_price=sell_limit,
                        fill=pending_fill,
                        trade_id=pending_exit["trade_id"],
                        latency_seconds=params.latency_seconds,
                        latency_blocks=params.latency_blocks,
                        submit_x_override=sell_submit_x,
                    )
                    _attach_markout_to_order(order, points, index)
                    _attach_missed_opportunity_to_order(order, points, index)
                    orders.append(order)
                    trade = _close_trade(run, x_axis, open_position, point, index, "limit_exit", params, exit_fill=pending_fill, exit_order_id=pending_exit["order_id"])
                    trades.append(trade)
                    equity += trade["pnl"]
                    events.append(_event("close", x_axis, point.x_value, pending_exit["trade_id"], point.price, "resting sell order filled", meta=pending_fill))
                    open_position = None
                    pending_exit = None
                    sell_order_emitted = True
            elif not sell_order_emitted:
                sell_crossed = _limit_replay_crossed_at(
                    replay_events,
                    point,
                    "SELL_YES",
                    sell_limit,
                    decision_x=sell_order_signal_x,
                    submit_x=sell_submit_x,
                    params=params,
                    x_axis=x_axis,
                )
                if not sell_crossed:
                    exit_raw_replay_suppression = _raw_replay_fallback_suppression_context(
                        replay_events,
                        point,
                        "SELL_YES",
                        sell_limit,
                        decision_x=sell_order_signal_x,
                        submit_x=sell_submit_x,
                        params=params,
                        x_axis=x_axis,
                    ) or exit_raw_replay_suppression
                if int(point.x_value) >= sell_submit_x and _limit_replay_should_attempt(replay_role, point, sell_submit_x, sell_crossed):
                    order_index += 1
                    order_id = next_order_id(order_index)
                    exit_fill = _limit_replay_fill(
                        params,
                        point,
                        "SELL_YES",
                        limit_price=sell_limit,
                        decision_x=sell_order_signal_x,
                        signal_index=index + 1,
                        submit_x=sell_submit_x,
                        target_size=open_position.size,
                        x_axis=x_axis,
                        replay_events=replay_events,
                    )
                    trade_id = f"T-{open_position.trade_index:04d}"
                    if _should_continue_resting_exit(exit_fill, params, replay_role):
                        pending_exit = {
                            "order_id": order_id,
                            "trade_id": trade_id,
                            "signal_index": index + 1,
                            "signal_x": sell_order_signal_x,
                            "submit_x": sell_submit_x,
                            "fill": _mark_resting_limit_fill(exit_fill, x_value=int(point.x_value)),
                            "last_x": int(point.x_value),
                            "last_sequence": _last_consumed_event_sequence(exit_fill),
                        }
                        sell_order_emitted = True
                        events.append(_event("sell_partial_fill", x_axis, point.x_value, trade_id, point.price, "resting sell order partially filled", meta=exit_fill))
                        continue
                    order = order_from_fill(
                        order_id=order_id,
                        signal_index=index + 1,
                        x_axis=x_axis,
                        x_value=sell_order_signal_x,
                        side="SELL_YES",
                        role=replay_role,
                        order_type=_limit_replay_order_type(params),
                        decision_price=sell_limit,
                        fill=exit_fill,
                        trade_id=trade_id,
                        latency_seconds=params.latency_seconds,
                        latency_blocks=params.latency_blocks,
                        submit_x_override=sell_submit_x,
                    )
                    _attach_markout_to_order(order, points, index)
                    _attach_missed_opportunity_to_order(order, points, index)
                    orders.append(order)
                    sell_order_emitted = True
                    if exit_fill.get("rejected") or Decimal(str(exit_fill.get("size") or 0)) <= 0:
                        events.append(_event("sell_no_fill", x_axis, point.x_value, trade_id, point.price, "sell limit crossed but volume cap prevented fill", meta=exit_fill))
                        continue
                    trade = _close_trade(run, x_axis, open_position, point, index, "limit_exit", params, exit_fill=exit_fill, exit_order_id=order_id)
                    trades.append(trade)
                    equity += trade["pnl"]
                    events.append(_event("close", x_axis, point.x_value, trade_id, point.price, "sell limit crossed", meta=exit_fill))
                    open_position = None

        mark_equity = equity
        if open_position is not None:
            mark_equity += (point.price - open_position.entry_price) * open_position.size
        peak = max(peak, mark_equity)
        drawdown = mark_equity - peak
        equity_rows.append(
            {
                "point_index": index + 1,
                "x_axis": x_axis,
                "x_value": point.x_value,
                "equity": mark_equity,
                "drawdown": drawdown,
                "drawdown_pct": _pct(drawdown, peak),
                "cumulative_return": _pct(mark_equity - params.initial_capital, params.initial_capital),
            }
        )

    if pending_entry is not None and open_position is None:
        last = points[-1]
        pending_fill = pending_entry["fill"]
        order = order_from_fill(
            order_id=pending_entry["order_id"],
            signal_index=1,
            x_axis=x_axis,
            x_value=first.x_value,
            side="BUY_YES",
            role=replay_role,
            order_type=_limit_replay_order_type(params),
            decision_price=buy_limit,
            fill=pending_fill,
            trade_id=pending_entry["trade_id"],
            latency_seconds=params.latency_seconds,
            latency_blocks=params.latency_blocks,
            submit_x_override=buy_submit_x,
        )
        _attach_markout_to_order(order, points, len(points) - 1)
        _attach_missed_opportunity_to_order(order, points, len(points) - 1)
        orders.append(order)
        open_position = OpenPosition(
            trade_index=len(trades) + 1,
            entry_index=len(points) - 1,
            entry_x=last.x_value,
            entry_price=pending_fill.get("entry_price") or buy_limit,
            size=pending_fill["size"],
            requested_notional=pending_fill["requested_notional"],
            filled_notional=pending_fill["filled_notional"],
            fill_pct=pending_fill["fill_pct"],
            fill_status=pending_fill.get("fill_status", "FILLED"),
            book_snapshot_id=pending_fill.get("book_snapshot_id"),
            snapshot_version=pending_fill.get("snapshot_version"),
            staleness_seconds=pending_fill.get("staleness_seconds"),
            staleness_blocks=pending_fill.get("staleness_blocks"),
            avg_fill_price=pending_fill.get("avg_fill_price"),
            fill_probability=pending_fill.get("fill_probability", Decimal("100")),
            block_volume=pending_fill.get("block_volume", last.volume),
            trade_count=int(pending_fill.get("trade_count", last.trade_count) or 0),
            available_notional=pending_fill.get("available_notional", pending_fill["filled_notional"]),
            entry_order_id=pending_entry["order_id"],
            entry_fee_cost=Decimal(str(pending_fill.get("fee_cost") or 0)),
            entry_rebate=Decimal(str(pending_fill.get("rebate") or pending_fill.get("rebate_cost") or 0)),
            entry_slippage_cost=Decimal(str(pending_fill.get("slippage_cost") or 0)),
        )
        sell_order_signal_x = last.x_value
        sell_order_signal_index = len(points) - 1
        sell_limit = _sell_limit_price(params, open_position.entry_price)
        events.append(_event("open", x_axis, last.x_value, pending_entry["trade_id"], last.price, "resting buy order ended with partial fill", meta=pending_fill))
        pending_entry = None

    if pending_exit is not None and open_position is not None:
        last = points[-1]
        pending_fill = pending_exit["fill"]
        order = order_from_fill(
            order_id=pending_exit["order_id"],
            signal_index=pending_exit["signal_index"],
            x_axis=x_axis,
            x_value=pending_exit["signal_x"],
            side="SELL_YES",
            role=replay_role,
            order_type=_limit_replay_order_type(params),
            decision_price=sell_limit or Decimal(str(pending_fill.get("avg_fill_price") or 0)),
            fill=pending_fill,
            trade_id=pending_exit["trade_id"],
            latency_seconds=params.latency_seconds,
            latency_blocks=params.latency_blocks,
            submit_x_override=pending_exit["submit_x"],
        )
        _attach_markout_to_order(order, points, len(points) - 1)
        _attach_missed_opportunity_to_order(order, points, len(points) - 1)
        orders.append(order)
        closed_size = Decimal(str(pending_fill.get("size") or 0))
        if closed_size > 0:
            closed_position = _slice_open_position(open_position, closed_size)
            trade = _close_trade(run, x_axis, closed_position, last, len(points) - 1, "limit_exit", params, exit_fill=pending_fill, exit_order_id=pending_exit["order_id"])
            trades.append(trade)
            equity += trade["pnl"]
            events.append(_event("close_partial", x_axis, last.x_value, pending_exit["trade_id"], last.price, "resting sell order ended with partial fill", meta=pending_fill))
        residual_size = max(Decimal("0"), open_position.size - closed_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        if residual_size > Decimal("0.0000000001"):
            open_position = _slice_open_position(open_position, residual_size, trade_index=len(trades) + 1)
        else:
            open_position = None
        pending_exit = None
        sell_order_emitted = True

    if not buy_order_emitted:
        order_index += 1
        order = order_from_fill(
            order_id=next_order_id(order_index),
            signal_index=1,
            x_axis=x_axis,
            x_value=first.x_value,
            side="BUY_YES",
            role=replay_role,
            order_type=_limit_replay_order_type(params),
            decision_price=buy_limit,
            fill=_limit_replay_no_fill(
                params,
                entry_raw_replay_suppression.get("point") if entry_raw_replay_suppression else first,
                "BUY_YES",
                limit_price=buy_limit,
                note="buy_limit_not_crossed",
                raw_replay_context=entry_raw_replay_suppression,
            ),
            latency_seconds=params.latency_seconds,
            latency_blocks=params.latency_blocks,
            submit_x_override=buy_submit_x,
        )
        _attach_missed_opportunity_to_order(order, points, 0)
        orders.append(order)
        events.append(_event("buy_no_fill", x_axis, first.x_value, None, first.price, "buy limit never crossed"))

    if open_position is not None:
        assert sell_order_signal_x is not None
        assert sell_limit is not None
        if not sell_order_emitted:
            order_index += 1
            sell_submit_x = _effective_submit_x(sell_order_signal_x, points, sell_order_signal_index or open_position.entry_index, params, x_axis)
            order = order_from_fill(
                order_id=next_order_id(order_index),
                signal_index=open_position.entry_index + 1,
                x_axis=x_axis,
                x_value=sell_order_signal_x,
                side="SELL_YES",
                role=replay_role,
                order_type=_limit_replay_order_type(params),
                decision_price=sell_limit,
                fill=_limit_replay_no_fill(
                    params,
                    exit_raw_replay_suppression.get("point") if exit_raw_replay_suppression else points[-1],
                    "SELL_YES",
                    limit_price=sell_limit,
                    note="sell_limit_not_crossed" if sell_limit < Decimal("0.98") else "terminal_price_limit_not_fillable",
                    target_size=open_position.size,
                    raw_replay_context=exit_raw_replay_suppression,
                ),
                trade_id=f"T-{open_position.trade_index:04d}",
                latency_seconds=params.latency_seconds,
                latency_blocks=params.latency_blocks,
                submit_x_override=sell_submit_x,
            )
            _attach_missed_opportunity_to_order(order, points, open_position.entry_index)
            orders.append(order)
        settlement_value = _settlement_value(params, points)
        if settlement_value is not None:
            last = points[-1]
            order_index += 1
            order_id = next_order_id(order_index)
            settlement_fill = _settlement_fill(params, last, open_position.size, settlement_value)
            trade_id = f"T-{open_position.trade_index:04d}"
            orders.append(order_from_fill(
                order_id=order_id,
                signal_index=len(points),
                x_axis=x_axis,
                x_value=last.x_value,
                side="SELL_YES",
                role="settlement",
                order_type="settlement",
                decision_price=settlement_value,
                fill=settlement_fill,
                trade_id=trade_id,
                latency_seconds=Decimal("0"),
                latency_blocks=0,
            ))
            trade = _close_trade(run, x_axis, open_position, last, len(points) - 1, "settlement", params, exit_fill=settlement_fill, exit_order_id=order_id)
            trades.append(trade)
            equity += trade["pnl"]
            events.append(_event("settlement", x_axis, last.x_value, trade_id, settlement_value, "held to settlement payoff", meta=settlement_fill))
        else:
            events.append(_event(
                "unresolved_open",
                x_axis,
                points[-1].x_value,
                f"T-{open_position.trade_index:04d}",
                points[-1].price,
                "sell limit not crossed and settlement value unavailable",
                meta={"settlement_value": None, "position_size": _decimal_text(open_position.size)},
            ))

    ledger_rows = build_ledger_rows(
        trades,
        params.initial_capital,
        gas_cost_per_order=params.gas_cost_per_order,
        settlement_cost=params.settlement_cost,
        redeem_cost=params.redeem_cost,
        capital_cost_bps=params.capital_cost_bps,
        cashflow_events=_run_cashflow_events(run),
    )
    metrics = build_metrics(trades, equity_rows, points, params, orders=orders, ledger_rows=ledger_rows)
    return {"trades": trades, "equity": equity_rows, "metrics": metrics, "events": events, "orders": orders, "ledger": ledger_rows}


def build_metrics(
    trades: list[dict[str, Any]],
    equity_rows: list[dict[str, Any]],
    points: list[PricePoint],
    params: BacktestParameters,
    *,
    orders: list[dict[str, Any]] | None = None,
    ledger_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    account = ledger_summary(ledger_rows or [], params.initial_capital)
    net = account["realized_pnl"] if ledger_rows else sum((trade["pnl"] for trade in trades), Decimal("0"))
    settlement_pnl = sum(
        (trade["pnl"] for trade in trades if str(trade.get("exit_reason") or "").lower() == "settlement"),
        Decimal("0"),
    )
    trade_exit_pnl = net - settlement_pnl
    gross_profit = sum((trade["pnl"] for trade in trades if trade["pnl"] > 0), Decimal("0"))
    gross_loss = sum((trade["pnl"] for trade in trades if trade["pnl"] < 0), Decimal("0"))
    winners = len([trade for trade in trades if trade["pnl"] > 0])
    max_drawdown = min((row["drawdown"] for row in equity_rows), default=Decimal("0"))
    avg_trade = net / Decimal(max(1, len(trades)))
    avg_holding = sum((trade["holding_bars"] for trade in trades), 0) / max(1, len(trades))
    total_return = _pct(net, params.initial_capital)
    profit_factor = gross_profit / abs(gross_loss) if gross_loss else Decimal("0")
    fill_coverage = Decimal(len(points)) / Decimal(max(1, len(points))) * Decimal("100")
    stale_ratio = Decimal("0")
    execution_cost = sum((trade.get("execution_cost", Decimal("0")) for trade in trades), Decimal("0"))
    filled_notional = sum((Decimal(str(trade.get("filled_notional") or trade.get("notional") or 0)) for trade in trades), Decimal("0"))
    requested_notional = sum((Decimal(str(trade.get("requested_notional") or trade.get("notional") or 0)) for trade in trades), Decimal("0"))
    liquidity_fill_rate = _pct(filled_notional, requested_notional) if requested_notional else Decimal("0")
    capped_trades = len([
        trade for trade in trades
        if Decimal(str(trade.get("filled_notional") or trade.get("notional") or 0))
        < Decimal(str(trade.get("requested_notional") or params.position_size)) * Decimal("0.999")
    ])
    partial_trades = len([trade for trade in trades if str(trade.get("fill_status") or "").upper() == "PARTIAL"])
    snapshot_trades = len([trade for trade in trades if trade.get("book_snapshot_id") is not None])
    orderfilled_trades = len([trade for trade in trades if str(trade.get("execution_source") or "").lower() == "orderfilled_volume"])
    fill_probability_values = [
        Decimal(str(trade.get("fill_probability") or 0))
        for trade in trades
        if trade.get("fill_probability") is not None
    ]
    avg_fill_probability = sum(fill_probability_values, Decimal("0")) / Decimal(max(1, len(fill_probability_values)))
    stale_trade_count = len([
        trade for trade in trades
        if trade.get("staleness_seconds") is not None
        and Decimal(str(trade.get("staleness_seconds") or 0)) > Decimal(str(params.max_book_staleness_seconds))
    ])
    avg_staleness_values = [
        Decimal(str(trade.get("staleness_seconds") or 0))
        for trade in trades
        if trade.get("staleness_seconds") is not None
    ]
    avg_book_staleness = sum(avg_staleness_values, Decimal("0")) / Decimal(max(1, len(avg_staleness_values)))
    avg_notional = filled_notional / Decimal(max(1, len(trades)))
    order_counts = summarize_orders(orders or [])
    resolved_pnl = settlement_pnl
    unrealized_pnl = net - resolved_pnl
    mode = normalize_execution_price_mode(params.execution_price_mode, "ORDERFILLED")
    if mode == "DEPTH":
        mode_delta = "depth replay"
        snapshot_value = _ratio(snapshot_trades, len(trades)) * Decimal("100")
        snapshot_formatted = f"{snapshot_value:.1f}%"
        snapshot_delta = f"{snapshot_trades} / {len(trades)} trades"
        snapshot_status = "positive" if snapshot_trades == len(trades) and trades else "negative" if trades else "neutral"
        snapshot_tooltip = "Closed trades linked to persisted historical CLOB snapshots"
        stale_status = "negative" if stale_trade_count else "positive"
    elif is_prediction_l2_replay_v1_mode(mode):
        mode_delta = "Prediction L2 replay V1"
        snapshot_value = _ratio(snapshot_trades, len(trades)) * Decimal("100")
        snapshot_formatted = f"{snapshot_value:.1f}%"
        snapshot_delta = f"{snapshot_trades} / {len(trades)} trades"
        snapshot_status = "positive" if snapshot_trades == len(trades) and trades else "negative" if trades else "neutral"
        snapshot_tooltip = "Arrival-time L2 execution with condition-level YES/NO shared residual; no future OrderFilled gate"
        stale_status = "negative" if stale_trade_count else "positive"
    elif is_orderfilled_lob_mode(mode):
        mode_delta = "orderfilled + LOB"
        snapshot_value = _ratio(snapshot_trades, len(trades)) * Decimal("100")
        snapshot_formatted = f"{snapshot_value:.1f}%"
        snapshot_delta = f"{snapshot_trades} / {len(trades)} trades"
        snapshot_status = "positive" if snapshot_trades == len(trades) and trades else "negative" if trades else "neutral"
        snapshot_tooltip = "OrderFilled historical fill evidence intersected with persisted historical CLOB depth snapshots"
        stale_status = "negative" if stale_trade_count else "positive"
    elif is_orderfilled_v2_tape_mode(mode) or is_orderfilled_v3_trade_mode(mode):
        is_v3 = is_orderfilled_v3_trade_mode(mode)
        mode_delta = "Fill-only V3 expected tape" if is_v3 else "OrderFilled-only V2 tape"
        snapshot_value = Decimal("0")
        snapshot_formatted = "not required"
        snapshot_delta = "trade-tape evidence"
        snapshot_status = "neutral"
        snapshot_tooltip = (
            "Fill-only V3 separates source-confirmed actual fills from modeled expected fills and uses no LOB data"
            if is_v3
            else "OrderFilled-only V2 uses one-sided trade prints, same-side future flow, limit checks, price buffers, and participation capacity"
        )
        stale_status = "neutral"
    elif mode == "ORDERFILLED":
        mode_delta = "historical fills"
        snapshot_value = Decimal("0")
        snapshot_formatted = "not required"
        snapshot_delta = f"{orderfilled_trades} orderfilled trades"
        snapshot_status = "neutral"
        snapshot_tooltip = "OrderFilled execution uses historical traded volume and participation caps; CLOB snapshots are optional, not required"
        stale_status = "neutral"
    elif is_orderfilled_cross_mode(mode):
        mode_delta = "orderfilled cross"
        snapshot_value = Decimal("0")
        snapshot_formatted = "not required"
        snapshot_delta = "crossing price required"
        snapshot_status = "neutral"
        snapshot_tooltip = "OrderFilled limit replay only fills BUY when trade price <= limit and SELL when trade price >= limit; residual positions settle at 0/1 when known"
        stale_status = "neutral"
    else:
        mode_delta = "legacy"
        snapshot_value = Decimal("0")
        snapshot_formatted = "not required"
        snapshot_delta = "legacy mode"
        snapshot_status = "neutral"
        snapshot_tooltip = "Legacy execution does not require persisted CLOB snapshots"
        stale_status = "neutral"
    rows = [
        ("net_profit", "Net Profit", "overview", net, _money(net), _percent(total_return), _status(net), "Closed realized strategy PnL"),
        ("total_return", "Total Return", "overview", total_return, _percent(total_return), "capital", _status(total_return), "Return on initial capital"),
        ("max_drawdown", "Max Drawdown", "overview", max_drawdown, _money(max_drawdown), _percent(_pct(max_drawdown, params.initial_capital)), "negative", "Largest peak-to-trough equity loss"),
        ("win_rate", "Win Rate", "overview", _ratio(winners, len(trades)) * Decimal("100"), f"{_ratio(winners, len(trades)) * Decimal('100'):.2f}%", f"{winners} / {len(trades)}", "positive" if winners else "neutral", "Percent profitable closed trades"),
        ("profit_factor", "Profit Factor", "overview", profit_factor, f"{profit_factor:.3f}", "gross P/L", "neutral", "Gross profit divided by gross loss"),
        ("total_trades", "Total Trades", "overview", Decimal(len(trades)), str(len(trades)), "closed", "neutral", "Closed strategy trades"),
        ("signal_count", "Signals", "overview", Decimal(order_counts["signal_count"]), str(order_counts["signal_count"]), "order intents", "neutral", "Entry and exit order intents generated by strategy signals"),
        ("submitted_orders", "Submitted Orders", "overview", Decimal(order_counts["submitted_count"]), str(order_counts["submitted_count"]), "lifecycle", "neutral", "Orders submitted to the simulated execution model"),
        ("no_fill_orders", "No Fill Orders", "overview", Decimal(order_counts["no_fill_count"]), str(order_counts["no_fill_count"]), "missed fills", "negative" if order_counts["no_fill_count"] else "positive", "Orders that saw a signal but did not receive executable historical flow"),
        ("avg_trade", "Avg Trade", "overview", avg_trade, _money(avg_trade), _percent(_pct(avg_trade, params.initial_capital)), _status(avg_trade), "Average closed trade PnL"),
        ("avg_holding", "Avg Holding", "overview", Decimal(str(avg_holding)), f"{avg_holding:.1f} bars", "bars", "neutral", "Average bars held per trade"),
        ("resolved_pnl", "Resolved PnL", "prediction", resolved_pnl, _money(resolved_pnl), "settled", _status(resolved_pnl), "PnL from resolved markets"),
        ("unrealized_pnl", "Unrealized PnL", "prediction", unrealized_pnl, _money(unrealized_pnl), "pending", _status(unrealized_pnl), "Mark-to-market PnL for unresolved exposure"),
        ("settlement_pnl", "Settlement PnL", "prediction", settlement_pnl, _money(settlement_pnl), "resolution payoff", _status(settlement_pnl), "PnL attributable to final payoff"),
        ("trade_exit_pnl", "Trade Exit PnL", "prediction", trade_exit_pnl, _money(trade_exit_pnl), "matched exits", _status(trade_exit_pnl), "PnL from exits that crossed a real historical sell limit"),
        ("slippage_cost", "Execution Cost", "prediction", -execution_cost, _money(-execution_cost), f"{params.fee_bps} fee bps / {params.slippage_bps} slip bps", "negative" if execution_cost else "neutral", "Modeled fees plus entry/exit slippage"),
        ("ledger_cash_balance", "Ledger Cash", "prediction", account["cash_balance"], _money(account["cash_balance"]), "cash after fills", "neutral", "Cash balance reconstructed from BUY/SELL cashflow ledger"),
        ("ledger_realized_pnl", "Ledger Realized PnL", "prediction", account["realized_pnl"], _money(account["realized_pnl"]), f"{int(account['ledger_rows'])} ledger rows", _status(account["realized_pnl"]), "Realized PnL sourced from ledger cashflows"),
        ("ledger_fee_total", "Ledger Fees", "prediction", -account["fee_total"], _money(-account["fee_total"]), "fee attribution", "negative" if account["fee_total"] else "neutral", "Total simulated fees recorded in the ledger"),
        ("ledger_rebate_total", "Ledger Rebates", "prediction", account["rebate_total"], _money(account["rebate_total"]), "rebate attribution", "positive" if account["rebate_total"] else "neutral", "Total simulated rebates credited in the ledger"),
        ("ledger_external_cost_total", "Ledger External Costs", "prediction", -account["external_cost_total"], _money(-account["external_cost_total"]), f"gas {account['gas_cost_total']} / redeem {account['redeem_cost_total']}", "negative" if account["external_cost_total"] else "neutral", "Gas, settlement, redeem, and capital occupation costs recorded as ledger events"),
        ("ledger_capital_cost_total", "Capital Cost", "prediction", -account["capital_cost_total"], _money(-account["capital_cost_total"]), f"{params.capital_cost_bps} bps/bar", "negative" if account["capital_cost_total"] else "neutral", "Modeled capital occupation cost based on filled notional and holding bars"),
        ("execution_profile", "Execution Profile", "prediction", Decimal("0"), str(params.execution_profile), f"{params.order_role} role", "neutral", "Execution assumption profile controlling fill probability haircut, latency, and adverse slippage"),
        ("liquidity_fill_rate", "Liquidity Fill", "prediction", liquidity_fill_rate, f"{liquidity_fill_rate:.1f}%", f"{_money(filled_notional)} filled", "positive" if liquidity_fill_rate >= Decimal("99") else "negative" if capped_trades else "neutral", "Share of requested USDC notional actually filled after position, liquidity, and min-fill constraints"),
        ("capped_trades", "Capped Trades", "prediction", Decimal(capped_trades), str(capped_trades), f"{_money(avg_notional)} avg fill", "negative" if capped_trades else "positive", "Trades whose filled notional was reduced by volume/liquidity constraints"),
        ("min_fill_pct", "Min Fill", "prediction", params.min_fill_pct, f"{params.min_fill_pct:.1f}%", "entry gate", "neutral", "Entry signals below this fill percentage are rejected instead of partially filled"),
        ("max_position_notional", "Max Position", "prediction", params.max_position_notional, _money(params.max_position_notional) if params.max_position_notional > 0 else "off", "per trade", "neutral", "Maximum requested USDC notional per open position"),
        ("execution_mode", "Execution Mode", "prediction", Decimal("0"), mode, mode_delta, "neutral", "Fill model selected for this run"),
        ("avg_fill_probability", "Avg Fill Probability", "prediction", avg_fill_probability, f"{avg_fill_probability:.1f}%", "orderfilled participation" if mode == "ORDERFILLED" else "fills", "neutral", "Deterministic expected fill probability from the execution model, not a random draw"),
        ("snapshot_fill_coverage", "Snapshot Fill Coverage", "prediction", snapshot_value, snapshot_formatted, snapshot_delta, snapshot_status, snapshot_tooltip),
        ("partial_fill_count", "Partial Fills", "prediction", Decimal(partial_trades), str(partial_trades), "liquidity constrained", "negative" if partial_trades else "positive", "Closed trades where the requested order could only partially fill"),
        ("stale_book_trade_count", "Stale Book Trades", "prediction", Decimal(stale_trade_count), str(stale_trade_count), f"limit {params.max_book_staleness_seconds}s", stale_status, "Closed trades whose book staleness exceeded the configured limit"),
        ("avg_book_staleness", "Avg Book Staleness", "prediction", avg_book_staleness, f"{avg_book_staleness:.1f}s", "execution snapshots", "neutral", "Average age of the book snapshots used by closed trades"),
        ("fill_coverage", "Fill Coverage", "prediction", fill_coverage, f"{fill_coverage:.1f}%", "price rows", "positive", "Usable fill price coverage"),
        ("stale_price_ratio", "Stale Price Ratio", "prediction", stale_ratio, f"{stale_ratio:.2f}%", "exact rows", "neutral", "Share of stale/forward-filled prices"),
    ]
    return [
        {
            "metric_key": key,
            "metric_name": name,
            "metric_group": group,
            "value": value,
            "formatted_value": formatted,
            "delta": delta,
            "status": status,
            "tooltip": tooltip,
            "sort_order": index,
        }
        for index, (key, name, group, value, formatted, delta, status, tooltip) in enumerate(rows, start=1)
    ]


def build_data_quality_report(points: list[PricePoint], run: dict[str, Any]) -> dict[str, Any]:
    x_values = [int(point.x_value) for point in points]
    prices = [point.price for point in points]
    deltas = [x_values[index] - x_values[index - 1] for index in range(1, len(x_values)) if x_values[index] > x_values[index - 1]]
    sorted_deltas = sorted(deltas)
    median_delta = sorted_deltas[len(sorted_deltas) // 2] if sorted_deltas else 0
    gap_threshold = int(median_delta * 4) if median_delta else 0
    gaps = [
        {"from_x": x_values[index - 1], "to_x": x_values[index], "span": x_values[index] - x_values[index - 1]}
        for index in range(1, len(x_values))
        if gap_threshold and x_values[index] - x_values[index - 1] > gap_threshold
    ]
    jumps = [
        {
            "x": x_values[index],
            "from_price": _decimal_text(prices[index - 1]),
            "to_price": _decimal_text(prices[index]),
            "delta": _decimal_text((prices[index] - prices[index - 1]).copy_abs()),
        }
        for index in range(1, len(prices))
        if (prices[index] - prices[index - 1]).copy_abs() > Decimal("0.18")
    ]
    requested_from = run.get("from_block") if run.get("price_source") == "orderfilled_block_close" else run.get("from_ts")
    requested_to = run.get("to_block") if run.get("price_source") == "orderfilled_block_close" else run.get("to_ts")
    first_x = x_values[0] if x_values else None
    last_x = x_values[-1] if x_values else None
    requested_span = int(requested_to - requested_from) if requested_from is not None and requested_to is not None and requested_to > requested_from else None
    observed_span = int(last_x - first_x) if first_x is not None and last_x is not None and last_x >= first_x else None
    span_coverage = Decimal(str(observed_span or 0)) / Decimal(str(requested_span)) if requested_span else Decimal("1")
    status = "ready"
    caveats: list[str] = []
    if gaps:
        status = "review"
        caveats.append(f"{len(gaps)} large x-axis gaps")
    if jumps:
        status = "review"
        caveats.append(f"{len(jumps)} price jumps over 18 percentage points")
    if len(points) < 50:
        status = "review"
        caveats.append("fewer than 50 rows")
    if span_coverage < Decimal("0.75"):
        status = "review"
        caveats.append("observed span covers less than 75% of requested range")
    warning_level = "OK"
    if caveats:
        warning_level = "WARN"
    if len(points) < 10 or span_coverage < Decimal("0.50"):
        warning_level = "BAD"
    data_version = _points_data_version(points, run)
    data_access = price_access_report(run, points)
    block_bar = _block_bar_quality(points) if run.get("price_source") == "orderfilled_block_close" else {
        "required": False,
        "rows": len(points),
        "ohlc_complete_count": 0,
        "ohlc_complete_pct": "0.0000",
        "vwap_available_count": 0,
        "vwap_available_pct": "0.0000",
        "invalid_range_count": 0,
        "close_outside_range_count": 0,
    }
    if block_bar.get("required") and block_bar.get("invalid_range_count"):
        status = "review"
        caveats.append(f"{block_bar['invalid_range_count']} block bars have high below low")
        warning_level = "WARN" if warning_level == "OK" else warning_level
    if block_bar.get("required") and block_bar.get("close_outside_range_count"):
        status = "review"
        caveats.append(f"{block_bar['close_outside_range_count']} closes outside high/low range")
        warning_level = "WARN" if warning_level == "OK" else warning_level
    return {
        "status": status,
        "warning_level": warning_level,
        "price_source": run.get("price_source"),
        "x_axis": "block_number" if run.get("price_source") == "orderfilled_block_close" else "timestamp",
        "data_version": data_version,
        "data_access": {**data_access, "data_version": data_version},
        "source_table": data_access["source_table"],
        "access_path": data_access["access_path"],
        "index_hint": data_access["index_hint"],
        "query_guard_version": data_access["query_guard_version"],
        "checksum": data_version,
        "version_basis": "x_value:price:volume:trade_count:ohlcv",
        "block_bar": block_bar,
        "rows": len(points),
        "first_x": first_x,
        "last_x": last_x,
        "median_delta": median_delta,
        "gap_count": len(gaps),
        "gap_threshold": gap_threshold,
        "largest_gaps": gaps[:8],
        "jump_count": len(jumps),
        "largest_jumps": jumps[:8],
        "requested_from": requested_from,
        "requested_to": requested_to,
        "observed_span": observed_span,
        "requested_span": requested_span,
        "span_coverage_pct": _decimal_text((span_coverage * Decimal("100")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "caveats": caveats,
    }


def _block_bar_quality(points: list[PricePoint]) -> dict[str, Any]:
    rows = len(points)
    ohlc_complete = [
        point
        for point in points
        if point.open_price is not None
        and point.high_price is not None
        and point.low_price is not None
        and point.close_price is not None
    ]
    vwap_available = [point for point in points if point.vwap_price is not None]
    invalid_range = [
        point
        for point in ohlc_complete
        if Decimal(str(point.high_price)) < Decimal(str(point.low_price))
    ]
    close_outside_range = [
        point
        for point in ohlc_complete
        if not (Decimal(str(point.low_price)) <= Decimal(str(point.close_price)) <= Decimal(str(point.high_price)))
    ]
    return {
        "required": True,
        "rows": rows,
        "ohlc_complete_count": len(ohlc_complete),
        "ohlc_complete_pct": _decimal_text((_ratio(len(ohlc_complete), rows) * Decimal("100")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "vwap_available_count": len(vwap_available),
        "vwap_available_pct": _decimal_text((_ratio(len(vwap_available), rows) * Decimal("100")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "invalid_range_count": len(invalid_range),
        "close_outside_range_count": len(close_outside_range),
        "buy_volume": _decimal_text(sum((point.buy_volume for point in points), Decimal("0"))),
        "sell_volume": _decimal_text(sum((point.sell_volume for point in points), Decimal("0"))),
    }


def _points_data_version(points: list[PricePoint], run: dict[str, Any]) -> str:
    digest = hashlib.sha256()
    digest.update(str(run.get("market_slug") or "").encode("utf-8"))
    digest.update(b"|")
    digest.update(str(run.get("token_side") or "").encode("utf-8"))
    digest.update(b"|")
    digest.update(str(run.get("price_source") or "").encode("utf-8"))
    for point in points:
        digest.update(str(point.x_value).encode("ascii"))
        digest.update(b":")
        digest.update(_decimal_text(point.price).encode("ascii"))
        digest.update(b":")
        digest.update(_decimal_text(point.volume).encode("ascii"))
        digest.update(b":")
        digest.update(str(point.trade_count).encode("ascii"))
        digest.update(b":")
        digest.update(_decimal_text(point.open_price or point.price).encode("ascii"))
        digest.update(b":")
        digest.update(_decimal_text(point.high_price or point.price).encode("ascii"))
        digest.update(b":")
        digest.update(_decimal_text(point.low_price or point.price).encode("ascii"))
        digest.update(b":")
        digest.update(_decimal_text(point.vwap_price or Decimal("0")).encode("ascii"))
        digest.update(b";")
    return digest.hexdigest()[:20]


def data_quality_metrics(report: dict[str, Any]) -> list[dict[str, Any]]:
    status = "positive" if report.get("status") == "ready" else "negative"
    rows = [
        {
            "metric_key": "data_quality_status",
            "metric_name": "Data Quality",
            "metric_group": "prediction",
            "value": Decimal("1") if report.get("status") == "ready" else Decimal("0"),
            "formatted_value": str(report.get("warning_level") or report.get("status") or "unknown"),
            "delta": f"{report.get('rows', 0)} rows",
            "status": status,
            "tooltip": "; ".join(report.get("caveats") or []) or "No large gaps or jumps detected in the executed price rows",
            "sort_order": 90,
        },
        {
            "metric_key": "gap_count",
            "metric_name": "Gap Count",
            "metric_group": "prediction",
            "value": Decimal(int(report.get("gap_count") or 0)),
            "formatted_value": str(report.get("gap_count") or 0),
            "delta": f"threshold {report.get('gap_threshold') or 0}",
            "status": "negative" if report.get("gap_count") else "positive",
            "tooltip": "Large x-axis gaps detected in the rows used by this backtest",
            "sort_order": 91,
        },
        {
            "metric_key": "jump_count",
            "metric_name": "Jump Count",
            "metric_group": "prediction",
            "value": Decimal(int(report.get("jump_count") or 0)),
            "formatted_value": str(report.get("jump_count") or 0),
            "delta": ">18 pct points",
            "status": "negative" if report.get("jump_count") else "positive",
            "tooltip": "Large adjacent price jumps detected in the rows used by this backtest",
            "sort_order": 92,
        },
        {
            "metric_key": "data_version",
            "metric_name": "Data Version",
            "metric_group": "prediction",
            "value": Decimal("0"),
            "formatted_value": str(report.get("data_version") or "-"),
            "delta": str(report.get("version_basis") or "price rows"),
            "status": "neutral",
            "tooltip": "Stable checksum of the exact x/price/volume rows used by this backtest run",
            "sort_order": 93,
        },
        {
            "metric_key": "data_access_path",
            "metric_name": "Data Access Path",
            "metric_group": "system",
            "value": Decimal("0"),
            "formatted_value": str(report.get("access_path") or "-"),
            "delta": str(report.get("source_table") or "-"),
            "status": "neutral",
            "tooltip": f"Index hint: {report.get('index_hint') or '-'}; guard: {report.get('query_guard_version') or '-'}",
            "sort_order": 94,
        },
        {
            "metric_key": "span_coverage",
            "metric_name": "Span Coverage",
            "metric_group": "prediction",
            "value": Decimal(str(report.get("span_coverage_pct") or "0")),
            "formatted_value": f"{report.get('span_coverage_pct') or '0'}%",
            "delta": f"{report.get('first_x') or '-'} -> {report.get('last_x') or '-'}",
            "status": "positive" if Decimal(str(report.get("span_coverage_pct") or "0")) >= Decimal("75") else "negative",
            "tooltip": "Observed row span compared with the requested backtest range",
            "sort_order": 95,
        },
    ]
    block_bar = report.get("block_bar") or {}
    if block_bar.get("required"):
        ohlc_pct = Decimal(str(block_bar.get("ohlc_complete_pct") or "0"))
        rows.append(
            {
                "metric_key": "block_bar_coverage",
                "metric_name": "Block Bar Coverage",
                "metric_group": "prediction",
                "value": ohlc_pct,
                "formatted_value": f"{ohlc_pct}%",
                "delta": f"{block_bar.get('ohlc_complete_count', 0)} / {block_bar.get('rows', 0)} OHLC rows",
                "status": "positive" if ohlc_pct >= Decimal("99") and not block_bar.get("invalid_range_count") and not block_bar.get("close_outside_range_count") else "negative",
                "tooltip": (
                    f"vwap={block_bar.get('vwap_available_pct', '0')}%, "
                    f"invalid_range={block_bar.get('invalid_range_count', 0)}, "
                    f"close_outside_range={block_bar.get('close_outside_range_count', 0)}"
                ),
                "sort_order": 96,
            }
        )
    lob_coverage = report.get("lob_execution_coverage") or {}
    if lob_coverage.get("required"):
        coverage_pct = Decimal(str(lob_coverage.get("coverage_pct") or "0"))
        rows.append(
            {
                "metric_key": "lob_execution_coverage",
                "metric_name": "LOB Coverage",
                "metric_group": "prediction",
                "value": coverage_pct,
                "formatted_value": f"{coverage_pct}%",
                "delta": f"{lob_coverage.get('covered_order_count', 0)} / {lob_coverage.get('order_count', 0)} orders",
                "status": "positive" if lob_coverage.get("status") == "ready" else "negative",
                "tooltip": f"no_book={lob_coverage.get('no_book_count', 0)}, stale={lob_coverage.get('stale_book_count', 0)}, snapshots={lob_coverage.get('snapshot_count', 0)}",
                "sort_order": 97,
            }
        )
    return rows


def build_fill_quality_report(
    orders: list[dict[str, Any]],
    *,
    replay_context: dict[str, Any] | None = None,
    data_quality_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    submitted = len(orders)
    filled_orders = [row for row in orders if Decimal(str(row.get("filled_size") or 0)) > 0]
    partial_orders = [row for row in orders if str(row.get("status") or "").upper() == "PARTIAL_FILLED"]
    no_fill_orders = [row for row in orders if str(row.get("status") or "").upper() == "NO_FILL"]
    rejected_orders = [row for row in orders if str(row.get("status") or "").upper() == "REJECTED"]
    expired_orders = [row for row in orders if str(row.get("status") or "").upper() == "EXPIRED"]

    no_fill_reasons: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    side_counts: dict[str, int] = {}
    role_counts: dict[str, int] = {}
    order_type_counts: dict[str, int] = {}
    time_in_force_counts: dict[str, int] = {}
    execution_evidence_counts: dict[str, int] = {}
    fill_evidence_counts: dict[str, int] = {}
    block_bar_execution_models: dict[str, int] = {}
    fill_probability_model_counts: dict[str, int] = {}
    side_compatibility_counts: dict[str, int] = {}
    fill_schedule_tick_count = 0
    fill_schedule_fillable_size = Decimal("0")
    side_discounted_tick_count = 0
    block_participation_discount_tick_count = 0
    max_requested_block_participation_pct = Decimal("0")
    min_block_participation_factor = Decimal("1")
    resting_order_continued_count = 0
    raw_orderfilled_fill_count = 0
    block_bar_synthetic_fill_count = 0
    raw_replay_fallback_suppressed_count = 0
    raw_replay_synthetic_cross_no_fill_count = 0
    raw_candidate_orders = 0
    fallback_candidate_orders = 0
    candidate_event_count = 0
    candidate_events_with_counterparty = 0
    candidate_event_keys: list[str] = []
    candidate_volume = Decimal("0")
    candidate_notional = Decimal("0")
    candidate_unique_notional_by_key: dict[str, Decimal] = {}
    candidate_keyless_count = 0
    consumed_event_count = 0
    consumed_event_keys: list[str] = []
    consumed_volume = Decimal("0")
    consumed_notional = Decimal("0")
    consumed_unique_notional_by_key: dict[str, Decimal] = {}
    consumed_keyless_count = 0
    available_notional_total = Decimal("0")
    requested_notional = Decimal("0")
    filled_notional = Decimal("0")
    expected_fill_size_total = Decimal("0")
    actual_fill_size_total = Decimal("0")
    expected_fill_notional_total = Decimal("0")
    actual_fill_notional_total = Decimal("0")
    fee_total = Decimal("0")
    rebate_total = Decimal("0")
    slippage_total = Decimal("0")
    execution_cost_total = Decimal("0")
    latency_blocks: list[Decimal] = []
    latency_seconds: list[Decimal] = []
    effective_latency_x_spans: list[Decimal] = []
    fill_prices: list[Decimal] = []
    participation_rates: list[Decimal] = []
    effective_liquidity_caps: list[Decimal] = []
    adverse_slippage_values: list[Decimal] = []
    fill_haircut_values: list[Decimal] = []
    queue_adjusted_count = 0
    queue_fill_factors: list[Decimal] = []
    queue_ahead_sizes: list[Decimal] = []
    markouts: dict[str, list[Decimal]] = {str(horizon): [] for horizon in MARKOUT_BAR_HORIZONS}
    markout_seconds: dict[str, list[Decimal]] = {str(horizon): [] for horizon in MARKOUT_SECOND_HORIZONS}
    adverse_selection_buckets: dict[str, int] = {}
    missed_opportunity_count = 0
    missed_opportunity_notional_total = Decimal("0")
    missed_opportunity_price_moves: list[Decimal] = []
    missed_opportunity_notionals: list[Decimal] = []
    missed_opportunity_by_reason: dict[str, Decimal] = {}
    missed_opportunity_buckets: dict[str, int] = {}
    missed_opportunity_notional_by_bucket: dict[str, Decimal] = {}
    order_anomaly_flags: dict[str, int] = {}
    environment_flags: dict[str, int] = {}
    real_order_state_counts: dict[str, int] = {}
    real_order_state_flags: dict[str, int] = {}
    real_order_state_observed_count = 0
    submit_accept_latency_seconds: list[Decimal] = []
    cancel_accept_latency_seconds: list[Decimal] = []
    anomaly_order_ids: set[str] = set()
    consumed_event_orders: dict[str, set[str]] = {}

    for row in orders:
        order_flags: set[str] = set()
        status = str(row.get("status") or "UNKNOWN").upper()
        source = str(row.get("execution_source") or "unknown")
        side = str(row.get("side") or "unknown")
        role = str(row.get("role") or "unknown")
        order_type = str(row.get("order_type") or "unknown")
        order_id = str(row.get("order_id") or row.get("id") or "")
        requested_size = Decimal(str(row.get("requested_size") or 0))
        filled_size = Decimal(str(row.get("filled_size") or 0))
        unfilled_size = Decimal(str(row.get("unfilled_size") or 0))
        row_requested_notional = Decimal(str(row.get("requested_notional") or 0))
        row_filled_notional = Decimal(str(row.get("filled_notional") or 0))
        row_expected_fill_size = Decimal(
            str(row["expected_fill_size"] if "expected_fill_size" in row else filled_size)
        )
        row_actual_fill_size = Decimal(
            str(row["actual_fill_size"] if "actual_fill_size" in row else filled_size)
        )
        row_expected_fill_notional = Decimal(
            str(
                row["expected_fill_notional"]
                if "expected_fill_notional" in row
                else row_filled_notional
            )
        )
        row_actual_fill_notional = Decimal(
            str(
                row["actual_fill_notional"]
                if "actual_fill_notional" in row
                else row_filled_notional
            )
        )
        row_fee = Decimal(str(row.get("fee_cost") or 0))
        row_rebate = Decimal(str(row.get("rebate") or row.get("rebate_cost") or 0))
        row_slippage = Decimal(str(row.get("slippage_cost") or 0))
        row_execution_cost = Decimal(str(row.get("execution_cost") or 0))
        status_counts[status] = status_counts.get(status, 0) + 1
        source_counts[source] = source_counts.get(source, 0) + 1
        side_counts[side] = side_counts.get(side, 0) + 1
        role_counts[role] = role_counts.get(role, 0) + 1
        order_type_counts[order_type] = order_type_counts.get(order_type, 0) + 1
        requested_notional += row_requested_notional
        filled_notional += row_filled_notional
        expected_fill_size_total += row_expected_fill_size
        actual_fill_size_total += row_actual_fill_size
        expected_fill_notional_total += row_expected_fill_notional
        actual_fill_notional_total += row_actual_fill_notional
        available_notional_total += Decimal(str(row.get("available_notional") or 0))
        fee_total += row_fee
        rebate_total += row_rebate
        slippage_total += row_slippage
        execution_cost_total += row_execution_cost
        latency_blocks.append(Decimal(str(row.get("latency_blocks") or 0)))
        latency_seconds.append(Decimal(str(row.get("latency_seconds") or 0)))
        if row.get("signal_x") is not None and row.get("submit_x") is not None:
            effective_latency_x_spans.append(max(Decimal("0"), Decimal(str(row.get("submit_x"))) - Decimal(str(row.get("signal_x")))))
        if row.get("avg_fill_price") is not None and filled_size > 0:
            fill_prices.append(Decimal(str(row.get("avg_fill_price") or 0)))
        if row.get("participation_rate") is not None:
            participation_rates.append(Decimal(str(row.get("participation_rate") or 0)))
        meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
        evidence_type = str(row.get("execution_evidence_type") or meta.get("execution_evidence_type") or execution_evidence_type(row))
        execution_evidence_counts[evidence_type] = execution_evidence_counts.get(evidence_type, 0) + 1
        if filled_size > 0:
            fill_evidence_counts[evidence_type] = fill_evidence_counts.get(evidence_type, 0) + 1
            if evidence_type == "raw_orderfilled":
                raw_orderfilled_fill_count += 1
            elif evidence_type == "block_bar_ohlcv_fallback":
                block_bar_synthetic_fill_count += 1
        if meta.get("effective_liquidity_cap_pct") is not None:
            effective_liquidity_caps.append(Decimal(str(meta.get("effective_liquidity_cap_pct") or 0)))
        if meta.get("adverse_slippage_cents") is not None:
            adverse_slippage_values.append(Decimal(str(meta.get("adverse_slippage_cents") or 0)))
        if meta.get("fill_probability_haircut_pct") is not None:
            fill_haircut_values.append(Decimal(str(meta.get("fill_probability_haircut_pct") or 0)))
        if meta.get("fill_probability_model"):
            model = str(meta.get("fill_probability_model"))
            fill_probability_model_counts[model] = fill_probability_model_counts.get(model, 0) + 1
        fill_schedule = meta.get("fill_schedule") if isinstance(meta, dict) else None
        if isinstance(fill_schedule, list):
            fill_schedule_tick_count += len(fill_schedule)
            for item in fill_schedule:
                if isinstance(item, dict):
                    fill_schedule_fillable_size += Decimal(str(item.get("fillable_size") or 0))
                    compatibility = str(item.get("side_compatibility") or "unknown")
                    side_compatibility_counts[compatibility] = side_compatibility_counts.get(compatibility, 0) + 1
                    if Decimal(str(item.get("side_compatibility_factor") or 1)) < Decimal("1"):
                        side_discounted_tick_count += 1
                    participation_factor = Decimal(str(item.get("block_participation_factor") or 1))
                    if participation_factor < Decimal("1"):
                        block_participation_discount_tick_count += 1
                        min_block_participation_factor = min(min_block_participation_factor, participation_factor)
                        max_requested_block_participation_pct = max(
                            max_requested_block_participation_pct,
                            Decimal(str(item.get("requested_block_participation_pct") or 0)),
                        )
        if meta.get("block_bar_execution_model"):
            model = str(meta.get("block_bar_execution_model"))
            block_bar_execution_models[model] = block_bar_execution_models.get(model, 0) + 1
        if bool(meta.get("resting_order_continued") or row.get("resting_order_continued")):
            resting_order_continued_count += 1
        queue_adjusted = bool(row.get("queue_adjusted") or meta.get("queue_adjusted"))
        queue_fill_factor = row.get("queue_fill_factor") if row.get("queue_fill_factor") is not None else meta.get("queue_fill_factor")
        queue_ahead_size = row.get("queue_ahead_size") if row.get("queue_ahead_size") is not None else meta.get("queue_ahead_size")
        if queue_fill_factor is not None:
            queue_fill_factors.append(Decimal(str(queue_fill_factor or 0)))
        if queue_ahead_size is not None:
            queue_ahead_sizes.append(Decimal(str(queue_ahead_size or 0)))
        if queue_adjusted:
            queue_adjusted_count += 1
            order_flags.add("maker_queue_adjusted")
        if filled_size < 0 or row_filled_notional < 0:
            order_flags.add("negative_fill")
        if unfilled_size < 0:
            order_flags.add("negative_unfilled_size")
        if row_fee < 0:
            order_flags.add("negative_fee")
        if row_rebate < 0:
            order_flags.add("negative_rebate")
        if status in {"FILLED", "PARTIAL_FILLED"} and filled_size <= 0:
            order_flags.add("filled_status_zero_size")
        if status in {"NO_FILL", "REJECTED", "EXPIRED", "CANCELED", "CANCEL_FAILED"} and filled_size > 0:
            order_flags.add("non_fill_status_positive_size")
        if requested_size > 0 and filled_size > requested_size + Decimal("0.0000000001"):
            order_flags.add("overfilled_size")
        expected_is_observed = bool(
            row.get(
                "expected_fill_is_observed_execution",
                meta.get("expected_fill_is_observed_execution", True),
            )
        )
        if expected_is_observed and row_actual_fill_size != filled_size:
            order_flags.add("actual_fill_size_mismatch")
        if expected_is_observed and row_actual_fill_notional != row_filled_notional:
            order_flags.add("actual_fill_notional_mismatch")
        if row_expected_fill_size < row_actual_fill_size:
            order_flags.add("actual_fill_exceeds_expected_size")
        if row_expected_fill_notional < row_actual_fill_notional:
            order_flags.add("actual_fill_exceeds_expected_notional")
        if row_requested_notional > 0 and row_filled_notional > row_requested_notional + Decimal("0.0000000001"):
            order_flags.add("overfilled_notional")
        if row.get("expected_fill_size") is None or row.get("actual_fill_size") is None or row.get("participation_rate") is None:
            order_flags.add("missing_fill_expectation_fields")
        if Decimal(str(row.get("fill_pct") or 0)) > Decimal("100.0001"):
            order_flags.add("fill_pct_over_100")
        if "synthetic" in source and filled_size > 0:
            order_flags.add("synthetic_fill_fallback")
            environment_flags["synthetic_execution_used"] = environment_flags.get("synthetic_execution_used", 0) + 1
        if bool(meta.get("block_bar_fallback_suppressed_by_raw_replay")):
            raw_replay_fallback_suppressed_count += 1
            order_flags.add("raw_replay_suppressed_block_bar_fallback")
            environment_flags["raw_replay_suppressed_block_bar_fallback"] = environment_flags.get("raw_replay_suppressed_block_bar_fallback", 0) + 1
        if bool(meta.get("synthetic_block_bar_crossed_without_raw_fill")):
            raw_replay_synthetic_cross_no_fill_count += 1
            order_flags.add("synthetic_block_bar_crossed_without_raw_fill")
        if status in {"CANCELED", "CANCEL_FAILED"}:
            order_flags.add("cancel_status_observed")
        markout_after_bars = meta.get("markout_after_bars") if isinstance(meta, dict) else None
        if isinstance(markout_after_bars, dict):
            for horizon in markouts:
                if markout_after_bars.get(horizon) is not None:
                    value = Decimal(str(markout_after_bars.get(horizon)))
                    markouts[horizon].append(value)
                    bucket = _markout_bucket(value)
                    key = f"bars_{horizon}_{bucket}"
                    adverse_selection_buckets[key] = adverse_selection_buckets.get(key, 0) + 1
        markout_after_seconds = meta.get("markout_after_seconds") if isinstance(meta, dict) else None
        if isinstance(markout_after_seconds, dict):
            for horizon in markout_seconds:
                if markout_after_seconds.get(horizon) is not None:
                    value = Decimal(str(markout_after_seconds.get(horizon)))
                    markout_seconds[horizon].append(value)
                    bucket = _markout_bucket(value)
                    key = f"seconds_{horizon}_{bucket}"
                    adverse_selection_buckets[key] = adverse_selection_buckets.get(key, 0) + 1
        tif = meta.get("time_in_force") if isinstance(meta, dict) else None
        if tif:
            tif_text = str(tif)
            time_in_force_counts[tif_text] = time_in_force_counts.get(tif_text, 0) + 1
        missed_opportunity = meta.get("missed_opportunity") if isinstance(meta, dict) else None
        if status in {"NO_FILL", "REJECTED", "EXPIRED"} and isinstance(missed_opportunity, dict):
            missed_notional = Decimal(str(missed_opportunity.get("missed_notional") or 0))
            missed_price_move = Decimal(str(missed_opportunity.get("missed_price_move") or 0))
            if missed_notional > 0:
                missed_opportunity_count += 1
                missed_opportunity_notional_total += missed_notional
                missed_opportunity_notionals.append(missed_notional)
                missed_opportunity_price_moves.append(missed_price_move)
                reason = str(row.get("no_fill_reason") or status.lower() or "unknown")
                missed_opportunity_by_reason[reason] = missed_opportunity_by_reason.get(reason, Decimal("0")) + missed_notional
                for bucket in _missed_opportunity_bucket_keys(row, missed_opportunity, reason):
                    missed_opportunity_buckets[bucket] = missed_opportunity_buckets.get(bucket, 0) + 1
                    missed_opportunity_notional_by_bucket[bucket] = missed_opportunity_notional_by_bucket.get(bucket, Decimal("0")) + missed_notional
        candidate_events = meta.get("candidate_events") if isinstance(meta, dict) else None
        if isinstance(candidate_events, list):
            candidate_event_count += len(candidate_events)
            if candidate_events:
                raw_candidate_orders += 1
            for event in candidate_events:
                if isinstance(event, dict):
                    candidate_volume += Decimal(str(event.get("size") or 0))
                    event_notional = _fill_event_notional(event)
                    candidate_notional += event_notional
                    if event.get("maker") or event.get("taker"):
                        candidate_events_with_counterparty += 1
                    event_key = _fill_event_key(event)
                    if event_key:
                        candidate_event_keys.append(event_key)
                        candidate_unique_notional_by_key.setdefault(event_key, event_notional)
                    else:
                        candidate_keyless_count += 1
        consumed_events = meta.get("consumed_events") if isinstance(meta, dict) else None
        if isinstance(consumed_events, list):
            consumed_event_count += len(consumed_events)
            for event in consumed_events:
                if isinstance(event, dict):
                    consumed_volume += Decimal(str(event.get("size") or 0))
                    event_notional = _fill_event_notional(event)
                    consumed_notional += event_notional
                    event_key = _fill_event_key(event)
                    if event_key:
                        consumed_event_keys.append(event_key)
                        consumed_unique_notional_by_key.setdefault(event_key, event_notional)
                        consumed_event_orders.setdefault(event_key, set()).add(order_id or f"order-{len(consumed_event_orders)}")
                    else:
                        consumed_keyless_count += 1
        if filled_size > 0 and "orderfilled_limit_replay_raw" in source and not consumed_events:
            order_flags.add("filled_without_consumed_raw_event")
        if status in {"NO_FILL", "REJECTED", "EXPIRED"} and isinstance(candidate_events, list) and candidate_events:
            order_flags.add("candidate_events_but_no_fill")
        if "synthetic" in source:
            fallback_candidate_orders += 1
        if status in {"NO_FILL", "REJECTED", "EXPIRED"}:
            reason = str(row.get("no_fill_reason") or status.lower() or "unknown")
            no_fill_reasons[reason] = no_fill_reasons.get(reason, 0) + 1
        for note in _order_notes(meta):
            note_text = note.lower()
            if "cancel" in note_text:
                order_flags.add("cancel_note_observed")
            if "ghost" in note_text:
                order_flags.add("ghost_fill_note_observed")
            if "order_status" in note_text or "status_mismatch" in note_text:
                order_flags.add("order_status_mismatch_note")
        state_audit = _order_state_audit(row, meta)
        if state_audit["observed"]:
            real_order_state_observed_count += 1
        for key, value in state_audit["counts"].items():
            real_order_state_counts[key] = real_order_state_counts.get(key, 0) + int(value)
        for key, value in state_audit["flags"].items():
            real_order_state_flags[key] = real_order_state_flags.get(key, 0) + int(value)
            if key in {
                "submit_rejected",
                "post_only_rejected",
                "cancel_failed",
                "cancel_race",
                "ghost_fill_observed",
                "api_status_lagging_after_chain_fill",
                "service_not_ready_425",
            }:
                order_flags.add(f"real_order_state_{key}")
        submit_latency = state_audit.get("submit_accept_latency_seconds")
        if isinstance(submit_latency, Decimal):
            submit_accept_latency_seconds.append(submit_latency)
        cancel_latency = state_audit.get("cancel_accept_latency_seconds")
        if isinstance(cancel_latency, Decimal):
            cancel_accept_latency_seconds.append(cancel_latency)
        if order_flags:
            anomaly_order_ids.add(order_id or f"index-{len(anomaly_order_ids)}")
            for flag in order_flags:
                order_anomaly_flags[flag] = order_anomaly_flags.get(flag, 0) + 1

    reused_consumed_event_count = 0
    for order_ids in consumed_event_orders.values():
        if len(order_ids) > 1:
            reused_consumed_event_count += 1
            order_anomaly_flags["reused_consumed_fill_event"] = order_anomaly_flags.get("reused_consumed_fill_event", 0) + 1
            anomaly_order_ids.update(order_ids)

    replay = replay_context or {}
    raw_event_count = int(replay.get("event_count") or 0)
    loaded_raw_event_count = int(replay.get("loaded_event_count") or raw_event_count)
    deduped_raw_event_count = int(replay.get("deduped_event_count") or raw_event_count)
    raw_duplicate_event_count = int(replay.get("duplicate_event_count") or 0)
    duplicate_classified = "exact_duplicate_event_count" in replay or "conflicting_duplicate_event_count" in replay
    raw_exact_duplicate_event_count = int(replay.get("exact_duplicate_event_count") or 0)
    raw_conflicting_duplicate_event_count = (
        int(replay.get("conflicting_duplicate_event_count") or 0)
        if duplicate_classified
        else raw_duplicate_event_count
    )
    replay_loaded_window = replay.get("loaded_block_window") if isinstance(replay.get("loaded_block_window"), dict) else {}
    raw_duplicate_group_count = int(
        replay.get("duplicate_group_count")
        or replay_loaded_window.get("duplicate_group_count")
        or 0
    )
    raw_conflicting_duplicate_group_count = int(
        replay.get("conflicting_duplicate_group_count")
        or replay_loaded_window.get("conflicting_duplicate_group_count")
        or 0
    )
    raw_trade_tick_report = replay.get("raw_trade_tick_report") if isinstance(replay.get("raw_trade_tick_report"), dict) else {}
    fallback = replay.get("fallback")
    raw_enabled = replay.get("enabled") is True and not fallback and raw_event_count > 0
    if replay.get("enabled") is not True:
        environment_flags["raw_replay_disabled"] = environment_flags.get("raw_replay_disabled", 0) + 1
    if fallback:
        flag = f"raw_replay_fallback_{_flag_key(fallback)}"
        environment_flags[flag] = environment_flags.get(flag, 0) + 1
    if replay.get("enabled") is True and raw_event_count <= 0:
        environment_flags["raw_replay_empty"] = environment_flags.get("raw_replay_empty", 0) + 1
    if replay.get("warning"):
        environment_flags["raw_replay_warning"] = environment_flags.get("raw_replay_warning", 0) + 1
        if "hit limit" in str(replay.get("warning")).lower():
            environment_flags["raw_replay_event_limit_hit"] = environment_flags.get("raw_replay_event_limit_hit", 0) + 1
    if raw_conflicting_duplicate_event_count:
        environment_flags["raw_replay_duplicate_events"] = raw_conflicting_duplicate_event_count
        environment_flags["raw_replay_conflicting_duplicate_events"] = raw_conflicting_duplicate_event_count
    if replay.get("hit_limit") is True:
        environment_flags["raw_replay_event_limit_hit"] = environment_flags.get("raw_replay_event_limit_hit", 0) + 1
    if candidate_event_count and candidate_events_with_counterparty < candidate_event_count:
        environment_flags["missing_counterparty_tags"] = candidate_event_count - candidate_events_with_counterparty
    candidate_unique = len(set(candidate_event_keys))
    consumed_unique = len(set(consumed_event_keys))
    candidate_duplicates = max(0, len(candidate_event_keys) - candidate_unique)
    consumed_duplicates = max(0, len(consumed_event_keys) - consumed_unique)
    counterparty_tag_rate = _ratio(candidate_events_with_counterparty, candidate_event_count) * Decimal("100")
    raw_evidence_summary = {
        "canonical_key_fields": list(CANONICAL_FILL_KEY_FIELDS),
        "candidate_event_count": candidate_event_count,
        "candidate_event_keyed_count": len(candidate_event_keys),
        "candidate_event_keyless_count": candidate_keyless_count,
        "candidate_event_unique_count": candidate_unique,
        "candidate_event_duplicate_count": candidate_duplicates,
        "candidate_events_with_counterparty": candidate_events_with_counterparty,
        "candidate_events_missing_counterparty": max(0, candidate_event_count - candidate_events_with_counterparty),
        "candidate_size": _decimal_text(candidate_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "candidate_notional": _decimal_text(candidate_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "candidate_unique_notional": _decimal_text(sum(candidate_unique_notional_by_key.values(), Decimal("0")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "consumed_event_count": consumed_event_count,
        "consumed_event_keyed_count": len(consumed_event_keys),
        "consumed_event_keyless_count": consumed_keyless_count,
        "consumed_event_unique_count": consumed_unique,
        "consumed_event_duplicate_count": consumed_duplicates,
        "consumed_size": _decimal_text(consumed_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "consumed_notional": _decimal_text(consumed_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "consumed_unique_notional": _decimal_text(sum(consumed_unique_notional_by_key.values(), Decimal("0")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "reused_consumed_event_count": reused_consumed_event_count,
        "counterparty_tag_rate": _decimal_text(counterparty_tag_rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "raw_candidate_orders": raw_candidate_orders,
        "fallback_candidate_orders": fallback_candidate_orders,
        "execution_evidence_counts": dict(sorted(execution_evidence_counts.items())),
        "fill_evidence_counts": dict(sorted(fill_evidence_counts.items())),
        "block_bar_execution_models": dict(sorted(block_bar_execution_models.items())),
        "fill_probability_model_counts": dict(sorted(fill_probability_model_counts.items())),
        "side_compatibility_counts": dict(sorted(side_compatibility_counts.items())),
        "side_discounted_tick_count": side_discounted_tick_count,
        "block_participation_discount_tick_count": block_participation_discount_tick_count,
        "max_requested_block_participation_pct": _decimal_text(max_requested_block_participation_pct.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "min_block_participation_factor": _decimal_text(min_block_participation_factor.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)) if block_participation_discount_tick_count else "1",
        "fill_schedule_tick_count": fill_schedule_tick_count,
        "fill_schedule_fillable_size": _decimal_text(fill_schedule_fillable_size.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "resting_order_continued_count": resting_order_continued_count,
        "raw_orderfilled_fill_count": raw_orderfilled_fill_count,
        "block_bar_synthetic_fill_count": block_bar_synthetic_fill_count,
        "raw_replay_fallback_suppressed_count": raw_replay_fallback_suppressed_count,
        "raw_replay_synthetic_cross_no_fill_count": raw_replay_synthetic_cross_no_fill_count,
        "raw_replay_coverage_pct": _decimal_text((_ratio(raw_orderfilled_fill_count, len(filled_orders)) * Decimal("100")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "block_bar_fallback_pct": _decimal_text((_ratio(block_bar_synthetic_fill_count, len(filled_orders)) * Decimal("100")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "loaded_raw_event_count": loaded_raw_event_count,
        "deduped_raw_event_count": deduped_raw_event_count,
        "raw_duplicate_event_count": raw_duplicate_event_count,
        "raw_exact_duplicate_event_count": raw_exact_duplicate_event_count,
        "raw_conflicting_duplicate_event_count": raw_conflicting_duplicate_event_count,
        "raw_duplicate_group_count": raw_duplicate_group_count,
        "raw_conflicting_duplicate_group_count": raw_conflicting_duplicate_group_count,
        "raw_duplicate_classification": "classified" if duplicate_classified else "legacy_unclassified",
        "raw_canonical_event_count": int(replay.get("canonical_event_count") or 0),
        "raw_fallback_key_event_count": int(replay.get("fallback_key_event_count") or 0),
        "raw_unknown_key_event_count": int(replay.get("unknown_key_event_count") or 0),
        "raw_canonical_fill_key_coverage_pct": str(replay.get("raw_canonical_fill_key_coverage_pct") or raw_trade_tick_report.get("canonical_fill_key_coverage_pct") or "0"),
        "raw_maker_taker_side_coverage_pct": str(replay.get("raw_maker_taker_side_coverage_pct") or raw_trade_tick_report.get("maker_taker_side_coverage_pct") or "0"),
        "raw_trade_tick_count": int(replay.get("raw_trade_tick_count") or raw_trade_tick_report.get("trade_tick_count") or 0),
        "raw_block_count": int(replay.get("raw_block_count") or raw_trade_tick_report.get("block_count") or 0),
        "raw_trade_tick_report": raw_trade_tick_report,
        "strategy_requested_notional": _decimal_text(requested_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "strategy_available_notional": _decimal_text(available_notional_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "strategy_filled_notional": _decimal_text(filled_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
    }
    data_report = data_quality_report or {}
    if data_report.get("status") and data_report.get("status") != "ready":
        environment_flags[f"data_quality_{_flag_key(data_report.get('status'))}"] = 1
    if data_report.get("warning_level") and data_report.get("warning_level") != "OK":
        environment_flags[f"data_warning_{_flag_key(data_report.get('warning_level'))}"] = 1
    if int(data_report.get("gap_count") or 0) > 0:
        environment_flags["price_gap_detected"] = int(data_report.get("gap_count") or 0)
    if int(data_report.get("jump_count") or 0) > 0:
        environment_flags["price_jump_detected"] = int(data_report.get("jump_count") or 0)
    if Decimal(str(data_report.get("span_coverage_pct") or "100")) < Decimal("75"):
        environment_flags["low_span_coverage"] = 1
    real_order_state_report = data_report.get("real_order_state") if isinstance(data_report.get("real_order_state"), dict) else {}
    state_event_count = int(real_order_state_report.get("event_count") or 0)
    state_attached_count = int(real_order_state_report.get("attached_order_count") or 0)
    if state_event_count > state_attached_count:
        environment_flags["unmatched_real_order_state_events"] = state_event_count - state_attached_count
    incident_report = data_report.get("platform_incidents") if isinstance(data_report.get("platform_incidents"), dict) else {}
    for flag, value in (incident_report.get("environment_flags") or {}).items():
        environment_flags[str(flag)] = environment_flags.get(str(flag), 0) + int(value or 0)
    fill_rate = _ratio(len(filled_orders), submitted) * Decimal("100")
    no_fill_rate = _ratio(len(no_fill_orders), submitted) * Decimal("100")
    partial_fill_rate = _ratio(len(partial_orders), submitted) * Decimal("100")
    filled_notional_rate = _pct(filled_notional, requested_notional) if requested_notional else Decimal("0")
    return {
        "signal_count": submitted,
        "submitted_count": submitted,
        "filled_count": len(filled_orders),
        "partial_fill_count": len(partial_orders),
        "no_fill_count": len(no_fill_orders),
        "rejected_count": len(rejected_orders),
        "expired_count": len(expired_orders),
        "fill_rate": _decimal_text(fill_rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "partial_fill_rate": _decimal_text(partial_fill_rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "no_fill_rate": _decimal_text(no_fill_rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "requested_notional": _decimal_text(requested_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "filled_notional": _decimal_text(filled_notional.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "expected_fill_size": _decimal_text(expected_fill_size_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "actual_fill_size": _decimal_text(actual_fill_size_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "expected_fill_notional": _decimal_text(expected_fill_notional_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "actual_fill_notional": _decimal_text(actual_fill_notional_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "filled_notional_rate": _decimal_text(filled_notional_rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "fee_total": _decimal_text(fee_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "rebate_total": _decimal_text(rebate_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "slippage_total": _decimal_text(slippage_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "execution_cost_total": _decimal_text(execution_cost_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_fill_price": _decimal_text(_avg_decimal(fill_prices).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_participation_rate": _decimal_text(_avg_decimal(participation_rates).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "avg_latency_blocks": _decimal_text(_avg_decimal(latency_blocks).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_latency_seconds": _decimal_text(_avg_decimal(latency_seconds).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_effective_latency_x_span": _decimal_text(_avg_decimal(effective_latency_x_spans).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "max_effective_latency_x_span": _decimal_text((max(effective_latency_x_spans) if effective_latency_x_spans else Decimal("0")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_effective_liquidity_cap_pct": _decimal_text(_avg_decimal(effective_liquidity_caps).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_adverse_slippage_cents": _decimal_text(_avg_decimal(adverse_slippage_values).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_fill_probability_haircut_pct": _decimal_text(_avg_decimal(fill_haircut_values).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "queue_adjusted_count": queue_adjusted_count,
        "avg_queue_fill_factor": _decimal_text(_avg_decimal(queue_fill_factors).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_queue_ahead_size": _decimal_text(_avg_decimal(queue_ahead_sizes).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "raw_event_count": raw_event_count,
        "loaded_raw_event_count": loaded_raw_event_count,
        "deduped_raw_event_count": deduped_raw_event_count,
        "raw_duplicate_event_count": raw_duplicate_event_count,
        "raw_exact_duplicate_event_count": raw_exact_duplicate_event_count,
        "raw_conflicting_duplicate_event_count": raw_conflicting_duplicate_event_count,
        "raw_duplicate_group_count": raw_duplicate_group_count,
        "raw_conflicting_duplicate_group_count": raw_conflicting_duplicate_group_count,
        "raw_duplicate_classification": "classified" if duplicate_classified else "legacy_unclassified",
        "raw_canonical_event_count": int(replay.get("canonical_event_count") or 0),
        "raw_fallback_key_event_count": int(replay.get("fallback_key_event_count") or 0),
        "raw_unknown_key_event_count": int(replay.get("unknown_key_event_count") or 0),
        "raw_canonical_fill_key_coverage_pct": str(replay.get("raw_canonical_fill_key_coverage_pct") or raw_trade_tick_report.get("canonical_fill_key_coverage_pct") or "0"),
        "raw_maker_taker_side_coverage_pct": str(replay.get("raw_maker_taker_side_coverage_pct") or raw_trade_tick_report.get("maker_taker_side_coverage_pct") or "0"),
        "raw_trade_tick_count": int(replay.get("raw_trade_tick_count") or raw_trade_tick_report.get("trade_tick_count") or 0),
        "raw_block_count": int(replay.get("raw_block_count") or raw_trade_tick_report.get("block_count") or 0),
        "raw_enabled": raw_enabled,
        "raw_fallback": fallback or None,
        "loaded_block_window": {
            "from_block": replay_loaded_window.get("from_block", replay.get("from_block")),
            "to_block": replay_loaded_window.get("to_block", replay.get("to_block")),
            "loaded_first_block": replay_loaded_window.get("loaded_first_block", replay.get("loaded_first_block")),
            "loaded_last_block": replay_loaded_window.get("loaded_last_block", replay.get("loaded_last_block")),
            "replay_first_block": replay_loaded_window.get("replay_first_block", replay.get("replay_first_block")),
            "replay_last_block": replay_loaded_window.get("replay_last_block", replay.get("replay_last_block")),
            "market_id": replay_loaded_window.get("market_id", replay.get("market_id")),
            "token_id": replay_loaded_window.get("token_id", replay.get("token_id")),
            "token_id_hex": replay_loaded_window.get("token_id_hex", replay.get("token_id_hex")),
            "loaded_event_count": replay_loaded_window.get("loaded_event_count", loaded_raw_event_count),
            "replay_event_count": replay_loaded_window.get("replay_event_count", raw_event_count),
            "duplicate_event_count": replay_loaded_window.get("duplicate_event_count", raw_duplicate_event_count),
            "exact_duplicate_event_count": replay_loaded_window.get("exact_duplicate_event_count", raw_exact_duplicate_event_count),
            "conflicting_duplicate_event_count": replay_loaded_window.get("conflicting_duplicate_event_count", raw_conflicting_duplicate_event_count),
            "duplicate_group_count": replay_loaded_window.get("duplicate_group_count", raw_duplicate_group_count),
            "conflicting_duplicate_group_count": replay_loaded_window.get("conflicting_duplicate_group_count", raw_conflicting_duplicate_group_count),
            "hit_limit": replay_loaded_window.get("hit_limit", replay.get("hit_limit")),
            "limit": replay_loaded_window.get("limit", replay.get("limit")),
        },
        "candidate_event_count": candidate_event_count,
        "candidate_events_with_counterparty": candidate_events_with_counterparty,
        "candidate_event_unique_count": candidate_unique,
        "candidate_event_duplicate_count": candidate_duplicates,
        "counterparty_tag_rate": _decimal_text(counterparty_tag_rate.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "candidate_volume": _decimal_text(candidate_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "consumed_event_count": consumed_event_count,
        "consumed_event_unique_count": consumed_unique,
        "consumed_event_duplicate_count": consumed_duplicates,
        "consumed_volume": _decimal_text(consumed_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "raw_evidence_summary": raw_evidence_summary,
        "fill_probability_model_counts": dict(sorted(fill_probability_model_counts.items())),
        "side_compatibility_counts": dict(sorted(side_compatibility_counts.items())),
        "side_discounted_tick_count": side_discounted_tick_count,
        "block_participation_discount_tick_count": block_participation_discount_tick_count,
        "max_requested_block_participation_pct": _decimal_text(max_requested_block_participation_pct.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "min_block_participation_factor": _decimal_text(min_block_participation_factor.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)) if block_participation_discount_tick_count else "1",
        "fill_schedule_tick_count": fill_schedule_tick_count,
        "fill_schedule_fillable_size": _decimal_text(fill_schedule_fillable_size.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "raw_candidate_orders": raw_candidate_orders,
        "fallback_candidate_orders": fallback_candidate_orders,
        "execution_evidence_counts": dict(sorted(execution_evidence_counts.items())),
        "fill_evidence_counts": dict(sorted(fill_evidence_counts.items())),
        "block_bar_execution_models": dict(sorted(block_bar_execution_models.items())),
        "resting_order_continued_count": resting_order_continued_count,
        "raw_orderfilled_fill_count": raw_orderfilled_fill_count,
        "block_bar_synthetic_fill_count": block_bar_synthetic_fill_count,
        "raw_replay_fallback_suppressed_count": raw_replay_fallback_suppressed_count,
        "raw_replay_synthetic_cross_no_fill_count": raw_replay_synthetic_cross_no_fill_count,
        "raw_replay_coverage_pct": _decimal_text((_ratio(raw_orderfilled_fill_count, len(filled_orders)) * Decimal("100")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "block_bar_fallback_pct": _decimal_text((_ratio(block_bar_synthetic_fill_count, len(filled_orders)) * Decimal("100")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)),
        "avg_markout_after_1_bars": _decimal_text(_avg_decimal(markouts["1"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_markout_after_5_bars": _decimal_text(_avg_decimal(markouts["5"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_markout_after_20_bars": _decimal_text(_avg_decimal(markouts["20"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "markout_sample_count": {horizon: len(values) for horizon, values in markouts.items()},
        "avg_markout_after_60_seconds": _decimal_text(_avg_decimal(markout_seconds["60"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_markout_after_300_seconds": _decimal_text(_avg_decimal(markout_seconds["300"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_markout_after_1200_seconds": _decimal_text(_avg_decimal(markout_seconds["1200"]).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "markout_seconds_sample_count": {horizon: len(values) for horizon, values in markout_seconds.items()},
        "adverse_selection_buckets": dict(sorted(adverse_selection_buckets.items())),
        "adverse_selection_count": sum(count for key, count in adverse_selection_buckets.items() if key.endswith("_adverse")),
        "missed_opportunity_count": missed_opportunity_count,
        "missed_opportunity_notional_total": _decimal_text(missed_opportunity_notional_total.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_missed_opportunity_notional": _decimal_text(_avg_decimal(missed_opportunity_notionals).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "max_missed_opportunity_notional": _decimal_text((max(missed_opportunity_notionals) if missed_opportunity_notionals else Decimal("0")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_missed_opportunity_price_move": _decimal_text(_avg_decimal(missed_opportunity_price_moves).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "missed_opportunity_by_reason": {
            key: _decimal_text(value.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP))
            for key, value in sorted(missed_opportunity_by_reason.items())
        },
        "missed_opportunity_buckets": dict(sorted(missed_opportunity_buckets.items())),
        "missed_opportunity_notional_by_bucket": {
            key: _decimal_text(value.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP))
            for key, value in sorted(missed_opportunity_notional_by_bucket.items())
        },
        "status_counts": dict(sorted(status_counts.items())),
        "source_counts": dict(sorted(source_counts.items())),
        "side_counts": dict(sorted(side_counts.items())),
        "role_counts": dict(sorted(role_counts.items())),
        "order_type_counts": dict(sorted(order_type_counts.items())),
        "time_in_force_counts": dict(sorted(time_in_force_counts.items())),
        "no_fill_reasons": dict(sorted(no_fill_reasons.items())),
        "order_anomaly_flags": dict(sorted(order_anomaly_flags.items())),
        "order_anomaly_count": sum(order_anomaly_flags.values()),
        "anomaly_order_count": len(anomaly_order_ids),
        "real_order_state_observed_count": real_order_state_observed_count,
        "real_order_state_counts": dict(sorted(real_order_state_counts.items())),
        "real_order_state_flags": dict(sorted(real_order_state_flags.items())),
        "real_order_state_flag_count": sum(real_order_state_flags.values()),
        "avg_order_submit_accept_latency_seconds": _decimal_text(_avg_decimal(submit_accept_latency_seconds).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "avg_cancel_accept_latency_seconds": _decimal_text(_avg_decimal(cancel_accept_latency_seconds).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "environment_flags": dict(sorted(environment_flags.items())),
        "environment_flag_count": sum(environment_flags.values()),
    }


def _flag_key(value: Any) -> str:
    return "".join(char if char.isalnum() else "_" for char in str(value or "unknown").lower()).strip("_") or "unknown"


def _order_notes(meta: dict[str, Any]) -> list[str]:
    notes = meta.get("notes") if isinstance(meta, dict) else None
    if isinstance(notes, list):
        return [str(item) for item in notes]
    if isinstance(notes, str) and notes:
        return [notes]
    return []


def _order_state_audit(row: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    flags: dict[str, int] = {}
    observed = False

    def add_count(prefix: str, value: Any) -> None:
        nonlocal observed
        if value in (None, ""):
            return
        observed = True
        key = f"{prefix}:{_flag_key(value)}"
        counts[key] = counts.get(key, 0) + 1

    def add_flag(key: str) -> None:
        nonlocal observed
        observed = True
        flags[key] = flags.get(key, 0) + 1

    submit_status = _first_meta_value(meta, "submit_status", "submission_status", "order_submit_status")
    accepted_status = _first_meta_value(meta, "accepted_status", "order_accepted_status", "clob_accept_status")
    cancel_status = _first_meta_value(meta, "cancel_status", "order_cancel_status", "cancel_result")
    api_status = _first_meta_value(meta, "api_order_status", "api_status", "get_order_status")
    chain_status = _first_meta_value(meta, "chain_order_status", "chain_status", "onchain_status")
    clob_status = _first_meta_value(meta, "clob_order_status", "clob_status", "matching_status")

    add_count("submit", submit_status)
    add_count("accepted", accepted_status)
    add_count("cancel", cancel_status)
    add_count("api", api_status)
    add_count("chain", chain_status)
    add_count("clob", clob_status)

    submit_key = _flag_key(submit_status)
    cancel_key = _flag_key(cancel_status)
    api_key = _flag_key(api_status)
    chain_key = _flag_key(chain_status)
    clob_key = _flag_key(clob_status)
    order_type = str(row.get("order_type") or "")

    if submit_key in {"rejected", "reject", "failed", "error"}:
        add_flag("submit_rejected")
    if (submit_key in {"rejected", "reject"} or _bool_meta(meta, "post_only_rejected", "postOnlyRejected")) and "post_only" in order_type:
        add_flag("post_only_rejected")
    if cancel_key in {"submitted", "pending"}:
        add_flag("cancel_submitted")
    if cancel_key in {"accepted", "cancelled", "canceled", "success"}:
        add_flag("cancel_accepted")
    if cancel_key in {"failed", "rejected", "error"}:
        add_flag("cancel_failed")
    if api_key in {"open", "live", "active", "resting"} and chain_key in {"filled", "matched", "executed"}:
        add_flag("api_status_lagging_after_chain_fill")
    if clob_key in {"filled", "matched", "executed"} and str(row.get("status") or "").upper() in {"NO_FILL", "REJECTED", "CANCELED", "CANCEL_FAILED"}:
        add_flag("clob_fill_status_conflict")

    notes_text = " ".join(_order_notes(meta)).lower()
    if "postonly" in notes_text or "post_only" in notes_text:
        if "reject" in notes_text or "rejected" in notes_text:
            add_flag("post_only_rejected")
    if "submit_rejected" in notes_text or "order submit rejected" in notes_text:
        add_flag("submit_rejected")
    if "service not ready" in notes_text or " 425" in notes_text or "425" == notes_text.strip():
        add_flag("service_not_ready_425")
        add_flag("submit_rejected")
    if "cancel_race" in notes_text or "cancel race" in notes_text:
        add_flag("cancel_race")
    if "ghost" in notes_text:
        add_flag("ghost_fill_observed")
    if "status_mismatch" in notes_text or "order_status" in notes_text:
        add_flag("order_status_mismatch")

    submit_latency = _seconds_between(
        _first_meta_value(meta, "submit_at", "submitted_at", "submit_timestamp", "submitted_timestamp"),
        _first_meta_value(meta, "accepted_at", "order_accepted_at", "accepted_timestamp"),
    )
    cancel_latency = _seconds_between(
        _first_meta_value(meta, "cancel_submitted_at", "cancel_submit_at", "cancel_requested_at"),
        _first_meta_value(meta, "cancel_accepted_at", "cancelled_at", "canceled_at", "cancel_timestamp"),
    )
    return {
        "observed": observed,
        "counts": counts,
        "flags": flags,
        "submit_accept_latency_seconds": submit_latency,
        "cancel_accept_latency_seconds": cancel_latency,
    }


def _first_meta_value(meta: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in meta and meta.get(key) not in (None, ""):
            return meta.get(key)
    return None


def _bool_meta(meta: dict[str, Any], *keys: str) -> bool:
    value = _first_meta_value(meta, *keys)
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _seconds_between(start: Any, end: Any) -> Decimal | None:
    start_dt = _parse_datetime_like(start)
    end_dt = _parse_datetime_like(end)
    if start_dt is None or end_dt is None:
        return None
    seconds = max(0.0, (end_dt - start_dt).total_seconds())
    return Decimal(str(seconds)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _parse_datetime_like(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw = raw / 1000.0
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        raw = float(text)
        if raw > 10_000_000_000:
            raw = raw / 1000.0
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _fill_event_key(event: dict[str, Any]) -> str:
    parts = [
        event.get("tx_hash"),
        event.get("log_index"),
        event.get("market_id"),
        event.get("token_id"),
        event.get("maker"),
        event.get("taker"),
        event.get("side_code") or event.get("side"),
    ]
    key = "|".join(str(part) for part in parts if part not in (None, ""))
    return key


def _fill_event_notional(event: dict[str, Any]) -> Decimal:
    try:
        size = Decimal(str(event.get("size") or 0))
    except Exception:
        size = Decimal("0")
    try:
        price = Decimal(str(event.get("trade_price") or event.get("price") or event.get("avg_fill_price") or 0))
    except Exception:
        price = Decimal("0")
    return (max(Decimal("0"), size) * max(Decimal("0"), price)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _missed_opportunity_bucket_keys(row: dict[str, Any], missed_opportunity: dict[str, Any], reason: str) -> list[str]:
    side = str(row.get("side") or "").upper()
    lifecycle = "entry" if side.startswith("BUY") else "exit" if side.startswith("SELL") else "unknown"
    role = _flag_key(row.get("role") or "unknown")
    direction = _flag_key(missed_opportunity.get("direction") or "unknown")
    decision_price = Decimal(str(row.get("decision_price") or row.get("requested_price") or missed_opportunity.get("limit_price") or 0))
    observed_notional = _missed_observed_notional(row, decision_price)
    return [
        f"lifecycle:{lifecycle}",
        f"role:{role}",
        f"reason:{_flag_key(reason)}",
        f"direction:{direction}",
        f"signal_strength:{_probability_strength_bucket(decision_price)}",
        f"liquidity:{_liquidity_notional_bucket(observed_notional)}",
    ]


def _missed_observed_notional(row: dict[str, Any], decision_price: Decimal) -> Decimal:
    available = Decimal(str(row.get("available_notional") or 0))
    if available > 0:
        return available
    block_volume = Decimal(str(row.get("block_volume") or 0))
    price = max(Decimal("0"), decision_price)
    return (block_volume * price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _probability_strength_bucket(value: Decimal) -> str:
    value = max(Decimal("0"), min(Decimal("1"), Decimal(str(value or 0))))
    if value < Decimal("0.20"):
        return "very_low"
    if value < Decimal("0.40"):
        return "low"
    if value < Decimal("0.60"):
        return "mid"
    if value < Decimal("0.80"):
        return "high"
    return "very_high"


def _liquidity_notional_bucket(value: Decimal) -> str:
    value = max(Decimal("0"), Decimal(str(value or 0)))
    if value == 0:
        return "zero"
    if value < Decimal("10"):
        return "tiny"
    if value < Decimal("100"):
        return "small"
    if value < Decimal("1000"):
        return "medium"
    return "large"


def fill_quality_metrics(report: dict[str, Any]) -> list[dict[str, Any]]:
    submitted = int(report.get("submitted_count") or 0)
    filled = int(report.get("filled_count") or 0)
    no_fill = int(report.get("no_fill_count") or 0)
    partial = int(report.get("partial_fill_count") or 0)
    candidate_events = int(report.get("candidate_event_count") or 0)
    consumed_events = int(report.get("consumed_event_count") or 0)
    raw_event_duplicates = int(report.get("candidate_event_duplicate_count") or 0) + int(report.get("consumed_event_duplicate_count") or 0)
    counterparty_tag_rate = Decimal(str(report.get("counterparty_tag_rate") or "0"))
    raw_evidence = report.get("raw_evidence_summary") if isinstance(report.get("raw_evidence_summary"), dict) else {}
    consumed_unique_notional = Decimal(str(raw_evidence.get("consumed_unique_notional") or "0"))
    candidate_unique_notional = Decimal(str(raw_evidence.get("candidate_unique_notional") or "0"))
    raw_events = int(report.get("raw_event_count") or 0)
    raw_orderfilled_fill_count = int(report.get("raw_orderfilled_fill_count") or 0)
    block_bar_synthetic_fill_count = int(report.get("block_bar_synthetic_fill_count") or 0)
    raw_replay_fallback_suppressed_count = int(report.get("raw_replay_fallback_suppressed_count") or 0)
    raw_replay_coverage_pct = Decimal(str(report.get("raw_replay_coverage_pct") or "0"))
    block_bar_fallback_pct = Decimal(str(report.get("block_bar_fallback_pct") or "0"))
    block_bar_models = report.get("block_bar_execution_models") if isinstance(report.get("block_bar_execution_models"), dict) else {}
    rich_block_bar_count = int(block_bar_models.get("ohlcv_vwap_tick_sequence") or 0)
    resting_order_continued_count = int(report.get("resting_order_continued_count") or 0)
    fill_probability_models = report.get("fill_probability_model_counts") if isinstance(report.get("fill_probability_model_counts"), dict) else {}
    side_compatibility_counts = report.get("side_compatibility_counts") if isinstance(report.get("side_compatibility_counts"), dict) else {}
    side_discounted_tick_count = int(report.get("side_discounted_tick_count") or 0)
    block_participation_discount_tick_count = int(report.get("block_participation_discount_tick_count") or 0)
    max_requested_block_participation_pct = Decimal(str(report.get("max_requested_block_participation_pct") or "0"))
    min_block_participation_factor = Decimal(str(report.get("min_block_participation_factor") or "1"))
    fill_schedule_tick_count = int(report.get("fill_schedule_tick_count") or 0)
    fill_schedule_fillable_size = Decimal(str(report.get("fill_schedule_fillable_size") or "0"))
    markout_1 = Decimal(str(report.get("avg_markout_after_1_bars") or "0"))
    markout_5 = Decimal(str(report.get("avg_markout_after_5_bars") or "0"))
    markout_60_seconds = Decimal(str(report.get("avg_markout_after_60_seconds") or "0"))
    markout_300_seconds = Decimal(str(report.get("avg_markout_after_300_seconds") or "0"))
    adverse_selection_count = int(report.get("adverse_selection_count") or 0)
    avg_effective_latency_x_span = Decimal(str(report.get("avg_effective_latency_x_span") or "0"))
    avg_effective_liquidity_cap_pct = Decimal(str(report.get("avg_effective_liquidity_cap_pct") or "0"))
    avg_adverse_slippage_cents = Decimal(str(report.get("avg_adverse_slippage_cents") or "0"))
    avg_fill_probability_haircut_pct = Decimal(str(report.get("avg_fill_probability_haircut_pct") or "0"))
    queue_adjusted_count = int(report.get("queue_adjusted_count") or 0)
    avg_queue_fill_factor = Decimal(str(report.get("avg_queue_fill_factor") or "0"))
    avg_queue_ahead_size = Decimal(str(report.get("avg_queue_ahead_size") or "0"))
    fee_total = Decimal(str(report.get("fee_total") or "0"))
    rebate_total = Decimal(str(report.get("rebate_total") or "0"))
    execution_cost_total = Decimal(str(report.get("execution_cost_total") or "0"))
    missed_opportunity_count = int(report.get("missed_opportunity_count") or 0)
    missed_opportunity_notional_total = Decimal(str(report.get("missed_opportunity_notional_total") or "0"))
    avg_missed_opportunity_price_move = Decimal(str(report.get("avg_missed_opportunity_price_move") or "0"))
    order_anomaly_count = int(report.get("order_anomaly_count") or 0)
    anomaly_order_count = int(report.get("anomaly_order_count") or 0)
    real_order_state_observed_count = int(report.get("real_order_state_observed_count") or 0)
    real_order_state_flag_count = int(report.get("real_order_state_flag_count") or 0)
    avg_order_submit_accept_latency_seconds = Decimal(str(report.get("avg_order_submit_accept_latency_seconds") or "0"))
    avg_cancel_accept_latency_seconds = Decimal(str(report.get("avg_cancel_accept_latency_seconds") or "0"))
    environment_flag_count = int(report.get("environment_flag_count") or 0)
    fill_rate = Decimal(str(report.get("fill_rate") or "0"))
    no_fill_rate = Decimal(str(report.get("no_fill_rate") or "0"))
    reasons = report.get("no_fill_reasons") if isinstance(report.get("no_fill_reasons"), dict) else {}
    top_reason = "-"
    if reasons:
        top_reason = max(reasons.items(), key=lambda item: int(item[1]))[0]
    rows = [
        ("fill_quality_fill_rate", "Fill Rate", "prediction", fill_rate, f"{fill_rate:.1f}%", f"{filled} / {submitted} orders", "positive" if no_fill == 0 and submitted else "negative" if submitted and fill_rate < Decimal("50") else "neutral", "Share of submitted orders with positive simulated filled size"),
        ("fill_quality_no_fill_rate", "No Fill Rate", "prediction", no_fill_rate, f"{no_fill_rate:.1f}%", top_reason, "negative" if no_fill else "positive", "Share of submitted orders that generated a signal but did not fill; delta shows the top reason"),
        ("fill_quality_partial_count", "Partial Fill Orders", "prediction", Decimal(partial), str(partial), "partial lifecycle", "negative" if partial else "positive", "Orders that filled less than their requested size"),
        ("fill_quality_raw_events", "Raw Fill Events", "prediction", Decimal(raw_events), f"{raw_events:,}", "orderfilled_fact", "positive" if report.get("raw_enabled") else "negative" if submitted else "neutral", "Raw ClickHouse orderfilled_fact events loaded for this execution replay"),
        ("fill_quality_candidate_events", "Candidate Events", "prediction", Decimal(candidate_events), f"{candidate_events:,}", "crossed limit evidence", "positive" if candidate_events else "negative" if submitted else "neutral", "Raw candidate fills attached to orders after latency and limit-price filters"),
        ("fill_quality_consumed_events", "Consumed Events", "prediction", Decimal(consumed_events), f"{consumed_events:,}", "actual fill evidence", "positive" if consumed_events else "negative" if submitted else "neutral", "Raw fill events actually consumed to compute filled size, fill price, and PnL"),
        ("fill_quality_raw_replay_coverage", "Raw Replay Coverage", "prediction", raw_replay_coverage_pct, f"{raw_replay_coverage_pct:.1f}%", f"{raw_orderfilled_fill_count} / {filled} filled orders", "positive" if raw_replay_coverage_pct >= Decimal("90") or filled == 0 else "negative" if raw_replay_coverage_pct <= Decimal("0") else "neutral", "Share of filled simulated orders supported by raw OrderFilled evidence"),
        ("fill_quality_block_bar_fallback", "Block Bar Fallback", "prediction", block_bar_fallback_pct, f"{block_bar_fallback_pct:.1f}%", f"{block_bar_synthetic_fill_count} / {filled} filled orders", "negative" if block_bar_synthetic_fill_count else "positive", "Share of filled simulated orders that used OHLCV block-bar fallback rather than raw OrderFilled evidence"),
        ("fill_quality_raw_suppressed_fallback", "Raw Suppressed Fallback", "prediction", Decimal(raw_replay_fallback_suppressed_count), str(raw_replay_fallback_suppressed_count), "raw replay authoritative", "positive" if raw_replay_fallback_suppressed_count else "neutral", "Orders where OHLCV block bar crossed the limit but raw OrderFilled replay did not support a fill, so synthetic fallback was suppressed"),
        ("fill_quality_block_bar_tick_sequence", "Block Bar Tick Sequence", "prediction", Decimal(rich_block_bar_count), str(rich_block_bar_count), "OHLCV/VWAP fallback", "neutral" if rich_block_bar_count else "positive", "Block-bar fallback orders replayed as synthetic OHLCV/VWAP tick sequences instead of one full-volume close tick"),
        ("fill_quality_probability_models", "Fill Probability Models", "prediction", Decimal(len(fill_probability_models)), str(len(fill_probability_models)), ",".join(sorted(fill_probability_models)) or "-", "positive" if fill_probability_models else "neutral", "Distinct raw tick sequence fill probability models used by simulated orders"),
        ("fill_quality_schedule_ticks", "Fill Schedule Ticks", "prediction", Decimal(fill_schedule_tick_count), str(fill_schedule_tick_count), f"fillable {fill_schedule_fillable_size:.4f}", "positive" if fill_schedule_tick_count else "neutral", "Raw tick schedule entries used to convert candidate OrderFilled volume into profile-adjusted fillable size"),
        ("fill_quality_side_compatibility", "Side Compatibility", "prediction", Decimal(side_discounted_tick_count), str(side_discounted_tick_count), ",".join(f"{key}:{value}" for key, value in sorted(side_compatibility_counts.items())) or "-", "negative" if side_discounted_tick_count else "positive" if fill_schedule_tick_count else "neutral", "Raw tick schedule entries discounted because historical trade side did not match maker/taker execution direction"),
        ("fill_quality_block_participation_pressure", "Block Participation Pressure", "prediction", Decimal(block_participation_discount_tick_count), str(block_participation_discount_tick_count), f"max {max_requested_block_participation_pct:.1f}% min_factor {min_block_participation_factor:.3f}", "negative" if block_participation_discount_tick_count else "positive" if fill_schedule_tick_count else "neutral", "Raw tick schedule entries discounted because requested order size was too large relative to same-block OrderFilled volume"),
        ("fill_quality_resting_order_continued", "Resting Order Continuation", "prediction", Decimal(resting_order_continued_count), str(resting_order_continued_count), "GTC partial lifecycle", "neutral" if resting_order_continued_count else "positive", "Maker GTC orders that remained resting after an initial partial fill and continued across later blocks"),
        ("fill_quality_raw_event_duplicates", "Raw Event Duplicates", "prediction", Decimal(raw_event_duplicates), str(raw_event_duplicates), "candidate + consumed", "negative" if raw_event_duplicates else "positive", "Duplicate raw fill event keys observed in attached candidate or consumed evidence"),
        ("fill_quality_counterparty_rate", "Counterparty Tags", "prediction", counterparty_tag_rate, f"{counterparty_tag_rate:.1f}%", "maker/taker coverage", "positive" if counterparty_tag_rate >= Decimal("90") or candidate_events == 0 else "negative", "Share of candidate raw events that include maker or taker tags"),
        ("fill_quality_candidate_unique_notional", "Candidate Unique Notional", "prediction", candidate_unique_notional, _money(candidate_unique_notional), "canonical raw evidence", "positive" if candidate_unique_notional > 0 else "neutral", "Deduplicated candidate raw OrderFilled notional using canonical fill keys"),
        ("fill_quality_consumed_unique_notional", "Consumed Unique Notional", "prediction", consumed_unique_notional, _money(consumed_unique_notional), "canonical raw evidence", "positive" if consumed_unique_notional > 0 else "neutral", "Deduplicated consumed raw OrderFilled notional used by simulated fills"),
        ("fill_quality_effective_latency", "Effective Latency", "prediction", avg_effective_latency_x_span, f"{avg_effective_latency_x_span:.1f}", "x-axis span", "negative" if avg_effective_latency_x_span > 0 else "positive", "Average submit_x - signal_x after combining latency_blocks and latency_seconds"),
        ("fill_quality_effective_liquidity_cap", "Effective Liquidity Cap", "prediction", avg_effective_liquidity_cap_pct, f"{avg_effective_liquidity_cap_pct:.1f}%", "after haircut", "negative" if avg_effective_liquidity_cap_pct < Decimal("100") and submitted else "positive", "Average liquidity cap after fill_probability_haircut_pct stress is applied"),
        ("fill_quality_adverse_slippage", "Adverse Slippage", "prediction", avg_adverse_slippage_cents, f"{avg_adverse_slippage_cents:.4f}", f"{avg_fill_probability_haircut_pct:.1f}% haircut", "negative" if avg_adverse_slippage_cents > 0 or avg_fill_probability_haircut_pct > 0 else "positive", "Average adverse slippage and fill haircut stress configured on orders"),
        ("fill_quality_queue_adjusted", "Queue Adjusted Orders", "prediction", Decimal(queue_adjusted_count), str(queue_adjusted_count), f"avg factor {avg_queue_fill_factor:.3f}", "neutral" if queue_adjusted_count else "positive", "Maker fills whose expected size was reduced by same-price LOB queue evidence"),
        ("fill_quality_avg_queue_ahead", "Avg Queue Ahead", "prediction", avg_queue_ahead_size, f"{avg_queue_ahead_size:.4f}", "same-price depth", "negative" if avg_queue_ahead_size > 0 else "positive", "Average same-price size ahead of simulated maker orders when LOB queue evidence is present"),
        ("fill_quality_fee_total", "Fill Fees", "prediction", -fee_total, _money(-fee_total), "fee attribution", "negative" if fee_total else "neutral", "Total simulated order fees before rebates"),
        ("fill_quality_rebate_total", "Fill Rebates", "prediction", rebate_total, _money(rebate_total), "maker rebate", "positive" if rebate_total else "neutral", "Total simulated maker rebates credited to fills"),
        ("fill_quality_net_execution_cost", "Net Execution Cost", "prediction", -execution_cost_total, _money(-execution_cost_total), "fees + slippage - rebates", "negative" if execution_cost_total > 0 else "positive" if execution_cost_total < 0 else "neutral", "Net modeled execution cost after fee, slippage, and rebate attribution"),
        ("fill_quality_missed_opportunity", "Missed Opportunity", "prediction", -missed_opportunity_notional_total, _money(-missed_opportunity_notional_total), f"{missed_opportunity_count} no-fill orders", "negative" if missed_opportunity_notional_total > 0 else "positive", "Diagnostic opportunity cost for NO_FILL orders where later price movement would have favored the intended order side"),
        ("fill_quality_avg_missed_move", "Avg Missed Move", "prediction", avg_missed_opportunity_price_move, f"{avg_missed_opportunity_price_move:.4f}", "price points", "negative" if avg_missed_opportunity_price_move > 0 else "positive", "Average favorable price move after no-fill orders; diagnostic only, not counted as a fill"),
        ("fill_quality_order_anomalies", "Order Anomalies", "prediction", Decimal(order_anomaly_count), str(order_anomaly_count), f"{anomaly_order_count} orders", "negative" if order_anomaly_count else "positive", "Order-level fill inconsistencies such as fallback fills, status/size conflicts, reused raw events, or cancel/ghost notes"),
        ("fill_quality_real_order_state", "Real Order State", "prediction", Decimal(real_order_state_observed_count), str(real_order_state_observed_count), f"{real_order_state_flag_count} flags", "negative" if real_order_state_flag_count else "positive" if real_order_state_observed_count else "neutral", "Orders with real submit/accepted/cancel/API/chain status evidence attached to meta"),
        ("fill_quality_submit_accept_latency", "Submit Accept Latency", "prediction", avg_order_submit_accept_latency_seconds, f"{avg_order_submit_accept_latency_seconds:.3f}s", "real order state", "negative" if avg_order_submit_accept_latency_seconds > Decimal("2") else "positive" if real_order_state_observed_count else "neutral", "Average submit -> accepted latency from real order state metadata"),
        ("fill_quality_cancel_accept_latency", "Cancel Accept Latency", "prediction", avg_cancel_accept_latency_seconds, f"{avg_cancel_accept_latency_seconds:.3f}s", "real order state", "negative" if avg_cancel_accept_latency_seconds > Decimal("2") else "positive" if real_order_state_observed_count else "neutral", "Average cancel submitted -> cancel accepted latency from real order state metadata"),
        ("fill_quality_environment_flags", "Environment Flags", "system", Decimal(environment_flag_count), str(environment_flag_count), "data/runtime flags", "negative" if environment_flag_count else "positive", "Run-level data and replay environment flags such as raw fallback, price gaps, jumps, low coverage, or missing counterparty tags"),
        ("fill_quality_markout_1", "Markout 1 Bar", "prediction", markout_1, f"{markout_1:.4f}", "positive favors order side", "positive" if markout_1 >= 0 else "negative", "Average one-bar post-fill markout; positive means price moved favorably for the filled order side"),
        ("fill_quality_markout_5", "Markout 5 Bars", "prediction", markout_5, f"{markout_5:.4f}", "positive favors order side", "positive" if markout_5 >= 0 else "negative", "Average five-bar post-fill markout; negative values flag adverse selection risk"),
        ("fill_quality_markout_60s", "Markout 60s", "prediction", markout_60_seconds, f"{markout_60_seconds:.4f}", "timestamp horizon", "positive" if markout_60_seconds >= 0 else "negative", "Average 60-second post-fill markout using timestamped price rows"),
        ("fill_quality_markout_300s", "Markout 300s", "prediction", markout_300_seconds, f"{markout_300_seconds:.4f}", "timestamp horizon", "positive" if markout_300_seconds >= 0 else "negative", "Average 300-second post-fill markout using timestamped price rows"),
        ("fill_quality_adverse_selection", "Adverse Selection", "prediction", Decimal(adverse_selection_count), str(adverse_selection_count), "negative markout samples", "negative" if adverse_selection_count else "positive", "Count of post-fill markout samples where price moved against the filled order side"),
    ]
    return [
        {
            "metric_key": key,
            "metric_name": name,
            "metric_group": group,
            "value": value,
            "formatted_value": formatted,
            "delta": delta,
            "status": status,
            "tooltip": tooltip,
            "sort_order": 80 + index,
        }
        for index, (key, name, group, value, formatted, delta, status, tooltip) in enumerate(rows, start=1)
    ]


def _avg_decimal(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def replace_backtest_results(conn: Any, run_id: int, result: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM quant.quant_backtest_metrics WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM quant.quant_backtest_equity WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM quant.quant_backtest_trades WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM quant.quant_backtest_orders WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM quant.quant_backtest_ledger WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM quant.quant_backtest_events WHERE run_id = %s", (run_id,))
        cur.executemany(
            """
            INSERT INTO quant.quant_backtest_metrics (
                run_id, metric_key, metric_name, metric_group, value,
                formatted_value, delta, status, tooltip, sort_order
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    run_id,
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
                for row in result["metrics"]
            ],
        )
        cur.executemany(
            """
            INSERT INTO quant.quant_backtest_equity (
                run_id, point_index, x_axis, x_value, equity,
                drawdown, drawdown_pct, cumulative_return
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    run_id,
                    row["point_index"],
                    row["x_axis"],
                    row["x_value"],
                    row["equity"],
                    row["drawdown"],
                    row["drawdown_pct"],
                    row["cumulative_return"],
                )
                for row in result["equity"]
            ],
        )
        cur.executemany(
            """
            INSERT INTO quant.quant_backtest_trades (
                run_id, trade_id, entry_order_id, exit_order_id,
                market_slug, token_side, side, x_axis,
                entry_x, exit_x, entry_price, exit_price, size, notional,
                requested_notional, filled_notional, fill_pct,
                requested_size, filled_size, unfilled_size, fill_status,
                book_snapshot_id, snapshot_version, staleness_seconds, staleness_blocks,
                avg_fill_price, fill_probability, block_volume, trade_count,
                available_notional, execution_source, fee_cost, rebate_cost, slippage_cost, execution_cost,
                pnl, pnl_pct, holding_bars, exit_reason
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    run_id,
                    row["trade_id"],
                    row.get("entry_order_id"),
                    row.get("exit_order_id"),
                    row["market_slug"],
                    row["token_side"],
                    row["side"],
                    row["x_axis"],
                    row["entry_x"],
                    row["exit_x"],
                    row["entry_price"],
                    row["exit_price"],
                    row["size"],
                    row["notional"],
                    row.get("requested_notional", row["notional"]),
                    row.get("filled_notional", row["notional"]),
                    row.get("fill_pct", Decimal("100")),
                    row.get("requested_size", row.get("size", Decimal("0"))),
                    row.get("filled_size", row.get("size", Decimal("0"))),
                    row.get("unfilled_size", Decimal("0")),
                    row.get("fill_status", "FILLED"),
                    row.get("book_snapshot_id"),
                    row.get("snapshot_version"),
                    row.get("staleness_seconds"),
                    row.get("staleness_blocks"),
                    row.get("avg_fill_price", row.get("exit_price")),
                    row.get("fill_probability", Decimal("0")),
                    row.get("block_volume", Decimal("0")),
                    row.get("trade_count", 0),
                    row.get("available_notional", Decimal("0")),
                    row.get("execution_source", "unknown"),
                    row.get("fee_cost", Decimal("0")),
                    row.get("rebate", row.get("rebate_cost", Decimal("0"))),
                    row.get("slippage_cost", Decimal("0")),
                    row.get("execution_cost", Decimal("0")),
                    row["pnl"],
                    row["pnl_pct"],
                    row["holding_bars"],
                    row["exit_reason"],
                )
                for row in result["trades"]
            ],
        )
        cur.executemany(
            """
            INSERT INTO quant.quant_backtest_orders (
                run_id, order_id, signal_index, trade_id, x_axis,
                signal_x, submit_x, decision_price, requested_price, side,
                role, order_type, status, requested_size, requested_notional,
                expected_fill_size, expected_fill_notional, actual_fill_size, actual_fill_notional,
                filled_size, filled_notional, unfilled_size, avg_fill_price,
                fill_probability, fill_pct, block_volume, trade_count,
                available_notional, participation_rate, fee_cost, rebate_cost, slippage_cost, execution_cost,
                latency_blocks, latency_seconds, no_fill_reason, execution_source,
                execution_evidence_type, raw_candidate_event_count, raw_consumed_event_count,
                block_bar_crossed, block_bar_cross_field, block_bar_cross_price, meta
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            [
                (
                    run_id,
                    row["order_id"],
                    row["signal_index"],
                    row.get("trade_id"),
                    row["x_axis"],
                    row["signal_x"],
                    row["submit_x"],
                    row["decision_price"],
                    row.get("requested_price"),
                    row["side"],
                    row["role"],
                    row["order_type"],
                    row["status"],
                    row["requested_size"],
                    row["requested_notional"],
                    row.get("expected_fill_size", row.get("filled_size", Decimal("0"))),
                    row.get("expected_fill_notional", row.get("filled_notional", Decimal("0"))),
                    row.get("actual_fill_size", row.get("filled_size", Decimal("0"))),
                    row.get("actual_fill_notional", row.get("filled_notional", Decimal("0"))),
                    row["filled_size"],
                    row["filled_notional"],
                    row["unfilled_size"],
                    row.get("avg_fill_price"),
                    row["fill_probability"],
                    row["fill_pct"],
                    row["block_volume"],
                    row["trade_count"],
                    row["available_notional"],
                    row.get("participation_rate", Decimal("0")),
                    row["fee_cost"],
                    row.get("rebate", row.get("rebate_cost", Decimal("0"))),
                    row["slippage_cost"],
                    row["execution_cost"],
                    row["latency_blocks"],
                    row["latency_seconds"],
                    row.get("no_fill_reason"),
                    row["execution_source"],
                    row.get("execution_evidence_type") or (row.get("meta") or {}).get("execution_evidence_type") or "unknown",
                    int(row.get("raw_candidate_event_count") or (row.get("meta") or {}).get("raw_candidate_event_count") or 0),
                    int(row.get("raw_consumed_event_count") or (row.get("meta") or {}).get("raw_consumed_event_count") or 0),
                    bool(row.get("block_bar_crossed") or (row.get("meta") or {}).get("block_bar_crossed") or False),
                    row.get("block_bar_cross_field") or (row.get("meta") or {}).get("block_bar_cross_field"),
                    row.get("block_bar_cross_price") or (row.get("meta") or {}).get("block_bar_cross_price"),
                    json.dumps(row.get("meta") or {}, default=str),
                )
                for row in result.get("orders", [])
            ],
        )
        cur.executemany(
            """
            INSERT INTO quant.quant_backtest_ledger (
                run_id, ledger_id, order_id, trade_id, event_type, x_axis,
                x_value, market_slug, token_side, shares_delta, cash_delta,
                fee, rebate, slippage_cost, execution_cost, realized_pnl,
                position_after, cash_after, price, source, meta
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            [
                (
                    run_id,
                    row["ledger_id"],
                    row.get("order_id"),
                    row.get("trade_id"),
                    row["event_type"],
                    row["x_axis"],
                    row["x_value"],
                    row["market_slug"],
                    row["token_side"],
                    row["shares_delta"],
                    row["cash_delta"],
                    row["fee"],
                    row["rebate"],
                    row["slippage_cost"],
                    row["execution_cost"],
                    row["realized_pnl"],
                    row["position_after"],
                    row["cash_after"],
                    row.get("price"),
                    row["source"],
                    json.dumps(row.get("meta") or {}, default=str),
                )
                for row in result.get("ledger", [])
            ],
        )
        cur.executemany(
            """
            INSERT INTO quant.quant_backtest_events (
                run_id, event_index, event_type, x_axis, x_value,
                trade_id, price, message, meta
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            [
                (
                    run_id,
                    index,
                    row["event_type"],
                    row["x_axis"],
                    row["x_value"],
                    row["trade_id"],
                    row["price"],
                    row["message"],
                    json.dumps(row["meta"], default=str),
                )
                for index, row in enumerate(result["events"], start=1)
            ],
        )


def get_backtest_run_for_update_free(conn: Any, run_id: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM quant.quant_backtest_runs WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
    if not row:
        raise KeyError(f"backtest run not found: {run_id}")
    return dict(row)


def _get_run(conn: Any, run_id: int) -> dict[str, Any]:
    return get_backtest_run_for_update_free(conn, run_id)


def _get_parameters(conn: Any, run_id: int) -> BacktestParameters:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM quant.quant_backtest_parameters WHERE run_id = %s", (run_id,))
        row = cur.fetchone()
    if not row:
        raise KeyError(f"backtest parameters not found: {run_id}")
    return BacktestParameters(
        entry_threshold=row["entry_threshold"],
        exit_threshold=row["exit_threshold"],
        stop_loss=row["stop_loss"],
        take_profit=row["take_profit"],
        max_holding_bars=int(row["max_holding_bars"]),
        initial_capital=row["initial_capital"],
        position_size=row["position_size"],
        fee_bps=row.get("fee_bps", Decimal("0")),
        maker_fee_bps=row.get("maker_fee_bps"),
        taker_fee_bps=row.get("taker_fee_bps"),
        maker_rebate_bps=row.get("maker_rebate_bps", Decimal("0")),
        slippage_bps=row.get("slippage_bps", Decimal("0")),
        liquidity_cap_pct=row.get("liquidity_cap_pct", Decimal("100")),
        max_position_notional=row.get("max_position_notional", Decimal("0")),
        min_fill_pct=row.get("min_fill_pct", Decimal("0")),
        execution_price_mode=normalize_execution_price_mode(row.get("execution_price_mode", ORDERFILLED_CROSS_MODE)),
        execution_profile=row.get("execution_profile", "realistic"),
        pml2_audit_mode=normalize_pml2_audit_mode(
            row.get("pml2_audit_mode", ReplayAuditMode.CHAIN_ONLY.value)
        ),
        order_role=row.get("order_role", "taker"),
        latency_blocks=int(row.get("latency_blocks", 0) or 0),
        adverse_slippage_cents=row.get("adverse_slippage_cents", Decimal("0.005")),
        fill_probability_haircut_pct=row.get("fill_probability_haircut_pct", Decimal("20")),
        latency_seconds=row.get("latency_seconds", Decimal("0")),
        max_book_staleness_seconds=row.get("max_book_staleness_seconds", Decimal("900")),
        allow_partial_fill=bool(row.get("allow_partial_fill", True)),
        min_fill_size=row.get("min_fill_size", Decimal("0")),
        reject_on_stale_book=bool(row.get("reject_on_stale_book", True)),
        final_valuation_mode=row.get("final_valuation_mode", "SETTLEMENT"),
        max_entry_price=row.get("max_entry_price", Decimal("1")),
        min_exit_price=row.get("min_exit_price", Decimal("0")),
        buy_limit_price=row.get("buy_limit_price"),
        sell_limit_price=row.get("sell_limit_price"),
        settlement_value=row.get("settlement_value"),
        gas_cost_per_order=row.get("gas_cost_per_order", Decimal("0")),
        settlement_cost=row.get("settlement_cost", Decimal("0")),
        redeem_cost=row.get("redeem_cost", Decimal("0")),
        capital_cost_bps=row.get("capital_cost_bps", Decimal("0")),
        cancel_after_blocks=int(row.get("cancel_after_blocks", 0) or 0),
        cancel_ack_delay_blocks=int(row.get("cancel_ack_delay_blocks", 0) or 0),
        cancel_fail=bool(row.get("cancel_fail", False)),
    )


def _payload_cashflow_events(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = (
        payload.get("cashflow_events")
        or payload.get("cashflowEvents")
        or payload.get("polymarket_activity")
        or payload.get("polymarketActivity")
        or payload.get("activity_events")
        or payload.get("activityEvents")
        or []
    )
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, dict)]


def _run_cashflow_events(run: Mapping[str, Any]) -> list[dict[str, Any]]:
    direct = _payload_cashflow_events(run)
    meta = run.get("meta") if isinstance(run, Mapping) else {}
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    if isinstance(meta, Mapping):
        return direct + _payload_cashflow_events(meta)
    return direct


def _set_run_status(conn: Any, run_id: int, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_runs
            SET status = %s,
                started_at = CASE WHEN started_at IS NULL THEN clock_timestamp() ELSE started_at END
            WHERE run_id = %s
            """,
            (status, run_id),
        )


def _price_point(row: dict[str, Any]) -> PricePoint:
    return PricePoint(
        x_value=int(row["x_value"]),
        price=Decimal(str(row["price"])),
        volume=Decimal(str(row.get("volume") or 0)),
        trade_count=int(row.get("trade_count") or 0),
        timestamp=_datetime_or_none(row.get("block_timestamp") or row.get("timestamp")),
        open_price=_decimal_or_none(row.get("open_price")) or Decimal(str(row["price"])),
        high_price=_decimal_or_none(row.get("high_price")) or Decimal(str(row["price"])),
        low_price=_decimal_or_none(row.get("low_price")) or Decimal(str(row["price"])),
        close_price=_decimal_or_none(row.get("close_price")) or Decimal(str(row["price"])),
        vwap_price=_decimal_or_none(row.get("vwap_price")),
        buy_volume=_decimal_or_none(row.get("buy_volume")) or Decimal("0"),
        sell_volume=_decimal_or_none(row.get("sell_volume")) or Decimal("0"),
        first_log_index=_int_or_none(row.get("first_log_index")),
        last_log_index=_int_or_none(row.get("last_log_index")),
    )


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _datetime_or_none(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _exit_reason(price: Decimal, entry_price: Decimal, holding_bars: int, params: BacktestParameters) -> str | None:
    if price <= params.exit_threshold:
        return "exit_threshold"
    if price <= entry_price * (Decimal("1") - params.stop_loss):
        return "stop_loss"
    if price >= entry_price * (Decimal("1") + params.take_profit):
        return "take_profit"
    if holding_bars >= params.max_holding_bars:
        return "max_holding_bars"
    return None


def _close_trade(
    run: dict[str, Any],
    x_axis: str,
    position: OpenPosition,
    point: PricePoint,
    point_index: int,
    exit_reason: str,
    params: BacktestParameters,
    *,
    exit_fill: dict[str, Any] | None = None,
    exit_order_id: str | None = None,
) -> dict[str, Any]:
    close_size = Decimal(str(exit_fill.get("size"))) if exit_fill else position.size
    exit_price_value = None
    if exit_fill:
        exit_price_value = exit_fill.get("exit_price")
        if exit_price_value is None:
            exit_price_value = exit_fill.get("avg_fill_price")
    exit_price = Decimal(str(exit_price_value)) if exit_price_value is not None else _execution_price(point.price, params, "exit")
    notional = position.entry_price * close_size
    exit_notional = exit_price * close_size
    entry_fee_cost = Decimal(str(getattr(position, "entry_fee_cost", Decimal("0")) or 0))
    if entry_fee_cost <= 0 and params.fee_bps > 0:
        entry_fee_cost, _ = _fee_rebate_for_notional(params, notional, params.order_role)
    entry_rebate = Decimal(str(getattr(position, "entry_rebate", Decimal("0")) or 0))
    exit_fee_cost = Decimal(str(exit_fill.get("fee_cost") or 0)) if exit_fill else (exit_notional * _bps_fraction(params.fee_bps))
    if exit_fee_cost <= 0 and params.fee_bps > 0 and exit_reason != "settlement":
        exit_fee_cost, _ = _fee_rebate_for_notional(params, exit_notional, params.order_role)
    exit_rebate = Decimal(str(exit_fill.get("rebate") or exit_fill.get("rebate_cost") or 0)) if exit_fill else Decimal("0")
    fee_cost = entry_fee_cost + exit_fee_cost
    rebate = entry_rebate + exit_rebate
    entry_slippage_cost = Decimal(str(getattr(position, "entry_slippage_cost", Decimal("0")) or 0))
    if entry_slippage_cost <= 0 and params.slippage_bps > 0:
        entry_slippage_cost = (notional * _bps_fraction(params.slippage_bps)).copy_abs()
    exit_slippage_cost = Decimal(str(exit_fill.get("slippage_cost") or 0)) if exit_fill else ((point.price - exit_price) * close_size).copy_abs()
    if exit_slippage_cost <= 0 and params.slippage_bps > 0 and exit_reason != "settlement":
        exit_slippage_cost = (exit_notional * _bps_fraction(params.slippage_bps)).copy_abs()
    slippage_cost = entry_slippage_cost + exit_slippage_cost
    execution_cost = fee_cost + slippage_cost - rebate
    pnl = (exit_price - position.entry_price) * close_size - fee_cost + rebate
    return {
        "trade_id": f"T-{position.trade_index:04d}",
        "entry_order_id": position.entry_order_id,
        "exit_order_id": exit_order_id,
        "market_slug": run["market_slug"],
        "token_side": run["token_side"],
        "side": "LONG",
        "x_axis": x_axis,
        "entry_x": position.entry_x,
        "exit_x": point.x_value,
        "entry_price": position.entry_price,
        "exit_price": exit_price,
        "size": close_size,
        "notional": notional,
        "requested_notional": position.requested_notional,
        "filled_notional": position.filled_notional,
        "requested_size": position.size,
        "filled_size": close_size,
        "unfilled_size": max(Decimal("0"), position.size - close_size),
        "fill_pct": position.fill_pct,
        "fill_status": exit_fill.get("fill_status") if exit_fill else position.fill_status,
        "book_snapshot_id": (exit_fill.get("book_snapshot_id") or position.book_snapshot_id) if exit_fill else position.book_snapshot_id,
        "snapshot_version": (exit_fill.get("snapshot_version") or position.snapshot_version) if exit_fill else position.snapshot_version,
        "staleness_seconds": exit_fill.get("staleness_seconds") if exit_fill and exit_fill.get("staleness_seconds") is not None else position.staleness_seconds,
        "staleness_blocks": exit_fill.get("staleness_blocks") if exit_fill and exit_fill.get("staleness_blocks") is not None else position.staleness_blocks,
        "fill_probability": exit_fill.get("fill_probability") if exit_fill else position.fill_probability,
        "block_volume": exit_fill.get("block_volume") if exit_fill else position.block_volume,
        "trade_count": exit_fill.get("trade_count") if exit_fill else position.trade_count,
        "available_notional": exit_fill.get("available_notional") if exit_fill else position.available_notional,
        "execution_source": exit_fill.get("execution_source") if exit_fill else "unknown",
        "avg_fill_price": exit_price,
        "pnl": pnl.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "pnl_pct": _pct(pnl, notional),
        "holding_bars": max(1, point_index - position.entry_index),
        "exit_reason": exit_reason,
        "fee_cost": fee_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "rebate": rebate.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "entry_rebate": entry_rebate.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "exit_rebate": exit_rebate.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "slippage_cost": slippage_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "execution_cost": execution_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "entry_fee_cost": entry_fee_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "exit_fee_cost": exit_fee_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "entry_slippage_cost": entry_slippage_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "entry_fill_slices": list(getattr(position, "entry_fill_slices", []) or []),
        "exit_slippage_cost": exit_slippage_cost.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
    }


def _event(event_type: str, x_axis: str, x_value: int, trade_id: str | None, price: Decimal, message: str, *, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "x_axis": x_axis,
        "x_value": x_value,
        "trade_id": trade_id,
        "price": price,
        "message": message,
        "meta": meta or {},
    }


def _attach_markout_to_order(order: dict[str, Any], points: list[PricePoint], fill_index: int) -> None:
    if Decimal(str(order.get("filled_size") or 0)) <= 0:
        return
    avg_price_value = order.get("avg_fill_price")
    if avg_price_value is None:
        return
    try:
        avg_price = Decimal(str(avg_price_value))
    except Exception:
        return
    if not points or fill_index < 0 or fill_index >= len(points):
        return
    side = str(order.get("side") or "").upper()
    markouts: dict[str, str] = {}
    reference_prices: dict[str, str] = {}
    for horizon in MARKOUT_BAR_HORIZONS:
        target_index = fill_index + horizon
        if target_index >= len(points):
            continue
        future_price = Decimal(str(points[target_index].price))
        markout = _markout_value(side, avg_price, future_price)
        markouts[str(horizon)] = _decimal_text(markout.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP))
        reference_prices[str(horizon)] = _decimal_text(future_price.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP))
    seconds_markouts: dict[str, str] = {}
    seconds_reference_prices: dict[str, str] = {}
    seconds_reference_x: dict[str, int] = {}
    fill_timestamp = points[fill_index].timestamp
    if fill_timestamp is not None:
        for horizon in MARKOUT_SECOND_HORIZONS:
            target_timestamp = fill_timestamp + timedelta(seconds=horizon)
            target_point = _first_point_at_or_after(points, fill_index + 1, target_timestamp)
            if target_point is None:
                continue
            future_price = Decimal(str(target_point.price))
            markout = _markout_value(side, avg_price, future_price)
            seconds_markouts[str(horizon)] = _decimal_text(markout.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP))
            seconds_reference_prices[str(horizon)] = _decimal_text(future_price.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP))
            seconds_reference_x[str(horizon)] = int(target_point.x_value)
    if not markouts and not seconds_markouts:
        return
    meta = order.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        order["meta"] = meta
    if markouts:
        meta["markout_after_bars"] = markouts
        meta["markout_reference_prices"] = reference_prices
    if seconds_markouts:
        meta["markout_after_seconds"] = seconds_markouts
        meta["markout_seconds_reference_prices"] = seconds_reference_prices
        meta["markout_seconds_reference_x"] = seconds_reference_x
    meta["markout_basis"] = "positive_is_favorable_for_order_side"


def _markout_value(side: str, avg_price: Decimal, future_price: Decimal) -> Decimal:
    if side.startswith("SELL"):
        return avg_price - future_price
    return future_price - avg_price


def _markout_bucket(value: Decimal) -> str:
    if value < 0:
        return "adverse"
    if value > 0:
        return "favorable"
    return "flat"


def _first_point_at_or_after(points: list[PricePoint], start_index: int, target_timestamp: datetime) -> PricePoint | None:
    for point in points[max(0, start_index):]:
        if point.timestamp is not None and point.timestamp >= target_timestamp:
            return point
    return None


def _attach_missed_opportunity_to_order(order: dict[str, Any], points: list[PricePoint], start_index: int) -> None:
    if str(order.get("status") or "").upper() not in {"NO_FILL", "REJECTED", "EXPIRED"}:
        return
    requested_size = Decimal(str(order.get("requested_size") or 0))
    if requested_size <= 0 or not points:
        return
    start_index = max(0, min(int(start_index), len(points) - 1))
    future_points = points[start_index + 1:]
    if not future_points:
        return
    side = str(order.get("side") or "").upper()
    limit_price = Decimal(str(order.get("decision_price") or order.get("requested_price") or 0))
    if side.startswith("BUY"):
        best = max(future_points, key=lambda item: item.price)
        missed_price_move = Decimal(str(best.price)) - limit_price
        direction = "buy_no_fill_then_price_rose"
    elif side.startswith("SELL"):
        best = min(future_points, key=lambda item: item.price)
        missed_price_move = limit_price - Decimal(str(best.price))
        direction = "sell_no_fill_then_price_fell"
    else:
        return
    missed_price_move = max(Decimal("0"), missed_price_move).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    missed_notional = (missed_price_move * requested_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    meta = order.get("meta")
    if not isinstance(meta, dict):
        meta = {}
        order["meta"] = meta
    meta["missed_opportunity"] = {
        "basis": "diagnostic_only_not_a_fill",
        "direction": direction,
        "missed_price_move": _decimal_text(missed_price_move),
        "missed_notional": _decimal_text(missed_notional),
        "reference_price": _decimal_text(Decimal(str(best.price)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "reference_x": int(best.x_value),
        "bars_after_signal": max(0, future_points.index(best) + 1),
        "requested_size": _decimal_text(requested_size.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
        "limit_price": _decimal_text(limit_price.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)),
    }


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except Exception:
        return None


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip() or None


def _is_limit_replay_mode(params: BacktestParameters) -> bool:
    return is_orderfilled_cross_mode(params.execution_price_mode)


def _buy_limit_price(params: BacktestParameters) -> Decimal:
    value = params.buy_limit_price if params.buy_limit_price is not None else params.entry_threshold
    return min(max(Decimal("0"), Decimal(str(value))), Decimal("1")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _sell_limit_price(params: BacktestParameters, entry_price: Decimal) -> Decimal:
    if params.sell_limit_price is not None:
        value = params.sell_limit_price
    else:
        value = Decimal(str(entry_price)) * (Decimal("1") + Decimal(str(params.take_profit)))
    return min(max(Decimal("0"), Decimal(str(value))), Decimal("1")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _settlement_value(params: BacktestParameters, points: list[PricePoint]) -> Decimal | None:
    if params.settlement_value is not None:
        value = Decimal(str(params.settlement_value))
        if value in (Decimal("0"), Decimal("1")):
            return value
    if points:
        last_price = Decimal(str(points[-1].price))
        if last_price in (Decimal("0"), Decimal("1")):
            return last_price
    return None


def _after_latency(first: PricePoint, point: PricePoint, params: BacktestParameters, x_axis: str) -> bool:
    return _after_latency_value(first.x_value, point, params, x_axis)


def _after_latency_value(start_x: int, point: PricePoint, params: BacktestParameters, x_axis: str) -> bool:
    if x_axis == "block_number":
        return int(point.x_value) >= int(start_x) + int(params.latency_blocks or 0)
    return True


def _effective_submit_x(
    decision_x: int,
    points: list[PricePoint],
    decision_index: int,
    params: BacktestParameters,
    x_axis: str,
) -> int:
    block_submit_x = int(decision_x) + (int(params.latency_blocks or 0) if x_axis == "block_number" else 0)
    latency_seconds = max(Decimal("0"), Decimal(str(params.latency_seconds or 0)))
    if latency_seconds <= 0:
        return block_submit_x
    if x_axis != "block_number":
        return int((Decimal(str(decision_x)) + latency_seconds).to_integral_value(rounding=ROUND_CEILING))
    if not points:
        return block_submit_x
    decision_index = max(0, min(int(decision_index), len(points) - 1))
    decision_ts = points[decision_index].timestamp
    if decision_ts is None:
        return block_submit_x
    target_ts = decision_ts + timedelta(seconds=float(latency_seconds))
    for point in points[decision_index:]:
        if point.timestamp is not None and point.timestamp >= target_ts:
            return max(block_submit_x, int(point.x_value))
    return max(block_submit_x, int(points[-1].x_value) + 1)


def _sell_limit_can_fill(trade_price: Decimal, sell_limit: Decimal) -> bool:
    if sell_limit >= Decimal("0.98"):
        return False
    return Decimal(str(trade_price)) >= Decimal(str(sell_limit))


def _run_replay_events(run: dict[str, Any]) -> list[ReplayTradeEvent]:
    events: list[ReplayTradeEvent] = []
    for value in run.get("_orderfilled_replay_events") or []:
        event = _replay_event_from_any(value)
        if event is not None:
            events.append(event)
    events.sort(key=lambda item: item.event_sequence)
    return events


def _replay_event_from_any(value: Any) -> ReplayTradeEvent | None:
    if isinstance(value, ReplayTradeEvent):
        return value
    if not isinstance(value, dict):
        return None
    try:
        return ReplayTradeEvent(
            market_id=int(value.get("market_id") or value.get("marketId") or 0),
            token_id=str(value.get("token_id") or value.get("tokenId") or ""),
            block_number=int(value.get("block_number") or value.get("blockNumber") or 0),
            transaction_index=int(value.get("transaction_index") or value.get("transactionIndex") or 0),
            log_index=int(value.get("log_index") or value.get("logIndex") or 0),
            tx_hash=str(value.get("tx_hash") or value.get("txHash") or ""),
            trade_price=Decimal(str(value.get("trade_price") or value.get("tradePrice") or value.get("price") or 0)),
            size=Decimal(str(value.get("size") or 0)),
            maker=str(value.get("maker")) if value.get("maker") not in (None, "") else None,
            taker=str(value.get("taker")) if value.get("taker") not in (None, "") else None,
            side_code=str(value.get("side_code") or value.get("sideCode")) if value.get("side_code") or value.get("sideCode") else None,
        )
    except Exception:
        return None


def _limit_replay_submit_x(decision_x: int, params: BacktestParameters, x_axis: str) -> int:
    if x_axis == "block_number":
        return int(decision_x) + int(params.latency_blocks or 0)
    latency_seconds = max(Decimal("0"), Decimal(str(params.latency_seconds or 0)))
    return int((Decimal(str(decision_x)) + latency_seconds).to_integral_value(rounding=ROUND_CEILING))


def _limit_replay_events_through(
    events: list[ReplayTradeEvent],
    *,
    current_x: int,
    decision_x: int,
    submit_x: int | None = None,
    params: BacktestParameters,
    x_axis: str,
) -> list[ReplayTradeEvent]:
    submit_x = int(submit_x) if submit_x is not None else _limit_replay_submit_x(decision_x, params, x_axis)
    if x_axis != "block_number":
        return events
    return [
        event
        for event in events
        if int(event.block_number) <= int(current_x)
        and event.event_sequence > sequence_key(submit_x, 0, 0, "synthetic-submit")
    ]


def _block_bar_cross_context(point: PricePoint, side: str, limit_price: Decimal) -> dict[str, Any]:
    limit = Decimal(str(limit_price))
    side_text = str(side or "").upper()
    if side_text.startswith("BUY"):
        field = "low_price" if point.low_price is not None else "price"
        cross_price = Decimal(str(point.low_price if point.low_price is not None else point.price))
        crossed = cross_price <= limit
    else:
        field = "high_price" if point.high_price is not None else "price"
        cross_price = Decimal(str(point.high_price if point.high_price is not None else point.price))
        crossed = False if limit >= Decimal("0.98") else cross_price >= limit
    return {
        "block_bar_basis": "ohlcv_fallback_candidate",
        "block_bar_crossed": crossed,
        "block_bar_cross_field": field,
        "block_bar_cross_price": cross_price.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "block_bar_open_price": point.open_price,
        "block_bar_high_price": point.high_price,
        "block_bar_low_price": point.low_price,
        "block_bar_close_price": point.close_price,
        "block_bar_vwap_price": point.vwap_price,
    }


def _synthetic_block_bar_replay_events(
    point: PricePoint,
    side: str,
    *,
    order_role: str,
    limit_price: Decimal,
) -> tuple[list[ReplayTradeEvent], dict[str, Any]]:
    context = _block_bar_cross_context(point, side, limit_price)
    total_volume = max(Decimal("0"), Decimal(str(point.volume or 0)))
    side_volume = _block_bar_side_volume(point, side, order_role)
    effective_volume = side_volume if side_volume > 0 else total_volume
    rich_bar = bool(
        point.vwap_price is not None
        or point.open_price is not None
        or point.close_price is not None
        or point.buy_volume > 0
        or point.sell_volume > 0
    )
    if not rich_bar:
        price = Decimal(str(context.get("block_bar_cross_price") or point.price)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        event = ReplayTradeEvent(
            market_id=0,
            token_id="",
            block_number=int(point.x_value),
            transaction_index=0,
            log_index=max(1, int(point.first_log_index or 1)),
            tx_hash=f"synthetic-{int(point.x_value)}-cross",
            trade_price=price,
            size=effective_volume.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            side_code=_synthetic_block_bar_side_code(side, order_role),
        )
        return [event], {
            **context,
            "block_bar_execution_model": "single_cross_tick",
            "block_bar_synthetic_tick_count": 1,
            "block_bar_total_volume": total_volume,
            "block_bar_side_volume": side_volume,
            "block_bar_effective_volume": effective_volume,
            "block_bar_order_sequence": [_replay_event_to_meta(event)],
        }

    prices: list[tuple[str, Decimal]] = []
    for label, value in (
        ("open", point.open_price),
        ("high", point.high_price),
        ("low", point.low_price),
        ("vwap", point.vwap_price),
        ("close", point.close_price),
    ):
        if value is None:
            continue
        price = Decimal(str(value)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        if not prices or prices[-1][1] != price:
            prices.append((label, price))
    if not prices:
        prices.append(("cross", Decimal(str(context.get("block_bar_cross_price") or point.price)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)))

    tick_count = max(1, len(prices))
    base_size = (effective_volume / Decimal(tick_count)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if effective_volume > 0 else Decimal("0")
    remaining = effective_volume
    first_log = max(1, int(point.first_log_index or 1))
    last_log = max(first_log + tick_count - 1, int(point.last_log_index or first_log + tick_count - 1))
    log_step = max(1, (last_log - first_log) // max(1, tick_count - 1))
    events: list[ReplayTradeEvent] = []
    for index, (label, price) in enumerate(prices, start=1):
        if index == tick_count:
            size = max(Decimal("0"), remaining).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
        else:
            size = min(max(Decimal("0"), remaining), base_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
            remaining -= size
        events.append(
            ReplayTradeEvent(
                market_id=0,
                token_id="",
                block_number=int(point.x_value),
                transaction_index=0,
                log_index=first_log + (index - 1) * log_step,
                tx_hash=f"synthetic-{int(point.x_value)}-{index:03d}-{label}",
                trade_price=price,
                size=size,
                side_code=_synthetic_block_bar_side_code(side, order_role),
            )
        )
    return events, {
        **context,
        "block_bar_execution_model": "ohlcv_vwap_tick_sequence",
        "block_bar_synthetic_tick_count": len(events),
        "block_bar_total_volume": total_volume,
        "block_bar_side_volume": side_volume,
        "block_bar_effective_volume": effective_volume,
        "block_bar_order_sequence": [_replay_event_to_meta(event) for event in events],
    }


def _block_bar_side_volume(point: PricePoint, side: str, order_role: str) -> Decimal:
    side_text = str(side or "").upper()
    role = str(order_role or "").lower()
    buy_volume = max(Decimal("0"), Decimal(str(point.buy_volume or 0)))
    sell_volume = max(Decimal("0"), Decimal(str(point.sell_volume or 0)))
    if side_text.startswith("BUY"):
        return sell_volume if role == "maker" else buy_volume
    return buy_volume if role == "maker" else sell_volume


def _synthetic_block_bar_side_code(side: str, order_role: str) -> str:
    side_text = str(side or "").upper()
    role = str(order_role or "").lower()
    if side_text.startswith("BUY"):
        return "SELL" if role == "maker" else "BUY"
    return "BUY" if role == "maker" else "SELL"


def _replay_event_to_meta(event: ReplayTradeEvent) -> dict[str, Any]:
    return {
        "block_number": event.block_number,
        "transaction_index": event.transaction_index,
        "log_index": event.log_index,
        "tx_hash": event.tx_hash,
        "trade_price": event.trade_price,
        "size": event.size,
        "maker": event.maker,
        "taker": event.taker,
        "side_code": event.side_code,
        "canonical_fill_key": event.canonical_fill_key,
        "canonical_fill_key_kind": event.canonical_fill_key_kind,
    }


def _should_continue_resting_entry(fill: dict[str, Any], params: BacktestParameters, role: str) -> bool:
    if str(role or "").lower() != "maker":
        return False
    if _limit_replay_time_in_force(params) != "GTC":
        return False
    if not params.allow_partial_fill:
        return False
    return Decimal(str(fill.get("size") or 0)) > 0 and Decimal(str(fill.get("unfilled_size") or 0)) > Decimal("0.0000000001")


def _should_continue_resting_exit(fill: dict[str, Any], params: BacktestParameters, role: str) -> bool:
    return _should_continue_resting_entry(fill, params, role)


def _mark_resting_limit_fill(fill: dict[str, Any], *, x_value: int) -> dict[str, Any]:
    row = dict(fill)
    row["resting_order_continued"] = True
    row["resting_order_fill_updates"] = [_resting_fill_update(row, x_value=x_value)]
    notes = list(row.get("notes") or [])
    if "resting_order_partial" not in notes:
        notes.append("resting_order_partial")
    row["notes"] = notes
    return row


def _merge_resting_limit_fills(base: dict[str, Any], update: dict[str, Any], *, x_value: int) -> dict[str, Any]:
    merged = dict(base)
    requested_size = Decimal(str(merged.get("requested_size") or 0))
    base_size = Decimal(str(merged.get("size") or 0))
    update_size = Decimal(str(update.get("size") or 0))
    total_size = (base_size + update_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    base_notional = Decimal(str(merged.get("filled_notional") or 0))
    update_notional = Decimal(str(update.get("filled_notional") or 0))
    total_notional = (base_notional + update_notional).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    avg_price = (total_notional / total_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if total_size > 0 else Decimal(str(merged.get("avg_fill_price") or 0))
    unfilled = max(Decimal("0"), requested_size - total_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    merged.update(
        {
            "filled_notional": total_notional,
            "actual_fill_notional": total_notional,
            "expected_fill_notional": (Decimal(str(merged.get("expected_fill_notional") or 0)) + Decimal(str(update.get("expected_fill_notional") or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            "size": total_size,
            "filled_size": total_size,
            "actual_fill_size": total_size,
            "expected_fill_size": (Decimal(str(merged.get("expected_fill_size") or 0)) + Decimal(str(update.get("expected_fill_size") or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            "unfilled_size": unfilled,
            "fill_pct": (total_size * Decimal("100") / requested_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if requested_size else Decimal("0"),
            "fill_probability": (total_size * Decimal("100") / requested_size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP) if requested_size else Decimal("0"),
            "entry_price": avg_price,
            "exit_price": avg_price,
            "avg_fill_price": avg_price,
            "fee_cost": (Decimal(str(merged.get("fee_cost") or 0)) + Decimal(str(update.get("fee_cost") or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            "rebate": (Decimal(str(merged.get("rebate") or 0)) + Decimal(str(update.get("rebate") or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            "rebate_cost": (Decimal(str(merged.get("rebate_cost") or 0)) + Decimal(str(update.get("rebate_cost") or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            "slippage_cost": (Decimal(str(merged.get("slippage_cost") or 0)) + Decimal(str(update.get("slippage_cost") or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            "execution_cost": (Decimal(str(merged.get("execution_cost") or 0)) + Decimal(str(update.get("execution_cost") or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            "block_volume": (Decimal(str(merged.get("block_volume") or 0)) + Decimal(str(update.get("block_volume") or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            "trade_count": int(merged.get("trade_count") or 0) + int(update.get("trade_count") or 0),
            "available_notional": (Decimal(str(merged.get("available_notional") or 0)) + Decimal(str(update.get("available_notional") or 0))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
            "fill_status": "FILLED" if unfilled <= Decimal("0.0000000001") else "PARTIAL",
            "partial_fill": unfilled > Decimal("0.0000000001"),
            "rejected": False,
            "resting_order_continued": True,
        }
    )
    merged["candidate_events"] = list(merged.get("candidate_events") or []) + list(update.get("candidate_events") or [])
    merged["consumed_events"] = list(merged.get("consumed_events") or []) + list(update.get("consumed_events") or [])
    merged["resting_order_fill_updates"] = list(merged.get("resting_order_fill_updates") or []) + [_resting_fill_update(update, x_value=x_value)]
    merged["resting_order_final_x"] = int(x_value)
    notes = list(merged.get("notes") or [])
    if "resting_order_continued" not in notes:
        notes.append("resting_order_continued")
    merged["notes"] = notes
    return merged


def _resting_fill_update(fill: dict[str, Any], *, x_value: int) -> dict[str, Any]:
    return {
        "x_value": int(x_value),
        "filled_size": Decimal(str(fill.get("size") or 0)),
        "filled_notional": Decimal(str(fill.get("filled_notional") or 0)),
        "avg_fill_price": Decimal(str(fill.get("avg_fill_price") or 0)),
        "consumed_event_count": len(fill.get("consumed_events") or []),
    }


def _last_consumed_event_sequence(fill: dict[str, Any]) -> tuple[int, int, int, str, str] | None:
    events = fill.get("consumed_events") or []
    if not events:
        return None
    sequences = []
    for event in events:
        if not isinstance(event, dict):
            continue
        sequences.append(
            sequence_key(
                int(event.get("block_number") or 0),
                int(event.get("transaction_index") or 0),
                int(event.get("log_index") or 0),
                str(event.get("tx_hash") or ""),
                str(event.get("canonical_fill_key") or ""),
            )
        )
    return max(sequences) if sequences else None


def _filter_replay_events_after(events: list[ReplayTradeEvent], sequence: tuple[int, int, int, str, str] | None) -> list[ReplayTradeEvent]:
    if sequence is None:
        return list(events)
    return [event for event in events if event.event_sequence > sequence]


def _slice_open_position(position: OpenPosition, size: Decimal, *, trade_index: int | None = None) -> OpenPosition:
    sliced_size = max(Decimal("0"), Decimal(str(size))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    if position.size <= 0:
        ratio = Decimal("0")
    else:
        ratio = min(Decimal("1"), sliced_size / Decimal(str(position.size)))
    return OpenPosition(
        trade_index=int(trade_index if trade_index is not None else position.trade_index),
        entry_index=position.entry_index,
        entry_x=position.entry_x,
        entry_price=position.entry_price,
        size=sliced_size,
        requested_notional=(Decimal(str(position.requested_notional)) * ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        filled_notional=(Decimal(str(position.filled_notional)) * ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        fill_pct=position.fill_pct,
        fill_status=position.fill_status,
        book_snapshot_id=position.book_snapshot_id,
        snapshot_version=position.snapshot_version,
        staleness_seconds=position.staleness_seconds,
        staleness_blocks=position.staleness_blocks,
        avg_fill_price=position.avg_fill_price,
        fill_probability=position.fill_probability,
        block_volume=(Decimal(str(position.block_volume or 0)) * ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        trade_count=position.trade_count,
        available_notional=(Decimal(str(position.available_notional or 0)) * ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        entry_order_id=position.entry_order_id,
        entry_fee_cost=(Decimal(str(position.entry_fee_cost or 0)) * ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        entry_rebate=(Decimal(str(position.entry_rebate or 0)) * ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        entry_slippage_cost=(Decimal(str(position.entry_slippage_cost or 0)) * ratio).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
    )


def _limit_replay_crossed_at(
    events: list[ReplayTradeEvent],
    point: PricePoint,
    side: str,
    limit_price: Decimal,
    *,
    decision_x: int,
    submit_x: int | None = None,
    params: BacktestParameters,
    x_axis: str,
) -> bool:
    if not events:
        return bool(_block_bar_cross_context(point, side, limit_price)["block_bar_crossed"])
    current_events = _limit_replay_events_through(
        events,
        current_x=int(point.x_value),
        decision_x=decision_x,
        submit_x=submit_x,
        params=params,
        x_axis=x_axis,
    )
    limit = Decimal(str(limit_price))
    if side.startswith("BUY"):
        return any(Decimal(str(event.trade_price)) <= limit for event in current_events)
    if limit >= Decimal("0.98"):
        return False
    return any(Decimal(str(event.trade_price)) >= limit for event in current_events)


def _raw_replay_fallback_suppression_context(
    events: list[ReplayTradeEvent],
    point: PricePoint,
    side: str,
    limit_price: Decimal,
    *,
    decision_x: int,
    submit_x: int | None,
    params: BacktestParameters,
    x_axis: str,
) -> dict[str, Any] | None:
    """Explain when raw OrderFilled replay suppresses an OHLCV fallback fill."""

    if not events:
        return None
    block_bar_context = _block_bar_cross_context(point, side, limit_price)
    if not bool(block_bar_context.get("block_bar_crossed")):
        return None
    candidate_events = _limit_replay_events_through(
        events,
        current_x=int(point.x_value),
        decision_x=decision_x,
        submit_x=submit_x,
        params=params,
        x_axis=x_axis,
    )
    limit = Decimal(str(limit_price))
    if side.startswith("BUY"):
        raw_crossed = any(Decimal(str(event.trade_price)) <= limit for event in candidate_events)
    elif limit >= Decimal("0.98"):
        raw_crossed = False
    else:
        raw_crossed = any(Decimal(str(event.trade_price)) >= limit for event in candidate_events)
    if raw_crossed:
        return None
    return {
        "point": point,
        "raw_replay_available": True,
        "raw_replay_total_event_count": len(events),
        "raw_replay_candidate_count": len(candidate_events),
        "raw_replay_candidate_events": [_replay_event_to_meta(event) for event in candidate_events[:20]],
        "raw_replay_authoritative": True,
        "block_bar_fallback_allowed": False,
        "block_bar_fallback_suppressed_by_raw_replay": True,
        "synthetic_block_bar_crossed_without_raw_fill": True,
        "raw_replay_no_fill_reason": "raw_replay_authoritative_no_cross",
        **block_bar_context,
    }


def _limit_replay_fill(
    params: BacktestParameters,
    point: PricePoint,
    side: str,
    *,
    limit_price: Decimal,
    decision_x: int,
    signal_index: int | None = None,
    submit_x: int | None = None,
    target_size: Decimal | None = None,
    x_axis: str = "block_number",
    replay_events: list[ReplayTradeEvent] | None = None,
    allow_block_bar_fallback: bool = True,
) -> dict[str, Any]:
    price = Decimal(str(limit_price)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    profile = effective_execution_profile(params)
    role = _limit_replay_role(params)
    raw_cap_pct = max(Decimal("0"), Decimal(str(params.liquidity_cap_pct)))
    effective_cap_pct = _stress_liquidity_cap_pct(params, profile)
    target_notional = _target_notional(params)
    if target_size is None:
        requested_size = (target_notional / max(price, Decimal("0.0000000001"))).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    else:
        requested_size = max(Decimal("0"), Decimal(str(target_size)))
        target_notional = (requested_size * price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    submit_x = int(submit_x) if submit_x is not None else _limit_replay_submit_x(decision_x, params, x_axis)
    replay_events_available = bool(replay_events)
    block_bar_context = _block_bar_cross_context(point, side, price)
    source_events = _limit_replay_events_through(
        replay_events or [],
        current_x=int(point.x_value),
        decision_x=decision_x,
        submit_x=submit_x,
        params=params,
        x_axis=x_axis,
    )
    execution_source = "orderfilled_limit_replay_raw"
    raw_replay_fallback_suppressed = bool(replay_events_available and not source_events)
    if not source_events and not replay_events_available and allow_block_bar_fallback:
        source_events, block_bar_context = _synthetic_block_bar_replay_events(
            point,
            side,
            order_role=role,
            limit_price=price,
        )
        execution_source = "orderfilled_limit_replay_synthetic"
        raw_replay_fallback_suppressed = False
    time_in_force = _limit_replay_time_in_force(params)
    cancel_x, cancel_ack_x, cancel_fail_x = _limit_replay_cancel_window(params, submit_x=submit_x)
    strategy_intent = build_threshold_limit_intent(
        strategy_name=BACKTEST_STRATEGY_NAME,
        strategy_version=BACKTEST_STRATEGY_VERSION,
        signal_index=signal_index or 0,
        signal_x=int(decision_x),
        submit_x=int(submit_x),
        side="BUY_YES" if side.startswith("BUY") else "SELL_YES",
        signal_price=Decimal(str(point.price)),
        limit_price=price,
        target_notional=target_notional,
        target_size=requested_size,
        order_id="strategy-intent",
        reason="entry_limit" if side.startswith("BUY") else "exit_limit",
        time_in_force=time_in_force,
        cancel_x=cancel_x,
        cancel_ack_x=cancel_ack_x,
        cancel_fail_x=cancel_fail_x,
        liquidity_cap_pct=effective_cap_pct,
        role=role,
        order_type=_limit_replay_order_type(params),
        execution_model=execution_source,
        execution_profile=profile.name,
        metadata={
            "x_axis": x_axis,
            "decision_x": int(decision_x),
            "crossing_x": int(point.x_value),
            "block_bar_basis": block_bar_context["block_bar_basis"],
            "block_bar_crossed": bool(block_bar_context["block_bar_crossed"]),
            "block_bar_cross_field": block_bar_context["block_bar_cross_field"],
            "block_bar_cross_price": _decimal_text(block_bar_context["block_bar_cross_price"]),
            "raw_liquidity_cap_pct": _decimal_text(raw_cap_pct),
            "effective_liquidity_cap_pct": _decimal_text(effective_cap_pct),
            "cancel_after_blocks": int(params.cancel_after_blocks),
            "cancel_ack_delay_blocks": int(params.cancel_ack_delay_blocks),
            "cancel_fail": bool(params.cancel_fail),
        },
    )
    replay = replay_limit_order(strategy_intent.to_replay_order_intent(), source_events)
    replay_fill = replay.to_fill_dict()
    price_key = "entry_price" if side.startswith("BUY") else "exit_price"
    raw_avg_fill_price = Decimal(str(replay.avg_fill_price or price)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    actual_price = _limit_replay_stressed_price(raw_avg_fill_price, price, profile, side)
    filled_notional = (replay.filled_size * actual_price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    fee, rebate = _fee_rebate_for_notional(params, filled_notional, role)
    slippage_cost = _limit_replay_slippage_cost(raw_avg_fill_price, actual_price, replay.filled_size, side)
    candidate_events = replay_fill.get("candidate_events") or []
    candidate_volume = sum(
        (Decimal(str(event.get("size") or 0)) for event in candidate_events if isinstance(event, dict)),
        Decimal("0"),
    ).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    effective_available_notional = (replay.available_size * actual_price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    price_improvement = (price - actual_price if side.startswith("BUY") else actual_price - price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    expected_fill_size = replay.expected_fill_size
    expected_fill_notional = (expected_fill_size * actual_price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    participation_rate = _pct(target_notional, effective_available_notional) if effective_available_notional else Decimal("0")
    notes = list(replay_fill.get("notes") or (["buy_limit_crossed"] if side.startswith("BUY") else ["sell_limit_crossed"]))
    if raw_replay_fallback_suppressed and "block_bar_fallback_suppressed_by_raw_replay" not in notes:
        notes.append("block_bar_fallback_suppressed_by_raw_replay")
    return {
        "requested_notional": target_notional,
        "filled_notional": filled_notional,
        "expected_fill_notional": expected_fill_notional,
        "actual_fill_notional": filled_notional,
        "fill_pct": replay.fill_pct,
        "fill_probability": replay.fill_probability,
        "raw_fill_probability": replay.fill_pct,
        "effective_fill_probability": replay.fill_probability,
        "fill_probability_model": replay.fill_probability_model,
        "size": replay.filled_size,
        price_key: actual_price,
        "avg_fill_price": actual_price,
        "liquidity_cap_pct": raw_cap_pct,
        "effective_liquidity_cap_pct": effective_cap_pct,
        "min_fill_pct": max(Decimal("0"), Decimal(str(params.min_fill_pct))),
        "partial_fill": replay.status == "PARTIAL_FILLED",
        "rejected": replay.status in {"NO_FILL", "REJECTED", "EXPIRED", "CANCELED", "CANCEL_FAILED"},
        "fill_status": "PARTIAL" if replay.status == "PARTIAL_FILLED" else replay.status,
        "requested_size": requested_size,
        "expected_fill_size": expected_fill_size,
        "actual_fill_size": replay.filled_size,
        "filled_size": replay.filled_size,
        "unfilled_size": replay.unfilled_size,
        "block_volume": candidate_volume if candidate_events else max(Decimal("0"), Decimal(str(point.volume or 0))),
        "trade_count": len(candidate_events) if candidate_events else int(point.trade_count or 0),
        "available_notional": effective_available_notional if candidate_events else (max(Decimal("0"), Decimal(str(point.volume or 0))) * actual_price * effective_cap_pct / Decimal("100")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP),
        "participation_rate": participation_rate,
        "fee_cost": fee,
        "rebate": rebate,
        "rebate_cost": rebate,
        "slippage_cost": slippage_cost,
        "execution_cost": fee + slippage_cost - rebate,
        "execution_source": execution_source,
        "execution_profile": profile.name,
        "order_role": role,
        "time_in_force": time_in_force,
        "cancel_x": cancel_x,
        "cancel_ack_x": cancel_ack_x,
        "cancel_fail_x": cancel_fail_x,
        "cancel_fail": bool(params.cancel_fail),
        "execution_semantics": _limit_replay_execution_semantics(params),
        "fee_bps": _role_fee_bps(params, role),
        "rebate_bps": _role_rebate_bps(params, role),
        "limit_price": price,
        "raw_avg_fill_price": raw_avg_fill_price,
        "price_improvement_vs_limit": price_improvement,
        "adverse_slippage_cents": profile.adverse_slippage_cents,
        "fill_probability_haircut_pct": profile.fill_probability_haircut_pct,
        "stress_fill_haircut_pct": profile.fill_probability_haircut_pct,
        "crossing_price": block_bar_context["block_bar_cross_price"] if execution_source == "orderfilled_limit_replay_synthetic" else Decimal(str(point.price)),
        **block_bar_context,
        "block_bar_used_for_fill": execution_source == "orderfilled_limit_replay_synthetic",
        "raw_replay_available": replay_events_available,
        "raw_replay_total_event_count": len(replay_events or []),
        "raw_replay_candidate_count": len(source_events),
        "raw_replay_authoritative": replay_events_available,
        "block_bar_fallback_allowed": bool(allow_block_bar_fallback and not replay_events_available),
        "block_bar_fallback_suppressed_by_raw_replay": raw_replay_fallback_suppressed,
        "decision_x": int(decision_x),
        "submit_x": int(submit_x),
        "crossing_x": int(point.x_value),
        "strategy_intent": strategy_intent.as_dict(),
        "notes": notes,
        "fill_schedule": replay_fill.get("fill_schedule") or [],
        "candidate_events": candidate_events,
        "consumed_events": replay_fill.get("consumed_events") or [],
    }


def _limit_replay_no_fill(
    params: BacktestParameters,
    point: PricePoint,
    side: str,
    *,
    limit_price: Decimal,
    note: str,
    target_size: Decimal | None = None,
    raw_replay_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    profile = effective_execution_profile(params)
    role = _limit_replay_role(params)
    price = max(Decimal(str(limit_price)), Decimal("0.0000000001"))
    target_notional = _target_notional(params)
    if target_size is None:
        requested_size = (target_notional / price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    else:
        requested_size = max(Decimal("0"), Decimal(str(target_size)))
        target_notional = (requested_size * price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    raw_context = raw_replay_context if isinstance(raw_replay_context, Mapping) else {}
    block_bar_context = dict(raw_context) if raw_context else _block_bar_cross_context(point, side, price)
    block_bar_context.pop("point", None)
    notes = [note]
    if raw_context.get("block_bar_fallback_suppressed_by_raw_replay"):
        notes.extend(
            item
            for item in (
                "raw_replay_authoritative_no_fill",
                "block_bar_fallback_suppressed_by_raw_replay",
            )
            if item not in notes
        )
    return {
        "requested_notional": target_notional,
        "filled_notional": Decimal("0"),
        "expected_fill_notional": Decimal("0"),
        "actual_fill_notional": Decimal("0"),
        "fill_pct": Decimal("0"),
        "fill_probability": Decimal("0"),
        "size": Decimal("0"),
        "liquidity_cap_pct": max(Decimal("0"), Decimal(str(params.liquidity_cap_pct))),
        "effective_liquidity_cap_pct": _stress_liquidity_cap_pct(params, profile),
        "min_fill_pct": max(Decimal("0"), Decimal(str(params.min_fill_pct))),
        "partial_fill": False,
        "rejected": True,
        "fill_status": "NO_FILL",
        "requested_size": requested_size,
        "expected_fill_size": Decimal("0"),
        "actual_fill_size": Decimal("0"),
        "filled_size": Decimal("0"),
        "unfilled_size": requested_size,
        "block_volume": max(Decimal("0"), Decimal(str(point.volume or 0))),
        "trade_count": int(point.trade_count or 0),
        "available_notional": Decimal("0"),
        "participation_rate": Decimal("0"),
        "fee_cost": Decimal("0"),
        "rebate": Decimal("0"),
        "rebate_cost": Decimal("0"),
        "slippage_cost": Decimal("0"),
        "execution_cost": Decimal("0"),
        "execution_source": "orderfilled_limit_replay",
        "execution_profile": profile.name,
        "order_role": role,
        "time_in_force": _limit_replay_time_in_force(params),
        "execution_semantics": _limit_replay_execution_semantics(params),
        "fee_bps": _role_fee_bps(params, role),
        "rebate_bps": _role_rebate_bps(params, role),
        "adverse_slippage_cents": profile.adverse_slippage_cents,
        "fill_probability_haircut_pct": profile.fill_probability_haircut_pct,
        "stress_fill_haircut_pct": profile.fill_probability_haircut_pct,
        "limit_price": Decimal(str(limit_price)),
        "last_seen_price": Decimal(str(point.price)),
        **block_bar_context,
        "block_bar_used_for_fill": False,
        "raw_replay_available": bool(raw_context.get("raw_replay_available")),
        "raw_replay_total_event_count": int(raw_context.get("raw_replay_total_event_count") or 0),
        "raw_replay_candidate_count": int(raw_context.get("raw_replay_candidate_count") or 0),
        "raw_replay_authoritative": bool(raw_context.get("raw_replay_authoritative")),
        "block_bar_fallback_allowed": bool(raw_context.get("block_bar_fallback_allowed", True)) if raw_context else True,
        "block_bar_fallback_suppressed_by_raw_replay": bool(raw_context.get("block_bar_fallback_suppressed_by_raw_replay")),
        "synthetic_block_bar_crossed_without_raw_fill": bool(raw_context.get("synthetic_block_bar_crossed_without_raw_fill")),
        "raw_replay_no_fill_reason": raw_context.get("raw_replay_no_fill_reason"),
        "raw_replay_candidate_events": raw_context.get("raw_replay_candidate_events") or [],
        "notes": notes,
    }


def _settlement_fill(params: BacktestParameters, point: PricePoint, target_size: Decimal, settlement_value: Decimal) -> dict[str, Any]:
    requested_size = max(Decimal("0"), Decimal(str(target_size)))
    payoff = Decimal(str(settlement_value))
    filled_notional = (requested_size * payoff).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    return {
        "requested_notional": filled_notional,
        "filled_notional": filled_notional,
        "expected_fill_notional": filled_notional,
        "actual_fill_notional": filled_notional,
        "fill_pct": Decimal("100"),
        "fill_probability": Decimal("100"),
        "size": requested_size,
        "exit_price": payoff,
        "avg_fill_price": payoff,
        "liquidity_cap_pct": Decimal("0"),
        "min_fill_pct": Decimal("0"),
        "partial_fill": False,
        "rejected": False,
        "fill_status": "FILLED",
        "requested_size": requested_size,
        "expected_fill_size": requested_size,
        "actual_fill_size": requested_size,
        "filled_size": requested_size,
        "unfilled_size": Decimal("0"),
        "block_volume": max(Decimal("0"), Decimal(str(point.volume or 0))),
        "trade_count": int(point.trade_count or 0),
        "available_notional": filled_notional,
        "participation_rate": Decimal("100"),
        "fee_cost": Decimal("0"),
        "rebate": Decimal("0"),
        "rebate_cost": Decimal("0"),
        "slippage_cost": Decimal("0"),
        "execution_cost": Decimal("0"),
        "execution_source": "settlement_payoff",
        "execution_profile": params.execution_profile,
        "order_role": "settlement",
        "settlement_value": payoff,
        "notes": ["settlement_payoff"],
    }


def _pct(numerator: Decimal, denominator: Decimal) -> Decimal:
    if not denominator:
        return Decimal("0")
    return (numerator / denominator * Decimal("100")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _bps_fraction(value: Decimal) -> Decimal:
    return orderfilled_execution.bps_fraction(value)


def _role_fee_bps(params: BacktestParameters, role: str) -> Decimal:
    return orderfilled_execution.role_fee_bps(params, role)


def _role_rebate_bps(params: BacktestParameters, role: str) -> Decimal:
    return orderfilled_execution.role_rebate_bps(params, role)


def _limit_replay_role(params: BacktestParameters) -> str:
    role = effective_execution_profile(params).order_role
    if role in {"maker", "taker"}:
        return role
    return "maker"


def _limit_replay_time_in_force(params: BacktestParameters) -> str:
    return "FAK" if _limit_replay_role(params) == "taker" else "GTC"


def _limit_replay_cancel_window(params: BacktestParameters, *, submit_x: int) -> tuple[int | None, int | None, int | None]:
    cancel_after = max(0, int(params.cancel_after_blocks or 0))
    if cancel_after <= 0:
        return None, None, None
    cancel_x = int(submit_x) + cancel_after
    if bool(params.cancel_fail):
        return cancel_x, None, cancel_x + max(0, int(params.cancel_ack_delay_blocks or 0))
    return cancel_x, cancel_x + max(0, int(params.cancel_ack_delay_blocks or 0)), None


def _limit_replay_order_type(params: BacktestParameters) -> str:
    return "marketable_limit" if _limit_replay_role(params) == "taker" else "post_only_limit"


def _limit_replay_execution_semantics(params: BacktestParameters) -> str:
    return "taker_marketable_submit_window" if _limit_replay_role(params) == "taker" else "maker_post_only_wait_for_raw_fill"


def _limit_replay_should_attempt(role: str, point: PricePoint, submit_x: int, crossed: bool) -> bool:
    if str(role or "").lower() == "taker":
        return int(point.x_value) >= int(submit_x)
    return crossed


def _stress_liquidity_cap_pct(params: BacktestParameters, profile: Any | None = None) -> Decimal:
    profile = profile or effective_execution_profile(params)
    cap_pct = max(Decimal("0"), Decimal(str(params.liquidity_cap_pct)))
    haircut_pct = min(Decimal("100"), max(Decimal("0"), Decimal(str(profile.fill_probability_haircut_pct or 0))))
    return (cap_pct * (Decimal("100") - haircut_pct) / Decimal("100")).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _limit_replay_stressed_price(raw_price: Decimal, limit_price: Decimal, profile: Any, side: str) -> Decimal:
    raw = Decimal(str(raw_price))
    limit = Decimal(str(limit_price))
    stressed = apply_adverse_slippage(raw, profile, side)
    if str(side).upper().startswith("BUY"):
        stressed = min(stressed, limit)
    else:
        stressed = max(stressed, limit)
    return min(Decimal("0.9999999999"), max(Decimal("0"), stressed)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _limit_replay_slippage_cost(raw_price: Decimal, actual_price: Decimal, filled_size: Decimal, side: str) -> Decimal:
    raw = Decimal(str(raw_price))
    actual = Decimal(str(actual_price))
    size = max(Decimal("0"), Decimal(str(filled_size)))
    if str(side).upper().startswith("BUY"):
        unit_cost = max(Decimal("0"), actual - raw)
    else:
        unit_cost = max(Decimal("0"), raw - actual)
    return (unit_cost * size).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _fee_rebate_for_notional(params: BacktestParameters, notional: Decimal, role: str) -> tuple[Decimal, Decimal]:
    return orderfilled_execution.fee_rebate_for_notional(params, notional, role)


def _execution_price(price: Decimal, params: BacktestParameters, side: str) -> Decimal:
    return orderfilled_execution.execution_price(price, params, side)


def _target_notional(params: BacktestParameters) -> Decimal:
    return orderfilled_execution.target_notional(params)


def _fill_decision(
    params: BacktestParameters,
    point: PricePoint,
    run: dict[str, Any],
    side: str,
    *,
    target_size: Decimal | None = None,
) -> dict[str, Any]:
    mode = normalize_execution_price_mode(params.execution_price_mode, "ORDERFILLED")
    if mode == "ORDERFILLED":
        return _orderfilled_fill_decision(params, point, side, target_size=target_size)
    if is_orderfilled_cross_mode(mode):
        return _orderfilled_fill_decision(params, point, side, target_size=target_size)
    if is_prediction_l2_replay_v1_mode(mode):
        return _pml2_fill_decision(
            params,
            point,
            run,
            side,
            target_size=target_size,
        )
    if is_orderfilled_lob_mode(mode):
        orderfilled_fill = _orderfilled_fill_decision(params, point, side, target_size=target_size)
        l2_fill = _depth_fill_decision(params, point, run, side, target_size=orderfilled_fill.get("requested_size") or target_size)
        return combine_orderfilled_l2_execution(orderfilled_fill, l2_fill, signal_price=point.price, side=side)
    return _depth_fill_decision(params, point, run, side, target_size=target_size)


def _pml2_fill_decision(
    params: BacktestParameters,
    point: PricePoint,
    run: dict[str, Any],
    side: str,
    *,
    target_size: Decimal | Any | None = None,
) -> dict[str, Any]:
    session = run.get("_pml2_session")
    if not isinstance(session, ReplayExecutionSession):
        session = ReplayExecutionSession(
            run_id=str(run.get("run_id") or "pml2-backtest"),
            profile=params.execution_profile,
            audit_mode=params.pml2_audit_mode,
        )
        run["_pml2_session"] = session
        run["_pml2_ingested_snapshot_ids"] = set()
    signal_ts = point.timestamp
    requested_size = Decimal(str(target_size)) if target_size is not None else None
    if requested_size is None:
        requested_size = (
            _target_notional(params)
            / max(point.price, Decimal("0.0000000001"))
        ).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    requested_size = max(Decimal("0"), requested_size)
    if signal_ts is None:
        return _pml2_unavailable_fill(
            params,
            point,
            side,
            requested_size,
            "PREDICTION_L2_REPLAY_V1 requires timestamped strategy points",
        )
    token_context = run.get("_pml2_token_context")
    if not isinstance(token_context, Mapping):
        token_context = {}
    condition_id = str(token_context.get("condition_id") or run.get("condition_id") or "").lower()
    asset_id = str(token_context.get("token_id") or (run.get("meta") or {}).get("token_id") or "")
    market_id = str(token_context.get("market_id") or run.get("market_id") or run.get("market_slug") or "")
    outcome_text = str(token_context.get("token_side") or run.get("token_side") or "YES").upper()
    outcome = Pml2Outcome.NO if outcome_text == "NO" else Pml2Outcome.YES
    raw_side = (
        Pml2RawOrderSide.BUY
        if str(side).upper().startswith("BUY")
        else Pml2RawOrderSide.SELL
    )
    limit_price = (
        params.buy_limit_price or params.max_entry_price or point.price
        if raw_side == Pml2RawOrderSide.BUY
        else params.sell_limit_price or params.min_exit_price or point.price
    )
    limit_price = min(Decimal("0.9999999999"), max(Decimal("0.0000000001"), Decimal(str(limit_price))))
    order_role = str(params.order_role or "taker").lower()
    tif = (
        Pml2TimeInForce.GTC
        if order_role == "maker"
        else Pml2TimeInForce.IOC
        if params.allow_partial_fill
        else Pml2TimeInForce.FOK
    )
    configured_latency_seconds = max(
        Decimal("0"), Decimal(str(params.latency_seconds or 0))
    )
    entry_latency_override_ms = (
        int(configured_latency_seconds * Decimal("1000"))
        if configured_latency_seconds > 0
        else None
    )
    order_sequence = int(run.get("_pml2_order_sequence") or 0) + 1
    run["_pml2_order_sequence"] = order_sequence
    order_id = f"pml2-{order_sequence:08d}-{point.x_value}-{raw_side.value.lower()}"
    fee_bps = (
        params.taker_fee_bps
        if order_role != "maker" and params.taker_fee_bps is not None
        else params.maker_fee_bps
        if order_role == "maker" and params.maker_fee_bps is not None
        else params.fee_bps
    )
    order = Pml2OrderIntent(
        run_id=session.run_id,
        order_id=order_id,
        strategy_id=str(run.get("strategy_name") or BACKTEST_STRATEGY_NAME),
        condition_id=condition_id or f"unknown:{market_id}",
        market_id=market_id,
        asset_id=asset_id or f"unknown:{outcome.value}",
        outcome=outcome,
        side=raw_side,
        size=requested_size,
        limit_price=limit_price,
        tif=tif,
        signal_ts=signal_ts,
        observed_ts=signal_ts,
        submit_ts=signal_ts,
        post_only=order_role == "maker",
        # A zero frontend value means "use the selected execution profile".
        # Otherwise the UI default would silently erase realistic/strict
        # profile latency on every main-engine order.
        entry_latency_ms=entry_latency_override_ms,
        fee_rate=max(Decimal("0"), Decimal(str(fee_bps))) / Decimal("10000"),
    )
    execution_ts = signal_ts + timedelta(
        milliseconds=(
            session.profile.entry_latency_ms
            if order.entry_latency_ms is None
            else order.entry_latency_ms
        )
    )
    terminal_ts = execution_ts + timedelta(
        milliseconds=(0 if order_role == "maker" else session.profile.venue_delay_ms)
    )
    setup_error = str(run.get("_pml2_data_not_ready_reason") or "").strip()
    if setup_error:
        return _pml2_unavailable_fill(
            params,
            point,
            side,
            requested_size,
            setup_error,
        )
    native_provider = run.get("_pml2_native_provider")
    if isinstance(native_provider, XueNativeExecutionProvider):
        if not native_provider.prepare_at(terminal_ts):
            reason = str(
                native_provider.last_result.get("reason")
                or "XUE Native L2 is unavailable at order arrival"
            )
            return _pml2_unavailable_fill(
                params,
                point,
                side,
                requested_size,
                reason,
            )
    else:
        snapshot = _pml2_snapshot_for_execution(run, execution_ts)
        if snapshot is not None:
            ingested = run.setdefault("_pml2_ingested_snapshot_ids", set())
            snapshot_key = str(snapshot.snapshot_version or snapshot.snapshot_id)
            if snapshot_key not in ingested:
                event = legacy_snapshot_to_pml2(
                    snapshot,
                    condition_id=order.condition_id,
                    market_id=order.market_id,
                    outcome=order.outcome,
                    book_epoch=0,
                    local_ts=snapshot.timestamp,
                )
                session.ingest_snapshot(event)
                ingested.add(snapshot_key)
    session.submit_order(order)
    session.run(until=terminal_ts)
    result = session.result(order_id)
    return _pml2_result_to_fill(
        result,
        params=params,
        point=point,
        side=side,
    )


def _pml2_snapshot_for_execution(
    run: dict[str, Any], execution_ts: datetime
) -> BookSnapshot | None:
    provider = run.get("_pmxt_compact_book_provider")
    if provider is not None:
        return provider.snapshot_at(execution_ts)
    snapshots = [
        snapshot
        for snapshot in (
            snapshot_from_any(item) for item in run.get("_clob_snapshots") or []
        )
        if snapshot is not None
        and snapshot.timestamp is not None
        and snapshot.timestamp <= execution_ts
    ]
    return max(snapshots, key=lambda item: item.timestamp or datetime.min.replace(tzinfo=timezone.utc), default=None)


def _pml2_result_to_fill(
    result: Any,
    *,
    params: BacktestParameters,
    point: PricePoint,
    side: str,
) -> dict[str, Any]:
    filled_size = result.filled_size
    avg_price = result.avg_fill_price or Decimal("0")
    requested_size = result.order.size
    requested_notional = (requested_size * max(point.price, Decimal("0.0000000001"))).quantize(
        Decimal("0.0000000001"), rounding=ROUND_HALF_UP
    )
    filled_notional = sum((item.notional for item in result.fills), Decimal("0")).quantize(
        Decimal("0.0000000001"), rounding=ROUND_HALF_UP
    )
    fee = sum((item.fee for item in result.fills), Decimal("0")).quantize(
        Decimal("0.0000000001"), rounding=ROUND_HALF_UP
    )
    rebate = sum((item.rebate_accrual for item in result.fills), Decimal("0")).quantize(
        Decimal("0.0000000001"), rounding=ROUND_HALF_UP
    )
    slippage = (
        abs(avg_price - point.price) * filled_size
        if filled_size > 0
        else Decimal("0")
    ).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    fill_pct = _pct(filled_size, requested_size)
    price_key = "entry_price" if str(side).upper().startswith("BUY") else "exit_price"
    accepted = result.status.value in {"FILLED", "PARTIAL"}
    matches = [item.as_dict() for item in result.fills]
    return {
        "requested_notional": requested_notional,
        "filled_notional": filled_notional,
        "expected_fill_notional": filled_notional,
        "actual_fill_notional": filled_notional,
        "fill_pct": fill_pct,
        "fill_probability": Decimal("100") if filled_size > 0 else Decimal("0"),
        "size": filled_size,
        price_key: avg_price if filled_size > 0 else None,
        "avg_fill_price": avg_price,
        "liquidity_cap_pct": Decimal("100"),
        "min_fill_pct": max(Decimal("0"), params.min_fill_pct),
        "partial_fill": result.status.value == "PARTIAL",
        "rejected": not accepted,
        "fill_status": result.status.value,
        "requested_size": requested_size,
        "expected_fill_size": filled_size,
        "actual_fill_size": filled_size,
        "filled_size": filled_size,
        "unfilled_size": result.remaining_size,
        "available_notional": filled_notional,
        "participation_rate": Decimal("100"),
        "fee_cost": fee,
        "rebate": rebate,
        "rebate_cost": rebate,
        "slippage_cost": slippage,
        "execution_cost": fee + slippage - rebate,
        "execution_source": "prediction_l2_replay_v1",
        "execution_model": PREDICTION_L2_REPLAY_V1_MODE,
        "execution_profile": result.fills[0].profile if result.fills else params.execution_profile,
        "order_role": str(params.order_role or "taker").lower(),
        "book_snapshot_id": result.fills[0].snapshot_id if result.fills else None,
        "execution_audit": result.as_dict(),
        "pml2_replay_v1": {"execution_matches": matches},
        "notes": [result.reason, "no_future_orderfilled_gate"],
    }


def _pml2_unavailable_fill(
    params: BacktestParameters,
    point: PricePoint,
    side: str,
    requested_size: Decimal,
    reason: str,
) -> dict[str, Any]:
    requested_notional = requested_size * max(point.price, Decimal("0.0000000001"))
    return {
        "requested_notional": requested_notional,
        "filled_notional": Decimal("0"),
        "expected_fill_notional": Decimal("0"),
        "actual_fill_notional": Decimal("0"),
        "fill_pct": Decimal("0"),
        "fill_probability": Decimal("0"),
        "size": Decimal("0"),
        "entry_price" if str(side).upper().startswith("BUY") else "exit_price": None,
        "avg_fill_price": Decimal("0"),
        "partial_fill": False,
        "rejected": True,
        "fill_status": "DATA_NOT_READY",
        "requested_size": requested_size,
        "filled_size": Decimal("0"),
        "unfilled_size": requested_size,
        "fee_cost": Decimal("0"),
        "rebate": Decimal("0"),
        "rebate_cost": Decimal("0"),
        "slippage_cost": Decimal("0"),
        "execution_cost": Decimal("0"),
        "execution_source": "prediction_l2_replay_v1",
        "execution_model": PREDICTION_L2_REPLAY_V1_MODE,
        "execution_profile": params.execution_profile,
        "notes": [reason, "no_future_orderfilled_gate"],
    }


def _depth_fill_decision(
    params: BacktestParameters,
    point: PricePoint,
    run: dict[str, Any],
    side: str,
    *,
    target_size: Decimal | Any | None = None,
) -> dict[str, Any]:
    snapshots = _execution_snapshots_for_point(params, point, run)
    requested_size = Decimal(str(target_size)) if target_size is not None else None
    if requested_size is None:
        target_notional = _target_notional(params)
        reference_price = max(point.price, Decimal("0.0000000001"))
        requested_size = (target_notional / reference_price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    return simulate_l2_depth_execution(
        snapshots=snapshots,
        decision_block=point.x_value if run.get("price_source") == "orderfilled_block_close" else None,
        decision_timestamp=point.timestamp,
        side=side,  # type: ignore[arg-type]
        target_size=max(Decimal("0"), requested_size),
        signal_price=point.price,
        params=params,
        market_id=str(run.get("market_id") or run.get("market_slug") or ""),
        asset_id=str((run.get("meta") or {}).get("token_id") if isinstance(run.get("meta"), dict) else ""),
    )


def _execution_snapshots_for_point(
    params: BacktestParameters,
    point: PricePoint,
    run: dict[str, Any],
) -> list[BookSnapshot]:
    provider = run.get("_pmxt_compact_book_provider")
    if provider is not None and point.timestamp is not None:
        config = l2_config_from_params(params)
        execution_ts = point.timestamp + timedelta(milliseconds=config.submit_latency_ms)
        snapshot = provider.snapshot_at(execution_ts)
        return [snapshot] if snapshot is not None else []
    return [
        snapshot
        for snapshot in (snapshot_from_any(item) for item in run.get("_clob_snapshots") or [])
        if snapshot is not None
    ]


def _orderfilled_fill_decision(
    params: BacktestParameters,
    point: PricePoint,
    side: str,
    *,
    target_size: Decimal | None = None,
) -> dict[str, Any]:
    return orderfilled_execution.orderfilled_fill_decision(params, point, side, target_size=target_size)


def _force_close_fill(params: BacktestParameters, point: PricePoint, target_size: Decimal) -> dict[str, Any]:
    requested_size = max(Decimal("0"), Decimal(str(target_size)))
    profile = effective_execution_profile(params)
    raw_price = Decimal(str(point.price))
    exit_price = apply_adverse_slippage(_execution_price(raw_price, params, "exit"), profile, "SELL_YES")
    filled_notional = (requested_size * exit_price).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    fee_cost, rebate = _fee_rebate_for_notional(params, filled_notional, profile.order_role)
    slippage_cost = ((raw_price - exit_price) * requested_size).copy_abs().quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)
    return {
        "requested_notional": filled_notional,
        "filled_notional": filled_notional,
        "expected_fill_notional": filled_notional,
        "actual_fill_notional": filled_notional,
        "fill_pct": Decimal("100"),
        "fill_probability": Decimal("100"),
        "size": requested_size,
        "exit_price": exit_price,
        "avg_fill_price": exit_price,
        "liquidity_cap_pct": max(Decimal("0"), Decimal(str(params.liquidity_cap_pct))),
        "min_fill_pct": max(Decimal("0"), Decimal(str(params.min_fill_pct))),
        "partial_fill": False,
        "rejected": False,
        "fill_status": "FILLED",
        "requested_size": requested_size,
        "expected_fill_size": requested_size,
        "actual_fill_size": requested_size,
        "filled_size": requested_size,
        "unfilled_size": Decimal("0"),
        "block_volume": max(Decimal("0"), Decimal(str(point.volume or 0))),
        "trade_count": int(point.trade_count or 0),
        "available_notional": filled_notional,
        "participation_rate": Decimal("100"),
        "fee_cost": fee_cost,
        "rebate": rebate,
        "rebate_cost": rebate,
        "slippage_cost": slippage_cost,
        "execution_cost": fee_cost + slippage_cost - rebate,
        "execution_source": "forced_mark_to_market",
        "execution_profile": profile.name,
        "order_role": profile.order_role,
        "fee_bps": _role_fee_bps(params, profile.order_role),
        "rebate_bps": _role_rebate_bps(params, profile.order_role),
        "notes": ["force_close_marked_to_last_orderfilled_price"],
    }


def _ratio(numerator: int, denominator: int) -> Decimal:
    if not denominator:
        return Decimal("0")
    return Decimal(numerator) / Decimal(denominator)


def _money(value: Decimal) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,} USDC"


def _percent(value: Decimal) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)}%"


def _status(value: Decimal) -> str:
    if value > 0:
        return "positive"
    if value < 0:
        return "negative"
    return "neutral"
