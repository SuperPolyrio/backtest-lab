"""Bounded service contract for OrderFilled-only V2 replay.

The service accepts reviewed, structured taker orders. It never accepts code,
never reads LOB data, and never treats missing derived data as a clean no-fill.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from quant.backtest.orderfilled_v2_replay import (
    V2_EXECUTION_PROFILES,
    RequiredTradeWindow,
    TradeSliceLimitExceeded,
    V2TakerOrder,
    build_required_trade_windows,
    get_v2_execution_profile,
    load_v2_trade_slices_for_windows,
    merge_required_trade_windows,
    replay_v2_taker_orders_with_diagnostics,
    summarize_v2_results,
    with_v2_execution_profile,
)
from quant.core.db import ClickHouseClient

SCHEMA_VERSION = "fill-only-v2-replay-api-v1"
EXECUTION_MODEL = "orderfilled_v2_trade_tape_taker_participation"
DEFAULT_PROFILE = "conservative_trade_tape"
DEFAULT_HORIZON_BLOCKS = 2_000
DEFAULT_LOOKBACK_BLOCKS = 2_000
DEFAULT_LOOKBACK_SECONDS = 2_000
DEFAULT_MAX_ORDERS = 100
DEFAULT_MAX_ROWS_PER_WINDOW = 100_000
DEFAULT_MAX_TOTAL_TRADE_ROWS = 500_000
DEFAULT_MAX_HORIZON_BLOCKS = 100_000
DEFAULT_ANCHOR_MAX_DISTANCE_SECONDS = 120
LOB_FREE_PROFILE_NAMES = tuple(
    name for name in V2_EXECUTION_PROFILES if name != "lob_holdout_calibrated_fill_only"
)
REPLAY_FIELDS = frozenset(
    {
        "request_id",
        "requestId",
        "profile",
        "executionProfile",
        "source_max_block",
        "sourceMaxBlock",
        "default_horizon_blocks",
        "defaultHorizonBlocks",
        "default_lookback_blocks",
        "defaultLookbackBlocks",
        "default_horizon_seconds",
        "defaultHorizonSeconds",
        "default_lookback_seconds",
        "defaultLookbackSeconds",
        "merge_gap_blocks",
        "mergeGapBlocks",
        "merge_gap_seconds",
        "mergeGapSeconds",
        "max_rows_per_window",
        "maxRowsPerWindow",
        "matcher_backend",
        "matcherBackend",
        "orders",
    }
)
REPLAY_ALIAS_GROUPS = (
    ("request_id", "requestId"),
    ("profile", "executionProfile"),
    ("source_max_block", "sourceMaxBlock"),
    ("default_horizon_blocks", "defaultHorizonBlocks"),
    ("default_lookback_blocks", "defaultLookbackBlocks"),
    ("default_horizon_seconds", "defaultHorizonSeconds"),
    ("default_lookback_seconds", "defaultLookbackSeconds"),
    ("merge_gap_blocks", "mergeGapBlocks"),
    ("merge_gap_seconds", "mergeGapSeconds"),
    ("max_rows_per_window", "maxRowsPerWindow"),
    ("matcher_backend", "matcherBackend"),
)
ORDER_FIELDS = frozenset(
    {
        "order_id",
        "orderId",
        "market_id",
        "marketId",
        "asset_id",
        "assetId",
        "token_id",
        "tokenId",
        "side",
        "limit_price",
        "limitPrice",
        "size",
        "signal_block",
        "signalBlock",
        "signal_ts",
        "signalTs",
        "tif",
        "allow_partial_fill",
        "allowPartialFill",
        "signal_source_trade_id",
        "signalSourceTradeId",
        "signal_source_tx_hash",
        "signalSourceTxHash",
        "signal_source_log_indexes",
        "signalSourceLogIndexes",
        "horizon_blocks",
        "horizonBlocks",
        "horizon_seconds",
        "horizonSeconds",
        "latency_blocks",
        "latencyBlocks",
        "latency_seconds",
        "latencySeconds",
        "lookback_blocks",
        "lookbackBlocks",
        "lookback_seconds",
        "lookbackSeconds",
    }
)
ORDER_ALIAS_GROUPS = (
    ("order_id", "orderId"),
    ("market_id", "marketId"),
    ("asset_id", "assetId", "token_id", "tokenId"),
    ("limit_price", "limitPrice"),
    ("signal_block", "signalBlock"),
    ("signal_ts", "signalTs"),
    ("allow_partial_fill", "allowPartialFill"),
    ("signal_source_trade_id", "signalSourceTradeId"),
    ("signal_source_tx_hash", "signalSourceTxHash"),
    ("signal_source_log_indexes", "signalSourceLogIndexes"),
    ("horizon_blocks", "horizonBlocks"),
    ("horizon_seconds", "horizonSeconds"),
    ("latency_blocks", "latencyBlocks"),
    ("latency_seconds", "latencySeconds"),
    ("lookback_blocks", "lookbackBlocks"),
    ("lookback_seconds", "lookbackSeconds"),
)
ANCHOR_FIELDS = frozenset(
    {
        "request_id",
        "requestId",
        "signal_ts",
        "signalTs",
        "source_max_block",
        "sourceMaxBlock",
        "max_distance_seconds",
        "maxDistanceSeconds",
    }
)
ANCHOR_ALIAS_GROUPS = (
    ("request_id", "requestId"),
    ("signal_ts", "signalTs"),
    ("source_max_block", "sourceMaxBlock"),
    ("max_distance_seconds", "maxDistanceSeconds"),
)


class FillOnlyV2Error(RuntimeError):
    status_code = 500
    error_code = "FILL_ONLY_V2_ERROR"

    def as_dict(self) -> dict[str, Any]:
        return {"error": str(self), "error_code": self.error_code}


class FillOnlyV2RequestError(FillOnlyV2Error):
    status_code = 400
    error_code = "INVALID_FILL_ONLY_REQUEST"


class FillOnlyV2CoverageError(FillOnlyV2Error):
    status_code = 409
    error_code = "TRADE_TAPE_COVERAGE_GAP"


class FillOnlyV2UnavailableError(FillOnlyV2Error):
    status_code = 503
    error_code = "TRADE_TAPE_UNAVAILABLE"


class FillOnlyV2LimitError(FillOnlyV2Error):
    status_code = 413
    error_code = "FILL_ONLY_REQUEST_LIMIT_EXCEEDED"


class FillOnlyV2AnchorError(FillOnlyV2Error):
    status_code = 409
    error_code = "TRADE_TAPE_ANCHOR_UNRESOLVABLE"


@dataclass(frozen=True)
class TradeTapeCoverage:
    intervals: tuple[tuple[int, int], ...]
    min_block: int
    max_block: int
    build_tags: tuple[str, ...]
    receipt_count: int

    def contains(self, start_block: int, end_block: int) -> bool:
        start = min(int(start_block), int(end_block))
        end = max(int(start_block), int(end_block))
        return any(left <= start and end <= right for left, right in self.intervals)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_table": "trade_prints_one_sided",
            "receipt_table": "orderfilled_v2_build_chunks",
            "min_block": self.min_block,
            "max_block": self.max_block,
            "intervals": [
                {"from_block": left, "to_block": right}
                for left, right in self.intervals
            ],
            "build_tags": list(self.build_tags),
            "receipt_count": self.receipt_count,
        }


def list_fill_only_v2_profiles() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name in LOB_FREE_PROFILE_NAMES:
        profile = get_v2_execution_profile(name)
        probability = profile.orderfilled_probability_profile or {}
        probability_params_raw = probability.get("params")
        probability_params: Mapping[str, Any] = (
            probability_params_raw
            if isinstance(probability_params_raw, Mapping)
            else {}
        )
        probability_contract_raw = probability.get("data_contract")
        probability_contract: Mapping[str, Any] = (
            probability_contract_raw
            if isinstance(probability_contract_raw, Mapping)
            else {}
        )
        rows.append(
            {
                "name": profile.name,
                "participation_rate": str(profile.participation_rate),
                "latency_seconds": str(profile.latency.total_seconds()),
                "latency_blocks": profile.latency_blocks,
                "horizon_seconds": str(profile.horizon.total_seconds()),
                "horizon_blocks": profile.horizon_blocks,
                "price_buffer": str(profile.price_buffer),
                "exclude_signal_source_trade": profile.exclude_signal_source_trade,
                "allow_runtime_lob": False,
                "profile_activation": profile.profile_activation,
                "execution_stability_grade": profile.execution_stability_grade,
                "trade_side_evidence_mode": profile.trade_side_evidence_mode,
                "probability_enabled": bool(probability),
                "probability_model_version": probability.get("model_version"),
                "probability_profile_name": probability.get("profile_name")
                or probability.get("name"),
                "min_fill_probability": probability_params.get(
                    "min_probability", probability.get("min_probability")
                ),
                "hard_reject_below_probability": probability_params.get(
                    "hard_reject_below_probability",
                    probability.get("hard_reject_below_probability"),
                ),
                "capacity_variant": profile.orderfilled_capacity_variant,
                "fills_require_source_trade": probability_contract.get(
                    "fills_require_source_trade", True if probability else None
                ),
            }
        )
    return rows


def load_trade_tape_coverage(
    client: ClickHouseClient | None = None,
) -> TradeTapeCoverage:
    ch = client or ClickHouseClient()
    try:
        rows = ch.query_json_rows(
            """
            SELECT
                from_block,
                to_block,
                argMax(build_tag, created_at) AS build_tag
            FROM orderfilled_v2_build_chunks
            WHERE table_name = 'trade_prints_one_sided'
              AND status = 'inserted'
            GROUP BY from_block, to_block
            ORDER BY from_block ASC, to_block ASC
            """,
            timeout_seconds=30,
        )
    except Exception as exc:
        raise FillOnlyV2UnavailableError(
            f"cannot read trade-tape build receipts: {exc}"
        ) from exc
    if not rows:
        raise FillOnlyV2UnavailableError(
            "trade_prints_one_sided has no completed build receipts"
        )

    raw_intervals = sorted(
        (int(row["from_block"]), int(row["to_block"])) for row in rows
    )
    merged: list[tuple[int, int]] = []
    for start, end in raw_intervals:
        left, right = min(start, end), max(start, end)
        if merged and left <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], right))
        else:
            merged.append((left, right))
    tags = tuple(
        sorted(
            {str(row.get("build_tag") or "") for row in rows if row.get("build_tag")}
        )
    )
    return TradeTapeCoverage(
        intervals=tuple(merged),
        min_block=min(left for left, _ in merged),
        max_block=max(right for _, right in merged),
        build_tags=tags,
        receipt_count=len(rows),
    )


def run_fill_only_v2_replay(
    payload: Mapping[str, Any],
    *,
    client: ClickHouseClient | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise FillOnlyV2RequestError("request body must be a JSON object")
    _validate_object_fields(
        payload, REPLAY_FIELDS, REPLAY_ALIAS_GROUPS, context="request"
    )
    ch = client or ClickHouseClient()
    profile_name = str(
        _value(payload, "profile", "executionProfile", default=DEFAULT_PROFILE)
    ).strip()
    if profile_name not in LOB_FREE_PROFILE_NAMES:
        if profile_name == "lob_holdout_calibrated_fill_only":
            raise FillOnlyV2RequestError(
                "LOB-calibrated profiles are not exposed by the OrderFilled-only API"
            )
        raise FillOnlyV2RequestError(f"unsupported fill-only profile: {profile_name}")
    matcher_backend = (
        str(_value(payload, "matcher_backend", "matcherBackend", default="auto"))
        .strip()
        .lower()
    )
    if matcher_backend not in {"auto", "python", "rust"}:
        raise FillOnlyV2RequestError(
            "matcher_backend must be one of: auto, python, rust"
        )

    raw_orders = _value(payload, "orders", default=[])
    if not isinstance(raw_orders, list) or not raw_orders:
        raise FillOnlyV2RequestError("orders must be a non-empty list")
    max_orders = _env_int("POLYDATA_QUANT_FILL_ONLY_API_MAX_ORDERS", DEFAULT_MAX_ORDERS)
    if len(raw_orders) > max_orders:
        raise FillOnlyV2LimitError(
            f"orders exceeds server limit: {len(raw_orders)} > {max_orders}"
        )

    default_horizon_blocks = _bounded_int(
        _value(
            payload,
            "default_horizon_blocks",
            "defaultHorizonBlocks",
            default=DEFAULT_HORIZON_BLOCKS,
        ),
        "default_horizon_blocks",
        low=1,
        high=_env_int(
            "POLYDATA_QUANT_FILL_ONLY_API_MAX_HORIZON_BLOCKS",
            DEFAULT_MAX_HORIZON_BLOCKS,
        ),
    )
    default_lookback_blocks = _bounded_int(
        _value(
            payload,
            "default_lookback_blocks",
            "defaultLookbackBlocks",
            default=DEFAULT_LOOKBACK_BLOCKS,
        ),
        "default_lookback_blocks",
        low=0,
        high=_env_int(
            "POLYDATA_QUANT_FILL_ONLY_API_MAX_HORIZON_BLOCKS",
            DEFAULT_MAX_HORIZON_BLOCKS,
        ),
    )
    default_horizon_seconds_raw = _value(
        payload,
        "default_horizon_seconds",
        "defaultHorizonSeconds",
    )
    default_horizon_seconds = (
        _bounded_int(
            default_horizon_seconds_raw,
            "default_horizon_seconds",
            low=1,
            high=_env_int(
                "POLYDATA_QUANT_FILL_ONLY_API_MAX_HORIZON_SECONDS",
                DEFAULT_MAX_HORIZON_BLOCKS,
            ),
        )
        if default_horizon_seconds_raw is not None
        else None
    )
    default_lookback_seconds = _bounded_int(
        _value(
            payload,
            "default_lookback_seconds",
            "defaultLookbackSeconds",
            default=DEFAULT_LOOKBACK_SECONDS,
        ),
        "default_lookback_seconds",
        low=0,
        high=_env_int(
            "POLYDATA_QUANT_FILL_ONLY_API_MAX_HORIZON_SECONDS",
            DEFAULT_MAX_HORIZON_BLOCKS,
        ),
    )
    orders = [
        _parse_order(
            row,
            profile_name=profile_name,
            default_horizon_blocks=default_horizon_blocks,
            default_lookback_blocks=default_lookback_blocks,
            default_horizon_seconds=default_horizon_seconds,
            default_lookback_seconds=default_lookback_seconds,
        )
        for row in raw_orders
    ]
    order_ids = [order.order_id for order in orders]
    if len(set(order_ids)) != len(order_ids):
        raise FillOnlyV2RequestError("order_id values must be unique within one replay")

    coverage = load_trade_tape_coverage(ch)
    requested_pin = _optional_int(
        _value(payload, "source_max_block", "sourceMaxBlock"), "source_max_block"
    )
    source_max_block = coverage.max_block if requested_pin is None else requested_pin
    if source_max_block > coverage.max_block:
        raise FillOnlyV2CoverageError(
            f"source_max_block {source_max_block} exceeds derived trade-tape coverage {coverage.max_block}"
        )
    if source_max_block < coverage.min_block:
        raise FillOnlyV2CoverageError(
            f"source_max_block {source_max_block} precedes derived trade-tape coverage {coverage.min_block}"
        )

    windows = build_required_trade_windows(
        orders,
        source_min_block=coverage.min_block,
        source_max_block=source_max_block,
    )
    time_coverage_envelope = None
    time_windows = [window for window in windows if window.axis == "time"]
    if time_windows:
        time_coverage_envelope = _validate_time_window_coverage(
            time_windows,
            coverage=coverage,
            source_max_block=source_max_block,
            client=ch,
        )
    for window in windows:
        if window.axis == "block":
            _validate_window_coverage(window, coverage, source_max_block)
    merge_gap_blocks = _bounded_int(
        _value(payload, "merge_gap_blocks", "mergeGapBlocks", default=0),
        "merge_gap_blocks",
        low=0,
        high=10_000,
    )
    merge_gap_seconds = _bounded_int(
        _value(payload, "merge_gap_seconds", "mergeGapSeconds", default=0),
        "merge_gap_seconds",
        low=0,
        high=10_000,
    )
    merged_windows = merge_required_trade_windows(
        windows,
        merge_gap_blocks=merge_gap_blocks,
        merge_gap_seconds=merge_gap_seconds,
    )
    max_rows_per_window = _bounded_int(
        _value(
            payload,
            "max_rows_per_window",
            "maxRowsPerWindow",
            default=DEFAULT_MAX_ROWS_PER_WINDOW,
        ),
        "max_rows_per_window",
        low=1,
        high=_env_int(
            "POLYDATA_QUANT_FILL_ONLY_API_MAX_ROWS_PER_WINDOW",
            DEFAULT_MAX_ROWS_PER_WINDOW,
        ),
    )
    max_total_rows = _env_int(
        "POLYDATA_QUANT_FILL_ONLY_API_MAX_TOTAL_TRADE_ROWS",
        DEFAULT_MAX_TOTAL_TRADE_ROWS,
    )
    per_window_budget = max(1, max_total_rows // max(1, len(merged_windows)))
    effective_window_limit = min(max_rows_per_window, per_window_budget)

    try:
        loaded = load_v2_trade_slices_for_windows(
            windows,
            client=ch,
            merge_gap_blocks=merge_gap_blocks,
            merge_gap_seconds=merge_gap_seconds,
            limit_per_window=effective_window_limit,
            reject_truncated_windows=True,
        )
    except TradeSliceLimitExceeded as exc:
        raise FillOnlyV2LimitError(str(exc)) from exc
    if loaded.rows_loaded > max_total_rows:
        raise FillOnlyV2LimitError(
            f"loaded trade rows exceed server limit: {loaded.rows_loaded} > {max_total_rows}"
        )

    try:
        results, ledger, diagnostics = replay_v2_taker_orders_with_diagnostics(
            orders,
            loaded.trades,
            backend=matcher_backend,  # type: ignore[arg-type]
        )
    except ValueError as exc:
        raise FillOnlyV2RequestError(str(exc)) from exc
    request_id = (
        str(_value(payload, "request_id", "requestId", default="")).strip() or None
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "execution_model": EXECUTION_MODEL,
        "profile": profile_name,
        "source_table": "trade_prints_one_sided",
        "source_max_block": source_max_block,
        "default_horizon_blocks": default_horizon_blocks,
        "default_lookback_blocks": default_lookback_blocks,
        "default_horizon_seconds": default_horizon_seconds,
        "default_lookback_seconds": default_lookback_seconds,
        "merge_gap_blocks": merge_gap_blocks,
        "merge_gap_seconds": merge_gap_seconds,
        "max_rows_per_window": effective_window_limit,
        "matcher_backend": matcher_backend,
        "orders": [_order_manifest(order) for order in orders],
    }
    manifest_hash = hashlib.sha256(
        json.dumps(_json_ready(manifest), sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "manifest_hash": manifest_hash,
        "execution_model": EXECUTION_MODEL,
        "execution_grade": "trade_tape_participation",
        "uses_lob_data": False,
        "profile": profile_name,
        "source_max_block": source_max_block,
        "source_coverage": coverage.as_dict(),
        "time_coverage_envelope": _json_ready(time_coverage_envelope),
        "trade_slice_load": _json_ready(loaded.as_dict()),
        "match_diagnostics": _json_ready(diagnostics.as_dict()),
        "summary": _json_ready(summarize_v2_results(results)),
        "orders": [_json_ready(result.as_dict()) for result in results],
        "capacity_ledger": ledger.as_dict(),
        "market_window_capacity_ledger": ledger.market_window_as_dict(),
        "manifest": _json_ready(manifest),
    }


def resolve_fill_only_v2_anchor(
    payload: Mapping[str, Any],
    *,
    client: ClickHouseClient | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise FillOnlyV2RequestError("request body must be a JSON object")
    _validate_object_fields(
        payload, ANCHOR_FIELDS, ANCHOR_ALIAS_GROUPS, context="request"
    )
    signal_ts = _parse_datetime(_value(payload, "signal_ts", "signalTs"), "signal_ts")
    max_distance_seconds = _bounded_int(
        _value(
            payload,
            "max_distance_seconds",
            "maxDistanceSeconds",
            default=DEFAULT_ANCHOR_MAX_DISTANCE_SECONDS,
        ),
        "max_distance_seconds",
        low=0,
        high=3_600,
    )
    ch = client or ClickHouseClient()
    coverage = load_trade_tape_coverage(ch)
    requested_pin = _optional_int(
        _value(payload, "source_max_block", "sourceMaxBlock"), "source_max_block"
    )
    source_max_block = coverage.max_block if requested_pin is None else requested_pin
    if source_max_block > coverage.max_block:
        raise FillOnlyV2CoverageError(
            f"source_max_block {source_max_block} exceeds derived trade-tape coverage {coverage.max_block}"
        )

    timestamp_sql = _quote_ch(signal_ts.isoformat())
    try:
        rows = ch.query_json_rows(
            f"""
            (
                SELECT
                    'before' AS relation,
                    trade_id,
                    block_number,
                    block_time
                FROM trade_prints_one_sided
                PREWHERE block_number BETWEEN {coverage.min_block} AND {source_max_block}
                WHERE block_time BETWEEN
                    subtractSeconds(parseDateTime64BestEffort({timestamp_sql}), {max_distance_seconds})
                    AND parseDateTime64BestEffort({timestamp_sql})
                ORDER BY block_time DESC, block_number DESC, tx_index DESC, trade_id DESC
                LIMIT 1
            )
            UNION ALL
            (
                SELECT
                    'after' AS relation,
                    trade_id,
                    block_number,
                    block_time
                FROM trade_prints_one_sided
                PREWHERE block_number BETWEEN {coverage.min_block} AND {source_max_block}
                WHERE block_time BETWEEN
                    parseDateTime64BestEffort({timestamp_sql})
                    AND addSeconds(parseDateTime64BestEffort({timestamp_sql}), {max_distance_seconds})
                ORDER BY block_time ASC, block_number ASC, tx_index ASC, trade_id ASC
                LIMIT 1
            )
            """,
            timeout_seconds=30,
        )
    except Exception as exc:
        raise FillOnlyV2UnavailableError(
            f"cannot resolve trade-tape anchor: {exc}"
        ) from exc
    by_relation = {str(row.get("relation") or ""): row for row in rows}
    before = _anchor_evidence(by_relation.get("before"))
    after = _anchor_evidence(by_relation.get("after"))
    if before is None or after is None:
        raise FillOnlyV2AnchorError(
            "signal_ts cannot be bracketed by trade_prints_one_sided within the pinned source coverage"
        )
    before_distance = max(0.0, (signal_ts - before["block_time"]).total_seconds())
    after_distance = max(0.0, (after["block_time"] - signal_ts).total_seconds())
    if before_distance > max_distance_seconds or after_distance > max_distance_seconds:
        raise FillOnlyV2AnchorError(
            "nearest trade-tape anchors exceed max_distance_seconds "
            f"before={before_distance:.3f} after={after_distance:.3f} max={max_distance_seconds}"
        )
    before_block = int(before["block_number"])
    after_block = int(after["block_number"])
    if after_block < before_block:
        raise FillOnlyV2AnchorError(
            f"trade-tape anchor blocks are not monotonic: before={before_block} after={after_block}"
        )
    if before["block_time"] == after["block_time"] or before_block == after_block:
        anchor_block = min(before_block, after_block)
        method = "exact_trade_time"
    else:
        total_seconds = (after["block_time"] - before["block_time"]).total_seconds()
        if total_seconds <= 0:
            raise FillOnlyV2AnchorError("trade-tape anchor times are not monotonic")
        elapsed_seconds = (signal_ts - before["block_time"]).total_seconds()
        anchor_block = before_block + int(
            (after_block - before_block) * elapsed_seconds / total_seconds
        )
        method = "interpolated_trade_bracket"
    if (
        not coverage.contains(anchor_block, anchor_block)
        or anchor_block > source_max_block
    ):
        raise FillOnlyV2CoverageError(
            f"resolved anchor block {anchor_block} is outside pinned trade-tape coverage"
        )

    request_id = (
        str(_value(payload, "request_id", "requestId", default="")).strip() or None
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "uses_lob_data": False,
        "source_table": "trade_prints_one_sided",
        "source_max_block": source_max_block,
        "signal_ts": signal_ts.isoformat(),
        "anchor_block": anchor_block,
        "resolution_method": method,
        "max_distance_seconds": max_distance_seconds,
        "before_distance_seconds": str(before_distance),
        "after_distance_seconds": str(after_distance),
        "coverage_eligible": True,
        "bracket": {
            "before": _json_ready(before),
            "after": _json_ready(after),
        },
    }


def _validate_time_window_coverage(
    windows: list[RequiredTradeWindow],
    *,
    coverage: TradeTapeCoverage,
    source_max_block: int,
    client: ClickHouseClient,
) -> dict[str, Any]:
    requested_start = min(
        window.start_ts for window in windows if window.start_ts is not None
    )
    requested_end = max(
        window.end_ts for window in windows if window.end_ts is not None
    )
    start_sql = _quote_ch(requested_start.isoformat())
    end_sql = _quote_ch(requested_end.isoformat())
    try:
        rows = client.query_json_rows(
            f"""
            (
                SELECT
                    'coverage_start' AS relation,
                    trade_id,
                    block_number,
                    block_time
                FROM trade_prints_one_sided
                PREWHERE block_number BETWEEN {coverage.min_block} AND {source_max_block}
                WHERE block_time <= parseDateTime64BestEffort({start_sql})
                ORDER BY block_time DESC, block_number DESC, tx_index DESC, trade_id DESC
                LIMIT 1
            )
            UNION ALL
            (
                SELECT
                    'coverage_end' AS relation,
                    trade_id,
                    block_number,
                    block_time
                FROM trade_prints_one_sided
                PREWHERE block_number BETWEEN {coverage.min_block} AND {source_max_block}
                WHERE block_time >= parseDateTime64BestEffort({end_sql})
                ORDER BY block_time ASC, block_number ASC, tx_index ASC, trade_id ASC
                LIMIT 1
            )
            """,
            timeout_seconds=30,
        )
    except Exception as exc:
        raise FillOnlyV2UnavailableError(
            f"cannot verify timestamp trade-tape coverage: {exc}"
        ) from exc

    by_relation = {str(row.get("relation") or ""): row for row in rows}
    before = _anchor_evidence(by_relation.get("coverage_start"))
    after = _anchor_evidence(by_relation.get("coverage_end"))
    if before is None or after is None:
        raise FillOnlyV2CoverageError(
            "timestamp order windows are not enclosed by real trade-tape evidence "
            "inside the pinned source coverage"
        )
    before_block = int(before["block_number"])
    after_block = int(after["block_number"])
    if (
        before["block_time"] > requested_start
        or after["block_time"] < requested_end
        or after_block < before_block
        or after_block > source_max_block
        or not coverage.contains(before_block, after_block)
    ):
        raise FillOnlyV2CoverageError(
            "completed trade-tape receipts do not continuously enclose the timestamp order windows"
        )
    return {
        "validation_method": "run_level_trade_time_envelope",
        "is_order_anchor": False,
        "requested_start_ts": requested_start,
        "requested_end_ts": requested_end,
        "before": before,
        "after": after,
    }


def _parse_order(
    row: Any,
    *,
    profile_name: str,
    default_horizon_blocks: int,
    default_lookback_blocks: int,
    default_horizon_seconds: int | None,
    default_lookback_seconds: int,
) -> V2TakerOrder:
    if not isinstance(row, Mapping):
        raise FillOnlyV2RequestError("each order must be a JSON object")
    _validate_object_fields(row, ORDER_FIELDS, ORDER_ALIAS_GROUPS, context="order")
    order_id = str(_value(row, "order_id", "orderId", default="")).strip()
    if not order_id or len(order_id) > 128:
        raise FillOnlyV2RequestError(
            "order_id is required and must be at most 128 characters"
        )
    market_id = _bounded_int(
        _value(row, "market_id", "marketId"),
        f"{order_id}.market_id",
        low=1,
        high=2**63 - 1,
    )
    asset_id = (
        str(_value(row, "asset_id", "assetId", "token_id", "tokenId", default=""))
        .strip()
        .lower()
    )
    if not asset_id or len(asset_id) > 256:
        raise FillOnlyV2RequestError(
            f"{order_id}.asset_id is required and must be at most 256 characters"
        )
    side = str(_value(row, "side", default="")).strip().upper()
    if side not in {"BUY", "SELL"}:
        raise FillOnlyV2RequestError(f"{order_id}.side must be BUY or SELL")
    limit_price = _decimal(
        _value(row, "limit_price", "limitPrice"), f"{order_id}.limit_price"
    )
    if limit_price <= 0 or limit_price > 1:
        raise FillOnlyV2RequestError(f"{order_id}.limit_price must be in (0, 1]")
    size = _decimal(_value(row, "size"), f"{order_id}.size")
    if size <= 0:
        raise FillOnlyV2RequestError(f"{order_id}.size must be positive")
    signal_block = _optional_int(
        _value(row, "signal_block", "signalBlock"),
        f"{order_id}.signal_block",
    )
    signal_ts = _parse_datetime(
        _value(row, "signal_ts", "signalTs"), f"{order_id}.signal_ts"
    )
    tif = str(_value(row, "tif", default="GTC")).strip().upper()
    if tif not in {"GTC", "GTD", "IOC", "FOK", "FAK"}:
        raise FillOnlyV2RequestError(f"{order_id}.tif is invalid")
    base = V2TakerOrder(
        order_id=order_id,
        market_id=market_id,
        asset_id=asset_id,
        side=side,  # type: ignore[arg-type]
        limit_price=limit_price,
        size=size,
        signal_block=signal_block,
        signal_ts=signal_ts,
        tif=tif,  # type: ignore[arg-type]
        allow_partial_fill=_bool(
            _value(row, "allow_partial_fill", "allowPartialFill", default=True)
        ),
        signal_source_trade_id=_optional_text(
            _value(row, "signal_source_trade_id", "signalSourceTradeId")
        ),
        signal_source_tx_hash=_optional_text(
            _value(row, "signal_source_tx_hash", "signalSourceTxHash")
        ),
        signal_source_log_indexes=tuple(
            int(item)
            for item in (
                _value(
                    row,
                    "signal_source_log_indexes",
                    "signalSourceLogIndexes",
                    default=[],
                )
                or []
            )
        ),
    )
    profiled = with_v2_execution_profile(base, profile_name)
    explicit_horizon = _value(row, "horizon_blocks", "horizonBlocks")
    horizon_blocks = (
        _bounded_int(
            explicit_horizon,
            f"{order_id}.horizon_blocks",
            low=1,
            high=DEFAULT_MAX_HORIZON_BLOCKS,
        )
        if explicit_horizon is not None
        else profiled.horizon_blocks or default_horizon_blocks
    )
    explicit_latency = _value(row, "latency_blocks", "latencyBlocks")
    latency_blocks = (
        _bounded_int(
            explicit_latency,
            f"{order_id}.latency_blocks",
            low=0,
            high=DEFAULT_MAX_HORIZON_BLOCKS,
        )
        if explicit_latency is not None
        else profiled.latency_blocks
    )
    explicit_horizon_seconds = _value(row, "horizon_seconds", "horizonSeconds")
    horizon_seconds = (
        _bounded_int(
            explicit_horizon_seconds,
            f"{order_id}.horizon_seconds",
            low=1,
            high=DEFAULT_MAX_HORIZON_BLOCKS,
        )
        if explicit_horizon_seconds is not None
        else default_horizon_seconds
    )
    explicit_latency_seconds = _value(row, "latency_seconds", "latencySeconds")
    latency_seconds = (
        _bounded_int(
            explicit_latency_seconds,
            f"{order_id}.latency_seconds",
            low=0,
            high=DEFAULT_MAX_HORIZON_BLOCKS,
        )
        if explicit_latency_seconds is not None
        else None
    )
    lookback_blocks = _bounded_int(
        _value(
            row, "lookback_blocks", "lookbackBlocks", default=default_lookback_blocks
        ),
        f"{order_id}.lookback_blocks",
        low=0,
        high=DEFAULT_MAX_HORIZON_BLOCKS,
    )
    lookback_seconds = _bounded_int(
        _value(
            row,
            "lookback_seconds",
            "lookbackSeconds",
            default=default_lookback_seconds,
        ),
        f"{order_id}.lookback_seconds",
        low=0,
        high=DEFAULT_MAX_HORIZON_BLOCKS,
    )
    return replace(
        profiled,
        horizon_blocks=horizon_blocks,
        latency_blocks=latency_blocks,
        horizon=(
            timedelta(seconds=horizon_seconds)
            if horizon_seconds is not None
            else profiled.horizon
        ),
        latency=(
            timedelta(seconds=latency_seconds)
            if latency_seconds is not None
            else profiled.latency
        ),
        trade_slice_lookback_blocks=lookback_blocks,
        trade_slice_lookback=timedelta(seconds=lookback_seconds),
    )


def _validate_window_coverage(
    window: RequiredTradeWindow,
    coverage: TradeTapeCoverage,
    source_max_block: int,
) -> None:
    if window.axis != "block":
        raise ValueError("block coverage validator received a timestamp window")
    assert window.start_block is not None and window.end_block is not None
    if window.end_block > source_max_block:
        raise FillOnlyV2CoverageError(
            f"order window ends at block {window.end_block}, beyond pinned source_max_block {source_max_block}"
        )
    if not coverage.contains(window.start_block, window.end_block):
        raise FillOnlyV2CoverageError(
            "trade-tape build receipts do not fully cover "
            f"market_id={window.market_id} asset_id={window.asset_id} "
            f"from_block={window.start_block} to_block={window.end_block}"
        )


def _order_manifest(order: V2TakerOrder) -> dict[str, Any]:
    return {
        "order_id": order.order_id,
        "market_id": order.market_id,
        "asset_id": order.asset_id,
        "side": order.side,
        "limit_price": str(order.limit_price),
        "size": str(order.size),
        "signal_block": order.signal_block,
        "signal_ts": order.signal_ts.isoformat() if order.signal_ts else None,
        "arrival_block": order.arrival_block,
        "arrival_ts": order.arrival_ts.isoformat() if order.arrival_ts else None,
        "deadline_block": order.deadline_block,
        "deadline_ts": order.deadline_ts.isoformat() if order.deadline_ts else None,
        "trade_slice_lookback_blocks": order.trade_slice_lookback_blocks,
        "trade_slice_lookback_seconds": str(order.trade_slice_lookback.total_seconds()),
        "tif": order.tif,
        "allow_partial_fill": order.allow_partial_fill,
        "participation_rate": str(order.participation_rate),
        "price_buffer": str(order.price_buffer),
        "exclude_signal_source_trade": order.exclude_signal_source_trade,
        "trade_side_evidence_mode": order.trade_side_evidence_mode,
    }


def _value(payload: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in payload and payload[name] is not None:
            return payload[name]
    return default


def _validate_object_fields(
    payload: Mapping[str, Any],
    allowed_fields: frozenset[str],
    alias_groups: tuple[tuple[str, ...], ...],
    *,
    context: str,
) -> None:
    unknown = sorted(str(key) for key in payload if str(key) not in allowed_fields)
    if unknown:
        raise FillOnlyV2RequestError(
            f"{context} contains unknown fields: {', '.join(unknown)}"
        )
    conflicts = [
        tuple(name for name in group if name in payload) for group in alias_groups
    ]
    conflicts = [group for group in conflicts if len(group) > 1]
    if conflicts:
        rendered = "; ".join(", ".join(group) for group in conflicts)
        raise FillOnlyV2RequestError(
            f"{context} contains conflicting aliases: {rendered}"
        )


def _anchor_evidence(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return {
        "trade_id": str(row.get("trade_id") or ""),
        "block_number": int(row["block_number"]),
        "block_time": _parse_datetime(row["block_time"], "block_time"),
    }


def _bounded_int(value: Any, name: str, *, low: int, high: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise FillOnlyV2RequestError(f"{name} must be an integer") from exc
    if parsed < low or parsed > high:
        raise FillOnlyV2RequestError(f"{name} must be between {low} and {high}")
    return parsed


def _optional_int(value: Any, name: str) -> int | None:
    if value in (None, ""):
        return None
    return _bounded_int(value, name, low=0, high=2**63 - 1)


def _decimal(value: Any, name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except Exception as exc:
        raise FillOnlyV2RequestError(f"{name} must be a decimal") from exc
    if not parsed.is_finite():
        raise FillOnlyV2RequestError(f"{name} must be finite")
    return parsed


def _parse_datetime(value: Any, name: str) -> datetime:
    if value in (None, ""):
        raise FillOnlyV2RequestError(f"{name} is required")
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise FillOnlyV2RequestError(
                f"{name} must be an ISO-8601 timestamp"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    return value


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _quote_ch(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
