from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from quant.backtest.fill_only_v2_service import (
    FillOnlyV2AnchorError,
    FillOnlyV2CoverageError,
    FillOnlyV2Error,
    FillOnlyV2RequestError,
    FillOnlyV2UnavailableError,
    load_trade_tape_coverage,
    resolve_fill_only_v2_anchor,
)
from quant.backtest.orderfilled_v2_replay import (
    RequiredTradeWindow,
    TradeSliceLimitExceeded,
    load_v2_trade_slices_for_windows,
)
from quant.backtest.rust_kernel import rust_kernel_available
from quant.core.db import ClickHouseClient
from quant.core.metadata import derive_clickhouse_token_id_hex

from .engine import replay_trade_only_orders_with_diagnostics
from .live_labels import build_live_order_label_readiness
from .models import (
    LiquidityIntent,
    TradeOnlyOrder,
    get_trade_only_profile,
    list_trade_only_profiles,
)

SCHEMA_VERSION = "fill-only-v3-trade-only-api-v1"
DEFAULT_PROFILE = "central_trade_only_l2_reference_expected_fak"
DEFAULT_LOOKBACK_BLOCKS = 300
DEFAULT_HORIZON_BLOCKS = 2_000
DEFAULT_MAX_ORDERS = 100
DEFAULT_MAX_ROWS_PER_WINDOW = 100_000
DEFAULT_MAX_TOTAL_ROWS = 500_000

REQUEST_FIELDS = frozenset(
    {
        "request_id",
        "requestId",
        "profile",
        "source_max_block",
        "sourceMaxBlock",
        "default_lookback_blocks",
        "defaultLookbackBlocks",
        "default_horizon_blocks",
        "defaultHorizonBlocks",
        "max_rows_per_window",
        "maxRowsPerWindow",
        "merge_gap_blocks",
        "mergeGapBlocks",
        "random_seed",
        "randomSeed",
        "monte_carlo_paths",
        "monteCarloPaths",
        "market_slug",
        "marketSlug",
        "market_title",
        "marketTitle",
        "category",
        "league",
        "market_end_ts",
        "marketEndTs",
        "anchor_max_distance_seconds",
        "anchorMaxDistanceSeconds",
        "matcher_backend",
        "matcherBackend",
        "orders",
    }
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
        "liquidity_intent",
        "liquidityIntent",
        "allow_partial_fill",
        "allowPartialFill",
        "latency_seconds",
        "latencySeconds",
        "latency_blocks",
        "latencyBlocks",
        "horizon_seconds",
        "horizonSeconds",
        "horizon_blocks",
        "horizonBlocks",
        "lookback_seconds",
        "lookbackSeconds",
        "lookback_blocks",
        "lookbackBlocks",
        "signal_source_trade_id",
        "signalSourceTradeId",
        "random_seed",
        "randomSeed",
        "monte_carlo_paths",
        "monteCarloPaths",
        "market_slug",
        "marketSlug",
        "market_title",
        "marketTitle",
        "category",
        "league",
        "market_end_ts",
        "marketEndTs",
    }
)


class FillOnlyV3Error(RuntimeError):
    status_code = 500
    error_code = "FILL_ONLY_V3_ERROR"

    def as_dict(self) -> dict[str, Any]:
        return {"error": str(self), "error_code": self.error_code}


class FillOnlyV3RequestError(FillOnlyV3Error):
    status_code = 400
    error_code = "INVALID_FILL_ONLY_V3_REQUEST"


class FillOnlyV3CoverageError(FillOnlyV3Error):
    status_code = 409
    error_code = "TRADE_ONLY_V3_COVERAGE_GAP"


class FillOnlyV3LimitError(FillOnlyV3Error):
    status_code = 413
    error_code = "FILL_ONLY_V3_REQUEST_LIMIT_EXCEEDED"


class FillOnlyV3UnavailableError(FillOnlyV3Error):
    status_code = 503
    error_code = "TRADE_ONLY_V3_UNAVAILABLE"


class FillOnlyV3AnchorError(FillOnlyV3Error):
    status_code = 409
    error_code = "TRADE_ONLY_V3_ANCHOR_UNRESOLVABLE"


def list_fill_only_v3_profiles() -> list[dict[str, Any]]:
    return list_trade_only_profiles()


def resolve_fill_only_v3_anchor(
    payload: Mapping[str, Any],
    *,
    client: ClickHouseClient | None = None,
) -> dict[str, Any]:
    """Resolve a timestamp against the same pinned OrderFilled tape as V2."""

    try:
        resolved = resolve_fill_only_v2_anchor(payload, client=client)
    except FillOnlyV2RequestError as exc:
        raise FillOnlyV3RequestError(str(exc)) from exc
    except FillOnlyV2CoverageError as exc:
        raise FillOnlyV3CoverageError(str(exc)) from exc
    except FillOnlyV2AnchorError as exc:
        raise FillOnlyV3AnchorError(str(exc)) from exc
    except FillOnlyV2UnavailableError as exc:
        raise FillOnlyV3UnavailableError(str(exc)) from exc
    return {
        **resolved,
        "schema_version": "fill-only-v3-timestamp-anchor-v1",
        "execution_model": "trade_only_v3_evidence_tiered",
        "uses_lob_data": False,
    }


def build_fill_only_v3_readiness(
    *,
    client: ClickHouseClient | None = None,
    postgres_conn: Any | None = None,
) -> dict[str, Any]:
    ch = client or ClickHouseClient()
    coverage = load_trade_tape_coverage(ch)
    project_root = Path(__file__).resolve().parents[3]
    probability_paths = {
        "5": project_root
        / "config/execution/orderfilled_probability_profile.5s.v1.json",
        "30": project_root / "config/execution/orderfilled_probability_profile.v1.json",
        "120": project_root
        / "config/execution/orderfilled_probability_profile.120s.v1.json",
        "300": project_root
        / "config/execution/orderfilled_probability_profile.300s.v1.json",
    }
    probability = {
        horizon: _profile_summary(
            path, ("activation", "training_rows", "model_version")
        )
        for horizon, path in probability_paths.items()
    }
    l2_reference_probability = _profile_summary(
        project_root / "config/execution/fill_only_v3_l2_reference_probability.v1.json",
        (
            "activation",
            "training_rows",
            "model_version",
            "promotion_allowed",
        ),
    )
    hierarchy = _profile_summary(
        project_root / "config/execution/trade_only_hierarchical_priors.v1.json",
        ("schema_version", "min_cell_samples"),
    )
    tif_aware_hierarchy = _profile_summary(
        project_root
        / "config/execution/trade_only_hierarchical_priors.tif_aware.v1.json",
        ("schema_version", "min_cell_samples"),
    )
    buffer_profile = _profile_summary(
        project_root / "config/execution/trade_only_price_buffer.v1.json",
        ("schema_version", "status", "tau_seconds"),
    )
    live_labels = build_live_order_label_readiness(postgres_conn)
    live_probability_calibration = _live_probability_calibration_summary(
        project_root
        / "runtime_outputs/taker_calibration/fill-only-v3-live-calibration-latest.json"
    )
    central_ready = probability["30"].get(
        "activation"
    ) == "READY_SOURCE_CONFIRMED" and bool(hierarchy.get("available"))
    tif_aware_ready = probability["5"].get(
        "activation"
    ) == "READY_SOURCE_CONFIRMED" and bool(tif_aware_hierarchy.get("available"))
    l2_reference_ready = bool(
        l2_reference_probability.get("available")
        and l2_reference_probability.get("promotion_allowed")
    )
    rust_available = rust_kernel_available()
    profile_backends = {}
    for item in list_trade_only_profiles():
        name = str(item["name"])
        if name == "taker_source_confirmed":
            backend = "PERSISTENT_RUST_COMPATIBLE_SUBSET"
        elif name == "generative_tape_mc":
            backend = "RUST_MONTE_CARLO"
        elif name.startswith("central_trade_only"):
            backend = "HYBRID_RUST_SOURCE_PYTHON_MODEL"
        else:
            backend = "PYTHON"
        profile_backends[name] = {
            "backend": backend,
            "rust_available": rust_available,
            "python_fallback": True,
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "ready": central_ready,
        "ready_for_local_research": central_ready,
        "ready_for_live_transfer_claim": bool(
            live_probability_calibration.get("population_probability_claim_allowed")
        ),
        "default_profile": DEFAULT_PROFILE,
        "uses_lob_data": False,
        "source_coverage": coverage.as_dict(),
        "probability_models": probability,
        "l2_reference_probability_model": l2_reference_probability,
        "l2_reference_expected_local_research_ready": l2_reference_ready,
        "hierarchical_prior": hierarchy,
        "tif_aware_hierarchical_prior": tif_aware_hierarchy,
        "tif_aware_local_research_ready": tif_aware_ready,
        "price_buffer": buffer_profile,
        "live_order_labels": live_labels,
        "live_probability_calibration": live_probability_calibration,
        "rust_kernel_available": rust_available,
        "profile_backends": profile_backends,
        "capabilities": {
            "timestamp_native_orders": True,
            "resolve_anchor": True,
            "indexed_replay": True,
            "tif_aware_fak_fok_expected_execution": tif_aware_ready,
            "prior_only_sparse_fallback": tif_aware_ready,
            "l2_reference_expected_fak": l2_reference_ready,
            "arrival_probability_live_calibration": bool(
                live_probability_calibration.get("available")
            ),
            "parquet_long_runner": "scripts/run_unified_fill_only_replay.py",
            "persistent_rust_source_session": rust_available,
            "central_router_backend": "HYBRID_RUST_SOURCE_PYTHON_MODEL",
        },
    }


def _live_probability_calibration_summary(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        return {"available": False, "path": str(path), "error": str(exc)}
    selection = payload.get("selection_bias") or {}
    return {
        "available": True,
        "path": str(path),
        "schema_version": payload.get("schema_version"),
        "generated_at": payload.get("generated_at"),
        "status": payload.get("status"),
        "profile": payload.get("profile"),
        "counts": payload.get("counts") or {},
        "metrics": payload.get("metrics") or {},
        "same_order_model_comparison": payload.get(
            "same_order_model_comparison"
        )
        or {},
        "quality_gates": payload.get("quality_gates") or {},
        "collection_requirements": payload.get("collection_requirements") or {},
        "claim_boundaries": payload.get("claim_boundaries") or {},
        "population_probability_claim_allowed": bool(
            selection.get("population_probability_claim_allowed")
        ),
        "sampling_policy": selection.get("sampling_policy"),
    }


def _profile_summary(path: Path, fields: tuple[str, ...]) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError) as exc:
        return {"available": False, "path": str(path), "error": str(exc)}
    result = {"available": True, "path": str(path)}
    result.update({field: payload.get(field) for field in fields})
    if isinstance(payload.get("horizons"), Mapping):
        result["horizon_samples"] = {
            key: value.get("samples")
            for key, value in payload["horizons"].items()
            if isinstance(value, Mapping)
        }
    return result


def run_fill_only_v3_replay(
    payload: Mapping[str, Any],
    *,
    client: ClickHouseClient | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise FillOnlyV3RequestError("request body must be a JSON object")
    _reject_unknown(payload, REQUEST_FIELDS, "request")
    profile_name = (
        str(_value(payload, "profile", default=DEFAULT_PROFILE)).strip().lower()
    )
    matcher_backend = (
        str(_value(payload, "matcher_backend", "matcherBackend", default="auto"))
        .strip()
        .lower()
    )
    if matcher_backend not in {"auto", "python", "rust"}:
        raise FillOnlyV3RequestError(
            "matcher_backend must be one of: auto, python, rust"
        )
    try:
        profile = get_trade_only_profile(profile_name)
    except ValueError as exc:
        raise FillOnlyV3RequestError(str(exc)) from exc
    rows = _value(payload, "orders", default=[])
    if not isinstance(rows, list) or not rows:
        raise FillOnlyV3RequestError("orders must be a non-empty list")
    if len(rows) > DEFAULT_MAX_ORDERS:
        raise FillOnlyV3LimitError(
            f"orders exceeds server limit: {len(rows)} > {DEFAULT_MAX_ORDERS}"
        )

    lookback_blocks = _integer(
        _value(
            payload,
            "default_lookback_blocks",
            "defaultLookbackBlocks",
            default=profile.default_lookback_blocks,
        ),
        "default_lookback_blocks",
        minimum=0,
    )
    horizon_blocks = _integer(
        _value(
            payload,
            "default_horizon_blocks",
            "defaultHorizonBlocks",
            default=profile.default_horizon_blocks,
        ),
        "default_horizon_blocks",
        minimum=1,
    )
    seed = _integer(
        _value(payload, "random_seed", "randomSeed", default=0),
        "random_seed",
        minimum=0,
    )
    paths_raw = _value(payload, "monte_carlo_paths", "monteCarloPaths")
    default_paths = (
        _integer(paths_raw, "monte_carlo_paths", minimum=10, maximum=5_000)
        if paths_raw is not None
        else None
    )
    ch = client or ClickHouseClient()
    anchor_distance = _integer(
        _value(
            payload,
            "anchor_max_distance_seconds",
            "anchorMaxDistanceSeconds",
            default=120,
        ),
        "anchor_max_distance_seconds",
        minimum=0,
        maximum=3_600,
    )
    timestamp_anchors: list[dict[str, Any]] = []
    orders: list[TradeOnlyOrder] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise FillOnlyV3RequestError("each order must be a JSON object")
        _reject_unknown(row, ORDER_FIELDS, "order")
        order_row = dict(row)
        for snake, camel in (
            ("market_slug", "marketSlug"),
            ("market_title", "marketTitle"),
            ("category", "category"),
            ("league", "league"),
            ("market_end_ts", "marketEndTs"),
        ):
            order_value = (
                _value(order_row, snake)
                if snake == camel
                else _value(order_row, snake, camel)
            )
            if order_value is None:
                default = (
                    _value(payload, snake)
                    if snake == camel
                    else _value(payload, snake, camel)
                )
                if default is not None:
                    order_row[snake] = default
        signal_block_raw = _value(order_row, "signal_block", "signalBlock")
        resolved_signal_block = None
        if signal_block_raw is None:
            anchor = resolve_fill_only_v3_anchor(
                {
                    "requestId": str(
                        _value(order_row, "order_id", "orderId", default="")
                    ),
                    "signalTs": _value(order_row, "signal_ts", "signalTs"),
                    "sourceMaxBlock": _value(
                        payload, "source_max_block", "sourceMaxBlock"
                    ),
                    "maxDistanceSeconds": anchor_distance,
                },
                client=ch,
            )
            resolved_signal_block = int(anchor["anchor_block"])
            timestamp_anchors.append(anchor)
        orders.append(
            _parse_order(
                order_row,
                profile=profile,
                default_lookback_blocks=lookback_blocks,
                default_horizon_blocks=horizon_blocks,
                default_seed=seed,
                default_paths=default_paths,
                resolved_signal_block=resolved_signal_block,
            )
        )
    if len({order.order_id for order in orders}) != len(orders):
        raise FillOnlyV3RequestError("order_id values must be unique within one replay")

    if profile.execution_mode.value in {"HIERARCHICAL_EXPECTED_FILL", "CENTRAL_ROUTER"}:
        orders = _enrich_market_context(ch, orders)
    try:
        coverage = load_trade_tape_coverage(ch)
    except FillOnlyV2Error as exc:
        raise FillOnlyV3UnavailableError(str(exc)) from exc
    source_max_block_raw = _value(payload, "source_max_block", "sourceMaxBlock")
    source_max_block = (
        coverage.max_block
        if source_max_block_raw is None
        else _integer(source_max_block_raw, "source_max_block", minimum=0)
    )
    if source_max_block > coverage.max_block:
        raise FillOnlyV3CoverageError(
            f"source_max_block {source_max_block} exceeds derived coverage {coverage.max_block}"
        )
    windows: list[RequiredTradeWindow] = []
    for order in orders:
        arrival_block = order.arrival_block
        deadline_block = order.deadline_block
        if arrival_block is None or deadline_block is None:
            raise FillOnlyV3CoverageError(
                "block-native replay requires signal_block or a resolved timestamp anchor"
            )
        windows.append(
            RequiredTradeWindow(
                market_id=order.market_id,
                asset_id=order.asset_id,
                aggressor_side=None,
                start_block=max(0, arrival_block - order.lookback_blocks),
                end_block=deadline_block,
            )
        )
    for window in windows:
        if window.start_block is None or window.end_block is None:
            raise FillOnlyV3CoverageError("trade-tape block window is incomplete")
        if window.end_block > source_max_block or not coverage.contains(
            window.start_block, window.end_block
        ):
            raise FillOnlyV3CoverageError(
                "trade tape does not cover complete order window "
                f"market_id={window.market_id} asset_id={window.asset_id} "
                f"from_block={window.start_block} to_block={window.end_block}"
            )
    merge_gap = _integer(
        _value(payload, "merge_gap_blocks", "mergeGapBlocks", default=0),
        "merge_gap_blocks",
        minimum=0,
        maximum=10_000,
    )
    row_limit = _integer(
        _value(
            payload,
            "max_rows_per_window",
            "maxRowsPerWindow",
            default=DEFAULT_MAX_ROWS_PER_WINDOW,
        ),
        "max_rows_per_window",
        minimum=1,
        maximum=DEFAULT_MAX_ROWS_PER_WINDOW,
    )
    try:
        loaded = load_v2_trade_slices_for_windows(
            windows,
            client=ch,
            merge_gap_blocks=merge_gap,
            limit_per_window=row_limit,
            reject_truncated_windows=True,
        )
    except TradeSliceLimitExceeded as exc:
        raise FillOnlyV3LimitError(str(exc)) from exc
    if loaded.rows_loaded > DEFAULT_MAX_TOTAL_ROWS:
        raise FillOnlyV3LimitError(
            f"loaded trade rows exceed server limit: {loaded.rows_loaded} > {DEFAULT_MAX_TOTAL_ROWS}"
        )

    request_id = (
        str(_value(payload, "request_id", "requestId", default="")).strip() or None
    )
    ledger_id = f"trade-only-v3:{request_id or 'anonymous'}"
    try:
        results, ledger, diagnostics = replay_trade_only_orders_with_diagnostics(
            orders,
            loaded.trades,
            profile,
            ledger_id=ledger_id,
            backend=matcher_backend,  # type: ignore[arg-type]
        )
    except ValueError as exc:
        raise FillOnlyV3RequestError(str(exc)) from exc
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "profile": profile.as_dict(),
        "source_table": "trade_prints_one_sided",
        "source_max_block": source_max_block,
        "orders": [_order_manifest(order) for order in orders],
        "random_seed": seed,
        "monte_carlo_paths": default_paths,
        "matcher_backend": matcher_backend,
        "timestamp_anchors": timestamp_anchors,
    }
    manifest_hash = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "manifest_hash": manifest_hash,
        "execution_model": "trade_only_v3_evidence_tiered",
        "uses_lob_data": False,
        "profile": profile.as_dict(),
        "source_max_block": source_max_block,
        "source_coverage": coverage.as_dict(),
        "trade_slice_load": loaded.as_dict(),
        "match_diagnostics": diagnostics.as_dict(),
        "summary": _summary(results),
        "orders": [result.as_dict() for result in results],
        "run_liquidity_ledger": ledger.as_dict(),
        "manifest": manifest,
        "timestamp_anchors": timestamp_anchors,
        "warnings": _warnings(profile),
    }


def _parse_order(
    row: Any,
    *,
    profile: Any,
    default_lookback_blocks: int,
    default_horizon_blocks: int,
    default_seed: int,
    default_paths: int | None,
    resolved_signal_block: int | None = None,
) -> TradeOnlyOrder:
    if not isinstance(row, Mapping):
        raise FillOnlyV3RequestError("each order must be a JSON object")
    _reject_unknown(row, ORDER_FIELDS, "order")
    order_id = str(_value(row, "order_id", "orderId", default="")).strip()
    if not order_id:
        raise FillOnlyV3RequestError("order_id is required")
    side = str(_value(row, "side", default="")).strip().upper()
    if side not in {"BUY", "SELL"}:
        raise FillOnlyV3RequestError(f"{order_id}.side must be BUY or SELL")
    tif = str(_value(row, "tif", default=profile.default_tif)).strip().upper()
    if tif not in {"GTC", "GTD", "IOC", "FOK", "FAK"}:
        raise FillOnlyV3RequestError(f"{order_id}.tif is invalid")
    intent_text = (
        str(
            _value(
                row,
                "liquidity_intent",
                "liquidityIntent",
                default=profile.default_liquidity_intent.value,
            )
        )
        .strip()
        .upper()
    )
    try:
        intent = LiquidityIntent(intent_text)
    except ValueError as exc:
        raise FillOnlyV3RequestError(f"{order_id}.liquidity_intent is invalid") from exc
    limit = _decimal(
        _value(row, "limit_price", "limitPrice"), f"{order_id}.limit_price"
    )
    size = _decimal(_value(row, "size"), f"{order_id}.size")
    if limit <= 0 or limit > 1 or size <= 0:
        raise FillOnlyV3RequestError(
            f"{order_id} requires limit_price in (0,1] and positive size"
        )
    signal_ts = _datetime(_value(row, "signal_ts", "signalTs"), f"{order_id}.signal_ts")
    signal_block = (
        int(resolved_signal_block)
        if resolved_signal_block is not None
        else _integer(
            _value(row, "signal_block", "signalBlock"),
            f"{order_id}.signal_block",
            minimum=0,
        )
    )
    latency_seconds = _decimal(
        _value(
            row,
            "latency_seconds",
            "latencySeconds",
            default=profile.latency.total_seconds(),
        ),
        f"{order_id}.latency_seconds",
    )
    horizon_seconds = _decimal(
        _value(
            row,
            "horizon_seconds",
            "horizonSeconds",
            default=profile.horizon.total_seconds(),
        ),
        f"{order_id}.horizon_seconds",
    )
    lookback_seconds = _decimal(
        _value(row, "lookback_seconds", "lookbackSeconds", default=300),
        f"{order_id}.lookback_seconds",
    )
    if min(latency_seconds, horizon_seconds, lookback_seconds) < 0:
        raise FillOnlyV3RequestError(f"{order_id} time durations must be non-negative")
    paths_raw = _value(row, "monte_carlo_paths", "monteCarloPaths")
    paths = (
        _integer(paths_raw, f"{order_id}.monte_carlo_paths", minimum=10, maximum=5_000)
        if paths_raw is not None
        else default_paths
    )
    raw_asset_id = (
        str(_value(row, "asset_id", "assetId", "token_id", "tokenId", default=""))
        .strip()
        .lower()
    )
    if not raw_asset_id:
        raise FillOnlyV3RequestError(f"{order_id}.asset_id is required")
    asset_id = derive_clickhouse_token_id_hex(raw_asset_id) or raw_asset_id
    return TradeOnlyOrder(
        order_id=order_id,
        market_id=_integer(
            _value(row, "market_id", "marketId"), f"{order_id}.market_id", minimum=1
        ),
        asset_id=asset_id,
        side=side,  # type: ignore[arg-type]
        limit_price=limit,
        size=size,
        signal_block=signal_block,
        signal_ts=signal_ts,
        tif=tif,  # type: ignore[arg-type]
        liquidity_intent=intent,
        latency=timedelta(seconds=float(latency_seconds)),
        latency_blocks=_integer(
            _value(row, "latency_blocks", "latencyBlocks", default=0),
            f"{order_id}.latency_blocks",
            minimum=0,
        ),
        horizon=timedelta(seconds=float(horizon_seconds)),
        horizon_blocks=_integer(
            _value(
                row, "horizon_blocks", "horizonBlocks", default=default_horizon_blocks
            ),
            f"{order_id}.horizon_blocks",
            minimum=1,
        ),
        lookback=timedelta(seconds=float(lookback_seconds)),
        lookback_blocks=_integer(
            _value(
                row,
                "lookback_blocks",
                "lookbackBlocks",
                default=default_lookback_blocks,
            ),
            f"{order_id}.lookback_blocks",
            minimum=0,
        ),
        allow_partial_fill=_boolean(
            _value(row, "allow_partial_fill", "allowPartialFill", default=True)
        ),
        signal_source_trade_id=_optional_text(
            _value(row, "signal_source_trade_id", "signalSourceTradeId")
        ),
        random_seed=_integer(
            _value(row, "random_seed", "randomSeed", default=default_seed),
            f"{order_id}.random_seed",
            minimum=0,
        ),
        monte_carlo_paths=paths,
        market_slug=_optional_text(_value(row, "market_slug", "marketSlug")),
        market_title=_optional_text(_value(row, "market_title", "marketTitle")),
        category=_optional_text(_value(row, "category")),
        league=_optional_text(_value(row, "league")),
        market_end_ts=_optional_datetime(
            _value(row, "market_end_ts", "marketEndTs"),
            f"{order_id}.market_end_ts",
        ),
    )


def _enrich_market_context(
    client: ClickHouseClient,
    orders: list[TradeOnlyOrder],
) -> list[TradeOnlyOrder]:
    missing = sorted(
        {
            order.market_id
            for order in orders
            if not order.category or not order.market_end_ts
        }
    )
    if not missing:
        return orders
    joined = ",".join(str(value) for value in missing)
    try:
        rows = client.query_json_rows(
            f"""
            SELECT market_id, argMax(slug, updated_at) slug,
                   argMax(title, updated_at) title,
                   argMax(category, updated_at) category,
                   argMax(end_time, updated_at) end_time
            FROM pnl_market_condition_metadata_full
            WHERE market_id IN ({joined})
            GROUP BY market_id
            """,
            timeout_seconds=30,
        )
    except Exception:  # noqa: BLE001 - metadata absence falls back to global prior
        return orders
    metadata = {int(row["market_id"]): row for row in rows}
    enriched: list[TradeOnlyOrder] = []
    for order in orders:
        row = metadata.get(order.market_id, {})
        enriched.append(
            replace(
                order,
                market_slug=order.market_slug or _optional_text(row.get("slug")),
                market_title=order.market_title or _optional_text(row.get("title")),
                category=order.category or _optional_text(row.get("category")),
                market_end_ts=order.market_end_ts
                or _optional_datetime(row.get("end_time"), "market_end_ts"),
            )
        )
    return enriched


def _summary(results: list[Any]) -> dict[str, Any]:
    return {
        "attempted_orders": len(results),
        "filled_orders": sum(result.status == "FILLED" for result in results),
        "partial_orders": sum(result.status == "PARTIAL_FILLED" for result in results),
        "no_fill_orders": sum(result.status == "NO_FILL" for result in results),
        "unobservable_orders": sum(
            result.status == "UNOBSERVABLE" for result in results
        ),
        "modeled_distribution_orders": sum(
            result.status == "MODELED_DISTRIBUTION" for result in results
        ),
        "modeled_expectation_orders": sum(
            result.status == "MODELED_EXPECTATION" for result in results
        ),
        "positive_modeled_expectation_orders": sum(
            result.status == "MODELED_EXPECTATION" and result.filled_size > 0
            for result in results
        ),
        "filled_size": str(sum((result.filled_size for result in results), Decimal(0))),
        "evidence_tier_counts": _counts(result.evidence_tier for result in results),
        "status_counts": _counts(result.status for result in results),
    }


def _warnings(profile: Any) -> list[str]:
    warnings = [
        "trade-only V3 does not observe L2 depth, queue, cancellations, or true offchain match time",
        "evidence tiers must be reported separately",
    ]
    if "UNVALIDATED" in profile.calibration_status:
        warnings.append(
            "the source-event probability model is trained, but transfer to latent execution probability is not live-order validated"
        )
    elif (
        profile.result_role == "SENSITIVITY_ONLY"
        or "UNCALIBRATED" in profile.calibration_status
    ):
        warnings.append(
            "this profile is an uncalibrated sensitivity model and is not primary execution truth"
        )
    return warnings


def _order_manifest(order: TradeOnlyOrder) -> dict[str, Any]:
    row = asdict(order)
    row["signal_ts"] = order.signal_ts.isoformat()
    row["latency_seconds"] = str(order.latency.total_seconds())
    row["horizon_seconds"] = str(order.horizon.total_seconds())
    row["lookback_seconds"] = str(order.lookback.total_seconds())
    row.pop("latency")
    row.pop("horizon")
    row.pop("lookback")
    row["liquidity_intent"] = order.liquidity_intent.value
    return _json_ready(row)


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    return counts


def _reject_unknown(
    row: Mapping[str, Any], allowed: frozenset[str], context: str
) -> None:
    unknown = sorted(set(row) - allowed)
    if unknown:
        raise FillOnlyV3RequestError(
            f"{context} contains unknown fields: {', '.join(unknown)}"
        )


def _value(row: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    present = [key for key in keys if key in row]
    if len(present) > 1:
        raise FillOnlyV3RequestError(f"conflicting aliases: {', '.join(present)}")
    return row[present[0]] if present else default


def _integer(value: Any, name: str, *, minimum: int, maximum: int = 2**63 - 1) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise FillOnlyV3RequestError(f"{name} must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise FillOnlyV3RequestError(f"{name} must be between {minimum} and {maximum}")
    return parsed


def _decimal(value: Any, name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise FillOnlyV3RequestError(f"{name} must be numeric") from exc


def _datetime(value: Any, name: str) -> datetime:
    if not value:
        raise FillOnlyV3RequestError(f"{name} is required")
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise FillOnlyV3RequestError(f"{name} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_datetime(value: Any, name: str) -> datetime | None:
    if value in (None, ""):
        return None
    return _datetime(value, name)


def _boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if str(value).strip().lower() in {"true", "1", "yes"}:
        return True
    if str(value).strip().lower() in {"false", "0", "no"}:
        return False
    raise FillOnlyV3RequestError("boolean field has invalid value")


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value
