#!/usr/bin/env python3
"""Batch-compare real Fill-only and L2 execution opportunities.

The runner discovers point-in-time compatible markets from the intersection of
the local L2 archive, Postgres market metadata, and ClickHouse OrderFilled tape.
Every selected model receives the same independently evaluated orders.
"""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from bisect import bisect_right
from collections import Counter, defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from enum import Enum
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, cast

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_only_v2_service import resolve_fill_only_v2_anchor
from quant.backtest.orderfilled_probability import (
    ARRIVAL_FEATURE_CONTRACT,
    extract_orderfilled_probability_features,
)
from quant.backtest.orderfilled_v2_replay import (
    V2OrderResult,
    V2TakerOrder,
    V2TradePrint,
    prepare_v2_trade_tape,
    replay_v2_taker_orders_with_diagnostics,
    trade_print_from_row,
    with_v2_execution_profile,
)
from quant.backtest.pml2.contracts import (
    BookLevel,
    BookSnapshotEvent,
    Outcome,
    Pml2OrderIntent,
    Pml2OrderResult,
    RawOrderSide,
    TimeInForce,
)
from quant.backtest.pml2.profiles import get_pml2_profile
from quant.backtest.pml2.rust_kernel import (
    IndependentSnapshotCase,
    replay_independent_snapshot_takers,
)
from quant.backtest.pml2.session import ReplayExecutionSession
from quant.backtest.trade_only_v3 import (
    LiquidityIntent,
    TradeOnlyOrder,
    TradeOnlyOrderResult,
    get_trade_only_profile,
    replay_trade_only_orders,
)
from quant.core.db import ClickHouseClient, postgres_connection
from quant.orderbook.l2_subscription_snapshot import subscription_affinity_shard
from quant.orderbook.subscriptions import token_shard

UTC = timezone.utc
HEX64 = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_ARCHIVE = Path("/data/jiahuaiyu/prediction-market-quant/lob_l2_archive_xue")
DEFAULT_OUTPUT_DIR = (
    PROJECT_ROOT
    / "backtest_framework"
    / "nautilus_trader_comparison"
    / "batch_real_execution_models"
)
DEFAULT_COHORT_REGISTRY = (
    PROJECT_ROOT
    / "backtest_framework"
    / "nautilus_trader_comparison"
    / "fill_only_cross_validation_registry.jsonl"
)
DEFAULT_L2_SLICE_CACHE = Path(
    "/data/jiahuaiyu/prediction-market-quant/l2_backtest_slice_cache"
)
DEFAULT_PREPARED_INPUT_CACHE = Path(
    "/data/jiahuaiyu/prediction-market-quant/execution_comparison_input_cache"
)
DEFAULT_NAUTILUS_PYTHON = Path(
    "/home/jiahuaiyu/.conda/envs/polymonitor-nautilus312/bin/python"
)
MODEL_CHOICES = (
    "v2_fak",
    "v2_gtd",
    "v3_source_fak",
    "v3_source_gtd",
    "v3_expected30",
    "v3_tif_expected5",
    "v3_tif_central",
    "v3_tif_recall",
    "v3_l2_reference",
    "v3_l2_expected",
    "v3_source_contract",
    "v3_contract_probability",
    "v3_contract_expected",
    "pml2_fak",
    "pml2_fok",
    "nautilus_fak",
    "nautilus_fok",
)
SOURCE_MODELS = {
    "v2_fak",
    "v2_gtd",
    "v3_source_fak",
    "v3_source_gtd",
    "v3_source_contract",
}


@dataclass(frozen=True, slots=True)
class BatchConfig:
    start: datetime
    end: datetime
    market_limit: int
    orders_per_market: int
    models: tuple[str, ...]
    order_size: Decimal
    order_sizes: tuple[Decimal, ...]
    order_sides: tuple[Literal["BUY", "SELL"], ...]
    order_tif: Literal["FAK", "FOK"]
    latency: timedelta
    horizon: timedelta
    lookback: timedelta
    limit_buffer: Decimal
    tick_size: Decimal
    lookback_blocks: int
    horizon_blocks: int
    candidate_multiplier: int
    market_ids: tuple[int, ...]
    exclude_market_ids: tuple[int, ...]
    categories: tuple[str, ...]
    archive: Path
    archive_baseline_lookback: timedelta
    archive_shard_count: int
    l2_source: str
    l2_slice_cache: Path | None
    output_dir: Path
    nautilus_python: Path
    cohort_registry: Path | None
    cohort_split: str
    allow_cohort_reuse: bool
    prepared_input_cache: Path | None = None
    v3_probability_artifact: Path | None = None
    v3_fak_probability_artifact: Path | None = None
    v3_fok_probability_artifact: Path | None = None
    pml2_backend: Literal["auto", "python", "rust"] = "auto"


@dataclass(frozen=True, slots=True)
class Candidate:
    market_id: int
    asset_id: str
    asset_hex: str
    condition_id: str
    outcome: str
    title: str
    slug: str
    category: str
    event_title: str
    trade_count: int
    trade_volume: Decimal
    probe_depth: Decimal = Decimal(0)
    depth_regime: str = "UNKNOWN"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _parse_ts(raw: str) -> datetime:
    text = raw.strip().replace("Z", "+00:00")
    return _utc(datetime.fromisoformat(text))


def _parse_db_ts(raw: str) -> datetime:
    value = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    return (value.replace(tzinfo=UTC) if value.tzinfo is None else value).astimezone(
        UTC
    )


def _csv(raw: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _excluded_market_ids(raw: str, path: Path | None) -> tuple[int, ...]:
    values = {int(item) for item in _csv(raw)}
    if path is not None:
        values.update(
            int(item)
            for item in re.split(r"[\s,]+", path.read_text(encoding="utf-8"))
            if item
        )
    return tuple(sorted(values))


def _json_ready(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _json_ready(asdict(value))
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return _utc(value).isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


PREPARED_INPUT_SCHEMA = "pmq_execution_comparison_prepared_input_v2"


def _prepared_input_identity(config: BatchConfig) -> dict[str, Any]:
    return _json_ready(
        {
            "schema_version": PREPARED_INPUT_SCHEMA,
            "start": config.start,
            "end": config.end,
            "market_limit": config.market_limit,
            "orders_per_market": config.orders_per_market,
            "order_size": config.order_size,
            "order_sizes": config.order_sizes,
            "order_sides": config.order_sides,
            "order_tif": config.order_tif,
            "latency": config.latency,
            "horizon": config.horizon,
            "lookback": config.lookback,
            "limit_buffer": config.limit_buffer,
            "tick_size": config.tick_size,
            "lookback_blocks": config.lookback_blocks,
            "horizon_blocks": config.horizon_blocks,
            "candidate_multiplier": config.candidate_multiplier,
            "market_ids": config.market_ids,
            "exclude_market_ids": config.exclude_market_ids,
            "categories": config.categories,
            "archive": config.archive,
            "archive_baseline_lookback": config.archive_baseline_lookback,
            "archive_shard_count": config.archive_shard_count,
            "l2_source": config.l2_source,
        }
    )


def _prepared_input_paths(config: BatchConfig) -> tuple[Path, Path, str]:
    if config.prepared_input_cache is None:
        raise ValueError("prepared input cache is disabled")
    identity = _prepared_input_identity(config)
    fingerprint = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    root = config.prepared_input_cache
    return (
        root / f"{fingerprint}.json.gz",
        root / f"{fingerprint}.meta.json",
        fingerprint,
    )


def _trade_payload(trade: V2TradePrint) -> dict[str, Any]:
    return _json_ready(asdict(trade))


def _trade_from_payload(row: Mapping[str, Any]) -> V2TradePrint:
    return V2TradePrint(
        trade_id=str(row["trade_id"]),
        trade_group_id=(
            None if row.get("trade_group_id") is None else str(row["trade_group_id"])
        ),
        market_id=int(row["market_id"]),
        condition_id=str(row["condition_id"]),
        asset_id=str(row["asset_id"]),
        outcome=str(row["outcome"]),
        block_number=int(row["block_number"]),
        block_time=_parse_ts(str(row["block_time"])),
        tx_hash=str(row["tx_hash"]),
        tx_index=int(row["tx_index"]),
        tx_index_source=str(row["tx_index_source"]),
        price=Decimal(str(row["price"])),
        size=Decimal(str(row["size"])),
        notional=Decimal(str(row["notional"])),
        aggressor_side=str(row["aggressor_side"]),  # type: ignore[arg-type]
        passive_side=str(row["passive_side"]),  # type: ignore[arg-type]
        source_log_indexes=tuple(int(item) for item in row["source_log_indexes"]),
        source_fill_count=int(row["source_fill_count"]),
    )


def _book_from_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for field_name in (
        "baseline_exchange_ts",
        "baseline_received_ts",
        "latest_exchange_ts",
        "latest_received_ts",
    ):
        result[field_name] = _parse_ts(str(result[field_name]))
    for field_name in (
        "best_bid",
        "best_bid_size",
        "best_ask",
        "best_ask_size",
        "spread",
        "raw_depth",
        "effective_depth",
        "order_to_effective_depth",
    ):
        if result.get(field_name) is not None:
            result[field_name] = Decimal(str(result[field_name]))
    result["bids"] = tuple(
        (Decimal(str(price)), Decimal(str(size))) for price, size in result["bids"]
    )
    result["asks"] = tuple(
        (Decimal(str(price)), Decimal(str(size))) for price, size in result["asks"]
    )
    return result


def _prepared_input_payload(
    *,
    config: BatchConfig,
    source: Mapping[str, Any],
    discovery: Mapping[str, Any],
    selected: Sequence[Candidate],
    specs: Sequence[Mapping[str, Any]],
    trades: Sequence[V2TradePrint],
    probe_rejections: Mapping[str, int],
    build_rejections: Mapping[str, int],
    probe_valid_markets: int,
    archive_hour_partitions: Sequence[Path],
) -> dict[str, Any]:
    return {
        "schema_version": PREPARED_INPUT_SCHEMA,
        "identity": _prepared_input_identity(config),
        "source": _json_ready(source),
        "discovery": _json_ready(discovery),
        "selected": [_json_ready(asdict(candidate)) for candidate in selected],
        "trades": [_trade_payload(trade) for trade in trades],
        "specs": [
            {
                "order_id": str(spec["order_id"]),
                "candidate_key": [
                    int(spec["candidate"].market_id),
                    str(spec["candidate"].asset_hex).lower(),
                ],
                "decision_ts": _json_ready(spec["decision_ts"]),
                "arrival_ts": _json_ready(spec["arrival_ts"]),
                "deadline_ts": _json_ready(spec["deadline_ts"]),
                "anchor_block": int(spec["anchor_block"]),
                "signal_trade_id": str(spec["signal"].trade_id),
                "limit_price": str(spec["limit_price"]),
                "side": str(spec["side"]),
                "size": str(spec["size"]),
                "tif": str(spec["tif"]),
                "book": _json_ready(spec["book"]),
            }
            for spec in specs
        ],
        "probe_rejections": dict(probe_rejections),
        "build_rejections": dict(build_rejections),
        "probe_valid_markets": int(probe_valid_markets),
        "archive_hour_partitions": [str(path) for path in archive_hour_partitions],
    }


def _write_prepared_input(config: BatchConfig, payload: Mapping[str, Any]) -> None:
    data_path, metadata_path, fingerprint = _prepared_input_paths(config)
    data_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = data_path.with_suffix(data_path.suffix + ".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
    temporary.replace(data_path)
    metadata = {
        "schema_version": PREPARED_INPUT_SCHEMA,
        "fingerprint": fingerprint,
        "identity": _prepared_input_identity(config),
        "path": str(data_path),
        "bytes": data_path.stat().st_size,
        "sha256": _sha256_file(data_path),
        "source": payload["source"],
        "trade_rows": len(payload["trades"]),
        "orders": len(payload["specs"]),
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _load_prepared_input(config: BatchConfig) -> dict[str, Any] | None:
    if config.prepared_input_cache is None:
        return None
    data_path, metadata_path, fingerprint = _prepared_input_paths(config)
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if (
            metadata.get("schema_version") != PREPARED_INPUT_SCHEMA
            or metadata.get("fingerprint") != fingerprint
            or metadata.get("identity") != _prepared_input_identity(config)
            or not data_path.is_file()
            or data_path.stat().st_size != int(metadata["bytes"])
            or _sha256_file(data_path) != str(metadata["sha256"])
        ):
            return None
        with gzip.open(data_path, "rt", encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, KeyError, ValueError, json.JSONDecodeError):
        return None
    if raw.get("schema_version") != PREPARED_INPUT_SCHEMA:
        return None
    selected = [
        Candidate(
            market_id=int(row["market_id"]),
            asset_id=str(row["asset_id"]),
            asset_hex=str(row["asset_hex"]),
            condition_id=str(row["condition_id"]),
            outcome=str(row["outcome"]),
            title=str(row["title"]),
            slug=str(row["slug"]),
            category=str(row["category"]),
            event_title=str(row["event_title"]),
            trade_count=int(row["trade_count"]),
            trade_volume=Decimal(str(row["trade_volume"])),
            probe_depth=Decimal(str(row["probe_depth"])),
            depth_regime=str(row["depth_regime"]),
        )
        for row in raw["selected"]
    ]
    candidates = {(row.market_id, row.asset_hex.lower()): row for row in selected}
    trades = [_trade_from_payload(row) for row in raw["trades"]]
    trades_by_pair: dict[tuple[int, str], list[V2TradePrint]] = defaultdict(list)
    trades_by_id = {trade.trade_id: trade for trade in trades}
    for trade in trades:
        trades_by_pair[(trade.market_id, trade.asset_id.lower())].append(trade)
    specs: list[dict[str, Any]] = []
    for row in raw["specs"]:
        key = (int(row["candidate_key"][0]), str(row["candidate_key"][1]).lower())
        candidate = candidates[key]
        signal = trades_by_id[str(row["signal_trade_id"])]
        specs.append(
            {
                "order_id": str(row["order_id"]),
                "candidate": candidate,
                "decision_ts": _parse_ts(str(row["decision_ts"])),
                "arrival_ts": _parse_ts(str(row["arrival_ts"])),
                "deadline_ts": _parse_ts(str(row["deadline_ts"])),
                "anchor_block": int(row["anchor_block"]),
                "signal": signal,
                "limit_price": Decimal(str(row["limit_price"])),
                "side": str(row["side"]),
                "size": Decimal(str(row["size"])),
                "tif": str(row["tif"]),
                "book": _book_from_payload(row["book"]),
                "trades": trades_by_pair[(candidate.market_id, candidate.asset_hex)],
            }
        )
    return {
        "source": raw["source"],
        "discovery": raw["discovery"],
        "selected": selected,
        "specs": specs,
        "trades": trades,
        "probe_rejections": Counter(raw["probe_rejections"]),
        "build_rejections": Counter(raw["build_rejections"]),
        "probe_valid_markets": int(raw["probe_valid_markets"]),
        "archive_hour_partitions": tuple(
            Path(path) for path in raw["archive_hour_partitions"]
        ),
        "cache_path": data_path,
        "cache_sha256": metadata["sha256"],
    }


def _asset_decimal(asset_hex: str) -> str:
    normalized = str(asset_hex).lower().removeprefix("0x")
    if not HEX64.fullmatch(normalized):
        raise ValueError(f"invalid trade-tape asset_id: {asset_hex!r}")
    return str(int(normalized, 16))


def _activity_regime(count: int) -> str:
    if count <= 5:
        return "SPARSE_LE_5"
    if count <= 50:
        return "MEDIUM_6_TO_50"
    return "ACTIVE_GT_50"


def _depth_regime(depth: Decimal, order_size: Decimal) -> str:
    ratio = depth / order_size
    if ratio < 1:
        return "SHALLOW_LT_1X"
    if ratio < 10:
        return "MEDIUM_1X_TO_10X"
    return "DEEP_GE_10X"


def _spread_regime(spread: Decimal) -> str:
    if spread <= Decimal("0.002"):
        return "TIGHT_LE_0_002"
    if spread <= Decimal("0.02"):
        return "NORMAL_0_002_TO_0_02"
    return "WIDE_GT_0_02"


def _round_robin(rows: Iterable[Candidate], *, include_depth: bool) -> list[Candidate]:
    groups: dict[tuple[str, ...], deque[Candidate]] = {}
    for row in rows:
        key: tuple[str, ...] = (row.category, _activity_regime(row.trade_count))
        if include_depth:
            key = (*key, row.depth_regime)
        groups.setdefault(key, deque()).append(row)
    for values in groups.values():
        ordered = sorted(
            values,
            key=lambda item: (-item.trade_count, item.market_id, item.outcome),
        )
        values.clear()
        values.extend(ordered)
    ordered_rows: list[Candidate] = []
    keys: list[tuple[str, ...]] = sorted(groups)
    while keys:
        next_keys: list[tuple[str, ...]] = []
        for key in keys:
            values = groups[key]
            if values:
                ordered_rows.append(values.popleft())
            if values:
                next_keys.append(key)
        keys = next_keys
    return ordered_rows


def _resolve_source_bounds(
    config: BatchConfig, client: ClickHouseClient
) -> tuple[dict[str, Any], dict[str, Any], int, int]:
    start_anchor = resolve_fill_only_v2_anchor(
        {"signalTs": config.start.isoformat(), "maxDistanceSeconds": 60},
        client=client,
    )
    end_anchor = resolve_fill_only_v2_anchor(
        {"signalTs": config.end.isoformat(), "maxDistanceSeconds": 60},
        client=client,
    )
    source_min = max(0, int(start_anchor["anchor_block"]) - config.lookback_blocks)
    source_max = int(end_anchor["anchor_block"]) + config.horizon_blocks
    return start_anchor, end_anchor, source_min, source_max


def _hour_floor(value: datetime) -> datetime:
    return _utc(value).replace(minute=0, second=0, microsecond=0)


def _archive_hour_paths(config: BatchConfig) -> tuple[Path, ...]:
    current = _hour_floor(config.start - config.archive_baseline_lookback)
    stop = _hour_floor(config.end + config.latency)
    paths: list[Path] = []
    while current <= stop:
        path = (
            config.archive
            / f"dt={current.date().isoformat()}"
            / f"hour={current.hour:02d}"
        )
        if path.is_dir():
            paths.append(path)
        current += timedelta(hours=1)
    if not paths:
        raise RuntimeError(
            "no L2 archive hour partitions cover the requested interval: "
            f"archive={config.archive} start={config.start.isoformat()} "
            f"end={config.end.isoformat()}"
        )
    return tuple(paths)


def _candidate_shard_ids(
    candidates: Sequence[Candidate], *, shard_count: int
) -> set[int]:
    routed: set[int] = set()
    for candidate in candidates:
        routed.add(token_shard(candidate.asset_id, shard_count=shard_count))
        routed.add(
            subscription_affinity_shard(
                asset_id=candidate.asset_id,
                condition_id=candidate.condition_id,
                shard_count=shard_count,
            )
        )
        routed.add(
            int(hashlib.md5(candidate.asset_id.encode()).hexdigest()[:12], 16)
            % shard_count
        )
    return routed


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _commit_l2_slice(
    *,
    temporary: Path,
    target: Path,
    metadata_path: Path,
    fingerprint: str,
    identity: Mapping[str, Any],
    proof: Mapping[str, Any],
) -> Path:
    actual_sha = _sha256_file(temporary)
    if temporary.stat().st_size != int(proof["bytes"]) or actual_sha != str(
        proof["sha256"]
    ):
        raise RuntimeError("materialized L2 slice failed size/SHA verification")
    temporary.replace(target)
    metadata = {
        "schema_version": "pmq_l2_local_slice_cache_v1",
        "fingerprint": fingerprint,
        "request": dict(identity),
        **dict(proof),
        "path": str(target),
    }
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target


def _xue_ssh_target() -> str | None:
    explicit = os.environ.get("BOOK_L2_XUE_SSH_TARGET")
    if explicit:
        return explicit
    values: dict[str, str] = {}
    dotenv = PROJECT_ROOT / ".env"
    try:
        for raw in dotenv.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            if name.strip() not in {"xue_lab_ip", "xue_lab_user"}:
                continue
            value = value.strip().strip("\"'")
            values[name.strip()] = value
    except OSError:
        return None
    host = values.get("xue_lab_ip", "")
    user = values.get("xue_lab_user", "")
    safe = re.compile(r"^[A-Za-z0-9._-]+$")
    if safe.fullmatch(host) and safe.fullmatch(user):
        return f"{user}@{host}"
    return None


def _materialize_l2_slice(config: BatchConfig, candidates: Sequence[Candidate]) -> Path:
    if config.l2_slice_cache is None:
        raise ValueError("L2 slice cache is disabled")
    lower = config.start - config.archive_baseline_lookback
    upper = config.end + config.latency
    shard_ids = sorted(
        _candidate_shard_ids(candidates, shard_count=config.archive_shard_count)
    )
    remote_archive = os.environ.get(
        "BOOK_L2_XUE_REMOTE_ARCHIVE_DIR",
        "/mnt/hdd22t/prediction-market-quant/lob_l2_archive_full",
    )
    identity = {
        "archive": remote_archive,
        "start": lower.isoformat(),
        "end": upper.isoformat(),
        "asset_ids": sorted({candidate.asset_id for candidate in candidates}),
        "shard_ids": shard_ids,
        "source": config.l2_source,
    }
    rendered = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(rendered.encode()).hexdigest()
    cache = config.l2_slice_cache
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / f"{fingerprint}.parquet"
    metadata_path = cache / f"{fingerprint}.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if (
            metadata.get("fingerprint") == fingerprint
            and target.is_file()
            and target.stat().st_size == int(metadata["bytes"])
            and _sha256_file(target) == metadata["sha256"]
        ):
            return target
    except (OSError, KeyError, ValueError, json.JSONDecodeError):
        pass

    remote_output = f"/tmp/pmq-l2-slice-{fingerprint}.parquet"
    request = {**identity, "output": remote_output}
    request_json = json.dumps(request, separators=(",", ":"))
    materializer = PROJECT_ROOT / "scripts" / "remote_materialize_l2_slice.py"
    temporary = target.with_suffix(".tmp.parquet")
    temporary.unlink(missing_ok=True)
    errors: list[str] = []
    ssh_target = _xue_ssh_target()
    ssh_port = os.environ.get("BOOK_L2_XUE_SSH_PORT", "22")
    ssh_key = os.environ.get(
        "BOOK_L2_XUE_SSH_KEY",
        str(Path.home() / ".ssh" / "prediction_market_quant_xue_archive_ed25519"),
    )
    ssh_base = [
        "-p",
        str(ssh_port),
        "-i",
        str(ssh_key),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=accept-new",
    ]
    remote_script = materializer.read_text(encoding="utf-8")
    remote_request = f"/tmp/pmq-l2-slice-{fingerprint}.json"
    if ssh_target is not None:
        try:
            uploaded = subprocess.run(
                ["ssh", *ssh_base, ssh_target, f"cat > {remote_request}"],
                input=request_json,
                text=True,
                capture_output=True,
                timeout=30,
                check=False,
            )
            if uploaded.returncode != 0:
                detail = uploaded.stderr or uploaded.stdout or "request upload failed"
                raise RuntimeError(detail[-2000:])
            completed = subprocess.run(
                [
                    "ssh",
                    *ssh_base,
                    ssh_target,
                    "python3",
                    "-",
                    "--request-file",
                    remote_request,
                ],
                input=remote_script,
                text=True,
                capture_output=True,
                timeout=900,
                check=False,
            )
            if completed.returncode != 0:
                detail = completed.stderr or completed.stdout or "remote slice failed"
                raise RuntimeError(detail[-2000:])
            lines = [line for line in completed.stdout.splitlines() if line.strip()]
            proof = json.loads(lines[-1])
            subprocess.run(
                [
                    "scp",
                    "-q",
                    "-P",
                    str(ssh_port),
                    "-i",
                    str(ssh_key),
                    "-o",
                    "BatchMode=yes",
                    f"{ssh_target}:{remote_output}",
                    str(temporary),
                ],
                timeout=900,
                check=True,
            )
            return _commit_l2_slice(
                temporary=temporary,
                target=target,
                metadata_path=metadata_path,
                fingerprint=fingerprint,
                identity=identity,
                proof=proof,
            )
        except (
            OSError,
            RuntimeError,
            ValueError,
            IndexError,
            subprocess.SubprocessError,
        ) as exc:
            errors.append(f"remote={exc}")
        finally:
            temporary.unlink(missing_ok=True)
            subprocess.run(
                [
                    "ssh",
                    *ssh_base,
                    ssh_target,
                    "rm",
                    "-f",
                    remote_output,
                    remote_request,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
                check=False,
            )

    # The mounted archive is a slower but complete fallback when the remote
    # materializer cannot run on the storage host.
    if config.archive.is_dir():
        local_request = {**request, "archive": str(config.archive)}
        local_output = Path(remote_output)
        local_request_path = Path(remote_request)
        local_output.unlink(missing_ok=True)
        local_request_path.write_text(
            json.dumps(local_request, separators=(",", ":")), encoding="utf-8"
        )
        try:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(materializer),
                    "--request-file",
                    str(local_request_path),
                ],
                text=True,
                capture_output=True,
                timeout=900,
                check=False,
            )
            if completed.returncode != 0:
                detail = completed.stderr or completed.stdout or "local slice failed"
                raise RuntimeError(detail[-2000:])
            lines = [line for line in completed.stdout.splitlines() if line.strip()]
            proof = json.loads(lines[-1])
            shutil.copyfile(local_output, temporary)
            return _commit_l2_slice(
                temporary=temporary,
                target=target,
                metadata_path=metadata_path,
                fingerprint=fingerprint,
                identity=identity,
                proof=proof,
            )
        except (
            OSError,
            RuntimeError,
            ValueError,
            IndexError,
            subprocess.SubprocessError,
        ) as exc:
            errors.append(f"mounted={exc}")
        finally:
            local_output.unlink(missing_ok=True)
            local_request_path.unlink(missing_ok=True)
            temporary.unlink(missing_ok=True)

    raise RuntimeError("L2 slice materialization failed: " + "; ".join(errors))


def _l2_dataset(config: BatchConfig, candidates: Sequence[Candidate]) -> Any:
    try:
        import pyarrow.dataset as ds
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for the L2 archive") from exc
    if config.l2_slice_cache is not None:
        return ds.dataset(
            str(_materialize_l2_slice(config, candidates)), format="parquet"
        )
    shard_ids = _candidate_shard_ids(candidates, shard_count=config.archive_shard_count)
    shard_suffixes = tuple(f"_shard{item}.parquet" for item in sorted(shard_ids))
    files = []
    for path in _archive_hour_paths(config):
        for file in path.glob("*.parquet"):
            if not file.is_file():
                continue
            if file.name.endswith("_shardall.parquet") or file.name.endswith(
                shard_suffixes
            ):
                files.append(str(file))
    if not files:
        raise RuntimeError(
            "selected L2 archive hours contain no routed Parquet files: "
            f"shards={sorted(shard_ids)}"
        )
    return ds.dataset(files, format="parquet")


def _l2_asset_ids(config: BatchConfig, candidates: Sequence[Candidate]) -> set[str]:
    if not candidates:
        return set()
    asset_ids = [candidate.asset_id for candidate in candidates]
    lower = _hour_floor(config.start - config.archive_baseline_lookback)
    upper = _hour_floor(config.end + config.latency)
    with postgres_connection(readonly=True) as conn:
        rows = conn.execute(
            """
            SELECT asset_id
            FROM quant.clob_l2_active_active_token_hour_coverage
            WHERE asset_id = ANY(%s)
              AND hour_start BETWEEN %s AND %s
            GROUP BY asset_id
            HAVING bool_or(has_book) AND bool_or(fill_depth_ready)
            """,
            (asset_ids, lower, upper),
        ).fetchall()
    return {str(row["asset_id"]) for row in rows}


def _candidate_l2_prefilter(
    config: BatchConfig,
    candidates: Sequence[Candidate],
    coverage_assets: set[str],
) -> tuple[set[str], str]:
    sources = {value.strip() for value in config.l2_source.split(",") if value.strip()}
    if sources and "pmxt" not in sources:
        return {candidate.asset_id for candidate in candidates}, "DIRECT_ARCHIVE_PROBE"
    return coverage_assets, "SERVING_TABLE_COVERAGE"


def _candidate_tape_rows(
    client: ClickHouseClient, source_min: int, source_max: int
) -> list[dict[str, Any]]:
    return client.query_json_rows(
        f"""
        SELECT
            market_id,
            asset_id,
            any(condition_id) AS condition_id,
            any(outcome) AS outcome,
            count() AS trade_count,
            sum(size_shares) AS trade_volume,
            min(block_time) AS first_trade_ts,
            max(block_time) AS last_trade_ts
        FROM trade_prints_one_sided
        WHERE block_number BETWEEN {source_min} AND {source_max}
        GROUP BY market_id, asset_id
        ORDER BY trade_count DESC, market_id ASC, asset_id ASC
        """,
        timeout_seconds=120,
    )


def _metadata_rows(asset_ids: Sequence[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    with postgres_connection(readonly=True) as conn:
        for offset in range(0, len(asset_ids), 1_000):
            rows = conn.execute(
                """
                SELECT
                    mt.token_id::text AS asset_id,
                    mt.market_id::text AS market_id,
                    lower(mt.condition_id) AS condition_id,
                    upper(mt.outcome) AS outcome,
                    m.title,
                    m.slug,
                    coalesce(nullif(lower(m.category), ''), 'unknown') AS category,
                    coalesce(m.event_title, '') AS event_title
                FROM core.market_tokens mt
                JOIN core.markets m ON m.id=mt.market_id
                WHERE mt.token_id::text=ANY(%s)
                """,
                (list(asset_ids[offset : offset + 1_000]),),
            ).fetchall()
            result.extend(dict(row) for row in rows)
    return result


def _discover_candidates(
    config: BatchConfig,
    client: ClickHouseClient,
    source_min: int,
    source_max: int,
) -> tuple[list[Candidate], dict[str, Any]]:
    tape_rows = _candidate_tape_rows(client, source_min, source_max)
    metadata = _metadata_rows(
        sorted({_asset_decimal(str(row["asset_id"])) for row in tape_rows})
    )
    metadata_by_contract = {
        (
            str(row["asset_id"]),
            int(row["market_id"]),
            str(row["condition_id"]).lower(),
            str(row["outcome"]).upper(),
        ): row
        for row in metadata
    }
    requested_markets = set(config.market_ids)
    excluded_markets = set(config.exclude_market_ids)
    requested_categories = {item.lower() for item in config.categories}
    valid: list[Candidate] = []
    identity_conflicts = 0
    placeholder_count = 0
    for tape in tape_rows:
        market_id = int(tape["market_id"])
        if market_id in excluded_markets:
            continue
        if requested_markets and market_id not in requested_markets:
            continue
        asset_hex = str(tape["asset_id"]).lower()
        asset_id = _asset_decimal(asset_hex)
        key = (
            asset_id,
            market_id,
            str(tape["condition_id"]).lower(),
            str(tape["outcome"]).upper(),
        )
        item = metadata_by_contract.get(key)
        if item is None:
            identity_conflicts += 1
            continue
        category = str(item["category"]).lower()
        if category == "orderfilled-placeholder":
            placeholder_count += 1
            continue
        if requested_categories and category not in requested_categories:
            continue
        valid.append(
            Candidate(
                market_id=market_id,
                asset_id=asset_id,
                asset_hex=asset_hex,
                condition_id=str(tape["condition_id"]).lower(),
                outcome=str(tape["outcome"]).upper(),
                title=str(item["title"]),
                slug=str(item["slug"]),
                category=category,
                event_title=str(item["event_title"]),
                trade_count=int(tape["trade_count"]),
                trade_volume=Decimal(str(tape["trade_volume"])),
            )
        )

    # A market is one unit in --market-limit. Pick its most active outcome.
    by_market: dict[int, Candidate] = {}
    for row in valid:
        current = by_market.get(row.market_id)
        if current is None or (row.trade_count, row.outcome) > (
            current.trade_count,
            current.outcome,
        ):
            by_market[row.market_id] = row
    ordered = _round_robin(by_market.values(), include_depth=False)
    if config.market_limit > 0 and not requested_markets:
        pool_limit = max(
            config.market_limit,
            config.market_limit * config.candidate_multiplier,
        )
        ordered = ordered[:pool_limit]
    pool_before_l2 = len(ordered)
    coverage_l2_assets = _l2_asset_ids(config, ordered)
    l2_assets, l2_prefilter_mode = _candidate_l2_prefilter(
        config, ordered, coverage_l2_assets
    )
    ordered = [row for row in ordered if row.asset_id in l2_assets]
    stats = {
        "l2_asset_count": len(l2_assets),
        "coverage_l2_asset_count": len(coverage_l2_assets),
        "l2_prefilter_mode": l2_prefilter_mode,
        "trade_tape_pair_count": len(tape_rows),
        "l2_trade_tape_pair_intersection": len(ordered),
        "identity_conflicts": identity_conflicts,
        "placeholder_pairs_excluded": placeholder_count,
        "eligible_distinct_markets": len(by_market),
        "candidate_pool_before_l2": pool_before_l2,
        "candidate_pool_markets": len(ordered),
    }
    return ordered, stats


def _pair_predicate(candidates: Sequence[Candidate]) -> str:
    pairs: list[str] = []
    for row in candidates:
        if not HEX64.fullmatch(row.asset_hex):
            raise ValueError(f"invalid asset hex: {row.asset_hex!r}")
        pairs.append(f"({row.market_id}, '{row.asset_hex}')")
    if not pairs:
        raise ValueError("candidate pair list is empty")
    return "(" + ",".join(pairs) + ")"


def _load_trades(
    client: ClickHouseClient,
    candidates: Sequence[Candidate],
    source_min: int,
    source_max: int,
) -> list[V2TradePrint]:
    rows = client.query_json_rows(
        f"""
        SELECT
            trade_id,
            trade_group_id,
            market_id,
            condition_id,
            asset_id,
            outcome,
            block_number,
            block_time,
            tx_hash,
            tx_index,
            tx_index_source,
            price,
            size_shares,
            notional_usdc,
            aggressor_side,
            passive_side,
            source_log_indexes,
            source_fill_count
        FROM trade_prints_one_sided
        WHERE block_number BETWEEN {source_min} AND {source_max}
          AND (market_id, asset_id) IN {_pair_predicate(candidates)}
        ORDER BY block_number, tx_index, tx_hash, arrayMin(source_log_indexes), trade_id
        """,
        timeout_seconds=180,
    )
    return [trade_print_from_row(row) for row in rows]


def _load_block_timeline(
    client: ClickHouseClient, source_min: int, source_max: int
) -> tuple[tuple[datetime, ...], tuple[int, ...]]:
    rows = client.query_json_rows(
        f"""
        SELECT block_number, min(block_time) AS block_time
        FROM trade_prints_one_sided
        WHERE block_number BETWEEN {source_min} AND {source_max}
        GROUP BY block_number
        ORDER BY block_time, block_number
        """,
        timeout_seconds=120,
    )
    return (
        tuple(_parse_db_ts(str(row["block_time"])) for row in rows),
        tuple(int(row["block_number"]) for row in rows),
    )


def _load_l2_rows(
    config: BatchConfig, candidates: Sequence[Candidate]
) -> dict[str, list[dict[str, Any]]]:
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for the L2 archive") from exc
    asset_ids = [row.asset_id for row in candidates]
    dataset = _l2_dataset(config, candidates)
    lower = config.start - config.archive_baseline_lookback
    upper = config.end + config.latency
    columns = [
        "event_type",
        "timestamp",
        "timestamp_received",
        "collector_seq",
        "sequence_in_message",
        "bids",
        "asks",
        "side",
        "price",
        "size",
        "best_bid",
        "best_ask",
        "book_hash",
        "payload_hash",
        "source",
        "asset_id",
    ]
    table = dataset.to_table(
        columns=columns,
        filter=(
            pc.is_in(pc.field("asset_id"), value_set=pa.array(asset_ids))
            & pc.is_in(
                pc.field("source"),
                value_set=pa.array(
                    [
                        value.strip()
                        for value in config.l2_source.split(",")
                        if value.strip()
                    ]
                ),
            )
            & (pc.field("timestamp_received") >= pc.scalar(lower))
            & (pc.field("timestamp_received") <= pc.scalar(upper))
        ),
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in table.to_pylist():
        grouped[str(row["asset_id"])].append(row)
    for values in grouped.values():
        values.sort(
            key=lambda row: (
                row["timestamp_received"],
                int(row.get("collector_seq") or 0),
                int(row.get("sequence_in_message") or 0),
            )
        )
    return grouped


def _parse_levels(raw: Any) -> dict[Decimal, Decimal]:
    if raw is None or str(raw).strip() in {"", "<NA>", "nan"}:
        return {}
    parsed = json.loads(str(raw))
    levels: dict[Decimal, Decimal] = {}
    for row in parsed if isinstance(parsed, list) else ():
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        try:
            price = Decimal(str(row[0]))
            size = Decimal(str(row[1]))
        except (ArithmeticError, ValueError):
            continue
        if price > 0 and size > 0:
            levels[price] = size
    return levels


def _snapshot_book(
    bids: Mapping[Decimal, Decimal],
    asks: Mapping[Decimal, Decimal],
    baseline: Mapping[str, Any] | None,
    latest: Mapping[str, Any] | None,
    arrival: datetime,
    applied: int,
) -> dict[str, Any] | None:
    if baseline is None or latest is None or not bids or not asks:
        return None
    bid_rows = sorted(bids.items(), key=lambda item: item[0], reverse=True)
    ask_rows = sorted(asks.items(), key=lambda item: item[0])
    if bid_rows[0][0] >= ask_rows[0][0]:
        return None
    return {
        "bids": tuple(bid_rows),
        "asks": tuple(ask_rows),
        "best_bid": bid_rows[0][0],
        "best_bid_size": bid_rows[0][1],
        "best_ask": ask_rows[0][0],
        "best_ask_size": ask_rows[0][1],
        "spread": ask_rows[0][0] - bid_rows[0][0],
        "baseline_exchange_ts": baseline["timestamp"],
        "baseline_received_ts": baseline["timestamp_received"],
        "latest_exchange_ts": latest["timestamp"],
        "latest_received_ts": latest["timestamp_received"],
        "latest_collector_seq": int(latest.get("collector_seq") or 0),
        "book_hash": str(baseline.get("book_hash") or ""),
        "source": str(latest.get("source") or ""),
        "applied_depth_events": applied,
        "book_age_ms": max(
            0,
            int((arrival - latest["timestamp_received"]).total_seconds() * 1_000),
        ),
    }


def _books_at(
    rows: Sequence[Mapping[str, Any]], arrivals: Sequence[datetime]
) -> dict[datetime, dict[str, Any] | None]:
    targets = sorted(set(arrivals))
    result: dict[datetime, dict[str, Any] | None] = {}
    bids: dict[Decimal, Decimal] = {}
    asks: dict[Decimal, Decimal] = {}
    baseline: Mapping[str, Any] | None = None
    latest: Mapping[str, Any] | None = None
    applied = 0
    cursor = 0
    for arrival in targets:
        while cursor < len(rows) and rows[cursor]["timestamp_received"] <= arrival:
            row = rows[cursor]
            cursor += 1
            event_type = str(row["event_type"])
            if event_type == "book":
                bids = _parse_levels(row.get("bids"))
                asks = _parse_levels(row.get("asks"))
                baseline = row
                latest = row
                applied += 1
                continue
            if event_type != "price_change" or baseline is None:
                continue
            target = bids if str(row.get("side") or "").upper() == "BUY" else asks
            price = Decimal(str(row["price"]))
            size = Decimal(str(row["size"]))
            if size <= 0:
                target.pop(price, None)
            else:
                target[price] = size
            latest = row
            applied += 1
            if row.get("best_bid") is not None and row.get("best_ask") is not None:
                best_bid = Decimal(str(row["best_bid"]))
                best_ask = Decimal(str(row["best_ask"]))
                bids = {
                    price: size for price, size in bids.items() if price <= best_bid
                }
                asks = {
                    price: size for price, size in asks.items() if price >= best_ask
                }
        result[arrival] = _snapshot_book(bids, asks, baseline, latest, arrival, applied)
    return result


def _limit_price(
    signal_price: Decimal,
    config: BatchConfig,
    *,
    side: Literal["BUY", "SELL"],
) -> Decimal:
    if side == "BUY":
        raw = min(Decimal("0.999"), signal_price + config.limit_buffer)
        rounding = ROUND_CEILING
    else:
        raw = max(Decimal("0.001"), signal_price - config.limit_buffer)
        rounding = ROUND_FLOOR
    return (raw / config.tick_size).to_integral_value(
        rounding=rounding
    ) * config.tick_size


def _anchor_at(
    decision: datetime,
    block_times: Sequence[datetime],
    block_numbers: Sequence[int],
) -> int:
    index = bisect_right(block_times, decision) - 1
    if index < 0:
        raise ValueError(f"no causal block anchor before {decision.isoformat()}")
    if decision - block_times[index] > timedelta(seconds=60):
        raise ValueError(
            f"nearest causal block anchor is too far from {decision.isoformat()}"
        )
    return int(block_numbers[index])


def _candidate_probe(
    candidate: Candidate,
    trades: Sequence[V2TradePrint],
    rows: Sequence[Mapping[str, Any]],
    config: BatchConfig,
) -> Candidate | None:
    full_books = [row for row in rows if str(row["event_type"]) == "book"]
    if not trades or not full_books:
        return None
    usable_start = max(
        config.start,
        trades[0].block_time,
        full_books[0]["timestamp_received"],
    )
    usable_end = config.end - config.latency
    if usable_end <= usable_start:
        return None
    decision = usable_start + (usable_end - usable_start) / 2
    arrival = decision + config.latency
    trade_times = [trade.block_time for trade in trades]
    signal_index = bisect_right(trade_times, decision) - 1
    if signal_index < 0:
        return None
    book = _books_at(rows, [arrival])[arrival]
    if book is None:
        return None
    buy_limit = _limit_price(trades[signal_index].price, config, side="BUY")
    sell_limit = _limit_price(trades[signal_index].price, config, side="SELL")
    buy_depth = sum(
        (size for price, size in book["asks"] if price <= buy_limit), Decimal(0)
    )
    sell_depth = sum(
        (size for price, size in book["bids"] if price >= sell_limit), Decimal(0)
    )
    depth = max(buy_depth, sell_depth)
    return replace(
        candidate,
        probe_depth=depth,
        depth_regime=_depth_regime(depth, max(config.order_sizes)),
    )


def _decision_grid(start: datetime, end: datetime, count: int) -> tuple[datetime, ...]:
    if end <= start:
        return ()
    step = (end - start) / (count + 1)
    values = tuple(start + step * (index + 1) for index in range(count))
    return values if len(set(values)) == count else ()


def _build_order_specs(
    candidate: Candidate,
    trades: Sequence[V2TradePrint],
    rows: Sequence[Mapping[str, Any]],
    block_times: Sequence[datetime],
    block_numbers: Sequence[int],
    config: BatchConfig,
) -> tuple[list[dict[str, Any]], str]:
    full_books = [row for row in rows if str(row["event_type"]) == "book"]
    if not trades:
        return [], "NO_TRADE_TAPE"
    if not full_books:
        return [], "NO_FULL_L2_BASELINE"
    usable_start = max(
        config.start,
        trades[0].block_time,
        full_books[0]["timestamp_received"],
    )
    usable_end = config.end - config.latency
    decisions = _decision_grid(usable_start, usable_end, config.orders_per_market)
    if len(decisions) != config.orders_per_market:
        return [], "INSUFFICIENT_DISTINCT_DECISION_TIMES"
    arrivals = tuple(item + config.latency for item in decisions)
    books = _books_at(rows, arrivals)
    if any(books[arrival] is None for arrival in arrivals):
        return [], "INCOMPLETE_L2_BOOK_AT_DECISION"
    trade_times = [trade.block_time for trade in trades]
    pml2_profile = get_pml2_profile("realistic")
    specs: list[dict[str, Any]] = []
    for ordinal, (decision, arrival) in enumerate(
        zip(decisions, arrivals, strict=True)
    ):
        signal_index = bisect_right(trade_times, decision) - 1
        if signal_index < 0:
            return [], "NO_CAUSAL_SIGNAL_TRADE"
        signal = trades[signal_index]
        book = books[arrival]
        assert book is not None
        side = config.order_sides[ordinal % len(config.order_sides)]
        size_index = (ordinal // len(config.order_sides)) % len(config.order_sizes)
        order_size = config.order_sizes[size_index]
        limit = _limit_price(signal.price, config, side=side)
        levels = book["asks"] if side == "BUY" else book["bids"]
        raw_depth = sum(
            (
                level_size
                for price, level_size in levels
                if (price <= limit if side == "BUY" else price >= limit)
            ),
            Decimal(0),
        )
        effective_depth = raw_depth * pml2_profile.depth_haircut
        size_slug = format(order_size, "f").replace(".", "p")
        order_id = (
            f"batch-{candidate.market_id}-{candidate.outcome.lower()}-"
            f"{config.order_tif.lower()}-{side.lower()}-{size_slug}-{ordinal:06d}"
        )
        try:
            anchor_block = _anchor_at(decision, block_times, block_numbers)
        except ValueError:
            return [], "NO_NEARBY_CAUSAL_BLOCK_ANCHOR"
        specs.append(
            {
                "order_id": order_id,
                "candidate": candidate,
                "decision_ts": decision,
                "arrival_ts": arrival,
                "deadline_ts": arrival + timedelta(seconds=1),
                "anchor_block": anchor_block,
                "signal": signal,
                "limit_price": limit,
                "side": side,
                "size": order_size,
                "tif": config.order_tif,
                "book": {
                    **book,
                    "raw_depth": raw_depth,
                    "effective_depth": effective_depth,
                    "order_to_effective_depth": (
                        order_size / effective_depth if effective_depth > 0 else None
                    ),
                },
                "trades": trades,
            }
        )
    return specs, "OK"


def _v2_order(
    spec: Mapping[str, Any], config: BatchConfig, *, tif: str
) -> V2TakerOrder:
    candidate: Candidate = spec["candidate"]
    signal: V2TradePrint = spec["signal"]
    base = V2TakerOrder(
        order_id=str(spec["order_id"]),
        market_id=candidate.market_id,
        asset_id=candidate.asset_hex,
        side=cast(Literal["BUY", "SELL"], str(spec["side"])),
        limit_price=Decimal(str(spec["limit_price"])),
        size=Decimal(str(spec["size"])),
        signal_block=int(spec["anchor_block"]),
        signal_ts=spec["decision_ts"],
        latency_blocks=1,
        latency=config.latency,
        horizon_blocks=config.horizon_blocks,
        horizon=config.horizon,
        tif=tif,
        signal_source_trade_id=signal.trade_id,
        signal_source_tx_hash=signal.tx_hash,
        signal_source_log_indexes=signal.source_log_indexes,
        exclude_signal_source_trade=True,
    )
    profiled = with_v2_execution_profile(base, "probabilistic_taker_30s")
    return replace(
        profiled,
        latency_blocks=1,
        latency=config.latency,
        horizon_blocks=config.horizon_blocks,
        horizon=config.horizon,
        tif=tif,
    )


def _stable_seed(order_id: str) -> int:
    return int.from_bytes(hashlib.sha256(order_id.encode()).digest()[:4], "big")


def _v3_order(
    spec: Mapping[str, Any], config: BatchConfig, *, tif: str
) -> TradeOnlyOrder:
    candidate: Candidate = spec["candidate"]
    signal: V2TradePrint = spec["signal"]
    return TradeOnlyOrder(
        order_id=str(spec["order_id"]),
        market_id=candidate.market_id,
        asset_id=candidate.asset_hex,
        side=cast(Literal["BUY", "SELL"], str(spec["side"])),
        limit_price=Decimal(str(spec["limit_price"])),
        size=Decimal(str(spec["size"])),
        signal_block=int(spec["anchor_block"]),
        signal_ts=spec["decision_ts"],
        tif=tif,
        liquidity_intent=LiquidityIntent.TAKER,
        latency=config.latency,
        latency_blocks=1,
        horizon=config.horizon,
        horizon_blocks=config.horizon_blocks,
        lookback=config.lookback,
        lookback_blocks=config.lookback_blocks,
        signal_source_trade_id=signal.trade_id,
        random_seed=_stable_seed(str(spec["order_id"])),
        market_slug=candidate.slug,
        market_title=candidate.title,
        category=candidate.category,
    )


def _v2_row(result: V2OrderResult) -> dict[str, Any]:
    return {
        "status": result.status,
        "reason": result.reason_unfilled,
        "filled_size": result.filled_size,
        "avg_price": result.avg_price if result.filled_size > 0 else None,
        "p_fill": result.p_fill,
        "source_trade_ids": [fill.source_trade_id for fill in result.fills],
    }


def _v3_row(result: TradeOnlyOrderResult) -> dict[str, Any]:
    return {
        "status": result.status,
        "reason": result.reason,
        "filled_size": result.filled_size,
        "avg_price": result.avg_price if result.filled_size > 0 else None,
        "result_role": result.result_role,
        "calibration_status": result.calibration_status,
        "source_trade_ids": [
            source_id for fill in result.fills for source_id in fill.source_trade_ids
        ],
        "probability_bounds": result.probability_bounds,
        "capacity_bounds": result.capacity_bounds,
        "model_diagnostics": result.model_diagnostics,
        "fills": [fill.as_dict() for fill in result.fills],
    }


def _pml2_case(spec: Mapping[str, Any], config: BatchConfig) -> IndependentSnapshotCase:
    candidate: Candidate = spec["candidate"]
    book = spec["book"]
    order_id = str(spec["order_id"])
    run_id = f"batch-real:{order_id}"
    outcome = Outcome(candidate.outcome)
    limit_price = Decimal(str(spec["limit_price"]))
    pml2_profile = get_pml2_profile("realistic")
    side = RawOrderSide(str(spec["side"]))
    order_size = Decimal(str(spec["size"]))
    selected_levels: list[BookLevel] = []
    effective_depth = Decimal(0)
    source_levels = book["asks"] if side == RawOrderSide.BUY else book["bids"]
    for level_price, level_size in sorted(
        source_levels,
        key=lambda level: level[0],
        reverse=side == RawOrderSide.SELL,
    ):
        if (side == RawOrderSide.BUY and level_price > limit_price) or (
            side == RawOrderSide.SELL and level_price < limit_price
        ):
            break
        selected_levels.append(BookLevel(level_price, level_size))
        effective_depth += level_size * pml2_profile.depth_haircut
        if effective_depth >= order_size:
            break
    source_id = (
        f"legacy-batch:{candidate.market_id}:{candidate.asset_id}:"
        f"{book['latest_collector_seq']}:{int(spec['arrival_ts'].timestamp() * 1_000_000)}"
    )
    snapshot = BookSnapshotEvent(
        snapshot_id=source_id,
        condition_id=candidate.condition_id,
        market_id=str(candidate.market_id),
        asset_id=candidate.asset_id,
        outcome=outcome,
        exchange_ts=book["latest_exchange_ts"],
        source_received_ts=book["latest_received_ts"],
        local_ts=book["latest_received_ts"],
        book_epoch=0,
        bids=tuple(selected_levels) if side == RawOrderSide.SELL else (),
        asks=tuple(selected_levels) if side == RawOrderSide.BUY else (),
        source="xue_native_l2_archive",
        is_full_depth=False,
        is_truncated=True,
        depth_scope="ORDER_MARKETABLE_PREFIX_WITH_FULL_VISIBLE_DEPTH_AUDIT",
        tick_size=config.tick_size,
        book_hash=str(book["book_hash"] or source_id),
    )
    order = Pml2OrderIntent(
        run_id=run_id,
        order_id=order_id,
        strategy_id="batch-real-execution-comparison",
        condition_id=candidate.condition_id,
        market_id=str(candidate.market_id),
        asset_id=candidate.asset_id,
        outcome=outcome,
        side=side,
        size=order_size,
        limit_price=limit_price,
        tif=TimeInForce(str(spec["tif"])),
        signal_ts=spec["decision_ts"],
        observed_ts=spec["decision_ts"],
        submit_ts=spec["decision_ts"],
        entry_latency_ms=int(config.latency.total_seconds() * 1_000),
        response_latency_ms=0,
        venue_delay_ms=0,
    )
    return IndependentSnapshotCase(
        order=order,
        snapshot=snapshot,
        visible_depth_within_limit=Decimal(str(book["raw_depth"])),
    )


def _run_pml2(spec: Mapping[str, Any], config: BatchConfig) -> Pml2OrderResult:
    case = _pml2_case(spec, config)
    session = ReplayExecutionSession(run_id=case.order.run_id, profile="realistic")
    session.ingest_snapshot(case.snapshot)
    session.submit_order(case.order)
    session.run(until=spec["arrival_ts"] + timedelta(seconds=1))
    return session.result(case.order.order_id)


def _pml2_row(result: Pml2OrderResult) -> dict[str, Any]:
    return {
        "status": result.status.value,
        "reason": result.reason,
        "filled_size": result.filled_size,
        "avg_price": result.avg_fill_price,
        "source_event_ids": sorted(
            {source_id for fill in result.fills for source_id in fill.source_event_ids}
        ),
        "counterfactual_impact": result.counterfactual_impact,
        "fills": [fill.as_dict() for fill in result.fills],
    }


def _run_nautilus(
    specs: Sequence[Mapping[str, Any]], config: BatchConfig
) -> dict[str, dict[str, Any]]:
    if not config.nautilus_python.exists():
        raise RuntimeError(f"Nautilus Python is missing: {config.nautilus_python}")
    cases = []
    for spec in specs:
        book = spec["book"]
        cases.append(
            {
                "case_id": str(spec["order_id"]),
                "side": str(spec["side"]),
                "size": str(spec["size"]),
                "limit_price": str(spec["limit_price"]),
                "tick_size": str(config.tick_size),
                "tif": str(spec["tif"]),
                "bids": [[str(price), str(size)] for price, size in book["bids"]],
                "asks": [[str(price), str(size)] for price, size in book["asks"]],
            }
        )
    with tempfile.TemporaryDirectory(prefix="batch-real-l2-") as raw_dir:
        directory = Path(raw_dir)
        corpus = directory / "cases.json"
        output = directory / "results.json"
        corpus.write_text(json.dumps({"cases": cases}), encoding="utf-8")
        subprocess.run(
            [
                str(config.nautilus_python),
                str(PROJECT_ROOT / "scripts" / "offline_book_differential_worker.py"),
                "--engine",
                "nautilus",
                "--input",
                str(corpus),
                "--output",
                str(output),
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )
        rows = json.loads(output.read_text(encoding="utf-8"))["rows"]
    result: dict[str, dict[str, Any]] = {}
    sizes = {str(spec["order_id"]): Decimal(str(spec["size"])) for spec in specs}
    for row in rows:
        filled = Decimal(str(row["filled_size"]))
        order_size = sizes[str(row["case_id"])]
        result[str(row["case_id"])] = {
            "status": (
                "FILLED"
                if filled >= order_size
                else "PARTIAL"
                if filled > 0
                else "NO_FILL"
            ),
            "reason": "raw_static_l2_book_walk_control",
            "filled_size": filled,
            "avg_price": (
                Decimal(str(row["avg_fill_price"]))
                if row.get("avg_fill_price") is not None
                else None
            ),
            "fills": row.get("fills", []),
        }
    return result


def _run_models(
    specs: Sequence[Mapping[str, Any]],
    trades: Sequence[V2TradePrint],
    config: BatchConfig,
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, Any]]:
    selected_pairs = {
        (spec["candidate"].market_id, spec["candidate"].asset_hex) for spec in specs
    }
    selected_trades = [
        trade
        for trade in trades
        if (trade.market_id, trade.asset_id.lower()) in selected_pairs
    ]
    prepared = prepare_v2_trade_tape(selected_trades)
    output: dict[str, dict[str, dict[str, Any]]] = {}
    timings: dict[str, float] = {}
    backends: dict[str, str] = {}
    ordered_specs = sorted(
        specs,
        key=lambda spec: (
            spec["arrival_ts"],
            str(spec["order_id"]),
        ),
    )

    for model in config.models:
        started = perf_counter()
        if model in {"v2_fak", "v2_gtd"}:
            tif = "FAK" if model.endswith("fak") else "GTD"
            orders = [_v2_order(spec, config, tif=tif) for spec in ordered_specs]
            results, _, _ = replay_v2_taker_orders_with_diagnostics(
                orders, prepared_tape=prepared, backend="auto"
            )
            output[model] = {
                order.order_id: _v2_row(result)
                for order, result in zip(orders, results, strict=True)
            }
        elif model in {
            "v3_source_fak",
            "v3_source_gtd",
            "v3_source_contract",
            "v3_expected30",
            "v3_tif_expected5",
            "v3_tif_central",
            "v3_tif_recall",
            "v3_l2_reference",
            "v3_l2_expected",
            "v3_contract_probability",
            "v3_contract_expected",
        }:
            profile_by_model = {
                "v3_source_fak": "taker_source_confirmed",
                "v3_source_gtd": "taker_source_confirmed",
                "v3_source_contract": "taker_source_confirmed",
                "v3_expected30": "taker_hierarchical_expected_30s",
                "v3_tif_expected5": "taker_tif_aware_expected_5s",
                "v3_tif_central": "central_trade_only_tif_aware_5s",
                "v3_tif_recall": "central_trade_only_tif_aware_5s_recall",
                "v3_l2_reference": "central_trade_only_l2_reference_fak",
                "v3_l2_expected": ("central_trade_only_l2_reference_expected_fak"),
                "v3_contract_probability": ("taker_arrival_contract_probability_only"),
                "v3_contract_expected": "central_trade_only_contract_aware",
            }
            tif = (
                "GTD"
                if model in {"v3_source_gtd", "v3_expected30"}
                else str(config.order_tif)
                if model
                in {
                    "v3_source_contract",
                    "v3_contract_probability",
                    "v3_contract_expected",
                }
                else "FAK"
            )
            orders = [_v3_order(spec, config, tif=tif) for spec in ordered_specs]
            profile: str | Any = profile_by_model[model]
            if config.v3_probability_artifact is not None and model in {
                "v3_l2_reference",
                "v3_l2_expected",
            }:
                profile = replace(
                    get_trade_only_profile(profile_by_model[model]),
                    probability_profile_path=str(
                        config.v3_probability_artifact.resolve()
                    ),
                )
            if model in {"v3_contract_probability", "v3_contract_expected"}:
                replacements: dict[str, Any] = {}
                if config.v3_fak_probability_artifact is not None:
                    replacements["probability_profile_path"] = str(
                        config.v3_fak_probability_artifact.resolve()
                    )
                if config.v3_fok_probability_artifact is not None:
                    replacements["full_fill_probability_profile_path"] = str(
                        config.v3_fok_probability_artifact.resolve()
                    )
                if replacements:
                    profile = replace(
                        get_trade_only_profile(profile_by_model[model]),
                        **replacements,
                    )
            results, _ = replay_trade_only_orders(
                orders,
                None,
                profile,
                ledger_id=f"batch-shared:{model}",
                prepared_tape=prepared,
                backend="auto",
            )
            output[model] = {
                order.order_id: _v3_row(result)
                for order, result in zip(orders, results, strict=True)
            }
        elif model in {"pml2_fak", "pml2_fok"}:
            expected_tif = "FOK" if model.endswith("fok") else "FAK"
            if config.order_tif != expected_tif:
                raise ValueError(
                    f"{model} requires --order-tif {expected_tif}, "
                    f"got {config.order_tif}"
                )
            cases = [_pml2_case(spec, config) for spec in ordered_specs]
            results, diagnostics = replay_independent_snapshot_takers(
                cases,
                profile="realistic",
                backend=config.pml2_backend,
            )
            backends[model] = diagnostics.backend
            output[model] = {
                case.order.order_id: _pml2_row(result)
                for case, result in zip(cases, results, strict=True)
            }
        elif model in {"nautilus_fak", "nautilus_fok"}:
            expected_tif = "FOK" if model.endswith("fok") else "FAK"
            if config.order_tif != expected_tif:
                raise ValueError(
                    f"{model} requires --order-tif {expected_tif}, "
                    f"got {config.order_tif}"
                )
            output[model] = _run_nautilus(specs, config)
        else:
            raise ValueError(f"unsupported model: {model}")
        timings[model] = perf_counter() - started
    return output, {
        "trade_rows_indexed": prepared.trade_rows_indexed,
        "trade_groups": prepared.trade_groups,
        "index_build_sec": prepared.index_build_sec,
        "model_seconds": timings,
        "model_backends": backends,
    }


def _future_evidence(spec: Mapping[str, Any], config: BatchConfig) -> dict[str, Any]:
    arrival = spec["arrival_ts"]
    deadline = spec["deadline_ts"]
    limit = Decimal(str(spec["limit_price"]))
    side = str(spec["side"])
    candidates = [
        trade for trade in spec["trades"] if arrival <= trade.block_time <= deadline
    ]
    same_side = [trade for trade in candidates if trade.aggressor_side == side]
    raw = [
        trade
        for trade in same_side
        if (trade.price <= limit if side == "BUY" else trade.price >= limit)
    ]
    buffered = [
        trade
        for trade in raw
        if (
            trade.price + Decimal("0.005") <= limit
            if side == "BUY"
            else trade.price - Decimal("0.005") >= limit
        )
    ]
    return {
        "all_trade_count": len(candidates),
        "same_side_count": len(same_side),
        "same_side_buy_count": len(same_side) if side == "BUY" else 0,
        "raw_limit_eligible_count": len(raw),
        "central_buffer_eligible_count": len(buffered),
        "central_buffer_eligible_volume": sum(
            (trade.size for trade in buffered), Decimal(0)
        ),
        "eligible_trade_ids": [trade.trade_id for trade in buffered],
        "window_seconds": config.horizon.total_seconds(),
    }


def _assemble_rows(
    specs: Sequence[Mapping[str, Any]],
    model_rows: Mapping[str, Mapping[str, Mapping[str, Any]]],
    config: BatchConfig,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in specs:
        candidate: Candidate = spec["candidate"]
        signal: V2TradePrint = spec["signal"]
        book = spec["book"]
        decision = spec["decision_ts"]
        trailing = [
            trade
            for trade in spec["trades"]
            if decision - config.lookback <= trade.block_time <= decision
        ]
        arrival = spec["arrival_ts"]
        feature_order = _v3_order(spec, config, tif=str(spec["tif"]))
        arrival_block = feature_order.arrival_block
        start_block = (
            max(0, arrival_block - feature_order.lookback_blocks)
            if arrival_block is not None
            else None
        )
        feature_trailing = [
            trade
            for trade in spec["trades"]
            if arrival - config.lookback <= trade.block_time < arrival
            and (start_block is None or trade.block_number >= start_block)
            and (arrival_block is None or trade.block_number < arrival_block)
            and trade.trade_id != feature_order.signal_source_trade_id
        ]
        fill_only_features = extract_orderfilled_probability_features(
            feature_order,
            feature_trailing,
            lookback=config.lookback,
            tick_size=config.tick_size,
            presorted=True,
            same_market_asset=True,
        )
        rows.append(
            {
                "order_id": spec["order_id"],
                "market_id": candidate.market_id,
                "condition_id": candidate.condition_id,
                "asset_id": candidate.asset_id,
                "outcome": candidate.outcome,
                "title": candidate.title,
                "slug": candidate.slug,
                "category": candidate.category,
                "decision_ts": decision,
                "arrival_ts": spec["arrival_ts"],
                "deadline_ts": spec["deadline_ts"],
                "side": spec["side"],
                "tif": spec["tif"],
                "amount_unit": "SHARES",
                "size": spec["size"],
                "limit_price": spec["limit_price"],
                "signal_trade": {
                    "trade_id": signal.trade_id,
                    "block_number": signal.block_number,
                    "block_time": signal.block_time,
                    "aggressor_side": signal.aggressor_side,
                    "price": signal.price,
                    "size": signal.size,
                },
                "trailing_trade_count": len(trailing),
                "trailing_volume": sum((trade.size for trade in trailing), Decimal(0)),
                "fill_only_feature_as_of_ts": arrival,
                "fill_only_feature_contract": ARRIVAL_FEATURE_CONTRACT,
                "fill_only_features": fill_only_features.as_dict(),
                "best_bid": book["best_bid"],
                "best_ask": book["best_ask"],
                "spread": book["spread"],
                "raw_visible_side_depth": book["raw_depth"],
                "haircut_visible_side_depth": book["effective_depth"],
                "raw_visible_ask_depth": (
                    book["raw_depth"] if spec["side"] == "BUY" else Decimal(0)
                ),
                "haircut_visible_ask_depth": (
                    book["effective_depth"] if spec["side"] == "BUY" else Decimal(0)
                ),
                "order_to_haircut_depth_ratio": book["order_to_effective_depth"],
                "book_age_ms": book["book_age_ms"],
                "book_source": book["source"],
                "book_evidence_status": "XUE_NATIVE_EVENT_STREAM_RECONSTRUCTED_WITHOUT_INTERVAL_COVERAGE_JOIN",
                "depth_regime": _depth_regime(
                    Decimal(str(book["raw_depth"])), Decimal(str(spec["size"]))
                ),
                "activity_regime": _activity_regime(
                    fill_only_features.trailing_same_count
                    + fill_only_features.trailing_opposite_count
                ),
                "spread_regime": _spread_regime(Decimal(str(book["spread"]))),
                "future_source_evidence": _future_evidence(spec, config),
                "models": {
                    model: values[str(spec["order_id"])]
                    for model, values in model_rows.items()
                },
            }
        )
    return rows


def _validate_rows(
    rows: Sequence[Mapping[str, Any]], config: BatchConfig
) -> dict[str, Any]:
    if len({str(row["order_id"]) for row in rows}) != len(rows):
        raise RuntimeError("order ids are not unique")
    pml2_source_count = 0
    source_fill_count = 0
    for row in rows:
        limit = Decimal(str(row["limit_price"]))
        order_size = Decimal(str(row["size"]))
        raw_depth = Decimal(str(row["raw_visible_side_depth"]))
        side = str(row["side"])
        for model, result in row["models"].items():
            filled = Decimal(str(result["filled_size"]))
            if not Decimal(0) <= filled <= order_size:
                raise RuntimeError(
                    f"{model} violates order size for {row['order_id']}: {filled}"
                )
            if model in SOURCE_MODELS and filled > 0:
                source_fill_count += 1
                if not result["source_trade_ids"]:
                    raise RuntimeError(
                        f"{model} has no source trade for {row['order_id']}"
                    )
            if model in {"pml2_fak", "pml2_fok"} and filled > 0:
                pml2_source_count += 1
                if not result["source_event_ids"]:
                    raise RuntimeError(
                        f"PML2 has no source event for {row['order_id']}"
                    )
                for fill in result["fills"]:
                    price = Decimal(str(fill["raw_price"]))
                    if (side == "BUY" and price > limit) or (
                        side == "SELL" and price < limit
                    ):
                        raise RuntimeError(
                            f"PML2 violates {side} limit for {row['order_id']}"
                        )
                if model == "pml2_fok" and filled != order_size:
                    raise RuntimeError(
                        f"PML2 violates FOK atomicity for {row['order_id']}"
                    )
            if model in {"nautilus_fak", "nautilus_fok"}:
                if filled > min(order_size, raw_depth):
                    raise RuntimeError(
                        f"Nautilus exceeds raw depth for {row['order_id']}"
                    )
                for price, _size in result["fills"]:
                    price_value = Decimal(str(price))
                    if (side == "BUY" and price_value > limit) or (
                        side == "SELL" and price_value < limit
                    ):
                        raise RuntimeError(
                            f"Nautilus violates {side} limit for {row['order_id']}"
                        )
                if model == "nautilus_fok" and filled not in {
                    Decimal(0),
                    order_size,
                }:
                    raise RuntimeError(
                        f"Nautilus violates FOK atomicity for {row['order_id']}"
                    )
    return {
        "status": "PASS",
        "orders_checked": len(rows),
        "unique_order_ids": len(rows),
        "source_confirmed_positive_results": source_fill_count,
        "pml2_positive_results_with_l2_source": pml2_source_count,
        "checks": [
            "unique_order_ids",
            "market_token_condition_outcome_identity",
            "order_size_bounds",
            "side_aware_limit_price",
            "orderfilled_source_trade_required",
            "pml2_l2_source_event_required",
            "nautilus_raw_static_depth_bound",
        ],
    }


def _summarize(rows: Sequence[Mapping[str, Any]], model: str) -> dict[str, Any]:
    values = [row["models"][model] for row in rows]
    return {
        "orders": len(values),
        "positive_quantity_orders": sum(
            Decimal(str(value["filled_size"])) > 0 for value in values
        ),
        "quantity": sum(
            (Decimal(str(value["filled_size"])) for value in values), Decimal(0)
        ),
        "expected_filled_order_equivalents": sum(
            (_expected_fill_probability(value) for value in values), Decimal(0)
        ),
        "statuses": dict(Counter(str(value["status"]) for value in values)),
        "reasons": dict(Counter(str(value.get("reason") or "") for value in values)),
    }


def _ratio_or_none(
    numerator: int | Decimal, denominator: int | Decimal
) -> float | None:
    if Decimal(str(denominator)) == 0:
        return None
    return float(Decimal(str(numerator)) / Decimal(str(denominator)))


def _expected_fill_probability(value: Mapping[str, Any]) -> Decimal:
    filled = Decimal(str(value.get("filled_size") or 0))
    fills = value.get("fills")
    source_confirmed = str(value.get("evidence_tier") or "").startswith(
        "A_SOURCE_CONFIRMED"
    ) or (
        isinstance(fills, Sequence)
        and any(
            isinstance(fill, Mapping)
            and str(fill.get("evidence_tier") or "").startswith("A_SOURCE_CONFIRMED")
            for fill in fills
        )
    )
    if filled > 0 and source_confirmed:
        return Decimal(1)
    bounds = value.get("probability_bounds")
    if isinstance(bounds, Mapping):
        for key in (
            "full_fill_probability",
            "any_fill_execution_horizon",
            "model_target_horizon",
            "horizon",
        ):
            if bounds.get(key) is not None:
                return min(Decimal(1), max(Decimal(0), Decimal(str(bounds[key]))))
    return Decimal(1) if filled > 0 else Decimal(0)


def _binary_reference_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    model: str,
    reference: str,
) -> dict[str, Any]:
    labeled: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    excluded: Counter[str] = Counter()
    reference_ready_samples = 0
    all_expected_positive_orders = Decimal(0)
    all_reference_positive_orders = Decimal(0)
    all_expected_quantity = Decimal(0)
    all_reference_quantity = Decimal(0)
    all_brier_sum = Decimal(0)
    for row in rows:
        predicted = row["models"][model]
        observed = row["models"][reference]
        if reference.startswith("pml2_") and observed["status"] == "DATA_NOT_READY":
            excluded["PML2_DATA_NOT_READY"] += 1
            continue
        reference_ready_samples += 1
        predicted_size = Decimal(str(predicted["filled_size"]))
        observed_size = Decimal(str(observed["filled_size"]))
        expected_probability = _expected_fill_probability(predicted)
        observed_positive = observed_size > 0
        all_expected_positive_orders += expected_probability
        all_reference_positive_orders += Decimal(1) if observed_positive else Decimal(0)
        all_expected_quantity += predicted_size
        all_reference_quantity += observed_size
        all_brier_sum += (expected_probability - Decimal(int(observed_positive))) ** 2
        if predicted.get("reason") in {
            "orderfilled_probability_model_out_of_domain",
            "probability_model_contract_violation",
        }:
            excluded["MODEL_OUT_OF_DOMAIN"] += 1
            continue
        labeled.append((predicted, observed))

    tp = fp = tn = fn = 0
    quantity_abs_error = Decimal(0)
    quantity_signed_error = Decimal(0)
    price_abs_error = Decimal(0)
    price_pairs = 0
    expected_positive_orders = Decimal(0)
    reference_positive_orders = Decimal(0)
    expected_quantity = Decimal(0)
    reference_quantity = Decimal(0)
    brier_sum = Decimal(0)
    for predicted, observed in labeled:
        predicted_size = Decimal(str(predicted["filled_size"]))
        observed_size = Decimal(str(observed["filled_size"]))
        expected_probability = _expected_fill_probability(predicted)
        predicted_positive = predicted_size > 0
        observed_positive = observed_size > 0
        if predicted_positive and observed_positive:
            tp += 1
        elif predicted_positive:
            fp += 1
        elif observed_positive:
            fn += 1
        else:
            tn += 1
        difference = predicted_size - observed_size
        quantity_abs_error += abs(difference)
        quantity_signed_error += difference
        expected_positive_orders += expected_probability
        reference_positive_orders += Decimal(1) if observed_positive else Decimal(0)
        expected_quantity += predicted_size
        reference_quantity += observed_size
        brier_sum += (expected_probability - Decimal(int(observed_positive))) ** 2
        if (
            predicted_positive
            and observed_positive
            and predicted.get("avg_price") is not None
            and observed.get("avg_price") is not None
        ):
            price_abs_error += abs(
                Decimal(str(predicted["avg_price"]))
                - Decimal(str(observed["avg_price"]))
            )
            price_pairs += 1

    samples = len(labeled)
    return {
        "model": model,
        "reference": reference,
        "reference_role": (
            "EVENT_DRIVEN_L2_EXECUTABILITY_REFERENCE"
            if reference.startswith("pml2_")
            else "RAW_STATIC_L2_IMPLEMENTATION_CONTROL"
        ),
        "reference_ready_samples": reference_ready_samples,
        "model_supported_samples": samples,
        "model_support_rate": _ratio_or_none(samples, reference_ready_samples),
        "samples": samples,
        "excluded": dict(excluded),
        "true_positive": tp,
        "false_positive": fp,
        "true_negative": tn,
        "false_negative": fn,
        "precision": _ratio_or_none(tp, tp + fp),
        "recall": _ratio_or_none(tp, tp + fn),
        "false_positive_rate": _ratio_or_none(fp, fp + tn),
        "false_negative_rate": _ratio_or_none(fn, tp + fn),
        "accuracy": _ratio_or_none(tp + tn, samples),
        "predicted_positive_rate": _ratio_or_none(tp + fp, samples),
        "reference_positive_rate": _ratio_or_none(tp + fn, samples),
        "positive_count_ratio": _ratio_or_none(tp + fp, tp + fn),
        "expected_positive_orders": expected_positive_orders,
        "reference_positive_orders": reference_positive_orders,
        "expected_positive_order_ratio": _ratio_or_none(
            expected_positive_orders, reference_positive_orders
        ),
        "expected_quantity": expected_quantity,
        "reference_quantity": reference_quantity,
        "expected_quantity_ratio": _ratio_or_none(
            expected_quantity, reference_quantity
        ),
        "probability_brier_score": (brier_sum / Decimal(samples) if samples else None),
        "all_sample_expected_positive_orders": all_expected_positive_orders,
        "all_sample_reference_positive_orders": all_reference_positive_orders,
        "all_sample_expected_positive_order_ratio": _ratio_or_none(
            all_expected_positive_orders, all_reference_positive_orders
        ),
        "all_sample_expected_quantity": all_expected_quantity,
        "all_sample_reference_quantity": all_reference_quantity,
        "all_sample_expected_quantity_ratio": _ratio_or_none(
            all_expected_quantity, all_reference_quantity
        ),
        "all_sample_probability_brier_score": (
            all_brier_sum / Decimal(reference_ready_samples)
            if reference_ready_samples
            else None
        ),
        "mean_abs_quantity_error": (
            quantity_abs_error / Decimal(samples) if samples else None
        ),
        "mean_signed_quantity_error": (
            quantity_signed_error / Decimal(samples) if samples else None
        ),
        "mean_abs_price_error_on_joint_fills": (
            price_abs_error / Decimal(price_pairs) if price_pairs else None
        ),
        "joint_fill_price_pairs": price_pairs,
    }


def _reference_comparisons(
    rows: Sequence[Mapping[str, Any]], models: Sequence[str]
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    references = [
        item
        for item in ("pml2_fak", "nautilus_fak", "pml2_fok", "nautilus_fok")
        if item in models
    ]
    for model in models:
        if model in references:
            continue
        result[model] = {
            reference: _binary_reference_metrics(rows, model=model, reference=reference)
            for reference in references
        }
    if "pml2_fak" in models and "nautilus_fak" in models:
        result["pml2_vs_nautilus_control"] = _binary_reference_metrics(
            rows, model="pml2_fak", reference="nautilus_fak"
        )
    if "pml2_fok" in models and "nautilus_fok" in models:
        result["pml2_vs_nautilus_fok_control"] = _binary_reference_metrics(
            rows, model="pml2_fok", reference="nautilus_fok"
        )
    return result


def _summaries_by(
    rows: Sequence[Mapping[str, Any]], models: Sequence[str], dimension: str
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row[dimension])].append(row)
    return {
        model: {
            name: _summarize(values, model) for name, values in sorted(groups.items())
        }
        for model in models
    }


def _write_outputs(
    rows: Sequence[Mapping[str, Any]], summary: Mapping[str, Any], output_dir: Path
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(
        json.dumps(_json_ready(summary), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "orders.jsonl").open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_json_ready(row), ensure_ascii=False) + "\n")
    markets: dict[int, dict[str, Any]] = {}
    for row in rows:
        markets.setdefault(
            int(row["market_id"]),
            {
                key: row[key]
                for key in (
                    "market_id",
                    "condition_id",
                    "asset_id",
                    "outcome",
                    "title",
                    "slug",
                    "category",
                )
            },
        )
    (output_dir / "markets.json").write_text(
        json.dumps(_json_ready(list(markets.values())), ensure_ascii=False, indent=2)
        + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Batch Real Execution Model Comparison",
        "",
        "This is an independent-order execution-opportunity comparison, not portfolio PnL.",
        "",
        "| Model | Positive orders | Quantity | Statuses |",
        "| --- | ---: | ---: | --- |",
    ]
    for model, values in summary["models"].items():
        lines.append(
            f"| `{model}` | {values['positive_quantity_orders']}/{values['orders']} "
            f"| {values['quantity']} | `{json.dumps(values['statuses'], sort_keys=True)}` |"
        )
    lines.extend(
        [
            "",
            "## Evidence Boundary",
            "",
            "- Fill-only source models require post-arrival OrderFilled evidence.",
            "- V3 expected quantity is modeled, not an observed fill.",
            "- PML2 uses reconstructed L2 with its configured depth haircut and impact gate.",
            "- Nautilus is a raw static-book control, not baseline truth.",
            "- This comparator reconstructs the XUE Native L2 event stream but does not yet join interval coverage manifests.",
            "",
        ]
    )
    (output_dir / "README.md").write_text("\n".join(lines), encoding="utf-8")


def _cohort_record(
    config: BatchConfig,
    specs: Sequence[Mapping[str, Any]],
    *,
    source_min: int,
    source_max: int,
) -> dict[str, Any]:
    pairs: dict[tuple[int, str], dict[str, Any]] = {}
    order_rows: list[dict[str, Any]] = []
    for spec in specs:
        candidate: Candidate = spec["candidate"]
        key = (candidate.market_id, candidate.asset_hex)
        pair = pairs.setdefault(
            key,
            {
                "market_id": candidate.market_id,
                "asset_id": candidate.asset_hex,
                "first_decision_ts": spec["decision_ts"],
                "last_decision_ts": spec["decision_ts"],
            },
        )
        pair["first_decision_ts"] = min(pair["first_decision_ts"], spec["decision_ts"])
        pair["last_decision_ts"] = max(pair["last_decision_ts"], spec["decision_ts"])
        order_rows.append(
            {
                "order_id": str(spec["order_id"]),
                "market_id": candidate.market_id,
                "asset_id": candidate.asset_hex,
                "decision_ts": spec["decision_ts"],
                "side": spec["side"],
                "tif": spec["tif"],
                "size": spec["size"],
                "limit_price": spec["limit_price"],
            }
        )
    identity = {
        "start": config.start,
        "end": config.end,
        "source_min_block": source_min,
        "source_max_block": source_max,
        "order_size": config.order_size,
        "order_sizes": config.order_sizes,
        "order_sides": config.order_sides,
        "order_tif": config.order_tif,
        "latency": config.latency,
        "horizon": config.horizon,
        "pairs": sorted(
            pairs.values(), key=lambda row: (row["market_id"], row["asset_id"])
        ),
        "orders": sorted(order_rows, key=lambda row: row["order_id"]),
    }
    rendered = json.dumps(
        _json_ready(identity), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return {
        "schema_version": "fill_only_cross_validation_cohort_v1",
        "fingerprint": hashlib.sha256(rendered.encode("utf-8")).hexdigest(),
        "split": config.cohort_split,
        "start": config.start,
        "end": config.end,
        "source_min_block": source_min,
        "source_max_block": source_max,
        "market_asset_pairs": sorted(
            pairs.values(), key=lambda row: (row["market_id"], row["asset_id"])
        ),
        "orders": len(specs),
        "output_dir": config.output_dir,
    }


def _registry_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_SH)
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"invalid cohort registry row {path}:{line_number}"
                ) from exc
            if not isinstance(row, dict):
                raise TypeError(f"invalid cohort registry row {path}:{line_number}")
            rows.append(row)
    return rows


def _cohort_overlap(current: Mapping[str, Any], previous: Mapping[str, Any]) -> bool:
    current_pairs = {
        (int(row["market_id"]), str(row["asset_id"]).lower())
        for row in current.get("market_asset_pairs", [])
    }
    previous_pairs = {
        (int(row["market_id"]), str(row["asset_id"]).lower())
        for row in previous.get("market_asset_pairs", [])
    }
    if not current_pairs.intersection(previous_pairs):
        return False
    current_start = _parse_ts(str(current["start"]))
    current_end = _parse_ts(str(current["end"]))
    previous_start = _parse_ts(str(previous["start"]))
    previous_end = _parse_ts(str(previous["end"]))
    return current_start < previous_end and previous_start < current_end


def _assert_new_cohort(config: BatchConfig, record: Mapping[str, Any]) -> None:
    if config.cohort_registry is None or config.allow_cohort_reuse:
        return
    conflicts = [
        row
        for row in _registry_records(config.cohort_registry)
        if row.get("fingerprint") == record.get("fingerprint")
        or _cohort_overlap(record, row)
    ]
    if conflicts:
        details = [
            {
                "fingerprint": row.get("fingerprint"),
                "split": row.get("split"),
                "start": row.get("start"),
                "end": row.get("end"),
                "output_dir": row.get("output_dir"),
            }
            for row in conflicts[:5]
        ]
        raise RuntimeError(
            "cohort reuses a registered market/time interval; choose new data or "
            f"pass --allow-cohort-reuse for an explicitly labeled sensitivity: {details}"
        )


def _register_cohort(config: BatchConfig, record: Mapping[str, Any]) -> None:
    if config.cohort_registry is None:
        return
    config.cohort_registry.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        **dict(record),
        "registered_at": datetime.now(UTC),
        "reuse_override": config.allow_cohort_reuse,
    }
    with config.cohort_registry.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.write(json.dumps(_json_ready(payload), ensure_ascii=False) + "\n")
        handle.flush()


def run(config: BatchConfig) -> dict[str, Any]:
    started = perf_counter()
    stages: dict[str, float] = {}
    stage_started = perf_counter()
    cached = _load_prepared_input(config)
    stages["prepared_input_cache_load"] = perf_counter() - stage_started
    if cached is not None:
        source = dict(cached["source"])
        source_min = int(source["source_min_block"])
        source_max = int(source["source_max_block"])
        discovery = dict(cached["discovery"])
        selected = list(cached["selected"])
        specs = list(cached["specs"])
        trades = list(cached["trades"])
        probe_rejections = cached["probe_rejections"]
        build_rejections = cached["build_rejections"]
        probe_valid_markets = int(cached["probe_valid_markets"])
        archive_hour_partitions = cached["archive_hour_partitions"]
        prepared_cache = {
            "status": "HIT",
            "path": cached["cache_path"],
            "sha256": cached["cache_sha256"],
        }
    else:
        client = ClickHouseClient()
        stage_started = perf_counter()
        start_anchor, end_anchor, source_min, source_max = _resolve_source_bounds(
            config, client
        )
        stages["source_bounds"] = perf_counter() - stage_started
        source = {
            "start_anchor": start_anchor,
            "end_anchor": end_anchor,
            "source_min_block": source_min,
            "source_max_block": source_max,
        }
        stage_started = perf_counter()
        candidates, discovery = _discover_candidates(
            config, client, source_min, source_max
        )
        stages["candidate_discovery"] = perf_counter() - stage_started
        if not candidates:
            raise RuntimeError("no identity-valid L2/trade-tape candidate markets")
        stage_started = perf_counter()
        l2_rows = _load_l2_rows(config, candidates)
        stages["l2_load_and_decode"] = perf_counter() - stage_started
        stage_started = perf_counter()
        trades = _load_trades(client, candidates, source_min, source_max)
        block_times, block_numbers = _load_block_timeline(
            client, source_min, source_max
        )
        stages["trade_tape_and_block_timeline"] = perf_counter() - stage_started
        trades_by_pair: dict[tuple[int, str], list[V2TradePrint]] = defaultdict(list)
        for trade in trades:
            trades_by_pair[(trade.market_id, trade.asset_id.lower())].append(trade)

        stage_started = perf_counter()
        probed: list[Candidate] = []
        probe_rejections = Counter()
        for candidate in candidates:
            rows = l2_rows.get(candidate.asset_id, [])
            pair_trades = trades_by_pair.get(
                (candidate.market_id, candidate.asset_hex), []
            )
            enriched = _candidate_probe(candidate, pair_trades, rows, config)
            if enriched is None:
                probe_rejections["NO_COMPLETE_PROBE"] += 1
            else:
                probed.append(enriched)
        ordered_candidates = _round_robin(probed, include_depth=True)

        selected = []
        specs = []
        build_rejections = Counter()
        target = (
            len(config.market_ids)
            if config.market_ids
            else len(ordered_candidates)
            if config.market_limit == 0
            else config.market_limit
        )
        for candidate in ordered_candidates:
            pair_trades = trades_by_pair.get(
                (candidate.market_id, candidate.asset_hex), []
            )
            market_specs, reason = _build_order_specs(
                candidate,
                pair_trades,
                l2_rows.get(candidate.asset_id, []),
                block_times,
                block_numbers,
                config,
            )
            if len(market_specs) != config.orders_per_market:
                build_rejections[reason] += 1
                continue
            selected.append(candidate)
            specs.extend(market_specs)
            if len(selected) >= target:
                break
        if len(selected) < target:
            raise RuntimeError(
                f"requested {target} markets but only {len(selected)} produced "
                f"{config.orders_per_market} valid orders; "
                f"probe_rejections={dict(probe_rejections)}; "
                f"build_rejections={dict(build_rejections)}"
            )
        probe_valid_markets = len(probed)
        archive_hour_partitions = _archive_hour_paths(config)
        stages["order_input_build"] = perf_counter() - stage_started
        prepared_cache = {"status": "DISABLED"}
        if config.prepared_input_cache is not None:
            stage_started = perf_counter()
            _write_prepared_input(
                config,
                _prepared_input_payload(
                    config=config,
                    source=source,
                    discovery=discovery,
                    selected=selected,
                    specs=specs,
                    trades=trades,
                    probe_rejections=probe_rejections,
                    build_rejections=build_rejections,
                    probe_valid_markets=probe_valid_markets,
                    archive_hour_partitions=archive_hour_partitions,
                ),
            )
            data_path, _, _ = _prepared_input_paths(config)
            prepared_cache = {
                "status": "MISS_WRITTEN",
                "path": data_path,
                "sha256": _sha256_file(data_path),
            }
            stages["prepared_input_cache_write"] = perf_counter() - stage_started

    cohort_record = _cohort_record(
        config, specs, source_min=source_min, source_max=source_max
    )
    _assert_new_cohort(config, cohort_record)
    stage_started = perf_counter()
    model_rows, performance = _run_models(specs, trades, config)
    stages["models"] = perf_counter() - stage_started
    stage_started = perf_counter()
    rows = _assemble_rows(specs, model_rows, config)
    validation = _validate_rows(rows, config)
    models = {model: _summarize(rows, model) for model in config.models}
    stages["assemble_validate_summarize"] = perf_counter() - stage_started
    summary: dict[str, Any] = {
        "schema_version": "batch_real_execution_model_comparison_v1",
        "generated_at": datetime.now(UTC),
        "comparison_contract": "TIME_ORDERED_SHARED_CAPACITY_PER_EXECUTION_MODEL",
        "config": config,
        "source": {
            **source,
            "archive": config.archive,
            "l2_evidence": "XUE_NATIVE_EVENT_STREAM_RECONSTRUCTED_WITHOUT_INTERVAL_COVERAGE_JOIN",
            "archive_hour_partitions": archive_hour_partitions,
            "prepared_input_cache": prepared_cache,
        },
        "discovery": {
            **discovery,
            "probe_valid_markets": probe_valid_markets,
            "probe_rejections": dict(probe_rejections),
            "build_rejections": dict(build_rejections),
        },
        "cohort": {
            **cohort_record,
            "markets": len(selected),
            "orders": len(rows),
            "orders_per_market": config.orders_per_market,
            "categories": dict(Counter(item.category for item in selected)),
            "probe_depth_regimes": dict(
                Counter(item.depth_regime for item in selected)
            ),
            "order_depth_regimes": dict(
                Counter(str(row["depth_regime"]) for row in rows)
            ),
            "activity_regimes": dict(
                Counter(str(row["activity_regime"]) for row in rows)
            ),
            "spread_regimes": dict(Counter(str(row["spread_regime"]) for row in rows)),
        },
        "models": models,
        "by_category": _summaries_by(rows, config.models, "category"),
        "by_depth_regime": _summaries_by(rows, config.models, "depth_regime"),
        "by_activity_regime": _summaries_by(rows, config.models, "activity_regime"),
        "reference_comparisons": _reference_comparisons(rows, config.models),
        "performance": {
            **performance,
            "stage_seconds": stages,
            "total_seconds": 0,
        },
        "validation": validation,
    }
    stage_started = perf_counter()
    _write_outputs(rows, _json_ready(summary), config.output_dir)
    stages["result_write"] = perf_counter() - stage_started
    _register_cohort(config, cohort_record)
    summary["performance"]["stage_seconds"] = stages
    summary["performance"]["total_seconds"] = perf_counter() - started
    (config.output_dir / "summary.json").write_text(
        json.dumps(_json_ready(summary), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def _config_from_args(args: argparse.Namespace) -> BatchConfig:
    models = (
        MODEL_CHOICES if args.models.strip().lower() == "all" else _csv(args.models)
    )
    unknown = sorted(set(models) - set(MODEL_CHOICES))
    if unknown:
        raise ValueError(f"unknown models: {unknown}; choices={MODEL_CHOICES}")
    market_ids = tuple(int(item) for item in _csv(args.market_ids))
    exclude_market_ids = _excluded_market_ids(
        getattr(args, "exclude_market_ids", ""),
        getattr(args, "exclude_market_ids_file", None),
    )
    order_size = Decimal(args.order_size)
    order_sizes = tuple(
        Decimal(item) for item in _csv(getattr(args, "order_sizes", ""))
    ) or (order_size,)
    raw_order_sides = tuple(
        item.upper() for item in _csv(getattr(args, "order_sides", "BUY"))
    )
    invalid_sides = sorted(set(raw_order_sides) - {"BUY", "SELL"})
    if invalid_sides:
        raise ValueError(f"unsupported --order-sides values: {invalid_sides}")
    order_tif = str(getattr(args, "order_tif", "FAK")).upper()
    if order_tif not in {"FAK", "FOK"}:
        raise ValueError("--order-tif must be FAK or FOK")
    if args.models.strip().lower() == "all":
        incompatible = (
            {"pml2_fok", "nautilus_fok"}
            if order_tif == "FAK"
            else {"pml2_fak", "nautilus_fak"}
        )
        models = tuple(model for model in MODEL_CHOICES if model not in incompatible)
    config = BatchConfig(
        start=_parse_ts(args.start),
        end=_parse_ts(args.end),
        market_limit=int(args.market_limit),
        orders_per_market=int(args.orders_per_market),
        models=tuple(models),
        order_size=order_size,
        order_sizes=order_sizes,
        order_sides=cast(tuple[Literal["BUY", "SELL"], ...], raw_order_sides),
        order_tif=cast(Literal["FAK", "FOK"], order_tif),
        latency=timedelta(milliseconds=int(args.latency_ms)),
        horizon=timedelta(seconds=int(args.horizon_seconds)),
        lookback=timedelta(minutes=int(args.lookback_minutes)),
        limit_buffer=Decimal(args.limit_buffer),
        tick_size=Decimal(args.tick_size),
        lookback_blocks=int(args.lookback_blocks),
        horizon_blocks=int(args.horizon_blocks),
        candidate_multiplier=int(args.candidate_multiplier),
        market_ids=market_ids,
        exclude_market_ids=exclude_market_ids,
        categories=tuple(item.lower() for item in _csv(args.categories)),
        archive=args.archive,
        archive_baseline_lookback=timedelta(
            hours=int(getattr(args, "archive_baseline_lookback_hours", 2))
        ),
        archive_shard_count=int(getattr(args, "archive_shard_count", 48)),
        l2_source=str(getattr(args, "l2_source", "polymarket_market_ws_archive")),
        l2_slice_cache=(
            None
            if getattr(args, "no_l2_slice_cache", False)
            else getattr(args, "l2_slice_cache", DEFAULT_L2_SLICE_CACHE)
        ),
        prepared_input_cache=(
            None
            if getattr(args, "no_prepared_input_cache", False)
            else getattr(
                args,
                "prepared_input_cache",
                DEFAULT_PREPARED_INPUT_CACHE,
            )
        ),
        output_dir=args.output_dir,
        nautilus_python=args.nautilus_python,
        cohort_registry=(
            None
            if getattr(args, "no_cohort_registry", False)
            else getattr(args, "cohort_registry", DEFAULT_COHORT_REGISTRY)
        ),
        cohort_split=str(getattr(args, "cohort_split", "validation")),
        allow_cohort_reuse=bool(getattr(args, "allow_cohort_reuse", False)),
        v3_probability_artifact=getattr(args, "v3_probability_artifact", None),
        v3_fak_probability_artifact=getattr(args, "v3_fak_probability_artifact", None),
        v3_fok_probability_artifact=getattr(args, "v3_fok_probability_artifact", None),
        pml2_backend=cast(
            Literal["auto", "python", "rust"],
            str(getattr(args, "pml2_backend", "auto")),
        ),
    )
    if config.end <= config.start:
        raise ValueError("--end must be after --start")
    if config.market_limit < 0:
        raise ValueError("--market-limit must be >= 0; 0 means all eligible markets")
    if config.orders_per_market <= 0:
        raise ValueError("--orders-per-market must be positive")
    if any(size <= 0 for size in config.order_sizes) or config.tick_size <= 0:
        raise ValueError("--order-size and --tick-size must be positive")
    if config.latency < timedelta(0) or config.horizon <= timedelta(0):
        raise ValueError("latency must be nonnegative and horizon must be positive")
    if config.candidate_multiplier <= 0:
        raise ValueError("--candidate-multiplier must be positive")
    if config.archive_baseline_lookback < timedelta(0):
        raise ValueError("--archive-baseline-lookback-hours must be nonnegative")
    if config.archive_shard_count <= 0:
        raise ValueError("--archive-shard-count must be positive")
    if not config.l2_source.strip():
        raise ValueError("--l2-source cannot be empty")
    if config.pml2_backend not in {"auto", "python", "rust"}:
        raise ValueError("--pml2-backend must be auto, python, or rust")
    return config


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2026-07-08T09:30:00Z")
    parser.add_argument("--end", default="2026-07-08T11:00:00Z")
    parser.add_argument(
        "--market-limit",
        type=int,
        default=30,
        help="auto-discovery market count; 0 means all; --market-ids takes precedence",
    )
    parser.add_argument("--orders-per-market", type=int, default=20)
    parser.add_argument("--market-ids", default="")
    parser.add_argument("--exclude-market-ids", default="")
    parser.add_argument("--exclude-market-ids-file", type=Path)
    parser.add_argument("--categories", default="")
    parser.add_argument(
        "--models",
        default="all",
        help=f"comma-separated subset or 'all'; choices: {', '.join(MODEL_CHOICES)}",
    )
    parser.add_argument("--order-size", default="10")
    parser.add_argument(
        "--order-sizes",
        default="",
        help="comma-separated share sizes; overrides --order-size when provided",
    )
    parser.add_argument(
        "--order-sides",
        default="BUY",
        help="comma-separated BUY/SELL rotation",
    )
    parser.add_argument(
        "--order-tif",
        choices=("FAK", "FOK"),
        default="FAK",
    )
    parser.add_argument("--latency-ms", type=int, default=1_000)
    parser.add_argument("--horizon-seconds", type=int, default=30)
    parser.add_argument("--lookback-minutes", type=int, default=15)
    parser.add_argument("--limit-buffer", default="0.01")
    parser.add_argument("--tick-size", default="0.001")
    parser.add_argument("--lookback-blocks", type=int, default=600)
    parser.add_argument("--horizon-blocks", type=int, default=150)
    parser.add_argument("--candidate-multiplier", type=int, default=8)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--archive-baseline-lookback-hours", type=int, default=2)
    parser.add_argument("--archive-shard-count", type=int, default=48)
    parser.add_argument("--l2-source", default="polymarket_market_ws_archive")
    parser.add_argument("--l2-slice-cache", type=Path, default=DEFAULT_L2_SLICE_CACHE)
    parser.add_argument("--no-l2-slice-cache", action="store_true")
    parser.add_argument(
        "--prepared-input-cache",
        type=Path,
        default=DEFAULT_PREPARED_INPUT_CACHE,
    )
    parser.add_argument("--no-prepared-input-cache", action="store_true")
    parser.add_argument(
        "--pml2-backend",
        choices=("auto", "python", "rust"),
        default="auto",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--nautilus-python", type=Path, default=DEFAULT_NAUTILUS_PYTHON)
    parser.add_argument("--cohort-registry", type=Path, default=DEFAULT_COHORT_REGISTRY)
    parser.add_argument(
        "--cohort-split",
        choices=("calibration", "validation", "test", "sensitivity"),
        default="validation",
    )
    parser.add_argument("--allow-cohort-reuse", action="store_true")
    parser.add_argument("--no-cohort-registry", action="store_true")
    parser.add_argument(
        "--v3-probability-artifact",
        type=Path,
        help="optional candidate artifact for V3 L2-reference profiles",
    )
    parser.add_argument("--v3-fak-probability-artifact", type=Path)
    parser.add_argument("--v3-fok-probability-artifact", type=Path)
    args = parser.parse_args()
    summary = run(_config_from_args(args))
    print(json.dumps(_json_ready(summary["cohort"]), ensure_ascii=False, indent=2))
    print(json.dumps(_json_ready(summary["models"]), ensure_ascii=False, indent=2))
    print(json.dumps(_json_ready(summary["performance"]), ensure_ascii=False, indent=2))
    print(f"output_dir={args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
