#!/usr/bin/env python3
"""Materialize the frozen PMQ-073 timestamp-native cohort for unified replay."""

from __future__ import annotations

import argparse
import gc
import gzip
import hashlib
import json
import os
import sqlite3
import sys
from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
ALPHA_ROOT = ROOT.parents[1] / "alphaPredictionMarket"
ALPHA_SRC = ALPHA_ROOT / "src"
for candidate in (str(ROOT), str(ALPHA_ROOT), str(ALPHA_SRC)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

from pm_alpha.timestamp_native_fill_only import (  # type: ignore[import-not-found]  # noqa: E402
    DockerClickHouseExternalTableTransport,
    TimestampNativeBulkTradeLoader,
    TimestampNativeCoveragePinV1,
    TimestampNativeDayPlanV1,
)
from scripts.run_pmq073_real_trade_tape_backtest import (  # type: ignore[import-not-found]  # noqa: E402
    _database_environment,
)

from quant.backtest.fill_only_v2_service import load_trade_tape_coverage  # noqa: E402
from quant.backtest.frozen_order_catalog import (  # noqa: E402
    FrozenOrderCatalog,
    FrozenOrderCatalogBuilder,
)
from quant.backtest.orderfilled_v2_replay import (  # noqa: E402
    OrderSide,
    TimeInForce,
    V2TakerOrder,
)
from quant.backtest.trade_catalog import (  # noqa: E402
    ParquetTradeCatalog,
    ParquetTradeCatalogBuilder,
)

DEFAULT_SOURCE_ROOT = (
    ALPHA_ROOT
    / "strategies/pmq-guide-073/backtests/"
    "full_period_all_traded_value_only_v2_source_pinned_v3"
)
DEFAULT_PLAN_ROOT = DEFAULT_SOURCE_ROOT / "timestamp_native_fill_only_v5"
DEFAULT_OUTPUT_ROOT = ROOT / "runtime_outputs/unified_fill_only_acceleration/pmq073_173d"
DEFAULT_ENV_FILE = ROOT / ".env"


class _PinnedCoverageReader:
    def __init__(self, pin: TimestampNativeCoveragePinV1) -> None:
        self.pin = pin

    def read_coverage(self) -> TimestampNativeCoveragePinV1:
        return self.pin


def _canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _iter_plans(path: Path, *, max_days: int | None) -> Iterator[TimestampNativeDayPlanV1]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if max_days is not None and index >= max_days:
                break
            yield TimestampNativeDayPlanV1.model_validate_json(line)


def _load_contracts(
    source_root: Path,
    plan_root: Path,
    *,
    max_days: int | None,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    Path,
    Path,
    list[str],
    list[dict[str, Any]],
]:
    execution_manifest_path = plan_root / "execution_manifest.json"
    cache_path = plan_root / "execution_plan_cache.json"
    manifest = json.loads(execution_manifest_path.read_text(encoding="utf-8"))
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    plans_path = plan_root / str(cache["plan_file"])
    frozen_path = source_root / "frozen_orders.jsonl"
    if _file_sha256(plans_path) != cache["plan_file_sha256"]:
        raise ValueError("execution plan file checksum mismatch")
    if _file_sha256(frozen_path) != manifest["frozen_orders_file_sha256"]:
        raise ValueError("frozen order file checksum mismatch")
    order_ids: list[str] = []
    day_rows: list[dict[str, Any]] = []
    for plan in _iter_plans(plans_path, max_days=max_days):
        order_ids.extend(plan.order_ids)
        day_rows.append(
            {
                "signal_day": plan.signal_day.isoformat(),
                "orders": len(plan.order_ids),
                "intervals": len(plan.intervals),
                "cumulative_orders": len(order_ids),
            }
        )
    if not order_ids:
        raise ValueError("selected execution plans contain no orders")
    if len(order_ids) != len(set(order_ids)):
        raise ValueError("selected execution plans repeat order IDs")
    if max_days is None and len(order_ids) != int(cache["selected_order_count"]):
        raise ValueError("full execution-plan order count mismatch")
    return manifest, cache, plans_path, frozen_path, order_ids, day_rows


def _current_pin(frozen: TimestampNativeCoveragePinV1) -> TimestampNativeCoveragePinV1:
    coverage = load_trade_tape_coverage()
    if not coverage.contains(frozen.source_min_block, frozen.source_max_block):
        raise ValueError("current build receipts do not cover the frozen source block range")
    coverage_payload = coverage.as_dict()
    coverage_hash = _canonical_sha256(coverage_payload)
    return TimestampNativeCoveragePinV1(
        source_min_block=frozen.source_min_block,
        source_max_block=frozen.source_max_block,
        min_trade_ts=frozen.min_trade_ts,
        max_trade_ts=frozen.max_trade_ts,
        source_coverage_sha256=coverage_hash,
        build_identity_sha256=_canonical_sha256(
            {
                "source_pin": frozen.model_dump(mode="json"),
                "current_receipt_coverage": coverage_payload,
            }
        ),
    )


def _to_order(row: dict[str, Any]) -> V2TakerOrder:
    intent = row["intent"]
    side = str(intent["side"]).upper()
    tif = str(intent["tif"]).upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"invalid frozen order side: {side}")
    if tif not in {"GTC", "GTD", "IOC", "FOK", "FAK"}:
        raise ValueError(f"invalid frozen order tif: {tif}")
    signal_ts = datetime.fromisoformat(str(intent["signal_ts"]).replace("Z", "+00:00"))
    if signal_ts.tzinfo is None or signal_ts.utcoffset() is None:
        signal_ts = signal_ts.replace(tzinfo=timezone.utc)
    return V2TakerOrder(
        order_id=str(intent["client_order_id"]),
        market_id=int(intent["market_id"]),
        asset_id=str(intent["asset_id"]).lower(),
        side=cast(OrderSide, side),
        limit_price=Decimal(str(intent["limit_price"])),
        size=Decimal(str(intent["size"])),
        signal_block=None,
        signal_ts=signal_ts.astimezone(timezone.utc),
        tif=cast(TimeInForce, tif),
        allow_partial_fill=tif != "FOK",
        exclude_signal_source_trade=True,
    )


def _materialize_orders(
    target: Path,
    frozen_path: Path,
    order_ids: list[str],
    *,
    source_pin: str,
    strategy_hash: str,
    resume: bool,
    batch_size: int,
) -> dict[str, Any]:
    if target.exists():
        catalog = FrozenOrderCatalog(
            target,
            source_pin=source_pin,
            strategy_hash=strategy_hash,
        )
        if catalog.rows != len(order_ids):
            raise ValueError("existing frozen order catalog row count mismatch")
        return catalog.manifest
    lookup_path = target.parent / f".{target.name}-lookup.sqlite3"
    lookup_path.unlink(missing_ok=True)
    connection = sqlite3.connect(lookup_path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute(
        "CREATE TABLE desired (sequence INTEGER PRIMARY KEY, order_id TEXT UNIQUE NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE payloads (order_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)"
    )
    connection.executemany(
        "INSERT INTO desired(sequence, order_id) VALUES (?, ?)", enumerate(order_ids)
    )
    selected = set(order_ids)
    pending: list[tuple[str, str]] = []
    with frozen_path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            order_id = str(row["intent"]["client_order_id"])
            if order_id not in selected:
                continue
            pending.append((order_id, line.strip()))
            if len(pending) >= batch_size:
                connection.executemany(
                    "INSERT INTO payloads(order_id, payload_json) VALUES (?, ?)", pending
                )
                pending = []
    if pending:
        connection.executemany(
            "INSERT INTO payloads(order_id, payload_json) VALUES (?, ?)", pending
        )
    connection.commit()
    loaded_count = int(connection.execute("SELECT count(*) FROM payloads").fetchone()[0])
    if loaded_count != len(order_ids):
        connection.close()
        raise ValueError("not every planned order exists in the frozen inventory")
    builder = FrozenOrderCatalogBuilder(
        target,
        source_pin=source_pin,
        strategy_hash=strategy_hash,
        row_group_size=batch_size,
        resume=resume,
    )
    batch: list[V2TakerOrder] = []
    cursor = connection.execute(
        """
        SELECT d.sequence, p.payload_json
        FROM desired AS d
        INNER JOIN payloads AS p USING (order_id)
        WHERE d.sequence >= ?
        ORDER BY d.sequence ASC
        """,
        (builder.rows,),
    )
    for _, payload_json in cursor:
        batch.append(_to_order(json.loads(payload_json)))
        if len(batch) >= batch_size:
            builder.append(batch)
            batch = []
    if batch:
        builder.append(batch)
    connection.close()
    if builder.rows != len(order_ids):
        raise ValueError("resumed frozen order catalog row count mismatch")
    result = builder.finalize()
    lookup_path.unlink(missing_ok=True)
    lookup_path.with_suffix(".sqlite3-wal").unlink(missing_ok=True)
    lookup_path.with_suffix(".sqlite3-shm").unlink(missing_ok=True)
    return result


def _materialize_trades(
    target: Path,
    plans_path: Path,
    *,
    max_days: int | None,
    pin: TimestampNativeCoveragePinV1,
    source_pin: str,
    profile_hash: str,
    strategy_hash: str,
    resume: bool,
    env_file: Path,
    loader_config: dict[str, Any],
    receipts_path: Path,
    load_workers: int,
) -> dict[str, Any]:
    if target.exists():
        return ParquetTradeCatalog(
            target,
            source_pin=source_pin,
            profile_hash=profile_hash,
            strategy_hash=strategy_hash,
        ).manifest.as_dict()
    builder = ParquetTradeCatalogBuilder(
        target,
        source_pin=source_pin,
        profile_hash=profile_hash,
        strategy_hash=strategy_hash,
        row_group_size=50_000,
        resume=resume,
    )
    completed = int((builder.high_watermark or {}).get("plan_index", -1)) + 1
    receipts_path.parent.mkdir(parents=True, exist_ok=True)
    if receipts_path.exists():
        lines = receipts_path.read_text(encoding="utf-8").splitlines()
        receipts_path.write_text(
            "\n".join(lines[:completed]) + ("\n" if completed else ""),
            encoding="utf-8",
        )
    with _database_environment(env_file):
        loader = TimestampNativeBulkTradeLoader(
            transport=DockerClickHouseExternalTableTransport(),
            coverage_reader=_PinnedCoverageReader(pin),
            source_pin=pin,
            max_rows_per_day=int(loader_config["max_rows_per_day"]),
            max_intervals_per_day=int(loader_config["max_intervals_per_day"]),
            timeout_seconds=float(loader_config["timeout_seconds"]),
            max_threads=int(loader_config["max_threads"]),
        )
        remaining = (
            (index, plan)
            for index, plan in enumerate(_iter_plans(plans_path, max_days=max_days))
            if index >= completed
        )
        pending: deque[
            tuple[int, TimestampNativeDayPlanV1, Future[Any]]
        ] = deque()
        with ThreadPoolExecutor(max_workers=max(1, load_workers)) as pool:
            for _ in range(max(1, load_workers)):
                try:
                    plan_index, plan = next(remaining)
                except StopIteration:
                    break
                pending.append((plan_index, plan, pool.submit(loader.load_day, plan)))
            while pending:
                plan_index, plan, future = pending.popleft()
                loaded = future.result()
                watermark = {
                    "plan_index": plan_index,
                    "signal_day": plan.signal_day.isoformat(),
                    "receipt": loaded.receipt.model_dump(mode="json"),
                }
                builder.append(loaded.trades, high_watermark=watermark)
                with receipts_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(watermark, sort_keys=True) + "\n")
                print(
                    json.dumps(
                        {
                            "event": "day_materialized",
                            "plan_index": plan_index,
                            "signal_day": plan.signal_day.isoformat(),
                            "orders": len(plan.order_ids),
                            "intervals": len(plan.intervals),
                            "trades": len(loaded.trades),
                            "catalog_rows": builder.rows,
                            "load_workers": max(1, load_workers),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                del loaded
                gc.collect()
                pa.default_memory_pool().release_unused()
                try:
                    next_index, next_plan = next(remaining)
                except StopIteration:
                    continue
                pending.append(
                    (next_index, next_plan, pool.submit(loader.load_day, next_plan))
                )
    return builder.finalize().as_dict()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--plan-root", type=Path, default=DEFAULT_PLAN_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--max-days", type=int)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--load-workers", type=int, default=2)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--orders-only", action="store_true")
    parser.add_argument("--trades-only", action="store_true")
    args = parser.parse_args()
    if args.orders_only and args.trades_only:
        parser.error("--orders-only and --trades-only are mutually exclusive")
    if args.max_days is not None and args.max_days <= 0:
        parser.error("--max-days must be positive")
    if not 1 <= args.load_workers <= 4:
        parser.error("--load-workers must be in [1, 4]")

    source_root = args.source_root.resolve(strict=True)
    plan_root = args.plan_root.resolve(strict=True)
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    manifest, cache, plans_path, frozen_path, order_ids, days = _load_contracts(
        source_root,
        plan_root,
        max_days=args.max_days,
    )
    frozen_pin = TimestampNativeCoveragePinV1.model_validate_json(
        json.dumps(manifest["source_pin"], sort_keys=True)
    )
    with _database_environment(args.env_file):
        pin = _current_pin(frozen_pin)
    source_pin = _canonical_sha256(pin.model_dump(mode="json"))
    strategy_hash = _canonical_sha256(
        {
            "candidate_code_sha256": manifest["candidate_code_sha256"],
            "frozen_order_manifest_sha256": manifest["frozen_order_manifest_sha256"],
            "chronological_order_inventory_sha256": manifest[
                "chronological_order_inventory_sha256"
            ],
            "plan_inventory_sha256": cache["plan_inventory_sha256"],
            "selected_days": len(days),
        }
    )
    profile_hash = _canonical_sha256(manifest["frozen_profile_contracts"])

    order_manifest: dict[str, Any] | None = None
    trade_manifest: dict[str, Any] | None = None
    if not args.trades_only:
        order_manifest = _materialize_orders(
            output_root / "orders",
            frozen_path,
            order_ids,
            source_pin=source_pin,
            strategy_hash=strategy_hash,
            resume=args.resume,
            batch_size=max(1, args.batch_size),
        )
    if not args.orders_only:
        trade_manifest = _materialize_trades(
            output_root / "trades",
            plans_path,
            max_days=args.max_days,
            pin=pin,
            source_pin=source_pin,
            profile_hash=profile_hash,
            strategy_hash=strategy_hash,
            resume=args.resume,
            env_file=args.env_file,
            loader_config=manifest["loader"],
            receipts_path=output_root / "trade_load_receipts.jsonl",
            load_workers=args.load_workers,
        )
    receipt = {
        "schema_version": "PMQ073UnifiedCatalogMaterializationReceiptV1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_root": str(source_root),
        "plan_root": str(plan_root),
        "source_pin": pin.model_dump(mode="json"),
        "source_pin_sha256": source_pin,
        "strategy_hash": strategy_hash,
        "profile_hash": profile_hash,
        "selected_days": len(days),
        "selected_orders": len(order_ids),
        "days": days,
        "order_manifest": order_manifest,
        "trade_manifest": trade_manifest,
        "lob_used": False,
    }
    receipt["receipt_sha256"] = _canonical_sha256(receipt)
    _atomic_json(output_root / "materialization_receipt.json", receipt)
    print(
        json.dumps(
            {
                "event": "materialization_complete",
                "receipt": str(output_root / "materialization_receipt.json"),
                "receipt_sha256": receipt["receipt_sha256"],
                "selected_days": len(days),
                "selected_orders": len(order_ids),
                "order_rows": (
                    int(order_manifest["rows"]) if order_manifest is not None else None
                ),
                "trade_rows": (
                    int(trade_manifest["rows"]) if trade_manifest is not None else None
                ),
                "trade_files": (
                    len(trade_manifest["files"])
                    if trade_manifest is not None
                    else None
                ),
                "lob_used": False,
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
