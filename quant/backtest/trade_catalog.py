"""Versioned Parquet catalog for bounded OrderFilled trade-tape slices."""

from __future__ import annotations

import hashlib
import heapq
import json
import os
import shutil
import uuid
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Self, cast
from urllib.parse import quote

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from quant.backtest.columnar_trade_tape import (
    ColumnarTradeStore,
    prepare_columnar_trade_tape,
)
from quant.backtest.orderfilled_v2_replay import (
    RequiredTradeWindow,
    V2TakerOrder,
    V2TradePrint,
    build_required_trade_windows,
    load_v2_trade_slices_for_windows,
    merge_required_trade_windows,
    trade_print_from_row,
)
from quant.backtest.prepared_trade_tape import (
    PreparedTradeTape,
    prepare_trade_tape,
    slice_trade_group,
)
from quant.backtest.trade_only_v3.models import TradeOnlyOrder
from quant.core.db import ClickHouseClient

CATALOG_SCHEMA_VERSION = "UnifiedFillOnlyTradeCatalogV3"
LEGACY_CATALOG_SCHEMA_VERSIONS = {
    "UnifiedFillOnlyTradeCatalogV1",
    "UnifiedFillOnlyTradeCatalogV2",
}
CATALOG_CHECKPOINT_VERSION = "UnifiedFillOnlyTradeCatalogCheckpointV1"
TradeCatalogPartitionMode = Literal["date", "market_asset_date"]
TRADE_SCHEMA = pa.schema(
    [
        pa.field("trade_id", pa.string(), nullable=False),
        pa.field("trade_group_id", pa.string()),
        pa.field("market_id", pa.uint64(), nullable=False),
        pa.field("condition_id", pa.string(), nullable=False),
        pa.field("asset_id", pa.string(), nullable=False),
        pa.field("outcome", pa.string(), nullable=False),
        pa.field("block_number", pa.uint64(), nullable=False),
        pa.field("block_time", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("tx_hash", pa.string(), nullable=False),
        pa.field("tx_index", pa.uint32(), nullable=False),
        pa.field("tx_index_source", pa.string(), nullable=False),
        pa.field("price", pa.decimal128(20, 10), nullable=False),
        pa.field("size_shares", pa.decimal256(38, 10), nullable=False),
        pa.field("notional_usdc", pa.decimal256(38, 10), nullable=False),
        pa.field("aggressor_side", pa.string(), nullable=False),
        pa.field("passive_side", pa.string(), nullable=False),
        pa.field("source_log_indexes", pa.list_(pa.uint32()), nullable=False),
        pa.field("source_fill_count", pa.uint32(), nullable=False),
    ]
)


@dataclass(frozen=True)
class TradeCatalogFile:
    path: str
    rows: int
    sha256: str
    market_id: int
    asset_id: str
    trade_date: str
    min_block: int
    max_block: int
    min_ts: str
    max_ts: str


@dataclass(frozen=True)
class TradeCatalogManifest:
    schema_version: str
    source_table: str
    source_pin: str
    profile_hash: str
    strategy_hash: str
    arrow_schema_sha256: str
    row_group_size: int
    partition_mode: TradeCatalogPartitionMode
    rows: int
    files: tuple[TradeCatalogFile, ...]
    manifest_sha256: str

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["files"] = [asdict(item) for item in self.files]
        return row


class ParquetTradeCatalogBuilder:
    """Append bounded batches to a staging tree, then atomically publish it."""

    def __init__(
        self,
        root: str | Path,
        *,
        source_pin: str,
        profile_hash: str,
        strategy_hash: str,
        source_table: str = "trade_prints_one_sided",
        row_group_size: int = 50_000,
        partition_mode: TradeCatalogPartitionMode = "market_asset_date",
        resume: bool = False,
    ) -> None:
        for name, value in (
            ("source_pin", source_pin),
            ("profile_hash", profile_hash),
            ("strategy_hash", strategy_hash),
        ):
            if not str(value).strip():
                raise ValueError(f"{name} is required")
        self.root = Path(root).resolve()
        if self.root.exists():
            raise FileExistsError(f"catalog target already exists: {self.root}")
        self.source_pin = str(source_pin)
        self.profile_hash = str(profile_hash)
        self.strategy_hash = str(strategy_hash)
        self.source_table = str(source_table)
        self.row_group_size = max(1, int(row_group_size))
        self.partition_mode = _partition_mode(partition_mode)
        self.resume = bool(resume)
        self.staging = (
            self.root.with_name(f".{self.root.name}.building")
            if self.resume
            else self.root.with_name(f".{self.root.name}.tmp-{uuid.uuid4().hex}")
        )
        self.checkpoint_path = self.staging / "builder_checkpoint.json"
        self._files: list[TradeCatalogFile] = []
        self._part = 0
        self._rows = 0
        self._closed = False
        self._high_watermark: Any = None
        if self.staging.exists():
            if not self.resume:
                raise FileExistsError(f"catalog staging already exists: {self.staging}")
            self._restore_checkpoint()
        else:
            self.staging.mkdir(parents=True)
            self._write_checkpoint()

    @property
    def rows(self) -> int:
        return self._rows

    @property
    def high_watermark(self) -> Any:
        return self._high_watermark

    def append(self, trades: Iterable[V2TradePrint], *, high_watermark: Any = None) -> int:
        if self._closed:
            raise RuntimeError("catalog builder is closed")
        grouped: dict[tuple[str, int, str], list[V2TradePrint]] = defaultdict(list)
        for trade in trades:
            trade_date = _utc(trade.block_time).date().isoformat()
            key = (
                (trade_date, 0, "*")
                if self.partition_mode == "date"
                else (trade_date, int(trade.market_id), trade.asset_id.lower())
            )
            grouped[key].append(trade)
        appended = 0
        for (trade_date, market_id, asset_id), rows in sorted(grouped.items()):
            ordered = tuple(sorted(rows, key=lambda item: item.sequence))
            if not ordered:
                continue
            directory = self.staging / f"date={trade_date}"
            if self.partition_mode == "market_asset_date":
                directory = (
                    directory
                    / f"market_id={market_id}"
                    / f"asset_id={quote(asset_id, safe='')}"
                )
            directory.mkdir(parents=True, exist_ok=True)
            relative = (
                directory.relative_to(self.staging) / f"part-{self._part:08d}.parquet"
            )
            target = self.staging / relative
            temporary = target.with_suffix(".parquet.tmp")
            table = _trades_to_table(ordered)
            pq.write_table(
                table,
                temporary,
                compression="zstd",
                use_dictionary=["outcome", "aggressor_side", "passive_side"],
                row_group_size=self.row_group_size,
                write_statistics=True,
            )
            os.replace(temporary, target)
            receipt = TradeCatalogFile(
                path=relative.as_posix(),
                rows=len(ordered),
                sha256=_file_sha256(target),
                market_id=market_id,
                asset_id=asset_id,
                trade_date=trade_date,
                min_block=min(row.block_number for row in ordered),
                max_block=max(row.block_number for row in ordered),
                min_ts=_utc(min(row.block_time for row in ordered)).isoformat(),
                max_ts=_utc(max(row.block_time for row in ordered)).isoformat(),
            )
            self._files.append(receipt)
            self._part += 1
            self._rows += len(ordered)
            appended += len(ordered)
        if high_watermark is not None:
            self._high_watermark = high_watermark
        self._write_checkpoint()
        return appended

    def finalize(self) -> TradeCatalogManifest:
        if self._closed:
            raise RuntimeError("catalog builder is already closed")
        draft = {
            "schema_version": CATALOG_SCHEMA_VERSION,
            "source_table": self.source_table,
            "source_pin": self.source_pin,
            "profile_hash": self.profile_hash,
            "strategy_hash": self.strategy_hash,
            "arrow_schema_sha256": _schema_sha256(TRADE_SCHEMA),
            "row_group_size": self.row_group_size,
            "partition_mode": self.partition_mode,
            "rows": self._rows,
            "files": [asdict(item) for item in self._files],
        }
        digest = _canonical_sha256(draft)
        payload = {**draft, "manifest_sha256": digest}
        temporary = self.staging / "manifest.json.tmp"
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, self.staging / "manifest.json")
        self.checkpoint_path.unlink(missing_ok=True)
        self.root.parent.mkdir(parents=True, exist_ok=True)
        os.rename(self.staging, self.root)
        self._closed = True
        return _manifest_from_payload(payload)

    def abort(self, *, preserve_resume_state: bool = True) -> None:
        if self.staging.exists() and not (self.resume and preserve_resume_state):
            shutil.rmtree(self.staging)
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del traceback
        if exc_type is not None:
            self.abort(preserve_resume_state=True)

    def _write_checkpoint(self) -> None:
        draft = {
            "schema_version": CATALOG_CHECKPOINT_VERSION,
            "catalog_schema_version": CATALOG_SCHEMA_VERSION,
            "source_table": self.source_table,
            "source_pin": self.source_pin,
            "profile_hash": self.profile_hash,
            "strategy_hash": self.strategy_hash,
            "arrow_schema_sha256": _schema_sha256(TRADE_SCHEMA),
            "row_group_size": self.row_group_size,
            "partition_mode": self.partition_mode,
            "rows": self._rows,
            "next_part": self._part,
            "files": [asdict(item) for item in self._files],
            "high_watermark": self._high_watermark,
        }
        _atomic_json(
            self.checkpoint_path,
            {**draft, "checkpoint_sha256": _canonical_sha256(draft)},
        )

    def _restore_checkpoint(self) -> None:
        if not self.checkpoint_path.exists():
            raise ValueError("resumable trade catalog staging has no checkpoint")
        payload = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        digest = str(payload.pop("checkpoint_sha256", ""))
        if digest != _canonical_sha256(payload):
            raise ValueError("trade catalog builder checkpoint checksum mismatch")
        for key, expected in (
            ("schema_version", CATALOG_CHECKPOINT_VERSION),
            ("catalog_schema_version", CATALOG_SCHEMA_VERSION),
            ("source_table", self.source_table),
            ("source_pin", self.source_pin),
            ("profile_hash", self.profile_hash),
            ("strategy_hash", self.strategy_hash),
            ("arrow_schema_sha256", _schema_sha256(TRADE_SCHEMA)),
            ("row_group_size", self.row_group_size),
            ("partition_mode", self.partition_mode),
        ):
            if payload.get(key) != expected:
                raise ValueError(f"trade catalog builder checkpoint {key} mismatch")
        self._files = [TradeCatalogFile(**item) for item in payload["files"]]
        self._rows = int(payload["rows"])
        self._part = int(payload["next_part"])
        self._high_watermark = payload.get("high_watermark")
        referenced = {item.path for item in self._files}
        for item in self._files:
            path = self.staging / item.path
            if _file_sha256(path) != item.sha256:
                raise ValueError(f"trade catalog checkpoint file mismatch: {item.path}")
        for path in self.staging.rglob("part-*.parquet"):
            if path.relative_to(self.staging).as_posix() not in referenced:
                path.unlink()


class ParquetTradeCatalog:
    """Memory-mapped, column-pruned access to one immutable catalog version."""

    def __init__(
        self,
        root: str | Path,
        *,
        source_pin: str | None = None,
        profile_hash: str | None = None,
        strategy_hash: str | None = None,
        verify_checksums: bool = True,
    ) -> None:
        self.root = Path(root).resolve()
        payload = json.loads((self.root / "manifest.json").read_text(encoding="utf-8"))
        self.manifest = _manifest_from_payload(payload)
        _verify_manifest_hash(payload)
        for name, expected, actual in (
            ("source_pin", source_pin, self.manifest.source_pin),
            ("profile_hash", profile_hash, self.manifest.profile_hash),
            ("strategy_hash", strategy_hash, self.manifest.strategy_hash),
        ):
            if expected is not None and str(expected) != actual:
                raise ValueError(f"catalog {name} mismatch")
        if self.manifest.arrow_schema_sha256 != _schema_sha256(TRADE_SCHEMA):
            raise ValueError("catalog Arrow schema hash mismatch")
        self._paths = tuple(self.root / item.path for item in self.manifest.files)
        self._files_by_pair: dict[tuple[int, str], tuple[TradeCatalogFile, ...]] = {}
        self._shared_files: tuple[TradeCatalogFile, ...] = ()
        grouped_files: dict[tuple[int, str], list[TradeCatalogFile]] = defaultdict(list)
        shared_files: list[TradeCatalogFile] = []
        for item in self.manifest.files:
            if item.asset_id == "*":
                shared_files.append(item)
            else:
                grouped_files[(item.market_id, item.asset_id.lower())].append(item)
        self._files_by_pair = {
            key: tuple(sorted(rows, key=lambda item: (item.min_block, item.min_ts, item.path)))
            for key, rows in grouped_files.items()
        }
        self._shared_files = tuple(
            sorted(shared_files, key=lambda item: (item.min_block, item.min_ts, item.path))
        )
        self._file_time_bounds = {
            item.path: (_parse_ts(item.min_ts), _parse_ts(item.max_ts))
            for item in self.manifest.files
        }
        self._shared_files_by_time = tuple(
            sorted(
                self._shared_files,
                key=lambda item: (self._file_time_bounds[item.path][0], item.path),
            )
        )
        self._shared_min_times = tuple(
            self._file_time_bounds[item.path][0] for item in self._shared_files_by_time
        )
        self._shared_files_by_block = tuple(
            sorted(self._shared_files, key=lambda item: (item.min_block, item.path))
        )
        self._shared_min_blocks = tuple(
            item.min_block for item in self._shared_files_by_block
        )
        if verify_checksums:
            for item, path in zip(self.manifest.files, self._paths, strict=True):
                if _file_sha256(path) != item.sha256:
                    raise ValueError(f"catalog file checksum mismatch: {item.path}")
        self.clickhouse_query_count = 0
        self.parquet_file_read_count = 0

    @property
    def source_pin(self) -> str:
        return self.manifest.source_pin

    @property
    def row_count(self) -> int:
        return self.manifest.rows

    @property
    def min_block(self) -> int | None:
        return min((item.min_block for item in self.manifest.files), default=None)

    @property
    def max_block(self) -> int | None:
        return max((item.max_block for item in self.manifest.files), default=None)

    def load_window(
        self,
        *,
        market_id: int,
        asset_id: str,
        start_block: int | None = None,
        end_block: int | None = None,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> tuple[V2TradePrint, ...]:
        candidates = [
            self.root / item.path
            for item in self.manifest.files
            if (
                (item.market_id == int(market_id) and item.asset_id == asset_id.lower())
                or item.asset_id == "*"
            )
            and (start_block is None or item.max_block >= int(start_block))
            and (end_block is None or item.min_block <= int(end_block))
            and (start_ts is None or _parse_ts(item.max_ts) >= _utc(start_ts))
            and (end_ts is None or _parse_ts(item.min_ts) <= _utc(end_ts))
        ]
        if not candidates:
            return ()
        expression = (ds.field("market_id") == int(market_id)) & (
            ds.field("asset_id") == asset_id.lower()
        )
        if start_block is not None:
            expression &= ds.field("block_number") >= int(start_block)
        if end_block is not None:
            expression &= ds.field("block_number") <= int(end_block)
        if start_ts is not None:
            expression &= ds.field("block_time") >= _utc(start_ts)
        if end_ts is not None:
            expression &= ds.field("block_time") <= _utc(end_ts)
        table = ds.dataset(
            [str(path) for path in candidates], format="parquet"
        ).to_table(
            columns=TRADE_SCHEMA.names,
            filter=expression,
        )
        return tuple(sorted(_table_to_trades(table), key=lambda item: item.sequence))

    def prepare_for_orders(
        self, orders: Sequence[V2TakerOrder | TradeOnlyOrder]
    ) -> PreparedTradeTape[V2TradePrint]:
        windows = merge_required_trade_windows(
            required_windows_for_orders(
                orders,
                source_min_block=self.min_block,
                source_max_block=self.max_block,
            )
        )
        windows_by_pair: dict[tuple[int, str], list[RequiredTradeWindow]] = defaultdict(list)
        selected: dict[str, TradeCatalogFile] = {}
        for window in windows:
            key = (int(window.market_id), window.asset_id.lower())
            windows_by_pair[key].append(window)
            for item in self._files_by_pair.get(key, ()):
                if _file_overlaps_window(item, window):
                    selected.setdefault(item.path, item)
            for item in self._shared_candidates(window):
                if _file_overlaps_window(
                    item,
                    window,
                    time_bounds=self._file_time_bounds[item.path],
                ):
                    selected.setdefault(item.path, item)
        trades: dict[str, V2TradePrint] = {}
        selected_files = tuple(sorted(selected.values(), key=lambda row: row.path))
        self.parquet_file_read_count += len(selected_files)
        for row in self._iter_selected_files(
            selected_files,
            asset_ids={asset_id for _, asset_id in windows_by_pair},
            batch_size=65_536,
        ):
            key = (row.market_id, row.asset_id.lower())
            if any(
                _trade_in_window(row, window)
                for window in windows_by_pair.get(key, ())
            ):
                trades.setdefault(row.trade_id, row)
        return prepare_trade_tape(trades.values())

    def prepare_columnar_for_orders(
        self, orders: Sequence[V2TakerOrder | TradeOnlyOrder]
    ) -> PreparedTradeTape[V2TradePrint]:
        """Build the same bounded tape while retaining rows in Arrow buffers."""

        windows = merge_required_trade_windows(
            required_windows_for_orders(
                orders,
                source_min_block=self.min_block,
                source_max_block=self.max_block,
            )
        )
        windows_by_pair: dict[
            tuple[int, str], list[RequiredTradeWindow]
        ] = defaultdict(list)
        selected: dict[str, TradeCatalogFile] = {}
        for window in windows:
            key = (int(window.market_id), window.asset_id.lower())
            windows_by_pair[key].append(window)
            for item in self._files_by_pair.get(key, ()):
                if _file_overlaps_window(item, window):
                    selected.setdefault(item.path, item)
            for item in self._shared_candidates(window):
                if _file_overlaps_window(
                    item,
                    window,
                    time_bounds=self._file_time_bounds[item.path],
                ):
                    selected.setdefault(item.path, item)
        selected_files = tuple(sorted(selected.values(), key=lambda row: row.path))
        self.parquet_file_read_count += len(selected_files)
        if not selected_files or not windows_by_pair:
            return prepare_trade_tape(())
        dataset = ds.dataset(
            [str(self.root / item.path) for item in selected_files],
            format="parquet",
        )
        table = dataset.to_table(
            columns=TRADE_SCHEMA.names,
            filter=ds.field("asset_id").isin(
                sorted({asset_id for _, asset_id in windows_by_pair})
            ),
            use_threads=True,
        )
        return prepare_columnar_trade_tape(table, windows_by_pair)

    def prepared_view_for_orders(
        self,
        prepared: PreparedTradeTape[V2TradePrint],
        orders: Sequence[V2TakerOrder | TradeOnlyOrder],
    ) -> PreparedTradeTape[V2TradePrint]:
        """Build an exact profile window view without reading Parquet again."""

        windows = merge_required_trade_windows(
            required_windows_for_orders(
                orders,
                source_min_block=self.min_block,
                source_max_block=self.max_block,
            )
        )
        cache_key = (
            "catalog_window_view_v1",
            tuple(
                (
                    int(window.market_id),
                    window.asset_id.lower(),
                    window.start_block,
                    window.end_block,
                    window.start_ts.isoformat() if window.start_ts else None,
                    window.end_ts.isoformat() if window.end_ts else None,
                )
                for window in windows
            ),
        )
        cached = prepared.backend_payloads.get(cache_key)
        if isinstance(cached, PreparedTradeTape):
            return cached
        columnar_store = prepared.backend_payloads.get("fill_only_columnar_store_v1")
        if isinstance(columnar_store, ColumnarTradeStore):
            windows_by_pair: dict[
                tuple[int, str], list[RequiredTradeWindow]
            ] = defaultdict(list)
            for window in windows:
                windows_by_pair[
                    (int(window.market_id), window.asset_id.lower())
                ].append(window)
            view = prepare_columnar_trade_tape(
                columnar_store.table,
                windows_by_pair,
                ordered_unique=True,
            )
            prepared.backend_payloads[cache_key] = view
            return view
        selected: dict[str, V2TradePrint] = {}
        for window in windows:
            rows = slice_trade_group(
                prepared.group(window.market_id, window.asset_id),
                start_block=window.start_block,
                end_block=window.end_block,
                start_ts=window.start_ts,
                end_ts=window.end_ts,
            )
            for trade in rows:
                selected.setdefault(trade.trade_id, trade)
        view = prepare_trade_tape(selected.values())
        prepared.backend_payloads[cache_key] = view
        return view

    def _shared_candidates(
        self, window: RequiredTradeWindow
    ) -> tuple[TradeCatalogFile, ...]:
        if window.end_ts is not None:
            end = bisect_right(self._shared_min_times, _utc(window.end_ts))
            return self._shared_files_by_time[:end]
        if window.end_block is not None:
            end = bisect_right(self._shared_min_blocks, int(window.end_block))
            return self._shared_files_by_block[:end]
        return self._shared_files

    def iter_trades(self, *, batch_size: int = 65_536) -> Iterator[V2TradePrint]:
        streams = [self._iter_file(path, batch_size=batch_size) for path in self._paths]
        yield from heapq.merge(*streams, key=lambda item: item.sequence)

    def _iter_selected_files(
        self,
        files: Sequence[TradeCatalogFile],
        *,
        asset_ids: set[str],
        batch_size: int,
    ) -> Iterator[V2TradePrint]:
        if not files or not asset_ids:
            return
        dataset = ds.dataset(
            [str(self.root / item.path) for item in files],
            format="parquet",
        )
        expression = ds.field("asset_id").isin(sorted(asset_ids))
        for batch in dataset.to_batches(
            columns=TRADE_SCHEMA.names,
            filter=expression,
            batch_size=max(1, int(batch_size)),
            use_threads=True,
        ):
            yield from _table_to_trades(
                pa.Table.from_batches([batch], schema=TRADE_SCHEMA)
            )

    def iter_aligned_chunks(
        self, *, target_rows: int, batch_size: int = 65_536
    ) -> Iterator[tuple[V2TradePrint, ...]]:
        if target_rows <= 0:
            raise ValueError("target_rows must be positive")
        buffer: list[V2TradePrint] = []
        for trade in self.iter_trades(batch_size=batch_size):
            if (
                len(buffer) >= target_rows
                and buffer
                and not _same_atomic_boundary(buffer[-1], trade)
            ):
                yield tuple(buffer)
                buffer = []
            buffer.append(trade)
        if buffer:
            yield tuple(buffer)

    @staticmethod
    def _iter_file(path: Path, *, batch_size: int) -> Iterator[V2TradePrint]:
        parquet = pq.ParquetFile(path, memory_map=True)
        for batch in parquet.iter_batches(
            batch_size=max(1, int(batch_size)), columns=TRADE_SCHEMA.names
        ):
            yield from _table_to_trades(
                pa.Table.from_batches([batch], schema=TRADE_SCHEMA)
            )


def required_windows_for_orders(
    orders: Sequence[V2TakerOrder | TradeOnlyOrder],
    *,
    source_min_block: int | None = None,
    source_max_block: int | None = None,
) -> list[RequiredTradeWindow]:
    if not orders:
        return []
    v2_orders = [order for order in orders if isinstance(order, V2TakerOrder)]
    if len(v2_orders) == len(orders):
        return build_required_trade_windows(
            v2_orders,
            source_min_block=source_min_block,
            source_max_block=source_max_block,
        )
    v3_orders = [order for order in orders if isinstance(order, TradeOnlyOrder)]
    if len(v3_orders) != len(orders):
        raise TypeError("order catalog batch cannot mix V2 and V3 orders")
    return [
        RequiredTradeWindow(
            market_id=order.market_id,
            asset_id=order.asset_id,
            aggressor_side=None,
            start_block=(
                max(0, order.arrival_block - order.lookback_blocks)
                if order.arrival_block is not None
                else None
            ),
            end_block=order.deadline_block,
            start_ts=(
                order.arrival_ts - order.lookback
                if order.arrival_block is None
                else None
            ),
            end_ts=order.deadline_ts if order.arrival_block is None else None,
        )
        for order in v3_orders
    ]


def materialize_clickhouse_trade_catalog(
    root: str | Path,
    windows: Iterable[RequiredTradeWindow],
    *,
    source_pin: str,
    profile_hash: str,
    strategy_hash: str,
    client: ClickHouseClient | None = None,
    merge_gap_blocks: int = 0,
    max_rows_per_window: int = 1_000_000,
    resume: bool = False,
) -> TradeCatalogManifest:
    """Scan each merged source interval once and publish a reusable catalog."""

    ch = client or ClickHouseClient()
    merged = merge_required_trade_windows(windows, merge_gap_blocks=merge_gap_blocks)
    builder = ParquetTradeCatalogBuilder(
        root,
        source_pin=source_pin,
        profile_hash=profile_hash,
        strategy_hash=strategy_hash,
        resume=resume,
    )
    try:
        for window in merged:
            loaded = load_v2_trade_slices_for_windows(
                [window],
                client=ch,
                limit_per_window=max_rows_per_window,
                reject_truncated_windows=True,
            )
            builder.append(
                loaded.trades,
                high_watermark={
                    "market_id": window.market_id,
                    "asset_id": window.asset_id,
                    "end_block": window.end_block,
                    "end_ts": window.end_ts.isoformat() if window.end_ts else None,
                },
            )
        return builder.finalize()
    except Exception:
        builder.abort(preserve_resume_state=True)
        raise


def _trades_to_table(trades: Sequence[V2TradePrint]) -> pa.Table:
    rows = {
        "trade_id": [row.trade_id for row in trades],
        "trade_group_id": [row.trade_group_id for row in trades],
        "market_id": [row.market_id for row in trades],
        "condition_id": [row.condition_id for row in trades],
        "asset_id": [row.asset_id.lower() for row in trades],
        "outcome": [row.outcome for row in trades],
        "block_number": [row.block_number for row in trades],
        "block_time": [_utc(row.block_time) for row in trades],
        "tx_hash": [row.tx_hash for row in trades],
        "tx_index": [row.tx_index for row in trades],
        "tx_index_source": [row.tx_index_source for row in trades],
        "price": [row.price for row in trades],
        "size_shares": [row.size for row in trades],
        "notional_usdc": [row.notional for row in trades],
        "aggressor_side": [row.aggressor_side for row in trades],
        "passive_side": [row.passive_side for row in trades],
        "source_log_indexes": [list(row.source_log_indexes) for row in trades],
        "source_fill_count": [row.source_fill_count for row in trades],
    }
    return pa.Table.from_pydict(rows, schema=TRADE_SCHEMA)


def _table_to_trades(table: pa.Table) -> Iterator[V2TradePrint]:
    columns = {name: table.column(name).to_pylist() for name in TRADE_SCHEMA.names}
    trade_ids = columns["trade_id"]
    trade_group_ids = columns["trade_group_id"]
    market_ids = columns["market_id"]
    condition_ids = columns["condition_id"]
    asset_ids = columns["asset_id"]
    outcomes = columns["outcome"]
    block_numbers = columns["block_number"]
    block_times = columns["block_time"]
    tx_hashes = columns["tx_hash"]
    tx_indexes = columns["tx_index"]
    tx_index_sources = columns["tx_index_source"]
    prices = columns["price"]
    sizes = columns["size_shares"]
    notionals = columns["notional_usdc"]
    aggressor_sides = columns["aggressor_side"]
    passive_sides = columns["passive_side"]
    source_log_indexes = columns["source_log_indexes"]
    source_fill_counts = columns["source_fill_count"]
    for index in range(table.num_rows):
        block_time = block_times[index]
        if not isinstance(block_time, datetime):
            yield trade_print_from_row(
                {name: values[index] for name, values in columns.items()}
            )
            continue
        trade_group_id = trade_group_ids[index]
        yield V2TradePrint(
            trade_id=str(trade_ids[index]),
            market_id=int(market_ids[index]),
            condition_id=str(condition_ids[index] or ""),
            asset_id=str(asset_ids[index]).lower(),
            outcome=str(outcomes[index] or ""),
            block_number=int(block_numbers[index]),
            block_time=block_time,
            tx_hash=str(tx_hashes[index]).lower(),
            tx_index=int(tx_indexes[index] or 0),
            tx_index_source=str(tx_index_sources[index] or ""),
            price=_as_decimal(prices[index]),
            size=_as_decimal(sizes[index]),
            notional=_as_decimal(notionals[index]),
            aggressor_side=cast(
                Literal["BUY", "SELL"], str(aggressor_sides[index]).upper()
            ),
            passive_side=cast(
                Literal["BUY", "SELL"], str(passive_sides[index]).upper()
            ),
            source_log_indexes=tuple(
                int(value) for value in (source_log_indexes[index] or ())
            ),
            source_fill_count=int(source_fill_counts[index] or 0),
            trade_group_id=str(trade_group_id) if trade_group_id else None,
        )


def _as_decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _manifest_from_payload(payload: dict[str, Any]) -> TradeCatalogManifest:
    if payload.get("schema_version") not in {
        CATALOG_SCHEMA_VERSION,
        *LEGACY_CATALOG_SCHEMA_VERSIONS,
    }:
        raise ValueError("unsupported trade catalog schema")
    return TradeCatalogManifest(
        schema_version=str(payload["schema_version"]),
        source_table=str(payload["source_table"]),
        source_pin=str(payload["source_pin"]),
        profile_hash=str(payload["profile_hash"]),
        strategy_hash=str(payload["strategy_hash"]),
        arrow_schema_sha256=str(payload["arrow_schema_sha256"]),
        row_group_size=int(payload["row_group_size"]),
        partition_mode=_partition_mode(payload.get("partition_mode", "date")),
        rows=int(payload["rows"]),
        files=tuple(TradeCatalogFile(**item) for item in payload["files"]),
        manifest_sha256=str(payload["manifest_sha256"]),
    )


def _verify_manifest_hash(payload: dict[str, Any]) -> None:
    expected = str(payload.get("manifest_sha256") or "")
    draft = {key: value for key, value in payload.items() if key != "manifest_sha256"}
    if not expected or _canonical_sha256(draft) != expected:
        raise ValueError("catalog manifest checksum mismatch")


def _partition_mode(value: Any) -> TradeCatalogPartitionMode:
    normalized = str(value)
    if normalized not in {"date", "market_asset_date"}:
        raise ValueError(f"unsupported trade catalog partition mode: {normalized}")
    return cast(TradeCatalogPartitionMode, normalized)


def _schema_sha256(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_ts(value: str) -> datetime:
    return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _file_overlaps_window(
    item: TradeCatalogFile,
    window: RequiredTradeWindow,
    *,
    time_bounds: tuple[datetime, datetime] | None = None,
) -> bool:
    min_ts, max_ts = time_bounds or (_parse_ts(item.min_ts), _parse_ts(item.max_ts))
    return bool(
        (window.start_block is None or item.max_block >= int(window.start_block))
        and (window.end_block is None or item.min_block <= int(window.end_block))
        and (window.start_ts is None or max_ts >= window.start_ts)
        and (window.end_ts is None or min_ts <= window.end_ts)
    )


def _trade_in_window(trade: V2TradePrint, window: RequiredTradeWindow) -> bool:
    return bool(
        trade.market_id == window.market_id
        and trade.asset_id.lower() == window.asset_id.lower()
        and (
            window.start_block is None
            or trade.block_number >= int(window.start_block)
        )
        and (window.end_block is None or trade.block_number <= int(window.end_block))
        and (
            window.start_ts is None
            or trade.block_time >= window.start_ts
        )
        and (window.end_ts is None or trade.block_time <= window.end_ts)
    )


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _same_atomic_boundary(left: V2TradePrint, right: V2TradePrint) -> bool:
    if _utc(left.block_time) == _utc(right.block_time):
        return True
    if left.tx_hash and left.tx_hash.lower() == right.tx_hash.lower():
        return True
    return bool(
        left.trade_group_id
        and right.trade_group_id
        and left.trade_group_id == right.trade_group_id
    )
