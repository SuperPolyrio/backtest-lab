"""Replay local L2 archive parquet into point-in-time order books."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from quant.backtest.l2_orderfilled_execution import BookLevel, BookSnapshot
from quant.core.db import postgres_connection
from quant.orderbook.l2_active_active import (
    ACTIVE_ACTIVE_COVERAGE_TABLE,
    PRIMARY_FEED_SOURCE,
    SECONDARY_FEED_SOURCE,
    dedupe_active_active_rows,
)
from quant.orderbook.l2_coverage_manifest import (
    DEFAULT_COVERAGE_TABLE,
    coverage_table_sql,
)
from quant.orderbook.l2_subscription_snapshot import subscription_affinity_shard
from quant.orderbook.subscriptions import token_shard
from quant.orderbook.three_source_gold_overlay import (
    ThreeSourceGoldOverlayError,
    VerifiedThreeSourceGoldSet,
    dedupe_gold_overlay_rows,
    load_verified_three_source_gold,
)


def _validate_latest_book_clock(
    exchange_ts: datetime | None,
    received_at: datetime | None,
    *,
    cutoff: datetime,
) -> None:
    if (exchange_ts is None) != (received_at is None):
        raise ValueError(
            "latest book exchange and received timestamps must be provided together"
        )
    if exchange_ts is not None and received_at is not None:
        exchange = _coerce_datetime(exchange_ts)
        received = _coerce_datetime(received_at)
        if exchange > received:
            raise ValueError(
                "latest book exchange timestamp cannot exceed received timestamp"
            )
        if received > _coerce_datetime(cutoff):
            raise ValueError(
                "latest book received timestamp cannot exceed checkpoint cutoff"
            )


@dataclass(frozen=True)
class L2ReplayCheckpoint:
    asset_id: str
    timestamp: datetime
    latest_received_at: datetime | None
    latest_source: str | None
    row_count: int
    has_ws_book: bool
    has_rest_seed_book: bool
    has_price_change: bool
    snapshot_version: str
    generation: int
    tick_size: Decimal | None
    book_quality: str
    latest_book_is_rest: bool
    price_changes_after_latest_book: int
    source_files: tuple[str, ...]
    # True source clocks for the last event that changed the reconstructed
    # book state. ``timestamp`` remains the requested PIT cutoff.
    latest_book_exchange_ts: datetime | None = None
    latest_book_received_at: datetime | None = None

    def __post_init__(self) -> None:
        _validate_latest_book_clock(
            self.latest_book_exchange_ts,
            self.latest_book_received_at,
            cutoff=self.timestamp,
        )


@dataclass(frozen=True)
class L2ReplaySnapshot:
    snapshot: BookSnapshot
    checkpoint: L2ReplayCheckpoint


@dataclass(frozen=True)
class L2StateCheckpoint:
    asset_id: str
    timestamp: datetime
    latest_received_at: datetime | None
    latest_source: str | None
    market_id: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    row_count: int
    generation: int
    tick_size: Decimal | None
    has_ws_book: bool
    has_rest_seed_book: bool
    has_price_change: bool
    latest_book_is_rest: bool
    price_changes_after_latest_book: int
    snapshot_version: str
    source_files: tuple[str, ...]
    # Optional for backward compatibility with checkpoints produced before
    # the dual-clock state provenance contract.
    latest_book_exchange_ts: datetime | None = None
    latest_book_received_at: datetime | None = None

    def __post_init__(self) -> None:
        _validate_latest_book_clock(
            self.latest_book_exchange_ts,
            self.latest_book_received_at,
            cutoff=self.timestamp,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "timestamp": self.timestamp.isoformat(),
            "latest_received_at": self.latest_received_at.isoformat() if self.latest_received_at else None,
            "latest_book_exchange_ts": (
                self.latest_book_exchange_ts.isoformat()
                if self.latest_book_exchange_ts
                else None
            ),
            "latest_book_received_at": (
                self.latest_book_received_at.isoformat()
                if self.latest_book_received_at
                else None
            ),
            "latest_source": self.latest_source,
            "market_id": self.market_id,
            "bids": [[str(price), str(size)] for price, size in self.bids],
            "asks": [[str(price), str(size)] for price, size in self.asks],
            "row_count": self.row_count,
            "generation": self.generation,
            "tick_size": str(self.tick_size) if self.tick_size is not None else None,
            "has_ws_book": self.has_ws_book,
            "has_rest_seed_book": self.has_rest_seed_book,
            "has_price_change": self.has_price_change,
            "latest_book_is_rest": self.latest_book_is_rest,
            "price_changes_after_latest_book": self.price_changes_after_latest_book,
            "snapshot_version": self.snapshot_version,
            "source_files": list(self.source_files),
        }


@dataclass(frozen=True)
class HashBoundL2ArchiveReceipt:
    source_files: tuple[str, ...]
    source_manifest_hash: str
    file_sha256: tuple[tuple[str, str], ...]
    row_count: int
    asset_ids: tuple[str, ...]


@dataclass(frozen=True)
class L2TopOfBookFenceResult:
    applied_change_count: int
    deleted_bid_prices: tuple[Decimal, ...]
    deleted_ask_prices: tuple[Decimal, ...]
    raw_best_bid: Decimal | None
    raw_best_ask: Decimal | None


@dataclass(frozen=True)
class L2CoverageGoldContract:
    """Gold evidence that one materialized token-hour promises to replay."""

    row_exists: bool = False
    repair_window_count: int = 0
    repair_manifest_sha256: tuple[str, ...] = ()
    overlay_event_count: int = 0
    overlay_manifest_sha256: tuple[str, ...] = ()
    validation_status: str = "DISABLED"

    @property
    def manifest_sha256(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                set(self.repair_manifest_sha256)
                | set(self.overlay_manifest_sha256)
            )
        )


class L2ReplayNotReady(RuntimeError):
    """Raised when the archive cannot produce a valid point-in-time book."""


class L2ArchiveReplayReader:
    """Read compressed local L2 archive parquet and reconstruct books."""

    def __init__(
        self,
        archive_dir: Path | str = Path("runtime_outputs/lob_l2_archive_full"),
        *,
        shard_count: int | None = None,
        coverage_table: str = DEFAULT_COVERAGE_TABLE,
        active_active_sources: Sequence[str] | None = None,
        active_active_shard_counts: Sequence[int] = (48, 2, 48, 1),
        baseline_search_hours: int = 0,
        require_proven_shard_routes: bool = False,
        require_complete_top_hints: bool = False,
        archive_file_allowlist: Sequence[Path | str] | None = None,
        three_source_gold_root: Path | str | None = None,
    ) -> None:
        self.archive_dir = Path(archive_dir)
        self.shard_count = shard_count
        self.active_active_sources = tuple(str(item) for item in (active_active_sources or ()) if str(item))
        self.active_active_shard_counts = tuple(int(item) for item in active_active_shard_counts if int(item) > 0)
        self.baseline_search_hours = max(0, int(baseline_search_hours))
        self.require_proven_shard_routes = bool(require_proven_shard_routes)
        self.require_complete_top_hints = bool(require_complete_top_hints)
        self.archive_file_allowlist = self._validated_archive_file_allowlist(
            archive_file_allowlist
        )
        if self.archive_file_allowlist is not None and three_source_gold_root is not None:
            raise ValueError(
                "three_source_gold_root cannot be combined with an explicit archive allowlist"
            )
        self.three_source_gold_root = (
            None
            if three_source_gold_root is None or not str(three_source_gold_root).strip()
            else Path(three_source_gold_root)
        )
        self._gold_overlay_paths: set[str] = set()
        self._gold_manifest_sha256: set[str] = set()
        self._gold_validation_attempted = False
        if self.active_active_sources and coverage_table == DEFAULT_COVERAGE_TABLE:
            coverage_table = ACTIVE_ACTIVE_COVERAGE_TABLE
        self.coverage_table = coverage_table_sql(coverage_table)
        self._coverage_route_cache: dict[
            tuple[str, datetime],
            tuple[datetime | None, str | None, int | None, int | None, tuple[int, ...]],
        ] = {}
        self._coverage_gold_cache: dict[
            tuple[str, datetime], L2CoverageGoldContract
        ] = {}
        self._archive_file_cache: dict[
            tuple[datetime | None, datetime | None, tuple[int, ...] | None, datetime | None],
            tuple[str, ...],
        ] = {}

    @property
    def three_source_gold_manifest_sha256(self) -> tuple[str, ...]:
        """Strict manifest SHAs that contributed to this reader instance."""

        return tuple(sorted(self._gold_manifest_sha256))

    @property
    def three_source_gold_validation_status(self) -> str:
        """Status of strict Gold validation attempted by this reader."""

        if self.three_source_gold_root is None:
            return "DISABLED"
        if self._gold_manifest_sha256:
            return "PASS"
        return (
            "NO_MATCHING_MANIFEST"
            if self._gold_validation_attempted
            else "NOT_EVALUATED"
        )

    def snapshot_at(
        self,
        *,
        asset_id: str,
        timestamp: datetime,
        require_price_change: bool = False,
        allow_rest_seed_only: bool = False,
        allow_one_sided: bool = False,
    ) -> L2ReplaySnapshot:
        ts = _coerce_datetime(timestamp)
        (
            baseline,
            condition_id,
            connection_shard_id,
            expected_archive_rows,
            observed_archive_shards,
        ) = self._coverage_route(str(asset_id), ts)
        shard_ids = self._shard_candidates(
            str(asset_id),
            condition_id=condition_id,
            assigned_shard_id=connection_shard_id,
            observed_shard_ids=observed_archive_shards,
        )
        if baseline is None and self.baseline_search_hours > 0:
            baseline = self._find_latest_book_baseline(
                str(asset_id),
                ts,
                max_hours=self.baseline_search_hours,
                shard_ids=shard_ids,
            )
        files = self._archive_files(
            since=baseline,
            until=ts,
            shard_id=shard_ids,
            baseline_at=baseline,
        )
        self._require_coverage_gold_contract(
            str(asset_id),
            ts,
            coverage_row_exists=expected_archive_rows is not None,
        )
        file_list = _duckdb_file_list(files)
        filters = ["asset_id = ?", "timestamp_received <= ?"]
        params: list[Any] = [str(asset_id), ts]
        if self.active_active_sources:
            allowed_sources = (*self.active_active_sources, "polymarket_clob_rest_seed", "polymarket_clob_rest_reconcile")
            params.extend(allowed_sources)
            filters.append(
                self._gold_inclusive_filter(
                    "source IN (" + ",".join("?" for _ in allowed_sources) + ")",
                    params,
                )
            )
        elif self.shard_count is not None:
            params.append(int(self.shard_count))
            filters.append(self._gold_inclusive_filter("shard_count = ?", params))
        where = " AND ".join(filters)
        def read_rows(candidate_file_list: str) -> Any:
            con = duckdb.connect(
                ":memory:",
                config={"threads": str(max(1, int(os.environ.get("BOOK_L2_DUCKDB_THREADS", "8"))))},
            )
            try:
                return con.execute(
                f"""
                SELECT event_type, timestamp_received, timestamp, market, asset_id,
                       collector_seq, sequence_in_message, raw_connection_id,
                       raw_connection_generation, raw_frame_seq, message_index, group_id,
                       change_index, bids, asks,
                       price, size, side, best_bid, best_ask, fee_rate_bps,
                       transaction_hash, source, book_hash, payload_hash,
                       old_tick_size, new_tick_size, filename
                FROM read_parquet({candidate_file_list}, filename=true, union_by_name=true)
                WHERE {where}
                ORDER BY timestamp_received, collector_seq, sequence_in_message
                """,
                params,
                ).fetchdf()
            except duckdb.Error as exc:
                raise L2ReplayNotReady(
                    f"native L2 archive read failed: {exc}"
                ) from exc
            finally:
                con.close()

        rows = self._dedupe(read_rows(file_list))
        if expected_archive_rows is not None and _is_hour_end_replay_target(ts):
            # A token can move between collector shards during an hour.  The
            # hash/assignment candidates are enough for an intrahour point in
            # time, but a mature hour-end replay must include every shard file
            # promised by coverage.  Union that full-hour fleet with the
            # baseline files so state reconstruction cannot silently omit a
            # pre-cutover delta.
            hour_start = ts.replace(minute=0, second=0, microsecond=0)
            coverage_files = self._archive_files(
                since=hour_start,
                until=ts,
                shard_id=None,
                baseline_at=None,
            )
            complete_files = tuple(sorted(set(files) | set(coverage_files)))
            if set(complete_files) != set(files):
                rows = self._dedupe(
                    read_rows(_duckdb_file_list(complete_files))
                )
            actual_archive_rows = _archive_rows_in_target_hour(rows, ts)
            if actual_archive_rows < expected_archive_rows:
                raise L2ReplayNotReady(
                    "native L2 full-fleet archive is shorter than materialized "
                    "coverage: "
                    f"asset_id={asset_id} hour="
                    f"{ts.replace(minute=0, second=0, microsecond=0).isoformat()} "
                    f"expected_rows={expected_archive_rows} "
                    f"loaded_rows={actual_archive_rows}"
                )
        if rows.empty:
            raise L2ReplayNotReady(f"no L2 archive rows for {asset_id} at or before {ts.isoformat()}")
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        ready = False
        has_ws_book = False
        has_rest_seed_book = False
        has_price_change = False
        latest_book_is_rest = False
        price_changes_after_latest_book = 0
        latest_received: datetime | None = None
        latest_book_exchange_ts: datetime | None = None
        latest_book_received_at: datetime | None = None
        latest_source: str | None = None
        market_id = ""
        generation = 0
        tick_size: Decimal | None = None
        for grouped_rows in _group_native_replay_rows(rows):
            row = grouped_rows[-1]
            event_type = str(row.get("event_type") or "")
            source = str(row.get("source") or "")
            latest_source = source or latest_source
            latest_received = _coerce_datetime(row.get("timestamp_received")) or latest_received
            market_id = str(row.get("market") or market_id or "")
            if event_type == "book":
                bids = _levels_from_json(row.get("bids"))
                asks = _levels_from_json(row.get("asks"))
                (
                    latest_book_exchange_ts,
                    latest_book_received_at,
                ) = _book_state_clock_from_row(row, asset_id=str(asset_id))
                ready = True
                generation += 1
                latest_book_is_rest = _is_rest_book_source(source)
                price_changes_after_latest_book = 0
                if latest_book_is_rest:
                    has_rest_seed_book = True
                else:
                    has_ws_book = True
                continue
            if event_type == "tick_size_change":
                tick_size = _decimal_or_none(row.get("new_tick_size")) or tick_size
                if tick_size is not None:
                    (
                        latest_book_exchange_ts,
                        latest_book_received_at,
                    ) = _book_state_clock_from_row(row, asset_id=str(asset_id))
                continue
            if event_type != "price_change" or not ready:
                continue
            fence = apply_price_change_group_with_top_fence(
                bids,
                asks,
                grouped_rows,
                asset_id=str(asset_id),
                require_complete_top_hints=self.require_complete_top_hints,
            )
            state_change_count = (
                fence.applied_change_count
                + len(fence.deleted_bid_prices)
                + len(fence.deleted_ask_prices)
            )
            if state_change_count <= 0:
                continue
            (
                latest_book_exchange_ts,
                latest_book_received_at,
            ) = _book_state_clock_from_row(row, asset_id=str(asset_id))
            has_price_change = True
            price_changes_after_latest_book += fence.applied_change_count
        if not ready:
            raise L2ReplayNotReady(f"no book baseline for {asset_id} at or before {ts.isoformat()}")
        if require_price_change and price_changes_after_latest_book <= 0:
            raise L2ReplayNotReady(f"no price_change after latest book baseline for {asset_id} at or before {ts.isoformat()}")
        if latest_book_is_rest and price_changes_after_latest_book <= 0 and not allow_rest_seed_only:
            raise L2ReplayNotReady(f"latest book baseline is REST-only for {asset_id} at or before {ts.isoformat()}")
        _validate_uncrossed_book(bids, asks, asset_id=str(asset_id))
        if (not bids or not asks) and not allow_one_sided:
            raise L2ReplayNotReady(f"book is missing one side for {asset_id} at or before {ts.isoformat()}")
        bids_tuple = tuple(sorted(bids.items(), key=lambda item: item[0], reverse=True))
        asks_tuple = tuple(sorted(asks.items(), key=lambda item: item[0]))
        version = _snapshot_version(asset_id, ts, bids_tuple, asks_tuple, row_count=len(rows))
        source_files = tuple(sorted(str(item) for item in rows["filename"].dropna().unique()))
        checkpoint = L2ReplayCheckpoint(
            asset_id=str(asset_id),
            timestamp=ts,
            latest_received_at=latest_received,
            latest_source=latest_source,
            row_count=len(rows),
            has_ws_book=has_ws_book,
            has_rest_seed_book=has_rest_seed_book,
            has_price_change=has_price_change,
            snapshot_version=version,
            generation=generation,
            tick_size=tick_size,
            book_quality=_book_quality(bids_tuple, asks_tuple),
            latest_book_is_rest=latest_book_is_rest,
            price_changes_after_latest_book=price_changes_after_latest_book,
            source_files=source_files,
            latest_book_exchange_ts=latest_book_exchange_ts,
            latest_book_received_at=latest_book_received_at,
        )
        snapshot = BookSnapshot(
            ts=ts,
            market_id=market_id,
            asset_id=str(asset_id),
            sequence=len(rows),
            source="local_l2_archive_replay",
            bids=tuple(BookLevel(price, size) for price, size in bids_tuple),
            asks=tuple(BookLevel(price, size) for price, size in asks_tuple),
            hash=version,
            is_full_depth=True,
        )
        return L2ReplaySnapshot(snapshot=snapshot, checkpoint=checkpoint)

    def create_checkpoint(self, *, asset_id: str, timestamp: datetime) -> L2StateCheckpoint:
        replay = self.snapshot_at(
            asset_id=asset_id,
            timestamp=timestamp,
            allow_rest_seed_only=True,
            allow_one_sided=True,
        )
        return L2StateCheckpoint(
            asset_id=str(asset_id),
            timestamp=_coerce_datetime(timestamp),
            latest_received_at=replay.checkpoint.latest_received_at,
            latest_source=replay.checkpoint.latest_source,
            market_id=replay.snapshot.market_id,
            bids=tuple((level.price, level.size) for level in replay.snapshot.bids),
            asks=tuple((level.price, level.size) for level in replay.snapshot.asks),
            row_count=replay.checkpoint.row_count,
            generation=replay.checkpoint.generation,
            tick_size=replay.checkpoint.tick_size,
            has_ws_book=replay.checkpoint.has_ws_book,
            has_rest_seed_book=replay.checkpoint.has_rest_seed_book,
            has_price_change=replay.checkpoint.has_price_change,
            latest_book_is_rest=replay.checkpoint.latest_book_is_rest,
            price_changes_after_latest_book=replay.checkpoint.price_changes_after_latest_book,
            snapshot_version=replay.checkpoint.snapshot_version,
            source_files=replay.checkpoint.source_files,
            latest_book_exchange_ts=replay.checkpoint.latest_book_exchange_ts,
            latest_book_received_at=replay.checkpoint.latest_book_received_at,
        )

    def snapshot_from_checkpoint(
        self,
        checkpoint: L2StateCheckpoint,
        *,
        timestamp: datetime,
        allow_one_sided: bool = False,
    ) -> L2ReplaySnapshot:
        ts = _coerce_datetime(timestamp)
        if ts < checkpoint.timestamp:
            raise ValueError("replay target precedes checkpoint")
        (
            _,
            condition_id,
            connection_shard_id,
            expected_archive_rows,
            observed_archive_shards,
        ) = self._coverage_route(checkpoint.asset_id, ts)
        shard_ids = self._shard_candidates(
            checkpoint.asset_id,
            condition_id=condition_id or checkpoint.market_id,
            assigned_shard_id=connection_shard_id,
            observed_shard_ids=observed_archive_shards,
        )
        files = self._archive_files(
            since=checkpoint.timestamp,
            until=ts,
            shard_id=shard_ids,
            baseline_at=checkpoint.timestamp,
        )
        self._require_coverage_gold_contract(
            checkpoint.asset_id,
            ts,
            coverage_row_exists=expected_archive_rows is not None,
        )
        file_list = _duckdb_file_list(files)
        filters = ["asset_id = ?", "timestamp_received > ?", "timestamp_received <= ?"]
        params: list[Any] = [checkpoint.asset_id, checkpoint.timestamp, ts]
        if self.active_active_sources:
            allowed_sources = (*self.active_active_sources, "polymarket_clob_rest_seed", "polymarket_clob_rest_reconcile")
            params.extend(allowed_sources)
            filters.append(
                self._gold_inclusive_filter(
                    "source IN (" + ",".join("?" for _ in allowed_sources) + ")",
                    params,
                )
            )
        elif self.shard_count is not None:
            params.append(int(self.shard_count))
            filters.append(self._gold_inclusive_filter("shard_count = ?", params))
        con = duckdb.connect(
            ":memory:",
            config={"threads": str(max(1, int(os.environ.get("BOOK_L2_DUCKDB_THREADS", "8"))))},
        )
        try:
            rows = con.execute(
                f"""
                SELECT event_type, timestamp_received, timestamp, market, asset_id,
                       collector_seq, sequence_in_message, raw_connection_id,
                       raw_connection_generation, raw_frame_seq, message_index, group_id,
                       change_index, bids, asks,
                       price, size, side, best_bid, best_ask, fee_rate_bps,
                       transaction_hash, source, book_hash, payload_hash,
                       old_tick_size, new_tick_size, filename
                FROM read_parquet({file_list}, filename=true, union_by_name=true)
                WHERE {' AND '.join(filters)}
                ORDER BY timestamp_received, collector_seq, sequence_in_message
                """,
                params,
            ).fetchdf()
        except duckdb.Error as exc:
            raise L2ReplayNotReady(
                f"native L2 event window read failed: {exc}"
            ) from exc
        finally:
            con.close()
        rows = self._dedupe(rows)

        bids = dict(checkpoint.bids)
        asks = dict(checkpoint.asks)
        generation = checkpoint.generation
        tick_size = checkpoint.tick_size
        has_ws_book = checkpoint.has_ws_book
        has_rest_seed_book = checkpoint.has_rest_seed_book
        has_price_change = checkpoint.has_price_change
        latest_book_is_rest = checkpoint.latest_book_is_rest
        price_changes_after_latest_book = checkpoint.price_changes_after_latest_book
        latest_received = checkpoint.latest_received_at
        latest_book_exchange_ts = checkpoint.latest_book_exchange_ts
        latest_book_received_at = checkpoint.latest_book_received_at
        latest_source = checkpoint.latest_source
        market_id = checkpoint.market_id
        for grouped_rows in _group_native_replay_rows(rows):
            row = grouped_rows[-1]
            event_type = str(row.get("event_type") or "")
            source = str(row.get("source") or "")
            latest_source = source or latest_source
            latest_received = _coerce_datetime(row.get("timestamp_received")) or latest_received
            market_id = str(row.get("market") or market_id or "")
            if event_type == "book":
                bids = _levels_from_json(row.get("bids"))
                asks = _levels_from_json(row.get("asks"))
                (
                    latest_book_exchange_ts,
                    latest_book_received_at,
                ) = _book_state_clock_from_row(
                    row,
                    asset_id=checkpoint.asset_id,
                )
                generation += 1
                latest_book_is_rest = _is_rest_book_source(source)
                price_changes_after_latest_book = 0
                if latest_book_is_rest:
                    has_rest_seed_book = True
                else:
                    has_ws_book = True
                continue
            if event_type == "tick_size_change":
                tick_size = _decimal_or_none(row.get("new_tick_size")) or tick_size
                if tick_size is not None:
                    (
                        latest_book_exchange_ts,
                        latest_book_received_at,
                    ) = _book_state_clock_from_row(
                        row,
                        asset_id=checkpoint.asset_id,
                    )
                continue
            if event_type != "price_change":
                continue
            fence = apply_price_change_group_with_top_fence(
                bids,
                asks,
                grouped_rows,
                asset_id=checkpoint.asset_id,
                require_complete_top_hints=self.require_complete_top_hints,
            )
            state_change_count = (
                fence.applied_change_count
                + len(fence.deleted_bid_prices)
                + len(fence.deleted_ask_prices)
            )
            if state_change_count <= 0:
                continue
            (
                latest_book_exchange_ts,
                latest_book_received_at,
            ) = _book_state_clock_from_row(
                row,
                asset_id=checkpoint.asset_id,
            )
            has_price_change = True
            price_changes_after_latest_book += fence.applied_change_count
        _validate_uncrossed_book(bids, asks, asset_id=checkpoint.asset_id)
        if (not bids or not asks) and not allow_one_sided:
            raise L2ReplayNotReady(f"book is missing one side for {checkpoint.asset_id} at or before {ts.isoformat()}")
        bids_tuple = tuple(sorted(bids.items(), key=lambda item: item[0], reverse=True))
        asks_tuple = tuple(sorted(asks.items(), key=lambda item: item[0]))
        row_count = checkpoint.row_count + len(rows)
        version = _snapshot_version(checkpoint.asset_id, ts, bids_tuple, asks_tuple, row_count=row_count)
        source_files = tuple(sorted(set(checkpoint.source_files) | {str(item) for item in rows["filename"].dropna().unique()}))
        replay_checkpoint = L2ReplayCheckpoint(
            asset_id=checkpoint.asset_id,
            timestamp=ts,
            latest_received_at=latest_received,
            latest_source=latest_source,
            row_count=row_count,
            has_ws_book=has_ws_book,
            has_rest_seed_book=has_rest_seed_book,
            has_price_change=has_price_change,
            snapshot_version=version,
            generation=generation,
            tick_size=tick_size,
            book_quality=_book_quality(bids_tuple, asks_tuple),
            latest_book_is_rest=latest_book_is_rest,
            price_changes_after_latest_book=price_changes_after_latest_book,
            source_files=source_files,
            latest_book_exchange_ts=latest_book_exchange_ts,
            latest_book_received_at=latest_book_received_at,
        )
        snapshot = BookSnapshot(
            ts=ts,
            market_id=market_id,
            asset_id=checkpoint.asset_id,
            sequence=row_count,
            source="local_l2_archive_checkpoint_replay",
            bids=tuple(BookLevel(price, size) for price, size in bids_tuple),
            asks=tuple(BookLevel(price, size) for price, size in asks_tuple),
            hash=version,
            is_full_depth=True,
        )
        return L2ReplaySnapshot(snapshot=snapshot, checkpoint=replay_checkpoint)

    def event_rows_between(
        self,
        *,
        asset_ids: Sequence[str],
        start_timestamp: datetime,
        end_timestamp: datetime,
        event_types: Sequence[str] = (
            "book",
            "price_change",
            "last_trade_price",
        ),
        max_rows: int = 50_000,
    ) -> Any:
        """Return ordered native archive events for a bounded replay window.

        The point-in-time snapshot helpers intentionally collapse history into
        one book state.  Maker replay needs the intervening trade and level
        events as well, so this method exposes the same archive routing and
        ordering without assigning any execution semantics to those rows.
        """

        start = _coerce_datetime(start_timestamp)
        end = _coerce_datetime(end_timestamp)
        if end < start:
            raise ValueError("event replay end_timestamp precedes start_timestamp")
        assets = tuple(sorted({str(item).strip() for item in asset_ids if str(item).strip()}))
        kinds = tuple(sorted({str(item).strip() for item in event_types if str(item).strip()}))
        if not assets or not kinds:
            raise ValueError("event replay requires asset_ids and event_types")
        limit = max(1, int(max_rows))
        routed_shards = self._event_window_route_shards(
            asset_ids=assets,
            start_timestamp=start,
            end_timestamp=end,
        )
        files = self._archive_files(
            since=start,
            until=end,
            shard_id=routed_shards,
            baseline_at=None,
        )
        self._require_window_coverage_gold_contracts(
            asset_ids=assets,
            start_timestamp=start,
            end_timestamp=end,
        )
        base_filters = [
            "timestamp_received > ?",
            "timestamp_received <= ?",
        ]
        params: list[Any] = [start, end]
        if self.active_active_sources:
            allowed_sources = (
                *self.active_active_sources,
                "polymarket_clob_rest_seed",
                "polymarket_clob_rest_reconcile",
            )
            params.extend(allowed_sources)
            base_filters.append(
                self._gold_inclusive_filter(
                    "source IN (" + ",".join("?" for _ in allowed_sources) + ")",
                    params,
                )
            )
        params.extend((*assets, *kinds))
        con = duckdb.connect(
            ":memory:",
            config={
                "threads": str(
                    max(1, int(os.environ.get("BOOK_L2_DUCKDB_THREADS", "8")))
                )
            },
        )
        try:
            rows = con.execute(
                f"""
                WITH bounded_archive AS (
                    SELECT event_type, timestamp_received, timestamp_normalized,
                           timestamp, market, asset_id, collector_seq,
                           sequence_in_message, raw_connection_id,
                           raw_connection_generation, raw_frame_seq,
                           raw_received_wall_ns, message_index, change_index,
                           group_id, is_last_in_group,
                           raw_frame_complete, bids, asks, price, size, side,
                           best_bid, best_ask,
                           source, transaction_hash, book_hash, payload_hash,
                           old_tick_size, new_tick_size, filename
                    FROM read_parquet(
                        {_duckdb_file_list(files)},
                        filename=true,
                        union_by_name=true
                    )
                    WHERE {' AND '.join(base_filters)}
                ), evidenced_archive AS (
                    SELECT *,
                           bool_or(coalesce(raw_frame_complete, false)) OVER (
                               PARTITION BY source, raw_connection_id,
                                            raw_connection_generation, raw_frame_seq
                           ) AS frame_raw_complete,
                           bool_or(coalesce(is_last_in_group, false)) OVER (
                               PARTITION BY source, raw_connection_id,
                                            raw_connection_generation, raw_frame_seq,
                                            group_id
                           ) AS group_has_terminal
                    FROM bounded_archive
                ), target_frames AS (
                    SELECT DISTINCT source, raw_connection_id,
                                    raw_connection_generation, raw_frame_seq
                    FROM evidenced_archive
                    WHERE asset_id IN ({','.join('?' for _ in assets)})
                      AND event_type IN ({','.join('?' for _ in kinds)})
                )
                SELECT archive.*
                FROM evidenced_archive AS archive
                INNER JOIN target_frames AS target
                    ON archive.source IS NOT DISTINCT FROM target.source
                   AND archive.raw_connection_id IS NOT DISTINCT FROM
                       target.raw_connection_id
                   AND archive.raw_connection_generation IS NOT DISTINCT FROM
                       target.raw_connection_generation
                   AND archive.raw_frame_seq IS NOT DISTINCT FROM
                       target.raw_frame_seq
                ORDER BY archive.timestamp_received, archive.collector_seq,
                         archive.sequence_in_message, archive.raw_frame_seq,
                         archive.message_index, archive.change_index,
                         archive.filename
                LIMIT {limit + 1}
                """,
                params,
            ).fetchdf()
        except duckdb.Error as exc:
            raise L2ReplayNotReady(
                f"native L2 event window read failed: {exc}"
            ) from exc
        finally:
            con.close()
        rows = self._dedupe(rows)
        if len(rows) > limit:
            raise L2ReplayNotReady(
                f"native L2 event window exceeds max_rows={limit}"
            )
        _validate_native_frame_evidence(rows)
        return rows

    def _event_window_route_shards(
        self,
        *,
        asset_ids: Sequence[str],
        start_timestamp: datetime,
        end_timestamp: datetime,
    ) -> tuple[int, ...] | None:
        """Return only coverage-proven shards for every asset/hour.

        Event replay must sometimes inspect another token row from the same
        raw frame to prove atomic completion. It must not inspect unrelated
        connection shards: besides being wasteful, a broken remote mount on an
        unrelated shard must not invalidate a request whose shard route is
        already proven. Missing route evidence fails closed; hash affinity is
        deliberately not used as a guess on this path.
        """

        if self.archive_file_allowlist is not None:
            return None
        start = _coerce_datetime(start_timestamp)
        end = _coerce_datetime(end_timestamp)
        current_hour = start.replace(minute=0, second=0, microsecond=0)
        end_hour = end.replace(minute=0, second=0, microsecond=0)
        routed: set[int] = set()
        while current_hour <= end_hour:
            probe = max(start, current_hour)
            for asset_id in asset_ids:
                (
                    _baseline,
                    _condition_id,
                    assigned_shard,
                    _archive_row_count,
                    observed_shards,
                ) = self._coverage_route(str(asset_id), probe)
                proven = {
                    int(value)
                    for value in observed_shards
                    if int(value) >= 0
                }
                if assigned_shard is not None and int(assigned_shard) >= 0:
                    proven.add(int(assigned_shard))
                if not proven:
                    raise L2ReplayNotReady(
                        "native L2 event shard route is unproven "
                        f"for asset_id={asset_id} hour={current_hour.isoformat()}"
                    )
                routed.update(proven)
            current_hour += timedelta(hours=1)
        if not routed:
            raise L2ReplayNotReady("native L2 event shard route is empty")
        return tuple(sorted(routed))

    def _coverage_route(
        self,
        asset_id: str,
        timestamp: datetime,
    ) -> tuple[datetime | None, str | None, int | None, int | None, tuple[int, ...]]:
        key = (str(asset_id), timestamp)
        route = self._coverage_route_cache.get(key)
        if route is None:
            route = _coverage_route(
                str(asset_id),
                timestamp,
                coverage_table=self.coverage_table,
                fail_on_query_error=self.require_proven_shard_routes,
            )
            self._coverage_route_cache[key] = route
        return route

    def coverage_gold_contract_at(
        self,
        *,
        asset_id: str,
        timestamp: datetime,
    ) -> L2CoverageGoldContract:
        """Return the materialized Gold provenance for one token-hour."""

        ts = _coerce_datetime(timestamp)
        hour = ts.astimezone(timezone.utc).replace(
            minute=0,
            second=0,
            microsecond=0,
        )
        key = (str(asset_id), hour)
        contract = self._coverage_gold_cache.get(key)
        if contract is None:
            contract = _coverage_gold_contract(
                str(asset_id),
                hour,
                coverage_table=self.coverage_table,
                fail_on_query_error=(
                    self.three_source_gold_root is not None
                    or self.require_proven_shard_routes
                ),
            )
            self._coverage_gold_cache[key] = contract
        return contract

    def coverage_replay_checkpoint_at(
        self,
        *,
        asset_id: str,
        timestamp: datetime,
    ) -> datetime:
        """Return the materialized baseline that is safe for checkpoint replay."""

        target = _coerce_datetime(timestamp)
        baseline = self._coverage_route(str(asset_id), target)[0]
        if baseline is None:
            return target.replace(minute=0, second=0, microsecond=0)
        checkpoint = _coerce_datetime(baseline)
        if checkpoint > target:
            raise L2ReplayNotReady(
                "materialized L2 baseline is after replay target: "
                f"asset_id={asset_id} baseline={checkpoint.isoformat()} "
                f"target={target.isoformat()}"
            )
        return checkpoint

    def _require_coverage_gold_contract(
        self,
        asset_id: str,
        timestamp: datetime,
        *,
        coverage_row_exists: bool,
    ) -> L2CoverageGoldContract:
        if not coverage_row_exists:
            return L2CoverageGoldContract()
        contract = self.coverage_gold_contract_at(
            asset_id=asset_id,
            timestamp=timestamp,
        )
        expected = set(contract.manifest_sha256)
        if not expected:
            return contract
        loaded = set(self._gold_manifest_sha256)
        missing = sorted(expected - loaded)
        if missing:
            raise L2ReplayNotReady(
                "materialized coverage declares three-source Gold evidence "
                "that replay did not load: "
                f"asset_id={asset_id} hour="
                f"{_coerce_datetime(timestamp).replace(minute=0, second=0, microsecond=0).isoformat()} "
                f"expected_manifest_sha256={sorted(expected)!r} "
                f"loaded_manifest_sha256={sorted(loaded)!r}"
            )
        return contract

    def _require_window_coverage_gold_contracts(
        self,
        *,
        asset_ids: Sequence[str],
        start_timestamp: datetime,
        end_timestamp: datetime,
    ) -> None:
        current_hour = _coerce_datetime(start_timestamp).replace(
            minute=0,
            second=0,
            microsecond=0,
        )
        end_hour = _coerce_datetime(end_timestamp).replace(
            minute=0,
            second=0,
            microsecond=0,
        )
        while current_hour <= end_hour:
            for asset_id in asset_ids:
                route = self._coverage_route(str(asset_id), current_hour)
                self._require_coverage_gold_contract(
                    str(asset_id),
                    current_hour,
                    coverage_row_exists=route[3] is not None,
                )
            current_hour += timedelta(hours=1)

    def _find_latest_book_baseline(
        self,
        asset_id: str,
        timestamp: datetime,
        *,
        max_hours: int,
        shard_ids: Sequence[int] | None,
    ) -> datetime | None:
        upper = timestamp.astimezone(timezone.utc)
        step = timedelta(
            minutes=max(
                1,
                int(os.environ.get("PML2_XUE_BASELINE_SEARCH_STEP_MINUTES", "5")),
            )
        )
        max_steps = max(1, int(max_hours * 3600 / step.total_seconds()))
        for _ in range(max_steps):
            lower = upper - step
            try:
                files = self._archive_files(
                    since=lower,
                    until=upper,
                    shard_id=shard_ids,
                    baseline_at=None,
                )
            except FileNotFoundError:
                files = []
            if not files:
                continue
            con = duckdb.connect(
                ":memory:",
                config={
                    "threads": str(
                        max(
                            1,
                            int(os.environ.get("BOOK_L2_DUCKDB_THREADS", "8")),
                        )
                    )
                },
            )
            try:
                row = con.execute(
                    f"""
                    SELECT max(timestamp_received)
                    FROM read_parquet(
                        {_duckdb_file_list(files)},
                        union_by_name=true
                    )
                    WHERE asset_id = ?
                      AND event_type = 'book'
                      AND timestamp_received > ?
                      AND timestamp_received <= ?
                    """,
                    [asset_id, lower, upper],
                ).fetchone()
            except duckdb.Error as exc:
                raise L2ReplayNotReady(
                    f"native L2 baseline search failed: {exc}"
                ) from exc
            finally:
                con.close()
            if row and row[0] is not None:
                return _coerce_datetime(row[0])
            upper = lower
        return None

    def _archive_files(
        self,
        *,
        since: datetime | None,
        until: datetime | None,
        shard_id: int | Sequence[int] | None,
        baseline_at: datetime | None,
    ) -> list[str]:
        if since is not None and until is not None and until < since:
            raise ValueError("replay archive interval must not be reversed")
        if self.archive_file_allowlist is not None:
            return list(self.archive_file_allowlist)
        normalized_shards = (
            None
            if shard_id is None
            else (
                (int(shard_id),)
                if isinstance(shard_id, int)
                else tuple(sorted({int(item) for item in shard_id}))
            )
        )
        key = (since, until, normalized_shards, baseline_at)
        cached = self._archive_file_cache.get(key)
        if cached is None:
            cached = tuple(
                _archive_files(
                    self.archive_dir,
                    since=since,
                    until=until,
                    shard_id=normalized_shards,
                    shard_count=self.shard_count,
                    baseline_at=baseline_at,
                )
            )
            self._archive_file_cache[key] = cached
        base_files = list(cached)
        if self.three_source_gold_root is None or (
            since is not None and until is not None and until == since
        ):
            return base_files
        try:
            gold = load_verified_three_source_gold(
                self.three_source_gold_root,
                since=since,
                until=until,
                memory_limit=os.environ.get(
                    "BOOK_L2_GOLD_VALIDATION_MEMORY_LIMIT", "2GB"
                ),
            )
        except ThreeSourceGoldOverlayError as exc:
            raise L2ReplayNotReady(
                f"three-source Gold evidence failed closed: {exc}"
            ) from exc
        self._gold_validation_attempted = True
        self._remember_gold(gold)
        return [*base_files, *(str(path) for path in gold.overlay_paths)]

    def _remember_gold(self, gold: VerifiedThreeSourceGoldSet) -> None:
        self._gold_overlay_paths.update(str(path) for path in gold.overlay_paths)
        self._gold_manifest_sha256.update(gold.manifest_sha256)

    def _shard_candidates(
        self,
        asset_id: str,
        *,
        condition_id: str | None = None,
        assigned_shard_id: int | None = None,
        observed_shard_ids: Sequence[int] | None = None,
    ) -> tuple[int, ...] | None:
        if self.archive_file_allowlist is not None:
            return None
        if self.require_proven_shard_routes:
            proven = {
                int(value)
                for value in (observed_shard_ids or ())
                if int(value) >= 0
            }
            if assigned_shard_id is not None and int(assigned_shard_id) >= 0:
                proven.add(int(assigned_shard_id))
            if not proven:
                raise L2ReplayNotReady(
                    "native L2 checkpoint shard route is unproven "
                    f"for asset_id={asset_id}"
                )
            return tuple(sorted(proven))
        if not self.active_active_sources:
            return _replay_shard_candidates(
                asset_id,
                self.shard_count,
                condition_id=condition_id,
                assigned_shard_id=assigned_shard_id,
                observed_shard_ids=observed_shard_ids,
            )
        candidates: set[int] = set()
        for count in self.active_active_shard_counts:
            routed = _replay_shard_candidates(
                asset_id,
                count,
                condition_id=condition_id,
                assigned_shard_id=(
                    assigned_shard_id
                    if assigned_shard_id is not None
                    and assigned_shard_id < count
                    else None
                ),
                observed_shard_ids=tuple(
                    shard
                    for shard in (observed_shard_ids or ())
                    if 0 <= int(shard) < count
                ),
            )
            if routed is None:
                return None
            candidates.update(routed)
        return tuple(sorted(candidates)) or None

    def _validated_archive_file_allowlist(
        self,
        values: Sequence[Path | str] | None,
    ) -> tuple[str, ...] | None:
        if values is None:
            return None
        if not values:
            raise ValueError("archive file allowlist cannot be empty")
        try:
            root = self.archive_dir.resolve(strict=True)
        except OSError as exc:
            raise ValueError(f"archive root is unavailable: {exc}") from exc
        resolved: set[str] = set()
        for value in values:
            try:
                candidate = Path(value).resolve(strict=True)
                candidate.relative_to(root)
            except (OSError, ValueError) as exc:
                raise ValueError(
                    "archive allowlist file must resolve under archive root"
                ) from exc
            if not candidate.is_file() or candidate.suffix.lower() != ".parquet":
                raise ValueError("archive allowlist entries must be parquet files")
            resolved.add(str(candidate))
        if len(resolved) != len(values):
            raise ValueError("archive file allowlist contains duplicate paths")
        return tuple(sorted(resolved))

    def _gold_inclusive_filter(
        self,
        base_predicate: str,
        params: list[Any],
    ) -> str:
        paths = sorted(self._gold_overlay_paths)
        if not paths:
            return base_predicate
        params.extend(paths)
        return (
            f"({base_predicate} OR filename IN "
            f"({','.join('?' for _ in paths)}))"
        )

    def _dedupe(self, rows: Any) -> Any:
        result = rows
        if self.active_active_sources:
            result = dedupe_active_active_rows(
                result,
                source_priority=self.active_active_sources
                or (PRIMARY_FEED_SOURCE, SECONDARY_FEED_SOURCE),
            )
        return dedupe_gold_overlay_rows(
            result,
            gold_overlay_paths=self._gold_overlay_paths,
        )


def verify_hash_bound_archive_files(
    archive_dir: Path | str,
    source_files: Sequence[Mapping[str, str]],
    *,
    asset_ids: Sequence[str],
) -> HashBoundL2ArchiveReceipt:
    """Verify an explicit, self-contained archive packet before replay."""

    if not source_files:
        raise L2ReplayNotReady("hash-bound archive source_files cannot be empty")
    required_assets = tuple(
        sorted({str(item).strip() for item in asset_ids if str(item).strip()})
    )
    if not required_assets:
        raise L2ReplayNotReady("hash-bound archive requires target asset_ids")
    try:
        root = Path(archive_dir).resolve(strict=True)
    except OSError as exc:
        raise L2ReplayNotReady(f"archive root is unavailable: {exc}") from exc
    entries: list[dict[str, Any]] = []
    resolved_paths: list[str] = []
    seen_paths: set[Path] = set()
    for item in source_files:
        raw_path = str(item.get("path") or "").strip()
        expected_sha256 = str(item.get("sha256") or "").strip().lower()
        if not raw_path or len(expected_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in expected_sha256
        ):
            raise L2ReplayNotReady(
                "hash-bound archive entries require path and lowercase SHA256"
            )
        requested = Path(raw_path)
        candidate = requested if requested.is_absolute() else root / requested
        try:
            resolved = candidate.resolve(strict=True)
            relative = resolved.relative_to(root)
        except (OSError, ValueError) as exc:
            raise L2ReplayNotReady(
                "hash-bound archive file must resolve under archive root"
            ) from exc
        if resolved in seen_paths:
            raise L2ReplayNotReady("hash-bound archive contains duplicate files")
        seen_paths.add(resolved)
        if not resolved.is_file() or resolved.suffix.lower() != ".parquet":
            raise L2ReplayNotReady(
                "hash-bound archive entries must be readable parquet files"
            )
        digest = hashlib.sha256()
        try:
            with resolved.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
            stat = resolved.stat()
        except OSError as exc:
            raise L2ReplayNotReady(
                f"hash-bound archive file is unreadable: {relative.as_posix()}"
            ) from exc
        actual_sha256 = digest.hexdigest()
        if actual_sha256 != expected_sha256:
            raise L2ReplayNotReady(
                "hash-bound archive SHA256 mismatch for "
                f"{relative.as_posix()}"
            )
        entries.append(
            {
                "path": relative.as_posix(),
                "sha256": actual_sha256,
                "size": stat.st_size,
            }
        )
        resolved_paths.append(str(resolved))
    file_list = _duckdb_file_list(tuple(sorted(resolved_paths)))
    con = duckdb.connect(
        ":memory:",
        config={
            "threads": str(
                max(1, int(os.environ.get("BOOK_L2_DUCKDB_THREADS", "8")))
            )
        },
    )
    try:
        summary = con.execute(
            f"""
            WITH archive AS (
                SELECT asset_id, source, timestamp, timestamp_received,
                       raw_connection_id, raw_connection_generation,
                       raw_frame_seq, group_id, is_last_in_group,
                       raw_frame_complete
                FROM read_parquet(
                    {file_list},
                    union_by_name=true
                )
            ), invalid_frames AS (
                SELECT 1
                FROM archive
                GROUP BY source, raw_connection_id, raw_connection_generation,
                         raw_frame_seq
                HAVING count(*) FILTER (WHERE raw_frame_complete) <> 1
            ), invalid_groups AS (
                SELECT 1
                FROM archive
                GROUP BY source, raw_connection_id, raw_connection_generation,
                         raw_frame_seq, group_id
                HAVING count(*) FILTER (WHERE is_last_in_group) <> 1
            )
            SELECT
                (SELECT count(*) FROM archive) AS row_count,
                (SELECT count(*) FROM archive
                 WHERE raw_connection_id IS NULL OR raw_connection_id = ''
                    OR raw_connection_generation IS NULL
                    OR raw_frame_seq IS NULL
                    OR group_id IS NULL OR group_id = '') AS missing_provenance,
                (SELECT count(*) FROM archive
                 WHERE timestamp IS NULL OR timestamp_received IS NULL
                    OR timestamp > timestamp_received) AS invalid_clocks,
                (SELECT count(*) FROM invalid_frames) AS invalid_frame_count,
                (SELECT count(*) FROM invalid_groups) AS invalid_group_count
            """
        ).fetchone()
        present_assets = {
            str(row[0])
            for row in con.execute(
                f"""
                SELECT DISTINCT asset_id
                FROM read_parquet({file_list}, union_by_name=true)
                WHERE asset_id IN ({','.join('?' for _ in required_assets)})
                """,
                list(required_assets),
            ).fetchall()
        }
    except duckdb.Error as exc:
        raise L2ReplayNotReady(
            f"hash-bound archive schema/content validation failed: {exc}"
        ) from exc
    finally:
        con.close()
    assert summary is not None
    row_count, missing_provenance, invalid_clocks, invalid_frames, invalid_groups = (
        int(value or 0) for value in summary
    )
    missing_assets = sorted(set(required_assets) - present_assets)
    if row_count <= 0:
        raise L2ReplayNotReady("hash-bound archive packet is empty")
    if missing_assets:
        raise L2ReplayNotReady(
            f"hash-bound archive is missing target asset_ids: {missing_assets}"
        )
    if missing_provenance:
        raise L2ReplayNotReady(
            "hash-bound archive has rows without raw frame/group provenance"
        )
    if invalid_clocks:
        raise L2ReplayNotReady(
            "hash-bound archive has missing or causally inverted clocks"
        )
    if invalid_frames or invalid_groups:
        raise L2ReplayNotReady(
            "hash-bound archive is not globally frame/group complete: "
            f"invalid_frames={invalid_frames}, invalid_groups={invalid_groups}"
        )
    canonical_entries = sorted(entries, key=lambda item: str(item["path"]))
    source_manifest_hash = hashlib.sha256(
        json.dumps(
            canonical_entries,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return HashBoundL2ArchiveReceipt(
        source_files=tuple(sorted(resolved_paths)),
        source_manifest_hash=source_manifest_hash,
        file_sha256=tuple(
            (str(item["path"]), str(item["sha256"]))
            for item in canonical_entries
        ),
        row_count=row_count,
        asset_ids=tuple(sorted(present_assets)),
    )


def _archive_files(
    path: Path,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    shard_id: int | Sequence[int] | None = None,
    shard_count: int | None = None,
    baseline_at: datetime | None = None,
) -> list[str]:
    manifest_files = _manifest_routed_archive_files(
        path,
        since=since,
        until=until,
        shard_id=shard_id,
        shard_count=shard_count,
        baseline_at=baseline_at,
    )
    if manifest_files is not None:
        if not manifest_files:
            raise FileNotFoundError(f"no ready manifest files under {path}")
        from .l2_archive_read_cache import materialize_replay_files_from_env

        return materialize_replay_files_from_env(manifest_files, archive_dir=path)

    bounded_partition_files: list[str] | None = None
    if since is not None and until is not None:
        start_hour = since.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0) if since else None
        end_hour = until.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0) if until else None
        selected: list[str] = []
        current = start_hour
        while current is not None and end_hour is not None and current <= end_hour:
            selected.extend(_partition_hour_files(path, current, shard_id=shard_id))
            current += timedelta(hours=1)
        if selected or (path / f"dt={start_hour:%Y-%m-%d}").exists():
            bounded_partition_files = sorted(set(selected))
    if bounded_partition_files is not None:
        files = bounded_partition_files
    else:
        files = sorted(
            str(item)
            for item in path.rglob("*.parquet")
            if not item.name.endswith(".tmp.parquet") and _matches_shard(item, shard_id)
        )
    if not files:
        raise FileNotFoundError(f"no parquet files under {path}")
    from .l2_archive_read_cache import materialize_replay_files_from_env

    return materialize_replay_files_from_env(files, archive_dir=path)


def _partition_hour_files(
    path: Path,
    hour: datetime,
    *,
    shard_id: int | Sequence[int] | None,
) -> list[str]:
    normalized = hour.astimezone(timezone.utc)
    hour_dir = (
        path
        / f"dt={normalized:%Y-%m-%d}"
        / f"hour={normalized:%H}"
    )
    try:
        return sorted(
            str(item)
            for item in hour_dir.glob("*.parquet")
            if not item.name.endswith(".tmp.parquet")
            and _matches_shard(item, shard_id)
        )
    except OSError:
        return []


def _manifest_routed_archive_files(
    path: Path,
    *,
    since: datetime | None,
    until: datetime | None,
    shard_id: int | Sequence[int] | None,
    shard_count: int | None,
    baseline_at: datetime | None,
) -> list[str] | None:
    """Resolve replay files from the durable manifest when it is available.

    Source-A WS files are routed by the coverage row's observed shards.  The
    ``shardall`` files are REST baseline batches; opening every such batch for
    every replay sample makes the gate quadratic in tokens and files.  A
    finalized coverage baseline is the latest book at the replay target, so
    only the REST batch whose receive interval contains that exact baseline can
    contribute to reconstruction.

    Returning ``None`` deliberately keeps the filesystem fallback for unit
    tests and standalone archives that do not have a manifest database.
    """
    if since is None and until is None:
        return None
    root = path.absolute()
    start_hour = (
        since.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        if since is not None
        else None
    )
    end_hour = (
        until.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        if until is not None
        else None
    )
    shard_ids = (
        None
        if shard_id is None
        else (
            [int(shard_id)]
            if isinstance(shard_id, int)
            else sorted({int(item) for item in shard_id})
        )
    )
    predicates = ["status = 'ready'"]
    params: list[Any] = []
    if start_hour is not None:
        predicates.append("archive_hour >= %s")
        params.append(start_hour)
    if end_hour is not None:
        predicates.append("archive_hour <= %s")
        params.append(end_hour)
    if since is not None:
        predicates.append("last_received_at >= %s")
        params.append(since)
    if until is not None:
        predicates.append("first_received_at <= %s")
        params.append(until)
    if shard_count is not None:
        predicates.append("shard_count = %s")
        params.append(int(shard_count))
    if shard_ids is not None:
        if baseline_at is None:
            predicates.append("shard_id = ANY(%s::integer[])")
            params.append(shard_ids)
        else:
            predicates.append(
                "(shard_id = ANY(%s::integer[]) OR "
                "(shard_id IS NULL AND first_received_at <= %s "
                "AND last_received_at >= %s))"
            )
            params.extend((shard_ids, baseline_at, baseline_at))
    try:
        with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT path FROM quant.clob_l2_archive_manifest WHERE "
                + " AND ".join(predicates)
                + " ORDER BY archive_hour, path",
                params,
            )
            rows = cur.fetchall()
    except Exception:  # noqa: BLE001 - optional inventory DB fallback
        return None
    selected: list[str] = []
    for row in rows:
        candidate = _rebase_archive_path(
            Path(str(row["path"])).absolute(),
            root,
        )
        try:
            candidate.relative_to(root)
        except ValueError:
            continue
        if candidate.name.endswith(".tmp.parquet"):
            continue
        try:
            if not candidate.exists():
                continue
        except OSError:
            continue
        selected.append(str(candidate))
    return selected or None


def _rebase_archive_path(candidate: Path, root: Path) -> Path:
    """Map a local manifest path onto an equivalent XUE archive mount."""

    try:
        candidate.relative_to(root)
        return candidate
    except ValueError:
        pass
    parts = candidate.parts
    partition_index = next(
        (index for index, part in enumerate(parts) if part.startswith("dt=")),
        None,
    )
    if partition_index is None:
        return candidate
    return root.joinpath(*parts[partition_index:]).absolute()


def _matches_shard(path: Path, shard_id: int | Sequence[int] | None) -> bool:
    if shard_id is None:
        return True
    if path.stem.endswith("_shardall"):
        return True
    shard_ids = {int(shard_id)} if isinstance(shard_id, int) else {int(item) for item in shard_id}
    return any(path.stem.endswith(f"_shard{item}") for item in shard_ids)


def _replay_shard_candidates(
    asset_id: str,
    shard_count: int | None,
    *,
    condition_id: str | None = None,
    assigned_shard_id: int | None = None,
    observed_shard_ids: Sequence[int] | None = None,
) -> tuple[int, ...] | None:
    if shard_count is None:
        return None
    if assigned_shard_id is not None and not (
        0 <= int(assigned_shard_id) < int(shard_count)
    ):
        raise ValueError("assigned shard is outside shard_count")
    observed = {
        int(value)
        for value in (observed_shard_ids or ())
        if 0 <= int(value) < int(shard_count)
    }
    if (
        not str(condition_id or "").strip()
        and assigned_shard_id is None
        and not observed
    ):
        # New archives use condition affinity. Without condition metadata there
        # is no safe single-shard prediction, so scan the bounded fleet rather
        # than silently miss rows.
        return None
    current = token_shard(asset_id, shard_count=int(shard_count))
    condition = subscription_affinity_shard(
        asset_id=asset_id,
        condition_id=condition_id,
        shard_count=int(shard_count),
    )
    # A short-lived archive rollout used MD5 before the stable SHA256 contract.
    # Reading all three candidates preserves both immutable layouts across the
    # condition-affinity migration.
    legacy = int(hashlib.md5(asset_id.encode("utf-8")).hexdigest()[:12], 16) % int(shard_count)
    candidates = {current, condition, legacy}
    if assigned_shard_id is not None:
        # Load-aware generation planning deliberately overrides stable hash
        # affinity. The finalized token-hour coverage row records the actual
        # connection shard that wrote the archive and is authoritative for
        # replay routing across rebalances.
        candidates.add(int(assigned_shard_id))
    candidates.update(observed)
    return tuple(sorted(candidates))


def _coverage_baseline(
    asset_id: str,
    timestamp: datetime,
    *,
    coverage_table: str = DEFAULT_COVERAGE_TABLE,
) -> datetime | None:
    return _coverage_route(
        asset_id,
        timestamp,
        coverage_table=coverage_table,
    )[0]


def _coverage_gold_contract(
    asset_id: str,
    timestamp: datetime,
    *,
    coverage_table: str = DEFAULT_COVERAGE_TABLE,
    fail_on_query_error: bool = False,
) -> L2CoverageGoldContract:
    """Read the immutable Gold SHA/count contract for one token-hour."""

    table = coverage_table_sql(coverage_table)
    hour = _coerce_datetime(timestamp).astimezone(timezone.utc).replace(
        minute=0,
        second=0,
        microsecond=0,
    )
    try:
        with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT three_source_gold_repair_window_count,
                       three_source_gold_repair_manifest_sha256,
                       three_source_gold_overlay_event_count,
                       three_source_gold_overlay_manifest_sha256,
                       three_source_gold_validation_status
                FROM {table}
                WHERE asset_id = %s
                  AND hour_start = %s
                """,
                (str(asset_id), hour),
            )
            row = cur.fetchone()
    except Exception as exc:
        if fail_on_query_error:
            raise L2ReplayNotReady(
                "native L2 Gold coverage query failed: "
                f"table={table} asset_id={asset_id} hour={hour.isoformat()} "
                f"error={type(exc).__name__}: {str(exc)[:500]}"
            ) from exc
        return L2CoverageGoldContract()
    if not row:
        return L2CoverageGoldContract()
    try:
        repair_count = int(row.get("three_source_gold_repair_window_count") or 0)
        overlay_count = int(row.get("three_source_gold_overlay_event_count") or 0)
        repair_sha = _normalize_gold_manifest_sha256(
            row.get("three_source_gold_repair_manifest_sha256")
        )
        overlay_sha = _normalize_gold_manifest_sha256(
            row.get("three_source_gold_overlay_manifest_sha256")
        )
        status = str(
            row.get("three_source_gold_validation_status") or "DISABLED"
        ).strip()
        if repair_count < 0 or overlay_count < 0:
            raise ValueError("Gold coverage counts must be non-negative")
        if repair_count > 0 and not repair_sha:
            raise ValueError("Gold repair count is missing manifest SHA")
        if overlay_count > 0 and not overlay_sha:
            raise ValueError("Gold overlay count is missing manifest SHA")
        if repair_count == 0 and repair_sha:
            raise ValueError("Gold repair SHA has no repair window")
        if overlay_count == 0 and overlay_sha:
            raise ValueError("Gold overlay SHA has no overlay event")
        if (repair_count or overlay_count) and status != "PASS":
            raise ValueError("Gold coverage provenance is not validation PASS")
    except (TypeError, ValueError) as exc:
        raise L2ReplayNotReady(
            "invalid materialized three-source Gold coverage contract: "
            f"asset_id={asset_id} hour={hour.isoformat()} error={exc}"
        ) from exc
    return L2CoverageGoldContract(
        row_exists=True,
        repair_window_count=repair_count,
        repair_manifest_sha256=repair_sha,
        overlay_event_count=overlay_count,
        overlay_manifest_sha256=overlay_sha,
        validation_status=status,
    )


def _normalize_gold_manifest_sha256(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)):
        raise TypeError("Gold manifest SHA provenance must be an array")
    try:
        values = tuple(value)
    except TypeError as exc:
        raise ValueError("Gold manifest SHA provenance must be an array") from exc
    digests = tuple(sorted({str(item).strip() for item in values if item is not None}))
    invalid = tuple(
        digest
        for digest in digests
        if len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    )
    if invalid:
        raise ValueError(f"invalid Gold manifest SHA provenance: {invalid!r}")
    return digests


def _coverage_route(
    asset_id: str,
    timestamp: datetime,
    *,
    coverage_table: str = DEFAULT_COVERAGE_TABLE,
    fail_on_query_error: bool = False,
) -> tuple[
    datetime | None,
    str | None,
    int | None,
    int | None,
    tuple[int, ...],
]:
    table = coverage_table_sql(coverage_table)
    baseline: datetime | None = None
    condition_id: str | None = None
    connection_shard_id: int | None = None
    archive_row_count: int | None = None
    archive_shard_ids: tuple[int, ...] = ()
    try:
        with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT c.baseline_received_at, c.condition_id,
                       c.connection_shard_id, c.archive_row_count,
                       ARRAY(
                           SELECT DISTINCT observed.shard_id
                           FROM unnest(
                               COALESCE(c.archive_shard_ids, ARRAY[]::INTEGER[])
                               || COALESCE(b.archive_shard_ids, ARRAY[]::INTEGER[])
                           ) AS observed(shard_id)
                           ORDER BY observed.shard_id
                       ) AS archive_shard_ids
                FROM {table} c
                LEFT JOIN {table} b
                  ON b.asset_id = c.asset_id
                 AND b.hour_start = date_trunc('hour', c.baseline_received_at)
                WHERE c.asset_id = %s
                  AND c.hour_start = date_trunc('hour', %s::timestamptz)
                """,
                (asset_id, timestamp),
            )
            row = cur.fetchone()
        if row:
            baseline = row["baseline_received_at"] or None
            condition_id = str(row.get("condition_id") or "").strip() or None
            raw_shard_id = row.get("connection_shard_id")
            connection_shard_id = (
                int(raw_shard_id) if raw_shard_id is not None else None
            )
            raw_archive_rows = row.get("archive_row_count")
            archive_row_count = (
                int(raw_archive_rows) if raw_archive_rows is not None else None
            )
            archive_shard_ids = tuple(
                sorted(int(value) for value in (row.get("archive_shard_ids") or ()))
            )
    except Exception as exc:
        if fail_on_query_error:
            hour = timestamp.astimezone(timezone.utc).replace(
                minute=0,
                second=0,
                microsecond=0,
            )
            raise L2ReplayNotReady(
                "native L2 coverage route query failed: "
                f"table={table} asset_id={asset_id} "
                f"hour={hour.isoformat()} error={type(exc).__name__}: "
                f"{str(exc)[:500]}"
            ) from exc
        archive_shard_ids = ()
    if condition_id is not None:
        return (
            baseline,
            condition_id,
            connection_shard_id,
            archive_row_count,
            archive_shard_ids,
        )
    try:
        with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT condition_id
                FROM quant.paper_market_registry_tokens
                WHERE asset_id = %s
                LIMIT 1
                """,
                (asset_id,),
            )
            row = cur.fetchone()
        condition_id = str(row.get("condition_id") or "").strip() if row else ""
    except Exception:  # noqa: BLE001 - optional registry DB fallback
        condition_id = ""
    return (
        baseline,
        condition_id or None,
        connection_shard_id,
        archive_row_count,
        archive_shard_ids,
    )


def _hour_from_partition(path: Path) -> datetime | None:
    try:
        date_text = path.parent.name.removeprefix("dt=")
        hour = int(path.name.removeprefix("hour="))
        return datetime.fromisoformat(f"{date_text}T{hour:02d}:00:00+00:00")
    except (TypeError, ValueError):
        return None


def _duckdb_file_list(files: Sequence[str]) -> str:
    return "[" + ",".join("'" + item.replace("'", "''") + "'" for item in files) + "]"


def _group_native_replay_rows(rows: Any) -> tuple[tuple[Any, ...], ...]:
    """Keep one raw price-change group atomic during state reconstruction."""

    grouped: list[tuple[Any, ...]] = []
    pending: list[Any] = []
    pending_key: tuple[str, ...] | None = None
    for position, (_, row) in enumerate(rows.iterrows()):
        event_type = (
            ""
            if _frame_value_missing(row.get("event_type"))
            else str(row.get("event_type")).strip().lower()
        )
        if event_type != "price_change":
            if pending:
                grouped.append(tuple(pending))
                pending = []
                pending_key = None
            grouped.append((row,))
            continue
        identity_fields = (
            row.get("source"),
            row.get("raw_connection_id"),
            row.get("raw_connection_generation"),
            row.get("raw_frame_seq"),
            row.get("group_id"),
        )
        key: tuple[str, ...]
        if any(_frame_value_missing(item) for item in identity_fields):
            key = ("unproven-price-change", str(position))
        else:
            key = tuple(str(item).strip() for item in identity_fields)
        if pending and key != pending_key:
            grouped.append(tuple(pending))
            pending = []
        pending.append(row)
        pending_key = key
    if pending:
        grouped.append(tuple(pending))
    return tuple(grouped)


def apply_price_change_group_with_top_fence(
    bids: dict[Decimal, Decimal],
    asks: dict[Decimal, Decimal],
    rows: Sequence[Any],
    *,
    asset_id: str,
    require_complete_top_hints: bool = False,
) -> L2TopOfBookFenceResult:
    """Apply one raw atomic group and enforce its authoritative post-state top.

    The archive's ``best_bid``/``best_ask`` pair describes the state after the
    complete source message.  It can therefore remove stale levels outside the
    authoritative spread, but it can never supply a missing level or size.
    Mutations are staged so an invalid group fails without partially changing
    the caller's state.
    """

    if not rows:
        return L2TopOfBookFenceResult(0, (), (), None, None)
    label = _native_atomic_group_label(rows, asset_id=asset_id)
    hint_pairs: set[tuple[Decimal, Decimal]] = set()
    for row in rows:
        bid_missing = _frame_value_missing(row.get("best_bid"))
        ask_missing = _frame_value_missing(row.get("best_ask"))
        if bid_missing != ask_missing:
            raise L2ReplayNotReady(
                f"native L2 top hint is incomplete for {label}"
            )
        if bid_missing:
            continue
        raw_bid = _decimal_or_none(row.get("best_bid"))
        raw_ask = _decimal_or_none(row.get("best_ask"))
        if raw_bid is None or raw_ask is None:
            raise L2ReplayNotReady(
                f"native L2 top hint is not finite for {label}"
            )
        hint_pairs.add((raw_bid, raw_ask))
    if len(hint_pairs) > 1:
        raise L2ReplayNotReady(
            f"native L2 top hints disagree within atomic group for {label}"
        )
    fenced = bool(hint_pairs)
    staged_bids = dict(bids)
    staged_asks = dict(asks)
    applied_change_count = 0
    for row in rows:
        price = (
            None
            if _frame_value_missing(row.get("price"))
            else _decimal_or_none(row.get("price"))
        )
        size = (
            None
            if _frame_value_missing(row.get("size"))
            else _decimal_or_none(row.get("size"))
        )
        side = (
            ""
            if _frame_value_missing(row.get("side"))
            else str(row.get("side")).strip().upper()
        )
        target = (
            staged_bids
            if side in {"BUY", "BID", "BIDS"}
            else staged_asks
            if side in {"SELL", "ASK", "ASKS"}
            else None
        )
        if price is None or size is None or target is None:
            if fenced:
                raise L2ReplayNotReady(
                    f"native L2 fenced price_change is malformed for {label}"
                )
            continue
        # Prices at 0/1 are valid only in the authoritative top pair where
        # they mean an empty side.  A delta itself must always name an actual
        # in-market level, and a negative size is never a deletion signal.
        # Validate before mutating the staged state so malformed input cannot
        # be hidden by the following top fence.
        if price <= 0 or price >= 1 or size < 0:
            raise L2ReplayNotReady(
                f"native L2 price_change is out of range for {label}: "
                f"price={price}, size={size}"
            )
        if size == 0:
            target.pop(price, None)
        else:
            target[price] = size
        applied_change_count += 1
    if not fenced:
        if require_complete_top_hints:
            raise L2ReplayNotReady(
                f"native L2 strict replay requires a complete top pair for {label}"
            )
        _validate_uncrossed_book(
            staged_bids,
            staged_asks,
            asset_id=asset_id,
        )
        bids.clear()
        bids.update(staged_bids)
        asks.clear()
        asks.update(staged_asks)
        return L2TopOfBookFenceResult(
            applied_change_count,
            (),
            (),
            None,
            None,
        )

    raw_best_bid, raw_best_ask = next(iter(hint_pairs))
    # Polymarket's native price_change top pair uses the market boundaries as
    # empty-side sentinels: bid=0 means no bids and ask=1 means no asks.  They
    # are authoritative absence evidence, not executable price levels.  Keep
    # accepting only strict in-range prices for populated sides.
    expected_best_bid = None if raw_best_bid == 0 else raw_best_bid
    expected_best_ask = None if raw_best_ask == 1 else raw_best_ask
    if (
        raw_best_bid < 0
        or raw_best_bid >= 1
        or raw_best_ask <= 0
        or raw_best_ask > 1
        or (
            expected_best_bid is not None
            and expected_best_ask is not None
            and expected_best_bid >= expected_best_ask
        )
    ):
        raise L2ReplayNotReady(
            f"native L2 authoritative top is invalid for {label}: "
            f"bid={raw_best_bid}, ask={raw_best_ask}"
        )
    deleted_bid_prices = tuple(
        sorted(
            (
                price
                for price in staged_bids
                if expected_best_bid is None or price > expected_best_bid
            ),
            reverse=True,
        )
    )
    deleted_ask_prices = tuple(
        sorted(
            price
            for price in staged_asks
            if expected_best_ask is None or price < expected_best_ask
        )
    )
    for price in deleted_bid_prices:
        staged_bids.pop(price, None)
    for price in deleted_ask_prices:
        staged_asks.pop(price, None)
    actual_best_bid = max(staged_bids, default=None)
    actual_best_ask = min(staged_asks, default=None)
    if actual_best_bid != expected_best_bid or actual_best_ask != expected_best_ask:
        raise L2ReplayNotReady(
            "native L2 state cannot reach authoritative top without inventing "
            f"liquidity for {label}: "
            f"expected={expected_best_bid}/{expected_best_ask}, "
            f"actual={actual_best_bid}/{actual_best_ask}"
        )
    bids.clear()
    bids.update(staged_bids)
    asks.clear()
    asks.update(staged_asks)
    return L2TopOfBookFenceResult(
        applied_change_count=applied_change_count,
        deleted_bid_prices=deleted_bid_prices,
        deleted_ask_prices=deleted_ask_prices,
        raw_best_bid=expected_best_bid,
        raw_best_ask=expected_best_ask,
    )


def _validate_uncrossed_book(
    bids: Mapping[Decimal, Decimal],
    asks: Mapping[Decimal, Decimal],
    *,
    asset_id: str,
) -> None:
    if bids and asks and max(bids) >= min(asks):
        raise L2ReplayNotReady(
            "native L2 replay produced a locked or crossed book for "
            f"asset_id={asset_id}: bid={max(bids)}, ask={min(asks)}"
        )


def _native_atomic_group_label(rows: Sequence[Any], *, asset_id: str) -> str:
    row = rows[-1]
    group_id = (
        "unknown"
        if _frame_value_missing(row.get("group_id"))
        else str(row.get("group_id")).strip()
    )
    frame_seq = (
        "unknown"
        if _frame_value_missing(row.get("raw_frame_seq"))
        else str(row.get("raw_frame_seq")).strip()
    )
    return f"asset_id={asset_id}, frame={frame_seq}, group={group_id}"


def _levels_from_json(value: Any) -> dict[Decimal, Decimal]:
    if value in (None, ""):
        return {}
    parsed = json.loads(str(value))
    levels: dict[Decimal, Decimal] = {}
    if not isinstance(parsed, list):
        return levels
    for row in parsed:
        if not isinstance(row, list) or len(row) < 2:
            continue
        price = _decimal_or_none(row[0])
        size = _decimal_or_none(row[1])
        if price is not None and size is not None and price > 0 and size > 0:
            levels[price] = size
    return levels


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (ArithmeticError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _is_rest_book_source(source: str) -> bool:
    return source in {"polymarket_clob_rest_seed", "polymarket_clob_rest_reconcile"}


def _book_quality(
    bids: Sequence[tuple[Decimal, Decimal]],
    asks: Sequence[tuple[Decimal, Decimal]],
) -> str:
    if bids and asks:
        return "READY_TWO_SIDED"
    if bids or asks:
        return "ONE_SIDED"
    return "EMPTY"


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _optional_datetime(value: Any) -> datetime | None:
    if value is None or type(value).__name__ in {"NAType", "NaTType"}:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nat", "nan", "none"}:
        return None
    try:
        return _coerce_datetime(value)
    except (TypeError, ValueError):
        return None


def _book_state_clock_from_row(
    row: Any,
    *,
    asset_id: str,
) -> tuple[datetime | None, datetime | None]:
    exchange_ts = _optional_datetime(row.get("timestamp"))
    received_at = _optional_datetime(row.get("timestamp_received"))
    if exchange_ts is None or received_at is None:
        return None, None
    if exchange_ts > received_at:
        raise L2ReplayNotReady(
            "native L2 state clock is causally inverted "
            f"for asset_id={asset_id}: exchange={exchange_ts.isoformat()} "
            f"received={received_at.isoformat()}"
        )
    return exchange_ts, received_at


def _frame_value_missing(value: Any) -> bool:
    if value is None or type(value).__name__ in {"NAType", "NaTType"}:
        return True
    text = str(value).strip().lower()
    return text in {"", "nan", "nat", "none", "<na>"}


def _frame_bool(value: Any) -> bool:
    if _frame_value_missing(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "t", "yes"}


def _validate_native_frame_evidence(rows: Any) -> None:
    if rows.empty:
        return
    required = {
        "raw_connection_id",
        "group_id",
        "raw_frame_seq",
        "is_last_in_group",
        "raw_frame_complete",
        "frame_raw_complete",
        "group_has_terminal",
    }
    missing_columns = sorted(required - set(rows.columns))
    if missing_columns:
        raise L2ReplayNotReady(
            f"native L2 frame evidence columns missing: {missing_columns}"
        )
    for _, row in rows.iterrows():
        raw_connection_id = row.get("raw_connection_id")
        raw_group_id = row.get("group_id")
        group_id = (
            "" if _frame_value_missing(raw_group_id) else str(raw_group_id).strip()
        )
        if (
            _frame_value_missing(raw_connection_id)
            or not group_id
            or _frame_value_missing(row.get("raw_frame_seq"))
        ):
            raise L2ReplayNotReady(
                "native L2 frame evidence requires raw_connection_id, "
                "group_id, and raw_frame_seq"
            )
        if not _frame_bool(row.get("frame_raw_complete")):
            raise L2ReplayNotReady(
                f"native L2 raw frame is incomplete: group_id={group_id}"
            )
        if not _frame_bool(row.get("group_has_terminal")):
            raise L2ReplayNotReady(
                f"native L2 atomic group is incomplete: group_id={group_id}"
            )


def _is_hour_end_replay_target(timestamp: datetime) -> bool:
    hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
    return timestamp >= hour_start + timedelta(hours=1) - timedelta(milliseconds=1)


def _archive_rows_in_target_hour(rows: Any, timestamp: datetime) -> int:
    hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)
    received = rows["timestamp_received"]
    return int(((received >= hour_start) & (received < hour_end)).sum())


def _snapshot_version(
    asset_id: str,
    timestamp: datetime,
    bids: tuple[tuple[Decimal, Decimal], ...],
    asks: tuple[tuple[Decimal, Decimal], ...],
    *,
    row_count: int,
) -> str:
    digest = hashlib.sha256()
    digest.update(str(asset_id).encode("utf-8"))
    digest.update(b"|")
    digest.update(timestamp.isoformat().encode("ascii"))
    digest.update(b"|")
    digest.update(str(row_count).encode("ascii"))
    for levels in (bids, asks):
        digest.update(b"|")
        for price, size in levels:
            digest.update(str(price).encode("ascii"))
            digest.update(b":")
            digest.update(str(size).encode("ascii"))
            digest.update(b";")
    return digest.hexdigest()[:20]
