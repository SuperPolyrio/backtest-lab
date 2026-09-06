"""Adapters from the authoritative XUE Native L2 archive into PML2 contracts."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from quant.backtest.execution import BookSnapshot as LegacyBookSnapshot
from quant.backtest.l2_orderfilled_execution import (
    BookSnapshot as TimelineBookSnapshot,
)
from quant.core.db import postgres_connection
from quant.orderbook.l2_active_active import (
    ACTIVE_ACTIVE_COVERAGE_TABLE,
    ACTIVE_ACTIVE_GAP_TABLE,
    dedupe_active_active_rows,
)
from quant.orderbook.l2_replay import (
    HashBoundL2ArchiveReceipt,
    L2ArchiveReplayReader,
    L2ReplayNotReady,
    L2ReplaySnapshot,
    L2StateCheckpoint,
    apply_price_change_group_with_top_fence,
    verify_hash_bound_archive_files,
)

from .contracts import (
    BookDeltaEvent,
    BookFrameBatchEvent,
    BookLevel,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    EconomicBookSide,
    Outcome,
    RawOrderSide,
    TradeEvent,
    TransportCoverageState,
    TransportCoverageWindow,
    canonical_hash,
)

DEFAULT_ARCHIVE_CANDIDATES = (
    Path("/data/jiahuaiyu/prediction-market-quant/lob_l2_archive_xue"),
)
XUE_NATIVE_SOURCES = (
    "polymarket_market_ws_raw_a",
    "polymarket_market_ws_raw_b",
)
_RawGroupKey = tuple[str, str, str, str, str]


def default_l2_archive_dir() -> Path:
    configured = os.environ.get("PML2_L2_ARCHIVE_DIR")
    if configured:
        return Path(configured)
    for candidate in DEFAULT_ARCHIVE_CANDIDATES:
        try:
            if candidate.exists() and next(candidate.rglob("*.parquet"), None) is not None:
                return candidate
        except OSError:
            continue
    return DEFAULT_ARCHIVE_CANDIDATES[0]


def legacy_snapshot_to_pml2(
    snapshot: LegacyBookSnapshot | TimelineBookSnapshot,
    *,
    condition_id: str,
    market_id: str,
    outcome: Outcome,
    book_epoch: int,
    local_ts: datetime | None = None,
) -> BookSnapshotEvent:
    exchange_ts = getattr(snapshot, "timestamp", None) or getattr(
        snapshot, "ts", None
    ) or getattr(snapshot, "captured_at", None)
    if exchange_ts is None:
        raise ValueError("legacy L2 snapshot has no point-in-time timestamp")
    exchange_ts = _utc(exchange_ts)
    delivered = _utc(local_ts) if local_ts is not None else exchange_ts
    snapshot_id = str(
        getattr(snapshot, "snapshot_version", "")
        or getattr(snapshot, "snapshot_id", "")
        or getattr(snapshot, "hash", "")
        or getattr(snapshot, "event_id", "")
        or canonical_hash(
            {
                "condition_id": condition_id,
                "asset_id": getattr(
                    snapshot, "token_id", getattr(snapshot, "asset_id", "")
                ),
                "exchange_ts": exchange_ts,
                "bids": snapshot.bids,
                "asks": snapshot.asks,
            }
        )
    )
    return BookSnapshotEvent(
        snapshot_id=snapshot_id,
        condition_id=condition_id,
        market_id=market_id,
        asset_id=str(
            getattr(snapshot, "token_id", getattr(snapshot, "asset_id", ""))
        ),
        outcome=outcome,
        exchange_ts=exchange_ts,
        local_ts=delivered,
        book_epoch=book_epoch,
        bids=tuple(_level(item) for item in snapshot.bids),
        asks=tuple(_level(item) for item in snapshot.asks),
        source=snapshot.source,
        sequence=getattr(snapshot, "block_number", None)
        or getattr(snapshot, "sequence", None),
        is_full_depth=snapshot.is_full_depth,
        is_truncated=not snapshot.is_full_depth,
        depth_scope=(
            "FULL"
            if snapshot.is_full_depth
            else f"TOP_{getattr(snapshot, 'observed_depth_levels', None) or 'N'}"
        ),
        tick_size=getattr(snapshot, "tick_size", None),
        min_order_size=getattr(snapshot, "min_order_size", None),
        book_hash=str(getattr(snapshot, "hash", "") or snapshot_id),
    )


@dataclass(frozen=True)
class Pml2ColdRestoreResult:
    events: tuple[BookSnapshotEvent, ...]
    source_files: tuple[str, ...]
    source: str
    restored: bool
    reason: str
    row_count: int = 0
    source_manifest_hash: str = ""
    clock_verified: bool = False
    clock_evidence: str = "UNVERIFIED_CHECKPOINT_CLOCK"
    baseline_clock_count: int = 0


@dataclass(frozen=True)
class Pml2ArchiveEventRestoreResult:
    events: tuple[
        BookSnapshotEvent | BookFrameBatchEvent | BookLevelBatchEvent | TradeEvent,
        ...,
    ]
    source_files: tuple[str, ...]
    source: str
    restored: bool
    reason: str
    row_count: int = 0
    source_manifest_hash: str = ""
    snapshot_count: int = 0
    delta_count: int = 0
    trade_count: int = 0
    clock_verified: bool = False
    clock_evidence: str = "UNVERIFIED_ARCHIVE_CLOCK"
    baseline_clock_count: int = 0
    raw_event_clock_count: int = 0
    frame_evidence_verified: bool = False
    source_binding: str = "COVERAGE_ROUTE"
    hash_bound_file_count: int = 0
    source_file_sha256: tuple[tuple[str, str], ...] = ()
    transport_coverage_windows: tuple[TransportCoverageWindow, ...] = ()


class Pml2ArchiveSnapshotLoader:
    """Point-in-time XUE Native L2 restore with no future-snapshot fallback."""

    def __init__(
        self,
        archive_dir: Path | str | None = None,
        *,
        reader: L2ArchiveReplayReader | None = None,
    ) -> None:
        self.archive_dir = (
            default_l2_archive_dir() if archive_dir is None else Path(archive_dir)
        )
        self.reader = reader or L2ArchiveReplayReader(
            self.archive_dir,
            active_active_sources=XUE_NATIVE_SOURCES,
            baseline_search_hours=max(
                1,
                int(os.environ.get("PML2_XUE_BASELINE_SEARCH_HOURS", "24")),
            ),
            require_proven_shard_routes=True,
            require_complete_top_hints=True,
        )

    def restore_condition(
        self,
        *,
        condition_id: str,
        market_id: str,
        yes_asset_id: str,
        no_asset_id: str,
        start_time: datetime,
        book_epoch: int = 0,
        feed_latency_ms: int = 0,
    ) -> Pml2ColdRestoreResult:
        events: list[BookSnapshotEvent] = []
        files: set[str] = set()
        row_count = 0
        baseline_clock_count = 0
        all_clocks_verified = True
        for asset_id, outcome in (
            (yes_asset_id, Outcome.YES),
            (no_asset_id, Outcome.NO),
        ):
            try:
                replay = self.reader.snapshot_at(
                    asset_id=asset_id,
                    timestamp=_utc(start_time),
                    allow_one_sided=False,
                )
                checkpoint = _state_checkpoint_from_replay(replay)
                clock_verified = _checkpoint_clock_verified(checkpoint)
            except (L2ReplayNotReady, FileNotFoundError, OSError, ValueError) as exc:
                return Pml2ColdRestoreResult(
                    events=(),
                    source_files=tuple(sorted(files)),
                    source="xue_native_l2_archive",
                    restored=False,
                    reason=f"{type(exc).__name__}: {exc}",
                )
            files.update(replay.checkpoint.source_files)
            row_count += replay.checkpoint.row_count
            baseline_clock_count += int(clock_verified)
            all_clocks_verified = all_clocks_verified and clock_verified
            events.append(
                _checkpoint_event(
                    checkpoint,
                    condition_id=condition_id,
                    market_id=market_id,
                    outcome=outcome,
                    book_epoch=book_epoch,
                    feed_latency_ms=feed_latency_ms,
                )
            )
        try:
            source_manifest_hash = _source_manifest_hash(files)
        except OSError as exc:
            return Pml2ColdRestoreResult(
                events=(),
                source_files=tuple(sorted(files)),
                source="xue_native_l2_archive",
                restored=False,
                reason=f"archive_checksum_failed: {exc}",
                row_count=row_count,
            )
        return Pml2ColdRestoreResult(
            events=tuple(events),
            source_files=tuple(sorted(files)),
            source="xue_native_l2_archive",
            restored=True,
            reason="point_in_time_archive_restore_complete",
            row_count=row_count,
            source_manifest_hash=source_manifest_hash,
            clock_verified=all_clocks_verified and baseline_clock_count == 2,
            clock_evidence=(
                "CHECKPOINT_STATE_EVENT_DUAL_CLOCK_VERIFIED"
                if all_clocks_verified and baseline_clock_count == 2
                else "UNVERIFIED_CHECKPOINT_CLOCK"
            ),
            baseline_clock_count=baseline_clock_count,
        )

    def restore_condition_timeline(
        self,
        *,
        condition_id: str,
        market_id: str,
        yes_asset_id: str,
        no_asset_id: str,
        start_time: datetime,
        target_times: tuple[datetime, ...],
        book_epoch: int = 0,
        feed_latency_ms: int = 0,
    ) -> Pml2ColdRestoreResult:
        """Restore one baseline and ordered PIT snapshots without future fallback."""

        start = _utc(start_time)
        targets = tuple(
            sorted(
                {
                    _utc(item)
                    for item in target_times
                    if _utc(item) >= start
                }
            )
        )
        events: list[BookSnapshotEvent] = []
        files: set[str] = set()
        final_row_counts: dict[str, int] = {}
        baseline_clock_count = 0
        checkpoint_count = 0
        all_clocks_verified = True
        for asset_id, outcome in (
            (yes_asset_id, Outcome.YES),
            (no_asset_id, Outcome.NO),
        ):
            try:
                checkpoint = self.reader.create_checkpoint(
                    asset_id=asset_id,
                    timestamp=start,
                )
                checkpoint_count += 1
                clock_verified = _checkpoint_clock_verified(checkpoint)
                baseline_clock_count += int(clock_verified)
                all_clocks_verified = all_clocks_verified and clock_verified
                events.append(
                    _checkpoint_event(
                        checkpoint,
                        condition_id=condition_id,
                        market_id=market_id,
                        outcome=outcome,
                        book_epoch=book_epoch,
                        feed_latency_ms=feed_latency_ms,
                    )
                )
                files.update(checkpoint.source_files)
                for target in targets:
                    if target <= checkpoint.timestamp:
                        continue
                    replay = self.reader.snapshot_from_checkpoint(
                        checkpoint,
                        timestamp=target,
                    )
                    checkpoint = _state_checkpoint_from_replay(replay)
                    checkpoint_count += 1
                    clock_verified = _checkpoint_clock_verified(checkpoint)
                    baseline_clock_count += int(clock_verified)
                    all_clocks_verified = all_clocks_verified and clock_verified
                    events.append(
                        _checkpoint_event(
                            checkpoint,
                            condition_id=condition_id,
                            market_id=market_id,
                            outcome=outcome,
                            book_epoch=book_epoch,
                            feed_latency_ms=feed_latency_ms,
                        )
                    )
                    files.update(checkpoint.source_files)
                final_row_counts[asset_id] = checkpoint.row_count
            except (L2ReplayNotReady, FileNotFoundError, OSError, ValueError) as exc:
                return Pml2ColdRestoreResult(
                    events=(),
                    source_files=tuple(sorted(files)),
                    source="xue_native_l2_archive",
                    restored=False,
                    reason=f"{type(exc).__name__}: {exc}",
                    row_count=sum(final_row_counts.values()),
                )
        try:
            source_manifest_hash = _source_manifest_hash(files)
        except OSError as exc:
            return Pml2ColdRestoreResult(
                events=(),
                source_files=tuple(sorted(files)),
                source="xue_native_l2_archive",
                restored=False,
                reason=f"archive_checksum_failed: {exc}",
                row_count=sum(final_row_counts.values()),
            )
        events.sort(
            key=lambda item: (
                item.exchange_ts,
                item.outcome.value,
                item.asset_id,
                item.snapshot_id,
            )
        )
        return Pml2ColdRestoreResult(
            events=tuple(events),
            source_files=tuple(sorted(files)),
            source="xue_native_l2_archive",
            restored=True,
            reason="point_in_time_archive_timeline_restore_complete",
            row_count=sum(final_row_counts.values()),
            source_manifest_hash=source_manifest_hash,
            clock_verified=(
                all_clocks_verified
                and checkpoint_count > 0
                and baseline_clock_count == checkpoint_count
            ),
            clock_evidence=(
                "CHECKPOINT_TIMELINE_STATE_EVENT_DUAL_CLOCK_VERIFIED"
                if (
                    all_clocks_verified
                    and checkpoint_count > 0
                    and baseline_clock_count == checkpoint_count
                )
                else "UNVERIFIED_CHECKPOINT_CLOCK"
            ),
            baseline_clock_count=baseline_clock_count,
        )

    def restore_condition_events(
        self,
        *,
        condition_id: str,
        market_id: str,
        yes_asset_id: str,
        no_asset_id: str,
        start_time: datetime,
        end_time: datetime,
        book_epoch: int = 0,
        feed_latency_ms: int = 0,
        max_events: int = 50_000,
        source_files: Sequence[Mapping[str, str]] | None = None,
    ) -> Pml2ArchiveEventRestoreResult:
        """Restore a baseline plus every archived book delta and trade.

        XUE is the authoritative Native L2 source for this path.  The proof is
        scoped to the requested interval: both baseline clocks and every raw
        event clock/frame must validate before the session may upgrade its
        cold-restore temporal quality.
        """

        start = _utc(start_time)
        end = _utc(end_time)
        if end < start:
            raise ValueError("archive end_time precedes start_time")
        reader = self.reader
        hash_bound_receipt: HashBoundL2ArchiveReceipt | None = None
        if source_files is not None:
            try:
                hash_bound_receipt = verify_hash_bound_archive_files(
                    self.archive_dir,
                    source_files,
                    asset_ids=(yes_asset_id, no_asset_id),
                )
                reader = L2ArchiveReplayReader(
                    self.archive_dir,
                    active_active_sources=XUE_NATIVE_SOURCES,
                    baseline_search_hours=max(
                        1,
                        int(
                            os.environ.get(
                                "PML2_XUE_BASELINE_SEARCH_HOURS",
                                "24",
                            )
                        ),
                    ),
                    require_proven_shard_routes=True,
                    require_complete_top_hints=True,
                    archive_file_allowlist=(
                        hash_bound_receipt.source_files
                    ),
                )
            except (L2ReplayNotReady, OSError, ValueError) as exc:
                return Pml2ArchiveEventRestoreResult(
                    events=(),
                    source_files=(),
                    source="xue_native_l2_archive",
                    restored=False,
                    reason=f"{type(exc).__name__}: {exc}",
                    source_binding="HASH_BOUND_ALLOWLIST",
                )
        events: list[
            BookSnapshotEvent | BookFrameBatchEvent | BookLevelBatchEvent | TradeEvent
        ] = []
        files: set[str] = set(
            hash_bound_receipt.source_files
            if hash_bound_receipt is not None
            else ()
        )
        row_count = 0
        baseline_clock_count = 0
        baseline_exchange_by_outcome: dict[Outcome, datetime] = {}
        baseline_local_by_outcome: dict[Outcome, datetime] = {}
        initial_books: dict[
            str,
            tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]],
        ] = {}
        all_baseline_clocks_verified = True
        outcomes = {
            str(yes_asset_id): Outcome.YES,
            str(no_asset_id): Outcome.NO,
        }
        for asset_id, outcome in outcomes.items():
            try:
                checkpoint = reader.create_checkpoint(
                    asset_id=asset_id,
                    timestamp=start,
                )
                clock_verified = _checkpoint_clock_verified(checkpoint)
            except (L2ReplayNotReady, FileNotFoundError, OSError, ValueError) as exc:
                return Pml2ArchiveEventRestoreResult(
                    events=(),
                    source_files=tuple(sorted(files)),
                    source="xue_native_l2_archive",
                    restored=False,
                    reason=f"{type(exc).__name__}: {exc}",
                )
            baseline_event = _checkpoint_event(
                checkpoint,
                condition_id=condition_id,
                market_id=market_id,
                outcome=outcome,
                book_epoch=book_epoch,
                feed_latency_ms=feed_latency_ms,
            )
            events.append(baseline_event)
            baseline_clock_count += int(clock_verified)
            all_baseline_clocks_verified = (
                all_baseline_clocks_verified and clock_verified
            )
            baseline_exchange_by_outcome[outcome] = baseline_event.exchange_ts
            baseline_local_by_outcome[outcome] = baseline_event.local_ts
            initial_books[asset_id] = (
                {level.price: level.size for level in baseline_event.bids},
                {level.price: level.size for level in baseline_event.asks},
            )
            files.update(checkpoint.source_files)
            row_count += checkpoint.row_count
        try:
            rows = reader.event_rows_between(
                asset_ids=tuple(outcomes),
                start_timestamp=start,
                end_timestamp=end,
                max_rows=max_events,
            )
            rows = dedupe_active_active_rows(
                rows,
                source_priority=XUE_NATIVE_SOURCES,
            )
            frame_evidence_verified = _validate_archive_frame_evidence(rows)
        except (L2ReplayNotReady, FileNotFoundError, OSError) as exc:
            return Pml2ArchiveEventRestoreResult(
                events=(),
                source_files=tuple(sorted(files)),
                source="xue_native_l2_archive",
                restored=False,
                reason=f"{type(exc).__name__}: {exc}",
                row_count=row_count,
            )
        row_count += len(rows)
        if "filename" in rows.columns:
            files.update(
                str(item)
                for item in rows["filename"].dropna().unique()
                if str(item)
            )
        try:
            converted = _xue_rows_to_pml2_events(
                rows,
                condition_id=condition_id,
                market_id=market_id,
                outcomes=outcomes,
                book_epoch=book_epoch,
                feed_latency_ms=feed_latency_ms,
                initial_books=initial_books,
                require_complete_top_hints=True,
            )
            _validate_events_follow_baselines(
                converted,
                baseline_exchange_by_outcome=baseline_exchange_by_outcome,
                baseline_local_by_outcome=baseline_local_by_outcome,
            )
        except L2ReplayNotReady as exc:
            return Pml2ArchiveEventRestoreResult(
                events=(),
                source_files=tuple(sorted(files)),
                source="xue_native_l2_archive",
                restored=False,
                reason=f"archive_top_of_book_invalid: {exc}",
                row_count=row_count,
            )
        except ValueError as exc:
            return Pml2ArchiveEventRestoreResult(
                events=(),
                source_files=tuple(sorted(files)),
                source="xue_native_l2_archive",
                restored=False,
                reason=f"archive_causal_clock_invalid: {exc}",
                row_count=row_count,
            )
        events.extend(converted)
        events.sort(key=_pml2_archive_event_sort_key)
        try:
            transport_coverage_windows = _load_transport_coverage_windows(
                reader=reader,
                condition_id=condition_id,
                market_id=market_id,
                asset_ids=(yes_asset_id, no_asset_id),
                start=start,
                end=end,
            )
        except Exception as exc:  # noqa: BLE001 - external coverage evidence boundary
            return Pml2ArchiveEventRestoreResult(
                events=(),
                source_files=tuple(sorted(files)),
                source="xue_native_l2_archive",
                restored=False,
                reason=(
                    "transport_coverage_restore_failed: "
                    f"{type(exc).__name__}: {str(exc)[:500]}"
                ),
                row_count=row_count,
            )
        if hash_bound_receipt is not None:
            source_manifest_hash = hash_bound_receipt.source_manifest_hash
        else:
            try:
                source_manifest_hash = _source_manifest_hash(files)
            except OSError as exc:
                return Pml2ArchiveEventRestoreResult(
                    events=(),
                    source_files=tuple(sorted(files)),
                    source="xue_native_l2_archive",
                    restored=False,
                    reason=f"archive_inventory_failed: {exc}",
                    row_count=row_count,
                )
        snapshot_count = sum(isinstance(item, BookSnapshotEvent) for item in events)
        delta_count = sum(
            (
                sum(len(batch.updates) for batch in item.batches)
                if isinstance(item, BookFrameBatchEvent)
                else len(item.updates)
            )
            for item in events
            if isinstance(item, (BookFrameBatchEvent, BookLevelBatchEvent))
        )
        trade_count = sum(isinstance(item, TradeEvent) for item in events)
        return Pml2ArchiveEventRestoreResult(
            events=tuple(events),
            source_files=tuple(sorted(files)),
            source="xue_native_l2_archive",
            restored=True,
            reason=(
                "xue_native_l2_hash_bound_event_timeline_restore_complete"
                if hash_bound_receipt is not None
                else "xue_native_l2_event_timeline_restore_complete"
            ),
            row_count=row_count,
            source_manifest_hash=source_manifest_hash,
            snapshot_count=snapshot_count,
            delta_count=delta_count,
            trade_count=trade_count,
            clock_verified=(
                all_baseline_clocks_verified
                and baseline_clock_count == len(outcomes)
                and frame_evidence_verified
            ),
            clock_evidence=(
                "BASELINE_AND_RAW_EVENT_DUAL_CLOCK_FRAME_VERIFIED"
                if (
                    all_baseline_clocks_verified
                    and baseline_clock_count == len(outcomes)
                    and frame_evidence_verified
                )
                else "UNVERIFIED_ARCHIVE_CLOCK"
            ),
            baseline_clock_count=baseline_clock_count,
            raw_event_clock_count=len(rows),
            frame_evidence_verified=frame_evidence_verified,
            source_binding=(
                "HASH_BOUND_ALLOWLIST"
                if hash_bound_receipt is not None
                else "COVERAGE_ROUTE"
            ),
            hash_bound_file_count=(
                len(hash_bound_receipt.source_files)
                if hash_bound_receipt is not None
                else 0
            ),
            source_file_sha256=(
                hash_bound_receipt.file_sha256
                if hash_bound_receipt is not None
                else ()
            ),
            transport_coverage_windows=transport_coverage_windows,
        )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("archive replay timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _load_transport_coverage_windows(
    *,
    reader: L2ArchiveReplayReader,
    condition_id: str,
    market_id: str,
    asset_ids: tuple[str, str],
    start: datetime,
    end: datetime,
) -> tuple[TransportCoverageWindow, ...]:
    # Lightweight/test readers predate transport coverage. Event-driven book
    # validity still honors explicit replay gap/lifecycle events; production
    # readers additionally restore persisted transport windows, and any query
    # failure remains fail-closed in restore_condition_events.
    table = getattr(reader, "coverage_table", None)
    if table is None:
        return ()
    active_active = table == ACTIVE_ACTIVE_COVERAGE_TABLE
    outside_ready = (
        "fill_depth_ready_outside_gap"
        if active_active
        else "fill_depth_ready"
    )
    continuity_state = (
        "'COVERAGE_PROVEN'::text"
        if active_active
        else "connection_continuity_state"
    )
    dual_gap_count = "dual_gap_overlap_count" if active_active else "0::bigint"
    with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT asset_id, hour_start, fill_depth_ready, fill_depth_reason,
                   {outside_ready} AS outside_ready,
                   {continuity_state} AS continuity_state,
                   {dual_gap_count} AS dual_gap_count,
                   manifest_generated_at
            FROM {table}
            WHERE asset_id = ANY(%s)
              AND hour_start >= date_trunc('hour', %s::timestamptz)
              AND hour_start <= date_trunc('hour', %s::timestamptz)
            ORDER BY hour_start, asset_id
            """,
            (list(asset_ids), start, end),
        )
        rows = tuple(dict(item) for item in cur.fetchall())
        gaps: tuple[dict[str, Any], ...] = ()
        if active_active:
            cur.execute(
                f"""
                SELECT asset_id, gap_start, recovered_at
                FROM {ACTIVE_ACTIVE_GAP_TABLE}
                WHERE asset_id = ANY(%s)
                  AND gap_start <= %s
                  AND recovered_at >= %s
                ORDER BY gap_start, recovered_at, asset_id
                """,
                (list(asset_ids), end, start),
            )
            gaps = tuple(dict(item) for item in cur.fetchall())
    return _build_transport_coverage_windows(
        condition_id=condition_id,
        market_id=market_id,
        asset_ids=asset_ids,
        start=start,
        end=end,
        source=table,
        rows=rows,
        gaps=gaps,
        active_active=active_active,
    )


def _build_transport_coverage_windows(
    *,
    condition_id: str,
    market_id: str,
    asset_ids: tuple[str, str],
    start: datetime,
    end: datetime,
    source: str,
    rows: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]] = (),
    active_active: bool,
) -> tuple[TransportCoverageWindow, ...]:
    interval_start = _utc(start)
    interval_end = _utc(end) + timedelta(microseconds=1)
    rows_by_hour_asset = {
        (_utc(row["hour_start"]), str(row["asset_id"])): row for row in rows
    }
    gap_ranges = _merged_gap_ranges(gaps, start=interval_start, end=interval_end)
    windows: list[TransportCoverageWindow] = []
    hour = interval_start.replace(minute=0, second=0, microsecond=0)
    final_hour = (interval_end - timedelta(microseconds=1)).replace(
        minute=0,
        second=0,
        microsecond=0,
    )
    while hour <= final_hour:
        segment_start = max(interval_start, hour)
        segment_end = min(interval_end, hour + timedelta(hours=1))
        hour_rows = [rows_by_hour_asset.get((hour, asset_id)) for asset_id in asset_ids]
        if any(row is None for row in hour_rows):
            windows.append(
                _transport_window(
                    condition_id=condition_id,
                    market_id=market_id,
                    asset_ids=asset_ids,
                    start=segment_start,
                    end=segment_end,
                    allowed=False,
                    state=TransportCoverageState.MISSING,
                    reason="token_hour_coverage_missing",
                    source=source,
                )
            )
            hour += timedelta(hours=1)
            continue
        present_rows = [row for row in hour_rows if row is not None]
        if not all(bool(row.get("outside_ready")) for row in present_rows):
            reasons = sorted(
                {
                    str(row.get("fill_depth_reason") or "coverage_not_ready")
                    for row in present_rows
                    if not bool(row.get("outside_ready"))
                }
            )
            windows.append(
                _transport_window(
                    condition_id=condition_id,
                    market_id=market_id,
                    asset_ids=asset_ids,
                    start=segment_start,
                    end=segment_end,
                    allowed=False,
                    state=_blocked_coverage_state(present_rows),
                    reason="+".join(reasons),
                    source=source,
                )
            )
            hour += timedelta(hours=1)
            continue
        hour_gaps = [
            (max(segment_start, gap_start), min(segment_end, gap_end))
            for gap_start, gap_end in gap_ranges
            if gap_start < segment_end and gap_end > segment_start
        ]
        expects_gap = any(int(row.get("dual_gap_count") or 0) > 0 for row in present_rows)
        if active_active and expects_gap and not hour_gaps:
            windows.append(
                _transport_window(
                    condition_id=condition_id,
                    market_id=market_id,
                    asset_ids=asset_ids,
                    start=segment_start,
                    end=segment_end,
                    allowed=False,
                    state=TransportCoverageState.COVERAGE_GAP,
                    reason="dual_gap_intervals_missing",
                    source=source,
                )
            )
            hour += timedelta(hours=1)
            continue
        cursor = segment_start
        ready_state = _ready_coverage_state(present_rows)
        for gap_start, gap_end in hour_gaps:
            if cursor < gap_start:
                windows.append(
                    _transport_window(
                        condition_id=condition_id,
                        market_id=market_id,
                        asset_ids=asset_ids,
                        start=cursor,
                        end=gap_start,
                        allowed=True,
                        state=ready_state,
                        reason="coverage_ready",
                        source=source,
                    )
                )
            windows.append(
                _transport_window(
                    condition_id=condition_id,
                    market_id=market_id,
                    asset_ids=asset_ids,
                    start=gap_start,
                    end=gap_end,
                    allowed=False,
                    state=TransportCoverageState.COVERAGE_GAP,
                    reason="dual_feed_gap_overlap",
                    source=source,
                )
            )
            cursor = max(cursor, gap_end)
        if cursor < segment_end:
            windows.append(
                _transport_window(
                    condition_id=condition_id,
                    market_id=market_id,
                    asset_ids=asset_ids,
                    start=cursor,
                    end=segment_end,
                    allowed=True,
                    state=ready_state,
                    reason="coverage_ready",
                    source=source,
                )
            )
        hour += timedelta(hours=1)
    return tuple(windows)


def _merged_gap_ranges(
    gaps: Sequence[Mapping[str, Any]],
    *,
    start: datetime,
    end: datetime,
) -> tuple[tuple[datetime, datetime], ...]:
    ranges = sorted(
        (
            max(start, _utc(row["gap_start"])),
            min(end, _utc(row["recovered_at"])),
        )
        for row in gaps
        if _utc(row["gap_start"]) < end and _utc(row["recovered_at"]) > start
    )
    merged: list[tuple[datetime, datetime]] = []
    for gap_start, gap_end in ranges:
        if gap_end <= gap_start:
            continue
        if merged and gap_start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], gap_end))
        else:
            merged.append((gap_start, gap_end))
    return tuple(merged)


def _ready_coverage_state(
    rows: Sequence[Mapping[str, Any]],
) -> TransportCoverageState:
    states = {str(row.get("continuity_state") or "").upper() for row in rows}
    if states == {TransportCoverageState.QUIET_BUT_COVERED.value}:
        return TransportCoverageState.QUIET_BUT_COVERED
    if TransportCoverageState.FRESH.value in states:
        return TransportCoverageState.FRESH
    return TransportCoverageState.COVERAGE_PROVEN


def _blocked_coverage_state(
    rows: Sequence[Mapping[str, Any]],
) -> TransportCoverageState:
    states = {str(row.get("continuity_state") or "").upper() for row in rows}
    if TransportCoverageState.TRANSPORT_BACKLOG.value in states:
        return TransportCoverageState.TRANSPORT_BACKLOG
    return TransportCoverageState.COVERAGE_GAP


def _transport_window(
    *,
    condition_id: str,
    market_id: str,
    asset_ids: tuple[str, str],
    start: datetime,
    end: datetime,
    allowed: bool,
    state: TransportCoverageState,
    reason: str,
    source: str,
) -> TransportCoverageWindow:
    payload = {
        "condition_id": condition_id,
        "market_id": market_id,
        "asset_ids": sorted(asset_ids),
        "start_ts": start,
        "end_ts": end,
        "allowed": allowed,
        "state": state,
        "reason": reason,
        "source": source,
    }
    return TransportCoverageWindow(
        proof_id="l2-coverage-" + canonical_hash(payload),
        condition_id=condition_id,
        market_id=market_id,
        start_ts=start,
        end_ts=end,
        allowed=allowed,
        state=state,
        reason=reason,
        asset_ids=asset_ids,
        source=source,
    )


def _xue_rows_to_pml2_events(
    rows: Any,
    *,
    condition_id: str,
    market_id: str,
    outcomes: dict[str, Outcome],
    book_epoch: int,
    feed_latency_ms: int,
    initial_books: Mapping[
        str,
        tuple[Mapping[Decimal, Decimal], Mapping[Decimal, Decimal]],
    ] | None = None,
    require_complete_top_hints: bool = False,
) -> tuple[
    BookSnapshotEvent | BookFrameBatchEvent | BookLevelBatchEvent | TradeEvent,
    ...,
]:
    if require_complete_top_hints:
        _validate_one_atomic_group_per_raw_frame(rows)
    events: list[
        BookSnapshotEvent | BookFrameBatchEvent | BookLevelBatchEvent | TradeEvent
    ] = []
    delta_groups: dict[
        _RawGroupKey,
        dict[str, list[BookDeltaEvent]],
    ] = {}
    delta_group_rows: dict[_RawGroupKey, dict[str, list[Any]]] = {}
    delta_group_context: dict[
        _RawGroupKey,
        tuple[datetime, datetime, str, str],
    ] = {}
    delta_group_outcomes: dict[tuple[_RawGroupKey, str], Outcome] = {}
    for _, row in rows.iterrows():
        asset_id = _archive_text(row.get("asset_id"))
        outcome = outcomes.get(asset_id)
        event_type = _archive_text(row.get("event_type")).lower()
        if outcome is None:
            if require_complete_top_hints:
                raise L2ReplayNotReady(
                    "native L2 global frame contains an unmapped asset: "
                    f"asset_id={asset_id}, event_type={event_type}"
                )
            continue
        received_at = _archive_datetime(row.get("timestamp_received"))
        exchange_at = _archive_datetime(row.get("timestamp"))
        if received_at is None or exchange_at is None:
            raise ValueError(
                "XUE archive event requires timestamp and timestamp_received"
            )
        if exchange_at > received_at:
            raise ValueError(
                "XUE archive event timestamp cannot exceed timestamp_received"
            )
        local_at = received_at + timedelta(milliseconds=max(0, feed_latency_ms))
        source = _archive_text(row.get("source")) or "xue_native_l2_archive"
        sequence = _archive_int(row.get("collector_seq"))
        source_ids = tuple(
            item
            for item in (
                _archive_text(row.get("payload_hash")),
                _archive_text(row.get("transaction_hash")),
            )
            if item
        )
        if event_type == "book":
            bids = _archive_levels(row.get("bids"))
            asks = _archive_levels(row.get("asks"))
            if not bids and not asks:
                continue
            snapshot_id = (
                _archive_text(row.get("book_hash"))
                or _archive_text(row.get("payload_hash"))
                or "xue-book-"
                + canonical_hash(
                    {
                        "asset_id": asset_id,
                        "exchange_at": exchange_at,
                        "received_at": received_at,
                        "sequence": sequence,
                        "bids": bids,
                        "asks": asks,
                    }
                )
            )
            events.append(
                BookSnapshotEvent(
                    snapshot_id=snapshot_id,
                    condition_id=condition_id,
                    market_id=market_id,
                    asset_id=asset_id,
                    outcome=outcome,
                    exchange_ts=exchange_at,
                    local_ts=local_at,
                    book_epoch=book_epoch,
                    bids=bids,
                    asks=asks,
                    source=source,
                    # collector_seq is connection-local in the XUE archive;
                    # load-aware connection migrations can interleave ranges.
                    sequence=0,
                    is_full_depth=True,
                    is_truncated=False,
                    depth_scope="FULL_XUE_NATIVE_ARCHIVE",
                    book_hash=_archive_text(row.get("book_hash")) or snapshot_id,
                    source_received_ts=received_at,
                )
            )
            continue
        if event_type == "price_change":
            group_id = (
                _archive_text(row.get("group_id"))
                or _archive_text(row.get("raw_frame_seq"))
                or str(sequence)
            )
            raw_connection_id = _archive_text(row.get("raw_connection_id"))
            raw_generation = _archive_text(
                row.get("raw_connection_generation")
            )
            raw_frame_seq = _archive_text(row.get("raw_frame_seq"))
            if not raw_connection_id or not raw_frame_seq:
                legacy_identity = f"legacy-{sequence}"
                raw_connection_id = raw_connection_id or legacy_identity
                raw_frame_seq = raw_frame_seq or legacy_identity
            batch_key = (
                source,
                raw_connection_id,
                raw_generation,
                raw_frame_seq,
                group_id,
            )
            context = (exchange_at, received_at, source, group_id)
            existing_context = delta_group_context.get(batch_key)
            if existing_context is not None and existing_context != context:
                raise L2ReplayNotReady(
                    "native L2 atomic group has inconsistent event clocks: "
                    f"group_id={group_id}"
                )
            delta_group_context[batch_key] = context
            delta_group_outcomes[(batch_key, asset_id)] = outcome
            delta_group_rows.setdefault(batch_key, {}).setdefault(
                asset_id,
                [],
            ).append(row)
            raw_side = _archive_text(row.get("side")).upper()
            side = (
                EconomicBookSide.BID
                if raw_side in {"BUY", "BID", "BIDS"}
                else EconomicBookSide.ASK
                if raw_side in {"SELL", "ASK", "ASKS"}
                else None
            )
            raw_price = _archive_decimal(row.get("price"))
            raw_size = _archive_decimal(row.get("size"))
            if side is None or raw_price is None or raw_size is None:
                continue
            update_index = (
                _archive_int(row.get("change_index"))
                if not _archive_missing(row.get("change_index"))
                else _archive_int(row.get("sequence_in_message"))
            )
            update_id = "xue-delta-" + canonical_hash(
                {
                    "asset_id": asset_id,
                    "exchange_at": exchange_at,
                    "received_at": received_at,
                    "sequence": sequence,
                    "update_index": update_index,
                    "raw_connection_id": raw_connection_id,
                    "raw_connection_generation": raw_generation,
                    "raw_frame_seq": raw_frame_seq,
                    "group_id": group_id,
                    "side": side,
                    "price": raw_price,
                    "size": raw_size,
                    "source_ids": source_ids,
                }
            )
            delta_groups.setdefault(batch_key, {}).setdefault(
                asset_id,
                [],
            ).append(
                BookDeltaEvent(
                    event_id=update_id,
                    condition_id=condition_id,
                    market_id=market_id,
                    asset_id=asset_id,
                    outcome=outcome,
                    exchange_ts=exchange_at,
                    local_ts=local_at,
                    book_epoch=book_epoch,
                    side=side,
                    price=raw_price,
                    new_size=max(Decimal(0), raw_size),
                    source=source,
                    sequence=update_index,
                    linked_trade_event_ids=(),
                    source_received_ts=received_at,
                )
            )
            continue
        if event_type != "last_trade_price":
            continue
        raw_price = _archive_decimal(row.get("price"))
        raw_size = _archive_decimal(row.get("size"))
        raw_side_text = _archive_text(row.get("side")).upper()
        if (
            raw_price is None
            or raw_size is None
            or raw_size <= 0
            or raw_side_text not in {"BUY", "SELL"}
        ):
            continue
        raw_side = RawOrderSide(raw_side_text)
        transaction_hash = _archive_text(row.get("transaction_hash"))
        canonical_price = raw_price if outcome == Outcome.YES else Decimal(1) - raw_price
        canonical_side = (
            raw_side
            if outcome == Outcome.YES
            else RawOrderSide.SELL
            if raw_side == RawOrderSide.BUY
            else RawOrderSide.BUY
        )
        evidence_link_id = ""
        if transaction_hash:
            evidence_link_id = ":".join(
                (
                    transaction_hash,
                    format(canonical_price, "f"),
                    canonical_side.value,
                    format(raw_size, "f"),
                )
            )
        event_id = "xue-trade-" + canonical_hash(
            {
                "asset_id": asset_id,
                "exchange_at": exchange_at,
                "received_at": received_at,
                "sequence": sequence,
                "price": raw_price,
                "size": raw_size,
                "side": raw_side,
                "transaction_hash": transaction_hash,
            }
        )
        events.append(
            TradeEvent(
                event_id=event_id,
                condition_id=condition_id,
                market_id=market_id,
                asset_id=asset_id,
                outcome=outcome,
                exchange_ts=exchange_at,
                local_ts=local_at,
                book_epoch=book_epoch,
                price=raw_price,
                size=raw_size,
                aggressor_side=raw_side,
                source=source,
                source_sequence=0,
                event_group_id=transaction_hash,
                evidence_link_id=evidence_link_id,
                evidence_kind="XUE_NATIVE_L2_TRADE_PRINT",
                source_event_ids=source_ids,
                source_received_ts=received_at,
            )
        )
    frame_rows_by_event_id: dict[str, dict[str, tuple[Any, ...]]] = {}
    legacy_rows_by_event_id: dict[str, tuple[Any, ...]] = {}
    ordered_group_keys = sorted(
        delta_group_rows,
        key=lambda key: (
            delta_group_context[key][1],
            delta_group_context[key][0],
            key,
        ),
    )
    for frame_order, frame_key in enumerate(ordered_group_keys, start=1):
        source, raw_connection_id, raw_generation, raw_frame_seq, group_id = frame_key
        exchange_at, received_at, _, _ = delta_group_context[frame_key]
        leg_batches: list[BookLevelBatchEvent] = []
        legacy_batches: list[BookLevelBatchEvent] = []
        all_legs_have_top = True
        update_ids_by_asset: dict[str, list[str]] = {}
        for asset_id, raw_rows in sorted(delta_group_rows[frame_key].items()):
            updates = delta_groups.get(frame_key, {}).get(asset_id, [])
            if not updates:
                if any(
                    not _archive_missing(row.get("best_bid"))
                    or not _archive_missing(row.get("best_ask"))
                    for row in raw_rows
                ):
                    raise L2ReplayNotReady(
                        "native L2 fenced atomic group has no valid price_change: "
                        f"asset_id={asset_id}, group_id={group_id}"
                    )
                continue
            updates.sort(key=lambda item: (item.sequence or 0, item.event_id))
            update_ids_by_asset[asset_id] = [item.event_id for item in updates]
            raw_best_bid, raw_best_ask = _archive_group_top_pair(
                raw_rows,
                asset_id=asset_id,
                group_id=group_id,
                required=require_complete_top_hints,
            )
            all_legs_have_top = all_legs_have_top and raw_best_bid is not None
            first = updates[0]
            leg_batch = BookLevelBatchEvent(
                event_id="xue-raw-leg-"
                + canonical_hash(
                    {
                        "raw_connection_id": raw_connection_id,
                        "raw_connection_generation": raw_generation,
                        "raw_frame_seq": raw_frame_seq,
                        "group_id": group_id,
                        "asset_id": asset_id,
                        "updates": update_ids_by_asset[asset_id],
                    }
                ),
                condition_id=condition_id,
                market_id=market_id,
                asset_id=asset_id,
                outcome=delta_group_outcomes[(frame_key, asset_id)],
                exchange_ts=exchange_at,
                local_ts=first.local_ts,
                book_epoch=book_epoch,
                updates=tuple(updates),
                source=source,
                source_sequence=0,
                source_batch_sequence=0,
                source_received_ts=received_at,
                authoritative_best_bid=raw_best_bid,
                authoritative_best_ask=raw_best_ask,
            )
            legacy_rows_by_event_id[leg_batch.event_id] = tuple(raw_rows)
            leg_batches.append(leg_batch)
            legacy_batches.append(leg_batch)
        if not leg_batches:
            continue
        group_event_id = "xue-raw-group-" + canonical_hash(
            {
                "exchange_at": exchange_at,
                "received_at": received_at,
                "raw_connection_id": raw_connection_id,
                "raw_connection_generation": raw_generation,
                "raw_frame_seq": raw_frame_seq,
                "group_id": group_id,
                "updates_by_asset": update_ids_by_asset,
            }
        )
        frame_rows_by_event_id[group_event_id] = {
            asset_id: tuple(raw_rows)
            for asset_id, raw_rows in delta_group_rows[frame_key].items()
        }
        if all_legs_have_top:
            events.append(
                BookFrameBatchEvent(
                    event_id=group_event_id,
                    condition_id=condition_id,
                    market_id=market_id,
                    exchange_ts=exchange_at,
                    source_received_ts=received_at,
                    local_ts=received_at
                    + timedelta(milliseconds=max(0, feed_latency_ms)),
                    book_epoch=book_epoch,
                    batches=tuple(leg_batches),
                    source=source,
                    source_sequence=0,
                    source_batch_sequence=frame_order,
                )
            )
        else:
            events.extend(legacy_batches)
    mutable_books: dict[
        str,
        tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]],
    ] = {
        asset_id: (dict(book[0]), dict(book[1]))
        for asset_id, book in (initial_books or {}).items()
    }
    reconciled: list[
        BookSnapshotEvent | BookFrameBatchEvent | BookLevelBatchEvent | TradeEvent
    ] = []
    for event in sorted(events, key=_pml2_archive_event_sort_key):
        if isinstance(event, BookSnapshotEvent):
            mutable_books[event.asset_id] = (
                {level.price: level.size for level in event.bids},
                {level.price: level.size for level in event.asks},
            )
            reconciled.append(event)
            continue
        if isinstance(event, BookFrameBatchEvent):
            staged_legs: dict[
                str,
                tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]],
            ] = {}
            for batch in event.batches:
                current_bids, current_asks = mutable_books.setdefault(
                    batch.asset_id,
                    ({}, {}),
                )
                staged_bids = dict(current_bids)
                staged_asks = dict(current_asks)
                apply_price_change_group_with_top_fence(
                    staged_bids,
                    staged_asks,
                    frame_rows_by_event_id[event.event_id][batch.asset_id],
                    asset_id=batch.asset_id,
                    require_complete_top_hints=require_complete_top_hints,
                )
                staged_legs[batch.asset_id] = (staged_bids, staged_asks)
            mutable_books.update(staged_legs)
            final_frame_id = (
                f"xue-frame-batch-{event.source_batch_sequence:012d}-"
                + canonical_hash(
                    {
                        "raw_group_event_id": event.event_id,
                        "legs": [batch.event_id for batch in event.batches],
                    }
                )
            )
            reconciled.append(
                BookFrameBatchEvent(
                    event_id=final_frame_id,
                    condition_id=event.condition_id,
                    market_id=event.market_id,
                    exchange_ts=event.exchange_ts,
                    source_received_ts=event.source_received_ts,
                    local_ts=event.local_ts,
                    book_epoch=event.book_epoch,
                    batches=event.batches,
                    source=event.source,
                    source_sequence=0,
                    source_batch_sequence=event.source_batch_sequence,
                )
            )
            continue
        if not isinstance(event, BookLevelBatchEvent):
            reconciled.append(event)
            continue
        state_bids, state_asks = mutable_books.setdefault(
            event.asset_id,
            ({}, {}),
        )
        raw_group_rows = legacy_rows_by_event_id[event.event_id]
        apply_price_change_group_with_top_fence(
            state_bids,
            state_asks,
            raw_group_rows,
            asset_id=event.asset_id,
            require_complete_top_hints=require_complete_top_hints,
        )
        reconciled.append(event)
    return tuple(reconciled)


def _validate_one_atomic_group_per_raw_frame(rows: Any) -> None:
    """Keep the global-frame envelope honest on the formal XUE path.

    PML2 currently models one atomic upstream message group per raw websocket
    frame.  The selected PMQ-071 packet satisfies that invariant.  A future
    archive containing multiple group ids in one frame must fail closed until
    the contract can represent their ordering without publishing partial state.
    """

    groups_by_frame: dict[tuple[str, str, str, str], set[str]] = {}
    for _, row in rows.iterrows():
        source = _archive_text(row.get("source"))
        connection_id = _archive_text(row.get("raw_connection_id"))
        generation = _archive_text(row.get("raw_connection_generation"))
        frame_sequence = _archive_text(row.get("raw_frame_seq"))
        group_id = _archive_text(row.get("group_id"))
        if not all((source, connection_id, generation, frame_sequence, group_id)):
            raise L2ReplayNotReady(
                "formal native L2 frame requires source, connection, generation, "
                "frame sequence, and group id"
            )
        groups_by_frame.setdefault(
            (source, connection_id, generation, frame_sequence),
            set(),
        ).add(group_id)
    for frame, group_ids in groups_by_frame.items():
        if len(group_ids) > 1:
            raise L2ReplayNotReady(
                "formal native L2 frame contains multiple atomic group ids: "
                f"frame={frame}, group_ids={sorted(group_ids)}"
            )


def _pml2_archive_event_sort_key(
    event: BookSnapshotEvent | BookFrameBatchEvent | BookLevelBatchEvent | TradeEvent,
) -> tuple[datetime, int, datetime, int, int, str]:
    kind_order = (
        0
        if isinstance(event, BookSnapshotEvent)
        else 1
        if isinstance(event, (BookFrameBatchEvent, BookLevelBatchEvent))
        else 2
    )
    sequence = (
        event.sequence
        if isinstance(event, BookSnapshotEvent)
        else event.source_sequence
    )
    event_id = (
        event.snapshot_id
        if isinstance(event, BookSnapshotEvent)
        else event.event_id
    )
    batch_sequence = (
        event.source_batch_sequence
        if isinstance(event, (BookFrameBatchEvent, BookLevelBatchEvent))
        else 0
    )
    received_at = event.source_received_ts or event.local_ts
    return (
        received_at,
        kind_order,
        event.exchange_ts,
        int(sequence or 0),
        int(batch_sequence),
        event_id,
    )


def _archive_group_top_pair(
    rows: Sequence[Any],
    *,
    asset_id: str,
    group_id: str,
    required: bool,
) -> tuple[Decimal | None, Decimal | None]:
    pairs: set[tuple[Decimal, Decimal]] = set()
    for row in rows:
        bid_missing = _archive_missing(row.get("best_bid"))
        ask_missing = _archive_missing(row.get("best_ask"))
        if bid_missing != ask_missing:
            raise L2ReplayNotReady(
                "native L2 raw frame has a partial top pair: "
                f"asset_id={asset_id}, group_id={group_id}"
            )
        if bid_missing:
            continue
        raw_bid = _archive_decimal(row.get("best_bid"))
        raw_ask = _archive_decimal(row.get("best_ask"))
        if (
            raw_bid is None
            or raw_ask is None
            or raw_bid <= 0
            or raw_ask <= 0
            or raw_bid >= raw_ask
        ):
            raise L2ReplayNotReady(
                "native L2 raw frame has an invalid top pair: "
                f"asset_id={asset_id}, group_id={group_id}"
            )
        pairs.add((raw_bid, raw_ask))
    if len(pairs) > 1:
        raise L2ReplayNotReady(
            "native L2 raw frame top pairs disagree: "
            f"asset_id={asset_id}, group_id={group_id}"
        )
    if not pairs:
        if required:
            raise L2ReplayNotReady(
                "native L2 strict frame requires a complete top pair: "
                f"asset_id={asset_id}, group_id={group_id}"
            )
        return None, None
    return next(iter(pairs))


def _archive_text(value: Any) -> str:
    if _archive_missing(value):
        return ""
    return str(value).strip()


def _archive_int(value: Any) -> int:
    if _archive_missing(value):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _archive_decimal(value: Any) -> Decimal | None:
    if _archive_missing(value):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _archive_datetime(value: Any) -> datetime | None:
    if _archive_missing(value):
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _archive_levels(value: Any) -> tuple[BookLevel, ...]:
    if _archive_missing(value):
        return ()
    try:
        decoded = json.loads(str(value)) if isinstance(value, str) else value
    except (TypeError, ValueError):
        return ()
    if not isinstance(decoded, list):
        return ()
    levels: list[BookLevel] = []
    for row in decoded:
        if isinstance(row, dict):
            raw_price, raw_size = row.get("price"), row.get("size")
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            raw_price, raw_size = row[0], row[1]
        else:
            continue
        parsed_price = _archive_decimal(raw_price)
        parsed_size = _archive_decimal(raw_size)
        if parsed_price is None or parsed_size is None or parsed_size <= 0:
            continue
        levels.append(BookLevel(parsed_price, parsed_size))
    return tuple(levels)


def _archive_missing(value: Any) -> bool:
    if value is None:
        return True
    if type(value).__name__ in {"NAType", "NaTType"}:
        return True
    try:
        missing = value != value  # noqa: PLR0124 - NaN is the only self-unequal scalar.
    except (TypeError, ValueError):
        return False
    return bool(missing) if isinstance(missing, bool) else False


def _archive_bool(value: Any) -> bool:
    if _archive_missing(value):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "t", "yes"}


def _validate_archive_frame_evidence(rows: Any) -> bool:
    """Require complete raw-frame evidence without token-row terminal bias."""

    if rows.empty:
        return True
    required = {
        "raw_connection_id",
        "group_id",
        "raw_frame_seq",
        "frame_raw_complete",
        "group_has_terminal",
        "is_last_in_group",
        "raw_frame_complete",
    }
    missing = sorted(required - set(rows.columns))
    if missing:
        raise L2ReplayNotReady(
            f"native L2 frame evidence columns missing: {missing}"
        )
    for _, row in rows.iterrows():
        raw_connection_id = _archive_text(row.get("raw_connection_id"))
        group_id = _archive_text(row.get("group_id"))
        if (
            not raw_connection_id
            or not group_id
            or _archive_missing(row.get("raw_frame_seq"))
        ):
            raise L2ReplayNotReady(
                "native L2 frame evidence requires raw_connection_id, "
                "group_id, and raw_frame_seq"
            )
        if not _archive_bool(row.get("frame_raw_complete")):
            raise L2ReplayNotReady(
                f"native L2 raw frame is incomplete: group_id={group_id}"
            )
        if not _archive_bool(row.get("group_has_terminal")):
            raise L2ReplayNotReady(
                f"native L2 atomic group is incomplete: group_id={group_id}"
            )
        # The selected token projection need not itself carry either terminal
        # bit. The aggregate evidence above is computed before asset filtering.
    return True


def _validate_events_follow_baselines(
    events: tuple[
        BookSnapshotEvent | BookFrameBatchEvent | BookLevelBatchEvent | TradeEvent,
        ...,
    ],
    *,
    baseline_exchange_by_outcome: dict[Outcome, datetime],
    baseline_local_by_outcome: dict[Outcome, datetime],
) -> None:
    for event in events:
        legs = event.batches if isinstance(event, BookFrameBatchEvent) else (event,)
        for leg in legs:
            baseline_exchange = baseline_exchange_by_outcome.get(leg.outcome)
            baseline_local = baseline_local_by_outcome.get(leg.outcome)
            if baseline_exchange is None or baseline_local is None:
                raise ValueError(
                    f"missing verified baseline for outcome={leg.outcome.value}"
                )
            # Native exchange timestamps can legitimately regress between two
            # frames received in order.  The checkpoint boundary is therefore
            # enforced on the receive/local clock; the raw exchange clock is
            # retained as evidence and must not be rewritten or sorted first.
            if leg.local_ts <= baseline_local:
                raise ValueError(
                    "archive event receive clock does not follow reconstructed "
                    f"baseline for outcome={leg.outcome.value}"
                )


def _checkpoint_event(
    checkpoint: L2StateCheckpoint,
    *,
    condition_id: str,
    market_id: str,
    outcome: Outcome,
    book_epoch: int,
    feed_latency_ms: int,
) -> BookSnapshotEvent:
    clock_verified = _checkpoint_clock_verified(checkpoint)
    exchange_ts = (
        checkpoint.latest_book_exchange_ts
        if clock_verified
        else checkpoint.timestamp
    )
    received_at = (
        checkpoint.latest_book_received_at
        if clock_verified
        else checkpoint.timestamp
    )
    assert exchange_ts is not None
    assert received_at is not None
    return BookSnapshotEvent(
        snapshot_id=checkpoint.snapshot_version,
        condition_id=condition_id,
        market_id=market_id,
        asset_id=checkpoint.asset_id,
        outcome=outcome,
        exchange_ts=exchange_ts,
        local_ts=received_at
        + timedelta(milliseconds=max(0, feed_latency_ms)),
        book_epoch=book_epoch,
        bids=tuple(BookLevel(price, size) for price, size in checkpoint.bids),
        asks=tuple(BookLevel(price, size) for price, size in checkpoint.asks),
        source=checkpoint.latest_source or "xue_native_l2_archive",
        sequence=0,
        is_full_depth=True,
        is_truncated=False,
        depth_scope="FULL_ARCHIVE_RECONSTRUCTION",
        tick_size=checkpoint.tick_size,
        book_hash=checkpoint.snapshot_version,
        source_received_ts=received_at,
    )


def _state_checkpoint_from_replay(
    replay: L2ReplaySnapshot,
) -> L2StateCheckpoint:
    checkpoint = replay.checkpoint
    return L2StateCheckpoint(
        asset_id=checkpoint.asset_id,
        timestamp=checkpoint.timestamp,
        latest_received_at=checkpoint.latest_received_at,
        latest_source=checkpoint.latest_source,
        market_id=replay.snapshot.market_id,
        bids=tuple((item.price, item.size) for item in replay.snapshot.bids),
        asks=tuple((item.price, item.size) for item in replay.snapshot.asks),
        row_count=checkpoint.row_count,
        generation=checkpoint.generation,
        tick_size=checkpoint.tick_size,
        has_ws_book=checkpoint.has_ws_book,
        has_rest_seed_book=checkpoint.has_rest_seed_book,
        has_price_change=checkpoint.has_price_change,
        latest_book_is_rest=checkpoint.latest_book_is_rest,
        price_changes_after_latest_book=checkpoint.price_changes_after_latest_book,
        snapshot_version=checkpoint.snapshot_version,
        source_files=checkpoint.source_files,
        latest_book_exchange_ts=checkpoint.latest_book_exchange_ts,
        latest_book_received_at=checkpoint.latest_book_received_at,
    )


def _checkpoint_clock_verified(checkpoint: L2StateCheckpoint) -> bool:
    exchange_ts = checkpoint.latest_book_exchange_ts
    received_at = checkpoint.latest_book_received_at
    if exchange_ts is None or received_at is None:
        return False
    if exchange_ts > received_at:
        raise ValueError(
            "checkpoint latest book exchange timestamp cannot exceed received timestamp"
        )
    if received_at > checkpoint.timestamp:
        raise ValueError(
            "checkpoint latest book received timestamp cannot exceed PIT cutoff"
        )
    return True


def pml2_snapshot_from_any(
    value: Any,
    *,
    condition_id: str,
    market_id: str,
    outcome: Outcome,
    book_epoch: int,
) -> BookSnapshotEvent:
    if isinstance(value, BookSnapshotEvent):
        return value
    if isinstance(value, (LegacyBookSnapshot, TimelineBookSnapshot)):
        return legacy_snapshot_to_pml2(
            value,
            condition_id=condition_id,
            market_id=market_id,
            outcome=outcome,
            book_epoch=book_epoch,
        )
    raise TypeError(f"unsupported L2 snapshot type: {type(value).__name__}")


def _level(value: Any) -> BookLevel:
    if isinstance(value, tuple):
        return BookLevel(value[0], value[1])
    return BookLevel(value.price, value.size)


def _source_manifest_hash(files: set[str]) -> str:
    """Hash the authoritative XUE file inventory, not every remote byte."""

    manifest: list[dict[str, object]] = []
    for raw_path in sorted(files):
        path = Path(raw_path)
        stat = path.stat()
        manifest.append(
            {
                "path": str(path),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
    return canonical_hash(manifest)
