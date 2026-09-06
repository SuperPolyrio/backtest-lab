"""Compressed append-only L2 archive writer for Polymarket market WS events."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    import orjson as _orjson
except ImportError:  # Preserve the minimal local/test environment.
    _orjson = None


ARCHIVE_SOURCE = "polymarket_market_ws_archive"
ARCHIVE_SCHEMA_VERSION = "polymarket-l2-archive-v2-provenance"
DEFAULT_SORT_KEY = ("market", "asset_id", "timestamp_received", "collector_seq", "sequence_in_message")
_PLAIN_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")


CREATE_ARCHIVE_MANIFEST_SQL = """
CREATE TABLE IF NOT EXISTS quant.clob_l2_archive_manifest (
    manifest_id BIGSERIAL PRIMARY KEY,
    archive_hour TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL DEFAULT 'polymarket_market_ws_archive',
    ws_url TEXT,
    shard_id INTEGER,
    shard_count INTEGER,
    path TEXT NOT NULL UNIQUE,
    format TEXT NOT NULL DEFAULT 'parquet',
    compression TEXT NOT NULL DEFAULT 'zstd',
    compression_level INTEGER NOT NULL DEFAULT 9,
    row_group_size BIGINT NOT NULL DEFAULT 1048576,
    sort_key JSONB NOT NULL DEFAULT '[]'::jsonb,
    schema_version TEXT NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    asset_count BIGINT NOT NULL DEFAULT 0,
    market_count BIGINT NOT NULL DEFAULT 0,
    first_event_ts TIMESTAMPTZ,
    last_event_ts TIMESTAMPTZ,
    first_received_at TIMESTAMPTZ,
    last_received_at TIMESTAMPTZ,
    file_size_bytes BIGINT NOT NULL DEFAULT 0,
    sha256 TEXT,
    connection_generation BIGINT,
    event_type_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_local_seq BIGINT,
    last_local_seq BIGINT,
    writer_version TEXT NOT NULL DEFAULT 'l2_archive_v2_integrity',
    status TEXT NOT NULL DEFAULT 'ready',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    written_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    committed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

ARCHIVE_MANIFEST_INDEX_SQL: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_archive_hour ON quant.clob_l2_archive_manifest (archive_hour DESC, shard_id)",
    "CREATE INDEX IF NOT EXISTS idx_quant_clob_l2_archive_status ON quant.clob_l2_archive_manifest (status, updated_at DESC)",
)

ARCHIVE_COLUMNS: tuple[str, ...] = (
    "timestamp_received",
    "timestamp_normalized",
    "timestamp",
    "raw_event_timestamp",
    "clock_invalid_reason",
    "rest_request_start_ns",
    "rest_response_end_ns",
    "market",
    "event_type",
    "asset_id",
    "collector_seq",
    "sequence_in_message",
    "bids",
    "asks",
    "price",
    "size",
    "side",
    "best_bid",
    "best_ask",
    "fee_rate_bps",
    "transaction_hash",
    "old_tick_size",
    "new_tick_size",
    "book_hash",
    "payload_hash",
    "source",
    "ws_url",
    "shard_id",
    "shard_count",
    "raw_connection_id",
    "raw_connection_generation",
    "raw_frame_seq",
    "raw_received_wall_ns",
    "message_index",
    "change_index",
    "group_id",
    "is_last_in_group",
    "raw_frame_complete",
)


@dataclass(frozen=True)
class L2ArchiveFile:
    path: Path
    archive_hour: datetime
    event_count: int
    asset_count: int
    market_count: int
    first_event_ts: datetime | None
    last_event_ts: datetime | None
    first_received_at: datetime | None
    last_received_at: datetime | None
    file_size_bytes: int
    sha256: str | None
    event_type_counts: dict[str, int] = field(default_factory=dict)
    first_local_seq: int | None = None
    last_local_seq: int | None = None
    raw_frame_watermarks: dict[int, int] = field(default_factory=dict)
    raw_frame_ranges: dict[int, tuple[tuple[int, int], ...]] = field(
        default_factory=dict
    )


@dataclass
class CompressedL2ArchiveWriter:
    """Buffer WS L2 events and write sorted ZSTD Parquet chunks.

    This is the long-term historical truth layer. The database only receives a
    manifest row per written file; current book state stays in the existing
    BookState tables.
    """

    conn: Any | None = None
    base_dir: Path | str = Path("runtime_outputs/lob_l2_archive")
    source: str = ARCHIVE_SOURCE
    ws_url: str | None = None
    shard_id: int | None = None
    shard_count: int | None = None
    compression: str = "zstd"
    compression_level: int = 9
    row_group_size: int = 1_048_576
    flush_rows: int = 50_000
    flush_bytes: int = 0
    flush_seconds: float = 60.0
    parquet_threads: int = 4
    parquet_timeout_seconds: float = 180.0
    write_sidecar: bool = False
    compact_provenance: bool = False
    parquet_lock_path: Path | str | None = None
    parquet_lock_slots: int = 1
    _rows: list[dict[str, Any]] = field(default_factory=list, init=False, repr=False)
    _buffer_bytes: int = field(default=0, init=False, repr=False)
    _rows_lock: Any = field(default_factory=threading.Lock, init=False, repr=False)
    _flush_lock: Any = field(default_factory=threading.Lock, init=False, repr=False)
    _collector_seq: int = field(default=0, init=False, repr=False)
    _last_flush_monotonic: float = field(default_factory=time.monotonic, init=False, repr=False)
    files_written: int = 0
    rows_written: int = 0

    def __post_init__(self) -> None:
        self.base_dir = Path(self.base_dir)
        if self.parquet_lock_path is not None:
            self.parquet_lock_path = Path(self.parquet_lock_path)
            self.parquet_lock_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )
        self.parquet_lock_slots = max(1, int(self.parquet_lock_slots))
        self.compression_level = int(self.compression_level)
        self.row_group_size = int(self.row_group_size)
        self.flush_rows = max(1, int(self.flush_rows))
        self.flush_bytes = max(0, int(self.flush_bytes))
        self.flush_seconds = max(1.0, float(self.flush_seconds))
        self.parquet_threads = max(1, int(os.environ.get("BOOK_L2_PARQUET_THREADS", self.parquet_threads)))
        self.parquet_timeout_seconds = max(
            30.0,
            float(
                os.environ.get(
                    "BOOK_L2_PARQUET_WRITE_TIMEOUT_SECONDS",
                    self.parquet_timeout_seconds,
                )
            ),
        )

    def ensure_schema(self) -> None:
        if self.conn is None:
            return
        with self.conn.cursor() as cur:
            cur.execute(CREATE_ARCHIVE_MANIFEST_SQL)
            for statement in ARCHIVE_MANIFEST_INDEX_SQL:
                cur.execute(statement)

    def insert_manifest_files(self, files: Sequence[L2ArchiveFile], *, ensure_schema: bool = True) -> int:
        if self.conn is None or not files:
            return 0
        if ensure_schema:
            self.ensure_schema()
        for item in files:
            self._insert_manifest(item)
        return len(files)

    def insert_messages(self, messages: Sequence[Mapping[str, Any]], *, received_ts_ms: int | None = None) -> int:
        received_at = _ms_to_datetime(received_ts_ms or int(time.time() * 1000))
        accepted = 0
        with self._rows_lock:
            for message in messages:
                for row in self._rows_for_message(message, received_at=received_at):
                    self._collector_seq += 1
                    row["collector_seq"] = self._collector_seq
                    self._rows.append(row)
                    self._buffer_bytes += _estimated_row_bytes(row)
                    accepted += 1
        return accepted

    def should_flush(self) -> bool:
        with self._rows_lock:
            return self._should_flush_locked()

    @property
    def pending_spool_bytes(self) -> int:
        """Bytes accepted after the currently running flush was sealed."""

        with self._rows_lock:
            return self._spool_bytes

    def stagger_initial_flush(self, phase_seconds: float) -> None:
        """Offset only the first time-based flush to avoid I/O stampedes."""

        phase = max(
            0.0,
            min(
                float(phase_seconds),
                max(0.0, self.flush_seconds - 0.001),
            ),
        )
        with self._rows_lock:
            self._last_flush_monotonic -= phase

    def flush(self, *, force: bool = False) -> list[L2ArchiveFile]:
        with self._flush_lock:
            written: list[L2ArchiveFile] = []
            while True:
                with self._rows_lock:
                    if not self._rows:
                        break
                    if not force and not self._should_flush_locked():
                        break
                    rows = self._take_flush_chunk_locked(force=force)
                groups = list(_group_by_archive_hour(rows).items())
                for index, (archive_hour, hour_rows) in enumerate(groups):
                    try:
                        written_file = self._write_hour_rows(archive_hour, hour_rows)
                        written.append(written_file)
                        self.files_written += 1
                        self.rows_written += written_file.event_count
                        self._insert_manifest(written_file)
                    except Exception:
                        remaining = [
                            row
                            for _remaining_hour, remaining_rows in groups[index:]
                            for row in remaining_rows
                        ]
                        with self._rows_lock:
                            self._rows = remaining + self._rows
                            self._buffer_bytes += sum(_estimated_row_bytes(row) for row in remaining)
                        raise
                with self._rows_lock:
                    self._last_flush_monotonic = time.monotonic()
                if not force:
                    break
            return written

    def _should_flush_locked(self) -> bool:
        if not self._rows:
            return False
        return (
            len(self._rows) >= self.flush_rows
            or (self.flush_bytes > 0 and self._buffer_bytes >= self.flush_bytes)
            or time.monotonic() - self._last_flush_monotonic >= self.flush_seconds
        )

    def _take_flush_chunk_locked(self, *, force: bool) -> list[dict[str, Any]]:
        selected_count = 0
        selected_bytes = 0
        for row in self._rows:
            row_bytes = _estimated_row_bytes(row)
            if selected_count and (
                (not force and selected_count >= self.flush_rows)
                or (self.flush_bytes > 0 and selected_bytes + row_bytes > self.flush_bytes)
            ):
                break
            selected_count += 1
            selected_bytes += row_bytes
        if selected_count and selected_count < len(self._rows):
            group_id = self._rows[selected_count - 1].get("group_id")
            if group_id:
                while (
                    selected_count < len(self._rows)
                    and self._rows[selected_count].get("group_id") == group_id
                ):
                    selected_bytes += _estimated_row_bytes(
                        self._rows[selected_count]
                    )
                    selected_count += 1
        rows = self._rows[:selected_count]
        self._rows = self._rows[selected_count:]
        self._buffer_bytes = max(0, self._buffer_bytes - selected_bytes)
        return rows

    def close(self) -> list[L2ArchiveFile]:
        return self.flush(force=True)

    def _rows_for_message(self, message: Mapping[str, Any], *, received_at: datetime) -> list[dict[str, Any]]:
        event = message
        raw_received_wall_ns = _int_or_none(
            event.get("_raw_received_wall_ns")
        )
        if raw_received_wall_ns is not None:
            received_at = datetime.fromtimestamp(
                raw_received_wall_ns / 1_000_000_000,
                tz=timezone.utc,
            )
        normalized_at = datetime.now(timezone.utc)
        event_type = str(event.get("event_type") or "").strip()
        if event_type not in {
            "book",
            "price_change",
            "last_trade_price",
            "tick_size_change",
            "best_bid_ask",
            "connection_gap",
            "connection_heartbeat",
            "connection_recovered",
        }:
            return []
        event_ts, raw_event_timestamp, clock_invalid_reason = (
            _event_clock(
                event.get("timestamp"),
                received_at=received_at,
                preserve_raw=bool(
                    event.get("_preserve_raw_event_timestamp")
                ),
            )
        )
        market = _text(event.get("market") or event.get("condition_id"))
        # The durable raw WAL already preserves the complete upstream frame.
        # For relay-oriented Silver, repeating two high-entropy hashes on
        # every flattened price delta dominates Parquet size without changing
        # replay state or the PMXT comparison key.  Keep hashes on snapshots
        # and lifecycle markers, where they are integrity/recovery evidence.
        retain_provenance_hashes = (
            not self.compact_provenance
            or event_type
            in {
                "book",
                "connection_gap",
                "connection_recovered",
            }
        )
        payload_hash = (
            _payload_hash(event) if retain_provenance_hashes else None
        )

        row_source = _text(event.get("source")) or self.source
        row_shard_id = _int_or_none(event.get("_archive_shard_id"))
        raw_fields = {
            "raw_connection_id": _text(event.get("_raw_connection_id")),
            "raw_connection_generation": _int_or_none(
                event.get("_raw_connection_generation")
            ),
            "raw_frame_seq": _int_or_none(event.get("_raw_frame_seq")),
            "raw_received_wall_ns": _int_or_none(
                event.get("_raw_received_wall_ns")
            ),
            "raw_event_timestamp": raw_event_timestamp,
            "clock_invalid_reason": clock_invalid_reason,
            "rest_request_start_ns": _int_or_none(
                event.get("_rest_request_start_ns")
            ),
            "rest_response_end_ns": _int_or_none(
                event.get("_rest_response_end_ns")
            ),
            "message_index": _int_or_none(event.get("_raw_message_index")),
            "group_id": _text(event.get("_raw_group_id")),
        }
        raw_frame_last_message = bool(
            event.get("_raw_is_last_archivable_message")
        )
        raw_group_last_message = bool(
            event.get("_raw_is_last_group_message", True)
        )

        if event_type == "price_change" and isinstance(event.get("price_changes"), list):
            rows: list[dict[str, Any]] = []
            changes = [
                item
                for item in (event.get("price_changes") or [])
                if isinstance(item, Mapping)
            ]
            for idx, item in enumerate(changes):
                if not isinstance(item, Mapping):
                    continue
                rows.append(
                    self._base_row(
                        received_at=received_at,
                        normalized_at=normalized_at,
                        event_ts=event_ts,
                        event_type=event_type,
                        market=market,
                        payload_hash=payload_hash,
                        sequence_in_message=idx,
                        asset_id=_text(item.get("asset_id")),
                        price=_decimal_text_or_none(item.get("price")),
                        size=_decimal_text_or_none(item.get("size")),
                        side=_text(item.get("side")),
                        best_bid=_decimal_text_or_none(item.get("best_bid")),
                        best_ask=_decimal_text_or_none(item.get("best_ask")),
                        book_hash=(
                            _text(item.get("hash") or event.get("hash"))
                            if retain_provenance_hashes
                            else None
                        ),
                        source=row_source,
                        shard_id=row_shard_id,
                        change_index=idx,
                        is_last_in_group=(
                            raw_group_last_message
                            and idx == len(changes) - 1
                        ),
                        raw_frame_complete=(
                            raw_frame_last_message
                            and idx == len(changes) - 1
                        ),
                        **raw_fields,
                    )
                )
            return rows

        return [
            self._base_row(
                received_at=received_at,
                normalized_at=normalized_at,
                event_ts=event_ts,
                event_type=event_type,
                market=market,
                payload_hash=payload_hash,
                sequence_in_message=0,
                asset_id=_text(event.get("asset_id")),
                bids=_levels_json(event.get("bids")) if event_type == "book" else None,
                asks=_levels_json(event.get("asks")) if event_type == "book" else None,
                price=_decimal_text_or_none(event.get("price")),
                size=_decimal_text_or_none(event.get("size")),
                side=_text(event.get("side")),
                best_bid=_decimal_text_or_none(event.get("best_bid")),
                best_ask=_decimal_text_or_none(event.get("best_ask")),
                fee_rate_bps=_int_or_none(event.get("fee_rate_bps")),
                transaction_hash=_text(event.get("transaction_hash")),
                old_tick_size=_decimal_text_or_none(event.get("old_tick_size")),
                new_tick_size=_decimal_text_or_none(event.get("new_tick_size")),
                book_hash=_text(event.get("hash")),
                source=row_source,
                shard_id=row_shard_id,
                change_index=0,
                is_last_in_group=raw_group_last_message,
                raw_frame_complete=raw_frame_last_message,
                **raw_fields,
            )
        ]

    def _base_row(
        self,
        *,
        received_at: datetime,
        normalized_at: datetime,
        event_ts: datetime | None,
        event_type: str,
        market: str | None,
        payload_hash: str | None,
        sequence_in_message: int,
        asset_id: str | None,
        bids: str | None = None,
        asks: str | None = None,
        price: str | None = None,
        size: str | None = None,
        side: str | None = None,
        best_bid: str | None = None,
        best_ask: str | None = None,
        fee_rate_bps: int | None = None,
        transaction_hash: str | None = None,
        old_tick_size: str | None = None,
        new_tick_size: str | None = None,
        book_hash: str | None = None,
        source: str | None = None,
        shard_id: int | None = None,
        raw_connection_id: str | None = None,
        raw_connection_generation: int | None = None,
        raw_frame_seq: int | None = None,
        raw_received_wall_ns: int | None = None,
        raw_event_timestamp: str | None = None,
        clock_invalid_reason: str | None = None,
        rest_request_start_ns: int | None = None,
        rest_response_end_ns: int | None = None,
        message_index: int | None = None,
        change_index: int | None = None,
        group_id: str | None = None,
        is_last_in_group: bool = True,
        raw_frame_complete: bool = False,
    ) -> dict[str, Any]:
        return {
            "timestamp_received": received_at,
            "timestamp_normalized": normalized_at,
            "timestamp": event_ts,
            "raw_event_timestamp": raw_event_timestamp,
            "clock_invalid_reason": clock_invalid_reason,
            "rest_request_start_ns": rest_request_start_ns,
            "rest_response_end_ns": rest_response_end_ns,
            "market": market,
            "event_type": event_type,
            "asset_id": asset_id,
            "collector_seq": 0,
            "sequence_in_message": int(sequence_in_message),
            "bids": bids,
            "asks": asks,
            "price": price,
            "size": size,
            "side": side,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "fee_rate_bps": fee_rate_bps,
            "transaction_hash": transaction_hash,
            "old_tick_size": old_tick_size,
            "new_tick_size": new_tick_size,
            "book_hash": book_hash,
            "payload_hash": payload_hash,
            "source": source or self.source,
            "ws_url": self.ws_url,
            "shard_id": self.shard_id if shard_id is None else shard_id,
            "shard_count": self.shard_count,
            "raw_connection_id": raw_connection_id,
            "raw_connection_generation": raw_connection_generation,
            "raw_frame_seq": raw_frame_seq,
            "raw_received_wall_ns": raw_received_wall_ns,
            "message_index": message_index,
            "change_index": change_index,
            "group_id": group_id,
            "is_last_in_group": bool(is_last_in_group),
            "raw_frame_complete": bool(raw_frame_complete),
        }

    def _write_hour_rows(self, archive_hour: datetime, rows: list[dict[str, Any]]) -> L2ArchiveFile:
        archive_dir = self.base_dir / f"dt={archive_hour:%Y-%m-%d}" / f"hour={archive_hour:%H}"
        archive_dir.mkdir(parents=True, exist_ok=True)
        seq = int(time.time() * 1_000_000)
        shard = "all" if self.shard_id is None else str(self.shard_id)
        final_path = archive_dir / f"l2_events_{archive_hour:%Y%m%dT%H}_{seq}_shard{shard}.parquet"
        tmp_path = final_path.with_suffix(".tmp.parquet")
        input_path = final_path.with_suffix(".input.jsonl")
        stats_path = final_path.with_suffix(".stats.json")

        input_format = "newline_delimited"
        try:
            with input_path.open("wb") as output:
                if _orjson is not None:
                    output.write(
                        _orjson.dumps(rows, default=_json_default)
                    )
                    input_format = "array"
                else:
                    for row in rows:
                        output.write(_compact_json_bytes(row))
                        output.write(b"\n")
                output.flush()
                os.fsync(output.fileno())
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "quant.orderbook.l2_parquet_writer",
                    "--input",
                    str(input_path),
                    "--input-format",
                    input_format,
                    "--output",
                    str(tmp_path),
                    "--stats-output",
                    str(stats_path),
                    "--compression",
                    self.compression,
                    "--compression-level",
                    str(self.compression_level),
                    "--row-group-size",
                    str(self.row_group_size),
                    "--threads",
                    str(self.parquet_threads),
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=self.parquet_timeout_seconds,
            )
            if completed.returncode != 0:
                detail = (completed.stderr or completed.stdout or "unknown parquet writer error").strip()
                raise RuntimeError(f"L2 parquet writer failed: {detail[-1000:]}")
            stats = _load_file_stats(stats_path)
            if int(stats["event_count"]) != len(rows):
                raise RuntimeError(
                    "L2 parquet writer row count mismatch: "
                    f"expected={len(rows)} "
                    f"actual={stats['event_count']}"
                )
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        finally:
            input_path.unlink(missing_ok=True)
            stats_path.unlink(missing_ok=True)
        tmp_path.replace(final_path)
        digest = _sha256_file(final_path)
        item = L2ArchiveFile(
            path=final_path,
            archive_hour=archive_hour,
            event_count=len(rows),
            asset_count=stats["asset_count"],
            market_count=stats["market_count"],
            first_event_ts=stats["first_event_ts"],
            last_event_ts=stats["last_event_ts"],
            first_received_at=stats["first_received_at"],
            last_received_at=stats["last_received_at"],
            file_size_bytes=final_path.stat().st_size,
            sha256=digest,
            event_type_counts=stats["event_type_counts"],
            first_local_seq=stats["first_local_seq"],
            last_local_seq=stats["last_local_seq"],
            raw_frame_watermarks=stats["raw_frame_watermarks"],
            raw_frame_ranges=_raw_frame_ranges(rows),
        )
        if self.write_sidecar:
            self._write_sidecar(item)
        return item

    def _write_sidecar(self, item: L2ArchiveFile) -> None:
        relative_path = item.path.relative_to(self.base_dir)
        sidecar_path = archive_sidecar_path(item.path)
        payload = {
            "schema_version": "polymarket-l2-file-manifest-v1",
            "relative_path": relative_path.as_posix(),
            "archive_hour": item.archive_hour.isoformat(),
            "source": self.source,
            "ws_url": self.ws_url,
            "shard_id": self.shard_id,
            "shard_count": self.shard_count,
            "compression": self.compression.lower(),
            "compression_level": self.compression_level,
            "row_group_size": self.row_group_size,
            "event_count": item.event_count,
            "asset_count": item.asset_count,
            "market_count": item.market_count,
            "first_event_ts": _optional_iso(item.first_event_ts),
            "last_event_ts": _optional_iso(item.last_event_ts),
            "first_received_at": _optional_iso(item.first_received_at),
            "last_received_at": _optional_iso(item.last_received_at),
            "file_size_bytes": item.file_size_bytes,
            "sha256": item.sha256,
            "event_type_counts": item.event_type_counts,
            "first_local_seq": item.first_local_seq,
            "last_local_seq": item.last_local_seq,
            "raw_frame_watermarks": item.raw_frame_watermarks,
            "raw_frame_ranges": item.raw_frame_ranges,
            "written_at": datetime.now(timezone.utc).isoformat(),
        }
        tmp_path = sidecar_path.with_name(f".{sidecar_path.name}.{os.getpid()}.tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        os.replace(tmp_path, sidecar_path)

    def _insert_manifest(self, item: L2ArchiveFile) -> None:
        if self.conn is None:
            return
        metadata = {
            "sort_key": list(DEFAULT_SORT_KEY),
            "buffer_flush_rows": self.flush_rows,
            "buffer_flush_bytes": self.flush_bytes,
            "buffer_flush_seconds": self.flush_seconds,
            "raw_frame_watermarks": item.raw_frame_watermarks,
            "raw_frame_ranges": item.raw_frame_ranges,
        }
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.clob_l2_archive_manifest (
                    archive_hour, source, ws_url, shard_id, shard_count, path,
                    format, compression, compression_level, row_group_size, sort_key,
                    schema_version, event_count, asset_count, market_count,
                    first_event_ts, last_event_ts, first_received_at, last_received_at,
                    file_size_bytes, sha256, event_type_counts, first_local_seq, last_local_seq,
                    writer_version, status, metadata, written_at, committed_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s,
                    'parquet', %s, %s, %s, %s::jsonb,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s::jsonb, %s, %s,
                    'l2_archive_v2_integrity', 'ready', %s::jsonb, now(), now(), now()
                )
                ON CONFLICT (path) DO UPDATE SET
                    event_count = EXCLUDED.event_count,
                    asset_count = EXCLUDED.asset_count,
                    market_count = EXCLUDED.market_count,
                    first_event_ts = EXCLUDED.first_event_ts,
                    last_event_ts = EXCLUDED.last_event_ts,
                    first_received_at = EXCLUDED.first_received_at,
                    last_received_at = EXCLUDED.last_received_at,
                    file_size_bytes = EXCLUDED.file_size_bytes,
                    sha256 = EXCLUDED.sha256,
                    event_type_counts = EXCLUDED.event_type_counts,
                    first_local_seq = EXCLUDED.first_local_seq,
                    last_local_seq = EXCLUDED.last_local_seq,
                    writer_version = EXCLUDED.writer_version,
                    status = EXCLUDED.status,
                    metadata = EXCLUDED.metadata,
                    committed_at = EXCLUDED.committed_at,
                    updated_at = now()
                """,
                (
                    item.archive_hour,
                    self.source,
                    self.ws_url,
                    self.shard_id,
                    self.shard_count,
                    str(item.path),
                    self.compression.lower(),
                    self.compression_level,
                    self.row_group_size,
                    json.dumps(list(DEFAULT_SORT_KEY), ensure_ascii=False),
                    ARCHIVE_SCHEMA_VERSION,
                    item.event_count,
                    item.asset_count,
                    item.market_count,
                    item.first_event_ts,
                    item.last_event_ts,
                    item.first_received_at,
                    item.last_received_at,
                    item.file_size_bytes,
                    item.sha256,
                    json.dumps(item.event_type_counts, ensure_ascii=False, sort_keys=True),
                    item.first_local_seq,
                    item.last_local_seq,
                    json.dumps(metadata, ensure_ascii=False),
                ),
            )


@dataclass
class _StreamingSpool:
    archive_hour: datetime
    path: Path
    output: Any
    row_count: int = 0
    byte_count: int = 0
    raw_frame_ranges: dict[int, list[tuple[int, int]]] = field(
        default_factory=dict
    )


@dataclass
class StreamingSpoolL2ArchiveWriter(CompressedL2ArchiveWriter):
    """Stream normalized rows to a sealable spool before Parquet conversion.

    The regular writer retains Python dictionaries and serializes the complete
    flush chunk in one call.  That is efficient for normal collectors, but a
    large source-B flush can hold the parser process GIL long enough for the
    live WAL tail to fall behind.  This variant pays the serialization cost
    incrementally during normal batches.  A flush only fsyncs and seals the
    active files; the expensive sort, Parquet conversion, and statistics stay
    in a child process while a new spool accepts rows immediately.

    Raw WAL remains the durability boundary.  Stale, unsealed spool files are
    discarded on parser restart and replayed from the last Parquet watermark.
    """

    _spools: dict[datetime, _StreamingSpool] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _spool_rows: int = field(default=0, init=False, repr=False)
    _spool_bytes: int = field(default=0, init=False, repr=False)
    _spool_sequence: int = field(default=0, init=False, repr=False)
    _spool_dir: Path = field(init=False, repr=False)

    def __post_init__(self) -> None:
        super().__post_init__()
        shard = "all" if self.shard_id is None else str(self.shard_id)
        self._spool_dir = (
            self.base_dir / ".silver_spool" / f"shard-{shard}"
        )
        self._spool_dir.mkdir(parents=True, exist_ok=True)
        # File names include the writer PID and are therefore private even
        # though the shard directory is shared.  Do not delete another PID's
        # spool here: during a bounded systemd hand-off the replacement parser
        # can start while the previous parser is still sealing Parquet.  The
        # old startup cleanup deleted that live `.active`/`.sealed` file and
        # produced `write to closed file` or `FileNotFoundError` retries.
        # Bronze WAL remains authoritative and the bounded ephemeral janitor
        # removes only hour-old files whose exact parser PID is no longer live.

    def insert_messages(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        received_ts_ms: int | None = None,
    ) -> int:
        received_at = _ms_to_datetime(
            received_ts_ms or int(time.time() * 1000)
        )
        accepted = 0
        encoded_by_hour: dict[datetime, bytearray] = {}
        rows_by_hour: dict[datetime, int] = {}
        completed_frames_by_hour: dict[
            datetime, list[tuple[int, int]]
        ] = {}
        archive_hours_by_received: dict[str, datetime] = {}
        with self._rows_lock:
            for message in messages:
                rows = self._streaming_rows_for_message(
                    message,
                    received_at=received_at,
                )
                for row in rows:
                    self._collector_seq += 1
                    row["collector_seq"] = self._collector_seq
                    row_received = row.get("timestamp_received")
                    if isinstance(row_received, str):
                        archive_hour = archive_hours_by_received.get(
                            row_received
                        )
                        if archive_hour is None:
                            parsed_received = datetime.fromisoformat(
                                row_received.replace("Z", "+00:00")
                            )
                            if parsed_received.tzinfo is None:
                                parsed_received = parsed_received.replace(
                                    tzinfo=timezone.utc
                                )
                            archive_hour = (
                                parsed_received.astimezone(timezone.utc)
                                .replace(
                                    minute=0,
                                    second=0,
                                    microsecond=0,
                                )
                            )
                            archive_hours_by_received[row_received] = (
                                archive_hour
                            )
                    else:
                        archive_hour = None
                    if archive_hour is None and not isinstance(
                        row_received,
                        datetime,
                    ):
                        row_received = received_at
                    if archive_hour is None:
                        archive_hour = (
                            row_received.astimezone(timezone.utc)
                            .replace(minute=0, second=0, microsecond=0)
                        )
                    # read_json() receives an explicit schema, so omitted
                    # fields are restored as NULL in the final Parquet.  Do
                    # not write dozens of repeated `"field":null` pairs to
                    # the temporary spool; that only consumes GCP disk and
                    # decoder CPU while preserving no information.
                    spool_row = {
                        key: value
                        for key, value in row.items()
                        if value is not None
                    }
                    encoded = _compact_json_bytes(spool_row) + b"\n"
                    encoded_by_hour.setdefault(
                        archive_hour,
                        bytearray(),
                    ).extend(encoded)
                    rows_by_hour[archive_hour] = (
                        rows_by_hour.get(archive_hour, 0) + 1
                    )
                    if bool(row.get("raw_frame_complete")):
                        try:
                            completed_frame = (
                                int(row.get("shard_id")),
                                int(row.get("raw_frame_seq")),
                            )
                        except (TypeError, ValueError):
                            pass
                        else:
                            completed_frames_by_hour.setdefault(
                                archive_hour, []
                            ).append(completed_frame)
                    accepted += 1
            for archive_hour, encoded in encoded_by_hour.items():
                spool = self._spools.get(archive_hour)
                if spool is None:
                    spool = self._open_spool_locked(archive_hour)
                    self._spools[archive_hour] = spool
                spool.output.write(encoded)
                row_count = rows_by_hour[archive_hour]
                byte_count = len(encoded)
                spool.row_count += row_count
                spool.byte_count += byte_count
                for shard_id, frame_seq in completed_frames_by_hour.get(
                    archive_hour, ()
                ):
                    _append_frame_range(
                        spool.raw_frame_ranges.setdefault(shard_id, []),
                        frame_seq,
                    )
                self._spool_rows += row_count
                self._spool_bytes += byte_count
        return accepted

    def _streaming_rows_for_message(
        self,
        message: Mapping[str, Any],
        *,
        received_at: datetime,
    ) -> list[dict[str, Any]]:
        """Build compact delta rows without allocating null-heavy base rows.

        Source-A Silver deliberately omits recomputable hashes from price
        deltas.  The generic normalizer still allocates every nullable archive
        column for each delta and the streaming writer immediately removes
        those nulls again.  A busy hour contains more than one hundred million
        deltas, so that redundant Python object churn can starve the raw WS
        collectors on the 16-vCPU GCP host.

        Keep snapshots and every non-delta event on the generic path.  This
        specialization is used only for the already-compact nested
        ``price_changes`` representation and emits the same non-null fields,
        group boundaries and durable raw-frame watermark.
        """

        if (
            not self.compact_provenance
            or str(message.get("event_type") or "").strip()
            != "price_change"
            or not isinstance(message.get("price_changes"), list)
        ):
            return self._rows_for_message(
                message,
                received_at=received_at,
            )

        raw_received_wall_ns = _int_or_none(
            message.get("_raw_received_wall_ns")
        )
        if raw_received_wall_ns is not None:
            received_at = datetime.fromtimestamp(
                raw_received_wall_ns / 1_000_000_000,
                tz=timezone.utc,
            )
        event_ts, raw_event_timestamp, clock_invalid_reason = (
            _event_clock(
                message.get("timestamp"),
                received_at=received_at,
                preserve_raw=bool(
                    message.get("_preserve_raw_event_timestamp")
                ),
            )
        )
        # The JSON spool declares these columns as VARCHAR and DuckDB performs
        # the single authoritative TIMESTAMPTZ cast.  Serialize each repeated
        # frame timestamp once here instead of invoking ``datetime.isoformat``
        # through orjson's fallback for every nested price change.
        received_at_text = received_at.isoformat()
        normalized_at_text = datetime.now(timezone.utc).isoformat()
        event_ts_text = (
            event_ts.isoformat() if event_ts is not None else None
        )
        market = _text(
            message.get("market") or message.get("condition_id")
        )
        row_shard_id = _int_or_none(message.get("_archive_shard_id"))
        shard_id = self.shard_id if row_shard_id is None else row_shard_id
        source = _text(message.get("source")) or self.source
        raw_connection_id = _text(message.get("_raw_connection_id"))
        raw_connection_generation = _int_or_none(
            message.get("_raw_connection_generation")
        )
        raw_frame_seq = _int_or_none(message.get("_raw_frame_seq"))
        message_index = _int_or_none(message.get("_raw_message_index"))
        group_id = _text(message.get("_raw_group_id"))
        raw_frame_last_message = bool(
            message.get("_raw_is_last_archivable_message")
        )
        raw_group_last_message = bool(
            message.get("_raw_is_last_group_message", True)
        )
        changes = [
            item
            for item in message.get("price_changes") or ()
            if isinstance(item, Mapping)
        ]
        rows: list[dict[str, Any]] = []
        last_change_index = len(changes) - 1
        for change_index, item in enumerate(changes):
            row: dict[str, Any] = {
                "timestamp_received": received_at_text,
                "timestamp_normalized": normalized_at_text,
                "event_type": "price_change",
                "collector_seq": 0,
                "sequence_in_message": change_index,
                "source": source,
                "ws_url": self.ws_url,
                "shard_count": self.shard_count,
                "change_index": change_index,
                "is_last_in_group": (
                    raw_group_last_message
                    and change_index == last_change_index
                ),
                "raw_frame_complete": (
                    raw_frame_last_message
                    and change_index == last_change_index
                ),
            }
            optional = {
                "market": market,
                "timestamp": event_ts_text,
                "raw_event_timestamp": raw_event_timestamp,
                "clock_invalid_reason": clock_invalid_reason,
                "asset_id": _text(item.get("asset_id")),
                # These are cast with TRY_CAST by l2_parquet_writer.  Keeping
                # the upstream scalar text avoids four regex/Decimal
                # canonicalization passes per hot delta while producing the
                # same typed Parquet value.
                "price": _raw_scalar_text_or_none(item.get("price")),
                "size": _raw_scalar_text_or_none(item.get("size")),
                "side": _text(item.get("side")),
                "best_bid": _raw_scalar_text_or_none(
                    item.get("best_bid")
                ),
                "best_ask": _raw_scalar_text_or_none(
                    item.get("best_ask")
                ),
                "shard_id": shard_id,
                "raw_connection_id": raw_connection_id,
                "raw_connection_generation": raw_connection_generation,
                "raw_frame_seq": raw_frame_seq,
                "raw_received_wall_ns": raw_received_wall_ns,
                "message_index": message_index,
                "group_id": group_id,
            }
            row.update(
                (key, value)
                for key, value in optional.items()
                if value is not None
            )
            rows.append(row)
        return rows

    def should_flush(self) -> bool:
        with self._rows_lock:
            return self._should_flush_locked()

    def _should_flush_locked(self) -> bool:
        if self._spool_rows <= 0:
            return False
        return (
            self._spool_rows >= self.flush_rows
            or (
                self.flush_bytes > 0
                and self._spool_bytes >= self.flush_bytes
            )
            or (
                time.monotonic() - self._last_flush_monotonic
                >= self.flush_seconds
            )
        )

    def flush(self, *, force: bool = False) -> list[L2ArchiveFile]:
        with self._flush_lock:
            with self._rows_lock:
                if not self._spools:
                    return []
                if not force and not self._should_flush_locked():
                    return []
                sealed = self._seal_spools_locked()
                self._last_flush_monotonic = time.monotonic()
            written: list[L2ArchiveFile] = []
            for spool in sealed:
                item = self._write_hour_spool(spool)
                written.append(item)
                self.files_written += 1
                self.rows_written += item.event_count
                self._insert_manifest(item)
            return written

    def close(self) -> list[L2ArchiveFile]:
        return self.flush(force=True)

    def _open_spool_locked(
        self,
        archive_hour: datetime,
    ) -> _StreamingSpool:
        self._spool_sequence += 1
        path = self._spool_dir / (
            f"{archive_hour:%Y%m%dT%H}-"
            f"{os.getpid()}-{self._spool_sequence}.active.jsonl"
        )
        return _StreamingSpool(
            archive_hour=archive_hour,
            path=path,
            output=path.open("ab", buffering=1024 * 1024),
        )

    def _seal_spools_locked(self) -> list[_StreamingSpool]:
        sealed: list[_StreamingSpool] = []
        for archive_hour in sorted(self._spools):
            spool = self._spools[archive_hour]
            spool.output.flush()
            os.fsync(spool.output.fileno())
            spool.output.close()
            sealed_path = spool.path.with_name(
                spool.path.name.replace(
                    ".active.jsonl",
                    ".sealed.jsonl",
                )
            )
            os.replace(spool.path, sealed_path)
            spool.path = sealed_path
            sealed.append(spool)
        self._spools = {}
        self._spool_rows = 0
        self._spool_bytes = 0
        return sealed

    def _write_hour_spool(
        self,
        spool: _StreamingSpool,
    ) -> L2ArchiveFile:
        archive_hour = spool.archive_hour
        archive_dir = (
            self.base_dir
            / f"dt={archive_hour:%Y-%m-%d}"
            / f"hour={archive_hour:%H}"
        )
        archive_dir.mkdir(parents=True, exist_ok=True)
        shard = "all" if self.shard_id is None else str(self.shard_id)
        sequence = time.time_ns()
        final_path = archive_dir / (
            f"l2_events_{archive_hour:%Y%m%dT%H}_{sequence}_"
            f"shard{shard}.parquet"
        )
        tmp_path = final_path.with_suffix(".tmp.parquet")
        stats_path = final_path.with_suffix(".stats.json")
        parquet_lock = None
        try:
            if self.parquet_lock_path is not None:
                parquet_lock_path = self.parquet_lock_path
                if self.parquet_lock_slots > 1:
                    slot = int(self.shard_id or 0) % self.parquet_lock_slots
                    parquet_lock_path = parquet_lock_path.with_name(
                        f"{parquet_lock_path.name}.slot-{slot}"
                    )
                parquet_lock = parquet_lock_path.open("a+b")
                fcntl.flock(parquet_lock.fileno(), fcntl.LOCK_EX)
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "quant.orderbook.l2_parquet_writer",
                    "--input",
                    str(spool.path),
                    "--input-format",
                    "newline_delimited",
                    "--output",
                    str(tmp_path),
                    "--stats-output",
                    str(stats_path),
                    "--compression",
                    self.compression,
                    "--compression-level",
                    str(self.compression_level),
                    "--row-group-size",
                    str(self.row_group_size),
                    "--threads",
                    str(self.parquet_threads),
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=self.parquet_timeout_seconds,
            )
            if completed.returncode != 0:
                detail = (
                    completed.stderr
                    or completed.stdout
                    or "unknown parquet writer error"
                ).strip()
                raise RuntimeError(
                    f"L2 parquet writer failed: {detail[-1000:]}"
                )
            stats = _load_file_stats(stats_path)
            if int(stats["event_count"]) != spool.row_count:
                raise RuntimeError(
                    "L2 parquet writer row count mismatch: "
                    f"expected={spool.row_count} "
                    f"actual={stats['event_count']}"
                )
            os.replace(tmp_path, final_path)
            digest = _sha256_file(final_path)
            item = L2ArchiveFile(
                path=final_path,
                archive_hour=archive_hour,
                event_count=spool.row_count,
                asset_count=stats["asset_count"],
                market_count=stats["market_count"],
                first_event_ts=stats["first_event_ts"],
                last_event_ts=stats["last_event_ts"],
                first_received_at=stats["first_received_at"],
                last_received_at=stats["last_received_at"],
                file_size_bytes=final_path.stat().st_size,
                sha256=digest,
                event_type_counts=stats["event_type_counts"],
                first_local_seq=stats["first_local_seq"],
                last_local_seq=stats["last_local_seq"],
                raw_frame_watermarks=stats[
                    "raw_frame_watermarks"
                ],
                raw_frame_ranges={
                    shard_id: tuple(ranges)
                    for shard_id, ranges in spool.raw_frame_ranges.items()
                },
            )
            if self.write_sidecar:
                self._write_sidecar(item)
            return item
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        finally:
            if parquet_lock is not None:
                fcntl.flock(parquet_lock.fileno(), fcntl.LOCK_UN)
                parquet_lock.close()
            stats_path.unlink(missing_ok=True)
            spool.path.unlink(missing_ok=True)


def insert_l2_archive_manifest_files(
    conn: Any,
    files: Sequence[L2ArchiveFile],
    *,
    source: str = ARCHIVE_SOURCE,
    ws_url: str | None = None,
    shard_id: int | None = None,
    shard_count: int | None = None,
    compression_level: int = 9,
    row_group_size: int = 1_048_576,
    flush_rows: int = 50_000,
    flush_seconds: float = 60.0,
    ensure_schema: bool = True,
) -> int:
    """Insert manifest rows for already-written archive files."""

    writer = CompressedL2ArchiveWriter(
        conn=conn,
        source=source,
        ws_url=ws_url,
        shard_id=shard_id,
        shard_count=shard_count,
        compression_level=compression_level,
        row_group_size=row_group_size,
        flush_rows=flush_rows,
        flush_seconds=flush_seconds,
    )
    return writer.insert_manifest_files(files, ensure_schema=ensure_schema)


def _group_by_archive_hour(rows: Sequence[Mapping[str, Any]]) -> dict[datetime, list[dict[str, Any]]]:
    grouped: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        received = row.get("timestamp_received")
        if not isinstance(received, datetime):
            received = datetime.now(timezone.utc)
        hour = received.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        grouped.setdefault(hour, []).append(
            row if isinstance(row, dict) else dict(row)
        )
    return grouped


def _file_stats(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    assets: set[str] = set()
    markets: set[str] = set()
    first_event_ts: datetime | None = None
    last_event_ts: datetime | None = None
    first_received_at: datetime | None = None
    last_received_at: datetime | None = None
    event_type_counts: dict[str, int] = {}
    first_local_seq: int | None = None
    last_local_seq: int | None = None
    raw_frame_watermarks: dict[int, int] = {}
    for row in rows:
        asset_id = str(row.get("asset_id") or "")
        if asset_id:
            assets.add(asset_id)
        market = str(row.get("market") or "")
        if market:
            markets.add(market)
        event_ts = row.get("timestamp")
        if isinstance(event_ts, datetime):
            first_event_ts = (
                event_ts if first_event_ts is None else min(first_event_ts, event_ts)
            )
            last_event_ts = (
                event_ts if last_event_ts is None else max(last_event_ts, event_ts)
            )
        received_at = row.get("timestamp_received")
        if isinstance(received_at, datetime):
            first_received_at = (
                received_at
                if first_received_at is None
                else min(first_received_at, received_at)
            )
            last_received_at = (
                received_at
                if last_received_at is None
                else max(last_received_at, received_at)
            )
        event_type = str(row.get("event_type") or "unknown")
        event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
        try:
            local_seq = int(row.get("collector_seq"))
        except (TypeError, ValueError):
            pass
        else:
            first_local_seq = (
                local_seq if first_local_seq is None else min(first_local_seq, local_seq)
            )
            last_local_seq = (
                local_seq if last_local_seq is None else max(last_local_seq, local_seq)
            )
        if bool(row.get("raw_frame_complete")):
            try:
                raw_shard_id = int(row.get("shard_id"))
                raw_frame_seq = int(row.get("raw_frame_seq"))
            except (TypeError, ValueError):
                pass
            else:
                raw_frame_watermarks[raw_shard_id] = max(
                    raw_frame_watermarks.get(raw_shard_id, 0),
                    raw_frame_seq,
                )
    return {
        "asset_count": len(assets),
        "market_count": len(markets),
        "first_event_ts": first_event_ts,
        "last_event_ts": last_event_ts,
        "first_received_at": first_received_at,
        "last_received_at": last_received_at,
        "event_type_counts": event_type_counts,
        "first_local_seq": first_local_seq,
        "last_local_seq": last_local_seq,
        "raw_frame_watermarks": raw_frame_watermarks,
        "raw_frame_ranges": _raw_frame_ranges(rows),
    }


def _raw_frame_ranges(
    rows: Sequence[Mapping[str, Any]],
) -> dict[int, tuple[tuple[int, int], ...]]:
    completed: dict[int, set[int]] = {}
    for row in rows:
        if not bool(row.get("raw_frame_complete")):
            continue
        try:
            shard_id = int(row.get("shard_id"))
            frame_seq = int(row.get("raw_frame_seq"))
        except (TypeError, ValueError):
            continue
        completed.setdefault(shard_id, set()).add(frame_seq)
    result: dict[int, tuple[tuple[int, int], ...]] = {}
    for shard_id, frame_seqs in completed.items():
        ranges: list[tuple[int, int]] = []
        for frame_seq in sorted(frame_seqs):
            _append_frame_range(ranges, frame_seq)
        result[shard_id] = tuple(ranges)
    return result


def _append_frame_range(
    ranges: list[tuple[int, int]],
    frame_seq: int,
) -> None:
    """Append a normally ordered frame while tolerating duplicate evidence."""

    value = int(frame_seq)
    if not ranges or value > ranges[-1][1] + 1:
        ranges.append((value, value))
    elif value > ranges[-1][1]:
        ranges[-1] = (ranges[-1][0], value)


def _load_file_stats(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("L2 parquet stats must be a JSON object")
    return {
        "event_count": int(payload.get("event_count") or 0),
        "asset_count": int(payload.get("asset_count") or 0),
        "market_count": int(payload.get("market_count") or 0),
        "first_event_ts": _parse_optional_datetime(
            payload.get("first_event_ts")
        ),
        "last_event_ts": _parse_optional_datetime(
            payload.get("last_event_ts")
        ),
        "first_received_at": _parse_optional_datetime(
            payload.get("first_received_at")
        ),
        "last_received_at": _parse_optional_datetime(
            payload.get("last_received_at")
        ),
        "event_type_counts": {
            str(key): int(value)
            for key, value in (
                payload.get("event_type_counts") or {}
            ).items()
        },
        "first_local_seq": _int_or_none(
            payload.get("first_local_seq")
        ),
        "last_local_seq": _int_or_none(
            payload.get("last_local_seq")
        ),
        "raw_frame_watermarks": {
            int(key): int(value)
            for key, value in (
                payload.get("raw_frame_watermarks") or {}
            ).items()
        },
    }


def _parse_optional_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    parsed = datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _payload_hash(value: Mapping[str, Any]) -> str:
    # Collector-local annotations must not change the identity of an upstream
    # event. This makes the hash stable across the local and GCP feeds.
    canonical = {
        key: item
        for key, item in value.items()
        if not str(key).startswith("_") and key not in {"source", "received_at"}
    }
    return hashlib.sha256(_canonical_json_bytes(canonical)).hexdigest()
def _levels_json(value: Any) -> str:
    levels: list[list[str]] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, Mapping):
                price = _decimal_text_or_none(item.get("price"))
                size = _decimal_text_or_none(item.get("size"))
            elif isinstance(item, (list, tuple)) and len(item) >= 2:
                price = _decimal_text_or_none(item[0])
                size = _decimal_text_or_none(item[1])
            else:
                continue
            if price is not None and size is not None:
                levels.append([price, size])
    return _compact_json_bytes(levels).decode("utf-8")


def _canonical_json_bytes(value: Any) -> bytes:
    if _orjson is not None:
        return _orjson.dumps(value, option=_orjson.OPT_SORT_KEYS, default=str)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _compact_json_bytes(value: Any) -> bytes:
    if _orjson is not None:
        return _orjson.dumps(value, default=_json_default)
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")


def _timestamp_ms(value: Any) -> int | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite():
        return None
    if parsed > 10**17:
        return int(parsed / 1_000_000)
    if parsed > 10**14:
        return int(parsed / 1_000)
    if parsed < 10**11:
        return int(parsed * 1000)
    return int(parsed)


def _event_clock(
    value: Any,
    *,
    received_at: datetime,
    preserve_raw: bool = False,
) -> tuple[datetime | None, str | None, str | None]:
    """Parse an upstream clock without inventing exchange-time evidence.

    Bronze keeps the original frame, while Silver needs a queryable reason for
    refusing a malformed clock.  A missing or invalid exchange timestamp is
    therefore represented as ``NULL`` instead of silently borrowing the local
    receive clock.  The row is still persisted so acquisition remains
    lossless and downstream replay can fail closed explicitly.
    """

    raw_value = _raw_event_timestamp(value)
    if value is None:
        return None, None, "missing_exchange_timestamp"
    event_ts_ms = _timestamp_ms(value)
    if event_ts_ms is None:
        return None, raw_value, "invalid_exchange_timestamp"
    try:
        event_ts = _ms_to_datetime(event_ts_ms)
    except (OverflowError, OSError, ValueError):
        return None, raw_value, "invalid_exchange_timestamp"
    normalized_received_at = (
        received_at
        if received_at.tzinfo is not None
        else received_at.replace(tzinfo=timezone.utc)
    ).astimezone(timezone.utc)
    if event_ts > normalized_received_at:
        return (
            event_ts,
            raw_value,
            "exchange_after_received_timestamp",
        )
    return event_ts, raw_value if preserve_raw else None, None


def _raw_event_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    try:
        return _canonical_json_bytes(value).decode("utf-8")
    except (TypeError, ValueError):
        return str(value)


def _ms_to_datetime(value: int) -> datetime:
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


def _decimal_text_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        text = value.strip()
        # Polymarket's WS hot path already emits plain decimal strings.  Keep
        # them as text and let the Parquet worker cast once in vectorized
        # DuckDB code; constructing Decimal four times per price-change row was
        # the collector's dominant Python CPU cost.
        if _PLAIN_DECIMAL_RE.fullmatch(text):
            return _canonical_plain_decimal(text)
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not parsed.is_finite():
        return None
    text = format(parsed.normalize(), "f")
    return "0" if text == "-0" else text


def _raw_scalar_text_or_none(value: Any) -> str | None:
    """Return scalar text for a downstream vectorized TRY_CAST.

    Unlike ``_text``, numeric zero is data rather than an empty value.
    """

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _canonical_plain_decimal(value: str) -> str:
    negative = value.startswith("-")
    unsigned = value[1:] if value[:1] in {"+", "-"} else value
    whole, separator, fraction = unsigned.partition(".")
    whole = whole.lstrip("0") or "0"
    fraction = fraction.rstrip("0") if separator else ""
    if whole == "0" and not fraction:
        return "0"
    canonical = f"{whole}.{fraction}" if fraction else whole
    return f"-{canonical}" if negative else canonical


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def archive_sidecar_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".manifest.json")


def _optional_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _estimated_row_bytes(row: Mapping[str, Any]) -> int:
    """Estimate retained payload bytes without serializing every hot-path row."""

    # Most fields have a small fixed upper bound.  Only inspect the variable
    # width identity/depth fields instead of walking all 24 columns for every
    # price-change row.
    return 384 + sum(
        len(value)
        for key in (
            "market",
            "asset_id",
            "bids",
            "asks",
            "transaction_hash",
            "book_hash",
        )
        if isinstance((value := row.get(key)), str)
    )
