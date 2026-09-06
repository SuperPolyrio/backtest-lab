"""Lazy XUE Native L2 archive delivery for the main backtest engine."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import os
from typing import Any

from .adapters import Pml2ArchiveSnapshotLoader
from .contracts import (
    BookFrameBatchEvent,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    OrderStatus,
)
from .session import ReplayExecutionSession


class XueNativeExecutionProvider:
    """Restore only the Native L2 intervals reached by actual strategy orders."""

    def __init__(
        self,
        *,
        session: ReplayExecutionSession,
        condition_id: str,
        market_id: str,
        yes_asset_id: str,
        no_asset_id: str,
        loader: Pml2ArchiveSnapshotLoader | None = None,
        max_events: int | None = None,
        chunk_seconds: int | None = None,
        initial_lookback_seconds: int | None = None,
    ) -> None:
        self.session = session
        self.condition_id = str(condition_id).lower()
        self.market_id = str(market_id)
        self.yes_asset_id = str(yes_asset_id)
        self.no_asset_id = str(no_asset_id)
        self.loader = loader or Pml2ArchiveSnapshotLoader()
        self.max_events = max(
            1,
            int(
                max_events
                if max_events is not None
                else os.environ.get("POLYDATA_QUANT_PML2_XUE_MAX_EVENTS", "500000")
            ),
        )
        self.chunk_seconds = max(
            1,
            int(
                chunk_seconds
                if chunk_seconds is not None
                else os.environ.get("POLYDATA_QUANT_PML2_XUE_CHUNK_SECONDS", "3600")
            ),
        )
        self.initial_lookback_seconds = max(
            1,
            int(
                initial_lookback_seconds
                if initial_lookback_seconds is not None
                else os.environ.get("POLYDATA_QUANT_PML2_XUE_LOOKBACK_SECONDS", "1")
            ),
        )
        self.prepared_until: datetime | None = None
        self.request_count = 0
        self.restore_count = 0
        self.failed_restore_count = 0
        self.row_count = 0
        self.event_count = 0
        self.snapshot_count = 0
        self.delta_count = 0
        self.trade_count = 0
        self.baseline_clock_count = 0
        self.raw_event_clock_count = 0
        self.frame_evidence_count = 0
        self._all_clock_verified = True
        self._manifest_hashes: set[str] = set()
        self._source_files: set[str] = set()
        self.last_result: dict[str, Any] = {
            "status": "configured",
            "reason": "awaiting first strategy order",
        }

    def prepare_at(self, target_ts: datetime) -> bool:
        """Deliver causally available archive events through ``target_ts``."""

        target = _utc(target_ts)
        self.request_count += 1
        if self.prepared_until is not None and target < self.prepared_until:
            self.last_result = {
                "status": "data_not_ready",
                "reason": "strategy orders are not chronological",
                "target_ts": target.isoformat(),
                "prepared_until": self.prepared_until.isoformat(),
            }
            return False
        if self.prepared_until == target:
            return True

        if self.prepared_until is None or not self._has_live_orders():
            cursor = target - timedelta(seconds=self.initial_lookback_seconds)
        else:
            cursor = self.prepared_until

        while cursor < target:
            end = min(target, cursor + timedelta(seconds=self.chunk_seconds))
            result = self.loader.restore_condition_events(
                condition_id=self.condition_id,
                market_id=self.market_id,
                yes_asset_id=self.yes_asset_id,
                no_asset_id=self.no_asset_id,
                start_time=cursor,
                end_time=end,
                feed_latency_ms=self.session.profile.feed_latency_ms,
                max_events=self.max_events,
            )
            if not result.restored:
                self.failed_restore_count += 1
                self.last_result = {
                    "status": "data_not_ready",
                    "reason": result.reason,
                    "start_time": cursor.isoformat(),
                    "end_time": end.isoformat(),
                }
                return False
            self._ingest(result)
            self.prepared_until = end
            cursor = end

        self.last_result = {
            "status": "ready",
            "reason": "xue_native_l2_available_at_order_arrival",
            "target_ts": target.isoformat(),
            "prepared_until": target.isoformat(),
        }
        return True

    def context(self) -> dict[str, Any]:
        manifest_hash = self._aggregate_manifest_hash()
        return {
            "schema_version": "pml2_xue_native_execution_context_v1",
            "status": self.last_result.get("status", "configured"),
            "source": "xue_native_l2_archive",
            "source_mode": "XUE_NATIVE",
            "archive_dir": str(self.loader.archive_dir),
            "condition_id": self.condition_id,
            "market_id": self.market_id,
            "yes_asset_id": self.yes_asset_id,
            "no_asset_id": self.no_asset_id,
            "request_count": self.request_count,
            "restore_count": self.restore_count,
            "failed_restore_count": self.failed_restore_count,
            "row_count": self.row_count,
            "event_count": self.event_count,
            "snapshot_count": self.snapshot_count,
            "delta_count": self.delta_count,
            "trade_count": self.trade_count,
            "coverage_proof_count": len(
                self.session.coverage_manifest().transport_coverage_proof_ids
            ),
            "source_file_count": len(self._source_files),
            "source_files_sample": sorted(self._source_files)[:20],
            "source_manifest_hash": manifest_hash,
            "clock_verified": bool(
                self.restore_count
                and self._all_clock_verified
                and self.baseline_clock_count == self.restore_count * 2
                and self.frame_evidence_count == self.restore_count
            ),
            "prepared_until": (
                self.prepared_until.isoformat() if self.prepared_until else None
            ),
            "fallback_used": False,
            "last_result": dict(self.last_result),
        }

    def _ingest(self, result: Any) -> None:
        for window in result.transport_coverage_windows:
            self.session.register_transport_coverage(window)
        for event in result.events:
            if isinstance(event, BookSnapshotEvent):
                self.session.ingest_snapshot(event)
            elif isinstance(event, BookFrameBatchEvent):
                self.session.ingest_frame_batch(event)
            elif isinstance(event, BookLevelBatchEvent):
                self.session.ingest_level_batch(event)
            else:
                self.session.ingest_trade(event)

        self.restore_count += 1
        self.row_count += int(result.row_count)
        self.event_count += len(result.events)
        self.snapshot_count += int(result.snapshot_count)
        self.delta_count += int(result.delta_count)
        self.trade_count += int(result.trade_count)
        self.baseline_clock_count += int(result.baseline_clock_count)
        self.raw_event_clock_count += int(result.raw_event_clock_count)
        self.frame_evidence_count += int(result.frame_evidence_verified)
        self._all_clock_verified = self._all_clock_verified and bool(
            result.clock_verified
        )
        if result.source_manifest_hash:
            self._manifest_hashes.add(str(result.source_manifest_hash))
        self._source_files.update(str(item) for item in result.source_files)

        manifest_hash = self._aggregate_manifest_hash()
        if (
            self._all_clock_verified
            and self.baseline_clock_count == self.restore_count * 2
            and self.frame_evidence_count == self.restore_count
            and manifest_hash
        ):
            self.session.register_verified_cold_restore_clock_evidence(
                restore_count=self.restore_count,
                baseline_clock_count=self.baseline_clock_count,
                raw_event_clock_count=self.raw_event_clock_count,
                frame_evidence_count=self.frame_evidence_count,
                source_manifest_hash=manifest_hash,
            )

    def _has_live_orders(self) -> bool:
        return any(
            state.status
            in {
                OrderStatus.PENDING,
                OrderStatus.WAITING_FOR_DATA,
                OrderStatus.WORKING,
                OrderStatus.PARTIAL,
            }
            for state in self.session.orders.values()
        )

    def _aggregate_manifest_hash(self) -> str:
        if not self._manifest_hashes:
            return ""
        payload = "\n".join(sorted(self._manifest_hashes)).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("PML2 archive execution timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)
