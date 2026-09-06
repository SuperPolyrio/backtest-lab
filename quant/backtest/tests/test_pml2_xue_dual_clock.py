from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

from quant.backtest.pml2.adapters import (
    Pml2ArchiveSnapshotLoader,
    _xue_rows_to_pml2_events,
)
from quant.backtest.pml2.contracts import (
    BookFrameBatchEvent,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    EconomicBookSide,
    Outcome,
)
from quant.orderbook.l2_archive import CompressedL2ArchiveWriter
from quant.orderbook.l2_replay import (
    L2ArchiveReplayReader,
    L2ReplayNotReady,
    L2StateCheckpoint,
)

UTC = timezone.utc


def _book_row(*, exchange_ts: datetime, received_ts: datetime) -> dict[str, object]:
    return {
        "timestamp": exchange_ts,
        "timestamp_received": received_ts,
        "event_type": "book",
        "asset_id": "yes-token",
        "bids": '[["0.40", "20"]]',
        "asks": '[["0.50", "10"]]',
        "source": "polymarket_market_ws_raw_a",
        "collector_seq": 7,
        "payload_hash": "payload-7",
        "book_hash": "book-7",
        "raw_connection_id": "connection-7",
        "raw_connection_generation": 1,
        "group_id": "frame-7",
        "raw_frame_seq": 7,
        "frame_raw_complete": True,
        "group_has_terminal": True,
        "is_last_in_group": True,
        "raw_frame_complete": True,
    }


def test_xue_adapter_preserves_exchange_and_receive_clocks() -> None:
    exchange_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    received_ts = exchange_ts + timedelta(milliseconds=80)

    events = _xue_rows_to_pml2_events(
        pd.DataFrame([_book_row(exchange_ts=exchange_ts, received_ts=received_ts)]),
        condition_id="condition-1",
        market_id="market-1",
        outcomes={"yes-token": Outcome.YES},
        book_epoch=0,
        feed_latency_ms=20,
    )

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, BookSnapshotEvent)
    assert event.exchange_ts == exchange_ts
    assert event.local_ts == received_ts + timedelta(milliseconds=20)


def test_same_timestamp_snapshots_from_distinct_raw_frames_stay_separate() -> None:
    exchange_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    yes_row = _book_row(
        exchange_ts=exchange_ts,
        received_ts=exchange_ts + timedelta(milliseconds=80),
    )
    no_row = {
        **_book_row(
            exchange_ts=exchange_ts,
            received_ts=exchange_ts + timedelta(milliseconds=81),
        ),
        "asset_id": "no-token",
        "collector_seq": 8,
        "payload_hash": "payload-8",
        "book_hash": "book-8",
        "raw_frame_seq": 8,
        "group_id": "frame-8",
    }

    events = _xue_rows_to_pml2_events(
        pd.DataFrame([yes_row, no_row]),
        condition_id="condition-1",
        market_id="market-1",
        outcomes={"yes-token": Outcome.YES, "no-token": Outcome.NO},
        book_epoch=0,
        feed_latency_ms=0,
        require_complete_top_hints=True,
    )

    assert len(events) == 2
    assert all(isinstance(event, BookSnapshotEvent) for event in events)
    assert [event.snapshot_id for event in events] == ["book-7", "book-8"]
    assert events[0].exchange_ts == events[1].exchange_ts
    assert events[0].source_received_ts < events[1].source_received_ts


def test_xue_adapter_fails_closed_on_causal_clock_inversion() -> None:
    received_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    exchange_ts = received_ts + timedelta(milliseconds=1)

    with pytest.raises(
        ValueError,
        match="timestamp cannot exceed timestamp_received",
    ):
        _xue_rows_to_pml2_events(
            pd.DataFrame([_book_row(exchange_ts=exchange_ts, received_ts=received_ts)]),
            condition_id="condition-1",
            market_id="market-1",
            outcomes={"yes-token": Outcome.YES},
            book_epoch=0,
            feed_latency_ms=0,
        )


def test_xue_connection_local_sequences_do_not_create_false_global_gaps() -> None:
    exchange_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    rows = pd.DataFrame(
        [
            {
                **_book_row(
                    exchange_ts=exchange_ts,
                    received_ts=exchange_ts + timedelta(milliseconds=80),
                ),
                "event_type": "price_change",
                "collector_seq": 20,
                "raw_connection_id": "connection-20",
                "raw_frame_seq": 20,
                "group_id": "frame-20",
                "sequence_in_message": 0,
                "side": "BUY",
                "price": "0.40",
                "size": "8",
            },
            {
                **_book_row(
                    exchange_ts=exchange_ts + timedelta(milliseconds=1),
                    received_ts=exchange_ts + timedelta(milliseconds=81),
                ),
                "event_type": "price_change",
                "collector_seq": 5,
                "raw_connection_id": "connection-5",
                "raw_frame_seq": 5,
                "group_id": "frame-5",
                "sequence_in_message": 0,
                "side": "BUY",
                "price": "0.40",
                "size": "7",
            },
        ]
    )

    events = _xue_rows_to_pml2_events(
        rows,
        condition_id="condition-1",
        market_id="market-1",
        outcomes={"yes-token": Outcome.YES},
        book_epoch=0,
        feed_latency_ms=0,
    )

    assert all(isinstance(event, BookLevelBatchEvent) for event in events)
    assert [event.source_sequence for event in events] == [0, 0]
    assert [event.source_batch_sequence for event in events] == [0, 0]


def test_xue_adapter_carries_authoritative_top_in_one_raw_frame() -> None:
    exchange_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    row = {
        **_book_row(
            exchange_ts=exchange_ts,
            received_ts=exchange_ts + timedelta(milliseconds=80),
        ),
        "event_type": "price_change",
        "raw_connection_generation": 1,
        "collector_seq": 20,
        "sequence_in_message": 0,
        "change_index": 0,
        "side": "BUY",
        "price": "0.36",
        "size": "8",
        "best_bid": "0.36",
        "best_ask": "0.37",
    }

    events = _xue_rows_to_pml2_events(
        pd.DataFrame([row]),
        condition_id="condition-1",
        market_id="market-1",
        outcomes={"yes-token": Outcome.YES},
        book_epoch=0,
        feed_latency_ms=0,
        initial_books={
            "yes-token": (
                {Decimal("0.33"): Decimal(10)},
                {
                    Decimal("0.34"): Decimal(5),
                    Decimal("0.37"): Decimal(10),
                },
            )
        },
    )

    assert len(events) == 1
    frame = events[0]
    assert isinstance(frame, BookFrameBatchEvent)
    assert len(frame.batches) == 1
    batch = frame.batches[0]
    assert len(batch.updates) == 1
    explicit = batch.updates[0]
    assert (explicit.side, explicit.price, explicit.new_size) == (
        EconomicBookSide.BID,
        Decimal("0.36"),
        Decimal(8),
    )
    assert batch.authoritative_best_bid == Decimal("0.36")
    assert batch.authoritative_best_ask == Decimal("0.37")


def test_formal_adapter_rejects_multiple_groups_in_one_raw_frame() -> None:
    exchange_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    received_ts = exchange_ts + timedelta(milliseconds=80)
    rows = []
    for index, group_id in enumerate(("group-a", "group-b")):
        rows.append(
            {
                **_price_change_row(
                    exchange_ts=exchange_ts,
                    received_ts=received_ts,
                ),
                "group_id": group_id,
                "sequence_in_message": index,
                "change_index": index,
            }
        )

    with pytest.raises(L2ReplayNotReady, match="multiple atomic group ids"):
        _xue_rows_to_pml2_events(
            pd.DataFrame(rows),
            condition_id="condition-1",
            market_id="market-1",
            outcomes={"yes-token": Outcome.YES},
            book_epoch=0,
            feed_latency_ms=0,
            initial_books={
                "yes-token": (
                    {Decimal("0.40"): Decimal(10)},
                    {Decimal("0.50"): Decimal(10)},
                )
            },
            require_complete_top_hints=True,
        )


def test_formal_adapter_rejects_unmapped_asset_in_selected_global_frame() -> None:
    exchange_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    received_ts = exchange_ts + timedelta(milliseconds=80)
    known = _price_change_row(
        exchange_ts=exchange_ts,
        received_ts=received_ts,
    )
    unknown = {
        **known,
        "asset_id": "unexpected-token",
        "sequence_in_message": 1,
        "change_index": 1,
    }

    with pytest.raises(L2ReplayNotReady, match="unmapped asset"):
        _xue_rows_to_pml2_events(
            pd.DataFrame([known, unknown]),
            condition_id="condition-1",
            market_id="market-1",
            outcomes={"yes-token": Outcome.YES},
            book_epoch=0,
            feed_latency_ms=0,
            initial_books={
                "yes-token": (
                    {Decimal("0.40"): Decimal(10)},
                    {Decimal("0.50"): Decimal(10)},
                )
            },
            require_complete_top_hints=True,
        )


def test_xue_restore_preserves_late_received_exchange_clock_regression(
    tmp_path: Path,
) -> None:
    start = datetime(2026, 8, 10, 16, 0, 3, tzinfo=UTC)
    source_file = tmp_path / "slice.parquet"
    source_file.write_bytes(b"xue")

    class FakeReader:
        def create_checkpoint(
            self, *, asset_id: str, timestamp: datetime
        ) -> L2StateCheckpoint:
            return L2StateCheckpoint(
                asset_id=asset_id,
                timestamp=timestamp,
                latest_received_at=timestamp,
                latest_source="polymarket_market_ws_raw_a",
                market_id="market-1",
                bids=((Decimal("0.40"), Decimal(10)),),
                asks=((Decimal("0.50"), Decimal(10)),),
                row_count=1,
                generation=1,
                tick_size=Decimal("0.01"),
                has_ws_book=True,
                has_rest_seed_book=False,
                has_price_change=True,
                latest_book_is_rest=False,
                price_changes_after_latest_book=0,
                snapshot_version=f"baseline-{asset_id}",
                source_files=(str(source_file),),
            )

        def event_rows_between(self, **_: object) -> pd.DataFrame:
            row = _book_row(
                exchange_ts=start - timedelta(milliseconds=100),
                received_ts=start + timedelta(milliseconds=50),
            )
            row["event_type"] = "price_change"
            row["side"] = "BUY"
            row["price"] = "0.40"
            row["size"] = "8"
            row["best_bid"] = "0.40"
            row["best_ask"] = "0.50"
            return pd.DataFrame([row])

    restored = Pml2ArchiveSnapshotLoader(reader=FakeReader()).restore_condition_events(
        condition_id="condition-1",
        market_id="market-1",
        yes_asset_id="yes-token",
        no_asset_id="no-token",
        start_time=start,
        end_time=start + timedelta(seconds=1),
    )

    assert restored.restored is True
    frame = next(
        event
        for event in restored.events
        if isinstance(event, BookFrameBatchEvent)
    )
    assert frame.exchange_ts == start - timedelta(milliseconds=100)
    assert frame.source_received_ts == start + timedelta(milliseconds=50)


def _checkpoint(
    *,
    asset_id: str,
    timestamp: datetime,
    source_file: Path,
    exchange_ts: datetime | None,
    received_ts: datetime | None,
) -> L2StateCheckpoint:
    return L2StateCheckpoint(
        asset_id=asset_id,
        timestamp=timestamp,
        latest_received_at=received_ts or timestamp,
        latest_source="polymarket_market_ws_raw_a",
        market_id="market-1",
        bids=((Decimal("0.40"), Decimal(10)),),
        asks=((Decimal("0.50"), Decimal(10)),),
        row_count=1,
        generation=1,
        tick_size=Decimal("0.01"),
        has_ws_book=True,
        has_rest_seed_book=False,
        has_price_change=False,
        latest_book_is_rest=False,
        price_changes_after_latest_book=0,
        snapshot_version=f"baseline-{asset_id}",
        source_files=(str(source_file),),
        latest_book_exchange_ts=exchange_ts,
        latest_book_received_at=received_ts,
    )


def _price_change_row(
    *,
    exchange_ts: datetime,
    received_ts: datetime,
) -> dict[str, object]:
    return {
        **_book_row(exchange_ts=exchange_ts, received_ts=received_ts),
        "event_type": "price_change",
        "side": "BUY",
        "price": "0.40",
        "size": "8",
        "best_bid": "0.40",
        "best_ask": "0.50",
    }


def test_xue_restore_uses_true_checkpoint_clocks_and_aggregate_frame_proof(
    tmp_path: Path,
) -> None:
    start = datetime(2026, 8, 10, 16, 0, 3, tzinfo=UTC)
    source_file = tmp_path / "slice.parquet"
    source_file.write_bytes(b"xue")
    baseline_exchange = start - timedelta(milliseconds=200)
    baseline_received = start - timedelta(milliseconds=100)

    class FakeReader:
        def create_checkpoint(
            self, *, asset_id: str, timestamp: datetime
        ) -> L2StateCheckpoint:
            return _checkpoint(
                asset_id=asset_id,
                timestamp=timestamp,
                source_file=source_file,
                exchange_ts=baseline_exchange,
                received_ts=baseline_received,
            )

        def event_rows_between(self, **_: object) -> pd.DataFrame:
            row = _price_change_row(
                exchange_ts=start + timedelta(milliseconds=10),
                received_ts=start + timedelta(milliseconds=50),
            )
            # The requested asset is not the terminal flattened row. The
            # pre-projection aggregate still proves its raw frame/group closed.
            row["is_last_in_group"] = False
            row["raw_frame_complete"] = False
            return pd.DataFrame([row])

    restored = Pml2ArchiveSnapshotLoader(reader=FakeReader()).restore_condition_events(
        condition_id="condition-1",
        market_id="market-1",
        yes_asset_id="yes-token",
        no_asset_id="no-token",
        start_time=start,
        end_time=start + timedelta(seconds=1),
    )

    assert restored.restored is True
    assert restored.clock_verified is True
    assert restored.baseline_clock_count == 2
    assert restored.raw_event_clock_count == 1
    assert restored.frame_evidence_verified is True
    baselines = [
        event for event in restored.events if isinstance(event, BookSnapshotEvent)
    ]
    frame_event = next(
        event for event in restored.events if isinstance(event, BookFrameBatchEvent)
    )
    delta_event = frame_event.batches[0]
    assert all(event.exchange_ts == baseline_exchange for event in baselines)
    assert all(event.local_ts == baseline_received for event in baselines)
    assert all(event.source_received_ts == baseline_received for event in baselines)
    assert all(event.exchange_ts < delta_event.exchange_ts for event in baselines)


def test_xue_restore_does_not_upgrade_missing_checkpoint_clocks(
    tmp_path: Path,
) -> None:
    start = datetime(2026, 8, 10, 16, 0, 3, tzinfo=UTC)
    source_file = tmp_path / "slice.parquet"
    source_file.write_bytes(b"xue")

    class FakeReader:
        def create_checkpoint(
            self, *, asset_id: str, timestamp: datetime
        ) -> L2StateCheckpoint:
            return _checkpoint(
                asset_id=asset_id,
                timestamp=timestamp,
                source_file=source_file,
                exchange_ts=None,
                received_ts=None,
            )

        def event_rows_between(self, **_: object) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    _price_change_row(
                        exchange_ts=start + timedelta(milliseconds=10),
                        received_ts=start + timedelta(milliseconds=50),
                    )
                ]
            )

    restored = Pml2ArchiveSnapshotLoader(reader=FakeReader()).restore_condition_events(
        condition_id="condition-1",
        market_id="market-1",
        yes_asset_id="yes-token",
        no_asset_id="no-token",
        start_time=start,
        end_time=start + timedelta(seconds=1),
    )

    assert restored.restored is True
    assert restored.clock_verified is False
    baselines = [
        event for event in restored.events if isinstance(event, BookSnapshotEvent)
    ]
    assert all(event.exchange_ts == start for event in baselines)
    assert all(event.local_ts == start for event in baselines)


def test_xue_restore_rejects_missing_raw_frame_evidence(tmp_path: Path) -> None:
    start = datetime(2026, 8, 10, 16, 0, 3, tzinfo=UTC)
    source_file = tmp_path / "slice.parquet"
    source_file.write_bytes(b"xue")

    class FakeReader:
        def create_checkpoint(
            self, *, asset_id: str, timestamp: datetime
        ) -> L2StateCheckpoint:
            return _checkpoint(
                asset_id=asset_id,
                timestamp=timestamp,
                source_file=source_file,
                exchange_ts=start - timedelta(milliseconds=200),
                received_ts=start - timedelta(milliseconds=100),
            )

        def event_rows_between(self, **_: object) -> pd.DataFrame:
            row = _price_change_row(
                exchange_ts=start + timedelta(milliseconds=10),
                received_ts=start + timedelta(milliseconds=50),
            )
            del row["group_has_terminal"]
            return pd.DataFrame([row])

    restored = Pml2ArchiveSnapshotLoader(reader=FakeReader()).restore_condition_events(
        condition_id="condition-1",
        market_id="market-1",
        yes_asset_id="yes-token",
        no_asset_id="no-token",
        start_time=start,
        end_time=start + timedelta(seconds=1),
    )

    assert restored.restored is False
    assert "frame evidence columns missing" in restored.reason


def test_native_reader_aggregates_frame_proof_before_asset_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exchange_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    received_ts = exchange_ts + timedelta(milliseconds=80)
    writer = CompressedL2ArchiveWriter(
        base_dir=tmp_path,
        source="polymarket_market_ws_raw_a",
        shard_id=2,
        shard_count=48,
        flush_rows=100,
    )
    writer.insert_messages(
        [
            {
                "event_type": "price_change",
                "market": "condition-1",
                "timestamp": str(int(exchange_ts.timestamp() * 1000)),
                "price_changes": [
                    {
                        "asset_id": "yes-token",
                        "price": "0.40",
                        "size": "8",
                        "side": "BUY",
                    },
                    {
                        "asset_id": "no-token",
                        "price": "0.60",
                        "size": "8",
                        "side": "SELL",
                    },
                ],
                "_raw_connection_id": "raw-a-shard-2",
                "_raw_connection_generation": 3,
                "_raw_frame_seq": 17,
                "_raw_received_wall_ns": int(
                    received_ts.timestamp() * 1_000_000_000
                ),
                "_raw_message_index": 0,
                "_raw_group_id": "frame-17-group-0",
                "_raw_is_last_group_message": True,
                "_raw_is_last_archivable_message": True,
            }
        ]
    )
    writer.flush(force=True)
    monkeypatch.setattr(
        "quant.orderbook.l2_replay._coverage_route",
        lambda *_args, **_kwargs: (None, "condition-1", 2, 2, (2,)),
    )

    rows = L2ArchiveReplayReader(tmp_path).event_rows_between(
        asset_ids=("yes-token",),
        start_timestamp=exchange_ts,
        end_timestamp=received_ts + timedelta(milliseconds=1),
        event_types=("price_change",),
    )

    assert len(rows) == 2
    assert set(rows["asset_id"]) == {"yes-token", "no-token"}
    selected = rows.loc[rows["asset_id"] == "yes-token"].iloc[0]
    assert bool(selected["is_last_in_group"]) is False
    assert bool(selected["raw_frame_complete"]) is False
    assert bool(selected["group_has_terminal"]) is True
    assert bool(selected["frame_raw_complete"]) is True


def test_event_reader_does_not_open_unrelated_broken_shard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exchange_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    received_ts = exchange_ts + timedelta(milliseconds=80)
    writer = CompressedL2ArchiveWriter(
        base_dir=tmp_path,
        source="polymarket_market_ws_raw_a",
        shard_id=2,
        shard_count=48,
        flush_rows=100,
    )
    writer.insert_messages(
        [
            {
                "event_type": "price_change",
                "market": "condition-1",
                "timestamp": str(int(exchange_ts.timestamp() * 1000)),
                "price_changes": [
                    {
                        "asset_id": "yes-token",
                        "price": "0.40",
                        "size": "8",
                        "side": "BUY",
                    }
                ],
                "_raw_connection_id": "raw-a-shard-2",
                "_raw_connection_generation": 3,
                "_raw_frame_seq": 18,
                "_raw_received_wall_ns": int(
                    received_ts.timestamp() * 1_000_000_000
                ),
                "_raw_message_index": 0,
                "_raw_group_id": "frame-18-group-0",
                "_raw_is_last_group_message": True,
                "_raw_is_last_archivable_message": True,
            }
        ]
    )
    written = writer.flush(force=True)
    hour_dir = Path(written[0].path).parent
    (hour_dir / "l2_events_broken_shard0.parquet").write_bytes(b"broken")
    monkeypatch.setattr(
        "quant.orderbook.l2_replay._coverage_route",
        lambda *_args, **_kwargs: (None, "condition-1", 2, 1, (2,)),
    )

    rows = L2ArchiveReplayReader(tmp_path).event_rows_between(
        asset_ids=("yes-token",),
        start_timestamp=exchange_ts,
        end_timestamp=received_ts + timedelta(milliseconds=1),
        event_types=("price_change",),
    )

    assert len(rows) == 1
    assert rows.iloc[0]["asset_id"] == "yes-token"


def test_event_reader_fails_closed_when_routed_shard_is_broken(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exchange_ts = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    hour_dir = tmp_path / "dt=2026-08-10" / "hour=16"
    hour_dir.mkdir(parents=True)
    (hour_dir / "l2_events_broken_shard2.parquet").write_bytes(b"broken")
    monkeypatch.setattr(
        "quant.orderbook.l2_replay._coverage_route",
        lambda *_args, **_kwargs: (None, "condition-1", 2, 1, (2,)),
    )

    with pytest.raises(L2ReplayNotReady, match="event window read failed"):
        L2ArchiveReplayReader(tmp_path).event_rows_between(
            asset_ids=("yes-token",),
            start_timestamp=exchange_ts,
            end_timestamp=exchange_ts + timedelta(seconds=1),
            event_types=("price_change",),
        )


def test_event_reader_fails_closed_without_route_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    at = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    monkeypatch.setattr(
        "quant.orderbook.l2_replay._coverage_route",
        lambda *_args, **_kwargs: (None, "condition-1", None, None, ()),
    )

    with pytest.raises(L2ReplayNotReady, match="route is unproven"):
        L2ArchiveReplayReader(tmp_path).event_rows_between(
            asset_ids=("yes-token", "no-token"),
            start_timestamp=at,
            end_timestamp=at + timedelta(seconds=1),
        )


def _write_hash_bound_packet(
    root: Path,
    *,
    include_no: bool = True,
    complete_frames: bool = True,
) -> tuple[datetime, dict[str, str]]:
    start = datetime(2026, 8, 10, 16, 0, 3, tzinfo=UTC)
    writer = CompressedL2ArchiveWriter(
        base_dir=root,
        source="polymarket_market_ws_raw_a",
        shard_id=2,
        shard_count=48,
        flush_rows=100,
    )
    assets = ("yes-token", "no-token") if include_no else ("yes-token",)
    for frame_seq, asset_id in enumerate(assets, start=30):
        received = start - timedelta(seconds=1)
        writer.insert_messages(
            [
                {
                    "event_type": "book",
                    "asset_id": asset_id,
                    "market": "condition-1",
                    "timestamp": str(int(received.timestamp() * 1000)),
                    "bids": [{"price": "0.40", "size": "20"}],
                    "asks": [{"price": "0.50", "size": "10"}],
                    "_raw_connection_id": "raw-a-shard-2",
                    "_raw_connection_generation": 3,
                    "_raw_frame_seq": frame_seq,
                    "_raw_received_wall_ns": int(
                        received.timestamp() * 1_000_000_000
                    ),
                    "_raw_message_index": 0,
                    "_raw_group_id": f"frame-{frame_seq}-group-0",
                    "_raw_is_last_group_message": True,
                    "_raw_is_last_archivable_message": (
                        complete_frames or frame_seq != 30
                    ),
                }
            ]
        )
    event_received = start + timedelta(milliseconds=100)
    writer.insert_messages(
        [
            {
                "event_type": "price_change",
                "market": "condition-1",
                "timestamp": str(int(event_received.timestamp() * 1000)),
                "price_changes": [
                    {
                        "asset_id": asset_id,
                        "price": "0.40",
                        "size": "8",
                        "side": "BUY",
                        "best_bid": "0.40",
                        "best_ask": "0.50",
                    }
                    for asset_id in assets
                ],
                "_raw_connection_id": "raw-a-shard-2",
                "_raw_connection_generation": 3,
                "_raw_frame_seq": 40,
                "_raw_received_wall_ns": int(
                    event_received.timestamp() * 1_000_000_000
                ),
                "_raw_message_index": 0,
                "_raw_group_id": "frame-40-group-0",
                "_raw_is_last_group_message": True,
                "_raw_is_last_archivable_message": True,
            }
        ]
    )
    written = writer.flush(force=True)
    assert len(written) == 1
    path = Path(written[0].path)
    sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
    return start, {
        "path": path.relative_to(root).as_posix(),
        "sha256": sha256,
    }


def test_hash_bound_allowlist_bypasses_missing_route_after_full_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start, source_file = _write_hash_bound_packet(tmp_path)
    monkeypatch.setattr(
        "quant.backtest.pml2.adapters._load_transport_coverage_windows",
        lambda **_kwargs: (),
    )
    monkeypatch.setattr(
        "quant.orderbook.l2_replay._coverage_route",
        lambda *_args, **_kwargs: (None, "condition-1", None, None, ()),
    )

    restored = Pml2ArchiveSnapshotLoader(
        archive_dir=tmp_path
    ).restore_condition_events(
        condition_id="condition-1",
        market_id="market-1",
        yes_asset_id="yes-token",
        no_asset_id="no-token",
        start_time=start,
        end_time=start + timedelta(seconds=1),
        source_files=(source_file,),
    )

    assert restored.restored is True
    assert restored.clock_verified is True
    assert restored.source_binding == "HASH_BOUND_ALLOWLIST"
    assert restored.hash_bound_file_count == 1
    assert restored.source_file_sha256 == (
        (source_file["path"], source_file["sha256"]),
    )
    assert restored.reason == (
        "xue_native_l2_hash_bound_event_timeline_restore_complete"
    )


def test_hash_bound_allowlist_rejects_sha_mismatch(tmp_path: Path) -> None:
    start, source_file = _write_hash_bound_packet(tmp_path)
    source_file["sha256"] = "0" * 64

    restored = Pml2ArchiveSnapshotLoader(
        archive_dir=tmp_path
    ).restore_condition_events(
        condition_id="condition-1",
        market_id="market-1",
        yes_asset_id="yes-token",
        no_asset_id="no-token",
        start_time=start,
        end_time=start + timedelta(seconds=1),
        source_files=(source_file,),
    )

    assert restored.restored is False
    assert "SHA256 mismatch" in restored.reason


def test_hash_bound_allowlist_requires_both_target_tokens(tmp_path: Path) -> None:
    start, source_file = _write_hash_bound_packet(tmp_path, include_no=False)

    restored = Pml2ArchiveSnapshotLoader(
        archive_dir=tmp_path
    ).restore_condition_events(
        condition_id="condition-1",
        market_id="market-1",
        yes_asset_id="yes-token",
        no_asset_id="no-token",
        start_time=start,
        end_time=start + timedelta(seconds=1),
        source_files=(source_file,),
    )

    assert restored.restored is False
    assert "missing target asset_ids" in restored.reason
    assert "no-token" in restored.reason


def test_hash_bound_allowlist_requires_global_frame_completion(
    tmp_path: Path,
) -> None:
    start, source_file = _write_hash_bound_packet(
        tmp_path,
        complete_frames=False,
    )

    restored = Pml2ArchiveSnapshotLoader(
        archive_dir=tmp_path
    ).restore_condition_events(
        condition_id="condition-1",
        market_id="market-1",
        yes_asset_id="yes-token",
        no_asset_id="no-token",
        start_time=start,
        end_time=start + timedelta(seconds=1),
        source_files=(source_file,),
    )

    assert restored.restored is False
    assert "not globally frame/group complete" in restored.reason


def test_hash_bound_allowlist_rejects_path_escape(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    archive_root.mkdir()
    escaped = tmp_path / "outside.parquet"
    escaped.write_bytes(b"not trusted")

    restored = Pml2ArchiveSnapshotLoader(
        archive_dir=archive_root
    ).restore_condition_events(
        condition_id="condition-1",
        market_id="market-1",
        yes_asset_id="yes-token",
        no_asset_id="no-token",
        start_time=datetime(2026, 8, 10, 16, 0, tzinfo=UTC),
        end_time=datetime(2026, 8, 10, 16, 1, tzinfo=UTC),
        source_files=(
            {
                "path": str(escaped),
                "sha256": hashlib.sha256(escaped.read_bytes()).hexdigest(),
            },
        ),
    )

    assert restored.restored is False
    assert "must resolve under archive root" in restored.reason


def test_checkpoint_rejects_causal_inversion_and_future_receive_clock(
    tmp_path: Path,
) -> None:
    cutoff = datetime(2026, 8, 10, 16, 0, tzinfo=UTC)
    source_file = tmp_path / "slice.parquet"

    with pytest.raises(ValueError, match="cannot exceed received"):
        _checkpoint(
            asset_id="yes-token",
            timestamp=cutoff,
            source_file=source_file,
            exchange_ts=cutoff - timedelta(milliseconds=10),
            received_ts=cutoff - timedelta(milliseconds=20),
        )
    with pytest.raises(ValueError, match="cannot exceed checkpoint cutoff"):
        _checkpoint(
            asset_id="yes-token",
            timestamp=cutoff,
            source_file=source_file,
            exchange_ts=cutoff,
            received_ts=cutoff + timedelta(milliseconds=1),
        )
