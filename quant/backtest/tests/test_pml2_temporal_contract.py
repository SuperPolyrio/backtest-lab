from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from quant.backtest.backtest_engine import (
    PREDICTION_L2_REPLAY_V1_MODE,
    BacktestParameters,
    PricePoint,
    _fill_decision,
)
from quant.backtest.execution import BookSnapshot as EngineBookSnapshot
from quant.backtest.pml2.contracts import (
    BookDeltaEvent,
    BookFrameBatchEvent,
    BookLevel,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    BookSyncState,
    EconomicBookSide,
    OrderStatus,
    Outcome,
    Pml2OrderIntent,
    RawOrderSide,
    TimeInForce,
)
from quant.backtest.pml2.maker import MakerTemporalConflict
from quant.backtest.pml2.session import ReplayExecutionSession
from quant.backtest.pml2.strategy import DynamicPredictionReplay

T0 = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def _snapshot(
    *,
    at: datetime = T0,
    ask: str = "0.50",
    ask_size: str = "10",
    source: str = "native_l2",
    sequence: int | None = 1,
    snapshot_id: str = "snapshot-1",
) -> BookSnapshotEvent:
    return BookSnapshotEvent(
        snapshot_id=snapshot_id,
        condition_id="condition-1",
        market_id="market-1",
        asset_id="yes",
        outcome=Outcome.YES,
        exchange_ts=at,
        local_ts=at,
        book_epoch=0,
        bids=(BookLevel(Decimal("0.49"), Decimal(10)),),
        asks=(BookLevel(Decimal(ask), Decimal(ask_size)),),
        source=source,
        sequence=sequence,
        is_full_depth=True,
    )


def _delta(
    event_id: str,
    *,
    at: datetime,
    sequence: int,
    source: str,
    price: str = "0.50",
    new_size: str = "9",
) -> BookDeltaEvent:
    return BookDeltaEvent(
        event_id=event_id,
        condition_id="condition-1",
        market_id="market-1",
        asset_id="yes",
        outcome=Outcome.YES,
        exchange_ts=at,
        local_ts=at,
        book_epoch=0,
        side=EconomicBookSide.ASK,
        price=Decimal(price),
        new_size=Decimal(new_size),
        source=source,
        sequence=sequence,
    )


def _leg_snapshot(
    *,
    asset_id: str,
    outcome: Outcome,
    bids: tuple[tuple[str, str], ...],
    asks: tuple[tuple[str, str], ...],
    source: str = "native_l2",
    sequence: int | None = None,
) -> BookSnapshotEvent:
    return BookSnapshotEvent(
        snapshot_id=f"snapshot-{asset_id}",
        condition_id="condition-1",
        market_id="market-1",
        asset_id=asset_id,
        outcome=outcome,
        exchange_ts=T0,
        source_received_ts=T0,
        local_ts=T0,
        book_epoch=0,
        bids=tuple(BookLevel(Decimal(price), Decimal(size)) for price, size in bids),
        asks=tuple(BookLevel(Decimal(price), Decimal(size)) for price, size in asks),
        source=source,
        sequence=sequence,
        is_full_depth=True,
    )


def _frame_leg(
    *,
    frame_id: str,
    asset_id: str,
    outcome: Outcome,
    exchange_ts: datetime,
    received_ts: datetime,
    updates: tuple[tuple[EconomicBookSide, str, str], ...],
    best_bid: str,
    best_ask: str,
    source: str = "native_l2",
    source_sequence: int = 0,
    local_ts: datetime | None = None,
) -> BookLevelBatchEvent:
    common = {
        "condition_id": "condition-1",
        "market_id": "market-1",
        "asset_id": asset_id,
        "outcome": outcome,
        "exchange_ts": exchange_ts,
        "source_received_ts": received_ts,
        "local_ts": received_ts if local_ts is None else local_ts,
        "book_epoch": 0,
        "source": source,
    }
    return BookLevelBatchEvent(
        event_id=f"{frame_id}:{asset_id}",
        updates=tuple(
            BookDeltaEvent(
                event_id=f"{frame_id}:{asset_id}:{index}",
                side=side,
                price=Decimal(raw_price),
                new_size=Decimal(raw_size),
                sequence=index,
                **common,
            )
            for index, (side, raw_price, raw_size) in enumerate(updates)
        ),
        source_sequence=source_sequence,
        authoritative_best_bid=Decimal(best_bid),
        authoritative_best_ask=Decimal(best_ask),
        **common,
    )


def _raw_frame(
    frame_id: str,
    *,
    exchange_ts: datetime,
    received_ts: datetime,
    batches: tuple[BookLevelBatchEvent, ...],
    source: str = "native_l2",
    source_batch_sequence: int = 0,
    local_ts: datetime | None = None,
) -> BookFrameBatchEvent:
    return BookFrameBatchEvent(
        event_id=frame_id,
        condition_id="condition-1",
        market_id="market-1",
        exchange_ts=exchange_ts,
        source_received_ts=received_ts,
        local_ts=received_ts if local_ts is None else local_ts,
        book_epoch=0,
        batches=batches,
        source=source,
        source_batch_sequence=source_batch_sequence,
    )


def _order(
    order_id: str,
    *,
    submit_at: datetime,
    size: str = "5",
    limit: str = "0.55",
    tif: TimeInForce = TimeInForce.IOC,
    entry_latency_ms: int = 0,
    venue_delay_ms: int = 0,
) -> Pml2OrderIntent:
    return Pml2OrderIntent(
        run_id="run-temporal",
        order_id=order_id,
        strategy_id="strategy-temporal",
        condition_id="condition-1",
        market_id="market-1",
        asset_id="yes",
        outcome=Outcome.YES,
        side=RawOrderSide.BUY,
        size=Decimal(size),
        limit_price=Decimal(limit),
        tif=tif,
        signal_ts=submit_at,
        observed_ts=submit_at,
        submit_ts=submit_at,
        entry_latency_ms=entry_latency_ms,
        venue_delay_ms=venue_delay_ms,
        response_latency_ms=0,
    )


def test_contiguous_sequence_contract_fails_closed_on_jump() -> None:
    source = "contiguous_l2"
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
        sequence_step_by_source={source: 1},
    )
    session.ingest_snapshot(_snapshot(source=source, sequence=1))
    session.ingest_delta(
        _delta(
            "delta-2",
            at=T0 + timedelta(milliseconds=100),
            sequence=2,
            source=source,
        )
    )
    session.ingest_delta(
        _delta(
            "delta-4",
            at=T0 + timedelta(milliseconds=200),
            sequence=4,
            source=source,
        )
    )
    session.submit_order(_order("after-gap", submit_at=T0 + timedelta(seconds=1)))
    session.run()

    condition = session.exchange_book.condition("condition-1")
    assert condition.sync_state == BookSyncState.GAP
    assert any(
        "missing_sequence:delta-4:expected=3:actual=4" in item
        for item in condition.gap_event_ids
    )
    assert session.result("after-gap").status == OrderStatus.DATA_NOT_READY
    assert session.coverage_manifest().data_quality_status == "REJECTED"


def test_uncontracted_filtered_source_allows_legitimate_sequence_jump() -> None:
    source = "collector_wide_sequence_filtered_to_asset"
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
    )
    session.ingest_snapshot(_snapshot(source=source, sequence=1))
    session.ingest_delta(
        _delta(
            "delta-2",
            at=T0 + timedelta(milliseconds=100),
            sequence=2,
            source=source,
        )
    )
    session.ingest_delta(
        _delta(
            "delta-4",
            at=T0 + timedelta(milliseconds=200),
            sequence=4,
            source=source,
        )
    )
    session.run()

    condition = session.exchange_book.condition("condition-1")
    assert condition.sync_state == BookSyncState.SYNCED
    assert condition.gap_event_ids == []


def test_batch_uses_outer_source_sequence_not_resetting_inner_level_index() -> None:
    source = "xue_filtered_collector"
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
    )
    session.ingest_snapshot(_snapshot(source=source, sequence=None))
    for batch_index, source_sequence in enumerate((100, 105), start=1):
        at = T0 + timedelta(milliseconds=batch_index * 100)
        update = _delta(
            f"batch-{batch_index}-level-0",
            at=at,
            sequence=0,
            source=source,
            new_size=str(10 - batch_index),
        )
        session.ingest_level_batch(
            BookLevelBatchEvent(
                event_id=f"batch-{batch_index}",
                condition_id="condition-1",
                market_id="market-1",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=at,
                local_ts=at,
                book_epoch=0,
                updates=(update,),
                source=source,
                source_sequence=source_sequence,
            )
        )
    session.run()

    condition = session.exchange_book.condition("condition-1")
    assert condition.sync_state == BookSyncState.SYNCED
    assert condition.gap_event_ids == []


def test_global_frame_updates_both_legs_before_one_strategy_callback() -> None:
    session = ReplayExecutionSession(run_id="run-frame", profile="optimistic")
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="yes",
            outcome=Outcome.YES,
            bids=(("0.33", "10"),),
            asks=(("0.34", "5"), ("0.37", "10")),
        )
    )
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="no",
            outcome=Outcome.NO,
            bids=(("0.63", "10"), ("0.66", "5")),
            asks=(("0.67", "10"),),
        )
    )
    exchange_ts = T0 + timedelta(seconds=1)
    received_ts = exchange_ts + timedelta(milliseconds=50)
    frame = _raw_frame(
        "mixed-frame",
        exchange_ts=exchange_ts,
        received_ts=received_ts,
        batches=(
            _frame_leg(
                frame_id="mixed-frame",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=exchange_ts,
                received_ts=received_ts,
                updates=((EconomicBookSide.BID, "0.36", "8"),),
                best_bid="0.36",
                best_ask="0.37",
            ),
            _frame_leg(
                frame_id="mixed-frame",
                asset_id="no",
                outcome=Outcome.NO,
                exchange_ts=exchange_ts,
                received_ts=received_ts,
                updates=((EconomicBookSide.ASK, "0.64", "8"),),
                best_bid="0.63",
                best_ask="0.64",
            ),
        ),
    )

    class Strategy:
        strategy_id = "atomic-observer"

        def __init__(self) -> None:
            self.frame_calls = 0

        def on_book(self, ctx, event) -> None:
            if not isinstance(event, BookFrameBatchEvent):
                return
            self.frame_calls += 1
            assert (
                ctx.book.best("condition-1", EconomicBookSide.BID).canonical_yes_price
                == Decimal("0.3600000000")
            )
            assert (
                ctx.book.best("condition-1", EconomicBookSide.ASK).canonical_yes_price
                == Decimal("0.3700000000")
            )

    strategy = Strategy()
    replay = DynamicPredictionReplay(session=session, strategies=(strategy,))
    session.ingest_frame_batch(frame)
    replay.run()

    assert strategy.frame_calls == 1
    yes_bids, yes_asks = session.observed_book._raw_leg_levels(frame.batches[0])
    no_bids, no_asks = session.observed_book._raw_leg_levels(frame.batches[1])
    assert Decimal("0.34") not in yes_asks
    assert Decimal("0.66") not in no_bids
    assert max(yes_bids) == Decimal("0.36")
    assert min(no_asks) == Decimal("0.64")


def test_atomic_frame_does_not_record_transient_leg_mirror_skew() -> None:
    session = ReplayExecutionSession(run_id="run-frame", profile="strict")
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="yes",
            outcome=Outcome.YES,
            bids=(("0.33", "10"),),
            asks=(("0.50", "10"),),
        )
    )
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="no",
            outcome=Outcome.NO,
            bids=(("0.50", "10"),),
            asks=(("0.67", "10"),),
        )
    )
    exchange_ts = T0 + timedelta(seconds=1)
    frame = _raw_frame(
        "equal-final-mirror-frame",
        exchange_ts=exchange_ts,
        received_ts=exchange_ts,
        batches=(
            _frame_leg(
                frame_id="equal-final-mirror-frame",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=exchange_ts,
                received_ts=exchange_ts,
                updates=((EconomicBookSide.BID, "0.33", "400"),),
                best_bid="0.33",
                best_ask="0.50",
            ),
            _frame_leg(
                frame_id="equal-final-mirror-frame",
                asset_id="no",
                outcome=Outcome.NO,
                exchange_ts=exchange_ts,
                received_ts=exchange_ts,
                updates=((EconomicBookSide.ASK, "0.67", "400"),),
                best_bid="0.50",
                best_ask="0.67",
            ),
        ),
    )
    session.ingest_frame_batch(frame)
    session.run()

    assert session.exchange_book.mirror_mismatches == []
    level = session.exchange_book.best_level(
        "condition-1", EconomicBookSide.BID
    )
    assert level is not None
    assert level.displayed_size == Decimal("400.0000000000")
    assert level.available_size == Decimal("102.5000000000")
    assert level.mirror_size_skew == 0


def test_atomic_frame_records_one_genuine_final_mirror_skew() -> None:
    session = ReplayExecutionSession(run_id="run-frame", profile="realistic")
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="yes",
            outcome=Outcome.YES,
            bids=(("0.33", "10"),),
            asks=(("0.50", "10"),),
        )
    )
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="no",
            outcome=Outcome.NO,
            bids=(("0.50", "10"),),
            asks=(("0.67", "10"),),
        )
    )
    exchange_ts = T0 + timedelta(seconds=1)
    frame = _raw_frame(
        "skewed-final-mirror-frame",
        exchange_ts=exchange_ts,
        received_ts=exchange_ts,
        batches=(
            _frame_leg(
                frame_id="skewed-final-mirror-frame",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=exchange_ts,
                received_ts=exchange_ts,
                updates=((EconomicBookSide.BID, "0.33", "400"),),
                best_bid="0.33",
                best_ask="0.50",
            ),
            _frame_leg(
                frame_id="skewed-final-mirror-frame",
                asset_id="no",
                outcome=Outcome.NO,
                exchange_ts=exchange_ts,
                received_ts=exchange_ts,
                updates=((EconomicBookSide.ASK, "0.67", "200"),),
                best_bid="0.50",
                best_ask="0.67",
            ),
        ),
    )
    session.ingest_frame_batch(frame)
    session.run()

    assert len(session.exchange_book.mirror_mismatches) == 1
    mismatch = session.exchange_book.mirror_mismatches[0]
    assert mismatch["side"] == "BID"
    assert mismatch["canonical_yes_price"] == "0.3300000000"
    assert mismatch["size_skew"] == "0.5"


def test_raw_frames_follow_receive_order_without_rewriting_exchange_clock() -> None:
    session = ReplayExecutionSession(run_id="run-clock", profile="optimistic")
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="yes",
            outcome=Outcome.YES,
            bids=(("0.30", "10"),),
            asks=(("0.50", "10"),),
        )
    )
    first_exchange = T0 + timedelta(seconds=1)
    first_received = first_exchange + timedelta(milliseconds=100)
    second_exchange = first_exchange - timedelta(milliseconds=50)
    second_received = first_received + timedelta(milliseconds=100)
    first = _raw_frame(
        "receive-first",
        exchange_ts=first_exchange,
        received_ts=first_received,
        batches=(
            _frame_leg(
                frame_id="receive-first",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=first_exchange,
                received_ts=first_received,
                updates=((EconomicBookSide.BID, "0.40", "10"),),
                best_bid="0.40",
                best_ask="0.50",
            ),
        ),
        source_batch_sequence=1,
    )
    second = _raw_frame(
        "receive-second-exchange-regression",
        exchange_ts=second_exchange,
        received_ts=second_received,
        batches=(
            _frame_leg(
                frame_id="receive-second-exchange-regression",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=second_exchange,
                received_ts=second_received,
                updates=((EconomicBookSide.BID, "0.41", "10"),),
                best_bid="0.41",
                best_ask="0.50",
            ),
        ),
        source_batch_sequence=2,
    )
    session.ingest_frame_batch(first)
    session.ingest_frame_batch(second)
    session.run()

    best = session.exchange_book.best_level(
        "condition-1",
        EconomicBookSide.BID,
    )
    assert best is not None
    assert best.key.canonical_yes_price == Decimal("0.4100000000")
    assert second.exchange_ts < first.exchange_ts
    assert session.exchange_book.condition("condition-1").last_exchange_ts == (
        second_exchange
    )


def test_second_leg_sequence_rejection_rolls_back_frame_and_hides_it() -> None:
    source = "contiguous-frame-source"
    session = ReplayExecutionSession(
        run_id="run-frame-rollback",
        profile="optimistic",
        sequence_step_by_source={source: 1},
    )
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="yes",
            outcome=Outcome.YES,
            bids=(("0.33", "10"),),
            asks=(("0.37", "10"),),
            source=source,
            sequence=1,
        )
    )
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="no",
            outcome=Outcome.NO,
            bids=(("0.63", "10"),),
            asks=(("0.67", "10"),),
            source=source,
            sequence=1,
        )
    )
    exchange_ts = T0 + timedelta(seconds=1)
    received_ts = exchange_ts + timedelta(milliseconds=10)
    frame = _raw_frame(
        "rejected-frame",
        exchange_ts=exchange_ts,
        received_ts=received_ts,
        source=source,
        batches=(
            _frame_leg(
                frame_id="rejected-frame",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=exchange_ts,
                received_ts=received_ts,
                updates=((EconomicBookSide.BID, "0.36", "8"),),
                best_bid="0.36",
                best_ask="0.37",
                source=source,
                source_sequence=2,
            ),
            _frame_leg(
                frame_id="rejected-frame",
                asset_id="no",
                outcome=Outcome.NO,
                exchange_ts=exchange_ts,
                received_ts=received_ts,
                updates=((EconomicBookSide.ASK, "0.64", "8"),),
                best_bid="0.63",
                best_ask="0.64",
                source=source,
                # The second leg repeats its baseline sequence and rejects.
                source_sequence=1,
            ),
        ),
    )

    class Strategy:
        strategy_id = "must-not-see-rejected-frame"

        def __init__(self) -> None:
            self.frame_calls = 0

        def on_book(self, _ctx, event) -> None:
            self.frame_calls += int(isinstance(event, BookFrameBatchEvent))

    strategy = Strategy()
    replay = DynamicPredictionReplay(session=session, strategies=(strategy,))
    session.ingest_frame_batch(frame)
    replay.run()

    for book in (session.exchange_book, session.observed_book):
        condition = book.condition("condition-1")
        assert condition.sync_state == BookSyncState.GAP
        yes_leg = book._leg_key(
            "condition-1", 0, source, "yes", Outcome.YES
        )
        assert condition.last_sequence_by_leg[yes_leg] == 1
        yes_bids, _ = book._raw_leg_levels(frame.batches[0])
        _, no_asks = book._raw_leg_levels(frame.batches[1])
        assert Decimal("0.36") not in yes_bids
        assert Decimal("0.64") not in no_asks
    assert frame.event_id not in session.observed_event_ids
    assert strategy.frame_calls == 0


def test_exchange_rejected_frame_cannot_publish_under_local_reordering() -> None:
    source = "contiguous-reordered-frame-source"
    session = ReplayExecutionSession(
        run_id="run-frame-local-reorder",
        profile="optimistic",
        sequence_step_by_source={source: 1},
    )
    session.ingest_snapshot(
        _leg_snapshot(
            asset_id="yes",
            outcome=Outcome.YES,
            bids=(("0.33", "10"),),
            asks=(("0.50", "10"),),
            source=source,
            sequence=1,
        )
    )
    accepted_exchange_ts = T0 + timedelta(seconds=1)
    accepted_received_ts = accepted_exchange_ts + timedelta(milliseconds=100)
    accepted_local_ts = T0 + timedelta(seconds=10)
    accepted = _raw_frame(
        "exchange-accepted-local-late",
        exchange_ts=accepted_exchange_ts,
        received_ts=accepted_received_ts,
        source=source,
        batches=(
            _frame_leg(
                frame_id="exchange-accepted-local-late",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=accepted_exchange_ts,
                received_ts=accepted_received_ts,
                updates=((EconomicBookSide.BID, "0.40", "10"),),
                best_bid="0.40",
                best_ask="0.50",
                source=source,
                source_sequence=2,
                local_ts=accepted_local_ts,
            ),
        ),
        source_batch_sequence=1,
        local_ts=accepted_local_ts,
    )
    rejected_exchange_ts = T0 + timedelta(seconds=2)
    rejected_received_ts = rejected_exchange_ts
    rejected = _raw_frame(
        "exchange-rejected-local-first",
        exchange_ts=rejected_exchange_ts,
        received_ts=rejected_received_ts,
        source=source,
        batches=(
            _frame_leg(
                frame_id="exchange-rejected-local-first",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=rejected_exchange_ts,
                received_ts=rejected_received_ts,
                updates=((EconomicBookSide.BID, "0.41", "10"),),
                best_bid="0.41",
                best_ask="0.50",
                source=source,
                # Duplicate sequence: exchange has already accepted sequence 2.
                source_sequence=2,
            ),
        ),
        source_batch_sequence=2,
    )

    class Strategy:
        strategy_id = "must-not-see-exchange-rejected-frame"

        def __init__(self) -> None:
            self.frame_ids: list[str] = []

        def on_book(self, _ctx, event) -> None:
            if isinstance(event, BookFrameBatchEvent):
                self.frame_ids.append(event.event_id)

    strategy = Strategy()
    replay = DynamicPredictionReplay(session=session, strategies=(strategy,))
    session.ingest_frame_batch(accepted)
    session.ingest_frame_batch(rejected)
    replay.run()

    assert rejected.event_id not in session.observed_event_ids
    assert rejected.event_id not in strategy.frame_ids
    assert strategy.frame_ids == []
    observed = session.observed_book.condition("condition-1")
    assert observed.sync_state == BookSyncState.GAP
    assert any(
        f"exchange_rejected_raw_frame:{rejected.event_id}" in event_id
        for event_id in observed.gap_event_ids
    )


def test_session_rejects_scheduling_before_processed_replay_clock() -> None:
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
    )
    session.ingest_snapshot(_snapshot(at=T0 + timedelta(seconds=10)))
    session.run()

    try:
        session.submit_order(
            _order("past-order", submit_at=T0 + timedelta(seconds=1))
        )
    except ValueError as exc:
        assert "before the current replay clock" in str(exc)
    else:  # pragma: no cover - explicit time-travel assertion
        raise AssertionError("past event was scheduled after replay advanced")


def test_late_decrease_fails_closed_for_mixed_age_maker_queue() -> None:
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
    )
    old_state = session.maker_queues.admit(
        _order(
            "old-maker",
            submit_at=T0,
            tif=TimeInForce.GTC,
            limit="0.49",
        ),
        accepted_ts=T0 + timedelta(milliseconds=100),
        book_epoch=0,
        displayed_external_size=Decimal(10),
    )
    future_state = session.maker_queues.admit(
        _order(
            "future-maker",
            submit_at=T0,
            tif=TimeInForce.GTC,
            limit="0.49",
        ),
        accepted_ts=T0 + timedelta(seconds=2),
        book_epoch=0,
        displayed_external_size=Decimal(10),
    )
    queue = session.maker_queues.queues[old_state.key]
    before_external = queue.external_queue_ahead
    before_future_ahead = future_state.queue_ahead

    try:
        session.maker_queues.on_unmatched_book_decrease(
            old_state.key,
            Decimal(5),
            event_ts=T0 + timedelta(seconds=1),
        )
    except MakerTemporalConflict:
        pass
    else:  # pragma: no cover - explicit mixed-age queue assertion
        raise AssertionError("late decrease mutated a mixed-age maker queue")

    assert queue.external_queue_ahead == before_external
    assert future_state.queue_ahead == before_future_ahead


def test_market_data_wins_order_arrival_at_same_exchange_timestamp() -> None:
    arrival = T0 + timedelta(seconds=1)
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
    )
    session.ingest_snapshot(_snapshot())
    session.submit_order(
        _order(
            "same-time",
            submit_at=T0,
            limit="0.55",
            entry_latency_ms=1000,
        )
    )
    session.ingest_snapshot(
        _snapshot(
            at=arrival,
            ask="0.60",
            sequence=2,
            snapshot_id="replacement-at-arrival",
        )
    )
    session.run()

    result = session.result("same-time")
    assert result.status == OrderStatus.CANCELLED
    assert result.filled_size == 0
    assert session.coverage_manifest().event_order_policy == (
        "pml2_market_data_before_order_arrival_v2"
    )


def test_legacy_order_first_tie_policy_requires_explicit_opt_out() -> None:
    arrival = T0 + timedelta(seconds=1)
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
        market_data_first_on_tie=False,
    )
    session.ingest_snapshot(_snapshot())
    session.submit_order(
        _order(
            "same-time-legacy",
            submit_at=T0,
            limit="0.55",
            entry_latency_ms=1000,
        )
    )
    session.ingest_snapshot(
        _snapshot(
            at=arrival,
            ask="0.60",
            sequence=2,
            snapshot_id="replacement-at-arrival",
        )
    )
    session.run()

    result = session.result("same-time-legacy")
    assert result.status == OrderStatus.FILLED
    assert result.avg_fill_price == Decimal("0.5000000000")
    assert session.coverage_manifest().event_order_policy == (
        "pml2_contract_priority_legacy_v1"
    )


def test_ioc_partially_fills_and_immediately_cancels_remainder() -> None:
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
    )
    session.ingest_snapshot(_snapshot(ask_size="3"))
    session.submit_order(
        _order(
            "ioc-partial",
            submit_at=T0 + timedelta(seconds=1),
            size="5",
            limit="0.50",
        )
    )
    session.run()

    result = session.result("ioc-partial")
    assert result.status == OrderStatus.PARTIAL
    assert result.filled_size == Decimal("3.0000000000")
    assert result.remaining_size == Decimal("2.0000000000")
    assert result.reason == "ioc_remainder_cancelled"


def test_taker_match_preserves_arrival_and_venue_execution_clocks() -> None:
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
    )
    session.ingest_snapshot(_snapshot())
    submit_at = T0 + timedelta(seconds=1)
    session.submit_order(
        _order(
            "venue-delayed-ioc",
            submit_at=submit_at,
            size="1",
            limit="0.50",
            entry_latency_ms=100,
            venue_delay_ms=250,
        )
    )
    session.run()

    result = session.result("venue-delayed-ioc")
    assert result.status == OrderStatus.FILLED
    assert result.exchange_arrival_ts == submit_at + timedelta(milliseconds=100)
    assert len(result.fills) == 1
    match = result.fills[0]
    assert match.exchange_arrival_ts == submit_at + timedelta(milliseconds=100)
    assert match.fill_exchange_ts == submit_at + timedelta(milliseconds=350)
    assert match.fill_exchange_ts > match.exchange_arrival_ts
    assert match.evidence_kind == "L2_VISIBLE_DEPTH_AT_VENUE_EXECUTION"


def test_cold_restore_without_exchange_clock_proof_is_reported_as_degraded() -> None:
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
        cold_restore_used=True,
    )
    session.ingest_snapshot(_snapshot())
    session.run()

    report = session.report()
    assert report["coverage_manifest"]["data_quality_status"] == (
        "VALID_WITH_DEGRADATION"
    )
    assert report["temporal_contract"]["cold_restore_exchange_clock_status"] == (
        "UNVERIFIED_CHECKPOINT_CUTOFF_PROXY"
    )


def test_cold_restore_caller_boolean_cannot_spoof_clock_verification() -> None:
    try:
        ReplayExecutionSession(
            run_id="run-temporal",
            profile="optimistic",
            cold_restore_used=True,
            cold_restore_exchange_clock_verified=True,  # type: ignore[call-arg]
        )
    except TypeError as exc:
        assert "cold_restore_exchange_clock_verified" in str(exc)
    else:  # pragma: no cover - explicit anti-forgery assertion
        raise AssertionError("caller-provided clock verification was accepted")


def test_cold_restore_unlock_requires_complete_aggregate_archive_receipt() -> None:
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
        cold_restore_used=True,
    )

    try:
        session.register_verified_cold_restore_clock_evidence(
            restore_count=1,
            baseline_clock_count=2,
            raw_event_clock_count=1,
            frame_evidence_count=0,
            source_manifest_hash="archive-manifest",
        )
    except ValueError as exc:
        assert "frame evidence is incomplete" in str(exc)
    else:  # pragma: no cover - explicit anti-forgery assertion
        raise AssertionError("incomplete frame evidence unlocked cold restore")

    session.ingest_snapshot(_snapshot())
    session.run()
    report = session.report()
    assert report["temporal_contract"]["cold_restore_exchange_clock_status"] == (
        "UNVERIFIED_CHECKPOINT_CUTOFF_PROXY"
    )
    assert report["temporal_contract"]["cold_restore_clock_evidence"] is None


def test_cold_restore_unlock_records_aggregate_archive_evidence() -> None:
    session = ReplayExecutionSession(
        run_id="run-temporal",
        profile="optimistic",
        cold_restore_used=True,
    )
    session.register_verified_cold_restore_clock_evidence(
        restore_count=1,
        baseline_clock_count=2,
        raw_event_clock_count=1,
        frame_evidence_count=1,
        source_manifest_hash="archive-manifest",
    )
    session.ingest_snapshot(_snapshot())
    session.run()

    report = session.report()
    evidence = report["temporal_contract"]["cold_restore_clock_evidence"]
    assert report["temporal_contract"]["cold_restore_exchange_clock_status"] == (
        "VERIFIED"
    )
    assert report["coverage_manifest"]["data_quality_status"] == "VALID"
    assert evidence["baseline_clock_count"] == 2
    assert evidence["raw_event_clock_count"] == 1
    assert evidence["frame_evidence_count"] == 1
    assert evidence["source_manifest_hash"] == "archive-manifest"
    assert len(evidence["evidence_hash"]) == 64


class _RecordingProvider:
    def __init__(self) -> None:
        self.targets: list[datetime] = []

    def snapshot_at(self, target: datetime) -> EngineBookSnapshot:
        self.targets.append(target)
        return EngineBookSnapshot(
            snapshot_id=len(self.targets),
            token_id="yes",
            side="YES",
            bids=((Decimal("0.49"), Decimal(10)),),
            asks=((Decimal("0.50"), Decimal(10)),),
            source="pmxt_compact",
            timestamp=target,
            snapshot_version=f"engine-{len(self.targets)}",
            is_full_depth=True,
        )


def _main_engine_run(provider: _RecordingProvider) -> dict[str, object]:
    return {
        "run_id": "engine-temporal",
        "market_id": "market-1",
        "market_slug": "market-1",
        "token_side": "YES",
        "_pmxt_compact_book_provider": provider,
        "_pml2_token_context": {
            "condition_id": "condition-1",
            "market_id": "market-1",
            "token_id": "yes",
            "token_side": "YES",
        },
    }


def test_main_engine_zero_frontend_latency_preserves_profile_latency() -> None:
    provider = _RecordingProvider()
    run = _main_engine_run(provider)
    signal_ts = T0 + timedelta(seconds=1)
    params = BacktestParameters(
        execution_price_mode=PREDICTION_L2_REPLAY_V1_MODE,
        execution_profile="realistic",
        order_role="taker",
        latency_seconds=Decimal(0),
        max_entry_price=Decimal("0.50"),
        allow_partial_fill=True,
    )

    _fill_decision(
        params,
        PricePoint(1, Decimal("0.50"), Decimal(1), timestamp=signal_ts),
        run,
        "BUY_YES",
        target_size=Decimal(1),
    )

    result = run["_pml2_session"].results()[0]
    assert provider.targets == [signal_ts + timedelta(milliseconds=200)]
    assert result.order.entry_latency_ms is None
    assert result.exchange_arrival_ts == signal_ts + timedelta(milliseconds=200)
    assert result.order.tif == TimeInForce.IOC


def test_main_engine_positive_frontend_latency_is_explicit_override() -> None:
    provider = _RecordingProvider()
    run = _main_engine_run(provider)
    signal_ts = T0 + timedelta(seconds=1)
    params = BacktestParameters(
        execution_price_mode=PREDICTION_L2_REPLAY_V1_MODE,
        execution_profile="realistic",
        order_role="taker",
        latency_seconds=Decimal("0.075"),
        max_entry_price=Decimal("0.50"),
        allow_partial_fill=True,
    )

    _fill_decision(
        params,
        PricePoint(1, Decimal("0.50"), Decimal(1), timestamp=signal_ts),
        run,
        "BUY_YES",
        target_size=Decimal(1),
    )

    result = run["_pml2_session"].results()[0]
    assert provider.targets == [signal_ts + timedelta(milliseconds=75)]
    assert result.order.entry_latency_ms == 75
    assert result.exchange_arrival_ts == signal_ts + timedelta(milliseconds=75)
