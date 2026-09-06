from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from flask import Flask

from quant.backtest.backtest_engine import (
    PREDICTION_L2_REPLAY_V1_MODE,
    BacktestParameters,
    PricePoint,
    _fill_decision,
    is_prediction_l2_replay_v1_mode,
    normalize_execution_price_mode,
    parse_parameters,
)
from quant.backtest.execution import BookSnapshot as EngineBookSnapshot
from quant.backtest.l2_orderfilled_execution import (
    BookLevel as LegacyBookLevel,
)
from quant.backtest.l2_orderfilled_execution import (
    BookSnapshot as LegacyBookSnapshot,
)
from quant.backtest.l2_orderfilled_execution import (
    simulate_l2_depth_execution,
)
from quant.backtest.pml2.contracts import (
    BookDeltaEvent,
    BookLevel,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    CtfMatchType,
    EconomicAction,
    EconomicBookSide,
    MarketLifecycleEvent,
    MatchFinalityState,
    OrderAmountUnit,
    OrderGroupIntent,
    OrderGroupPolicy,
    OrderStatus,
    Outcome,
    Pml2OrderIntent,
    RawOrderSide,
    ReplayAuditMode,
    TimeInForce,
    TradeEvent,
    TradingMode,
    VenueAdmissionStatus,
)
from quant.backtest.pml2.financial import (
    PredictionPortfolioLedger,
    SettlementLifecycle,
    default_execution_adapter_registry,
)
from quant.backtest.pml2.profiles import get_pml2_profile
from quant.backtest.pml2.service import (
    Pml2DataNotReadyError,
    Pml2RequestError,
    build_pml2_readiness,
    run_pml2_maker_forecast,
    run_pml2_profile_matrix,
    run_pml2_replay,
)
from quant.backtest.pml2.session import ReplayExecutionSession
from quant.backtest.pml2.strategy import DynamicPredictionReplay
from quant.settlement.payout_vector import PayoutVector
from quant.simulator.economics import FeeSchedule, FeeScheduleRegistry
from scripts.api.routes import quant as quant_routes

T0 = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def _snapshot(
    *,
    snapshot_id: str = "snap-1",
    outcome: Outcome = Outcome.YES,
    asset_id: str | None = None,
    bids: tuple[tuple[str, str], ...] = (("0.49", "10"),),
    asks: tuple[tuple[str, str], ...] = (("0.50", "10"),),
    at: datetime = T0,
    local_at: datetime | None = None,
    source: str = "native_l2",
    epoch: int = 0,
    sequence: int = 0,
    is_full_depth: bool = True,
    is_truncated: bool = False,
    tick_size: str | None = None,
    min_order_size: str | None = None,
) -> BookSnapshotEvent:
    return BookSnapshotEvent(
        snapshot_id=snapshot_id,
        condition_id="condition-1",
        market_id="market-1",
        asset_id=asset_id or outcome.value.lower(),
        outcome=outcome,
        exchange_ts=at,
        local_ts=local_at or at,
        book_epoch=epoch,
        bids=tuple(BookLevel(Decimal(price), Decimal(size)) for price, size in bids),
        asks=tuple(BookLevel(Decimal(price), Decimal(size)) for price, size in asks),
        source=source,
        sequence=sequence,
        is_full_depth=is_full_depth,
        is_truncated=is_truncated,
        depth_scope="FULL" if is_full_depth else "TOP_N",
        tick_size=None if tick_size is None else Decimal(tick_size),
        min_order_size=(
            None if min_order_size is None else Decimal(min_order_size)
        ),
    )


def _order(
    order_id: str,
    *,
    side: RawOrderSide = RawOrderSide.BUY,
    outcome: Outcome = Outcome.YES,
    asset_id: str | None = None,
    size: str = "5",
    limit: str = "0.50",
    tif: TimeInForce = TimeInForce.FAK,
    at: datetime = T0 + timedelta(seconds=1),
    post_only: bool = False,
    expires_at: datetime | None = None,
    fee_rate: str = "0",
    amount_unit: OrderAmountUnit = OrderAmountUnit.SHARES,
    signed_maker_amount: str | None = None,
    signed_taker_amount: str | None = None,
    venue_admission: VenueAdmissionStatus = VenueAdmissionStatus.UNKNOWN,
    venue_admission_evidence_id: str | None = None,
) -> Pml2OrderIntent:
    return Pml2OrderIntent(
        run_id="run-1",
        order_id=order_id,
        strategy_id="strategy-1",
        condition_id="condition-1",
        market_id="market-1",
        asset_id=asset_id or outcome.value.lower(),
        outcome=outcome,
        side=side,
        size=Decimal(size),
        limit_price=Decimal(limit),
        tif=tif,
        signal_ts=at,
        observed_ts=at,
        submit_ts=at,
        amount_unit=amount_unit,
        signed_maker_amount=(
            None if signed_maker_amount is None else Decimal(signed_maker_amount)
        ),
        signed_taker_amount=(
            None if signed_taker_amount is None else Decimal(signed_taker_amount)
        ),
        venue_admission=venue_admission,
        venue_admission_evidence_id=(
            venue_admission_evidence_id
            or (
                f"test-venue-response:{order_id}"
                if venue_admission != VenueAdmissionStatus.UNKNOWN
                else ""
            )
        ),
        post_only=post_only,
        expires_at=expires_at,
        entry_latency_ms=0,
        cancel_latency_ms=0,
        response_latency_ms=0,
        venue_delay_ms=0,
        fee_rate=Decimal(fee_rate),
    )


def _trade(
    event_id: str,
    *,
    side: RawOrderSide,
    price: str,
    size: str,
    at: datetime,
    outcome: Outcome = Outcome.YES,
    asset_id: str | None = None,
    local_at: datetime | None = None,
    sequence: int = 0,
    group: str = "",
) -> TradeEvent:
    return TradeEvent(
        event_id=event_id,
        condition_id="condition-1",
        market_id="market-1",
        asset_id=asset_id or outcome.value.lower(),
        outcome=outcome,
        exchange_ts=at,
        local_ts=local_at or at,
        book_epoch=0,
        price=Decimal(price),
        size=Decimal(size),
        aggressor_side=side,
        source="clob_trade",
        source_sequence=sequence,
        event_group_id=group,
    )


def _session(profile: str = "realistic", *, cold: bool = False) -> ReplayExecutionSession:
    return ReplayExecutionSession(run_id="run-1", profile=profile, cold_restore_used=cold)


def _filled_match():
    session = _session()
    session.ingest_snapshot(_snapshot())
    session.submit_order(_order("buy", size="2"))
    session.run()
    return session.result("buy").fills[0]


def test_01_taker_does_not_require_future_trade() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    session.submit_order(_order("o-1"))
    session.run()

    assert session.result("o-1").status == OrderStatus.FILLED
    assert not any(event["event_type"] == "TRADE" for event in session.audit_events)


def test_02_taker_orders_share_run_scoped_capacity() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(asks=(("0.50", "10"),)))
    session.submit_order(_order("o-1", size="6"))
    session.submit_order(_order("o-2", size="6", at=T0 + timedelta(seconds=2)))
    session.run()

    assert session.result("o-1").filled_size == Decimal("6.0000000000")
    assert session.result("o-2").filled_size == Decimal("4.0000000000")


def test_03_yes_buy_and_no_sell_share_mirror_capacity() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(asks=(("0.60", "10"),)))
    session.ingest_snapshot(
        _snapshot(
            snapshot_id="snap-no",
            outcome=Outcome.NO,
            bids=(("0.40", "10"),),
            asks=(("0.41", "10"),),
        )
    )
    session.submit_order(_order("yes-buy", size="6", limit="0.60"))
    session.submit_order(
        _order(
            "no-sell",
            outcome=Outcome.NO,
            side=RawOrderSide.SELL,
            size="6",
            limit="0.39",
            at=T0 + timedelta(seconds=2),
        )
    )
    session.run()

    assert session.result("yes-buy").filled_size == Decimal("6.0000000000")
    assert session.result("no-sell").filled_size == Decimal("4.0000000000")


def test_04_limit_is_checked_on_every_child_fill() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    session.submit_order(_order("o-1", limit="0.49"))
    session.run()

    assert session.result("o-1").filled_size == 0
    assert session.result("o-1").status == OrderStatus.CANCELLED


def test_05_fok_preview_is_atomic() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(asks=(("0.50", "8"),)))
    session.submit_order(_order("fok", size="9", tif=TimeInForce.FOK))
    session.submit_order(
        _order("after", size="8", at=T0 + timedelta(seconds=2))
    )
    session.run()

    assert session.result("fok").status == OrderStatus.REJECTED
    assert session.result("after").filled_size == Decimal("8.0000000000")


def test_06_fak_partially_fills_then_cancels_remainder() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(asks=(("0.50", "8"),)))
    session.submit_order(_order("fak", size="10", tif=TimeInForce.FAK))
    session.run()

    result = session.result("fak")
    assert result.status == OrderStatus.PARTIAL
    assert result.filled_size == Decimal("8.0000000000")
    assert result.remaining_size == Decimal("2.0000000000")


def test_06b_fak_large_order_keeps_visible_partial_fill() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(asks=(("0.50", "3"),)))
    session.submit_order(_order("large-fak", size="10", tif=TimeInForce.FAK))
    session.run()

    result = session.result("large-fak")
    assert result.status == OrderStatus.PARTIAL
    assert result.filled_size == Decimal("3.0000000000")
    assert result.reason == "fak_remainder_cancelled"
    assert result.counterfactual_impact == {
        "gate_version": "pml2-visible-depth-impact-v2",
        "requested_size": "10.0000000000",
        "visible_depth_within_limit": "3.0000000000",
        "order_to_visible_depth_ratio": "3.333333333333333333333333333",
        "warning_ratio": "3.00",
        "decision": "VISIBLE_DEPTH_ONLY_REMAINDER_UNMODELED",
        "impact_policy": "EXECUTE_VISIBLE_DEPTH_ONLY",
    }


def test_06c_fok_large_order_still_rejects_without_consuming_depth() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(asks=(("0.50", "3"),)))
    session.submit_order(_order("large-fok", size="10", tif=TimeInForce.FOK))
    session.submit_order(
        _order("after-large-fok", size="2", at=T0 + timedelta(seconds=2))
    )
    session.run()

    rejected = session.result("large-fok")
    after = session.result("after-large-fok")
    assert rejected.status == OrderStatus.REJECTED
    assert rejected.filled_size == 0
    assert rejected.reason == "fok_insufficient_economic_residual"
    assert rejected.counterfactual_impact["decision"] == (
        "VISIBLE_DEPTH_ONLY_REMAINDER_UNMODELED"
    )
    assert after.filled_size == Decimal("2.0000000000")


def test_07_post_only_cross_is_rejected() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    session.submit_order(
        _order("post", tif=TimeInForce.GTC, post_only=True, limit="0.50")
    )
    session.run()

    assert session.result("post").status == OrderStatus.REJECTED


def test_08_gap_fails_closed_until_new_snapshot() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    session.ingest_delta(
        BookDeltaEvent(
            event_id="bad-epoch",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            exchange_ts=T0 + timedelta(milliseconds=500),
            local_ts=T0 + timedelta(milliseconds=500),
            book_epoch=1,
            side=EconomicBookSide.ASK,
            price=Decimal("0.50"),
            new_size=Decimal(9),
            source="native_l2",
        )
    )
    session.submit_order(_order("gap"))
    session.run()

    assert session.result("gap").status == OrderStatus.DATA_NOT_READY
    assert "gap" in session.result("gap").reason


def test_09_delayed_order_uses_exchange_book_at_arrival() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    session.ingest_snapshot(
        _snapshot(
            snapshot_id="snap-2",
            asks=(("0.70", "10"),),
            at=T0 + timedelta(milliseconds=100),
        )
    )
    delayed = replace(
        _order("delayed", limit="0.60", at=T0 + timedelta(milliseconds=50)),
        entry_latency_ms=100,
    )
    session.submit_order(delayed)
    session.run()

    assert session.result("delayed").filled_size == 0


def test_10_wrong_side_trade_does_not_advance_maker() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(bids=(("0.49", "1"),)))
    session.submit_order(
        _order(
            "maker",
            side=RawOrderSide.BUY,
            limit="0.49",
            tif=TimeInForce.GTC,
            post_only=True,
        )
    )
    session.ingest_trade(
        _trade(
            "wrong",
            side=RawOrderSide.BUY,
            price="0.50",
            size="20",
            at=T0 + timedelta(seconds=2),
        )
    )
    session.run()

    assert session.result("maker").filled_size == 0


def test_11_trade_and_linked_l2_decrease_are_not_double_counted() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(bids=(("0.49", "5"),)))
    session.submit_order(
        _order(
            "maker",
            side=RawOrderSide.BUY,
            limit="0.49",
            size="5",
            tif=TimeInForce.GTC,
            post_only=True,
        )
    )
    session.ingest_trade(
        _trade(
            "trade-5",
            side=RawOrderSide.SELL,
            price="0.49",
            size="5",
            at=T0 + timedelta(seconds=2),
            sequence=1,
        )
    )
    session.ingest_delta(
        BookDeltaEvent(
            event_id="delta-5",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            exchange_ts=T0 + timedelta(seconds=2),
            local_ts=T0 + timedelta(seconds=2),
            book_epoch=0,
            side=EconomicBookSide.BID,
            price=Decimal("0.49"),
            new_size=Decimal(0),
            source="native_l2",
            sequence=2,
            linked_trade_event_ids=("trade-5",),
        )
    )
    session.ingest_trade(
        _trade(
            "trade-next",
            side=RawOrderSide.SELL,
            price="0.49",
            size="5",
            at=T0 + timedelta(seconds=3),
            sequence=3,
        )
    )
    session.run(until=T0 + timedelta(seconds=2, milliseconds=500))
    assert session.result("maker").filled_size == 0
    session.run()
    assert session.result("maker").filled_size == Decimal("5.0000000000")


def test_12_queue_ahead_never_becomes_negative() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot(bids=(("0.49", "2"),)))
    session.submit_order(
        _order(
            "maker",
            limit="0.49",
            tif=TimeInForce.GTC,
            post_only=True,
        )
    )
    session.run(until=T0 + timedelta(seconds=1))
    key = session.orders["maker"].maker_state.key
    session.maker_queues.on_unmatched_book_decrease(key, Decimal(100))

    assert session.maker_queues.queue_ahead("maker") == 0


def test_13_simulated_makers_follow_exchange_arrival_fifo() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(bids=(("0.49", "0.1"),)))
    for index in (1, 2):
        session.submit_order(
            _order(
                f"maker-{index}",
                limit="0.49",
                size="3",
                tif=TimeInForce.GTC,
                post_only=True,
                at=T0 + timedelta(seconds=index),
            )
        )
    session.ingest_trade(
        _trade(
            "sell-flow",
            side=RawOrderSide.SELL,
            price="0.49",
            size="4.1",
            at=T0 + timedelta(seconds=3),
        )
    )
    session.run()

    assert session.result("maker-1").filled_size == Decimal("3.0000000000")
    assert session.result("maker-2").filled_size == Decimal("1.0000000000")


def test_14_fill_before_cancel_arrival_remains_valid() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(bids=(("0.49", "0.1"),)))
    session.submit_order(
        _order(
            "maker",
            limit="0.49",
            size="2",
            tif=TimeInForce.GTC,
            post_only=True,
        )
    )
    session.ingest_trade(
        _trade(
            "sell-flow",
            side=RawOrderSide.SELL,
            price="0.49",
            size="2.1",
            at=T0 + timedelta(seconds=2),
        )
    )
    session.submit_cancel(
        "maker",
        signal_ts=T0 + timedelta(seconds=1, milliseconds=950),
        cancel_latency_ms=100,
    )
    session.run()

    assert session.result("maker").status == OrderStatus.FILLED


def test_15_no_trade_normalizes_into_yes_economic_queue() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(bids=(("0.60", "0.1"),), asks=()))
    session.submit_order(
        _order(
            "maker",
            limit="0.60",
            size="2",
            tif=TimeInForce.GTC,
            post_only=True,
        )
    )
    session.ingest_trade(
        _trade(
            "no-buy",
            outcome=Outcome.NO,
            asset_id="no",
            side=RawOrderSide.BUY,
            price="0.40",
            size="2.1",
            at=T0 + timedelta(seconds=2),
        )
    )
    session.run()

    assert session.result("maker").filled_size == Decimal("2.0000000000")


def test_16_same_data_configuration_and_seed_have_same_audit_hash() -> None:
    def run_once() -> str:
        session = _session()
        session.ingest_snapshot(_snapshot())
        session.submit_order(_order("o-1"))
        session.run()
        return session.replay_hash

    assert run_once() == run_once()


def test_17_future_snapshot_cannot_initialize_past_order() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(at=T0 + timedelta(seconds=1)))
    session.submit_order(_order("past", at=T0))
    session.run()

    assert session.result("past").status == OrderStatus.DATA_NOT_READY


def test_18_hot_and_cold_replay_have_identical_event_hash() -> None:
    hashes = []
    for cold in (False, True):
        session = _session(cold=cold)
        session.ingest_snapshot(_snapshot())
        session.submit_order(_order("o-1"))
        session.run()
        hashes.append(session.replay_hash)
    assert hashes[0] == hashes[1]


def test_19_source_handover_without_overlap_rejects_execution() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    session.mark_source_handover_pending(
        "condition-1", event_id="handover-gap", at=T0 + timedelta(milliseconds=500)
    )
    session.submit_order(_order("o-1"))
    session.run()

    assert session.result("o-1").status == OrderStatus.DATA_NOT_READY
    assert "handover" in session.result("o-1").reason


def test_20_strict_mirror_mismatch_rejects_level_capacity() -> None:
    session = _session("strict")
    session.ingest_snapshot(_snapshot(asks=(("0.60", "10"),)))
    session.ingest_snapshot(
        _snapshot(
            snapshot_id="no-mismatch",
            outcome=Outcome.NO,
            bids=(("0.40", "1"),),
            asks=(("0.41", "1"),),
        )
    )
    session.submit_order(_order("o-1", limit="0.60"))
    session.run()

    assert session.result("o-1").filled_size == 0
    assert session.exchange_book.mirror_mismatches


def test_21_buy_sell_cash_and_token_flows_conserve_without_fees() -> None:
    buy = _filled_match()
    sell = replace(
        buy,
        fill_id="sell-fill",
        raw_side=RawOrderSide.SELL,
        canonical_side=EconomicAction.SELL,
        audit_hash="",
    )
    ledger = PredictionPortfolioLedger(initial_cash=Decimal(100))
    ledger.confirm_match(buy)
    ledger.confirm_match(sell)

    assert ledger.cash == Decimal("100.0000000000")
    assert ledger.tokens[buy.token_id] == 0


def test_22_split_then_merge_restores_collateral_minus_explicit_fees() -> None:
    ledger = PredictionPortfolioLedger(initial_cash=Decimal(10))
    ledger.split(
        operation_id="split",
        yes_token_id="yes",
        no_token_id="no",
        size=Decimal(3),
        fee=Decimal("0.1"),
    )
    ledger.merge(
        operation_id="merge",
        yes_token_id="yes",
        no_token_id="no",
        size=Decimal(3),
        fee=Decimal("0.2"),
    )

    assert ledger.cash == Decimal("9.7")
    assert ledger.tokens == {"yes": Decimal("0E-10"), "no": Decimal("0E-10")}


def test_23_mint_and_merge_match_types_emit_correct_system_flows() -> None:
    match = _filled_match()
    mint = replace(match, match_type_hint=CtfMatchType.MINT, audit_hash="")
    merge = replace(match, match_type_hint=CtfMatchType.MERGE, audit_hash="")

    assert PredictionPortfolioLedger.ctf_system_flow(mint) == {
        "collateral": -match.qty,
        "yes": match.qty,
        "no": match.qty,
    }
    assert PredictionPortfolioLedger.ctf_system_flow(merge) == {
        "collateral": match.qty,
        "yes": -match.qty,
        "no": -match.qty,
    }


def test_24_redeem_is_idempotent() -> None:
    ledger = PredictionPortfolioLedger()
    ledger.tokens = {"yes": Decimal(2), "no": Decimal(2)}
    payouts = PayoutVector(
        condition_id="condition-1",
        payouts={"yes": Decimal(1), "no": Decimal(0)},
        resolution_source="oracle",
        oracle_finalized_at=T0.isoformat(),
    )

    assert ledger.redeem(operation_id="redeem", payout_vector=payouts) == 2
    assert ledger.redeem(operation_id="redeem", payout_vector=payouts) == 0


def test_25_half_half_resolution_pays_both_tokens() -> None:
    ledger = PredictionPortfolioLedger()
    ledger.tokens = {"yes": Decimal(2), "no": Decimal(2)}
    payouts = PayoutVector(
        condition_id="condition-1",
        payouts={"yes": Decimal("0.5"), "no": Decimal("0.5")},
        resolution_source="oracle",
        oracle_finalized_at=T0.isoformat(),
    )

    assert ledger.redeem(operation_id="redeem-half", payout_vector=payouts) == 2


def test_26_failed_settlement_reverses_provisional_effects() -> None:
    match = _filled_match()
    ledger = PredictionPortfolioLedger(initial_cash=Decimal(10))
    ledger.record_match(match)
    assert ledger.provisional_cash < 0
    assert ledger.provisional_tokens[match.token_id] > 0
    ledger.fail_match(match)

    assert ledger.provisional_cash == 0
    assert ledger.provisional_tokens[match.token_id] == 0
    assert ledger.cash == 10


def test_27_taker_fee_is_symmetric_at_p_and_one_minus_p() -> None:
    yes = _order("yes", limit="0.20", fee_rate="0.02")
    no = _order(
        "no",
        outcome=Outcome.NO,
        asset_id="no",
        limit="0.80",
        fee_rate="0.02",
    )

    assert ReplayExecutionSession._fee(yes, Decimal("0.20"), Decimal(10)) == ReplayExecutionSession._fee(
        no, Decimal("0.80"), Decimal(10)
    )


def test_28_pml2_execution_match_enters_unified_finalizer_adapter() -> None:
    match = _filled_match()
    rows = default_execution_adapter_registry().extract(
        {"pml2_replay_v1": {"execution_matches": [match.as_dict()]}}
    )

    assert len(rows) == 1
    assert rows[0].source_fill_id == match.fill_id
    assert rows[0].execution_model == PREDICTION_L2_REPLAY_V1_MODE


def test_29_strategy_cannot_use_trade_before_local_delivery() -> None:
    session = _session()
    event = _trade(
        "delayed-trade",
        side=RawOrderSide.BUY,
        price="0.50",
        size="1",
        at=T0,
        local_at=T0 + timedelta(seconds=2),
    )
    session.ingest_trade(event)
    session.run(until=T0 + timedelta(seconds=1))
    assert not session.information_available("delayed-trade", T0 + timedelta(seconds=1))
    session.run()
    assert session.information_available("delayed-trade", T0 + timedelta(seconds=2))


def test_30_maker_must_arrive_before_trade_at_exchange() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    same_time = T0 + timedelta(seconds=1)
    session.submit_order(
        _order(
            "maker",
            limit="0.49",
            tif=TimeInForce.GTC,
            post_only=True,
            at=same_time,
        )
    )
    session.ingest_trade(
        _trade(
            "same-time-trade",
            side=RawOrderSide.SELL,
            price="0.49",
            size="100",
            at=same_time,
        )
    )
    session.run()

    assert session.result("maker").filled_size == 0


def test_31_external_signal_is_visible_only_after_available_at() -> None:
    session = _session()
    session.ingest_external_signal(
        event_id="news",
        available_at=T0 + timedelta(seconds=2),
        source="news",
    )
    session.run(until=T0 + timedelta(seconds=1))
    assert not session.information_available("news", T0 + timedelta(seconds=1))
    session.run()
    assert session.information_available("news", T0 + timedelta(seconds=2))


def test_32_calibration_parameters_cannot_use_future_window() -> None:
    profile = get_pml2_profile("realistic")
    with pytest.raises(ValueError, match="training_end"):
        profile.validate_calibration_for(profile.training_end)
    profile.validate_calibration_for(profile.training_end + timedelta(seconds=1))


def test_33_settlement_lifecycle_supports_pending_confirmed_and_failed() -> None:
    match = _filled_match()
    lifecycle = SettlementLifecycle()
    lifecycle.register(match)
    lifecycle.transition(
        match.fill_id,
        MatchFinalityState.PENDING_SETTLEMENT,
        observed_at=T0 + timedelta(seconds=1),
    )
    receipt = lifecycle.transition(
        match.fill_id,
        MatchFinalityState.CONFIRMED,
        observed_at=T0 + timedelta(seconds=2),
        tx_hash="0xabc",
        block_number=123,
    )

    assert receipt.state == MatchFinalityState.CONFIRMED
    assert lifecycle.matches[match.fill_id].finality_state == MatchFinalityState.CONFIRMED


def test_34_gtd_remainder_expires_at_exchange() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    expires = T0 + timedelta(seconds=2)
    session.submit_order(
        _order(
            "gtd",
            limit="0.40",
            tif=TimeInForce.GTD,
            expires_at=expires,
        )
    )
    session.run()

    assert session.result("gtd").status == OrderStatus.EXPIRED


def test_35_service_rejects_unknown_json_fields() -> None:
    with pytest.raises(Pml2RequestError, match="unknown request fields"):
        run_pml2_replay({"orders": [{}], "events": [], "profil": "realistic"})


def test_36_service_replays_contract_payload_and_mode_aliases() -> None:
    payload = {
        "runId": "run-1",
        "profile": "realistic",
        "events": [
            {
                "type": "SNAPSHOT",
                "snapshotId": "snap-1",
                "conditionId": "condition-1",
                "marketId": "market-1",
                "assetId": "yes",
                "outcome": "YES",
                "exchangeTs": T0.isoformat(),
                "localTs": T0.isoformat(),
                "bookEpoch": 0,
                "source": "native_l2",
                "bids": [{"price": "0.49", "size": "10"}],
                "asks": [{"price": "0.50", "size": "10"}],
            }
        ],
        "orders": [
            {
                "orderId": "o-1",
                "strategyId": "s-1",
                "conditionId": "condition-1",
                "marketId": "market-1",
                "assetId": "yes",
                "outcome": "YES",
                "side": "BUY",
                "size": "5",
                "limitPrice": "0.50",
                "tif": "FAK",
                "signalTs": (T0 + timedelta(seconds=1)).isoformat(),
                "entryLatencyMs": 0,
                "venueDelayMs": 0,
                "responseLatencyMs": 0,
            }
        ],
    }
    result = run_pml2_replay(payload)

    assert result["filled_size"] == "5.0000000000"
    assert len(result["execution_matches"]) == 1
    assert normalize_execution_price_mode("pml2") == PREDICTION_L2_REPLAY_V1_MODE
    assert is_prediction_l2_replay_v1_mode("prediction-l2")


def _api_client():
    app = Flask(__name__)
    app.register_blueprint(quant_routes.create_quant_blueprint({}))
    return app.test_client()


def test_main_backtest_pml2_audit_mode_is_explicit_and_validated() -> None:
    assert parse_parameters({}).pml2_audit_mode == "CHAIN_ONLY"
    assert parse_parameters({"pml2AuditMode": "full"}).pml2_audit_mode == "FULL"
    with pytest.raises(ValueError, match="pml2_audit_mode"):
        parse_parameters({"pml2AuditMode": "fast"})


def test_37_prediction_l2_http_contract() -> None:
    client = _api_client()

    profiles = client.get("/quant/prediction-l2/v1/profiles")
    readiness = client.get("/quant/prediction-l2/v1/readiness")
    replay = client.post(
        "/quant/prediction-l2/v1/replay",
        json={
            "runId": "http-run",
            "profile": "realistic",
            "events": [
                {
                    "type": "SNAPSHOT",
                    "eventId": "book-1",
                    "snapshotId": "snapshot-1",
                    "conditionId": "condition-1",
                    "marketId": "market-1",
                    "assetId": "yes",
                    "outcome": "YES",
                    "exchangeTs": T0.isoformat(),
                    "localTs": T0.isoformat(),
                    "bids": [{"price": "0.49", "size": "10"}],
                    "asks": [{"price": "0.50", "size": "10"}],
                    "source": "native_l2",
                }
            ],
            "orders": [
                {
                    "orderId": "order-1",
                    "strategyId": "strategy-1",
                    "conditionId": "condition-1",
                    "marketId": "market-1",
                    "assetId": "yes",
                    "outcome": "YES",
                    "side": "BUY",
                    "size": "5",
                    "limitPrice": "0.50",
                    "tif": "FAK",
                    "signalTs": (T0 + timedelta(seconds=1)).isoformat(),
                    "entryLatencyMs": 0,
                    "venueDelayMs": 0,
                    "responseLatencyMs": 0,
                }
            ],
        },
    )

    assert profiles.status_code == 200
    assert profiles.get_json()["default_profile"] == "realistic"
    assert readiness.status_code == 200
    assert readiness.get_json()["capabilities"]["dual_clock"] is True
    assert replay.status_code == 200
    assert replay.get_json()["execution_matches"][0]["qty"] == "5.0000000000"


def test_38_prediction_l2_http_rejects_unknown_fields() -> None:
    response = _api_client().post(
        "/quant/prediction-l2/v1/replay",
        json={"orders": [{}], "lookbackBloks": 100},
    )

    assert response.status_code == 400
    assert response.get_json()["error_code"] == "INVALID_PML2_REQUEST"


def test_38a_prediction_l2_http_preserves_quote_and_signed_amounts() -> None:
    response = _api_client().post(
        "/quant/prediction-l2/v1/replay",
        json={
            "runId": "http-quote-run",
            "profile": "realistic",
            "events": [
                {
                    "type": "SNAPSHOT",
                    "eventId": "book-quote",
                    "snapshotId": "snapshot-quote",
                    "conditionId": "condition-quote",
                    "marketId": "market-quote",
                    "assetId": "yes-quote",
                    "outcome": "YES",
                    "exchangeTs": T0.isoformat(),
                    "localTs": T0.isoformat(),
                    "bids": [{"price": "0.39", "size": "10"}],
                    "asks": [
                        {"price": "0.40", "size": "1"},
                        {"price": "0.50", "size": "2"},
                    ],
                    "source": "native_l2",
                }
            ],
            "orders": [
                {
                    "orderId": "quote-order",
                    "strategyId": "strategy-quote",
                    "conditionId": "condition-quote",
                    "marketId": "market-quote",
                    "assetId": "yes-quote",
                    "outcome": "YES",
                    "side": "BUY",
                    "size": "1",
                    "amountUnit": "QUOTE",
                    "signedMakerAmount": "1",
                    "signedTakerAmount": "2",
                    "venueAdmission": "ACCEPTED",
                    "venueAdmissionEvidenceId": "paper-order:quote-order",
                    "limitPrice": "0.50",
                    "tif": "FAK",
                    "signalTs": (T0 + timedelta(seconds=1)).isoformat(),
                    "entryLatencyMs": 0,
                    "venueDelayMs": 0,
                    "responseLatencyMs": 0,
                }
            ],
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["filled_size"] == "2.2000000000"
    assert payload["amount_totals_by_unit"]["QUOTE"] == {
        "filled_amount": "1.0000000000",
        "remaining_amount": "0",
        "requested_amount": "1.0000000000",
    }
    order = payload["orders"][0]
    assert order["order"]["amount_unit"] == "QUOTE"
    assert order["order"]["signed_maker_amount"] == "1.0000000000"
    assert order["order"]["signed_taker_amount"] == "2.0000000000"


def test_39_single_order_matches_legacy_depth_but_run_capacity_does_not_reset() -> None:
    params = BacktestParameters(
        execution_profile="optimistic",
        order_role="taker",
        latency_seconds=Decimal(0),
        max_entry_price=Decimal("0.50"),
        allow_partial_fill=True,
    )
    legacy_snapshot = LegacyBookSnapshot(
        ts=T0,
        market_id="market-1",
        asset_id="yes",
        sequence=1,
        source="native_l2",
        bids=(LegacyBookLevel(Decimal("0.49"), Decimal(10)),),
        asks=(LegacyBookLevel(Decimal("0.50"), Decimal(10)),),
        is_full_depth=True,
    )
    legacy_first = simulate_l2_depth_execution(
        snapshots=[legacy_snapshot],
        decision_block=None,
        decision_timestamp=T0 + timedelta(seconds=1),
        side="BUY_YES",
        target_size=Decimal(6),
        signal_price=Decimal("0.50"),
        params=params,
        market_id="market-1",
        asset_id="yes",
    )
    legacy_second = simulate_l2_depth_execution(
        snapshots=[legacy_snapshot],
        decision_block=None,
        decision_timestamp=T0 + timedelta(seconds=2),
        side="BUY_YES",
        target_size=Decimal(6),
        signal_price=Decimal("0.50"),
        params=params,
        market_id="market-1",
        asset_id="yes",
    )
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot(asks=(("0.50", "10"),)))
    session.submit_order(_order("new-1", size="6"))
    session.submit_order(_order("new-2", size="6", at=T0 + timedelta(seconds=2)))
    session.run()

    assert legacy_first["filled_size"] == Decimal("6.0000000000")
    assert session.result("new-1").filled_size == legacy_first["filled_size"]
    assert legacy_second["filled_size"] == Decimal("6.0000000000")
    assert session.result("new-2").filled_size == Decimal("4.0000000000")
    assert sum((item.filled_size for item in session.results()), Decimal(0)) == 10


def test_40_external_trade_removes_taker_residual_without_l2_delta() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot(asks=(("0.50", "10"),)))
    session.ingest_trade(
        _trade(
            "external-buy",
            side=RawOrderSide.BUY,
            price="0.50",
            size="4",
            at=T0 + timedelta(seconds=1),
        )
    )
    session.submit_order(_order("after-trade", size="10", at=T0 + timedelta(seconds=2)))
    session.run()

    assert session.result("after-trade").filled_size == Decimal("6.0000000000")
    residual = session.exchange_book.residual_snapshot()
    ask = next(item for item in residual if item["side"] == "ASK")
    assert ask["external_trade_removed_size"] == "4.0000000000"


def test_41_delta_before_linked_trade_does_not_remove_residual_twice() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot(asks=(("0.50", "10"),)))
    session.ingest_delta(
        BookDeltaEvent(
            event_id="ask-down",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            exchange_ts=T0 + timedelta(seconds=1),
            local_ts=T0 + timedelta(seconds=1),
            book_epoch=0,
            side=EconomicBookSide.ASK,
            price=Decimal("0.50"),
            new_size=Decimal(6),
            source="native_l2",
            sequence=1,
            linked_trade_event_ids=("external-buy",),
        )
    )
    session.ingest_trade(
        _trade(
            "external-buy",
            side=RawOrderSide.BUY,
            price="0.50",
            size="4",
            at=T0 + timedelta(seconds=1),
            sequence=2,
        )
    )
    session.submit_order(_order("after-pair", size="10", at=T0 + timedelta(seconds=2)))
    session.run()

    assert session.result("after-pair").filled_size == Decimal("6.0000000000")


def test_42_duplicate_market_event_is_idempotent_and_reported() -> None:
    session = _session()
    snapshot = _snapshot()
    session.ingest_snapshot(snapshot)
    session.ingest_snapshot(snapshot)
    session.submit_order(_order("dedup"))
    session.run()

    manifest = session.coverage_manifest()
    assert session.result("dedup").filled_size == Decimal("5.0000000000")
    assert manifest.snapshot_count == 1
    assert manifest.duplicate_event_count == 1
    assert manifest.data_quality_status == "VALID_WITH_DEGRADATION"


def test_43_conflicting_duplicate_event_id_fails_closed() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())

    with pytest.raises(ValueError, match="conflicting payloads"):
        session.ingest_snapshot(_snapshot(asks=(("0.50", "99"),)))

    assert session.coverage_manifest().data_quality_status == "REJECTED"


def test_44_profile_matrix_runs_same_episode_under_all_profiles() -> None:
    result = run_pml2_profile_matrix(
        {
            "runId": "matrix-run",
            "events": [
                {
                    "type": "SNAPSHOT",
                    "snapshotId": "matrix-snapshot",
                    "conditionId": "condition-1",
                    "marketId": "market-1",
                    "assetId": "yes",
                    "outcome": "YES",
                    "exchangeTs": T0.isoformat(),
                    "localTs": T0.isoformat(),
                    "source": "native_l2",
                    "bids": [{"price": "0.49", "size": "10"}],
                    "asks": [{"price": "0.50", "size": "10"}],
                }
            ],
            "orders": [
                {
                    "orderId": "matrix-order",
                    "strategyId": "strategy-1",
                    "conditionId": "condition-1",
                    "marketId": "market-1",
                    "assetId": "yes",
                    "outcome": "YES",
                    "side": "BUY",
                    "size": "10",
                    "limitPrice": "0.50",
                    "tif": "FAK",
                    "signalTs": (T0 + timedelta(seconds=1)).isoformat(),
                    "entryLatencyMs": 0,
                    "venueDelayMs": 0,
                }
            ],
        }
    )

    assert result["profiles"] == ["strict", "realistic", "optimistic"]
    assert result["comparison"]["strict"]["filled_size"] == "5.0000000000"
    assert result["comparison"]["realistic"]["filled_size"] == "10.0000000000"
    assert result["comparison"]["optimistic"]["filled_size"] == "10.0000000000"


def test_45_main_backtest_engine_reuses_one_pml2_session() -> None:
    class Provider:
        def snapshot_at(self, _target):
            return EngineBookSnapshot(
                snapshot_id=1,
                token_id="yes",
                side="YES",
                bids=((Decimal("0.49"), Decimal(10)),),
                asks=((Decimal("0.50"), Decimal(10)),),
                source="pmxt_compact",
                timestamp=T0,
                snapshot_version="engine-snapshot",
                is_full_depth=True,
            )

    params = BacktestParameters(
        execution_price_mode=PREDICTION_L2_REPLAY_V1_MODE,
        execution_profile="optimistic",
        order_role="taker",
        latency_seconds=Decimal(0),
        max_entry_price=Decimal("0.50"),
        allow_partial_fill=True,
    )
    run = {
        "run_id": 45,
        "market_id": 1,
        "market_slug": "real-market",
        "token_side": "YES",
        "_pmxt_compact_book_provider": Provider(),
        "_pml2_token_context": {
            "condition_id": "condition-1",
            "market_id": 1,
            "token_id": "yes",
            "token_side": "YES",
        },
    }
    first = _fill_decision(
        params,
        PricePoint(1, Decimal("0.50"), Decimal(1), timestamp=T0 + timedelta(seconds=1)),
        run,
        "BUY_YES",
        target_size=Decimal(6),
    )
    second = _fill_decision(
        params,
        PricePoint(2, Decimal("0.50"), Decimal(1), timestamp=T0 + timedelta(seconds=2)),
        run,
        "BUY_YES",
        target_size=Decimal(6),
    )

    assert first["filled_size"] == Decimal("6.0000000000")
    assert second["filled_size"] == Decimal("4.0000000000")
    assert first["execution_model"] == PREDICTION_L2_REPLAY_V1_MODE
    assert run["_pml2_session"].report()["match_count"] == 2


def test_46_cancel_only_mode_rejects_new_orders() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    session.ingest_lifecycle(
        MarketLifecycleEvent(
            event_id="cancel-only",
            condition_id="condition-1",
            market_id="market-1",
            exchange_ts=T0 + timedelta(seconds=1),
            local_ts=T0 + timedelta(seconds=1),
            trading_mode=TradingMode.CANCEL_ONLY,
            source="venue",
        )
    )
    session.submit_order(_order("blocked", at=T0 + timedelta(seconds=2)))
    session.run()

    assert session.result("blocked").status == OrderStatus.REJECTED
    assert session.result("blocked").reason == "market_cancel_only"


def test_47_market_close_cancels_resting_orders() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot())
    session.submit_order(
        _order(
            "resting",
            limit="0.40",
            tif=TimeInForce.GTC,
            post_only=True,
        )
    )
    session.ingest_lifecycle(
        MarketLifecycleEvent(
            event_id="closed",
            condition_id="condition-1",
            market_id="market-1",
            exchange_ts=T0 + timedelta(seconds=2),
            local_ts=T0 + timedelta(seconds=2),
            trading_mode=TradingMode.CLOSED,
            source="venue",
        )
    )
    session.run()

    assert session.result("resting").status == OrderStatus.CANCELLED
    assert session.result("resting").reason == "market_closed_at_exchange"


def test_48_restart_requires_fresh_snapshot_before_execution() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot())
    session.ingest_lifecycle(
        MarketLifecycleEvent(
            event_id="restart",
            condition_id="condition-1",
            market_id="market-1",
            exchange_ts=T0 + timedelta(seconds=1),
            local_ts=T0 + timedelta(seconds=1),
            trading_mode=TradingMode.LIVE,
            source="venue",
            requires_fresh_snapshot=True,
        )
    )
    session.submit_order(_order("too-early", at=T0 + timedelta(seconds=2)))
    session.ingest_snapshot(
        _snapshot(
            snapshot_id="fresh",
            at=T0 + timedelta(seconds=3),
            asks=(("0.50", "10"),),
        )
    )
    session.submit_order(_order("after-fresh", at=T0 + timedelta(seconds=4)))
    session.run()

    assert session.result("too-early").status == OrderStatus.DATA_NOT_READY
    assert session.result("too-early").reason == "book_waiting_for_snapshot"
    assert session.result("after-fresh").status == OrderStatus.FILLED


def test_49_source_handover_verification_requires_post_handover_snapshot() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot())
    session.mark_source_handover_pending(
        "condition-1",
        event_id="handover",
        at=T0 + timedelta(seconds=1),
    )
    session.verify_source_handover(
        "condition-1",
        event_id="verify-too-early",
        at=T0 + timedelta(seconds=2),
    )
    session.submit_order(_order("blocked", at=T0 + timedelta(seconds=3)))
    session.ingest_snapshot(
        _snapshot(
            snapshot_id="post-handover",
            at=T0 + timedelta(seconds=4),
            source="pmxt_compact",
        )
    )
    session.verify_source_handover(
        "condition-1",
        event_id="verify-after-snapshot",
        at=T0 + timedelta(seconds=5),
    )
    session.submit_order(_order("accepted", at=T0 + timedelta(seconds=6)))
    session.run()

    assert session.result("blocked").status == OrderStatus.DATA_NOT_READY
    assert session.result("accepted").status == OrderStatus.FILLED
    assert session.coverage_manifest().source_handover_verified is True


def test_50_truncated_depth_allows_visible_fak_but_marks_unknown_remainder() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(
        _snapshot(
            asks=(("0.50", "3"),),
            is_full_depth=False,
            is_truncated=True,
        )
    )
    session.submit_order(_order("truncated-fak", size="5"))
    session.run()

    result = session.result("truncated-fak")
    assert result.filled_size == Decimal("3.0000000000")
    assert result.reason == "truncated_depth_remainder_unknown"
    assert session.coverage_manifest().data_quality_status == "VALID_WITH_DEGRADATION"


def test_51_truncated_depth_cannot_prove_fok_atomic_capacity() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(
        _snapshot(
            asks=(("0.50", "3"),),
            is_full_depth=False,
            is_truncated=True,
        )
    )
    session.submit_order(
        _order("truncated-fok", size="5", tif=TimeInForce.FOK)
    )
    session.run()

    result = session.result("truncated-fok")
    assert result.filled_size == 0
    assert result.reason == "fok_truncated_depth_unverifiable"


def test_52_tick_and_minimum_order_constraints_apply_at_arrival() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(
        _snapshot(tick_size="0.01", min_order_size="2")
    )
    session.submit_order(_order("too-small", size="1", limit="0.50"))
    session.submit_order(
        _order(
            "bad-tick",
            size="2",
            limit="0.505",
            at=T0 + timedelta(seconds=2),
        )
    )
    session.run()

    assert session.result("too-small").reason == "below_market_min_order_size"
    assert session.result("bad-tick").reason == "limit_not_tick_aligned"


def test_53_snapshot_rejects_zero_tick_or_minimum_size() -> None:
    with pytest.raises(ValueError, match="tick_size must be positive"):
        _snapshot(tick_size="0")
    with pytest.raises(ValueError, match="min_order_size must be positive"):
        _snapshot(min_order_size="0")


def test_53a_quote_buy_fok_walks_by_budget_and_keeps_fee_separate() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(
        _snapshot(
            asks=(("0.012", "79.97"), ("0.027", "23.78")),
            tick_size="0.001",
            min_order_size="5",
        )
    )
    session.submit_order(
        _order(
            "quote-fok",
            size="1.10",
            limit="0.027",
            tif=TimeInForce.FOK,
            fee_rate="0.02",
            amount_unit=OrderAmountUnit.QUOTE,
            signed_maker_amount="1.08",
            signed_taker_amount="40",
            venue_admission=VenueAdmissionStatus.ACCEPTED,
        )
    )
    session.run()

    result = session.result("quote-fok")
    assert result.status == OrderStatus.FILLED
    assert result.order.effective_requested_amount == Decimal("1.0800000000")
    assert result.filled_size == Decimal("84.4277777777")
    assert result.filled_amount == Decimal("1.0800000000")
    assert result.remaining_amount == 0
    assert result.remaining_size == 0
    assert [row.raw_price for row in result.fills] == [
        Decimal("0.0120000000"),
        Decimal("0.0270000000"),
    ]
    assert sum((row.fee for row in result.fills), Decimal(0)) > 0


def test_53b_quote_buy_fak_partial_uses_quote_remainder() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot(asks=(("0.02", "10"),), tick_size="0.01"))
    session.submit_order(
        _order(
            "quote-fak",
            size="1",
            limit="0.02",
            amount_unit=OrderAmountUnit.QUOTE,
        )
    )
    session.run()

    result = session.result("quote-fak")
    assert result.status == OrderStatus.PARTIAL
    assert result.filled_size == Decimal("10.0000000000")
    assert result.filled_amount == Decimal("0.2000000000")
    assert result.remaining_amount == Decimal("0.8000000000")
    assert result.remaining_size == Decimal("40.0000000000")


def test_53c_quote_buy_fok_is_atomic_when_budget_cannot_be_spent() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot(asks=(("0.02", "10"),), tick_size="0.01"))
    session.submit_order(
        _order(
            "quote-fok-insufficient",
            size="1",
            limit="0.02",
            tif=TimeInForce.FOK,
            amount_unit=OrderAmountUnit.QUOTE,
        )
    )
    session.submit_order(
        _order(
            "capacity-still-present",
            size="10",
            limit="0.02",
            at=T0 + timedelta(seconds=2),
        )
    )
    session.run()

    rejected = session.result("quote-fok-insufficient")
    assert rejected.status == OrderStatus.REJECTED
    assert rejected.filled_size == 0
    assert rejected.remaining_amount == Decimal("1.0000000000")
    assert session.result("capacity-still-present").filled_size == Decimal(
        "10.0000000000"
    )


def test_53d_observed_venue_acceptance_audits_minimum_conflict() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot(min_order_size="5"))
    session.submit_order(
        _order(
            "accepted-small",
            size="1",
            venue_admission=VenueAdmissionStatus.ACCEPTED,
        )
    )
    session.run()

    result = session.result("accepted-small")
    assert result.status == OrderStatus.FILLED
    assert result.admission_diagnostics == ("MIN_ORDER_SIZE_CONTRACT_CONFLICT",)
    assert session.report()["min_order_size_contract_conflict_count"] == 1


def test_53e_quote_contract_rejects_sell_and_resting_tif() -> None:
    with pytest.raises(ValueError, match="QUOTE amount is supported only for BUY"):
        _order(
            "quote-sell",
            side=RawOrderSide.SELL,
            amount_unit=OrderAmountUnit.QUOTE,
        )
    with pytest.raises(ValueError, match="immediate TIF"):
        _order(
            "quote-gtc",
            tif=TimeInForce.GTC,
            amount_unit=OrderAmountUnit.QUOTE,
        )
    with pytest.raises(ValueError, match="requires an evidence id"):
        replace(
            _order("missing-admission-evidence"),
            venue_admission=VenueAdmissionStatus.ACCEPTED,
        )


def test_54_dynamic_strategy_sees_atomic_batch_and_receives_fill_response() -> None:
    class Strategy:
        strategy_id = "dynamic"

        def __init__(self) -> None:
            self.batch_calls = 0
            self.responses: list[str] = []

        def on_book(self, ctx, event) -> None:
            if not isinstance(event, BookLevelBatchEvent):
                return
            self.batch_calls += 1
            assert ctx.book.best("condition-1", EconomicBookSide.BID).canonical_yes_price == Decimal("0.4800000000")
            assert ctx.book.best("condition-1", EconomicBookSide.ASK).canonical_yes_price == Decimal("0.5100000000")
            ctx.submit_order(
                replace(
                    _order("dynamic-order", limit="0.51", at=ctx.now),
                    strategy_id=self.strategy_id,
                )
            )

        def on_order_update(self, ctx, update) -> None:
            assert ctx.order(update.order_id) is not None
            self.responses.append(update.fill_id)

    session = _session("optimistic")
    strategy = Strategy()
    replay = DynamicPredictionReplay(session=session, strategies=(strategy,))
    session.ingest_snapshot(_snapshot())
    batch_at = T0 + timedelta(seconds=1)
    common = {
        "condition_id": "condition-1",
        "market_id": "market-1",
        "asset_id": "yes",
        "outcome": Outcome.YES,
        "exchange_ts": batch_at,
        "local_ts": batch_at,
        "book_epoch": 0,
        "source": "native_l2",
    }
    batch = BookLevelBatchEvent(
        event_id="batch-1",
        updates=(
            BookDeltaEvent(
                event_id="batch-1:bid",
                side=EconomicBookSide.BID,
                price=Decimal("0.49"),
                new_size=Decimal(0),
                **common,
            ),
            BookDeltaEvent(
                event_id="batch-1:new-bid",
                side=EconomicBookSide.BID,
                price=Decimal("0.48"),
                new_size=Decimal(10),
                **common,
            ),
            BookDeltaEvent(
                event_id="batch-1:old-ask",
                side=EconomicBookSide.ASK,
                price=Decimal("0.50"),
                new_size=Decimal(0),
                **common,
            ),
            BookDeltaEvent(
                event_id="batch-1:new-ask",
                side=EconomicBookSide.ASK,
                price=Decimal("0.51"),
                new_size=Decimal(10),
                **common,
            ),
        ),
        **common,
    )
    session.ingest_level_batch(batch)
    replay.run()

    assert strategy.batch_calls == 1
    assert session.result("dynamic-order").filled_size == Decimal("5.0000000000")
    assert len(strategy.responses) == 1


def test_55_dynamic_strategy_cannot_submit_from_future_information() -> None:
    class Strategy:
        strategy_id = "future-reader"

        def on_book(self, ctx, event) -> None:
            future = ctx.now + timedelta(seconds=1)
            ctx.submit_order(
                replace(
                    _order("future-order", at=future),
                    strategy_id=self.strategy_id,
                )
            )

    session = _session()
    DynamicPredictionReplay(session=session, strategies=(Strategy(),))
    session.ingest_snapshot(_snapshot())

    with pytest.raises(ValueError, match="not yet observed"):
        session.run()


def test_56_effective_dated_fee_registry_is_resolved_at_fill_time() -> None:
    schedules = FeeScheduleRegistry(
        (
            FeeSchedule(
                schedule_id="old",
                asset_id="yes",
                condition_id="condition-1",
                effective_from=T0 - timedelta(days=1),
                effective_until=T0 + timedelta(milliseconds=500),
                platform_fee_rate=Decimal("0.01"),
                source="historical-old",
            ),
            FeeSchedule(
                schedule_id="new",
                asset_id="yes",
                condition_id="condition-1",
                effective_from=T0 + timedelta(milliseconds=500),
                effective_until=None,
                platform_fee_rate=Decimal("0.02"),
                source="historical-new",
            ),
        )
    )
    session = ReplayExecutionSession(
        run_id="run-1",
        profile="optimistic",
        fee_schedules=schedules,
    )
    session.ingest_snapshot(_snapshot())
    session.submit_order(_order("fee-order", at=T0 + timedelta(seconds=1)))
    session.run()

    fill = session.result("fee-order").fills[0]
    assert fill.fee_schedule_id == "new"
    assert fill.fee_source == "historical-new"
    assert fill.fee == Decimal("0.02500")


def test_57_negative_risk_conversion_is_explicit_and_inventory_conserving() -> None:
    ledger = PredictionPortfolioLedger(initial_cash=Decimal(1))
    ledger.tokens = {"a-no": Decimal(3)}

    deltas = ledger.neg_risk_convert(
        operation_id="neg-risk-1",
        source_no_token_id="a-no",
        source_yes_token_id="a-yes",
        event_yes_token_ids=("a-yes", "b-yes", "c-yes"),
        size=Decimal(2),
        fee=Decimal("0.1"),
    )

    assert deltas == {
        "a-no": Decimal("-2.0000000000"),
        "b-yes": Decimal("2.0000000000"),
        "c-yes": Decimal("2.0000000000"),
    }
    assert ledger.tokens == {
        "a-no": Decimal("1.0000000000"),
        "b-yes": Decimal("2.0000000000"),
        "c-yes": Decimal("2.0000000000"),
    }
    assert ledger.cash == Decimal("0.9")


def test_58_failed_match_cannot_be_resurrected_in_portfolio() -> None:
    match = _filled_match()
    ledger = PredictionPortfolioLedger(initial_cash=Decimal(10))
    ledger.record_match(match)
    ledger.fail_match(match)

    with pytest.raises(ValueError, match="failed match cannot be confirmed"):
        ledger.confirm_match(match)


def test_59_linked_trade_and_orderfilled_evidence_are_not_double_counted() -> None:
    session = _session("optimistic")
    session.ingest_snapshot(_snapshot())
    session.submit_order(
        _order(
            "linked-maker",
            size="10",
            limit="0.49",
            tif=TimeInForce.GTC,
            post_only=True,
        )
    )
    first = replace(
        _trade(
            "clob-trade",
            side=RawOrderSide.SELL,
            price="0.49",
            size="10",
            at=T0 + timedelta(seconds=2),
        ),
        evidence_link_id="economic-match-1",
        evidence_kind="TRADE_PRINT",
    )
    duplicate = replace(
        first,
        event_id="orderfilled-evidence",
        source="orderfilled",
        evidence_kind="ORDERFILLED_EVIDENCE",
    )
    session.ingest_trade(first)
    session.ingest_trade(duplicate)
    session.run()

    assert session.result("linked-maker").filled_size == Decimal("5.0000000000")
    assert session.coverage_manifest().duplicate_event_count == 1


def test_60_conflicting_evidence_link_fails_closed() -> None:
    session = _session()
    first = replace(
        _trade(
            "trade-a",
            side=RawOrderSide.BUY,
            price="0.50",
            size="1",
            at=T0,
        ),
        evidence_link_id="same-match",
    )
    conflict = replace(
        first,
        event_id="trade-b",
        size=Decimal(2),
    )
    session.ingest_trade(first)

    with pytest.raises(ValueError, match="conflicting trades"):
        session.ingest_trade(conflict)


def test_61_service_parses_atomic_batch_and_effective_fee_schedule() -> None:
    result = run_pml2_replay(
        {
            "run_id": "batch-api",
            "profile": "optimistic",
            "events": [
                {
                    "type": "SNAPSHOT",
                    "snapshot_id": "initial",
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "asset_id": "yes",
                    "outcome": "YES",
                    "exchange_ts": T0.isoformat(),
                    "local_ts": T0.isoformat(),
                    "source": "native_l2",
                    "bids": [{"price": "0.49", "size": "10"}],
                    "asks": [{"price": "0.50", "size": "10"}],
                },
                {
                    "type": "LEVEL_BATCH",
                    "event_id": "api-batch",
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "asset_id": "yes",
                    "outcome": "YES",
                    "exchange_ts": (T0 + timedelta(milliseconds=500)).isoformat(),
                    "local_ts": (T0 + timedelta(milliseconds=500)).isoformat(),
                    "source": "native_l2",
                    "updates": [
                        {
                            "side": "ASK",
                            "price": "0.50",
                            "new_size": "0",
                            "sequence": 1,
                        },
                        {
                            "side": "ASK",
                            "price": "0.51",
                            "new_size": "10",
                            "sequence": 2,
                        },
                    ],
                },
            ],
            "fee_schedules": [
                {
                    "schedule_id": "api-fee",
                    "asset_id": "yes",
                    "condition_id": "condition-1",
                    "effective_from": (T0 - timedelta(days=1)).isoformat(),
                    "platform_fee_rate": "0.02",
                    "source": "historical-api-fixture",
                }
            ],
            "orders": [
                {
                    "order_id": "api-order",
                    "strategy_id": "api-strategy",
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "asset_id": "yes",
                    "outcome": "YES",
                    "side": "BUY",
                    "size": "5",
                    "limit_price": "0.51",
                    "tif": "FAK",
                    "signal_ts": (T0 + timedelta(seconds=1)).isoformat(),
                    "entry_latency_ms": 0,
                    "response_latency_ms": 0,
                    "venue_delay_ms": 0,
                }
            ],
        }
    )

    fill = result["execution_matches"][0]
    assert fill["raw_price"] == "0.5100000000"
    assert fill["fee_schedule_id"] == "api-fee"
    assert fill["fee"] == "0.02499"
    assert result["coverage_manifest"]["delta_count"] == 2


def test_62_archive_restore_baseline_must_precede_exchange_arrival() -> None:
    at = T0 + timedelta(seconds=1)
    with pytest.raises(Pml2RequestError, match="must precede"):
        run_pml2_replay(
            {
                "profile": "optimistic",
                "archive_restore": [
                    {
                        "condition_id": "condition-1",
                        "market_id": "market-1",
                        "yes_asset_id": "yes",
                        "no_asset_id": "no",
                        "start_time": at.isoformat(),
                    }
                ],
                "orders": [
                    {
                        "order_id": "archive-order",
                        "strategy_id": "strategy",
                        "condition_id": "condition-1",
                        "market_id": "market-1",
                        "asset_id": "yes",
                        "outcome": "YES",
                        "side": "BUY",
                        "size": "1",
                        "limit_price": "0.50",
                        "tif": "FAK",
                        "signal_ts": at.isoformat(),
                        "entry_latency_ms": 0,
                        "venue_delay_ms": 0,
                    }
                ],
            }
        )


def test_63_readiness_reports_research_grade_boundaries() -> None:
    readiness = build_pml2_readiness()

    assert readiness["ready_for_contract_replay"] is True
    assert readiness["maturity"] == "RESEARCH_GRADE"
    assert readiness["production_ready"] is False
    assert "AUTHENTICATED_MAKER_FILL_OUTCOME_DIVERSITY_INSUFFICIENT" in readiness[
        "production_blockers"
    ]
    assert readiness["capabilities"]["multi_leg_atomic_coordinator"] is True
    assert readiness["capabilities"]["maker_archive_trade_delta_restore"] is True
    assert readiness["capabilities"]["event_driven_book_validity"] is True
    assert (
        readiness["capabilities"]["elapsed_age_alone_invalidates_synced_book"]
        is False
    )
    assert readiness["capabilities"]["ttl_fallback_without_coverage_proof"] is False
    assert readiness["historical_source_authority"] == (
        "XUE_NATIVE_L2_BOUNDED_WINDOW_RESTORE_ONLY"
    )
    assert readiness["historical_coverage_scope"] == "PER_REQUEST_INTERVAL_ONLY"
    assert readiness["global_historical_coverage_proven"] is False
    assert readiness["historical_clock_policy"] == (
        "BASELINE_AND_RAW_EVENT_DUAL_CLOCK_WITH_FRAME_EVIDENCE"
    )
    assert readiness["historical_coverage_scope"] == "PER_REQUEST_INTERVAL_ONLY"
    assert readiness["global_historical_coverage_proven"] is False
    assert "pmxt_archive_required" not in readiness
    assert "agent_based_market_impact_supported" not in readiness
    assert readiness["market_impact_policy"] == (
        "VISIBLE_DEPTH_ONLY_WITH_UNMODELED_REMAINDER"
    )


def test_64_invalid_fee_schedule_is_a_request_error() -> None:
    with pytest.raises(Pml2RequestError, match="invalid fee schedule"):
        run_pml2_replay(
            {
                "profile": "optimistic",
                "events": [],
                "fee_schedules": [
                    {
                        "schedule_id": "invalid-fee",
                        "asset_id": "yes",
                        "condition_id": "condition-1",
                        "effective_from": (T0 - timedelta(days=1)).isoformat(),
                        "platform_fee_rate": "0.02",
                        "builder_taker_fee_bps": 101,
                    }
                ],
                "orders": [
                    {
                        "order_id": "invalid-fee-order",
                        "strategy_id": "strategy",
                        "condition_id": "condition-1",
                        "market_id": "market-1",
                        "asset_id": "yes",
                        "outcome": "YES",
                        "side": "BUY",
                        "size": "1",
                        "limit_price": "0.50",
                        "tif": "FAK",
                        "signal_ts": T0.isoformat(),
                    }
                ],
            }
        )


def test_65_missing_effective_fee_schedule_is_data_not_ready() -> None:
    order_ts = T0 + timedelta(seconds=1)
    with pytest.raises(Pml2DataNotReadyError, match="no fee schedule"):
        run_pml2_replay(
            {
                "profile": "optimistic",
                "events": [
                    {
                        "type": "SNAPSHOT",
                        "snapshot_id": "fee-gap-snapshot",
                        "condition_id": "condition-1",
                        "market_id": "market-1",
                        "asset_id": "yes",
                        "outcome": "YES",
                        "exchange_ts": T0.isoformat(),
                        "local_ts": T0.isoformat(),
                        "source": "native_l2",
                        "bids": [{"price": "0.49", "size": "10"}],
                        "asks": [{"price": "0.50", "size": "10"}],
                    }
                ],
                "fee_schedules": [
                    {
                        "schedule_id": "expired-fee",
                        "asset_id": "yes",
                        "condition_id": "condition-1",
                        "effective_from": (T0 - timedelta(days=1)).isoformat(),
                        "effective_until": T0.isoformat(),
                        "platform_fee_rate": "0.02",
                    }
                ],
                "orders": [
                    {
                        "order_id": "fee-gap-order",
                        "strategy_id": "strategy",
                        "condition_id": "condition-1",
                        "market_id": "market-1",
                        "asset_id": "yes",
                        "outcome": "YES",
                        "side": "BUY",
                        "size": "1",
                        "limit_price": "0.50",
                        "tif": "FAK",
                        "signal_ts": order_ts.isoformat(),
                        "entry_latency_ms": 0,
                        "response_latency_ms": 0,
                        "venue_delay_ms": 0,
                    }
                ],
            }
        )


def test_66_http_reports_missing_effective_fee_schedule_as_409() -> None:
    order_ts = T0 + timedelta(seconds=1)
    response = _api_client().post(
        "/quant/prediction-l2/v1/replay",
        json={
            "profile": "optimistic",
            "events": [
                {
                    "type": "SNAPSHOT",
                    "snapshot_id": "http-fee-gap-snapshot",
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "asset_id": "yes",
                    "outcome": "YES",
                    "exchange_ts": T0.isoformat(),
                    "local_ts": T0.isoformat(),
                    "source": "native_l2",
                    "bids": [{"price": "0.49", "size": "10"}],
                    "asks": [{"price": "0.50", "size": "10"}],
                }
            ],
            "fee_schedules": [
                {
                    "schedule_id": "http-expired-fee",
                    "asset_id": "yes",
                    "condition_id": "condition-1",
                    "effective_from": (T0 - timedelta(days=1)).isoformat(),
                    "effective_until": T0.isoformat(),
                    "platform_fee_rate": "0.02",
                }
            ],
            "orders": [
                {
                    "order_id": "http-fee-gap-order",
                    "strategy_id": "strategy",
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "asset_id": "yes",
                    "outcome": "YES",
                    "side": "BUY",
                    "size": "1",
                    "limit_price": "0.50",
                    "tif": "FAK",
                    "signal_ts": order_ts.isoformat(),
                    "entry_latency_ms": 0,
                    "response_latency_ms": 0,
                    "venue_delay_ms": 0,
                }
            ],
        },
    )

    assert response.status_code == 409
    assert response.get_json()["error_code"] == "PML2_HISTORICAL_DATA_NOT_READY"


def test_67_maker_survival_uses_hierarchical_proxy_without_claiming_live_calibration() -> None:
    result = run_pml2_maker_forecast(
        {
            "decisionTs": "2026-08-27T00:00:00Z",
            "horizonSeconds": 300,
            "category": "sports",
            "side": "BUY",
            "quotePosition": "AT_BEST",
            "queueBucket": "Q1_LE_1X",
        }
    )

    forecast = result["forecast"]
    assert forecast["domain_status"] == (
        "ORDERFILLED_PROXY_CALIBRATED_TRANSFER_UNVALIDATED"
    )
    assert Decimal(0) < Decimal(forecast["fill_probability"]) < Decimal(1)
    assert forecast["authenticated_trial_count"] == 4
    assert forecast["authenticated_promotion_allowed"] is False
    assert Decimal(0) < Decimal(forecast["stratum_weight"]) < Decimal(1)


def test_68_all_legs_fok_commits_both_legs_atomically() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(snapshot_id="yes-snapshot"))
    session.ingest_snapshot(
        _snapshot(
            snapshot_id="no-snapshot",
            outcome=Outcome.NO,
            bids=(("0.50", "10"),),
            asks=(("0.51", "10"),),
        )
    )
    group = OrderGroupIntent(
        run_id="run-1",
        group_id="atomic-group",
        strategy_id="strategy-1",
        policy=OrderGroupPolicy.ALL_LEGS_FOK,
        legs=(
            _order("atomic-yes", tif=TimeInForce.FOK),
            _order(
                "atomic-no",
                outcome=Outcome.NO,
                limit="0.51",
                tif=TimeInForce.FOK,
            ),
        ),
    )

    session.submit_order_group(group)
    session.run()

    assert session.group_result("atomic-group").status == "FILLED"
    assert session.result("atomic-yes").filled_size == Decimal("5.0000000000")
    assert session.result("atomic-no").filled_size == Decimal("5.0000000000")


def test_69_all_legs_fok_failure_does_not_consume_any_residual() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(snapshot_id="yes-snapshot"))
    session.ingest_snapshot(
        _snapshot(
            snapshot_id="no-snapshot",
            outcome=Outcome.NO,
            bids=(("0.50", "10"),),
            asks=(("0.51", "10"),),
        )
    )
    group = OrderGroupIntent(
        run_id="run-1",
        group_id="atomic-failure",
        strategy_id="strategy-1",
        policy=OrderGroupPolicy.ALL_LEGS_FOK,
        legs=(
            _order("failure-yes", tif=TimeInForce.FOK),
            _order(
                "failure-no",
                outcome=Outcome.NO,
                limit="0.49",
                tif=TimeInForce.FOK,
            ),
        ),
    )

    session.submit_order_group(group)
    session.run()

    assert session.group_result("atomic-failure").status == "FAILED"
    assert all(result.filled_size == 0 for result in session.results())
    assert all(
        Decimal(str(row["consumed_size"])) == 0
        for row in session.exchange_book.residual_snapshot()
    )


def test_70_hedge_on_leg_failure_submits_registered_hedge_after_failure() -> None:
    session = _session()
    session.ingest_snapshot(_snapshot(snapshot_id="yes-snapshot"))
    session.ingest_snapshot(
        _snapshot(
            snapshot_id="no-snapshot",
            outcome=Outcome.NO,
            bids=(("0.50", "10"),),
            asks=(("0.51", "10"),),
        )
    )
    group = OrderGroupIntent(
        run_id="run-1",
        group_id="hedged-group",
        strategy_id="strategy-1",
        policy=OrderGroupPolicy.HEDGE_ON_LEG_FAILURE,
        legs=(
            _order("hedged-yes"),
            _order("failed-no", outcome=Outcome.NO, limit="0.49"),
        ),
        hedge_legs=(
            _order(
                "exit-yes",
                side=RawOrderSide.SELL,
                limit="0.49",
            ),
        ),
    )

    session.submit_order_group(group)
    session.run()

    result = session.group_result("hedged-group")
    assert result.status == "HEDGED"
    assert result.executed_hedge_order_ids == ("exit-yes",)
    assert session.result("exit-yes").filled_size == Decimal("5.0000000000")


def test_71_counterfactual_impact_warning_keeps_visible_fak_fill() -> None:
    session = _session("realistic")
    session.ingest_snapshot(_snapshot())
    session.submit_order(_order("too-large", size="31"))

    session.run()

    result = session.result("too-large")
    assert result.status == OrderStatus.PARTIAL
    assert result.reason == "fak_remainder_cancelled"
    assert result.filled_size == Decimal("10.0000000000")
    assert result.counterfactual_impact is not None
    assert result.counterfactual_impact["decision"] == (
        "VISIBLE_DEPTH_ONLY_REMAINDER_UNMODELED"
    )
    assert session.report()["counterfactual_impact_rejection_count"] == 0
    assert session.report()["counterfactual_impact_warning_count"] == 1


def test_72_service_accepts_all_legs_fok_order_group() -> None:
    order_ts = T0 + timedelta(seconds=1)
    events = [
        {
            "type": "SNAPSHOT",
            "snapshot_id": "group-yes-snapshot",
            "condition_id": "condition-1",
            "market_id": "market-1",
            "asset_id": "yes",
            "outcome": "YES",
            "exchange_ts": T0.isoformat(),
            "local_ts": T0.isoformat(),
            "source": "native_l2",
            "bids": [{"price": "0.49", "size": "10"}],
            "asks": [{"price": "0.50", "size": "10"}],
        },
        {
            "type": "SNAPSHOT",
            "snapshot_id": "group-no-snapshot",
            "condition_id": "condition-1",
            "market_id": "market-1",
            "asset_id": "no",
            "outcome": "NO",
            "exchange_ts": T0.isoformat(),
            "local_ts": T0.isoformat(),
            "source": "native_l2",
            "bids": [{"price": "0.50", "size": "10"}],
            "asks": [{"price": "0.51", "size": "10"}],
        },
    ]

    def leg(order_id: str, asset_id: str, outcome: str, limit: str) -> dict[str, object]:
        return {
            "order_id": order_id,
            "condition_id": "condition-1",
            "market_id": "market-1",
            "asset_id": asset_id,
            "outcome": outcome,
            "side": "BUY",
            "size": "5",
            "limit_price": limit,
            "tif": "FOK",
            "signal_ts": order_ts.isoformat(),
            "entry_latency_ms": 0,
            "venue_delay_ms": 0,
        }

    result = run_pml2_replay(
        {
            "run_id": "group-api-run",
            "profile": "realistic",
            "events": events,
            "orderGroups": [
                {
                    "groupId": "api-atomic-group",
                    "strategyId": "api-strategy",
                    "policy": "ALL_LEGS_FOK",
                    "legs": [
                        leg("api-yes", "yes", "YES", "0.50"),
                        leg("api-no", "no", "NO", "0.51"),
                    ],
                }
            ],
        }
    )

    assert result["filled_size"] == "10.0000000000"
    assert result["order_groups"][0]["status"] == "FILLED"


def test_73_maker_forecast_http_route_and_strict_unknown_field_rejection() -> None:
    client = _api_client()
    response = client.post(
        "/quant/prediction-l2/v1/maker-forecast",
        json={
            "decisionTs": "2026-08-27T00:00:00Z",
            "horizonSeconds": 300,
            "category": "sports",
            "side": "BUY",
            "quotePosition": "AT_BEST",
            "queueBucket": "Q1_LE_1X",
        },
    )
    invalid = client.post(
        "/quant/prediction-l2/v1/maker-forecast",
        json={
            "decisionTs": "2026-08-27T00:00:00Z",
            "horizonSeconds": 300,
            "side": "BUY",
            "typo": True,
        },
    )

    assert response.status_code == 200
    assert response.get_json()["forecast"]["authenticated_promotion_allowed"] is False
    assert invalid.status_code == 400


def test_74_archive_restore_materializes_trade_and_delta_for_maker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from quant.backtest.pml2 import service as pml2_service
    from quant.backtest.pml2.adapters import Pml2ArchiveEventRestoreResult

    batch_at = T0 + timedelta(seconds=1, milliseconds=500)
    trade_at = T0 + timedelta(seconds=2)
    delta = BookDeltaEvent(
        event_id="xue-delta",
        condition_id="condition-1",
        market_id="market-1",
        asset_id="yes",
        outcome=Outcome.YES,
        exchange_ts=batch_at,
        local_ts=batch_at,
        book_epoch=0,
        side=EconomicBookSide.ASK,
        price=Decimal("0.50"),
        new_size=Decimal(9),
        source="xue_native_l2_archive",
    )
    events = (
        _snapshot(snapshot_id="xue-yes", source="xue_native_l2_archive"),
        _snapshot(
            snapshot_id="xue-no",
            outcome=Outcome.NO,
            bids=(("0.50", "10"),),
            asks=(("0.51", "10"),),
            source="xue_native_l2_archive",
        ),
        BookLevelBatchEvent(
            event_id="xue-delta-batch",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            exchange_ts=batch_at,
            local_ts=batch_at,
            book_epoch=0,
            updates=(delta,),
            source="xue_native_l2_archive",
        ),
        _trade(
            "xue-maker-trade",
            side=RawOrderSide.SELL,
            price="0.49",
            size="20",
            at=trade_at,
        ),
    )

    class FakeArchiveLoader:
        archive_dir = Path("/xue/native/l2")

        def restore_condition_events(self, **_: object) -> Pml2ArchiveEventRestoreResult:
            return Pml2ArchiveEventRestoreResult(
                events=events,
                source_files=("xue-fixture.parquet",),
                source="xue_native_l2_archive",
                restored=True,
                reason="xue_native_l2_event_timeline_restore_complete",
                row_count=4,
                source_manifest_hash="inventory-hash",
                snapshot_count=2,
                delta_count=1,
                trade_count=1,
                clock_verified=True,
                clock_evidence="BASELINE_AND_RAW_EVENT_DUAL_CLOCK_FRAME_VERIFIED",
                baseline_clock_count=2,
                raw_event_clock_count=2,
                frame_evidence_verified=True,
            )

    monkeypatch.setattr(pml2_service, "Pml2ArchiveSnapshotLoader", FakeArchiveLoader)
    result = run_pml2_replay(
        {
            "run_id": "xue-maker-restore",
            "profile": "realistic",
            "archive_restore": [
                {
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "yes_asset_id": "yes",
                    "no_asset_id": "no",
                    "start_time": T0.isoformat(),
                    "end_time": (T0 + timedelta(seconds=3)).isoformat(),
                    "maker_horizon_seconds": None,
                    "max_events": None,
                }
            ],
            "orders": [
                {
                    "order_id": "archive-maker",
                    "strategy_id": "strategy",
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "asset_id": "yes",
                    "outcome": "YES",
                    "side": "BUY",
                    "size": "5",
                    "limit_price": "0.49",
                    "tif": "GTC",
                    "signal_ts": (T0 + timedelta(seconds=1)).isoformat(),
                    "entry_latency_ms": 0,
                    "venue_delay_ms": 0,
                }
            ],
        }
    )

    restore = result["data_source_plan"]["restores"][0]
    assert restore["authority_scope"] == (
        "XUE_NATIVE_L2_BOUNDED_WINDOW_RESTORED_CLOCK_VERIFIED"
    )
    assert restore["coverage_scope"] == "REQUESTED_INTERVAL_ONLY"
    assert restore["delta_count"] == 1
    assert restore["trade_count"] == 1
    assert restore["clock_verified"] is True
    assert result["temporal_contract"]["cold_restore_exchange_clock_status"] == (
        "VERIFIED"
    )
    assert result["orders"][0]["filled_size"] == "5.0000000000"
    assert result["execution_matches"][0]["evidence_kind"] == (
        "TRADE_ADVANCED_ECONOMIC_MAKER_QUEUE"
    )


def test_75_http_reports_unreadable_routed_archive_as_409(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from quant.backtest.pml2 import service as pml2_service
    from quant.backtest.pml2.adapters import Pml2ArchiveEventRestoreResult

    captured: dict[str, object] = {}

    class BrokenRoutedArchiveLoader:
        archive_dir = Path("/xue/native/l2")

        def restore_condition_events(
            self,
            *,
            source_files: object = None,
            **_: object,
        ) -> Pml2ArchiveEventRestoreResult:
            captured["source_files"] = source_files
            return Pml2ArchiveEventRestoreResult(
                events=(),
                source_files=("routed-shard2.parquet",),
                source="xue_native_l2_archive",
                restored=False,
                reason=(
                    "L2ReplayNotReady: native L2 event window read failed: "
                    "routed shard2 is unreadable"
                ),
            )

    monkeypatch.setattr(
        pml2_service,
        "Pml2ArchiveSnapshotLoader",
        BrokenRoutedArchiveLoader,
    )
    response = _api_client().post(
        "/quant/prediction-l2/v1/replay",
        json={
            "profile": "optimistic",
            "archive_restore": [
                {
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "yes_asset_id": "yes",
                    "no_asset_id": "no",
                    "start_time": T0.isoformat(),
                    "end_time": (T0 + timedelta(seconds=2)).isoformat(),
                    "source_files": [
                        {
                            "path": "dt=2026-08-10/hour=16/routed-shard2.parquet",
                            "sha256": "a" * 64,
                        }
                    ],
                }
            ],
            "orders": [
                {
                    "order_id": "broken-routed-archive",
                    "strategy_id": "strategy",
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "asset_id": "yes",
                    "outcome": "YES",
                    "side": "BUY",
                    "size": "1",
                    "limit_price": "0.50",
                    "tif": "FAK",
                    "signal_ts": (T0 + timedelta(seconds=1)).isoformat(),
                    "entry_latency_ms": 0,
                    "venue_delay_ms": 0,
                }
            ],
        },
    )

    assert response.status_code == 409
    body = response.get_json()
    assert body["error_code"] == "PML2_HISTORICAL_DATA_NOT_READY"
    assert "routed shard2 is unreadable" in body["error"]
    assert captured["source_files"] == (
        {
            "path": "dt=2026-08-10/hour=16/routed-shard2.parquet",
            "sha256": "a" * 64,
        },
    )


def test_76_http_rejects_unrecognized_hash_bound_file_fields() -> None:
    response = _api_client().post(
        "/quant/prediction-l2/v1/replay",
        json={
            "profile": "optimistic",
            "archive_restore": [
                {
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "yes_asset_id": "yes",
                    "no_asset_id": "no",
                    "start_time": T0.isoformat(),
                    "end_time": (T0 + timedelta(seconds=2)).isoformat(),
                    "source_files": [
                        {
                            "path": "routed-shard2.parquet",
                            "sha256": "a" * 64,
                            "trusted": True,
                        }
                    ],
                }
            ],
            "orders": [
                {
                    "order_id": "invalid-source-file-field",
                    "strategy_id": "strategy",
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "asset_id": "yes",
                    "outcome": "YES",
                    "side": "BUY",
                    "size": "1",
                    "limit_price": "0.50",
                    "tif": "FAK",
                    "signal_ts": (T0 + timedelta(seconds=1)).isoformat(),
                    "entry_latency_ms": 0,
                    "venue_delay_ms": 0,
                }
            ],
        },
    )

    assert response.status_code == 400
    assert "unknown archive source_files entry fields" in response.get_json()[
        "error"
    ]


def test_77_chain_only_audit_preserves_execution_and_chunk_replay() -> None:
    def build(run_id: str) -> ReplayExecutionSession:
        session = ReplayExecutionSession(
            run_id=run_id,
            profile="realistic",
            audit_mode=ReplayAuditMode.CHAIN_ONLY,
        )
        session.ingest_snapshot(_snapshot())
        session.submit_order(replace(_order("chain-order"), run_id=run_id))
        return session

    one_shot = build("chain-run")
    one_shot.run()
    chunked = build("chain-run")
    chunked.run(until=T0 + timedelta(milliseconds=500))
    chunked.run()
    full = ReplayExecutionSession(run_id="chain-run", profile="realistic")
    full.ingest_snapshot(_snapshot())
    full.submit_order(replace(_order("chain-order"), run_id="chain-run"))
    full.run()

    assert one_shot.result("chain-order").as_dict() == chunked.result(
        "chain-order"
    ).as_dict()
    assert one_shot.replay_hash == chunked.replay_hash
    assert one_shot.execution_state_hash == chunked.execution_state_hash
    assert one_shot.execution_state_hash == full.execution_state_hash
    assert one_shot.audit_event_count == len(full.audit_events)
    assert one_shot.audit_events == []
