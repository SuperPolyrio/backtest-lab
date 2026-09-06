from datetime import datetime, timedelta, timezone
from decimal import Decimal

from quant.backtest.l2_orderfilled_execution import (
    BookDelta,
    BookLevel,
    BookSnapshot,
    FillTick,
    L2ExecutionConfig,
    L2OrderFilledExecutionModel,
    StrategyCancelIntent,
    StrategyOrderIntent,
    simulate_l2_depth_execution,
    normalize_orderfilled_event,
)


TS = datetime(2026, 6, 22, 7, 0, 0, tzinfo=timezone.utc)


def _snapshot() -> BookSnapshot:
    return BookSnapshot(
        ts=TS,
        market_id="market-1",
        asset_id="token-yes",
        sequence=1,
        source="pmxt",
        bids=(
            BookLevel(Decimal("0.45"), Decimal("1000")),
            BookLevel(Decimal("0.44"), Decimal("500")),
        ),
        asks=(
            BookLevel(Decimal("0.46"), Decimal("100")),
            BookLevel(Decimal("0.47"), Decimal("200")),
            BookLevel(Decimal("0.50"), Decimal("1000")),
        ),
        is_full_depth=True,
    )


def _intent(
    order_id: str,
    side: str,
    price: str,
    size: str,
    tif: str = "FAK",
    *,
    post_only: bool = False,
) -> StrategyOrderIntent:
    return StrategyOrderIntent(
        client_order_id=order_id,
        signal_ts=TS,
        market_id="market-1",
        asset_id="token-yes",
        side=side,
        order_type="LIMIT",
        limit_price=Decimal(price),
        size=Decimal(size),
        tif=tif,
        post_only=post_only,
    )


def _tick(size: str, *, side: str = "BUY", price: str = "0.45", log_index: int = 1) -> FillTick:
    return FillTick(
        ts=TS,
        block_number=88_000_001,
        tx_hash=f"0x{log_index}",
        log_index=log_index,
        order_hash=f"0xorder{log_index}",
        market_id="market-1",
        asset_id="token-yes",
        price=Decimal(price),
        size=Decimal(size),
        passive_side=side,
        aggressor_side="SELL" if side == "BUY" else "BUY",
        maker="0xmaker",
        taker="0xtaker",
        fee=Decimal("0"),
    )


def test_snapshot_and_delta_sort_levels_and_remove_zero_size_level() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1")))
    model.apply_snapshot(_snapshot())

    assert [level.price for level in model.book.remaining_bids()] == [Decimal("0.4500000000"), Decimal("0.4400000000")]
    assert [level.price for level in model.book.remaining_asks()] == [
        Decimal("0.4600000000"),
        Decimal("0.4700000000"),
        Decimal("0.5000000000"),
    ]

    applied = model.apply_delta(
        BookDelta(
            ts=TS,
            market_id="market-1",
            asset_id="token-yes",
            side="SELL",
            price=Decimal("0.46"),
            new_size=Decimal("0"),
            sequence=2,
            source="pmxt",
        )
    )

    assert applied is True
    assert [level.price for level in model.book.remaining_asks()] == [Decimal("0.4700000000"), Decimal("0.5000000000")]


def test_taker_buy_walks_asks_and_respects_limit_price() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1")))
    model.apply_snapshot(_snapshot())

    result = model.execute_taker(_intent("O-1", "BUY", "0.47", "250"))

    assert result.state == "FILLED"
    assert result.filled_size == Decimal("250.0000000000")
    assert result.avg_fill_price == Decimal("0.4660000000")
    assert [(fill.price, fill.size) for fill in result.fills] == [
        (Decimal("0.4600000000"), Decimal("100.0000000000")),
        (Decimal("0.4700000000"), Decimal("150.0000000000")),
    ]
    audit = result.audit_dict()
    assert audit["schema_version"] == "l2_orderfilled_execution_audit_v1"
    assert audit["state"] == "FILLED"
    assert audit["fill_count"] == 2
    assert audit["fills"][0]["liquidity_flag"] == "TAKER"
    assert audit["fills"][0]["reason"] == "taker_walk_visible_book"
    assert audit["submitted_at"] == TS.isoformat()
    assert audit["venue_received_at"] == (TS + timedelta(milliseconds=100)).isoformat()
    assert audit["book_snapshot_id"] == _snapshot().event_id
    assert audit["book_quality"]["confidence"] == "HIGH"
    assert audit["mode"] == "conservative"
    assert audit["invariant_violations"] == []


def test_taker_fok_rejects_without_committing_partial_residual() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1")))
    model.apply_snapshot(_snapshot())

    fok = model.execute_taker(_intent("O-FOK", "BUY", "0.47", "400", "FOK"))
    follow_up = model.execute_taker(_intent("O-2", "BUY", "0.46", "100", "FAK"))

    assert fok.state == "REJECTED"
    assert fok.filled_size == Decimal("0E-10")
    assert fok.reject_reason == "fok_insufficient_liquidity"
    assert follow_up.state == "FILLED"
    assert follow_up.filled_size == Decimal("100.0000000000")


def test_taker_residual_book_prevents_double_counting_same_depth() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1")))
    model.apply_snapshot(_snapshot())

    first = model.execute_taker(_intent("O-1", "BUY", "0.46", "80"))
    second = model.execute_taker(_intent("O-2", "BUY", "0.46", "80"))

    assert first.filled_size == Decimal("80.0000000000")
    assert second.filled_size == Decimal("20.0000000000")
    assert second.state == "PARTIAL"
    assert second.reject_reason == "unfilled_remainder_cancelled"


def test_post_only_crossing_order_is_rejected_before_taker_fill() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1")))
    model.apply_snapshot(_snapshot())

    result = model.execute_taker(_intent("O-POST", "BUY", "0.46", "10", "GTC", post_only=True))

    assert result.state == "REJECTED"
    assert result.reject_reason == "post_only_crosses_book"
    assert result.fills == ()


def test_maker_queue_requires_orderfilled_to_clear_queue_ahead_before_fill() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), submit_latency_ms=0))
    model.apply_snapshot(_snapshot())
    resting = model.admit_maker_order(_intent("M-1", "BUY", "0.45", "100", "GTC", post_only=True))

    first = model.process_fill_tick(_tick("500", side="BUY", price="0.45", log_index=1))
    second = model.process_fill_tick(_tick("600", side="BUY", price="0.45", log_index=2))

    assert resting.queue_ahead == Decimal("0")
    assert resting.queue_ahead_at_admit == Decimal("1000.0000000000")
    assert first == ()
    assert len(second) == 1
    assert second[0].size == Decimal("100.0000000000")
    assert second[0].queue_ahead_before == Decimal("500.0000000000")
    assert second[0].source_event_ids
    assert resting.state == "FILLED"


def test_maker_queue_ignores_wrong_side_or_wrong_price_fill_ticks() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), submit_latency_ms=0))
    model.apply_snapshot(_snapshot())
    resting = model.admit_maker_order(_intent("M-1", "BUY", "0.45", "100", "GTC", post_only=True))

    wrong_side = model.process_fill_tick(_tick("2000", side="SELL", price="0.45", log_index=1))
    wrong_price = model.process_fill_tick(_tick("2000", side="BUY", price="0.44", log_index=2))

    assert wrong_side == ()
    assert wrong_price == ()
    assert resting.remaining_size == Decimal("100.0000000000")
    assert resting.state == "WORKING"


def test_reconciled_queue_only_uses_configured_cancel_ahead_fraction() -> None:
    trade_only = L2OrderFilledExecutionModel(
        L2ExecutionConfig(depth_haircut=Decimal("1"), queue_mode="trade_only", cancel_ahead_fraction=Decimal("0"))
    )
    realistic = L2OrderFilledExecutionModel(
        L2ExecutionConfig(depth_haircut=Decimal("1"), queue_mode="reconciled", cancel_ahead_fraction=Decimal("0.5"))
    )
    optimistic = L2OrderFilledExecutionModel(
        L2ExecutionConfig(depth_haircut=Decimal("1"), queue_mode="optimistic", cancel_ahead_fraction=Decimal("1"))
    )
    for model in (trade_only, realistic, optimistic):
        model.apply_snapshot(_snapshot())
        model.admit_maker_order(_intent("M-1", "BUY", "0.45", "100", "GTC", post_only=True))

    assert trade_only.reconcile_lob_change(
        asset_id="token-yes",
        side="BUY",
        price=Decimal("0.45"),
        old_size=Decimal("1000"),
        new_size=Decimal("500"),
        orderfilled_size=Decimal("400"),
    ) == Decimal("600.0000000000")
    assert realistic.reconcile_lob_change(
        asset_id="token-yes",
        side="BUY",
        price=Decimal("0.45"),
        old_size=Decimal("1000"),
        new_size=Decimal("500"),
        orderfilled_size=Decimal("400"),
    ) == Decimal("550.0000000000")
    assert optimistic.reconcile_lob_change(
        asset_id="token-yes",
        side="BUY",
        price=Decimal("0.45"),
        old_size=Decimal("1000"),
        new_size=Decimal("500"),
        orderfilled_size=Decimal("400"),
    ) == Decimal("500.0000000000")


def test_snapshot_update_preserves_resting_maker_queue() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), submit_latency_ms=0))
    model.apply_snapshot(_snapshot())
    resting = model.admit_maker_order(_intent("M-1", "BUY", "0.45", "100", "GTC", post_only=True))
    model.apply_snapshot(
        BookSnapshot(
            ts=TS + timedelta(seconds=1),
            market_id="market-1",
            asset_id="token-yes",
            sequence=2,
            source="pmxt",
            bids=(BookLevel(Decimal("0.45"), Decimal("1000")),),
            asks=(BookLevel(Decimal("0.46"), Decimal("100")),),
            is_full_depth=True,
        )
    )

    fills = model.process_fill_tick(
        FillTick(
            ts=TS + timedelta(seconds=2),
            block_number=88_000_003,
            tx_hash="0xqueue",
            log_index=5,
            order_hash="0xorder5",
            market_id="market-1",
            asset_id="token-yes",
            price=Decimal("0.45"),
            size=Decimal("1100"),
            passive_side="BUY",
            aggressor_side="SELL",
            maker="0xmaker",
            taker="0xtaker",
            fee=Decimal("0"),
        )
    )

    assert resting.state == "FILLED"
    assert len(fills) == 1


def test_snapshot_decrease_reconciles_queue_only_when_enabled() -> None:
    trade_only = L2OrderFilledExecutionModel(
        L2ExecutionConfig(depth_haircut=Decimal("1"), queue_mode="trade_only", use_lob_decrease_for_queue=False, submit_latency_ms=0)
    )
    reconciled = L2OrderFilledExecutionModel(
        L2ExecutionConfig(
            depth_haircut=Decimal("1"),
            queue_mode="reconciled",
            cancel_ahead_fraction=Decimal("0.5"),
            use_lob_decrease_for_queue=True,
            submit_latency_ms=0,
        )
    )
    for model in (trade_only, reconciled):
        model.apply_snapshot(_snapshot())
        model.admit_maker_order(_intent("M-1", "BUY", "0.45", "100", "GTC", post_only=True))
        model.apply_snapshot(
            BookSnapshot(
                ts=TS + timedelta(seconds=1),
                market_id="market-1",
                asset_id="token-yes",
                sequence=2,
                source="pmxt",
                bids=(BookLevel(Decimal("0.45"), Decimal("500")),),
                asks=(BookLevel(Decimal("0.46"), Decimal("100")),),
                is_full_depth=True,
            )
        )

    assert trade_only.queues[("token-yes", "BUY", Decimal("0.4500000000"))].env_ahead == Decimal("1000.0000000000")
    assert reconciled.queues[("token-yes", "BUY", Decimal("0.4500000000"))].env_ahead == Decimal("750.0000000000")


def test_taker_adverse_impact_is_optional_and_reported_in_fill_price() -> None:
    no_impact = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), impact_strength_bps=Decimal("0")))
    impacted = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), impact_strength_bps=Decimal("100")))
    for model in (no_impact, impacted):
        model.apply_snapshot(_snapshot())

    plain = no_impact.execute_taker(_intent("plain", "BUY", "0.46", "100"))
    adverse = impacted.execute_taker(_intent("adverse", "BUY", "0.46", "100"))

    assert plain.avg_fill_price == Decimal("0.4600000000")
    assert adverse.avg_fill_price > plain.avg_fill_price


def test_depth_adapter_reports_execution_config_matrix_and_capacity_fields() -> None:
    class Params:
        execution_profile = "realistic"
        allow_partial_fill = True
        buy_limit_price = Decimal("0.46")
        position_size = Decimal("46")
        liquidity_cap_pct = Decimal("100")
        min_fill_pct = Decimal("0")
        fee_bps = Decimal("0")
        impact_strength_bps = Decimal("25")
        reject_on_stale_book = True

    fill = simulate_l2_depth_execution(
        snapshots=[_snapshot()],
        decision_block=88_000_001,
        decision_timestamp=TS,
        side="BUY",
        target_size=Decimal("100"),
        signal_price=Decimal("0.46"),
        params=Params(),
        market_id="market-1",
        asset_id="token-yes",
    )

    assert fill["execution_config"]["mode"] == "realistic"
    assert set(fill["execution_profile_config_matrix"]) == {"conservative", "realistic", "optimistic"}
    assert fill["execution_profile_config_matrix"]["conservative"]["queue_mode"] == "trade_only"
    assert fill["visible_depth_size"] == Decimal("1300.0000000000")
    assert fill["order_size_to_visible_depth_pct"] == Decimal("7.6923")
    assert fill["execution_audit"]["submitted_at"] == TS.isoformat()
    assert fill["execution_audit"]["venue_received_at"] == (TS + timedelta(milliseconds=100)).isoformat()
    assert fill["execution_audit"]["invariant_violations"] == []


def test_depth_adapter_rejected_no_book_still_reports_execution_config() -> None:
    class Params:
        execution_profile = "conservative"
        position_size = Decimal("46")
        liquidity_cap_pct = Decimal("100")
        min_fill_pct = Decimal("0")

    fill = simulate_l2_depth_execution(
        snapshots=[],
        decision_block=88_000_001,
        decision_timestamp=TS,
        side="BUY",
        target_size=Decimal("100"),
        signal_price=Decimal("0.46"),
        params=Params(),
        market_id="market-1",
        asset_id="token-yes",
    )

    assert fill["rejected"] is True
    assert fill["execution_config"]["mode"] == "conservative"
    assert fill["execution_audit"]["reject_reason"] == "no historical book snapshot"
    assert fill["execution_audit"]["submitted_at"] == TS.isoformat()


def test_agent_same_price_fifo_after_environment_queue() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), submit_latency_ms=0))
    model.apply_snapshot(_snapshot())
    first = model.admit_maker_order(_intent("A", "BUY", "0.45", "50", "GTC", post_only=True))
    second = model.admit_maker_order(_intent("B", "BUY", "0.45", "50", "GTC", post_only=True))

    fills = model.process_fill_tick(_tick("1075", side="BUY", price="0.45", log_index=1))

    assert first.state == "FILLED"
    assert second.state == "PARTIAL"
    assert first.remaining_size == Decimal("0E-10")
    assert second.remaining_size == Decimal("25.0000000000")
    assert [(fill.order_id, fill.size) for fill in fills] == [
        ("A", Decimal("50.0000000000")),
        ("B", Decimal("25.0000000000")),
    ]


def test_orderfilled_normalizer_maps_maker_buy_to_passive_buy_fill_tick() -> None:
    normalized = normalize_orderfilled_event(
        {
            "tx_hash": "0xabc",
            "log_index": 7,
            "order_hash": "0xorder",
            "block_number": 100,
            "block_timestamp": "2026-06-22T07:00:00+00:00",
            "makerAssetId": "0",
            "makerAmountFilled": "45000000",
            "takerAssetId": "YES_TOKEN",
            "takerAmountFilled": "100000000",
            "maker": "0xmaker",
            "taker": "0xtaker",
            "fee": "1234",
        },
        market_id="market-1",
    )

    assert normalized.status == "ready"
    assert normalized.reason == "normalized"
    assert normalized.canonical_fill_key == "0xabc|7|0xorder|0|yes_token"
    assert normalized.fill_tick is not None
    assert normalized.fill_tick.passive_side == "BUY"
    assert normalized.fill_tick.aggressor_side == "SELL"
    assert normalized.fill_tick.asset_id == "YES_TOKEN"
    assert normalized.fill_tick.size == Decimal("100.0000000000")
    assert normalized.fill_tick.price == Decimal("0.4500000000")
    assert normalized.fill_tick.fee == Decimal("0.001234")


def test_orderfilled_normalizer_maps_maker_sell_to_passive_sell_fill_tick() -> None:
    normalized = normalize_orderfilled_event(
        {
            "tx_hash": "0xdef",
            "log_index": 8,
            "order_hash": "0xorder2",
            "makerAssetId": "YES_TOKEN",
            "makerAmountFilled": "100000000",
            "takerAssetId": "0",
            "takerAmountFilled": "45000000",
        },
        market_id="market-1",
    )

    assert normalized.status == "ready"
    assert normalized.fill_tick is not None
    assert normalized.fill_tick.passive_side == "SELL"
    assert normalized.fill_tick.aggressor_side == "BUY"
    assert normalized.fill_tick.asset_id == "YES_TOKEN"
    assert normalized.fill_tick.size == Decimal("100.0000000000")
    assert normalized.fill_tick.price == Decimal("0.4500000000")


def test_orderfilled_normalizer_quarantines_unsupported_or_out_of_bounds_events() -> None:
    unsupported = normalize_orderfilled_event(
        {
            "tx_hash": "0xabc",
            "log_index": 1,
            "order_hash": "0xorder",
            "makerAssetId": "TOKEN_A",
            "makerAmountFilled": "1000000",
            "takerAssetId": "TOKEN_B",
            "takerAmountFilled": "1000000",
        }
    )
    bad_price = normalize_orderfilled_event(
        {
            "tx_hash": "0xabc",
            "log_index": 2,
            "order_hash": "0xorder",
            "makerAssetId": "0",
            "makerAmountFilled": "200000000",
            "takerAssetId": "YES_TOKEN",
            "takerAmountFilled": "100000000",
        }
    )

    assert unsupported.status == "quarantine"
    assert unsupported.reason == "unsupported_asset_path"
    assert unsupported.fill_tick is None
    assert bad_price.status == "quarantine"
    assert bad_price.reason == "price_out_of_bounds"
    assert bad_price.fill_tick is None


def test_timeline_sorts_non_monotonic_events_and_is_deterministic() -> None:
    first = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), submit_latency_ms=0))
    second = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), submit_latency_ms=0))
    late_intent = _intent("O-1", "BUY", "0.46", "75")
    late_intent.signal_ts = TS + timedelta(seconds=1)
    events = [late_intent, _snapshot()]

    first_result = first.run_event_timeline(events)
    second_result = second.run_event_timeline(events)

    assert first_result.corrections == ("non_monotonic_events_sorted",)
    assert first_result.fills == second_result.fills
    assert first_result.order_results["O-1"].state == "FILLED"
    assert first_result.audit_dict() == second_result.audit_dict()
    assert first_result.audit_dict()["invariant_violations"] == {}


def test_timeline_taker_rejects_when_no_book_is_available() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(submit_latency_ms=0))

    result = model.run_event_timeline([_intent("O-1", "BUY", "0.46", "10")])

    assert result.fills == ()
    assert result.order_results["O-1"].state == "REJECTED"
    assert result.order_results["O-1"].reject_reason == "book_stale_or_missing"


def test_timeline_maker_does_not_fill_without_orderfilled_tick() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), submit_latency_ms=0))

    result = model.run_event_timeline([_snapshot(), _intent("M-1", "BUY", "0.45", "100", "GTC", post_only=True)])

    assert result.fills == ()
    assert result.order_results["M-1"].state == "WORKING"
    assert result.order_results["M-1"].resting_order is not None
    assert result.order_results["M-1"].resting_order.queue_ahead == Decimal("1000.0000000000")


def test_timeline_maker_fills_only_after_orderfilled_tick_consumes_queue() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), submit_latency_ms=0))

    result = model.run_event_timeline(
        [
            _snapshot(),
            _intent("M-1", "BUY", "0.45", "100", "GTC", post_only=True),
            FillTick(
                ts=TS + timedelta(seconds=1),
                block_number=88_000_002,
                tx_hash="0x3",
                log_index=3,
                order_hash="0xorder3",
                market_id="market-1",
                asset_id="token-yes",
                price=Decimal("0.45"),
                size=Decimal("1100"),
                passive_side="BUY",
                aggressor_side="SELL",
                maker="0xmaker",
                taker="0xtaker",
                fee=Decimal("0"),
            ),
        ]
    )

    assert len(result.fills) == 1
    assert result.fills[0].liquidity_flag == "MAKER"
    assert result.order_results["M-1"].state == "FILLED"
    assert result.order_results["M-1"].audit_dict()["source_event_ids"] == [result.fills[0].source_event_ids[0]]


def test_timeline_cancel_prevents_later_maker_fill() -> None:
    model = L2OrderFilledExecutionModel(L2ExecutionConfig(depth_haircut=Decimal("1"), submit_latency_ms=0))

    result = model.run_event_timeline(
        [
            _snapshot(),
            _intent("M-1", "BUY", "0.45", "100", "GTC", post_only=True),
            StrategyCancelIntent(
                client_order_id="M-1",
                signal_ts=TS + timedelta(seconds=1),
                market_id="market-1",
                asset_id="token-yes",
                cancel_latency_ms=0,
            ),
            _tick("2000", side="BUY", price="0.45", log_index=4),
        ]
    )

    assert result.fills == ()
    assert result.order_results["M-1"].state == "CANCELLED"
    assert result.order_results["M-1"].reject_reason == "cancelled_by_strategy"
