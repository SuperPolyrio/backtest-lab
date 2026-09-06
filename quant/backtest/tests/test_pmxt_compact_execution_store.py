from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from quant.backtest.backtest_engine import (
    BacktestParameters,
    PricePoint,
    _fill_decision,
    _schedule_lob_maker_order,
    build_pmxt_compact_book_provider,
    parse_parameters,
    simulate_strategy,
)
from quant.backtest.execution import BookSnapshot
from quant.backtest.l2_orderfilled_execution import ExecutionFill, L2ExecutionConfig, OrderExecutionResult
from quant.backtest.pmxt_compact_execution_store import PmxtCompactBookProvider


UTC = timezone.utc


class _CompactClient:
    def __init__(self, *, available: bool = True, with_snapshot: bool = True) -> None:
        self.available_value = available
        self.with_snapshot = with_snapshot
        self.queries: list[str] = []

    def query_json_rows(self, query: str, **_kwargs):
        self.queries.append(query)
        if "SELECT 1 AS available" in query:
            return [{"available": 1}] if self.available_value else []
        if "event_type='book_snapshot'" in query and "ORDER BY event_time DESC" in query:
            if not self.with_snapshot:
                return []
            return [
                {
                    "event_time_text": "2026-06-22 12:00:00.000",
                    "source_row_index": 10,
                    "source_event_index": 0,
                    "source_hash": "snapshot-hash",
                }
            ]
        if "FROM pmxt_l2_book_level_compact" in query:
            return [
                {"side": "bid", "level_index": 0, "price": "0.49", "size": "100"},
                {"side": "bid", "level_index": 1, "price": "0.48", "size": "50"},
                {"side": "ask", "level_index": 0, "price": "0.51", "size": "80"},
                {"side": "ask", "level_index": 1, "price": "0.52", "size": "40"},
            ]
        if "event_type='price_change'" in query:
            return [
                {
                    "event_time_text": "2026-06-22 12:00:01.000",
                    "operation": "upsert",
                    "side": "ask",
                    "price": "0.51",
                    "size": "60",
                    "source_row_index": 11,
                    "source_event_index": 0,
                    "source_hash": "delta-1",
                    "best_bid": "0.49",
                    "best_ask": "0.51",
                },
                {
                    "event_time_text": "2026-06-22 12:00:02.000",
                    "operation": "delete",
                    "side": "bid",
                    "price": "0.48",
                    "size": "0",
                    "source_row_index": 12,
                    "source_event_index": 0,
                    "source_hash": "delta-2",
                    "best_bid": "0.49",
                    "best_ask": "0.51",
                },
            ]
        raise AssertionError(query)


def _provider(client: _CompactClient) -> PmxtCompactBookProvider:
    return PmxtCompactBookProvider(
        condition_id="0xcondition",
        token_id="12345",
        market_id=42,
        token_side="YES",
        client=client,
    )


def test_compact_provider_reconstructs_snapshot_and_deltas_at_order_time() -> None:
    client = _CompactClient()
    provider = _provider(client)

    snapshot = provider.snapshot_at(datetime(2026, 6, 22, 12, 0, 3, tzinfo=UTC))

    assert snapshot is not None
    assert snapshot.snapshot_id <= 2**53 - 1
    assert snapshot.source == "pmxt_l2_compact"
    assert snapshot.timestamp == datetime(2026, 6, 22, 12, 0, 2, tzinfo=UTC)
    assert snapshot.bids == ((Decimal("0.49"), Decimal("100")),)
    assert snapshot.asks[0] == (Decimal("0.51"), Decimal("60"))
    assert provider.context()["hit_count"] == 1
    assert provider.context()["loaded_delta_rows"] == 2
    assert all("condition_id='0xcondition'" in query for query in client.queries)
    assert all("token_id='12345'" in query for query in client.queries)


def test_compact_provider_returns_no_book_without_snapshot_anchor() -> None:
    provider = _provider(_CompactClient(with_snapshot=False))

    assert provider.snapshot_at(datetime(2026, 6, 22, 12, 0, 3, tzinfo=UTC)) is None
    assert provider.context()["no_snapshot_count"] == 1
    assert provider.context()["last_result"]["status"] == "no_snapshot"


class _MetadataCursor:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _query, _params):
        return None

    def fetchone(self):
        return {
            "market_id": 42,
            "condition_id": "0xCondition",
            "token_id": "12345",
            "token_id_hex": "0x3039",
            "market_slug": "market-a",
            "token_side": "YES",
        }


class _MetadataConnection:
    def cursor(self):
        return _MetadataCursor()


def test_formal_provider_selection_requires_real_compact_rows() -> None:
    provider, context = build_pmxt_compact_book_provider(
        _MetadataConnection(),
        {"market_id": 42, "market_slug": "market-a", "token_side": "YES", "meta": {}},
        client=_CompactClient(available=True),
    )

    assert provider is not None
    assert provider.condition_id == "0xcondition"
    assert provider.orderfilled_token_id == "0x3039"
    assert context["status"] == "configured"
    assert context["fallback_used"] is False


def test_formal_provider_selection_reports_fallback_without_compact_rows() -> None:
    provider, context = build_pmxt_compact_book_provider(
        _MetadataConnection(),
        {"market_id": 42, "market_slug": "market-a", "token_side": "YES", "meta": {}},
        client=_CompactClient(available=False),
    )

    assert provider is None
    assert context["status"] == "fallback"
    assert context["reason"] == "no compact PMXT rows for market token"


def test_nested_frontend_parameters_are_applied_with_top_level_precedence() -> None:
    params = parse_parameters(
        {
            "execution_price_mode": "ORDERFILLED_LOB",
            "entry_threshold": "0.20",
            "parameters": {
                "entry_threshold": "0.10",
                "position_size": "10",
                "buy_limit_price": "0.99",
            },
        }
    )

    assert params.execution_price_mode == "ORDERFILLED_LOB"
    assert params.entry_threshold == Decimal("0.20")
    assert params.position_size == Decimal("10")
    assert params.buy_limit_price == Decimal("0.99")


class _PointProvider:
    def __init__(self) -> None:
        self.targets: list[datetime] = []

    def snapshot_at(self, target: datetime) -> BookSnapshot:
        self.targets.append(target)
        return BookSnapshot(
            snapshot_id=99,
            token_id="12345",
            side="YES",
            bids=((Decimal("0.49"), Decimal("100")),),
            asks=((Decimal("0.51"), Decimal("80")),),
            source="pmxt_l2_compact",
            book_status="ok",
            timestamp=target,
            captured_at=target,
            snapshot_version="compact-99",
        )


def test_orderfilled_lob_fill_uses_compact_provider_instead_of_pg_snapshots() -> None:
    provider = _PointProvider()
    point = PricePoint(
        x_value=100,
        price=Decimal("0.50"),
        volume=Decimal("100"),
        trade_count=5,
        timestamp=datetime(2026, 6, 22, 12, 0, tzinfo=UTC),
    )
    params = BacktestParameters(
        execution_price_mode="ORDERFILLED_LOB",
        execution_profile="optimistic",
        order_role="taker",
        position_size=Decimal("10"),
        liquidity_cap_pct=Decimal("100"),
        fill_probability_haircut_pct=Decimal("0"),
    )
    run = {
        "market_id": 42,
        "price_source": "orderfilled_block_close",
        "meta": {"token_id": "12345"},
        "_pmxt_compact_book_provider": provider,
        "_clob_snapshots": [],
    }

    fill = _fill_decision(params, point, run, "BUY_YES")

    assert provider.targets == [point.timestamp]
    assert fill["book_snapshot_id"] == 99
    assert fill["execution_source"] == "l2_orderfilled"
    assert fill["book_quality"]["source"] == "pmxt_l2_compact"


class _MakerTimelineClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def query_json_rows(self, query: str, **_kwargs):
        self.queries.append(query)
        if "event_type='book_snapshot'" in query:
            return [{"event_time_text": "2026-06-22 12:00:00.000", "source_row_index": 10, "source_event_index": 0, "source_hash": "s1"}]
        if "FROM pmxt_l2_book_level_compact" in query:
            return [
                {"side": "bid", "level_index": 0, "price": "0.49", "size": "5"},
                {"side": "ask", "level_index": 0, "price": "0.51", "size": "8"},
            ]
        if "event_type='price_change'" in query:
            return []
        if "FROM maker_fill_ticks" in query:
            return [
                {
                    "fill_id": "fill-1",
                    "block_number": 100,
                    "transaction_index": 1,
                    "log_index": 2,
                    "tx_hash": "0xfirst",
                    "order_hash": "order-1",
                    "trade_price": "0.49",
                    "size": "4",
                    "passive_side": "BUY",
                    "aggressor_side": "SELL",
                    "maker": "0xmaker",
                    "taker": "0xtaker",
                    "block_ts_ms": 1782129610000,
                    "fee": "0",
                },
                {
                    "fill_id": "fill-2",
                    "block_number": 101,
                    "transaction_index": 1,
                    "log_index": 3,
                    "tx_hash": "0xsecond",
                    "order_hash": "order-2",
                    "trade_price": "0.49",
                    "size": "3",
                    "passive_side": "BUY",
                    "aggressor_side": "SELL",
                    "maker": "0xmaker",
                    "taker": "0xtaker",
                    "block_ts_ms": 1782129620000,
                    "fee": "0",
                },
            ]
        raise AssertionError(query)


def test_maker_timeline_consumes_visible_queue_before_filling_order() -> None:
    provider = PmxtCompactBookProvider(
        condition_id="0xcondition",
        token_id="12345",
        orderfilled_token_id="0x3039",
        market_id=42,
        token_side="YES",
        client=_MakerTimelineClient(),
    )
    result = provider.execute_maker_timeline(
        client_order_id="O-1",
        signal_ts=datetime(2026, 6, 22, 12, 0, 3, tzinfo=UTC),
        end_ts=datetime(2026, 6, 22, 12, 1, tzinfo=UTC),
        side="BUY_YES",
        limit_price=Decimal("0.49"),
        size=Decimal("2"),
        config=L2ExecutionConfig(submit_latency_ms=0, queue_ahead_fraction=Decimal("1")),
    )

    assert result is not None
    assert result.state == "FILLED"
    assert result.filled_size == Decimal("2.0000000000")
    assert len(result.fills) == 1
    assert result.fills[0].source_event_ids
    assert result.queue_ahead_at_admit == Decimal("5.0000000000")
    assert provider.context()["loaded_fill_rows"] == 2
    assert provider.context()["maker_timeline_count"] == 1
    assert any("asset_id='0x3039'" in query for query in provider.client.queries)


def test_maker_timeline_cancel_before_historical_fill_stays_unfilled() -> None:
    provider = PmxtCompactBookProvider(
        condition_id="0xcondition",
        token_id="12345",
        orderfilled_token_id="0x3039",
        market_id=42,
        token_side="YES",
        client=_MakerTimelineClient(),
    )
    result = provider.execute_maker_timeline(
        client_order_id="O-2",
        signal_ts=datetime(2026, 6, 22, 12, 0, 3, tzinfo=UTC),
        end_ts=datetime(2026, 6, 22, 12, 1, tzinfo=UTC),
        cancel_ts=datetime(2026, 6, 22, 12, 0, 5, tzinfo=UTC),
        side="BUY_YES",
        limit_price=Decimal("0.49"),
        size=Decimal("2"),
        config=L2ExecutionConfig(submit_latency_ms=0, cancel_latency_ms=0),
    )

    assert result is not None
    assert result.state == "CANCELLED"
    assert result.filled_size == Decimal("0E-10")


class _FilledMakerProvider:
    def __init__(self, fill_ts: datetime) -> None:
        self.fill_ts = fill_ts
        self.calls: list[dict] = []

    def execute_maker_timeline(self, **kwargs):
        self.calls.append(kwargs)
        fill = ExecutionFill(
            order_id=kwargs["client_order_id"],
            ts=self.fill_ts,
            asset_id="12345",
            side="BUY",
            liquidity_flag="MAKER",
            price=Decimal("0.49"),
            size=Decimal("10"),
            fee=Decimal("0"),
            source_event_ids=("real-fill-1",),
            reason="maker_queue_consumed_by_orderfilled",
            book_ts=datetime(2026, 6, 22, 12, 0, tzinfo=UTC),
            queue_ahead_before=Decimal("0"),
            queue_ahead_after=Decimal("0"),
        )
        return OrderExecutionResult(
            order_id=kwargs["client_order_id"],
            state="FILLED",
            fills=(fill,),
            remaining_size=Decimal("0"),
            submitted_at=kwargs["signal_ts"],
            venue_received_at=kwargs["signal_ts"],
            book_snapshot_id="snapshot-1",
            queue_ahead_at_admit=Decimal("5"),
            mode="realistic",
        )


def test_formal_maker_strategy_activates_position_on_fill_tick_not_signal_bar() -> None:
    timestamps = [
        datetime(2026, 6, 22, 12, 0, tzinfo=UTC),
        datetime(2026, 6, 22, 12, 1, tzinfo=UTC),
        datetime(2026, 6, 22, 12, 2, tzinfo=UTC),
    ]
    points = [
        PricePoint(x_value=100 + idx, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1, timestamp=ts)
        for idx, ts in enumerate(timestamps)
    ]
    provider = _FilledMakerProvider(timestamps[1])
    params = BacktestParameters(
        execution_price_mode="ORDERFILLED_LOB",
        execution_profile="realistic",
        order_role="maker",
        entry_threshold=Decimal("0.58"),
        buy_limit_price=Decimal("0.49"),
        position_size=Decimal("4.9"),
        take_profit=Decimal("10"),
        stop_loss=Decimal("0.99"),
        settlement_value=Decimal("1"),
    )
    run = {
        "market_id": 42,
        "market_slug": "market-a",
        "token_side": "YES",
        "price_source": "orderfilled_block_close",
        "meta": {"token_id": "12345"},
        "_pmxt_compact_book_provider": provider,
    }

    result = simulate_strategy(points, run, params)

    assert len(provider.calls) == 1
    assert result["orders"][0]["execution_source"] == "pmxt_l2_orderfilled_maker_timeline"
    open_event = next(event for event in result["events"] if event["event_type"] == "open")
    assert open_event["x_value"] == 101
    assert result["orders"][0]["meta"]["execution_model_evidence"]["fill_timestamps"] == [timestamps[1].isoformat()]
    assert result["trades"][0]["exit_reason"] == "settlement"


class _PartialLifecycleProvider:
    def __init__(self, timestamps: list[datetime]) -> None:
        self.timestamps = timestamps
        self.calls: list[dict] = []

    def execute_maker_timeline(self, **kwargs):
        self.calls.append(kwargs)
        is_entry = str(kwargs["side"]).startswith("BUY")
        sizes = (Decimal("4"), Decimal("6")) if is_entry else (Decimal("3"), Decimal("7"))
        fill_times = self.timestamps[1:3] if is_entry else self.timestamps[3:5]
        fills = tuple(
            ExecutionFill(
                order_id=kwargs["client_order_id"],
                ts=ts,
                asset_id="12345",
                side="BUY" if is_entry else "SELL",
                liquidity_flag="MAKER",
                price=kwargs["limit_price"],
                size=size,
                fee=Decimal("0"),
                source_event_ids=(f"real-fill-{len(self.calls)}-{idx}",),
                reason="maker_queue_consumed_by_orderfilled",
                book_ts=ts,
                queue_ahead_before=Decimal("1") if idx == 0 else Decimal("0"),
                queue_ahead_after=Decimal("0"),
            )
            for idx, (ts, size) in enumerate(zip(fill_times, sizes), start=1)
        )
        return OrderExecutionResult(
            order_id=kwargs["client_order_id"],
            state="FILLED",
            fills=fills,
            remaining_size=Decimal("0"),
            submitted_at=kwargs["signal_ts"],
            venue_received_at=kwargs["signal_ts"],
            book_snapshot_id=f"snapshot-{len(self.calls)}",
            queue_ahead_at_admit=Decimal("1"),
            mode="realistic",
        )


def test_formal_maker_strategy_applies_partial_entry_and_exit_fill_ticks_incrementally() -> None:
    timestamps = [datetime(2026, 6, 22, 12, minute, tzinfo=UTC) for minute in range(6)]
    points = [
        PricePoint(
            x_value=100 + idx,
            price=Decimal("0.60") if idx < 4 else Decimal("0.50"),
            volume=Decimal("10"),
            trade_count=1,
            timestamp=ts,
        )
        for idx, ts in enumerate(timestamps)
    ]
    provider = _PartialLifecycleProvider(timestamps)
    params = BacktestParameters(
        execution_price_mode="ORDERFILLED_LOB",
        execution_profile="realistic",
        order_role="maker",
        entry_threshold=Decimal("0.58"),
        buy_limit_price=Decimal("0.49"),
        position_size=Decimal("4.9"),
        max_holding_bars=1,
        take_profit=Decimal("0.10"),
        stop_loss=Decimal("0.99"),
        maker_fee_bps=Decimal("10"),
    )
    run = {
        "market_id": 42,
        "market_slug": "market-a",
        "token_side": "YES",
        "price_source": "orderfilled_block_close",
        "meta": {"token_id": "12345"},
        "_pmxt_compact_book_provider": provider,
    }

    result = simulate_strategy(points, run, params)

    assert [call["side"] for call in provider.calls] == ["BUY_YES", "SELL_YES"]
    assert [trade["size"] for trade in result["trades"]] == [Decimal("3.0000000000"), Decimal("7.0000000000")]
    assert [trade["exit_x"] for trade in result["trades"]] == [103, 104]
    entry_order = result["orders"][0]
    assert entry_order["filled_size"] == Decimal("10.0000000000")
    assert entry_order["meta"]["execution_model_evidence"]["position_activation"] == "each_fill_tick"
    assert entry_order["meta"]["execution_model_evidence"]["fill_timestamps"] == [
        timestamps[1].isoformat(),
        timestamps[2].isoformat(),
    ]
    position_events = [(event["event_type"], event["x_value"]) for event in result["events"]]
    assert ("open", 101) in position_events
    assert ("buy_partial_fill", 102) in position_events
    assert ("sell_partial_fill", 103) in position_events
    assert ("close", 104) in position_events
    trade_ledger = [row for row in result["ledger"] if row["event_type"] in {"BUY", "SELL"}]
    assert [row["event_type"] for row in trade_ledger] == ["BUY", "BUY", "SELL", "SELL"]
    assert [row["shares_delta"] for row in trade_ledger[:2]] == [Decimal("4.0000000000"), Decimal("6.0000000000")]
    assert [row["x_value"] for row in trade_ledger[:2]] == [101, 102]
    assert all(row["meta"]["fill_level"] is True for row in trade_ledger[:2])
    assert trade_ledger[-1]["position_after"] == Decimal("0E-10")
    assert sum((trade["entry_fee_cost"] for trade in result["trades"]), Decimal("0")) == entry_order["fee_cost"]


class _WorkingPartialProvider:
    def execute_maker_timeline(self, **kwargs):
        fill = ExecutionFill(
            order_id=kwargs["client_order_id"],
            ts=kwargs["signal_ts"].replace(minute=kwargs["signal_ts"].minute + 1),
            asset_id="12345",
            side="BUY",
            liquidity_flag="MAKER",
            price=kwargs["limit_price"],
            size=Decimal("4"),
            fee=Decimal("0"),
            source_event_ids=("partial-fill",),
            reason="maker_queue_consumed_by_orderfilled",
            book_ts=kwargs["signal_ts"],
        )
        return OrderExecutionResult(
            order_id=kwargs["client_order_id"],
            state="PARTIAL",
            fills=(fill,),
            remaining_size=Decimal("6"),
            submitted_at=kwargs["signal_ts"],
            venue_received_at=kwargs["signal_ts"],
            book_snapshot_id="snapshot-partial",
        )


def test_working_partial_maker_order_remains_pending_until_window_end() -> None:
    timestamps = [datetime(2026, 6, 22, 12, minute, tzinfo=UTC) for minute in range(5)]
    points = [
        PricePoint(x_value=100 + idx, price=Decimal("0.60"), volume=Decimal("1"), timestamp=ts)
        for idx, ts in enumerate(timestamps)
    ]
    params = BacktestParameters(position_size=Decimal("4.9"), buy_limit_price=Decimal("0.49"))

    pending = _schedule_lob_maker_order(
        provider=_WorkingPartialProvider(),
        points=points,
        point_index=0,
        order_id="O-PARTIAL",
        side="BUY_YES",
        limit_price=Decimal("0.49"),
        size=Decimal("10"),
        params=params,
        entry=True,
        trade_id="T-PARTIAL",
    )

    assert pending["timeline_fills"][0]["activation_index"] == 1
    assert pending["terminal_index"] == len(points) - 1
