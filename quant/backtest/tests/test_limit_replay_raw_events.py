from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from quant.backtest.backtest_engine import (
    ORDERFILLED_CROSS_MODE,
    BacktestParameters,
    PricePoint,
    _orderfilled_replay_context_with_event_stats,
    build_fill_quality_report,
    fill_quality_metrics,
    is_orderfilled_cross_mode,
    normalize_execution_price_mode,
    parse_parameters,
    simulate_strategy,
)
from quant.backtest.runners.data_sources import ClickHouseOrderFilledStore
from quant.backtest.runners.execution_replay import ReplayTradeEvent


pytestmark = pytest.mark.backtest_validation


def test_orderfilled_cross_is_default_execution_mode_and_old_limit_replay_alias_still_crosses():
    params = parse_parameters({})

    assert params.execution_price_mode == ORDERFILLED_CROSS_MODE
    assert normalize_execution_price_mode("ORDERFILLED_LIMIT_REPLAY") == "ORDERFILLED_CROSS"
    assert is_orderfilled_cross_mode("LIMIT_REPLAY")


def test_raw_replay_context_reports_loaded_deduped_and_trade_tick_coverage():
    events = [
        ReplayTradeEvent(
            market_id=7,
            token_id="token-a",
            block_number=100,
            transaction_index=1,
            log_index=3,
            tx_hash="0xdup",
            trade_price=Decimal("0.49"),
            size=Decimal("2"),
            maker="0xmaker",
            taker="0xtaker",
            side_code="BUY",
        ),
        ReplayTradeEvent(
            market_id=7,
            token_id="token-a",
            block_number=100,
            transaction_index=1,
            log_index=3,
            tx_hash="0xdup",
            trade_price=Decimal("0.49"),
            size=Decimal("2"),
            maker="0xmaker",
            taker="0xtaker",
            side_code="BUY",
        ),
        ReplayTradeEvent(
            market_id=7,
            token_id="token-a",
            block_number=101,
            transaction_index=1,
            log_index=4,
            tx_hash="0xfallback",
            trade_price=Decimal("0.51"),
            size=Decimal("3"),
        ),
    ]

    replay_events, context = _orderfilled_replay_context_with_event_stats(
        events,
        {
            "source": "ClickHouse orderfilled_fact",
            "loaded_block_window": {"from_block": 100, "to_block": 101, "market_id": 7, "token_id_hex": "token-a", "limit": 3},
        },
        limit=3,
    )

    assert [event.tx_hash for event in replay_events] == ["0xdup", "0xfallback"]
    assert context["loaded_event_count"] == 3
    assert context["event_count"] == 2
    assert context["deduped_event_count"] == 2
    assert context["duplicate_event_count"] == 1
    assert context["exact_duplicate_event_count"] == 1
    assert context["conflicting_duplicate_event_count"] == 0
    assert context["duplicate_group_count"] == 1
    assert context["conflicting_duplicate_group_count"] == 0
    assert context["dedupe_applied"] is True
    assert context["hit_limit"] is True
    assert context["canonical_event_count"] == 2
    assert context["fallback_key_event_count"] == 1
    assert context["loaded_block_window"]["loaded_first_block"] == 100
    assert context["loaded_block_window"]["replay_last_block"] == 101
    assert context["loaded_block_window"]["duplicate_group_count"] == 1
    assert context["loaded_block_window"]["conflicting_duplicate_group_count"] == 0
    assert context["raw_trade_tick_count"] == 2
    assert context["raw_block_count"] == 2
    assert context["raw_trade_tick_report"]["blocks"][0]["vwap"] == "0.49"
    assert context["raw_canonical_fill_key_coverage_pct"] == "50"
    assert context["raw_maker_taker_side_coverage_pct"] == "50"

    report = build_fill_quality_report([], replay_context=context)
    assert report["raw_event_count"] == 2
    assert report["loaded_raw_event_count"] == 3
    assert report["deduped_raw_event_count"] == 2
    assert report["raw_duplicate_event_count"] == 1
    assert report["raw_exact_duplicate_event_count"] == 1
    assert report["raw_conflicting_duplicate_event_count"] == 0
    assert report["raw_duplicate_group_count"] == 1
    assert report["raw_conflicting_duplicate_group_count"] == 0
    assert report["raw_canonical_event_count"] == 2
    assert report["raw_fallback_key_event_count"] == 1
    assert report["raw_trade_tick_count"] == 2
    assert report["raw_block_count"] == 2
    assert report["loaded_block_window"]["loaded_event_count"] == 3
    assert report["loaded_block_window"]["duplicate_event_count"] == 1
    assert report["loaded_block_window"]["duplicate_group_count"] == 1
    assert report["loaded_block_window"]["conflicting_duplicate_group_count"] == 0
    assert report["loaded_block_window"]["hit_limit"] is True
    assert report["raw_evidence_summary"]["raw_duplicate_event_count"] == 1
    assert report["raw_evidence_summary"]["raw_duplicate_group_count"] == 1
    assert report["raw_evidence_summary"]["raw_conflicting_duplicate_group_count"] == 0
    assert report["raw_evidence_summary"]["raw_trade_tick_report"]["trade_tick_count"] == 2
    assert "raw_replay_duplicate_events" not in report["environment_flags"]
    assert report["environment_flags"]["raw_replay_event_limit_hit"] == 1


def test_raw_replay_context_flags_conflicting_duplicate_payloads() -> None:
    base = ReplayTradeEvent(
        market_id=7,
        token_id="token-a",
        block_number=100,
        transaction_index=1,
        log_index=3,
        tx_hash="0xdup",
        trade_price=Decimal("0.49"),
        size=Decimal("2"),
        maker="0xmaker",
        taker="0xtaker",
        side_code="BUY",
    )
    conflict = ReplayTradeEvent(
        market_id=7,
        token_id="token-a",
        block_number=100,
        transaction_index=1,
        log_index=3,
        tx_hash="0xdup",
        trade_price=Decimal("0.49"),
        size=Decimal("3"),
        maker="0xmaker",
        taker="0xtaker",
        side_code="BUY",
    )

    replay_events, context = _orderfilled_replay_context_with_event_stats(
        [base, conflict],
        {"enabled": True},
        limit=10,
    )
    report = build_fill_quality_report([], replay_context=context)

    assert len(replay_events) == 1
    assert context["duplicate_event_count"] == 1
    assert context["exact_duplicate_event_count"] == 0
    assert context["conflicting_duplicate_event_count"] == 1
    assert context["conflicting_duplicate_group_count"] == 1
    assert context["loaded_block_window"]["duplicate_group_count"] == 1
    assert context["loaded_block_window"]["conflicting_duplicate_group_count"] == 1
    assert report["raw_conflicting_duplicate_event_count"] == 1
    assert report["raw_duplicate_group_count"] == 1
    assert report["raw_conflicting_duplicate_group_count"] == 1
    assert report["loaded_block_window"]["conflicting_duplicate_group_count"] == 1
    assert report["environment_flags"]["raw_replay_duplicate_events"] == 1
    assert report["environment_flags"]["raw_replay_conflicting_duplicate_events"] == 1


def test_clickhouse_orderfilled_store_uses_prewhere_keyed_range_query():
    class Settings:
        orderfilled_table = "orderfilled_fact"
        database = "default"

    class FakeClient:
        settings = Settings()

        def __init__(self):
            self.query = ""

        def query_scalar(self, query):
            return 1

        def query_json_rows(self, query):
            self.query = query
            return []

    store = ClickHouseOrderFilledStore.__new__(ClickHouseOrderFilledStore)
    fake = FakeClient()
    store.client = fake

    rows = store.load_trade_events(market_id=42, token_id="0xabc", from_block=100, to_block=200, limit=50)

    assert rows == []
    assert "PREWHERE market_id = 42" in fake.query
    assert "AND token_id = '0xabc'" in fake.query
    assert "AND block_number >= 100" in fake.query
    assert "AND block_number <= 200" in fake.query
    assert "ORDER BY block_number ASC, transaction_index ASC, log_index ASC, tx_hash ASC" in fake.query
    assert "LIMIT 50" in fake.query


def test_clickhouse_orderfilled_store_prefers_materialized_trade_replay_cache():
    class Settings:
        orderfilled_table = "orderfilled_fact"
        database = "default"

    class FakeClient:
        settings = Settings()

        def __init__(self):
            self.queries = []

        def query_scalar(self, query, timeout_seconds=None):
            raise AssertionError("materialized replay cache should not inspect raw columns")

        def query_json_rows(self, query, timeout_seconds=None):
            self.queries.append(query)
            if len(self.queries) == 1:
                return [{"market_id": 42, "token_id": "0xabc", "row_count": 1}]
            return [
                {
                    "market_id": 42,
                    "token_id": "0xabc",
                    "block_number": 101,
                    "transaction_index": 2,
                    "log_index": 7,
                    "tx_hash": "0xmat",
                    "trade_price": "0.49",
                    "size": "3",
                    "maker": "0xmaker",
                    "taker": "0xtaker",
                    "side_code": "SELL",
                }
            ]

    store = ClickHouseOrderFilledStore.__new__(ClickHouseOrderFilledStore)
    fake = FakeClient()
    store.client = fake

    rows = store.load_trade_events(market_id=42, token_id="0xabc", from_block=100, to_block=200, limit=50)

    assert len(rows) == 1
    assert rows[0].tx_hash == "0xmat"
    assert rows[0].side_code == "SELL"
    assert store.last_replay_source_table == "orderfilled_trade_replay"
    assert store.last_replay_access_path == "market_id_token_id_block_number_tick_replay_range"
    assert "FROM orderfilled_trade_replay_coverage" in fake.queries[0]
    assert "FROM orderfilled_trade_replay" in fake.queries[1]


def test_clickhouse_orderfilled_store_falls_back_to_raw_when_trade_replay_cache_uncovered():
    class Settings:
        orderfilled_table = "orderfilled_fact"
        database = "default"

    class FakeClient:
        settings = Settings()

        def __init__(self):
            self.queries = []
            self.scalars = []

        def query_scalar(self, query, timeout_seconds=None):
            self.scalars.append(query)
            return 1

        def query_json_rows(self, query, timeout_seconds=None):
            self.queries.append(query)
            if "orderfilled_trade_replay_coverage" in query:
                return []
            return []

    store = ClickHouseOrderFilledStore.__new__(ClickHouseOrderFilledStore)
    fake = FakeClient()
    store.client = fake

    rows = store.load_trade_events(market_id=42, token_id="0xabc", from_block=100, to_block=200, limit=50)

    assert rows == []
    assert store.last_replay_source_table == "orderfilled_fact"
    assert store.last_replay_cache_fallback == "trade_replay_cache_missing_coverage"
    assert any("FROM orderfilled_fact" in query for query in fake.queries)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"market_id": 0, "token_id": "0xabc", "from_block": 100, "to_block": 200, "limit": 50}, "market_id > 0"),
        ({"market_id": 42, "token_id": "", "from_block": 100, "to_block": 200, "limit": 50}, "non-empty token_id"),
        ({"market_id": 42, "token_id": "0xabc", "from_block": 201, "to_block": 200, "limit": 50}, "to_block >= from_block"),
        ({"market_id": 42, "token_id": "0xabc", "from_block": 100, "to_block": 200, "limit": 0}, "limit > 0"),
    ],
)
def test_clickhouse_orderfilled_store_rejects_unbounded_or_invalid_raw_replay_queries(kwargs, message):
    class Settings:
        orderfilled_table = "orderfilled_fact"
        database = "default"

    class FakeClient:
        settings = Settings()

        def query_scalar(self, query):
            return 1

        def query_json_rows(self, query):
            raise AssertionError("invalid raw replay bounds must be rejected before ClickHouse query")

    store = ClickHouseOrderFilledStore.__new__(ClickHouseOrderFilledStore)
    store.client = FakeClient()

    with pytest.raises(ValueError, match=message):
        store.load_trade_events(**kwargs)


def test_limit_replay_uses_raw_orderfilled_events_not_block_close_only():
    start = datetime(2026, 6, 22, 12, 0, 0, tzinfo=timezone.utc)
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1, timestamp=start),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1, timestamp=start + timedelta(seconds=2)),
        PricePoint(x_value=102, price=Decimal("0.62"), volume=Decimal("10"), trade_count=1, timestamp=start + timedelta(seconds=62)),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-buy",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=102,
            transaction_index=0,
            log_index=4,
            tx_hash="0xraw-sell",
            trade_price=Decimal("0.62"),
            size=Decimal("10"),
        ),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.60"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )

    assert result["trades"]
    assert result["orders"][0]["execution_source"] == "orderfilled_limit_replay_raw"
    assert result["orders"][0]["status"] == "FILLED"
    assert result["orders"][0]["submit_x"] == 100
    assert result["orders"][0]["filled_size"] == Decimal("9.8000000000")
    assert result["orders"][0]["avg_fill_price"] == Decimal("0.4900000000")
    assert result["orders"][0]["execution_evidence_type"] == "raw_orderfilled"
    assert result["orders"][0]["raw_candidate_event_count"] == 1
    assert result["orders"][0]["raw_consumed_event_count"] == 1
    assert result["orders"][0]["meta"]["execution_evidence_type"] == "raw_orderfilled"
    assert result["trades"][0]["entry_price"] == Decimal("0.4900000000")
    assert result["trades"][0]["exit_price"] == Decimal("0.6200000000")
    assert result["orders"][0]["meta"]["candidate_events"][0]["tx_hash"] == "0xraw-buy"
    assert result["orders"][0]["meta"]["markout_after_bars"]["1"] == "0.13"
    assert result["orders"][0]["meta"]["markout_after_seconds"]["60"] == "0.13"


def test_limit_replay_exposes_profile_adjusted_fill_probability_schedule():
    start = datetime(2026, 6, 22, 12, 0, 0, tzinfo=timezone.utc)
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1, timestamp=start),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1, timestamp=start + timedelta(seconds=2)),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-buy",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        ),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.80"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="neutral",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )
    order = result["orders"][0]
    report = build_fill_quality_report(result["orders"], replay_context={"enabled": True, "event_count": len(raw_events)})
    metrics = {row["metric_key"]: row for row in fill_quality_metrics(report)}

    assert order["status"] == "PARTIAL_FILLED"
    assert order["fill_probability"] < order["fill_pct"]
    assert order["meta"]["fill_probability_model"] == "raw_tick_sequence_neutral_maker"
    assert order["meta"]["fill_schedule"][0]["raw_size"] == Decimal("10.0000000000")
    assert order["meta"]["fill_schedule"][0]["fillable_size"] == Decimal("6.3000000000")
    assert report["fill_probability_model_counts"] == {"raw_tick_sequence_neutral_maker": 1}
    assert report["fill_schedule_tick_count"] == 1
    assert report["fill_schedule_fillable_size"] == "6.3"
    assert metrics["fill_quality_probability_models"]["formatted_value"] == "1"
    assert metrics["fill_quality_schedule_ticks"]["formatted_value"] == "1"


def test_limit_replay_fallback_uses_block_bar_low_when_close_does_not_cross_buy_limit():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.60"), high_price=Decimal("0.61")),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.49"), high_price=Decimal("0.56")),
        PricePoint(x_value=102, price=Decimal("0.54"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.53"), high_price=Decimal("0.56")),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.80"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"},
        params,
    )

    assert result["orders"][0]["status"] == "FILLED"
    assert result["orders"][0]["execution_source"] == "orderfilled_limit_replay_synthetic"
    assert result["orders"][0]["avg_fill_price"] == Decimal("0.4900000000")
    assert result["orders"][0]["execution_evidence_type"] == "block_bar_ohlcv_fallback"
    assert result["orders"][0]["meta"]["block_bar_used_for_fill"] is True
    assert result["orders"][0]["meta"]["block_bar_cross_field"] == "low_price"
    assert result["orders"][0]["meta"]["block_bar_cross_price"] == Decimal("0.4900000000")


def test_limit_replay_fallback_uses_block_bar_high_when_close_does_not_cross_sell_limit():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.49"), high_price=Decimal("0.61")),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.54"), high_price=Decimal("0.56")),
        PricePoint(x_value=102, price=Decimal("0.58"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.57"), high_price=Decimal("0.62")),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.60"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"},
        params,
    )

    assert result["trades"]
    assert result["orders"][1]["status"] == "FILLED"
    assert result["orders"][1]["execution_source"] == "orderfilled_limit_replay_synthetic"
    assert result["orders"][1]["avg_fill_price"] == Decimal("0.6200000000")
    assert result["orders"][1]["execution_evidence_type"] == "block_bar_ohlcv_fallback"
    assert result["orders"][1]["meta"]["block_bar_used_for_fill"] is True
    assert result["orders"][1]["meta"]["block_bar_cross_field"] == "high_price"
    assert result["trades"][0]["exit_price"] == Decimal("0.6200000000")


def test_limit_replay_rich_block_bar_fallback_uses_tick_sequence_not_full_block_volume():
    points = [
        PricePoint(
            x_value=100,
            price=Decimal("0.55"),
            volume=Decimal("20"),
            trade_count=5,
            open_price=Decimal("0.60"),
            high_price=Decimal("0.62"),
            low_price=Decimal("0.49"),
            close_price=Decimal("0.55"),
            vwap_price=Decimal("0.54"),
            buy_volume=Decimal("12"),
            sell_volume=Decimal("8"),
            first_log_index=2,
            last_log_index=10,
        ),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("0"), trade_count=0),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.80"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"},
        params,
    )
    report = build_fill_quality_report(result["orders"], replay_context={"enabled": False, "event_count": 0})
    metrics = {row["metric_key"]: row for row in fill_quality_metrics(report)}

    order = result["orders"][0]
    assert order["status"] == "PARTIAL_FILLED"
    assert order["execution_source"] == "orderfilled_limit_replay_synthetic"
    assert order["filled_size"] == Decimal("1.6000000000")
    assert order["avg_fill_price"] == Decimal("0.4900000000")
    assert order["block_volume"] == Decimal("1.6000000000")
    assert order["meta"]["block_bar_execution_model"] == "ohlcv_vwap_tick_sequence"
    assert order["meta"]["block_bar_synthetic_tick_count"] == 5
    assert order["meta"]["block_bar_side_volume"] == Decimal("8")
    assert order["meta"]["block_bar_effective_volume"] == Decimal("8")
    assert [row["tx_hash"] for row in order["meta"]["candidate_events"]] == ["synthetic-100-003-low"]
    assert [row["tx_hash"] for row in order["meta"]["block_bar_order_sequence"]] == [
        "synthetic-100-001-open",
        "synthetic-100-002-high",
        "synthetic-100-003-low",
        "synthetic-100-004-vwap",
        "synthetic-100-005-close",
    ]
    assert report["block_bar_execution_models"] == {"ohlcv_vwap_tick_sequence": 1}
    assert metrics["fill_quality_block_bar_tick_sequence"]["formatted_value"] == "1"


def test_limit_replay_maker_gtc_entry_continues_across_blocks_until_filled():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("0"), trade_count=0),
        PricePoint(x_value=101, price=Decimal("0.49"), volume=Decimal("1"), trade_count=1),
        PricePoint(x_value=102, price=Decimal("0.48"), volume=Decimal("3"), trade_count=1),
        PricePoint(x_value=103, price=Decimal("0.70"), volume=Decimal("4"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-entry-1",
            trade_price=Decimal("0.49"),
            size=Decimal("1"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=102,
            transaction_index=0,
            log_index=4,
            tx_hash="0xraw-entry-2",
            trade_price=Decimal("0.48"),
            size=Decimal("3"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=103,
            transaction_index=0,
            log_index=5,
            tx_hash="0xraw-exit",
            trade_price=Decimal("0.70"),
            size=Decimal("4"),
        ),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("2.00"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.70"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )
    report = build_fill_quality_report(result["orders"], replay_context={"enabled": True, "event_count": len(raw_events)})
    metrics = {row["metric_key"]: row for row in fill_quality_metrics(report)}

    entry = result["orders"][0]
    assert entry["status"] == "FILLED"
    assert entry["filled_size"] == Decimal("4.0000000000")
    assert entry["avg_fill_price"] == Decimal("0.4825000000")
    assert entry["meta"]["resting_order_continued"] is True
    assert [row["x_value"] for row in entry["meta"]["resting_order_fill_updates"]] == [101, 102]
    assert [row["tx_hash"] for row in entry["meta"]["consumed_events"]] == ["0xraw-entry-1", "0xraw-entry-2"]
    assert result["orders"][1]["side"] == "SELL_YES"
    assert result["orders"][1]["submit_x"] == 102
    assert result["orders"][1]["status"] == "FILLED"
    assert result["trades"][0]["entry_price"] == Decimal("0.4825000000")
    assert result["trades"][0]["exit_price"] == Decimal("0.7000000000")
    assert report["resting_order_continued_count"] == 1
    assert metrics["fill_quality_resting_order_continued"]["formatted_value"] == "1"


def test_limit_replay_maker_gtc_exit_continues_across_blocks_until_filled():
    points = [
        PricePoint(x_value=100, price=Decimal("0.49"), volume=Decimal("4"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.70"), volume=Decimal("1"), trade_count=1),
        PricePoint(x_value=102, price=Decimal("0.72"), volume=Decimal("3"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=100,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-entry",
            trade_price=Decimal("0.49"),
            size=Decimal("4"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=4,
            tx_hash="0xraw-exit-1",
            trade_price=Decimal("0.70"),
            size=Decimal("1"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=102,
            transaction_index=0,
            log_index=5,
            tx_hash="0xraw-exit-2",
            trade_price=Decimal("0.72"),
            size=Decimal("3"),
        ),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("2.00"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.70"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )
    report = build_fill_quality_report(result["orders"], replay_context={"enabled": True, "event_count": len(raw_events)})

    entry = result["orders"][0]
    exit_order = result["orders"][1]
    assert entry["side"] == "BUY_YES"
    assert entry["status"] == "FILLED"
    assert exit_order["side"] == "SELL_YES"
    assert exit_order["status"] == "FILLED"
    assert exit_order["filled_size"] == Decimal("4.0000000000")
    assert exit_order["avg_fill_price"] == Decimal("0.7150000000")
    assert exit_order["meta"]["resting_order_continued"] is True
    assert [row["x_value"] for row in exit_order["meta"]["resting_order_fill_updates"]] == [101, 102]
    assert [row["tx_hash"] for row in exit_order["meta"]["consumed_events"]] == ["0xraw-exit-1", "0xraw-exit-2"]
    assert result["trades"][0]["exit_price"] == Decimal("0.7150000000")
    assert result["trades"][0]["size"] == Decimal("4.0000000000")
    assert report["resting_order_continued_count"] == 1


def test_limit_replay_partial_resting_exit_at_end_keeps_residual_position_unresolved():
    points = [
        PricePoint(x_value=100, price=Decimal("0.49"), volume=Decimal("4"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.70"), volume=Decimal("1"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=100,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-entry",
            trade_price=Decimal("0.49"),
            size=Decimal("4"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=4,
            tx_hash="0xraw-exit-1",
            trade_price=Decimal("0.70"),
            size=Decimal("1"),
        ),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("2.00"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.70"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )

    exit_order = result["orders"][1]
    assert exit_order["status"] == "PARTIAL_FILLED"
    assert exit_order["filled_size"] == Decimal("1.0000000000")
    assert exit_order["unfilled_size"] == Decimal("3.0000000000")
    assert result["trades"][0]["size"] == Decimal("1.0000000000")
    assert result["trades"][0]["unfilled_size"] == Decimal("0")
    assert any(event["event_type"] == "unresolved_open" and event["meta"]["position_size"] == "3" for event in result["events"])


def test_limit_replay_raw_events_remain_authoritative_over_block_bar_fallback():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.60"), high_price=Decimal("0.61")),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.49"), high_price=Decimal("0.56")),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-above-limit",
            trade_price=Decimal("0.55"),
            size=Decimal("10"),
        )
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )

    assert not result["trades"]
    order = result["orders"][0]
    assert order["status"] == "NO_FILL"
    assert order["execution_source"] == "orderfilled_limit_replay"
    assert order["execution_evidence_type"] == "none"
    assert order["block_bar_crossed"] is True
    assert order["block_bar_cross_field"] == "low_price"
    assert order["block_bar_cross_price"] == Decimal("0.4900000000")
    assert order["no_fill_reason"] == "buy_limit_not_crossed,raw_replay_authoritative_no_fill,block_bar_fallback_suppressed_by_raw_replay"
    assert order["meta"]["raw_replay_available"] is True
    assert order["meta"]["raw_replay_total_event_count"] == 1
    assert order["meta"]["raw_replay_candidate_count"] == 1
    assert order["meta"]["raw_replay_authoritative"] is True
    assert order["meta"]["block_bar_fallback_allowed"] is False
    assert order["meta"]["block_bar_fallback_suppressed_by_raw_replay"] is True
    assert order["meta"]["synthetic_block_bar_crossed_without_raw_fill"] is True
    assert order["meta"]["raw_replay_no_fill_reason"] == "raw_replay_authoritative_no_cross"
    assert order["meta"]["raw_replay_candidate_events"][0]["tx_hash"] == "0xraw-above-limit"
    report = build_fill_quality_report(result["orders"], replay_context={"enabled": True, "event_count": len(raw_events)})
    metrics = {row["metric_key"]: row for row in fill_quality_metrics(report)}
    assert report["raw_replay_fallback_suppressed_count"] == 1
    assert report["raw_replay_synthetic_cross_no_fill_count"] == 1
    assert report["order_anomaly_flags"]["raw_replay_suppressed_block_bar_fallback"] == 1
    assert report["order_anomaly_flags"]["synthetic_block_bar_crossed_without_raw_fill"] == 1
    assert report["environment_flags"]["raw_replay_suppressed_block_bar_fallback"] == 1
    assert metrics["fill_quality_raw_suppressed_fallback"]["formatted_value"] == "1"


def test_limit_replay_latency_blocks_excludes_pre_submit_raw_events():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=102, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-too-early",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        )
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.60"),
        liquidity_cap_pct=Decimal("100"),
        latency_blocks=2,
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )

    assert result["trades"] == []
    assert result["orders"][0]["status"] == "NO_FILL"
    assert result["orders"][0]["no_fill_reason"] == "buy_limit_not_crossed"


def test_limit_replay_latency_seconds_maps_to_block_submit_window():
    start = datetime(2026, 6, 22, 12, 0, 0, tzinfo=timezone.utc)
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1, timestamp=start),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1, timestamp=start + timedelta(seconds=2)),
        PricePoint(x_value=102, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1, timestamp=start + timedelta(seconds=12)),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-too-early-seconds",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=102,
            transaction_index=0,
            log_index=4,
            tx_hash="0xraw-after-seconds",
            trade_price=Decimal("0.48"),
            size=Decimal("10"),
        ),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.80"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.80"),
        liquidity_cap_pct=Decimal("100"),
        latency_seconds=Decimal("5"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )
    report = build_fill_quality_report(result["orders"], replay_context={"enabled": True, "event_count": len(raw_events)})

    assert result["orders"][0]["status"] == "FILLED"
    assert result["orders"][0]["submit_x"] == 102
    assert result["orders"][0]["avg_fill_price"] == Decimal("0.4800000000")
    assert [event["tx_hash"] for event in result["orders"][0]["meta"]["candidate_events"]] == ["0xraw-after-seconds"]
    assert report["max_effective_latency_x_span"] == "2"


def test_limit_replay_maker_fee_rebate_flows_to_trade_ledger_and_report():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=102, price=Decimal("0.62"), volume=Decimal("10"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-buy",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=102,
            transaction_index=0,
            log_index=4,
            tx_hash="0xraw-sell",
            trade_price=Decimal("0.62"),
            size=Decimal("10"),
        ),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.60"),
        liquidity_cap_pct=Decimal("100"),
        maker_fee_bps=Decimal("10"),
        maker_rebate_bps=Decimal("2"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )
    report = build_fill_quality_report(result["orders"])

    assert [row["fee_cost"] for row in result["orders"]] == [Decimal("0.0048020000"), Decimal("0.0060760000")]
    assert [row["rebate"] for row in result["orders"]] == [Decimal("0.0009604000"), Decimal("0.0012152000")]
    assert result["trades"][0]["fee_cost"] == Decimal("0.0108780000")
    assert result["trades"][0]["rebate"] == Decimal("0.0021756000")
    assert result["trades"][0]["pnl"] == Decimal("1.2652976000")
    assert result["ledger"][0]["rebate"] == Decimal("0.0009604000")
    assert result["ledger"][1]["rebate"] == Decimal("0.0012152000")
    assert result["ledger"][-1]["cash_after"] == Decimal("101.2652976000")
    assert Decimal(report["fee_total"]) == Decimal("0.0108780000")
    assert Decimal(report["rebate_total"]) == Decimal("0.0021756000")
    assert Decimal(report["execution_cost_total"]) == Decimal("0.0087024000")


def test_limit_replay_stress_haircut_and_adverse_slippage_affect_fill_price_and_size():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-buy",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        )
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.80"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="realistic",
        order_role="taker",
        latency_blocks=1,
        adverse_slippage_cents=Decimal("0.01"),
        fill_probability_haircut_pct=Decimal("50"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )
    report = build_fill_quality_report(result["orders"])

    assert result["orders"][0]["status"] == "PARTIAL_FILLED"
    assert result["orders"][0]["filled_size"] == Decimal("3.1666666664")
    assert result["orders"][0]["avg_fill_price"] == Decimal("0.5000000000")
    assert result["orders"][0]["filled_notional"] == Decimal("1.5833333332")
    assert result["orders"][0]["slippage_cost"] == Decimal("0.0316666667")
    assert result["orders"][0]["meta"]["raw_avg_fill_price"] == Decimal("0.4900000000")
    assert result["orders"][0]["meta"]["effective_liquidity_cap_pct"] == Decimal("50.0000000000")
    assert result["orders"][0]["meta"]["fill_schedule"][0]["effective_liquidity_cap_pct"] == Decimal("44.4444444400")
    assert result["orders"][0]["meta"]["fill_schedule"][0]["liquidity_curve"] == "nonlinear_cap_sequence_decay_realistic_taker"
    assert result["orders"][0]["role"] == "taker"
    assert Decimal(report["slippage_total"]) == Decimal("0.0316666667")
    assert Decimal(report["avg_effective_liquidity_cap_pct"]) == Decimal("50.0000000000")
    assert Decimal(report["avg_adverse_slippage_cents"]) == Decimal("0.0100000000")
    assert Decimal(report["avg_fill_probability_haircut_pct"]) == Decimal("50.0000000000")
    assert report["role_counts"]["taker"] == 2


def test_limit_replay_taker_marketable_does_not_wait_for_future_fill():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-future-buy",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        )
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="taker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )
    report = build_fill_quality_report(result["orders"])

    assert result["trades"] == []
    assert result["orders"][0]["status"] == "NO_FILL"
    assert result["orders"][0]["order_type"] == "marketable_limit"
    assert result["orders"][0]["meta"]["time_in_force"] == "FAK"
    assert result["orders"][0]["meta"]["candidate_events"] == []
    assert report["order_type_counts"] == {"marketable_limit": 1}
    assert report["time_in_force_counts"] == {"FAK": 1}


def test_limit_replay_taker_marketable_can_fill_submit_block_raw_event():
    points = [
        PricePoint(x_value=100, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.62"), volume=Decimal("10"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=100,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-submit-buy",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        )
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.80"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="taker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )

    assert result["orders"][0]["status"] == "FILLED"
    assert result["orders"][0]["order_type"] == "marketable_limit"
    assert result["orders"][0]["meta"]["time_in_force"] == "FAK"
    assert result["orders"][0]["meta"]["candidate_events"][0]["tx_hash"] == "0xraw-submit-buy"


def test_limit_replay_default_role_is_maker_post_only_waiting_order():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-future-buy",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        )
    ]

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        BacktestParameters(
            initial_capital=Decimal("100"),
            position_size=Decimal("4.90"),
            buy_limit_price=Decimal("0.50"),
            liquidity_cap_pct=Decimal("100"),
            execution_profile="optimistic",
            adverse_slippage_cents=Decimal("0"),
            fill_probability_haircut_pct=Decimal("0"),
        ),
    )

    assert result["orders"][0]["role"] == "maker"
    assert result["orders"][0]["order_type"] == "post_only_limit"
    assert result["orders"][0]["meta"]["time_in_force"] == "GTC"
    assert result["orders"][0]["status"] == "FILLED"


def test_limit_replay_cancelled_maker_order_cuts_off_later_raw_fill():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=102, price=Decimal("0.49"), volume=Decimal("10"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=102,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-after-cancel",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        )
    ]

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        BacktestParameters(
            initial_capital=Decimal("100"),
            position_size=Decimal("4.90"),
            buy_limit_price=Decimal("0.50"),
            liquidity_cap_pct=Decimal("100"),
            execution_profile="optimistic",
            order_role="maker",
            adverse_slippage_cents=Decimal("0"),
            fill_probability_haircut_pct=Decimal("0"),
            cancel_after_blocks=1,
        ),
    )

    order = result["orders"][0]
    assert result["trades"] == []
    assert order["status"] == "CANCELED"
    assert order["no_fill_reason"] == "cancelled_before_fill"
    assert order["meta"]["cancel_x"] == 101
    assert order["meta"]["cancel_ack_x"] == 101
    assert order["meta"]["candidate_events"] == []


def test_limit_replay_cancel_ack_delay_keeps_order_active_until_ack():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.49"), volume=Decimal("10"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-before-cancel-ack",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        )
    ]

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        BacktestParameters(
            initial_capital=Decimal("100"),
            position_size=Decimal("4.90"),
            buy_limit_price=Decimal("0.50"),
            sell_limit_price=Decimal("0.80"),
            liquidity_cap_pct=Decimal("100"),
            execution_profile="optimistic",
            order_role="maker",
            adverse_slippage_cents=Decimal("0"),
            fill_probability_haircut_pct=Decimal("0"),
            cancel_after_blocks=1,
            cancel_ack_delay_blocks=1,
        ),
    )

    order = result["orders"][0]
    assert order["status"] == "FILLED"
    assert order["meta"]["cancel_x"] == 101
    assert order["meta"]["cancel_ack_x"] == 102
    assert order["meta"]["consumed_events"][0]["tx_hash"] == "0xraw-before-cancel-ack"


def test_limit_replay_cancel_failed_keeps_order_active_for_later_raw_fill():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1),
        PricePoint(x_value=102, price=Decimal("0.49"), volume=Decimal("10"), trade_count=1),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=102,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-after-cancel-failed",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        )
    ]

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        BacktestParameters(
            initial_capital=Decimal("100"),
            position_size=Decimal("4.90"),
            buy_limit_price=Decimal("0.50"),
            sell_limit_price=Decimal("0.80"),
            liquidity_cap_pct=Decimal("100"),
            execution_profile="optimistic",
            order_role="maker",
            adverse_slippage_cents=Decimal("0"),
            fill_probability_haircut_pct=Decimal("0"),
            cancel_after_blocks=1,
            cancel_fail=True,
        ),
    )

    order = result["orders"][0]
    assert order["status"] == "FILLED"
    assert order["meta"]["cancel_x"] == 101
    assert order["meta"]["cancel_ack_x"] is None
    assert order["meta"]["cancel_fail_x"] == 101
    assert order["meta"]["cancel_fail"] is True
    assert order["meta"]["consumed_events"][0]["tx_hash"] == "0xraw-after-cancel-failed"


def test_fill_quality_report_counts_raw_candidates_and_no_fill_reasons():
    start = datetime(2026, 6, 22, 12, 0, 0, tzinfo=timezone.utc)
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1, timestamp=start),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1, timestamp=start + timedelta(seconds=2)),
        PricePoint(x_value=102, price=Decimal("0.62"), volume=Decimal("10"), trade_count=1, timestamp=start + timedelta(seconds=62)),
    ]
    raw_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=3,
            tx_hash="0xraw-buy",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        )
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.80"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {
            "market_slug": "demo",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "_orderfilled_replay_events": raw_events,
        },
        params,
    )
    report = build_fill_quality_report(
        result["orders"],
        replay_context={
            "enabled": True,
            "event_count": len(raw_events),
            "fallback": None,
            "from_block": 100,
            "to_block": 102,
            "limit": 250000,
        },
    )

    assert report["submitted_count"] == 2
    assert report["filled_count"] == 1
    assert report["no_fill_count"] == 1
    assert report["candidate_event_count"] == 1
    assert report["raw_candidate_orders"] == 1
    assert report["execution_evidence_counts"] == {"none": 1, "raw_orderfilled": 1}
    assert report["fill_evidence_counts"] == {"raw_orderfilled": 1}
    assert report["raw_orderfilled_fill_count"] == 1
    assert report["block_bar_synthetic_fill_count"] == 0
    assert report["raw_replay_coverage_pct"] == "100"
    assert report["block_bar_fallback_pct"] == "0"
    assert report["raw_enabled"] is True
    assert report["candidate_event_unique_count"] == 1
    assert report["candidate_event_duplicate_count"] == 0
    assert report["consumed_event_unique_count"] == 1
    assert report["consumed_event_duplicate_count"] == 0
    assert report["raw_evidence_summary"]["canonical_key_fields"] == ["tx_hash", "log_index", "market_id", "condition_id", "token_id", "maker", "taker", "side"]
    assert report["raw_evidence_summary"]["candidate_event_keyed_count"] == 1
    assert report["raw_evidence_summary"]["candidate_event_keyless_count"] == 0
    assert report["raw_evidence_summary"]["candidate_notional"] == "4.9"
    assert report["raw_evidence_summary"]["candidate_unique_notional"] == "4.9"
    assert report["raw_evidence_summary"]["consumed_notional"] == "4.802"
    assert report["raw_evidence_summary"]["consumed_unique_notional"] == "4.802"
    assert report["raw_evidence_summary"]["strategy_available_notional"] == "4.9"
    assert report["counterparty_tag_rate"] == "0"
    assert report["no_fill_reasons"] == {"sell_limit_not_crossed": 1}
    assert report["avg_markout_after_1_bars"] == "0.13"
    assert report["avg_markout_after_60_seconds"] == "0.13"
    assert report["markout_seconds_sample_count"] == {"60": 1, "300": 0, "1200": 0}
    assert report["adverse_selection_buckets"]["seconds_60_favorable"] == 1
    assert report["adverse_selection_count"] == 0
    assert report["missed_opportunity_count"] == 1
    assert report["missed_opportunity_notional_total"] == "1.764"
    assert report["avg_missed_opportunity_price_move"] == "0.18"
    assert report["missed_opportunity_by_reason"] == {"sell_limit_not_crossed": "1.764"}
    assert report["missed_opportunity_buckets"]["lifecycle:exit"] == 1
    assert report["missed_opportunity_buckets"]["role:maker"] == 1
    assert report["missed_opportunity_buckets"]["reason:sell_limit_not_crossed"] == 1
    assert report["missed_opportunity_buckets"]["signal_strength:very_high"] == 1
    assert report["missed_opportunity_buckets"]["liquidity:tiny"] == 1
    assert report["missed_opportunity_notional_by_bucket"]["lifecycle:exit"] == "1.764"
    metrics = {row["metric_key"]: row for row in fill_quality_metrics(report)}
    assert metrics["fill_quality_raw_replay_coverage"]["formatted_value"] == "100.0%"
    assert metrics["fill_quality_block_bar_fallback"]["formatted_value"] == "0.0%"


def test_fill_quality_report_surfaces_side_compatibility_discount_counts():
    orders = [
        {
            "order_id": "O-1",
            "status": "PARTIAL_FILLED",
            "side": "BUY_YES",
            "role": "maker",
            "order_type": "post_only_limit",
            "execution_source": "orderfilled_limit_replay_raw",
            "requested_size": Decimal("10"),
            "filled_size": Decimal("2"),
            "unfilled_size": Decimal("8"),
            "requested_notional": Decimal("5"),
            "filled_notional": Decimal("1"),
            "expected_fill_size": Decimal("2"),
            "actual_fill_size": Decimal("2"),
            "expected_fill_notional": Decimal("1"),
            "actual_fill_notional": Decimal("1"),
            "available_notional": Decimal("1"),
            "fee_cost": Decimal("0"),
            "rebate": Decimal("0"),
            "slippage_cost": Decimal("0"),
            "execution_cost": Decimal("0"),
            "latency_blocks": 0,
            "latency_seconds": Decimal("0"),
            "fill_pct": Decimal("20"),
            "meta": {
                "fill_probability_model": "raw_tick_sequence_conservative_maker",
                "fill_schedule": [
                    {
                        "fillable_size": Decimal("1"),
                        "side_compatibility": "incompatible_discounted",
                        "side_compatibility_factor": Decimal("0.4"),
                        "requested_block_participation_pct": Decimal("500"),
                        "block_participation_factor": Decimal("0.5"),
                    },
                    {
                        "fillable_size": Decimal("1"),
                        "side_compatibility": "compatible",
                        "side_compatibility_factor": Decimal("1"),
                        "requested_block_participation_pct": Decimal("100"),
                        "block_participation_factor": Decimal("1"),
                    },
                ],
            },
        }
    ]

    report = build_fill_quality_report(orders, replay_context={"enabled": True, "event_count": 2})
    metrics = {row["metric_key"]: row for row in fill_quality_metrics(report)}

    assert report["side_compatibility_counts"] == {"compatible": 1, "incompatible_discounted": 1}
    assert report["side_discounted_tick_count"] == 1
    assert report["block_participation_discount_tick_count"] == 1
    assert report["max_requested_block_participation_pct"] == "500"
    assert report["min_block_participation_factor"] == "0.5"
    assert report["raw_evidence_summary"]["side_discounted_tick_count"] == 1
    assert report["raw_evidence_summary"]["block_participation_discount_tick_count"] == 1
    assert metrics["fill_quality_side_compatibility"]["formatted_value"] == "1"
    assert metrics["fill_quality_block_participation_pressure"]["formatted_value"] == "1"


def test_fill_quality_report_counts_block_bar_fallback_evidence():
    points = [
        PricePoint(x_value=100, price=Decimal("0.60"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.60"), high_price=Decimal("0.61")),
        PricePoint(x_value=101, price=Decimal("0.55"), volume=Decimal("10"), trade_count=1, low_price=Decimal("0.49"), high_price=Decimal("0.56")),
    ]
    params = BacktestParameters(
        initial_capital=Decimal("100"),
        position_size=Decimal("4.90"),
        buy_limit_price=Decimal("0.50"),
        sell_limit_price=Decimal("0.80"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="maker",
        adverse_slippage_cents=Decimal("0"),
        fill_probability_haircut_pct=Decimal("0"),
    )

    result = simulate_strategy(
        points,
        {"market_slug": "demo", "token_side": "YES", "price_source": "orderfilled_block_close"},
        params,
    )
    report = build_fill_quality_report(result["orders"], replay_context={"enabled": False, "event_count": 0})

    assert report["execution_evidence_counts"] == {"block_bar_ohlcv_fallback": 1, "none": 1}
    assert report["fill_evidence_counts"] == {"block_bar_ohlcv_fallback": 1}
    assert report["raw_orderfilled_fill_count"] == 0
    assert report["block_bar_synthetic_fill_count"] == 1
    assert report["raw_replay_coverage_pct"] == "0"
    assert report["block_bar_fallback_pct"] == "100"


def test_fill_quality_report_flags_order_anomalies_and_environment():
    orders = [
        {
            "order_id": "O-1",
            "status": "FILLED",
            "side": "BUY",
            "role": "maker",
            "order_type": "post_only_limit",
            "execution_source": "orderfilled_limit_replay_synthetic",
            "requested_size": Decimal("10"),
            "filled_size": Decimal("10"),
            "unfilled_size": Decimal("0"),
            "requested_notional": Decimal("5"),
            "filled_notional": Decimal("5"),
            "fee_cost": Decimal("0"),
            "rebate": Decimal("0"),
            "slippage_cost": Decimal("0"),
            "execution_cost": Decimal("0"),
            "latency_blocks": 0,
            "latency_seconds": Decimal("0"),
            "fill_pct": Decimal("100"),
            "meta": {
                "candidate_events": [],
                "consumed_events": [],
                "submit_status": "accepted",
                "accepted_status": "accepted",
                "submit_at": "2026-06-22T12:00:00Z",
                "accepted_at": "2026-06-22T12:00:01.500000Z",
                "api_order_status": "open",
                "chain_order_status": "filled",
            },
        },
        {
            "order_id": "O-2",
            "status": "NO_FILL",
            "side": "SELL",
            "role": "maker",
            "order_type": "post_only_limit",
            "execution_source": "orderfilled_limit_replay_raw",
            "requested_size": Decimal("10"),
            "filled_size": Decimal("0"),
            "unfilled_size": Decimal("10"),
            "requested_notional": Decimal("6"),
            "filled_notional": Decimal("0"),
            "fee_cost": Decimal("0"),
            "rebate": Decimal("0"),
            "slippage_cost": Decimal("0"),
            "execution_cost": Decimal("0"),
            "latency_blocks": 0,
            "latency_seconds": Decimal("0"),
            "fill_pct": Decimal("0"),
            "no_fill_reason": "cancel_race",
            "meta": {
                "candidate_events": [
                    {"tx_hash": "0x1", "log_index": 1, "size": "1", "trade_price": "0.61"}
                ],
                "consumed_events": [],
                "notes": ["cancel_race", "post_only_rejected"],
                "submit_status": "rejected",
                "post_only_rejected": True,
                "cancel_status": "failed",
                "cancel_submitted_at": "2026-06-22T12:00:02Z",
                "cancel_accepted_at": "2026-06-22T12:00:05Z",
            },
        },
    ]

    report = build_fill_quality_report(
        orders,
        replay_context={
            "enabled": True,
            "event_count": 0,
            "fallback": "synthetic_block_close_events",
            "warning": "raw replay event load hit limit; widen env",
        },
        data_quality_report={
            "status": "review",
            "warning_level": "WARN",
            "gap_count": 1,
            "jump_count": 2,
            "span_coverage_pct": "40",
        },
    )

    assert report["order_anomaly_flags"]["synthetic_fill_fallback"] == 1
    assert report["order_anomaly_flags"]["candidate_events_but_no_fill"] == 1
    assert report["order_anomaly_flags"]["cancel_note_observed"] == 1
    assert report["order_anomaly_flags"]["real_order_state_submit_rejected"] == 1
    assert report["order_anomaly_flags"]["real_order_state_post_only_rejected"] == 1
    assert report["order_anomaly_flags"]["real_order_state_cancel_failed"] == 1
    assert report["order_anomaly_flags"]["real_order_state_api_status_lagging_after_chain_fill"] == 1
    assert report["real_order_state_observed_count"] == 2
    assert report["real_order_state_counts"]["submit:accepted"] == 1
    assert report["real_order_state_counts"]["submit:rejected"] == 1
    assert report["real_order_state_counts"]["cancel:failed"] == 1
    assert report["real_order_state_flags"]["post_only_rejected"] == 2
    assert report["real_order_state_flags"]["cancel_failed"] == 1
    assert report["real_order_state_flags"]["api_status_lagging_after_chain_fill"] == 1
    assert report["avg_order_submit_accept_latency_seconds"] == "1.5"
    assert report["avg_cancel_accept_latency_seconds"] == "3"
    assert report["environment_flags"]["raw_replay_fallback_synthetic_block_close_events"] == 1
    assert report["environment_flags"]["raw_replay_empty"] == 1
    assert report["environment_flags"]["raw_replay_event_limit_hit"] == 1
    assert report["environment_flags"]["missing_counterparty_tags"] == 1
    assert report["environment_flags"]["price_gap_detected"] == 1
    assert report["environment_flags"]["price_jump_detected"] == 2
    assert report["environment_flags"]["low_span_coverage"] == 1


def test_fill_quality_report_dedupes_raw_evidence_notional_by_canonical_key():
    event = {
        "tx_hash": "0xdup",
        "log_index": 7,
        "market_id": 1,
        "token_id": "abc",
        "maker": "0xmaker",
        "taker": "0xtaker",
        "side": "BUY",
        "size": "3",
        "trade_price": "0.25",
    }
    orders = [
        {
            "order_id": "O-1",
            "status": "PARTIAL_FILLED",
            "side": "BUY",
            "role": "maker",
            "order_type": "post_only_limit",
            "execution_source": "orderfilled_limit_replay_raw",
            "requested_size": Decimal("10"),
            "filled_size": Decimal("3"),
            "unfilled_size": Decimal("7"),
            "requested_notional": Decimal("2.5"),
            "filled_notional": Decimal("0.75"),
            "available_notional": Decimal("0.75"),
            "fee_cost": Decimal("0"),
            "rebate": Decimal("0"),
            "slippage_cost": Decimal("0"),
            "execution_cost": Decimal("0"),
            "latency_blocks": 0,
            "latency_seconds": Decimal("0"),
            "fill_pct": Decimal("30"),
            "avg_fill_price": Decimal("0.25"),
            "meta": {
                "candidate_events": [event, dict(event)],
                "consumed_events": [event, dict(event)],
            },
        },
        {
            "order_id": "O-2",
            "status": "NO_FILL",
            "side": "BUY",
            "role": "maker",
            "order_type": "post_only_limit",
            "execution_source": "orderfilled_limit_replay_raw",
            "requested_size": Decimal("10"),
            "filled_size": Decimal("0"),
            "unfilled_size": Decimal("10"),
            "requested_notional": Decimal("2.5"),
            "filled_notional": Decimal("0"),
            "available_notional": Decimal("0"),
            "fee_cost": Decimal("0"),
            "rebate": Decimal("0"),
            "slippage_cost": Decimal("0"),
            "execution_cost": Decimal("0"),
            "latency_blocks": 0,
            "latency_seconds": Decimal("0"),
            "fill_pct": Decimal("0"),
            "no_fill_reason": "insufficient_orderfilled_volume",
            "meta": {
                "candidate_events": [{"size": "1", "trade_price": "0.20"}],
                "consumed_events": [event],
            },
        },
    ]

    report = build_fill_quality_report(orders, replay_context={"enabled": True, "event_count": 3})

    summary = report["raw_evidence_summary"]
    assert report["candidate_event_count"] == 3
    assert report["candidate_event_duplicate_count"] == 1
    assert report["consumed_event_duplicate_count"] == 2
    assert report["order_anomaly_flags"]["reused_consumed_fill_event"] == 1
    assert summary["candidate_event_keyed_count"] == 2
    assert summary["candidate_event_keyless_count"] == 1
    assert summary["candidate_notional"] == "1.7"
    assert summary["candidate_unique_notional"] == "0.75"
    assert summary["consumed_notional"] == "2.25"
    assert summary["consumed_unique_notional"] == "0.75"
    assert summary["reused_consumed_event_count"] == 1
    assert summary["candidate_events_missing_counterparty"] == 1
