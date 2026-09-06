from decimal import Decimal

import pytest

from quant.backtest.runners.data_sources import FixtureReplayStore
from quant.backtest.runners.execution_replay import OrderIntent, ReplayTradeEvent, dedupe_replay_events, replay_limit_order, sequence_key


pytestmark = pytest.mark.backtest_validation


def test_buy_sell_crossing_only_creates_candidates():
    events = FixtureReplayStore().load_trade_events("single_fill")

    buy = replay_limit_order(
        OrderIntent("BUY_YES", Decimal("0.50"), Decimal("4"), "GTC", sequence_key(100, 0, 2, "0xsubmit"), liquidity_cap_pct=Decimal("50")),
        events,
    )
    sell = replay_limit_order(
        OrderIntent("SELL_YES", Decimal("0.55"), Decimal("4"), "GTC", sequence_key(100, 0, 2, "0xsubmit"), liquidity_cap_pct=Decimal("100")),
        events,
    )

    assert buy.status == "FILLED"
    assert all(event.trade_price <= Decimal("0.50") for event in buy.candidate_events)
    assert all(event.event_sequence > sequence_key(100, 0, 2, "0xsubmit") for event in buy.candidate_events)
    assert sell.status == "NO_FILL"
    assert sell.candidate_events == []


def test_volume_cap_produces_partial_fill():
    events = FixtureReplayStore().load_trade_events("illiquid")

    result = replay_limit_order(
        OrderIntent("BUY_YES", Decimal("0.50"), Decimal("100"), "GTC", sequence_key(299, 0, 0, "0xsubmit"), liquidity_cap_pct=Decimal("10")),
        events,
    )

    assert result.status == "PARTIAL_FILLED"
    assert result.filled_size == Decimal("0.2000000000")
    assert result.expected_fill_size == Decimal("0.2000000000")
    assert result.to_fill_dict()["actual_fill_size"] == Decimal("0.2000000000")
    assert result.filled_size <= sum((event.size for event in result.candidate_events), Decimal("0")) * Decimal("0.10")
    assert result.unfilled_size == Decimal("99.8000000000")


def test_replay_fill_price_uses_consumed_raw_event_weighted_average():
    events = [
        *FixtureReplayStore().load_trade_events("single_fill"),
    ]

    result = replay_limit_order(
        OrderIntent("BUY_YES", Decimal("0.50"), Decimal("12"), "GTC", sequence_key(100, 0, 2, "0xsubmit"), liquidity_cap_pct=Decimal("100")),
        events,
    )

    assert result.status == "FILLED"
    assert result.filled_size == Decimal("12.0000000000")
    assert result.filled_notional == Decimal("5.7400000000")
    assert result.expected_fill_notional == Decimal("5.7400000000")
    assert result.avg_fill_price == Decimal("0.4783333333")
    assert [event.size for event in result.candidate_events] == [Decimal("10"), Decimal("4")]
    assert [event.size for event in result.consumed_events] == [Decimal("10.0000000000"), Decimal("2.0000000000")]
    fill_dict = result.to_fill_dict()
    assert fill_dict["candidate_events"][0]["market_id"] == 1
    assert fill_dict["candidate_events"][0]["token_id"] == "token-yes"
    assert fill_dict["candidate_events"][0]["canonical_fill_key"]
    assert fill_dict["consumed_events"][0]["market_id"] == 1
    assert fill_dict["consumed_events"][0]["token_id"] == "token-yes"
    assert fill_dict["limit_price"] == Decimal("0.5000000000")
    assert fill_dict["candidate_notional"] == Decimal("6.6800000000")
    assert fill_dict["available_notional"] == Decimal("6.6800000000")
    assert fill_dict["available_notional"] > fill_dict["filled_notional"]
    assert [row["consumed_size"] for row in fill_dict["fill_schedule"]] == [Decimal("10.0000000000"), Decimal("2.0000000000")]
    assert [row["unconsumed_fillable_size"] for row in fill_dict["fill_schedule"]] == [Decimal("0E-10"), Decimal("2.0000000000")]
    assert [row["consumption_status"] for row in fill_dict["fill_schedule"]] == ["fully_consumed", "partially_consumed"]
    evidence = fill_dict["execution_model_evidence"]
    assert evidence["consumed_tick_count"] == 2
    assert evidence["partially_consumed_tick_count"] == 1
    assert evidence["unconsumed_fillable_size"] == Decimal("2.0000000000")
    assert evidence["consumed_notional"] == Decimal("5.7400000000")


def test_replay_fill_dict_keeps_available_liquidity_separate_from_filled_notional():
    events = FixtureReplayStore().load_trade_events("single_fill")

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("1"),
            "GTC",
            sequence_key(100, 0, 2, "0xsubmit"),
            liquidity_cap_pct=Decimal("50"),
            order_role="maker",
            execution_profile="neutral",
        ),
        events,
    )

    fill_dict = result.to_fill_dict()
    assert result.status == "FILLED"
    assert fill_dict["limit_price"] == Decimal("0.5000000000")
    assert fill_dict["candidate_notional"] == Decimal("6.6800000000")
    assert fill_dict["available_notional"] == sum(
        row["fillable_size"] * row["trade_price"]
        for row in fill_dict["fill_schedule"]
    ).quantize(Decimal("0.0000000001"))
    assert fill_dict["filled_notional"] < fill_dict["available_notional"] < fill_dict["candidate_notional"]


def test_replay_fee_and_rebate_affect_cash_delta():
    events = FixtureReplayStore().load_trade_events("single_fill")

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("4"),
            "GTC",
            sequence_key(100, 0, 2, "0xsubmit"),
            fee_bps=Decimal("10"),
            rebate_bps=Decimal("2"),
        ),
        events,
    )

    assert result.status == "FILLED"
    assert result.filled_notional == Decimal("1.9200000000")
    assert result.fee == Decimal("0.0019200000")
    assert result.rebate == Decimal("0.0003840000")
    assert result.cash_delta == Decimal("-1.9215360000")
    assert result.to_fill_dict()["execution_cost"] == Decimal("0.0015360000")


def test_same_block_sequence_prevents_future_function():
    events = FixtureReplayStore().load_trade_events("single_fill")

    after_log_2 = replay_limit_order(
        OrderIntent("BUY_YES", Decimal("0.50"), Decimal("100"), "GTC", sequence_key(100, 0, 2, "0xsubmit")),
        events,
    )
    after_log_4 = replay_limit_order(
        OrderIntent("BUY_YES", Decimal("0.50"), Decimal("100"), "GTC", sequence_key(100, 0, 4, "0xsubmit")),
        events,
    )

    assert [event.log_index for event in after_log_2.candidate_events] == [3, 1]
    assert [event.log_index for event in after_log_4.candidate_events] == [1]


def test_replay_dedupes_strong_canonical_orderfilled_identity_before_fill() -> None:
    duplicated = ReplayTradeEvent(
        market_id=42,
        token_id="token-yes",
        block_number=100,
        transaction_index=2,
        log_index=7,
        tx_hash="0xABC",
        trade_price=Decimal("0.49"),
        size=Decimal("5"),
        maker="0xMaker",
        taker="0xTaker",
        side_code="BUY",
    )
    events = [
        duplicated,
        ReplayTradeEvent(
            market_id=42,
            token_id="token-yes",
            block_number=100,
            transaction_index=2,
            log_index=7,
            tx_hash="0xabc",
            trade_price=Decimal("0.49"),
            size=Decimal("5"),
            maker="0xmaker",
            taker="0xtaker",
            side_code="BUY",
        ),
    ]

    result = replay_limit_order(
        OrderIntent("BUY_YES", Decimal("0.50"), Decimal("10"), "GTC", sequence_key(100, 2, 6, "0xsubmit")),
        events,
    )

    assert duplicated.canonical_fill_key == "0xabc|7|42|token-yes|0xmaker|0xtaker|BUY"
    assert len(dedupe_replay_events(events)) == 1
    assert result.status == "PARTIAL_FILLED"
    assert result.filled_size == Decimal("5.0000000000")
    assert len(result.candidate_events) == 1
    assert result.to_fill_dict()["candidate_events"][0]["canonical_fill_key_kind"] == "canonical"


def test_replay_keeps_same_tx_different_log_index_as_distinct_fills() -> None:
    events = [
        ReplayTradeEvent(
            market_id=42,
            token_id="token-yes",
            block_number=100,
            transaction_index=2,
            log_index=7,
            tx_hash="0xabc",
            trade_price=Decimal("0.49"),
            size=Decimal("5"),
            maker="0xmaker",
            taker="0xtaker",
            side_code="BUY",
        ),
        ReplayTradeEvent(
            market_id=42,
            token_id="token-yes",
            block_number=100,
            transaction_index=2,
            log_index=8,
            tx_hash="0xabc",
            trade_price=Decimal("0.48"),
            size=Decimal("4"),
            maker="0xmaker",
            taker="0xtaker",
            side_code="BUY",
        ),
    ]

    result = replay_limit_order(
        OrderIntent("BUY_YES", Decimal("0.50"), Decimal("9"), "GTC", sequence_key(100, 2, 6, "0xsubmit")),
        events,
    )

    assert len(dedupe_replay_events(events)) == 2
    assert result.status == "FILLED"
    assert [event.log_index for event in result.consumed_events] == [7, 8]
    assert result.avg_fill_price == Decimal("0.4855555556")


def test_block_context_is_partitioned_by_condition_id() -> None:
    events = [
        ReplayTradeEvent(
            market_id=42,
            condition_id="condition-a",
            token_id="token-yes",
            block_number=100,
            transaction_index=1,
            log_index=1,
            tx_hash="0xa",
            trade_price=Decimal("0.49"),
            size=Decimal("5"),
            maker="0xmaker-a",
            taker="0xtaker-a",
            side_code="SELL",
        ),
        ReplayTradeEvent(
            market_id=42,
            condition_id="condition-b",
            token_id="token-yes",
            block_number=100,
            transaction_index=1,
            log_index=2,
            tx_hash="0xb",
            trade_price=Decimal("0.90"),
            size=Decimal("100"),
            maker="0xmaker-b",
            taker="0xtaker-b",
            side_code="SELL",
        ),
    ]

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("5"),
            "GTC",
            sequence_key(99, 0, 0, "0xsubmit"),
            execution_profile="neutral",
            order_role="maker",
            liquidity_cap_pct=Decimal("100"),
        ),
        events[:1],
    )
    mixed_context_result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("5"),
            "GTC",
            sequence_key(99, 0, 0, "0xsubmit"),
            execution_profile="neutral",
            order_role="maker",
            liquidity_cap_pct=Decimal("100"),
        ),
        events,
    )

    schedule = mixed_context_result.to_fill_dict()["fill_schedule"]
    assert result.avg_fill_price == Decimal("0.4900000000")
    assert mixed_context_result.avg_fill_price == Decimal("0.4900000000")
    assert schedule[0]["block_trade_count"] == 1
    assert schedule[0]["block_high_price"] == Decimal("0.4900000000")
    assert schedule[0]["block_low_price"] == Decimal("0.4900000000")
    assert schedule[0]["block_vwap_price"] == Decimal("0.4900000000")
    assert schedule[0]["block_volume"] == Decimal("5.0000000000")


def test_time_in_force_gtc_gtd_fok_fak():
    events = FixtureReplayStore().load_trade_events("lifecycle")

    gtc = replay_limit_order(OrderIntent("BUY_YES", Decimal("0.50"), Decimal("5"), "GTC", sequence_key(99, 0, 9, "0xsubmit")), events)
    gtd = replay_limit_order(OrderIntent("BUY_YES", Decimal("0.46"), Decimal("2"), "GTD", sequence_key(100, 0, 9, "0xsubmit"), expire_sequence=sequence_key(101, 0, 9, "0xexpire")), events)
    fok = replay_limit_order(OrderIntent("BUY_YES", Decimal("0.50"), Decimal("20"), "FOK", sequence_key(100, 0, 0, "0xsubmit"), liquidity_cap_pct=Decimal("10")), events)
    fak = replay_limit_order(OrderIntent("BUY_YES", Decimal("0.50"), Decimal("20"), "FAK", sequence_key(100, 0, 0, "0xsubmit"), liquidity_cap_pct=Decimal("10")), events)

    assert gtc.status == "FILLED"
    assert gtd.status == "EXPIRED"
    assert fok.status == "REJECTED"
    assert fok.filled_size == Decimal("0")
    assert fak.status == "PARTIAL_FILLED"
    assert fak.filled_size == Decimal("1.0000000000")


def test_execution_profile_and_role_change_raw_replay_fill_size():
    events = FixtureReplayStore().load_trade_events("single_fill")

    optimistic_taker = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("20"),
            "GTC",
            sequence_key(100, 0, 2, "0xsubmit"),
            liquidity_cap_pct=Decimal("100"),
            order_role="taker",
            execution_profile="optimistic",
        ),
        events,
    )
    conservative_maker = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("20"),
            "GTC",
            sequence_key(100, 0, 2, "0xsubmit"),
            liquidity_cap_pct=Decimal("100"),
            order_role="maker",
            execution_profile="conservative",
        ),
        events,
    )

    assert optimistic_taker.filled_size == Decimal("14.0000000000")
    assert conservative_maker.status == "PARTIAL_FILLED"
    assert conservative_maker.filled_size < optimistic_taker.filled_size
    assert conservative_maker.available_size == conservative_maker.expected_fill_size
    assert conservative_maker.to_fill_dict()["order_role"] == "maker"
    assert conservative_maker.to_fill_dict()["execution_profile"] == "conservative"


def test_maker_receives_less_fill_than_taker_under_neutral_profile():
    events = FixtureReplayStore().load_trade_events("single_fill")
    common = {
        "side": "BUY_YES",
        "limit_price": Decimal("0.50"),
        "size": Decimal("20"),
        "time_in_force": "GTC",
        "submit_sequence": sequence_key(100, 0, 2, "0xsubmit"),
        "liquidity_cap_pct": Decimal("100"),
        "execution_profile": "neutral",
    }

    maker = replay_limit_order(OrderIntent(**common, order_role="maker"), events)
    taker = replay_limit_order(OrderIntent(**common, order_role="taker"), events)

    assert maker.status == "PARTIAL_FILLED"
    assert taker.status == "PARTIAL_FILLED"
    assert maker.filled_size < taker.filled_size
    assert maker.available_size < taker.available_size


def test_fill_probability_profiles_are_switchable_and_report_tick_schedule():
    events = FixtureReplayStore().load_trade_events("single_fill")
    common = {
        "side": "BUY_YES",
        "limit_price": Decimal("0.50"),
        "size": Decimal("20"),
        "time_in_force": "GTC",
        "submit_sequence": sequence_key(100, 0, 2, "0xsubmit"),
        "liquidity_cap_pct": Decimal("100"),
        "order_role": "maker",
    }

    neutral = replay_limit_order(OrderIntent(**common, execution_profile="neutral"), events)
    conservative = replay_limit_order(OrderIntent(**common, execution_profile="conservative"), events)
    stress = replay_limit_order(OrderIntent(**common, execution_profile="stress"), events)

    assert neutral.fill_probability > conservative.fill_probability > stress.fill_probability
    assert neutral.filled_size > conservative.filled_size > stress.filled_size
    schedule = conservative.to_fill_dict()["fill_schedule"]
    assert conservative.to_fill_dict()["fill_probability_model"] == "raw_tick_sequence_conservative_maker"
    assert schedule[0]["raw_size"] == Decimal("10.0000000000")
    assert schedule[0]["block_trade_index"] == 2
    assert schedule[0]["candidate_block_trade_index"] == 1
    assert schedule[0]["sequence_decay_factor"] == Decimal("0.8196721311")
    assert schedule[0]["fillable_size"] == Decimal("1.8032786884")
    assert schedule[0]["cumulative_fillable_size"] == Decimal("1.8032786884")
    assert schedule[0]["liquidity_curve"] == "nonlinear_cap_sequence_decay_conservative_maker"
    assert schedule[0]["effective_liquidity_cap_pct"] == Decimal("100.0000000000")
    assert conservative.consumed_events[0].size == Decimal("1.8032786884")


def test_replay_result_exposes_order_level_execution_model_evidence():
    events = FixtureReplayStore().load_trade_events("single_fill")

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("20"),
            "GTC",
            sequence_key(100, 0, 2, "0xsubmit"),
            liquidity_cap_pct=Decimal("50"),
            order_role="maker",
            execution_profile="stress",
        ),
        events,
    )

    evidence = result.to_fill_dict()["execution_model_evidence"]
    assert evidence["model_version"] == "orderfilled_tick_execution_v4"
    assert evidence["execution_source"] == "orderfilled_limit_replay"
    assert evidence["execution_profile"] == "stress"
    assert evidence["order_role"] == "maker"
    assert evidence["fill_probability_model"] == "raw_tick_sequence_stress_maker"
    assert evidence["fill_probability_algorithm"] == "stress_tail_orderfilled_tick_haircut_maker"
    assert evidence["requested_size"] == Decimal("20.0000000000")
    assert evidence["raw_candidate_size"] == Decimal("14.0000000000")
    assert evidence["raw_candidate_notional"] == Decimal("6.6800000000")
    assert evidence["requested_liquidity_cap_pct"] == Decimal("50.0000000000")
    assert evidence["effective_liquidity_cap_pct"] == Decimal("25.0000000000")
    assert evidence["liquidity_curve"] == "nonlinear_cap_sequence_decay_stress_maker"
    assert evidence["available_size_after_model"] == result.available_size
    assert evidence["coverage_ratio"] == (result.available_size / result.requested_size).quantize(Decimal("0.0000000001"))
    assert evidence["raw_coverage_ratio"] == Decimal("0.7000000000")
    assert evidence["candidate_tick_count"] == 2
    assert evidence["fillable_tick_count"] == 2
    assert evidence["sequence_adjusted_tick_count"] == 2
    assert evidence["uses_block_ohlcv_vwap"] is True
    assert evidence["uses_same_block_order_sequence"] is True
    assert evidence["uses_block_participation_pressure"] is True
    assert evidence["block_participation_discount_tick_count"] >= 1
    assert evidence["min_block_participation_factor"] < Decimal("1")
    assert evidence["uses_canonical_trade_ticks"] is False
    assert evidence["min_sequence_decay_factor"] < Decimal("1")


def test_liquidity_cap_is_non_linear_by_profile_and_role():
    events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=3,
            tx_hash="0xraw-buy",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        )
    ]
    common = {
        "side": "BUY_YES",
        "limit_price": Decimal("0.50"),
        "size": Decimal("10"),
        "time_in_force": "GTC",
        "submit_sequence": sequence_key(100, 0, 1, "0xsubmit"),
        "liquidity_cap_pct": Decimal("50"),
        "order_role": "maker",
    }

    neutral = replay_limit_order(OrderIntent(**common, execution_profile="neutral"), events)
    conservative = replay_limit_order(OrderIntent(**common, execution_profile="conservative"), events)
    stress = replay_limit_order(OrderIntent(**common, execution_profile="stress"), events)

    neutral_schedule = neutral.to_fill_dict()["fill_schedule"][0]
    conservative_schedule = conservative.to_fill_dict()["fill_schedule"][0]
    stress_schedule = stress.to_fill_dict()["fill_schedule"][0]
    assert neutral_schedule["liquidity_cap_pct"] == Decimal("50.0000000000")
    assert neutral_schedule["effective_liquidity_cap_pct"] == Decimal("42.5531914900")
    assert conservative_schedule["effective_liquidity_cap_pct"] == Decimal("33.3333333300")
    assert stress_schedule["effective_liquidity_cap_pct"] == Decimal("25.0000000000")
    assert neutral.filled_size > conservative.filled_size > stress.filled_size
    assert neutral.fill_probability > conservative.fill_probability > stress.fill_probability


def test_large_order_participation_pressure_discounts_block_liquidity():
    events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=1,
            tx_hash="0xlarge1",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=2,
            tx_hash="0xlarge2",
            trade_price=Decimal("0.48"),
            size=Decimal("10"),
            side_code="SELL",
        ),
    ]
    common = {
        "side": "BUY_YES",
        "limit_price": Decimal("0.50"),
        "time_in_force": "GTC",
        "submit_sequence": sequence_key(100, 0, 1, "0xsubmit"),
        "liquidity_cap_pct": Decimal("100"),
        "order_role": "maker",
        "execution_profile": "conservative",
    }

    small = replay_limit_order(OrderIntent(**common, size=Decimal("20")), events)
    large = replay_limit_order(OrderIntent(**common, size=Decimal("1000")), events)
    schedule = large.to_fill_dict()["fill_schedule"]
    evidence = large.to_fill_dict()["execution_model_evidence"]

    assert small.to_fill_dict()["execution_model_evidence"]["uses_block_participation_pressure"] is False
    assert large.status == "PARTIAL_FILLED"
    assert large.filled_size < small.filled_size
    assert large.fill_probability < small.fill_probability
    assert schedule[0]["requested_block_participation_pct"] == Decimal("5000.0000000000")
    assert schedule[0]["block_participation_reason"] == "large_order_block_participation_discount"
    assert schedule[0]["block_participation_factor"] < Decimal("0.10")
    assert evidence["uses_block_participation_pressure"] is True
    assert evidence["block_participation_discount_tick_count"] == 2
    assert evidence["max_requested_block_participation_pct"] == Decimal("5000.0000000000")
    assert evidence["min_block_participation_factor"] == schedule[0]["block_participation_factor"]


def test_same_block_later_ticks_get_sequence_decay_for_maker_orders():
    events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=1,
            tx_hash="0x1",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=2,
            tx_hash="0x2",
            trade_price=Decimal("0.48"),
            size=Decimal("10"),
        ),
    ]

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("20"),
            "GTC",
            sequence_key(100, 0, 1, "0xsubmit"),
            liquidity_cap_pct=Decimal("100"),
            order_role="maker",
            execution_profile="conservative",
        ),
        events,
    )
    schedule = result.to_fill_dict()["fill_schedule"]

    assert [row["block_trade_index"] for row in schedule] == [1, 2]
    assert [row["block_trade_count"] for row in schedule] == [2, 2]
    assert schedule[0]["block_open_price"] == Decimal("0.4900000000")
    assert schedule[0]["block_high_price"] == Decimal("0.4900000000")
    assert schedule[0]["block_low_price"] == Decimal("0.4800000000")
    assert schedule[0]["block_close_price"] == Decimal("0.4800000000")
    assert schedule[0]["block_vwap_price"] == Decimal("0.4850000000")
    assert schedule[0]["block_volume"] == Decimal("20.0000000000")
    assert schedule[0]["block_notional"] == Decimal("9.7000000000")
    assert schedule[0]["block_first_sequence"]["log_index"] == 1
    assert schedule[0]["block_last_sequence"]["log_index"] == 2
    assert schedule[0]["sequence_decay_factor"] == Decimal("1.0000000000")
    assert schedule[1]["sequence_decay_factor"] == Decimal("0.7575757576")
    assert schedule[0]["block_context_factor"] == Decimal("1.0000000000")
    assert schedule[0]["block_context_reason"] == "stable_block_context"
    assert schedule[0]["tick_quality_factor"] == Decimal("1.0000000000")
    assert schedule[0]["tick_quality_reason"] == "normal_tick_quality"
    assert schedule[0]["fillable_size"] == Decimal("2.2000000000")
    assert schedule[1]["fillable_size"] == Decimal("1.6666666667")
    assert result.filled_size == Decimal("3.8666666667")


def test_same_price_orderfilled_queue_ahead_discounts_maker_not_taker():
    events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=1,
            tx_hash="0xqueue-ahead",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=2,
            tx_hash="0xqueue-later",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        ),
    ]
    common = {
        "side": "BUY_YES",
        "limit_price": Decimal("0.50"),
        "size": Decimal("20"),
        "time_in_force": "GTC",
        "submit_sequence": sequence_key(100, 0, 1, "0xsubmit"),
        "liquidity_cap_pct": Decimal("100"),
        "execution_profile": "conservative",
    }

    maker = replay_limit_order(OrderIntent(**common, order_role="maker"), events)
    taker = replay_limit_order(OrderIntent(**common, order_role="taker"), events)
    maker_schedule = maker.to_fill_dict()["fill_schedule"]
    taker_schedule = taker.to_fill_dict()["fill_schedule"]
    evidence = maker.to_fill_dict()["execution_model_evidence"]

    assert maker_schedule[0]["maker_queue_factor"] == Decimal("1.0000000000")
    assert maker_schedule[0]["remaining_order_size_before_tick"] == Decimal("20.0000000000")
    assert maker_schedule[0]["remaining_order_size_after_tick"] == Decimal("17.8000000000")
    assert maker_schedule[0]["maker_queue_reason"] == "no_same_price_orderfilled_queue_ahead"
    assert maker_schedule[1]["maker_queue_ahead_size"] == Decimal("10.0000000000")
    assert maker_schedule[1]["remaining_order_size_before_tick"] == Decimal("17.8000000000")
    assert maker_schedule[1]["remaining_order_size_after_tick"] == Decimal("16.6274044795")
    assert maker_schedule[1]["maker_queue_factor"] == Decimal("0.7035573123")
    assert maker_schedule[1]["maker_queue_reason"] == "same_price_orderfilled_queue_ahead_discount"
    assert maker_schedule[1]["fillable_size"] < Decimal("1.6666666667")
    assert evidence["uses_orderfilled_maker_queue_proxy"] is True
    assert evidence["maker_queue_adjusted_tick_count"] == 1
    assert evidence["max_maker_queue_ahead_size"] == Decimal("10.0000000000")
    assert evidence["min_maker_queue_factor"] == Decimal("0.7035573123")
    assert evidence["dynamic_remaining_pressure_tick_count"] == 1
    assert evidence["min_remaining_order_size_before_tick"] == Decimal("17.8000000000")
    assert evidence["uses_dynamic_remaining_order_pressure"] is True
    assert [row["maker_queue_factor"] for row in taker_schedule] == [Decimal("1.0000000000"), Decimal("1.0000000000")]
    assert taker.to_fill_dict()["execution_model_evidence"]["uses_orderfilled_maker_queue_proxy"] is False
    assert taker.filled_size > maker.filled_size


def test_remaining_order_size_reduces_later_block_participation_pressure() -> None:
    events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=1,
            tx_hash="0xpart-a",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=2,
            tx_hash="0xpart-b",
            trade_price=Decimal("0.48"),
            size=Decimal("10"),
            side_code="SELL",
        ),
    ]

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("80"),
            "GTC",
            sequence_key(100, 0, 1, "0xsubmit"),
            liquidity_cap_pct=Decimal("100"),
            order_role="maker",
            execution_profile="conservative",
        ),
        events,
    )

    schedule = result.to_fill_dict()["fill_schedule"]
    evidence = result.to_fill_dict()["execution_model_evidence"]

    assert schedule[0]["requested_block_participation_pct"] == Decimal("400.0000000000")
    assert schedule[0]["block_participation_reason"] == "large_order_block_participation_discount"
    assert schedule[1]["remaining_order_size_before_tick"] < schedule[0]["remaining_order_size_before_tick"]
    assert schedule[1]["requested_block_participation_pct"] < schedule[0]["requested_block_participation_pct"]
    assert schedule[1]["block_participation_factor"] > schedule[0]["block_participation_factor"]
    assert evidence["uses_dynamic_remaining_order_pressure"] is True
    assert evidence["dynamic_remaining_pressure_tick_count"] == 1


def test_sequence_decay_uses_raw_block_order_not_only_candidate_order():
    first_cross_only = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=2,
            tx_hash="0xcross",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        )
    ]
    late_cross_after_non_cross = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=1,
            tx_hash="0xnoncross",
            trade_price=Decimal("0.51"),
            size=Decimal("10"),
        ),
        *first_cross_only,
    ]
    intent = {
        "side": "BUY_YES",
        "limit_price": Decimal("0.50"),
        "size": Decimal("10"),
        "time_in_force": "GTC",
        "submit_sequence": sequence_key(100, 0, 1, "0xsubmit"),
        "liquidity_cap_pct": Decimal("100"),
        "order_role": "maker",
        "execution_profile": "conservative",
    }

    first = replay_limit_order(OrderIntent(**intent), first_cross_only)
    late = replay_limit_order(OrderIntent(**intent), late_cross_after_non_cross)
    first_schedule = first.to_fill_dict()["fill_schedule"][0]
    late_schedule = late.to_fill_dict()["fill_schedule"][0]

    assert first_schedule["candidate_block_trade_index"] == 1
    assert first_schedule["block_trade_index"] == 1
    assert first_schedule["sequence_decay_factor"] == Decimal("1.0000000000")
    assert late_schedule["candidate_block_trade_index"] == 1
    assert late_schedule["block_trade_index"] == 2
    assert late_schedule["sequence_decay_factor"] == Decimal("0.8196721311")
    assert late.filled_size < first.filled_size
    assert late.to_fill_dict()["execution_model_evidence"]["sequence_adjusted_tick_count"] == 1


def test_volatile_block_context_discounts_conservative_maker_fill():
    stable_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=1,
            tx_hash="0xstable1",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=2,
            tx_hash="0xstable2",
            trade_price=Decimal("0.48"),
            size=Decimal("10"),
        ),
    ]
    volatile_events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=1,
            tx_hash="0xvolatile1",
            trade_price=Decimal("0.50"),
            size=Decimal("10"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=2,
            tx_hash="0xvolatile2",
            trade_price=Decimal("0.30"),
            size=Decimal("10"),
        ),
    ]
    intent = {
        "side": "BUY_YES",
        "limit_price": Decimal("0.50"),
        "size": Decimal("20"),
        "time_in_force": "GTC",
        "submit_sequence": sequence_key(100, 0, 1, "0xsubmit"),
        "liquidity_cap_pct": Decimal("100"),
        "order_role": "maker",
        "execution_profile": "conservative",
    }

    stable = replay_limit_order(OrderIntent(**intent), stable_events)
    volatile = replay_limit_order(OrderIntent(**intent), volatile_events)
    schedule = volatile.to_fill_dict()["fill_schedule"]

    assert volatile.filled_size < stable.filled_size
    assert volatile.fill_probability < stable.fill_probability
    assert schedule[0]["block_context_reason"] == "block_volatility_vwap_discount"
    assert schedule[0]["block_range_pct"] == Decimal("0.5000000000")
    assert schedule[0]["vwap_dislocation_pct"] == Decimal("0.2500000000")
    assert schedule[0]["block_context_factor"] < Decimal("1")
    assert schedule[0]["fillable_size"] < Decimal("2.2000000000")


def test_tiny_dislocated_tick_is_evidence_but_not_full_fair_liquidity():
    events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=1,
            tx_hash="0xnormal",
            trade_price=Decimal("0.50"),
            size=Decimal("100"),
        ),
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            log_index=2,
            tx_hash="0xspike",
            trade_price=Decimal("0.99"),
            size=Decimal("0.01"),
        ),
    ]

    result = replay_limit_order(
        OrderIntent(
            "SELL_YES",
            Decimal("0.95"),
            Decimal("0.005"),
            "GTC",
            sequence_key(100, 0, 1, "0xsubmit"),
            liquidity_cap_pct=Decimal("100"),
            order_role="maker",
            execution_profile="conservative",
        ),
        events,
    )
    schedule = result.to_fill_dict()["fill_schedule"]

    assert len(result.candidate_events) == 1
    assert result.candidate_events[0].tx_hash == "0xspike"
    assert schedule[0]["block_trade_index"] == 2
    assert schedule[0]["block_trade_count"] == 2
    assert schedule[0]["candidate_block_trade_index"] == 1
    assert schedule[0]["block_volume"] == Decimal("100.0100000000")
    assert schedule[0]["block_vwap_price"] == Decimal("0.5000489951")
    assert schedule[0]["tick_volume_share_pct"] == Decimal("0.0099990000")
    assert schedule[0]["tick_quality_reason"] == "small_dislocated_tick_discount"
    assert schedule[0]["tick_quality_factor"] < Decimal("0.10")
    assert schedule[0]["fillable_size"] < Decimal("0.0001000000")
    assert result.status == "PARTIAL_FILLED"


def test_side_attribution_changes_maker_fill_schedule_when_trade_direction_is_known():
    same_side = [
        ReplayTradeEvent(
            market_id=42,
            token_id="token-yes",
            block_number=101,
            log_index=1,
            tx_hash="0xsame",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="BUY",
        )
    ]
    contra_side = [
        ReplayTradeEvent(
            market_id=42,
            token_id="token-yes",
            block_number=101,
            log_index=1,
            tx_hash="0xcontra",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        )
    ]
    mixed_context = same_side + contra_side
    intent = {
        "side": "BUY_YES",
        "limit_price": Decimal("0.50"),
        "size": Decimal("10"),
        "time_in_force": "GTC",
        "submit_sequence": sequence_key(100, 0, 1, "0xsubmit"),
        "liquidity_cap_pct": Decimal("100"),
        "order_role": "maker",
        "execution_profile": "conservative",
    }

    discounted = replay_limit_order(OrderIntent(**intent), same_side)
    compatible = replay_limit_order(OrderIntent(**intent), contra_side)
    mixed = replay_limit_order(OrderIntent(**intent), mixed_context)

    discounted_schedule = discounted.to_fill_dict()["fill_schedule"][0]
    compatible_schedule = compatible.to_fill_dict()["fill_schedule"][0]
    mixed_schedule = mixed.to_fill_dict()["fill_schedule"][0]
    assert discounted.filled_size < compatible.filled_size
    assert discounted_schedule["expected_event_side"] == "SELL"
    assert discounted_schedule["observed_event_side"] == "BUY"
    assert discounted_schedule["side_compatibility"] == "incompatible_discounted"
    assert discounted_schedule["side_compatibility_factor"] == Decimal("0.4000000000")
    assert compatible_schedule["side_compatibility"] == "compatible"
    assert compatible_schedule["side_compatibility_factor"] == Decimal("1.0000000000")
    assert mixed_schedule["block_buy_volume"] == Decimal("10.0000000000")
    assert mixed_schedule["block_sell_volume"] == Decimal("10.0000000000")
    assert mixed_schedule["block_unknown_side_volume"] == Decimal("0E-10")
    assert mixed_schedule["block_buy_notional"] == Decimal("4.9000000000")
    assert mixed_schedule["block_sell_notional"] == Decimal("4.9000000000")
    assert mixed_schedule["block_unknown_side_notional"] == Decimal("0E-10")


def test_cancel_sequence_cuts_off_later_crossing_events():
    events = FixtureReplayStore().load_trade_events("single_fill")

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("4"),
            "GTC",
            sequence_key(100, 0, 2, "0xsubmit"),
            cancel_sequence=sequence_key(100, 0, 2, "0xcancel"),
        ),
        events,
    )

    assert result.status == "CANCELED"
    assert result.filled_size == Decimal("0")
    assert result.no_fill_reason == "cancelled_before_fill"


def test_cancel_ack_sequence_keeps_order_active_until_ack():
    events = FixtureReplayStore().load_trade_events("single_fill")

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("4"),
            "GTC",
            sequence_key(100, 0, 2, "0xsubmit"),
            cancel_sequence=sequence_key(100, 0, 2, "0xcancel-request"),
            cancel_ack_sequence=sequence_key(100, 0, 3, "0xcancel-ack"),
        ),
        events,
    )

    assert result.status == "FILLED"
    assert result.filled_size == Decimal("4.0000000000")
    assert [event.log_index for event in result.consumed_events] == [3]


def test_cancel_pending_window_discounts_fill_before_ack() -> None:
    events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=2,
            tx_hash="0xafter-cancel",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        )
    ]
    common = {
        "side": "BUY_YES",
        "limit_price": Decimal("0.50"),
        "size": Decimal("10"),
        "time_in_force": "GTC",
        "submit_sequence": sequence_key(100, 0, 1, "0xsubmit"),
        "liquidity_cap_pct": Decimal("100"),
        "order_role": "maker",
        "execution_profile": "conservative",
    }

    baseline = replay_limit_order(OrderIntent(**common), events)
    pending_cancel = replay_limit_order(
        OrderIntent(
            **common,
            cancel_sequence=sequence_key(101, 0, 1, "0xcancel-request"),
            cancel_ack_sequence=sequence_key(101, 0, 3, "0xcancel-ack"),
        ),
        events,
    )
    schedule = pending_cancel.to_fill_dict()["fill_schedule"][0]
    evidence = pending_cancel.to_fill_dict()["execution_model_evidence"]

    assert baseline.filled_size > pending_cancel.filled_size
    assert schedule["cancel_pending_factor"] == Decimal("0.3500000000")
    assert schedule["cancel_pending_reason"] == "cancel_pending_before_ack_discount"
    assert evidence["cancel_pending_adjusted_tick_count"] == 1
    assert evidence["uses_cancel_pending_race_discount"] is True


def test_cancel_pending_window_discounts_fill_before_cancel_failed() -> None:
    events = [
        ReplayTradeEvent(
            market_id=1,
            token_id="abc",
            block_number=101,
            transaction_index=0,
            log_index=2,
            tx_hash="0xafter-cancel",
            trade_price=Decimal("0.49"),
            size=Decimal("10"),
            side_code="SELL",
        )
    ]

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("10"),
            "GTC",
            sequence_key(100, 0, 1, "0xsubmit"),
            liquidity_cap_pct=Decimal("100"),
            order_role="maker",
            execution_profile="conservative",
            cancel_sequence=sequence_key(101, 0, 1, "0xcancel-request"),
            cancel_fail_sequence=sequence_key(101, 0, 3, "0xcancel-failed"),
        ),
        events,
    )
    schedule = result.to_fill_dict()["fill_schedule"][0]

    assert result.status == "PARTIAL_FILLED"
    assert schedule["cancel_pending_factor"] == Decimal("0.4500000000")
    assert schedule["cancel_pending_reason"] == "cancel_pending_before_failed_cancel_discount"


def test_partial_fill_before_cancel_ack_marks_residual_as_cancelled():
    events = [
        ReplayTradeEvent(
            market_id=1,
            condition_id="cond-worldcup",
            token_id="token-yes",
            block_number=101,
            transaction_index=0,
            log_index=1,
            tx_hash="0xpartial",
            trade_price=Decimal("0.49"),
            size=Decimal("2"),
        ),
        ReplayTradeEvent(
            market_id=1,
            condition_id="cond-worldcup",
            token_id="token-yes",
            block_number=102,
            transaction_index=0,
            log_index=1,
            tx_hash="0xlater",
            trade_price=Decimal("0.48"),
            size=Decimal("20"),
        ),
    ]

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("10"),
            "GTC",
            sequence_key(100, 0, 1, "0xsubmit"),
            cancel_sequence=sequence_key(101, 0, 2, "0xcancel-request"),
            cancel_ack_sequence=sequence_key(101, 0, 3, "0xcancel-ack"),
        ),
        events,
    )

    fill_dict = result.to_fill_dict()
    evidence = fill_dict["execution_model_evidence"]
    assert result.status == "PARTIAL_FILLED"
    assert result.filled_size == Decimal("2.0000000000")
    assert result.unfilled_size == Decimal("8.0000000000")
    assert result.no_fill_reason == "residual_cancelled_after_partial_fill"
    assert fill_dict["notes"] == ["residual_cancelled_after_partial_fill"]
    assert evidence["partial_residual_reason"] == "residual_cancelled_after_partial_fill"
    assert evidence["partial_residual_lifecycle"] == "cancelled"
    assert evidence["partial_residual_size"] == Decimal("8.0000000000")
    assert [event.tx_hash for event in result.candidate_events] == ["0xpartial"]
    assert [event.tx_hash for event in result.consumed_events] == ["0xpartial"]
    assert result.consumed_events[0].condition_id == "cond-worldcup"
    assert fill_dict["consumed_events"][0]["condition_id"] == "cond-worldcup"
    assert fill_dict["fill_schedule"][0]["condition_id"] == "cond-worldcup"


def test_cancel_failed_keeps_order_active_for_later_fill():
    events = FixtureReplayStore().load_trade_events("single_fill")

    result = replay_limit_order(
        OrderIntent(
            "BUY_YES",
            Decimal("0.50"),
            Decimal("4"),
            "GTC",
            sequence_key(100, 0, 2, "0xsubmit"),
            cancel_sequence=sequence_key(100, 0, 2, "0xcancel"),
            cancel_fail_sequence=sequence_key(100, 0, 3, "0xcancel-failed"),
        ),
        events,
    )

    assert result.status == "FILLED"
    assert result.filled_size == Decimal("4.0000000000")


def test_invalid_zero_size_order_is_rejected_before_fill_scan():
    events = FixtureReplayStore().load_trade_events("single_fill")

    result = replay_limit_order(
        OrderIntent("BUY_YES", Decimal("0.50"), Decimal("0"), "GTC", sequence_key(100, 0, 2, "0xsubmit")),
        events,
    )

    assert result.status == "REJECTED"
    assert result.no_fill_reason == "invalid_order_size"
    assert result.candidate_events
    assert result.consumed_events == []
    assert result.fill_probability == Decimal("0E-10")
