from quant.backtest.event_stream import (
    EVENT_STREAM_SCHEMA_VERSION,
    build_backtest_event_stream,
    build_event_stream_contract_report,
    build_joint_replay_execution_report,
    build_joint_replay_plan_report,
    build_raw_trade_tick_report,
    canonical_fill_key,
    event_stream_to_dicts,
)


def test_build_backtest_event_stream_orders_price_fill_ledger_by_x_value() -> None:
    events = build_backtest_event_stream(
        price_points=[
            {"x_axis": "block_number", "x_value": 101, "price": "0.51", "volume": "5"},
            {"x_axis": "block_number", "x_value": 100, "price": "0.49", "volume": "3"},
        ],
        orders=[
            {
                "order_id": "o1",
                "trade_id": "t1",
                "x_axis": "block_number",
                "signal_x": 100,
                "submit_x": 101,
                "requested_price": "0.50",
                "requested_size": "10",
                "filled_size": "6",
                "avg_fill_price": "0.50",
                "status": "PARTIAL_FILLED",
                "execution_source": "orderfilled_limit_replay_raw",
            }
        ],
        ledger=[
            {
                "ledger_id": "l1",
                "event_type": "BUY",
                "x_axis": "block_number",
                "x_value": 101,
                "trade_id": "t1",
                "cash_delta": "-3",
                "price": "0.50",
            },
            {
                "ledger_id": "l2",
                "event_type": "SETTLEMENT",
                "x_axis": "block_number",
                "x_value": 120,
                "trade_id": "t1",
                "cash_delta": "6",
                "price": "1",
            },
        ],
        market_slug="demo-market",
        token_side="YES",
    )

    rows = event_stream_to_dicts(events)
    assert [row["event_type"] for row in rows] == ["PRICE_BLOCK", "PRICE_BLOCK", "ORDER", "FILL", "LEDGER", "SETTLEMENT"]
    assert [row["x_value"] for row in rows] == [100, 101, 101, 101, 101, 120]
    assert rows[3]["source"] == "orderfilled_limit_replay_raw"
    assert rows[-1]["message"] == "SETTLEMENT"


def test_event_stream_contract_report_requires_order_fill_and_ledger_contract() -> None:
    report = build_event_stream_contract_report(
        orders=[
            {
                "order_id": "o1",
                "x_axis": "block_number",
                "signal_x": 100,
                "submit_x": 101,
                "requested_size": "10",
                "filled_size": "10",
            }
        ],
        ledger=[
            {
                "ledger_id": "l1",
                "event_type": "BUY",
                "x_axis": "block_number",
                "x_value": 101,
                "cash_delta": "-5",
            }
        ],
    )

    assert report["status"] == "ready"
    assert report["schema_version"] == EVENT_STREAM_SCHEMA_VERSION
    assert report["type_counts"]["ORDER"] == 1
    assert report["type_counts"]["FILL"] == 1
    assert report["type_counts"]["LEDGER"] == 1
    assert report["sorted"] is True
    assert "OrderEvent" in report["required_event_classes"]


def test_event_stream_converts_orderfilled_evidence_to_deduped_raw_trade_ticks() -> None:
    shared_trade = {
        "block_number": 101,
        "transaction_index": 2,
        "log_index": 7,
        "tx_hash": "0xabc",
        "market_id": "42",
        "token_id": "token-yes",
        "maker": "0xmaker",
        "taker": "0xtaker",
        "side_code": "BUY",
        "trade_price": "0.49",
        "size": "3",
    }

    events = build_backtest_event_stream(
        orders=[
            {
                "order_id": "o1",
                "trade_id": "t1",
                "submit_x": 100,
                "requested_size": "5",
                "filled_size": "3",
                "avg_fill_price": "0.49",
                "status": "PARTIAL_FILLED",
                "meta": {
                    "consumed_events": [shared_trade],
                    "candidate_events": [shared_trade],
                },
            }
        ],
        market_slug="demo-market",
        token_side="YES",
    )

    rows = event_stream_to_dicts(events)
    raw_rows = [row for row in rows if row["event_type"] == "RAW_TRADE"]
    assert len(raw_rows) == 1
    assert raw_rows[0]["source"] == "orderfilled_fact"
    assert raw_rows[0]["order_id"] == "o1"
    assert raw_rows[0]["trade_id"] == "t1"
    assert raw_rows[0]["price"] == "0.49"
    assert raw_rows[0]["size"] == "3"
    assert raw_rows[0]["meta"]["canonical_fill_key"] == "0xabc|7|42|token-yes|0xmaker|0xtaker|BUY"
    assert raw_rows[0]["meta"]["evidence_role"] == "consumed"
    assert raw_rows[0]["meta"]["block_trade_index"] == 1
    assert raw_rows[0]["meta"]["block_vwap_price"] == "0.49"
    assert [row["event_type"] for row in rows] == ["ORDER", "FILL", "RAW_TRADE"]


def test_raw_orderfilled_events_sort_by_block_transaction_log_before_hash() -> None:
    events = build_backtest_event_stream(
        raw_orderfilled_events=[
            {"block_number": 100, "transaction_index": 2, "log_index": 5, "tx_hash": "0xc", "trade_price": "0.52", "size": "1"},
            {"block_number": 100, "transaction_index": 1, "log_index": 9, "tx_hash": "0xb", "trade_price": "0.51", "size": "1"},
            {"block_number": 100, "transaction_index": 1, "log_index": 3, "tx_hash": "0xa", "trade_price": "0.50", "size": "1"},
            {"block_number": 99, "transaction_index": 9, "log_index": 9, "tx_hash": "0xold", "trade_price": "0.49", "size": "1"},
        ]
    )

    rows = event_stream_to_dicts(events)
    assert [row["meta"]["tx_hash"] for row in rows] == ["0xold", "0xa", "0xb", "0xc"]
    assert [row["meta"]["log_index"] for row in rows] == [9, 3, 9, 5]
    assert [row["meta"]["block_trade_index"] for row in rows if row["x_value"] == 100] == [1, 2, 3]


def test_raw_trade_tick_report_builds_block_ohlcv_vwap_and_order_sequence() -> None:
    events = build_backtest_event_stream(
        raw_orderfilled_events=[
            {
                "block_number": 100,
                "transaction_index": 1,
                "log_index": 3,
                "tx_hash": "0xa",
                "market_id": "42",
                "token_id": "token-yes",
                "maker": "0xmaker1",
                "taker": "0xtaker1",
                "side": "BUY",
                "trade_price": "0.50",
                "size": "2",
            },
            {
                "block_number": 100,
                "transaction_index": 1,
                "log_index": 5,
                "tx_hash": "0xb",
                "market_id": "42",
                "token_id": "token-yes",
                "maker": "0xmaker2",
                "taker": "0xtaker2",
                "side": "SELL",
                "trade_price": "0.60",
                "size": "3",
            },
            {
                "block_number": 100,
                "transaction_index": 1,
                "log_index": 5,
                "tx_hash": "0xb",
                "market_id": "42",
                "token_id": "token-yes",
                "maker": "0xmaker2",
                "taker": "0xtaker2",
                "side": "SELL",
                "trade_price": "0.60",
                "size": "3",
            },
            {
                "block_number": 101,
                "transaction_index": 0,
                "log_index": 1,
                "tx_hash": "0xc",
                "market_id": "42",
                "token_id": "token-yes",
                "maker": "0xmaker3",
                "taker": "0xtaker3",
                "side": "BUY",
                "trade_price": "0.40",
                "size": "1",
            },
        ]
    )

    report = build_raw_trade_tick_report(events)

    assert report["status"] == "ready"
    assert report["trade_tick_count"] == 3
    assert report["block_count"] == 2
    assert report["maker_taker_side_coverage_pct"] == "100"
    assert report["canonical_key_kind_counts"] == {"canonical": 3}
    assert report["canonical_fill_key_count"] == 3
    assert report["fallback_fill_key_count"] == 0
    assert report["canonical_fill_key_coverage_pct"] == "100"
    assert report["block_context_event_count"] == 3
    assert report["block_context_coverage_pct"] == "100"
    assert report["maker_taker_side_missing_count"] == 0
    first_block = report["blocks"][0]
    assert first_block["block_number"] == 100
    assert first_block["trade_tick_count"] == 2
    assert first_block["open"] == "0.5"
    assert first_block["high"] == "0.6"
    assert first_block["low"] == "0.5"
    assert first_block["close"] == "0.6"
    assert first_block["volume"] == "5"
    assert first_block["notional"] == "2.8"
    assert first_block["buy_volume"] == "2"
    assert first_block["sell_volume"] == "3"
    assert first_block["unknown_side_volume"] == "0"
    assert first_block["buy_notional"] == "1"
    assert first_block["sell_notional"] == "1.8"
    assert first_block["unknown_side_notional"] == "0"
    assert first_block["vwap"] == "0.56"
    assert [row["tx_hash"] for row in first_block["order_sequence"]] == ["0xa", "0xb"]
    assert first_block["first_sequence"]["log_index"] == 3
    assert first_block["last_sequence"]["log_index"] == 5

    event_rows = event_stream_to_dicts(events)
    first_tick = next(row for row in event_rows if row["event_type"] == "RAW_TRADE" and row["meta"]["tx_hash"] == "0xa")
    second_tick = next(row for row in event_rows if row["event_type"] == "RAW_TRADE" and row["meta"]["tx_hash"] == "0xb")
    assert first_tick["meta"]["block_trade_index"] == 1
    assert second_tick["meta"]["block_trade_index"] == 2
    assert first_tick["meta"]["block_trade_count"] == 2
    assert first_tick["meta"]["block_open_price"] == "0.5"
    assert first_tick["meta"]["block_high_price"] == "0.6"
    assert first_tick["meta"]["block_low_price"] == "0.5"
    assert first_tick["meta"]["block_close_price"] == "0.6"
    assert first_tick["meta"]["block_vwap_price"] == "0.56"
    assert first_tick["meta"]["block_volume"] == "5"
    assert first_tick["meta"]["block_notional"] == "2.8"
    assert first_tick["meta"]["block_buy_volume"] == "2"
    assert first_tick["meta"]["block_sell_volume"] == "3"
    assert first_tick["meta"]["block_unknown_side_volume"] == "0"
    assert first_tick["meta"]["block_buy_notional"] == "1"
    assert first_tick["meta"]["block_sell_notional"] == "1.8"
    assert first_tick["meta"]["block_unknown_side_notional"] == "0"
    assert first_tick["meta"]["block_first_sequence"]["log_index"] == 3
    assert first_tick["meta"]["block_last_sequence"]["log_index"] == 5


def test_raw_trade_tick_report_partitions_block_summary_by_market_condition_and_token() -> None:
    events = build_backtest_event_stream(
        raw_orderfilled_events=[
            {
                "block_number": 100,
                "transaction_index": 1,
                "log_index": 1,
                "tx_hash": "0xa",
                "market_slug": "world-cup-france",
                "token_side": "YES",
                "market_id": "42",
                "condition_id": "condition-france",
                "token_id": "france-token",
                "maker": "0xmaker-a",
                "taker": "0xtaker-a",
                "side": "BUY",
                "trade_price": "0.20",
                "size": "5",
            },
            {
                "block_number": 100,
                "transaction_index": 1,
                "log_index": 2,
                "tx_hash": "0xb",
                "market_slug": "world-cup-spain",
                "token_side": "YES",
                "market_id": "43",
                "condition_id": "condition-spain",
                "token_id": "spain-token",
                "maker": "0xmaker-b",
                "taker": "0xtaker-b",
                "side": "SELL",
                "trade_price": "0.80",
                "size": "10",
            },
        ]
    )

    report = build_raw_trade_tick_report(events)

    assert report["trade_tick_count"] == 2
    assert report["block_count"] == 2
    assert report["block_identity_fields"] == ["market_slug", "token_side", "market_id", "condition_id", "token_id", "block_number"]
    assert [(row["market_slug"], row["condition_id"], row["token_id"], row["vwap"]) for row in report["blocks"]] == [
        ("world-cup-france", "condition-france", "france-token", "0.2"),
        ("world-cup-spain", "condition-spain", "spain-token", "0.8"),
    ]
    assert [row["trade_tick_count"] for row in report["blocks"]] == [1, 1]
    assert report["blocks"][0]["order_sequence"][0]["condition_id"] == "condition-france"
    assert report["blocks"][1]["order_sequence"][0]["condition_id"] == "condition-spain"


def test_canonical_fill_key_keeps_same_tx_different_log_as_distinct_trade() -> None:
    first = {
        "tx_hash": "0xabc",
        "log_index": 10,
        "market_id": "42",
        "token_id": "token-yes",
        "maker": "0xmaker",
        "taker": "0xtaker",
        "side": "BUY",
    }
    second = {**first, "log_index": 11}

    assert canonical_fill_key(first) != canonical_fill_key(second)


def test_canonical_fill_key_preserves_condition_id_when_available() -> None:
    first = {
        "tx_hash": "0xabc",
        "log_index": 10,
        "market_id": "42",
        "condition_id": "0xcond-a",
        "token_id": "token-yes",
        "maker": "0xmaker",
        "taker": "0xtaker",
        "side": "BUY",
    }
    second = {**first, "condition_id": "0xcond-b"}

    events = build_backtest_event_stream(raw_orderfilled_events=[first | {"block_number": 100, "trade_price": "0.50", "size": "1"}])
    rows = event_stream_to_dicts(events)

    assert canonical_fill_key(first) == "0xabc|10|42|0xcond-a|token-yes|0xmaker|0xtaker|BUY"
    assert canonical_fill_key(first) != canonical_fill_key(second)
    assert rows[0]["meta"]["condition_id"] == "0xcond-a"
    assert rows[0]["meta"]["canonical_fill_key"] == "0xabc|10|42|0xcond-a|token-yes|0xmaker|0xtaker|BUY"
    assert rows[0]["meta"]["block_first_sequence"]["condition_id"] == "0xcond-a"


def test_raw_trade_tick_report_sorts_same_tx_log_by_canonical_fill_key() -> None:
    events = build_backtest_event_stream(
        raw_orderfilled_events=[
            {
                "block_number": 100,
                "transaction_index": 1,
                "log_index": 7,
                "tx_hash": "0xabc",
                "market_slug": "world-cup-france",
                "market_id": "42",
                "condition_id": "condition-france",
                "token_id": "france-token",
                "token_side": "YES",
                "maker": "0xb-maker",
                "taker": "0xtaker",
                "side": "BUY",
                "trade_price": "0.20",
                "size": "1",
            },
            {
                "block_number": 100,
                "transaction_index": 1,
                "log_index": 7,
                "tx_hash": "0xabc",
                "market_slug": "world-cup-france",
                "market_id": "42",
                "condition_id": "condition-france",
                "token_id": "france-token",
                "token_side": "YES",
                "maker": "0xa-maker",
                "taker": "0xtaker",
                "side": "BUY",
                "trade_price": "0.21",
                "size": "1",
            },
        ]
    )

    report = build_raw_trade_tick_report(events)
    order_sequence = report["blocks"][0]["order_sequence"]

    assert [row["maker"] for row in order_sequence] == ["0xa-maker", "0xb-maker"]
    assert order_sequence[0]["canonical_fill_key"] < order_sequence[1]["canonical_fill_key"]


def test_raw_orderfilled_dedupe_normalizes_tx_addresses_and_side() -> None:
    events = build_backtest_event_stream(
        raw_orderfilled_events=[
            {
                "block_number": 100,
                "transaction_index": 1,
                "log_index": 7,
                "tx_hash": "0xABC",
                "market_id": "42",
                "token_id": "token-yes",
                "maker": "0xMAKER",
                "taker": "0xTAKER",
                "side_code": "buy_yes",
                "trade_price": "0.50",
                "size": "2",
            },
            {
                "block_number": 100,
                "transaction_index": 1,
                "log_index": 7,
                "tx_hash": "0xabc",
                "market_id": "42",
                "token_id": "token-yes",
                "maker": "0xmaker",
                "taker": "0xtaker",
                "side": "BUY",
                "trade_price": "0.50",
                "size": "2",
            },
        ]
    )

    rows = event_stream_to_dicts(events)

    assert len(rows) == 1
    assert rows[0]["meta"]["canonical_fill_key"] == "0xabc|7|42|token-yes|0xmaker|0xtaker|BUY"
    assert rows[0]["meta"]["canonical_fill_key_kind"] == "canonical"
    assert rows[0]["meta"]["maker"] == "0xmaker"
    assert rows[0]["meta"]["taker"] == "0xtaker"
    assert rows[0]["meta"]["maker_side"] == "SELL"
    assert rows[0]["meta"]["taker_side"] == "BUY"


def test_raw_orderfilled_fallback_key_is_marked_when_canonical_identity_incomplete() -> None:
    events = build_backtest_event_stream(
        raw_orderfilled_events=[
            {
                "block_number": 100,
                "transaction_index": 1,
                "tx_hash": "0xaaa",
                "log_index": 7,
                "market_id": "42",
                "token_id": "token-yes",
                "trade_price": "0.50",
                "size": "2",
            },
            {
                "block_number": 100,
                "transaction_index": 2,
                "tx_hash": "0xbbb",
                "log_index": 8,
                "market_id": "42",
                "token_id": "token-yes",
                "maker": "0xmaker",
                "taker": "0xtaker",
                "trade_price": "0.50",
                "size": "2",
            },
        ]
    )

    rows = event_stream_to_dicts(events)

    assert len(rows) == 2
    assert {row["meta"]["canonical_fill_key_kind"] for row in rows} == {"fallback"}
    assert rows[0]["meta"]["canonical_fill_key"] != rows[1]["meta"]["canonical_fill_key"]


def test_raw_orderfilled_incomplete_canonical_identity_does_not_dedupe_same_tx_log() -> None:
    incomplete = {
        "block_number": 100,
        "transaction_index": 1,
        "log_index": 7,
        "tx_hash": "0xabc",
        "market_id": "42",
        "token_id": "token-yes",
        "trade_price": "0.50",
        "size": "2",
    }

    events = build_backtest_event_stream(raw_orderfilled_events=[incomplete, dict(incomplete)])
    rows = event_stream_to_dicts(events)
    report = build_raw_trade_tick_report(events)

    assert len(rows) == 2
    assert report["canonical_key_kind_counts"] == {"fallback": 2}
    assert report["canonical_fill_key_coverage_pct"] == "0"
    assert report["maker_taker_side_missing_count"] == 2


def test_raw_orderfilled_fallback_key_does_not_dedupe_weak_identity_ticks() -> None:
    weak_tick = {
        "block_number": 100,
        "market_id": "42",
        "token_id": "token-yes",
        "trade_price": "0.50",
        "size": "2",
    }

    events = build_backtest_event_stream(raw_orderfilled_events=[weak_tick, dict(weak_tick)])
    rows = event_stream_to_dicts(events)
    report = build_raw_trade_tick_report(events)

    assert len(rows) == 2
    assert report["trade_tick_count"] == 2
    assert report["canonical_key_kind_counts"] == {"fallback": 2}


def test_empty_event_stream_contract_is_missing() -> None:
    report = build_event_stream_contract_report()

    assert report["status"] == "missing"
    assert report["event_count"] == 0


def test_joint_replay_plan_merges_multiple_outcomes_in_x_order() -> None:
    report = build_joint_replay_plan_report(
        [
            {
                "outcome_key": "france",
                "market_slug": "world-cup-france",
                "token_side": "YES",
                "price_points": [{"x_value": 101, "price": "0.20"}],
                "orders": [{"order_id": "o-fr", "submit_x": 102, "filled_size": "5", "requested_size": "5"}],
                "ledger": [{"ledger_id": "l-fr", "event_type": "BUY", "x_value": 102, "cash_delta": "-1"}],
            },
            {
                "outcome_key": "spain",
                "market_slug": "world-cup-spain",
                "token_side": "YES",
                "price_points": [{"x_value": 100, "price": "0.14"}],
                "orders": [{"order_id": "o-es", "submit_x": 103, "filled_size": "3", "requested_size": "3"}],
                "ledger": [{"ledger_id": "l-es", "event_type": "BUY", "x_value": 103, "cash_delta": "-0.42"}],
            },
        ]
    )

    assert report["status"] == "ready"
    assert report["joint_replay_verdict"] == "joint_replay_ready"
    assert report["replay_mode"] == "joint_event_stream"
    assert report["outcome_count"] == 2
    assert report["type_counts"]["PRICE_BLOCK"] == 2
    assert report["type_counts"]["ORDER"] == 2
    assert report["type_counts"]["FILL"] == 2
    assert report["sorted"] is True
    assert report["posthoc_sum_risk"] == "controlled"


def test_joint_replay_plan_marks_single_outcome_as_contract_ready_but_not_joint() -> None:
    report = build_joint_replay_plan_report(
        [
            {
                "outcome_key": "france",
                "market_slug": "world-cup-france",
                "orders": [{"order_id": "o-fr", "submit_x": 102, "filled_size": "0", "requested_size": "5"}],
                "ledger": [{"ledger_id": "l-fr", "event_type": "BUY", "x_value": 102, "cash_delta": "0"}],
            }
        ]
    )

    assert report["status"] == "ready"
    assert report["joint_replay_verdict"] == "single_outcome_only"
    assert report["posthoc_sum_risk"] == "review_multi_outcome_batches"
    assert report["next_actions"]


def test_joint_replay_execution_report_replays_portfolio_in_global_x_order() -> None:
    report = build_joint_replay_execution_report(
        [
            {
                "outcome_key": "france",
                "market_slug": "world-cup-france",
                "token_side": "YES",
                "price_points": [
                    {"x_value": 100, "price": "0.20"},
                    {"x_value": 103, "price": "0.10"},
                    {"x_value": 104, "price": "0.25"},
                ],
                "raw_orderfilled_events": [
                    {
                        "block_number": 102,
                        "transaction_index": 1,
                        "log_index": 7,
                        "tx_hash": "0xfr",
                        "market_id": "1",
                        "token_id": "fr-token",
                        "maker": "0xmaker-fr",
                        "taker": "0xtaker-fr",
                        "side": "BUY",
                        "trade_price": "0.20",
                        "size": "5",
                    }
                ],
                "orders": [{"order_id": "o-fr", "submit_x": 102, "filled_size": "5", "requested_size": "5"}],
                "ledger": [{"ledger_id": "l-fr", "event_type": "BUY", "x_value": 102, "cash_delta": "-1.00"}],
            },
            {
                "outcome_key": "spain",
                "market_slug": "world-cup-spain",
                "token_side": "YES",
                "price_points": [{"x_value": 101, "price": "0.15"}],
                "raw_orderfilled_events": [
                    {
                        "block_number": 101,
                        "transaction_index": 0,
                        "log_index": 3,
                        "tx_hash": "0xes",
                        "market_id": "2",
                        "token_id": "es-token",
                        "maker": "0xmaker-es",
                        "taker": "0xtaker-es",
                        "side": "SELL",
                        "trade_price": "0.15",
                        "size": "4",
                    }
                ],
                "orders": [{"order_id": "o-es", "submit_x": 101, "filled_size": "4", "requested_size": "4"}],
                "ledger": [{"ledger_id": "l-es", "event_type": "BUY", "x_value": 101, "cash_delta": "-0.60"}],
            },
        ]
    )

    assert report["status"] == "ready"
    assert report["execution_verdict"] == "joint_execution_ready"
    assert report["replay_mode"] == "joint_event_stream_execution"
    assert report["plan"]["joint_replay_verdict"] == "joint_replay_ready"
    assert report["submitted_orders"] == 2
    assert report["fill_events"] == 2
    assert report["ledger_events"] == 2
    assert report["raw_trade_tick_count"] == 2
    assert report["raw_trade_block_count"] == 2
    assert report["raw_canonical_fill_key_coverage_pct"] == "100"
    assert report["raw_block_context_coverage_pct"] == "100"
    assert report["plan"]["raw_trade_tick_count"] == 2
    assert report["plan"]["raw_trade_block_count"] == 2
    assert report["raw_trade_tick_report"]["blocks"][0]["order_sequence"][0]["market_slug"] == "world-cup-spain"
    assert report["raw_trade_tick_report"]["blocks"][0]["order_sequence"][0]["token_side"] == "YES"
    assert report["cash_balance"] == "-1.6"
    assert report["max_cash_at_risk"] == "1.6"
    assert report["marked_position_value"] == "1.85"
    assert report["portfolio_equity"] == "0.25"
    assert report["equity_curve_points"] == len(report["equity_curve"])
    assert report["max_drawdown"] == "0.5"
    assert any(row["event_type"] == "PRICE_BLOCK" and row["drawdown"] == "0.5" for row in report["equity_curve"])
    assert [row["outcome_key"] for row in report["positions"]] == ["france", "spain"]
    assert report["event_probability_report"]["probability_sum"] == "0.4"
    assert report["event_probability_report"]["probability_gap"] == "-0.6"
    assert report["event_probability_report"]["status"] == "review"
    assert report["timeline"][0]["x_value"] == 101
    assert report["timeline"][-1]["cash_balance"] == "-1.6"
    assert report["posthoc_sum_replaced"] is True


def test_joint_replay_execution_uses_raw_trade_ticks_as_mark_prices_without_price_blocks() -> None:
    report = build_joint_replay_execution_report(
        [
            {
                "outcome_key": "france",
                "market_slug": "world-cup-france",
                "token_side": "YES",
                "orders": [{"order_id": "o-fr", "submit_x": 100, "filled_size": "5", "requested_size": "5"}],
                "ledger": [{"ledger_id": "l-fr", "event_type": "BUY", "x_value": 100, "cash_delta": "-1.00"}],
                "raw_orderfilled_events": [
                    {
                        "block_number": 101,
                        "transaction_index": 1,
                        "log_index": 7,
                        "tx_hash": "0xfr",
                        "market_id": "1",
                        "token_id": "fr-token",
                        "maker": "0xmaker-fr",
                        "taker": "0xtaker-fr",
                        "side": "BUY",
                        "trade_price": "0.30",
                        "size": "8",
                    }
                ],
            }
        ]
    )

    assert report["status"] == "ready"
    assert report["raw_trade_tick_count"] == 1
    assert report["raw_trade_price_update_count"] == 1
    assert report["marked_position_value"] == "1.5"
    assert report["portfolio_equity"] == "0.5"
    assert report["positions"][0]["mark_price"] == "0.3"
    assert report["event_probability_report"]["rows"][0]["latest_probability"] == "0.3"
    assert any(row["event_type"] == "RAW_TRADE" and row["portfolio_equity"] == "0.5" for row in report["equity_curve"])


def test_joint_replay_execution_report_checks_event_probability_sum_and_yes_no_complement() -> None:
    report = build_joint_replay_execution_report(
        [
            {
                "outcome_key": "will-win-yes",
                "market_slug": "will-france-win",
                "token_side": "YES",
                "price_points": [{"x_value": 100, "price": "0.62"}],
                "orders": [{"order_id": "o-yes", "submit_x": 101, "filled_size": "2", "requested_size": "2"}],
                "ledger": [{"ledger_id": "l-yes", "event_type": "BUY", "x_value": 101, "cash_delta": "-1.24"}],
            },
            {
                "outcome_key": "will-win-no",
                "market_slug": "will-france-win",
                "token_side": "NO",
                "price_points": [{"x_value": 100, "price": "0.38"}],
                "orders": [{"order_id": "o-no", "submit_x": 102, "filled_size": "1", "requested_size": "1"}],
                "ledger": [{"ledger_id": "l-no", "event_type": "BUY", "x_value": 102, "cash_delta": "-0.38"}],
            },
        ]
    )

    probability = report["event_probability_report"]
    complement = report["yes_no_complement_report"]

    assert probability["status"] == "ready"
    assert probability["probability_sum"] == "1"
    assert probability["probability_gap"] == "0"
    assert [row["latest_probability"] for row in probability["rows"]] == ["0.38", "0.62"]
    assert complement["status"] == "ready"
    assert complement["checked_count"] == 1
    assert complement["bad_count"] == 0
    assert complement["max_deviation"] == "0"
    assert complement["rows"][0]["sum"] == "1"
