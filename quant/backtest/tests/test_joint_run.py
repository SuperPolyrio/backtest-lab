from decimal import Decimal

from quant.backtest.joint_run import _block_window, build_joint_backtest_result


def test_build_joint_backtest_result_persists_prefixed_multi_outcome_state() -> None:
    result = build_joint_backtest_result(
        {"eventSlug": "world-cup", "priceSource": "orderfilled_block_close"},
        [
            {
                "outcomeKey": "france",
                "marketSlug": "france-market",
                "tokenSide": "YES",
                "pricePoints": [{"x_value": 100, "price": "0.20"}],
                "orders": [
                    {
                        "orderId": "o1",
                        "signalIndex": 1,
                        "submitX": 101,
                        "requestedPrice": "0.20",
                        "requestedSize": "5",
                        "requestedNotional": "1.00",
                        "filledSize": "5",
                        "filledNotional": "1.00",
                        "status": "FILLED",
                        "side": "BUY_YES",
                    }
                ],
                "ledger": [
                    {
                        "ledgerId": "l1",
                        "eventType": "BUY",
                        "xValue": 101,
                        "orderId": "o1",
                        "sharesDelta": "5",
                        "cashDelta": "-1.00",
                        "price": "0.20",
                    }
                ],
            },
            {
                "outcomeKey": "spain",
                "marketSlug": "spain-market",
                "tokenSide": "YES",
                "pricePoints": [{"x_value": 100, "price": "0.15"}],
                "orders": [
                    {
                        "orderId": "o1",
                        "signalIndex": 1,
                        "submitX": 102,
                        "requestedPrice": "0.15",
                        "requestedSize": "4",
                        "requestedNotional": "0.60",
                        "filledSize": "4",
                        "filledNotional": "0.60",
                        "status": "FILLED",
                        "side": "BUY_YES",
                    }
                ],
                "ledger": [
                    {
                        "ledgerId": "l1",
                        "eventType": "BUY",
                        "xValue": 102,
                        "orderId": "o1",
                        "sharesDelta": "4",
                        "cashDelta": "-0.60",
                        "price": "0.15",
                    }
                ],
            },
        ],
    )

    assert result["joint_execution_report"]["execution_verdict"] == "joint_execution_ready"
    assert result["joint_execution_report"]["portfolio_equity"] == "0"
    assert result["joint_execution_report"]["event_probability_report"]["probability_sum"] == "0.35"
    assert result["joint_execution_report"]["event_probability_report"]["probability_gap"] == "0.65"
    exposure = result["joint_execution_report"]["event_exposure_report"]
    assert exposure["status"] == "ready"
    assert exposure["total_gross_cash_at_risk"] == "1.6"
    assert exposure["worst_case_cash_loss"] == "0.6"
    assert exposure["rows"][0]["event_key"] == "world-cup"
    assert exposure["rows"][0]["outcome_count"] == 2
    assert exposure["rows"][0]["gross_cash_at_risk"] == "1.6"
    assert exposure["rows"][0]["max_single_winner_marked_value"] == "1"
    assert exposure["rows"][0]["worst_case_cash_loss"] == "0.6"
    assert result["data_quality"]["event_exposure_report"]["status"] == "ready"
    assert result["data_quality"]["event_probability_report"]["outcome_count"] == 2
    assert result["orders"][0]["order_id"] == "france:o1"
    assert result["orders"][1]["order_id"] == "spain:o1"
    assert result["ledger"][-1]["cash_after"] == Decimal("-1.60")
    assert result["equity"][-1]["equity"] == Decimal("0")
    assert {row["metric_key"] for row in result["metrics"]} >= {
        "joint_execution_status",
        "joint_fill_rate",
        "joint_max_cash_at_risk",
        "joint_portfolio_equity",
        "joint_probability_sum",
        "joint_probability_gap",
        "joint_event_gross_cash_at_risk",
        "joint_event_worst_case_cash_loss",
    }


def test_joint_backtest_probability_sum_uses_latest_outcome_prices() -> None:
    result = build_joint_backtest_result(
        {"eventSlug": "two-way", "priceSource": "orderfilled_block_close"},
        [
            {
                "outcomeKey": "yes",
                "marketSlug": "yes-market",
                "tokenSide": "YES",
                "pricePoints": [
                    {"x_value": 10, "price": "0.55"},
                    {"x_value": 12, "price": "0.60"},
                ],
                "orders": [{"orderId": "o1", "submitX": 11, "filledSize": "1", "filledNotional": "0.55"}],
                "ledger": [{"ledgerId": "l1", "eventType": "BUY", "xValue": 11, "cashDelta": "-0.55", "sharesDelta": "1"}],
            },
            {
                "outcomeKey": "no",
                "marketSlug": "no-market",
                "tokenSide": "YES",
                "pricePoints": [
                    {"x_value": 10, "price": "0.42"},
                    {"x_value": 13, "price": "0.40"},
                ],
                "orders": [{"orderId": "o1", "submitX": 12, "filledSize": "1", "filledNotional": "0.40"}],
                "ledger": [{"ledgerId": "l1", "eventType": "BUY", "xValue": 12, "cashDelta": "-0.40", "sharesDelta": "1"}],
            },
        ],
    )
    metrics = {row["metric_key"]: row for row in result["metrics"]}

    assert result["data_quality"]["event_probability_report"]["probability_sum"] == "1"
    assert result["data_quality"]["event_probability_report"]["probability_gap"] == "0"
    assert result["data_quality"]["event_probability_report"]["sum_status"] == "ready"
    assert metrics["joint_probability_sum"]["value"] == Decimal("1.0000000000")
    assert metrics["joint_probability_gap"]["value"] == Decimal("0E-10")


def test_joint_backtest_equity_rows_use_positive_portfolio_drawdown() -> None:
    result = build_joint_backtest_result(
        {"eventSlug": "world-cup", "priceSource": "orderfilled_block_close"},
        [
            {
                "outcomeKey": "france",
                "marketSlug": "france-market",
                "tokenSide": "YES",
                "pricePoints": [
                    {"x_value": 100, "price": "0.20"},
                    {"x_value": 103, "price": "0.50"},
                    {"x_value": 104, "price": "0.10"},
                ],
                "orders": [{"orderId": "o1", "submitX": 101, "filledSize": "5", "filledNotional": "1.00"}],
                "ledger": [{"ledgerId": "l1", "eventType": "BUY", "xValue": 101, "cashDelta": "-1.00", "sharesDelta": "5"}],
            },
            {
                "outcomeKey": "spain",
                "marketSlug": "spain-market",
                "tokenSide": "YES",
                "pricePoints": [{"x_value": 100, "price": "0.15"}],
                "orders": [{"orderId": "o2", "submitX": 102, "filledSize": "4", "filledNotional": "0.60"}],
                "ledger": [{"ledgerId": "l2", "eventType": "BUY", "xValue": 102, "cashDelta": "-0.60", "sharesDelta": "4"}],
            },
        ],
    )

    equity_rows = result["equity"]
    drawdowns = [row["drawdown"] for row in equity_rows]
    assert all(value >= Decimal("0") for value in drawdowns)
    assert max(drawdowns) == Decimal("2.0000000000")
    assert result["joint_execution_report"]["max_drawdown"] == "2"


def test_joint_backtest_result_replays_payload_cashflow_events_into_global_ledger() -> None:
    result = build_joint_backtest_result(
        {
            "eventSlug": "world-cup",
            "priceSource": "orderfilled_block_close",
            "cashflowEvents": [
                {
                    "id": "split-1",
                    "type": "SPLIT",
                    "market_slug": "spain-market",
                    "event_slug": "world-cup",
                    "token_id": "spain-yes",
                    "token_side": "YES",
                    "amount": "2",
                    "size": "2",
                    "block_number": 20,
                },
                {
                    "id": "merge-1",
                    "type": "MERGE",
                    "market_slug": "spain-market",
                    "event_slug": "world-cup",
                    "token_id": "spain-yes",
                    "token_side": "YES",
                    "amount": "1",
                    "size": "1",
                    "block_number": 30,
                },
            ],
        },
        [
            {
                "outcomeKey": "france",
                "marketSlug": "france-market",
                "tokenSide": "YES",
                "pricePoints": [{"x_value": 10, "price": "0.20"}],
                "orders": [
                    {
                        "orderId": "o1",
                        "submitX": 10,
                        "filledSize": "5",
                        "filledNotional": "1.00",
                        "status": "FILLED",
                    }
                ],
                "ledger": [
                    {
                        "ledgerId": "fr-buy",
                        "eventType": "BUY",
                        "xValue": 10,
                        "orderId": "o1",
                        "sharesDelta": "5",
                        "cashDelta": "-1.00",
                    }
                ],
            },
            {
                "outcomeKey": "spain",
                "marketSlug": "spain-market",
                "tokenSide": "YES",
                "pricePoints": [{"x_value": 10, "price": "0.15"}],
                "orders": [],
                "ledger": [],
            },
        ],
    )

    assert [row["event_type"] for row in result["ledger"]] == ["BUY", "SPLIT", "MERGE"]
    assert [row["cash_after"] for row in result["ledger"]] == [
        Decimal("-1.00"),
        Decimal("-3.0000000000"),
        Decimal("-2.0000000000"),
    ]
    assert result["ledger"][-1]["position_after"] == Decimal("1.0000000000")
    assert Decimal(result["joint_execution_report"]["max_cash_at_risk"]) == Decimal("3")


def test_joint_backtest_result_carries_raw_orderfilled_ticks_into_replay_report() -> None:
    result = build_joint_backtest_result(
        {"eventSlug": "world-cup", "priceSource": "orderfilled_block_close"},
        [
            {
                "outcomeKey": "france",
                "marketSlug": "france-market",
                "tokenSide": "YES",
                "pricePoints": [{"x_value": 101, "price": "0.20"}],
                "orders": [
                    {
                        "orderId": "entry",
                        "submitX": 101,
                        "requestedPrice": "0.20",
                        "requestedSize": "2",
                        "filledSize": "2",
                        "filledNotional": "0.40",
                        "status": "FILLED",
                    }
                ],
                "ledger": [
                    {
                        "ledgerId": "cash",
                        "eventType": "BUY",
                        "xValue": 101,
                        "orderId": "entry",
                        "sharesDelta": "2",
                        "cashDelta": "-0.40",
                    }
                ],
                "rawOrderfilledEvents": [
                    {
                        "block_number": 102,
                        "transaction_index": 1,
                        "log_index": 4,
                        "tx_hash": "0xf1",
                        "market_id": "m-france",
                        "token_id": "token-france",
                        "maker": "0xmaker1",
                        "taker": "0xtaker1",
                        "side": "BUY",
                        "trade_price": "0.21",
                        "size": "2",
                    },
                    {
                        "block_number": 102,
                        "transaction_index": 1,
                        "log_index": 4,
                        "tx_hash": "0xf1",
                        "market_id": "m-france",
                        "token_id": "token-france",
                        "maker": "0xmaker1",
                        "taker": "0xtaker1",
                        "side": "BUY",
                        "trade_price": "0.21",
                        "size": "2",
                    },
                    {
                        "block_number": 101,
                        "transaction_index": 2,
                        "log_index": 1,
                        "tx_hash": "0xf0",
                        "market_id": "m-france",
                        "token_id": "token-france",
                        "maker": "0xmaker2",
                        "taker": "0xtaker2",
                        "side": "SELL",
                        "trade_price": "0.18",
                        "size": "1",
                    },
                ],
            },
            {
                "outcomeKey": "spain",
                "marketSlug": "spain-market",
                "tokenSide": "YES",
                "pricePoints": [{"x_value": 101, "price": "0.15"}],
                "orders": [
                    {
                        "orderId": "entry",
                        "submitX": 101,
                        "requestedPrice": "0.15",
                        "requestedSize": "4",
                        "filledSize": "4",
                        "filledNotional": "0.60",
                        "status": "FILLED",
                    }
                ],
                "ledger": [
                    {
                        "ledgerId": "cash",
                        "eventType": "BUY",
                        "xValue": 101,
                        "orderId": "entry",
                        "sharesDelta": "4",
                        "cashDelta": "-0.60",
                    }
                ],
                "tradeTicks": [
                    {
                        "block_number": 101,
                        "transaction_index": 1,
                        "log_index": 3,
                        "tx_hash": "0xs",
                        "market_id": "m-spain",
                        "token_id": "token-spain",
                        "maker": "0xmaker3",
                        "taker": "0xtaker3",
                        "side": "BUY",
                        "trade_price": "0.15",
                        "size": "4",
                    }
                ],
            },
        ],
    )

    raw_report = result["joint_execution_report"]["raw_trade_tick_report"]

    assert raw_report["status"] == "ready"
    assert raw_report["trade_tick_count"] == 3
    assert raw_report["block_count"] == 3
    assert raw_report["canonical_fill_key_coverage_pct"] == "100"
    assert raw_report["maker_taker_side_coverage_pct"] == "100"
    assert raw_report["block_context_coverage_pct"] == "100"
    assert raw_report["blocks"][0]["block_number"] == 101
    assert raw_report["blocks"][0]["market_slug"] == "france-market"
    assert raw_report["blocks"][0]["vwap"] == "0.18"
    assert raw_report["blocks"][1]["market_slug"] == "spain-market"
    assert raw_report["blocks"][1]["vwap"] == "0.15"
    assert [row["tx_hash"] for row in raw_report["blocks"][0]["order_sequence"]] == ["0xf0"]
    assert [row["tx_hash"] for row in raw_report["blocks"][1]["order_sequence"]] == ["0xs"]
    assert raw_report["blocks"][2]["open"] == "0.21"
    assert raw_report["blocks"][2]["close"] == "0.21"


def test_joint_block_window_includes_raw_orderfilled_ticks_and_order_evidence() -> None:
    from_block, to_block = _block_window(
        [
            {
                "outcomeKey": "france",
                "pricePoints": [{"x_value": 110, "price": "0.20"}],
                "orders": [
                    {
                        "orderId": "o1",
                        "submitX": 111,
                        "meta": {
                            "consumed_events": [
                                {"block_number": 109, "transaction_index": 1, "log_index": 1},
                            ]
                        },
                    }
                ],
                "rawOrderfilledEvents": [
                    {"block_number": 108, "transaction_index": 1, "log_index": 1},
                    {"block_number": 112, "transaction_index": 1, "log_index": 2},
                ],
            }
        ]
    )

    assert (from_block, to_block) == (108, 112)


def test_native_joint_backtest_result_executes_multi_outcome_price_points() -> None:
    result = build_joint_backtest_result(
        {
            "eventSlug": "world-cup",
            "priceSource": "orderfilled_block_close",
            "nativeJointRun": True,
            "runMode": "native_joint_event_stream",
            "entryThreshold": 0.18,
            "exitThreshold": 0.12,
            "positionSize": 10,
            "liquidityCapPct": 100,
            "fillProbabilityHaircutPct": 0,
            "executionPriceMode": "ORDERFILLED_LIMIT_REPLAY",
        },
        [
            {
                "outcomeKey": "france",
                "marketSlug": "france-market",
                "tokenSide": "YES",
                "pricePoints": [
                    {"x_value": 100, "price": "0.19", "volume": "100", "trade_count": 2},
                    {"x_value": 101, "price": "0.20", "volume": "100", "trade_count": 2},
                    {"x_value": 102, "price": "0.11", "volume": "100", "trade_count": 2},
                ],
            },
            {
                "outcomeKey": "spain",
                "marketSlug": "spain-market",
                "tokenSide": "YES",
                "pricePoints": [
                    {"x_value": 100, "price": "0.15", "volume": "100", "trade_count": 2},
                    {"x_value": 101, "price": "0.16", "volume": "100", "trade_count": 2},
                ],
            },
        ],
    )

    assert result["data_quality"]["source_table"] == "native_joint_event_stream"
    assert result["data_quality"]["native_joint_runner"] is True
    assert result["data_quality"]["requested_execution_price_mode"] == "ORDERFILLED_CROSS"
    assert result["data_quality"]["actual_execution_price_mode"] == "ORDERFILLED_CROSS"
    assert result["joint_execution_report"]["execution_verdict"] == "joint_execution_ready"
    assert [row["side"] for row in result["orders"]] == ["BUY_YES", "SELL_YES", "SELL_YES"]
    assert result["orders"][0]["order_id"] == "france:O-0001"
    assert result["trades"][0]["trade_id"] == "france:T-0001"
    assert [row["event_type"] for row in result["ledger"]] == ["BUY", "SELL", "SELL"]
    assert result["orders"][0]["execution_source"] == "native_joint_event_stream:orderfilled_limit_replay_synthetic"
    assert result["orders"][0]["status"] == "FILLED"
    assert result["orders"][0]["meta"]["resting_order_continued"] is True
    assert result["orders"][1]["status"] == "PARTIAL_FILLED"
    assert result["fill_quality"]["submitted_count"] == 3
    assert result["fill_quality"]["filled_count"] == 3
    assert result["fill_quality"]["partial_fill_count"] == 1
    assert result["fill_quality"]["avg_fill_probability_haircut_pct"] == "0"


def test_native_joint_cross_mode_uses_raw_orderfilled_ticks_for_execution_size() -> None:
    result = build_joint_backtest_result(
        {
            "eventSlug": "world-cup",
            "priceSource": "orderfilled_block_close",
            "nativeJointRun": True,
            "runMode": "native_joint_event_stream",
            "entryThreshold": 0.18,
            "exitThreshold": 0.20,
            "positionSize": 1,
            "liquidityCapPct": 100,
            "fillProbabilityHaircutPct": 0,
            "executionProfile": "optimistic",
            "orderRole": "maker",
            "executionPriceMode": "ORDERFILLED_LIMIT_REPLAY",
        },
        [
            {
                "outcomeKey": "france",
                "marketSlug": "france-market",
                "tokenSide": "YES",
                "pricePoints": [
                    {"x_value": 100, "price": "0.20", "volume": "100", "trade_count": 2},
                    {"x_value": 101, "price": "0.20", "volume": "100", "trade_count": 2},
                    {"x_value": 102, "price": "0.25", "volume": "100", "trade_count": 2},
                    {"x_value": 103, "price": "0.25", "volume": "100", "trade_count": 2},
                ],
                "rawOrderfilledEvents": [
                    {
                        "market_id": 1,
                        "token_id": "abc",
                        "block_number": 100,
                        "transaction_index": 0,
                        "log_index": 3,
                        "tx_hash": "0xentry",
                        "trade_price": "0.19",
                        "size": "1",
                        "maker": "0xmaker",
                        "taker": "0xtaker",
                        "side_code": "BUY",
                    },
                    {
                        "market_id": 1,
                        "token_id": "abc",
                        "block_number": 101,
                        "transaction_index": 0,
                        "log_index": 4,
                        "tx_hash": "0xentry-2",
                        "trade_price": "0.20",
                        "size": "4",
                        "maker": "0xmaker1b",
                        "taker": "0xtaker1b",
                        "side_code": "BUY",
                    },
                    {
                        "market_id": 1,
                        "token_id": "abc",
                        "block_number": 102,
                        "transaction_index": 0,
                        "log_index": 5,
                        "tx_hash": "0xexit",
                        "trade_price": "0.25",
                        "size": "2",
                        "maker": "0xmaker2",
                        "taker": "0xtaker2",
                        "side_code": "SELL",
                    },
                    {
                        "market_id": 1,
                        "token_id": "abc",
                        "block_number": 103,
                        "transaction_index": 0,
                        "log_index": 6,
                        "tx_hash": "0xexit-2",
                        "trade_price": "0.26",
                        "size": "3",
                        "maker": "0xmaker2b",
                        "taker": "0xtaker2b",
                        "side_code": "SELL",
                    },
                ],
            }
        ],
    )

    entry, exit_order = result["orders"]
    raw_report = result["joint_execution_report"]["raw_trade_tick_report"]

    assert result["data_quality"]["actual_execution_price_mode"] == "ORDERFILLED_CROSS"
    assert entry["execution_source"] == "native_joint_event_stream:orderfilled_limit_replay_raw"
    assert exit_order["execution_source"] == "native_joint_event_stream:orderfilled_limit_replay_raw"
    assert entry["filled_size"] == Decimal("5.0000000000")
    assert entry["block_volume"] == Decimal("5.0000000000")
    assert entry["meta"]["resting_order_continued"] is True
    assert entry["meta"]["candidate_events"][0]["tx_hash"] == "0xentry"
    assert entry["meta"]["consumed_events"][-1]["tx_hash"] == "0xentry-2"
    assert exit_order["filled_size"] == Decimal("5.0000000000")
    assert exit_order["meta"]["resting_order_continued"] is True
    assert exit_order["meta"]["candidate_events"][0]["tx_hash"] == "0xexit"
    assert exit_order["meta"]["consumed_events"][-1]["tx_hash"] == "0xexit-2"
    assert raw_report["trade_tick_count"] == 4
    assert raw_report["block_count"] == 4
    assert raw_report["canonical_fill_key_coverage_pct"] == "100"
    assert result["trades"][0]["size"] == Decimal("5.0000000000")


def test_native_joint_orderfilled_lob_mode_uses_outcome_snapshots() -> None:
    result = build_joint_backtest_result(
        {
            "eventSlug": "world-cup",
            "priceSource": "orderfilled_block_close",
            "nativeJointRun": True,
            "runMode": "native_joint_event_stream",
            "entryThreshold": 0.18,
            "exitThreshold": 0.10,
            "positionSize": 1,
            "liquidityCapPct": 100,
            "fillProbabilityHaircutPct": 0,
            "executionProfile": "optimistic",
            "orderRole": "taker",
            "finalValuationMode": "FORCE_CLOSE",
            "executionPriceMode": "ORDERFILLED_LOB",
        },
        [
            {
                "outcomeKey": "france",
                "marketSlug": "france-market",
                "tokenSide": "YES",
                "pricePoints": [
                    {
                        "x_value": 100,
                        "price": "0.20",
                        "volume": "100",
                        "trade_count": 3,
                        "timestamp": "2026-06-22T12:00:00Z",
                    },
                    {
                        "x_value": 101,
                        "price": "0.21",
                        "volume": "100",
                        "trade_count": 3,
                        "timestamp": "2026-06-22T12:01:00Z",
                    },
                ],
                "clobSnapshots": [
                    {
                        "snapshot_id": 42,
                        "token_id": "yes-token",
                        "side": "YES",
                        "block_number": 99,
                        "snapshot_timestamp": "2026-06-22T11:59:50Z",
                        "bids": [{"price": "0.19", "size": "10"}],
                        "asks": [{"price": "0.21", "size": "2"}],
                        "snapshot_version": "book-42",
                    }
                ],
            }
        ],
    )

    assert result["data_quality"]["requested_execution_price_mode"] == "ORDERFILLED_LOB"
    assert result["data_quality"]["actual_execution_price_mode"] == "ORDERFILLED_LOB"
    assert result["orders"][0]["execution_source"] == "native_joint_event_stream:l2_orderfilled"
    assert result["orders"][0]["meta"]["execution_model"] == "L2OrderFilledExecutionModel"
    assert result["orders"][0]["meta"]["book_snapshot_id"] == 42
    assert result["orders"][0]["filled_size"] == Decimal("2.0000000000")
    assert result["orders"][0]["meta"]["snapshot_version"] == "book-42"
    assert result["trades"][0]["book_snapshot_id"] == 42


def test_native_joint_depth_mode_uses_outcome_snapshots_without_raw_replay() -> None:
    result = build_joint_backtest_result(
        {
            "eventSlug": "world-cup",
            "priceSource": "orderfilled_block_close",
            "nativeJointRun": True,
            "runMode": "native_joint_event_stream",
            "entryThreshold": 0.18,
            "exitThreshold": 0.10,
            "positionSize": 1,
            "executionProfile": "optimistic",
            "orderRole": "taker",
            "finalValuationMode": "FORCE_CLOSE",
            "executionPriceMode": "DEPTH",
        },
        [
            {
                "outcomeKey": "france",
                "marketSlug": "france-market",
                "tokenSide": "YES",
                "pricePoints": [
                    {
                        "x_value": 100,
                        "price": "0.20",
                        "volume": "100",
                        "trade_count": 3,
                        "timestamp": "2026-06-22T12:00:00Z",
                    },
                    {
                        "x_value": 101,
                        "price": "0.21",
                        "volume": "100",
                        "trade_count": 3,
                        "timestamp": "2026-06-22T12:01:00Z",
                    },
                ],
                "clobSnapshots": [
                    {
                        "snapshot_id": 84,
                        "token_id": "yes-token",
                        "side": "YES",
                        "block_number": 99,
                        "snapshot_timestamp": "2026-06-22T11:59:50Z",
                        "bids": [{"price": "0.19", "size": "10"}],
                        "asks": [{"price": "0.21", "size": "3"}],
                        "snapshot_version": "book-84",
                    }
                ],
            }
        ],
    )

    assert result["data_quality"]["requested_execution_price_mode"] == "DEPTH"
    assert result["data_quality"]["actual_execution_price_mode"] == "DEPTH"
    assert result["orders"][0]["execution_source"] == "native_joint_event_stream:l2_orderfilled_depth"
    assert result["orders"][0]["meta"]["execution_model"] == "L2OrderFilledExecutionModel"
    assert result["orders"][0]["meta"]["book_snapshot_id"] == 84
    assert result["orders"][0]["filled_size"] == Decimal("3.0000000000")
    assert result["trades"][0]["book_snapshot_id"] == 84
