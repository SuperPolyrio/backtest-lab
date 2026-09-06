from __future__ import annotations

from decimal import Decimal

from quant.backtest.replay_contract import REPLAY_SCHEMA_VERSION, build_backtest_replay


def test_replay_contract_uses_ledger_fill_without_order_or_trade_duplicate() -> None:
    replay = build_backtest_replay(
        {
            "run_id": 42,
            "status": "succeeded",
            "market_slug": "world-cup-winner",
            "token_side": "YES",
            "outcome_label": "France",
        },
        orders=[
            {
                "order_id": "O-1",
                "trade_id": "T-1",
                "x_axis": "block_number",
                "submit_x": 101,
                "side": "BUY_YES",
                "status": "FILLED",
                "actual_fill_size": "10",
                "actual_fill_notional": "2.0",
                "avg_fill_price": "0.20",
                "execution_source": "orderfilled_v2_trade_tape",
                "meta": {"signal_id": "S-1"},
            },
            {
                "order_id": "O-2",
                "x_axis": "block_number",
                "submit_x": 103,
                "side": "SELL_YES",
                "status": "NO_FILL",
                "actual_fill_size": "0",
                "no_fill_reason": "no eligible trade prints",
            },
        ],
        ledger=[
            {
                "ledger_id": "L-1",
                "order_id": "O-1",
                "trade_id": "T-1",
                "event_type": "BUY",
                "x_axis": "block_number",
                "x_value": 102,
                "market_slug": "world-cup-winner",
                "token_side": "YES",
                "shares_delta": "10",
                "cash_delta": "-2.01",
                "fee": "0.01",
                "price": "0.20",
                "position_after": "10",
                "cash_after": "997.99",
                "source": "orderfilled_v2_replay",
                "meta": {"fill_index": 1, "source_event_ids": ["trade-print-7"]},
            },
            {
                "ledger_id": "L-2",
                "order_id": "O-1",
                "trade_id": "T-1",
                "event_type": "GAS_COST",
                "x_axis": "block_number",
                "x_value": 102,
                "market_slug": "world-cup-winner",
                "token_side": "YES",
                "shares_delta": "0",
                "cash_delta": "-0.05",
                "position_after": "10",
                "cash_after": "997.94",
                "source": "backtest",
                "meta": {},
            },
        ],
        trades=[{"trade_id": "T-1", "pnl_pct": "4.2", "exit_reason": "take_profit"}],
        events=[
            {
                "event_index": 1,
                "event_type": "open",
                "x_axis": "block_number",
                "x_value": 101,
                "trade_id": "T-1",
                "price": "0.19",
                "message": "entry threshold crossed",
                "meta": {"signal_id": "S-1"},
            }
        ],
    )

    assert replay["schema_version"] == REPLAY_SCHEMA_VERSION
    assert replay["summary"] == {
        "item_count": 4,
        "fill_count": 1,
        "order_count": 1,
        "signal_count": 1,
        "cashflow_count": 1,
        "ledger_fill_count": 1,
        "order_only_fill_count": 0,
    }
    fill = next(item for item in replay["items"] if item["event_type"] == "FILL")
    assert fill["source_table"] == "quant.quant_backtest_ledger"
    assert fill["evidence_level"] == "fill_ledger"
    assert fill["order_id"] == "O-1"
    assert fill["fill_id"] == "L-1:fill"
    assert fill["provenance"]["source_event_ids"] == ["trade-print-7"]
    assert not any(item["replay_id"] == "order:O-1" for item in replay["items"])
    rejected = next(item for item in replay["items"] if item["replay_id"] == "order:O-2")
    assert rejected["event_type"] == "ORDER"
    assert rejected["filled_size"] is None
    assert rejected["reason"] == "no eligible trade prints"


def test_replay_contract_labels_unmatched_filled_order_as_order_only_evidence() -> None:
    replay = build_backtest_replay(
        {"run_id": 7, "status": "succeeded", "market_slug": "demo", "token_side": "YES"},
        orders=[{
            "order_id": "O-7",
            "x_axis": "block_number",
            "submit_x": 700,
            "side": "BUY_YES",
            "status": "PARTIAL",
            "filled_size": "2",
            "filled_notional": "0.5",
            "avg_fill_price": "0.25",
        }],
        ledger=[],
        trades=[],
        events=[],
    )

    assert replay["summary"]["fill_count"] == 1
    assert replay["summary"]["order_only_fill_count"] == 1
    assert replay["items"][0]["evidence_level"] == "order_only"


def test_replay_contract_reads_order_fill_evidence_from_meta() -> None:
    replay = build_backtest_replay(
        {"run_id": 9, "status": "succeeded", "market_slug": "demo", "token_side": "YES"},
        orders=[{
            "order_id": "O-9",
            "x_axis": "block_number",
            "submit_x": 900,
            "side": "BUY_YES",
            "status": "PARTIAL_FILLED",
            "meta": {
                "actual_fill_size": "2",
                "actual_fill_notional": "0.5",
                "avgFillPrice": "0.25",
            },
        }],
        ledger=[],
        trades=[],
        events=[],
    )

    item = replay["items"][0]
    assert item["event_type"] == "FILL"
    assert item["evidence_level"] == "order_only"
    assert item["filled_size"] == 2
    assert item["size_usd"] == "0.5"
    assert item["fill_price"] == Decimal("0.25")


def test_replay_contract_excludes_non_temporal_framework_metadata() -> None:
    replay = build_backtest_replay(
        {"run_id": 8, "status": "succeeded", "market_slug": "demo", "token_side": "YES"},
        orders=[],
        ledger=[],
        trades=[],
        events=[
            {
                "event_index": 1,
                "event_type": "framework",
                "x_axis": "block_number",
                "x_value": 0,
                "message": "backtest engine: builtin",
            },
            {
                "event_index": 2,
                "event_type": "open",
                "x_axis": "block_number",
                "x_value": 123,
                "message": "entry threshold crossed",
            },
        ],
    )

    assert replay["summary"]["signal_count"] == 1
    assert replay["items"][0]["x_value"] == 123


def test_replay_contract_prefers_source_timestamp_over_artifact_created_at() -> None:
    replay = build_backtest_replay(
        {"run_id": 10, "status": "succeeded", "market_slug": "demo", "token_side": "YES"},
        orders=[],
        ledger=[{
            "ledger_id": "L-10",
            "event_type": "BUY",
            "x_axis": "block_number",
            "x_value": 1_000,
            "shares_delta": "1",
            "cash_delta": "-0.2",
            "price": "0.2",
            "created_at": "2026-07-17T15:20:23Z",
            "meta": {"source_timestamp": "2026-04-12T12:26:13+00:00"},
        }],
        trades=[],
        events=[],
    )

    assert replay["items"][0]["timestamp"] == "2026-04-12T12:26:13+00:00"
