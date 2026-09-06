from decimal import Decimal
import json

from quant.backtest.materialized_execution_summary import (
    ExecutionSummaryFilters,
    build_filter_sql,
    build_summary_filter_sql,
    classify_liquidity_bucket,
    classify_side_bucket,
    grouped_execution_summary,
    summary_from_aggregate,
    group_summary_to_db_params,
    summary_to_db_params,
)


def test_classify_side_bucket_from_maker_taker_amounts() -> None:
    assert classify_side_bucket(60, 40) == "maker_dominant"
    assert classify_side_bucket(40, 60) == "taker_dominant"
    assert classify_side_bucket(50, 50) == "balanced"
    assert classify_side_bucket(0, 0) == "no_counterparty_amount"


def test_classify_liquidity_bucket_uses_volume_and_trades() -> None:
    assert classify_liquidity_bucket(0, 10) == "no_trades"
    assert classify_liquidity_bucket(99, 100) == "thin"
    assert classify_liquidity_bucket(5000, 50) == "medium"
    assert classify_liquidity_bucket(500000, 500) == "deep"
    assert classify_liquidity_bucket(2000000, 2000) == "very_deep"


def test_summary_from_aggregate_computes_materialized_execution_fields() -> None:
    summary = summary_from_aggregate(
        {
            "token_id": "tok-yes",
            "market_id": 123,
            "market_slug": "demo-market",
            "token_side": "yes",
            "first_block": 10,
            "last_block": 20,
            "block_row_count": 3,
            "trade_count": 12,
            "raw_trade_count": 14,
            "volume": "250.5",
            "maker_amount": "80",
            "taker_amount": "20",
            "internal_filtered_count": 1,
            "invalid_size_count": 2,
            "invalid_price_count": 0,
            "amount_ratio_count": 3,
            "raw_price_fallback_count": 0,
            "extreme_trade_count": 1,
            "anomaly_flags": ["invalid_size_filtered", "extreme_price_trade_present", "invalid_size_filtered"],
        }
    )

    assert summary["token_side"] == "YES"
    assert summary["maker_share"] == Decimal("0.8000000000")
    assert summary["taker_share"] == Decimal("0.2000000000")
    assert summary["side_bucket"] == "maker_dominant"
    assert summary["liquidity_bucket"] == "medium"
    assert summary["anomaly_count"] == 7
    assert summary["anomaly_flags"] == ["extreme_price_trade_present", "invalid_size_filtered"]


def test_build_filter_sql_is_keyed_and_bounded() -> None:
    where_sql, params = build_filter_sql(
        ExecutionSummaryFilters(
            market_id=123,
            market_slug="demo-market",
            token_id="tok",
            token_side="yes",
            from_block=10,
            to_block=20,
        )
    )

    assert "market_id = %s" in where_sql
    assert "market_slug = %s" in where_sql
    assert "token_id = %s" in where_sql
    assert "token_side = %s" in where_sql
    assert "block_number >= %s" in where_sql
    assert "block_number <= %s" in where_sql
    assert params == [123, "demo-market", "tok", "YES", 10, 20]


def test_build_summary_filter_sql_can_filter_event_membership() -> None:
    where_sql, params = build_summary_filter_sql(
        ExecutionSummaryFilters(event_slug="world-cup", market_id=123),
        table_alias="s",
        event_alias="mem",
    )

    assert "s.market_id = %s" in where_sql
    assert "mem.event_slug = %s" in where_sql
    assert params == [123, "world-cup"]


def test_summary_to_db_params_serializes_flags() -> None:
    params = summary_to_db_params({"anomaly_flags": ["b", "a"]})

    assert json.loads(params["anomaly_flags_json"]) == ["b", "a"]


def test_grouped_execution_summary_rolls_token_rows_to_market_or_event() -> None:
    rows = [
        {
            "token_id": "yes-1",
            "market_id": 1,
            "market_slug": "market-a",
            "first_block": 10,
            "last_block": 20,
            "block_row_count": 5,
            "trade_count": 12,
            "raw_trade_count": 14,
            "volume": "100",
            "maker_amount": "60",
            "taker_amount": "40",
            "anomaly_count": 1,
            "side_bucket": "maker_dominant",
            "liquidity_bucket": "medium",
        },
        {
            "token_id": "no-1",
            "market_id": 1,
            "market_slug": "market-a",
            "first_block": 12,
            "last_block": 25,
            "block_row_count": 7,
            "trade_count": 8,
            "raw_trade_count": 9,
            "volume": "50",
            "maker_amount": "10",
            "taker_amount": "40",
            "anomaly_count": 2,
            "side_bucket": "taker_dominant",
            "liquidity_bucket": "thin",
        },
    ]

    grouped = grouped_execution_summary(rows, group_key="market_id", id_field="market_id", label_field="market_slug")

    assert len(grouped) == 1
    summary = grouped[0]
    assert summary["market_id"] == "1"
    assert summary["market_slug"] == "market-a"
    assert summary["token_count"] == 2
    assert summary["first_block"] == 10
    assert summary["last_block"] == 25
    assert summary["block_row_count"] == 12
    assert summary["trade_count"] == 20
    assert summary["raw_trade_count"] == 23
    assert summary["volume"] == Decimal("150.0000000000")
    assert summary["maker_share"] == Decimal("0.4666666667")
    assert summary["taker_share"] == Decimal("0.5333333333")
    assert summary["anomaly_count"] == 3
    assert summary["side_bucket_counts"] == {"maker_dominant": 1, "taker_dominant": 1}
    assert summary["liquidity_bucket_counts"] == {"medium": 1, "thin": 1}


def test_group_summary_to_db_params_serializes_bucket_counts() -> None:
    params = group_summary_to_db_params(
        {
            "side_bucket_counts": {"maker_dominant": 2},
            "liquidity_bucket_counts": {"deep": 1},
        }
    )

    assert json.loads(params["side_bucket_counts_json"]) == {"maker_dominant": 2}
    assert json.loads(params["liquidity_bucket_counts_json"]) == {"deep": 1}
