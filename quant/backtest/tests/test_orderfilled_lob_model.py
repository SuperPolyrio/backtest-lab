from datetime import datetime, timezone
from decimal import Decimal

from quant.backtest.backtest_engine import (
    BacktestParameters,
    PricePoint,
    _build_lob_execution_coverage_context,
    _fill_decision,
    data_quality_metrics,
    load_clob_execution_snapshots,
    normalize_execution_price_mode,
    simulate_strategy,
)


def test_engine_orderfilled_lob_mode_uses_l2_orderfilled_execution_model() -> None:
    params = BacktestParameters(
        execution_price_mode="ORDERFILLED_LOB",
        position_size=Decimal("10"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="taker",
        fill_probability_haircut_pct=Decimal("0"),
        adverse_slippage_cents=Decimal("0"),
    )
    point = PricePoint(
        x_value=200,
        price=Decimal("0.20"),
        volume=Decimal("100"),
        trade_count=3,
        timestamp=datetime(2026, 6, 22, tzinfo=timezone.utc),
    )
    run = {
        "price_source": "orderfilled_block_close",
        "_clob_snapshots": [
            {
                "snapshot_id": 42,
                "token_id": "yes-token",
                "side": "YES",
                "block_number": 199,
                "bids": [{"price": "0.19", "size": "10"}],
                "asks": [{"price": "0.21", "size": "20"}],
                "snapshot_version": "book-42",
            }
        ],
    }

    fill = _fill_decision(params, point, run, "BUY_YES")

    assert normalize_execution_price_mode("ORDERFILLED_DEPTH") == "ORDERFILLED_LOB"
    assert fill["execution_source"] == "l2_orderfilled"
    assert fill["execution_model"] == "L2OrderFilledExecutionModel"
    assert fill["fill_model"] == "l2_orderfilled_execution_v1"
    assert fill["residual_book_model"] == "visible_depth_residual_v1"
    assert fill["queue_model"] == "l2_level_queue_v1"
    assert fill["execution_audit"]["schema_version"] == "l2_orderfilled_execution_audit_v1"
    assert fill["execution_audit"]["fill_count"] == 1
    assert fill["book_snapshot_id"] == 42
    assert fill["filled_size"] == Decimal("20.0000000000")
    assert fill["fill_status"] == "PARTIAL"
    assert fill["orderfilled_filled_size"] == Decimal("50.0000000000")
    assert fill["l2_filled_size"] == Decimal("20.0000000000")


def test_orderfilled_lob_trade_keeps_entry_snapshot_when_force_closed() -> None:
    params = BacktestParameters(
        execution_price_mode="ORDERFILLED_LOB",
        entry_threshold=Decimal("0.19"),
        exit_threshold=Decimal("0.10"),
        take_profit=Decimal("0.99"),
        max_holding_bars=100,
        position_size=Decimal("10"),
        liquidity_cap_pct=Decimal("100"),
        execution_profile="optimistic",
        order_role="taker",
        fill_probability_haircut_pct=Decimal("0"),
        adverse_slippage_cents=Decimal("0"),
        final_valuation_mode="FORCE_CLOSE",
    )
    points = [
        PricePoint(
            x_value=200,
            price=Decimal("0.20"),
            volume=Decimal("100"),
            trade_count=3,
            timestamp=datetime(2026, 6, 22, 12, 0, tzinfo=timezone.utc),
        ),
        PricePoint(
            x_value=201,
            price=Decimal("0.20"),
            volume=Decimal("100"),
            trade_count=3,
            timestamp=datetime(2026, 6, 22, 12, 1, tzinfo=timezone.utc),
        ),
    ]
    run = {
        "market_slug": "world-cup-france",
        "token_side": "YES",
        "price_source": "orderfilled_block_close",
        "_clob_snapshots": [
            {
                "snapshot_id": 42,
                "token_id": "yes-token",
                "side": "YES",
                "block_number": 199,
                "bids": [{"price": "0.19", "size": "10"}],
                "asks": [{"price": "0.21", "size": "20"}],
                "snapshot_version": "book-42",
            }
        ],
    }

    result = simulate_strategy(points, run, params)

    assert len(result["trades"]) == 1
    assert result["trades"][0]["exit_reason"] == "end_of_data"
    assert result["trades"][0]["execution_source"] == "forced_mark_to_market"
    assert result["trades"][0]["book_snapshot_id"] == 42
    assert result["trades"][0]["snapshot_version"] == "book-42"
    snapshot_metric = next(row for row in result["metrics"] if row["metric_key"] == "snapshot_fill_coverage")
    assert snapshot_metric["formatted_value"] == "100.0%"
    assert snapshot_metric["delta"] == "1 / 1 trades"


class _SnapshotCursor:
    def __init__(self) -> None:
        self.query = ""
        self.params = ()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        self.query = query
        self.params = tuple(params or ())

    def fetchall(self):
        return [
            {
                "snapshot_id": 101,
                "token_id": "yes-token",
                "side": "YES",
                "source": "pmxt_l2_sampled",
                "book_status": "ok",
                "block_number": 199,
                "snapshot_timestamp": datetime(2026, 6, 22, 11, 59, 30, tzinfo=timezone.utc),
                "snapshot_version": "snap-101",
                "best_bid": Decimal("0.19"),
                "best_ask": Decimal("0.21"),
                "spread": Decimal("0.02"),
                "mid": Decimal("0.20"),
                "bid_depth": Decimal("100"),
                "ask_depth": Decimal("200"),
                "depth_total": Decimal("300"),
                "level_count_bid": 1,
                "level_count_ask": 1,
                "payload": {
                    "bids": [{"price": "0.19", "size": "100"}],
                    "asks": [{"price": "0.21", "size": "200"}],
                },
                "fetched_at": datetime(2026, 6, 22, 11, 59, 30, tzinfo=timezone.utc),
                "created_at": datetime(2026, 6, 22, 12, 0, tzinfo=timezone.utc),
            }
        ]


class _SnapshotConn:
    def __init__(self) -> None:
        self.cursor_obj = _SnapshotCursor()

    def cursor(self):
        return self.cursor_obj


def test_load_clob_execution_snapshots_is_run_window_specific() -> None:
    conn = _SnapshotConn()
    params = BacktestParameters(
        execution_price_mode="ORDERFILLED_LOB",
        max_book_staleness_seconds=Decimal("900"),
        latency_seconds=Decimal("2"),
        latency_blocks=1,
    )
    points = [
        PricePoint(200, Decimal("0.20"), Decimal("10"), timestamp=datetime(2026, 6, 22, 12, 0, tzinfo=timezone.utc)),
        PricePoint(220, Decimal("0.21"), Decimal("10"), timestamp=datetime(2026, 6, 22, 12, 1, tzinfo=timezone.utc)),
    ]

    snapshots = load_clob_execution_snapshots(
        conn,
        {"price_source": "orderfilled_block_close", "token_side": "YES", "meta": {"token_id": "yes-token"}},
        points=points,
        params=params,
        limit=123,
    )

    assert len(snapshots) == 1
    assert snapshots[0].source == "pmxt_l2_sampled"
    assert "COALESCE(snapshot_timestamp, fetched_at)" in conn.cursor_obj.query
    assert "block_number IS NOT NULL" in conn.cursor_obj.query
    assert conn.cursor_obj.params[0:2] == ("yes-token", "YES")
    assert conn.cursor_obj.params[-1] == 123


def test_orderfilled_lob_coverage_context_and_metric_use_loaded_snapshots() -> None:
    params = BacktestParameters(execution_price_mode="ORDERFILLED_LOB", max_book_staleness_seconds=Decimal("900"))
    point = PricePoint(200, Decimal("0.20"), Decimal("10"), timestamp=datetime(2026, 6, 22, 12, 0, tzinfo=timezone.utc))
    snapshot = {
        "snapshot_id": 201,
        "token_id": "yes-token",
        "side": "YES",
        "block_number": 199,
        "timestamp": "2026-06-22T11:59:00+00:00",
        "bids": [{"price": "0.19", "size": "10"}],
        "asks": [{"price": "0.21", "size": "10"}],
        "book_status": "ok",
        "snapshot_version": "book-201",
    }
    fill = _fill_decision(
        params,
        point,
        {"price_source": "orderfilled_block_close", "_clob_snapshots": [snapshot]},
        "BUY_YES",
    )
    orders = [
        {
            "order_id": "O-0001",
            "x_axis": "block_number",
            "submit_x": 200,
            "signal_x": 200,
            "meta": fill,
        }
    ]
    coverage = _build_lob_execution_coverage_context(
        orders,
        [
            snapshot
            for snapshot in (
                load_clob_execution_snapshots(
                    _SnapshotConn(),
                    {"price_source": "orderfilled_block_close", "token_side": "YES", "meta": {"token_id": "yes-token"}},
                    points=[point],
                    params=params,
                    limit=1,
                )
            )
        ],
        run={"price_source": "orderfilled_block_close", "token_side": "YES", "meta": {"token_id": "yes-token"}},
        points=[point],
        params=params,
    )
    metrics = data_quality_metrics({"status": "ready", "rows": 1, "lob_execution_coverage": coverage})

    assert coverage["required"] is True
    assert coverage["covered_order_count"] == 1
    assert coverage["coverage_pct"] == "100.00"
    assert any(row["metric_key"] == "lob_execution_coverage" and row["formatted_value"] == "100.00%" for row in metrics)
