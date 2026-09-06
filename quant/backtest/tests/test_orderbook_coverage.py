from datetime import datetime, timezone
from decimal import Decimal

from quant.backtest.execution import BookSnapshot
from quant.orderbook.coverage import build_lob_execution_coverage_report


def test_lob_coverage_counts_covered_stale_and_no_book_orders() -> None:
    snapshots = [
        {
            "snapshot_id": 1,
            "token_id": "token-a",
            "snapshot_timestamp": "2026-06-27T12:00:00Z",
            "book_status": "ok",
            "snapshot_version": "v1",
        },
        {
            "snapshot_id": 2,
            "token_id": "token-a",
            "snapshot_timestamp": "2026-06-27T12:05:00Z",
            "book_status": "ok",
            "snapshot_version": "v2",
        },
    ]
    orders = [
        {"order_id": "covered", "token_id": "token-a", "submit_time": "2026-06-27T12:06:00Z"},
        {"order_id": "stale", "token_id": "token-a", "submit_time": "2026-06-27T12:20:01Z"},
        {"order_id": "missing", "token_id": "token-b", "submit_time": "2026-06-27T12:06:00Z"},
    ]

    report = build_lob_execution_coverage_report(orders, snapshots, max_staleness_seconds=Decimal("900"))

    assert report["status"] == "review"
    assert report["order_count"] == 3
    assert report["snapshot_count"] == 2
    assert report["covered_order_count"] == 1
    assert report["stale_book_count"] == 1
    assert report["no_book_count"] == 1
    assert report["coverage_pct"] == "33.33"
    assert {row["order_id"]: row["reason"] for row in report["rows"]} == {
        "covered": "covered",
        "stale": "stale_book",
        "missing": "no_book",
    }


def test_lob_coverage_never_uses_future_snapshots() -> None:
    report = build_lob_execution_coverage_report(
        [{"order_id": "order-1", "token_id": "token-a", "submit_time": "2026-06-27T12:00:00Z"}],
        [
            {
                "snapshot_id": 99,
                "token_id": "token-a",
                "snapshot_timestamp": "2026-06-27T12:00:01Z",
                "book_status": "ok",
            }
        ],
    )

    assert report["covered_order_count"] == 0
    assert report["no_book_count"] == 1
    assert report["rows"][0]["snapshot_id"] is None


def test_lob_coverage_accepts_booksnapshot_and_block_axis() -> None:
    snapshot = BookSnapshot(
        snapshot_id=7,
        token_id="token-a",
        side="YES",
        bids=((Decimal("0.49"), Decimal("100")),),
        asks=((Decimal("0.51"), Decimal("100")),),
        book_status="ok",
        block_number=100,
        timestamp=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
        snapshot_version="book-v7",
    )

    report = build_lob_execution_coverage_report(
        [{"order_id": "order-1", "token_id": "token-a", "submit_block": 101}],
        [snapshot],
    )

    assert report["status"] == "ready"
    assert report["covered_order_count"] == 1
    assert report["rows"][0]["snapshot_id"] == 7
    assert report["rows"][0]["snapshot_block"] == 100
    assert report["rows"][0]["snapshot_version"] == "book-v7"
