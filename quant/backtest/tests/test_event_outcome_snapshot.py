from datetime import datetime, timezone
from decimal import Decimal

from quant.backtest.backtest_engine import (
    _load_event_outcome_correlation,
    _load_event_outcome_snapshot,
    _optional_text,
)


class SnapshotCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.params = ()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self.rows


class SnapshotConnection:
    def __init__(self, rows):
        self.cursor_obj = SnapshotCursor(rows)

    def cursor(self):
        return self.cursor_obj


def test_optional_text_preserves_database_nulls():
    assert _optional_text(None) is None
    assert _optional_text("") is None
    assert _optional_text(" token-1 ") == "token-1"


def test_event_outcome_snapshot_uses_event_members_and_bounded_token_prices():
    timestamp = datetime(2026, 7, 18, tzinfo=timezone.utc)
    conn = SnapshotConnection(
        [
            {
                "event_slug": "world-cup",
                "event_id": "event-1",
                "event_title": "World Cup Winner",
                "event_category": "sports",
                "market_id": 1,
                "market_slug": "france",
                "question": "Will France win?",
                "outcome_label": "France",
                "outcome_key": "france-1",
                "outcome_order": 0,
                "token_yes_id": "yes-1",
                "token_no_id": "no-1",
                "status": "OPEN",
                "active": True,
                "closed": False,
                "resolved": False,
                "yes_probability": Decimal("0.1970"),
                "yes_block": 200,
                "yes_timestamp": timestamp,
                "no_probability": Decimal("0.8030"),
                "no_block": 199,
                "no_timestamp": timestamp,
            },
            {
                "event_slug": "world-cup",
                "event_id": "event-1",
                "event_title": "World Cup Winner",
                "event_category": "sports",
                "market_id": 2,
                "market_slug": "spain",
                "question": "Will Spain win?",
                "outcome_label": "Spain",
                "outcome_key": "spain-2",
                "outcome_order": 1,
                "token_yes_id": "yes-2",
                "token_no_id": "no-2",
                "status": "OPEN",
                "active": True,
                "closed": False,
                "resolved": False,
                "yes_probability": None,
                "yes_block": None,
                "yes_timestamp": None,
                "no_probability": None,
                "no_block": None,
                "no_timestamp": None,
            },
        ]
    )

    snapshot = _load_event_outcome_snapshot(conn, market_slug="france", to_block=200)

    assert snapshot["schema_version"] == "event_outcome_snapshot_v1"
    assert snapshot["event_slug"] == "world-cup"
    assert snapshot["event_outcome_count"] == 2
    assert snapshot["priced_yes_outcome_count"] == 1
    assert snapshot["priced_no_outcome_count"] == 1
    assert snapshot["priced_pair_count"] == 1
    assert snapshot["price_coverage_pct"] == "50"
    assert snapshot["outcomes"][0]["yes_probability"] == "0.197"
    assert snapshot["outcomes"][0]["no_probability"] == "0.803"
    assert snapshot["outcomes"][1]["yes_probability"] is None
    assert conn.cursor_obj.params == ("france", 200)
    assert "LEFT JOIN LATERAL" in conn.cursor_obj.sql
    assert "p.block_number <= bound.to_block" in conn.cursor_obj.sql


def test_event_outcome_snapshot_is_explicitly_empty_when_market_is_unmapped():
    snapshot = _load_event_outcome_snapshot(
        SnapshotConnection([]),
        market_slug="standalone-market",
        to_block=None,
    )

    assert snapshot["event_slug"] is None
    assert snapshot["event_outcome_count"] == 0
    assert snapshot["price_coverage_pct"] == "0"
    assert snapshot["outcomes"] == []


def test_event_outcome_correlation_uses_bounded_asof_grid_and_probability_changes():
    changes = [Decimal("0.01"), Decimal("0.02"), Decimal("-0.01"), Decimal("0.03"), Decimal("0.01"), Decimal("-0.02"), Decimal("0.04"), Decimal("0.01"), Decimal("-0.01")]
    alpha = Decimal("0.20")
    beta = Decimal("0.40")
    rows = [
        {"outcome_key": "alpha", "outcome_order": 0, "sample_block": 1, "close_price": alpha},
        {"outcome_key": "beta", "outcome_order": 1, "sample_block": 1, "close_price": beta},
    ]
    for block, change in enumerate(changes, start=2):
        alpha += change
        beta += change * 2
        rows.extend(
            [
                {"outcome_key": "alpha", "outcome_order": 0, "sample_block": block, "close_price": alpha},
                {"outcome_key": "beta", "outcome_order": 1, "sample_block": block, "close_price": beta},
            ]
        )
    conn = SnapshotConnection(rows)

    report = _load_event_outcome_correlation(
        conn,
        event_snapshot={
            "outcomes": [
                {"outcome_key": "alpha", "outcome_order": 0, "token_yes_id": "yes-alpha"},
                {"outcome_key": "beta", "outcome_order": 1, "token_yes_id": "yes-beta"},
            ]
        },
        from_block=1,
        to_block=10,
    )

    assert report["status"] == "ready"
    assert report["method"] == "block_grid_locf_probability_change_pearson_v1"
    assert report["sample_count"] == 9
    assert report["pair_count"] == 1
    assert report["max_abs_correlation"] == "1"
    assert report["matrix"]["alpha"]["beta"] == "1"
    assert "LEFT JOIN LATERAL" in conn.cursor_obj.sql
    assert "p.block_number >= %s" in conn.cursor_obj.sql
    assert conn.cursor_obj.params[3:] == (1, 10, 1, 10, 1)


def test_event_outcome_correlation_requires_explicit_block_bounds():
    conn = SnapshotConnection([])

    report = _load_event_outcome_correlation(
        conn,
        event_snapshot={
            "outcomes": [
                {"outcome_key": "alpha", "token_yes_id": "yes-alpha"},
                {"outcome_key": "beta", "token_yes_id": "yes-beta"},
            ]
        },
        from_block=None,
        to_block=10,
    )

    assert report["status"] == "review"
    assert report["sample_count"] == 0
    assert conn.cursor_obj.sql == ""
