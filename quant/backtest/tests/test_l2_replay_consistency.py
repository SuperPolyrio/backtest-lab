from quant.backtest.l2_replay_consistency import (
    MISSING,
    READY,
    REVIEW,
    build_l2_replay_consistency_report,
    l2_replay_consistency_to_markdown,
)


def _alignment(**overrides):
    base = {
        "status": "ready",
        "orderfilled_rows_matched": 10,
        "aligned_count": 10,
        "price_outside_spread_count": 0,
        "missing_timestamp_count": 0,
        "missing_l2_before_fill_count": 0,
        "stale_l2_count": 0,
        "depth_checked_count": 10,
        "depth_sufficient_count": 9,
        "crossable_depth_sufficient_count": 8,
        "sample_rows": [
            {"classification": "price_within_spread_or_touch", "lag_ms": 10},
            {"classification": "price_within_spread_or_touch", "lag_ms": 20},
            {"classification": "price_within_spread_or_touch", "lag_ms": 50},
        ],
    }
    base.update(overrides)
    return base


def test_l2_replay_consistency_ready_when_l2_explains_orderfilled() -> None:
    report = build_l2_replay_consistency_report(
        _alignment(),
        book_decrease_rows=[
            {"old_size": "100", "new_size": "70", "orderfilled_size": "25"},
            {"old_size": "50", "new_size": "45", "orderfilled_size": "5"},
        ],
    )

    assert report["status"] == READY
    assert report["coverage_ratio"] == "100"
    assert report["orderfilled_matched_to_book_ratio"] == "100"
    assert report["price_compatible_ratio"] == "100"
    assert report["depth_sufficient_ratio"] == "90"
    assert report["crossable_depth_sufficient_ratio"] == "80"
    assert report["unexplained_book_decrease_ratio"] == "14.2857"
    assert report["book_age_distribution"]["p95_ms"] == 50
    assert report["review_reasons"] == []


def test_l2_replay_consistency_reviews_unexplained_orderfilled_and_stale_l2() -> None:
    report = build_l2_replay_consistency_report(
        _alignment(
            aligned_count=7,
            price_outside_spread_count=1,
            stale_l2_count=2,
            sample_rows=[{"classification": "stale_l2", "lag_ms": 65_000}],
        )
    )

    assert report["status"] == REVIEW
    assert report["coverage_ratio"] == "70"
    assert report["unexplained_orderfilled_ratio"] == "40"
    assert "stale_l2_before_fill" in report["review_reasons"]
    assert "orderfilled_price_outside_spread" in report["review_reasons"]
    assert "unexplained_orderfilled_ratio_high" in report["review_reasons"]


def test_l2_replay_consistency_reviews_unexplained_book_decrease() -> None:
    report = build_l2_replay_consistency_report(
        _alignment(),
        book_decrease_rows=[{"old_size": "100", "new_size": "20", "orderfilled_size": "10"}],
    )

    assert report["status"] == REVIEW
    assert report["unexplained_book_decrease_ratio"] == "87.5"
    assert "unexplained_book_decrease_ratio_high" in report["review_reasons"]


def test_l2_replay_consistency_missing_without_orderfilled_rows() -> None:
    report = build_l2_replay_consistency_report(_alignment(orderfilled_rows_matched=0, aligned_count=0))

    assert report["status"] == MISSING
    assert "no_orderfilled_rows" in report["review_reasons"]
    assert "no_aligned_l2_book_state" in report["review_reasons"]


def test_l2_replay_consistency_markdown_contains_key_metrics() -> None:
    report = build_l2_replay_consistency_report(_alignment())

    markdown = l2_replay_consistency_to_markdown(report)

    assert "# L2 Replay Consistency: ready" in markdown
    assert "coverage_ratio: 100" in markdown
    assert "book_age_ms_p95: 50" in markdown
