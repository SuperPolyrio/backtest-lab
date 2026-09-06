from quant.backtest.order_state_collection_health import (
    READY,
    REVIEW,
    UNKNOWN,
    collection_health_to_markdown,
    evaluate_collection_state_health,
)


NOW = "2026-06-25T12:00:00Z"


def test_collection_health_reports_unknown_without_state_rows() -> None:
    report = evaluate_collection_state_health([], now=NOW)

    assert report["status"] == UNKNOWN
    assert report["reason"] == "no_collection_state_rows"
    assert report["state_count"] == 0


def test_collection_health_reports_ready_for_fresh_success() -> None:
    report = evaluate_collection_state_health(
        [
            {
                "state_key": "order-api-live",
                "source": "order-api",
                "last_success_at": "2026-06-25T11:59:00Z",
                "updated_at": "2026-06-25T11:59:00Z",
                "last_events_written": 0,
            }
        ],
        now=NOW,
        max_stale_seconds=900,
    )

    assert report["status"] == READY
    assert report["reason"] == "all_ready"
    assert report["items"][0]["reason"] == "ready"


def test_collection_health_reports_review_for_last_error() -> None:
    report = evaluate_collection_state_health(
        [
            {
                "state_key": "order-api-live",
                "source": "order-api",
                "last_success_at": "2026-06-25T11:59:00Z",
                "updated_at": "2026-06-25T11:59:00Z",
                "last_error": "timeout",
            }
        ],
        now=NOW,
    )

    assert report["status"] == REVIEW
    assert report["reason"] == "last_error"
    assert report["error_count"] == 1


def test_collection_health_reports_review_for_stale_success() -> None:
    report = evaluate_collection_state_health(
        [
            {
                "state_key": "order-api-live",
                "source": "order-api",
                "last_success_at": "2026-06-25T11:00:00Z",
                "updated_at": "2026-06-25T11:00:00Z",
            }
        ],
        now=NOW,
        max_stale_seconds=60,
    )

    assert report["status"] == REVIEW
    assert report["reason"] == "stale_success"
    assert report["stale_count"] == 1


def test_collection_health_reports_unknown_when_success_is_required() -> None:
    report = evaluate_collection_state_health(
        [{"state_key": "order-api-live", "source": "order-api", "updated_at": "2026-06-25T11:59:00Z"}],
        now=NOW,
    )

    assert report["status"] == UNKNOWN
    assert report["reason"] == "missing_success"


def test_collection_health_can_require_min_events_written() -> None:
    report = evaluate_collection_state_health(
        [
            {
                "state_key": "order-api-live",
                "source": "order-api",
                "last_success_at": "2026-06-25T11:59:00Z",
                "last_events_written": 0,
            }
        ],
        now=NOW,
        min_events_written=1,
    )

    assert report["status"] == REVIEW
    assert report["reason"] == "below_min_events_written"


def test_collection_health_markdown_contains_state_rows() -> None:
    report = evaluate_collection_state_health(
        [
            {
                "state_key": "order-api-live",
                "source": "order-api",
                "last_success_at": "2026-06-25T11:59:00Z",
            }
        ],
        now=NOW,
    )

    text = collection_health_to_markdown(report)

    assert "status: ready" in text
    assert "order-api-live" in text
