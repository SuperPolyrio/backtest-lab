from quant.backtest.external_source_state import (
    READY,
    REVIEW,
    UNKNOWN,
    evaluate_external_source_import_health,
    external_source_import_health_to_markdown,
)


NOW = "2026-06-25T12:00:00Z"


def test_external_source_health_reports_unknown_without_rows() -> None:
    report = evaluate_external_source_import_health([], now=NOW)

    assert report["status"] == UNKNOWN
    assert report["reason"] == "no_import_state_rows"


def test_external_source_health_reports_ready_for_fresh_import() -> None:
    report = evaluate_external_source_import_health(
        [
            {
                "state_key": "real-cost-events-live",
                "source_type": "real_cost_events",
                "source": "wallet-ledger",
                "last_success_at": "2026-06-25T11:59:00Z",
                "updated_at": "2026-06-25T11:59:00Z",
                "last_rows_written": 3,
            }
        ],
        now=NOW,
        max_stale_seconds=900,
    )

    assert report["status"] == READY
    assert report["reason"] == "all_ready"


def test_external_source_health_reports_review_for_stale_or_error() -> None:
    stale = evaluate_external_source_import_health(
        [
            {
                "state_key": "platform-incidents-live",
                "source_type": "platform_incidents",
                "last_success_at": "2026-06-25T11:00:00Z",
                "updated_at": "2026-06-25T11:00:00Z",
                "last_rows_written": 1,
            }
        ],
        now=NOW,
        max_stale_seconds=60,
    )
    error = evaluate_external_source_import_health(
        [
            {
                "state_key": "real-cost-events-live",
                "source_type": "real_cost_events",
                "last_success_at": "2026-06-25T11:59:00Z",
                "last_error": "timeout",
            }
        ],
        now=NOW,
    )

    assert stale["status"] == REVIEW
    assert stale["reason"] == "stale_success"
    assert error["status"] == REVIEW
    assert error["reason"] == "last_error"


def test_external_source_health_markdown_contains_import_rows() -> None:
    report = evaluate_external_source_import_health(
        [
            {
                "state_key": "real-cost-events-live",
                "source_type": "real_cost_events",
                "source": "wallet-ledger",
                "last_success_at": "2026-06-25T11:59:00Z",
                "last_rows_written": 3,
            }
        ],
        now=NOW,
    )

    markdown = external_source_import_health_to_markdown(report)

    assert "status: ready" in markdown
    assert "real-cost-events-live" in markdown
