from quant.backtest.external_source_run_coverage import (
    READY,
    REVIEW,
    build_external_source_run_coverage_report,
    external_source_run_coverage_to_markdown,
)


def test_external_source_run_coverage_ready_when_orders_and_calibration_match() -> None:
    report = build_external_source_run_coverage_report(
        {
            "run": {"run_id": 7, "market_slug": "market-a", "token_side": "YES"},
            "orders": [
                {"order_id": "o1", "status": "FILLED", "execution_source": "orderfilled_limit_replay_raw"},
                {"order_id": "o2", "status": "NO_FILL", "execution_source": "orderfilled_limit_replay_raw"},
                {"order_id": "o3", "order_type": "SETTLEMENT", "execution_source": "settlement_payoff"},
            ],
            "real_order_events": [
                {"order_id": "o1", "payload": {"live_status": "FILLED"}},
                {"order_id": "o2", "payload": {"live_status": "NO_FILL"}},
            ],
            "calibration_rows": [
                {"simulated_order_id": "o1", "live_status": "FILLED"},
                {"simulated_order_id": "o2", "live_status": "NO_FILL"},
            ],
            "real_cost_events": [],
            "cost_calibration_rows": [],
            "platform_incidents": [{"incident_key": "i1"}],
            "external_states": [{"state_key": "orders-live"}],
        }
    )

    assert report["status"] == READY
    assert report["external_order_candidate_count"] == 2
    assert report["order_state_coverage_pct"] == "100"
    assert report["calibration_coverage_pct"] == "100"


def test_external_source_run_coverage_reviews_missing_calibration_and_state() -> None:
    report = build_external_source_run_coverage_report(
        {
            "run": {"run_id": 8, "market_slug": "market-b", "token_side": "YES"},
            "orders": [{"order_id": "o1", "status": "FILLED"}],
            "real_order_events": [],
            "calibration_rows": [],
            "real_cost_events": [],
            "cost_calibration_rows": [],
            "platform_incidents": [],
            "external_states": [],
        }
    )

    assert report["status"] == REVIEW
    assert report["missing_order_state_order_ids"] == ["o1"]
    assert report["missing_calibration_order_ids"] == ["o1"]
    assert any(check["name"] == "external source state" and check["status"] == REVIEW for check in report["checks"])


def test_external_source_run_coverage_markdown_contains_next_actions() -> None:
    markdown = external_source_run_coverage_to_markdown(
        build_external_source_run_coverage_report(
            {
                "run": {"run_id": 9},
                "orders": [{"order_id": "o1"}],
                "real_order_events": [],
                "calibration_rows": [],
                "real_cost_events": [],
                "cost_calibration_rows": [],
                "platform_incidents": [],
                "external_states": [],
            }
        )
    )

    assert "External Source Run Coverage" in markdown
    assert "Next Actions" in markdown


def test_external_source_run_coverage_does_not_treat_slippage_as_wallet_cost() -> None:
    report = build_external_source_run_coverage_report(
        {
            "run": {"run_id": 10},
            "orders": [{"order_id": "o1", "slippage_cost": "0.05", "fee_cost": "0", "rebate_cost": "0"}],
            "real_order_events": [{"order_id": "o1"}],
            "calibration_rows": [{"simulated_order_id": "o1"}],
            "real_cost_events": [],
            "cost_calibration_rows": [],
            "platform_incidents": [{"incident_key": "i1"}],
            "external_states": [{"state_key": "orders-live"}],
        }
    )

    cost = next(check for check in report["checks"] if check["name"] == "cost evidence coverage")
    assert report["expected_cost_order_count"] == 0
    assert cost["status"] == READY
