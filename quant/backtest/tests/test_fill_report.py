from quant.backtest.fill_report import (
    MISSING,
    REVIEW,
    backtest_fill_report_to_markdown,
    build_backtest_fill_report,
)
from quant.backtest.tests.test_run_artifacts import complete_inputs


def test_fill_report_summarizes_no_fill_and_evidence() -> None:
    inputs = complete_inputs()
    inputs["calibration_count"] = 0
    inputs["cost_calibration_count"] = 0
    report = build_backtest_fill_report(inputs, run_id=7)

    assert report["status"] == REVIEW
    assert report["summary"]["market_slug"] == "demo-market"
    assert report["summary"]["requested_engine"] == "builtin"
    assert report["fill_quality"]["submitted_count"] == 2
    assert report["fill_quality"]["fill_rate"] == "50%"
    assert report["no_fill"]["reasons"] == {"NO_LIQUIDITY": 1}
    assert report["evidence"]["raw_event_count"] == 10
    assert report["evidence"]["candidate_event_unique_count"] == 3
    assert report["evidence"]["raw_orderfilled_fill_count"] == 1
    assert report["evidence"]["block_bar_synthetic_fill_count"] == 0
    assert report["evidence"]["block_participation_discount_tick_count"] == 0
    assert report["evidence"]["min_block_participation_factor"] == "1"
    assert report["evidence"]["raw_replay_coverage_pct"] == "100"
    assert report["evidence"]["block_bar_fallback_pct"] == "0"
    assert report["diagnostics"]["verdict"] == REVIEW
    assert "missing live-vs-sim calibration samples" in report["diagnostics"]["reasons"]


def test_fill_report_lists_missed_orders_by_missed_notional() -> None:
    inputs = complete_inputs()
    inputs["orders"].append(
        {
            "order_id": "o3",
            "status": "NO_FILL",
            "side": "SELL_YES",
            "role": "maker",
            "submit_x": 30,
            "requested_notional": "20",
            "actual_fill_notional": "0",
            "no_fill_reason": "post_only_would_take",
            "meta": {
                "fill_schedule": [
                    {
                        "market_id": 42,
                        "token_id": "token-yes",
                        "block_number": 101,
                        "fillable_size": "0.25",
                        "side_compatibility": "incompatible_discounted",
                        "side_compatibility_factor": "0.4",
                        "requested_block_participation_pct": "500",
                        "block_participation_factor": "0.5",
                        "block_buy_volume": "4",
                        "block_sell_volume": "6",
                        "block_unknown_side_volume": "0",
                    },
                    {
                        "market_id": 42,
                        "token_id": "token-yes",
                        "block_number": 101,
                        "fillable_size": "0.50",
                        "side_compatibility": "compatible",
                        "side_compatibility_factor": "1",
                        "requested_block_participation_pct": "500",
                        "block_participation_factor": "0.5",
                        "block_buy_volume": "4",
                        "block_sell_volume": "6",
                        "block_unknown_side_volume": "0",
                    },
                ],
            },
        }
    )

    report = build_backtest_fill_report(inputs, run_id=7, max_orders=2)

    assert [row["order_id"] for row in report["missed_orders"]] == ["o3", "o2"]
    assert report["missed_orders"][0]["missed_notional"] == "20"
    assert report["missed_orders"][0]["no_fill_reason"] == "post_only_would_take"
    assert report["missed_orders"][0]["fill_schedule_tick_count"] == 2
    assert report["missed_orders"][0]["fill_schedule_fillable_size"] == "0.75"
    assert report["missed_orders"][0]["side_discounted_tick_count"] == 1
    assert report["missed_orders"][0]["block_participation_discount_tick_count"] == 2
    assert report["missed_orders"][0]["max_requested_block_participation_pct"] == "500"
    assert report["missed_orders"][0]["min_block_participation_factor"] == "0.5"
    assert report["missed_orders"][0]["block_buy_volume"] == "4"
    assert report["missed_orders"][0]["block_sell_volume"] == "6"
    markdown = backtest_fill_report_to_markdown(report)
    assert "| o3 | SELL_YES | maker | 30 | post_only_would_take | 20 | 20 | 2 | 1 | 2 | 4/6/0 |" in markdown


def test_fill_report_marks_missing_without_fill_quality() -> None:
    inputs = complete_inputs()
    inputs["run"]["meta"]["actual_data_quality"].pop("fill_quality")

    report = build_backtest_fill_report(inputs, run_id=7)

    assert report["status"] == MISSING
    assert "missing fill_quality artifact" in report["diagnostics"]["reasons"]
    assert report["next_actions"] == ["Rebuild or repair fill_quality for this run before using it in research."]


def test_fill_report_markdown_contains_actionable_sections() -> None:
    inputs = complete_inputs()
    inputs["calibration_count"] = 0
    inputs["cost_calibration_count"] = 0
    report = build_backtest_fill_report(inputs, run_id=7)
    markdown = backtest_fill_report_to_markdown(report)

    assert "# Fill Report: review" in markdown
    assert "## No Fill" in markdown
    assert "| NO_LIQUIDITY | 1 |" in markdown
    assert "## Evidence" in markdown
    assert "raw_event_count: 10" in markdown
    assert "raw_replay_coverage_pct: 100%" in markdown
