from quant.backtest.shadow_live_pipeline import (
    ShadowLivePipelineOptions,
    run_shadow_live_calibration_pipeline,
    shadow_live_pipeline_report_to_markdown,
)


def sample_order() -> dict:
    return {
        "run_id": 42,
        "order_id": "O-1",
        "run_market_slug": "market-a",
        "run_token_side": "YES",
        "side": "BUY_YES",
        "role": "taker",
        "order_type": "marketable_limit",
        "requested_price": "0.53",
        "requested_size": "10",
        "status": "FILLED",
        "avg_fill_price": "0.53",
        "filled_size": "10",
        "filled_notional": "5.30",
        "fee_cost": "0.01",
        "rebate_cost": "0",
        "slippage_cost": "0",
        "latency_seconds": "1.2",
        "meta": {"external_order_id": "L-1", "token_id": "token-yes"},
    }


def sample_event() -> dict:
    return {
        "run_id": 42,
        "order_id": "O-1",
        "external_order_id": "L-1",
        "event_time": "2026-06-25T12:00:02Z",
        "source": "live-shadow",
        "api_order_status": "filled",
        "payload": {
            "live_status": "FILLED",
            "live_fill_price": "0.53",
            "live_fill_size": "10",
            "live_slippage": "0",
            "live_fee": "0.01",
            "live_rebate": "0",
            "live_cash_delta": "-5.31",
            "live_position_delta": "10",
            "live_latency_seconds": "1.2",
        },
    }


def test_shadow_live_pipeline_builds_ready_report_without_db_writes() -> None:
    report = run_shadow_live_calibration_pipeline(
        orders=[sample_order()],
        raw_events=[sample_event()],
        options=ShadowLivePipelineOptions(run_id=42, dry_run=True),
    )

    assert report["status"] == "ready"
    assert report["events_written"] == 0
    assert report["samples_built"] == 1
    assert report["calibration_report"]["trust_status"] == "ready"
    assert report["periodic_report"]["sample_count"] == 1
    assert report["l2_shadow_calibration_report"]["status"] == "ready"
    assert report["l2_shadow_calibration_report"]["taker"]["classification_accuracy_pct"] == "100"


def test_shadow_live_pipeline_fails_before_import_when_validation_fails() -> None:
    event = sample_event()
    event["event_time"] = ""
    event["payload"] = {"live_status": ""}

    report = run_shadow_live_calibration_pipeline(
        orders=[sample_order()],
        raw_events=[event],
        options=ShadowLivePipelineOptions(run_id=42, dry_run=True),
    )

    assert report["status"] == "fail"
    assert report["samples_built"] == 0
    assert report["validation"]["error_count"] > 0


def test_shadow_live_pipeline_markdown_contains_summary() -> None:
    report = run_shadow_live_calibration_pipeline(
        orders=[sample_order()],
        raw_events=[sample_event()],
        options=ShadowLivePipelineOptions(run_id=42, dry_run=True),
    )
    markdown = shadow_live_pipeline_report_to_markdown(report)

    assert "Shadow/Live Calibration Pipeline" in markdown
    assert "calibration_trust" in markdown
    assert "l2_shadow_calibration" in markdown
    assert "samples_built" in markdown
