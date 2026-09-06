from quant.backtest.order_execution_fixture import (
    order_execution_fixture_pipeline_to_markdown,
    run_order_execution_fixture_pipeline,
)


def test_order_execution_fixture_pipeline_runs_full_local_chain() -> None:
    report = run_order_execution_fixture_pipeline()

    assert report["status"] == "ready"
    assert report["runner_status"] == "ready"
    assert report["guarded_executor_status"] == "dry_run_ready"
    assert report["adapter_status"] == "submitted"
    assert report["runnable_count"] == 1
    assert report["intent_count"] == 1
    assert report["request_count"] == 1
    assert report["response_event_count"] == 1
    assert report["samples_built"] == 1
    assert report["transport_call_count"] == 1
    assert report["calibration_summary"]["trust_status"] == "ready"


def test_order_execution_fixture_pipeline_markdown_summarizes_chain() -> None:
    markdown = order_execution_fixture_pipeline_to_markdown(run_order_execution_fixture_pipeline())

    assert "Order Execution Fixture Pipeline" in markdown
    assert "adapter_status" in markdown
    assert "samples_built: 1" in markdown
