import json

from quant.backtest.external_source_missing_evidence import (
    build_external_source_missing_evidence_plan,
    write_external_source_missing_evidence_task_pack,
)
from quant.backtest.missing_evidence_pipeline import (
    load_missing_external_evidence_task_pack,
    missing_external_evidence_pipeline_to_markdown,
    run_missing_external_evidence_task_pack_pipeline,
)


def sample_inputs() -> dict:
    return {
        "run": {
            "run_id": 171,
            "market_slug": "demo-market",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "backtest_engine": "builtin",
        },
        "parameters": {"execution_price_mode": "ORDERFILLED_CROSS"},
        "orders": [
            {
                "order_id": "missing-order-state",
                "status": "NO_FILL",
                "side": "BUY_YES",
                "role": "maker",
                "order_type": "LIMIT",
                "submit_x": 120,
                "requested_price": "0.40",
                "requested_size": "10",
                "execution_source": "orderfilled_limit_replay_raw",
            }
        ],
        "real_order_state_rows": [],
        "calibration_rows": [],
    }


def test_task_pack_pipeline_rejects_unfilled_templates(tmp_path) -> None:
    plan = build_external_source_missing_evidence_plan(sample_inputs(), source="live-shadow")
    write_external_source_missing_evidence_task_pack(plan, tmp_path)

    report = run_missing_external_evidence_task_pack_pipeline(task_pack_dir=tmp_path)

    assert report["status"] == "fail"
    assert report["reason"] == "validation_failed"
    assert report["validation"]["error_count"] > 0
    assert "Fill or fix" in report["next_actions"][0]


def test_task_pack_pipeline_accepts_filled_event_dry_run(tmp_path) -> None:
    plan = build_external_source_missing_evidence_plan(sample_inputs(), source="live-shadow")
    write_external_source_missing_evidence_task_pack(plan, tmp_path)
    loaded = load_missing_external_evidence_task_pack(tmp_path)
    event = loaded["events"][0]
    event["event_time"] = "2026-06-22T12:00:00Z"
    event["external_order_id"] = "live-order-1"
    event["api_order_status"] = "CANCELED"
    event["payload"]["live_status"] = "CANCELED"
    filled_path = tmp_path / "filled_events.jsonl"
    filled_path.write_text(json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8")

    report = run_missing_external_evidence_task_pack_pipeline(
        task_pack_dir=tmp_path,
        events_path=filled_path,
    )
    markdown = missing_external_evidence_pipeline_to_markdown(report)

    assert report["status"] == "ready"
    assert report["reason"] == "dry_run_validation_complete"
    assert report["validation"]["calibration_ready_count"] == 1
    assert report["write"] is False
    assert "Missing Evidence Task Pack Pipeline" in markdown
    assert "Re-run with --write" in markdown
