import json

from quant.backtest.external_source_missing_evidence import (
    PLAN_SCHEMA_VERSION,
    build_external_source_missing_evidence_plan,
    external_source_missing_evidence_event_templates_jsonl,
    external_source_missing_evidence_plan_to_markdown,
    write_external_source_missing_evidence_task_pack,
)


def sample_inputs() -> dict:
    return {
        "run": {
            "run_id": 71,
            "market_slug": "demo-market",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "backtest_engine": "builtin",
            "meta": {"token_id": "token-yes"},
        },
        "parameters": {
            "execution_price_mode": "ORDERFILLED_CROSS",
            "execution_profile": "realistic",
            "order_role": "taker",
        },
        "orders": [
            {
                "order_id": "o1",
                "status": "FILLED",
                "side": "BUY_YES",
                "role": "taker",
                "order_type": "LIMIT",
                "signal_x": 10,
                "submit_x": 11,
                "requested_price": "0.40",
                "requested_size": "10",
                "execution_source": "orderfilled_limit_replay_raw",
            },
            {
                "order_id": "o2",
                "status": "NO_FILL",
                "side": "SELL_YES",
                "role": "maker",
                "order_type": "LIMIT",
                "signal_x": 20,
                "submit_x": 21,
                "requested_price": "0.60",
                "requested_size": "5",
                "execution_source": "orderfilled_limit_replay_raw",
            },
            {
                "order_id": "settle-1",
                "status": "FILLED",
                "order_type": "SETTLEMENT",
                "execution_source": "settlement_payoff",
            },
        ],
        "real_order_state_rows": [
            {"order_id": "o1", "payload": {"live_status": "FILLED"}},
        ],
        "calibration_rows": [],
    }


def test_missing_evidence_plan_exports_only_missing_order_state_templates() -> None:
    plan = build_external_source_missing_evidence_plan(sample_inputs(), source="live-shadow")

    assert plan["schema_version"] == PLAN_SCHEMA_VERSION
    assert plan["status"] == "review"
    assert plan["external_order_candidate_count"] == 2
    assert plan["missing_order_state_count"] == 1
    assert plan["missing_calibration_count"] == 2
    assert [row["order_id"] for row in plan["orders_requiring_order_state"]] == ["o2"]
    assert [row["order_id"] for row in plan["orders_requiring_calibration"]] == ["o1", "o2"]

    jsonl = external_source_missing_evidence_event_templates_jsonl(plan)
    rows = [json.loads(line) for line in jsonl.splitlines()]
    assert len(rows) == 1
    assert rows[0]["order_id"] == "o2"
    assert rows[0]["payload"]["simulated_order_id"] == "o2"


def test_missing_evidence_plan_ready_when_evidence_and_calibration_match() -> None:
    inputs = sample_inputs()
    inputs["real_order_state_rows"].append({"order_id": "o2", "payload": {"live_status": "NO_FILL"}})
    inputs["calibration_rows"] = [
        {"simulated_order_id": "o1", "live_status": "FILLED"},
        {"simulated_order_id": "o2", "live_status": "NO_FILL"},
    ]

    plan = build_external_source_missing_evidence_plan(inputs)

    assert plan["status"] == "ready"
    assert plan["missing_order_state_count"] == 0
    assert plan["missing_calibration_count"] == 0
    assert plan["event_templates"] == []


def test_missing_evidence_markdown_contains_commands() -> None:
    markdown = external_source_missing_evidence_plan_to_markdown(
        build_external_source_missing_evidence_plan(sample_inputs())
    )

    assert "Missing External Evidence Plan" in markdown
    assert "Orders Requiring Order-State Evidence" in markdown
    assert "build_calibration_samples" in markdown


def test_missing_evidence_task_pack_writes_actionable_files(tmp_path) -> None:
    plan = build_external_source_missing_evidence_plan(sample_inputs(), source="live-shadow")

    summary = write_external_source_missing_evidence_task_pack(plan, tmp_path)

    assert summary["status"] == "review"
    assert summary["run_id"] == 71
    assert summary["missing_order_state_count"] == 1
    assert summary["missing_calibration_count"] == 2
    assert summary["event_template_count"] == 1

    files = summary["files"]
    plan_json = json.loads((tmp_path / "missing_external_evidence_71.json").read_text(encoding="utf-8"))
    markdown = (tmp_path / "missing_external_evidence_71.md").read_text(encoding="utf-8")
    event_rows = [
        json.loads(line)
        for line in (tmp_path / "missing_external_evidence_71.event_templates.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    commands = (tmp_path / "missing_external_evidence_71.commands.sh").read_text(encoding="utf-8")

    assert files["plan_json"].endswith("missing_external_evidence_71.json")
    assert plan_json["schema_version"] == PLAN_SCHEMA_VERSION
    assert "Orders Requiring Calibration Samples" in markdown
    assert [row["order_id"] for row in event_rows] == ["o2"]
    assert "validate_shadow_live_order_events.py" in commands
