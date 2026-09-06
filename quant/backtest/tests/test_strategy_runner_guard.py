from quant.backtest.strategy_runner_guard import (
    build_strategy_runner_plan,
    strategy_runner_plan_to_markdown,
)


def enabled_row(**overrides):
    row = {
        "enable_id": 1,
        "decision_id": 10,
        "run_id": 20,
        "target_mode": "paper",
        "strategy_name": "fill_first_demo",
        "strategy_version": "v1",
        "market_slug": "demo-market",
        "token_side": "YES",
        "price_source": "orderfilled_block_close",
        "actual_execution_engine": "builtin",
        "enabled": True,
        "activation_allowed": True,
        "decision_verdict": "ready",
        "promotion_verdict": "ready",
        "paper_live_evidence_gate_status": "ready",
        "paper_live_paper_allowed": True,
        "paper_live_live_allowed": False,
        "activation_decision": {
            "decision_id": 10,
            "paper_live_evidence_gate_report": {
                "status": "ready",
                "paper_allowed": True,
                "live_allowed": False,
            },
        },
    }
    row.update(overrides)
    return row


def test_runner_plan_uses_enabled_strategy_state_as_only_source() -> None:
    plan = build_strategy_runner_plan([enabled_row()], target_mode="paper")

    assert plan["status"] == "ready"
    assert plan["runner_status"] == "ready"
    assert plan["runnable_count"] == 1
    assert plan["runner_contract"]["read_source"] == "quant.strategy_enable_state"
    assert plan["runner_contract"]["order_state_sink"] == "quant.real_order_state_events"
    assert plan["runner_contract"]["lob_required"] is False
    assert plan["runner_contract"]["requires_paper_live_evidence_gate"] is True
    assert plan["items"][0]["planned_action"] == "paper_shadow_submit"
    assert plan["items"][0]["paper_live_evidence_gate_status"] == "ready"


def test_runner_plan_is_idle_without_enabled_state_rows() -> None:
    plan = build_strategy_runner_plan([], target_mode="paper")

    assert plan["status"] == "review"
    assert plan["runner_status"] == "idle"
    assert plan["runnable_count"] == 0
    assert "no enabled" in plan["reason"]


def test_runner_plan_blocks_bad_enabled_state_rows() -> None:
    plan = build_strategy_runner_plan(
        [enabled_row(activation_allowed=False, decision_verdict="blocked")],
        target_mode="paper",
        include_blocked=True,
    )

    assert plan["status"] == "blocked"
    assert plan["runnable_count"] == 0
    assert plan["blocked_count"] == 1
    assert "activation_allowed=false" in plan["items"][0]["reason"]
    assert "decision_verdict=blocked" in plan["items"][0]["reason"]


def test_runner_plan_blocks_enabled_row_without_paper_live_gate() -> None:
    plan = build_strategy_runner_plan(
        [
            enabled_row(
                paper_live_evidence_gate_status="missing",
                paper_live_paper_allowed=False,
                activation_decision={"decision_id": 10},
            )
        ],
        target_mode="paper",
        include_blocked=True,
    )

    assert plan["status"] == "blocked"
    assert plan["runnable_count"] == 0
    assert "missing paper_live_evidence_gate_report" in plan["items"][0]["reason"]
    assert "paper_live_paper_allowed=false" in plan["items"][0]["reason"]


def test_runner_plan_markdown_lists_runnable_items() -> None:
    plan = build_strategy_runner_plan([enabled_row(target_mode="live", paper_live_live_allowed=True)], target_mode="live")
    markdown = strategy_runner_plan_to_markdown(plan)

    assert "Strategy Runner Plan" in markdown
    assert "fill_first_demo@v1" in markdown
    assert "live_guarded_submit" not in markdown
