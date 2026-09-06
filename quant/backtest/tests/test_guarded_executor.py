from datetime import datetime, timezone

from quant.backtest.guarded_executor import (
    build_guarded_executor_report,
    guarded_executor_report_to_markdown,
)
from quant.backtest.strategy_runner_guard import build_strategy_runner_plan


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


def test_guarded_executor_builds_dry_run_intent_from_runnable_runner_plan() -> None:
    runner_plan = build_strategy_runner_plan([enabled_row()], target_mode="paper")
    report = build_guarded_executor_report(
        runner_plan,
        now=datetime(2026, 6, 22, 1, 2, 3, tzinfo=timezone.utc),
    )

    assert report["status"] == "ready"
    assert report["executor_status"] == "dry_run_ready"
    assert report["record_intent"] is False
    assert report["dry_run"] is True
    assert report["executor_contract"]["lob_required"] is False
    assert report["executor_contract"]["real_order_submission"] is False
    assert report["executor_contract"]["requires_paper_live_evidence_gate"] is True
    assert report["planned_action_count"] == 1

    event = report["event_templates"][0]
    assert event["source"] == "guarded-paper-executor"
    assert event["event_type"] == "submit_intent"
    assert event["submit_status"] == "INTENT_RECORDED"
    assert event["api_order_status"] == "DRY_RUN"
    assert event["clob_order_status"] == "NOT_SUBMITTED"
    assert event["external_order_id"] == "guarded-paper-run-20-decision-10-enable-1"
    assert event["payload"]["planned_action"] == "paper_shadow_submit"
    assert event["payload"]["requires_external_order_adapter"] is True
    assert event["payload"]["lob_required"] is False
    assert event["payload"]["paper_live_evidence_gate_status"] == "ready"
    assert event["payload"]["paper_live_paper_allowed"] is True


def test_guarded_executor_can_mark_intents_for_recording_without_live_submit() -> None:
    runner_plan = build_strategy_runner_plan([enabled_row(target_mode="live", paper_live_live_allowed=True)], target_mode="live")
    report = build_guarded_executor_report(
        runner_plan,
        record_intent=True,
        event_source="custom-live-source",
    )

    assert report["executor_status"] == "intent_ready"
    assert report["record_intent"] is True
    assert report["dry_run"] is False
    assert report["event_templates"][0]["source"] == "custom-live-source"
    assert report["event_templates"][0]["payload"]["real_order_submission"] is False
    assert report["event_templates"][0]["payload"]["planned_action"] == "live_guarded_submit"


def test_guarded_executor_is_idle_without_runner_items() -> None:
    runner_plan = build_strategy_runner_plan([], target_mode="paper")
    report = build_guarded_executor_report(runner_plan)

    assert report["status"] == "review"
    assert report["executor_status"] == "idle"
    assert report["planned_action_count"] == 0
    assert report["event_templates"] == []


def test_guarded_executor_blocks_when_runner_plan_is_blocked() -> None:
    runner_plan = build_strategy_runner_plan(
        [enabled_row(activation_allowed=False, decision_verdict="blocked")],
        target_mode="paper",
        include_blocked=True,
    )
    report = build_guarded_executor_report(runner_plan)

    assert report["status"] == "blocked"
    assert report["executor_status"] == "blocked"
    assert report["planned_action_count"] == 0
    assert report["event_templates"] == []


def test_guarded_executor_markdown_lists_intent_templates() -> None:
    runner_plan = build_strategy_runner_plan([enabled_row()], target_mode="paper")
    report = build_guarded_executor_report(runner_plan)
    markdown = guarded_executor_report_to_markdown(report)

    assert "Guarded Executor" in markdown
    assert "paper_shadow_submit" in markdown
    assert "fill_first_demo@v1" in markdown
