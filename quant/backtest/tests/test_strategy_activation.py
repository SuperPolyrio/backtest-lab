from quant.backtest.strategy_activation import (
    build_strategy_activation_decision,
    build_strategy_enable_state,
    strategy_activation_decision_to_markdown,
    strategy_enable_state_to_markdown,
)


def artifact_report(gate: dict | None, paper_live_gate: dict | None = None) -> dict:
    report = {
        "status": "ready",
        "run_id": 42,
        "market_slug": "demo-market",
        "token_side": "YES",
        "price_source": "orderfilled_block_close",
        "backtest_engine": "builtin",
        "artifacts": {
            "strategy_name": "fill_first_demo",
            "strategy_version": "v1",
            "data_quality_verdict": "ready",
            "reproducibility_verdict": "ready",
            "materialized_cache_verdict": "ready",
            "shadow_live_triangulation_verdict": "ready",
            "fill_model_suspect": False,
            "settlement_compatibility_verdict": "ready",
        },
    }
    if gate is not None:
        report["promotion_gate_report"] = gate
        report["paper_live_evidence_gate_report"] = paper_live_gate if paper_live_gate is not None else paper_live_gate_for(gate)
    return report


def ready_gate() -> dict:
    return {
        "status": "ready",
        "promotion_verdict": "ready",
        "production_promotion_allowed": True,
        "paper_promotion_allowed": True,
        "allowed_next_modes": ["backtest", "paper", "live"],
        "blocked_reasons": [],
        "review_reasons": [],
        "missing_reasons": [],
    }


def paper_live_gate_for(gate: dict) -> dict:
    return {
        "status": "ready" if gate.get("paper_promotion_allowed") else "blocked",
        "paper_allowed": bool(gate.get("paper_promotion_allowed")),
        "live_allowed": bool(gate.get("production_promotion_allowed")),
        "run_evidence_ready": True,
        "missing_evidence_ready": True,
        "promotion_ready": bool(gate.get("paper_promotion_allowed") or gate.get("production_promotion_allowed")),
        "order_state_coverage_pct": "100",
        "calibration_coverage_pct": "100",
        "missing_order_state_count": 0,
        "missing_calibration_count": 0,
        "blocked_reasons": [],
        "review_reasons": list(gate.get("review_reasons") or []),
    }


def test_strategy_activation_allows_live_when_promotion_gate_is_ready() -> None:
    decision = build_strategy_activation_decision(artifact_report(ready_gate()), target_mode="live")

    assert decision["decision_verdict"] == "ready"
    assert decision["activation_allowed"] is True
    assert decision["target_mode"] == "live"
    assert decision["actual_execution_engine"] == "builtin"
    assert decision["strategy_name"] == "fill_first_demo"
    assert decision["paper_live_evidence_gate_status"] == "ready"
    assert decision["paper_live_live_allowed"] is True


def test_strategy_activation_allows_paper_but_blocks_live_for_review_gate() -> None:
    gate = ready_gate()
    gate.update(
        {
            "promotion_verdict": "review",
            "production_promotion_allowed": False,
            "paper_promotion_allowed": True,
            "allowed_next_modes": ["backtest", "paper"],
            "review_reasons": ["strategy_scope=regime_specific"],
        }
    )

    paper = build_strategy_activation_decision(artifact_report(gate), target_mode="paper")
    live = build_strategy_activation_decision(artifact_report(gate), target_mode="live")

    assert paper["activation_allowed"] is True
    assert paper["decision_verdict"] == "ready"
    assert live["activation_allowed"] is False
    assert live["decision_verdict"] == "blocked"
    assert "live promotion is not allowed by fill-first gate" in live["decision_reasons"]


def test_strategy_activation_blocks_when_promotion_gate_is_missing() -> None:
    decision = build_strategy_activation_decision(artifact_report(None), target_mode="paper")

    assert decision["activation_allowed"] is False
    assert decision["decision_verdict"] == "missing"
    assert "missing promotion_gate_report" in decision["decision_reasons"]


def test_strategy_activation_blocks_paper_when_evidence_gate_is_missing() -> None:
    report = artifact_report(ready_gate())
    report.pop("paper_live_evidence_gate_report")

    decision = build_strategy_activation_decision(report, target_mode="paper")

    assert decision["activation_allowed"] is False
    assert decision["decision_verdict"] == "missing"
    assert "missing paper_live_evidence_gate_report" in decision["missing_reasons"]


def test_strategy_activation_blocks_live_when_evidence_gate_disallows_live() -> None:
    gate = ready_gate()
    paper_live = paper_live_gate_for(gate)
    paper_live["live_allowed"] = False

    decision = build_strategy_activation_decision(artifact_report(gate, paper_live), target_mode="live")

    assert decision["activation_allowed"] is False
    assert decision["decision_verdict"] == "blocked"
    assert "live is not allowed by paper/live evidence gate" in decision["decision_reasons"]


def test_strategy_activation_markdown_includes_gate_and_engine() -> None:
    decision = build_strategy_activation_decision(artifact_report(ready_gate()), target_mode="paper")
    markdown = strategy_activation_decision_to_markdown(decision)

    assert "Strategy Activation Decision" in markdown
    assert "actual_execution_engine: builtin" in markdown
    assert "paper_promotion_allowed: True" in markdown
    assert "paper_live_evidence_gate_status: ready" in markdown


def test_strategy_enable_state_enables_only_allowed_persisted_decision() -> None:
    decision = build_strategy_activation_decision(artifact_report(ready_gate()), target_mode="paper")
    decision["decision_id"] = 11

    state = build_strategy_enable_state(decision, enable=True, requested_by="tester")

    assert state["enabled"] is True
    assert state["enable_status"] == "enabled"
    assert state["decision_id"] == 11
    assert state["target_mode"] == "paper"
    assert state["paper_live_paper_allowed"] is True


def test_strategy_enable_state_blocks_unallowed_enable_request() -> None:
    decision = build_strategy_activation_decision(artifact_report(None), target_mode="paper")
    decision["decision_id"] = 12

    state = build_strategy_enable_state(decision, enable=True)

    assert state["enabled"] is False
    assert state["enable_status"] == "missing"
    assert "activation_allowed=false" in state["enable_reasons"]


def test_strategy_enable_state_blocks_old_decision_without_evidence_gate() -> None:
    decision = build_strategy_activation_decision(artifact_report(ready_gate()), target_mode="paper")
    decision["decision_id"] = 13
    decision.pop("paper_live_evidence_gate_report")
    decision.pop("paper_live_paper_allowed")
    decision.pop("paper_live_live_allowed")
    decision.pop("paper_live_evidence_gate_status")
    decision["artifact_summary"].pop("paper_live_evidence_gate_report")

    state = build_strategy_enable_state(decision, enable=True)

    assert state["enabled"] is False
    assert state["enable_status"] == "missing"
    assert "missing paper_live_evidence_gate_report" in state["enable_reasons"]


def test_strategy_enable_state_allows_disable_even_when_gate_is_blocked() -> None:
    decision = build_strategy_activation_decision(artifact_report(None), target_mode="paper")

    state = build_strategy_enable_state(decision, enable=False)
    markdown = strategy_enable_state_to_markdown(state)

    assert state["enabled"] is False
    assert state["enable_status"] == "disabled"
    assert "Strategy Enable State" in markdown
