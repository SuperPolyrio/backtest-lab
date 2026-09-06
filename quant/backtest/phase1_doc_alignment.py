"""Executable alignment check for the local Phase 1 fill-first backtest spec."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from quant.backtest.fill_first_readiness import MISSING, READY


@dataclass(frozen=True)
class Phase1Requirement:
    name: str
    detail: str
    evidence: str
    tokens: tuple[str, ...]


PHASE1_REQUIREMENTS: tuple[Phase1Requirement, ...] = (
    Phase1Requirement(
        "orders table lifecycle fields",
        "quant_backtest_orders stores submitted, filled, partial, no-fill and rejected order rows with size/cost/no-fill evidence",
        "quant/core/schema.py",
        (
            "CREATE TABLE IF NOT EXISTS quant.quant_backtest_orders",
            "requested_size",
            "filled_size",
            "unfilled_size",
            "no_fill_reason",
            "latency_blocks",
            "latency_seconds",
            "execution_source",
        ),
    ),
    Phase1Requirement(
        "ledger cashflow table",
        "quant_backtest_ledger stores cash/share deltas, execution costs and per-order trade linkage",
        "quant/core/schema.py",
        (
            "CREATE TABLE IF NOT EXISTS quant.quant_backtest_ledger",
            "cash_delta",
            "shares_delta",
            "fee",
            "rebate",
            "slippage_cost",
            "execution_cost",
        ),
    ),
    Phase1Requirement(
        "orderfilled limit replay default",
        "ORDERFILLED_LIMIT_REPLAY aliases into the default ORDERFILLED_CROSS limit-crossing model",
        "quant/backtest/backtest_engine.py",
        (
            'ORDERFILLED_CROSS_MODE = "ORDERFILLED_CROSS"',
            "ORDERFILLED_LIMIT_REPLAY",
            "normalize_execution_price_mode",
            "buy_limit_price",
            "sell_limit_price",
        ),
    ),
    Phase1Requirement(
        "buy and sell crossing semantics tests",
        "BUY waits for later raw price <= limit and SELL waits for later raw price >= limit, including no-fill cases",
        "quant/backtest/tests/test_limit_replay_raw_events.py",
        (
            "buy_limit_not_crossed",
            "sell_limit_not_crossed",
            "test_limit_replay_latency_blocks_excludes_pre_submit_raw_events",
            "test_fill_quality_report_counts_raw_candidates_and_no_fill_reasons",
        ),
    ),
    Phase1Requirement(
        "no force close without settlement",
        "limit replay separates matched trade exits from final settlement payoff instead of silently using the last price",
        "quant/backtest/backtest_engine.py",
        (
            "settlement_pnl",
            "trade_exit_pnl",
            "exit_reason",
            "settlement",
        ),
    ),
    Phase1Requirement(
        "fill quality report",
        "Fill Quality exposes signal/submitted/filled/partial/no-fill counts, raw evidence, markout and no-fill reasons",
        "quant/backtest/backtest_engine.py",
        (
            "build_fill_quality_report",
            "partial_fill_count",
            "no_fill_reasons",
            "raw_evidence_summary",
            "avg_markout_after_1_bars",
            "missed_opportunity_count",
        ),
    ),
    Phase1Requirement(
        "orders and ledger read api",
        "API can read persisted orders and ledger rows for a run",
        "quant/api/read_api.py",
        (
            "def get_backtest_orders",
            "quant.quant_backtest_orders",
            "def get_backtest_ledger",
            "quant.quant_backtest_ledger",
        ),
    ),
    Phase1Requirement(
        "orders and ledger routes",
        "Flask routes expose /orders and /ledger endpoints for Strategy Tester and audits",
        "scripts/api/routes/quant.py",
        (
            "/backtest-runs/<int:run_id>/orders",
            "get_backtest_orders",
            "/backtest-runs/<int:run_id>/ledger",
            "get_backtest_ledger",
        ),
    ),
    Phase1Requirement(
        "static frontend entrypoint",
        "The local frontend is a single static HTML entrypoint served from / and wired to the quant API.",
        "webpage/quant.html",
        (
            "polyData Quant",
            "Prediction market backtest workspace",
            "/wm-api/quant/price-window",
            "Strategy Tester",
            "Execution Replay",
        ),
    ),
    Phase1Requirement(
        "frontend fill quality display",
        "Strategy Tester displays Fill Quality from persisted orders, including execution status and no-fill/rejection reasons.",
        "webpage/app.js",
        (
            "renderTesterDetail",
            "Execution Quality",
            "No-fill / rejection reasons",
            "state.artifactRows.orders",
        ),
    ),
    Phase1Requirement(
        "sample backtest smoke command",
        "A bounded current-schema sample run can be created with initial_capital=100 and audited immediately",
        "scripts/run_fill_first_sample_backtest.py",
        (
            "--initial-capital",
            "run_fill_first_sample_backtest",
            "artifact_status",
            "execution_price_mode",
            "rows_processed",
        ),
    ),
    Phase1Requirement(
        "current schema artifact fixture",
        "Quality gate has a no-DB current-schema artifact fixture with orders, ledger, fill quality and schema version",
        "quant/backtest/fill_first_quality_gate.py",
        (
            "_current_schema_artifact_fixture_check",
            "BACKTEST_ARTIFACT_SCHEMA_VERSION",
            "fill_quality",
            "ledger",
            "orders",
        ),
    ),
)


def build_phase1_doc_alignment_report(project_root: Path) -> dict[str, Any]:
    """Check whether current code has concrete evidence for the Phase 1 spec."""
    from .repository_scope import OUT_OF_SCOPE, external_owner

    checks: list[dict[str, str]] = []
    for requirement in PHASE1_REQUIREMENTS:
        owner = external_owner(requirement.evidence)
        if owner:
            checks.append({"name": requirement.name, "status": OUT_OF_SCOPE,
                           "detail": f"{requirement.detail}; owned by {owner}, not evaluated here",
                           "evidence": requirement.evidence})
            continue
        path = Path(project_root) / requirement.evidence
        status = READY
        detail = requirement.detail
        missing: list[str] = []
        if not path.exists():
            status = MISSING
            missing = ["file"]
        else:
            text = path.read_text(encoding="utf-8")
            missing = [token for token in requirement.tokens if token not in text]
            if missing:
                status = MISSING
        if missing:
            detail = f"{requirement.detail}; missing tokens: {', '.join(missing[:5])}"
        checks.append(
            {
                "name": requirement.name,
                "status": status,
                "detail": detail,
                "evidence": requirement.evidence,
            }
        )
    missing_count = sum(1 for check in checks if check["status"] == MISSING)
    ready_count = sum(1 for check in checks if check["status"] == READY)
    return {
        "status": READY if missing_count == 0 else MISSING,
        "scope": "docs/量化/phase1.md fill-first acceptance requirements; LOB/DEPTH excluded",
        "ready_count": ready_count,
        "missing_count": missing_count,
        "checks": checks,
        "next_actions": _next_actions(checks),
    }


def phase1_doc_alignment_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Phase 1 Fill-first Doc Alignment: {report.get('status')}",
        "",
        f"Scope: {report.get('scope')}",
        "",
        f"- ready: {report.get('ready_count', 0)}",
        f"- missing: {report.get('missing_count', 0)}",
        "",
        "| Requirement | Status | Detail | Evidence |",
        "| --- | --- | --- | --- |",
    ]
    for check in report.get("checks", []):
        if not isinstance(check, Mapping):
            continue
        lines.append(
            "| {name} | {status} | {detail} | `{evidence}` |".format(
                name=check.get("name", ""),
                status=check.get("status", ""),
                detail=str(check.get("detail", "")).replace("|", "\\|"),
                evidence=check.get("evidence", ""),
            )
        )
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions", []))
    return "\n".join(lines)


def _next_actions(checks: list[dict[str, str]]) -> list[str]:
    actions = [
        f"Implement Phase 1 evidence: {check['name']} ({check['evidence']})"
        for check in checks
        if check["status"] == MISSING
    ]
    if not actions:
        actions.append("Phase 1 fill-first acceptance evidence is present; keep running quality gate after each implementation stage.")
    return actions
