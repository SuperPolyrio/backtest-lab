"""Executable alignment check for the L2 + OrderFilled execution guidance."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from quant.backtest.fill_first_readiness import MISSING, READY


@dataclass(frozen=True)
class L2ExecutionRequirement:
    name: str
    detail: str
    evidence: str
    tokens: tuple[str, ...]


L2_EXECUTION_REQUIREMENTS: tuple[L2ExecutionRequirement, ...] = (
    L2ExecutionRequirement(
        "decimal event types",
        "Core L2, FillTick, strategy intent, resting order and execution fill types exist with stable event ids.",
        "quant/backtest/l2_orderfilled_execution.py",
        (
            "class BookLevel",
            "class BookSnapshot",
            "class BookDelta",
            "class FillTick",
            "class StrategyOrderIntent",
            "class RestingOrder",
            "class ExecutionFill",
            "event_id",
        ),
    ),
    L2ExecutionRequirement(
        "orderfilled normalizer",
        "Raw OrderFilled rows are mapped into maker-level FillTick evidence with side, price, size and quarantine handling.",
        "quant/backtest/l2_orderfilled_execution.py",
        (
            "normalize_orderfilled_event",
            "makerAssetId",
            "takerAssetId",
            "passive_side",
            "aggressor_side",
            "price_out_of_bounds",
            "unsupported_asset_path",
            "_canonical_orderfilled_key",
        ),
    ),
    L2ExecutionRequirement(
        "book state residual depth",
        "BookState applies snapshots/deltas, tracks stale/gap quality and prevents double-counting visible depth.",
        "quant/backtest/l2_orderfilled_execution.py",
        (
            "class BookState",
            "apply_snapshot",
            "apply_delta",
            "best_bid",
            "best_ask",
            "residual_consume",
            "quality_at",
            "gap_since_last_snapshot",
        ),
    ),
    L2ExecutionRequirement(
        "taker matching",
        "Aggressive limit orders walk visible L2 depth with TIF, post-only, residual depth, fee and optional impact semantics.",
        "quant/backtest/l2_orderfilled_execution.py",
        (
            "execute_taker",
            "fok_insufficient_liquidity",
            "post_only_crosses_book",
            "unfilled_remainder_cancelled",
            "depth_haircut",
            "impact_strength_bps",
            "_apply_adverse_impact",
        ),
    ),
    L2ExecutionRequirement(
        "maker queue",
        "Resting maker orders use LevelQueue, env_ahead, agent FIFO, OrderFilled queue consumption and LOB reconciliation.",
        "quant/backtest/l2_orderfilled_execution.py",
        (
            "class LevelQueue",
            "env_ahead",
            "agent_orders",
            "place_order",
            "cancel_order",
            "process_fill_tick",
            "update_env_from_lob_change",
            "use_lob_decrease_for_queue",
        ),
    ),
    L2ExecutionRequirement(
        "matching engine timeline",
        "Book, FillTick, strategy order and cancel events merge into a deterministic latency-aware execution timeline.",
        "quant/backtest/l2_orderfilled_execution.py",
        (
            "class StrategyCancelIntent",
            "class ExecutionTimelineResult",
            "process_event",
            "run_event_timeline",
            "_event_effective_ts",
            "_event_priority",
            "non_monotonic_events_sorted",
        ),
    ),
    L2ExecutionRequirement(
        "execution audit",
        "Every order result exposes submitted/received times, source evidence, book quality, queue state, mode and invariant violations.",
        "quant/backtest/l2_orderfilled_execution.py",
        (
            "submitted_at",
            "venue_received_at",
            "book_snapshot_id",
            "queue_ahead_at_admit",
            "book_quality",
            "invariant_violations",
            "l2_order_result_invariant_violations",
        ),
    ),
    L2ExecutionRequirement(
        "profile matrix reporting",
        "L2 results expose conservative, realistic and optimistic execution configuration for sensitivity reporting.",
        "quant/backtest/l2_orderfilled_execution.py",
        (
            "l2_execution_config_matrix",
            "conservative",
            "realistic",
            "optimistic",
            "execution_profile_config_matrix",
        ),
    ),
    L2ExecutionRequirement(
        "replay consistency report",
        "PMXT/L2 and OrderFilled replay alignment is summarized with coverage, book age, unexplained orderfilled and unexplained book decrease metrics.",
        "quant/backtest/l2_replay_consistency.py",
        (
            "build_l2_replay_consistency_report",
            "coverage_ratio",
            "book_age_distribution",
            "orderfilled_matched_to_book_ratio",
            "unexplained_book_decrease_ratio",
            "unexplained_orderfilled_ratio",
            "l2_replay_consistency_to_markdown",
        ),
    ),
    L2ExecutionRequirement(
        "shadow live calibration report",
        "Paper/live paired orders calibrate taker price/size, maker fill probability, time-to-fill, Brier score and execution profile parameters.",
        "quant/backtest/l2_shadow_calibration.py",
        (
            "build_l2_shadow_calibration_report",
            "predicted_fill_price",
            "actual_fill_price",
            "brier_score",
            "calibration_curve",
            "queue_ahead_fraction",
            "cancel_ahead_fraction",
            "depth_haircut",
            "book_ttl_ms",
            "impact_bps",
        ),
    ),
    L2ExecutionRequirement(
        "execution model validation report",
        "The same strategy can be compared across OHLCV, formula slippage and L2 + OrderFilled execution, with regime stress, capacity curve and strategy-level attribution.",
        "quant/backtest/execution_model_validation.py",
        (
            "build_execution_model_validation_report",
            "ohlcv_close",
            "formula_slippage",
            "l2_orderfilled",
            "l2_profile_sensitivity",
            "capacity_curve",
            "strategy_attribution",
            "regime_stress",
            "monotonic_fill_rate_ok",
        ),
    ),
    L2ExecutionRequirement(
        "static frontend execution source",
        "The frontend no longer exposes legacy React execution-mode components; the static entrypoint shows the current OrderFilled price source.",
        "webpage/quant.html",
        (
            "OrderFilled block-close",
            "/wm-api/quant/price-window",
        ),
    ),
    L2ExecutionRequirement(
        "unit test coverage",
        "L2 execution tests cover taker, maker queue, timeline, audit, profile matrix, impact and no-book rejection.",
        "quant/backtest/tests/test_l2_orderfilled_execution.py",
        (
            "test_taker_buy_walks_asks_and_respects_limit_price",
            "test_maker_queue_requires_orderfilled_to_clear_queue_ahead_before_fill",
            "test_timeline_maker_fills_only_after_orderfilled_tick_consumes_queue",
            "test_depth_adapter_reports_execution_config_matrix_and_capacity_fields",
            "test_taker_adverse_impact_is_optional_and_reported_in_fill_price",
            "test_depth_adapter_rejected_no_book_still_reports_execution_config",
        ),
    ),
)


FORBIDDEN_TOKENS: tuple[tuple[str, str, str], ...] = (
    ("legacy volume option", "webpage/quant.html", '<option value=' + '"LEGACY">'),
    ("legacy fill helper", "quant/backtest/backtest_engine.py", "def " + "_legacy_fill_decision"),
    ("legacy framework helper", "quant/backtest/frameworks.py", "def " + "_legacy_fill_decision"),
    ("old depth simulator", "quant/backtest/execution.py", "def " + "simulate_depth_fill"),
    ("old lob model import", "quant/backtest/backtest_engine.py", "fill" + "_lob_model"),
)


def build_l2_execution_doc_alignment_report(project_root: Path) -> dict[str, Any]:
    from .repository_scope import OUT_OF_SCOPE, external_owner

    checks: list[dict[str, Any]] = []
    for requirement in L2_EXECUTION_REQUIREMENTS:
        owner = external_owner(requirement.evidence)
        if owner:
            checks.append({"name": requirement.name, "status": OUT_OF_SCOPE,
                           "detail": f"Owned by {owner}, not evaluated here",
                           "evidence": requirement.evidence})
            continue
        checks.append(_check_required_tokens(project_root, requirement))
    legacy_checks: list[dict[str, Any]] = []
    for name, evidence, token in FORBIDDEN_TOKENS:
        if external_owner(evidence):
            continue
        legacy_checks.append(_check_forbidden_token(project_root, name, evidence, token))
    checks.extend(legacy_checks)
    legacy_missing = [check["name"] for check in legacy_checks if check["status"] == MISSING]
    checks.append(
        {
            "name": "legacy execution removed",
            "status": MISSING if legacy_missing else READY,
            "detail": (
                f"Legacy execution paths remain: {', '.join(legacy_missing)}"
                if legacy_missing
                else "All forbidden legacy execution helpers and frontend modes are absent."
            ),
            "evidence": "quant/backtest + webpage/quant.html",
        }
    )
    missing_count = sum(1 for check in checks if check["status"] == MISSING)
    ready_count = sum(1 for check in checks if check["status"] == READY)
    return {
        "status": READY if missing_count == 0 else MISSING,
        "scope": "docs/量化/成交模型/polymarket_execution_model_guidance_for_codex.md L2 + OrderFilled execution requirements",
        "ready_count": ready_count,
        "missing_count": missing_count,
        "checks": checks,
        "next_actions": _next_actions(checks),
    }


def l2_execution_doc_alignment_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# L2 + OrderFilled Execution Doc Alignment: {report.get('status')}",
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


def _check_required_tokens(project_root: Path, requirement: L2ExecutionRequirement) -> dict[str, Any]:
    path = project_root / requirement.evidence
    missing: list[str] = []
    if not path.exists():
        missing = ["file"]
    else:
        text = path.read_text(encoding="utf-8")
        missing = [token for token in requirement.tokens if token not in text]
    status = MISSING if missing else READY
    detail = requirement.detail if not missing else f"{requirement.detail}; missing tokens: {', '.join(missing[:5])}"
    return {"name": requirement.name, "status": status, "detail": detail, "evidence": requirement.evidence}


def _check_forbidden_token(project_root: Path, name: str, evidence: str, token: str) -> dict[str, Any]:
    path = project_root / evidence
    if not path.exists():
        return {"name": name, "status": READY, "detail": "Forbidden legacy token absent because evidence file is absent.", "evidence": evidence}
    text = path.read_text(encoding="utf-8")
    if token in text:
        return {"name": name, "status": MISSING, "detail": f"Forbidden legacy token still present: {token}", "evidence": evidence}
    return {"name": name, "status": READY, "detail": "Forbidden legacy token absent.", "evidence": evidence}


def _next_actions(checks: list[dict[str, Any]]) -> list[str]:
    actions = [
        f"Implement L2 execution evidence: {check['name']} ({check['evidence']})"
        for check in checks
        if check["status"] == MISSING
    ]
    if not actions:
        actions.append("L2 + OrderFilled execution guidance evidence is present; keep running unit, backend, and frontend checks after each change.")
    return actions
