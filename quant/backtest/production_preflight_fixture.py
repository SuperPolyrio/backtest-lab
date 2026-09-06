"""Local fixture for fill-first production preflight checks."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from quant.backtest.configured_external_sources import load_external_source_env_files
from quant.backtest.external_source_env_audit import build_external_source_env_audit
from quant.backtest.external_source_fixture import build_fill_first_external_source_fixture
from quant.backtest.fill_first_production_readiness import build_fill_first_production_readiness_report
from quant.backtest.order_execution_safety import build_order_execution_run_safety_report


READY = "ready"
REVIEW = "review"
FAIL = "fail"


def run_fill_first_production_preflight_fixture(
    output_dir: Path,
    *,
    project_root: Path,
    run_id: int = 9101,
    source_prefix: str = "preflight-fixture",
    overwrite: bool = False,
) -> dict[str, Any]:
    """Generate a local env fixture and run paper-mode production preflight without network calls."""

    fixture = build_fill_first_external_source_fixture(
        output_dir,
        run_id=run_id,
        source_prefix=source_prefix,
        overwrite=overwrite,
    )
    env_file = Path(str(fixture["files"]["env_file"]))
    env = load_external_source_env_files([env_file], base_env={})
    env.update(
        {
            "ORDER_EXECUTION_TARGET_MODE": "paper",
            "ORDER_EXECUTION_SUBMIT_URL": "http://127.0.0.1:9/fill-first-preflight-dry-run",
            "ORDER_EXECUTION_SOURCE": f"{source_prefix}-paper-dry-run",
        }
    )
    artifact_report = _build_fixture_run_artifact_report(run_id=run_id, source_prefix=source_prefix)
    external_env_audit = build_external_source_env_audit([env_file], env=env, project_root=project_root)
    production_readiness = build_fill_first_production_readiness_report(
        target_mode="paper",
        env=env,
        env_files=[env_file],
        project_root=project_root,
        check_db=False,
        run_id=run_id,
        run_artifact_report=artifact_report,
    )
    order_execution_safety = build_order_execution_run_safety_report(
        target_mode="paper",
        execute=False,
        record_events=False,
        submit_url=env["ORDER_EXECUTION_SUBMIT_URL"],
        headers={},
    )
    status = _fixture_status(
        fixture=fixture,
        external_env_audit=external_env_audit,
        production_readiness=production_readiness,
        order_execution_safety=order_execution_safety,
    )
    return {
        "status": status,
        "run_id": run_id,
        "source_prefix": source_prefix,
        "output_dir": str(output_dir),
        "env_file": str(env_file),
        "paper_dry_run_submit_url": env["ORDER_EXECUTION_SUBMIT_URL"],
        "fixture": fixture,
        "run_artifact_report": artifact_report,
        "external_source_env_audit": external_env_audit,
        "production_readiness": production_readiness,
        "order_execution_safety": order_execution_safety,
    }


def production_preflight_fixture_to_markdown(report: Mapping[str, Any]) -> str:
    external = report.get("external_source_env_audit") if isinstance(report.get("external_source_env_audit"), Mapping) else {}
    readiness = report.get("production_readiness") if isinstance(report.get("production_readiness"), Mapping) else {}
    safety = report.get("order_execution_safety") if isinstance(report.get("order_execution_safety"), Mapping) else {}
    gate = (
        readiness.get("paper_live_evidence_gate")
        if isinstance(readiness.get("paper_live_evidence_gate"), Mapping)
        else {}
    )
    lines = [
        f"# Fill-first Production Preflight Fixture: {report.get('status')}",
        "",
        f"- run_id: {report.get('run_id')}",
        f"- source_prefix: {report.get('source_prefix')}",
        f"- env_file: `{report.get('env_file')}`",
        f"- paper_dry_run_submit_url: `{report.get('paper_dry_run_submit_url')}`",
        "",
        "| Check | Status | Detail |",
        "| --- | --- | --- |",
        f"| external source env audit | {external.get('status')} | ready={external.get('ready_count', 0)} review={external.get('review_count', 0)} fail={external.get('fail_count', 0)} |",
        f"| production readiness | {readiness.get('status')} | launch_allowed={readiness.get('launch_allowed')} target={readiness.get('target_mode')} |",
        f"| paper/live evidence gate | {gate.get('status')} | checked={gate.get('checked')} paper_allowed={gate.get('paper_allowed')} live_allowed={gate.get('live_allowed')} |",
        f"| order execution safety | {safety.get('safety_status')} | {safety.get('reason')} |",
    ]
    actions = list(readiness.get("next_actions") or [])
    if actions:
        lines.extend(["", "## Next Actions"])
        lines.extend(f"- {action}" for action in actions)
    return "\n".join(lines)


def _fixture_status(
    *,
    fixture: Mapping[str, Any],
    external_env_audit: Mapping[str, Any],
    production_readiness: Mapping[str, Any],
    order_execution_safety: Mapping[str, Any],
) -> str:
    if fixture.get("status") != READY or external_env_audit.get("status") == FAIL:
        return FAIL
    if external_env_audit.get("status") != READY:
        return REVIEW
    if production_readiness.get("status") != READY:
        return REVIEW
    paper_live_gate = (
        production_readiness.get("paper_live_evidence_gate")
        if isinstance(production_readiness.get("paper_live_evidence_gate"), Mapping)
        else {}
    )
    if paper_live_gate.get("status") != READY or paper_live_gate.get("paper_allowed") is not True:
        return REVIEW
    if order_execution_safety.get("safety_status") != "dry_run_safe":
        return REVIEW
    return READY


def _build_fixture_run_artifact_report(*, run_id: int, source_prefix: str) -> dict[str, Any]:
    return {
        "status": READY,
        "run_id": int(run_id),
        "source": f"{source_prefix}-local-artifact",
        "paper_live_evidence_gate_report": {
            "status": READY,
            "paper_allowed": True,
            "live_allowed": False,
            "run_evidence_ready": True,
            "missing_evidence_ready": True,
            "promotion_ready": True,
            "order_state_coverage_pct": "100",
            "calibration_coverage_pct": "100",
            "missing_order_state_count": 0,
            "missing_calibration_count": 0,
            "blocked_reasons": [],
            "review_reasons": ["fixture is local paper-only evidence; do not treat as live production evidence"],
            "next_actions": ["replace fixture evidence with real run artifact before live execution"],
        },
    }
