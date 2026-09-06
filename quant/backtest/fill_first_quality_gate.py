"""Project-level quality gate for the fill-first backtest stack."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from quant.backtest.external_source_discovery import (
    default_external_source_roots,
    discover_external_source_files,
    preview_external_source_import,
)
from quant.backtest.configured_external_sources import build_configured_external_source_report, load_external_source_env_files
from quant.backtest.external_source_env_audit import build_external_source_env_audit
from quant.backtest.external_source_env_bootstrap import bootstrap_external_source_env
from quant.backtest.external_source_onboarding import build_external_source_onboarding_plan
from quant.backtest.order_execution_env_bootstrap import bootstrap_order_execution_env
from quant.backtest.order_execution_safety import build_order_execution_env_audit
from quant.backtest.order_execution_fixture import run_order_execution_fixture_pipeline
from quant.backtest.phase1_doc_alignment import build_phase1_doc_alignment_report
from quant.backtest.production_preflight_fixture import run_fill_first_production_preflight_fixture
from quant.backtest.external_source_state import (
    evaluate_external_source_import_health,
    load_external_source_import_states,
)
from quant.backtest.external_source_run_coverage import (
    build_external_source_run_coverage_report,
    load_external_source_run_coverage_inputs,
)
from quant.backtest.external_source_missing_evidence import build_external_source_missing_evidence_plan
from quant.backtest.fill_evidence import build_fill_evidence_validation_report
from quant.backtest.calibration_fixture import run_shadow_live_calibration_fixture
from quant.backtest.external_source_fixture import build_fill_first_external_source_fixture
from quant.backtest.external_source_fixture_pipeline import run_fill_first_external_source_fixture_pipeline
from quant.backtest.backtest_engine import BACKTEST_ARTIFACT_SCHEMA_VERSION
from quant.backtest.fill_first_readiness import MISSING, READY, REVIEW, build_fill_first_readiness_report
from quant.backtest.fill_first_production_readiness import build_fill_first_production_readiness_report
from quant.backtest.parameter_search_plan import build_parameter_search_plan
from quant.backtest.parameter_search_results import build_parameter_search_results_report
from quant.backtest.parameter_search_runner import run_parameter_search_plan
from quant.backtest.parameter_search_scheduler import build_parameter_search_progress_report
from quant.backtest.production_parameter_staging import (
    normalize_production_parameter_staging,
    validate_production_parameter_staging_status_update,
)
from quant.backtest.run_artifacts import (
    build_backtest_run_artifact_report,
    load_backtest_run_artifact_inputs,
    load_latest_fill_first_backtest_run_id,
)
from quant.smoke_check import run_smoke


FAIL = "fail"
UNKNOWN = "unknown"

CommandRunner = Callable[[Sequence[str], Path], dict[str, Any]]


@dataclass(frozen=True)
class GateCheck:
    name: str
    status: str
    detail: str
    evidence: str
    payload: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "evidence": self.evidence,
            "payload": dict(self.payload or {}),
        }


def build_fill_first_quality_gate_report(
    project_root: Path,
    *,
    conn: Any | None = None,
    check_db: bool = False,
    discovery_roots: Sequence[Path] | None = None,
    max_stale_seconds: int = 86400,
    allow_empty_external: bool = True,
    run_fixture_smoke: bool = True,
    include_db_smoke: bool = False,
    include_run_artifact_audit: bool = False,
    include_fill_evidence_validation: bool = False,
    include_external_run_coverage: bool = False,
    include_external_fixture_db_smoke: bool = False,
    include_pytest: bool = False,
    include_frontend_smoke: bool = False,
    external_source_env: Mapping[str, str] | None = None,
    external_source_env_files: Sequence[Path] | None = None,
    order_execution_env: Mapping[str, str] | None = None,
    order_execution_env_files: Sequence[Path] | None = None,
    command_runner: CommandRunner | None = None,
) -> dict[str, Any]:
    root = Path(project_root)
    env_values = external_source_env
    if env_values is None and external_source_env_files:
        try:
            env_values = load_external_source_env_files(external_source_env_files)
        except Exception:
            env_values = external_source_env
    order_env_values = order_execution_env
    if order_env_values is None and order_execution_env_files:
        try:
            order_env_values = load_external_source_env_files(order_execution_env_files)
        except Exception:
            order_env_values = order_execution_env
    if order_env_values is None:
        order_env_values = env_values
    merged_env_values = _merge_env_values(env_values, order_env_values)
    checks: list[GateCheck] = [
        _readiness_check(root, conn=conn, check_db=check_db),
        _phase1_doc_alignment_check(root),
        _external_discovery_check(root, discovery_roots=discovery_roots),
        _external_env_audit_check(root, env=env_values, env_files=external_source_env_files),
        _external_source_onboarding_check(root, env=env_values, env_files=external_source_env_files),
        _order_execution_env_audit_check(root, env=order_env_values),
        _production_readiness_check(
            root,
            env=merged_env_values,
            env_files=external_source_env_files,
            conn=conn,
            check_db=check_db,
            max_stale_seconds=max_stale_seconds,
        ),
        _configured_external_sources_check(root, env=env_values),
        _external_health_check(
            conn,
            max_stale_seconds=max_stale_seconds,
            allow_empty_external=allow_empty_external,
        ),
    ]
    if run_fixture_smoke:
        checks.append(_smoke_check(include_db=include_db_smoke))
        checks.append(_calibration_fixture_check())
        checks.append(_order_execution_fixture_check())
        checks.append(_production_preflight_fixture_check(root))
        checks.append(_production_readiness_cli_fixture_check(root, command_runner=command_runner))
        checks.append(_external_source_env_bootstrap_fixture_check(root))
        checks.append(_order_execution_env_bootstrap_fixture_check(root))
        checks.append(_external_source_onboarding_fixture_check(root))
        checks.append(_parameter_search_plan_fixture_check())
        checks.append(_parameter_search_results_fixture_check())
        checks.append(_parameter_search_batch_fixture_check())
        checks.append(_parameter_search_scheduler_fixture_check())
        checks.append(_production_parameter_staging_fixture_check())
        checks.append(_production_parameter_staging_review_fixture_check())
        checks.append(_external_source_fixture_check())
        checks.append(_missing_external_evidence_plan_fixture_check())
        checks.append(_current_schema_artifact_fixture_check())
    if include_run_artifact_audit:
        checks.append(_run_artifact_audit_check(conn))
    if include_fill_evidence_validation:
        checks.append(_fill_evidence_validation_check(conn))
    if include_external_run_coverage:
        checks.append(_external_run_coverage_check(conn))
    if include_external_fixture_db_smoke:
        checks.append(_external_source_fixture_db_smoke_check(conn, project_root=root))
    if include_pytest:
        checks.append(
            _command_check(
                "pytest backtest suite",
                [sys.executable, "-m", "pytest", "-q", "quant/backtest/tests", "--maxfail=1"],
                cwd=root,
                evidence="quant/backtest/tests",
                command_runner=command_runner,
            )
        )
    if include_frontend_smoke:
        checks.append(_static_frontend_smoke_check(root))
    status = aggregate_quality_gate_status(check.status for check in checks)
    return {
        "status": status,
        "scope": "fill-first/orderfilled-calibrated execution; LOB/DEPTH intentionally excluded",
        "ready_count": sum(1 for check in checks if check.status == READY),
        "review_count": sum(1 for check in checks if check.status in {REVIEW, UNKNOWN}),
        "fail_count": sum(1 for check in checks if check.status in {FAIL, MISSING}),
        "checks": [check.as_dict() for check in checks],
        "next_actions": quality_gate_next_actions(checks),
    }


def _merge_env_values(*envs: Mapping[str, str] | None) -> dict[str, str] | None:
    merged: dict[str, str] = {}
    for env in envs:
        if env:
            merged.update({str(key): str(value) for key, value in env.items()})
    return merged or None


def _static_frontend_smoke_check(project_root: Path) -> GateCheck:
    return GateCheck(
        "static frontend smoke",
        REVIEW,
        "Not evaluated in backtest-lab; run frontend acceptance in platform-website",
        "platform-website",
    )


def aggregate_quality_gate_status(statuses: Sequence[str] | Any) -> str:
    values = set(statuses)
    if FAIL in values or MISSING in values:
        return FAIL
    if REVIEW in values or UNKNOWN in values:
        return REVIEW
    return READY


def quality_gate_next_actions(checks: Sequence[GateCheck]) -> list[str]:
    actions: list[str] = []
    for check in checks:
        if check.status in {FAIL, MISSING}:
            actions.append(f"Fix failing gate: {check.name} ({check.detail})")
        elif check.status in {REVIEW, UNKNOWN}:
            actions.append(f"Review gate: {check.name} ({check.detail})")
        blockers = (check.payload or {}).get("production_blockers") if isinstance(check.payload, Mapping) else None
        if check.status in {REVIEW, UNKNOWN, FAIL, MISSING} and isinstance(blockers, Sequence) and not isinstance(blockers, (str, bytes)):
            actions.extend(f"{check.name}: {blocker}" for blocker in list(blockers)[:3])
    if not actions:
        actions.append("Fill-first quality gate is ready; continue live/shadow calibration monitoring.")
    return actions


def quality_gate_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Fill-first Quality Gate: {report.get('status')}",
        "",
        f"Scope: {report.get('scope')}",
        "",
        f"- ready: {report.get('ready_count', 0)}",
        f"- review: {report.get('review_count', 0)}",
        f"- fail: {report.get('fail_count', 0)}",
        "",
        "| Gate | Status | Detail | Evidence |",
        "| --- | --- | --- | --- |",
    ]
    for check in report.get("checks", []):
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


def _readiness_check(project_root: Path, *, conn: Any | None, check_db: bool) -> GateCheck:
    report = build_fill_first_readiness_report(project_root, check_db=check_db, conn=conn)
    status = FAIL if report["missing_count"] else report["status"]
    detail = f"ready={report['ready_count']} review={report['review_count']} missing={report['missing_count']}"
    return GateCheck("structural readiness", status, detail, "scripts/check_fill_first_backtest_readiness.py", report)


def _phase1_doc_alignment_check(project_root: Path) -> GateCheck:
    report = build_phase1_doc_alignment_report(project_root)
    status = READY if report["status"] == READY else FAIL
    detail = f"ready={report['ready_count']} missing={report['missing_count']}"
    return GateCheck("phase1 doc alignment", status, detail, "docs/量化/phase1.md", report)


def _external_discovery_check(project_root: Path, *, discovery_roots: Sequence[Path] | None) -> GateCheck:
    roots = list(discovery_roots or default_external_source_roots(project_root))
    candidates = discover_external_source_files(roots)
    report = preview_external_source_import(candidates, base=project_root)
    status = READY if report["status"] == READY else REVIEW
    detail = f"files={report['file_count']} records={report['records_read']} rows_written={report['rows_written']}"
    if not candidates:
        detail += "; no local cost/incident export files discovered"
    return GateCheck("external source discovery", status, detail, "scripts/import_discovered_external_sources.py", report)


def _configured_external_sources_check(project_root: Path, *, env: Mapping[str, str] | None) -> GateCheck:
    report = build_configured_external_source_report(env, project_root=project_root, dry_run=True)
    status = READY if report["status"] == READY else REVIEW
    detail = f"configured={report['configured_count']} skipped={report['skipped_count']}"
    if not report["configured_count"]:
        detail += "; no env-configured external source imports"
    return GateCheck("configured external source imports", status, detail, "scripts/run_configured_external_source_imports.py", report)


def _external_env_audit_check(
    project_root: Path,
    *,
    env: Mapping[str, str] | None,
    env_files: Sequence[Path] | None,
) -> GateCheck:
    report = build_external_source_env_audit(env_files or (), env=env, project_root=project_root)
    if report["status"] == FAIL:
        status = FAIL
    elif report["status"] == READY:
        status = READY
    else:
        status = REVIEW
    detail = (
        f"ready={report.get('ready_count', 0)} review={report.get('review_count', 0)} "
        f"fail={report.get('fail_count', 0)} env_files={report.get('loaded_env_file_count', 0)}"
    )
    if report.get("issues"):
        detail += "; " + str(report["issues"][0])
    return GateCheck("external source env audit", status, detail, "scripts/audit_external_source_env.py", report)


def _external_source_onboarding_check(
    project_root: Path,
    *,
    env: Mapping[str, str] | None,
    env_files: Sequence[Path] | None,
) -> GateCheck:
    report = build_external_source_onboarding_plan(env_files or (), env=env, project_root=project_root)
    if report["status"] == FAIL:
        status = FAIL
    elif report["status"] == READY:
        status = READY
    else:
        status = REVIEW
    blocker_count = len(report.get("production_blockers") or [])
    detail = (
        f"ready={report.get('ready_count', 0)} review={report.get('review_count', 0)} "
        f"fail={report.get('fail_count', 0)} blockers={blocker_count}"
    )
    if report.get("production_blockers"):
        detail += "; " + str(report["production_blockers"][0])
    return GateCheck("external source onboarding", status, detail, "scripts/plan_external_source_onboarding.py", report)


def _order_execution_env_audit_check(project_root: Path, *, env: Mapping[str, str] | None) -> GateCheck:
    report = build_order_execution_env_audit(env)
    status = READY if report["status"] == READY else REVIEW
    detail = (
        f"target={report.get('target_mode')} configured={report.get('configured')} "
        f"auth={report.get('auth')} live_confirmed={report.get('live_confirmed')}"
    )
    if report.get("issues"):
        detail += "; " + str(report["issues"][0])
    return GateCheck("order execution env audit", status, detail, "scripts/audit_order_execution_env.py", report)


def _production_readiness_check(
    project_root: Path,
    *,
    env: Mapping[str, str] | None,
    env_files: Sequence[Path] | None,
    conn: Any | None,
    check_db: bool,
    max_stale_seconds: int,
) -> GateCheck:
    report = build_fill_first_production_readiness_report(
        target_mode="paper",
        env=env,
        env_files=env_files or (),
        project_root=project_root,
        conn=conn,
        check_db=check_db,
        max_stale_seconds=max_stale_seconds,
    )
    status = READY if report["status"] == READY else REVIEW
    detail = (
        f"target={report.get('target_mode')} launch_allowed={report.get('launch_allowed')} "
        f"blocked={len(report.get('blocked_reasons') or [])} review={len(report.get('review_reasons') or [])}"
    )
    return GateCheck("fill-first production readiness", status, detail, "scripts/check_fill_first_production_readiness.py", report)


def _external_health_check(
    conn: Any | None,
    *,
    max_stale_seconds: int,
    allow_empty_external: bool,
) -> GateCheck:
    if conn is None:
        return GateCheck(
            "external source health",
            REVIEW,
            "database not checked; pass --check-db to inspect import freshness rows",
            "scripts/check_external_source_import_health.py",
            {},
        )
    states = load_external_source_import_states(conn, limit=100)
    report = evaluate_external_source_import_health(states, max_stale_seconds=max_stale_seconds)
    status = report["status"]
    if status == UNKNOWN and allow_empty_external:
        status = REVIEW
    detail = f"states={report['state_count']} reason={report['reason']}"
    return GateCheck("external source health", status, detail, "quant.external_source_import_state", report)


def _smoke_check(*, include_db: bool) -> GateCheck:
    try:
        report = run_smoke(include_db=include_db)
    except Exception as exc:  # pragma: no cover - defensive live smoke path
        return GateCheck("fixture smoke", FAIL, f"smoke raised: {exc}", "quant.smoke_check", {})
    status = READY if report.get("passed") else FAIL
    detail = f"mode={report.get('mode')} passed={report.get('passed')}"
    return GateCheck("fixture smoke", status, detail, "quant.smoke_check", report)


def _calibration_fixture_check() -> GateCheck:
    try:
        report = run_shadow_live_calibration_fixture()
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck("shadow/live calibration fixture", FAIL, f"fixture raised: {exc}", "quant.backtest.calibration_fixture", {})
    status = READY if report.get("passed") else FAIL
    detail = (
        f"plan={report.get('plan_status')} validation={report.get('validation_status')} "
        f"samples={report.get('calibration_sample_count')} trust={report.get('calibration_trust_status')}"
    )
    return GateCheck("shadow/live calibration fixture", status, detail, "quant.backtest.calibration_fixture", report)


def _order_execution_fixture_check() -> GateCheck:
    try:
        report = run_order_execution_fixture_pipeline()
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck("order execution fixture", FAIL, f"fixture raised: {exc}", "quant.backtest.order_execution_fixture", {})
    status = READY if report.get("status") == READY else FAIL
    detail = (
        f"runner={report.get('runner_status')} adapter={report.get('adapter_status')} "
        f"samples={report.get('samples_built')} transport_calls={report.get('transport_call_count')}"
    )
    return GateCheck("order execution fixture", status, detail, "quant.backtest.order_execution_fixture", report)


def _production_preflight_fixture_check(project_root: Path) -> GateCheck:
    try:
        with tempfile.TemporaryDirectory(prefix="fill-first-production-preflight-") as tmp:
            report = run_fill_first_production_preflight_fixture(
                Path(tmp),
                project_root=project_root,
                overwrite=True,
            )
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck("production preflight fixture", FAIL, f"fixture raised: {exc}", "quant.backtest.production_preflight_fixture", {})
    readiness = report.get("production_readiness") if isinstance(report.get("production_readiness"), Mapping) else {}
    safety = report.get("order_execution_safety") if isinstance(report.get("order_execution_safety"), Mapping) else {}
    status = READY if report.get("status") == READY else FAIL
    detail = (
        f"preflight={report.get('status')} production={readiness.get('status')} "
        f"launch_allowed={readiness.get('launch_allowed')} safety={safety.get('safety_status')}"
    )
    return GateCheck("production preflight fixture", status, detail, "quant.backtest.production_preflight_fixture", report)


def _production_readiness_cli_fixture_check(project_root: Path, *, command_runner: CommandRunner | None) -> GateCheck:
    try:
        with tempfile.TemporaryDirectory(prefix="fill-first-production-cli-") as tmp:
            tmp_path = Path(tmp)
            order_state = tmp_path / "order_state.jsonl"
            cost_events = tmp_path / "cost_events.jsonl"
            incidents = tmp_path / "incidents.jsonl"
            signals = tmp_path / "signals.jsonl"
            order_state.write_text("{}\n", encoding="utf-8")
            cost_events.write_text("{}\n", encoding="utf-8")
            incidents.write_text("{}\n", encoding="utf-8")
            signals.write_text("{}\n", encoding="utf-8")
            env_file = tmp_path / "fill_first.env"
            env_file.write_text(
                "\n".join(
                    [
                        "ORDER_EXECUTION_TARGET_MODE=paper",
                        "ORDER_EXECUTION_SUBMIT_URL=http://127.0.0.1:9/fill-first-offline-preflight",
                        f"ORDER_STATE_INPUT={order_state}",
                        "ORDER_STATE_SOURCE=offline-fixture-order-state",
                        "ORDER_STATE_KEY=offline-fixture-order-state",
                        f"COST_EVENTS_INPUT={cost_events}",
                        "COST_EVENTS_SOURCE=offline-fixture-cost",
                        "COST_EVENTS_STATE_KEY=offline-fixture-cost",
                        f"PLATFORM_INCIDENTS_INPUT={incidents}",
                        "PLATFORM_INCIDENTS_SOURCE=offline-fixture-incidents",
                        "PLATFORM_INCIDENTS_STATE_KEY=offline-fixture-incidents",
                        f"EXTERNAL_SIGNAL_INPUT={signals}",
                        "EXTERNAL_SIGNAL_SOURCE=offline-fixture-signals",
                        "EXTERNAL_SIGNAL_STATE_KEY=offline-fixture-signals",
                    ]
                ),
                encoding="utf-8",
            )
            artifact_json = tmp_path / "run_artifact.json"
            artifact_json.write_text(
                json.dumps(
                    {
                        "status": READY,
                        "run_id": 42,
                        "paper_live_evidence_gate_report": {
                            "status": READY,
                            "paper_allowed": True,
                            "live_allowed": False,
                            "run_evidence_ready": True,
                            "missing_evidence_ready": True,
                            "blocked_reasons": [],
                            "review_reasons": [],
                        },
                    }
                ),
                encoding="utf-8",
            )
            command = [
                sys.executable,
                str(project_root / "scripts" / "check_fill_first_production_readiness.py"),
                "--env-file",
                str(env_file),
                "--run-artifact-json",
                str(artifact_json),
                "--format",
                "json",
                "--strict-review",
            ]
            result = (command_runner or _run_command)(command, project_root)
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck(
            "production readiness CLI artifact gate fixture",
            FAIL,
            f"fixture raised: {exc}",
            "scripts/check_fill_first_production_readiness.py --run-artifact-json",
            {},
        )
    if int(result.get("returncode", 1)) != 0:
        return GateCheck(
            "production readiness CLI artifact gate fixture",
            FAIL,
            f"exit={result.get('returncode')}",
            "scripts/check_fill_first_production_readiness.py --run-artifact-json",
            result,
        )
    output = str(result.get("stdout") or result.get("output_tail") or "")
    report: Mapping[str, Any] = {}
    if output.strip():
        try:
            parsed = json.loads(output)
            report = parsed if isinstance(parsed, Mapping) else {}
        except json.JSONDecodeError:
            if command_runner is None:
                return GateCheck(
                    "production readiness CLI artifact gate fixture",
                    FAIL,
                    "CLI output was not valid JSON",
                    "scripts/check_fill_first_production_readiness.py --run-artifact-json",
                    result,
                )
    gate = report.get("paper_live_evidence_gate") if isinstance(report.get("paper_live_evidence_gate"), Mapping) else {}
    if report and not (gate.get("checked") is True and gate.get("paper_allowed") is True and gate.get("live_allowed") is False):
        return GateCheck(
            "production readiness CLI artifact gate fixture",
            FAIL,
            (
                f"gate checked={gate.get('checked')} paper_allowed={gate.get('paper_allowed')} "
                f"live_allowed={gate.get('live_allowed')}"
            ),
            "scripts/check_fill_first_production_readiness.py --run-artifact-json",
            {"command_result": result, "report": report},
        )
    detail = (
        f"exit=0 gate_checked={gate.get('checked', 'unknown')} "
        f"paper_allowed={gate.get('paper_allowed', 'unknown')} live_allowed={gate.get('live_allowed', 'unknown')}"
    )
    return GateCheck(
        "production readiness CLI artifact gate fixture",
        READY,
        detail,
        "scripts/check_fill_first_production_readiness.py --run-artifact-json",
        {"command_result": result, "report": report},
    )


def _external_source_onboarding_fixture_check(project_root: Path) -> GateCheck:
    try:
        with tempfile.TemporaryDirectory(prefix="fill-first-external-onboarding-") as tmp:
            fixture = build_fill_first_external_source_fixture(Path(tmp), run_id=9101)
            report = build_external_source_onboarding_plan(
                [Path(fixture["files"]["env_file"])],
                env={},
                project_root=project_root,
            )
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck(
            "external source onboarding fixture",
            FAIL,
            f"fixture raised: {exc}",
            "scripts/plan_external_source_onboarding.py",
            {},
        )
    source_count = int(report.get("source_count") or 0)
    ready_count = int(report.get("ready_count") or 0)
    command_ready = all(
        bool(item.get("dry_run_command")) and bool(item.get("write_command"))
        for item in report.get("sources", [])
        if isinstance(item, Mapping)
    )
    status = READY if report.get("status") == READY and source_count >= 4 and ready_count == source_count and command_ready else FAIL
    detail = f"status={report.get('status')} ready={ready_count}/{source_count} command_ready={command_ready}"
    return GateCheck("external source onboarding fixture", status, detail, "scripts/plan_external_source_onboarding.py", report)


def _external_source_env_bootstrap_fixture_check(project_root: Path) -> GateCheck:
    try:
        with tempfile.TemporaryDirectory(prefix="fill-first-external-bootstrap-") as tmp:
            report = bootstrap_external_source_env(Path(tmp) / "sources", project_root=project_root)
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck(
            "external source env bootstrap fixture",
            FAIL,
            f"fixture raised: {exc}",
            "scripts/bootstrap_fill_first_external_sources.py",
            {},
        )
    status = READY if report.get("status") == READY and report.get("audit_status") == READY and report.get("onboarding_status") == READY else FAIL
    detail = (
        f"status={report.get('status')} audit={report.get('audit_status')} "
        f"onboarding={report.get('onboarding_status')} env_written={report.get('env_written')}"
    )
    return GateCheck("external source env bootstrap fixture", status, detail, "scripts/bootstrap_fill_first_external_sources.py", report)


def _order_execution_env_bootstrap_fixture_check(project_root: Path) -> GateCheck:
    try:
        with tempfile.TemporaryDirectory(prefix="fill-first-order-execution-bootstrap-") as tmp:
            report = bootstrap_order_execution_env(Path(tmp) / "order-execution")
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck(
            "order execution env bootstrap fixture",
            FAIL,
            f"fixture raised: {exc}",
            "scripts/bootstrap_fill_first_order_execution.py",
            {},
        )
    status = READY if report.get("status") == READY and report.get("audit_status") == READY and report.get("contains_live_execution") is False else FAIL
    detail = (
        f"status={report.get('status')} audit={report.get('audit_status')} "
        f"live={report.get('contains_live_execution')} env_written={report.get('env_written')}"
    )
    return GateCheck("order execution env bootstrap fixture", status, detail, "scripts/bootstrap_fill_first_order_execution.py", report)


def _parameter_search_plan_fixture_check() -> GateCheck:
    try:
        report = build_parameter_search_plan(base_payload={"price_source": "orderfilled_block_close"})
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck("parameter search plan fixture", FAIL, f"fixture raised: {exc}", "scripts/plan_fill_first_parameter_search.py", {})
    status = READY if report.get("status") == READY else FAIL
    detail = (
        f"status={report.get('status')} sets={report.get('parameter_set_count')} "
        f"runs={report.get('planned_run_count')} modes={','.join(str(mode) for mode in report.get('evidence_modes', []))}"
    )
    return GateCheck("parameter search plan fixture", status, detail, "scripts/plan_fill_first_parameter_search.py", report)


def _parameter_search_results_fixture_check() -> GateCheck:
    try:
        plan = build_parameter_search_plan(
            grid={
                "entry_threshold": ["0.56", "0.58", "0.60"],
                "execution_profile": ["realistic", "conservative"],
            },
            evidence_modes=["train", "test", "walk_forward"],
            max_runs=50,
        )
        report = build_parameter_search_results_report(plan, _parameter_search_result_fixture_rows(plan))
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck(
            "parameter search results fixture",
            FAIL,
            f"fixture raised: {exc}",
            "scripts/check_fill_first_parameter_search_results.py",
            {},
        )
    staging = report.get("production_parameter_staging") if isinstance(report.get("production_parameter_staging"), Mapping) else {}
    robustness = report.get("robustness_report") if isinstance(report.get("robustness_report"), Mapping) else {}
    status = READY if report.get("status") == READY and staging.get("staging_allowed") is True else FAIL
    detail = (
        f"status={report.get('status')} coverage={report.get('coverage_pct')} "
        f"robustness={robustness.get('robustness_verdict')} staging={staging.get('staging_allowed')}"
    )
    return GateCheck("parameter search results fixture", status, detail, "scripts/check_fill_first_parameter_search_results.py", report)


def _parameter_search_batch_fixture_check() -> GateCheck:
    try:
        plan = build_parameter_search_plan(
            grid={
                "entry_threshold": ["0.56", "0.58", "0.60"],
                "execution_profile": ["realistic", "conservative"],
            },
            evidence_modes=["train", "test", "walk_forward"],
            max_runs=50,
        )
        report = run_parameter_search_plan(
            plan,
            executor=_parameter_search_fixture_executor,
            dry_run=False,
            stage_parameters=True,
            strategy_name="favorite_hold_v1",
            strategy_version="fixture",
        )
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck(
            "parameter search batch fixture",
            FAIL,
            f"fixture raised: {exc}",
            "scripts/run_fill_first_parameter_search_batch.py",
            {},
        )
    results = report.get("parameter_search_results") if isinstance(report.get("parameter_search_results"), Mapping) else {}
    staging = report.get("staging_preview") if isinstance(report.get("staging_preview"), Mapping) else {}
    robustness = results.get("robustness_report") if isinstance(results.get("robustness_report"), Mapping) else {}
    status = READY if report.get("status") == READY and results.get("status") == READY and staging.get("staging_allowed") is True else FAIL
    detail = (
        f"status={report.get('status')} executed={report.get('executed_run_count')}/{report.get('planned_run_count')} "
        f"results={results.get('status')} robustness={robustness.get('robustness_verdict')} staging={staging.get('staging_allowed')}"
    )
    return GateCheck("parameter search batch fixture", status, detail, "scripts/run_fill_first_parameter_search_batch.py", report)


def _parameter_search_scheduler_fixture_check() -> GateCheck:
    try:
        plan = build_parameter_search_plan(
            grid={
                "entry_threshold": ["0.56", "0.58", "0.60"],
                "execution_profile": ["realistic", "conservative"],
            },
            evidence_modes=["train", "test", "walk_forward"],
            max_runs=50,
        )
        ready_items = _parameter_search_scheduler_fixture_items(plan, status="succeeded")
        ready_report = build_parameter_search_progress_report(
            {
                "batch_id": 9101,
                "plan": plan,
                "universe_name": "fill_first_parameter_search",
                "strategy_name": "favorite_hold_v1",
                "strategy_version": "fixture",
            },
            ready_items,
        )
        retry_items = _parameter_search_scheduler_fixture_items(plan, status="queued")
        retry_items[0]["status"] = "failed"
        retry_items[0]["attempt_count"] = 1
        retry_items[0]["max_attempts"] = 2
        retry_report = build_parameter_search_progress_report({"batch_id": 9102, "plan": plan}, retry_items)
        canceled_items = _parameter_search_scheduler_fixture_items(plan, status="canceled")
        canceled_report = build_parameter_search_progress_report({"batch_id": 9103, "plan": plan}, canceled_items)
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck(
            "parameter search scheduler fixture",
            FAIL,
            f"fixture raised: {exc}",
            "scripts/run_fill_first_parameter_search_scheduler.py",
            {},
        )
    status = (
        READY
        if ready_report.get("status") == READY
        and retry_report.get("retryable_count") == 1
        and canceled_report.get("status") == "canceled"
        else FAIL
    )
    detail = (
        f"ready={ready_report.get('status')} completion={ready_report.get('completion_pct')} "
        f"retryable={retry_report.get('retryable_count')} queued={retry_report.get('queued_count')} "
        f"canceled={canceled_report.get('canceled_count')}"
    )
    return GateCheck(
        "parameter search scheduler fixture",
        status,
        detail,
        "scripts/run_fill_first_parameter_search_scheduler.py",
        {"ready": ready_report, "retryable": retry_report, "canceled": canceled_report},
    )


def _production_parameter_staging_fixture_check() -> GateCheck:
    try:
        plan = build_parameter_search_plan(
            grid={
                "entry_threshold": ["0.56", "0.58", "0.60"],
                "execution_profile": ["realistic", "conservative"],
            },
            evidence_modes=["train", "test", "walk_forward"],
            max_runs=50,
        )
        report = build_parameter_search_results_report(plan, _parameter_search_result_fixture_rows(plan))
        staging = normalize_production_parameter_staging(
            report,
            status="pending",
            source="quality-gate-fixture",
            strategy_name="favorite_hold_v1",
            strategy_version="fixture",
            universe_name="fill_first_parameter_search",
        )
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck(
            "production parameter staging fixture",
            FAIL,
            f"fixture raised: {exc}",
            "scripts/stage_production_parameters.py",
            {},
        )
    status = READY if staging.get("staging_allowed") is True and staging.get("status") == "pending" and staging.get("parameter_fingerprint") else FAIL
    detail = (
        f"status={staging.get('status')} allowed={staging.get('staging_allowed')} "
        f"robustness={staging.get('robustness_verdict')} action={staging.get('default_action')}"
    )
    return GateCheck("production parameter staging fixture", status, detail, "scripts/stage_production_parameters.py", staging)


def _production_parameter_staging_review_fixture_check() -> GateCheck:
    try:
        plan = build_parameter_search_plan(
            grid={
                "entry_threshold": ["0.56", "0.58", "0.60"],
                "execution_profile": ["realistic", "conservative"],
            },
            evidence_modes=["train", "test", "walk_forward"],
            max_runs=50,
        )
        ready_report = build_parameter_search_results_report(plan, _parameter_search_result_fixture_rows(plan))
        ready_row = normalize_production_parameter_staging(ready_report, strategy_name="favorite_hold_v1", strategy_version="fixture")
        approved = validate_production_parameter_staging_status_update(
            ready_row,
            status="approved",
            reviewed_by="quality-gate",
            review_note="fixture evidence reviewed",
        )
        review_report = build_parameter_search_results_report(plan, _parameter_search_result_fixture_rows(plan)[:-1])
        blocked_row = normalize_production_parameter_staging(review_report, force_review=True)
        rejected = validate_production_parameter_staging_status_update(
            blocked_row,
            status="rejected",
            reviewed_by="quality-gate",
            review_note="missing planned result row",
        )
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck(
            "production parameter staging review fixture",
            FAIL,
            f"fixture raised: {exc}",
            "scripts/review_production_parameter_staging.py",
            {},
        )
    status = READY if approved.get("approved") is True and rejected.get("status") == "rejected" and rejected.get("approved") is False else FAIL
    detail = f"approve={approved.get('status')} reviewer={approved.get('reviewed_by')} reject={rejected.get('status')}"
    return GateCheck(
        "production parameter staging review fixture",
        status,
        detail,
        "scripts/review_production_parameter_staging.py",
        {"approved": approved, "rejected": rejected},
    )


def _parameter_search_fixture_executor(payload: Mapping[str, Any]) -> dict[str, Any]:
    threshold = str(payload.get("entry_threshold"))
    threshold_decimal = Decimal(threshold)
    profile = str(payload.get("execution_profile"))
    mode = str(payload.get("evidence_mode"))
    base_score = {"0.56": 4.0, "0.58": 7.0, "0.6": 2.0, "0.60": 2.0}.get(threshold, 1.0)
    profile_adjustment = 0.5 if profile == "realistic" else 0.0
    return {
        "run_id": f"fixture-{payload.get('parameter_fingerprint')}-{payload.get('evidence_mode')}",
        "status": "succeeded",
        "market_slug": "fixture-market",
        "parameters": {
            "entry_threshold": threshold,
            "execution_profile": profile,
        },
        "market_category": "crypto" if threshold_decimal == Decimal("0.58") else "sports",
        "liquidity_bucket": "active" if profile == "realistic" else "thin",
        "volatility_bucket": "normal" if profile == "realistic" else "high",
        "time_to_expiry_bucket": "gte_7d" if mode == "train" else "lt_1d",
        "final_minute": "final_minute" if mode == "walk_forward" else "not_final_minute",
        "event_outcome_count_bucket": "large_multi_21_plus" if threshold_decimal == Decimal("0.60") else "binary",
        "metrics": [
            {"metric_key": "net_profit", "value": str(base_score + profile_adjustment)},
            {"metric_key": "performance_score", "value": str(base_score + profile_adjustment)},
            {"metric_key": "max_drawdown", "value": "1"},
            {"metric_key": "liquidity_fill_rate", "value": "80"},
            {"metric_key": "total_trades", "value": "20"},
            {"metric_key": "submitted_orders", "value": "25"},
        ],
    }


def _parameter_search_scheduler_fixture_items(plan: Mapping[str, Any], *, status: str) -> list[dict[str, Any]]:
    result_rows = _parameter_search_result_fixture_rows(plan)
    items: list[dict[str, Any]] = []
    for index, item in enumerate(plan.get("plan_items") or [], start=1):
        if not isinstance(item, Mapping):
            continue
        items.append(
            {
                "item_id": index,
                "batch_id": 9101,
                "item_index": index,
                "item_key": item.get("key"),
                "status": status,
                "parameter_fingerprint": item.get("parameter_fingerprint"),
                "evidence_mode": item.get("evidence_mode"),
                "parameters": dict(item.get("parameters") or {}),
                "request_payload": dict(item.get("request_payload") or {}),
                "result_row": result_rows[index - 1] if status == "succeeded" else {},
                "attempt_count": 1 if status != "queued" else 0,
                "max_attempts": 2,
            }
        )
    return items


def _parameter_search_result_fixture_rows(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    score_by_threshold = {"0.56": "4", "0.58": "7", "0.6": "2", "0.60": "2"}
    for index, item in enumerate(plan.get("plan_items") or [], start=1):
        if not isinstance(item, Mapping):
            continue
        params = dict(item.get("parameters") or {})
        threshold = str(params.get("entry_threshold"))
        profile = str(params.get("execution_profile"))
        base_score = float(score_by_threshold.get(threshold, "1"))
        profile_adjustment = 0.5 if profile == "realistic" else 0.0
        rows.append(
            {
                "run_id": index,
                "parameter_fingerprint": item.get("parameter_fingerprint"),
                "parameters": params,
                "mode": item.get("evidence_mode"),
                "market_category": "sports" if index % 2 else "crypto",
                "liquidity_bucket": "active" if index % 2 else "thin",
                "volatility_bucket": "normal" if index % 3 else "high",
                "time_to_expiry_bucket": "gte_7d" if index % 2 else "lt_1d",
                "final_minute": "not_final_minute" if index % 2 else "final_minute",
                "event_outcome_count_bucket": "binary" if index % 2 else "large_multi_21_plus",
                "net_pnl": str(base_score + profile_adjustment),
                "performance_score": str(base_score + profile_adjustment),
                "max_drawdown": "1",
                "fill_rate": "0.80",
                "sample_count": 20,
            }
        )
    return rows


def _external_source_fixture_check() -> GateCheck:
    try:
        with tempfile.TemporaryDirectory(prefix="fill-first-external-fixture-") as tmp:
            report = build_fill_first_external_source_fixture(Path(tmp), run_id=9001)
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck("external source fixture", FAIL, f"fixture raised: {exc}", "quant.backtest.external_source_fixture", {})
    status = READY if report.get("status") == READY else FAIL
    counts = report.get("event_counts") if isinstance(report.get("event_counts"), Mapping) else {}
    validation = report.get("validation") if isinstance(report.get("validation"), Mapping) else {}
    configured = report.get("configured_imports") if isinstance(report.get("configured_imports"), Mapping) else {}
    detail = (
        f"orders={counts.get('order_state', 0)} costs={counts.get('cost_events', 0)} "
        f"incidents={counts.get('platform_incidents', 0)} signals={counts.get('external_signals', 0)} "
        f"validation={validation.get('status')} "
        f"configured={configured.get('status')}"
    )
    return GateCheck("external source fixture", status, detail, "quant.backtest.external_source_fixture", report)


def _missing_external_evidence_plan_fixture_check() -> GateCheck:
    try:
        report = build_external_source_missing_evidence_plan(_current_schema_artifact_fixture_inputs())
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck("missing external evidence plan fixture", FAIL, f"fixture raised: {exc}", "quant.backtest.external_source_missing_evidence", {})
    status = READY if report.get("status") == READY else FAIL
    detail = (
        f"status={report.get('status')} missing_order_state={report.get('missing_order_state_count')} "
        f"missing_calibration={report.get('missing_calibration_count')}"
    )
    return GateCheck("missing external evidence plan fixture", status, detail, "quant.backtest.external_source_missing_evidence", report)


def _current_schema_artifact_fixture_check() -> GateCheck:
    try:
        report = build_backtest_run_artifact_report(_current_schema_artifact_fixture_inputs())
    except Exception as exc:  # pragma: no cover - defensive quality gate path
        return GateCheck("current schema run artifact fixture", FAIL, f"fixture raised: {exc}", "quant.backtest.run_artifacts", {})
    schema = _artifact_report_schema_version(report)
    artifact_status = str(report.get("status") or UNKNOWN)
    status = READY if schema == BACKTEST_ARTIFACT_SCHEMA_VERSION and artifact_status not in {MISSING, UNKNOWN, FAIL} else FAIL
    detail = f"artifact_status={artifact_status} schema={schema or '-'}"
    return GateCheck("current schema run artifact fixture", status, detail, "quant.backtest.run_artifacts", report)


def _current_schema_artifact_fixture_inputs() -> dict[str, Any]:
    fill_quality = {
        "signal_count": 2,
        "submitted_count": 2,
        "filled_count": 1,
        "partial_fill_count": 0,
        "no_fill_count": 1,
        "no_fill_reasons": {"NO_LIQUIDITY": 1},
        "expected_fill_size": "10",
        "actual_fill_size": "10",
        "expected_fill_notional": "4",
        "actual_fill_notional": "4",
        "avg_participation_rate": "25",
        "raw_event_count": 2,
        "loaded_raw_event_count": 2,
        "deduped_raw_event_count": 2,
        "raw_duplicate_event_count": 0,
        "raw_canonical_event_count": 2,
        "raw_fallback_key_event_count": 0,
        "raw_unknown_key_event_count": 0,
        "raw_canonical_fill_key_coverage_pct": "100",
        "raw_maker_taker_side_coverage_pct": "100",
        "raw_block_context_coverage_pct": "100",
        "raw_trade_tick_count": 2,
        "raw_block_count": 1,
        "candidate_event_count": 1,
        "consumed_event_count": 1,
        "raw_evidence_summary": {
            "canonical_key_fields": ["tx_hash", "log_index", "market_id", "condition_id", "token_id", "maker", "taker", "side"],
            "candidate_event_unique_count": 1,
            "candidate_event_duplicate_count": 0,
            "consumed_event_unique_count": 1,
            "consumed_event_duplicate_count": 0,
            "candidate_unique_notional": "4",
            "consumed_unique_notional": "4",
            "raw_trade_tick_report": {
                "status": READY,
                "trade_tick_count": 2,
                "block_count": 1,
                "canonical_fill_key_coverage_pct": "100",
                "maker_taker_side_coverage_pct": "100",
                "block_context_coverage_pct": "100",
            },
        },
        "loaded_block_window": {
            "from_block": 10,
            "to_block": 20,
            "loaded_first_block": 11,
            "loaded_last_block": 11,
            "replay_first_block": 11,
            "replay_last_block": 11,
            "market_id": 1,
            "token_id_hex": "0xabc",
            "limit": 100,
            "loaded_event_count": 2,
            "replay_event_count": 2,
            "duplicate_event_count": 0,
            "hit_limit": False,
        },
        "environment_flags": {},
        "order_anomaly_flags": {},
        "avg_markout_after_1_bars": "0.01",
    }
    data_quality = {
        "status": READY,
        "data_version": "artifact-fixture-v1",
        "source_table": "quant.market_token_block_close",
        "access_path": "token_id_block_range",
        "x_axis": "block_number",
        "rows": 10,
        "first_x": 10,
        "last_x": 20,
        "median_delta": 1,
        "gap_count": 0,
        "gap_threshold": 4,
        "largest_gaps": [],
        "jump_count": 0,
        "largest_jumps": [],
        "requested_from": 10,
        "requested_to": 20,
        "span_coverage_pct": "100",
        "warning_level": "OK",
        "caveats": [],
        "orderfilled_replay": {
            "source": "orderfilled_fact",
            "fallback": None,
            "from_block": 10,
            "to_block": 20,
            "market_id": 1,
            "token_id": "0xabc",
            "limit": 100,
            "loaded_event_count": 2,
            "deduped_event_count": 2,
            "duplicate_event_count": 0,
            "canonical_event_count": 2,
            "fallback_key_event_count": 0,
            "unknown_key_event_count": 0,
            "raw_trade_tick_count": 2,
            "raw_block_count": 1,
            "raw_canonical_fill_key_coverage_pct": "100",
            "raw_maker_taker_side_coverage_pct": "100",
            "raw_block_context_coverage_pct": "100",
            "loaded_block_window": {
                "from_block": 10,
                "to_block": 20,
                "loaded_first_block": 11,
                "loaded_last_block": 11,
                "replay_first_block": 11,
                "replay_last_block": 11,
                "market_id": 1,
                "token_id_hex": "0xabc",
                "limit": 100,
                "loaded_event_count": 2,
                "replay_event_count": 2,
                "duplicate_event_count": 0,
                "hit_limit": False,
            },
            "raw_trade_tick_report": {
                "status": READY,
                "trade_tick_count": 2,
                "block_count": 1,
                "canonical_fill_key_coverage_pct": "100",
                "maker_taker_side_coverage_pct": "100",
                "block_context_coverage_pct": "100",
            },
        },
        "pmxt_l2_alignment": {
            "status": READY,
            "pmxt_rows_seen": 2,
            "pmxt_matched_events": 2,
            "pmxt_applied_events": 2,
            "orderfilled_rows_seen": 1,
            "orderfilled_rows_matched": 1,
            "aligned_count": 1,
            "alignment_pct": "100",
            "max_lag_ms": 60000,
            "missing_timestamp_count": 0,
            "missing_l2_before_fill_count": 0,
            "stale_l2_count": 0,
            "price_outside_spread_count": 0,
            "price_unchecked_count": 0,
            "sample_rows": [{"tx_hash": "0xfixture", "status": "aligned"}],
        },
        "fill_quality": fill_quality,
    }
    parameters = {
        "entry_threshold": "0.6",
        "exit_threshold": "0.5",
        "initial_capital": "1000",
        "position_size": "10",
        "execution_price_mode": "ORDERFILLED_CROSS",
        "execution_profile": "realistic",
        "order_role": "maker",
        "latency_blocks": 1,
        "latency_seconds": "0",
        "allow_partial_fill": True,
        "final_valuation_mode": "SETTLEMENT",
        "settlement_value": "1",
        "resolution_source": "polymarket",
        "settlement_rule": "official result",
        "price_to_beat_source": "not_applicable",
        "oracle_source": "uma",
        "market_lifecycle_status": "closed",
        "resolved_outcome": "YES",
        "end_date": "2026-06-22T12:00:00Z",
    }
    event_outcomes = [
        {"market_slug": "fixture-market", "outcome_label": "Alpha", "yes_probability": "0.40", "no_probability": "0.60"},
        {"market_slug": "fixture-market-b", "outcome_label": "Beta", "yes_probability": "0.35", "no_probability": "0.65"},
        {"market_slug": "fixture-market-c", "outcome_label": "Gamma", "yes_probability": "0.25", "no_probability": "0.75"},
    ]
    return {
        "run": {
            "run_id": -9101,
            "status": "succeeded",
            "market_slug": "fixture-market",
            "token_side": "YES",
            "price_source": "orderfilled_block_close",
            "backtest_engine": "builtin",
            "from_block": 10,
            "to_block": 20,
            "rows_processed": 10,
            "meta": {
                "strategy_name": "fixed_threshold",
                "strategy_version": "fixed_threshold_v1",
                "artifact_schema_version": BACKTEST_ARTIFACT_SCHEMA_VERSION,
                "code_commit": "artifact-fixture",
                "code_dirty": False,
                "code_source": "fixture",
                "model_versions": {
                    "fill_model": "orderfilled_cross_then_settlement",
                    "fill_model_version": "orderfilled_limit_cross_v1",
                    "fee_model_version": "maker_taker_rebate_v1",
                    "slippage_model_version": "adverse_slippage_bps_cents_v1",
                },
                "parameter_fingerprint": "artifact-fixture-fp",
                "parameter_snapshot": parameters,
                "event_slug": "fixture-event",
                "event_outcome_count": len(event_outcomes),
                "event_outcomes": event_outcomes,
                "outcome_correlation": {
                    "status": READY,
                    "method": "fixture",
                    "sample_count": 10,
                    "max_abs_correlation": "0",
                    "matrix": {"fixture-market": {"fixture-market": 1}},
                },
                "actual_data_quality": data_quality,
            },
        },
        "parameters": parameters,
        "metrics": [{"metric_key": key} for key in ("data_quality_status", "gap_count", "data_version", "fill_quality_fill_rate", "fill_quality_no_fill_rate")],
        "orders": [
            {
                "order_id": "o1",
                "signal_index": 1,
                "status": "FILLED",
                "side": "BUY_YES",
                "role": "maker",
                "order_type": "post_only_limit",
                "x_axis": "block_number",
                "signal_x": 10,
                "submit_x": 11,
                "decision_price": "0.40",
                "requested_price": "0.40",
                "requested_size": "10",
                "requested_notional": "4",
                "expected_fill_size": "10",
                "expected_fill_notional": "4",
                "actual_fill_size": "10",
                "actual_fill_notional": "4",
                "filled_size": "10",
                "filled_notional": "4",
                "unfilled_size": "0",
                "fill_probability": "100",
                "fill_pct": "100",
                "block_volume": "12",
                "trade_count": 2,
                "available_notional": "9.6",
                "participation_rate": "25",
                "latency_blocks": 1,
                "latency_seconds": "0",
                "execution_source": "orderfilled_cross",
                "meta": {
                    "time_in_force": "GTC",
                    "strategy_intent": {"role": "maker", "order_type": "post_only_limit", "time_in_force": "GTC"},
                    "effective_liquidity_cap_pct": "80",
                    "fill_probability_haircut_pct": "20",
                    "raw_fill_probability": "100",
                    "effective_fill_probability": "80",
                    "context": {"market_category": "sports", "liquidity_bucket": "medium", "volatility_bucket": "low", "time_to_expiry_bucket": "lt_7d", "time_to_expiry_seconds": 3600, "event_outcome_count": len(event_outcomes)},
                },
            },
            {
                "order_id": "o2",
                "signal_index": 2,
                "status": "NO_FILL",
                "side": "BUY_YES",
                "role": "taker",
                "order_type": "marketable_limit",
                "x_axis": "block_number",
                "signal_x": 12,
                "submit_x": 13,
                "decision_price": "0.41",
                "requested_price": "0.41",
                "requested_size": "5",
                "requested_notional": "2.05",
                "expected_fill_size": "0",
                "expected_fill_notional": "0",
                "actual_fill_size": "0",
                "actual_fill_notional": "0",
                "filled_size": "0",
                "filled_notional": "0",
                "unfilled_size": "5",
                "fill_probability": "0",
                "fill_pct": "0",
                "block_volume": "1",
                "trade_count": 1,
                "available_notional": "0",
                "participation_rate": "0",
                "latency_blocks": 1,
                "latency_seconds": "0",
                "no_fill_reason": "NO_LIQUIDITY",
                "execution_source": "orderfilled_cross",
                "meta": {
                    "time_in_force": "FAK",
                    "strategy_intent": {"role": "taker", "order_type": "marketable_limit", "time_in_force": "FAK"},
                    "effective_liquidity_cap_pct": "100",
                    "fill_probability_haircut_pct": "0",
                    "raw_fill_probability": "0",
                    "effective_fill_probability": "0",
                    "context": {"market_category": "sports", "liquidity_bucket": "thin", "volatility_bucket": "low", "time_to_expiry_bucket": "lt_7d", "time_to_expiry_seconds": 3500, "event_outcome_count": len(event_outcomes)},
                },
            },
        ],
        "trades": [{"trade_id": "t1", "entry_price": "0.40", "close_line_probability": "0.55", "exit_price": "1", "payoff_per_share": "1", "size": "10", "notional": "4", "pnl": "6", "exit_reason": "settlement"}],
        "ledger": [
            {"ledger_id": "l1", "trade_id": "t1", "event_type": "BUY", "x_axis": "block_number", "x_value": 11, "shares_delta": "10", "cash_delta": "-4", "fee": "0", "rebate": "0", "slippage_cost": "0", "execution_cost": "0", "realized_pnl": "0", "position_after": "10", "cash_after": "996", "price": "0.40", "source": "simulated_trade"},
            {"ledger_id": "l2", "trade_id": "t1", "event_type": "SETTLEMENT", "x_axis": "block_number", "x_value": 20, "shares_delta": "-10", "cash_delta": "10", "fee": "0", "rebate": "0", "slippage_cost": "0", "execution_cost": "0", "realized_pnl": "6", "position_after": "0", "cash_after": "1006", "price": "1", "source": "simulated_trade"},
        ],
        "events": [
            {"event_type": "open", "x_axis": "block_number", "x_value": 10, "trade_id": "t1", "price": "0.40", "message": "entry", "meta": {}},
            {"event_type": "settlement", "x_axis": "block_number", "x_value": 20, "trade_id": "t1", "price": "1", "message": "settlement", "meta": {}},
        ],
        "calibration_count": 2,
        "cost_calibration_count": 1,
        "calibration_rows": [
            {"simulated_order_id": "o1", "live_order_id": "live-o1", "simulated_status": "FILLED", "live_status": "FILLED", "simulated_fill_price": "0.40", "live_fill_price": "0.40", "simulated_fill_size": "10", "live_fill_size": "10", "simulated_slippage": "0", "live_slippage": "0", "simulated_fee": "0", "live_fee": "0", "simulated_rebate": "0", "live_rebate": "0", "simulated_cash_delta": "-4", "live_cash_delta": "-4", "simulated_position_delta": "10", "live_position_delta": "10", "simulated_latency_seconds": "1", "live_latency_seconds": "1", "role": "maker", "side": "BUY_YES", "liquidity_bucket": "medium", "volatility_bucket": "low", "time_to_expiry_bucket": "lt_7d"},
            {"simulated_order_id": "o2", "live_order_id": "live-o2", "simulated_status": "NO_FILL", "live_status": "NO_FILL", "simulated_fill_price": None, "live_fill_price": None, "simulated_fill_size": "0", "live_fill_size": "0", "simulated_slippage": "0", "live_slippage": "0", "simulated_fee": "0", "live_fee": "0", "simulated_rebate": "0", "live_rebate": "0", "simulated_cash_delta": "0", "live_cash_delta": "0", "simulated_position_delta": "0", "live_position_delta": "0", "simulated_latency_seconds": "1", "live_latency_seconds": "1", "role": "taker", "side": "BUY_YES", "liquidity_bucket": "thin", "volatility_bucket": "low", "time_to_expiry_bucket": "lt_7d"},
        ],
        "cost_calibration_rows": [{"event_type": "FEE", "simulated_amount": "0", "live_amount": "0", "amount_error": "0", "simulated_count": 1, "live_count": 1, "verdict": "matched"}],
        "real_order_state_event_count": 2,
        "real_order_state_rows": [
            {"order_id": "o1", "external_order_id": "live-o1", "event_type": "FILLED", "payload": {"simulated_order_id": "o1"}},
            {"order_id": "o2", "external_order_id": "live-o2", "event_type": "NO_FILL", "payload": {"simulated_order_id": "o2"}},
        ],
        "real_cost_event_rows": [],
        "external_source_state_count": 1,
        "external_source_state_rows": [{"state_key": "fixture-external-source", "source_name": "fixture", "updated_at": "2026-06-22T12:00:00Z"}],
        "platform_incidents": [{"incident_key": "fixture-ok", "severity": "info", "component": "platform", "title": "fixture incident coverage"}],
        "event_outcomes": event_outcomes,
        "outcome_correlation": {"status": READY, "method": "fixture", "sample_count": 10, "max_abs_correlation": "0", "matrix": {"fixture-market": {"fixture-market": 1}}},
    }


def _run_artifact_audit_check(conn: Any | None) -> GateCheck:
    if conn is None:
        return GateCheck(
            "latest run artifact audit",
            REVIEW,
            "database not checked; pass --check-db with --include-run-artifact-audit",
            "scripts/audit_backtest_run_artifacts.py",
            {},
        )
    run_id = load_latest_fill_first_backtest_run_id(conn)
    inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id) if run_id is not None else None
    report = build_backtest_run_artifact_report(inputs, run_id=run_id)
    if run_id is None:
        report = {
            **report,
            "reason": "no_fill_first_backtest_runs",
            "next_actions": ["Create or select an ORDERFILLED_CROSS backtest run before strict artifact audit."],
        }
    status = report["status"]
    if status == MISSING:
        status = FAIL if _artifact_report_is_current_fill_first(report) else REVIEW
    mode = _artifact_report_execution_mode(report)
    schema_version = _artifact_report_schema_version(report)
    detail = f"run_id={report.get('run_id')} status={report.get('status')} mode={mode or '-'} schema={schema_version or '-'}"
    if report["status"] == MISSING and status == REVIEW:
        detail += "; selected run is not current fill-first schema, create a fresh ORDERFILLED_CROSS run for strict artifact audit"
    if run_id is None:
        detail += "; no fill-first run found"
    return GateCheck("latest fill-first run artifact audit", status, detail, "scripts/audit_backtest_run_artifacts.py", report)


def _fill_evidence_validation_check(conn: Any | None) -> GateCheck:
    if conn is None:
        return GateCheck(
            "latest fill evidence validation",
            REVIEW,
            "database not checked; pass --check-db with --include-fill-evidence-validation",
            "scripts/validate_fill_evidence.py",
            {},
        )
    run_id = load_latest_fill_first_backtest_run_id(conn)
    if run_id is None:
        return GateCheck(
            "latest fill evidence validation",
            REVIEW,
            "no fill-first run found",
            "scripts/validate_fill_evidence.py",
            {"status": MISSING, "reason": "no_fill_first_backtest_runs"},
        )
    inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
    if inputs is None:
        return GateCheck(
            "latest fill evidence validation",
            REVIEW,
            f"run_id={run_id} not found",
            "scripts/validate_fill_evidence.py",
            {"status": UNKNOWN, "run_id": run_id, "reason": "run_not_found"},
        )
    fill_quality = _fill_quality_from_run_inputs(inputs)
    orders = list(inputs.get("orders") or [])
    report = build_fill_evidence_validation_report(orders, fill_quality=fill_quality)
    report["run_id"] = run_id
    unsupported_filled = _unsupported_filled_evidence_count(orders)
    status = READY if report.get("status") == READY else REVIEW
    if unsupported_filled:
        status = FAIL
        report["status"] = FAIL
        report["unsupported_filled_evidence_count"] = unsupported_filled
        report["reason"] = (
            f"{unsupported_filled} filled/partial orders have no auditable execution evidence; "
            + str(report.get("reason") or "").strip()
        ).strip("; ")
    detail = (
        f"run_id={run_id} status={report.get('status')} filled={report.get('filled_count', 0)} "
        f"raw={report.get('raw_orderfilled_fill_count', 0)} "
        f"block_fallback={report.get('block_bar_synthetic_fill_count', 0)} "
        f"unsupported_filled={unsupported_filled}"
    )
    return GateCheck("latest fill evidence validation", status, detail, "scripts/validate_fill_evidence.py", report)


def _external_run_coverage_check(conn: Any | None) -> GateCheck:
    if conn is None:
        return GateCheck(
            "external source run coverage",
            REVIEW,
            "database not checked; pass --check-db with --include-external-run-coverage",
            "scripts/check_external_source_run_coverage.py",
            {},
        )
    run_id = load_latest_fill_first_backtest_run_id(conn)
    inputs = load_external_source_run_coverage_inputs(conn, run_id=run_id) if run_id is not None else None
    report = build_external_source_run_coverage_report(inputs, run_id=run_id)
    status = READY if report.get("status") == READY else REVIEW
    detail = (
        f"run_id={report.get('run_id')} status={report.get('status')} "
        f"order_coverage={report.get('order_state_coverage_pct', 0)}% "
        f"calibration_coverage={report.get('calibration_coverage_pct', 0)}% "
        f"samples={report.get('calibration_sample_count', 0)}"
    )
    return GateCheck("external source run coverage", status, detail, "scripts/check_external_source_run_coverage.py", report)


def _fill_quality_from_run_inputs(inputs: Mapping[str, Any]) -> Mapping[str, Any]:
    run = inputs.get("run") if isinstance(inputs.get("run"), Mapping) else {}
    meta = _json_dict(run.get("meta") if isinstance(run, Mapping) else None)
    data_quality = _json_dict(meta.get("actual_data_quality"))
    return _json_dict(data_quality.get("fill_quality")) or _json_dict(meta.get("fill_quality"))


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _unsupported_filled_evidence_count(orders: Sequence[Mapping[str, Any]]) -> int:
    supported = {"raw_orderfilled", "block_bar_ohlcv_fallback", "lob_depth", "settlement"}
    count = 0
    for order in orders:
        status = str(order.get("status") or "").upper()
        if status not in {"FILLED", "PARTIAL"}:
            continue
        evidence = str(order.get("execution_evidence_type") or "").strip().lower()
        if evidence not in supported:
            count += 1
    return count


def _artifact_report_execution_mode(report: Mapping[str, Any]) -> str:
    reproducibility = report.get("reproducibility_report")
    if isinstance(reproducibility, Mapping):
        snapshot = reproducibility.get("parameter_snapshot")
        if isinstance(snapshot, Mapping):
            params = snapshot.get("parameters")
            if isinstance(params, Mapping):
                return str(params.get("execution_price_mode") or "").strip().upper().replace("-", "_")
    artifacts = report.get("artifacts")
    if isinstance(artifacts, Mapping):
        return str(artifacts.get("execution_price_mode") or "").strip().upper().replace("-", "_")
    return ""


def _artifact_report_is_fill_first(report: Mapping[str, Any]) -> bool:
    mode = _artifact_report_execution_mode(report)
    return mode in {"ORDERFILLED_CROSS", "ORDERFILLED_LIMIT_REPLAY", "LIMIT_REPLAY", "ORDERFILLED"}


def _artifact_report_schema_version(report: Mapping[str, Any]) -> str:
    reproducibility = report.get("reproducibility_report")
    if isinstance(reproducibility, Mapping):
        value = reproducibility.get("artifact_schema_version")
        if value:
            return str(value)
    artifacts = report.get("artifacts")
    if isinstance(artifacts, Mapping):
        value = artifacts.get("artifact_schema_version")
        if value:
            return str(value)
    return ""


def _artifact_report_is_current_fill_first(report: Mapping[str, Any]) -> bool:
    return _artifact_report_is_fill_first(report) and _artifact_report_schema_version(report) == BACKTEST_ARTIFACT_SCHEMA_VERSION


def _external_source_fixture_db_smoke_check(conn: Any | None, *, project_root: Path) -> GateCheck:
    if conn is None:
        return GateCheck(
            "external source fixture DB smoke",
            REVIEW,
            "database not checked; pass --check-db or use CLI --include-external-fixture-db-smoke",
            "scripts/run_fill_first_external_source_fixture_pipeline.py --rollback-smoke",
            {},
        )
    try:
        with tempfile.TemporaryDirectory(prefix="fill-first-external-db-smoke-") as tmp:
            report = run_fill_first_external_source_fixture_pipeline(
                Path(tmp),
                project_root=project_root,
                overwrite=True,
                rollback_smoke=True,
                conn=conn,
            )
    except Exception as exc:  # pragma: no cover - defensive live DB smoke path
        return GateCheck(
            "external source fixture DB smoke",
            FAIL,
            f"rollback smoke raised: {exc}",
            "scripts/run_fill_first_external_source_fixture_pipeline.py --rollback-smoke",
            {},
        )
    status = READY if report.get("status") == READY and report.get("db", {}).get("status") == READY else FAIL
    db = report.get("db") if isinstance(report.get("db"), Mapping) else {}
    counts = db.get("table_counts") if isinstance(db.get("table_counts"), Mapping) else {}
    coverage = report.get("run_coverage") if isinstance(report.get("run_coverage"), Mapping) else {}
    detail = (
        f"db={db.get('status')} orders={counts.get('quant.real_order_state_events', 0)} "
        f"costs={counts.get('quant.real_backtest_cost_events', 0)} "
        f"incidents={counts.get('quant.platform_incidents', 0)} "
        f"signals={counts.get('quant.external_signal_events', 0)} rolled_back={db.get('rolled_back')} "
        f"run_coverage={coverage.get('status', '-')}"
    )
    return GateCheck(
        "external source fixture DB smoke",
        status,
        detail,
        "scripts/run_fill_first_external_source_fixture_pipeline.py --rollback-smoke",
        report,
    )


def _command_check(
    name: str,
    command: Sequence[str],
    *,
    cwd: Path,
    evidence: str,
    command_runner: CommandRunner | None,
) -> GateCheck:
    runner = command_runner or _run_command
    result = runner(command, cwd)
    status = READY if int(result.get("returncode", 1)) == 0 else FAIL
    detail = f"exit={result.get('returncode')}"
    return GateCheck(name, status, detail, evidence, result)


def _run_command(command: Sequence[str], cwd: Path) -> dict[str, Any]:
    completed = subprocess.run(
        list(command),
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    output = completed.stdout or ""
    return {
        "command": list(command),
        "cwd": str(cwd),
        "returncode": completed.returncode,
        "stdout": output,
        "output_tail": output[-4000:],
    }
