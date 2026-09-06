"""Validate, import, and re-check missing external evidence task packs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from quant.backtest.calibration import build_calibration_report, upsert_calibration_orders
from quant.backtest.calibration_samples import build_calibration_samples_from_order_events
from quant.backtest.external_source_run_coverage import (
    build_external_source_run_coverage_report,
    load_external_source_run_coverage_inputs,
)
from quant.backtest.external_source_state import upsert_external_source_import_state
from quant.backtest.order_state import normalize_order_state_event, upsert_real_order_state_events
from quant.backtest.shadow_live_validation import load_shadow_live_event_rows, validate_shadow_live_order_events
from quant.core.schema import create_schema


PIPELINE_SCHEMA_VERSION = "fill_first_missing_external_evidence_pipeline_v1"


def run_missing_external_evidence_task_pack_pipeline(
    *,
    task_pack_dir: str | Path,
    conn: Any | None = None,
    events_path: str | Path | None = None,
    run_id: int | None = None,
    source: str | None = None,
    state_key: str | None = None,
    write: bool = False,
    require_cost_fields: bool = False,
    include_open_events: bool = False,
    skip_init_schema: bool = False,
) -> dict[str, Any]:
    """Run the task-pack path from filled order-state evidence to coverage report."""

    loaded = load_missing_external_evidence_task_pack(task_pack_dir, events_path=events_path, run_id=run_id)
    plan = loaded["plan"]
    rows = loaded["events"]
    resolved_run_id = int(run_id or plan.get("run_id") or 0) or None
    resolved_source = source or _source_from_rows(rows) or plan.get("source") or "live-shadow"
    validation = validate_shadow_live_order_events(rows, require_cost_fields=require_cost_fields)

    report: dict[str, Any] = {
        "schema_version": PIPELINE_SCHEMA_VERSION,
        "status": "review",
        "run_id": resolved_run_id,
        "source": resolved_source,
        "write": bool(write),
        "task_pack": {
            "dir": str(Path(task_pack_dir)),
            "plan_path": str(loaded["plan_path"]),
            "events_path": str(loaded["events_path"]),
            "plan_status": plan.get("status"),
            "missing_order_state_count": plan.get("missing_order_state_count"),
            "missing_calibration_count": plan.get("missing_calibration_count"),
        },
        "validation": validation,
        "import": {"events_written": 0},
        "calibration": {"samples_built": 0, "samples_written": 0},
        "coverage": None,
        "next_actions": [],
    }
    if validation.get("status") == "fail":
        report["status"] = "fail"
        report["reason"] = "validation_failed"
        report["next_actions"] = ["Fill or fix the task-pack event JSONL before importing evidence."]
        return report
    if write and validation.get("calibration_ready_count", 0) <= 0 and not include_open_events:
        report["status"] = "fail"
        report["reason"] = "no_calibration_ready_events"
        report["next_actions"] = ["Add terminal filled/no-fill statuses before writing calibration evidence."]
        return report
    if not write:
        report["status"] = "ready" if validation.get("status") == "ready" else "review"
        report["reason"] = "dry_run_validation_complete"
        report["next_actions"] = _dry_run_next_actions(report)
        return report
    if conn is None:
        raise ValueError("conn is required when write=True")
    if resolved_run_id is None:
        report["status"] = "fail"
        report["reason"] = "missing_run_id"
        report["next_actions"] = ["Provide --run-id or include run_id in the task-pack plan/events."]
        return report

    if not skip_init_schema:
        create_schema(conn)
    normalized_events = [
        normalize_order_state_event(row, source=resolved_source, run_id=resolved_run_id)
        for row in rows
    ]
    events_written = upsert_real_order_state_events(conn, normalized_events)
    if state_key:
        upsert_external_source_import_state(
            conn,
            state_key=state_key,
            source_type="real_order_state_events",
            source=resolved_source,
            endpoint=str(loaded["events_path"]),
            params={"run_id": resolved_run_id, "task_pack_dir": str(Path(task_pack_dir))},
            last_payload_count=len(rows),
            last_rows_written=events_written,
            last_error=None,
        )
    coverage_inputs = load_external_source_run_coverage_inputs(conn, run_id=resolved_run_id)
    orders = [dict(row) for row in (coverage_inputs or {}).get("orders") or []]
    real_events = [dict(row) for row in (coverage_inputs or {}).get("real_order_events") or []]
    samples = build_calibration_samples_from_order_events(
        orders,
        real_events,
        source=f"{resolved_source}-task-pack",
        include_open_events=include_open_events,
    )
    samples_written = upsert_calibration_orders(conn, samples)
    refreshed_inputs = load_external_source_run_coverage_inputs(conn, run_id=resolved_run_id)
    coverage = build_external_source_run_coverage_report(refreshed_inputs, run_id=resolved_run_id)
    calibration_report = build_calibration_report(samples)

    report["import"] = {
        "events_read": len(rows),
        "events_written": events_written,
        "state_key": state_key,
    }
    report["calibration"] = {
        "samples_built": len(samples),
        "samples_written": samples_written,
        "report": calibration_report,
    }
    report["coverage"] = coverage
    report["status"] = coverage.get("status") or "review"
    report["reason"] = "write_complete"
    report["next_actions"] = _write_next_actions(report)
    return report


def load_missing_external_evidence_task_pack(
    task_pack_dir: str | Path,
    *,
    events_path: str | Path | None = None,
    run_id: int | None = None,
) -> dict[str, Any]:
    """Load a task-pack plan and filled/edited event evidence rows."""

    target_dir = Path(task_pack_dir)
    plan_path = _find_task_pack_plan(target_dir, run_id=run_id)
    event_path = Path(events_path) if events_path else _find_task_pack_events(target_dir, run_id=run_id)
    return {
        "plan_path": plan_path,
        "events_path": event_path,
        "plan": _load_json_object(plan_path),
        "events": load_shadow_live_event_rows(event_path),
    }


def missing_external_evidence_pipeline_to_markdown(report: Mapping[str, Any]) -> str:
    task_pack = report.get("task_pack") if isinstance(report.get("task_pack"), Mapping) else {}
    validation = report.get("validation") if isinstance(report.get("validation"), Mapping) else {}
    imported = report.get("import") if isinstance(report.get("import"), Mapping) else {}
    calibration = report.get("calibration") if isinstance(report.get("calibration"), Mapping) else {}
    coverage = report.get("coverage") if isinstance(report.get("coverage"), Mapping) else {}
    lines = [
        f"# Missing Evidence Task Pack Pipeline: {report.get('status')}",
        "",
        f"- schema: {report.get('schema_version')}",
        f"- reason: {report.get('reason')}",
        f"- run_id: {report.get('run_id')}",
        f"- source: {report.get('source')}",
        f"- write: {report.get('write')}",
        f"- plan_path: {task_pack.get('plan_path')}",
        f"- events_path: {task_pack.get('events_path')}",
        "",
        "## Validation",
        f"- status: {validation.get('status')}",
        f"- reason: {validation.get('reason')}",
        f"- events: {validation.get('event_count', 0)}",
        f"- calibration_ready: {validation.get('calibration_ready_count', 0)}",
        f"- errors: {validation.get('error_count', 0)}",
        f"- warnings: {validation.get('warning_count', 0)}",
        "",
        "## Import And Calibration",
        f"- events_written: {imported.get('events_written', 0)}",
        f"- samples_built: {calibration.get('samples_built', 0)}",
        f"- samples_written: {calibration.get('samples_written', 0)}",
    ]
    if coverage:
        lines.extend(
            [
                "",
                "## Coverage",
                f"- status: {coverage.get('status')}",
                f"- reason: {coverage.get('reason')}",
                f"- order_state_coverage_pct: {coverage.get('order_state_coverage_pct')}",
                f"- calibration_coverage_pct: {coverage.get('calibration_coverage_pct')}",
            ]
        )
    actions = list(report.get("next_actions") or [])
    if actions:
        lines.extend(["", "## Next Actions"])
        lines.extend(f"- {action}" for action in actions)
    return "\n".join(lines)


def _find_task_pack_plan(target_dir: Path, *, run_id: int | None) -> Path:
    candidates: list[Path] = []
    if run_id is not None:
        candidates.append(target_dir / f"missing_external_evidence_{int(run_id)}.json")
    candidates.extend(sorted(target_dir.glob("missing_external_evidence_*.json")))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"missing task-pack plan JSON in {target_dir}")


def _find_task_pack_events(target_dir: Path, *, run_id: int | None) -> Path:
    candidates: list[Path] = []
    if run_id is not None:
        candidates.append(target_dir / f"missing_external_evidence_{int(run_id)}.event_templates.jsonl")
    candidates.extend(sorted(target_dir.glob("missing_external_evidence_*.event_templates.jsonl")))
    candidates.extend(sorted(target_dir.glob("*.jsonl")))
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"missing task-pack event JSONL in {target_dir}")


def _load_json_object(path: Path) -> dict[str, Any]:
    parsed = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, Mapping):
        raise ValueError(f"task-pack plan must be a JSON object: {path}")
    return dict(parsed)


def _source_from_rows(rows: list[Mapping[str, Any]]) -> str | None:
    for row in rows:
        source = row.get("source")
        if source not in (None, ""):
            return str(source)
    return None


def _dry_run_next_actions(report: Mapping[str, Any]) -> list[str]:
    validation = report.get("validation") if isinstance(report.get("validation"), Mapping) else {}
    if validation.get("status") == "ready":
        return ["Re-run with --write to import evidence, build calibration samples, and re-check run coverage."]
    return ["Review validation warnings before writing; use --write only after the evidence rows are intentionally filled."]


def _write_next_actions(report: Mapping[str, Any]) -> list[str]:
    coverage = report.get("coverage") if isinstance(report.get("coverage"), Mapping) else {}
    if coverage.get("status") == "ready":
        return ["Re-run artifact audit or promotion gate; external evidence coverage is now ready for this run."]
    return list(coverage.get("next_actions") or ["Review remaining coverage gaps and update the task-pack evidence."])
