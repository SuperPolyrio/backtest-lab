"""Batch runner for fill-first parameter search plans."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from quant.api.read_api import get_backtest_metrics
from quant.backtest.backtest_engine import create_and_execute_backtest
from quant.backtest.parameter_search_plan import build_parameter_search_plan, load_json_mapping
from quant.backtest.parameter_search_results import (
    READY,
    build_parameter_search_results_report,
)
from quant.backtest.production_parameter_staging import (
    normalize_production_parameter_staging,
    upsert_production_parameter_staging,
)


REVIEW = "review"
FAIL = "fail"
MISSING = "missing"
SCHEMA_VERSION = "fill_first_parameter_search_batch_v1"

Executor = Callable[[Mapping[str, Any]], Mapping[str, Any]]


def run_parameter_search_plan(
    plan: Mapping[str, Any],
    *,
    conn: Any | None = None,
    executor: Executor | None = None,
    dry_run: bool = True,
    max_runs: int | None = None,
    stop_on_error: bool = False,
    stage_parameters: bool = False,
    write_staging: bool = False,
    staging_status: str = "pending",
    staging_source: str = "parameter-search-batch-runner",
    approved_by: str | None = None,
    force_review_staging: bool = False,
    strategy_name: str = "unknown",
    strategy_version: str = "unknown",
    universe_name: str | None = None,
) -> dict[str, Any]:
    """Execute or preview a parameter search plan and build result/staging reports."""

    normalized_plan = dict(plan or {})
    plan_items = [_as_mapping(item) for item in normalized_plan.get("plan_items") or []]
    if max_runs is not None:
        plan_items = plan_items[: max(0, int(max_runs))]

    run_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    executed = 0
    succeeded = 0

    for index, item in enumerate(plan_items, start=1):
        payload = _payload_for_item(item)
        if dry_run:
            run_rows.append(_planned_row(item, index=index, payload=payload))
            continue
        try:
            result = _execute_payload(payload, conn=conn, executor=executor)
            executed += 1
            row = normalize_parameter_search_execution_result(item, result, index=index, conn=conn)
            run_rows.append(row)
            if str(row.get("status") or "").lower() not in {"failed", "error"}:
                succeeded += 1
        except Exception as exc:
            executed += 1
            error_row = _error_row(item, index=index, payload=payload, error=exc)
            run_rows.append(error_row)
            errors.append(error_row)
            if stop_on_error:
                break

    scored_rows = [row for row in run_rows if str(row.get("status") or "") != "planned"]
    results_report = build_parameter_search_results_report(normalized_plan, scored_rows)
    staging_preview = _staging_preview(
        results_report,
        enabled=stage_parameters,
        status=staging_status,
        source=staging_source,
        approved_by=approved_by,
        force_review=force_review_staging,
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        universe_name=universe_name or str(normalized_plan.get("universe_name") or ""),
    )
    written_staging_count = 0
    if write_staging:
        if not stage_parameters:
            raise ValueError("--write-staging requires stage_parameters=True")
        if dry_run:
            raise ValueError("--write-staging requires dry_run=False")
        if conn is None:
            raise ValueError("--write-staging requires conn")
        written_staging_count = upsert_production_parameter_staging(
            conn,
            [
                {
                    **results_report,
                    "strategy_name": strategy_name,
                    "strategy_version": strategy_version,
                    "universe_name": universe_name or str(normalized_plan.get("universe_name") or ""),
                }
            ],
            status=staging_status,
            source=staging_source,
            approved_by=approved_by,
            force_review=force_review_staging,
        )

    status = _batch_status(dry_run=dry_run, errors=errors, results_report=results_report)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "reason": _batch_reason(status, dry_run=dry_run, errors=errors, results_report=results_report),
        "dry_run": bool(dry_run),
        "planned_run_count": len(plan_items),
        "executed_run_count": executed,
        "succeeded_run_count": succeeded,
        "failed_run_count": len(errors),
        "result_row_count": len(scored_rows),
        "plan": normalized_plan,
        "rows": run_rows,
        "parameter_search_results": results_report,
        "staging_preview": staging_preview,
        "written_staging_count": written_staging_count,
        "next_actions": _next_actions(status, dry_run=dry_run, errors=errors, results_report=results_report, stage_parameters=stage_parameters),
    }


def normalize_parameter_search_execution_result(
    plan_item: Mapping[str, Any],
    result: Mapping[str, Any],
    *,
    index: int = 1,
    conn: Any | None = None,
) -> dict[str, Any]:
    """Convert one executed run into a parameter robustness row."""

    item = _as_mapping(plan_item)
    plain = _as_mapping(_as_plain(result))
    payload = _payload_for_item(item)
    run_id = _first_text(plain, "run_id", "runId", "id") or _first_text(payload, "run_id", "runId")
    metrics = _extract_metrics(plain)
    if conn is not None and run_id and not metrics:
        try:
            metrics = get_backtest_metrics(conn, run_id=int(run_id))
        except Exception:
            metrics = []
    metrics_map = _metrics_map(metrics)
    parameters = {**_as_mapping(item.get("parameters")), **_first_mapping(plain, "parameters", "parameter_snapshot", "parameterSnapshot")}
    context = _first_mapping(plain, "context", "meta", "execution_context", "executionContext")
    meta_context = _first_mapping(context, "execution_context", "executionContext")
    combined = {
        **payload,
        **parameters,
        **context,
        **meta_context,
        **plain,
        **metrics_map,
    }
    return {
        "run_id": run_id or f"row-{index}",
        "key": item.get("key") or f"planned-{index}",
        "parameter_index": item.get("parameter_index"),
        "parameter_fingerprint": item.get("parameter_fingerprint") or combined.get("parameter_fingerprint"),
        "parameters": parameters or _as_mapping(combined.get("parameters")),
        "mode": item.get("evidence_mode") or combined.get("evidence_mode") or combined.get("mode"),
        "evidence_mode": item.get("evidence_mode") or combined.get("evidence_mode") or combined.get("mode"),
        "status": str(combined.get("status") or "succeeded"),
        "market_slug": _first_text(combined, "market_slug", "marketSlug"),
        "market_category": _value_or_default(combined, "market_category", "marketCategory", default="unknown"),
        "liquidity_bucket": _value_or_default(combined, "liquidity_bucket", "liquidityBucket", default="unknown"),
        "volatility_bucket": _value_or_default(combined, "volatility_bucket", "volatilityBucket", default="unknown"),
        "time_to_expiry_bucket": _value_or_default(combined, "time_to_expiry_bucket", "timeToExpiryBucket", default="unknown"),
        "final_minute": _value_or_default(combined, "final_minute", "finalMinute", default="unknown"),
        "event_outcome_count_bucket": _value_or_default(
            combined,
            "event_outcome_count_bucket",
            "eventOutcomeCountBucket",
            default="unknown",
        ),
        "net_pnl": _number_text(_first_present(combined, "net_pnl", "netPnl", "net_profit", "netProfit", "total_pnl", "totalPnl", "pnl", "settlement_pnl")),
        "performance_score": _number_text(_first_present(combined, "performance_score", "performanceScore", "score", "objective")),
        "max_drawdown": _number_text(_first_present(combined, "max_drawdown", "maxDrawdown", "drawdown", "drawdownPnl")),
        "fill_rate": _fill_rate(combined),
        "sample_count": _sample_count(combined),
        "metrics": metrics,
        "payload": payload,
        "source": plain,
    }


def parameter_search_batch_to_markdown(report: Mapping[str, Any]) -> str:
    results = _as_mapping(report.get("parameter_search_results"))
    staging = _as_mapping(report.get("staging_preview"))
    lines = [
        f"# Fill-first Parameter Search Batch: {report.get('status')}",
        "",
        f"- dry_run: {report.get('dry_run')}",
        f"- planned_runs: {report.get('planned_run_count', 0)}",
        f"- executed_runs: {report.get('executed_run_count', 0)}",
        f"- succeeded_runs: {report.get('succeeded_run_count', 0)}",
        f"- failed_runs: {report.get('failed_run_count', 0)}",
        f"- result_coverage: {results.get('coverage_pct', '0')}%",
        f"- result_status: {results.get('status') or '-'}",
        f"- staging_allowed: {staging.get('staging_allowed', False)}",
        f"- reason: {report.get('reason') or '-'}",
        "",
        "## Next Actions",
    ]
    actions = list(report.get("next_actions") or [])
    lines.extend(f"- {action}" for action in actions) if actions else lines.append("- none")
    return "\n".join(lines)


def load_parameter_search_plan_or_build(
    *,
    plan_json: Path | str | None = None,
    base_payload_json: Path | str | None = None,
    grid_json: Path | str | None = None,
    evidence_modes: Sequence[str] | None = None,
    max_runs: int = 250,
    universe_name: str = "fill_first_parameter_search",
) -> dict[str, Any]:
    if plan_json:
        value = json.loads(Path(plan_json).expanduser().read_text(encoding="utf-8"))
        if not isinstance(value, Mapping):
            raise ValueError("plan JSON must be an object")
        return dict(value.get("plan") if isinstance(value.get("plan"), Mapping) else value)
    base_payload = load_json_mapping(base_payload_json) if base_payload_json else {}
    grid = load_json_mapping(grid_json) if grid_json else None
    return build_parameter_search_plan(
        base_payload=base_payload,
        grid=grid,
        evidence_modes=evidence_modes or ("train", "test", "walk_forward"),
        max_runs=max_runs,
        universe_name=universe_name,
    )


def _execute_payload(payload: Mapping[str, Any], *, conn: Any | None, executor: Executor | None) -> Mapping[str, Any]:
    if executor is not None:
        return _as_mapping(executor(payload))
    if conn is None:
        raise ValueError("conn is required when executor is not provided")
    return _as_mapping(create_and_execute_backtest(conn, dict(payload)))


def _payload_for_item(item: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(_as_mapping(item.get("request_payload")))
    payload["parameter_fingerprint"] = item.get("parameter_fingerprint") or payload.get("parameter_fingerprint")
    payload["evidence_mode"] = item.get("evidence_mode") or payload.get("evidence_mode")
    payload.setdefault("execution_context", {})
    if isinstance(payload["execution_context"], Mapping):
        payload["execution_context"] = {
            **dict(payload["execution_context"]),
            "parameter_search_key": item.get("key"),
            "parameter_index": item.get("parameter_index"),
            "evidence_mode": item.get("evidence_mode"),
            "parameter_fingerprint": item.get("parameter_fingerprint"),
        }
    return payload


def _planned_row(item: Mapping[str, Any], *, index: int, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "run_id": f"planned-{index}",
        "key": item.get("key") or f"planned-{index}",
        "parameter_index": item.get("parameter_index"),
        "parameter_fingerprint": item.get("parameter_fingerprint"),
        "parameters": dict(_as_mapping(item.get("parameters"))),
        "mode": item.get("evidence_mode"),
        "evidence_mode": item.get("evidence_mode"),
        "status": "planned",
        "payload": dict(payload),
    }


def _error_row(item: Mapping[str, Any], *, index: int, payload: Mapping[str, Any], error: Exception) -> dict[str, Any]:
    row = _planned_row(item, index=index, payload=payload)
    row.update({"status": "failed", "error": str(error)})
    return row


def _staging_preview(
    report: Mapping[str, Any],
    *,
    enabled: bool,
    status: str,
    source: str,
    approved_by: str | None,
    force_review: bool,
    strategy_name: str,
    strategy_version: str,
    universe_name: str,
) -> dict[str, Any]:
    if not enabled:
        return {"enabled": False, "staging_allowed": False, "reason": "stage_parameters is disabled"}
    try:
        return {
            "enabled": True,
            **normalize_production_parameter_staging(
                report,
                status=status,
                source=source,
                approved_by=approved_by,
                force_review=force_review,
                strategy_name=strategy_name,
                strategy_version=strategy_version,
                universe_name=universe_name,
            ),
        }
    except Exception as exc:
        return {
            "enabled": True,
            "staging_allowed": False,
            "default_action": "do_not_stage",
            "blocked_reasons": [str(exc)],
            "reason": str(exc),
        }


def _batch_status(*, dry_run: bool, errors: Sequence[Mapping[str, Any]], results_report: Mapping[str, Any]) -> str:
    if errors:
        return FAIL
    if dry_run:
        return REVIEW
    return READY if results_report.get("status") == READY else REVIEW


def _batch_reason(status: str, *, dry_run: bool, errors: Sequence[Mapping[str, Any]], results_report: Mapping[str, Any]) -> str:
    if errors:
        return f"{len(errors)} parameter search runs failed"
    if dry_run:
        return "dry-run preview only; no parameter jobs were executed"
    if status == READY:
        return "parameter search batch executed and result coverage is ready"
    return str(results_report.get("reason") or "parameter search results require review")


def _next_actions(
    status: str,
    *,
    dry_run: bool,
    errors: Sequence[Mapping[str, Any]],
    results_report: Mapping[str, Any],
    stage_parameters: bool,
) -> list[str]:
    if dry_run:
        return ["Run again with --execute after reviewing planned payloads."]
    if errors:
        return ["Fix failed parameter jobs and re-run the same plan before reading robustness."]
    if results_report.get("status") != READY:
        return [str(action) for action in results_report.get("next_actions", [])[:5]]
    if not stage_parameters:
        return ["Run with --stage-parameters to preview the production staging row."]
    return ["Review staging_preview before approving or writing production parameters."]


def _extract_metrics(result: Mapping[str, Any]) -> list[dict[str, Any]]:
    metrics = result.get("metrics")
    if isinstance(metrics, list):
        return [_as_mapping(row) for row in metrics if isinstance(row, Mapping)]
    summary = result.get("summary")
    if isinstance(summary, Mapping) and isinstance(summary.get("metrics"), list):
        return [_as_mapping(row) for row in summary.get("metrics") if isinstance(row, Mapping)]
    return []


def _metrics_map(metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for row in metrics:
        key = _first_text(row, "metric_key", "metricKey", "key")
        if not key:
            continue
        value = _first_present(row, "value", "metric_value", "metricValue", "formatted_value", "formattedValue")
        output[key] = value
    return output


def _fill_rate(combined: Mapping[str, Any]) -> str:
    explicit = _first_present(combined, "fill_rate", "fillRate", "liquidity_fill_rate", "fill_quality_fill_rate")
    if explicit is not None:
        return _number_text(_ratioish(explicit))
    filled = _decimal(_first_present(combined, "filled_count", "filledCount", "total_trades", "trades"))
    submitted = _decimal(_first_present(combined, "submitted_count", "submittedCount", "signal_count", "signalCount", "submitted_orders"))
    if submitted > 0:
        return _number_text(filled / submitted)
    return "0"


def _sample_count(combined: Mapping[str, Any]) -> int:
    value = _first_present(combined, "sample_count", "sampleCount", "total_trades", "trades", "signal_count", "signalCount", "submitted_orders")
    try:
        return int(Decimal(str(value or 0)))
    except Exception:
        return 0


def _value_or_default(mapping: Mapping[str, Any], *keys: str, default: str) -> Any:
    value = _first_present(mapping, *keys)
    return default if _is_blank(value) else value


def _first_mapping(mapping: Mapping[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if not _is_blank(value):
            return value
    return None


def _first_text(mapping: Mapping[str, Any], *keys: str) -> str:
    value = _first_present(mapping, *keys)
    return "" if value is None else str(value)


def _number_text(value: Any) -> str:
    try:
        dec = Decimal(str(value if value is not None else "0"))
    except (InvalidOperation, ValueError):
        dec = Decimal("0")
    text = format(dec.normalize(), "f")
    return "0" if text == "-0" else text


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value if value is not None else "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _ratioish(value: Any) -> Decimal:
    dec = _decimal(value)
    return dec / Decimal("100") if dec > Decimal("1") else dec


def _is_blank(value: Any) -> bool:
    return value is None or value == ""


def _as_plain(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    return value


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}
