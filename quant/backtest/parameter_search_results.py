"""Result coverage checks for fill-first parameter search plans."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from quant.backtest.parameter_robustness import build_parameter_robustness_report
from quant.backtest.performance_score_validation import build_performance_score_validation_report
from quant.backtest.regime_coverage import build_regime_coverage_report


READY = "ready"
REVIEW = "review"
MISSING = "missing"


def build_parameter_search_results_report(
    plan: Mapping[str, Any] | None,
    rows: Sequence[Any],
    *,
    min_result_coverage_pct: Decimal | int | str = Decimal("100"),
) -> dict[str, Any]:
    """Check that planned parameter jobs produced usable robustness rows."""

    normalized_plan = _as_mapping(plan)
    plan_items = [_normalize_plan_item(item, index=index) for index, item in enumerate(normalized_plan.get("plan_items") or [], start=1)]
    result_rows = [_normalize_result_row(row, index=index) for index, row in enumerate(rows or [], start=1)]
    planned_keys = {item["match_key"]: item for item in plan_items if item["match_key"]}
    result_keys: dict[str, list[dict[str, Any]]] = {}
    for row in result_rows:
        if row["match_key"]:
            result_keys.setdefault(row["match_key"], []).append(row)

    covered_keys = sorted(key for key in planned_keys if key in result_keys)
    missing_items = [planned_keys[key] for key in sorted(planned_keys) if key not in result_keys]
    duplicate_results = [row for bucket in result_keys.values() if len(bucket) > 1 for row in bucket[1:]]
    unplanned_results = [row for row in result_rows if row["match_key"] not in planned_keys]
    required_field_report = _required_field_report(plan_items, result_rows)
    coverage_pct = _pct(len(covered_keys), len(planned_keys))
    source_rows = [row["source"] for row in result_rows]
    robustness_report = build_parameter_robustness_report(source_rows)
    regime_coverage_report = build_regime_coverage_report(source_rows)
    performance_score_validation_report = build_performance_score_validation_report(source_rows)
    robustness_verdict = str(robustness_report.get("robustness_verdict") or MISSING)
    regime_coverage_verdict = str(regime_coverage_report.get("coverage_verdict") or MISSING)
    score_bias_verdict = str(performance_score_validation_report.get("score_bias_verdict") or MISSING)
    coverage_ready = bool(planned_keys) and coverage_pct >= _decimal(min_result_coverage_pct)
    fields_ready = not required_field_report["rows_missing_required_fields"]
    robustness_ready = robustness_verdict == READY
    regime_coverage_ready = regime_coverage_verdict == READY
    score_bias_ready = score_bias_verdict == READY
    staging = _production_parameter_staging(
        coverage_ready=coverage_ready,
        fields_ready=fields_ready,
        robustness_report=robustness_report,
        regime_coverage_report=regime_coverage_report,
        performance_score_validation_report=performance_score_validation_report,
        missing_items=missing_items,
        unplanned_results=unplanned_results,
    )

    if not planned_keys:
        status = MISSING
        reason = "no planned parameter search items supplied"
    elif not result_rows:
        status = MISSING
        reason = "no parameter search result rows supplied"
    elif coverage_ready and fields_ready and robustness_ready and regime_coverage_ready and score_bias_ready:
        status = READY
        reason = "planned parameter search results cover all runs and pass robustness, regime coverage, and score bias gates"
    else:
        status = REVIEW
        reason = "; ".join(
            _review_reasons(
                coverage_ready,
                fields_ready,
                robustness_report,
                regime_coverage_report,
                performance_score_validation_report,
                missing_items,
                unplanned_results,
            )
        )

    return {
        "schema_version": "fill_first_parameter_search_results_v1",
        "status": status,
        "reason": reason,
        "planned_run_count": len(planned_keys),
        "result_row_count": len(result_rows),
        "covered_run_count": len(covered_keys),
        "missing_run_count": len(missing_items),
        "coverage_pct": _decimal_text(coverage_pct),
        "duplicate_result_count": len(duplicate_results),
        "unplanned_result_count": len(unplanned_results),
        "missing_items": [_public_plan_item(item) for item in missing_items],
        "duplicate_results": [_public_result_row(row) for row in duplicate_results[:50]],
        "unplanned_results": [_public_result_row(row) for row in unplanned_results[:50]],
        "required_field_report": required_field_report,
        "robustness_report": robustness_report,
        "regime_coverage_report": regime_coverage_report,
        "performance_score_validation_report": performance_score_validation_report,
        "production_parameter_staging": staging,
        "next_actions": _next_actions(
            status,
            missing_items,
            unplanned_results,
            required_field_report,
            robustness_report,
            regime_coverage_report,
            performance_score_validation_report,
            staging,
        ),
    }


def parameter_search_results_to_markdown(report: Mapping[str, Any]) -> str:
    robustness = _as_mapping(report.get("robustness_report"))
    regime_coverage = _as_mapping(report.get("regime_coverage_report"))
    score_validation = _as_mapping(report.get("performance_score_validation_report"))
    staging = _as_mapping(report.get("production_parameter_staging"))
    field_report = _as_mapping(report.get("required_field_report"))
    lines = [
        f"# Fill-first Parameter Search Results: {report.get('status')}",
        "",
        f"- planned_runs: {report.get('planned_run_count', 0)}",
        f"- result_rows: {report.get('result_row_count', 0)}",
        f"- coverage: {report.get('coverage_pct', '0')}%",
        f"- missing_runs: {report.get('missing_run_count', 0)}",
        f"- duplicates: {report.get('duplicate_result_count', 0)}",
        f"- unplanned_results: {report.get('unplanned_result_count', 0)}",
        f"- robustness_verdict: {robustness.get('robustness_verdict') or '-'}",
        f"- regime_coverage_verdict: {regime_coverage.get('coverage_verdict') or '-'}",
        f"- score_bias_verdict: {score_validation.get('score_bias_verdict') or '-'}",
        f"- strategy_scope: {regime_coverage.get('strategy_scope') or '-'}",
        f"- staging_allowed: {staging.get('staging_allowed')}",
        f"- reason: {report.get('reason') or '-'}",
        "",
        "## Field Coverage",
        "",
        f"- required_fields: {', '.join(str(field) for field in field_report.get('required_fields', [])) or '-'}",
        f"- rows_missing_required_fields: {len(field_report.get('rows_missing_required_fields') or [])}",
        "",
        "## Missing Planned Runs",
        "",
    ]
    missing_items = list(report.get("missing_items") or [])[:50]
    if missing_items:
        lines.extend("- `{}` mode={} fingerprint={}".format(item.get("key"), item.get("evidence_mode"), item.get("parameter_fingerprint")) for item in missing_items)
    else:
        lines.append("- none")
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions", []))
    return "\n".join(lines)


def load_json_rows(path: Path | str) -> list[Any]:
    value = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if isinstance(value, list):
        return value
    if isinstance(value, Mapping):
        for key in ("rows", "results", "result_rows", "benchmark_rows", "items"):
            rows = value.get(key)
            if isinstance(rows, list):
                return rows
    raise ValueError(f"expected JSON list or object with rows/results in {path}")


def load_json_report(path: Path | str) -> dict[str, Any]:
    value = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object in {path}")
    if isinstance(value.get("plan"), Mapping):
        return dict(value["plan"])
    return dict(value)


def _normalize_plan_item(item: Any, *, index: int) -> dict[str, Any]:
    plain = _as_mapping(_as_plain(item))
    fingerprint = _first_text(plain, "parameter_fingerprint", "parameterFingerprint")
    mode = _normalize_mode(_first_text(plain, "evidence_mode", "evidenceMode", "mode", "phase"))
    key = _first_text(plain, "key") or f"planned-{index}"
    return {
        "source": plain,
        "key": key,
        "parameter_fingerprint": fingerprint,
        "evidence_mode": mode,
        "match_key": _match_key(fingerprint, mode),
        "parameters": _as_mapping(plain.get("parameters")),
        "expected_robustness_row_fields": list(plain.get("expected_robustness_row_fields") or []),
    }


def _normalize_result_row(row: Any, *, index: int) -> dict[str, Any]:
    plain = _as_mapping(_as_plain(row))
    payload = _as_mapping(plain.get("payload"))
    parameters = _first_mapping(plain, payload, "parameters", "parameter_snapshot", "parameterSnapshot", "strategy_parameters", "strategyParameters")
    context = _first_mapping(plain, payload, "context", "meta", "execution_context", "executionContext")
    combined = {**payload, **context, **parameters, **plain}
    fingerprint = _first_text(combined, "parameter_fingerprint", "parameterFingerprint")
    mode = _normalize_mode(_first_text(combined, "evidence_mode", "evidenceMode", "split", "sample", "phase", "mode", "run_type", "runType"))
    return {
        "source": plain,
        "row_index": index,
        "run_id": _first_text(combined, "run_id", "runId", "benchmark_id", "benchmarkId", "key") or f"row-{index}",
        "parameter_fingerprint": fingerprint,
        "evidence_mode": mode,
        "match_key": _match_key(fingerprint, mode),
        "combined": combined,
    }


def _required_field_report(plan_items: Sequence[Mapping[str, Any]], rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    required_fields = sorted(
        {
            str(field)
            for item in plan_items
            for field in (item.get("expected_robustness_row_fields") or [])
            if not _is_blank(field)
        }
    )
    missing_rows: list[dict[str, Any]] = []
    if required_fields:
        for row in rows:
            missing = [field for field in required_fields if _is_blank(_field_value(row.get("combined") or {}, field))]
            if missing:
                missing_rows.append(
                    {
                        "row_index": row.get("row_index"),
                        "run_id": row.get("run_id"),
                        "parameter_fingerprint": row.get("parameter_fingerprint"),
                        "evidence_mode": row.get("evidence_mode"),
                        "missing_fields": missing,
                    }
                )
    return {
        "required_fields": required_fields,
        "rows_checked": len(rows),
        "rows_missing_required_fields": missing_rows[:50],
        "rows_missing_required_field_count": len(missing_rows),
    }


def _production_parameter_staging(
    *,
    coverage_ready: bool,
    fields_ready: bool,
    robustness_report: Mapping[str, Any],
    regime_coverage_report: Mapping[str, Any],
    performance_score_validation_report: Mapping[str, Any],
    missing_items: Sequence[Mapping[str, Any]],
    unplanned_results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    policy = _as_mapping(robustness_report.get("production_parameter_policy"))
    best_run = _as_mapping(robustness_report.get("best_run"))
    robustness_allowed = bool(policy.get("best_parameter_promotion_allowed"))
    regime_coverage_verdict = str(regime_coverage_report.get("coverage_verdict") or MISSING)
    regime_coverage_ready = regime_coverage_verdict == READY
    score_bias_verdict = str(performance_score_validation_report.get("score_bias_verdict") or MISSING)
    score_bias_ready = score_bias_verdict == READY
    allowed = coverage_ready and fields_ready and robustness_allowed and regime_coverage_ready and score_bias_ready and not missing_items and not unplanned_results
    blockers: list[str] = []
    if not coverage_ready or missing_items:
        blockers.append("planned parameter runs are missing result rows")
    if not fields_ready:
        blockers.append("some result rows are missing required robustness fields")
    if not robustness_allowed:
        blockers.append(str(policy.get("reason") or "parameter robustness gate blocks promotion"))
    if not regime_coverage_ready:
        blockers.append(str(regime_coverage_report.get("reason") or "regime coverage gate blocks production staging"))
    if not score_bias_ready:
        blockers.append(str(performance_score_validation_report.get("reason") or "performance score bias gate blocks production staging"))
    if unplanned_results:
        blockers.append("unplanned result rows must be reviewed before staging")
    return {
        "staging_allowed": allowed,
        "default_action": "stage_after_human_review" if allowed else "do_not_stage",
        "best_parameter_fingerprint": best_run.get("parameter_fingerprint"),
        "best_parameters": best_run.get("parameters") or {},
        "requires_human_review": True,
        "blocked_reasons": blockers,
        "robustness_policy": policy,
        "regime_coverage_verdict": regime_coverage_verdict,
        "strategy_scope": regime_coverage_report.get("strategy_scope"),
        "regime_specific": bool(regime_coverage_report.get("regime_specific")),
        "regime_coverage_report": dict(regime_coverage_report),
        "score_bias_verdict": score_bias_verdict,
        "performance_score_validation_report": dict(performance_score_validation_report),
    }


def _review_reasons(
    coverage_ready: bool,
    fields_ready: bool,
    robustness_report: Mapping[str, Any],
    regime_coverage_report: Mapping[str, Any],
    performance_score_validation_report: Mapping[str, Any],
    missing_items: Sequence[Mapping[str, Any]],
    unplanned_results: Sequence[Mapping[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    if not coverage_ready:
        reasons.append(f"{len(missing_items)} planned parameter runs are missing result rows")
    if not fields_ready:
        reasons.append("some result rows are missing required robustness fields")
    if robustness_report.get("robustness_verdict") != READY:
        reasons.append(str(robustness_report.get("reason") or "parameter robustness verdict is not ready"))
    if regime_coverage_report.get("coverage_verdict") != READY:
        reasons.append(str(regime_coverage_report.get("reason") or "regime coverage verdict is not ready"))
    if performance_score_validation_report.get("score_bias_verdict") != READY:
        reasons.append(str(performance_score_validation_report.get("reason") or "performance score bias verdict is not ready"))
    if unplanned_results:
        reasons.append(f"{len(unplanned_results)} unplanned result rows need review")
    return reasons or ["parameter search results require review"]


def _next_actions(
    status: str,
    missing_items: Sequence[Mapping[str, Any]],
    unplanned_results: Sequence[Mapping[str, Any]],
    field_report: Mapping[str, Any],
    robustness_report: Mapping[str, Any],
    regime_coverage_report: Mapping[str, Any],
    performance_score_validation_report: Mapping[str, Any],
    staging: Mapping[str, Any],
) -> list[str]:
    if status == READY:
        return ["Review the staged best/median/worst-decile evidence before approving production parameters."]
    actions: list[str] = []
    if missing_items:
        actions.append("Run or re-run the missing planned parameter jobs before reading robustness as complete.")
    if unplanned_results:
        actions.append("Either attach unplanned result rows to the parameter plan or exclude them from this report.")
    if field_report.get("rows_missing_required_fields"):
        actions.append("Re-export result rows with net_pnl, max_drawdown, fill_rate, and regime fields.")
    if robustness_report.get("robustness_verdict") != READY:
        actions.extend(str(action) for action in robustness_report.get("next_actions", [])[:3])
    if regime_coverage_report.get("coverage_verdict") != READY:
        actions.extend(str(action) for action in regime_coverage_report.get("next_actions", [])[:3])
    if performance_score_validation_report.get("score_bias_verdict") != READY:
        actions.extend(str(action) for action in performance_score_validation_report.get("next_actions", [])[:3])
    for reason in staging.get("blocked_reasons", [])[:3]:
        actions.append(f"Do not stage parameters: {reason}")
    return actions or ["Review parameter result coverage before staging parameters."]


def _public_plan_item(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "key": item.get("key"),
        "parameter_fingerprint": item.get("parameter_fingerprint"),
        "evidence_mode": item.get("evidence_mode"),
        "parameters": item.get("parameters") or {},
    }


def _public_result_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "row_index": row.get("row_index"),
        "run_id": row.get("run_id"),
        "parameter_fingerprint": row.get("parameter_fingerprint"),
        "evidence_mode": row.get("evidence_mode"),
    }


def _match_key(fingerprint: str | None, mode: str | None) -> str:
    if _is_blank(fingerprint) or _is_blank(mode):
        return ""
    return f"{fingerprint}::{mode}"


def _normalize_mode(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_")
    if "walk" in raw or raw == "wf":
        return "walk_forward"
    if "train" in raw:
        return "train"
    if "test" in raw or "holdout" in raw:
        return "test"
    if "split" in raw:
        return "split"
    return raw or "unsplit"


def _first_mapping(*items: Any) -> dict[str, Any]:
    sources = [item for item in items if isinstance(item, Mapping)]
    keys = [str(item) for item in items if not isinstance(item, Mapping)]
    for source in sources:
        for key in keys:
            value = source.get(key)
            if isinstance(value, Mapping):
                return dict(value)
    return {}


def _field_value(row: Mapping[str, Any], field: str) -> Any:
    if field in row:
        return row.get(field)
    payload = _as_mapping(row.get("payload"))
    if field in payload:
        return payload.get(field)
    parameters = _as_mapping(row.get("parameters"))
    if field in parameters:
        return parameters.get(field)
    context = _as_mapping(row.get("context"))
    return context.get(field)


def _as_plain(value: Any) -> Any:
    if is_dataclass(value):
        return _as_plain(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _as_plain(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_as_plain(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    return value


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first_text(row: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if not _is_blank(value):
            return str(value)
    return None


def _pct(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return Decimal(numerator) * Decimal("100") / Decimal(denominator)


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _decimal_text(value: Decimal | int | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else _decimal(value)
    return format(decimal_value.quantize(Decimal("0.01")).normalize(), "f")


def _is_blank(value: Any) -> bool:
    return value is None or value == "" or value == {}
