"""Parameter search planning for fill-first backtest robustness."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import hashlib
import json
from itertools import product
from pathlib import Path
from typing import Any, Mapping, Sequence


READY = "ready"
REVIEW = "review"
MISSING = "missing"

DEFAULT_GRID: dict[str, tuple[Any, ...]] = {
    "entry_threshold": ("0.56", "0.58", "0.60"),
    "exit_threshold": ("0.40", "0.44"),
    "execution_profile": ("realistic", "conservative"),
    "order_role": ("maker",),
    "liquidity_cap_pct": ("25", "50"),
    "latency_blocks": (0, 1),
}

DEFAULT_EVIDENCE_MODES = ("train", "test", "walk_forward")
REQUIRED_EXECUTION_PROFILES = ("realistic", "conservative")


def build_parameter_search_plan(
    *,
    base_payload: Mapping[str, Any] | None = None,
    grid: Mapping[str, Sequence[Any]] | None = None,
    evidence_modes: Sequence[str] = DEFAULT_EVIDENCE_MODES,
    max_runs: int = 250,
    universe_name: str = "fill_first_parameter_search",
) -> dict[str, Any]:
    """Build a dry-run plan for parameter scans before robustness analysis."""

    base = dict(base_payload or {})
    normalized_grid = _normalize_grid(grid or DEFAULT_GRID)
    if not normalized_grid:
        return {
            "schema_version": "fill_first_parameter_search_plan_v1",
            "status": MISSING,
            "reason": "no parameter grid values supplied",
            "universe_name": universe_name,
            "parameter_fields": [],
            "parameter_set_count": 0,
            "candidate_run_count": 0,
            "planned_run_count": 0,
            "evidence_modes": [],
            "execution_profiles": [],
            "required_execution_profiles": list(REQUIRED_EXECUTION_PROFILES),
            "plan_items": [],
            "robustness_requirements": _robustness_requirements(),
            "next_actions": ["Provide at least one parameter grid field before running a parameter search."],
        }

    modes = _normalize_modes(evidence_modes)
    parameter_sets = _parameter_sets(normalized_grid)
    candidate_count = len(parameter_sets) * max(1, len(modes))
    limited = candidate_count > int(max_runs)
    plan_items = _plan_items(parameter_sets, modes, base_payload=base, max_runs=max_runs, universe_name=universe_name)
    execution_profiles = sorted({str(params.get("execution_profile")) for params in parameter_sets if params.get("execution_profile") is not None})
    review_reasons = _review_reasons(
        parameter_sets=parameter_sets,
        modes=modes,
        execution_profiles=execution_profiles,
        limited=limited,
        max_runs=max_runs,
    )
    status = READY if not review_reasons else REVIEW
    return {
        "schema_version": "fill_first_parameter_search_plan_v1",
        "status": status,
        "reason": "parameter search plan covers robustness inputs" if status == READY else "; ".join(review_reasons),
        "universe_name": universe_name,
        "parameter_fields": list(normalized_grid.keys()),
        "parameter_set_count": len(parameter_sets),
        "candidate_run_count": candidate_count,
        "planned_run_count": len(plan_items),
        "truncated": limited,
        "max_runs": int(max_runs),
        "evidence_modes": modes,
        "execution_profiles": execution_profiles,
        "required_execution_profiles": list(REQUIRED_EXECUTION_PROFILES),
        "plan_items": plan_items,
        "robustness_requirements": _robustness_requirements(),
        "next_actions": _next_actions(status, review_reasons),
    }


def parameter_search_plan_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# Fill-first Parameter Search Plan: {report.get('status')}",
        "",
        f"- universe: {report.get('universe_name') or '-'}",
        f"- parameter_sets: {report.get('parameter_set_count', 0)}",
        f"- planned_runs: {report.get('planned_run_count', 0)} / {report.get('candidate_run_count', 0)}",
        f"- evidence_modes: {', '.join(str(mode) for mode in report.get('evidence_modes', [])) or '-'}",
        f"- execution_profiles: {', '.join(str(profile) for profile in report.get('execution_profiles', [])) or '-'}",
        f"- reason: {report.get('reason') or '-'}",
        "",
        "| Key | Mode | Parameter fingerprint | Parameters |",
        "| --- | --- | --- | --- |",
    ]
    for item in list(report.get("plan_items") or [])[:50]:
        params = item.get("parameters") if isinstance(item, Mapping) else {}
        param_text = ", ".join(f"{key}={value}" for key, value in sorted((params or {}).items()))
        lines.append(
            "| {key} | {mode} | `{fingerprint}` | {params} |".format(
                key=item.get("key") or "",
                mode=item.get("evidence_mode") or "",
                fingerprint=item.get("parameter_fingerprint") or "",
                params=param_text.replace("|", "\\|"),
            )
        )
    requirements = report.get("robustness_requirements") if isinstance(report.get("robustness_requirements"), Mapping) else {}
    lines.extend(
        [
            "",
            "## Robustness Requirements",
            "",
            f"- min_runs: {requirements.get('min_runs')}",
            f"- min_parameter_sets: {requirements.get('min_parameter_sets')}",
            f"- required_evidence: {', '.join(str(item) for item in requirements.get('required_evidence', []))}",
            f"- production_policy: {requirements.get('production_policy')}",
        ]
    )
    lines.extend(["", "## Next Actions"])
    lines.extend(f"- {action}" for action in report.get("next_actions", []))
    return "\n".join(lines)


def load_json_mapping(path: Path | str) -> dict[str, Any]:
    value = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object in {path}")
    return value


def _normalize_grid(grid: Mapping[str, Sequence[Any]]) -> dict[str, list[Any]]:
    normalized: dict[str, list[Any]] = {}
    for field, raw_values in grid.items():
        if isinstance(raw_values, (str, bytes)) or not isinstance(raw_values, Sequence):
            values = [raw_values]
        else:
            values = list(raw_values)
        cleaned = [_normalize_value(value) for value in values if not _is_blank(value)]
        if cleaned:
            normalized[str(field)] = cleaned
    return normalized


def _normalize_modes(modes: Sequence[str]) -> list[str]:
    cleaned: list[str] = []
    for mode in modes:
        text = str(mode or "").strip().lower().replace("-", "_")
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned or ["train", "test"]


def _parameter_sets(grid: Mapping[str, Sequence[Any]]) -> list[dict[str, Any]]:
    fields = list(grid.keys())
    values = [list(grid[field]) for field in fields]
    return [dict(zip(fields, combo)) for combo in product(*values)]


def _plan_items(
    parameter_sets: Sequence[Mapping[str, Any]],
    modes: Sequence[str],
    *,
    base_payload: Mapping[str, Any],
    max_runs: int,
    universe_name: str,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for param_index, params in enumerate(parameter_sets, start=1):
        fingerprint = _fingerprint(params)
        for mode in modes:
            payload = {**base_payload, **params, "evidence_mode": mode, "parameter_fingerprint": fingerprint}
            items.append(
                {
                    "key": f"{universe_name}:{param_index:04d}:{mode}",
                    "parameter_index": param_index,
                    "evidence_mode": mode,
                    "parameter_fingerprint": fingerprint,
                    "parameters": dict(params),
                    "request_payload": payload,
                    "expected_robustness_row_fields": [
                        "net_pnl",
                        "max_drawdown",
                        "fill_rate",
                        "market_category",
                        "liquidity_bucket",
                        "volatility_bucket",
                        "time_to_expiry_bucket",
                    ],
                }
            )
            if len(items) >= int(max_runs):
                return items
    return items


def _review_reasons(
    *,
    parameter_sets: Sequence[Mapping[str, Any]],
    modes: Sequence[str],
    execution_profiles: Sequence[str],
    limited: bool,
    max_runs: int,
) -> list[str]:
    reasons: list[str] = []
    if len(parameter_sets) < 3:
        reasons.append("fewer than 3 parameter sets")
    missing_profiles = [profile for profile in REQUIRED_EXECUTION_PROFILES if profile not in set(execution_profiles)]
    if missing_profiles:
        reasons.append("missing required execution profiles: " + ", ".join(missing_profiles))
    if not ({"train", "test"} <= set(modes) or "walk_forward" in set(modes)):
        reasons.append("missing train/test or walk-forward evidence mode")
    if limited:
        reasons.append(f"candidate runs exceed max_runs={max_runs}; plan was truncated")
    return reasons


def _robustness_requirements() -> dict[str, Any]:
    return {
        "min_runs": 5,
        "min_parameter_sets": 3,
        "required_execution_profiles": list(REQUIRED_EXECUTION_PROFILES),
        "required_evidence": ["train/test split", "walk-forward batch"],
        "report": "quant.backtest.parameter_robustness.build_parameter_robustness_report",
        "production_policy": "do_not_promote_best_only until robustness_verdict is ready",
    }


def _next_actions(status: str, review_reasons: Sequence[str]) -> list[str]:
    if status == READY:
        return [
            "Run the planned parameter jobs, persist rows with parameter_fingerprint and evidence_mode, then build Parameter Robustness Report.",
            "Review best, median, worst-decile, sensitivity, and regime split before staging parameters.",
        ]
    actions = ["Do not promote best-only parameters from this plan."]
    actions.extend(f"Fix plan gap: {reason}" for reason in review_reasons)
    return actions


def _fingerprint(params: Mapping[str, Any]) -> str:
    encoded = json.dumps(params, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def _normalize_value(value: Any) -> Any:
    if isinstance(value, bool) or value is None:
        return value
    text = str(value).strip()
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError):
        return text
    if parsed == parsed.to_integral_value():
        return int(parsed)
    return format(parsed.normalize(), "f")


def _is_blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""
