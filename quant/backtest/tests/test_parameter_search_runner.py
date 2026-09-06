import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from quant.backtest.parameter_search_plan import READY, build_parameter_search_plan
from quant.backtest.parameter_search_runner import (
    normalize_parameter_search_execution_result,
    run_parameter_search_plan,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "scripts" / "run_fill_first_parameter_search_batch.py"


def _small_plan() -> dict[str, Any]:
    return build_parameter_search_plan(
        grid={
            "entry_threshold": ["0.56", "0.58", "0.60"],
            "execution_profile": ["realistic", "conservative"],
        },
        evidence_modes=["train", "test", "walk_forward"],
        max_runs=50,
    )


def _fake_executor(payload: Mapping[str, Any]) -> dict[str, Any]:
    threshold = str(payload.get("entry_threshold"))
    profile = str(payload.get("execution_profile"))
    mode = str(payload.get("evidence_mode"))
    threshold_decimal = Decimal(threshold)
    threshold_score = {Decimal("0.56"): "4", Decimal("0.58"): "7", Decimal("0.60"): "2"}[threshold_decimal]
    profile_adjustment = "0.5" if profile == "realistic" else "0"
    return {
        "run_id": f"run-{payload.get('parameter_fingerprint')}-{payload.get('evidence_mode')}",
        "status": "succeeded",
        "market_slug": "fixture-market",
        "parameters": {
            "entry_threshold": threshold,
            "execution_profile": profile,
        },
        "market_category": "crypto" if threshold == "0.58" else "sports",
        "liquidity_bucket": "active" if profile == "realistic" else "thin",
        "volatility_bucket": "normal" if profile == "realistic" else "high",
        "time_to_expiry_bucket": "gte_7d" if mode == "train" else "lt_1d",
        "final_minute": "final_minute" if mode == "walk_forward" else "not_final_minute",
        "event_outcome_count_bucket": "large_multi_21_plus" if threshold_decimal == Decimal("0.60") else "binary",
        "metrics": [
            {"metric_key": "net_profit", "value": str(float(threshold_score) + float(profile_adjustment))},
            {"metric_key": "performance_score", "value": str(float(threshold_score) + float(profile_adjustment))},
            {"metric_key": "max_drawdown", "value": "1"},
            {"metric_key": "liquidity_fill_rate", "value": "80"},
            {"metric_key": "total_trades", "value": "20"},
            {"metric_key": "submitted_orders", "value": "25"},
        ],
    }


def test_parameter_search_batch_dry_run_does_not_execute() -> None:
    calls: list[Mapping[str, Any]] = []
    report = run_parameter_search_plan(
        _small_plan(),
        executor=lambda payload: calls.append(payload) or {},
        dry_run=True,
        max_runs=4,
    )

    assert calls == []
    assert report["status"] == "review"
    assert report["dry_run"] is True
    assert report["planned_run_count"] == 4
    assert report["executed_run_count"] == 0
    assert all(row["status"] == "planned" for row in report["rows"])


def test_parameter_search_batch_ready_with_fake_executor_and_staging_preview() -> None:
    report = run_parameter_search_plan(
        _small_plan(),
        executor=_fake_executor,
        dry_run=False,
        stage_parameters=True,
        strategy_name="favorite_hold_v1",
        strategy_version="fixture",
    )

    assert report["status"] == READY
    assert report["executed_run_count"] == report["planned_run_count"]
    assert report["parameter_search_results"]["status"] == READY
    assert report["parameter_search_results"]["coverage_pct"] == "100"
    assert report["parameter_search_results"]["robustness_report"]["robustness_verdict"] == READY
    assert report["staging_preview"]["staging_allowed"] is True
    assert report["staging_preview"]["strategy_name"] == "favorite_hold_v1"


def test_parameter_search_batch_captures_execution_errors() -> None:
    def failing_executor(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        if payload.get("evidence_mode") == "test":
            raise RuntimeError("fixture failure")
        return _fake_executor(payload)

    report = run_parameter_search_plan(
        _small_plan(),
        executor=failing_executor,
        dry_run=False,
        max_runs=3,
    )

    assert report["status"] == "fail"
    assert report["failed_run_count"] == 1
    assert any(row.get("error") == "fixture failure" for row in report["rows"])


def test_normalize_execution_result_extracts_metrics() -> None:
    plan = _small_plan()
    row = normalize_parameter_search_execution_result(plan["plan_items"][0], _fake_executor(plan["plan_items"][0]["request_payload"]))

    assert row["net_pnl"] == "4.5"
    assert row["performance_score"] == "4.5"
    assert row["max_drawdown"] == "1"
    assert row["fill_rate"] == "0.8"
    assert row["sample_count"] == 20
    assert row["market_category"] == "sports"


def test_run_fill_first_parameter_search_batch_script_dry_run_outputs_json(tmp_path: Path) -> None:
    plan = _small_plan()
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--plan-json",
            str(plan_path),
            "--max-runs",
            "2",
            "--format",
            "json",
        ],
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout
    report = json.loads(completed.stdout)
    assert report["schema_version"] == "fill_first_parameter_search_batch_v1"
    assert report["dry_run"] is True
    assert report["planned_run_count"] == 2
    assert report["executed_run_count"] == 0
