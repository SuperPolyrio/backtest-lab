from decimal import Decimal
import subprocess
import sys
from pathlib import Path

from quant.backtest.parameter_search_plan import READY, build_parameter_search_plan
from quant.backtest.parameter_search_scheduler import (
    build_parameter_search_progress_report,
    parameter_search_progress_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "scripts" / "run_fill_first_parameter_search_scheduler.py"


def _plan() -> dict:
    return build_parameter_search_plan(
        grid={
            "entry_threshold": ["0.56", "0.58", "0.60"],
            "execution_profile": ["realistic", "conservative"],
        },
        evidence_modes=["train", "test", "walk_forward"],
        max_runs=50,
    )


def _result_row(index: int, item: dict) -> dict:
    params = dict(item["parameters"])
    threshold = Decimal(str(params["entry_threshold"]))
    pnl = {Decimal("0.56"): "4", Decimal("0.58"): "7", Decimal("0.60"): "2"}[threshold]
    profile = str(params.get("execution_profile"))
    adjustment = Decimal("0.5") if profile == "realistic" else Decimal("0")
    return {
        "run_id": index,
        "parameter_fingerprint": item["parameter_fingerprint"],
        "parameters": params,
        "mode": item["evidence_mode"],
        "market_category": "sports" if index % 2 else "crypto",
        "liquidity_bucket": "active" if index % 2 else "thin",
        "volatility_bucket": "normal" if index % 3 else "high",
        "time_to_expiry_bucket": "gte_7d" if index % 2 else "lt_1d",
        "final_minute": "not_final_minute" if index % 2 else "final_minute",
        "event_outcome_count_bucket": "binary" if index % 2 else "large_multi_21_plus",
        "net_pnl": str(Decimal(pnl) + adjustment),
        "performance_score": str(Decimal(pnl) + adjustment),
        "max_drawdown": "1",
        "fill_rate": "0.80",
        "sample_count": 20,
    }


def _items_for_plan(plan: dict, *, status: str = "succeeded") -> list[dict]:
    rows = []
    for index, item in enumerate(plan["plan_items"], start=1):
        rows.append(
            {
                "item_id": index,
                "batch_id": 7,
                "item_index": index,
                "item_key": item["key"],
                "status": status,
                "parameter_fingerprint": item["parameter_fingerprint"],
                "evidence_mode": item["evidence_mode"],
                "parameters": item["parameters"],
                "request_payload": item["request_payload"],
                "result_row": _result_row(index, item) if status == "succeeded" else {},
                "attempt_count": 1 if status != "queued" else 0,
                "max_attempts": 2,
            }
        )
    return rows


def test_parameter_search_progress_ready_when_all_items_succeeded() -> None:
    plan = _plan()
    report = build_parameter_search_progress_report(
        {
            "batch_id": 7,
            "status": "running",
            "plan": plan,
            "universe_name": "fixture",
            "strategy_name": "favorite_hold_v1",
            "strategy_version": "fixture",
        },
        _items_for_plan(plan),
    )

    assert report["status"] == READY
    assert report["planned_run_count"] == plan["planned_run_count"]
    assert report["succeeded_count"] == plan["planned_run_count"]
    assert report["completion_pct"] == "100"
    assert report["parameter_search_results"]["status"] == READY
    assert report["parameter_search_results"]["coverage_pct"] == "100"
    assert report["parameter_search_results"]["performance_score_validation_report"]["score_bias_verdict"] == READY


def test_parameter_search_progress_reports_queued_and_retryable_items() -> None:
    plan = _plan()
    items = _items_for_plan(plan, status="queued")
    items[0]["status"] = "failed"
    items[0]["attempt_count"] = 1
    items[0]["max_attempts"] = 2

    report = build_parameter_search_progress_report({"batch_id": 7, "plan": plan}, items)

    assert report["status"] == "queued"
    assert report["queued_count"] == plan["planned_run_count"] - 1
    assert report["failed_count"] == 1
    assert report["retryable_count"] == 1
    assert any("workers" in action for action in report["next_actions"])


def test_parameter_search_progress_reviews_permanent_failures() -> None:
    plan = _plan()
    items = _items_for_plan(plan)
    items[-1]["status"] = "failed"
    items[-1]["attempt_count"] = 2
    items[-1]["max_attempts"] = 2
    items[-1]["result_row"] = {}

    report = build_parameter_search_progress_report({"batch_id": 7, "plan": plan}, items)

    assert report["status"] == "review"
    assert report["failed_count"] == 1
    assert report["retryable_count"] == 0
    assert report["parameter_search_results"]["missing_run_count"] == 1


def test_parameter_search_progress_markdown_mentions_retryable() -> None:
    plan = _plan()
    items = _items_for_plan(plan, status="queued")
    items[0]["status"] = "failed"
    items[0]["attempt_count"] = 1
    items[0]["max_attempts"] = 2

    markdown = parameter_search_progress_to_markdown(build_parameter_search_progress_report({"batch_id": 7, "plan": plan}, items))

    assert "retryable: 1" in markdown
    assert "Fill-first Parameter Search Progress" in markdown


def test_parameter_search_progress_reports_canceled_items() -> None:
    plan = _plan()
    items = _items_for_plan(plan, status="queued")
    for item in items:
        item["status"] = "canceled"
        item["attempt_count"] = 1
        item["error"] = "operator canceled"

    report = build_parameter_search_progress_report({"batch_id": 7, "plan": plan}, items)

    assert report["status"] == "canceled"
    assert report["canceled_count"] == plan["planned_run_count"]
    assert report["queued_count"] == 0
    assert "canceled" in report["reason"]


def test_parameter_search_scheduler_cli_exposes_worker_loop() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    assert completed.returncode == 0
    assert "--worker-loop" in completed.stdout
    assert "--stream-json" in completed.stdout
    assert "--poll-seconds" in completed.stdout
    assert "--idle-exit" in completed.stdout
