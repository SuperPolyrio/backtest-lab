import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from quant.backtest.parameter_search_plan import READY, build_parameter_search_plan
from quant.backtest.parameter_search_results import (
    REVIEW,
    build_parameter_search_results_report,
    parameter_search_results_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "scripts" / "check_fill_first_parameter_search_results.py"


def _small_plan() -> dict:
    return build_parameter_search_plan(
        grid={
            "entry_threshold": ["0.56", "0.58", "0.60"],
            "execution_profile": ["realistic", "conservative"],
        },
        evidence_modes=["train", "test", "walk_forward"],
        max_runs=50,
    )


def _rows_for_plan(plan: dict) -> list[dict]:
    rows: list[dict] = []
    for index, item in enumerate(plan["plan_items"], start=1):
        params = dict(item["parameters"])
        threshold = str(params["entry_threshold"])
        profile = str(params["execution_profile"])
        threshold_score = {Decimal("0.56"): "4", Decimal("0.58"): "7", Decimal("0.60"): "2"}[Decimal(threshold)]
        profile_adjustment = "0.5" if profile == "realistic" else "0"
        rows.append(
            {
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
                "net_pnl": str(float(threshold_score) + float(profile_adjustment)),
                "performance_score": str(float(threshold_score) + float(profile_adjustment)),
                "max_drawdown": "1",
                "fill_rate": "0.80",
                "sample_count": 20,
            }
        )
    return rows


def test_parameter_search_results_ready_when_plan_is_fully_covered() -> None:
    plan = _small_plan()
    report = build_parameter_search_results_report(plan, _rows_for_plan(plan))

    assert report["status"] == READY
    assert report["planned_run_count"] == plan["planned_run_count"]
    assert report["covered_run_count"] == plan["planned_run_count"]
    assert report["coverage_pct"] == "100"
    assert report["missing_items"] == []
    assert report["robustness_report"]["robustness_verdict"] == READY
    assert report["regime_coverage_report"]["coverage_verdict"] == READY
    assert report["performance_score_validation_report"]["score_bias_verdict"] == READY
    assert report["production_parameter_staging"]["staging_allowed"] is True
    assert report["production_parameter_staging"]["best_parameter_fingerprint"]


def test_parameter_search_results_reviews_missing_and_unplanned_rows() -> None:
    plan = _small_plan()
    rows = _rows_for_plan(plan)
    rows.pop()
    rows.append(
        {
            "run_id": 999,
            "parameter_fingerprint": "unplanned",
            "parameters": {"entry_threshold": "0.99", "execution_profile": "realistic"},
            "mode": "train",
            "market_category": "sports",
            "liquidity_bucket": "active",
            "volatility_bucket": "normal",
            "time_to_expiry_bucket": "medium",
            "net_pnl": "99",
            "max_drawdown": "1",
            "fill_rate": "0.80",
            "sample_count": 20,
        }
    )

    report = build_parameter_search_results_report(plan, rows)

    assert report["status"] == REVIEW
    assert report["missing_run_count"] == 1
    assert report["unplanned_result_count"] == 1
    assert report["production_parameter_staging"]["staging_allowed"] is False
    assert any("missing result rows" in action for action in report["next_actions"])


def test_parameter_search_results_detects_duplicate_results() -> None:
    plan = _small_plan()
    rows = _rows_for_plan(plan)
    rows.append(dict(rows[0], run_id=1000))

    report = build_parameter_search_results_report(plan, rows)

    assert report["duplicate_result_count"] == 1
    assert report["duplicate_results"][0]["parameter_fingerprint"] == rows[0]["parameter_fingerprint"]


def test_parameter_search_results_blocks_staging_when_regime_coverage_is_narrow() -> None:
    plan = _small_plan()
    rows = [
        {
            **row,
            "market_category": "sports",
            "liquidity_bucket": "active",
            "time_to_expiry_bucket": "gte_7d",
        }
        for row in _rows_for_plan(plan)
    ]

    report = build_parameter_search_results_report(plan, rows)

    assert report["status"] == REVIEW
    assert report["robustness_report"]["robustness_verdict"] == READY
    assert report["regime_coverage_report"]["coverage_verdict"] == REVIEW
    assert report["production_parameter_staging"]["staging_allowed"] is False
    assert any("coverage too narrow" in reason for reason in report["production_parameter_staging"]["blocked_reasons"])


def test_parameter_search_results_blocks_staging_when_score_is_small_sample_biased() -> None:
    plan = _small_plan()
    rows = _rows_for_plan(plan)
    rows[0]["performance_score"] = "999"
    rows[0]["sample_count"] = 1

    report = build_parameter_search_results_report(plan, rows)

    assert report["status"] == REVIEW
    assert report["robustness_report"]["robustness_verdict"] == READY
    assert report["regime_coverage_report"]["coverage_verdict"] == READY
    assert report["performance_score_validation_report"]["score_bias_verdict"] == REVIEW
    assert report["production_parameter_staging"]["staging_allowed"] is False
    assert report["production_parameter_staging"]["score_bias_verdict"] == REVIEW
    assert any("small-sample" in reason for reason in report["production_parameter_staging"]["blocked_reasons"])


def test_parameter_search_results_markdown_mentions_staging() -> None:
    plan = _small_plan()
    markdown = parameter_search_results_to_markdown(build_parameter_search_results_report(plan, _rows_for_plan(plan)))

    assert "staging_allowed: True" in markdown
    assert "regime_coverage_verdict: ready" in markdown
    assert "score_bias_verdict: ready" in markdown
    assert "Missing Planned Runs" in markdown


def test_check_fill_first_parameter_search_results_script_outputs_json(tmp_path: Path) -> None:
    plan = _small_plan()
    plan_path = tmp_path / "plan.json"
    results_path = tmp_path / "results.json"
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    results_path.write_text(json.dumps({"rows": _rows_for_plan(plan)}), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--plan-json",
            str(plan_path),
            "--results-json",
            str(results_path),
            "--format",
            "json",
            "--strict-review",
        ],
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout
    report = json.loads(completed.stdout)
    assert report["status"] == READY
    assert report["production_parameter_staging"]["staging_allowed"] is True
