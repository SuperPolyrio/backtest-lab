import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest

from quant.backtest.parameter_search_plan import build_parameter_search_plan
from quant.backtest.parameter_search_results import build_parameter_search_results_report
from quant.backtest.production_parameter_staging import (
    extract_parameter_search_results_reports,
    normalize_production_parameter_staging,
    validate_production_parameter_staging_status_update,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "scripts" / "stage_production_parameters.py"
REVIEW_SCRIPT = PROJECT_ROOT / "scripts" / "review_production_parameter_staging.py"


def _plan() -> dict:
    return build_parameter_search_plan(
        grid={
            "entry_threshold": ["0.56", "0.58", "0.60"],
            "execution_profile": ["realistic", "conservative"],
        },
        evidence_modes=["train", "test", "walk_forward"],
        max_runs=50,
    )


def _rows(plan: dict) -> list[dict]:
    output = []
    for index, item in enumerate(plan["plan_items"], start=1):
        params = dict(item["parameters"])
        threshold = Decimal(str(params["entry_threshold"]))
        pnl = {Decimal("0.56"): "4", Decimal("0.58"): "7", Decimal("0.60"): "2"}[threshold]
        output.append(
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
                "net_pnl": pnl,
                "performance_score": pnl,
                "max_drawdown": "1",
                "fill_rate": "0.80",
                "sample_count": 20,
            }
        )
    return output


def _ready_report() -> dict:
    plan = _plan()
    return build_parameter_search_results_report(plan, _rows(plan))


def test_normalize_production_parameter_staging_from_ready_report() -> None:
    row = normalize_production_parameter_staging(
        _ready_report(),
        status="pending",
        source="unit-test",
        strategy_name="favorite_hold_v1",
        strategy_version="v1",
        universe_name="fixture",
    )

    assert row["status"] == "pending"
    assert row["source"] == "unit-test"
    assert row["strategy_name"] == "favorite_hold_v1"
    assert row["universe_name"] == "fixture"
    assert row["staging_allowed"] is True
    assert row["coverage_pct"] == Decimal("100")
    assert row["robustness_verdict"] == "ready"
    assert row["regime_coverage_verdict"] == "ready"
    assert row["score_bias_verdict"] == "ready"
    assert row["strategy_scope"] == "generalizable_candidate"
    assert row["parameter_fingerprint"]
    assert row["parameters"]["entry_threshold"]


def test_staging_blocks_review_report_without_force() -> None:
    plan = _plan()
    report = build_parameter_search_results_report(plan, _rows(plan)[:-1])

    with pytest.raises(ValueError, match="not staging_allowed"):
        normalize_production_parameter_staging(report)


def test_staging_can_record_review_report_with_force() -> None:
    plan = _plan()
    report = build_parameter_search_results_report(plan, _rows(plan)[:-1])

    row = normalize_production_parameter_staging(report, force_review=True)

    assert row["staging_allowed"] is False
    assert row["default_action"] == "do_not_stage"
    assert row["blocked_reasons"]


def test_approved_staging_requires_reviewer() -> None:
    with pytest.raises(ValueError, match="approved_by"):
        normalize_production_parameter_staging(_ready_report(), status="approved")


def test_validate_staging_status_update_requires_reviewer_for_approval() -> None:
    row = normalize_production_parameter_staging(_ready_report())

    with pytest.raises(ValueError, match="reviewed_by"):
        validate_production_parameter_staging_status_update(row, status="approved")


def test_validate_staging_status_update_approves_ready_row() -> None:
    row = normalize_production_parameter_staging(_ready_report())

    update = validate_production_parameter_staging_status_update(row, status="approved", reviewed_by="researcher")

    assert update["status"] == "approved"
    assert update["approved"] is True
    assert update["approved_by"] == "researcher"
    assert update["reviewed_by"] == "researcher"


def test_validate_staging_status_update_blocks_approval_when_report_is_not_allowed() -> None:
    plan = _plan()
    review_report = build_parameter_search_results_report(plan, _rows(plan)[:-1])
    row = normalize_production_parameter_staging(review_report, force_review=True)

    with pytest.raises(ValueError, match="staging_allowed"):
        validate_production_parameter_staging_status_update(row, status="approved", reviewed_by="researcher")


def test_validate_staging_status_update_blocks_approval_when_regime_coverage_is_not_ready() -> None:
    row = normalize_production_parameter_staging(_ready_report())
    row["regime_coverage_verdict"] = "review"

    with pytest.raises(ValueError, match="regime_coverage_verdict"):
        validate_production_parameter_staging_status_update(row, status="approved", reviewed_by="researcher")


def test_validate_staging_status_update_blocks_approval_when_score_bias_is_not_ready() -> None:
    row = normalize_production_parameter_staging(_ready_report())
    row["score_bias_verdict"] = "review"

    with pytest.raises(ValueError, match="score_bias_verdict"):
        validate_production_parameter_staging_status_update(row, status="approved", reviewed_by="researcher")


def test_validate_staging_status_update_can_reject_blocked_row() -> None:
    plan = _plan()
    review_report = build_parameter_search_results_report(plan, _rows(plan)[:-1])
    row = normalize_production_parameter_staging(review_report, force_review=True)

    update = validate_production_parameter_staging_status_update(
        row,
        status="rejected",
        reviewed_by="researcher",
        review_note="missing planned rows",
    )

    assert update["status"] == "rejected"
    assert update["approved"] is False
    assert update["review_note"] == "missing planned rows"


def test_extract_parameter_search_results_from_benchmark_artifact_wrapper() -> None:
    report = _ready_report()
    payload = {"artifacts": [{"artifactKey": "parameter_search_results", "payload": report}]}

    assert extract_parameter_search_results_reports(payload) == [report]


def test_stage_production_parameters_script_dry_run(tmp_path: Path) -> None:
    input_path = tmp_path / "parameter-search-results.json"
    input_path.write_text(json.dumps(_ready_report()), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--input",
            str(input_path),
            "--strategy-name",
            "favorite_hold_v1",
            "--strategy-version",
            "v1",
            "--universe-name",
            "fixture",
            "--dry-run",
        ],
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout
    output = json.loads(completed.stdout)
    assert output["count"] == 1
    assert output["rows"][0]["staging_allowed"] is True
    assert output["rows"][0]["strategy_name"] == "favorite_hold_v1"


def test_review_production_parameter_staging_script_dry_run(tmp_path: Path) -> None:
    row_path = tmp_path / "staging-row.json"
    row_path.write_text(json.dumps(normalize_production_parameter_staging(_ready_report()), default=str), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(REVIEW_SCRIPT),
            "--dry-run-row-json",
            str(row_path),
            "--status",
            "approved",
            "--reviewed-by",
            "researcher",
            "--review-note",
            "ready evidence reviewed",
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
    output = json.loads(completed.stdout)
    assert output["dry_run"] is True
    assert output["valid"] is True
    assert output["update"]["status"] == "approved"
    assert output["update"]["approved_by"] == "researcher"
