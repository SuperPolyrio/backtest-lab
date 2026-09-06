import json
import subprocess
import sys
from pathlib import Path

from quant.backtest.parameter_search_plan import (
    READY,
    REVIEW,
    build_parameter_search_plan,
    parameter_search_plan_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = PROJECT_ROOT / "scripts" / "plan_fill_first_parameter_search.py"


def test_parameter_search_plan_default_is_ready() -> None:
    report = build_parameter_search_plan(base_payload={"price_source": "orderfilled_block_close"})

    assert report["status"] == READY
    assert report["parameter_set_count"] >= 3
    assert report["planned_run_count"] >= 5
    assert {"train", "test", "walk_forward"} <= set(report["evidence_modes"])
    assert {"realistic", "conservative"} <= set(report["execution_profiles"])
    item = report["plan_items"][0]
    assert item["request_payload"]["price_source"] == "orderfilled_block_close"
    assert item["parameter_fingerprint"]
    assert "net_pnl" in item["expected_robustness_row_fields"]


def test_parameter_search_plan_reviews_missing_conservative_profile() -> None:
    report = build_parameter_search_plan(
        grid={
            "entry_threshold": ["0.56", "0.58", "0.60"],
            "execution_profile": ["realistic"],
        },
        evidence_modes=["train", "test"],
    )

    assert report["status"] == REVIEW
    assert "missing required execution profiles: conservative" in report["reason"]
    assert any("Do not promote best-only" in action for action in report["next_actions"])


def test_parameter_search_plan_reviews_truncated_plan() -> None:
    report = build_parameter_search_plan(max_runs=3)

    assert report["status"] == REVIEW
    assert report["truncated"] is True
    assert report["planned_run_count"] == 3
    assert "truncated" in report["reason"]


def test_parameter_search_plan_markdown_lists_requirements() -> None:
    markdown = parameter_search_plan_to_markdown(build_parameter_search_plan())

    assert "Robustness Requirements" in markdown
    assert "do_not_promote_best_only" in markdown
    assert "walk_forward" in markdown


def test_plan_fill_first_parameter_search_script_outputs_json(tmp_path: Path) -> None:
    base_payload = tmp_path / "base.json"
    base_payload.write_text(json.dumps({"market_slug": "fixture-market"}), encoding="utf-8")

    completed = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--base-payload-json",
            str(base_payload),
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
    assert report["plan_items"][0]["request_payload"]["market_slug"] == "fixture-market"
