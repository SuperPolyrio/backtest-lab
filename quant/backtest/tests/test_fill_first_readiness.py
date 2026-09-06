from pathlib import Path

from quant.backtest.fill_first_readiness import (
    FILE_REQUIREMENTS,
    MISSING,
    READY,
    REVIEW,
    aggregate_status,
    build_fill_first_readiness_report,
    evaluate_file_requirements,
    readiness_to_markdown,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_aggregate_status_prioritizes_missing_then_review() -> None:
    assert aggregate_status([READY, READY]) == READY
    assert aggregate_status([READY, REVIEW]) == REVIEW
    assert aggregate_status([READY, MISSING, REVIEW]) == MISSING


def test_file_requirement_reports_missing_tokens(tmp_path: Path) -> None:
    target = tmp_path / "demo.py"
    target.write_text("alpha beta\n", encoding="utf-8")
    requirement = FILE_REQUIREMENTS[0].__class__(
        name="demo",
        path="demo.py",
        tokens=("alpha", "gamma"),
        detail="demo detail",
    )

    checks = evaluate_file_requirements(tmp_path, (requirement,))

    assert checks[0].status == MISSING
    assert "gamma" in checks[0].detail


def test_current_project_has_fill_first_structural_coverage() -> None:
    report = build_fill_first_readiness_report(
        PROJECT_ROOT,
        env={
            "ORDER_STATE_API_URL": "https://replace-with-private-order-api.example/orders",
            "ORDER_STATE_AUTH_HEADER": "Authorization=REPLACE_WITH_PRIVATE_AUTH_VALUE",
        },
    )

    assert report["status"] == REVIEW
    assert report["missing_count"] == 0
    assert report["review_count"] >= 1
    assert "LOB/DEPTH is intentionally excluded" in report["scope"]
    assert any(check["name"] == "real cost calibration" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "shadow/live triangulation report" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "fill-first promotion gate" and check["status"] == READY for check in report["checks"])
    assert any(check["name"] == "frontend fill quality result" and check["status"] == "out_of_scope"
               for check in report["checks"])


def test_artifact_audit_is_required_and_frontend_has_an_explicit_external_owner() -> None:
    report = build_fill_first_readiness_report(PROJECT_ROOT, env={})

    assert any(
        check["name"] == "Flask routes"
        and check["status"] == READY
        and "artifact audit" in check["detail"]
        for check in report["checks"]
    )
    assert any(
        check["name"] == "frontend fill quality result"
        and check["status"] == "out_of_scope"
        and "platform-website" in check["detail"]
        for check in report["checks"]
    )
    assert any(
        check["name"] == "run artifact audit"
        and check["status"] == READY
        and "shadow_live_triangulation_report" in check["detail"]
        and "promotion_gate_report" in check["detail"]
        and "execution regime 全维度" in check["detail"]
        and "tail risk" in check["detail"]
        and "settlement/source compatibility" in check["detail"]
        and "data quality report" in check["detail"]
        for check in report["checks"]
    )
    assert any(
        check["name"] == "frontend result adapter"
        and check["status"] == "out_of_scope"
        and "platform-website" in check["detail"]
        for check in report["checks"]
    )


def test_readiness_markdown_contains_next_actions() -> None:
    report = build_fill_first_readiness_report(PROJECT_ROOT, env={})
    markdown = readiness_to_markdown(report)

    assert "Fill-first Backtest Readiness" in markdown
    assert "Next Actions" in markdown
    assert "external order-state source" in markdown
