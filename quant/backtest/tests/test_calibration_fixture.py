from quant.backtest.calibration_fixture import run_shadow_live_calibration_fixture


def test_shadow_live_calibration_fixture_exercises_full_local_pipeline() -> None:
    report = run_shadow_live_calibration_fixture()

    assert report["passed"] is True
    assert report["plan_status"] == "ready"
    assert report["validation_status"] == "ready"
    assert report["calibration_sample_count"] == 2
    assert report["calibration_status_error_count"] == 0
    assert report["calibration_trust_status"] == "ready"
