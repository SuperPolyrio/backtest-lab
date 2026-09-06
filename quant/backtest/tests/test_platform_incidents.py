from datetime import datetime, timezone
from decimal import Decimal

from quant.backtest.backtest_engine import PricePoint, build_fill_quality_report
from quant.backtest.platform_incidents import build_platform_incident_report, normalize_platform_incident


def test_normalize_platform_incident_accepts_time_and_block_window() -> None:
    incident = normalize_platform_incident(
        {
            "id": "clob-upgrade-1",
            "service": "CLOB API",
            "level": "critical",
            "summary": "service not ready during upgrade",
            "startBlock": 100,
            "endBlock": 120,
        },
        source="ops-notes",
    )

    assert incident["source"] == "ops-notes"
    assert incident["component"] == "clob_api"
    assert incident["severity"] == "critical"
    assert incident["incident_key"] == "clob-upgrade-1"


def test_platform_incident_report_generates_environment_flags() -> None:
    report = build_platform_incident_report([
        {"incident_key": "gamma-1", "component": "gamma", "severity": "warning"},
        {"incident_key": "clob-1", "component": "clob-api", "severity": "critical"},
    ])

    assert report["incident_count"] == 2
    assert report["severity_counts"]["critical"] == 1
    assert report["environment_flags"]["platform_incident_gamma_warning"] == 1
    assert report["environment_flags"]["platform_incident_clob_api_critical"] == 1


def test_fill_quality_includes_platform_incident_environment_flags() -> None:
    report = build_fill_quality_report(
        [],
        data_quality_report={
            "platform_incidents": build_platform_incident_report([
                {"incident_key": "api-1", "component": "api", "severity": "error"},
            ])
        },
    )

    assert report["environment_flags"]["platform_incident_api"] == 1
    assert report["environment_flags"]["platform_incident_api_error"] == 1


def test_price_point_timestamp_shape_is_compatible_with_incident_windows() -> None:
    point = PricePoint(
        x_value=10,
        price=Decimal("1"),
        volume=Decimal("0"),
        trade_count=0,
        timestamp=datetime(2026, 6, 25, tzinfo=timezone.utc),
    )

    assert point.timestamp.isoformat() == "2026-06-25T00:00:00+00:00"
