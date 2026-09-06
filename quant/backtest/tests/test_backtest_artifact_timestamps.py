from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from quant.backtest.backtest_engine import PricePoint, _attach_artifact_source_timestamps


def test_attach_artifact_source_timestamps_uses_chart_price_point_clock() -> None:
    first = datetime(2026, 4, 12, 12, 26, 13, tzinfo=timezone.utc)
    second = datetime(2026, 4, 12, 12, 27, 1, tzinfo=timezone.utc)
    points = [
        PricePoint(x_value=100, price=Decimal("0.20"), volume=Decimal("1"), timestamp=first),
        PricePoint(x_value=200, price=Decimal("0.21"), volume=Decimal("1"), timestamp=second),
    ]
    result = {
        "events": [{"x_value": 100, "meta": {}}],
        "ledger": [{"x_value": 150, "meta": {}}],
        "orders": [{"signal_x": 100, "submit_x": 150, "meta": {}}],
    }

    _attach_artifact_source_timestamps(result, points)

    assert result["events"][0]["meta"]["source_timestamp"] == first.isoformat()
    assert result["ledger"][0]["meta"]["source_timestamp"] == second.isoformat()
    assert result["orders"][0]["meta"]["signal_timestamp"] == first.isoformat()
    assert result["orders"][0]["meta"]["submit_timestamp"] == second.isoformat()
