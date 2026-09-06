from decimal import Decimal

from scripts.explain_fill_only_v3_cross_validation_orders import (
    _expected_probability,
    explain_order,
    summarize,
)


def _raw_order(*, pml2_size: str, nautilus_size: str) -> dict[str, object]:
    return {
        "order_id": "order-1",
        "market_id": 7,
        "condition_id": "condition",
        "asset_id": "asset",
        "title": "Example market",
        "slug": "example-market",
        "category": "sports",
        "outcome": "YES",
        "decision_ts": "2026-07-15T00:00:00+00:00",
        "arrival_ts": "2026-07-15T00:00:01+00:00",
        "side": "BUY",
        "size": "10",
        "limit_price": "0.51",
        "signal_trade": {"price": "0.50", "size": "5"},
        "trailing_trade_count": 2,
        "trailing_volume": "10",
        "models": {
            "v3_source_fak": {
                "status": "NO_FILL",
                "reason": "no_source",
                "filled_size": "0",
            },
            "v3_l2_expected": {
                "status": "MODELED_EXPECTATION",
                "reason": "modeled",
                "filled_size": "4",
                "avg_price": "0.51",
                "probability_bounds": {"any_fill_execution_horizon": "0.5"},
                "capacity_bounds": {"conditional_expected": "8"},
                "fills": [{"conditional_fill_size": "8"}],
                "model_diagnostics": {"selected_route": "expected"},
            },
            "pml2_fak": {
                "status": "FILLED" if Decimal(pml2_size) > 0 else "CANCELLED",
                "reason": "reference",
                "filled_size": pml2_size,
            },
            "nautilus_fak": {
                "status": "FILLED" if Decimal(nautilus_size) > 0 else "NO_FILL",
                "reason": "reference",
                "filled_size": nautilus_size,
            },
        },
    }


def test_expected_probability_prefers_execution_horizon() -> None:
    assert _expected_probability(
        {
            "filled_size": "1",
            "probability_bounds": {
                "any_fill_execution_horizon": "0.25",
                "horizon": "0.75",
            },
        }
    ) == Decimal("0.25")


def test_order_contributions_reconcile() -> None:
    filled = explain_order(_raw_order(pml2_size="10", nautilus_size="5"), window="a")
    empty = explain_order(_raw_order(pml2_size="0", nautilus_size="0"), window="b")
    totals = summarize([filled, empty])

    assert totals.orders == 2
    assert totals.expected_orders == Decimal("1.0")
    assert totals.expected_quantity == Decimal(8)
    assert totals.pml2_orders == Decimal(1)
    assert totals.pml2_quantity == Decimal(10)
    assert totals.pml2_brier / totals.orders == Decimal("0.25")
    assert totals.nautilus_orders == Decimal(1)
    assert totals.nautilus_quantity == Decimal(5)
    assert totals.nautilus_brier / totals.orders == Decimal("0.25")
