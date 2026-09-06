"""The extraction must stay runnable without the monolith or private data."""

import importlib
from pathlib import Path

import pytest
from flask import Flask

from scripts.api.routes.quant import create_quant_blueprint


@pytest.mark.parametrize("name", [
    "quant.backtest.orderfilled_v2_replay",
    "quant.backtest.trade_only_v3.engine",
    "quant.backtest.pml2.session",
    "quant.backtest.financial_finalization_service",
    "quant.orderbook.l2_parquet_writer",
])
def test_runtime_modules_resolve_inside_this_checkout(name):
    root = Path(__file__).resolve().parents[1]
    assert Path(importlib.import_module(name).__file__).resolve().is_relative_to(root)


def test_only_research_routes_are_exposed():
    app = Flask(__name__)
    app.register_blueprint(create_quant_blueprint({"app": app}))
    paths = {rule.rule for rule in app.url_map.iter_rules()}
    assert "/quant/backtest-runs" in paths
    assert "/quant/fill-only/v3/replay" in paths
    assert "/quant/prediction-l2/v2/replay" in paths
    assert "/quant/backtest-runs/<int:run_id>/finalize" in paths
    assert not any("paper" in p or "product-hub" in p or "price-window" in p for p in paths)
    for version in ("fill-only/v2", "fill-only/v3", "prediction-l2/v1", "prediction-l2/v2"):
        assert app.test_client().get(f"/quant/{version}/profiles").status_code == 200


def test_missing_external_assets_are_not_reported_as_passing():
    from quant.backtest.fill_first_readiness import build_fill_first_readiness_report

    report = build_fill_first_readiness_report(Path(__file__).resolve().parents[1], env={})
    external = [row for row in report["checks"] if row["status"] == "out_of_scope"]
    assert external
    assert all("owned by" in row["detail"] for row in external)


def test_installed_backtrader_replaces_the_vendor_copy():
    from decimal import Decimal

    import backtrader
    from quant.backtest.backtest_engine import BacktestParameters, PricePoint
    from quant.backtest.frameworks import run_framework_backtest

    assert "/vendor/" not in backtrader.__file__
    points = [PricePoint(i, Decimal(price), Decimal("100000"), trade_count=100)
              for i, price in enumerate(("0.50", "0.60", "0.70", "0.80", "0.55"))]
    result = run_framework_backtest(
        "backtrader", points, {"price_source": "orderfilled_block_close", "market_slug": "migration-fixture", "token_side": "YES"},
        BacktestParameters(execution_price_mode="ORDERFILLED", order_role="taker"),
        builtin_simulator=lambda *_args: pytest.fail("unexpected builtin fallback"),
        metrics_builder=lambda *_args: [],
    )
    assert result["actual_backtest_engine"] == "backtrader"
    assert len(result["equity"]) == len(points)
    assert result["trades"]
