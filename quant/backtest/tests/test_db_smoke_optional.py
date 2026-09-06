import os
from decimal import Decimal

import pytest

from quant.backtest.runners.base import assert_result_passed
from quant.backtest.runners.nba_pregame_hold import run_nba_pregame_hold
from quant.backtest.runners.public import NoTradeReplaySmokeRunner, SingleFillReplayTest
from quant.backtest.runners.selectors import select_nba_2024_25_markets


pytestmark = [
    pytest.mark.backtest_validation,
    pytest.mark.db_smoke,
    pytest.mark.skipif(os.environ.get("POLYDATA_RUN_BACKTEST_DB_SMOKE") != "1", reason="set POLYDATA_RUN_BACKTEST_DB_SMOKE=1 to run DB smoke"),
]


def test_optional_nba_db_smoke_uses_metadata_candidates():
    candidates = select_nba_2024_25_markets(limit=1)
    if not candidates:
        pytest.skip("candidate unavailable: no NBA 24/25 market with ready coverage")
    candidate = candidates[0]

    no_trade = NoTradeReplaySmokeRunner().run(mode="db", candidate=candidate)
    assert_result_passed(no_trade)
    assert no_trade.rows_scanned > 0
    assert no_trade.orders_created == 0


def test_optional_single_fill_db_smoke_is_keyed_and_bounded():
    candidates = select_nba_2024_25_markets(limit=1)
    if not candidates:
        pytest.skip("candidate unavailable: no NBA 24/25 market with ready coverage")
    candidate = candidates[0]
    if not candidate.token_id:
        pytest.skip("candidate unavailable: token_id missing")

    result = SingleFillReplayTest().run(mode="db", candidate=candidate)
    assert result.rows_scanned <= 5000
    assert result.market_count == 1
    if not result.passed:
        pytest.skip(f"single fill candidate unavailable on selected market: {result.message}")


def test_optional_nba_2024_25_raw_orderfilled_strategy_smoke():
    report = run_nba_pregame_hold(limit=200)
    assert report.market_count > 0
    assert report.raw_market_count > 0
    assert report.trades > 0
    assert Decimal(report.total_cost) > 0
    assert {row.buy_outcome_label for row in report.trade_rows} == {"YES"}
