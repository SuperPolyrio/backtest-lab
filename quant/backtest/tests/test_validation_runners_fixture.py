import pytest

from quant.backtest.runners.base import RESULT_FIELDS, assert_result_passed
from quant.backtest.runners.public import PUBLIC_RUNNERS, run_all_public


pytestmark = pytest.mark.backtest_validation


def test_public_runners_fixture_mode_emit_required_fields():
    results = run_all_public(mode="fixture")

    assert len(results) == len(PUBLIC_RUNNERS)
    for result in results:
        payload = result.as_dict()
        for field in RESULT_FIELDS:
            assert field in payload
        assert payload["data_version"]
        assert payload["rows_scanned"] >= 0
        assert payload["total_runtime_sec"] >= 0
        assert_result_passed(result)


def test_runner_counts_cover_execution_states():
    results = {result.run_id: result for result in run_all_public(mode="fixture")}

    assert results["no_trade_replay_smoke"].orders_created == 0
    assert results["single_fill_replay"].fills == 1
    assert results["limit_order_lifecycle"].partial_fills == 1
    assert results["limit_order_lifecycle"].rejected == 1
    assert results["limit_order_lifecycle"].expired == 1
    assert results["illiquid_rejection_smoke"].partial_fills >= 1
    assert results["illiquid_rejection_smoke"].rejected >= 1
    assert results["resolution_settlement_smoke"].settlements == 2
    assert results["account_ledger_replay"].meta["position"] == "6"
