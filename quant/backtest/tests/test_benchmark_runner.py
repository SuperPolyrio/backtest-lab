from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

from quant.backtest.runners import benchmark
from quant.backtest.runners.selectors import ResolvedMarketCandidate, UniverseSpec, universe_spec_from_payload


@dataclass(frozen=True)
class FakeBackfill:
    table: str = "orderfilled_block_replay"
    market_count: int = 2
    from_block: int = 100
    to_block: int = 200
    before_rows: int = 10
    inserted_rows: int = 0
    after_rows: int = 12
    coverage_rows: int = 2
    elapsed_sec: float = 0.01


def _trade(market_id: int, status: str, pnl: str, fill_block: int):
    return SimpleNamespace(
        market_id=market_id,
        market_slug=f"nba-market-{market_id}",
        title=f"NBA Market {market_id}",
        end_date="2025-02-01T00:00:00",
        buy_outcome_label="Home YES",
        signal_time="2025-01-31T23:00:00",
        order_status=status,
        limit_price="0.6500",
        signal_probability="0.6500",
        buy_cost="10.0000",
        filled_size="15.3846",
        pnl=pnl,
        signal_block=100,
        fill_block=fill_block,
        token_id=f"token-{market_id}",
        settlement_outcome="1",
    )


def _report(execution_profile: str, trades):
    return SimpleNamespace(
        execution_profile=execution_profile,
        signal_count=2,
        trades=len(trades),
        no_fills=2 - len(trades),
        trade_rows=trades,
        total_pnl=str(sum(float(row.pnl) for row in trades)),
        settlement_pnl=str(sum(float(row.pnl) for row in trades)),
        trade_exit_pnl="0",
        fee_total="0",
        slippage_total="0",
        db_query_sec=0.02,
        engine_sec=0.03,
        total_runtime_sec=0.05,
    )


def test_nba_benchmark_bundle_uses_shared_datasets_and_builds_diff(monkeypatch):
    loads = []
    runs = []

    markets = [
        ResolvedMarketCandidate(1, "nba-market-1", "NBA Market 1", None, 1, "YES", category="sports"),
        ResolvedMarketCandidate(2, "nba-market-2", "NBA Market 2", None, 2, "NO", category="sports"),
    ]

    def fake_select_universe(spec):
        assert isinstance(spec, UniverseSpec)
        assert spec.universe_name == "nba_2024_25_moneyline"
        return markets

    def fake_load_dataset(selected_markets, *, specs, replay_mode, force_block_replay_backfill=False, build_tag=""):
        loads.append((len(selected_markets), replay_mode, force_block_replay_backfill, specs[0].execution_profile, build_tag))
        return SimpleNamespace(
            replay_mode=replay_mode,
            raw_row_count=20 if replay_mode == "fast" else 200,
            raw_market_count=2,
            db_query_sec=0.01 if replay_mode == "fast" else 0.09,
            backfill_result=FakeBackfill() if replay_mode == "fast" else None,
        )

    def fake_run_from_dataset(dataset, *, spec, db_query_sec, total_start):
        runs.append((dataset.replay_mode, spec.execution_profile))
        if dataset.replay_mode == "fast":
            return _report(spec.execution_profile, [_trade(1, "FILLED", "1.00", 110), _trade(2, "FILLED", "-2.00", 120)])
        return _report(spec.execution_profile, [_trade(1, "FILLED", "1.00", 110), _trade(2, "NO_FILL", "0", 0)])

    monkeypatch.setattr(benchmark, "select_replay_universe", fake_select_universe)
    monkeypatch.setattr(benchmark, "load_favorite_replay_dataset_for_markets", fake_load_dataset)
    monkeypatch.setattr(benchmark, "run_nba_pregame_favorite_hold_from_dataset", fake_run_from_dataset)

    result = benchmark.run_nba_fast_accurate_benchmark(limit=2)

    assert result.status == "completed"
    assert result.market_count == 2
    assert [key for key, *_ in loads] == [2, 2]
    assert {(mode, profile) for mode, profile in runs} == {
        ("fast", "optimistic"),
        ("fast", "realistic"),
        ("fast", "conservative"),
        ("fast", "stress"),
        ("accurate", "realistic"),
    }
    assert len(result.profile_results) == 5
    assert result.summary["fast_raw_rows"] == 20
    assert result.summary["accurate_raw_rows"] == 200
    assert result.summary["status_mismatches"] == 1
    assert result.summary["coverage"]["coverage_rows"] == 2
    assert result.summary["fill_quality"]["signal_count"] == 2
    assert result.summary["regime_coverage"]["status"] == "ready"
    assert result.summary["regime_coverage"]["strategy_scope"] == "regime_specific"
    assert result.summary["prediction_quality"]["sample_count"] == 2
    assert result.summary["performance_score"]["status"] == "ready"
    assert "performance_score" in result.summary["performance_score"]
    assert result.summary["parameter_robustness"]["status"] == "ready"
    assert result.summary["parameter_robustness"]["robustness_verdict"] == "review"
    assert result.summary["parameter_robustness"]["production_parameter_policy"]["default_action"] == "do_not_promote_best_only"
    assert result.summary["parameter_search_plan"]["planned_run_count"] == len(result.summary["profiles"])
    assert result.summary["parameter_search_results"]["coverage_pct"] == "100"
    assert result.summary["parameter_search_results"]["production_parameter_staging"]["staging_allowed"] is False
    assert "train/test" in result.summary["parameter_search_results"]["reason"]
    assert result.summary["execution_profile_matrix"]["coverage_verdict"] == "ready"
    assert result.summary["execution_profile_matrix"]["missing_profiles"] == []
    assert "conservative" in result.summary["execution_profile_matrix"]["present_profiles"]
    assert {row["execution_model"] for row in result.summary["execution_model_rows"]} == {"ohlcv_close", "l2_orderfilled"}
    assert result.summary["execution_model_validation"]["status"] == "ready"
    assert result.summary["execution_model_validation"]["validation_verdict"] == "review"
    assert result.summary["execution_model_validation"]["missing_models"] == ["formula_slippage"]
    assert len(result.rows) == 2
    assert result.rows[1].data_quality == "status_mismatch"


def test_universe_spec_from_payload_supports_generic_presets():
    spec = universe_spec_from_payload(
        {
            "universe": "crypto_recent_ready",
            "limit": 25,
            "strategySpec": {"ignored": True},
            "universeSpec": {"requireResolved": True},
        }
    )

    assert spec.universe_name == "crypto_recent_ready"
    assert spec.universe_type == "category"
    assert spec.category == "crypto"
    assert spec.limit == 25
    assert spec.require_resolved is True


def test_watchlist_universe_requires_explicit_markets():
    spec = universe_spec_from_payload({"universe": "watchlist_slugs", "marketSlugs": ["a", "b"], "limit": 99})

    assert spec.universe_type == "watchlist"
    assert spec.market_slugs == ("a", "b")
    assert spec.limit == 99
