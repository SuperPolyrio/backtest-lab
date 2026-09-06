from __future__ import annotations

import json
import random
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from quant.backtest.orderfilled_v2_compare import compare_replay_results
from quant.backtest.orderfilled_v2_replay import (
    V2_EXECUTION_PROFILES,
    CapacityLedger,
    RequiredTradeWindow,
    V2MakerOrder,
    V2TakerOrder,
    V2TradePrint,
    build_required_trade_windows,
    build_v2_formula_slippage_baseline,
    build_v2_observed_fill_order,
    build_v2_observed_fill_replay_report,
    build_v2_replay_report,
    build_v2_robustness_report,
    build_v2_unsupported_report,
    calibrate_v2_live_fills,
    classify_v2_execution_grade,
    load_v2_calibration_actual_rows,
    load_v2_trade_prints,
    load_v2_trade_slices_for_orders,
    load_v2_wallet_fill_ticks,
    merge_required_trade_windows,
    persist_v2_replay_run,
    prepare_v2_trade_tape,
    replay_v2_maker_order,
    replay_v2_taker_order,
    replay_v2_taker_orders,
    replay_v2_taker_orders_reference,
    replay_v2_taker_orders_with_diagnostics,
    summarize_v2_results,
    wallet_fill_tick_from_row,
    wallet_fill_to_observed_order,
    with_v2_execution_profile,
)

pytestmark = pytest.mark.backtest_validation


def trade(
    trade_id: str,
    block: int,
    side: str,
    price: str,
    size: str,
    *,
    seconds: int = 0,
    market_id: int = 1,
    asset_id: str = "token-yes",
) -> V2TradePrint:
    return V2TradePrint(
        trade_id=trade_id,
        market_id=market_id,
        condition_id="condition",
        asset_id=asset_id,
        outcome="YES",
        block_number=block,
        block_time=datetime(2026, 2, 1, 0, 0, seconds, tzinfo=timezone.utc),
        tx_hash=f"0x{trade_id}",
        tx_index=0,
        tx_index_source="missing_in_orderfilled_fact",
        price=Decimal(price),
        size=Decimal(size),
        notional=Decimal(price) * Decimal(size),
        aggressor_side=side,  # type: ignore[arg-type]
        passive_side="SELL" if side == "BUY" else "BUY",  # type: ignore[arg-type]
        source_log_indexes=(block,),
        source_fill_count=1,
    )


def order(side: str, limit: str, size: str = "100") -> V2TakerOrder:
    return V2TakerOrder(
        order_id=f"o-{side}-{limit}",
        market_id=1,
        asset_id="token-yes",
        side=side,  # type: ignore[arg-type]
        limit_price=Decimal(limit),
        size=Decimal(size),
        signal_block=99,
        signal_ts=datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        latency_blocks=1,
        latency=timedelta(seconds=1),
        horizon_blocks=10,
        horizon=timedelta(minutes=5),
        participation_rate=Decimal("0.025"),
    )


def test_buy_taker_uses_only_future_same_side_limit_trade() -> None:
    result = replay_v2_taker_order(
        order("BUY", "0.53"),
        [
            trade("before", 99, "BUY", "0.52", "1000"),
            trade("sell-side", 100, "SELL", "0.51", "1000", seconds=2),
            trade("too-expensive", 101, "BUY", "0.55", "1000", seconds=3),
            trade("eligible", 102, "BUY", "0.52", "1000", seconds=4),
        ],
    )

    assert result.status == "PARTIAL_FILLED"
    assert result.filled_size == Decimal("25.0000000000")
    assert [fill.source_trade_id for fill in result.fills] == ["eligible"]
    assert result.reason_unfilled == "insufficient_trade_capacity"


def test_any_order_side_mode_uses_real_economic_trade_without_claiming_aggressor() -> None:
    opposite_only = trade("opposite-order-side", 100, "SELL", "0.52", "1000", seconds=2)
    strict = V2TakerOrder(
        **{
            **order("BUY", "0.53").__dict__,
            "participation_rate": Decimal("0.01"),
        }
    )
    relaxed = V2TakerOrder(
        **{
            **strict.__dict__,
            "trade_side_evidence_mode": "any_order_side",
        }
    )

    strict_result = replay_v2_taker_order(strict, [opposite_only])
    relaxed_result = replay_v2_taker_order(relaxed, [opposite_only])
    indexed_results, _ = replay_v2_taker_orders([relaxed], [opposite_only])

    assert strict_result.status == "NO_FILL"
    assert relaxed_result.status == "PARTIAL_FILLED"
    assert relaxed_result.filled_size == Decimal("10.0000000000")
    assert relaxed_result.fills[0].source_trade_id == "opposite-order-side"
    assert indexed_results[0].as_dict() == relaxed_result.as_dict()


def test_short_arrival_profile_rejects_late_trade() -> None:
    base = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="1").__dict__,
            "signal_block": None,
            "latency_blocks": 0,
            "participation_rate": Decimal("1"),
        }
    )
    short = with_v2_execution_profile(base, "probabilistic_taker_5s")
    result = replay_v2_taker_order(
        short,
        [trade("late", 101, "BUY", "0.52", "100", seconds=10)],
    )

    assert short.deadline_ts == short.arrival_ts + timedelta(seconds=5)
    assert result.status == "NO_FILL"


def test_sell_taker_uses_only_future_same_side_limit_trade() -> None:
    result = replay_v2_taker_order(
        order("SELL", "0.48"),
        [
            trade("buy-side", 100, "BUY", "0.50", "1000", seconds=2),
            trade("too-cheap", 101, "SELL", "0.46", "1000", seconds=3),
            trade("eligible", 102, "SELL", "0.49", "1000", seconds=4),
        ],
    )

    assert result.status == "PARTIAL_FILLED"
    assert result.filled_size == Decimal("25.0000000000")
    assert result.fills[0].exec_price == Decimal("0.4900000000")


def test_price_buffer_never_gives_better_than_historical_price() -> None:
    buy = V2TakerOrder(**{**order("BUY", "0.53").__dict__, "price_buffer": Decimal("0.01")})
    sell = V2TakerOrder(**{**order("SELL", "0.48").__dict__, "price_buffer": Decimal("0.01")})

    buy_result = replay_v2_taker_order(buy, [trade("buy", 100, "BUY", "0.52", "1000", seconds=2)])
    sell_result = replay_v2_taker_order(sell, [trade("sell", 100, "SELL", "0.49", "1000", seconds=2)])

    assert buy_result.fills[0].exec_price == Decimal("0.5300000000")
    assert buy_result.fills[0].exec_price >= buy_result.fills[0].historical_price
    assert buy_result.fills[0].price_buffer_paid == Decimal("0.0100000000")
    assert buy_result.avg_price_buffer == Decimal("0.0100000000")
    assert sell_result.fills[0].exec_price == Decimal("0.4800000000")
    assert sell_result.fills[0].exec_price <= sell_result.fills[0].historical_price
    assert sell_result.fills[0].price_buffer_paid == Decimal("0.0100000000")
    assert sell_result.avg_price_buffer == Decimal("0.0100000000")


def test_counterfactual_replay_can_exclude_signal_source_trade() -> None:
    source = trade("source", 100, "BUY", "0.52", "100", seconds=2)
    later = trade("later", 102, "BUY", "0.52", "100", seconds=3)
    blocked = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="1").__dict__,
            "signal_source_trade_id": "source",
            "exclude_signal_source_trade": True,
            "participation_rate": Decimal("1"),
            "horizon_blocks": 1,
        }
    )
    allowed = V2TakerOrder(**{**blocked.__dict__, "horizon_blocks": 3})

    blocked_result = replay_v2_taker_order(blocked, [source, later])
    allowed_result = replay_v2_taker_order(allowed, [source, later])

    assert blocked_result.status == "NO_FILL"
    assert blocked_result.reason_unfilled == "no_post_arrival_same_side_trade"
    assert allowed_result.status == "FILLED"
    assert [fill.source_trade_id for fill in allowed_result.fills] == ["later"]


def test_counterfactual_replay_excludes_source_tx_and_log_group() -> None:
    source = trade("source", 100, "BUY", "0.52", "100", seconds=2)
    same_log = trade("same-log", 101, "BUY", "0.52", "100", seconds=3)
    later = trade("later", 102, "BUY", "0.52", "100", seconds=4)
    same_log = replace(same_log, source_log_indexes=(100,))
    by_tx = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="1").__dict__,
            "signal_source_tx_hash": source.tx_hash,
            "exclude_signal_source_trade": True,
            "participation_rate": Decimal("1"),
        }
    )
    by_log = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="1").__dict__,
            "signal_source_log_indexes": (100,),
            "exclude_signal_source_trade": True,
            "participation_rate": Decimal("1"),
        }
    )

    tx_result = replay_v2_taker_order(by_tx, [source, later])
    log_result = replay_v2_taker_order(by_log, [same_log, later])

    assert tx_result.status == "FILLED"
    assert [fill.source_trade_id for fill in tx_result.fills] == ["later"]
    assert log_result.status == "FILLED"
    assert [fill.source_trade_id for fill in log_result.fills] == ["later"]


def test_pre_arrival_trade_quote_proxy_gate_blocks_contextless_fill() -> None:
    strict = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="1").__dict__,
            "require_pre_arrival_quote_proxy": True,
            "quote_proxy_ttl": timedelta(minutes=5),
            "participation_rate": Decimal("1"),
        }
    )
    with_context = V2TakerOrder(**strict.__dict__)

    no_context = replay_v2_taker_order(strict, [trade("future", 100, "BUY", "0.52", "100", seconds=2)])
    context = replay_v2_taker_order(
        with_context,
        [
            trade("pre", 99, "BUY", "0.52", "100", seconds=0),
            trade("future", 100, "BUY", "0.52", "100", seconds=2),
        ],
    )

    assert no_context.status == "NO_FILL"
    assert no_context.reason_unfilled == "missing_pre_arrival_trade_quote_proxy"
    assert context.status == "FILLED"


def test_density_gate_rejects_isolated_future_trade_print() -> None:
    dense = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="1").__dict__,
            "participation_rate": Decimal("1"),
            "min_future_eligible_trade_count": 2,
        }
    )

    isolated = replay_v2_taker_order(dense, [trade("single", 100, "BUY", "0.52", "100", seconds=2)])
    supported = replay_v2_taker_order(
        dense,
        [
            trade("first", 100, "BUY", "0.52", "100", seconds=2),
            trade("second", 101, "BUY", "0.52", "100", seconds=3),
        ],
    )

    assert isolated.status == "NO_FILL"
    assert isolated.reason_unfilled == "future_trade_count_too_low"
    assert supported.status == "FILLED"


def test_trailing_capacity_caps_future_trade_fill() -> None:
    capped = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="10").__dict__,
            "participation_rate": Decimal("1"),
            "min_trailing_same_side_trade_count": 1,
            "trailing_participation_rate": Decimal("0.10"),
        }
    )

    result = replay_v2_taker_order(
        capped,
        [
            trade("pre", 99, "BUY", "0.52", "20", seconds=0),
            trade("future", 100, "BUY", "0.52", "100", seconds=2),
        ],
    )

    assert result.status == "PARTIAL_FILLED"
    assert result.filled_size == Decimal("2.0000000000")
    assert result.reason_unfilled == "insufficient_trade_capacity"


def test_capacity_ledger_prevents_double_consuming_trade_print() -> None:
    shared = [trade("shared", 100, "BUY", "0.50", "1000", seconds=2)]
    first = V2TakerOrder(**{**order("BUY", "0.51").__dict__, "order_id": "first", "size": Decimal("20")})
    second = V2TakerOrder(**{**order("BUY", "0.51").__dict__, "order_id": "second", "size": Decimal("20")})

    results, ledger = replay_v2_taker_orders([first, second], shared)

    assert results[0].filled_size == Decimal("20.0000000000")
    assert results[1].filled_size == Decimal("5.0000000000")
    assert ledger.as_dict()["shared"] == "25.0000000000"


def test_fak_tif_caps_future_horizon_to_immediate_window() -> None:
    immediate = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="2").__dict__,
            "tif": "FAK",
            "participation_rate": Decimal("1"),
            "horizon_blocks": 10,
            "horizon": timedelta(minutes=5),
        }
    )

    result = replay_v2_taker_order(
        immediate,
        [
            trade("near", 100, "BUY", "0.52", "1", seconds=2),
            trade("late", 102, "BUY", "0.52", "10", seconds=3),
        ],
    )

    assert immediate.deadline_block == 101
    assert result.status == "PARTIAL_FILLED"
    assert result.filled_size == Decimal("1.0000000000")
    assert [fill.source_trade_id for fill in result.fills] == ["near"]
    assert result.reason_unfilled == "unfilled_remainder_cancelled_by_tif"


def test_fok_tif_requires_full_fill_and_does_not_consume_capacity() -> None:
    fok = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="10").__dict__,
            "tif": "FOK",
            "participation_rate": Decimal("1"),
        }
    )
    normal = V2TakerOrder(**{**order("BUY", "0.53", size="5").__dict__, "order_id": "normal", "participation_rate": Decimal("1")})
    rows = [trade("small", 100, "BUY", "0.52", "5", seconds=2)]
    ledger = CapacityLedger()

    rejected = replay_v2_taker_order(fok, rows, ledger)
    filled = replay_v2_taker_order(normal, rows, ledger)

    assert rejected.status == "NO_FILL"
    assert rejected.filled_size == Decimal("0E-10")
    assert rejected.reason_unfilled == "insufficient_trade_capacity"
    assert filled.status == "FILLED"
    assert ledger.as_dict() == {"small": "5.0000000000"}


def test_ioc_tif_allows_partial_fill_and_cancels_remainder() -> None:
    ioc = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="10").__dict__,
            "tif": "IOC",
            "participation_rate": Decimal("1"),
        }
    )

    result = replay_v2_taker_order(ioc, [trade("small", 100, "BUY", "0.52", "5", seconds=2)])

    assert result.status == "PARTIAL_FILLED"
    assert result.filled_size == Decimal("5.0000000000")
    assert result.reason_unfilled == "unfilled_remainder_cancelled_by_tif"


def test_market_window_cap_limits_shared_orders_in_same_window() -> None:
    rows = [
        trade("w1", 100, "BUY", "0.52", "100", seconds=2),
        trade("w2", 101, "BUY", "0.52", "100", seconds=3),
    ]
    first = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="5").__dict__,
            "order_id": "window-first",
            "participation_rate": Decimal("1"),
            "market_window_cap": Decimal("5"),
            "market_window_blocks": 10,
        }
    )
    second = V2TakerOrder(**{**first.__dict__, "order_id": "window-second"})

    results, ledger = replay_v2_taker_orders([first, second], rows)

    assert [row.status for row in results] == ["FILLED", "NO_FILL"]
    assert results[0].filled_size == Decimal("5.0000000000")
    assert results[1].reason_unfilled == "insufficient_trade_capacity"
    assert ledger.market_window_as_dict() == {"1:token-yes:BUY:10": "5.0000000000"}


def test_lob_holdout_calibrated_profile_rejects_low_validity_context() -> None:
    calibrated = with_v2_execution_profile(
        V2TakerOrder(**{**order("BUY", "0.53", size="1").__dict__, "participation_rate": Decimal("1")}),
        "lob_holdout_calibrated_fill_only",
    )

    result = replay_v2_taker_order(calibrated, [trade("future", 100, "BUY", "0.52", "100", seconds=2)])

    assert result.status == "NO_FILL"
    assert result.reason_unfilled == "missing_pre_arrival_quote_proxy"
    assert result.fill_validity_reason == "lob_holdout_calibrated_missing_quote_proxy"
    assert result.p_depth_valid == Decimal("0E-10")
    assert result.fill_validity_features is not None


def test_lob_holdout_profile_loads_execution_params_from_config(tmp_path, monkeypatch) -> None:
    profile_path = tmp_path / "fill_only_profile.json"
    profile_path.write_text(
        json.dumps(
            {
                "schema_version": "fill_only_lob_validity_profile_v1",
                "params": {
                    "participation_rate": "0.007",
                    "price_buffer_abs": "0.012",
                    "quote_proxy_ttl_seconds": "60",
                    "min_trailing_same_side_count": 2,
                    "min_trailing_same_side_volume": "5",
                    "trailing_volume_cap_fraction": "0.03",
                    "market_window_cap_fraction": "0.50",
                    "market_window_blocks": 20,
                    "min_future_eligible_trade_count": 3,
                    "p_depth_valid_threshold": "0.61",
                    "require_pre_arrival_quote_proxy": True,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("POLYDATA_QUANT_FILL_ONLY_LOB_VALIDITY_PROFILE", str(profile_path))

    configured = with_v2_execution_profile(order("BUY", "0.80", size="100"), "lob_holdout_calibrated_fill_only")

    assert configured.participation_rate == Decimal("0.007")
    assert configured.price_buffer == Decimal("0.012")
    assert configured.quote_proxy_ttl == timedelta(seconds=60)
    assert configured.min_trailing_same_side_trade_count == 2
    assert configured.min_trailing_same_side_volume == Decimal("5")
    assert configured.trailing_participation_rate == Decimal("0.03")
    assert configured.market_window_cap == Decimal("50.0000000000")
    assert configured.market_window_blocks == 20
    assert configured.min_future_eligible_trade_count == 3
    assert configured.execution_profile_name == "lob_holdout_calibrated_fill_only"
    assert configured.execution_profile_activation == "review_only_missing_stability"
    assert configured.execution_stability_grade is None
    assert configured.lob_validity_rule is not None
    assert configured.lob_validity_rule["min_probability"] == "0.61"


def test_lob_holdout_calibrated_rule_scales_order_capacity() -> None:
    scaled = V2TakerOrder(
        **{
            **order("BUY", "0.53", size="10").__dict__,
            "participation_rate": Decimal("1"),
            "lob_validity_rule": {
                "name": "unit-test-half-capacity",
                "min_probability": "0.50",
                "capacity_scale": True,
                "require_pre_arrival_quote_proxy": True,
                "min_trailing_same_side_count": 2,
                "min_future_eligible_trade_count": 1,
            },
        }
    )

    result = replay_v2_taker_order(
        scaled,
        [
            trade("pre", 99, "BUY", "0.52", "100", seconds=0),
            trade("future", 100, "BUY", "0.52", "100", seconds=2),
        ],
    )

    assert result.status == "PARTIAL_FILLED"
    assert result.filled_size == Decimal("5.0000000000")
    assert result.p_depth_valid == Decimal("0.5000000000")
    assert result.fill_validity_rule is not None
    assert result.fill_validity_features is not None
    assert result.as_dict()["p_depth_valid"] == Decimal("0.5000000000")


def test_indexed_taker_batch_matches_reference_results_and_ledger() -> None:
    rows = [
        trade("noise-market", 100, "BUY", "0.50", "999", seconds=1, market_id=2),
        trade("before", 99, "BUY", "0.50", "999", seconds=1),
        trade("buy-1", 100, "BUY", "0.50", "100", seconds=2),
        trade("sell-1", 101, "SELL", "0.49", "100", seconds=3),
        trade("buy-too-high", 102, "BUY", "0.55", "100", seconds=4),
        trade("buy-2", 103, "BUY", "0.51", "100", seconds=5),
    ]
    orders = [
        V2TakerOrder(**{**order("BUY", "0.52").__dict__, "order_id": "buy-a", "size": Decimal("2")}),
        V2TakerOrder(**{**order("BUY", "0.52").__dict__, "order_id": "buy-b", "size": Decimal("2")}),
        V2TakerOrder(**{**order("SELL", "0.48").__dict__, "order_id": "sell-a", "size": Decimal("2")}),
    ]

    indexed_results, indexed_ledger = replay_v2_taker_orders(orders, rows)
    reference_results, reference_ledger = replay_v2_taker_orders_reference(orders, rows)

    assert [result.as_dict() for result in indexed_results] == [result.as_dict() for result in reference_results]
    assert indexed_ledger.as_dict() == reference_ledger.as_dict()
    comparison = compare_replay_results(reference_results, reference_ledger, indexed_results, indexed_ledger)
    assert comparison["diff_count"] == 0
    assert comparison["reference_hash"] == comparison["indexed_hash"]


def test_randomized_fill_only_invariants_hold() -> None:
    rng = random.Random(20260709)
    rows = [
        trade(
            f"rnd-{idx}",
            90 + idx,
            "BUY" if idx % 2 == 0 else "SELL",
            str(Decimal("0.30") + Decimal(rng.randint(0, 40)) / Decimal("100")),
            str(rng.randint(1, 200)),
            seconds=min(59, idx),
        )
        for idx in range(40)
    ]
    orders = []
    for idx in range(20):
        side = "BUY" if idx % 2 == 0 else "SELL"
        orders.append(
            V2TakerOrder(
                **{
                    **order(side, "0.70" if side == "BUY" else "0.30", size=str(rng.randint(1, 20))).__dict__,
                    "order_id": f"rnd-order-{idx}",
                    "signal_block": 95 + idx,
                    "horizon_blocks": rng.randint(0, 8),
                    "participation_rate": Decimal("0.05"),
                    "price_buffer": Decimal("0.005"),
                    "signal_source_trade_id": "rnd-10" if idx % 5 == 0 else None,
                    "exclude_signal_source_trade": idx % 5 == 0,
                }
            )
        )

    results, ledger = replay_v2_taker_orders(orders, rows)

    assert len(results) == len(orders)
    assert all(result.filled_size <= result.requested_size for result in results)
    for result in results:
        source_ids = {fill.source_trade_id for fill in result.fills}
        if result.order_id.endswith(("0", "5")):
            assert "rnd-10" not in source_ids
        for fill in result.fills:
            assert fill.fill_block >= (result.arrival_block or 0)
            if result.side == "BUY":
                assert fill.exec_price <= result.limit_price
            else:
                assert fill.exec_price >= result.limit_price
    assert all(Decimal(value) >= 0 for value in ledger.as_dict().values())


def test_indexed_taker_diagnostics_report_candidate_scan_distribution() -> None:
    rows = [
        trade("buy-1", 100, "BUY", "0.50", "100", seconds=2),
        trade("buy-2", 101, "BUY", "0.51", "100", seconds=3),
        trade("sell-1", 102, "SELL", "0.49", "100", seconds=4),
        trade("buy-outside", 200, "BUY", "0.50", "100", seconds=5),
    ]
    first = V2TakerOrder(**{**order("BUY", "0.49").__dict__, "order_id": "scan-all", "size": Decimal("10")})
    second = V2TakerOrder(**{**order("SELL", "0.48").__dict__, "order_id": "scan-sell", "size": Decimal("1")})

    _, _, diagnostics = replay_v2_taker_orders_with_diagnostics([first, second], rows)

    assert diagnostics.orders_count == 2
    assert diagnostics.trade_rows_indexed == 4
    assert diagnostics.trade_groups == 2
    assert diagnostics.candidate_rows_scanned == 3
    assert diagnostics.naive_rows_scanned == 8
    assert diagnostics.scan_reduction_ratio == Decimal("0.3750000000")
    assert diagnostics.candidate_rows_per_order_p50 == Decimal("2.0000000000")
    assert diagnostics.candidate_rows_per_order_p95 == Decimal("2.0000000000")
    assert diagnostics.candidate_rows_per_order_max == 2
    assert diagnostics.matching_sec >= Decimal("0")


def test_prepared_trade_tape_reuses_index_without_changing_results() -> None:
    rows = [
        trade("buy-1", 100, "BUY", "0.50", "100", seconds=2),
        trade("buy-2", 101, "BUY", "0.51", "100", seconds=3),
        trade("sell-1", 102, "SELL", "0.49", "100", seconds=4),
    ]
    orders = [
        V2TakerOrder(
            **{**order("BUY", "0.52").__dict__, "order_id": "prepared-a", "size": Decimal("2")}
        ),
        V2TakerOrder(
            **{**order("BUY", "0.52").__dict__, "order_id": "prepared-b", "size": Decimal("2")}
        ),
    ]

    expected, expected_ledger, _ = replay_v2_taker_orders_with_diagnostics(orders, rows)
    prepared = prepare_v2_trade_tape(rows)
    actual, actual_ledger, diagnostics = replay_v2_taker_orders_with_diagnostics(
        orders,
        ledger=CapacityLedger(),
        prepared_tape=prepared,
    )

    assert [row.as_dict() for row in actual] == [row.as_dict() for row in expected]
    assert actual_ledger.as_dict() == expected_ledger.as_dict()
    assert diagnostics.index_reused is True
    assert diagnostics.index_build_sec == Decimal("0")
    assert diagnostics.trade_rows_indexed == len(rows)
    assert prepared.trade_groups == 2


def test_capacity_ledger_delta_tracking_reports_only_current_batch() -> None:
    rows = [trade("shared", 100, "BUY", "0.50", "1000", seconds=2)]
    first = V2TakerOrder(
        **{**order("BUY", "0.52").__dict__, "order_id": "delta-a", "size": Decimal("2")}
    )
    second = V2TakerOrder(
        **{**order("BUY", "0.52").__dict__, "order_id": "delta-b", "size": Decimal("3")}
    )
    ledger = CapacityLedger()

    ledger.begin_delta_tracking()
    first_results, _ = replay_v2_taker_orders([first], rows, ledger=ledger)
    first_delta = ledger.drain_delta()
    ledger.begin_delta_tracking()
    second_results, _ = replay_v2_taker_orders([second], rows, ledger=ledger)
    second_delta = ledger.drain_delta()

    assert first_delta.source_trade_consumed == {"shared": first_results[0].filled_size}
    assert second_delta.source_trade_consumed == {"shared": second_results[0].filled_size}
    assert first_delta.market_window_consumed == {}
    assert second_delta.market_window_consumed == {}
    assert ledger.as_dict()["shared"] == str(
        first_results[0].filled_size + second_results[0].filled_size
    )


def test_capacity_ledger_prunes_only_source_ids_absent_from_next_slice() -> None:
    ledger = CapacityLedger()
    ledger.consume("past", Decimal("1"))
    ledger.consume("overlap", Decimal("2"))

    removed = ledger.retain_source_trade_ids(
        trade_id for trade_id in ("overlap", "future")
    )

    assert removed == 1
    assert ledger.as_dict() == {"overlap": "2.0000000000"}


def test_indexed_metamorphic_relations_hold() -> None:
    rows = [
        trade("buy-1", 100, "BUY", "0.50", "100", seconds=2),
        trade("buy-2", 101, "BUY", "0.54", "100", seconds=3),
        trade("buy-3", 102, "BUY", "0.56", "100", seconds=4),
        trade("sell-1", 100, "SELL", "0.50", "100", seconds=2),
        trade("sell-2", 101, "SELL", "0.47", "100", seconds=3),
    ]
    strict_buy = V2TakerOrder(**{**order("BUY", "0.50", size="100").__dict__, "participation_rate": Decimal("0.01"), "horizon_blocks": 1})
    loose_buy = V2TakerOrder(**{**strict_buy.__dict__, "limit_price": Decimal("0.55")})
    higher_participation = V2TakerOrder(**{**strict_buy.__dict__, "participation_rate": Decimal("0.025")})
    longer_horizon = V2TakerOrder(**{**strict_buy.__dict__, "horizon_blocks": 10})
    worse_buffer = V2TakerOrder(**{**loose_buy.__dict__, "price_buffer": Decimal("0.10")})
    strict_sell = V2TakerOrder(**{**order("SELL", "0.50", size="100").__dict__, "horizon_blocks": 10})
    loose_sell = V2TakerOrder(**{**strict_sell.__dict__, "limit_price": Decimal("0.47")})

    strict_buy_result = replay_v2_taker_order(strict_buy, rows)
    assert replay_v2_taker_order(loose_buy, rows).filled_size >= strict_buy_result.filled_size
    assert replay_v2_taker_order(higher_participation, rows).filled_size >= strict_buy_result.filled_size
    assert replay_v2_taker_order(longer_horizon, rows).filled_size >= strict_buy_result.filled_size
    assert replay_v2_taker_order(worse_buffer, rows).filled_size <= replay_v2_taker_order(loose_buy, rows).filled_size
    assert replay_v2_taker_order(loose_sell, rows).filled_size >= replay_v2_taker_order(strict_sell, rows).filled_size
    assert replay_v2_taker_order(strict_buy, rows[:1]).filled_size <= strict_buy_result.filled_size


def test_required_trade_windows_follow_order_side_and_block_window() -> None:
    buy = V2TakerOrder(**{**order("BUY", "0.53").__dict__, "order_id": "buy-window"})
    sell = V2TakerOrder(**{**order("SELL", "0.48").__dict__, "order_id": "sell-window", "horizon_blocks": 20})

    windows = build_required_trade_windows([buy, sell])

    assert windows == [
        RequiredTradeWindow(1, "token-yes", "BUY", 100, 110),
        RequiredTradeWindow(1, "token-yes", "SELL", 100, 120),
    ]


def test_required_trade_windows_support_timestamp_only_orders() -> None:
    timestamp_order = V2TakerOrder(
        **{
            **order("BUY", "0.53").__dict__,
            "order_id": "time-window",
            "signal_block": None,
            "latency_blocks": 0,
            "horizon_blocks": None,
            "trade_slice_lookback": timedelta(seconds=20),
            "horizon": timedelta(seconds=30),
        }
    )

    windows = build_required_trade_windows(
        [timestamp_order],
        source_min_block=1,
        source_max_block=1_000,
    )

    assert windows == [
        RequiredTradeWindow(
            market_id=1,
            asset_id="token-yes",
            aggressor_side="BUY",
            start_block=None,
            end_block=None,
            start_ts=datetime(2026, 1, 31, 23, 59, 41, tzinfo=timezone.utc),
            end_ts=datetime(2026, 2, 1, 0, 0, 31, tzinfo=timezone.utc),
            source_min_block=1,
            source_max_block=1_000,
        )
    ]


def test_timestamp_trade_slice_loader_merges_windows_and_pins_source_block() -> None:
    class FakeClickHouse:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def query_json_rows(self, sql: str, timeout_seconds: int | None = None):
            self.queries.append(sql)
            return []

    base = V2TakerOrder(
        **{
            **order("BUY", "0.53").__dict__,
            "signal_block": None,
            "latency_blocks": 0,
            "horizon_blocks": None,
            "trade_slice_lookback": timedelta(seconds=5),
            "horizon": timedelta(seconds=10),
        }
    )
    second = V2TakerOrder(
        **{
            **base.__dict__,
            "order_id": "time-second",
            "signal_ts": base.signal_ts + timedelta(seconds=8),
        }
    )
    client = FakeClickHouse()

    loaded = load_v2_trade_slices_for_orders(
        [base, second],
        client=client,  # type: ignore[arg-type]
        source_min_block=10,
        source_max_block=1_000,
    )

    assert loaded.windows_count == 2
    assert loaded.merged_windows_count == 1
    assert loaded.db_query_count == 1
    sql = client.queries[0]
    assert "block_number BETWEEN 10 AND 1000" in sql
    assert "block_time BETWEEN" in sql
    assert "ORDER BY block_number ASC, tx_index ASC" in sql


def test_merge_required_trade_windows_merges_overlap_and_gap_by_group() -> None:
    windows = [
        RequiredTradeWindow(1, "token-yes", "BUY", 100, 110),
        RequiredTradeWindow(1, "token-yes", "BUY", 111, 120),
        RequiredTradeWindow(1, "token-yes", "BUY", 140, 150),
        RequiredTradeWindow(1, "token-yes", "SELL", 105, 115),
    ]

    merged = merge_required_trade_windows(windows, merge_gap_blocks=1)

    assert merged == [
        RequiredTradeWindow(1, "token-yes", "BUY", 100, 120),
        RequiredTradeWindow(1, "token-yes", "BUY", 140, 150),
        RequiredTradeWindow(1, "token-yes", "SELL", 105, 115),
    ]


def test_trade_slice_loader_queries_merged_windows_not_each_order() -> None:
    class FakeClickHouse:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def query_json_rows(self, sql: str, timeout_seconds: int | None = None):
            self.queries.append(sql)
            return []

    first = V2TakerOrder(**{**order("BUY", "0.53").__dict__, "order_id": "first"})
    second = V2TakerOrder(**{**order("BUY", "0.53").__dict__, "order_id": "second", "signal_block": 104, "horizon_blocks": 10})
    sell = V2TakerOrder(**{**order("SELL", "0.48").__dict__, "order_id": "sell"})
    client = FakeClickHouse()

    result = load_v2_trade_slices_for_orders([first, second, sell], client=client, merge_gap_blocks=0)  # type: ignore[arg-type]

    sql = "\n".join(client.queries)
    assert result.windows_count == 3
    assert result.merged_windows_count == 2
    assert result.db_query_count == 2
    assert "FROM trade_prints_one_sided" in sql
    assert "aggressor_side = 'BUY'" in sql
    assert "aggressor_side = 'SELL'" in sql
    assert "block_number BETWEEN 100 AND 115" in sql
    assert "maker_fill_ticks" not in sql
    assert "block_trade_bars_sparse" not in sql
    assert "time_bars" not in sql


def test_fill_size_respects_order_size_and_participation_cap() -> None:
    result = replay_v2_taker_order(
        V2TakerOrder(**{**order("BUY", "0.51").__dict__, "size": Decimal("10")}),
        [trade("cap", 100, "BUY", "0.50", "100", seconds=2)],
    )

    assert result.filled_size <= result.requested_size
    assert result.filled_size <= Decimal("2.5000000000")
    assert result.filled_size == Decimal("2.5000000000")


def test_no_trade_evidence_means_no_fill() -> None:
    result = replay_v2_taker_order(order("BUY", "0.53"), [])

    assert result.status == "NO_FILL"
    assert result.filled_size == Decimal("0E-10")
    assert result.reason_unfilled == "no_post_arrival_same_side_trade"


def test_fill_audit_contains_source_trade_fields() -> None:
    result = replay_v2_taker_order(order("BUY", "0.53", size="1"), [trade("audit", 100, "BUY", "0.52", "100", seconds=2)])
    row = result.as_dict()["fills"][0]

    assert row["order_id"] == result.order_id
    assert row["source_trade_id"] == "audit"
    assert row["source_tx_hash"] == "0xaudit"
    assert row["source_log_indexes"] == [100]
    assert row["historical_price"] == Decimal("0.52")
    assert row["price_buffer_paid"] == Decimal("0E-10")
    assert row["participation_rate"] == Decimal("0.0250000000")


def test_result_serialization_matches_legacy_recursive_asdict() -> None:
    result = replay_v2_taker_order(
        order("BUY", "0.53", size="1"),
        [trade("serialization", 100, "BUY", "0.52", "100", seconds=2)],
    )
    expected = asdict(result)
    expected["arrival_ts"] = (
        result.arrival_ts.isoformat() if result.arrival_ts is not None else None
    )
    expected["fills"] = []
    for fill in result.fills:
        fill_row = asdict(fill)
        fill_row["fill_ts"] = fill.fill_ts.isoformat()
        fill_row["source_log_indexes"] = list(fill.source_log_indexes)
        expected["fills"].append(fill_row)

    assert result.as_dict() == expected


def test_summary_reports_basic_metrics() -> None:
    results, _ = replay_v2_taker_orders(
        [V2TakerOrder(**{**order("BUY", "0.53").__dict__, "size": Decimal("1")})],
        [trade("summary", 100, "BUY", "0.52", "100", seconds=2)],
    )

    summary = summarize_v2_results(results)

    assert summary["attempted_orders"] == 1
    assert summary["filled_orders"] == 1
    assert summary["simulated_volume"] == Decimal("1.0000000000")
    assert summary["eligible_historical_volume"] == Decimal("100.0000000000")
    assert summary["avg_fill_delay_seconds"] == Decimal("1.0000000000")
    assert summary["avg_price_buffer"] == Decimal("0E-10")
    assert summary["filled_notional"] == Decimal("0.5200000000")
    assert summary["cash_delta"] == Decimal("-0.5200000000")
    assert summary["position_delta"] == Decimal("1.0000000000")


def test_cash_and_position_delta_follow_order_side() -> None:
    buy = V2TakerOrder(**{**order("BUY", "0.53").__dict__, "order_id": "buy", "size": Decimal("1")})
    sell = V2TakerOrder(**{**order("SELL", "0.48").__dict__, "order_id": "sell", "size": Decimal("1")})

    buy_result = replay_v2_taker_order(buy, [trade("buy", 100, "BUY", "0.52", "100", seconds=2)])
    sell_result = replay_v2_taker_order(sell, [trade("sell", 100, "SELL", "0.49", "100", seconds=2)])

    assert buy_result.cash_delta == Decimal("-0.5200000000")
    assert buy_result.position_delta == Decimal("1.0000000000")
    assert sell_result.cash_delta == Decimal("0.4900000000")
    assert sell_result.position_delta == Decimal("-1.0000000000")


def test_execution_profiles_apply_named_modes() -> None:
    base = order("BUY", "0.53")

    strict = with_v2_execution_profile(base, "strict_audit")
    conservative = with_v2_execution_profile(base, "conservative_trade_tape")
    calibrated = with_v2_execution_profile(base, "lob_holdout_calibrated_fill_only")
    probabilistic = with_v2_execution_profile(base, "probabilistic_trade_tape")
    probabilistic_conservative = with_v2_execution_profile(base, "probabilistic_conservative")
    probabilistic_source_confirmed = with_v2_execution_profile(base, "probabilistic_source_confirmed")
    optimistic = with_v2_execution_profile(base, "optimistic_sensitivity")

    assert set(V2_EXECUTION_PROFILES) == {
        "strict_audit",
        "conservative_trade_tape",
        "lob_holdout_calibrated_fill_only",
        "probabilistic_trade_tape",
        "probabilistic_conservative",
        "probabilistic_source_confirmed",
        "probabilistic_taker_5s",
        "probabilistic_taker_30s",
        "probabilistic_taker_120s",
        "probabilistic_taker_30s_any_order_side",
        "probabilistic_taker_120s_any_order_side",
        "optimistic_sensitivity",
    }
    assert strict.participation_rate < conservative.participation_rate < optimistic.participation_rate
    assert calibrated.lob_validity_rule is not None
    assert probabilistic.orderfilled_probability_profile is not None
    assert probabilistic.orderfilled_probability_profile["capacity_variant"] == "expected"
    assert probabilistic_conservative.orderfilled_probability_profile is not None
    assert probabilistic_conservative.orderfilled_probability_profile["capacity_variant"] == "conservative"
    assert probabilistic_source_confirmed.orderfilled_probability_profile is not None
    assert probabilistic_source_confirmed.orderfilled_probability_profile["capacity_variant"] == "source_confirmed"
    assert probabilistic.lob_validity_rule is None
    assert strict.latency > conservative.latency > optimistic.latency


def test_report_outputs_mode_capacity_latency_and_horizon_curves() -> None:
    report = build_v2_replay_report(
        [V2TakerOrder(**{**order("BUY", "0.53").__dict__, "size": Decimal("1")})],
        [
            trade("report-pre", 98, "BUY", "0.52", "100", seconds=0),
            trade("report", 100, "BUY", "0.52", "100", seconds=2),
        ],
        capacity_rates=(Decimal("0.005"), Decimal("0.025")),
        latency_values=(timedelta(0), timedelta(seconds=1)),
        horizon_values=(timedelta(seconds=5), timedelta(minutes=5)),
        settlement_price=Decimal("1"),
        fee_bps=Decimal("10"),
    )

    assert report["execution_grade"] == "trade_tape_participation"
    assert set(report["mode_comparison"]) == {
        "strict_audit",
        "conservative_trade_tape",
        "probabilistic_conservative",
        "probabilistic_trade_tape",
        "probabilistic_source_confirmed",
        "optimistic_sensitivity",
    }
    assert [row["value"] for row in report["capacity_curve"]] == ["0.005", "0.025"]
    assert [row["value"] for row in report["latency_curve"]] == ["0E-10s", "1.0000000000s"]
    assert [row["value"] for row in report["horizon_curve"]] == ["5.0000000000s", "300.0000000000s"]
    assert report["pnl_assumption"]["status"] == "estimated"
    assert sum(report["unfilled_reason_distribution"].values()) == 1
    assert "low_orderfilled_fill_probability" not in report["unfilled_reason_distribution"]


def test_maker_strict_no_fill_and_phantom_queue_sensitivity() -> None:
    maker = V2MakerOrder(
        order_id="maker-buy",
        market_id=1,
        asset_id="token-yes",
        side="BUY",
        limit_price=Decimal("0.45"),
        size=Decimal("10"),
        signal_block=99,
        signal_ts=datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
    )
    strict = replay_v2_maker_order(maker, [trade("hit", 100, "SELL", "0.45", "1000", seconds=2)], "strict_audit")
    optimistic = replay_v2_maker_order(maker, [trade("hit", 100, "SELL", "0.45", "1000", seconds=2)], "optimistic_sensitivity")

    assert strict.status == "WORKING_BUT_NON_EXECUTABLE_IN_ORDERFILLED_ONLY"
    assert strict.filled_size == Decimal("0E-10")
    assert strict.reason_unfilled == "maker_strict_no_fill"
    assert optimistic.status == "NO_FILL"
    assert optimistic.initial_phantom_queue == Decimal("100.0000000000")
    assert optimistic.remaining_phantom_queue == Decimal("50.0000000000")


def test_maker_phantom_queue_can_fill_after_queue_clears() -> None:
    maker = V2MakerOrder(
        order_id="maker-sell",
        market_id=1,
        asset_id="token-yes",
        side="SELL",
        limit_price=Decimal("0.55"),
        size=Decimal("10"),
        signal_block=99,
        signal_ts=datetime(2026, 2, 1, 0, 0, 0, tzinfo=timezone.utc),
        latency=timedelta(seconds=1),
        horizon=timedelta(minutes=5),
    )
    result = replay_v2_maker_order(
        maker,
        [trade("hit", 100, "BUY", "0.55", "3000", seconds=2)],
        "optimistic_sensitivity",
    )

    assert result.status == "FILLED"
    assert result.filled_size == Decimal("10.0000000000")
    assert result.fills[0].exec_price == Decimal("0.5500000000")


def test_execution_grade_classifier_names_supported_layers() -> None:
    assert classify_v2_execution_grade(observed_fill=True) == "observed_fill_replay"
    assert classify_v2_execution_grade() == "trade_tape_participation"
    assert classify_v2_execution_grade(formula_only=True, trade_tape=False) == "formula_slippage_baseline"
    assert classify_v2_execution_grade(needs_l2_or_queue=True) == "unsupported"


def test_observed_fill_replay_builds_order_from_trade_evidence() -> None:
    source = trade("observed", 100, "BUY", "0.52", "100", seconds=2)
    observed_order = build_v2_observed_fill_order(source, size_fraction=Decimal("0.025"), allowed_buffer=Decimal("0.01"))

    assert observed_order.side == "BUY"
    assert observed_order.limit_price == Decimal("0.5300000000")
    assert observed_order.size == Decimal("2.5000000000")
    result = replay_v2_taker_order(observed_order, [source])
    assert result.status == "FILLED"
    assert result.fills[0].source_trade_id == "observed"


def test_observed_fill_replay_report_is_grade_a_style_reconstruction() -> None:
    report = build_v2_observed_fill_replay_report(
        [trade("observed-buy", 100, "BUY", "0.52", "100", seconds=2)],
        size_fraction=Decimal("0.025"),
    )

    assert report["execution_grade"] == "observed_fill_replay"
    assert report["summary"]["filled_orders"] == 1
    assert report["orders"][0]["fills"][0]["source_trade_id"] == "observed-buy"


def test_live_fill_calibration_flags_false_positive_and_size_error() -> None:
    predicted = replay_v2_taker_order(
        V2TakerOrder(**{**order("BUY", "0.53").__dict__, "order_id": "live-1", "size": Decimal("1")}),
        [trade("predicted", 100, "BUY", "0.52", "100", seconds=2)],
    )

    report = calibrate_v2_live_fills(
        [predicted],
        [{"order_id": "live-1", "filled_size": "0", "avg_price": "0"}],
    )

    assert report["status"] == "ready"
    assert report["false_positive_fills"] == 1
    assert report["false_negative_fills"] == 0
    assert report["avg_abs_size_error"] == Decimal("1.0000000000")


def test_calibration_actual_loader_reads_existing_calibration_table() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[object]]] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql: str, params=None) -> None:
            self.calls.append((sql, list(params or [])))

        def fetchall(self):
            return [{"order_id": "o1", "filled_size": Decimal("1"), "avg_price": Decimal("0.5")}]

    class FakeConn:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

    conn = FakeConn()

    rows = load_v2_calibration_actual_rows(conn, run_id=7, source="live-shadow", limit=5)

    sql, params = conn.cursor_obj.calls[-1]
    assert "quant.quant_backtest_calibration_orders" in sql
    assert params == [7, "live-shadow", 5]
    assert rows == [{"order_id": "o1", "filled_size": Decimal("1"), "avg_price": Decimal("0.5")}]


def test_formula_slippage_baseline_is_labeled_non_audited_baseline() -> None:
    result = build_v2_formula_slippage_baseline(
        V2TakerOrder(**{**order("BUY", "0.53").__dict__, "size": Decimal("10")}),
        reference_price=Decimal("0.52"),
        available_volume=Decimal("100"),
        spread=Decimal("0.01"),
        slippage=Decimal("0.005"),
        volume_limit=Decimal("0.025"),
    )

    assert result["execution_grade"] == "formula_slippage_baseline"
    assert result["exec_price"] == Decimal("0.5300000000")
    assert result["filled_size"] == Decimal("2.5000000000")
    assert "no OrderFilled source trade audit" in result["audit_warning"]


def test_unsupported_report_marks_depth_and_queue_requests_out_of_scope() -> None:
    report = build_v2_unsupported_report("maker queue requires full LOB", requested_layer="l3_queue")

    assert report["execution_grade"] == "unsupported"
    assert report["requested_layer"] == "l3_queue"
    assert report["status"] == "unsupported"


def test_robustness_report_outputs_parameter_walk_forward_category_and_regime_splits() -> None:
    rows = [
        trade("r1", 100, "BUY", "0.52", "100", seconds=2, market_id=1),
        trade("r2", 105, "BUY", "0.51", "200", seconds=4, market_id=1),
        trade("r3", 110, "SELL", "0.49", "300", seconds=6, market_id=2),
    ]
    orders = [
        V2TakerOrder(**{**order("BUY", "0.53").__dict__, "order_id": "robust-buy", "market_id": 1, "size": Decimal("1")}),
        V2TakerOrder(**{**order("SELL", "0.48").__dict__, "order_id": "robust-sell", "market_id": 2, "size": Decimal("1")}),
    ]

    report = build_v2_robustness_report(orders, rows, category_by_market={1: "sports", 2: "politics"}, walk_forward_splits=2)

    assert report["status"] == "ready"
    assert len(report["parameter_grid"]) == 3
    assert len(report["walk_forward"]) == 2
    assert {row["category"] for row in report["category_split"]} == {"sports", "politics"}
    assert {row["regime"] for row in report["regime_split"]} == {"low_trade_size", "high_trade_size"}
    assert report["overfit_warning"]["parameter_count"] == 3
    assert "deflated_sharpe_ratio_heuristic" in report["overfit_warning"]
    assert report["overfit_warning"]["deflated_sharpe_ratio_heuristic"]["trial_count"] == 3


def test_persist_v2_replay_run_writes_existing_backtest_tables() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...] | None]] = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql: str, params=None) -> None:
            self.calls.append((sql, params))

        def fetchone(self):
            return {"run_id": 42}

    class FakeConn:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

    order_row = V2TakerOrder(**{**order("BUY", "0.53").__dict__, "order_id": "persist-1", "size": Decimal("1")})
    result = replay_v2_taker_order(order_row, [trade("persist", 100, "BUY", "0.52", "100", seconds=2)])
    conn = FakeConn()

    run_id = persist_v2_replay_run(
        conn,
        market_slug="market",
        token_side="YES",
        orders=[order_row],
        results=[result],
        report={"status": "ready"},
    )

    sql_text = "\n".join(sql for sql, _ in conn.cursor_obj.calls)
    assert run_id == 42
    assert "quant.quant_backtest_runs" in sql_text
    assert "quant.quant_backtest_parameters" in sql_text
    assert "quant.quant_backtest_metrics" in sql_text
    assert "quant.quant_backtest_orders" in sql_text
    assert "quant.quant_backtest_ledger" in sql_text
    assert "quant.quant_backtest_events" in sql_text


def test_wallet_fill_loader_reads_maker_fill_ticks_only() -> None:
    class FakeClickHouse:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def query_json_rows(self, sql: str, timeout_seconds: int | None = None):
            self.queries.append(sql)
            return []

    client = FakeClickHouse()

    load_v2_wallet_fill_ticks(wallet="0xABC", market_id=1, from_block=10, to_block=20, client=client)  # type: ignore[arg-type]

    sql = client.queries[-1]
    assert "FROM maker_fill_ticks" in sql
    assert "maker = '0xabc' OR taker = '0xabc'" in sql
    assert "orderfilled_quarantine" not in sql


def test_wallet_fill_to_observed_order_uses_wallet_role_side() -> None:
    row = {
        "fill_id": "fill1",
        "wallet_role": "taker",
        "market_id": 1,
        "condition_id": "condition",
        "asset_id": "token-yes",
        "outcome": "YES",
        "block_number": 100,
        "block_time": "2026-02-01 00:00:02",
        "tx_hash": "0xabc",
        "log_index": 7,
        "order_hash": "0xorder",
        "price": "0.52",
        "size_shares": "100",
        "fee_usdc": "0",
        "passive_side": "SELL",
        "aggressor_side": "BUY",
    }
    taker_fill = wallet_fill_tick_from_row(row, wallet="0xwallet")
    maker_fill = wallet_fill_tick_from_row({**row, "wallet_role": "maker"}, wallet="0xwallet")

    taker_order = wallet_fill_to_observed_order(taker_fill)
    maker_order = wallet_fill_to_observed_order(maker_fill)

    assert taker_order.side == "BUY"
    assert maker_order.side == "SELL"
    assert taker_order.size == Decimal("2.5000000000")


def test_loader_reads_only_trade_prints_one_sided() -> None:
    class FakeClickHouse:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def query_json_rows(self, sql: str, timeout_seconds: int | None = None):
            self.queries.append(sql)
            return []

    client = FakeClickHouse()

    load_v2_trade_prints(market_id=1, asset_id="TOKEN", from_block=1, to_block=2, client=client)  # type: ignore[arg-type]

    sql = client.queries[-1]
    assert "FROM trade_prints_one_sided" in sql
    assert "orderfilled_quarantine" not in sql
    assert "block_trade_bars_sparse" not in sql
