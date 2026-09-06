from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from quant.backtest.frozen_order_catalog import (
    FrozenOrderCatalog,
    FrozenOrderCatalogBuilder,
    convert_v2_frozen_order_catalog_to_v3,
    write_frozen_order_catalog,
)
from quant.backtest.orderfilled_v2_replay import V2TakerOrder, V2TradePrint
from quant.backtest.profile_worker import ProfileWorkerTask, run_profile_workers
from quant.backtest.trade_catalog import ParquetTradeCatalogBuilder
from quant.backtest.trade_only_v3 import LiquidityIntent, TradeOnlyOrder

BASE_TS = datetime(2026, 7, 1, tzinfo=timezone.utc)


def _trade(index: int) -> V2TradePrint:
    return V2TradePrint(
        trade_id=f"trade-{index}",
        market_id=9,
        condition_id="condition",
        asset_id="asset",
        outcome="YES",
        block_number=100 + index,
        block_time=BASE_TS + timedelta(seconds=index),
        tx_hash=f"0x{index}",
        tx_index=index,
        tx_index_source="receipt",
        price=Decimal("0.55"),
        size=Decimal(100),
        notional=Decimal(55),
        aggressor_side="BUY",
        passive_side="SELL",
        source_log_indexes=(index,),
        source_fill_count=1,
        trade_group_id=f"group-{index}",
    )


def _v3_order(index: int) -> TradeOnlyOrder:
    signal = _trade(index)
    return TradeOnlyOrder(
        order_id=f"order-{index}",
        market_id=9,
        asset_id="asset",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=signal.block_number,
        signal_ts=signal.block_time,
        tif="GTD",
        liquidity_intent=LiquidityIntent.TAKER,
        latency=timedelta(0),
        latency_blocks=0,
        horizon=timedelta(seconds=10),
        horizon_blocks=10,
        lookback=timedelta(seconds=10),
        lookback_blocks=10,
        signal_source_trade_id=signal.trade_id,
        random_seed=73,
    )


def _write_inputs(tmp_path):
    trade_root = tmp_path / "trades"
    builder = ParquetTradeCatalogBuilder(
        trade_root,
        source_pin="source",
        profile_hash="shared-profiles",
        strategy_hash="strategy",
    )
    builder.append([_trade(index) for index in range(20)])
    builder.finalize()
    order_root = tmp_path / "orders"
    orders = [_v3_order(index) for index in range(2, 8)]
    write_frozen_order_catalog(
        order_root, orders, source_pin="source", strategy_hash="strategy"
    )
    return trade_root, order_root, orders


def _write_v2_inputs(tmp_path):
    trade_root = tmp_path / "trades-v2-shared"
    builder = ParquetTradeCatalogBuilder(
        trade_root,
        source_pin="source",
        profile_hash="shared-profiles",
        strategy_hash="strategy",
    )
    trades = [_trade(index) for index in range(20)]
    builder.append(trades)
    builder.finalize()
    orders = [
        V2TakerOrder(
            order_id=f"v2-order-{index}",
            market_id=9,
            asset_id="asset",
            side="BUY",
            limit_price=Decimal("0.60"),
            size=Decimal(1),
            signal_block=trades[index].block_number,
            signal_ts=trades[index].block_time,
            latency=timedelta(0),
            latency_blocks=0,
            horizon=timedelta(seconds=10),
            horizon_blocks=10,
            signal_source_trade_id=trades[index].trade_id,
            exclude_signal_source_trade=True,
        )
        for index in range(2, 8)
    ]
    order_root = tmp_path / "orders-v2-shared"
    write_frozen_order_catalog(
        order_root, orders, source_pin="source", strategy_hash="strategy"
    )
    return trade_root, order_root


def test_frozen_order_catalog_round_trips_v2_and_v3(tmp_path) -> None:
    v2 = V2TakerOrder(
        order_id="v2",
        market_id=1,
        asset_id="asset",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=100,
        signal_ts=BASE_TS,
        latency=timedelta(milliseconds=250),
        horizon=timedelta(seconds=30),
        signal_source_log_indexes=(1, 2),
        max_fill_size_per_order=Decimal("0.5"),
    )
    v2_root = tmp_path / "v2"
    write_frozen_order_catalog(
        v2_root, [v2], source_pin="source", strategy_hash="strategy"
    )
    loaded_v2 = next(FrozenOrderCatalog(v2_root).iter_batches())[0]
    assert loaded_v2 == v2

    v3 = _v3_order(2)
    v3_root = tmp_path / "v3"
    write_frozen_order_catalog(
        v3_root, [v3], source_pin="source", strategy_hash="strategy"
    )
    loaded_v3 = next(FrozenOrderCatalog(v3_root).iter_batches())[0]
    assert loaded_v3 == v3


def test_v2_catalog_converts_to_timestamp_native_v3_templates(tmp_path) -> None:
    trade_root, v2_root = _write_v2_inputs(tmp_path)
    del trade_root
    target = tmp_path / "converted-v3"

    receipt = convert_v2_frozen_order_catalog_to_v3(v2_root, target, row_group_size=2)
    converted = FrozenOrderCatalog(target)
    rows = [
        order
        for batch in converted.iter_batches(batch_size=2)
        for order in batch
    ]

    assert converted.execution_family == "V3"
    assert converted.rows == 6
    assert receipt["contract"]["profile_defaults_applied_at_replay"] is True
    assert all(isinstance(order, TradeOnlyOrder) for order in rows)
    assert all(order.liquidity_intent == LiquidityIntent.TAKER for order in rows)
    assert len({order.random_seed for order in rows}) == len(rows)


def test_frozen_order_catalog_multipart_resume_is_lossless(tmp_path) -> None:
    root = tmp_path / "resumable-orders"
    orders = [_v3_order(index) for index in range(2, 9)]
    first = FrozenOrderCatalogBuilder(
        root,
        source_pin="source",
        strategy_hash="strategy",
        row_group_size=2,
        resume=True,
    )
    first.append(orders[:3])

    resumed = FrozenOrderCatalogBuilder(
        root,
        source_pin="source",
        strategy_hash="strategy",
        row_group_size=2,
        resume=True,
    )
    assert resumed.rows == 3
    resumed.append(orders[3:5])
    resumed.append(orders[5:])
    manifest = resumed.finalize()

    loaded = [
        order
        for batch in FrozenOrderCatalog(root).iter_batches(batch_size=2)
        for order in batch
    ]
    assert loaded == orders
    assert manifest["rows"] == len(orders)
    assert len(manifest["files"]) == 3


def test_frozen_order_catalog_seeks_without_decoding_inventory_prefix(tmp_path) -> None:
    root = tmp_path / "seekable-orders"
    orders = [
        replace(
            _v3_order(index),
            signal_ts=BASE_TS + timedelta(days=index // 2, seconds=index),
        )
        for index in range(8)
    ]
    write_frozen_order_catalog(
        root,
        orders,
        source_pin="source",
        strategy_hash="strategy",
        row_group_size=3,
    )
    catalog = FrozenOrderCatalog(root)

    seeked = [
        order
        for batch in catalog.iter_batches(batch_size=2, start_offset=5)
        for order in batch
    ]
    by_day = [
        order
        for batch in catalog.iter_signal_day_batches(
            read_batch_size=2,
            days_per_batch=1,
            start_offset=4,
        )
        for order in batch
    ]

    assert seeked == orders[5:]
    assert by_day == orders[4:]
    with pytest.raises(ValueError, match="start_offset"):
        list(catalog.iter_batches(start_offset=-1))


def test_profile_workers_share_paths_and_are_worker_count_deterministic(tmp_path) -> None:
    trade_root, order_root, _ = _write_inputs(tmp_path)

    def tasks(label: str, count: int) -> list[ProfileWorkerTask]:
        return [
            ProfileWorkerTask(
                execution_family="V3",
                profile="taker_source_confirmed",
                trade_catalog_path=str(trade_root),
                order_catalog_path=str(order_root),
                result_path=str(tmp_path / f"results-{label}-{index}"),
                run_id="same-run",
                batch_size=2,
            )
            for index in range(count)
        ]

    one = run_profile_workers(tasks("one", 1), max_workers=1)[0]
    two = run_profile_workers(tasks("two", 2), max_workers=2)
    four = run_profile_workers(tasks("four", 4), max_workers=4)

    receipts = [one, *two, *four]
    assert {receipt.results_sha256 for receipt in receipts} == {
        one.results_sha256
    }
    assert {receipt.ledger_sha256 for receipt in receipts} == {one.ledger_sha256}
    assert {tuple(receipt.statuses.items()) for receipt in receipts} == {
        tuple(one.statuses.items())
    }
    assert all(receipt.orders == 6 for receipt in receipts)
    assert all(receipt.clickhouse_query_count == 0 for receipt in receipts)


def test_multiple_profiles_share_one_prepared_tape_without_result_drift(
    tmp_path,
) -> None:
    trade_root, order_root, _ = _write_inputs(tmp_path)
    profiles = ("taker_source_confirmed", "central_trade_only_30s")
    independent = [
        run_profile_workers(
            [
                ProfileWorkerTask(
                    execution_family="V3",
                    profile=profile,
                    trade_catalog_path=str(trade_root),
                    order_catalog_path=str(order_root),
                    result_path=str(tmp_path / "independent" / profile),
                    run_id="shared-tape-run",
                    batch_size=2,
                )
            ],
            max_workers=1,
        )[0]
        for profile in profiles
    ]
    grouped = run_profile_workers(
        [
            ProfileWorkerTask(
                execution_family="V3",
                profile=profile,
                trade_catalog_path=str(trade_root),
                order_catalog_path=str(order_root),
                result_path=str(tmp_path / "grouped" / profile),
                run_id="shared-tape-run",
                batch_size=2,
            )
            for profile in profiles
        ],
        max_workers=1,
    )

    for baseline, shared in zip(independent, grouped, strict=True):
        assert shared.results_sha256 == baseline.results_sha256
        assert shared.ledger_sha256 == baseline.ledger_sha256
        assert shared.statuses == baseline.statuses
        timings = json.loads(Path(shared.batch_timings_path).read_text())
        assert {row["shared_tape_profile_count"] for row in timings} == {2}
    assert len({receipt.worker_pid for receipt in grouped}) == 1


def test_v2_profiles_share_exact_views_and_report_matching_backend(tmp_path) -> None:
    trade_root, order_root = _write_v2_inputs(tmp_path)
    profiles = ("conservative_trade_tape", "optimistic_sensitivity")

    def task(root: str, profile: str) -> ProfileWorkerTask:
        return ProfileWorkerTask(
            execution_family="V2",
            profile=profile,
            trade_catalog_path=str(trade_root),
            order_catalog_path=str(order_root),
            result_path=str(tmp_path / root / profile),
            run_id="v2-shared-run",
            matcher_backend="python",
            batch_size=2,
        )

    independent = [
        run_profile_workers([task("independent-v2", profile)], max_workers=1)[0]
        for profile in profiles
    ]
    shared = run_profile_workers(
        [task("shared-v2", profile) for profile in profiles], max_workers=1
    )

    for baseline, grouped in zip(independent, shared, strict=True):
        assert grouped.results_sha256 == baseline.results_sha256
        assert grouped.ledger_sha256 == baseline.ledger_sha256
        assert grouped.statuses == baseline.statuses
        assert grouped.matching_backends == {"python": grouped.batches}
        timings = json.loads(Path(grouped.batch_timings_path).read_text())
        assert {row["shared_tape_profile_count"] for row in timings} == {2}
        assert {row["matching_backend"] for row in timings} == {"python"}


def test_shared_tape_profiles_recycle_and_resume_exactly(tmp_path) -> None:
    trade_root, order_root, _ = _write_inputs(tmp_path)
    profiles = ("taker_source_confirmed", "central_trade_only_30s")

    def tasks(root: str, *, recycle_batches: int | None) -> list[ProfileWorkerTask]:
        return [
            ProfileWorkerTask(
                execution_family="V3",
                profile=profile,
                trade_catalog_path=str(trade_root),
                order_catalog_path=str(order_root),
                result_path=str(tmp_path / root / profile),
                run_id="shared-recycle-run",
                batch_size=2,
                recycle_batches=recycle_batches,
            )
            for profile in profiles
        ]

    continuous = run_profile_workers(tasks("continuous", recycle_batches=None), max_workers=1)
    recycled = run_profile_workers(tasks("recycled", recycle_batches=1), max_workers=1)

    for baseline, resumed in zip(continuous, recycled, strict=True):
        assert resumed.results_sha256 == baseline.results_sha256
        assert resumed.ledger_sha256 == baseline.ledger_sha256
        assert resumed.statuses == baseline.statuses
        recycle = json.loads(
            (Path(resumed.result_path) / "recycle_receipt.json").read_text()
        )
        assert recycle["shared_tape_profile_count"] == 2
        assert len({row["worker_pid"] for row in recycle["segments"]}) > 1


def test_v2_worker_applies_requested_profile(tmp_path) -> None:
    trade_root = tmp_path / "trades-v2"
    builder = ParquetTradeCatalogBuilder(
        trade_root,
        source_pin="source",
        profile_hash="shared",
        strategy_hash="strategy",
    )
    builder.append([_trade(index) for index in range(5)])
    builder.finalize()
    signal = _trade(1)
    base = V2TakerOrder(
        order_id="v2-order",
        market_id=9,
        asset_id="asset",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=signal.block_number,
        signal_ts=signal.block_time,
        latency=timedelta(0),
        latency_blocks=0,
        horizon=timedelta(seconds=10),
        horizon_blocks=10,
        signal_source_trade_id=signal.trade_id,
        exclude_signal_source_trade=True,
    )
    order_root = tmp_path / "orders-v2"
    write_frozen_order_catalog(
        order_root, [replace(base)], source_pin="source", strategy_hash="strategy"
    )
    task = ProfileWorkerTask(
        execution_family="V2",
        profile="optimistic_sensitivity",
        trade_catalog_path=str(trade_root),
        order_catalog_path=str(order_root),
        result_path=str(tmp_path / "result-v2"),
        run_id="v2-run",
    )

    receipt = run_profile_workers([task], max_workers=1)[0]

    assert receipt.orders == 1
    assert receipt.clickhouse_query_count == 0


def test_profile_worker_crash_resume_matches_continuous_run(tmp_path) -> None:
    trade_root, order_root, _ = _write_inputs(tmp_path)
    continuous_task = ProfileWorkerTask(
        execution_family="V3",
        profile="central_trade_only_30s",
        trade_catalog_path=str(trade_root),
        order_catalog_path=str(order_root),
        result_path=str(tmp_path / "continuous"),
        run_id="resume-run",
        batch_size=2,
    )
    partial_task = replace(
        continuous_task,
        result_path=str(tmp_path / "resumed"),
        max_batches=1,
    )

    continuous = run_profile_workers([continuous_task], max_workers=1)[0]
    partial = run_profile_workers([partial_task], max_workers=1)[0]
    resumed = run_profile_workers(
        [replace(partial_task, max_batches=None, resume=True)], max_workers=1
    )[0]

    assert partial.complete is False
    assert partial.orders == 2
    assert partial.ledger_sha256 == ""
    assert partial.ledger_entries is None
    assert resumed.complete is True
    assert resumed.orders == continuous.orders == 6
    assert resumed.results_sha256 == continuous.results_sha256
    assert resumed.ledger_sha256 == continuous.ledger_sha256
    assert resumed.statuses == continuous.statuses
    checkpoint = __import__("json").loads(
        (tmp_path / "resumed" / "runner_checkpoint.json").read_text()
    )
    assert checkpoint["complete"] is True
    assert len(checkpoint["ledger_deltas"]) == resumed.batches
    assert "v3_ledger" not in checkpoint
    timings = json.loads(
        (tmp_path / "resumed" / "batch_timings.json").read_text()
    )
    assert len(timings) == resumed.batches
    assert [row["batch"] for row in timings] == list(range(1, resumed.batches + 1))
    assert resumed.batch_timings_sha256


def test_v3_monte_carlo_crash_recycle_and_resume_are_bit_exact(tmp_path) -> None:
    trade_root, order_root, _ = _write_inputs(tmp_path)
    base = ProfileWorkerTask(
        execution_family="V3",
        profile="generative_tape_mc",
        trade_catalog_path=str(trade_root),
        order_catalog_path=str(order_root),
        result_path=str(tmp_path / "mc-continuous"),
        run_id="mc-resume-run",
        batch_size=2,
        matcher_backend="rust",
    )

    continuous = run_profile_workers([base], max_workers=1)[0]
    recycled = run_profile_workers(
        [
            replace(
                base,
                result_path=str(tmp_path / "mc-recycled"),
                recycle_batches=1,
            )
        ],
        max_workers=1,
    )[0]

    partial_task = replace(
        base,
        result_path=str(tmp_path / "mc-resumed"),
        max_batches=1,
    )
    partial = run_profile_workers([partial_task], max_workers=1)[0]
    resumed = run_profile_workers(
        [replace(partial_task, max_batches=None, resume=True)], max_workers=1
    )[0]

    assert partial.complete is False
    for candidate in (recycled, resumed):
        assert candidate.complete is True
        assert candidate.orders == continuous.orders
        assert candidate.results_sha256 == continuous.results_sha256
        assert candidate.ledger_sha256 == continuous.ledger_sha256
        assert candidate.statuses == continuous.statuses
        assert candidate.matching_backends == continuous.matching_backends


def test_profile_worker_can_replay_an_exact_inventory_slice(tmp_path) -> None:
    trade_root, order_root, orders = _write_inputs(tmp_path)
    result_root = tmp_path / "sliced"
    receipt = run_profile_workers(
        [
            ProfileWorkerTask(
                execution_family="V3",
                profile="taker_source_confirmed",
                trade_catalog_path=str(trade_root),
                order_catalog_path=str(order_root),
                result_path=str(result_root),
                run_id="slice-run",
                batch_size=2,
                skip_orders=2,
                max_orders=3,
            )
        ],
        max_workers=1,
    )[0]

    manifest = json.loads((result_root / "manifest.json").read_text())
    order_ids = [
        value
        for part in manifest["parts"]
        for value in pq.read_table(result_root / part["path"], columns=["order_id"])
        .column("order_id")
        .to_pylist()
    ]
    assert order_ids == [order.order_id for order in orders[2:5]]
    assert receipt.orders == 3
    timings = json.loads((result_root / "batch_timings.json").read_text())
    assert timings[0]["catalog_order_start"] == 2
    assert timings[-1]["catalog_order_end"] == 5


def test_recycled_worker_matches_one_process_replay(tmp_path) -> None:
    trade_root, order_root, _ = _write_inputs(tmp_path)
    base = ProfileWorkerTask(
        execution_family="V3",
        profile="central_trade_only_30s",
        trade_catalog_path=str(trade_root),
        order_catalog_path=str(order_root),
        result_path=str(tmp_path / "one-process"),
        run_id="recycle-run",
        batch_size=2,
    )
    one_process = run_profile_workers([base], max_workers=1)[0]
    recycled = run_profile_workers(
        [
            replace(
                base,
                result_path=str(tmp_path / "recycled"),
                recycle_batches=1,
            )
        ],
        max_workers=1,
    )[0]

    assert recycled.results_sha256 == one_process.results_sha256
    assert recycled.ledger_sha256 == one_process.ledger_sha256
    assert recycled.statuses == one_process.statuses
    assert recycled.orders == one_process.orders
    recycle_receipt = json.loads(
        (tmp_path / "recycled" / "recycle_receipt.json").read_text()
    )
    assert len(recycle_receipt["segments"]) == 4
    assert len({row["worker_pid"] for row in recycle_receipt["segments"]}) == 4
