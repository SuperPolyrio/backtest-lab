from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from quant.backtest.orderfilled_v2_replay import (
    V2TakerOrder,
    V2TradePrint,
    replay_v2_taker_orders_with_diagnostics,
)
from quant.backtest.replay_session import FrozenOrderStrategy, ReplaySession
from quant.backtest.trade_catalog import (
    ParquetTradeCatalog,
    ParquetTradeCatalogBuilder,
)

BASE_TS = datetime(2026, 7, 1, tzinfo=timezone.utc)


def _trade(
    trade_id: str,
    *,
    market_id: int,
    asset_id: str,
    seconds: int,
    block: int,
    tx_hash: str | None = None,
    trade_group_id: str | None = None,
) -> V2TradePrint:
    return V2TradePrint(
        trade_id=trade_id,
        market_id=market_id,
        condition_id=f"condition-{market_id}",
        asset_id=asset_id,
        outcome="YES",
        block_number=block,
        block_time=BASE_TS + timedelta(seconds=seconds),
        tx_hash=tx_hash or f"0x{trade_id}",
        tx_index=block,
        tx_index_source="receipt",
        price=Decimal("0.55"),
        size=Decimal("12.3456789000"),
        notional=Decimal("6.7901233950"),
        aggressor_side="BUY",
        passive_side="SELL",
        source_log_indexes=(block,),
        source_fill_count=1,
        trade_group_id=trade_group_id,
    )


def _catalog(tmp_path, trades) -> ParquetTradeCatalog:
    root = tmp_path / "catalog"
    builder = ParquetTradeCatalogBuilder(
        root,
        source_pin="source-sha",
        profile_hash="profiles-sha",
        strategy_hash="strategy-sha",
        row_group_size=2,
    )
    midpoint = len(trades) // 2
    builder.append(trades[:midpoint])
    builder.append(trades[midpoint:])
    manifest = builder.finalize()
    assert manifest.rows == len(trades)
    return ParquetTradeCatalog(
        root,
        source_pin="source-sha",
        profile_hash="profiles-sha",
        strategy_hash="strategy-sha",
    )


def test_parquet_catalog_round_trip_and_window_pruning(tmp_path) -> None:
    trades = [
        _trade("a", market_id=1, asset_id="asset-a", seconds=0, block=100),
        _trade("b", market_id=1, asset_id="asset-a", seconds=1, block=101),
        _trade("c", market_id=1, asset_id="asset-a", seconds=2, block=102),
        _trade("other", market_id=2, asset_id="asset-b", seconds=1, block=101),
    ]
    catalog = _catalog(tmp_path, trades)

    loaded = catalog.load_window(
        market_id=1, asset_id="asset-a", start_block=101, end_block=102
    )

    assert [row.trade_id for row in loaded] == ["b", "c"]
    assert loaded[0].size == Decimal("12.3456789000")
    assert loaded[0].trade_group_id is None
    assert catalog.clickhouse_query_count == 0


def test_catalog_defaults_to_market_asset_date_physical_partitions(tmp_path) -> None:
    catalog = _catalog(
        tmp_path,
        [
            _trade("a", market_id=1, asset_id="asset-a", seconds=0, block=100),
            _trade("b", market_id=2, asset_id="asset-b", seconds=1, block=101),
        ],
    )

    assert catalog.manifest.partition_mode == "market_asset_date"
    assert {(row.market_id, row.asset_id) for row in catalog.manifest.files} == {
        (1, "asset-a"),
        (2, "asset-b"),
    }
    assert all("market_id=" in row.path for row in catalog.manifest.files)
    assert all("asset_id=" in row.path for row in catalog.manifest.files)


def test_catalog_reads_legacy_date_partition_mode(tmp_path) -> None:
    root = tmp_path / "date-catalog"
    builder = ParquetTradeCatalogBuilder(
        root,
        source_pin="source-sha",
        profile_hash="profiles-sha",
        strategy_hash="strategy-sha",
        partition_mode="date",
    )
    builder.append(
        [_trade("a", market_id=1, asset_id="asset-a", seconds=0, block=100)]
    )
    builder.finalize()

    catalog = ParquetTradeCatalog(root)

    assert catalog.manifest.partition_mode == "date"
    assert catalog.manifest.files[0].market_id == 0
    assert catalog.manifest.files[0].asset_id == "*"
    assert [row.trade_id for row in catalog.load_window(
        market_id=1,
        asset_id="asset-a",
        start_block=100,
        end_block=100,
    )] == ["a"]


def test_catalog_prepare_for_orders_loads_only_required_market_window(tmp_path) -> None:
    trades = [
        _trade(
            f"a-{index}",
            market_id=1,
            asset_id="asset-a",
            seconds=index,
            block=100 + index,
        )
        for index in range(10)
    ] + [
        _trade(
            f"b-{index}",
            market_id=2,
            asset_id="asset-b",
            seconds=index,
            block=100 + index,
        )
        for index in range(10)
    ]
    catalog = _catalog(tmp_path, trades)
    order = V2TakerOrder(
        order_id="order",
        market_id=1,
        asset_id="asset-a",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=103,
        signal_ts=BASE_TS + timedelta(seconds=3),
        latency_blocks=0,
        latency=timedelta(0),
        horizon_blocks=2,
        horizon=timedelta(seconds=2),
    )

    prepared = catalog.prepare_for_orders([order])

    assert prepared.trade_rows_indexed == 3
    assert {row.trade_id for row in prepared.ordered_trades} == {"a-3", "a-4", "a-5"}
    assert catalog.parquet_file_read_count == 1
    assert catalog.parquet_file_read_count < len(catalog.manifest.files)


def test_columnar_prepared_tape_matches_object_and_rust_replay(tmp_path) -> None:
    trades = [
        _trade(
            f"a-{index}",
            market_id=1,
            asset_id="asset-a",
            seconds=index,
            block=100 + index,
        )
        for index in range(10)
    ]
    catalog = _catalog(tmp_path, trades)
    order = V2TakerOrder(
        order_id="columnar-order",
        market_id=1,
        asset_id="asset-a",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=103,
        signal_ts=BASE_TS + timedelta(seconds=3),
        latency_blocks=1,
        latency=timedelta(seconds=1),
        horizon_blocks=2,
        horizon=timedelta(seconds=2),
        participation_rate=Decimal("0.025"),
    )

    objects = catalog.prepare_for_orders([order])
    columnar = catalog.prepare_columnar_for_orders([order])

    object_rows = [
        (
            row.trade_id,
            row.sequence,
            row.price,
            row.size,
            row.source_log_indexes,
        )
        for row in objects.ordered_trades
    ]
    columnar_rows = [
        (
            row.trade_id,
            row.sequence,
            row.price,
            row.size,
            row.source_log_indexes,
        )
        for row in columnar.ordered_trades
    ]
    object_results, object_ledger, _ = replay_v2_taker_orders_with_diagnostics(
        [order], prepared_tape=objects, backend="rust"
    )
    columnar_results, columnar_ledger, diagnostics = (
        replay_v2_taker_orders_with_diagnostics(
            [order], prepared_tape=columnar, backend="rust"
        )
    )

    assert columnar_rows == object_rows
    assert [row.as_dict() for row in columnar_results] == [
        row.as_dict() for row in object_results
    ]
    assert columnar_ledger.as_dict() == object_ledger.as_dict()
    assert diagnostics.matching_backend == "rust"


def test_columnar_sort_matches_normalized_tx_hash_and_empty_log_contract(
    tmp_path,
) -> None:
    upper = replace(
        _trade(
            "upper",
            market_id=1,
            asset_id="asset-a",
            seconds=1,
            block=101,
            tx_hash="0xB",
        ),
        source_log_indexes=(),
    )
    lower = _trade(
        "lower",
        market_id=1,
        asset_id="asset-a",
        seconds=1,
        block=101,
        tx_hash="0xa",
    )
    catalog = _catalog(tmp_path, [upper, lower])
    order = V2TakerOrder(
        order_id="ordering",
        market_id=1,
        asset_id="asset-a",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=100,
        signal_ts=BASE_TS,
        horizon_blocks=2,
        horizon=timedelta(seconds=2),
    )

    objects = catalog.prepare_for_orders([order])
    columnar = catalog.prepare_columnar_for_orders([order])

    assert [row.sequence for row in columnar.ordered_trades] == [
        row.sequence for row in objects.ordered_trades
    ]


def test_prepared_view_matches_direct_catalog_read_without_reopening_parquet(
    tmp_path,
) -> None:
    trades = [
        _trade(
            f"a-{index}",
            market_id=1,
            asset_id="asset-a",
            seconds=index,
            block=100 + index,
        )
        for index in range(12)
    ]
    catalog = _catalog(tmp_path, trades)
    broad = V2TakerOrder(
        order_id="broad",
        market_id=1,
        asset_id="asset-a",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=101,
        signal_ts=BASE_TS + timedelta(seconds=1),
        latency_blocks=0,
        latency=timedelta(0),
        horizon_blocks=9,
        horizon=timedelta(seconds=9),
    )
    narrow = V2TakerOrder(
        order_id="narrow",
        market_id=1,
        asset_id="asset-a",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=104,
        signal_ts=BASE_TS + timedelta(seconds=4),
        latency_blocks=0,
        latency=timedelta(0),
        horizon_blocks=2,
        horizon=timedelta(seconds=2),
    )

    union = catalog.prepare_for_orders([broad, narrow])
    reads_after_union = catalog.parquet_file_read_count
    view = catalog.prepared_view_for_orders(union, [narrow])
    cached = catalog.prepared_view_for_orders(union, [narrow])
    direct = catalog.prepare_for_orders([narrow])

    assert view is cached
    assert [row.trade_id for row in view.ordered_trades] == [
        row.trade_id for row in direct.ordered_trades
    ]
    assert view.trade_rows_indexed < union.trade_rows_indexed
    assert catalog.parquet_file_read_count > reads_after_union
    direct_read_count = catalog.parquet_file_read_count
    catalog.prepared_view_for_orders(union, [narrow])
    assert catalog.parquet_file_read_count == direct_read_count


def test_columnar_profile_view_retains_rust_arrays_without_parquet_reread(
    tmp_path,
) -> None:
    trades = [
        _trade(
            f"a-{index}",
            market_id=1,
            asset_id="asset-a",
            seconds=index,
            block=100 + index,
        )
        for index in range(12)
    ]
    catalog = _catalog(tmp_path, trades)
    broad = V2TakerOrder(
        order_id="broad-columnar",
        market_id=1,
        asset_id="asset-a",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=101,
        signal_ts=BASE_TS + timedelta(seconds=1),
        horizon_blocks=9,
        horizon=timedelta(seconds=9),
    )
    narrow = replace(
        broad,
        order_id="narrow-columnar",
        signal_block=104,
        signal_ts=BASE_TS + timedelta(seconds=4),
        horizon_blocks=2,
        horizon=timedelta(seconds=2),
    )
    union = catalog.prepare_columnar_for_orders([broad, narrow])
    reads = catalog.parquet_file_read_count

    view = catalog.prepared_view_for_orders(union, [narrow])

    assert [row.trade_id for row in view.ordered_trades] == ["a-4", "a-5", "a-6"]
    assert "fill_only_rust_arrays_v2" in view.backend_payloads
    assert catalog.parquet_file_read_count == reads


def test_catalog_streaming_merge_and_atomic_boundaries(tmp_path) -> None:
    trades = [
        _trade(
            "a",
            market_id=1,
            asset_id="asset-a",
            seconds=0,
            block=100,
            tx_hash="0xshared-ts-a",
        ),
        _trade(
            "b",
            market_id=2,
            asset_id="asset-b",
            seconds=0,
            block=100,
            tx_hash="0xshared-ts-b",
        ),
        _trade(
            "c",
            market_id=1,
            asset_id="asset-a",
            seconds=1,
            block=101,
            trade_group_id="group",
        ),
        _trade(
            "d",
            market_id=1,
            asset_id="asset-a",
            seconds=2,
            block=102,
            trade_group_id="group",
        ),
    ]
    catalog = _catalog(tmp_path, trades)

    chunks = list(catalog.iter_aligned_chunks(target_rows=1, batch_size=1))

    assert [[row.trade_id for row in chunk] for chunk in chunks] == [
        ["a", "b"],
        ["c", "d"],
    ]


def test_catalog_rejects_binding_mismatch_and_file_corruption(tmp_path) -> None:
    catalog = _catalog(
        tmp_path,
        [_trade("a", market_id=1, asset_id="asset", seconds=0, block=100)],
    )
    with pytest.raises(ValueError, match="profile_hash mismatch"):
        ParquetTradeCatalog(catalog.root, profile_hash="wrong")

    file_path = catalog.root / catalog.manifest.files[0].path
    with file_path.open("ab") as handle:
        handle.write(b"corrupt")
    with pytest.raises(ValueError, match="file checksum mismatch"):
        ParquetTradeCatalog(catalog.root)


def test_builder_does_not_replace_existing_catalog(tmp_path) -> None:
    _catalog(
        tmp_path,
        [_trade("a", market_id=1, asset_id="asset", seconds=0, block=100)],
    )

    with pytest.raises(FileExistsError):
        ParquetTradeCatalogBuilder(
            tmp_path / "catalog",
            source_pin="source-sha",
            profile_hash="profiles-sha",
            strategy_hash="strategy-sha",
        )


def test_trade_catalog_builder_resume_keeps_high_watermark(tmp_path) -> None:
    root = tmp_path / "resumable-catalog"
    first = ParquetTradeCatalogBuilder(
        root,
        source_pin="source-sha",
        profile_hash="profiles-sha",
        strategy_hash="strategy-sha",
        row_group_size=2,
        resume=True,
    )
    first.append(
        [_trade("a", market_id=1, asset_id="asset", seconds=0, block=100)],
        high_watermark={"day": "2026-07-01"},
    )

    resumed = ParquetTradeCatalogBuilder(
        root,
        source_pin="source-sha",
        profile_hash="profiles-sha",
        strategy_hash="strategy-sha",
        row_group_size=2,
        resume=True,
    )
    assert resumed.rows == 1
    assert resumed.high_watermark == {"day": "2026-07-01"}
    resumed.append(
        [_trade("b", market_id=1, asset_id="asset", seconds=1, block=101)],
        high_watermark={"day": "2026-07-02"},
    )
    manifest = resumed.finalize()

    catalog = ParquetTradeCatalog(root)
    assert manifest.rows == 2
    assert [trade.trade_id for trade in catalog.iter_trades()] == ["a", "b"]


def test_prepare_for_orders_reads_each_candidate_file_once(tmp_path) -> None:
    trades = [
        _trade(
            f"a-{index}",
            market_id=1,
            asset_id="asset-a",
            seconds=index,
            block=100 + index,
        )
        for index in range(10)
    ]
    catalog = _catalog(tmp_path, trades)
    orders = [
        V2TakerOrder(
            order_id=f"order-{block}",
            market_id=1,
            asset_id="asset-a",
            side="BUY",
            limit_price=Decimal("0.60"),
            size=Decimal(1),
            signal_block=block,
            signal_ts=BASE_TS + timedelta(seconds=block - 100),
            latency_blocks=0,
            latency=timedelta(0),
            horizon_blocks=1,
            horizon=timedelta(seconds=1),
        )
        for block in (101, 107)
    ]

    prepared = catalog.prepare_for_orders(orders)

    assert {trade.trade_id for trade in prepared.ordered_trades} == {
        "a-1",
        "a-2",
        "a-7",
        "a-8",
    }
    assert catalog.parquet_file_read_count == len(catalog.manifest.files)


def test_parquet_catalog_drives_dynamic_session_without_clickhouse(tmp_path) -> None:
    signal = _trade("signal", market_id=1, asset_id="asset", seconds=0, block=100)
    future = _trade("future", market_id=1, asset_id="asset", seconds=2, block=102)
    catalog = _catalog(tmp_path, [signal, future])
    order = V2TakerOrder(
        order_id="order",
        market_id=1,
        asset_id="asset",
        side="BUY",
        limit_price=Decimal("0.60"),
        size=Decimal(1),
        signal_block=100,
        signal_ts=BASE_TS,
        latency=timedelta(0),
        latency_blocks=0,
        horizon=timedelta(seconds=10),
        horizon_blocks=10,
        participation_rate=Decimal("0.025"),
        signal_source_trade_id="signal",
        exclude_signal_source_trade=True,
    )
    session = ReplaySession(run_id="parquet", execution_family="V2", catalog=catalog)

    results, receipt = session.replay_dynamic(FrozenOrderStrategy([order]), chunk_size=1)

    assert results[0].filled_size == Decimal("0.3086419725")
    assert receipt.catalog_rows == 2
    assert catalog.clickhouse_query_count == 0
