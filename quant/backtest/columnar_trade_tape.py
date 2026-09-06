"""Arrow-backed trade references for the Rust Fill-only replay path."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from time import perf_counter
from typing import Any, Literal, cast, overload

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from quant.backtest.orderfilled_v2_replay import (
    EventKey,
    RequiredTradeWindow,
    V2TradePrint,
)
from quant.backtest.prepared_trade_tape import PreparedTradeTape, TradeGroupIndex

Q = Decimal("0.0000000001")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_pc: Any = pc


class ColumnarTradeStore:
    """Own Arrow buffers and expose V2 trade fields only when they are used."""

    def __init__(self, table: pa.Table) -> None:
        self.table = table.combine_chunks()
        self.trade_ids = tuple(
            str(value) for value in self.table.column("trade_id").to_pylist()
        )
        encoded_assets = _pc.dictionary_encode(
            self.table.column("asset_id").combine_chunks()
        )
        if not pa.types.is_dictionary(encoded_assets.type):
            raise TypeError("asset_id dictionary encoding failed")
        dictionary = cast(pa.DictionaryArray, encoded_assets)
        self.asset_codes = np.ascontiguousarray(
            dictionary.indices.to_numpy(zero_copy_only=False), dtype=np.int64
        )
        self.asset_values = tuple(
            str(value).lower() for value in dictionary.dictionary.to_pylist()
        )
        self.asset_code_by_id = {
            asset_id: index for index, asset_id in enumerate(self.asset_values)
        }
        self.market_ids = _integer_i64(self.table.column("market_id"))
        self.block_numbers = _integer_i64(self.table.column("block_number"))
        self.block_times_us = _timestamp_us(self.table.column("block_time"))
        self.prices = _decimal_raw_i64(self.table.column("price"))
        self.sizes = _decimal_raw_i64(self.table.column("size_shares"))
        self.notionals = _decimal_raw_i64(self.table.column("notional_usdc"))
        encoded_sides = _pc.dictionary_encode(
            _pc.utf8_upper(self.table.column("aggressor_side").combine_chunks())
        )
        if not pa.types.is_dictionary(encoded_sides.type):
            raise TypeError("aggressor_side dictionary encoding failed")
        side_dictionary = cast(pa.DictionaryArray, encoded_sides)
        side_values = tuple(str(value) for value in side_dictionary.dictionary.to_pylist())
        if any(side not in {"BUY", "SELL"} for side in side_values):
            raise ValueError("columnar tape contains an invalid aggressor side")
        side_lookup = np.asarray(
            [0 if side == "BUY" else 1 for side in side_values], dtype=np.uint8
        )
        side_indices = np.asarray(
            side_dictionary.indices.to_numpy(zero_copy_only=False), dtype=np.int64
        )
        self.side_codes = np.ascontiguousarray(side_lookup[side_indices])
        self.first_log_indexes = _first_log_indexes(
            self.table.column("source_log_indexes")
        )
        self._columns = {
            name: self.table.column(name).combine_chunks()
            for name in self.table.column_names
        }

    def __len__(self) -> int:
        return self.table.num_rows

    def ref(self, index: int) -> ColumnarTradeRef:
        return ColumnarTradeRef(self, int(index))

    def text(self, name: str, index: int, *, lower: bool = False) -> str:
        value = self._columns[name][index].as_py()
        text = str(value or "")
        return text.lower() if lower else text

    def optional_text(self, name: str, index: int) -> str | None:
        value = self._columns[name][index].as_py()
        return str(value) if value else None

    def integer(self, name: str, index: int) -> int:
        return int(self._columns[name][index].as_py() or 0)

    def integer_tuple(self, name: str, index: int) -> tuple[int, ...]:
        values = self._columns[name][index].as_py() or ()
        return tuple(int(value) for value in values)


class ColumnarTradeRef:
    """Small immutable reference into one :class:`ColumnarTradeStore`."""

    __slots__ = ("_index", "_store")

    def __init__(self, store: ColumnarTradeStore, index: int) -> None:
        self._store = store
        self._index = int(index)

    @property
    def trade_id(self) -> str:
        return self._store.trade_ids[self._index]

    @property
    def trade_group_id(self) -> str | None:
        return self._store.optional_text("trade_group_id", self._index)

    @property
    def market_id(self) -> int:
        return int(self._store.market_ids[self._index])

    @property
    def condition_id(self) -> str:
        return self._store.text("condition_id", self._index)

    @property
    def asset_id(self) -> str:
        return self._store.asset_values[int(self._store.asset_codes[self._index])]

    @property
    def outcome(self) -> str:
        return self._store.text("outcome", self._index)

    @property
    def block_number(self) -> int:
        return int(self._store.block_numbers[self._index])

    @property
    def block_time(self) -> datetime:
        return _EPOCH + timedelta(microseconds=int(self._store.block_times_us[self._index]))

    @property
    def tx_hash(self) -> str:
        return self._store.text("tx_hash", self._index, lower=True)

    @property
    def tx_index(self) -> int:
        return self._store.integer("tx_index", self._index)

    @property
    def tx_index_source(self) -> str:
        return self._store.text("tx_index_source", self._index)

    @property
    def price(self) -> Decimal:
        return _from_scaled(int(self._store.prices[self._index]))

    @property
    def size(self) -> Decimal:
        return _from_scaled(int(self._store.sizes[self._index]))

    @property
    def notional(self) -> Decimal:
        return _from_scaled(int(self._store.notionals[self._index]))

    @property
    def aggressor_side(self) -> Literal["BUY", "SELL"]:
        return "BUY" if int(self._store.side_codes[self._index]) == 0 else "SELL"

    @property
    def passive_side(self) -> Literal["BUY", "SELL"]:
        value = self._store.text("passive_side", self._index).upper()
        if value not in {"BUY", "SELL"}:
            raise ValueError(f"invalid passive side: {value!r}")
        return cast(Literal["BUY", "SELL"], value)

    @property
    def source_log_indexes(self) -> tuple[int, ...]:
        return self._store.integer_tuple("source_log_indexes", self._index)

    @property
    def source_fill_count(self) -> int:
        return self._store.integer("source_fill_count", self._index)

    @property
    def sequence(self) -> tuple[int, int, str, int, str]:
        return (
            self.block_number,
            self.tx_index,
            self.tx_hash,
            int(self._store.first_log_indexes[self._index]),
            self.trade_id,
        )

    @property
    def event_key(self) -> EventKey:
        return EventKey(*self.sequence)


class ColumnarTradeSequence(Sequence[ColumnarTradeRef]):
    __slots__ = ("_indices", "_store")

    def __init__(self, store: ColumnarTradeStore, indices: np.ndarray[Any, Any]) -> None:
        self._store = store
        self._indices = indices

    def __len__(self) -> int:
        return int(self._indices.size)

    @overload
    def __getitem__(self, index: int) -> ColumnarTradeRef: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[ColumnarTradeRef, ...]: ...

    def __getitem__(
        self, index: int | slice
    ) -> ColumnarTradeRef | tuple[ColumnarTradeRef, ...]:
        if isinstance(index, slice):
            return tuple(self._store.ref(int(value)) for value in self._indices[index])
        return self._store.ref(int(self._indices[index]))


class IndexedIntegerSequence(Sequence[int]):
    __slots__ = ("_indices", "_values")

    def __init__(
        self, values: np.ndarray[Any, Any], indices: np.ndarray[Any, Any]
    ) -> None:
        self._values = values
        self._indices = indices

    def __len__(self) -> int:
        return int(self._indices.size)

    @overload
    def __getitem__(self, index: int) -> int: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[int, ...]: ...

    def __getitem__(self, index: int | slice) -> int | tuple[int, ...]:
        if isinstance(index, slice):
            return tuple(int(value) for value in self._values[self._indices[index]])
        return int(self._values[int(self._indices[index])])


class IndexedDatetimeSequence(Sequence[datetime]):
    __slots__ = ("_indices", "_values")

    def __init__(
        self, values: np.ndarray[Any, Any], indices: np.ndarray[Any, Any]
    ) -> None:
        self._values = values
        self._indices = indices

    def __len__(self) -> int:
        return int(self._indices.size)

    @overload
    def __getitem__(self, index: int) -> datetime: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[datetime, ...]: ...

    def __getitem__(self, index: int | slice) -> datetime | tuple[datetime, ...]:
        if isinstance(index, slice):
            return tuple(
                _EPOCH + timedelta(microseconds=int(value))
                for value in self._values[self._indices[index]]
            )
        return _EPOCH + timedelta(
            microseconds=int(self._values[int(self._indices[index])])
        )


def prepare_columnar_trade_tape(
    table: pa.Table,
    windows_by_pair: Mapping[tuple[int, str], Sequence[RequiredTradeWindow]],
    *,
    ordered_unique: bool = False,
) -> PreparedTradeTape[V2TradePrint]:
    """Filter, order and index a table without materializing every trade object."""

    started = perf_counter()
    filtered = _filter_to_required_windows(table, windows_by_pair)
    ordered = filtered if ordered_unique else _sort_and_deduplicate(filtered)
    store = ColumnarTradeStore(ordered)
    row_indices = np.arange(len(store), dtype=np.int64)
    combined, sided = _build_all_groups(store, row_indices)
    prepared = PreparedTradeTape(
        index=cast(dict[tuple[int, str, str], TradeGroupIndex[V2TradePrint]], sided),
        combined_groups=cast(
            dict[tuple[int, str], TradeGroupIndex[V2TradePrint]], combined
        ),
        trade_rows_indexed=len(store),
        trade_groups=len(sided),
        index_build_sec=Decimal(str(perf_counter() - started)).quantize(Q),
        ordered_trades=cast(Sequence[V2TradePrint], ColumnarTradeSequence(store, row_indices)),
    )
    from quant.backtest.rust_kernel import install_columnar_prepared_arrays

    install_columnar_prepared_arrays(
        prepared,
        trade_market=store.market_ids,
        trade_asset=store.asset_codes,
        trade_side=store.side_codes,
        trade_block=store.block_numbers,
        trade_time_us=store.block_times_us,
        trade_price=store.prices,
        trade_size=store.sizes,
        asset_codes=store.asset_code_by_id,
        trade_ids=store.trade_ids,
    )
    prepared.backend_payloads["fill_only_columnar_store_v1"] = store
    return prepared


def _build_all_groups(
    store: ColumnarTradeStore,
    row_indices: np.ndarray[Any, Any],
) -> tuple[
    dict[tuple[int, str], TradeGroupIndex[ColumnarTradeRef]],
    dict[tuple[int, str, str], TradeGroupIndex[ColumnarTradeRef]],
]:
    order = np.lexsort((row_indices, store.asset_codes, store.market_ids))
    if not order.size:
        return {}, {}
    changes = np.zeros(order.size, dtype=np.bool_)
    changes[0] = True
    for values in (store.market_ids, store.asset_codes):
        grouped_values = values[order]
        changes[1:] |= grouped_values[1:] != grouped_values[:-1]
    starts = np.flatnonzero(changes)
    ends = np.append(starts[1:], order.size)
    combined: dict[tuple[int, str], TradeGroupIndex[ColumnarTradeRef]] = {}
    sided: dict[tuple[int, str, str], TradeGroupIndex[ColumnarTradeRef]] = {}
    for start, end in zip(starts.tolist(), ends.tolist(), strict=True):
        indices = order[start:end]
        first = int(indices[0])
        market_id = int(store.market_ids[first])
        asset_id = store.asset_values[int(store.asset_codes[first])]
        combined_key = (market_id, asset_id)
        combined[combined_key] = TradeGroupIndex(
            key=(market_id, asset_id, "*"),
            trades=ColumnarTradeSequence(store, indices),
            block_numbers=IndexedIntegerSequence(store.block_numbers, indices),
            block_times=IndexedDatetimeSequence(store.block_times_us, indices),
        )
        group_sides = store.side_codes[indices]
        for side_code, side in ((0, "BUY"), (1, "SELL")):
            side_indices = indices[group_sides == side_code]
            if not side_indices.size:
                continue
            key = (market_id, asset_id, side)
            sided[key] = TradeGroupIndex(
                key=key,
                trades=ColumnarTradeSequence(store, side_indices),
                block_numbers=IndexedIntegerSequence(
                    store.block_numbers, side_indices
                ),
                block_times=IndexedDatetimeSequence(
                    store.block_times_us, side_indices
                ),
            )
    return combined, sided


def _filter_to_required_windows(
    table: pa.Table,
    windows_by_pair: Mapping[tuple[int, str], Sequence[RequiredTradeWindow]],
) -> pa.Table:
    if not table.num_rows:
        return table
    markets = _integer_i64(table.column("market_id"))
    lowered_assets = _pc.utf8_lower(table.column("asset_id").combine_chunks())
    encoded_assets = _pc.dictionary_encode(lowered_assets)
    if not pa.types.is_dictionary(encoded_assets.type):
        raise TypeError("asset_id dictionary encoding failed")
    asset_dictionary = cast(pa.DictionaryArray, encoded_assets)
    asset_codes = np.ascontiguousarray(
        asset_dictionary.indices.to_numpy(zero_copy_only=False), dtype=np.int64
    )
    asset_values = tuple(str(value) for value in asset_dictionary.dictionary.to_pylist())
    asset_code_by_id = {
        asset_id: index for index, asset_id in enumerate(asset_values)
    }
    blocks = _integer_i64(table.column("block_number"))
    times = _timestamp_us(table.column("block_time"))
    normalized = {
        (int(market_id), asset_code_by_id[str(asset_id).lower()]): tuple(windows)
        for (market_id, asset_id), windows in windows_by_pair.items()
        if str(asset_id).lower() in asset_code_by_id
    }
    if not normalized:
        return table.slice(0, 0)

    keep = np.zeros(table.num_rows, dtype=np.bool_)
    order = np.lexsort((asset_codes, markets))
    changes = np.zeros(order.size, dtype=np.bool_)
    changes[0] = True
    ordered_markets = markets[order]
    ordered_assets = asset_codes[order]
    changes[1:] = (ordered_markets[1:] != ordered_markets[:-1]) | (
        ordered_assets[1:] != ordered_assets[:-1]
    )
    starts = np.flatnonzero(changes)
    ends = np.append(starts[1:], order.size)
    for start, end in zip(starts.tolist(), ends.tolist(), strict=True):
        indices = order[start:end]
        windows = normalized.get(
            (int(markets[int(indices[0])]), int(asset_codes[int(indices[0])])), ()
        )
        if not windows:
            continue
        group_keep = np.zeros(indices.size, dtype=np.bool_)
        for window in windows:
            if window.start_block is not None:
                matches = blocks[indices] >= int(window.start_block)
                if window.end_block is not None:
                    matches &= blocks[indices] <= int(window.end_block)
            elif window.start_ts is not None:
                matches = times[indices] >= _datetime_us(window.start_ts)
                if window.end_ts is not None:
                    matches &= times[indices] <= _datetime_us(window.end_ts)
            else:
                matches = np.ones(indices.size, dtype=np.bool_)
            group_keep |= matches
        keep[indices[group_keep]] = True
    if bool(keep.all()):
        return table
    return table.filter(pa.array(keep))


def _sort_and_deduplicate(table: pa.Table) -> pa.Table:
    if table.num_rows <= 1:
        return table
    first_logs = pa.array(
        _first_log_indexes(table.column("source_log_indexes")), type=pa.int64()
    )
    lowered_tx_hashes = _pc.utf8_lower(table.column("tx_hash").combine_chunks())
    sortable = table.append_column("__first_log_index", first_logs).append_column(
        "__tx_hash_lower", lowered_tx_hashes
    )
    indices = _pc.sort_indices(
        sortable,
        sort_keys=[
            ("block_number", "ascending"),
            ("tx_index", "ascending"),
            ("__tx_hash_lower", "ascending"),
            ("__first_log_index", "ascending"),
            ("trade_id", "ascending"),
        ],
    )
    ordered = sortable.take(indices).drop_columns(
        ["__first_log_index", "__tx_hash_lower"]
    )
    trade_ids = ordered.column("trade_id").combine_chunks()
    distinct = int(_pc.count_distinct(trade_ids).as_py() or 0)
    if distinct == ordered.num_rows:
        return ordered
    encoded_trade_ids = _pc.dictionary_encode(trade_ids)
    if not pa.types.is_dictionary(encoded_trade_ids.type):
        raise TypeError("trade_id dictionary encoding failed")
    dictionary = cast(pa.DictionaryArray, encoded_trade_ids)
    codes = np.asarray(
        dictionary.indices.to_numpy(zero_copy_only=False), dtype=np.int64
    )
    _, first_indices = np.unique(codes, return_index=True)
    keep = np.sort(first_indices)
    return ordered.take(pa.array(keep, type=pa.int64()))


def _integer_i64(column: pa.ChunkedArray) -> np.ndarray[Any, Any]:
    values = column.combine_chunks().to_numpy(zero_copy_only=False)
    if np.issubdtype(values.dtype, np.unsignedinteger) and bool(
        np.any(values > np.iinfo(np.int64).max)
    ):
        raise OverflowError("unsigned Arrow value exceeds Rust int64")
    return np.ascontiguousarray(values, dtype=np.int64)


def _timestamp_us(column: pa.ChunkedArray) -> np.ndarray[Any, Any]:
    values = column.combine_chunks().to_numpy(zero_copy_only=False)
    return np.ascontiguousarray(values.astype("datetime64[us]").view(np.int64))


def _decimal_raw_i64(column: pa.ChunkedArray) -> np.ndarray[Any, Any]:
    array = column.combine_chunks()
    if not (pa.types.is_decimal128(array.type) or pa.types.is_decimal256(array.type)):
        raise TypeError(f"expected Arrow decimal column, got {array.type}")
    if int(array.type.scale) != 10:
        raise ValueError(f"expected scale=10 decimal column, got {array.type}")
    if array.null_count:
        raise ValueError("Rust trade arrays cannot contain null decimals")
    words_per_value = int(array.type.bit_width) // 64
    data = array.buffers()[1]
    if data is None:
        return np.empty(0, dtype=np.int64)
    all_words = np.frombuffer(data, dtype="<i8").reshape(-1, words_per_value)
    selected_words = all_words[array.offset : array.offset + len(array)]
    low = selected_words[:, 0]
    if words_per_value > 1 and bool(np.any(selected_words[:, 1:] != 0)):
        raise OverflowError("scaled Arrow decimal exceeds non-negative Rust int64")
    if bool(np.any(low < 0)):
        raise OverflowError("Rust trade arrays require non-negative decimals")
    return np.ascontiguousarray(low, dtype=np.int64)


def _first_log_indexes(column: pa.ChunkedArray) -> np.ndarray[Any, Any]:
    array = column.combine_chunks()
    if not (pa.types.is_list(array.type) or pa.types.is_large_list(array.type)):
        raise TypeError(f"expected Arrow list column, got {array.type}")
    offsets = np.asarray(array.offsets.to_numpy(zero_copy_only=False), dtype=np.int64)
    values = np.asarray(array.values.to_numpy(zero_copy_only=False), dtype=np.int64)
    result = np.zeros(len(array), dtype=np.int64)
    nonempty = offsets[1:] > offsets[:-1]
    result[nonempty] = values[offsets[:-1][nonempty]]
    return result


def _from_scaled(value: int) -> Decimal:
    return Decimal(int(value)).scaleb(-10).quantize(Q)


def _datetime_us(value: datetime) -> int:
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    delta = normalized.astimezone(timezone.utc) - _EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
