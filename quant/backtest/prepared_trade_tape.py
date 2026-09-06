"""Immutable indexes shared by Fill-only V2 and Trade-only V3 replay."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Hashable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from heapq import merge
from itertools import pairwise
from time import perf_counter
from typing import Any, Generic, Protocol, TypeVar


class IndexedTrade(Protocol):
    @property
    def market_id(self) -> int: ...

    @property
    def asset_id(self) -> str: ...

    @property
    def aggressor_side(self) -> str: ...

    @property
    def block_number(self) -> int: ...

    @property
    def block_time(self) -> datetime: ...

    @property
    def sequence(self) -> tuple[int, int, str, int, str]: ...


TTrade = TypeVar("TTrade", bound=IndexedTrade)
Q = Decimal("0.0000000001")


@dataclass(frozen=True)
class TradeGroupIndex(Generic[TTrade]):
    key: tuple[int, str, str]
    trades: Sequence[TTrade]
    block_numbers: Sequence[int]
    block_times: Sequence[datetime]


@dataclass(frozen=True)
class PreparedTradeTape(Generic[TTrade]):
    """One sorted, reusable index over an immutable trade partition."""

    index: dict[tuple[int, str, str], TradeGroupIndex[TTrade]]
    combined_groups: dict[tuple[int, str], TradeGroupIndex[TTrade]]
    trade_rows_indexed: int
    trade_groups: int
    index_build_sec: Decimal
    ordered_trades: Sequence[TTrade]
    backend_payloads: dict[Hashable, Any] = field(
        default_factory=dict,
        repr=False,
        compare=False,
    )

    def group(
        self,
        market_id: int,
        asset_id: str,
        aggressor_side: str | None = None,
    ) -> TradeGroupIndex[TTrade] | None:
        key = (int(market_id), str(asset_id).lower())
        if aggressor_side is None:
            return self.combined_groups.get(key)
        return self.index.get((*key, str(aggressor_side).upper()))


def prepare_trade_tape(
    trades: Iterable[TTrade], *, presorted: bool = False
) -> PreparedTradeTape[TTrade]:
    """Sort each source stream once and build a combined market/token stream."""

    rows = list(trades)
    started = perf_counter()
    ordered_rows = (
        tuple(rows)
        if presorted
        else tuple(sorted(rows, key=lambda item: item.sequence))
    )
    if presorted and any(
        left.sequence > right.sequence
        for left, right in pairwise(ordered_rows)
    ):
        raise ValueError("presorted trade tape is not ordered by sequence")
    grouped: dict[tuple[int, str, str], list[TTrade]] = {}
    for trade in ordered_rows:
        key = (
            int(trade.market_id),
            str(trade.asset_id).lower(),
            str(trade.aggressor_side).upper(),
        )
        grouped.setdefault(key, []).append(trade)

    index: dict[tuple[int, str, str], TradeGroupIndex[TTrade]] = {}
    for key, source_rows in grouped.items():
        ordered = tuple(source_rows)
        index[key] = _group_index(key, ordered)

    pairs = {(market_id, asset_id) for market_id, asset_id, _ in index}
    combined: dict[tuple[int, str], TradeGroupIndex[TTrade]] = {}
    for market_id, asset_id in pairs:
        streams = [
            group.trades
            for side in ("BUY", "SELL")
            if (group := index.get((market_id, asset_id, side))) is not None
        ]
        ordered = tuple(
            merge(*streams, key=lambda item: item.sequence)
        )
        combined[(market_id, asset_id)] = _group_index(
            (market_id, asset_id, "*"), ordered
        )

    elapsed = Decimal(str(perf_counter() - started)).quantize(
        Q, rounding=ROUND_HALF_UP
    )
    return PreparedTradeTape(
        index=index,
        combined_groups=combined,
        trade_rows_indexed=len(rows),
        trade_groups=len(index),
        index_build_sec=elapsed,
        ordered_trades=ordered_rows,
    )


def slice_trade_group(
    group: TradeGroupIndex[TTrade] | None,
    *,
    start_block: int | None = None,
    end_block: int | None = None,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    start_inclusive: bool = True,
    end_inclusive: bool = True,
) -> tuple[TTrade, ...]:
    """Return an ordered bounded view without scanning unrelated rows."""

    if group is None:
        return ()
    start = 0
    end = len(group.trades)
    if start_block is not None:
        block_start = (
            bisect_left(group.block_numbers, int(start_block))
            if start_inclusive
            else bisect_right(group.block_numbers, int(start_block))
        )
        start = max(start, block_start)
    if end_block is not None:
        block_end = (
            bisect_right(group.block_numbers, int(end_block))
            if end_inclusive
            else bisect_left(group.block_numbers, int(end_block))
        )
        end = min(end, block_end)
    if start_ts is not None:
        time_start = (
            bisect_left(group.block_times, start_ts)
            if start_inclusive
            else bisect_right(group.block_times, start_ts)
        )
        start = max(start, time_start)
    if end_ts is not None:
        time_end = (
            bisect_right(group.block_times, end_ts)
            if end_inclusive
            else bisect_left(group.block_times, end_ts)
        )
        end = min(end, time_end)
    if end <= start:
        return ()
    return tuple(group.trades[start:end])


def _group_index(
    key: tuple[int, str, str], ordered: tuple[TTrade, ...]
) -> TradeGroupIndex[TTrade]:
    return TradeGroupIndex(
        key=key,
        trades=ordered,
        block_numbers=tuple(int(row.block_number) for row in ordered),
        block_times=tuple(row.block_time for row in ordered),
    )


def prepared_tape_summary(tape: PreparedTradeTape[IndexedTrade]) -> Mapping[str, Any]:
    return {
        "trade_rows_indexed": tape.trade_rows_indexed,
        "trade_groups": tape.trade_groups,
        "index_build_sec": tape.index_build_sec,
    }
