#!/usr/bin/env python3
"""Profile PML2 dynamic maker replay on real compact L2 rows."""

from __future__ import annotations

import argparse
import cProfile
import gzip
import hashlib
import json
import pstats
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from types import MethodType
from typing import Any, Literal

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.pml2.contracts import (  # noqa: E402
    BookDeltaEvent,
    BookLevel,
    BookSnapshotEvent,
    EconomicBookSide,
    Outcome,
    Pml2OrderIntent,
    RawOrderSide,
    ReplayAuditMode,
    TimeInForce,
    TradeEvent,
    canonical_hash,
    canonical_value,
)
from quant.backtest.pml2.session import ReplayExecutionSession  # noqa: E402

UTC = timezone.utc
DEFAULT_PREPARED_INPUT = Path(
    "/data/jiahuaiyu/prediction-market-quant/execution_comparison_input_cache/"
    "8bb82e0f7b8872eed529513d2c7c41f02e27e431099a47372095e986c85d1e7a.json.gz"
)
DEFAULT_L2_SLICE = Path(
    "/data/jiahuaiyu/prediction-market-quant/l2_backtest_slice_cache/"
    "437d89999b6b79b58fcf244606b111e929fa3daf1cc2729b143f9423446b0b15.parquet"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "backtest_framework"
    / "nautilus_trader_comparison"
    / "pml2_dynamic_profile"
    / "latest.json"
)


def _parse_levels(raw: Any) -> tuple[BookLevel, ...]:
    parsed = json.loads(str(raw or "[]"))
    return tuple(
        BookLevel(Decimal(str(item[0])), Decimal(str(item[1])))
        for item in parsed
        if isinstance(item, list)
        and len(item) >= 2
        and Decimal(str(item[0])) > 0
        and Decimal(str(item[1])) > 0
    )


def _load_metadata(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    return [dict(item) for item in payload["selected"]]


def _load_rows(path: Path, asset_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - environment contract
        raise RuntimeError("pyarrow is required") from exc
    table = pq.read_table(
        path,
        columns=[
            "event_type",
            "timestamp",
            "timestamp_received",
            "collector_seq",
            "sequence_in_message",
            "bids",
            "asks",
            "side",
            "price",
            "size",
            "book_hash",
            "source",
            "asset_id",
        ],
        filters=[("asset_id", "in", asset_ids)],
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in table.to_pylist():
        grouped[str(row["asset_id"])].append(row)
    for rows in grouped.values():
        rows.sort(
            key=lambda row: (
                row["timestamp_received"],
                int(row.get("collector_seq") or 0),
                int(row.get("sequence_in_message") or 0),
            )
        )
    return grouped


def _event_id(row: dict[str, Any]) -> str:
    raw = "|".join(
        str(row.get(field) or "")
        for field in (
            "asset_id",
            "event_type",
            "timestamp",
            "timestamp_received",
            "collector_seq",
            "sequence_in_message",
            "side",
            "price",
            "size",
            "book_hash",
            "bids",
            "asks",
        )
    )
    return f"compact-l2:{hashlib.sha256(raw.encode()).hexdigest()}"


def _real_events(
    metadata: dict[str, Any], rows: list[dict[str, Any]], window_seconds: int
) -> tuple[list[Any], datetime, datetime]:
    baseline_index = next(
        index for index, row in enumerate(rows) if row["event_type"] == "book"
    )
    start = rows[baseline_index]["timestamp_received"]
    end = min(rows[-1]["timestamp_received"], start + timedelta(seconds=window_seconds))
    outcome = Outcome(str(metadata["outcome"]).upper())
    events: list[Any] = []
    for row in rows[baseline_index:]:
        received = row["timestamp_received"]
        if received > end:
            break
        exchange = row["timestamp"]
        if received < exchange:
            continue
        event_type = str(row["event_type"])
        common = {
            "condition_id": str(metadata["condition_id"]),
            "market_id": str(metadata["market_id"]),
            "asset_id": str(metadata["asset_id"]),
            "outcome": outcome,
            "exchange_ts": exchange,
            "local_ts": received,
            "book_epoch": 0,
            "source": str(row.get("source") or "polymarket_market_ws_archive"),
            "source_received_ts": received,
        }
        if event_type == "book":
            events.append(
                BookSnapshotEvent(
                    snapshot_id=_event_id(row),
                    bids=_parse_levels(row.get("bids")),
                    asks=_parse_levels(row.get("asks")),
                    sequence=None,
                    is_full_depth=True,
                    book_hash=str(row.get("book_hash") or ""),
                    **common,
                )
            )
        elif event_type == "price_change":
            events.append(
                BookDeltaEvent(
                    event_id=_event_id(row),
                    side=(
                        EconomicBookSide.BID
                        if str(row.get("side")).upper() == "BUY"
                        else EconomicBookSide.ASK
                    ),
                    price=Decimal(str(row["price"])),
                    new_size=Decimal(str(row["size"])),
                    sequence=None,
                    **common,
                )
            )
        elif event_type == "last_trade_price" and Decimal(str(row["size"])) > 0:
            events.append(
                TradeEvent(
                    event_id=_event_id(row),
                    price=Decimal(str(row["price"])),
                    size=Decimal(str(row["size"])),
                    aggressor_side=RawOrderSide(str(row.get("side") or "BUY").upper()),
                    source_sequence=int(row.get("collector_seq") or 0),
                    event_group_id=_event_id(row),
                    source_event_ids=(_event_id(row),),
                    **common,
                )
            )
    return events, start, end


def _orders(
    *,
    run_id: str,
    metadata: dict[str, Any],
    events: list[Any],
    start: datetime,
    end: datetime,
    count: int,
    lifetime_seconds: int,
) -> list[Pml2OrderIntent]:
    book_events = [
        event
        for event in events
        if isinstance(event, (BookSnapshotEvent, BookDeltaEvent))
    ]
    if not any(isinstance(event, BookSnapshotEvent) for event in book_events):
        return []
    prices: dict[Decimal, Decimal] = {}
    cursor = 0
    usable_end = end - timedelta(seconds=lifetime_seconds + 1)
    if usable_end <= start:
        return []
    step = (usable_end - start) / (count + 1)
    orders: list[Pml2OrderIntent] = []
    for ordinal in range(count):
        submit = start + step * (ordinal + 1)
        while cursor < len(book_events) and book_events[cursor].local_ts <= submit:
            event = book_events[cursor]
            cursor += 1
            if isinstance(event, BookSnapshotEvent):
                prices = {level.price: level.size for level in event.bids}
                continue
            if event.side != EconomicBookSide.BID:
                continue
            if event.new_size <= 0:
                prices.pop(event.price, None)
            else:
                prices[event.price] = event.new_size
        if not prices:
            continue
        limit = max(prices)
        orders.append(
            Pml2OrderIntent(
                run_id=run_id,
                order_id=f"dynamic-{metadata['market_id']}-{ordinal:06d}",
                strategy_id="pml2-dynamic-profile",
                condition_id=str(metadata["condition_id"]),
                market_id=str(metadata["market_id"]),
                asset_id=str(metadata["asset_id"]),
                outcome=Outcome(str(metadata["outcome"]).upper()),
                side=RawOrderSide.BUY,
                size=Decimal("1"),
                limit_price=limit,
                tif=TimeInForce.GTD,
                signal_ts=submit,
                observed_ts=submit,
                submit_ts=submit,
                post_only=True,
                expires_at=submit + timedelta(seconds=lifetime_seconds),
                entry_latency_ms=0,
                response_latency_ms=0,
                venue_delay_ms=0,
                metadata={"category": str(metadata.get("category") or "unknown")},
            )
        )
    return orders


def _execution_digest(session: ReplayExecutionSession) -> str:
    return canonical_hash(
        {
            "orders": [
                {
                    "order_id": result.order.order_id,
                    "status": result.status,
                    "reason": result.reason,
                    "remaining_size": result.remaining_size,
                    "queue_ahead": result.queue_ahead,
                    "fills": [fill.as_dict() for fill in result.fills],
                }
                for result in session.results()
            ],
            "final_book_hash": session.exchange_book.state_hash,
        }
    )


def _run(
    events: list[Any],
    orders: list[Pml2OrderIntent],
    mode: Literal["full", "chain_only", "no_audit"],
    profile_path: Path,
) -> dict[str, Any]:
    session = ReplayExecutionSession(
        run_id=orders[0].run_id,
        profile="realistic",
        audit_mode=(
            ReplayAuditMode.CHAIN_ONLY
            if mode == "chain_only"
            else ReplayAuditMode.FULL
        ),
    )
    if mode == "no_audit":
        setattr(
            session,
            "_append_audit",
            MethodType(lambda _self, _event, _ts: None, session),
        )
    stage = perf_counter()
    for event in events:
        if isinstance(event, BookSnapshotEvent):
            session.ingest_snapshot(event)
        elif isinstance(event, BookDeltaEvent):
            session.ingest_delta(event)
        else:
            session.ingest_trade(event)
    ingest_seconds = perf_counter() - stage
    stage = perf_counter()
    for order in orders:
        session.submit_order(order)
    submit_seconds = perf_counter() - stage
    profiler = cProfile.Profile()
    stage = perf_counter()
    profiler.enable()
    session.run()
    profiler.disable()
    run_seconds = perf_counter() - stage
    profiler.dump_stats(profile_path)
    stats = pstats.Stats(profiler).sort_stats("cumulative")
    top = []
    raw_stats: dict[Any, Any] = getattr(stats, "stats")
    for (filename, line, function), values in list(raw_stats.items()):
        primitive, total, own, cumulative, _ = values
        top.append(
            {
                "function": f"{Path(filename).name}:{line}:{function}",
                "calls": total,
                "primitive_calls": primitive,
                "own_seconds": own,
                "cumulative_seconds": cumulative,
            }
        )
    top.sort(key=lambda item: item["cumulative_seconds"], reverse=True)
    statuses = Counter(result.status.value for result in session.results())
    return {
        "mode": mode,
        "ingest_seconds": ingest_seconds,
        "submit_seconds": submit_seconds,
        "run_seconds": run_seconds,
        "total_seconds": ingest_seconds + submit_seconds + run_seconds,
        "event_envelopes_processed": session.audit_event_count,
        "stored_audit_events": len(session.audit_events),
        "order_count": len(orders),
        "match_count": len(session.matches),
        "status_counts": dict(statuses),
        "execution_digest": _execution_digest(session),
        "replay_hash": session.replay_hash,
        "final_book_hash": session.exchange_book.state_hash,
        "top_cumulative": top[:30],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepared-input", type=Path, default=DEFAULT_PREPARED_INPUT)
    parser.add_argument("--l2-slice", type=Path, default=DEFAULT_L2_SLICE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--market-limit", type=int, default=10)
    parser.add_argument("--orders-per-market", type=int, default=50)
    parser.add_argument("--window-seconds", type=int, default=300)
    parser.add_argument("--lifetime-seconds", type=int, default=30)
    parser.add_argument("--modes", default="full,chain_only,no_audit")
    args = parser.parse_args()

    started = perf_counter()
    metadata = _load_metadata(args.prepared_input)[: args.market_limit]
    grouped = _load_rows(args.l2_slice, [str(item["asset_id"]) for item in metadata])
    load_seconds = perf_counter() - started
    run_id = "pml2-dynamic-profile-v1"
    events: list[Any] = []
    orders: list[Pml2OrderIntent] = []
    markets: list[dict[str, Any]] = []
    for item in metadata:
        asset_id = str(item["asset_id"])
        market_events, start, end = _real_events(
            item, grouped[asset_id], args.window_seconds
        )
        market_orders = _orders(
            run_id=run_id,
            metadata=item,
            events=market_events,
            start=start,
            end=end,
            count=args.orders_per_market,
            lifetime_seconds=args.lifetime_seconds,
        )
        events.extend(market_events)
        orders.extend(market_orders)
        markets.append(
            {
                "market_id": item["market_id"],
                "asset_id": asset_id,
                "category": item.get("category"),
                "event_count": len(market_events),
                "order_count": len(market_orders),
                "start": start,
                "end": end,
            }
        )
    if not orders:
        raise RuntimeError("benchmark corpus produced no orders")
    modes = tuple(item.strip() for item in args.modes.split(",") if item.strip())
    output_dir = args.output.parent
    output_dir.mkdir(parents=True, exist_ok=True)
    results = [
        _run(
            events,
            orders,
            mode,
            output_dir / f"{args.output.stem}.{mode}.prof",
        )
        for mode in modes
        if mode in {"full", "chain_only", "no_audit"}
    ]
    digests = {str(item["execution_digest"]) for item in results}
    payload = canonical_value(
        {
            "schema_version": "pml2_dynamic_profile_v1",
            "data_contract": "REAL_COMPACT_L2_RESEARCH_PERFORMANCE_CORPUS",
            "formal_archive_completeness_proven": False,
            "prepared_input": str(args.prepared_input),
            "l2_slice": str(args.l2_slice),
            "load_seconds": load_seconds,
            "market_count": len(markets),
            "event_count": len(events),
            "order_count": len(orders),
            "markets": markets,
            "runs": results,
            "execution_equivalent": len(digests) == 1,
            "speedup_without_full_audit": (
                None
                if len(results) < 2 or results[1]["run_seconds"] <= 0
                else results[0]["run_seconds"] / results[1]["run_seconds"]
            ),
        }
    )
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if payload["execution_equivalent"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
