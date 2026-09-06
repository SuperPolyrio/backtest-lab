#!/usr/bin/env python3
"""Run one independent L2 book-walk implementation over a JSON corpus."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _reference(case: Mapping[str, Any]) -> dict[str, Any]:
    remaining = Decimal(str(case["size"]))
    side = str(case["side"]).upper()
    limit = Decimal(str(case["limit_price"]))
    levels = case["asks"] if side == "BUY" else case["bids"]
    ordered = sorted(
        ((Decimal(str(row[0])), Decimal(str(row[1]))) for row in levels),
        key=lambda row: row[0],
        reverse=side == "SELL",
    )
    fills: list[tuple[Decimal, Decimal]] = []
    for price, available in ordered:
        if (side == "BUY" and price > limit) or (side == "SELL" and price < limit):
            break
        quantity = min(remaining, available)
        if quantity > 0:
            fills.append((price, quantity))
            remaining -= quantity
        if remaining <= 0:
            break
    return _result(case, fills, engine="independent_reference")


def _pml2(case: Mapping[str, Any]) -> dict[str, Any]:
    from datetime import datetime, timedelta, timezone

    from quant.backtest.pml2.contracts import (
        BookLevel,
        BookSnapshotEvent,
        Outcome,
        Pml2OrderIntent,
        RawOrderSide,
        TimeInForce,
    )
    from quant.backtest.pml2.session import ReplayExecutionSession

    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run_id = f"differential:{case['case_id']}"
    session = ReplayExecutionSession(run_id=run_id, profile="optimistic")
    session.ingest_snapshot(
        BookSnapshotEvent(
            snapshot_id=f"snapshot:{case['case_id']}",
            condition_id="differential-condition",
            market_id="differential-market",
            asset_id="yes",
            outcome=Outcome.YES,
            exchange_ts=start,
            local_ts=start,
            book_epoch=0,
            bids=tuple(
                BookLevel(Decimal(str(price)), Decimal(str(size)))
                for price, size in case["bids"]
            ),
            asks=tuple(
                BookLevel(Decimal(str(price)), Decimal(str(size)))
                for price, size in case["asks"]
            ),
            source="differential_fixture",
            tick_size=Decimal(str(case.get("tick_size") or "0.001")),
        )
    )
    order_at = start + timedelta(seconds=1)
    side = RawOrderSide(str(case["side"]).upper())
    session.submit_order(
        Pml2OrderIntent(
            run_id=run_id,
            order_id=f"order:{case['case_id']}",
            strategy_id="differential",
            condition_id="differential-condition",
            market_id="differential-market",
            asset_id="yes",
            outcome=Outcome.YES,
            side=side,
            size=Decimal(str(case["size"])),
            limit_price=Decimal(str(case["limit_price"])),
            tif=TimeInForce(str(case.get("tif") or "FAK").upper()),
            signal_ts=order_at,
            observed_ts=order_at,
            submit_ts=order_at,
            entry_latency_ms=0,
            response_latency_ms=0,
            venue_delay_ms=0,
        )
    )
    session.run()
    result = session.result(f"order:{case['case_id']}")
    return _result(
        case,
        [(fill.raw_price, fill.qty) for fill in result.fills],
        engine="pml2_replay_v1",
    )


def _nautilus(case: Mapping[str, Any]) -> dict[str, Any]:
    from nautilus_trader.core.uuid import UUID4
    from nautilus_trader.model.book import OrderBook
    from nautilus_trader.model.data import BookOrder, OrderBookDelta
    from nautilus_trader.model.enums import (
        BookAction,
        BookType,
        OrderSide,
        TimeInForce,
    )
    from nautilus_trader.model.identifiers import (
        ClientOrderId,
        InstrumentId,
        StrategyId,
        TraderId,
    )
    from nautilus_trader.model.objects import Price, Quantity
    from nautilus_trader.model.orders import LimitOrder

    price_prec = max(
        1,
        -Decimal(str(case.get("tick_size") or "0.001")).as_tuple().exponent,
        -Decimal(str(case["limit_price"])).as_tuple().exponent,
    )
    size_prec = 6

    def price_text(value: Any) -> str:
        return f"{Decimal(str(value)):.{price_prec}f}"

    def size_text(value: Any) -> str:
        return f"{Decimal(str(value)):.{size_prec}f}"

    instrument_id = InstrumentId.from_str("OFFLINE.POLY")
    book = OrderBook(instrument_id=instrument_id, book_type=BookType.L2_MBP)
    sequence = 0
    for _side_name, enum_side, rows in (
        ("BUY", OrderSide.BUY, case["bids"]),
        ("SELL", OrderSide.SELL, case["asks"]),
    ):
        for price, size in rows:
            sequence += 1
            book.apply_delta(
                OrderBookDelta(
                    instrument_id,
                    BookAction.ADD,
                    BookOrder(
                        enum_side,
                        Price.from_str(price_text(price)),
                        Quantity.from_str(size_text(size)),
                        sequence,
                    ),
                    0,
                    sequence,
                    0,
                    0,
                )
            )
    side = OrderSide.BUY if str(case["side"]).upper() == "BUY" else OrderSide.SELL
    requested_tif = str(case.get("tif") or "FOK").upper()
    nautilus_tif = {
        "FAK": TimeInForce.IOC,
        "IOC": TimeInForce.IOC,
        "FOK": TimeInForce.FOK,
    }.get(requested_tif)
    if nautilus_tif is None:
        raise ValueError(
            f"offline Nautilus book walk does not support TIF {requested_tif!r}"
        )
    order = LimitOrder(
        TraderId("OFFLINE-001"),
        StrategyId("FIDELITY-001"),
        instrument_id,
        ClientOrderId(f"O-{case['case_id']}"),
        side,
        Quantity.from_str(size_text(case["size"])),
        Price.from_str(price_text(case["limit_price"])),
        UUID4(),
        0,
        nautilus_tif,
    )
    fills = book.simulate_fills(
        order,
        price_prec,
        size_prec,
        # In the installed Nautilus API, True walks the book as an unbounded
        # market order. False preserves the LimitOrder price boundary.
        False,
    )
    if requested_tif == "FOK" and sum(size for _, size in fills) < Decimal(
        str(case["size"])
    ):
        fills = []
    return _result(
        case,
        [(Decimal(str(price)), Decimal(str(size))) for price, size in fills],
        engine="nautilus_trader",
    )


def _hftbacktest(case: Mapping[str, Any]) -> dict[str, Any]:
    import hftbacktest as hft
    import numpy as np

    rows = [
        ("BUY", Decimal(str(price)), Decimal(str(size))) for price, size in case["bids"]
    ] + [
        ("SELL", Decimal(str(price)), Decimal(str(size)))
        for price, size in case["asks"]
    ]
    data = np.zeros(len(rows) + 1, dtype=hft.event_dtype)
    for index, (side, price, size) in enumerate(rows):
        data[index] = (
            hft.EXCH_EVENT
            | hft.LOCAL_EVENT
            | hft.DEPTH_SNAPSHOT_EVENT
            | (hft.BUY_EVENT if side == "BUY" else hft.SELL_EVENT),
            1,
            1,
            float(price),
            float(size),
            0,
            0,
            0,
        )
    data[-1] = (
        hft.EXCH_EVENT | hft.LOCAL_EVENT | hft.TRADE_EVENT | hft.BUY_EVENT,
        1_000_000,
        1_000_000,
        0.5,
        0.000001,
        0,
        0,
        0,
    )
    tick_size = max(Decimal("0.000001"), Decimal(str(case.get("tick_size") or "0.001")))
    lot_size = Decimal("0.000001")
    asset = (
        hft.BacktestAsset()
        .data(data)
        .linear_asset(1.0)
        .constant_order_latency(0, 0)
        .risk_adverse_queue_model()
        .partial_fill_exchange()
        .trading_value_fee_model(0, 0)
        .tick_size(float(tick_size))
        .lot_size(float(lot_size))
    )
    engine = hft.HashMapMarketDepthBacktest([asset])
    try:
        engine.elapse(100)
        if str(case["side"]).upper() == "BUY":
            engine.submit_buy_order(
                0,
                1,
                float(case["limit_price"]),
                float(case["size"]),
                hft.GTC,
                hft.LIMIT,
                True,
            )
        else:
            engine.submit_sell_order(
                0,
                1,
                float(case["limit_price"]),
                float(case["size"]),
                hft.GTC,
                hft.LIMIT,
                True,
            )
        state = engine.state_values(0)
        filled = abs(Decimal(str(state.position)))
        notional = abs(Decimal(str(state.trading_value)))
        avg = notional / filled if filled > 0 else None
        return {
            "case_id": str(case["case_id"]),
            "engine": "hftbacktest",
            "filled_size": str(filled),
            "filled_notional": str(notional),
            "avg_fill_price": str(avg) if avg is not None else None,
        }
    finally:
        engine.close()


def _result(
    case: Mapping[str, Any],
    fills: list[tuple[Decimal, Decimal]],
    *,
    engine: str,
) -> dict[str, Any]:
    size = sum((quantity for _, quantity in fills), Decimal(0))
    notional = sum((price * quantity for price, quantity in fills), Decimal(0))
    return {
        "case_id": str(case["case_id"]),
        "engine": engine,
        "filled_size": str(size),
        "filled_notional": str(notional),
        "avg_fill_price": str(notional / size) if size > 0 else None,
        "fills": [[str(price), str(quantity)] for price, quantity in fills],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--engine",
        choices=("reference", "pml2", "nautilus", "hftbacktest"),
        required=True,
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    corpus = json.loads(args.input.read_text(encoding="utf-8"))
    runner = {
        "reference": _reference,
        "pml2": _pml2,
        "nautilus": _nautilus,
        "hftbacktest": _hftbacktest,
    }[args.engine]
    rows = [runner(case) for case in corpus["cases"]]
    payload = {
        "schema_version": "offline_book_differential_v1",
        "engine": args.engine,
        "case_count": len(rows),
        "rows": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
