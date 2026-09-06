#!/usr/bin/env python3
"""Run Python/Rust V2/V3 parity against a pinned real OrderFilled tape."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.orderfilled_v2_replay import (  # noqa: E402
    CapacityLedger,
    V2TakerOrder,
    load_v2_trade_prints,
    prepare_v2_trade_tape,
    replay_v2_taker_orders_with_diagnostics,
)
from quant.backtest.trade_only_v3 import (  # noqa: E402
    LiquidityIntent,
    TradeOnlyOrder,
    replay_trade_only_orders_with_diagnostics,
)


DEFAULT_MARKET_ID = 2_416_380
DEFAULT_ASSET_ID = "68318bbce9b14a8f63bdd6e516b07b0e6198e1e503ab965f93446698c7a1d5b5"
DEFAULT_FROM_BLOCK = 89_439_341
DEFAULT_TO_BLOCK = 89_483_591


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market-id", type=int, default=DEFAULT_MARKET_ID)
    parser.add_argument("--asset-id", default=DEFAULT_ASSET_ID)
    parser.add_argument("--from-block", type=int, default=DEFAULT_FROM_BLOCK)
    parser.add_argument("--to-block", type=int, default=DEFAULT_TO_BLOCK)
    parser.add_argument("--trade-limit", type=int, default=50_000)
    parser.add_argument("--orders", type=int, default=100)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    load_started = perf_counter()
    trades = load_v2_trade_prints(
        market_id=args.market_id,
        asset_id=args.asset_id,
        from_block=args.from_block,
        to_block=args.to_block,
        limit=max(1, args.trade_limit),
    )
    load_seconds = perf_counter() - load_started
    if len(trades) < 2:
        raise SystemExit("real canary requires at least two pinned trade rows")
    prepared = prepare_v2_trade_tape(trades)
    v2_orders = _v2_orders(trades, max(1, args.orders))
    v3_orders = [_to_v3(order) for order in v2_orders]

    v2_runs = {
        backend: _run_v2(v2_orders, prepared, backend)
        for backend in ("python", "rust")
    }
    v3_runs = {
        backend: _run_v3(v3_orders, prepared, backend)
        for backend in ("python", "rust")
    }
    v2_equal = _parity(v2_runs)
    v3_equal = _parity(v3_runs)
    payload = {
        "schema_version": "FillOnlyRealClickHouseCanaryV1",
        "uses_lob_data": False,
        "source_table": "trade_prints_one_sided",
        "source_pin": {
            "market_id": args.market_id,
            "asset_id": args.asset_id.lower(),
            "from_block": args.from_block,
            "to_block": args.to_block,
            "first_trade_id": trades[0].trade_id,
            "last_trade_id": trades[-1].trade_id,
            "first_trade_ts": trades[0].block_time.isoformat(),
            "last_trade_ts": trades[-1].block_time.isoformat(),
        },
        "trade_rows": len(trades),
        "orders": len(v2_orders),
        "clickhouse_load_seconds": load_seconds,
        "v2": {"parity": v2_equal, "runs": v2_runs},
        "v3_source_confirmed": {"parity": v3_equal, "runs": v3_runs},
        "passed": v2_equal and v3_equal,
    }
    encoded = json.dumps(payload, default=str, indent=2, sort_keys=True)
    print(encoded)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(encoded + "\n", encoding="utf-8")
        os.replace(temporary, args.output)
    return 0 if payload["passed"] else 1


def _v2_orders(trades: list[Any], count: int) -> list[V2TakerOrder]:
    usable_end = max(1, len(trades) - 2)
    stride = max(1, usable_end // count)
    indexes = list(range(0, usable_end, stride))[:count]
    orders: list[V2TakerOrder] = []
    for order_index, trade_index in enumerate(indexes):
        signal = trades[trade_index]
        limit = (
            min(Decimal(1), signal.price + Decimal("0.03"))
            if signal.aggressor_side == "BUY"
            else max(Decimal(0), signal.price - Decimal("0.03"))
        )
        orders.append(
            V2TakerOrder(
                order_id=f"real-canary-{order_index}",
                market_id=signal.market_id,
                asset_id=signal.asset_id,
                side=signal.aggressor_side,
                limit_price=limit,
                size=Decimal("10"),
                signal_block=signal.block_number,
                signal_ts=signal.block_time,
                latency_blocks=1,
                latency=timedelta(seconds=1),
                horizon_blocks=100,
                horizon=timedelta(seconds=30),
                participation_rate=Decimal("0.025"),
                price_buffer=Decimal("0.005"),
                tif="GTD",
                signal_source_trade_id=signal.trade_id,
                exclude_signal_source_trade=True,
            )
        )
    return orders


def _to_v3(order: V2TakerOrder) -> TradeOnlyOrder:
    assert order.signal_block is not None
    assert order.signal_ts is not None
    assert order.horizon is not None
    assert order.horizon_blocks is not None
    return TradeOnlyOrder(
        order_id=order.order_id,
        market_id=order.market_id,
        asset_id=order.asset_id,
        side=order.side,
        limit_price=order.limit_price,
        size=order.size,
        signal_block=order.signal_block,
        signal_ts=order.signal_ts,
        tif="GTD",
        liquidity_intent=LiquidityIntent.TAKER,
        latency=order.latency,
        latency_blocks=order.latency_blocks,
        horizon=order.horizon,
        horizon_blocks=order.horizon_blocks,
        signal_source_trade_id=order.signal_source_trade_id,
    )


def _run_v2(orders: list[V2TakerOrder], prepared: Any, backend: str) -> dict[str, Any]:
    started = perf_counter()
    results, ledger, diagnostics = replay_v2_taker_orders_with_diagnostics(
        orders,
        prepared_tape=prepared,
        ledger=CapacityLedger(),
        backend=backend,  # type: ignore[arg-type]
    )
    return _receipt(results, ledger.snapshot(), diagnostics, perf_counter() - started)


def _run_v3(orders: list[TradeOnlyOrder], prepared: Any, backend: str) -> dict[str, Any]:
    started = perf_counter()
    results, ledger, diagnostics = replay_trade_only_orders_with_diagnostics(
        orders,
        None,
        "taker_source_confirmed",
        prepared_tape=prepared,
        ledger_id="real-canary",
        backend=backend,  # type: ignore[arg-type]
    )
    ledger_payload = ledger.snapshot()
    # Ledger IDs are run metadata, not allocation semantics.
    ledger_payload.pop("ledger_id", None)
    return _receipt(results, ledger_payload, diagnostics, perf_counter() - started)


def _receipt(results: list[Any], ledger: Any, diagnostics: Any, elapsed: float) -> dict[str, Any]:
    rows = [result.as_dict() for result in results]
    sample_fills = [
        fill
        for result in rows
        for fill in result.get("fills", [])
    ][:10]
    return {
        "elapsed_seconds": elapsed,
        "status_counts": _status_counts(rows),
        "result_sha256": _sha256(rows),
        "ledger_sha256": _sha256(ledger),
        "diagnostics": diagnostics.as_dict(),
        "sample_fills": sample_fills,
    }


def _parity(runs: dict[str, dict[str, Any]]) -> bool:
    return (
        runs["python"]["result_sha256"] == runs["rust"]["result_sha256"]
        and runs["python"]["ledger_sha256"] == runs["rust"]["ledger_sha256"]
    )


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        status = str(row["status"])
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _sha256(value: Any) -> str:
    encoded = json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
