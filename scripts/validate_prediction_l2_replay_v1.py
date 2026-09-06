#!/usr/bin/env python3
"""Run a read-only Prediction L2 Replay V1 smoke against a real L2 archive."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant.backtest.pml2.adapters import (  # noqa: E402
    Pml2ArchiveSnapshotLoader,
    default_l2_archive_dir,
)
from quant.backtest.pml2.contracts import (  # noqa: E402
    BookLevelBatchEvent,
    BookSnapshotEvent,
    Outcome,
    Pml2OrderIntent,
    RawOrderSide,
    TimeInForce,
    TradeEvent,
)
from quant.backtest.pml2.session import ReplayExecutionSession  # noqa: E402

DEFAULT_CONDITION_ID = (
    "0x0001cb8c0b39aeb614ab9a43867595317f06ede9c011661513065c638fbbefda"
)
DEFAULT_YES_ASSET_ID = (
    "50868012450412588231700991321379235183301872220529434142919756787462014093776"
)
DEFAULT_NO_ASSET_ID = (
    "37843096702983984154813593339817451110105539743235209768242171001456417281055"
)
DEFAULT_TIMESTAMP = "2026-07-08T10:53:23+00:00"
def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive-dir", type=Path, default=default_l2_archive_dir())
    parser.add_argument("--condition-id", default=DEFAULT_CONDITION_ID)
    parser.add_argument("--market-id", default="")
    parser.add_argument("--yes-asset-id", default=DEFAULT_YES_ASSET_ID)
    parser.add_argument("--no-asset-id", default=DEFAULT_NO_ASSET_ID)
    parser.add_argument("--timestamp", type=_timestamp, default=_timestamp(DEFAULT_TIMESTAMP))
    parser.add_argument("--maker-horizon-seconds", type=int, default=300)
    parser.add_argument(
        "--profile",
        choices=("strict", "realistic", "optimistic"),
        default="optimistic",
    )
    parser.add_argument("--size", type=Decimal, default=Decimal(1))
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = _args()
    market_id = args.market_id or args.condition_id
    end_time = args.timestamp + timedelta(
        seconds=max(1, args.maker_horizon_seconds)
    )
    restored = Pml2ArchiveSnapshotLoader(args.archive_dir).restore_condition_events(
        condition_id=args.condition_id,
        market_id=market_id,
        yes_asset_id=args.yes_asset_id,
        no_asset_id=args.no_asset_id,
        start_time=args.timestamp,
        end_time=end_time,
    )
    if not restored.restored:
        payload: dict[str, Any] = {
            "status": "DATA_NOT_READY",
            "archive_dir": str(args.archive_dir),
            "reason": restored.reason,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 2

    session = ReplayExecutionSession(
        run_id="real-archive-smoke",
        profile=args.profile,
        cold_restore_used=True,
    )
    if restored.clock_verified:
        session.register_verified_cold_restore_clock_evidence(
            restore_count=1,
            baseline_clock_count=restored.baseline_clock_count,
            raw_event_clock_count=restored.raw_event_clock_count,
            frame_evidence_count=int(restored.frame_evidence_verified),
            source_manifest_hash=restored.source_manifest_hash,
        )
    for event in restored.events:
        if isinstance(event, BookSnapshotEvent):
            session.ingest_snapshot(event)
        elif isinstance(event, BookLevelBatchEvent):
            session.ingest_level_batch(event)
        elif isinstance(event, TradeEvent):
            session.ingest_trade(event)
    yes_event = next(
        event
        for event in restored.events
        if isinstance(event, BookSnapshotEvent) and event.outcome == Outcome.YES
    )
    best_ask = min(level.price for level in yes_event.asks)
    order_ts = args.timestamp + timedelta(seconds=1)
    session.submit_order(
        Pml2OrderIntent(
            run_id=session.run_id,
            order_id="real-buy-yes",
            strategy_id="archive-smoke",
            condition_id=args.condition_id,
            market_id=market_id,
            asset_id=args.yes_asset_id,
            outcome=Outcome.YES,
            side=RawOrderSide.BUY,
            size=args.size,
            limit_price=best_ask,
            tif=TimeInForce.FAK,
            signal_ts=order_ts,
            observed_ts=order_ts,
            submit_ts=order_ts,
            entry_latency_ms=0,
            venue_delay_ms=0,
            response_latency_ms=0,
        )
    )
    session.run()
    result = session.result("real-buy-yes")
    payload = {
        "status": "PASS" if result.filled_size > 0 else "NO_FILL",
        "archive_dir": str(args.archive_dir),
        "condition_id": args.condition_id,
        "yes_asset_id": args.yes_asset_id,
        "no_asset_id": args.no_asset_id,
        "point_in_time": args.timestamp.isoformat(),
        "event_window_end": end_time.isoformat(),
        "source_files": list(restored.source_files),
        "source_file_count": len(restored.source_files),
        "source_row_count": restored.row_count,
        "source_manifest_hash": restored.source_manifest_hash,
        "restored_snapshot_count": restored.snapshot_count,
        "restored_delta_update_count": restored.delta_count,
        "restored_trade_count": restored.trade_count,
        "clock_verified": restored.clock_verified,
        "clock_evidence": restored.clock_evidence,
        "baseline_clock_count": restored.baseline_clock_count,
        "raw_event_clock_count": restored.raw_event_clock_count,
        "frame_evidence_verified": restored.frame_evidence_verified,
        "best_ask": format(best_ask, "f"),
        "order": result.as_dict(),
        "report": session.report(),
        "execution_matches": [item.as_dict() for item in session.matches],
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
