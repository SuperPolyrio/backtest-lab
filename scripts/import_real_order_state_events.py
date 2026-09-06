#!/usr/bin/env python3
"""Import real order state events for fill-first backtest audits."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_source_state import upsert_external_source_import_state
from quant.backtest.order_state import normalize_order_state_event, upsert_real_order_state_events
from quant.core.db import postgres_connection
from quant.core.schema import create_schema


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="JSON or JSONL file containing order state events")
    parser.add_argument("--source", default=None, help="Override source for all rows, for example clob-api or order-stream")
    parser.add_argument("--run-id", type=int, default=None, help="Attach all rows to this quant backtest run_id")
    parser.add_argument("--state-key", default=None, help="Persist import freshness under this key.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not run quant schema initialization before import")
    return parser.parse_args()


def load_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if path.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    parsed = json.loads(text)
    if isinstance(parsed, list):
        return [dict(item) for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict) and isinstance(parsed.get("events"), list):
        return [dict(item) for item in parsed["events"] if isinstance(item, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    raise ValueError(f"unsupported input JSON shape in {path}")


def main() -> int:
    args = parse_args()
    raw_events = load_events(args.input)
    events = [
        normalize_order_state_event(event, source=args.source, run_id=args.run_id)
        for event in raw_events
    ]
    with postgres_connection() as conn:
        if not args.skip_init_schema:
            create_schema(conn)
        inserted = upsert_real_order_state_events(conn, events)
        if args.state_key:
            upsert_external_source_import_state(
                conn,
                state_key=args.state_key,
                source_type="real_order_state_events",
                source=args.source or "manual",
                endpoint=str(args.input),
                params={"run_id": args.run_id} if args.run_id is not None else {},
                last_payload_count=len(raw_events),
                last_rows_written=inserted,
                last_error=None,
            )
    print(
        json.dumps(
            {
                "input": str(args.input),
                "source_override": args.source,
                "run_id_override": args.run_id,
                "state_key": args.state_key,
                "events_read": len(raw_events),
                "events_written": inserted,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
