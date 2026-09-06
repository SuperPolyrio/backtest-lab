#!/usr/bin/env python3
"""Collect external order API payloads into quant.real_order_state_events."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.order_event_collector import events_from_order_payload  # noqa: E402
from quant.backtest.order_state_collection import (  # noqa: E402
    DEFAULT_CURSOR_KEYS,
    build_collection_request_params,
    event_time_watermark,
    extract_response_cursor,
    load_real_order_state_collection_state,
    upsert_real_order_state_collection_state,
)
from quant.backtest.order_state import upsert_real_order_state_events  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=None, help="JSON/JSONL file containing external order payloads.")
    parser.add_argument("--url", default=None, help="HTTP endpoint returning order payloads.")
    parser.add_argument("--param", action="append", default=[], help="Query param as key=value; repeatable.")
    parser.add_argument("--header", action="append", default=[], help="HTTP header as Key=Value; repeatable.")
    parser.add_argument("--source", default="order-api", help="Source stored in real_order_state_events.")
    parser.add_argument("--run-id", type=int, default=None, help="Attach collected events to this backtest run_id.")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds.")
    parser.add_argument("--poll-seconds", type=float, default=0.0, help="Repeat collection every N seconds. 0 means once.")
    parser.add_argument("--max-polls", type=int, default=1, help="Max polls when --poll-seconds is set.")
    parser.add_argument("--state-key", default=None, help="Persist incremental collection cursor/watermark under this key.")
    parser.add_argument("--since-param", default=None, help="Query parameter name populated from last_event_time, e.g. updated_after.")
    parser.add_argument("--cursor-param", default=None, help="Query parameter name populated from last_cursor, e.g. cursor.")
    parser.add_argument("--response-cursor-key", action="append", default=[], help="Response field containing the next cursor; repeatable.")
    parser.add_argument("--initial-since", default=None, help="Initial timestamp used with --since-param when no state exists.")
    parser.add_argument("--dry-run", action="store_true", help="Print normalized events; do not write Postgres.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not run schema initialization before writing.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.input and not args.url:
        raise SystemExit("provide --input or --url")
    total_read = 0
    total_events = 0
    total_written = 0
    normalized_preview: list[dict[str, Any]] = []
    last_state: dict[str, Any] | None = None
    base_params = _kv_pairs(args.param)
    headers = _kv_pairs(args.header)
    polls = max(1, int(args.max_polls))
    for index in range(polls):
        request_params = request_params_for_poll(args, base_params)
        payload = load_payload(args, params=request_params, headers=headers)
        events = events_from_order_payload(payload, source=args.source, run_id=args.run_id)
        total_read += _payload_count(payload)
        total_events += len(events)
        if args.dry_run:
            normalized_preview.extend(events[:20])
            last_state = state_preview(args, payload, events, request_params)
        else:
            with postgres_connection() as conn:
                if not args.skip_init_schema:
                    create_schema(conn)
                written = upsert_real_order_state_events(conn, events)
                total_written += written
                if args.state_key:
                    last_state = update_collection_state(args, conn, payload, events, request_params, written)
        if args.poll_seconds <= 0 or index >= polls - 1:
            break
        time.sleep(float(args.poll_seconds))
    print(
        json.dumps(
            {
                "source": args.source,
                "run_id": args.run_id,
                "payload_orders_read": total_read,
                "events_normalized": total_events,
                "events_written": total_written,
                "dry_run": bool(args.dry_run),
                "state": last_state or {},
                "preview": normalized_preview if args.dry_run else [],
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )
    return 0


def load_payload(args: argparse.Namespace, *, params: dict[str, str], headers: dict[str, str]) -> Any:
    if args.input:
        return _read_payload_file(args.input)
    return _read_payload_url(args.url, params=params, headers=headers, timeout=float(args.timeout))


def request_params_for_poll(args: argparse.Namespace, base_params: dict[str, str]) -> dict[str, str]:
    if not args.state_key:
        return dict(base_params)
    with postgres_connection(readonly=True) as conn:
        state = load_real_order_state_collection_state(conn, args.state_key) or {}
    return build_collection_request_params(
        base_params,
        state,
        since_param=args.since_param,
        cursor_param=args.cursor_param,
        initial_since=args.initial_since,
    )


def update_collection_state(
    args: argparse.Namespace,
    conn: Any,
    payload: Any,
    events: list[dict[str, Any]],
    request_params: dict[str, str],
    written: int,
) -> dict[str, Any]:
    cursor_keys = tuple(args.response_cursor_key or DEFAULT_CURSOR_KEYS)
    next_cursor = extract_response_cursor(payload, cursor_keys)
    watermark = event_time_watermark(events)
    upsert_real_order_state_collection_state(
        conn,
        state_key=args.state_key,
        source=args.source,
        endpoint=args.url or str(args.input),
        params=request_params,
        last_event_time=watermark,
        last_cursor=next_cursor,
        last_payload_count=_payload_count(payload),
        last_events_written=written,
        last_error=None,
    )
    return {
        "state_key": args.state_key,
        "last_event_time": watermark,
        "last_cursor": next_cursor,
        "last_payload_count": _payload_count(payload),
        "last_events_written": written,
    }


def state_preview(args: argparse.Namespace, payload: Any, events: list[dict[str, Any]], request_params: dict[str, str]) -> dict[str, Any]:
    if not args.state_key:
        return {}
    return {
        "state_key": args.state_key,
        "request_params": request_params,
        "last_event_time": event_time_watermark(events),
        "last_cursor": extract_response_cursor(payload, tuple(args.response_cursor_key or DEFAULT_CURSOR_KEYS)),
        "last_payload_count": _payload_count(payload),
        "last_events_written": 0,
    }


def _read_payload_file(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if path.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return json.loads(text)


def _read_payload_url(url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float) -> Any:
    suffix = urlencode(params)
    full_url = f"{url}{'&' if '?' in url else '?'}{suffix}" if suffix else url
    request = Request(full_url, headers={"Accept": "application/json", **headers})
    with urlopen(request, timeout=timeout) as response:
        data = response.read().decode("utf-8")
    return json.loads(data)


def _kv_pairs(items: list[str]) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"expected key=value, got {item!r}")
        key, value = item.split("=", 1)
        pairs[key.strip()] = value.strip()
    return pairs


def _payload_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("orders", "items", "data", "results", "events"):
            if isinstance(payload.get(key), list):
                return len(payload[key])
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
