#!/usr/bin/env python3
"""Import canonical external signal events for strategy input auditing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_signals import (  # noqa: E402
    build_external_signal_import_report,
    normalize_external_signal_event,
    upsert_external_signal_events,
)
from quant.backtest.external_source_state import upsert_external_source_import_state  # noqa: E402
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=None, help="JSON or JSONL external signal file.")
    parser.add_argument("--url", default=None, help="HTTP endpoint returning external signal events.")
    parser.add_argument("--param", action="append", default=[], help="Query param as key=value; repeatable.")
    parser.add_argument("--header", action="append", default=[], help="HTTP header as Key=Value; repeatable.")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds.")
    parser.add_argument("--source", default=None, help="Override source for all rows.")
    parser.add_argument("--run-id", type=int, default=None, help="Attach all rows to this backtest run_id.")
    parser.add_argument("--state-key", default=None, help="Persist import freshness under this key.")
    parser.add_argument("--dry-run", action="store_true", help="Print normalized events/report; do not write Postgres.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not run quant schema initialization before import.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.input and not args.url:
        raise SystemExit("provide --input or --url")
    source_endpoint = str(args.input) if args.input else str(args.url)
    request_params = _kv_pairs(args.param)
    headers = _kv_pairs(args.header)
    raw_events = load_events(args.input, url=args.url, params=request_params, headers=headers, timeout=float(args.timeout))
    events = [
        normalize_external_signal_event(event, source=args.source, run_id=args.run_id)
        for event in raw_events
    ]
    report = build_external_signal_import_report(events)
    written = 0
    if not args.dry_run:
        with postgres_connection() as conn:
            if not args.skip_init_schema:
                create_schema(conn)
            written = upsert_external_signal_events(conn, events)
            if args.state_key:
                upsert_external_source_import_state(
                    conn,
                    state_key=args.state_key,
                    source_type="external_signal_events",
                    source=args.source or "manual",
                    endpoint=source_endpoint,
                    params=request_params,
                    last_payload_count=len(raw_events),
                    last_rows_written=written,
                    last_error=None,
                )
    print(json.dumps({
        "input": str(args.input) if args.input else None,
        "url": args.url,
        "state_key": args.state_key,
        "source_override": args.source,
        "run_id_override": args.run_id,
        "dry_run": bool(args.dry_run),
        "events_read": len(raw_events),
        "events_written": written,
        "report": report,
        "preview": events[:20] if args.dry_run else [],
    }, ensure_ascii=False, indent=2, default=str))
    return 0


def load_events(
    path: Path | None = None,
    *,
    url: str | None = None,
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15.0,
) -> list[dict[str, Any]]:
    if url:
        parsed = _read_payload_url(url, params=params or {}, headers=headers or {}, timeout=timeout)
        return _events_from_payload(parsed, source=str(url))
    if path is None:
        raise ValueError("provide path or url")
    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if path.suffix.lower() == ".jsonl":
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return _events_from_payload(json.loads(text), source=str(path))


def _events_from_payload(parsed: Any, *, source: str) -> list[dict[str, Any]]:
    if isinstance(parsed, list):
        return [dict(item) for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict) and isinstance(parsed.get("events"), list):
        return [dict(item) for item in parsed["events"] if isinstance(item, dict)]
    if isinstance(parsed, dict) and isinstance(parsed.get("signals"), list):
        return [dict(item) for item in parsed["signals"] if isinstance(item, dict)]
    if isinstance(parsed, dict) and isinstance(parsed.get("items"), list):
        return [dict(item) for item in parsed["items"] if isinstance(item, dict)]
    if isinstance(parsed, dict) and isinstance(parsed.get("data"), list):
        return [dict(item) for item in parsed["data"] if isinstance(item, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    raise ValueError(f"unsupported input JSON shape in {source}")


def _read_payload_url(url: str, *, params: dict[str, str], headers: dict[str, str], timeout: float) -> Any:
    suffix = urlencode(params)
    full_url = f"{url}{'&' if '?' in url else '?'}{suffix}" if suffix else url
    request = Request(full_url, headers={"Accept": "application/json", **headers})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _kv_pairs(items: list[str]) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"expected key=value, got {item!r}")
        key, value = item.split("=", 1)
        pairs[key.strip()] = value.strip()
    return pairs


if __name__ == "__main__":
    raise SystemExit(main())
