#!/usr/bin/env python3
"""Import backtest-vs-live fill calibration samples."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.calibration import build_calibration_report, normalize_calibration_order, upsert_calibration_orders
from quant.core.db import postgres_connection
from quant.core.schema import create_schema


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="JSON or JSONL calibration sample file")
    parser.add_argument("--source", default=None, help="Override source for all samples, for example live-shadow")
    parser.add_argument("--run-id", type=int, default=None, help="Attach all samples to this backtest run_id")
    parser.add_argument("--dry-run", action="store_true", help="Only print the report; do not write Postgres")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not run quant schema initialization before import")
    return parser.parse_args()


def load_rows(path: Path) -> list[dict[str, Any]]:
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
    if isinstance(parsed, dict) and isinstance(parsed.get("samples"), list):
        return [dict(item) for item in parsed["samples"] if isinstance(item, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    raise ValueError(f"unsupported input JSON shape in {path}")


def main() -> int:
    args = parse_args()
    raw_rows = load_rows(args.input)
    rows = [
        normalize_calibration_order(row, source=args.source, run_id=args.run_id)
        for row in raw_rows
    ]
    report = build_calibration_report(rows)
    written = 0
    if not args.dry_run:
        with postgres_connection() as conn:
            if not args.skip_init_schema:
                create_schema(conn)
            written = upsert_calibration_orders(conn, rows)
    print(
        json.dumps(
            {
                "input": str(args.input),
                "source_override": args.source,
                "run_id_override": args.run_id,
                "dry_run": bool(args.dry_run),
                "samples_read": len(raw_rows),
                "samples_written": written,
                "report": report,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
