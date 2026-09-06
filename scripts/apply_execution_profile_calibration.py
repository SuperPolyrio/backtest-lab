#!/usr/bin/env python3
"""Persist reviewed execution profile calibration suggestions as overrides."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.execution_profile_overrides import (  # noqa: E402
    normalize_execution_profile_override,
    upsert_execution_profile_overrides,
)
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path, help="JSON suggestions list, or a calibration report containing execution_profile_suggestions")
    parser.add_argument("--source", default="calibration-suggestion", help="Source label stored on overrides.")
    parser.add_argument("--approve", action="store_true", help="Store suggestions as approved overrides instead of pending review.")
    parser.add_argument("--approved-by", default=None, help="Reviewer name or operator id. Required with --approve.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print normalized overrides without writing.")
    return parser.parse_args()


def load_suggestions(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        if isinstance(payload.get("execution_profile_suggestions"), list):
            payload = payload["execution_profile_suggestions"]
        elif isinstance(payload.get("items"), list):
            payload = payload["items"]
        elif isinstance(payload.get("suggestions"), list):
            payload = payload["suggestions"]
    if not isinstance(payload, list):
        raise ValueError("input must be a JSON list, an object with items/suggestions, or a calibration report with execution_profile_suggestions")
    return [dict(item) for item in payload if isinstance(item, dict)]


def main() -> int:
    args = parse_args()
    if args.approve and not args.approved_by:
        raise SystemExit("--approved-by is required when --approve is used")
    status = "approved" if args.approve else "pending"
    suggestions = load_suggestions(args.input)
    normalized = [
        normalize_execution_profile_override(
            suggestion,
            status=status,
            source=args.source,
            approved_by=args.approved_by,
        )
        for suggestion in suggestions
    ]
    if args.dry_run:
        print(json.dumps(normalized, ensure_ascii=False, indent=2, default=str))
        return 0
    with postgres_connection(readonly=False) as conn:
        create_schema(conn)
        written = upsert_execution_profile_overrides(
            conn,
            suggestions,
            status=status,
            source=args.source,
            approved_by=args.approved_by,
        )
        conn.commit()
    print(json.dumps({"written": written, "status": status}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
