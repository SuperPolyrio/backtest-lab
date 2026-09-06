#!/usr/bin/env python3
"""Approve, reject, or archive a staged fill-first production parameter row."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.production_parameter_staging import (  # noqa: E402
    update_production_parameter_staging_status,
    validate_production_parameter_staging_status_update,
)
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staging-id", type=int, help="quant.production_parameter_staging.staging_id to review.")
    parser.add_argument("--status", required=True, choices=("approved", "rejected", "archived", "pending"))
    parser.add_argument("--reviewed-by", default=None, help="Reviewer/operator id. Required for approved/rejected/archived.")
    parser.add_argument("--review-note", default=None)
    parser.add_argument("--dry-run-row-json", type=Path, help="Validate a row JSON without connecting to the database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.dry_run_row_json:
        row = json.loads(args.dry_run_row_json.read_text(encoding="utf-8"))
        if not isinstance(row, dict):
            raise SystemExit("--dry-run-row-json must contain a JSON object")
        result = validate_production_parameter_staging_status_update(
            row,
            status=args.status,
            reviewed_by=args.reviewed_by,
            review_note=args.review_note,
        )
        payload = {"dry_run": True, "valid": True, "update": result}
    else:
        if args.staging_id is None:
            raise SystemExit("--staging-id is required unless --dry-run-row-json is supplied")
        with postgres_connection(readonly=False) as conn:
            create_schema(conn)
            row = update_production_parameter_staging_status(
                conn,
                staging_id=args.staging_id,
                status=args.status,
                reviewed_by=args.reviewed_by,
                review_note=args.review_note,
            )
            conn.commit()
        payload = {"dry_run": False, "item": row, "staging_id": row.get("staging_id"), "status": row.get("status")}
    if args.format == "text":
        print(_text_summary(payload))
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
    return 0


def _text_summary(payload: dict[str, Any]) -> str:
    if payload.get("dry_run"):
        update = payload.get("update") or {}
        return f"valid dry-run update: status={update.get('status')} reviewed_by={update.get('reviewed_by') or '-'}"
    item = payload.get("item") or {}
    return f"updated staging_id={payload.get('staging_id')} status={payload.get('status')} reviewed_by={item.get('reviewed_by') or '-'}"


def _json_default(value: Any) -> str:
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
