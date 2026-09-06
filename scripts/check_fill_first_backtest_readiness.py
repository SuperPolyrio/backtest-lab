#!/usr/bin/env python3
"""Check fill-first backtest stack completeness."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_first_readiness import (  # noqa: E402
    MISSING,
    READY,
    REVIEW,
    build_fill_first_readiness_report,
    readiness_to_markdown,
)
from quant.core.db import postgres_connection  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT, help="Project root to inspect.")
    parser.add_argument("--check-db", action="store_true", help="Also verify fill-first Postgres tables exist.")
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero for review items as well as missing items.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.check_db:
        with postgres_connection(readonly=True) as conn:
            report = build_fill_first_readiness_report(args.project_root, check_db=True, conn=conn)
    else:
        report = build_fill_first_readiness_report(args.project_root, check_db=False)

    if args.format == "markdown":
        print(readiness_to_markdown(report))
    else:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))

    if report["status"] == READY:
        return 0
    if report["status"] == REVIEW:
        return 1 if args.strict else 0
    if report["status"] == MISSING:
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
