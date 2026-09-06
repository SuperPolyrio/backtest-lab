#!/usr/bin/env python3
"""Audit ORDER_EXECUTION_* settings before enabling paper/live execution."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.configured_external_sources import load_external_source_env_files  # noqa: E402
from quant.backtest.order_execution_safety import (  # noqa: E402
    READY,
    build_order_execution_env_audit,
    order_execution_env_audit_to_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, action="append", default=[], help="Load KEY=VALUE settings before auditing.")
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero for review findings.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env = load_external_source_env_files(args.env_file) if args.env_file else None
    report = build_order_execution_env_audit(env)
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(order_execution_env_audit_to_markdown(report))
    if report["status"] == READY:
        return 0
    return 1 if args.strict_review else 0


if __name__ == "__main__":
    raise SystemExit(main())
