#!/usr/bin/env python3
"""Run the guarded order execution fixture pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.order_execution_fixture import (  # noqa: E402
    READY,
    order_execution_fixture_pipeline_to_markdown,
    run_order_execution_fixture_pipeline,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero unless the fixture is ready.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_order_execution_fixture_pipeline()
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(order_execution_fixture_pipeline_to_markdown(report))
    if report["status"] == READY:
        return 0
    return 1 if args.strict_review else 0


if __name__ == "__main__":
    raise SystemExit(main())
