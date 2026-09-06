#!/usr/bin/env python3
"""Print a production onboarding plan for fill-first external sources."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_source_onboarding import (  # noqa: E402
    READY,
    build_external_source_onboarding_plan,
    external_source_onboarding_plan_to_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", action="append", type=Path, default=[], help="Private env-file to load. May be repeated.")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict-review", action="store_true", help="Return non-zero when onboarding status is not ready.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_external_source_onboarding_plan(args.env_file, project_root=args.project_root)
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(external_source_onboarding_plan_to_markdown(report))
    return 2 if args.strict_review and report.get("status") != READY else 0


if __name__ == "__main__":
    raise SystemExit(main())
