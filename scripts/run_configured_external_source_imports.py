#!/usr/bin/env python3
"""Run configured fill-first external source imports from environment variables.

Reads ORDER_STATE_API_URL / ORDER_STATE_INPUT, COST_EVENTS_INPUT / COST_EVENTS_URL,
PLATFORM_INCIDENTS_INPUT / PLATFORM_INCIDENTS_URL, and
EXTERNAL_SIGNAL_INPUT / EXTERNAL_SIGNAL_URL from the process environment or
one or more --env-file files.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.configured_external_sources import (  # noqa: E402
    FAIL,
    READY,
    build_configured_external_source_report,
    configured_external_source_report_to_markdown,
    load_external_source_env_files,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--env-file",
        action="append",
        default=[],
        type=Path,
        help="Load dotenv/systemd-style KEY=VALUE settings before building the import plan. Can be repeated.",
    )
    parser.add_argument("--write", action="store_true", help="Execute imports. Default is dry-run plan only.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Pass --skip-init-schema to import scripts.")
    parser.add_argument("--timeout", type=int, default=300, help="Per-source command timeout seconds.")
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero when no source is configured.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    env = load_external_source_env_files(args.env_file) if args.env_file else None
    report = build_configured_external_source_report(
        env,
        project_root=args.project_root,
        dry_run=not args.write,
        skip_init_schema=args.skip_init_schema,
        command_timeout_seconds=max(1, int(args.timeout)),
    )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(configured_external_source_report_to_markdown(report))
    if report["status"] == READY:
        return 0
    if report["status"] == FAIL:
        return 2
    return 1 if args.strict_review else 0


if __name__ == "__main__":
    raise SystemExit(main())
