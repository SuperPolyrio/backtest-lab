#!/usr/bin/env python3
"""Audit fill-first external source env-files before running imports."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_source_env_audit import (  # noqa: E402
    FAIL,
    READY,
    REVIEW,
    build_external_source_env_audit,
    external_source_env_audit_to_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--env-file",
        action="append",
        default=[],
        type=Path,
        help="Load dotenv/systemd-style KEY=VALUE settings before auditing. Can be repeated.",
    )
    parser.add_argument("--strict-review", action="store_true", help="Exit non-zero for review findings.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_external_source_env_audit(args.env_file, project_root=args.project_root)
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(external_source_env_audit_to_markdown(report))
    if report["status"] == READY:
        return 0
    if report["status"] == FAIL:
        return 2
    return 1 if args.strict_review else 0


if __name__ == "__main__":
    raise SystemExit(main())
