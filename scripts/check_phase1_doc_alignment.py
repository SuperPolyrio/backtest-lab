#!/usr/bin/env python3
"""Check current code against the local Phase 1 fill-first backtest spec."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.fill_first_readiness import READY  # noqa: E402
from quant.backtest.phase1_doc_alignment import (  # noqa: E402
    build_phase1_doc_alignment_report,
    phase1_doc_alignment_to_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero unless all Phase 1 evidence is present.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_phase1_doc_alignment_report(args.project_root)
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(phase1_doc_alignment_to_markdown(report))
    if report["status"] == READY:
        return 0
    return 2 if args.strict else 0


if __name__ == "__main__":
    raise SystemExit(main())
