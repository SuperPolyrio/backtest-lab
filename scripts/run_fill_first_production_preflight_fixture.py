#!/usr/bin/env python3
"""Run a local fill-first production preflight fixture without external API calls."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.production_preflight_fixture import (  # noqa: E402
    FAIL,
    READY,
    production_preflight_fixture_to_markdown,
    run_fill_first_production_preflight_fixture,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "runtime_outputs" / "fill_first_production_preflight_fixture",
        help="Directory to write local fixture evidence and env-file.",
    )
    parser.add_argument("--run-id", type=int, default=9101)
    parser.add_argument("--source-prefix", default="preflight-fixture")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = run_fill_first_production_preflight_fixture(
        args.output_dir,
        project_root=PROJECT_ROOT,
        run_id=args.run_id,
        source_prefix=args.source_prefix,
        overwrite=args.overwrite,
    )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(production_preflight_fixture_to_markdown(report))
    if report["status"] == READY:
        return 0
    if report["status"] == FAIL:
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
