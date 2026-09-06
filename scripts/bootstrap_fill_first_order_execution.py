#!/usr/bin/env python3
"""Create a local paper-only ORDER_EXECUTION env for fill-first dry-runs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.order_execution_env_bootstrap import (  # noqa: E402
    READY,
    bootstrap_order_execution_env,
    order_execution_env_bootstrap_to_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=PROJECT_ROOT / ".local" / "fill-first-order-execution",
        help="Directory for the generated paper-only env-file.",
    )
    parser.add_argument("--env-file", type=Path, default=None, help="Optional env-file path. Defaults under --target-dir.")
    parser.add_argument("--submit-url", default="http://127.0.0.1:9/fill-first-paper-submit")
    parser.add_argument("--cancel-url", default="")
    parser.add_argument("--source", default="paper-dry-run-local")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite an existing generated env-file.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero unless bootstrap status is ready.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = bootstrap_order_execution_env(
        args.target_dir,
        env_file=args.env_file,
        submit_url=args.submit_url,
        cancel_url=args.cancel_url,
        source=args.source,
        overwrite=args.overwrite,
    )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(order_execution_env_bootstrap_to_markdown(report))
    if report["status"] == READY:
        return 0
    return 2 if args.strict else 0


if __name__ == "__main__":
    raise SystemExit(main())
