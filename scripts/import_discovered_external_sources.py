#!/usr/bin/env python3
"""Discover real_order_state_events, cost, incident, and external_signal_events files and optionally import them."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_source_discovery import (  # noqa: E402
    default_external_source_roots,
    discover_external_source_files,
    external_source_discovery_to_markdown,
    import_external_source_candidates,
    preview_external_source_import,
)
from quant.core.db import postgres_connection  # noqa: E402
from quant.core.schema import create_schema  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        action="append",
        default=[],
        help="Directory to scan for shadow_live/order_state, cost, incident, and external_signal_events files. Repeatable. Defaults to runtime_outputs, exports, data.",
    )
    parser.add_argument("--max-depth", type=int, default=5, help="Maximum directory depth under each root.")
    parser.add_argument("--max-files-per-kind", type=int, default=50, help="Maximum files to process per source kind.")
    parser.add_argument("--source-prefix", default="local-discovery", help="Source prefix stored on imported rows.")
    parser.add_argument("--state-prefix", default="local-discovery", help="State-key prefix for import freshness rows.")
    parser.add_argument("--max-preview-rows", type=int, default=5, help="Preview rows per discovered file.")
    parser.add_argument("--write", action="store_true", help="Write discovered rows to Postgres. Default is preview only.")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero when no source files are discovered.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not initialize quant schema before write.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    roots = args.root or default_external_source_roots(PROJECT_ROOT)
    candidates = discover_external_source_files(
        roots,
        max_depth=max(0, int(args.max_depth)),
        max_files_per_kind=max(1, int(args.max_files_per_kind)),
    )
    if args.write:
        with postgres_connection() as conn:
            if not args.skip_init_schema:
                create_schema(conn)
            report = import_external_source_candidates(
                conn,
                candidates,
                source_prefix=args.source_prefix,
                state_prefix=args.state_prefix,
                base=PROJECT_ROOT,
                max_preview_rows=max(0, int(args.max_preview_rows)),
            )
    else:
        report = preview_external_source_import(
            candidates,
            source_prefix=args.source_prefix,
            base=PROJECT_ROOT,
            max_preview_rows=max(0, int(args.max_preview_rows)),
        )
    report["roots"] = [str(path) for path in roots]
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(external_source_discovery_to_markdown(report))
    if args.strict and report.get("status") == "unknown":
        return 2
    if report.get("status") == "review":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
