#!/usr/bin/env python3
"""Create a local file-mode env for fill-first external evidence sources."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.external_source_env_bootstrap import (  # noqa: E402
    READY,
    bootstrap_external_source_env,
    external_source_env_bootstrap_to_markdown,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=PROJECT_ROOT / ".local" / "fill-first-external-sources",
        help="Directory for local JSONL placeholders and default env-file.",
    )
    parser.add_argument("--env-file", type=Path, default=None, help="Optional env-file path. Defaults under --target-dir.")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--conda-exe", default="/opt/anaconda3/bin/conda")
    parser.add_argument("--conda-env", default="polyBacktest")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite an existing generated env-file.")
    parser.add_argument("--no-create-files", action="store_true", help="Only write env-file; do not create JSONL placeholders.")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero unless bootstrap status is ready.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = bootstrap_external_source_env(
        args.target_dir,
        env_file=args.env_file,
        project_root=args.project_root,
        conda_exe=args.conda_exe,
        conda_env=args.conda_env,
        overwrite=args.overwrite,
        create_files=not args.no_create_files,
    )
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(external_source_env_bootstrap_to_markdown(report))
    if report["status"] == READY:
        return 0
    return 2 if args.strict else 0


if __name__ == "__main__":
    raise SystemExit(main())
