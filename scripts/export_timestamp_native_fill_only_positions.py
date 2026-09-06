#!/usr/bin/env python3
"""Export any completed timestamp-native Fill-only profile for settlement."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant.backtest.timestamp_native_position_adapter import (
    export_timestamp_native_execution_positions,
)
from quant.core.db import PostgresSettings, postgres_connection


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-root", required=True, type=Path)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    with postgres_connection(PostgresSettings(), readonly=True) as conn:
        manifest = export_timestamp_native_execution_positions(
            conn,
            execution_root=args.execution_root,
            profile=args.profile,
            output_dir=args.output_dir,
        )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
