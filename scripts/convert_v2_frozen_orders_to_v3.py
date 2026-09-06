#!/usr/bin/env python3
"""Convert one immutable V2 order inventory into V3 replay templates."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.frozen_order_catalog import (  # noqa: E402
    convert_v2_frozen_order_catalog_to_v3,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a timestamp-native V3 frozen-order catalog from V2 orders"
    )
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--row-group-size", type=int, default=10_000)
    args = parser.parse_args()
    if args.row_group_size <= 0:
        parser.error("--row-group-size must be positive")

    payload = convert_v2_frozen_order_catalog_to_v3(
        args.source,
        args.output,
        row_group_size=args.row_group_size,
    )
    receipt_path = args.output / "conversion_receipt.json"
    temporary = receipt_path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, receipt_path)
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
