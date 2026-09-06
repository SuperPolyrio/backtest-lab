#!/usr/bin/env python3
"""Finalize any frozen Fill-only position inventory against a frozen catalog."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant.backtest.fill_only_financials import (
    FINANCIAL_EVIDENCE_REQUIREMENTS,
    finalize_fill_only_positions,
    load_fill_only_execution_position_market_ids,
    load_fill_only_execution_positions,
    write_fill_only_financial_bundle,
)
from quant.backtest.market_settlement import (
    load_market_settlement_catalog,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-positions", required=True, type=Path)
    parser.add_argument("--settlement-catalog", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--required-evidence-grade",
        choices=sorted(FINANCIAL_EVIDENCE_REQUIREMENTS),
        default="oracle-finalized",
        help=(
            "Normal backtests require final Oracle evidence, settlement block, "
            "credible settlement time, and fill-before-settlement ordering. "
            "exact-header is an optional stronger audit; "
            "oracle-event-cutoff-block is legacy reproduction."
        ),
    )
    parser.add_argument(
        "--verify-all-catalog-files",
        action="store_true",
        help=(
            "Hash every catalog partition. By default only the root manifest and "
            "partitions containing this position inventory are verified."
        ),
    )
    args = parser.parse_args()

    inventory_manifest, market_ids = load_fill_only_execution_position_market_ids(
        args.execution_positions
    )
    catalog_manifest = json.loads(
        (args.settlement_catalog / "manifest.json").read_text(encoding="utf-8")
    )
    settlements = load_market_settlement_catalog(
        args.settlement_catalog,
        market_ids=market_ids,
        verify_all_files=args.verify_all_catalog_files,
    )
    execution_manifest, positions = load_fill_only_execution_positions(
        args.execution_positions
    )
    if execution_manifest != inventory_manifest:
        raise SystemExit("execution-position manifest changed during finalization")
    cutoff = datetime.fromisoformat(str(catalog_manifest["cutoff_ts"]))
    missing = sorted(set(market_ids) - set(settlements))
    if missing:
        raise SystemExit(
            f"settlement catalog is missing {len(missing)} execution markets"
        )
    summary, rows = finalize_fill_only_positions(
        positions,
        settlements,
        execution_manifest_sha256=str(
            execution_manifest["execution_manifest_sha256"]
        ),
        settlement_catalog_sha256=str(catalog_manifest["catalog_sha256"]),
        cutoff_ts=cutoff,
        required_evidence_grade=args.required_evidence_grade,
    )
    manifest = write_fill_only_financial_bundle(
        output_dir=args.output_dir,
        summary=summary,
        positions=rows,
    )
    print(
        json.dumps(
            {
                "status": summary["status"],
                "summary": summary,
                "bundle": manifest,
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
