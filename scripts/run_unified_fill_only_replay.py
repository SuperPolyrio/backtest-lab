#!/usr/bin/env python3
"""Run V2/V3 frozen orders against shared Parquet inputs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from time import perf_counter

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.frozen_order_catalog import FrozenOrderCatalog  # noqa: E402
from quant.backtest.profile_worker import (  # noqa: E402
    ProfileWorkerTask,
    run_profile_workers,
)
from quant.backtest.trade_catalog import ParquetTradeCatalog  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Unified no-LOB Fill-only V2/V3 Parquet replay runner"
    )
    parser.add_argument("--family", choices=("V2", "V3"), required=True)
    parser.add_argument("--trade-catalog", type=Path, required=True)
    parser.add_argument("--order-catalog", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--profiles", required=True, help="Comma-separated profile names")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument(
        "--matcher-backend",
        choices=("auto", "python", "rust"),
        default="auto",
    )
    parser.add_argument("--audit", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--max-orders", type=int, default=None)
    parser.add_argument("--skip-orders", type=int, default=0)
    parser.add_argument(
        "--evict-os-cache",
        action="store_true",
        help="Advise Linux to evict catalog Parquet pages before timed replay",
    )
    parser.add_argument(
        "--chunk-mode",
        choices=("rows", "signal_day"),
        default="signal_day",
    )
    parser.add_argument("--chunk-days", type=int, default=1)
    parser.add_argument(
        "--recycle-batches",
        type=int,
        default=5,
        help="Restart each worker after this many committed chunks; 0 disables recycling",
    )
    args = parser.parse_args()

    # run_profile_workers performs one parent-side checksum verification before
    # spawning workers; this initial open only reads immutable metadata.
    trade_catalog = ParquetTradeCatalog(args.trade_catalog, verify_checksums=False)
    order_catalog = FrozenOrderCatalog(
        args.order_catalog,
        source_pin=trade_catalog.source_pin,
        verify_checksums=False,
    )
    if order_catalog.execution_family != args.family:
        parser.error("--family does not match the frozen order catalog")
    profiles = tuple(
        item.strip() for item in str(args.profiles).split(",") if item.strip()
    )
    if not profiles:
        parser.error("--profiles must contain at least one profile")
    if args.chunk_days <= 0:
        parser.error("--chunk-days must be positive")
    if args.skip_orders < 0:
        parser.error("--skip-orders cannot be negative")
    if args.recycle_batches < 0:
        parser.error("--recycle-batches cannot be negative")
    cache_eviction = (
        _evict_catalog_cache(args.trade_catalog, args.order_catalog)
        if args.evict_os_cache
        else {
            "requested": False,
            "supported": hasattr(os, "posix_fadvise"),
            "files": 0,
            "bytes": 0,
            "errors": [],
        }
    )
    args.output.mkdir(parents=True, exist_ok=args.resume)
    tasks = [
        ProfileWorkerTask(
            execution_family=args.family,
            profile=profile,
            trade_catalog_path=str(args.trade_catalog.resolve()),
            order_catalog_path=str(args.order_catalog.resolve()),
            result_path=str((args.output / profile).resolve()),
            run_id=args.run_id,
            matcher_backend=args.matcher_backend,
            batch_size=max(1, args.batch_size),
            writer_mode="audit" if args.audit else "research",
            resume=args.resume,
            max_batches=args.max_batches,
            max_orders=args.max_orders,
            skip_orders=args.skip_orders,
            chunk_mode=args.chunk_mode,
            chunk_days=args.chunk_days,
            recycle_batches=args.recycle_batches or None,
        )
        for profile in profiles
    ]
    started = perf_counter()
    receipts = run_profile_workers(tasks, max_workers=max(1, args.workers))
    payload = {
        "schema_version": "UnifiedFillOnlyLongRunReceiptV1",
        "run_id": args.run_id,
        "execution_family": args.family,
        "source_pin": trade_catalog.source_pin,
        "trade_catalog_manifest_sha256": trade_catalog.manifest.manifest_sha256,
        "strategy_hash": order_catalog.strategy_hash,
        "catalog_orders": order_catalog.rows,
        "orders": max((receipt.orders for receipt in receipts), default=0),
        "profiles": list(profiles),
        "workers": max(1, args.workers),
        "batch_size": max(1, args.batch_size),
        "matcher_backend": args.matcher_backend,
        "max_orders": args.max_orders,
        "skip_orders": args.skip_orders,
        "chunk_mode": args.chunk_mode,
        "chunk_days": args.chunk_days,
        "recycle_batches": args.recycle_batches or None,
        "os_cache_eviction": cache_eviction,
        "warm_catalog_clickhouse_queries": sum(
            receipt.clickhouse_query_count for receipt in receipts
        ),
        "elapsed_seconds": perf_counter() - started,
        "peak_worker_rss_kb": max(
            (receipt.peak_rss_kb for receipt in receipts), default=0
        ),
        "complete": all(receipt.complete for receipt in receipts),
        "receipts": [receipt.as_dict() for receipt in receipts],
    }
    temporary = args.output / "run_receipt.json.tmp"
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, args.output / "run_receipt.json")
    print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
    return 0 if payload["complete"] else 2


def _evict_catalog_cache(*roots: Path) -> dict[str, object]:
    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED"):
        raise RuntimeError("--evict-os-cache requires Linux posix_fadvise")
    files = 0
    bytes_advised = 0
    errors: list[str] = []
    for root in roots:
        for path in sorted(Path(root).resolve().rglob("*.parquet")):
            try:
                descriptor = os.open(path, os.O_RDONLY)
                try:
                    size = path.stat().st_size
                    os.posix_fadvise(
                        descriptor,
                        0,
                        0,
                        os.POSIX_FADV_DONTNEED,
                    )
                finally:
                    os.close(descriptor)
                files += 1
                bytes_advised += size
            except OSError as exc:
                errors.append(f"{path}:{exc.errno}")
    if errors:
        raise RuntimeError(
            f"OS cache eviction failed for {len(errors)} catalog files: {errors[:3]}"
        )
    return {
        "requested": True,
        "supported": True,
        "files": files,
        "bytes": bytes_advised,
        "errors": errors,
    }


if __name__ == "__main__":
    raise SystemExit(main())
