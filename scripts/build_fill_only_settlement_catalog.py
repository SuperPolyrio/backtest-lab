#!/usr/bin/env python3
"""Build a reusable as-of-cutoff settlement catalog for any market universe."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant.backtest.fill_only_financials import (
    load_fill_only_execution_position_market_ids,
)
from quant.backtest.market_settlement import (
    EXACT_HEADER_REQUIRED,
    ORACLE_EVENT_CUTOFF_BLOCK,
    ORACLE_FINALIZED_REQUIRED,
    file_sha256,
    finalize_partitioned_market_settlement_catalog,
    initialize_partitioned_market_settlement_catalog,
    iter_market_settlement_catalog,
    resolve_polygon_cutoff_boundary,
    write_market_settlement_catalog,
    write_market_settlement_catalog_part,
)
from quant.core.db import (
    ClickHouseClient,
    PostgresSettings,
    postgres_connection,
)

UTC = timezone.utc
TRADED_MARKET_INVENTORY_SCHEMA_VERSION = "fill_only_traded_market_inventory_v1"


def _timestamp(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _market_ids_from_json(path: Path) -> tuple[int, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    values = payload.get("market_ids") if isinstance(payload, dict) else payload
    if not isinstance(values, list):
        raise TypeError("market-id JSON must be a list or contain market_ids")
    return tuple(sorted({int(value) for value in values}))


def _traded_market_ids(
    clickhouse: ClickHouseClient,
    *,
    from_ts: datetime,
    to_ts: datetime,
    limit: int | None,
) -> tuple[int, ...]:
    start = from_ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    end = to_ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    limit_clause = "" if limit is None else f"LIMIT {int(limit)}"
    query = f"""
        SELECT DISTINCT toUInt64(market_id) AS market_id
        FROM poly_orderfilled.trade_prints_one_sided
        WHERE block_time >= toDateTime('{start}', 'UTC')
          AND block_time < toDateTime('{end}', 'UTC')
        ORDER BY market_id
        {limit_clause}
    """
    return tuple(
        int(row["market_id"])
        for row in clickhouse.query_json_rows(query, timeout_seconds=900)
    )


def _chunks(values: tuple[int, ...], size: int):
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _market_ids_sha256(values: tuple[int, ...]) -> str:
    encoded = json.dumps(values, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def _manifest_sha256(payload: dict[str, object]) -> str:
    return sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _traded_inventory_query_scope(args: argparse.Namespace) -> dict[str, object]:
    assert args.trade_from_ts is not None
    assert args.trade_to_ts is not None
    return {
        "trade_from_ts": args.trade_from_ts.isoformat(),
        "trade_to_ts": args.trade_to_ts.isoformat(),
        "limit": args.limit,
        "source_table": "poly_orderfilled.trade_prints_one_sided",
    }


def _write_traded_market_inventory(
    output_dir: Path,
    *,
    market_ids: tuple[int, ...],
    query_scope: dict[str, object],
) -> dict[str, object]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    root = output_dir.resolve()
    parquet_path = root / "market_inventory.parquet"
    manifest_path = root / "market_inventory_manifest.json"
    if parquet_path.exists() or manifest_path.exists():
        raise FileExistsError("traded-market inventory artifact already exists")
    temporary = parquet_path.with_suffix(".parquet.tmp")
    pq.write_table(
        pa.table({"market_id": pa.array(market_ids, type=pa.int64())}),
        temporary,
        compression="zstd",
        row_group_size=100_000,
    )
    temporary.replace(parquet_path)
    manifest_without_hash = {
        "schema_version": TRADED_MARKET_INVENTORY_SCHEMA_VERSION,
        "query_scope": query_scope,
        "market_count": len(market_ids),
        "market_ids_sha256": _market_ids_sha256(market_ids),
        "parquet_file": parquet_path.name,
        "parquet_file_sha256": file_sha256(parquet_path),
    }
    manifest = {
        **manifest_without_hash,
        "inventory_manifest_sha256": _manifest_sha256(manifest_without_hash),
    }
    temporary_manifest = manifest_path.with_suffix(".json.tmp")
    temporary_manifest.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    temporary_manifest.replace(manifest_path)
    return manifest


def _load_traded_market_inventory(
    output_dir: Path,
    *,
    expected_query_scope: dict[str, object],
) -> tuple[tuple[int, ...], dict[str, object]]:
    import pyarrow.parquet as pq

    root = output_dir.resolve()
    manifest_path = root / "market_inventory_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError("resumed catalog lacks market inventory manifest")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != TRADED_MARKET_INVENTORY_SCHEMA_VERSION:
        raise RuntimeError("unsupported traded-market inventory schema")
    stored_hash = manifest.get("inventory_manifest_sha256")
    if _manifest_sha256(
        {
            key: value
            for key, value in manifest.items()
            if key != "inventory_manifest_sha256"
        }
    ) != stored_hash:
        raise RuntimeError("traded-market inventory manifest drifted")
    if manifest.get("query_scope") != expected_query_scope:
        raise RuntimeError("traded-market inventory query scope drifted")
    parquet_path = root / str(manifest["parquet_file"])
    if file_sha256(parquet_path) != manifest.get("parquet_file_sha256"):
        raise RuntimeError("traded-market inventory Parquet drifted")
    values = tuple(
        int(value)
        for batch in pq.ParquetFile(parquet_path).iter_batches(
            batch_size=100_000, columns=["market_id"]
        )
        for value in batch.column(0).to_pylist()
    )
    if values != tuple(sorted(set(values))):
        raise RuntimeError("traded-market inventory is not sorted and unique")
    if len(values) != int(manifest["market_count"]):
        raise RuntimeError("traded-market inventory count drifted")
    if _market_ids_sha256(values) != manifest["market_ids_sha256"]:
        raise RuntimeError("traded-market inventory identity drifted")
    return values, manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--execution-positions", type=Path)
    source.add_argument("--market-ids-json", type=Path)
    source.add_argument("--all-traded-markets", action="store_true")
    parser.add_argument("--trade-from-ts", type=_timestamp)
    parser.add_argument("--trade-to-ts", type=_timestamp)
    parser.add_argument("--cutoff-ts", required=True, type=_timestamp)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--market-chunk-size", type=int, default=2_000)
    parser.add_argument("--block-chunk-size", type=int, default=5_000)
    parser.add_argument("--rpc-batch-size", type=int, default=100)
    parser.add_argument("--rpc-workers", type=int, default=4)
    parser.add_argument("--rpc-retry-count", type=int, default=3)
    parser.add_argument(
        "--evidence-policy",
        choices=(
            "oracle-finalized",
            "exact-header",
            "oracle-event-cutoff-block",
        ),
        default="oracle-finalized",
        help=(
            "oracle-finalized is the normal backtest policy and requires final "
            "Oracle identity, settlement transaction/block, credible settlement "
            "time, and a frozen cutoff boundary. exact-header adds per-block RPC "
            "header verification. oracle-event-cutoff-block is legacy reproduction "
            "which may accept block-only settlement."
        ),
    )
    parser.add_argument(
        "--partitioned",
        action="store_true",
        help="Write resumable Parquet parts instead of one monolithic file.",
    )
    parser.add_argument("--partition-size", type=int, default=10_000)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reuse verified completed parts under the identical frozen contract.",
    )
    parser.add_argument(
        "--polygon-rpc-url",
        default=(
            os.environ.get("POLY_QUANT_SETTLEMENT_RPC_URL")
            or os.environ.get("POLYMARKET_POLYGON_RPC_URL")
            or os.environ.get("POLYGON_RPC_URL")
            or "http://127.0.0.1:28545"
        ),
    )
    args = parser.parse_args()
    for field in (
        "market_chunk_size",
        "block_chunk_size",
        "rpc_batch_size",
        "rpc_workers",
        "rpc_retry_count",
        "partition_size",
    ):
        if int(getattr(args, field)) <= 0:
            parser.error(f"--{field.replace('_', '-')} must be positive")

    clickhouse = ClickHouseClient()
    evidence_policy = {
        "oracle-finalized": ORACLE_FINALIZED_REQUIRED,
        "exact-header": EXACT_HEADER_REQUIRED,
        "oracle-event-cutoff-block": ORACLE_EVENT_CUTOFF_BLOCK,
    }[args.evidence_policy]
    cutoff_boundary = (
        None
        if evidence_policy == EXACT_HEADER_REQUIRED
        else resolve_polygon_cutoff_boundary(
            args.polygon_rpc_url,
            cutoff_ts=args.cutoff_ts,
            retry_count=args.rpc_retry_count,
            clickhouse=clickhouse,
        )
    )
    traded_inventory_manifest: dict[str, object] | None = None
    if args.execution_positions is not None:
        position_manifest, market_ids = load_fill_only_execution_position_market_ids(
            args.execution_positions
        )
        source_scope = {
            "mode": "EXECUTION_POSITION_MARKET_UNION",
            "execution_manifest_sha256": position_manifest[
                "execution_manifest_sha256"
            ],
        }
    elif args.market_ids_json is not None:
        market_ids = _market_ids_from_json(args.market_ids_json)
        source_scope = {
            "mode": "EXPLICIT_MARKET_ID_INVENTORY",
            "source_file": str(args.market_ids_json.resolve()),
        }
    else:
        if args.trade_from_ts is None or args.trade_to_ts is None:
            parser.error("--all-traded-markets requires --trade-from-ts and --trade-to-ts")
        if args.trade_from_ts >= args.trade_to_ts:
            parser.error("trade-from-ts must precede trade-to-ts")
        inventory_scope = _traded_inventory_query_scope(args)
        inventory_manifest_path = (
            args.output_dir.resolve() / "market_inventory_manifest.json"
        )
        if args.resume and inventory_manifest_path.is_file():
            market_ids, traded_inventory_manifest = _load_traded_market_inventory(
                args.output_dir,
                expected_query_scope=inventory_scope,
            )
        else:
            market_ids = _traded_market_ids(
                clickhouse,
                from_ts=args.trade_from_ts,
                to_ts=args.trade_to_ts,
                limit=args.limit,
            )
        source_scope = {
            "mode": "ALL_ONE_SIDED_TRADE_TAPE_MARKETS",
            "trade_from_ts": args.trade_from_ts,
            "trade_to_ts": args.trade_to_ts,
            "inventory_query_scope": inventory_scope,
        }
    if args.limit is not None and not args.all_traded_markets:
        market_ids = market_ids[: args.limit]
    if not market_ids:
        raise SystemExit("market universe is empty")

    source_scope["requested_market_count"] = len(market_ids)
    source_scope["market_ids_sha256"] = _market_ids_sha256(market_ids)
    source_scope["settlement_evidence_policy"] = {
        "policy": evidence_policy,
        "clickhouse_header_source_prefixes": [
            "rpc",
            "polygon_rpc",
            "trade_time:pg_api_trades_tx_path",
            "trade_time:pg_api_trades_main",
        ],
        "polygon_rpc_backfill_enabled": (
            evidence_policy == EXACT_HEADER_REQUIRED
            and bool(str(args.polygon_rpc_url).strip())
        ),
        "rpc_batch_size": args.rpc_batch_size,
        "rpc_retry_count": args.rpc_retry_count,
        "cutoff_block_boundary": cutoff_boundary,
    }
    partitioned = bool(args.partitioned or args.all_traded_markets)
    with postgres_connection(PostgresSettings(), readonly=True) as conn:
        if partitioned:
            contract = initialize_partitioned_market_settlement_catalog(
                output_dir=args.output_dir,
                cutoff_ts=args.cutoff_ts,
                source_scope=source_scope,
                partition_size=args.partition_size,
                resume=args.resume,
            )
            if args.all_traded_markets and traded_inventory_manifest is None:
                traded_inventory_manifest = _write_traded_market_inventory(
                    args.output_dir,
                    market_ids=market_ids,
                    query_scope=_traded_inventory_query_scope(args),
                )
            if traded_inventory_manifest is not None and (
                traded_inventory_manifest["market_ids_sha256"]
                != source_scope["market_ids_sha256"]
                or int(traded_inventory_manifest["market_count"])
                != len(market_ids)
            ):
                raise RuntimeError(
                    "traded-market inventory differs from catalog build contract"
                )
            part_count = 0
            for part_index, market_chunk in enumerate(
                _chunks(market_ids, args.partition_size)
            ):
                records = iter_market_settlement_catalog(
                    conn,
                    market_chunk,
                    cutoff_ts=args.cutoff_ts,
                    clickhouse=clickhouse,
                    market_chunk_size=args.market_chunk_size,
                    block_chunk_size=args.block_chunk_size,
                    polygon_rpc_url=args.polygon_rpc_url,
                    rpc_batch_size=args.rpc_batch_size,
                    rpc_workers=args.rpc_workers,
                    rpc_retry_count=args.rpc_retry_count,
                    evidence_policy=evidence_policy,
                    cutoff_block_boundary=cutoff_boundary,
                    rpc_backfill_missing_headers=(
                        evidence_policy == EXACT_HEADER_REQUIRED
                    ),
                )
                write_market_settlement_catalog_part(
                    records,
                    output_dir=args.output_dir,
                    part_index=part_index,
                    expected_market_ids=market_chunk,
                    build_contract_sha256=str(contract["build_contract_sha256"]),
                    resume=args.resume,
                )
                part_count += 1
                print(
                    json.dumps(
                        {
                            "status": "PART_COMPLETE",
                            "partIndex": part_index,
                            "partCount": (len(market_ids) + args.partition_size - 1)
                            // args.partition_size,
                            "recordsComplete": min(
                                (part_index + 1) * args.partition_size,
                                len(market_ids),
                            ),
                        },
                        sort_keys=True,
                    ),
                    file=sys.stderr,
                )
            manifest = finalize_partitioned_market_settlement_catalog(
                output_dir=args.output_dir,
                expected_part_count=part_count,
                expected_record_count=len(market_ids),
            )
        else:
            if args.resume:
                parser.error("--resume requires --partitioned or --all-traded-markets")
            records = iter_market_settlement_catalog(
                conn,
                market_ids,
                cutoff_ts=args.cutoff_ts,
                clickhouse=clickhouse,
                market_chunk_size=args.market_chunk_size,
                block_chunk_size=args.block_chunk_size,
                polygon_rpc_url=args.polygon_rpc_url,
                rpc_batch_size=args.rpc_batch_size,
                rpc_workers=args.rpc_workers,
                rpc_retry_count=args.rpc_retry_count,
                evidence_policy=evidence_policy,
                cutoff_block_boundary=cutoff_boundary,
                rpc_backfill_missing_headers=(
                    evidence_policy == EXACT_HEADER_REQUIRED
                ),
            )
            manifest = write_market_settlement_catalog(
                records,
                output_dir=args.output_dir,
                cutoff_ts=args.cutoff_ts,
                source_scope=source_scope,
            )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
