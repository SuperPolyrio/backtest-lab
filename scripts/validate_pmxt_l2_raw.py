#!/usr/bin/env python3
"""Validate that PMXT raw parquet can produce token-level L2 book state.

This is intentionally a small read-only probe. It does not materialize snapshots
or write database rows; it answers whether a local PMXT mirror can be filtered
to a market/token window and replayed into the existing LocalOrderBook.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

try:
    from datetime import UTC
except ImportError:  # pragma: no cover - Python < 3.11 compatibility
    from datetime import timezone

    UTC = timezone.utc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.core.db import postgres_connection
from quant.orderbook import (
    NormalizedBookDelta,
    NormalizedBookSnapshot,
    OrderBookNotReady,
    OrderBookOutOfOrder,
    TokenBookIdentity,
    normalize_polymarket_event,
)
from quant.orderbook.registry import OrderBookRegistry

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - environment guard
    raise SystemExit(
        "pyarrow is required. Use the project env, for example: "
        "conda run -n polyBots python scripts/validate_pmxt_l2_raw.py ..."
    ) from exc


RAW_PREFIX = "polymarket_orderbook_"
RAW_SUFFIX = ".parquet"
OLD_PAYLOAD_COLUMNS = {"market_id", "update_type", "data"}
FIXED_RAW_COLUMNS = {
    "timestamp",
    "market",
    "event_type",
    "asset_id",
    "bids",
    "asks",
    "price",
    "size",
    "side",
}


@dataclass
class TokenSelection:
    condition_id: str
    token_id: str
    token_side: str = "YES"
    market_id: int = 0
    market_slug: str | None = None
    market_title: str | None = None


@dataclass
class ValidationSummary:
    pmxt_root: str
    selection: dict[str, Any]
    files_considered: int = 0
    files_read: int = 0
    schemas_seen: dict[str, int] = field(default_factory=dict)
    rows_scanned: int = 0
    matched_events: int = 0
    snapshots_seen: int = 0
    deltas_seen: int = 0
    l2_delta_count: int = 0
    snapshot_delta_count: int = 0
    upsert_delta_count: int = 0
    delete_delta_count: int = 0
    applied_events: int = 0
    skipped_delta_before_snapshot: int = 0
    out_of_order_events: int = 0
    parse_errors: int = 0
    sample_events: list[dict[str, Any]] = field(default_factory=list)
    sample_deltas: list[dict[str, Any]] = field(default_factory=list)
    final_book: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class L2DeltaRecord:
    token_id: str
    event_type: str
    operation: str
    side: str
    price: str
    size: str
    event_ts_ms: int
    level_index: int | None = None
    source_hash: str | None = None


@dataclass
class L2ReplayReport:
    status: str
    selection: dict[str, Any]
    rows_seen: int = 0
    normalized_events: int = 0
    matched_events: int = 0
    snapshot_events: int = 0
    price_change_events: int = 0
    l2_delta_count: int = 0
    snapshot_delta_count: int = 0
    upsert_delta_count: int = 0
    delete_delta_count: int = 0
    applied_events: int = 0
    skipped_delta_before_snapshot: int = 0
    out_of_order_events: int = 0
    parse_errors: int = 0
    first_event_ts_ms: int | None = None
    last_event_ts_ms: int | None = None
    sample_deltas: list[dict[str, Any]] = field(default_factory=list)
    final_book: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass
class OrderFilledL2AlignmentReport:
    status: str
    selection: dict[str, Any]
    max_lag_ms: int
    pmxt_rows_seen: int = 0
    pmxt_matched_events: int = 0
    pmxt_applied_events: int = 0
    orderfilled_rows_seen: int = 0
    orderfilled_rows_matched: int = 0
    missing_timestamp_count: int = 0
    missing_l2_before_fill_count: int = 0
    stale_l2_count: int = 0
    aligned_count: int = 0
    depth_checked_count: int = 0
    depth_sufficient_count: int = 0
    depth_insufficient_count: int = 0
    crossable_depth_sufficient_count: int = 0
    price_outside_spread_count: int = 0
    price_unchecked_count: int = 0
    first_l2_ts_ms: int | None = None
    last_l2_ts_ms: int | None = None
    first_fill_ts_ms: int | None = None
    last_fill_ts_ms: int | None = None
    alignment_pct: str = "0"
    price_compatible_pct: str = "0"
    depth_sufficient_pct: str = "0"
    crossable_depth_sufficient_pct: str = "0"
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read a PMXT raw parquet mirror and validate L2 book replay for one token."
    )
    parser.add_argument("--pmxt-root", type=Path, required=True, help="Local PMXT raw mirror root.")
    parser.add_argument("--market-slug", default=None, help="Resolve condition/token from Postgres.")
    parser.add_argument("--condition-id", default=None, help="Polymarket condition id / PMXT market id.")
    parser.add_argument("--token-id", default=None, help="CLOB token id / PMXT asset id.")
    parser.add_argument("--token-side", default="YES", help="Token side used when resolving market slug.")
    parser.add_argument("--start-hour", default=None, help="UTC hour, e.g. 2026-06-22T15.")
    parser.add_argument("--end-hour", default=None, help="UTC hour, e.g. 2026-06-22T17.")
    parser.add_argument("--max-hours", type=int, default=24, help="Max files to scan when no hour window is set.")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--event-limit", type=int, default=1_000, help="Stop after this many matched events.")
    parser.add_argument("--sample-limit", type=int, default=8)
    parser.add_argument("--depth-levels", type=int, default=5)
    parser.add_argument("--json-out", type=Path, default=None)
    args = parser.parse_args()

    selection = resolve_selection(args)
    summary = ValidationSummary(
        pmxt_root=str(args.pmxt_root),
        selection=asdict(selection),
    )

    paths = list(iter_pmxt_paths(
        args.pmxt_root,
        start_hour=parse_hour(args.start_hour),
        end_hour=parse_hour(args.end_hour),
        max_hours=max(1, args.max_hours),
    ))
    summary.files_considered = len(paths)
    if not paths:
        summary.warnings.append(
            "No PMXT raw parquet files found. Expected polymarket_orderbook_YYYY-MM-DDTHH.parquet "
            "under either the root or YYYY/MM/DD subdirectories."
        )
        emit_summary(summary, args.json_out)
        return 2

    registry = OrderBookRegistry()
    identity = TokenBookIdentity(
        token_id=selection.token_id,
        market_id=int(selection.market_id or 0),
        condition_id=selection.condition_id,
        outcome=selection.token_side,
        market_slug=selection.market_slug,
    )

    for path in paths:
        if summary.matched_events >= args.event_limit:
            break
        try:
            parquet_file = pq.ParquetFile(path)
        except Exception as exc:
            summary.parse_errors += 1
            summary.warnings.append(f"Could not open {path}: {exc}")
            continue
        schema_kind = classify_schema(parquet_file.schema_arrow.names)
        summary.schemas_seen[schema_kind] = summary.schemas_seen.get(schema_kind, 0) + 1
        if schema_kind == "unsupported":
            summary.warnings.append(f"Unsupported PMXT schema at {path}: {parquet_file.schema_arrow.names}")
            continue
        summary.files_read += 1
        for row in iter_matching_rows(
            parquet_file,
            schema_kind=schema_kind,
            selection=selection,
            batch_size=max(1, args.batch_size),
        ):
            summary.rows_scanned += 1
            normalized_events = row_to_normalized_events(row, schema_kind=schema_kind)
            if not normalized_events:
                summary.parse_errors += 1
                continue
            for event in normalized_events:
                if event.token_id != selection.token_id:
                    continue
                summary.matched_events += 1
                if event.__class__.__name__.endswith("Snapshot"):
                    summary.snapshots_seen += 1
                    event_kind = "snapshot"
                else:
                    summary.deltas_seen += 1
                    event_kind = "delta"
                delta_records = l2_delta_records_from_event(event)
                summary.l2_delta_count += len(delta_records)
                if event_kind == "snapshot":
                    summary.snapshot_delta_count += len(delta_records)
                else:
                    for delta in delta_records:
                        if delta.operation == "delete":
                            summary.delete_delta_count += 1
                        elif delta.operation == "upsert":
                            summary.upsert_delta_count += 1
                if len(summary.sample_events) < args.sample_limit:
                    summary.sample_events.append(sample_event_payload(row, event_kind))
                if len(summary.sample_deltas) < args.sample_limit:
                    remaining = args.sample_limit - len(summary.sample_deltas)
                    summary.sample_deltas.extend(asdict(delta) for delta in delta_records[:remaining])
                try:
                    registry.apply(identity, event)
                    summary.applied_events += 1
                except OrderBookNotReady:
                    summary.skipped_delta_before_snapshot += 1
                except OrderBookOutOfOrder:
                    summary.out_of_order_events += 1
                except Exception as exc:
                    summary.parse_errors += 1
                    if len(summary.warnings) < 20:
                        summary.warnings.append(f"Failed to apply event from {path}: {exc}")
                if summary.matched_events >= args.event_limit:
                    break
            if summary.matched_events >= args.event_limit:
                break

    book = registry.get(selection.token_id)
    if book is None:
        summary.warnings.append("No events matched the requested condition/token.")
    else:
        summary.final_book = book_metrics_payload(book.metrics(depth_levels=args.depth_levels))

    emit_summary(summary, args.json_out)
    return 0 if summary.final_book.get("status") == "ready" else 1


def build_pmxt_orderfilled_alignment_report(
    pmxt_rows: Iterable[dict[str, Any]],
    *,
    schema_kind: str,
    selection: TokenSelection,
    orderfilled_rows: Iterable[dict[str, Any]],
    max_lag_ms: int = 60_000,
    depth_levels: int = 5,
    sample_limit: int = 20,
) -> OrderFilledL2AlignmentReport:
    """Align historical PMXT L2 state to OrderFilled trade evidence.

    This is a pure, DB-free calibration primitive. PMXT rows rebuild a point-in-time
    L2 book tape; OrderFilled rows are then checked against the latest book state
    at or before each fill timestamp. The report does not claim exact queue/FIFO
    replay. It answers the more basic question needed before DEPTH execution:
    was there a fresh L2 book state near the fill, and was the fill price
    compatible with that book?
    """

    report = OrderFilledL2AlignmentReport(
        status="missing",
        selection=asdict(selection),
        max_lag_ms=max(0, int(max_lag_ms)),
    )
    registry = OrderBookRegistry()
    identity = TokenBookIdentity(
        token_id=selection.token_id,
        market_id=int(selection.market_id or 0),
        condition_id=selection.condition_id,
        outcome=selection.token_side,
        market_slug=selection.market_slug,
    )
    book_points: list[dict[str, Any]] = []
    for row in sorted(list(pmxt_rows), key=lambda item: row_sort_key(item, schema_kind=schema_kind)):
        report.pmxt_rows_seen += 1
        normalized_events = row_to_normalized_events(row, schema_kind=schema_kind)
        if not normalized_events:
            continue
        for event in normalized_events:
            if event.token_id != selection.token_id:
                continue
            report.pmxt_matched_events += 1
            event_ts_ms = int(event.event_ts_ms or 0)
            if event_ts_ms:
                if report.first_l2_ts_ms is None or event_ts_ms < report.first_l2_ts_ms:
                    report.first_l2_ts_ms = event_ts_ms
                if report.last_l2_ts_ms is None or event_ts_ms > report.last_l2_ts_ms:
                    report.last_l2_ts_ms = event_ts_ms
            try:
                metrics = registry.apply(identity, event)
            except OrderBookNotReady:
                continue
            except OrderBookOutOfOrder:
                continue
            except Exception as exc:
                if len(report.warnings) < 20:
                    report.warnings.append(f"failed_to_apply_l2_event: {exc}")
                continue
            report.pmxt_applied_events += 1
            if metrics.status == "ready" and metrics.last_event_ts_ms is not None:
                book = registry.get(selection.token_id)
                if book is not None:
                    book_points.append(book_point_payload(book, depth_levels=depth_levels))

    book_points.sort(key=lambda point: int(point.get("last_event_ts_ms") or 0))
    fills = [_normalize_orderfilled_alignment_row(row, selection=selection) for row in orderfilled_rows]
    report.orderfilled_rows_seen = len(fills)
    matched_fills = [row for row in fills if row is not None]
    report.orderfilled_rows_matched = len(matched_fills)
    for fill in matched_fills:
        fill_ts_ms = int(fill["event_ts_ms"] or 0)
        if fill_ts_ms:
            if report.first_fill_ts_ms is None or fill_ts_ms < report.first_fill_ts_ms:
                report.first_fill_ts_ms = fill_ts_ms
            if report.last_fill_ts_ms is None or fill_ts_ms > report.last_fill_ts_ms:
                report.last_fill_ts_ms = fill_ts_ms
        else:
            report.missing_timestamp_count += 1
            _append_alignment_sample(report, sample_limit, fill, None, "missing_fill_timestamp")
            continue

        book = _latest_book_at_or_before(book_points, fill_ts_ms)
        if book is None:
            report.missing_l2_before_fill_count += 1
            _append_alignment_sample(report, sample_limit, fill, None, "missing_l2_before_fill")
            continue
        lag_ms = fill_ts_ms - int(book.get("last_event_ts_ms") or 0)
        if lag_ms > report.max_lag_ms:
            report.stale_l2_count += 1
            _append_alignment_sample(report, sample_limit, fill, book, "stale_l2", lag_ms=lag_ms)
            continue

        price_check = _price_book_compatibility(fill["trade_price"], book)
        depth_check = _fill_l2_depth_check(fill, book)
        report.aligned_count += 1
        if depth_check["depth_check"] != "depth_unchecked":
            report.depth_checked_count += 1
            if depth_check["depth_sufficient"]:
                report.depth_sufficient_count += 1
            else:
                report.depth_insufficient_count += 1
            if depth_check["crossable_depth_sufficient"]:
                report.crossable_depth_sufficient_count += 1
        if price_check == "price_outside_spread":
            report.price_outside_spread_count += 1
        elif price_check != "price_within_spread_or_touch":
            report.price_unchecked_count += 1
        _append_alignment_sample(report, sample_limit, fill, book, price_check, lag_ms=lag_ms, depth_check=depth_check)

    denominator = max(1, report.orderfilled_rows_matched)
    report.alignment_pct = decimal_text((Decimal(report.aligned_count) / Decimal(denominator) * Decimal("100")).quantize(Decimal("0.0001"))) or "0"
    compatible = max(0, report.aligned_count - report.price_outside_spread_count)
    report.price_compatible_pct = decimal_text((Decimal(compatible) / Decimal(denominator) * Decimal("100")).quantize(Decimal("0.0001"))) or "0"
    depth_denominator = max(1, report.depth_checked_count)
    report.depth_sufficient_pct = decimal_text((Decimal(report.depth_sufficient_count) / Decimal(depth_denominator) * Decimal("100")).quantize(Decimal("0.0001"))) or "0"
    report.crossable_depth_sufficient_pct = decimal_text((Decimal(report.crossable_depth_sufficient_count) / Decimal(depth_denominator) * Decimal("100")).quantize(Decimal("0.0001"))) or "0"
    if report.orderfilled_rows_matched <= 0:
        report.status = "missing"
        report.warnings.append("No OrderFilled rows matched the requested token.")
    elif report.pmxt_applied_events <= 0:
        report.status = "missing"
        report.warnings.append("No applicable PMXT L2 book state was available for alignment.")
    elif (
        report.aligned_count == report.orderfilled_rows_matched
        and report.price_outside_spread_count == 0
        and report.missing_timestamp_count == 0
        and report.missing_l2_before_fill_count == 0
        and report.stale_l2_count == 0
    ):
        report.status = "ready"
    else:
        report.status = "review"
    return report


def build_pmxt_l2_replay_report(
    rows: Iterable[dict[str, Any]],
    *,
    schema_kind: str,
    selection: TokenSelection,
    depth_levels: int = 5,
    sample_limit: int = 20,
) -> L2ReplayReport:
    """Convert already-filtered PMXT rows into an auditable L2 replay report.

    This is the historical analogue of a small L2 MBP tape: snapshots rebuild the
    whole book and price_change rows become explicit upsert/delete depth deltas.
    It is intentionally pure and DB-free so tests and future run artifacts can
    prove the PMXT parquet path before materializing snapshots.
    """

    report = L2ReplayReport(status="empty", selection=asdict(selection))
    registry = OrderBookRegistry()
    identity = TokenBookIdentity(
        token_id=selection.token_id,
        market_id=int(selection.market_id or 0),
        condition_id=selection.condition_id,
        outcome=selection.token_side,
        market_slug=selection.market_slug,
    )
    sorted_rows = sorted(list(rows), key=lambda row: row_sort_key(row, schema_kind=schema_kind))
    for row in sorted_rows:
        report.rows_seen += 1
        normalized_events = row_to_normalized_events(row, schema_kind=schema_kind)
        if not normalized_events:
            report.parse_errors += 1
            continue
        report.normalized_events += len(normalized_events)
        for event in normalized_events:
            if event.token_id != selection.token_id:
                continue
            report.matched_events += 1
            event_ts_ms = int(event.event_ts_ms or 0)
            if report.first_event_ts_ms is None or event_ts_ms < report.first_event_ts_ms:
                report.first_event_ts_ms = event_ts_ms
            if report.last_event_ts_ms is None or event_ts_ms > report.last_event_ts_ms:
                report.last_event_ts_ms = event_ts_ms
            delta_records = l2_delta_records_from_event(event)
            report.l2_delta_count += len(delta_records)
            if isinstance(event, NormalizedBookSnapshot):
                report.snapshot_events += 1
                report.snapshot_delta_count += len(delta_records)
            else:
                report.price_change_events += 1
                for delta in delta_records:
                    if delta.operation == "delete":
                        report.delete_delta_count += 1
                    elif delta.operation == "upsert":
                        report.upsert_delta_count += 1
            if len(report.sample_deltas) < sample_limit:
                remaining = sample_limit - len(report.sample_deltas)
                report.sample_deltas.extend(asdict(delta) for delta in delta_records[:remaining])
            try:
                registry.apply(identity, event)
                report.applied_events += 1
            except OrderBookNotReady:
                report.skipped_delta_before_snapshot += 1
            except OrderBookOutOfOrder:
                report.out_of_order_events += 1
            except Exception as exc:
                report.parse_errors += 1
                if len(report.warnings) < 20:
                    report.warnings.append(f"failed_to_apply_l2_event: {exc}")
    book = registry.get(selection.token_id)
    if book is not None:
        report.final_book = book_metrics_payload(book.metrics(depth_levels=depth_levels))
    if report.final_book.get("status") == "ready":
        report.status = "ready"
    elif report.matched_events:
        report.status = "review"
    else:
        report.status = "missing"
        report.warnings.append("No PMXT L2 events matched the requested condition/token.")
    return report


def orderfilled_l2_alignment_report_payload(report: OrderFilledL2AlignmentReport) -> dict[str, Any]:
    return asdict(report)


def l2_delta_records_from_event(event: Any) -> list[L2DeltaRecord]:
    if isinstance(event, NormalizedBookSnapshot):
        records: list[L2DeltaRecord] = []
        for side, levels in (("bid", event.bids), ("ask", event.asks)):
            for idx, (price, size) in enumerate(levels):
                records.append(
                    L2DeltaRecord(
                        token_id=event.token_id,
                        event_type="book_snapshot",
                        operation="snapshot",
                        side=side,
                        price=decimal_text(price) or "0",
                        size=decimal_text(size) or "0",
                        event_ts_ms=int(event.event_ts_ms or 0),
                        level_index=idx,
                        source_hash=event.source_hash,
                    )
                )
        return records
    if isinstance(event, NormalizedBookDelta):
        operation = "delete" if event.size <= 0 else "upsert"
        return [
            L2DeltaRecord(
                token_id=event.token_id,
                event_type="price_change",
                operation=operation,
                side=event.side,
                price=decimal_text(event.price) or "0",
                size=decimal_text(event.size) or "0",
                event_ts_ms=int(event.event_ts_ms or 0),
                source_hash=event.source_hash,
            )
        ]
    return []


def resolve_selection(args: argparse.Namespace) -> TokenSelection:
    if args.condition_id and args.token_id:
        return TokenSelection(
            condition_id=str(args.condition_id),
            token_id=str(args.token_id),
            token_side=str(args.token_side or "YES").upper(),
        )
    if not args.market_slug:
        raise SystemExit("Provide either --condition-id and --token-id, or --market-slug.")
    rows: list[dict[str, Any]]
    with postgres_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT market_id, market_slug, condition_id, market_title, token_id, token_side
                FROM quant.market_token_metadata
                WHERE market_slug = %s
                  AND upper(token_side) = upper(%s)
                ORDER BY outcome_index NULLS LAST, token_id
                LIMIT 1
                """,
                (args.market_slug, args.token_side),
            )
            rows = list(cur.fetchall())
    if not rows:
        raise SystemExit(
            f"Could not resolve market_slug={args.market_slug!r} token_side={args.token_side!r} "
            "from quant.market_token_metadata. Pass --condition-id and --token-id directly."
        )
    row = rows[0]
    return TokenSelection(
        condition_id=str(row["condition_id"]),
        token_id=str(row["token_id"]),
        token_side=str(row["token_side"] or args.token_side or "YES").upper(),
        market_id=int(row["market_id"] or 0),
        market_slug=str(row["market_slug"] or args.market_slug),
        market_title=str(row["market_title"] or ""),
    )


def iter_pmxt_paths(
    root: Path,
    *,
    start_hour: datetime | None,
    end_hour: datetime | None,
    max_hours: int,
) -> Iterable[Path]:
    if start_hour is not None or end_hour is not None:
        if start_hour is None or end_hour is None:
            raise SystemExit("--start-hour and --end-hour must be provided together.")
        current = start_hour
        while current <= end_hour:
            filename = archive_filename_for_hour(current)
            for candidate in (root / filename, root / f"{current:%Y/%m/%d}" / filename):
                if candidate.exists():
                    yield candidate
                    break
            current += timedelta(hours=1)
        return

    found = sorted(
        root.rglob(f"{RAW_PREFIX}*{RAW_SUFFIX}"),
        key=lambda path: path.name,
        reverse=True,
    )
    yield from found[:max_hours]


def archive_filename_for_hour(hour: datetime) -> str:
    return f"{RAW_PREFIX}{hour.astimezone(UTC):%Y-%m-%dT%H}{RAW_SUFFIX}"


def parse_hour(raw: str | None) -> datetime | None:
    if not raw:
        return None
    value = raw.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def classify_schema(names: list[str]) -> str:
    name_set = set(names)
    if OLD_PAYLOAD_COLUMNS.issubset(name_set):
        return "payload"
    if FIXED_RAW_COLUMNS.issubset(name_set):
        return "fixed"
    return "unsupported"


def iter_matching_rows(
    parquet_file: pq.ParquetFile,
    *,
    schema_kind: str,
    selection: TokenSelection,
    batch_size: int,
) -> Iterable[dict[str, Any]]:
    columns = (
        ["market_id", "update_type", "data"]
        if schema_kind == "payload"
        else ["timestamp", "market", "event_type", "asset_id", "bids", "asks", "price", "size", "side"]
    )
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
        filtered = filter_batch(batch, schema_kind=schema_kind, selection=selection)
        rows = filtered.to_pylist()
        rows.sort(key=lambda row: row_sort_key(row, schema_kind=schema_kind))
        for row in rows:
            yield row


def filter_batch(
    batch: pa.RecordBatch,
    *,
    schema_kind: str,
    selection: TokenSelection,
) -> pa.RecordBatch:
    if batch.num_rows == 0:
        return batch
    if schema_kind == "payload":
        market_mask = pc.equal(batch.column("market_id"), selection.condition_id)
        update_mask = pc.is_in(
            batch.column("update_type"),
            value_set=pa.array(["book_snapshot", "price_change"]),
        )
        token_mask = pc.match_substring(batch.column("data"), selection.token_id)
        mask = pc.and_(
            pc.and_(pc.fill_null(market_mask, False), pc.fill_null(update_mask, False)),
            pc.fill_null(token_mask, False),
        )
        return batch.filter(mask)

    market_type = batch.schema.field("market").type
    market_value: bytes | str
    if pa.types.is_binary(market_type) or pa.types.is_fixed_size_binary(market_type):
        market_value = selection.condition_id.encode("utf-8")
    else:
        market_value = selection.condition_id
    market_mask = pc.equal(batch.column("market"), pa.scalar(market_value, type=market_type))
    event_mask = pc.is_in(
        batch.column("event_type"),
        value_set=pa.array(["book", "price_change"]),
    )
    token_mask = pc.equal(batch.column("asset_id"), selection.token_id)
    mask = pc.and_(
        pc.and_(pc.fill_null(market_mask, False), pc.fill_null(event_mask, False)),
        pc.fill_null(token_mask, False),
    )
    return batch.filter(mask)


def row_to_normalized_events(row: dict[str, Any], *, schema_kind: str):
    try:
        if schema_kind == "fixed":
            event_type = str(row.get("event_type") or "")
            timestamp_ms = timestamp_to_ms(row.get("timestamp"))
            if event_type == "book":
                event = {
                    "event_type": "book",
                    "asset_id": row.get("asset_id"),
                    "timestamp": timestamp_ms,
                    "bids": parse_json_list(row.get("bids")),
                    "asks": parse_json_list(row.get("asks")),
                }
            elif event_type == "price_change":
                event = {
                    "event_type": "price_change",
                    "market": row.get("market"),
                    "timestamp": timestamp_ms,
                    "price_changes": [
                        {
                            "asset_id": row.get("asset_id"),
                            "side": row.get("side"),
                            "price": row.get("price"),
                            "size": row.get("size"),
                        }
                    ],
                }
            else:
                return []
            return normalize_polymarket_event(event)

        payload = json.loads(str(row.get("data") or "{}"))
        if isinstance(payload, dict) and payload.get("event_type"):
            return normalize_polymarket_event(payload)
        update_type = str(row.get("update_type") or "")
        if update_type == "book_snapshot":
            event = {
                "event_type": "book",
                "asset_id": payload.get("token_id") or payload.get("asset_id"),
                "timestamp": payload.get("timestamp"),
                "bids": payload.get("bids") or payload.get("buys") or [],
                "asks": payload.get("asks") or payload.get("sells") or [],
                "hash": payload.get("hash"),
            }
        elif update_type == "price_change":
            if "price_changes" in payload:
                event = {"event_type": "price_change", **payload}
            else:
                event = {
                    "event_type": "price_change",
                    "market": payload.get("market") or payload.get("market_id"),
                    "timestamp": payload.get("timestamp"),
                    "price_changes": [
                        {
                            "asset_id": payload.get("token_id") or payload.get("asset_id"),
                            "side": payload.get("change_side") or payload.get("side"),
                            "price": payload.get("change_price") or payload.get("price"),
                            "size": payload.get("change_size") or payload.get("size"),
                            "hash": payload.get("hash"),
                        }
                    ],
                }
        else:
            return []
        return normalize_polymarket_event(event)
    except Exception:
        return []


def row_sort_key(row: dict[str, Any], *, schema_kind: str) -> tuple[int, str, str]:
    if schema_kind == "fixed":
        return (
            timestamp_to_ms(row.get("timestamp")),
            str(row.get("event_type") or ""),
            str(row.get("price") or ""),
        )
    try:
        payload = json.loads(str(row.get("data") or "{}"))
    except json.JSONDecodeError:
        payload = {}
    timestamp = payload.get("timestamp") if isinstance(payload, dict) else None
    return (
        timestamp_to_ms(timestamp),
        str(row.get("update_type") or ""),
        str(row.get("data") or ""),
    )


def parse_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        decoded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return decoded if isinstance(decoded, list) else []


def timestamp_to_ms(value: Any) -> int:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return int(value.timestamp() * 1000)
    text = str(value or "").strip()
    if not text:
        return 0
    if re.fullmatch(r"\d+(\.\d+)?", text):
        number_float = float(text)
        number = int(number_float)
        if number > 10**17:
            return number // 1_000_000
        if number > 10**14:
            return number // 1_000
        if number < 10**11:
            return int(number_float * 1000)
        return number
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def _normalize_orderfilled_alignment_row(row: dict[str, Any], *, selection: TokenSelection) -> dict[str, Any] | None:
    token_id = _first_present(row, "token_id", "tokenId", "asset_id", "assetId")
    if token_id not in (None, "") and str(token_id) != selection.token_id:
        return None
    market_id = _first_present(row, "market_id", "marketId")
    if market_id not in (None, "") and selection.market_id and int(market_id) != int(selection.market_id):
        return None
    trade_price = _decimal_or_none(_first_present(row, "trade_price", "price", "avg_fill_price", "fill_price", "fillPrice"))
    if trade_price is None:
        return None
    size = _decimal_or_none(_first_present(row, "size", "filled_size", "filledSize", "amount"))
    event_ts_ms = _orderfilled_event_ts_ms(row)
    return {
        "event_ts_ms": event_ts_ms,
        "block_number": _optional_int(_first_present(row, "block_number", "blockNumber")),
        "transaction_index": _optional_int(_first_present(row, "transaction_index", "transactionIndex")),
        "log_index": _optional_int(_first_present(row, "log_index", "logIndex")),
        "tx_hash": str(_first_present(row, "tx_hash", "txHash") or ""),
        "token_id": str(token_id or selection.token_id),
        "market_id": int(market_id or selection.market_id or 0),
        "trade_price": trade_price,
        "size": size or Decimal("0"),
        "side": _normalize_orderfilled_side(_first_present(row, "side", "side_code", "sideCode", "taker_side", "takerSide")),
    }


def _orderfilled_event_ts_ms(row: dict[str, Any]) -> int:
    for key in (
        "event_ts_ms",
        "timestamp_ms",
        "timestamp",
        "created_at",
        "createdAt",
        "block_timestamp",
        "blockTimestamp",
        "block_time",
        "blockTime",
        "time",
    ):
        if key in row and row.get(key) not in (None, ""):
            return timestamp_to_ms(row.get(key))
    return 0


def _latest_book_at_or_before(book_points: list[dict[str, Any]], ts_ms: int) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for point in book_points:
        point_ts = int(point.get("last_event_ts_ms") or 0)
        if point_ts > ts_ms:
            break
        latest = point
    return latest


def _price_book_compatibility(price: Decimal, book: dict[str, Any]) -> str:
    best_bid = _decimal_or_none(book.get("best_bid"))
    best_ask = _decimal_or_none(book.get("best_ask"))
    if best_bid is None or best_ask is None:
        return "price_unchecked_missing_one_side"
    tolerance = Decimal("0.0000000001")
    if price < best_bid - tolerance or price > best_ask + tolerance:
        return "price_outside_spread"
    return "price_within_spread_or_touch"


def _append_alignment_sample(
    report: OrderFilledL2AlignmentReport,
    sample_limit: int,
    fill: dict[str, Any],
    book: dict[str, Any] | None,
    classification: str,
    *,
    lag_ms: int | None = None,
    depth_check: dict[str, Any] | None = None,
) -> None:
    if len(report.sample_rows) >= sample_limit:
        return
    depth = depth_check or {}
    report.sample_rows.append(
        {
            "classification": classification,
            "depth_check": depth.get("depth_check"),
            "lag_ms": lag_ms,
            "fill_ts_ms": fill.get("event_ts_ms"),
            "book_ts_ms": book.get("last_event_ts_ms") if book else None,
            "block_number": fill.get("block_number"),
            "transaction_index": fill.get("transaction_index"),
            "log_index": fill.get("log_index"),
            "tx_hash": fill.get("tx_hash"),
            "side": fill.get("side"),
            "trade_price": decimal_text(fill.get("trade_price")),
            "size": decimal_text(fill.get("size")),
            "best_bid": book.get("best_bid") if book else None,
            "best_ask": book.get("best_ask") if book else None,
            "mid": book.get("mid") if book else None,
            "spread": book.get("spread") if book else None,
            "bid_depth": book.get("bid_depth") if book else None,
            "ask_depth": book.get("ask_depth") if book else None,
            "l2_depth_side": depth.get("depth_side"),
            "l2_side_depth_size": depth.get("side_depth_size"),
            "l2_crossable_depth_size": depth.get("crossable_depth_size"),
            "l2_fill_participation_pct": depth.get("fill_participation_pct"),
            "l2_crossable_participation_pct": depth.get("crossable_participation_pct"),
        }
    )


def _fill_l2_depth_check(fill: dict[str, Any], book: dict[str, Any]) -> dict[str, Any]:
    side = str(fill.get("side") or "").upper()
    fill_size = _decimal_or_none(fill.get("size")) or Decimal("0")
    trade_price = _decimal_or_none(fill.get("trade_price"))
    if side not in {"BUY", "SELL"} or fill_size <= 0:
        return {
            "depth_check": "depth_unchecked",
            "depth_side": None,
            "side_depth_size": None,
            "crossable_depth_size": None,
            "fill_participation_pct": None,
            "crossable_participation_pct": None,
            "depth_sufficient": False,
            "crossable_depth_sufficient": False,
        }
    depth_side = "ask" if side == "BUY" else "bid"
    levels = _book_levels(book, depth_side)
    if not levels:
        return {
            "depth_check": "missing_l2_depth_side",
            "depth_side": depth_side,
            "side_depth_size": "0",
            "crossable_depth_size": "0",
            "fill_participation_pct": None,
            "crossable_participation_pct": None,
            "depth_sufficient": False,
            "crossable_depth_sufficient": False,
        }
    side_depth_size = sum((size for _price, size in levels), Decimal("0"))
    crossable_depth_size = Decimal("0")
    if trade_price is not None:
        for price, size in levels:
            if depth_side == "ask" and price <= trade_price:
                crossable_depth_size += size
            elif depth_side == "bid" and price >= trade_price:
                crossable_depth_size += size
    depth_sufficient = side_depth_size >= fill_size
    crossable_depth_sufficient = crossable_depth_size >= fill_size
    return {
        "depth_check": "l2_depth_sufficient" if depth_sufficient else "l2_depth_insufficient",
        "depth_side": depth_side,
        "side_depth_size": decimal_text(side_depth_size.quantize(Decimal("0.0000000001"))),
        "crossable_depth_size": decimal_text(crossable_depth_size.quantize(Decimal("0.0000000001"))),
        "fill_participation_pct": decimal_text(_pct_decimal(fill_size, side_depth_size)),
        "crossable_participation_pct": decimal_text(_pct_decimal(fill_size, crossable_depth_size)) if crossable_depth_size > 0 else None,
        "depth_sufficient": depth_sufficient,
        "crossable_depth_sufficient": crossable_depth_sufficient,
    }


def _book_levels(book: dict[str, Any], side: str) -> list[tuple[Decimal, Decimal]]:
    key = "asks" if side == "ask" else "bids"
    rows = book.get(key)
    if not isinstance(rows, list):
        return []
    levels: list[tuple[Decimal, Decimal]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        price = _decimal_or_none(row.get("price"))
        size = _decimal_or_none(row.get("size"))
        if price is not None and size is not None and price > 0 and size > 0:
            levels.append((price, size))
    return levels


def _normalize_orderfilled_side(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"1", "BUY", "BID", "BUY_YES", "TAKER_BUY"}:
        return "BUY"
    if text in {"2", "SELL", "ASK", "SELL_YES", "TAKER_SELL"}:
        return "SELL"
    return text or "UNKNOWN"


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed.is_finite() else None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _first_present(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def sample_event_payload(row: dict[str, Any], event_kind: str) -> dict[str, Any]:
    payload = {key: stringify_value(value) for key, value in row.items()}
    payload["kind"] = event_kind
    return payload


def book_metrics_payload(metrics: Any) -> dict[str, Any]:
    return {
        "status": metrics.status,
        "last_event_ts_ms": metrics.last_event_ts_ms,
        "best_bid": decimal_text(metrics.best_bid),
        "best_bid_size": decimal_text(metrics.best_bid_size),
        "best_ask": decimal_text(metrics.best_ask),
        "best_ask_size": decimal_text(metrics.best_ask_size),
        "mid": decimal_text(metrics.mid),
        "spread": decimal_text(metrics.spread),
        "bid_depth": decimal_text(metrics.bid_depth),
        "ask_depth": decimal_text(metrics.ask_depth),
        "depth_total": decimal_text(metrics.depth_total),
        "level_count_bid": metrics.level_count_bid,
        "level_count_ask": metrics.level_count_ask,
        "stale_reason": metrics.stale_reason,
    }


def book_point_payload(book: Any, *, depth_levels: int) -> dict[str, Any]:
    payload = book.snapshot_payload(depth_levels=depth_levels)
    return {
        "status": payload.get("status"),
        "last_event_ts_ms": payload.get("last_event_ts_ms"),
        "best_bid": payload.get("best_bid"),
        "best_bid_size": payload.get("best_bid_size"),
        "best_ask": payload.get("best_ask"),
        "best_ask_size": payload.get("best_ask_size"),
        "mid": payload.get("mid"),
        "spread": payload.get("spread"),
        "bid_depth": payload.get("bid_depth"),
        "ask_depth": payload.get("ask_depth"),
        "depth_total": payload.get("depth_total"),
        "level_count_bid": len(payload.get("bids") or []),
        "level_count_ask": len(payload.get("asks") or []),
        "stale_reason": payload.get("stale_reason"),
        "bids": payload.get("bids") or [],
        "asks": payload.get("asks") or [],
    }


def decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")


def _pct_decimal(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator <= 0:
        return None
    return (numerator * Decimal("100") / denominator).quantize(Decimal("0.0001"))


def stringify_value(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def emit_summary(summary: ValidationSummary, json_out: Path | None) -> None:
    payload = asdict(summary)
    text = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
    print(text)
    if json_out is not None:
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
