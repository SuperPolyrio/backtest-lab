"""ClickHouse PMXT L2 event replay store.

Sampled snapshots are enough for a UI or a first DEPTH smoke test, but a
historical execution model needs the original L2 event tape: book snapshots as
reset points plus every price_change delta in timestamp order.  This module
materializes bounded PMXT parquet windows into a token-level replay cache.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Iterable, Mapping, Sequence

from quant.core.db import ClickHouseClient, safe_identifier
from quant.backtest.l2_orderfilled_execution import BookDelta, BookLevel, BookSnapshot
from scripts.validate_pmxt_l2_raw import (
    TokenSelection,
    archive_filename_for_hour,
    classify_schema,
    l2_delta_records_from_event,
    parse_hour,
    row_sort_key,
    row_to_normalized_events,
)

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - guarded by scripts/tests that need pyarrow
    pa = None
    pc = None
    pq = None


UTC = timezone.utc
DEFAULT_L2_EVENT_TABLE = "pmxt_l2_event_replay"
DEFAULT_L2_COVERAGE_TABLE = "pmxt_l2_event_replay_coverage"
PMXT_L2_EVENT_DATA_VERSION = "pmxt_l2_event_replay_v1"


@dataclass(frozen=True)
class PmxtL2ReplaySelection:
    market_id: int
    condition_id: str
    token_id: str
    token_side: str
    market_slug: str = ""
    market_title: str = ""


@dataclass(frozen=True)
class PmxtL2EventReplayBackfillResult:
    table: str
    coverage_table: str
    token_count: int
    from_hour: str
    to_hour: str
    expected_hours: int
    local_hours: int
    files_read: int
    inserted_rows: int
    snapshot_events: int
    price_change_events: int
    skipped_delta_before_snapshot: int
    parse_errors: int
    first_event_ts_ms: int | None
    last_event_ts_ms: int | None
    elapsed_sec: float
    coverage_rows: int
    missing_hours: list[str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PmxtL2EventRow:
    market_id: int
    condition_id: str
    market_slug: str
    market_title: str
    token_id: str
    token_side: str
    source_hour: str
    event_ts_ms: int
    event_time: str
    event_type: str
    operation: str
    side: str
    price: str | None
    size: str | None
    level_count_bid: int
    level_count_ask: int
    source_file: str
    source_row_index: int
    source_event_index: int
    source_hash: str
    payload_json: str
    build_tag: str

    def as_insert_row(self) -> dict[str, Any]:
        return asdict(self)


def ensure_pmxt_l2_event_replay_tables(
    client: ClickHouseClient | None = None,
    *,
    table: str = DEFAULT_L2_EVENT_TABLE,
    coverage_table: str = DEFAULT_L2_COVERAGE_TABLE,
) -> None:
    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    coverage_name = safe_identifier(coverage_table)
    ch.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            market_id UInt64 CODEC(Delta, ZSTD(3)),
            condition_id String CODEC(ZSTD(3)),
            market_slug String CODEC(ZSTD(3)),
            market_title String CODEC(ZSTD(3)),
            token_id String CODEC(ZSTD(3)),
            token_side LowCardinality(String) CODEC(ZSTD(3)),
            source_hour DateTime CODEC(Delta, ZSTD(3)),
            event_ts_ms UInt64 CODEC(Delta, ZSTD(3)),
            event_time DateTime64(3) CODEC(Delta, ZSTD(3)),
            event_type LowCardinality(String) CODEC(ZSTD(3)),
            operation LowCardinality(String) CODEC(ZSTD(3)),
            side LowCardinality(String) CODEC(ZSTD(3)),
            price Nullable(Decimal(20, 10)) CODEC(ZSTD(3)),
            size Nullable(Decimal(30, 10)) CODEC(ZSTD(3)),
            level_count_bid UInt32 CODEC(ZSTD(3)),
            level_count_ask UInt32 CODEC(ZSTD(3)),
            source_file String CODEC(ZSTD(3)),
            source_row_index UInt64 CODEC(Delta, ZSTD(3)),
            source_event_index UInt32 CODEC(Delta, ZSTD(3)),
            source_hash String CODEC(ZSTD(3)),
            payload_json String CODEC(ZSTD(9)),
            data_version LowCardinality(String) DEFAULT '{PMXT_L2_EVENT_DATA_VERSION}' CODEC(ZSTD(3)),
            build_tag LowCardinality(String) DEFAULT 'manual_backfill' CODEC(ZSTD(3)),
            ingested_at DateTime DEFAULT now() CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(ingested_at)
        PARTITION BY toYYYYMM(event_time)
        ORDER BY (market_id, condition_id, token_id, event_ts_ms, source_hour, source_row_index, source_event_index, source_hash)
        SETTINGS index_granularity = 8192
        """,
        timeout_seconds=60,
    )
    ch.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {coverage_name}
        (
            market_id UInt64 CODEC(Delta, ZSTD(3)),
            condition_id String CODEC(ZSTD(3)),
            market_slug String CODEC(ZSTD(3)),
            token_id String CODEC(ZSTD(3)),
            token_side LowCardinality(String) CODEC(ZSTD(3)),
            from_hour DateTime CODEC(Delta, ZSTD(3)),
            to_hour DateTime CODEC(Delta, ZSTD(3)),
            expected_hours UInt32 CODEC(ZSTD(3)),
            local_hours UInt32 CODEC(ZSTD(3)),
            files_read UInt32 CODEC(ZSTD(3)),
            row_count UInt64 CODEC(ZSTD(3)),
            snapshot_events UInt64 CODEC(ZSTD(3)),
            price_change_events UInt64 CODEC(ZSTD(3)),
            first_event_ts_ms UInt64 CODEC(Delta, ZSTD(3)),
            last_event_ts_ms UInt64 CODEC(Delta, ZSTD(3)),
            missing_hour_count UInt32 CODEC(ZSTD(3)),
            data_version String CODEC(ZSTD(3)),
            build_tag LowCardinality(String) CODEC(ZSTD(3)),
            updated_at DateTime DEFAULT now() CODEC(Delta, ZSTD(3))
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (market_id, condition_id, token_id, from_hour, to_hour, data_version)
        SETTINGS index_granularity = 8192
        """,
        timeout_seconds=60,
    )


def backfill_pmxt_l2_event_replay(
    pmxt_root: Path,
    selections: Sequence[PmxtL2ReplaySelection],
    *,
    start_hour: datetime,
    end_hour: datetime,
    table: str = DEFAULT_L2_EVENT_TABLE,
    coverage_table: str = DEFAULT_L2_COVERAGE_TABLE,
    client: ClickHouseClient | None = None,
    batch_size: int = 250_000,
    insert_batch_rows: int = 50_000,
    build_tag: str = "manual_backfill",
) -> PmxtL2EventReplayBackfillResult:
    if pq is None or pa is None or pc is None:
        raise RuntimeError("pyarrow is required for PMXT L2 replay backfill")
    normalized = _normalized_selections(selections)
    from_hour = _ensure_hour(start_hour)
    to_hour = _ensure_hour(end_hour)
    if to_hour < from_hour:
        raise ValueError("end_hour must be >= start_hour")
    if not normalized:
        return _empty_result(table, coverage_table, from_hour, to_hour)

    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    coverage_name = safe_identifier(coverage_table)
    ensure_pmxt_l2_event_replay_tables(ch, table=table_name, coverage_table=coverage_name)

    expected_hours = _hour_count(from_hour, to_hour)
    missing_hours: list[str] = []
    local_hours = 0
    files_read = 0
    inserted_rows = 0
    snapshot_events = 0
    price_change_events = 0
    skipped_delta_before_snapshot = 0
    parse_errors = 0
    first_event_ts_ms: int | None = None
    last_event_ts_ms: int | None = None
    start = time.perf_counter()
    pending: list[dict[str, Any]] = []
    per_token_stats: dict[tuple[int, str], dict[str, int | None]] = {
        (selection.market_id, selection.token_id): {
            "row_count": 0,
            "snapshot_events": 0,
            "price_change_events": 0,
            "first_event_ts_ms": None,
            "last_event_ts_ms": None,
        }
        for selection in normalized
    }

    for hour in _iter_hours(from_hour, to_hour):
        path = find_pmxt_hour_path(pmxt_root, hour)
        if path is None:
            missing_hours.append(hour.isoformat())
            continue
        local_hours += 1
        parquet_file = pq.ParquetFile(path)
        schema_kind = classify_schema(parquet_file.schema_arrow.names)
        if schema_kind == "unsupported":
            parse_errors += 1
            continue
        files_read += 1
        matched_rows = list(iter_matching_pmxt_rows_for_selections(
            parquet_file,
            path=path,
            schema_kind=schema_kind,
            selections=normalized,
            batch_size=max(1, int(batch_size)),
        ))
        matched_rows.sort(key=lambda row: row_sort_key(row, schema_kind=schema_kind))
        for row_index, row in enumerate(matched_rows):
            events = row_to_normalized_events(row, schema_kind=schema_kind)
            if not events:
                parse_errors += 1
                continue
            for event_index, event in enumerate(events):
                selection = _selection_for_event(normalized, row=row, schema_kind=schema_kind, token_id=str(event.token_id))
                if selection is None:
                    continue
                event_ts_ms = int(getattr(event, "event_ts_ms", 0) or 0)
                if event_ts_ms <= 0:
                    parse_errors += 1
                    continue
                first_event_ts_ms = event_ts_ms if first_event_ts_ms is None else min(first_event_ts_ms, event_ts_ms)
                last_event_ts_ms = event_ts_ms if last_event_ts_ms is None else max(last_event_ts_ms, event_ts_ms)
                event_type = "book_snapshot" if event.__class__.__name__.endswith("Snapshot") else "price_change"
                operation, side, price, size, level_count_bid, level_count_ask = _event_scalar_fields(event)
                token_stats = per_token_stats[(selection.market_id, selection.token_id)]
                token_stats["row_count"] = int(token_stats["row_count"] or 0) + 1
                token_stats["first_event_ts_ms"] = (
                    event_ts_ms
                    if token_stats["first_event_ts_ms"] is None
                    else min(int(token_stats["first_event_ts_ms"]), event_ts_ms)
                )
                token_stats["last_event_ts_ms"] = (
                    event_ts_ms
                    if token_stats["last_event_ts_ms"] is None
                    else max(int(token_stats["last_event_ts_ms"]), event_ts_ms)
                )
                if event_type == "book_snapshot":
                    snapshot_events += 1
                    token_stats["snapshot_events"] = int(token_stats["snapshot_events"] or 0) + 1
                else:
                    price_change_events += 1
                    token_stats["price_change_events"] = int(token_stats["price_change_events"] or 0) + 1
                    if operation == "delta_before_snapshot":
                        skipped_delta_before_snapshot += 1
                pending.append(
                    PmxtL2EventRow(
                        market_id=selection.market_id,
                        condition_id=selection.condition_id,
                        market_slug=selection.market_slug,
                        market_title=selection.market_title,
                        token_id=selection.token_id,
                        token_side=selection.token_side,
                        source_hour=_clickhouse_datetime(hour),
                        event_ts_ms=event_ts_ms,
                        event_time=_clickhouse_datetime_ms(event_ts_ms),
                        event_type=event_type,
                        operation=operation,
                        side=side,
                        price=price,
                        size=size,
                        level_count_bid=level_count_bid,
                        level_count_ask=level_count_ask,
                        source_file=str(path),
                        source_row_index=row_index,
                        source_event_index=event_index,
                        source_hash=_source_hash(event, path=path, row_index=row_index, event_index=event_index),
                        payload_json=_event_payload_json(event),
                        build_tag=build_tag,
                    ).as_insert_row()
                )
                if len(pending) >= max(1, int(insert_batch_rows)):
                    inserted_rows += _insert_json_rows(ch, table_name, pending)
                    pending.clear()
    if pending:
        inserted_rows += _insert_json_rows(ch, table_name, pending)

    coverage_rows = _insert_coverage_rows(
        ch,
        coverage_name,
        normalized,
        from_hour=from_hour,
        to_hour=to_hour,
        expected_hours=expected_hours,
        local_hours=local_hours,
        files_read=files_read,
        missing_hour_count=len(missing_hours),
        per_token_stats=per_token_stats,
        build_tag=build_tag,
    )
    return PmxtL2EventReplayBackfillResult(
        table=table_name,
        coverage_table=coverage_name,
        token_count=len(normalized),
        from_hour=from_hour.isoformat(),
        to_hour=to_hour.isoformat(),
        expected_hours=expected_hours,
        local_hours=local_hours,
        files_read=files_read,
        inserted_rows=inserted_rows,
        snapshot_events=snapshot_events,
        price_change_events=price_change_events,
        skipped_delta_before_snapshot=skipped_delta_before_snapshot,
        parse_errors=parse_errors,
        first_event_ts_ms=first_event_ts_ms,
        last_event_ts_ms=last_event_ts_ms,
        elapsed_sec=round(time.perf_counter() - start, 6),
        coverage_rows=coverage_rows,
        missing_hours=missing_hours,
    )


def iter_matching_pmxt_rows_for_selections(
    parquet_file: Any,
    *,
    path: Path | None = None,
    schema_kind: str,
    selections: Sequence[PmxtL2ReplaySelection],
    batch_size: int,
) -> Iterable[dict[str, Any]]:
    if pa is None or pc is None:
        raise RuntimeError("pyarrow is required for PMXT filtering")
    if not selections:
        return
    columns = (
        ["market_id", "update_type", "data"]
        if schema_kind == "payload"
        else ["timestamp", "market", "event_type", "asset_id", "bids", "asks", "price", "size", "side"]
    )
    if schema_kind == "fixed" and path is not None:
        token_ids = sorted({selection.token_id for selection in selections})
        table = pq.read_table(
            path,
            columns=columns,
            filters=[
                ("asset_id", "in", token_ids),
                ("event_type", "in", ["book", "price_change"]),
            ],
        )
        filtered = _filter_batch_for_selections(table, schema_kind=schema_kind, selections=selections)
        for row in filtered.to_pylist():
            yield row
        return
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
        filtered = _filter_batch_for_selections(batch, schema_kind=schema_kind, selections=selections)
        for row in filtered.to_pylist():
            yield row


def find_pmxt_hour_path(root: Path, hour: datetime) -> Path | None:
    hour = _ensure_hour(hour)
    filename = archive_filename_for_hour(hour)
    for candidate in (root / filename, root / f"{hour:%Y/%m/%d}" / filename):
        if candidate.exists():
            return candidate
    return None


def load_pmxt_l2_event_coverage(
    selections: Sequence[PmxtL2ReplaySelection],
    *,
    from_hour: datetime,
    to_hour: datetime,
    table: str = DEFAULT_L2_COVERAGE_TABLE,
    client: ClickHouseClient | None = None,
) -> list[dict[str, Any]]:
    normalized = _normalized_selections(selections)
    if not normalized:
        return []
    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    pairs_sql = _pairs_sql(normalized)
    from_dt = _quote(_clickhouse_datetime(_ensure_hour(from_hour)))
    to_dt = _quote(_clickhouse_datetime(_ensure_hour(to_hour)))
    return ch.query_json_rows(
        f"""
        SELECT *
        FROM {table_name} FINAL
        WHERE (market_id, token_id) IN ({pairs_sql})
          AND from_hour = toDateTime({from_dt})
          AND to_hour = toDateTime({to_dt})
        ORDER BY market_id ASC, token_id ASC, updated_at DESC
        """
    )


def load_pmxt_l2_execution_events(
    *,
    market_id: int,
    token_id: str,
    from_event_ts_ms: int,
    to_event_ts_ms: int,
    table: str = DEFAULT_L2_EVENT_TABLE,
    client: ClickHouseClient | None = None,
    limit: int = 100_000,
) -> list[BookSnapshot | BookDelta]:
    """Load materialized PMXT L2 replay rows as execution-model events."""

    ch = client or ClickHouseClient()
    table_name = safe_identifier(table)
    rows = ch.query_json_rows(
        f"""
        SELECT *
        FROM {table_name} FINAL
        WHERE market_id = {int(market_id)}
          AND token_id = {_quote(str(token_id).strip().lower())}
          AND event_ts_ms BETWEEN {int(from_event_ts_ms)} AND {int(to_event_ts_ms)}
        ORDER BY event_ts_ms ASC, source_hour ASC, source_row_index ASC, source_event_index ASC, source_hash ASC
        LIMIT {max(1, int(limit))}
        """
    )
    return [event for row in rows if (event := pmxt_l2_event_row_to_execution_event(row)) is not None]


def pmxt_l2_event_row_to_execution_event(row: Mapping[str, Any]) -> BookSnapshot | BookDelta | None:
    """Convert one PMXT L2 replay row into the DEPTH execution event type."""

    event_type = str(row.get("event_type") or "")
    ts = _datetime_from_ms(int(row.get("event_ts_ms") or 0))
    market_id = str(row.get("condition_id") or row.get("market_id") or "")
    token_id = str(row.get("token_id") or "")
    if not token_id or int(row.get("event_ts_ms") or 0) <= 0:
        return None
    if event_type == "book_snapshot":
        try:
            payload = json.loads(str(row.get("payload_json") or "{}"))
        except json.JSONDecodeError:
            payload = {}
        return BookSnapshot(
            ts=ts,
            market_id=market_id,
            asset_id=token_id,
            sequence=_optional_int(row.get("source_row_index")),
            source="pmxt_l2_event_replay",
            bids=_book_levels(payload.get("bids")),
            asks=_book_levels(payload.get("asks")),
            hash=str(row.get("source_hash") or "") or None,
            is_full_depth=True,
            observed_depth_levels=max(int(row.get("level_count_bid") or 0), int(row.get("level_count_ask") or 0)) or None,
        )
    if event_type == "price_change":
        side = _execution_side(row.get("side"))
        price = _decimal_or_none(row.get("price"))
        size = _decimal_or_none(row.get("size"))
        if side is None or price is None or size is None:
            return None
        return BookDelta(
            ts=ts,
            market_id=market_id,
            asset_id=token_id,
            side=side,
            price=price,
            new_size=size,
            sequence=_optional_int(row.get("source_row_index")),
            source="pmxt_l2_event_replay",
            hash=str(row.get("source_hash") or "") or None,
        )
    return None


def _filter_batch_for_selections(batch: Any, *, schema_kind: str, selections: Sequence[PmxtL2ReplaySelection]) -> Any:
    if batch.num_rows == 0:
        return batch
    condition_ids = sorted({selection.condition_id for selection in selections})
    token_ids = sorted({selection.token_id for selection in selections})
    if schema_kind == "payload":
        market_mask = pc.is_in(batch.column("market_id"), value_set=pa.array(condition_ids))
        update_mask = pc.is_in(batch.column("update_type"), value_set=pa.array(["book_snapshot", "price_change"]))
        # Legacy PMXT stores token ids inside a JSON payload.  Searching the
        # whole hourly payload column for every selected token is the dominant
        # cost for historical backfills.  Filter cheaply by condition/update
        # here; row_to_normalized_events + _selection_for_event performs exact
        # token matching after the much smaller condition slice is decoded.
        mask = pc.and_(pc.fill_null(market_mask, False), pc.fill_null(update_mask, False))
        return batch.filter(mask)

    market_type = batch.schema.field("market").type
    if pa.types.is_binary(market_type) or pa.types.is_fixed_size_binary(market_type):
        market_values = [value.encode("utf-8") for value in condition_ids]
    else:
        market_values = condition_ids
    market_mask = pc.is_in(batch.column("market"), value_set=pa.array(market_values, type=market_type))
    event_mask = pc.is_in(batch.column("event_type"), value_set=pa.array(["book", "price_change"]))
    token_mask = pc.is_in(batch.column("asset_id"), value_set=pa.array(token_ids))
    mask = pc.and_(pc.and_(pc.fill_null(market_mask, False), pc.fill_null(event_mask, False)), pc.fill_null(token_mask, False))
    return batch.filter(mask)


def _selection_for_event(
    selections: Sequence[PmxtL2ReplaySelection],
    *,
    row: Mapping[str, Any],
    schema_kind: str,
    token_id: str,
) -> PmxtL2ReplaySelection | None:
    condition_id = _condition_id_from_row(row, schema_kind=schema_kind)
    for selection in selections:
        if selection.token_id == token_id and selection.condition_id == condition_id:
            return selection
    return None


def _condition_id_from_row(row: Mapping[str, Any], *, schema_kind: str) -> str:
    if schema_kind == "fixed":
        raw = row.get("market")
        if isinstance(raw, (bytes, bytearray)):
            return raw.decode("utf-8", errors="replace")
        return str(raw or "")
    return str(row.get("market_id") or "")


def _event_scalar_fields(event: Any) -> tuple[str, str, str | None, str | None, int, int]:
    if event.__class__.__name__.endswith("Snapshot"):
        return (
            "snapshot",
            "",
            None,
            None,
            len(getattr(event, "bids", ()) or ()),
            len(getattr(event, "asks", ()) or ()),
        )
    deltas = l2_delta_records_from_event(event)
    if not deltas:
        return ("delta_before_snapshot", "", None, None, 0, 0)
    delta = deltas[0]
    return (
        delta.operation,
        delta.side,
        delta.price,
        delta.size,
        0,
        0,
    )


def _event_payload_json(event: Any) -> str:
    raw = getattr(event, "raw", None)
    if raw:
        return json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str)
    if event.__class__.__name__.endswith("Snapshot"):
        payload = {
            "event_type": "book",
            "asset_id": event.token_id,
            "timestamp": int(event.event_ts_ms or 0),
            "bids": [{"price": _decimal_text(price), "size": _decimal_text(size)} for price, size in event.bids],
            "asks": [{"price": _decimal_text(price), "size": _decimal_text(size)} for price, size in event.asks],
        }
    else:
        payload = {
            "event_type": "price_change",
            "timestamp": int(event.event_ts_ms or 0),
            "price_changes": [
                {
                    "asset_id": event.token_id,
                    "side": event.side,
                    "price": _decimal_text(event.price),
                    "size": _decimal_text(event.size),
                }
            ],
        }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _source_hash(event: Any, *, path: Path, row_index: int, event_index: int) -> str:
    raw_hash = str(getattr(event, "source_hash", "") or "")
    if raw_hash:
        return raw_hash
    text = "|".join(
        [
            path.name,
            str(row_index),
            str(event_index),
            str(getattr(event, "token_id", "")),
            str(getattr(event, "event_ts_ms", "")),
            _event_payload_json(event),
        ]
    )
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _insert_json_rows(client: ClickHouseClient, table: str, rows: Sequence[Mapping[str, Any]]) -> int:
    if not rows:
        return 0
    payload = "\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in rows)
    client.execute(f"INSERT INTO {safe_identifier(table)} FORMAT JSONEachRow", stdin=payload, timeout_seconds=600)
    return len(rows)


def _insert_coverage_rows(
    client: ClickHouseClient,
    coverage_table: str,
    selections: Sequence[PmxtL2ReplaySelection],
    *,
    from_hour: datetime,
    to_hour: datetime,
    expected_hours: int,
    local_hours: int,
    files_read: int,
    missing_hour_count: int,
    per_token_stats: Mapping[tuple[int, str], Mapping[str, int | None]],
    build_tag: str,
) -> int:
    if not selections:
        return 0
    rows = [
        {
            "market_id": selection.market_id,
            "condition_id": selection.condition_id,
            "market_slug": selection.market_slug,
            "token_id": selection.token_id,
            "token_side": selection.token_side,
            "from_hour": _clickhouse_datetime(from_hour),
            "to_hour": _clickhouse_datetime(to_hour),
            "expected_hours": expected_hours,
            "local_hours": local_hours,
            "files_read": files_read,
            "row_count": int((per_token_stats.get((selection.market_id, selection.token_id)) or {}).get("row_count") or 0),
            "snapshot_events": int((per_token_stats.get((selection.market_id, selection.token_id)) or {}).get("snapshot_events") or 0),
            "price_change_events": int((per_token_stats.get((selection.market_id, selection.token_id)) or {}).get("price_change_events") or 0),
            "first_event_ts_ms": int((per_token_stats.get((selection.market_id, selection.token_id)) or {}).get("first_event_ts_ms") or 0),
            "last_event_ts_ms": int((per_token_stats.get((selection.market_id, selection.token_id)) or {}).get("last_event_ts_ms") or 0),
            "missing_hour_count": missing_hour_count,
            "data_version": PMXT_L2_EVENT_DATA_VERSION,
            "build_tag": build_tag,
        }
        for selection in selections
    ]
    _insert_json_rows(client, coverage_table, rows)
    return len(rows)


def _normalized_selections(selections: Sequence[PmxtL2ReplaySelection]) -> list[PmxtL2ReplaySelection]:
    deduped: dict[tuple[int, str, str], PmxtL2ReplaySelection] = {}
    for selection in selections:
        token_id = str(selection.token_id or "").strip().lower()
        condition_id = str(selection.condition_id or "").strip()
        if not token_id or not condition_id:
            continue
        key = (int(selection.market_id or 0), condition_id, token_id)
        deduped[key] = PmxtL2ReplaySelection(
            market_id=int(selection.market_id or 0),
            condition_id=condition_id,
            token_id=token_id,
            token_side=str(selection.token_side or "").upper(),
            market_slug=str(selection.market_slug or ""),
            market_title=str(selection.market_title or ""),
        )
    return list(deduped.values())


def _iter_hours(start_hour: datetime, end_hour: datetime) -> Iterable[datetime]:
    current = _ensure_hour(start_hour)
    end = _ensure_hour(end_hour)
    while current <= end:
        yield current
        current += timedelta(hours=1)


def _hour_count(start_hour: datetime, end_hour: datetime) -> int:
    return int((_ensure_hour(end_hour) - _ensure_hour(start_hour)).total_seconds() // 3600) + 1


def _ensure_hour(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def _clickhouse_datetime(value: datetime) -> str:
    return _ensure_hour(value).strftime("%Y-%m-%d %H:%M:%S")


def _clickhouse_datetime_ms(value_ms: int) -> str:
    dt = datetime.fromtimestamp(int(value_ms) / 1000, tz=UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _decimal_text(value: Any) -> str:
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    return str(value)


def _datetime_from_ms(value_ms: int) -> datetime:
    return datetime.fromtimestamp(int(value_ms) / 1000, tz=UTC)


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _book_levels(value: Any) -> tuple[BookLevel, ...]:
    if not isinstance(value, list):
        return ()
    levels: list[BookLevel] = []
    for item in value:
        if isinstance(item, Mapping):
            price = _decimal_or_none(item.get("price"))
            size = _decimal_or_none(item.get("size"))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            price = _decimal_or_none(item[0])
            size = _decimal_or_none(item[1])
        else:
            continue
        if price is None or size is None:
            continue
        levels.append(BookLevel(price=price, size=size))
    return tuple(levels)


def _execution_side(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if text in {"bid", "buy"}:
        return "BUY"
    if text in {"ask", "sell"}:
        return "SELL"
    return None


def _quote(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _pairs_sql(selections: Sequence[PmxtL2ReplaySelection]) -> str:
    return ", ".join(f"({int(item.market_id)}, {_quote(item.token_id)})" for item in selections)


def _empty_result(table: str, coverage_table: str, from_hour: datetime, to_hour: datetime) -> PmxtL2EventReplayBackfillResult:
    return PmxtL2EventReplayBackfillResult(
        table=table,
        coverage_table=coverage_table,
        token_count=0,
        from_hour=from_hour.isoformat(),
        to_hour=to_hour.isoformat(),
        expected_hours=_hour_count(from_hour, to_hour),
        local_hours=0,
        files_read=0,
        inserted_rows=0,
        snapshot_events=0,
        price_change_events=0,
        skipped_delta_before_snapshot=0,
        parse_errors=0,
        first_event_ts_ms=None,
        last_event_ts_ms=None,
        elapsed_sec=0.0,
        coverage_rows=0,
        missing_hours=[],
    )
