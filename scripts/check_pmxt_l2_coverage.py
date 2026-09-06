#!/usr/bin/env python3
"""Check PMXT L2 raw archive coverage for local block-close markets.

This script answers a narrow question:

    For tokens that have orderfilled block-close rows in a time window, do local
    PMXT raw parquet files contain matching condition_id/token_id book events?

It does not materialize order books and it does not write database rows. It scans
each PMXT hour file once, counts target token events, and writes JSON/CSV coverage
reports for follow-up materialization.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.core.db import postgres_connection
from scripts.validate_pmxt_l2_raw import (
    archive_filename_for_hour,
    classify_schema,
    parse_hour,
    timestamp_to_ms,
)

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover - environment guard
    raise SystemExit(
        "pyarrow is required. Use the project env, for example: "
        "conda run -n polyBots python scripts/check_pmxt_l2_coverage.py ..."
    ) from exc


DEFAULT_START_HOUR = "2026-02-21T16"
DEFAULT_REMOTE_BASE_URLS = ("https://r2v2.pmxt.dev", "https://r2.pmxt.dev")
DEPTH_EVENT_TYPES = {"book", "price_change", "book_snapshot"}


@dataclass(frozen=True)
class TargetHour:
    hour: datetime
    market_id: int
    market_slug: str
    market_title: str
    condition_id: str
    token_id: str
    token_side: str
    block_close_rows: int
    first_block: int | None
    last_block: int | None

    @property
    def pair(self) -> tuple[str, str]:
        return self.condition_id, self.token_id

    @property
    def row_key(self) -> tuple[datetime, str, str]:
        return self.hour, self.condition_id, self.token_id


@dataclass
class CoverageRow:
    hour: str
    market_id: int
    market_slug: str
    market_title: str
    condition_id: str
    token_id: str
    token_side: str
    block_close_rows: int
    first_block: int | None
    last_block: int | None
    pmxt_file: str | None = None
    pmxt_schema: str | None = None
    pmxt_file_rows: int = 0
    pmxt_market_count: int | None = None
    pmxt_asset_count: int | None = None
    pmxt_total_events: int = 0
    pmxt_book_events: int = 0
    pmxt_price_change_events: int = 0
    pmxt_last_trade_price_events: int = 0
    first_pmxt_ts: str | None = None
    last_pmxt_ts: str | None = None
    coverage_status: str = "unknown"
    warning: str | None = None


@dataclass
class CoverageSummary:
    pmxt_root: str
    start_hour: str
    end_hour: str
    target_hours: int = 0
    target_markets: int = 0
    target_tokens: int = 0
    pmxt_hours_needed: int = 0
    pmxt_files_found: int = 0
    pmxt_files_missing_local: int = 0
    pmxt_files_read: int = 0
    rows_scanned: int = 0
    status_counts: dict[str, int] = field(default_factory=dict)
    remote_hour_status: dict[str, dict[str, Any]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check local PMXT raw parquet coverage for orderfilled block-close tokens."
    )
    parser.add_argument("--pmxt-root", type=Path, required=True, help="Local PMXT raw mirror root.")
    parser.add_argument("--start-hour", default=DEFAULT_START_HOUR, help="UTC hour, default 2026-02-21T16.")
    parser.add_argument("--end-hour", default=None, help="UTC hour. Defaults to latest block_close hour.")
    parser.add_argument("--market-slug", action="append", default=[], help="Restrict to one or more market slugs.")
    parser.add_argument("--token-side", choices=("YES", "NO", "ALL"), default="ALL")
    parser.add_argument("--market-limit", type=int, default=0, help="Limit markets by block-close row count.")
    parser.add_argument("--token-limit", type=int, default=0, help="Limit tokens by block-close row count.")
    parser.add_argument("--batch-size", type=int, default=500_000)
    parser.add_argument(
        "--check-remote-missing",
        action="store_true",
        help="HEAD-check r2v2/r2 PMXT URLs for locally missing hours.",
    )
    parser.add_argument(
        "--remote-base-url",
        action="append",
        default=[],
        help="Remote base URL for --check-remote-missing. May repeat.",
    )
    parser.add_argument(
        "--fail-on-no-match",
        action="store_true",
        help="Return non-zero when no token-hour is matched. Useful for CI gates.",
    )
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--csv-out", type=Path, default=None)
    args = parser.parse_args()

    start_hour = require_hour(args.start_hour, "--start-hour")
    end_hour = parse_hour(args.end_hour) if args.end_hour else latest_block_close_hour(start_hour)
    if end_hour is None:
        raise SystemExit("No block_close rows found for the requested start.")
    if end_hour < start_hour:
        raise SystemExit("--end-hour must be >= --start-hour.")

    targets = fetch_target_hours(
        start_hour=start_hour,
        end_hour=end_hour,
        market_slugs=tuple(args.market_slug),
        token_side=args.token_side,
        market_limit=max(0, args.market_limit),
        token_limit=max(0, args.token_limit),
    )
    summary = CoverageSummary(
        pmxt_root=str(args.pmxt_root),
        start_hour=start_hour.isoformat(),
        end_hour=end_hour.isoformat(),
        target_hours=len(targets),
        target_markets=len({target.market_id for target in targets}),
        target_tokens=len({target.token_id for target in targets}),
        pmxt_hours_needed=len({target.hour for target in targets}),
    )
    if not targets:
        summary.warnings.append("No block-close target rows found for the requested filters.")
        emit_outputs(summary, [], args.json_out, args.csv_out)
        return 2

    by_hour: dict[datetime, list[TargetHour]] = defaultdict(list)
    for target in targets:
        by_hour[target.hour].append(target)

    rows: list[CoverageRow] = []
    remote_bases = tuple(args.remote_base_url) or DEFAULT_REMOTE_BASE_URLS

    for hour in sorted(by_hour):
        hour_targets = by_hour[hour]
        path = find_pmxt_path(args.pmxt_root, hour)
        if path is None:
            summary.pmxt_files_missing_local += 1
            remote_status = None
            if args.check_remote_missing:
                remote_status = check_remote_hour(hour, remote_bases)
                summary.remote_hour_status[hour_key(hour)] = remote_status
            status = "missing_pmxt_hour"
            if remote_status and any(item.get("exists") for item in remote_status.values()):
                status = "missing_local_pmxt_hour"
            for target in hour_targets:
                rows.append(base_row(target, pmxt_file=None, status=status))
            continue

        summary.pmxt_files_found += 1
        hour_rows = scan_hour_file(path, hour_targets, batch_size=max(1, args.batch_size))
        summary.pmxt_files_read += 1
        summary.rows_scanned += sum(row.pmxt_file_rows for row in hour_rows[:1])
        rows.extend(hour_rows)

    status_counts = Counter(row.coverage_status for row in rows)
    summary.status_counts = dict(sorted(status_counts.items()))
    emit_outputs(summary, rows, args.json_out, args.csv_out)
    if args.fail_on_no_match and not status_counts.get("matched", 0):
        return 1
    return 0


def require_hour(raw: str, label: str) -> datetime:
    parsed = parse_hour(raw)
    if parsed is None:
        raise SystemExit(f"{label} is required")
    return parsed


def latest_block_close_hour(start_hour: datetime) -> datetime | None:
    with postgres_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT max(date_trunc('hour', block_timestamp)) AS max_hour
                FROM quant.market_token_block_close
                WHERE block_timestamp >= %s
                """,
                (start_hour,),
            )
            row = cur.fetchone()
    value = row["max_hour"] if row else None
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def fetch_target_hours(
    *,
    start_hour: datetime,
    end_hour: datetime,
    market_slugs: tuple[str, ...],
    token_side: str,
    market_limit: int,
    token_limit: int,
) -> list[TargetHour]:
    filters = [
        "b.block_timestamp >= %s",
        "b.block_timestamp < %s",
        "m.condition_id IS NOT NULL",
        "m.token_id IS NOT NULL",
    ]
    params: list[Any] = [start_hour, end_hour + timedelta(hours=1)]
    if market_slugs:
        filters.append("b.market_slug = ANY(%s)")
        params.append(list(market_slugs))
    if token_side != "ALL":
        filters.append("upper(b.token_side) = %s")
        params.append(token_side)
    where_sql = " AND ".join(filters)

    market_limit_sql = ""
    if market_limit:
        market_limit_sql = """
        , ranked_markets AS (
            SELECT market_id
            FROM base
            GROUP BY market_id
            ORDER BY sum(block_close_rows) DESC
            LIMIT %s
        )
        """
        params.append(market_limit)

    token_limit_sql = ""
    if token_limit:
        token_limit_sql = """
        , ranked_tokens AS (
            SELECT token_id
            FROM base
            GROUP BY token_id
            ORDER BY sum(block_close_rows) DESC
            LIMIT %s
        )
        """
        params.append(token_limit)

    outer_filters = []
    if market_limit:
        outer_filters.append("market_id IN (SELECT market_id FROM ranked_markets)")
    if token_limit:
        outer_filters.append("token_id IN (SELECT token_id FROM ranked_tokens)")
    outer_where = f"WHERE {' AND '.join(outer_filters)}" if outer_filters else ""

    sql = f"""
        WITH base AS (
            SELECT
                date_trunc('hour', b.block_timestamp) AS hour,
                b.market_id,
                coalesce(b.market_slug, m.market_slug, '') AS market_slug,
                coalesce(m.market_title, '') AS market_title,
                m.condition_id,
                b.token_id,
                upper(coalesce(b.token_side, m.token_side, '')) AS token_side,
                count(*) AS block_close_rows,
                min(b.block_number) AS first_block,
                max(b.block_number) AS last_block
            FROM quant.market_token_block_close b
            JOIN quant.market_token_metadata m ON m.token_id = b.token_id
            WHERE {where_sql}
            GROUP BY 1,2,3,4,5,6,7
        )
        {market_limit_sql}
        {token_limit_sql}
        SELECT *
        FROM base
        {outer_where}
        ORDER BY hour, market_id, token_side, token_id
    """

    with postgres_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())

    targets: list[TargetHour] = []
    for row in rows:
        hour = row["hour"]
        if hour.tzinfo is None:
            hour = hour.replace(tzinfo=UTC)
        targets.append(
            TargetHour(
                hour=hour.astimezone(UTC).replace(minute=0, second=0, microsecond=0),
                market_id=int(row["market_id"]),
                market_slug=str(row["market_slug"] or ""),
                market_title=str(row["market_title"] or ""),
                condition_id=str(row["condition_id"]),
                token_id=str(row["token_id"]),
                token_side=str(row["token_side"] or ""),
                block_close_rows=int(row["block_close_rows"] or 0),
                first_block=int(row["first_block"]) if row["first_block"] is not None else None,
                last_block=int(row["last_block"]) if row["last_block"] is not None else None,
            )
        )
    return targets


def find_pmxt_path(root: Path, hour: datetime) -> Path | None:
    filename = archive_filename_for_hour(hour)
    for candidate in (root / filename, root / f"{hour:%Y/%m/%d}" / filename):
        if candidate.exists():
            return candidate
    return None


def scan_hour_file(path: Path, targets: list[TargetHour], *, batch_size: int) -> list[CoverageRow]:
    target_by_pair = {target.pair: target for target in targets}
    pairs = set(target_by_pair)
    conditions = {condition for condition, _ in pairs}
    tokens = {token for _, token in pairs}
    counts: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    first_ts: dict[tuple[str, str], int] = {}
    last_ts: dict[tuple[str, str], int] = {}
    warning: str | None = None

    try:
        parquet_file = pq.ParquetFile(path)
        schema_kind = classify_schema(parquet_file.schema_arrow.names)
    except Exception as exc:
        return [
            base_row(target, pmxt_file=str(path), status="pmxt_read_error", warning=str(exc))
            for target in targets
        ]

    if schema_kind == "unsupported":
        return [
            base_row(
                target,
                pmxt_file=str(path),
                status="unsupported_pmxt_schema",
                warning=str(parquet_file.schema_arrow.names),
            )
            for target in targets
        ]

    pmxt_market_count: int | None = None
    pmxt_asset_count: int | None = None
    file_rows = int(parquet_file.metadata.num_rows or 0)

    if schema_kind == "fixed":
        pmxt_market_count, pmxt_asset_count = scan_fixed_counts(
            parquet_file,
            pairs=pairs,
            conditions=conditions,
            tokens=tokens,
            counts=counts,
            first_ts=first_ts,
            last_ts=last_ts,
            batch_size=batch_size,
        )
    else:
        warning = "payload schema uses substring token matching; counts are coverage estimates"
        scan_payload_counts(
            parquet_file,
            pairs=pairs,
            conditions=conditions,
            tokens=tokens,
            counts=counts,
            first_ts=first_ts,
            last_ts=last_ts,
            batch_size=batch_size,
        )

    rows: list[CoverageRow] = []
    for target in targets:
        pair_counts = counts[target.pair]
        total = sum(pair_counts.values())
        if total <= 0:
            status = "missing_token_events"
        elif pair_counts.get("book", 0) <= 0 and pair_counts.get("book_snapshot", 0) <= 0:
            status = "no_book_snapshot"
        else:
            status = "matched"
        row = base_row(target, pmxt_file=str(path), status=status, warning=warning)
        row.pmxt_schema = schema_kind
        row.pmxt_file_rows = file_rows
        row.pmxt_market_count = pmxt_market_count
        row.pmxt_asset_count = pmxt_asset_count
        row.pmxt_total_events = int(total)
        row.pmxt_book_events = int(pair_counts.get("book", 0) + pair_counts.get("book_snapshot", 0))
        row.pmxt_price_change_events = int(pair_counts.get("price_change", 0))
        row.pmxt_last_trade_price_events = int(pair_counts.get("last_trade_price", 0))
        if target.pair in first_ts:
            row.first_pmxt_ts = ms_to_iso(first_ts[target.pair])
            row.last_pmxt_ts = ms_to_iso(last_ts[target.pair])
        rows.append(row)
    return rows


def scan_fixed_counts(
    parquet_file: pq.ParquetFile,
    *,
    pairs: set[tuple[str, str]],
    conditions: set[str],
    tokens: set[str],
    counts: dict[tuple[str, str], Counter[str]],
    first_ts: dict[tuple[str, str], int],
    last_ts: dict[tuple[str, str], int],
    batch_size: int,
) -> tuple[int, int]:
    all_markets: set[str] = set()
    all_assets: set[str] = set()
    columns = ["timestamp", "market", "event_type", "asset_id"]
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
        if batch.num_rows == 0:
            continue
        market_col = batch.column("market")
        asset_col = batch.column("asset_id")
        all_markets.update(str_value(value) for value in pc.unique(market_col).to_pylist() if value is not None)
        all_assets.update(str_value(value) for value in pc.unique(asset_col).to_pylist() if value is not None)

        market_mask = isin_with_column_type(market_col, conditions)
        asset_mask = isin_with_column_type(asset_col, tokens)
        mask = pc.and_(pc.fill_null(market_mask, False), pc.fill_null(asset_mask, False))
        filtered = batch.filter(mask)
        if filtered.num_rows == 0:
            continue
        data = filtered.to_pydict()
        for ts, market, event_type, asset_id in zip(
            data["timestamp"],
            data["market"],
            data["event_type"],
            data["asset_id"],
            strict=False,
        ):
            pair = (str_value(market), str_value(asset_id))
            if pair not in pairs:
                continue
            event = str(event_type or "")
            counts[pair][event] += 1
            ts_ms = timestamp_to_ms(ts)
            if ts_ms:
                first_ts[pair] = ts_ms if pair not in first_ts else min(first_ts[pair], ts_ms)
                last_ts[pair] = ts_ms if pair not in last_ts else max(last_ts[pair], ts_ms)
    return len(all_markets), len(all_assets)


def scan_payload_counts(
    parquet_file: pq.ParquetFile,
    *,
    pairs: set[tuple[str, str]],
    conditions: set[str],
    tokens: set[str],
    counts: dict[tuple[str, str], Counter[str]],
    first_ts: dict[tuple[str, str], int],
    last_ts: dict[tuple[str, str], int],
    batch_size: int,
) -> None:
    columns = ["market_id", "update_type", "data"]
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=columns):
        if batch.num_rows == 0:
            continue
        market_mask = isin_with_column_type(batch.column("market_id"), conditions)
        update_mask = pc.is_in(
            batch.column("update_type"),
            value_set=pa.array(["book_snapshot", "price_change", "last_trade_price"]),
        )
        filtered = batch.filter(pc.and_(pc.fill_null(market_mask, False), pc.fill_null(update_mask, False)))
        if filtered.num_rows == 0:
            continue
        data = filtered.to_pydict()
        for market, update_type, payload in zip(
            data["market_id"],
            data["update_type"],
            data["data"],
            strict=False,
        ):
            condition = str_value(market)
            payload_text = str(payload or "")
            for token in tokens:
                pair = (condition, token)
                if pair not in pairs or token not in payload_text:
                    continue
                event = "book" if str(update_type) == "book_snapshot" else str(update_type or "")
                counts[pair][event] += 1
                ts_ms = extract_payload_timestamp_ms(payload_text)
                if ts_ms:
                    first_ts[pair] = ts_ms if pair not in first_ts else min(first_ts[pair], ts_ms)
                    last_ts[pair] = ts_ms if pair not in last_ts else max(last_ts[pair], ts_ms)


def isin_with_column_type(column: Any, values: set[str]) -> Any:
    value_list: list[Any]
    if pa.types.is_binary(column.type) or pa.types.is_fixed_size_binary(column.type):
        value_list = [value.encode("utf-8") for value in values]
    else:
        value_list = list(values)
    return pc.is_in(column, value_set=pa.array(value_list, type=column.type))


def extract_payload_timestamp_ms(payload_text: str) -> int:
    # Avoid full JSON parsing for the coverage path. timestamp_to_ms tolerates
    # ISO strings and integer ns/us/ms values once the small field is extracted.
    for marker in ('"timestamp":', '"ts":'):
        idx = payload_text.find(marker)
        if idx < 0:
            continue
        rest = payload_text[idx + len(marker):].lstrip()
        if rest.startswith('"'):
            end = rest.find('"', 1)
            return timestamp_to_ms(rest[1:end]) if end > 1 else 0
        number = []
        for char in rest:
            if char.isdigit():
                number.append(char)
            else:
                break
        if number:
            return timestamp_to_ms("".join(number))
    return 0


def base_row(
    target: TargetHour,
    *,
    pmxt_file: str | None,
    status: str,
    warning: str | None = None,
) -> CoverageRow:
    return CoverageRow(
        hour=target.hour.isoformat(),
        market_id=target.market_id,
        market_slug=target.market_slug,
        market_title=target.market_title,
        condition_id=target.condition_id,
        token_id=target.token_id,
        token_side=target.token_side,
        block_close_rows=target.block_close_rows,
        first_block=target.first_block,
        last_block=target.last_block,
        pmxt_file=pmxt_file,
        coverage_status=status,
        warning=warning,
    )


def check_remote_hour(hour: datetime, bases: Iterable[str]) -> dict[str, Any]:
    filename = archive_filename_for_hour(hour)
    result: dict[str, Any] = {}
    for base in bases:
        url = f"{base.rstrip('/')}/{filename}"
        request = Request(url, method="HEAD", headers={"User-Agent": "prediction-market-quant/pmxt-coverage"})
        try:
            with urlopen(request, timeout=20) as response:
                result[base] = {
                    "exists": 200 <= response.status < 300,
                    "status": response.status,
                    "content_length": response.headers.get("Content-Length"),
                    "last_modified": response.headers.get("Last-Modified"),
                }
        except HTTPError as exc:
            result[base] = {"exists": False, "status": exc.code}
        except Exception as exc:
            result[base] = {"exists": False, "error": f"{type(exc).__name__}: {exc}"}
    return result


def hour_key(hour: datetime) -> str:
    return f"{hour.astimezone(UTC):%Y-%m-%dT%H}"


def str_value(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def ms_to_iso(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()


def emit_outputs(
    summary: CoverageSummary,
    rows: list[CoverageRow],
    json_out: Path | None,
    csv_out: Path | None,
) -> None:
    payload = {
        "summary": asdict(summary),
        "rows": [asdict(row) for row in rows],
    }
    if json_out is not None:
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    if csv_out is not None:
        csv_out.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = list(asdict(rows[0]).keys()) if rows else list(CoverageRow.__dataclass_fields__.keys())
        with csv_out.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(asdict(row))
    print(json.dumps({"summary": asdict(summary), "json_out": str(json_out) if json_out else None, "csv_out": str(csv_out) if csv_out else None}, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    raise SystemExit(main())
try:
    from datetime import UTC
except ImportError:  # pragma: no cover - Python < 3.11 compatibility
    from datetime import timezone

    UTC = timezone.utc
