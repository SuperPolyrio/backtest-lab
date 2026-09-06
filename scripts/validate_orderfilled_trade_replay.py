#!/usr/bin/env python3
"""Validate materialized tick-level OrderFilled replay rows."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from quant.core.db import ClickHouseClient, postgres_connection, safe_identifier
from quant.backtest.event_stream import build_backtest_event_stream, build_raw_trade_tick_report
from quant.backtest.l2_replay_consistency import (
    build_l2_replay_consistency_report,
    l2_replay_consistency_to_markdown,
)
from quant.backtest.runners.trade_replay_store import (
    backfill_orderfilled_trade_replay,
    load_orderfilled_trade_replay_coverage,
    load_orderfilled_trade_replay_rows,
)
from scripts.validate_pmxt_l2_raw import (
    TokenSelection,
    build_pmxt_orderfilled_alignment_report,
    classify_schema,
    iter_matching_rows,
    iter_pmxt_paths,
    orderfilled_l2_alignment_report_payload,
    parse_hour,
    timestamp_to_ms,
)

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - only needed for --pmxt-root
    pq = None


def discover_orderfilled_trade_replay_candidate(
    conn: Any,
    *,
    min_orderfilled_rows: int = 25,
    window_rows: int = 1000,
    token_side: str = "YES",
) -> dict[str, Any]:
    """Find a bounded materialized-price candidate for real tick replay validation."""

    side = str(token_side or "YES").upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                e.market_id,
                e.market_slug,
                m.condition_id,
                e.token_yes_id AS price_token_id,
                COALESCE(NULLIF(m.token_id_hex, ''), m.token_id, e.token_yes_id) AS replay_token_id,
                m.token_id AS metadata_token_id,
                m.token_id_hex,
                %s AS token_side,
                e.latest_block,
                e.orderfilled_rows
            FROM quant.market_event_members e
            JOIN quant.market_token_metadata m
              ON m.market_id = e.market_id
             AND m.token_side = %s
            WHERE e.token_yes_id IS NOT NULL
              AND e.latest_block IS NOT NULL
              AND e.orderfilled_rows >= %s
            ORDER BY e.latest_block DESC, e.orderfilled_rows DESC, e.market_id ASC
            LIMIT 1
            """,
            (side, side, int(min_orderfilled_rows)),
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError("no OrderFilled trade replay candidate found in materialized market_event_members")
    candidate = dict(row)
    price_token_id = str(candidate.get("price_token_id") or candidate.get("metadata_token_id") or "").strip()
    if not price_token_id:
        raise RuntimeError("auto-discovered candidate is missing price token_id")
    window = _recent_block_window_for_price_token(conn, token_id=price_token_id, window_rows=window_rows)
    replay_token_id = str(candidate.get("replay_token_id") or price_token_id).strip().lower()
    return {
        "market_id": int(candidate["market_id"]),
        "market_slug": str(candidate.get("market_slug") or ""),
        "condition_id": str(candidate.get("condition_id") or ""),
        "price_token_id": price_token_id,
        "token_id": replay_token_id,
        "token_side": side,
        "from_block": int(window["from_block"]),
        "to_block": int(window["to_block"]),
        "price_rows": int(window["rows"]),
        "latest_block": int(candidate.get("latest_block") or 0),
        "orderfilled_rows": int(candidate.get("orderfilled_rows") or 0),
        "source": "market_event_members+market_token_metadata",
    }


def _recent_block_window_for_price_token(conn: Any, *, token_id: str, window_rows: int) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MIN(block_number) AS from_block,
                   MAX(block_number) AS to_block,
                   COUNT(*) AS rows
            FROM (
                SELECT block_number
                FROM quant.market_token_block_close
                WHERE token_id = %s
                ORDER BY block_number DESC
                LIMIT %s
            ) recent
            """,
            (str(token_id), int(window_rows)),
        )
        row = cur.fetchone()
    rows = int((row or {}).get("rows") or 0)
    if rows <= 0:
        raise RuntimeError("auto-discovered candidate has no materialized block-close rows")
    return {
        "from_block": int((row or {}).get("from_block")),
        "to_block": int((row or {}).get("to_block")),
        "rows": rows,
    }


def build_orderfilled_trade_replay_validation_report(
    rows: Sequence[Mapping[str, Any]],
    *,
    market_id: int,
    token_id: str,
    from_block: int,
    to_block: int,
    coverage: Sequence[Mapping[str, Any]] | None = None,
    backfill_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    events = build_backtest_event_stream(raw_orderfilled_events=[dict(row) for row in rows])
    tick_report = build_raw_trade_tick_report(events)
    loaded_count = len(rows)
    coverage_rows = list(coverage or [])
    missing: list[str] = []
    if not coverage_rows:
        missing.append("coverage")
    if loaded_count <= 0:
        missing.append("rows")
    if str(tick_report.get("status") or "") != "ready":
        missing.append("raw_trade_tick_report")
    if str(tick_report.get("canonical_fill_key_coverage_pct") or "0") != "100":
        missing.append("canonical_fill_key_coverage")
    if str(tick_report.get("maker_taker_side_coverage_pct") or "0") != "100":
        missing.append("maker_taker_side_coverage")
    if str(tick_report.get("block_context_coverage_pct") or "0") != "100":
        missing.append("block_context_coverage")
    status = "ready" if not missing else "review" if loaded_count else "missing"
    return {
        "status": status,
        "schema_version": "orderfilled_trade_replay_validation_v1",
        "reason": "materialized OrderFilled ticks are replayable as raw trade ticks" if status == "ready" else "materialized OrderFilled tick replay needs review",
        "market_id": int(market_id),
        "token_id": str(token_id).lower(),
        "from_block": int(from_block),
        "to_block": int(to_block),
        "loaded_row_count": loaded_count,
        "coverage_row_count": len(coverage_rows),
        "coverage": coverage_rows,
        "backfill_result": dict(backfill_result or {}),
        "raw_trade_tick_report": tick_report,
        "missing": missing,
    }


def build_orderfilled_pmxt_alignment_validation_report(
    orderfilled_rows: Sequence[Mapping[str, Any]],
    *,
    pmxt_rows: Sequence[Mapping[str, Any]],
    schema_kind: str,
    selection: TokenSelection,
    max_lag_ms: int = 60_000,
    depth_levels: int = 5,
    sample_limit: int = 20,
) -> dict[str, Any]:
    alignment = build_pmxt_orderfilled_alignment_report(
        [dict(row) for row in pmxt_rows],
        schema_kind=schema_kind,
        selection=selection,
        orderfilled_rows=[dict(row) for row in orderfilled_rows],
        max_lag_ms=max_lag_ms,
        depth_levels=depth_levels,
        sample_limit=sample_limit,
    )
    payload = orderfilled_l2_alignment_report_payload(alignment)
    payload["schema_version"] = "orderfilled_pmxt_l2_alignment_validation_v1"
    payload["reason"] = (
        "PMXT L2 book state is aligned with OrderFilled fill evidence"
        if payload.get("status") == "ready"
        else "PMXT L2 and OrderFilled fill evidence need review"
    )
    payload["pmxt_schema_kind"] = schema_kind
    payload["l2_replay_consistency"] = build_l2_replay_consistency_report(payload)
    return payload


def orderfilled_trade_replay_validation_to_markdown(report: Mapping[str, Any]) -> str:
    tick = report.get("raw_trade_tick_report") if isinstance(report.get("raw_trade_tick_report"), Mapping) else {}
    lines = [
        f"# OrderFilled Trade Replay Validation: {report.get('status')}",
        "",
        f"- market_id: {report.get('market_id')}",
        f"- token_id: {report.get('token_id')}",
        f"- window: {report.get('from_block')} -> {report.get('to_block')}",
        f"- loaded_rows: {report.get('loaded_row_count', 0)}",
        f"- coverage_rows: {report.get('coverage_row_count', 0)}",
        f"- trade_ticks: {tick.get('trade_tick_count', 0)}",
        f"- blocks: {tick.get('block_count', 0)}",
        f"- canonical_key_coverage: {tick.get('canonical_fill_key_coverage_pct', '0')}%",
        f"- maker_taker_side_coverage: {tick.get('maker_taker_side_coverage_pct', '0')}%",
        f"- block_context_coverage: {tick.get('block_context_coverage_pct', '0')}%",
    ]
    missing = report.get("missing") or []
    if missing:
        lines.extend(["", "## Missing / Review"])
        lines.extend(f"- {item}" for item in missing)
    alignment = report.get("pmxt_l2_alignment") if isinstance(report.get("pmxt_l2_alignment"), Mapping) else {}
    if alignment:
        lines.extend(
            [
                "",
                f"## PMXT L2 Alignment: {alignment.get('status')}",
                f"- pmxt_rows: {alignment.get('pmxt_rows_seen', 0)}",
                f"- pmxt_events: {alignment.get('pmxt_matched_events', 0)} matched / {alignment.get('pmxt_applied_events', 0)} applied",
                f"- orderfilled_rows: {alignment.get('orderfilled_rows_matched', 0)} matched",
                f"- aligned: {alignment.get('aligned_count', 0)} ({alignment.get('alignment_pct', '0')}%)",
                f"- stale_l2: {alignment.get('stale_l2_count', 0)}",
                f"- missing_l2_before_fill: {alignment.get('missing_l2_before_fill_count', 0)}",
                f"- price_outside_spread: {alignment.get('price_outside_spread_count', 0)}",
            ]
        )
        consistency = alignment.get("l2_replay_consistency") if isinstance(alignment.get("l2_replay_consistency"), Mapping) else {}
        if consistency:
            lines.extend(["", l2_replay_consistency_to_markdown(consistency)])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-id", type=int, default=None)
    parser.add_argument("--token-id", default=None, help="ClickHouse replay token id; use token_id_hex when available.")
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--limit", type=int, default=250_000)
    parser.add_argument("--backfill", action="store_true", help="Build the materialized tick replay cache before validating.")
    parser.add_argument("--force", action="store_true", help="Force a new ReplacingMergeTree version when backfilling.")
    parser.add_argument("--build-tag", default="manual_validation")
    parser.add_argument("--auto-discover", action="store_true", help="Pick a recent materialized market/token/window from Postgres.")
    parser.add_argument("--window-rows", type=int, default=1000, help="Recent block-close rows used for --auto-discover window.")
    parser.add_argument("--min-orderfilled-rows", type=int, default=25, help="Minimum materialized orderfilled rows for --auto-discover.")
    parser.add_argument("--token-side", default="YES", choices=("YES", "NO"), help="Outcome side used for --auto-discover.")
    parser.add_argument("--pmxt-root", type=Path, default=None, help="Optional local PMXT raw mirror root for L2-fill alignment.")
    parser.add_argument("--condition-id", default=None, help="PMXT condition id for --pmxt-root. Auto-discovered when possible.")
    parser.add_argument("--price-token-id", default=None, help="PMXT asset/token id for --pmxt-root. Defaults to auto-discovered price token.")
    parser.add_argument("--market-slug", default=None, help="Market slug used in PMXT alignment metadata when not auto-discovered.")
    parser.add_argument("--pmxt-max-hours", type=int, default=24, help="Safety cap for PMXT hour files read during alignment.")
    parser.add_argument("--pmxt-batch-size", type=int, default=50_000)
    parser.add_argument("--max-lag-ms", type=int, default=60_000)
    parser.add_argument("--depth-levels", type=int, default=5)
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    args = parser.parse_args(argv)

    candidate: dict[str, Any] = {}
    market_id = args.market_id
    token_id = args.token_id
    from_block = args.from_block
    to_block = args.to_block
    if args.auto_discover:
        with postgres_connection(readonly=True) as conn:
            candidate = discover_orderfilled_trade_replay_candidate(
                conn,
                min_orderfilled_rows=args.min_orderfilled_rows,
                window_rows=args.window_rows,
                token_side=args.token_side,
            )
        market_id = int(candidate["market_id"])
        token_id = str(candidate["token_id"])
        from_block = int(candidate["from_block"])
        to_block = int(candidate["to_block"])
    if market_id is None or not token_id or from_block is None or to_block is None:
        raise SystemExit("--market-id, --token-id, --from-block and --to-block are required unless --auto-discover is used")

    pairs = [(int(market_id), str(token_id))]
    backfill_result = None
    if args.backfill:
        backfill_result = backfill_orderfilled_trade_replay(
            pairs,
            from_block=int(from_block),
            to_block=int(to_block),
            force=args.force,
            build_tag=args.build_tag,
        ).as_dict()
    coverage = load_orderfilled_trade_replay_coverage(pairs, from_block=int(from_block), to_block=int(to_block))
    rows = load_orderfilled_trade_replay_rows(pairs, from_block=int(from_block), to_block=int(to_block), limit=args.limit)
    pmxt_selection = _alignment_selection_from_args(args, candidate=candidate, market_id=int(market_id), replay_token_id=str(token_id))
    if args.pmxt_root is not None:
        rows = enrich_orderfilled_rows_with_block_timestamps(rows)
    alignment_rows = rows_for_pmxt_alignment(rows, pmxt_selection)
    report = build_orderfilled_trade_replay_validation_report(
        rows,
        market_id=int(market_id),
        token_id=str(token_id),
        from_block=int(from_block),
        to_block=int(to_block),
        coverage=coverage,
        backfill_result=backfill_result,
    )
    if candidate:
        report["candidate"] = candidate
    if args.pmxt_root is not None:
        if pmxt_selection is None:
            report["pmxt_l2_alignment"] = {
                "status": "missing",
                "reason": "PMXT alignment requires condition_id and price_token_id/PMXT token id",
            }
        else:
            pmxt_rows, schema_kind = load_pmxt_rows_for_orderfilled_alignment(
                args.pmxt_root,
                selection=pmxt_selection,
                orderfilled_rows=alignment_rows,
                max_hours=max(1, args.pmxt_max_hours),
                batch_size=max(1, args.pmxt_batch_size),
            )
            report["pmxt_l2_alignment"] = build_orderfilled_pmxt_alignment_validation_report(
                alignment_rows,
                pmxt_rows=pmxt_rows,
                schema_kind=schema_kind,
                selection=pmxt_selection,
                max_lag_ms=args.max_lag_ms,
                depth_levels=args.depth_levels,
                sample_limit=args.sample_limit,
            )
    if args.format == "json":
        print(json.dumps(report, default=str, ensure_ascii=False, sort_keys=True, indent=2))
    else:
        print(orderfilled_trade_replay_validation_to_markdown(report))
    return 0 if report["status"] == "ready" else 1


def rows_for_pmxt_alignment(
    rows: Sequence[Mapping[str, Any]],
    selection: TokenSelection | None,
) -> list[dict[str, Any]]:
    """Convert replay-token rows to PMXT asset-id rows for L2 alignment.

    ClickHouse replay can use token_id_hex for efficient raw OrderFilled lookup,
    while PMXT uses the decimal CLOB asset id.  Alignment is about market/asset
    evidence, so keep the replay token as provenance and expose PMXT's token id
    in the field consumed by the L2 alignment primitive.
    """

    output = [dict(row) for row in rows]
    if selection is None or not selection.token_id:
        return output
    for row in output:
        row.setdefault("replay_token_id", row.get("token_id"))
        row["token_id"] = selection.token_id
        row.setdefault("pmxt_asset_id", selection.token_id)
    return output


def _alignment_selection_from_args(
    args: argparse.Namespace,
    *,
    candidate: Mapping[str, Any],
    market_id: int,
    replay_token_id: str,
) -> TokenSelection | None:
    condition_id = str(args.condition_id or candidate.get("condition_id") or "").strip()
    token_id = str(args.price_token_id or candidate.get("price_token_id") or "").strip()
    if not token_id and not candidate:
        token_id = str(replay_token_id or "").strip()
    if not condition_id or not token_id:
        return None
    return TokenSelection(
        condition_id=condition_id,
        token_id=token_id,
        token_side=str(args.token_side or candidate.get("token_side") or "YES").upper(),
        market_id=int(candidate.get("market_id") or market_id or 0),
        market_slug=str(args.market_slug or candidate.get("market_slug") or "") or None,
        market_title=str(candidate.get("market_title") or "") or None,
    )


def enrich_orderfilled_rows_with_block_timestamps(
    rows: Sequence[Mapping[str, Any]],
    *,
    client: ClickHouseClient | None = None,
    overwrite: bool = False,
    interpolate: bool = False,
) -> list[dict[str, Any]]:
    enriched = [dict(row) for row in rows]
    target_blocks = sorted({
        int(row.get("block_number") or 0)
        for row in enriched
        if int(row.get("block_number") or 0) > 0 and (overwrite or not _row_has_timestamp(row))
    })
    if not target_blocks:
        return enriched
    timestamps = (
        load_block_timestamps_with_interpolation(target_blocks, client=client)
        if interpolate
        else load_block_timestamps(target_blocks, client=client)
    )
    for row in enriched:
        block_number = int(row.get("block_number") or 0)
        timestamp = timestamps.get(block_number)
        if timestamp and (overwrite or not _row_has_timestamp(row)):
            row["block_timestamp"] = timestamp
            row["block_timestamp_source"] = "block_timestamps_interpolated" if interpolate else "block_timestamps"
    return enriched


def load_block_timestamps(
    block_numbers: Iterable[int],
    *,
    client: ClickHouseClient | None = None,
    chunk_size: int = 10_000,
) -> dict[int, str]:
    blocks = sorted({int(block) for block in block_numbers if int(block) > 0})
    if not blocks:
        return {}
    ch = client or ClickHouseClient()
    result: dict[int, str] = {}
    table_name = safe_identifier("block_timestamps")
    for idx in range(0, len(blocks), max(1, int(chunk_size))):
        chunk = blocks[idx:idx + max(1, int(chunk_size))]
        block_sql = ",".join(str(block) for block in chunk)
        rows = ch.query_json_rows(
            f"""
            SELECT
                block_number,
                formatDateTime(block_time, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS block_timestamp
            FROM {table_name}
            WHERE block_number IN ({block_sql})
            ORDER BY block_number ASC
            """,
            timeout_seconds=120,
        )
        for row in rows:
            result[int(row["block_number"])] = str(row["block_timestamp"])
    return result


def load_block_timestamps_with_interpolation(
    block_numbers: Iterable[int],
    *,
    client: ClickHouseClient | None = None,
) -> dict[int, str]:
    blocks = sorted({int(block) for block in block_numbers if int(block) > 0})
    if not blocks:
        return {}
    exact = load_block_timestamps(blocks, client=client)
    missing = [block for block in blocks if block not in exact]
    if not missing:
        return exact
    anchors = load_block_timestamp_anchors(min(blocks), max(blocks), client=client)
    if len(anchors) < 2:
        return exact
    anchor_blocks = [block for block, _ts_ms in anchors]
    anchor_ms = [ts_ms for _block, ts_ms in anchors]
    result = dict(exact)
    for block in missing:
        interpolated = _interpolate_block_timestamp_ms(block, anchor_blocks, anchor_ms)
        if interpolated is not None:
            result[block] = _ms_to_iso(interpolated)
    return result


def load_block_timestamp_anchors(
    from_block: int,
    to_block: int,
    *,
    client: ClickHouseClient | None = None,
) -> list[tuple[int, int]]:
    start = int(from_block)
    end = int(to_block)
    if start <= 0 or end < start:
        return []
    ch = client or ClickHouseClient()
    table_name = safe_identifier("block_timestamps")
    rows = ch.query_json_rows(
        f"""
        SELECT
            block_number,
            toUnixTimestamp64Milli(block_time) AS ts_ms
        FROM {table_name}
        WHERE block_number BETWEEN {start} AND {end}
        ORDER BY block_number ASC
        """,
        timeout_seconds=120,
    )
    anchors: list[tuple[int, int]] = []
    for row in rows:
        block = int(row.get("block_number") or 0)
        ts_ms = int(row.get("ts_ms") or 0)
        if block > 0 and ts_ms > 0:
            anchors.append((block, ts_ms))
    return anchors


def _interpolate_block_timestamp_ms(block: int, anchor_blocks: Sequence[int], anchor_ms: Sequence[int]) -> int | None:
    if not anchor_blocks:
        return None
    if block < anchor_blocks[0] or block > anchor_blocks[-1]:
        return None
    lo = 0
    hi = len(anchor_blocks) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        mid_block = anchor_blocks[mid]
        if mid_block == block:
            return int(anchor_ms[mid])
        if mid_block < block:
            lo = mid + 1
        else:
            hi = mid - 1
    left = hi
    right = lo
    if left < 0 or right >= len(anchor_blocks):
        return None
    left_block = anchor_blocks[left]
    right_block = anchor_blocks[right]
    if right_block <= left_block:
        return int(anchor_ms[left])
    ratio = (int(block) - left_block) / (right_block - left_block)
    return int(round(anchor_ms[left] + ratio * (anchor_ms[right] - anchor_ms[left])))


def _ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_pmxt_rows_for_orderfilled_alignment(
    pmxt_root: Path,
    *,
    selection: TokenSelection,
    orderfilled_rows: Sequence[Mapping[str, Any]],
    max_hours: int,
    batch_size: int,
) -> tuple[list[dict[str, Any]], str]:
    if pq is None:
        raise RuntimeError("pyarrow is required for --pmxt-root alignment")
    hours = _orderfilled_pmxt_hours(orderfilled_rows)
    if not hours:
        return [], "fixed"
    start_hour = min(hours) - timedelta(hours=1)
    end_hour = max(hours)
    paths = list(iter_pmxt_paths(pmxt_root, start_hour=start_hour, end_hour=end_hour, max_hours=max_hours))
    if len(paths) > max_hours:
        paths = paths[:max_hours]
    loaded: list[dict[str, Any]] = []
    schema_kind = "fixed"
    for path in paths:
        parquet_file = pq.ParquetFile(path)
        schema_kind = classify_schema(parquet_file.schema_arrow.names)
        if schema_kind == "unsupported":
            continue
        for row in iter_matching_rows(
            parquet_file,
            schema_kind=schema_kind,
            selection=selection,
            batch_size=batch_size,
        ):
            loaded.append(row)
    return loaded, schema_kind


def _orderfilled_pmxt_hours(rows: Sequence[Mapping[str, Any]]) -> list[datetime]:
    hours: set[datetime] = set()
    for row in rows:
        ts_ms = _row_timestamp_ms(row)
        if ts_ms <= 0:
            continue
        hour = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).replace(minute=0, second=0, microsecond=0)
        hours.add(hour)
    return sorted(hours)


def _row_timestamp_ms(row: Mapping[str, Any]) -> int:
    for key in ("event_ts_ms", "timestamp_ms", "timestamp", "block_timestamp", "block_time", "created_at"):
        if row.get(key) not in (None, ""):
            return timestamp_to_ms(row.get(key))
    return 0


def _row_has_timestamp(row: Mapping[str, Any]) -> bool:
    return _row_timestamp_ms(row) > 0


if __name__ == "__main__":
    raise SystemExit(main())
try:
    from datetime import UTC
except ImportError:  # pragma: no cover - Python < 3.11 compatibility
    from datetime import timezone

    UTC = timezone.utc
