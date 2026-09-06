"""Merge independent L2 feeds without hiding simultaneous delivery gaps."""

from __future__ import annotations

import argparse
from bisect import bisect_left
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import time
from typing import Any, Iterable, Mapping, Sequence

import duckdb
import pandas as pd

from quant.core.db import postgres_connection
from .l2_coverage_manifest import coverage_table_sql


PRIMARY_FEED_SOURCE = "polymarket_market_ws_archive"
SECONDARY_FEED_SOURCE = "polymarket_market_ws_archive_gcp_sg_batch"
PRIMARY_COVERAGE_TABLE = "quant.clob_l2_token_hour_coverage"
SECONDARY_COVERAGE_TABLE = "quant.clob_l2_token_hour_coverage_gcp_batch"
ACTIVE_ACTIVE_COVERAGE_TABLE = "quant.clob_l2_active_active_token_hour_coverage"
ACTIVE_ACTIVE_GAP_TABLE = "quant.clob_l2_active_active_gap_intervals"
PRIMARY_COVERAGE_WATERMARK_ID = "coverage-finalizer"
SECONDARY_COVERAGE_WATERMARK_ID = "coverage-finalizer-gcp-batch"
EXECUTION_REDUNDANT_FEED_SOURCE = (
    "polymarket_market_ws_archive_gcp_sg_execution_redundant"
)
EXECUTION_REDUNDANT_COVERAGE_TABLE = (
    "quant.clob_l2_token_hour_coverage_gcp_execution_redundant"
)
EXECUTION_REDUNDANT_COVERAGE_WATERMARK_ID = (
    "coverage-finalizer-gcp-execution-redundant"
)
HOT_STANDBY_FEED_SOURCE = "polymarket_market_ws_archive_gcp_sg_hot_standby"
HOT_STANDBY_PATCH_COVERAGE_TABLE = (
    "quant.clob_l2_token_hour_coverage_gcp_hot_standby_patch"
)
HOT_STANDBY_PATCH_COVERAGE_WATERMARK_ID = (
    "coverage-finalizer-gcp-hot-standby-patch"
)
RAW_A_FEED_SOURCE = "polymarket_market_ws_raw_a"
RAW_A_COVERAGE_TABLE = SECONDARY_COVERAGE_TABLE
RAW_A_COVERAGE_WATERMARK_ID = SECONDARY_COVERAGE_WATERMARK_ID
RAW_B_FEED_SOURCE = "polymarket_market_ws_raw_b"
RAW_B_PATCH_COVERAGE_TABLE = HOT_STANDBY_PATCH_COVERAGE_TABLE
RAW_B_PATCH_COVERAGE_WATERMARK_ID = HOT_STANDBY_PATCH_COVERAGE_WATERMARK_ID
DEFAULT_OUTPUT_DIR = Path("runtime_outputs/lob_l2_active_active_coverage")


@dataclass(frozen=True)
class FeedGapInterval:
    source: str
    asset_id: str
    connection_id: str
    gap_start: datetime
    recovered_at: datetime
    shard_id: int | None = None
    original_gap_start: datetime | None = None
    recovery_kind: str = "exact_marker"

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.recovered_at - self.gap_start).total_seconds())


@dataclass(frozen=True)
class GapOverlap:
    count: int = 0
    seconds: float = 0.0


@dataclass(frozen=True)
class DualFeedGapInterval:
    asset_id: str
    gap_start: datetime
    recovered_at: datetime

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.recovered_at - self.gap_start).total_seconds())


@dataclass(frozen=True)
class ActiveActiveHourSummary:
    hour_start: datetime
    token_count: int
    replay_eligible: int
    complemented: int
    dual_gap_tokens: int
    primary_only_rows: int
    secondary_only_rows: int
    output_path: str
    generated_at: datetime

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["hour_start"] = self.hour_start.isoformat()
        payload["generated_at"] = self.generated_at.isoformat()
        return payload


def validate_active_active_contract(
    *,
    start_at: datetime,
    primary_source: str,
    secondary_source: str,
    primary_coverage_table: str,
    secondary_coverage_table: str,
    primary_watermark_id: str,
    secondary_watermark_id: str,
    primary_retired_at: datetime | None,
    secondary_retired_at: datetime | None,
    secondary_sparse: bool,
    secondary_watermark_optional: bool,
) -> dict[str, Any]:
    """Validate and describe the effective active-active worker contract.

    Legacy historical invocations remain supported.  The production
    post-cutover mode is explicit and pins Source A to the finalized GCP batch
    coverage while treating Source B as a sparse, late-arriving patch table.
    """

    sparse = bool(secondary_sparse)
    optional = bool(secondary_watermark_optional)
    if sparse != optional:
        raise ValueError(
            "secondary_sparse and secondary_watermark_optional must be enabled together"
        )
    mode = "secondary_sparse" if sparse else "dual_watermark"
    if sparse:
        expected_primary = (
            RAW_A_FEED_SOURCE,
            RAW_A_COVERAGE_TABLE,
            RAW_A_COVERAGE_WATERMARK_ID,
        )
        expected_secondary = (
            RAW_B_FEED_SOURCE,
            RAW_B_PATCH_COVERAGE_TABLE,
            RAW_B_PATCH_COVERAGE_WATERMARK_ID,
        )
        actual_primary = (
            str(primary_source),
            str(primary_coverage_table),
            str(primary_watermark_id),
        )
        actual_secondary = (
            str(secondary_source),
            str(secondary_coverage_table),
            str(secondary_watermark_id),
        )
        if actual_primary != expected_primary:
            raise ValueError(
                "secondary_sparse primary contract must be "
                f"source/table/watermark={expected_primary!r}; got {actual_primary!r}"
            )
        if actual_secondary != expected_secondary:
            raise ValueError(
                "secondary_sparse secondary contract must be "
                f"source/table/watermark={expected_secondary!r}; got {actual_secondary!r}"
            )
        retired = _datetime_or_none(primary_retired_at)
        if retired is not None and retired <= _utc(start_at):
            raise ValueError(
                "raw Source A cannot be retired at or before active-active start_at"
            )
    return {
        "mode": mode,
        "start_at": _utc(start_at).isoformat(),
        "primary": {
            "source": str(primary_source),
            "coverage_table": str(primary_coverage_table),
            "watermark_id": str(primary_watermark_id),
            "watermark_required": True,
            "retired_at": (
                _utc(primary_retired_at).isoformat()
                if primary_retired_at is not None
                else None
            ),
        },
        "secondary": {
            "source": str(secondary_source),
            "coverage_table": str(secondary_coverage_table),
            "watermark_id": str(secondary_watermark_id),
            "sparse": sparse,
            "watermark_required": not optional,
            "retired_at": (
                _utc(secondary_retired_at).isoformat()
                if secondary_retired_at is not None
                else None
            ),
        },
    }


def canonical_event_key(row: Mapping[str, Any]) -> tuple[str, ...]:
    """Return a feed-independent identity for one normalized archive row."""

    event_type = _text(row.get("event_type"))
    source = _text(row.get("source"))
    asset_id = _text(row.get("asset_id"))
    event_ts = _timestamp_text(row.get("timestamp"))
    connection_id = _text(row.get("transaction_hash"))
    if event_type in {"connection_gap", "connection_recovered"}:
        return (source, event_type, asset_id, connection_id, event_ts)
    return (
        event_type,
        asset_id,
        event_ts,
        _text(row.get("market")),
        _text(row.get("sequence_in_message")),
        _text(row.get("book_hash")),
        connection_id,
        _text(row.get("bids")),
        _text(row.get("asks")),
        _text(row.get("price")),
        _text(row.get("size")),
        _text(row.get("side")).upper(),
        _text(row.get("best_bid")),
        _text(row.get("best_ask")),
        _text(row.get("fee_rate_bps")),
        _text(row.get("old_tick_size")),
        _text(row.get("new_tick_size")),
    )


def dedupe_active_active_rows(
    rows: pd.DataFrame,
    *,
    source_priority: Sequence[str] = (PRIMARY_FEED_SOURCE, SECONDARY_FEED_SOURCE),
) -> pd.DataFrame:
    """Keep the earliest observation of each upstream event across feeds."""

    if rows.empty:
        return rows.copy()
    result = rows.copy()
    priority = {source: index for index, source in enumerate(source_priority)}
    result["_canonical_key"] = [canonical_event_key(row) for row in result.to_dict("records")]
    result["_source_priority"] = [priority.get(_text(value), len(priority)) for value in result.get("source", "")]
    result["_received_order"] = pd.to_datetime(result["timestamp_received"], utc=True, errors="coerce")
    for column in ("collector_seq", "sequence_in_message"):
        if column not in result:
            result[column] = 0
    result = result.sort_values(
        ["_received_order", "_source_priority", "collector_seq", "sequence_in_message"],
        kind="stable",
        na_position="last",
    )
    result = result.drop_duplicates("_canonical_key", keep="first")
    return result.drop(columns=["_canonical_key", "_source_priority", "_received_order"]).reset_index(drop=True)


def pair_gap_intervals(
    marker_rows: Iterable[Mapping[str, Any]],
    *,
    window_start: datetime,
    window_end: datetime,
) -> list[FeedGapInterval]:
    """Pair feed-specific gap/recovery markers and clip them to one window.

    Collectors preserve the disconnected connection id on the normal recovery
    marker.  A hard process restart can lose that exact marker, however.  In
    that case, a later recovery for the same feed and asset still proves that
    the earlier gap is no longer open.
    """

    start = _utc(window_start)
    end = _utc(window_end)
    gaps: dict[tuple[str, str, str], set[datetime]] = {}
    gap_shards: dict[tuple[tuple[str, str, str], datetime], int | None] = {}
    recoveries: dict[tuple[str, str, str], set[datetime]] = {}
    feed_asset_recoveries: dict[tuple[str, str], set[datetime]] = {}
    for row in marker_rows:
        event_type = _text(row.get("event_type"))
        if event_type not in {"connection_gap", "connection_recovered"}:
            continue
        key = (
            _text(row.get("source")),
            _text(row.get("asset_id")),
            _text(row.get("transaction_hash")),
        )
        if not all(key):
            continue
        observed_at = _datetime_or_none(row.get("timestamp")) or _datetime_or_none(row.get("timestamp_received"))
        if observed_at is None:
            continue
        if event_type == "connection_gap":
            gaps.setdefault(key, set()).add(observed_at)
            gap_shards[(key, observed_at)] = _integer_or_none(row.get("shard_id"))
        else:
            recovery_detail = _text(row.get("book_hash") or row.get("hash"))
            if not recovery_detail.startswith("snapshot_confirmed:"):
                continue
            recoveries.setdefault(key, set()).add(observed_at)
            feed_asset_recoveries.setdefault(key[:2], set()).add(observed_at)
    intervals: list[FeedGapInterval] = []
    for key, gap_times in gaps.items():
        recovery_times = sorted(recoveries.get(key, set()))
        fallback_recovery_times = sorted(feed_asset_recoveries.get(key[:2], set()))
        recovery_index = 0
        for gap_start in sorted(gap_times):
            shard_id = gap_shards.get((key, gap_start))
            while recovery_index < len(recovery_times) and recovery_times[recovery_index] < gap_start:
                recovery_index += 1
            if recovery_index < len(recovery_times):
                recovered_at = recovery_times[recovery_index]
                recovery_index += 1
                recovery_kind = "exact_marker"
            else:
                fallback_times = fallback_recovery_times
                fallback_index = bisect_left(fallback_times, gap_start)
                if fallback_index < len(fallback_times):
                    recovered_at = fallback_times[fallback_index]
                    recovery_kind = "feed_asset_marker"
                else:
                    recovered_at = end
                    recovery_kind = "window_end"
            if recovered_at <= start or gap_start >= end:
                continue
            clipped_start = max(start, gap_start)
            clipped_end = min(end, max(gap_start, recovered_at))
            if clipped_end <= clipped_start:
                continue
            intervals.append(
                FeedGapInterval(
                    source=key[0],
                    asset_id=key[1],
                    connection_id=key[2],
                    gap_start=clipped_start,
                    recovered_at=clipped_end,
                    shard_id=shard_id,
                    original_gap_start=gap_start,
                    recovery_kind=recovery_kind,
                )
            )
    return intervals


def apply_interval_recovery_evidence(
    intervals: Sequence[FeedGapInterval],
    evidence: Mapping[int, datetime],
) -> list[FeedGapInterval]:
    """Shorten only orphan intervals when a later raw WS event proves recovery."""

    result: list[FeedGapInterval] = []
    for index, interval in enumerate(intervals):
        observed_at = _datetime_or_none(evidence.get(index))
        original_gap_start = interval.original_gap_start or interval.gap_start
        if (
            interval.recovery_kind != "exact_marker"
            and observed_at is not None
            and original_gap_start < observed_at < interval.recovered_at
        ):
            if observed_at <= interval.gap_start:
                continue
            result.append(replace(interval, recovered_at=observed_at, recovery_kind="archive_event"))
            continue
        result.append(interval)
    return result


def gap_overlap_by_asset(
    intervals: Sequence[FeedGapInterval],
    *,
    primary_source: str = PRIMARY_FEED_SOURCE,
    secondary_source: str = SECONDARY_FEED_SOURCE,
) -> dict[str, GapOverlap]:
    ranges = dual_gap_intervals_by_asset(
        intervals,
        primary_source=primary_source,
        secondary_source=secondary_source,
    )
    return {
        asset_id: GapOverlap(
            count=len(items),
            seconds=sum(item.duration_seconds for item in items),
        )
        for asset_id, items in ranges.items()
    }


def dual_gap_intervals_by_asset(
    intervals: Sequence[FeedGapInterval],
    *,
    primary_source: str = PRIMARY_FEED_SOURCE,
    secondary_source: str = SECONDARY_FEED_SOURCE,
) -> dict[str, list[DualFeedGapInterval]]:
    grouped: dict[tuple[str, str], list[tuple[datetime, datetime]]] = {}
    for item in intervals:
        grouped.setdefault((item.asset_id, item.source), []).append((item.gap_start, item.recovered_at))
    assets = {asset_id for asset_id, _source in grouped}
    result: dict[str, list[DualFeedGapInterval]] = {}
    for asset_id in assets:
        primary = _merge_ranges(grouped.get((asset_id, primary_source), []))
        secondary = _merge_ranges(grouped.get((asset_id, secondary_source), []))
        overlaps: list[tuple[datetime, datetime]] = []
        for left_start, left_end in primary:
            for right_start, right_end in secondary:
                overlap_start = max(left_start, right_start)
                overlap_end = min(left_end, right_end)
                if overlap_end > overlap_start:
                    overlaps.append((overlap_start, overlap_end))
        merged = _merge_ranges(overlaps)
        if merged:
            result[asset_id] = [
                DualFeedGapInterval(asset_id=asset_id, gap_start=start, recovered_at=end)
                for start, end in merged
            ]
    return result


def unavailable_gap_intervals_by_asset(
    intervals: Sequence[FeedGapInterval],
    *,
    primary_assets: set[str],
    secondary_assets: set[str],
    primary_source: str = PRIMARY_FEED_SOURCE,
    secondary_source: str = SECONDARY_FEED_SOURCE,
) -> dict[str, list[DualFeedGapInterval]]:
    """Return exact periods where no available feed can provide L2 events."""

    dual = dual_gap_intervals_by_asset(
        intervals,
        primary_source=primary_source,
        secondary_source=secondary_source,
    )
    grouped: dict[tuple[str, str], list[tuple[datetime, datetime]]] = {}
    for item in intervals:
        grouped.setdefault((item.asset_id, item.source), []).append(
            (item.gap_start, item.recovered_at)
        )
    result: dict[str, list[DualFeedGapInterval]] = {}
    for asset_id in primary_assets | secondary_assets:
        if asset_id in primary_assets and asset_id in secondary_assets:
            ranges = [
                (item.gap_start, item.recovered_at)
                for item in dual.get(asset_id, ())
            ]
        elif asset_id in primary_assets:
            ranges = _merge_ranges(grouped.get((asset_id, primary_source), ()))
        else:
            ranges = _merge_ranges(grouped.get((asset_id, secondary_source), ()))
        if ranges:
            result[asset_id] = [
                DualFeedGapInterval(
                    asset_id=asset_id,
                    gap_start=start,
                    recovered_at=end,
                )
                for start, end in ranges
            ]
    return result


def build_active_active_hour(
    *,
    hour_start: datetime,
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    primary_source: str = PRIMARY_FEED_SOURCE,
    secondary_source: str = SECONDARY_FEED_SOURCE,
    primary_coverage_table: str = PRIMARY_COVERAGE_TABLE,
    secondary_coverage_table: str = SECONDARY_COVERAGE_TABLE,
    primary_retired_at: datetime | None = None,
    secondary_retired_at: datetime | None = None,
    marker_lookback_hours: float = 2.0,
    write_db: bool = True,
) -> ActiveActiveHourSummary:
    hour = _floor_hour(hour_start)
    hour_end = hour + timedelta(hours=1)
    primary_rows = _load_coverage_rows(coverage_table_sql(primary_coverage_table), hour)
    secondary_rows = _load_coverage_rows(coverage_table_sql(secondary_coverage_table), hour)
    if not primary_rows and not secondary_rows:
        raise RuntimeError(f"neither feed has coverage rows for {hour.isoformat()}")
    marker_sources = []
    if primary_retired_at is None or hour < _utc(primary_retired_at):
        marker_sources.append(primary_source)
    if secondary_retired_at is None or hour < _utc(secondary_retired_at):
        marker_sources.append(secondary_source)
    marker_rows = _load_marker_rows(
        since=hour - timedelta(hours=max(0.0, float(marker_lookback_hours))),
        window_start=hour,
        until=hour_end,
        sources=tuple(marker_sources),
    )
    intervals = pair_gap_intervals(marker_rows, window_start=hour, window_end=hour_end)
    intervals = _apply_archive_event_recovery(
        intervals,
        since=hour,
        until=hour_end,
        sources=tuple(marker_sources),
    )
    primary_assets = {_text(row.get("asset_id")) for row in primary_rows}
    secondary_assets = {_text(row.get("asset_id")) for row in secondary_rows}
    unavailable_gaps = unavailable_gap_intervals_by_asset(
        intervals,
        primary_assets=primary_assets,
        secondary_assets=secondary_assets,
        primary_source=primary_source,
        secondary_source=secondary_source,
    )
    overlaps = {
        asset_id: GapOverlap(
            count=len(items),
            seconds=sum(item.duration_seconds for item in items),
        )
        for asset_id, items in unavailable_gaps.items()
    }
    generated_at = datetime.now(timezone.utc)
    merged = merge_coverage_rows(
        primary_rows,
        secondary_rows,
        overlaps=overlaps,
        primary_source=primary_source,
        secondary_source=secondary_source,
        generated_at=generated_at,
    )
    merged_assets = {str(row["asset_id"]) for row in merged}
    persisted_gaps = {
        asset_id: items for asset_id, items in unavailable_gaps.items() if asset_id in merged_assets
    }
    output_path = (
        Path(output_dir)
        / f"dt={hour:%Y-%m-%d}"
        / f"hour={hour:%H}"
        / "active_active_coverage.parquet"
    )
    _write_parquet_atomic(merged, output_path)
    _write_gap_parquet_atomic(
        _gap_rows(
            hour_start=hour,
            gaps=persisted_gaps,
            primary_source=primary_source,
            secondary_source=secondary_source,
            generated_at=generated_at,
        ),
        output_path.with_name("active_active_gaps.parquet"),
    )
    if write_db:
        _write_coverage_db(merged)
        _write_gap_db(
            hour_start=hour,
            rows=_gap_rows(
                hour_start=hour,
                gaps=persisted_gaps,
                primary_source=primary_source,
                secondary_source=secondary_source,
                generated_at=generated_at,
            ),
        )
    return ActiveActiveHourSummary(
        hour_start=hour,
        token_count=len(merged),
        replay_eligible=sum(bool(row["fill_depth_ready"]) for row in merged),
        complemented=sum(bool(row["complemented"]) for row in merged),
        dual_gap_tokens=sum(int(row["dual_gap_overlap_count"] or 0) > 0 for row in merged),
        primary_only_rows=sum(not bool(row["secondary_present"]) for row in merged),
        secondary_only_rows=sum(not bool(row["primary_present"]) for row in merged),
        output_path=str(output_path),
        generated_at=generated_at,
    )


def merge_coverage_rows(
    primary_rows: Sequence[Mapping[str, Any]],
    secondary_rows: Sequence[Mapping[str, Any]],
    *,
    overlaps: Mapping[str, GapOverlap] | None = None,
    primary_source: str = PRIMARY_FEED_SOURCE,
    secondary_source: str = SECONDARY_FEED_SOURCE,
    generated_at: datetime | None = None,
) -> list[dict[str, Any]]:
    overlap_map = dict(overlaps or {})
    generated = generated_at or datetime.now(timezone.utc)
    primary = {_text(row.get("asset_id")): dict(row) for row in primary_rows}
    secondary = {_text(row.get("asset_id")): dict(row) for row in secondary_rows}
    result: list[dict[str, Any]] = []
    for asset_id in sorted(set(primary) | set(secondary)):
        left = primary.get(asset_id)
        right = secondary.get(asset_id)
        base = left or right or {}
        overlap = overlap_map.get(asset_id, GapOverlap())
        primary_present = left is not None
        secondary_present = right is not None
        has_book = _either(left, right, "has_book")
        has_two_sided = _either(left, right, "has_two_sided_book")
        has_ws_book = _either(left, right, "has_ws_book")
        has_rest_seed = _either(left, right, "has_rest_seed_book")
        has_price_change = _either(left, right, "has_price_change")
        has_price_after = _either(left, right, "has_price_change_after_baseline")
        rest_seed_only = has_rest_seed and not has_ws_book
        both_rest_error = _positive(left, "rest_error_count") and _positive(right, "rest_error_count")
        primary_ready = bool(left and left.get("fill_depth_ready"))
        secondary_ready = bool(right and right.get("fill_depth_ready"))
        archive_shard_ids = _merged_archive_shard_ids(left, right)
        connection_shard_id = _merged_connection_shard_id(left, right)
        archive_row_count = _single_source_archive_row_count(left, right)
        if not primary_present or not secondary_present:
            present = left or right
            outside_gap_ready = _single_feed_ready_outside_gap(
                present,
                has_exact_gap_intervals=overlap.count > 0,
            )
            present_reason = _text(_value(present, "fill_depth_reason")) or "not_ready"
            outside_gap_reason = "ready_single_feed" if outside_gap_ready else f"single_feed_{present_reason}"
        elif not has_book:
            outside_gap_ready = False
            outside_gap_reason = "no_book_baseline"
        elif not has_two_sided:
            outside_gap_ready = False
            outside_gap_reason = "one_sided_book"
        elif rest_seed_only and not has_price_after:
            outside_gap_ready = False
            outside_gap_reason = "rest_seed_without_ws_delta"
        elif both_rest_error:
            outside_gap_ready = False
            outside_gap_reason = "dual_feed_rest_error"
        else:
            outside_gap_ready = True
            outside_gap_reason = "ready"
        # A short simultaneous outage does not invalidate the other 3,599+
        # seconds in the token-hour.  Exact intervals remain fail-closed in
        # l2_coverage_gate; this summary flag means the hour has usable time.
        ready = outside_gap_ready
        if overlap.count > 0 and outside_gap_ready:
            reason = "ready_with_dual_gap_intervals"
        else:
            reason = outside_gap_reason
        complemented = ready and not (primary_ready and secondary_ready)
        token_hour_coverage_status = _token_hour_coverage_status(
            primary_present=primary_present,
            secondary_present=secondary_present,
            dual_gap_count=overlap.count,
        )
        result.append(
            {
                "asset_id": asset_id,
                "hour_start": _datetime_or_none(base.get("hour_start")),
                "desired_from": _first(left, right, "desired_from"),
                "condition_id": _first(left, right, "condition_id") or "",
                "market_slug": _first(left, right, "market_slug"),
                "market_state": _first(left, right, "market_state"),
                "execution_eligible": bool(_first(left, right, "execution_eligible")),
                # Preserve source-proven routes. Conflicting assigned shards
                # are deliberately left unknown instead of guessed.
                "archive_shard_ids": archive_shard_ids,
                "connection_shard_id": connection_shard_id,
                # A source row count is exact only for a single-source hour.
                # A/B can contain overlapping REST/control evidence, so their
                # counts must not be added and advertised as exact.
                "archive_row_count": archive_row_count,
                "event_count": _sum(left, right, "event_count"),
                "ws_event_count": _sum(left, right, "ws_event_count"),
                "book_count": _sum(left, right, "book_count"),
                "ws_book_count": _sum(left, right, "ws_book_count"),
                "rest_seed_book_count": _sum(left, right, "rest_seed_book_count"),
                "price_change_count": _sum(left, right, "price_change_count"),
                "best_bid_ask_count": _sum(left, right, "best_bid_ask_count"),
                "last_trade_price_count": _sum(left, right, "last_trade_price_count"),
                "first_received_at": _min_time(left, right, "first_received_at"),
                "last_received_at": _max_time(left, right, "last_received_at"),
                "baseline_book_count": _sum(left, right, "baseline_book_count"),
                "ws_baseline_book_count": _sum(left, right, "ws_baseline_book_count"),
                "rest_seed_baseline_book_count": _sum(left, right, "rest_seed_baseline_book_count"),
                "baseline_received_at": _max_time(left, right, "baseline_received_at"),
                "has_two_sided_book": has_two_sided,
                "price_change_after_baseline_count": _sum(left, right, "price_change_after_baseline_count"),
                "ws_gap_rest_seeded_count": _sum(left, right, "ws_gap_rest_seeded_count"),
                "ws_gap_unseeded_count": overlap.count,
                "rest_error_count": int(both_rest_error),
                "has_book": has_book,
                "has_ws_book": has_ws_book,
                "has_rest_seed_book": has_rest_seed,
                "has_price_change": has_price_change,
                "has_price_change_after_baseline": has_price_after,
                "rest_seed_only": rest_seed_only,
                "fill_depth_ready": ready,
                "fill_depth_reason": reason,
                "fill_depth_ready_outside_gap": outside_gap_ready,
                "fill_depth_reason_outside_gap": outside_gap_reason,
                "manifest_generated_at": generated,
                "archive_dir": "active_active",
                "shard_count": None,
                "primary_source": primary_source,
                "secondary_source": secondary_source,
                "primary_present": primary_present,
                "secondary_present": secondary_present,
                "token_hour_coverage_status": token_hour_coverage_status,
                "primary_fill_depth_ready": primary_ready,
                "secondary_fill_depth_ready": secondary_ready,
                "primary_fill_depth_reason": _value(left, "fill_depth_reason"),
                "secondary_fill_depth_reason": _value(right, "fill_depth_reason"),
                "primary_event_count": _number(left, "event_count"),
                "secondary_event_count": _number(right, "event_count"),
                "primary_gap_count": _number(left, "ws_gap_unseeded_count"),
                "secondary_gap_count": _number(right, "ws_gap_unseeded_count"),
                "dual_gap_overlap_count": overlap.count,
                "dual_gap_overlap_seconds": round(overlap.seconds, 6),
                "fillable_seconds": round(max(0.0, 3600.0 - overlap.seconds), 6),
                "fillable_ratio": round(max(0.0, 1.0 - overlap.seconds / 3600.0), 12),
                "complemented": complemented,
            }
        )
    return result


def _single_feed_ready_outside_gap(
    row: Mapping[str, Any] | None,
    *,
    has_exact_gap_intervals: bool,
) -> bool:
    if not row:
        return False
    if bool(row.get("fill_depth_ready")):
        return True
    return (
        has_exact_gap_intervals
        and _text(row.get("fill_depth_reason")) == "ws_gap_unseeded"
        and bool(row.get("has_book"))
        and bool(row.get("has_two_sided_book"))
        and not (
            bool(row.get("rest_seed_only"))
            and not bool(row.get("has_price_change_after_baseline"))
        )
        and not _positive(row, "rest_error_count")
    )


def ensure_active_active_schema() -> None:
    with postgres_connection(readonly=False) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ACTIVE_ACTIVE_COVERAGE_TABLE} (
                asset_id TEXT NOT NULL, hour_start TIMESTAMPTZ NOT NULL,
                desired_from TIMESTAMPTZ, condition_id TEXT, market_slug TEXT,
                market_state TEXT, execution_eligible BOOLEAN NOT NULL DEFAULT FALSE,
                archive_shard_ids INTEGER[] NOT NULL DEFAULT '{{}}',
                connection_shard_id INTEGER,
                archive_row_count BIGINT,
                event_count BIGINT NOT NULL DEFAULT 0, ws_event_count BIGINT NOT NULL DEFAULT 0,
                book_count BIGINT NOT NULL DEFAULT 0, ws_book_count BIGINT NOT NULL DEFAULT 0,
                rest_seed_book_count BIGINT NOT NULL DEFAULT 0,
                price_change_count BIGINT NOT NULL DEFAULT 0,
                best_bid_ask_count BIGINT NOT NULL DEFAULT 0,
                last_trade_price_count BIGINT NOT NULL DEFAULT 0,
                first_received_at TIMESTAMPTZ, last_received_at TIMESTAMPTZ,
                baseline_book_count BIGINT NOT NULL DEFAULT 0,
                ws_baseline_book_count BIGINT NOT NULL DEFAULT 0,
                rest_seed_baseline_book_count BIGINT NOT NULL DEFAULT 0,
                baseline_received_at TIMESTAMPTZ,
                has_two_sided_book BOOLEAN NOT NULL DEFAULT FALSE,
                price_change_after_baseline_count BIGINT NOT NULL DEFAULT 0,
                ws_gap_rest_seeded_count BIGINT NOT NULL DEFAULT 0,
                ws_gap_unseeded_count BIGINT NOT NULL DEFAULT 0,
                rest_error_count BIGINT NOT NULL DEFAULT 0,
                has_book BOOLEAN NOT NULL DEFAULT FALSE,
                has_ws_book BOOLEAN NOT NULL DEFAULT FALSE,
                has_rest_seed_book BOOLEAN NOT NULL DEFAULT FALSE,
                has_price_change BOOLEAN NOT NULL DEFAULT FALSE,
                has_price_change_after_baseline BOOLEAN NOT NULL DEFAULT FALSE,
                rest_seed_only BOOLEAN NOT NULL DEFAULT FALSE,
                fill_depth_ready BOOLEAN NOT NULL DEFAULT FALSE,
                fill_depth_reason TEXT NOT NULL,
                fill_depth_ready_outside_gap BOOLEAN NOT NULL DEFAULT FALSE,
                fill_depth_reason_outside_gap TEXT NOT NULL DEFAULT 'not_ready',
                manifest_generated_at TIMESTAMPTZ NOT NULL,
                archive_dir TEXT NOT NULL DEFAULT 'active_active', shard_count INTEGER,
                primary_source TEXT NOT NULL, secondary_source TEXT NOT NULL,
                primary_present BOOLEAN NOT NULL DEFAULT FALSE,
                secondary_present BOOLEAN NOT NULL DEFAULT FALSE,
                token_hour_coverage_status TEXT NOT NULL DEFAULT 'BOTH_MISSING',
                primary_fill_depth_ready BOOLEAN NOT NULL DEFAULT FALSE,
                secondary_fill_depth_ready BOOLEAN NOT NULL DEFAULT FALSE,
                primary_fill_depth_reason TEXT, secondary_fill_depth_reason TEXT,
                primary_event_count BIGINT NOT NULL DEFAULT 0,
                secondary_event_count BIGINT NOT NULL DEFAULT 0,
                primary_gap_count BIGINT NOT NULL DEFAULT 0,
                secondary_gap_count BIGINT NOT NULL DEFAULT 0,
                dual_gap_overlap_count BIGINT NOT NULL DEFAULT 0,
                dual_gap_overlap_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                fillable_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
                fillable_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
                complemented BOOLEAN NOT NULL DEFAULT FALSE,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (asset_id, hour_start)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_clob_l2_active_active_hour_ready "
            f"ON {ACTIVE_ACTIVE_COVERAGE_TABLE} (hour_start DESC, fill_depth_ready)"
        )
        cur.execute(
            f"ALTER TABLE {ACTIVE_ACTIVE_COVERAGE_TABLE} "
            "ADD COLUMN IF NOT EXISTS fill_depth_ready_outside_gap BOOLEAN NOT NULL DEFAULT FALSE"
        )
        cur.execute(
            f"ALTER TABLE {ACTIVE_ACTIVE_COVERAGE_TABLE} "
            "ADD COLUMN IF NOT EXISTS fill_depth_reason_outside_gap TEXT NOT NULL DEFAULT 'not_ready'"
        )
        cur.execute(
            f"ALTER TABLE {ACTIVE_ACTIVE_COVERAGE_TABLE} "
            "ADD COLUMN IF NOT EXISTS fillable_seconds DOUBLE PRECISION NOT NULL DEFAULT 0"
        )
        cur.execute(
            f"ALTER TABLE {ACTIVE_ACTIVE_COVERAGE_TABLE} "
            "ADD COLUMN IF NOT EXISTS fillable_ratio DOUBLE PRECISION NOT NULL DEFAULT 0"
        )
        cur.execute(
            f"ALTER TABLE {ACTIVE_ACTIVE_COVERAGE_TABLE} "
            "ADD COLUMN IF NOT EXISTS token_hour_coverage_status TEXT NOT NULL DEFAULT 'BOTH_MISSING'"
        )
        cur.execute(
            f"ALTER TABLE {ACTIVE_ACTIVE_COVERAGE_TABLE} "
            "ADD COLUMN IF NOT EXISTS archive_shard_ids INTEGER[] NOT NULL DEFAULT '{}'"
        )
        cur.execute(
            f"ALTER TABLE {ACTIVE_ACTIVE_COVERAGE_TABLE} "
            "ADD COLUMN IF NOT EXISTS connection_shard_id INTEGER"
        )
        cur.execute(
            f"ALTER TABLE {ACTIVE_ACTIVE_COVERAGE_TABLE} "
            "ADD COLUMN IF NOT EXISTS archive_row_count BIGINT"
        )
        # Release schema locks before the one-time historical status repair.
        conn.commit()
        cur.execute(
            f"""
            UPDATE {ACTIVE_ACTIVE_COVERAGE_TABLE}
            SET token_hour_coverage_status = CASE
                WHEN primary_present AND secondary_present AND dual_gap_overlap_count > 0
                    THEN 'BOTH_PRESENT_DUAL_GAP'
                WHEN primary_present AND secondary_present THEN 'BOTH_PRESENT'
                WHEN primary_present THEN 'PRIMARY_ONLY'
                WHEN secondary_present THEN 'SECONDARY_ONLY'
                ELSE 'BOTH_MISSING'
            END
            WHERE token_hour_coverage_status = 'BOTH_MISSING'
              AND (primary_present OR secondary_present)
              AND hour_start = (
                  SELECT max(hour_start) FROM {ACTIVE_ACTIVE_COVERAGE_TABLE}
              )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ACTIVE_ACTIVE_GAP_TABLE} (
                asset_id TEXT NOT NULL,
                hour_start TIMESTAMPTZ NOT NULL,
                gap_start TIMESTAMPTZ NOT NULL,
                recovered_at TIMESTAMPTZ NOT NULL,
                duration_seconds DOUBLE PRECISION NOT NULL,
                primary_source TEXT NOT NULL,
                secondary_source TEXT NOT NULL,
                generated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (asset_id, hour_start, gap_start, recovered_at)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_clob_l2_active_active_gap_lookup "
            f"ON {ACTIVE_ACTIVE_GAP_TABLE} (asset_id, gap_start, recovered_at)"
        )
        conn.commit()


def _token_hour_coverage_status(
    *,
    primary_present: bool,
    secondary_present: bool,
    dual_gap_count: int,
) -> str:
    if not primary_present and not secondary_present:
        return "BOTH_MISSING"
    if primary_present and not secondary_present:
        return "PRIMARY_ONLY"
    if secondary_present and not primary_present:
        return "SECONDARY_ONLY"
    if int(dual_gap_count) > 0:
        return "BOTH_PRESENT_DUAL_GAP"
    return "BOTH_PRESENT"


def next_common_hour(
    *,
    start_at: datetime,
    primary_retired_at: datetime | None = None,
    secondary_retired_at: datetime | None = None,
    stop_before: datetime | None = None,
    primary_coverage_table: str = PRIMARY_COVERAGE_TABLE,
    secondary_coverage_table: str = SECONDARY_COVERAGE_TABLE,
    primary_watermark_id: str = PRIMARY_COVERAGE_WATERMARK_ID,
    secondary_watermark_id: str = SECONDARY_COVERAGE_WATERMARK_ID,
    secondary_sparse: bool = False,
    secondary_watermark_optional: bool = False,
) -> datetime | None:
    primary_table = coverage_table_sql(primary_coverage_table)
    secondary_table = coverage_table_sql(secondary_coverage_table)
    if secondary_sparse:
        # Source A is the authoritative hourly spine.  Sparse Source B rows
        # may arrive later and advance source_updated_at, but an absent B row
        # must not prevent a finalized A hour from being materialized.
        source_hours_sql = f"""
            primary_hours AS (
                SELECT hour_start, max(updated_at) AS primary_updated_at
                FROM {primary_table}
                GROUP BY hour_start
            ), secondary_hours AS (
                SELECT hour_start, max(updated_at) AS secondary_updated_at
                FROM {secondary_table}
                GROUP BY hour_start
            ), source_hours AS (
                SELECT p.hour_start,
                       greatest(
                           p.primary_updated_at,
                           COALESCE(s.secondary_updated_at, '-infinity'::timestamptz)
                       ) AS source_updated_at
                FROM primary_hours p
                LEFT JOIN secondary_hours s USING (hour_start)
            )
        """
    else:
        source_hours_sql = f"""
            source_rows AS (
                SELECT hour_start, updated_at FROM {primary_table}
                UNION ALL
                SELECT hour_start, updated_at FROM {secondary_table}
            ), source_hours AS (
                SELECT hour_start, max(updated_at) AS source_updated_at
                FROM source_rows
                GROUP BY hour_start
            )
        """
    secondary_ready_sql = (
        "TRUE"
        if secondary_watermark_optional
        else """(
                    (w.secondary_watermark IS NOT NULL AND h.hour_start < w.secondary_watermark)
                 OR (w.secondary_retired_at IS NOT NULL AND h.hour_start >= w.secondary_retired_at)
              )"""
    )
    with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
        try:
            cur.execute(
                f"""
            WITH watermarks AS (
                SELECT
                    max(coverage_finalize_watermark) FILTER (WHERE worker_id = %s) AS primary_watermark,
                    max(coverage_finalize_watermark) FILTER (WHERE worker_id = %s) AS secondary_watermark,
                    %s::timestamptz AS primary_retired_at,
                    %s::timestamptz AS secondary_retired_at
                FROM quant.clob_l2_watermarks
            ), {source_hours_sql}, active_hours AS (
                SELECT hour_start, max(updated_at) AS active_updated_at
                FROM {ACTIVE_ACTIVE_COVERAGE_TABLE}
                GROUP BY hour_start
            )
            SELECT min(h.hour_start) AS hour_start
            FROM source_hours h
            CROSS JOIN watermarks w
            LEFT JOIN active_hours a ON a.hour_start = h.hour_start
            WHERE h.hour_start >= %s
              AND (%s::timestamptz IS NULL OR h.hour_start < %s::timestamptz)
              AND (a.hour_start IS NULL OR h.source_updated_at > a.active_updated_at)
              AND (
                    (w.primary_watermark IS NOT NULL AND h.hour_start < w.primary_watermark)
                 OR (w.primary_retired_at IS NOT NULL AND h.hour_start >= w.primary_retired_at)
              )
              AND ({secondary_ready_sql})
                """,
                (
                    str(primary_watermark_id),
                    str(secondary_watermark_id),
                    _datetime_or_none(primary_retired_at),
                    _datetime_or_none(secondary_retired_at),
                    _floor_hour(start_at),
                    _datetime_or_none(stop_before),
                    _datetime_or_none(stop_before),
                ),
            )
        except Exception as exc:
            if getattr(exc, "sqlstate", None) == "42P01":
                return None
            raise
        row = cur.fetchone()
    return row["hour_start"] if row and row["hour_start"] else None


def _load_coverage_rows(table: str, hour_start: datetime) -> list[dict[str, Any]]:
    table = coverage_table_sql(table)
    with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table} WHERE hour_start = %s", (_floor_hour(hour_start),))
        return [dict(row) for row in cur.fetchall()]


def _load_marker_rows(
    *,
    since: datetime,
    window_start: datetime,
    until: datetime,
    sources: Sequence[str],
) -> list[dict[str, Any]]:
    with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT path
            FROM quant.clob_l2_archive_manifest
            WHERE status = 'ready'
              AND source = ANY(%s)
              AND archive_hour >= date_trunc('hour', %s::timestamptz)
              AND archive_hour < date_trunc('hour', %s::timestamptz) + interval '1 hour'
              AND (
                    COALESCE((event_type_counts ->> 'connection_gap')::bigint, 0) > 0
                 OR COALESCE((event_type_counts ->> 'connection_recovered')::bigint, 0) > 0
              )
            ORDER BY path
            """,
            (list(sources), _utc(since), _utc(until)),
        )
        paths = [str(row["path"]) for row in cur.fetchall() if Path(str(row["path"])).is_file()]
    if not paths:
        return []
    return _read_marker_rows(
        paths,
        since=since,
        window_start=window_start,
        until=until,
    )


def _read_marker_rows(
    paths: Sequence[str],
    *,
    since: datetime,
    window_start: datetime,
    until: datetime,
) -> list[dict[str, Any]]:
    con = duckdb.connect(":memory:", config={"threads": str(max(1, int(os.environ.get("BOOK_L2_DUCKDB_THREADS", "4"))))})
    try:
        frame = con.execute(
            f"""
            WITH markers AS (
                SELECT source, asset_id, shard_id, event_type, transaction_hash, book_hash,
                       timestamp_received, timestamp
                FROM read_parquet({_duckdb_file_list(paths)}, union_by_name=true)
                WHERE event_type IN ('connection_gap', 'connection_recovered')
                  AND timestamp_received >= ? AND timestamp_received < ?
            ), pre_window AS (
                SELECT source, asset_id, shard_id, event_type, transaction_hash, book_hash,
                       timestamp_received, timestamp
                FROM (
                    SELECT *, row_number() OVER (
                        PARTITION BY source, asset_id, shard_id
                        ORDER BY timestamp_received DESC, event_type DESC
                    ) AS marker_rank
                    FROM markers
                    WHERE timestamp_received < ?
                ) ranked
                WHERE marker_rank = 1
            ), window_markers AS (
                SELECT source, asset_id, shard_id, event_type, transaction_hash, book_hash,
                       timestamp_received, timestamp
                FROM markers
                WHERE timestamp_received >= ?
            )
            SELECT * FROM pre_window
            UNION ALL
            SELECT * FROM window_markers
            """,
            [_utc(since), _utc(until), _utc(window_start), _utc(window_start)],
        ).fetchdf()
    finally:
        con.close()
    return frame.to_dict("records")


def _apply_archive_event_recovery(
    intervals: Sequence[FeedGapInterval],
    *,
    since: datetime,
    until: datetime,
    sources: Sequence[str],
) -> list[FeedGapInterval]:
    candidate_ids: dict[tuple[Any, ...], int] = {}
    candidate_rows: list[dict[str, Any]] = []
    interval_candidates: dict[int, int] = {}
    for index, interval in enumerate(intervals):
        if interval.recovery_kind == "exact_marker":
            continue
        key = (
            interval.source,
            interval.asset_id,
            interval.gap_start,
            interval.recovered_at,
        )
        candidate_id = candidate_ids.get(key)
        if candidate_id is None:
            candidate_id = len(candidate_rows)
            candidate_ids[key] = candidate_id
            candidate_rows.append(
                {
                    "candidate_id": candidate_id,
                    "source": interval.source,
                    "asset_id": interval.asset_id,
                    "gap_start": interval.gap_start,
                    "recovered_at": interval.recovered_at,
                }
            )
        interval_candidates[index] = candidate_id
    if not candidate_rows:
        return list(intervals)
    with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT path
            FROM quant.clob_l2_archive_manifest
            WHERE status = 'ready'
              AND source = ANY(%s)
              AND archive_hour >= date_trunc('hour', %s::timestamptz)
              AND archive_hour < date_trunc('hour', %s::timestamptz) + interval '1 hour'
            ORDER BY path
            """,
            (list(sources), _utc(since), _utc(until)),
        )
        paths = [str(row["path"]) for row in cur.fetchall() if Path(str(row["path"])).is_file()]
    if not paths:
        return list(intervals)
    con = duckdb.connect(
        ":memory:",
        config={"threads": str(max(1, int(os.environ.get("BOOK_L2_DUCKDB_THREADS", "4"))))},
    )
    try:
        candidate_frame = pd.DataFrame(candidate_rows)
        con.register("orphan_gaps", candidate_frame)
        evidence_frame = con.execute(
            f"""
            SELECT g.candidate_id, min(e.timestamp_received) AS observed_at
            FROM read_parquet({_duckdb_file_list(paths)}, union_by_name=true) e
            JOIN orphan_gaps g
              ON e.source = g.source
             AND e.asset_id = g.asset_id
             AND e.timestamp_received >= g.gap_start
             AND e.timestamp_received < g.recovered_at
            WHERE e.timestamp_received >= ? AND e.timestamp_received < ?
              AND e.event_type = 'book'
            GROUP BY g.candidate_id
            """,
            [_utc(since), _utc(until)],
        ).fetchdf()
    finally:
        con.close()
    candidate_evidence = {
        int(row.candidate_id): _datetime_or_none(row.observed_at)
        for row in evidence_frame.itertuples(index=False)
    }
    evidence = {
        interval_id: candidate_evidence[candidate_id]
        for interval_id, candidate_id in interval_candidates.items()
        if candidate_id in candidate_evidence
    }
    return apply_interval_recovery_evidence(intervals, evidence)


def _write_parquet_atomic(rows: Sequence[Mapping[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    frame = pd.DataFrame(list(rows))
    con = duckdb.connect(":memory:")
    try:
        con.register("coverage_rows", frame)
        con.execute(
            "COPY (SELECT * FROM coverage_rows ORDER BY execution_eligible DESC, asset_id) "
            f"TO {_sql_string(str(tmp))} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    os.replace(tmp, output)


def _gap_rows(
    *,
    hour_start: datetime,
    gaps: Mapping[str, Sequence[DualFeedGapInterval]],
    primary_source: str,
    secondary_source: str,
    generated_at: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "asset_id": asset_id,
            "hour_start": hour_start,
            "gap_start": item.gap_start,
            "recovered_at": item.recovered_at,
            "duration_seconds": round(item.duration_seconds, 6),
            "primary_source": primary_source,
            "secondary_source": secondary_source,
            "generated_at": generated_at,
        }
        for asset_id, items in sorted(gaps.items())
        for item in items
    ]


def _write_gap_parquet_atomic(rows: Sequence[Mapping[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            """
            CREATE TABLE gap_rows (
                asset_id VARCHAR, hour_start TIMESTAMPTZ, gap_start TIMESTAMPTZ,
                recovered_at TIMESTAMPTZ, duration_seconds DOUBLE,
                primary_source VARCHAR, secondary_source VARCHAR, generated_at TIMESTAMPTZ
            )
            """
        )
        if rows:
            frame = pd.DataFrame(list(rows))
            con.register("incoming_gaps", frame)
            con.execute("INSERT INTO gap_rows SELECT * FROM incoming_gaps")
        con.execute(
            "COPY (SELECT * FROM gap_rows ORDER BY asset_id, gap_start) "
            f"TO {_sql_string(str(tmp))} (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    os.replace(tmp, output)


def _write_coverage_db(rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    ensure_active_active_schema()
    columns = [key for key in rows[0] if key not in {"updated_at"}]
    placeholders = ",".join(["%s"] * len(columns))
    updates = ",".join(f"{column}=EXCLUDED.{column}" for column in columns if column not in {"asset_id", "hour_start"})
    sql = (
        f"INSERT INTO {ACTIVE_ACTIVE_COVERAGE_TABLE} ({','.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT (asset_id,hour_start) DO UPDATE SET {updates},updated_at=now()"
    )
    with postgres_connection(readonly=False) as conn, conn.cursor() as cur:
        hours = sorted({_floor_hour(row["hour_start"]) for row in rows if row.get("hour_start")})
        cur.execute(
            f"DELETE FROM {ACTIVE_ACTIVE_COVERAGE_TABLE} WHERE hour_start = ANY(%s)",
            (hours,),
        )
        cur.executemany(sql, [tuple(row.get(column) for column in columns) for row in rows])
        conn.commit()


def _write_gap_db(*, hour_start: datetime, rows: Sequence[Mapping[str, Any]]) -> None:
    ensure_active_active_schema()
    with postgres_connection(readonly=False) as conn, conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {ACTIVE_ACTIVE_GAP_TABLE} WHERE hour_start = %s",
            (_floor_hour(hour_start),),
        )
        if rows:
            cur.executemany(
                f"""
                INSERT INTO {ACTIVE_ACTIVE_GAP_TABLE} (
                    asset_id, hour_start, gap_start, recovered_at, duration_seconds,
                    primary_source, secondary_source, generated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        row["asset_id"], row["hour_start"], row["gap_start"],
                        row["recovered_at"], row["duration_seconds"],
                        row["primary_source"], row["secondary_source"], row["generated_at"],
                    )
                    for row in rows
                ],
            )
        conn.commit()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-at", required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--interval-seconds", type=float, default=300.0)
    parser.add_argument("--max-hours", type=int, default=1)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--primary-retired-at")
    parser.add_argument("--secondary-retired-at")
    parser.add_argument("--primary-source", default=PRIMARY_FEED_SOURCE)
    parser.add_argument("--secondary-source", default=SECONDARY_FEED_SOURCE)
    parser.add_argument("--primary-coverage-table", default=PRIMARY_COVERAGE_TABLE)
    parser.add_argument("--secondary-coverage-table", default=SECONDARY_COVERAGE_TABLE)
    parser.add_argument("--primary-watermark-id", default=PRIMARY_COVERAGE_WATERMARK_ID)
    parser.add_argument("--secondary-watermark-id", default=SECONDARY_COVERAGE_WATERMARK_ID)
    parser.add_argument("--secondary-sparse", action="store_true")
    parser.add_argument("--secondary-watermark-optional", action="store_true")
    parser.add_argument("--stop-before")
    parser.add_argument("--marker-lookback-hours", type=float, default=2.0)
    parser.add_argument("--json-out", type=Path, default=Path("runtime_outputs/lob_l2_active_active_coverage/worker.json"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    start_at = _datetime_or_none(args.start_at)
    if start_at is None:
        raise SystemExit("invalid --start-at")
    primary_retired_at = _datetime_or_none(args.primary_retired_at)
    secondary_retired_at = _datetime_or_none(args.secondary_retired_at)
    stop_before = _datetime_or_none(args.stop_before)
    try:
        effective_contract = validate_active_active_contract(
            start_at=start_at,
            primary_source=args.primary_source,
            secondary_source=args.secondary_source,
            primary_coverage_table=args.primary_coverage_table,
            secondary_coverage_table=args.secondary_coverage_table,
            primary_watermark_id=args.primary_watermark_id,
            secondary_watermark_id=args.secondary_watermark_id,
            primary_retired_at=primary_retired_at,
            secondary_retired_at=secondary_retired_at,
            secondary_sparse=args.secondary_sparse,
            secondary_watermark_optional=args.secondary_watermark_optional,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    ensure_active_active_schema()
    while True:
        summaries: list[dict[str, Any]] = []
        for _ in range(max(1, int(args.max_hours))):
            hour = next_common_hour(
                start_at=start_at,
                primary_retired_at=primary_retired_at,
                secondary_retired_at=secondary_retired_at,
                stop_before=stop_before,
                primary_coverage_table=args.primary_coverage_table,
                secondary_coverage_table=args.secondary_coverage_table,
                primary_watermark_id=args.primary_watermark_id,
                secondary_watermark_id=args.secondary_watermark_id,
                secondary_sparse=args.secondary_sparse,
                secondary_watermark_optional=args.secondary_watermark_optional,
            )
            if hour is None:
                break
            summaries.append(
                build_active_active_hour(
                    hour_start=hour,
                    output_dir=args.output_dir,
                    primary_source=args.primary_source,
                    secondary_source=args.secondary_source,
                    primary_coverage_table=args.primary_coverage_table,
                    secondary_coverage_table=args.secondary_coverage_table,
                    primary_retired_at=primary_retired_at,
                    secondary_retired_at=secondary_retired_at,
                    marker_lookback_hours=args.marker_lookback_hours,
                ).as_dict()
            )
        payload = {
            "status": "updated" if summaries else "caught_up",
            "hours": summaries,
            "effective_contract": effective_contract,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        tmp = args.json_out.with_name(f".{args.json_out.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp, args.json_out)
        print(json.dumps(payload, sort_keys=True))
        if args.once:
            return 0
        # Historical repair must yield to the hourly archive publisher.  A
        # near-continuous read lock can otherwise starve verified transfers.
        time.sleep(max(5.0, float(args.interval_seconds)))


def _merge_ranges(ranges: Sequence[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    merged: list[tuple[datetime, datetime]] = []
    for start, end in sorted(ranges):
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def _value(row: Mapping[str, Any] | None, key: str) -> Any:
    return row.get(key) if row else None


def _first(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None, key: str) -> Any:
    value = _value(left, key)
    return value if value is not None else _value(right, key)


def _number(row: Mapping[str, Any] | None, key: str) -> int:
    try:
        return int(_value(row, key) or 0)
    except (TypeError, ValueError):
        return 0


def _merged_archive_shard_ids(
    left: Mapping[str, Any] | None,
    right: Mapping[str, Any] | None,
) -> list[int]:
    """Return the sorted union of source-observed archive shard routes."""

    routes: set[int] = set()
    for row in (left, right):
        value = _value(row, "archive_shard_ids")
        if value is None or isinstance(value, (str, bytes)):
            continue
        try:
            values = list(value)
        except TypeError:
            values = [value]
        for item in values:
            shard_id = _integer_or_none(item)
            if shard_id is not None and shard_id >= 0:
                routes.add(shard_id)
    return sorted(routes)


def _merged_connection_shard_id(
    left: Mapping[str, Any] | None,
    right: Mapping[str, Any] | None,
) -> int | None:
    """Keep an assigned route only when every non-null proof is consistent."""

    routes = {
        shard_id
        for shard_id in (
            _integer_or_none(_value(left, "connection_shard_id")),
            _integer_or_none(_value(right, "connection_shard_id")),
        )
        if shard_id is not None and shard_id >= 0
    }
    return next(iter(routes)) if len(routes) == 1 else None


def _single_source_archive_row_count(
    left: Mapping[str, Any] | None,
    right: Mapping[str, Any] | None,
) -> int | None:
    """Preserve exact row cardinality only when one coverage source is present."""

    present = [row for row in (left, right) if row is not None]
    if len(present) != 1:
        return None
    count = _integer_or_none(_value(present[0], "archive_row_count"))
    return count if count is not None and count >= 0 else None


def _integer_or_none(value: Any) -> int | None:
    try:
        if value is None or bool(pd.isna(value)):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _sum(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None, key: str) -> int:
    return _number(left, key) + _number(right, key)


def _positive(row: Mapping[str, Any] | None, key: str) -> bool:
    return _number(row, key) > 0


def _either(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None, key: str) -> bool:
    return bool(_value(left, key)) or bool(_value(right, key))


def _min_time(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None, key: str) -> datetime | None:
    values = [_datetime_or_none(_value(row, key)) for row in (left, right)]
    return min((value for value in values if value is not None), default=None)


def _max_time(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None, key: str) -> datetime | None:
    values = [_datetime_or_none(_value(row, key)) for row in (left, right)]
    return max((value for value in values if value is not None), default=None)


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _timestamp_text(value: Any) -> str:
    parsed = _datetime_or_none(value)
    return parsed.isoformat() if parsed is not None else _text(value)


def _datetime_or_none(value: Any) -> datetime | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        return _utc(value)
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime().astimezone(timezone.utc) if parsed.tzinfo else parsed.to_pydatetime().replace(tzinfo=timezone.utc)


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _floor_hour(value: datetime) -> datetime:
    return _utc(value).replace(minute=0, second=0, microsecond=0)


def _duckdb_file_list(paths: Sequence[str]) -> str:
    return "[" + ",".join(_sql_string(path) for path in paths) + "]"


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


if __name__ == "__main__":
    raise SystemExit(main())
