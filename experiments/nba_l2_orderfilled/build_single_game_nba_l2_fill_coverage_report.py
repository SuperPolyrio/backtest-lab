#!/usr/bin/env python3
"""Report PMXT L2 coverage against OrderFilled replay for NBA single-game markets."""

from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sys
from typing import Any, Iterable, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from experiments.nba_l2_orderfilled.materialize_single_game_nba_execution_replay import _required_hour
from experiments.nba_l2_orderfilled.run_complete_nba_l2_orderfilled_backfill import (
    DEFAULT_MARKET_SLUGS,
    DEFAULT_OUTPUT_DIR,
    lifecycle_for_market,
    load_markets_by_slug,
    orderfilled_replay_token_id,
)
from experiments.nba_l2_orderfilled.run_nba_l2_orderfilled_backtest import NbaMarket, NbaToken, all_hours, load_market_tokens
from quant.core.db import ClickHouseClient, postgres_connection
from scripts.validate_orderfilled_trade_replay import load_block_timestamps_with_interpolation


DEFAULT_BUILD_TAG = "single_game_nba_execution_replay"
DEFAULT_THRESHOLDS = (60, 300, 3600, 21600, 86400)


@dataclass(frozen=True)
class TokenMapping:
    market_id: int
    market_slug: str
    token_side: str
    fill_token_id: str
    l2_token_id: str


def main() -> int:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    slugs = tuple(args.market_slug or DEFAULT_MARKET_SLUGS)
    thresholds = tuple(sorted({int(value) for value in args.freshness_seconds if int(value) > 0}))

    with postgres_connection(readonly=True) as conn:
        markets = load_markets_by_slug(conn, slugs)
        lifecycles = [lifecycle_for_market(market) for market in markets]
        tokens_by_market: dict[int, list[NbaToken]] = {}
        for market, lifecycle in zip(markets, lifecycles, strict=True):
            start = _required_hour(lifecycle.start_hour, market.market_slug)
            end = _required_hour(lifecycle.end_hour, market.market_slug)
            tokens_by_market[market.market_id] = load_market_tokens(conn, [market], start_hour=start, end_hour=end)

    mappings = build_token_mappings(tokens_by_market)
    ch = ClickHouseClient()
    orderfilled_build_tag = str(args.orderfilled_build_tag or args.build_tag)
    l2_build_tag = str(args.l2_build_tag or args.build_tag)
    fills = load_orderfilled_rows(ch, mappings, build_tag=orderfilled_build_tag)
    l2_events = load_l2_events(ch, mappings, build_tag=l2_build_tag)

    market_reports: list[dict[str, Any]] = []
    for market, lifecycle in zip(markets, lifecycles, strict=True):
        report = build_market_report(
            market,
            lifecycle=asdict(lifecycle),
            tokens=tokens_by_market.get(market.market_id, []),
            mappings=[mapping for mapping in mappings if mapping.market_id == market.market_id],
            fills=[row for row in fills if int(row.get("market_id") or 0) == market.market_id],
            l2_events=[row for row in l2_events if int(row.get("market_id") or 0) == market.market_id],
            thresholds=thresholds,
        )
        market_reports.append(report)
        write_market_report(output_dir, report)

    summary = build_summary_report(args, slugs=slugs, thresholds=thresholds, market_reports=market_reports)
    (output_dir / "single_game_nba_l2_fill_coverage_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    (output_dir / "single_game_nba_l2_fill_coverage_summary.md").write_text(
        summary_to_markdown(summary),
        encoding="utf-8",
    )
    print(summary_to_markdown(summary))
    return 0 if summary["status"] in {"ready", "review"} else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-slug", action="append", default=[])
    parser.add_argument("--build-tag", default=DEFAULT_BUILD_TAG)
    parser.add_argument("--orderfilled-build-tag", default=None)
    parser.add_argument("--l2-build-tag", default=None)
    parser.add_argument("--freshness-seconds", action="append", type=int, default=list(DEFAULT_THRESHOLDS))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR / "l2_fill_coverage"))
    return parser


def build_token_mappings(tokens_by_market: Mapping[int, Sequence[NbaToken]]) -> list[TokenMapping]:
    mappings: list[TokenMapping] = []
    for tokens in tokens_by_market.values():
        for token in tokens:
            fill_token_id = orderfilled_replay_token_id(token)
            if not fill_token_id or not token.token_id:
                continue
            mappings.append(
                TokenMapping(
                    market_id=int(token.market_id),
                    market_slug=str(token.market_slug),
                    token_side=str(token.token_side or ""),
                    fill_token_id=fill_token_id,
                    l2_token_id=str(token.token_id),
                )
            )
    return mappings


def load_orderfilled_rows(client: ClickHouseClient, mappings: Sequence[TokenMapping], *, build_tag: str) -> list[dict[str, Any]]:
    if not mappings:
        return []
    pairs_sql = ",".join(f"({m.market_id}, '{_ch_escape(m.fill_token_id)}')" for m in mappings)
    tag = _quote(build_tag)
    rows = client.query_json_rows(
        f"""
        SELECT
            f.market_id,
            f.token_id AS fill_token_id,
            f.block_number,
            f.transaction_index,
            f.log_index,
            f.tx_hash,
            f.trade_price,
            f.size,
            f.side,
            f.canonical_fill_key,
            f.canonical_fill_key_kind,
            toUnixTimestamp64Milli(bt.block_time) AS fill_ts_ms,
            formatDateTime(bt.block_time, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS fill_time
        FROM (
            SELECT *
            FROM orderfilled_trade_replay FINAL
            PREWHERE (market_id, token_id) IN ({pairs_sql})
            WHERE build_tag = {tag}
        ) AS f
        LEFT JOIN block_timestamps AS bt ON bt.block_number = f.block_number
        ORDER BY f.market_id ASC, f.token_id ASC, f.block_number ASC, f.transaction_index ASC, f.log_index ASC, f.tx_hash ASC, f.canonical_fill_key ASC
        """,
        timeout_seconds=240,
    )
    return fill_missing_orderfilled_timestamps(rows, client=client)


def fill_missing_orderfilled_timestamps(rows: Sequence[Mapping[str, Any]], *, client: ClickHouseClient) -> list[dict[str, Any]]:
    enriched = [dict(row) for row in rows]
    missing_blocks = sorted({
        int(row.get("block_number") or 0)
        for row in enriched
        if int(row.get("block_number") or 0) > 0 and int(row.get("fill_ts_ms") or 0) <= 0
    })
    timestamps = load_block_timestamps_with_interpolation(missing_blocks, client=client) if missing_blocks else {}
    for row in enriched:
        if int(row.get("fill_ts_ms") or 0) > 0:
            row["fill_timestamp_source"] = "block_timestamps"
            continue
        block_number = int(row.get("block_number") or 0)
        timestamp = timestamps.get(block_number)
        if not timestamp:
            row["fill_timestamp_source"] = "missing"
            continue
        row["fill_time"] = timestamp
        row["fill_ts_ms"] = iso_to_ms(timestamp)
        row["fill_timestamp_source"] = "block_timestamps_interpolated"
    return enriched


def load_l2_events(client: ClickHouseClient, mappings: Sequence[TokenMapping], *, build_tag: str) -> list[dict[str, Any]]:
    if not mappings:
        return []
    pairs_sql = ",".join(f"({m.market_id}, '{_ch_escape(m.l2_token_id)}')" for m in mappings)
    tag = _quote(build_tag)
    return client.query_json_rows(
        f"""
        SELECT
            market_id,
            market_slug,
            token_id AS l2_token_id,
            token_side,
            event_ts_ms,
            formatDateTime(event_time, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') AS event_time,
            event_type,
            operation,
            side,
            price,
            size
        FROM pmxt_l2_event_replay FINAL
        PREWHERE (market_id, token_id) IN ({pairs_sql})
        WHERE build_tag = {tag}
        ORDER BY market_id ASC, token_id ASC, event_ts_ms ASC, source_hour ASC, source_row_index ASC, source_event_index ASC
        """,
        timeout_seconds=240,
    )


def build_market_report(
    market: NbaMarket,
    *,
    lifecycle: Mapping[str, Any],
    tokens: Sequence[NbaToken],
    mappings: Sequence[TokenMapping],
    fills: Sequence[Mapping[str, Any]],
    l2_events: Sequence[Mapping[str, Any]],
    thresholds: Sequence[int],
) -> dict[str, Any]:
    lifecycle_hours = all_hours(_parse_hour(str(lifecycle["start_hour"])), _parse_hour(str(lifecycle["end_hour"])))
    fill_token_to_l2 = {mapping.fill_token_id: mapping.l2_token_id for mapping in mappings}
    token_side_by_l2 = {mapping.l2_token_id: mapping.token_side for mapping in mappings}
    l2_by_token: dict[str, list[Mapping[str, Any]]] = {}
    for row in l2_events:
        l2_by_token.setdefault(str(row.get("l2_token_id") or ""), []).append(row)
    for rows in l2_by_token.values():
        rows.sort(key=lambda row: int(row.get("event_ts_ms") or 0))

    fill_matches = match_fills_to_l2(fills, fill_token_to_l2=fill_token_to_l2, l2_by_token=l2_by_token, thresholds=thresholds)
    fill_ts_values = [int(row.get("fill_ts_ms") or 0) for row in fills if int(row.get("fill_ts_ms") or 0) > 0]
    l2_ts_values = [int(row.get("event_ts_ms") or 0) for row in l2_events if int(row.get("event_ts_ms") or 0) > 0]
    lifecycle_hour_set = {_hour_floor_ms(_dt_to_ms(hour)) for hour in lifecycle_hours}
    l2_active_hours = {_hour_floor_ms(int(row.get("event_ts_ms") or 0)) for row in l2_events if int(row.get("event_ts_ms") or 0) > 0}
    fill_span_hours = _hour_range_ms(min(fill_ts_values), max(fill_ts_values)) if fill_ts_values else set()
    orderfilled_stats = build_orderfilled_stats(fills, fill_ts_values)
    l2_stats = build_l2_stats(l2_events, l2_ts_values)
    coverage = {
        "lifecycle_hours": len(lifecycle_hour_set),
        "l2_active_hours": len(l2_active_hours & lifecycle_hour_set),
        "l2_active_hour_coverage_pct": pct(len(l2_active_hours & lifecycle_hour_set), len(lifecycle_hour_set)),
        "orderfilled_span_hours": len(fill_span_hours),
        "l2_active_hours_in_orderfilled_span": len(l2_active_hours & fill_span_hours),
        "l2_active_hour_coverage_in_orderfilled_span_pct": pct(len(l2_active_hours & fill_span_hours), len(fill_span_hours)),
        "l2_time_overlap_with_orderfilled_span_pct": interval_overlap_pct(l2_ts_values, fill_ts_values),
    }
    token_reports = []
    for mapping in mappings:
        token_fills = [row for row in fills if str(row.get("fill_token_id") or "") == mapping.fill_token_id]
        token_l2 = l2_by_token.get(mapping.l2_token_id, [])
        token_fill_ts = [int(row.get("fill_ts_ms") or 0) for row in token_fills if int(row.get("fill_ts_ms") or 0) > 0]
        token_l2_ts = [int(row.get("event_ts_ms") or 0) for row in token_l2 if int(row.get("event_ts_ms") or 0) > 0]
        token_matches = [row for row in fill_matches["sampled_all_matches"] if row.get("fill_token_id") == mapping.fill_token_id]
        token_reports.append(
            {
                "token_side": mapping.token_side,
                "fill_token_id": mapping.fill_token_id,
                "l2_token_id": mapping.l2_token_id,
                "orderfilled_rows": len(token_fills),
                "orderfilled_first": ms_to_iso(min(token_fill_ts)) if token_fill_ts else None,
                "orderfilled_last": ms_to_iso(max(token_fill_ts)) if token_fill_ts else None,
                "l2_rows": len(token_l2),
                "l2_snapshots": sum(1 for row in token_l2 if row.get("event_type") == "book_snapshot"),
                "l2_price_changes": sum(1 for row in token_l2 if row.get("event_type") == "price_change"),
                "l2_first": ms_to_iso(min(token_l2_ts)) if token_l2_ts else None,
                "l2_last": ms_to_iso(max(token_l2_ts)) if token_l2_ts else None,
                "fills_with_any_l2_before_pct": pct(sum(1 for row in token_matches if row.get("matched_any_l2_before")), len(token_fills)),
            }
        )
    return {
        "schema_version": "single_game_nba_l2_fill_coverage_market_v1",
        "status": "ready" if l2_events and fills else "review",
        "market": asdict(market),
        "lifecycle": dict(lifecycle),
        "tokens": [asdict(token) for token in tokens],
        "orderfilled": orderfilled_stats,
        "pmxt_l2": l2_stats,
        "coverage": coverage,
        "fill_l2_match": fill_matches["summary"],
        "token_reports": token_reports,
        "samples": {
            "fresh_matches": fill_matches["fresh_samples"][:10],
            "stale_matches": fill_matches["stale_samples"][:10],
            "missing_l2_matches": fill_matches["missing_samples"][:10],
        },
        "notes": build_market_notes(orderfilled_stats, l2_stats, coverage, fill_matches["summary"]),
    }


def match_fills_to_l2(
    fills: Sequence[Mapping[str, Any]],
    *,
    fill_token_to_l2: Mapping[str, str],
    l2_by_token: Mapping[str, Sequence[Mapping[str, Any]]],
    thresholds: Sequence[int],
) -> dict[str, Any]:
    l2_ts_by_token = {
        token_id: [int(row.get("event_ts_ms") or 0) for row in rows if int(row.get("event_ts_ms") or 0) > 0]
        for token_id, rows in l2_by_token.items()
    }
    total = len(fills)
    with_ts = 0
    matched_any = 0
    threshold_counts = {int(threshold): 0 for threshold in thresholds}
    lags: list[int] = []
    fresh_samples: list[dict[str, Any]] = []
    stale_samples: list[dict[str, Any]] = []
    missing_samples: list[dict[str, Any]] = []
    sampled_all_matches: list[dict[str, Any]] = []
    for row in fills:
        fill_ts = int(row.get("fill_ts_ms") or 0)
        fill_token_id = str(row.get("fill_token_id") or "")
        l2_token_id = fill_token_to_l2.get(fill_token_id, "")
        sample = {
            "market_id": row.get("market_id"),
            "fill_token_id": fill_token_id,
            "l2_token_id": l2_token_id,
            "block_number": row.get("block_number"),
            "fill_time": row.get("fill_time"),
            "price": str(row.get("trade_price")),
            "size": str(row.get("size")),
            "side": row.get("side"),
        }
        if fill_ts <= 0:
            missing_samples.append(sample | {"reason": "missing_block_timestamp"})
            continue
        with_ts += 1
        l2_ts = l2_ts_by_token.get(l2_token_id, [])
        idx = bisect_right(l2_ts, fill_ts) - 1
        if idx < 0:
            missing_samples.append(sample | {"reason": "no_l2_before_fill"})
            sampled_all_matches.append(sample | {"matched_any_l2_before": False})
            continue
        matched_any += 1
        latest_l2_ts = l2_ts[idx]
        lag = max(0, fill_ts - latest_l2_ts)
        lag_sec = lag // 1000
        lags.append(lag_sec)
        matched_sample = sample | {
            "matched_any_l2_before": True,
            "latest_l2_time": ms_to_iso(latest_l2_ts),
            "l2_lag_seconds": lag_sec,
        }
        sampled_all_matches.append(matched_sample)
        for threshold in thresholds:
            if lag_sec <= int(threshold):
                threshold_counts[int(threshold)] += 1
        if thresholds and lag_sec <= min(thresholds):
            if len(fresh_samples) < 10:
                fresh_samples.append(matched_sample)
        elif len(stale_samples) < 10:
            stale_samples.append(matched_sample)
    summary = {
        "fill_rows": total,
        "fill_rows_with_timestamp": with_ts,
        "missing_block_timestamp_rows": total - with_ts,
        "fills_with_any_l2_before": matched_any,
        "fills_with_any_l2_before_pct": pct(matched_any, with_ts),
        "freshness_thresholds": {
            f"{threshold}s": {
                "matched_rows": count,
                "matched_pct": pct(count, with_ts),
            }
            for threshold, count in threshold_counts.items()
        },
        "l2_lag_seconds_min": min(lags) if lags else None,
        "l2_lag_seconds_p50": percentile(lags, 0.5),
        "l2_lag_seconds_p95": percentile(lags, 0.95),
        "l2_lag_seconds_max": max(lags) if lags else None,
    }
    return {
        "summary": summary,
        "fresh_samples": fresh_samples,
        "stale_samples": stale_samples,
        "missing_samples": missing_samples,
        "sampled_all_matches": sampled_all_matches,
    }


def build_orderfilled_stats(fills: Sequence[Mapping[str, Any]], fill_ts_values: Sequence[int]) -> dict[str, Any]:
    blocks = [int(row.get("block_number") or 0) for row in fills if int(row.get("block_number") or 0) > 0]
    timestamp_sources = dict(Counter(str(row.get("fill_timestamp_source") or "unknown") for row in fills))
    return {
        "rows": len(fills),
        "first_block": min(blocks) if blocks else None,
        "last_block": max(blocks) if blocks else None,
        "first_time": ms_to_iso(min(fill_ts_values)) if fill_ts_values else None,
        "last_time": ms_to_iso(max(fill_ts_values)) if fill_ts_values else None,
        "rows_with_timestamp": len(fill_ts_values),
        "timestamp_sources": timestamp_sources,
        "canonical_rows": sum(1 for row in fills if row.get("canonical_fill_key_kind") == "canonical"),
        "fallback_rows": sum(1 for row in fills if row.get("canonical_fill_key_kind") != "canonical"),
    }


def build_l2_stats(l2_events: Sequence[Mapping[str, Any]], l2_ts_values: Sequence[int]) -> dict[str, Any]:
    return {
        "rows": len(l2_events),
        "book_snapshots": sum(1 for row in l2_events if row.get("event_type") == "book_snapshot"),
        "price_changes": sum(1 for row in l2_events if row.get("event_type") == "price_change"),
        "first_time": ms_to_iso(min(l2_ts_values)) if l2_ts_values else None,
        "last_time": ms_to_iso(max(l2_ts_values)) if l2_ts_values else None,
    }


def build_market_notes(
    orderfilled: Mapping[str, Any],
    l2: Mapping[str, Any],
    coverage: Mapping[str, Any],
    match: Mapping[str, Any],
) -> list[str]:
    notes: list[str] = []
    if l2.get("last_time") and orderfilled.get("last_time") and str(l2["last_time"]) < str(orderfilled["last_time"]):
        notes.append("PMXT L2 ends before the last OrderFilled fill; late fills will only match stale L2 or no fresh L2.")
    if float(coverage.get("l2_active_hour_coverage_pct") or 0) < 10:
        notes.append("L2 active-hour coverage is low; DEPTH is only reliable inside observed active L2 windows.")
    fresh_5m = ((match.get("freshness_thresholds") or {}).get("300s") or {}).get("matched_pct")
    if fresh_5m is not None and float(fresh_5m) < 50:
        notes.append("Most fills do not have a fresh 5-minute L2 observation; use OrderFilled/fill-first as the primary execution evidence.")
    return notes


def build_summary_report(
    args: argparse.Namespace,
    *,
    slugs: Sequence[str],
    thresholds: Sequence[int],
    market_reports: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    total_fills = sum(int((report.get("orderfilled") or {}).get("rows") or 0) for report in market_reports)
    total_l2 = sum(int((report.get("pmxt_l2") or {}).get("rows") or 0) for report in market_reports)
    matched_any = sum(int((report.get("fill_l2_match") or {}).get("fills_with_any_l2_before") or 0) for report in market_reports)
    timestamp_fills = sum(int((report.get("fill_l2_match") or {}).get("fill_rows_with_timestamp") or 0) for report in market_reports)
    return {
        "schema_version": "single_game_nba_l2_fill_coverage_summary_v1",
        "status": "ready" if market_reports else "missing",
        "parameters": {
            "market_slug": list(slugs),
            "build_tag": str(args.build_tag),
            "orderfilled_build_tag": str(args.orderfilled_build_tag or args.build_tag),
            "l2_build_tag": str(args.l2_build_tag or args.build_tag),
            "freshness_seconds": list(thresholds),
        },
        "aggregate": {
            "markets": len(market_reports),
            "orderfilled_rows": total_fills,
            "pmxt_l2_rows": total_l2,
            "fills_with_any_l2_before": matched_any,
            "fills_with_any_l2_before_pct": pct(matched_any, timestamp_fills),
        },
        "markets": list(market_reports),
    }


def write_market_report(output_dir: Path, report: Mapping[str, Any]) -> None:
    slug = str(((report.get("market") or {}).get("market_slug") or "market")).replace("/", "-")
    (output_dir / f"{slug}_l2_fill_coverage.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    (output_dir / f"{slug}_l2_fill_coverage.md").write_text(market_to_markdown(report), encoding="utf-8")


def summary_to_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        f"# NBA Single-Game L2 vs OrderFilled Coverage: {report.get('status')}",
        "",
        f"- markets: `{(report.get('aggregate') or {}).get('markets')}`",
        f"- orderfilled_rows: `{(report.get('aggregate') or {}).get('orderfilled_rows')}`",
        f"- pmxt_l2_rows: `{(report.get('aggregate') or {}).get('pmxt_l2_rows')}`",
        f"- fills_with_any_l2_before_pct: `{(report.get('aggregate') or {}).get('fills_with_any_l2_before_pct')}`",
        "",
        "| market | OrderFilled span | L2 span | L2 rows | active-hour coverage | any L2 before fill | 5m fresh | 1h fresh |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in report.get("markets") or []:
        market = item.get("market") or {}
        orderfilled = item.get("orderfilled") or {}
        l2 = item.get("pmxt_l2") or {}
        coverage = item.get("coverage") or {}
        match = item.get("fill_l2_match") or {}
        thresholds = match.get("freshness_thresholds") or {}
        lines.append(
            "| "
            f"`{market.get('market_slug')}` | "
            f"{orderfilled.get('first_time')} -> {orderfilled.get('last_time')} | "
            f"{l2.get('first_time')} -> {l2.get('last_time')} | "
            f"{l2.get('rows')} | "
            f"{coverage.get('l2_active_hour_coverage_pct')}% | "
            f"{match.get('fills_with_any_l2_before_pct')}% | "
            f"{(thresholds.get('300s') or {}).get('matched_pct')}% | "
            f"{(thresholds.get('3600s') or {}).get('matched_pct')}% |"
        )
    lines.extend(["", "## Output Files", ""])
    for item in report.get("markets") or []:
        slug = (item.get("market") or {}).get("market_slug")
        lines.append(f"- `{slug}_l2_fill_coverage.md`")
    return "\n".join(lines) + "\n"


def market_to_markdown(report: Mapping[str, Any]) -> str:
    market = report.get("market") or {}
    orderfilled = report.get("orderfilled") or {}
    l2 = report.get("pmxt_l2") or {}
    coverage = report.get("coverage") or {}
    match = report.get("fill_l2_match") or {}
    thresholds = match.get("freshness_thresholds") or {}
    lines = [
        f"# {market.get('market_slug')} L2 vs OrderFilled Coverage",
        "",
        f"- status: `{report.get('status')}`",
        f"- title: `{market.get('market_title')}`",
        f"- condition_id: `{market.get('condition_id')}`",
        "",
        "## Data Windows",
        "",
        f"- lifecycle: `{(report.get('lifecycle') or {}).get('start_hour')}` -> `{(report.get('lifecycle') or {}).get('end_hour')}`",
        f"- OrderFilled: `{orderfilled.get('first_time')}` -> `{orderfilled.get('last_time')}`",
        f"- OrderFilled blocks: `{orderfilled.get('first_block')}` -> `{orderfilled.get('last_block')}`",
        f"- PMXT L2: `{l2.get('first_time')}` -> `{l2.get('last_time')}`",
        "",
        "## Coverage",
        "",
        f"- lifecycle hours: `{coverage.get('lifecycle_hours')}`",
        f"- L2 active hours: `{coverage.get('l2_active_hours')}`",
        f"- L2 active-hour coverage: `{coverage.get('l2_active_hour_coverage_pct')}%`",
        f"- OrderFilled span hours: `{coverage.get('orderfilled_span_hours')}`",
        f"- L2 active hours inside OrderFilled span: `{coverage.get('l2_active_hours_in_orderfilled_span')}`",
        f"- L2 active-hour coverage inside OrderFilled span: `{coverage.get('l2_active_hour_coverage_in_orderfilled_span_pct')}%`",
        f"- L2 time overlap with OrderFilled span: `{coverage.get('l2_time_overlap_with_orderfilled_span_pct')}%`",
        "",
        "## Fill Match",
        "",
        f"- fill rows: `{match.get('fill_rows')}`",
        f"- fills with block timestamp: `{match.get('fill_rows_with_timestamp')}`",
        f"- fills with any L2 before fill: `{match.get('fills_with_any_l2_before')}` / `{match.get('fills_with_any_l2_before_pct')}%`",
        f"- L2 lag seconds p50/p95/max: `{match.get('l2_lag_seconds_p50')}` / `{match.get('l2_lag_seconds_p95')}` / `{match.get('l2_lag_seconds_max')}`",
        "",
        "| threshold | matched rows | matched pct |",
        "| ---: | ---: | ---: |",
    ]
    for key, value in thresholds.items():
        lines.append(f"| `{key}` | {value.get('matched_rows')} | {value.get('matched_pct')}% |")
    lines.extend([
        "",
        "## Token Breakdown",
        "",
        "| token | OrderFilled rows | PMXT L2 rows | snapshots | price changes | OrderFilled span | L2 span | any L2 before fill |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- | ---: |",
    ])
    for token in report.get("token_reports") or []:
        lines.append(
            "| "
            f"`{token.get('token_side')}` | "
            f"{token.get('orderfilled_rows')} | "
            f"{token.get('l2_rows')} | "
            f"{token.get('l2_snapshots')} | "
            f"{token.get('l2_price_changes')} | "
            f"{token.get('orderfilled_first')} -> {token.get('orderfilled_last')} | "
            f"{token.get('l2_first')} -> {token.get('l2_last')} | "
            f"{token.get('fills_with_any_l2_before_pct')}% |"
        )
    if report.get("notes"):
        lines.extend(["", "## Notes", ""])
        lines.extend(f"- {note}" for note in report.get("notes") or [])
    return "\n".join(lines) + "\n"


def pct(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return round(float(numerator) * 100.0 / float(denominator), 4)


def percentile(values: Sequence[int], q: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * float(q)))))
    return int(ordered[idx])


def interval_overlap_pct(l2_ts: Sequence[int], fill_ts: Sequence[int]) -> float:
    if not l2_ts or not fill_ts:
        return 0.0
    l2_start, l2_end = min(l2_ts), max(l2_ts)
    fill_start, fill_end = min(fill_ts), max(fill_ts)
    if fill_end <= fill_start:
        return 100.0 if l2_start <= fill_start <= l2_end else 0.0
    overlap = max(0, min(l2_end, fill_end) - max(l2_start, fill_start))
    return pct(overlap, fill_end - fill_start)


def _hour_range_ms(start_ms: int, end_ms: int) -> set[int]:
    if start_ms <= 0 or end_ms <= 0:
        return set()
    start_hour = _hour_floor_ms(start_ms)
    end_hour = _hour_floor_ms(end_ms)
    hours: set[int] = set()
    current = start_hour
    while current <= end_hour:
        hours.add(current)
        current += 3_600_000
    return hours


def _hour_floor_ms(value: int) -> int:
    return int(value) - (int(value) % 3_600_000)


def _dt_to_ms(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return int(value.timestamp() * 1000)


def _parse_hour(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def ms_to_iso(value: int | None) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(int(value) / 1000, tz=UTC).isoformat().replace("+00:00", "Z")


def iso_to_ms(value: str) -> int:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def _quote(value: str) -> str:
    return "'" + _ch_escape(value) + "'"


def _ch_escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


if __name__ == "__main__":
    raise SystemExit(main())
