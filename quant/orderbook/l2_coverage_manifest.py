"""Build token/hour L2 archive coverage manifests for backtests."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

os.environ["OPENBLAS_NUM_THREADS"] = os.environ.get("BOOK_L2_OPENBLAS_NUM_THREADS", "1")
os.environ["OMP_NUM_THREADS"] = os.environ.get("BOOK_L2_OMP_NUM_THREADS", "1")
os.environ["MKL_NUM_THREADS"] = os.environ.get("BOOK_L2_MKL_NUM_THREADS", "1")
os.environ["NUMEXPR_NUM_THREADS"] = os.environ.get("BOOK_L2_NUMEXPR_NUM_THREADS", "1")

import duckdb
import pandas as pd

from quant.core.db import postgres_connection

from .l2_subscription_snapshot import subscription_affinity_shard
from .three_source_gold_overlay import (
    VerifiedThreeSourceGoldSet,
    load_verified_three_source_gold,
)

DEFAULT_ARCHIVE_DIR = Path("runtime_outputs/lob_l2_archive_full")
DEFAULT_OUTPUT = Path("runtime_outputs/lob_l2_coverage_manifest/latest.parquet")
DEFAULT_COVERAGE_TABLE = "quant.clob_l2_token_hour_coverage"
GCP_BATCH_COVERAGE_TABLE = "quant.clob_l2_token_hour_coverage_gcp_batch"
GCP_EXECUTION_REDUNDANT_COVERAGE_TABLE = (
    "quant.clob_l2_token_hour_coverage_gcp_execution_redundant"
)
GCP_HOT_STANDBY_PATCH_COVERAGE_TABLE = (
    "quant.clob_l2_token_hour_coverage_gcp_hot_standby_patch"
)
ACTIVE_ACTIVE_COVERAGE_TABLE = "quant.clob_l2_active_active_token_hour_coverage"
ALLOWED_COVERAGE_TABLES = (
    DEFAULT_COVERAGE_TABLE,
    GCP_BATCH_COVERAGE_TABLE,
    GCP_EXECUTION_REDUNDANT_COVERAGE_TABLE,
    GCP_HOT_STANDBY_PATCH_COVERAGE_TABLE,
    ACTIVE_ACTIVE_COVERAGE_TABLE,
)
THREE_SOURCE_GOLD_VALIDATION_STATUSES = frozenset(
    {"DISABLED", "NO_MATCHING_MANIFEST", "PASS"}
)


def coverage_table_sql(value: str) -> str:
    if value not in ALLOWED_COVERAGE_TABLES:
        raise ValueError(f"unsupported L2 coverage table: {value}")
    return value


@dataclass(frozen=True)
class L2CoverageManifestSummary:
    archive_dir: str
    output_path: str
    since: str
    until: str | None
    desired_tokens: int
    token_hours: int
    fill_depth_ready_token_hours: int
    no_book_token_hours: int
    one_sided_book_token_hours: int
    no_price_change_token_hours: int
    rest_seed_only_token_hours: int
    ws_gap_rest_seeded_token_hours: int
    ws_gap_recovered_token_hours: int
    ws_gap_unseeded_token_hours: int
    rest_error_token_hours: int
    stream_stale_token_hours: int
    generated_at: str
    three_source_gold_manifest_count: int = 0
    three_source_gold_overlay_event_count: int = 0
    three_source_gold_repair_window_count: int = 0
    three_source_gold_manifest_sha256: tuple[str, ...] = ()
    three_source_gold_validation_status: str = "DISABLED"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--since", default=None, help="TIMESTAMPTZ cutoff. Defaults to now - --lookback-hours.")
    parser.add_argument("--until", default=None, help="Optional TIMESTAMPTZ upper bound.")
    parser.add_argument("--lookback-hours", type=float, default=8.0)
    parser.add_argument(
        "--baseline-lookback-hours",
        type=float,
        default=6.0,
        help="Extra archive hours to scan before --since so a token-hour can find an earlier book baseline.",
    )
    parser.add_argument(
        "--inherit-verified-baselines",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Reuse prior baselines only when their archive hour still has a ready manifest.",
    )
    parser.add_argument(
        "--recovery-lookahead-hours",
        type=float,
        default=1.0,
        help="Extra archive time scanned after --until to pair connection gaps with recovery snapshots.",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=None,
        help="Optional shard_count filter. Defaults to all shard generations in the archive window.",
    )
    parser.add_argument(
        "--connection-count",
        type=int,
        default=0,
        help="Logical WS connection count used to map tokens to stream-lag evidence. Zero disables this gate.",
    )
    parser.add_argument(
        "--max-upstream-lag-p99-seconds",
        type=float,
        default=0.0,
        help=(
            "Report a connection-hour as latency-stale when p99 source-to-receive "
            "lag exceeds this value. Continuity remains proven by the minimum "
            "sample count and explicit gap evidence. Zero disables."
        ),
    )
    parser.add_argument("--min-connection-lag-samples", type=int, default=100)
    parser.add_argument(
        "--heartbeat-continuity-tolerance-seconds",
        type=float,
        default=90.0,
        help=(
            "Maximum distance from each hour edge, and maximum internal gap, "
            "for 30-second connection_heartbeat continuity proof."
        ),
    )
    parser.add_argument(
        "--pmxt-supplement-cache-dir",
        type=Path,
        help="PMXT hourly coverage cache. Exact overlapping hours are added to the desired token universe.",
    )
    parser.add_argument(
        "--require-pmxt-supplement",
        action="store_true",
        help="Fail closed when an exact PMXT cache hour is unavailable.",
    )
    parser.add_argument(
        "--fail-closed-shard-gaps",
        type=Path,
        help=(
            "JSON evidence for source/shard intervals whose raw frames are absent. "
            "Every desired token mapped to an overlapping shard-hour is blocked."
        ),
    )
    parser.add_argument(
        "--three-source-gold-root",
        type=Path,
        help=(
            "Optional immutable three-source Gold root. Matching manifests are "
            "strictly SHA/state validated before their overlays or repair windows "
            "can affect coverage."
        ),
    )
    parser.add_argument("--write-db", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--execution-only",
        action="store_true",
        help="Build rows only for execution-eligible tokens in the redundant raw feed.",
    )
    parser.add_argument("--coverage-table", choices=ALLOWED_COVERAGE_TABLES, default=DEFAULT_COVERAGE_TABLE)
    parser.add_argument(
        "--migrate-db-schema",
        action="store_true",
        help="Run the one-time coverage table migration and exit.",
    )
    parser.add_argument("--json-out", type=Path, default=None)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.migrate_db_schema:
        migrate_coverage_table_schema(args.coverage_table)
        print(
            json.dumps(
                {
                    "status": "migrated",
                    "coverage_table": args.coverage_table,
                },
                sort_keys=True,
            )
        )
        return 0
    summary = build_l2_coverage_manifest(
        archive_dir=args.archive_dir,
        output_path=args.output,
        since=_resolve_since(args.since, args.lookback_hours),
        until=_parse_datetime(args.until) if args.until else None,
        shard_count=args.shard_count,
        connection_count=args.connection_count,
        max_upstream_lag_p99_seconds=args.max_upstream_lag_p99_seconds,
        min_connection_lag_samples=args.min_connection_lag_samples,
        heartbeat_continuity_tolerance_seconds=(
            args.heartbeat_continuity_tolerance_seconds
        ),
        baseline_lookback_hours=args.baseline_lookback_hours,
        recovery_lookahead_hours=args.recovery_lookahead_hours,
        inherit_verified_baselines=args.inherit_verified_baselines,
        pmxt_supplement_cache_dir=args.pmxt_supplement_cache_dir,
        require_pmxt_supplement=bool(args.require_pmxt_supplement),
        fail_closed_shard_gaps_path=args.fail_closed_shard_gaps,
        three_source_gold_root=args.three_source_gold_root,
        execution_only=bool(args.execution_only),
        write_db=args.write_db,
        coverage_table=args.coverage_table,
    )
    text = json.dumps(summary.as_dict(), ensure_ascii=False, indent=2, default=str)
    print(text)
    if args.json_out is not None:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(text + "\n", encoding="utf-8")
    return 0


def build_l2_coverage_manifest(
    *,
    archive_dir: Path | str = DEFAULT_ARCHIVE_DIR,
    output_path: Path | str = DEFAULT_OUTPUT,
    since: datetime,
    until: datetime | None = None,
    shard_count: int | None = None,
    connection_count: int = 0,
    max_upstream_lag_p99_seconds: float = 0.0,
    min_connection_lag_samples: int = 100,
    heartbeat_continuity_tolerance_seconds: float = 90.0,
    desired: pd.DataFrame | None = None,
    gap_events: pd.DataFrame | None = None,
    shard_gap_events: pd.DataFrame | None = None,
    baseline_seeds: pd.DataFrame | None = None,
    baseline_lookback_hours: float = 6.0,
    recovery_lookahead_hours: float = 1.0,
    inherit_verified_baselines: bool = False,
    pmxt_supplement_cache_dir: Path | None = None,
    require_pmxt_supplement: bool = False,
    fail_closed_shard_gaps_path: Path | None = None,
    three_source_gold_root: Path | None = None,
    execution_only: bool = False,
    write_db: bool = False,
    coverage_table: str = DEFAULT_COVERAGE_TABLE,
) -> L2CoverageManifestSummary:
    """Write a Parquet manifest that marks each desired token-hour as usable or blocked."""

    # This timestamp is the evidence snapshot boundary, not the end of a
    # potentially long DuckDB/PostgreSQL build. A Parquet batch imported while
    # this build is running must compare newer than the resulting rows so the
    # bounded repair lane cannot miss it.
    generated_at = datetime.now(timezone.utc)
    archive_path = Path(archive_dir)
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    files = _archive_files(
        archive_path,
        since=since,
        until=until,
        extra_lookback_hours=baseline_lookback_hours,
        extra_lookahead_hours=recovery_lookahead_hours,
    )
    gold = load_verified_three_source_gold(
        three_source_gold_root,
        # Formal repair requests are positive single-hour gaps. Gold contains
        # price-change overlays, not book baselines or lookahead recovery
        # markers, so only the materialized coverage interval may contribute.
        since=since,
        until=until,
        memory_limit=os.environ.get("BOOK_L2_GOLD_VALIDATION_MEMORY_LIMIT", "2GB"),
    )
    gold_root_configured = three_source_gold_root is not None and bool(
        str(three_source_gold_root).strip()
    )
    gold_validation_status = (
        "DISABLED"
        if not gold_root_configured
        else "PASS"
        if gold.overlays
        else "NO_MATCHING_MANIFEST"
    )
    if desired is None:
        historical_desired = _load_existing_desired_tokens(
            since=since,
            until=until,
            coverage_table=coverage_table,
        )
        desired_inputs = [historical_desired, _load_desired_tokens()]
        if pmxt_supplement_cache_dir is not None:
            desired_inputs.append(
                _load_pmxt_supplement_desired(
                    Path(pmxt_supplement_cache_dir),
                    since=since,
                    until=until,
                    required=require_pmxt_supplement,
                )
            )
        desired_df = pd.concat(desired_inputs, ignore_index=True)
        desired_df = _normalize_desired(desired_df)
        if execution_only:
            desired_df = desired_df[desired_df["execution_eligible"]].reset_index(drop=True)
        lifecycle_hour_df = _load_lifecycle_hour_states(
            asset_ids=desired_df["asset_id"].tolist(),
            since=since,
            until=until,
        )
    else:
        desired_df = _normalize_desired(desired.copy())
        if execution_only:
            desired_df = desired_df[desired_df["execution_eligible"]].reset_index(drop=True)
        lifecycle_hour_df = _empty_lifecycle_hour_states()
    if gap_events is not None:
        gap_df = gap_events.copy()
    elif coverage_table in {
        GCP_BATCH_COVERAGE_TABLE,
        GCP_EXECUTION_REDUNDANT_COVERAGE_TABLE,
        GCP_HOT_STANDBY_PATCH_COVERAGE_TABLE,
    }:
        # The legacy DB gap table is written by the retired local collector and
        # has no source column. GCP continuity evidence lives in its Parquet
        # connection_gap/connection_recovered rows.
        gap_df = _empty_gap_events()
    else:
        gap_df = _load_gap_events(since=since, until=until)
    if shard_gap_events is not None:
        shard_gap_df = shard_gap_events.copy()
    elif fail_closed_shard_gaps_path is not None:
        shard_gap_df = _load_fail_closed_shard_gaps(
            Path(fail_closed_shard_gaps_path),
            since=since,
            until=until,
        )
    else:
        shard_gap_df = _empty_shard_gap_events()
    if baseline_seeds is not None:
        baseline_seed_df = baseline_seeds.copy()
    elif inherit_verified_baselines and desired is None:
        baseline_seed_df = _load_baseline_seeds(
            since=since,
            coverage_table=coverage_table,
            asset_ids=desired_df["asset_id"].tolist(),
        )
    else:
        baseline_seed_df = _empty_baseline_seeds()
    stream_lag_gate_enabled = (
        int(connection_count) > 0 and float(max_upstream_lag_p99_seconds) > 0
    )
    heartbeat_tolerance_seconds = max(
        1,
        int(round(float(heartbeat_continuity_tolerance_seconds))),
    )
    stream_fresh_sql = (
        "TRUE"
        if not stream_lag_gate_enabled
        else "COALESCE(c.connection_stream_fresh, FALSE)"
    )
    stream_proven_sql = (
        "TRUE"
        if not stream_lag_gate_enabled
        else "COALESCE(c.connection_stream_proven, FALSE)"
    )
    desired_df["connection_shard_id"] = (
        desired_df.apply(
            lambda row: subscription_affinity_shard(
                asset_id=str(row["asset_id"]),
                condition_id=str(row.get("condition_id") or ""),
                shard_count=int(connection_count),
            ),
            axis=1,
        )
        if stream_lag_gate_enabled
        else -1
    )

    con = duckdb.connect(
        ":memory:",
        config={"threads": str(max(1, int(os.environ.get("BOOK_L2_DUCKDB_THREADS", "8"))))},
    )
    try:
        con.register("desired_input", desired_df)
        con.register("lifecycle_hour_input", _normalize_lifecycle_hour_states(lifecycle_hour_df))
        con.register("gap_input", _normalize_gap_events(gap_df))
        normalized_shard_gaps = _normalize_shard_gap_events(shard_gap_df)
        con.register("shard_gap_input", normalized_shard_gaps)
        con.register(
            "shard_gap_repair_input",
            _normalize_shard_gap_repairs(shard_gap_df),
        )
        con.register("gold_repair_window_input", gold.repair_windows_frame())
        con.register("baseline_seed_input", _normalize_baseline_seeds(baseline_seed_df))
        _create_events_view(
            con,
            files,
            gold=gold,
            since=since,
            until=until,
            evidence_until=(
                until + timedelta(hours=max(0.0, float(recovery_lookahead_hours)))
                if until is not None
                else None
            ),
            shard_count=shard_count,
        )
        _create_hours_view(con, since=since, until=until)
        con.execute(
            """
            CREATE TEMP VIEW desired_hours AS
            SELECT d.asset_id,
                   d.condition_id,
                   d.market_slug,
                   COALESCE(l.market_state, d.market_state) AS market_state,
                   COALESCE(l.execution_eligible, d.execution_eligible) AS execution_eligible,
                   d.desired_from,
                   d.connection_shard_id,
                   h.hour_start
            FROM desired_input d
            CROSS JOIN hours h
            LEFT JOIN lifecycle_hour_input l
              ON l.asset_id = d.asset_id
             AND l.hour_start = h.hour_start
            WHERE d.desired_from IS NULL
               OR h.hour_start + INTERVAL 1 HOUR > d.desired_from
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW connection_event_hours AS
            WITH lagged AS (
                SELECT shard_id,
                       date_trunc('hour', timestamp_received) AS hour_start,
                       greatest(
                           0.0,
                           epoch(timestamp_received) - epoch(timestamp)
                       ) AS upstream_lag_seconds
                FROM events
                WHERE shard_id IS NOT NULL
                  AND timestamp IS NOT NULL
                  AND NOT three_source_gold_overlay
                  AND event_type IN (
                      'price_change', 'best_bid_ask',
                      'last_trade_price', 'tick_size_change'
                  )
                  AND source NOT IN (
                      'polymarket_clob_rest_seed',
                      'polymarket_clob_rest_reconcile'
                  )
            )
            SELECT shard_id,
                   hour_start,
                   count(*)::BIGINT AS connection_upstream_lag_sample_count,
                   quantile_cont(upstream_lag_seconds, 0.50) AS connection_upstream_lag_p50_seconds,
                   quantile_cont(upstream_lag_seconds, 0.99) AS connection_upstream_lag_p99_seconds,
                   max(upstream_lag_seconds) AS connection_upstream_lag_max_seconds
            FROM lagged
            GROUP BY shard_id, hour_start
            """
        )
        con.execute(
            f"""
            CREATE TEMP VIEW connection_heartbeat_hours AS
            WITH heartbeat_events AS (
                SELECT shard_id,
                       date_trunc('hour', timestamp_received) AS hour_start,
                       timestamp_received AS heartbeat_at,
                       lag(timestamp_received) OVER (
                           PARTITION BY shard_id, date_trunc('hour', timestamp_received)
                           ORDER BY timestamp_received
                       ) AS previous_heartbeat_at
                FROM events
                WHERE shard_id IS NOT NULL
                  AND NOT three_source_gold_overlay
                  AND event_type = 'connection_heartbeat'
                  AND source NOT IN (
                      'polymarket_clob_rest_seed',
                      'polymarket_clob_rest_reconcile'
                  )
            ), heartbeat_hours AS (
                SELECT shard_id,
                       hour_start,
                       count(*)::BIGINT AS connection_heartbeat_count,
                       min(heartbeat_at) AS first_heartbeat_at,
                       max(heartbeat_at) AS last_heartbeat_at,
                       max(
                           epoch(heartbeat_at) - epoch(previous_heartbeat_at)
                       ) FILTER (WHERE previous_heartbeat_at IS NOT NULL)
                           AS heartbeat_max_gap_seconds
                FROM heartbeat_events
                GROUP BY shard_id, hour_start
            )
            SELECT *,
                   first_heartbeat_at
                       <= hour_start
                          + INTERVAL {heartbeat_tolerance_seconds} SECOND
                   AND last_heartbeat_at
                       >= hour_start + INTERVAL 1 HOUR
                          - INTERVAL {heartbeat_tolerance_seconds} SECOND
                   AND COALESCE(heartbeat_max_gap_seconds, 0.0)
                       <= {float(heartbeat_tolerance_seconds)}
                       AS connection_heartbeat_edge_proven
            FROM heartbeat_hours
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW event_hours AS
            SELECT asset_id,
                   date_trunc('hour', timestamp_received) AS hour_start,
                   count(*)::BIGINT AS archive_row_count,
                   list_sort(
                       list_distinct(
                           list(shard_id) FILTER (
                               WHERE shard_id IS NOT NULL
                                 AND NOT three_source_gold_overlay
                           )
                       )
                   ) AS archive_shard_ids,
                   sum(CASE WHEN event_type IN (
                       'book', 'price_change', 'best_bid_ask',
                       'last_trade_price', 'tick_size_change'
                   ) THEN 1 ELSE 0 END)::BIGINT AS event_count,
                   sum(CASE WHEN event_type IN (
                       'book', 'price_change', 'best_bid_ask',
                       'last_trade_price', 'tick_size_change'
                   ) AND source <> 'polymarket_clob_rest_seed' THEN 1 ELSE 0 END)::BIGINT AS ws_event_count,
                   sum(CASE WHEN event_type NOT IN (
                       'book', 'price_change', 'best_bid_ask',
                       'last_trade_price', 'tick_size_change'
                   ) THEN 1 ELSE 0 END)::BIGINT AS control_event_count,
                   sum(CASE WHEN event_type = 'book' THEN 1 ELSE 0 END)::BIGINT AS book_count,
                   sum(CASE WHEN event_type = 'book' AND source <> 'polymarket_clob_rest_seed' THEN 1 ELSE 0 END)::BIGINT AS ws_book_count,
                   sum(CASE WHEN event_type = 'book' AND source = 'polymarket_clob_rest_seed' THEN 1 ELSE 0 END)::BIGINT AS rest_seed_book_count,
                   sum(CASE WHEN event_type = 'price_change' THEN 1 ELSE 0 END)::BIGINT AS price_change_count,
                   sum(CASE WHEN event_type = 'best_bid_ask' THEN 1 ELSE 0 END)::BIGINT AS best_bid_ask_count,
                   sum(CASE WHEN event_type = 'last_trade_price' THEN 1 ELSE 0 END)::BIGINT AS last_trade_price_count,
                   min(CASE WHEN event_type IN (
                       'book', 'price_change', 'best_bid_ask',
                       'last_trade_price', 'tick_size_change'
                   ) THEN timestamp_received END) AS first_received_at,
                   max(CASE WHEN event_type IN (
                       'book', 'price_change', 'best_bid_ask',
                       'last_trade_price', 'tick_size_change'
                   ) THEN timestamp_received END) AS last_received_at,
                   sum(CASE WHEN three_source_gold_overlay THEN 1 ELSE 0 END)::BIGINT
                       AS three_source_gold_overlay_event_count,
                   list_sort(
                       list_distinct(
                           list(three_source_gold_manifest_sha256) FILTER (
                               WHERE three_source_gold_overlay
                                 AND three_source_gold_manifest_sha256 IS NOT NULL
                           )
                       )
                   ) AS three_source_gold_overlay_manifest_sha256
            FROM events
            WHERE asset_id IS NOT NULL AND asset_id <> ''
            GROUP BY asset_id, hour_start
            """
        )
        con.execute(
            f"""
            CREATE TEMP VIEW token_connection_event_hours AS
            WITH observed AS (
                SELECT e.asset_id,
                       e.hour_start,
                       observed_shard.shard_id
                FROM event_hours e,
                     UNNEST(e.archive_shard_ids) AS observed_shard(shard_id)
            ), condition_observed AS (
                SELECT DISTINCT d.condition_id,
                       o.hour_start,
                       o.shard_id
                FROM observed o
                JOIN desired_hours d
                  ON d.asset_id = o.asset_id
                 AND d.hour_start = o.hour_start
                WHERE d.condition_id IS NOT NULL
                  AND d.condition_id <> ''
            ), token_shards AS (
                -- Prefer the shard(s) actually observed for a token that
                -- emitted an event.  A quiet outcome has no token-local row;
                -- first inherit the shard observed for its sibling outcome in
                -- the same market (which preserves load-aware assignment),
                -- then fall back to deterministic subscription affinity only
                -- when the whole market was quiet.
                SELECT asset_id, hour_start, shard_id
                FROM observed
                UNION ALL
                SELECT d.asset_id,
                       d.hour_start,
                       c.shard_id
                FROM desired_hours d
                JOIN condition_observed c
                  ON c.condition_id = d.condition_id
                 AND c.hour_start = d.hour_start
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM observed o
                    WHERE o.asset_id = d.asset_id
                      AND o.hour_start = d.hour_start
                )
                UNION ALL
                SELECT d.asset_id,
                       d.hour_start,
                       d.connection_shard_id AS shard_id
                FROM desired_hours d
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM observed o
                    WHERE o.asset_id = d.asset_id
                      AND o.hour_start = d.hour_start
                )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM condition_observed c
                    WHERE c.condition_id = d.condition_id
                      AND c.hour_start = d.hour_start
                )
            )
            SELECT t.asset_id,
                   t.hour_start,
                   sum(COALESCE(c.connection_upstream_lag_sample_count, 0))::BIGINT
                       AS connection_upstream_lag_sample_count,
                   sum(COALESCE(h.connection_heartbeat_count, 0))::BIGINT
                       AS connection_heartbeat_count,
                   max(c.connection_upstream_lag_p50_seconds)
                       AS connection_upstream_lag_p50_seconds,
                   max(c.connection_upstream_lag_p99_seconds)
                       AS connection_upstream_lag_p99_seconds,
                   max(c.connection_upstream_lag_max_seconds)
                       AS connection_upstream_lag_max_seconds,
                   bool_and(
                       COALESCE(h.connection_heartbeat_edge_proven, FALSE)
                   ) AS connection_heartbeat_edge_proven,
                   bool_and(
                       COALESCE(h.connection_heartbeat_edge_proven, FALSE)
                       OR COALESCE(c.connection_upstream_lag_sample_count, 0)
                              >= {max(1, int(min_connection_lag_samples))}
                   ) AS connection_stream_proven,
                   bool_and(
                       (
                           COALESCE(h.connection_heartbeat_edge_proven, FALSE)
                           OR COALESCE(c.connection_upstream_lag_sample_count, 0)
                                  >= {max(1, int(min_connection_lag_samples))}
                       )
                       AND (
                           COALESCE(c.connection_upstream_lag_sample_count, 0) = 0
                           OR c.connection_upstream_lag_p99_seconds
                              <= {float(max_upstream_lag_p99_seconds)}
                       )
                   ) AS connection_stream_fresh
            FROM token_shards t
            LEFT JOIN connection_event_hours c
              ON c.shard_id = t.shard_id
             AND c.hour_start = t.hour_start
            LEFT JOIN connection_heartbeat_hours h
              ON h.shard_id = t.shard_id
             AND h.hour_start = t.hour_start
            GROUP BY t.asset_id, t.hour_start
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW recorded_gap_hours AS
            WITH segmented AS (
                SELECT r.*,
                       d.hour_start,
                       CASE
                           WHEN r.gap_at IS NOT NULL
                            AND r.recovered_at IS NOT NULL
                            AND r.recovered_at > r.gap_at
                           THEN greatest(r.gap_at, d.hour_start)
                       END AS segment_gap_at,
                       CASE
                           WHEN r.gap_at IS NOT NULL
                            AND r.recovered_at IS NOT NULL
                            AND r.recovered_at > r.gap_at
                           THEN least(
                               r.recovered_at,
                               d.hour_start + INTERVAL 1 HOUR
                           )
                       END AS segment_recovered_at
                FROM gap_input r
                JOIN desired_hours d
                  ON d.asset_id = r.asset_id
                 AND (
                     (
                         r.gap_at IS NOT NULL
                         AND r.recovered_at IS NOT NULL
                         AND r.recovered_at > r.gap_at
                         AND d.hour_start < r.recovered_at
                         AND d.hour_start + INTERVAL 1 HOUR > r.gap_at
                     )
                     OR (
                         (
                             r.gap_at IS NULL
                             OR r.recovered_at IS NULL
                             OR r.recovered_at <= r.gap_at
                         )
                         AND d.hour_start = date_trunc('hour', r.created_at)
                     )
                 )
            ), unrepaired AS (
                SELECT r.*
                FROM segmented r
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM gold_repair_window_input g
                    WHERE g.asset_id = r.asset_id
                      AND g.shard_id = r.shard_id
                      AND g.hour_start = r.hour_start
                      AND r.segment_gap_at IS NOT NULL
                      AND r.segment_recovered_at IS NOT NULL
                      AND g.gap_at <= r.segment_gap_at
                      AND g.recovered_at >= r.segment_recovered_at
                )
            )
            SELECT asset_id,
                   hour_start,
                   sum(CASE WHEN status = 'rest_seeded' THEN 1 ELSE 0 END)::BIGINT AS ws_gap_rest_seeded_count,
                   sum(CASE WHEN status = 'rest_seeded' THEN 1 ELSE 0 END)::BIGINT AS ws_gap_recovered_count,
                   sum(CASE WHEN status IN (
                       'rest_error', 'rest_timeout', 'rest_http_error', 'rest_invalid_schema',
                       'rest_empty_payload', 'book_not_found', 'ws_gap_unseeded', 'unseeded',
                       'rest_bbo_mismatch', 'rest_bbo_missing_bbo'
                   ) THEN 1 ELSE 0 END)::BIGINT AS ws_gap_unseeded_count,
                   sum(CASE WHEN status IN (
                       'rest_error', 'rest_timeout', 'rest_http_error', 'rest_invalid_schema',
                       'rest_empty_payload', 'book_not_found'
                   ) THEN 1 ELSE 0 END)::BIGINT AS rest_error_count
            FROM unrepaired
            GROUP BY asset_id, hour_start
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW archive_gap_hours AS
            WITH gaps AS (
                SELECT asset_id,
                       shard_id,
                       COALESCE(
                           NULLIF(transaction_hash, ''),
                           'missing:' || asset_id || ':'
                               || COALESCE(CAST(shard_id AS VARCHAR), 'unknown') || ':'
                               || CAST(min(timestamp_received) AS VARCHAR)
                       ) AS gap_connection_id,
                       min(timestamp_received) AS gap_at
                FROM events
                WHERE event_type = 'connection_gap'
                  AND asset_id IS NOT NULL
                  AND asset_id <> ''
                  AND NOT three_source_gold_overlay
                GROUP BY asset_id, shard_id, transaction_hash
            ), marker_recoveries AS (
                SELECT asset_id,
                       shard_id,
                       transaction_hash AS gap_connection_id,
                       min(timestamp_received) AS recovered_at,
                       arg_min(book_hash, timestamp_received) AS recovery_detail
                FROM all_events
                WHERE event_type = 'connection_recovered'
                  AND asset_id IS NOT NULL
                  AND asset_id <> ''
                  AND transaction_hash IS NOT NULL
                  AND transaction_hash <> ''
                  AND COALESCE(book_hash, '') LIKE 'snapshot_confirmed:%'
                  AND NOT three_source_gold_overlay
                GROUP BY asset_id, shard_id, transaction_hash
            ), book_recoveries AS (
                SELECT asset_id,
                       shard_id,
                       timestamp_received AS recovered_at,
                       CASE
                           WHEN source IN (
                               'polymarket_clob_rest_seed',
                               'polymarket_clob_rest_reconcile'
                           )
                           THEN 'snapshot_confirmed:rest_seed_archive_book'
                           ELSE 'snapshot_confirmed:archive_book'
                       END AS recovery_detail
                FROM all_events
                WHERE event_type = 'book'
                  AND asset_id IS NOT NULL
                  AND asset_id <> ''
                  AND NOT three_source_gold_overlay
            ), classified AS (
                SELECT g.asset_id,
                       g.shard_id,
                       g.gap_at,
                       r.recovered_at,
                       r.recovery_detail
                FROM gaps g
                LEFT JOIN LATERAL (
                    SELECT candidate.recovered_at, candidate.recovery_detail
                    FROM (
                        SELECT recovered_at,
                               recovery_detail,
                               CASE
                                   WHEN gap_connection_id = g.gap_connection_id THEN 0
                                   ELSE 1
                               END AS recovery_rank
                        FROM marker_recoveries
                        WHERE asset_id = g.asset_id
                          AND recovered_at >= g.gap_at
                        UNION ALL
                        SELECT recovered_at,
                               recovery_detail,
                               1 AS recovery_rank
                        FROM book_recoveries
                        WHERE asset_id = g.asset_id
                          AND recovered_at > g.gap_at
                    ) candidate
                    ORDER BY
                        candidate.recovery_rank,
                        candidate.recovered_at
                    LIMIT 1
                ) r ON TRUE
            ), segmented AS (
                SELECT c.*,
                       d.hour_start,
                       greatest(c.gap_at, d.hour_start) AS segment_gap_at,
                       CASE
                           WHEN c.recovered_at IS NOT NULL
                           THEN least(
                               c.recovered_at,
                               d.hour_start + INTERVAL 1 HOUR
                           )
                       END AS segment_recovered_at
                FROM classified c
                JOIN desired_hours d
                  ON d.asset_id = c.asset_id
                 AND d.hour_start + INTERVAL 1 HOUR > c.gap_at
                 AND (
                     c.recovered_at IS NULL
                     OR d.hour_start < c.recovered_at
                 )
            ), unrepaired AS (
                SELECT c.*
                FROM segmented c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM gold_repair_window_input gr
                    WHERE gr.asset_id = c.asset_id
                      AND gr.shard_id = c.shard_id
                      AND gr.hour_start = c.hour_start
                      AND c.segment_recovered_at IS NOT NULL
                      AND gr.gap_at <= c.segment_gap_at
                      AND gr.recovered_at >= c.segment_recovered_at
                )
            )
            SELECT g.asset_id,
                   g.hour_start,
                   count(*) FILTER (
                       WHERE g.recovered_at IS NOT NULL
                         AND COALESCE(g.recovery_detail, '') LIKE 'snapshot_confirmed:rest_seed%'
                   )::BIGINT AS ws_gap_rest_seeded_count,
                   count(*) FILTER (WHERE g.recovered_at IS NOT NULL)::BIGINT AS ws_gap_recovered_count,
                   count(*) FILTER (WHERE g.recovered_at IS NULL)::BIGINT AS ws_gap_unseeded_count,
                   0::BIGINT AS rest_error_count
            FROM unrepaired g
            GROUP BY g.asset_id, hour_start
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW three_source_gold_repair_hours AS
            SELECT d.asset_id,
                   d.hour_start,
                   count(DISTINCT g.request_id)::BIGINT
                       AS three_source_gold_repair_window_count,
                   list_sort(list_distinct(list(g.manifest_sha256)))
                       AS three_source_gold_manifest_sha256
            FROM desired_hours d
            JOIN gold_repair_window_input g
              ON g.asset_id = d.asset_id
             AND g.hour_start = d.hour_start
            GROUP BY d.asset_id, d.hour_start
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW source_shard_gap_hours AS
            SELECT d.asset_id,
                   d.hour_start,
                   0::BIGINT AS ws_gap_rest_seeded_count,
                   0::BIGINT AS ws_gap_recovered_count,
                   count(*)::BIGINT AS ws_gap_unseeded_count,
                   0::BIGINT AS rest_error_count
            FROM desired_hours d
            LEFT JOIN event_hours e USING(asset_id, hour_start)
            JOIN shard_gap_input g
              ON d.hour_start < g.recovered_at
             AND d.hour_start + INTERVAL 1 HOUR > g.gap_at
            LEFT JOIN shard_gap_repair_input r
              ON r.gap_id = g.gap_id
             AND r.asset_id = d.asset_id
            LEFT JOIN gold_repair_window_input gr
              ON gr.asset_id = d.asset_id
             AND gr.hour_start = d.hour_start
             AND gr.shard_id = g.shard_id
             AND gr.gap_at <= greatest(g.gap_at, d.hour_start)
             AND gr.recovered_at >= least(
                 g.recovered_at,
                 d.hour_start + INTERVAL 1 HOUR
             )
            WHERE r.asset_id IS NULL
              AND gr.asset_id IS NULL
              AND (
                  (
                      array_length(COALESCE(e.archive_shard_ids, []::INTEGER[])) > 0
                      AND list_contains(e.archive_shard_ids, g.shard_id)
                  )
                  OR d.connection_shard_id = g.shard_id
              )
            GROUP BY d.asset_id, d.hour_start
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW gap_hours AS
            SELECT asset_id, hour_start,
                   sum(ws_gap_rest_seeded_count)::BIGINT AS ws_gap_rest_seeded_count,
                   sum(ws_gap_recovered_count)::BIGINT AS ws_gap_recovered_count,
                   sum(ws_gap_unseeded_count)::BIGINT AS ws_gap_unseeded_count,
                   sum(rest_error_count)::BIGINT AS rest_error_count
            FROM (
                SELECT * FROM recorded_gap_hours
                UNION ALL
                SELECT * FROM archive_gap_hours
                UNION ALL
                SELECT * FROM source_shard_gap_hours
            ) combined
            GROUP BY asset_id, hour_start
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW baseline_events AS
            SELECT asset_id, timestamp_received, event_type, source,
                   COALESCE(json_array_length(TRY_CAST(bids AS JSON)), 0) > 0
                   AND COALESCE(json_array_length(TRY_CAST(asks AS JSON)), 0) > 0 AS two_sided
            FROM all_events
            WHERE event_type = 'book'
            UNION ALL
            SELECT asset_id, timestamp_received, 'book' AS event_type, source, two_sided
            FROM baseline_seed_input
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW baseline_books AS
            SELECT d.asset_id,
                   d.hour_start,
                   count(e.asset_id)::BIGINT AS baseline_book_count,
                   sum(CASE WHEN e.source NOT IN ('polymarket_clob_rest_seed', 'polymarket_clob_rest_reconcile') THEN 1 ELSE 0 END)::BIGINT AS ws_baseline_book_count,
                   sum(CASE WHEN e.source IN ('polymarket_clob_rest_seed', 'polymarket_clob_rest_reconcile') THEN 1 ELSE 0 END)::BIGINT AS rest_seed_baseline_book_count,
                   max(e.timestamp_received) AS baseline_received_at,
                   COALESCE(arg_max(e.two_sided, e.timestamp_received), FALSE) AS baseline_two_sided
            FROM desired_hours d
            LEFT JOIN baseline_events e
              ON e.asset_id = d.asset_id
             AND e.event_type = 'book'
             AND e.timestamp_received < d.hour_start + INTERVAL 1 HOUR
            GROUP BY d.asset_id, d.hour_start
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW price_after_baseline AS
            SELECT b.asset_id,
                   b.hour_start,
                   count(e.asset_id)::BIGINT AS price_change_after_baseline_count
            FROM baseline_books b
            LEFT JOIN events e
              ON e.asset_id = b.asset_id
             AND e.event_type = 'price_change'
             AND e.timestamp_received >= b.baseline_received_at
             AND e.timestamp_received >= b.hour_start
             AND e.timestamp_received < b.hour_start + INTERVAL 1 HOUR
            GROUP BY b.asset_id, b.hour_start
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW book_state_events AS
            SELECT asset_id, timestamp_received, collector_seq, sequence_in_message,
                   COALESCE(json_array_length(TRY_CAST(bids AS JSON)), 0) > 0
                   AND COALESCE(json_array_length(TRY_CAST(asks AS JSON)), 0) > 0 AS two_sided
            FROM all_events
            WHERE event_type = 'book'
            UNION ALL
            SELECT asset_id, timestamp_received, collector_seq, sequence_in_message,
                   best_bid IS NOT NULL AND best_bid > 0
                   AND best_ask IS NOT NULL AND best_ask < 1
                   AND best_bid < best_ask AS two_sided
            FROM all_events
            WHERE event_type IN ('price_change', 'best_bid_ask')
              AND (best_bid IS NOT NULL OR best_ask IS NOT NULL)
            """
        )
        con.execute(
            """
            CREATE TEMP VIEW final_book_states AS
            SELECT d.asset_id, d.hour_start,
                   arg_max(
                       e.two_sided,
                       struct_pack(
                           received := e.timestamp_received,
                           collector := COALESCE(e.collector_seq, 0),
                           sequence := COALESCE(e.sequence_in_message, 0)
                       )
                   ) AS final_two_sided
            FROM desired_hours d
            LEFT JOIN book_state_events e
              ON e.asset_id = d.asset_id
             AND e.timestamp_received < d.hour_start + INTERVAL 1 HOUR
            GROUP BY d.asset_id, d.hour_start
            """
        )
        con.execute(
            f"""
            CREATE TEMP VIEW manifest AS
            WITH joined AS (
                SELECT
                    d.asset_id,
                    d.condition_id,
                    d.market_slug,
                    d.market_state,
                    d.execution_eligible,
                    d.desired_from,
                    d.connection_shard_id,
                    d.hour_start,
                    COALESCE(e.archive_row_count, 0)::BIGINT AS archive_row_count,
                    COALESCE(e.archive_shard_ids, []::INTEGER[])
                        AS archive_shard_ids,
                    COALESCE(e.event_count, 0)::BIGINT AS event_count,
                    COALESCE(e.ws_event_count, 0)::BIGINT AS ws_event_count,
                    COALESCE(e.control_event_count, 0)::BIGINT AS control_event_count,
                    COALESCE(e.book_count, 0)::BIGINT AS book_count,
                    COALESCE(e.ws_book_count, 0)::BIGINT AS ws_book_count,
                    COALESCE(e.rest_seed_book_count, 0)::BIGINT AS rest_seed_book_count,
                    COALESCE(e.price_change_count, 0)::BIGINT AS price_change_count,
                    COALESCE(e.best_bid_ask_count, 0)::BIGINT AS best_bid_ask_count,
                    COALESCE(e.last_trade_price_count, 0)::BIGINT AS last_trade_price_count,
                    e.first_received_at,
                    e.last_received_at,
                    COALESCE(b.baseline_book_count, 0)::BIGINT AS baseline_book_count,
                    COALESCE(b.ws_baseline_book_count, 0)::BIGINT AS ws_baseline_book_count,
                    COALESCE(b.rest_seed_baseline_book_count, 0)::BIGINT AS rest_seed_baseline_book_count,
                    b.baseline_received_at,
                    COALESCE(b.baseline_two_sided, FALSE) AS baseline_two_sided,
                    COALESCE(f.final_two_sided, b.baseline_two_sided, FALSE) AS final_two_sided,
                    COALESCE(p.price_change_after_baseline_count, 0)::BIGINT AS price_change_after_baseline_count,
                    COALESCE(g.ws_gap_rest_seeded_count, 0)::BIGINT AS ws_gap_rest_seeded_count,
                    COALESCE(g.ws_gap_recovered_count, 0)::BIGINT AS ws_gap_recovered_count,
                    COALESCE(g.ws_gap_unseeded_count, 0)::BIGINT AS ws_gap_unseeded_count,
                    COALESCE(g.rest_error_count, 0)::BIGINT AS rest_error_count,
                    COALESCE(gr.three_source_gold_repair_window_count, 0)::BIGINT
                        AS three_source_gold_repair_window_count,
                    COALESCE(
                        gr.three_source_gold_manifest_sha256,
                        []::VARCHAR[]
                    ) AS three_source_gold_repair_manifest_sha256,
                    COALESCE(e.three_source_gold_overlay_event_count, 0)::BIGINT
                        AS three_source_gold_overlay_event_count,
                    COALESCE(
                        e.three_source_gold_overlay_manifest_sha256,
                        []::VARCHAR[]
                    ) AS three_source_gold_overlay_manifest_sha256,
                    {_string_literal(gold_validation_status)}
                        AS three_source_gold_validation_status,
                    COALESCE(c.connection_upstream_lag_sample_count, 0)::BIGINT
                        AS connection_upstream_lag_sample_count,
                    COALESCE(c.connection_heartbeat_count, 0)::BIGINT
                        AS connection_heartbeat_count,
                    c.connection_upstream_lag_p50_seconds,
                    c.connection_upstream_lag_p99_seconds,
                    c.connection_upstream_lag_max_seconds,
                    COALESCE(c.connection_heartbeat_edge_proven, FALSE)
                        AS connection_heartbeat_edge_proven,
                    {stream_proven_sql} AS connection_stream_proven,
                    {stream_fresh_sql} AS connection_stream_fresh
                FROM desired_hours d
                LEFT JOIN event_hours e USING(asset_id, hour_start)
                LEFT JOIN baseline_books b USING(asset_id, hour_start)
                LEFT JOIN price_after_baseline p USING(asset_id, hour_start)
                LEFT JOIN final_book_states f USING(asset_id, hour_start)
                LEFT JOIN gap_hours g USING(asset_id, hour_start)
                LEFT JOIN three_source_gold_repair_hours gr
                  USING(asset_id, hour_start)
                LEFT JOIN token_connection_event_hours c
                  USING(asset_id, hour_start)
            )
            SELECT *,
                   baseline_book_count > 0 AS has_book,
                   final_two_sided AS has_two_sided_book,
                   ws_baseline_book_count > 0 AS has_ws_book,
                   rest_seed_baseline_book_count > 0 AS has_rest_seed_book,
                   price_change_count > 0 AS has_price_change,
                   price_change_after_baseline_count > 0 AS has_price_change_after_baseline,
                   rest_seed_baseline_book_count > 0 AND ws_baseline_book_count = 0 AS rest_seed_only,
                   CASE
                       WHEN ws_gap_unseeded_count > 0
                         OR ws_gap_recovered_count > 0
                         OR rest_error_count > 0
                         OR NOT connection_stream_proven
                           THEN 'COVERAGE_GAP'
                       WHEN NOT connection_stream_fresh
                           THEN 'TRANSPORT_BACKLOG'
                       WHEN connection_heartbeat_edge_proven
                         AND connection_upstream_lag_sample_count
                             < {max(1, int(min_connection_lag_samples))}
                           THEN 'QUIET_BUT_COVERED'
                       ELSE 'FRESH'
                   END AS connection_continuity_state,
                   -- Source-to-receive latency is reported by
                   -- connection_stream_fresh, but it is not a continuity
                   -- break. Require heartbeat edge proof or the legacy event
                   -- sample fallback here; explicit gap/recovery evidence
                   -- below remains independently fail-closed.
                   CASE
                       WHEN baseline_book_count <= 0 THEN FALSE
                       WHEN NOT final_two_sided THEN FALSE
                       WHEN NOT connection_stream_proven THEN FALSE
                       WHEN rest_seed_baseline_book_count > 0
                         AND ws_baseline_book_count = 0
                         AND price_change_after_baseline_count <= 0 THEN FALSE
                       WHEN ws_gap_unseeded_count > 0 THEN FALSE
                       WHEN ws_gap_recovered_count > 0 THEN FALSE
                       WHEN rest_error_count > 0 THEN FALSE
                       ELSE TRUE
                   END AS fill_depth_ready,
                   CASE
                       WHEN baseline_book_count <= 0 THEN 'no_book_baseline'
                       WHEN NOT final_two_sided THEN 'one_sided_book'
                       WHEN NOT connection_stream_proven THEN 'connection_stream_unproven'
                       WHEN rest_seed_baseline_book_count > 0
                         AND ws_baseline_book_count = 0
                         AND price_change_after_baseline_count <= 0 THEN 'rest_seed_without_ws_delta'
                       WHEN ws_gap_unseeded_count > 0 THEN 'ws_gap_unseeded'
                       WHEN ws_gap_recovered_count > 0 THEN 'ws_gap_recovered_interval'
                       WHEN rest_error_count > 0 THEN 'rest_seed_error'
                       ELSE 'ready'
                   END AS fill_depth_reason,
                   { _timestamp_literal(generated_at) } AS manifest_generated_at,
                   { _string_literal(str(archive_path)) } AS archive_dir,
                   { _int_or_null(shard_count) } AS shard_count
            FROM joined
            """
        )
        con.execute(
            "COPY (SELECT * FROM manifest ORDER BY hour_start, execution_eligible DESC, market_state, asset_id) "
            f"TO {_string_literal(str(out_path))} (FORMAT PARQUET, COMPRESSION ZSTD)",
        )
        summary_row = con.execute(
            """
            SELECT
                count(DISTINCT asset_id)::BIGINT AS desired_tokens,
                count(*)::BIGINT AS token_hours,
                sum(CASE WHEN fill_depth_ready THEN 1 ELSE 0 END)::BIGINT AS fill_depth_ready_token_hours,
                sum(CASE WHEN fill_depth_reason = 'no_book_baseline' THEN 1 ELSE 0 END)::BIGINT AS no_book_token_hours,
                sum(CASE WHEN fill_depth_reason = 'one_sided_book' THEN 1 ELSE 0 END)::BIGINT AS one_sided_book_token_hours,
                sum(CASE WHEN fill_depth_reason IN ('no_ws_price_change', 'no_ws_price_change_after_baseline', 'rest_seed_without_ws_delta') THEN 1 ELSE 0 END)::BIGINT AS no_price_change_token_hours,
                sum(CASE WHEN rest_seed_only THEN 1 ELSE 0 END)::BIGINT AS rest_seed_only_token_hours,
                sum(CASE WHEN ws_gap_rest_seeded_count > 0 THEN 1 ELSE 0 END)::BIGINT AS ws_gap_rest_seeded_token_hours,
                sum(CASE WHEN ws_gap_recovered_count > 0 THEN 1 ELSE 0 END)::BIGINT AS ws_gap_recovered_token_hours,
                sum(CASE WHEN ws_gap_unseeded_count > 0 THEN 1 ELSE 0 END)::BIGINT AS ws_gap_unseeded_token_hours,
                sum(CASE WHEN rest_error_count > 0 THEN 1 ELSE 0 END)::BIGINT AS rest_error_token_hours,
                sum(CASE WHEN NOT connection_stream_fresh THEN 1 ELSE 0 END)::BIGINT AS stream_stale_token_hours,
                sum(three_source_gold_overlay_event_count)::BIGINT
                    AS three_source_gold_overlay_event_count,
                sum(three_source_gold_repair_window_count)::BIGINT
                    AS three_source_gold_repair_window_count
            FROM manifest
            """
        ).fetchone()
        if write_db:
            manifest_df = con.execute("SELECT * FROM manifest").fetchdf()
            _write_manifest_db(manifest_df, coverage_table=coverage_table)
    finally:
        con.close()

    return L2CoverageManifestSummary(
        archive_dir=str(archive_path),
        output_path=str(out_path),
        since=since.isoformat(),
        until=until.isoformat() if until else None,
        desired_tokens=int(summary_row[0] or 0),
        token_hours=int(summary_row[1] or 0),
        fill_depth_ready_token_hours=int(summary_row[2] or 0),
        no_book_token_hours=int(summary_row[3] or 0),
        one_sided_book_token_hours=int(summary_row[4] or 0),
        no_price_change_token_hours=int(summary_row[5] or 0),
        rest_seed_only_token_hours=int(summary_row[6] or 0),
        ws_gap_rest_seeded_token_hours=int(summary_row[7] or 0),
        ws_gap_recovered_token_hours=int(summary_row[8] or 0),
        ws_gap_unseeded_token_hours=int(summary_row[9] or 0),
        rest_error_token_hours=int(summary_row[10] or 0),
        stream_stale_token_hours=int(summary_row[11] or 0),
        generated_at=generated_at.isoformat(),
        three_source_gold_manifest_count=len(gold.overlays),
        three_source_gold_overlay_event_count=int(summary_row[12] or 0),
        three_source_gold_repair_window_count=int(summary_row[13] or 0),
        three_source_gold_manifest_sha256=gold.manifest_sha256,
        three_source_gold_validation_status=gold_validation_status,
    )


def _load_desired_tokens() -> pd.DataFrame:
    with postgres_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            # The subscription target is the acquisition authority and already
            # contains the reconciled dynamic universe.  The former query
            # joined all 2M+ target rows to all 2.6M+ registry rows and then
            # evaluated several lifecycle OR branches; PostgreSQL spilled more
            # than 1 GiB of temporary hash data before returning the ~300k
            # desired tokens.  Drive from the exact desired set and use the
            # registry primary key for optional metadata.  Historical closing
            # tokens for the materialized hour are supplied separately by the
            # immutable assignment ledger in ``historical_desired``.
            cur.execute("SET LOCAL enable_hashjoin = off")
            cur.execute("SET LOCAL enable_mergejoin = off")
            cur.execute(
                """
                SELECT
                    t.asset_id,
                    COALESCE(r.condition_id, t.condition_id, '') AS condition_id,
                    r.market_slug,
                    COALESCE(r.market_state, 'TRADABLE_PENDING_BOOK') AS market_state,
                    COALESCE(r.execution_eligible, FALSE) AS execution_eligible,
                    COALESCE(
                        r.book_seen_first_at,
                        t.last_actual_subscribe_at,
                        t.last_subscribe_request_at,
                        t.last_requested_at,
                        r.last_transition_at,
                        r.updated_at,
                        t.updated_at
                    ) AS desired_from
                FROM quant.paper_lob_subscription_targets t
                LEFT JOIN quant.paper_market_registry_tokens r ON r.asset_id = t.asset_id
                WHERE t.desired_subscribed = TRUE
                  AND t.asset_id IS NOT NULL
                  AND t.asset_id <> ''
                """
            )
            return pd.DataFrame([dict(row) for row in cur.fetchall()])


def _load_pmxt_supplement_desired(
    cache_dir: Path,
    *,
    since: datetime,
    until: datetime | None,
    required: bool,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    for hour in _coverage_hours(since=since, until=until):
        cache_path = cache_dir / f"{hour:%Y%m%dT%H}.parquet"
        if not cache_path.exists():
            missing.append(hour.isoformat())
            continue
        con = duckdb.connect(":memory:")
        try:
            values = con.execute(
                """
                SELECT token_id::VARCHAR AS asset_id,
                       arg_max(condition_id::VARCHAR, event_count) AS condition_id
                FROM read_parquet(?)
                WHERE token_id IS NOT NULL
                  AND token_id <> ''
                  AND event_count > 0
                GROUP BY token_id
                """,
                [str(cache_path)],
            ).fetchall()
        finally:
            con.close()
        rows.extend(
            {
                "asset_id": str(asset_id),
                "condition_id": str(condition_id or ""),
                "market_slug": None,
                "market_state": "PMXT_ACTIVE",
                "execution_eligible": False,
                "desired_from": hour,
            }
            for asset_id, condition_id in values
        )
    if missing and required:
        raise FileNotFoundError(
            f"required PMXT supplement cache hours are missing under {cache_dir}: {missing}"
        )
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["asset_id", "condition_id", "market_slug", "market_state", "execution_eligible", "desired_from"]
    )


def _load_existing_desired_tokens(
    *,
    since: datetime,
    until: datetime | None,
    coverage_table: str = DEFAULT_COVERAGE_TABLE,
) -> pd.DataFrame:
    end = until or since + timedelta(hours=1)
    table = coverage_table_sql(coverage_table)
    try:
        with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT ON (asset_id)
                       asset_id, condition_id, market_slug, market_state,
                       execution_eligible, desired_from
                FROM {table}
                WHERE hour_start >= date_trunc('hour', %s::timestamptz)
                  AND hour_start < %s
                ORDER BY asset_id, updated_at DESC
                """,
                (since, end),
            )
            rows = [dict(row) for row in cur.fetchall()]
    except Exception:
        rows = []
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["asset_id", "condition_id", "market_slug", "market_state", "execution_eligible", "desired_from"]
    )


def _load_lifecycle_hour_states(
    *,
    asset_ids: Sequence[str],
    since: datetime,
    until: datetime | None,
) -> pd.DataFrame:
    """Return each candidate token's last registry state before every hour boundary."""

    assets = sorted({str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()})
    if not assets:
        return _empty_lifecycle_hour_states()

    rows: list[dict[str, Any]] = []
    with postgres_connection(readonly=True) as conn, conn.cursor() as cur:
        for hour_start in _coverage_hours(since=since, until=until):
            hour_end = hour_start + timedelta(hours=1)
            cur.execute(
                """
                SELECT candidates.asset_id,
                       %s::timestamptz AS hour_start,
                       latest.new_state AS market_state,
                       latest.new_execution_eligible AS execution_eligible
                FROM unnest(%s::text[]) AS candidates(asset_id)
                CROSS JOIN LATERAL (
                    SELECT e.new_state, e.new_execution_eligible
                    FROM quant.paper_market_lifecycle_events e
                    WHERE e.asset_id = candidates.asset_id
                      AND e.created_at < %s
                    ORDER BY e.created_at DESC, e.event_id DESC
                    LIMIT 1
                ) latest
                """,
                (hour_start, assets, hour_end),
            )
            rows.extend(dict(row) for row in cur.fetchall())
    return pd.DataFrame(rows) if rows else _empty_lifecycle_hour_states()


def _load_gap_events(*, since: datetime, until: datetime | None) -> pd.DataFrame:
    with postgres_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('quant.clob_l2_archive_gap_events') AS table_name")
            if cur.fetchone().get("table_name") is None:
                return _empty_gap_events()
            params: list[Any] = [since]
            until_sql = ""
            if until is not None:
                until_sql = "AND created_at < %s"
                params.append(until)
            cur.execute(
                f"""
                SELECT asset_id, status, created_at, shard_id,
                       created_at AS gap_at,
                       NULL::timestamptz AS recovered_at
                FROM quant.clob_l2_archive_gap_events
                WHERE created_at >= %s
                  {until_sql}
                  AND asset_id IS NOT NULL
                  AND asset_id <> ''
                """,
                params,
            )
            rows = [dict(row) for row in cur.fetchall()]
    return pd.DataFrame(rows) if rows else _empty_gap_events()


def _load_baseline_seeds(
    *,
    since: datetime,
    coverage_table: str = DEFAULT_COVERAGE_TABLE,
    asset_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    table = coverage_table_sql(coverage_table)
    table_name = coverage_table.split(".", 1)[1]
    assets = sorted({str(asset_id).strip() for asset_id in (asset_ids or ()) if str(asset_id).strip()})
    if not assets:
        return _empty_baseline_seeds()
    with postgres_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (coverage_table,))
            exists = bool(cur.fetchone()["exists"])
            if not exists:
                return _empty_baseline_seeds()
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'quant'
                      AND table_name = %s
                      AND column_name = 'has_two_sided_book'
                ) AS exists
                """,
                (table_name,),
            )
            has_two_sided_column = bool(cur.fetchone()["exists"])
            two_sided_sql = "c.has_two_sided_book" if has_two_sided_column else "FALSE"
            cur.execute(
                f"""
                SELECT candidates.asset_id,
                       c.baseline_received_at AS timestamp_received,
                       CASE WHEN c.ws_baseline_book_count > 0
                            THEN 'polymarket_market_ws_archive'
                            ELSE 'polymarket_clob_rest_seed' END AS source,
                       {two_sided_sql} AS two_sided
                FROM unnest(%s::text[]) AS candidates(asset_id)
                CROSS JOIN LATERAL (
                    SELECT c.*
                    FROM {table} c
                    WHERE c.asset_id = candidates.asset_id
                      AND c.hour_start < date_trunc('hour', %s::timestamptz)
                      AND c.baseline_received_at IS NOT NULL
                    ORDER BY c.hour_start DESC
                    LIMIT 1
                ) c
                WHERE EXISTS (
                      SELECT 1
                      FROM quant.clob_l2_archive_manifest m
                      WHERE m.archive_hour = date_trunc('hour', c.baseline_received_at)
                        AND m.status = 'ready'
                        AND (c.shard_count IS NULL OR m.shard_count = c.shard_count)
                        AND m.first_received_at <= c.baseline_received_at
                        AND m.last_received_at >= c.baseline_received_at
                  )
                """,
                (assets, since),
            )
            rows = [dict(row) for row in cur.fetchall()]
    return pd.DataFrame(rows) if rows else _empty_baseline_seeds()


def migrate_coverage_table_schema(
    coverage_table: str = DEFAULT_COVERAGE_TABLE,
) -> None:
    """Apply coverage DDL once, outside the minute-by-minute writer path."""

    table = coverage_table_sql(coverage_table)
    table_name = coverage_table.split(".", 1)[1]
    with postgres_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '5s'")
            cur.execute("SET LOCAL statement_timeout = '5min'")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    asset_id TEXT NOT NULL,
                    hour_start TIMESTAMPTZ NOT NULL,
                    desired_from TIMESTAMPTZ,
                    condition_id TEXT,
                    market_slug TEXT,
                    market_state TEXT,
                    execution_eligible BOOLEAN NOT NULL DEFAULT FALSE,
                    archive_row_count BIGINT NOT NULL DEFAULT 0,
                    archive_shard_ids INTEGER[] NOT NULL DEFAULT '{{}}',
                    event_count BIGINT NOT NULL DEFAULT 0,
                    ws_event_count BIGINT NOT NULL DEFAULT 0,
                    control_event_count BIGINT NOT NULL DEFAULT 0,
                    book_count BIGINT NOT NULL DEFAULT 0,
                    ws_book_count BIGINT NOT NULL DEFAULT 0,
                    rest_seed_book_count BIGINT NOT NULL DEFAULT 0,
                    price_change_count BIGINT NOT NULL DEFAULT 0,
                    best_bid_ask_count BIGINT NOT NULL DEFAULT 0,
                    last_trade_price_count BIGINT NOT NULL DEFAULT 0,
                    first_received_at TIMESTAMPTZ,
                    last_received_at TIMESTAMPTZ,
                    baseline_book_count BIGINT NOT NULL DEFAULT 0,
                    ws_baseline_book_count BIGINT NOT NULL DEFAULT 0,
                    rest_seed_baseline_book_count BIGINT NOT NULL DEFAULT 0,
                    baseline_received_at TIMESTAMPTZ,
                    has_two_sided_book BOOLEAN NOT NULL DEFAULT FALSE,
                    price_change_after_baseline_count BIGINT NOT NULL DEFAULT 0,
                    ws_gap_rest_seeded_count BIGINT NOT NULL DEFAULT 0,
                    ws_gap_recovered_count BIGINT NOT NULL DEFAULT 0,
                    ws_gap_unseeded_count BIGINT NOT NULL DEFAULT 0,
                    rest_error_count BIGINT NOT NULL DEFAULT 0,
                    has_book BOOLEAN NOT NULL DEFAULT FALSE,
                    has_ws_book BOOLEAN NOT NULL DEFAULT FALSE,
                    has_rest_seed_book BOOLEAN NOT NULL DEFAULT FALSE,
                    has_price_change BOOLEAN NOT NULL DEFAULT FALSE,
                    has_price_change_after_baseline BOOLEAN NOT NULL DEFAULT FALSE,
                    rest_seed_only BOOLEAN NOT NULL DEFAULT FALSE,
                    connection_shard_id INTEGER,
                    connection_upstream_lag_sample_count BIGINT NOT NULL DEFAULT 0,
                    connection_upstream_lag_p50_seconds DOUBLE PRECISION,
                    connection_upstream_lag_p99_seconds DOUBLE PRECISION,
                    connection_upstream_lag_max_seconds DOUBLE PRECISION,
                    connection_heartbeat_count BIGINT NOT NULL DEFAULT 0,
                    connection_heartbeat_edge_proven BOOLEAN NOT NULL DEFAULT FALSE,
                    connection_stream_proven BOOLEAN NOT NULL DEFAULT TRUE,
                    connection_stream_fresh BOOLEAN NOT NULL DEFAULT TRUE,
                    connection_continuity_state TEXT NOT NULL DEFAULT 'FRESH',
                    three_source_gold_repair_window_count BIGINT NOT NULL DEFAULT 0,
                    three_source_gold_repair_manifest_sha256
                        TEXT[] NOT NULL DEFAULT '{{}}',
                    three_source_gold_overlay_event_count BIGINT NOT NULL DEFAULT 0,
                    three_source_gold_overlay_manifest_sha256
                        TEXT[] NOT NULL DEFAULT '{{}}',
                    three_source_gold_validation_status
                        TEXT NOT NULL DEFAULT 'DISABLED',
                    fill_depth_ready BOOLEAN NOT NULL DEFAULT FALSE,
                    fill_depth_reason TEXT NOT NULL,
                    manifest_generated_at TIMESTAMPTZ NOT NULL,
                    archive_dir TEXT NOT NULL,
                    shard_count INTEGER,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (asset_id, hour_start)
                )
                """
            )
            for statement in (
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS baseline_book_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS ws_baseline_book_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS rest_seed_baseline_book_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS baseline_received_at TIMESTAMPTZ",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS has_two_sided_book BOOLEAN NOT NULL DEFAULT FALSE",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS desired_from TIMESTAMPTZ",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS price_change_after_baseline_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS has_price_change_after_baseline BOOLEAN NOT NULL DEFAULT FALSE",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS archive_row_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS archive_shard_ids INTEGER[] NOT NULL DEFAULT '{{}}'",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS control_event_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS ws_gap_recovered_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_shard_id INTEGER",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_upstream_lag_sample_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_upstream_lag_p50_seconds DOUBLE PRECISION",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_upstream_lag_p99_seconds DOUBLE PRECISION",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_upstream_lag_max_seconds DOUBLE PRECISION",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_heartbeat_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_heartbeat_edge_proven BOOLEAN NOT NULL DEFAULT FALSE",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_stream_proven BOOLEAN NOT NULL DEFAULT TRUE",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_stream_fresh BOOLEAN NOT NULL DEFAULT TRUE",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS connection_continuity_state TEXT NOT NULL DEFAULT 'FRESH'",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "three_source_gold_repair_window_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "three_source_gold_repair_manifest_sha256 "
                "TEXT[] NOT NULL DEFAULT '{}'",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "three_source_gold_overlay_event_count BIGINT NOT NULL DEFAULT 0",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "three_source_gold_overlay_manifest_sha256 "
                "TEXT[] NOT NULL DEFAULT '{}'",
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                "three_source_gold_validation_status "
                "TEXT NOT NULL DEFAULT 'DISABLED'",
            ):
                cur.execute(statement)
        conn.commit()
        if coverage_table != GCP_BATCH_COVERAGE_TABLE:
            return
        conn.autocommit = True
        with conn.cursor() as cur:
            index_names = (
                f"{table_name}_shard_hour_idx",
                f"{table_name}_hour_idx",
            )
            cur.execute(
                """
                SELECT indexrelid::regclass::text AS index_name
                FROM pg_index
                WHERE indrelid = %s::regclass
                  AND NOT indisvalid
                """,
                (coverage_table,),
            )
            invalid_indexes = {
                str(row["index_name"]).split(".")[-1]
                for row in cur.fetchall()
            }
            for index_name in index_names:
                if index_name in invalid_indexes:
                    cur.execute(
                        f"DROP INDEX CONCURRENTLY IF EXISTS quant.{index_name}"
                    )
            cur.execute("SET lock_timeout = '5min'")
            cur.execute("SET statement_timeout = '30min'")
            cur.execute(
                f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS
                    {table_name}_shard_hour_idx
                ON {table} (shard_count, hour_start DESC)
                """
            )
            cur.execute(
                f"""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS
                    {table_name}_hour_idx
                ON {table} (hour_start DESC)
                """
            )


def _write_manifest_db(
    rows: pd.DataFrame,
    *,
    coverage_table: str = DEFAULT_COVERAGE_TABLE,
) -> None:
    if rows.empty:
        return
    table = coverage_table_sql(coverage_table)
    watermark_id = _coverage_watermark_id(coverage_table)
    with postgres_connection(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {table} (
                    asset_id, hour_start, desired_from, condition_id, market_slug, market_state, execution_eligible,
                    archive_row_count, event_count, ws_event_count, control_event_count,
                    book_count, ws_book_count, rest_seed_book_count,
                    price_change_count, best_bid_ask_count, last_trade_price_count,
                    first_received_at, last_received_at,
                    baseline_book_count, ws_baseline_book_count, rest_seed_baseline_book_count, baseline_received_at, has_two_sided_book,
                    price_change_after_baseline_count,
                    ws_gap_rest_seeded_count, ws_gap_recovered_count, ws_gap_unseeded_count, rest_error_count,
                    has_book, has_ws_book, has_rest_seed_book, has_price_change, has_price_change_after_baseline, rest_seed_only,
                    connection_shard_id, connection_upstream_lag_sample_count,
                    connection_heartbeat_count,
                    connection_upstream_lag_p50_seconds, connection_upstream_lag_p99_seconds,
                    connection_upstream_lag_max_seconds,
                    connection_heartbeat_edge_proven, connection_stream_proven,
                    connection_stream_fresh, connection_continuity_state,
                    fill_depth_ready, fill_depth_reason, manifest_generated_at, archive_dir, shard_count,
                    archive_shard_ids,
                    three_source_gold_repair_window_count,
                    three_source_gold_repair_manifest_sha256,
                    three_source_gold_overlay_event_count,
                    three_source_gold_overlay_manifest_sha256,
                    three_source_gold_validation_status
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (asset_id, hour_start) DO UPDATE SET
                    desired_from = EXCLUDED.desired_from,
                    condition_id = EXCLUDED.condition_id,
                    market_slug = EXCLUDED.market_slug,
                    market_state = EXCLUDED.market_state,
                    execution_eligible = EXCLUDED.execution_eligible,
                    archive_row_count = EXCLUDED.archive_row_count,
                    event_count = EXCLUDED.event_count,
                    ws_event_count = EXCLUDED.ws_event_count,
                    control_event_count = EXCLUDED.control_event_count,
                    book_count = EXCLUDED.book_count,
                    ws_book_count = EXCLUDED.ws_book_count,
                    rest_seed_book_count = EXCLUDED.rest_seed_book_count,
                    price_change_count = EXCLUDED.price_change_count,
                    best_bid_ask_count = EXCLUDED.best_bid_ask_count,
                    last_trade_price_count = EXCLUDED.last_trade_price_count,
                    first_received_at = EXCLUDED.first_received_at,
                    last_received_at = EXCLUDED.last_received_at,
                    baseline_book_count = EXCLUDED.baseline_book_count,
                    ws_baseline_book_count = EXCLUDED.ws_baseline_book_count,
                    rest_seed_baseline_book_count = EXCLUDED.rest_seed_baseline_book_count,
                    baseline_received_at = EXCLUDED.baseline_received_at,
                    has_two_sided_book = EXCLUDED.has_two_sided_book,
                    price_change_after_baseline_count = EXCLUDED.price_change_after_baseline_count,
                    ws_gap_rest_seeded_count = EXCLUDED.ws_gap_rest_seeded_count,
                    ws_gap_recovered_count = EXCLUDED.ws_gap_recovered_count,
                    ws_gap_unseeded_count = EXCLUDED.ws_gap_unseeded_count,
                    rest_error_count = EXCLUDED.rest_error_count,
                    has_book = EXCLUDED.has_book,
                    has_ws_book = EXCLUDED.has_ws_book,
                    has_rest_seed_book = EXCLUDED.has_rest_seed_book,
                    has_price_change = EXCLUDED.has_price_change,
                    has_price_change_after_baseline = EXCLUDED.has_price_change_after_baseline,
                    rest_seed_only = EXCLUDED.rest_seed_only,
                    connection_shard_id = EXCLUDED.connection_shard_id,
                    connection_upstream_lag_sample_count = EXCLUDED.connection_upstream_lag_sample_count,
                    connection_upstream_lag_p50_seconds = EXCLUDED.connection_upstream_lag_p50_seconds,
                    connection_upstream_lag_p99_seconds = EXCLUDED.connection_upstream_lag_p99_seconds,
                    connection_upstream_lag_max_seconds = EXCLUDED.connection_upstream_lag_max_seconds,
                    connection_heartbeat_count = EXCLUDED.connection_heartbeat_count,
                    connection_heartbeat_edge_proven = EXCLUDED.connection_heartbeat_edge_proven,
                    connection_stream_proven = EXCLUDED.connection_stream_proven,
                    connection_stream_fresh = EXCLUDED.connection_stream_fresh,
                    connection_continuity_state = EXCLUDED.connection_continuity_state,
                    fill_depth_ready = EXCLUDED.fill_depth_ready,
                    fill_depth_reason = EXCLUDED.fill_depth_reason,
                    manifest_generated_at = EXCLUDED.manifest_generated_at,
                    archive_dir = EXCLUDED.archive_dir,
                    shard_count = EXCLUDED.shard_count,
                    archive_shard_ids = EXCLUDED.archive_shard_ids,
                    three_source_gold_repair_window_count =
                        EXCLUDED.three_source_gold_repair_window_count,
                    three_source_gold_repair_manifest_sha256 =
                        EXCLUDED.three_source_gold_repair_manifest_sha256,
                    three_source_gold_overlay_event_count =
                        EXCLUDED.three_source_gold_overlay_event_count,
                    three_source_gold_overlay_manifest_sha256 =
                        EXCLUDED.three_source_gold_overlay_manifest_sha256,
                    three_source_gold_validation_status =
                        EXCLUDED.three_source_gold_validation_status,
                    updated_at = now()
                """,
                [_db_row(row) for row in rows.to_dict("records")],
            )
            finalized = pd.to_datetime(rows["hour_start"], utc=True, errors="coerce").max()
            if not pd.isna(finalized):
                cur.execute(
                    """
                    INSERT INTO quant.clob_l2_watermarks (
                        worker_id, coverage_finalize_watermark, updated_at
                    ) VALUES (%s, %s, now())
                    ON CONFLICT (worker_id) DO UPDATE SET
                        coverage_finalize_watermark = GREATEST(
                            COALESCE(quant.clob_l2_watermarks.coverage_finalize_watermark, 'epoch'),
                            EXCLUDED.coverage_finalize_watermark
                        ),
                        updated_at = now()
                    """,
                    (watermark_id, finalized.to_pydatetime() + timedelta(hours=1)),
                )
        conn.commit()


def _coverage_watermark_id(coverage_table: str) -> str:
    if coverage_table == DEFAULT_COVERAGE_TABLE:
        return "coverage-finalizer"
    if coverage_table == GCP_EXECUTION_REDUNDANT_COVERAGE_TABLE:
        return "coverage-finalizer-gcp-execution-redundant"
    if coverage_table == GCP_HOT_STANDBY_PATCH_COVERAGE_TABLE:
        return "coverage-finalizer-gcp-hot-standby-patch"
    return "coverage-finalizer-gcp-batch"


def _normalize_archive_shard_ids(value: Any) -> list[int]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if isinstance(value, (str, bytes)):
        raise ValueError("archive_shard_ids must be an integer sequence")
    try:
        items = list(value)
    except TypeError:
        items = [value]
    return sorted(
        {
            int(item)
            for item in items
            if item is not None and not pd.isna(item)
        }
    )


def _normalize_gold_manifest_sha256(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    if isinstance(value, (str, bytes)):
        raise TypeError("Gold manifest SHA provenance must be an array")
    try:
        items = list(value)
    except TypeError:
        items = [value]
    digests = sorted({str(item).strip() for item in items if item is not None})
    invalid = [
        digest
        for digest in digests
        if len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ]
    if invalid:
        raise ValueError(f"invalid Gold manifest SHA provenance: {invalid!r}")
    return digests


def _gold_db_provenance(
    row: dict[str, Any],
) -> tuple[int, list[str], int, list[str], str]:
    repair_count = int(row.get("three_source_gold_repair_window_count") or 0)
    overlay_count = int(row.get("three_source_gold_overlay_event_count") or 0)
    repair_sha = _normalize_gold_manifest_sha256(
        row.get("three_source_gold_repair_manifest_sha256")
    )
    overlay_sha = _normalize_gold_manifest_sha256(
        row.get("three_source_gold_overlay_manifest_sha256")
    )
    status = str(row.get("three_source_gold_validation_status") or "DISABLED").strip()
    if repair_count < 0 or overlay_count < 0:
        raise ValueError("Gold coverage counts must be non-negative")
    if status not in THREE_SOURCE_GOLD_VALIDATION_STATUSES:
        raise ValueError(f"unsupported Gold validation status: {status!r}")
    if repair_count > 0 and not repair_sha:
        raise ValueError("Gold repair count is missing manifest SHA provenance")
    if overlay_count > 0 and not overlay_sha:
        raise ValueError("Gold overlay count is missing manifest SHA provenance")
    if repair_count == 0 and repair_sha:
        raise ValueError("Gold repair manifest SHA has no repair window")
    if overlay_count == 0 and overlay_sha:
        raise ValueError("Gold overlay manifest SHA has no overlay event")
    if (repair_count or overlay_count) and status != "PASS":
        raise ValueError("Gold coverage provenance is not validation PASS")
    return repair_count, repair_sha, overlay_count, overlay_sha, status


def _db_row(row: dict[str, Any]) -> tuple[Any, ...]:
    gold_provenance = _gold_db_provenance(row)
    return (
        row.get("asset_id"),
        row.get("hour_start"),
        _none_if_nat(row.get("desired_from")),
        row.get("condition_id"),
        row.get("market_slug"),
        row.get("market_state"),
        bool(row.get("execution_eligible")),
        int(row.get("archive_row_count") or 0),
        int(row.get("event_count") or 0),
        int(row.get("ws_event_count") or 0),
        int(row.get("control_event_count") or 0),
        int(row.get("book_count") or 0),
        int(row.get("ws_book_count") or 0),
        int(row.get("rest_seed_book_count") or 0),
        int(row.get("price_change_count") or 0),
        int(row.get("best_bid_ask_count") or 0),
        int(row.get("last_trade_price_count") or 0),
        _none_if_nat(row.get("first_received_at")),
        _none_if_nat(row.get("last_received_at")),
        int(row.get("baseline_book_count") or 0),
        int(row.get("ws_baseline_book_count") or 0),
        int(row.get("rest_seed_baseline_book_count") or 0),
        _none_if_nat(row.get("baseline_received_at")),
        bool(row.get("has_two_sided_book")),
        int(row.get("price_change_after_baseline_count") or 0),
        int(row.get("ws_gap_rest_seeded_count") or 0),
        int(row.get("ws_gap_recovered_count") or 0),
        int(row.get("ws_gap_unseeded_count") or 0),
        int(row.get("rest_error_count") or 0),
        bool(row.get("has_book")),
        bool(row.get("has_ws_book")),
        bool(row.get("has_rest_seed_book")),
        bool(row.get("has_price_change")),
        bool(row.get("has_price_change_after_baseline")),
        bool(row.get("rest_seed_only")),
        int(row["connection_shard_id"]) if row.get("connection_shard_id") is not None and not pd.isna(row.get("connection_shard_id")) else None,
        int(row.get("connection_upstream_lag_sample_count") or 0),
        int(row.get("connection_heartbeat_count") or 0),
        _none_if_nat(row.get("connection_upstream_lag_p50_seconds")),
        _none_if_nat(row.get("connection_upstream_lag_p99_seconds")),
        _none_if_nat(row.get("connection_upstream_lag_max_seconds")),
        bool(row.get("connection_heartbeat_edge_proven")),
        bool(row.get("connection_stream_proven")),
        bool(row.get("connection_stream_fresh")),
        str(row.get("connection_continuity_state") or "COVERAGE_GAP"),
        bool(row.get("fill_depth_ready")),
        str(row.get("fill_depth_reason") or ""),
        _none_if_nat(row.get("manifest_generated_at")),
        row.get("archive_dir"),
        int(row["shard_count"]) if row.get("shard_count") is not None and not pd.isna(row.get("shard_count")) else None,
        _normalize_archive_shard_ids(row.get("archive_shard_ids")),
        *gold_provenance,
    )


def _normalize_desired(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(
            columns=["asset_id", "condition_id", "market_slug", "market_state", "execution_eligible", "desired_from"]
        )
    result = rows.copy()
    for column in ("asset_id", "condition_id", "market_slug", "market_state"):
        if column not in result:
            result[column] = ""
    if "execution_eligible" not in result:
        result["execution_eligible"] = False
    if "desired_from" not in result:
        result["desired_from"] = pd.NaT
    result["asset_id"] = result["asset_id"].astype(str)
    result["desired_from"] = pd.to_datetime(result["desired_from"], utc=True, errors="coerce")
    result = result[result["asset_id"].str.len() > 0]
    return result[
        ["asset_id", "condition_id", "market_slug", "market_state", "execution_eligible", "desired_from"]
    ].drop_duplicates("asset_id")


def _normalize_lifecycle_hour_states(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return _empty_lifecycle_hour_states()
    result = rows.copy()
    for column in ("asset_id", "market_state"):
        if column not in result:
            result[column] = ""
    if "hour_start" not in result:
        result["hour_start"] = pd.NaT
    if "execution_eligible" not in result:
        result["execution_eligible"] = False
    result["asset_id"] = result["asset_id"].astype(str)
    result["hour_start"] = pd.to_datetime(result["hour_start"], utc=True, errors="coerce")
    result["execution_eligible"] = result["execution_eligible"].fillna(False).astype(bool)
    return result[(result["asset_id"].str.len() > 0) & result["hour_start"].notna()][
        ["asset_id", "hour_start", "market_state", "execution_eligible"]
    ].drop_duplicates(["asset_id", "hour_start"], keep="last")


def _normalize_gap_events(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return _empty_gap_events()
    result = rows.copy()
    for column in ("asset_id", "status"):
        if column not in result:
            result[column] = ""
    if "created_at" not in result:
        result["created_at"] = pd.NaT
    if "shard_id" not in result:
        result["shard_id"] = pd.NA
    if "gap_at" not in result:
        result["gap_at"] = pd.NaT
    if "recovered_at" not in result:
        result["recovered_at"] = pd.NaT
    result["asset_id"] = result["asset_id"].astype(str)
    result["status"] = result["status"].astype(str)
    result["created_at"] = pd.to_datetime(result["created_at"], utc=True, errors="coerce")
    result["shard_id"] = pd.to_numeric(result["shard_id"], errors="coerce").astype(
        "Int64"
    )
    result["gap_at"] = pd.to_datetime(
        result["gap_at"], utc=True, errors="coerce", format="mixed"
    )
    result["recovered_at"] = pd.to_datetime(
        result["recovered_at"], utc=True, errors="coerce", format="mixed"
    )
    result = result[result["asset_id"].str.len() > 0]
    result = result[result["created_at"].notna()]
    return result[
        [
            "asset_id",
            "status",
            "created_at",
            "shard_id",
            "gap_at",
            "recovered_at",
        ]
    ]


def _load_fail_closed_shard_gaps(
    path: Path,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
) -> pd.DataFrame:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "polymarket-source-a-fail-closed-shard-gaps-v1":
        raise ValueError(f"unsupported fail-closed shard-gap schema: {path}")
    if payload.get("status") != "SOURCE_A_FAIL_CLOSED_RAW_PREFIX_ABSENT":
        raise ValueError(f"fail-closed shard-gap contract is missing: {path}")
    gaps = payload.get("gaps")
    if not isinstance(gaps, list):
        raise ValueError(f"fail-closed shard gaps must be a list: {path}")
    if not gaps:
        return _empty_shard_gap_events()
    normalized: list[dict[str, Any]] = []
    for raw in gaps:
        if not isinstance(raw, dict):
            raise ValueError(f"fail-closed shard gap must be an object: {path}")
        row = dict(raw)
        proof = row.pop("repair_evidence", None)
        # A repair exemption is privileged input: never accept an inline list
        # from the mutable coverage contract.  It must point at a SHA-bound
        # evidence packet whose request and complete Source-B WAL patch are
        # both still locally verifiable.
        if "repaired_asset_ids" in row:
            raise ValueError(
                f"inline repaired_asset_ids are not permitted in {path}"
            )
        # Repair evidence can live on the Xue cold tier and may include a full
        # Source-B patch.  A one-hour hot coverage build must not SHA-read every
        # historical patch in the global loss contract: that turns a transient
        # SSHFS slowdown into a current coverage stall.  Validate the interval
        # fields for every row, then resolve privileged proof only for gaps that
        # overlap the requested coverage window.  Skipped intervals remain in
        # the global contract and will be verified when their own hour is built.
        gap_at = pd.to_datetime(row.get("gap_at"), utc=True, errors="coerce")
        recovered_at = pd.to_datetime(
            row.get("recovered_at"), utc=True, errors="coerce"
        )
        if pd.isna(gap_at) or pd.isna(recovered_at) or recovered_at <= gap_at:
            raise ValueError("fail-closed shard gap contains an invalid interval")
        if since is not None and recovered_at <= pd.Timestamp(since):
            continue
        if until is not None and gap_at >= pd.Timestamp(until):
            continue
        if proof is not None:
            row["repaired_asset_ids"] = _load_verified_shard_gap_repair(
                contract_path=path,
                gap=row,
                proof=proof,
            )
        normalized.append(row)
    return pd.DataFrame(normalized) if normalized else _empty_shard_gap_events()


def _normalize_shard_gap_events(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return _empty_shard_gap_events()
    result = rows.copy()
    for column in ("shard_id", "gap_at", "recovered_at"):
        if column not in result:
            raise ValueError(f"fail-closed shard gap is missing {column}")
    result["shard_id"] = pd.to_numeric(result["shard_id"], errors="coerce")
    result["gap_at"] = pd.to_datetime(
        result["gap_at"], utc=True, errors="coerce", format="mixed"
    )
    result["recovered_at"] = pd.to_datetime(
        result["recovered_at"], utc=True, errors="coerce", format="mixed"
    )
    valid = (
        result["shard_id"].notna()
        & (result["shard_id"] >= 0)
        & result["gap_at"].notna()
        & result["recovered_at"].notna()
        & (result["recovered_at"] > result["gap_at"])
    )
    if not bool(valid.all()):
        raise ValueError("fail-closed shard gap contains an invalid interval")
    result["shard_id"] = result["shard_id"].astype("int64")
    result["gap_id"] = result.apply(
        lambda row: _shard_gap_id(
            int(row["shard_id"]), row["gap_at"], row["recovered_at"]
        ),
        axis=1,
    )
    return result[
        ["gap_id", "shard_id", "gap_at", "recovered_at"]
    ].drop_duplicates()


def _normalize_shard_gap_repairs(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty or "repaired_asset_ids" not in rows:
        return _empty_shard_gap_repairs()
    normalized_gaps = _normalize_shard_gap_events(rows)
    gap_ids = normalized_gaps.set_index(
        ["shard_id", "gap_at", "recovered_at"]
    )["gap_id"].to_dict()
    repaired: list[dict[str, str]] = []
    for row in rows.to_dict("records"):
        raw_assets = row.get("repaired_asset_ids")
        if raw_assets is None or (
            isinstance(raw_assets, float) and pd.isna(raw_assets)
        ):
            continue
        if not isinstance(raw_assets, (list, tuple, set, frozenset)):
            raise ValueError("repaired_asset_ids must be a list of token IDs")
        shard_id = int(row["shard_id"])
        gap_at = pd.to_datetime(row["gap_at"], utc=True)
        recovered_at = pd.to_datetime(row["recovered_at"], utc=True)
        gap_id = gap_ids[(shard_id, gap_at, recovered_at)]
        for asset_id in sorted({str(value) for value in raw_assets if str(value)}):
            repaired.append({"gap_id": gap_id, "asset_id": asset_id})
    if not repaired:
        return _empty_shard_gap_repairs()
    return pd.DataFrame(repaired).drop_duplicates(["gap_id", "asset_id"])


def _load_verified_shard_gap_repair(
    *,
    contract_path: Path,
    gap: dict[str, Any],
    proof: Any,
) -> list[str]:
    if not isinstance(proof, dict):
        raise ValueError("shard-gap repair_evidence must be an object")
    evidence_path = _proof_path(contract_path, proof.get("path"))
    expected_sha = str(proof.get("sha256") or "")
    if len(expected_sha) != 64 or _sha256_file(evidence_path) != expected_sha:
        raise ValueError(f"shard-gap repair evidence SHA mismatch: {evidence_path}")
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    if (
        evidence.get("schema_version")
        != "polymarket-source-a-shard-gap-repair-v1"
        or evidence.get("status") != "PASS"
        or evidence.get("source_b_assignment_continuous") is not True
        or evidence.get("source_b_wal_window_complete") is not True
    ):
        raise ValueError(f"invalid shard-gap repair evidence: {evidence_path}")
    gap_at = pd.to_datetime(gap.get("gap_at"), utc=True)
    recovered_at = pd.to_datetime(gap.get("recovered_at"), utc=True)
    if (
        int(evidence.get("primary_shard_id", -1)) != int(gap["shard_id"])
        or pd.to_datetime(evidence.get("gap_at"), utc=True) != gap_at
        or pd.to_datetime(evidence.get("recovered_at"), utc=True)
        != recovered_at
    ):
        raise ValueError(f"shard-gap repair interval mismatch: {evidence_path}")
    assets = sorted(
        {
            str(value)
            for value in evidence.get("repaired_asset_ids") or ()
            if str(value)
        }
    )
    if not assets or int(evidence.get("repaired_asset_count") or -1) != len(assets):
        raise ValueError(f"invalid repaired asset set: {evidence_path}")
    for field in ("request", "patch_manifest"):
        artifact = evidence.get(field)
        if not isinstance(artifact, dict):
            raise ValueError(f"missing {field} proof: {evidence_path}")
        artifact_path = _proof_path(evidence_path, artifact.get("path"))
        artifact_sha = str(artifact.get("sha256") or "")
        if len(artifact_sha) != 64 or _sha256_file(artifact_path) != artifact_sha:
            raise ValueError(f"{field} SHA mismatch: {artifact_path}")
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        if field == "request":
            request_assets = {
                str(item.get("asset_id") or "")
                for item in payload.get("requests") or ()
                if str(item.get("asset_id") or "")
            }
            if not set(assets).issubset(request_assets):
                raise ValueError(f"repair assets are absent from request: {artifact_path}")
        else:
            window = payload.get("wal_window") or {}
            if (
                payload.get("schema_version")
                != "polymarket-l2-raw-wal-sparse-patch-v1"
                or payload.get("status") != "PASS"
                or window.get("complete") is not True
                or sorted(int(value) for value in window.get("expected_shards") or ())
                != list(range(48))
                or window.get("missing_shards")
                or window.get("late_start_shards")
                or window.get("early_end_shards")
                or window.get("segment_sequence_gaps")
            ):
                raise ValueError(f"Source-B patch is not complete: {artifact_path}")
            patch_root = Path(str(payload.get("output_dir") or ""))
            if not patch_root.is_absolute():
                patch_root = artifact_path.parent / patch_root
            patch_root = patch_root.resolve(strict=True)
            files = payload.get("files") or ()
            if not files or int(payload.get("file_count") or -1) != len(files):
                raise ValueError(f"Source-B patch file count is invalid: {artifact_path}")
            for item in files:
                relative = Path(str(item.get("path") or ""))
                if relative.is_absolute() or not relative.parts or ".." in relative.parts:
                    raise ValueError(f"unsafe Source-B patch path: {relative}")
                patch_file = patch_root / relative
                expected_size = int(item.get("file_size_bytes") or -1)
                expected_file_sha = str(item.get("sha256") or "")
                try:
                    patch_stat = patch_file.stat()
                except OSError:
                    # Cold-tier unavailability revokes this repair exemption;
                    # it is not corruption of the main archive and must not
                    # crash coverage for newer locally hot hours. Returning no
                    # repaired assets keeps the whole interval fail-closed.
                    return []
                if (
                    not stat.S_ISREG(patch_stat.st_mode)
                    or patch_stat.st_size != expected_size
                    or len(expected_file_sha) != 64
                ):
                    raise ValueError(f"Source-B patch file mismatch: {patch_file}")
                try:
                    actual_file_sha = _sha256_file(patch_file)
                except OSError:
                    return []
                if actual_file_sha != expected_file_sha:
                    raise ValueError(f"Source-B patch file mismatch: {patch_file}")
    return assets


def _proof_path(parent_artifact: Path, value: Any) -> Path:
    path = Path(str(value or ""))
    if not str(path):
        raise ValueError(f"empty proof path in {parent_artifact}")
    if not path.is_absolute():
        path = parent_artifact.parent / path
    return path.resolve(strict=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _shard_gap_id(shard_id: int, gap_at: Any, recovered_at: Any) -> str:
    return (
        f"{int(shard_id)}:"
        f"{pd.Timestamp(gap_at).value}:"
        f"{pd.Timestamp(recovered_at).value}"
    )


def _normalize_baseline_seeds(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return _empty_baseline_seeds()
    result = rows.copy()
    for column in ("asset_id", "source"):
        if column not in result:
            result[column] = ""
    if "timestamp_received" not in result:
        result["timestamp_received"] = pd.NaT
    if "two_sided" not in result:
        result["two_sided"] = False
    result["asset_id"] = result["asset_id"].astype(str)
    result["source"] = result["source"].astype(str)
    result["timestamp_received"] = pd.to_datetime(result["timestamp_received"], utc=True, errors="coerce")
    return result[(result["asset_id"].str.len() > 0) & result["timestamp_received"].notna()][
        ["asset_id", "timestamp_received", "source", "two_sided"]
    ].drop_duplicates("asset_id", keep="last")


def _empty_gap_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "asset_id": pd.Series(dtype="string"),
            "status": pd.Series(dtype="string"),
            "created_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "shard_id": pd.Series(dtype="Int64"),
            "gap_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "recovered_at": pd.Series(dtype="datetime64[ns, UTC]"),
        }
    )


def _empty_shard_gap_events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "gap_id": pd.Series(dtype="string"),
            "shard_id": pd.Series(dtype="int64"),
            "gap_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "recovered_at": pd.Series(dtype="datetime64[ns, UTC]"),
        }
    )


def _empty_shard_gap_repairs() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "gap_id": pd.Series(dtype="string"),
            "asset_id": pd.Series(dtype="string"),
        }
    )


def _empty_lifecycle_hour_states() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "asset_id": pd.Series(dtype="string"),
            "hour_start": pd.Series(dtype="datetime64[ns, UTC]"),
            "market_state": pd.Series(dtype="string"),
            "execution_eligible": pd.Series(dtype="bool"),
        }
    )


def _empty_baseline_seeds() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "asset_id": pd.Series(dtype="string"),
            "timestamp_received": pd.Series(dtype="datetime64[ns, UTC]"),
            "source": pd.Series(dtype="string"),
            "two_sided": pd.Series(dtype="bool"),
        }
    )


def _create_events_view(
    con: duckdb.DuckDBPyConnection,
    files: Sequence[str],
    *,
    gold: VerifiedThreeSourceGoldSet,
    since: datetime,
    until: datetime | None,
    evidence_until: datetime | None,
    shard_count: int | None,
) -> None:
    filters = [f"timestamp_received >= {_timestamp_literal(since)}"]
    all_filters = []
    if until is not None:
        filters.append(f"timestamp_received < {_timestamp_literal(until)}")
    if evidence_until is not None:
        all_filters.append(f"timestamp_received < {_timestamp_literal(evidence_until)}")
    if shard_count is not None:
        filters.append(f"shard_count = {int(shard_count)}")
        all_filters.append(f"shard_count = {int(shard_count)}")
    where = " AND ".join(filters)
    all_where = ("WHERE " + " AND ".join(all_filters)) if all_filters else ""
    event_columns = (
        "timestamp_received, timestamp, asset_id, event_type, source, shard_id, "
        "shard_count, bids, asks, collector_seq, sequence_in_message, best_bid, "
        "best_ask, transaction_hash, book_hash"
    )
    if not gold.overlays:
        # Preserve the pre-Gold query plan exactly when the optional tier is
        # disabled.  The two constant provenance columns are consumed only by
        # additive summary fields and cannot change readiness decisions.
        con.execute(
            f"""
            CREATE TEMP VIEW all_events AS
            SELECT {event_columns},
                   FALSE AS three_source_gold_overlay,
                   NULL::VARCHAR AS three_source_gold_manifest_sha256
            FROM read_parquet({_duckdb_file_list(files)})
            {all_where}
            """
        )
    else:
        overlay_paths = [str(item.overlay_path) for item in gold.overlays]
        combined_files = [*files, *overlay_paths]
        gold_files = pd.DataFrame(
            [
                {
                    "filename": str(item.overlay_path),
                    "manifest_sha256": item.manifest_sha256,
                }
                for item in gold.overlays
            ]
        )
        con.register("three_source_gold_file_input", gold_files)
        parquet_sql = (
            f"read_parquet({_duckdb_file_list(combined_files)}, "
            "filename=true, union_by_name=true)"
        )
        available = {
            str(row[0])
            for row in con.execute(f"DESCRIBE SELECT * FROM {parquet_sql}").fetchall()
        }
        payload_hash = _optional_event_column(available, "payload_hash", "VARCHAR")
        raw_connection_id = _optional_event_column(
            available, "raw_connection_id", "VARCHAR"
        )
        raw_generation = _optional_event_column(
            available, "raw_connection_generation", "BIGINT"
        )
        raw_frame_seq = _optional_event_column(available, "raw_frame_seq", "BIGINT")
        message_index = _optional_event_column(available, "message_index", "BIGINT")
        change_index = _optional_event_column(available, "change_index", "BIGINT")
        group_id = _optional_event_column(available, "group_id", "VARCHAR")
        gold_all_filters = []
        if evidence_until is not None:
            gold_all_filters.append(
                f"e.timestamp_received < {_timestamp_literal(evidence_until)}"
            )
        if shard_count is not None:
            # Gold is a C/D repair artifact for Source A. Its physical source
            # generation need not equal the primary generation being covered;
            # exact primary shards are bound by the validated repair windows.
            gold_all_filters.append(
                f"(e.shard_count = {int(shard_count)} "
                "OR g.manifest_sha256 IS NOT NULL)"
            )
        gold_all_where = (
            "WHERE " + " AND ".join(gold_all_filters)
            if gold_all_filters
            else ""
        )
        con.execute(
            f"""
            CREATE TEMP VIEW all_events AS
            WITH combined AS (
                SELECT e.*,
                       g.manifest_sha256 IS NOT NULL
                           AS three_source_gold_overlay,
                       g.manifest_sha256
                           AS three_source_gold_manifest_sha256,
                       {payload_hash} AS _payload_hash,
                       {raw_connection_id} AS _raw_connection_id,
                       {raw_generation} AS _raw_connection_generation,
                       {raw_frame_seq} AS _raw_frame_seq,
                       {message_index} AS _message_index,
                       {change_index} AS _change_index,
                       {group_id} AS _group_id
                FROM {parquet_sql} e
                LEFT JOIN three_source_gold_file_input g
                  ON e.filename = g.filename
                {gold_all_where}
            ), keyed AS (
                SELECT *,
                       CASE
                           WHEN length(COALESCE(_payload_hash, '')) = 64 THEN
                               concat_ws(':', 'payload', _payload_hash,
                                   COALESCE(CAST(asset_id AS VARCHAR), ''),
                                   COALESCE(CAST(event_type AS VARCHAR), ''),
                                   COALESCE(CAST(_message_index AS VARCHAR), ''),
                                   COALESCE(CAST(_change_index AS VARCHAR), ''))
                           WHEN length(COALESCE(_raw_connection_id, '')) > 0
                            AND _raw_frame_seq IS NOT NULL THEN
                               concat_ws(':', 'raw',
                                   COALESCE(CAST(source AS VARCHAR), ''),
                                   _raw_connection_id,
                                   COALESCE(CAST(_raw_connection_generation AS VARCHAR), ''),
                                   CAST(_raw_frame_seq AS VARCHAR),
                                   COALESCE(CAST(_message_index AS VARCHAR), ''),
                                   COALESCE(CAST(_change_index AS VARCHAR), ''),
                                   COALESCE(CAST(_group_id AS VARCHAR), ''),
                                   COALESCE(CAST(asset_id AS VARCHAR), ''),
                                   COALESCE(CAST(event_type AS VARCHAR), ''))
                           ELSE concat_ws(':', 'unique', filename,
                                   COALESCE(CAST(collector_seq AS VARCHAR), ''),
                                   COALESCE(CAST(sequence_in_message AS VARCHAR), ''))
                       END AS _gold_dedupe_key
                FROM combined
            ), participating AS (
                SELECT *,
                       bool_or(three_source_gold_overlay) OVER (
                           PARTITION BY _gold_dedupe_key
                       ) AS _gold_identity_has_overlay,
                       row_number() OVER (
                           PARTITION BY _gold_dedupe_key
                           ORDER BY timestamp_received,
                                    three_source_gold_overlay ASC,
                                    COALESCE(collector_seq, 0),
                                    COALESCE(sequence_in_message, 0),
                                    filename
                       ) AS _gold_dedupe_rank
                FROM keyed
            )
            SELECT {event_columns},
                   three_source_gold_overlay,
                   three_source_gold_manifest_sha256
            FROM participating
            WHERE NOT _gold_identity_has_overlay OR _gold_dedupe_rank = 1
            """
        )
        filters = [f"timestamp_received >= {_timestamp_literal(since)}"]
        if until is not None:
            filters.append(f"timestamp_received < {_timestamp_literal(until)}")
        if shard_count is not None:
            filters.append(
                f"(shard_count = {int(shard_count)} "
                "OR three_source_gold_overlay)"
            )
        where = " AND ".join(filters)
    con.execute(
        f"""
        CREATE TEMP VIEW events AS
        SELECT timestamp_received, timestamp, asset_id, event_type, source, shard_id, shard_count, bids, asks,
               collector_seq, sequence_in_message, best_bid, best_ask,
               transaction_hash, book_hash,
               three_source_gold_overlay,
               three_source_gold_manifest_sha256
        FROM all_events
        WHERE {where}
        """
    )


def _optional_event_column(
    available: set[str], name: str, sql_type: str
) -> str:
    if name in available:
        return f"e.{name}"
    return f"NULL::{sql_type}"


def _create_hours_view(con: duckdb.DuckDBPyConnection, *, since: datetime, until: datetime | None) -> None:
    end = until or datetime.now(timezone.utc)
    start_hour = since.replace(minute=0, second=0, microsecond=0)
    end_hour = end.replace(minute=0, second=0, microsecond=0)
    if end > end_hour:
        end_hour = end_hour + timedelta(hours=1)
    con.execute(
        f"""
        CREATE TEMP VIEW hours AS
        SELECT hour_start
        FROM generate_series({_timestamp_literal(start_hour)}, ({_timestamp_literal(end_hour)} - INTERVAL 1 HOUR), INTERVAL 1 HOUR) AS t(hour_start)
        """,
    )


def _coverage_hours(*, since: datetime, until: datetime | None) -> list[datetime]:
    end = until or datetime.now(timezone.utc)
    hour = since.replace(minute=0, second=0, microsecond=0)
    end_hour = end.replace(minute=0, second=0, microsecond=0)
    if end > end_hour:
        end_hour += timedelta(hours=1)
    hours: list[datetime] = []
    while hour < end_hour:
        hours.append(hour)
        hour += timedelta(hours=1)
    return hours


def _archive_files(
    path: Path,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    extra_lookback_hours: float = 0.0,
    extra_lookahead_hours: float = 0.0,
) -> list[str]:
    files = sorted(
        str(item)
        for item in _archive_hour_dirs(
            path,
            since=since,
            until=until,
            extra_lookback_hours=extra_lookback_hours,
            extra_lookahead_hours=extra_lookahead_hours,
        )
        if not item.name.endswith(".tmp.parquet")
    )
    if not files:
        raise FileNotFoundError(f"no parquet files under {path}")
    return files


def _archive_hour_dirs(
    path: Path,
    *,
    since: datetime | None,
    until: datetime | None,
    extra_lookback_hours: float,
    extra_lookahead_hours: float,
) -> list[Path]:
    if since is None and until is None:
        return [item for item in path.rglob("*.parquet") if not item.name.endswith(".tmp.parquet")]
    if since is None:
        start = datetime.min.replace(tzinfo=timezone.utc)
    else:
        start = _floor_hour(since - timedelta(hours=max(0.0, float(extra_lookback_hours))))
    end_source = (until or datetime.now(timezone.utc) + timedelta(hours=1)) + timedelta(
        hours=max(0.0, float(extra_lookahead_hours))
    )
    end = _ceil_hour(end_source)
    structured: list[Path] = []
    found_structured_dirs = False
    for dt_dir in sorted(path.glob("dt=*")):
        if not dt_dir.is_dir():
            continue
        date_text = dt_dir.name.removeprefix("dt=")
        for hour_dir in sorted(dt_dir.glob("hour=*")):
            if not hour_dir.is_dir():
                continue
            found_structured_dirs = True
            hour_text = hour_dir.name.removeprefix("hour=")
            try:
                hour_start = datetime.fromisoformat(f"{date_text}T{int(hour_text):02d}:00:00+00:00")
            except ValueError:
                continue
            if hour_start < start or hour_start >= end:
                continue
            structured.extend(item for item in hour_dir.glob("*.parquet") if not item.name.endswith(".tmp.parquet"))
    if structured or found_structured_dirs:
        return structured
    return [item for item in path.rglob("*.parquet") if not item.name.endswith(".tmp.parquet")]


def _floor_hour(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _ceil_hour(value: datetime) -> datetime:
    floored = _floor_hour(value)
    return floored if value == floored else floored + timedelta(hours=1)


def _duckdb_file_list(files: Sequence[str]) -> str:
    return "[" + ",".join(_string_literal(item) for item in files) + "]"


def _resolve_since(value: str | None, lookback_hours: float) -> datetime:
    if value:
        return _parse_datetime(value)
    return datetime.now(timezone.utc) - timedelta(hours=max(0.1, float(lookback_hours)))


def _parse_datetime(value: str) -> datetime:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _none_if_nat(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        return value
    return value


def _timestamp_literal(value: datetime) -> str:
    return "TIMESTAMPTZ " + _string_literal(value.isoformat())


def _string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _int_or_null(value: int | None) -> str:
    return "NULL" if value is None else str(int(value))


if __name__ == "__main__":
    raise SystemExit(main())
