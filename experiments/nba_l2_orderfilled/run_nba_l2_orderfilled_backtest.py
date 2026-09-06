#!/usr/bin/env python3
"""Build a first-month NBA full-game matchup OrderFilled + PMXT L2 experiment.

This experiment is intentionally narrow:

* universe: 2025-2026 NBA single-game team-vs-team markets with slugs like
  ``nba-lal-hou-2026-04-24`` and titles like "Lakers vs. Rockets";
* historical fills: local OrderFilled-derived block tape plus optional
  ClickHouse tick replay backfill;
* LOB: PMXT hourly parquet, scanned at condition/token/hour granularity;
* execution: the existing L2OrderFilledExecutionModel / DEPTH path, not a new
  matching engine.

The default run is conservative and read-only: it selects the first eligible
one-month window and reports what is available. Add --download-remote,
--backfill-trade-replay, --materialize-snapshots, or --run-depth-backtests to
perform heavier steps.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.l2_orderfilled_execution import L2ExecutionConfig, l2_execution_config_to_audit_dict
from quant.backtest.runners.trade_replay_store import backfill_orderfilled_trade_replay
from quant.core.db import postgres_connection
from scripts.check_pmxt_l2_coverage import TargetHour, scan_hour_file
from scripts.validate_pmxt_l2_raw import archive_filename_for_hour, parse_hour


PMXT_LEGACY_START = datetime(2026, 2, 21, 16, tzinfo=UTC)
PMXT_V2_START = datetime(2026, 4, 13, 19, tzinfo=UTC)
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime_outputs" / "nba_l2_orderfilled"
DEFAULT_PMXT_ROOT = PROJECT_ROOT / "runtime_outputs" / "pmxt_archive"
DEFAULT_REMOTE_BASE_URLS = ("https://r2v2.pmxt.dev", "https://r2.pmxt.dev")

NBA_H2H_INCLUDE_SQL = """
    market_slug ~ '^nba-[a-z]{2,3}-[a-z]{2,3}-20[0-9]{2}-[0-9]{2}-[0-9]{2}$'
    AND txt ~ '( vs\\.? |\\-vs\\-)'
"""
NBA_H2H_EXCLUDE_SQL = """
    txt !~ '(series|total games|ou|o/u|spread|draft|lottery|receive|points|rebounds|assists|mvp|championship|conference|division|all-star|parley|cup|1h|first half|moneyline:)'
"""


@dataclass(frozen=True)
class NbaMarket:
    market_id: int
    market_slug: str
    market_title: str
    condition_id: str
    created_at: str | None
    end_date: str | None
    first_fill_ts: str | None
    last_fill_ts: str | None
    orderfilled_trades: int
    orderfilled_block_rows: int
    from_block: int | None
    to_block: int | None


@dataclass(frozen=True)
class NbaToken:
    market_id: int
    market_slug: str
    market_title: str
    condition_id: str
    token_id: str
    token_side: str
    token_id_hex: str | None = None
    block_rows: int = 0
    trade_count: int = 0
    volume: str = "0"
    first_block: int | None = None
    last_block: int | None = None
    first_block_ts: str | None = None
    last_block_ts: str | None = None


@dataclass
class CommandPlan:
    label: str
    command: list[str]


@dataclass
class CommandResult:
    label: str
    command: list[str]
    returncode: int
    duration_seconds: float
    stdout_tail: str = ""
    stderr_tail: str = ""


@dataclass
class ExperimentReport:
    schema_version: str
    status: str
    parameters: dict[str, Any]
    selected_window: dict[str, Any]
    universe: dict[str, Any]
    orderfilled_data: dict[str, Any]
    pmxt_l2_data: dict[str, Any]
    execution_model: dict[str, Any]
    command_plan: list[dict[str, Any]] = field(default_factory=list)
    command_results: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    next_actions: list[str] = field(default_factory=list)


def main() -> int:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    start_hour = parse_hour(args.start_hour)
    end_hour = parse_hour(args.end_hour)

    with postgres_connection(readonly=not bool(args.backfill_trade_replay)) as conn:
        if start_hour is None:
            start_hour = discover_first_nba_h2h_orderfilled_hour(conn)
        if start_hour is None:
            report = empty_report(args, "missing_universe", "No NBA Team-vs-Team market with OrderFilled data was found.")
            write_outputs(output_dir, report)
            print(report_to_markdown(report))
            return 2
        if end_hour is None:
            end_hour = start_hour + timedelta(days=max(1, int(args.month_days))) - timedelta(hours=1)

        markets = select_nba_h2h_markets(
            conn,
            start_hour=start_hour,
            end_hour=end_hour,
            limit=max(1, int(args.max_markets)),
            min_trades=max(0, int(args.min_orderfilled_trades)),
        )
        tokens = load_market_tokens(conn, markets, start_hour=start_hour, end_hour=end_hour)

    if int(args.max_hours) > 0:
        target_hours = sorted({hour for token in tokens for hour in token_hours(token, start_hour=start_hour, end_hour=end_hour)})[: int(args.max_hours)]
    else:
        target_hours = all_hours(start_hour, end_hour)

    hour_statuses = prepare_pmxt_hours(
        Path(args.pmxt_root),
        target_hours,
        download_remote=bool(args.download_remote),
        remote_bases=tuple(args.remote_base_url or DEFAULT_REMOTE_BASE_URLS),
        timeout_seconds=max(1.0, float(args.remote_timeout_seconds)),
    )
    pmxt_rows = scan_pmxt_l2_coverage(Path(args.pmxt_root), tokens, target_hours, batch_size=max(1, int(args.pmxt_batch_size)))
    command_plan = build_command_plan(args, markets=markets, tokens=tokens, start_hour=start_hour, end_hour=end_hour)
    command_results: list[CommandResult] = []

    if args.backfill_trade_replay and tokens:
        command_results.append(run_trade_replay_backfill(tokens, build_tag=args.build_tag))
    if args.materialize_snapshots:
        command_results.extend(run_planned_commands([plan for plan in command_plan if plan.label.startswith("materialize")], max_commands=int(args.max_executed_commands)))
    if args.run_depth_backtests:
        command_results.extend(run_planned_commands([plan for plan in command_plan if plan.label.startswith("backtest")], max_commands=int(args.max_executed_commands)))

    report = build_report(
        args,
        start_hour=start_hour,
        end_hour=end_hour,
        markets=markets,
        tokens=tokens,
        target_hours=target_hours,
        hour_statuses=hour_statuses,
        pmxt_rows=pmxt_rows,
        command_plan=command_plan,
        command_results=command_results,
    )
    write_outputs(output_dir, report)
    if args.format == "json":
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str))
    else:
        print(report_to_markdown(report))
    return 0 if report.status in {"ready", "review"} else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pmxt-root", type=Path, default=DEFAULT_PMXT_ROOT)
    parser.add_argument("--start-hour", default=None, help="UTC hour. Defaults to first NBA H2H OrderFilled hour.")
    parser.add_argument("--end-hour", default=None, help="UTC hour. Defaults to start + --month-days - 1 hour.")
    parser.add_argument("--month-days", type=int, default=31)
    parser.add_argument("--max-markets", type=int, default=5)
    parser.add_argument("--max-hours", type=int, default=0, help="0 means all hours in the selected window.")
    parser.add_argument("--min-orderfilled-trades", type=int, default=1)
    parser.add_argument("--download-remote", action="store_true")
    parser.add_argument("--remote-base-url", action="append", default=[])
    parser.add_argument("--remote-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--pmxt-batch-size", type=int, default=250_000)
    parser.add_argument("--backfill-trade-replay", action="store_true")
    parser.add_argument("--materialize-snapshots", action="store_true")
    parser.add_argument("--run-depth-backtests", action="store_true")
    parser.add_argument("--max-executed-commands", type=int, default=3)
    parser.add_argument("--sample-interval-seconds", type=int, default=60)
    parser.add_argument("--position-size", default="10")
    parser.add_argument("--entry-threshold", default="0.50")
    parser.add_argument("--exit-threshold", default="0.35")
    parser.add_argument("--take-profit", default="0.10")
    parser.add_argument("--build-tag", default="nba_l2_orderfilled_first_month")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    return parser


def discover_first_nba_h2h_orderfilled_hour(conn: Any) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH nba AS (
                SELECT DISTINCT
                    market_id,
                    market_slug,
                    lower(coalesce(market_slug,'') || ' ' || coalesce(market_title,'')) AS txt
                FROM quant.market_token_metadata
            ),
            selected AS (
                SELECT market_id
                FROM nba
                WHERE {NBA_H2H_INCLUDE_SQL}
                  AND {NBA_H2H_EXCLUDE_SQL}
            )
            SELECT date_trunc('hour', MIN(b.block_timestamp)) AS first_hour
            FROM quant.market_token_block_close b
            JOIN selected s ON s.market_id = b.market_id
            WHERE b.block_timestamp >= %s
              AND b.block_timestamp < TIMESTAMPTZ '2026-07-01 00:00:00+00'
              AND COALESCE(b.trade_count, 0) > 0
            """,
            (PMXT_LEGACY_START,),
        )
        row = cur.fetchone()
    value = row["first_hour"] if row else None
    if value is None:
        return None
    return ensure_utc_hour(value)


def select_nba_h2h_markets(
    conn: Any,
    *,
    start_hour: datetime,
    end_hour: datetime,
    limit: int,
    min_trades: int,
) -> list[NbaMarket]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH nba AS (
                SELECT DISTINCT
                    market_id,
                    market_slug,
                    market_title,
                    condition_id,
                    created_at,
                    end_date,
                    lower(coalesce(market_slug,'') || ' ' || coalesce(market_title,'')) AS txt
                FROM quant.market_token_metadata
            ),
            selected AS (
                SELECT *
                FROM nba
                WHERE {NBA_H2H_INCLUDE_SQL}
                  AND {NBA_H2H_EXCLUDE_SQL}
            ),
            fill_stats AS (
                SELECT
                    b.market_id,
                    COUNT(*)::bigint AS block_rows,
                    SUM(COALESCE(b.trade_count, 0))::bigint AS trades,
                    MIN(b.block_timestamp) AS first_fill_ts,
                    MAX(b.block_timestamp) AS last_fill_ts,
                    MIN(b.block_number) AS from_block,
                    MAX(b.block_number) AS to_block
                FROM quant.market_token_block_close b
                JOIN selected s ON s.market_id = b.market_id
                WHERE b.block_timestamp >= %(start_hour)s
                  AND b.block_timestamp <= %(end_hour)s + INTERVAL '1 hour'
                  AND COALESCE(b.trade_count, 0) > 0
                GROUP BY b.market_id
            )
            SELECT
                s.market_id, s.market_slug, s.market_title, s.condition_id, s.created_at, s.end_date,
                f.first_fill_ts, f.last_fill_ts, f.trades, f.block_rows, f.from_block, f.to_block
            FROM selected s
            JOIN fill_stats f ON f.market_id = s.market_id
            WHERE f.trades >= %(min_trades)s
            ORDER BY f.trades DESC, f.first_fill_ts ASC, s.market_id ASC
            LIMIT %(limit)s
            """,
            {
                "start_hour": start_hour,
                "end_hour": end_hour,
                "min_trades": int(min_trades),
                "limit": int(limit),
            },
        )
        rows = list(cur.fetchall())
    return [
        NbaMarket(
            market_id=int(row["market_id"]),
            market_slug=str(row["market_slug"] or ""),
            market_title=str(row["market_title"] or ""),
            condition_id=str(row["condition_id"] or ""),
            created_at=iso_or_none(row["created_at"]),
            end_date=iso_or_none(row["end_date"]),
            first_fill_ts=iso_or_none(row["first_fill_ts"]),
            last_fill_ts=iso_or_none(row["last_fill_ts"]),
            orderfilled_trades=int(row["trades"] or 0),
            orderfilled_block_rows=int(row["block_rows"] or 0),
            from_block=int(row["from_block"]) if row["from_block"] is not None else None,
            to_block=int(row["to_block"]) if row["to_block"] is not None else None,
        )
        for row in rows
    ]


def load_market_tokens(conn: Any, markets: Sequence[NbaMarket], *, start_hour: datetime, end_hour: datetime) -> list[NbaToken]:
    if not markets:
        return []
    market_ids = [market.market_id for market in markets]
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH token_meta AS (
                SELECT DISTINCT
                    market_id, market_slug, market_title, condition_id, token_id,
                    upper(coalesce(token_side,'')) AS token_side, token_id_hex
                FROM quant.market_token_metadata
                WHERE market_id = ANY(%(market_ids)s)
            ),
            stats AS (
                SELECT
                    market_id,
                    token_id,
                    COUNT(*)::bigint AS block_rows,
                    SUM(COALESCE(trade_count, 0))::bigint AS trade_count,
                    SUM(COALESCE(volume, 0)) AS volume,
                    MIN(block_number) AS first_block,
                    MAX(block_number) AS last_block,
                    MIN(block_timestamp) AS first_block_ts,
                    MAX(block_timestamp) AS last_block_ts
                FROM quant.market_token_block_close
                WHERE market_id = ANY(%(market_ids)s)
                  AND block_timestamp >= %(start_hour)s
                  AND block_timestamp <= %(end_hour)s + INTERVAL '1 hour'
                  AND COALESCE(trade_count, 0) > 0
                GROUP BY market_id, token_id
            )
            SELECT
                m.*, COALESCE(s.block_rows, 0) AS block_rows,
                COALESCE(s.trade_count, 0) AS trade_count,
                COALESCE(s.volume, 0) AS volume,
                s.first_block, s.last_block, s.first_block_ts, s.last_block_ts
            FROM token_meta m
            LEFT JOIN stats s ON s.market_id = m.market_id AND lower(s.token_id) = lower(m.token_id)
            ORDER BY m.market_id, m.token_side
            """,
            {"market_ids": market_ids, "start_hour": start_hour, "end_hour": end_hour},
        )
        rows = list(cur.fetchall())
    return [
        NbaToken(
            market_id=int(row["market_id"]),
            market_slug=str(row["market_slug"] or ""),
            market_title=str(row["market_title"] or ""),
            condition_id=str(row["condition_id"] or ""),
            token_id=str(row["token_id"] or "").lower(),
            token_side=str(row["token_side"] or ""),
            token_id_hex=str(row["token_id_hex"]) if row["token_id_hex"] else None,
            block_rows=int(row["block_rows"] or 0),
            trade_count=int(row["trade_count"] or 0),
            volume=str(row["volume"] or "0"),
            first_block=int(row["first_block"]) if row["first_block"] is not None else None,
            last_block=int(row["last_block"]) if row["last_block"] is not None else None,
            first_block_ts=iso_or_none(row["first_block_ts"]),
            last_block_ts=iso_or_none(row["last_block_ts"]),
        )
        for row in rows
    ]


def token_hours(token: NbaToken, *, start_hour: datetime, end_hour: datetime) -> list[datetime]:
    start = parse_hour(token.first_block_ts) if token.first_block_ts else start_hour
    end = parse_hour(token.last_block_ts) if token.last_block_ts else end_hour
    if start is None:
        start = start_hour
    if end is None:
        end = end_hour
    start = max(ensure_utc_hour(start), start_hour)
    end = min(ensure_utc_hour(end), end_hour)
    return all_hours(start, end)


def all_hours(start_hour: datetime, end_hour: datetime) -> list[datetime]:
    hours: list[datetime] = []
    current = ensure_utc_hour(start_hour)
    limit = ensure_utc_hour(end_hour)
    while current <= limit:
        hours.append(current)
        current += timedelta(hours=1)
    return hours


def prepare_pmxt_hours(
    pmxt_root: Path,
    hours: Sequence[datetime],
    *,
    download_remote: bool,
    remote_bases: Sequence[str],
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    statuses: list[dict[str, Any]] = []
    for hour in hours:
        local = local_pmxt_path(pmxt_root, hour)
        if local is not None:
            statuses.append({"hour": hour.isoformat(), "status": "local", "path": str(local), "bytes": local.stat().st_size})
            continue
        remote = find_remote_pmxt(hour, remote_bases=remote_bases, timeout_seconds=timeout_seconds)
        if remote is None:
            statuses.append({"hour": hour.isoformat(), "status": "missing_remote"})
            continue
        if not download_remote:
            statuses.append({"hour": hour.isoformat(), "status": "remote_available", **remote})
            continue
        downloaded = download_pmxt_hour(pmxt_root, hour, remote["url"], timeout_seconds=timeout_seconds)
        statuses.append({"hour": hour.isoformat(), "status": "downloaded", "path": str(downloaded), "bytes": downloaded.stat().st_size, **remote})
    return statuses


def scan_pmxt_l2_coverage(pmxt_root: Path, tokens: Sequence[NbaToken], hours: Sequence[datetime], *, batch_size: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    token_by_hour: dict[datetime, list[NbaToken]] = {}
    for token in tokens:
        for hour in hours:
            if token.first_block_ts and parse_hour(token.first_block_ts) and hour < parse_hour(token.first_block_ts):  # type: ignore[arg-type]
                continue
            if token.last_block_ts and parse_hour(token.last_block_ts) and hour > parse_hour(token.last_block_ts):  # type: ignore[arg-type]
                continue
            token_by_hour.setdefault(hour, []).append(token)
    for hour, hour_tokens in sorted(token_by_hour.items()):
        path = local_pmxt_path(pmxt_root, hour)
        if path is None:
            for token in hour_tokens:
                rows.append(base_coverage_row(token, hour, "missing_local_pmxt_hour"))
            continue
        targets = [
            TargetHour(
                hour=hour,
                market_id=token.market_id,
                market_slug=token.market_slug,
                market_title=token.market_title,
                condition_id=token.condition_id,
                token_id=token.token_id,
                token_side=token.token_side,
                block_close_rows=token.block_rows,
                first_block=token.first_block,
                last_block=token.last_block,
            )
            for token in hour_tokens
        ]
        rows.extend(row.__dict__ for row in scan_hour_file(path, targets, batch_size=batch_size))
    return rows


def build_command_plan(
    args: argparse.Namespace,
    *,
    markets: Sequence[NbaMarket],
    tokens: Sequence[NbaToken],
    start_hour: datetime,
    end_hour: datetime,
) -> list[CommandPlan]:
    plans: list[CommandPlan] = []
    for market in markets:
        market_tokens = [token for token in tokens if token.market_id == market.market_id and token.trade_count > 0]
        if not market_tokens:
            continue
        top_token = sorted(market_tokens, key=lambda token: token.trade_count, reverse=True)[0]
        plans.append(
            CommandPlan(
                label=f"materialize:{market.market_slug}:{top_token.token_side}",
                command=[
                    sys.executable,
                    str(PROJECT_ROOT / "scripts" / "materialize_pmxt_l2_snapshots.py"),
                    "--pmxt-root",
                    str(args.pmxt_root),
                    "--market-slug",
                    market.market_slug,
                    "--token-id",
                    top_token.token_id,
                    "--token-side",
                    top_token.token_side if top_token.token_side in {"YES", "NO"} else "YES",
                    "--start-hour",
                    hour_arg(start_hour),
                    "--end-hour",
                    hour_arg(end_hour),
                    "--sample-interval-seconds",
                    str(max(1, int(args.sample_interval_seconds))),
                    "--write",
                ],
            )
        )
        if top_token.first_block is not None and top_token.last_block is not None:
            plans.append(
                CommandPlan(
                    label=f"backtest:{market.market_slug}:{top_token.token_side}",
                    command=[
                        sys.executable,
                        str(PROJECT_ROOT / "scripts" / "run_fill_first_sample_backtest.py"),
                        "--market-slug",
                        market.market_slug,
                        "--token-id",
                        top_token.token_id,
                        "--token-side",
                        top_token.token_side if top_token.token_side in {"YES", "NO"} else "YES",
                        "--from-block",
                        str(top_token.first_block),
                        "--to-block",
                        str(top_token.last_block),
                        "--execution-price-mode",
                        "DEPTH",
                        "--order-role",
                        "taker",
                        "--entry-threshold",
                        str(args.entry_threshold),
                        "--exit-threshold",
                        str(args.exit_threshold),
                        "--take-profit",
                        str(args.take_profit),
                        "--position-size",
                        str(args.position_size),
                        "--format",
                        "markdown",
                    ],
                )
            )
    return plans


def run_trade_replay_backfill(tokens: Sequence[NbaToken], *, build_tag: str) -> CommandResult:
    pairs = [
        (token.market_id, str(token.token_id_hex or token.token_id or "").strip().lower())
        for token in tokens
        if token.first_block is not None
        and token.last_block is not None
        and str(token.token_id_hex or token.token_id or "").strip()
    ]
    if not pairs:
        return CommandResult("orderfilled_trade_replay", [], 2, 0.0, stderr_tail="no token/block pairs")
    from_block = min(int(token.first_block) for token in tokens if token.first_block is not None)
    to_block = max(int(token.last_block) for token in tokens if token.last_block is not None)
    start = time.perf_counter()
    try:
        result = backfill_orderfilled_trade_replay(pairs, from_block=from_block, to_block=to_block, build_tag=build_tag)
        return CommandResult("orderfilled_trade_replay", ["backfill_orderfilled_trade_replay"], 0, round(time.perf_counter() - start, 6), stdout_tail=json.dumps(result.as_dict(), default=str))
    except Exception as exc:
        return CommandResult("orderfilled_trade_replay", ["backfill_orderfilled_trade_replay"], 2, round(time.perf_counter() - start, 6), stderr_tail=f"{type(exc).__name__}: {exc}")


def run_planned_commands(plans: Sequence[CommandPlan], *, max_commands: int) -> list[CommandResult]:
    results: list[CommandResult] = []
    for plan in list(plans)[: max(0, int(max_commands))]:
        start = time.perf_counter()
        proc = subprocess.run(plan.command, cwd=PROJECT_ROOT, text=True, capture_output=True, timeout=1800, check=False)
        results.append(
            CommandResult(
                label=plan.label,
                command=plan.command,
                returncode=int(proc.returncode),
                duration_seconds=round(time.perf_counter() - start, 6),
                stdout_tail=tail(proc.stdout),
                stderr_tail=tail(proc.stderr),
            )
        )
    return results


def build_report(
    args: argparse.Namespace,
    *,
    start_hour: datetime,
    end_hour: datetime,
    markets: Sequence[NbaMarket],
    tokens: Sequence[NbaToken],
    target_hours: Sequence[datetime],
    hour_statuses: Sequence[Mapping[str, Any]],
    pmxt_rows: Sequence[Mapping[str, Any]],
    command_plan: Sequence[CommandPlan],
    command_results: Sequence[CommandResult],
) -> ExperimentReport:
    matched_rows = [row for row in pmxt_rows if coverage_status(row) == "matched"]
    token_pairs = {(token.condition_id, token.token_id) for token in tokens}
    matched_pairs = {(str(row.get("condition_id")), str(row.get("token_id"))) for row in matched_rows}
    local_hours = sum(1 for row in hour_statuses if row.get("status") in {"local", "downloaded"})
    remote_hours = sum(1 for row in hour_statuses if row.get("status") == "remote_available")
    missing_hours = sum(1 for row in hour_statuses if str(row.get("status", "")).startswith("missing"))
    warnings: list[str] = []
    if not markets:
        warnings.append("No NBA Team-vs-Team markets were found in the selected window.")
    if remote_hours and not args.download_remote:
        warnings.append("PMXT raw exists remotely but was not downloaded; L2 coverage can only be scanned for local hours.")
    if missing_hours:
        warnings.append("Some PMXT hours were not found locally or remotely.")
    if not matched_rows:
        warnings.append("No local PMXT token-level L2 matches were scanned; download/materialize PMXT hours before DEPTH execution.")

    status = "ready" if matched_rows and command_plan else "review"
    if not markets or not tokens:
        status = "missing"
    next_actions = [
        "Run with --download-remote for the selected month to populate the PMXT raw cache.",
        "Run with --materialize-snapshots after raw hours are local, then execute the generated DEPTH backtests.",
        "Use --backfill-trade-replay to materialize deterministic OrderFilled TradeTick rows for maker queue calibration.",
    ]
    if matched_rows:
        next_actions.insert(0, "The selected local PMXT hours have token-level L2 matches; run DEPTH smoke on these markets first.")

    return ExperimentReport(
        schema_version="nba_l2_orderfilled_first_month_v1",
        status=status,
        parameters={
            "pmxt_root": str(args.pmxt_root),
            "start_hour": start_hour.isoformat(),
            "end_hour": end_hour.isoformat(),
            "month_days": int(args.month_days),
            "max_markets": int(args.max_markets),
            "max_hours": int(args.max_hours),
            "download_remote": bool(args.download_remote),
            "backfill_trade_replay": bool(args.backfill_trade_replay),
            "materialize_snapshots": bool(args.materialize_snapshots),
            "run_depth_backtests": bool(args.run_depth_backtests),
        },
        selected_window={
            "start_hour": start_hour.isoformat(),
            "end_hour": end_hour.isoformat(),
            "hour_count": len(all_hours(start_hour, end_hour)),
            "scanned_hour_count": len(target_hours),
            "pmxt_legacy_start": PMXT_LEGACY_START.isoformat(),
            "pmxt_v2_start": PMXT_V2_START.isoformat(),
        },
        universe={
            "selector": "NBA playoff Team-vs-Team market, excluding props/totals/draft/parley/cup",
            "market_count": len(markets),
            "token_count": len(tokens),
            "markets": [asdict(market) for market in markets],
            "tokens": [asdict(token) for token in tokens],
        },
        orderfilled_data={
            "block_rows": sum(token.block_rows for token in tokens),
            "trade_count": sum(token.trade_count for token in tokens),
            "token_pairs_with_trades": sum(1 for token in tokens if token.trade_count > 0),
            "backfill_command_executed": bool(args.backfill_trade_replay),
        },
        pmxt_l2_data={
            "hour_status_counts": counts(row.get("status") for row in hour_statuses),
            "local_or_downloaded_hour_count": local_hours,
            "remote_available_hour_count": remote_hours,
            "missing_hour_count": missing_hours,
            "coverage_rows": len(pmxt_rows),
            "matched_coverage_rows": len(matched_rows),
            "matched_condition_token_pairs": len(matched_pairs),
            "target_condition_token_pairs": len(token_pairs),
            "pmxt_total_events": sum(int(row.get("pmxt_total_events") or 0) for row in pmxt_rows),
            "pmxt_book_events": sum(int(row.get("pmxt_book_events") or 0) for row in pmxt_rows),
            "pmxt_price_change_events": sum(int(row.get("pmxt_price_change_events") or 0) for row in pmxt_rows),
            "sample_rows": list(pmxt_rows[:20]),
        },
        execution_model={
            "model": "L2OrderFilledExecutionModel",
            "data_contract": {
                "lob": "PMXT book_snapshot/book + price_change replay by condition_id/token_id/hour",
                "fill_evidence": "OrderFilled TradeTick cache ordered by block_number, transaction_index, log_index, tx_hash",
                "execution": "DEPTH taker uses visible L2 depth with residual book; maker queue is OrderFilled-calibrated",
            },
            "profiles": {
                "conservative": l2_execution_config_to_audit_dict(L2ExecutionConfig(mode="conservative", queue_mode="trade_only", depth_haircut=Decimal("0.7"))),
                "realistic": l2_execution_config_to_audit_dict(L2ExecutionConfig(mode="realistic", queue_mode="reconciled", depth_haircut=Decimal("0.85"), cancel_ahead_fraction=Decimal("0.5"))),
                "optimistic": l2_execution_config_to_audit_dict(L2ExecutionConfig(mode="optimistic", queue_mode="optimistic", depth_haircut=Decimal("1.0"), cancel_ahead_fraction=Decimal("1.0"))),
            },
        },
        command_plan=[{"label": plan.label, "command": plan.command} for plan in command_plan],
        command_results=[asdict(result) for result in command_results],
        warnings=warnings,
        next_actions=next_actions,
    )


def local_pmxt_path(root: Path, hour: datetime) -> Path | None:
    filename = archive_filename_for_hour(hour)
    for candidate in (root / filename, root / f"{hour:%Y/%m/%d}" / filename):
        if candidate.exists():
            return candidate
    return None


def find_remote_pmxt(hour: datetime, *, remote_bases: Sequence[str], timeout_seconds: float) -> dict[str, Any] | None:
    filename = archive_filename_for_hour(hour)
    for base in remote_bases:
        url = f"{base.rstrip('/')}/{filename}"
        try:
            request = Request(url, method="HEAD", headers={"User-Agent": "prediction-market-quant/nba-l2-orderfilled"})
            with urlopen(request, timeout=timeout_seconds) as response:
                return {
                    "url": url,
                    "http_status": int(getattr(response, "status", 200)),
                    "content_length": int(response.headers.get("Content-Length") or 0),
                    "last_modified": response.headers.get("Last-Modified"),
                }
        except HTTPError as exc:
            if exc.code == 404:
                continue
        except (URLError, TimeoutError, OSError):
            continue
    return None


def download_pmxt_hour(root: Path, hour: datetime, url: str, *, timeout_seconds: float) -> Path:
    filename = archive_filename_for_hour(hour)
    path = root / f"{hour:%Y/%m/%d}" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    request = Request(url, method="GET", headers={"User-Agent": "prediction-market-quant/nba-l2-orderfilled"})
    with urlopen(request, timeout=timeout_seconds) as response, tmp.open("wb") as out:
        while chunk := response.read(1024 * 1024):
            out.write(chunk)
    tmp.replace(path)
    return path


def base_coverage_row(token: NbaToken, hour: datetime, status: str) -> dict[str, Any]:
    return {
        "hour": hour.isoformat(),
        "market_id": token.market_id,
        "market_slug": token.market_slug,
        "market_title": token.market_title,
        "condition_id": token.condition_id,
        "token_id": token.token_id,
        "token_side": token.token_side,
        "coverage_status": status,
        "pmxt_total_events": 0,
        "pmxt_book_events": 0,
        "pmxt_price_change_events": 0,
        "pmxt_last_trade_price_events": 0,
    }


def write_outputs(output_dir: Path, report: ExperimentReport) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = asdict(report)
    (output_dir / "nba_l2_orderfilled_first_month.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    (output_dir / "nba_l2_orderfilled_first_month.md").write_text(report_to_markdown(report), encoding="utf-8")


def report_to_markdown(report: ExperimentReport) -> str:
    lines = [
        f"# NBA L2 + OrderFilled First Month: {report.status}",
        "",
        f"- window: `{report.selected_window.get('start_hour')}` -> `{report.selected_window.get('end_hour')}`",
        f"- scanned_hours: `{report.selected_window.get('scanned_hour_count')}` / `{report.selected_window.get('hour_count')}`",
        f"- markets: `{report.universe.get('market_count')}`",
        f"- tokens: `{report.universe.get('token_count')}`",
        f"- orderfilled_trades: `{report.orderfilled_data.get('trade_count')}`",
        f"- pmxt_matched_rows: `{report.pmxt_l2_data.get('matched_coverage_rows')}` / `{report.pmxt_l2_data.get('coverage_rows')}`",
        f"- pmxt_matched_pairs: `{report.pmxt_l2_data.get('matched_condition_token_pairs')}` / `{report.pmxt_l2_data.get('target_condition_token_pairs')}`",
        f"- command_plan_count: `{len(report.command_plan)}`",
        f"- executed_command_count: `{len(report.command_results)}`",
        "",
        "## Markets",
        "",
        "| market_id | slug | trades | first_fill | last_fill |",
        "| --- | --- | ---: | --- | --- |",
    ]
    for market in report.universe.get("markets", [])[:20]:
        lines.append(
            f"| {market.get('market_id')} | `{market.get('market_slug')}` | {market.get('orderfilled_trades')} | {market.get('first_fill_ts')} | {market.get('last_fill_ts')} |"
        )
    lines.extend(["", "## PMXT", ""])
    for key, value in (report.pmxt_l2_data.get("hour_status_counts") or {}).items():
        lines.append(f"- {key}: `{value}`")
    if report.warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in report.warnings)
    if report.command_plan:
        lines.extend(["", "## Command Plan", ""])
        for item in report.command_plan[:20]:
            lines.append(f"- {item['label']}: `{' '.join(item['command'])}`")
    if report.command_results:
        lines.extend(["", "## Command Results", ""])
        for item in report.command_results:
            lines.append(f"- {item['label']}: rc={item['returncode']} duration={item['duration_seconds']}s")
    lines.extend(["", "## Next Actions", ""])
    lines.extend(f"- {action}" for action in report.next_actions)
    return "\n".join(lines) + "\n"


def empty_report(args: argparse.Namespace, status: str, warning: str) -> ExperimentReport:
    return ExperimentReport(
        schema_version="nba_l2_orderfilled_first_month_v1",
        status=status,
        parameters={"pmxt_root": str(args.pmxt_root)},
        selected_window={},
        universe={"market_count": 0, "token_count": 0, "markets": [], "tokens": []},
        orderfilled_data={},
        pmxt_l2_data={},
        execution_model={},
        warnings=[warning],
    )


def ensure_utc_hour(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def coverage_status(row: Mapping[str, Any]) -> str:
    return str(row.get("coverage_status") or row.get("status") or "")


def hour_arg(value: datetime) -> str:
    return ensure_utc_hour(value).strftime("%Y-%m-%dT%H")


def iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return str(value)


def counts(values: Iterable[Any]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = str(value)
        result[key] = result.get(key, 0) + 1
    return result


def tail(text: str, *, max_chars: int = 4000) -> str:
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


if __name__ == "__main__":
    raise SystemExit(main())
