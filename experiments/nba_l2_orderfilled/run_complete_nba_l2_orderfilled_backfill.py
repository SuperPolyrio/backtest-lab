#!/usr/bin/env python3
"""Build complete NBA single-game matchup L2 + OrderFilled replay data.

This is the heavy, lifecycle-oriented pipeline:

* market window: created_at hour -> end_date hour for each selected market;
* LOB: PMXT hourly raw parquet filtered to the market condition/token pair and
  materialized as full book_snapshot + price_change replay events;
* fills: raw OrderFilled tick replay materialized in deterministic block/log
  order for the same market tokens;
* output: a coverage report showing whether each market now has enough L2 and
  OrderFilled data to drive the fill-first + DEPTH execution model.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from experiments.nba_l2_orderfilled.run_nba_l2_orderfilled_backtest import (
    DEFAULT_PMXT_ROOT,
    DEFAULT_REMOTE_BASE_URLS,
    NbaMarket,
    NbaToken,
    all_hours,
    counts,
    download_pmxt_hour,
    find_remote_pmxt,
    hour_arg,
    load_market_tokens,
    local_pmxt_path,
    prepare_pmxt_hours,
)
from quant.backtest.pmxt_l2_event_replay_store import (
    PmxtL2EventReplayBackfillResult,
    PmxtL2ReplaySelection,
    backfill_pmxt_l2_event_replay,
    load_pmxt_l2_event_coverage,
)
from quant.backtest.runners.trade_replay_store import (
    backfill_orderfilled_trade_replay,
    load_orderfilled_trade_replay_coverage,
)
from quant.core.db import postgres_connection
from scripts.validate_pmxt_l2_raw import parse_hour


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime_outputs" / "nba_l2_orderfilled"
DEFAULT_MARKET_SLUGS = (
    "nba-lal-det-2026-03-23",
    "nba-gsw-dal-2026-03-23",
    "nba-sas-mia-2026-03-23",
    "nba-tor-phx-2026-03-22",
    "nba-bkn-sac-2026-03-22",
)


@dataclass(frozen=True)
class MarketLifecycle:
    market: NbaMarket
    start_hour: str
    end_hour: str
    expected_hours: int
    selected_hours: int
    capped_by_max_hours: bool


@dataclass
class CompleteMarketReport:
    lifecycle: dict[str, Any]
    tokens: list[dict[str, Any]]
    pmxt_hours: dict[str, Any]
    pmxt_l2_replay: dict[str, Any]
    orderfilled_replay: dict[str, Any]
    status: str
    warnings: list[str] = field(default_factory=list)


@dataclass
class CompletePipelineReport:
    schema_version: str
    status: str
    parameters: dict[str, Any]
    market_count: int
    token_count: int
    markets: list[dict[str, Any]]
    aggregate: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    commands: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PmxtHourPreparationSummary:
    statuses: list[dict[str, Any]]

    @property
    def status_counts(self) -> dict[str, int]:
        return dict(counts(row.get("status") for row in self.statuses))


def main() -> int:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    slugs = tuple(args.market_slug or DEFAULT_MARKET_SLUGS)
    if args.download_remote and proxy_env_detected() and not args.allow_proxy_download:
        proxy_keys = ", ".join(sorted(proxy_env_detected()))
        raise SystemExit(
            "Refusing PMXT remote download because proxy environment variables are set "
            f"({proxy_keys}). PMXT hourly parquet files are large and can exhaust Clash/VPS "
            "bandwidth. Unset proxy env vars for direct download, or pass --allow-proxy-download "
            "only when you intentionally want this traffic to use the proxy."
        )

    with postgres_connection(readonly=not bool(args.backfill_orderfilled)) as conn:
        markets = load_markets_by_slug(conn, slugs)
        lifecycles = [lifecycle_for_market(market, max_hours=max(0, int(args.max_hours))) for market in markets]
        unique_hours = unique_lifecycle_hours(lifecycles)
        if args.download_remote and unique_hours:
            summary = prepare_unique_pmxt_hours(
                Path(args.pmxt_root),
                unique_hours,
                remote_bases=tuple(args.remote_base_url or DEFAULT_REMOTE_BASE_URLS),
                timeout_seconds=max(1.0, float(args.remote_timeout_seconds)),
                workers=max(1, int(args.download_workers)),
                progress=bool(args.progress),
            )
            if args.progress:
                print(
                    f"[complete-nba] unique_pmxt_hours={len(unique_hours)} "
                    f"status_counts={summary.status_counts}",
                    flush=True,
                )
        market_reports: list[CompleteMarketReport] = []
        for market, lifecycle in zip(markets, lifecycles, strict=True):
            start_hour = parse_hour(lifecycle.start_hour)
            end_hour = parse_hour(lifecycle.end_hour)
            if start_hour is None or end_hour is None:
                if args.progress:
                    print(f"[complete-nba] skip market={market.market_slug} missing lifecycle window", flush=True)
                continue
            if args.progress:
                print(
                    f"[complete-nba] market={market.market_slug} window={lifecycle.start_hour}->{lifecycle.end_hour} "
                    f"hours={lifecycle.selected_hours}/{lifecycle.expected_hours}",
                    flush=True,
                )
            tokens = load_market_tokens(conn, [market], start_hour=start_hour, end_hour=end_hour)
            hours = all_hours(start_hour, end_hour)
            if args.max_hours:
                hours = hours[: int(args.max_hours)]
            hour_statuses = prepare_pmxt_hours(
                Path(args.pmxt_root),
                hours,
                download_remote=False,
                remote_bases=tuple(args.remote_base_url or DEFAULT_REMOTE_BASE_URLS),
                timeout_seconds=max(1.0, float(args.remote_timeout_seconds)),
            )
            if args.progress:
                print(f"[complete-nba] market={market.market_slug} pmxt_hours={dict(counts(row.get('status') for row in hour_statuses))}", flush=True)
            selections = selections_from_tokens(tokens)
            l2_result: PmxtL2EventReplayBackfillResult | None = None
            l2_coverage: list[dict[str, Any]] = []
            if args.write_l2_events and selections:
                if args.progress:
                    print(f"[complete-nba] market={market.market_slug} writing_l2_events tokens={len(selections)}", flush=True)
                l2_result = backfill_pmxt_l2_event_replay(
                    Path(args.pmxt_root),
                    selections,
                    start_hour=start_hour,
                    end_hour=end_hour,
                    batch_size=max(1, int(args.pmxt_batch_size)),
                    insert_batch_rows=max(1, int(args.insert_batch_rows)),
                    build_tag=str(args.build_tag),
                )
                l2_coverage = load_pmxt_l2_event_coverage(selections, from_hour=start_hour, to_hour=end_hour)
                if args.progress:
                    print(f"[complete-nba] market={market.market_slug} l2_inserted_rows={l2_result.inserted_rows}", flush=True)

            orderfilled_payload: dict[str, Any] = {}
            if args.backfill_orderfilled:
                if args.progress:
                    print(f"[complete-nba] market={market.market_slug} writing_orderfilled_replay", flush=True)
                orderfilled_payload = backfill_orderfilled_for_market(tokens, build_tag=str(args.build_tag))
                if args.progress:
                    result = orderfilled_payload.get("result") or {}
                    print(
                        f"[complete-nba] market={market.market_slug} orderfilled_status={orderfilled_payload.get('status')} "
                        f"inserted_rows={result.get('inserted_rows')}",
                        flush=True,
                    )
            elif tokens:
                orderfilled_payload = load_existing_orderfilled_coverage(tokens)

            market_reports.append(
                build_market_report(
                    lifecycle,
                    tokens=tokens,
                    hour_statuses=hour_statuses,
                    l2_result=l2_result,
                    l2_coverage=l2_coverage,
                    orderfilled_payload=orderfilled_payload,
                    write_l2_events=bool(args.write_l2_events),
                    backfill_orderfilled=bool(args.backfill_orderfilled),
                )
            )

    report = build_report(args, markets=market_reports)
    write_outputs(output_dir, report)
    if args.format == "json":
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str))
    else:
        print(report_to_markdown(report))
    return 0 if report.status in {"ready", "review"} else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-slug", action="append", default=[])
    parser.add_argument("--pmxt-root", type=Path, default=DEFAULT_PMXT_ROOT)
    parser.add_argument("--download-remote", action="store_true")
    parser.add_argument("--allow-proxy-download", action="store_true", help="Allow large PMXT downloads even when proxy env vars are set.")
    parser.add_argument("--remote-base-url", action="append", default=[])
    parser.add_argument("--remote-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--download-workers", type=int, default=4, help="Concurrent PMXT hour downloads for full lifecycle runs.")
    parser.add_argument("--write-l2-events", action="store_true")
    parser.add_argument("--backfill-orderfilled", action="store_true")
    parser.add_argument("--max-hours", type=int, default=0, help="0 means full created_at -> end_date lifecycle.")
    parser.add_argument("--pmxt-batch-size", type=int, default=250_000)
    parser.add_argument("--insert-batch-rows", type=int, default=50_000)
    parser.add_argument("--build-tag", default="complete_nba_l2_orderfilled")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    parser.add_argument("--progress", action="store_true", help="Print market-level progress while downloading/materializing.")
    return parser


def proxy_env_detected() -> dict[str, str]:
    keys = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
    return {key: value for key in keys if (value := os.environ.get(key))}


def unique_lifecycle_hours(lifecycles: Sequence[MarketLifecycle]) -> list[datetime]:
    seen: set[datetime] = set()
    hours: list[datetime] = []
    for lifecycle in lifecycles:
        start = parse_hour(lifecycle.start_hour)
        end = parse_hour(lifecycle.end_hour)
        if start is None or end is None:
            continue
        for hour in all_hours(start, end):
            normalized = hour.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
            if normalized in seen:
                continue
            seen.add(normalized)
            hours.append(normalized)
    return sorted(hours)


def prepare_unique_pmxt_hours(
    pmxt_root: Path,
    hours: Sequence[datetime],
    *,
    remote_bases: Sequence[str],
    timeout_seconds: float,
    workers: int,
    progress: bool = False,
) -> PmxtHourPreparationSummary:
    """Download missing PMXT hour files once for all selected markets.

    The complete NBA pipeline has overlapping market lifecycles. Preparing the
    union first avoids repeatedly downloading or HEAD-checking the same large
    hourly parquet files inside each per-market pass.
    """

    statuses: list[dict[str, Any]] = []
    missing_hours: list[datetime] = []
    for hour in sorted({hour.astimezone(UTC).replace(minute=0, second=0, microsecond=0) for hour in hours}):
        local = local_pmxt_path(pmxt_root, hour)
        if local is not None:
            statuses.append({"hour": hour.isoformat(), "status": "local", "path": str(local), "bytes": local.stat().st_size})
            continue
        missing_hours.append(hour)

    if progress:
        print(
            f"[complete-nba] pmxt_prepare local={len(statuses)} missing={len(missing_hours)} workers={workers}",
            flush=True,
        )

    if not missing_hours:
        return PmxtHourPreparationSummary(statuses=sorted(statuses, key=lambda row: str(row.get("hour"))))

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        futures = {
            executor.submit(
                prepare_one_pmxt_hour,
                pmxt_root,
                hour,
                remote_bases=remote_bases,
                timeout_seconds=timeout_seconds,
                progress=progress,
            ): hour
            for hour in missing_hours
        }
        completed = 0
        for future in as_completed(futures):
            completed += 1
            hour = futures[future]
            try:
                row = future.result()
            except Exception as exc:  # pragma: no cover - defensive status reporting for long data jobs
                row = {"hour": hour.isoformat(), "status": "download_error", "error": f"{type(exc).__name__}: {exc}"}
            statuses.append(row)
            if progress:
                print(
                    f"[complete-nba] pmxt_prepare_done {completed}/{len(missing_hours)} "
                    f"hour={hour_arg(hour)} status={row.get('status')}",
                    flush=True,
                )

    return PmxtHourPreparationSummary(statuses=sorted(statuses, key=lambda row: str(row.get("hour"))))


def prepare_one_pmxt_hour(
    pmxt_root: Path,
    hour: datetime,
    *,
    remote_bases: Sequence[str],
    timeout_seconds: float,
    progress: bool = False,
) -> dict[str, Any]:
    local = local_pmxt_path(pmxt_root, hour)
    if local is not None:
        return {"hour": hour.isoformat(), "status": "local", "path": str(local), "bytes": local.stat().st_size}
    remote = find_remote_pmxt(hour, remote_bases=remote_bases, timeout_seconds=timeout_seconds)
    if remote is None:
        return {"hour": hour.isoformat(), "status": "missing_remote"}
    if progress:
        size = int(remote.get("content_length") or 0)
        print(f"[complete-nba] pmxt_download_start hour={hour_arg(hour)} bytes={size}", flush=True)
    path = download_pmxt_hour(pmxt_root, hour, str(remote["url"]), timeout_seconds=timeout_seconds)
    return {"hour": hour.isoformat(), "status": "downloaded", "path": str(path), "bytes": path.stat().st_size, **remote}


def load_markets_by_slug(conn: Any, slugs: Sequence[str]) -> list[NbaMarket]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH selected AS (
                SELECT DISTINCT market_id, market_slug, market_title, condition_id, created_at, end_date
                FROM quant.market_token_metadata
                WHERE market_slug = ANY(%(slugs)s)
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
                WHERE COALESCE(b.trade_count, 0) > 0
                GROUP BY b.market_id
            )
            SELECT
                s.market_id, s.market_slug, s.market_title, s.condition_id, s.created_at, s.end_date,
                f.first_fill_ts, f.last_fill_ts, COALESCE(f.trades, 0) AS trades,
                COALESCE(f.block_rows, 0) AS block_rows, f.from_block, f.to_block
            FROM selected s
            LEFT JOIN fill_stats f ON f.market_id = s.market_id
            ORDER BY array_position(%(slugs)s::text[], s.market_slug)
            """,
            {"slugs": list(slugs)},
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


def lifecycle_for_market(market: NbaMarket, *, max_hours: int = 0) -> MarketLifecycle:
    start = parse_hour(market.created_at) or parse_hour(market.first_fill_ts)
    end = parse_hour(market.end_date) or parse_hour(market.last_fill_ts)
    if start is None or end is None:
        raise ValueError(f"market {market.market_slug} is missing both created/end and fill timestamps")
    expected = len(all_hours(start, end))
    selected = min(expected, max_hours) if max_hours else expected
    capped_end = all_hours(start, end)[selected - 1] if selected else end
    return MarketLifecycle(
        market=market,
        start_hour=hour_arg(start),
        end_hour=hour_arg(capped_end),
        expected_hours=expected,
        selected_hours=selected,
        capped_by_max_hours=bool(max_hours and selected < expected),
    )


def selections_from_tokens(tokens: Sequence[NbaToken]) -> list[PmxtL2ReplaySelection]:
    return [
        PmxtL2ReplaySelection(
            market_id=token.market_id,
            condition_id=token.condition_id,
            token_id=token.token_id,
            token_side=token.token_side,
            market_slug=token.market_slug,
            market_title=token.market_title,
        )
        for token in tokens
        if token.token_id and token.condition_id
    ]


def backfill_orderfilled_for_market(tokens: Sequence[NbaToken], *, build_tag: str) -> dict[str, Any]:
    pairs = [
        (token.market_id, orderfilled_replay_token_id(token))
        for token in tokens
        if token.first_block is not None and token.last_block is not None and orderfilled_replay_token_id(token)
    ]
    if not pairs:
        return {"status": "missing", "reason": "no token block range"}
    from_block = min(int(token.first_block) for token in tokens if token.first_block is not None)
    to_block = max(int(token.last_block) for token in tokens if token.last_block is not None)
    result = backfill_orderfilled_trade_replay(pairs, from_block=from_block, to_block=to_block, build_tag=build_tag)
    coverage = load_orderfilled_trade_replay_coverage(pairs, from_block=from_block, to_block=to_block)
    return {"status": "ready" if coverage else "review", "result": result.as_dict(), "coverage": coverage}


def load_existing_orderfilled_coverage(tokens: Sequence[NbaToken]) -> dict[str, Any]:
    pairs = [
        (token.market_id, orderfilled_replay_token_id(token))
        for token in tokens
        if token.first_block is not None and token.last_block is not None and orderfilled_replay_token_id(token)
    ]
    if not pairs:
        return {"status": "missing", "reason": "no token block range"}
    from_block = min(int(token.first_block) for token in tokens if token.first_block is not None)
    to_block = max(int(token.last_block) for token in tokens if token.last_block is not None)
    coverage = load_orderfilled_trade_replay_coverage(pairs, from_block=from_block, to_block=to_block)
    return {"status": "ready" if coverage else "not_materialized", "coverage": coverage, "from_block": from_block, "to_block": to_block}


def orderfilled_replay_token_id(token: NbaToken) -> str:
    return str(token.token_id_hex or token.token_id or "").strip().lower()


def build_market_report(
    lifecycle: MarketLifecycle,
    *,
    tokens: Sequence[NbaToken],
    hour_statuses: Sequence[Mapping[str, Any]],
    l2_result: PmxtL2EventReplayBackfillResult | None,
    l2_coverage: Sequence[Mapping[str, Any]],
    orderfilled_payload: Mapping[str, Any],
    write_l2_events: bool,
    backfill_orderfilled: bool,
) -> CompleteMarketReport:
    status_counts = counts(row.get("status") for row in hour_statuses)
    warnings: list[str] = []
    if lifecycle.capped_by_max_hours:
        warnings.append("window capped by --max-hours; this is not a complete lifecycle run")
    if status_counts.get("remote_available", 0) and not any(row.get("status") == "downloaded" for row in hour_statuses):
        warnings.append("PMXT hours are remote_available but not local; rerun with --download-remote before writing full L2 replay")
    if write_l2_events and (l2_result is None or l2_result.inserted_rows <= 0):
        warnings.append("L2 replay write requested but no PMXT L2 rows were inserted")
    if backfill_orderfilled and orderfilled_payload.get("status") != "ready":
        warnings.append("OrderFilled replay backfill did not produce ready coverage")
    status = "ready"
    if warnings:
        status = "review"
    if not tokens:
        status = "missing"
        warnings.append("no tokens resolved for market")
    return CompleteMarketReport(
        lifecycle={
            **asdict(lifecycle),
            "market": asdict(lifecycle.market),
        },
        tokens=[asdict(token) for token in tokens],
        pmxt_hours={
            "status_counts": status_counts,
            "expected_hours": lifecycle.expected_hours,
            "selected_hours": lifecycle.selected_hours,
            "sample": list(hour_statuses[:10]),
        },
        pmxt_l2_replay={
            "write_requested": write_l2_events,
            "backfill_result": l2_result.as_dict() if l2_result else None,
            "coverage": list(l2_coverage),
        },
        orderfilled_replay={
            "backfill_requested": backfill_orderfilled,
            **dict(orderfilled_payload),
        },
        status=status,
        warnings=warnings,
    )


def build_report(args: argparse.Namespace, *, markets: Sequence[CompleteMarketReport]) -> CompletePipelineReport:
    warnings = [warning for market in markets for warning in market.warnings]
    status = "ready" if markets and not warnings else "review"
    if not markets:
        status = "missing"
        warnings.append("no selected NBA markets were found")
    aggregate = {
        "ready_markets": sum(1 for market in markets if market.status == "ready"),
        "review_markets": sum(1 for market in markets if market.status == "review"),
        "missing_markets": sum(1 for market in markets if market.status == "missing"),
        "tokens": sum(len(market.tokens) for market in markets),
        "expected_hours": sum(int(market.pmxt_hours.get("expected_hours") or 0) for market in markets),
        "selected_hours": sum(int(market.pmxt_hours.get("selected_hours") or 0) for market in markets),
        "l2_inserted_rows": sum(int(((market.pmxt_l2_replay.get("backfill_result") or {}).get("inserted_rows") or 0)) for market in markets),
        "orderfilled_trade_count": sum(sum(int(token.get("trade_count") or 0) for token in market.tokens) for market in markets),
    }
    return CompletePipelineReport(
        schema_version="complete_nba_l2_orderfilled_v1",
        status=status,
        parameters={
            "market_slug": args.market_slug or list(DEFAULT_MARKET_SLUGS),
            "pmxt_root": str(args.pmxt_root),
            "download_remote": bool(args.download_remote),
            "write_l2_events": bool(args.write_l2_events),
            "backfill_orderfilled": bool(args.backfill_orderfilled),
            "max_hours": int(args.max_hours),
            "build_tag": str(args.build_tag),
        },
        market_count=len(markets),
        token_count=aggregate["tokens"],
        markets=[asdict(market) for market in markets],
        aggregate=aggregate,
        warnings=warnings,
        commands={
            "materialize_existing_pmxt": (
                "experiments/nba_l2_orderfilled/run_materialize_single_game_nba_execution_replay.sh"
            ),
            "inspect_only": (
                "conda run --no-capture-output -n polyBots python "
                "experiments/nba_l2_orderfilled/run_complete_nba_l2_orderfilled_backfill.py"
            ),
        },
    )


def write_outputs(output_dir: Path, report: CompletePipelineReport) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "nba_l2_orderfilled_complete.json").write_text(
        json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    (output_dir / "nba_l2_orderfilled_complete.md").write_text(report_to_markdown(report), encoding="utf-8")


def report_to_markdown(report: CompletePipelineReport) -> str:
    lines = [
        f"# Complete NBA L2 + OrderFilled Backfill: {report.status}",
        "",
        f"- markets: `{report.market_count}`",
        f"- tokens: `{report.token_count}`",
        f"- expected_hours: `{report.aggregate.get('expected_hours')}`",
        f"- selected_hours: `{report.aggregate.get('selected_hours')}`",
        f"- l2_inserted_rows: `{report.aggregate.get('l2_inserted_rows')}`",
        f"- orderfilled_trade_count: `{report.aggregate.get('orderfilled_trade_count')}`",
        "",
        "## Markets",
        "",
        "| market | lifecycle | tokens | pmxt hours | l2 rows | orderfilled | status |",
        "| --- | --- | ---: | --- | ---: | --- | --- |",
    ]
    for item in report.markets:
        lifecycle = item.get("lifecycle") or {}
        market = (lifecycle.get("market") or {})
        pmxt_counts = (item.get("pmxt_hours") or {}).get("status_counts") or {}
        l2_result = ((item.get("pmxt_l2_replay") or {}).get("backfill_result") or {})
        orderfilled = item.get("orderfilled_replay") or {}
        lines.append(
            "| "
            f"`{market.get('market_slug')}` | "
            f"`{lifecycle.get('start_hour')}` -> `{lifecycle.get('end_hour')}` | "
            f"{len(item.get('tokens') or [])} | "
            f"{pmxt_counts} | "
            f"{l2_result.get('inserted_rows', 0)} | "
            f"{orderfilled.get('status')} | "
            f"{item.get('status')} |"
        )
    if report.warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in report.warnings[:40])
    lines.extend(["", "## Commands", ""])
    for key, command in report.commands.items():
        lines.append(f"- {key}: `{command}`")
    return "\n".join(lines) + "\n"


def iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat()
    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
