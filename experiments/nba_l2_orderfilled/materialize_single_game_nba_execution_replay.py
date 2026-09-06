#!/usr/bin/env python3
"""Materialize single-game NBA PMXT L2 and OrderFilled execution replay.

This is the replay-building entrypoint for the five selected Team A vs Team B
NBA markets.  It does not download PMXT data.  It only consumes local PMXT raw
parquet files and database OrderFilled facts, then writes:

* PMXT book/book_snapshot + price_change -> pmxt_l2_event_replay
* OrderFilled fact ticks -> orderfilled_trade_replay
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from typing import Any, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from experiments.nba_l2_orderfilled.run_complete_nba_l2_orderfilled_backfill import (
    DEFAULT_MARKET_SLUGS,
    DEFAULT_OUTPUT_DIR,
    backfill_orderfilled_for_market,
    lifecycle_for_market,
    load_markets_by_slug,
    orderfilled_replay_token_id,
    selections_from_tokens,
    unique_lifecycle_hours,
)
from experiments.nba_l2_orderfilled.run_nba_l2_orderfilled_backtest import (
    DEFAULT_PMXT_ROOT,
    NbaMarket,
    NbaToken,
    all_hours,
    load_market_tokens,
    local_pmxt_path,
)
from quant.backtest.pmxt_l2_event_replay_store import (
    PmxtL2EventReplayBackfillResult,
    backfill_pmxt_l2_event_replay,
    load_pmxt_l2_event_coverage,
)
from quant.core.db import ClickHouseClient, postgres_connection
from scripts.validate_pmxt_l2_raw import parse_hour


@dataclass(frozen=True)
class MarketMaterialization:
    market: dict[str, Any]
    lifecycle: dict[str, Any]
    tokens: list[dict[str, Any]]
    local_pmxt_hours: int
    missing_pmxt_hours: int
    orderfilled_replay: dict[str, Any]
    status: str
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class MaterializationReport:
    schema_version: str
    status: str
    parameters: dict[str, Any]
    pmxt_l2_replay: dict[str, Any]
    markets: list[dict[str, Any]]
    aggregate: dict[str, Any]
    warnings: list[str] = field(default_factory=list)


def main() -> int:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    slugs = tuple(args.market_slug or DEFAULT_MARKET_SLUGS)

    with postgres_connection(readonly=True) as conn:
        markets = load_markets_by_slug(conn, slugs)
        lifecycles = [lifecycle_for_market(market, max_hours=max(0, int(args.max_hours))) for market in markets]
        market_tokens: dict[int, list[NbaToken]] = {}
        for market, lifecycle in zip(markets, lifecycles, strict=True):
            start = _required_hour(lifecycle.start_hour, market.market_slug)
            end = _required_hour(lifecycle.end_hour, market.market_slug)
            market_tokens[market.market_id] = load_market_tokens(conn, [market], start_hour=start, end_hour=end)

    selected_hours = unique_lifecycle_hours(lifecycles)
    if args.max_hours:
        selected_hours = selected_hours[: int(args.max_hours)]
    if not selected_hours:
        report = MaterializationReport(
            schema_version="single_game_nba_execution_replay_v1",
            status="missing",
            parameters=_parameters(args, slugs),
            pmxt_l2_replay={"status": "missing", "reason": "no lifecycle hours"},
            markets=[],
            aggregate={},
            warnings=["no lifecycle hours resolved"],
        )
        write_outputs(output_dir, report)
        print(report_to_markdown(report))
        return 2

    local_hours, missing_hours = split_local_pmxt_hours(Path(args.pmxt_root), selected_hours)
    if args.progress:
        print(
            f"[nba-replay] markets={len(markets)} tokens={sum(len(v) for v in market_tokens.values())} "
            f"unique_hours={len(selected_hours)} local_hours={len(local_hours)} missing_hours={len(missing_hours)}",
            flush=True,
        )

    all_tokens = [token for tokens in market_tokens.values() for token in tokens]
    selections = selections_from_tokens(all_tokens)
    l2_result: PmxtL2EventReplayBackfillResult | None = None
    l2_coverage: list[dict[str, Any]] = []
    if selections and local_hours:
        if args.progress:
            print(
                f"[nba-replay] writing_pmxt_l2_event_replay selections={len(selections)} "
                f"window={selected_hours[0].isoformat()}->{selected_hours[-1].isoformat()}",
                flush=True,
            )
        l2_result = backfill_pmxt_l2_event_replay(
            Path(args.pmxt_root),
            selections,
            start_hour=selected_hours[0],
            end_hour=selected_hours[-1],
            batch_size=max(1, int(args.pmxt_batch_size)),
            insert_batch_rows=max(1, int(args.insert_batch_rows)),
            build_tag=str(args.build_tag),
        )
        l2_coverage = load_pmxt_l2_event_coverage(selections, from_hour=selected_hours[0], to_hour=selected_hours[-1])
        if args.progress:
            print(
                f"[nba-replay] pmxt_l2_inserted_rows={l2_result.inserted_rows} "
                f"snapshots={l2_result.snapshot_events} price_changes={l2_result.price_change_events}",
                flush=True,
            )

    market_reports: list[MarketMaterialization] = []
    for market, lifecycle in zip(markets, lifecycles, strict=True):
        tokens = market_tokens.get(market.market_id, [])
        warnings: list[str] = []
        start = _required_hour(lifecycle.start_hour, market.market_slug)
        end = _required_hour(lifecycle.end_hour, market.market_slug)
        market_hours = all_hours(start, end)
        if args.max_hours:
            market_hours = market_hours[: int(args.max_hours)]
        market_local_hours, market_missing_hours = split_local_pmxt_hours(Path(args.pmxt_root), market_hours)
        if market_missing_hours:
            warnings.append(f"missing local PMXT hours: {len(market_missing_hours)}")
        if args.progress:
            print(f"[nba-replay] writing_orderfilled_trade_replay market={market.market_slug}", flush=True)
        orderfilled = backfill_orderfilled_for_market(tokens, build_tag=str(args.build_tag))
        if orderfilled.get("status") != "ready":
            warnings.append(str(orderfilled.get("reason") or "OrderFilled replay coverage not ready"))
        market_reports.append(
            MarketMaterialization(
                market=asdict(market),
                lifecycle=asdict(lifecycle),
                tokens=[asdict(token) | {"orderfilled_replay_token_id": orderfilled_replay_token_id(token)} for token in tokens],
                local_pmxt_hours=len(market_local_hours),
                missing_pmxt_hours=len(market_missing_hours),
                orderfilled_replay=orderfilled,
                status="ready" if not warnings else "review",
                warnings=warnings,
            )
        )

    report = build_report(
        args,
        slugs=slugs,
        selected_hours=selected_hours,
        local_hours=local_hours,
        missing_hours=missing_hours,
        l2_result=l2_result,
        l2_coverage=l2_coverage,
        markets=market_reports,
    )
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
    parser.add_argument("--max-hours", type=int, default=0, help="0 means full selected market lifecycle union.")
    parser.add_argument("--pmxt-batch-size", type=int, default=250_000)
    parser.add_argument("--insert-batch-rows", type=int, default=50_000)
    parser.add_argument("--build-tag", default="single_game_nba_execution_replay")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    parser.add_argument("--progress", action="store_true")
    return parser


def split_local_pmxt_hours(pmxt_root: Path, hours: Sequence[datetime]) -> tuple[list[datetime], list[datetime]]:
    local: list[datetime] = []
    missing: list[datetime] = []
    for hour in hours:
        if local_pmxt_path(pmxt_root, hour) is None:
            missing.append(hour)
        else:
            local.append(hour)
    return local, missing


def build_report(
    args: argparse.Namespace,
    *,
    slugs: Sequence[str],
    selected_hours: Sequence[datetime],
    local_hours: Sequence[datetime],
    missing_hours: Sequence[datetime],
    l2_result: PmxtL2EventReplayBackfillResult | None,
    l2_coverage: Sequence[dict[str, Any]],
    markets: Sequence[MarketMaterialization],
) -> MaterializationReport:
    warnings = [warning for market in markets for warning in market.warnings]
    if missing_hours:
        warnings.append(f"missing local PMXT hours: {len(missing_hours)}")
    if l2_result is None:
        warnings.append("PMXT L2 replay was not written")
    elif l2_result.inserted_rows <= 0:
        warnings.append("PMXT L2 replay inserted zero rows")
    status = "ready" if markets and not warnings else "review"
    if not markets:
        status = "missing"
        warnings.append("no markets materialized")
    return MaterializationReport(
        schema_version="single_game_nba_execution_replay_v1",
        status=status,
        parameters={
            **_parameters(args, slugs),
            "from_hour": selected_hours[0].isoformat() if selected_hours else None,
            "to_hour": selected_hours[-1].isoformat() if selected_hours else None,
        },
        pmxt_l2_replay={
            "status": "ready" if l2_result and l2_result.inserted_rows > 0 else "review",
            "backfill_result": l2_result.as_dict() if l2_result else None,
            "coverage_rows": list(l2_coverage),
            "unique_hours": len(selected_hours),
            "local_hours": len(local_hours),
            "missing_hours": len(missing_hours),
            "missing_hour_sample": [hour.isoformat() for hour in missing_hours[:20]],
        },
        markets=[asdict(market) for market in markets],
        aggregate={
            "markets": len(markets),
            "tokens": sum(len(market.tokens) for market in markets),
            "unique_pmxt_hours": len(selected_hours),
            "local_pmxt_hours": len(local_hours),
            "missing_pmxt_hours": len(missing_hours),
            "pmxt_l2_inserted_rows": int(l2_result.inserted_rows if l2_result else 0),
            "pmxt_l2_snapshot_events": int(l2_result.snapshot_events if l2_result else 0),
            "pmxt_l2_price_change_events": int(l2_result.price_change_events if l2_result else 0),
            "orderfilled_inserted_rows": sum(
                int(((market.orderfilled_replay.get("result") or {}).get("inserted_rows") or 0)) for market in markets
            ),
            "orderfilled_after_rows": sum(
                int(((market.orderfilled_replay.get("result") or {}).get("after_rows") or 0)) for market in markets
            ),
        },
        warnings=warnings,
    )


def report_to_markdown(report: MaterializationReport) -> str:
    lines = [
        f"# Single-Game NBA Execution Replay Materialization: {report.status}",
        "",
        f"- markets: `{report.aggregate.get('markets')}`",
        f"- tokens: `{report.aggregate.get('tokens')}`",
        f"- unique_pmxt_hours: `{report.aggregate.get('unique_pmxt_hours')}`",
        f"- local_pmxt_hours: `{report.aggregate.get('local_pmxt_hours')}`",
        f"- pmxt_l2_inserted_rows: `{report.aggregate.get('pmxt_l2_inserted_rows')}`",
        f"- orderfilled_after_rows: `{report.aggregate.get('orderfilled_after_rows')}`",
        "",
        "## Markets",
        "",
        "| market | lifecycle | tokens | PMXT local/missing | OrderFilled rows | status |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for market in report.markets:
        raw_market = market.get("market") or {}
        lifecycle = market.get("lifecycle") or {}
        orderfilled = market.get("orderfilled_replay") or {}
        result = orderfilled.get("result") or {}
        lines.append(
            "| "
            f"`{raw_market.get('market_slug')}` | "
            f"`{lifecycle.get('start_hour')}` -> `{lifecycle.get('end_hour')}` | "
            f"{len(market.get('tokens') or [])} | "
            f"{market.get('local_pmxt_hours')}/{market.get('missing_pmxt_hours')} | "
            f"{result.get('after_rows', 0)} | "
            f"{market.get('status')} |"
        )
    if report.warnings:
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in report.warnings[:40])
    return "\n".join(lines) + "\n"


def write_outputs(output_dir: Path, report: MaterializationReport) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "single_game_nba_execution_replay.json").write_text(
        json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    (output_dir / "single_game_nba_execution_replay.md").write_text(report_to_markdown(report), encoding="utf-8")


def _parameters(args: argparse.Namespace, slugs: Sequence[str]) -> dict[str, Any]:
    return {
        "market_slug": list(slugs),
        "pmxt_root": str(args.pmxt_root),
        "max_hours": int(args.max_hours),
        "build_tag": str(args.build_tag),
        "download_remote": False,
    }


def _required_hour(value: str | None, market_slug: str) -> datetime:
    parsed = parse_hour(value)
    if parsed is None:
        raise ValueError(f"market {market_slug} is missing a valid lifecycle hour: {value}")
    return parsed.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


if __name__ == "__main__":
    raise SystemExit(main())
