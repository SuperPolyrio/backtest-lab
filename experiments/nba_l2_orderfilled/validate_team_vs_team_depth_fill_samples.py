#!/usr/bin/env python3
"""Validate real OrderFilled samples against materialized PMXT L2 books.

The historical execution check is deliberately concrete:

* pick real OrderFilled rows from the selected team-vs-team main markets;
* map OrderFilled token ids to PMXT L2 asset ids;
* rebuild the latest L2 book before each fill from snapshot + deltas;
* submit a same-side, same-price, same-size taker intent to the current
  L2OrderFilledExecutionModel;
* compare simulated state/price/size against the historical fill.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from experiments.nba_l2_orderfilled.build_single_game_nba_l2_fill_coverage_report import build_token_mappings
from experiments.nba_l2_orderfilled.materialize_single_game_nba_execution_replay import _required_hour
from experiments.nba_l2_orderfilled.run_complete_nba_l2_orderfilled_backfill import lifecycle_for_market, load_markets_by_slug
from experiments.nba_l2_orderfilled.run_nba_l2_orderfilled_backtest import load_market_tokens
from quant.backtest.l2_orderfilled_execution import (
    BookDelta,
    BookLevel,
    BookSnapshot,
    L2ExecutionConfig,
    L2OrderFilledExecutionModel,
    StrategyOrderIntent,
)
from quant.core.db import ClickHouseClient, postgres_connection


DEFAULT_MARKET_SLUGS = (
    "fifwc-arg-aut-2026-06-22-arg",
    "fifwc-arg-aut-2026-06-22-aut",
    "fifwc-arg-aut-2026-06-22-draw",
    "fifwc-fra-irq-2026-06-22-fra",
    "fifwc-fra-irq-2026-06-22-irq",
    "fifwc-fra-irq-2026-06-22-draw",
    "fifwc-nor-sen-2026-06-22-nor",
    "fifwc-nor-sen-2026-06-22-sen",
    "fifwc-nor-sen-2026-06-22-draw",
)
DEFAULT_BUILD_TAG = "pmxt_v2_team_vs_team_main_lifecycle"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime_outputs" / "pmxt_v2_team_vs_team_main_lifecycle" / "sample_validation"


@dataclass(frozen=True)
class FillSample:
    sample_id: str
    market_id: int
    market_slug: str
    market_title: str
    fill_token_id: str
    l2_token_id: str
    token_side: str
    block_number: int
    transaction_index: int
    log_index: int
    tx_hash: str
    fill_time: str
    fill_ts_ms: int
    trade_price: Decimal
    size: Decimal
    side: str
    canonical_fill_key: str


def main() -> int:
    args = build_parser().parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    slugs = tuple(args.market_slug or DEFAULT_MARKET_SLUGS)
    mappings = load_mappings(slugs)
    samples = load_fill_samples(
        ClickHouseClient(),
        mappings=mappings,
        build_tag=str(args.build_tag),
        per_market=max(1, int(args.per_market)),
    )
    results = [
        validate_sample(
            ClickHouseClient(),
            sample,
            build_tag=str(args.build_tag),
            lookback_hours=max(1, int(args.lookback_hours)),
            book_ttl_ms=max(1, int(args.book_ttl_ms)),
            depth_haircut=Decimal(str(args.depth_haircut)),
            price_tolerance=Decimal(str(args.price_tolerance)),
        )
        for sample in samples
    ]
    summary = build_summary(args, slugs=slugs, results=results)
    (output_dir / "depth_fill_sample_validation.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    (output_dir / "depth_fill_sample_validation.md").write_text(
        summary_to_markdown(summary),
        encoding="utf-8",
    )
    print(summary_to_markdown(summary))
    return 0 if summary["status"] in {"ready", "review"} else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-slug", action="append", default=[])
    parser.add_argument("--build-tag", default=DEFAULT_BUILD_TAG)
    parser.add_argument("--per-market", type=int, default=1)
    parser.add_argument("--lookback-hours", type=int, default=6)
    parser.add_argument("--book-ttl-ms", type=int, default=300_000)
    parser.add_argument("--depth-haircut", default="1.0")
    parser.add_argument("--price-tolerance", default="0.000001")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def load_mappings(slugs: Sequence[str]):
    with postgres_connection(readonly=True) as conn:
        markets = load_markets_by_slug(conn, slugs)
        tokens_by_market = {}
        for market in markets:
            lifecycle = lifecycle_for_market(market)
            start = _required_hour(lifecycle.start_hour, market.market_slug)
            end = _required_hour(lifecycle.end_hour, market.market_slug)
            tokens_by_market[market.market_id] = load_market_tokens(conn, [market], start_hour=start, end_hour=end)
    return build_token_mappings(tokens_by_market)


def load_fill_samples(
    client: ClickHouseClient,
    *,
    mappings: Sequence[Any],
    build_tag: str,
    per_market: int,
) -> list[FillSample]:
    if not mappings:
        return []
    l2_by_fill = {m.fill_token_id: m.l2_token_id for m in mappings}
    token_side_by_fill = {m.fill_token_id: m.token_side for m in mappings}
    market_slug_by_market = {int(m.market_id): m.market_slug for m in mappings}
    pairs_sql = ",".join(f"({int(m.market_id)}, '{escape(m.fill_token_id)}')" for m in mappings)
    rows = client.query_json_rows(
        f"""
        WITH ranked AS (
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
                toUnixTimestamp64Milli(bt.block_time) AS fill_ts_ms,
                formatDateTime(bt.block_time, '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS fill_time,
                row_number() OVER (PARTITION BY f.market_id ORDER BY sipHash64(f.canonical_fill_key)) AS rn
            FROM (
                SELECT *
                FROM orderfilled_trade_replay FINAL
                WHERE build_tag = '{escape(build_tag)}'
            ) AS f
            LEFT JOIN block_timestamps AS bt ON bt.block_number = f.block_number
            WHERE (f.market_id, f.token_id) IN ({pairs_sql})
              AND bt.block_time > toDateTime('2000-01-01', 'UTC')
        )
        SELECT *
        FROM ranked
        WHERE rn <= {int(per_market)}
        ORDER BY market_id ASC, rn ASC
        """,
        timeout_seconds=240,
    )
    samples: list[FillSample] = []
    for row in rows:
        fill_token_id = str(row.get("fill_token_id") or "")
        market_id = int(row.get("market_id") or 0)
        samples.append(
            FillSample(
                sample_id=f"{row.get('market_id')}-{row.get('block_number')}-{row.get('log_index')}",
                market_id=market_id,
                market_slug=str(market_slug_by_market.get(market_id) or ""),
                market_title="",
                fill_token_id=fill_token_id,
                l2_token_id=str(l2_by_fill.get(fill_token_id) or ""),
                token_side=str(token_side_by_fill.get(fill_token_id) or ""),
                block_number=int(row.get("block_number") or 0),
                transaction_index=int(row.get("transaction_index") or 0),
                log_index=int(row.get("log_index") or 0),
                tx_hash=str(row.get("tx_hash") or ""),
                fill_time=str(row.get("fill_time") or ""),
                fill_ts_ms=int(row.get("fill_ts_ms") or 0),
                trade_price=Decimal(str(row.get("trade_price") or "0")),
                size=Decimal(str(row.get("size") or "0")),
                side=str(row.get("side") or "").upper(),
                canonical_fill_key=str(row.get("canonical_fill_key") or ""),
            )
        )
    return samples


def validate_sample(
    client: ClickHouseClient,
    sample: FillSample,
    *,
    build_tag: str,
    lookback_hours: int,
    book_ttl_ms: int,
    depth_haircut: Decimal,
    price_tolerance: Decimal,
) -> dict[str, Any]:
    events = load_l2_events_for_sample(client, sample, build_tag=build_tag, lookback_hours=lookback_hours)
    model = L2OrderFilledExecutionModel(
        L2ExecutionConfig(
            mode="optimistic",
            depth_haircut=depth_haircut,
            require_fresh_book=True,
            book_ttl_ms=book_ttl_ms,
            submit_latency_ms=0,
            fee_bps=Decimal("0"),
            impact_strength_bps=Decimal("0"),
        )
    )
    applied = 0
    snapshot_count = 0
    delta_count = 0
    for event in events:
        parsed = parse_l2_event(event, sample)
        if isinstance(parsed, BookSnapshot):
            model.apply_snapshot(parsed)
            snapshot_count += 1
            applied += 1
        elif isinstance(parsed, BookDelta):
            if model.apply_delta(parsed):
                applied += 1
            delta_count += 1

    best_bid = model.book.best_bid
    best_ask = model.book.best_ask
    attempts = []
    for attempted_side in ("BUY", "SELL"):
        attempt_model = clone_model_from_events(
            events,
            sample,
            book_ttl_ms=book_ttl_ms,
            depth_haircut=depth_haircut,
        )
        intent = StrategyOrderIntent(
            client_order_id=f"validate-{sample.sample_id}-{attempted_side.lower()}",
            signal_ts=ms_to_dt(sample.fill_ts_ms),
            market_id=str(sample.market_id),
            asset_id=sample.l2_token_id,
            side=attempted_side,
            order_type="MARKETABLE_LIMIT",
            limit_price=limit_price_for_attempt(sample.trade_price, attempted_side, price_tolerance),
            size=sample.size,
            tif="FOK",
            post_only=False,
        )
        attempt_result = attempt_model.execute_taker(intent)
        attempt_price_diff = abs(attempt_result.avg_fill_price - sample.trade_price) if attempt_result.filled_size > 0 else None
        attempt_size_diff = abs(attempt_result.filled_size - sample.size)
        attempts.append(
            {
                "attempted_side": attempted_side,
                "state": attempt_result.state,
                "reject_reason": attempt_result.reject_reason,
                "filled_size": attempt_result.filled_size,
                "avg_fill_price": attempt_result.avg_fill_price,
                "remaining_size": attempt_result.remaining_size,
                "fill_count": len(attempt_result.fills),
                "book_quality": attempt_result.book_quality.confidence if attempt_result.book_quality else None,
                "price_diff": attempt_price_diff,
                "size_diff": attempt_size_diff,
            }
        )
    exact_attempts = [
        item for item in attempts
        if item["state"] == "FILLED" and item["filled_size"] == sample.size and abs(item["avg_fill_price"] - sample.trade_price) <= price_tolerance
    ]
    filled_attempts = [
        item for item in attempts
        if item["state"] == "FILLED" and item["filled_size"] == sample.size
    ]
    chosen = exact_attempts[0] if exact_attempts else filled_attempts[0] if filled_attempts else attempts[0]
    price_diff = chosen["price_diff"]
    size_diff = chosen["size_diff"]
    l2_lag_seconds = None
    if model.book.last_update_ts is not None:
        l2_lag_seconds = max(0, int((ms_to_dt(sample.fill_ts_ms) - model.book.last_update_ts).total_seconds()))
    original_side_matches_direction_price = (
        (sample.side == "BUY" and best_ask is not None and best_ask <= sample.trade_price)
        or (sample.side == "SELL" and best_bid is not None and best_bid >= sample.trade_price)
    )
    any_side_matches_book_price = (
        (best_ask is not None and best_ask <= sample.trade_price + price_tolerance)
        or (best_bid is not None and best_bid >= sample.trade_price - price_tolerance)
    )
    strict_match = (
        chosen["state"] == "FILLED"
        and chosen["filled_size"] == sample.size
        and abs(chosen["avg_fill_price"] - sample.trade_price) <= price_tolerance
    )
    executable_match = chosen["state"] == "FILLED" and chosen["filled_size"] == sample.size and any_side_matches_book_price
    return {
        "sample": asdict(sample),
        "l2_context": {
            "events_loaded": len(events),
            "events_applied": applied,
            "snapshot_events": snapshot_count,
            "delta_events": delta_count,
            "book_ts": model.book.last_update_ts.isoformat() if model.book.last_update_ts else None,
            "l2_lag_seconds": l2_lag_seconds,
            "best_bid": str(best_bid) if best_bid is not None else None,
            "best_ask": str(best_ask) if best_ask is not None else None,
            "bid_depth_levels": len(model.book.bids),
            "ask_depth_levels": len(model.book.asks),
        },
        "simulation": {
            "chosen_attempted_side": chosen["attempted_side"],
            "state": chosen["state"],
            "reject_reason": chosen["reject_reason"],
            "filled_size": str(chosen["filled_size"]),
            "avg_fill_price": str(chosen["avg_fill_price"]),
            "remaining_size": str(chosen["remaining_size"]),
            "fill_count": chosen["fill_count"],
            "book_quality": chosen["book_quality"],
            "price_diff": str(price_diff) if price_diff is not None else None,
            "size_diff": str(size_diff),
            "strict_exact_match": strict_match,
            "directional_executable_match": bool(executable_match),
            "original_side_matches_direction_price": bool(original_side_matches_direction_price),
            "any_side_matches_book_price": bool(any_side_matches_book_price),
            "price_tolerance": str(price_tolerance),
            "attempts": [
                {
                    **item,
                    "filled_size": str(item["filled_size"]),
                    "avg_fill_price": str(item["avg_fill_price"]),
                    "remaining_size": str(item["remaining_size"]),
                    "price_diff": str(item["price_diff"]) if item["price_diff"] is not None else None,
                    "size_diff": str(item["size_diff"]),
                }
                for item in attempts
            ],
        },
    }


def clone_model_from_events(
    events: Sequence[Mapping[str, Any]],
    sample: FillSample,
    *,
    book_ttl_ms: int,
    depth_haircut: Decimal,
) -> L2OrderFilledExecutionModel:
    model = L2OrderFilledExecutionModel(
        L2ExecutionConfig(
            mode="optimistic",
            depth_haircut=depth_haircut,
            require_fresh_book=True,
            book_ttl_ms=book_ttl_ms,
            submit_latency_ms=0,
            fee_bps=Decimal("0"),
            impact_strength_bps=Decimal("0"),
        )
    )
    for event in events:
        parsed = parse_l2_event(event, sample)
        if isinstance(parsed, BookSnapshot):
            model.apply_snapshot(parsed)
        elif isinstance(parsed, BookDelta):
            model.apply_delta(parsed)
    return model


def load_l2_events_for_sample(
    client: ClickHouseClient,
    sample: FillSample,
    *,
    build_tag: str,
    lookback_hours: int,
) -> list[dict[str, Any]]:
    start_ms = sample.fill_ts_ms - lookback_hours * 3600 * 1000
    snapshot = client.query_json_rows(
        f"""
        SELECT *
        FROM pmxt_l2_event_replay FINAL
        PREWHERE market_id = {sample.market_id} AND token_id = '{escape(sample.l2_token_id)}'
        WHERE build_tag = '{escape(build_tag)}'
          AND event_type = 'book_snapshot'
          AND event_ts_ms <= {sample.fill_ts_ms}
        ORDER BY event_ts_ms DESC, source_hour DESC, source_row_index DESC, source_event_index DESC
        LIMIT 1
        """,
        timeout_seconds=120,
    )
    if not snapshot:
        return []
    snapshot_ts = int(snapshot[0].get("event_ts_ms") or 0)
    from_ms = max(start_ms, snapshot_ts)
    deltas = client.query_json_rows(
        f"""
        SELECT *
        FROM pmxt_l2_event_replay FINAL
        PREWHERE market_id = {sample.market_id} AND token_id = '{escape(sample.l2_token_id)}'
        WHERE build_tag = '{escape(build_tag)}'
          AND event_type = 'price_change'
          AND event_ts_ms > {from_ms}
          AND event_ts_ms <= {sample.fill_ts_ms}
        ORDER BY event_ts_ms ASC, source_hour ASC, source_row_index ASC, source_event_index ASC
        """,
        timeout_seconds=180,
    )
    return snapshot + deltas


def parse_l2_event(row: Mapping[str, Any], sample: FillSample) -> BookSnapshot | BookDelta | None:
    event_type = str(row.get("event_type") or "")
    ts = ms_to_dt(int(row.get("event_ts_ms") or 0))
    payload = json.loads(str(row.get("payload_json") or "{}") or "{}")
    if event_type == "book_snapshot":
        bids = tuple(BookLevel(Decimal(str(price)), Decimal(str(size))) for price, size in payload.get("bids", []) or [])
        asks = tuple(BookLevel(Decimal(str(price)), Decimal(str(size))) for price, size in payload.get("asks", []) or [])
        return BookSnapshot(
            ts=ts,
            market_id=str(sample.market_id),
            asset_id=sample.l2_token_id,
            sequence=int(row.get("source_row_index") or 0),
            source="pmxt_l2_event_replay",
            bids=bids,
            asks=asks,
            hash=str(row.get("source_hash") or ""),
            is_full_depth=True,
            observed_depth_levels=len(bids) + len(asks),
        )
    if event_type == "price_change":
        return BookDelta(
            ts=ts,
            market_id=str(sample.market_id),
            asset_id=sample.l2_token_id,
            side="BUY" if str(row.get("side") or "").lower() == "bid" else "SELL",
            price=Decimal(str(row.get("price") or "0")),
            new_size=Decimal(str(row.get("size") or "0")),
            sequence=int(row.get("source_row_index") or 0),
            source="pmxt_l2_event_replay",
            hash=str(row.get("source_hash") or ""),
        )
    return None


def build_summary(args: argparse.Namespace, *, slugs: Sequence[str], results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(results)
    filled = sum(1 for row in results if (row.get("simulation") or {}).get("state") == "FILLED")
    strict = sum(1 for row in results if (row.get("simulation") or {}).get("strict_exact_match"))
    executable = sum(1 for row in results if (row.get("simulation") or {}).get("directional_executable_match"))
    return {
        "schema_version": "team_vs_team_depth_fill_sample_validation_v1",
        "status": "ready" if total and executable == total else "review" if total else "missing",
        "parameters": {
            "market_slug": list(slugs),
            "build_tag": str(args.build_tag),
            "per_market": int(args.per_market),
            "lookback_hours": int(args.lookback_hours),
            "book_ttl_ms": int(args.book_ttl_ms),
            "depth_haircut": str(args.depth_haircut),
            "price_tolerance": str(args.price_tolerance),
        },
        "aggregate": {
            "samples": total,
            "simulated_filled": filled,
            "simulated_filled_pct": pct(filled, total),
            "strict_exact_matches": strict,
            "strict_exact_match_pct": pct(strict, total),
            "directional_executable_matches": executable,
            "directional_executable_match_pct": pct(executable, total),
        },
        "results": list(results),
    }


def summary_to_markdown(summary: Mapping[str, Any]) -> str:
    aggregate = summary.get("aggregate") or {}
    lines = [
        f"# DEPTH + Fill Sample Validation: {summary.get('status')}",
        "",
        f"- samples: `{aggregate.get('samples')}`",
        f"- simulated_filled: `{aggregate.get('simulated_filled')}` / `{aggregate.get('simulated_filled_pct')}%`",
        f"- directional_executable_matches: `{aggregate.get('directional_executable_matches')}` / `{aggregate.get('directional_executable_match_pct')}%`",
        f"- strict_exact_matches: `{aggregate.get('strict_exact_matches')}` / `{aggregate.get('strict_exact_match_pct')}%`",
        "",
        "| market | raw side | chosen taker | fill price | size | best bid/ask | sim state | sim price | sim size | lag | verdict |",
        "| --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in summary.get("results") or []:
        sample = row.get("sample") or {}
        l2 = row.get("l2_context") or {}
        sim = row.get("simulation") or {}
        verdict = "ok" if sim.get("directional_executable_match") else "review"
        lines.append(
            "| "
            f"`{sample.get('market_slug')}` | "
            f"{sample.get('side')} {sample.get('token_side')} | "
            f"{sim.get('chosen_attempted_side')} | "
            f"{sample.get('trade_price')} | "
            f"{sample.get('size')} | "
            f"{l2.get('best_bid')} / {l2.get('best_ask')} | "
            f"{sim.get('state')} | "
            f"{sim.get('avg_fill_price')} | "
            f"{sim.get('filled_size')} | "
            f"{l2.get('l2_lag_seconds')}s | "
            f"{verdict} |"
        )
    lines.extend(
        [
            "",
            "Interpretation:",
            "",
            "- `raw side` is the side stored on the OrderFilled replay row. It is not blindly treated as the strategy taker side.",
            "- `chosen taker` is the taker direction, BUY or SELL, that can explain the historical fill from the latest L2 book before the fill.",
            "- `directional_executable_match` means the fill could be executed from that L2 book with the same size and a price within the configured tolerance.",
            "- `strict_exact_match` uses the configured price tolerance because several on-chain fill prices are `0.409999` while the PMXT tick is `0.410000`.",
            "",
        ]
    )
    return "\n".join(lines)


def pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator * 100.0 / denominator, 6)


def ms_to_dt(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def limit_price_for_attempt(price: Decimal, side: str, tolerance: Decimal) -> Decimal:
    if side == "BUY":
        return min(Decimal("1"), price + tolerance)
    return max(Decimal("0"), price - tolerance)


def escape(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


if __name__ == "__main__":
    raise SystemExit(main())
