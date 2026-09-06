#!/usr/bin/env python3
"""Run a real-market OrderFilled + PMXT L2 execution evidence batch report.

The report is intentionally data-facing: it selects markets that already have a
PMXT market-hour coverage summary, loads bounded OrderFilled tick replay rows,
aligns those fills to PMXT L2 book state, and summarizes whether DEPTH /
ORDERFILLED_LOB execution is currently evidence-backed for the sampled markets.
"""

from __future__ import annotations

import argparse
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.runners.trade_replay_store import (  # noqa: E402
    backfill_orderfilled_trade_replay,
    load_orderfilled_trade_replay_coverage,
    load_orderfilled_trade_replay_rows,
)
from quant.core.db import postgres_connection  # noqa: E402
from scripts.validate_orderfilled_trade_replay import (  # noqa: E402
    _alignment_selection_from_args,
    build_orderfilled_pmxt_alignment_validation_report,
    build_orderfilled_trade_replay_validation_report,
    enrich_orderfilled_rows_with_block_timestamps,
    load_pmxt_rows_for_orderfilled_alignment,
    rows_for_pmxt_alignment,
)


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime_outputs" / "orderfilled_pmxt_l2_batch_report"
DEFAULT_PMXT_ROOT = PROJECT_ROOT / "runtime_outputs" / "pmxt_l2_validation" / "raw_sample"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pmxt-root", type=Path, default=DEFAULT_PMXT_ROOT)
    parser.add_argument("--limit", type=int, default=5, help="Number of depth-ready markets to sample.")
    parser.add_argument("--min-orderfilled-rows", type=int, default=25)
    parser.add_argument("--min-depth-events", type=int, default=100)
    parser.add_argument("--max-orderfilled-rows", type=int, default=5_000)
    parser.add_argument("--pmxt-max-hours", type=int, default=4)
    parser.add_argument("--pmxt-batch-size", type=int, default=50_000)
    parser.add_argument("--max-lag-ms", type=int, default=60_000)
    parser.add_argument("--depth-levels", type=int, default=5)
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument("--backfill", action="store_true", help="Materialize bounded raw OrderFilled ticks before validating.")
    parser.add_argument("--force", action="store_true", help="Force a new bounded tick-replay backfill version.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_batch_report(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "orderfilled_pmxt_l2_batch_report.json"
    md_path = args.output_dir / "orderfilled_pmxt_l2_batch_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    md_path.write_text(batch_report_to_markdown(report), encoding="utf-8")
    print(json_path if args.format == "json" else md_path)
    return 0 if report.get("status") in {"ready", "review"} else 1


def build_batch_report(args: argparse.Namespace) -> dict[str, Any]:
    pmxt_root = Path(args.pmxt_root)
    with postgres_connection(readonly=not bool(args.backfill)) as conn:
        candidates = select_depth_ready_markets(
            conn,
            limit=max(1, int(args.limit)),
            min_orderfilled_rows=max(1, int(args.min_orderfilled_rows)),
            min_depth_events=max(0, int(args.min_depth_events)),
        )
        market_reports: list[dict[str, Any]] = []
        for candidate in candidates:
            market_reports.append(run_market_report(conn, candidate, args=args, pmxt_root=pmxt_root))
        if args.backfill:
            conn.commit()
    aggregate = aggregate_market_reports(market_reports)
    status = "ready" if aggregate["ready_market_count"] else "review" if market_reports else "missing"
    return {
        "schema_version": "orderfilled_pmxt_l2_batch_report_v1",
        "status": status,
        "pmxt_root": str(pmxt_root),
        "sample_policy": {
            "limit": int(args.limit),
            "min_orderfilled_rows": int(args.min_orderfilled_rows),
            "min_depth_events": int(args.min_depth_events),
            "max_orderfilled_rows_per_market": int(args.max_orderfilled_rows),
            "max_lag_ms": int(args.max_lag_ms),
            "depth_levels": int(args.depth_levels),
            "backfill": bool(args.backfill),
        },
        "aggregate": aggregate,
        "markets": market_reports,
        "next_actions": next_actions_for_report(aggregate, market_reports),
    }


def select_depth_ready_markets(
    conn: Any,
    *,
    limit: int,
    min_orderfilled_rows: int,
    min_depth_events: int,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.window_start_hour,
                c.window_end_hour,
                c.market_id,
                c.market_slug,
                c.market_title,
                c.block_tape_first_hour,
                c.block_tape_last_hour,
                c.block_tape_hours,
                c.observed_pmxt_hours,
                c.depth_ready_hours,
                c.missing_depth_hours,
                c.first_depth_ready_hour,
                c.last_depth_ready_hour,
                c.depth_available_hour_pct,
                c.overlap_depth_events,
                c.total_block_rows,
                m.condition_id,
                m.token_id AS price_token_id,
                COALESCE(NULLIF(m.token_id_hex, ''), m.token_id) AS replay_token_id,
                m.token_side,
                COALESCE(e.orderfilled_rows, 0) AS orderfilled_rows
            FROM quant.pmxt_market_depth_coverage_summary c
            JOIN quant.market_token_metadata m
              ON m.market_id = c.market_id
             AND upper(m.token_side) = 'YES'
            LEFT JOIN quant.market_event_members e
              ON e.market_id = c.market_id
            WHERE c.can_do_depth
              AND c.overlap_depth_events >= %s
              AND COALESCE(e.orderfilled_rows, 0) >= %s
            ORDER BY c.depth_ready_hours DESC,
                     c.overlap_depth_events DESC,
                     c.total_block_rows DESC,
                     c.market_id ASC
            LIMIT %s
            """,
            (int(min_depth_events), int(min_orderfilled_rows), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def run_market_report(conn: Any, candidate: Mapping[str, Any], *, args: argparse.Namespace, pmxt_root: Path) -> dict[str, Any]:
    window = block_window_for_candidate(conn, candidate)
    if not window:
        return {
            "status": "missing",
            "market": market_payload(candidate),
            "reason": "no block-close rows in the PMXT depth-ready hour window",
        }
    market_id = int(candidate["market_id"])
    replay_token_id = str(candidate["replay_token_id"] or candidate["price_token_id"]).lower()
    pairs = [(market_id, replay_token_id)]
    backfill_result = None
    if args.backfill:
        backfill_result = backfill_orderfilled_trade_replay(
            pairs,
            from_block=int(window["from_block"]),
            to_block=int(window["to_block"]),
            force=bool(args.force),
            build_tag="orderfilled_pmxt_l2_batch_report",
        ).as_dict()
    coverage = load_orderfilled_trade_replay_coverage(pairs, from_block=int(window["from_block"]), to_block=int(window["to_block"]))
    rows = load_orderfilled_trade_replay_rows(
        pairs,
        from_block=int(window["from_block"]),
        to_block=int(window["to_block"]),
        limit=max(1, int(args.max_orderfilled_rows)),
    )
    rows = enrich_orderfilled_rows_with_block_timestamps(rows, overwrite=True, interpolate=True)
    replay_report = build_orderfilled_trade_replay_validation_report(
        rows,
        market_id=market_id,
        token_id=replay_token_id,
        from_block=int(window["from_block"]),
        to_block=int(window["to_block"]),
        coverage=coverage,
        backfill_result=backfill_result,
    )
    selection_args = argparse.Namespace(
        condition_id=str(candidate.get("condition_id") or ""),
        price_token_id=str(candidate.get("price_token_id") or ""),
        token_side=str(candidate.get("token_side") or "YES"),
        market_slug=str(candidate.get("market_slug") or ""),
    )
    selection = _alignment_selection_from_args(
        selection_args,
        candidate=candidate,
        market_id=market_id,
        replay_token_id=replay_token_id,
    )
    if selection is None:
        alignment = {"status": "missing", "reason": "missing condition_id or PMXT token id"}
        pmxt_rows = []
        schema_kind = "missing"
    else:
        alignment_rows = rows_for_pmxt_alignment(rows, selection)
        pmxt_rows, schema_kind = load_pmxt_rows_for_orderfilled_alignment(
            pmxt_root,
            selection=selection,
            orderfilled_rows=alignment_rows,
            max_hours=max(1, int(args.pmxt_max_hours)),
            batch_size=max(1, int(args.pmxt_batch_size)),
        )
        alignment = build_orderfilled_pmxt_alignment_validation_report(
            alignment_rows,
            pmxt_rows=pmxt_rows,
            schema_kind=schema_kind,
            selection=selection,
            max_lag_ms=max(0, int(args.max_lag_ms)),
            depth_levels=max(1, int(args.depth_levels)),
            sample_limit=max(0, int(args.sample_limit)),
        )
    explainability = build_fill_no_fill_explainability(alignment)
    profile_calibration = build_profile_calibration(alignment, explainability)
    backtest_orders = load_backtest_order_reconciliation(conn, candidate, window)
    status = market_status(replay_report, alignment, explainability)
    return {
        "status": status,
        "market": market_payload(candidate),
        "block_window": window,
        "replay_token_id": replay_token_id,
        "price_token_id": str(candidate.get("price_token_id") or ""),
        "orderfilled_replay": replay_report,
        "pmxt_l2_alignment": alignment,
        "pmxt_rows_loaded": len(pmxt_rows),
        "pmxt_schema_kind": schema_kind,
        "fill_no_fill_explainability": explainability,
        "profile_calibration": profile_calibration,
        "backtest_order_reconciliation": backtest_orders,
    }


def block_window_for_candidate(conn: Any, candidate: Mapping[str, Any]) -> dict[str, Any] | None:
    first_hour = candidate.get("first_depth_ready_hour")
    last_hour = candidate.get("last_depth_ready_hour")
    token_id = str(candidate.get("price_token_id") or "")
    if not first_hour or not last_hour or not token_id:
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                MIN(block_number) AS from_block,
                MAX(block_number) AS to_block,
                COUNT(*) AS block_rows,
                MIN(block_timestamp) AS first_block_timestamp,
                MAX(block_timestamp) AS last_block_timestamp
            FROM quant.market_token_block_close
            WHERE market_id = %s
              AND token_id = %s
              AND block_timestamp >= %s
              AND block_timestamp < %s
            """,
            (
                int(candidate["market_id"]),
                token_id,
                first_hour,
                last_hour + timedelta(hours=1),
            ),
        )
        row = cur.fetchone()
    if not row or not row.get("block_rows"):
        return None
    return {
        "from_block": int(row["from_block"]),
        "to_block": int(row["to_block"]),
        "block_rows": int(row["block_rows"]),
        "first_block_timestamp": row.get("first_block_timestamp"),
        "last_block_timestamp": row.get("last_block_timestamp"),
    }


def enrich_rows_with_postgres_block_timestamps(
    conn: Any,
    rows: Sequence[Mapping[str, Any]],
    candidate: Mapping[str, Any],
) -> list[dict[str, Any]]:
    output = [dict(row) for row in rows]
    missing_blocks = sorted({
        int(row.get("block_number") or 0)
        for row in output
        if int(row.get("block_number") or 0) > 0 and not _row_has_timestamp(row)
    })
    if not missing_blocks:
        return output
    token_id = str(candidate.get("price_token_id") or "")
    if not token_id:
        return output
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT block_number, MIN(block_timestamp) AS block_timestamp
            FROM quant.market_token_block_close
            WHERE market_id = %s
              AND token_id = %s
              AND block_number = ANY(%s)
              AND block_timestamp IS NOT NULL
            GROUP BY block_number
            """,
            (int(candidate["market_id"]), token_id, missing_blocks),
        )
        timestamp_by_block = {int(row["block_number"]): row["block_timestamp"] for row in cur.fetchall()}
    for row in output:
        block_number = int(row.get("block_number") or 0)
        if block_number in timestamp_by_block and not _row_has_timestamp(row):
            row["block_timestamp"] = timestamp_by_block[block_number]
            row["block_timestamp_source"] = "quant.market_token_block_close"
    return output


def load_backtest_order_reconciliation(
    conn: Any,
    candidate: Mapping[str, Any],
    window: Mapping[str, Any],
) -> dict[str, Any]:
    market_slug = str(candidate.get("market_slug") or "").strip()
    if not market_slug:
        return {
            "status": "missing",
            "reason": "candidate has no market_slug",
            "run_ids": [],
            "order_count": 0,
        }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                r.run_id,
                r.status AS run_status,
                r.from_block,
                r.to_block,
                r.backtest_engine,
                o.order_id,
                o.status AS order_status,
                o.filled_size,
                o.filled_notional,
                o.requested_size,
                o.no_fill_reason,
                o.execution_source,
                o.execution_evidence_type,
                o.raw_candidate_event_count,
                o.raw_consumed_event_count,
                o.expected_fill_size,
                o.actual_fill_size,
                o.expected_fill_notional,
                o.actual_fill_notional,
                o.meta
            FROM quant.quant_backtest_runs r
            LEFT JOIN quant.quant_backtest_orders o
              ON o.run_id = r.run_id
            WHERE r.market_slug = %s
              AND r.from_block <= %s
              AND r.to_block >= %s
            ORDER BY r.run_id DESC, o.order_id
            LIMIT 10000
            """,
            (market_slug, int(window["to_block"]), int(window["from_block"])),
        )
        rows = [dict(row) for row in cur.fetchall()]
    return build_backtest_order_reconciliation(rows)


def build_backtest_order_reconciliation(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    run_ids = sorted({int(row["run_id"]) for row in rows if row.get("run_id")}, reverse=True)
    orders = [dict(row) for row in rows if row.get("order_id")]
    status_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    profile_counts: dict[str, int] = {}
    evidence_count = 0
    explained_count = 0
    accounted_count = 0
    for order in orders:
        status = str(order.get("order_status") or "UNKNOWN").upper()
        status_counts[status] = status_counts.get(status, 0) + 1
        source = str(order.get("execution_source") or "missing")
        source_counts[source] = source_counts.get(source, 0) + 1
        meta = order.get("meta") if isinstance(order.get("meta"), Mapping) else {}
        profile = str(meta.get("execution_profile") or "missing")
        profile_counts[profile] = profile_counts.get(profile, 0) + 1
        has_evidence = _order_has_orderfilled_evidence(order)
        if has_evidence:
            evidence_count += 1
        if _order_fill_no_fill_explained(order, has_evidence=has_evidence):
            explained_count += 1
        if _order_accounted(order):
            accounted_count += 1
    if not run_ids:
        status = "missing"
        reason = "no overlapping quant_backtest_runs for sampled market/window"
    elif not orders:
        status = "review"
        reason = "overlapping runs exist but no persisted quant_backtest_orders"
    elif explained_count == len(orders) and accounted_count == len(orders):
        status = "ready"
        reason = "persisted backtest orders have fill/no-fill explanations and accounting fields"
    else:
        status = "review"
        reason = "some persisted backtest orders need stronger explanation or accounting evidence"
    return {
        "status": status,
        "reason": reason,
        "run_ids": run_ids,
        "order_count": len(orders),
        "status_counts": status_counts,
        "execution_source_counts": source_counts,
        "profile_counts": profile_counts,
        "orderfilled_evidence_order_count": evidence_count,
        "orderfilled_evidence_order_pct": pct_text(evidence_count, len(orders)),
        "fill_no_fill_explained_order_count": explained_count,
        "fill_no_fill_explained_order_pct": pct_text(explained_count, len(orders)),
        "accounted_order_count": accounted_count,
        "accounted_order_pct": pct_text(accounted_count, len(orders)),
    }


def market_payload(candidate: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "market_id": int(candidate.get("market_id") or 0),
        "market_slug": candidate.get("market_slug"),
        "market_title": candidate.get("market_title"),
        "condition_id": candidate.get("condition_id"),
        "depth_available_hour_pct": str(candidate.get("depth_available_hour_pct") or "0"),
        "block_tape_hours": int(candidate.get("block_tape_hours") or 0),
        "depth_ready_hours": int(candidate.get("depth_ready_hours") or 0),
        "observed_pmxt_hours": int(candidate.get("observed_pmxt_hours") or 0),
        "overlap_depth_events": int(candidate.get("overlap_depth_events") or 0),
        "total_block_rows": int(candidate.get("total_block_rows") or 0),
        "orderfilled_rows": int(candidate.get("orderfilled_rows") or 0),
        "first_depth_ready_hour": candidate.get("first_depth_ready_hour"),
        "last_depth_ready_hour": candidate.get("last_depth_ready_hour"),
    }


def build_fill_no_fill_explainability(alignment: Mapping[str, Any]) -> dict[str, Any]:
    matched = _int(alignment.get("orderfilled_rows_matched"))
    aligned = _int(alignment.get("aligned_count"))
    depth_checked = _int(alignment.get("depth_checked_count"))
    depth_sufficient = _int(alignment.get("depth_sufficient_count"))
    crossable = _int(alignment.get("crossable_depth_sufficient_count"))
    no_fill_reasons = {
        "missing_fill_timestamp": _int(alignment.get("missing_timestamp_count")),
        "missing_l2_before_fill": _int(alignment.get("missing_l2_before_fill_count")),
        "stale_l2": _int(alignment.get("stale_l2_count")),
        "price_outside_spread": _int(alignment.get("price_outside_spread_count")),
        "l2_depth_insufficient": _int(alignment.get("depth_insufficient_count")),
        "price_unchecked": _int(alignment.get("price_unchecked_count")),
    }
    no_fill_explained = sum(no_fill_reasons.values())
    classified = depth_sufficient + no_fill_explained
    unclassified = max(0, matched - classified)
    return {
        "status": "ready" if matched and unclassified == 0 else "review" if matched else "missing",
        "backtest_order_proxy_count": matched,
        "aligned_fill_evidence_count": aligned,
        "depth_checked_count": depth_checked,
        "l2_fill_explained_count": depth_sufficient,
        "crossable_fill_explained_count": crossable,
        "l2_fill_explained_pct": pct_text(depth_sufficient, matched),
        "crossable_fill_explained_pct": pct_text(crossable, matched),
        "no_fill_or_review_explained_count": no_fill_explained,
        "no_fill_or_review_reason_counts": no_fill_reasons,
        "unclassified_count": unclassified,
        "alignment_pct": alignment.get("alignment_pct") or "0",
        "price_compatible_pct": alignment.get("price_compatible_pct") or "0",
    }


def build_profile_calibration(alignment: Mapping[str, Any], explainability: Mapping[str, Any]) -> dict[str, Any]:
    matched = _int(alignment.get("orderfilled_rows_matched"))
    conservative_count = _int(explainability.get("crossable_fill_explained_count"))
    realistic_count = _int(explainability.get("l2_fill_explained_count"))
    optimistic_count = _int(alignment.get("aligned_count"))
    rows = [
        {
            "execution_profile": "conservative",
            "evidence_rule": "fresh L2, price-compatible, crossable same-side depth sufficient",
            "supported_count": conservative_count,
            "supported_pct": pct_text(conservative_count, matched),
        },
        {
            "execution_profile": "realistic",
            "evidence_rule": "fresh L2, price-compatible, side depth sufficient",
            "supported_count": realistic_count,
            "supported_pct": pct_text(realistic_count, matched),
        },
        {
            "execution_profile": "optimistic",
            "evidence_rule": "fresh L2 aligned before fill, regardless of full depth sufficiency",
            "supported_count": optimistic_count,
            "supported_pct": pct_text(optimistic_count, matched),
        },
    ]
    price_compatible = _decimal(alignment.get("price_compatible_pct"))
    if matched <= 0:
        recommendation = "missing"
        reason = "no matched OrderFilled rows for calibration"
    elif _decimal(rows[0]["supported_pct"]) >= Decimal("70") and price_compatible >= Decimal("90"):
        recommendation = "conservative"
        reason = "crossable L2 depth explains most matched fills"
    elif _decimal(rows[1]["supported_pct"]) >= Decimal("70") and price_compatible >= Decimal("90"):
        recommendation = "realistic"
        reason = "side L2 depth explains most matched fills, but crossable depth is weaker"
    else:
        recommendation = "review"
        reason = "historical OrderFilled/L2 calibration evidence is weak; this does not by itself disable order-time DEPTH execution"
    return {
        "status": "ready" if recommendation in {"conservative", "realistic"} else "review" if matched else "missing",
        "sample_count": matched,
        "recommended_profile": recommendation,
        "reason": reason,
        "profile_rows": rows,
    }


def market_status(
    replay_report: Mapping[str, Any],
    alignment: Mapping[str, Any],
    explainability: Mapping[str, Any],
) -> str:
    if replay_report.get("status") != "ready":
        return "missing"
    if alignment.get("status") == "ready" and explainability.get("status") == "ready":
        return "ready"
    return "review"


def aggregate_market_reports(markets: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total_markets = len(markets)
    ready = sum(1 for row in markets if row.get("status") == "ready")
    review = sum(1 for row in markets if row.get("status") == "review")
    missing = sum(1 for row in markets if row.get("status") == "missing")
    matched = sum(_int(_nested(row, "pmxt_l2_alignment", "orderfilled_rows_matched")) for row in markets)
    aligned = sum(_int(_nested(row, "pmxt_l2_alignment", "aligned_count")) for row in markets)
    depth_sufficient = sum(_int(_nested(row, "pmxt_l2_alignment", "depth_sufficient_count")) for row in markets)
    crossable = sum(_int(_nested(row, "pmxt_l2_alignment", "crossable_depth_sufficient_count")) for row in markets)
    pmxt_depth_ready_hours = sum(_int(_nested(row, "market", "depth_ready_hours")) for row in markets)
    pmxt_block_tape_hours = sum(_int(_nested(row, "market", "block_tape_hours")) for row in markets)
    backtest_orders = sum(_int(_nested(row, "backtest_order_reconciliation", "order_count")) for row in markets)
    backtest_explained = sum(_int(_nested(row, "backtest_order_reconciliation", "fill_no_fill_explained_order_count")) for row in markets)
    backtest_accounted = sum(_int(_nested(row, "backtest_order_reconciliation", "accounted_order_count")) for row in markets)
    return {
        "market_count": total_markets,
        "ready_market_count": ready,
        "review_market_count": review,
        "missing_market_count": missing,
        "orderfilled_rows_matched": matched,
        "aligned_count": aligned,
        "alignment_pct": pct_text(aligned, matched),
        "l2_depth_sufficient_count": depth_sufficient,
        "l2_depth_sufficient_pct": pct_text(depth_sufficient, matched),
        "crossable_depth_sufficient_count": crossable,
        "crossable_depth_sufficient_pct": pct_text(crossable, matched),
        "pmxt_depth_ready_hours": pmxt_depth_ready_hours,
        "pmxt_block_tape_hours": pmxt_block_tape_hours,
        "pmxt_depth_available_hour_pct": pct_text(pmxt_depth_ready_hours, pmxt_block_tape_hours),
        "backtest_order_count": backtest_orders,
        "backtest_order_fill_no_fill_explained_pct": pct_text(backtest_explained, backtest_orders),
        "backtest_order_accounted_pct": pct_text(backtest_accounted, backtest_orders),
    }


def next_actions_for_report(aggregate: Mapping[str, Any], markets: Sequence[Mapping[str, Any]]) -> list[str]:
    actions: list[str] = []
    if not markets:
        return ["Run PMXT market-hour coverage first, then rerun this batch report."]
    if _int(aggregate.get("missing_market_count")):
        actions.append("Inspect missing markets: bounded OrderFilled replay or PMXT rows were unavailable.")
    if _decimal(aggregate.get("pmxt_depth_available_hour_pct")) <= Decimal("0"):
        actions.append("Materialize PMXT raw parquet for the target market/token/hour window before running DEPTH.")
    if _decimal(aggregate.get("alignment_pct")) < Decimal("90"):
        actions.append("Treat low historical fill-L2 alignment as calibration/review evidence, not as an order-time DEPTH availability gate.")
    if _decimal(aggregate.get("l2_depth_sufficient_pct")) < Decimal("70"):
        actions.append("Keep profile calibration in review; use order-level LOB coverage to decide whether a DEPTH run is executable.")
    if _int(aggregate.get("backtest_order_count")) <= 0:
        actions.append("Run or select backtests on the sampled PMXT-covered markets; this batch has no overlapping persisted backtest orders.")
    if not actions:
        actions.append("This sampled window is ready for an order-level DEPTH coverage check with materialized PMXT snapshots.")
    return actions


def batch_report_to_markdown(report: Mapping[str, Any]) -> str:
    aggregate = report.get("aggregate") if isinstance(report.get("aggregate"), Mapping) else {}
    lines = [
        f"# OrderFilled + PMXT L2 Batch Report: {report.get('status')}",
        "",
        f"- pmxt_root: `{report.get('pmxt_root')}`",
        f"- markets: {aggregate.get('market_count', 0)}",
        f"- ready/review/missing: {aggregate.get('ready_market_count', 0)} / {aggregate.get('review_market_count', 0)} / {aggregate.get('missing_market_count', 0)}",
        f"- orderfilled_rows_matched: {aggregate.get('orderfilled_rows_matched', 0)}",
        f"- alignment_pct: {aggregate.get('alignment_pct', '0')}%",
        f"- l2_depth_sufficient_pct: {aggregate.get('l2_depth_sufficient_pct', '0')}%",
        f"- crossable_depth_sufficient_pct: {aggregate.get('crossable_depth_sufficient_pct', '0')}%",
        f"- pmxt_depth_available_hour_pct: {aggregate.get('pmxt_depth_available_hour_pct', '0')}% ({aggregate.get('pmxt_depth_ready_hours', 0)} / {aggregate.get('pmxt_block_tape_hours', 0)} market-hours)",
        f"- backtest_order_count: {aggregate.get('backtest_order_count', 0)}",
        f"- backtest_order_fill_no_fill_explained_pct: {aggregate.get('backtest_order_fill_no_fill_explained_pct', '0')}%",
        f"- backtest_order_accounted_pct: {aggregate.get('backtest_order_accounted_pct', '0')}%",
        "",
        "## Interpretation",
        "",
        "- `alignment_pct` is historical OrderFilled-vs-L2 calibration coverage; it is not the DEPTH execution availability gate.",
        "- DEPTH execution availability must be checked at simulated order submit time against materialized `quant.clob_orderbook_snapshots`.",
        "- PMXT raw parquet coverage, snapshot materialization coverage, and historical fill calibration are separate checks.",
        "",
        "| Market | Status | PMXT hours | Matched fills | Align % | Depth % | Crossable % | Backtest orders | Order explain % | Accounted % | Recommended profile |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in report.get("markets") or []:
        if not isinstance(row, Mapping):
            continue
        market = row.get("market") if isinstance(row.get("market"), Mapping) else {}
        align = row.get("pmxt_l2_alignment") if isinstance(row.get("pmxt_l2_alignment"), Mapping) else {}
        explain = row.get("fill_no_fill_explainability") if isinstance(row.get("fill_no_fill_explainability"), Mapping) else {}
        profile = row.get("profile_calibration") if isinstance(row.get("profile_calibration"), Mapping) else {}
        order_recon = row.get("backtest_order_reconciliation") if isinstance(row.get("backtest_order_reconciliation"), Mapping) else {}
        lines.append(
            "| "
            + " | ".join(
                [
                    str(market.get("market_slug") or market.get("market_id") or "-"),
                    str(row.get("status") or "-"),
                    f"{market.get('depth_ready_hours') or 0}/{market.get('block_tape_hours') or 0}",
                    str(align.get("orderfilled_rows_matched") or 0),
                    str(align.get("alignment_pct") or "0"),
                    str(explain.get("l2_fill_explained_pct") or "0"),
                    str(explain.get("crossable_fill_explained_pct") or "0"),
                    str(order_recon.get("order_count") or 0),
                    str(order_recon.get("fill_no_fill_explained_order_pct") or "0"),
                    str(order_recon.get("accounted_order_pct") or "0"),
                    str(profile.get("recommended_profile") or "-"),
                ]
            )
            + " |"
        )
    actions = report.get("next_actions") or []
    if actions:
        lines.extend(["", "## Next Actions", ""])
        lines.extend(f"- {action}" for action in actions)
    return "\n".join(lines).rstrip() + "\n"


def pct_text(num: int, den: int) -> str:
    if den <= 0:
        return "0"
    value = (Decimal(int(num)) / Decimal(int(den)) * Decimal("100")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    text = format(value, "f").rstrip("0").rstrip(".")
    return text or "0"


def _nested(row: Mapping[str, Any], *keys: str) -> Any:
    value: Any = row
    for key in keys:
        if not isinstance(value, Mapping):
            return None
        value = value.get(key)
    return value


def _int(value: Any) -> int:
    try:
        return int(Decimal(str(value or 0)))
    except Exception:
        return 0


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _order_has_orderfilled_evidence(order: Mapping[str, Any]) -> bool:
    evidence_type = str(order.get("execution_evidence_type") or "").lower()
    execution_source = str(order.get("execution_source") or "").lower()
    meta = order.get("meta") if isinstance(order.get("meta"), Mapping) else {}
    consumed_events = meta.get("consumed_events") if isinstance(meta, Mapping) else None
    return (
        "orderfilled" in evidence_type
        or "orderfilled" in execution_source
        or _int(order.get("raw_consumed_event_count")) > 0
        or (isinstance(consumed_events, Sequence) and not isinstance(consumed_events, (str, bytes)) and len(consumed_events) > 0)
    )


def _order_fill_no_fill_explained(order: Mapping[str, Any], *, has_evidence: bool) -> bool:
    status = str(order.get("order_status") or "").upper()
    filled_size = _decimal(order.get("filled_size"))
    if filled_size > 0 or status in {"FILLED", "PARTIAL"}:
        return has_evidence or str(order.get("execution_source") or "").strip() != ""
    meta = order.get("meta") if isinstance(order.get("meta"), Mapping) else {}
    return (
        str(order.get("no_fill_reason") or "").strip() != ""
        or str(meta.get("lob_fill_status") or "").strip() != ""
        or status in {"NO_FILL", "REJECTED", "CANCELED", "CANCELLED"}
    )


def _order_accounted(order: Mapping[str, Any]) -> bool:
    actual = order.get("actual_fill_size")
    filled = order.get("filled_size")
    if actual is None or filled is None:
        return False
    return _decimal(actual) == _decimal(filled)


def _row_has_timestamp(row: Mapping[str, Any]) -> bool:
    for key in ("event_ts_ms", "timestamp_ms", "timestamp", "block_timestamp", "block_time", "created_at"):
        if row.get(key) not in (None, ""):
            return True
    return False


if __name__ == "__main__":
    raise SystemExit(main())
