"""Validation helpers for OrderFilled-only fill-trade replay.

These helpers validate the model's declared boundary: fills must come from
real trade-print evidence, not from L2/L3 depth or OHLCV bars.
"""

from __future__ import annotations

import hashlib
import json
import random
import resource
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from time import perf_counter
from typing import Any, Mapping, Sequence

from quant.core.db import ClickHouseClient, postgres_connection

from .orderfilled_v2_compare import compare_replay_results
from .orderfilled_v2_replay import (
    CapacityLedger,
    V2MakerOrder,
    V2OrderResult,
    V2TakerOrder,
    V2TradePrint,
    build_v2_replay_report,
    load_v2_trade_prints,
    load_v2_wallet_fill_ticks,
    replay_v2_maker_order,
    replay_v2_taker_order,
    replay_v2_taker_orders_reference,
    replay_v2_taker_orders_with_diagnostics,
    summarize_v2_results,
    wallet_fill_to_observed_order,
    with_v2_execution_profile,
)


Q = Decimal("0.0000000001")
VALIDATION_OUT_DIR = (
    Path(__file__).resolve().parents[2] / "runtime_outputs" / "fill_trade_validation"
)


@dataclass(frozen=True)
class ValidationCheck:
    name: str
    status: str
    detail: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "status": self.status, "detail": self.detail}


def check(name: str, ok: bool, detail: Any = "") -> ValidationCheck:
    return ValidationCheck(
        name=name, status="PASS" if ok else "FAIL", detail=str(detail)
    )


def review(name: str, detail: Any = "") -> ValidationCheck:
    return ValidationCheck(name=name, status="REVIEW", detail=str(detail))


def skipped(name: str, detail: Any = "") -> ValidationCheck:
    return ValidationCheck(name=name, status="SKIPPED", detail=str(detail))


def status_from_checks(
    checks: Sequence[ValidationCheck], *, strict_review: bool = False
) -> str:
    if any(row.status == "FAIL" for row in checks):
        return "fail"
    if strict_review and any(row.status in {"REVIEW", "SKIPPED"} for row in checks):
        return "fail"
    if any(row.status in {"REVIEW", "SKIPPED"} for row in checks):
        return "review"
    return "pass"


def write_report(
    payload: Mapping[str, Any], *, output_json: Path, output_md: Path | None = None
) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    if output_md is not None:
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(markdown_report(payload), encoding="utf-8")


def markdown_report(payload: Mapping[str, Any]) -> str:
    checks = payload.get("checks") or []
    lines = [
        f"# {payload.get('title', 'Fill Trade Validation')}",
        "",
        f"Status: **{payload.get('status')}**",
        "",
        "This validates fill-evidence constrained execution, not full L2/L3 queue accuracy.",
        "",
        "## Summary",
        "",
    ]
    summary = (
        payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    )
    if summary:
        lines.extend(
            f"- {key}: `{_brief(value)}`"
            for key, value in summary.items()
            if key != "comparisons"
        )
    else:
        lines.append("- no summary")
    lines.extend(["", "## Checks", ""])
    if checks:
        lines.extend(
            f"- {row.get('status')} `{row.get('name')}`: {_brief(row.get('detail', ''))}"
            for row in checks
        )
    else:
        lines.append("- no checks")
    artifacts = (
        payload.get("artifacts")
        if isinstance(payload.get("artifacts"), Mapping)
        else {}
    )
    if artifacts:
        lines.extend(["", "## Artifacts", ""])
        lines.extend(f"- {key}: `{_brief(value)}`" for key, value in artifacts.items())
    comparisons = summary.get("comparisons") if isinstance(summary, Mapping) else None
    if (
        isinstance(comparisons, Sequence)
        and not isinstance(comparisons, (str, bytes))
        and comparisons
    ):
        lines.extend(
            [
                "",
                "## LOB Holdout Comparisons",
                "",
                "| verdict | market | order | fill-only | depth+fill | price delta | size delta | source |",
                "| --- | --- | --- | --- | --- | ---: | ---: | --- |",
            ]
        )
        for row in comparisons[:50]:
            if not isinstance(row, Mapping):
                continue
            sample = row.get("sample") if isinstance(row.get("sample"), Mapping) else {}
            fill_only = (
                row.get("fill_only")
                if isinstance(row.get("fill_only"), Mapping)
                else {}
            )
            depth = row.get("depth") if isinstance(row.get("depth"), Mapping) else {}
            order = row.get("order") if isinstance(row.get("order"), Mapping) else {}
            lines.append(
                "| "
                f"{row.get('verdict')} | "
                f"`{sample.get('market_slug')}` | "
                f"{order.get('side')} {order.get('size')} @ {order.get('limit_price')} | "
                f"{fill_only.get('status')} {fill_only.get('filled_size')} @ {fill_only.get('avg_price')} | "
                f"{depth.get('status')} {depth.get('filled_size')} @ {depth.get('avg_price')} | "
                f"{row.get('price_delta')} | "
                f"{row.get('size_delta')} | "
                f"`{str(sample.get('tx_hash') or '')[:12]}:{sample.get('log_index')}` |"
            )
        if len(comparisons) > 50:
            lines.append(f"| ... | omitted {len(comparisons) - 50} rows | | | | | | |")
    return "\n".join(lines) + "\n"


def summarize_checks(checks: Sequence[ValidationCheck]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in checks:
        counts[row.status] = counts.get(row.status, 0) + 1
    return dict(sorted(counts.items()))


def validate_orderfilled_data_contract(
    *,
    client: ClickHouseClient | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    chain_id: int = 137,
) -> dict[str, Any]:
    ch = client or ClickHouseClient()
    tables = [
        "raw_orderfilled",
        "maker_fill_ticks",
        "orderfilled_quarantine",
        "trade_prints_one_sided",
        "block_trade_bars_sparse",
        "time_bars",
    ]
    checks: list[ValidationCheck] = []
    exists = {table: _table_exists(ch, table) for table in tables}
    checks.extend(
        check(f"table exists: {table}", ok, ok) for table, ok in exists.items()
    )
    if not all(exists.values()):
        return _payload(
            "OrderFilled Data Contract",
            checks,
            {"tables": exists, "reason": "missing tables"},
        )

    predicates = _block_predicate(from_block, to_block)
    raw_predicates = predicates
    counts = {
        table: _count(ch, table, raw_predicates if table != "time_bars" else "")
        for table in tables
    }
    source_counts = _one(
        ch,
        f"""
        SELECT
            count() AS physical_rows,
            uniqExact(tuple(
                lower(tx_hash),
                log_index,
                market_id,
                lower(token_id),
                lower(maker),
                lower(taker),
                side_code
            )) AS canonical_rows
        FROM orderfilled_fact
        WHERE 1=1{predicates}
        """,
    )
    source_physical_count = int(source_counts.get("physical_rows") or 0)
    source_canonical_count = int(source_counts.get("canonical_rows") or 0)
    quality = {
        "raw": _one(
            ch,
            f"""
            SELECT
                countIf(source_price <= 0 OR source_price > 1) AS invalid_price_count,
                countIf(source_size <= 0) AS invalid_size_count,
                countIf(length(tx_hash) = 0 OR length(order_hash) = 0) AS missing_key_count,
                count() - uniqExact(chain_id, block_number, tx_hash, log_index, order_hash) AS duplicate_tx_log_orderhash_count,
                countIf(block_time IS NULL) AS missing_timestamp_count
            FROM raw_orderfilled
            WHERE chain_id = toUInt64({int(chain_id)}){predicates}
            """,
        ),
        "maker": _one(
            ch,
            f"""
            SELECT
                countIf(price <= 0 OR price > 1) AS invalid_price_count,
                countIf(size_shares <= 0) AS invalid_size_count,
                countIf(market_id = 0 OR length(condition_id) = 0 OR length(asset_id) = 0) AS missing_mapping_count,
                countIf(passive_side NOT IN ('BUY', 'SELL') OR aggressor_side NOT IN ('BUY', 'SELL')) AS invalid_side_count
            FROM maker_fill_ticks
            WHERE chain_id = toUInt64({int(chain_id)}){predicates}
            """,
        ),
        "trade_prints": _one(
            ch,
            f"""
            SELECT
                countIf(price <= 0 OR price > 1) AS invalid_price_count,
                countIf(size_shares <= 0) AS invalid_size_count,
                countIf(market_id = 0 OR length(condition_id) = 0 OR length(asset_id) = 0) AS missing_mapping_count,
                countIf(aggressor_side NOT IN ('BUY', 'SELL') OR passive_side NOT IN ('BUY', 'SELL')) AS invalid_side_count,
                countIf(source_fill_count = 0 OR length(source_fill_ids) = 0) AS missing_source_fill_count,
                sum(source_fill_count) AS source_fill_count_sum,
                count() - uniqExact(trade_id) AS duplicate_trade_id_count
            FROM trade_prints_one_sided
            WHERE chain_id = toUInt64({int(chain_id)}){predicates}
            """,
        ),
    }
    side_samples = _rows(
        ch,
        f"""
        SELECT passive_side, aggressor_side, count() AS rows
        FROM maker_fill_ticks
        WHERE chain_id = toUInt64({int(chain_id)}){predicates}
        GROUP BY passive_side, aggressor_side
        ORDER BY rows DESC
        """,
    )

    checks.extend(
        [
            check(
                "raw_orderfilled_count > 0",
                counts["raw_orderfilled"] > 0,
                counts["raw_orderfilled"],
            ),
            check(
                "maker_fill_ticks_count > 0",
                counts["maker_fill_ticks"] > 0,
                counts["maker_fill_ticks"],
            ),
            check(
                "trade_prints_one_sided_count > 0",
                counts["trade_prints_one_sided"] > 0,
                counts["trade_prints_one_sided"],
            ),
            check(
                "raw matches canonical source orderfilled_fact",
                counts["raw_orderfilled"] == source_canonical_count,
                (
                    f"raw={counts['raw_orderfilled']} "
                    f"canonical={source_canonical_count} physical={source_physical_count}"
                ),
            ),
            check(
                "maker + quarantine equals raw",
                counts["maker_fill_ticks"] + counts["orderfilled_quarantine"]
                == counts["raw_orderfilled"],
                counts,
            ),
            check(
                "trade prints do not exceed maker ticks",
                counts["trade_prints_one_sided"] <= counts["maker_fill_ticks"],
                counts,
            ),
            check(
                "trade source fill sum matches maker ticks",
                _dec(quality["trade_prints"].get("source_fill_count_sum"))
                == _dec(counts["maker_fill_ticks"]),
                quality["trade_prints"].get("source_fill_count_sum"),
            ),
            check(
                "raw invalid price rows = 0",
                _dec(quality["raw"].get("invalid_price_count")) == 0,
                quality["raw"].get("invalid_price_count"),
            ),
            check(
                "raw invalid size rows = 0",
                _dec(quality["raw"].get("invalid_size_count")) == 0,
                quality["raw"].get("invalid_size_count"),
            ),
            check(
                "raw duplicate tx/log/orderhash rows = 0",
                _dec(quality["raw"].get("duplicate_tx_log_orderhash_count")) == 0,
                quality["raw"].get("duplicate_tx_log_orderhash_count"),
            ),
            check(
                "trade_prints invalid price rows = 0",
                _dec(quality["trade_prints"].get("invalid_price_count")) == 0,
                quality["trade_prints"].get("invalid_price_count"),
            ),
            check(
                "trade_prints invalid size rows = 0",
                _dec(quality["trade_prints"].get("invalid_size_count")) == 0,
                quality["trade_prints"].get("invalid_size_count"),
            ),
            check(
                "trade_prints missing source fill rows = 0",
                _dec(quality["trade_prints"].get("missing_source_fill_count")) == 0,
                quality["trade_prints"].get("missing_source_fill_count"),
            ),
            check(
                "trade_prints duplicate trade_id rows = 0",
                _dec(quality["trade_prints"].get("duplicate_trade_id_count")) == 0,
                quality["trade_prints"].get("duplicate_trade_id_count"),
            ),
            check(
                "side mapping emits BUY/SELL only",
                all(
                    row.get("passive_side") in {"BUY", "SELL"}
                    and row.get("aggressor_side") in {"BUY", "SELL"}
                    for row in side_samples
                ),
                side_samples,
            ),
        ]
    )
    summary = {
        "from_block": from_block,
        "to_block": to_block,
        "counts": counts,
        "source_counts": {
            "physical_orderfilled_fact": source_physical_count,
            "canonical_orderfilled_fact": source_canonical_count,
            "duplicate_extra_rows": max(
                0, source_physical_count - source_canonical_count
            ),
        },
        "quality": quality,
        "side_samples": side_samples,
    }
    return _payload("OrderFilled Data Contract", checks, summary)


def run_golden_fixtures() -> dict[str, Any]:
    checks: list[ValidationCheck] = []
    results: dict[str, Any] = {}
    base_order = _order("BUY", "0.53", size="100", participation="0.10")

    buy_result = replay_v2_taker_order(
        base_order,
        [
            _trade("buy", 100, "BUY", "0.52", "1000"),
            _trade("sell", 101, "SELL", "0.51", "1000"),
        ],
    )
    results["buy_direction"] = buy_result.as_dict()
    checks.append(
        check(
            "BUY taker uses only BUY aggressor trades",
            [fill.source_trade_id for fill in buy_result.fills] == ["buy"],
            buy_result.as_dict(),
        )
    )
    checks.append(
        check(
            "BUY taker fill respects participation cap",
            buy_result.filled_size == Decimal("100.0000000000"),
            buy_result.filled_size,
        )
    )

    sell_result = replay_v2_taker_order(
        _order("SELL", "0.50", size="100", participation="0.10"),
        [
            _trade("buy2", 100, "BUY", "0.52", "1000"),
            _trade("sell2", 101, "SELL", "0.51", "1000"),
        ],
    )
    results["sell_direction"] = sell_result.as_dict()
    checks.append(
        check(
            "SELL taker uses only SELL aggressor trades",
            [fill.source_trade_id for fill in sell_result.fills] == ["sell2"],
            sell_result.as_dict(),
        )
    )

    limit_result = replay_v2_taker_order(
        base_order,
        [
            _trade("cheap", 100, "BUY", "0.52", "1000"),
            _trade("expensive", 101, "BUY", "0.55", "1000"),
        ],
    )
    results["limit"] = limit_result.as_dict()
    checks.append(
        check(
            "BUY limit excludes price above limit",
            [fill.source_trade_id for fill in limit_result.fills] == ["cheap"],
            limit_result.as_dict(),
        )
    )

    latency_order = V2TakerOrder(
        **{
            **base_order.__dict__,
            "order_id": "latency",
            "signal_block": 9,
            "latency_blocks": 2,
            "signal_ts": _ts(9),
            "latency": timedelta(seconds=2),
        }
    )
    latency_result = replay_v2_taker_order(
        latency_order,
        [
            _trade("too-early", 10, "BUY", "0.52", "1000", seconds=10),
            _trade("after-arrival", 12, "BUY", "0.52", "1000", seconds=12),
        ],
    )
    results["latency"] = latency_result.as_dict()
    checks.append(
        check(
            "latency/no-lookahead excludes pre-arrival trade",
            [fill.source_trade_id for fill in latency_result.fills]
            == ["after-arrival"],
            latency_result.as_dict(),
        )
    )

    shared_trade = [_trade("shared", 100, "BUY", "0.52", "1000")]
    first = V2TakerOrder(
        **{**base_order.__dict__, "order_id": "first", "size": Decimal("80")}
    )
    second = V2TakerOrder(
        **{**base_order.__dict__, "order_id": "second", "size": Decimal("80")}
    )
    batch_results, ledger, diagnostics = replay_v2_taker_orders_with_diagnostics(
        [first, second], shared_trade
    )
    results["capacity"] = [row.as_dict() for row in batch_results]
    checks.append(
        check(
            "shared capacity ledger caps two orders at 100",
            [row.filled_size for row in batch_results]
            == [Decimal("80.0000000000"), Decimal("20.0000000000")],
            results["capacity"],
        )
    )
    checks.append(
        check(
            "indexed matcher scans candidate trades, not full cartesian path",
            diagnostics.candidate_rows_scanned <= diagnostics.naive_rows_scanned,
            diagnostics.as_dict(),
        )
    )

    no_trade = replay_v2_taker_order(base_order, [])
    results["no_trade"] = no_trade.as_dict()
    checks.append(
        check(
            "no trade evidence means NO_FILL",
            no_trade.status == "NO_FILL" and no_trade.filled_size == 0,
            no_trade.as_dict(),
        )
    )

    ohlcv_order = V2TakerOrder(
        **{
            **base_order.__dict__,
            "order_id": "ohlcv-cheat",
            "size": Decimal("1000"),
            "limit_price": Decimal("0.70"),
            "signal_block": 999,
            "signal_ts": _ts(999),
            "horizon_blocks": 2,
        }
    )
    ohlcv_result = replay_v2_taker_order(
        ohlcv_order,
        [
            _trade("tiny-buy", 1000, "BUY", "0.70", "5"),
            _trade("huge-sell", 1000, "SELL", "0.55", "9995"),
        ],
    )
    results["ohlcv_anti_cheat"] = ohlcv_result.as_dict()
    checks.append(
        check(
            "OHLCV anti-cheat caps BUY by actual BUY trade only",
            ohlcv_result.filled_size == Decimal("0.5000000000"),
            ohlcv_result.as_dict(),
        )
    )

    maker = V2MakerOrder(
        order_id="maker-strict",
        market_id=1,
        asset_id="token-yes",
        side="BUY",
        limit_price=Decimal("0.45"),
        size=Decimal("10"),
        signal_block=99,
        signal_ts=_ts(99),
        latency=timedelta(0),
        horizon=timedelta(minutes=5),
        horizon_blocks=10,
    )
    maker_result = replay_v2_maker_order(
        maker, [_trade("seller-hit", 100, "SELL", "0.45", "1000")], "strict_audit"
    )
    results["maker_strict"] = maker_result.as_dict()
    checks.append(
        check(
            "maker strict mode produces zero fills",
            maker_result.filled_size == 0 and not maker_result.fills,
            maker_result.as_dict(),
        )
    )

    return _payload("Fill Trade Golden Fixtures", checks, {"results": results})


def compare_reference_vs_indexed(
    *, trades_count: int, orders_count: int, seed: int
) -> dict[str, Any]:
    orders, trades = synthetic_orders_and_trades(
        trades_count=trades_count, orders_count=orders_count, seed=seed
    )
    reference_results, reference_ledger = replay_v2_taker_orders_reference(
        orders, trades
    )
    indexed_results, indexed_ledger, diagnostics = (
        replay_v2_taker_orders_with_diagnostics(orders, trades)
    )
    comparison = compare_replay_results(
        reference_results, reference_ledger, indexed_results, indexed_ledger
    )
    checks = [
        check(
            "reference vs indexed diff_count = 0",
            int(comparison["diff_count"]) == 0,
            comparison,
        ),
        check(
            "reference hash equals indexed hash",
            comparison["reference_hash"] == comparison["indexed_hash"],
            comparison,
        ),
        check(
            "candidate scan less or equal naive scan",
            diagnostics.candidate_rows_scanned <= diagnostics.naive_rows_scanned,
            diagnostics.as_dict(),
        ),
    ]
    return _payload(
        "Fill Trade Reference vs Indexed",
        checks,
        {
            "trades_count": trades_count,
            "orders_count": orders_count,
            "seed": seed,
            "comparison": comparison,
            "diagnostics": diagnostics.as_dict(),
        },
    )


def randomized_reference_vs_indexed(
    *, cases: int, trades_count: int, orders_count: int, seed: int
) -> dict[str, Any]:
    checks: list[ValidationCheck] = []
    case_rows: list[dict[str, Any]] = []
    rng = random.Random(seed)
    for index in range(max(1, cases)):
        case_seed = rng.randint(1, 10_000_000)
        payload = compare_reference_vs_indexed(
            trades_count=trades_count, orders_count=orders_count, seed=case_seed
        )
        case_rows.append(
            {
                "case": index,
                "seed": case_seed,
                "status": payload["status"],
                "summary": payload["summary"],
            }
        )
        checks.append(
            check(
                f"randomized case {index} diff_count = 0",
                payload["status"] == "pass",
                case_rows[-1],
            )
        )
    return _payload(
        "Fill Trade Randomized Reference vs Indexed", checks, {"cases": case_rows}
    )


def validate_execution_invariants(
    *,
    orders: Sequence[V2TakerOrder],
    results: Sequence[V2OrderResult],
    ledger: CapacityLedger | Mapping[str, Any],
    trades: Sequence[V2TradePrint] = (),
    require_fills: bool = False,
) -> dict[str, Any]:
    checks: list[ValidationCheck] = []
    order_by_id = {row.order_id: row for row in orders}
    trade_by_id = {row.trade_id: row for row in trades}
    violations: dict[str, list[str]] = {
        "order_size": [],
        "wrong_side": [],
        "limit": [],
        "lookahead": [],
        "missing_source": [],
        "source_trade_mismatch": [],
        "non_positive_fill": [],
        "capacity_overuse": [],
    }
    fill_count = 0
    for result in results:
        order_row = order_by_id.get(result.order_id)
        if result.filled_size > result.requested_size:
            violations["order_size"].append(result.order_id)
        for fill in result.fills:
            fill_count += 1
            if fill.filled_size <= 0:
                violations["non_positive_fill"].append(result.order_id)
            if order_row and fill.side != order_row.side:
                violations["wrong_side"].append(result.order_id)
            if fill.side == "BUY" and fill.exec_price > fill.limit_price:
                violations["limit"].append(result.order_id)
            if fill.side == "SELL" and fill.exec_price < fill.limit_price:
                violations["limit"].append(result.order_id)
            if (
                order_row
                and order_row.arrival_block is not None
                and fill.fill_block < order_row.arrival_block
            ):
                violations["lookahead"].append(result.order_id)
            if (
                order_row
                and order_row.arrival_ts is not None
                and fill.fill_ts < order_row.arrival_ts
            ):
                violations["lookahead"].append(result.order_id)
            if (
                not fill.source_trade_id
                or not fill.source_tx_hash
                or not fill.source_log_indexes
            ):
                violations["missing_source"].append(result.order_id)
            source = trade_by_id.get(fill.source_trade_id)
            if source:
                if (
                    source.aggressor_side != fill.side
                    or source.price != fill.historical_price
                    or source.size != fill.historical_size
                ):
                    violations["source_trade_mismatch"].append(result.order_id)
    ledger_map = (
        ledger.as_dict()
        if isinstance(ledger, CapacityLedger)
        else {str(key): str(value) for key, value in dict(ledger).items()}
    )
    for trade in trades:
        used = _dec(ledger_map.get(trade.trade_id))
        max_rate = max(
            (_dec(result.participation_rate) for result in results),
            default=Decimal("0"),
        )
        cap = (trade.size * max_rate).quantize(Q, rounding=ROUND_HALF_UP)
        if used > cap + Q:
            violations["capacity_overuse"].append(
                f"{trade.trade_id} used={used} cap={cap}"
            )

    checks.append(
        check(
            "fill_count > 0" if require_fills else "fill_count recorded",
            fill_count > 0 if require_fills else fill_count >= 0,
            fill_count,
        )
    )
    for name, rows in violations.items():
        checks.append(check(f"{name} violations = 0", not rows, _sample(rows)))
    comparable = {
        "orders": [row.as_dict() for row in results],
        "ledger": ledger_map,
    }
    digest = hashlib.sha256(
        json.dumps(_plain(comparable), ensure_ascii=True, sort_keys=True).encode()
    ).hexdigest()
    checks.append(check("determinism hash generated", len(digest) == 64, digest))
    return _payload(
        "Fill Trade Execution Invariants",
        checks,
        {
            "orders_count": len(orders),
            "results_count": len(results),
            "fill_count": fill_count,
            "determinism_hash": digest,
        },
    )


def validate_latest_v2_run_from_postgres(
    *, run_id: int | None = None
) -> dict[str, Any]:
    try:
        with postgres_connection(readonly=True) as conn:
            _set_statement_timeout(conn)
            resolved_run_id = run_id or _latest_v2_run_id(conn)
            if resolved_run_id is None:
                return _payload(
                    "Persisted V2 Run Invariants",
                    [
                        review(
                            "latest V2 run exists", "no ORDERFILLED_V2_TAPE run found"
                        )
                    ],
                    {},
                )
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM quant.quant_backtest_orders WHERE run_id = %s ORDER BY signal_index ASC, order_id ASC",
                    (resolved_run_id,),
                )
                order_rows = cur.fetchall()
                cur.execute(
                    "SELECT * FROM quant.quant_backtest_ledger WHERE run_id = %s",
                    (resolved_run_id,),
                )
                ledger_rows = cur.fetchall()
    except Exception as exc:
        return _payload(
            "Persisted V2 Run Invariants",
            [review("Postgres V2 run validation queryable", exc)],
            {},
        )
    checks = [
        check("persisted run_id present", resolved_run_id is not None, resolved_run_id),
        check("orders persisted", len(order_rows) > 0, len(order_rows)),
        check("ledger rows queryable", len(ledger_rows) >= 0, len(ledger_rows)),
        check(
            "NO_FILL is not persisted as REJECTED",
            not any(
                str(row.get("status")).upper() == "REJECTED"
                and _dec(row.get("filled_size")) == 0
                for row in order_rows
            ),
            "zero-fill rejected rows",
        ),
        check(
            "filled persisted orders have source trade evidence",
            all(
                _persisted_order_has_source(row)
                for row in order_rows
                if _dec(row.get("filled_size")) > 0
            ),
            "filled order source audit",
        ),
    ]
    return _payload(
        "Persisted V2 Run Invariants",
        checks,
        {
            "run_id": resolved_run_id,
            "orders_count": len(order_rows),
            "ledger_count": len(ledger_rows),
        },
    )


def run_trade_print_plumbing_validation(
    *,
    market_id: int | None = None,
    asset_id: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    sample_limit: int = 20,
    wallet: str | None = None,
) -> dict[str, Any]:
    checks: list[ValidationCheck] = []
    trades: list[V2TradePrint] = []
    source = "trade_prints_one_sided"
    if wallet:
        ticks = load_v2_wallet_fill_ticks(
            wallet=wallet,
            market_id=market_id,
            asset_id=asset_id,
            from_block=from_block,
            to_block=to_block,
            limit=sample_limit,
        )
        orders = [
            wallet_fill_to_observed_order(
                row, size_fraction=Decimal("1"), allowed_buffer=Decimal("0")
            )
            for row in ticks
        ]
        source = "maker_fill_ticks"
        if not ticks:
            checks.append(review("wallet fill sample exists", f"wallet={wallet}"))
            return _payload(
                "Wallet / Real Fill Plumbing Validation", checks, {"source": source}
            )
        trades = [
            V2TradePrint(
                trade_id=row.fill_id,
                market_id=row.market_id,
                condition_id=row.condition_id,
                asset_id=row.asset_id,
                outcome=row.outcome,
                block_number=row.block_number,
                block_time=row.block_time,
                tx_hash=row.tx_hash,
                tx_index=0,
                tx_index_source="wallet_fill_tick",
                price=row.price,
                size=row.size,
                notional=(row.price * row.size).quantize(Q),
                aggressor_side=row.aggressor_side,
                passive_side=row.passive_side,
                source_log_indexes=(row.log_index,),
                source_fill_count=1,
            )
            for row in ticks
        ]
    else:
        if (
            market_id is None
            or asset_id is None
            or from_block is None
            or to_block is None
        ):
            sample = _sample_trade_print_context(sample_limit=1)
            if not sample:
                checks.append(
                    review(
                        "trade print sample context exists",
                        "no trade_prints_one_sided rows found",
                    )
                )
                return _payload(
                    "Wallet / Real Fill Plumbing Validation", checks, {"source": source}
                )
            first = sample[0]
            market_id = int(first["market_id"])
            asset_id = str(first["asset_id"])
            from_block = int(first["block_number"])
            to_block = int(first["block_number"])
        trades = load_v2_trade_prints(
            market_id=int(market_id),
            asset_id=str(asset_id),
            from_block=int(from_block),
            to_block=int(to_block),
            limit=sample_limit,
        )
        if not trades and _looks_decimal_token_id(asset_id):
            asset_hex = _decimal_token_to_hex(asset_id)
            trades = load_v2_trade_prints(
                market_id=int(market_id),
                asset_id=asset_hex,
                from_block=int(from_block),
                to_block=int(to_block),
                limit=sample_limit,
            )
            if trades:
                asset_id = asset_hex
        orders = [
            V2TakerOrder(
                order_id=f"plumbing-{trade.trade_id}",
                market_id=trade.market_id,
                asset_id=trade.asset_id,
                side=trade.aggressor_side,
                limit_price=trade.price,
                size=trade.size,
                signal_block=max(0, trade.block_number - 1),
                signal_ts=trade.block_time - timedelta(seconds=1),
                latency=timedelta(0),
                horizon=timedelta(seconds=2),
                horizon_blocks=2,
                participation_rate=Decimal("1"),
                price_buffer=Decimal("0"),
            )
            for trade in trades
        ]
    results, ledger, diagnostics = replay_v2_taker_orders_with_diagnostics(
        orders, trades
    )
    checks.extend(
        [
            check("real evidence sample loaded", len(trades) > 0, len(trades)),
            check(
                "plumbing mode replays at least one source fill",
                any(row.filled_size > 0 for row in results),
                [row.as_dict() for row in results[:3]],
            ),
            check(
                "all fills point to source trade ids",
                all(fill.source_trade_id for row in results for fill in row.fills),
                "source_trade_id",
            ),
        ]
    )
    invariant = validate_execution_invariants(
        orders=orders, results=results, ledger=ledger, trades=trades
    )
    checks.extend(
        ValidationCheck(f"invariant: {row['name']}", row["status"], row["detail"])
        for row in invariant["checks"]
    )
    return _payload(
        "Wallet / Real Fill Plumbing Validation",
        checks,
        {
            "source": source,
            "market_id": market_id,
            "asset_id": asset_id,
            "from_block": from_block,
            "to_block": to_block,
            "orders_count": len(orders),
            "trades_count": len(trades),
            "diagnostics": diagnostics.as_dict(),
            "summary": summarize_v2_results(results),
        },
    )


def run_lob_holdout_validation(
    *,
    market_slugs: Sequence[str] | None = None,
    build_tag: str = "pmxt_v2_team_vs_team_main_lifecycle",
    per_market: int = 1,
    lookback_hours: int = 6,
    book_ttl_ms: int = 300_000,
    depth_haircut: Decimal = Decimal("1"),
    price_tolerance: Decimal = Decimal("0.000001"),
    order_side: str = "SAMPLE",
    fill_only_profile: str = "conservative_trade_tape",
    max_false_positive_rate: Decimal = Decimal("0.05"),
    max_overfill_rate: Decimal = Decimal("0.05"),
    max_adverse_price_error: Decimal = Decimal("0.005"),
    min_precision: Decimal = Decimal("0.90"),
    discover_market_limit: int | None = None,
) -> dict[str, Any]:
    """Compare the same observed order through fill-only and L2-depth replay.

    This is an observed-fill holdout. It does not use the production 2.5%
    participation cap; it asks whether the same concrete historical order can
    be explained by the fill tape and by the L2 book around the fill.
    """

    checks: list[ValidationCheck] = []
    depth_haircut = _dec(depth_haircut)
    price_tolerance = _dec(price_tolerance)
    try:
        from experiments.nba_l2_orderfilled.validate_team_vs_team_depth_fill_samples import (
            DEFAULT_MARKET_SLUGS,
            clone_model_from_events,
            load_fill_samples,
            load_l2_events_for_sample,
            load_mappings,
            ms_to_dt,
        )
        from quant.backtest.l2_orderfilled_execution import StrategyOrderIntent

        discovered = (
            discover_lob_holdout_market_slugs(
                build_tag=build_tag, limit=discover_market_limit
            )
            if discover_market_limit
            else []
        )
        slugs = tuple(market_slugs or discovered or DEFAULT_MARKET_SLUGS)
        mappings = load_mappings(slugs)
        ch = ClickHouseClient()
        samples = load_fill_samples(
            ch,
            mappings=mappings,
            build_tag=build_tag,
            per_market=max(1, int(per_market)),
        )
        comparisons = []
        for sample in samples:
            events = load_l2_events_for_sample(
                ch,
                sample,
                build_tag=build_tag,
                lookback_hours=max(1, int(lookback_hours)),
            )
            side = _holdout_order_side(sample.side, order_side)
            limit = _holdout_limit_price(sample.trade_price, side, price_tolerance)
            fill_only = _run_fill_only_holdout(
                sample,
                side=side,
                limit=limit,
                price_tolerance=price_tolerance,
                profile=fill_only_profile,
                client=ch,
                lookback_hours=max(1, int(lookback_hours)),
            )
            depth = _run_depth_holdout(
                sample,
                events,
                side=side,
                limit=limit,
                book_ttl_ms=max(1, int(book_ttl_ms)),
                depth_haircut=depth_haircut,
                intent_cls=StrategyOrderIntent,
                clone_model_from_events=clone_model_from_events,
                ms_to_dt=ms_to_dt,
            )
            comparisons.append(
                _holdout_comparison_row(
                    sample,
                    side=side,
                    limit=limit,
                    fill_only=fill_only,
                    depth=depth,
                    price_tolerance=price_tolerance,
                )
            )
    except Exception as exc:
        return _payload(
            "Fill Trade LOB Holdout Validation",
            [review("order-level LOB holdout queryable", exc)],
            {"order_side": order_side},
        )

    summary = _summarize_holdout_comparisons(comparisons)
    checks.extend(
        [
            check(
                "real LOB holdout samples loaded",
                len(comparisons) > 0,
                len(comparisons),
            ),
            check(
                "fill-only replay attempted for every sample",
                all(row.get("fill_only", {}).get("status") for row in comparisons),
                summary,
            ),
            check(
                "depth replay attempted for every sample",
                all(row.get("depth", {}).get("status") for row in comparisons),
                summary,
            ),
        ]
    )
    if str(fill_only_profile) != "plumbing_replay":
        checks.extend(
            _lob_holdout_quality_gate_checks(
                summary,
                max_false_positive_rate=max_false_positive_rate,
                max_overfill_rate=max_overfill_rate,
                max_adverse_price_error=max_adverse_price_error,
                min_precision=min_precision,
            )
        )
    return _payload(
        "Fill Trade LOB Holdout Validation",
        checks,
        {
            "mode": "order_level_side_by_side",
            "build_tag": build_tag,
            "market_slugs": list(market_slugs or []),
            "discovered_market_slugs": discovered,
            "effective_market_slugs": list(slugs),
            "per_market": int(per_market),
            "lookback_hours": int(lookback_hours),
            "book_ttl_ms": int(book_ttl_ms),
            "depth_haircut": str(depth_haircut),
            "price_tolerance": str(price_tolerance),
            "order_side": order_side.upper(),
            "fill_only_profile": fill_only_profile,
            "max_false_positive_rate": str(max_false_positive_rate),
            "max_overfill_rate": str(max_overfill_rate),
            "max_adverse_price_error": str(max_adverse_price_error),
            "min_precision": str(min_precision),
            **summary,
            "comparisons": comparisons,
        },
    )


def discover_lob_holdout_market_slugs(
    *, build_tag: str, limit: int | None = None, client: ClickHouseClient | None = None
) -> list[str]:
    rows = _query_lob_holdout_market_rows(
        build_tag=build_tag, limit=limit, client=client
    )
    return [
        str(row.get("market_slug") or "")
        for row in rows
        if str(row.get("market_slug") or "").strip()
    ]


def build_lob_holdout_calibration_plan(
    *,
    build_tag: str,
    target_samples: int = 1000,
    discover_market_limit: int | None = None,
    client: ClickHouseClient | None = None,
) -> dict[str, Any]:
    rows = _query_lob_holdout_market_rows(
        build_tag=build_tag, limit=discover_market_limit, client=client
    )
    return _lob_holdout_plan_from_rows(rows, target_samples=target_samples)


def _query_lob_holdout_market_rows(
    *, build_tag: str, limit: int | None = None, client: ClickHouseClient | None = None
) -> list[dict[str, Any]]:
    ch = client or ClickHouseClient()
    limit_sql = f"LIMIT {max(1, int(limit))}" if limit else ""
    return ch.query_json_rows(
        f"""
        WITH
            l2 AS (
                SELECT
                    market_id,
                    anyLast(market_slug) AS market_slug,
                    count() AS l2_rows,
                    uniqExact(token_id) AS l2_tokens
                FROM pmxt_l2_event_replay
                WHERE build_tag = {_quote(build_tag)}
                GROUP BY market_id
            ),
            of AS (
                SELECT
                    market_id,
                    count() AS orderfilled_rows
                FROM orderfilled_trade_replay
                WHERE build_tag = {_quote(build_tag)}
                GROUP BY market_id
            )
        SELECT
            l2.market_slug AS market_slug,
            l2.market_id AS market_id,
            l2.l2_rows AS l2_rows,
            of.orderfilled_rows AS orderfilled_rows
        FROM l2
        INNER JOIN of ON of.market_id = l2.market_id
        WHERE l2.market_slug != ''
          AND of.orderfilled_rows > 0
        ORDER BY of.orderfilled_rows DESC, l2.l2_rows DESC, l2.market_id ASC
        {limit_sql}
        """,
        timeout_seconds=120,
    )


def _lob_holdout_plan_from_rows(
    rows: Sequence[Mapping[str, Any]], *, target_samples: int = 1000
) -> dict[str, Any]:
    clean_rows = [
        {
            "market_slug": str(row.get("market_slug") or ""),
            "market_id": int(row.get("market_id") or 0),
            "l2_rows": int(row.get("l2_rows") or 0),
            "orderfilled_rows": int(row.get("orderfilled_rows") or 0),
        }
        for row in rows
        if str(row.get("market_slug") or "").strip()
        and int(row.get("orderfilled_rows") or 0) > 0
    ]
    market_count = len(clean_rows)
    target = max(1, int(target_samples))
    recommended_per_market = max(
        1, (target + max(1, market_count) - 1) // max(1, market_count)
    )
    expected_samples = sum(
        min(row["orderfilled_rows"], recommended_per_market) for row in clean_rows
    )
    available_samples = sum(row["orderfilled_rows"] for row in clean_rows)
    return {
        "schema_version": "fill_only_lob_calibration_plan_v1",
        "target_samples": target,
        "market_count": market_count,
        "available_samples": available_samples,
        "recommended_per_market": recommended_per_market if market_count else 0,
        "expected_samples_at_recommended_per_market": expected_samples,
        "sample_shortfall": max(0, target - expected_samples),
        "status": "ready"
        if expected_samples >= target
        else "limited_lob_holdout_scope",
        "market_slugs": [row["market_slug"] for row in clean_rows],
        "markets": clean_rows,
    }


def _lob_holdout_quality_gate_checks(
    summary: Mapping[str, Any],
    *,
    max_false_positive_rate: Decimal = Decimal("0.05"),
    max_overfill_rate: Decimal = Decimal("0.05"),
    max_adverse_price_error: Decimal = Decimal("0.005"),
    min_precision: Decimal = Decimal("0.90"),
) -> list[ValidationCheck]:
    return [
        check(
            "fill-only false-positive rate within LOB holdout gate",
            _dec(summary.get("false_positive_rate")) <= _dec(max_false_positive_rate),
            {"max_false_positive_rate": str(max_false_positive_rate), **summary},
        ),
        check(
            "fill-only precision within LOB holdout gate",
            _dec(summary.get("precision")) >= _dec(min_precision),
            {"min_precision": str(min_precision), **summary},
        ),
        check(
            "fill-only overfill rate within LOB holdout gate",
            _dec(summary.get("overfill_rate")) <= _dec(max_overfill_rate),
            {"max_overfill_rate": str(max_overfill_rate), **summary},
        ),
        check(
            "fill-only price advantage error within LOB holdout gate",
            _dec(summary.get("adverse_price_error")) <= _dec(max_adverse_price_error),
            {"max_adverse_price_error": str(max_adverse_price_error), **summary},
        ),
    ]


def run_lob_holdout_run_summary_validation(
    *, v2_run_id: int | None = None, l2_run_id: int | None = None
) -> dict[str, Any]:
    checks: list[ValidationCheck] = []
    try:
        with postgres_connection(readonly=True) as conn:
            _set_statement_timeout(conn)
            if v2_run_id is None:
                v2_run_id = _latest_v2_run_id(conn)
            if l2_run_id is None:
                l2_run_id = _latest_l2_run_id(conn)
            if v2_run_id is None or l2_run_id is None:
                checks.append(
                    skipped(
                        "V2 and L2 holdout runs exist",
                        f"v2_run_id={v2_run_id} l2_run_id={l2_run_id}",
                    )
                )
                return _payload(
                    "Fill Trade LOB Holdout Run Summary Validation",
                    checks,
                    {"v2_run_id": v2_run_id, "l2_run_id": l2_run_id},
                )
            v2 = _run_order_summary(conn, v2_run_id)
            l2 = _run_order_summary(conn, l2_run_id)
    except Exception as exc:
        return _payload(
            "Fill Trade LOB Holdout Run Summary Validation",
            [review("LOB holdout queryable", exc)],
            {"v2_run_id": v2_run_id, "l2_run_id": l2_run_id},
        )
    checks.extend(
        [
            check("V2 run has persisted orders", v2["orders"] > 0, v2),
            check("L2 run has persisted orders", l2["orders"] > 0, l2),
            review(
                "OrderFilled-only should not be obviously more optimistic than L2",
                f"v2_fill_rate={v2['fill_rate']} l2_fill_rate={l2['fill_rate']}",
            ),
        ]
    )
    if v2["orders"] > 0 and l2["orders"] > 0:
        checks.append(
            check(
                "V2 fill rate <= L2 fill rate + 20pp holdout tolerance",
                _dec(v2["fill_rate"]) <= _dec(l2["fill_rate"]) + Decimal("0.20"),
                {"v2": v2, "l2": l2},
            )
        )
    return _payload(
        "Fill Trade LOB Holdout Run Summary Validation",
        checks,
        {"v2_run_id": v2_run_id, "l2_run_id": l2_run_id, "v2": v2, "l2": l2},
    )


def _holdout_order_side(sample_side: str, requested: str) -> str:
    normalized = str(requested or "SAMPLE").upper()
    if normalized in {"SAMPLE", "HISTORICAL", "AUTO"}:
        normalized = str(sample_side or "BUY").upper()
    if normalized not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported holdout order_side={requested!r}")
    return normalized


def _holdout_limit_price(price: Decimal, side: str, tolerance: Decimal) -> Decimal:
    if side == "BUY":
        return min(Decimal("1"), _dec(price) + _dec(tolerance))
    return max(Decimal("0"), _dec(price) - _dec(tolerance))


def _run_fill_only_holdout(
    sample: Any,
    *,
    side: str,
    limit: Decimal,
    price_tolerance: Decimal,
    profile: str,
    client: ClickHouseClient,
    lookback_hours: int,
) -> dict[str, Any]:
    fill_ts = _holdout_fill_ts(sample)
    source_trade = V2TradePrint(
        trade_id=f"holdout:{sample.canonical_fill_key or sample.tx_hash}:{sample.log_index}",
        market_id=int(sample.market_id),
        condition_id="holdout",
        asset_id=str(sample.l2_token_id or sample.fill_token_id).lower(),
        outcome=str(sample.token_side or ""),
        block_number=int(sample.block_number),
        block_time=fill_ts,
        tx_hash=str(sample.tx_hash or ""),
        tx_index=int(sample.transaction_index),
        tx_index_source="orderfilled_trade_replay",
        price=_dec(sample.trade_price),
        size=_dec(sample.size),
        notional=(_dec(sample.trade_price) * _dec(sample.size)).quantize(Q),
        aggressor_side=str(sample.side).upper(),  # type: ignore[arg-type]
        passive_side="SELL" if str(sample.side).upper() == "BUY" else "BUY",  # type: ignore[arg-type]
        source_log_indexes=(int(sample.log_index),),
        source_fill_count=1,
    )
    if str(profile) == "plumbing_replay":
        trades = [source_trade]
        signal_source_trade_id = source_trade.trade_id
    else:
        from_block = max(
            0, int(sample.block_number) - max(1, int(lookback_hours)) * 1800
        )
        to_block = int(sample.block_number) + 600
        trades = _load_holdout_trade_tape(
            sample,
            client=client,
            from_block=from_block,
            to_block=to_block,
        )
        signal_source_trade_id = (
            _find_source_trade_id(trades, sample) or source_trade.trade_id
        )
        if not trades:
            trades = [source_trade]
    replay_asset_id = _holdout_replay_asset_id(
        trades, signal_source_trade_id, source_trade.asset_id
    )
    order = V2TakerOrder(
        order_id=f"fill-only-holdout-{sample.sample_id}-{side.lower()}",
        market_id=int(sample.market_id),
        asset_id=replay_asset_id,
        side=side,  # type: ignore[arg-type]
        limit_price=limit,
        size=_dec(sample.size),
        signal_block=max(0, int(sample.block_number) - 1),
        signal_ts=fill_ts - timedelta(seconds=1),
        latency_blocks=1,
        latency=timedelta(0),
        horizon_blocks=1,
        horizon=timedelta(seconds=2),
        participation_rate=Decimal("1"),
        price_buffer=Decimal("0"),
        signal_source_trade_id=signal_source_trade_id,
        signal_source_tx_hash=str(sample.tx_hash or "").lower(),
        signal_source_log_indexes=(int(sample.log_index),),
    )
    if str(profile) != "plumbing_replay":
        order = with_v2_execution_profile(order, profile)
    results, _, diagnostics = replay_v2_taker_orders_with_diagnostics([order], trades)
    result = results[0]
    return {
        "profile": profile,
        "status": result.status,
        "filled_size": result.filled_size,
        "avg_price": result.avg_price,
        "reason_unfilled": result.reason_unfilled,
        "p_depth_valid": result.p_depth_valid,
        "fill_only_eligibility": result.fill_only_eligibility,
        "fill_validity_reason": result.fill_validity_reason,
        "fill_validity_features": result.fill_validity_features,
        "fill_validity_rule": result.fill_validity_rule,
        "execution_profile_name": result.execution_profile_name,
        "execution_profile_activation": result.execution_profile_activation,
        "execution_stability_grade": result.execution_stability_grade,
        "trades_loaded": len(trades),
        "diagnostics": diagnostics.as_dict(),
        "signal_source_trade_id": signal_source_trade_id,
        "source_trade_ids": [fill.source_trade_id for fill in result.fills],
        "source_tx_hashes": [fill.source_tx_hash for fill in result.fills],
        "source_log_indexes": [list(fill.source_log_indexes) for fill in result.fills],
        "price_tolerance": price_tolerance,
    }


def _run_depth_holdout(
    sample: Any,
    events: Sequence[Mapping[str, Any]],
    *,
    side: str,
    limit: Decimal,
    book_ttl_ms: int,
    depth_haircut: Decimal,
    intent_cls: Any,
    clone_model_from_events: Any,
    ms_to_dt: Any,
) -> dict[str, Any]:
    model = clone_model_from_events(
        events,
        sample,
        book_ttl_ms=book_ttl_ms,
        depth_haircut=depth_haircut,
    )
    best_bid = model.book.best_bid
    best_ask = model.book.best_ask
    fill_ts = ms_to_dt(int(sample.fill_ts_ms))
    intent = intent_cls(
        client_order_id=f"depth-holdout-{sample.sample_id}-{side.lower()}",
        signal_ts=fill_ts,
        market_id=str(sample.market_id),
        asset_id=str(sample.l2_token_id),
        side=side,
        order_type="MARKETABLE_LIMIT",
        limit_price=limit,
        size=_dec(sample.size),
        tif="FOK",
        post_only=False,
    )
    result = model.execute_taker(intent)
    return {
        "status": result.state,
        "reject_reason": result.reject_reason,
        "filled_size": result.filled_size,
        "avg_price": result.avg_fill_price,
        "remaining_size": result.remaining_size,
        "fill_count": len(result.fills),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "events_loaded": len(events),
        "book_ts": model.book.last_update_ts,
        "l2_lag_seconds": max(
            0, int((fill_ts - model.book.last_update_ts).total_seconds())
        )
        if model.book.last_update_ts
        else None,
    }


def _holdout_comparison_row(
    sample: Any,
    *,
    side: str,
    limit: Decimal,
    fill_only: Mapping[str, Any],
    depth: Mapping[str, Any],
    price_tolerance: Decimal,
) -> dict[str, Any]:
    fill_only_filled = _dec(fill_only.get("filled_size")) > 0
    depth_filled = _dec(depth.get("filled_size")) > 0
    price_delta = (
        _dec(depth.get("avg_price")) - _dec(fill_only.get("avg_price"))
        if fill_only_filled and depth_filled
        else Decimal("0")
    )
    size_delta = _dec(depth.get("filled_size")) - _dec(fill_only.get("filled_size"))
    verdict = _classify_holdout(
        fill_only_filled,
        depth_filled,
        price_delta=price_delta,
        size_delta=size_delta,
        tolerance=price_tolerance,
    )
    return {
        "verdict": verdict,
        "sample": {
            "sample_id": sample.sample_id,
            "market_id": int(sample.market_id),
            "market_slug": sample.market_slug,
            "token_side": sample.token_side,
            "historical_side": sample.side,
            "fill_time": sample.fill_time,
            "block_number": int(sample.block_number),
            "tx_hash": sample.tx_hash,
            "log_index": int(sample.log_index),
            "trade_price": _dec(sample.trade_price),
            "size": _dec(sample.size),
        },
        "order": {
            "side": side,
            "limit_price": limit,
            "size": _dec(sample.size),
            "signal_source_trade_id": fill_only.get("signal_source_trade_id"),
            "signal_source_tx_hash": sample.tx_hash,
            "signal_source_log_indexes": [int(sample.log_index)],
        },
        "calibration_sample": {
            "order_intent": {
                "market_id": int(sample.market_id),
                "market_slug": sample.market_slug,
                "asset_id": sample.l2_token_id,
                "side": side,
                "limit_price": limit,
                "size": _dec(sample.size),
                "signal_source_trade_id": fill_only.get("signal_source_trade_id"),
                "signal_source_tx_hash": sample.tx_hash,
                "signal_source_log_indexes": [int(sample.log_index)],
            },
            "fill_only_features": (fill_only.get("fill_validity_features") or {}),
            "fill_only_result": {
                "status": fill_only.get("status"),
                "filled_size": fill_only.get("filled_size"),
                "avg_price": fill_only.get("avg_price"),
                "p_depth_valid": fill_only.get("p_depth_valid"),
                "eligibility": fill_only.get("fill_only_eligibility"),
                "reason_unfilled": fill_only.get("reason_unfilled"),
                "execution_profile_name": fill_only.get("execution_profile_name"),
                "execution_profile_activation": fill_only.get(
                    "execution_profile_activation"
                ),
                "execution_stability_grade": fill_only.get("execution_stability_grade"),
            },
            "depth_model_result": {
                "status": depth.get("status"),
                "filled_size": depth.get("filled_size"),
                "avg_price": depth.get("avg_price"),
                "reject_reason": depth.get("reject_reason"),
                "best_bid": depth.get("best_bid"),
                "best_ask": depth.get("best_ask"),
                "l2_lag_seconds": depth.get("l2_lag_seconds"),
            },
            "label": 1 if depth_filled else 0,
            "label_name": "depth_filled" if depth_filled else "depth_rejected",
            "verdict": verdict,
        },
        "fill_only": dict(fill_only),
        "depth": dict(depth),
        "price_delta": price_delta,
        "size_delta": size_delta,
    }


def _classify_holdout(
    fill_only_filled: bool,
    depth_filled: bool,
    *,
    price_delta: Decimal,
    size_delta: Decimal,
    tolerance: Decimal,
) -> str:
    if fill_only_filled and depth_filled:
        if abs(price_delta) <= tolerance and abs(size_delta) <= tolerance:
            return "both_filled_same"
        if abs(size_delta) > tolerance:
            return "both_filled_size_delta"
        return "both_filled_price_delta"
    if fill_only_filled:
        return "fill_only_only"
    if depth_filled:
        return "depth_only"
    return "both_no_fill"


def _summarize_holdout_comparisons(
    comparisons: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    size_error_abs = Decimal("0")
    price_error_abs = Decimal("0")
    adverse_price_error_abs = Decimal("0")
    size_error_count = 0
    price_error_count = 0
    overfill_count = 0
    underfill_count = 0
    for row in comparisons:
        verdict = str(row.get("verdict") or "unknown")
        counts[verdict] = counts.get(verdict, 0) + 1
        fill_only_filled = (
            _dec((row.get("fill_only") or {}).get("filled_size")) > 0
            if isinstance(row.get("fill_only"), Mapping)
            else False
        )
        depth_filled = (
            _dec((row.get("depth") or {}).get("filled_size")) > 0
            if isinstance(row.get("depth"), Mapping)
            else False
        )
        size_delta = _dec(row.get("size_delta"))
        price_delta = _dec(row.get("price_delta"))
        side = (
            str((row.get("order") or {}).get("side") or "").upper()
            if isinstance(row.get("order"), Mapping)
            else ""
        )
        if fill_only_filled and depth_filled:
            size_error_abs += abs(size_delta).quantize(Q, rounding=ROUND_HALF_UP)
            size_error_count += 1
            price_error_abs += abs(price_delta).quantize(Q, rounding=ROUND_HALF_UP)
            price_error_count += 1
            if size_delta < 0:
                overfill_count += 1
            elif size_delta > 0:
                underfill_count += 1
            if side == "BUY" and price_delta > 0:
                adverse_price_error_abs += price_delta
            elif side == "SELL" and price_delta < 0:
                adverse_price_error_abs += abs(price_delta)
    total = len(comparisons)
    same = counts.get("both_filled_same", 0)
    both_filled = (
        same
        + counts.get("both_filled_size_delta", 0)
        + counts.get("both_filled_price_delta", 0)
    )
    false_positive = counts.get("fill_only_only", 0)
    false_negative = counts.get("depth_only", 0)
    fill_only_positive = both_filled + false_positive
    depth_positive = both_filled + false_negative
    return {
        "samples": total,
        "verdict_counts": dict(sorted(counts.items())),
        "both_filled_same": same,
        "both_filled": both_filled,
        "fill_only_only": false_positive,
        "depth_only": false_negative,
        "both_filled_same_pct": (Decimal(same) / Decimal(total)).quantize(Q)
        if total
        else Decimal("0"),
        "false_positive_rate": (Decimal(false_positive) / Decimal(total)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if total
        else Decimal("0"),
        "false_negative_rate": (Decimal(false_negative) / Decimal(total)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if total
        else Decimal("0"),
        "precision": (Decimal(both_filled) / Decimal(fill_only_positive)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if fill_only_positive
        else Decimal("1"),
        "recall": (Decimal(both_filled) / Decimal(depth_positive)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if depth_positive
        else Decimal("1"),
        "overfill_rate": (Decimal(overfill_count) / Decimal(total)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if total
        else Decimal("0"),
        "underfill_rate": (Decimal(underfill_count) / Decimal(total)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if total
        else Decimal("0"),
        "adverse_price_error": (
            adverse_price_error_abs / Decimal(price_error_count)
        ).quantize(Q, rounding=ROUND_HALF_UP)
        if price_error_count
        else Decimal("0"),
        "avg_abs_size_error": (size_error_abs / Decimal(size_error_count)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if size_error_count
        else Decimal("0"),
        "avg_abs_price_error": (price_error_abs / Decimal(price_error_count)).quantize(
            Q, rounding=ROUND_HALF_UP
        )
        if price_error_count
        else Decimal("0"),
    }


def _holdout_fill_ts(sample: Any) -> datetime:
    value = getattr(sample, "fill_ts_ms", 0) or 0
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)


def _find_source_trade_id(trades: Sequence[V2TradePrint], sample: Any) -> str | None:
    tx_hash = str(getattr(sample, "tx_hash", "") or "").lower()
    log_index = int(getattr(sample, "log_index", 0) or 0)
    for trade in trades:
        if str(trade.tx_hash or "").lower() == tx_hash and log_index in set(
            trade.source_log_indexes
        ):
            return trade.trade_id
    return None


def _holdout_replay_asset_id(
    trades: Sequence[V2TradePrint], source_trade_id: str | None, fallback: str
) -> str:
    if source_trade_id:
        for trade in trades:
            if trade.trade_id == source_trade_id:
                return trade.asset_id.lower()
    return trades[0].asset_id.lower() if trades else str(fallback).lower()


def _load_holdout_trade_tape(
    sample: Any,
    *,
    client: ClickHouseClient,
    from_block: int,
    to_block: int,
) -> list[V2TradePrint]:
    seen_assets: set[str] = set()
    trades_by_id: dict[str, V2TradePrint] = {}
    for asset_id in _holdout_asset_id_candidates(sample):
        normalized = str(asset_id or "").lower()
        if not normalized or normalized in seen_assets:
            continue
        seen_assets.add(normalized)
        for trade in load_v2_trade_prints(
            market_id=int(sample.market_id),
            asset_id=normalized,
            from_block=from_block,
            to_block=to_block,
            client=client,
            limit=50_000,
        ):
            trades_by_id[trade.trade_id] = trade
    return sorted(trades_by_id.values(), key=lambda item: item.sequence)


def _holdout_asset_id_candidates(sample: Any) -> list[str]:
    candidates = [
        str(getattr(sample, "l2_token_id", "") or ""),
        str(getattr(sample, "fill_token_id", "") or ""),
    ]
    fill_token = str(getattr(sample, "fill_token_id", "") or "")
    if _looks_decimal_token_id(fill_token):
        candidates.append(_decimal_token_to_hex(fill_token))
    return candidates


def benchmark_fill_trade_replay(
    *, trades_count: int, orders_count: int, seed: int, reference: bool = False
) -> dict[str, Any]:
    orders, trades = synthetic_orders_and_trades(
        trades_count=trades_count, orders_count=orders_count, seed=seed
    )
    start = perf_counter()
    results, ledger, diagnostics = replay_v2_taker_orders_with_diagnostics(
        orders, trades
    )
    matching_total_sec = _elapsed(start)
    comparison: dict[str, Any] = {"status": "not_run"}
    reference_sec: Decimal | None = None
    if reference:
        ref_start = perf_counter()
        reference_results, reference_ledger = replay_v2_taker_orders_reference(
            orders, trades
        )
        reference_sec = _elapsed(ref_start)
        comparison = compare_replay_results(
            reference_results, reference_ledger, results, ledger
        )
    orders_per_sec = (
        (Decimal(len(orders)) / matching_total_sec).quantize(Q)
        if matching_total_sec > 0
        else Decimal("0")
    )
    summary = {
        "trades_count": len(trades),
        "orders_count": len(orders),
        "fills_count": sum(len(row.fills) for row in results),
        "matching_sec": matching_total_sec,
        "reference_sec": reference_sec,
        "orders_per_sec": orders_per_sec,
        "candidate_rows_per_order_p50": diagnostics.candidate_rows_per_order_p50,
        "candidate_rows_per_order_p95": diagnostics.candidate_rows_per_order_p95,
        "candidate_rows_per_order_p99": diagnostics.candidate_rows_per_order_p99,
        "candidate_rows_per_order_max": diagnostics.candidate_rows_per_order_max,
        "candidate_trades_scanned": diagnostics.candidate_rows_scanned,
        "naive_rows_scanned": diagnostics.naive_rows_scanned,
        "scan_reduction_ratio": diagnostics.scan_reduction_ratio,
        "capacity_updates_count": len(ledger.as_dict()),
        "peak_memory_mb": Decimal(
            str(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
        ).quantize(Q),
        "comparison": comparison,
    }
    checks = [
        check(
            "benchmark produced diagnostics",
            diagnostics.orders_count == len(orders),
            diagnostics.as_dict(),
        ),
        check(
            "matching hot path avoids full cartesian scan",
            diagnostics.candidate_rows_scanned <= diagnostics.naive_rows_scanned,
            diagnostics.as_dict(),
        ),
    ]
    if reference:
        checks.append(
            check(
                "reference comparison diff_count = 0",
                int(comparison.get("diff_count") or 0) == 0,
                comparison,
            )
        )
    return _payload("Fill Trade Replay Benchmark", checks, summary)


def build_parameter_sensitivity_report(
    *, trades_count: int = 2000, orders_count: int = 100, seed: int = 42
) -> dict[str, Any]:
    orders, trades = synthetic_orders_and_trades(
        trades_count=trades_count, orders_count=orders_count, seed=seed
    )
    report = build_v2_replay_report(
        orders,
        trades,
        capacity_rates=(
            Decimal("0.005"),
            Decimal("0.01"),
            Decimal("0.025"),
            Decimal("0.05"),
            Decimal("0.10"),
        ),
        latency_values=(
            timedelta(0),
            timedelta(milliseconds=100),
            timedelta(milliseconds=500),
            timedelta(seconds=1),
            timedelta(seconds=5),
        ),
        horizon_values=(
            timedelta(seconds=5),
            timedelta(seconds=30),
            timedelta(minutes=5),
            timedelta(minutes=15),
            timedelta(hours=1),
        ),
    )
    checks = [
        check(
            "participation curve complete",
            [row.get("value") for row in report.get("capacity_curve", [])]
            == ["0.005", "0.01", "0.025", "0.05", "0.10"],
            report.get("capacity_curve"),
        ),
        check(
            "latency curve complete",
            len(report.get("latency_curve", [])) == 5,
            report.get("latency_curve"),
        ),
        check(
            "horizon curve complete",
            len(report.get("horizon_curve", [])) == 5,
            report.get("horizon_curve"),
        ),
        check(
            "strict/conservative/probabilistic/optimistic profiles present",
            set((report.get("mode_comparison") or {}).keys())
            == {
                "strict_audit",
                "conservative_trade_tape",
                "probabilistic_conservative",
                "probabilistic_trade_tape",
                "probabilistic_source_confirmed",
                "optimistic_sensitivity",
            },
            report.get("mode_comparison"),
        ),
    ]
    return _payload("Fill Trade Parameter Sensitivity", checks, report)


def synthetic_orders_and_trades(
    *, trades_count: int, orders_count: int, seed: int
) -> tuple[list[V2TakerOrder], list[V2TradePrint]]:
    rng = random.Random(seed)
    trades: list[V2TradePrint] = []
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index in range(max(0, trades_count)):
        side = "BUY" if index % 2 == 0 else "SELL"
        price = (
            Decimal("0.50") + Decimal(rng.randint(-10, 10)) / Decimal("100")
        ).quantize(Q)
        size = Decimal(rng.randint(1, 500)).quantize(Q)
        trades.append(
            V2TradePrint(
                trade_id=f"synthetic-{seed}-{index}",
                market_id=1 + (index % 3),
                condition_id=f"condition-{index % 3}",
                asset_id=f"token-{index % 2}",
                outcome="YES",
                block_number=index + 1,
                block_time=base + timedelta(seconds=index),
                tx_hash=f"0x{seed:08x}{index:056x}"[-66:],
                tx_index=index,
                tx_index_source="synthetic",
                price=price,
                size=size,
                notional=(price * size).quantize(Q),
                aggressor_side=side,  # type: ignore[arg-type]
                passive_side="SELL" if side == "BUY" else "BUY",  # type: ignore[arg-type]
                source_log_indexes=(index,),
                source_fill_count=1,
            )
        )
    orders: list[V2TakerOrder] = []
    step = max(1, max(1, trades_count) // max(1, orders_count))
    for index in range(max(0, orders_count)):
        block = index * step + 1
        side = "BUY" if index % 2 == 0 else "SELL"
        orders.append(
            V2TakerOrder(
                order_id=f"synthetic-order-{seed}-{index}",
                market_id=1 + (index % 3),
                asset_id=f"token-{index % 2}",
                side=side,  # type: ignore[arg-type]
                limit_price=Decimal("0.60") if side == "BUY" else Decimal("0.40"),
                size=Decimal("10"),
                signal_block=max(0, block - 1),
                signal_ts=base + timedelta(seconds=max(0, block - 1)),
                latency=timedelta(0),
                horizon=timedelta(seconds=120),
                horizon_blocks=120,
                participation_rate=Decimal("0.025"),
                price_buffer=Decimal("0.005"),
            )
        )
    return orders, trades


def _payload(
    title: str, checks: Sequence[ValidationCheck], summary: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "title": title,
        "status": status_from_checks(checks),
        "summary": dict(summary),
        "check_summary": summarize_checks(checks),
        "checks": [row.as_dict() for row in checks],
    }


def _table_exists(client: ClickHouseClient, table: str) -> bool:
    value = client.query_scalar(
        f"""
        SELECT count()
        FROM system.tables
        WHERE database = currentDatabase()
          AND name = {_quote(table)}
        """,
        timeout_seconds=30,
    )
    return str(value).strip() not in {"", "0"}


def _count(client: ClickHouseClient, table: str, predicate: str) -> int:
    return _scalar_int(
        client, f"SELECT count() FROM {table} WHERE 1=1{predicate}", default=0
    )


def _one(client: ClickHouseClient, query: str) -> dict[str, Any]:
    rows = _rows(client, query)
    return rows[0] if rows else {}


def _rows(client: ClickHouseClient, query: str) -> list[dict[str, Any]]:
    return client.query_json_rows(query, timeout_seconds=180)


def _scalar_int(client: ClickHouseClient, query: str, *, default: int) -> int:
    try:
        value = client.query_scalar(query, timeout_seconds=180)
        return int(str(value).strip() or default)
    except Exception:
        return default


def _block_predicate(from_block: int | None, to_block: int | None) -> str:
    parts = []
    if from_block is not None:
        parts.append(f"block_number >= toUInt64({int(from_block)})")
    if to_block is not None:
        parts.append(f"block_number <= toUInt64({int(to_block)})")
    return "" if not parts else " AND " + " AND ".join(parts)


def _quote(value: Any) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _looks_decimal_token_id(value: Any) -> bool:
    text = str(value or "").strip()
    return len(text) > 20 and text.isdigit()


def _decimal_token_to_hex(value: Any) -> str:
    return f"{int(str(value).strip()):064x}"


def _sample_trade_print_context(*, sample_limit: int) -> list[dict[str, Any]]:
    ch = ClickHouseClient()
    return ch.query_json_rows(
        f"""
        SELECT market_id, asset_id, block_number
        FROM trade_prints_one_sided
        ORDER BY block_number DESC
        LIMIT {max(1, int(sample_limit))}
        """,
        timeout_seconds=120,
    )


def _latest_v2_run_id(conn: Any) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM quant.quant_backtest_parameters
            WHERE upper(replace(coalesce(execution_price_mode, ''), '-', '_')) = 'ORDERFILLED_V2_TAPE'
            ORDER BY run_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    return int(row["run_id"]) if row else None


def _latest_l2_run_id(conn: Any) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM quant.quant_backtest_parameters
            WHERE upper(replace(coalesce(execution_price_mode, ''), '-', '_')) IN ('DEPTH', 'ORDERFILLED_LOB', 'L2_ORDERFILLED')
            ORDER BY run_id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    return int(row["run_id"]) if row else None


def _run_order_summary(conn: Any, run_id: int) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                count(*) AS orders,
                count(*) FILTER (WHERE upper(status) IN ('FILLED', 'PARTIAL_FILLED')) AS filled_orders,
                coalesce(sum(filled_size), 0) AS filled_size
            FROM quant.quant_backtest_orders
            WHERE run_id = %s
            """,
            (int(run_id),),
        )
        row = cur.fetchone() or {}
    orders = int(row.get("orders") or 0)
    filled = int(row.get("filled_orders") or 0)
    return {
        "run_id": int(run_id),
        "orders": orders,
        "filled_orders": filled,
        "fill_rate": (Decimal(filled) / Decimal(orders)).quantize(Q)
        if orders
        else Decimal("0"),
        "filled_size": _dec(row.get("filled_size")),
    }


def _set_statement_timeout(conn: Any, timeout_ms: int = 15_000) -> None:
    safe_timeout = max(1, int(timeout_ms))
    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = {safe_timeout}")


def _persisted_order_has_source(row: Mapping[str, Any]) -> bool:
    meta = row.get("meta") if isinstance(row.get("meta"), Mapping) else {}
    evidence = row.get("evidence") if isinstance(row.get("evidence"), Mapping) else {}
    text = json.dumps(
        _plain({"meta": meta, "evidence": evidence}), ensure_ascii=False, default=str
    )
    return (
        "source_trade_id" in text
        or "tx_hash" in text
        or "trade_prints_one_sided" in text
    )


def _trade(
    trade_id: str,
    block: int,
    side: str,
    price: str,
    size: str,
    *,
    seconds: int | None = None,
) -> V2TradePrint:
    ts = _ts(seconds if seconds is not None else block)
    return V2TradePrint(
        trade_id=trade_id,
        market_id=1,
        condition_id="condition",
        asset_id="token-yes",
        outcome="YES",
        block_number=block,
        block_time=ts,
        tx_hash=f"0x{trade_id}",
        tx_index=block,
        tx_index_source="fixture",
        price=Decimal(price),
        size=Decimal(size),
        notional=(Decimal(price) * Decimal(size)).quantize(Q),
        aggressor_side=side,  # type: ignore[arg-type]
        passive_side="SELL" if side == "BUY" else "BUY",  # type: ignore[arg-type]
        source_log_indexes=(block,),
        source_fill_count=1,
    )


def _order(
    side: str, limit: str, *, size: str, participation: str = "0.025"
) -> V2TakerOrder:
    return V2TakerOrder(
        order_id=f"order-{side}-{limit}",
        market_id=1,
        asset_id="token-yes",
        side=side,  # type: ignore[arg-type]
        limit_price=Decimal(limit),
        size=Decimal(size),
        signal_block=99,
        signal_ts=_ts(99),
        latency_blocks=1,
        latency=timedelta(seconds=1),
        horizon_blocks=10,
        horizon=timedelta(minutes=5),
        participation_rate=Decimal(participation),
    )


def _ts(seconds: int) -> datetime:
    return datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def _dec(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value)).quantize(Q, rounding=ROUND_HALF_UP)


def _elapsed(start: float) -> Decimal:
    return Decimal(str(perf_counter() - start)).quantize(Q, rounding=ROUND_HALF_UP)


def _sample(rows: Sequence[Any], limit: int = 5) -> str:
    if not rows:
        return "none"
    suffix = "" if len(rows) <= limit else f" ... +{len(rows) - limit} more"
    return ", ".join(str(row) for row in rows[:limit]) + suffix


def _brief(value: Any, limit: int = 1200) -> str:
    if isinstance(value, (Mapping, list, tuple)):
        text = json.dumps(
            _plain(value), ensure_ascii=False, sort_keys=True, default=str
        )
    else:
        text = str(value)
    return (
        text
        if len(text) <= limit
        else text[:limit] + f"... <truncated {len(text) - limit} chars>"
    )


def _plain(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _plain(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value
