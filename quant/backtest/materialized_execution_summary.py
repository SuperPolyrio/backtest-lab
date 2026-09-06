"""Materialized execution summaries built from block-close tape."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import json
from typing import Any, Iterable, Mapping, Sequence


Q = Decimal("0.0000000001")
ZERO = Decimal("0")


@dataclass(frozen=True)
class ExecutionSummaryFilters:
    market_id: int | None = None
    market_slug: str | None = None
    event_slug: str | None = None
    token_id: str | None = None
    token_side: str | None = None
    from_block: int | None = None
    to_block: int | None = None


def classify_side_bucket(maker_amount: Any, taker_amount: Any) -> str:
    maker = max(ZERO, decimal_value(maker_amount))
    taker = max(ZERO, decimal_value(taker_amount))
    total = maker + taker
    if total <= ZERO:
        return "no_counterparty_amount"
    maker_share = maker / total
    taker_share = taker / total
    if maker_share >= Decimal("0.60"):
        return "maker_dominant"
    if taker_share >= Decimal("0.60"):
        return "taker_dominant"
    return "balanced"


def classify_liquidity_bucket(volume: Any, trade_count: Any) -> str:
    vol = max(ZERO, decimal_value(volume))
    trades = int(decimal_value(trade_count))
    if trades <= 0 or vol <= ZERO:
        return "no_trades"
    if trades < 10 or vol < Decimal("100"):
        return "thin"
    if trades < 100 or vol < Decimal("10000"):
        return "medium"
    if trades < 1000 or vol < Decimal("1000000"):
        return "deep"
    return "very_deep"


def summary_from_aggregate(row: Mapping[str, Any]) -> dict[str, Any]:
    maker_amount = max(ZERO, decimal_value(row.get("maker_amount")))
    taker_amount = max(ZERO, decimal_value(row.get("taker_amount")))
    total_counterparty_amount = maker_amount + taker_amount
    maker_share = None
    taker_share = None
    if total_counterparty_amount > ZERO:
        maker_share = quantize_decimal(maker_amount / total_counterparty_amount)
        taker_share = quantize_decimal(taker_amount / total_counterparty_amount)
    anomaly_count = sum(
        int(decimal_value(row.get(field)))
        for field in (
            "internal_filtered_count",
            "invalid_size_count",
            "invalid_price_count",
            "amount_ratio_count",
            "raw_price_fallback_count",
            "extreme_trade_count",
        )
    )
    flags = normalize_flags(row.get("anomaly_flags"))
    return {
        "token_id": str(row.get("token_id") or ""),
        "market_id": int(decimal_value(row.get("market_id"))),
        "market_slug": row.get("market_slug"),
        "token_side": str(row.get("token_side") or "").upper(),
        "first_block": int(decimal_value(row.get("first_block"))) if row.get("first_block") is not None else None,
        "last_block": int(decimal_value(row.get("last_block"))) if row.get("last_block") is not None else None,
        "block_row_count": int(decimal_value(row.get("block_row_count"))),
        "trade_count": int(decimal_value(row.get("trade_count"))),
        "raw_trade_count": int(decimal_value(row.get("raw_trade_count"))),
        "volume": quantize_decimal(decimal_value(row.get("volume"))),
        "maker_amount": maker_amount,
        "taker_amount": taker_amount,
        "maker_share": maker_share,
        "taker_share": taker_share,
        "internal_filtered_count": int(decimal_value(row.get("internal_filtered_count"))),
        "invalid_size_count": int(decimal_value(row.get("invalid_size_count"))),
        "invalid_price_count": int(decimal_value(row.get("invalid_price_count"))),
        "amount_ratio_count": int(decimal_value(row.get("amount_ratio_count"))),
        "raw_price_fallback_count": int(decimal_value(row.get("raw_price_fallback_count"))),
        "extreme_trade_count": int(decimal_value(row.get("extreme_trade_count"))),
        "anomaly_count": anomaly_count,
        "anomaly_flags": flags,
        "side_bucket": classify_side_bucket(maker_amount, taker_amount),
        "liquidity_bucket": classify_liquidity_bucket(row.get("volume"), row.get("trade_count")),
    }


def grouped_execution_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    group_key: str,
    id_field: str,
    label_field: str,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        key = str(row.get(group_key) or "")
        if key:
            grouped.setdefault(key, []).append(row)
    summaries: list[dict[str, Any]] = []
    for key, items in grouped.items():
        maker_amount = sum((decimal_value(item.get("maker_amount")) for item in items), ZERO)
        taker_amount = sum((decimal_value(item.get("taker_amount")) for item in items), ZERO)
        total_counterparty_amount = maker_amount + taker_amount
        maker_share = quantize_decimal(maker_amount / total_counterparty_amount) if total_counterparty_amount > ZERO else None
        taker_share = quantize_decimal(taker_amount / total_counterparty_amount) if total_counterparty_amount > ZERO else None
        side_counts = bucket_counts(item.get("side_bucket") for item in items)
        liquidity_counts = bucket_counts(item.get("liquidity_bucket") for item in items)
        first_blocks = [int(decimal_value(item.get("first_block"))) for item in items if item.get("first_block") is not None]
        last_blocks = [int(decimal_value(item.get("last_block"))) for item in items if item.get("last_block") is not None]
        summaries.append(
            {
                id_field: key,
                label_field: first_non_blank(*(item.get(label_field) for item in items)),
                "token_count": len({str(item.get("token_id") or "") for item in items if item.get("token_id")}),
                "first_block": min(first_blocks) if first_blocks else None,
                "last_block": max(last_blocks) if last_blocks else None,
                "block_row_count": sum(int(decimal_value(item.get("block_row_count"))) for item in items),
                "trade_count": sum(int(decimal_value(item.get("trade_count"))) for item in items),
                "raw_trade_count": sum(int(decimal_value(item.get("raw_trade_count"))) for item in items),
                "volume": quantize_decimal(sum((decimal_value(item.get("volume")) for item in items), ZERO)),
                "maker_amount": maker_amount,
                "taker_amount": taker_amount,
                "maker_share": maker_share,
                "taker_share": taker_share,
                "anomaly_count": sum(int(decimal_value(item.get("anomaly_count"))) for item in items),
                "side_bucket_counts": side_counts,
                "liquidity_bucket_counts": liquidity_counts,
                "dominant_side_bucket": dominant_bucket(side_counts),
                "dominant_liquidity_bucket": dominant_bucket(liquidity_counts),
            }
        )
    return sorted(summaries, key=lambda item: (decimal_value(item.get("volume")), decimal_value(item.get("trade_count"))), reverse=True)


def bucket_counts(values: Iterable[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "unknown")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def dominant_bucket(counts: Mapping[str, int]) -> str:
    if not counts:
        return "unknown"
    return sorted(counts.items(), key=lambda item: (int(item[1]), item[0]), reverse=True)[0][0]


def first_non_blank(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def build_filter_sql(filters: ExecutionSummaryFilters) -> tuple[str, list[Any]]:
    clauses = ["TRUE"]
    params: list[Any] = []
    if filters.market_id is not None:
        clauses.append("market_id = %s")
        params.append(int(filters.market_id))
    if filters.market_slug:
        clauses.append("market_slug = %s")
        params.append(str(filters.market_slug))
    if filters.token_id:
        clauses.append("token_id = %s")
        params.append(str(filters.token_id))
    if filters.token_side:
        clauses.append("token_side = %s")
        params.append(str(filters.token_side).upper())
    if filters.from_block is not None:
        clauses.append("block_number >= %s")
        params.append(int(filters.from_block))
    if filters.to_block is not None:
        clauses.append("block_number <= %s")
        params.append(int(filters.to_block))
    return " AND ".join(clauses), params


def aggregate_execution_summary_rows(conn: Any, filters: ExecutionSummaryFilters) -> list[dict[str, Any]]:
    where_sql, params = build_filter_sql(filters)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH filtered AS (
                SELECT *
                FROM quant.market_token_block_close
                WHERE {where_sql}
            ),
            agg AS (
                SELECT
                    token_id,
                    max(market_id) AS market_id,
                    max(market_slug) AS market_slug,
                    max(token_side) AS token_side,
                    min(block_number) AS first_block,
                    max(block_number) AS last_block,
                    count(*) AS block_row_count,
                    sum(trade_count) AS trade_count,
                    sum(raw_trade_count) AS raw_trade_count,
                    sum(volume) AS volume,
                    sum(COALESCE(close_maker_amount, 0)) AS maker_amount,
                    sum(COALESCE(close_taker_amount, 0)) AS taker_amount,
                    sum(internal_filtered_count) AS internal_filtered_count,
                    sum(invalid_size_count) AS invalid_size_count,
                    sum(invalid_price_count) AS invalid_price_count,
                    sum(amount_ratio_count) AS amount_ratio_count,
                    sum(raw_price_fallback_count) AS raw_price_fallback_count,
                    sum(extreme_trade_count) AS extreme_trade_count
                FROM filtered
                GROUP BY token_id
            ),
            flags AS (
                SELECT
                    token_id,
                    jsonb_agg(DISTINCT f.flag ORDER BY f.flag) AS anomaly_flags
                FROM filtered
                CROSS JOIN LATERAL jsonb_array_elements_text(anomaly_flags) AS f(flag)
                GROUP BY token_id
            )
            SELECT agg.*, COALESCE(flags.anomaly_flags, '[]'::jsonb) AS anomaly_flags
            FROM agg
            LEFT JOIN flags USING (token_id)
            ORDER BY volume DESC, trade_count DESC, token_id ASC
            """,
            params,
        )
        return [summary_from_aggregate(dict(row)) for row in cur.fetchall()]


def upsert_execution_summaries(conn: Any, rows: Iterable[Mapping[str, Any]]) -> int:
    values = list(rows)
    if not values:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO quant.market_token_execution_summary (
                token_id, market_id, market_slug, token_side,
                first_block, last_block, block_row_count,
                trade_count, raw_trade_count, volume,
                maker_amount, taker_amount, maker_share, taker_share,
                internal_filtered_count, invalid_size_count, invalid_price_count,
                amount_ratio_count, raw_price_fallback_count, extreme_trade_count,
                anomaly_count, anomaly_flags, side_bucket, liquidity_bucket,
                refreshed_at
            )
            VALUES (
                %(token_id)s, %(market_id)s, %(market_slug)s, %(token_side)s,
                %(first_block)s, %(last_block)s, %(block_row_count)s,
                %(trade_count)s, %(raw_trade_count)s, %(volume)s,
                %(maker_amount)s, %(taker_amount)s, %(maker_share)s, %(taker_share)s,
                %(internal_filtered_count)s, %(invalid_size_count)s, %(invalid_price_count)s,
                %(amount_ratio_count)s, %(raw_price_fallback_count)s, %(extreme_trade_count)s,
                %(anomaly_count)s, %(anomaly_flags_json)s::jsonb, %(side_bucket)s, %(liquidity_bucket)s,
                now()
            )
            ON CONFLICT (token_id) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                first_block = EXCLUDED.first_block,
                last_block = EXCLUDED.last_block,
                block_row_count = EXCLUDED.block_row_count,
                trade_count = EXCLUDED.trade_count,
                raw_trade_count = EXCLUDED.raw_trade_count,
                volume = EXCLUDED.volume,
                maker_amount = EXCLUDED.maker_amount,
                taker_amount = EXCLUDED.taker_amount,
                maker_share = EXCLUDED.maker_share,
                taker_share = EXCLUDED.taker_share,
                internal_filtered_count = EXCLUDED.internal_filtered_count,
                invalid_size_count = EXCLUDED.invalid_size_count,
                invalid_price_count = EXCLUDED.invalid_price_count,
                amount_ratio_count = EXCLUDED.amount_ratio_count,
                raw_price_fallback_count = EXCLUDED.raw_price_fallback_count,
                extreme_trade_count = EXCLUDED.extreme_trade_count,
                anomaly_count = EXCLUDED.anomaly_count,
                anomaly_flags = EXCLUDED.anomaly_flags,
                side_bucket = EXCLUDED.side_bucket,
                liquidity_bucket = EXCLUDED.liquidity_bucket,
                refreshed_at = now()
            """,
            [summary_to_db_params(row) for row in values],
        )
    return len(values)


def aggregate_market_execution_summary_rows(conn: Any, filters: ExecutionSummaryFilters) -> list[dict[str, Any]]:
    where_sql, params = build_summary_filter_sql(filters, table_alias="s")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                s.*
            FROM quant.market_token_execution_summary s
            WHERE {where_sql}
            ORDER BY s.market_id ASC, s.token_side ASC, s.volume DESC
            """,
            params,
        )
        token_rows = [dict(row) for row in cur.fetchall()]
    grouped = grouped_execution_summary(token_rows, group_key="market_id", id_field="market_id", label_field="market_slug")
    for row in grouped:
        row["market_id"] = int(row["market_id"])
        row["market_count"] = 1
    return grouped


def aggregate_event_execution_summary_rows(conn: Any, filters: ExecutionSummaryFilters) -> list[dict[str, Any]]:
    where_sql, params = build_summary_filter_sql(filters, table_alias="s", event_alias="mem")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                s.*,
                mem.event_slug,
                evt.event_title
            FROM quant.market_token_execution_summary s
            JOIN quant.market_event_members mem ON mem.market_id = s.market_id
            LEFT JOIN quant.market_event_metadata evt ON evt.event_slug = mem.event_slug
            WHERE {where_sql}
            ORDER BY mem.event_slug ASC, s.volume DESC
            """,
            params,
        )
        token_rows = [dict(row) for row in cur.fetchall()]
    grouped = grouped_execution_summary(token_rows, group_key="event_slug", id_field="event_slug", label_field="event_title")
    markets_by_event: dict[str, set[int]] = {}
    for row in token_rows:
        event_slug = str(row.get("event_slug") or "")
        if event_slug and row.get("market_id") is not None:
            markets_by_event.setdefault(event_slug, set()).add(int(row["market_id"]))
    for row in grouped:
        row["market_count"] = len(markets_by_event.get(str(row["event_slug"]), set()))
    return grouped


def build_summary_filter_sql(
    filters: ExecutionSummaryFilters,
    *,
    table_alias: str = "s",
    event_alias: str | None = None,
) -> tuple[str, list[Any]]:
    clauses = ["TRUE"]
    params: list[Any] = []
    prefix = f"{table_alias}."
    if filters.market_id is not None:
        clauses.append(f"{prefix}market_id = %s")
        params.append(int(filters.market_id))
    if filters.market_slug:
        clauses.append(f"{prefix}market_slug = %s")
        params.append(str(filters.market_slug))
    if filters.event_slug and event_alias:
        clauses.append(f"{event_alias}.event_slug = %s")
        params.append(str(filters.event_slug))
    if filters.token_id:
        clauses.append(f"{prefix}token_id = %s")
        params.append(str(filters.token_id))
    if filters.token_side:
        clauses.append(f"{prefix}token_side = %s")
        params.append(str(filters.token_side).upper())
    if filters.from_block is not None:
        clauses.append(f"{prefix}last_block >= %s")
        params.append(int(filters.from_block))
    if filters.to_block is not None:
        clauses.append(f"{prefix}first_block <= %s")
        params.append(int(filters.to_block))
    return " AND ".join(clauses), params


def refresh_execution_summaries_for_tokens(conn: Any, token_ids: Iterable[str]) -> dict[str, int]:
    """Refresh token, market, and event execution summaries for affected tokens."""
    unique_token_ids = sorted({str(token_id) for token_id in token_ids if str(token_id or "").strip()})
    token_rows: list[dict[str, Any]] = []
    for token_id in unique_token_ids:
        token_rows.extend(aggregate_execution_summary_rows(conn, ExecutionSummaryFilters(token_id=token_id)))
    token_written = upsert_execution_summaries(conn, token_rows) if token_rows else 0

    market_ids = sorted({int(row["market_id"]) for row in token_rows if row.get("market_id") is not None})
    market_rows: list[dict[str, Any]] = []
    for market_id in market_ids:
        market_rows.extend(aggregate_market_execution_summary_rows(conn, ExecutionSummaryFilters(market_id=market_id)))
    market_written = upsert_market_execution_summaries(conn, market_rows) if market_rows else 0

    event_slugs: list[str] = []
    if market_ids:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT event_slug
                FROM quant.market_event_members
                WHERE market_id = ANY(%s::bigint[])
                ORDER BY event_slug
                """,
                [market_ids],
            )
            event_slugs = [str(row["event_slug"]) for row in cur.fetchall() if row.get("event_slug")]
    event_rows: list[dict[str, Any]] = []
    for event_slug in event_slugs:
        event_rows.extend(aggregate_event_execution_summary_rows(conn, ExecutionSummaryFilters(event_slug=event_slug)))
    event_written = upsert_event_execution_summaries(conn, event_rows) if event_rows else 0

    return {
        "token_ids": len(unique_token_ids),
        "token_rows": len(token_rows),
        "market_rows": len(market_rows),
        "event_rows": len(event_rows),
        "token_written": token_written,
        "market_written": market_written,
        "event_written": event_written,
    }


def upsert_market_execution_summaries(conn: Any, rows: Iterable[Mapping[str, Any]]) -> int:
    values = list(rows)
    if not values:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO quant.market_execution_summary (
                market_id, market_slug, token_count, first_block, last_block,
                block_row_count, trade_count, raw_trade_count, volume,
                maker_amount, taker_amount, maker_share, taker_share, anomaly_count,
                side_bucket_counts, liquidity_bucket_counts,
                dominant_side_bucket, dominant_liquidity_bucket, refreshed_at
            )
            VALUES (
                %(market_id)s, %(market_slug)s, %(token_count)s, %(first_block)s, %(last_block)s,
                %(block_row_count)s, %(trade_count)s, %(raw_trade_count)s, %(volume)s,
                %(maker_amount)s, %(taker_amount)s, %(maker_share)s, %(taker_share)s, %(anomaly_count)s,
                %(side_bucket_counts_json)s::jsonb, %(liquidity_bucket_counts_json)s::jsonb,
                %(dominant_side_bucket)s, %(dominant_liquidity_bucket)s, now()
            )
            ON CONFLICT (market_id) DO UPDATE SET
                market_slug = EXCLUDED.market_slug,
                token_count = EXCLUDED.token_count,
                first_block = EXCLUDED.first_block,
                last_block = EXCLUDED.last_block,
                block_row_count = EXCLUDED.block_row_count,
                trade_count = EXCLUDED.trade_count,
                raw_trade_count = EXCLUDED.raw_trade_count,
                volume = EXCLUDED.volume,
                maker_amount = EXCLUDED.maker_amount,
                taker_amount = EXCLUDED.taker_amount,
                maker_share = EXCLUDED.maker_share,
                taker_share = EXCLUDED.taker_share,
                anomaly_count = EXCLUDED.anomaly_count,
                side_bucket_counts = EXCLUDED.side_bucket_counts,
                liquidity_bucket_counts = EXCLUDED.liquidity_bucket_counts,
                dominant_side_bucket = EXCLUDED.dominant_side_bucket,
                dominant_liquidity_bucket = EXCLUDED.dominant_liquidity_bucket,
                refreshed_at = now()
            """,
            [group_summary_to_db_params(row) for row in values],
        )
    return len(values)


def upsert_event_execution_summaries(conn: Any, rows: Iterable[Mapping[str, Any]]) -> int:
    values = list(rows)
    if not values:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO quant.event_execution_summary (
                event_slug, event_title, market_count, token_count, first_block, last_block,
                block_row_count, trade_count, raw_trade_count, volume,
                maker_amount, taker_amount, maker_share, taker_share, anomaly_count,
                side_bucket_counts, liquidity_bucket_counts,
                dominant_side_bucket, dominant_liquidity_bucket, refreshed_at
            )
            VALUES (
                %(event_slug)s, %(event_title)s, %(market_count)s, %(token_count)s, %(first_block)s, %(last_block)s,
                %(block_row_count)s, %(trade_count)s, %(raw_trade_count)s, %(volume)s,
                %(maker_amount)s, %(taker_amount)s, %(maker_share)s, %(taker_share)s, %(anomaly_count)s,
                %(side_bucket_counts_json)s::jsonb, %(liquidity_bucket_counts_json)s::jsonb,
                %(dominant_side_bucket)s, %(dominant_liquidity_bucket)s, now()
            )
            ON CONFLICT (event_slug) DO UPDATE SET
                event_title = EXCLUDED.event_title,
                market_count = EXCLUDED.market_count,
                token_count = EXCLUDED.token_count,
                first_block = EXCLUDED.first_block,
                last_block = EXCLUDED.last_block,
                block_row_count = EXCLUDED.block_row_count,
                trade_count = EXCLUDED.trade_count,
                raw_trade_count = EXCLUDED.raw_trade_count,
                volume = EXCLUDED.volume,
                maker_amount = EXCLUDED.maker_amount,
                taker_amount = EXCLUDED.taker_amount,
                maker_share = EXCLUDED.maker_share,
                taker_share = EXCLUDED.taker_share,
                anomaly_count = EXCLUDED.anomaly_count,
                side_bucket_counts = EXCLUDED.side_bucket_counts,
                liquidity_bucket_counts = EXCLUDED.liquidity_bucket_counts,
                dominant_side_bucket = EXCLUDED.dominant_side_bucket,
                dominant_liquidity_bucket = EXCLUDED.dominant_liquidity_bucket,
                refreshed_at = now()
            """,
            [group_summary_to_db_params(row) for row in values],
        )
    return len(values)


def group_summary_to_db_params(row: Mapping[str, Any]) -> dict[str, Any]:
    params = dict(row)
    params["side_bucket_counts_json"] = json.dumps(dict(row.get("side_bucket_counts") or {}), ensure_ascii=True, sort_keys=True)
    params["liquidity_bucket_counts_json"] = json.dumps(dict(row.get("liquidity_bucket_counts") or {}), ensure_ascii=True, sort_keys=True)
    return params


def summary_to_db_params(row: Mapping[str, Any]) -> dict[str, Any]:
    params = dict(row)
    params["anomaly_flags_json"] = json.dumps(list(row.get("anomaly_flags") or []), ensure_ascii=True, sort_keys=True)
    return params


def normalize_flags(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [value]
    return sorted({str(item) for item in value if str(item)})


def decimal_value(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        parsed = Decimal(str(value if value is not None else "0"))
    except (InvalidOperation, ValueError):
        return ZERO
    return parsed if parsed.is_finite() else ZERO


def quantize_decimal(value: Decimal) -> Decimal:
    return value.quantize(Q)
