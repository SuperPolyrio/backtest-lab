"""ClickHouse SQL for Polymarket data-api trades."""

from __future__ import annotations

from typing import Iterable

from .block_close_algorithm import quote_clickhouse_string


def api_trades_sql(
    *,
    table: str = "pg_api_trades_main",
    start_ts: int,
    end_ts: int,
    market_ids: Iterable[int] | None = None,
    token_ids: Iterable[str] | None = None,
) -> str:
    filters = [
        f"trade_timestamp >= {int(start_ts)}",
        f"trade_timestamp <= {int(end_ts)}",
        f"trade_time >= toDateTime({int(start_ts)}, 'UTC')",
        f"trade_time <= toDateTime({int(end_ts)}, 'UTC')",
        "price >= 0",
        "price <= 1",
        "size > 0",
    ]
    market_list = sorted({int(market_id) for market_id in market_ids or [] if int(market_id or 0) > 0})
    if market_list:
        filters.append(f"market_id IN ({','.join(str(market_id) for market_id in market_list)})")
    token_list = [str(token).strip() for token in token_ids or [] if str(token or "").strip()]
    if token_list:
        quoted = ",".join(quote_clickhouse_string(token) for token in token_list)
        filters.append(f"asset IN ({quoted})")
    where_sql = " AND ".join(filters)
    return f"""
        SELECT
            trade_key,
            proxy_wallet,
            side,
            asset AS token_id,
            condition_id,
            size,
            price,
            notional,
            trade_timestamp AS timestamp,
            trade_time,
            slug,
            event_slug,
            outcome,
            outcome_index,
            transaction_hash,
            source_endpoint,
            taker_only,
            market_filter_condition_id,
            market_id,
            gamma_market_id,
            market_slug
        FROM {table}
        WHERE {where_sql}
        ORDER BY market_id ASC, trade_timestamp ASC, trade_key ASC
    """
