"""Backfill frontend prices-history data into quant.market_token_frontend_price_1m."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any

from .frontend_client import DEFAULT_CLOB_API_BASE, FrontendPriceClient, FrontendPricePoint
from ..core.db import PostgresSettings, postgres_connection
from ..core.metadata import refresh_market_token_metadata
from ..core.schema import create_schema


SOURCE = "frontend"


def _utc_from_ts(timestamp: int) -> datetime:
    return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).replace(second=0, microsecond=0)


def insert_frontend_points(
    conn: Any,
    *,
    market_id: int,
    market_slug: str | None,
    token_id: str,
    token_side: str,
    points: list[FrontendPricePoint],
    fidelity_minutes: int,
) -> int:
    if not points:
        return 0
    rows = [
        (
            token_id,
            market_id,
            market_slug,
            token_side,
            _utc_from_ts(point.timestamp),
            int(point.timestamp),
            point.price,
            fidelity_minutes,
        )
        for point in points
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO quant.market_token_frontend_price_1m (
                token_id, market_id, market_slug, token_side,
                ts_minute, timestamp, price, fidelity_minutes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (token_id, ts_minute) DO UPDATE SET
                market_id = EXCLUDED.market_id,
                market_slug = EXCLUDED.market_slug,
                token_side = EXCLUDED.token_side,
                timestamp = EXCLUDED.timestamp,
                price = EXCLUDED.price,
                fidelity_minutes = EXCLUDED.fidelity_minutes,
                fetched_at = now()
            """,
            rows,
        )
        return len(rows)


def fetch_eligible_frontend_tokens(conn: Any, *, limit: int | None = None) -> list[dict[str, Any]]:
    params: list[Any] = []
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT %s"
        params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT m.token_id, m.market_id, m.market_slug, m.token_side
            FROM quant.market_token_metadata m
            JOIN quant.market_price_eligibility e ON e.token_id = m.token_id
            WHERE e.eligible = TRUE
            ORDER BY m.market_id ASC, m.outcome_index ASC, m.token_id ASC
            {limit_sql}
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def backfill_frontend_prices(
    conn: Any,
    client: FrontendPriceClient,
    *,
    start_ts: int,
    end_ts: int,
    fidelity_minutes: int = 1,
    limit: int | None = None,
) -> dict[str, int]:
    tokens = fetch_eligible_frontend_tokens(conn, limit=limit)
    rows_written = 0
    failures = 0
    for token in tokens:
        points, token_failures = client.fetch_segmented_prices_history(
            str(token["token_id"]),
            start_ts=int(start_ts),
            end_ts=int(end_ts),
            fidelity_minutes=fidelity_minutes,
        )
        failures += len(token_failures)
        rows_written += insert_frontend_points(
            conn,
            market_id=int(token["market_id"]),
            market_slug=token.get("market_slug"),
            token_id=str(token["token_id"]),
            token_side=str(token["token_side"]),
            points=points,
            fidelity_minutes=fidelity_minutes,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.market_price_build_market_state (
                    source, token_id, market_id, market_slug, token_side,
                    status, last_complete_ts, attempt_count, last_error, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s), %s, %s, now())
                ON CONFLICT (source, token_id) DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    market_slug = EXCLUDED.market_slug,
                    token_side = EXCLUDED.token_side,
                    status = EXCLUDED.status,
                    last_complete_ts = EXCLUDED.last_complete_ts,
                    attempt_count = quant.market_price_build_market_state.attempt_count + EXCLUDED.attempt_count,
                    last_error = EXCLUDED.last_error,
                    updated_at = now()
                """,
                (
                    SOURCE,
                    token["token_id"],
                    token["market_id"],
                    token.get("market_slug"),
                    token["token_side"],
                    "error" if token_failures else "complete",
                    int(end_ts),
                    1,
                    "; ".join(f.error for f in token_failures)[:4000] if token_failures else None,
                ),
            )
    return {"tokens": len(tokens), "rows_written": rows_written, "failures": failures}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill quant frontend prices-history data once.")
    parser.add_argument("--start-ts", type=int, required=True)
    parser.add_argument("--end-ts", type=int, required=True)
    parser.add_argument("--fidelity-minutes", type=int, default=1)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--refresh-metadata", action="store_true")
    parser.add_argument("--clob-api-base", default=os.environ.get("POLYMARKET_CLOB_API_BASE", DEFAULT_CLOB_API_BASE))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    with postgres_connection(PostgresSettings()) as conn:
        create_schema(conn)
        if args.refresh_metadata:
            refresh_market_token_metadata(conn)
        client = FrontendPriceClient(clob_api_base=args.clob_api_base)
        result = backfill_frontend_prices(
            conn,
            client,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            fidelity_minutes=args.fidelity_minutes,
            limit=args.limit,
        )
    print(result)


if __name__ == "__main__":
    main()
