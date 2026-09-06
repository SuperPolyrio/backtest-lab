"""Read helpers for future /quant API endpoints."""

from __future__ import annotations

import re
from typing import Any, Mapping

from quant.backtest.calibration import build_calibration_report, empty_calibration_report
from quant.backtest.cost_calibration import build_cost_calibration_report, empty_cost_calibration_report
from quant.backtest.orders import enrich_order_evidence_fields
from quant.backtest.replay_contract import build_backtest_replay


MATCHUP_RE = re.compile(r"\s+(?:vs\.?|v\.?)\s+", re.IGNORECASE)
NON_EVENT_TITLE_PREFIXES = {"spread", "total", "moneyline", "winner", "will"}


def _clean_label(value: str | None) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text.removesuffix("?").strip()


def _title_event_prefix(title: str | None) -> str | None:
    text = _clean_label(title)
    if ":" not in text:
        return None
    prefix = _clean_label(text.split(":", 1)[0])
    if not prefix:
        return None
    first_word = prefix.split(" ", 1)[0].lower()
    if first_word in NON_EVENT_TITLE_PREFIXES:
        return None
    return prefix


def _label_from_title_suffix(title: str | None) -> str | None:
    text = _clean_label(title)
    if ":" not in text:
        return None
    suffix = _clean_label(text.split(":", 1)[1])
    return suffix or None


def infer_outcome_label(market_title: str | None, token_side: str | None, *, event_scope: bool = False) -> str:
    """Return a display label for a token/outcome without changing stored prices."""

    side = str(token_side or "").upper()
    title = _clean_label(market_title)
    if event_scope:
        suffix = _label_from_title_suffix(title)
        if suffix:
            return suffix
    parts = [part.strip() for part in MATCHUP_RE.split(title, maxsplit=1) if part.strip()]
    if len(parts) == 2:
        return parts[0] if side == "YES" else parts[1]
    suffix = _label_from_title_suffix(title)
    if suffix:
        return suffix if side == "YES" else f"Not {suffix}"
    if title and side == "YES" and not title.lower().startswith("will "):
        return title
    return "Yes" if side == "YES" else "No"


def _market_payload(rows: list[dict[str, Any]], *, market_slug: str, source: str, scope: str, x_axis: str) -> dict[str, Any]:
    first = next((row for row in rows if row.get("market_slug") == market_slug), rows[0] if rows else {})
    return {
        "market_id": first.get("market_id"),
        "market_slug": first.get("market_slug") or market_slug,
        "market_title": first.get("market_title"),
        "condition_id": first.get("condition_id"),
        "end_date": first.get("end_date"),
        "source": source,
        "scope": scope,
        "x_axis": x_axis,
    }


def get_quant_price_markets(
    conn: Any,
    *,
    search: str | None = None,
    token_side: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Return markets that actually have quant price rows available."""

    params: list[Any] = []
    search_text = (search or "").strip().lower()
    if search_text:
        text = f"%{search_text}%"
        prefix_text = f"{search_text}%"
        slug_prefix_text = f"{search_text}-%"
        slug_token_text = f"%-{search_text}-%"
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH titled AS (
                    SELECT
                        p.market_id,
                        p.market_slug,
                        COALESCE(md.token_side, 'YES') AS token_side,
                        COALESCE(p.block_rows_written, 0) AS block_rows,
                        p.first_orderfilled_block AS first_block,
                        COALESCE(p.max_block_complete, p.last_orderfilled_block) AS last_block,
                        NULL::numeric AS latest_block_price,
                        p.updated_at AS latest_block_at,
                        COALESCE(p.frontend_rows_written, 0) AS frontend_rows,
                        extract(epoch FROM p.min_frontend_complete_ts)::bigint AS first_ts,
                        extract(epoch FROM p.max_frontend_complete_ts)::bigint AS last_ts,
                        NULL::numeric AS latest_frontend_price,
                        p.updated_at AS latest_frontend_at,
                        max(md.market_title) AS market_title,
                        max(md.condition_id) AS condition_id,
                        max(md.end_date) AS end_date,
                        CASE
                            WHEN lower(p.market_slug) = %s THEN 0
                            WHEN lower(p.market_slug) LIKE %s THEN 1
                            WHEN lower(p.market_slug) LIKE %s THEN 2
                            WHEN lower(p.market_slug) LIKE %s THEN 3
                            WHEN lower(max(md.market_title)) = %s THEN 4
                            WHEN lower(max(md.market_title)) LIKE %s THEN 5
                            ELSE 9
                        END AS search_rank
                    FROM quant.market_price_build_market_progress p
                    LEFT JOIN quant.market_token_metadata md
                        ON md.market_id = p.market_id AND md.token_side = COALESCE(%s, 'YES')
                    WHERE p.market_slug IS NOT NULL
                      AND (COALESCE(p.block_rows_written, 0) > 0 OR COALESCE(p.frontend_rows_written, 0) > 0)
                      AND (lower(p.market_slug) LIKE %s OR lower(md.market_title) LIKE %s)
                    GROUP BY
                        p.market_id, p.market_slug, md.token_side, p.block_rows_written,
                        p.first_orderfilled_block, p.max_block_complete, p.last_orderfilled_block,
                        p.frontend_rows_written, p.min_frontend_complete_ts, p.max_frontend_complete_ts,
                        p.updated_at
                )
                SELECT *
                FROM titled
                ORDER BY search_rank ASC,
                         (block_rows + frontend_rows) DESC,
                         market_slug ASC
                LIMIT %s
                """,
                [
                    search_text,
                    slug_prefix_text,
                    prefix_text,
                    slug_token_text,
                    search_text,
                    prefix_text,
                    token_side.upper() if token_side else None,
                    text,
                    text,
                    int(limit),
                ],
            )
            return [dict(row) for row in cur.fetchall()]

    filters: list[str] = []
    if search_text:
        filters.append("(lower(market_slug) LIKE %s OR lower(market_title) LIKE %s)")
        text = f"%{search_text}%"
        params.extend([text, text])
    if token_side:
        filters.append("token_side = %s")
        params.append(token_side.upper())
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(int(limit))

    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH titled AS (
                SELECT
                    p.market_id,
                    p.market_slug,
                    COALESCE(md.token_side, 'YES') AS token_side,
                    COALESCE(p.block_rows_written, 0) AS block_rows,
                    p.first_orderfilled_block AS first_block,
                    COALESCE(p.max_block_complete, p.last_orderfilled_block) AS last_block,
                    NULL::numeric AS latest_block_price,
                    p.updated_at AS latest_block_at,
                    COALESCE(p.frontend_rows_written, 0) AS frontend_rows,
                    extract(epoch FROM p.min_frontend_complete_ts)::bigint AS first_ts,
                    extract(epoch FROM p.max_frontend_complete_ts)::bigint AS last_ts,
                    NULL::numeric AS latest_frontend_price,
                    p.updated_at AS latest_frontend_at,
                    max(md.market_title) AS market_title,
                    max(md.condition_id) AS condition_id,
                    max(md.end_date) AS end_date
                FROM quant.market_price_build_market_progress p
                LEFT JOIN quant.market_token_metadata md
                    ON md.market_id = p.market_id AND md.token_side = 'YES'
                WHERE p.market_slug IS NOT NULL
                  AND (COALESCE(p.block_rows_written, 0) > 0 OR COALESCE(p.frontend_rows_written, 0) > 0)
                GROUP BY
                    p.market_id, p.market_slug, md.token_side, p.block_rows_written,
                    p.first_orderfilled_block, p.max_block_complete, p.last_orderfilled_block,
                    p.frontend_rows_written, p.min_frontend_complete_ts, p.max_frontend_complete_ts,
                    p.updated_at
            )
            SELECT *
            FROM titled
            {where_sql}
            ORDER BY GREATEST(COALESCE(last_ts, 0), COALESCE(last_block, 0)) DESC,
                     (block_rows + frontend_rows) DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_quant_price_events(
    conn: Any,
    *,
    search: str | None = None,
    limit: int = 50,
    default_only: bool = False,
) -> list[dict[str, Any]]:
    where_params: list[Any] = []
    filters = [
        "member_count > 0",
        """
        NOT (
            member_count = 1
            AND COALESCE(source, '') LIKE 'fallback.%%'
        )
        """,
    ]
    search_text = (search or "").strip().lower()
    if search_text:
        text = f"%{search_text}%"
        conditions = [
            """
            (
                lower(event_slug) LIKE %s
                OR lower(event_title) LIKE %s
                OR EXISTS (
                    SELECT 1
                    FROM quant.market_event_members sm
                    WHERE sm.event_slug = ranked.event_slug
                      AND (
                        lower(sm.market_slug) LIKE %s
                        OR lower(sm.question) LIKE %s
                        OR lower(sm.outcome_label) LIKE %s
                      )
                )
            )
            """
        ]
        where_params.extend([text, text, text, text, text])
        terms = [term for term in re.split(r"[^a-z0-9]+", search_text) if term]
        if len(terms) > 1:
            conditions.append(
                "("
                + " AND ".join(["lower(event_slug || ' ' || event_title) LIKE %s" for _ in terms])
                + ")"
            )
            where_params.extend([f"%{term}%" for term in terms])
        filters.append("(" + " OR ".join(conditions) + ")")
    if default_only:
        filters.extend(
            [
                "ready_members > 0",
                "(block_rows + frontend_rows) > 0",
                "(end_date IS NULL OR end_date >= now())",
                """
                lower(COALESCE(status, '')) NOT IN (
                    'closed', 'complete', 'ended', 'ended_awaiting_oracle',
                    'resolved', 'settled'
                )
                """,
            ]
        )
    where_sql = "WHERE " + " AND ".join(filters)
    order_sql = (
        """
                CASE WHEN lower(COALESCE(status, '')) IN ('active', 'open', 'trading') THEN 0 ELSE 1 END,
                last_block DESC NULLS LAST,
                latest_frontend_at DESC NULLS LAST,
                ready_members DESC,
                end_date ASC NULLS LAST,
                event_title ASC
        """
        if default_only
        else """
                CASE
                    WHEN lower(event_slug) = %s THEN 0
                    WHEN lower(event_title) = %s THEN 1
                    WHEN lower(event_slug) LIKE %s THEN 2
                    WHEN lower(event_title) LIKE %s THEN 3
                    ELSE 9
                END,
                ready_members DESC,
                (block_rows + frontend_rows) DESC,
                event_title ASC
        """
    )
    params = [
        *where_params,
        *(
            []
            if default_only
            else [search_text, search_text, f"{search_text}%", f"{search_text}%"]
        ),
        int(limit),
    ]
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH ranked AS (
                SELECT
                    e.event_id,
                    e.event_slug,
                    e.event_title,
                    e.event_category,
                    e.event_subcategory,
                    e.event_image_url,
                    e.event_icon_url,
                    e.start_date,
                    e.end_date,
                    e.resolution_date,
                    e.status,
                    e.grouping_confidence,
                    e.source,
                    COUNT(m.market_id) AS member_count,
                    COUNT(*) FILTER (WHERE m.coverage_status IN ('ready', 'partial')) AS ready_members,
                    COALESCE(SUM(m.block_rows), 0) AS block_rows,
                    COALESCE(SUM(m.frontend_rows), 0) AS frontend_rows,
                    COALESCE(SUM(m.orderfilled_rows), 0) AS orderfilled_rows,
                    MIN(m.latest_block) AS first_block,
                    MAX(m.latest_block) AS last_block,
                    MAX(m.latest_timestamp) AS latest_frontend_at
                FROM quant.market_event_metadata e
                JOIN quant.market_event_members m ON m.event_slug = e.event_slug
                GROUP BY
                    e.event_id, e.event_slug, e.event_title, e.event_category,
                    e.event_subcategory, e.event_image_url, e.event_icon_url,
                    e.start_date, e.end_date, e.resolution_date, e.status,
                    e.grouping_confidence, e.source
            )
            SELECT
                'event' AS item_kind,
                event_id,
                event_slug,
                event_slug AS market_slug,
                event_title,
                event_title AS market_title,
                event_category,
                event_subcategory,
                event_image_url,
                event_icon_url,
                status,
                grouping_confidence,
                source,
                'EVENT' AS token_side,
                member_count AS outcome_count,
                member_count AS total_members,
                ready_members,
                block_rows,
                frontend_rows,
                orderfilled_rows,
                first_block,
                last_block,
                extract(epoch FROM start_date)::bigint AS first_ts,
                extract(epoch FROM end_date)::bigint AS last_ts,
                latest_frontend_at,
                end_date
            FROM ranked
            {where_sql}
            ORDER BY {order_sql}
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def _fetch_market_tokens(conn: Any, *, market_slug: str, scope: str, max_outcomes: int) -> tuple[list[dict[str, Any]], str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                market_id, market_slug, market_title, condition_id, end_date,
                token_id, token_side, outcome_index
            FROM quant.market_token_metadata
            WHERE market_slug = %s
            ORDER BY market_id, outcome_index NULLS LAST, token_side, token_id
            """,
            (market_slug,),
        )
        base_rows = [dict(row) for row in cur.fetchall()]
        if not base_rows:
            return [], "market"
        event_prefix = _title_event_prefix(base_rows[0].get("market_title"))
        effective_scope = "event" if scope == "event" or (scope == "auto" and event_prefix) else "market"
        if effective_scope != "event" or not event_prefix:
            return base_rows[: int(max_outcomes)], "market"
        cur.execute(
            """
            WITH markets AS (
                SELECT DISTINCT market_id, market_slug, market_title, condition_id, end_date
                FROM quant.market_token_metadata
                WHERE market_title ILIKE %s
                ORDER BY market_title ASC, market_id ASC
                LIMIT %s
            )
            SELECT
                m.market_id, m.market_slug, m.market_title, m.condition_id, m.end_date,
                md.token_id, md.token_side, md.outcome_index
            FROM markets m
            JOIN quant.market_token_metadata md
                ON md.market_id = m.market_id AND md.token_side = 'YES'
            ORDER BY m.market_title ASC, md.outcome_index NULLS LAST, md.token_id
            """,
            (f"{event_prefix}:%", int(max_outcomes)),
        )
        return [dict(row) for row in cur.fetchall()], "event"


def _complement_side(token_side: str | None) -> str:
    return "NO" if str(token_side or "").upper() == "YES" else "YES"


def _no_label_from_member(question: str | None, outcome_label: str | None) -> str:
    title = _clean_label(question)
    parts = [part.strip() for part in MATCHUP_RE.split(title, maxsplit=1) if part.strip()]
    if len(parts) == 2 and _clean_label(outcome_label).lower() == parts[0].lower():
        return parts[1]
    label = _clean_label(outcome_label)
    return f"{label} No" if label else "No"


def _fetch_token_price_points(
    cur: Any,
    *,
    token_id: str | None,
    source: str,
    from_ts: int | None = None,
    to_ts: int | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    limit: int = 2500,
) -> list[dict[str, Any]]:
    if not token_id:
        return []
    params: list[Any] = [str(token_id)]
    if source == "frontend":
        filters = ["token_id = %s"]
        if from_ts is not None:
            filters.append("timestamp >= %s")
            params.append(int(from_ts))
        if to_ts is not None:
            filters.append("timestamp <= %s")
            params.append(int(to_ts))
        params.append(int(limit))
        cur.execute(
            f"""
            WITH limited AS (
                SELECT token_id, market_id, market_slug, token_side, ts_minute, timestamp, price
                FROM quant.market_token_frontend_price_1m
                WHERE {" AND ".join(filters)}
                ORDER BY ts_minute DESC
                LIMIT %s
            )
            SELECT *
            FROM limited
            ORDER BY ts_minute ASC
            """,
            params,
        )
        return [
            {
                "x": row["timestamp"],
                "timestamp": row["timestamp"],
                "token_id": row["token_id"],
                "token_side": row["token_side"],
                "price": row["price"],
                "volume": 0,
                "is_implied": False,
            }
            for row in cur.fetchall()
        ]

    filters = ["token_id = %s"]
    if from_block is not None:
        filters.append("block_number >= %s")
        params.append(int(from_block))
    if to_block is not None:
        filters.append("block_number <= %s")
        params.append(int(to_block))
    params.append(int(limit))
    cur.execute(
        f"""
        WITH limited AS (
            SELECT
                token_id, market_id, market_slug, token_side, block_number,
                close_price, yes_probability_close, vwap_price, yes_probability_vwap,
                volume, trade_count, block_timestamp
            FROM quant.market_token_block_close
            WHERE {" AND ".join(filters)}
            ORDER BY block_number DESC
            LIMIT %s
        )
        SELECT *
        FROM limited
        ORDER BY block_number ASC
        """,
        params,
    )
    return [
        {
            "x": row["block_number"],
            "block_number": row["block_number"],
            "timestamp": row.get("block_timestamp"),
            "token_id": row["token_id"],
            "token_side": row["token_side"],
            "price": row["close_price"],
            "yes_probability_close": row["yes_probability_close"],
            "vwap_price": row["vwap_price"],
            "yes_probability_vwap": row["yes_probability_vwap"],
            "volume": row["volume"],
            "trade_count": row["trade_count"],
            "is_implied": False,
        }
        for row in cur.fetchall()
    ]


def _fetch_token_price_points_sampled(
    cur: Any,
    *,
    token_id: str | None,
    source: str,
    from_ts: int | None = None,
    to_ts: int | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    max_points: int = 900,
) -> list[dict[str, Any]]:
    if not token_id:
        return []
    # M4 keeps each bucket's open/high/low/close observations in chronological
    # order. Unlike a low/high-only sample, it preserves the path entering and
    # leaving a bucket and does not invent price-ordered zig-zags.
    bucket_count = max(1, (int(max_points or 900) - 2) // 4)
    params: list[Any] = [str(token_id)]
    if source == "frontend":
        filters = ["token_id = %s"]
        if from_ts is not None:
            filters.append("timestamp >= %s")
            params.append(int(from_ts))
        if to_ts is not None:
            filters.append("timestamp <= %s")
            params.append(int(to_ts))
        params.extend([bucket_count, int(max_points)])
        cur.execute(
            f"""
            WITH ordered AS (
                SELECT
                    token_id, market_id, market_slug, token_side, ts_minute,
                    timestamp, price,
                    lag(timestamp) OVER (ORDER BY ts_minute ASC) AS previous_x,
                    row_number() OVER (ORDER BY ts_minute ASC) AS rn,
                    count(*) OVER () AS total_rows
                FROM quant.market_token_frontend_price_1m
                WHERE {" AND ".join(filters)}
            ),
            stats AS (
                SELECT
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY timestamp - previous_x) AS median_step,
                    percentile_cont(0.99) WITHIN GROUP (ORDER BY timestamp - previous_x) AS p99_step,
                    percentile_cont(0.999) WITHIN GROUP (ORDER BY timestamp - previous_x) AS p999_step
                FROM ordered
                WHERE previous_x IS NOT NULL AND timestamp > previous_x
            ),
            bucketed AS (
                SELECT ordered.*,
                    floor((rn - 1)::numeric / GREATEST(1, ceil(total_rows::numeric / %s)::int)) AS bucket_id
                    , GREATEST(
                        1,
                        COALESCE(stats.median_step, 1) * 4,
                        COALESCE(stats.p99_step, stats.median_step, 1) * 2
                      ) AS stale_threshold
                    , previous_x IS NOT NULL
                      AND timestamp - previous_x > GREATEST(
                          1,
                          COALESCE(stats.median_step, 1) * 30,
                          COALESCE(stats.p99_step, stats.median_step, 1) * 8,
                          COALESCE(stats.p999_step, stats.p99_step, stats.median_step, 1) * 4
                      ) AS gap_before
                FROM ordered CROSS JOIN stats
            ),
            ranked AS (
                SELECT *,
                    row_number() OVER (PARTITION BY bucket_id ORDER BY ts_minute ASC) AS open_rank,
                    row_number() OVER (PARTITION BY bucket_id ORDER BY ts_minute DESC) AS close_rank,
                    row_number() OVER (PARTITION BY bucket_id ORDER BY price ASC, ts_minute ASC) AS lo_rank,
                    row_number() OVER (PARTITION BY bucket_id ORDER BY price DESC, ts_minute ASC) AS hi_rank
                FROM bucketed
            )
            SELECT token_id, market_id, market_slug, token_side, ts_minute, timestamp, price,
                   total_rows, previous_x, gap_before, stale_threshold
            FROM ranked
            WHERE rn = 1 OR rn = total_rows
               OR open_rank = 1 OR close_rank = 1 OR lo_rank = 1 OR hi_rank = 1 OR gap_before
            ORDER BY ts_minute ASC
            LIMIT %s
            """,
            params,
        )
        return [
            {
                "x": row["timestamp"],
                "timestamp": row["timestamp"],
                "token_id": row["token_id"],
                "token_side": row["token_side"],
                "price": row["price"],
                "volume": 0,
                "is_implied": False,
                "_raw_count": row["total_rows"],
                "_aggregation": "m4",
                "_stale_threshold": float(row["stale_threshold"]),
                "gap_before": bool(row.get("gap_before")),
                "gap_width": int(row["timestamp"] - row["previous_x"]) if row.get("gap_before") else None,
                "_gap_from_x": row.get("previous_x"),
            }
            for row in cur.fetchall()
        ]

    filters = ["token_id = %s"]
    if from_block is not None:
        filters.append("block_number >= %s")
        params.append(int(from_block))
    if to_block is not None:
        filters.append("block_number <= %s")
        params.append(int(to_block))
    params.extend([bucket_count, int(max_points)])
    cur.execute(
        f"""
            WITH ordered AS (
            SELECT
                token_id, market_id, market_slug, token_side, block_number,
                close_price, yes_probability_close, vwap_price, yes_probability_vwap,
                volume, trade_count, block_timestamp,
                lag(block_number) OVER (ORDER BY block_number ASC) AS previous_x,
                row_number() OVER (ORDER BY block_number ASC) AS rn,
                count(*) OVER () AS total_rows
            FROM quant.market_token_block_close
            WHERE {" AND ".join(filters)}
        ),
        stats AS (
            SELECT
                percentile_cont(0.5) WITHIN GROUP (ORDER BY block_number - previous_x) AS median_step,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY block_number - previous_x) AS p99_step,
                percentile_cont(0.999) WITHIN GROUP (ORDER BY block_number - previous_x) AS p999_step
            FROM ordered
            WHERE previous_x IS NOT NULL AND block_number > previous_x
        ),
        bucketed AS (
            SELECT ordered.*,
                floor((rn - 1)::numeric / GREATEST(1, ceil(total_rows::numeric / %s)::int)) AS bucket_id
                , GREATEST(
                    1,
                    COALESCE(stats.median_step, 1) * 4,
                    COALESCE(stats.p99_step, stats.median_step, 1) * 2
                  ) AS stale_threshold
                , previous_x IS NOT NULL
                  AND block_number - previous_x > GREATEST(
                      1,
                      COALESCE(stats.median_step, 1) * 30,
                      COALESCE(stats.p99_step, stats.median_step, 1) * 8,
                      COALESCE(stats.p999_step, stats.p99_step, stats.median_step, 1) * 4
                  ) AS gap_before
            FROM ordered CROSS JOIN stats
        ),
            ranked AS (
                SELECT *,
                    row_number() OVER (PARTITION BY bucket_id ORDER BY block_number ASC) AS open_rank,
                    row_number() OVER (PARTITION BY bucket_id ORDER BY block_number DESC) AS close_rank,
                    row_number() OVER (PARTITION BY bucket_id ORDER BY close_price ASC, block_number ASC) AS lo_rank,
                    row_number() OVER (PARTITION BY bucket_id ORDER BY close_price DESC, block_number ASC) AS hi_rank
                FROM bucketed
        )
        SELECT
            token_id, market_id, market_slug, token_side, block_number,
            close_price, yes_probability_close, vwap_price, yes_probability_vwap,
            volume, trade_count, block_timestamp, total_rows, previous_x, gap_before, stale_threshold
        FROM ranked
        WHERE rn = 1 OR rn = total_rows
           OR open_rank = 1 OR close_rank = 1 OR lo_rank = 1 OR hi_rank = 1 OR gap_before
        ORDER BY block_number ASC
        LIMIT %s
        """,
        params,
    )
    return [
        {
            "x": row["block_number"],
            "block_number": row["block_number"],
            "timestamp": row.get("block_timestamp"),
            "token_id": row["token_id"],
            "token_side": row["token_side"],
            "price": row["close_price"],
            "yes_probability_close": row["yes_probability_close"],
            "vwap_price": row["vwap_price"],
            "yes_probability_vwap": row["yes_probability_vwap"],
            "volume": row["volume"],
            "trade_count": row["trade_count"],
            "is_implied": False,
            "_raw_count": row["total_rows"],
            "_aggregation": "m4",
            "_stale_threshold": float(row["stale_threshold"]),
            "gap_before": bool(row.get("gap_before")),
            "gap_width": int(row["block_number"] - row["previous_x"]) if row.get("gap_before") else None,
            "_gap_from_x": row.get("previous_x"),
        }
        for row in cur.fetchall()
    ]


def _fetch_latest_points_for_tokens(
    cur: Any,
    *,
    token_ids: list[str | None],
    source: str,
    from_ts: int | None = None,
    to_ts: int | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
) -> dict[str, dict[str, Any]]:
    tokens = [str(token_id) for token_id in token_ids if token_id]
    if not tokens:
        return {}
    params: list[Any] = [tokens]
    if source == "frontend":
        filters = ["token_id = ANY(%s)"]
        if from_ts is not None:
            filters.append("timestamp >= %s")
            params.append(int(from_ts))
        if to_ts is not None:
            filters.append("timestamp <= %s")
            params.append(int(to_ts))
        cur.execute(
            f"""
            SELECT DISTINCT ON (token_id)
                token_id, market_id, market_slug, token_side, ts_minute, timestamp, price
            FROM quant.market_token_frontend_price_1m
            WHERE {" AND ".join(filters)}
            ORDER BY token_id, ts_minute DESC
            """,
            params,
        )
        return {
            str(row["token_id"]): {
                "x": row["timestamp"],
                "timestamp": row["timestamp"],
                "token_id": row["token_id"],
                "token_side": row["token_side"],
                "price": row["price"],
                "volume": 0,
                "is_implied": False,
            }
            for row in cur.fetchall()
        }

    filters = ["token_id = ANY(%s)"]
    if from_block is not None:
        filters.append("block_number >= %s")
        params.append(int(from_block))
    if to_block is not None:
        filters.append("block_number <= %s")
        params.append(int(to_block))
    cur.execute(
        f"""
        SELECT DISTINCT ON (token_id)
            token_id, market_id, market_slug, token_side, block_number,
            close_price, yes_probability_close, vwap_price, yes_probability_vwap,
            volume, trade_count, block_timestamp
        FROM quant.market_token_block_close
        WHERE {" AND ".join(filters)}
        ORDER BY token_id, block_number DESC
        """,
        params,
    )
    return {
        str(row["token_id"]): {
            "x": row["block_number"],
            "block_number": row["block_number"],
            "timestamp": row.get("block_timestamp"),
            "token_id": row["token_id"],
            "token_side": row["token_side"],
            "price": row["close_price"],
            "yes_probability_close": row["yes_probability_close"],
            "vwap_price": row["vwap_price"],
            "yes_probability_vwap": row["yes_probability_vwap"],
            "volume": row["volume"],
            "trade_count": row["trade_count"],
            "is_implied": False,
        }
        for row in cur.fetchall()
    }


def _point_x(point: dict[str, Any]) -> int:
    value = point.get("x") or point.get("block_number") or point.get("timestamp") or 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _point_price(point: dict[str, Any]) -> float:
    try:
        return float(point.get("price") or 0)
    except (TypeError, ValueError):
        return 0.0


def _latest_yes_score(yes_point: dict[str, Any] | None, no_point: dict[str, Any] | None) -> float:
    if yes_point:
        return _point_price(yes_point)
    if no_point:
        return max(0.0, min(1.0, 1.0 - _point_price(no_point)))
    return 0.0


def _minmax_downsample(points: list[dict[str, Any]], max_points: int) -> list[dict[str, Any]]:
    """Return an M4 sample while retaining the historical helper name.

    Callers already depend on this private name. Keeping it avoids a broad API
    churn while changing the semantics from low/high-only to chronological
    open/high/low/close sampling.
    """
    max_points = max(2, int(max_points or 600))
    raw_count = len(points)
    by_x: dict[int, dict[str, Any]] = {}
    for point in sorted(points, key=_point_x):
        by_x[_point_x(point)] = point
    points = list(by_x.values())
    steps = [
        _point_x(current) - _point_x(previous)
        for previous, current in zip(points, points[1:])
        if _point_x(current) > _point_x(previous)
    ]

    def percentile(ratio: float, fallback: float = 1.0) -> float:
        if not steps:
            return fallback
        ordered = sorted(steps)
        return float(ordered[min(len(ordered) - 1, int((len(ordered) - 1) * ratio))])

    median_step = max(1.0, percentile(0.5))
    stale_threshold = max(1.0, median_step * 4.0, percentile(0.99, median_step) * 2.0)
    gap_threshold = max(
        1.0,
        median_step * 30.0,
        percentile(0.99, median_step) * 8.0,
        percentile(0.999, median_step) * 4.0,
    )
    mandatory_x = {_point_x(points[0]), _point_x(points[-1])} if points else set()
    for previous, current in zip(points, points[1:]):
        width = _point_x(current) - _point_x(previous)
        if width > gap_threshold:
            current["gap_before"] = True
            current["gap_width"] = width
            mandatory_x.update((_point_x(previous), _point_x(current)))
    for point in points:
        point["_stale_threshold"] = stale_threshold
    if len(points) <= max_points:
        return points
    bucket_count = max(1, (max_points - 2) // 4)
    middle = points[1:-1]
    bucket_size = max(1, (len(middle) + bucket_count - 1) // bucket_count)
    selected: dict[int, dict[str, Any]] = {x: by_x[x] for x in mandatory_x}
    for start in range(0, len(middle), bucket_size):
        bucket = middle[start:start + bucket_size]
        if not bucket:
            continue
        for point in (bucket[0], min(bucket, key=_point_price), max(bucket, key=_point_price), bucket[-1]):
            selected[_point_x(point)] = point
    sampled = sorted(selected.values(), key=_point_x)
    if len(sampled) > max_points:
        optional = [point for point in sampled if _point_x(point) not in mandatory_x]
        keep_optional = max(0, max_points - len(mandatory_x))
        stride = max(1, len(optional) // max(1, keep_optional))
        sampled = sorted(
            [by_x[x] for x in mandatory_x] + optional[::stride][:keep_optional],
            key=_point_x,
        )
    for point in sampled:
        point["_raw_count"] = raw_count
        point["_aggregation"] = "m4"
        point["_stale_threshold"] = stale_threshold
    return sampled


def _event_outcome_payload(
    *,
    member: dict[str, Any],
    event: dict[str, Any],
    label: str,
    no_label: str,
    yes_points: list[dict[str, Any]],
    no_points: list[dict[str, Any]],
) -> dict[str, Any]:
    raw_rows = int(yes_points[0].get("_raw_count") or len(yes_points)) if yes_points else 0
    aggregation = str(yes_points[0].get("_aggregation") or "raw") if yes_points else "raw"
    stale_threshold = yes_points[0].get("_stale_threshold") if yes_points else None
    return {
        "market_id": member.get("market_id"),
        "market_slug": member.get("market_slug"),
        "market_title": member.get("question"),
        "condition_id": member.get("condition_id"),
        "end_date": event.get("end_date"),
        "event_slug": member.get("event_slug"),
        "event_id": member.get("event_id"),
        "token_id": member.get("token_yes_id"),
        "token_side": "YES",
        "outcome_index": member.get("outcome_order"),
        "outcome_label": label,
        "outcome_key": member.get("outcome_key"),
        "coverage_status": member.get("coverage_status"),
        "buy_yes_token_id": member.get("token_yes_id"),
        "buy_yes_token_side": "YES",
        "buy_yes_label": label,
        "buy_yes_price": yes_points[-1]["price"] if yes_points else None,
        "buy_no_token_id": member.get("token_no_id"),
        "buy_no_token_side": "NO",
        "buy_no_label": no_label,
        "buy_no_price": no_points[-1]["price"] if no_points else None,
        "rows": len(yes_points),
        "raw_rows": raw_rows,
        "rendered_rows": len(yes_points),
        "aggregation": aggregation,
        "resolution": "event_block" if yes_points and yes_points[0].get("block_number") is not None else "event_time",
        "is_raw": aggregation == "raw",
        "is_imputed": any(bool(point.get("is_implied")) for point in yes_points),
        "stale_threshold": stale_threshold,
        "first_x": yes_points[0]["x"] if yes_points else None,
        "last_x": yes_points[-1]["x"] if yes_points else None,
        "latest_price": yes_points[-1]["price"] if yes_points else None,
        "points": yes_points,
        "complement_rows": len(no_points),
        "complement_first_x": no_points[0]["x"] if no_points else None,
        "complement_last_x": no_points[-1]["x"] if no_points else None,
        "complement_latest_price": no_points[-1]["price"] if no_points else None,
        "complement_points": no_points,
    }


def get_quant_event_members(
    conn: Any,
    *,
    event_slug: str,
    limit: int = 200,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                event_slug,
                event_id,
                market_id,
                market_slug,
                question,
                outcome_key,
                outcome_label,
                outcome_order,
                token_yes_id,
                token_no_id,
                condition_id,
                clob_token_ids,
                status,
                active,
                closed,
                resolved,
                coverage_status,
                block_rows,
                frontend_rows,
                orderfilled_rows,
                latest_yes,
                latest_no,
                latest_block,
                latest_timestamp,
                volume,
                liquidity,
                grouping_confidence,
                source,
                updated_at
            FROM quant.market_event_members
            WHERE event_slug = %s
            ORDER BY outcome_order ASC, outcome_label ASC, market_id ASC
            LIMIT %s
            """,
            (event_slug, int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def get_event_price_head(
    conn: Any,
    *,
    event_slug: str,
    price_source: str,
    from_ts: int | None = None,
    to_ts: int | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    max_outcomes: int = 100,
    top_n: int = 12,
) -> dict[str, Any]:
    """Return fast event metadata plus latest outcome prices only."""

    source = "orderfilled_block_close" if price_source == "orderfilled_block_close" else "frontend"
    top_n = max(1, min(int(top_n or 12), int(max_outcomes or 100)))
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM quant.market_event_metadata WHERE event_slug = %s", (event_slug,))
        event = dict(cur.fetchone() or {})
        if not event:
            return {
                "event": {
                    "event_slug": event_slug,
                    "event_title": event_slug,
                    "source": source,
                    "scope": "event_head",
                    "x_axis": "block_number" if source == "orderfilled_block_close" else "timestamp",
                },
                "members": [],
                "outcomes": [],
                "count": 0,
                "status": "missing",
                "cache_status": "missing",
            }

        cur.execute(
            """
            SELECT *
            FROM quant.market_event_members
            WHERE event_slug = %s
            ORDER BY outcome_order ASC, outcome_label ASC, market_id ASC
            LIMIT %s
            """,
            (event_slug, int(max_outcomes)),
        )
        members = [dict(row) for row in cur.fetchall()]
        latest_by_token = _fetch_latest_points_for_tokens(
            cur,
            token_ids=[token_id for member in members for token_id in (member.get("token_yes_id"), member.get("token_no_id"))],
            source=source,
            from_ts=from_ts,
            to_ts=to_ts,
            from_block=from_block,
            to_block=to_block,
        )

    latest_items: list[dict[str, Any]] = []
    for index, member in enumerate(members):
        yes_latest = latest_by_token.get(str(member.get("token_yes_id") or ""))
        no_latest = latest_by_token.get(str(member.get("token_no_id") or ""))
        latest_items.append({
            "index": index,
            "member": member,
            "yes_latest": [yes_latest] if yes_latest else [],
            "no_latest": [no_latest] if no_latest else [],
            "score": _latest_yes_score(yes_latest, no_latest),
            "latest_x": max(_point_x(yes_latest) if yes_latest else 0, _point_x(no_latest) if no_latest else 0),
        })

    ranked = sorted(latest_items, key=lambda row: (row["score"], row["latest_x"]), reverse=True)
    keep_indexes = {row["index"] for row in ranked[:top_n]}
    outcomes: list[dict[str, Any]] = []
    for item in latest_items:
        if item["index"] not in keep_indexes:
            continue
        member = item["member"]
        label = _clean_label(member.get("outcome_label")) or _clean_label(member.get("question"))
        no_label = _no_label_from_member(member.get("question"), label)
        outcomes.append(_event_outcome_payload(
            member=member,
            event=event,
            label=label,
            no_label=no_label,
            yes_points=item["yes_latest"],
            no_points=item["no_latest"],
        ))

    return {
        "event": {
            **event,
            "source": source,
            "scope": "event_head",
            "x_axis": "block_number" if source == "orderfilled_block_close" else "timestamp",
        },
        "members": members,
        "outcomes": outcomes,
        "count": len(outcomes),
        "status": "partial" if outcomes else "empty",
        "cache_status": "head",
        "tile": {
            "range": "head",
            "resolution": "latest",
            "top_n": top_n,
            "max_points": 1,
        },
    }


def get_event_price_series(
    conn: Any,
    *,
    event_slug: str,
    price_source: str,
    from_ts: int | None = None,
    to_ts: int | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    limit: int = 2500,
    max_outcomes: int = 100,
) -> dict[str, Any]:
    source = "orderfilled_block_close" if price_source == "orderfilled_block_close" else "frontend"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.market_event_metadata
            WHERE event_slug = %s
            """,
            (event_slug,),
        )
        event = dict(cur.fetchone() or {})
        if not event:
            return {
                "event": {
                    "event_slug": event_slug,
                    "event_title": event_slug,
                    "source": source,
                    "scope": "event",
                    "x_axis": "block_number" if source == "orderfilled_block_close" else "timestamp",
                },
                "members": [],
                "outcomes": [],
                "count": 0,
            }

        cur.execute(
            """
            SELECT *
            FROM quant.market_event_members
            WHERE event_slug = %s
            ORDER BY outcome_order ASC, outcome_label ASC, market_id ASC
            LIMIT %s
            """,
            (event_slug, int(max_outcomes)),
        )
        members = [dict(row) for row in cur.fetchall()]

        outcomes: list[dict[str, Any]] = []
        for member in members:
            yes_points = _fetch_token_price_points(
                cur,
                token_id=member.get("token_yes_id"),
                source=source,
                from_ts=from_ts,
                to_ts=to_ts,
                from_block=from_block,
                to_block=to_block,
                limit=limit,
            )
            no_points = _fetch_token_price_points(
                cur,
                token_id=member.get("token_no_id"),
                source=source,
                from_ts=from_ts,
                to_ts=to_ts,
                from_block=from_block,
                to_block=to_block,
                limit=limit,
            )
            label = _clean_label(member.get("outcome_label")) or _clean_label(member.get("question"))
            no_label = _no_label_from_member(member.get("question"), label)
            outcomes.append(_event_outcome_payload(
                member=member,
                event=event,
                label=label,
                no_label=no_label,
                yes_points=yes_points,
                no_points=no_points,
            ))

    return {
        "event": {
            **event,
            "source": source,
            "scope": "event",
            "x_axis": "block_number" if source == "orderfilled_block_close" else "timestamp",
        },
        "members": members,
        "outcomes": outcomes,
        "count": len(outcomes),
    }


def get_event_price_tile(
    conn: Any,
    *,
    event_slug: str,
    price_source: str,
    from_ts: int | None = None,
    to_ts: int | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    limit: int = 2500,
    max_outcomes: int = 100,
    top_n: int = 12,
    max_points: int = 600,
    tile_range: str = "latest",
    resolution: str = "auto",
) -> dict[str, Any]:
    source = "orderfilled_block_close" if price_source == "orderfilled_block_close" else "frontend"
    top_n = max(1, min(int(top_n or 12), int(max_outcomes or 100)))
    normalized_range = str(tile_range or "latest").strip().lower()
    max_points_cap = 2500
    max_points = max(50, min(int(max_points or 600), max_points_cap))
    if normalized_range in {"all", "full"}:
        source_limit = min(max(int(limit or 0), 250000, max_points * 16), 250000)
    else:
        source_limit = min(int(limit or 2500), max(250, max_points * 2))
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM quant.market_event_metadata WHERE event_slug = %s", (event_slug,))
        event = dict(cur.fetchone() or {})
        if not event:
            return {
                "event": {
                    "event_slug": event_slug,
                    "event_title": event_slug,
                    "source": source,
                    "scope": "event_tile",
                    "x_axis": "block_number" if source == "orderfilled_block_close" else "timestamp",
                },
                "members": [],
                "outcomes": [],
                "count": 0,
                "tile": {"range": tile_range, "resolution": resolution, "top_n": top_n, "max_points": max_points},
            }

        cur.execute(
            """
            SELECT *
            FROM quant.market_event_members
            WHERE event_slug = %s
            ORDER BY outcome_order ASC, outcome_label ASC, market_id ASC
            LIMIT %s
            """,
            (event_slug, int(max_outcomes)),
        )
        members = [dict(row) for row in cur.fetchall()]
        latest_rows: list[dict[str, Any]] = []
        for index, member in enumerate(members):
            yes_latest = _fetch_token_price_points(
                cur,
                token_id=member.get("token_yes_id"),
                source=source,
                from_ts=from_ts,
                to_ts=to_ts,
                from_block=from_block,
                to_block=to_block,
                limit=1,
            )
            no_latest = _fetch_token_price_points(
                cur,
                token_id=member.get("token_no_id"),
                source=source,
                from_ts=from_ts,
                to_ts=to_ts,
                from_block=from_block,
                to_block=to_block,
                limit=1,
            )
            latest_rows.append({
                "index": index,
                "member": member,
                "yes_latest": yes_latest,
                "no_latest": no_latest,
                "score": _latest_yes_score(yes_latest[-1] if yes_latest else None, no_latest[-1] if no_latest else None),
                "latest_x": max(_point_x(yes_latest[-1]) if yes_latest else 0, _point_x(no_latest[-1]) if no_latest else 0),
            })

        ranked = sorted(latest_rows, key=lambda row: (row["score"], row["latest_x"]), reverse=True)
        keep_indexes = {row["index"] for row in ranked[:top_n]}

        outcomes: list[dict[str, Any]] = []
        use_sampled_points = normalized_range in {"all", "full"}
        for item in latest_rows:
            member = item["member"]
            label = _clean_label(member.get("outcome_label")) or _clean_label(member.get("question"))
            no_label = _no_label_from_member(member.get("question"), label)
            if item["index"] in keep_indexes:
                if use_sampled_points:
                    yes_points = _fetch_token_price_points_sampled(
                        cur,
                        token_id=member.get("token_yes_id"),
                        source=source,
                        from_ts=from_ts,
                        to_ts=to_ts,
                        from_block=from_block,
                        to_block=to_block,
                        max_points=max_points,
                    )
                    no_points = _fetch_token_price_points_sampled(
                        cur,
                        token_id=member.get("token_no_id"),
                        source=source,
                        from_ts=from_ts,
                        to_ts=to_ts,
                        from_block=from_block,
                        to_block=to_block,
                        max_points=max_points,
                    )
                else:
                    yes_points = _fetch_token_price_points(
                        cur,
                        token_id=member.get("token_yes_id"),
                        source=source,
                        from_ts=from_ts,
                        to_ts=to_ts,
                        from_block=from_block,
                        to_block=to_block,
                        limit=source_limit,
                    )
                    no_points = _fetch_token_price_points(
                        cur,
                        token_id=member.get("token_no_id"),
                        source=source,
                        from_ts=from_ts,
                        to_ts=to_ts,
                        from_block=from_block,
                        to_block=to_block,
                        limit=source_limit,
                    )
                    yes_points = _minmax_downsample(yes_points, max_points)
                    no_points = _minmax_downsample(no_points, max_points)
            else:
                yes_points = item["yes_latest"]
                no_points = item["no_latest"]
            outcomes.append(_event_outcome_payload(
                member=member,
                event=event,
                label=label,
                no_label=no_label,
                yes_points=yes_points,
                no_points=no_points,
            ))

    return {
        "event": {
            **event,
            "source": source,
            "scope": "event_tile",
            "x_axis": "block_number" if source == "orderfilled_block_close" else "timestamp",
        },
        "members": members,
        "outcomes": outcomes,
        "count": len(outcomes),
        "tile": {
            "range": tile_range,
            "resolution": resolution,
            "top_n": top_n,
            "max_points": max_points,
            "source_limit": source_limit,
        },
    }


def get_market_price_series(
    conn: Any,
    *,
    market_slug: str,
    price_source: str,
    scope: str = "auto",
    token_id: str | None = None,
    token_side: str | None = None,
    from_ts: int | None = None,
    to_ts: int | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    limit: int = 2500,
    max_outcomes: int = 24,
    max_points: int = 900,
) -> dict[str, Any]:
    """Return semantic market/event series grouped by outcome token.

    Raw price tables stay token-granular. This read model packages those token
    rows into user-facing outcomes, so sports matchups and selection markets can
    render multiple lines without forcing the backtest engine to guess meaning.
    """

    source = "orderfilled_block_close" if price_source == "orderfilled_block_close" else "frontend"
    requested_scope = scope if scope in {"auto", "market", "event"} else "auto"
    tokens, effective_scope = _fetch_market_tokens(
        conn,
        market_slug=market_slug,
        scope=requested_scope,
        max_outcomes=max_outcomes,
    )
    selected_token_id = str(token_id or "").strip()
    if selected_token_id:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    market_id, market_slug, market_title, condition_id, end_date,
                    token_id, token_side, outcome_index
                FROM quant.market_token_metadata
                WHERE token_id = %s
                LIMIT 1
                """,
                (selected_token_id,),
            )
            selected_row = cur.fetchone()
        if selected_row:
            tokens = [dict(selected_row)]
            effective_scope = "market"
        else:
            tokens = [token for token in tokens if str(token.get("token_id") or "") == selected_token_id]
    if token_side and effective_scope == "market":
        wanted = token_side.upper()
        tokens = [token for token in tokens if str(token.get("token_side") or "").upper() == wanted]

    outcomes: list[dict[str, Any]] = []
    market_ids = sorted({int(token["market_id"]) for token in tokens if token.get("market_id") is not None})
    tokens_by_market_side: dict[tuple[int, str], dict[str, Any]] = {}
    with conn.cursor() as cur:
        if market_ids:
            cur.execute(
                """
                SELECT
                    market_id, market_slug, market_title, condition_id, end_date,
                    token_id, token_side, outcome_index
                FROM quant.market_token_metadata
                WHERE market_id = ANY(%s::bigint[])
                """,
                (market_ids,),
            )
            for row in cur.fetchall():
                item = dict(row)
                tokens_by_market_side[(int(item["market_id"]), str(item.get("token_side") or "").upper())] = item

        points_cache: dict[str, list[dict[str, Any]]] = {}

        def fetch_points(token_id: str | None) -> list[dict[str, Any]]:
            if not token_id:
                return []
            cache_key = str(token_id)
            if cache_key in points_cache:
                return points_cache[cache_key]
            if max_points and int(limit or 0) > int(max_points):
                rows = _fetch_token_price_points_sampled(
                    cur,
                    token_id=cache_key,
                    source=source,
                    from_ts=from_ts,
                    to_ts=to_ts,
                    from_block=from_block,
                    to_block=to_block,
                    max_points=max_points,
                )
                points_cache[cache_key] = rows
                return rows
            params: list[Any] = [cache_key]
            if source == "frontend":
                filters = ["token_id = %s"]
                if from_ts is not None:
                    filters.append("timestamp >= %s")
                    params.append(int(from_ts))
                if to_ts is not None:
                    filters.append("timestamp <= %s")
                    params.append(int(to_ts))
                params.append(int(limit))
                cur.execute(
                    f"""
                    WITH limited AS (
                        SELECT
                            token_id, market_id, market_slug, token_side,
                            ts_minute, timestamp, price, 0::numeric AS volume
                        FROM quant.market_token_frontend_price_1m
                        WHERE {" AND ".join(filters)}
                        ORDER BY ts_minute DESC
                        LIMIT %s
                    )
                    SELECT *
                    FROM limited
                    ORDER BY ts_minute ASC
                    """,
                    params,
                )
                rows = [
                    {
                        "x": row["timestamp"],
                        "timestamp": row["timestamp"],
                        "price": row["price"],
                        "volume": row["volume"],
                    }
                    for row in cur.fetchall()
                ]
            else:
                filters = ["token_id = %s"]
                if from_block is not None:
                    filters.append("block_number >= %s")
                    params.append(int(from_block))
                if to_block is not None:
                    filters.append("block_number <= %s")
                    params.append(int(to_block))
                params.append(int(limit))
                cur.execute(
                    f"""
                    WITH limited AS (
                        SELECT
                            token_id, market_id, market_slug, token_side, block_number,
                            close_price, yes_probability_close, vwap_price, yes_probability_vwap,
                            volume, trade_count, block_timestamp
                        FROM quant.market_token_block_close
                        WHERE {" AND ".join(filters)}
                        ORDER BY block_number DESC
                        LIMIT %s
                    )
                    SELECT *
                    FROM limited
                    ORDER BY block_number ASC
                    """,
                    params,
                )
                rows = [
                    {
                        "x": row["block_number"],
                        "block_number": row["block_number"],
                        "timestamp": row.get("block_timestamp"),
                        "price": row["close_price"],
                        "yes_probability_close": row["yes_probability_close"],
                        "vwap_price": row["vwap_price"],
                        "yes_probability_vwap": row["yes_probability_vwap"],
                        "volume": row["volume"],
                        "trade_count": row["trade_count"],
                    }
                    for row in cur.fetchall()
                ]
            points_cache[cache_key] = rows
            return rows

        for token in tokens:
            points = fetch_points(str(token.get("token_id") or ""))
            complement_token = tokens_by_market_side.get(
                (int(token["market_id"]), _complement_side(str(token.get("token_side") or "")))
            )
            complement_points = fetch_points(str(complement_token.get("token_id") or "")) if complement_token else []
            label = infer_outcome_label(
                token.get("market_title"),
                token.get("token_side"),
                event_scope=effective_scope == "event",
            )
            outcomes.append(
                {
                    "market_id": token.get("market_id"),
                    "market_slug": token.get("market_slug"),
                    "market_title": token.get("market_title"),
                    "condition_id": token.get("condition_id"),
                    "end_date": token.get("end_date"),
                    "token_id": token.get("token_id"),
                    "token_side": token.get("token_side"),
                    "outcome_index": token.get("outcome_index"),
                    "outcome_label": label,
                    "buy_yes_token_id": token.get("token_id"),
                    "buy_yes_token_side": token.get("token_side"),
                    "buy_yes_label": f"{label} Yes",
                    "buy_yes_price": points[-1]["price"] if points else None,
                    "buy_no_token_id": complement_token.get("token_id") if complement_token else None,
                    "buy_no_token_side": complement_token.get("token_side") if complement_token else None,
                    "buy_no_label": f"{label} No",
                    "buy_no_price": complement_points[-1]["price"] if complement_points else None,
                    "rows": len(points),
                    "first_x": points[0]["x"] if points else None,
                    "last_x": points[-1]["x"] if points else None,
                    "latest_price": points[-1]["price"] if points else None,
                    "points": points,
                    "complement_rows": len(complement_points),
                    "complement_first_x": complement_points[0]["x"] if complement_points else None,
                    "complement_last_x": complement_points[-1]["x"] if complement_points else None,
                    "complement_latest_price": complement_points[-1]["price"] if complement_points else None,
                    "complement_points": complement_points,
                }
            )

    return {
        "market": _market_payload(
            tokens,
            market_slug=market_slug,
            source=source,
            scope=effective_scope,
            x_axis="block_number" if source == "orderfilled_block_close" else "timestamp",
        ),
        "outcomes": outcomes,
        "count": len(outcomes),
    }


def get_frontend_prices(
    conn: Any,
    *,
    market_slug: str | None = None,
    token_side: str | None = None,
    token_id: str | None = None,
    from_ts: int | None = None,
    to_ts: int | None = None,
    limit: int = 10000,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if market_slug:
        filters.append("market_slug = %s")
        params.append(market_slug)
    if token_side:
        filters.append("token_side = %s")
        params.append(token_side.upper())
    if token_id:
        filters.append("token_id = %s")
        params.append(token_id)
    if from_ts is not None:
        filters.append("timestamp >= %s")
        params.append(int(from_ts))
    if to_ts is not None:
        filters.append("timestamp <= %s")
        params.append(int(to_ts))
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT token_id, market_id, market_slug, token_side, ts_minute, timestamp, price
            FROM quant.market_token_frontend_price_1m
            {where_sql}
            ORDER BY ts_minute ASC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_block_close_prices(
    conn: Any,
    *,
    market_slug: str | None = None,
    token_side: str | None = None,
    token_id: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    limit: int = 10000,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if market_slug:
        filters.append("market_slug = %s")
        params.append(market_slug)
    if token_side:
        filters.append("token_side = %s")
        params.append(token_side.upper())
    if token_id:
        filters.append("token_id = %s")
        params.append(token_id)
    if from_block is not None:
        filters.append("block_number >= %s")
        params.append(int(from_block))
    if to_block is not None:
        filters.append("block_number <= %s")
        params.append(int(to_block))
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                token_id, market_id, market_slug, token_side, block_number,
                close_price, yes_probability_close, vwap_price, yes_probability_vwap,
                close_raw_price, close_price_source, close_tx_hash, close_log_index,
                close_maker_amount, close_taker_amount, trade_count, raw_trade_count,
                internal_filtered_count, invalid_size_count, invalid_price_count,
                amount_ratio_count, raw_price_fallback_count, extreme_trade_count,
                anomaly_flags, volume, block_timestamp
            FROM quant.market_token_block_close
            {where_sql}
            ORDER BY block_number ASC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_market_token_execution_summaries(
    conn: Any,
    *,
    market_id: int | None = None,
    market_slug: str | None = None,
    token_side: str | None = None,
    liquidity_bucket: str | None = None,
    side_bucket: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if market_id is not None:
        filters.append("market_id = %s")
        params.append(int(market_id))
    if market_slug:
        filters.append("market_slug = %s")
        params.append(str(market_slug))
    if token_side:
        filters.append("token_side = %s")
        params.append(str(token_side).upper())
    if liquidity_bucket:
        filters.append("liquidity_bucket = %s")
        params.append(str(liquidity_bucket))
    if side_bucket:
        filters.append("side_bucket = %s")
        params.append(str(side_bucket))
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                token_id, market_id, market_slug, token_side,
                first_block, last_block, block_row_count,
                trade_count, raw_trade_count, volume,
                maker_amount, taker_amount, maker_share, taker_share,
                internal_filtered_count, invalid_size_count, invalid_price_count,
                amount_ratio_count, raw_price_fallback_count, extreme_trade_count,
                anomaly_count, anomaly_flags, side_bucket, liquidity_bucket,
                refreshed_at
            FROM quant.market_token_execution_summary
            {where_sql}
            ORDER BY volume DESC, trade_count DESC, refreshed_at DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_market_execution_summaries(
    conn: Any,
    *,
    market_slug: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if market_slug:
        filters.append("market_slug = %s")
        params.append(str(market_slug))
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                market_id, market_slug, token_count, first_block, last_block,
                block_row_count, trade_count, raw_trade_count, volume,
                maker_amount, taker_amount, maker_share, taker_share,
                anomaly_count, side_bucket_counts, liquidity_bucket_counts,
                dominant_side_bucket, dominant_liquidity_bucket, refreshed_at
            FROM quant.market_execution_summary
            {where_sql}
            ORDER BY volume DESC, trade_count DESC, refreshed_at DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_event_execution_summaries(
    conn: Any,
    *,
    event_slug: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if event_slug:
        filters.append("event_slug = %s")
        params.append(str(event_slug))
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                event_slug, event_title, market_count, token_count,
                first_block, last_block, block_row_count,
                trade_count, raw_trade_count, volume,
                maker_amount, taker_amount, maker_share, taker_share,
                anomaly_count, side_bucket_counts, liquidity_bucket_counts,
                dominant_side_bucket, dominant_liquidity_bucket, refreshed_at
            FROM quant.event_execution_summary
            {where_sql}
            ORDER BY volume DESC, trade_count DESC, refreshed_at DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_api_trades_prices(
    conn: Any,
    *,
    market_slug: str | None = None,
    token_side: str | None = None,
    token_id: str | None = None,
    from_ts: int | None = None,
    to_ts: int | None = None,
    limit: int = 10000,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if market_slug:
        filters.append("market_slug = %s")
        params.append(market_slug)
    if token_side:
        filters.append("token_side = %s")
        params.append(token_side.upper())
    if token_id:
        filters.append("token_id = %s")
        params.append(token_id)
    if from_ts is not None:
        filters.append("timestamp >= %s")
        params.append(int(from_ts))
    if to_ts is not None:
        filters.append("timestamp <= %s")
        params.append(int(to_ts))
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                token_id, market_id, market_slug, token_side,
                trade_key, transaction_hash, timestamp, trade_time,
                price, yes_probability, size, notional,
                side, proxy_wallet, condition_id, market_filter_condition_id,
                outcome, outcome_index, source_endpoint, taker_only
            FROM quant.market_token_api_trades
            {where_sql}
            ORDER BY trade_time ASC, trade_key ASC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_price_build_status(conn: Any, *, source: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    params: list[Any] = []
    where_sql = ""
    if source:
        where_sql = "WHERE source = %s"
        params.append(source)
    params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.market_price_build_runs
            {where_sql}
            ORDER BY started_at DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def _compact_backtest_run_from_snapshot(row: Mapping[str, Any]) -> dict[str, Any] | None:
    snapshot = row.get("run_snapshot")
    if not isinstance(snapshot, Mapping) or not snapshot:
        return None
    snapshot = dict(snapshot)
    parameters = snapshot.pop("parameters", {})
    execution_context = snapshot.get("execution_context")
    meta = {
        "actual_data_quality_summary": {"details_omitted": True},
        "parameter_snapshot": {**snapshot, "parameters": dict(parameters) if isinstance(parameters, Mapping) else {}},
    }
    if isinstance(execution_context, Mapping):
        meta["execution_context"] = dict(execution_context)
    for key in ("strategy", "strategy_name", "strategy_version"):
        if snapshot.get(key) is not None:
            meta[key] = snapshot[key]
    item = {
        key: value
        for key, value in snapshot.items()
        if key not in {"strategy", "strategy_name", "strategy_version", "execution_context"}
    }
    if isinstance(parameters, Mapping):
        item.update(parameters)
    item.update(
        {
            "run_id": row.get("run_id"),
            "status": row.get("status"),
            "rows_processed": row.get("rows_processed") or 0,
            "error": row.get("error"),
            "created_at": row.get("created_at"),
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
            "meta": meta,
        }
    )
    return item


def get_backtest_run(conn: Any, *, run_id: int, compact: bool = False) -> dict[str, Any] | None:
    if compact:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, status, rows_processed, error,
                       created_at, started_at, finished_at, run_snapshot
                FROM quant.quant_backtest_run_progress
                WHERE run_id = %s
                """,
                (int(run_id),),
            )
            compact_row = cur.fetchone()
        if compact_row:
            item = _compact_backtest_run_from_snapshot(compact_row)
            if item is not None:
                return item

    run_columns = """
        r.run_id, r.status, r.market_slug, r.token_side, r.price_source,
        r.backtest_engine, r.from_ts, r.to_ts, r.from_block, r.to_block,
        r.rows_processed, r.error, r.created_at, r.started_at, r.finished_at
    """
    meta_column = "r.meta"
    if compact:
        # Do not touch r.meta here: a run can carry tens of MB of replay evidence in
        # its toasted JSONB value. The workbench loads quality/audit details from the
        # dedicated artifact endpoints instead.
        meta_column = """jsonb_build_object(
            'actual_data_quality_summary', jsonb_build_object('details_omitted', TRUE)
        )"""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {run_columns}, {meta_column} AS meta,
                   p.entry_threshold, p.exit_threshold, p.stop_loss, p.take_profit,
                   p.max_holding_bars, p.initial_capital, p.position_size,
                   p.fee_bps, p.maker_fee_bps, p.taker_fee_bps, p.maker_rebate_bps,
                   p.slippage_bps, p.liquidity_cap_pct,
                   p.max_position_notional, p.min_fill_pct,
                   p.execution_price_mode, p.execution_profile, p.pml2_audit_mode, p.order_role,
                   p.latency_blocks, p.adverse_slippage_cents, p.fill_probability_haircut_pct,
                   p.latency_seconds, p.max_book_staleness_seconds,
                   p.allow_partial_fill, p.min_fill_size, p.reject_on_stale_book,
                   p.final_valuation_mode, p.max_entry_price, p.min_exit_price,
                   p.buy_limit_price, p.sell_limit_price, p.settlement_value
            FROM quant.quant_backtest_runs r
            LEFT JOIN quant.quant_backtest_parameters p ON p.run_id = r.run_id
            WHERE r.run_id = %s
            """,
            (int(run_id),),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_backtest_run_progress(conn: Any, *, run_id: int) -> dict[str, Any] | None:
    """Return progress without reading or rewriting the large run audit JSON."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, status, phase, progress, current_x, x_axis,
                   rows_processed, total_rows, eta_seconds, error,
                   created_at, started_at, finished_at, updated_at
            FROM quant.quant_backtest_run_progress
            WHERE run_id = %s
            """,
            (int(run_id),),
        )
        row = cur.fetchone()
    if not row:
        return None
    item = dict(row)
    status = str(item.get("status") or "queued")
    item["status"] = status
    item["phase"] = item.get("phase") or ("waiting for backtest worker" if status == "queued" else status)
    item["progress"] = item.get("progress") if item.get("progress") is not None else (100 if status == "succeeded" else 0)
    item["rows_processed"] = item.get("rows_processed") or 0
    return item


def get_backtest_runs(conn: Any, *, market_slug: str | None = None, limit: int = 25) -> list[dict[str, Any]]:
    where_sql = ""
    params: list[Any] = []
    if market_slug:
        where_sql = "WHERE r.market_slug = %s"
        params.append(market_slug)
    params.append(int(limit))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT r.run_id, r.status, r.market_slug, r.token_side, r.price_source,
                   r.backtest_engine, r.from_ts, r.to_ts, r.from_block, r.to_block,
                   r.rows_processed, r.error, r.created_at, r.started_at, r.finished_at,
                   p.entry_threshold, p.exit_threshold, p.stop_loss, p.take_profit,
                   p.max_holding_bars, p.initial_capital, p.position_size,
                   p.fee_bps, p.maker_fee_bps, p.taker_fee_bps, p.maker_rebate_bps,
                   p.slippage_bps, p.liquidity_cap_pct,
                   p.max_position_notional, p.min_fill_pct,
                   p.execution_price_mode, p.execution_profile, p.pml2_audit_mode, p.order_role,
                   p.latency_blocks, p.adverse_slippage_cents, p.fill_probability_haircut_pct,
                   p.latency_seconds, p.max_book_staleness_seconds,
                   p.allow_partial_fill, p.min_fill_size, p.reject_on_stale_book,
                   p.final_valuation_mode, p.max_entry_price, p.min_exit_price,
                   p.buy_limit_price, p.sell_limit_price, p.settlement_value
            FROM quant.quant_backtest_runs r
            LEFT JOIN quant.quant_backtest_parameters p ON p.run_id = r.run_id
            {where_sql}
            ORDER BY r.created_at DESC, r.run_id DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_backtest_metrics(conn: Any, *, run_id: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_metrics
            WHERE run_id = %s
            ORDER BY sort_order ASC, metric_key ASC
            """,
            (int(run_id),),
        )
        return [dict(row) for row in cur.fetchall()]


def get_backtest_equity(conn: Any, *, run_id: int, limit: int = 25000) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_equity
            WHERE run_id = %s
            ORDER BY point_index ASC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def get_backtest_trades(conn: Any, *, run_id: int, limit: int = 10000) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_trades
            WHERE run_id = %s
            ORDER BY entry_x ASC, trade_id ASC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def get_backtest_orders(conn: Any, *, run_id: int, limit: int = 10000) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_orders
            WHERE run_id = %s
            ORDER BY signal_index ASC, order_id ASC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        return [enrich_order_evidence_fields(dict(row)) for row in cur.fetchall()]


def get_backtest_ledger(conn: Any, *, run_id: int, limit: int = 10000) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_ledger
            WHERE run_id = %s
            ORDER BY x_value ASC, ledger_id ASC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def get_backtest_events(conn: Any, *, run_id: int, limit: int = 10000) -> list[dict[str, Any]]:
    """Return persisted strategy events without inferring signals from prices."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_events
            WHERE run_id = %s
            ORDER BY event_index ASC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def _get_backtest_execution_orders(conn: Any, *, run_id: int, limit: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_orders
            WHERE run_id = %s
              AND status IN ('FILLED', 'PARTIAL_FILLED', 'PARTIALLY_FILLED', 'PARTIAL')
            ORDER BY signal_index ASC, order_id ASC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        return [enrich_order_evidence_fields(dict(row)) for row in cur.fetchall()]


def _get_backtest_execution_events(
    conn: Any,
    *,
    run_id: int,
    trade_ids: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    if not trade_ids:
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_events
            WHERE run_id = %s
              AND x_value > 0
              AND trade_id = ANY(%s::text[])
            ORDER BY event_index ASC
            LIMIT %s
            """,
            (int(run_id), trade_ids, int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def _get_backtest_replay_source_counts(conn: Any, *, run_id: int) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                (SELECT count(*) FROM quant.quant_backtest_orders WHERE run_id = %s) AS orders,
                (SELECT count(*) FROM quant.quant_backtest_ledger WHERE run_id = %s) AS ledger,
                (SELECT count(*) FROM quant.quant_backtest_trades WHERE run_id = %s) AS trades,
                (SELECT count(*) FROM quant.quant_backtest_events WHERE run_id = %s) AS events
            """,
            (int(run_id), int(run_id), int(run_id), int(run_id)),
        )
        row = dict(cur.fetchone() or {})
    return {key: int(value or 0) for key, value in row.items()}


def get_backtest_replay(
    conn: Any,
    *,
    run_id: int,
    limit: int = 25000,
    view: str = "full",
) -> dict[str, Any] | None:
    """Return one canonical, evidence-labelled replay stream for the workbench."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, status, market_slug, token_side, price_source, backtest_engine,
                   from_ts, to_ts, from_block, to_block, rows_processed,
                   meta ->> 'outcome_label' AS outcome_label
            FROM quant.quant_backtest_runs
            WHERE run_id = %s
            """,
            (int(run_id),),
        )
        row = cur.fetchone()
    if not row:
        return None
    run = dict(row)
    ledger = get_backtest_ledger(conn, run_id=run_id, limit=limit)
    trades = get_backtest_trades(conn, run_id=run_id, limit=limit)
    if view == "executions":
        orders = _get_backtest_execution_orders(conn, run_id=run_id, limit=limit)
        trade_ids = sorted({str(item.get("trade_id")) for item in ledger if item.get("trade_id")})
        events = _get_backtest_execution_events(conn, run_id=run_id, trade_ids=trade_ids, limit=limit)
    else:
        orders = get_backtest_orders(conn, run_id=run_id, limit=limit)
        events = get_backtest_events(conn, run_id=run_id, limit=limit)
    replay = build_backtest_replay(
        run,
        orders=orders,
        ledger=ledger,
        trades=trades,
        events=events,
    )
    replay["summary"]["view"] = view
    replay["summary"]["source_counts"] = _get_backtest_replay_source_counts(conn, run_id=run_id)
    return replay


def get_backtest_position_timeline(conn: Any, *, run_id: int, limit: int = 10000) -> list[dict[str, Any]]:
    """Project authoritative position snapshots from the persisted cashflow ledger."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, ledger_id, order_id, trade_id, event_type, x_axis,
                   x_value, market_slug, token_side, position_after, cash_after,
                   price, realized_pnl, source, meta, created_at
            FROM quant.quant_backtest_ledger
            WHERE run_id = %s
            ORDER BY x_value ASC, ledger_id ASC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def get_backtest_calibration_orders(conn: Any, *, run_id: int, limit: int = 1000) -> list[dict[str, Any]]:
    if not _table_exists(conn, "quant.quant_backtest_calibration_orders"):
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_calibration_orders
            WHERE run_id = %s
            ORDER BY observed_at DESC NULLS LAST, calibration_id DESC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def get_backtest_calibration_report(conn: Any, *, run_id: int, limit: int = 5000) -> dict[str, Any]:
    if not _table_exists(conn, "quant.quant_backtest_calibration_orders"):
        report = empty_calibration_report(reason="calibration table missing")
        report["run_id"] = int(run_id)
        return report
    rows = get_backtest_calibration_orders(conn, run_id=run_id, limit=limit)
    if not rows:
        report = empty_calibration_report()
    else:
        report = build_calibration_report(rows)
    report["run_id"] = int(run_id)
    return report


def get_backtest_cost_calibration_rows(conn: Any, *, run_id: int, limit: int = 1000) -> list[dict[str, Any]]:
    if not _table_exists(conn, "quant.quant_backtest_cost_calibration"):
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_cost_calibration
            WHERE run_id = %s
            ORDER BY observed_at DESC NULLS LAST, cost_calibration_id DESC
            LIMIT %s
            """,
            (int(run_id), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def get_backtest_cost_calibration_report(conn: Any, *, run_id: int, limit: int = 5000) -> dict[str, Any]:
    if not _table_exists(conn, "quant.quant_backtest_cost_calibration"):
        report = empty_cost_calibration_report(reason="cost calibration table missing")
        report["run_id"] = int(run_id)
        return report
    rows = get_backtest_cost_calibration_rows(conn, run_id=run_id, limit=limit)
    if not rows:
        report = empty_cost_calibration_report()
    else:
        report = build_cost_calibration_report(rows)
    report["run_id"] = int(run_id)
    return report


def get_execution_profile_overrides(conn: Any, *, status: str | None = "approved", limit: int = 50) -> list[dict[str, Any]]:
    if not _table_exists(conn, "quant.execution_profile_overrides"):
        return []
    filters: list[str] = []
    params: list[Any] = []
    if status:
        filters.append("status = %s")
        params.append(status)
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(max(1, int(limit)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.execution_profile_overrides
            {where_sql}
            ORDER BY updated_at DESC, override_id DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def get_production_parameter_staging(conn: Any, *, status: str | None = "approved", limit: int = 50) -> list[dict[str, Any]]:
    if not _table_exists(conn, "quant.production_parameter_staging"):
        return []
    filters: list[str] = []
    params: list[Any] = []
    if status:
        filters.append("status = %s")
        params.append(status)
    where_sql = "WHERE " + " AND ".join(filters) if filters else ""
    params.append(max(1, int(limit)))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.production_parameter_staging
            {where_sql}
            ORDER BY updated_at DESC, staging_id DESC
            LIMIT %s
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def _table_exists(conn: Any, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (table_name,))
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False
