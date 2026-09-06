"""Validate physical Gold L2 rows and raw-message atomicity fail-closed."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

EVENT_REPAIR_MODE = "EVENT_REPAIR"
CONTINUITY_ONLY_MODE = "CONTINUITY_ONLY"
_CONTINUITY_REQUIRED_COLUMNS = frozenset(
    {
        "event_type",
        "market",
        "asset_id",
        "timestamp",
        "timestamp_received",
        "timestamp_normalized",
        "source",
        "bids",
        "asks",
        "price",
        "size",
        "side",
        "best_bid",
        "best_ask",
        "raw_connection_id",
        "raw_connection_generation",
        "raw_frame_seq",
        "message_index",
        "change_index",
        "group_id",
        "is_last_in_group",
        "raw_frame_complete",
        "occurrence_rank",
        "canonical_source",
        "source_mask",
        "repair_status",
        "evidence_level",
        "request_id",
        "candidate_sha256",
        "idempotency_key",
        "state_replay_receipt_sha256",
    }
)


def validate_gold_hour(
    *,
    path: Path,
    hour_start: datetime,
    memory_limit: str = "8GB",
    mode: str = EVENT_REPAIR_MODE,
) -> dict[str, Any]:
    hour = _floor_hour(hour_start)
    stop = hour + timedelta(hours=1)
    if mode not in {EVENT_REPAIR_MODE, CONTINUITY_ONLY_MODE}:
        raise ValueError(f"unsupported Gold validation mode: {mode}")
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            f"SET memory_limit='{_sql(memory_limit)}'"
        )
        connection.execute("SET threads=2")
        connection.execute(
            f"""
            CREATE TEMP VIEW gold AS
            SELECT *
            FROM read_parquet('{_sql(str(path))}')
            """
        )
        if mode == CONTINUITY_ONLY_MODE:
            return _validate_continuity_only_overlay(connection, hour=hour)
        row = connection.execute(
            """
            SELECT
                count(*)::BIGINT AS rows,
                count(*) FILTER (
                    WHERE event_type NOT IN (
                        'book',
                        'price_change',
                        'last_trade_price',
                        'tick_size_change'
                    )
                )::BIGINT AS invalid_event_type,
                count(*) FILTER (
                    WHERE asset_id IS NULL OR asset_id = ''
                       OR market IS NULL OR market = ''
                )::BIGINT AS missing_identity,
                count(*) FILTER (
                    WHERE "timestamp" IS NULL
                       OR "timestamp" < ?::TIMESTAMPTZ
                       OR "timestamp" >= ?::TIMESTAMPTZ
                )::BIGINT AS timestamp_outside_hour,
                count(*) FILTER (
                    WHERE timestamp_received IS NULL
                       OR timestamp_normalized IS NULL
                )::BIGINT AS missing_pipeline_timestamp,
                count(*) FILTER (
                    WHERE event_type = 'price_change'
                      AND (
                        try_cast(price AS DECIMAL(38, 18)) IS NULL
                        OR try_cast(price AS DECIMAL(38, 18)) <= 0
                        OR try_cast(price AS DECIMAL(38, 18)) >= 1
                        OR try_cast(size AS DECIMAL(38, 18)) IS NULL
                        OR try_cast(size AS DECIMAL(38, 18)) < 0
                        OR upper(coalesce(side, '')) NOT IN (
                            'BUY', 'SELL'
                        )
                      )
                )::BIGINT AS invalid_price_change,
                count(*) FILTER (
                    WHERE event_type = 'last_trade_price'
                      AND (
                        try_cast(price AS DECIMAL(38, 18)) <= 0
                        OR try_cast(price AS DECIMAL(38, 18)) > 1
                      )
                )::BIGINT AS invalid_last_trade_price,
                count(*) FILTER (
                    WHERE event_type = 'tick_size_change'
                      AND (
                        try_cast(new_tick_size AS DECIMAL(38, 18))
                            <= 0
                        OR try_cast(new_tick_size AS DECIMAL(38, 18))
                            > 1
                      )
                )::BIGINT AS invalid_tick_size,
                count(*) FILTER (
                    WHERE (
                        best_bid IS NOT NULL
                        AND (
                            try_cast(best_bid AS DECIMAL(38, 18)) IS NULL
                            OR try_cast(best_bid AS DECIMAL(38, 18)) < 0
                            OR try_cast(best_bid AS DECIMAL(38, 18)) >= 1
                        )
                    )
                    OR (
                        best_ask IS NOT NULL
                        AND (
                            try_cast(best_ask AS DECIMAL(38, 18)) IS NULL
                            OR try_cast(best_ask AS DECIMAL(38, 18)) <= 0
                            OR try_cast(best_ask AS DECIMAL(38, 18))
                                > 1
                        )
                    )
                    OR (
                        best_bid IS NOT NULL
                        AND best_ask IS NOT NULL
                        AND try_cast(
                            best_bid AS DECIMAL(38, 18)
                        ) >= try_cast(
                            best_ask AS DECIMAL(38, 18)
                        )
                    )
                )::BIGINT AS invalid_or_crossed_bbo,
                count(*) FILTER (
                    WHERE canonical_source NOT IN (
                        'A', 'B', 'PMXT', 'C', 'D'
                    )
                       OR source_mask IS NULL
                       OR source_mask <= 0
                       OR source_mask > 31
                       OR evidence_level IS NULL
                       OR evidence_level <= 0
                       OR repair_status IS NULL
                )::BIGINT AS invalid_provenance,
                count(*) FILTER (
                    WHERE (
                        canonical_source = 'A'
                        AND (source_mask & 1) = 0
                    )
                    OR (
                        canonical_source = 'B'
                        AND (source_mask & 2) = 0
                    )
                    OR (
                        canonical_source = 'PMXT'
                        AND (source_mask & 4) = 0
                    )
                    OR (
                        canonical_source = 'C'
                        AND (source_mask & 8) = 0
                    )
                    OR (
                        canonical_source = 'D'
                        AND (source_mask & 16) = 0
                    )
                )::BIGINT AS source_mask_mismatch,
                count(*) FILTER (
                    WHERE canonical_source IN ('C', 'D')
                      AND (
                        source_mask != 24
                        OR evidence_level != 2
                        OR repair_status != 'EVENT_REPAIRED_QUORUM'
                      )
                )::BIGINT AS invalid_observer_quorum_provenance,
                count(*) FILTER (
                    WHERE canonical_source = 'PMXT'
                )::BIGINT AS pmxt_synthetic_rows,
                count(*) FILTER (
                    WHERE group_id IS NULL
                       OR raw_frame_seq IS NULL
                       OR message_index IS NULL
                       OR change_index IS NULL
                )::BIGINT AS missing_raw_group_provenance
            FROM gold
            """,
            [hour, stop],
        ).fetchone()
        assert row is not None
        names = [
            item[0]
            for item in connection.execute(
                """
                DESCRIBE SELECT
                    count(*) AS rows,
                    0 AS invalid_event_type,
                    0 AS missing_identity,
                    0 AS timestamp_outside_hour,
                    0 AS missing_pipeline_timestamp,
                    0 AS invalid_price_change,
                    0 AS invalid_last_trade_price,
                    0 AS invalid_tick_size,
                    0 AS invalid_or_crossed_bbo,
                    0 AS invalid_provenance,
                    0 AS source_mask_mismatch,
                    0 AS invalid_observer_quorum_provenance,
                    0 AS pmxt_synthetic_rows,
                    0 AS missing_raw_group_provenance
                FROM gold
                """
            ).fetchall()
        ]
        counters = {
            name: int(value or 0) for name, value in zip(names, row)
        }
        counters.update(_book_level_checks(connection))
        counters.update(_tick_grid_checks(connection))
        group = connection.execute(
            """
            WITH grouped AS (
                SELECT
                    canonical_source,
                    group_id,
                    count(*)::BIGINT AS row_count,
                    count(*) FILTER (
                        WHERE is_last_in_group
                    )::BIGINT AS final_count,
                    count(*) FILTER (
                        WHERE raw_frame_complete
                    )::BIGINT AS complete_count,
                    max(
                        struct_pack(
                            message_index := coalesce(
                                message_index, -1
                            ),
                            change_index := coalesce(
                                change_index, -1
                            )
                        )
                    ) FILTER (
                        WHERE is_last_in_group
                    ) AS final_position,
                    max(
                        struct_pack(
                            message_index := coalesce(
                                message_index, -1
                            ),
                            change_index := coalesce(
                                change_index, -1
                            )
                        )
                    ) AS last_position
                FROM gold
                WHERE group_id IS NOT NULL
                GROUP BY canonical_source, group_id
            )
            SELECT
                count(*)::BIGINT,
                count(*) FILTER (
                    WHERE final_count != 1
                       OR complete_count != 1
                       OR final_position IS DISTINCT FROM last_position
                )::BIGINT
            FROM grouped
            """
        ).fetchone()
        counters["raw_groups"] = int(group[0] or 0)
        counters["incomplete_or_nonatomic_groups"] = int(
            group[1] or 0
        )
        counters["pmxt_synthetic_groups"] = int(
            connection.execute(
                """
                SELECT count(DISTINCT group_id)::BIGINT
                FROM gold
                WHERE canonical_source = 'PMXT'
                """
            ).fetchone()[0]
            or 0
        )
    finally:
        connection.close()
    violations = sum(
        value
        for name, value in counters.items()
        if name
        not in {
            "rows",
            "raw_groups",
            "pmxt_synthetic_rows",
            "pmxt_synthetic_groups",
        }
    )
    return {
        "schema_version": "polymarket-l2-gold-invariants-v1",
        "hour_start": hour.isoformat(),
        "mode": EVENT_REPAIR_MODE,
        **counters,
        "violations": violations,
        "status": (
            "PASS"
            if counters.get("rows", 0) > 0 and violations == 0
            else "FAIL_CLOSED"
        ),
    }


def _validate_continuity_only_overlay(
    connection: duckdb.DuckDBPyConnection,
    *,
    hour: datetime,
) -> dict[str, Any]:
    """Validate the physical zero-row sentinel without treating it as events."""

    columns = {
        str(row[0])
        for row in connection.execute("DESCRIBE SELECT * FROM gold").fetchall()
    }
    missing = sorted(_CONTINUITY_REQUIRED_COLUMNS - columns)
    rows = int(connection.execute("SELECT count(*) FROM gold").fetchone()[0])
    schema_complete = not missing
    violations = rows + len(missing)
    return {
        "schema_version": "polymarket-l2-gold-invariants-v1",
        "hour_start": hour.isoformat(),
        "mode": CONTINUITY_ONLY_MODE,
        "rows": rows,
        "schema_complete": schema_complete,
        "missing_schema_columns": missing,
        "coverage_repair_proven": schema_complete and rows == 0,
        "violations": violations,
        "status": "PASS" if violations == 0 else "FAIL_CLOSED",
    }


def _book_level_checks(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    row = connection.execute(
        """
        WITH levels AS (
            SELECT
                asset_id,
                group_id,
                'BID' AS side,
                try_cast(
                    json_extract_string(level.value, '$[0]')
                    AS DECIMAL(38, 18)
                ) AS price,
                try_cast(
                    json_extract_string(level.value, '$[1]')
                    AS DECIMAL(38, 18)
                ) AS size
            FROM gold, json_each(coalesce(bids, '[]')) level
            WHERE event_type = 'book'
            UNION ALL
            SELECT
                asset_id,
                group_id,
                'ASK' AS side,
                try_cast(
                    json_extract_string(level.value, '$[0]')
                    AS DECIMAL(38, 18)
                ) AS price,
                try_cast(
                    json_extract_string(level.value, '$[1]')
                    AS DECIMAL(38, 18)
                ) AS size
            FROM gold, json_each(coalesce(asks, '[]')) level
            WHERE event_type = 'book'
        ),
        bad_levels AS (
            SELECT count(*)::BIGINT AS count
            FROM levels
            WHERE price IS NULL OR price <= 0 OR price >= 1
               OR size IS NULL OR size <= 0
        ),
        books AS (
            SELECT
                asset_id,
                group_id,
                max(price) FILTER (WHERE side = 'BID') AS best_bid,
                min(price) FILTER (WHERE side = 'ASK') AS best_ask
            FROM levels
            GROUP BY asset_id, group_id
        )
        SELECT
            (SELECT count FROM bad_levels),
            count(*) FILTER (
                WHERE best_bid IS NOT NULL
                  AND best_ask IS NOT NULL
                  AND best_bid >= best_ask
            )::BIGINT
        FROM books
        """
    ).fetchone()
    return {
        "invalid_book_levels": int(row[0] or 0),
        "crossed_book_snapshots": int(row[1] or 0),
    }


def _tick_grid_checks(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, int]:
    row = connection.execute(
        """
        WITH ordered AS (
            SELECT
                event_type,
                try_cast(price AS DECIMAL(38, 18)) AS price,
                last_value(
                    CASE
                        WHEN event_type = 'tick_size_change'
                        THEN try_cast(
                            new_tick_size AS DECIMAL(38, 18)
                        )
                        ELSE NULL
                    END IGNORE NULLS
                ) OVER (
                    PARTITION BY asset_id
                    ORDER BY
                        timestamp_received,
                        coalesce(raw_frame_seq, 0),
                        coalesce(message_index, 0),
                        coalesce(change_index, 0),
                        coalesce(collector_seq, 0)
                    ROWS BETWEEN UNBOUNDED PRECEDING
                             AND CURRENT ROW
                ) AS active_tick
            FROM gold
        )
        SELECT count(*)::BIGINT
        FROM ordered
        WHERE event_type = 'price_change'
          AND active_tick IS NOT NULL
          AND active_tick > 0
          AND mod(price, active_tick) != 0
        """
    ).fetchone()
    return {"tick_grid_violations": int(row[0] or 0)}


def _floor_hour(value: datetime) -> datetime:
    observed = (
        value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    )
    return observed.astimezone(timezone.utc).replace(
        minute=0,
        second=0,
        microsecond=0,
    )


def _sql(value: str) -> str:
    return str(value).replace("'", "''")
