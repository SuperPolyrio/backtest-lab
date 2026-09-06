"""Persistence helpers for multi-market backtest benchmarks."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
import json
from typing import Any


def create_benchmark_run(
    conn: Any,
    *,
    universe_type: str,
    universe_name: str,
    market_count: int,
    strategy_name: str,
    parameters: dict[str, Any],
    profiles: dict[str, Any],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO quant.quant_backtest_benchmark_runs (
                status, universe_type, universe_name, market_count,
                strategy_name, parameters, profiles, started_at
            )
            VALUES ('running', %s, %s, %s, %s, %s::jsonb, %s::jsonb, now())
            RETURNING benchmark_id
            """,
            (
                universe_type,
                universe_name,
                int(market_count),
                strategy_name,
                _json_dumps(parameters),
                _json_dumps(profiles),
            ),
        )
        row = cur.fetchone()
        return int(row["benchmark_id"] if isinstance(row, dict) else row[0])


def complete_benchmark_run(
    conn: Any,
    *,
    benchmark_id: int,
    summary: dict[str, Any],
    rows: list[Any],
    artifacts: dict[str, Any] | None = None,
    data_version: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM quant.quant_backtest_benchmark_rows WHERE benchmark_id = %s", (int(benchmark_id),))
        cur.execute("DELETE FROM quant.quant_backtest_benchmark_artifacts WHERE benchmark_id = %s", (int(benchmark_id),))
        cur.executemany(
            """
            INSERT INTO quant.quant_backtest_benchmark_rows (
                benchmark_id, row_index, market_id, market_slug, title, event_time,
                outcome, signal_time, fast_status, accurate_status, fast_pnl,
                accurate_pnl, pnl_diff, fast_fill_block, accurate_fill_block,
                data_quality, payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            [
                (
                    int(benchmark_id),
                    index,
                    _get(row, "market_id"),
                    _get(row, "market_slug"),
                    _get(row, "title"),
                    _parse_timestamp(_get(row, "event_time")),
                    _get(row, "buy_outcome_label") or _get(row, "outcome"),
                    _parse_timestamp(_get(row, "signal_time")),
                    _get(row, "fast_status"),
                    _get(row, "accurate_status"),
                    Decimal(str(_get(row, "fast_pnl") or "0")),
                    Decimal(str(_get(row, "accurate_pnl") or "0")),
                    Decimal(str(_get(row, "pnl_diff") or "0")),
                    int(_get(row, "fast_fill_block") or 0),
                    int(_get(row, "accurate_fill_block") or 0),
                    _get(row, "data_quality") or "unknown",
                    _json_dumps(_as_plain(row)),
                )
                for index, row in enumerate(rows, start=1)
            ],
        )
        artifact_items = artifacts or {}
        cur.executemany(
            """
            INSERT INTO quant.quant_backtest_benchmark_artifacts (
                benchmark_id, artifact_key, artifact_kind, payload
            )
            VALUES (%s, %s, %s, %s::jsonb)
            """,
            [
                (int(benchmark_id), key, _artifact_kind(value), _json_dumps(value))
                for key, value in artifact_items.items()
            ],
        )
        cur.execute(
            """
            UPDATE quant.quant_backtest_benchmark_runs
            SET status = 'completed',
                summary = %s::jsonb,
                data_version = %s,
                finished_at = now(),
                error = NULL
            WHERE benchmark_id = %s
            """,
            (_json_dumps(summary), data_version, int(benchmark_id)),
        )


def fail_benchmark_run(conn: Any, *, benchmark_id: int, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE quant.quant_backtest_benchmark_runs
            SET status = 'failed', error = %s, finished_at = now()
            WHERE benchmark_id = %s
            """,
            (str(error), int(benchmark_id)),
        )


def list_benchmark_runs(conn: Any, *, limit: int = 25) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_benchmark_runs
            ORDER BY created_at DESC, benchmark_id DESC
            LIMIT %s
            """,
            (int(limit),),
        )
        return [dict(row) for row in cur.fetchall()]


def get_benchmark_run(conn: Any, *, benchmark_id: int) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_benchmark_runs
            WHERE benchmark_id = %s
            """,
            (int(benchmark_id),),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_benchmark_rows(conn: Any, *, benchmark_id: int, limit: int = 10000) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_benchmark_rows
            WHERE benchmark_id = %s
            ORDER BY row_index ASC
            LIMIT %s
            """,
            (int(benchmark_id), int(limit)),
        )
        return [dict(row) for row in cur.fetchall()]


def get_benchmark_artifacts(conn: Any, *, benchmark_id: int) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_benchmark_artifacts
            WHERE benchmark_id = %s
            ORDER BY artifact_key ASC
            """,
            (int(benchmark_id),),
        )
        return [dict(row) for row in cur.fetchall()]


def _get(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def _as_plain(value: Any) -> Any:
    if is_dataclass(value):
        return _as_plain(asdict(value))
    if isinstance(value, dict):
        return {str(key): _as_plain(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_as_plain(item) for item in value]
    if isinstance(value, tuple):
        return [_as_plain(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _json_dumps(value: Any) -> str:
    return json.dumps(_as_plain(value), ensure_ascii=True, sort_keys=True, default=str)


def _artifact_kind(value: Any) -> str:
    if isinstance(value, list):
        return "rows"
    if isinstance(value, dict):
        return "json"
    return "value"


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
