"""Utilities for creating a bounded fill-first sample backtest run."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from quant.backtest.backtest_engine import (
    BACKTEST_ARTIFACT_SCHEMA_VERSION,
    ORDERFILLED_CROSS_MODE,
    create_and_execute_backtest,
    normalize_execution_price_mode,
)
from quant.backtest.run_artifacts import (
    build_backtest_run_artifact_report,
    load_backtest_run_artifact_inputs,
    load_latest_fill_first_backtest_run_id,
)


READY = "ready"
REVIEW = "review"
MISSING = "missing"
UNKNOWN = "unknown"


@dataclass(frozen=True)
class FillFirstSampleCandidate:
    market_slug: str
    token_id: str | None
    token_side: str
    from_block: int | None
    to_block: int | None
    rows: int | None = None
    source: str = "unknown"
    seed_run_id: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_slug": self.market_slug,
            "token_id": self.token_id,
            "token_side": self.token_side,
            "from_block": self.from_block,
            "to_block": self.to_block,
            "rows": self.rows,
            "source": self.source,
            "seed_run_id": self.seed_run_id,
        }


def run_fill_first_sample_backtest(
    conn: Any,
    *,
    seed_run_id: int | None = None,
    market_slug: str | None = None,
    token_id: str | None = None,
    token_side: str = "YES",
    from_block: int | None = None,
    to_block: int | None = None,
    window_rows: int = 1000,
    min_orderfilled_rows: int = 25,
    entry_threshold: Decimal | str | None = None,
    exit_threshold: Decimal | str | None = None,
    take_profit: Decimal | str | None = None,
    position_size: Decimal | str | None = None,
    initial_capital: Decimal | str | None = None,
    order_role: str | None = None,
    buy_limit_price: Decimal | str | None = None,
    sell_limit_price: Decimal | str | None = None,
    settlement_value: Decimal | str | None = None,
    liquidity_cap_pct: Decimal | str | None = None,
    fill_probability_haircut_pct: Decimal | str | None = None,
    execution_price_mode: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Create a bounded sample run and audit it.

    Selection is intentionally bounded:
    - explicit market/token/window wins;
    - otherwise clone the latest persisted fill-first run window;
    - otherwise choose one token from materialized event-member coverage and
      read only that token's recent block-close rows.
    """

    candidate, payload = build_fill_first_sample_payload(
        conn,
        seed_run_id=seed_run_id,
        market_slug=market_slug,
        token_id=token_id,
        token_side=token_side,
        from_block=from_block,
        to_block=to_block,
        window_rows=window_rows,
        min_orderfilled_rows=min_orderfilled_rows,
        entry_threshold=entry_threshold,
        exit_threshold=exit_threshold,
        take_profit=take_profit,
        position_size=position_size,
        initial_capital=initial_capital,
        order_role=order_role,
        buy_limit_price=buy_limit_price,
        sell_limit_price=sell_limit_price,
        settlement_value=settlement_value,
        liquidity_cap_pct=liquidity_cap_pct,
        fill_probability_haircut_pct=fill_probability_haircut_pct,
        execution_price_mode=execution_price_mode,
    )
    if dry_run:
        return {
            "status": READY,
            "dry_run": True,
            "candidate": candidate.as_dict(),
            "payload": payload,
            "run": None,
            "artifact_report": None,
            "artifact_status": None,
            "artifact_schema_version": None,
        }

    row = create_and_execute_backtest(conn, payload)
    run_id = int(_row_get(row, "run_id"))
    inputs = load_backtest_run_artifact_inputs(conn, run_id=run_id)
    artifact_report = build_backtest_run_artifact_report(inputs, run_id=run_id)
    schema = _artifact_schema_version(artifact_report)
    status = READY if schema == BACKTEST_ARTIFACT_SCHEMA_VERSION and artifact_report.get("status") in {READY, REVIEW} else REVIEW
    return {
        "status": status,
        "dry_run": False,
        "candidate": candidate.as_dict(),
        "payload": payload,
        "run": dict(row),
        "run_id": run_id,
        "artifact_report": artifact_report,
        "artifact_status": artifact_report.get("status"),
        "artifact_schema_version": schema,
    }


def build_fill_first_sample_payload(
    conn: Any,
    *,
    seed_run_id: int | None = None,
    market_slug: str | None = None,
    token_id: str | None = None,
    token_side: str = "YES",
    from_block: int | None = None,
    to_block: int | None = None,
    window_rows: int = 1000,
    min_orderfilled_rows: int = 25,
    entry_threshold: Decimal | str | None = None,
    exit_threshold: Decimal | str | None = None,
    take_profit: Decimal | str | None = None,
    position_size: Decimal | str | None = None,
    initial_capital: Decimal | str | None = None,
    order_role: str | None = None,
    buy_limit_price: Decimal | str | None = None,
    sell_limit_price: Decimal | str | None = None,
    settlement_value: Decimal | str | None = None,
    liquidity_cap_pct: Decimal | str | None = None,
    fill_probability_haircut_pct: Decimal | str | None = None,
    execution_price_mode: str | None = None,
) -> tuple[FillFirstSampleCandidate, dict[str, Any]]:
    explicit = bool(market_slug or token_id)
    if explicit:
        candidate = _explicit_candidate(
            conn,
            market_slug=market_slug,
            token_id=token_id,
            token_side=token_side,
            from_block=from_block,
            to_block=to_block,
            window_rows=window_rows,
        )
        seed_params: Mapping[str, Any] = {}
    else:
        seed = _load_seed_run(conn, seed_run_id=seed_run_id)
        if seed is not None:
            candidate, seed_params = seed
        else:
            candidate = _select_materialized_candidate(conn, min_orderfilled_rows=min_orderfilled_rows, window_rows=window_rows)
            seed_params = {}

    resolved_execution_price_mode = normalize_execution_price_mode(
        execution_price_mode or seed_params.get("execution_price_mode") or ORDERFILLED_CROSS_MODE
    )
    payload = {
        "market_slug": candidate.market_slug,
        "token_id": candidate.token_id,
        "token_side": candidate.token_side,
        "price_source": "orderfilled_block_close",
        "backtest_engine": "builtin",
        "from_block": candidate.from_block,
        "to_block": candidate.to_block,
        "execution_price_mode": resolved_execution_price_mode,
        "execution_profile": str(seed_params.get("execution_profile") or "realistic"),
        "order_role": str(order_role or seed_params.get("order_role") or "maker").lower(),
        "entry_threshold": _decimal_text(entry_threshold, seed_params.get("entry_threshold"), "0.58"),
        "exit_threshold": _decimal_text(exit_threshold, seed_params.get("exit_threshold"), "0.44"),
        "take_profit": _decimal_text(take_profit, seed_params.get("take_profit"), "0.16"),
        "position_size": _decimal_text(position_size, seed_params.get("position_size"), "20"),
        "initial_capital": _decimal_text(initial_capital, seed_params.get("initial_capital"), "1000"),
        "buy_limit_price": _decimal_text_or_none(buy_limit_price, seed_params.get("buy_limit_price")),
        "sell_limit_price": _decimal_text_or_none(sell_limit_price, seed_params.get("sell_limit_price")),
        "settlement_value": _decimal_text_or_none(settlement_value, seed_params.get("settlement_value")),
        "liquidity_cap_pct": _decimal_text(liquidity_cap_pct, seed_params.get("liquidity_cap_pct"), "100"),
        "fill_probability_haircut_pct": _decimal_text(fill_probability_haircut_pct, seed_params.get("fill_probability_haircut_pct"), "20"),
        "allow_partial_fill": True,
        "final_valuation_mode": str(seed_params.get("final_valuation_mode") or "SETTLEMENT"),
    }
    return candidate, {key: value for key, value in payload.items() if value is not None}


def _load_seed_run(conn: Any, *, seed_run_id: int | None) -> tuple[FillFirstSampleCandidate, Mapping[str, Any]] | None:
    run_id = seed_run_id if seed_run_id is not None else load_latest_fill_first_backtest_run_id(conn)
    if run_id is None:
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.run_id, r.market_slug, r.token_side, r.from_block, r.to_block, r.meta,
                   p.entry_threshold, p.exit_threshold, p.initial_capital, p.position_size,
                   p.execution_profile, p.order_role, p.final_valuation_mode,
                   p.take_profit, p.buy_limit_price, p.sell_limit_price, p.settlement_value,
                   p.liquidity_cap_pct, p.fill_probability_haircut_pct
            FROM quant.quant_backtest_runs r
            LEFT JOIN quant.quant_backtest_parameters p ON p.run_id = r.run_id
            WHERE r.run_id = %s
            """,
            (int(run_id),),
        )
        row = cur.fetchone()
    if not row:
        return None
    meta = _row_get(row, "meta") or {}
    token_id = meta.get("token_id") if isinstance(meta, Mapping) else None
    candidate = FillFirstSampleCandidate(
        market_slug=str(_row_get(row, "market_slug") or ""),
        token_id=str(token_id).strip() if token_id else None,
        token_side=str(_row_get(row, "token_side") or "YES").upper(),
        from_block=_optional_int(_row_get(row, "from_block")),
        to_block=_optional_int(_row_get(row, "to_block")),
        rows=None,
        source="latest_fill_first_run",
        seed_run_id=int(run_id),
    )
    return candidate, dict(row)


def _explicit_candidate(
    conn: Any,
    *,
    market_slug: str | None,
    token_id: str | None,
    token_side: str,
    from_block: int | None,
    to_block: int | None,
    window_rows: int,
) -> FillFirstSampleCandidate:
    side = str(token_side or "YES").upper()
    resolved_token = str(token_id or "").strip() or None
    resolved_market = str(market_slug or "").strip()
    if not resolved_token and not resolved_market:
        raise ValueError("market_slug or token_id is required for explicit sample selection")
    if resolved_token and not resolved_market:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT market_slug, token_side
                FROM quant.market_token_metadata
                WHERE token_id = %s
                LIMIT 1
                """,
                (resolved_token,),
            )
            row = cur.fetchone()
        if row:
            resolved_market = str(_row_get(row, "market_slug") or resolved_market)
            side = str(_row_get(row, "token_side") or side).upper()
    if from_block is None or to_block is None:
        window = _recent_block_window_for_token_or_market(
            conn,
            token_id=resolved_token,
            market_slug=resolved_market,
            token_side=side,
            window_rows=window_rows,
        )
        from_block = window["from_block"] if from_block is None else from_block
        to_block = window["to_block"] if to_block is None else to_block
        rows = window["rows"]
    else:
        rows = None
    return FillFirstSampleCandidate(
        market_slug=resolved_market,
        token_id=resolved_token,
        token_side=side,
        from_block=_optional_int(from_block),
        to_block=_optional_int(to_block),
        rows=rows,
        source="explicit",
    )


def _select_materialized_candidate(conn: Any, *, min_orderfilled_rows: int, window_rows: int) -> FillFirstSampleCandidate:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT market_slug, token_yes_id AS token_id, 'YES' AS token_side,
                   latest_block, orderfilled_rows
            FROM quant.market_event_members
            WHERE token_yes_id IS NOT NULL
              AND latest_block IS NOT NULL
              AND orderfilled_rows >= %s
            ORDER BY latest_block DESC, orderfilled_rows DESC, market_id ASC
            LIMIT 1
            """,
            (int(min_orderfilled_rows),),
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError("no materialized fill-first sample candidate found")
    token_id = str(_row_get(row, "token_id"))
    window = _recent_block_window_for_token_or_market(
        conn,
        token_id=token_id,
        market_slug=str(_row_get(row, "market_slug") or ""),
        token_side=str(_row_get(row, "token_side") or "YES"),
        window_rows=window_rows,
    )
    return FillFirstSampleCandidate(
        market_slug=str(_row_get(row, "market_slug") or ""),
        token_id=token_id,
        token_side=str(_row_get(row, "token_side") or "YES").upper(),
        from_block=window["from_block"],
        to_block=window["to_block"],
        rows=window["rows"],
        source="market_event_members",
    )


def _recent_block_window_for_token_or_market(
    conn: Any,
    *,
    token_id: str | None,
    market_slug: str,
    token_side: str,
    window_rows: int,
) -> dict[str, int]:
    if token_id:
        where_sql = "token_id = %s"
        params: tuple[Any, ...] = (token_id, int(window_rows))
    else:
        where_sql = "market_slug = %s AND token_side = %s"
        params = (market_slug, token_side.upper(), int(window_rows))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT MIN(block_number) AS from_block,
                   MAX(block_number) AS to_block,
                   COUNT(*) AS rows
            FROM (
                SELECT block_number
                FROM quant.market_token_block_close
                WHERE {where_sql}
                ORDER BY block_number DESC
                LIMIT %s
            ) recent
            """,
            params,
        )
        row = cur.fetchone()
    rows = int(_row_get(row, "rows") or 0)
    if rows < 2:
        raise RuntimeError("selected sample candidate has fewer than 2 block-close rows")
    return {
        "from_block": int(_row_get(row, "from_block")),
        "to_block": int(_row_get(row, "to_block")),
        "rows": rows,
    }


def _artifact_schema_version(report: Mapping[str, Any]) -> str | None:
    reproducibility = report.get("reproducibility_report")
    if isinstance(reproducibility, Mapping) and reproducibility.get("artifact_schema_version"):
        return str(reproducibility["artifact_schema_version"])
    artifacts = report.get("artifacts")
    if isinstance(artifacts, Mapping) and artifacts.get("artifact_schema_version"):
        return str(artifacts["artifact_schema_version"])
    return None


def _decimal_text(override: Decimal | str | None, seed_value: Any, default: str) -> str:
    value = override if override is not None else seed_value
    if value in (None, ""):
        value = default
    return str(Decimal(str(value)))


def _decimal_text_or_none(override: Decimal | str | None, seed_value: Any) -> str | None:
    value = override if override is not None else seed_value
    if value in (None, ""):
        return None
    return str(Decimal(str(value)))


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _row_get(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return getattr(row, key)
