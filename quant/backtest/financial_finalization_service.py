"""Database and API service for post-execution Fill-only finalization."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from typing import Any, cast

from quant.core.db import ClickHouseClient

from .fill_only_financials import (
    NORMAL_ORACLE_FINALIZED,
    FillOnlyExecutionPosition,
    finalize_fill_only_positions,
)
from .market_settlement import (
    EXACT_HEADER_REQUIRED,
    ORACLE_EVENT_CUTOFF_BLOCK,
    ORACLE_FINALIZED_REQUIRED,
    iter_market_settlement_catalog,
    resolve_polygon_cutoff_boundary,
)
from .pml2.financial import default_execution_adapter_registry

UTC = timezone.utc
ALLOWED_FINALIZE_FIELDS = {
    "cutoffTs",
    "cutoff_ts",
    "evidencePolicy",
    "evidence_policy",
    "replace",
}


class FinancialFinalizationServiceError(RuntimeError):
    def __init__(self, message: str, *, error_code: str, status_code: int) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code

    def as_dict(self) -> dict[str, object]:
        return {"error": str(self), "error_code": self.error_code}


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _canonical_sha256(value: object) -> str:
    return sha256(
        json.dumps(
            _json_value(value),
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _cutoff(payload: Mapping[str, object], run_meta: Mapping[str, object]) -> datetime:
    raw = (
        payload.get("cutoffTs")
        or payload.get("cutoff_ts")
        or run_meta.get("settlement_cutoff_ts")
        or run_meta.get("settlementCutoffTs")
    )
    if raw is None:
        raise FinancialFinalizationServiceError(
            "financial finalization requires a precommitted cutoffTs",
            error_code="SETTLEMENT_CUTOFF_REQUIRED",
            status_code=400,
        )
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise FinancialFinalizationServiceError(
            "cutoffTs must be ISO-8601",
            error_code="INVALID_SETTLEMENT_CUTOFF",
            status_code=400,
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FinancialFinalizationServiceError(
            "cutoffTs must be timezone-aware",
            error_code="INVALID_SETTLEMENT_CUTOFF",
            status_code=400,
        )
    return parsed.astimezone(UTC)


def _run_and_market(conn: Any, run_id: int) -> tuple[dict[str, object], dict[str, object]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT r.*, p.final_valuation_mode
            FROM quant.quant_backtest_runs r
            LEFT JOIN quant.quant_backtest_parameters p ON p.run_id=r.run_id
            WHERE r.run_id=%s
            """,
            (run_id,),
        )
        run_raw = cursor.fetchone()
        if run_raw is None:
            raise FinancialFinalizationServiceError(
                "backtest run not found",
                error_code="BACKTEST_RUN_NOT_FOUND",
                status_code=404,
            )
        run = dict(run_raw)
        if str(run.get("status") or "").lower() != "succeeded":
            raise FinancialFinalizationServiceError(
                "execution must succeed before financial finalization",
                error_code="EXECUTION_NOT_COMPLETE",
                status_code=409,
            )
        cursor.execute(
            """
            WITH candidates AS (
                SELECT m.id AS market_id, lower(m.condition_id) AS condition_id,
                       mt.token_id AS asset_id, upper(mt.outcome) AS outcome
                FROM core.markets m
                JOIN core.market_tokens mt ON mt.market_id=m.id
                WHERE m.slug=%s AND upper(mt.outcome)=upper(%s)
                UNION ALL
                SELECT m.id, lower(m.condition_id), m.yes_token_id, 'YES'
                FROM core.markets m
                WHERE m.slug=%s AND upper(%s)='YES' AND m.yes_token_id IS NOT NULL
                UNION ALL
                SELECT m.id, lower(m.condition_id), m.no_token_id, 'NO'
                FROM core.markets m
                WHERE m.slug=%s AND upper(%s)='NO' AND m.no_token_id IS NOT NULL
            )
            SELECT DISTINCT market_id, condition_id, asset_id, outcome
            FROM candidates
            ORDER BY market_id DESC, outcome, asset_id
            """,
            (
                run["market_slug"],
                run["token_side"],
                run["market_slug"],
                run["token_side"],
                run["market_slug"],
                run["token_side"],
            ),
        )
        markets = [dict(item) for item in cursor.fetchall()]
    if len(markets) != 1:
        raise FinancialFinalizationServiceError(
            "run market/token identity is missing or ambiguous",
            error_code="RUN_MARKET_IDENTITY_UNRESOLVABLE",
            status_code=409,
        )
    return run, markets[0]


def _fill_evidence(
    meta: Mapping[str, object],
) -> tuple[tuple[datetime, int | None, str], ...]:
    """Extract execution timing through the shared model-adapter registry."""

    evidence = default_execution_adapter_registry().extract(meta)
    return tuple(
        (item.fill_ts, item.fill_block, item.source_fill_id) for item in evidence
    )


def _open_positions(
    conn: Any,
    *,
    run: Mapping[str, object],
    market: Mapping[str, object],
) -> tuple[str, tuple[FillOnlyExecutionPosition, ...]]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT order_id, submit_x, side, status, actual_fill_size,
                   actual_fill_notional,
                   COALESCE(NULLIF(meta->>'actual_fee_cost', '')::numeric, fee_cost) AS fee_cost,
                   COALESCE(NULLIF(meta->>'actual_rebate', '')::numeric, rebate_cost) AS rebate_cost,
                   meta
            FROM quant.quant_backtest_orders
            WHERE run_id=%s AND actual_fill_size > 0
            ORDER BY submit_x, order_id
            """,
            (run["run_id"],),
        )
        order_rows = [dict(item) for item in cursor.fetchall()]
    execution_hash = _canonical_sha256(
        {
            "run": {
                "run_id": run["run_id"],
                "market_slug": run["market_slug"],
                "token_side": run["token_side"],
                "price_source": run["price_source"],
                "backtest_engine": run["backtest_engine"],
            },
            "orders": order_rows,
        }
    )
    lots: list[dict[str, object]] = []
    for row in order_rows:
        size = Decimal(str(row.get("actual_fill_size") or 0))
        notional = Decimal(str(row.get("actual_fill_notional") or 0))
        fee = Decimal(str(row.get("fee_cost") or 0)) - Decimal(
            str(row.get("rebate_cost") or 0)
        )
        side = str(row.get("side") or "").upper()
        if side.startswith("BUY"):
            raw_meta = row.get("meta")
            meta: Mapping[str, object] = raw_meta if isinstance(raw_meta, Mapping) else {}
            fills = _fill_evidence(meta)
            if not fills:
                raise FinancialFinalizationServiceError(
                    f"order {row['order_id']} lacks supported fill timing evidence",
                    error_code="EXECUTION_TIMING_EVIDENCE_MISSING",
                    status_code=409,
                )
            lots.append(
                {
                    "order_id": str(row["order_id"]),
                    "remaining_size": size,
                    "remaining_notional": notional,
                    "remaining_fee": max(fee, Decimal(0)),
                    "fills": fills,
                    "meta_sha256": _canonical_sha256(meta),
                }
            )
            continue
        if not side.startswith("SELL"):
            continue
        remaining = size
        while remaining > 0 and lots:
            lot = lots[0]
            available = Decimal(str(lot["remaining_size"]))
            consumed = min(remaining, available)
            fraction = consumed / available
            lot["remaining_size"] = available - consumed
            lot["remaining_notional"] = Decimal(str(lot["remaining_notional"])) * (
                Decimal(1) - fraction
            )
            lot["remaining_fee"] = Decimal(str(lot["remaining_fee"])) * (
                Decimal(1) - fraction
            )
            remaining -= consumed
            if Decimal(str(lot["remaining_size"])) == 0:
                lots.pop(0)
        if remaining > 0:
            raise FinancialFinalizationServiceError(
                "filled SELL inventory exceeds prior BUY inventory",
                error_code="EXECUTION_POSITION_LEDGER_CONFLICT",
                status_code=409,
            )

    positions: list[FillOnlyExecutionPosition] = []
    for lot in lots:
        remaining_size = Decimal(str(lot["remaining_size"]))
        if remaining_size <= 0:
            continue
        lot_fills = cast(
            tuple[tuple[datetime, int | None, str], ...], lot["fills"]
        )
        positions.append(
            FillOnlyExecutionPosition(
                execution_run_id=str(run["run_id"]),
                profile=str(run.get("backtest_engine") or "builtin"),
                position_id=str(lot["order_id"]),
                market_id=int(str(market["market_id"])),
                condition_id=str(market["condition_id"]),
                asset_id=str(market["asset_id"]),
                outcome=str(market["outcome"]),
                filled_size=remaining_size,
                entry_notional=Decimal(str(lot["remaining_notional"])),
                fee_paid=Decimal(str(lot["remaining_fee"])),
                first_fill_ts=lot_fills[0][0],
                last_fill_ts=lot_fills[-1][0],
                first_fill_block=lot_fills[0][1],
                last_fill_block=lot_fills[-1][1],
                source_order_ids=(str(lot["order_id"]),),
                source_fill_ids=tuple(item[2] for item in lot_fills),
                execution_evidence_sha256=str(lot["meta_sha256"]),
            )
        )
    return execution_hash, tuple(positions)


def get_backtest_financials(conn: Any, run_id: int) -> dict[str, object] | None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT run_id, status, cutoff_ts, execution_manifest_sha256,
                   settlement_catalog_sha256, summary, error_code, error,
                   created_at, started_at, finished_at, updated_at
            FROM quant.quant_backtest_financial_finalizations
            WHERE run_id=%s
            """,
            (run_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        cursor.execute(
            """
            SELECT result
            FROM quant.quant_backtest_financial_positions
            WHERE run_id=%s
            ORDER BY position_id
            """,
            (run_id,),
        )
        positions = [dict(item)["result"] for item in cursor.fetchall()]
    result = dict(row)
    result["positions"] = positions
    return result


def finalize_registered_backtest_run(
    conn: Any,
    run_id: int,
    payload: Mapping[str, object],
    *,
    clickhouse: ClickHouseClient | None = None,
    polygon_rpc_url: str | None = None,
) -> dict[str, object]:
    unknown = set(payload) - ALLOWED_FINALIZE_FIELDS
    if unknown:
        raise FinancialFinalizationServiceError(
            f"unknown finalization fields: {sorted(unknown)}",
            error_code="UNKNOWN_FINALIZATION_FIELD",
            status_code=400,
        )
    run, market = _run_and_market(conn, run_id)
    raw_run_meta = run.get("meta")
    run_meta: Mapping[str, object] = (
        raw_run_meta if isinstance(raw_run_meta, Mapping) else {}
    )
    cutoff = _cutoff(payload, run_meta)
    raw_policy = payload.get(
        "evidencePolicy", payload.get("evidence_policy", NORMAL_ORACLE_FINALIZED)
    )
    policy_by_api_name = {
        NORMAL_ORACLE_FINALIZED: ORACLE_FINALIZED_REQUIRED,
        "exact-header": EXACT_HEADER_REQUIRED,
        "oracle-event-cutoff-block": ORACLE_EVENT_CUTOFF_BLOCK,
    }
    evidence_policy = policy_by_api_name.get(str(raw_policy))
    if evidence_policy is None:
        raise FinancialFinalizationServiceError(
            "evidencePolicy must be oracle-finalized, exact-header, or "
            "oracle-event-cutoff-block",
            error_code="INVALID_SETTLEMENT_EVIDENCE_POLICY",
            status_code=400,
        )
    execution_hash, positions = _open_positions(conn, run=run, market=market)
    replace = payload.get("replace") is True
    existing = get_backtest_financials(conn, run_id)
    if existing is not None and not replace:
        raw_existing_summary = existing.get("summary")
        existing_summary: Mapping[str, object] = (
            raw_existing_summary
            if isinstance(raw_existing_summary, Mapping)
            else {}
        )
        if (
            str(existing["execution_manifest_sha256"]) == execution_hash
            and existing["cutoff_ts"] == cutoff
            and existing_summary.get("required_settlement_evidence")
            == str(raw_policy)
            and str(existing["status"]) in {"FINANCIAL_COMPLETE", "FINANCIAL_PARTIAL", "N/A_NO_DEPLOYED_CAPITAL"}
        ):
            return existing
        raise FinancialFinalizationServiceError(
            "financial finalization already exists with a different contract",
            error_code="FINALIZATION_CONTRACT_CONFLICT",
            status_code=409,
        )
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO quant.quant_backtest_financial_finalizations (
                run_id, status, cutoff_ts, execution_manifest_sha256, started_at, updated_at
            ) VALUES (%s, 'SETTLEMENT_PENDING', %s, %s, clock_timestamp(), clock_timestamp())
            ON CONFLICT (run_id) DO UPDATE SET
                status='SETTLEMENT_PENDING', cutoff_ts=EXCLUDED.cutoff_ts,
                execution_manifest_sha256=EXCLUDED.execution_manifest_sha256,
                settlement_catalog_sha256=NULL, summary='{}'::jsonb,
                error_code=NULL, error=NULL, started_at=clock_timestamp(),
                finished_at=NULL, updated_at=clock_timestamp()
            """,
            (run_id, cutoff, execution_hash),
        )
        cursor.execute(
            "DELETE FROM quant.quant_backtest_market_settlement_snapshots WHERE run_id=%s",
            (run_id,),
        )
        cursor.execute(
            "DELETE FROM quant.quant_backtest_financial_positions WHERE run_id=%s",
            (run_id,),
        )

    source = clickhouse or ClickHouseClient()
    cutoff_boundary = (
        None
        if evidence_policy == EXACT_HEADER_REQUIRED
        else resolve_polygon_cutoff_boundary(
            polygon_rpc_url or "",
            cutoff_ts=cutoff,
            clickhouse=source,
        )
    )
    records = tuple(
        iter_market_settlement_catalog(
            conn,
            [int(str(market["market_id"]))],
            cutoff_ts=cutoff,
            clickhouse=source,
            polygon_rpc_url=polygon_rpc_url,
            evidence_policy=evidence_policy,
            cutoff_block_boundary=cutoff_boundary,
            rpc_backfill_missing_headers=(
                evidence_policy == EXACT_HEADER_REQUIRED
            ),
        )
    )
    settlement = records[0]
    catalog_hash = _canonical_sha256(
        {
            "cutoff_ts": cutoff,
            "evidence_policy": evidence_policy,
            "cutoff_block_boundary_sha256": (
                None
                if cutoff_boundary is None
                else cutoff_boundary["boundary_sha256"]
            ),
            "records": [settlement.record_sha256],
        }
    )
    summary, position_results = finalize_fill_only_positions(
        positions,
        {settlement.market_id: settlement},
        execution_manifest_sha256=execution_hash,
        settlement_catalog_sha256=catalog_hash,
        cutoff_ts=cutoff,
        required_evidence_grade=str(raw_policy),
    )
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO quant.quant_backtest_market_settlement_snapshots (
                run_id, market_id, cutoff_ts, classification, record_sha256, record
            ) VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                run_id,
                settlement.market_id,
                cutoff,
                settlement.classification,
                settlement.record_sha256,
                json.dumps(_json_value(settlement.as_dict())),
            ),
        )
        for result in position_results:
            cursor.execute(
                """
                INSERT INTO quant.quant_backtest_financial_positions (
                    run_id, position_id, market_id, condition_id, asset_id, outcome,
                    filled_size, entry_notional, recorded_fee,
                    settlement_classification, settlement_complete,
                    payout_per_share, settlement_payout, net_pnl, result
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    run_id,
                    result["position_id"],
                    result["market_id"],
                    result["condition_id"],
                    result["asset_id"],
                    result["outcome"],
                    result["filled_size"],
                    result["entry_notional"],
                    result["fee_paid"],
                    result["settlement_classification"],
                    result["settlement_complete"],
                    result["payout_per_share"],
                    result["settlement_payout"],
                    result["net_pnl_after_recorded_fee"],
                    json.dumps(_json_value(result)),
                ),
            )
        cursor.execute(
            """
            UPDATE quant.quant_backtest_financial_finalizations
            SET status=%s, settlement_catalog_sha256=%s, summary=%s::jsonb,
                finished_at=clock_timestamp(), updated_at=clock_timestamp()
            WHERE run_id=%s
            """,
            (
                summary["status"],
                catalog_hash,
                json.dumps(_json_value(summary)),
                run_id,
            ),
        )
    return get_backtest_financials(conn, run_id) or {
        "run_id": run_id,
        "status": summary["status"],
        "summary": summary,
        "positions": position_results,
    }
