"""Synchronous API service for strategy-independent market settlement."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

from quant.core.db import ClickHouseClient

from .market_settlement import (
    EXACT_HEADER_REQUIRED,
    ORACLE_EVENT_CUTOFF_BLOCK,
    ORACLE_FINALIZED_REQUIRED,
    iter_market_settlement_catalog,
    resolve_polygon_cutoff_boundary,
)

UTC = timezone.utc
MAX_SYNC_MARKETS = 5_000
ALLOWED_RESOLVE_FIELDS = {
    "marketIds",
    "market_ids",
    "cutoffTs",
    "cutoff_ts",
    "persist",
    "replace",
    "evidencePolicy",
    "evidence_policy",
}


class MarketSettlementServiceError(RuntimeError):
    def __init__(self, message: str, *, error_code: str, status_code: int) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code

    def as_dict(self) -> dict[str, object]:
        return {"error": str(self), "error_code": self.error_code}


def _cutoff(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise MarketSettlementServiceError(
            "cutoffTs is required",
            error_code="SETTLEMENT_CUTOFF_REQUIRED",
            status_code=400,
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MarketSettlementServiceError(
            "cutoffTs must be ISO-8601",
            error_code="INVALID_SETTLEMENT_CUTOFF",
            status_code=400,
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MarketSettlementServiceError(
            "cutoffTs must be timezone-aware",
            error_code="INVALID_SETTLEMENT_CUTOFF",
            status_code=400,
        )
    return parsed.astimezone(UTC)


def _market_ids(payload: Mapping[str, object]) -> tuple[int, ...]:
    raw = payload.get("marketIds", payload.get("market_ids"))
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise MarketSettlementServiceError(
            "marketIds must be a non-empty integer list",
            error_code="INVALID_MARKET_IDS",
            status_code=400,
        )
    try:
        values = tuple(sorted({int(str(item)) for item in raw}))
    except (TypeError, ValueError) as exc:
        raise MarketSettlementServiceError(
            "marketIds must contain integers",
            error_code="INVALID_MARKET_IDS",
            status_code=400,
        ) from exc
    if not values or any(item <= 0 for item in values):
        raise MarketSettlementServiceError(
            "marketIds must contain positive integers",
            error_code="INVALID_MARKET_IDS",
            status_code=400,
        )
    if len(values) > MAX_SYNC_MARKETS:
        raise MarketSettlementServiceError(
            f"synchronous resolution is limited to {MAX_SYNC_MARKETS} markets; use the partitioned catalog builder",
            error_code="SETTLEMENT_BATCH_TOO_LARGE",
            status_code=413,
        )
    return values


def _json_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if hasattr(value, "as_tuple"):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _persist_records(
    conn: Any,
    *,
    cutoff_ts: datetime,
    records: Sequence[Mapping[str, object]],
    replace: bool,
    source_scope: Mapping[str, object],
) -> None:
    market_ids = [int(record["market_id"]) for record in records]
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT market_id, record_sha256
            FROM quant.fill_only_market_settlement_catalog
            WHERE cutoff_ts=%s AND market_id=ANY(%s::bigint[])
            """,
            (cutoff_ts, market_ids),
        )
        existing = {
            int(row["market_id"]): str(row["record_sha256"])
            for row in cursor.fetchall()
        }
        conflicts = [
            int(record["market_id"])
            for record in records
            if int(record["market_id"]) in existing
            and existing[int(record["market_id"])] != record["record_sha256"]
        ]
        if conflicts and not replace:
            raise MarketSettlementServiceError(
                f"stored settlement records differ for markets {conflicts[:10]}",
                error_code="SETTLEMENT_SNAPSHOT_CONTRACT_CONFLICT",
                status_code=409,
            )
        for record in records:
            cursor.execute(
                """
                INSERT INTO quant.fill_only_market_settlement_catalog (
                    cutoff_ts, market_id, classification, record_sha256,
                    record, source_scope
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
                ON CONFLICT (cutoff_ts, market_id) DO UPDATE SET
                    classification=EXCLUDED.classification,
                    record_sha256=EXCLUDED.record_sha256,
                    record=EXCLUDED.record,
                    source_scope=EXCLUDED.source_scope,
                    updated_at=clock_timestamp()
                """,
                (
                    cutoff_ts,
                    record["market_id"],
                    record["classification"],
                    record["record_sha256"],
                    json.dumps(_json_value(record)),
                    json.dumps(_json_value(source_scope)),
                ),
            )


def resolve_market_settlements(
    conn: Any,
    payload: Mapping[str, object],
    *,
    clickhouse: ClickHouseClient | None = None,
    polygon_rpc_url: str | None = None,
) -> dict[str, object]:
    unknown = set(payload) - ALLOWED_RESOLVE_FIELDS
    if unknown:
        raise MarketSettlementServiceError(
            f"unknown settlement fields: {sorted(unknown)}",
            error_code="UNKNOWN_SETTLEMENT_FIELD",
            status_code=400,
        )
    market_ids = _market_ids(payload)
    cutoff_ts = _cutoff(payload.get("cutoffTs", payload.get("cutoff_ts")))
    persist = payload.get("persist", True)
    replace = payload.get("replace", False)
    if not isinstance(persist, bool) or not isinstance(replace, bool):
        raise MarketSettlementServiceError(
            "persist and replace must be booleans",
            error_code="INVALID_SETTLEMENT_OPTION",
            status_code=400,
        )
    raw_policy = payload.get(
        "evidencePolicy", payload.get("evidence_policy", "oracle-finalized")
    )
    policy_by_api_name = {
        "oracle-finalized": ORACLE_FINALIZED_REQUIRED,
        "exact-header": EXACT_HEADER_REQUIRED,
        "oracle-event-cutoff-block": ORACLE_EVENT_CUTOFF_BLOCK,
    }
    evidence_policy = policy_by_api_name.get(str(raw_policy))
    if evidence_policy is None:
        raise MarketSettlementServiceError(
            "evidencePolicy must be oracle-finalized, exact-header, or "
            "oracle-event-cutoff-block",
            error_code="INVALID_SETTLEMENT_EVIDENCE_POLICY",
            status_code=400,
        )
    source = clickhouse or ClickHouseClient()
    cutoff_boundary = (
        None
        if evidence_policy == EXACT_HEADER_REQUIRED
        else resolve_polygon_cutoff_boundary(
            polygon_rpc_url or "",
            cutoff_ts=cutoff_ts,
            clickhouse=source,
        )
    )
    records = tuple(
        iter_market_settlement_catalog(
            conn,
            market_ids,
            cutoff_ts=cutoff_ts,
            clickhouse=source,
            polygon_rpc_url=polygon_rpc_url,
            evidence_policy=evidence_policy,
            cutoff_block_boundary=cutoff_boundary,
            rpc_backfill_missing_headers=(
                evidence_policy == EXACT_HEADER_REQUIRED
            ),
        )
    )
    rendered = tuple(record.as_dict() for record in records)
    source_scope = {
        "mode": "SYNC_MARKET_ID_BATCH",
        "market_count": len(market_ids),
        "market_ids_sha256": sha256(
            json.dumps(market_ids, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "evidence_policy": evidence_policy,
        "cutoff_block_boundary_sha256": (
            None
            if cutoff_boundary is None
            else cutoff_boundary["boundary_sha256"]
        ),
    }
    if persist:
        _persist_records(
            conn,
            cutoff_ts=cutoff_ts,
            records=rendered,
            replace=replace,
            source_scope=source_scope,
        )
    counts = Counter(record.classification for record in records)
    return {
        "schema_version": "fill_only_market_settlement_batch_response_v1",
        "cutoff_ts": cutoff_ts,
        "market_count": len(records),
        "classification_counts": dict(sorted(counts.items())),
        "evidence_policy": evidence_policy,
        "cutoff_block_boundary": cutoff_boundary,
        "persisted": persist,
        "records": rendered,
    }


def get_market_settlement_snapshot(
    conn: Any, *, market_id: int, cutoff_ts: datetime
) -> dict[str, object] | None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT cutoff_ts, market_id, classification, record_sha256,
                   record, source_scope, created_at, updated_at
            FROM quant.fill_only_market_settlement_catalog
            WHERE cutoff_ts=%s AND market_id=%s
            """,
            (cutoff_ts, market_id),
        )
        row = cursor.fetchone()
    return None if row is None else dict(row)


def get_market_settlement_coverage(
    conn: Any, *, cutoff_ts: datetime
) -> dict[str, object]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT classification, count(*)::bigint AS market_count
            FROM quant.fill_only_market_settlement_catalog
            WHERE cutoff_ts=%s
            GROUP BY classification
            ORDER BY classification
            """,
            (cutoff_ts,),
        )
        rows = [dict(row) for row in cursor.fetchall()]
    return {
        "cutoff_ts": cutoff_ts,
        "market_count": sum(int(row["market_count"]) for row in rows),
        "classification_counts": {
            str(row["classification"]): int(row["market_count"]) for row in rows
        },
    }


def parse_settlement_cutoff(value: object) -> datetime:
    """Public strict parser shared by GET routes."""

    return _cutoff(value)
