"""Strategy-independent, point-in-time Polymarket settlement catalog.

The catalog separates the current registry projection from the protocol
evidence required to pay a historical position. Ordinary financial settlement
requires a consistent token map, final Oracle event, settlement transaction and
block, credible settlement time, and a frozen cutoff boundary. Exact Polygon
block headers remain available as a stronger optional audit grade.
"""

from __future__ import annotations

import json
import os
import time
from bisect import bisect_left
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from http.client import IncompleteRead
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from quant.core.db import ClickHouseClient

UTC = timezone.utc
CATALOG_SCHEMA_VERSION = "fill_only_market_settlement_catalog_v1"
PARTITIONED_CATALOG_SCHEMA_VERSION = (
    "fill_only_market_settlement_partitioned_catalog_v1"
)
CATALOG_PART_SCHEMA_VERSION = "fill_only_market_settlement_catalog_part_v1"
CATALOG_BUILD_CONTRACT_SCHEMA_VERSION = (
    "fill_only_market_settlement_catalog_build_contract_v1"
)
RECORD_SCHEMA_VERSION = "fill_only_market_settlement_record_v1"
CUTOFF_BOUNDARY_SCHEMA_VERSION = "polygon_cutoff_block_boundary_v1"

EXACT_HEADER_REQUIRED = "EXACT_HEADER_REQUIRED"
ORACLE_FINALIZED_REQUIRED = "ORACLE_FINALIZED_REQUIRED"
ORACLE_EVENT_CUTOFF_BLOCK = "ORACLE_EVENT_CUTOFF_BLOCK"
SETTLEMENT_EVIDENCE_POLICIES = {
    EXACT_HEADER_REQUIRED,
    ORACLE_FINALIZED_REQUIRED,
    ORACLE_EVENT_CUTOFF_BLOCK,
}
EXACT_CANONICAL_BLOCK_HEADER_GRADE = "EXACT_CANONICAL_BLOCK_HEADER"
ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE = (
    "ORACLE_FINALIZED_WITH_CREDIBLE_TIME"
)
ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE = (
    "ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK"
)
SETTLEMENT_EVIDENCE_GRADES = {
    EXACT_CANONICAL_BLOCK_HEADER_GRADE,
    ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE,
    ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE,
}
CREDIBLE_BLOCK_TIME_SOURCE_PREFIXES = (
    "trade_time:pg_api_trades_tx_path",
    "trade_time:pg_api_trades_main",
)

SETTLED_YES = "SETTLED_VERIFIED_YES_AS_OF_CUTOFF"
SETTLED_NO = "SETTLED_VERIFIED_NO_AS_OF_CUTOFF"
CANCELLED_REFUND = "CANCELLED_VERIFIED_HALF_REFUND_AS_OF_CUTOFF"
UNRESOLVED = "UNRESOLVED_AS_OF_CUTOFF"
POST_CUTOFF = "POST_CUTOFF_SETTLEMENT"
TECHNICAL_MISSING = "TECHNICAL_EVIDENCE_MISSING"
IDENTITY_CONFLICT = "TOKEN_OR_IDENTITY_CONFLICT"

PAYABLE_CLASSIFICATIONS = {SETTLED_YES, SETTLED_NO, CANCELLED_REFUND}
UNRESOLVED_COMPLETION_STATUSES = {
    "OPEN",
    "ENDED_AWAITING_ORACLE",
    "GAMMA_CLOSED_FALLBACK",
    "CLOSED_UNRESOLVED",
    "PROPOSED",
    "DISPUTED",
}


class MarketSettlementError(RuntimeError):
    """Settlement catalog input or evidence violates the frozen contract."""


@dataclass(frozen=True)
class MarketSettlementRecord:
    market_id: int
    condition_id: str | None
    slug: str | None
    title: str | None
    category: str | None
    cutoff_ts: datetime
    classification: str
    classification_reason: str
    completion_status: str | None
    settlement_code: int | None
    settlement_outcome: str | None
    settlement_source: str | None
    protocol_finalized_at: datetime | None
    protocol_finalized_block: int | None
    settlement_event_id: int | None
    settlement_tx_hash: str | None
    tokens: tuple[dict[str, object], ...]
    payout_by_token: Mapping[str, Decimal] | None
    evidence: Mapping[str, object]
    record_sha256: str

    @property
    def payable(self) -> bool:
        return self.classification in PAYABLE_CLASSIFICATIONS

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": RECORD_SCHEMA_VERSION,
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "slug": self.slug,
            "title": self.title,
            "category": self.category,
            "cutoff_ts": self.cutoff_ts,
            "classification": self.classification,
            "classification_reason": self.classification_reason,
            "completion_status": self.completion_status,
            "settlement_code": self.settlement_code,
            "settlement_outcome": self.settlement_outcome,
            "settlement_source": self.settlement_source,
            "protocol_finalized_at": self.protocol_finalized_at,
            "protocol_finalized_block": self.protocol_finalized_block,
            "settlement_event_id": self.settlement_event_id,
            "settlement_tx_hash": self.settlement_tx_hash,
            "tokens": self.tokens,
            "payout_by_token": self.payout_by_token,
            "evidence": self.evidence,
            "record_sha256": self.record_sha256,
        }


def _utc(value: object, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise MarketSettlementError(f"{field} must be ISO-8601") from exc
    else:
        raise MarketSettlementError(f"{field} must be a datetime")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MarketSettlementError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _optional_utc(value: object, *, field: str) -> datetime | None:
    return None if value is None else _utc(value, field=field)


def _optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _canonical_json(value: object) -> str:
    return json.dumps(
        _json_value(value),
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _utc(value, field="artifact timestamp").isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _canonical_sha256(value: object) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def file_sha256(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Hash large artifacts without copying the entire file into memory."""

    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(_json_value(payload), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _normalized_hash(value: object) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value).strip().lower()
    body = text.removeprefix("0x")
    if len(body) != 64 or any(char not in "0123456789abcdef" for char in body):
        return None
    return "0x" + body


def _normalized_condition(value: object) -> str | None:
    return _normalized_hash(value)


def _normalized_address(value: object) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value).strip().lower()
    body = text.removeprefix("0x")
    if len(body) != 40 or any(char not in "0123456789abcdef" for char in body):
        return None
    return "0x" + body


def _normalize_tokens(raw: object) -> tuple[dict[str, object], ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return ()
    tokens: list[dict[str, object]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            return ()
        outcome = str(item.get("outcome") or "").strip().upper()
        token_id = str(item.get("token_id") or "").strip()
        outcome_index = _optional_int(item.get("outcome_index"))
        condition_id = _normalized_condition(item.get("condition_id"))
        if outcome not in {"YES", "NO"} or not token_id.isdigit():
            return ()
        tokens.append(
            {
                "outcome": outcome,
                "outcome_index": outcome_index,
                "token_id": token_id,
                "condition_id": condition_id,
            }
        )
    return tuple(
        sorted(tokens, key=lambda item: (str(item["outcome"]), str(item["token_id"])))
    )


def _trusted_header(
    rows: Sequence[Mapping[str, object]], *, expected_block: int
) -> tuple[datetime, str, tuple[str, ...], str] | None:
    if not rows:
        return None
    normalized: list[dict[str, object]] = []
    for row in rows:
        block = _optional_int(row.get("block_number"))
        block_hash = _normalized_hash(row.get("block_hash"))
        source = str(row.get("source") or "").strip().lower()
        if not source.startswith(("rpc", "polygon_rpc")):
            continue
        if block != expected_block or block_hash is None:
            return None
        normalized.append(
            {
                "block_number": block,
                "block_hash": block_hash,
                "block_time": _utc(row.get("block_time"), field="trusted block time"),
                "source": source,
            }
        )
    hashes = {str(item["block_hash"]) for item in normalized}
    times = {item["block_time"] for item in normalized}
    if len(hashes) != 1 or len(times) != 1:
        return None
    ordered = tuple(
        sorted(
            normalized,
            key=lambda item: (
                str(item["source"]),
                str(item["block_time"]),
                str(item["block_hash"]),
            ),
        )
    )
    return (
        next(iter(times)),
        next(iter(hashes)),
        tuple(sorted({str(item["source"]) for item in ordered})),
        _canonical_sha256(ordered),
    )


def _credible_block_time(
    rows: Sequence[Mapping[str, object]], *, expected_block: int
) -> tuple[datetime, tuple[str, ...], str] | None:
    """Resolve a block time without requiring a per-block canonical hash."""

    for source_prefix in CREDIBLE_BLOCK_TIME_SOURCE_PREFIXES:
        candidates: list[dict[str, object]] = []
        for row in rows:
            source = str(row.get("source") or "").strip().lower()
            if not source.startswith(source_prefix):
                continue
            block = _optional_int(row.get("block_number"))
            if block != expected_block:
                return None
            candidates.append(
                {
                    "block_number": block,
                    "block_time": _utc(
                        row.get("block_time"), field="credible block time"
                    ),
                    "source": source,
                }
            )
        if not candidates:
            continue
        times = {item["block_time"] for item in candidates}
        if len(times) != 1:
            return None
        return (
            next(iter(times)),
            tuple(sorted({str(item["source"]) for item in candidates})),
            _canonical_sha256(
                sorted(
                    candidates,
                    key=lambda item: (
                        str(item["source"]),
                        str(item["block_time"]),
                    ),
                )
            ),
        )
    return None


def classify_market_settlement(
    row: Mapping[str, object],
    *,
    cutoff_ts: datetime,
    trusted_header_rows: Sequence[Mapping[str, object]] = (),
    evidence_policy: str = EXACT_HEADER_REQUIRED,
    cutoff_block_boundary: Mapping[str, object] | None = None,
) -> MarketSettlementRecord:
    """Classify one market without turning missing evidence into a loss."""

    cutoff = _utc(cutoff_ts, field="settlement cutoff")
    if evidence_policy not in SETTLEMENT_EVIDENCE_POLICIES:
        raise MarketSettlementError(
            f"unsupported settlement evidence policy: {evidence_policy}"
        )
    boundary = _validated_cutoff_boundary(
        cutoff_block_boundary, cutoff_ts=cutoff
    )
    if evidence_policy in {
        ORACLE_FINALIZED_REQUIRED,
        ORACLE_EVENT_CUTOFF_BLOCK,
    } and boundary is None:
        raise MarketSettlementError(
            "Oracle settlement policies require a frozen cutoff boundary"
        )
    market_id = _optional_int(row.get("requested_market_id") or row.get("market_id"))
    if market_id is None or market_id <= 0:
        raise MarketSettlementError("requested market_id must be positive")

    condition_id = _normalized_condition(row.get("condition_id"))
    tokens = _normalize_tokens(row.get("tokens"))
    completion_status = str(row.get("completion_status") or "").strip().upper() or None
    settlement_outcome = str(row.get("settlement_outcome") or "").strip().upper() or None
    settlement_code = _optional_int(row.get("settlement_code"))
    settlement_event_id = _optional_int(row.get("settlement_event_id"))
    settlement_block = _optional_int(row.get("settlement_block_number"))
    settlement_tx = _normalized_hash(row.get("settlement_transaction"))
    event_tx = _normalized_hash(row.get("settlement_tx_hash"))
    event_condition = _normalized_condition(row.get("source_condition_id"))
    event_status = str(row.get("source_event_status") or "").strip().lower()
    source_event_time = _optional_utc(
        row.get("source_event_time"), field="Oracle source event time"
    )
    reported_event_time = _optional_utc(
        row.get("settlement_event_time"), field="reported settlement event time"
    )

    classification = TECHNICAL_MISSING
    reason = "MARKET_OR_STATUS_NOT_FOUND"
    payout_by_token: dict[str, Decimal] | None = None
    protocol_finalized_at: datetime | None = None
    exact_header_hash: str | None = None
    exact_header_sources: tuple[str, ...] = ()
    exact_header_rows_hash: str | None = None
    credible_time_sources: tuple[str, ...] = ()
    credible_time_rows_hash: str | None = None
    settlement_time_source: str | None = None
    evidence_grade: str | None = None

    market_found = bool(row.get("market_found"))
    status_found = bool(row.get("status_found"))
    token_outcomes = {str(item["outcome"]): str(item["token_id"]) for item in tokens}
    token_conditions = {item.get("condition_id") for item in tokens}
    token_map_valid = (
        market_found
        and len(tokens) == 2
        and set(token_outcomes) == {"YES", "NO"}
        and len(set(token_outcomes.values())) == 2
        and condition_id is not None
        and token_conditions == {condition_id}
        and str(row.get("yes_token_id") or "") == token_outcomes["YES"]
        and str(row.get("no_token_id") or "") == token_outcomes["NO"]
    )

    if not market_found:
        reason = "MARKET_NOT_FOUND"
    elif not status_found:
        reason = "MARKET_STATUS_NOT_FOUND"
    elif not token_map_valid:
        classification = IDENTITY_CONFLICT
        reason = "BINARY_TOKEN_MAPPING_OR_CONDITION_CONFLICT"
    elif completion_status in UNRESOLVED_COMPLETION_STATUSES and not bool(
        row.get("is_final")
    ):
        classification = UNRESOLVED
        reason = f"CURRENT_COMPLETION_STATUS_{completion_status}"
    elif completion_status == "UNKNOWN":
        reason = "UNKNOWN_COMPLETION_STATUS"
    elif completion_status not in {"SETTLED", "CANCELLED"}:
        reason = "UNSUPPORTED_COMPLETION_STATUS"
    else:
        expected = {1: "YES", 2: "NO", 3: "CANCELLED"}.get(settlement_code)
        lifecycle_valid = bool(row.get("has_settle")) and bool(row.get("is_resolved")) and bool(
            row.get("is_final")
        )
        event_identity_valid = (
            settlement_event_id is not None
            and settlement_block is not None
            and settlement_block > 0
            and event_status == "settle"
            and event_condition == condition_id
            and settlement_tx is not None
            and settlement_tx == event_tx
            and expected == settlement_outcome
            and (reported_event_time is None or source_event_time is None or reported_event_time == source_event_time)
        )
        event_provenance_valid = (
            _normalized_address(row.get("source_adapter")) is not None
            and _normalized_address(row.get("source_oracle")) is not None
        )
        header = (
            None
            if settlement_block is None
            else _trusted_header(trusted_header_rows, expected_block=settlement_block)
        )
        settlement_is_pre_cutoff: bool | None = None
        if not lifecycle_valid:
            reason = "FINAL_SETTLEMENT_LIFECYCLE_INCOMPLETE"
        elif not event_identity_valid:
            reason = "ORACLE_SETTLEMENT_EVENT_IDENTITY_MISSING_OR_CONFLICTING"
        elif header is not None:
            (
                protocol_finalized_at,
                exact_header_hash,
                exact_header_sources,
                exact_header_rows_hash,
            ) = header
            if reported_event_time is not None and reported_event_time != protocol_finalized_at:
                reason = "REPORTED_SETTLEMENT_TIME_DIFFERS_FROM_EXACT_BLOCK_HEADER"
            elif source_event_time is not None and source_event_time != protocol_finalized_at:
                reason = "ORACLE_EVENT_TIME_DIFFERS_FROM_EXACT_BLOCK_HEADER"
            else:
                evidence_grade = EXACT_CANONICAL_BLOCK_HEADER_GRADE
                settlement_is_pre_cutoff = protocol_finalized_at <= cutoff
        elif evidence_policy == EXACT_HEADER_REQUIRED:
            reason = "TRUSTED_EXACT_BLOCK_HEADER_MISSING_OR_CONFLICTING"
        elif not event_provenance_valid:
            reason = "ORACLE_EVENT_SOURCE_PROVENANCE_MISSING_OR_CONFLICTING"
        else:
            assert boundary is not None
            assert settlement_block is not None
            before_block, _ = boundary
            settlement_is_pre_cutoff = settlement_block <= before_block
            if source_event_time is not None:
                protocol_finalized_at = source_event_time
                settlement_time_source = "oracle.oracle_events.event_time"
            elif reported_event_time is not None:
                protocol_finalized_at = reported_event_time
                settlement_time_source = (
                    "core.market_status_snapshot.settlement_event_time"
                )
            elif evidence_policy == ORACLE_FINALIZED_REQUIRED:
                credible_time = _credible_block_time(
                    trusted_header_rows, expected_block=settlement_block
                )
                if credible_time is not None:
                    (
                        protocol_finalized_at,
                        credible_time_sources,
                        credible_time_rows_hash,
                    ) = credible_time
                    settlement_time_source = "block_timestamps"
            if evidence_policy == ORACLE_FINALIZED_REQUIRED and protocol_finalized_at is None:
                settlement_is_pre_cutoff = None
                reason = "CREDIBLE_SETTLEMENT_TIME_MISSING_OR_CONFLICTING"
            elif protocol_finalized_at is not None and (
                (protocol_finalized_at <= cutoff) != settlement_is_pre_cutoff
            ):
                settlement_is_pre_cutoff = None
                protocol_finalized_at = None
                reason = "ORACLE_EVENT_TIME_CONFLICTS_WITH_CANONICAL_CUTOFF_BLOCK"
            else:
                evidence_grade = (
                    ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE
                    if evidence_policy == ORACLE_FINALIZED_REQUIRED
                    else ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE
                )

        if settlement_is_pre_cutoff is False:
            classification = POST_CUTOFF
            reason = "PROTOCOL_FINALIZATION_AFTER_FROZEN_CUTOFF"
        elif settlement_is_pre_cutoff is True:
            if settlement_code == 1:
                classification = SETTLED_YES
                reason = "PROTOCOL_YES_PAYOUT"
                payout_by_token = {
                    token_outcomes["YES"]: Decimal(1),
                    token_outcomes["NO"]: Decimal(0),
                }
            elif settlement_code == 2:
                classification = SETTLED_NO
                reason = "PROTOCOL_NO_PAYOUT"
                payout_by_token = {
                    token_outcomes["YES"]: Decimal(0),
                    token_outcomes["NO"]: Decimal(1),
                }
            elif settlement_code == 3 and str(
                row.get("settled_price") or ""
            ).strip() in {
                "0.5",
                "0.50",
                "0.500000000000000000",
            }:
                classification = CANCELLED_REFUND
                reason = "PROTOCOL_HALF_REFUND_PAYOUT"
                payout_by_token = {
                    token_outcomes["YES"]: Decimal("0.5"),
                    token_outcomes["NO"]: Decimal("0.5"),
                }
            else:
                reason = "SETTLEMENT_CODE_OR_PAYOUT_VECTOR_UNSUPPORTED"

    evidence = {
        "market_found": market_found,
        "status_found": status_found,
        "token_map_valid": token_map_valid,
        "token_mapping_source": row.get("token_mapping_source"),
        "oracle_event_found": bool(row.get("oracle_event_found")),
        "settlement_event_time": reported_event_time,
        "source_event_time": source_event_time,
        "exact_block_hash": exact_header_hash,
        "exact_block_sources": exact_header_sources,
        "exact_block_rows_sha256": exact_header_rows_hash,
        "credible_time_sources": credible_time_sources,
        "credible_time_rows_sha256": credible_time_rows_hash,
        "settlement_time_source": settlement_time_source,
        "evidence_policy": evidence_policy,
        "evidence_grade": evidence_grade,
        "cutoff_block_boundary_sha256": (
            None
            if cutoff_block_boundary is None
            else cutoff_block_boundary.get("boundary_sha256")
        ),
        "source_adapter": row.get("source_adapter"),
        "source_oracle": row.get("source_oracle"),
        "settled_price": row.get("settled_price"),
        "registry_updated_at": row.get("registry_updated_at"),
    }
    payload_without_hash = {
        "schema_version": RECORD_SCHEMA_VERSION,
        "market_id": market_id,
        "condition_id": condition_id,
        "slug": row.get("slug"),
        "title": row.get("title"),
        "category": row.get("category"),
        "cutoff_ts": cutoff,
        "classification": classification,
        "classification_reason": reason,
        "completion_status": completion_status,
        "settlement_code": settlement_code,
        "settlement_outcome": settlement_outcome,
        "settlement_source": row.get("settlement_source"),
        "protocol_finalized_at": protocol_finalized_at,
        "protocol_finalized_block": settlement_block,
        "settlement_event_id": settlement_event_id,
        "settlement_tx_hash": event_tx,
        "tokens": tokens,
        "payout_by_token": payout_by_token,
        "evidence": evidence,
    }
    return MarketSettlementRecord(
        market_id=market_id,
        condition_id=condition_id,
        slug=None if row.get("slug") is None else str(row.get("slug")),
        title=None if row.get("title") is None else str(row.get("title")),
        category=None if row.get("category") is None else str(row.get("category")),
        cutoff_ts=cutoff,
        classification=classification,
        classification_reason=reason,
        completion_status=completion_status,
        settlement_code=settlement_code,
        settlement_outcome=settlement_outcome,
        settlement_source=(
            None if row.get("settlement_source") is None else str(row.get("settlement_source"))
        ),
        protocol_finalized_at=protocol_finalized_at,
        protocol_finalized_block=settlement_block,
        settlement_event_id=settlement_event_id,
        settlement_tx_hash=event_tx,
        tokens=tokens,
        payout_by_token=payout_by_token,
        evidence=evidence,
        record_sha256=_canonical_sha256(payload_without_hash),
    )


def _chunked(values: Sequence[int], size: int) -> Iterator[tuple[int, ...]]:
    if size <= 0:
        raise ValueError("chunk size must be positive")
    for offset in range(0, len(values), size):
        yield tuple(values[offset : offset + size])


def _settlement_rows(conn: Any, market_ids: Sequence[int]) -> list[dict[str, object]]:
    sql_path = Path(__file__).with_name("sql") / "market_settlement_catalog_batch_v1.sql"
    query = sql_path.read_text(encoding="utf-8")
    with conn.cursor() as cursor:
        market_array = list(market_ids)
        cursor.execute(query, (market_array, market_array))
        return [dict(row) for row in cursor.fetchall()]


def _trusted_header_rows(
    clickhouse: ClickHouseClient, block_numbers: Sequence[int]
) -> dict[int, list[dict[str, object]]]:
    blocks = tuple(sorted(set(block_numbers)))
    if not blocks:
        return {}
    rendered = ",".join(str(item) for item in blocks)
    query = f"""
        SELECT
            toUInt64(block_number) AS block_number,
            lower(block_hash) AS block_hash,
            formatDateTime(toTimeZone(block_time, 'UTC'), '%Y-%m-%dT%H:%i:%SZ', 'UTC') AS block_time,
            lower(source) AS source
        FROM block_timestamps
        WHERE block_number IN ({rendered})
        ORDER BY block_number, source, block_time, block_hash
    """
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for row in clickhouse.query_json_rows(query, timeout_seconds=120):
        block = _optional_int(row.get("block_number"))
        if block is not None:
            grouped[block].append(dict(row))
    return grouped


def _rpc_header_rows_once(
    rpc_url: str,
    block_numbers: Sequence[int],
    *,
    timeout_seconds: float = 30,
) -> dict[int, list[dict[str, object]]]:
    blocks = tuple(sorted(set(block_numbers)))
    if not blocks:
        return {}
    payload = [
        {
            "jsonrpc": "2.0",
            "id": block,
            "method": "eth_getBlockByNumber",
            "params": [hex(block), False],
        }
        for block in blocks
    ]
    request = Request(
        rpc_url,
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        raw = json.loads(response.read().decode("utf-8"))
    if not isinstance(raw, list):
        raise MarketSettlementError("Polygon RPC batch response must be a list")
    requested = set(blocks)
    grouped: dict[int, list[dict[str, object]]] = defaultdict(list)
    for item in raw:
        if not isinstance(item, Mapping) or item.get("error") is not None:
            continue
        result = item.get("result")
        if not isinstance(result, Mapping):
            continue
        try:
            block = int(str(result["number"]), 16)
            timestamp = datetime.fromtimestamp(int(str(result["timestamp"]), 16), tz=UTC)
        except (KeyError, TypeError, ValueError, OSError):
            continue
        block_hash = _normalized_hash(result.get("hash"))
        if block not in requested or block_hash is None:
            continue
        grouped[block].append(
            {
                "block_number": block,
                "block_hash": block_hash,
                "block_time": timestamp,
                "source": "polygon_rpc_json_rpc",
            }
        )
    return grouped


def _rpc_header_rows(
    rpc_url: str,
    block_numbers: Sequence[int],
    *,
    timeout_seconds: float = 30,
    retry_count: int = 3,
) -> dict[int, list[dict[str, object]]]:
    """Fetch canonical headers and recursively split oversized RPC batches."""

    blocks = tuple(sorted(set(block_numbers)))
    if not blocks:
        return {}
    last_error: Exception | None = None
    for attempt in range(max(retry_count, 1)):
        try:
            return _rpc_header_rows_once(
                rpc_url, blocks, timeout_seconds=timeout_seconds
            )
        except (
            HTTPError,
            URLError,
            TimeoutError,
            IncompleteRead,
            json.JSONDecodeError,
            OSError,
        ) as exc:
            last_error = exc
            if attempt + 1 < max(retry_count, 1):
                time.sleep(min(0.25 * (2**attempt), 2.0))
    if len(blocks) == 1:
        return {}
    midpoint = len(blocks) // 2
    left = _rpc_header_rows(
        rpc_url,
        blocks[:midpoint],
        timeout_seconds=timeout_seconds,
        retry_count=retry_count,
    )
    right = _rpc_header_rows(
        rpc_url,
        blocks[midpoint:],
        timeout_seconds=timeout_seconds,
        retry_count=retry_count,
    )
    if not left and not right and last_error is not None:
        return {}
    return {**left, **right}


def _rpc_latest_block_number(
    rpc_url: str, *, timeout_seconds: float = 30, retry_count: int = 3
) -> int:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_blockNumber",
        "params": [],
    }
    request = Request(
        rpc_url,
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    for attempt in range(max(retry_count, 1)):
        try:
            with urlopen(request, timeout=timeout_seconds) as response:
                raw = json.loads(response.read().decode("utf-8"))
            if not isinstance(raw, Mapping) or raw.get("error") is not None:
                raise MarketSettlementError("Polygon RPC block-number response is invalid")
            value = int(str(raw["result"]), 16)
            if value <= 0:
                raise MarketSettlementError("Polygon RPC latest block is invalid")
            return value
        except (
            HTTPError,
            URLError,
            TimeoutError,
            IncompleteRead,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
            OSError,
        ):
            if attempt + 1 < max(retry_count, 1):
                time.sleep(min(0.25 * (2**attempt), 2.0))
    raise MarketSettlementError("Polygon RPC latest block could not be resolved")


def _rpc_single_header(
    rpc_url: str, block_number: int, *, retry_count: int = 3
) -> dict[str, object]:
    rows = _rpc_header_rows(
        rpc_url, (block_number,), retry_count=retry_count
    ).get(block_number, ())
    header = _trusted_header(rows, expected_block=block_number)
    if header is None:
        raise MarketSettlementError(
            f"Polygon RPC header unavailable for block {block_number}"
        )
    block_time, block_hash, sources, rows_hash = header
    return {
        "block_number": block_number,
        "block_time": block_time,
        "block_hash": block_hash,
        "sources": sources,
        "header_rows_sha256": rows_hash,
    }


def _cutoff_boundary_payload(
    *,
    cutoff: datetime,
    at_or_before: Mapping[str, object],
    after: Mapping[str, object],
) -> dict[str, object]:
    before_time = _utc(at_or_before["block_time"], field="cutoff boundary time")
    after_time = _utc(after["block_time"], field="cutoff boundary next time")
    before_block = int(str(at_or_before["block_number"]))
    after_block = int(str(after["block_number"]))
    if not before_time <= cutoff < after_time or after_block != before_block + 1:
        raise MarketSettlementError("Polygon cutoff boundary does not bracket cutoff")
    boundary_without_hash = {
        "schema_version": CUTOFF_BOUNDARY_SCHEMA_VERSION,
        "cutoff_ts": cutoff,
        "at_or_before": dict(at_or_before),
        "after": dict(after),
    }
    return {
        **boundary_without_hash,
        "boundary_sha256": _canonical_sha256(boundary_without_hash),
    }


def _clickhouse_cutoff_boundary(
    clickhouse: ClickHouseClient, *, cutoff: datetime
) -> dict[str, object] | None:
    rendered = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    queries = (
        f"""
        SELECT toUInt64(block_number) AS block_number
        FROM block_timestamps
        WHERE block_time <= toDateTime('{rendered}', 'UTC')
          AND (startsWith(lower(source), 'rpc') OR startsWith(lower(source), 'polygon_rpc'))
          AND notEmpty(block_hash)
        ORDER BY block_time DESC, block_number DESC
        LIMIT 1
        """,
        f"""
        SELECT toUInt64(block_number) AS block_number
        FROM block_timestamps
        WHERE block_time > toDateTime('{rendered}', 'UTC')
          AND (startsWith(lower(source), 'rpc') OR startsWith(lower(source), 'polygon_rpc'))
          AND notEmpty(block_hash)
        ORDER BY block_time ASC, block_number ASC
        LIMIT 1
        """,
    )
    candidate_rows = [
        clickhouse.query_json_rows(query, timeout_seconds=120) for query in queries
    ]
    if any(len(rows) != 1 for rows in candidate_rows):
        return None
    before_block = int(candidate_rows[0][0]["block_number"])
    after_block = int(candidate_rows[1][0]["block_number"])
    if after_block != before_block + 1:
        return None
    grouped = _trusted_header_rows(clickhouse, (before_block, after_block))
    rendered_headers: list[dict[str, object]] = []
    for block in (before_block, after_block):
        header = _trusted_header(grouped.get(block, ()), expected_block=block)
        if header is None:
            return None
        block_time, block_hash, sources, rows_hash = header
        rendered_headers.append(
            {
                "block_number": block,
                "block_time": block_time,
                "block_hash": block_hash,
                "sources": sources,
                "header_rows_sha256": rows_hash,
            }
        )
    return _cutoff_boundary_payload(
        cutoff=cutoff,
        at_or_before=rendered_headers[0],
        after=rendered_headers[1],
    )


def resolve_polygon_cutoff_boundary(
    rpc_url: str,
    *,
    cutoff_ts: datetime,
    retry_count: int = 3,
    clickhouse: ClickHouseClient | None = None,
) -> dict[str, object]:
    """Freeze adjacent canonical blocks which bracket an as-of cutoff."""

    cutoff = _utc(cutoff_ts, field="settlement cutoff")
    if clickhouse is not None:
        cached = _clickhouse_cutoff_boundary(clickhouse, cutoff=cutoff)
        if cached is not None:
            return cached
    if not rpc_url.strip():
        raise MarketSettlementError("Polygon RPC is required for cutoff boundary")
    latest = _rpc_latest_block_number(rpc_url, retry_count=retry_count)
    latest_header = _rpc_single_header(rpc_url, latest, retry_count=retry_count)
    if _utc(latest_header["block_time"], field="latest block time") <= cutoff:
        raise MarketSettlementError("settlement cutoff is not behind the chain head")

    low = 0
    high = latest
    while low + 1 < high:
        midpoint = (low + high) // 2
        header = _rpc_single_header(rpc_url, midpoint, retry_count=retry_count)
        if _utc(header["block_time"], field="boundary probe time") <= cutoff:
            low = midpoint
        else:
            high = midpoint
    at_or_before = _rpc_single_header(rpc_url, low, retry_count=retry_count)
    after = _rpc_single_header(rpc_url, high, retry_count=retry_count)
    return _cutoff_boundary_payload(
        cutoff=cutoff, at_or_before=at_or_before, after=after
    )


def _validated_cutoff_boundary(
    value: Mapping[str, object] | None, *, cutoff_ts: datetime
) -> tuple[int, int] | None:
    if value is None:
        return None
    if value.get("schema_version") != CUTOFF_BOUNDARY_SCHEMA_VERSION:
        raise MarketSettlementError("unsupported Polygon cutoff boundary schema")
    stored_hash = value.get("boundary_sha256")
    if _canonical_sha256(
        {key: item for key, item in value.items() if key != "boundary_sha256"}
    ) != stored_hash:
        raise MarketSettlementError("Polygon cutoff boundary hash drifted")
    cutoff = _utc(value.get("cutoff_ts"), field="boundary cutoff")
    if cutoff != _utc(cutoff_ts, field="settlement cutoff"):
        raise MarketSettlementError("Polygon boundary cutoff differs from catalog cutoff")
    before = value.get("at_or_before")
    after = value.get("after")
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        raise MarketSettlementError("Polygon cutoff boundary rows are missing")
    before_block = _optional_int(before.get("block_number"))
    after_block = _optional_int(after.get("block_number"))
    before_hash = _normalized_hash(before.get("block_hash"))
    after_hash = _normalized_hash(after.get("block_hash"))
    before_time = _utc(before.get("block_time"), field="boundary before time")
    after_time = _utc(after.get("block_time"), field="boundary after time")
    if (
        before_block is None
        or after_block != before_block + 1
        or before_hash is None
        or after_hash is None
        or not before_time <= cutoff < after_time
    ):
        raise MarketSettlementError("Polygon cutoff boundary is inconsistent")
    return before_block, after_block


def iter_market_settlement_catalog(
    conn: Any,
    market_ids: Iterable[int],
    *,
    cutoff_ts: datetime,
    clickhouse: ClickHouseClient | None = None,
    market_chunk_size: int = 2_000,
    block_chunk_size: int = 5_000,
    polygon_rpc_url: str | None = None,
    rpc_batch_size: int = 100,
    rpc_workers: int = 4,
    rpc_retry_count: int = 3,
    evidence_policy: str = EXACT_HEADER_REQUIRED,
    cutoff_block_boundary: Mapping[str, object] | None = None,
    rpc_backfill_missing_headers: bool = True,
) -> Iterator[MarketSettlementRecord]:
    """Build records in bounded chunks for any market inventory."""

    normalized = tuple(sorted({int(item) for item in market_ids}))
    if any(item <= 0 for item in normalized):
        raise ValueError("market ids must be positive")
    source = clickhouse or ClickHouseClient()
    rpc_url = (
        polygon_rpc_url
        or os.environ.get("POLY_QUANT_SETTLEMENT_RPC_URL")
        or os.environ.get("POLYMARKET_POLYGON_RPC_URL")
        or os.environ.get("POLYGON_RPC_URL")
        or ""
    ).strip()
    for market_chunk in _chunked(normalized, market_chunk_size):
        rows = _settlement_rows(conn, market_chunk)
        if {int(row["requested_market_id"]) for row in rows} != set(market_chunk):
            raise MarketSettlementError("settlement query did not preserve requested inventory")
        blocks = tuple(
            sorted(
                {
                    block
                    for row in rows
                    if (block := _optional_int(row.get("settlement_block_number")))
                    is not None
                    and block > 0
                }
            )
        )
        headers: dict[int, list[dict[str, object]]] = defaultdict(list)
        for block_chunk in _chunked(blocks, block_chunk_size):
            for block, block_rows in _trusted_header_rows(source, block_chunk).items():
                headers[block].extend(block_rows)
        missing_blocks = tuple(
            block
            for block in blocks
            if _trusted_header(headers.get(block, ()), expected_block=block) is None
        )
        if rpc_url and missing_blocks and rpc_backfill_missing_headers:
            rpc_chunks = tuple(_chunked(missing_blocks, rpc_batch_size))

            def fetch_rpc_chunk(
                rpc_chunk: tuple[int, ...],
            ) -> dict[int, list[dict[str, object]]]:
                return _rpc_header_rows(
                    rpc_url,
                    rpc_chunk,
                    retry_count=rpc_retry_count,
                )

            if rpc_workers <= 1 or len(rpc_chunks) == 1:
                rpc_results = map(fetch_rpc_chunk, rpc_chunks)
            else:
                executor = ThreadPoolExecutor(max_workers=rpc_workers)
                rpc_results = executor.map(fetch_rpc_chunk, rpc_chunks)
            try:
                for rpc_result in rpc_results:
                    for block, block_rows in rpc_result.items():
                        headers[block].extend(block_rows)
            finally:
                if rpc_workers > 1 and len(rpc_chunks) > 1:
                    executor.shutdown(wait=True)
        for row in rows:
            block = _optional_int(row.get("settlement_block_number"))
            yield classify_market_settlement(
                row,
                cutoff_ts=cutoff_ts,
                trusted_header_rows=() if block is None else headers.get(block, ()),
                evidence_policy=evidence_policy,
                cutoff_block_boundary=cutoff_block_boundary,
            )


def _record_storage_row(record: MarketSettlementRecord) -> dict[str, object]:
    value = dict(record.as_dict())
    value["tokens_json"] = _canonical_json(value.pop("tokens"))
    value["payout_by_token_json"] = _canonical_json(value.pop("payout_by_token"))
    value["evidence_json"] = _canonical_json(value.pop("evidence"))
    return dict(_json_value(value))


def _record_from_storage_row(raw: Mapping[str, object]) -> MarketSettlementRecord:
    row = dict(raw)
    tokens = tuple(json.loads(str(row.pop("tokens_json"))))
    payout_raw = json.loads(str(row.pop("payout_by_token_json")))
    evidence = json.loads(str(row.pop("evidence_json")))
    payout = (
        None
        if payout_raw is None
        else {str(key): Decimal(str(value)) for key, value in payout_raw.items()}
    )
    record = MarketSettlementRecord(
        market_id=int(row["market_id"]),
        condition_id=(
            None if row.get("condition_id") is None else str(row["condition_id"])
        ),
        slug=None if row.get("slug") is None else str(row["slug"]),
        title=None if row.get("title") is None else str(row["title"]),
        category=None if row.get("category") is None else str(row["category"]),
        cutoff_ts=_utc(row["cutoff_ts"], field="catalog cutoff"),
        classification=str(row["classification"]),
        classification_reason=str(row["classification_reason"]),
        completion_status=(
            None
            if row.get("completion_status") is None
            else str(row["completion_status"])
        ),
        settlement_code=_optional_int(row.get("settlement_code")),
        settlement_outcome=(
            None
            if row.get("settlement_outcome") is None
            else str(row["settlement_outcome"])
        ),
        settlement_source=(
            None
            if row.get("settlement_source") is None
            else str(row["settlement_source"])
        ),
        protocol_finalized_at=_optional_utc(
            row.get("protocol_finalized_at"), field="protocol finalized at"
        ),
        protocol_finalized_block=_optional_int(row.get("protocol_finalized_block")),
        settlement_event_id=_optional_int(row.get("settlement_event_id")),
        settlement_tx_hash=(
            None
            if row.get("settlement_tx_hash") is None
            else str(row["settlement_tx_hash"])
        ),
        tokens=tokens,
        payout_by_token=payout,
        evidence=evidence,
        record_sha256=str(row["record_sha256"]),
    )
    expected = dict(record.as_dict())
    stored_hash = str(expected.pop("record_sha256"))
    if _canonical_sha256(expected) != stored_hash:
        raise MarketSettlementError(f"market {record.market_id} record hash drifted")
    return record


def write_market_settlement_catalog(
    records: Iterable[MarketSettlementRecord],
    *,
    output_dir: Path,
    cutoff_ts: datetime,
    source_scope: Mapping[str, object],
) -> dict[str, object]:
    """Write a reusable Parquet catalog plus a hash-bound manifest."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    output = output_dir.resolve()
    if output.exists():
        raise FileExistsError(f"refusing to reuse settlement catalog: {output}")
    output.mkdir(parents=True)
    parquet_path = output / "market_settlements.parquet"
    writer: pq.ParquetWriter | None = None
    counts: Counter[str] = Counter()
    record_hash_chain = "0" * 64
    batch: list[MarketSettlementRecord] = []
    total = 0

    def flush() -> None:
        nonlocal writer
        if not batch:
            return
        rows = [_record_storage_row(item) for item in batch]
        table = pa.Table.from_pylist(rows)
        if writer is None:
            writer = pq.ParquetWriter(
                parquet_path,
                table.schema,
                compression="zstd",
                use_dictionary=True,
            )
        writer.write_table(table, row_group_size=len(rows))
        batch.clear()

    try:
        for record in records:
            counts[record.classification] += 1
            total += 1
            record_hash_chain = sha256(
                bytes.fromhex(record_hash_chain) + bytes.fromhex(record.record_sha256)
            ).hexdigest()
            batch.append(record)
            if len(batch) >= 10_000:
                flush()
        flush()
    finally:
        if writer is not None:
            writer.close()

    if total == 0:
        empty = pa.table(
            {
                "schema_version": pa.array([], type=pa.string()),
                "market_id": pa.array([], type=pa.int64()),
                "record_sha256": pa.array([], type=pa.string()),
            }
        )
        pq.write_table(empty, parquet_path, compression="zstd")

    file_sha = file_sha256(parquet_path)
    manifest = {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "cutoff_ts": _utc(cutoff_ts, field="settlement cutoff"),
        "record_count": total,
        "classification_counts": dict(sorted(counts.items())),
        "source_scope": dict(source_scope),
        "record_hash_chain_sha256": record_hash_chain,
        "parquet_file": parquet_path.name,
        "parquet_file_sha256": file_sha,
    }
    manifest["catalog_sha256"] = _canonical_sha256(manifest)
    _write_json_atomic(output / "manifest.json", manifest)
    return _json_value(manifest)  # type: ignore[return-value]


def initialize_partitioned_market_settlement_catalog(
    *,
    output_dir: Path,
    cutoff_ts: datetime,
    source_scope: Mapping[str, object],
    partition_size: int,
    resume: bool,
) -> dict[str, object]:
    """Create or verify the immutable contract for a resumable catalog."""

    if partition_size <= 0:
        raise ValueError("partition_size must be positive")
    output = output_dir.resolve()
    contract_without_hash = {
        "schema_version": CATALOG_BUILD_CONTRACT_SCHEMA_VERSION,
        "cutoff_ts": _utc(cutoff_ts, field="settlement cutoff"),
        "source_scope": dict(source_scope),
        "partition_size": partition_size,
    }
    contract = {
        **contract_without_hash,
        "build_contract_sha256": _canonical_sha256(contract_without_hash),
    }
    contract_path = output / "build_contract.json"
    if output.exists():
        if not resume:
            raise FileExistsError(f"refusing to reuse settlement catalog: {output}")
        if not contract_path.is_file():
            raise MarketSettlementError(
                "resumed settlement catalog lacks build_contract.json"
            )
        existing = json.loads(contract_path.read_text(encoding="utf-8"))
        if existing != _json_value(contract):
            raise MarketSettlementError("settlement catalog build contract drifted")
    else:
        output.mkdir(parents=True)
        (output / "parts").mkdir()
        _write_json_atomic(contract_path, contract)
    (output / "parts").mkdir(exist_ok=True)
    return _json_value(contract)  # type: ignore[return-value]


def _load_catalog_part_manifest(
    root: Path,
    manifest_path: Path,
    *,
    expected_contract_sha256: str,
    verify_parquet: bool,
) -> dict[str, object]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != CATALOG_PART_SCHEMA_VERSION:
        raise MarketSettlementError("unsupported settlement catalog part schema")
    stored_hash = manifest.get("part_manifest_sha256")
    if _canonical_sha256(
        {key: value for key, value in manifest.items() if key != "part_manifest_sha256"}
    ) != stored_hash:
        raise MarketSettlementError("settlement catalog part manifest drifted")
    if manifest.get("build_contract_sha256") != expected_contract_sha256:
        raise MarketSettlementError("settlement catalog part contract drifted")
    parquet_path = root / str(manifest["parquet_file"])
    if not parquet_path.is_file():
        raise MarketSettlementError("settlement catalog part Parquet is missing")
    if verify_parquet and file_sha256(parquet_path) != manifest.get(
        "parquet_file_sha256"
    ):
        raise MarketSettlementError("settlement catalog part Parquet hash drifted")
    return manifest


def write_market_settlement_catalog_part(
    records: Iterable[MarketSettlementRecord],
    *,
    output_dir: Path,
    part_index: int,
    expected_market_ids: Sequence[int],
    build_contract_sha256: str,
    resume: bool,
) -> dict[str, object]:
    """Atomically write one deterministic market-id partition."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    if part_index < 0:
        raise ValueError("part_index cannot be negative")
    expected = tuple(int(item) for item in expected_market_ids)
    if not expected or expected != tuple(sorted(set(expected))):
        raise ValueError("partition market ids must be non-empty, sorted, and unique")
    root = output_dir.resolve()
    relative_parquet = Path("parts") / f"part-{part_index:06d}.parquet"
    relative_manifest = Path("parts") / f"part-{part_index:06d}.json"
    parquet_path = root / relative_parquet
    manifest_path = root / relative_manifest
    expected_ids_sha = _canonical_sha256(expected)
    if manifest_path.is_file():
        if not resume:
            raise FileExistsError(f"settlement catalog part already exists: {manifest_path}")
        existing = _load_catalog_part_manifest(
            root,
            manifest_path,
            expected_contract_sha256=build_contract_sha256,
            verify_parquet=True,
        )
        if (
            int(existing["part_index"]) != part_index
            or existing.get("market_ids_sha256") != expected_ids_sha
            or int(existing["record_count"]) != len(expected)
        ):
            raise MarketSettlementError("resumed settlement catalog partition drifted")
        return existing
    if parquet_path.exists():
        if not resume:
            raise FileExistsError(f"orphan settlement catalog part exists: {parquet_path}")
        parquet_path.unlink()

    materialized = tuple(records)
    actual_ids = tuple(item.market_id for item in materialized)
    if actual_ids != expected:
        raise MarketSettlementError("settlement records differ from partition inventory")
    table = pa.Table.from_pylist([_record_storage_row(item) for item in materialized])
    temporary = parquet_path.with_suffix(".parquet.tmp")
    pq.write_table(
        table,
        temporary,
        compression="zstd",
        use_dictionary=True,
        row_group_size=min(10_000, len(materialized)),
    )
    parquet_hash = file_sha256(temporary)
    temporary.replace(parquet_path)

    counts = Counter(item.classification for item in materialized)
    record_hash_chain = "0" * 64
    for record in materialized:
        record_hash_chain = sha256(
            bytes.fromhex(record_hash_chain) + bytes.fromhex(record.record_sha256)
        ).hexdigest()
    manifest_without_hash = {
        "schema_version": CATALOG_PART_SCHEMA_VERSION,
        "build_contract_sha256": build_contract_sha256,
        "part_index": part_index,
        "first_market_id": expected[0],
        "last_market_id": expected[-1],
        "record_count": len(materialized),
        "market_ids_sha256": expected_ids_sha,
        "classification_counts": dict(sorted(counts.items())),
        "record_hash_chain_sha256": record_hash_chain,
        "parquet_file": str(relative_parquet),
        "parquet_file_sha256": parquet_hash,
    }
    manifest = {
        **manifest_without_hash,
        "part_manifest_sha256": _canonical_sha256(manifest_without_hash),
    }
    _write_json_atomic(manifest_path, manifest)
    return _json_value(manifest)  # type: ignore[return-value]


def finalize_partitioned_market_settlement_catalog(
    *,
    output_dir: Path,
    expected_part_count: int,
    expected_record_count: int,
) -> dict[str, object]:
    """Seal all completed partitions into one reusable catalog manifest."""

    if expected_part_count <= 0 or expected_record_count <= 0:
        raise ValueError("completed catalog requires positive part and record counts")
    root = output_dir.resolve()
    contract = json.loads((root / "build_contract.json").read_text(encoding="utf-8"))
    contract_hash = str(contract["build_contract_sha256"])
    part_manifests: list[dict[str, object]] = []
    counts: Counter[str] = Counter()
    previous_last: int | None = None
    total = 0
    part_chain = "0" * 64
    for part_index in range(expected_part_count):
        part_path = root / "parts" / f"part-{part_index:06d}.json"
        if not part_path.is_file():
            raise MarketSettlementError(f"settlement catalog part {part_index} is missing")
        part = _load_catalog_part_manifest(
            root,
            part_path,
            expected_contract_sha256=contract_hash,
            verify_parquet=True,
        )
        if int(part["part_index"]) != part_index:
            raise MarketSettlementError("settlement catalog part index drifted")
        first_market = int(part["first_market_id"])
        last_market = int(part["last_market_id"])
        if previous_last is not None and first_market <= previous_last:
            raise MarketSettlementError("settlement catalog market partitions overlap")
        previous_last = last_market
        total += int(part["record_count"])
        counts.update(
            {
                str(key): int(value)
                for key, value in dict(part["classification_counts"]).items()
            }
        )
        part_chain = sha256(
            bytes.fromhex(part_chain)
            + bytes.fromhex(str(part["part_manifest_sha256"]))
        ).hexdigest()
        part_manifests.append(
            {
                "part_index": part_index,
                "first_market_id": first_market,
                "last_market_id": last_market,
                "record_count": int(part["record_count"]),
                "manifest_file": str(Path("parts") / f"part-{part_index:06d}.json"),
                "part_manifest_sha256": part["part_manifest_sha256"],
                "parquet_file": part["parquet_file"],
                "parquet_file_sha256": part["parquet_file_sha256"],
            }
        )
    if total != expected_record_count:
        raise MarketSettlementError("settlement catalog completed record count drifted")
    manifest_without_hash = {
        "schema_version": PARTITIONED_CATALOG_SCHEMA_VERSION,
        "status": "COMPLETE",
        "cutoff_ts": contract["cutoff_ts"],
        "source_scope": contract["source_scope"],
        "build_contract_sha256": contract_hash,
        "partition_size": contract["partition_size"],
        "part_count": len(part_manifests),
        "record_count": total,
        "classification_counts": dict(sorted(counts.items())),
        "part_manifest_hash_chain_sha256": part_chain,
        "parts": part_manifests,
    }
    manifest = {
        **manifest_without_hash,
        "catalog_sha256": _canonical_sha256(manifest_without_hash),
    }
    _write_json_atomic(root / "manifest.json", manifest)
    return _json_value(manifest)  # type: ignore[return-value]


def load_market_settlement_catalog(
    path: Path,
    *,
    market_ids: Iterable[int] | None = None,
    verify_all_files: bool = True,
) -> dict[int, MarketSettlementRecord]:
    """Load a whole catalog or only the markets used by one replay."""

    import pyarrow.parquet as pq

    root = path.resolve()
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    selected = None if market_ids is None else {int(item) for item in market_ids}
    selected_ordered = () if selected is None else tuple(sorted(selected))
    if selected is not None and any(item <= 0 for item in selected):
        raise ValueError("market ids must be positive")
    records: dict[int, MarketSettlementRecord] = {}

    parquet_paths: list[Path] = []
    if manifest.get("schema_version") == CATALOG_SCHEMA_VERSION:
        parquet_path = root / str(manifest["parquet_file"])
        if file_sha256(parquet_path) != manifest["parquet_file_sha256"]:
            raise MarketSettlementError("settlement catalog Parquet hash drifted")
        if _canonical_sha256(
            {key: value for key, value in manifest.items() if key != "catalog_sha256"}
        ) != manifest["catalog_sha256"]:
            raise MarketSettlementError("settlement catalog manifest hash drifted")
        parquet_paths.append(parquet_path)
    elif manifest.get("schema_version") == PARTITIONED_CATALOG_SCHEMA_VERSION:
        if _canonical_sha256(
            {key: value for key, value in manifest.items() if key != "catalog_sha256"}
        ) != manifest.get("catalog_sha256"):
            raise MarketSettlementError("partitioned settlement catalog manifest drifted")
        contract_hash = str(manifest["build_contract_sha256"])
        for summary in manifest["parts"]:
            first_market = int(summary["first_market_id"])
            last_market = int(summary["last_market_id"])
            selected_offset = bisect_left(selected_ordered, first_market)
            relevant = selected is None or (
                selected_offset < len(selected_ordered)
                and selected_ordered[selected_offset] <= last_market
            )
            part_manifest_path = root / str(summary["manifest_file"])
            part = _load_catalog_part_manifest(
                root,
                part_manifest_path,
                expected_contract_sha256=contract_hash,
                verify_parquet=verify_all_files or relevant,
            )
            if part.get("part_manifest_sha256") != summary.get(
                "part_manifest_sha256"
            ):
                raise MarketSettlementError("catalog root and part manifest disagree")
            if relevant:
                parquet_paths.append(root / str(part["parquet_file"]))
    else:
        raise MarketSettlementError("unsupported settlement catalog schema")

    for parquet_path in parquet_paths:
        parquet = pq.ParquetFile(parquet_path)
        for batch in parquet.iter_batches(batch_size=10_000):
            for row in batch.to_pylist():
                market_id = int(row["market_id"])
                if selected is not None and market_id not in selected:
                    continue
                record = _record_from_storage_row(row)
                if record.market_id in records:
                    raise MarketSettlementError(
                        "settlement catalog contains duplicate markets"
                    )
                records[record.market_id] = record
    if selected is None:
        if len(records) != int(manifest["record_count"]):
            raise MarketSettlementError("settlement catalog count drifted")
    elif set(records) != selected:
        missing = sorted(selected - set(records))
        raise MarketSettlementError(
            f"settlement catalog is missing requested markets: {missing[:10]}"
        )
    return records
