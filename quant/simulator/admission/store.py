"""Durable idempotent admission evidence."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime
from decimal import Decimal
from typing import Any

from quant.core.db import postgres_connection

from .domain import (
    AdmissionDecision,
    AdmissionOperation,
    AdmissionRequest,
    AdmissionStatus,
    ExposureEffect,
    GeoblockSnapshot,
    JurisdictionMode,
)
from .policy import JurisdictionPolicy


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS quant.simulator_admission_policies (
        policy_version TEXT PRIMARY KEY,
        rules_hash TEXT NOT NULL CHECK (length(rules_hash)=64),
        rules JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.simulator_geoblock_snapshots (
        snapshot_id TEXT PRIMARY KEY,
        blocked BOOLEAN NOT NULL,
        country TEXT NOT NULL,
        region TEXT NOT NULL,
        detected_ip TEXT NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        raw_payload_hash TEXT NOT NULL CHECK (length(raw_payload_hash)=64),
        source TEXT NOT NULL,
        proxy_url TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.simulator_admission_decisions (
        decision_id TEXT PRIMARY KEY,
        request_id TEXT NOT NULL UNIQUE,
        request_hash TEXT NOT NULL CHECK (length(request_hash)=72),
        operation_type TEXT NOT NULL,
        account_id TEXT NOT NULL,
        strategy_id TEXT,
        asset_id TEXT,
        condition_id TEXT,
        market_id TEXT,
        exposure_effect TEXT NOT NULL,
        exposure_before NUMERIC,
        exposure_after NUMERIC,
        country TEXT,
        region TEXT,
        detected_ip TEXT,
        policy_version TEXT NOT NULL,
        geoblock_raw_payload_hash TEXT,
        geoblock_snapshot_id TEXT,
        admission_status TEXT NOT NULL,
        jurisdiction_mode TEXT NOT NULL,
        reason_codes JSONB NOT NULL,
        request_payload JSONB NOT NULL,
        decided_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS simulator_admission_decisions_account_time_idx
    ON quant.simulator_admission_decisions (account_id,decided_at DESC)
    """,
)


class PostgresAdmissionStore:
    def __init__(
        self,
        connection_factory: Callable[..., AbstractContextManager[Any]] = (
            postgres_connection
        ),
    ) -> None:
        self.connection_factory = connection_factory

    def ensure_schema(self) -> None:
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS quant")
            for statement in SCHEMA_STATEMENTS:
                cur.execute(statement)

    def record_policy(self, policy: JurisdictionPolicy) -> None:
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.simulator_admission_policies (
                    policy_version,rules_hash,rules
                ) VALUES (%s,%s,%s::jsonb)
                ON CONFLICT (policy_version) DO UPDATE SET
                    rules=EXCLUDED.rules
                WHERE quant.simulator_admission_policies.rules_hash=EXCLUDED.rules_hash
                RETURNING policy_version
                """,
                (policy.version, policy.rules_hash, json.dumps(policy.rules)),
            )
            if cur.fetchone() is None:
                raise ValueError("admission policy version conflicts with prior rules")

    def record_snapshot(self, snapshot: GeoblockSnapshot) -> None:
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.simulator_geoblock_snapshots (
                    snapshot_id,blocked,country,region,detected_ip,observed_at,
                    expires_at,raw_payload_hash,source,proxy_url
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (snapshot_id) DO NOTHING
                """,
                (
                    snapshot.snapshot_id,
                    snapshot.blocked,
                    snapshot.country,
                    snapshot.region,
                    snapshot.detected_ip,
                    snapshot.observed_at,
                    snapshot.expires_at,
                    snapshot.raw_payload_hash,
                    snapshot.source,
                    snapshot.proxy_url,
                ),
            )

    def decision(self, request_id: str) -> AdmissionDecision | None:
        with self.connection_factory(readonly=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT decision.*, snapshot.blocked,snapshot.observed_at AS geo_observed_at,
                       snapshot.expires_at AS geo_expires_at,snapshot.source AS geo_source,
                       snapshot.proxy_url
                FROM quant.simulator_admission_decisions decision
                LEFT JOIN quant.simulator_geoblock_snapshots snapshot
                  ON snapshot.snapshot_id=decision.geoblock_snapshot_id
                WHERE decision.request_id=%s
                """,
                (str(request_id),),
            )
            row = cur.fetchone()
        return _decision_from_row(dict(row)) if row is not None else None

    def record_decision(self, decision: AdmissionDecision) -> AdmissionDecision:
        current = self.decision(decision.request.request_id)
        if current is not None:
            if current.request.request_hash != decision.request.request_hash:
                raise ValueError("admission request id conflicts with prior payload")
            return current
        if decision.geoblock_snapshot is not None:
            self.record_snapshot(decision.geoblock_snapshot)
        request = decision.request
        snapshot = decision.geoblock_snapshot
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.simulator_admission_decisions (
                    decision_id,request_id,request_hash,operation_type,account_id,
                    strategy_id,asset_id,condition_id,market_id,exposure_effect,
                    exposure_before,exposure_after,country,region,detected_ip,
                    policy_version,geoblock_raw_payload_hash,geoblock_snapshot_id,
                    admission_status,jurisdiction_mode,reason_codes,request_payload,
                    decided_at
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s::jsonb,%s::jsonb,%s
                ) ON CONFLICT (request_id) DO NOTHING
                RETURNING decision_id
                """,
                (
                    decision.decision_id,
                    request.request_id,
                    request.request_hash,
                    request.operation.value,
                    request.account_id,
                    request.strategy_id,
                    request.asset_id,
                    request.condition_id,
                    request.market_id,
                    request.exposure_effect.value,
                    request.exposure_before,
                    request.exposure_after,
                    snapshot.country if snapshot else None,
                    snapshot.region if snapshot else None,
                    snapshot.detected_ip if snapshot else None,
                    decision.policy_version,
                    snapshot.raw_payload_hash if snapshot else None,
                    snapshot.snapshot_id if snapshot else None,
                    decision.status.value,
                    decision.jurisdiction_mode.value,
                    json.dumps(list(decision.reason_codes)),
                    json.dumps(request.as_dict(), sort_keys=True),
                    decision.decided_at,
                ),
            )
            inserted = cur.fetchone()
        if inserted is not None:
            return decision
        replayed = self.decision(request.request_id)
        if replayed is None or replayed.request.request_hash != request.request_hash:
            raise ValueError("admission request id conflicts with concurrent payload")
        return replayed


class MemoryAdmissionStore:
    """Deterministic test/store adapter with the same collision contract."""

    def __init__(self) -> None:
        self.policies: dict[str, JurisdictionPolicy] = {}
        self.snapshots: dict[str, GeoblockSnapshot] = {}
        self.decisions: dict[str, AdmissionDecision] = {}

    def ensure_schema(self) -> None:
        return None

    def record_policy(self, policy: JurisdictionPolicy) -> None:
        current = self.policies.get(policy.version)
        if current is not None and current.rules_hash != policy.rules_hash:
            raise ValueError("admission policy version conflicts with prior rules")
        self.policies[policy.version] = policy

    def record_snapshot(self, snapshot: GeoblockSnapshot) -> None:
        self.snapshots[snapshot.snapshot_id] = snapshot

    def decision(self, request_id: str) -> AdmissionDecision | None:
        return self.decisions.get(str(request_id))

    def record_decision(self, decision: AdmissionDecision) -> AdmissionDecision:
        current = self.decisions.get(decision.request.request_id)
        if current is not None:
            if current.request.request_hash != decision.request.request_hash:
                raise ValueError("admission request id conflicts with prior payload")
            return current
        if decision.geoblock_snapshot is not None:
            self.record_snapshot(decision.geoblock_snapshot)
        self.decisions[decision.request.request_id] = decision
        return decision


def _decision_from_row(row: dict[str, Any]) -> AdmissionDecision:
    payload = dict(row["request_payload"])
    request = AdmissionRequest(
        request_id=str(payload["request_id"]),
        operation=AdmissionOperation(str(payload["operation"])),
        account_id=str(payload["account_id"]),
        strategy_id=payload.get("strategy_id"),
        asset_id=payload.get("asset_id"),
        condition_id=payload.get("condition_id"),
        market_id=payload.get("market_id"),
        exposure_effect=ExposureEffect(str(payload["exposure_effect"])),
        exposure_before=_decimal(payload.get("exposure_before")),
        exposure_after=_decimal(payload.get("exposure_after")),
        observed_at=_datetime(payload["observed_at"]),
        metadata=dict(payload.get("metadata") or {}),
    )
    snapshot = None
    if row.get("geoblock_snapshot_id"):
        snapshot = GeoblockSnapshot(
            blocked=bool(row["blocked"]),
            country=str(row.get("country") or ""),
            region=str(row.get("region") or ""),
            detected_ip=str(row.get("detected_ip") or ""),
            observed_at=row["geo_observed_at"],
            expires_at=row["geo_expires_at"],
            raw_payload_hash=str(row["geoblock_raw_payload_hash"]),
            source=str(row["geo_source"]),
            proxy_url=row.get("proxy_url"),
        )
    return AdmissionDecision(
        decision_id=str(row["decision_id"]),
        request=request,
        status=AdmissionStatus(str(row["admission_status"])),
        jurisdiction_mode=JurisdictionMode(str(row["jurisdiction_mode"])),
        policy_version=str(row["policy_version"]),
        reason_codes=tuple(row["reason_codes"]),
        decided_at=row["decided_at"],
        geoblock_snapshot=snapshot,
    )


def _decimal(value: Any) -> Decimal | None:
    return Decimal(str(value)) if value is not None else None


def _datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
