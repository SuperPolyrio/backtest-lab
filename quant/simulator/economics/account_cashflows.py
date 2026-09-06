"""Durable confirmed-only account cashflows outside CLOB execution."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any


class AccountCashflowType(str, Enum):
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    BRIDGE_DEPOSIT = "BRIDGE_DEPOSIT"
    BRIDGE_WITHDRAWAL = "BRIDGE_WITHDRAWAL"
    BRIDGE_FEE = "BRIDGE_FEE"
    SPONSOR_COMMITMENT = "SPONSOR_COMMITMENT"
    SPONSOR_REFUND = "SPONSOR_REFUND"
    SPONSOR_DISTRIBUTION = "SPONSOR_DISTRIBUTION"
    DISPUTE_BOND = "DISPUTE_BOND"
    DISPUTE_BOND_RETURN = "DISPUTE_BOND_RETURN"
    DISPUTE_BOUNTY = "DISPUTE_BOUNTY"
    DISPUTE_BOND_LOSS = "DISPUTE_BOND_LOSS"


class AccountCashflowState(str, Enum):
    CREATED = "CREATED"
    RESERVED = "RESERVED"
    SUBMITTED = "SUBMITTED"
    PROCESSING = "PROCESSING"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


TERMINAL_CASHFLOW_STATES = frozenset(
    {
        AccountCashflowState.CONFIRMED,
        AccountCashflowState.FAILED,
        AccountCashflowState.REFUNDED,
    }
)

_NON_TERMINAL_STATE_RANK = {
    AccountCashflowState.CREATED: 0,
    AccountCashflowState.RESERVED: 1,
    AccountCashflowState.SUBMITTED: 2,
    AccountCashflowState.PROCESSING: 3,
}


@dataclass(frozen=True)
class AccountCashflowOperation:
    operation_id: str
    account_id: str
    strategy_id: str
    operation_type: AccountCashflowType
    amount: Decimal
    currency: str
    state: AccountCashflowState
    effective_ts: datetime
    source: str
    source_event_id: str
    idempotency_key: str
    condition_id: str | None = None
    asset_id: str | None = None
    source_tx_hash: str | None = None
    raw_payload_hash: str | None = None
    rule_version: str = "account-cashflow-v1"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        required = {
            "operation_id": self.operation_id,
            "account_id": self.account_id,
            "strategy_id": self.strategy_id,
            "currency": self.currency,
            "source": self.source,
            "source_event_id": self.source_event_id,
            "idempotency_key": self.idempotency_key,
        }
        if any(not str(value).strip() for value in required.values()):
            raise ValueError("account cashflow identity is incomplete")
        if Decimal(self.amount) <= 0:
            raise ValueError("account cashflow amount must be positive")
        if self.effective_ts.tzinfo is None:
            raise ValueError("account cashflow timestamp must be timezone-aware")
        object.__setattr__(self, "amount", Decimal(self.amount))

    @property
    def cash_delta(self) -> Decimal:
        amount = self.amount
        if self.operation_type in {
            AccountCashflowType.DEPOSIT,
            AccountCashflowType.BRIDGE_DEPOSIT,
            AccountCashflowType.SPONSOR_REFUND,
            AccountCashflowType.DISPUTE_BOND_RETURN,
            AccountCashflowType.DISPUTE_BOUNTY,
        }:
            return amount
        if self.operation_type in {
            AccountCashflowType.WITHDRAWAL,
            AccountCashflowType.BRIDGE_WITHDRAWAL,
            AccountCashflowType.BRIDGE_FEE,
            AccountCashflowType.SPONSOR_COMMITMENT,
            AccountCashflowType.DISPUTE_BOND,
        }:
            return -amount
        return Decimal(0)

    @property
    def return_delta(self) -> Decimal:
        if self.operation_type is AccountCashflowType.BRIDGE_FEE:
            return -self.amount
        if self.operation_type is AccountCashflowType.SPONSOR_DISTRIBUTION:
            return -self.amount
        if self.operation_type is AccountCashflowType.DISPUTE_BOUNTY:
            return self.amount
        if self.operation_type is AccountCashflowType.DISPUTE_BOND_LOSS:
            return -self.amount
        return Decimal(0)


ACCOUNT_CASHFLOW_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS quant.paper_account_cashflow_operations (
        operation_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        operation_type TEXT NOT NULL,
        amount NUMERIC NOT NULL CHECK (amount > 0),
        currency TEXT NOT NULL,
        state TEXT NOT NULL,
        cash_delta NUMERIC NOT NULL,
        return_delta NUMERIC NOT NULL,
        effective_ts TIMESTAMPTZ NOT NULL,
        confirmed_ts TIMESTAMPTZ,
        source TEXT NOT NULL,
        source_event_id TEXT NOT NULL,
        source_tx_hash TEXT,
        raw_payload_hash TEXT,
        rule_version TEXT NOT NULL,
        idempotency_key TEXT NOT NULL UNIQUE,
        condition_id TEXT,
        asset_id TEXT,
        cash_applied BOOLEAN NOT NULL DEFAULT FALSE,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        UNIQUE (source,source_event_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS paper_account_cashflows_strategy_ts_idx
    ON quant.paper_account_cashflow_operations (strategy_id,effective_ts,operation_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_account_economic_events (
        event_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        condition_id TEXT,
        asset_id TEXT,
        event_type TEXT NOT NULL,
        amount NUMERIC NOT NULL,
        currency TEXT NOT NULL,
        quantity NUMERIC,
        status TEXT NOT NULL,
        effective_ts TIMESTAMPTZ NOT NULL,
        confirmed_ts TIMESTAMPTZ,
        source TEXT NOT NULL,
        source_event_id TEXT,
        source_tx_hash TEXT,
        economics_regime_id TEXT,
        model_version TEXT NOT NULL,
        idempotency_key TEXT NOT NULL UNIQUE,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
)


class PostgresAccountCashflowStore:
    def __init__(self, connection_factory: Any) -> None:
        self.connection_factory = connection_factory
        self.ensure_schema()

    def ensure_schema(self) -> None:
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS quant")
            for statement in ACCOUNT_CASHFLOW_SCHEMA_STATEMENTS:
                cur.execute(statement)
            conn.commit()

    def record(self, operation: AccountCashflowOperation) -> bool:
        """Idempotently persist and apply a confirmed official operation once."""

        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            self._require_account(cur, operation.strategy_id)
            cur.execute(
                """
                INSERT INTO quant.paper_account_cashflow_operations (
                    operation_id,account_id,strategy_id,operation_type,amount,
                    currency,state,cash_delta,return_delta,effective_ts,
                    confirmed_ts,source,source_event_id,source_tx_hash,
                    raw_payload_hash,rule_version,idempotency_key,condition_id,
                    asset_id,metadata
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb
                ) ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING operation_id
                """,
                (
                    operation.operation_id,
                    operation.account_id,
                    operation.strategy_id,
                    operation.operation_type.value,
                    operation.amount,
                    operation.currency,
                    operation.state.value,
                    operation.cash_delta,
                    operation.return_delta,
                    operation.effective_ts,
                    operation.effective_ts
                    if operation.state is AccountCashflowState.CONFIRMED
                    else None,
                    operation.source,
                    operation.source_event_id,
                    operation.source_tx_hash,
                    operation.raw_payload_hash,
                    operation.rule_version,
                    operation.idempotency_key,
                    operation.condition_id,
                    operation.asset_id,
                    _json(operation.metadata),
                ),
            )
            inserted = cur.fetchone() is not None
            if not inserted:
                existing = self._assert_same(cur, operation)
                existing_state = AccountCashflowState(str(existing["state"]))
                should_advance = self._validate_state_transition(
                    existing_state, operation.state
                )
                if should_advance and operation.state is AccountCashflowState.CONFIRMED:
                    cur.execute(
                        """
                        UPDATE quant.paper_account_cashflow_operations
                        SET state='CONFIRMED',confirmed_ts=COALESCE(confirmed_ts,%s),
                            source_tx_hash=COALESCE(source_tx_hash,%s),
                            raw_payload_hash=COALESCE(raw_payload_hash,%s),
                            updated_at=clock_timestamp()
                        WHERE idempotency_key=%s AND state NOT IN ('CONFIRMED','FAILED')
                        """,
                        (
                            operation.effective_ts,
                            operation.source_tx_hash,
                            operation.raw_payload_hash,
                            operation.idempotency_key,
                        ),
                    )
                elif should_advance and operation.state in {
                    AccountCashflowState.FAILED,
                    AccountCashflowState.REFUNDED,
                }:
                    cur.execute(
                        """
                        UPDATE quant.paper_account_cashflow_operations
                        SET state=%s,updated_at=clock_timestamp()
                        WHERE idempotency_key=%s
                          AND state NOT IN ('CONFIRMED','FAILED','REFUNDED')
                        """,
                        (operation.state.value, operation.idempotency_key),
                    )
            if operation.state is AccountCashflowState.CONFIRMED:
                self._apply_confirmed(cur, operation.idempotency_key)
            conn.commit()
        return inserted

    def transition(
        self,
        idempotency_key: str,
        *,
        state: AccountCashflowState,
        observed_at: datetime,
        source_tx_hash: str | None = None,
    ) -> bool:
        if observed_at.tzinfo is None:
            raise ValueError("cashflow transition timestamp must be timezone-aware")
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT state,source_tx_hash
                FROM quant.paper_account_cashflow_operations
                WHERE idempotency_key=%s FOR UPDATE
                """,
                (idempotency_key,),
            )
            current = cur.fetchone()
            if current is None:
                return False
            existing_hash = str(current["source_tx_hash"] or "")
            if existing_hash and source_tx_hash and existing_hash != source_tx_hash:
                raise ValueError("account cashflow transaction hash collision")
            should_advance = self._validate_state_transition(
                AccountCashflowState(str(current["state"])), state
            )
            if not should_advance:
                return False
            cur.execute(
                """
                UPDATE quant.paper_account_cashflow_operations
                SET state=%s,confirmed_ts=CASE WHEN %s='CONFIRMED' THEN %s ELSE confirmed_ts END,
                    source_tx_hash=COALESCE(%s,source_tx_hash),updated_at=clock_timestamp()
                WHERE idempotency_key=%s AND state NOT IN ('CONFIRMED','FAILED','REFUNDED')
                RETURNING idempotency_key
                """,
                (state.value, state.value, observed_at, source_tx_hash, idempotency_key),
            )
            changed = cur.fetchone() is not None
            if state is AccountCashflowState.CONFIRMED:
                self._apply_confirmed(cur, idempotency_key)
            conn.commit()
        return changed

    def apply_due(self, *, as_of: datetime | None = None) -> int:
        cutoff = as_of or datetime.now(timezone.utc)
        if cutoff.tzinfo is None:
            raise ValueError("cashflow cutoff must be timezone-aware")
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT idempotency_key
                FROM quant.paper_account_cashflow_operations
                WHERE state='CONFIRMED' AND cash_applied=FALSE AND effective_ts <= %s
                ORDER BY effective_ts,operation_id FOR UPDATE
                """,
                (cutoff,),
            )
            keys = [str(row["idempotency_key"]) for row in cur.fetchall()]
            for key in keys:
                self._apply_confirmed(cur, key)
            conn.commit()
        return len(keys)

    def _apply_confirmed(self, cur: Any, idempotency_key: str) -> None:
        cur.execute(
            """
            SELECT * FROM quant.paper_account_cashflow_operations
            WHERE idempotency_key=%s FOR UPDATE
            """,
            (idempotency_key,),
        )
        row = cur.fetchone()
        if row is None or str(row["state"]) != AccountCashflowState.CONFIRMED.value:
            return
        if bool(row["cash_applied"]):
            return
        if row["effective_ts"] > datetime.now(timezone.utc):
            return
        cash_delta = Decimal(row["cash_delta"])
        if cash_delta:
            if cash_delta < 0:
                cur.execute(
                    """
                    SELECT cash_balance,cash_reserved FROM quant.paper_accounts
                    WHERE strategy_id=%s FOR UPDATE
                    """,
                    (str(row["strategy_id"]),),
                )
                account = cur.fetchone()
                available = Decimal(account["cash_balance"]) - Decimal(
                    account["cash_reserved"]
                )
                if -cash_delta > available:
                    raise ValueError(
                        "insufficient available paper cash for account cashflow"
                    )
            cur.execute(
                """
                UPDATE quant.paper_accounts
                SET cash_balance=cash_balance + %s,updated_at=clock_timestamp()
                WHERE strategy_id=%s
                """,
                (cash_delta, str(row["strategy_id"])),
            )
        return_delta = Decimal(row["return_delta"])
        event_amount = return_delta if return_delta else cash_delta
        event_id = _deterministic_id(
            "account-cashflow-event", {"operation_id": str(row["operation_id"])}
        )
        cur.execute(
            """
            INSERT INTO quant.paper_account_economic_events (
                event_id,account_id,strategy_id,condition_id,asset_id,event_type,
                amount,currency,status,effective_ts,confirmed_ts,source,
                source_event_id,source_tx_hash,model_version,idempotency_key,metadata
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,'CONFIRMED',%s,%s,%s,%s,%s,%s,%s,%s::jsonb
            ) ON CONFLICT (idempotency_key) DO NOTHING
            """,
            (
                event_id,
                str(row["account_id"]),
                str(row["strategy_id"]),
                str(row["condition_id"] or "") or None,
                str(row["asset_id"] or "") or None,
                str(row["operation_type"]),
                event_amount,
                str(row["currency"]),
                row["effective_ts"],
                row["confirmed_ts"] or row["effective_ts"],
                str(row["source"]),
                str(row["source_event_id"]),
                str(row["source_tx_hash"] or "") or None,
                str(row["rule_version"]),
                f"account-cashflow-event:{idempotency_key}",
                _json(
                    {
                        **dict(row["metadata"] or {}),
                        "cash_delta": str(cash_delta),
                        "return_delta": str(return_delta),
                        "operation_id": str(row["operation_id"]),
                    }
                ),
            ),
        )
        cur.execute(
            """
            UPDATE quant.paper_account_cashflow_operations
            SET cash_applied=TRUE,updated_at=clock_timestamp()
            WHERE idempotency_key=%s
            """,
            (idempotency_key,),
        )

    @staticmethod
    def _require_account(cur: Any, strategy_id: str) -> None:
        cur.execute(
            "SELECT strategy_id FROM quant.paper_accounts WHERE strategy_id=%s FOR UPDATE",
            (strategy_id,),
        )
        if cur.fetchone() is None:
            raise LookupError(f"paper account not found: {strategy_id}")

    @staticmethod
    def _assert_same(cur: Any, operation: AccountCashflowOperation) -> Any:
        cur.execute(
            """
            SELECT operation_id,account_id,strategy_id,operation_type,amount,
                   currency,state,cash_delta,return_delta,source,source_event_id,
                   source_tx_hash,raw_payload_hash,rule_version,condition_id,asset_id
            FROM quant.paper_account_cashflow_operations WHERE idempotency_key=%s
            """,
            (operation.idempotency_key,),
        )
        row = cur.fetchone()
        existing_tx_hash = str(row["source_tx_hash"] or "") if row else ""
        if row is None or (
            str(row["operation_id"]) != operation.operation_id
            or str(row["account_id"]) != operation.account_id
            or str(row["strategy_id"]) != operation.strategy_id
            or str(row["operation_type"]) != operation.operation_type.value
            or Decimal(row["amount"]) != operation.amount
            or str(row["currency"]) != operation.currency
            or Decimal(row["cash_delta"]) != operation.cash_delta
            or Decimal(row["return_delta"]) != operation.return_delta
            or str(row["source"]) != operation.source
            or str(row["source_event_id"]) != operation.source_event_id
            or str(row["rule_version"]) != operation.rule_version
            or (str(row["condition_id"] or "") or None) != operation.condition_id
            or (str(row["asset_id"] or "") or None) != operation.asset_id
            or bool(
                existing_tx_hash
                and operation.source_tx_hash
                and existing_tx_hash != operation.source_tx_hash
            )
        ):
            raise ValueError("account cashflow idempotency collision")
        return row

    @staticmethod
    def _validate_state_transition(
        current: AccountCashflowState, requested: AccountCashflowState
    ) -> bool:
        if current is requested:
            return False
        if current in TERMINAL_CASHFLOW_STATES:
            if requested in TERMINAL_CASHFLOW_STATES:
                raise ValueError("account cashflow terminal state cannot change")
            return False
        if requested not in TERMINAL_CASHFLOW_STATES and (
            _NON_TERMINAL_STATE_RANK[requested] < _NON_TERMINAL_STATE_RANK[current]
        ):
            raise ValueError("account cashflow state cannot move backwards")
        return True


def account_cashflow_operation_id(
    source: str, source_event_id: str, operation_type: AccountCashflowType
) -> str:
    return _deterministic_id(
        "account-cashflow",
        {
            "operation_type": operation_type.value,
            "source": source,
            "source_event_id": source_event_id,
        },
    )


def _deterministic_id(prefix: str, payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload), sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return f"{prefix}:{hashlib.sha256(encoded).hexdigest()}"


def _json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), sort_keys=True, default=str)
