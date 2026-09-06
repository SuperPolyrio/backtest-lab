"""Bridge, sponsorship and dispute account-operation contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from quant.simulator.admission import (
    AdmissionOperation,
    AdmissionRequest,
    ExposureEffect,
    PostgresAdmissionStore,
    UnifiedAdmissionService,
)

from .account_cashflows import (
    AccountCashflowOperation,
    AccountCashflowState,
    AccountCashflowType,
    account_cashflow_operation_id,
)


class BridgeDirection(str, Enum):
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"


class BridgeTransferState(str, Enum):
    DEPOSIT_DETECTED = "DEPOSIT_DETECTED"
    PROCESSING = "PROCESSING"
    ORIGIN_TX_CONFIRMED = "ORIGIN_TX_CONFIRMED"
    SUBMITTED = "SUBMITTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class DisputeOutcome(str, Enum):
    WON = "WON"
    LOST = "LOST"


ACCOUNT_PROGRAM_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS quant.paper_bridge_transfer_states (
        transfer_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        direction TEXT NOT NULL,
        amount NUMERIC NOT NULL CHECK (amount > 0),
        fee NUMERIC NOT NULL CHECK (fee >= 0),
        currency TEXT NOT NULL,
        state TEXT NOT NULL,
        observed_at TIMESTAMPTZ NOT NULL,
        source_event_id TEXT NOT NULL UNIQUE,
        transaction_hash TEXT,
        raw_payload_hash TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_sponsor_commitments (
        sponsor_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        committed_amount NUMERIC NOT NULL CHECK (committed_amount > 0),
        distributed_amount NUMERIC NOT NULL DEFAULT 0 CHECK (distributed_amount >= 0),
        refunded_amount NUMERIC NOT NULL DEFAULT 0 CHECK (refunded_amount >= 0),
        currency TEXT NOT NULL,
        state TEXT NOT NULL,
        starts_at TIMESTAMPTZ NOT NULL,
        cancellation_requested_at TIMESTAMPTZ,
        cancellation_effective_at TIMESTAMPTZ,
        source_event_id TEXT NOT NULL UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        CHECK (distributed_amount + refunded_amount <= committed_amount)
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS paper_sponsor_one_active_market_idx
    ON quant.paper_sponsor_commitments (account_id,condition_id)
    WHERE state IN ('ACTIVE','CANCEL_PENDING')
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_sponsor_distributions (
        distribution_id TEXT PRIMARY KEY,
        sponsor_id TEXT NOT NULL,
        distribution_date DATE NOT NULL,
        amount NUMERIC NOT NULL CHECK (amount > 0),
        effective_ts TIMESTAMPTZ NOT NULL,
        source_event_id TEXT NOT NULL UNIQUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        UNIQUE (sponsor_id,distribution_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_dispute_bonds (
        dispute_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        bond_amount NUMERIC NOT NULL CHECK (bond_amount > 0),
        bounty_amount NUMERIC NOT NULL DEFAULT 0 CHECK (bounty_amount >= 0),
        currency TEXT NOT NULL,
        state TEXT NOT NULL,
        posted_at TIMESTAMPTZ NOT NULL,
        resolved_at TIMESTAMPTZ,
        source_event_id TEXT NOT NULL UNIQUE,
        transaction_hash TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
)


@dataclass(frozen=True)
class BridgeTransfer:
    transfer_id: str
    account_id: str
    strategy_id: str
    direction: BridgeDirection
    amount: Decimal
    fee: Decimal
    currency: str
    state: BridgeTransferState
    observed_at: datetime
    source_event_id: str
    transaction_hash: str | None = None
    raw_payload_hash: str | None = None

    def __post_init__(self) -> None:
        if Decimal(self.amount) <= 0 or Decimal(self.fee) < 0:
            raise ValueError("bridge amount must be positive and fee non-negative")
        if Decimal(self.fee) > Decimal(self.amount):
            raise ValueError("bridge fee cannot exceed principal")
        if self.observed_at.tzinfo is None:
            raise ValueError("bridge timestamp must be timezone-aware")
        if self.state is BridgeTransferState.COMPLETED and not self.transaction_hash:
            raise ValueError("completed bridge transfer requires destination tx hash")

    def account_operations(self) -> tuple[AccountCashflowOperation, ...]:
        state = (
            AccountCashflowState.CONFIRMED
            if self.state is BridgeTransferState.COMPLETED
            else AccountCashflowState.FAILED
            if self.state is BridgeTransferState.FAILED
            else AccountCashflowState.PROCESSING
        )
        principal_type = (
            AccountCashflowType.BRIDGE_DEPOSIT
            if self.direction is BridgeDirection.DEPOSIT
            else AccountCashflowType.BRIDGE_WITHDRAWAL
        )
        operations = [
            _operation(
                account_id=self.account_id,
                strategy_id=self.strategy_id,
                operation_type=principal_type,
                amount=Decimal(self.amount),
                currency=self.currency,
                state=state,
                effective_ts=self.observed_at,
                source="POLYMARKET_BRIDGE_API",
                source_event_id=f"{self.source_event_id}:principal",
                transaction_hash=self.transaction_hash,
                raw_payload_hash=self.raw_payload_hash,
                metadata={"bridge_transfer_id": self.transfer_id},
            )
        ]
        if self.fee > 0:
            operations.append(
                _operation(
                    account_id=self.account_id,
                    strategy_id=self.strategy_id,
                    operation_type=AccountCashflowType.BRIDGE_FEE,
                    amount=Decimal(self.fee),
                    currency=self.currency,
                    state=state,
                    effective_ts=self.observed_at,
                    source="POLYMARKET_BRIDGE_API",
                    source_event_id=f"{self.source_event_id}:fee",
                    transaction_hash=self.transaction_hash,
                    raw_payload_hash=self.raw_payload_hash,
                    metadata={"bridge_transfer_id": self.transfer_id},
                )
            )
        return tuple(operations)


@dataclass(frozen=True)
class SponsorCommitment:
    sponsor_id: str
    account_id: str
    strategy_id: str
    condition_id: str
    committed_amount: Decimal
    currency: str
    starts_at: datetime
    source_event_id: str
    distributed_amount: Decimal = Decimal(0)

    def __post_init__(self) -> None:
        if Decimal(self.committed_amount) <= 0:
            raise ValueError("sponsor commitment must be positive")
        if not Decimal(0) <= Decimal(self.distributed_amount) <= Decimal(
            self.committed_amount
        ):
            raise ValueError("sponsor distribution exceeds commitment")
        if self.starts_at.tzinfo is None:
            raise ValueError("sponsor timestamp must be timezone-aware")

    @property
    def remaining_amount(self) -> Decimal:
        return Decimal(self.committed_amount) - Decimal(self.distributed_amount)

    def commitment_operation(self) -> AccountCashflowOperation:
        return _operation(
            account_id=self.account_id,
            strategy_id=self.strategy_id,
            operation_type=AccountCashflowType.SPONSOR_COMMITMENT,
            amount=Decimal(self.committed_amount),
            currency=self.currency,
            state=AccountCashflowState.CONFIRMED,
            effective_ts=self.starts_at,
            source="POLYMARKET_SPONSOR_PROGRAM",
            source_event_id=f"{self.source_event_id}:commitment",
            condition_id=self.condition_id,
            metadata={"sponsor_id": self.sponsor_id},
        )

    def distribution_operation(
        self, *, amount: Decimal, distribution_date: datetime
    ) -> AccountCashflowOperation:
        value = Decimal(amount)
        if value <= 0 or value > self.remaining_amount:
            raise ValueError("invalid sponsor daily distribution")
        if distribution_date.tzinfo is None:
            raise ValueError("sponsor distribution timestamp must be timezone-aware")
        return _operation(
            account_id=self.account_id,
            strategy_id=self.strategy_id,
            operation_type=AccountCashflowType.SPONSOR_DISTRIBUTION,
            amount=value,
            currency=self.currency,
            state=AccountCashflowState.CONFIRMED,
            effective_ts=distribution_date,
            source="POLYMARKET_SPONSOR_PROGRAM",
            source_event_id=(
                f"{self.source_event_id}:distribution:"
                f"{distribution_date.astimezone(timezone.utc).date().isoformat()}"
            ),
            condition_id=self.condition_id,
            metadata={"sponsor_id": self.sponsor_id},
        )

    def cancellation_refund_operation(
        self, *, requested_at: datetime
    ) -> AccountCashflowOperation | None:
        if requested_at.tzinfo is None:
            raise ValueError("sponsor cancellation timestamp must be timezone-aware")
        if self.remaining_amount == 0:
            return None
        utc = requested_at.astimezone(timezone.utc)
        effective = datetime.combine(
            utc.date() + timedelta(days=1), time.min, tzinfo=timezone.utc
        )
        return _operation(
            account_id=self.account_id,
            strategy_id=self.strategy_id,
            operation_type=AccountCashflowType.SPONSOR_REFUND,
            amount=self.remaining_amount,
            currency=self.currency,
            state=AccountCashflowState.PROCESSING,
            effective_ts=effective,
            source="POLYMARKET_SPONSOR_PROGRAM",
            source_event_id=f"{self.source_event_id}:refund:{effective.date().isoformat()}",
            condition_id=self.condition_id,
            metadata={"sponsor_id": self.sponsor_id, "requested_at": requested_at.isoformat()},
        )


@dataclass(frozen=True)
class DisputeBond:
    dispute_id: str
    account_id: str
    strategy_id: str
    condition_id: str
    bond_amount: Decimal
    currency: str
    posted_at: datetime
    source_event_id: str

    def __post_init__(self) -> None:
        if Decimal(self.bond_amount) <= 0:
            raise ValueError("dispute bond must be positive")
        if self.posted_at.tzinfo is None:
            raise ValueError("dispute bond timestamp must be timezone-aware")

    def post_operation(self) -> AccountCashflowOperation:
        return _operation(
            account_id=self.account_id,
            strategy_id=self.strategy_id,
            operation_type=AccountCashflowType.DISPUTE_BOND,
            amount=Decimal(self.bond_amount),
            currency=self.currency,
            state=AccountCashflowState.CONFIRMED,
            effective_ts=self.posted_at,
            source="POLYMARKET_RESOLUTION",
            source_event_id=f"{self.source_event_id}:bond",
            condition_id=self.condition_id,
            metadata={"dispute_id": self.dispute_id},
        )

    def resolution_operations(
        self,
        *,
        outcome: DisputeOutcome,
        resolved_at: datetime,
        bounty_amount: Decimal = Decimal(0),
        transaction_hash: str | None = None,
    ) -> tuple[AccountCashflowOperation, ...]:
        if resolved_at.tzinfo is None:
            raise ValueError("dispute resolution timestamp must be timezone-aware")
        bounty = Decimal(bounty_amount)
        if bounty < 0:
            raise ValueError("dispute bounty cannot be negative")
        if outcome is DisputeOutcome.LOST:
            return (
                _operation(
                    account_id=self.account_id,
                    strategy_id=self.strategy_id,
                    operation_type=AccountCashflowType.DISPUTE_BOND_LOSS,
                    amount=Decimal(self.bond_amount),
                    currency=self.currency,
                    state=AccountCashflowState.CONFIRMED,
                    effective_ts=resolved_at,
                    source="POLYMARKET_RESOLUTION",
                    source_event_id=f"{self.source_event_id}:loss",
                    transaction_hash=transaction_hash,
                    condition_id=self.condition_id,
                    metadata={"dispute_id": self.dispute_id},
                ),
            )
        operations = [
            _operation(
                account_id=self.account_id,
                strategy_id=self.strategy_id,
                operation_type=AccountCashflowType.DISPUTE_BOND_RETURN,
                amount=Decimal(self.bond_amount),
                currency=self.currency,
                state=AccountCashflowState.CONFIRMED,
                effective_ts=resolved_at,
                source="POLYMARKET_RESOLUTION",
                source_event_id=f"{self.source_event_id}:bond-return",
                transaction_hash=transaction_hash,
                condition_id=self.condition_id,
                metadata={"dispute_id": self.dispute_id},
            )
        ]
        if bounty > 0:
            operations.append(
                _operation(
                    account_id=self.account_id,
                    strategy_id=self.strategy_id,
                    operation_type=AccountCashflowType.DISPUTE_BOUNTY,
                    amount=bounty,
                    currency=self.currency,
                    state=AccountCashflowState.CONFIRMED,
                    effective_ts=resolved_at,
                    source="POLYMARKET_RESOLUTION",
                    source_event_id=f"{self.source_event_id}:bounty",
                    transaction_hash=transaction_hash,
                    condition_id=self.condition_id,
                    metadata={"dispute_id": self.dispute_id},
                )
            )
        return tuple(operations)


class PostgresAccountProgramStore:
    """Durable P2 program state with one-way transitions and cash idempotency."""

    _BRIDGE_RANK = {
        BridgeTransferState.DEPOSIT_DETECTED: 0,
        BridgeTransferState.PROCESSING: 1,
        BridgeTransferState.ORIGIN_TX_CONFIRMED: 2,
        BridgeTransferState.SUBMITTED: 3,
        BridgeTransferState.COMPLETED: 4,
        BridgeTransferState.FAILED: 4,
    }

    def __init__(
        self,
        connection_factory: Any,
        *,
        admission_service: UnifiedAdmissionService | None = None,
    ) -> None:
        from .account_cashflows import PostgresAccountCashflowStore

        self.connection_factory = connection_factory
        self.cashflows = PostgresAccountCashflowStore(connection_factory)
        self.admission_store = PostgresAdmissionStore(connection_factory)
        self.admission_service = admission_service or UnifiedAdmissionService(
            store=self.admission_store
        )
        self.ensure_schema()

    def ensure_schema(self) -> None:
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS quant")
            for statement in ACCOUNT_PROGRAM_SCHEMA_STATEMENTS:
                cur.execute(statement)
            conn.commit()
        self.admission_store.ensure_schema()

    def record_bridge(self, transfer: BridgeTransfer) -> tuple[AccountCashflowOperation, ...]:
        self._admit(
            request_id=(
                f"account-program:bridge:{transfer.transfer_id}:"
                f"{transfer.state.value}:{transfer.source_event_id}"
            ),
            operation=(
                AdmissionOperation.BRIDGE_DEPOSIT
                if transfer.direction is BridgeDirection.DEPOSIT
                else AdmissionOperation.BRIDGE_WITHDRAWAL
            ),
            account_id=transfer.account_id,
            strategy_id=transfer.strategy_id,
            observed_at=transfer.observed_at,
            metadata={"transfer_id": transfer.transfer_id, "state": transfer.state.value},
        )
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM quant.paper_bridge_transfer_states "
                "WHERE transfer_id=%s FOR UPDATE",
                (transfer.transfer_id,),
            )
            current = cur.fetchone()
            if current is None:
                cur.execute(
                    """
                    INSERT INTO quant.paper_bridge_transfer_states (
                        transfer_id,account_id,strategy_id,direction,amount,fee,
                        currency,state,observed_at,source_event_id,transaction_hash,
                        raw_payload_hash
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        transfer.transfer_id,
                        transfer.account_id,
                        transfer.strategy_id,
                        transfer.direction.value,
                        transfer.amount,
                        transfer.fee,
                        transfer.currency,
                        transfer.state.value,
                        transfer.observed_at,
                        transfer.source_event_id,
                        transfer.transaction_hash,
                        transfer.raw_payload_hash,
                    ),
                )
            else:
                self._assert_bridge_identity(current, transfer)
                old = BridgeTransferState(str(current["state"]))
                existing_tx_hash = str(current["transaction_hash"] or "")
                if (
                    existing_tx_hash
                    and transfer.transaction_hash
                    and existing_tx_hash != transfer.transaction_hash
                ):
                    raise ValueError("bridge transaction hash collision")
                if old in {BridgeTransferState.COMPLETED, BridgeTransferState.FAILED}:
                    if old is not transfer.state:
                        raise ValueError("bridge terminal state cannot change")
                elif self._BRIDGE_RANK[transfer.state] < self._BRIDGE_RANK[old]:
                    raise ValueError("bridge state cannot move backwards")
                cur.execute(
                    """
                    UPDATE quant.paper_bridge_transfer_states
                    SET state=%s,observed_at=GREATEST(observed_at,%s),
                        transaction_hash=COALESCE(transaction_hash,%s),
                        raw_payload_hash=COALESCE(%s,raw_payload_hash),
                        updated_at=clock_timestamp()
                    WHERE transfer_id=%s
                    """,
                    (
                        transfer.state.value,
                        transfer.observed_at,
                        transfer.transaction_hash,
                        transfer.raw_payload_hash,
                        transfer.transfer_id,
                    ),
                )
            conn.commit()
        operations = transfer.account_operations()
        for operation in operations:
            self.cashflows.record(operation)
        return operations

    def open_sponsorship(self, sponsor: SponsorCommitment) -> AccountCashflowOperation:
        self._admit(
            request_id=f"account-program:sponsor:{sponsor.sponsor_id}:open",
            operation=AdmissionOperation.SPONSOR,
            account_id=sponsor.account_id,
            strategy_id=sponsor.strategy_id,
            observed_at=sponsor.starts_at,
            condition_id=sponsor.condition_id,
            metadata={"sponsor_id": sponsor.sponsor_id},
        )
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"paper-sponsor:{sponsor.account_id}:{sponsor.condition_id}",),
            )
            cur.execute(
                "SELECT * FROM quant.paper_sponsor_commitments "
                "WHERE sponsor_id=%s FOR UPDATE",
                (sponsor.sponsor_id,),
            )
            current = cur.fetchone()
            if current is None:
                cur.execute(
                    """
                    SELECT sponsor_id FROM quant.paper_sponsor_commitments
                    WHERE account_id=%s AND condition_id=%s
                      AND state IN ('ACTIVE','CANCEL_PENDING')
                    """,
                    (sponsor.account_id, sponsor.condition_id),
                )
                other = cur.fetchone()
                if other is not None:
                    raise ValueError("only one active sponsorship is allowed per market")
                cur.execute(
                    """
                    INSERT INTO quant.paper_sponsor_commitments (
                        sponsor_id,account_id,strategy_id,condition_id,
                        committed_amount,distributed_amount,currency,state,
                        starts_at,source_event_id
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,'ACTIVE',%s,%s)
                    """,
                    (
                        sponsor.sponsor_id,
                        sponsor.account_id,
                        sponsor.strategy_id,
                        sponsor.condition_id,
                        sponsor.committed_amount,
                        sponsor.distributed_amount,
                        sponsor.currency,
                        sponsor.starts_at,
                        sponsor.source_event_id,
                    ),
                )
            elif not self._same_sponsor(current, sponsor):
                raise ValueError("sponsorship identity collision")
            conn.commit()
        operation = sponsor.commitment_operation()
        self.cashflows.record(operation)
        return operation

    def record_sponsor_distribution(
        self, sponsor_id: str, *, amount: Decimal, effective_ts: datetime
    ) -> AccountCashflowOperation:
        value = Decimal(amount)
        if value <= 0 or effective_ts.tzinfo is None:
            raise ValueError("invalid sponsor distribution")
        day = effective_ts.astimezone(timezone.utc).date()
        identity = self._sponsor_admission_identity(sponsor_id)
        self._admit(
            request_id=(
                f"account-program:sponsor:{sponsor_id}:"
                f"distribution:{day.isoformat()}"
            ),
            operation=AdmissionOperation.RECONCILIATION,
            account_id=identity["account_id"],
            strategy_id=identity["strategy_id"],
            observed_at=effective_ts,
            condition_id=identity["condition_id"],
            metadata={
                "sponsor_id": sponsor_id,
                "action": "distribution",
                "amount": str(value),
            },
        )
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM quant.paper_sponsor_commitments "
                "WHERE sponsor_id=%s FOR UPDATE",
                (sponsor_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise LookupError("sponsorship not found")
            state = str(row["state"])
            if state not in {"ACTIVE", "CANCEL_PENDING"}:
                raise ValueError("sponsorship is not active")
            cancellation_effective = row["cancellation_effective_at"]
            if cancellation_effective is not None and effective_ts >= cancellation_effective:
                raise ValueError("sponsorship distribution is after cancellation")
            cur.execute(
                "SELECT amount FROM quant.paper_sponsor_distributions "
                "WHERE sponsor_id=%s AND distribution_date=%s",
                (sponsor_id, day),
            )
            existing = cur.fetchone()
            if existing is not None and Decimal(existing["amount"]) != value:
                raise ValueError("sponsor distribution identity collision")
            before = Decimal(row["distributed_amount"])
            operation_distributed_before = before
            if existing is None:
                if before + value + Decimal(row["refunded_amount"]) > Decimal(
                    row["committed_amount"]
                ):
                    raise ValueError("sponsor distribution exceeds commitment")
                source_event_id = f"{row['source_event_id']}:distribution:{day.isoformat()}"
                cur.execute(
                    """
                    INSERT INTO quant.paper_sponsor_distributions (
                        distribution_id,sponsor_id,distribution_date,amount,
                        effective_ts,source_event_id
                    ) VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        f"sponsor-distribution:{sponsor_id}:{day.isoformat()}",
                        sponsor_id,
                        day,
                        value,
                        effective_ts,
                        source_event_id,
                    ),
                )
                cur.execute(
                    """
                    UPDATE quant.paper_sponsor_commitments
                    SET distributed_amount=distributed_amount+%s,
                        updated_at=clock_timestamp()
                    WHERE sponsor_id=%s
                    """,
                    (value, sponsor_id),
                )
            else:
                operation_distributed_before = before - value
            conn.commit()
        sponsor = self._sponsor_from_row(
            row, distributed_amount=operation_distributed_before
        )
        operation = sponsor.distribution_operation(
            amount=value, distribution_date=effective_ts
        )
        self.cashflows.record(operation)
        return operation

    def request_sponsor_cancellation(
        self, sponsor_id: str, *, requested_at: datetime
    ) -> datetime:
        if requested_at.tzinfo is None:
            raise ValueError("sponsor cancellation timestamp must be timezone-aware")
        identity = self._sponsor_admission_identity(sponsor_id)
        self._admit(
            request_id=f"account-program:sponsor:{sponsor_id}:cancel",
            operation=AdmissionOperation.SPONSOR,
            account_id=identity["account_id"],
            strategy_id=identity["strategy_id"],
            observed_at=requested_at,
            condition_id=identity["condition_id"],
            metadata={"sponsor_id": sponsor_id, "action": "cancel"},
        )
        utc = requested_at.astimezone(timezone.utc)
        effective = datetime.combine(
            utc.date() + timedelta(days=1), time.min, tzinfo=timezone.utc
        )
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE quant.paper_sponsor_commitments
                SET state='CANCEL_PENDING',cancellation_requested_at=%s,
                    cancellation_effective_at=%s,updated_at=clock_timestamp()
                WHERE sponsor_id=%s AND state='ACTIVE'
                RETURNING sponsor_id
                """,
                (requested_at, effective, sponsor_id),
            )
            changed = cur.fetchone()
            if changed is None:
                cur.execute(
                    "SELECT state,cancellation_effective_at "
                    "FROM quant.paper_sponsor_commitments WHERE sponsor_id=%s",
                    (sponsor_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise LookupError("sponsorship not found")
                if str(row["state"]) != "CANCEL_PENDING":
                    raise ValueError("only active sponsorship can be cancelled")
                effective = row["cancellation_effective_at"]
            conn.commit()
        return effective

    def confirm_sponsor_refund(
        self,
        sponsor_id: str,
        *,
        amount: Decimal,
        confirmed_at: datetime,
        transaction_hash: str,
    ) -> AccountCashflowOperation:
        value = Decimal(amount)
        if value <= 0 or confirmed_at.tzinfo is None or not transaction_hash:
            raise ValueError("confirmed sponsor refund evidence is incomplete")
        identity = self._sponsor_admission_identity(sponsor_id)
        self._admit(
            request_id=(
                f"account-program:sponsor:{sponsor_id}:refund:{transaction_hash}:"
                f"{confirmed_at.astimezone(timezone.utc).isoformat()}"
            ),
            operation=AdmissionOperation.RECONCILIATION,
            account_id=identity["account_id"],
            strategy_id=identity["strategy_id"],
            observed_at=confirmed_at,
            condition_id=identity["condition_id"],
            metadata={
                "sponsor_id": sponsor_id,
                "action": "refund",
                "amount": str(value),
                "transaction_hash": transaction_hash,
            },
        )
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM quant.paper_sponsor_commitments "
                "WHERE sponsor_id=%s FOR UPDATE",
                (sponsor_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise LookupError("sponsorship not found")
            if str(row["state"]) == "CLOSED":
                if Decimal(row["refunded_amount"]) != value:
                    raise ValueError("sponsor refund identity collision")
            else:
                effective = row["cancellation_effective_at"]
                if str(row["state"]) != "CANCEL_PENDING" or confirmed_at < effective:
                    raise ValueError("sponsor refund is not yet payable")
                if Decimal(row["distributed_amount"]) + value != Decimal(
                    row["committed_amount"]
                ):
                    raise ValueError("sponsor refund must close the remaining commitment")
                cur.execute(
                    """
                    UPDATE quant.paper_sponsor_commitments
                    SET state='CLOSED',refunded_amount=%s,updated_at=clock_timestamp()
                    WHERE sponsor_id=%s
                    """,
                    (value, sponsor_id),
                )
            conn.commit()
        operation = _operation(
            account_id=str(row["account_id"]),
            strategy_id=str(row["strategy_id"]),
            operation_type=AccountCashflowType.SPONSOR_REFUND,
            amount=value,
            currency=str(row["currency"]),
            state=AccountCashflowState.CONFIRMED,
            effective_ts=confirmed_at,
            source="POLYMARKET_SPONSOR_PROGRAM",
            source_event_id=f"{row['source_event_id']}:refund",
            transaction_hash=transaction_hash,
            condition_id=str(row["condition_id"]),
            metadata={"sponsor_id": sponsor_id},
        )
        self.cashflows.record(operation)
        return operation

    def post_dispute(self, dispute: DisputeBond) -> AccountCashflowOperation:
        self._admit(
            request_id=f"account-program:dispute:{dispute.dispute_id}:post",
            operation=AdmissionOperation.DISPUTE,
            account_id=dispute.account_id,
            strategy_id=dispute.strategy_id,
            observed_at=dispute.posted_at,
            condition_id=dispute.condition_id,
            metadata={"dispute_id": dispute.dispute_id},
        )
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.paper_dispute_bonds (
                    dispute_id,account_id,strategy_id,condition_id,bond_amount,
                    currency,state,posted_at,source_event_id
                ) VALUES (%s,%s,%s,%s,%s,%s,'POSTED',%s,%s)
                ON CONFLICT (dispute_id) DO NOTHING RETURNING dispute_id
                """,
                (
                    dispute.dispute_id,
                    dispute.account_id,
                    dispute.strategy_id,
                    dispute.condition_id,
                    dispute.bond_amount,
                    dispute.currency,
                    dispute.posted_at,
                    dispute.source_event_id,
                ),
            )
            inserted = cur.fetchone() is not None
            if not inserted:
                cur.execute(
                    "SELECT * FROM quant.paper_dispute_bonds WHERE dispute_id=%s",
                    (dispute.dispute_id,),
                )
                row = cur.fetchone()
                if row is None or not self._same_dispute(row, dispute):
                    raise ValueError("dispute identity collision")
            conn.commit()
        operation = dispute.post_operation()
        self.cashflows.record(operation)
        return operation

    def resolve_dispute(
        self,
        dispute: DisputeBond,
        *,
        outcome: DisputeOutcome,
        resolved_at: datetime,
        transaction_hash: str,
        bounty_amount: Decimal = Decimal(0),
    ) -> tuple[AccountCashflowOperation, ...]:
        if not transaction_hash:
            raise ValueError("dispute resolution requires transaction hash")
        bounty = Decimal(bounty_amount)
        self._admit(
            request_id=(
                f"account-program:dispute:{dispute.dispute_id}:resolve:"
                f"{outcome.value}"
            ),
            operation=AdmissionOperation.RECONCILIATION,
            account_id=dispute.account_id,
            strategy_id=dispute.strategy_id,
            observed_at=resolved_at,
            condition_id=dispute.condition_id,
            metadata={"dispute_id": dispute.dispute_id, "outcome": outcome.value},
        )
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM quant.paper_dispute_bonds "
                "WHERE dispute_id=%s FOR UPDATE",
                (dispute.dispute_id,),
            )
            row = cur.fetchone()
            if row is None or not self._same_dispute(row, dispute):
                raise LookupError("dispute bond not found")
            old_state = str(row["state"])
            if old_state != "POSTED" and old_state != outcome.value:
                raise ValueError("dispute terminal outcome cannot change")
            if old_state == outcome.value and (
                Decimal(row["bounty_amount"]) != bounty
                or str(row["transaction_hash"] or "") != transaction_hash
            ):
                raise ValueError("dispute resolution identity collision")
            if old_state == "POSTED":
                cur.execute(
                    """
                    UPDATE quant.paper_dispute_bonds
                    SET state=%s,bounty_amount=%s,resolved_at=%s,
                        transaction_hash=%s,updated_at=clock_timestamp()
                    WHERE dispute_id=%s
                    """,
                    (
                        outcome.value,
                        bounty,
                        resolved_at,
                        transaction_hash,
                        dispute.dispute_id,
                    ),
                )
            conn.commit()
        operations = dispute.resolution_operations(
            outcome=outcome,
            resolved_at=resolved_at,
            bounty_amount=bounty,
            transaction_hash=transaction_hash,
        )
        for operation in operations:
            self.cashflows.record(operation)
        return operations

    def _admit(
        self,
        *,
        request_id: str,
        operation: AdmissionOperation,
        account_id: str,
        strategy_id: str,
        observed_at: datetime,
        condition_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        decision = self.admission_service.decide(
            AdmissionRequest(
                request_id=request_id,
                operation=operation,
                account_id=account_id,
                strategy_id=strategy_id,
                condition_id=condition_id,
                exposure_effect=ExposureEffect.NEUTRAL,
                observed_at=observed_at,
                metadata=metadata or {},
            )
        )
        if not decision.allowed:
            raise ValueError(
                "account program rejected by unified admission: "
                + ",".join(decision.reason_codes)
            )

    def _sponsor_admission_identity(self, sponsor_id: str) -> dict[str, str]:
        with self.connection_factory(readonly=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT account_id,strategy_id,condition_id "
                "FROM quant.paper_sponsor_commitments WHERE sponsor_id=%s",
                (sponsor_id,),
            )
            row = cur.fetchone()
        if row is None:
            raise LookupError("sponsorship not found")
        return {
            "account_id": str(row["account_id"]),
            "strategy_id": str(row["strategy_id"]),
            "condition_id": str(row["condition_id"]),
        }

    @staticmethod
    def _assert_bridge_identity(row: Any, transfer: BridgeTransfer) -> None:
        if (
            str(row["account_id"]) != transfer.account_id
            or str(row["strategy_id"]) != transfer.strategy_id
            or str(row["direction"]) != transfer.direction.value
            or Decimal(row["amount"]) != Decimal(transfer.amount)
            or Decimal(row["fee"]) != Decimal(transfer.fee)
            or str(row["currency"]) != transfer.currency
            or str(row["source_event_id"]) != transfer.source_event_id
        ):
            raise ValueError("bridge transfer identity collision")

    @staticmethod
    def _same_sponsor(row: Any, sponsor: SponsorCommitment) -> bool:
        return (
            str(row["account_id"]) == sponsor.account_id
            and str(row["strategy_id"]) == sponsor.strategy_id
            and str(row["condition_id"]) == sponsor.condition_id
            and Decimal(row["committed_amount"]) == Decimal(sponsor.committed_amount)
            and str(row["currency"]) == sponsor.currency
            and str(row["source_event_id"]) == sponsor.source_event_id
        )

    @staticmethod
    def _same_dispute(row: Any, dispute: DisputeBond) -> bool:
        return (
            str(row["account_id"]) == dispute.account_id
            and str(row["strategy_id"]) == dispute.strategy_id
            and str(row["condition_id"]) == dispute.condition_id
            and Decimal(row["bond_amount"]) == Decimal(dispute.bond_amount)
            and str(row["currency"]) == dispute.currency
            and str(row["source_event_id"]) == dispute.source_event_id
        )

    @staticmethod
    def _sponsor_from_row(
        row: Any, *, distributed_amount: Decimal
    ) -> SponsorCommitment:
        return SponsorCommitment(
            sponsor_id=str(row["sponsor_id"]),
            account_id=str(row["account_id"]),
            strategy_id=str(row["strategy_id"]),
            condition_id=str(row["condition_id"]),
            committed_amount=Decimal(row["committed_amount"]),
            currency=str(row["currency"]),
            starts_at=row["starts_at"],
            source_event_id=str(row["source_event_id"]),
            distributed_amount=distributed_amount,
        )


def _operation(
    *,
    account_id: str,
    strategy_id: str,
    operation_type: AccountCashflowType,
    amount: Decimal,
    currency: str,
    state: AccountCashflowState,
    effective_ts: datetime,
    source: str,
    source_event_id: str,
    transaction_hash: str | None = None,
    raw_payload_hash: str | None = None,
    condition_id: str | None = None,
    metadata: dict | None = None,
) -> AccountCashflowOperation:
    operation_id = account_cashflow_operation_id(
        source, source_event_id, operation_type
    )
    return AccountCashflowOperation(
        operation_id=operation_id,
        account_id=account_id,
        strategy_id=strategy_id,
        operation_type=operation_type,
        amount=amount,
        currency=currency,
        state=state,
        effective_ts=effective_ts,
        source=source,
        source_event_id=source_event_id,
        idempotency_key=f"account-program:{operation_id}",
        condition_id=condition_id,
        source_tx_hash=transaction_hash,
        raw_payload_hash=raw_payload_hash,
        metadata=metadata or {},
    )
