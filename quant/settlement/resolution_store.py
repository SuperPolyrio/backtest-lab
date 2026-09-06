"""Durable oracle lifecycle with atomic paper receivable and redeem accounting."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import AbstractContextManager
from decimal import Decimal
from typing import Any

from quant.core.db import postgres_connection
from quant.simulator.complete_set import (
    COMPLETE_SET_SCHEMA_STATEMENTS,
    CompleteSetLotStore,
)

from .oracle_state import OracleResolutionState, ResolutionPhase
from .payout_vector import PayoutVector

SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS quant.market_resolution_states (
        condition_id TEXT PRIMARY KEY,
        phase TEXT NOT NULL,
        expected_resolution_at TIMESTAMPTZ,
        trading_stopped_at TIMESTAMPTZ,
        actual_finalized_at TIMESTAMPTZ,
        redeemed_at TIMESTAMPTZ,
        capital_locked_seconds BIGINT,
        truth_hash TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
    "ALTER TABLE quant.market_resolution_states ADD COLUMN IF NOT EXISTS redeem_started_at TIMESTAMPTZ",
    "ALTER TABLE quant.market_resolution_states ADD COLUMN IF NOT EXISTS proposal_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE quant.market_resolution_states ADD COLUMN IF NOT EXISTS dispute_round INTEGER NOT NULL DEFAULT 0",
    """
    CREATE TABLE IF NOT EXISTS quant.market_resolution_events (
        event_id TEXT PRIMARY KEY,
        condition_id TEXT NOT NULL,
        from_phase TEXT,
        to_phase TEXT NOT NULL,
        event_ts TIMESTAMPTZ NOT NULL,
        proposal_count INTEGER NOT NULL,
        dispute_round INTEGER NOT NULL,
        truth_hash TEXT,
        source TEXT NOT NULL,
        reason TEXT NOT NULL DEFAULT '',
        payload JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS market_resolution_events_condition_idx
        ON quant.market_resolution_events (condition_id,event_ts,created_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.market_settlement_payouts (
        market_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        asset_id TEXT NOT NULL,
        payout_per_share NUMERIC NOT NULL CHECK (
            payout_per_share >= 0 AND payout_per_share <= 1
        ),
        resolution_source TEXT NOT NULL,
        oracle_finalized_at TIMESTAMPTZ NOT NULL,
        truth_hash TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (condition_id,asset_id,truth_hash)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.paper_settlement_receivables (
        receivable_key TEXT PRIMARY KEY,
        strategy_id TEXT NOT NULL,
        market_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        asset_id TEXT NOT NULL,
        truth_hash TEXT NOT NULL,
        quantity NUMERIC NOT NULL,
        cost_basis NUMERIC NOT NULL,
        payout_per_share NUMERIC NOT NULL,
        expected_payout NUMERIC NOT NULL,
        expected_realized_pnl NUMERIC NOT NULL,
        state TEXT NOT NULL,
        cash_applied BOOLEAN NOT NULL DEFAULT FALSE,
        accrued_at TIMESTAMPTZ NOT NULL,
        redeem_started_at TIMESTAMPTZ,
        redeemed_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        UNIQUE (strategy_id,asset_id,truth_hash)
    )
    """,
    "ALTER TABLE quant.paper_settlements ADD COLUMN IF NOT EXISTS truth_hash TEXT",
    "ALTER TABLE quant.paper_settlements ADD COLUMN IF NOT EXISTS lifecycle_event_id TEXT",
    "ALTER TABLE quant.paper_settlements ADD COLUMN IF NOT EXISTS accounting_status TEXT NOT NULL DEFAULT 'CASH_APPLIED'",
)


class PostgresResolutionLifecycleStore:
    """Single-transaction resolution truth and paper redemption accounting."""

    def __init__(
        self,
        connection_factory: Callable[..., AbstractContextManager[Any]] = (
            postgres_connection
        ),
    ) -> None:
        self.connection_factory = connection_factory

    def ensure_schema(self) -> None:
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            for statement in SCHEMA_STATEMENTS:
                cur.execute(statement)
            for statement in COMPLETE_SET_SCHEMA_STATEMENTS:
                cur.execute(statement)

    def initialize(
        self,
        state: OracleResolutionState,
        *,
        event_id: str,
        source: str,
        reason: str = "trading_stopped",
        payload: dict[str, Any] | None = None,
    ) -> OracleResolutionState:
        if state.phase is not ResolutionPhase.TRADING_STOPPED:
            raise ValueError("durable resolution must initialize at TRADING_STOPPED")
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            self._lock(cur, state.condition_id)
            current = self._state(cur, state.condition_id, lock=True)
            if current is not None:
                return current
            self._upsert_state(
                cur, state, truth_hash=None, observed_at=state.trading_stopped_at
            )
            self._append_event(
                cur,
                event_id=event_id,
                condition_id=state.condition_id,
                from_phase=None,
                state=state,
                event_ts=state.trading_stopped_at,
                truth_hash=None,
                source=source,
                reason=reason,
                payload=payload,
            )
            return state

    def transition(
        self,
        condition_id: str,
        target: ResolutionPhase,
        *,
        event_id: str,
        event_ts: Any,
        source: str,
        reason: str = "",
        payout_vector: PayoutVector | None = None,
        market_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> OracleResolutionState:
        target = ResolutionPhase(target)
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            self._lock(cur, condition_id)
            current = self._require_state(cur, condition_id, lock=True)
            existing = self._event(cur, event_id)
            if existing is not None:
                if (
                    str(existing["condition_id"]) != str(condition_id)
                    or str(existing["to_phase"]) != target.value
                ):
                    raise ValueError("resolution event id collision")
                return current
            if current.phase is target:
                return current

            final_target = target in {
                ResolutionPhase.RESOLUTION_FINAL,
                ResolutionPhase.FINALIZED,
            }
            if final_target:
                if payout_vector is None or not str(market_id or "").strip():
                    raise ValueError(
                        "final resolution requires market_id and complete payout vector"
                    )
                if payout_vector.condition_id != condition_id:
                    raise ValueError("payout vector condition does not match lifecycle")
            elif payout_vector is not None:
                raise ValueError(
                    "payout vector may only be attached at final resolution"
                )

            next_state = current.transition(target, at=event_ts)
            truth_hash = self._truth_hash(cur, condition_id)
            if payout_vector is not None:
                truth_hash = payout_vector.truth_hash
                self._persist_payout_vector(cur, str(market_id), payout_vector)
            if (
                target
                in {
                    ResolutionPhase.REDEEMABLE,
                    ResolutionPhase.REDEEMING,
                    ResolutionPhase.REDEEMED,
                }
                and not truth_hash
            ):
                raise ValueError("redeem lifecycle requires persisted payout truth")

            self._append_event(
                cur,
                event_id=event_id,
                condition_id=condition_id,
                from_phase=current.phase,
                state=next_state,
                event_ts=event_ts,
                truth_hash=truth_hash,
                source=source,
                reason=reason,
                payload=payload,
            )
            self._upsert_state(
                cur,
                next_state,
                truth_hash=truth_hash,
                observed_at=event_ts,
            )
            if target is ResolutionPhase.REDEEMABLE:
                cur.execute(
                    """
                    UPDATE quant.paper_settlement_receivables
                    SET state='REDEEMABLE',updated_at=clock_timestamp()
                    WHERE condition_id=%s AND truth_hash=%s
                      AND state='REDEEMING' AND cash_applied=FALSE
                    """,
                    (condition_id, truth_hash),
                )
                self._accrue_receivables(cur, condition_id, truth_hash, event_ts)
            elif target is ResolutionPhase.REDEEMING:
                cur.execute(
                    """
                    UPDATE quant.paper_settlement_receivables
                    SET state='REDEEMING',redeem_started_at=%s,
                        updated_at=clock_timestamp()
                    WHERE condition_id=%s AND truth_hash=%s
                      AND state='REDEEMABLE' AND cash_applied=FALSE
                    """,
                    (event_ts, condition_id, truth_hash),
                )
            elif target is ResolutionPhase.REDEEMED:
                self._apply_redeemed_cash(
                    cur,
                    condition_id=condition_id,
                    truth_hash=str(truth_hash),
                    event_id=event_id,
                    event_ts=event_ts,
                )
            return next_state

    def state(self, condition_id: str) -> OracleResolutionState | None:
        with self.connection_factory(readonly=True) as conn, conn.cursor() as cur:
            return self._state(cur, condition_id, lock=False)

    def events(self, condition_id: str) -> tuple[dict[str, Any], ...]:
        with self.connection_factory(readonly=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM quant.market_resolution_events
                WHERE condition_id=%s ORDER BY event_ts,created_at,event_id
                """,
                (condition_id,),
            )
            return tuple(dict(row) for row in cur.fetchall())

    def receivables(self, condition_id: str) -> tuple[dict[str, Any], ...]:
        with self.connection_factory(readonly=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM quant.paper_settlement_receivables
                WHERE condition_id=%s ORDER BY strategy_id,asset_id
                """,
                (condition_id,),
            )
            return tuple(dict(row) for row in cur.fetchall())

    @staticmethod
    def _lock(cur: Any, condition_id: str) -> None:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"paper-resolution:{condition_id}",),
        )

    def _require_state(
        self, cur: Any, condition_id: str, *, lock: bool
    ) -> OracleResolutionState:
        state = self._state(cur, condition_id, lock=lock)
        if state is None:
            raise KeyError(f"unknown resolution condition: {condition_id}")
        return state

    @staticmethod
    def _state(
        cur: Any, condition_id: str, *, lock: bool
    ) -> OracleResolutionState | None:
        cur.execute(
            "SELECT * FROM quant.market_resolution_states WHERE condition_id=%s"
            + (" FOR UPDATE" if lock else ""),
            (condition_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return OracleResolutionState(
            condition_id=str(row["condition_id"]),
            phase=ResolutionPhase(str(row["phase"])),
            trading_stopped_at=row["trading_stopped_at"],
            expected_resolution_at=row.get("expected_resolution_at"),
            actual_finalized_at=row.get("actual_finalized_at"),
            redeemed_at=row.get("redeemed_at"),
            redeem_started_at=row.get("redeem_started_at"),
            proposal_count=int(row.get("proposal_count") or 0),
            dispute_round=int(row.get("dispute_round") or 0),
        )

    @staticmethod
    def _upsert_state(
        cur: Any,
        state: OracleResolutionState,
        *,
        truth_hash: str | None,
        observed_at: Any,
    ) -> None:
        cur.execute(
            """
            INSERT INTO quant.market_resolution_states (
                condition_id,phase,expected_resolution_at,trading_stopped_at,
                actual_finalized_at,redeemed_at,redeem_started_at,
                capital_locked_seconds,truth_hash,proposal_count,dispute_round,
                updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp())
            ON CONFLICT (condition_id) DO UPDATE SET
                phase=EXCLUDED.phase,
                expected_resolution_at=EXCLUDED.expected_resolution_at,
                trading_stopped_at=EXCLUDED.trading_stopped_at,
                actual_finalized_at=EXCLUDED.actual_finalized_at,
                redeemed_at=EXCLUDED.redeemed_at,
                redeem_started_at=EXCLUDED.redeem_started_at,
                capital_locked_seconds=EXCLUDED.capital_locked_seconds,
                truth_hash=COALESCE(EXCLUDED.truth_hash,
                                    quant.market_resolution_states.truth_hash),
                proposal_count=EXCLUDED.proposal_count,
                dispute_round=EXCLUDED.dispute_round,
                updated_at=clock_timestamp()
            """,
            (
                state.condition_id,
                state.phase.value,
                state.expected_resolution_at,
                state.trading_stopped_at,
                state.actual_finalized_at,
                state.redeemed_at,
                state.redeem_started_at,
                state.capital_locked_seconds(now=observed_at),
                truth_hash,
                state.proposal_count,
                state.dispute_round,
            ),
        )

    @staticmethod
    def _event(cur: Any, event_id: str) -> Any | None:
        cur.execute(
            "SELECT * FROM quant.market_resolution_events WHERE event_id=%s",
            (event_id,),
        )
        return cur.fetchone()

    def _append_event(
        self,
        cur: Any,
        *,
        event_id: str,
        condition_id: str,
        from_phase: ResolutionPhase | None,
        state: OracleResolutionState,
        event_ts: Any,
        truth_hash: str | None,
        source: str,
        reason: str,
        payload: dict[str, Any] | None,
    ) -> None:
        cur.execute(
            """
            INSERT INTO quant.market_resolution_events (
                event_id,condition_id,from_phase,to_phase,event_ts,
                proposal_count,dispute_round,truth_hash,source,reason,payload
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            ON CONFLICT (event_id) DO NOTHING RETURNING event_id
            """,
            (
                event_id,
                condition_id,
                from_phase.value if from_phase else None,
                state.phase.value,
                event_ts,
                state.proposal_count,
                state.dispute_round,
                truth_hash,
                source,
                reason,
                json.dumps(payload or {}, sort_keys=True, default=str),
            ),
        )
        if cur.fetchone() is not None:
            return
        existing = self._event(cur, event_id)
        if existing is None or (
            str(existing["condition_id"]) != condition_id
            or str(existing["to_phase"]) != state.phase.value
        ):
            raise ValueError("resolution event id collision")

    @staticmethod
    def _truth_hash(cur: Any, condition_id: str) -> str | None:
        cur.execute(
            "SELECT truth_hash FROM quant.market_resolution_states WHERE condition_id=%s",
            (condition_id,),
        )
        row = cur.fetchone()
        return str(row["truth_hash"]) if row and row.get("truth_hash") else None

    @staticmethod
    def _persist_payout_vector(
        cur: Any, market_id: str, payout_vector: PayoutVector
    ) -> None:
        finalized_at = payout_vector.oracle_finalized_at
        for asset_id, payout in payout_vector.payouts.items():
            cur.execute(
                """
                INSERT INTO quant.market_settlement_payouts (
                    market_id,condition_id,asset_id,payout_per_share,
                    resolution_source,oracle_finalized_at,truth_hash
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (condition_id,asset_id,truth_hash) DO NOTHING
                """,
                (
                    market_id,
                    payout_vector.condition_id,
                    str(asset_id),
                    Decimal(payout),
                    payout_vector.resolution_source,
                    finalized_at,
                    payout_vector.truth_hash,
                ),
            )

    @staticmethod
    def _accrue_receivables(
        cur: Any, condition_id: str, truth_hash: str, event_ts: Any
    ) -> None:
        cur.execute(
            """
            SELECT p.strategy_id,p.market_id,p.asset_id,p.quantity,
                   p.reserved_quantity,p.cost_basis,
                   payout.payout_per_share
            FROM quant.paper_positions p
            JOIN quant.market_settlement_payouts payout
              ON payout.condition_id=p.condition_id
             AND payout.asset_id=p.asset_id
             AND payout.truth_hash=%s
            WHERE p.condition_id=%s AND p.quantity>0
            ORDER BY p.strategy_id,p.asset_id
            FOR UPDATE OF p
            """,
            (truth_hash, condition_id),
        )
        for row in cur.fetchall():
            if Decimal(row["reserved_quantity"]) > 0:
                raise ValueError("paper redeem position remains reserved")
            quantity = Decimal(row["quantity"])
            cost_basis = Decimal(row["cost_basis"])
            payout_per_share = Decimal(row["payout_per_share"])
            expected_payout = quantity * payout_per_share
            expected_realized = expected_payout - cost_basis
            key = (
                f"resolution-receivable:{row['strategy_id']}:"
                f"{row['asset_id']}:{truth_hash}"
            )
            cur.execute(
                """
                INSERT INTO quant.paper_settlement_receivables (
                    receivable_key,strategy_id,market_id,condition_id,asset_id,
                    truth_hash,quantity,cost_basis,payout_per_share,
                    expected_payout,expected_realized_pnl,state,accrued_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'REDEEMABLE',%s)
                ON CONFLICT (receivable_key) DO NOTHING
                """,
                (
                    key,
                    row["strategy_id"],
                    row["market_id"],
                    condition_id,
                    row["asset_id"],
                    truth_hash,
                    quantity,
                    cost_basis,
                    payout_per_share,
                    expected_payout,
                    expected_realized,
                    event_ts,
                ),
            )

    @staticmethod
    def _apply_redeemed_cash(
        cur: Any,
        *,
        condition_id: str,
        truth_hash: str,
        event_id: str,
        event_ts: Any,
    ) -> None:
        cur.execute(
            """
            SELECT * FROM quant.paper_settlement_receivables
            WHERE condition_id=%s AND truth_hash=%s
              AND state IN ('REDEEMABLE','REDEEMING') AND cash_applied=FALSE
            ORDER BY strategy_id,asset_id
            FOR UPDATE
            """,
            (condition_id, truth_hash),
        )
        for receivable in cur.fetchall():
            strategy_id = str(receivable["strategy_id"])
            asset_id = str(receivable["asset_id"])
            cur.execute(
                """
                SELECT a.cash_balance,p.quantity,p.reserved_quantity,
                       p.cost_basis,p.realized_pnl,p.market_id,p.condition_id
                FROM quant.paper_accounts a
                JOIN quant.paper_positions p ON p.strategy_id=a.strategy_id
                WHERE a.strategy_id=%s AND p.asset_id=%s
                FOR UPDATE OF a,p
                """,
                (strategy_id, asset_id),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError("paper redeem position disappeared before cash apply")
            if Decimal(row["reserved_quantity"]) > 0:
                raise ValueError("paper redeem position remains reserved")
            quantity = Decimal(row["quantity"])
            cost_basis = Decimal(row["cost_basis"])
            if quantity != Decimal(receivable["quantity"]) or cost_basis != Decimal(
                receivable["cost_basis"]
            ):
                raise ValueError(
                    "paper redeem position changed after receivable accrual"
                )
            payout = Decimal(receivable["expected_payout"])
            realized = Decimal(receivable["expected_realized_pnl"])
            cash_after = Decimal(row["cash_balance"]) + payout
            realized_after = Decimal(row["realized_pnl"]) + realized
            settlement_key = (
                f"resolution-redeemed:{strategy_id}:{asset_id}:{truth_hash}"
            )
            cur.execute(
                """
                INSERT INTO quant.paper_settlements (
                    settlement_key,strategy_id,market_id,condition_id,asset_id,
                    winning_asset_id,quantity,payout_per_share,cash_delta,
                    realized_pnl_delta,resolution_source,resolved_at,truth_hash,
                    lifecycle_event_id,accounting_status
                )
                SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                       payout.resolution_source,%s,%s,%s,'CASH_APPLIED'
                FROM quant.market_settlement_payouts payout
                WHERE payout.condition_id=%s AND payout.asset_id=%s
                  AND payout.truth_hash=%s
                ON CONFLICT (settlement_key) DO NOTHING
                RETURNING settlement_key
                """,
                (
                    settlement_key,
                    strategy_id,
                    receivable["market_id"],
                    condition_id,
                    asset_id,
                    asset_id,
                    quantity,
                    receivable["payout_per_share"],
                    payout,
                    realized,
                    event_ts,
                    truth_hash,
                    event_id,
                    condition_id,
                    asset_id,
                    truth_hash,
                ),
            )
            if cur.fetchone() is None:
                raise ValueError("paper redeem settlement key already exists")
            CompleteSetLotStore.ensure_position_coverage(
                cur,
                strategy_id=strategy_id,
                market_id=str(row["market_id"]),
                condition_id=str(row["condition_id"]),
                asset_id=asset_id,
                quantity=quantity,
                cost_basis=cost_basis,
                observed_at=event_ts,
            )
            CompleteSetLotStore.consume_assets(
                cur,
                consumption_id=f"resolution-redeem-consumption:{settlement_key}",
                strategy_id=strategy_id,
                market_id=str(row["market_id"]),
                condition_id=str(row["condition_id"]),
                consumer_type="RESOLUTION_REDEEM",
                consumer_ref=settlement_key,
                quantities={asset_id: quantity},
                expected_basis_by_asset={asset_id: cost_basis},
                cash_delta=payout,
                realized_pnl_delta=realized,
                consumed_at=event_ts,
                metadata={
                    "truth_hash": truth_hash,
                    "lifecycle_event_id": event_id,
                },
            )
            cur.execute(
                """
                UPDATE quant.paper_accounts
                SET cash_balance=%s,realized_pnl=realized_pnl+%s,
                    updated_at=clock_timestamp()
                WHERE strategy_id=%s
                """,
                (cash_after, realized, strategy_id),
            )
            cur.execute(
                """
                UPDATE quant.paper_positions
                SET quantity=0,cost_basis=0,realized_pnl=%s,settled_at=%s,
                    updated_at=clock_timestamp()
                WHERE strategy_id=%s AND asset_id=%s
                """,
                (realized_after, event_ts, strategy_id, asset_id),
            )
            cur.execute(
                """
                INSERT INTO quant.paper_ledger_entries (
                    idempotency_key,strategy_id,event_type,market_id,condition_id,
                    asset_id,event_ts,price,shares_delta,cash_delta,
                    realized_pnl_delta,cash_after,position_after,cost_basis_after,
                    metadata
                ) VALUES (%s,%s,'REDEEMED_SETTLEMENT',%s,%s,%s,%s,%s,%s,%s,
                          %s,%s,0,0,%s::jsonb)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (
                    settlement_key,
                    strategy_id,
                    receivable["market_id"],
                    condition_id,
                    asset_id,
                    event_ts,
                    receivable["payout_per_share"],
                    -quantity,
                    payout,
                    realized,
                    cash_after,
                    json.dumps(
                        {
                            "truth_hash": truth_hash,
                            "lifecycle_event_id": event_id,
                            "receivable_key": receivable["receivable_key"],
                        },
                        sort_keys=True,
                    ),
                ),
            )
            cur.execute(
                """
                UPDATE quant.paper_settlement_receivables
                SET state='REDEEMED',cash_applied=TRUE,redeemed_at=%s,
                    updated_at=clock_timestamp()
                WHERE receivable_key=%s
                """,
                (event_ts, receivable["receivable_key"]),
            )
