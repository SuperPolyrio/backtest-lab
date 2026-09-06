"""Confirmed and estimated paper account return decomposition."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

ACCOUNT_RETURN_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS quant.paper_account_return_reports (
        report_id TEXT PRIMARY KEY,
        strategy_id TEXT NOT NULL,
        account_id TEXT NOT NULL,
        as_of TIMESTAMPTZ NOT NULL,
        model_version TEXT NOT NULL,
        report_hash TEXT NOT NULL,
        gross_execution_pnl NUMERIC NOT NULL,
        platform_taker_fee_paid NUMERIC NOT NULL,
        builder_fee_paid NUMERIC NOT NULL,
        net_trading_pnl NUMERIC NOT NULL,
        split_merge_realized_pnl NUMERIC NOT NULL,
        settlement_realized_pnl NUMERIC NOT NULL,
        confirmed_account_return NUMERIC NOT NULL,
        estimated_account_return NUMERIC NOT NULL,
        report JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        UNIQUE (strategy_id,account_id,as_of,model_version)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS paper_account_return_reports_strategy_idx
    ON quant.paper_account_return_reports (strategy_id,as_of DESC)
    """,
    """
    ALTER TABLE quant.paper_account_return_reports
    ADD COLUMN IF NOT EXISTS confirmed_external_cashflow_return NUMERIC NOT NULL DEFAULT 0
    """,
    """
    ALTER TABLE quant.paper_account_return_reports
    ADD COLUMN IF NOT EXISTS capital_flows JSONB NOT NULL DEFAULT '{}'::jsonb
    """,
)

CONFIRMED_FINALITY = "CONFIRMED_FINAL"
PROVISIONAL_FINALITY = frozenset({"MATCHED_PROVISIONAL", "RETRYING", "UNKNOWN"})
FAILED_FINALITY = frozenset({"FAILED_FINAL", "VOIDED", "REVERSAL_APPLIED"})
PRIMARY_REWARD_TYPES = (
    "MAKER_REBATE",
    "TAKER_REBATE",
    "LIQUIDITY_REWARD",
    "HOLDING_REWARD",
)
CAPITAL_FLOW_TYPES = frozenset(
    {
        "DEPOSIT",
        "WITHDRAWAL",
        "BRIDGE_DEPOSIT",
        "BRIDGE_WITHDRAWAL",
        "SPONSOR_COMMITMENT",
        "SPONSOR_REFUND",
        "DISPUTE_BOND",
        "DISPUTE_BOND_RETURN",
    }
)
EXTERNAL_RETURN_TYPES = frozenset(
    {
        "BRIDGE_FEE",
        "SPONSOR_DISTRIBUTION",
        "DISPUTE_BOUNTY",
        "DISPUTE_BOND_LOSS",
    }
)


@dataclass(frozen=True)
class TradeReturnEvent:
    event_id: str
    net_realized_pnl: Decimal
    platform_fee: Decimal
    builder_fee: Decimal
    finality_state: str


@dataclass(frozen=True)
class OperationReturnEvent:
    event_id: str
    category: str
    realized_pnl: Decimal
    confirmed: bool = True


@dataclass(frozen=True)
class RewardReturnEvent:
    event_id: str
    reward_type: str
    modeled_amount: Decimal = Decimal(0)
    allocated_to_received: Decimal = Decimal(0)
    received_amount: Decimal = Decimal(0)
    clawback_received: Decimal = Decimal(0)


@dataclass(frozen=True)
class AccountCashflowEvent:
    event_id: str
    event_type: str
    amount: Decimal
    status: str


@dataclass(frozen=True)
class AccountReturnReport:
    report_id: str
    strategy_id: str
    account_id: str
    as_of: datetime
    model_version: str
    gross_execution_pnl: Decimal
    platform_taker_fee_paid: Decimal
    builder_fee_paid: Decimal
    net_trading_pnl: Decimal
    provisional_net_trading_pnl: Decimal
    failed_or_voided_net_trading_pnl: Decimal
    split_merge_realized_pnl: Decimal
    settlement_realized_pnl: Decimal
    confirmed_operation_costs: Decimal
    confirmed_external_cashflow_return: Decimal
    capital_flows: dict[str, Decimal]
    reward_estimated: dict[str, Decimal]
    reward_received: dict[str, Decimal]
    confirmed_account_return: Decimal
    estimated_account_return: Decimal
    reward_coverage: dict[str, str]
    unmodeled_cashflows: tuple[dict[str, str], ...]
    source_counts: dict[str, int]
    report_hash: str

    @property
    def trading_only(self) -> Decimal:
        return self.net_trading_pnl


def build_account_return_report(
    *,
    strategy_id: str,
    account_id: str,
    as_of: datetime,
    trades: Iterable[TradeReturnEvent] = (),
    operations: Iterable[OperationReturnEvent] = (),
    rewards: Iterable[RewardReturnEvent] = (),
    cashflows: Iterable[AccountCashflowEvent] = (),
    model_version: str = "account-return-v1",
) -> AccountReturnReport:
    if as_of.tzinfo is None:
        raise ValueError("account return as_of must be timezone-aware")
    trade_rows = tuple(trades)
    operation_rows = tuple(operations)
    reward_rows = tuple(rewards)
    cashflow_rows = tuple(cashflows)
    if len({row.event_id for row in trade_rows}) != len(trade_rows):
        raise ValueError("duplicate account-return trade event")
    confirmed_trades = tuple(
        row for row in trade_rows if row.finality_state == CONFIRMED_FINALITY
    )
    provisional_trades = tuple(
        row for row in trade_rows if row.finality_state in PROVISIONAL_FINALITY
    )
    failed_trades = tuple(
        row for row in trade_rows if row.finality_state in FAILED_FINALITY
    )
    platform_fee = sum((row.platform_fee for row in confirmed_trades), Decimal(0))
    builder_fee = sum((row.builder_fee for row in confirmed_trades), Decimal(0))
    net_trading = sum((row.net_realized_pnl for row in confirmed_trades), Decimal(0))
    gross_execution = net_trading + platform_fee + builder_fee
    provisional_net = sum(
        (row.net_realized_pnl for row in provisional_trades), Decimal(0)
    )
    failed_net = sum((row.net_realized_pnl for row in failed_trades), Decimal(0))
    split_merge = sum(
        (
            row.realized_pnl
            for row in operation_rows
            if row.confirmed and row.category in {"SPLIT", "MERGE", "CONVERSION"}
        ),
        Decimal(0),
    )
    settlement = sum(
        (
            row.realized_pnl
            for row in operation_rows
            if row.confirmed and row.category in {"SETTLEMENT", "REDEEM"}
        ),
        Decimal(0),
    )
    estimated_by_type: dict[str, Decimal] = defaultdict(Decimal)
    received_by_type: dict[str, Decimal] = defaultdict(Decimal)
    for row in reward_rows:
        key = str(getattr(row.reward_type, "value", row.reward_type))
        outstanding = max(Decimal(0), row.modeled_amount - row.allocated_to_received)
        estimated_by_type[key] += outstanding
        received_by_type[key] += row.received_amount - row.clawback_received
    operation_costs = sum(
        (
            abs(row.amount)
            for row in cashflow_rows
            if row.event_type == "OPERATION_COST"
            and row.status in {"RECEIVED", "CONFIRMED"}
        ),
        Decimal(0),
    )
    external_cashflow_return = sum(
        (
            row.amount
            for row in cashflow_rows
            if row.event_type in EXTERNAL_RETURN_TYPES
            and row.status in {"RECEIVED", "CONFIRMED"}
        ),
        Decimal(0),
    )
    capital_flows: dict[str, Decimal] = defaultdict(Decimal)
    for row in cashflow_rows:
        if (
            row.event_type in CAPITAL_FLOW_TYPES
            and row.status in {"RECEIVED", "CONFIRMED"}
        ):
            capital_flows[row.event_type] += row.amount
    unmodeled = tuple(
        {
            "event_id": row.event_id,
            "event_type": row.event_type,
            "amount": str(row.amount),
            "status": row.status,
        }
        for row in sorted(cashflow_rows, key=lambda item: item.event_id)
        if row.event_type
        not in {"OPERATION_COST", *CAPITAL_FLOW_TYPES, *EXTERNAL_RETURN_TYPES}
    )
    confirmed_rewards = sum(received_by_type.values(), Decimal(0))
    unconfirmed_estimates = sum(estimated_by_type.values(), Decimal(0))
    confirmed = (
        net_trading
        + split_merge
        + settlement
        + confirmed_rewards
        - operation_costs
        + external_cashflow_return
    )
    estimated = confirmed + unconfirmed_estimates
    coverage = {}
    for key in PRIMARY_REWARD_TYPES:
        if received_by_type[key] != 0:
            coverage[key] = "OFFICIAL_RECEIVED"
        elif estimated_by_type[key] != 0:
            coverage[key] = "MODEL_ESTIMATED_ONLY"
        else:
            coverage[key] = "NO_EVIDENCE"
    payload = {
        "strategy_id": strategy_id,
        "account_id": account_id,
        "as_of": as_of.isoformat(),
        "model_version": model_version,
        "gross_execution_pnl": str(gross_execution),
        "platform_taker_fee_paid": str(platform_fee),
        "builder_fee_paid": str(builder_fee),
        "net_trading_pnl": str(net_trading),
        "provisional_net_trading_pnl": str(provisional_net),
        "failed_or_voided_net_trading_pnl": str(failed_net),
        "split_merge_realized_pnl": str(split_merge),
        "settlement_realized_pnl": str(settlement),
        "confirmed_operation_costs": str(operation_costs),
        "confirmed_external_cashflow_return": str(external_cashflow_return),
        "capital_flows": {
            key: str(value) for key, value in sorted(capital_flows.items())
        },
        "reward_estimated": {
            key: str(value) for key, value in sorted(estimated_by_type.items())
        },
        "reward_received": {
            key: str(value) for key, value in sorted(received_by_type.items())
        },
        "confirmed_account_return": str(confirmed),
        "estimated_account_return": str(estimated),
        "reward_coverage": coverage,
        "unmodeled_cashflows": unmodeled,
        "source_counts": {
            "trade_events": len(trade_rows),
            "confirmed_trade_events": len(confirmed_trades),
            "provisional_trade_events": len(provisional_trades),
            "failed_or_voided_trade_events": len(failed_trades),
            "operation_events": len(operation_rows),
            "reward_events": len(reward_rows),
            "cashflow_events": len(cashflow_rows),
        },
    }
    report_hash = _payload_hash(payload)
    return AccountReturnReport(
        report_id=f"account-return:{report_hash}",
        strategy_id=strategy_id,
        account_id=account_id,
        as_of=as_of,
        model_version=model_version,
        gross_execution_pnl=gross_execution,
        platform_taker_fee_paid=platform_fee,
        builder_fee_paid=builder_fee,
        net_trading_pnl=net_trading,
        provisional_net_trading_pnl=provisional_net,
        failed_or_voided_net_trading_pnl=failed_net,
        split_merge_realized_pnl=split_merge,
        settlement_realized_pnl=settlement,
        confirmed_operation_costs=operation_costs,
        confirmed_external_cashflow_return=external_cashflow_return,
        capital_flows=dict(capital_flows),
        reward_estimated=dict(estimated_by_type),
        reward_received=dict(received_by_type),
        confirmed_account_return=confirmed,
        estimated_account_return=estimated,
        reward_coverage=coverage,
        unmodeled_cashflows=unmodeled,
        source_counts=payload["source_counts"],
        report_hash=report_hash,
    )


class PostgresAccountReturnStore:
    def __init__(self, connection_factory: Any) -> None:
        self.connection_factory = connection_factory
        self.ensure_schema()

    def ensure_schema(self) -> None:
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS quant")
            for statement in ACCOUNT_RETURN_SCHEMA_STATEMENTS:
                cur.execute(statement)
            conn.commit()

    def build(
        self,
        *,
        strategy_id: str,
        account_id: str | None = None,
        as_of: datetime | None = None,
    ) -> AccountReturnReport:
        cutoff = as_of or datetime.now(timezone.utc)
        account = account_id or strategy_id
        with self.connection_factory(readonly=True) as conn, conn.cursor() as cur:
            trades = self._trades(cur, strategy_id, cutoff)
            operations = self._operations(cur, strategy_id, cutoff)
            rewards = self._rewards(cur, strategy_id, cutoff)
            cashflows = self._cashflows(cur, strategy_id, cutoff)
        return build_account_return_report(
            strategy_id=strategy_id,
            account_id=account,
            as_of=cutoff,
            trades=trades,
            operations=operations,
            rewards=rewards,
            cashflows=cashflows,
        )

    def persist(self, report: AccountReturnReport) -> bool:
        payload = _report_payload(report)
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.paper_account_return_reports (
                    report_id,strategy_id,account_id,as_of,model_version,report_hash,
                    gross_execution_pnl,platform_taker_fee_paid,builder_fee_paid,
                    net_trading_pnl,split_merge_realized_pnl,settlement_realized_pnl,
                    confirmed_external_cashflow_return,capital_flows,
                    confirmed_account_return,estimated_account_return,report
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s::jsonb
                ) ON CONFLICT (strategy_id,account_id,as_of,model_version)
                  DO NOTHING RETURNING report_id
                """,
                (
                    report.report_id,
                    report.strategy_id,
                    report.account_id,
                    report.as_of,
                    report.model_version,
                    report.report_hash,
                    report.gross_execution_pnl,
                    report.platform_taker_fee_paid,
                    report.builder_fee_paid,
                    report.net_trading_pnl,
                    report.split_merge_realized_pnl,
                    report.settlement_realized_pnl,
                    report.confirmed_external_cashflow_return,
                    json.dumps(
                        {
                            key: str(value)
                            for key, value in sorted(report.capital_flows.items())
                        },
                        sort_keys=True,
                    ),
                    report.confirmed_account_return,
                    report.estimated_account_return,
                    json.dumps(payload, sort_keys=True),
                ),
            )
            inserted = cur.fetchone() is not None
            if not inserted:
                cur.execute(
                    "SELECT report_id,report_hash "
                    "FROM quant.paper_account_return_reports "
                    "WHERE strategy_id=%s AND account_id=%s AND as_of=%s "
                    "AND model_version=%s",
                    (
                        report.strategy_id,
                        report.account_id,
                        report.as_of,
                        report.model_version,
                    ),
                )
                row = cur.fetchone()
                if (
                    row is None
                    or str(row["report_id"]) != report.report_id
                    or str(row["report_hash"]) != report.report_hash
                ):
                    raise ValueError("account return logical snapshot collision")
            conn.commit()
        return inserted

    @staticmethod
    def _trades(
        cur: Any, strategy_id: str, cutoff: datetime
    ) -> tuple[TradeReturnEvent, ...]:
        cur.execute(
            """
            SELECT e.idempotency_key,
                   e.realized_pnl_delta,
                   COALESCE(c.platform_fee,e.fee) AS platform_fee,
                   COALESCE(c.builder_fee,0) AS builder_fee,
                   COALESCE(f.state,'UNKNOWN') AS finality_state
            FROM quant.paper_ledger_entries e
            LEFT JOIN quant.paper_fills p
              ON e.idempotency_key=('fill:' || p.audit_key || ':' || p.fill_index::text)
            LEFT JOIN quant.paper_fill_fee_charges c
              ON c.audit_key=p.audit_key AND c.fill_index=p.fill_index
            LEFT JOIN quant.simulator_fill_finality_trades f
              ON f.audit_key=p.audit_key AND f.fill_index=p.fill_index
            WHERE e.strategy_id=%s AND e.event_type IN ('BUY','SELL')
              AND e.event_ts <= %s
            ORDER BY e.event_ts,e.entry_id
            """,
            (strategy_id, cutoff),
        )
        return tuple(
            TradeReturnEvent(
                event_id=str(row["idempotency_key"]),
                net_realized_pnl=Decimal(row["realized_pnl_delta"]),
                platform_fee=Decimal(row["platform_fee"]),
                builder_fee=Decimal(row["builder_fee"]),
                finality_state=str(row["finality_state"]),
            )
            for row in cur.fetchall()
        )

    @staticmethod
    def _operations(
        cur: Any, strategy_id: str, cutoff: datetime
    ) -> tuple[OperationReturnEvent, ...]:
        cur.execute(
            """
            SELECT operation_id AS event_id,
                   CASE WHEN operation_type='NEG_RISK_CONVERSION'
                        THEN 'CONVERSION' ELSE operation_type END AS category,
                   realized_pnl_delta
            FROM quant.paper_position_operation_applications
            WHERE strategy_id=%s AND applied_at <= %s
            UNION ALL
            SELECT merge_id,'MERGE',realized_pnl_delta
            FROM quant.paper_complete_set_merges m
            WHERE strategy_id=%s AND merged_at <= %s
              AND NOT EXISTS (
                  SELECT 1 FROM quant.paper_position_operation_applications a
                  WHERE a.operation_id=m.merge_id
              )
            UNION ALL
            SELECT settlement_key,'SETTLEMENT',realized_pnl_delta
            FROM quant.paper_settlements
            WHERE strategy_id=%s AND applied_at <= %s
            ORDER BY event_id
            """,
            (strategy_id, cutoff, strategy_id, cutoff, strategy_id, cutoff),
        )
        return tuple(
            OperationReturnEvent(
                event_id=str(row["event_id"]),
                category=str(row["category"]),
                realized_pnl=Decimal(row["realized_pnl_delta"]),
            )
            for row in cur.fetchall()
        )

    @staticmethod
    def _rewards(
        cur: Any, strategy_id: str, cutoff: datetime
    ) -> tuple[RewardReturnEvent, ...]:
        result: list[RewardReturnEvent] = []
        cur.execute(
            """
            WITH modeled AS (
                SELECT reward_type,period_end::date AS reward_date,
                       CASE
                         WHEN reward_type IN (
                           'MAKER_REBATE','LIQUIDITY_REWARD',
                           'SPONSOR_REWARD','DISPUTE_REWARD'
                         ) AND NULLIF(condition_id,'') IS NOT NULL
                         THEN 'condition:' || lower(condition_id)
                         ELSE 'account'
                       END AS scope_key,
                       sum(amount) AS amount
                FROM quant.paper_reward_accruals
                WHERE strategy_id=%s AND effective_ts <= %s
                GROUP BY reward_type,period_end::date,scope_key
            ), received AS (
                SELECT reward_type,reward_date,
                       CASE
                         WHEN reward_type IN (
                           'MAKER_REBATE','LIQUIDITY_REWARD',
                           'SPONSOR_REWARD','DISPUTE_REWARD'
                         ) AND NULLIF(condition_id,'') IS NOT NULL
                         THEN 'condition:' || lower(condition_id)
                         ELSE 'account'
                       END AS scope_key,
                       sum(amount) AS amount
                FROM quant.paper_reward_payouts
                WHERE strategy_id=%s AND status='RECEIVED'
                  AND effective_ts <= %s
                GROUP BY reward_type,reward_date,scope_key
            )
            SELECT m.reward_type,m.reward_date,m.scope_key,m.amount,
                   least(m.amount,COALESCE(r.amount,0)) AS allocated
            FROM modeled m
            LEFT JOIN received r
              ON r.reward_type=m.reward_type
             AND r.reward_date=m.reward_date
             AND r.scope_key=m.scope_key
            ORDER BY m.reward_type,m.reward_date,m.scope_key
            """,
            (strategy_id, cutoff, strategy_id, cutoff),
        )
        result.extend(
            RewardReturnEvent(
                event_id=(
                    f"accrual-group:{row['reward_type']}:{row['reward_date']}:"
                    f"{row['scope_key']}"
                ),
                reward_type=str(row["reward_type"]),
                modeled_amount=Decimal(row["amount"]),
                allocated_to_received=Decimal(row["allocated"]),
            )
            for row in cur.fetchall()
        )
        cur.execute(
            """
            SELECT payout_id,reward_type,amount
            FROM quant.paper_reward_payouts
            WHERE strategy_id=%s AND status='RECEIVED' AND effective_ts <= %s
            ORDER BY payout_id
            """,
            (strategy_id, cutoff),
        )
        result.extend(
            RewardReturnEvent(
                event_id=f"payout:{row['payout_id']}",
                reward_type=str(row["reward_type"]),
                received_amount=Decimal(row["amount"]),
            )
            for row in cur.fetchall()
        )
        cur.execute(
            """
            SELECT clawback_id,reward_type,amount
            FROM quant.paper_reward_clawbacks
            WHERE strategy_id=%s AND status='RECEIVED' AND effective_ts <= %s
            ORDER BY clawback_id
            """,
            (strategy_id, cutoff),
        )
        result.extend(
            RewardReturnEvent(
                event_id=f"clawback:{row['clawback_id']}",
                reward_type=str(row["reward_type"]),
                clawback_received=Decimal(row["amount"]),
            )
            for row in cur.fetchall()
        )
        return tuple(result)

    @staticmethod
    def _cashflows(
        cur: Any, strategy_id: str, cutoff: datetime
    ) -> tuple[AccountCashflowEvent, ...]:
        reward_prefixes = tuple(
            f"{item}_"
            for item in (
                *PRIMARY_REWARD_TYPES,
                "REFERRAL_REWARD",
                "SPONSOR_REWARD",
                "DISPUTE_REWARD",
            )
        )
        cur.execute(
            """
            SELECT event_id,event_type,amount,status
            FROM quant.paper_account_economic_events
            WHERE strategy_id=%s AND effective_ts <= %s
            ORDER BY effective_ts,event_id
            """,
            (strategy_id, cutoff),
        )
        return tuple(
            AccountCashflowEvent(
                event_id=str(row["event_id"]),
                event_type=str(row["event_type"]),
                amount=Decimal(row["amount"]),
                status=str(row["status"]),
            )
            for row in cur.fetchall()
            if not str(row["event_type"]).startswith(reward_prefixes)
            and str(row["event_type"]) != "REWARD_CLAWBACK"
        )


def _report_payload(report: AccountReturnReport) -> dict[str, Any]:
    payload = asdict(report)
    return _jsonable(payload)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _payload_hash(payload: Any) -> str:
    encoded = json.dumps(
        _jsonable(payload), sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
