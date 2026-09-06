"""Normalized complete-set lots with proportional cost-basis consumption."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

from quant.core.db import postgres_connection

COMPLETE_SET_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS quant.simulator_complete_set_lots (
        lot_id TEXT PRIMARY KEY,
        strategy_id TEXT NOT NULL,
        market_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        provenance TEXT NOT NULL,
        created_by_type TEXT NOT NULL,
        source_ref TEXT NOT NULL,
        paired BOOLEAN NOT NULL,
        original_joint_cost_basis NUMERIC NOT NULL,
        remaining_joint_cost_basis NUMERIC NOT NULL,
        status TEXT NOT NULL DEFAULT 'OPEN',
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        consumed_at TIMESTAMPTZ,
        UNIQUE (strategy_id,created_by_type,source_ref)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.simulator_complete_set_lot_legs (
        lot_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        asset_id TEXT NOT NULL,
        original_quantity NUMERIC NOT NULL,
        remaining_quantity NUMERIC NOT NULL,
        original_cost_basis NUMERIC NOT NULL,
        remaining_cost_basis NUMERIC NOT NULL,
        status TEXT NOT NULL DEFAULT 'OPEN',
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        consumed_at TIMESTAMPTZ,
        PRIMARY KEY (lot_id,asset_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS simulator_complete_set_lot_legs_open_idx
        ON quant.simulator_complete_set_lot_legs (strategy_id,asset_id,lot_id)
        WHERE status='OPEN'
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.simulator_complete_set_consumptions (
        consumption_id TEXT PRIMARY KEY,
        strategy_id TEXT NOT NULL,
        market_id TEXT NOT NULL,
        condition_id TEXT NOT NULL,
        consumer_type TEXT NOT NULL,
        consumer_ref TEXT NOT NULL,
        quantities JSONB NOT NULL,
        cost_basis_removed NUMERIC NOT NULL,
        cash_delta NUMERIC NOT NULL DEFAULT 0,
        realized_pnl_delta NUMERIC NOT NULL DEFAULT 0,
        settlement_match_type TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        consumed_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        UNIQUE (strategy_id,consumer_type,consumer_ref)
    )
    """,
    """
    ALTER TABLE quant.simulator_complete_set_consumptions
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL
        DEFAULT clock_timestamp()
    """,
    """
    CREATE TABLE IF NOT EXISTS quant.simulator_complete_set_consumption_legs (
        consumption_id TEXT NOT NULL,
        lot_id TEXT NOT NULL,
        asset_id TEXT NOT NULL,
        quantity NUMERIC NOT NULL,
        cost_basis_removed NUMERIC NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (consumption_id,lot_id,asset_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS simulator_complete_set_consumptions_ref_idx
        ON quant.simulator_complete_set_consumptions
        (strategy_id,condition_id,consumer_type,consumed_at)
    """,
)


class CompleteSetProvenance(str, Enum):
    SPLIT = "SPLIT"
    SEPARATE_MARKET_BUYS = "SEPARATE_MARKET_BUYS"
    CTF_MINT_MATCH = "CTF_MINT_MATCH"
    NEG_RISK_CONVERSION = "NEG_RISK_CONVERSION"
    TRANSFER_IN = "TRANSFER_IN"


@dataclass(frozen=True)
class CompleteSetLotLegInput:
    quantity: Decimal
    cost_basis: Decimal

    def __post_init__(self) -> None:
        if Decimal(self.quantity) <= 0:
            raise ValueError("complete-set lot quantity must be positive")
        if Decimal(self.cost_basis) < 0:
            raise ValueError("complete-set lot cost basis cannot be negative")


@dataclass(frozen=True)
class CompleteSetConsumptionAllocation:
    lot_id: str
    asset_id: str
    quantity: Decimal
    cost_basis_removed: Decimal


@dataclass(frozen=True)
class CompleteSetConsumptionResult:
    consumption_id: str
    cost_basis_removed: Decimal
    allocations: tuple[CompleteSetConsumptionAllocation, ...]
    replayed: bool = False


class CompleteSetLotStore:
    """Cursor-level helpers keep lot changes atomic with the paper ledger."""

    def __init__(self, connection_factory: Any = postgres_connection) -> None:
        self.connection_factory = connection_factory

    def ensure_schema(self) -> None:
        with self.connection_factory(readonly=False) as conn, conn.cursor() as cur:
            for statement in COMPLETE_SET_SCHEMA_STATEMENTS:
                cur.execute(statement)

    @staticmethod
    def create_lot(
        cur: Any,
        *,
        strategy_id: str,
        market_id: str,
        condition_id: str,
        provenance: CompleteSetProvenance | str,
        created_by_type: str,
        source_ref: str,
        legs: Mapping[str, CompleteSetLotLegInput],
        created_at: Any,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        if not legs:
            raise ValueError("complete-set lot requires at least one leg")
        normalized = {
            str(asset_id): CompleteSetLotLegInput(
                Decimal(leg.quantity), Decimal(leg.cost_basis)
            )
            for asset_id, leg in legs.items()
        }
        source_type = str(created_by_type).strip()
        source = str(source_ref).strip()
        if not source_type or not source:
            raise ValueError("complete-set lot requires source type and reference")
        provenance_value = (
            provenance.value
            if isinstance(provenance, CompleteSetProvenance)
            else str(provenance)
        )
        lot_id = _stable_id(
            "complete-set-lot", strategy_id, source_type, source
        )
        joint_basis = sum(
            (leg.cost_basis for leg in normalized.values()), Decimal(0)
        )
        cur.execute(
            """
            INSERT INTO quant.simulator_complete_set_lots (
                lot_id,strategy_id,market_id,condition_id,provenance,
                created_by_type,source_ref,paired,original_joint_cost_basis,
                remaining_joint_cost_basis,metadata,created_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
            ON CONFLICT (lot_id) DO NOTHING RETURNING lot_id
            """,
            (
                lot_id,
                str(strategy_id),
                str(market_id),
                str(condition_id),
                provenance_value,
                source_type,
                source,
                len(normalized) > 1,
                joint_basis,
                joint_basis,
                json.dumps(dict(metadata or {}), sort_keys=True, default=str),
                created_at,
            ),
        )
        inserted = cur.fetchone() is not None
        if not inserted:
            cur.execute(
                """
                SELECT strategy_id,market_id,condition_id,provenance,
                       created_by_type,source_ref,original_joint_cost_basis
                FROM quant.simulator_complete_set_lots WHERE lot_id=%s
                """,
                (lot_id,),
            )
            existing = cur.fetchone()
            expected = (
                str(strategy_id),
                str(market_id),
                str(condition_id),
                provenance_value,
                source_type,
                source,
                joint_basis,
            )
            actual = (
                str(existing["strategy_id"]),
                str(existing["market_id"]),
                str(existing["condition_id"]),
                str(existing["provenance"]),
                str(existing["created_by_type"]),
                str(existing["source_ref"]),
                Decimal(existing["original_joint_cost_basis"]),
            )
            if actual != expected:
                raise ValueError("complete-set lot id collision")
        for asset_id, leg in sorted(normalized.items()):
            cur.execute(
                """
                INSERT INTO quant.simulator_complete_set_lot_legs (
                    lot_id,strategy_id,asset_id,original_quantity,
                    remaining_quantity,original_cost_basis,remaining_cost_basis
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (lot_id,asset_id) DO NOTHING RETURNING asset_id
                """,
                (
                    lot_id,
                    str(strategy_id),
                    asset_id,
                    leg.quantity,
                    leg.quantity,
                    leg.cost_basis,
                    leg.cost_basis,
                ),
            )
            if cur.fetchone() is not None:
                continue
            cur.execute(
                """
                SELECT original_quantity,original_cost_basis
                FROM quant.simulator_complete_set_lot_legs
                WHERE lot_id=%s AND asset_id=%s
                """,
                (lot_id, asset_id),
            )
            existing_leg = cur.fetchone()
            if existing_leg is None or (
                Decimal(existing_leg["original_quantity"]) != leg.quantity
                or Decimal(existing_leg["original_cost_basis"]) != leg.cost_basis
            ):
                raise ValueError("complete-set lot leg collision")
        return lot_id

    @classmethod
    def ensure_position_coverage(
        cls,
        cur: Any,
        *,
        strategy_id: str,
        market_id: str,
        condition_id: str,
        asset_id: str,
        quantity: Decimal,
        cost_basis: Decimal,
        observed_at: Any,
        tolerance: Decimal = Decimal("0.00000001"),
    ) -> str | None:
        quantity = Decimal(quantity)
        cost_basis = Decimal(cost_basis)
        cur.execute(
            """
            SELECT COALESCE(sum(remaining_quantity),0) AS quantity,
                   COALESCE(sum(remaining_cost_basis),0) AS cost_basis
            FROM quant.simulator_complete_set_lot_legs
            WHERE strategy_id=%s AND asset_id=%s AND status='OPEN'
            """,
            (str(strategy_id), str(asset_id)),
        )
        covered = cur.fetchone()
        covered_quantity = Decimal(covered["quantity"])
        covered_basis = Decimal(covered["cost_basis"])
        quantity_deficit = quantity - covered_quantity
        basis_deficit = cost_basis - covered_basis
        if abs(quantity_deficit) <= tolerance and abs(basis_deficit) <= tolerance:
            return None
        if quantity_deficit < -tolerance or basis_deficit < -tolerance:
            raise ValueError(
                "complete-set lots exceed aggregate position:"
                f"{asset_id}:{covered_quantity}:{quantity}:"
                f"{covered_basis}:{cost_basis}"
            )
        if quantity_deficit <= tolerance:
            raise ValueError(
                "complete-set lot quantity matches but cost basis does not:"
                f"{asset_id}:{basis_deficit}"
            )
        source_ref = (
            f"bootstrap:{asset_id}:{format(covered_quantity, 'f')}:"
            f"{format(covered_basis, 'f')}:{format(quantity_deficit, 'f')}:"
            f"{format(max(basis_deficit, Decimal(0)), 'f')}"
        )
        return cls.create_lot(
            cur,
            strategy_id=strategy_id,
            market_id=market_id,
            condition_id=condition_id,
            provenance=CompleteSetProvenance.TRANSFER_IN,
            created_by_type="POSITION_BOOTSTRAP",
            source_ref=source_ref,
            legs={
                str(asset_id): CompleteSetLotLegInput(
                    quantity=quantity_deficit,
                    cost_basis=max(basis_deficit, Decimal(0)),
                )
            },
            created_at=observed_at,
            metadata={"reason": "legacy_position_without_lot_provenance"},
        )

    @classmethod
    def pair_separate_market_buys(
        cls,
        cur: Any,
        *,
        strategy_id: str,
        market_id: str,
        condition_id: str,
        paired_at: Any,
        tolerance: Decimal = Decimal("0.00000001"),
    ) -> tuple[str, ...]:
        """Move opposite one-sided BUY lots into joint-cost paired lots."""

        paired_lot_ids: list[str] = []
        while True:
            cur.execute(
                """
                SELECT leg.lot_id,leg.asset_id,leg.remaining_quantity,
                       leg.remaining_cost_basis,lot.created_at
                FROM quant.simulator_complete_set_lot_legs leg
                JOIN quant.simulator_complete_set_lots lot
                  ON lot.lot_id=leg.lot_id
                WHERE leg.strategy_id=%s AND lot.condition_id=%s
                  AND lot.provenance IN ('SEPARATE_MARKET_BUYS','CTF_MINT_MATCH')
                  AND lot.paired=FALSE AND leg.status='OPEN'
                  AND leg.remaining_quantity>0
                ORDER BY leg.asset_id,lot.created_at,leg.lot_id
                FOR UPDATE OF leg,lot
                """,
                (str(strategy_id), str(condition_id)),
            )
            rows = [dict(row) for row in cur.fetchall()]
            asset_ids = sorted({str(row["asset_id"]) for row in rows})
            if len(asset_ids) != 2:
                break
            left = next(
                row for row in rows if str(row["asset_id"]) == asset_ids[0]
            )
            right = next(
                row for row in rows if str(row["asset_id"]) == asset_ids[1]
            )
            quantity = min(
                Decimal(left["remaining_quantity"]),
                Decimal(right["remaining_quantity"]),
            )
            if quantity <= tolerance:
                break
            leg_inputs: dict[str, CompleteSetLotLegInput] = {}
            for row in (left, right):
                remaining_quantity = Decimal(row["remaining_quantity"])
                remaining_basis = Decimal(row["remaining_cost_basis"])
                paired_basis = remaining_basis * quantity / remaining_quantity
                leg_inputs[str(row["asset_id"])] = CompleteSetLotLegInput(
                    quantity=quantity,
                    cost_basis=paired_basis,
                )
            source_lot_ids = sorted(
                (str(left["lot_id"]), str(right["lot_id"]))
            )
            paired_lot_id = cls.create_lot(
                cur,
                strategy_id=strategy_id,
                market_id=market_id,
                condition_id=condition_id,
                provenance=CompleteSetProvenance.SEPARATE_MARKET_BUYS,
                created_by_type="LOT_PAIRING",
                source_ref=f"pair:{source_lot_ids[0]}:{source_lot_ids[1]}",
                legs=leg_inputs,
                created_at=paired_at,
                metadata={"source_lot_ids": source_lot_ids},
            )
            for row in (left, right):
                cls._move_to_paired_lot(
                    cur,
                    row=row,
                    quantity=quantity,
                    cost_basis=leg_inputs[str(row["asset_id"])].cost_basis,
                    paired_at=paired_at,
                    tolerance=tolerance,
                )
            paired_lot_ids.append(paired_lot_id)
        return tuple(paired_lot_ids)

    @classmethod
    def consume_assets(
        cls,
        cur: Any,
        *,
        consumption_id: str,
        strategy_id: str,
        market_id: str,
        condition_id: str,
        consumer_type: str,
        consumer_ref: str,
        quantities: Mapping[str, Decimal],
        expected_basis_by_asset: Mapping[str, Decimal],
        cash_delta: Decimal,
        realized_pnl_delta: Decimal,
        consumed_at: Any,
        metadata: Mapping[str, Any] | None = None,
        tolerance: Decimal = Decimal("0.00000001"),
    ) -> CompleteSetConsumptionResult:
        normalized_quantities = {
            str(asset): Decimal(quantity)
            for asset, quantity in quantities.items()
            if Decimal(quantity) > 0
        }
        expected_basis = {
            str(asset): Decimal(value)
            for asset, value in expected_basis_by_asset.items()
        }
        if not normalized_quantities:
            raise ValueError("complete-set consumption requires positive quantities")
        cur.execute(
            """
            SELECT strategy_id,market_id,condition_id,consumer_type,
                   consumer_ref,quantities,cost_basis_removed,cash_delta,
                   realized_pnl_delta
            FROM quant.simulator_complete_set_consumptions
            WHERE consumption_id=%s FOR UPDATE
            """,
            (str(consumption_id),),
        )
        existing = cur.fetchone()
        if existing is not None:
            existing_quantities = {
                str(asset): Decimal(str(quantity))
                for asset, quantity in dict(existing["quantities"]).items()
            }
            expected_removed = sum(
                (
                    expected_basis.get(asset, Decimal(0))
                    for asset in normalized_quantities
                ),
                Decimal(0),
            )
            expected_identity = (
                str(strategy_id),
                str(market_id),
                str(condition_id),
                str(consumer_type),
                str(consumer_ref),
                normalized_quantities,
                expected_removed,
                Decimal(cash_delta),
                Decimal(realized_pnl_delta),
            )
            actual_identity = (
                str(existing["strategy_id"]),
                str(existing["market_id"]),
                str(existing["condition_id"]),
                str(existing["consumer_type"]),
                str(existing["consumer_ref"]),
                existing_quantities,
                Decimal(existing["cost_basis_removed"]),
                Decimal(existing["cash_delta"]),
                Decimal(existing["realized_pnl_delta"]),
            )
            if actual_identity != expected_identity:
                raise ValueError("complete-set consumption id collision")
            return cls._consumption_result(
                cur, str(consumption_id), replayed=True
            )

        allocations: list[CompleteSetConsumptionAllocation] = []
        touched_lots: set[str] = set()
        for asset_id, requested_quantity in sorted(normalized_quantities.items()):
            cur.execute(
                """
                SELECT leg.lot_id,leg.remaining_quantity,
                       leg.remaining_cost_basis,lot.created_at
                FROM quant.simulator_complete_set_lot_legs leg
                JOIN quant.simulator_complete_set_lots lot
                  ON lot.lot_id=leg.lot_id
                WHERE leg.strategy_id=%s AND leg.asset_id=%s
                  AND leg.status='OPEN' AND leg.remaining_quantity>0
                ORDER BY lot.created_at,leg.lot_id
                FOR UPDATE OF leg,lot
                """,
                (str(strategy_id), asset_id),
            )
            rows = [dict(row) for row in cur.fetchall()]
            total_quantity = sum(
                (Decimal(row["remaining_quantity"]) for row in rows), Decimal(0)
            )
            total_basis = sum(
                (Decimal(row["remaining_cost_basis"]) for row in rows), Decimal(0)
            )
            if total_quantity + tolerance < requested_quantity:
                raise ValueError(
                    "insufficient complete-set lot quantity:"
                    f"{asset_id}:{total_quantity}:{requested_quantity}"
                )
            basis_to_remove = (
                total_basis * requested_quantity / total_quantity
                if total_quantity
                else Decimal(0)
            )
            expected = expected_basis.get(asset_id, basis_to_remove)
            if abs(basis_to_remove - expected) > tolerance:
                raise ValueError(
                    "complete-set lot cost basis diverges from aggregate position:"
                    f"{asset_id}:{basis_to_remove}:{expected}"
                )
            quantity_remaining = requested_quantity
            basis_remaining = basis_to_remove
            for index, row in enumerate(rows):
                row_quantity = Decimal(row["remaining_quantity"])
                row_basis = Decimal(row["remaining_cost_basis"])
                if index == len(rows) - 1:
                    allocated_quantity = quantity_remaining
                    allocated_basis = basis_remaining
                else:
                    allocated_quantity = requested_quantity * row_quantity / total_quantity
                    allocated_basis = (
                        basis_to_remove * row_basis / total_basis
                        if total_basis
                        else Decimal(0)
                    )
                    allocated_quantity = min(allocated_quantity, quantity_remaining)
                    allocated_basis = min(allocated_basis, basis_remaining)
                if allocated_quantity <= 0:
                    continue
                quantity_remaining -= allocated_quantity
                basis_remaining -= allocated_basis
                next_quantity = row_quantity - allocated_quantity
                next_basis = max(Decimal(0), row_basis - allocated_basis)
                status = "CONSUMED" if next_quantity <= tolerance else "OPEN"
                if status == "CONSUMED":
                    next_quantity = Decimal(0)
                    next_basis = Decimal(0)
                cur.execute(
                    """
                    UPDATE quant.simulator_complete_set_lot_legs
                    SET remaining_quantity=%s,remaining_cost_basis=%s,status=%s,
                        updated_at=clock_timestamp(),
                        consumed_at=CASE WHEN %s='CONSUMED' THEN %s ELSE NULL END
                    WHERE lot_id=%s AND asset_id=%s
                    """,
                    (
                        next_quantity,
                        next_basis,
                        status,
                        status,
                        consumed_at,
                        str(row["lot_id"]),
                        asset_id,
                    ),
                )
                allocation = CompleteSetConsumptionAllocation(
                    lot_id=str(row["lot_id"]),
                    asset_id=asset_id,
                    quantity=allocated_quantity,
                    cost_basis_removed=allocated_basis,
                )
                allocations.append(allocation)
                touched_lots.add(allocation.lot_id)

        removed_total = sum(
            (allocation.cost_basis_removed for allocation in allocations),
            Decimal(0),
        )
        cur.execute(
            """
            INSERT INTO quant.simulator_complete_set_consumptions (
                consumption_id,strategy_id,market_id,condition_id,
                consumer_type,consumer_ref,quantities,cost_basis_removed,
                cash_delta,realized_pnl_delta,metadata,consumed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s::jsonb,%s)
            """,
            (
                str(consumption_id),
                str(strategy_id),
                str(market_id),
                str(condition_id),
                str(consumer_type),
                str(consumer_ref),
                json.dumps(
                    {
                        asset: format(quantity, "f")
                        for asset, quantity in normalized_quantities.items()
                    },
                    sort_keys=True,
                ),
                removed_total,
                Decimal(cash_delta),
                Decimal(realized_pnl_delta),
                json.dumps(dict(metadata or {}), sort_keys=True, default=str),
                consumed_at,
            ),
        )
        for allocation in allocations:
            cur.execute(
                """
                INSERT INTO quant.simulator_complete_set_consumption_legs (
                    consumption_id,lot_id,asset_id,quantity,cost_basis_removed
                ) VALUES (%s,%s,%s,%s,%s)
                """,
                (
                    str(consumption_id),
                    allocation.lot_id,
                    allocation.asset_id,
                    allocation.quantity,
                    allocation.cost_basis_removed,
                ),
            )
        for lot_id in sorted(touched_lots):
            cur.execute(
                """
                SELECT COALESCE(sum(remaining_cost_basis),0) AS basis,
                       count(*) FILTER (WHERE status='OPEN') AS open_legs
                FROM quant.simulator_complete_set_lot_legs WHERE lot_id=%s
                """,
                (lot_id,),
            )
            aggregate = cur.fetchone()
            lot_status = "OPEN" if int(aggregate["open_legs"]) else "CONSUMED"
            cur.execute(
                """
                UPDATE quant.simulator_complete_set_lots
                SET remaining_joint_cost_basis=%s,status=%s,
                    updated_at=clock_timestamp(),
                    consumed_at=CASE WHEN %s='CONSUMED' THEN %s ELSE NULL END
                WHERE lot_id=%s
                """,
                (
                    aggregate["basis"],
                    lot_status,
                    lot_status,
                    consumed_at,
                    lot_id,
                ),
            )
        return CompleteSetConsumptionResult(
            consumption_id=str(consumption_id),
            cost_basis_removed=removed_total,
            allocations=tuple(allocations),
        )

    @staticmethod
    def annotate_fill_settlement(
        cur: Any,
        *,
        audit_key: str,
        fill_index: int,
        settlement_match_type: str,
    ) -> None:
        source_ref = f"fill:{audit_key}:{int(fill_index)}"
        if settlement_match_type == "MINT":
            cur.execute(
                """
                UPDATE quant.simulator_complete_set_lots
                SET provenance='CTF_MINT_MATCH',updated_at=clock_timestamp(),
                    metadata=metadata || %s::jsonb
                WHERE created_by_type='PAPER_FILL' AND source_ref=%s
                """,
                (json.dumps({"settlement_match_type": "MINT"}), source_ref),
            )
        cur.execute(
            """
            UPDATE quant.simulator_complete_set_consumptions
            SET settlement_match_type=%s,updated_at=clock_timestamp(),
                metadata=metadata || %s::jsonb
            WHERE consumer_type='PAPER_SELL' AND consumer_ref=%s
            """,
            (
                str(settlement_match_type),
                json.dumps({"settlement_match_type": settlement_match_type}),
                source_ref,
            ),
        )

    @staticmethod
    def _move_to_paired_lot(
        cur: Any,
        *,
        row: Mapping[str, Any],
        quantity: Decimal,
        cost_basis: Decimal,
        paired_at: Any,
        tolerance: Decimal,
    ) -> None:
        lot_id = str(row["lot_id"])
        asset_id = str(row["asset_id"])
        next_quantity = Decimal(row["remaining_quantity"]) - quantity
        next_basis = Decimal(row["remaining_cost_basis"]) - cost_basis
        status = "CONSUMED" if next_quantity <= tolerance else "OPEN"
        if status == "CONSUMED":
            next_quantity = Decimal(0)
            next_basis = Decimal(0)
        cur.execute(
            """
            UPDATE quant.simulator_complete_set_lot_legs
            SET remaining_quantity=%s,remaining_cost_basis=%s,status=%s,
                updated_at=clock_timestamp(),
                consumed_at=CASE WHEN %s='CONSUMED' THEN %s ELSE NULL END
            WHERE lot_id=%s AND asset_id=%s
            """,
            (
                next_quantity,
                next_basis,
                status,
                status,
                paired_at,
                lot_id,
                asset_id,
            ),
        )
        cur.execute(
            """
            SELECT COALESCE(sum(remaining_cost_basis),0) AS basis,
                   count(*) FILTER (WHERE status='OPEN') AS open_legs
            FROM quant.simulator_complete_set_lot_legs WHERE lot_id=%s
            """,
            (lot_id,),
        )
        aggregate = cur.fetchone()
        lot_status = "OPEN" if int(aggregate["open_legs"]) else "CONSUMED"
        cur.execute(
            """
            UPDATE quant.simulator_complete_set_lots
            SET remaining_joint_cost_basis=%s,status=%s,
                updated_at=clock_timestamp(),
                consumed_at=CASE WHEN %s='CONSUMED' THEN %s ELSE NULL END
            WHERE lot_id=%s
            """,
            (
                aggregate["basis"],
                lot_status,
                lot_status,
                paired_at,
                lot_id,
            ),
        )

    @staticmethod
    def _consumption_result(
        cur: Any, consumption_id: str, *, replayed: bool
    ) -> CompleteSetConsumptionResult:
        cur.execute(
            """
            SELECT cost_basis_removed
            FROM quant.simulator_complete_set_consumptions
            WHERE consumption_id=%s
            """,
            (consumption_id,),
        )
        parent = cur.fetchone()
        cur.execute(
            """
            SELECT lot_id,asset_id,quantity,cost_basis_removed
            FROM quant.simulator_complete_set_consumption_legs
            WHERE consumption_id=%s ORDER BY asset_id,lot_id
            """,
            (consumption_id,),
        )
        allocations = tuple(
            CompleteSetConsumptionAllocation(
                lot_id=str(row["lot_id"]),
                asset_id=str(row["asset_id"]),
                quantity=Decimal(row["quantity"]),
                cost_basis_removed=Decimal(row["cost_basis_removed"]),
            )
            for row in cur.fetchall()
        )
        return CompleteSetConsumptionResult(
            consumption_id=consumption_id,
            cost_basis_removed=Decimal(parent["cost_basis_removed"]),
            allocations=allocations,
            replayed=replayed,
        )


def _stable_id(namespace: str, *parts: str) -> str:
    payload = json.dumps(
        [namespace, *[str(part) for part in parts]],
        separators=(",", ":"),
    )
    return f"{namespace}:{hashlib.sha256(payload.encode()).hexdigest()}"
