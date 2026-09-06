"""Unified execution adapters and prediction-market financial lifecycle."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, ClassVar

from quant.settlement.neg_risk_convert import plan_no_to_other_yes
from quant.settlement.payout_vector import PayoutVector

from .contracts import (
    CtfMatchType,
    ExecutionMatch,
    MatchFinalityState,
    RawOrderSide,
    SettlementReceipt,
    canonical_hash,
    qty,
)


@dataclass(frozen=True)
class ExecutionEvidence:
    fill_ts: datetime
    fill_block: int | None
    source_fill_id: str
    execution_model: str


ExecutionAdapter = Callable[[Mapping[str, Any]], tuple[ExecutionEvidence, ...]]


class ExecutionMatchAdapterRegistry:
    """Extract finalization timing from every supported execution model."""

    def __init__(self) -> None:
        self._adapters: list[tuple[str, ExecutionAdapter]] = []

    def register(self, name: str, adapter: ExecutionAdapter) -> None:
        self._adapters.append((str(name), adapter))

    def extract(self, meta: Mapping[str, Any]) -> tuple[ExecutionEvidence, ...]:
        for _, adapter in self._adapters:
            rows = adapter(meta)
            if rows:
                return tuple(
                    sorted(
                        rows,
                        key=lambda item: (
                            item.fill_ts,
                            -1 if item.fill_block is None else item.fill_block,
                            item.source_fill_id,
                        ),
                    )
                )
        return ()


def default_execution_adapter_registry() -> ExecutionMatchAdapterRegistry:
    registry = ExecutionMatchAdapterRegistry()
    registry.register("pml2_replay_v1", _pml2_evidence)
    registry.register("fill_only_v3", _v3_evidence)
    registry.register("orderfilled_v2", _v2_evidence)
    registry.register("legacy_l2_orderfilled", _legacy_l2_evidence)
    return registry


def _sequence(value: Any) -> Sequence[Any] | None:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return value
    return None


def _timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _rows_from_container(meta: Mapping[str, Any], *keys: str) -> Sequence[Any] | None:
    current: Any = meta
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return _sequence(current)


def _pml2_evidence(meta: Mapping[str, Any]) -> tuple[ExecutionEvidence, ...]:
    rows = (
        _rows_from_container(meta, "pml2_replay_v1", "execution_matches")
        or _rows_from_container(meta, "pml2_replay_v1", "fills")
        or _rows_from_container(meta, "execution_matches")
    )
    if rows is None:
        return ()
    result: list[ExecutionEvidence] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return ()
        fill_ts = _timestamp(
            row.get("fill_exchange_ts")
            or row.get("fillExchangeTs")
            or row.get("fill_ts")
        )
        source_id = str(
            row.get("fill_id")
            or row.get("fillId")
            or row.get("audit_hash")
            or ""
        )
        if fill_ts is None or not source_id:
            return ()
        result.append(
            ExecutionEvidence(
                fill_ts=fill_ts,
                fill_block=_positive_int(
                    row.get("fill_block") or row.get("fillBlock")
                ),
                source_fill_id=source_id,
                execution_model="PREDICTION_L2_REPLAY_V1",
            )
        )
    return tuple(result)


def _v2_evidence(meta: Mapping[str, Any]) -> tuple[ExecutionEvidence, ...]:
    rows = _rows_from_container(meta, "orderfilled_v2", "fills")
    if rows is None:
        return ()
    result: list[ExecutionEvidence] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return ()
        fill_ts = _timestamp(row.get("fill_ts"))
        source_id = str(row.get("source_trade_id") or "")
        fill_block = _positive_int(row.get("fill_block"))
        if fill_ts is None or fill_block is None or not source_id:
            return ()
        result.append(
            ExecutionEvidence(
                fill_ts,
                fill_block,
                source_id,
                "ORDERFILLED_V2_TAPE",
            )
        )
    return tuple(result)


def _v3_evidence(meta: Mapping[str, Any]) -> tuple[ExecutionEvidence, ...]:
    rows = (
        _rows_from_container(meta, "fill_only_v3", "fills")
        or _rows_from_container(meta, "trade_only_v3", "fills")
    )
    if rows is None:
        return ()
    result: list[ExecutionEvidence] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return ()
        fill_ts = _timestamp(row.get("fill_ts"))
        source_ids = _sequence(row.get("source_trade_ids")) or _sequence(
            row.get("source_event_ids")
        )
        if not source_ids:
            source_id = str(
                row.get("source_trade_id")
                or row.get("source_event_id")
                or row.get("fill_id")
                or ""
            )
            source_ids = (source_id,) if source_id else ()
        if not source_ids:
            continue
        fill_block = _positive_int(row.get("fill_block"))
        if fill_ts is None or fill_block is None:
            return ()
        result.extend(
            ExecutionEvidence(
                fill_ts,
                fill_block,
                str(source_id),
                "FILL_ONLY_V3",
            )
            for source_id in source_ids
            if str(source_id)
        )
    return tuple(result)


def _legacy_l2_evidence(meta: Mapping[str, Any]) -> tuple[ExecutionEvidence, ...]:
    rows = (
        _rows_from_container(meta, "execution_audit", "fills")
        or _rows_from_container(meta, "l2_execution", "fills")
    )
    if rows is None:
        return ()
    result: list[ExecutionEvidence] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return ()
        fill_ts = _timestamp(row.get("ts") or row.get("fill_ts"))
        raw_ids = _sequence(row.get("source_event_ids")) or ()
        source_id = str(row.get("fill_id") or (raw_ids[0] if raw_ids else ""))
        if fill_ts is None or not source_id:
            return ()
        result.append(
            ExecutionEvidence(
                fill_ts,
                _positive_int(row.get("fill_block")),
                source_id,
                "LEGACY_ORDERFILLED_LOB",
            )
        )
    return tuple(result)


class SettlementLifecycle:
    """Idempotent MATCHED -> settlement state transitions."""

    _ALLOWED: ClassVar[dict[MatchFinalityState, set[MatchFinalityState]]] = {
        MatchFinalityState.MATCHED: {MatchFinalityState.PENDING_SETTLEMENT},
        MatchFinalityState.PENDING_SETTLEMENT: {
            MatchFinalityState.MINED,
            MatchFinalityState.RETRYING,
            MatchFinalityState.CONFIRMED,
            MatchFinalityState.FAILED,
        },
        MatchFinalityState.MINED: {
            MatchFinalityState.CONFIRMED,
            MatchFinalityState.RETRYING,
            MatchFinalityState.FAILED,
        },
        MatchFinalityState.RETRYING: {
            MatchFinalityState.MINED,
            MatchFinalityState.CONFIRMED,
            MatchFinalityState.FAILED,
        },
        MatchFinalityState.CONFIRMED: set(),
        MatchFinalityState.FAILED: set(),
    }

    def __init__(self) -> None:
        self.matches: dict[str, ExecutionMatch] = {}
        self.receipts: dict[str, SettlementReceipt] = {}

    def register(self, match: ExecutionMatch) -> None:
        existing = self.matches.get(match.fill_id)
        if existing is not None and existing.audit_hash != match.audit_hash:
            raise ValueError("fill_id already registered with different evidence")
        self.matches[match.fill_id] = match

    def transition(
        self,
        fill_id: str,
        state: MatchFinalityState,
        *,
        observed_at: datetime,
        tx_hash: str | None = None,
        block_number: int | None = None,
        reason: str = "",
        source_event_ids: tuple[str, ...] = (),
    ) -> SettlementReceipt:
        match = self.matches[fill_id]
        if state == match.finality_state:
            existing = self.receipts.get(fill_id)
            if existing is not None:
                return existing
        if state not in self._ALLOWED[match.finality_state]:
            raise ValueError(
                f"invalid settlement transition {match.finality_state.value}->{state.value}"
            )
        updated = replace(match, finality_state=state, audit_hash="")
        self.matches[fill_id] = updated
        receipt = SettlementReceipt(
            receipt_id=canonical_hash(
                {
                    "fill_id": fill_id,
                    "state": state,
                    "observed_at": observed_at,
                    "tx_hash": tx_hash,
                    "block_number": block_number,
                }
            ),
            fill_id=fill_id,
            state=state,
            observed_at=observed_at,
            tx_hash=tx_hash,
            block_number=block_number,
            reason=reason,
            source_event_ids=source_event_ids,
        )
        self.receipts[fill_id] = receipt
        return receipt


class PredictionPortfolioLedger:
    """Small auditable portfolio ledger for matches and CTF operations."""

    def __init__(self, *, initial_cash: Decimal = Decimal(0)) -> None:
        self.cash = Decimal(initial_cash)
        self.tokens: dict[str, Decimal] = {}
        self.provisional_cash = Decimal(0)
        self.provisional_tokens: dict[str, Decimal] = {}
        self._pending: dict[str, tuple[Decimal, Decimal]] = {}
        self._confirmed: set[str] = set()
        self._failed: set[str] = set()
        self._operations: set[str] = set()
        self.operation_log: dict[str, dict[str, Any]] = {}

    def record_match(self, match: ExecutionMatch) -> None:
        if match.fill_id in self._failed:
            raise ValueError("failed match cannot be recorded again")
        if match.fill_id in self._pending or match.fill_id in self._confirmed:
            return
        cash_delta, token_delta = self._account_flow(match)
        self.provisional_cash += cash_delta
        self.provisional_tokens[match.token_id] = (
            self.provisional_tokens.get(match.token_id, Decimal(0)) + token_delta
        )
        self._pending[match.fill_id] = (cash_delta, token_delta)

    def confirm_match(self, match: ExecutionMatch) -> None:
        if match.fill_id in self._failed:
            raise ValueError("failed match cannot be confirmed")
        if match.fill_id in self._confirmed:
            return
        self.record_match(match)
        cash_delta, token_delta = self._pending.pop(match.fill_id)
        self.provisional_cash -= cash_delta
        self.provisional_tokens[match.token_id] = (
            self.provisional_tokens.get(match.token_id, Decimal(0)) - token_delta
        )
        self.cash += cash_delta
        self.tokens[match.token_id] = self.tokens.get(match.token_id, Decimal(0)) + token_delta
        self._confirmed.add(match.fill_id)

    def fail_match(self, match: ExecutionMatch) -> None:
        if match.fill_id in self._failed:
            return
        pending = self._pending.pop(match.fill_id, None)
        if pending is not None:
            cash_delta, token_delta = pending
            self.provisional_cash -= cash_delta
            self.provisional_tokens[match.token_id] = (
                self.provisional_tokens.get(match.token_id, Decimal(0))
                - token_delta
            )
        if match.fill_id in self._confirmed:
            cash_delta, token_delta = self._account_flow(match)
            self.cash -= cash_delta
            self.tokens[match.token_id] = (
                self.tokens.get(match.token_id, Decimal(0)) - token_delta
            )
            self._confirmed.remove(match.fill_id)
        self._failed.add(match.fill_id)

    def split(
        self,
        *,
        operation_id: str,
        yes_token_id: str,
        no_token_id: str,
        size: Decimal,
        fee: Decimal = Decimal(0),
    ) -> None:
        if operation_id in self._operations:
            return
        amount = qty(size)
        cost = amount + max(Decimal(0), fee)
        if self.cash < cost:
            raise ValueError("insufficient collateral for split")
        self.cash -= cost
        self.tokens[yes_token_id] = self.tokens.get(yes_token_id, Decimal(0)) + amount
        self.tokens[no_token_id] = self.tokens.get(no_token_id, Decimal(0)) + amount
        self._operations.add(operation_id)
        self.operation_log[operation_id] = {
            "operation_type": "SPLIT",
            "cash_delta": -cost,
            "token_deltas": {yes_token_id: amount, no_token_id: amount},
            "fee": max(Decimal(0), fee),
        }

    def merge(
        self,
        *,
        operation_id: str,
        yes_token_id: str,
        no_token_id: str,
        size: Decimal,
        fee: Decimal = Decimal(0),
    ) -> None:
        if operation_id in self._operations:
            return
        amount = qty(size)
        if self.tokens.get(yes_token_id, Decimal(0)) < amount or self.tokens.get(
            no_token_id, Decimal(0)
        ) < amount:
            raise ValueError("insufficient complete sets for merge")
        self.tokens[yes_token_id] -= amount
        self.tokens[no_token_id] -= amount
        self.cash += amount - max(Decimal(0), fee)
        self._operations.add(operation_id)
        self.operation_log[operation_id] = {
            "operation_type": "MERGE",
            "cash_delta": amount - max(Decimal(0), fee),
            "token_deltas": {yes_token_id: -amount, no_token_id: -amount},
            "fee": max(Decimal(0), fee),
        }

    def neg_risk_convert(
        self,
        *,
        operation_id: str,
        source_no_token_id: str,
        source_yes_token_id: str,
        event_yes_token_ids: tuple[str, ...],
        size: Decimal,
        augmented_neg_risk: bool = False,
        fee: Decimal = Decimal(0),
    ) -> dict[str, Decimal]:
        """Explicitly convert one event outcome NO into all other YES tokens."""

        if operation_id in self._operations:
            return dict(self.operation_log[operation_id]["token_deltas"])
        amount = qty(size)
        conversion = plan_no_to_other_yes(
            source_no_asset_id=source_no_token_id,
            event_yes_asset_ids=event_yes_token_ids,
            quantity=amount,
            augmented_neg_risk=augmented_neg_risk,
            source_yes_asset_id=source_yes_token_id,
        )
        if self.tokens.get(source_no_token_id, Decimal(0)) < amount:
            raise ValueError("insufficient source NO inventory for negative-risk conversion")
        operation_fee = max(Decimal(0), fee)
        if self.cash < operation_fee:
            raise ValueError("insufficient cash for negative-risk conversion fee")
        self.tokens[source_no_token_id] -= amount
        token_deltas: dict[str, Decimal] = {source_no_token_id: -amount}
        for token_id, token_delta in conversion.yes_deltas.items():
            self.tokens[token_id] = self.tokens.get(token_id, Decimal(0)) + token_delta
            token_deltas[token_id] = token_deltas.get(token_id, Decimal(0)) + token_delta
        self.cash -= operation_fee
        self._operations.add(operation_id)
        self.operation_log[operation_id] = {
            "operation_type": "NEG_RISK_CONVERT",
            "cash_delta": -operation_fee,
            "token_deltas": token_deltas,
            "fee": operation_fee,
            "conversion_status": conversion.status,
        }
        return dict(token_deltas)

    def redeem(
        self,
        *,
        operation_id: str,
        payout_vector: PayoutVector,
        fee: Decimal = Decimal(0),
    ) -> Decimal:
        if operation_id in self._operations:
            return Decimal(0)
        payout = sum(
            (
                self.tokens.get(token_id, Decimal(0))
                * payout_vector.payout_for(token_id)
                for token_id in payout_vector.payouts
            ),
            Decimal(0),
        )
        redeemed = {
            token_id: self.tokens.get(token_id, Decimal(0))
            for token_id in payout_vector.payouts
        }
        for token_id in payout_vector.payouts:
            self.tokens[token_id] = Decimal(0)
        net = max(Decimal(0), payout - max(Decimal(0), fee))
        self.cash += net
        self._operations.add(operation_id)
        self.operation_log[operation_id] = {
            "operation_type": "REDEEM",
            "cash_delta": net,
            "token_deltas": {
                token_id: -amount for token_id, amount in redeemed.items()
            },
            "fee": max(Decimal(0), fee),
            "payout_vector_hash": payout_vector.truth_hash,
        }
        return net

    @staticmethod
    def ctf_system_flow(match: ExecutionMatch) -> dict[str, Decimal]:
        if match.match_type_hint == CtfMatchType.MINT:
            return {
                "collateral": -match.qty,
                "yes": match.qty,
                "no": match.qty,
            }
        if match.match_type_hint == CtfMatchType.MERGE:
            return {
                "collateral": match.qty,
                "yes": -match.qty,
                "no": -match.qty,
            }
        return {
            "collateral": Decimal(0),
            "yes": Decimal(0),
            "no": Decimal(0),
        }

    @staticmethod
    def _account_flow(match: ExecutionMatch) -> tuple[Decimal, Decimal]:
        net_cost = match.notional + match.fee - match.rebate_accrual
        if match.raw_side == RawOrderSide.BUY:
            return -net_cost, match.qty
        return match.notional - match.fee + match.rebate_accrual, -match.qty
