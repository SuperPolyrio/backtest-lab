"""Dual-clock, deterministic prediction-market L2 replay session."""

from __future__ import annotations

import heapq
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256
from typing import Any

from quant.simulator.economics import FeeEngine, FeeScheduleRegistry, LiquidityRole

from .book import (
    EconomicLevelKey,
    EconomicResidualBook,
    TradeDeltaReconciler,
    canonical_action,
    canonical_book_side,
    canonical_yes_price,
    resting_side,
)
from .contracts import (
    BookDeltaEvent,
    BookFrameBatchEvent,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    BookSyncState,
    CoverageManifest,
    EventEnvelope,
    EventType,
    ExecutionMatch,
    MarketLifecycleEvent,
    MatchFinalityState,
    ORDER_AMOUNT_TOLERANCE,
    OrderAmountUnit,
    OrderGroupIntent,
    OrderGroupPolicy,
    OrderGroupResult,
    OrderStatus,
    Pml2OrderIntent,
    Pml2OrderResult,
    ReplayAuditMode,
    SettlementReceipt,
    SubmissionOnStaleBook,
    SubmissionPolicy,
    TimeInForce,
    TradeEvent,
    TradingMode,
    TransportCoverageWindow,
    VenueAdmissionStatus,
    canonical_hash,
    canonical_value,
    qty,
)
from .maker import (
    EconomicMakerQueueLedger,
    MakerFillAllocation,
    MakerOrderState,
    MakerTemporalConflict,
)
from .profiles import Pml2Profile, get_pml2_profile
from .survival import (
    MakerSurvivalArtifact,
    load_default_maker_survival_artifact,
)


@dataclass
class SessionOrderState:
    order: Pml2OrderIntent
    exchange_arrival_ts: datetime
    status: OrderStatus = OrderStatus.PENDING
    reason: str = "scheduled"
    fills: list[ExecutionMatch] = field(default_factory=list)
    remaining_size: Decimal = Decimal(0)
    remaining_amount: Decimal = Decimal(0)
    admission_diagnostics: list[str] = field(default_factory=list)
    maker_state: MakerOrderState | None = None
    maker_survival_forecast: dict[str, Any] | None = None
    counterfactual_impact: dict[str, Any] | None = None
    venue_delay_applied: bool = False
    effective_submit_ts: datetime | None = None
    data_wait_deadline: datetime | None = None
    data_wait_initial_reason: str | None = None
    data_wait_release_event_id: str | None = None
    data_wait_release_count: int = 0

    def __post_init__(self) -> None:
        self.remaining_size = self.order.requested_share_size
        self.remaining_amount = self.order.effective_requested_amount


@dataclass
class SessionOrderGroupState:
    intent: OrderGroupIntent
    status: str = "PENDING"
    reason: str = "scheduled"
    executed_hedge_order_ids: list[str] = field(default_factory=list)
    venue_delay_applied: bool = False


class ReplayExecutionSession:
    """One stateful execution session per backtest run.

    Exchange state is updated at ``exchange_ts``.  Observed state is updated
    only when the same event reaches ``local_ts``.  Strategy actions are sorted
    after historical events when their timestamps are otherwise ambiguous.
    """

    MODEL_NAME = "PREDICTION_L2_REPLAY_V1"
    EVENT_ORDER_POLICY = "pml2_market_data_before_order_arrival_v2"
    LEGACY_EVENT_ORDER_POLICY = "pml2_contract_priority_legacy_v1"
    _MARKET_DATA_EVENT_TYPES = frozenset(
        {
            EventType.BOOK_SNAPSHOT,
            EventType.BOOK_FRAME_BATCH,
            EventType.BOOK_LEVEL_BATCH,
            EventType.BOOK_DELTA,
            EventType.TRADE,
        }
    )
    _WAITABLE_DATA_REASONS = frozenset(
        {
            "book_uninitialized",
            "book_syncing",
            "book_gap",
            "book_stale",
            "book_handover_pending",
            "book_waiting_for_snapshot",
            "book_waiting_for_snapshot_after_transport_gap",
            "book_timestamp_missing",
            "book_snapshot_timestamp_missing",
            "source_handover_unverified",
        }
    )

    def __init__(
        self,
        *,
        run_id: str,
        profile: Pml2Profile | str = "realistic",
        cold_restore_used: bool = False,
        sequence_step_by_source: Mapping[str, int] | None = None,
        market_data_first_on_tie: bool = True,
        fee_schedules: FeeScheduleRegistry | None = None,
        maker_survival_artifact: MakerSurvivalArtifact | None = None,
        submission_policy: SubmissionPolicy | None = None,
        execution_model_name: str | None = None,
        audit_mode: ReplayAuditMode | str = ReplayAuditMode.FULL,
    ) -> None:
        self.run_id = str(run_id)
        self.profile = (
            profile if isinstance(profile, Pml2Profile) else get_pml2_profile(profile)
        )
        self.submission_policy = submission_policy or SubmissionPolicy()
        self.execution_model_name = str(execution_model_name or self.MODEL_NAME)
        self.audit_mode = (
            audit_mode
            if isinstance(audit_mode, ReplayAuditMode)
            else ReplayAuditMode(str(audit_mode).strip().upper())
        )
        self.exchange_book = EconomicResidualBook(
            self.profile,
            sequence_step_by_source=sequence_step_by_source,
        )
        self.observed_book = EconomicResidualBook(
            self.profile,
            sequence_step_by_source=sequence_step_by_source,
        )
        self.maker_queues = EconomicMakerQueueLedger(self.profile)
        self.reconciler = TradeDeltaReconciler(
            match_window_ms=self.profile.trade_delta_match_window_ms
        )
        self.fee_schedules = fee_schedules
        self.maker_survival_artifact = (
            maker_survival_artifact
            if maker_survival_artifact is not None
            else load_default_maker_survival_artifact()
        )
        self.orders: dict[str, SessionOrderState] = {}
        self.order_groups: dict[str, SessionOrderGroupState] = {}
        self.matches: list[ExecutionMatch] = []
        self.settlement_receipts: list[SettlementReceipt] = []
        self.audit_events: list[dict[str, Any]] = []
        self.observed_event_ids: dict[str, datetime] = {}
        self._queue: list[tuple[datetime, int, int, int, str, EventEnvelope]] = []
        self._schedule_sequence = 0
        self._fill_sequence = 0
        self._audit_hash = "0" * 64
        self._audit_event_count = 0
        self._audited_match_count = 0
        self._current_ts: datetime | None = None
        self._source_ids: set[str] = set()
        self._full_snapshot_ids: list[str] = []
        self._gap_event_ids: list[str] = []
        self._event_start: datetime | None = None
        self._event_end: datetime | None = None
        self._source_handover_verified = True
        self._cold_restore_used = bool(cold_restore_used)
        self._cold_restore_exchange_clock_verified = not self._cold_restore_used
        self._cold_restore_clock_evidence: dict[str, Any] | None = None
        self._market_data_first_on_tie = bool(market_data_first_on_tie)
        self._event_order_policy = (
            self.EVENT_ORDER_POLICY
            if self._market_data_first_on_tie
            else self.LEGACY_EVENT_ORDER_POLICY
        )
        self._event_fingerprints: dict[str, str] = {}
        self._exchange_rejected_frame_ids: set[str] = set()
        self._evidence_link_fingerprints: dict[str, str] = {}
        self._market_event_counts: dict[EventType, int] = {}
        self._duplicate_event_count = 0
        self._stale_order_count = 0
        self._truncated_snapshot_count = 0
        self._level_update_count = 0
        self._transport_coverage_proof_ids: set[str] = set()
        self._waiting_order_ids_by_condition: dict[str, dict[str, None]] = {}
        self._local_event_listeners: list[
            Callable[[EventEnvelope, datetime], None]
        ] = []

    @property
    def current_ts(self) -> datetime | None:
        return self._current_ts

    @property
    def replay_hash(self) -> str:
        return self._audit_hash

    @property
    def audit_event_count(self) -> int:
        return self._audit_event_count

    @property
    def execution_state_hash(self) -> str:
        return canonical_hash(
            {
                "exchange_book_hash": self.exchange_book.state_hash,
                "orders": [item.as_dict() for item in self.results()],
                "match_hashes": [item.audit_hash for item in self.matches],
                "settlement_receipts": [
                    asdict(item) for item in self.settlement_receipts
                ],
            }
        )

    def ingest_snapshot(self, event: BookSnapshotEvent) -> None:
        envelope = EventEnvelope(
            event_id=event.snapshot_id,
            event_type=EventType.BOOK_SNAPSHOT,
            exchange_ts=event.exchange_ts,
            local_ts=event.local_ts,
            source=event.source,
            source_sequence=event.sequence or 0,
            payload=event,
        )
        if not self._track_market_event(envelope):
            return
        self._schedule(
            envelope,
            at=event.source_received_ts or event.exchange_ts,
        )
        self._schedule_local_delivery(envelope)
        if event.is_full_depth:
            self._full_snapshot_ids.append(event.snapshot_id)
        if event.is_truncated or not event.is_full_depth:
            self._truncated_snapshot_count += 1

    def ingest_delta(self, event: BookDeltaEvent) -> None:
        envelope = EventEnvelope(
            event_id=event.event_id,
            event_type=EventType.BOOK_DELTA,
            exchange_ts=event.exchange_ts,
            local_ts=event.local_ts,
            source=event.source,
            source_sequence=event.sequence or 0,
            payload=event,
        )
        if not self._track_market_event(envelope):
            return
        self._schedule(
            envelope,
            at=event.source_received_ts or event.exchange_ts,
        )
        self._schedule_local_delivery(envelope)

    def ingest_frame_batch(self, event: BookFrameBatchEvent) -> None:
        envelope = EventEnvelope(
            event_id=event.event_id,
            event_type=EventType.BOOK_FRAME_BATCH,
            exchange_ts=event.exchange_ts,
            local_ts=event.local_ts,
            source=event.source,
            source_sequence=event.source_sequence,
            source_batch_sequence=event.source_batch_sequence,
            payload=event,
        )
        if not self._track_market_event(envelope):
            return
        self._level_update_count += sum(len(batch.updates) for batch in event.batches)
        self._schedule(envelope, at=event.source_received_ts)
        self._schedule_local_delivery(envelope)

    def ingest_level_batch(self, event: BookLevelBatchEvent) -> None:
        envelope = EventEnvelope(
            event_id=event.event_id,
            event_type=EventType.BOOK_LEVEL_BATCH,
            exchange_ts=event.exchange_ts,
            local_ts=event.local_ts,
            source=event.source,
            source_sequence=event.source_sequence,
            source_batch_sequence=event.source_batch_sequence,
            payload=event,
        )
        if not self._track_market_event(envelope):
            return
        self._level_update_count += len(event.updates)
        self._schedule(
            envelope,
            at=event.source_received_ts or event.exchange_ts,
        )
        self._schedule_local_delivery(envelope)

    def ingest_trade(self, event: TradeEvent) -> None:
        envelope = EventEnvelope(
            event_id=event.event_id,
            event_type=EventType.TRADE,
            exchange_ts=event.exchange_ts,
            local_ts=event.local_ts,
            source=event.source,
            source_sequence=event.source_sequence,
            payload=event,
        )
        if not self._track_trade_evidence(event):
            return
        if not self._track_market_event(envelope):
            return
        self._schedule(
            envelope,
            at=event.source_received_ts or event.exchange_ts,
        )
        self._schedule_local_delivery(envelope)

    def ingest_lifecycle(self, event: MarketLifecycleEvent) -> None:
        envelope = EventEnvelope(
            event_id=event.event_id,
            event_type=EventType.MARKET_LIFECYCLE,
            exchange_ts=event.exchange_ts,
            local_ts=event.local_ts,
            source=event.source,
            source_sequence=event.source_sequence,
            payload=event,
        )
        if not self._track_market_event(envelope):
            return
        self._schedule(envelope, at=event.exchange_ts)
        self._schedule_local_delivery(envelope)

    def register_transport_coverage(self, window: TransportCoverageWindow) -> None:
        self.exchange_book.register_transport_coverage(window)
        self.observed_book.register_transport_coverage(window)
        self._transport_coverage_proof_ids.add(window.proof_id)

    def ingest_external_signal(
        self,
        *,
        event_id: str,
        available_at: datetime,
        source: str,
        payload: Any = None,
    ) -> None:
        envelope = EventEnvelope(
            event_id=event_id,
            event_type=EventType.EXTERNAL_SIGNAL,
            exchange_ts=available_at,
            local_ts=available_at,
            source=source,
            payload=payload,
        )
        self._schedule(envelope, at=available_at)

    def add_local_event_listener(
        self, listener: Callable[[EventEnvelope, datetime], None]
    ) -> None:
        """Observe only causally delivered market, signal, and response events."""

        if listener not in self._local_event_listeners:
            self._local_event_listeners.append(listener)

    def register_verified_cold_restore_clock_evidence(
        self,
        *,
        restore_count: int,
        baseline_clock_count: int,
        raw_event_clock_count: int,
        frame_evidence_count: int,
        source_manifest_hash: str,
    ) -> None:
        """Unlock cold-clock quality only from aggregate archive receipts."""

        restores = int(restore_count)
        baselines = int(baseline_clock_count)
        raw_events = int(raw_event_clock_count)
        frame_receipts = int(frame_evidence_count)
        manifest_hash = str(source_manifest_hash).strip()
        if not self._cold_restore_used:
            raise ValueError("cold restore clock evidence requires cold_restore_used")
        if restores <= 0 or baselines != restores * 2:
            raise ValueError("verified cold restore requires both outcome baselines")
        if raw_events < 0 or frame_receipts != restores:
            raise ValueError("verified cold restore frame evidence is incomplete")
        if not manifest_hash:
            raise ValueError("verified cold restore requires source manifest hash")
        evidence = canonical_value(
            {
                "evidence_version": "pml2-cold-restore-clock-evidence-v1",
                "restore_count": restores,
                "baseline_clock_count": baselines,
                "raw_event_clock_count": raw_events,
                "frame_evidence_count": frame_receipts,
                "source_manifest_hash": manifest_hash,
            }
        )
        self._cold_restore_clock_evidence = {
            **evidence,
            "evidence_hash": canonical_hash(evidence),
        }
        self._cold_restore_exchange_clock_verified = True

    def submit_order(self, order: Pml2OrderIntent) -> None:
        if order.run_id != self.run_id:
            raise ValueError("order run_id does not match replay session")
        if order.order_id in self.orders:
            raise ValueError(f"duplicate order_id: {order.order_id}")
        latency = (
            self.profile.entry_latency_ms
            if order.entry_latency_ms is None
            else max(0, order.entry_latency_ms)
        )
        arrival = order.submit_ts + timedelta(milliseconds=latency)
        state = SessionOrderState(order=order, exchange_arrival_ts=arrival)
        self.orders[order.order_id] = state
        action = EventEnvelope(
            event_id=f"strategy:{order.order_id}",
            event_type=EventType.STRATEGY_ACTION,
            exchange_ts=order.submit_ts,
            local_ts=order.submit_ts,
            source=f"strategy:{order.strategy_id}",
            payload={"kind": "SUBMIT", "order_id": order.order_id},
        )
        self._schedule(action, at=order.submit_ts)

    def submit_order_group(self, group: OrderGroupIntent) -> None:
        if group.run_id != self.run_id:
            raise ValueError("order group run_id does not match replay session")
        if group.group_id in self.order_groups:
            raise ValueError(f"duplicate order group_id: {group.group_id}")
        all_legs = (*group.legs, *group.hedge_legs)
        duplicates = [
            item.order_id for item in all_legs if item.order_id in self.orders
        ]
        if duplicates:
            raise ValueError(f"duplicate order_id in order group: {duplicates[0]}")
        state = SessionOrderGroupState(intent=group)
        self.order_groups[group.group_id] = state
        if group.policy == OrderGroupPolicy.SEQUENTIAL:
            for leg in group.legs:
                self.submit_order(leg)
            return
        primary_arrivals = {
            leg.submit_ts + timedelta(milliseconds=self._entry_latency_ms(leg))
            for leg in group.legs
        }
        primary_submits = {leg.submit_ts for leg in group.legs}
        if len(primary_arrivals) != 1 or len(primary_submits) != 1:
            self.order_groups.pop(group.group_id, None)
            raise ValueError(
                f"{group.policy.value} requires one shared submit and arrival timestamp"
            )
        for leg in all_legs:
            arrival = leg.submit_ts + timedelta(
                milliseconds=self._entry_latency_ms(leg)
            )
            order_state = SessionOrderState(
                order=leg,
                exchange_arrival_ts=arrival,
                reason=(
                    "hedge_contingent" if leg in group.hedge_legs else "group_scheduled"
                ),
            )
            self.orders[leg.order_id] = order_state
        submit_ts = next(iter(primary_submits))
        arrival_ts = next(iter(primary_arrivals))
        action = EventEnvelope(
            event_id=f"strategy-group:{group.group_id}",
            event_type=EventType.STRATEGY_ACTION,
            exchange_ts=submit_ts,
            local_ts=submit_ts,
            source=f"strategy:{group.strategy_id}",
            payload={
                "kind": "SUBMIT_GROUP",
                "group_id": group.group_id,
                "arrival_ts": arrival_ts,
            },
        )
        self._schedule(action, at=submit_ts)

    def submit_cancel(
        self,
        order_id: str,
        *,
        signal_ts: datetime,
        cancel_latency_ms: int | None = None,
    ) -> None:
        state = self.orders.get(order_id)
        if state is None:
            raise KeyError(order_id)
        latency = (
            state.order.cancel_latency_ms
            if cancel_latency_ms is None
            else cancel_latency_ms
        )
        latency = self.profile.cancel_latency_ms if latency is None else max(0, latency)
        action = EventEnvelope(
            event_id=f"strategy-cancel:{order_id}:{signal_ts.isoformat()}",
            event_type=EventType.STRATEGY_ACTION,
            exchange_ts=signal_ts,
            local_ts=signal_ts,
            source=f"strategy:{state.order.strategy_id}",
            payload={
                "kind": "CANCEL",
                "order_id": order_id,
                "arrival_ts": signal_ts + timedelta(milliseconds=latency),
            },
        )
        self._schedule(action, at=signal_ts)

    def mark_source_handover_pending(
        self, condition_id: str, *, event_id: str, at: datetime
    ) -> None:
        self._source_handover_verified = False
        self._gap_event_ids.append(event_id)
        envelope = EventEnvelope(
            event_id=event_id,
            event_type=EventType.MARKET_LIFECYCLE,
            exchange_ts=at,
            local_ts=at,
            source="source_handover",
            payload={
                "kind": "HANDOVER_PENDING",
                "condition_id": condition_id,
                "at": at,
            },
        )
        self._schedule(envelope, at=at)

    def verify_source_handover(
        self, condition_id: str, *, event_id: str, at: datetime
    ) -> None:
        envelope = EventEnvelope(
            event_id=event_id,
            event_type=EventType.MARKET_LIFECYCLE,
            exchange_ts=at,
            local_ts=at,
            source="source_handover",
            payload={
                "kind": "HANDOVER_VERIFIED",
                "condition_id": condition_id,
                "at": at,
            },
        )
        self._schedule(envelope, at=at)

    def run(self, *, until: datetime | None = None) -> None:
        limit = None if until is None else self._utc(until)
        while self._queue and (limit is None or self._queue[0][0] <= limit):
            scheduled_ts, _, _, _, _, envelope = heapq.heappop(self._queue)
            self._current_ts = scheduled_ts
            self._process(envelope, scheduled_ts)

    def result(self, order_id: str) -> Pml2OrderResult:
        state = self.orders[order_id]
        return Pml2OrderResult(
            order=state.order,
            status=state.status,
            reason=state.reason,
            exchange_arrival_ts=state.exchange_arrival_ts,
            fills=tuple(state.fills),
            remaining_size=state.remaining_size,
            remaining_amount=state.remaining_amount,
            admission_diagnostics=tuple(state.admission_diagnostics),
            queue_ahead=(
                None if state.maker_state is None else state.maker_state.queue_ahead
            ),
            maker_survival_forecast=state.maker_survival_forecast,
            counterfactual_impact=state.counterfactual_impact,
        )

    def results(self) -> tuple[Pml2OrderResult, ...]:
        return tuple(self.result(order_id) for order_id in sorted(self.orders))

    def submission_audit(self, order_id: str) -> dict[str, Any]:
        state = self.orders[order_id]
        return canonical_value(
            {
                "order_id": order_id,
                "amount_unit": state.order.amount_unit,
                "requested_amount": state.order.effective_requested_amount,
                "requested_share_size": state.order.requested_share_size,
                "venue_admission": state.order.venue_admission,
                "venue_admission_evidence_id": (
                    state.order.venue_admission_evidence_id
                ),
                "admission_diagnostics": state.admission_diagnostics,
                "signal_ts": state.order.signal_ts,
                "requested_submit_ts": state.order.submit_ts,
                "effective_submit_ts": state.effective_submit_ts,
                "planned_exchange_arrival_ts": (
                    state.order.submit_ts
                    + timedelta(milliseconds=self._entry_latency_ms(state.order))
                ),
                "exchange_arrival_ts": (
                    state.exchange_arrival_ts
                    if state.effective_submit_ts is not None
                    else None
                ),
                "submission_policy": self.submission_policy.as_dict(),
                "data_wait_deadline": state.data_wait_deadline,
                "data_wait_initial_reason": state.data_wait_initial_reason,
                "release_source_event_id": state.data_wait_release_event_id,
                "release_count": state.data_wait_release_count,
                "final_status": state.status,
                "final_reason": state.reason,
            }
        )

    def group_result(self, group_id: str) -> OrderGroupResult:
        group_state = self.order_groups[group_id]
        intent = group_state.intent
        primary = tuple(item.order_id for item in intent.legs)
        hedges = tuple(item.order_id for item in intent.hedge_legs)
        statuses = {
            order_id: self.orders[order_id].status.value
            for order_id in (*primary, *hedges)
        }
        status, reason = self._resolved_group_status(group_state)
        return OrderGroupResult(
            group_id=group_id,
            policy=intent.policy,
            status=status,
            reason=reason,
            primary_order_ids=primary,
            hedge_order_ids=hedges,
            executed_hedge_order_ids=tuple(group_state.executed_hedge_order_ids),
            leg_statuses=statuses,
        )

    def group_results(self) -> tuple[OrderGroupResult, ...]:
        return tuple(
            self.group_result(group_id) for group_id in sorted(self.order_groups)
        )

    def information_available(self, event_id: str, at: datetime) -> bool:
        delivered = self.observed_event_ids.get(event_id)
        return delivered is not None and delivered <= self._utc(at)

    def coverage_manifest(self) -> CoverageManifest:
        gaps = set(self._gap_event_ids)
        for condition in self.exchange_book.conditions.values():
            gaps.update(condition.gap_event_ids)
        mirror_mismatch_count = len(self.exchange_book.mirror_mismatches)
        unresolved_states = {
            BookSyncState.UNINITIALIZED,
            BookSyncState.SYNCING,
            BookSyncState.GAP,
            BookSyncState.STALE,
            BookSyncState.HANDOVER_PENDING,
            BookSyncState.WAITING_FOR_SNAPSHOT,
        }
        unresolved_quality = any(
            condition.sync_state in unresolved_states
            for condition in self.exchange_book.conditions.values()
        )
        if unresolved_quality or not self._source_handover_verified:
            quality = "REJECTED"
        elif (
            (self._cold_restore_used and not self._cold_restore_exchange_clock_verified)
            or gaps
            or mirror_mismatch_count
            or self._duplicate_event_count
            or self._stale_order_count
            or self._truncated_snapshot_count
        ):
            quality = "VALID_WITH_DEGRADATION"
        else:
            quality = "VALID"
        return CoverageManifest(
            run_id=self.run_id,
            source_ids=tuple(sorted(self._source_ids)),
            start_ts=self._event_start,
            end_ts=self._event_end,
            full_snapshot_ids=tuple(sorted(set(self._full_snapshot_ids))),
            gap_event_ids=tuple(sorted(gaps)),
            source_handover_verified=self._source_handover_verified,
            cold_restore_used=self._cold_restore_used,
            event_order_policy=self._event_order_policy,
            event_count=sum(self._market_event_counts.values()),
            snapshot_count=self._market_event_counts.get(EventType.BOOK_SNAPSHOT, 0),
            delta_count=(
                self._market_event_counts.get(EventType.BOOK_DELTA, 0)
                + self._level_update_count
            ),
            trade_count=self._market_event_counts.get(EventType.TRADE, 0),
            duplicate_event_count=self._duplicate_event_count,
            mirror_mismatch_count=mirror_mismatch_count,
            stale_order_count=self._stale_order_count,
            truncated_snapshot_count=self._truncated_snapshot_count,
            transport_coverage_window_count=len(
                self._transport_coverage_proof_ids
            ),
            transport_coverage_proof_ids=tuple(
                sorted(self._transport_coverage_proof_ids)
            ),
            data_quality_status=quality,
        )

    def report(self) -> dict[str, Any]:
        results = self.results()
        manifest = self.coverage_manifest()
        requested = sum(
            (item.order.requested_share_size for item in results), Decimal(0)
        )
        filled = sum((item.filled_size for item in results), Decimal(0))
        amount_by_unit: dict[str, dict[str, Decimal]] = {}
        for result in results:
            unit = result.order.amount_unit.value
            bucket = amount_by_unit.setdefault(
                unit,
                {
                    "requested_amount": Decimal(0),
                    "filled_amount": Decimal(0),
                    "remaining_amount": Decimal(0),
                },
            )
            bucket["requested_amount"] += result.order.effective_requested_amount
            bucket["filled_amount"] += result.filled_amount
            bucket["remaining_amount"] += result.remaining_amount or Decimal(0)
        by_status: dict[str, int] = {}
        for result in results:
            by_status[result.status.value] = by_status.get(result.status.value, 0) + 1
        return canonical_value(
            {
                "schema_version": "prediction_l2_replay_report_v1",
                "execution_model": self.execution_model_name,
                "profile": self.profile.as_dict(),
                "order_count": len(results),
                "match_count": len(self.matches),
                "requested_size": requested,
                "filled_size": filled,
                "fill_ratio": Decimal(0) if requested <= 0 else filled / requested,
                "amount_totals_by_unit": amount_by_unit,
                "status_counts": by_status,
                "min_order_size_contract_conflict_count": sum(
                    "MIN_ORDER_SIZE_CONTRACT_CONFLICT"
                    in item.admission_diagnostics
                    for item in results
                ),
                "counterfactual_impact_rejection_count": sum(
                    "COUNTERFACTUAL_IMPACT_UNMODELED" in item.reason for item in results
                ),
                "counterfactual_impact_warning_count": sum(
                    item.counterfactual_impact is not None
                    and item.counterfactual_impact.get("decision")
                    == "VISIBLE_DEPTH_ONLY_REMAINDER_UNMODELED"
                    for item in results
                ),
                "replay_hash": self.replay_hash,
                "audit_contract": {
                    "mode": self.audit_mode,
                    "event_count": self.audit_event_count,
                    "stored_event_count": len(self.audit_events),
                    "execution_state_hash": self.execution_state_hash,
                },
                "temporal_contract": {
                    "event_order_policy": self._event_order_policy,
                    "market_data_first_on_tie": self._market_data_first_on_tie,
                    "sequence_step_by_source": (
                        self.exchange_book.sequence_step_by_source
                    ),
                    "cold_restore_exchange_clock_status": (
                        "NOT_APPLICABLE"
                        if not self._cold_restore_used
                        else (
                            "VERIFIED"
                            if self._cold_restore_exchange_clock_verified
                            else "UNVERIFIED_CHECKPOINT_CUTOFF_PROXY"
                        )
                    ),
                    "cold_restore_clock_evidence": (self._cold_restore_clock_evidence),
                },
                "coverage_manifest": {
                    **manifest.__dict__,
                    "manifest_hash": manifest.manifest_hash,
                },
                "mirror_mismatches": self.exchange_book.mirror_mismatches,
                "residual": self.exchange_book.residual_snapshot(),
                "orders": [item.as_dict() for item in results],
                "order_groups": [item.as_dict() for item in self.group_results()],
            }
        )

    def _process(self, envelope: EventEnvelope, scheduled_ts: datetime) -> None:
        if envelope.event_type == EventType.BOOK_SNAPSHOT:
            self.exchange_book.apply_snapshot(envelope.payload)
        elif envelope.event_type == EventType.BOOK_FRAME_BATCH:
            frame: BookFrameBatchEvent = envelope.payload
            applied = self.exchange_book.apply_frame_batch_atomic(frame)
            if applied is None:
                self._exchange_rejected_frame_ids.add(frame.event_id)
            for update, decrease in applied or ():
                self._process_applied_delta(update, decrease)
        elif envelope.event_type == EventType.BOOK_LEVEL_BATCH:
            if self.exchange_book.accept_batch_sequence(envelope.payload):
                for update in envelope.payload.updates:
                    self._process_delta(update, validate_sequence=False)
        elif envelope.event_type == EventType.BOOK_DELTA:
            self._process_delta(envelope.payload)
        elif envelope.event_type == EventType.TRADE:
            self._process_trade(envelope.payload)
        elif envelope.event_type == EventType.STRATEGY_ACTION:
            self._process_strategy_action(envelope.payload)
        elif envelope.event_type in {EventType.ORDER_ARRIVAL, EventType.VENUE_EXECUTE}:
            self._process_order_arrival(
                str(envelope.payload["order_id"]),
                scheduled_ts,
                venue_execute=envelope.event_type == EventType.VENUE_EXECUTE,
            )
        elif envelope.event_type == EventType.ORDER_GROUP_ARRIVAL:
            self._process_order_group_arrival(
                str(envelope.payload["group_id"]), scheduled_ts
            )
        elif envelope.event_type == EventType.CANCEL_ARRIVAL:
            self._process_cancel(str(envelope.payload["order_id"]))
        elif envelope.event_type == EventType.RECONCILE_TIMEOUT:
            self._process_reconcile_timeout(
                str(envelope.payload["pending_decrease_id"])
            )
        elif envelope.event_type == EventType.EXPIRE:
            self._process_expire(str(envelope.payload["order_id"]))
        elif envelope.event_type == EventType.LOCAL_DELIVERY:
            self._process_local_delivery(envelope.payload, scheduled_ts)
        elif envelope.event_type == EventType.EXTERNAL_SIGNAL:
            self.observed_event_ids[envelope.event_id] = scheduled_ts
            self._notify_local_event(envelope, scheduled_ts)
        elif envelope.event_type == EventType.MARKET_LIFECYCLE:
            self._process_lifecycle(envelope.payload)
        elif envelope.event_type == EventType.RESPONSE:
            self.observed_event_ids[envelope.event_id] = scheduled_ts
            self._notify_local_event(envelope, scheduled_ts)
        elif envelope.event_type == EventType.DATA_WAIT_TIMEOUT:
            self._process_data_wait_timeout(str(envelope.payload["order_id"]))
        self._append_audit(envelope, scheduled_ts)

    def _process_strategy_action(self, payload: dict[str, Any]) -> None:
        kind = str(payload["kind"])
        if kind == "SUBMIT_GROUP":
            group_id = str(payload["group_id"])
            arrival = payload["arrival_ts"]
            envelope = EventEnvelope(
                event_id=f"group-arrival:{group_id}:{arrival.isoformat()}",
                event_type=EventType.ORDER_GROUP_ARRIVAL,
                exchange_ts=arrival,
                local_ts=arrival,
                source="strategy_order_gateway",
                payload={"group_id": group_id},
            )
            self._schedule(envelope, at=arrival)
            return
        order_id = str(payload["order_id"])
        if kind == "SUBMIT":
            self._begin_submission(self.orders[order_id])
            return
        arrival = payload["arrival_ts"]
        envelope = EventEnvelope(
            event_id=f"cancel-arrival:{order_id}:{arrival.isoformat()}",
            event_type=EventType.CANCEL_ARRIVAL,
            exchange_ts=arrival,
            local_ts=arrival,
            source="strategy_order_gateway",
            payload={"order_id": order_id},
        )
        self._schedule(envelope, at=arrival)

    def _begin_submission(self, state: SessionOrderState) -> None:
        order = state.order
        if self.submission_policy.on_stale_book != SubmissionOnStaleBook.WAIT_FOR_FRESH:
            state.effective_submit_ts = order.submit_ts
            self._schedule_order_arrival(state)
            return
        if (
            order.tif == TimeInForce.GTD
            and order.expires_at is not None
            and order.expires_at <= order.submit_ts
        ):
            state.status = OrderStatus.EXPIRED
            state.reason = "gtd_expired_before_submission"
            return
        quality = self.observed_book.quality_reason(
            order.condition_id,
            order.submit_ts,
            clock_basis="local",
        )
        if not self._is_waitable_data_reason(quality):
            state.effective_submit_ts = order.submit_ts
            self._schedule_order_arrival(state)
            return
        assert quality is not None
        state.status = OrderStatus.WAITING_FOR_DATA
        state.reason = quality
        state.data_wait_initial_reason = quality
        self._waiting_order_ids_by_condition.setdefault(
            order.condition_id.lower(), {}
        )[order.order_id] = None
        deadline = order.submit_ts + timedelta(
            milliseconds=self.submission_policy.max_data_wait_ms
        )
        state.data_wait_deadline = deadline
        timeout = EventEnvelope(
            event_id=f"data-wait-timeout:{order.order_id}:{deadline.isoformat()}",
            event_type=EventType.DATA_WAIT_TIMEOUT,
            exchange_ts=deadline,
            local_ts=deadline,
            source="local_data_gate",
            payload={"order_id": order.order_id},
        )
        self._schedule(timeout, at=deadline)
        if order.tif == TimeInForce.GTD and order.expires_at is not None:
            expiry = EventEnvelope(
                event_id=f"pre-submit-expire:{order.order_id}:{order.expires_at.isoformat()}",
                event_type=EventType.EXPIRE,
                exchange_ts=order.expires_at,
                local_ts=order.expires_at,
                source="local_data_gate",
                payload={"order_id": order.order_id},
            )
            self._schedule(expiry, at=order.expires_at)

    @classmethod
    def _is_waitable_data_reason(cls, reason: str | None) -> bool:
        if reason in cls._WAITABLE_DATA_REASONS:
            return True
        return bool(reason and reason.startswith("transport_coverage_"))

    def _schedule_order_arrival(self, state: SessionOrderState) -> None:
        effective_submit = state.effective_submit_ts or state.order.submit_ts
        state.exchange_arrival_ts = effective_submit + timedelta(
            milliseconds=self._entry_latency_ms(state.order)
        )
        envelope = EventEnvelope(
            event_id=f"arrival:{state.order.order_id}",
            event_type=EventType.ORDER_ARRIVAL,
            exchange_ts=state.exchange_arrival_ts,
            local_ts=state.exchange_arrival_ts,
            source="strategy_order_gateway",
            payload={"order_id": state.order.order_id},
        )
        self._schedule(envelope, at=state.exchange_arrival_ts)

    def _process_data_wait_timeout(self, order_id: str) -> None:
        state = self.orders[order_id]
        if state.status != OrderStatus.WAITING_FOR_DATA:
            return
        self._remove_waiting_order(state)
        state.status = self.submission_policy.on_timeout
        state.reason = (
            "max_data_wait_exceeded:"
            f"{state.data_wait_initial_reason or 'l2_data_not_ready'}"
        )

    def _process_order_group_arrival(self, group_id: str, at: datetime) -> None:
        group_state = self.order_groups[group_id]
        intent = group_state.intent
        max_delay = max((self._venue_delay_ms(item) for item in intent.legs), default=0)
        if max_delay > 0 and not group_state.venue_delay_applied:
            group_state.venue_delay_applied = True
            delayed = at + timedelta(milliseconds=max_delay)
            group_state.status = "VENUE_DELAY_PENDING"
            group_state.reason = "group_venue_delay_pending"
            envelope = EventEnvelope(
                event_id=f"group-venue-execute:{intent.group_id}",
                event_type=EventType.ORDER_GROUP_ARRIVAL,
                exchange_ts=delayed,
                local_ts=delayed,
                source="venue_delay",
                payload={"group_id": intent.group_id},
            )
            self._schedule(envelope, at=delayed)
            return
        if intent.policy == OrderGroupPolicy.ALL_LEGS_FOK:
            self._process_all_legs_fok(group_state, at)
            return
        for leg in intent.legs:
            self._process_order_arrival(leg.order_id, at, venue_execute=True)
        if intent.policy == OrderGroupPolicy.HEDGE_ON_LEG_FAILURE:
            failed = any(
                self.orders[leg.order_id].status != OrderStatus.FILLED
                for leg in intent.legs
            )
            if failed:
                group_state.status = "HEDGE_PENDING"
                group_state.reason = "primary_leg_failure_triggered_hedge"
                for hedge in intent.hedge_legs:
                    state = self.orders[hedge.order_id]
                    arrival = at + timedelta(milliseconds=self._entry_latency_ms(hedge))
                    state.exchange_arrival_ts = arrival
                    state.reason = "hedge_scheduled_after_leg_failure"
                    group_state.executed_hedge_order_ids.append(hedge.order_id)
                    envelope = EventEnvelope(
                        event_id=f"hedge-arrival:{group_id}:{hedge.order_id}",
                        event_type=EventType.ORDER_ARRIVAL,
                        exchange_ts=arrival,
                        local_ts=arrival,
                        source="strategy_hedge_gateway",
                        payload={"order_id": hedge.order_id},
                    )
                    self._schedule(envelope, at=arrival)
            else:
                group_state.status = "FILLED"
                group_state.reason = "all_primary_legs_filled_hedge_not_required"
                for hedge in intent.hedge_legs:
                    state = self.orders[hedge.order_id]
                    state.status = OrderStatus.CANCELLED
                    state.reason = "hedge_not_required"
            return
        group_state.status = "BEST_EFFORT_SUBMITTED"
        group_state.reason = "legs_processed_with_shared_residual"

    def _process_all_legs_fok(
        self, group_state: SessionOrderGroupState, at: datetime
    ) -> None:
        intent = group_state.intent
        rejection = None
        for leg in intent.legs:
            rejection = self._all_legs_fok_rejection_reason(
                self.orders[leg.order_id], at
            )
            if rejection is not None:
                break
        if rejection is not None:
            self._reject_all_group_legs(group_state, rejection)
            return
        previews = self.exchange_book.preview_taker_group(intent.legs)
        for leg in intent.legs:
            if self._remaining_amount_after_previews(
                self.orders[leg.order_id], previews[leg.order_id]
            ) > ORDER_AMOUNT_TOLERANCE:
                condition = self.exchange_book.condition(leg.condition_id)
                reason = (
                    "fok_truncated_depth_unverifiable"
                    if condition.depth_truncated
                    else "fok_insufficient_economic_residual"
                )
                self._reject_all_group_legs(group_state, reason)
                return
        self.exchange_book.commit_taker_group(
            previews,
            allocation_id=f"group:{intent.group_id}",
        )
        for leg in intent.legs:
            state = self.orders[leg.order_id]
            for preview in previews[leg.order_id]:
                match = self._taker_match(leg, preview, at)
                state.fills.append(match)
                self.matches.append(match)
            state.remaining_size = Decimal(0)
            state.remaining_amount = Decimal(0)
            state.status = OrderStatus.FILLED
            state.reason = "all_legs_fok_atomic_commit"
        group_state.status = "FILLED"
        group_state.reason = "all_legs_fok_atomic_commit"

    @staticmethod
    def _remaining_amount_after_previews(
        state: SessionOrderState, previews: Iterable[Any]
    ) -> Decimal:
        consumed = sum(
            (
                state.order.amount_for_fill(
                    fill_size=preview.qty,
                    fill_price=preview.raw_price,
                )
                for preview in previews
            ),
            Decimal(0),
        )
        return qty(max(Decimal(0), state.remaining_amount - consumed))

    @staticmethod
    def _min_order_size_rejection(
        state: SessionOrderState, minimum: Decimal | None
    ) -> str | None:
        admission = state.order.venue_admission
        if admission == VenueAdmissionStatus.REJECTED_MIN_ORDER_SIZE:
            return "venue_rejected_min_order_size"
        if minimum is None or state.order.requested_share_size >= minimum:
            return None
        if admission == VenueAdmissionStatus.ACCEPTED:
            diagnostic = "MIN_ORDER_SIZE_CONTRACT_CONFLICT"
            if diagnostic not in state.admission_diagnostics:
                state.admission_diagnostics.append(diagnostic)
            return None
        return "below_market_min_order_size"

    def _all_legs_fok_rejection_reason(
        self, state: SessionOrderState, at: datetime
    ) -> str | None:
        order = state.order
        condition = self.exchange_book.condition(order.condition_id)
        if order.expires_at is not None and at >= order.expires_at:
            return "expired_before_execution"
        if condition.trading_mode != TradingMode.LIVE:
            return f"market_{condition.trading_mode.value.lower()}"
        quality = self.exchange_book.quality_reason(order.condition_id, at)
        if quality is not None:
            return quality
        min_size_rejection = self._min_order_size_rejection(state, condition.min_order_size)
        if min_size_rejection is not None:
            return min_size_rejection
        if (
            condition.tick_size is not None
            and order.limit_price % condition.tick_size != 0
        ):
            return "limit_not_tick_aligned"
        if order.post_only:
            return "all_legs_fok_cannot_be_post_only"
        if not self.exchange_book.would_cross(order):
            return "no_marketable_exchange_depth"
        self._record_counterfactual_impact(state)
        return None

    def _reject_all_group_legs(
        self, group_state: SessionOrderGroupState, reason: str
    ) -> None:
        for leg in group_state.intent.legs:
            state = self.orders[leg.order_id]
            state.status = OrderStatus.REJECTED
            state.reason = f"all_legs_fok_failed:{reason}"
        group_state.status = "FAILED"
        group_state.reason = f"all_legs_fok_failed:{reason}"

    def _process_order_arrival(
        self, order_id: str, at: datetime, *, venue_execute: bool
    ) -> None:
        state = self.orders[order_id]
        order = state.order
        if state.status not in {OrderStatus.PENDING, OrderStatus.WORKING}:
            return
        if (
            order.tif == TimeInForce.GTD
            and order.expires_at is not None
            and at >= order.expires_at
        ):
            state.status = OrderStatus.EXPIRED
            state.reason = "gtd_expired_before_execution"
            return
        condition = self.exchange_book.condition(order.condition_id)
        if condition.trading_mode in {
            TradingMode.CANCEL_ONLY,
            TradingMode.PAUSED,
            TradingMode.CLOSED,
        }:
            state.status = OrderStatus.REJECTED
            state.reason = f"market_{condition.trading_mode.value.lower()}"
            return
        quality_reason = self.exchange_book.quality_reason(order.condition_id, at)
        if quality_reason is not None:
            if quality_reason == "book_stale":
                self._stale_order_count += 1
            state.status = OrderStatus.DATA_NOT_READY
            state.reason = quality_reason
            return
        if condition.trading_mode == TradingMode.POST_ONLY and not order.post_only:
            state.status = OrderStatus.REJECTED
            state.reason = "market_post_only_mode"
            return
        min_size_rejection = self._min_order_size_rejection(
            state, condition.min_order_size
        )
        if min_size_rejection is not None:
            state.status = OrderStatus.REJECTED
            state.reason = min_size_rejection
            return
        if (
            condition.tick_size is not None
            and order.limit_price % condition.tick_size != 0
        ):
            state.status = OrderStatus.REJECTED
            state.reason = "limit_not_tick_aligned"
            return
        crossing = self.exchange_book.would_cross(order)
        if order.post_only and crossing:
            state.status = OrderStatus.REJECTED
            state.reason = "post_only_crosses_exchange_book"
            return
        if (
            crossing
            and not venue_execute
            and not state.venue_delay_applied
            and self._venue_delay_ms(order) > 0
        ):
            state.venue_delay_applied = True
            delayed = at + timedelta(milliseconds=self._venue_delay_ms(order))
            envelope = EventEnvelope(
                event_id=f"venue-execute:{order_id}",
                event_type=EventType.VENUE_EXECUTE,
                exchange_ts=delayed,
                local_ts=delayed,
                source="venue_delay",
                payload={"order_id": order_id},
            )
            self._schedule(envelope, at=delayed)
            state.reason = "venue_delay_pending"
            return
        if not crossing:
            if order.tif in {TimeInForce.GTC, TimeInForce.GTD}:
                self._rest_order(state, at)
            else:
                state.status = (
                    OrderStatus.REJECTED
                    if order.tif == TimeInForce.FOK
                    else OrderStatus.CANCELLED
                )
                state.reason = "no_marketable_exchange_depth"
            return
        self._record_counterfactual_impact(state)
        previews = self.exchange_book.preview_taker(order)
        remaining_amount = self._remaining_amount_after_previews(state, previews)
        if (
            order.tif == TimeInForce.FOK
            and remaining_amount > ORDER_AMOUNT_TOLERANCE
        ):
            state.status = OrderStatus.REJECTED
            state.reason = (
                "fok_truncated_depth_unverifiable"
                if condition.depth_truncated
                else "fok_insufficient_economic_residual"
            )
            return
        if previews:
            allocation_id = f"{order.order_id}:{len(state.fills)}"
            self.exchange_book.commit_taker(previews, allocation_id=allocation_id)
            for preview in previews:
                match = self._taker_match(order, preview, at)
                state.fills.append(match)
                self.matches.append(match)
            state.remaining_amount = remaining_amount
            state.remaining_size = qty(
                max(
                    Decimal(0),
                    order.requested_share_size
                    - sum((item.qty for item in state.fills), Decimal(0)),
                )
            )
        if state.remaining_amount <= ORDER_AMOUNT_TOLERANCE:
            state.remaining_amount = Decimal(0)
            state.remaining_size = Decimal(0)
            state.status = OrderStatus.FILLED
            state.reason = "arrival_book_walk_complete"
        elif order.tif in {TimeInForce.FAK, TimeInForce.IOC}:
            state.status = OrderStatus.PARTIAL if state.fills else OrderStatus.CANCELLED
            state.reason = (
                "truncated_depth_remainder_unknown"
                if state.fills and condition.depth_truncated
                else (
                    "ioc_remainder_cancelled"
                    if order.tif == TimeInForce.IOC
                    else "fak_remainder_cancelled"
                )
            )
        elif order.tif in {TimeInForce.GTC, TimeInForce.GTD}:
            self._rest_order(state, at)
            if state.fills:
                state.status = OrderStatus.PARTIAL
                state.reason = "crossing_fill_remainder_resting"
        else:
            raise AssertionError("FOK committed an incomplete allocation")

    def _rest_order(self, state: SessionOrderState, at: datetime) -> None:
        order = state.order
        condition = self.exchange_book.condition(order.condition_id)
        action = canonical_action(order.outcome, order.side)
        key = EconomicLevelKey(
            condition_id=order.condition_id.lower(),
            book_epoch=condition.book_epoch,
            side=resting_side(action),
            canonical_yes_price=canonical_yes_price(order.outcome, order.limit_price),
        )
        displayed = self.exchange_book.level_size(key)
        state.maker_state = self.maker_queues.admit(
            order,
            accepted_ts=at,
            book_epoch=condition.book_epoch,
            displayed_external_size=displayed,
            remaining_size=state.remaining_size,
        )
        state.maker_survival_forecast = self._maker_survival_forecast(
            state,
            at=at,
        )
        state.status = OrderStatus.WORKING if not state.fills else OrderStatus.PARTIAL
        state.reason = "resting_on_economic_queue"
        if order.tif == TimeInForce.GTD and order.expires_at is not None:
            envelope = EventEnvelope(
                event_id=f"expire:{order.order_id}",
                event_type=EventType.EXPIRE,
                exchange_ts=order.expires_at,
                local_ts=order.expires_at,
                source="venue_expiry",
                payload={"order_id": order.order_id},
            )
            self._schedule(envelope, at=order.expires_at)

    def _maker_survival_forecast(
        self,
        state: SessionOrderState,
        *,
        at: datetime,
    ) -> dict[str, Any] | None:
        artifact = self.maker_survival_artifact
        maker_state = state.maker_state
        if artifact is None or maker_state is None:
            return None
        order = state.order
        raw_horizon = order.metadata.get("maker_survival_horizon_seconds")
        if raw_horizon is None and order.expires_at is not None:
            raw_horizon = max(1, int((order.expires_at - at).total_seconds()))
        horizon = max(1, int(raw_horizon or 900))
        queue_bucket = str(
            order.metadata.get("queue_bucket")
            or _queue_bucket(maker_state.queue_ahead, order.requested_share_size)
        )
        forecast = artifact.forecast(
            decision_ts=order.signal_ts,
            horizon_seconds=horizon,
            category=str(order.metadata.get("category") or "GLOBAL"),
            side=order.side,
            quote_position=str(order.metadata.get("quote_position") or "AT_BEST"),
            queue_bucket=queue_bucket,
        )
        return forecast.as_dict()

    def _process_trade(self, event: TradeEvent) -> None:
        reconciled = self.reconciler.record_trade(event)
        self.exchange_book.apply_external_trade(
            event,
            size=reconciled.unmatched_trade_size,
        )
        try:
            allocations = self.maker_queues.process_trade(event)
        except MakerTemporalConflict:
            self.exchange_book.mark_gap(
                event.condition_id,
                f"maker_temporal_conflict:{event.event_id}",
            )
            return
        for allocation in allocations:
            state = self.orders[allocation.order.order_id]
            match = self._maker_match(allocation)
            state.fills.append(match)
            self.matches.append(match)
            state.remaining_size = qty(state.remaining_size - allocation.size)
            state.remaining_amount = state.remaining_size
            if state.remaining_size <= 0:
                state.status = OrderStatus.FILLED
                state.reason = "maker_queue_filled_by_trade"
            else:
                state.status = OrderStatus.PARTIAL
                state.reason = "maker_queue_partial_fill"

    def _process_delta(
        self,
        event: BookDeltaEvent,
        *,
        validate_sequence: bool = True,
    ) -> None:
        decrease = self.exchange_book.apply_delta(
            event,
            validate_sequence=validate_sequence,
        )
        self._process_applied_delta(event, decrease)

    def _process_applied_delta(
        self,
        event: BookDeltaEvent,
        decrease: Decimal,
    ) -> None:
        if decrease <= 0:
            return
        key = EconomicLevelKey(
            condition_id=event.condition_id.lower(),
            book_epoch=event.book_epoch,
            side=canonical_book_side(event.outcome, event.side),
            canonical_yes_price=canonical_yes_price(event.outcome, event.price),
        )
        reconciled = self.reconciler.reconcile(
            key=key,
            decrease=decrease,
            event_id=event.event_id,
            event_ts=event.exchange_ts,
            linked_trade_event_ids=event.linked_trade_event_ids,
        )
        if reconciled.unmatched_cancel_size > 0:
            self.exchange_book.classify_external_cancel(
                key, reconciled.unmatched_cancel_size
            )
            try:
                self.maker_queues.on_unmatched_book_decrease(
                    key,
                    reconciled.unmatched_cancel_size,
                    event_ts=reconciled.event_ts,
                )
            except MakerTemporalConflict:
                self.exchange_book.mark_gap(
                    event.condition_id,
                    f"maker_temporal_conflict:{event.event_id}",
                )
        if reconciled.pending_decrease_id:
            exchange_deadline = event.exchange_ts + timedelta(
                milliseconds=self.profile.trade_delta_match_window_ms
            )
            # Market data is applied on its native receive clock.  A delayed
            # frame can arrive after the exchange-time reconciliation deadline;
            # never enqueue that timeout into the already processed past.
            timeout_at = max(
                exchange_deadline,
                self._current_ts or exchange_deadline,
            )
            envelope = EventEnvelope(
                event_id=f"reconcile-timeout:{reconciled.pending_decrease_id}",
                event_type=EventType.RECONCILE_TIMEOUT,
                exchange_ts=timeout_at,
                local_ts=timeout_at,
                source="trade_delta_reconciler",
                payload={"pending_decrease_id": reconciled.pending_decrease_id},
            )
            self._schedule(envelope, at=timeout_at)

    def _process_reconcile_timeout(self, pending_decrease_id: str) -> None:
        reconciled = self.reconciler.expire_pending(pending_decrease_id)
        if reconciled is None or reconciled.unmatched_cancel_size <= 0:
            return
        self.exchange_book.classify_external_cancel(
            reconciled.key,
            reconciled.unmatched_cancel_size,
        )
        try:
            self.maker_queues.on_unmatched_book_decrease(
                reconciled.key,
                reconciled.unmatched_cancel_size,
                event_ts=reconciled.event_ts,
            )
        except MakerTemporalConflict:
            self.exchange_book.mark_gap(
                reconciled.key.condition_id,
                f"maker_temporal_conflict:{pending_decrease_id}",
            )

    def _process_cancel(self, order_id: str) -> None:
        state = self.orders[order_id]
        if state.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
            OrderStatus.REJECTED,
            OrderStatus.DATA_NOT_READY,
        }:
            return
        if self.maker_queues.cancel(order_id):
            state.status = OrderStatus.CANCELLED
            state.reason = "cancel_arrived_at_exchange"
        elif state.status == OrderStatus.PENDING:
            state.status = OrderStatus.CANCELLED
            state.reason = "cancel_arrived_before_order_execution"
        elif state.status == OrderStatus.WAITING_FOR_DATA:
            self._remove_waiting_order(state)
            state.status = OrderStatus.CANCELLED
            state.reason = "cancelled_before_venue_submission"

    def _process_expire(self, order_id: str) -> None:
        state = self.orders[order_id]
        if state.status == OrderStatus.WAITING_FOR_DATA:
            self._remove_waiting_order(state)
            state.status = OrderStatus.EXPIRED
            state.reason = "gtd_expired_while_waiting_for_data"
            return
        if self.maker_queues.expire(order_id):
            state.status = OrderStatus.EXPIRED
            state.reason = "gtd_expired_at_exchange"

    def _process_local_delivery(self, original: EventEnvelope, at: datetime) -> None:
        payload = original.payload
        if original.event_type == EventType.BOOK_SNAPSHOT:
            self.observed_book.apply_snapshot(payload)
        elif original.event_type == EventType.BOOK_FRAME_BATCH:
            if original.event_id in self._exchange_rejected_frame_ids:
                self.observed_book.mark_gap(
                    payload.condition_id,
                    f"exchange_rejected_raw_frame:{original.event_id}",
                )
                return
            if self.observed_book.apply_frame_batch_atomic(payload) is None:
                # A rejected raw frame is evidence of a gap, not information
                # that a strategy may observe or act on.
                return
        elif original.event_type == EventType.BOOK_LEVEL_BATCH:
            if not self.observed_book.accept_batch_sequence(payload):
                return
            for update in payload.updates:
                self.observed_book.apply_delta(
                    update,
                    validate_sequence=False,
                )
        elif original.event_type == EventType.BOOK_DELTA:
            self.observed_book.apply_delta(payload)
        elif original.event_type == EventType.MARKET_LIFECYCLE and isinstance(
            payload, MarketLifecycleEvent
        ):
            self.observed_book.set_trading_mode(
                payload.condition_id,
                payload.trading_mode,
                requires_fresh_snapshot=payload.requires_fresh_snapshot,
            )
        self.observed_event_ids[original.event_id] = at
        condition_id = getattr(payload, "condition_id", None)
        if condition_id is not None:
            self._release_waiting_orders(
                str(condition_id),
                at=at,
                source_event_id=original.event_id,
            )
        self._notify_local_event(original, at)

    def _release_waiting_orders(
        self,
        condition_id: str,
        *,
        at: datetime,
        source_event_id: str,
    ) -> None:
        normalized = condition_id.lower()
        waiting = self._waiting_order_ids_by_condition.get(normalized)
        if not waiting:
            return
        for order_id in tuple(waiting):
            state = self.orders[order_id]
            if state.status != OrderStatus.WAITING_FOR_DATA:
                waiting.pop(order_id, None)
                continue
            if state.data_wait_release_count:
                waiting.pop(order_id, None)
                continue
            deadline = state.data_wait_deadline
            if deadline is not None and at > deadline:
                continue
            expires_at = state.order.expires_at
            if expires_at is not None and at >= expires_at:
                continue
            quality = self.observed_book.quality_reason(
                condition_id,
                at,
                clock_basis="local",
            )
            if quality is not None:
                state.reason = quality
                continue
            waiting.pop(order_id, None)
            state.status = OrderStatus.PENDING
            state.reason = "fresh_l2_released_submission"
            state.effective_submit_ts = at
            state.data_wait_release_event_id = source_event_id
            state.data_wait_release_count += 1
            self._schedule_order_arrival(state)
        if not waiting:
            self._waiting_order_ids_by_condition.pop(normalized, None)

    def _remove_waiting_order(self, state: SessionOrderState) -> None:
        condition_id = state.order.condition_id.lower()
        waiting = self._waiting_order_ids_by_condition.get(condition_id)
        if waiting is None:
            return
        waiting.pop(state.order.order_id, None)
        if not waiting:
            self._waiting_order_ids_by_condition.pop(condition_id, None)

    def _notify_local_event(self, envelope: EventEnvelope, at: datetime) -> None:
        for listener in tuple(self._local_event_listeners):
            listener(envelope, at)

    def _process_lifecycle(
        self, payload: dict[str, Any] | MarketLifecycleEvent
    ) -> None:
        if isinstance(payload, MarketLifecycleEvent):
            self.exchange_book.set_trading_mode(
                payload.condition_id,
                payload.trading_mode,
                requires_fresh_snapshot=payload.requires_fresh_snapshot,
            )
            if payload.trading_mode == TradingMode.CLOSED:
                self._cancel_condition_orders(
                    payload.condition_id,
                    reason="market_closed_at_exchange",
                )
            return
        condition_id = str(payload["condition_id"])
        if payload["kind"] == "HANDOVER_PENDING":
            self.exchange_book.mark_handover_pending(
                condition_id,
                "source_handover",
                at=payload.get("at"),
            )
            return
        if payload["kind"] == "HANDOVER_VERIFIED":
            self._source_handover_verified = self.exchange_book.verify_handover(
                condition_id,
                at=payload.get("at"),
            )

    def _cancel_condition_orders(self, condition_id: str, *, reason: str) -> None:
        normalized = str(condition_id).lower()
        for state in self.orders.values():
            if state.order.condition_id.lower() != normalized:
                continue
            if state.status not in {
                OrderStatus.PENDING,
                OrderStatus.WAITING_FOR_DATA,
                OrderStatus.WORKING,
                OrderStatus.PARTIAL,
            }:
                continue
            self.maker_queues.cancel(state.order.order_id)
            state.status = OrderStatus.CANCELLED
            state.reason = reason

    def _taker_match(
        self, order: Pml2OrderIntent, preview: Any, at: datetime
    ) -> ExecutionMatch:
        self._fill_sequence += 1
        fill_id = f"{self.run_id}:{order.order_id}:taker:{self._fill_sequence}"
        exchange_arrival_ts = self.orders[order.order_id].exchange_arrival_ts
        receive_ts = at + timedelta(milliseconds=self._response_latency_ms(order))
        fee, fee_schedule_id, fee_source = self._fee_details(
            order=order,
            fill_id=fill_id,
            raw_price=preview.raw_price,
            size=preview.qty,
            liquidity_role=LiquidityRole.TAKER,
            at=at,
        )
        match = ExecutionMatch(
            run_id=self.run_id,
            order_id=order.order_id,
            fill_id=fill_id,
            execution_model=self.execution_model_name,
            profile=self.profile.name,
            event_group_id=preview.snapshot_id,
            condition_id=order.condition_id,
            market_id=order.market_id,
            token_id=order.asset_id,
            outcome=order.outcome,
            raw_side=order.side,
            canonical_side=canonical_action(order.outcome, order.side),
            qty=preview.qty,
            raw_price=preview.raw_price,
            canonical_yes_price=preview.canonical_yes_price,
            notional=qty(preview.qty * preview.raw_price),
            liquidity_role="TAKER",
            tif=order.tif,
            match_type_hint=order.match_type_hint,
            signal_ts=order.signal_ts,
            observed_ts=order.observed_ts,
            submit_ts=order.submit_ts,
            exchange_arrival_ts=exchange_arrival_ts,
            fill_exchange_ts=at,
            fill_receive_ts=receive_ts,
            book_epoch=preview.key.book_epoch,
            snapshot_id=preview.snapshot_id,
            source_event_ids=self._execution_source_event_ids(
                order.condition_id,
                at,
                preview.source_event_ids,
            ),
            queue_ahead_before=None,
            queue_ahead_after=None,
            residual_before=preview.residual_before,
            residual_after=preview.residual_after,
            fee=fee,
            rebate_accrual=Decimal(0),
            evidence_kind="L2_VISIBLE_DEPTH_AT_VENUE_EXECUTION",
            fee_schedule_id=fee_schedule_id,
            fee_source=fee_source,
            finality_state=MatchFinalityState.MATCHED,
            fill_block=self._fill_block(order),
        )
        self._schedule_response(match)
        return match

    def _execution_source_event_ids(
        self,
        condition_id: str,
        at: datetime,
        source_event_ids: tuple[str, ...],
    ) -> tuple[str, ...]:
        coverage = self.exchange_book.transport_coverage_at(condition_id, at)
        freshness_event_id = self.exchange_book.condition(
            condition_id
        ).last_freshness_event_id
        evidence = (*source_event_ids, freshness_event_id)
        if coverage is not None:
            evidence = (*evidence, coverage.proof_id)
        return tuple(dict.fromkeys(item for item in evidence if item))

    def _maker_match(self, allocation: MakerFillAllocation) -> ExecutionMatch:
        order = allocation.order
        self._fill_sequence += 1
        fill_id = f"{self.run_id}:{order.order_id}:maker:{self._fill_sequence}"
        evidence_available_at = max(
            allocation.exchange_ts,
            self._current_ts or allocation.exchange_ts,
        )
        receive_ts = evidence_available_at + timedelta(
            milliseconds=self._response_latency_ms(order)
        )
        condition = self.exchange_book.condition(order.condition_id)
        fee, fee_schedule_id, fee_source = self._fee_details(
            order=order,
            fill_id=fill_id,
            raw_price=allocation.raw_price,
            size=allocation.size,
            liquidity_role=LiquidityRole.MAKER,
            at=allocation.exchange_ts,
        )
        match = ExecutionMatch(
            run_id=self.run_id,
            order_id=order.order_id,
            fill_id=fill_id,
            execution_model=self.execution_model_name,
            profile=self.profile.name,
            event_group_id=allocation.source_event_id,
            condition_id=order.condition_id,
            market_id=order.market_id,
            token_id=order.asset_id,
            outcome=order.outcome,
            raw_side=order.side,
            canonical_side=canonical_action(order.outcome, order.side),
            qty=allocation.size,
            raw_price=allocation.raw_price,
            canonical_yes_price=allocation.canonical_yes_price,
            notional=qty(allocation.size * allocation.raw_price),
            liquidity_role="MAKER",
            tif=order.tif,
            match_type_hint=order.match_type_hint,
            signal_ts=order.signal_ts,
            observed_ts=order.observed_ts,
            submit_ts=order.submit_ts,
            exchange_arrival_ts=(self.orders[order.order_id].exchange_arrival_ts),
            fill_exchange_ts=allocation.exchange_ts,
            fill_receive_ts=receive_ts,
            book_epoch=condition.book_epoch,
            snapshot_id=condition.last_snapshot_id,
            source_event_ids=(allocation.source_event_id,),
            queue_ahead_before=allocation.queue_ahead_before,
            queue_ahead_after=allocation.queue_ahead_after,
            residual_before=None,
            residual_after=None,
            fee=fee,
            rebate_accrual=Decimal(0),
            evidence_kind="TRADE_ADVANCED_ECONOMIC_MAKER_QUEUE",
            fee_schedule_id=fee_schedule_id,
            fee_source=fee_source,
            finality_state=MatchFinalityState.MATCHED,
            fill_block=self._fill_block(order),
        )
        self._schedule_response(match)
        return match

    def _schedule_response(self, match: ExecutionMatch) -> None:
        envelope = EventEnvelope(
            event_id=f"response:{match.fill_id}",
            event_type=EventType.RESPONSE,
            exchange_ts=match.fill_receive_ts,
            local_ts=match.fill_receive_ts,
            source="venue_response",
            payload={"fill_id": match.fill_id},
        )
        self._schedule(envelope, at=match.fill_receive_ts)

    def _schedule_local_delivery(self, original: EventEnvelope) -> None:
        envelope = EventEnvelope(
            event_id=f"local:{original.event_id}",
            event_type=EventType.LOCAL_DELIVERY,
            exchange_ts=original.exchange_ts,
            local_ts=original.local_ts,
            source=original.source,
            source_sequence=original.source_sequence,
            source_batch_sequence=original.source_batch_sequence,
            payload=original,
        )
        self._schedule(envelope, at=original.local_ts)

    def _schedule(self, envelope: EventEnvelope, *, at: datetime) -> None:
        scheduled = self._utc(at)
        if self._current_ts is not None and scheduled < self._current_ts:
            raise ValueError(
                "cannot schedule an event before the current replay clock: "
                f"event_id={envelope.event_id}, scheduled={scheduled.isoformat()}, "
                f"current={self._current_ts.isoformat()}"
            )
        self._schedule_sequence += 1
        priority = self._priority(envelope.event_type)
        heapq.heappush(
            self._queue,
            (
                scheduled,
                priority,
                envelope.source_sequence,
                envelope.source_batch_sequence,
                f"{self._schedule_sequence:020d}:{envelope.event_id}",
                envelope,
            ),
        )

    def _priority(self, event_type: EventType) -> int:
        from .contracts import EVENT_PRIORITY

        if (
            self._market_data_first_on_tie
            and event_type in self._MARKET_DATA_EVENT_TYPES
        ):
            return min(
                EVENT_PRIORITY[event_type],
                EVENT_PRIORITY[EventType.ORDER_ARRIVAL] - 1,
            )
        if event_type in self._MARKET_DATA_EVENT_TYPES:
            return max(
                EVENT_PRIORITY[event_type],
                EVENT_PRIORITY[EventType.ORDER_ARRIVAL] + 10,
            )
        return EVENT_PRIORITY[event_type]

    def _append_audit(self, envelope: EventEnvelope, scheduled_ts: datetime) -> None:
        self._audit_event_count += 1
        if self.audit_mode == ReplayAuditMode.CHAIN_ONLY:
            source_event_id = (
                envelope.payload.event_id
                if envelope.event_type == EventType.LOCAL_DELIVERY
                and isinstance(envelope.payload, EventEnvelope)
                else envelope.event_id
            )
            event_payload_hash = self._event_fingerprints.get(source_event_id)
            if event_payload_hash is None:
                event_payload_hash = canonical_hash(envelope.payload)
            new_match_hashes = tuple(
                item.audit_hash
                for item in self.matches[self._audited_match_count :]
            )
            digest = sha256()
            for value in (
                self._audit_hash,
                scheduled_ts.isoformat(),
                envelope.event_id,
                envelope.event_type.value,
                envelope.source,
                envelope.source_sequence,
                envelope.source_batch_sequence,
                event_payload_hash,
                len(self.matches),
                *new_match_hashes,
            ):
                encoded = str(value).encode("utf-8")
                digest.update(len(encoded).to_bytes(8, "big"))
                digest.update(encoded)
            self._audit_hash = digest.hexdigest()
            self._audited_match_count = len(self.matches)
            return
        payload = {
            "previous_hash": self._audit_hash,
            "scheduled_ts": scheduled_ts,
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "source": envelope.source,
            "source_sequence": envelope.source_sequence,
            "source_batch_sequence": envelope.source_batch_sequence,
            "exchange_book_hash": self.exchange_book.state_hash,
            "match_hashes": [item.audit_hash for item in self.matches],
        }
        self._audit_hash = canonical_hash(payload)
        self.audit_events.append(
            canonical_value({**payload, "audit_hash": self._audit_hash})
        )
        self._audited_match_count = len(self.matches)

    def _track_market_event(self, envelope: EventEnvelope) -> bool:
        fingerprint = canonical_hash(envelope.payload)
        existing = self._event_fingerprints.get(envelope.event_id)
        if existing is not None:
            if existing != fingerprint:
                condition_id = getattr(envelope.payload, "condition_id", "")
                if condition_id:
                    self.exchange_book.mark_gap(
                        condition_id,
                        f"conflicting_duplicate:{envelope.event_id}",
                    )
                raise ValueError(
                    f"event_id {envelope.event_id!r} has conflicting payloads"
                )
            self._duplicate_event_count += 1
            return False
        self._event_fingerprints[envelope.event_id] = fingerprint
        self._market_event_counts[envelope.event_type] = (
            self._market_event_counts.get(envelope.event_type, 0) + 1
        )
        self._source_ids.add(envelope.source)
        self._event_start = (
            envelope.exchange_ts
            if self._event_start is None
            else min(self._event_start, envelope.exchange_ts)
        )
        self._event_end = (
            envelope.exchange_ts
            if self._event_end is None
            else max(self._event_end, envelope.exchange_ts)
        )
        return True

    def _track_trade_evidence(self, event: TradeEvent) -> bool:
        link_id = str(event.evidence_link_id).strip()
        if not link_id:
            return True
        fingerprint = canonical_hash(
            {
                "condition_id": event.condition_id.lower(),
                "book_epoch": event.book_epoch,
                "canonical_action": canonical_action(
                    event.outcome, event.aggressor_side
                ),
                "canonical_yes_price": canonical_yes_price(event.outcome, event.price),
                "size": event.size,
            }
        )
        existing = self._evidence_link_fingerprints.get(link_id)
        if existing is None:
            self._evidence_link_fingerprints[link_id] = fingerprint
            return True
        if existing != fingerprint:
            self.exchange_book.mark_gap(
                event.condition_id,
                f"conflicting_evidence_link:{link_id}",
            )
            raise ValueError(f"evidence_link_id {link_id!r} maps to conflicting trades")
        self._duplicate_event_count += 1
        return False

    def _venue_delay_ms(self, order: Pml2OrderIntent) -> int:
        return (
            self.profile.venue_delay_ms
            if order.venue_delay_ms is None
            else max(0, order.venue_delay_ms)
        )

    def _entry_latency_ms(self, order: Pml2OrderIntent) -> int:
        return (
            self.profile.entry_latency_ms
            if order.entry_latency_ms is None
            else max(0, order.entry_latency_ms)
        )

    def _record_counterfactual_impact(self, state: SessionOrderState) -> None:
        visible_depth = self.exchange_book.visible_depth_for_taker(state.order)
        visible_amount = self.exchange_book.visible_amount_for_taker(state.order)
        impact_ratio = (
            Decimal("Infinity")
            if visible_amount <= 0
            else state.remaining_amount / visible_amount
        )
        exceeds_warning = (
            impact_ratio > self.profile.max_order_to_visible_depth_ratio
        )
        payload: dict[str, Any] = {
            "gate_version": "pml2-visible-depth-impact-v2",
            "requested_size": state.remaining_size,
            "visible_depth_within_limit": visible_depth,
            "order_to_visible_depth_ratio": impact_ratio,
            "warning_ratio": self.profile.max_order_to_visible_depth_ratio,
            "decision": (
                "VISIBLE_DEPTH_ONLY_REMAINDER_UNMODELED"
                if exceeds_warning
                else "PASS"
            ),
            "impact_policy": "EXECUTE_VISIBLE_DEPTH_ONLY",
        }
        if state.order.amount_unit == OrderAmountUnit.QUOTE:
            payload.update(
                requested_amount=state.remaining_amount,
                amount_unit=state.order.amount_unit,
                visible_amount_within_limit=visible_amount,
            )
        state.counterfactual_impact = canonical_value(payload)

    def _resolved_group_status(self, state: SessionOrderGroupState) -> tuple[str, str]:
        intent = state.intent
        primary_states = [self.orders[item.order_id] for item in intent.legs]
        if intent.policy == OrderGroupPolicy.ALL_LEGS_FOK:
            return state.status, state.reason
        if intent.policy == OrderGroupPolicy.HEDGE_ON_LEG_FAILURE:
            if not state.executed_hedge_order_ids:
                if all(item.status == OrderStatus.FILLED for item in primary_states):
                    return "FILLED", "all_primary_legs_filled_hedge_not_required"
                return state.status, state.reason
            hedge_states = [
                self.orders[order_id] for order_id in state.executed_hedge_order_ids
            ]
            if any(
                item.status
                in {
                    OrderStatus.PENDING,
                    OrderStatus.WAITING_FOR_DATA,
                    OrderStatus.WORKING,
                }
                for item in hedge_states
            ):
                return "HEDGE_PENDING", "hedge_orders_are_not_terminal"
            if all(item.status == OrderStatus.FILLED for item in hedge_states):
                return "HEDGED", "all_registered_hedge_legs_filled"
            if any(item.fills for item in hedge_states):
                return "PARTIALLY_HEDGED", "hedge_legs_left_residual_exposure"
            return "HEDGE_FAILED", "registered_hedge_legs_did_not_fill"
        if any(
            item.status in {OrderStatus.PENDING, OrderStatus.WORKING}
            for item in primary_states
        ):
            return "PENDING", "one_or_more_legs_are_not_terminal"
        if all(item.status == OrderStatus.FILLED for item in primary_states):
            return "FILLED", "all_primary_legs_filled"
        if any(item.fills for item in primary_states):
            return "PARTIAL", "legging_risk_realized"
        return "FAILED", "no_primary_leg_filled"

    def _response_latency_ms(self, order: Pml2OrderIntent) -> int:
        return (
            self.profile.response_latency_ms
            if order.response_latency_ms is None
            else max(0, order.response_latency_ms)
        )

    @staticmethod
    def _fill_block(order: Pml2OrderIntent) -> int | None:
        raw = order.metadata.get("fill_block") or order.metadata.get("exchange_block")
        if raw in (None, ""):
            return None
        parsed = int(str(raw))
        return parsed if parsed > 0 else None

    @staticmethod
    def _fee(order: Pml2OrderIntent, raw_price: Decimal, size: Decimal) -> Decimal:
        base = raw_price * (Decimal(1) - raw_price)
        return qty(size * order.fee_rate * (base**order.fee_exponent))

    def _fee_details(
        self,
        *,
        order: Pml2OrderIntent,
        fill_id: str,
        raw_price: Decimal,
        size: Decimal,
        liquidity_role: LiquidityRole,
        at: datetime,
    ) -> tuple[Decimal, str, str]:
        if self.fee_schedules is None:
            return (
                self._fee(order, raw_price, size),
                "ORDER_INTENT_FEE_FIELDS",
                "ORDER_INTENT_POINT_IN_TIME",
            )
        schedule = self.fee_schedules.resolve(order.asset_id, at=at)
        if schedule.condition_id.lower() != order.condition_id.lower():
            raise ValueError("fee schedule condition does not match order condition")
        charge = FeeEngine.calculate(
            fill_id=fill_id,
            schedule=schedule,
            liquidity_role=liquidity_role,
            price=raw_price,
            shares=size,
        )
        return charge.total_fee, charge.fee_schedule_id, charge.source

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("scheduled timestamp must be timezone-aware")
        return value.astimezone(timezone.utc)


def _queue_bucket(queue_ahead: Decimal, order_size: Decimal) -> str:
    if queue_ahead <= 0:
        return "Q0_FRONT"
    ratio = queue_ahead / max(order_size, Decimal("0.0000000001"))
    if ratio <= 1:
        return "Q1_LE_1X"
    if ratio <= 5:
        return "Q2_LE_5X"
    if ratio <= 20:
        return "Q3_LE_20X"
    return "Q4_GT_20X"


def replay_pml2(
    *,
    run_id: str,
    profile: str,
    market_events: Iterable[
        BookSnapshotEvent
        | BookFrameBatchEvent
        | BookLevelBatchEvent
        | BookDeltaEvent
        | TradeEvent
        | MarketLifecycleEvent
    ],
    orders: Iterable[Pml2OrderIntent] = (),
    order_groups: Iterable[OrderGroupIntent] = (),
) -> ReplayExecutionSession:
    session = ReplayExecutionSession(run_id=run_id, profile=profile)
    for event in market_events:
        if isinstance(event, BookSnapshotEvent):
            session.ingest_snapshot(event)
        elif isinstance(event, BookFrameBatchEvent):
            session.ingest_frame_batch(event)
        elif isinstance(event, BookLevelBatchEvent):
            session.ingest_level_batch(event)
        elif isinstance(event, BookDeltaEvent):
            session.ingest_delta(event)
        elif isinstance(event, TradeEvent):
            session.ingest_trade(event)
        elif isinstance(event, MarketLifecycleEvent):
            session.ingest_lifecycle(event)
        else:
            raise TypeError(f"unsupported PML2 event: {type(event).__name__}")
    for order in orders:
        session.submit_order(order)
    for group in order_groups:
        session.submit_order_group(group)
    session.run()
    return session
