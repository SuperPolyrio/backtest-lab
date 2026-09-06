"""Condition-level YES/NO normalization and residual liquidity accounting."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Mapping
from copy import copy, deepcopy
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal

from .contracts import (
    BookDeltaEvent,
    BookFrameBatchEvent,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    BookSyncState,
    BookValidityMode,
    EconomicAction,
    EconomicBookSide,
    OrderAmountUnit,
    Outcome,
    Pml2OrderIntent,
    RawOrderSide,
    TradeEvent,
    TradingMode,
    TransportCoverageWindow,
    canonical_hash,
    price,
    qty,
    qty_down,
)
from .profiles import Pml2Profile


@dataclass(frozen=True, order=True)
class EconomicLevelKey:
    condition_id: str
    book_epoch: int
    side: EconomicBookSide
    canonical_yes_price: Decimal


@dataclass(frozen=True)
class LevelObservation:
    source: str
    asset_id: str
    outcome: Outcome
    event_id: str
    observed_at: datetime
    size: Decimal

    @property
    def leg_key(self) -> str:
        return f"{self.source}|{self.asset_id}|{self.outcome.value}"


@dataclass
class EconomicLevelState:
    key: EconomicLevelKey
    displayed_size: Decimal = Decimal(0)
    available_size: Decimal = Decimal(0)
    consumed_size: Decimal = Decimal(0)
    replenished_size: Decimal = Decimal(0)
    external_removed_size: Decimal = Decimal(0)
    external_trade_removed_size: Decimal = Decimal(0)
    external_cancel_removed_size: Decimal = Decimal(0)
    external_unknown_removed_size: Decimal = Decimal(0)
    version: int = 0
    snapshot_id: str = ""
    source_event_ids: tuple[str, ...] = ()
    mirror_valid: bool = True
    mirror_size_skew: Decimal = Decimal(0)
    mirror_time_skew_ms: int = 0


@dataclass
class ConditionState:
    condition_id: str
    sync_state: BookSyncState = BookSyncState.UNINITIALIZED
    book_epoch: int = -1
    last_exchange_ts: datetime | None = None
    last_source_received_ts: datetime | None = None
    last_local_ts: datetime | None = None
    last_freshness_event_id: str = ""
    last_snapshot_id: str = ""
    last_snapshot_exchange_ts: datetime | None = None
    last_snapshot_source_received_ts: datetime | None = None
    last_snapshot_local_ts: datetime | None = None
    last_sequence_by_leg: dict[str, int] = field(default_factory=dict)
    gap_event_ids: list[str] = field(default_factory=list)
    source_ids: set[str] = field(default_factory=set)
    source_handover_verified: bool = True
    market_live: bool = True
    trading_mode: TradingMode = TradingMode.LIVE
    handover_pending_since: datetime | None = None
    depth_truncated: bool = False
    depth_scope: str = "UNKNOWN"
    tick_size: Decimal | None = None
    min_order_size: Decimal | None = None


@dataclass(frozen=True)
class TakerLevelPreview:
    key: EconomicLevelKey
    qty: Decimal
    canonical_yes_price: Decimal
    raw_price: Decimal
    residual_before: Decimal
    residual_after: Decimal
    level_version: int
    snapshot_id: str
    source_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class ReconciledDecrease:
    key: EconomicLevelKey
    raw_decrease: Decimal
    matched_trade_size: Decimal
    unmatched_cancel_size: Decimal
    trade_event_ids: tuple[str, ...]
    pending_decrease_id: str = ""
    pending_trade_size: Decimal = Decimal(0)
    event_ts: datetime | None = None


@dataclass(frozen=True)
class ReconciledTrade:
    key: EconomicLevelKey
    raw_trade_size: Decimal
    previously_delta_removed_size: Decimal
    unmatched_trade_size: Decimal


@dataclass
class PendingDecrease:
    pending_id: str
    key: EconomicLevelKey
    event_ts: datetime
    remaining_size: Decimal
    linked_trade_event_ids: tuple[str, ...]
    event_group_id: str = ""


def canonical_action(outcome: Outcome, side: RawOrderSide) -> EconomicAction:
    if outcome == Outcome.YES:
        return EconomicAction(side.value)
    return EconomicAction.SELL if side == RawOrderSide.BUY else EconomicAction.BUY


def canonical_book_side(
    outcome: Outcome, side: EconomicBookSide
) -> EconomicBookSide:
    if outcome == Outcome.YES:
        return side
    return EconomicBookSide.ASK if side == EconomicBookSide.BID else EconomicBookSide.BID


def canonical_yes_price(outcome: Outcome, raw_price: Decimal) -> Decimal:
    normalized = price(raw_price)
    return normalized if outcome == Outcome.YES else price(Decimal(1) - normalized)


def token_price(outcome: Outcome, yes_price: Decimal) -> Decimal:
    normalized = price(yes_price)
    return normalized if outcome == Outcome.YES else price(Decimal(1) - normalized)


def resting_side(action: EconomicAction) -> EconomicBookSide:
    return EconomicBookSide.BID if action == EconomicAction.BUY else EconomicBookSide.ASK


def consumed_side(action: EconomicAction) -> EconomicBookSide:
    return EconomicBookSide.ASK if action == EconomicAction.BUY else EconomicBookSide.BID


class EconomicResidualBook:
    """Fused condition-level book with a run-scoped counterfactual residual."""

    def __init__(
        self,
        profile: Pml2Profile,
        *,
        sequence_step_by_source: Mapping[str, int] | None = None,
    ) -> None:
        self.profile = profile
        self.sequence_step_by_source = self._normalize_sequence_contracts(
            sequence_step_by_source
        )
        self.conditions: dict[str, ConditionState] = {}
        self.levels: dict[EconomicLevelKey, EconomicLevelState] = {}
        self._observations: dict[
            EconomicLevelKey, dict[str, LevelObservation]
        ] = {}
        self._leg_levels: dict[str, set[EconomicLevelKey]] = {}
        self._allocation_ids: set[str] = set()
        self._coverage_windows: dict[str, list[TransportCoverageWindow]] = {}
        self._coverage_starts: dict[str, list[datetime]] = {}
        self.mirror_mismatches: list[dict[str, object]] = []

    def register_transport_coverage(self, window: TransportCoverageWindow) -> None:
        condition_id = window.condition_id.lower()
        windows = self._coverage_windows.setdefault(condition_id, [])
        starts = self._coverage_starts.setdefault(condition_id, [])
        index = bisect_left(starts, window.start_ts)
        if index > 0 and windows[index - 1].end_ts > window.start_ts:
            raise ValueError("transport coverage windows cannot overlap")
        if index < len(windows) and window.end_ts > windows[index].start_ts:
            raise ValueError("transport coverage windows cannot overlap")
        starts.insert(index, window.start_ts)
        windows.insert(index, window)

    def transport_coverage_at(
        self,
        condition_id: str,
        at: datetime,
    ) -> TransportCoverageWindow | None:
        normalized = condition_id.lower()
        windows = self._coverage_windows.get(normalized)
        starts = self._coverage_starts.get(normalized)
        if not windows or not starts:
            return None
        index = bisect_right(starts, at) - 1
        if index < 0:
            return None
        window = windows[index]
        return window if window.contains(at) else None

    def blocking_transport_window_since(
        self,
        condition_id: str,
        *,
        since: datetime,
        at: datetime,
    ) -> TransportCoverageWindow | None:
        """Return a known transport break not repaired by a later snapshot."""

        normalized = condition_id.lower()
        windows = self._coverage_windows.get(normalized)
        starts = self._coverage_starts.get(normalized)
        if not windows or not starts or at <= since:
            return None
        index = max(0, bisect_right(starts, since) - 1)
        for window in windows[index:]:
            if window.start_ts >= at:
                break
            if not window.allowed and window.end_ts > since:
                return window
        return None

    @staticmethod
    def _normalize_sequence_contracts(
        contracts: Mapping[str, int] | None,
    ) -> dict[str, int]:
        """Validate opt-in source contracts for contiguous delta sequences.

        Sequence values are not universally contiguous.  For example, a
        collector-wide sequence filtered down to one asset can legitimately
        jump.  A source is therefore checked for missing sequence values only
        when its producer contract explicitly declares a positive step.
        """

        normalized: dict[str, int] = {}
        for raw_source, raw_step in (contracts or {}).items():
            source = str(raw_source).strip().lower()
            step = int(raw_step)
            if not source:
                raise ValueError("sequence contract source cannot be empty")
            if step <= 0:
                raise ValueError("sequence contract step must be positive")
            normalized[source] = step
        return normalized

    def condition(self, condition_id: str) -> ConditionState:
        key = str(condition_id).lower()
        state = self.conditions.get(key)
        if state is None:
            state = ConditionState(condition_id=key)
            self.conditions[key] = state
        return state

    def apply_snapshot(self, event: BookSnapshotEvent) -> None:
        condition_id = event.condition_id.lower()
        state = self.condition(condition_id)
        if event.book_epoch < state.book_epoch:
            self.mark_gap(condition_id, f"stale_snapshot:{event.snapshot_id}")
            return
        if event.book_epoch > state.book_epoch:
            self._reset_condition_epoch(condition_id, event.book_epoch)
            state = self.condition(condition_id)
        state.sync_state = (
            BookSyncState.CLOSED
            if state.trading_mode == TradingMode.CLOSED
            else BookSyncState.SYNCED
            if event.is_full_depth or event.is_truncated
            else BookSyncState.SYNCING
        )
        state.last_exchange_ts = event.exchange_ts
        state.last_source_received_ts = event.source_received_ts or event.exchange_ts
        state.last_local_ts = event.local_ts
        state.last_freshness_event_id = event.snapshot_id
        state.last_snapshot_id = event.snapshot_id
        state.last_snapshot_exchange_ts = event.exchange_ts
        state.last_snapshot_source_received_ts = (
            event.source_received_ts or event.exchange_ts
        )
        state.last_snapshot_local_ts = event.local_ts
        state.source_ids.add(event.source)
        state.depth_truncated = event.is_truncated or not event.is_full_depth
        state.depth_scope = str(event.depth_scope)
        state.tick_size = event.tick_size
        state.min_order_size = event.min_order_size
        leg = self._leg_key(
            condition_id, event.book_epoch, event.source, event.asset_id, event.outcome
        )
        if event.source.strip().lower() in self.sequence_step_by_source:
            if event.sequence is None:
                state.last_sequence_by_leg.pop(leg, None)
            else:
                state.last_sequence_by_leg[leg] = event.sequence
        previous = self._leg_levels.get(leg, set())
        touched = set(previous)
        for level_key in previous:
            self._observations.get(level_key, {}).pop(
                self._observation_key(event.source, event.asset_id, event.outcome),
                None,
            )
        current: set[EconomicLevelKey] = set()
        for raw_side, levels in (
            (EconomicBookSide.BID, event.bids),
            (EconomicBookSide.ASK, event.asks),
        ):
            for level in levels:
                key = EconomicLevelKey(
                    condition_id=condition_id,
                    book_epoch=event.book_epoch,
                    side=canonical_book_side(event.outcome, raw_side),
                    canonical_yes_price=canonical_yes_price(event.outcome, level.price),
                )
                self._observations.setdefault(key, {})[
                    self._observation_key(event.source, event.asset_id, event.outcome)
                ] = LevelObservation(
                    source=event.source,
                    asset_id=event.asset_id,
                    outcome=event.outcome,
                    event_id=event.snapshot_id,
                    observed_at=event.exchange_ts,
                    size=level.size,
                )
                current.add(key)
                touched.add(key)
        self._leg_levels[leg] = current
        for key in touched:
            self._recompute(key, snapshot_id=event.snapshot_id)

    def accept_batch_sequence(self, event: BookLevelBatchEvent) -> bool:
        """Validate the message sequence once, not each inner level index."""

        condition_id = event.condition_id.lower()
        state = self.condition(condition_id)
        leg = self._leg_key(
            condition_id,
            event.book_epoch,
            event.source,
            event.asset_id,
            event.outcome,
        )
        sequence: int | None = event.source_sequence
        if sequence == 0 and event.source.strip().lower() not in self.sequence_step_by_source:
            sequence = None
        return self._accept_sequence(
            state=state,
            leg=leg,
            source=event.source,
            event_id=event.event_id,
            sequence=sequence,
        )

    def apply_frame_batch_atomic(
        self,
        event: BookFrameBatchEvent,
    ) -> tuple[tuple[BookDeltaEvent, Decimal], ...] | None:
        """Stage a complete raw frame and publish no partial leg state."""

        state = self.condition(event.condition_id)
        if state.sync_state != BookSyncState.SYNCED:
            self.mark_gap(
                event.condition_id,
                f"raw_frame_without_synced_snapshot:{event.event_id}",
            )
            return None
        if state.book_epoch != event.book_epoch:
            self.mark_gap(
                event.condition_id,
                f"raw_frame_epoch_mismatch:{event.event_id}",
            )
            return None
        staged = self._copy_for_atomic_frame()
        prepared = staged._prepare_frame_top_fences(event)
        for batch in prepared.batches:
            if not staged.accept_batch_sequence(batch):
                self.mark_gap(
                    event.condition_id,
                    f"raw_frame_sequence_rejected:{event.event_id}",
                )
                return None
        touched_keys: set[EconomicLevelKey] = set()
        before_displayed_by_key: dict[EconomicLevelKey, Decimal] = {}
        staged_updates: list[tuple[BookDeltaEvent, EconomicLevelKey]] = []
        for batch in prepared.batches:
            for update in batch.updates:
                key = self._delta_level_key(update)
                touched_keys.add(key)
                before = staged.levels.get(key)
                before_displayed_by_key.setdefault(
                    key,
                    before.displayed_size if before else Decimal(0),
                )
                staged._apply_delta_observation(update, key=key)
                staged_updates.append((update, key))
        # Inner leg changes belong to one raw frame.  Recompute each economic
        # level once from the complete frame so transient first-leg states
        # affect neither residual capacity nor mirror-mismatch coverage.
        condition = staged.condition(event.condition_id)
        for key in sorted(
            touched_keys,
            key=lambda item: (
                item.condition_id,
                item.book_epoch,
                item.side.value,
                item.canonical_yes_price,
            ),
        ):
            staged._recompute(key, snapshot_id=condition.last_snapshot_id)
        decrease_by_key = {
            key: qty(
                max(
                    Decimal(0),
                    before_displayed_by_key[key]
                    - staged.levels[key].displayed_size,
                )
            )
            for key in touched_keys
        }
        last_update_index_by_key = {
            key: index for index, (_, key) in enumerate(staged_updates)
        }
        applied = [
            (
                update,
                (
                    decrease_by_key[key]
                    if last_update_index_by_key[key] == index
                    else Decimal(0)
                ),
            )
            for index, (update, key) in enumerate(staged_updates)
        ]
        new_mismatches = staged.mirror_mismatches
        staged.mirror_mismatches = self.mirror_mismatches
        staged.mirror_mismatches.extend(new_mismatches)
        self.__dict__.clear()
        self.__dict__.update(staged.__dict__)
        return tuple(applied)

    def _copy_for_atomic_frame(self) -> EconomicResidualBook:
        """Copy mutable book state without repeatedly cloning audit history."""

        staged = copy(self)
        staged.conditions = deepcopy(self.conditions)
        staged.levels = deepcopy(self.levels)
        staged._observations = deepcopy(self._observations)
        staged._leg_levels = {
            leg: set(levels) for leg, levels in self._leg_levels.items()
        }
        staged._allocation_ids = set(self._allocation_ids)
        staged.mirror_mismatches = []
        return staged

    def _prepare_frame_top_fences(
        self,
        event: BookFrameBatchEvent,
    ) -> BookFrameBatchEvent:
        batches: list[BookLevelBatchEvent] = []
        for batch in event.batches:
            bids, asks = self._raw_leg_levels(batch)
            for update in batch.updates:
                target = (
                    bids
                    if update.side == EconomicBookSide.BID
                    else asks
                )
                if update.new_size <= 0:
                    target.pop(update.price, None)
                else:
                    target[update.price] = update.new_size
            raw_best_bid = batch.authoritative_best_bid
            raw_best_ask = batch.authoritative_best_ask
            if raw_best_bid is None or raw_best_ask is None:
                raise ValueError(
                    f"raw frame leg lacks authoritative top: {batch.event_id}"
                )
            deleted_bids = sorted(
                (level for level in bids if level > raw_best_bid),
                reverse=True,
            )
            deleted_asks = sorted(level for level in asks if level < raw_best_ask)
            for level in deleted_bids:
                bids.pop(level, None)
            for level in deleted_asks:
                asks.pop(level, None)
            actual_bid = max(bids, default=None)
            actual_ask = min(asks, default=None)
            if actual_bid != raw_best_bid or actual_ask != raw_best_ask:
                raise ValueError(
                    "raw frame cannot reach authoritative top without inventing "
                    f"liquidity: event={event.event_id}, asset={batch.asset_id}, "
                    f"expected={raw_best_bid}/{raw_best_ask}, "
                    f"actual={actual_bid}/{actual_ask}"
                )
            updates = list(batch.updates)
            next_sequence = max(
                (int(update.sequence or 0) for update in updates),
                default=0,
            )
            for side, levels in (
                (EconomicBookSide.BID, deleted_bids),
                (EconomicBookSide.ASK, deleted_asks),
            ):
                for level in levels:
                    next_sequence += 1
                    updates.append(
                        BookDeltaEvent(
                            event_id="xue-top-fence-"
                            + canonical_hash(
                                {
                                    "raw_frame_event_id": event.event_id,
                                    "asset_id": batch.asset_id,
                                    "side": side,
                                    "price": level,
                                    "raw_best_bid": raw_best_bid,
                                    "raw_best_ask": raw_best_ask,
                                }
                            ),
                            condition_id=batch.condition_id,
                            market_id=batch.market_id,
                            asset_id=batch.asset_id,
                            outcome=batch.outcome,
                            exchange_ts=batch.exchange_ts,
                            local_ts=batch.local_ts,
                            book_epoch=batch.book_epoch,
                            side=side,
                            price=level,
                            new_size=Decimal(0),
                            source=batch.source,
                            sequence=next_sequence,
                            linked_trade_event_ids=(),
                            source_received_ts=batch.source_received_ts,
                        )
                    )
            batches.append(replace(batch, updates=tuple(updates)))
        return replace(event, batches=tuple(batches))

    def _raw_leg_levels(
        self,
        batch: BookLevelBatchEvent,
    ) -> tuple[dict[Decimal, Decimal], dict[Decimal, Decimal]]:
        condition_id = batch.condition_id.lower()
        leg = self._leg_key(
            condition_id,
            batch.book_epoch,
            batch.source,
            batch.asset_id,
            batch.outcome,
        )
        observation_key = self._observation_key(
            batch.source,
            batch.asset_id,
            batch.outcome,
        )
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        for key in self._leg_levels.get(leg, set()):
            observation = self._observations.get(key, {}).get(observation_key)
            if observation is None or observation.size <= 0:
                continue
            raw_side = canonical_book_side(batch.outcome, key.side)
            raw_price = token_price(batch.outcome, key.canonical_yes_price)
            target = bids if raw_side == EconomicBookSide.BID else asks
            target[raw_price] = observation.size
        return bids, asks

    def apply_delta(
        self,
        event: BookDeltaEvent,
        *,
        validate_sequence: bool = True,
    ) -> Decimal:
        condition_id = event.condition_id.lower()
        state = self.condition(condition_id)
        if state.sync_state != BookSyncState.SYNCED:
            self.mark_gap(condition_id, f"delta_without_synced_snapshot:{event.event_id}")
            return Decimal(0)
        if event.book_epoch != state.book_epoch:
            self.mark_gap(condition_id, f"delta_epoch_mismatch:{event.event_id}")
            return Decimal(0)
        leg = self._leg_key(
            condition_id, event.book_epoch, event.source, event.asset_id, event.outcome
        )
        if validate_sequence and not self._accept_sequence(
            state=state,
            leg=leg,
            source=event.source,
            event_id=event.event_id,
            sequence=event.sequence,
        ):
            return Decimal(0)
        key = self._delta_level_key(event)
        before = self.levels.get(key)
        before_displayed = before.displayed_size if before else Decimal(0)
        self._apply_delta_observation(event, key=key)
        self._recompute(key, snapshot_id=state.last_snapshot_id)
        after = self.levels.get(key)
        after_displayed = after.displayed_size if after else Decimal(0)
        return qty(max(Decimal(0), before_displayed - after_displayed))

    def _apply_delta_observation(
        self,
        event: BookDeltaEvent,
        *,
        key: EconomicLevelKey,
    ) -> None:
        condition_id = event.condition_id.lower()
        state = self.condition(condition_id)
        leg = self._leg_key(
            condition_id,
            event.book_epoch,
            event.source,
            event.asset_id,
            event.outcome,
        )
        observation_key = self._observation_key(
            event.source, event.asset_id, event.outcome
        )
        if event.new_size <= 0:
            self._observations.get(key, {}).pop(observation_key, None)
            self._leg_levels.setdefault(leg, set()).discard(key)
        else:
            self._observations.setdefault(key, {})[observation_key] = LevelObservation(
                source=event.source,
                asset_id=event.asset_id,
                outcome=event.outcome,
                event_id=event.event_id,
                observed_at=event.exchange_ts,
                size=event.new_size,
            )
            self._leg_levels.setdefault(leg, set()).add(key)
        state.last_exchange_ts = event.exchange_ts
        state.last_source_received_ts = event.source_received_ts or event.exchange_ts
        state.last_local_ts = event.local_ts
        state.last_freshness_event_id = event.event_id
        state.source_ids.add(event.source)

    @staticmethod
    def _delta_level_key(event: BookDeltaEvent) -> EconomicLevelKey:
        return EconomicLevelKey(
            condition_id=event.condition_id.lower(),
            book_epoch=event.book_epoch,
            side=canonical_book_side(event.outcome, event.side),
            canonical_yes_price=canonical_yes_price(event.outcome, event.price),
        )

    def _accept_sequence(
        self,
        *,
        state: ConditionState,
        leg: str,
        source: str,
        event_id: str,
        sequence: int | None,
    ) -> bool:
        if sequence is None:
            return True
        last = state.last_sequence_by_leg.get(leg)
        if last is not None and sequence <= last:
            self.mark_gap(
                state.condition_id,
                f"non_monotonic_sequence:{event_id}",
            )
            return False
        expected_step = self.sequence_step_by_source.get(source.strip().lower())
        if (
            last is not None
            and expected_step is not None
            and sequence != last + expected_step
        ):
            self.mark_gap(
                state.condition_id,
                (
                    f"missing_sequence:{event_id}:"
                    f"expected={last + expected_step}:actual={sequence}"
                ),
            )
            return False
        state.last_sequence_by_leg[leg] = sequence
        return True

    def apply_external_trade(
        self,
        event: TradeEvent,
        *,
        size: Decimal | None = None,
    ) -> Decimal:
        """Remove trade-consumed liquidity not already represented by a delta."""

        action = canonical_action(event.outcome, event.aggressor_side)
        key = EconomicLevelKey(
            condition_id=event.condition_id.lower(),
            book_epoch=event.book_epoch,
            side=consumed_side(action),
            canonical_yes_price=canonical_yes_price(event.outcome, event.price),
        )
        current = self.levels.get(key)
        amount = qty(event.size if size is None else size)
        if current is None or amount <= 0:
            return Decimal(0)
        removed = qty(min(amount, current.displayed_size))
        if removed <= 0:
            return Decimal(0)
        observations = self._observations.get(key, {})
        for observation_key, observation in tuple(observations.items()):
            next_size = qty(max(Decimal(0), observation.size - removed))
            if next_size <= 0:
                observations.pop(observation_key, None)
            else:
                observations[observation_key] = replace(
                    observation,
                    event_id=event.event_id,
                    observed_at=event.exchange_ts,
                    size=next_size,
                )
        snapshot_id = current.snapshot_id
        self._recompute(key, snapshot_id=snapshot_id)
        refreshed = self.levels[key]
        refreshed.external_trade_removed_size = qty(
            refreshed.external_trade_removed_size + removed
        )
        return removed

    def classify_external_cancel(
        self,
        key: EconomicLevelKey,
        size: Decimal,
    ) -> None:
        level = self.levels.get(key)
        amount = qty(size)
        if level is None or amount <= 0:
            return
        level.external_cancel_removed_size = qty(
            level.external_cancel_removed_size + amount
        )

    def mark_gap(self, condition_id: str, event_id: str) -> None:
        state = self.condition(condition_id)
        state.sync_state = BookSyncState.GAP
        state.gap_event_ids.append(str(event_id))

    def mark_stale(self, condition_id: str) -> None:
        state = self.condition(condition_id)
        if state.sync_state == BookSyncState.SYNCED:
            state.sync_state = BookSyncState.STALE

    def mark_handover_pending(
        self,
        condition_id: str,
        event_id: str,
        *,
        at: datetime | None = None,
    ) -> None:
        state = self.condition(condition_id)
        state.sync_state = BookSyncState.HANDOVER_PENDING
        state.source_handover_verified = False
        state.handover_pending_since = at
        state.gap_event_ids.append(str(event_id))

    def verify_handover(
        self,
        condition_id: str,
        *,
        at: datetime | None = None,
    ) -> bool:
        state = self.condition(condition_id)
        if (
            state.handover_pending_since is not None
            and (
                state.last_exchange_ts is None
                or state.last_exchange_ts < state.handover_pending_since
            )
        ):
            state.sync_state = BookSyncState.WAITING_FOR_SNAPSHOT
            state.source_handover_verified = False
            return False
        state.source_handover_verified = True
        state.handover_pending_since = None
        if state.last_snapshot_id:
            state.sync_state = BookSyncState.SYNCED
        return True

    def set_trading_mode(
        self,
        condition_id: str,
        mode: TradingMode,
        *,
        requires_fresh_snapshot: bool = False,
    ) -> None:
        state = self.condition(condition_id)
        state.trading_mode = mode
        state.market_live = mode != TradingMode.CLOSED
        if mode == TradingMode.CLOSED:
            state.sync_state = BookSyncState.CLOSED
        elif requires_fresh_snapshot:
            state.sync_state = BookSyncState.WAITING_FOR_SNAPSHOT

    def quality_reason(
        self,
        condition_id: str,
        at: datetime,
        *,
        clock_basis: str = "source_received",
    ) -> str | None:
        state = self.condition(condition_id)
        if state.trading_mode == TradingMode.CLOSED:
            return "market_closed"
        if state.trading_mode == TradingMode.PAUSED:
            return "market_paused"
        if state.trading_mode == TradingMode.CANCEL_ONLY:
            return "market_cancel_only"
        if not state.market_live:
            return "market_not_live"
        if state.sync_state != BookSyncState.SYNCED:
            return f"book_{state.sync_state.value.lower()}"
        if not state.source_handover_verified:
            return "source_handover_unverified"
        if clock_basis == "local":
            freshness_ts = state.last_local_ts
        elif clock_basis == "source_received":
            freshness_ts = state.last_source_received_ts
        elif clock_basis == "exchange":
            freshness_ts = state.last_exchange_ts
        else:
            raise ValueError(f"unsupported freshness clock basis: {clock_basis}")
        if freshness_ts is None:
            return "book_timestamp_missing"
        coverage = self.transport_coverage_at(condition_id, at)
        if coverage is not None:
            if not coverage.allowed:
                reason = coverage.reason.strip().lower().replace(" ", "_")
                return f"transport_coverage_{reason or 'not_ready'}"
        if clock_basis == "local":
            snapshot_ts = state.last_snapshot_local_ts
        elif clock_basis == "source_received":
            snapshot_ts = state.last_snapshot_source_received_ts
        else:
            snapshot_ts = state.last_snapshot_exchange_ts
        if snapshot_ts is None:
            return "book_snapshot_timestamp_missing"
        if self.blocking_transport_window_since(
            condition_id,
            since=snapshot_ts,
            at=at,
        ) is not None:
            return "book_waiting_for_snapshot_after_transport_gap"
        if self.profile.book_validity_mode == BookValidityMode.EVENT_DRIVEN:
            return None
        age_ms = max(0, int((at - freshness_ts).total_seconds() * 1000))
        if age_ms > self.profile.max_book_age_ms:
            return "book_stale"
        return None

    def best_level(
        self, condition_id: str, side: EconomicBookSide
    ) -> EconomicLevelState | None:
        state = self.condition(condition_id)
        candidates = [
            level
            for key, level in self.levels.items()
            if key.condition_id == state.condition_id
            and key.book_epoch == state.book_epoch
            and key.side == side
            and level.available_size > 0
            and level.mirror_valid
        ]
        if not candidates:
            return None
        return (
            min(candidates, key=lambda item: item.key.canonical_yes_price)
            if side == EconomicBookSide.ASK
            else max(candidates, key=lambda item: item.key.canonical_yes_price)
        )

    def would_cross(self, order: Pml2OrderIntent) -> bool:
        action = canonical_action(order.outcome, order.side)
        level = self.best_level(order.condition_id, consumed_side(action))
        if level is None:
            return False
        canonical_limit = canonical_yes_price(order.outcome, order.limit_price)
        return self._limit_allows(
            action, level.key.canonical_yes_price, canonical_limit
        )

    def preview_taker(self, order: Pml2OrderIntent) -> tuple[TakerLevelPreview, ...]:
        availability = {
            key: level.available_size for key, level in self.levels.items()
        }
        return self._preview_taker_with_availability(order, availability)

    def preview_taker_group(
        self, orders: Iterable[Pml2OrderIntent]
    ) -> dict[str, tuple[TakerLevelPreview, ...]]:
        """Reserve a multi-leg group in memory without mutating the book."""

        availability = {
            key: level.available_size for key, level in self.levels.items()
        }
        result: dict[str, tuple[TakerLevelPreview, ...]] = {}
        for order in orders:
            rows = self._preview_taker_with_availability(order, availability)
            result[order.order_id] = rows
            for row in rows:
                availability[row.key] = row.residual_after
        return result

    def visible_depth_for_taker(self, order: Pml2OrderIntent) -> Decimal:
        """Return raw visible economic depth reachable at the order limit."""

        action = canonical_action(order.outcome, order.side)
        side = consumed_side(action)
        canonical_limit = canonical_yes_price(order.outcome, order.limit_price)
        condition = self.condition(order.condition_id)
        return qty(
            sum(
                (
                    level.displayed_size
                    for key, level in self.levels.items()
                    if key.condition_id == condition.condition_id
                    and key.book_epoch == condition.book_epoch
                    and key.side == side
                    and level.mirror_valid
                    and self._limit_allows(
                        action, key.canonical_yes_price, canonical_limit
                    )
                ),
                Decimal(0),
            )
        )

    def visible_amount_for_taker(self, order: Pml2OrderIntent) -> Decimal:
        """Return reachable residual liquidity in the order's amount unit."""

        action = canonical_action(order.outcome, order.side)
        side = consumed_side(action)
        canonical_limit = canonical_yes_price(order.outcome, order.limit_price)
        condition = self.condition(order.condition_id)
        total = Decimal(0)
        for key, level in self.levels.items():
            if (
                key.condition_id != condition.condition_id
                or key.book_epoch != condition.book_epoch
                or key.side != side
                or not level.mirror_valid
                or not self._limit_allows(
                    action, key.canonical_yes_price, canonical_limit
                )
            ):
                continue
            if order.amount_unit == OrderAmountUnit.QUOTE:
                total += level.displayed_size * token_price(
                    order.outcome, key.canonical_yes_price
                )
            else:
                total += level.displayed_size
        return qty(total)

    def commit_taker(
        self,
        previews: Iterable[TakerLevelPreview],
        *,
        allocation_id: str,
    ) -> None:
        if allocation_id in self._allocation_ids:
            return
        rows = tuple(previews)
        for row in rows:
            state = self.levels.get(row.key)
            if (
                state is None
                or state.version != row.level_version
                or state.available_size < row.qty
            ):
                raise RuntimeError("economic residual changed before atomic commit")
        for row in rows:
            state = self.levels[row.key]
            state.available_size = qty(state.available_size - row.qty)
            state.consumed_size = qty(state.consumed_size + row.qty)
            state.version += 1
        self._allocation_ids.add(allocation_id)

    def commit_taker_group(
        self,
        previews_by_order: dict[str, tuple[TakerLevelPreview, ...]],
        *,
        allocation_id: str,
    ) -> None:
        """Atomically consume all levels reserved by an ALL_LEGS_FOK group."""

        if allocation_id in self._allocation_ids:
            return
        totals: dict[EconomicLevelKey, Decimal] = {}
        expected_versions: dict[EconomicLevelKey, int] = {}
        for rows in previews_by_order.values():
            for row in rows:
                totals[row.key] = qty(totals.get(row.key, Decimal(0)) + row.qty)
                previous = expected_versions.setdefault(row.key, row.level_version)
                if previous != row.level_version:
                    raise RuntimeError("inconsistent group preview level versions")
        for key, requested in totals.items():
            state = self.levels.get(key)
            if (
                state is None
                or state.version != expected_versions[key]
                or state.available_size < requested
            ):
                raise RuntimeError("economic residual changed before group commit")
        for key, requested in totals.items():
            state = self.levels[key]
            state.available_size = qty(state.available_size - requested)
            state.consumed_size = qty(state.consumed_size + requested)
            state.version += 1
        self._allocation_ids.add(allocation_id)

    def level_size(self, key: EconomicLevelKey) -> Decimal:
        state = self.levels.get(key)
        return state.displayed_size if state else Decimal(0)

    def _preview_taker_with_availability(
        self,
        order: Pml2OrderIntent,
        availability: dict[EconomicLevelKey, Decimal],
    ) -> tuple[TakerLevelPreview, ...]:
        action = canonical_action(order.outcome, order.side)
        side = consumed_side(action)
        canonical_limit = canonical_yes_price(order.outcome, order.limit_price)
        state = self.condition(order.condition_id)
        candidates = [
            level
            for key, level in self.levels.items()
            if key.condition_id == state.condition_id
            and key.book_epoch == state.book_epoch
            and key.side == side
            and availability.get(key, Decimal(0)) > 0
            and level.mirror_valid
        ]
        candidates.sort(
            key=lambda item: item.key.canonical_yes_price,
            reverse=action == EconomicAction.SELL,
        )
        remaining = order.effective_requested_amount
        result: list[TakerLevelPreview] = []
        for level in candidates:
            canonical_price = level.key.canonical_yes_price
            if not self._limit_allows(action, canonical_price, canonical_limit):
                break
            before = availability.get(level.key, Decimal(0))
            raw = token_price(order.outcome, canonical_price)
            take = (
                qty_down(min(before, remaining / raw))
                if order.amount_unit == OrderAmountUnit.QUOTE
                else qty(min(remaining, before))
            )
            if take <= 0:
                continue
            if order.side == RawOrderSide.BUY and raw > order.limit_price:
                raise AssertionError("BUY child fill violates limit")
            if order.side == RawOrderSide.SELL and raw < order.limit_price:
                raise AssertionError("SELL child fill violates limit")
            result.append(
                TakerLevelPreview(
                    key=level.key,
                    qty=take,
                    canonical_yes_price=canonical_price,
                    raw_price=raw,
                    residual_before=before,
                    residual_after=qty(before - take),
                    level_version=level.version,
                    snapshot_id=level.snapshot_id,
                    source_event_ids=level.source_event_ids,
                )
            )
            consumed_amount = (
                take * raw
                if order.amount_unit == OrderAmountUnit.QUOTE
                else take
            )
            remaining = qty(max(Decimal(0), remaining - consumed_amount))
            availability[level.key] = qty(before - take)
            if remaining <= 0:
                break
        return tuple(result)

    def residual_snapshot(self) -> list[dict[str, object]]:
        return [
            {
                "condition_id": key.condition_id,
                "book_epoch": key.book_epoch,
                "side": key.side.value,
                "canonical_yes_price": format(key.canonical_yes_price, "f"),
                "displayed_size": format(level.displayed_size, "f"),
                "available_size": format(level.available_size, "f"),
                "consumed_size": format(level.consumed_size, "f"),
                "replenished_size": format(level.replenished_size, "f"),
                "external_removed_size": format(level.external_removed_size, "f"),
                "external_trade_removed_size": format(
                    level.external_trade_removed_size, "f"
                ),
                "external_cancel_removed_size": format(
                    level.external_cancel_removed_size, "f"
                ),
                "external_unknown_removed_size": format(
                    level.external_unknown_removed_size, "f"
                ),
                "mirror_valid": level.mirror_valid,
                "version": level.version,
            }
            for key, level in sorted(self.levels.items())
        ]

    @property
    def state_hash(self) -> str:
        return canonical_hash(self.residual_snapshot())

    def _reset_condition_epoch(self, condition_id: str, epoch: int) -> None:
        for key in [item for item in self.levels if item.condition_id == condition_id]:
            self.levels.pop(key, None)
            self._observations.pop(key, None)
        for leg in [item for item in self._leg_levels if item.startswith(f"{condition_id}|")]:
            self._leg_levels.pop(leg, None)
        state = self.condition(condition_id)
        state.book_epoch = int(epoch)
        state.sync_state = BookSyncState.SYNCING
        state.last_sequence_by_leg.clear()

    def _recompute(self, key: EconomicLevelKey, *, snapshot_id: str) -> None:
        observations = tuple(self._observations.get(key, {}).values())
        fused, mirror_valid, size_skew, time_skew, source_event_ids = self._fuse(
            key, observations
        )
        current = self.levels.get(key)
        if current is None:
            current = EconomicLevelState(key=key)
            self.levels[key] = current
        old = current.displayed_size
        cap = qty(fused * self.profile.depth_haircut)
        if current.version == 0 and old == 0:
            current.available_size = cap
        elif fused > old:
            addition = qty(
                (fused - old)
                * self.profile.depth_haircut
                * self.profile.positive_replenishment_fraction
            )
            current.available_size = min(cap, qty(current.available_size + addition))
            current.replenished_size = qty(current.replenished_size + addition)
        else:
            removed = qty(max(Decimal(0), old - fused))
            current.external_removed_size = qty(
                current.external_removed_size + removed
            )
            current.available_size = min(current.available_size, cap)
        current.displayed_size = qty(fused)
        current.snapshot_id = snapshot_id
        current.source_event_ids = source_event_ids
        current.mirror_valid = mirror_valid
        current.mirror_size_skew = size_skew
        current.mirror_time_skew_ms = time_skew
        current.version += 1
        if not observations and current.available_size <= 0:
            self._observations.pop(key, None)

    def _fuse(
        self,
        key: EconomicLevelKey,
        observations: tuple[LevelObservation, ...],
    ) -> tuple[Decimal, bool, Decimal, int, tuple[str, ...]]:
        if not observations:
            return Decimal(0), True, Decimal(0), 0, ()
        (
            source_sizes,
            mirror_valid,
            max_size_skew,
            max_time_skew,
            mismatch_rows,
        ) = self._mirror_metrics(key, observations)
        self.mirror_mismatches.extend(mismatch_rows)
        values = list(source_sizes.values())
        if self.profile.name == "strict":
            fused = min(values) if mirror_valid else Decimal(0)
        elif self.profile.name == "optimistic":
            fused = max(values)
        else:
            fused = next(
                (
                    source_sizes[source]
                    for source in self.profile.source_priority
                    if source in source_sizes
                ),
                values[0],
            )
        return (
            qty(fused),
            mirror_valid or self.profile.name != "strict",
            qty(max_size_skew),
            max_time_skew,
            tuple(sorted({row.event_id for row in observations})),
        )

    def _mirror_metrics(
        self,
        key: EconomicLevelKey,
        observations: tuple[LevelObservation, ...],
    ) -> tuple[
        dict[str, Decimal],
        bool,
        Decimal,
        int,
        tuple[dict[str, object], ...],
    ]:
        by_source: dict[str, list[LevelObservation]] = {}
        for observation in observations:
            by_source.setdefault(observation.source, []).append(observation)
        source_sizes: dict[str, Decimal] = {}
        mirror_valid = True
        max_size_skew = Decimal(0)
        max_time_skew = 0
        mismatch_rows: list[dict[str, object]] = []
        for source, rows in by_source.items():
            sizes = [row.size for row in rows]
            source_sizes[source] = min(sizes)
            if len(rows) < 2:
                continue
            largest = max(sizes)
            smallest = min(sizes)
            size_skew = (
                Decimal(0) if largest <= 0 else (largest - smallest) / largest
            )
            time_skew = int(
                (max(row.observed_at for row in rows) - min(row.observed_at for row in rows)).total_seconds()
                * 1000
            )
            max_size_skew = max(max_size_skew, size_skew)
            max_time_skew = max(max_time_skew, time_skew)
            valid = (
                size_skew <= self.profile.mirror_size_tolerance
                and time_skew <= self.profile.mirror_time_tolerance_ms
            )
            mirror_valid = mirror_valid and valid
            if not valid:
                mismatch_rows.append(
                    {
                        "condition_id": key.condition_id,
                        "book_epoch": key.book_epoch,
                        "side": key.side.value,
                        "canonical_yes_price": format(
                            key.canonical_yes_price, "f"
                        ),
                        "source": source,
                        "size_skew": format(size_skew, "f"),
                        "time_skew_ms": time_skew,
                    }
                )
        return (
            source_sizes,
            mirror_valid,
            max_size_skew,
            max_time_skew,
            tuple(mismatch_rows),
        )

    @staticmethod
    def _limit_allows(
        action: EconomicAction, book_price: Decimal, limit_price: Decimal
    ) -> bool:
        return (
            book_price <= limit_price
            if action == EconomicAction.BUY
            else book_price >= limit_price
        )

    @staticmethod
    def _observation_key(source: str, asset_id: str, outcome: Outcome) -> str:
        return f"{source}|{asset_id}|{outcome.value}"

    @staticmethod
    def _leg_key(
        condition_id: str,
        epoch: int,
        source: str,
        asset_id: str,
        outcome: Outcome,
    ) -> str:
        return f"{condition_id}|{epoch}|{source}|{asset_id}|{outcome.value}"


class TradeDeltaReconciler:
    """Prevents one economic removal from advancing maker queue twice."""

    def __init__(self, *, match_window_ms: int = 1000) -> None:
        self.match_window_ms = max(0, int(match_window_ms))
        self._credits: dict[
            str, tuple[EconomicLevelKey, Decimal, str, datetime]
        ] = {}
        self._used: dict[str, Decimal] = {}
        self._pending: dict[str, PendingDecrease] = {}

    def record_trade(self, event: TradeEvent) -> ReconciledTrade:
        action = canonical_action(event.outcome, event.aggressor_side)
        key = EconomicLevelKey(
            condition_id=event.condition_id.lower(),
            book_epoch=event.book_epoch,
            side=consumed_side(action),
            canonical_yes_price=canonical_yes_price(event.outcome, event.price),
        )
        remaining = event.size
        previously_removed = Decimal(0)
        for pending_id, pending in tuple(self._pending.items()):
            if remaining <= 0:
                break
            if pending.key != key or not self._pending_matches_trade(pending, event):
                continue
            take = qty(min(remaining, pending.remaining_size))
            pending.remaining_size = qty(pending.remaining_size - take)
            remaining = qty(remaining - take)
            previously_removed = qty(previously_removed + take)
            if pending.remaining_size <= 0:
                self._pending.pop(pending_id, None)
        self._credits[event.event_id] = (
            key,
            event.size,
            event.event_group_id,
            event.exchange_ts,
        )
        self._used[event.event_id] = previously_removed
        return ReconciledTrade(
            key=key,
            raw_trade_size=event.size,
            previously_delta_removed_size=previously_removed,
            unmatched_trade_size=remaining,
        )

    def reconcile(
        self,
        *,
        key: EconomicLevelKey,
        decrease: Decimal,
        event_id: str = "",
        event_ts: datetime | None = None,
        linked_trade_event_ids: Iterable[str] = (),
        event_group_id: str = "",
    ) -> ReconciledDecrease:
        remaining = qty(decrease)
        matched = Decimal(0)
        used_ids: list[str] = []
        explicit = {str(item) for item in linked_trade_event_ids}
        for trade_event_id, (
            trade_key,
            total,
            group_id,
            trade_ts,
        ) in self._credits.items():
            linked = trade_event_id in explicit or (
                bool(event_group_id) and event_group_id == group_id
            ) or (
                not explicit
                and trade_key == key
                and event_ts is not None
                and self._within_window(event_ts, trade_ts)
            )
            if not linked or trade_key != key or remaining <= 0:
                continue
            used = self._used.get(trade_event_id, Decimal(0))
            available = max(Decimal(0), total - used)
            take = min(remaining, available)
            if take <= 0:
                continue
            self._used[trade_event_id] = qty(used + take)
            matched = qty(matched + take)
            remaining = qty(remaining - take)
            used_ids.append(trade_event_id)
        pending_id = ""
        pending_size = Decimal(0)
        if remaining > 0 and event_ts is not None and self.match_window_ms > 0:
            pending_id = str(event_id or f"pending:{len(self._pending) + 1}")
            self._pending[pending_id] = PendingDecrease(
                pending_id=pending_id,
                key=key,
                event_ts=event_ts,
                remaining_size=remaining,
                linked_trade_event_ids=tuple(sorted(explicit)),
                event_group_id=event_group_id,
            )
            pending_size = remaining
            remaining = Decimal(0)
        return ReconciledDecrease(
            key=key,
            raw_decrease=qty(decrease),
            matched_trade_size=matched,
            unmatched_cancel_size=remaining,
            trade_event_ids=tuple(sorted(used_ids)),
            pending_decrease_id=pending_id,
            pending_trade_size=pending_size,
            event_ts=event_ts,
        )

    def expire_pending(self, pending_id: str) -> ReconciledDecrease | None:
        pending = self._pending.pop(str(pending_id), None)
        if pending is None or pending.remaining_size <= 0:
            return None
        return ReconciledDecrease(
            key=pending.key,
            raw_decrease=pending.remaining_size,
            matched_trade_size=Decimal(0),
            unmatched_cancel_size=pending.remaining_size,
            trade_event_ids=(),
            event_ts=pending.event_ts,
        )

    def _pending_matches_trade(
        self,
        pending: PendingDecrease,
        event: TradeEvent,
    ) -> bool:
        if pending.linked_trade_event_ids:
            return event.event_id in pending.linked_trade_event_ids
        if pending.event_group_id and event.event_group_id:
            return pending.event_group_id == event.event_group_id
        return self._within_window(pending.event_ts, event.exchange_ts)

    def _within_window(self, left: datetime, right: datetime) -> bool:
        delta_ms = abs((left - right).total_seconds() * 1000)
        return delta_ms <= self.match_window_ms
