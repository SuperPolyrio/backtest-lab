"""Token-level in-memory order book state.

The book is the source of truth; database rows are sampled projections of this
state. Updates are intentionally dict-based: price lookup is the hot path, while
sorting is delayed until top-N or metrics are requested.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

BookSide = Literal["bid", "ask"]
BookStatus = Literal["not_ready", "ready", "stale"]
EventClock = Literal["exchange", "receive"]


class OrderBookNotReady(RuntimeError):
    """Raised when a delta arrives before a trusted snapshot baseline."""


class OrderBookOutOfOrder(RuntimeError):
    """Raised when an update would move the local book backward in time."""


class OrderBookFrameInvalid(ValueError):
    """Raised when a complete source frame cannot be applied atomically."""


class OrderBookIdentityMismatch(ValueError):
    """Raised when an existing token is rebound to different market semantics."""


@dataclass(frozen=True)
class TokenBookIdentity:
    token_id: str
    market_id: int
    condition_id: str
    outcome: str
    outcome_index: int = 0
    market_slug: str | None = None


@dataclass(frozen=True)
class BookLevel:
    side: BookSide
    price: Decimal
    size: Decimal
    level_index: int

    def as_payload(self) -> dict[str, str]:
        return {"price": _decimal_text(self.price), "size": _decimal_text(self.size)}


@dataclass(frozen=True)
class BookMetrics:
    token_id: str
    market_id: int
    outcome: str
    status: BookStatus
    generation: int
    last_event_ts_ms: int | None
    best_bid: Decimal | None
    best_bid_size: Decimal | None
    best_ask: Decimal | None
    best_ask_size: Decimal | None
    mid: Decimal | None
    spread: Decimal | None
    bid_depth: Decimal
    ask_depth: Decimal
    depth_total: Decimal
    l1_imbalance: Decimal | None
    depth_imbalance: Decimal | None
    level_count_bid: int
    level_count_ask: int
    stale_reason: str | None = None
    state_ready: bool = False
    executable_ready: bool = False
    needs_resnapshot: bool = False
    open_gap_id: str | None = None
    last_exchange_ts_ms: int | None = None
    last_received_ts_ms: int | None = None
    last_transport_ts_ms: int | None = None
    connection_covered: bool = False
    connection_id: str | None = None
    connection_generation: int | None = None
    raw_frame_seq: int | None = None
    message_index: int | None = None
    group_id: str | None = None


@dataclass
class LocalOrderBook:
    identity: TokenBookIdentity
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    status: BookStatus = "not_ready"
    generation: int = 0
    last_event_ts_ms: int | None = None
    last_snapshot_ts_ms: int | None = None
    last_hash: str | None = None
    stale_reason: str | None = None
    last_exchange_ts_ms: int | None = None
    last_received_ts_ms: int | None = None
    last_transport_ts_ms: int | None = None
    last_snapshot_received_ts_ms: int | None = None
    last_source: str | None = None
    last_source_hash: str | None = None
    last_state_hash: str | None = None
    last_connection_id: str | None = None
    last_connection_generation: int | None = None
    last_raw_frame_seq: int | None = None
    last_message_index: int | None = None
    last_group_id: str | None = None
    connection_covered: bool = False
    needs_resnapshot: bool = False
    open_gap_id: str | None = None
    gap_started_received_ts_ms: int | None = None

    @property
    def ready(self) -> bool:
        return self.status == "ready" and not self.needs_resnapshot

    @property
    def state_ready(self) -> bool:
        """Whether the in-memory state has a trusted baseline and continuity."""

        return self.ready

    @property
    def executable_ready(self) -> bool:
        """Whether the trusted state is two-sided and strictly uncrossed."""

        bid = self.best_bid()
        ask = self.best_ask()
        return bool(
            self.ready
            and bid is not None
            and ask is not None
            and bid[0] < ask[0]
        )

    def reset(self, reason: str = "reset") -> None:
        self.bids.clear()
        self.asks.clear()
        self.status = "not_ready"
        self.generation += 1
        self.last_event_ts_ms = None
        self.last_snapshot_ts_ms = None
        self.stale_reason = reason
        self.last_hash = None
        self.last_exchange_ts_ms = None
        self.last_received_ts_ms = None
        self.last_transport_ts_ms = None
        self.last_snapshot_received_ts_ms = None
        self.last_source = None
        self.last_source_hash = None
        self.last_state_hash = None
        self.last_connection_id = None
        self.last_connection_generation = None
        self.last_raw_frame_seq = None
        self.last_message_index = None
        self.last_group_id = None
        self.connection_covered = False
        self.needs_resnapshot = True
        self.open_gap_id = None
        self.gap_started_received_ts_ms = None

    def mark_stale(self, reason: str = "stale", *, needs_resnapshot: bool = True) -> None:
        self.status = "stale"
        self.stale_reason = reason
        self.needs_resnapshot = bool(needs_resnapshot)

    def mark_gap(
        self,
        reason: str = "connection_gap",
        *,
        gap_id: str | None = None,
        received_ts_ms: int | None = None,
        connection_id: str | None = None,
        connection_generation: int | None = None,
    ) -> None:
        """Invalidate the state until a new full snapshot establishes a baseline."""

        self.mark_stale(reason, needs_resnapshot=True)
        self.open_gap_id = gap_id or self.open_gap_id or reason
        if received_ts_ms is not None:
            received = int(received_ts_ms)
            self.gap_started_received_ts_ms = received
            self.last_transport_ts_ms = max(
                received,
                int(self.last_transport_ts_ms or received),
            )
        if connection_id is not None:
            self.last_connection_id = str(connection_id)
        if connection_generation is not None:
            self.last_connection_generation = int(connection_generation)
        self.connection_covered = False

    def mark_connection_heartbeat(
        self,
        *,
        received_ts_ms: int,
        connection_id: str | None = None,
        connection_generation: int | None = None,
    ) -> bool:
        """Record transport continuity without pretending the book itself changed."""

        received = int(received_ts_ms)
        if (
            connection_generation is not None
            and self.last_connection_generation is not None
            and int(connection_generation) != self.last_connection_generation
        ):
            self.mark_gap(
                "connection_generation_changed",
                received_ts_ms=received,
                connection_id=connection_id,
                connection_generation=connection_generation,
            )
            return False
        if (
            connection_id is not None
            and self.last_connection_id is not None
            and str(connection_id) != self.last_connection_id
        ):
            self.mark_gap(
                "connection_id_changed",
                received_ts_ms=received,
                connection_id=connection_id,
                connection_generation=connection_generation,
            )
            return False
        if self.last_transport_ts_ms is not None and received < self.last_transport_ts_ms:
            return False
        self.last_transport_ts_ms = received
        if connection_id is not None:
            self.last_connection_id = str(connection_id)
        if connection_generation is not None:
            self.last_connection_generation = int(connection_generation)
        if not self.needs_resnapshot:
            self.connection_covered = True
        return True

    def mark_stale_if_idle(self, *, now_ms: int, stale_after_ms: int) -> bool:
        freshness_ts_ms = (
            self.last_transport_ts_ms
            if self.connection_covered and self.last_transport_ts_ms is not None
            else self.last_received_ts_ms
            if self.last_received_ts_ms is not None
            else self.last_event_ts_ms
        )
        if freshness_ts_ms is None:
            self.mark_stale("no_events")
            return True
        if int(now_ms) - int(freshness_ts_ms) >= int(stale_after_ms):
            self.mark_stale("idle_timeout")
            return True
        return False

    def apply_snapshot(
        self,
        *,
        bids: Iterable[tuple[Any, Any]],
        asks: Iterable[tuple[Any, Any]],
        event_ts_ms: int | None = None,
        source_hash: str | None = None,
        received_ts_ms: int | None = None,
        source: str | None = None,
        event_clock: EventClock = "exchange",
        connection_id: str | None = None,
        connection_generation: int | None = None,
        raw_frame_seq: int | None = None,
        message_index: int | None = None,
        group_id: str | None = None,
    ) -> BookMetrics:
        try:
            normalized_bids = _normalize_side(bids)
            normalized_asks = _normalize_side(asks)
            _validate_uncrossed(normalized_bids, normalized_asks)
            self._validate_event_order(
                is_snapshot=True,
                event_ts_ms=event_ts_ms,
                received_ts_ms=received_ts_ms,
                event_clock=event_clock,
                connection_id=connection_id,
                connection_generation=connection_generation,
                raw_frame_seq=raw_frame_seq,
                message_index=message_index,
            )
        except OrderBookOutOfOrder:
            self.mark_gap(
                "out_of_order_snapshot",
                received_ts_ms=received_ts_ms,
                connection_id=connection_id,
                connection_generation=connection_generation,
            )
            raise
        except (TypeError, ValueError) as exc:
            self.mark_gap(
                "invalid_snapshot_frame",
                received_ts_ms=received_ts_ms,
                connection_id=connection_id,
                connection_generation=connection_generation,
            )
            if isinstance(exc, OrderBookFrameInvalid):
                raise
            raise OrderBookFrameInvalid(
                f"invalid book snapshot for token {self.identity.token_id}: {exc}"
            ) from exc
        self.bids = normalized_bids
        self.asks = normalized_asks
        self.status = "ready"
        self.generation += 1
        self.stale_reason = None
        self.needs_resnapshot = False
        self.open_gap_id = None
        self.gap_started_received_ts_ms = None
        self._commit_event_provenance(
            event_ts_ms=event_ts_ms,
            received_ts_ms=received_ts_ms,
            source=source,
            source_hash=source_hash,
            event_clock=event_clock,
            connection_id=connection_id,
            connection_generation=connection_generation,
            raw_frame_seq=raw_frame_seq,
            message_index=message_index,
            group_id=group_id,
            is_snapshot=True,
        )
        return self.metrics()

    def apply_change(
        self,
        *,
        side: BookSide,
        price: Any,
        size: Any,
        event_ts_ms: int | None = None,
        received_ts_ms: int | None = None,
        source_hash: str | None = None,
        source: str | None = None,
        event_clock: EventClock = "exchange",
        connection_id: str | None = None,
        connection_generation: int | None = None,
        raw_frame_seq: int | None = None,
        message_index: int | None = None,
        group_id: str | None = None,
        best_bid: Any | None = None,
        best_ask: Any | None = None,
    ) -> BookMetrics:
        return self.apply_changes(
            ((side, price, size),),
            event_ts_ms=event_ts_ms,
            received_ts_ms=received_ts_ms,
            source_hash=source_hash,
            source=source,
            event_clock=event_clock,
            connection_id=connection_id,
            connection_generation=connection_generation,
            raw_frame_seq=raw_frame_seq,
            message_index=message_index,
            group_id=group_id,
            best_bid=best_bid,
            best_ask=best_ask,
        )

    def apply_changes(
        self,
        changes: Iterable[tuple[BookSide, Any, Any]],
        *,
        event_ts_ms: int | None = None,
        received_ts_ms: int | None = None,
        source_hash: str | None = None,
        source: str | None = None,
        event_clock: EventClock = "exchange",
        connection_id: str | None = None,
        connection_generation: int | None = None,
        raw_frame_seq: int | None = None,
        message_index: int | None = None,
        group_id: str | None = None,
        best_bid: Any | None = None,
        best_ask: Any | None = None,
    ) -> BookMetrics:
        """Atomically apply all changes for one token in one source frame."""

        if not self.ready:
            raise OrderBookNotReady(f"book for token {self.identity.token_id} is not ready")
        staged_bids = dict(self.bids)
        staged_asks = dict(self.asks)
        try:
            self._validate_event_order(
                is_snapshot=False,
                event_ts_ms=event_ts_ms,
                received_ts_ms=received_ts_ms,
                event_clock=event_clock,
                connection_id=connection_id,
                connection_generation=connection_generation,
                raw_frame_seq=raw_frame_seq,
                message_index=message_index,
            )
            for side, price, size in changes:
                if side not in {"bid", "ask"}:
                    raise OrderBookFrameInvalid(f"invalid order book side: {side!r}")
                target = staged_bids if side == "bid" else staged_asks
                parsed_price = _decimal(price)
                parsed_size = _decimal(size)
                if parsed_price <= 0 or parsed_price >= 1:
                    raise OrderBookFrameInvalid(f"invalid order book price: {price!r}")
                if parsed_size < 0:
                    raise OrderBookFrameInvalid(
                        f"invalid negative order book size: {size!r}"
                    )
                if parsed_size == 0:
                    target.pop(parsed_price, None)
                else:
                    target[parsed_price] = parsed_size
            _apply_top_fence(
                staged_bids,
                staged_asks,
                best_bid=best_bid,
                best_ask=best_ask,
            )
            _validate_uncrossed(staged_bids, staged_asks)
        except OrderBookNotReady:
            raise
        except OrderBookOutOfOrder:
            self.mark_gap(
                "out_of_order",
                received_ts_ms=received_ts_ms,
                connection_id=connection_id,
                connection_generation=connection_generation,
            )
            raise
        except (TypeError, ValueError) as exc:
            self.mark_gap(
                "invalid_delta_frame",
                received_ts_ms=received_ts_ms,
                connection_id=connection_id,
                connection_generation=connection_generation,
            )
            if isinstance(exc, OrderBookFrameInvalid):
                raise
            raise OrderBookFrameInvalid(
                f"invalid price_change frame for token {self.identity.token_id}: {exc}"
            ) from exc
        self.bids = staged_bids
        self.asks = staged_asks
        self._commit_event_provenance(
            event_ts_ms=event_ts_ms,
            received_ts_ms=received_ts_ms,
            source=source,
            source_hash=source_hash,
            event_clock=event_clock,
            connection_id=connection_id,
            connection_generation=connection_generation,
            raw_frame_seq=raw_frame_seq,
            message_index=message_index,
            group_id=group_id,
            is_snapshot=False,
        )
        return self.metrics()

    def _validate_event_order(
        self,
        *,
        is_snapshot: bool,
        event_ts_ms: int | None,
        received_ts_ms: int | None,
        event_clock: EventClock,
        connection_id: str | None,
        connection_generation: int | None,
        raw_frame_seq: int | None,
        message_index: int | None,
    ) -> None:
        if event_clock not in {"exchange", "receive"}:
            raise OrderBookFrameInvalid(f"invalid event clock: {event_clock!r}")
        event_ts = int(event_ts_ms) if event_ts_ms is not None else None
        received_ts = int(received_ts_ms) if received_ts_ms is not None else None
        if event_clock == "exchange" and event_ts is not None and received_ts is not None and event_ts > received_ts:
            raise OrderBookOutOfOrder(
                f"causally inverted order book clock for token {self.identity.token_id}: "
                f"exchange={event_ts} > received={received_ts}"
            )

        incoming_generation = (
            int(connection_generation) if connection_generation is not None else None
        )
        if (
            incoming_generation is not None
            and self.last_connection_generation is not None
        ):
            if incoming_generation < self.last_connection_generation:
                raise OrderBookOutOfOrder(
                    f"old connection generation for token {self.identity.token_id}: "
                    f"{incoming_generation} < {self.last_connection_generation}"
                )
            if incoming_generation > self.last_connection_generation and not is_snapshot:
                self.mark_gap(
                    "connection_generation_changed",
                    received_ts_ms=received_ts,
                    connection_id=connection_id,
                    connection_generation=incoming_generation,
                )
                raise OrderBookNotReady(
                    f"new connection generation for token {self.identity.token_id} requires snapshot"
                )
        if (
            connection_id is not None
            and self.last_connection_id is not None
            and str(connection_id) != self.last_connection_id
            and not is_snapshot
        ):
            self.mark_gap(
                "connection_id_changed",
                received_ts_ms=received_ts,
                connection_id=connection_id,
                connection_generation=incoming_generation,
            )
            raise OrderBookNotReady(
                f"new connection for token {self.identity.token_id} requires snapshot"
            )

        if (
            received_ts is not None
            and self.last_received_ts_ms is not None
            and received_ts < self.last_received_ts_ms
        ):
            raise OrderBookOutOfOrder(
                f"out-of-order received clock for token {self.identity.token_id}: "
                f"{received_ts} < {self.last_received_ts_ms}"
            )
        # A full snapshot is authoritative at its receive position.  Polymarket
        # may timestamp a snapshot with the book's last venue mutation, which
        # can legitimately be older than a delta observed before a resubscribe.
        # Keep that exchange clock as evidence, but never reject a newer raw
        # snapshot solely because the market itself was quiet.
        if (
            not is_snapshot
            and event_clock == "exchange"
            and event_ts is not None
            and self.last_exchange_ts_ms is not None
            and event_ts < self.last_exchange_ts_ms
        ):
            raise OrderBookOutOfOrder(
                f"out-of-order exchange clock for token {self.identity.token_id}: "
                f"{event_ts} < {self.last_exchange_ts_ms}"
            )
        same_connection = (
            (
                connection_id is None
                or self.last_connection_id is None
                or str(connection_id) == self.last_connection_id
            )
            and (
                incoming_generation is None
                or self.last_connection_generation is None
                or incoming_generation == self.last_connection_generation
            )
        )
        if (
            same_connection
            and raw_frame_seq is not None
            and self.last_raw_frame_seq is not None
        ):
            incoming_position = (int(raw_frame_seq), int(message_index or 0))
            previous_position = (
                self.last_raw_frame_seq,
                int(self.last_message_index or 0),
            )
            if incoming_position <= previous_position:
                raise OrderBookOutOfOrder(
                    f"non-monotonic raw frame for token {self.identity.token_id}: "
                    f"{incoming_position} <= {previous_position}"
                )

    def _commit_event_provenance(
        self,
        *,
        event_ts_ms: int | None,
        received_ts_ms: int | None,
        source: str | None,
        source_hash: str | None,
        event_clock: EventClock,
        connection_id: str | None,
        connection_generation: int | None,
        raw_frame_seq: int | None,
        message_index: int | None,
        group_id: str | None,
        is_snapshot: bool,
    ) -> None:
        event_ts = int(event_ts_ms) if event_ts_ms is not None else None
        received_ts = int(received_ts_ms) if received_ts_ms is not None else None
        if event_ts is not None:
            self.last_event_ts_ms = event_ts
        if event_clock == "exchange" and event_ts is not None:
            self.last_exchange_ts_ms = event_ts
        elif is_snapshot and event_clock == "receive":
            self.last_exchange_ts_ms = None
        if received_ts is not None:
            self.last_received_ts_ms = received_ts
            self.last_transport_ts_ms = received_ts
            self.connection_covered = True
        if is_snapshot:
            self.last_snapshot_ts_ms = event_ts
            self.last_snapshot_received_ts_ms = received_ts
        if source is not None:
            self.last_source = str(source)
        self.last_source_hash = source_hash
        if connection_id is not None:
            self.last_connection_id = str(connection_id)
        if connection_generation is not None:
            self.last_connection_generation = int(connection_generation)
        if raw_frame_seq is not None:
            self.last_raw_frame_seq = int(raw_frame_seq)
        if message_index is not None:
            self.last_message_index = int(message_index)
        if group_id is not None:
            self.last_group_id = str(group_id)
        self.last_state_hash = self.fingerprint()
        # ``last_hash`` remains as a compatibility alias, but now has one stable
        # meaning: the locally reconstructed state hash.  The opaque venue hash
        # is kept separately in ``last_source_hash``.
        self.last_hash = self.last_state_hash

    def best_bid(self) -> tuple[Decimal, Decimal] | None:
        if not self.bids:
            return None
        price = max(self.bids)
        return price, self.bids[price]

    def best_ask(self) -> tuple[Decimal, Decimal] | None:
        if not self.asks:
            return None
        price = min(self.asks)
        return price, self.asks[price]

    def top_n(self, n: int = 10) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
        limit = max(0, int(n))
        bid_levels = tuple(
            BookLevel("bid", price, size, idx)
            for idx, (price, size) in enumerate(sorted(self.bids.items(), reverse=True)[:limit])
        )
        ask_levels = tuple(
            BookLevel("ask", price, size, idx)
            for idx, (price, size) in enumerate(sorted(self.asks.items())[:limit])
        )
        return bid_levels, ask_levels

    def metrics(self, *, depth_levels: int = 5) -> BookMetrics:
        bid = self.best_bid()
        ask = self.best_ask()
        best_bid, best_bid_size = bid if bid else (None, None)
        best_ask, best_ask_size = ask if ask else (None, None)
        mid = (
            (best_bid + best_ask) / Decimal(2)
            if best_bid is not None and best_ask is not None
            else None
        )
        spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
        bid_top, ask_top = self.top_n(depth_levels)
        bid_depth = sum((level.price * level.size for level in bid_top), Decimal(0))
        ask_depth = sum((level.price * level.size for level in ask_top), Decimal(0))
        depth_total = bid_depth + ask_depth
        l1_total = Decimal(0)
        if best_bid is not None and best_bid_size is not None:
            l1_total += best_bid * best_bid_size
        if best_ask is not None and best_ask_size is not None:
            l1_total += best_ask * best_ask_size
        l1_bid_notional = (
            best_bid * best_bid_size
            if best_bid is not None and best_bid_size is not None
            else Decimal(0)
        )
        return BookMetrics(
            token_id=self.identity.token_id,
            market_id=self.identity.market_id,
            outcome=self.identity.outcome,
            status=self.status,
            generation=self.generation,
            last_event_ts_ms=self.last_event_ts_ms,
            best_bid=best_bid,
            best_bid_size=best_bid_size,
            best_ask=best_ask,
            best_ask_size=best_ask_size,
            mid=mid,
            spread=spread,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            depth_total=depth_total,
            l1_imbalance=(l1_bid_notional / l1_total) if l1_total > 0 else None,
            depth_imbalance=(bid_depth / depth_total) if depth_total > 0 else None,
            level_count_bid=len(self.bids),
            level_count_ask=len(self.asks),
            stale_reason=self.stale_reason,
            state_ready=self.state_ready,
            executable_ready=self.executable_ready,
            needs_resnapshot=self.needs_resnapshot,
            open_gap_id=self.open_gap_id,
            last_exchange_ts_ms=self.last_exchange_ts_ms,
            last_received_ts_ms=self.last_received_ts_ms,
            last_transport_ts_ms=self.last_transport_ts_ms,
            connection_covered=self.connection_covered,
            connection_id=self.last_connection_id,
            connection_generation=self.last_connection_generation,
            raw_frame_seq=self.last_raw_frame_seq,
            message_index=self.last_message_index,
            group_id=self.last_group_id,
        )

    def snapshot_payload(self, *, depth_levels: int = 10) -> dict[str, Any]:
        bid_levels, ask_levels = self.top_n(depth_levels)
        metrics = self.metrics(depth_levels=depth_levels)
        snapshot_version = self.fingerprint(depth_levels=depth_levels)
        return {
            "snapshot_id": 0,
            "token_id": self.identity.token_id,
            "market_id": self.identity.market_id,
            "condition_id": self.identity.condition_id,
            "outcome": self.identity.outcome,
            "side": self.identity.outcome,
            "status": self.status,
            "book_status": "ok" if self.ready else self.status,
            "source": "local_orderbook",
            "generation": self.generation,
            "state_ready": self.state_ready,
            "executable_ready": self.executable_ready,
            "needs_resnapshot": self.needs_resnapshot,
            "open_gap_id": self.open_gap_id,
            "last_event_ts_ms": self.last_event_ts_ms,
            "last_exchange_ts_ms": self.last_exchange_ts_ms,
            "last_received_ts_ms": self.last_received_ts_ms,
            "last_transport_ts_ms": self.last_transport_ts_ms,
            "connection_id": self.last_connection_id,
            "connection_generation": self.last_connection_generation,
            "raw_frame_seq": self.last_raw_frame_seq,
            "message_index": self.last_message_index,
            "group_id": self.last_group_id,
            "source_hash": self.last_source_hash,
            "state_hash": self.last_state_hash,
            "timestamp": _ms_to_iso(self.last_event_ts_ms),
            "snapshot_timestamp": _ms_to_iso(self.last_event_ts_ms),
            "snapshot_version": snapshot_version,
            "best_bid": _optional_decimal_text(metrics.best_bid),
            "best_ask": _optional_decimal_text(metrics.best_ask),
            "mid": _optional_decimal_text(metrics.mid),
            "spread": _optional_decimal_text(metrics.spread),
            "bid_depth": _decimal_text(metrics.bid_depth),
            "ask_depth": _decimal_text(metrics.ask_depth),
            "depth_total": _decimal_text(metrics.depth_total),
            "imbalance": _optional_decimal_text(metrics.depth_imbalance),
            "bids": [level.as_payload() for level in bid_levels],
            "asks": [level.as_payload() for level in ask_levels],
        }

    def fingerprint(self, *, depth_levels: int | None = None) -> str:
        import hashlib

        bid_items = sorted(self.bids.items(), reverse=True)
        ask_items = sorted(self.asks.items())
        if depth_levels is not None:
            limit = max(0, int(depth_levels))
            bid_items = bid_items[:limit]
            ask_items = ask_items[:limit]
        digest = hashlib.sha256()
        digest.update(self.identity.token_id.encode("utf-8"))
        digest.update(b"|")
        for side in (bid_items, ask_items):
            for price, size in side:
                digest.update(_decimal_text(price).encode("ascii"))
                digest.update(b":")
                digest.update(_decimal_text(size).encode("ascii"))
                digest.update(b";")
            digest.update(b"|")
        return digest.hexdigest()[:20]


def _normalize_side(rows: Iterable[tuple[Any, Any]]) -> dict[Decimal, Decimal]:
    normalized: dict[Decimal, Decimal] = {}
    for raw_price, raw_size in rows:
        price = _decimal(raw_price)
        size = _decimal(raw_size)
        if price <= 0 or price >= 1:
            raise OrderBookFrameInvalid(f"invalid snapshot price: {raw_price!r}")
        if size <= 0:
            raise OrderBookFrameInvalid(f"invalid snapshot size: {raw_size!r}")
        normalized[price] = size
    return normalized


def _validate_uncrossed(
    bids: dict[Decimal, Decimal],
    asks: dict[Decimal, Decimal],
) -> None:
    if bids and asks and max(bids) >= min(asks):
        raise OrderBookFrameInvalid(
            f"locked or crossed book: best_bid={max(bids)}, best_ask={min(asks)}"
        )


def _apply_top_fence(
    bids: dict[Decimal, Decimal],
    asks: dict[Decimal, Decimal],
    *,
    best_bid: Any | None,
    best_ask: Any | None,
) -> None:
    if (best_bid is None) != (best_ask is None):
        raise OrderBookFrameInvalid("top-of-book hint must provide both bid and ask")
    if best_bid is None:
        return
    raw_bid = _decimal(best_bid)
    raw_ask = _decimal(best_ask)
    expected_bid = None if raw_bid == 0 else raw_bid
    expected_ask = None if raw_ask == 1 else raw_ask
    if (
        raw_bid < 0
        or raw_bid >= 1
        or raw_ask <= 0
        or raw_ask > 1
        or raw_bid >= raw_ask
    ):
        raise OrderBookFrameInvalid(
            f"invalid top-of-book hint: bid={raw_bid}, ask={raw_ask}"
        )
    for price in tuple(bids):
        if expected_bid is None or price > expected_bid:
            bids.pop(price, None)
    for price in tuple(asks):
        if expected_ask is None or price < expected_ask:
            asks.pop(price, None)
    actual_bid = max(bids, default=None)
    actual_ask = min(asks, default=None)
    if actual_bid != expected_bid or actual_ask != expected_ask:
        raise OrderBookFrameInvalid(
            "state cannot reach authoritative top without inventing liquidity: "
            f"expected={expected_bid}/{expected_ask}, actual={actual_bid}/{actual_ask}"
        )


def _decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"non-finite decimal value: {value!r}")
    return parsed


def _decimal_text(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    return "0" if text == "-0" else text


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return _decimal_text(value) if value is not None else None


def _ms_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
