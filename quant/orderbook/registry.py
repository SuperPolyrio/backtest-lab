"""Small multi-token registry around LocalOrderBook."""

from __future__ import annotations

from dataclasses import dataclass, field

from .local_book import (
    BookMetrics,
    LocalOrderBook,
    OrderBookFrameInvalid,
    OrderBookIdentityMismatch,
    TokenBookIdentity,
)
from .polymarket_adapter import (
    NormalizedBookDelta,
    NormalizedBookEvent,
    NormalizedBookSnapshot,
)


@dataclass
class OrderBookRegistry:
    books: dict[str, LocalOrderBook] = field(default_factory=dict)

    def ensure(self, identity: TokenBookIdentity) -> LocalOrderBook:
        book = self.books.get(identity.token_id)
        if book is None:
            book = LocalOrderBook(identity)
            self.books[identity.token_id] = book
        elif _identity_key(book.identity) != _identity_key(identity):
            raise OrderBookIdentityMismatch(
                f"token {identity.token_id!r} identity changed from "
                f"{book.identity!r} to {identity!r}"
            )
        return book

    def get(self, token_id: str) -> LocalOrderBook | None:
        return self.books.get(str(token_id))

    def apply(self, identity: TokenBookIdentity, event: NormalizedBookEvent) -> BookMetrics:
        if event.token_id != identity.token_id:
            raise ValueError(f"event token {event.token_id!r} does not match identity token {identity.token_id!r}")
        book = self.ensure(identity)
        if isinstance(event, NormalizedBookSnapshot):
            return book.apply_snapshot(
                bids=event.bids,
                asks=event.asks,
                event_ts_ms=event.event_ts_ms,
                source_hash=event.source_hash,
                received_ts_ms=event.received_ts_ms,
                source=event.source,
                event_clock=event.event_clock,
                connection_id=event.connection_id,
                connection_generation=event.connection_generation,
                raw_frame_seq=event.raw_frame_seq,
                message_index=event.message_index,
                group_id=event.group_id,
            )
        if isinstance(event, NormalizedBookDelta):
            return book.apply_change(
                side=event.side,
                price=event.price,
                size=event.size,
                event_ts_ms=event.event_ts_ms,
                received_ts_ms=event.received_ts_ms,
                source_hash=event.source_hash,
                source=event.source,
                event_clock=event.event_clock,
                connection_id=event.connection_id,
                connection_generation=event.connection_generation,
                raw_frame_seq=event.raw_frame_seq,
                message_index=event.message_index,
                group_id=event.group_id,
                best_bid=event.best_bid,
                best_ask=event.best_ask,
            )
        raise TypeError(f"unsupported order book event: {type(event)!r}")

    def apply_frame(
        self,
        identity: TokenBookIdentity,
        events: tuple[NormalizedBookEvent, ...] | list[NormalizedBookEvent],
    ) -> BookMetrics:
        """Apply one token's portion of a raw source frame atomically."""

        frame = tuple(events)
        if not frame:
            raise OrderBookFrameInvalid("cannot apply an empty order book frame")
        if any(event.token_id != identity.token_id for event in frame):
            raise OrderBookIdentityMismatch(
                f"order book frame contains an event for a different token than {identity.token_id!r}"
            )
        if len(frame) == 1 and isinstance(frame[0], NormalizedBookSnapshot):
            return self.apply(identity, frame[0])
        if not all(isinstance(event, NormalizedBookDelta) for event in frame):
            raise OrderBookFrameInvalid(
                "one atomic order book frame cannot mix snapshots and deltas"
            )
        deltas = tuple(event for event in frame if isinstance(event, NormalizedBookDelta))
        first = deltas[0]
        provenance_key = _delta_provenance_key(first)
        if any(_delta_provenance_key(event) != provenance_key for event in deltas[1:]):
            raise OrderBookFrameInvalid(
                f"price_change provenance disagrees within token frame {identity.token_id!r}"
            )
        source_hashes = {event.source_hash for event in deltas if event.source_hash}
        if len(source_hashes) > 1:
            raise OrderBookFrameInvalid(
                f"price_change source hashes disagree within token frame {identity.token_id!r}"
            )
        top_pairs = {
            (event.best_bid, event.best_ask)
            for event in deltas
            if event.best_bid is not None or event.best_ask is not None
        }
        if len(top_pairs) > 1:
            raise OrderBookFrameInvalid(
                f"price_change top hints disagree within token frame {identity.token_id!r}"
            )
        best_bid, best_ask = next(iter(top_pairs), (None, None))
        book = self.ensure(identity)
        return book.apply_changes(
            tuple((event.side, event.price, event.size) for event in deltas),
            event_ts_ms=first.event_ts_ms,
            received_ts_ms=first.received_ts_ms,
            source_hash=next(iter(source_hashes), None),
            source=first.source,
            event_clock=first.event_clock,
            connection_id=first.connection_id,
            connection_generation=first.connection_generation,
            raw_frame_seq=first.raw_frame_seq,
            message_index=first.message_index,
            group_id=first.group_id,
            best_bid=best_bid,
            best_ask=best_ask,
        )

    def mark_all_stale(self, reason: str = "registry_stale") -> None:
        for book in self.books.values():
            book.mark_stale(reason)


def _identity_key(identity: TokenBookIdentity) -> tuple[str, int, str, str, int]:
    return (
        str(identity.token_id),
        int(identity.market_id),
        str(identity.condition_id),
        str(identity.outcome).upper(),
        int(identity.outcome_index),
    )


def _delta_provenance_key(event: NormalizedBookDelta) -> tuple[object, ...]:
    return (
        event.event_ts_ms,
        event.received_ts_ms,
        event.source,
        event.connection_id,
        event.connection_generation,
        event.raw_frame_seq,
        event.message_index,
        event.group_id,
        event.event_clock,
    )
