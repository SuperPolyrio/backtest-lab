"""CLOB book snapshot parsing helpers.

Execution semantics live in :mod:`quant.backtest.l2_orderfilled_execution`.
This module intentionally keeps only the lightweight snapshot DTO and parser
used by DB loading, orderbook coverage, and joint-run adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
from typing import Any


@dataclass(frozen=True)
class BookSnapshot:
    snapshot_id: int
    token_id: str
    side: str
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    source: str = "clob_orderbook_snapshots"
    book_status: str = "unknown"
    block_number: int | None = None
    timestamp: datetime | None = None
    captured_at: datetime | None = None
    snapshot_version: str = ""
    is_full_depth: bool = False
    observed_depth_levels: int | None = None

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0][0] if self.asks else None

    @property
    def spread(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def mid(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / Decimal("2")

    @property
    def bid_depth(self) -> Decimal:
        return sum((price * size for price, size in self.bids), Decimal("0"))

    @property
    def ask_depth(self) -> Decimal:
        return sum((price * size for price, size in self.asks), Decimal("0"))

    @property
    def bid_size_depth(self) -> Decimal:
        return sum((size for _price, size in self.bids), Decimal("0"))

    @property
    def ask_size_depth(self) -> Decimal:
        return sum((size for _price, size in self.asks), Decimal("0"))


def snapshot_version(snapshot: BookSnapshot) -> str:
    digest = hashlib.sha256()
    digest.update(str(snapshot.token_id).encode("utf-8"))
    digest.update(b"|")
    for side in (snapshot.bids, snapshot.asks):
        for price, size in side:
            digest.update(str(price).encode("ascii"))
            digest.update(b":")
            digest.update(str(size).encode("ascii"))
            digest.update(b";")
        digest.update(b"|")
    if snapshot.timestamp:
        digest.update(snapshot.timestamp.isoformat().encode("ascii"))
    if snapshot.block_number is not None:
        digest.update(str(snapshot.block_number).encode("ascii"))
    return digest.hexdigest()[:20]


def parse_book_snapshot(row: dict[str, Any]) -> BookSnapshot:
    raw_payload = row.get("payload")
    payload = raw_payload if isinstance(raw_payload, dict) else {}
    timestamp = _coerce_datetime(row.get("snapshot_timestamp") or row.get("fetched_at") or row.get("captured_at"))
    snapshot = BookSnapshot(
        snapshot_id=int(row["snapshot_id"]),
        token_id=str(row.get("token_id") or ""),
        side=str(row.get("side") or "YES"),
        bids=tuple(_levels(payload.get("bids"), reverse=True)),
        asks=tuple(_levels(payload.get("asks"), reverse=False)),
        source=str(row.get("source") or "clob_orderbook_snapshots"),
        book_status=str(row.get("book_status") or "unknown"),
        block_number=int(row["block_number"]) if row.get("block_number") is not None else None,
        timestamp=timestamp,
        captured_at=_coerce_datetime(row.get("captured_at") or row.get("created_at")),
        snapshot_version=str(row.get("snapshot_version") or ""),
        is_full_depth=bool(row.get("is_full_depth", False)),
        observed_depth_levels=int(row["observed_depth_levels"]) if row.get("observed_depth_levels") is not None else None,
    )
    if snapshot.snapshot_version:
        return snapshot
    return BookSnapshot(**{**snapshot.__dict__, "snapshot_version": snapshot_version(snapshot)})


def snapshot_to_dict(snapshot: BookSnapshot) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot.snapshot_id,
        "token_id": snapshot.token_id,
        "side": snapshot.side,
        "bids": [{"price": str(price), "size": str(size)} for price, size in snapshot.bids],
        "asks": [{"price": str(price), "size": str(size)} for price, size in snapshot.asks],
        "source": snapshot.source,
        "book_status": snapshot.book_status,
        "block_number": snapshot.block_number,
        "timestamp": snapshot.timestamp.isoformat() if snapshot.timestamp else None,
        "captured_at": snapshot.captured_at.isoformat() if snapshot.captured_at else None,
        "snapshot_version": snapshot.snapshot_version,
        "is_full_depth": snapshot.is_full_depth,
        "observed_depth_levels": snapshot.observed_depth_levels,
    }


def snapshot_from_any(value: Any) -> BookSnapshot | None:
    if isinstance(value, BookSnapshot):
        return value
    if not isinstance(value, dict):
        return None
    return BookSnapshot(
        snapshot_id=int(value.get("snapshot_id") or value.get("snapshotId") or 0),
        token_id=str(value.get("token_id") or value.get("tokenId") or ""),
        side=str(value.get("side") or "YES"),
        bids=tuple(_levels(value.get("bids"), reverse=True)),
        asks=tuple(_levels(value.get("asks"), reverse=False)),
        source=str(value.get("source") or "clob_orderbook_snapshots"),
        book_status=str(value.get("book_status") or value.get("bookStatus") or "unknown"),
        block_number=int(value["block_number"]) if value.get("block_number") is not None else int(value["blockNumber"]) if value.get("blockNumber") is not None else None,
        timestamp=_coerce_datetime(value.get("timestamp") or value.get("snapshot_timestamp") or value.get("snapshotTimestamp")),
        captured_at=_coerce_datetime(value.get("captured_at") or value.get("capturedAt")),
        snapshot_version=str(value.get("snapshot_version") or value.get("snapshotVersion") or ""),
        is_full_depth=bool(value.get("is_full_depth", value.get("isFullDepth", False))),
        observed_depth_levels=(
            int(value.get("observed_depth_levels", value.get("observedDepthLevels")))
            if value.get("observed_depth_levels", value.get("observedDepthLevels")) is not None
            else None
        ),
    )


def _levels(rows: Any, *, reverse: bool) -> list[tuple[Decimal, Decimal]]:
    if not isinstance(rows, list):
        return []
    levels: list[tuple[Decimal, Decimal]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        price = _decimal(row.get("price"))
        size = _decimal(row.get("size"))
        if price > 0 and size > 0:
            levels.append((price, size))
    levels.sort(key=lambda item: item[0], reverse=reverse)
    return levels


def _decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except Exception:
        return Decimal("0")
    return parsed if parsed.is_finite() else Decimal("0")


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
