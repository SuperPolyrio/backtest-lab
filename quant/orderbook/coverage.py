"""Coverage reports for LOB-backed execution evidence.

The report is intentionally small and deterministic. It answers one question:
for a simulated order, did we have a local order book snapshot at or before the
execution time that was fresh enough to audit DEPTH execution?
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping

from ..backtest.execution import BookSnapshot


@dataclass(frozen=True)
class LOBCoverageOrder:
    order_id: str
    token_id: str
    submit_time: datetime | None
    submit_block: int | None = None


@dataclass(frozen=True)
class LOBCoverageSnapshot:
    snapshot_id: int
    token_id: str
    timestamp: datetime | None
    block_number: int | None
    book_status: str
    snapshot_version: str = ""


def build_lob_execution_coverage_report(
    orders: list[Any],
    snapshots: list[Any],
    *,
    max_staleness_seconds: Decimal | int | str = Decimal("900"),
) -> dict[str, Any]:
    """Build an auditable no-book/stale-book coverage report for orders."""

    parsed_orders = [_order_from_any(order) for order in orders]
    parsed_snapshots = [_snapshot_from_any(snapshot) for snapshot in snapshots]
    max_staleness = Decimal(str(max_staleness_seconds))
    rows: list[dict[str, Any]] = []
    covered = 0
    no_book = 0
    stale = 0
    for order in parsed_orders:
        matched = _latest_snapshot_at_or_before(order, parsed_snapshots)
        if matched is None:
            no_book += 1
            rows.append(_row(order, reason="no_book", snapshot=None, staleness_seconds=None))
            continue
        staleness_seconds = _staleness_seconds(order, matched)
        if staleness_seconds is not None and staleness_seconds > max_staleness:
            stale += 1
            rows.append(_row(order, reason="stale_book", snapshot=matched, staleness_seconds=staleness_seconds))
            continue
        covered += 1
        rows.append(_row(order, reason="covered", snapshot=matched, staleness_seconds=staleness_seconds))

    order_count = len(parsed_orders)
    coverage_pct = _pct(covered, order_count)
    return {
        "schema_version": "lob_execution_coverage_v1",
        "status": "ready" if order_count and covered == order_count else "review",
        "order_count": order_count,
        "snapshot_count": len(parsed_snapshots),
        "covered_order_count": covered,
        "no_book_count": no_book,
        "stale_book_count": stale,
        "coverage_pct": str(coverage_pct),
        "max_staleness_seconds": str(max_staleness),
        "rows": rows,
    }


def _latest_snapshot_at_or_before(
    order: LOBCoverageOrder,
    snapshots: list[LOBCoverageSnapshot],
) -> LOBCoverageSnapshot | None:
    candidates = [snapshot for snapshot in snapshots if snapshot.token_id == order.token_id]
    candidates = [
        snapshot
        for snapshot in candidates
        if _is_at_or_before_order(snapshot, order) and str(snapshot.book_status or "").lower() not in {"stale", "not_ready"}
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda snapshot: (
            snapshot.timestamp or datetime.min.replace(tzinfo=timezone.utc),
            snapshot.block_number or -1,
            snapshot.snapshot_id,
        ),
        reverse=True,
    )
    return candidates[0]


def _is_at_or_before_order(snapshot: LOBCoverageSnapshot, order: LOBCoverageOrder) -> bool:
    if order.submit_time is not None and snapshot.timestamp is not None:
        return snapshot.timestamp <= order.submit_time
    if order.submit_block is not None and snapshot.block_number is not None:
        return int(snapshot.block_number) <= int(order.submit_block)
    return False


def _staleness_seconds(order: LOBCoverageOrder, snapshot: LOBCoverageSnapshot) -> Decimal | None:
    if order.submit_time is None or snapshot.timestamp is None:
        return None
    seconds = max(0.0, (order.submit_time - snapshot.timestamp).total_seconds())
    return Decimal(str(seconds)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def _row(
    order: LOBCoverageOrder,
    *,
    reason: str,
    snapshot: LOBCoverageSnapshot | None,
    staleness_seconds: Decimal | None,
) -> dict[str, Any]:
    return {
        "order_id": order.order_id,
        "token_id": order.token_id,
        "submit_time": order.submit_time.isoformat() if order.submit_time else None,
        "submit_block": order.submit_block,
        "reason": reason,
        "snapshot_id": snapshot.snapshot_id if snapshot else None,
        "snapshot_timestamp": snapshot.timestamp.isoformat() if snapshot and snapshot.timestamp else None,
        "snapshot_block": snapshot.block_number if snapshot else None,
        "snapshot_version": snapshot.snapshot_version if snapshot else "",
        "staleness_seconds": str(staleness_seconds) if staleness_seconds is not None else None,
    }


def _order_from_any(value: Any) -> LOBCoverageOrder:
    if isinstance(value, LOBCoverageOrder):
        return value
    if isinstance(value, Mapping):
        return LOBCoverageOrder(
            order_id=str(value.get("order_id") or value.get("client_order_id") or ""),
            token_id=str(value.get("token_id") or ""),
            submit_time=_datetime(value.get("submit_time") or value.get("submit_at") or value.get("event_time")),
            submit_block=_int_or_none(value.get("submit_block") or value.get("submit_x")),
        )
    return LOBCoverageOrder(
        order_id=str(getattr(value, "order_id", "") or getattr(value, "client_order_id", "") or ""),
        token_id=str(getattr(value, "token_id", "") or ""),
        submit_time=_datetime(
            getattr(value, "submit_time", None)
            or getattr(value, "submit_at", None)
            or getattr(value, "event_time", None)
        ),
        submit_block=_int_or_none(getattr(value, "submit_block", None) or getattr(value, "submit_x", None)),
    )


def _snapshot_from_any(value: Any) -> LOBCoverageSnapshot:
    if isinstance(value, LOBCoverageSnapshot):
        return value
    if isinstance(value, BookSnapshot):
        return LOBCoverageSnapshot(
            snapshot_id=int(value.snapshot_id),
            token_id=value.token_id,
            timestamp=value.timestamp,
            block_number=value.block_number,
            book_status=value.book_status,
            snapshot_version=value.snapshot_version,
        )
    if isinstance(value, Mapping):
        return LOBCoverageSnapshot(
            snapshot_id=int(value.get("snapshot_id") or 0),
            token_id=str(value.get("token_id") or ""),
            timestamp=_datetime(value.get("timestamp") or value.get("snapshot_timestamp") or value.get("captured_at")),
            block_number=_int_or_none(value.get("block_number")),
            book_status=str(value.get("book_status") or value.get("status") or "unknown"),
            snapshot_version=str(value.get("snapshot_version") or ""),
        )
    return LOBCoverageSnapshot(
        snapshot_id=int(getattr(value, "snapshot_id", 0) or 0),
        token_id=str(getattr(value, "token_id", "") or ""),
        timestamp=_datetime(
            getattr(value, "timestamp", None)
            or getattr(value, "snapshot_timestamp", None)
            or getattr(value, "captured_at", None)
        ),
        block_number=_int_or_none(getattr(value, "block_number", None)),
        book_status=str(getattr(value, "book_status", "unknown") or "unknown"),
        snapshot_version=str(getattr(value, "snapshot_version", "") or ""),
    )


def _datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _pct(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return (Decimal(numerator) / Decimal(denominator) * Decimal("100")).quantize(Decimal("0.01"))
