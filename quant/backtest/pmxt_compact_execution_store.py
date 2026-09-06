"""Bounded point-in-time books from the materialized PMXT compact tape."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
from typing import Any

from quant.core.db import ClickHouseClient, safe_identifier

from .execution import BookSnapshot, snapshot_version
from .l2_orderfilled_execution import (
    BookDelta as TimelineBookDelta,
    BookLevel as TimelineBookLevel,
    BookSnapshot as TimelineBookSnapshot,
    FillTick,
    L2ExecutionConfig,
    L2OrderFilledExecutionModel,
    OrderExecutionResult,
    StrategyCancelIntent,
    StrategyOrderIntent,
)


DEFAULT_EVENT_TABLE = "pmxt_l2_event_compact"
DEFAULT_LEVEL_TABLE = "pmxt_l2_book_level_compact"
DEFAULT_BUILD_TAG = "pmxt_depth_candidates_streaming_v1"


@dataclass
class PmxtCompactBookProvider:
    """Reconstruct only the book needed by an actual strategy order."""

    condition_id: str
    token_id: str
    market_id: int
    token_side: str
    orderfilled_token_id: str = ""
    client: ClickHouseClient = field(default_factory=ClickHouseClient)
    build_tag: str = DEFAULT_BUILD_TAG
    event_table: str = DEFAULT_EVENT_TABLE
    level_table: str = DEFAULT_LEVEL_TABLE
    fill_table: str = "maker_fill_ticks"
    anchor_lookback_hours: int = 24
    max_deltas_per_order: int = 100_000
    request_count: int = 0
    hit_count: int = 0
    no_snapshot_count: int = 0
    query_error_count: int = 0
    delta_limit_count: int = 0
    loaded_delta_rows: int = 0
    loaded_fill_rows: int = 0
    maker_timeline_count: int = 0
    last_result: dict[str, Any] = field(default_factory=dict)
    _resolved_snapshots: list[BookSnapshot] = field(default_factory=list, repr=False)

    def available(self) -> bool:
        rows = self.client.query_json_rows(
            f"""
            SELECT 1 AS available
            FROM {safe_identifier(self.event_table)}
            WHERE build_tag={_quote(self.build_tag)}
              AND condition_id={_quote(self.condition_id)}
              AND token_id={_quote(self.token_id)}
            LIMIT 1
            """
        )
        return bool(rows)

    def snapshot_at(self, target_ts: datetime) -> BookSnapshot | None:
        self.request_count += 1
        target = _utc(target_ts)
        try:
            anchor = self._latest_snapshot(target)
            if anchor is None:
                self.no_snapshot_count += 1
                self.last_result = {
                    "status": "no_snapshot",
                    "target_ts": target.isoformat(),
                }
                return None
            levels = self._snapshot_levels(anchor)
            deltas = self._deltas_after_anchor(anchor, target)
            if len(deltas) > self.max_deltas_per_order:
                self.delta_limit_count += 1
                self.last_result = {
                    "status": "delta_limit_exceeded",
                    "target_ts": target.isoformat(),
                    "delta_rows": len(deltas),
                    "max_delta_rows": self.max_deltas_per_order,
                }
                return None
            snapshot = self._reconstruct(anchor, levels, deltas)
        except Exception as exc:
            self.query_error_count += 1
            self.last_result = {
                "status": "query_error",
                "target_ts": target.isoformat(),
                "error": f"{type(exc).__name__}: {exc}",
            }
            return None
        self.hit_count += 1
        self.loaded_delta_rows += len(deltas)
        if not self._resolved_snapshots or self._resolved_snapshots[-1].snapshot_version != snapshot.snapshot_version:
            self._resolved_snapshots.append(snapshot)
        self.last_result = {
            "status": "ready",
            "target_ts": target.isoformat(),
            "book_ts": snapshot.timestamp.isoformat() if snapshot.timestamp else None,
            "anchor_ts": str(anchor.get("event_time_text") or ""),
            "snapshot_levels": len(levels),
            "delta_rows": len(deltas),
            "snapshot_id": snapshot.snapshot_id,
            "snapshot_version": snapshot.snapshot_version,
        }
        return snapshot

    def resolved_snapshots(self) -> list[BookSnapshot]:
        return list(self._resolved_snapshots)

    def execute_maker_timeline(
        self,
        *,
        client_order_id: str,
        signal_ts: datetime,
        end_ts: datetime,
        side: str,
        limit_price: Decimal,
        size: Decimal,
        config: L2ExecutionConfig,
        cancel_ts: datetime | None = None,
    ) -> OrderExecutionResult | None:
        """Replay one resting order against compact L2 and raw fill evidence."""

        signal = _utc(signal_ts)
        end = _utc(end_ts)
        if end < signal:
            raise ValueError("maker timeline end_ts must be >= signal_ts")
        received = signal + timedelta(milliseconds=config.submit_latency_ms)
        compact_snapshot = self.snapshot_at(received)
        if compact_snapshot is None:
            return None

        timeline_snapshot = _timeline_snapshot(
            compact_snapshot,
            market_id=str(self.market_id),
            asset_id=self.token_id,
        )
        deltas = self._timeline_deltas(received, end)
        fill_ticks = self._timeline_fill_ticks(received, end)
        intent = StrategyOrderIntent(
            client_order_id=client_order_id,
            signal_ts=signal,
            market_id=str(self.market_id),
            asset_id=self.token_id,
            side="BUY" if str(side).upper().startswith("BUY") else "SELL",
            order_type="LIMIT",
            limit_price=_decimal(limit_price),
            size=_decimal(size),
            tif="GTC",
            post_only=True,
        )
        events: list[Any] = [timeline_snapshot, intent, *deltas, *fill_ticks]
        if cancel_ts is not None:
            events.append(
                StrategyCancelIntent(
                    client_order_id=client_order_id,
                    signal_ts=_utc(cancel_ts),
                    market_id=str(self.market_id),
                    asset_id=self.token_id,
                    cancel_latency_ms=config.cancel_latency_ms,
                )
            )
        # A PMXT size decrease can be the same trade represented by the
        # OrderFilled tick. Until those two events are explicitly reconciled,
        # advancing queue from both would double-count executed liquidity.
        timeline_config = replace(config, use_lob_decrease_for_queue=False)
        model = L2OrderFilledExecutionModel(timeline_config)
        timeline = model.run_event_timeline(events)
        self.maker_timeline_count += 1
        self.loaded_delta_rows += len(deltas)
        self.loaded_fill_rows += len(fill_ticks)
        result = timeline.order_results.get(client_order_id)
        self.last_result = {
            **self.last_result,
            "maker_timeline": {
                "order_id": client_order_id,
                "signal_ts": signal.isoformat(),
                "received_ts": received.isoformat(),
                "end_ts": end.isoformat(),
                "delta_rows": len(deltas),
                "fill_tick_rows": len(fill_ticks),
                "queue_advancement": "orderfilled_trade_only",
                "state": result.state if result is not None else "missing",
                "filled_size": str(result.filled_size) if result is not None else "0",
            },
        }
        return result

    def context(self) -> dict[str, Any]:
        status = "ready" if self.hit_count else "review" if self.request_count else "configured"
        return {
            "schema_version": "pmxt_compact_execution_context_v1",
            "status": status,
            "source": "pmxt_l2_compact",
            "build_tag": self.build_tag,
            "event_table": self.event_table,
            "level_table": self.level_table,
            "fill_table": self.fill_table,
            "condition_id": self.condition_id,
            "market_id": self.market_id,
            "token_id": self.token_id,
            "token_side": self.token_side,
            "request_count": self.request_count,
            "hit_count": self.hit_count,
            "no_snapshot_count": self.no_snapshot_count,
            "query_error_count": self.query_error_count,
            "delta_limit_count": self.delta_limit_count,
            "loaded_delta_rows": self.loaded_delta_rows,
            "loaded_fill_rows": self.loaded_fill_rows,
            "maker_timeline_count": self.maker_timeline_count,
            "last_result": dict(self.last_result),
        }

    def _latest_snapshot(self, target: datetime) -> dict[str, Any] | None:
        from_ts = target - timedelta(hours=max(1, int(self.anchor_lookback_hours)))
        rows = self.client.query_json_rows(
            f"""
            SELECT
              toString(event_time) AS event_time_text,
              source_row_index,
              source_event_index,
              source_hash
            FROM {safe_identifier(self.event_table)}
            WHERE build_tag={_quote(self.build_tag)}
              AND condition_id={_quote(self.condition_id)}
              AND token_id={_quote(self.token_id)}
              AND event_type='book_snapshot'
              AND event_time >= {_dt64(from_ts)}
              AND event_time <= {_dt64(target)}
            ORDER BY event_time DESC, source_row_index DESC, source_event_index DESC
            LIMIT 1
            """
        )
        return dict(rows[0]) if rows else None

    def _snapshot_levels(self, anchor: dict[str, Any]) -> list[dict[str, Any]]:
        hash_filter = ""
        source_hash = str(anchor.get("source_hash") or "")
        if source_hash:
            hash_filter = f" AND source_hash={_quote(source_hash)}"
        return self.client.query_json_rows(
            f"""
            SELECT side, level_index, toString(price) AS price, toString(size) AS size
            FROM {safe_identifier(self.level_table)}
            WHERE build_tag={_quote(self.build_tag)}
              AND condition_id={_quote(self.condition_id)}
              AND token_id={_quote(self.token_id)}
              AND event_time={_dt64(_parse_dt(anchor['event_time_text']))}
              AND source_row_index={int(anchor.get('source_row_index') or 0)}
              {hash_filter}
            ORDER BY side ASC, level_index ASC
            """
        )

    def _deltas_after_anchor(self, anchor: dict[str, Any], target: datetime) -> list[dict[str, Any]]:
        anchor_ts = _parse_dt(anchor["event_time_text"])
        anchor_row = int(anchor.get("source_row_index") or 0)
        anchor_event = int(anchor.get("source_event_index") or 0)
        return self.client.query_json_rows(
            f"""
            SELECT
              toString(event_time) AS event_time_text,
              operation,
              side,
              toString(price) AS price,
              toString(size) AS size,
              toString(best_bid) AS best_bid,
              toString(best_ask) AS best_ask,
              source_row_index,
              source_event_index,
              source_hash
            FROM {safe_identifier(self.event_table)}
            WHERE build_tag={_quote(self.build_tag)}
              AND condition_id={_quote(self.condition_id)}
              AND token_id={_quote(self.token_id)}
              AND event_type='price_change'
              AND event_time <= {_dt64(target)}
              AND (
                event_time > {_dt64(anchor_ts)}
                OR (event_time = {_dt64(anchor_ts)} AND source_row_index > {anchor_row})
                OR (event_time = {_dt64(anchor_ts)} AND source_row_index = {anchor_row} AND source_event_index > {anchor_event})
              )
            ORDER BY event_time ASC, source_row_index ASC, source_event_index ASC
            LIMIT {int(self.max_deltas_per_order) + 1}
            """
        )

    def _timeline_deltas(self, from_ts: datetime, to_ts: datetime) -> list[TimelineBookDelta]:
        rows = self.client.query_json_rows(
            f"""
            SELECT
              toString(event_time) AS event_time_text,
              side,
              toString(price) AS price,
              toString(size) AS size,
              toString(best_bid) AS best_bid,
              toString(best_ask) AS best_ask,
              source_row_index,
              source_event_index,
              source_hash
            FROM {safe_identifier(self.event_table)}
            WHERE build_tag={_quote(self.build_tag)}
              AND condition_id={_quote(self.condition_id)}
              AND token_id={_quote(self.token_id)}
              AND event_type='price_change'
              AND event_time > {_dt64(from_ts)}
              AND event_time <= {_dt64(to_ts)}
            ORDER BY event_time ASC, source_row_index ASC, source_event_index ASC
            LIMIT {int(self.max_deltas_per_order) + 1}
            """
        )
        if len(rows) > self.max_deltas_per_order:
            self.delta_limit_count += 1
            return []
        result: list[TimelineBookDelta] = []
        for timeline_sequence, row in enumerate(rows, start=1):
            side = _book_side(row.get("side"))
            price = _decimal(row.get("price"))
            if side is None or price <= 0:
                continue
            result.append(
                TimelineBookDelta(
                    ts=_parse_dt(row["event_time_text"]),
                    market_id=str(self.market_id),
                    asset_id=self.token_id,
                    side="BUY" if side == "bid" else "SELL",
                    price=price,
                    new_size=max(Decimal("0"), _decimal(row.get("size"))),
                    # source_row_index is global to the hourly parquet and is
                    # not monotonic within one token after filtering.
                    sequence=timeline_sequence,
                    source="pmxt_l2_compact",
                    hash=str(row.get("source_hash") or "") or None,
                    best_bid=_decimal(row.get("best_bid")) if row.get("best_bid") not in {None, ""} else None,
                    best_ask=_decimal(row.get("best_ask")) if row.get("best_ask") not in {None, ""} else None,
                )
            )
        return result

    def _timeline_fill_ticks(self, from_ts: datetime, to_ts: datetime) -> list[FillTick]:
        replay_token = str(self.orderfilled_token_id or self.token_id).strip().lower()
        rows = self.client.query_json_rows(
            f"""
            SELECT
              fill_id,
              block_number,
              tx_index AS transaction_index,
              log_index,
              lower(tx_hash) AS tx_hash,
              order_hash,
              toString(price) AS trade_price,
              toString(size_shares) AS size,
              upper(toString(passive_side)) AS passive_side,
              upper(toString(aggressor_side)) AS aggressor_side,
              lower(maker) AS maker,
              lower(taker) AS taker,
              toUnixTimestamp64Milli(block_time) AS block_ts_ms,
              toString(fee_usdc) AS fee
            FROM {safe_identifier(self.fill_table)}
            PREWHERE market_id={int(self.market_id)}
              AND asset_id={_quote(replay_token)}
            WHERE block_time >= {_dt64(from_ts)}
              AND block_time <= {_dt64(to_ts)}
            ORDER BY block_number ASC, transaction_index ASC, log_index ASC,
                     tx_hash ASC, fill_id ASC
            LIMIT 1000001
            """
        )
        if len(rows) > 1_000_000:
            raise RuntimeError("maker timeline OrderFilled limit exceeded")
        result: list[FillTick] = []
        seen: set[str] = set()
        for row in rows:
            key = str(row.get("fill_id") or "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            passive_side = str(row.get("passive_side") or "").upper()
            aggressor_side = str(row.get("aggressor_side") or "").upper()
            if passive_side not in {"BUY", "SELL"} or aggressor_side not in {"BUY", "SELL"}:
                continue
            ts = datetime.fromtimestamp(int(row.get("block_ts_ms") or 0) / 1000, tz=timezone.utc)
            # Polygon block_time is second-resolution. Preserve deterministic
            # within-block ordering without moving the event into another second.
            offset_us = min(999_999, int(row.get("transaction_index") or 0) * 1_000 + int(row.get("log_index") or 0))
            ts += timedelta(microseconds=offset_us)
            result.append(
                FillTick(
                    ts=ts,
                    block_number=int(row.get("block_number") or 0),
                    tx_hash=str(row.get("tx_hash") or ""),
                    log_index=int(row.get("log_index") or 0),
                    order_hash=str(row.get("order_hash") or key or f"{row.get('tx_hash')}:{row.get('log_index')}"),
                    market_id=str(self.market_id),
                    asset_id=self.token_id,
                    price=_decimal(row.get("trade_price")),
                    size=max(Decimal("0"), _decimal(row.get("size"))),
                    passive_side=passive_side,
                    aggressor_side=aggressor_side,
                    maker=str(row.get("maker") or ""),
                    taker=str(row.get("taker") or ""),
                    fee=max(Decimal("0"), _decimal(row.get("fee"))),
                    source="orderfilled",
                )
            )
        return result

    def _reconstruct(
        self,
        anchor: dict[str, Any],
        levels: list[dict[str, Any]],
        deltas: list[dict[str, Any]],
    ) -> BookSnapshot:
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        for row in levels:
            side = _book_side(row.get("side"))
            price = _decimal(row.get("price"))
            size = _decimal(row.get("size"))
            if side and price > 0 and size > 0:
                (bids if side == "bid" else asks)[price] = size
        last_ts = _parse_dt(anchor["event_time_text"])
        last_row = int(anchor.get("source_row_index") or 0)
        for row in deltas:
            side = _book_side(row.get("side"))
            price = _decimal(row.get("price"))
            size = _decimal(row.get("size"))
            if side is None or price <= 0:
                continue
            book = bids if side == "bid" else asks
            if str(row.get("operation") or "").lower() in {"delete", "remove"} or size <= 0:
                book.pop(price, None)
            else:
                book[price] = size
            if row.get("best_bid") not in {None, ""}:
                best_bid = _decimal(row.get("best_bid"))
                for stale_price in [item for item in bids if item > best_bid]:
                    bids.pop(stale_price, None)
            if row.get("best_ask") not in {None, ""}:
                best_ask = _decimal(row.get("best_ask"))
                for stale_price in [item for item in asks if item < best_ask]:
                    asks.pop(stale_price, None)
            last_ts = _parse_dt(row["event_time_text"])
            last_row = int(row.get("source_row_index") or last_row)
        version_seed = "|".join(
            (
                self.build_tag,
                self.condition_id,
                self.token_id,
                str(anchor.get("event_time_text") or ""),
                str(anchor.get("source_row_index") or 0),
                last_ts.isoformat(),
                str(last_row),
            )
        )
        snapshot = BookSnapshot(
            snapshot_id=int(hashlib.sha256(version_seed.encode("utf-8")).hexdigest()[:13], 16),
            token_id=self.token_id,
            side=str(self.token_side or "YES").upper(),
            bids=tuple(sorted(bids.items(), key=lambda item: item[0], reverse=True)),
            asks=tuple(sorted(asks.items(), key=lambda item: item[0])),
            source="pmxt_l2_compact",
            book_status="ok" if bids or asks else "empty",
            timestamp=last_ts,
            captured_at=last_ts,
            snapshot_version="",
            is_full_depth=True,
            observed_depth_levels=len(bids) + len(asks),
        )
        return BookSnapshot(**{**snapshot.__dict__, "snapshot_version": snapshot_version(snapshot)})


def _quote(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _parse_dt(value: Any) -> datetime:
    text = str(value or "").strip().replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return _utc(parsed)


def _dt64(value: datetime) -> str:
    text = _utc(value).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"toDateTime64({_quote(text)}, 3, 'UTC')"


def _decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except Exception:
        return Decimal("0")
    return parsed if parsed.is_finite() else Decimal("0")


def _book_side(value: Any) -> str | None:
    side = str(value or "").strip().lower()
    if side in {"bid", "buy", "yes_bid"}:
        return "bid"
    if side in {"ask", "sell", "yes_ask"}:
        return "ask"
    return None


def _timeline_snapshot(snapshot: BookSnapshot, *, market_id: str, asset_id: str) -> TimelineBookSnapshot:
    ts = snapshot.timestamp or snapshot.captured_at
    if ts is None:
        raise ValueError("compact snapshot requires timestamp for maker timeline")
    return TimelineBookSnapshot(
        ts=_utc(ts),
        market_id=market_id,
        asset_id=asset_id,
        sequence=0,
        source=snapshot.source,
        bids=tuple(TimelineBookLevel(price, size) for price, size in snapshot.bids),
        asks=tuple(TimelineBookLevel(price, size) for price, size in snapshot.asks),
        hash=snapshot.snapshot_version or None,
        is_full_depth=snapshot.is_full_depth,
        observed_depth_levels=snapshot.observed_depth_levels,
    )
