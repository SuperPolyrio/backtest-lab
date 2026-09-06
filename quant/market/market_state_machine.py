"""Small deterministic state machine for registry lifecycle signals."""

from __future__ import annotations

from typing import Any, Mapping

from .enums import MarketLifecycleEventType, MarketState
from .models import LifecycleSignal, StateTransition


class MarketStateMachine:
    """Translate current state + one lifecycle signal into a next state."""

    def transition(self, current: Mapping[str, Any] | None, signal: LifecycleSignal) -> StateTransition:
        old_state = _state(current.get("market_state") if current else None)
        payload = signal.payload or {}
        event_type = str(signal.event_type or "").strip()
        assets = _asset_ids(current, signal)
        if _has_resolution(payload) or event_type == MarketLifecycleEventType.MARKET_RESOLVED:
            return StateTransition(
                old_state=old_state,
                new_state=MarketState.RESOLVED,
                reason="resolution_truth_present",
                should_subscribe_assets=[],
                should_unsubscribe_assets=assets,
                should_probe_assets=[],
                allow_execution=False,
            )
        if _truthy(payload.get("archived")):
            return StateTransition(old_state, MarketState.ARCHIVED, "archived", [], assets, [], False)
        if _closed(payload) or event_type in {MarketLifecycleEventType.MARKET_CLOSED, MarketLifecycleEventType.TRADING_HALTED}:
            return StateTransition(old_state, MarketState.CLOSING, "market_closed_or_inactive", [], assets, [], False)
        if event_type in {MarketLifecycleEventType.BOOK_STALE, MarketLifecycleEventType.BOOK_GAP}:
            return StateTransition(old_state, MarketState.STALE, "book_stale_or_gap", assets, [], [], False)
        if event_type == MarketLifecycleEventType.BOOK_READY or _book_ready(payload):
            return StateTransition(old_state, MarketState.LIVE, "book_ready", assets, [], [], True)
        if _metadata_complete(payload):
            return StateTransition(old_state, MarketState.TRADABLE_PENDING_BOOK, "metadata_ready_no_book", [], [], assets, False)
        return StateTransition(old_state, MarketState.DISCOVERED, "metadata_incomplete", [], [], [], False)


def _state(value: Any) -> MarketState | None:
    if value in (None, ""):
        return None
    try:
        return MarketState(str(value))
    except ValueError:
        return None


def _asset_ids(current: Mapping[str, Any] | None, signal: LifecycleSignal) -> list[str]:
    values = []
    if signal.asset_id:
        values.append(signal.asset_id)
    if current and current.get("asset_id"):
        values.append(str(current["asset_id"]))
    return sorted(set(values))


def _has_resolution(payload: Mapping[str, Any]) -> bool:
    return any(
        str(payload.get(key) or "").strip()
        for key in ("winning_asset_id", "winningAssetId", "winning_outcome", "winningOutcome", "oracle_result")
    )


def _closed(payload: Mapping[str, Any]) -> bool:
    if _truthy(payload.get("closed")) or _truthy(payload.get("is_trading_closed")):
        return True
    if _truthy(payload.get("active")) is False:
        return True
    if _truthy(payload.get("accepting_orders")) is False:
        return True
    if _truthy(payload.get("enable_order_book")) is False or _truthy(payload.get("enableOrderBook")) is False:
        return True
    return False


def _metadata_complete(payload: Mapping[str, Any]) -> bool:
    token_ids = payload.get("clobTokenIds") or payload.get("clob_token_ids") or payload.get("token_ids")
    return bool(str(payload.get("conditionId") or payload.get("condition_id") or "").strip() and token_ids)


def _book_ready(payload: Mapping[str, Any]) -> bool:
    quality = str(payload.get("book_quality") or payload.get("bookQuality") or "").upper()
    return quality in {"READY_HIGH", "READY_MEDIUM"} or bool(payload.get("best_bid") and payload.get("best_ask"))


def _truthy(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None
