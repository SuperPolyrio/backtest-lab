from __future__ import annotations

from datetime import datetime, timedelta, timezone

from quant.market.enums import MarketState
from quant.market.market_state_machine import MarketStateMachine
from quant.market.models import LifecycleSignal


NOW = datetime(2026, 7, 6, 8, 0, tzinfo=timezone.utc)


def _signal(event_type: str, payload: dict[str, object]) -> LifecycleSignal:
    return LifecycleSignal(
        source="test",
        event_type=event_type,
        market_id=str(payload.get("id") or "m1"),
        condition_id=str(payload.get("conditionId") or payload.get("condition_id") or "0xcond"),
        asset_id=str(payload.get("asset_id") or "111"),
        source_ts=None,
        local_receive_ts=NOW,
        payload=payload,
        payload_hash="hash",
    )


def test_state_discovered_when_metadata_incomplete() -> None:
    transition = MarketStateMachine().transition(None, _signal("MARKET_DISCOVERED", {"id": "m1"}))

    assert transition.new_state == MarketState.DISCOVERED
    assert transition.allow_execution is False


def test_state_pending_book_when_metadata_complete_no_book() -> None:
    transition = MarketStateMachine().transition(None, _signal("MARKET_DISCOVERED", {"conditionId": "0xcond", "clobTokenIds": ["111"]}))

    assert transition.new_state == MarketState.TRADABLE_PENDING_BOOK
    assert transition.should_probe_assets == ["111"]


def test_state_live_when_book_ready() -> None:
    transition = MarketStateMachine().transition({"market_state": "TRADABLE_PENDING_BOOK"}, _signal("BOOK_READY", {"book_quality": "READY_HIGH"}))

    assert transition.new_state == MarketState.LIVE
    assert transition.allow_execution is True


def test_state_stale_when_book_quality_stale() -> None:
    transition = MarketStateMachine().transition({"market_state": "LIVE"}, _signal("BOOK_STALE", {"book_quality": "STALE"}))

    assert transition.new_state == MarketState.STALE
    assert transition.allow_execution is False


def test_state_closing_when_closed_true() -> None:
    transition = MarketStateMachine().transition({"market_state": "LIVE"}, _signal("MARKET_CLOSED", {"closed": True}))

    assert transition.new_state == MarketState.CLOSING
    assert transition.should_unsubscribe_assets == ["111"]


def test_state_resolved_when_winning_asset_present() -> None:
    transition = MarketStateMachine().transition({"market_state": "CLOSING"}, _signal("MARKET_RESOLVED", {"winning_asset_id": "111"}))

    assert transition.new_state == MarketState.RESOLVED
    assert transition.should_unsubscribe_assets == ["111"]


def test_end_date_does_not_mean_resolved() -> None:
    transition = MarketStateMachine().transition(
        {"market_state": "LIVE"},
        _signal("MARKET_METADATA_UPDATED", {"conditionId": "0xcond", "clobTokenIds": ["111"], "endDate": (NOW - timedelta(hours=1)).isoformat()}),
    )

    assert transition.new_state == MarketState.TRADABLE_PENDING_BOOK
    assert transition.new_state != MarketState.RESOLVED
