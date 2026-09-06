"""Domain dataclasses for the dynamic paper Market Registry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from .enums import BookQuality, MarketState


@dataclass(frozen=True)
class NormalizedMarket:
    market_id: str
    condition_id: str | None
    question_id: str | None
    event_id: str | None
    slug: str | None
    question: str | None
    active: bool | None
    closed: bool | None
    archived: bool | None
    accepting_orders: bool | None
    enable_order_book: bool | None
    is_resolved: bool | None
    resolution_status: str | None
    current_tick_size: Decimal | None
    minimum_tick_size: Decimal | None
    min_order_size: Decimal | None
    neg_risk: bool | None
    end_date: datetime | None
    game_start_time: datetime | None
    winning_asset_id: str | None
    winning_outcome: str | None
    raw: dict[str, Any]
    metadata_hash: str


@dataclass(frozen=True)
class NormalizedMarketToken:
    market_id: str
    condition_id: str | None
    asset_id: str
    outcome_name: str | None
    outcome_index: int | None
    is_yes: bool | None
    is_no: bool | None
    is_winning: bool | None
    clob_enabled: bool | None
    current_tick_size: Decimal | None
    min_order_size: Decimal | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class LifecycleSignal:
    source: str
    event_type: str
    market_id: str | None
    condition_id: str | None
    asset_id: str | None
    source_ts: datetime | None
    local_receive_ts: datetime
    payload: dict[str, Any]
    payload_hash: str


@dataclass(frozen=True)
class StateTransition:
    old_state: MarketState | None
    new_state: MarketState
    reason: str
    should_subscribe_assets: list[str]
    should_unsubscribe_assets: list[str]
    should_probe_assets: list[str]
    allow_execution: bool


@dataclass(frozen=True)
class RegistryBookProbeResult:
    asset_id: str
    market: str | None
    ok: bool
    book_quality: BookQuality
    timestamp: datetime | None
    book_hash: str | None
    min_order_size: Decimal | None
    tick_size: Decimal | None
    best_bid: Decimal | None
    best_ask: Decimal | None
    error_code: str | None
    raw: dict[str, Any] | None
