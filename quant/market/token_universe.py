"""Compute paper-trading subscription and execution universes.

This module deliberately treats Postgres ``core.*`` market data as the source of
truth and keeps the paper registry as a derived read model.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping, Sequence


DEFAULT_READY_BOOK_STATUSES = frozenset({"ok", "ready", "ready_high", "ready_medium", "live"})
DEFAULT_SUBSCRIPTION_BOOK_STATUSES = frozenset(
    {"ok", "ready", "ready_high", "ready_medium", "live", "one_sided", "empty", "stale", "gap", "disconnected"}
)
DEFAULT_NO_BOOK_STATUSES = frozenset({"no_clob_book", "not_found", "404"})


@dataclass(frozen=True)
class MarketUniverseConfig:
    """Conservative defaults for paper execution eligibility."""

    book_ttl_seconds: int = 900
    min_market_token_count: int = 2
    require_status_snapshot: bool = True
    exclude_placeholders: bool = True
    execution_requires_two_sided_book: bool = True
    ready_book_statuses: frozenset[str] = DEFAULT_READY_BOOK_STATUSES
    subscription_requires_verified_book: bool = False
    subscription_book_statuses: frozenset[str] = DEFAULT_SUBSCRIPTION_BOOK_STATUSES
    no_book_statuses: frozenset[str] = DEFAULT_NO_BOOK_STATUSES


@dataclass(frozen=True)
class MarketRegistryToken:
    """One CLOB token enriched with market state and latest local book evidence."""

    asset_id: str
    market_id: int
    condition_id: str | None = None
    gamma_market_id: str | None = None
    market_slug: str | None = None
    market_title: str | None = None
    outcome_name: str = "UNKNOWN"
    outcome_index: int | None = None
    active: bool = True
    closed: bool = False
    resolved: bool = False
    archived: bool = False
    deprecated: bool = False
    status_present: bool = True
    completion_status: str | None = "OPEN"
    token_count: int = 0
    end_date: datetime | None = None
    latest_book_at: datetime | None = None
    book_status: str | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    book_source: str | None = None
    storage_tier: str | None = None
    winning_asset_id: str | None = None
    winning_outcome: str | None = None
    resolution_status: str | None = None
    resolution_source: str | None = None
    resolved_time: datetime | None = None
    source: str = "core"

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "MarketRegistryToken":
        return cls(
            asset_id=str(row.get("asset_id") or row.get("token_id") or "").strip(),
            market_id=_int(row.get("market_id")),
            condition_id=_text(row.get("condition_id")),
            gamma_market_id=_text(row.get("gamma_market_id")),
            market_slug=_text(row.get("market_slug") or row.get("slug")),
            market_title=_text(row.get("market_title") or row.get("title")),
            outcome_name=str(row.get("outcome_name") or row.get("token_side") or row.get("outcome") or "UNKNOWN").upper(),
            outcome_index=_optional_int(row.get("outcome_index")),
            active=_bool(row.get("active"), default=True),
            closed=_bool(row.get("closed") or row.get("is_trading_closed")),
            resolved=_bool(row.get("resolved") or row.get("is_resolved")),
            archived=_bool(row.get("archived")),
            deprecated=_bool(row.get("deprecated")),
            status_present=_bool(row.get("status_present"), default=True),
            completion_status=_text(row.get("completion_status")),
            token_count=_int(row.get("token_count")),
            end_date=_datetime(row.get("end_date")),
            latest_book_at=_datetime(row.get("latest_book_at") or row.get("snapshot_timestamp") or row.get("fetched_at")),
            book_status=_text(row.get("book_status")),
            best_bid=_decimal_or_none(row.get("best_bid")),
            best_ask=_decimal_or_none(row.get("best_ask")),
            book_source=_text(row.get("book_source") or row.get("source")),
            storage_tier=_text(row.get("storage_tier")),
            winning_asset_id=_text(row.get("winning_asset_id") or row.get("winningAssetId")),
            winning_outcome=_text(row.get("winning_outcome") or row.get("winningOutcome")),
            resolution_status=_text(row.get("resolution_status") or row.get("resolutionStatus")),
            resolution_source=_text(row.get("resolution_source") or row.get("resolutionSource")),
            resolved_time=_datetime(row.get("resolved_time") or row.get("resolvedTime")),
            source=_text(row.get("registry_source")) or _text(row.get("source")) or "core",
        )

    @property
    def is_placeholder(self) -> bool:
        slug = (self.market_slug or "").strip().lower()
        title = (self.market_title or "").strip().lower()
        return slug.startswith("trade-indexer-placeholder-") or title.startswith("trade indexer placeholder")


@dataclass(frozen=True)
class UniverseDecision:
    token: MarketRegistryToken
    subscription_eligible: bool
    execution_eligible: bool
    market_state: str
    subscription_reason: str
    execution_reason: str
    book_quality: str
    book_age_ms: int | None = None

    @property
    def asset_id(self) -> str:
        return self.token.asset_id

    def as_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "market_id": self.token.market_id,
            "condition_id": self.token.condition_id,
            "gamma_market_id": self.token.gamma_market_id,
            "market_slug": self.token.market_slug,
            "outcome_name": self.token.outcome_name,
            "outcome_index": self.token.outcome_index,
            "market_state": self.market_state,
            "subscription_eligible": self.subscription_eligible,
            "execution_eligible": self.execution_eligible,
            "subscription_reason": self.subscription_reason,
            "execution_reason": self.execution_reason,
            "book_quality": self.book_quality,
            "book_age_ms": self.book_age_ms,
            "best_bid": self.token.best_bid,
            "best_ask": self.token.best_ask,
            "book_status": self.token.book_status,
            "latest_book_at": self.token.latest_book_at,
            "book_source": self.token.book_source,
            "storage_tier": self.token.storage_tier,
            "winning_asset_id": self.token.winning_asset_id,
            "winning_outcome": self.token.winning_outcome,
            "resolution_status": self.token.resolution_status,
            "resolution_source": self.token.resolution_source,
            "resolved_time": self.token.resolved_time,
            "completion_status": self.token.completion_status,
            "status_present": self.token.status_present,
            "token_count": self.token.token_count,
        }


@dataclass(frozen=True)
class TokenUniverseDiff:
    generation: int
    subscription_asset_ids: set[str]
    execution_asset_ids: set[str]
    added_subscription_asset_ids: set[str]
    removed_subscription_asset_ids: set[str]
    added_execution_asset_ids: set[str]
    removed_execution_asset_ids: set[str]
    decisions: tuple[UniverseDecision, ...] = field(default_factory=tuple)


class TokenUniverseService:
    """Small service facade matching the spec while staying DB-adapter neutral."""

    def __init__(
        self,
        token_loader: Callable[[], Iterable[MarketRegistryToken]],
        *,
        config: MarketUniverseConfig | None = None,
    ) -> None:
        self.token_loader = token_loader
        self.config = config or MarketUniverseConfig()
        self._last_diff: TokenUniverseDiff | None = None

    def compute_decisions_sync(self, *, now: datetime | None = None) -> list[UniverseDecision]:
        return compute_universe_decisions(self.token_loader(), config=self.config, now=now)

    def compute_subscription_universe_sync(self, *, now: datetime | None = None) -> set[str]:
        return {decision.asset_id for decision in self.compute_decisions_sync(now=now) if decision.subscription_eligible}

    def compute_execution_universe_sync(self, *, now: datetime | None = None) -> set[str]:
        return {decision.asset_id for decision in self.compute_decisions_sync(now=now) if decision.execution_eligible}

    def snapshot_and_diff_sync(self, *, now: datetime | None = None) -> TokenUniverseDiff:
        generation = 1 if self._last_diff is None else self._last_diff.generation + 1
        diff = build_token_universe_diff(
            self.compute_decisions_sync(now=now),
            previous=self._last_diff,
            generation=generation,
        )
        self._last_diff = diff
        return diff

    async def compute_subscription_universe(self) -> set[str]:
        return self.compute_subscription_universe_sync()

    async def compute_execution_universe(self) -> set[str]:
        return self.compute_execution_universe_sync()

    async def snapshot_and_diff(self) -> TokenUniverseDiff:
        return self.snapshot_and_diff_sync()


def compute_universe_decisions(
    tokens: Iterable[MarketRegistryToken],
    *,
    config: MarketUniverseConfig | None = None,
    now: datetime | None = None,
) -> list[UniverseDecision]:
    cfg = config or MarketUniverseConfig()
    clock = _ensure_aware(now or datetime.now(timezone.utc))
    return [classify_market_token(token, config=cfg, now=clock) for token in tokens]


def classify_market_token(
    token: MarketRegistryToken,
    *,
    config: MarketUniverseConfig | None = None,
    now: datetime | None = None,
) -> UniverseDecision:
    cfg = config or MarketUniverseConfig()
    clock = _ensure_aware(now or datetime.now(timezone.utc))
    blockers = _metadata_blockers(token, cfg)
    if blockers:
        state = _blocked_state(blockers)
        reason = ",".join(blockers)
        return UniverseDecision(
            token=token,
            subscription_eligible=False,
            execution_eligible=False,
            market_state=state,
            subscription_reason=reason,
            execution_reason=reason,
            book_quality="NOT_CHECKED",
            book_age_ms=_book_age_ms(token, clock),
        )

    book_age_ms = _book_age_ms(token, clock)
    book_blocker, book_quality, subscription_allowed, state_override = _book_blocker(token, cfg, book_age_ms)
    if book_blocker is None:
        return UniverseDecision(
            token=token,
            subscription_eligible=True,
            execution_eligible=True,
            market_state="LIVE",
            subscription_reason="metadata_ready",
            execution_reason="book_ready",
            book_quality=book_quality,
            book_age_ms=book_age_ms,
        )

    state = state_override or ("STALE" if book_blocker == "stale_book" else "TRADABLE_PENDING_BOOK")
    return UniverseDecision(
        token=token,
        subscription_eligible=subscription_allowed,
        execution_eligible=False,
        market_state=state,
        subscription_reason="metadata_ready" if subscription_allowed else book_blocker,
        execution_reason=book_blocker,
        book_quality=book_quality,
        book_age_ms=book_age_ms,
    )


def build_token_universe_diff(
    decisions: Sequence[UniverseDecision],
    *,
    previous: TokenUniverseDiff | None = None,
    generation: int | None = None,
) -> TokenUniverseDiff:
    subscription = {decision.asset_id for decision in decisions if decision.subscription_eligible}
    execution = {decision.asset_id for decision in decisions if decision.execution_eligible}
    previous_subscription = previous.subscription_asset_ids if previous else set()
    previous_execution = previous.execution_asset_ids if previous else set()
    return TokenUniverseDiff(
        generation=int(generation if generation is not None else ((previous.generation + 1) if previous else 1)),
        subscription_asset_ids=subscription,
        execution_asset_ids=execution,
        added_subscription_asset_ids=subscription - previous_subscription,
        removed_subscription_asset_ids=previous_subscription - subscription,
        added_execution_asset_ids=execution - previous_execution,
        removed_execution_asset_ids=previous_execution - execution,
        decisions=tuple(decisions),
    )


def _metadata_blockers(token: MarketRegistryToken, config: MarketUniverseConfig) -> list[str]:
    blockers: list[str] = []
    if not token.asset_id:
        blockers.append("missing_asset_id")
    if not token.market_id and not token.gamma_market_id and not token.condition_id:
        blockers.append("missing_market_id")
    if not token.condition_id:
        blockers.append("missing_condition_id")
    if config.require_status_snapshot and not token.status_present:
        blockers.append("status_missing")
    if config.exclude_placeholders and token.is_placeholder:
        blockers.append("placeholder_market")
    if token.token_count < config.min_market_token_count:
        blockers.append("insufficient_token_mapping")
    if not token.active:
        blockers.append("inactive_token")
    if token.closed:
        blockers.append("closed_market")
    if token.resolved:
        blockers.append("resolved_market")
    if token.archived:
        blockers.append("archived_market")
    if token.deprecated:
        blockers.append("deprecated_market")
    return blockers


def _blocked_state(blockers: Sequence[str]) -> str:
    if "resolved_market" in blockers:
        return "RESOLVED"
    if "closed_market" in blockers or "inactive_token" in blockers:
        return "CLOSING"
    if "archived_market" in blockers or "deprecated_market" in blockers:
        return "ARCHIVED"
    return "DISCOVERED"


def _book_blocker(
    token: MarketRegistryToken,
    config: MarketUniverseConfig,
    book_age_ms: int | None,
) -> tuple[str | None, str, bool, str | None]:
    status = (token.book_status or "").strip().lower()
    no_book_statuses = {item.lower() for item in config.no_book_statuses}
    subscription_statuses = {item.lower() for item in config.subscription_book_statuses}
    ready_statuses = {item.lower() for item in config.ready_book_statuses}
    if status in no_book_statuses:
        return "no_clob_book", "NO_CLOB_BOOK", True, "TRADABLE_PENDING_BOOK"
    if status == "probe_error":
        return "book_probe_error", "PROBE_ERROR", True, "TRADABLE_PENDING_BOOK"
    if token.latest_book_at is None:
        state = "DISCOVERED_PENDING_BOOK" if config.subscription_requires_verified_book else "TRADABLE_PENDING_BOOK"
        return "no_book_snapshot", "PENDING_BOOK", not config.subscription_requires_verified_book, state
    if status in {"stale", "gap", "disconnected"}:
        return status if status != "disconnected" else "book_disconnected", status.upper(), True, "STALE"
    if book_age_ms is None or book_age_ms > int(config.book_ttl_seconds) * 1000:
        return "stale_book", "STALE", True, "STALE"
    if status and status not in ready_statuses:
        if status in subscription_statuses:
            return ("missing_two_sided_quotes" if status == "one_sided" else "empty_book"), status.upper(), True, "TRADABLE_PENDING_BOOK"
        return "bad_book_status", "BAD_STATUS", True, "TRADABLE_PENDING_BOOK"
    if config.execution_requires_two_sided_book and (token.best_bid is None or token.best_ask is None):
        return "missing_two_sided_quotes", "MISSING_QUOTES", True, "TRADABLE_PENDING_BOOK"
    return None, "READY_MEDIUM", True, None


def _book_age_ms(token: MarketRegistryToken, now: datetime) -> int | None:
    if token.latest_book_at is None:
        return None
    latest = _ensure_aware(token.latest_book_at)
    return max(0, int((now - latest).total_seconds() * 1000))


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _ensure_aware(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return _ensure_aware(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None
