"""Subscription selection for Polymarket local order books.

The selector is deliberately deterministic and capped. It decides which tokens
deserve live book state; it does not open network connections.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import hashlib
from typing import Any, Iterable, Literal, Mapping


SubscriptionTier = Literal["P0", "P1", "P2", "P3", "P4"]


@dataclass(frozen=True)
class MarketSubscriptionCandidate:
    token_id: str
    market_id: int
    condition_id: str | None = None
    market_slug: str | None = None
    market_title: str | None = None
    token_side: str = "YES"
    outcome_index: int = 0
    category: str | None = None
    focused_by_user: bool = False
    active_strategy_target: bool = False
    in_research_watchlist: bool = False
    volume_24h: Decimal = Decimal("0")
    trade_count_24h: int = 0
    ends_in_seconds: int | None = None
    recent_orderfilled_count: int = 0
    stale: bool = False
    active: bool = True
    closed: bool = False
    resolved: bool = False


@dataclass(frozen=True)
class SubscriptionDecision:
    candidate: MarketSubscriptionCandidate
    score: int
    tier: SubscriptionTier
    reason: str

    @property
    def token_id(self) -> str:
        return self.candidate.token_id


DEFAULT_PRIORITY_DOMAINS = frozenset({"crypto", "btc", "eth", "sol", "sports", "nba", "fifa", "macro"})


def score_subscription_candidate(
    candidate: MarketSubscriptionCandidate,
    *,
    priority_domains: Iterable[str] = DEFAULT_PRIORITY_DOMAINS,
) -> SubscriptionDecision:
    """Score one token for live order book maintenance."""

    if candidate.closed or candidate.resolved or not candidate.active:
        return SubscriptionDecision(candidate, score=-10_000, tier="P4", reason="closed_or_inactive")

    score = 0
    reasons: list[str] = []
    if candidate.focused_by_user:
        score += 1000
        reasons.append("focused")
    if candidate.active_strategy_target:
        score += 700
        reasons.append("strategy")
    if candidate.in_research_watchlist:
        score += 400
        reasons.append("watchlist")
    if (candidate.category or "").strip().lower() in {str(item).lower() for item in priority_domains}:
        score += 250
        reasons.append("priority_domain")
    volume_score = min(250, int(max(Decimal("0"), Decimal(candidate.volume_24h)) // Decimal("1000")))
    if volume_score:
        score += volume_score
        reasons.append("volume")
    trade_score = min(200, max(0, int(candidate.trade_count_24h)))
    if trade_score:
        score += trade_score
        reasons.append("trade_count")
    if candidate.ends_in_seconds is not None and 0 <= int(candidate.ends_in_seconds) <= 86_400:
        score += 100 if int(candidate.ends_in_seconds) <= 3_600 else 50
        reasons.append("near_end")
    recent_score = min(150, max(0, int(candidate.recent_orderfilled_count)) * 5)
    if recent_score:
        score += recent_score
        reasons.append("recent_orderfilled")
    if candidate.stale:
        score -= 200
        reasons.append("stale")

    tier = _tier(candidate, score)
    return SubscriptionDecision(candidate, score=score, tier=tier, reason=",".join(reasons) or "ordinary_active")


def select_subscription_tokens(
    candidates: Iterable[MarketSubscriptionCandidate],
    *,
    max_tokens: int,
    include_tiers: Iterable[SubscriptionTier] = ("P0", "P1", "P2"),
    shard_id: int | None = None,
    shard_count: int | None = None,
) -> list[SubscriptionDecision]:
    """Return capped token decisions sorted by priority."""

    allowed = set(include_tiers)
    decisions = [score_subscription_candidate(candidate) for candidate in candidates]
    decisions = [decision for decision in decisions if decision.tier in allowed]
    if shard_id is not None and shard_count is not None:
        decisions = [
            decision
            for decision in decisions
            if token_shard(decision.token_id, shard_count=shard_count) == int(shard_id)
        ]
    decisions.sort(key=lambda item: (-item.score, item.candidate.market_id, item.token_id))
    unique: list[SubscriptionDecision] = []
    seen: set[str] = set()
    for decision in decisions:
        if decision.token_id in seen:
            continue
        unique.append(decision)
        seen.add(decision.token_id)
        if len(unique) >= max(0, int(max_tokens)):
            break
    return unique


def build_subscription_candidates_from_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    focused_token_ids: Iterable[str] = (),
    strategy_token_ids: Iterable[str] = (),
    watchlist_token_ids: Iterable[str] = (),
) -> list[MarketSubscriptionCandidate]:
    """Build candidate objects from DB/API rows plus explicit runtime marks."""

    focused = {str(token_id) for token_id in focused_token_ids}
    strategy = {str(token_id) for token_id in strategy_token_ids}
    watchlist = {str(token_id) for token_id in watchlist_token_ids}
    candidates: list[MarketSubscriptionCandidate] = []
    for row in rows:
        token_id = str(row.get("token_id") or row.get("tokenId") or "").strip()
        if not token_id:
            continue
        candidates.append(
            MarketSubscriptionCandidate(
                token_id=token_id,
                market_id=int(row.get("market_id") or row.get("marketId") or 0),
                condition_id=_optional_text(row.get("condition_id") or row.get("conditionId")),
                market_slug=_optional_text(row.get("market_slug") or row.get("marketSlug")),
                market_title=_optional_text(row.get("market_title") or row.get("marketTitle") or row.get("question")),
                token_side=str(row.get("token_side") or row.get("tokenSide") or "YES").upper(),
                outcome_index=int(row.get("outcome_index") or row.get("outcomeIndex") or 0),
                category=_optional_text(row.get("category") or row.get("event_category") or row.get("eventCategory")),
                focused_by_user=token_id in focused or _truthy(row.get("focused_by_user") or row.get("focused")),
                active_strategy_target=token_id in strategy or _truthy(row.get("active_strategy_target") or row.get("strategy_target")),
                in_research_watchlist=token_id in watchlist or _truthy(row.get("in_research_watchlist") or row.get("watchlist")),
                volume_24h=_decimal(row.get("volume_24h") or row.get("volume") or 0),
                trade_count_24h=int(row.get("trade_count_24h") or row.get("trade_count") or 0),
                ends_in_seconds=_optional_int(row.get("ends_in_seconds") or row.get("endsInSeconds")),
                recent_orderfilled_count=int(row.get("recent_orderfilled_count") or row.get("orderfilled_rows") or 0),
                stale=_truthy(row.get("stale")),
                active=not _falsey(row.get("active")),
                closed=_truthy(row.get("closed")),
                resolved=_truthy(row.get("resolved")),
            )
        )
    return candidates


def token_shard(token_id: str, *, shard_count: int) -> int:
    if int(shard_count) <= 0:
        raise ValueError("shard_count must be positive")
    digest = hashlib.sha256(str(token_id).encode("utf-8")).hexdigest()
    return int(digest[:12], 16) % int(shard_count)


def _tier(candidate: MarketSubscriptionCandidate, score: int) -> SubscriptionTier:
    if candidate.focused_by_user or candidate.active_strategy_target:
        return "P0"
    if score >= 650:
        return "P1"
    if score > 0:
        return "P2"
    return "P3"


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _falsey(value: Any) -> bool:
    return str(value).strip().lower() in {"0", "false", "no", "n", "off"}
