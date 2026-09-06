from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any

from quant.backtest.orderfilled_v2_replay import V2TradePrint

Q = Decimal("0.0000000001")
UNKNOWN = "unknown"

LEAGUE_PATTERNS = (
    ("nba", r"\bnba\b|basketball"),
    ("wnba", r"\bwnba\b"),
    ("nfl", r"\bnfl\b|super-bowl"),
    ("mlb", r"\bmlb\b|world-series"),
    ("nhl", r"\bnhl\b|stanley-cup"),
    ("epl", r"\bepl\b|premier-league"),
    ("champions-league", r"champions-league|\bucl\b"),
    ("fifa", r"fifa|world-cup"),
    ("ufc", r"\bufc\b"),
    ("esports", r"\blol\b|league-of-legends|counter-strike|valorant|dota"),
)


@dataclass(frozen=True)
class HierarchicalContext:
    market_id: str
    category: str
    league: str
    price_bucket: str
    tte_bucket: str
    liquidity_regime: str
    side: str = UNKNOWN

    def value(self, field: str) -> str:
        return str(getattr(self, field, UNKNOWN) or UNKNOWN)

    def as_dict(self) -> dict[str, str]:
        return {
            "market_id": self.market_id,
            "category": self.category,
            "league": self.league,
            "price_bucket": self.price_bucket,
            "tte_bucket": self.tte_bucket,
            "liquidity_regime": self.liquidity_regime,
            "side": self.side,
        }


def context_for_order(
    order: Any,
    pre_arrival_trades: Iterable[V2TradePrint],
) -> HierarchicalContext:
    rows = list(pre_arrival_trades)
    category, league = infer_market_taxonomy(
        getattr(order, "category", None),
        getattr(order, "market_slug", None),
        getattr(order, "market_title", None),
        getattr(order, "league", None),
    )
    return HierarchicalContext(
        market_id=str(getattr(order, "market_id", UNKNOWN)),
        category=category,
        league=league,
        price_bucket=price_bucket(Decimal(str(getattr(order, "limit_price", 0)))),
        tte_bucket=tte_bucket(
            getattr(order, "signal_ts", None), getattr(order, "market_end_ts", None)
        ),
        liquidity_regime=liquidity_regime(rows),
        side=_normalize(getattr(order, "side", None)),
    )


def infer_market_taxonomy(
    category: Any,
    slug: Any,
    title: Any,
    explicit_league: Any = None,
) -> tuple[str, str]:
    raw_category = _normalize(category)
    text = " ".join((_normalize(slug), _normalize(title), raw_category))
    league = _normalize(explicit_league)
    if league == UNKNOWN:
        for candidate, pattern in LEAGUE_PATTERNS:
            if re.search(pattern, text, flags=re.IGNORECASE):
                league = candidate
                break
    sports_categories = {
        "sports",
        "soccer",
        "basketball",
        "football",
        "baseball",
        "hockey",
        "esports",
        "nfl",
        "nba",
        "wnba",
        "mlb",
        "nhl",
        "epl",
        "champions-league",
        "ufc",
    }
    broad = (
        "sports"
        if league != UNKNOWN or raw_category in sports_categories
        else raw_category
    )
    return broad, league


def price_bucket(price: Decimal) -> str:
    value = max(Decimal(0), min(Decimal(1), price))
    for upper, label in (
        (Decimal("0.10"), "00_10"),
        (Decimal("0.25"), "10_25"),
        (Decimal("0.40"), "25_40"),
        (Decimal("0.60"), "40_60"),
        (Decimal("0.75"), "60_75"),
        (Decimal("0.90"), "75_90"),
        (Decimal("1.01"), "90_100"),
    ):
        if value < upper:
            return label
    return "90_100"


def tte_bucket(signal_ts: datetime | None, end_ts: datetime | None) -> str:
    if not isinstance(signal_ts, datetime) or not isinstance(end_ts, datetime):
        return UNKNOWN
    seconds = max(0.0, (end_ts - signal_ts).total_seconds())
    for upper, label in (
        (600, "00_10m"),
        (1_800, "10_30m"),
        (5_400, "30_90m"),
        (14_400, "90_240m"),
        (86_400, "04h_01d"),
        (604_800, "01_07d"),
        (2_592_000, "07_30d"),
        (float("inf"), "30d_plus"),
    ):
        if seconds < upper:
            return label
    return "30d_plus"


def liquidity_regime(trades: Iterable[V2TradePrint]) -> str:
    rows = list(trades)
    count = len(rows)
    volume = sum((Decimal(str(row.size)) for row in rows), Decimal(0))
    if count == 0:
        return "none"
    if count <= 2 or volume < Decimal(50):
        return "sparse"
    if count <= 10 or volume < Decimal(500):
        return "medium"
    return "active"


@lru_cache(maxsize=8)
def load_hierarchical_prior_profile(path: str | Path) -> dict[str, Any]:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = Path(__file__).resolve().parents[3] / resolved
    return json.loads(resolved.read_text(encoding="utf-8"))


def resolve_hierarchical_prior(
    profile: Mapping[str, Any],
    *,
    horizon_seconds: int,
    context: HierarchicalContext,
    minimum_samples: int = 0,
) -> dict[str, Any]:
    horizons = (
        profile.get("horizons") if isinstance(profile.get("horizons"), Mapping) else {}
    )
    horizon = (
        horizons.get(str(int(horizon_seconds)))
        if isinstance(horizons, Mapping)
        else None
    )
    if not isinstance(horizon, Mapping):
        return {}
    raw_levels = horizon.get("levels")
    levels: list[Any] = raw_levels if isinstance(raw_levels, list) else []
    for level in reversed(levels):
        if not isinstance(level, Mapping):
            continue
        fields = tuple(str(value) for value in level.get("fields", ()))
        if any(context.value(field) == UNKNOWN for field in fields):
            continue
        cells = level.get("cells") if isinstance(level.get("cells"), Mapping) else {}
        key = _cell_key(fields, context)
        cell = cells.get(key) if isinstance(cells, Mapping) else None
        if (
            isinstance(cell, Mapping)
            and int(cell.get("samples") or 0) >= minimum_samples
        ):
            return {
                **dict(cell),
                "level": "+".join(fields),
                "cell_key": key,
                "context": context.as_dict(),
            }
    global_cell = horizon.get("global")
    if (
        isinstance(global_cell, Mapping)
        and int(global_cell.get("samples") or 0) >= minimum_samples
    ):
        return {
            **dict(global_cell),
            "level": "global",
            "cell_key": "global",
            "context": context.as_dict(),
        }
    return {}


def _cell_key(fields: Iterable[str], context: HierarchicalContext) -> str:
    return "|".join(context.value(field) for field in fields)


def _normalize(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return text or UNKNOWN
