"""Stable broad market domains for representative calibration sampling."""

from __future__ import annotations

import re
from typing import Any


REPRESENTATIVE_DOMAINS = ("politics", "sports", "weather", "crypto")

_CATEGORY_ALIASES: dict[str, frozenset[str]] = {
    "politics": frozenset(
        {
            "congress",
            "elections",
            "global-elections",
            "geopolitics",
            "main-election",
            "politics",
            "presidential-election",
            "senate-elections",
            "trump",
            "us-elections",
            "us-presidential-election",
            "world-elections",
        }
    ),
    "sports": frozenset(
        {
            "basketball",
            "formula1",
            "major-league-pickleball",
            "mlb",
            "nba",
            "nfl",
            "nhl",
            "soccer",
            "sports",
            "statistical-leader",
            "tennis",
        }
    ),
    "weather": frozenset(
        {
            "climate",
            "hurricane",
            "rainfall",
            "snowfall",
            "temperature",
            "weather",
        }
    ),
    "crypto": frozenset(
        {
            "bitcoin",
            "crypto",
            "crypto-prices",
            "ethereum",
            "hyperliquid",
            "solana",
            "up-or-down",
        }
    ),
}

_TEXT_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "politics": tuple(
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\belection(s)?\b",
            r"\bpresident(ial)?\b",
            r"\bprime minister\b",
            r"\bcongress\b",
            r"\bsenate\b",
            r"\bparliament\b",
            r"\bdemocrat(ic)?\b",
            r"\brepublican\b",
            r"\btrump\b",
        )
    ),
    "sports": tuple(
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\bnba\b",
            r"\bnfl\b",
            r"\bnhl\b",
            r"\bmlb\b",
            r"\buefa\b",
            r"\bfifa\b",
            r"\bworld cup\b",
            r"\bsoccer\b",
            r"\bfootball\b",
            r"\bbasketball\b",
            r"\bbaseball\b",
            r"\btennis\b",
            r"\bformula ?1\b",
        )
    ),
    "weather": tuple(
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\bweather\b",
            r"\btemperature\b",
            r"\bdegrees? (fahrenheit|celsius)\b",
            r"\brain(fall)?\b",
            r"\bsnow(fall)?\b",
            r"\bhurricane\b",
            r"\btropical storm\b",
        )
    ),
    "crypto": tuple(
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\bcrypto(currency)?\b",
            r"\bbitcoin\b",
            r"\bbtc\b",
            r"\bethereum\b",
            r"\beth\b",
            r"\bsolana\b",
            r"\bsol\b",
            r"\bhyperliquid\b",
        )
    ),
}


def normalize_market_domain(
    source_category: Any,
    *,
    market_title: Any = None,
    event_title: Any = None,
    market_slug: Any = None,
) -> str:
    """Map fine-grained Polymarket categories into a stable broad domain."""

    category = _slug(source_category)
    for domain in REPRESENTATIVE_DOMAINS:
        if category in _CATEGORY_ALIASES[domain]:
            return domain

    searchable = " ".join(
        str(value or "").replace("-", " ")
        for value in (market_title, event_title, market_slug)
    )
    matches = [
        domain
        for domain in REPRESENTATIVE_DOMAINS
        if any(pattern.search(searchable) for pattern in _TEXT_PATTERNS[domain])
    ]
    return matches[0] if len(matches) == 1 else "other"


def category_is_sports(
    source_category: Any,
    *,
    market_title: Any = None,
    event_title: Any = None,
    market_slug: Any = None,
) -> bool:
    return normalize_market_domain(
        source_category,
        market_title=market_title,
        event_title=event_title,
        market_slug=market_slug,
    ) == "sports"


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
