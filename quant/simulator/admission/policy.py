"""Versioned jurisdiction rules derived from the official local snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .domain import GeoblockSnapshot, JurisdictionMode, stable_hash


DEFAULT_POLICY_VERSION = "polymarket-geoblock-2026-08-18"

BLOCK_COMPLETELY_COUNTRIES = frozenset({"IR", "SY", "CU", "KP"})
BLOCK_COMPLETELY_REGIONS = frozenset({"UA-43", "UA-14", "UA-09"})
CLOSE_ONLY_API_COUNTRIES = frozenset(
    {
        "AU",
        "BY",
        "BE",
        "BI",
        "BR",
        "CF",
        "CD",
        "ET",
        "FR",
        "DE",
        "IQ",
        "IT",
        "LB",
        "LY",
        "MM",
        "NZ",
        "NI",
        "PL",
        "RU",
        "SG",
        "SO",
        "SK",
        "SS",
        "SD",
        "TW",
        "TH",
        "GB",
        "US",
        "UM",
        "VE",
        "YE",
        "ZW",
    }
)
CLOSE_ONLY_API_REGIONS = frozenset({"CA-BC", "CA-ON", "CA-AB", "CA-QC"})


@dataclass(frozen=True)
class JurisdictionPolicy:
    version: str = DEFAULT_POLICY_VERSION

    @property
    def rules(self) -> dict[str, Any]:
        return {
            "block_completely_countries": sorted(BLOCK_COMPLETELY_COUNTRIES),
            "block_completely_regions": sorted(BLOCK_COMPLETELY_REGIONS),
            "close_only_api_countries": sorted(CLOSE_ONLY_API_COUNTRIES),
            "close_only_api_regions": sorted(CLOSE_ONLY_API_REGIONS),
            "source": (
                "docs/reference/polymarket_official/snapshots/"
                "2026-08-18T033128Z/raw/api-reference/geoblock.md"
            ),
        }

    @property
    def rules_hash(self) -> str:
        return stable_hash(self.rules)

    def classify(self, snapshot: GeoblockSnapshot) -> JurisdictionMode:
        country = snapshot.country
        region = _region_code(country, snapshot.region)
        if not snapshot.blocked:
            return JurisdictionMode.UNRESTRICTED
        if country in BLOCK_COMPLETELY_COUNTRIES or region in BLOCK_COMPLETELY_REGIONS:
            return JurisdictionMode.BLOCK_COMPLETELY
        if country in CLOSE_ONLY_API_COUNTRIES or region in CLOSE_ONLY_API_REGIONS:
            return JurisdictionMode.CLOSE_ONLY
        return JurisdictionMode.UNKNOWN


def _region_code(country: str, region: str) -> str:
    selected_country = str(country).strip().upper()
    selected_region = str(region).strip().upper()
    if not selected_region:
        return selected_country
    if selected_region.startswith(f"{selected_country}-"):
        return selected_region
    return f"{selected_country}-{selected_region}"
