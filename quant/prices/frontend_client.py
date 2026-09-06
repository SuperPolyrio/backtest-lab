"""Polymarket prices-history client for frontend-like price series."""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_CLOB_API_BASE = "https://clob.polymarket.com"


@dataclass(frozen=True)
class FrontendPricePoint:
    timestamp: int
    price: Decimal


@dataclass(frozen=True)
class FrontendFetchFailure:
    token_id: str
    start_ts: int
    end_ts: int
    error: str


class FrontendPriceClient:
    def __init__(
        self,
        *,
        clob_api_base: str = DEFAULT_CLOB_API_BASE,
        timeout_seconds: float = 30.0,
        retries: int = 4,
        backoff_factor: float = 0.5,
        session: requests.Session | None = None,
    ) -> None:
        self.clob_api_base = clob_api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.retries = max(1, int(retries))
        self.backoff_factor = backoff_factor
        self.session = session or self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.retries,
            connect=self.retries,
            read=self.retries,
            status=self.retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"Accept": "application/json", "User-Agent": "polyData-quant-prices/1.0"})
        return session

    def fetch_prices_history(self, token_id: str, *, start_ts: int, end_ts: int, fidelity_minutes: int = 1) -> list[FrontendPricePoint]:
        response = self.session.get(
            f"{self.clob_api_base}/prices-history",
            params={
                "market": str(token_id),
                "startTs": int(start_ts),
                "endTs": int(end_ts),
                "fidelity": int(fidelity_minutes),
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json() if response.content else {}
        history = payload.get("history", []) if isinstance(payload, dict) else []
        points: dict[int, FrontendPricePoint] = {}
        for item in history:
            parsed = normalize_price_history_item(item)
            if parsed is None:
                continue
            if start_ts <= parsed.timestamp <= end_ts:
                points[parsed.timestamp] = parsed
        return [points[key] for key in sorted(points)]

    def fetch_prices_history_interval(
        self,
        token_id: str,
        *,
        interval: str = "all",
        fidelity_minutes: int = 60,
    ) -> list[FrontendPricePoint]:
        response = self.session.get(
            f"{self.clob_api_base}/prices-history",
            params={
                "market": str(token_id),
                "interval": str(interval or "all"),
                "fidelity": int(fidelity_minutes),
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json() if response.content else {}
        history = payload.get("history", []) if isinstance(payload, dict) else []
        points: dict[int, FrontendPricePoint] = {}
        for item in history:
            parsed = normalize_price_history_item(item)
            if parsed is None:
                continue
            points[parsed.timestamp] = parsed
        return [points[key] for key in sorted(points)]

    def fetch_segmented_prices_history(
        self,
        token_id: str,
        *,
        start_ts: int,
        end_ts: int,
        fidelity_minutes: int = 1,
        segment_days: int = 14,
        pause_seconds: float = 0.0,
    ) -> tuple[list[FrontendPricePoint], list[FrontendFetchFailure]]:
        rows: dict[int, FrontendPricePoint] = {}
        failures: list[FrontendFetchFailure] = []
        cursor = int(start_ts)
        segment_seconds = max(3600, int(segment_days) * 86400)
        while cursor <= int(end_ts):
            segment_end = min(int(end_ts), cursor + segment_seconds - 1)
            try:
                for point in self.fetch_prices_history(
                    token_id,
                    start_ts=cursor,
                    end_ts=segment_end,
                    fidelity_minutes=fidelity_minutes,
                ):
                    rows[point.timestamp] = point
            except Exception as exc:  # noqa: BLE001
                failures.append(FrontendFetchFailure(str(token_id), cursor, segment_end, repr(exc)))
            cursor = segment_end + 1
            if pause_seconds > 0 and cursor <= int(end_ts):
                time.sleep(pause_seconds)
        return [rows[key] for key in sorted(rows)], failures


def normalize_price_history_item(item: Any) -> FrontendPricePoint | None:
    if not isinstance(item, dict):
        return None
    try:
        timestamp = int(item.get("t"))
        price = Decimal(str(item.get("p")))
    except (TypeError, ValueError, InvalidOperation):
        return None
    if price.is_nan():
        return None
    return FrontendPricePoint(timestamp=timestamp, price=price)
