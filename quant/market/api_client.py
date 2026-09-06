"""Polymarket API helpers for the paper market registry service."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable, Mapping

from quant.core.db import env_bool, env_first

from .token_universe import MarketRegistryToken

requests: Any
try:
    import requests as requests
except ImportError:  # pragma: no cover
    requests = None


DEFAULT_GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DEFAULT_CLOB_API_BASE = "https://clob.polymarket.com"
DEFAULT_USER_AGENT = "prediction-market-quant/paper-market-registry"


@dataclass(frozen=True)
class MarketRegistryApiConfig:
    gamma_api_base: str = env_first("POLYDATA_GAMMA_API_BASE", default=DEFAULT_GAMMA_API_BASE)
    clob_api_base: str = env_first("POLYDATA_CLOB_API_BASE", default=DEFAULT_CLOB_API_BASE)
    timeout_seconds: float = 15.0
    proxy_mode: str = env_first("POLYDATA_MARKET_REGISTRY_PROXY_MODE", default="direct")
    proxy_url: str = env_first("POLYDATA_MARKET_REGISTRY_PROXY_URL", default="")
    fallback_proxy_urls: str = env_first("POLYDATA_MARKET_REGISTRY_FALLBACK_PROXY_URLS", default="")
    trust_env_proxy: bool = env_bool("POLYDATA_MARKET_REGISTRY_TRUST_ENV_PROXY", False)
    max_retries: int = 2
    backoff_seconds: float = 0.5
    primary_proxy_retry_seconds: float = 300.0


@dataclass(frozen=True)
class BookProbeResult:
    asset_id: str
    ok: bool
    book_status: str
    book_quality: str
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    bid_depth: Decimal = Decimal("0")
    ask_depth: Decimal = Decimal("0")
    level_count_bid: int = 0
    level_count_ask: int = 0
    payload: dict[str, Any] | None = None
    error: str | None = None
    observed_at: datetime | None = None

    def apply_to_token(self, token: MarketRegistryToken) -> MarketRegistryToken:
        observed_at = self.observed_at or datetime.now(timezone.utc)
        return replace(
            token,
            latest_book_at=observed_at if self.ok else token.latest_book_at,
            book_status=self.book_status,
            best_bid=self.best_bid,
            best_ask=self.best_ask,
            book_source="clob-book-probe",
            storage_tier="probe",
        )


class PolymarketApiClient:
    """Small synchronous client for Gamma market deltas and CLOB book probes."""

    def __init__(
        self,
        config: MarketRegistryApiConfig | None = None,
        *,
        session: Any | None = None,
    ) -> None:
        if requests is None:
            raise RuntimeError("requests is required for Polymarket API access")
        self.config = config or MarketRegistryApiConfig()
        self.session: Any = session or requests.Session()
        self.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
        self.discovery_errors: list[str] = []
        self.discovery_stats: dict[str, Any] = {}
        self._proxy_urls = _proxy_pool(self.config.proxy_url, self.config.fallback_proxy_urls)
        self._active_proxy_index = 0
        self._primary_proxy_retry_at = 0.0
        self._configure_proxy()

    def _configure_proxy(self) -> None:
        mode = str(self.config.proxy_mode or "direct").strip().lower()
        self.session.trust_env = bool(self.config.trust_env_proxy or mode == "env")
        if mode == "direct":
            self.session.trust_env = False
            self.session.proxies = {}
        if self._proxy_urls:
            self._set_active_proxy(0)

    @property
    def proxy_summary(self) -> dict[str, Any]:
        return {
            "proxy_mode": self.config.proxy_mode,
            "proxy_url_configured": bool(self.config.proxy_url),
            "fallback_proxy_count": max(0, len(self._proxy_urls) - 1),
            "active_proxy_is_fallback": self._active_proxy_index > 0,
            "trust_env_proxy": bool(getattr(self.session, "trust_env", False)),
            "env_proxy_detected": detect_proxy_environment(),
            "recommended_clash_primary_profile": "12",
            "recommended_clash_fallback_profile": "8",
        }

    def fetch_gamma_active_markets(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        url = self.config.gamma_api_base.rstrip("/") + "/markets"
        response = self._get(
            url,
            params={"active": "true", "closed": "false", "limit": max(1, int(limit)), "offset": max(0, int(offset))},
            timeout=float(self.config.timeout_seconds),
        )
        if response.status_code == 422 and "offset too large" in getattr(response, "text", "").lower():
            return []
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            data = payload.get("data") or payload.get("markets") or []
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
        return []

    def fetch_gamma_active_events(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        url = self.config.gamma_api_base.rstrip("/") + "/events"
        response = self._get(
            url,
            params={"active": "true", "closed": "false", "limit": max(1, int(limit)), "offset": max(0, int(offset))},
            timeout=float(self.config.timeout_seconds),
        )
        if response.status_code == 422 and "offset too large" in getattr(response, "text", "").lower():
            return []
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            data = payload.get("data") or payload.get("events") or []
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
        return []

    def fetch_gamma_recent_markets(self, *, limit: int = 500) -> list[dict[str, Any]]:
        """Return recently changed markets, including closed/resolved rows."""

        url = self.config.gamma_api_base.rstrip("/") + "/markets"
        response = self._get(
            url,
            params={
                "limit": min(500, max(1, int(limit))),
                "order": "updatedAt",
                "ascending": "false",
            },
            timeout=float(self.config.timeout_seconds),
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            data = payload.get("data") or payload.get("markets") or []
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
        return []

    def fetch_gamma_market(
        self,
        market_id: str,
        *,
        timeout_seconds: float | None = None,
        attempts: int | None = None,
    ) -> dict[str, Any]:
        value = str(market_id or "").strip()
        if not value:
            raise ValueError("market_id is required")
        url = self.config.gamma_api_base.rstrip("/") + "/markets/" + value
        response = self._get(
            url,
            timeout=float(timeout_seconds or self.config.timeout_seconds),
            _attempts=attempts,
        )
        response.raise_for_status()
        payload = response.json()
        return dict(payload) if isinstance(payload, dict) else {}

    def fetch_gamma_recent_keyset_markets(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        closed: bool | None = None,
    ) -> dict[str, Any]:
        url = self.config.gamma_api_base.rstrip("/") + "/markets/keyset"
        params: dict[str, Any] = {
            "limit": min(100, max(1, int(limit))),
            "order": "updatedAt",
            "ascending": "false",
        }
        if closed is not None:
            params["closed"] = "true" if closed else "false"
        if cursor:
            params["after_cursor"] = cursor
        response = self._get(url, params=params, timeout=float(self.config.timeout_seconds))
        response.raise_for_status()
        payload = response.json()
        rows: list[dict[str, Any]] = []
        next_cursor: str | None = None
        if isinstance(payload, dict):
            data = payload.get("markets") or payload.get("data") or []
            if isinstance(data, list):
                rows = [item for item in data if isinstance(item, dict)]
            next_cursor = _text(payload.get("next_cursor") or payload.get("cursor"))
        if isinstance(payload, list):
            rows = [item for item in payload if isinstance(item, dict)]
        return {"markets": rows, "next_cursor": next_cursor}

    def fetch_gamma_recent_keyset_markets_all(
        self,
        *,
        max_markets: int = 500,
        page_size: int = 100,
        closed: bool | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while len(rows) < max(1, int(max_markets)):
            remaining = max(1, int(max_markets)) - len(rows)
            page_limit = min(max(1, int(page_size)), remaining)
            page = self._fetch_keyset_page_with_retry(
                lambda: self.fetch_gamma_recent_keyset_markets(
                    limit=page_limit,
                    cursor=cursor,
                    closed=closed,
                ),
                label="gamma_recent_markets",
            )
            page_rows = list(page.get("markets") or [])
            rows.extend(page_rows)
            next_cursor = _text(page.get("next_cursor"))
            if not page_rows or not next_cursor:
                break
            if next_cursor == cursor or next_cursor in seen_cursors:
                raise RuntimeError("Gamma recent markets keyset pagination repeated its cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return rows[: max(1, int(max_markets))]

    def fetch_gamma_recent_keyset_markets_since(
        self,
        *,
        updated_since: datetime,
        max_markets: int = 20_000,
        page_size: int = 100,
        closed: bool | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Page recent market changes until the durable updatedAt watermark."""

        since = _datetime(updated_since)
        if since is None:
            raise ValueError("updated_since is required")
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        reached_watermark = False
        hard_limit = max(1, int(max_markets))
        while len(rows) < hard_limit:
            page_limit = min(max(1, int(page_size)), 100, hard_limit - len(rows))
            page = self._fetch_keyset_page_with_retry(
                lambda: self.fetch_gamma_recent_keyset_markets(
                    limit=page_limit,
                    cursor=cursor,
                    closed=closed,
                ),
                label="gamma_recent_markets",
            )
            page_rows = list(page.get("markets") or [])
            rows.extend(page_rows)
            timestamps = [
                value
                for value in (_datetime(row.get("updatedAt") or row.get("updated_at")) for row in page_rows)
                if value is not None
            ]
            if timestamps and min(timestamps) <= since:
                reached_watermark = True
                break
            next_cursor = _text(page.get("next_cursor"))
            if not page_rows or not next_cursor:
                reached_watermark = True
                break
            if next_cursor == cursor or next_cursor in seen_cursors:
                raise RuntimeError("Gamma recent markets keyset pagination repeated its cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return rows, reached_watermark

    def fetch_gamma_keyset_markets(self, *, limit: int = 100, cursor: str | None = None) -> dict[str, Any]:
        url = self.config.gamma_api_base.rstrip("/") + "/markets/keyset"
        params: dict[str, Any] = {
            "limit": max(1, int(limit)),
            "order": "createdAt",
            "ascending": "false",
            "active": "true",
            "closed": "false",
        }
        if cursor:
            params["after_cursor"] = cursor
        response = self._get(
            url,
            params=params,
            timeout=float(self.config.timeout_seconds),
        )
        response.raise_for_status()
        payload = response.json()
        rows: list[dict[str, Any]] = []
        next_cursor: str | None = None
        if isinstance(payload, dict):
            data = payload.get("markets") or payload.get("data") or []
            if isinstance(data, list):
                rows = [item for item in data if isinstance(item, dict)]
            next_cursor = _text(payload.get("next_cursor") or payload.get("cursor"))
        if isinstance(payload, list):
            rows = [item for item in payload if isinstance(item, dict)]
        return {"markets": rows, "next_cursor": next_cursor}

    def fetch_gamma_keyset_events(self, *, limit: int = 100, cursor: str | None = None) -> dict[str, Any]:
        url = self.config.gamma_api_base.rstrip("/") + "/events/keyset"
        params: dict[str, Any] = {
            "limit": max(1, int(limit)),
            "order": "createdAt",
            "ascending": "false",
            "active": "true",
            "closed": "false",
        }
        if cursor:
            params["after_cursor"] = cursor
        response = self._get(
            url,
            params=params,
            timeout=float(self.config.timeout_seconds),
        )
        response.raise_for_status()
        payload = response.json()
        rows: list[dict[str, Any]] = []
        next_cursor: str | None = None
        if isinstance(payload, dict):
            data = payload.get("events") or payload.get("data") or []
            if isinstance(data, list):
                rows = [item for item in data if isinstance(item, dict)]
            next_cursor = _text(payload.get("next_cursor") or payload.get("cursor"))
        if isinstance(payload, list):
            rows = [item for item in payload if isinstance(item, dict)]
        return {"events": rows, "next_cursor": next_cursor}

    def fetch_gamma_keyset_markets_all(
        self,
        *,
        max_markets: int | None = None,
        page_size: int = 500,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            remaining = None if max_markets is None else max(0, int(max_markets) - len(rows))
            if remaining == 0:
                break
            page_limit = min(max(1, int(page_size)), 100)
            if remaining is not None:
                page_limit = min(page_limit, remaining)
            page = self._fetch_keyset_page_with_retry(
                lambda: self.fetch_gamma_keyset_markets(limit=page_limit, cursor=cursor),
                label="gamma_markets",
            )
            page_rows = list(page.get("markets") or [])
            rows.extend(page_rows)
            next_cursor = _text(page.get("next_cursor"))
            if not next_cursor:
                break
            if next_cursor == cursor or next_cursor in seen_cursors:
                raise RuntimeError("Gamma markets keyset pagination repeated its cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return rows

    def fetch_gamma_keyset_events_all(
        self,
        *,
        max_events: int | None = None,
        page_size: int = 500,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        while True:
            remaining = None if max_events is None else max(0, int(max_events) - len(rows))
            if remaining == 0:
                break
            page_limit = min(max(1, int(page_size)), 100)
            if remaining is not None:
                page_limit = min(page_limit, remaining)
            page = self._fetch_keyset_page_with_retry(
                lambda: self.fetch_gamma_keyset_events(limit=page_limit, cursor=cursor),
                label="gamma_events",
            )
            page_rows = list(page.get("events") or [])
            rows.extend(page_rows)
            next_cursor = _text(page.get("next_cursor"))
            if not next_cursor:
                break
            if next_cursor == cursor or next_cursor in seen_cursors:
                raise RuntimeError("Gamma events keyset pagination repeated its cursor")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return rows

    def fetch_gamma_active_markets_all(
        self,
        *,
        max_markets: int | None = None,
        page_size: int = 500,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            remaining = None if max_markets is None else max(0, int(max_markets) - len(rows))
            if remaining == 0:
                break
            page_limit = min(max(1, int(page_size)), 100)
            if remaining is not None:
                page_limit = min(page_limit, remaining)
            page_rows = self.fetch_gamma_active_markets(limit=page_limit, offset=offset)
            rows.extend(page_rows)
            if not page_rows or len(page_rows) < page_limit:
                break
            offset += len(page_rows)
        return rows

    def fetch_gamma_active_events_all(
        self,
        *,
        max_events: int | None = None,
        page_size: int = 500,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            remaining = None if max_events is None else max(0, int(max_events) - len(rows))
            if remaining == 0:
                break
            page_limit = min(max(1, int(page_size)), 100)
            if remaining is not None:
                page_limit = min(page_limit, remaining)
            page_rows = self.fetch_gamma_active_events(limit=page_limit, offset=offset)
            rows.extend(page_rows)
            if not page_rows or len(page_rows) < page_limit:
                break
            offset += len(page_rows)
        return rows

    def fetch_clob_markets(self, *, cursor: str | None = None) -> dict[str, Any]:
        url = self.config.clob_api_base.rstrip("/") + "/markets"
        params: dict[str, Any] = {}
        if cursor:
            params["next_cursor"] = cursor
        response = self._get(
            url,
            params=params,
            timeout=float(self.config.timeout_seconds),
        )
        response.raise_for_status()
        payload = response.json()
        rows: list[dict[str, Any]] = []
        next_cursor: str | None = None
        if isinstance(payload, dict):
            data = payload.get("data") or payload.get("markets") or []
            if isinstance(data, list):
                rows = [item for item in data if isinstance(item, dict)]
            next_cursor = _text(payload.get("next_cursor") or payload.get("cursor"))
            if _is_terminal_clob_cursor(next_cursor):
                next_cursor = None
        if isinstance(payload, list):
            rows = [item for item in payload if isinstance(item, dict)]
        return {"markets": rows, "next_cursor": next_cursor}

    def fetch_clob_open_markets_all(
        self,
        *,
        max_markets: int | None = None,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Read CLOB /markets pages and keep only locally tradable market rows.

        The CLOB endpoint currently ignores ``limit`` and returns old-to-new
        pages, so ``max_markets`` caps accepted open/book-enabled rows rather
        than raw rows read.
        """

        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        pages_read = 0
        raw_rows_seen = 0
        while True:
            if max_pages is not None and pages_read >= max(0, int(max_pages)):
                break
            remaining = None if max_markets is None else max(0, int(max_markets) - len(rows))
            if remaining == 0:
                break
            page = self.fetch_clob_markets(cursor=cursor)
            pages_read += 1
            page_rows = list(page.get("markets") or [])
            raw_rows_seen += len(page_rows)
            for row in page_rows:
                if is_open_book_enabled_clob_market(row):
                    rows.append(row)
                    if max_markets is not None and len(rows) >= int(max_markets):
                        break
            next_cursor = _text(page.get("next_cursor"))
            if not page_rows or not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        self.discovery_stats["clob_markets_pages_read"] = pages_read
        self.discovery_stats["clob_markets_raw_rows_seen"] = raw_rows_seen
        self.discovery_stats["clob_markets_open_book_enabled"] = len(rows)
        return rows

    def fetch_api_market_tokens(
        self,
        *,
        limit: int | None = 100,
        page_size: int = 500,
        include_clob_markets: bool = True,
    ) -> list[MarketRegistryToken]:
        self.discovery_errors = []
        self.discovery_stats = {}
        rows: list[Mapping[str, Any]] = []
        clob_rows: list[Mapping[str, Any]] = []
        market_loaders = (
            ("markets_keyset", lambda: self.fetch_gamma_keyset_markets_all(max_markets=limit, page_size=page_size)),
            ("markets_offset", lambda: self.fetch_gamma_active_markets_all(max_markets=limit, page_size=page_size)),
        )
        for name, loader in market_loaders:
            try:
                loaded = loader()
                if loaded:
                    rows.extend(loaded)
                    break
            except Exception as exc:  # noqa: BLE001 - keep other discovery sources alive.
                self.discovery_errors.append(f"{name}: {exc}")
        # /markets is the full active-universe source. /events is a redundant
        # container feed and can be much larger because each event embeds its
        # markets. Keep it bounded so a full sync does not accumulate the
        # entire event catalogue in memory after already loading all markets.
        event_limit = min(1_000, max(1, int(limit))) if limit is not None else 1_000
        self.discovery_stats["gamma_events_supplement_limit"] = event_limit
        event_loaders = (
            ("events_keyset", lambda: self.fetch_gamma_keyset_events_all(max_events=event_limit, page_size=page_size)),
            ("events_offset", lambda: self.fetch_gamma_active_events_all(max_events=event_limit, page_size=page_size)),
        )
        for name, loader in event_loaders:
            try:
                loaded_events = loader()
                if loaded_events:
                    rows.extend(markets_from_gamma_events(loaded_events))
                    break
            except Exception as exc:  # noqa: BLE001 - events are supplemental to direct market listing.
                self.discovery_errors.append(f"{name}: {exc}")
        if include_clob_markets:
            try:
                clob_rows = self.fetch_clob_open_markets_all(max_markets=limit)
            except Exception as exc:  # noqa: BLE001 - CLOB /markets is supplemental to Gamma.
                self.discovery_errors.append(f"clob_markets: {exc}")
        if not rows and not clob_rows and self.discovery_errors:
            raise RuntimeError("; ".join(self.discovery_errors))
        tokens: list[MarketRegistryToken] = []
        gamma_rows = _unique_market_rows(rows)
        active_gamma_rows = [row for row in gamma_rows if is_active_open_gamma_market(row)]
        self.discovery_stats["gamma_market_rows_seen"] = len(gamma_rows)
        self.discovery_stats["gamma_active_open_rows"] = len(active_gamma_rows)
        self.discovery_stats["gamma_non_active_rows_ignored"] = len(gamma_rows) - len(active_gamma_rows)
        for row in active_gamma_rows:
            tokens.extend(tokens_from_gamma_market(row))
        for row in _unique_market_rows(clob_rows):
            tokens.extend(tokens_from_clob_market(row))
        return tokens

    def probe_book(self, asset_id: str) -> BookProbeResult:
        observed_at = datetime.now(timezone.utc)
        url = self.config.clob_api_base.rstrip("/") + "/book"
        try:
            response = self._get(
                url,
                params={"token_id": str(asset_id)},
                timeout=float(self.config.timeout_seconds),
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                payload = {"raw": payload}
            return book_probe_from_payload(str(asset_id), payload, observed_at=observed_at)
        except Exception as exc:  # noqa: BLE001 - probe result should be persisted, not crash loops
            return BookProbeResult(
                asset_id=str(asset_id),
                ok=False,
                book_status="probe_error",
                book_quality="ERROR",
                error=str(exc),
                observed_at=observed_at,
            )

    def probe_books(self, asset_ids: Iterable[str], *, batch_size: int = 50) -> dict[str, BookProbeResult]:
        ids = [str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()]
        results: dict[str, BookProbeResult] = {}
        if not ids:
            return results
        if len(ids) == 1:
            asset_id = ids[0]
            return {asset_id: self.probe_book(asset_id)}
        for batch in chunked(ids, max(1, int(batch_size))):
            results.update(self._probe_books_batch(batch))
        return results

    def _probe_books_batch(self, asset_ids: list[str]) -> dict[str, BookProbeResult]:
        observed_at = datetime.now(timezone.utc)
        url = self.config.clob_api_base.rstrip("/") + "/books"
        try:
            response = self._post(
                url,
                json=[{"token_id": str(asset_id)} for asset_id in asset_ids],
                timeout=float(self.config.timeout_seconds),
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                payload = []
            results: dict[str, BookProbeResult] = {}
            for item in payload:
                if not isinstance(item, Mapping):
                    continue
                asset_id = _text(item.get("asset_id") or item.get("token_id"))
                if not asset_id:
                    continue
                results[asset_id] = book_probe_from_payload(asset_id, item, observed_at=observed_at)
            for asset_id in asset_ids:
                if asset_id not in results:
                    results[asset_id] = BookProbeResult(
                        asset_id=asset_id,
                        ok=False,
                        book_status="no_clob_book",
                        book_quality="NO_CLOB_BOOK",
                        error="bulk /books response did not include this asset_id",
                        observed_at=observed_at,
                    )
            return results
        except Exception as exc:  # noqa: BLE001 - a bad/slow batch must not hang full-universe probing.
            return {
                str(asset_id): BookProbeResult(
                    asset_id=str(asset_id),
                    ok=False,
                    book_status="probe_error",
                    book_quality="ERROR",
                    error=str(exc),
                    observed_at=observed_at,
                )
                for asset_id in asset_ids
            }

    def _get(self, url: str, **kwargs: Any) -> Any:
        return self._request("get", url, **kwargs)

    def _post(self, url: str, **kwargs: Any) -> Any:
        return self._request("post", url, **kwargs)

    def _fetch_keyset_page_with_retry(
        self,
        loader: Callable[[], dict[str, Any]],
        *,
        label: str,
    ) -> dict[str, Any]:
        attempts = max(2, int(self.config.max_retries) + 2)
        for attempt in range(attempts):
            try:
                return loader()
            except Exception:
                if attempt >= attempts - 1:
                    raise
                close = getattr(self.session, "close", None)
                if callable(close):
                    close()
                key = f"{label}_page_retries"
                self.discovery_stats[key] = int(self.discovery_stats.get(key) or 0) + 1
                time.sleep(min(5.0, float(self.config.backoff_seconds) * (2 ** attempt)))
        raise RuntimeError(f"{label} page retry exhausted")  # pragma: no cover

    def _request(self, method: str, url: str, **kwargs: Any) -> Any:
        requested_attempts = kwargs.pop("_attempts", None)
        if (
            len(self._proxy_urls) > 1
            and self._active_proxy_index > 0
            and time.monotonic() >= self._primary_proxy_retry_at
        ):
            self._set_active_proxy(0)
        attempts = (
            max(1, int(requested_attempts))
            if requested_attempts is not None
            else max(1, int(self.config.max_retries) + 1, len(self._proxy_urls))
        )
        last_exc: Exception | None = None
        for attempt in range(attempts):
            try:
                func = getattr(self.session, method)
                return func(url, **kwargs)
            except Exception as exc:  # noqa: BLE001 - retry wrapper preserves the final request exception.
                last_exc = exc
                if attempt >= attempts - 1:
                    raise
                self._rotate_proxy()
                time.sleep(float(self.config.backoff_seconds) * (attempt + 1))
        if last_exc is not None:  # pragma: no cover
            raise last_exc
        raise RuntimeError("request failed before execution")

    def _set_active_proxy(self, index: int) -> None:
        if not self._proxy_urls:
            return
        self._active_proxy_index = int(index) % len(self._proxy_urls)
        proxy_url = self._proxy_urls[self._active_proxy_index]
        self.session.trust_env = False
        self.session.proxies = {"http": proxy_url, "https": proxy_url}

    def _rotate_proxy(self) -> None:
        if len(self._proxy_urls) <= 1:
            return
        next_index = (self._active_proxy_index + 1) % len(self._proxy_urls)
        self._set_active_proxy(next_index)
        if next_index > 0:
            self._primary_proxy_retry_at = time.monotonic() + max(
                0.0,
                float(self.config.primary_proxy_retry_seconds),
            )


def tokens_from_gamma_market(row: Mapping[str, Any]) -> list[MarketRegistryToken]:
    condition_id = _text(row.get("conditionId") or row.get("condition_id") or row.get("condition_id_hex"))
    slug = _text(row.get("slug"))
    gamma_market_id = _text(row.get("id") or row.get("gamma_market_id"))
    title = _text(row.get("question") or row.get("title") or row.get("name") or slug)
    token_ids = _json_list(row.get("clobTokenIds") or row.get("clob_token_ids"))
    outcomes = _json_list(row.get("outcomes") or row.get("outcomeNames") or row.get("outcome_names"))
    outcome_prices = _json_list(row.get("outcomePrices") or row.get("outcome_prices"))
    active = _truthy(row.get("active"), default=True)
    closed = _truthy(row.get("closed"), default=False)
    archived = _truthy(row.get("archived"), default=False)
    end_date = _datetime(row.get("endDate") or row.get("end_date"))
    winning_asset_id = _text(
        row.get("winningAssetId")
        or row.get("winning_asset_id")
        or row.get("winningTokenId")
        or row.get("winning_token_id")
        or row.get("winnerAssetId")
    )
    winning_outcome = _text(
        row.get("winningOutcome")
        or row.get("winning_outcome")
        or row.get("resolutionOutcome")
        or row.get("resolvedOutcome")
        or row.get("winner")
    )
    if winning_outcome:
        winning_outcome = winning_outcome.upper()
    if winning_outcome and not winning_asset_id:
        winning_asset_id = _asset_for_outcome(token_ids, outcomes, winning_outcome)
    if winning_asset_id and not winning_outcome:
        winning_outcome = _outcome_for_asset(token_ids, outcomes, winning_asset_id)
    status_text = str(
        row.get("resolution_status")
        or row.get("resolutionStatus")
        or row.get("umaResolutionStatus")
        or row.get("uma_resolution_status")
        or ""
    ).strip().lower()
    resolution_pending = status_text in {"proposed", "disputed", "pending"}
    explicitly_accepting_orders = (
        active
        and not closed
        and _truthy(row.get("acceptingOrders") or row.get("accepting_orders"), default=False)
        and _truthy(row.get("enableOrderBook") or row.get("enable_order_book"), default=False)
    )
    closed = closed or (resolution_pending and not explicitly_accepting_orders)
    explicit_resolved = (
        _truthy(row.get("resolved"), default=False)
        or _truthy(row.get("isResolved"), default=False)
        or _truthy(row.get("is_resolved"), default=False)
        or status_text == "resolved"
    )
    if explicit_resolved and not winning_asset_id and not winning_outcome:
        winning_index = _winning_outcome_index(outcome_prices)
        if winning_index is not None:
            if winning_index < len(token_ids):
                winning_asset_id = _text(token_ids[winning_index])
            if winning_index < len(outcomes):
                winning_outcome = _text(outcomes[winning_index])
                if winning_outcome:
                    winning_outcome = winning_outcome.upper()
    resolved = explicit_resolved or bool(winning_asset_id or winning_outcome)
    raw_resolution_status = _text(
        row.get("resolutionStatus")
        or row.get("resolution_status")
        or row.get("umaResolutionStatus")
        or row.get("uma_resolution_status")
    )
    resolution_status = raw_resolution_status.upper() if raw_resolution_status else ("RESOLVED" if resolved else None)
    resolution_source = _text(row.get("resolutionSource") or row.get("resolution_source")) or ("gamma_api" if resolved else None)
    resolved_time = None
    if resolved:
        resolved_time = _datetime(
            row.get("resolvedTime")
            or row.get("resolved_time")
            or row.get("resolutionTime")
            or row.get("closedTime")
            or row.get("umaEndDate")
        )
    tokens: list[MarketRegistryToken] = []
    for idx, token_id in enumerate(token_ids):
        text = str(token_id or "").strip()
        if not text:
            continue
        outcome = str(outcomes[idx] if idx < len(outcomes) else ("YES" if idx == 0 else "NO")).upper()
        tokens.append(
            MarketRegistryToken(
                asset_id=text,
                market_id=0,
                gamma_market_id=gamma_market_id,
                condition_id=condition_id,
                market_slug=slug,
                market_title=title,
                outcome_name=outcome,
                outcome_index=idx,
                active=active,
                closed=closed,
                resolved=resolved,
                archived=archived,
                deprecated=False,
                status_present=True,
                completion_status="RESOLVED" if resolved else ("OPEN" if active and not closed else "GAMMA_CLOSED"),
                token_count=len([item for item in token_ids if str(item or "").strip()]),
                end_date=end_date,
                winning_asset_id=winning_asset_id,
                winning_outcome=winning_outcome,
                resolution_status=resolution_status,
                resolution_source=resolution_source,
                resolved_time=resolved_time,
                source="gamma_api_open_book" if explicitly_accepting_orders else "gamma_api",
            )
        )
    return tokens


def tokens_from_clob_market(row: Mapping[str, Any]) -> list[MarketRegistryToken]:
    if not is_open_book_enabled_clob_market(row):
        return []
    condition_id = _text(row.get("condition_id") or row.get("conditionId"))
    slug = _text(row.get("market_slug") or row.get("slug"))
    title = _text(row.get("question") or row.get("title") or row.get("name") or slug)
    gamma_market_id = _text(row.get("gamma_market_id") or row.get("id") or row.get("market_id"))
    token_entries = _clob_token_entries(row)
    token_count = len(token_entries)
    end_date = _datetime(row.get("end_date_iso") or row.get("endDate") or row.get("end_date"))
    tokens: list[MarketRegistryToken] = []
    for idx, item in enumerate(token_entries):
        asset_id = _text(item.get("token_id") or item.get("asset_id") or item.get("asset"))
        if not asset_id:
            continue
        outcome = _text(item.get("outcome") or item.get("name") or item.get("outcome_name"))
        tokens.append(
            MarketRegistryToken(
                asset_id=asset_id,
                market_id=0,
                gamma_market_id=gamma_market_id,
                condition_id=condition_id,
                market_slug=slug,
                market_title=title,
                outcome_name=(outcome or ("YES" if idx == 0 else "NO")).upper(),
                outcome_index=idx,
                active=True,
                closed=False,
                resolved=False,
                archived=False,
                deprecated=False,
                status_present=True,
                completion_status="OPEN",
                token_count=token_count,
                end_date=end_date,
                source="clob_markets_api",
            )
        )
    return tokens


def tokens_from_ws_new_market(event: Mapping[str, Any]) -> list[MarketRegistryToken]:
    """Map the native market-stream discovery event without waiting for Gamma.

    Polymarket includes both outcome token IDs and the condition ID in every
    ``new_market`` message.  Those fields are sufficient to subscribe the L2
    collector immediately; book readiness remains a later, fail-closed step.
    """

    nested = event.get("payload")
    row = nested if isinstance(nested, Mapping) else event
    event_type = str(
        event.get("event_type")
        or event.get("type")
        or row.get("event_type")
        or row.get("type")
        or ""
    ).strip()
    if event_type != "new_market":
        return []
    token_ids = _json_list(
        row.get("assets_ids")
        or row.get("token_ids")
        or row.get("clob_token_ids")
        or row.get("clobTokenIds")
    )
    condition_id = _text(row.get("condition_id") or row.get("market"))
    if not condition_id or not token_ids:
        return []
    outcomes = _json_list(row.get("outcomes"))
    slug = _text(row.get("slug"))
    title = _text(row.get("question") or row.get("title") or slug)
    market_id = _text(row.get("id") or row.get("market_id"))
    active = _truthy(row.get("active"), default=True)
    normalized_ids = [
        value for value in (_text(token_id) for token_id in token_ids) if value
    ]
    token_count = len(normalized_ids)
    return [
        MarketRegistryToken(
            asset_id=asset_id,
            market_id=0,
            gamma_market_id=market_id,
            condition_id=condition_id,
            market_slug=slug,
            market_title=title,
            outcome_name=str(
                outcomes[index]
                if index < len(outcomes)
                else ("YES" if index == 0 else "NO")
            ).upper(),
            outcome_index=index,
            active=active,
            closed=False,
            resolved=False,
            archived=False,
            deprecated=False,
            status_present=True,
            completion_status="OPEN",
            token_count=token_count,
            source="ws_new_market",
        )
        for index, asset_id in enumerate(normalized_ids)
    ]


def is_open_book_enabled_clob_market(row: Mapping[str, Any]) -> bool:
    if not _truthy(row.get("active"), default=False):
        return False
    if _truthy(row.get("closed"), default=True):
        return False
    if _truthy(row.get("archived"), default=False):
        return False
    if not _truthy(_first_present(row, "enable_order_book", "enableOrderBook"), default=False):
        return False
    accepting_orders = _first_present(row, "accepting_orders", "acceptingOrders")
    if accepting_orders is not None and not _truthy(accepting_orders, default=False):
        return False
    return len(_clob_token_entries(row)) > 0


def is_active_open_gamma_market(row: Mapping[str, Any]) -> bool:
    active = _first_present(row, "active")
    if active is not None and not _truthy(active, default=False):
        return False
    if _truthy(_first_present(row, "closed"), default=False):
        return False
    if _truthy(_first_present(row, "archived"), default=False):
        return False
    return True


def _asset_for_outcome(token_ids: list[Any], outcomes: list[Any], winning_outcome: str) -> str | None:
    target = str(winning_outcome or "").strip().upper()
    for idx, outcome in enumerate(outcomes):
        if str(outcome or "").strip().upper() == target and idx < len(token_ids):
            return _text(token_ids[idx])
    return None


def _winning_outcome_index(prices: list[Any]) -> int | None:
    winners: list[int] = []
    for idx, value in enumerate(prices):
        try:
            if Decimal(str(value)) == Decimal("1"):
                winners.append(idx)
        except (ArithmeticError, ValueError):
            continue
    return winners[0] if len(winners) == 1 else None


def _outcome_for_asset(token_ids: list[Any], outcomes: list[Any], winning_asset_id: str) -> str | None:
    target = str(winning_asset_id or "").strip()
    for idx, token_id in enumerate(token_ids):
        if str(token_id or "").strip() == target and idx < len(outcomes):
            outcome = _text(outcomes[idx])
            return outcome.upper() if outcome else None
    return None


def markets_from_gamma_events(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, Mapping):
            continue
        event_active = _truthy(event.get("active"), default=True)
        event_closed = _truthy(event.get("closed"), default=False)
        for market in event.get("markets") or []:
            if not isinstance(market, Mapping):
                continue
            row = dict(market)
            row.setdefault("active", event_active)
            row.setdefault("closed", event_closed)
            rows.append(row)
    return rows


def _unique_market_rows(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    seen: set[str] = set()
    unique: list[Mapping[str, Any]] = []
    for row in rows:
        token_ids = _market_token_ids(row)
        key = _text(row.get("conditionId") or row.get("condition_id") or row.get("id"))
        if token_ids:
            key = ",".join(str(item) for item in token_ids if str(item or "").strip())
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _market_token_ids(row: Mapping[str, Any]) -> list[Any]:
    token_ids = _json_list(row.get("clobTokenIds") or row.get("clob_token_ids"))
    if token_ids:
        return token_ids
    return [item.get("token_id") for item in _clob_token_entries(row) if _text(item.get("token_id"))]


def _clob_token_entries(row: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    value = row.get("tokens")
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping) and _text(item.get("token_id") or item.get("asset_id") or item.get("asset"))]
    token_ids = _json_list(row.get("clobTokenIds") or row.get("clob_token_ids"))
    outcomes = _json_list(row.get("outcomes") or row.get("outcomeNames") or row.get("outcome_names"))
    entries: list[Mapping[str, Any]] = []
    for idx, token_id in enumerate(token_ids):
        text = _text(token_id)
        if not text:
            continue
        entries.append({"token_id": text, "outcome": outcomes[idx] if idx < len(outcomes) else None})
    return entries


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def book_probe_from_payload(asset_id: str, payload: Mapping[str, Any], *, observed_at: datetime) -> BookProbeResult:
    bids = _levels(payload.get("bids") or payload.get("buys") or [])
    asks = _levels(payload.get("asks") or payload.get("sells") or [])
    best_bid = max((level[0] for level in bids), default=None)
    best_ask = min((level[0] for level in asks), default=None)
    bid_depth = sum((level[1] for level in bids), Decimal("0"))
    ask_depth = sum((level[1] for level in asks), Decimal("0"))
    if best_bid is not None and best_ask is not None:
        quality = "READY_MEDIUM"
        status = "ok"
    elif bids or asks:
        quality = "READY_LOW"
        status = "one_sided"
    else:
        quality = "EMPTY"
        status = "empty"
    return BookProbeResult(
        asset_id=str(asset_id),
        ok=quality in {"READY_MEDIUM", "READY_LOW"},
        book_status=status,
        book_quality=quality,
        best_bid=best_bid,
        best_ask=best_ask,
        bid_depth=bid_depth,
        ask_depth=ask_depth,
        level_count_bid=len(bids),
        level_count_ask=len(asks),
        payload=dict(payload),
        observed_at=observed_at,
    )


def detect_proxy_environment() -> dict[str, str]:
    keys = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
    return {key: os.environ[key] for key in keys if os.environ.get(key)}


def _proxy_pool(primary: str, fallbacks: str) -> list[str]:
    values = [primary, *str(fallbacks or "").split(",")]
    return list(dict.fromkeys(value.strip() for value in values if value and value.strip()))


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return [text]
    return [value]


def _levels(value: Any) -> list[tuple[Decimal, Decimal]]:
    levels: list[tuple[Decimal, Decimal]] = []
    if not isinstance(value, list):
        return levels
    for item in value:
        if isinstance(item, Mapping):
            price = item.get("price") or item.get("p")
            size = item.get("size") or item.get("s") or item.get("quantity")
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            price, size = item[0], item[1]
        else:
            continue
        try:
            levels.append((Decimal(str(price)), Decimal(str(size))))
        except Exception:
            continue
    return levels


def _is_terminal_clob_cursor(value: str | None) -> bool:
    return str(value or "").strip() in {"-1", "LTE="}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _truthy(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip().replace("Z", "+00:00")
    offset_pos = max(text.rfind("+"), text.rfind("-"))
    if offset_pos > 10 and len(text) - offset_pos == 3 and text[offset_pos + 1 :].isdigit():
        text += ":00"
    try:
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def chunked(items: Iterable[str], size: int) -> Iterable[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(str(item))
        if len(batch) >= int(size):
            yield batch
            batch = []
    if batch:
        yield batch
