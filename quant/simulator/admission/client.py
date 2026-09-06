"""External geoblock adapter with explicit process-local routing."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from .domain import GeoblockSnapshot


GEOBLOCK_URL = "https://polymarket.com/api/geoblock"


class GeoblockUnavailable(RuntimeError):
    pass


class PolymarketGeoblockClient:
    """Fetch the route truth without reading global proxy environment variables."""

    def __init__(
        self,
        *,
        proxy_url: str | None = None,
        timeout_seconds: float = 5.0,
        ttl_seconds: float = 60.0,
        endpoint: str = GEOBLOCK_URL,
    ) -> None:
        self.proxy_url = str(proxy_url or "").strip() or None
        self.timeout_seconds = max(0.1, float(timeout_seconds))
        self.ttl_seconds = max(1.0, float(ttl_seconds))
        self.endpoint = str(endpoint)
        self._cached: GeoblockSnapshot | None = None

    def snapshot(self, *, now: datetime | None = None) -> GeoblockSnapshot:
        observed = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if self._cached is not None and self._cached.is_fresh(observed):
            return self._cached
        try:
            with httpx.Client(
                proxy=self.proxy_url,
                timeout=self.timeout_seconds,
                trust_env=False,
            ) as client:
                response = client.get(self.endpoint)
                response.raise_for_status()
                payload = response.json()
        except Exception as exc:
            raise GeoblockUnavailable(
                f"geoblock status unavailable via {self.proxy_url or 'direct'}"
            ) from exc
        if not isinstance(payload, Mapping) or not isinstance(payload.get("blocked"), bool):
            raise GeoblockUnavailable("geoblock response is invalid")
        raw_payload = json.dumps(
            dict(payload), sort_keys=True, separators=(",", ":"), default=str
        )
        snapshot = GeoblockSnapshot(
            blocked=bool(payload["blocked"]),
            country=str(payload.get("country") or ""),
            region=str(payload.get("region") or ""),
            detected_ip=str(payload.get("ip") or ""),
            observed_at=observed,
            expires_at=observed + timedelta(seconds=self.ttl_seconds),
            raw_payload_hash=hashlib.sha256(raw_payload.encode("utf-8")).hexdigest(),
            proxy_url=self.proxy_url,
        )
        self._cached = snapshot
        return snapshot

    def clear_cache(self) -> None:
        self._cached = None
