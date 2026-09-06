"""CLOB book readiness probe facade."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Iterable

from .api_client import PolymarketApiClient, book_probe_from_payload
from .enums import BookQuality
from .models import RegistryBookProbeResult


class ClobBookProbe:
    def __init__(
        self,
        client: PolymarketApiClient,
        *,
        pending_asset_loader: Callable[[int], Iterable[str]] | None = None,
    ) -> None:
        self.client = client
        self.pending_asset_loader = pending_asset_loader

    async def probe_asset(self, asset_id: str) -> RegistryBookProbeResult:
        probe = self.client.probe_book(asset_id)
        return _to_registry_result(probe)

    async def probe_assets(self, asset_ids: Iterable[str], *, batch_size: int = 50) -> list[RegistryBookProbeResult]:
        probes = self.client.probe_books(asset_ids, batch_size=batch_size)
        return [_to_registry_result(probe) for probe in probes.values()]

    async def probe_pending_assets(self, limit: int = 100) -> list[RegistryBookProbeResult]:
        if self.pending_asset_loader is None:
            raise RuntimeError("probe_pending_assets requires a pending_asset_loader")
        return await self.probe_assets(self.pending_asset_loader(int(limit)), batch_size=max(1, int(limit) or 1))


def validate_book_payload(payload: dict) -> tuple[bool, str | None]:
    required = {"market", "asset_id", "timestamp", "hash", "bids", "asks", "min_order_size", "tick_size"}
    missing = sorted(key for key in required if key not in payload)
    if missing:
        return False, f"INVALID_SCHEMA: missing {','.join(missing)}"
    if not _valid_levels(payload.get("bids") or []):
        return False, "INVALID_SCHEMA: invalid_bids"
    if not _valid_levels(payload.get("asks") or []):
        return False, "INVALID_SCHEMA: invalid_asks"
    return True, None


def parse_book_payload(asset_id: str, payload: dict) -> RegistryBookProbeResult:
    ok, error = validate_book_payload(payload)
    probe = book_probe_from_payload(asset_id, payload, observed_at=datetime.now(timezone.utc))
    result = _to_registry_result(probe)
    if ok:
        return result
    return RegistryBookProbeResult(
        asset_id=asset_id,
        market=str(payload.get("market") or "") or None,
        ok=False,
        book_quality=BookQuality.PROBE_ERROR,
        timestamp=result.timestamp,
        book_hash=str(payload.get("hash") or "") or None,
        min_order_size=_decimal(payload.get("min_order_size") or payload.get("minOrderSize")),
        tick_size=_decimal(payload.get("tick_size") or payload.get("tickSize")),
        best_bid=result.best_bid,
        best_ask=result.best_ask,
        error_code=error,
        raw=payload,
    )


def _to_registry_result(probe: object) -> RegistryBookProbeResult:
    quality = _book_quality(getattr(probe, "book_quality", None), getattr(probe, "book_status", None))
    payload = getattr(probe, "payload", None) or {}
    return RegistryBookProbeResult(
        asset_id=str(getattr(probe, "asset_id", "")),
        market=str(payload.get("market") or "") or None,
        ok=bool(getattr(probe, "ok", False)),
        book_quality=quality,
        timestamp=getattr(probe, "observed_at", None),
        book_hash=str(payload.get("hash") or "") or None,
        min_order_size=_decimal(payload.get("min_order_size") or payload.get("minOrderSize")),
        tick_size=_decimal(payload.get("tick_size") or payload.get("tickSize")),
        best_bid=getattr(probe, "best_bid", None),
        best_ask=getattr(probe, "best_ask", None),
        error_code=getattr(probe, "error", None),
        raw=payload or None,
    )


def _book_quality(value: object, status: object) -> BookQuality:
    text = str(value or status or "").upper()
    mapping: dict[str, BookQuality] = {
        "READY_MEDIUM": BookQuality.READY_MEDIUM,
        "READY_HIGH": BookQuality.READY_HIGH,
        "STALE": BookQuality.STALE,
        "GAP": BookQuality.GAP,
        "DISCONNECTED": BookQuality.DISCONNECTED,
        "NO_CLOB_BOOK": BookQuality.NO_CLOB_BOOK,
        "EMPTY": BookQuality.EMPTY,
        "ONE_SIDED": BookQuality.ONE_SIDED,
    }
    if text in mapping:
        return mapping[text]
    if text in {"ERROR", "PROBE_ERROR"}:
        return BookQuality.PROBE_ERROR
    return BookQuality.NOT_CHECKED


def _valid_levels(levels: object) -> bool:
    if not isinstance(levels, list):
        return False
    for level in levels:
        value = level.get("price") if isinstance(level, dict) else (level[0] if isinstance(level, (list, tuple)) and level else None)
        price = _decimal(value)
        if price is None:
            return False
    return True


def _decimal(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None
