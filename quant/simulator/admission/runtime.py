"""Runtime construction for the shared admission authority."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Mapping

from quant.core.db import postgres_connection

from .client import PolymarketGeoblockClient
from .service import UnifiedAdmissionService
from .store import PostgresAdmissionStore


@dataclass(frozen=True)
class AdmissionRuntime:
    service: UnifiedAdmissionService | None
    shadow: bool
    enforce: bool


def build_admission_runtime(
    *,
    connection_factory: Any = postgres_connection,
    environ: Mapping[str, str] | None = None,
    mode: str | None = None,
    proxy_url: str | None = None,
    ensure_schema: bool = False,
) -> AdmissionRuntime:
    env = os.environ if environ is None else environ
    selected_mode = str(
        mode if mode is not None else env.get("PAPER_UNIFIED_ADMISSION_MODE", "OFF")
    ).strip().upper()
    if selected_mode not in {"OFF", "SHADOW", "ENFORCE"}:
        raise ValueError("PAPER_UNIFIED_ADMISSION_MODE must be OFF, SHADOW, or ENFORCE")
    if selected_mode == "OFF":
        return AdmissionRuntime(service=None, shadow=False, enforce=False)
    store = PostgresAdmissionStore(connection_factory)
    if ensure_schema:
        store.ensure_schema()
    provider = PolymarketGeoblockClient(
        proxy_url=(
            proxy_url
            if proxy_url is not None
            else str(env.get("PAPER_GEOBLOCK_PROXY_URL") or "") or None
        ),
        timeout_seconds=float(env.get("PAPER_GEOBLOCK_TIMEOUT_SECONDS", "5")),
        ttl_seconds=float(env.get("PAPER_GEOBLOCK_TTL_SECONDS", "60")),
    )
    return AdmissionRuntime(
        service=UnifiedAdmissionService(store=store, geoblock_provider=provider),
        shadow=True,
        enforce=selected_mode == "ENFORCE",
    )
