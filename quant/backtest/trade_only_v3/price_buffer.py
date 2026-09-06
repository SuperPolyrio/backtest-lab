from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any

from .hierarchical_prior import HierarchicalContext


@lru_cache(maxsize=8)
def load_price_buffer_profile(path: str | Path) -> dict[str, Any]:
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = Path(__file__).resolve().parents[3] / resolved
    return json.loads(resolved.read_text(encoding="utf-8"))


def resolve_price_buffer(
    path: str | None,
    *,
    context: HierarchicalContext,
    fallback: Decimal,
) -> dict[str, Any]:
    if not path:
        return _fallback(fallback, "PROFILE_NOT_CONFIGURED")
    try:
        profile = load_price_buffer_profile(path)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return _fallback(fallback, "PROFILE_UNAVAILABLE")
    status = str(profile.get("status") or "UNKNOWN")
    if status != "READY":
        return _fallback(fallback, status)
    raw_levels = profile.get("levels")
    levels: list[Any] = raw_levels if isinstance(raw_levels, list) else []
    for level in reversed(levels):
        if not isinstance(level, Mapping):
            continue
        fields = tuple(str(value) for value in level.get("fields", ()))
        cells = level.get("cells") if isinstance(level.get("cells"), Mapping) else {}
        key = "|".join(context.value(field) for field in fields)
        cell = cells.get(key) if isinstance(cells, Mapping) else None
        if isinstance(cell, Mapping) and cell.get("buffer") is not None:
            return {
                "buffer": str(cell["buffer"]),
                "status": status,
                "level": "+".join(fields),
                "cell_key": key,
                "samples": int(cell.get("samples") or 0),
                "fallback_used": False,
            }
    global_cell = profile.get("global")
    if isinstance(global_cell, Mapping) and global_cell.get("buffer") is not None:
        return {
            "buffer": str(global_cell["buffer"]),
            "status": status,
            "level": "global",
            "cell_key": "global",
            "samples": int(global_cell.get("samples") or 0),
            "fallback_used": False,
        }
    return _fallback(fallback, "READY_WITHOUT_MATCHING_CELL")


def _fallback(value: Decimal, status: str) -> dict[str, Any]:
    return {
        "buffer": str(value),
        "status": status,
        "level": "rule_fallback",
        "cell_key": "fallback",
        "samples": 0,
        "fallback_used": True,
    }
