"""Normalize Polymarket market payloads into registry domain models."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping

from .models import NormalizedMarket, NormalizedMarketToken


def normalize_market_payload(
    raw: dict[str, Any],
    source: str,
) -> tuple[NormalizedMarket, list[NormalizedMarketToken], list[str]]:
    warnings: list[str] = []
    market_id = _text(raw.get("id") or raw.get("market_id") or raw.get("conditionId") or raw.get("condition_id"))
    condition_id = _text(raw.get("conditionId") or raw.get("condition_id") or raw.get("condition_id_hex"))
    token_ids = _json_list(raw.get("clobTokenIds") or raw.get("clob_token_ids") or raw.get("token_ids"))
    outcomes = _json_list(raw.get("outcomes") or raw.get("outcomeNames") or raw.get("outcome_names"))
    if not market_id:
        market_id = condition_id or _text(raw.get("slug")) or ""
        warnings.append("missing_market_id")
    if len(token_ids) != len(outcomes):
        warnings.append("token_outcome_count_mismatch")
    metadata_hash = _metadata_hash(raw)
    market = NormalizedMarket(
        market_id=market_id,
        condition_id=condition_id,
        question_id=_text(raw.get("questionID") or raw.get("questionId") or raw.get("question_id")),
        event_id=_text(raw.get("eventId") or raw.get("event_id")),
        slug=_text(raw.get("slug")),
        question=_text(raw.get("question") or raw.get("title") or raw.get("name")),
        active=_bool_or_none(raw.get("active")),
        closed=_bool_or_none(raw.get("closed")),
        archived=_bool_or_none(raw.get("archived")),
        accepting_orders=_bool_or_none(raw.get("accepting_orders") or raw.get("acceptingOrders")),
        enable_order_book=_bool_or_none(raw.get("enableOrderBook") or raw.get("enable_order_book")),
        is_resolved=_bool_or_none(raw.get("isResolved") or raw.get("resolved") or raw.get("is_resolved")),
        resolution_status=_text(raw.get("resolutionStatus") or raw.get("resolution_status")),
        current_tick_size=_decimal(raw.get("orderPriceMinTickSize") or raw.get("tickSize") or raw.get("current_tick_size")),
        minimum_tick_size=_decimal(raw.get("minimum_tick_size") or raw.get("minimumTickSize")),
        min_order_size=_decimal(raw.get("minOrderSize") or raw.get("minimumOrderSize") or raw.get("min_order_size")),
        neg_risk=_bool_or_none(raw.get("negRisk") or raw.get("neg_risk")),
        end_date=_datetime(raw.get("endDate") or raw.get("end_date")),
        game_start_time=_datetime(raw.get("gameStartTime") or raw.get("game_start_time")),
        winning_asset_id=_text(raw.get("winningAssetId") or raw.get("winning_asset_id")),
        winning_outcome=_text(raw.get("winningOutcome") or raw.get("winning_outcome") or raw.get("resolutionOutcome")),
        raw={**raw, "_registry_source": source},
        metadata_hash=metadata_hash,
    )
    tokens: list[NormalizedMarketToken] = []
    winning_asset_id = market.winning_asset_id
    winning_outcome = (market.winning_outcome or "").strip().upper()
    for index, token_id in enumerate(token_ids):
        asset_id = _text(token_id)
        if not asset_id:
            warnings.append("empty_asset_id")
            continue
        outcome = _text(outcomes[index] if index < len(outcomes) else None)
        upper = (outcome or "").strip().upper()
        tokens.append(
            NormalizedMarketToken(
                market_id=market.market_id,
                condition_id=condition_id,
                asset_id=asset_id,
                outcome_name=outcome,
                outcome_index=index,
                is_yes=True if upper == "YES" else (False if upper else None),
                is_no=True if upper == "NO" else (False if upper else None),
                is_winning=True if (winning_asset_id and asset_id == winning_asset_id) or (winning_outcome and upper == winning_outcome) else None,
                clob_enabled=_bool_or_none(raw.get("enableOrderBook") or raw.get("enable_order_book")),
                current_tick_size=market.current_tick_size,
                min_order_size=market.min_order_size,
                raw={"asset_id": asset_id, "outcome": outcome, "source": source},
            )
        )
    return market, tokens, warnings


def _metadata_hash(raw: Mapping[str, Any]) -> str:
    encoded = json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
