"""Polymarket account cashflow replay helpers.

This module is intentionally account-flow oriented. It complements the
strategy execution ledger by replaying the event types that Polymarket account
activity exposes directly: BUY, SELL, SPLIT, MERGE, REDEEM and REBATE.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Mapping


Q = Decimal("0.0000000001")

CREDIT_EVENTS = {"SELL", "REDEEM", "MERGE", "REBATE", "MAKER_REBATE", "SETTLEMENT", "REFUND"}
DEBIT_EVENTS = {"BUY", "SPLIT"}
EXCLUDED_EVENTS = {"REWARD", "REFERRAL", "REFERRAL_REWARD", "CONVERSION"}
SUPPORTED_EVENTS = CREDIT_EVENTS | DEBIT_EVENTS | EXCLUDED_EVENTS


@dataclass
class _PositionState:
    quantity: Decimal = Decimal("0")
    cost_basis: Decimal = Decimal("0")


def replay_polymarket_cashflows(
    events: Iterable[Mapping[str, Any]],
    *,
    initial_capital: Decimal = Decimal("0"),
    mark_prices: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay Polymarket activity rows into a cashflow ledger and summary.

    Net PnL follows the account-activity formula:

    ``SELL + REDEEM + MERGE + REBATE - BUY - SPLIT + residual marked value``.

    Platform-exogenous activity such as rewards, referrals and conversions is
    preserved as excluded evidence but does not enter trading PnL.
    """

    cash = Decimal(str(initial_capital)).quantize(Q, rounding=ROUND_HALF_UP)
    positions: dict[str, _PositionState] = {}
    ledger: list[dict[str, Any]] = []
    cashflow_total = Decimal("0")
    debit_total = Decimal("0")
    credit_total = Decimal("0")
    excluded_total = Decimal("0")

    for index, raw in enumerate(events, start=1):
        row = normalize_polymarket_activity_event(raw)
        event_type = row["event_type"]
        key = row["position_key"]
        amount = row["amount"]
        shares_delta = row["shares_delta"]
        excluded = bool(row["excluded"])
        cash_delta = Decimal("0") if excluded else _cash_delta_for(event_type, amount)
        position_legs = _position_legs(raw, row)

        if not excluded:
            cash += cash_delta
            cashflow_total += cash_delta
            if cash_delta < 0:
                debit_total += -cash_delta
            else:
                credit_total += cash_delta
            _apply_position_legs(positions, position_legs, event_type, amount)
        else:
            excluded_total += amount
        primary_position = positions.setdefault(key, _PositionState())

        ledger.append(
            {
                "ledger_id": f"PM-CF-{index:04d}",
                "event_id": row["event_id"],
                "event_type": event_type,
                "market_slug": row["market_slug"],
                "event_slug": row["event_slug"],
                "token_id": row["token_id"],
                "token_side": row["token_side"],
                "position_key": key,
                "x_value": row["x_value"],
                "cash_delta": cash_delta.quantize(Q, rounding=ROUND_HALF_UP),
                "shares_delta": shares_delta.quantize(Q, rounding=ROUND_HALF_UP),
                "position_after": primary_position.quantity.quantize(Q, rounding=ROUND_HALF_UP),
                "cost_basis_after": primary_position.cost_basis.quantize(Q, rounding=ROUND_HALF_UP),
                "cash_after": cash.quantize(Q, rounding=ROUND_HALF_UP),
                "amount": amount,
                "excluded_from_trading_pnl": excluded,
                "source": row["source"],
                "meta": {
                    "raw_event_type": row["raw_event_type"],
                    "amount_source": row["amount_source"],
                    "position_legs": [
                        {
                            "position_key": leg["position_key"],
                            "token_id": leg["token_id"],
                            "token_side": leg["token_side"],
                            "shares_delta": leg["shares_delta"].quantize(Q, rounding=ROUND_HALF_UP),
                            "cost_basis_delta": leg["cost_basis_delta"].quantize(Q, rounding=ROUND_HALF_UP),
                        }
                        for leg in position_legs
                    ],
                },
            }
        )

    residuals = _residual_positions(positions, mark_prices or {})
    unrealized_value = sum((row["mark_value"] for row in residuals), Decimal("0")).quantize(Q, rounding=ROUND_HALF_UP)
    residual_cost_basis = sum((row["cost_basis"] for row in residuals), Decimal("0")).quantize(Q, rounding=ROUND_HALF_UP)
    portfolio_cash_at_risk = residual_cost_basis
    event_risk: dict[str, Decimal] = {}
    for row in residuals:
        event_key = row["event_slug"] or row["market_slug"] or row["position_key"]
        event_risk[event_key] = event_risk.get(event_key, Decimal("0")) + row["cost_basis"]
    net_trading_pnl = (cashflow_total + unrealized_value).quantize(Q, rounding=ROUND_HALF_UP)

    return {
        "ledger": ledger,
        "summary": {
            "schema_version": "polymarket_account_cashflow_v1",
            "event_count": len(ledger),
            "credit_total": credit_total.quantize(Q, rounding=ROUND_HALF_UP),
            "debit_total": debit_total.quantize(Q, rounding=ROUND_HALF_UP),
            "cashflow_total": cashflow_total.quantize(Q, rounding=ROUND_HALF_UP),
            "unrealized_position_value": unrealized_value,
            "residual_position_count": len(residuals),
            "residual_cost_basis": residual_cost_basis,
            "portfolio_cash_at_risk": portfolio_cash_at_risk,
            "event_cash_at_risk": {
                key: value.quantize(Q, rounding=ROUND_HALF_UP)
                for key, value in sorted(event_risk.items())
            },
            "net_trading_pnl": net_trading_pnl,
            "cash_balance": cash.quantize(Q, rounding=ROUND_HALF_UP),
            "excluded_total": excluded_total.quantize(Q, rounding=ROUND_HALF_UP),
        },
        "residual_positions": residuals,
    }


def normalize_polymarket_activity_event(row: Mapping[str, Any]) -> dict[str, Any]:
    event_type = _normalize_event_type(_first(row, "event_type", "eventType", "type", "activity_type", "activityType"))
    trade_side = _trade_side(row)
    if event_type in {"TRADE", "ORDER", "FILL"} and trade_side in {"BUY", "SELL"}:
        event_type = trade_side
    if event_type not in SUPPORTED_EVENTS:
        event_type = trade_side if trade_side in {"BUY", "SELL"} else event_type
    shares = _decimal(_first(row, "shares_delta", "sharesDelta", "size", "quantity", "outcome_size", "outcomeSize"))
    amount, amount_source = _amount_for_event(row, event_type, shares)
    shares_delta = _shares_delta_for(event_type, shares)
    token_side = _token_side(row)
    return {
        "event_id": str(_first(row, "event_id", "eventId", "id", "tx_hash", "txHash") or ""),
        "raw_event_type": str(_first(row, "event_type", "eventType", "type", "activity_type", "activityType") or ""),
        "event_type": event_type,
        "source": str(_first(row, "source") or "polymarket_activity"),
        "event_slug": str(_first(row, "event_slug", "eventSlug") or ""),
        "market_slug": str(_first(row, "market_slug", "marketSlug", "slug") or ""),
        "token_id": str(_first(row, "token_id", "tokenId", "asset_id", "assetId") or ""),
        "token_side": token_side,
        "position_key": _position_key(row, token_side=token_side),
        "x_value": _first(row, "block_number", "blockNumber", "timestamp", "time", "created_at", "createdAt"),
        "amount": amount.quantize(Q, rounding=ROUND_HALF_UP),
        "amount_source": amount_source,
        "shares_delta": shares_delta.quantize(Q, rounding=ROUND_HALF_UP),
        "excluded": event_type in EXCLUDED_EVENTS,
    }


def _cash_delta_for(event_type: str, amount: Decimal) -> Decimal:
    if event_type in DEBIT_EVENTS:
        return -amount
    if event_type in CREDIT_EVENTS:
        return amount
    return Decimal("0")


def _amount_for_event(row: Mapping[str, Any], event_type: str, shares: Decimal) -> tuple[Decimal, str]:
    explicit = _first(
        row,
        "amount",
        "cash_delta",
        "cashDelta",
        "usdc_amount",
        "usdcAmount",
        "value",
        "notional",
        "price_paid",
        "pricePaid",
    )
    if explicit not in (None, ""):
        return abs(_decimal(explicit)), "explicit"
    unit_price = _cashflow_unit_price(row, event_type)
    if unit_price is None or shares == 0:
        return Decimal("0"), "missing"
    return abs(shares * unit_price), "shares_x_unit_price"


def _cashflow_unit_price(row: Mapping[str, Any], event_type: str) -> Decimal | None:
    explicit = _first(
        row,
        "payout",
        "payout_price",
        "payoutPrice",
        "settlement_value",
        "settlementValue",
        "redeem_price",
        "redeemPrice",
        "price",
        "avg_price",
        "avgPrice",
    )
    if explicit not in (None, ""):
        return abs(_decimal(explicit))
    if event_type in {"REDEEM", "MERGE", "SPLIT", "SETTLEMENT", "REFUND"}:
        return Decimal("1")
    return None


def _apply_position_event(position: _PositionState, event_type: str, amount: Decimal, shares_delta: Decimal) -> None:
    if shares_delta > 0:
        position.quantity += shares_delta
        if event_type in DEBIT_EVENTS:
            position.cost_basis += amount
        return
    if shares_delta < 0:
        closing = min(position.quantity, abs(shares_delta)) if position.quantity > 0 else abs(shares_delta)
        avg_cost = position.cost_basis / position.quantity if position.quantity > 0 else Decimal("0")
        position.quantity += shares_delta
        position.cost_basis = max(Decimal("0"), position.cost_basis - avg_cost * closing)


def _apply_position_legs(
    positions: dict[str, _PositionState],
    legs: list[dict[str, Any]],
    event_type: str,
    amount: Decimal,
) -> None:
    if not legs:
        return
    if event_type == "SPLIT":
        for leg in legs:
            position = positions.setdefault(str(leg["position_key"]), _PositionState())
            position.quantity += Decimal(str(leg["shares_delta"]))
            position.cost_basis += Decimal(str(leg["cost_basis_delta"]))
        return
    if event_type == "MERGE":
        for leg in legs:
            position = positions.setdefault(str(leg["position_key"]), _PositionState())
            shares_delta = Decimal(str(leg["shares_delta"]))
            if shares_delta < 0:
                closing = min(position.quantity, abs(shares_delta)) if position.quantity > 0 else abs(shares_delta)
                avg_cost = position.cost_basis / position.quantity if position.quantity > 0 else Decimal("0")
                position.quantity += shares_delta
                position.cost_basis = max(Decimal("0"), position.cost_basis - avg_cost * closing)
            else:
                position.quantity += shares_delta
        return
    leg = legs[0]
    position = positions.setdefault(str(leg["position_key"]), _PositionState())
    _apply_position_event(position, event_type, amount, Decimal(str(leg["shares_delta"])))


def _position_legs(raw: Mapping[str, Any], row: Mapping[str, Any]) -> list[dict[str, Any]]:
    event_type = str(row["event_type"])
    amount = Decimal(str(row["amount"]))
    shares_delta = Decimal(str(row["shares_delta"]))
    leg_specs = _complete_set_leg_specs(raw, row)
    if event_type in {"SPLIT", "MERGE"} and leg_specs:
        per_leg_cost = (amount / Decimal(len(leg_specs))).quantize(Q, rounding=ROUND_HALF_UP)
        signed_shares = abs(shares_delta)
        if event_type == "MERGE":
            signed_shares = -signed_shares
            per_leg_cost = Decimal("0").quantize(Q)
        return [
            {
                **leg,
                "shares_delta": signed_shares.quantize(Q, rounding=ROUND_HALF_UP),
                "cost_basis_delta": per_leg_cost,
            }
            for leg in leg_specs
        ]
    return [
        {
            "position_key": str(row["position_key"]),
            "token_id": str(row["token_id"]),
            "token_side": str(row["token_side"]),
            "shares_delta": shares_delta.quantize(Q, rounding=ROUND_HALF_UP),
            "cost_basis_delta": amount.quantize(Q, rounding=ROUND_HALF_UP) if event_type in DEBIT_EVENTS else Decimal("0").quantize(Q),
        }
    ]


def _complete_set_leg_specs(raw: Mapping[str, Any], row: Mapping[str, Any]) -> list[dict[str, str]]:
    specs = _explicit_position_legs(raw, row)
    if specs:
        return specs
    yes_token = _first(raw, "yes_token_id", "yesTokenId", "yes_asset_id", "yesAssetId")
    no_token = _first(raw, "no_token_id", "noTokenId", "no_asset_id", "noAssetId")
    if yes_token and no_token:
        return [
            _leg_spec(row, str(yes_token), "YES"),
            _leg_spec(row, str(no_token), "NO"),
        ]
    complement_token = _first(raw, "complement_token_id", "complementTokenId", "complementary_token_id", "complementaryTokenId")
    token_id = str(row["token_id"] or "")
    token_side = str(row["token_side"] or "")
    if token_id and complement_token:
        complement_side = "NO" if token_side.upper() == "YES" else "YES" if token_side.upper() == "NO" else ""
        return [
            _leg_spec(row, token_id, token_side),
            _leg_spec(row, str(complement_token), complement_side),
        ]
    return []


def _explicit_position_legs(raw: Mapping[str, Any], row: Mapping[str, Any]) -> list[dict[str, str]]:
    value = _first(raw, "position_legs", "positionLegs", "legs", "tokens", "outcome_tokens", "outcomeTokens")
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, Mapping)):
        return []
    specs: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        token_id = str(_first(item, "token_id", "tokenId", "asset_id", "assetId") or "")
        if not token_id:
            continue
        token_side = _normalize_token_side(_first(item, "token_side", "tokenSide", "outcome", "side"))
        specs.append(_leg_spec(row, token_id, token_side))
    return specs


def _leg_spec(row: Mapping[str, Any], token_id: str, token_side: str) -> dict[str, str]:
    market_slug = str(row["market_slug"])
    event_slug = str(row["event_slug"])
    normalized_side = _normalize_token_side(token_side)
    return {
        "position_key": "|".join((market_slug, event_slug, str(token_id), normalized_side)),
        "token_id": str(token_id),
        "token_side": normalized_side,
    }


def _residual_positions(positions: Mapping[str, _PositionState], mark_prices: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, state in sorted(positions.items()):
        quantity = state.quantity.quantize(Q, rounding=ROUND_HALF_UP)
        if quantity == 0:
            continue
        mark_price = _decimal(mark_prices.get(key))
        mark_value = (quantity * mark_price).quantize(Q, rounding=ROUND_HALF_UP)
        rows.append(
            {
                "position_key": key,
                "market_slug": _key_part(key, 0),
                "event_slug": _key_part(key, 1),
                "token_id": _key_part(key, 2),
                "token_side": _key_part(key, 3),
                "quantity": quantity,
                "cost_basis": state.cost_basis.quantize(Q, rounding=ROUND_HALF_UP),
                "mark_price": mark_price.quantize(Q, rounding=ROUND_HALF_UP),
                "mark_value": mark_value,
            }
        )
    return rows


def _position_key(row: Mapping[str, Any], *, token_side: str | None = None) -> str:
    return "|".join(
        (
            str(_first(row, "market_slug", "marketSlug", "slug") or ""),
            str(_first(row, "event_slug", "eventSlug") or ""),
            str(_first(row, "token_id", "tokenId", "asset_id", "assetId") or ""),
            str(token_side if token_side is not None else _token_side(row)),
        )
    )


def _trade_side(row: Mapping[str, Any]) -> str:
    """Return the BUY/SELL trade direction without treating outcome labels as side."""

    side = _normalize_event_type(_first(row, "trade_side", "tradeSide", "side"))
    return side if side in {"BUY", "SELL"} else ""


def _token_side(row: Mapping[str, Any]) -> str:
    """Return the outcome/token side while avoiding BUY/SELL trade-direction leakage."""

    direct = _first(
        row,
        "token_side",
        "tokenSide",
        "outcome",
        "outcome_name",
        "outcomeName",
        "asset_outcome",
        "assetOutcome",
    )
    normalized = _normalize_token_side(direct)
    if normalized:
        return normalized
    side = _first(row, "side")
    normalized_side = _normalize_token_side(side)
    if normalized_side and normalized_side not in {"BUY", "SELL"}:
        return normalized_side
    return ""


def _normalize_token_side(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = text.upper().replace("-", "_").replace(" ", "_")
    if normalized in {"YES", "NO", "BUY", "SELL"}:
        return normalized
    return text


def _key_part(key: str, index: int) -> str:
    parts = key.split("|")
    return parts[index] if index < len(parts) else ""


def _shares_delta_for(event_type: str, shares: Decimal) -> Decimal:
    shares = abs(shares)
    if event_type in {"BUY", "SPLIT"}:
        return shares
    if event_type in {"SELL", "REDEEM", "MERGE", "SETTLEMENT", "REFUND"}:
        return -shares
    return Decimal("0")


def _normalize_event_type(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "_").replace(" ", "_")


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))
