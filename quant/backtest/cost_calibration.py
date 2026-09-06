"""Real wallet/order cost calibration for fill-first backtests."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Any, Iterable, Mapping


REAL_COST_TABLE = "quant.real_backtest_cost_events"
COST_CALIBRATION_TABLE = "quant.quant_backtest_cost_calibration"
Q = Decimal("0.0000000001")

FEE_EVENTS = {"FEE", "TRADING_FEE", "MAKER_FEE", "TAKER_FEE"}
REBATE_EVENTS = {"REBATE", "MAKER_REBATE"}
EXTERNAL_COST_EVENTS = {"GAS_COST", "SETTLEMENT_COST", "REDEEM_COST", "CAPITAL_COST"}
SUPPORTED_EVENTS = FEE_EVENTS | REBATE_EVENTS | EXTERNAL_COST_EVENTS
EVENT_TYPE_ALIASES = {
    "GAS": "GAS_COST",
    "NETWORK_FEE": "GAS_COST",
    "TX_FEE": "GAS_COST",
    "SETTLEMENT": "SETTLEMENT_COST",
    "SETTLE_COST": "SETTLEMENT_COST",
    "REDEEM": "REDEEM_COST",
    "REDEMPTION": "REDEEM_COST",
    "CAPITAL": "CAPITAL_COST",
    "CAPITAL_OCCUPIED": "CAPITAL_COST",
}

ALIASES: dict[str, tuple[str, ...]] = {
    "run_id": ("run_id", "runId"),
    "cost_id": ("cost_id", "costId", "id", "event_id", "eventId"),
    "source": ("source",),
    "market_slug": ("market_slug", "marketSlug"),
    "token_id": ("token_id", "tokenId", "asset_id", "assetId"),
    "token_side": ("token_side", "tokenSide"),
    "order_id": ("order_id", "orderId", "client_order_id", "clientOrderId"),
    "trade_id": ("trade_id", "tradeId"),
    "event_type": ("event_type", "eventType", "type", "cost_type", "costType"),
    "observed_at": ("observed_at", "observedAt", "timestamp", "time", "created_at", "createdAt"),
    "observed_block": ("observed_block", "observedBlock", "block_number", "blockNumber"),
    "amount": ("amount", "cost", "fee", "rebate", "gas", "value"),
    "currency": ("currency", "asset", "token"),
    "tx_hash": ("tx_hash", "txHash", "transaction_hash", "transactionHash"),
    "payload": ("payload", "raw", "raw_payload", "rawPayload"),
}


def normalize_real_cost_event(row: Mapping[str, Any], *, source: str | None = None, run_id: int | None = None) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field, aliases in ALIASES.items():
        value = _first_value(row, aliases)
        if value not in (None, ""):
            normalized[field] = value
    if source:
        normalized["source"] = source
    if run_id is not None:
        normalized["run_id"] = run_id
    normalized.setdefault("source", "manual")
    normalized.setdefault("currency", "USDC")
    normalized["event_type"] = normalize_cost_event_type(normalized.get("event_type"))
    normalized["amount"] = abs(_decimal(normalized.get("amount"))).quantize(Q, rounding=ROUND_HALF_UP)
    normalized.setdefault("cost_id", _default_cost_id(normalized))
    payload = normalized.get("payload")
    normalized["payload"] = payload if isinstance(payload, dict) else dict(row)
    return normalized


def normalize_cost_event_type(value: Any) -> str:
    text = str(value or "COST").strip().upper().replace("-", "_").replace(" ", "_")
    if text in FEE_EVENTS:
        return "FEE"
    if text in REBATE_EVENTS:
        return "REBATE"
    if text in EVENT_TYPE_ALIASES:
        return EVENT_TYPE_ALIASES[text]
    return text


def build_cost_calibration_samples(
    ledger_rows: Iterable[Mapping[str, Any]],
    real_cost_events: Iterable[Mapping[str, Any]],
    *,
    source: str = "real-cost-events",
    run_id: int | None = None,
) -> list[dict[str, Any]]:
    simulated = _group_simulated_costs(ledger_rows)
    live = _group_live_costs(real_cost_events)
    keys = sorted(set(simulated) | set(live))
    samples: list[dict[str, Any]] = []
    for key in keys:
        sim = simulated.get(key, _empty_group(key))
        real = live.get(key, _empty_group(key))
        simulated_amount = sim["amount"].quantize(Q, rounding=ROUND_HALF_UP)
        live_amount = real["amount"].quantize(Q, rounding=ROUND_HALF_UP)
        amount_error = abs(simulated_amount - live_amount).quantize(Q, rounding=ROUND_HALF_UP)
        sample = {
            "run_id": run_id or sim.get("run_id") or real.get("run_id"),
            "sample_id": _sample_id(source, key),
            "source": source,
            "market_slug": sim.get("market_slug") or real.get("market_slug"),
            "token_id": sim.get("token_id") or real.get("token_id"),
            "token_side": sim.get("token_side") or real.get("token_side"),
            "order_id": sim.get("order_id") or real.get("order_id"),
            "trade_id": sim.get("trade_id") or real.get("trade_id"),
            "event_type": key[0],
            "simulated_amount": simulated_amount,
            "live_amount": live_amount,
            "amount_error": amount_error,
            "simulated_count": sim["count"],
            "live_count": real["count"],
            "observed_at": real.get("observed_at") or sim.get("observed_at"),
            "observed_block": real.get("observed_block") or sim.get("observed_block"),
            "verdict": _cost_verdict(simulated_amount, live_amount, sim["count"], real["count"]),
            "payload": {
                "match_key": {"event_type": key[0], "order_id": key[1], "trade_id": key[2]},
                "simulated_ids": sim["ids"],
                "live_ids": real["ids"],
            },
        }
        samples.append(sample)
    return samples


def build_cost_calibration_report(samples: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in samples]
    sample_count = len(rows)
    by_type: dict[str, dict[str, Any]] = {}
    total_error = Decimal("0")
    missing_live = 0
    missing_simulated = 0
    for row in rows:
        event_type = str(row.get("event_type") or "UNKNOWN")
        bucket = by_type.setdefault(event_type, {
            "sample_count": 0,
            "simulated_amount": Decimal("0"),
            "live_amount": Decimal("0"),
            "amount_error": Decimal("0"),
            "verdict_counts": {},
        })
        simulated_amount = _decimal(row.get("simulated_amount"))
        live_amount = _decimal(row.get("live_amount"))
        amount_error = _decimal(row.get("amount_error"))
        bucket["sample_count"] += 1
        bucket["simulated_amount"] += simulated_amount
        bucket["live_amount"] += live_amount
        bucket["amount_error"] += amount_error
        verdict = str(row.get("verdict") or "unknown")
        bucket["verdict_counts"][verdict] = bucket["verdict_counts"].get(verdict, 0) + 1
        total_error += amount_error
        if int(row.get("live_count") or 0) <= 0:
            missing_live += 1
        if int(row.get("simulated_count") or 0) <= 0:
            missing_simulated += 1
    return {
        "sample_count": sample_count,
        "total_amount_error": _decimal_text(total_error),
        "avg_amount_error": _decimal_text(total_error / Decimal(sample_count) if sample_count else Decimal("0")),
        "missing_live_count": missing_live,
        "missing_simulated_count": missing_simulated,
        "requires_recalibration": bool(total_error > Decimal("0") or missing_live or missing_simulated),
        "event_type_summary": {
            event_type: {
                "sample_count": bucket["sample_count"],
                "simulated_amount": _decimal_text(bucket["simulated_amount"]),
                "live_amount": _decimal_text(bucket["live_amount"]),
                "amount_error": _decimal_text(bucket["amount_error"]),
                "verdict_counts": bucket["verdict_counts"],
            }
            for event_type, bucket in sorted(by_type.items())
        },
    }


def empty_cost_calibration_report(reason: str = "no cost calibration samples") -> dict[str, Any]:
    return {
        "sample_count": 0,
        "total_amount_error": "0",
        "avg_amount_error": "0",
        "missing_live_count": 0,
        "missing_simulated_count": 0,
        "requires_recalibration": False,
        "event_type_summary": {},
        "trust_status": "unknown",
        "trust_reason": reason,
    }


def upsert_real_cost_events(conn: Any, events: Iterable[Mapping[str, Any]]) -> int:
    rows = [normalize_real_cost_event(event) for event in events]
    if not rows:
        return 0
    if not _table_exists(conn, REAL_COST_TABLE):
        raise RuntimeError(f"{REAL_COST_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO quant.real_backtest_cost_events (
                    run_id, cost_id, source, market_slug, token_id, token_side, order_id, trade_id,
                    event_type, observed_at, observed_block, amount, currency, tx_hash, payload
                )
                VALUES (
                    %(run_id)s, %(cost_id)s, %(source)s, %(market_slug)s, %(token_id)s, %(token_side)s,
                    %(order_id)s, %(trade_id)s, %(event_type)s, %(observed_at)s, %(observed_block)s,
                    %(amount)s, %(currency)s, %(tx_hash)s, %(payload)s::jsonb
                )
                ON CONFLICT (source, cost_id)
                DO UPDATE SET
                    run_id = COALESCE(EXCLUDED.run_id, quant.real_backtest_cost_events.run_id),
                    market_slug = COALESCE(EXCLUDED.market_slug, quant.real_backtest_cost_events.market_slug),
                    token_id = COALESCE(EXCLUDED.token_id, quant.real_backtest_cost_events.token_id),
                    token_side = COALESCE(EXCLUDED.token_side, quant.real_backtest_cost_events.token_side),
                    order_id = COALESCE(EXCLUDED.order_id, quant.real_backtest_cost_events.order_id),
                    trade_id = COALESCE(EXCLUDED.trade_id, quant.real_backtest_cost_events.trade_id),
                    event_type = EXCLUDED.event_type,
                    observed_at = COALESCE(EXCLUDED.observed_at, quant.real_backtest_cost_events.observed_at),
                    observed_block = COALESCE(EXCLUDED.observed_block, quant.real_backtest_cost_events.observed_block),
                    amount = EXCLUDED.amount,
                    currency = EXCLUDED.currency,
                    tx_hash = COALESCE(EXCLUDED.tx_hash, quant.real_backtest_cost_events.tx_hash),
                    payload = EXCLUDED.payload
                """,
                _cost_db_row(row),
            )
    return len(rows)


def upsert_cost_calibration_samples(conn: Any, samples: Iterable[Mapping[str, Any]]) -> int:
    rows = [dict(sample) for sample in samples]
    if not rows:
        return 0
    if not _table_exists(conn, COST_CALIBRATION_TABLE):
        raise RuntimeError(f"{COST_CALIBRATION_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO quant.quant_backtest_cost_calibration (
                    run_id, sample_id, source, market_slug, token_id, token_side, order_id, trade_id,
                    event_type, simulated_amount, live_amount, amount_error, simulated_count, live_count,
                    observed_at, observed_block, verdict, payload
                )
                VALUES (
                    %(run_id)s, %(sample_id)s, %(source)s, %(market_slug)s, %(token_id)s, %(token_side)s,
                    %(order_id)s, %(trade_id)s, %(event_type)s, %(simulated_amount)s, %(live_amount)s,
                    %(amount_error)s, %(simulated_count)s, %(live_count)s, %(observed_at)s,
                    %(observed_block)s, %(verdict)s, %(payload)s::jsonb
                )
                ON CONFLICT (source, sample_id)
                DO UPDATE SET
                    run_id = COALESCE(EXCLUDED.run_id, quant.quant_backtest_cost_calibration.run_id),
                    market_slug = COALESCE(EXCLUDED.market_slug, quant.quant_backtest_cost_calibration.market_slug),
                    token_id = COALESCE(EXCLUDED.token_id, quant.quant_backtest_cost_calibration.token_id),
                    token_side = COALESCE(EXCLUDED.token_side, quant.quant_backtest_cost_calibration.token_side),
                    order_id = COALESCE(EXCLUDED.order_id, quant.quant_backtest_cost_calibration.order_id),
                    trade_id = COALESCE(EXCLUDED.trade_id, quant.quant_backtest_cost_calibration.trade_id),
                    event_type = EXCLUDED.event_type,
                    simulated_amount = EXCLUDED.simulated_amount,
                    live_amount = EXCLUDED.live_amount,
                    amount_error = EXCLUDED.amount_error,
                    simulated_count = EXCLUDED.simulated_count,
                    live_count = EXCLUDED.live_count,
                    observed_at = COALESCE(EXCLUDED.observed_at, quant.quant_backtest_cost_calibration.observed_at),
                    observed_block = COALESCE(EXCLUDED.observed_block, quant.quant_backtest_cost_calibration.observed_block),
                    verdict = EXCLUDED.verdict,
                    payload = EXCLUDED.payload
                """,
                _calibration_db_row(row),
            )
    return len(rows)


def load_ledger_cost_rows_for_run(conn: Any, run_id: int) -> list[dict[str, Any]]:
    if not _table_exists(conn, "quant.quant_backtest_ledger"):
        return []
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM quant.quant_backtest_ledger
            WHERE run_id = %s
              AND (
                event_type IN ('GAS_COST', 'SETTLEMENT_COST', 'REDEEM_COST', 'CAPITAL_COST')
                OR COALESCE(fee, 0) <> 0
                OR COALESCE(rebate, 0) <> 0
              )
            ORDER BY x_value, ledger_id
            """,
            (run_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def load_real_cost_events_for_run(conn: Any, run_id: int, *, source: str | None = None) -> list[dict[str, Any]]:
    if not _table_exists(conn, REAL_COST_TABLE):
        return []
    params: list[Any] = [run_id]
    source_filter = ""
    if source:
        source_filter = "AND source = %s"
        params.append(source)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT *
            FROM quant.real_backtest_cost_events
            WHERE run_id = %s
            {source_filter}
            ORDER BY observed_at NULLS LAST, event_id
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]


def _group_simulated_costs(rows: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        for event_type, amount in _ledger_amounts(row):
            key = _match_key(event_type, row)
            group = groups.setdefault(key, _empty_group(key))
            _add_to_group(group, row, amount, _ledger_id(row), simulated=True)
    return groups


def _group_live_costs(events: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str, str], dict[str, Any]]:
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for raw in events:
        event = normalize_real_cost_event(raw)
        if event["event_type"] not in SUPPORTED_EVENTS:
            continue
        key = _match_key(event["event_type"], event)
        group = groups.setdefault(key, _empty_group(key))
        _add_to_group(group, event, event["amount"], str(event.get("cost_id") or ""), simulated=False)
    return groups


def _ledger_amounts(row: Mapping[str, Any]) -> list[tuple[str, Decimal]]:
    event_type = normalize_cost_event_type(row.get("event_type"))
    amounts: list[tuple[str, Decimal]] = []
    if event_type in EXTERNAL_COST_EVENTS:
        amounts.append((event_type, abs(_decimal(row.get("cash_delta")))))
    fee = abs(_decimal(row.get("fee")))
    rebate = abs(_decimal(row.get("rebate")))
    if fee > 0:
        amounts.append(("FEE", fee))
    if rebate > 0:
        amounts.append(("REBATE", rebate))
    return [(kind, amount.quantize(Q, rounding=ROUND_HALF_UP)) for kind, amount in amounts if amount > 0]


def _add_to_group(group: dict[str, Any], row: Mapping[str, Any], amount: Decimal, item_id: str, *, simulated: bool) -> None:
    group["amount"] += amount
    group["count"] += 1
    if item_id:
        group["ids"].append(item_id)
    for field in ("run_id", "market_slug", "token_id", "token_side", "order_id", "trade_id", "observed_at", "observed_block"):
        if group.get(field) in (None, "") and row.get(field) not in (None, ""):
            group[field] = row.get(field)
    if group.get("observed_at") in (None, ""):
        group["observed_at"] = row.get("created_at") or row.get("x_value")
    if simulated:
        group["run_id"] = row.get("run_id") or group.get("run_id")


def _empty_group(key: tuple[str, str, str]) -> dict[str, Any]:
    return {
        "event_type": key[0],
        "order_id": key[1] or None,
        "trade_id": key[2] or None,
        "amount": Decimal("0"),
        "count": 0,
        "ids": [],
    }


def _match_key(event_type: str, row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        normalize_cost_event_type(event_type),
        str(row.get("order_id") or ""),
        str(row.get("trade_id") or ""),
    )


def _cost_verdict(simulated_amount: Decimal, live_amount: Decimal, simulated_count: int, live_count: int) -> str:
    if simulated_count <= 0 and live_count > 0:
        return "missing_simulated"
    if live_count <= 0 and simulated_count > 0:
        return "missing_live"
    if simulated_amount == live_amount:
        return "matched"
    return "amount_mismatch"


def _sample_id(source: str, key: tuple[str, str, str]) -> str:
    return f"{source}:{key[0]}:{key[1] or '-'}:{key[2] or '-'}"


def _default_cost_id(row: Mapping[str, Any]) -> str:
    parts = [
        row.get("run_id"),
        row.get("event_type"),
        row.get("order_id"),
        row.get("trade_id"),
        row.get("observed_at"),
        row.get("observed_block"),
        row.get("amount"),
        row.get("tx_hash"),
    ]
    return "|".join(str(part or "") for part in parts).strip("|") or "manual-cost-event"


def _cost_db_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result = {field: row.get(field) for field in (
        "run_id",
        "cost_id",
        "source",
        "market_slug",
        "token_id",
        "token_side",
        "order_id",
        "trade_id",
        "event_type",
        "observed_at",
        "observed_block",
        "currency",
        "tx_hash",
    )}
    result["amount"] = _decimal(row.get("amount"))
    result["payload"] = json.dumps(row.get("payload") or {}, default=str)
    return result


def _calibration_db_row(row: Mapping[str, Any]) -> dict[str, Any]:
    result = {field: row.get(field) for field in (
        "run_id",
        "sample_id",
        "source",
        "market_slug",
        "token_id",
        "token_side",
        "order_id",
        "trade_id",
        "event_type",
        "simulated_count",
        "live_count",
        "observed_at",
        "observed_block",
        "verdict",
    )}
    for field in ("simulated_amount", "live_amount", "amount_error"):
        result[field] = _decimal(row.get(field))
    result["payload"] = json.dumps(row.get("payload") or {}, default=str)
    return result


def _table_exists(conn: Any, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (table,))
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


def _first_value(row: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _ledger_id(row: Mapping[str, Any]) -> str:
    return str(row.get("ledger_id") or row.get("event_id") or row.get("order_id") or row.get("trade_id") or "")


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value)).quantize(Q, rounding=ROUND_HALF_UP)


def _decimal_text(value: Decimal) -> str:
    text = format(value.quantize(Q, rounding=ROUND_HALF_UP), "f")
    text = text.rstrip("0").rstrip(".") if "." in text else text
    return text or "0"
