"""Backtest-vs-live fill calibration helpers."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Any, Iterable, Mapping


CALIBRATION_TABLE = "quant.quant_backtest_calibration_orders"
FILLED_STATUSES = {"FILLED", "PARTIAL_FILLED"}

ALIASES: dict[str, tuple[str, ...]] = {
    "run_id": ("run_id", "runId"),
    "sample_id": ("sample_id", "sampleId", "id"),
    "source": ("source",),
    "market_slug": ("market_slug", "marketSlug"),
    "token_id": ("token_id", "tokenId", "asset_id", "assetId"),
    "token_side": ("token_side", "tokenSide"),
    "side": ("side",),
    "role": ("role",),
    "order_type": ("order_type", "orderType"),
    "observed_at": ("observed_at", "observedAt", "timestamp", "time"),
    "observed_block": ("observed_block", "observedBlock", "block_number", "blockNumber"),
    "simulated_order_id": ("simulated_order_id", "simulatedOrderId", "sim_order_id", "simOrderId"),
    "live_order_id": ("live_order_id", "liveOrderId", "external_order_id", "externalOrderId", "order_id", "orderId"),
    "requested_price": ("requested_price", "requestedPrice", "limit_price", "limitPrice"),
    "requested_size": ("requested_size", "requestedSize", "size"),
    "simulated_status": ("simulated_status", "simulatedStatus", "sim_status", "simStatus"),
    "live_status": ("live_status", "liveStatus"),
    "simulated_fill_price": ("simulated_fill_price", "simulatedFillPrice", "sim_fill_price", "simFillPrice"),
    "live_fill_price": ("live_fill_price", "liveFillPrice"),
    "simulated_fill_size": ("simulated_fill_size", "simulatedFillSize", "sim_fill_size", "simFillSize"),
    "live_fill_size": ("live_fill_size", "liveFillSize"),
    "simulated_slippage": ("simulated_slippage", "simulatedSlippage", "sim_slippage", "simSlippage"),
    "live_slippage": ("live_slippage", "liveSlippage"),
    "simulated_fee": ("simulated_fee", "simulatedFee", "sim_fee", "simFee"),
    "live_fee": ("live_fee", "liveFee"),
    "simulated_rebate": ("simulated_rebate", "simulatedRebate", "sim_rebate", "simRebate"),
    "live_rebate": ("live_rebate", "liveRebate"),
    "simulated_cash_delta": ("simulated_cash_delta", "simulatedCashDelta", "sim_cash_delta", "simCashDelta"),
    "live_cash_delta": ("live_cash_delta", "liveCashDelta"),
    "simulated_position_delta": ("simulated_position_delta", "simulatedPositionDelta", "sim_position_delta", "simPositionDelta"),
    "live_position_delta": ("live_position_delta", "livePositionDelta"),
    "simulated_pnl": ("simulated_pnl", "simulatedPnl", "simulated_pnl_delta", "simulatedPnlDelta", "sim_pnl", "simPnl"),
    "live_pnl": ("live_pnl", "livePnl", "live_pnl_delta", "livePnlDelta"),
    "simulated_latency_seconds": ("simulated_latency_seconds", "simulatedLatencySeconds", "sim_latency_seconds", "simLatencySeconds"),
    "live_latency_seconds": ("live_latency_seconds", "liveLatencySeconds"),
    "liquidity_bucket": ("liquidity_bucket", "liquidityBucket"),
    "volatility_bucket": ("volatility_bucket", "volatilityBucket"),
    "time_to_expiry_bucket": ("time_to_expiry_bucket", "timeToExpiryBucket"),
    "payload": ("payload", "raw", "raw_payload", "rawPayload"),
}


def normalize_calibration_order(row: Mapping[str, Any], *, source: str | None = None, run_id: int | None = None) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for field, aliases in ALIASES.items():
        value = _first_value(row, aliases)
        if value not in (None, ""):
            normalized[field] = value
    nested_sim = row.get("simulated") if isinstance(row.get("simulated"), Mapping) else {}
    nested_live = row.get("live") if isinstance(row.get("live"), Mapping) else {}
    if nested_sim:
        _merge_nested(normalized, nested_sim, "simulated")
    if nested_live:
        _merge_nested(normalized, nested_live, "live")
    if source:
        normalized["source"] = source
    if run_id is not None:
        normalized["run_id"] = run_id
    normalized.setdefault("source", "manual")
    normalized.setdefault("sample_id", _default_sample_id(normalized))
    normalized["payload"] = normalized.get("payload") if isinstance(normalized.get("payload"), dict) else dict(row)
    return compare_calibration_order(normalized)


def compare_calibration_order(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    simulated_status = _status(result.get("simulated_status"))
    live_status = _status(result.get("live_status"))
    result["simulated_status"] = simulated_status
    result["live_status"] = live_status
    result["status_error"] = _status_filled(simulated_status) != _status_filled(live_status)
    result["price_error"] = _abs_diff(result.get("simulated_fill_price"), result.get("live_fill_price"))
    result["size_error"] = _abs_diff(result.get("simulated_fill_size"), result.get("live_fill_size"))
    result["slippage_error"] = _abs_diff(result.get("simulated_slippage"), result.get("live_slippage"))
    result["fee_error"] = _abs_diff(result.get("simulated_fee"), result.get("live_fee"))
    result["rebate_error"] = _abs_diff(result.get("simulated_rebate"), result.get("live_rebate"))
    result["cash_error"] = _abs_diff(result.get("simulated_cash_delta"), result.get("live_cash_delta"))
    result["position_error"] = _abs_diff(result.get("simulated_position_delta"), result.get("live_position_delta"))
    result["pnl_error"] = _abs_diff(result.get("simulated_pnl"), result.get("live_pnl"))
    result["latency_error_seconds"] = _abs_diff(result.get("simulated_latency_seconds"), result.get("live_latency_seconds"))
    result["verdict"] = _verdict(result)
    return result


def build_calibration_report(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    normalized = [compare_calibration_order(row) for row in rows]
    sample_count = len(normalized)
    status_errors = [row for row in normalized if bool(row.get("status_error"))]
    price_errors = [_decimal(row.get("price_error")) for row in normalized]
    size_errors = [_decimal(row.get("size_error")) for row in normalized]
    slippage_errors = [_decimal(row.get("slippage_error")) for row in normalized]
    fee_errors = [_decimal(row.get("fee_error")) for row in normalized]
    rebate_errors = [_decimal(row.get("rebate_error")) for row in normalized]
    cash_errors = [_decimal(row.get("cash_error")) for row in normalized]
    position_errors = [_decimal(row.get("position_error")) for row in normalized]
    pnl_errors = [_decimal(row.get("pnl_error")) for row in normalized]
    latency_errors = [_decimal(row.get("latency_error_seconds")) for row in normalized]
    report = {
        "sample_count": sample_count,
        "status_error_count": len(status_errors),
        "status_error_rate": _decimal_text(_ratio(len(status_errors), sample_count) * Decimal("100")),
        "avg_price_error": _decimal_text(_avg(price_errors)),
        "max_price_error": _decimal_text(max(price_errors) if price_errors else Decimal("0")),
        "avg_size_error": _decimal_text(_avg(size_errors)),
        "avg_slippage_error": _decimal_text(_avg(slippage_errors)),
        "avg_fee_error": _decimal_text(_avg(fee_errors)),
        "avg_rebate_error": _decimal_text(_avg(rebate_errors)),
        "avg_cash_error": _decimal_text(_avg(cash_errors)),
        "avg_position_error": _decimal_text(_avg(position_errors)),
        "avg_pnl_error": _decimal_text(_avg(pnl_errors)),
        "max_pnl_error": _decimal_text(max(pnl_errors) if pnl_errors else Decimal("0")),
        "avg_latency_error_seconds": _decimal_text(_avg(latency_errors)),
        "verdict_counts": _counts(normalized, "verdict"),
        "role_counts": _counts(normalized, "role"),
        "side_counts": _counts(normalized, "side"),
        "liquidity_bucket_counts": _counts(normalized, "liquidity_bucket"),
        "volatility_bucket_counts": _counts(normalized, "volatility_bucket"),
        "time_to_expiry_bucket_counts": _counts(normalized, "time_to_expiry_bucket"),
        "requires_recalibration": bool(status_errors)
        or _avg(price_errors) > Decimal("0.01")
        or _avg(slippage_errors) > Decimal("0.01")
        or _avg(pnl_errors) > Decimal("1")
        or _avg(latency_errors) > Decimal("2"),
    }
    report.update(calibration_trust(report))
    return report


def empty_calibration_report(*, reason: str = "no calibration samples") -> dict[str, Any]:
    report = {
        "sample_count": 0,
        "status_error_count": 0,
        "status_error_rate": "0",
        "avg_price_error": "0",
        "max_price_error": "0",
        "avg_size_error": "0",
        "avg_slippage_error": "0",
        "avg_fee_error": "0",
        "avg_rebate_error": "0",
        "avg_cash_error": "0",
        "avg_position_error": "0",
        "avg_pnl_error": "0",
        "max_pnl_error": "0",
        "avg_latency_error_seconds": "0",
        "verdict_counts": {},
        "role_counts": {},
        "side_counts": {},
        "liquidity_bucket_counts": {},
        "volatility_bucket_counts": {},
        "time_to_expiry_bucket_counts": {},
        "requires_recalibration": False,
    }
    report.update({"trust_status": "unknown", "trust_reason": reason})
    return report


def calibration_trust(report: Mapping[str, Any]) -> dict[str, Any]:
    sample_count = int(_decimal(report.get("sample_count")))
    if sample_count <= 0:
        return {"trust_status": "unknown", "trust_reason": "no calibration samples"}
    status_error_rate = _decimal(report.get("status_error_rate"))
    avg_price_error = _decimal(report.get("avg_price_error"))
    avg_slippage_error = _decimal(report.get("avg_slippage_error"))
    avg_pnl_error = _decimal(report.get("avg_pnl_error"))
    avg_latency_error = _decimal(report.get("avg_latency_error_seconds"))
    if bool(report.get("requires_recalibration")):
        reasons: list[str] = []
        if status_error_rate > Decimal("0"):
            reasons.append(f"status error {status_error_rate}%")
        if avg_price_error > Decimal("0.01"):
            reasons.append(f"avg price error {avg_price_error}")
        if avg_slippage_error > Decimal("0.01"):
            reasons.append(f"avg slippage error {avg_slippage_error}")
        if avg_pnl_error > Decimal("1"):
            reasons.append(f"avg pnl error {avg_pnl_error}")
        if avg_latency_error > Decimal("2"):
            reasons.append(f"avg latency error {avg_latency_error}s")
        return {"trust_status": "review", "trust_reason": "; ".join(reasons) or "requires recalibration"}
    return {"trust_status": "ready", "trust_reason": "calibration within fill-first thresholds"}


def upsert_calibration_orders(conn: Any, rows: Iterable[Mapping[str, Any]]) -> int:
    normalized = [normalize_calibration_order(row) for row in rows]
    if not normalized:
        return 0
    if not _table_exists(conn):
        raise RuntimeError(f"{CALIBRATION_TABLE} does not exist; run quant.core.schema.create_schema first")
    with conn.cursor() as cur:
        for row in normalized:
            cur.execute(
                """
                INSERT INTO quant.quant_backtest_calibration_orders (
                    run_id, sample_id, source, market_slug, token_id, token_side, side, role, order_type,
                    observed_at, observed_block, simulated_order_id, live_order_id,
                    requested_price, requested_size, simulated_status, live_status, status_error,
                    simulated_fill_price, live_fill_price, price_error,
                    simulated_fill_size, live_fill_size, size_error,
                    simulated_slippage, live_slippage, slippage_error,
                    simulated_fee, live_fee, fee_error,
                    simulated_rebate, live_rebate, rebate_error,
                    simulated_cash_delta, live_cash_delta, cash_error,
                    simulated_position_delta, live_position_delta, position_error,
                    simulated_pnl, live_pnl, pnl_error,
                    simulated_latency_seconds, live_latency_seconds, latency_error_seconds,
                    liquidity_bucket, volatility_bucket, time_to_expiry_bucket, verdict, payload
                )
                VALUES (
                    %(run_id)s, %(sample_id)s, %(source)s, %(market_slug)s, %(token_id)s, %(token_side)s,
                    %(side)s, %(role)s, %(order_type)s, %(observed_at)s, %(observed_block)s,
                    %(simulated_order_id)s, %(live_order_id)s, %(requested_price)s, %(requested_size)s,
                    %(simulated_status)s, %(live_status)s, %(status_error)s,
                    %(simulated_fill_price)s, %(live_fill_price)s, %(price_error)s,
                    %(simulated_fill_size)s, %(live_fill_size)s, %(size_error)s,
                    %(simulated_slippage)s, %(live_slippage)s, %(slippage_error)s,
                    %(simulated_fee)s, %(live_fee)s, %(fee_error)s,
                    %(simulated_rebate)s, %(live_rebate)s, %(rebate_error)s,
                    %(simulated_cash_delta)s, %(live_cash_delta)s, %(cash_error)s,
                    %(simulated_position_delta)s, %(live_position_delta)s, %(position_error)s,
                    %(simulated_pnl)s, %(live_pnl)s, %(pnl_error)s,
                    %(simulated_latency_seconds)s, %(live_latency_seconds)s, %(latency_error_seconds)s,
                    %(liquidity_bucket)s, %(volatility_bucket)s, %(time_to_expiry_bucket)s, %(verdict)s,
                    %(payload)s::jsonb
                )
                ON CONFLICT (source, sample_id)
                DO UPDATE SET
                    run_id = COALESCE(EXCLUDED.run_id, quant.quant_backtest_calibration_orders.run_id),
                    market_slug = COALESCE(EXCLUDED.market_slug, quant.quant_backtest_calibration_orders.market_slug),
                    token_id = COALESCE(EXCLUDED.token_id, quant.quant_backtest_calibration_orders.token_id),
                    token_side = COALESCE(EXCLUDED.token_side, quant.quant_backtest_calibration_orders.token_side),
                    side = COALESCE(EXCLUDED.side, quant.quant_backtest_calibration_orders.side),
                    role = COALESCE(EXCLUDED.role, quant.quant_backtest_calibration_orders.role),
                    order_type = COALESCE(EXCLUDED.order_type, quant.quant_backtest_calibration_orders.order_type),
                    observed_at = COALESCE(EXCLUDED.observed_at, quant.quant_backtest_calibration_orders.observed_at),
                    observed_block = COALESCE(EXCLUDED.observed_block, quant.quant_backtest_calibration_orders.observed_block),
                    simulated_order_id = COALESCE(EXCLUDED.simulated_order_id, quant.quant_backtest_calibration_orders.simulated_order_id),
                    live_order_id = COALESCE(EXCLUDED.live_order_id, quant.quant_backtest_calibration_orders.live_order_id),
                    requested_price = COALESCE(EXCLUDED.requested_price, quant.quant_backtest_calibration_orders.requested_price),
                    requested_size = COALESCE(EXCLUDED.requested_size, quant.quant_backtest_calibration_orders.requested_size),
                    simulated_status = COALESCE(EXCLUDED.simulated_status, quant.quant_backtest_calibration_orders.simulated_status),
                    live_status = COALESCE(EXCLUDED.live_status, quant.quant_backtest_calibration_orders.live_status),
                    status_error = EXCLUDED.status_error,
                    simulated_fill_price = COALESCE(EXCLUDED.simulated_fill_price, quant.quant_backtest_calibration_orders.simulated_fill_price),
                    live_fill_price = COALESCE(EXCLUDED.live_fill_price, quant.quant_backtest_calibration_orders.live_fill_price),
                    price_error = EXCLUDED.price_error,
                    simulated_fill_size = COALESCE(EXCLUDED.simulated_fill_size, quant.quant_backtest_calibration_orders.simulated_fill_size),
                    live_fill_size = COALESCE(EXCLUDED.live_fill_size, quant.quant_backtest_calibration_orders.live_fill_size),
                    size_error = EXCLUDED.size_error,
                    simulated_slippage = COALESCE(EXCLUDED.simulated_slippage, quant.quant_backtest_calibration_orders.simulated_slippage),
                    live_slippage = COALESCE(EXCLUDED.live_slippage, quant.quant_backtest_calibration_orders.live_slippage),
                    slippage_error = EXCLUDED.slippage_error,
                    simulated_fee = COALESCE(EXCLUDED.simulated_fee, quant.quant_backtest_calibration_orders.simulated_fee),
                    live_fee = COALESCE(EXCLUDED.live_fee, quant.quant_backtest_calibration_orders.live_fee),
                    fee_error = EXCLUDED.fee_error,
                    simulated_rebate = COALESCE(EXCLUDED.simulated_rebate, quant.quant_backtest_calibration_orders.simulated_rebate),
                    live_rebate = COALESCE(EXCLUDED.live_rebate, quant.quant_backtest_calibration_orders.live_rebate),
                    rebate_error = EXCLUDED.rebate_error,
                    simulated_cash_delta = COALESCE(EXCLUDED.simulated_cash_delta, quant.quant_backtest_calibration_orders.simulated_cash_delta),
                    live_cash_delta = COALESCE(EXCLUDED.live_cash_delta, quant.quant_backtest_calibration_orders.live_cash_delta),
                    cash_error = EXCLUDED.cash_error,
                    simulated_position_delta = COALESCE(EXCLUDED.simulated_position_delta, quant.quant_backtest_calibration_orders.simulated_position_delta),
                    live_position_delta = COALESCE(EXCLUDED.live_position_delta, quant.quant_backtest_calibration_orders.live_position_delta),
                    position_error = EXCLUDED.position_error,
                    simulated_pnl = COALESCE(EXCLUDED.simulated_pnl, quant.quant_backtest_calibration_orders.simulated_pnl),
                    live_pnl = COALESCE(EXCLUDED.live_pnl, quant.quant_backtest_calibration_orders.live_pnl),
                    pnl_error = EXCLUDED.pnl_error,
                    simulated_latency_seconds = COALESCE(EXCLUDED.simulated_latency_seconds, quant.quant_backtest_calibration_orders.simulated_latency_seconds),
                    live_latency_seconds = COALESCE(EXCLUDED.live_latency_seconds, quant.quant_backtest_calibration_orders.live_latency_seconds),
                    latency_error_seconds = EXCLUDED.latency_error_seconds,
                    liquidity_bucket = COALESCE(EXCLUDED.liquidity_bucket, quant.quant_backtest_calibration_orders.liquidity_bucket),
                    volatility_bucket = COALESCE(EXCLUDED.volatility_bucket, quant.quant_backtest_calibration_orders.volatility_bucket),
                    time_to_expiry_bucket = COALESCE(EXCLUDED.time_to_expiry_bucket, quant.quant_backtest_calibration_orders.time_to_expiry_bucket),
                    verdict = EXCLUDED.verdict,
                    payload = EXCLUDED.payload
                """,
                _db_row(row),
            )
    return len(normalized)


def _merge_nested(target: dict[str, Any], nested: Mapping[str, Any], prefix: str) -> None:
    for src_key, dest_key in (
        ("order_id", f"{prefix}_order_id"),
        ("orderId", f"{prefix}_order_id"),
        ("status", f"{prefix}_status"),
        ("fill_price", f"{prefix}_fill_price"),
        ("fillPrice", f"{prefix}_fill_price"),
        ("avg_fill_price", f"{prefix}_fill_price"),
        ("avgFillPrice", f"{prefix}_fill_price"),
        ("fill_size", f"{prefix}_fill_size"),
        ("fillSize", f"{prefix}_fill_size"),
        ("filled_size", f"{prefix}_fill_size"),
        ("filledSize", f"{prefix}_fill_size"),
        ("slippage", f"{prefix}_slippage"),
        ("fee", f"{prefix}_fee"),
        ("rebate", f"{prefix}_rebate"),
        ("cash_delta", f"{prefix}_cash_delta"),
        ("cashDelta", f"{prefix}_cash_delta"),
        ("position_delta", f"{prefix}_position_delta"),
        ("positionDelta", f"{prefix}_position_delta"),
        ("pnl", f"{prefix}_pnl"),
        ("pnl_delta", f"{prefix}_pnl"),
        ("pnlDelta", f"{prefix}_pnl"),
        ("profit", f"{prefix}_pnl"),
        ("net_pnl", f"{prefix}_pnl"),
        ("netPnl", f"{prefix}_pnl"),
        ("latency_seconds", f"{prefix}_latency_seconds"),
        ("latencySeconds", f"{prefix}_latency_seconds"),
    ):
        if src_key in nested and nested.get(src_key) not in (None, ""):
            target[dest_key] = nested.get(src_key)


def _db_row(row: Mapping[str, Any]) -> dict[str, Any]:
    numeric_fields = {
        "requested_price",
        "requested_size",
        "simulated_fill_price",
        "live_fill_price",
        "price_error",
        "simulated_fill_size",
        "live_fill_size",
        "size_error",
        "simulated_slippage",
        "live_slippage",
        "slippage_error",
        "simulated_fee",
        "live_fee",
        "fee_error",
        "simulated_rebate",
        "live_rebate",
        "rebate_error",
        "simulated_cash_delta",
        "live_cash_delta",
        "cash_error",
        "simulated_position_delta",
        "live_position_delta",
        "position_error",
        "simulated_pnl",
        "live_pnl",
        "pnl_error",
        "simulated_latency_seconds",
        "live_latency_seconds",
        "latency_error_seconds",
    }
    result: dict[str, Any] = {}
    for field in (
        "run_id",
        "sample_id",
        "source",
        "market_slug",
        "token_id",
        "token_side",
        "side",
        "role",
        "order_type",
        "observed_at",
        "observed_block",
        "simulated_order_id",
        "live_order_id",
        "simulated_status",
        "live_status",
        "status_error",
        "liquidity_bucket",
        "volatility_bucket",
        "time_to_expiry_bucket",
        "verdict",
    ):
        result[field] = row.get(field)
    for field in numeric_fields:
        value = row.get(field)
        result[field] = _decimal(value) if value not in (None, "") else None
    result["payload"] = json.dumps(row.get("payload") or {}, default=str)
    return result


def _table_exists(conn: Any) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('quant.quant_backtest_calibration_orders') IS NOT NULL AS exists")
        row = cur.fetchone()
    if isinstance(row, Mapping):
        return bool(row.get("exists"))
    return bool(row[0]) if row else False


def _first_value(row: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _default_sample_id(row: Mapping[str, Any]) -> str:
    parts = [
        row.get("run_id"),
        row.get("simulated_order_id"),
        row.get("live_order_id"),
        row.get("observed_at"),
        row.get("observed_block"),
    ]
    text = "|".join(str(part or "") for part in parts).strip("|")
    return text or "manual-sample"


def _status(value: Any) -> str:
    text = str(value or "UNKNOWN").strip().upper()
    return text or "UNKNOWN"


def _status_filled(status: str) -> bool:
    return status in FILLED_STATUSES


def _abs_diff(left: Any, right: Any) -> Decimal:
    if left in (None, "") or right in (None, ""):
        return Decimal("0")
    return abs(_decimal(left) - _decimal(right)).quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP)


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _decimal_text(value: Decimal) -> str:
    text = format(value.quantize(Decimal("0.0000000001"), rounding=ROUND_HALF_UP), "f")
    text = text.rstrip("0").rstrip(".") if "." in text else text
    return text or "0"


def _avg(values: list[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _ratio(numerator: int, denominator: int) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return Decimal(numerator) / Decimal(denominator)


def _counts(rows: list[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field) or "unknown").lower()
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _verdict(row: Mapping[str, Any]) -> str:
    if bool(row.get("status_error")):
        return "status_mismatch"
    if _decimal(row.get("price_error")) > Decimal("0.02"):
        return "price_mismatch"
    if _decimal(row.get("size_error")) > Decimal("0"):
        return "size_mismatch"
    if _decimal(row.get("latency_error_seconds")) > Decimal("2"):
        return "latency_mismatch"
    if _decimal(row.get("cash_error")) > Decimal("0.01"):
        return "cash_mismatch"
    if _decimal(row.get("pnl_error")) > Decimal("1"):
        return "pnl_mismatch"
    return "matched"
