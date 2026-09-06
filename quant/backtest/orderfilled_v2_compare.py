"""Reference-vs-indexed comparison helpers for OrderFilled V2 replay."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from typing import Any, Iterable

from .orderfilled_v2_replay import CapacityLedger, V2OrderResult


def compare_replay_results(
    reference_results: Iterable[V2OrderResult],
    reference_ledger: CapacityLedger | dict[str, Any],
    indexed_results: Iterable[V2OrderResult],
    indexed_ledger: CapacityLedger | dict[str, Any],
    *,
    first_n: int = 10,
) -> dict[str, Any]:
    reference_payload = _canonical_payload(reference_results, reference_ledger)
    indexed_payload = _canonical_payload(indexed_results, indexed_ledger)
    diffs = _diff_payloads(reference_payload, indexed_payload, first_n=max(1, int(first_n)))
    return {
        "status": "pass" if not diffs else "fail",
        "diff_count": len(diffs),
        "first_diffs": diffs,
        "reference_hash": _hash(reference_payload),
        "indexed_hash": _hash(indexed_payload),
    }


def _canonical_payload(results: Iterable[V2OrderResult], ledger: CapacityLedger | dict[str, Any]) -> dict[str, Any]:
    return {
        "orders": [_canonical_order(result) for result in sorted(results, key=lambda row: row.order_id)],
        "ledger": _ledger_dict(ledger),
    }


def _canonical_order(result: V2OrderResult) -> dict[str, Any]:
    return {
        "order_id": result.order_id,
        "status": result.status,
        "side": result.side,
        "filled_size": str(result.filled_size),
        "unfilled_size": str(result.unfilled_size),
        "avg_price": str(result.avg_price),
        "reason_unfilled": result.reason_unfilled,
        "fills": [
            {
                "source_trade_id": fill.source_trade_id,
                "filled_size": str(fill.filled_size),
                "exec_price": str(fill.exec_price),
                "historical_price": str(fill.historical_price),
                "source_tx_hash": fill.source_tx_hash,
                "source_log_indexes": list(fill.source_log_indexes),
            }
            for fill in result.fills
        ],
    }


def _ledger_dict(ledger: CapacityLedger | dict[str, Any]) -> dict[str, str]:
    if isinstance(ledger, CapacityLedger):
        return ledger.as_dict()
    return {str(key): str(value) for key, value in sorted(dict(ledger).items())}


def _diff_payloads(reference: dict[str, Any], indexed: dict[str, Any], *, first_n: int) -> list[dict[str, Any]]:
    diffs: list[dict[str, Any]] = []
    if reference.get("ledger") != indexed.get("ledger"):
        diffs.append({"field": "ledger", "reference": reference.get("ledger"), "indexed": indexed.get("ledger")})
    ref_orders = {row["order_id"]: row for row in reference.get("orders", [])}
    idx_orders = {row["order_id"]: row for row in indexed.get("orders", [])}
    for order_id in sorted(set(ref_orders) | set(idx_orders)):
        if ref_orders.get(order_id) != idx_orders.get(order_id):
            diffs.append({"field": "order", "order_id": order_id, "reference": ref_orders.get(order_id), "indexed": idx_orders.get(order_id)})
        if len(diffs) >= first_n:
            break
    return diffs


def _hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(_plain(payload), ensure_ascii=True, sort_keys=True).encode()).hexdigest()


def _plain(value: Any) -> Any:
    if is_dataclass(value):
        return _plain(asdict(value))
    if isinstance(value, dict):
        return {str(key): _plain(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    return value
