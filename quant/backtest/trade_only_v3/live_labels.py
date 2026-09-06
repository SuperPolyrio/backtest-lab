from __future__ import annotations

from contextlib import nullcontext
from typing import Any

from quant.core.db import postgres_connection


def build_live_order_label_readiness(
    conn: Any | None = None,
    *,
    min_labels: int = 200,
    min_positive: int = 20,
    min_negative: int = 20,
) -> dict[str, Any]:
    manager = (
        nullcontext(conn) if conn is not None else postgres_connection(readonly=True)
    )
    try:
        with manager as active, active.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)::bigint AS total,
                       count(*) FILTER (WHERE live_status IS NOT NULL)::bigint AS labeled,
                       count(*) FILTER (
                           WHERE upper(coalesce(live_status, '')) IN ('FILLED', 'PARTIAL_FILLED')
                       )::bigint AS positive,
                       count(*) FILTER (
                           WHERE live_status IS NOT NULL
                             AND upper(live_status) NOT IN ('FILLED', 'PARTIAL_FILLED')
                       )::bigint AS negative
                FROM quant.quant_backtest_calibration_orders
                """
            )
            calibration_total, labeled, positive, negative = _values(
                cur.fetchone(), ("total", "labeled", "positive", "negative")
            )
            cur.execute(
                """
                SELECT count(*)::bigint AS event_rows,
                       count(DISTINCT coalesce(external_order_id, order_id))::bigint AS event_orders
                FROM quant.real_order_state_events
                """
            )
            event_rows, event_orders = _values(
                cur.fetchone(), ("event_rows", "event_orders")
            )
    except Exception as exc:  # noqa: BLE001 - readiness reports unavailable DBs
        return {
            "status": "UNAVAILABLE",
            "ready_for_live_transfer_claim": False,
            "reason": str(exc),
        }
    ready = (
        labeled >= min_labels and positive >= min_positive and negative >= min_negative
    )
    return {
        "status": "READY" if ready else "BLOCKED_INSUFFICIENT_REAL_LABELS",
        "ready_for_live_transfer_claim": ready,
        "label_contract": "REAL_SUBMITTED_ORDER_FINAL_STATUS",
        "calibration_rows": int(calibration_total),
        "labeled_orders": int(labeled),
        "positive_fills": int(positive),
        "negative_no_fills": int(negative),
        "real_order_state_event_rows": int(event_rows),
        "real_order_state_orders": int(event_orders),
        "requirements": {
            "min_labels": min_labels,
            "min_positive": min_positive,
            "min_negative": min_negative,
        },
        "reason": (
            "real submitted-order positive and NO_FILL labels satisfy activation gates"
            if ready
            else "import real submitted-order terminal states and pair them with simulated orders"
        ),
    }


def _values(row: Any, names: tuple[str, ...]) -> tuple[int, ...]:
    if isinstance(row, dict):
        return tuple(int(row.get(name) or 0) for name in names)
    return tuple(int(value or 0) for value in row)
