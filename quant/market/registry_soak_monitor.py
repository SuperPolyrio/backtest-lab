"""Long-running Registry soak sampler.

This monitor does not own the registry daemon. It samples registry health and
dynamic-universe evidence so a 4-8 hour run can be judged after the fact.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

from quant.core.db import postgres_connection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=float, default=8 * 60 * 60)
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument("--jsonl-out", type=Path, default=Path("runtime_outputs/registry_soak/registry_soak_latest.jsonl"))
    parser.add_argument("--require-ws", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--ws-grace-seconds",
        type=float,
        default=300.0,
        help="Allow transient WS reconnect windows if a connected heartbeat was recorded recently.",
    )
    parser.add_argument(
        "--failure-threshold",
        type=int,
        default=3,
        help="Exit non-zero only after this many consecutive failed samples.",
    )
    parser.add_argument(
        "--pending-outbox-grace-seconds",
        type=float,
        default=300.0,
        help="Allow startup/republish pending outbox rows for this long before failing soak.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.jsonl_out.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    exit_code = 0
    consecutive_failures = 0
    failure_threshold = max(1, int(args.failure_threshold))
    while True:
        sample = collect_sample(
            require_ws=bool(args.require_ws),
            ws_grace_seconds=float(args.ws_grace_seconds),
            pending_outbox_grace_seconds=float(args.pending_outbox_grace_seconds),
        )
        if sample["status"] == "FAIL":
            consecutive_failures += 1
        else:
            consecutive_failures = 0
        sample["consecutive_failures"] = consecutive_failures
        sample["failure_threshold"] = failure_threshold
        with args.jsonl_out.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(sample, ensure_ascii=False, default=str) + "\n")
        print(json.dumps(sample, ensure_ascii=False, default=str))
        if consecutive_failures >= failure_threshold:
            exit_code = 1
        if args.duration_seconds and time.monotonic() - started >= float(args.duration_seconds):
            break
        time.sleep(max(5.0, float(args.interval_seconds)))
    return exit_code


def collect_sample(
    *,
    require_ws: bool = True,
    ws_grace_seconds: float = 300.0,
    pending_outbox_grace_seconds: float = 300.0,
) -> dict[str, Any]:
    with postgres_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT label, recorded_at, ws_connected, pending_outbox_count,
                       subscription_universe_count, execution_universe_count, stale_count,
                       pending_book_count, generation, meta
                FROM quant.paper_registry_health_snapshots
                ORDER BY recorded_at DESC
                LIMIT 1
                """
            )
            health = dict(cur.fetchone() or {})
            cur.execute(
                """
                SELECT
                    max(recorded_at) FILTER (WHERE ws_connected = TRUE) AS latest_ws_connected_at,
                    now() AS db_now
                FROM quant.paper_registry_health_snapshots
                """
            )
            ws_health = dict(cur.fetchone() or {})
            cur.execute(
                """
                SELECT COUNT(*) AS count, min(created_at) AS oldest_pending_at, now() AS db_now
                FROM quant.paper_registry_outbox
                WHERE status = 'pending'
                """
            )
            pending_row = dict(cur.fetchone() or {})
            pending_outbox = int(pending_row.get("count") or 0)
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE desired_subscribed = TRUE) AS desired_subscriptions,
                    COUNT(*) FILTER (WHERE actual_subscribed = TRUE) AS actual_subscriptions,
                    COUNT(*) FILTER (WHERE last_book_quality IN ('READY_HIGH', 'READY_MEDIUM')) AS ready_books,
                    COUNT(*) FILTER (WHERE last_book_quality = 'STALE') AS stale_books,
                    COUNT(*) FILTER (WHERE last_book_quality = 'DISCONNECTED') AS disconnected_books
                FROM quant.paper_lob_subscription_targets
                """
            )
            lob = dict(cur.fetchone() or {})
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE event_type IN ('ASSET_SUBSCRIBE_REQUESTED', 'MARKET_DISCOVERED')) AS added_hints,
                    COUNT(*) FILTER (WHERE event_type IN ('ASSET_UNSUBSCRIBE_REQUESTED', 'MARKET_CLOSED', 'MARKET_RESOLVED')) AS removed_hints,
                    COUNT(*) FILTER (WHERE event_type = 'BOOK_READY') AS book_ready_events,
                    COUNT(*) FILTER (WHERE event_type IN ('BOOK_STALE', 'BOOK_GAP')) AS book_problem_events
                FROM quant.paper_market_lifecycle_events
                WHERE created_at >= now() - interval '10 minutes'
                """
            )
            lifecycle_10m = dict(cur.fetchone() or {})
            cur.execute(
                """
                SELECT COUNT(*) AS live_to_discovered
                FROM quant.paper_market_lifecycle_events
                WHERE old_state = 'LIVE'
                  AND new_state = 'DISCOVERED'
                  AND created_at >= now() - interval '10 minutes'
                """
            )
            regressions = int((cur.fetchone() or {}).get("live_to_discovered") or 0)
    ws_connected = _ws_healthy(
        latest_health=health,
        latest_ws_connected_at=ws_health.get("latest_ws_connected_at"),
        now=ws_health.get("db_now"),
        grace_seconds=ws_grace_seconds,
    )
    status = "PASS"
    reasons: list[str] = []
    if require_ws and not ws_connected:
        status = "FAIL"
        reasons.append("ws_disconnected")
    pending_stale = _pending_outbox_stale(
        pending_count=pending_outbox,
        oldest_pending_at=pending_row.get("oldest_pending_at"),
        now=pending_row.get("db_now"),
        grace_seconds=pending_outbox_grace_seconds,
    )
    if pending_stale:
        status = "FAIL"
        reasons.append("pending_outbox")
    if regressions:
        status = "FAIL"
        reasons.append("live_to_discovered_regression")
    return {
        "sampled_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "reasons": reasons,
        "latest_health": health,
        "ws_healthy": ws_connected,
        "latest_ws_connected_at": ws_health.get("latest_ws_connected_at"),
        "ws_grace_seconds": ws_grace_seconds,
        "pending_outbox": pending_outbox,
        "oldest_pending_outbox_at": pending_row.get("oldest_pending_at"),
        "pending_outbox_grace_seconds": pending_outbox_grace_seconds,
        "pending_outbox_stale": pending_stale,
        "lob_targets": lob,
        "lifecycle_10m": lifecycle_10m,
        "live_to_discovered_10m": regressions,
    }


def _ws_healthy(
    *,
    latest_health: dict[str, Any],
    latest_ws_connected_at: Any,
    now: Any,
    grace_seconds: float,
) -> bool:
    if bool(latest_health.get("ws_connected")):
        return True
    if latest_ws_connected_at is None or now is None:
        return False
    if not isinstance(latest_ws_connected_at, datetime) or not isinstance(now, datetime):
        return False
    return (now - latest_ws_connected_at).total_seconds() <= max(0.0, float(grace_seconds))


def _pending_outbox_stale(
    *,
    pending_count: int,
    oldest_pending_at: Any,
    now: Any,
    grace_seconds: float,
) -> bool:
    if int(pending_count) <= 0:
        return False
    if not isinstance(oldest_pending_at, datetime) or not isinstance(now, datetime):
        return True
    return (now - oldest_pending_at).total_seconds() > max(0.0, float(grace_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
