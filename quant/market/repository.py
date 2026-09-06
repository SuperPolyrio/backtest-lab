"""Persistence layer for the paper market registry service."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from quant.core.db import env_float_first
from quant.core.schema import create_schema

from .api_client import BookProbeResult
from .token_universe import MarketRegistryToken
from .token_universe import TokenUniverseDiff, UniverseDecision, build_token_universe_diff

Jsonb: Any
try:
    from psycopg.types.json import Jsonb as Jsonb
except ImportError:  # pragma: no cover
    Jsonb = None


@dataclass(frozen=True)
class RegistryPersistSummary:
    tokens_seen: int = 0
    tokens_upserted: int = 0
    transitions_written: int = 0
    outbox_events: int = 0
    generation: int = 0
    subscription_count: int = 0
    execution_count: int = 0


@dataclass(frozen=True)
class RegistryOutboxPublishSummary:
    events_seen: int = 0
    events_published: int = 0
    subscribe_events: int = 0
    unsubscribe_events: int = 0
    execution_enabled_events: int = 0
    execution_disabled_events: int = 0
    scan_watermark: int = 0
    lock_deferred: bool = False

    def as_meta(self) -> dict[str, int | bool]:
        return {
            "events_seen": self.events_seen,
            "events_published": self.events_published,
            "subscribe_events": self.subscribe_events,
            "unsubscribe_events": self.unsubscribe_events,
            "execution_enabled_events": self.execution_enabled_events,
            "execution_disabled_events": self.execution_disabled_events,
            "scan_watermark": self.scan_watermark,
            "lock_deferred": self.lock_deferred,
        }


@dataclass(frozen=True)
class RegistryRepairSummary:
    asset_mappings_repaired: int = 0
    lifecycle_regressions_repaired: int = 0
    token_regressions_repaired: int = 0
    aggregate_keys_canonicalized: int = 0
    aggregate_rows_archived: int = 0
    subscription_targets_repaired: int = 0
    lob_projection_refreshed: int = 0

    def as_meta(self) -> dict[str, int]:
        return {
            "asset_mappings_repaired": self.asset_mappings_repaired,
            "lifecycle_regressions_repaired": self.lifecycle_regressions_repaired,
            "token_regressions_repaired": self.token_regressions_repaired,
            "aggregate_keys_canonicalized": self.aggregate_keys_canonicalized,
            "aggregate_rows_archived": self.aggregate_rows_archived,
            "subscription_targets_repaired": self.subscription_targets_repaired,
            "lob_projection_refreshed": self.lob_projection_refreshed,
        }


class MarketRegistryRepository:
    def __init__(self, conn: Any) -> None:
        self.conn = conn

    def init_schema(self) -> None:
        create_schema(self.conn)

    def get_state_json(self, key: str) -> dict[str, Any]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT value_json
                FROM quant.paper_market_registry_state
                WHERE key = %s
                """,
                (str(key),),
            )
            row = cur.fetchone()
        value = row["value_json"] if row else None
        return dict(value) if isinstance(value, Mapping) else {}

    def set_state_json(self, key: str, value: Mapping[str, Any]) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.paper_market_registry_state (key, value_json, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    updated_at = now()
                """,
                (str(key), _jsonb(dict(value))),
            )

    def begin_sync_run(self, sync_type: str, *, meta: Mapping[str, Any] | None = None) -> int:
        with self.conn.cursor() as cur:
            # Serialize registry writers across the daemon main loop, WS worker,
            # CLI repair jobs, and outbox publisher. Without this, two full
            # token upserts can deadlock on the token primary-key index.
            cur.execute("SET LOCAL lock_timeout = '0'")
            cur.execute("SELECT pg_advisory_xact_lock(914020250708)")
            cur.execute(
                """
                INSERT INTO quant.paper_registry_sync_runs (sync_type, status, started_at, meta)
                VALUES (%s, 'running', clock_timestamp(), %s)
                RETURNING run_id
                """,
                (sync_type, _jsonb(meta or {})),
            )
            row = cur.fetchone()
        return int(row["run_id"])

    def begin_hint_run(self, sync_type: str, *, meta: Mapping[str, Any] | None = None) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.paper_registry_sync_runs (sync_type, status, started_at, meta)
                VALUES (%s, 'running', clock_timestamp(), %s)
                RETURNING run_id
                """,
                (sync_type, _jsonb(meta or {})),
            )
            row = cur.fetchone()
        return int(row["run_id"])

    def finish_sync_run(
        self,
        run_id: int,
        *,
        status: str,
        summary: RegistryPersistSummary | None = None,
        error: str | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> None:
        summary = summary or RegistryPersistSummary()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE quant.paper_registry_sync_runs
                SET
                    status = %s,
                    finished_at = clock_timestamp(),
                    tokens_seen = %s,
                    tokens_upserted = %s,
                    transitions_written = %s,
                    outbox_events = %s,
                    error = %s,
                    meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
                WHERE run_id = %s
                """,
                (
                    status,
                    summary.tokens_seen,
                    summary.tokens_upserted,
                    summary.transitions_written,
                    summary.outbox_events,
                    error,
                    _jsonb(meta or {}),
                    int(run_id),
                ),
            )

    def latest_successful_sync_at(self, sync_type: str) -> datetime | None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT MAX(finished_at) AS finished_at
                FROM quant.paper_registry_sync_runs
                WHERE sync_type = %s
                  AND status = 'success'
                """,
                (str(sync_type),),
            )
            row = cur.fetchone()
        return row["finished_at"] if row else None

    def persist_decisions(
        self,
        decisions: Sequence[UniverseDecision],
        *,
        run_id: int | None,
        source: str,
        write_outbox: bool = True,
        aggregate_changed_only: bool = False,
    ) -> RegistryPersistSummary:
        decisions = sorted(decisions, key=lambda decision: decision.asset_id)
        previous_snapshot = self.latest_universe_diff()
        previous_states = self._previous_states([decision.asset_id for decision in decisions])
        decisions = self._guard_decision_state_regressions(decisions, previous_states)
        changed_market_keys = (
            _changed_market_keys(decisions, previous_states)
            if aggregate_changed_only
            else None
        )
        transitions, tokens_upserted = self._upsert_tokens_and_events(decisions, previous_states, run_id=run_id, source=source)
        self._upsert_market_aggregates(
            decisions,
            run_id=run_id,
            source=source,
            market_keys=changed_market_keys,
        )
        diff = build_token_universe_diff(
            list(decisions),
            previous=previous_snapshot,
            generation=(previous_snapshot.generation + 1) if previous_snapshot else 1,
        )
        self.insert_universe_snapshot(diff, source=source)
        outbox_events = 0
        if write_outbox:
            # Delta cycles update token and target state without writing a full
            # universe snapshot. Diffing against the previous full snapshot
            # would therefore replay already-published changes on every full
            # sync. Compare against the current token/target rows instead.
            outbox_events = self.upsert_subscription_targets_for_decisions(
                decisions,
                run_id=run_id,
                previous_states=previous_states,
            )
            self.repair_ineligible_subscription_targets(run_id=run_id)
        return RegistryPersistSummary(
            tokens_seen=len(decisions),
            tokens_upserted=tokens_upserted,
            transitions_written=transitions,
            outbox_events=outbox_events,
            generation=diff.generation,
            subscription_count=len(diff.subscription_asset_ids),
            execution_count=len(diff.execution_asset_ids),
        )

    def persist_partial_decisions(
        self,
        decisions: Sequence[UniverseDecision],
        *,
        run_id: int | None,
        source: str,
        aggregate_changed_only: bool = True,
        repair_global_targets: bool = False,
        include_universe_counts: bool = True,
    ) -> RegistryPersistSummary:
        """Persist a bounded delta without treating untouched assets as removals."""

        decisions = sorted(decisions, key=lambda decision: decision.asset_id)
        previous_states = self._previous_states([decision.asset_id for decision in decisions])
        decisions = self._guard_decision_state_regressions(decisions, previous_states)
        changed_market_keys = (
            _changed_market_keys(decisions, previous_states)
            if aggregate_changed_only
            else None
        )
        transitions, tokens_upserted = self._upsert_tokens_and_events(
            decisions,
            previous_states,
            run_id=run_id,
            source=source,
        )
        self._upsert_market_aggregates(
            decisions,
            run_id=run_id,
            source=source,
            market_keys=changed_market_keys,
        )
        outbox_events = self.upsert_subscription_targets_for_decisions(
            decisions,
            run_id=run_id,
            previous_states=previous_states,
        )
        if repair_global_targets:
            self.repair_ineligible_subscription_targets(run_id=run_id)
        counts: Mapping[str, Any] = {}
        latest = None
        if include_universe_counts:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE subscription_eligible = TRUE) AS subscription_count,
                        COUNT(*) FILTER (WHERE execution_eligible = TRUE) AS execution_count
                    FROM quant.paper_market_registry_tokens
                    """
                )
                counts = cur.fetchone() or {}
            latest = self.latest_universe_diff()
        return RegistryPersistSummary(
            tokens_seen=len(decisions),
            tokens_upserted=tokens_upserted,
            transitions_written=transitions,
            outbox_events=outbox_events,
            generation=latest.generation if latest else 0,
            subscription_count=int(counts.get("subscription_count") or 0),
            execution_count=int(counts.get("execution_count") or 0),
        )

    def _guard_decision_state_regressions(
        self,
        decisions: Sequence[UniverseDecision],
        previous_states: Mapping[str, Mapping[str, Any]],
    ) -> list[UniverseDecision]:
        return [
            _guard_illegal_state_regression(previous_states.get(decision.asset_id), decision)
            for decision in decisions
        ]

    def latest_universe_diff(self) -> TokenUniverseDiff | None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM quant.paper_token_universe_snapshots
                ORDER BY generation DESC, snapshot_id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        if not row:
            return None
        return TokenUniverseDiff(
            generation=int(row["generation"]),
            subscription_asset_ids=set(row["subscription_asset_ids"] or []),
            execution_asset_ids=set(row["execution_asset_ids"] or []),
            added_subscription_asset_ids=set(row["added_subscription_asset_ids"] or []),
            removed_subscription_asset_ids=set(row["removed_subscription_asset_ids"] or []),
            added_execution_asset_ids=set(row["added_execution_asset_ids"] or []),
            removed_execution_asset_ids=set(row["removed_execution_asset_ids"] or []),
        )

    def insert_universe_snapshot(self, diff: TokenUniverseDiff, *, source: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.paper_token_universe_snapshots (
                    generation, source,
                    subscription_asset_ids, execution_asset_ids,
                    added_subscription_asset_ids, removed_subscription_asset_ids,
                    added_execution_asset_ids, removed_execution_asset_ids,
                    subscription_count, execution_count, meta
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    diff.generation,
                    source,
                    sorted(diff.subscription_asset_ids),
                    sorted(diff.execution_asset_ids),
                    sorted(diff.added_subscription_asset_ids),
                    sorted(diff.removed_subscription_asset_ids),
                    sorted(diff.added_execution_asset_ids),
                    sorted(diff.removed_execution_asset_ids),
                    len(diff.subscription_asset_ids),
                    len(diff.execution_asset_ids),
                    _jsonb({}),
                ),
            )

    def write_outbox_for_diff(
        self,
        diff: TokenUniverseDiff,
        decisions: Sequence[UniverseDecision],
        *,
        run_id: int | None,
    ) -> int:
        by_asset = {decision.asset_id: decision for decision in decisions}
        rows: list[tuple[Any, ...]] = []
        for asset_id in sorted(diff.added_subscription_asset_ids):
            rows.append(self._outbox_row("ASSET_SUBSCRIBE_REQUESTED", asset_id, by_asset.get(asset_id), run_id))
        for asset_id in sorted(diff.removed_subscription_asset_ids):
            rows.append(self._outbox_row("ASSET_UNSUBSCRIBE_REQUESTED", asset_id, by_asset.get(asset_id), run_id))
        for asset_id in sorted(diff.added_execution_asset_ids):
            rows.append(self._outbox_row("ASSET_EXECUTION_ENABLED", asset_id, by_asset.get(asset_id), run_id))
        for asset_id in sorted(diff.removed_execution_asset_ids):
            rows.append(self._outbox_row("ASSET_EXECUTION_DISABLED", asset_id, by_asset.get(asset_id), run_id))
        self._supersede_conflicting_pending_outbox(rows)
        rows = self._drop_existing_pending_outbox(rows)
        if not rows:
            return 0
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO quant.paper_registry_outbox (
                    run_id, event_type, asset_id, market_id, condition_id, payload
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                rows,
            )
        return len(rows)

    def write_current_subscription_outbox(
        self,
        decisions: Sequence[UniverseDecision],
        *,
        run_id: int | None,
    ) -> int:
        rows = [
            self._outbox_row("ASSET_SUBSCRIBE_REQUESTED", decision.asset_id, decision, run_id)
            for decision in decisions
            if decision.subscription_eligible
        ]
        rows = self._drop_existing_pending_outbox(rows)
        if not rows:
            return 0
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO quant.paper_registry_outbox (
                    run_id, event_type, asset_id, market_id, condition_id, payload
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                rows,
            )
        return len(rows)

    def _supersede_conflicting_pending_outbox(self, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        inverse = {
            "ASSET_SUBSCRIBE_REQUESTED": "ASSET_UNSUBSCRIBE_REQUESTED",
            "ASSET_UNSUBSCRIBE_REQUESTED": "ASSET_SUBSCRIBE_REQUESTED",
            "ASSET_EXECUTION_ENABLED": "ASSET_EXECUTION_DISABLED",
            "ASSET_EXECUTION_DISABLED": "ASSET_EXECUTION_ENABLED",
        }
        conflicting: set[tuple[str, str]] = set()
        for row in rows:
            event_type = str(row[1])
            asset_id = str(row[2])
            other = inverse.get(event_type)
            if other:
                conflicting.add((other, asset_id))
        if not conflicting:
            return
        pending = self._select_pending_outbox(
            event_types={item[0] for item in conflicting},
            asset_ids={item[1] for item in conflicting},
        )
        superseded_ids = [
            int(row["outbox_id"])
            for row in pending
            if (str(row["event_type"]), str(row["asset_id"])) in conflicting
        ]
        if not superseded_ids:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE quant.paper_registry_outbox
                SET status = 'superseded',
                    published_at = now()
                WHERE outbox_id = ANY(%s)
                  AND status = 'pending'
                """,
                (superseded_ids,),
            )

    def _drop_existing_pending_outbox(self, rows: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        if not rows:
            return []
        requested = {(str(row[1]), str(row[2])) for row in rows}
        pending = self._select_pending_outbox(
            event_types={item[0] for item in requested},
            asset_ids={item[1] for item in requested},
        )
        existing = {
            (str(row["event_type"]), str(row["asset_id"]))
            for row in pending
            if (str(row["event_type"]), str(row["asset_id"])) in requested
        }
        seen: set[tuple[str, str]] = set()
        filtered: list[tuple[Any, ...]] = []
        for row in rows:
            pair = (str(row[1]), str(row[2]))
            if pair in existing or pair in seen:
                continue
            seen.add(pair)
            filtered.append(row)
        return filtered

    def _select_pending_outbox(
        self,
        *,
        event_types: set[str],
        asset_ids: set[str],
    ) -> list[dict[str, Any]]:
        if not event_types or not asset_ids:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT outbox_id, event_type, asset_id
                FROM quant.paper_registry_outbox
                WHERE status = 'pending'
                  AND event_type = ANY(%s)
                  AND asset_id = ANY(%s)
                """,
                (sorted(event_types), sorted(asset_ids)),
            )
            return [dict(row) for row in cur.fetchall()]

    def update_subscription_targets(
        self,
        diff: TokenUniverseDiff,
        decisions: Sequence[UniverseDecision],
    ) -> None:
        by_asset = {decision.asset_id: decision for decision in decisions}
        rows: list[tuple[Any, ...]] = []
        for asset_id in sorted(diff.added_subscription_asset_ids):
            decision = by_asset.get(asset_id)
            rows.append(
                (
                    asset_id,
                    decision.token.market_id if decision else None,
                    decision.token.condition_id if decision else None,
                    True,
                    "desired_subscribe",
                    decision.subscription_reason if decision else "subscription_eligible",
                )
            )
        for asset_id in sorted(diff.removed_subscription_asset_ids):
            decision = by_asset.get(asset_id)
            rows.append(
                (
                    asset_id,
                    decision.token.market_id if decision else None,
                    decision.token.condition_id if decision else None,
                    False,
                    "desired_unsubscribe",
                    decision.execution_reason if decision else "removed_from_subscription_universe",
                )
            )
        if not rows:
            return
        rows.sort(key=lambda row: str(row[0]))
        with self.conn.cursor() as cur:
            self._lock_subscription_target_rows([str(row[0]) for row in rows], cursor=cur)
            cur.executemany(
                """
                INSERT INTO quant.paper_lob_subscription_targets (
                    asset_id, market_id, condition_id,
                    desired_subscribed, target_status, reason, last_requested_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (asset_id) DO UPDATE SET
                    market_id = COALESCE(EXCLUDED.market_id, quant.paper_lob_subscription_targets.market_id),
                    condition_id = COALESCE(EXCLUDED.condition_id, quant.paper_lob_subscription_targets.condition_id),
                    desired_subscribed = EXCLUDED.desired_subscribed,
                    target_status = EXCLUDED.target_status,
                    reason = EXCLUDED.reason,
                    last_requested_at = now(),
                    last_subscribe_request_at = CASE
                        WHEN EXCLUDED.desired_subscribed THEN now()
                        ELSE quant.paper_lob_subscription_targets.last_subscribe_request_at
                    END,
                    last_unsubscribe_request_at = CASE
                        WHEN NOT EXCLUDED.desired_subscribed THEN now()
                        ELSE quant.paper_lob_subscription_targets.last_unsubscribe_request_at
                    END,
                    subscription_state = CASE
                        WHEN EXCLUDED.desired_subscribed THEN 'PENDING_SUBSCRIBE'
                        ELSE 'PENDING_UNSUBSCRIBE'
                    END,
                    updated_at = now()
                """,
                rows,
            )

    def upsert_subscription_targets_for_decisions(
        self,
        decisions: Sequence[UniverseDecision],
        *,
        run_id: int | None,
        previous_states: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> int:
        """Apply subscription target changes for a bounded delta decision set."""
        decisions = sorted(decisions, key=lambda decision: decision.asset_id)
        asset_ids = [decision.asset_id for decision in decisions if decision.asset_id]
        if not asset_ids:
            return 0
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT asset_id, desired_subscribed
                FROM quant.paper_lob_subscription_targets
                WHERE asset_id = ANY(%s)
                """,
                (asset_ids,),
            )
            current_desired = {str(row["asset_id"]): bool(row["desired_subscribed"]) for row in cur.fetchall()}
        target_rows: list[tuple[Any, ...]] = []
        outbox_rows: list[tuple[Any, ...]] = []
        for decision in decisions:
            desired = bool(decision.subscription_eligible)
            previous = current_desired.get(decision.asset_id)
            if not (previous is not None and previous == desired) and not (previous is None and not desired):
                target_rows.append(
                    (
                        decision.asset_id,
                        decision.token.market_id or None,
                        decision.token.condition_id,
                        desired,
                        "desired_subscribe" if desired else "desired_unsubscribe",
                        decision.subscription_reason if desired else decision.execution_reason,
                    )
                )
                outbox_rows.append(
                    self._outbox_row(
                        "ASSET_SUBSCRIBE_REQUESTED" if desired else "ASSET_UNSUBSCRIBE_REQUESTED",
                        decision.asset_id,
                        decision,
                        run_id,
                    )
                )
            if previous_states is not None:
                previous_state = previous_states.get(decision.asset_id)
                previous_execution = bool(previous_state and previous_state.get("execution_eligible"))
                current_execution = bool(decision.execution_eligible)
                if previous_execution != current_execution:
                    outbox_rows.append(
                        self._outbox_row(
                            "ASSET_EXECUTION_ENABLED" if current_execution else "ASSET_EXECUTION_DISABLED",
                            decision.asset_id,
                            decision,
                            run_id,
                        )
                    )
        if not target_rows and not outbox_rows:
            return 0
        self._supersede_conflicting_pending_outbox(outbox_rows)
        outbox_rows = self._drop_existing_pending_outbox(outbox_rows)
        target_rows.sort(key=lambda row: str(row[0]))
        with self.conn.cursor() as cur:
            if target_rows:
                self._lock_subscription_target_rows([str(row[0]) for row in target_rows], cursor=cur)
                cur.executemany(
                    """
                    INSERT INTO quant.paper_lob_subscription_targets (
                        asset_id, market_id, condition_id,
                        desired_subscribed, target_status, reason, last_requested_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, now(), now())
                    ON CONFLICT (asset_id) DO UPDATE SET
                        market_id = COALESCE(EXCLUDED.market_id, quant.paper_lob_subscription_targets.market_id),
                        condition_id = COALESCE(EXCLUDED.condition_id, quant.paper_lob_subscription_targets.condition_id),
                        desired_subscribed = EXCLUDED.desired_subscribed,
                        target_status = EXCLUDED.target_status,
                        reason = EXCLUDED.reason,
                        last_requested_at = now(),
                        last_subscribe_request_at = CASE
                            WHEN EXCLUDED.desired_subscribed THEN now()
                            ELSE quant.paper_lob_subscription_targets.last_subscribe_request_at
                        END,
                        last_unsubscribe_request_at = CASE
                            WHEN NOT EXCLUDED.desired_subscribed THEN now()
                            ELSE quant.paper_lob_subscription_targets.last_unsubscribe_request_at
                        END,
                        subscription_state = CASE
                            WHEN EXCLUDED.desired_subscribed THEN 'PENDING_SUBSCRIBE'
                            ELSE 'PENDING_UNSUBSCRIBE'
                        END,
                        updated_at = now()
                    """,
                    target_rows,
                )
            if outbox_rows:
                cur.executemany(
                    """
                    INSERT INTO quant.paper_registry_outbox (
                        run_id, event_type, asset_id, market_id, condition_id, payload
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    outbox_rows,
                )
        return len(outbox_rows)

    def publish_pending_outbox(
        self,
        *,
        limit: int | None = 1_000,
        after_outbox_id: int | None = None,
    ) -> RegistryOutboxPublishSummary:
        # The standalone outbox publisher historically bypassed
        # ``begin_sync_run`` and could lock subscription-target rows in the
        # opposite order to a full registry reconciliation.  Serialize this
        # short mutation with the same transaction advisory lock used by every
        # registry writer.  Do not block or count normal full-sync ownership as
        # a failure: defer this bounded outbox cycle and try again shortly.
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_xact_lock(914020250708) AS acquired"
            )
            acquired = cur.fetchone()
        if not bool(acquired and acquired.get("acquired")):
            return RegistryOutboxPublishSummary(lock_deferred=True)
        limit_sql = ""
        after_sql = ""
        params: list[Any] = []
        if after_outbox_id is not None:
            after_sql = "AND o.outbox_id > %s"
            params.append(max(0, int(after_outbox_id)))
        if limit is not None:
            limit_sql = "LIMIT %s"
            params.append(max(1, int(limit)))
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT o.outbox_id, o.event_type, o.asset_id, o.market_id, o.condition_id, o.payload,
                       t.desired_subscribed AS current_desired_subscribed,
                       r.execution_eligible AS current_execution_eligible
                FROM quant.paper_registry_outbox o
                LEFT JOIN quant.paper_lob_subscription_targets t USING(asset_id)
                LEFT JOIN quant.paper_market_registry_tokens r USING(asset_id)
                WHERE o.status = 'pending'
                {after_sql}
                ORDER BY o.outbox_id
                {limit_sql}
                FOR UPDATE OF o SKIP LOCKED
                """,
                tuple(params),
            )
            rows = [dict(row) for row in cur.fetchall()]
            if not rows:
                cur.execute(
                    "SELECT COALESCE(max(outbox_id), 0) AS outbox_id "
                    "FROM quant.paper_registry_outbox"
                )
                return RegistryOutboxPublishSummary(
                    scan_watermark=int(cur.fetchone()["outbox_id"])
                )
            publishable_rows = [row for row in rows if _outbox_matches_current_state(row)]
            publishable_ids = [int(row["outbox_id"]) for row in publishable_rows]
            publishable_id_set = set(publishable_ids)
            superseded_ids = [
                int(row["outbox_id"])
                for row in rows
                if int(row["outbox_id"]) not in publishable_id_set
            ]
            subscription_rows: list[tuple[Any, ...]] = []
            counts = {
                "ASSET_SUBSCRIBE_REQUESTED": 0,
                "ASSET_UNSUBSCRIBE_REQUESTED": 0,
                "ASSET_EXECUTION_ENABLED": 0,
                "ASSET_EXECUTION_DISABLED": 0,
            }
            for row in publishable_rows:
                event_type = str(row["event_type"])
                if event_type in counts:
                    counts[event_type] += 1
                if event_type not in {"ASSET_SUBSCRIBE_REQUESTED", "ASSET_UNSUBSCRIBE_REQUESTED"}:
                    continue
                desired = event_type == "ASSET_SUBSCRIBE_REQUESTED"
                subscription_rows.append(
                    (
                        row["asset_id"],
                        row["market_id"],
                        row["condition_id"],
                        desired,
                        "desired_subscribe" if desired else "desired_unsubscribe",
                        "PUBLISHED_SUBSCRIBE" if desired else "PUBLISHED_UNSUBSCRIBE",
                        event_type,
                        int(row["outbox_id"]),
                    )
                )
            if subscription_rows:
                subscription_rows.sort(key=lambda row: str(row[0]))
                self._lock_subscription_target_rows(
                    [str(row[0]) for row in subscription_rows],
                    cursor=cur,
                )
                cur.executemany(
                    """
                    UPDATE quant.paper_lob_subscription_targets
                    SET market_id = COALESCE(%s, market_id),
                        condition_id = COALESCE(%s, condition_id),
                        target_status = %s,
                        subscription_state = %s,
                        reason = %s,
                        last_requested_at = now(),
                        last_subscribe_request_at = CASE
                            WHEN %s THEN now() ELSE last_subscribe_request_at
                        END,
                        last_unsubscribe_request_at = CASE
                            WHEN NOT %s THEN now() ELSE last_unsubscribe_request_at
                        END,
                        last_outbox_id = %s,
                        updated_at = now()
                    WHERE asset_id = %s
                      AND desired_subscribed = %s
                    """,
                    [
                        (
                            market_id,
                            condition_id,
                            target_status,
                            subscription_state,
                            reason,
                            desired,
                            desired,
                            outbox_id,
                            asset_id,
                            desired,
                        )
                        for (
                            asset_id,
                            market_id,
                            condition_id,
                            desired,
                            target_status,
                            subscription_state,
                            reason,
                            outbox_id,
                        ) in subscription_rows
                    ],
                )
            if publishable_ids:
                cur.execute(
                    """
                    UPDATE quant.paper_registry_outbox
                    SET status = 'published', attempts = attempts + 1,
                        last_error = NULL, published_at = now()
                    WHERE outbox_id = ANY(%s)
                    """,
                    (publishable_ids,),
                )
            if superseded_ids:
                cur.execute(
                    """
                    UPDATE quant.paper_registry_outbox
                    SET status = 'superseded', attempts = attempts + 1,
                        last_error = 'superseded_by_current_registry_state'
                    WHERE outbox_id = ANY(%s)
                    """,
                    (superseded_ids,),
                )
        return RegistryOutboxPublishSummary(
            events_seen=len(rows),
            events_published=len(publishable_rows),
            subscribe_events=counts["ASSET_SUBSCRIBE_REQUESTED"],
            unsubscribe_events=counts["ASSET_UNSUBSCRIBE_REQUESTED"],
            execution_enabled_events=counts["ASSET_EXECUTION_ENABLED"],
            execution_disabled_events=counts["ASSET_EXECUTION_DISABLED"],
            scan_watermark=max(int(row["outbox_id"]) for row in rows),
        )

    def _lock_subscription_target_rows(self, asset_ids: Sequence[str], *, cursor: Any) -> None:
        ids = sorted({str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()})
        if not ids:
            return
        cursor.execute(
            """
            SELECT asset_id
            FROM quant.paper_lob_subscription_targets
            WHERE asset_id = ANY(%s)
            ORDER BY asset_id
            FOR UPDATE
            """,
            (ids,),
        )

    def repair_registry_state(self, *, run_id: int | None = None) -> RegistryRepairSummary:
        mapping = self.repair_asset_mappings_from_market_aggregates(run_id=run_id)
        lifecycle = self.repair_illegal_lifecycle_regressions()
        canonicalized, archived = self.canonicalize_market_aggregate_keys()
        lob_projection = self.refresh_lob_projection_from_targets(run_id=run_id)
        subscription_targets = self.repair_ineligible_subscription_targets(run_id=run_id)
        return RegistryRepairSummary(
            asset_mappings_repaired=mapping,
            lifecycle_regressions_repaired=int(lifecycle.get("lifecycle_events") or 0),
            token_regressions_repaired=int(lifecycle.get("tokens") or 0),
            aggregate_keys_canonicalized=canonicalized,
            aggregate_rows_archived=archived,
            subscription_targets_repaired=subscription_targets,
            lob_projection_refreshed=lob_projection,
        )

    def refresh_lob_projection_from_targets(self, *, run_id: int | None = None, ttl_seconds: int = 300) -> int:
        """Project fresh collector-owned L2 state into the registry execution view.

        This is intentionally narrower than a full market reconcile: it only
        promotes metadata-valid, two-sided, fresh L2 targets to LIVE. Terminal
        market exits still come from registry/full-sync lifecycle logic.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH candidates AS (
                    SELECT
                        r.asset_id,
                        r.market_id,
                        r.gamma_market_id,
                        r.condition_id,
                        r.market_state AS old_state,
                        r.execution_eligible AS old_execution_eligible,
                        t.last_book_update_at,
                        t.last_l2_event_at,
                        t.last_book_quality,
                        t.best_bid,
                        t.best_ask,
                        NULLIF(GREATEST(
                            COALESCE(t.last_book_update_at, '-infinity'::timestamptz),
                            COALESCE(t.last_l2_event_at, '-infinity'::timestamptz),
                            COALESCE(r.latest_book_at, '-infinity'::timestamptz)
                        ), '-infinity'::timestamptz) AS fresh_book_at
                    FROM quant.paper_market_registry_tokens r
                    JOIN quant.paper_lob_subscription_targets t USING(asset_id)
                    WHERE t.desired_subscribed = TRUE
                      AND t.actual_subscribed = TRUE
                      AND t.last_l2_event_at >= now() - make_interval(secs => %s)
                      AND t.last_book_quality IN ('READY_HIGH', 'READY_MEDIUM')
                      AND t.best_bid IS NOT NULL
                      AND t.best_ask IS NOT NULL
                      AND t.best_bid > 0
                      AND t.best_ask > 0
                      AND COALESCE(r.active, TRUE) = TRUE
                      AND COALESCE(r.closed, FALSE) = FALSE
                      AND COALESCE(r.resolved, FALSE) = FALSE
                      AND COALESCE(r.archived, FALSE) = FALSE
                      AND COALESCE(r.deprecated, FALSE) = FALSE
                      AND COALESCE(r.status_present, TRUE) = TRUE
                      AND COALESCE(r.token_count, 0) >= 2
                      AND NOT (
                          lower(COALESCE(r.market_slug, '')) LIKE 'trade-indexer-placeholder-%%'
                          OR lower(COALESCE(r.market_title, '')) LIKE 'trade indexer placeholder%%'
                      )
                ), updated AS (
                    UPDATE quant.paper_market_registry_tokens r
                    SET
                        market_state = 'LIVE',
                        subscription_eligible = TRUE,
                        execution_eligible = TRUE,
                        desired_subscribed = TRUE,
                        subscription_reason = 'metadata_ready',
                        execution_reason = 'book_ready',
                        book_quality = c.last_book_quality,
                        book_status = 'ok',
                        latest_book_at = c.fresh_book_at,
                        last_l2_event_at = c.last_l2_event_at,
                        last_book_update_at = c.last_book_update_at,
                        last_book_quality = c.last_book_quality,
                        best_bid = c.best_bid,
                        best_ask = c.best_ask,
                        book_source = 'lob_subscription_targets',
                        storage_tier = 'lob',
                        last_source = 'lob_projection_refresh',
                        last_run_id = %s,
                        last_checked_at = now(),
                        last_transition_at = CASE
                            WHEN r.market_state IS DISTINCT FROM 'LIVE'
                              OR r.execution_eligible IS DISTINCT FROM TRUE
                            THEN now()
                            ELSE r.last_transition_at
                        END,
                        updated_at = now()
                    FROM candidates c
                    WHERE r.asset_id = c.asset_id
                      AND (
                          r.market_state IS DISTINCT FROM 'LIVE'
                          OR r.execution_eligible IS DISTINCT FROM TRUE
                          OR r.latest_book_at IS DISTINCT FROM c.fresh_book_at
                          OR r.last_l2_event_at IS DISTINCT FROM c.last_l2_event_at
                          OR r.best_bid IS DISTINCT FROM c.best_bid
                          OR r.best_ask IS DISTINCT FROM c.best_ask
                          OR r.book_quality IS DISTINCT FROM c.last_book_quality
                      )
                    RETURNING
                        r.asset_id,
                        r.market_id,
                        r.gamma_market_id,
                        r.condition_id,
                        c.old_state,
                        c.old_execution_eligible,
                        r.market_state AS new_state,
                        r.execution_eligible AS new_execution_eligible
                ), events AS (
                    INSERT INTO quant.paper_market_lifecycle_events (
                        run_id, asset_id, market_id, gamma_market_id, condition_id,
                        event_type, old_state, new_state,
                        old_execution_eligible, new_execution_eligible,
                        source, reason, raw_payload
                    )
                    SELECT
                        %s,
                        asset_id,
                        market_id,
                        gamma_market_id,
                        condition_id,
                        'BOOK_READY',
                        old_state,
                        new_state,
                        old_execution_eligible,
                        new_execution_eligible,
                        'lob_projection_refresh',
                        'book_ready',
                        jsonb_build_object('asset_id', asset_id, 'reason', 'fresh_l2_target')
                    FROM updated
                    WHERE old_state IS DISTINCT FROM new_state
                       OR old_execution_eligible IS DISTINCT FROM new_execution_eligible
                    RETURNING asset_id
                )
                SELECT count(*) AS updated FROM updated
                """,
                (max(1, int(ttl_seconds)), run_id, run_id),
            )
            row = dict(cur.fetchone() or {})
        return int(row.get("updated") or 0)

    def count_lifecycle_events(self, *, run_id: int, source: str | None = None) -> int:
        params: list[Any] = [int(run_id)]
        source_sql = ""
        if source:
            source_sql = "AND source = %s"
            params.append(str(source))
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT count(*) AS event_count
                FROM quant.paper_market_lifecycle_events
                WHERE run_id = %s
                {source_sql}
                """,
                tuple(params),
            )
            row = dict(cur.fetchone() or {})
        return int(row.get("event_count") or 0)

    def repair_ineligible_subscription_targets(
        self,
        *,
        run_id: int | None = None,
        asset_ids: Sequence[str] | None = None,
    ) -> int:
        """Remove stale desired subscriptions for assets that left the active universe."""
        scoped_asset_ids = (
            sorted({str(asset_id) for asset_id in asset_ids if str(asset_id)})
            if asset_ids is not None
            else None
        )
        if scoped_asset_ids == []:
            return 0
        scope_sql = ""
        params: list[Any] = []
        if scoped_asset_ids is not None:
            scope_sql = "AND t.asset_id = ANY(%s)"
            params.append(scoped_asset_ids)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                WITH bad_candidates AS (
                    SELECT
                        t.asset_id,
                        t.market_id,
                        t.condition_id,
                        r.gamma_market_id,
                        r.market_state,
                        r.execution_eligible,
                        r.subscription_eligible,
                        t.target_status,
                        t.subscription_state,
                        t.actual_subscribed
                    FROM quant.paper_lob_subscription_targets t
                    JOIN quant.paper_market_registry_tokens r USING(asset_id)
                    WHERE t.desired_subscribed = TRUE
                      {scope_sql}
                      AND (
                            r.subscription_eligible IS DISTINCT FROM TRUE
                         OR r.market_state IN ('CLOSING', 'RESOLVED', 'ARCHIVED', 'INVALID_METADATA')
                      )
                ), bad AS (
                    SELECT b.*
                    FROM bad_candidates b
                    JOIN quant.paper_lob_subscription_targets t USING(asset_id)
                    ORDER BY b.asset_id
                    FOR UPDATE OF t
                ), updated AS (
                    UPDATE quant.paper_lob_subscription_targets t
                    SET desired_subscribed = FALSE,
                        target_status = 'desired_unsubscribe',
                        subscription_state = CASE
                            WHEN t.actual_subscribed THEN 'PENDING_UNSUBSCRIBE'
                            ELSE 'UNSUBSCRIBED'
                        END,
                        reason = 'registry_repair_ineligible_subscription',
                        last_unsubscribe_request_at = now(),
                        updated_at = now()
                    FROM bad b
                    WHERE t.asset_id = b.asset_id
                    RETURNING
                        t.asset_id,
                        COALESCE(t.market_id, b.market_id) AS market_id,
                        b.gamma_market_id,
                        COALESCE(t.condition_id, b.condition_id) AS condition_id,
                        b.market_state,
                        b.execution_eligible,
                        b.subscription_eligible,
                        b.target_status AS old_target_status,
                        b.subscription_state AS old_subscription_state,
                        t.subscription_state AS new_subscription_state,
                        b.actual_subscribed
                ), events AS (
                    INSERT INTO quant.paper_market_lifecycle_events (
                        run_id, asset_id, market_id, gamma_market_id, condition_id,
                        event_type, old_state, new_state,
                        old_execution_eligible, new_execution_eligible,
                        source, reason, raw_payload
                    )
                    SELECT
                        %s,
                        asset_id,
                        market_id,
                        gamma_market_id,
                        condition_id,
                        'SUBSCRIPTION_TARGET_REPAIRED',
                        market_state,
                        market_state,
                        execution_eligible,
                        execution_eligible,
                        'registry_repair.subscription_target',
                        'ineligible_asset_removed_from_desired_subscription',
                        jsonb_build_object(
                            'subscription_eligible', subscription_eligible,
                            'actual_subscribed', actual_subscribed,
                            'old_target_status', old_target_status,
                            'old_subscription_state', old_subscription_state,
                            'new_subscription_state', new_subscription_state
                        )
                    FROM updated
                    RETURNING asset_id
                )
                SELECT count(*) AS repaired
                FROM updated
                """,
                (*params, run_id),
            )
            row = dict(cur.fetchone() or {})
        return int(row.get("repaired") or 0)

    def repair_asset_mappings_from_market_aggregates(self, *, run_id: int | None = None) -> int:
        """Repair asset_id rows that were captured under placeholder conditions.

        The token table is keyed by asset_id, so a trade-indexer placeholder can
        arrive before the Gamma market mapping. Once market aggregates contain a
        non-placeholder raw_metadata.assets[] entry for that same asset_id, use
        it to correct only the market identity fields. State, desired
        subscription, and execution eligibility are recomputed by the service
        after this repair.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH market_assets AS (
                    SELECT
                        asset->>'asset_id' AS asset_id,
                        m.market_id,
                        m.gamma_market_id,
                        m.condition_id,
                        m.market_slug,
                        m.market_title,
                        m.active,
                        m.closed,
                        m.resolved,
                        m.archived,
                        m.deprecated,
                        m.token_count,
                        m.winning_asset_id,
                        m.winning_outcome,
                        m.resolution_status,
                        m.resolution_source,
                        m.resolved_time,
                        CASE
                            WHEN lower(COALESCE(asset->>'status_present', '')) IN ('true', 't', '1', 'yes') THEN TRUE
                            WHEN lower(COALESCE(asset->>'status_present', '')) IN ('false', 'f', '0', 'no') THEN FALSE
                            ELSE NULL
                        END AS status_present,
                        NULLIF(asset->>'completion_status', '') AS completion_status,
                        NULLIF(asset->>'outcome_name', '') AS outcome_name,
                        CASE
                            WHEN asset->>'outcome_index' ~ '^-?[0-9]+$'
                                THEN (asset->>'outcome_index')::integer
                            ELSE NULL
                        END AS outcome_index,
                        m.market_state,
                        m.updated_at
                    FROM quant.paper_market_registry_markets m
                    CROSS JOIN LATERAL jsonb_array_elements(m.raw_metadata->'assets') asset
                    WHERE jsonb_typeof(m.raw_metadata->'assets') = 'array'
                      AND NULLIF(asset->>'asset_id', '') IS NOT NULL
                      AND m.condition_id IS NOT NULL
                      AND m.condition_id <> ''
                      AND m.market_slug IS NOT NULL
                      AND m.market_slug NOT ILIKE 'trade-indexer-placeholder-%%'
                      AND COALESCE(asset->>'condition_id', '') = m.condition_id
                ), canonical AS (
                    SELECT *
                    FROM (
                        SELECT
                            ma.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY ma.asset_id
                                ORDER BY
                                    CASE ma.market_state
                                        WHEN 'LIVE' THEN 0
                                        WHEN 'TRADABLE_PENDING_BOOK' THEN 1
                                        WHEN 'STALE' THEN 2
                                        WHEN 'DISCOVERED' THEN 3
                                        WHEN 'CLOSING' THEN 4
                                        WHEN 'RESOLVED' THEN 5
                                        WHEN 'ARCHIVED' THEN 6
                                        ELSE 7
                                    END,
                                    ma.updated_at DESC,
                                    ma.market_slug
                            ) AS rn
                        FROM market_assets ma
                    ) ranked
                    WHERE rn = 1
                ), to_fix AS (
                    SELECT
                        t.asset_id,
                        t.market_id AS old_market_id,
                        t.gamma_market_id AS old_gamma_market_id,
                        t.condition_id AS old_condition_id,
                        t.market_slug AS old_market_slug,
                        t.market_title AS old_market_title,
                        t.outcome_name AS old_outcome_name,
                        t.outcome_index AS old_outcome_index,
                        t.market_state AS old_market_state,
                        t.execution_eligible AS old_execution_eligible,
                        c.market_id,
                        c.gamma_market_id,
                        c.condition_id,
                        c.market_slug,
                        c.market_title,
                        c.active,
                        c.closed,
                        c.resolved,
                        c.archived,
                        c.deprecated,
                        c.status_present,
                        c.completion_status,
                        c.token_count,
                        c.winning_asset_id,
                        c.winning_outcome,
                        c.resolution_status,
                        c.resolution_source,
                        c.resolved_time,
                        c.outcome_name,
                        c.outcome_index
                    FROM quant.paper_market_registry_tokens t
                    JOIN canonical c ON c.asset_id = t.asset_id
                    WHERE (
                           t.market_slug ILIKE 'trade-indexer-placeholder-%%'
                        OR t.condition_id IS DISTINCT FROM c.condition_id
                        OR t.market_slug IS DISTINCT FROM c.market_slug
                        OR t.gamma_market_id IS DISTINCT FROM c.gamma_market_id
                    )
                ), updated AS (
                    UPDATE quant.paper_market_registry_tokens t
                    SET
                        market_id = COALESCE(f.market_id, t.market_id),
                        gamma_market_id = COALESCE(f.gamma_market_id, t.gamma_market_id),
                        condition_id = f.condition_id,
                        market_slug = f.market_slug,
                        market_title = COALESCE(f.market_title, t.market_title),
                        outcome_name = COALESCE(f.outcome_name, t.outcome_name),
                        outcome_index = COALESCE(f.outcome_index, t.outcome_index),
                        active = COALESCE(f.active, t.active),
                        closed = COALESCE(f.closed, t.closed),
                        resolved = COALESCE(f.resolved, t.resolved),
                        archived = COALESCE(f.archived, t.archived),
                        deprecated = COALESCE(f.deprecated, t.deprecated),
                        status_present = COALESCE(f.status_present, t.status_present),
                        completion_status = COALESCE(f.completion_status, t.completion_status),
                        token_count = COALESCE(f.token_count, t.token_count),
                        winning_asset_id = COALESCE(f.winning_asset_id, t.winning_asset_id),
                        winning_outcome = COALESCE(f.winning_outcome, t.winning_outcome),
                        resolution_status = COALESCE(f.resolution_status, t.resolution_status),
                        resolution_source = COALESCE(f.resolution_source, t.resolution_source),
                        resolved_time = COALESCE(f.resolved_time, t.resolved_time),
                        last_source = 'registry_repair.asset_mapping',
                        last_run_id = %s,
                        raw_metadata = COALESCE(t.raw_metadata, '{}'::jsonb)
                            || jsonb_build_object(
                                'registry_repair',
                                jsonb_build_object(
                                    'repair_type', 'asset_mapping_from_market_aggregate',
                                    'previous_condition_id', f.old_condition_id,
                                    'previous_market_slug', f.old_market_slug,
                                    'new_condition_id', f.condition_id,
                                    'new_market_slug', f.market_slug
                                )
                            ),
                        updated_at = now()
                    FROM to_fix f
                    WHERE t.asset_id = f.asset_id
                    RETURNING
                        t.asset_id,
                        f.old_market_id,
                        f.old_gamma_market_id,
                        f.old_condition_id,
                        f.old_market_slug,
                        f.old_market_title,
                        f.old_outcome_name,
                        f.old_outcome_index,
                        f.old_market_state,
                        f.old_execution_eligible,
                        t.market_id AS new_market_id,
                        t.gamma_market_id AS new_gamma_market_id,
                        t.condition_id AS new_condition_id,
                        t.market_slug AS new_market_slug,
                        t.market_title AS new_market_title,
                        t.outcome_name AS new_outcome_name,
                        t.outcome_index AS new_outcome_index,
                        t.market_state AS new_market_state,
                        t.execution_eligible AS new_execution_eligible
                ), events AS (
                    INSERT INTO quant.paper_market_lifecycle_events (
                        run_id, asset_id, market_id, gamma_market_id, condition_id,
                        event_type, old_state, new_state,
                        old_execution_eligible, new_execution_eligible,
                        source, reason, raw_payload
                    )
                    SELECT
                        %s,
                        asset_id,
                        new_market_id,
                        new_gamma_market_id,
                        new_condition_id,
                        'MARKET_METADATA_REPAIRED',
                        old_market_state,
                        new_market_state,
                        old_execution_eligible,
                        new_execution_eligible,
                        'registry_repair.asset_mapping',
                        'asset_mapping_repaired_from_market_aggregate',
                        jsonb_build_object(
                            'old', jsonb_build_object(
                                'market_id', old_market_id,
                                'gamma_market_id', old_gamma_market_id,
                                'condition_id', old_condition_id,
                                'market_slug', old_market_slug,
                                'market_title', old_market_title,
                                'outcome_name', old_outcome_name,
                                'outcome_index', old_outcome_index
                            ),
                            'new', jsonb_build_object(
                                'market_id', new_market_id,
                                'gamma_market_id', new_gamma_market_id,
                                'condition_id', new_condition_id,
                                'market_slug', new_market_slug,
                                'market_title', new_market_title,
                                'outcome_name', new_outcome_name,
                                'outcome_index', new_outcome_index
                            )
                        )
                    FROM updated
                    RETURNING asset_id
                )
                SELECT count(*) AS repaired
                FROM updated
                """,
                (run_id, run_id),
            )
            row = dict(cur.fetchone() or {})
        return int(row.get("repaired") or 0)

    def repair_illegal_lifecycle_regressions(self) -> dict[str, int]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH live_bad AS (
                    SELECT event_id, asset_id
                    FROM quant.paper_market_lifecycle_events
                    WHERE old_state = 'LIVE'
                      AND new_state IN ('DISCOVERED', 'DISCOVERED_PENDING_BOOK')
                      AND (reason IS NULL OR reason NOT LIKE 'explicit_reopen:%')
                ), closing_bad AS (
                    SELECT event_id, asset_id
                    FROM quant.paper_market_lifecycle_events
                    WHERE old_state = 'CLOSING'
                      AND new_state NOT IN ('CLOSING', 'RESOLVED', 'ARCHIVED')
                      AND (reason IS NULL OR reason NOT LIKE 'explicit_reopen:%')
                ), live_event_fix AS (
                    UPDATE quant.paper_market_lifecycle_events e
                    SET event_type = 'BOOK_STALE',
                        new_state = 'STALE',
                        new_execution_eligible = FALSE,
                        reason = CASE
                            WHEN e.reason IS NULL OR e.reason = '' THEN 'metadata_regressed'
                            WHEN e.reason LIKE 'metadata_regressed:%' THEN e.reason
                            ELSE 'metadata_regressed:' || e.reason
                        END,
                        raw_payload = COALESCE(e.raw_payload, '{}'::jsonb)
                            || jsonb_build_object(
                                'registry_repair',
                                jsonb_build_object(
                                    'repair_type', 'live_to_discovered_guard',
                                    'previous_new_state', e.new_state
                                )
                            )
                    FROM live_bad b
                    WHERE e.event_id = b.event_id
                    RETURNING e.asset_id
                ), closing_event_fix AS (
                    UPDATE quant.paper_market_lifecycle_events e
                    SET event_type = 'MARKET_CLOSED',
                        new_state = 'CLOSING',
                        new_execution_eligible = FALSE,
                        reason = CASE
                            WHEN e.reason IS NULL OR e.reason = '' THEN 'closing_state_guard'
                            WHEN e.reason LIKE 'closing_state_guard:%' THEN e.reason
                            ELSE 'closing_state_guard:' || e.reason
                        END,
                        raw_payload = COALESCE(e.raw_payload, '{}'::jsonb)
                            || jsonb_build_object(
                                'registry_repair',
                                jsonb_build_object(
                                    'repair_type', 'closing_state_guard',
                                    'previous_new_state', e.new_state
                                )
                            )
                    FROM closing_bad b
                    WHERE e.event_id = b.event_id
                    RETURNING e.asset_id
                ), affected_raw AS (
                    SELECT asset_id, 'STALE'::text AS target_state FROM live_event_fix
                    UNION ALL
                    SELECT asset_id, 'CLOSING'::text AS target_state FROM closing_event_fix
                ), affected AS (
                    SELECT asset_id,
                           CASE WHEN bool_or(target_state = 'CLOSING') THEN 'CLOSING' ELSE 'STALE' END AS target_state
                    FROM affected_raw
                    GROUP BY asset_id
                ), token_fix AS (
                    UPDATE quant.paper_market_registry_tokens t
                    SET market_state = a.target_state,
                        subscription_eligible = (a.target_state = 'STALE'),
                        execution_eligible = FALSE,
                        desired_subscribed = (a.target_state = 'STALE'),
                        book_quality = 'STALE',
                        subscription_reason = CASE
                            WHEN a.target_state = 'CLOSING' THEN 'closing_state_guard'
                            WHEN t.subscription_reason IS NULL OR t.subscription_reason = '' THEN 'metadata_regressed'
                            WHEN t.subscription_reason LIKE 'metadata_regressed:%' THEN t.subscription_reason
                            ELSE 'metadata_regressed:' || t.subscription_reason
                        END,
                        execution_reason = CASE
                            WHEN a.target_state = 'CLOSING' THEN 'closing_state_guard'
                            WHEN t.execution_reason IS NULL OR t.execution_reason = '' THEN 'metadata_regressed'
                            WHEN t.execution_reason LIKE 'metadata_regressed:%' THEN t.execution_reason
                            ELSE 'metadata_regressed:' || t.execution_reason
                        END,
                        raw_metadata = COALESCE(t.raw_metadata, '{}'::jsonb)
                            || jsonb_build_object('registry_repair', CASE
                                WHEN a.target_state = 'CLOSING' THEN 'closing_state_guard'
                                ELSE 'live_to_discovered_guard'
                            END),
                        updated_at = now()
                    FROM affected a
                    WHERE t.asset_id = a.asset_id
                      AND t.market_state NOT IN ('RESOLVED', 'ARCHIVED')
                      AND (
                          t.market_state IS DISTINCT FROM a.target_state
                          OR t.subscription_eligible IS DISTINCT FROM (a.target_state = 'STALE')
                          OR t.execution_eligible IS DISTINCT FROM FALSE
                          OR t.desired_subscribed IS DISTINCT FROM (a.target_state = 'STALE')
                      )
                    RETURNING t.asset_id
                )
                SELECT
                    (SELECT count(*) FROM live_event_fix)
                        + (SELECT count(*) FROM closing_event_fix) AS lifecycle_events,
                    (SELECT count(*) FROM token_fix) AS tokens
                """
            )
            row = dict(cur.fetchone() or {})
        return {"lifecycle_events": int(row.get("lifecycle_events") or 0), "tokens": int(row.get("tokens") or 0)}

    def canonicalize_market_aggregate_keys(self) -> tuple[int, int]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH ranked AS (
                    SELECT
                        market_key,
                        condition_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY condition_id
                            ORDER BY
                                CASE WHEN market_key = condition_id THEN 0 ELSE 1 END,
                                updated_at DESC,
                                market_key
                        ) AS rn
                    FROM quant.paper_market_registry_markets
                    WHERE condition_id IS NOT NULL
                      AND condition_id <> ''
                ), archived AS (
                    UPDATE quant.paper_market_registry_markets m
                    SET condition_id = NULL,
                        market_state = 'ARCHIVED',
                        active = FALSE,
                        subscription_token_count = 0,
                        execution_token_count = 0,
                        raw_metadata = COALESCE(m.raw_metadata, '{}'::jsonb)
                            || jsonb_build_object(
                                'registry_repair',
                                jsonb_build_object(
                                    'repair_type', 'canonical_condition_aggregate',
                                    'superseded_by_market_key', r.condition_id,
                                    'previous_condition_id', r.condition_id,
                                    'previous_market_key', m.market_key
                                )
                            ),
                        updated_at = now()
                    FROM ranked r
                    WHERE m.market_key = r.market_key
                      AND r.rn > 1
                    RETURNING m.market_key
                ), canonicalized AS (
                    UPDATE quant.paper_market_registry_markets m
                    SET market_key = r.condition_id,
                        raw_metadata = COALESCE(m.raw_metadata, '{}'::jsonb)
                            || jsonb_build_object(
                                'registry_repair',
                                jsonb_build_object(
                                    'repair_type', 'canonical_condition_aggregate',
                                    'previous_market_key', m.market_key
                                )
                            ),
                        updated_at = now()
                    FROM ranked r
                    WHERE m.market_key = r.market_key
                      AND r.rn = 1
                      AND m.market_key <> r.condition_id
                    RETURNING m.market_key
                )
                SELECT
                    (SELECT count(*) FROM canonicalized) AS canonicalized,
                    (SELECT count(*) FROM archived) AS archived
                """
            )
            row = dict(cur.fetchone() or {})
        return int(row.get("canonicalized") or 0), int(row.get("archived") or 0)

    def list_current_asset_ids(self, *, limit: int | None = 10_000) -> list[str]:
        limit_sql = ""
        params: tuple[Any, ...] = ()
        if limit is not None:
            limit_sql = "LIMIT %s"
            params = (max(1, int(limit)),)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT asset_id
                FROM quant.paper_market_registry_tokens
                ORDER BY updated_at DESC
                {limit_sql}
                """,
                params,
            )
            return [str(row["asset_id"]) for row in cur.fetchall()]

    def list_reconciliation_asset_ids(self, *, limit: int | None = 50_000) -> list[str]:
        """Return tokens that can still affect the active derived universe.

        CLOSING rows are historical, already non-tradable, and are refreshed by
        lifecycle/recent-closed polling when resolution truth arrives. Including
        every unresolved historical row here would turn an active-universe sync
        into a full-history rewrite.
        """

        limit_sql = ""
        params: tuple[Any, ...] = ()
        if limit is not None:
            limit_sql = "LIMIT %s"
            params = (max(1, int(limit)),)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT r.asset_id
                FROM quant.paper_market_registry_tokens r
                LEFT JOIN quant.paper_lob_subscription_targets t USING(asset_id)
                WHERE r.subscription_eligible = TRUE
                   OR r.execution_eligible = TRUE
                   OR r.desired_subscribed = TRUE
                   OR t.desired_subscribed = TRUE
                   OR r.market_state IN (
                        'DISCOVERED',
                        'DISCOVERED_PENDING_BOOK',
                        'TRADABLE_PENDING_BOOK',
                        'LIVE',
                        'STALE',
                        'NO_CLOB_BOOK',
                        'BOOK_PROBE_ERROR'
                   )
                ORDER BY r.updated_at DESC
                {limit_sql}
                """,
                params,
            )
            return [str(row["asset_id"]) for row in cur.fetchall()]

    def terminal_asset_ids(self, asset_ids: Sequence[str]) -> set[str]:
        """Return immutable/history-only assets from a candidate API batch.

        CLOSING is deliberately reversible when an adapter supplies fresh,
        explicit active/open truth. Only resolved or archived assets are
        terminal.
        """

        ids = list(dict.fromkeys(str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()))
        if not ids:
            return set()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT asset_id
                FROM quant.paper_market_registry_tokens
                WHERE asset_id = ANY(%s)
                  AND market_state IN ('RESOLVED', 'ARCHIVED')
                  AND subscription_eligible = FALSE
                  AND execution_eligible = FALSE
                  AND desired_subscribed = FALSE
                """,
                (ids,),
            )
            return {str(row["asset_id"]) for row in cur.fetchall()}

    def list_probe_candidates(self, *, limit: int | None) -> list[str]:
        limit_sql = ""
        params: tuple[Any, ...] = ()
        if limit is not None:
            limit_sql = "LIMIT %s"
            params = (max(1, int(limit)),)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                WITH prior_execution AS (
                    -- A registry outage can expire every execution token at
                    -- once.  Re-probe the last non-empty execution universe
                    -- before walking the much larger ordinary pending queue.
                    -- The snapshot is tiny and identity-based; no market is
                    -- promoted without a fresh successful CLOB probe.
                    SELECT unnest(execution_asset_ids) AS asset_id
                    FROM (
                        SELECT execution_asset_ids
                        FROM quant.paper_token_universe_snapshots
                        WHERE execution_count > 0
                        ORDER BY snapshot_id DESC
                        LIMIT 1
                    ) latest_execution
                )
                SELECT r.asset_id
                FROM quant.paper_market_registry_tokens r
                LEFT JOIN prior_execution p ON p.asset_id = r.asset_id
                WHERE (
                    r.subscription_eligible = TRUE
                    AND r.execution_eligible = FALSE
                    AND r.market_state IN ('TRADABLE_PENDING_BOOK', 'STALE')
                  ) OR (
                        r.subscription_eligible = FALSE
                    AND r.execution_eligible = FALSE
                    AND r.market_state IN ('DISCOVERED_PENDING_BOOK', 'NO_CLOB_BOOK', 'BOOK_PROBE_ERROR')
                    AND (
                        r.market_state = 'DISCOVERED_PENDING_BOOK'
                        OR (r.market_state = 'BOOK_PROBE_ERROR' AND r.updated_at <= now() - interval '2 minutes')
                        OR (r.market_state = 'NO_CLOB_BOOK' AND r.updated_at <= now() - interval '15 minutes')
                    )
                    AND r.active = TRUE
                    AND r.closed = FALSE
                    AND r.resolved = FALSE
                    AND r.archived = FALSE
                    AND r.deprecated = FALSE
                    AND r.status_present = TRUE
                    AND r.condition_id IS NOT NULL
                    AND r.condition_id <> ''
                    AND r.token_count >= 2
                  )
                ORDER BY
                    (p.asset_id IS NULL),
                    CASE WHEN p.asset_id IS NOT NULL
                         THEN r.last_checked_at END ASC NULLS FIRST,
                    CASE r.market_state
                        WHEN 'DISCOVERED_PENDING_BOOK' THEN 0
                        WHEN 'BOOK_PROBE_ERROR' THEN 1
                        WHEN 'NO_CLOB_BOOK' THEN 2
                        WHEN 'TRADABLE_PENDING_BOOK' THEN 3
                        ELSE 4
                    END,
                    r.updated_at DESC
                {limit_sql}
                """,
                params,
            )
            return [str(row["asset_id"]) for row in cur.fetchall()]

    def list_stale_status_recheck_markets(
        self,
        *,
        limit: int,
        source_age_seconds: int,
        retry_seconds: int,
    ) -> list[dict[str, Any]]:
        """Return old open DB rows which need exact API lifecycle verification.

        Discovery-list absence is intentionally not treated as closure truth.
        These candidates are therefore limited to old status rows which still
        occupy the subscription universe without a usable book.
        """

        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    r.gamma_market_id,
                    array_agg(r.asset_id ORDER BY r.asset_id) AS asset_ids,
                    min(mss.updated_at) AS source_status_updated_at
                FROM quant.paper_market_registry_tokens r
                JOIN core.market_status_snapshot mss ON mss.market_id = r.market_id
                WHERE r.gamma_market_id IS NOT NULL
                  AND r.gamma_market_id <> ''
                  AND r.subscription_eligible = TRUE
                  AND r.execution_eligible = FALSE
                  AND r.market_state IN ('STALE', 'TRADABLE_PENDING_BOOK')
                  AND COALESCE(r.book_status, 'not_ready') IN (
                      'not_ready', 'no_clob_book', 'not_found', '404'
                  )
                  AND r.active = TRUE
                  AND r.closed = FALSE
                  AND r.resolved = FALSE
                  AND mss.updated_at <= now() - make_interval(secs => %s)
                  AND (
                      COALESCE(
                          NULLIF(
                              r.raw_metadata #>> '{targeted_status_recheck,checked_at}',
                              ''
                          )::timestamptz,
                          '-infinity'::timestamptz
                      ) <= now() - make_interval(secs => %s)
                      OR (
                          r.raw_metadata #>> '{targeted_status_recheck,outcome}' = 'api_error'
                          AND COALESCE(
                              NULLIF(
                                  r.raw_metadata #>> '{targeted_status_recheck,checked_at}',
                                  ''
                              )::timestamptz,
                              '-infinity'::timestamptz
                          ) <= now() - make_interval(secs => LEAST(%s, 300))
                      )
                  )
                GROUP BY r.gamma_market_id
                ORDER BY min(mss.updated_at), r.gamma_market_id
                LIMIT %s
                """,
                (
                    max(0, int(source_age_seconds)),
                    max(0, int(retry_seconds)),
                    max(0, int(retry_seconds)),
                    max(1, int(limit)),
                ),
            )
            return [dict(row) for row in cur.fetchall()]

    def mark_stale_status_recheck(
        self,
        gamma_market_id: str,
        *,
        outcome: str,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        self.mark_stale_status_rechecks([
            {
                "gamma_market_id": str(gamma_market_id),
                "outcome": str(outcome),
                "detail": dict(detail or {}),
            }
        ])

    def mark_stale_status_rechecks(self, checks: Sequence[Mapping[str, Any]]) -> None:
        rows = [
            {
                "gamma_market_id": str(check.get("gamma_market_id") or "").strip(),
                "outcome": str(check.get("outcome") or ""),
                "detail": dict(check.get("detail") or {}),
            }
            for check in checks
            if str(check.get("gamma_market_id") or "").strip()
        ]
        if not rows:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH checks AS (
                    SELECT gamma_market_id, outcome, detail
                    FROM jsonb_to_recordset(%s::jsonb) AS x(
                        gamma_market_id text,
                        outcome text,
                        detail jsonb
                    )
                )
                UPDATE quant.paper_market_registry_tokens
                SET raw_metadata = COALESCE(raw_metadata, '{}'::jsonb)
                        || jsonb_build_object(
                            'targeted_status_recheck',
                            jsonb_build_object(
                                'checked_at', clock_timestamp(),
                                'outcome', checks.outcome
                            ) || COALESCE(checks.detail, '{}'::jsonb)
                        ),
                    last_checked_at = now(),
                    updated_at = now()
                FROM checks
                WHERE quant.paper_market_registry_tokens.gamma_market_id = checks.gamma_market_id
                """,
                (_jsonb(rows),),
            )

    def list_desired_subscription_asset_ids(self, *, limit: int | None = 5_000) -> list[str]:
        limit_sql = ""
        params: tuple[Any, ...] = ()
        if limit is not None:
            limit_sql = "LIMIT %s"
            params = (max(1, int(limit)),)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT asset_id
                FROM quant.paper_lob_subscription_targets
                WHERE desired_subscribed = TRUE
                ORDER BY updated_at DESC
                {limit_sql}
                """,
                params,
            )
            return [str(row["asset_id"]) for row in cur.fetchall()]

    def fetch_state_tokens(self, asset_ids: Sequence[str]) -> list[MarketRegistryToken]:
        ids = [str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()]
        if not ids:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    r.asset_id,
                    r.market_id,
                    r.gamma_market_id,
                    r.condition_id,
                    r.market_slug,
                    r.market_title,
                    r.outcome_name,
                    r.outcome_index,
                    r.active,
                    r.closed,
                    r.resolved,
                    r.archived,
                    r.deprecated,
                    r.status_present,
                    r.completion_status,
                    r.token_count,
                    NULLIF(GREATEST(
                        COALESCE(t.last_book_update_at, '-infinity'::timestamptz),
                        COALESCE(t.last_l2_event_at, '-infinity'::timestamptz),
                        COALESCE(r.latest_book_at, '-infinity'::timestamptz)
                    ), '-infinity'::timestamptz) AS latest_book_at,
                    COALESCE(
                        CASE
                            WHEN t.last_book_quality IN ('READY_HIGH', 'READY_MEDIUM') THEN 'ok'
                            WHEN t.last_book_quality = 'STALE' THEN 'stale'
                            WHEN t.last_book_quality = 'DISCONNECTED' THEN 'disconnected'
                            WHEN t.last_book_quality = 'GAP' THEN 'gap'
                            WHEN t.last_book_quality = 'CROSSED' THEN 'crossed'
                            WHEN t.last_book_quality = 'MISMATCH' THEN 'mismatch'
                            WHEN t.last_book_quality = 'NOT_READY' THEN 'not_ready'
                            ELSE NULL
                        END,
                        r.book_status
                    ) AS book_status,
                    COALESCE(t.best_bid, r.best_bid) AS best_bid,
                    COALESCE(t.best_ask, r.best_ask) AS best_ask,
                    CASE WHEN COALESCE(t.last_l2_event_at, t.last_book_update_at) IS NOT NULL THEN 'lob_subscription_targets' ELSE r.book_source END AS book_source,
                    CASE WHEN COALESCE(t.last_l2_event_at, t.last_book_update_at) IS NOT NULL THEN 'lob' ELSE r.storage_tier END AS storage_tier,
                    r.winning_asset_id,
                    r.winning_outcome,
                    r.resolution_status,
                    r.resolution_source,
                    r.resolved_time,
                    r.last_source AS registry_source
                FROM quant.paper_market_registry_tokens r
                LEFT JOIN quant.paper_lob_subscription_targets t ON t.asset_id = r.asset_id
                WHERE r.asset_id = ANY(%s)
                """,
                (ids,),
            )
            return [MarketRegistryToken.from_row(dict(row)) for row in cur.fetchall()]

    def fetch_all_state_tokens(self, *, limit: int | None = 50_000) -> list[MarketRegistryToken]:
        limit_sql = ""
        params: tuple[Any, ...] = ()
        if limit is not None:
            limit_sql = "LIMIT %s"
            params = (max(1, int(limit)),)
        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    r.asset_id,
                    r.market_id,
                    r.gamma_market_id,
                    r.condition_id,
                    r.market_slug,
                    r.market_title,
                    r.outcome_name,
                    r.outcome_index,
                    r.active,
                    r.closed,
                    r.resolved,
                    r.archived,
                    r.deprecated,
                    r.status_present,
                    r.completion_status,
                    r.token_count,
                    NULLIF(GREATEST(
                        COALESCE(t.last_book_update_at, '-infinity'::timestamptz),
                        COALESCE(t.last_l2_event_at, '-infinity'::timestamptz),
                        COALESCE(r.latest_book_at, '-infinity'::timestamptz)
                    ), '-infinity'::timestamptz) AS latest_book_at,
                    COALESCE(
                        CASE
                            WHEN t.last_book_quality IN ('READY_HIGH', 'READY_MEDIUM') THEN 'ok'
                            WHEN t.last_book_quality = 'STALE' THEN 'stale'
                            WHEN t.last_book_quality = 'DISCONNECTED' THEN 'disconnected'
                            WHEN t.last_book_quality = 'GAP' THEN 'gap'
                            WHEN t.last_book_quality = 'CROSSED' THEN 'crossed'
                            WHEN t.last_book_quality = 'MISMATCH' THEN 'mismatch'
                            WHEN t.last_book_quality = 'NOT_READY' THEN 'not_ready'
                            ELSE NULL
                        END,
                        r.book_status
                    ) AS book_status,
                    COALESCE(t.best_bid, r.best_bid) AS best_bid,
                    COALESCE(t.best_ask, r.best_ask) AS best_ask,
                    CASE WHEN COALESCE(t.last_l2_event_at, t.last_book_update_at) IS NOT NULL THEN 'lob_subscription_targets' ELSE r.book_source END AS book_source,
                    CASE WHEN COALESCE(t.last_l2_event_at, t.last_book_update_at) IS NOT NULL THEN 'lob' ELSE r.storage_tier END AS storage_tier,
                    r.winning_asset_id,
                    r.winning_outcome,
                    r.resolution_status,
                    r.resolution_source,
                    r.resolved_time,
                    r.last_source AS registry_source
                FROM quant.paper_market_registry_tokens r
                LEFT JOIN quant.paper_lob_subscription_targets t ON t.asset_id = r.asset_id
                ORDER BY r.updated_at DESC
                {limit_sql}
                """,
                params,
            )
            return [MarketRegistryToken.from_row(dict(row)) for row in cur.fetchall()]

    def mark_condition_resolved(
        self,
        condition_id: str,
        *,
        run_id: int | None = None,
        source: str = "ws_lifecycle",
        raw_payload: Mapping[str, Any] | None = None,
    ) -> list[str]:
        condition = str(condition_id or "").strip()
        if not condition:
            return []
        payload = raw_payload or {}
        winning_asset_id = _text(payload.get("winning_asset_id") or payload.get("winningAssetId") or payload.get("asset_id"))
        winning_outcome = _text(
            payload.get("winning_outcome")
            or payload.get("winningOutcome")
            or payload.get("outcome")
            or payload.get("oracle_result")
            or payload.get("oracleResult")
            or payload.get("resolved_outcome")
            or payload.get("resolvedOutcome")
        )
        if winning_asset_id is None and winning_outcome is None:
            return []
        resolved_time = _datetime(payload.get("resolved_time") or payload.get("resolvedTime") or payload.get("timestamp")) or datetime.now(timezone.utc)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT asset_id, market_id, gamma_market_id, market_state, execution_eligible
                FROM quant.paper_market_registry_tokens
                WHERE condition_id = %s
                  AND market_state NOT IN ('RESOLVED', 'ARCHIVED')
                """,
                (condition,),
            )
            rows = cur.fetchall()
        asset_ids = [str(row["asset_id"]) for row in rows]
        if not rows:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE quant.paper_market_registry_tokens
                SET
                    resolved = TRUE,
                    closed = TRUE,
                    market_state = 'RESOLVED',
                    subscription_eligible = FALSE,
                    execution_eligible = FALSE,
                    desired_subscribed = FALSE,
                    execution_reason = 'market_resolved_signal',
                    subscription_reason = 'market_resolved_signal',
                    winning_asset_id = COALESCE(%s, winning_asset_id),
                    winning_outcome = COALESCE(%s, winning_outcome),
                    resolution_status = 'RESOLVED',
                    resolution_source = %s,
                    resolved_time = COALESCE(%s, resolved_time),
                    last_source = %s,
                    last_run_id = %s,
                    last_transition_at = now(),
                    updated_at = now()
                WHERE condition_id = %s
                """,
                (winning_asset_id, winning_outcome, source, resolved_time, source, run_id, condition),
            )
            cur.execute(
                """
                UPDATE quant.paper_lob_subscription_targets
                SET desired_subscribed = FALSE,
                    target_status = 'desired_unsubscribe',
                    subscription_state = 'PENDING_UNSUBSCRIBE',
                    reason = 'market_resolved_signal',
                    last_unsubscribe_request_at = now(),
                    updated_at = now()
                WHERE condition_id = %s
                """,
                (condition,),
            )
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO quant.paper_market_lifecycle_events (
                        run_id, asset_id, market_id, gamma_market_id, condition_id,
                        event_type, old_state, new_state,
                        old_execution_eligible, new_execution_eligible,
                        source, reason, raw_payload
                    ) VALUES (%s, %s, %s, %s, %s, 'MARKET_RESOLVED', %s, 'RESOLVED', %s, FALSE, %s, %s, %s)
                    """,
                    (
                        run_id,
                        row["asset_id"],
                        row["market_id"],
                        row["gamma_market_id"],
                        condition,
                        row["market_state"],
                        bool(row["execution_eligible"]),
                        source,
                        "market_resolved_signal",
                        _jsonb(raw_payload or {}),
                    ),
                )
        return asset_ids

    def apply_lob_collector_status(
        self,
        raw: Mapping[str, Any],
        *,
        run_id: int | None,
    ) -> list[str]:
        asset_id = _text(raw.get("asset_id"))
        if not asset_id:
            return []
        actual_subscribed = _bool(raw.get("actual_subscribed"), default=False)
        subscription_state = _text(raw.get("subscription_state")) or ("SUBSCRIBED" if actual_subscribed else "UNSUBSCRIBED")
        book_quality = _text(raw.get("book_quality"))
        best_bid = _positive_decimal_or_none(raw.get("best_bid"))
        best_ask = _positive_decimal_or_none(raw.get("best_ask"))
        book_snapshot_at = _datetime(raw.get("last_book_snapshot_at"))
        book_update_at = _datetime(raw.get("last_book_update_at")) or book_snapshot_at
        last_error = _text(raw.get("last_error") or raw.get("error"))
        bbo_invalid = book_quality in {"READY_HIGH", "READY_MEDIUM", "OK"} and (best_bid is None or best_ask is None)
        if bbo_invalid:
            book_quality = "NOT_READY"
            last_error = last_error or "missing_positive_bbo_side"
        book_status = _book_status_from_quality(book_quality)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.paper_lob_subscription_targets (
                    asset_id, desired_subscribed, actual_subscribed, subscription_state,
                    last_actual_subscribe_at, last_actual_unsubscribe_at,
                    last_book_snapshot_at, last_book_update_at, last_book_quality,
                    last_error, best_bid, best_ask, updated_at
                ) VALUES (
                    %s, FALSE, %s, %s,
                    CASE WHEN %s THEN now() ELSE NULL END,
                    CASE WHEN NOT %s THEN now() ELSE NULL END,
                    %s, %s, %s,
                    %s, %s, %s, now()
                )
                ON CONFLICT (asset_id) DO UPDATE SET
                    actual_subscribed = EXCLUDED.actual_subscribed,
                    subscription_state = EXCLUDED.subscription_state,
                    last_actual_subscribe_at = CASE
                        WHEN EXCLUDED.actual_subscribed THEN now()
                        ELSE quant.paper_lob_subscription_targets.last_actual_subscribe_at
                    END,
                    last_actual_unsubscribe_at = CASE
                        WHEN NOT EXCLUDED.actual_subscribed THEN now()
                        ELSE quant.paper_lob_subscription_targets.last_actual_unsubscribe_at
                    END,
                    last_book_snapshot_at = COALESCE(EXCLUDED.last_book_snapshot_at, quant.paper_lob_subscription_targets.last_book_snapshot_at),
                    last_book_update_at = COALESCE(EXCLUDED.last_book_update_at, quant.paper_lob_subscription_targets.last_book_update_at),
                    last_book_quality = COALESCE(EXCLUDED.last_book_quality, quant.paper_lob_subscription_targets.last_book_quality),
                    last_error = EXCLUDED.last_error,
                    best_bid = CASE
                        WHEN EXCLUDED.last_error = 'missing_positive_bbo_side' THEN EXCLUDED.best_bid
                        ELSE COALESCE(EXCLUDED.best_bid, quant.paper_lob_subscription_targets.best_bid)
                    END,
                    best_ask = CASE
                        WHEN EXCLUDED.last_error = 'missing_positive_bbo_side' THEN EXCLUDED.best_ask
                        ELSE COALESCE(EXCLUDED.best_ask, quant.paper_lob_subscription_targets.best_ask)
                    END,
                    updated_at = now()
                """,
                (
                    asset_id,
                    actual_subscribed,
                    subscription_state,
                    actual_subscribed,
                    actual_subscribed,
                    book_snapshot_at,
                    book_update_at,
                    book_quality,
                    last_error,
                    best_bid,
                    best_ask,
                ),
            )
            cur.execute(
                """
                UPDATE quant.paper_market_registry_tokens
                SET
                    actual_subscribed = %s,
                    latest_book_at = COALESCE(%s, latest_book_at),
                    last_l2_event_at = COALESCE(%s, last_l2_event_at),
                    last_book_update_at = COALESCE(%s, last_book_update_at),
                    last_book_quality = COALESCE(%s, last_book_quality),
                    book_status = COALESCE(%s, book_status),
                    best_bid = CASE WHEN %s THEN %s ELSE COALESCE(%s, best_bid) END,
                    best_ask = CASE WHEN %s THEN %s ELSE COALESCE(%s, best_ask) END,
                    book_source = CASE
                        WHEN %s IS NOT NULL THEN 'lob_collector_status'
                        ELSE book_source
                    END,
                    storage_tier = CASE
                        WHEN %s IS NOT NULL THEN 'lob'
                        ELSE storage_tier
                    END,
                    book_seen_first_at = CASE
                        WHEN %s IS NOT NULL AND book_seen_first_at IS NULL THEN %s
                        ELSE book_seen_first_at
                    END,
                    book_seen_last_at = COALESCE(%s, book_seen_last_at),
                    last_source = 'lob_collector_status',
                    last_run_id = %s,
                    raw_metadata = COALESCE(raw_metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = now()
                WHERE asset_id = %s
                """,
                (
                    actual_subscribed,
                    book_update_at,
                    book_update_at,
                    book_update_at,
                    book_quality,
                    book_status,
                    bbo_invalid,
                    best_bid,
                    best_bid,
                    bbo_invalid,
                    best_ask,
                    best_ask,
                    book_update_at,
                    book_update_at,
                    book_update_at,
                    book_update_at,
                    book_update_at,
                    run_id,
                    _jsonb({"lob_status": dict(raw)}),
                    asset_id,
                ),
            )
            cur.execute(
                """
                SELECT asset_id
                FROM quant.paper_market_registry_tokens
                WHERE asset_id = %s
                """,
                (asset_id,),
            )
            return [str(row["asset_id"]) for row in cur.fetchall()]

    def mark_assets_disconnected(
        self,
        asset_ids: Sequence[str],
        *,
        run_id: int | None,
        source: str = "ws_lifecycle",
        reason: str = "ws_disconnect",
    ) -> list[str]:
        ids = [str(asset_id).strip() for asset_id in asset_ids if str(asset_id).strip()]
        if not ids:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT asset_id, market_id, gamma_market_id, condition_id, market_state, execution_eligible
                FROM quant.paper_market_registry_tokens
                WHERE asset_id = ANY(%s)
                  AND market_state = 'LIVE'
                """,
                (ids,),
            )
            rows = cur.fetchall()
        affected = [str(row["asset_id"]) for row in rows]
        if not affected:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE quant.paper_lob_subscription_targets
                SET actual_subscribed = FALSE,
                    subscription_state = 'DISCONNECTED',
                    last_book_quality = 'DISCONNECTED',
                    last_error = %s,
                    updated_at = now()
                WHERE asset_id = ANY(%s)
                """,
                (reason, affected),
            )
            cur.execute(
                """
                UPDATE quant.paper_market_registry_tokens
                SET book_status = 'disconnected',
                    book_quality = 'DISCONNECTED',
                    last_book_quality = 'DISCONNECTED',
                    actual_subscribed = FALSE,
                    last_source = %s,
                    last_run_id = %s,
                    raw_metadata = COALESCE(raw_metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = now()
                WHERE asset_id = ANY(%s)
                """,
                (source, run_id, _jsonb({"ws_disconnect": {"reason": reason}}), affected),
            )
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO quant.paper_market_lifecycle_events (
                        run_id, asset_id, market_id, gamma_market_id, condition_id,
                        event_type, old_state, new_state,
                        old_execution_eligible, new_execution_eligible,
                        source, reason, raw_payload
                    ) VALUES (%s, %s, %s, %s, %s, 'BOOK_STALE', %s, 'STALE', %s, FALSE, %s, %s, %s)
                    """,
                    (
                        run_id,
                        row["asset_id"],
                        row["market_id"],
                        row["gamma_market_id"],
                        row["condition_id"],
                        row["market_state"],
                        bool(row["execution_eligible"]),
                        source,
                        reason,
                        _jsonb({"asset_id": row["asset_id"], "reason": reason}),
                    ),
                )
        return affected

    def update_probe_result(self, decision: UniverseDecision, probe: BookProbeResult, *, run_id: int | None) -> None:
        token = decision.token
        payload = {
            "probe": {
                "ok": probe.ok,
                "book_status": probe.book_status,
                "book_quality": probe.book_quality,
                "error": probe.error,
                "level_count_bid": probe.level_count_bid,
                "level_count_ask": probe.level_count_ask,
            }
        }
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE quant.paper_market_registry_tokens
                SET
                    book_status = %s,
                    book_quality = %s,
                    latest_book_at = COALESCE(%s, latest_book_at),
                    best_bid = %s,
                    best_ask = %s,
                    book_source = 'clob-book-probe',
                    storage_tier = 'probe',
                    last_run_id = %s,
                    raw_metadata = COALESCE(raw_metadata, '{}'::jsonb) || %s::jsonb,
                    updated_at = now()
                WHERE asset_id = %s
                """,
                (
                    probe.book_status,
                    probe.book_quality,
                    probe.observed_at,
                    probe.best_bid,
                    probe.best_ask,
                    run_id,
                    _jsonb(payload),
                    decision.asset_id,
                ),
            )
            cur.execute(
                """
                INSERT INTO quant.paper_market_lifecycle_events (
                    run_id, asset_id, market_id, gamma_market_id, condition_id,
                    event_type, old_state, new_state,
                    old_execution_eligible, new_execution_eligible,
                    source, reason, raw_payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, FALSE, %s, 'clob-book-probe', %s, %s)
                """,
                (
                    run_id,
                    decision.asset_id,
                    token.market_id or None,
                    token.gamma_market_id,
                    token.condition_id,
                    _probe_event_type(probe),
                    decision.market_state,
                    decision.market_state,
                    decision.execution_eligible,
                    probe.error or probe.book_status or "book_probe",
                    _jsonb(payload),
                ),
            )
        if probe.ok:
            self.insert_book_probe_snapshot(decision, probe)

    def insert_book_probe_snapshot(self, decision: UniverseDecision, probe: BookProbeResult) -> None:
        token = decision.token
        payload = probe.payload or {}
        best_bid = probe.best_bid
        best_ask = probe.best_ask
        spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
        mid = ((best_ask + best_bid) / Decimal("2")) if best_bid is not None and best_ask is not None else None
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.clob_orderbook_snapshots (
                    market_id, condition_id, market_slug, token_id, side,
                    source, event_type, book_generation, book_status,
                    snapshot_timestamp, best_bid, best_ask, spread, mid,
                    bid_depth, ask_depth, depth_total,
                    level_count_bid, level_count_ask,
                    storage_tier, payload, snapshot_version, fetched_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    'clob-book-probe', 'snapshot', 0, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    'probe', %s, 'paper-registry-probe-v1', %s
                )
                """,
                (
                    token.market_id or None,
                    token.condition_id,
                    token.market_slug,
                    token.asset_id,
                    token.outcome_name,
                    probe.book_status,
                    probe.observed_at,
                    best_bid,
                    best_ask,
                    spread,
                    mid,
                    probe.bid_depth,
                    probe.ask_depth,
                    probe.bid_depth + probe.ask_depth,
                    probe.level_count_bid,
                    probe.level_count_ask,
                    _jsonb(payload),
                    probe.observed_at,
                ),
            )

    def status(self) -> dict[str, Any]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT market_state, COUNT(*) AS count
                FROM quant.paper_market_registry_markets
                GROUP BY market_state
                ORDER BY count DESC
                """
            )
            states = {str(row["market_state"]): int(row["count"]) for row in cur.fetchall()}
            cur.execute(
                """
                SELECT
                    COUNT(*) AS tokens_total,
                    (SELECT COUNT(*) FROM quant.paper_market_registry_markets) AS markets_total,
                    (SELECT COUNT(*) FROM quant.paper_active_market_registry_tokens) AS subscription_universe_count,
                    (SELECT COUNT(*) FROM quant.paper_execution_market_registry_tokens) AS execution_universe_count,
                    COUNT(*) FILTER (WHERE market_state = 'STALE') AS stale_count,
                    COUNT(*) FILTER (WHERE market_state IN ('DISCOVERED_PENDING_BOOK', 'TRADABLE_PENDING_BOOK', 'BOOK_PROBE_ERROR')) AS pending_book_count
                FROM quant.paper_market_registry_tokens
                """
            )
            counts = dict(cur.fetchone() or {})
            ws_health_grace_seconds = max(
                0.0,
                env_float_first("REGISTRY_WS_HEALTH_GRACE_SECONDS", default=300.0),
            )
            cur.execute(
                """
                SELECT
                    (SELECT MAX(finished_at) FROM quant.paper_registry_sync_runs WHERE sync_type = 'full_sync' AND status = 'success') AS last_full_sync_at,
                    (SELECT MAX(finished_at) FROM quant.paper_registry_sync_runs WHERE sync_type = 'delta_poll' AND status = 'success') AS last_delta_poll_at,
                    (SELECT MAX(finished_at) FROM quant.paper_registry_sync_runs WHERE sync_type = 'api_delta_refresh' AND status = 'success') AS last_api_delta_refresh_at,
                    (SELECT COUNT(*) FROM quant.paper_registry_outbox WHERE status = 'pending') AS pending_outbox_count,
                    (SELECT MAX(generation) FROM quant.paper_token_universe_snapshots) AS generation,
                    (SELECT MAX(recorded_at) FROM quant.paper_registry_health_snapshots) AS latest_health_at,
                    (SELECT ws_connected
                     FROM quant.paper_registry_health_snapshots
                     ORDER BY recorded_at DESC
                     LIMIT 1) AS latest_ws_connected,
                    (SELECT MAX(recorded_at)
                     FROM quant.paper_registry_health_snapshots
                     WHERE ws_connected = TRUE) AS latest_ws_connected_at
                """,
            )
            extra = dict(cur.fetchone() or {})
        latest_ws_connected_at = extra.get("latest_ws_connected_at")
        ws_connected = bool(extra.get("latest_ws_connected"))
        if not ws_connected and isinstance(latest_ws_connected_at, datetime):
            age = datetime.now(timezone.utc) - latest_ws_connected_at
            ws_connected = age.total_seconds() <= ws_health_grace_seconds
        extra["ws_connected"] = ws_connected
        extra.pop("latest_ws_connected", None)
        return {**counts, "markets_by_state": states, **extra}

    def record_health_snapshot(
        self,
        *,
        label: str,
        uptime_seconds: float | None = None,
        ws_connected: bool | None = None,
        refresh_counts: bool = False,
        meta: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        status = self.health_snapshot_status(refresh_counts=refresh_counts)
        connected = bool(status.get("ws_connected")) if ws_connected is None else bool(ws_connected)
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quant.paper_registry_health_snapshots (
                    recorded_at, label, uptime_seconds,
                    tokens_total, subscription_universe_count, execution_universe_count,
                    stale_count, pending_book_count, pending_outbox_count,
                    generation, last_full_sync_at, last_delta_poll_at, ws_connected, meta
                ) VALUES (clock_timestamp(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING health_id, recorded_at
                """,
                (
                    str(label),
                    uptime_seconds,
                    int(status.get("tokens_total") or 0),
                    int(status.get("subscription_universe_count") or 0),
                    int(status.get("execution_universe_count") or 0),
                    int(status.get("stale_count") or 0),
                    int(status.get("pending_book_count") or 0),
                    int(status.get("pending_outbox_count") or 0),
                    status.get("generation"),
                    status.get("last_full_sync_at"),
                    status.get("last_delta_poll_at"),
                    connected,
                    _jsonb(meta or {}),
                ),
            )
            row = dict(cur.fetchone() or {})
        status.update(
            {
                "health_id": row.get("health_id"),
                "health_recorded_at": row.get("recorded_at"),
                "health_label": label,
                "ws_connected": connected,
            }
        )
        return status

    def health_snapshot_status(self, *, refresh_counts: bool = False) -> dict[str, Any]:
        """Read cached heartbeat counters, with an explicit low-frequency refresh."""

        if refresh_counts:
            return self._live_health_snapshot_status()

        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH previous AS (
                    SELECT tokens_total, subscription_universe_count, execution_universe_count,
                           stale_count, pending_book_count
                    FROM quant.paper_registry_health_snapshots
                    ORDER BY recorded_at DESC
                    LIMIT 1
                )
                SELECT
                    EXISTS(SELECT 1 FROM previous) AS has_previous,
                    COALESCE((SELECT tokens_total FROM previous), 0) AS tokens_total,
                    COALESCE((SELECT subscription_universe_count FROM previous), 0) AS subscription_universe_count,
                    COALESCE((SELECT execution_universe_count FROM previous), 0) AS execution_universe_count,
                    COALESCE((SELECT stale_count FROM previous), 0) AS stale_count,
                    COALESCE((SELECT pending_book_count FROM previous), 0) AS pending_book_count,
                    (SELECT MAX(finished_at) FROM quant.paper_registry_sync_runs
                     WHERE sync_type = 'full_sync' AND status = 'success') AS last_full_sync_at,
                    (SELECT MAX(finished_at) FROM quant.paper_registry_sync_runs
                     WHERE sync_type = 'delta_poll' AND status = 'success') AS last_delta_poll_at,
                    (SELECT COUNT(*) FROM quant.paper_registry_outbox WHERE status = 'pending') AS pending_outbox_count,
                    (SELECT MAX(generation) FROM quant.paper_token_universe_snapshots) AS generation,
                    (SELECT MAX(recorded_at) FROM quant.paper_registry_health_snapshots
                     WHERE ws_connected = TRUE) AS latest_ws_connected_at
                """
            )
            status = dict(cur.fetchone() or {})
        has_previous = bool(status.pop("has_previous", False))
        if not has_previous:
            return self._live_health_snapshot_status()
        latest_ws = status.get("latest_ws_connected_at")
        status["ws_connected"] = bool(
            isinstance(latest_ws, datetime)
            and (datetime.now(timezone.utc) - latest_ws).total_seconds()
            <= max(0.0, env_float_first("REGISTRY_WS_HEALTH_GRACE_SECONDS", default=300.0))
        )
        return status

    def _live_health_snapshot_status(self) -> dict[str, Any]:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                WITH registry_counts AS (
                    SELECT
                        COUNT(*) AS tokens_total,
                        COUNT(*) FILTER (
                            WHERE subscription_eligible = TRUE
                              AND market_state NOT IN ('CLOSING', 'RESOLVED', 'ARCHIVED', 'INVALID_METADATA')
                        ) AS subscription_universe_count,
                        COUNT(*) FILTER (WHERE market_state = 'STALE') AS stale_count,
                        COUNT(*) FILTER (
                            WHERE market_state IN (
                                'DISCOVERED_PENDING_BOOK',
                                'TRADABLE_PENDING_BOOK',
                                'BOOK_PROBE_ERROR'
                            )
                        ) AS pending_book_count
                    FROM quant.paper_market_registry_tokens
                )
                SELECT
                    registry_counts.tokens_total,
                    registry_counts.subscription_universe_count,
                    (SELECT COUNT(*) FROM quant.paper_execution_market_registry_tokens) AS execution_universe_count,
                    registry_counts.stale_count,
                    registry_counts.pending_book_count,
                    (SELECT MAX(finished_at) FROM quant.paper_registry_sync_runs
                     WHERE sync_type = 'full_sync' AND status = 'success') AS last_full_sync_at,
                    (SELECT MAX(finished_at) FROM quant.paper_registry_sync_runs
                     WHERE sync_type = 'delta_poll' AND status = 'success') AS last_delta_poll_at,
                    (SELECT COUNT(*) FROM quant.paper_registry_outbox WHERE status = 'pending') AS pending_outbox_count,
                    (SELECT MAX(generation) FROM quant.paper_token_universe_snapshots) AS generation,
                    (SELECT MAX(recorded_at) FROM quant.paper_registry_health_snapshots
                     WHERE ws_connected = TRUE) AS latest_ws_connected_at
                FROM registry_counts
                """
            )
            status = dict(cur.fetchone() or {})
        latest_ws = status.get("latest_ws_connected_at")
        status["ws_connected"] = bool(
            isinstance(latest_ws, datetime)
            and (datetime.now(timezone.utc) - latest_ws).total_seconds()
            <= max(0.0, env_float_first("REGISTRY_WS_HEALTH_GRACE_SECONDS", default=300.0))
        )
        return status

    def _upsert_market_aggregates(
        self,
        decisions: Sequence[UniverseDecision],
        *,
        run_id: int | None,
        source: str,
        market_keys: set[str] | None = None,
    ) -> None:
        aggregates: dict[str, dict[str, Any]] = {}
        for decision in decisions:
            token = decision.token
            market_key = _market_key(token)
            if not market_key or (market_keys is not None and market_key not in market_keys):
                continue
            aggregate = aggregates.setdefault(
                market_key,
                {
                    "market_key": market_key,
                    "market_id": token.market_id or None,
                    "gamma_market_id": token.gamma_market_id,
                    "condition_id": token.condition_id,
                    "market_slug": token.market_slug,
                    "market_title": token.market_title,
                    "market_state": decision.market_state,
                    "active_count": 0,
                    "closed_count": 0,
                    "resolved_count": 0,
                    "archived_count": 0,
                    "deprecated_count": 0,
                    "token_count": 0,
                    "subscription_token_count": 0,
                    "execution_token_count": 0,
                    "winning_asset_id": None,
                    "winning_outcome": None,
                    "resolution_status": None,
                    "resolution_source": None,
                    "resolved_time": None,
                    "raw_metadata": {"market_key": market_key, "assets": []},
                },
            )
            aggregate["market_id"] = aggregate["market_id"] or token.market_id or None
            aggregate["gamma_market_id"] = aggregate["gamma_market_id"] or token.gamma_market_id
            aggregate["condition_id"] = aggregate["condition_id"] or token.condition_id
            aggregate["market_slug"] = aggregate["market_slug"] or token.market_slug
            aggregate["market_title"] = aggregate["market_title"] or token.market_title
            aggregate["market_state"] = _dominant_market_state(str(aggregate["market_state"]), decision.market_state)
            aggregate["active_count"] += int(token.active)
            aggregate["closed_count"] += int(token.closed)
            aggregate["resolved_count"] += int(token.resolved)
            aggregate["archived_count"] += int(token.archived)
            aggregate["deprecated_count"] += int(token.deprecated)
            aggregate["token_count"] += 1
            aggregate["subscription_token_count"] += int(decision.subscription_eligible)
            aggregate["execution_token_count"] += int(decision.execution_eligible)
            aggregate["winning_asset_id"] = aggregate["winning_asset_id"] or token.winning_asset_id
            aggregate["winning_outcome"] = aggregate["winning_outcome"] or token.winning_outcome
            aggregate["resolution_status"] = aggregate["resolution_status"] or token.resolution_status
            aggregate["resolution_source"] = aggregate["resolution_source"] or token.resolution_source
            aggregate["resolved_time"] = aggregate["resolved_time"] or token.resolved_time
            aggregate["raw_metadata"]["assets"].append(decision.as_dict())
        if not aggregates:
            return
        rows: list[tuple[Any, ...]] = []
        for aggregate in aggregates.values():
            token_count = int(aggregate["token_count"])
            rows.append(
                (
                    aggregate["market_key"],
                    aggregate["market_id"],
                    aggregate["gamma_market_id"],
                    aggregate["condition_id"],
                    aggregate["market_slug"],
                    aggregate["market_title"],
                    aggregate["market_state"],
                    int(aggregate["active_count"]) > 0,
                    int(aggregate["closed_count"]) >= token_count,
                    int(aggregate["resolved_count"]) > 0,
                    int(aggregate["archived_count"]) >= token_count,
                    int(aggregate["deprecated_count"]) >= token_count,
                    token_count,
                    int(aggregate["subscription_token_count"]),
                    int(aggregate["execution_token_count"]),
                    aggregate["winning_asset_id"],
                    aggregate["winning_outcome"],
                    aggregate["resolution_status"],
                    aggregate["resolution_source"],
                    aggregate["resolved_time"],
                    source,
                    run_id,
                    _jsonb(aggregate["raw_metadata"]),
                )
            )
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO quant.paper_market_registry_markets (
                    market_key, market_id, gamma_market_id, condition_id, market_slug, market_title,
                    market_state, active, closed, resolved, archived, deprecated,
                    token_count, subscription_token_count, execution_token_count,
                    winning_asset_id, winning_outcome, resolution_status, resolution_source, resolved_time,
                    last_source, last_run_id, raw_metadata, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, now()
                )
                ON CONFLICT (market_key) DO UPDATE SET
                    market_id = COALESCE(EXCLUDED.market_id, quant.paper_market_registry_markets.market_id),
                    gamma_market_id = COALESCE(EXCLUDED.gamma_market_id, quant.paper_market_registry_markets.gamma_market_id),
                    condition_id = COALESCE(EXCLUDED.condition_id, quant.paper_market_registry_markets.condition_id),
                    market_slug = COALESCE(EXCLUDED.market_slug, quant.paper_market_registry_markets.market_slug),
                    market_title = COALESCE(EXCLUDED.market_title, quant.paper_market_registry_markets.market_title),
                    market_state = EXCLUDED.market_state,
                    active = EXCLUDED.active,
                    closed = EXCLUDED.closed,
                    resolved = EXCLUDED.resolved,
                    archived = EXCLUDED.archived,
                    deprecated = EXCLUDED.deprecated,
                    token_count = EXCLUDED.token_count,
                    subscription_token_count = EXCLUDED.subscription_token_count,
                    execution_token_count = EXCLUDED.execution_token_count,
                    winning_asset_id = COALESCE(EXCLUDED.winning_asset_id, quant.paper_market_registry_markets.winning_asset_id),
                    winning_outcome = COALESCE(EXCLUDED.winning_outcome, quant.paper_market_registry_markets.winning_outcome),
                    resolution_status = COALESCE(EXCLUDED.resolution_status, quant.paper_market_registry_markets.resolution_status),
                    resolution_source = COALESCE(EXCLUDED.resolution_source, quant.paper_market_registry_markets.resolution_source),
                    resolved_time = COALESCE(EXCLUDED.resolved_time, quant.paper_market_registry_markets.resolved_time),
                    last_source = EXCLUDED.last_source,
                    last_run_id = EXCLUDED.last_run_id,
                    raw_metadata = EXCLUDED.raw_metadata,
                    updated_at = now()
                """,
                rows,
            )

    def _previous_states(self, asset_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
        ids = [str(asset_id) for asset_id in asset_ids if str(asset_id).strip()]
        if not ids:
            return {}
        with self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    asset_id,
                    market_id,
                    gamma_market_id,
                    condition_id,
                    market_slug,
                    market_title,
                    outcome_name,
                    outcome_index,
                    active,
                    closed,
                    resolved,
                    archived,
                    deprecated,
                    status_present,
                    completion_status,
                    token_count,
                    winning_asset_id,
                    winning_outcome,
                    resolution_status,
                    resolution_source,
                    resolved_time,
                    market_state,
                    execution_eligible,
                    subscription_eligible
                FROM quant.paper_market_registry_tokens
                WHERE asset_id = ANY(%s)
                """,
                (ids,),
            )
            return {str(row["asset_id"]): dict(row) for row in cur.fetchall()}

    def _upsert_tokens_and_events(
        self,
        decisions: Sequence[UniverseDecision],
        previous_states: Mapping[str, Mapping[str, Any]],
        *,
        run_id: int | None,
        source: str,
    ) -> tuple[int, int]:
        event_rows: list[tuple[Any, ...]] = []
        token_rows: list[tuple[Any, ...]] = []
        for decision in decisions:
            token = decision.token
            previous = previous_states.get(decision.asset_id)
            state_changed = previous is None or str(previous.get("market_state")) != decision.market_state
            subscription_changed = previous is None or bool(previous.get("subscription_eligible")) != decision.subscription_eligible
            execution_changed = previous is None or bool(previous.get("execution_eligible")) != decision.execution_eligible
            metadata_changed = previous is None or _decision_market_identity_changed(previous, decision)
            if state_changed or execution_changed:
                payload = _jsonb(decision.as_dict())
                for event_type in _lifecycle_event_types(
                    previous,
                    decision,
                    state_changed=state_changed,
                    execution_changed=execution_changed,
                ):
                    event_rows.append(
                        (
                            run_id,
                            decision.asset_id,
                            token.market_id or None,
                            token.gamma_market_id,
                            token.condition_id,
                            event_type,
                            previous.get("market_state") if previous else None,
                            decision.market_state,
                            previous.get("execution_eligible") if previous else None,
                            decision.execution_eligible,
                            source,
                            decision.execution_reason,
                            payload,
                        )
                    )
            if (
                previous is not None
                and not state_changed
                and not subscription_changed
                and not execution_changed
                and not metadata_changed
            ):
                continue
            transition_changed = state_changed or execution_changed
            token_rows.append(
                (
                    decision.asset_id,
                    token.market_id or None,
                    token.gamma_market_id,
                    token.condition_id,
                    token.market_slug,
                    token.market_title,
                    token.outcome_name,
                    token.outcome_index,
                    token.active,
                    token.closed,
                    token.resolved,
                    token.archived,
                    token.deprecated,
                    token.status_present,
                    token.completion_status,
                    token.token_count,
                    decision.market_state,
                    decision.subscription_eligible,
                    decision.execution_eligible,
                    decision.subscription_reason,
                    decision.execution_reason,
                    decision.book_quality,
                    token.book_status,
                    token.latest_book_at,
                    decision.book_age_ms,
                    token.best_bid,
                    token.best_ask,
                    token.book_source,
                    token.storage_tier,
                    token.winning_asset_id,
                    token.winning_outcome,
                    token.resolution_status,
                    token.resolution_source,
                    token.resolved_time,
                    decision.subscription_eligible,
                    source,
                    run_id,
                    transition_changed,
                    _jsonb(decision.as_dict()),
                    transition_changed,
                )
            )

        with self.conn.cursor() as cur:
            if event_rows:
                cur.executemany(
                    """
                    INSERT INTO quant.paper_market_lifecycle_events (
                        run_id, asset_id, market_id, gamma_market_id, condition_id,
                        event_type, old_state, new_state,
                        old_execution_eligible, new_execution_eligible,
                        source, reason, raw_payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    event_rows,
                )
            if token_rows:
                cur.executemany(
                    """
                    INSERT INTO quant.paper_market_registry_tokens (
                        asset_id, market_id, gamma_market_id, condition_id, market_slug, market_title,
                        outcome_name, outcome_index, active, closed, resolved, archived, deprecated,
                        status_present, completion_status, token_count,
                        market_state, subscription_eligible, execution_eligible,
                        subscription_reason, execution_reason, book_quality, book_status,
                        latest_book_at, book_age_ms, best_bid, best_ask, book_source, storage_tier,
                        winning_asset_id, winning_outcome, resolution_status, resolution_source, resolved_time,
                        desired_subscribed, last_source, last_run_id, last_checked_at,
                        last_transition_at, raw_metadata, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, now(),
                        CASE WHEN %s THEN now() ELSE NULL END,
                        %s, now()
                    )
                    ON CONFLICT (asset_id) DO UPDATE SET
                        market_id = COALESCE(EXCLUDED.market_id, quant.paper_market_registry_tokens.market_id),
                        gamma_market_id = COALESCE(EXCLUDED.gamma_market_id, quant.paper_market_registry_tokens.gamma_market_id),
                        condition_id = COALESCE(EXCLUDED.condition_id, quant.paper_market_registry_tokens.condition_id),
                        market_slug = COALESCE(EXCLUDED.market_slug, quant.paper_market_registry_tokens.market_slug),
                        market_title = COALESCE(EXCLUDED.market_title, quant.paper_market_registry_tokens.market_title),
                        outcome_name = EXCLUDED.outcome_name,
                        outcome_index = EXCLUDED.outcome_index,
                        active = EXCLUDED.active,
                        closed = EXCLUDED.closed,
                        resolved = EXCLUDED.resolved,
                        archived = EXCLUDED.archived,
                        deprecated = EXCLUDED.deprecated,
                        status_present = EXCLUDED.status_present,
                        completion_status = EXCLUDED.completion_status,
                        token_count = EXCLUDED.token_count,
                        market_state = EXCLUDED.market_state,
                        subscription_eligible = EXCLUDED.subscription_eligible,
                        execution_eligible = EXCLUDED.execution_eligible,
                        subscription_reason = EXCLUDED.subscription_reason,
                        execution_reason = EXCLUDED.execution_reason,
                        book_quality = EXCLUDED.book_quality,
                        book_status = EXCLUDED.book_status,
                        latest_book_at = EXCLUDED.latest_book_at,
                        book_age_ms = EXCLUDED.book_age_ms,
                        best_bid = EXCLUDED.best_bid,
                        best_ask = EXCLUDED.best_ask,
                        book_source = EXCLUDED.book_source,
                        storage_tier = EXCLUDED.storage_tier,
                        winning_asset_id = COALESCE(EXCLUDED.winning_asset_id, quant.paper_market_registry_tokens.winning_asset_id),
                        winning_outcome = COALESCE(EXCLUDED.winning_outcome, quant.paper_market_registry_tokens.winning_outcome),
                        resolution_status = COALESCE(EXCLUDED.resolution_status, quant.paper_market_registry_tokens.resolution_status),
                        resolution_source = COALESCE(EXCLUDED.resolution_source, quant.paper_market_registry_tokens.resolution_source),
                        resolved_time = COALESCE(EXCLUDED.resolved_time, quant.paper_market_registry_tokens.resolved_time),
                        desired_subscribed = EXCLUDED.desired_subscribed,
                        last_source = EXCLUDED.last_source,
                        last_run_id = EXCLUDED.last_run_id,
                        last_checked_at = now(),
                        last_transition_at = CASE
                            WHEN %s THEN now()
                            ELSE quant.paper_market_registry_tokens.last_transition_at
                        END,
                        raw_metadata = COALESCE(quant.paper_market_registry_tokens.raw_metadata, '{}'::jsonb)
                            || EXCLUDED.raw_metadata,
                        updated_at = now()
                    """,
                    token_rows,
                )
        return len(event_rows), len(token_rows)

    def _outbox_row(
        self,
        event_type: str,
        asset_id: str,
        decision: UniverseDecision | None,
        run_id: int | None = None,
    ) -> tuple[Any, ...]:
        token = decision.token if decision else None
        payload = decision.as_dict() if decision else {"asset_id": asset_id}
        return (
            run_id,
            event_type,
            asset_id,
            token.market_id if token else None,
            token.condition_id if token else None,
            _jsonb(payload),
        )


def _jsonb(value: Any) -> Any:
    if Jsonb is not None:
        return Jsonb(value, dumps=lambda obj: json.dumps(obj, ensure_ascii=False, default=_json_default))
    return json.dumps(value, ensure_ascii=False, default=_json_default)


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    return str(value)


def _decision_market_identity_changed(previous: Mapping[str, Any], decision: UniverseDecision) -> bool:
    token = decision.token
    identity_checks = (
        ("market_id", token.market_id or None),
        ("gamma_market_id", token.gamma_market_id),
        ("condition_id", token.condition_id),
        ("market_slug", token.market_slug),
        ("market_title", token.market_title),
        ("outcome_name", token.outcome_name),
        ("outcome_index", token.outcome_index),
    )
    metadata_checks = (
        ("active", token.active),
        ("closed", token.closed),
        ("resolved", token.resolved),
        ("archived", token.archived),
        ("deprecated", token.deprecated),
        ("status_present", token.status_present),
        ("completion_status", token.completion_status),
        ("token_count", token.token_count),
        ("winning_asset_id", token.winning_asset_id),
        ("winning_outcome", token.winning_outcome),
        ("resolution_status", token.resolution_status),
        ("resolution_source", token.resolution_source),
        ("resolved_time", token.resolved_time),
    )
    return any(
        _normalise_identity_value(previous.get(key)) != _normalise_identity_value(value)
        for key, value in identity_checks
    ) or any(
        key in previous
        and _normalise_identity_value(previous.get(key)) != _normalise_identity_value(value)
        for key, value in metadata_checks
    )


def _changed_market_keys(
    decisions: Sequence[UniverseDecision],
    previous_states: Mapping[str, Mapping[str, Any]],
) -> set[str]:
    keys: set[str] = set()
    for decision in decisions:
        previous = previous_states.get(decision.asset_id)
        changed = (
            previous is None
            or str(previous.get("market_state")) != decision.market_state
            or bool(previous.get("subscription_eligible")) != decision.subscription_eligible
            or bool(previous.get("execution_eligible")) != decision.execution_eligible
            or _decision_market_identity_changed(previous, decision)
        )
        if changed and (market_key := _market_key(decision.token)):
            keys.add(market_key)
    return keys


def _normalise_identity_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "subscribed"}


def _outbox_matches_current_state(row: Mapping[str, Any]) -> bool:
    event_type = str(row.get("event_type") or "")
    if event_type == "ASSET_SUBSCRIBE_REQUESTED":
        return row.get("current_desired_subscribed") is True
    if event_type == "ASSET_UNSUBSCRIBE_REQUESTED":
        return row.get("current_desired_subscribed") is False
    if event_type == "ASSET_EXECUTION_ENABLED":
        return row.get("current_execution_eligible") is True
    if event_type == "ASSET_EXECUTION_DISABLED":
        return row.get("current_execution_eligible") is False
    return True


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _positive_decimal_or_none(value: Any) -> Decimal | None:
    parsed = _decimal_or_none(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


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


def _book_status_from_quality(value: str | None) -> str | None:
    quality = str(value or "").upper()
    if quality in {"READY_HIGH", "READY_MEDIUM", "OK"}:
        return "ok"
    if quality in {"STALE", "GAP", "DISCONNECTED"}:
        return "stale"
    if quality in {"EMPTY"}:
        return "empty"
    if quality in {"ONE_SIDED"}:
        return "one_sided"
    if quality in {"NO_CLOB_BOOK"}:
        return "no_clob_book"
    return None


def _market_key(token: MarketRegistryToken) -> str:
    for value in (token.condition_id, token.gamma_market_id, token.market_slug, token.market_id):
        text = str(value or "").strip()
        if text and text != "0":
            return text
    return ""


def _guard_illegal_state_regression(
    previous: Mapping[str, Any] | None,
    decision: UniverseDecision,
) -> UniverseDecision:
    if not previous:
        return decision
    old_state = str(previous.get("market_state") or "").strip().upper()
    new_state = str(decision.market_state or "").strip().upper()
    if old_state in {"RESOLVED", "ARCHIVED"} and new_state not in {old_state, "RESOLVED", "ARCHIVED"}:
        reason = _prefixed_reason("terminal_state_guard", decision.execution_reason or decision.subscription_reason)
        return replace(
            decision,
            subscription_eligible=False,
            execution_eligible=False,
            market_state=old_state,
            subscription_reason=reason,
            execution_reason=reason,
            book_quality="STALE",
        )
    trusted_reopen_reason = _trusted_open_reopen_reason(decision)
    if old_state == "CLOSING" and new_state not in {"CLOSING", "RESOLVED", "ARCHIVED"} and trusted_reopen_reason:
        decision = replace(
            decision,
            subscription_reason=trusted_reopen_reason,
            execution_reason=trusted_reopen_reason,
        )
    explicit_reopen = any(
        str(reason or "").startswith("explicit_reopen:")
        for reason in (decision.subscription_reason, decision.execution_reason)
    )
    if old_state == "CLOSING" and new_state not in {"CLOSING", "RESOLVED", "ARCHIVED"} and not explicit_reopen:
        reason = _prefixed_reason("closing_state_guard", decision.execution_reason or decision.subscription_reason)
        return replace(
            decision,
            subscription_eligible=False,
            execution_eligible=False,
            market_state="CLOSING",
            subscription_reason=reason,
            execution_reason=reason,
            book_quality="STALE",
        )
    if old_state in {"LIVE", "STALE", "TRADABLE_PENDING_BOOK"} and new_state in {"DISCOVERED", "DISCOVERED_PENDING_BOOK"}:
        reason = _prefixed_reason("metadata_regressed", decision.execution_reason or decision.subscription_reason)
        return replace(
            decision,
            subscription_eligible=True,
            execution_eligible=False,
            market_state="STALE",
            subscription_reason=reason,
            execution_reason=reason,
            book_quality="STALE",
        )
    return decision


def _trusted_open_reopen_reason(decision: UniverseDecision) -> str | None:
    token = decision.token
    source = str(token.source or "").strip().lower()
    if source not in {"gamma_api_open_book", "clob_markets_api"}:
        return None
    if (
        not token.status_present
        or not token.active
        or token.closed
        or token.resolved
        or token.archived
        or token.deprecated
    ):
        return None
    prior_reason = str(decision.execution_reason or decision.subscription_reason or "metadata_ready")
    return f"explicit_reopen:{source}:{prior_reason}"


def _prefixed_reason(prefix: str, reason: str | None) -> str:
    text = str(reason or "").strip()
    if not text:
        return prefix
    if text.startswith(prefix + ":"):
        return text
    return f"{prefix}:{text}"


def _dominant_market_state(current: str, new: str) -> str:
    priorities = {
        "RESOLVED": 90,
        "ARCHIVED": 85,
        "CLOSING": 80,
        "STALE": 70,
        "LIVE": 60,
        "TRADABLE_PENDING_BOOK": 50,
        "DISCOVERED_PENDING_BOOK": 45,
        "NO_CLOB_BOOK": 45,
        "BOOK_PROBE_ERROR": 45,
        "DISCOVERED": 40,
    }
    return new if priorities.get(new, 0) > priorities.get(current, 0) else current


def _probe_event_type(probe: BookProbeResult) -> str:
    if probe.ok:
        return "BOOK_READY"
    status = str(probe.book_status or "").lower()
    quality = str(probe.book_quality or "").upper()
    if status == "gap" or quality == "GAP":
        return "BOOK_GAP"
    if status in {"stale", "disconnected"} or quality in {"STALE", "DISCONNECTED"}:
        return "BOOK_STALE"
    return "BOOK_PROBE_REQUESTED"


def _lifecycle_event_types(
    previous: Mapping[str, Any] | None,
    decision: UniverseDecision,
    *,
    state_changed: bool,
    execution_changed: bool,
) -> list[str]:
    if previous is None:
        return ["MARKET_DISCOVERED", "TOKEN_ADDED"]
    if state_changed:
        new_state = str(decision.market_state or "").upper()
        if new_state == "LIVE":
            return ["BOOK_READY"]
        if new_state == "STALE":
            return ["BOOK_STALE"]
        if new_state == "CLOSING":
            return ["MARKET_CLOSED"]
        if new_state == "RESOLVED":
            return ["MARKET_RESOLVED"]
        if new_state == "ARCHIVED":
            return ["MARKET_ARCHIVED"]
        if new_state in {"DISCOVERED", "DISCOVERED_PENDING_BOOK", "TRADABLE_PENDING_BOOK"}:
            return ["MARKET_METADATA_UPDATED"]
        return ["MARKET_METADATA_UPDATED"]
    if execution_changed:
        return ["BOOK_READY" if decision.execution_eligible else "BOOK_STALE"]
    return []
