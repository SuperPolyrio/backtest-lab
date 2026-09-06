"""Portable subscription snapshots for isolated L2 archive collectors."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable

from quant.core.db import postgres_connection

from .subscriptions import token_shard
from .l2_weighted_shard_planner import TokenLoad, plan_weighted_shards


SCHEMA_VERSION = "polymarket-l2-subscriptions-v1"
SHARDED_SCHEMA_VERSION = "polymarket-l2-subscription-shards-v1"
TOKEN_INDEX_SCHEMA_VERSION = "polymarket-l2-subscription-token-index-v1"
SUBSCRIPTION_SHARD_AFFINITY = "assigned_shard_id_fallback_condition_id"


@dataclass(frozen=True, slots=True)
class L2SubscriptionEntry:
    asset_id: str
    market_id: int
    condition_id: str
    market_slug: str | None
    market_state: str | None
    execution_eligible: bool
    book_status: str | None = None
    subscription_reason: str | None = None
    priority_at: str | None = None
    assigned_shard_id: int | None = None
    migration_from_shard_id: int | None = None
    migration_target_shard_id: int | None = None
    migration_valid_until: str | None = None

@dataclass(frozen=True, slots=True)
class L2SubscriptionSnapshot:
    generated_at: str
    content_sha256: str
    subscription_sha256: str
    entries: tuple[L2SubscriptionEntry, ...]
    global_token_count: int | None = None

    @property
    def token_count(self) -> int:
        return (
            int(self.global_token_count)
            if self.global_token_count is not None
            else len(self.entries)
        )

    def entries_for_shard(self, *, shard_id: int, shard_count: int) -> tuple[L2SubscriptionEntry, ...]:
        return tuple(
            entry
            for entry in self.entries
            if subscription_entry_shard(entry, shard_count=shard_count) == shard_id
        )


def subscription_entry_shard(
    entry: L2SubscriptionEntry,
    *,
    shard_count: int,
) -> int:
    """Use a published load-aware assignment, with stable hash fallback."""

    if entry.assigned_shard_id is not None:
        assigned = int(entry.assigned_shard_id)
        if not 0 <= assigned < shard_count:
            raise ValueError(
                f"assigned shard {assigned} is outside shard_count={shard_count}"
            )
        return assigned
    return subscription_affinity_shard(
        asset_id=entry.asset_id,
        condition_id=entry.condition_id,
        shard_count=shard_count,
    )


def subscription_affinity_shard(
    *,
    asset_id: str,
    condition_id: str | None,
    shard_count: int,
) -> int:
    """Resolve the logical connection shard for one subscription."""

    affinity_key = str(condition_id or "").strip() or str(asset_id)
    return token_shard(affinity_key, shard_count=shard_count)


def export_subscription_snapshot(
    path: Path,
    *,
    execution_only: bool = False,
    shard_dir: Path | None = None,
    shard_count: int | None = None,
    load_weights_path: Path | None = None,
    assignment_salt: str = "source-a",
    maximum_migration_ratio: float = 0.01,
    migration_overlap_seconds: float = 180.0,
    rebalance_threshold_ratio: float = 1.25,
    maximum_tokens_per_shard: int | None = None,
    worker_count: int | None = None,
    worker_rebalance_threshold_ratio: float = 1.05,
) -> L2SubscriptionSnapshot:
    with postgres_connection(readonly=True) as connection:
        # The full registry union is intentionally a single consistent
        # read.  On the production registry it builds multi-hundred-thousand
        # row hash tables; the generic 32 MB session default spills several
        # GiB into PostgreSQL's nearly-full data volume.  Scope the larger
        # budget to this one read-only transaction so no other query or
        # service inherits it.
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT set_config('work_mem', %s, true)",
                (
                    os.getenv(
                        "BOOK_L2_SUBSCRIPTION_EXPORT_WORK_MEM",
                        "4GB",
                    ),
                ),
            )
        entries = _load_snapshot_entries(connection)
    if execution_only:
        entries = tuple(entry for entry in entries if entry.execution_eligible)
    snapshot = write_subscription_snapshot(path, entries)
    if shard_dir is not None:
        if shard_count is None or shard_count <= 0:
            raise ValueError("shard_count must be positive when shard_dir is set")
        export_sharded_subscription_snapshot(
            shard_dir,
            snapshot=snapshot,
            shard_count=shard_count,
            loads=_load_token_loads(load_weights_path),
            assignment_salt=assignment_salt,
            maximum_migration_ratio=maximum_migration_ratio,
            migration_overlap_seconds=migration_overlap_seconds,
            rebalance_threshold_ratio=rebalance_threshold_ratio,
            maximum_tokens_per_shard=maximum_tokens_per_shard,
            worker_count=worker_count,
            worker_rebalance_threshold_ratio=worker_rebalance_threshold_ratio,
        )
    return snapshot


def export_additive_subscription_snapshot(
    source: Path,
    path: Path,
    *,
    native_discovery_log: Path,
    maximum_universe_age_seconds: float = 7200.0,
    retained_asset_ids: set[str] | None = None,
    retire_market_states: set[str] | None = None,
    authoritative_membership: bool = False,
) -> L2SubscriptionSnapshot:
    """Update a known-good universe without running the multi-million-row join.

    This is the acquisition-safe control-plane path used while the registry's
    full cleanup projection is I/O bound. Optional retirement only removes
    explicitly inactive market states after preserving execution and recent
    first-party L2 activity evidence; current registry rows cannot immediately
    re-add those retired assets.
    """

    if maximum_universe_age_seconds <= 0:
        raise ValueError("maximum_universe_age_seconds must be positive")
    base = load_subscription_snapshot(source)
    closing_grace_seconds = max(
        1,
        int(
            os.getenv(
                "BOOK_L2_TERMINAL_SUBSCRIPTION_GRACE_SECONDS",
                "21600",
            )
        ),
    )
    # The additive export can spend several minutes loading and enriching a
    # large generation.  Remember the start of that work so a fresh
    # transaction can pick up positive registry changes committed while the
    # first snapshot was being assembled.  Absence in the tail read is never
    # retirement evidence.
    registry_overlay_started_at = datetime.now(timezone.utc)
    with postgres_connection(readonly=True) as connection, connection.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_ts, subscription_asset_ids, execution_asset_ids
            FROM quant.paper_token_universe_snapshots
            ORDER BY generation DESC
            LIMIT 1
            """
        )
        row = cur.fetchone() or {}
        snapshot_ts = row.get("snapshot_ts")
        if snapshot_ts is not None and snapshot_ts.tzinfo is None:
            snapshot_ts = snapshot_ts.replace(tzinfo=timezone.utc)
        universe_age = (
            (datetime.now(timezone.utc) - snapshot_ts).total_seconds()
            if snapshot_ts is not None
            else float("inf")
        )
        snapshot_is_fresh = (
            snapshot_ts is not None
            and 0 <= universe_age <= float(maximum_universe_age_seconds)
        )
        recent_closing_entries = _load_recent_closing_entries(
            connection,
            grace_seconds=closing_grace_seconds,
        )
        registry_retired_asset_ids = _load_registry_retired_asset_ids(
            connection,
            retire_market_states=retire_market_states or set(),
            updated_since=snapshot_ts if snapshot_is_fresh else None,
        )
        # The immutable universe snapshot owns the full base. Native WS
        # discovery catches brand-new markets, but it does not cover every
        # lifecycle transition (for example an existing market being reopened
        # or becoming book-eligible). Overlay only positive registry changes
        # committed after the universe snapshot. This keeps the query bounded
        # by the indexed update window while preventing eligible tokens from
        # waiting for the next two-hour universe build. Absence from this
        # overlay is never deletion evidence.
        (
            current_registry_subscription_asset_ids,
            current_registry_execution_asset_ids,
        ) = _load_current_registry_asset_sets(
            connection,
            # The periodic immutable array is only an optimization.  LOB
            # membership ownership belongs to the continuously maintained
            # Registry rows, so a missing/stale array must fall back to one
            # current positive-eligibility scan instead of blocking every new
            # token publication.  With a fresh array this remains the cheap
            # indexed delta read used by the ordinary five-minute cycle.
            # The periodic array is a useful fast base, but it is not a
            # complete owner view: incremental registry decisions can make a
            # token subscription-eligible without rebuilding that array.  An
            # authoritative publication must therefore scan every current
            # positive registry row before it is allowed to retire members.
            # The table predicate is indexed and is far cheaper than missing
            # reopened/pending-book markets until the next full projection.
            updated_since=(
                None
                if authoritative_membership
                else (snapshot_ts if snapshot_is_fresh else None)
            ),
        )

    snapshot_execution_asset_ids = {
        str(asset_id).strip()
        for asset_id in row.get("execution_asset_ids") or ()
        if str(asset_id).strip()
    }
    # Membership is additive, but execution priority is not.  Keeping a token
    # in the capture universe after it stops being execution eligible is safe;
    # keeping its old execution flag is not.  It causes reconnect initial-dump
    # storms and forces Source B to retain an ever-growing false priority set.
    # An authoritative publication already scans every current positive
    # Registry row, so that indexed current set owns both promotion and
    # demotion.  The periodic array remains a fail-safe base only for the
    # non-authoritative incremental path.
    execution_asset_ids = (
        current_registry_execution_asset_ids
        if authoritative_membership
        else snapshot_execution_asset_ids
        | current_registry_execution_asset_ids
    )
    required_asset_ids = {
        str(asset_id).strip()
        for asset_id in (retained_asset_ids or set())
        if str(asset_id).strip()
    }
    reconciled_base_entries = _reconcile_execution_eligibility(
        base.entries,
        execution_asset_ids=execution_asset_ids,
    )
    retained_entries, retired_asset_ids = _retire_inactive_entries(
        reconciled_base_entries,
        execution_asset_ids=execution_asset_ids,
        retained_asset_ids=required_asset_ids,
        retire_market_states=retire_market_states or set(),
    )
    # The additive base intentionally contains only the last published live
    # generation.  Once a terminal token is removed it is no longer present
    # in that base, so relying only on _retire_inactive_entries loses the
    # tombstone and the registry array can re-add it as an untyped
    # DISCOVERED_PENDING_BOOK row on the next cycle.  Reapply current owner
    # registry terminal-state evidence before augmenting missing IDs.  A bare
    # deprecated flag is intentionally not a deletion source: without an
    # indexed terminal lifecycle state, acquisition keeps the token.
    retired_asset_ids.update(
        registry_retired_asset_ids
        - execution_asset_ids
        - required_asset_ids
    )
    snapshot_subscription_asset_ids = {
        str(asset_id).strip()
        for asset_id in row.get("subscription_asset_ids") or ()
        if str(asset_id).strip()
    }
    registry_asset_ids = current_registry_subscription_asset_ids | (
        snapshot_subscription_asset_ids if snapshot_is_fresh else set()
    )
    if authoritative_membership:
        retained_entries, authoritative_retired = (
            _restrict_to_authoritative_membership(
                retained_entries,
                subscription_asset_ids=registry_asset_ids,
                execution_asset_ids=execution_asset_ids,
                retained_asset_ids=required_asset_ids,
            )
        )
        retired_asset_ids.update(authoritative_retired)
    base_by_asset = {entry.asset_id: entry for entry in retained_entries}
    registry_metadata_asset_ids = {
        asset_id
        for asset_id in registry_asset_ids
        if (
            asset_id not in base_by_asset
            or not str(base_by_asset[asset_id].condition_id or "").strip()
        )
    }
    with postgres_connection(readonly=True) as connection:
        current_registry_entries = _load_registry_entries_by_asset_ids(
            connection,
            asset_ids=registry_metadata_asset_ids,
        )
    entries = _augment_subscription_entries(
        retained_entries,
        subscription_asset_ids=registry_asset_ids | required_asset_ids,
        execution_asset_ids=execution_asset_ids,
        registry_entries=current_registry_entries,
        native_discovery_log=native_discovery_log,
        discovery_start_ns=(
            int(snapshot_ts.timestamp() * 1_000_000_000)
            if authoritative_membership and snapshot_is_fresh
            else max(
                _isoformat_epoch_ns(base.generated_at),
                (
                    int(snapshot_ts.timestamp() * 1_000_000_000)
                    if snapshot_ts is not None
                    else 0
                ),
            )
        ),
        excluded_asset_ids=retired_asset_ids,
    )
    entries = _merge_recent_closing_entries(
        entries,
        recent_closing_entries,
    )

    # Close the moving-target window before publishing shards.  A second
    # indexed positive-only read is cheap because it is bounded by the export
    # start timestamp, yet it prevents a reopen or newly eligible token from
    # waiting an entire control-plane cycle.  A current positive row also
    # supersedes a terminal tombstone observed by the earlier transaction.
    with postgres_connection(readonly=True) as connection:
        (
            late_registry_subscription_asset_ids,
            late_registry_execution_asset_ids,
        ) = _load_current_registry_asset_sets(
            connection,
            updated_since=registry_overlay_started_at,
        )
        late_registry_asset_ids = (
            late_registry_subscription_asset_ids
            | late_registry_execution_asset_ids
        )
        late_registry_entries = _load_registry_entries_by_asset_ids(
            connection,
            asset_ids=late_registry_asset_ids,
        )
    if late_registry_asset_ids:
        retired_asset_ids.difference_update(late_registry_asset_ids)
        execution_asset_ids.update(late_registry_execution_asset_ids)
        entries = _augment_subscription_entries(
            entries,
            subscription_asset_ids=(
                late_registry_subscription_asset_ids | required_asset_ids
            ),
            execution_asset_ids=execution_asset_ids,
            registry_entries=late_registry_entries,
            native_discovery_log=native_discovery_log,
            discovery_start_ns=(
                int(snapshot_ts.timestamp() * 1_000_000_000)
                if authoritative_membership and snapshot_is_fresh
                else max(
                    _isoformat_epoch_ns(base.generated_at),
                    (
                        int(snapshot_ts.timestamp() * 1_000_000_000)
                        if snapshot_ts is not None
                        else 0
                    ),
                )
            ),
            excluded_asset_ids=retired_asset_ids,
        )
        entries = _merge_recent_closing_entries(
            entries,
            recent_closing_entries,
        )
    if authoritative_membership:
        # Close the negative-priority race as well.  The positive delta read
        # above catches new members, while this small indexed set prevents a
        # demotion committed during export from surviving for another control
        # generation.  Capture membership remains unchanged.
        with postgres_connection(readonly=True) as connection:
            final_execution_asset_ids = (
                _load_current_registry_execution_asset_ids(connection)
            )
        entries = _reconcile_execution_eligibility(
            entries,
            execution_asset_ids=final_execution_asset_ids,
        )
    return write_subscription_snapshot(path, entries)


def _restrict_to_authoritative_membership(
    entries: Iterable[L2SubscriptionEntry],
    *,
    subscription_asset_ids: set[str],
    execution_asset_ids: set[str],
    retained_asset_ids: set[str],
) -> tuple[tuple[L2SubscriptionEntry, ...], set[str]]:
    """Drop stale additive members absent from two fresh first-party views.

    ``subscription_asset_ids`` is the union of the bounded immutable universe
    snapshot and the current Registry positive-eligibility rows.  Requiring an
    asset to be absent from both avoids turning a lagging projection into a
    delete source.  Only current execution membership is protected; a
    previously published execution flag is metadata, not durable ownership.
    """

    authoritative = {
        str(asset_id).strip()
        for asset_id in subscription_asset_ids
        if str(asset_id).strip()
    }
    protected = authoritative | execution_asset_ids | retained_asset_ids
    kept: list[L2SubscriptionEntry] = []
    retired: set[str] = set()
    for entry in entries:
        if entry.asset_id in protected:
            kept.append(entry)
        else:
            retired.add(entry.asset_id)
    return tuple(kept), retired


def _load_registry_retired_asset_ids(
    connection: Any,
    *,
    retire_market_states: set[str],
    updated_since: datetime | None = None,
) -> set[str]:
    """Load durable owner retirement tombstones for additive publication."""

    states = sorted(
        {
            str(value).strip().upper()
            for value in retire_market_states
            if str(value).strip()
        }
    )
    if not states:
        return set()
    with connection.cursor() as cur:
        query = """
            SELECT asset_id
            FROM quant.paper_market_registry_tokens
            WHERE market_state = ANY(%s::text[])
            """
        parameters: tuple[Any, ...] = (states,)
        if updated_since is not None:
            # The fresh immutable universe snapshot already owns all older
            # membership. Only terminal transitions after that snapshot can
            # otherwise be re-added by its arrays. The state/updated_at index
            # makes this a bounded delta rather than a multi-million-row scan.
            query += " AND updated_at >= %s"
            parameters += (updated_since,)
        cur.execute(query, parameters)
        rows = cur.fetchall()
    return {
        str(row.get("asset_id") or "").strip()
        for row in rows
        if str(row.get("asset_id") or "").strip()
    }


def _load_current_registry_asset_sets(
    connection: Any,
    *,
    updated_since: datetime | None = None,
) -> tuple[set[str], set[str]]:
    """Overlay current first-party eligibility without making it a delete source.

    The immutable universe snapshot is intentionally periodic, while market
    discovery and lifecycle rows update continuously.  Reading current positive
    eligibility here prevents a newly active token from waiting for the next
    full snapshot.  Absence from this query never removes an existing token;
    retirement remains owned by explicit terminal-state evidence.  Keep this
    scan narrow; full metadata is fetched by primary key only for new or
    previously untyped entries.
    """

    with connection.cursor() as cursor:
        query = """
            SELECT asset_id, subscription_eligible, desired_subscribed,
                   execution_eligible
            FROM quant.paper_market_registry_tokens
            WHERE subscription_eligible = TRUE
               OR desired_subscribed = TRUE
               OR execution_eligible = TRUE
            """
        parameters: tuple[Any, ...] = ()
        if updated_since is not None:
            # The immutable snapshot supplies the full base. This overlay is
            # only for eligibility changes that landed after its consistent
            # read, so do not retransmit the entire 500k-row registry every
            # five minutes.
            query = f"SELECT * FROM ({query}) AS eligible WHERE updated_at >= %s"
            # ``updated_at`` must be projected for the outer predicate.
            query = query.replace(
                "execution_eligible\n            FROM",
                "execution_eligible, updated_at\n            FROM",
                1,
            )
            parameters = (updated_since,)
        cursor.execute(query, parameters)
        rows = cursor.fetchall()
    subscription_asset_ids = {
        str(row.get("asset_id") or "").strip()
        for row in rows
        if str(row.get("asset_id") or "").strip()
        and (bool(row.get("subscription_eligible")) or bool(row.get("desired_subscribed")))
    }
    execution_asset_ids = {
        str(row.get("asset_id") or "").strip()
        for row in rows
        if str(row.get("asset_id") or "").strip()
        and bool(row.get("execution_eligible"))
    }
    return subscription_asset_ids, execution_asset_ids


def _load_current_registry_execution_asset_ids(connection: Any) -> set[str]:
    """Load the small execution-priority overlay through its leading index."""

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT asset_id
            FROM quant.paper_market_registry_tokens
            WHERE execution_eligible = TRUE
            """
        )
        rows = cursor.fetchall()
    return {
        str(row.get("asset_id") or "").strip()
        for row in rows
        if str(row.get("asset_id") or "").strip()
    }


def _load_registry_entries_by_asset_ids(
    connection: Any,
    *,
    asset_ids: Iterable[str],
) -> tuple[L2SubscriptionEntry, ...]:
    """Load market identity only for additive rows that still need it."""

    normalized = sorted(
        {
            str(asset_id).strip()
            for asset_id in asset_ids
            if str(asset_id).strip()
        }
    )
    if not normalized:
        return ()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT asset_id,
                   COALESCE(market_id, 0) AS market_id,
                   COALESCE(condition_id, '') AS condition_id,
                   market_slug,
                   market_state,
                   execution_eligible,
                   book_status,
                   subscription_reason,
                   GREATEST(
                       COALESCE(last_transition_at, 'epoch'::timestamptz),
                       COALESCE(latest_book_at, 'epoch'::timestamptz),
                       COALESCE(updated_at, 'epoch'::timestamptz)
                   ) AS priority_at
            FROM quant.paper_market_registry_tokens
            WHERE asset_id = ANY(%s::text[])
            """,
            (normalized,),
        )
        rows = cursor.fetchall()
    return tuple(
        _entry_from_payload(dict(row))
        for row in rows
        if str(row.get("asset_id") or "").strip()
    )


def _load_recent_closing_entries(
    connection: Any,
    *,
    grace_seconds: int,
) -> tuple[L2SubscriptionEntry, ...]:
    """Load only CLOSING tokens with recent registry or L2 activity."""

    if grace_seconds < 1:
        raise ValueError("closing grace_seconds must be positive")
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT r.asset_id,
                   COALESCE(r.market_id, 0) AS market_id,
                   COALESCE(r.condition_id, '') AS condition_id,
                   r.market_slug,
                   'CLOSING' AS market_state,
                   FALSE AS execution_eligible,
                   r.book_status,
                   COALESCE(
                       r.subscription_reason,
                       'recent_closing_l2_activity_grace'
                   ) AS subscription_reason,
                   GREATEST(
                       COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                       COALESCE(r.latest_book_at, 'epoch'::timestamptz)
                   ) AS priority_at
            FROM quant.paper_market_registry_tokens r
            WHERE r.market_state = 'CLOSING'
              AND COALESCE(r.deprecated, FALSE) = FALSE
              -- A routine Registry refresh updates row metadata even after a
              -- market has closed. It is not LOB activity and must not renew
              -- the capture lease forever. Only a real lifecycle transition
              -- or a recent book observation earns the bounded lookahead.
              AND GREATEST(
                    COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                    COALESCE(r.latest_book_at, 'epoch'::timestamptz)
                  ) >= now() - (%s * INTERVAL '1 second')
            """,
            (int(grace_seconds),),
        )
        rows = [dict(row) for row in cur.fetchall()]
    return tuple(_entry_from_payload(row) for row in rows)


def _merge_recent_closing_entries(
    entries: Iterable[L2SubscriptionEntry],
    recent_closing_entries: Iterable[L2SubscriptionEntry],
) -> tuple[L2SubscriptionEntry, ...]:
    """Restore bounded event-active CLOSING rows as capture-only tokens."""

    merged = {entry.asset_id: entry for entry in entries}
    for entry in recent_closing_entries:
        merged[entry.asset_id] = replace(
            entry,
            market_state="CLOSING",
            execution_eligible=False,
        )
    return tuple(sorted(merged.values(), key=lambda entry: entry.asset_id))


def _retire_inactive_entries(
    entries: Iterable[L2SubscriptionEntry],
    *,
    execution_asset_ids: set[str],
    retained_asset_ids: set[str],
    retire_market_states: set[str],
) -> tuple[tuple[L2SubscriptionEntry, ...], set[str]]:
    states = {str(value).strip().upper() for value in retire_market_states}
    kept: list[L2SubscriptionEntry] = []
    retired: set[str] = set()
    for entry in entries:
        state = str(entry.market_state or "").strip().upper()
        if (
            state in states
            and entry.asset_id not in execution_asset_ids
            and entry.asset_id not in retained_asset_ids
        ):
            retired.add(entry.asset_id)
        else:
            kept.append(entry)
    return tuple(kept), retired


def _reconcile_execution_eligibility(
    entries: Iterable[L2SubscriptionEntry],
    *,
    execution_asset_ids: Iterable[str],
) -> tuple[L2SubscriptionEntry, ...]:
    """Apply the current execution-priority set without changing membership."""

    execution_ids = {
        str(asset_id).strip()
        for asset_id in execution_asset_ids
        if str(asset_id).strip()
    }
    return tuple(
        replace(
            entry,
            execution_eligible=entry.asset_id in execution_ids,
        )
        if entry.execution_eligible != (entry.asset_id in execution_ids)
        else entry
        for entry in entries
    )


def _augment_subscription_entries(
    base_entries: Iterable[L2SubscriptionEntry],
    *,
    subscription_asset_ids: Iterable[str],
    execution_asset_ids: Iterable[str],
    registry_entries: Iterable[L2SubscriptionEntry] = (),
    native_discovery_log: Path,
    discovery_start_ns: int,
    excluded_asset_ids: set[str] | None = None,
) -> tuple[L2SubscriptionEntry, ...]:
    base_entries = tuple(base_entries)
    registry_by_asset = {
        entry.asset_id: entry
        for entry in registry_entries
        if str(entry.asset_id).strip()
    }
    excluded_asset_ids = excluded_asset_ids or set()
    execution_ids = {
        str(asset_id).strip()
        for asset_id in execution_asset_ids
        if str(asset_id).strip()
    }
    entries = {
        entry.asset_id: entry
        for entry in _reconcile_execution_eligibility(
            base_entries,
            execution_asset_ids=execution_ids,
        )
    }
    for raw_asset_id in subscription_asset_ids:
        asset_id = str(raw_asset_id).strip()
        if not asset_id or asset_id in excluded_asset_ids:
            continue
        current = entries.get(asset_id)
        registry = registry_by_asset.get(asset_id)
        if current is not None:
            if registry is not None:
                entries[asset_id] = replace(
                    current,
                    market_id=registry.market_id or current.market_id,
                    condition_id=registry.condition_id or current.condition_id,
                    market_slug=registry.market_slug or current.market_slug,
                    market_state=registry.market_state or current.market_state,
                    execution_eligible=asset_id in execution_ids,
                    book_status=registry.book_status or current.book_status,
                    subscription_reason=(
                        registry.subscription_reason
                        or current.subscription_reason
                    ),
                    priority_at=registry.priority_at or current.priority_at,
                )
            elif current.execution_eligible != (asset_id in execution_ids):
                entries[asset_id] = replace(
                    current,
                    execution_eligible=asset_id in execution_ids,
                )
            continue
        if registry is not None:
            entries[asset_id] = replace(
                registry,
                market_state=(
                    registry.market_state or "DISCOVERED_PENDING_BOOK"
                ),
                execution_eligible=asset_id in execution_ids,
                subscription_reason=(
                    registry.subscription_reason
                    or "registry_current_additive_overlay"
                ),
            )
        else:
            entries[asset_id] = L2SubscriptionEntry(
                asset_id=asset_id,
                market_id=0,
                condition_id="",
                market_slug=None,
                market_state="DISCOVERED_PENDING_BOOK",
                execution_eligible=asset_id in execution_ids,
                subscription_reason="registry_universe_additive_fallback",
            )

    try:
        source = native_discovery_log.open("r", encoding="utf-8")
    except FileNotFoundError as exc:
        raise ValueError("native discovery log is unavailable") from exc
    with source:
        for line in source:
            try:
                row = json.loads(line)
                observed_ns = int(row["discovered_wall_ns"])
                if (
                    row.get("schema_version")
                    != "polymarket-l2-native-discovery-log-v1"
                    or observed_ns < discovery_start_ns
                ):
                    continue
                entry = _entry_from_payload(row)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
            previous = entries.get(entry.asset_id)
            if previous is None:
                entries[entry.asset_id] = replace(
                    entry,
                    execution_eligible=entry.asset_id in execution_ids,
                )
            elif entry.asset_id in execution_ids and not previous.execution_eligible:
                entries[entry.asset_id] = replace(
                    previous,
                    execution_eligible=True,
                )

    return tuple(sorted(entries.values(), key=lambda entry: entry.asset_id))


def _isoformat_epoch_ns(value: str) -> int:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("subscription snapshot generated_at is invalid") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1_000_000_000)


def write_subscription_snapshot(
    path: Path,
    entries: Iterable[L2SubscriptionEntry],
    *,
    generated_at: str | None = None,
) -> L2SubscriptionSnapshot:
    """Write a validated portable snapshot from an explicit bounded universe."""

    normalized = tuple(entries)
    if len({entry.asset_id for entry in normalized}) != len(normalized):
        raise ValueError("subscription snapshot contains duplicate asset ids")
    observed_at = generated_at or datetime.now(timezone.utc).isoformat()
    content_sha256 = _entries_sha256(normalized)
    subscription_sha256 = _subscription_sha256(normalized)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": observed_at,
        "token_count": len(normalized),
        "content_sha256": content_sha256,
        "subscription_sha256": subscription_sha256,
        "tokens": [_snapshot_entry_payload(entry) for entry in normalized],
    }
    _atomic_write_json(path, payload)
    return L2SubscriptionSnapshot(
        generated_at=observed_at,
        content_sha256=content_sha256,
        subscription_sha256=subscription_sha256,
        entries=normalized,
    )


def export_subscription_subset(
    source: Path,
    output: Path,
    *,
    limit: int,
) -> L2SubscriptionSnapshot:
    """Create a deterministic execution-first canary universe."""

    if limit < 1:
        raise ValueError("subscription subset limit must be positive")
    snapshot = load_subscription_snapshot(source)
    entries = tuple(
        sorted(
            snapshot.entries,
            key=lambda entry: (
                not entry.execution_eligible,
                entry.condition_id,
                entry.asset_id,
            ),
        )[: int(limit)]
    )
    return write_subscription_snapshot(output, entries)


def export_execution_subscription_subset(
    source: Path,
    output: Path,
) -> L2SubscriptionSnapshot:
    """Derive the exact execution subset from an atomic full snapshot."""

    snapshot = load_subscription_snapshot(source)
    return write_subscription_snapshot(
        output,
        (entry for entry in snapshot.entries if entry.execution_eligible),
        generated_at=snapshot.generated_at,
    )


def export_unassigned_sharded_subscription_snapshot(
    root: Path,
    output: Path,
    *,
    expected_shard_count: int,
) -> L2SubscriptionSnapshot:
    """Recover the unique global membership from a validated shard generation."""

    loaded = load_sharded_subscription_snapshot(
        root,
        shard_ids=range(expected_shard_count),
        expected_shard_count=expected_shard_count,
    )
    by_asset: dict[str, L2SubscriptionEntry] = {}
    for entry in loaded.entries:
        unassigned = replace(
            entry,
            assigned_shard_id=None,
            migration_from_shard_id=None,
            migration_target_shard_id=None,
            migration_valid_until=None,
        )
        previous = by_asset.get(entry.asset_id)
        if previous is not None and previous != unassigned:
            raise ValueError(
                f"sharded membership metadata differs for asset {entry.asset_id}"
            )
        by_asset[entry.asset_id] = unassigned
    if len(by_asset) != loaded.token_count:
        raise ValueError(
            "sharded global token count mismatch: "
            f"{len(by_asset)} != {loaded.token_count}"
        )
    return write_subscription_snapshot(
        output,
        by_asset.values(),
        generated_at=loaded.generated_at,
    )


def stage_subscription_transition(
    previous_path: Path,
    desired_path: Path,
    output_path: Path,
    *,
    max_added_tokens: int,
    max_removed_tokens: int,
    priority_asset_ids: set[str] | None = None,
) -> L2SubscriptionSnapshot:
    """Publish a bounded step toward a rapidly changing live universe.

    A control-plane outage can leave the active generation many hours behind.
    Publishing the entire accumulated membership delta at once would turn an
    otherwise healthy collector into a large subscribe/unsubscribe and initial
    snapshot storm.  This function keeps the desired metadata for retained
    tokens, prioritizes new execution/LIVE tokens and recently closed grace
    tokens, and advances additions and removals independently in deterministic
    bounded batches.  Deferred removals remain subscribed for one or more
    control-plane cycles; they never make a missing desired token look covered.
    """

    if max_added_tokens < 0:
        raise ValueError("max_added_tokens must be non-negative")
    if max_removed_tokens < 0:
        raise ValueError("max_removed_tokens must be non-negative")
    previous = load_subscription_snapshot(previous_path)
    desired = load_subscription_snapshot(desired_path)
    previous_by_asset = {entry.asset_id: entry for entry in previous.entries}
    desired_by_asset = {entry.asset_id: entry for entry in desired.entries}
    previous_ids = set(previous_by_asset)
    desired_ids = set(desired_by_asset)
    priority_asset_ids = priority_asset_ids or set()

    added = sorted(
        desired_ids - previous_ids,
        key=lambda asset_id: _subscription_transition_add_priority_with_evidence(
            desired_by_asset[asset_id],
            asset_id in priority_asset_ids,
        ),
    )
    removed = sorted(
        previous_ids - desired_ids,
        key=lambda asset_id: _subscription_transition_remove_priority(
            previous_by_asset[asset_id]
        ),
    )
    selected_added = set(added[: int(max_added_tokens)])
    selected_removed = set(removed[: int(max_removed_tokens)])
    candidate_ids = (
        (previous_ids - selected_removed)
        | (previous_ids & desired_ids)
        | selected_added
    )
    entries = tuple(
        replace(
            (
                desired_by_asset[asset_id]
                if asset_id in desired_by_asset
                else replace(
                    previous_by_asset[asset_id],
                    # Membership retirement may be deliberately deferred
                    # while the collector is degraded, but execution
                    # eligibility is an authoritative current-registry flag.
                    # Keeping that flag sticky made thousands of retired
                    # tokens occupy Source-B and REST-baseline priority lanes
                    # even though their capture membership was retained only
                    # as a safety buffer.  Demotion changes no WS membership
                    # and is therefore safe in the additions-only path.
                    execution_eligible=False,
                )
            ),
            # Activity time is only needed while choosing this bounded add
            # batch. Do not carry volatile timestamps for hundreds of
            # thousands of retained tokens into the active control snapshot.
            priority_at=(
                desired_by_asset[asset_id].priority_at
                if asset_id in selected_added
                else None
            ),
        )
        for asset_id in sorted(candidate_ids)
    )
    return write_subscription_snapshot(output_path, entries)


def _subscription_transition_add_priority(
    entry: L2SubscriptionEntry,
) -> tuple[int, int, int, int, int, str]:
    state = str(entry.market_state or "").upper()
    book_status = str(entry.book_status or "").lower()
    state_rank = {
        "LIVE": 0,
        "TRADABLE": 0,
        "TRADABLE_PENDING_BOOK": 1,
        # Native lifecycle discovery and the registry status snapshot advance
        # independently.  Capture-only subscriptions for newly discovered
        # active markets must arrive before terminal/history backlog.
        "DISCOVERED": 1,
        "DISCOVERED_PENDING_BOOK": 1,
        # A market awaiting resolution can continue emitting real book and
        # price-change traffic after metadata flips closed.  It must therefore
        # precede an already RESOLVED market even when the latter transitioned
        # more recently.  Both rows passed the bounded terminal-grace filter.
        "CLOSING": 2,
        "RESOLVED": 3,
        "STALE": 4,
        "ARCHIVED": 5,
    }.get(state, 6)
    book_rank = {
        "ok": 0,
        "one_sided": 1,
        "not_ready": 2,
        "empty": 3,
        "no-book": 4,
        "no_clob_book": 5,
    }.get(book_status, 6)
    return (
        0 if entry.execution_eligible else 1,
        state_rank,
        -_subscription_priority_timestamp_us(entry.priority_at),
        book_rank,
        -int(entry.market_id),
        entry.asset_id,
    )


def _subscription_transition_add_priority_with_evidence(
    entry: L2SubscriptionEntry,
    has_pmxt_gap_evidence: bool,
) -> tuple[int, int, int, int, int, int, str]:
    base = _subscription_transition_add_priority(entry)
    # Execution and genuinely active/recent markets stay ahead of an external
    # comparison hint. PMXT evidence only breaks ties inside the same lifecycle
    # and activity rank; it must never displace a newly opened live token with
    # an old CLOSING/STALE snapshot.
    return (
        base[0],
        base[1],
        base[2],
        0 if has_pmxt_gap_evidence else 1,
        *base[3:],
    )


def _load_priority_asset_ids(path: Path | None) -> set[str]:
    if path is None:
        return set()
    payload = json.loads(path.read_text(encoding="utf-8"))
    values = payload.get("asset_ids") if isinstance(payload, dict) else payload
    if not isinstance(values, list):
        raise ValueError("priority asset file must contain an asset_ids list")
    active_values = payload.get("active_asset_ids", []) if isinstance(payload, dict) else []
    if not isinstance(active_values, list):
        raise ValueError("priority active_asset_ids must be a list")
    return {
        str(value).strip()
        for value in (*values, *active_values)
        if str(value).strip()
    }


def _subscription_priority_timestamp_us(value: str | None) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1_000_000)
    except (OverflowError, ValueError):
        return 0


def _subscription_transition_remove_priority(
    entry: L2SubscriptionEntry,
) -> tuple[int, int, str]:
    state = str(entry.market_state or "").upper()
    return (
        0 if state in {"RESOLVED", "ARCHIVED", "CLOSING"} else 1,
        0 if entry.execution_eligible else 1,
        entry.asset_id,
    )


def export_sharded_subscription_snapshot(
    root: Path,
    *,
    snapshot: L2SubscriptionSnapshot,
    shard_count: int,
    loads: dict[str, TokenLoad] | None = None,
    assignment_salt: str = "source-a",
    maximum_migration_ratio: float = 0.01,
    migration_overlap_seconds: float = 180.0,
    rebalance_threshold_ratio: float = 1.25,
    maximum_tokens_per_shard: int | None = None,
    worker_count: int | None = None,
    worker_rebalance_threshold_ratio: float = 1.05,
    release_expired_migrations: bool = False,
) -> Path:
    """Write one immutable file per logical WS connection and atomically publish it."""

    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    assigned_entries = snapshot.entries
    plan = None
    if loads is not None:
        previous = _load_previous_assignments(root)
        active_migrations = _load_active_migrations(
            root,
            include_expired=not release_expired_migrations,
        )
        plan = plan_weighted_shards(
            snapshot.entries,
            shard_count=shard_count,
            loads=loads,
            previous=previous,
            salt=assignment_salt,
            maximum_migration_ratio=maximum_migration_ratio,
            rebalance_threshold_ratio=rebalance_threshold_ratio,
            maximum_tokens_per_shard=maximum_tokens_per_shard,
            worker_count=worker_count,
            worker_rebalance_threshold_ratio=worker_rebalance_threshold_ratio,
        )
        now = datetime.now(timezone.utc)
        planned: list[L2SubscriptionEntry] = []
        for entry in snapshot.entries:
            target = plan.assignments[entry.asset_id]
            migration = active_migrations.get(entry.asset_id)
            old = previous.get(entry.asset_id)
            if old is not None and old != target:
                migration = (
                    old,
                    target,
                    now + timedelta(seconds=float(migration_overlap_seconds)),
                )
            if migration is not None:
                source_shard, migration_target, valid_until = migration
                if valid_until <= now:
                    # Retaining the source copy renews the bounded overlap for
                    # older collector processes that still interpret the
                    # deadline locally.  A publisher may release it only after
                    # current target-shard health has been proved explicitly.
                    valid_until = now + timedelta(
                        seconds=float(migration_overlap_seconds)
                    )
                target = migration_target
                common = {
                    "migration_from_shard_id": source_shard,
                    "migration_target_shard_id": migration_target,
                    "migration_valid_until": valid_until.isoformat(),
                }
                planned.append(
                    replace(
                        entry,
                        assigned_shard_id=target,
                        **common,
                    )
                )
                planned.append(
                    replace(
                        entry,
                        assigned_shard_id=source_shard,
                        **common,
                    )
                )
            else:
                planned.append(
                    replace(
                        entry,
                        assigned_shard_id=target,
                    )
                )
        assigned_entries = tuple(planned)
    generation = _entries_sha256(assigned_entries)
    generations_dir = root / "generations"
    generation_dir = generations_dir / generation
    if not generation_dir.is_dir():
        tmp_dir = generations_dir / f".{generation}.{os.getpid()}.tmp"
        if tmp_dir.exists():
            raise FileExistsError(f"temporary shard generation already exists: {tmp_dir}")
        tmp_dir.mkdir(parents=True)
        by_shard: list[list[L2SubscriptionEntry]] = [
            [] for _ in range(shard_count)
        ]
        for entry in assigned_entries:
            by_shard[
                subscription_entry_shard(entry, shard_count=shard_count)
            ].append(entry)
        shard_manifest: dict[str, dict[str, Any]] = {}
        for shard_id, shard_entries_list in enumerate(by_shard):
            shard_entries = tuple(shard_entries_list)
            shard_sha256 = _entries_sha256(shard_entries)
            filename = f"shard-{shard_id:03d}.json"
            _atomic_write_json(
                tmp_dir / filename,
                {
                    "schema_version": SHARDED_SCHEMA_VERSION,
                    "generated_at": snapshot.generated_at,
                    "generation": generation,
                    "shard_id": shard_id,
                    "shard_count": shard_count,
                    "token_count": len(shard_entries),
                    "content_sha256": shard_sha256,
                    "tokens": [_entry_payload(entry) for entry in shard_entries],
                },
            )
            shard_manifest[str(shard_id)] = {
                "file": filename,
                "token_count": len(shard_entries),
                "content_sha256": shard_sha256,
            }
        _atomic_write_json(
            tmp_dir / "manifest.json",
            {
                "schema_version": SHARDED_SCHEMA_VERSION,
                "generated_at": snapshot.generated_at,
                "generation": generation,
                "token_count": snapshot.token_count,
                "assignment_count": len(assigned_entries),
                "content_sha256": generation,
                "unassigned_content_sha256": snapshot.content_sha256,
                "subscription_sha256": snapshot.subscription_sha256,
                "shard_count": shard_count,
                "shard_affinity": SUBSCRIPTION_SHARD_AFFINITY,
                "assignment_salt": assignment_salt if plan is not None else None,
                "load_aware": plan is not None,
                "moved_tokens": plan.moved_tokens if plan is not None else 0,
                "new_tokens": plan.new_tokens if plan is not None else 0,
                "maximum_to_median_ratio": (
                    plan.maximum_to_median_ratio if plan is not None else None
                ),
                "shard_weights": (
                    list(plan.shard_weights) if plan is not None else None
                ),
                "shard_token_counts": (
                    list(plan.shard_token_counts) if plan is not None else None
                ),
                "worker_count": worker_count if plan is not None else None,
                "worker_weights": (
                    list(plan.worker_weights) if plan is not None else None
                ),
                "worker_token_counts": (
                    list(plan.worker_token_counts) if plan is not None else None
                ),
                "maximum_tokens_per_shard": (
                    plan.maximum_tokens_per_shard if plan is not None else None
                ),
                "shards": shard_manifest,
            },
        )
        os.replace(tmp_dir, generation_dir)
    # A compact unique-token index lets independent fast-path overlays test
    # whether a newly discovered token has already reached this immutable
    # generation without loading all 48 shard payloads in every worker.
    token_ids = tuple(
        sorted({entry.asset_id for entry in snapshot.entries})
    )
    token_index_sha256 = _token_index_sha256(token_ids)
    token_index_path = generation_dir / "token-index.json"
    if not token_index_path.is_file():
        _atomic_write_json(
            token_index_path,
            {
                "schema_version": TOKEN_INDEX_SCHEMA_VERSION,
                "generation": generation,
                "token_count": len(token_ids),
                "content_sha256": token_index_sha256,
                "asset_ids": token_ids,
            },
        )
    _atomic_write_json(
        root / "current.json",
        {
            "schema_version": SHARDED_SCHEMA_VERSION,
            "generation": generation,
            "generated_at": snapshot.generated_at,
            "token_count": snapshot.token_count,
            "assignment_count": len(assigned_entries),
            "content_sha256": generation,
            "unassigned_content_sha256": snapshot.content_sha256,
            "subscription_sha256": snapshot.subscription_sha256,
            "token_index_sha256": token_index_sha256,
            "shard_count": shard_count,
        },
    )
    return generation_dir


def load_sharded_subscription_snapshot(
    root: Path,
    *,
    shard_ids: Iterable[int],
    expected_shard_count: int,
) -> L2SubscriptionSnapshot:
    """Load only the logical connection shards owned by one collector worker."""

    pointer = json.loads((root / "current.json").read_text(encoding="utf-8"))
    _validate_sharded_schema(pointer)
    generation = str(pointer.get("generation") or "")
    if (
        len(generation) != 64
        or any(character not in "0123456789abcdef" for character in generation)
    ):
        raise ValueError("invalid sharded subscription generation")
    generation_dir = root / "generations" / generation
    manifest = json.loads(
        (generation_dir / "manifest.json").read_text(encoding="utf-8")
    )
    _validate_sharded_schema(manifest)
    shard_count = int(manifest.get("shard_count") or 0)
    if shard_count != int(expected_shard_count):
        raise ValueError(
            f"subscription shard_count mismatch: {shard_count} != {expected_shard_count}"
        )
    if str(manifest.get("generation") or "") != generation:
        raise ValueError("subscription shard manifest generation mismatch")
    manifest_content = str(
        manifest.get("assigned_content_sha256")
        or manifest.get("content_sha256")
        or ""
    )
    if manifest_content != generation:
        raise ValueError("subscription shard manifest content hash mismatch")
    declared_shards = manifest.get("shards")
    if not isinstance(declared_shards, dict):
        raise ValueError("subscription shard manifest is missing shards")
    declared_global_count = int(manifest.get("token_count") or 0)
    manifest_assignment_count = sum(
        int(metadata.get("token_count") or 0)
        for metadata in declared_shards.values()
        if isinstance(metadata, dict)
    )
    declared_assignment_count = int(
        manifest.get("assignment_count") or declared_global_count
    )
    if manifest_assignment_count != declared_assignment_count:
        raise ValueError(
            "subscription shard manifest assignment_count mismatch: "
            f"{manifest_assignment_count} != {declared_assignment_count}"
        )

    entries: list[L2SubscriptionEntry] = []
    for shard_id in sorted({int(value) for value in shard_ids}):
        if not 0 <= shard_id < shard_count:
            raise ValueError(f"invalid requested subscription shard: {shard_id}")
        metadata = declared_shards.get(str(shard_id))
        if not isinstance(metadata, dict):
            raise ValueError(f"subscription shard {shard_id} is missing")
        expected_filename = f"shard-{shard_id:03d}.json"
        if str(metadata.get("file") or "") != expected_filename:
            raise ValueError(f"subscription shard {shard_id} filename mismatch")
        payload = json.loads(
            (generation_dir / expected_filename).read_text(encoding="utf-8")
        )
        _validate_sharded_schema(payload)
        if (
            int(payload.get("shard_id", -1)) != shard_id
            or int(payload.get("shard_count") or 0) != shard_count
            or str(payload.get("generation") or "") != generation
        ):
            raise ValueError(f"subscription shard {shard_id} identity mismatch")
        rows = payload.get("tokens")
        if not isinstance(rows, list):
            raise ValueError(f"subscription shard {shard_id} tokens must be a list")
        shard_entries = tuple(_entry_from_payload(row) for row in rows)
        if int(payload.get("token_count") or 0) != len(shard_entries):
            raise ValueError(f"subscription shard {shard_id} token_count mismatch")
        actual_sha256 = _entries_sha256(shard_entries)
        if actual_sha256 != str(payload.get("content_sha256") or ""):
            raise ValueError(f"subscription shard {shard_id} content hash mismatch")
        if actual_sha256 != str(metadata.get("content_sha256") or ""):
            raise ValueError(f"subscription shard {shard_id} manifest hash mismatch")
        entries.extend(shard_entries)
    _validate_migration_duplicates(entries)
    return L2SubscriptionSnapshot(
        generated_at=str(manifest.get("generated_at") or ""),
        content_sha256=generation,
        subscription_sha256=str(manifest.get("subscription_sha256") or ""),
        entries=tuple(entries),
        global_token_count=declared_global_count,
    )


def load_sharded_token_index(root: Path) -> tuple[str, frozenset[str]]:
    """Load and verify the unique asset index for the active generation."""

    pointer = json.loads((root / "current.json").read_text(encoding="utf-8"))
    _validate_sharded_schema(pointer)
    generation = str(pointer.get("generation") or "")
    if (
        len(generation) != 64
        or any(character not in "0123456789abcdef" for character in generation)
    ):
        raise ValueError("invalid sharded subscription generation")
    path = root / "generations" / generation / "token-index.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != TOKEN_INDEX_SCHEMA_VERSION
        or str(payload.get("generation") or "") != generation
    ):
        raise ValueError("subscription token index identity mismatch")
    values = payload.get("asset_ids")
    if not isinstance(values, list):
        raise ValueError("subscription token index asset_ids must be a list")
    asset_ids = tuple(str(value) for value in values)
    if (
        asset_ids != tuple(sorted(set(asset_ids)))
        or int(payload.get("token_count") or 0) != len(asset_ids)
        or int(pointer.get("token_count") or 0) != len(asset_ids)
        or str(payload.get("content_sha256") or "")
        != _token_index_sha256(asset_ids)
    ):
        raise ValueError("subscription token index validation failed")
    declared = str(pointer.get("token_index_sha256") or "")
    if declared and declared != str(payload.get("content_sha256") or ""):
        raise ValueError("subscription token index pointer hash mismatch")
    return generation, frozenset(asset_ids)


def ensure_sharded_token_index(root: Path) -> Path:
    """Backfill the deterministic token index for an older generation."""

    pointer = json.loads((root / "current.json").read_text(encoding="utf-8"))
    _validate_sharded_schema(pointer)
    generation = str(pointer.get("generation") or "")
    generation_dir = root / "generations" / generation
    manifest = json.loads(
        (generation_dir / "manifest.json").read_text(encoding="utf-8")
    )
    _validate_sharded_schema(manifest)
    shard_count = int(manifest.get("shard_count") or 0)
    asset_ids: set[str] = set()
    for shard_id in range(shard_count):
        payload = json.loads(
            (
                generation_dir / f"shard-{shard_id:03d}.json"
            ).read_text(encoding="utf-8")
        )
        if (
            str(payload.get("generation") or "") != generation
            or int(payload.get("shard_id", -1)) != shard_id
        ):
            raise ValueError(
                f"subscription shard {shard_id} identity mismatch"
            )
        rows = payload.get("tokens")
        if not isinstance(rows, list):
            raise ValueError(
                f"subscription shard {shard_id} tokens must be a list"
            )
        asset_ids.update(
            str(row.get("asset_id") or "")
            for row in rows
            if isinstance(row, dict) and str(row.get("asset_id") or "")
        )
    expected = int(manifest.get("token_count") or 0)
    if len(asset_ids) != expected:
        raise ValueError(
            f"subscription token index count mismatch: "
            f"{len(asset_ids)} != {expected}"
        )
    ordered = tuple(sorted(asset_ids))
    path = generation_dir / "token-index.json"
    _atomic_write_json(
        path,
        {
            "schema_version": TOKEN_INDEX_SCHEMA_VERSION,
            "generation": generation,
            "token_count": len(ordered),
            "content_sha256": _token_index_sha256(ordered),
            "asset_ids": ordered,
        },
    )
    return path


def _token_index_sha256(asset_ids: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for asset_id in asset_ids:
        digest.update(str(asset_id).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _validate_sharded_schema(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise ValueError("sharded subscription payload must be an object")
    if payload.get("schema_version") != SHARDED_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported sharded subscription schema: {payload.get('schema_version')!r}"
        )


def _load_snapshot_entries(connection: Any) -> tuple[L2SubscriptionEntry, ...]:
    """Read the portable subscription snapshot without the collector ranking query.

    The realtime reconciler sorts and ranks rows for bounded per-shard reads. A
    full portable export needs every desired asset and sorts once in Python, so
    its database query deliberately avoids COUNT OVER, ORDER BY, and a second
    expansion of the dynamic execution view.
    """

    execution_book_ttl_seconds = max(
        1,
        int(os.getenv("BOOK_L2_EXECUTION_BOOK_TTL_SECONDS", "900")),
    )
    terminal_subscription_grace_seconds = max(
        1,
        int(
            os.getenv(
                "BOOK_L2_TERMINAL_SUBSCRIPTION_GRACE_SECONDS",
                "21600",
            )
        ),
    )
    settled_subscription_grace_seconds = max(
        1,
        int(
            os.getenv(
                "BOOK_L2_SETTLED_SUBSCRIPTION_GRACE_SECONDS",
                "21600",
            )
        ),
    )
    discovered_subscription_grace_seconds = max(
        1,
        int(
            os.getenv(
                "BOOK_L2_DISCOVERED_SUBSCRIPTION_GRACE_SECONDS",
                "21600",
            )
        ),
    )
    with connection.cursor() as cur:
        # The caller scopes a large work_mem to this read-only transaction.
        # Avoid JIT startup for a one-shot hash export; no server-wide setting
        # is changed.
        cur.execute("SELECT set_config('jit', 'off', TRUE)")
        # Registry discovery and the reconciled target projection advance on
        # independent schedules.  Export their union so a newly discovered
        # OPEN token cannot disappear merely because the target row has not
        # been materialized yet.  Keep CLOSING tokens long enough to capture
        # late book traffic while an oracle result is pending, but retire
        # already RESOLVED/ARCHIVED tokens sooner so old terminal backlog
        # cannot crowd newly active markets out of bounded transition cycles.
        # Neither category becomes execution eligible here.
        cur.execute(
            """
            WITH subscription_grace AS (
                SELECT
                    %s::bigint AS closing_seconds,
                    %s::bigint AS settled_seconds
            )
            SELECT
                COALESCE(t.asset_id, r.asset_id) AS asset_id,
                COALESCE(r.market_id, t.market_id, 0) AS market_id,
                COALESCE(r.condition_id, t.condition_id, '') AS condition_id,
                r.market_slug,
                CASE
                    WHEN COALESCE(r.completion_status, '') = 'OPEN'
                     AND COALESCE(r.closed, FALSE) = FALSE
                     AND COALESCE(r.resolved, FALSE) = FALSE
                     AND (
                            COALESCE(r.archived, FALSE) = TRUE
                         OR COALESCE(r.market_state, '') IN ('RESOLVED', 'ARCHIVED')
                         )
                    THEN 'TRADABLE_PENDING_BOOK'
                    WHEN COALESCE(r.execution_eligible, FALSE) = TRUE
                     AND COALESCE(r.market_state, '') = 'LIVE'
                     AND (
                            r.latest_book_at IS NULL
                         OR r.latest_book_at
                            < now() - (%s * INTERVAL '1 second')
                         )
                    THEN 'STALE'
                    ELSE r.market_state
                END AS market_state,
                r.book_status,
                r.subscription_reason,
                GREATEST(
                    COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                    COALESCE(r.latest_book_at, 'epoch'::timestamptz),
                    COALESCE(t.last_book_update_at, 'epoch'::timestamptz),
                    COALESCE(t.last_l2_event_at, 'epoch'::timestamptz)
                ) AS priority_at,
                (
                    COALESCE(r.execution_eligible, FALSE)
                    AND r.market_state = 'LIVE'
                    -- Registry rows are a derived cache.  A stopped or
                    -- partitioned registry daemon must not leave yesterday's
                    -- book evidence execution-eligible forever.
                    AND r.latest_book_at IS NOT NULL
                    AND r.latest_book_at
                        >= now() - (%s * INTERVAL '1 second')
                    -- A recent book probe can temporarily lag Gamma/token
                    -- lifecycle metadata after a market closes. Keep the
                    -- token in the broad LOB universe, but never elevate a
                    -- metadata-proven inactive/closed token into execution
                    -- baseline priority.
                    AND (
                        m.token_id IS NULL
                        OR (
                            COALESCE(m.active, FALSE) = TRUE
                            AND COALESCE(m.closed, FALSE) = FALSE
                            AND COALESCE(m.archived, FALSE) = FALSE
                            AND COALESCE(m.deprecated, FALSE) = FALSE
                        )
                    )
                ) AS execution_eligible
            FROM quant.paper_market_registry_tokens r
            FULL OUTER JOIN quant.paper_lob_subscription_targets t
              ON t.asset_id = r.asset_id
            LEFT JOIN quant.market_token_metadata m
              ON m.token_id = COALESCE(t.asset_id, r.asset_id)
             AND COALESCE(r.execution_eligible, FALSE) = TRUE
             AND COALESCE(r.market_state, '') = 'LIVE'
             AND r.latest_book_at IS NOT NULL
             AND r.latest_book_at
                 >= now() - (%s * INTERVAL '1 second')
            CROSS JOIN subscription_grace g
            WHERE (
                    t.desired_subscribed = TRUE
                 OR COALESCE(r.subscription_eligible, FALSE) = TRUE
                 OR (
                        COALESCE(r.active, FALSE) = TRUE
                    AND COALESCE(r.closed, FALSE) = FALSE
                    AND COALESCE(r.resolved, FALSE) = FALSE
                    AND COALESCE(r.archived, FALSE) = FALSE
                    AND COALESCE(r.deprecated, FALSE) = FALSE
                    AND COALESCE(r.market_state, '') IN (
                        'DISCOVERED', 'DISCOVERED_PENDING_BOOK'
                    )
                    AND GREATEST(
                            COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                            COALESCE(r.updated_at, 'epoch'::timestamptz)
                        ) >= now() - (%s * INTERVAL '1 second')
                    )
                 OR (
                        COALESCE(r.active, TRUE) = TRUE
                    AND COALESCE(r.completion_status, '') = 'OPEN'
                    AND COALESCE(r.closed, FALSE) = FALSE
                    AND COALESCE(r.resolved, FALSE) = FALSE
                    AND COALESCE(r.deprecated, FALSE) = FALSE
                    )
                 OR (
                        COALESCE(r.market_state, '') IN ('CLOSING', 'RESOLVED', 'ARCHIVED')
                    AND GREATEST(
                            COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                            COALESCE(r.latest_book_at, 'epoch'::timestamptz),
                            COALESCE(t.last_book_update_at, 'epoch'::timestamptz),
                            COALESCE(t.last_l2_event_at, 'epoch'::timestamptz)
                        ) >= now() - (
                            CASE
                                WHEN COALESCE(r.market_state, '') = 'CLOSING'
                                THEN g.closing_seconds
                                ELSE g.settled_seconds
                            END * INTERVAL '1 second'
                        )
                    )
                 OR (
                        COALESCE(r.active, TRUE) = TRUE
                    AND COALESCE(r.completion_status, '') = 'OPEN'
                    AND COALESCE(r.closed, FALSE) = FALSE
                    AND COALESCE(r.resolved, FALSE) = FALSE
                    AND (
                            COALESCE(r.archived, FALSE) = TRUE
                         OR COALESCE(r.market_state, '') IN ('RESOLVED', 'ARCHIVED')
                        )
                    )
                  )
              AND (
                    COALESCE(r.active, TRUE) = TRUE
                 OR (
                        COALESCE(r.market_state, '') IN ('CLOSING', 'RESOLVED', 'ARCHIVED')
                    AND GREATEST(
                            COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                            COALESCE(r.latest_book_at, 'epoch'::timestamptz),
                            COALESCE(t.last_book_update_at, 'epoch'::timestamptz),
                            COALESCE(t.last_l2_event_at, 'epoch'::timestamptz)
                        ) >= now() - (
                            CASE
                                WHEN COALESCE(r.market_state, '') = 'CLOSING'
                                THEN g.closing_seconds
                                ELSE g.settled_seconds
                            END * INTERVAL '1 second'
                        )
                    )
                  )
              AND (
                    COALESCE(r.closed, FALSE) = FALSE
                 OR (
                        COALESCE(r.market_state, '') IN ('CLOSING', 'RESOLVED', 'ARCHIVED')
                    AND GREATEST(
                            COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                            COALESCE(r.latest_book_at, 'epoch'::timestamptz),
                            COALESCE(t.last_book_update_at, 'epoch'::timestamptz),
                            COALESCE(t.last_l2_event_at, 'epoch'::timestamptz)
                        ) >= now() - (
                            CASE
                                WHEN COALESCE(r.market_state, '') = 'CLOSING'
                                THEN g.closing_seconds
                                ELSE g.settled_seconds
                            END * INTERVAL '1 second'
                        )
                    )
                  )
              AND (
                    COALESCE(r.resolved, FALSE) = FALSE
                 OR (
                        COALESCE(r.market_state, '') IN ('CLOSING', 'RESOLVED', 'ARCHIVED')
                    AND GREATEST(
                            COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                            COALESCE(r.latest_book_at, 'epoch'::timestamptz),
                            COALESCE(t.last_book_update_at, 'epoch'::timestamptz),
                            COALESCE(t.last_l2_event_at, 'epoch'::timestamptz)
                        ) >= now() - (
                            CASE
                                WHEN COALESCE(r.market_state, '') = 'CLOSING'
                                THEN g.closing_seconds
                                ELSE g.settled_seconds
                            END * INTERVAL '1 second'
                        )
                    )
                  )
              AND (
                    COALESCE(r.archived, FALSE) = FALSE
                 OR (
                        COALESCE(r.market_state, '') IN ('CLOSING', 'RESOLVED', 'ARCHIVED')
                    AND GREATEST(
                            COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                            COALESCE(r.latest_book_at, 'epoch'::timestamptz),
                            COALESCE(t.last_book_update_at, 'epoch'::timestamptz),
                            COALESCE(t.last_l2_event_at, 'epoch'::timestamptz)
                        ) >= now() - (
                            CASE
                                WHEN COALESCE(r.market_state, '') = 'CLOSING'
                                THEN g.closing_seconds
                                ELSE g.settled_seconds
                            END * INTERVAL '1 second'
                        )
                    )
                 OR (
                        COALESCE(r.completion_status, '') = 'OPEN'
                    AND COALESCE(r.closed, FALSE) = FALSE
                    AND COALESCE(r.resolved, FALSE) = FALSE
                    )
                  )
              AND COALESCE(r.deprecated, FALSE) = FALSE
              AND (
                    COALESCE(r.market_state, 'TRADABLE_PENDING_BOOK') NOT IN ('RESOLVED', 'ARCHIVED')
                 OR (
                        COALESCE(r.completion_status, '') = 'OPEN'
                    AND COALESCE(r.closed, FALSE) = FALSE
                    AND COALESCE(r.resolved, FALSE) = FALSE
                    )
                 OR (
                        COALESCE(r.market_state, '') IN ('CLOSING', 'RESOLVED', 'ARCHIVED')
                    AND GREATEST(
                            COALESCE(r.last_transition_at, 'epoch'::timestamptz),
                            COALESCE(r.latest_book_at, 'epoch'::timestamptz),
                            COALESCE(t.last_book_update_at, 'epoch'::timestamptz),
                            COALESCE(t.last_l2_event_at, 'epoch'::timestamptz)
                        ) >= now() - (
                            CASE
                                WHEN COALESCE(r.market_state, '') = 'CLOSING'
                                THEN g.closing_seconds
                                ELSE g.settled_seconds
                            END * INTERVAL '1 second'
                        )
                    )
                  )
              AND COALESCE(t.asset_id, r.asset_id) IS NOT NULL
              AND COALESCE(t.asset_id, r.asset_id) <> ''
            """,
            (
                terminal_subscription_grace_seconds,
                settled_subscription_grace_seconds,
                execution_book_ttl_seconds,
                execution_book_ttl_seconds,
                execution_book_ttl_seconds,
                discovered_subscription_grace_seconds,
            ),
        )
        desired_rows = [dict(row) for row in cur.fetchall()]

    return tuple(
        sorted(
            (
                L2SubscriptionEntry(
                    asset_id=str(row["asset_id"]),
                    market_id=int(row.get("market_id") or 0),
                    condition_id=str(row.get("condition_id") or ""),
                    market_slug=_optional_text(row.get("market_slug")),
                    market_state=_optional_text(row.get("market_state")),
                    execution_eligible=bool(row.get("execution_eligible")),
                    book_status=_optional_text(row.get("book_status")),
                    subscription_reason=_optional_text(row.get("subscription_reason")),
                    priority_at=_optional_text(row.get("priority_at")),
                )
                for row in desired_rows
            ),
            key=lambda item: item.asset_id,
        )
    )


def load_subscription_snapshot(path: Path) -> L2SubscriptionSnapshot:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"unsupported subscription snapshot schema: {payload.get('schema_version')!r}")
    rows = payload.get("tokens")
    if not isinstance(rows, list):
        raise ValueError("subscription snapshot tokens must be a list")
    transition_priorities = payload.get("transition_priorities") or {}
    if not isinstance(transition_priorities, dict):
        raise ValueError("subscription transition_priorities must be an object")
    entries = tuple(
        _entry_with_transition_priority(row, transition_priorities)
        for row in rows
    )
    if len({entry.asset_id for entry in entries}) != len(entries):
        raise ValueError("subscription snapshot contains duplicate asset_id values")
    declared_count = int(payload.get("token_count") or 0)
    if declared_count != len(entries):
        raise ValueError(f"subscription snapshot token_count mismatch: {declared_count} != {len(entries)}")
    actual_sha256 = _entries_sha256(entries)
    declared_sha256 = str(payload.get("content_sha256") or "")
    legacy_sha256 = hashlib.sha256(
        json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if declared_sha256 not in {actual_sha256, legacy_sha256}:
        raise ValueError("subscription snapshot content_sha256 mismatch")
    actual_subscription_sha256 = _subscription_sha256(entries)
    declared_subscription_sha256 = str(payload.get("subscription_sha256") or "")
    if declared_subscription_sha256 and declared_subscription_sha256 != actual_subscription_sha256:
        raise ValueError("subscription snapshot subscription_sha256 mismatch")
    return L2SubscriptionSnapshot(
        generated_at=str(payload.get("generated_at") or ""),
        content_sha256=actual_sha256,
        subscription_sha256=actual_subscription_sha256,
        entries=entries,
    )


def _entry_from_payload(value: Any) -> L2SubscriptionEntry:
    if not isinstance(value, dict):
        raise ValueError("subscription snapshot token row must be an object")
    asset_id = str(value.get("asset_id") or "").strip()
    if not asset_id:
        raise ValueError("subscription snapshot token row has empty asset_id")
    return L2SubscriptionEntry(
        asset_id=asset_id,
        market_id=int(value.get("market_id") or 0),
        condition_id=str(value.get("condition_id") or ""),
        market_slug=_optional_text(value.get("market_slug")),
        market_state=_optional_text(value.get("market_state")),
        execution_eligible=bool(value.get("execution_eligible")),
        book_status=_optional_text(value.get("book_status")),
        subscription_reason=_optional_text(value.get("subscription_reason")),
        priority_at=_optional_text(value.get("priority_at")),
        assigned_shard_id=(
            int(value["assigned_shard_id"])
            if value.get("assigned_shard_id") is not None
            else None
        ),
        migration_from_shard_id=(
            int(value["migration_from_shard_id"])
            if value.get("migration_from_shard_id") is not None
            else None
        ),
        migration_target_shard_id=(
            int(value["migration_target_shard_id"])
            if value.get("migration_target_shard_id") is not None
            else None
        ),
        migration_valid_until=_optional_text(value.get("migration_valid_until")),
    )


def _entry_with_transition_priority(
    value: Any,
    transition_priorities: dict[str, Any],
) -> L2SubscriptionEntry:
    entry = _entry_from_payload(value)
    sidecar_value = transition_priorities.get(entry.asset_id)
    return replace(
        entry,
        priority_at=_optional_text(sidecar_value) or entry.priority_at,
    )


def _entries_sha256(entries: Iterable[L2SubscriptionEntry]) -> str:
    encoded = json.dumps(
        [_entry_payload(entry) for entry in entries],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _entry_payload(entry: L2SubscriptionEntry) -> dict[str, Any]:
    payload = asdict(entry)
    for key in (
        "assigned_shard_id",
        "migration_from_shard_id",
        "migration_target_shard_id",
        "migration_valid_until",
    ):
        if payload.get(key) is None:
            payload.pop(key, None)
    payload.pop("priority_at", None)
    return payload


def _snapshot_entry_payload(entry: L2SubscriptionEntry) -> dict[str, Any]:
    """Include local transition order without changing collector hashes."""

    payload = _entry_payload(entry)
    if entry.priority_at:
        payload["priority_at"] = entry.priority_at
    return payload


def _load_token_loads(path: Path | None) -> dict[str, TokenLoad] | None:
    if path is None:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "polymarket-l2-token-load-v1":
        raise ValueError("unsupported L2 token load schema")
    rows = payload.get("tokens")
    if not isinstance(rows, dict):
        raise ValueError("L2 token load tokens must be an object")
    return {
        str(asset_id): TokenLoad(
            events_per_second_60s=float(row.get("events_per_second_60s") or 0),
            events_per_second_15m=float(row.get("events_per_second_15m") or 0),
            bytes_per_second_60s=float(row.get("bytes_per_second_60s") or 0),
            snapshot_bytes_p95=float(row.get("snapshot_bytes_p95") or 0),
            reconnect_snapshot_cost=float(
                row.get("reconnect_snapshot_cost") or 0
            ),
        )
        for asset_id, row in rows.items()
        if isinstance(row, dict) and str(asset_id)
    }


def _load_previous_assignments(root: Path) -> dict[str, int]:
    pointer_path = root / "current.json"
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        generation = str(pointer.get("generation") or "")
        manifest = json.loads(
            (root / "generations" / generation / "manifest.json").read_text(
                encoding="utf-8"
            )
        )
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    assignments: dict[str, int] = {}
    generation_dir = root / "generations" / generation
    for shard_id_text, metadata in (manifest.get("shards") or {}).items():
        if not isinstance(metadata, dict):
            continue
        try:
            shard_id = int(shard_id_text)
            rows = json.loads(
                (generation_dir / str(metadata["file"])).read_text(
                    encoding="utf-8"
                )
            ).get("tokens") or []
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset_id = str(row.get("asset_id") or "")
            if asset_id:
                target = row.get("migration_target_shard_id")
                assignments[asset_id] = (
                    int(target) if target is not None else shard_id
                )
    return assignments


def _load_active_migrations(
    root: Path,
    *,
    include_expired: bool = False,
) -> dict[str, tuple[int, int, datetime]]:
    pointer_path = root / "current.json"
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        generation = str(pointer.get("generation") or "")
        generation_dir = root / "generations" / generation
        manifest = json.loads(
            (generation_dir / "manifest.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError, TypeError):
        return {}
    now = datetime.now(timezone.utc)
    migrations: dict[str, tuple[int, int, datetime]] = {}
    for metadata in (manifest.get("shards") or {}).values():
        if not isinstance(metadata, dict):
            continue
        try:
            rows = json.loads(
                (generation_dir / str(metadata["file"])).read_text(
                    encoding="utf-8"
                )
            ).get("tokens") or []
        except (OSError, KeyError, json.JSONDecodeError):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                valid_until = datetime.fromisoformat(
                    str(row["migration_valid_until"]).replace("Z", "+00:00")
                ).astimezone(timezone.utc)
                source = int(row["migration_from_shard_id"])
                target = int(row["migration_target_shard_id"])
            except (KeyError, TypeError, ValueError):
                continue
            asset_id = str(row.get("asset_id") or "")
            if asset_id and (include_expired or valid_until > now):
                migrations[asset_id] = (source, target, valid_until)
    return migrations


def _validate_migration_duplicates(
    entries: Iterable[L2SubscriptionEntry],
) -> None:
    by_asset: dict[str, list[L2SubscriptionEntry]] = {}
    for entry in entries:
        by_asset.setdefault(entry.asset_id, []).append(entry)
    for asset_id, copies in by_asset.items():
        if len(copies) == 1:
            continue
        assigned = {copy.assigned_shard_id for copy in copies}
        migration_pairs = {
            (
                copy.migration_from_shard_id,
                copy.migration_target_shard_id,
                copy.migration_valid_until,
            )
            for copy in copies
        }
        if (
            len(copies) != 2
            or len(assigned) != 2
            or len(migration_pairs) != 1
            or None in next(iter(migration_pairs))
        ):
            raise ValueError(
                f"invalid duplicate migration assignments for asset {asset_id}"
            )


def _subscription_sha256(entries: Iterable[L2SubscriptionEntry]) -> str:
    encoded = json.dumps(
        sorted(entry.asset_id for entry in entries),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp_path.open("w", encoding="utf-8") as output:
        json.dump(payload, output, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(tmp_path, path)


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    export = subparsers.add_parser("export", help="Export the current registry subscription universe")
    export.add_argument("--out", type=Path, required=True)
    export.add_argument(
        "--execution-only",
        action="store_true",
        help="Export only LIVE execution-eligible tokens for the low-volume redundant feed.",
    )
    export.add_argument(
        "--shard-dir",
        type=Path,
        help="Also publish an immutable per-connection shard generation under this directory.",
    )
    export.add_argument("--shard-count", type=int)
    export.add_argument("--worker-count", type=int)
    export.add_argument(
        "--worker-rebalance-threshold-ratio",
        type=float,
        default=1.05,
    )
    export.add_argument(
        "--load-weights",
        type=Path,
        help="Optional token load telemetry JSON for stable weighted sharding.",
    )
    export.add_argument("--assignment-salt", default="source-a")
    export.add_argument(
        "--maximum-migration-ratio",
        type=float,
        default=0.01,
    )
    export.add_argument(
        "--migration-overlap-seconds",
        type=float,
        default=180.0,
    )
    export.add_argument(
        "--rebalance-threshold-ratio",
        type=float,
        default=1.25,
    )
    export.add_argument("--maximum-tokens-per-shard", type=int)
    additive = subparsers.add_parser(
        "additive-export",
        help="Extend a known-good snapshot from the latest universe and native discovery log.",
    )
    additive.add_argument("--source", type=Path, required=True)
    additive.add_argument("--out", type=Path, required=True)
    additive.add_argument("--native-discovery-log", type=Path, required=True)
    additive.add_argument(
        "--maximum-universe-age-seconds",
        type=float,
        default=7200.0,
    )
    additive.add_argument(
        "--retained-asset-ids",
        type=Path,
        help="PMXT evidence file whose active assets must never be retired",
    )
    additive.add_argument(
        "--retire-market-state",
        action="append",
        default=[],
        help="Inactive state eligible for bounded retirement (repeatable)",
    )
    additive.add_argument(
        "--authoritative-membership",
        action="store_true",
        help=(
            "Reconcile stale additive members against the fresh immutable "
            "universe plus current Registry positive eligibility."
        ),
    )
    subset = subparsers.add_parser(
        "subset",
        help="Create a deterministic execution-first canary subset.",
    )
    subset.add_argument("--source", type=Path, required=True)
    subset.add_argument("--out", type=Path, required=True)
    subset.add_argument("--limit", type=int, required=True)
    execution_subset = subparsers.add_parser(
        "execution-subset",
        help="Derive the exact execution-eligible subset from a full snapshot.",
    )
    execution_subset.add_argument("--source", type=Path, required=True)
    execution_subset.add_argument("--out", type=Path, required=True)
    unshard = subparsers.add_parser(
        "unshard",
        help="Recover a unique global snapshot from an immutable shard generation.",
    )
    unshard.add_argument("--shard-dir", type=Path, required=True)
    unshard.add_argument("--out", type=Path, required=True)
    unshard.add_argument("--shard-count", type=int, required=True)
    stage = subparsers.add_parser(
        "stage",
        help="Write one bounded membership step from an active snapshot toward a desired snapshot.",
    )
    stage.add_argument("--previous", type=Path, required=True)
    stage.add_argument("--desired", type=Path, required=True)
    stage.add_argument("--out", type=Path, required=True)
    stage.add_argument("--max-added-tokens", type=int, required=True)
    stage.add_argument("--max-removed-tokens", type=int, required=True)
    stage.add_argument("--priority-asset-ids", type=Path, action="append", default=[])
    reshard = subparsers.add_parser(
        "reshard",
        help="Publish another independent shard plan from an existing snapshot.",
    )
    reshard.add_argument("--source", type=Path, required=True)
    reshard.add_argument("--shard-dir", type=Path, required=True)
    reshard.add_argument("--shard-count", type=int, required=True)
    reshard.add_argument("--worker-count", type=int)
    reshard.add_argument(
        "--worker-rebalance-threshold-ratio",
        type=float,
        default=1.05,
    )
    reshard.add_argument("--load-weights", type=Path)
    reshard.add_argument("--assignment-salt", default="source-b")
    reshard.add_argument("--maximum-migration-ratio", type=float, default=0.01)
    reshard.add_argument("--migration-overlap-seconds", type=float, default=180.0)
    reshard.add_argument(
        "--release-expired-migrations",
        action="store_true",
        help=(
            "Remove expired make-before-break source copies. The caller must "
            "first prove every target shard loaded and sent the prior generation."
        ),
    )
    reshard.add_argument("--rebalance-threshold-ratio", type=float, default=1.25)
    reshard.add_argument("--maximum-tokens-per-shard", type=int)
    check = subparsers.add_parser("check", help="Validate an existing snapshot")
    check.add_argument("--path", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.command == "export":
        snapshot = export_subscription_snapshot(
            args.out,
            execution_only=bool(args.execution_only),
            shard_dir=args.shard_dir,
            shard_count=args.shard_count,
            load_weights_path=args.load_weights,
            assignment_salt=args.assignment_salt,
            maximum_migration_ratio=args.maximum_migration_ratio,
            migration_overlap_seconds=args.migration_overlap_seconds,
            rebalance_threshold_ratio=args.rebalance_threshold_ratio,
            maximum_tokens_per_shard=args.maximum_tokens_per_shard,
            worker_count=args.worker_count,
            worker_rebalance_threshold_ratio=(
                args.worker_rebalance_threshold_ratio
            ),
        )
    elif args.command == "additive-export":
        snapshot = export_additive_subscription_snapshot(
            args.source,
            args.out,
            native_discovery_log=args.native_discovery_log,
            maximum_universe_age_seconds=args.maximum_universe_age_seconds,
            retained_asset_ids=_load_priority_asset_ids(args.retained_asset_ids),
            retire_market_states=set(args.retire_market_state),
            authoritative_membership=bool(args.authoritative_membership),
        )
    elif args.command == "subset":
        snapshot = export_subscription_subset(
            args.source,
            args.out,
            limit=args.limit,
        )
    elif args.command == "execution-subset":
        snapshot = export_execution_subscription_subset(
            args.source,
            args.out,
        )
    elif args.command == "unshard":
        snapshot = export_unassigned_sharded_subscription_snapshot(
            args.shard_dir,
            args.out,
            expected_shard_count=args.shard_count,
        )
    elif args.command == "stage":
        snapshot = stage_subscription_transition(
            args.previous,
            args.desired,
            args.out,
            max_added_tokens=args.max_added_tokens,
            max_removed_tokens=args.max_removed_tokens,
            priority_asset_ids=set().union(
                *(_load_priority_asset_ids(path) for path in args.priority_asset_ids)
            ),
        )
    elif args.command == "reshard":
        snapshot = load_subscription_snapshot(args.source)
        export_sharded_subscription_snapshot(
            args.shard_dir,
            snapshot=snapshot,
            shard_count=args.shard_count,
            loads=_load_token_loads(args.load_weights),
            assignment_salt=args.assignment_salt,
            maximum_migration_ratio=args.maximum_migration_ratio,
            migration_overlap_seconds=args.migration_overlap_seconds,
            rebalance_threshold_ratio=args.rebalance_threshold_ratio,
            maximum_tokens_per_shard=args.maximum_tokens_per_shard,
            worker_count=args.worker_count,
            worker_rebalance_threshold_ratio=(
                args.worker_rebalance_threshold_ratio
            ),
            release_expired_migrations=args.release_expired_migrations,
        )
    else:
        snapshot = load_subscription_snapshot(args.path)
    print(
        json.dumps(
            {
                "generated_at": snapshot.generated_at,
                "token_count": snapshot.token_count,
                "content_sha256": snapshot.content_sha256,
                "subscription_sha256": snapshot.subscription_sha256,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
