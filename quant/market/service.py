"""Dynamic paper Market Registry service orchestration."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import threading
from typing import Any, Callable, Mapping

from .api_client import (
    PolymarketApiClient,
    markets_from_gamma_events,
    tokens_from_clob_market,
    tokens_from_gamma_market,
    tokens_from_ws_new_market,
)
from .existing_registry import ExistingMarketRegistry
from .repository import MarketRegistryRepository, RegistryPersistSummary, RegistryOutboxPublishSummary, RegistryRepairSummary
from .token_universe import MarketRegistryToken, MarketUniverseConfig, compute_universe_decisions


_DB_DELTA_STATE_KEY = "db_market_delta_cursor"
_DB_DELTA_OVERLAP_SECONDS = 300
_API_DELTA_STATE_KEY = "gamma_api_delta_watermark"
_API_DELTA_OVERLAP_SECONDS = 300
_API_DELTA_MAX_ROWS = 100_000
_HEALTH_COUNTS_REFRESH_SECONDS = 300.0


@dataclass(frozen=True)
class RegistryCycleSummary:
    sync_type: str
    run_id: int
    tokens_seen: int
    tokens_upserted: int
    transitions_written: int
    outbox_events: int
    generation: int
    subscription_count: int
    execution_count: int
    api_tokens_seen: int = 0
    db_tokens_seen: int = 0
    probes_attempted: int = 0
    probes_ok: int = 0
    removed_absent_count: int = 0
    api_complete: bool = False
    outbox_published: int = 0
    repairs_applied: int = 0


@dataclass(frozen=True)
class RegistryTargetedRecheckSummary:
    run_id: int
    markets_checked: int = 0
    markets_deactivated: int = 0
    tokens_deactivated: int = 0
    markets_kept_open: int = 0
    mapping_mismatches: int = 0
    api_errors: int = 0


class MarketRegistryService:
    """Full-sync, delta-poll, book-probe, and lifecycle coordinator."""

    def __init__(
        self,
        conn: Any,
        *,
        config: MarketUniverseConfig | None = None,
        api_client: PolymarketApiClient | None = None,
    ) -> None:
        self.conn = conn
        self.config = config or MarketUniverseConfig()
        self.repo = MarketRegistryRepository(conn)
        self.existing = ExistingMarketRegistry(conn, config=self.config)
        self.api_client = api_client
        self.api_errors: list[str] = []

    def init_schema(self) -> None:
        self.repo.init_schema()

    def full_sync(
        self,
        *,
        limit: int = 0,
        include_api: bool = True,
        api_limit: int | None = None,
        api_page_size: int = 500,
    ) -> RegistryCycleSummary:
        source_limit = _normalise_limit(limit)
        api_market_limit = _normalise_limit(api_limit)
        try:
            api_tokens, api_complete = self._fetch_api_tokens(
                limit=api_market_limit,
                page_size=api_page_size,
                require_full=api_market_limit is None,
                include_clob_markets=False,
            ) if include_api else ([], False)
            terminal_api_asset_ids = (
                self.repo.terminal_asset_ids([token.asset_id for token in api_tokens])
                if api_tokens
                else set()
            )
            if terminal_api_asset_ids:
                api_tokens = [
                    token for token in api_tokens
                    if token.asset_id not in terminal_api_asset_ids
                ]
            # A full universe reconciliation must not drag the full historical
            # order-book snapshot table through one lateral lookup per token.
            # The registry state and the live LOB projection already carry the
            # book-derived eligibility fields; this read is only authoritative
            # for market/token discovery and lifecycle state.
            db_tokens = self.existing.fetch_tokens(
                limit=source_limit,
                candidates_only=True,
                include_book=False,
            )
            known_asset_ids = self.repo.list_reconciliation_asset_ids(limit=None)
            active_asset_ids = {token.asset_id for token in db_tokens if token.asset_id}
            # The authoritative active scan already returned these rows.  A
            # second full-width fetch of every registry asset duplicated most
            # of the universe and recreated the same large database plan.  We
            # only need source rows for historical assets absent from the
            # current active snapshot; current registry rows are read below.
            absent_known_asset_ids = [
                asset_id
                for asset_id in known_asset_ids
                if asset_id not in active_asset_ids
            ]
            known_db_tokens = self.existing.fetch_tokens_by_asset_ids(
                absent_known_asset_ids,
                include_book=False,
            )
            current_state_tokens = self.repo.fetch_state_tokens(known_asset_ids)
            tokens, removed_absent_count = _reconcile_authoritative_active_snapshot(
                active_tokens=_merge_tokens(db_tokens, api_tokens),
                known_tokens=_merge_tokens(current_state_tokens, known_db_tokens),
                authoritative_complete=source_limit is None and include_api and api_complete,
            )
            decisions = compute_universe_decisions(tokens, config=self.config)
            run_id = self.repo.begin_sync_run(
                "full_sync",
                meta={
                    "limit": limit,
                    "include_api": include_api,
                    "api_limit": api_limit,
                    "api_page_size": api_page_size,
                    "reconciliation_asset_count": len(known_asset_ids),
                    "reconciliation_source_refetch_count": len(absent_known_asset_ids),
                    "terminal_api_tokens_ignored": len(terminal_api_asset_ids),
                },
            )
            summary = self.repo.persist_decisions(
                decisions,
                run_id=run_id,
                source="full_sync",
                aggregate_changed_only=True,
            )
            self.repo.finish_sync_run(
                run_id,
                status="success",
                summary=summary,
                meta={
                    "api_errors": self.api_errors[-5:],
                    "api_complete": api_complete,
                    "removed_absent_count": removed_absent_count,
                    "absence_reconciliation_enabled": False,
                    "absence_reconciliation_reason": "discovery_absence_is_not_lifecycle_truth",
                    "db_complete": source_limit is None,
                },
            )
            return _cycle_summary(
                "full_sync",
                run_id,
                summary,
                api_tokens_seen=len(api_tokens),
                db_tokens_seen=len(db_tokens) + len(known_db_tokens),
                removed_absent_count=removed_absent_count,
                api_complete=api_complete,
            )
        except Exception as exc:
            self._rollback_after_error()
            if "run_id" in locals():
                self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def delta_poll(
        self,
        *,
        limit: int = 0,
        include_api: bool = True,
        api_limit: int | None = None,
        api_page_size: int = 500,
    ) -> RegistryCycleSummary:
        source_limit = _normalise_limit(limit)
        api_market_limit = _normalise_limit(api_limit)
        try:
            api_tokens, api_complete = self._fetch_api_tokens(
                limit=api_market_limit,
                page_size=api_page_size,
                require_full=api_market_limit is None,
                include_clob_markets=False,
            ) if include_api else ([], False)
            incremental = source_limit is None and all(
                callable(getattr(owner, method, None))
                for owner, method in (
                    (self.existing, "source_watermark"),
                    (self.existing, "list_changed_asset_ids"),
                    (self.repo, "get_state_json"),
                    (self.repo, "set_state_json"),
                    (self.repo, "latest_successful_sync_at"),
                    (self.repo, "persist_partial_decisions"),
                )
            )
            source_until: datetime | None = None
            source_since: datetime | None = None
            changed_asset_ids: list[str] = []
            if incremental:
                source_until = self.existing.source_watermark()
                state = self.repo.get_state_json(_DB_DELTA_STATE_KEY)
                cursor = _datetime_from_state(state.get("source_until"))
                if cursor is None:
                    cursor = self.repo.latest_successful_sync_at("full_sync")
                if cursor is not None:
                    source_since = cursor - timedelta(seconds=_DB_DELTA_OVERLAP_SECONDS)
                    changed_asset_ids = self.existing.list_changed_asset_ids(
                        since=source_since,
                        until=source_until,
                    )
                else:
                    incremental = False
            if incremental:
                touched_asset_ids = list(dict.fromkeys([
                    *changed_asset_ids,
                    *(token.asset_id for token in api_tokens if token.asset_id),
                ]))
                source_tokens = self.existing.fetch_tokens_by_asset_ids(
                    changed_asset_ids,
                    include_book=False,
                )
                current_state_tokens = self.repo.fetch_state_tokens(touched_asset_ids)
                tokens = _merge_tokens(current_state_tokens, source_tokens, api_tokens)
                removed_absent_count = 0
            else:
                active_tokens = self.existing.fetch_tokens(
                    limit=source_limit,
                    candidates_only=True,
                    include_book=False,
                )
                # This fallback is used only before a durable source cursor exists.
                existing_asset_ids = self.repo.list_desired_subscription_asset_ids(limit=None)
                existing_tokens = self.existing.fetch_tokens_by_asset_ids(
                    existing_asset_ids,
                    include_book=False,
                )
                touched_asset_ids = list({
                    *(token.asset_id for token in active_tokens if token.asset_id),
                    *existing_asset_ids,
                })
                current_state_tokens = self.repo.fetch_state_tokens(touched_asset_ids)
                tokens, removed_absent_count = _reconcile_authoritative_active_snapshot(
                    active_tokens=_merge_tokens(active_tokens, api_tokens),
                    known_tokens=_merge_tokens(current_state_tokens, existing_tokens),
                    authoritative_complete=source_limit is None and include_api and api_complete,
                )
            decisions = compute_universe_decisions(tokens, config=self.config)
            # Discovery reads can scan a large upstream market table.  Do not
            # hold the registry-writer advisory lock while doing that work:
            # the independent outbox publisher must remain able to deliver an
            # already committed subscription generation.  Acquire the lock
            # only for the mutation phase, matching ``full_sync``.
            run_id = self.repo.begin_sync_run(
                "delta_poll",
                meta={
                    "limit": limit,
                    "include_api": include_api,
                    "api_limit": api_limit,
                    "api_page_size": api_page_size,
                },
            )
            if incremental:
                summary = self.repo.persist_partial_decisions(
                    decisions,
                    run_id=run_id,
                    source="delta_poll",
                    aggregate_changed_only=True,
                )
                self.repo.set_state_json(
                    _DB_DELTA_STATE_KEY,
                    {
                        "source_until": source_until.isoformat() if source_until else None,
                        "source_since": source_since.isoformat() if source_since else None,
                        "changed_asset_count": len(changed_asset_ids),
                    },
                )
            else:
                summary = self.repo.persist_decisions(
                    decisions,
                    run_id=run_id,
                    source="delta_poll",
                    aggregate_changed_only=True,
                )
            self.repo.finish_sync_run(
                run_id,
                status="success",
                summary=summary,
                meta={
                    "api_errors": self.api_errors[-5:],
                    "api_complete": api_complete,
                    "removed_absent_count": removed_absent_count,
                    "absence_reconciliation_enabled": False,
                    "absence_reconciliation_reason": "discovery_absence_is_not_lifecycle_truth",
                    "db_complete": source_limit is None,
                    "incremental": incremental,
                    "changed_asset_count": len(changed_asset_ids),
                    "source_since": source_since.isoformat() if source_since else None,
                    "source_until": source_until.isoformat() if source_until else None,
                },
            )
            return _cycle_summary(
                "delta_poll",
                run_id,
                summary,
                api_tokens_seen=len(api_tokens),
                db_tokens_seen=(
                    len(changed_asset_ids)
                    if incremental
                    else len(active_tokens) + len(existing_tokens)
                ),
                removed_absent_count=removed_absent_count,
                api_complete=api_complete,
            )
        except Exception as exc:
            self._rollback_after_error()
            if "run_id" in locals():
                self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def api_delta_refresh(
        self,
        *,
        api_limit: int | None = 1000,
        api_page_size: int = 100,
    ) -> RegistryCycleSummary:
        api_market_limit = _normalise_limit(api_limit)
        try:
            delta_state = self.repo.get_state_json(_API_DELTA_STATE_KEY)
            previous_watermark = _datetime_from_state(delta_state.get("updated_at"))
            updated_since = (
                previous_watermark - timedelta(seconds=_API_DELTA_OVERLAP_SECONDS)
                if previous_watermark is not None
                else None
            )
            api_tokens, api_complete, latest_updated_at = self._fetch_api_delta_tokens(
                limit=api_market_limit,
                page_size=api_page_size,
                updated_since=updated_since,
            )
            run_id = self.repo.begin_sync_run(
                "api_delta_refresh",
                meta={"api_limit": api_limit, "api_page_size": api_page_size},
            )
            asset_ids = [token.asset_id for token in api_tokens if token.asset_id]
            existing_tokens = self.repo.fetch_state_tokens(asset_ids) if asset_ids else []
            tokens = _merge_tokens(existing_tokens, api_tokens)
            decisions = compute_universe_decisions(tokens, config=self.config)
            previous_states = self.repo._previous_states([decision.asset_id for decision in decisions])
            decisions = self.repo._guard_decision_state_regressions(decisions, previous_states)
            transitions, tokens_upserted = self.repo._upsert_tokens_and_events(decisions, previous_states, run_id=run_id, source="api_delta_refresh")
            self.repo._upsert_market_aggregates(decisions, run_id=run_id, source="api_delta_refresh")
            outbox_events = self.repo.upsert_subscription_targets_for_decisions(decisions, run_id=run_id)
            self.repo.repair_ineligible_subscription_targets(
                run_id=run_id,
                asset_ids=asset_ids,
            )
            if api_complete and latest_updated_at is not None:
                self.repo.set_state_json(
                    _API_DELTA_STATE_KEY,
                    {
                        "updated_at": latest_updated_at.isoformat(),
                        "previous_watermark": previous_watermark.isoformat() if previous_watermark else None,
                        "overlap_seconds": _API_DELTA_OVERLAP_SECONDS,
                    },
                )
            summary = RegistryPersistSummary(
                tokens_seen=len(decisions),
                tokens_upserted=tokens_upserted,
                transitions_written=transitions,
                outbox_events=outbox_events,
                subscription_count=sum(1 for decision in decisions if decision.subscription_eligible),
                execution_count=sum(1 for decision in decisions if decision.execution_eligible),
            )
            self.repo.finish_sync_run(
                run_id,
                status="success",
                summary=summary,
                meta={
                    "api_errors": self.api_errors[-5:],
                    "api_complete": api_complete,
                    "updated_since": updated_since.isoformat() if updated_since else None,
                    "latest_updated_at": latest_updated_at.isoformat() if latest_updated_at else None,
                },
            )
            return _cycle_summary(
                "api_delta_refresh",
                run_id,
                summary,
                api_tokens_seen=len(api_tokens),
                api_complete=api_complete,
            )
        except Exception as exc:
            self._rollback_after_error()
            if "run_id" in locals():
                self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def clob_markets_refresh(
        self,
        *,
        page_budget: int = 10,
        max_open_markets: int | None = None,
        rescan_seconds: float = 1800.0,
        force_restart: bool = False,
    ) -> RegistryCycleSummary:
        if self.api_client is None:
            raise RuntimeError("api_client is required for CLOB /markets refresh")
        budget = max(1, int(page_budget))
        open_limit = _normalise_limit(max_open_markets)
        state = self.repo.get_state_json("clob_markets_cursor")
        completed_at = _datetime_from_state(state.get("completed_at"))
        if completed_at is not None and not force_restart:
            age = (datetime.now(timezone.utc) - completed_at).total_seconds()
            if age < max(0.0, float(rescan_seconds)):
                run_id = self.repo.begin_sync_run(
                    "clob_markets_refresh",
                    meta={"skipped": True, "completed_age_seconds": age, "rescan_seconds": rescan_seconds},
                )
                summary = RegistryPersistSummary()
                self.repo.finish_sync_run(
                    run_id,
                    status="success",
                    summary=summary,
                    meta={"skipped": True, "completed_age_seconds": age},
                )
                return _cycle_summary("clob_markets_refresh", run_id, summary, api_complete=True)
        cursor = None if force_restart else _state_text(state.get("next_cursor"))
        pages_read = 0
        raw_rows_seen = 0
        open_rows: list[Mapping[str, Any]] = []
        completed_scan = _is_terminal_clob_cursor(cursor)
        next_cursor: str | None = None if completed_scan else cursor
        seen_cursors: set[str] = set()
        while pages_read < budget and not completed_scan:
            page = self.api_client.fetch_clob_markets(cursor=next_cursor)
            pages_read += 1
            page_rows = list(page.get("markets") or [])
            raw_rows_seen += len(page_rows)
            for row in page_rows:
                if not isinstance(row, Mapping):
                    continue
                mapped = tokens_from_clob_market(row)
                if mapped:
                    open_rows.append(row)
                    if open_limit is not None and len(open_rows) >= open_limit:
                        break
            new_cursor = _state_text(page.get("next_cursor"))
            if not page_rows or not new_cursor or new_cursor in seen_cursors or new_cursor == next_cursor:
                completed_scan = True
                next_cursor = None
                break
            seen_cursors.add(new_cursor)
            next_cursor = new_cursor
            if open_limit is not None and len(open_rows) >= open_limit:
                break
        clob_tokens = [token for row in open_rows for token in tokens_from_clob_market(row)]
        run_id = self.repo.begin_sync_run(
            "clob_markets_refresh",
            meta={
                "page_budget": budget,
                "max_open_markets": max_open_markets,
                "start_cursor": cursor,
                "force_restart": force_restart,
            },
        )
        asset_ids = [token.asset_id for token in clob_tokens if token.asset_id]
        existing_tokens = self.repo.fetch_state_tokens(asset_ids) if asset_ids else []
        tokens = _merge_tokens(existing_tokens, clob_tokens)
        decisions = compute_universe_decisions(tokens, config=self.config)
        previous_states = self.repo._previous_states([decision.asset_id for decision in decisions])
        decisions = self.repo._guard_decision_state_regressions(decisions, previous_states)
        try:
            transitions, tokens_upserted = self.repo._upsert_tokens_and_events(decisions, previous_states, run_id=run_id, source="clob_markets_refresh")
            self.repo._upsert_market_aggregates(decisions, run_id=run_id, source="clob_markets_refresh")
            outbox_events = self.repo.upsert_subscription_targets_for_decisions(decisions, run_id=run_id)
            self.repo.repair_ineligible_subscription_targets(
                run_id=run_id,
                asset_ids=asset_ids,
            )
            now_iso = datetime.now(timezone.utc).isoformat()
            self.repo.set_state_json(
                "clob_markets_cursor",
                {
                    "next_cursor": next_cursor,
                    "last_cursor": cursor,
                    "pages_read": pages_read,
                    "raw_rows_seen": raw_rows_seen,
                    "open_book_enabled_rows": len(open_rows),
                    "tokens_seen": len(clob_tokens),
                    "completed_at": now_iso if completed_scan else state.get("completed_at"),
                    "last_run_at": now_iso,
                },
            )
            summary = RegistryPersistSummary(
                tokens_seen=len(decisions),
                tokens_upserted=tokens_upserted,
                transitions_written=transitions,
                outbox_events=outbox_events,
                subscription_count=sum(1 for decision in decisions if decision.subscription_eligible),
                execution_count=sum(1 for decision in decisions if decision.execution_eligible),
            )
            self.repo.finish_sync_run(
                run_id,
                status="success",
                summary=summary,
                meta={
                    "pages_read": pages_read,
                    "raw_rows_seen": raw_rows_seen,
                    "open_book_enabled_rows": len(open_rows),
                    "clob_tokens_seen": len(clob_tokens),
                    "next_cursor": next_cursor,
                    "completed_scan": completed_scan,
                },
            )
            return _cycle_summary(
                "clob_markets_refresh",
                run_id,
                summary,
                api_tokens_seen=len(clob_tokens),
                api_complete=completed_scan,
            )
        except Exception as exc:
            self._rollback_after_error()
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def probe_pending_books(self, *, limit: int = 100, batch_size: int = 50) -> RegistryCycleSummary:
        if self.api_client is None:
            raise RuntimeError("api_client is required for CLOB book probe")
        probe_limit = _normalise_limit(limit)
        probes_attempted = 0
        probes_ok = 0
        try:
            asset_ids = self.repo.list_probe_candidates(limit=probe_limit)
            tokens = self.repo.fetch_state_tokens(asset_ids)
            self.conn.commit()
            probe_results = self.api_client.probe_books(
                [token.asset_id for token in tokens],
                batch_size=batch_size,
            )
            for token in tokens:
                probes_attempted += 1
                probe = probe_results.get(token.asset_id)
                if probe is None:
                    probe = self.api_client.probe_book(token.asset_id)
                    probe_results[token.asset_id] = probe
                if probe.ok:
                    probes_ok += 1
            run_id = self.repo.begin_sync_run("book_probe", meta={"limit": limit, "batch_size": batch_size})
            current_tokens = self.repo.fetch_state_tokens(asset_ids)
            probed_tokens = [
                probe_results[token.asset_id].apply_to_token(token)
                for token in current_tokens
                if token.asset_id in probe_results
            ]
            probed_decisions = compute_universe_decisions(
                probed_tokens,
                config=self.config,
                now=datetime.now(timezone.utc),
            )
            for decision in probed_decisions:
                probe = probe_results.get(decision.asset_id)
                if probe is not None:
                    self.repo.update_probe_result(decision, probe, run_id=run_id)
            summary = self.repo.persist_partial_decisions(
                probed_decisions,
                run_id=run_id,
                source="book_probe",
                aggregate_changed_only=True,
            )
            self.repo.finish_sync_run(
                run_id,
                status="success",
                summary=summary,
                meta={"probes_attempted": probes_attempted, "probes_ok": probes_ok},
            )
            return _cycle_summary(
                "book_probe",
                run_id,
                summary,
                probes_attempted=probes_attempted,
                probes_ok=probes_ok,
            )
        except Exception as exc:
            self._rollback_after_error()
            if "run_id" in locals():
                self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def recheck_stale_markets(
        self,
        *,
        limit: int = 100,
        source_age_seconds: int = 7 * 24 * 60 * 60,
        retry_seconds: int = 6 * 60 * 60,
        probe_batch_size: int = 50,
        concurrency: int = 4,
    ) -> RegistryTargetedRecheckSummary:
        """Verify old DB-open/no-book markets using exact Gamma and CLOB truth.

        A market is removed from the active universe only when the exact Gamma
        record says it is inactive/closed/resolved and every mapped token also
        has no CLOB book. This avoids repeating the old "missing from one full
        listing means closed" failure mode.
        """

        if self.api_client is None:
            raise RuntimeError("api_client is required for stale market recheck")
        candidates = self.repo.list_stale_status_recheck_markets(
            limit=max(1, int(limit)),
            source_age_seconds=max(0, int(source_age_seconds)),
            retry_seconds=max(0, int(retry_seconds)),
        )
        markets_checked = 0
        markets_deactivated = 0
        tokens_deactivated = 0
        markets_kept_open = 0
        mapping_mismatches = 0
        api_errors = 0
        outcomes: list[tuple[str, str, dict[str, Any]]] = []
        inactive_markets: list[tuple[str, list[MarketRegistryToken], set[str]]] = []
        terminal_markets: list[tuple[str, list[MarketRegistryToken]]] = []
        run_id: int | None = None
        try:
            gamma_results: dict[str, tuple[list[MarketRegistryToken] | None, Exception | None]] = {}
            candidate_market_ids = sorted({
                str(candidate.get("gamma_market_id") or "").strip()
                for candidate in candidates
                if str(candidate.get("gamma_market_id") or "").strip()
            })
            thread_clients: list[PolymarketApiClient] = []
            thread_clients_lock = threading.Lock()
            thread_local = threading.local()

            def fetch_exact_gamma(gamma_market_id: str) -> dict[str, Any]:
                client = getattr(thread_local, "api_client", None)
                if client is None:
                    client = PolymarketApiClient(self.api_client.config)
                    thread_local.api_client = client
                    with thread_clients_lock:
                        thread_clients.append(client)
                return client.fetch_gamma_market(
                    gamma_market_id,
                    timeout_seconds=min(10.0, float(client.config.timeout_seconds)),
                    attempts=1,
                )

            with ThreadPoolExecutor(max_workers=max(1, min(8, int(concurrency)))) as executor:
                futures = {
                    executor.submit(fetch_exact_gamma, gamma_market_id): gamma_market_id
                    for gamma_market_id in candidate_market_ids
                }
                for future in as_completed(futures):
                    gamma_market_id = futures[future]
                    try:
                        gamma_results[gamma_market_id] = (
                            tokens_from_gamma_market(future.result()),
                            None,
                        )
                    except Exception as exc:  # noqa: BLE001
                        gamma_results[gamma_market_id] = (None, exc)
            for client in thread_clients:
                close = getattr(client.session, "close", None)
                if callable(close):
                    close()
            for candidate in candidates:
                gamma_market_id = str(candidate.get("gamma_market_id") or "").strip()
                candidate_assets = {
                    str(asset_id).strip()
                    for asset_id in (candidate.get("asset_ids") or [])
                    if str(asset_id).strip()
                }
                if not gamma_market_id or not candidate_assets:
                    continue
                markets_checked += 1
                gamma_tokens, fetch_error = gamma_results.get(
                    gamma_market_id,
                    (None, RuntimeError("targeted Gamma result missing")),
                )
                if fetch_error is not None or gamma_tokens is None:
                    exc = fetch_error or RuntimeError("targeted Gamma result missing")
                    api_errors += 1
                    outcomes.append((
                        gamma_market_id,
                        "api_error",
                        {"error": f"{type(exc).__name__}: {str(exc)[:300]}"},
                    ))
                    continue
                mapped_assets = {token.asset_id for token in gamma_tokens if token.asset_id}
                if not gamma_tokens or not candidate_assets.issubset(mapped_assets):
                    mapping_mismatches += 1
                    outcomes.append((
                        gamma_market_id,
                        "token_mapping_mismatch",
                        {
                            "candidate_asset_count": len(candidate_assets),
                            "gamma_asset_count": len(mapped_assets),
                        },
                    ))
                    continue
                lifecycle_inactive = all(
                    (not token.active) or token.closed or token.resolved or token.archived
                    for token in gamma_tokens
                )
                if not lifecycle_inactive:
                    markets_kept_open += 1
                    outcomes.append((
                        gamma_market_id,
                        "gamma_open",
                        {"asset_count": len(mapped_assets)},
                    ))
                    continue
                inactive_markets.append((gamma_market_id, gamma_tokens, mapped_assets))

            probe_assets = sorted({
                asset_id
                for _gamma_market_id, _gamma_tokens, mapped_assets in inactive_markets
                for asset_id in mapped_assets
            })
            probes = self.api_client.probe_books(
                probe_assets,
                batch_size=max(1, int(probe_batch_size)),
            ) if probe_assets else {}
            for gamma_market_id, gamma_tokens, mapped_assets in inactive_markets:
                no_book_confirmed = all(
                    asset_id in probes
                    and not probes[asset_id].ok
                    and str(probes[asset_id].book_status or "").lower()
                    in {"no_clob_book", "not_found", "404"}
                    for asset_id in mapped_assets
                )
                if not no_book_confirmed:
                    markets_kept_open += 1
                    outcomes.append((
                        gamma_market_id,
                        "inactive_but_book_present_or_probe_uncertain",
                        {"asset_count": len(mapped_assets)},
                    ))
                    continue
                terminal_markets.append((gamma_market_id, gamma_tokens))
                markets_deactivated += 1
                tokens_deactivated += len(gamma_tokens)
                outcomes.append((
                    gamma_market_id,
                    "deactivated",
                    {"asset_count": len(gamma_tokens)},
                ))

            run_id = self.repo.begin_sync_run(
                "targeted_stale_status_recheck",
                meta={
                    "limit": limit,
                    "source_age_seconds": source_age_seconds,
                    "retry_seconds": retry_seconds,
                },
            )
            decisions = []
            for _gamma_market_id, gamma_tokens in terminal_markets:
                mapped_assets = sorted(token.asset_id for token in gamma_tokens if token.asset_id)
                current_tokens = self.repo.fetch_state_tokens(mapped_assets)
                terminal_tokens = _merge_tokens(current_tokens, gamma_tokens)
                decisions.extend(compute_universe_decisions(terminal_tokens, config=self.config))
            persist_summary = (
                self.repo.persist_partial_decisions(
                    decisions,
                    run_id=run_id,
                    source="targeted_stale_status_recheck",
                    aggregate_changed_only=True,
                )
                if decisions
                else RegistryPersistSummary()
            )
            self.repo.mark_stale_status_rechecks([
                {
                    "gamma_market_id": gamma_market_id,
                    "outcome": outcome,
                    "detail": detail,
                }
                for gamma_market_id, outcome, detail in outcomes
            ])
            self.repo.finish_sync_run(
                run_id,
                status="success",
                summary=persist_summary,
                meta={
                    "markets_checked": markets_checked,
                    "markets_deactivated": markets_deactivated,
                    "tokens_deactivated": tokens_deactivated,
                    "markets_kept_open": markets_kept_open,
                    "mapping_mismatches": mapping_mismatches,
                    "api_errors": api_errors,
                },
            )
            return RegistryTargetedRecheckSummary(
                run_id=run_id,
                markets_checked=markets_checked,
                markets_deactivated=markets_deactivated,
                tokens_deactivated=tokens_deactivated,
                markets_kept_open=markets_kept_open,
                mapping_mismatches=mapping_mismatches,
                api_errors=api_errors,
            )
        except Exception as exc:
            self._rollback_after_error()
            if run_id is not None:
                self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def handle_lifecycle_event(self, event: Mapping[str, Any]) -> dict[str, Any]:
        event_type = str(event.get("event_type") or event.get("type") or "").strip()
        if event_type == "new_market":
            discovered = tokens_from_ws_new_market(event)
            if discovered:
                run_id = self.repo.begin_sync_run(
                    "ws_new_market",
                    meta={
                        "event_type": event_type,
                        "condition_id": discovered[0].condition_id,
                        "asset_count": len(discovered),
                    },
                )
                try:
                    asset_ids = [token.asset_id for token in discovered]
                    existing = self.repo.fetch_state_tokens(asset_ids)
                    decisions = compute_universe_decisions(
                        _merge_tokens(existing, discovered),
                        config=self.config,
                    )
                    summary = self.repo.persist_partial_decisions(
                        decisions,
                        run_id=run_id,
                        source="ws_new_market",
                    )
                    self.repo.finish_sync_run(
                        run_id,
                        status="success",
                        summary=summary,
                        meta={"asset_ids": asset_ids},
                    )
                    return {
                        "event_type": event_type,
                        "run_id": run_id,
                        "affected_asset_ids": asset_ids,
                        "native_discovery": True,
                    }
                except Exception as exc:
                    self._rollback_after_error()
                    self.repo.finish_sync_run(run_id, status="error", error=str(exc))
                    raise
            return self._handle_lifecycle_hint(
                event_type,
                meta={"hint_only": True, "reason": "native_token_ids_missing"},
            )
        if event_type == "market_resolved" and not _has_resolution_truth(event):
            return self._handle_lifecycle_hint(
                event_type,
                meta={"resolution_truth_present": False, "hint_only": True},
                response={"resolution_truth_present": False},
            )
        run_id = self.repo.begin_sync_run("ws_lifecycle", meta={"event_type": event_type})
        try:
            if event_type == "market_resolved":
                condition_id = str(event.get("market") or event.get("condition_id") or "").strip()
                asset_ids = self.repo.mark_condition_resolved(
                    condition_id,
                    run_id=run_id,
                    source="ws_lifecycle",
                    raw_payload=event,
                )
                # A lifecycle message affects only the tokens in one condition.
                # Recomputing the complete registry here used to sort/join every
                # known token for each resolution.  At the current universe size
                # that could exceed PostgreSQL's 1 GiB temp_file_limit, and the
                # caller then mistook the database error for a WebSocket drop.
                decisions = compute_universe_decisions(
                    self.repo.fetch_state_tokens(asset_ids),
                    config=self.config,
                )
                summary = (
                    self.repo.persist_partial_decisions(
                        decisions,
                        run_id=run_id,
                        source="ws_lifecycle",
                        repair_global_targets=False,
                        include_universe_counts=False,
                    )
                    if decisions
                    else RegistryPersistSummary()
                )
                self.repo.finish_sync_run(run_id, status="success", summary=summary)
                return {"event_type": event_type, "run_id": run_id, "affected_asset_ids": asset_ids}
            self.repo.finish_sync_run(run_id, status="success", summary=RegistryPersistSummary())
            return {"event_type": event_type or "ignored", "run_id": run_id, "ignored": True}
        except Exception as exc:
            self._rollback_after_error()
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def _handle_lifecycle_hint(
        self,
        event_type: str,
        *,
        meta: Mapping[str, Any],
        response: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        run_id = self.repo.begin_hint_run("ws_lifecycle", meta={"event_type": event_type})
        try:
            self.repo.finish_sync_run(
                run_id,
                status="success",
                summary=RegistryPersistSummary(),
                meta=meta,
            )
            return {"event_type": event_type, "run_id": run_id, "hint_only": True, **dict(response or {})}
        except Exception as exc:
            self._rollback_after_error()
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def handle_lob_collector_status(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        run_id = self.repo.begin_sync_run("lob_collector_status", meta={"asset_id": raw.get("asset_id")})
        try:
            asset_ids = self.repo.apply_lob_collector_status(raw, run_id=run_id)
            decisions = compute_universe_decisions(self.repo.fetch_all_state_tokens(limit=None), config=self.config)
            summary = self.repo.persist_decisions(decisions, run_id=run_id, source="lob_collector_status")
            self.repo.finish_sync_run(run_id, status="success", summary=summary)
            return {"event_type": "LOB_SUBSCRIPTION_STATUS", "run_id": run_id, "affected_asset_ids": asset_ids}
        except Exception as exc:
            self._rollback_after_error()
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def handle_ws_disconnect(self, asset_ids: list[str], *, reason: str = "ws_disconnect") -> dict[str, Any]:
        run_id = self.repo.begin_sync_run("ws_disconnect", meta={"asset_count": len(asset_ids), "reason": reason})
        try:
            affected = self.repo.mark_assets_disconnected(asset_ids, run_id=run_id, reason=reason)
            decisions = compute_universe_decisions(self.repo.fetch_all_state_tokens(limit=None), config=self.config)
            summary = self.repo.persist_decisions(decisions, run_id=run_id, source="ws_disconnect")
            self.repo.finish_sync_run(run_id, status="success", summary=summary, meta={"affected_count": len(affected)})
            return {"event_type": "WS_DISCONNECTED", "run_id": run_id, "affected_asset_ids": affected}
        except Exception as exc:
            self._rollback_after_error()
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def republish_desired_subscriptions(self, *, reason: str = "daemon_startup") -> dict[str, Any]:
        run_id = self.repo.begin_sync_run("republish_subscriptions", meta={"reason": reason})
        try:
            decisions = compute_universe_decisions(self.repo.fetch_all_state_tokens(limit=None), config=self.config)
            outbox_events = self.repo.write_current_subscription_outbox(decisions, run_id=run_id)
            latest_diff = self.repo.latest_universe_diff()
            summary = RegistryPersistSummary(
                tokens_seen=len(decisions),
                tokens_upserted=0,
                transitions_written=0,
                outbox_events=outbox_events,
                generation=latest_diff.generation if latest_diff else 0,
                subscription_count=sum(1 for decision in decisions if decision.subscription_eligible),
                execution_count=sum(1 for decision in decisions if decision.execution_eligible),
            )
            self.repo.finish_sync_run(run_id, status="success", summary=summary)
            return {"event_type": "REPUBLISH_SUBSCRIPTIONS", "run_id": run_id, "outbox_events": outbox_events}
        except Exception as exc:
            self._rollback_after_error()
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def publish_pending_outbox(self, *, limit: int = 1_000) -> RegistryOutboxPublishSummary:
        publish_limit = _normalise_limit(limit)
        run_id = self.repo.begin_sync_run("outbox_publish", meta={"limit": limit})
        try:
            publish_summary = self.repo.publish_pending_outbox(limit=publish_limit)
            summary = RegistryPersistSummary(outbox_events=publish_summary.events_published)
            self.repo.finish_sync_run(run_id, status="success", summary=summary, meta=publish_summary.as_meta())
            return publish_summary
        except Exception as exc:
            self._rollback_after_error()
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def repair_registry_state(self) -> RegistryRepairSummary:
        run_id = self.repo.begin_sync_run("registry_repair", meta={})
        try:
            repair_summary = self.repo.repair_registry_state(run_id=run_id)
            decisions = compute_universe_decisions(self.repo.fetch_all_state_tokens(limit=None), config=self.config)
            persist_summary = self.repo.persist_decisions(decisions, run_id=run_id, source="registry_repair")
            applied = (
                repair_summary.asset_mappings_repaired
                + repair_summary.lifecycle_regressions_repaired
                + repair_summary.token_regressions_repaired
                + repair_summary.aggregate_keys_canonicalized
                + repair_summary.aggregate_rows_archived
            )
            self.repo.finish_sync_run(
                run_id,
                status="success",
                summary=RegistryPersistSummary(
                    tokens_seen=persist_summary.tokens_seen,
                    tokens_upserted=persist_summary.tokens_upserted,
                    transitions_written=repair_summary.lifecycle_regressions_repaired + persist_summary.transitions_written,
                    outbox_events=persist_summary.outbox_events,
                    generation=persist_summary.generation,
                    subscription_count=persist_summary.subscription_count,
                    execution_count=persist_summary.execution_count,
                ),
                meta={**repair_summary.as_meta(), "repairs_applied": applied},
            )
            return repair_summary
        except Exception as exc:
            self._rollback_after_error()
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def refresh_lob_projection(self, *, ttl_seconds: int = 300) -> dict[str, int]:
        run_id = self.repo.begin_sync_run("lob_projection_refresh", meta={"ttl_seconds": ttl_seconds})
        try:
            refreshed = self.repo.refresh_lob_projection_from_targets(run_id=run_id, ttl_seconds=ttl_seconds)
            transitions = self.repo.count_lifecycle_events(run_id=run_id, source="lob_projection_refresh")
            self.repo.finish_sync_run(
                run_id,
                status="success",
                summary=RegistryPersistSummary(tokens_upserted=refreshed, transitions_written=transitions),
                meta={"lob_projection_refreshed": refreshed, "ttl_seconds": ttl_seconds},
            )
            return {"run_id": run_id, "lob_projection_refreshed": refreshed}
        except Exception as exc:
            self._rollback_after_error()
            self.repo.finish_sync_run(run_id, status="error", error=str(exc))
            raise

    def run_loop(
        self,
        *,
        poll_seconds: float = 30.0,
        probe_seconds: float = 10.0,
        full_sync_seconds: float = 1800.0,
        limit: int = 0,
        probe_limit: int = 0,
        api_limit: int | None = None,
        api_page_size: int = 500,
        probe_batch_size: int = 50,
        outbox_publish_seconds: float = 10.0,
        outbox_publish_limit: int = 5_000,
        api_delta_seconds: float = 30.0,
        api_delta_limit: int = 500,
        clob_markets_seconds: float = 120.0,
        clob_markets_page_budget: int = 10,
        clob_markets_rescan_seconds: float = 1800.0,
        stale_recheck_seconds: float = 300.0,
        stale_recheck_limit: int = 100,
        stale_recheck_source_age_seconds: int = 7 * 24 * 60 * 60,
        stale_recheck_retry_seconds: int = 6 * 60 * 60,
        stale_recheck_concurrency: int = 4,
        lob_projection_seconds: float = 300.0,
        skip_startup_probe: bool = False,
        skip_startup_lob_projection: bool = False,
        skip_startup_republish: bool = False,
        include_api: bool = True,
        periodic_full_include_api: bool = False,
        run_seconds: float = 0.0,
        once: bool = False,
        allow_start_without_full_sync: bool = False,
        skip_startup_full_sync: bool = False,
        ws_connected_provider: Callable[[], bool] | None = None,
    ) -> None:
        started = time.monotonic()
        last_probe = started
        last_outbox_publish = 0.0
        last_full_sync = 0.0
        last_delta_poll = 0.0
        last_api_delta = 0.0
        last_clob_markets = 0.0
        last_stale_recheck = 0.0
        last_lob_projection = started if skip_startup_lob_projection else 0.0
        self.repo.record_health_snapshot(
            label="starting",
            uptime_seconds=0.0,
            ws_connected=ws_connected_provider() if ws_connected_provider is not None else None,
            refresh_counts=True,
            meta={
                "include_api": include_api,
                "periodic_full_include_api": periodic_full_include_api,
                "once": once,
                "skip_startup_full_sync": skip_startup_full_sync,
            },
        )
        self._commit_after_cycle()
        last_health_counts_refresh = time.monotonic()
        projection: dict[str, Any] = {"lob_projection_skipped": True}
        if not skip_startup_lob_projection:
            projection = self.refresh_lob_projection(ttl_seconds=max(60, int(self.config.book_ttl_seconds)))
            self._commit_after_cycle()
            last_lob_projection = time.monotonic()
        if skip_startup_full_sync:
            last_full_sync = time.monotonic()
            last_delta_poll = 0.0
            last_api_delta = last_full_sync
            self.api_errors.append("startup_full_sync_skipped")
        else:
            try:
                self._run_with_deadlock_retry(
                    lambda: self.full_sync(limit=limit, include_api=include_api, api_limit=api_limit, api_page_size=api_page_size),
                    label="startup_full_sync",
                )
                self._commit_after_cycle()
                last_full_sync = time.monotonic()
                last_delta_poll = last_full_sync
                last_api_delta = last_full_sync
            except Exception as exc:
                if not allow_start_without_full_sync:
                    raise
                self.api_errors.append(f"startup_full_sync_failed: {exc}")
                self._run_with_deadlock_retry(
                    lambda: self.delta_poll(limit=limit, include_api=include_api, api_limit=api_limit, api_page_size=api_page_size),
                    label="startup_delta_poll",
                )
                self._commit_after_cycle()
                last_delta_poll = time.monotonic()
        self.repo.record_health_snapshot(
            label="startup_full_sync_complete",
            uptime_seconds=time.monotonic() - started,
            ws_connected=ws_connected_provider() if ws_connected_provider is not None else None,
            refresh_counts=True,
            meta={"include_api": include_api, "once": once, "startup_projection": projection, "skip_startup_full_sync": skip_startup_full_sync},
        )
        self._commit_after_cycle()
        last_health_counts_refresh = time.monotonic()
        if not skip_startup_republish:
            self.republish_desired_subscriptions(reason="daemon_startup")
            self._commit_after_cycle()
        if outbox_publish_seconds >= 0 and not skip_startup_republish:
            try:
                self._run_with_deadlock_retry(
                    lambda: self.publish_pending_outbox(limit=outbox_publish_limit),
                    label="startup_outbox_publish",
                )
                self._commit_after_cycle()
                last_outbox_publish = time.monotonic()
            except Exception as exc:  # noqa: BLE001 - outbox publishing must not kill discovery.
                self.api_errors.append(f"startup_outbox_publish_failed: {exc}")
        elif skip_startup_republish:
            last_outbox_publish = time.monotonic()
        self.repo.record_health_snapshot(
            label="startup_subscriptions_skipped" if skip_startup_republish else "startup_subscriptions_published",
            uptime_seconds=time.monotonic() - started,
            ws_connected=ws_connected_provider() if ws_connected_provider is not None else None,
            meta={"include_api": include_api, "once": once, "skip_startup_republish": skip_startup_republish},
        )
        self._commit_after_cycle()
        if self.api_client is not None and not skip_startup_probe:
            try:
                self._run_with_deadlock_retry(
                    lambda: self.probe_pending_books(limit=probe_limit, batch_size=probe_batch_size),
                    label="startup_book_probe",
                )
                self._commit_after_cycle()
                last_probe = time.monotonic()
            except Exception as exc:  # noqa: BLE001 - daemon should keep running after a probe cycle failure.
                self.api_errors.append(f"startup_probe_failed: {exc}")
        self.repo.record_health_snapshot(
            label="startup",
            uptime_seconds=time.monotonic() - started,
            ws_connected=ws_connected_provider() if ws_connected_provider is not None else None,
            meta={"include_api": include_api, "once": once},
        )
        self._commit_after_cycle()
        if once:
            self.repo.record_health_snapshot(
                label="once_complete",
                uptime_seconds=time.monotonic() - started,
                ws_connected=ws_connected_provider() if ws_connected_provider is not None else None,
                meta={"include_api": include_api},
            )
            self._commit_after_cycle()
            return
        while True:
            now = time.monotonic()
            if run_seconds and now - started >= float(run_seconds):
                self.repo.record_health_snapshot(
                    label="stopped",
                    uptime_seconds=now - started,
                    ws_connected=ws_connected_provider() if ws_connected_provider is not None else None,
                    meta={"reason": "run_seconds_elapsed", "run_seconds": run_seconds},
                )
                self._commit_after_cycle()
                return
            loop_meta: dict[str, Any] = {"include_api": include_api}
            if lob_projection_seconds >= 0 and now - last_lob_projection >= float(lob_projection_seconds):
                try:
                    projection = self.refresh_lob_projection(ttl_seconds=max(60, int(self.config.book_ttl_seconds)))
                    self._commit_after_cycle()
                    last_lob_projection = time.monotonic()
                    loop_meta["lob_projection_refreshed"] = projection.get("lob_projection_refreshed", 0)
                except Exception as exc:  # noqa: BLE001
                    self._rollback_after_error()
                    last_lob_projection = time.monotonic()
                    self.api_errors.append(f"lob_projection_refresh_failed: {exc}")
                    loop_meta["lob_projection_error"] = str(exc)
            now = time.monotonic()
            try:
                if full_sync_seconds > 0 and now - last_full_sync >= float(full_sync_seconds):
                    self._run_with_deadlock_retry(
                        lambda: self.full_sync(
                            limit=limit,
                            include_api=periodic_full_include_api,
                            api_limit=api_limit,
                            api_page_size=api_page_size,
                        ),
                        label="loop_full_sync",
                    )
                    self._commit_after_cycle()
                    last_full_sync = time.monotonic()
                    last_delta_poll = last_full_sync
                    loop_meta["cycle"] = "full_sync"
                    loop_meta["full_sync_include_api"] = periodic_full_include_api
                elif poll_seconds > 0 and now - last_delta_poll >= float(poll_seconds):
                    self._run_with_deadlock_retry(
                        lambda: self.delta_poll(limit=limit, include_api=False, api_limit=api_limit, api_page_size=api_page_size),
                        label="loop_delta_poll",
                    )
                    self._commit_after_cycle()
                    last_delta_poll = time.monotonic()
                    loop_meta["cycle"] = "delta_poll"
            except Exception as exc:  # noqa: BLE001 - one failing registry cycle must not kill the daemon.
                self._rollback_after_error()
                self.api_errors.append(f"poll_loop_failed: {exc}")
                loop_meta["poll_error"] = str(exc)
            if include_api and api_delta_seconds > 0 and now - last_api_delta >= float(api_delta_seconds):
                try:
                    self._run_with_deadlock_retry(
                        lambda: self.api_delta_refresh(api_limit=api_delta_limit, api_page_size=api_page_size),
                        label="loop_api_delta_refresh",
                    )
                    self._commit_after_cycle()
                    last_api_delta = time.monotonic()
                    loop_meta["api_delta_cycle"] = "api_delta_refresh"
                except Exception as exc:  # noqa: BLE001 - supplemental API failure must not starve other cycles.
                    self._rollback_after_error()
                    last_api_delta = time.monotonic()
                    self.api_errors.append(f"api_delta_loop_failed: {exc}")
                    loop_meta["api_delta_error"] = str(exc)
            now = time.monotonic()
            if include_api and clob_markets_seconds >= 0 and now - last_clob_markets >= float(clob_markets_seconds):
                try:
                    clob_summary = self._run_with_deadlock_retry(
                        lambda: self.clob_markets_refresh(
                            page_budget=clob_markets_page_budget,
                            rescan_seconds=clob_markets_rescan_seconds,
                        ),
                        label="loop_clob_markets_refresh",
                    )
                    self._commit_after_cycle()
                    last_clob_markets = time.monotonic()
                    loop_meta["clob_markets_cycle"] = "clob_markets_refresh"
                    loop_meta["clob_markets_tokens_seen"] = clob_summary.api_tokens_seen
                except Exception as exc:  # noqa: BLE001 - CLOB pagination is supplemental to full/delta sync.
                    self._rollback_after_error()
                    last_clob_markets = time.monotonic()
                    self.api_errors.append(f"clob_markets_loop_failed: {exc}")
                    loop_meta["clob_markets_error"] = str(exc)
            now = time.monotonic()
            if (
                include_api
                and stale_recheck_seconds > 0
                and now - last_stale_recheck >= float(stale_recheck_seconds)
            ):
                try:
                    stale_summary = self._run_with_deadlock_retry(
                        lambda: self.recheck_stale_markets(
                            limit=stale_recheck_limit,
                            source_age_seconds=stale_recheck_source_age_seconds,
                            retry_seconds=stale_recheck_retry_seconds,
                            probe_batch_size=probe_batch_size,
                            concurrency=stale_recheck_concurrency,
                        ),
                        label="loop_targeted_stale_status_recheck",
                    )
                    self._commit_after_cycle()
                    last_stale_recheck = time.monotonic()
                    loop_meta["stale_recheck_markets_checked"] = stale_summary.markets_checked
                    loop_meta["stale_recheck_markets_deactivated"] = stale_summary.markets_deactivated
                except Exception as exc:  # noqa: BLE001
                    self._rollback_after_error()
                    last_stale_recheck = time.monotonic()
                    self.api_errors.append(f"stale_recheck_loop_failed: {exc}")
                    loop_meta["stale_recheck_error"] = str(exc)
            now = time.monotonic()
            if self.api_client is not None and now - last_probe >= float(probe_seconds):
                try:
                    self._run_with_deadlock_retry(
                        lambda: self.probe_pending_books(limit=probe_limit, batch_size=probe_batch_size),
                        label="loop_book_probe",
                    )
                    self._commit_after_cycle()
                    last_probe = time.monotonic()
                    loop_meta["probe_cycle"] = "book_probe"
                except Exception as exc:  # noqa: BLE001
                    self.api_errors.append(f"probe_loop_failed: {exc}")
                    loop_meta["probe_error"] = str(exc)
            if outbox_publish_seconds >= 0 and now - last_outbox_publish >= float(outbox_publish_seconds):
                try:
                    publish_summary = self._run_with_deadlock_retry(
                        lambda: self.publish_pending_outbox(limit=outbox_publish_limit),
                        label="loop_outbox_publish",
                    )
                    self._commit_after_cycle()
                    last_outbox_publish = time.monotonic()
                    loop_meta["outbox_published"] = publish_summary.events_published
                except Exception as exc:  # noqa: BLE001
                    self.api_errors.append(f"outbox_publish_failed: {exc}")
                    loop_meta["outbox_publish_error"] = str(exc)
            refresh_health_counts = (
                time.monotonic() - last_health_counts_refresh >= _HEALTH_COUNTS_REFRESH_SECONDS
            )
            self.repo.record_health_snapshot(
                label="loop",
                uptime_seconds=time.monotonic() - started,
                ws_connected=ws_connected_provider() if ws_connected_provider is not None else None,
                refresh_counts=refresh_health_counts,
                meta=loop_meta,
            )
            self._commit_after_cycle()
            if refresh_health_counts:
                last_health_counts_refresh = time.monotonic()
            time.sleep(_loop_sleep_seconds(
                poll_seconds=poll_seconds,
                probe_seconds=probe_seconds,
                full_sync_seconds=full_sync_seconds,
                outbox_publish_seconds=outbox_publish_seconds,
                api_delta_seconds=api_delta_seconds,
                clob_markets_seconds=clob_markets_seconds,
                now=time.monotonic(),
                last_delta_poll=last_delta_poll,
                last_probe=last_probe,
                last_full_sync=last_full_sync,
                last_outbox_publish=last_outbox_publish,
                last_api_delta=last_api_delta,
                last_clob_markets=last_clob_markets,
            ))

    def _fetch_api_tokens(
        self,
        *,
        limit: int | None,
        page_size: int = 500,
        require_full: bool = True,
        include_clob_markets: bool = True,
    ) -> tuple[list[MarketRegistryToken], bool]:
        if self.api_client is None:
            return [], False
        try:
            tokens = self.api_client.fetch_api_market_tokens(
                limit=limit,
                page_size=page_size,
                include_clob_markets=include_clob_markets,
            )
            discovery_errors = list(getattr(self.api_client, "discovery_errors", []) or [])
            if discovery_errors:
                self.api_errors.extend(discovery_errors)
            return tokens, bool(require_full and not discovery_errors)
        except Exception as exc:  # noqa: BLE001 - API is supplemental; DB remains authoritative.
            self.api_errors.append(str(exc))
            return [], False

    def _fetch_api_delta_tokens(
        self,
        *,
        limit: int | None,
        page_size: int = 100,
        updated_since: datetime | None = None,
    ) -> tuple[list[MarketRegistryToken], bool, datetime | None]:
        if self.api_client is None:
            return [], False, None
        errors: list[str] = []
        rows: list[Mapping[str, Any]] = []
        recent_limit = min(500, max(1, int(limit or 500)))
        recent_loaded = False
        reached_watermark = updated_since is None
        try:
            if updated_since is not None:
                open_rows, reached_open_watermark = self.api_client.fetch_gamma_recent_keyset_markets_since(
                    updated_since=updated_since,
                    max_markets=max(_API_DELTA_MAX_ROWS, recent_limit),
                    page_size=page_size,
                    closed=False,
                )
                closed_rows, reached_closed_watermark = self.api_client.fetch_gamma_recent_keyset_markets_since(
                    updated_since=updated_since,
                    max_markets=max(_API_DELTA_MAX_ROWS, recent_limit),
                    page_size=page_size,
                    closed=True,
                )
                rows.extend(_latest_gamma_market_rows([*open_rows, *closed_rows]))
                reached_watermark = reached_open_watermark and reached_closed_watermark
            else:
                rows.extend(
                    self.api_client.fetch_gamma_recent_keyset_markets_all(
                        max_markets=recent_limit,
                        page_size=page_size,
                    )
                )
            recent_loaded = bool(rows)
        except Exception as exc:  # noqa: BLE001 - active listings remain a fallback for discovery.
            errors.append(f"markets_recent: {exc}")
        if not recent_loaded:
            market_loaders = (
                ("markets_keyset", lambda: self.api_client.fetch_gamma_keyset_markets_all(max_markets=limit, page_size=page_size)),
                ("markets_offset", lambda: self.api_client.fetch_gamma_active_markets_all(max_markets=limit, page_size=page_size)),
            )
            for name, loader in market_loaders:
                try:
                    active_rows = loader()
                    if active_rows:
                        rows.extend(active_rows)
                        break
                except Exception as exc:  # noqa: BLE001 - delta can fall back to the next Gamma listing.
                    errors.append(f"{name}: {exc}")
            event_limit = max(200, min(500, int(limit or 500)))
            event_loaders = (
                ("events_keyset", lambda: self.api_client.fetch_gamma_keyset_events_all(max_events=event_limit, page_size=page_size)),
                ("events_offset", lambda: self.api_client.fetch_gamma_active_events_all(max_events=event_limit, page_size=page_size)),
            )
            for name, loader in event_loaders:
                try:
                    event_rows = loader()
                    if event_rows:
                        rows.extend(markets_from_gamma_events(event_rows))
                        break
                except Exception as exc:  # noqa: BLE001 - events are supplemental to latest markets.
                    errors.append(f"{name}: {exc}")
        latest_updated_at = max(
            (
                value
                for value in (
                    _datetime_from_state(row.get("updatedAt") or row.get("updated_at"))
                    for row in rows
                )
                if value is not None
            ),
            default=None,
        )
        if errors:
            self.api_errors.extend(errors)
        tokens: list[MarketRegistryToken] = []
        seen: set[str] = set()
        for row in rows:
            for token in tokens_from_gamma_market(row):
                if not token.asset_id or token.asset_id in seen:
                    continue
                seen.add(token.asset_id)
                tokens.append(token)
        return tokens, bool(recent_loaded and reached_watermark and not errors), latest_updated_at

    def _rollback_after_error(self) -> None:
        try:
            self.conn.rollback()
        except Exception:
            pass

    def _run_with_deadlock_retry(
        self,
        action: Callable[[], Any],
        *,
        label: str,
        attempts: int = 8,
    ) -> Any:
        last_exc: Exception | None = None
        for attempt in range(max(1, int(attempts))):
            try:
                return action()
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if not _is_deadlock(exc) or attempt >= max(1, int(attempts)) - 1:
                    raise
                self._rollback_after_error()
                delay = min(10.0, 0.75 * (attempt + 1))
                self.api_errors.append(f"{label}_deadlock_retry_{attempt + 1}: {exc}")
                time.sleep(delay)
        if last_exc is not None:
            raise last_exc
        return action()

    def _commit_after_cycle(self) -> None:
        try:
            self.conn.commit()
        except Exception:
            self._rollback_after_error()
            raise


def _merge_tokens(*groups: list[MarketRegistryToken]) -> list[MarketRegistryToken]:
    merged: dict[str, MarketRegistryToken] = {}
    for group in groups:
        for token in group:
            if not token.asset_id:
                continue
            existing = merged.get(token.asset_id)
            if existing is None:
                merged[token.asset_id] = token
                continue
            if _token_rank(token) >= _token_rank(existing):
                merged[token.asset_id] = _carry_book_evidence(token, existing)
            else:
                merged[token.asset_id] = _carry_book_evidence(existing, token)
    return list(merged.values())


def _latest_gamma_market_rows(rows: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    latest: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        key = str(
            row.get("conditionId")
            or row.get("condition_id")
            or row.get("id")
            or row.get("slug")
            or ""
        ).strip()
        if not key:
            continue
        previous = latest.get(key)
        if previous is None:
            latest[key] = row
            continue
        previous_at = _datetime_from_state(previous.get("updatedAt") or previous.get("updated_at"))
        current_at = _datetime_from_state(row.get("updatedAt") or row.get("updated_at"))
        if current_at is not None and (previous_at is None or current_at > previous_at):
            latest[key] = row
            continue
        if current_at == previous_at and bool(row.get("closed")) and not bool(previous.get("closed")):
            latest[key] = row
    return list(latest.values())


def _append_absent_registry_removals(
    tokens: list[MarketRegistryToken],
    *,
    known_tokens: list[MarketRegistryToken],
) -> tuple[list[MarketRegistryToken], int]:
    current_asset_ids = {token.asset_id for token in tokens if token.asset_id}
    removed: list[MarketRegistryToken] = []
    for token in known_tokens:
        if not token.asset_id or token.asset_id in current_asset_ids:
            continue
        if token.closed or token.resolved or not token.active:
            removed.append(token)
            continue
        removed.append(
            replace(
                token,
                active=False,
                closed=True,
                resolved=False,
                status_present=True,
                completion_status="REMOVED_FROM_ACTIVE_UNIVERSE",
                latest_book_at=token.latest_book_at,
                source="registry_absent_reconcile",
            )
        )
    return _merge_tokens(tokens, removed), len(removed)


def _reconcile_authoritative_active_snapshot(
    *,
    active_tokens: list[MarketRegistryToken],
    known_tokens: list[MarketRegistryToken],
    authoritative_complete: bool,
    absence_is_lifecycle_truth: bool = False,
) -> tuple[list[MarketRegistryToken], int]:
    """Merge active truth with history without treating discovery gaps as closure.

    Gamma/CLOB active listings are discovery feeds, not lifecycle truth for rows
    they omit. Callers may opt in only when the source contract explicitly
    guarantees that absence means closed/inactive.
    """

    active_asset_ids = {token.asset_id for token in active_tokens if token.asset_id}
    matching_history = [token for token in known_tokens if token.asset_id in active_asset_ids]
    current_tokens = _merge_tokens(active_tokens, matching_history)
    if authoritative_complete and absence_is_lifecycle_truth:
        return _append_absent_registry_removals(current_tokens, known_tokens=known_tokens)
    return _merge_tokens(known_tokens, current_tokens), 0


def _source_rank(source: str | None) -> int:
    text = str(source or "").lower()
    if text.startswith("core"):
        return 30
    if "gamma" in text:
        return 20
    if "clob" in text:
        return 15
    if text in {"full_sync", "delta_poll", "book_probe", "ws_lifecycle"} or "registry" in text:
        return 5
    return 0


def _token_rank(token: MarketRegistryToken) -> int:
    rank = _source_rank(token.source)
    # A source with explicit lifecycle truth must beat a nominally stronger
    # source whose status row is absent. In particular, CLOB /markets is the
    # active/open supplement for core markets without market_status_snapshot.
    if token.status_present:
        rank += 40
    if token.is_placeholder:
        rank -= 100
    if token.condition_id and token.market_slug and not token.is_placeholder:
        rank += 2
    return rank


def _carry_book_evidence(winner: MarketRegistryToken, fallback: MarketRegistryToken) -> MarketRegistryToken:
    # Asset ids are CLOB identities and cannot legitimately move between
    # conditions. Correct stale core mappings when an API row supplies the
    # current asset-to-condition relationship.
    if (
        _is_api_identity_token(fallback)
        and fallback.condition_id
        and fallback.condition_id != winner.condition_id
        and not winner.resolved
        and not winner.archived
    ):
        winner = replace(
            winner,
            market_id=fallback.market_id,
            gamma_market_id=fallback.gamma_market_id or winner.gamma_market_id,
            condition_id=fallback.condition_id,
            market_slug=fallback.market_slug or winner.market_slug,
            market_title=fallback.market_title or winner.market_title,
            outcome_name=(
                fallback.outcome_name
                if fallback.outcome_name and fallback.outcome_name != "UNKNOWN"
                else winner.outcome_name
            ),
            outcome_index=(
                fallback.outcome_index
                if fallback.outcome_index is not None
                else winner.outcome_index
            ),
            token_count=fallback.token_count or winner.token_count,
            end_date=fallback.end_date or winner.end_date,
        )
    # Core can carry richer identity metadata while Gamma/CLOB carries the
    # current explicit open status. Keep that provenance for the reopen guard.
    if (
        _is_explicit_api_terminal_token(fallback)
        and not winner.resolved
        and not winner.archived
    ):
        winner = replace(
            winner,
            active=fallback.active,
            closed=fallback.closed,
            resolved=fallback.resolved,
            archived=fallback.archived,
            status_present=True,
            completion_status=fallback.completion_status,
            winning_asset_id=fallback.winning_asset_id or winner.winning_asset_id,
            winning_outcome=fallback.winning_outcome or winner.winning_outcome,
            resolution_status=fallback.resolution_status or winner.resolution_status,
            resolution_source=fallback.resolution_source or winner.resolution_source,
            resolved_time=fallback.resolved_time or winner.resolved_time,
            source=fallback.source,
        )
    if (
        _is_trusted_gamma_open_token(fallback)
        and not winner.resolved
        and not winner.archived
    ):
        winner = replace(
            winner,
            active=True,
            closed=False,
            deprecated=False,
            status_present=True,
            completion_status=fallback.completion_status or "OPEN",
            source=fallback.source,
        )
    if (
        _is_explicitly_open(winner)
        and not _is_trusted_open_api_token(winner)
        and _is_trusted_open_api_token(fallback)
    ):
        winner = replace(winner, source=fallback.source)
    if fallback.latest_book_at is None:
        return winner
    if winner.latest_book_at is not None and _ensure_aware(winner.latest_book_at) >= _ensure_aware(fallback.latest_book_at):
        return winner
    return replace(
        winner,
        latest_book_at=fallback.latest_book_at,
        book_status=fallback.book_status,
        best_bid=fallback.best_bid,
        best_ask=fallback.best_ask,
        book_source=fallback.book_source,
        storage_tier=fallback.storage_tier,
    )


def _is_explicitly_open(token: MarketRegistryToken) -> bool:
    return (
        token.status_present
        and token.active
        and not token.closed
        and not token.resolved
        and not token.archived
        and not token.deprecated
    )


def _is_trusted_open_api_token(token: MarketRegistryToken) -> bool:
    return (
        str(token.source or "").strip().lower() in {
            "gamma_api",
            "gamma_api_open_book",
            "clob_markets_api",
        }
        and _is_explicitly_open(token)
    )


def _is_trusted_gamma_open_token(token: MarketRegistryToken) -> bool:
    return (
        str(token.source or "").strip().lower() in {"gamma_api", "gamma_api_open_book"}
        and _is_explicitly_open(token)
    )


def _is_explicit_api_terminal_token(token: MarketRegistryToken) -> bool:
    return (
        str(token.source or "").strip().lower() in {"gamma_api", "gamma_api_open_book"}
        and token.status_present
        and (not token.active or token.closed or token.resolved or token.archived)
    )


def _is_api_identity_token(token: MarketRegistryToken) -> bool:
    return str(token.source or "").strip().lower() in {
        "gamma_api",
        "gamma_api_open_book",
        "clob_markets_api",
    }


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalise_limit(limit: int | None) -> int | None:
    if limit is None:
        return None
    value = int(limit)
    return None if value <= 0 else value


def _is_terminal_clob_cursor(value: str | None) -> bool:
    return str(value or "").strip() in {"-1", "LTE="}


def _state_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _datetime_from_state(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _loop_sleep_seconds(
    *,
    poll_seconds: float,
    probe_seconds: float,
    full_sync_seconds: float,
    outbox_publish_seconds: float,
    api_delta_seconds: float,
    clob_markets_seconds: float,
    now: float,
    last_delta_poll: float,
    last_probe: float,
    last_full_sync: float,
    last_outbox_publish: float,
    last_api_delta: float,
    last_clob_markets: float,
) -> float:
    candidates = [5.0]
    intervals = (
        (poll_seconds, last_delta_poll),
        (probe_seconds, last_probe),
        (full_sync_seconds, last_full_sync),
        (outbox_publish_seconds, last_outbox_publish),
        (api_delta_seconds, last_api_delta),
        (clob_markets_seconds, last_clob_markets),
    )
    for interval, last_run in intervals:
        if interval < 0:
            continue
        if interval == 0:
            candidates.append(0.1)
            continue
        candidates.append(max(0.1, float(interval) - (now - float(last_run))))
    return max(0.1, min(candidates))


def _has_resolution_truth(event: Mapping[str, Any]) -> bool:
    return any(
        _truthy_text(event.get(key))
        for key in (
            "winning_asset_id",
            "winningAssetId",
            "winning_outcome",
            "winningOutcome",
            "oracle_result",
            "oracleResult",
            "resolved_outcome",
            "resolvedOutcome",
        )
    )


def _truthy_text(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(text) and text.lower() not in {"none", "null", "unknown"}


def _is_deadlock(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    text = str(exc).lower()
    return "deadlock" in name or "deadlock detected" in text


def _cycle_summary(
    sync_type: str,
    run_id: int,
    summary: RegistryPersistSummary,
    *,
    api_tokens_seen: int = 0,
    db_tokens_seen: int = 0,
    probes_attempted: int = 0,
    probes_ok: int = 0,
    removed_absent_count: int = 0,
    api_complete: bool = False,
) -> RegistryCycleSummary:
    return RegistryCycleSummary(
        sync_type=sync_type,
        run_id=run_id,
        tokens_seen=summary.tokens_seen,
        tokens_upserted=summary.tokens_upserted,
        transitions_written=summary.transitions_written,
        outbox_events=summary.outbox_events,
        generation=summary.generation,
        subscription_count=summary.subscription_count,
        execution_count=summary.execution_count,
        api_tokens_seen=api_tokens_seen,
        db_tokens_seen=db_tokens_seen,
        probes_attempted=probes_attempted,
        probes_ok=probes_ok,
        removed_absent_count=removed_absent_count,
        api_complete=api_complete,
    )
