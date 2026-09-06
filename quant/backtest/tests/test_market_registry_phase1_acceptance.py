from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from quant.market.registry_daemon import (
    _EmbeddedDeltaPollWorker,
    _EmbeddedRegistryHeartbeatWorker,
    _EmbeddedWsLifecycleWorker,
    _bounded_cycle_limit,
    _decode_ws_payload,
    _positive_float_or_none,
    _process_ws_lifecycle_event,
    _reconnect_delay,
    _send_ws_subscription_batches,
    _ws_proxy_candidates,
    _ws_proxy_from_args,
    build_parser,
)
from quant.market.registry_soak_monitor import _pending_outbox_stale, _ws_healthy
from quant.market.existing_registry import ExistingMarketRegistry
from quant.market.repository import (
    MarketRegistryRepository,
    RegistryPersistSummary,
    _changed_market_keys,
    _decision_market_identity_changed,
    _guard_illegal_state_regression,
    _lifecycle_event_types,
    _market_key,
    _outbox_matches_current_state,
)
from quant.market.api_client import BookProbeResult
from quant.market.service import (
    MarketRegistryService,
    _has_resolution_truth,
    _is_deadlock,
    _latest_gamma_market_rows,
    _merge_tokens,
)
from quant.market.clob_book_probe import ClobBookProbe
from quant.market.cli import _force_recheck_assets
from quant.market.token_universe import (
    MarketRegistryToken,
    MarketUniverseConfig,
    TokenUniverseDiff,
    UniverseDecision,
    build_token_universe_diff,
    compute_universe_decisions,
)


NOW = datetime(2026, 7, 6, 8, 0, tzinfo=timezone.utc)


def test_unbounded_registry_read_does_not_sort_the_full_wide_universe() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.sql = ""
            self.params: list[object] = []
            self.statements: list[tuple[str, object]] = []

        def __enter__(self):
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, params: object = None) -> None:
            self.sql = sql
            self.params = list(params or [])
            self.statements.append((sql, params))

        def fetchone(self) -> dict[str, str]:
            return {"work_mem": "4MB"}

        def fetchall(self) -> list[object]:
            return []

    class FakeConnection:
        def __init__(self) -> None:
            self.last_cursor: FakeCursor | None = None

        def cursor(self) -> FakeCursor:
            self.last_cursor = FakeCursor()
            return self.last_cursor

    connection = FakeConnection()
    registry = ExistingMarketRegistry(connection)

    assert registry.fetch_tokens(limit=None, include_book=False) == []
    assert connection.last_cursor is not None
    full_query = next(
        sql
        for sql, _params in connection.last_cursor.statements
        if "FROM core.market_tokens mt" in sql
    )
    assert "ORDER BY m.created_at" not in full_query
    assert "quant.clob_orderbook_snapshots" not in full_query
    assert any("current_setting('work_mem')" in sql for sql, _ in connection.last_cursor.statements)
    set_values = [params for sql, params in connection.last_cursor.statements if "set_config('work_mem'" in sql]
    assert set_values == [("512MB",), ("4MB",)]

    assert registry.fetch_tokens(limit=10, include_book=False) == []
    assert connection.last_cursor is not None
    assert "ORDER BY m.created_at" in connection.last_cursor.sql
    assert connection.last_cursor.params[-1] == 10


def test_gamma_delta_merge_keeps_the_latest_open_or_closed_version() -> None:
    rows = _latest_gamma_market_rows([
        {
            "id": "market-1",
            "conditionId": "condition-1",
            "closed": True,
            "updatedAt": "2026-07-23T13:30:00Z",
        },
        {
            "id": "market-1",
            "conditionId": "condition-1",
            "closed": False,
            "updatedAt": "2026-07-23T13:32:00Z",
        },
    ])

    assert len(rows) == 1
    assert rows[0]["closed"] is False


class _HealthCursor:
    def __init__(self, *, has_previous: bool = True) -> None:
        self.has_previous = has_previous
        self.queries: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, query: str, _params=None) -> None:
        self.queries.append(query)

    def fetchone(self):
        query = self.queries[-1]
        if "WITH registry_counts AS" in query:
            return {
                "tokens_total": 100,
                "subscription_universe_count": 80,
                "execution_universe_count": 25,
                "stale_count": 10,
                "pending_book_count": 5,
                "pending_outbox_count": 0,
                "generation": 7,
                "last_full_sync_at": NOW,
                "last_delta_poll_at": NOW,
                "latest_ws_connected_at": datetime.now(timezone.utc),
            }
        return {
            "has_previous": self.has_previous,
            "tokens_total": 1,
            "subscription_universe_count": 1,
            "execution_universe_count": 1,
            "stale_count": 0,
            "pending_book_count": 0,
            "pending_outbox_count": 0,
            "generation": 6,
            "last_full_sync_at": NOW,
            "last_delta_poll_at": NOW,
            "latest_ws_connected_at": datetime.now(timezone.utc),
        }


class _HealthConnection:
    def __init__(self, *, has_previous: bool = True) -> None:
        self.cursor_obj = _HealthCursor(has_previous=has_previous)

    def cursor(self) -> _HealthCursor:
        return self.cursor_obj


def _token(asset_id: str, **overrides: object) -> MarketRegistryToken:
    base = {
        "asset_id": asset_id,
        "market_id": 100,
        "condition_id": "0xcond",
        "gamma_market_id": "gamma-100",
        "market_slug": "demo-market",
        "market_title": "Demo market",
        "outcome_name": "YES" if asset_id.endswith("yes") else "NO",
        "outcome_index": 0 if asset_id.endswith("yes") else 1,
        "active": True,
        "closed": False,
        "resolved": False,
        "archived": False,
        "deprecated": False,
        "status_present": True,
        "completion_status": "OPEN",
        "token_count": 2,
    }
    base.update(overrides)
    return MarketRegistryToken(**base)


def test_health_status_refreshes_live_registry_and_execution_counts() -> None:
    connection = _HealthConnection()

    status = MarketRegistryRepository(connection).health_snapshot_status(refresh_counts=True)

    assert status["tokens_total"] == 100
    assert status["subscription_universe_count"] == 80
    assert status["execution_universe_count"] == 25
    assert status["ws_connected"] is True
    assert "paper_execution_market_registry_tokens" in connection.cursor_obj.queries[-1]


def test_health_status_bootstraps_live_counts_when_cache_is_empty() -> None:
    connection = _HealthConnection(has_previous=False)

    status = MarketRegistryRepository(connection).health_snapshot_status()

    assert status["tokens_total"] == 100
    assert len(connection.cursor_obj.queries) == 2
    assert "WITH previous AS" in connection.cursor_obj.queries[0]
    assert "WITH registry_counts AS" in connection.cursor_obj.queries[1]


@dataclass
class _Harness:
    config: MarketUniverseConfig
    tokens: dict[str, MarketRegistryToken]
    previous: TokenUniverseDiff | None = None
    outbox: list[tuple[str, str]] | None = None
    sync_status: list[str] | None = None
    resolution_facts: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if self.outbox is None:
            self.outbox = []
        if self.sync_status is None:
            self.sync_status = []
        if self.resolution_facts is None:
            self.resolution_facts = {}

    def full_sync(self, rows: list[MarketRegistryToken], *, now: datetime = NOW) -> list[UniverseDecision]:
        for row in rows:
            self.tokens[row.asset_id] = row
        self.sync_status.append("SUCCESS")
        return self.recompute(now=now)

    def recompute(self, *, now: datetime = NOW) -> list[UniverseDecision]:
        decisions = compute_universe_decisions(self.tokens.values(), config=self.config, now=now)
        generation = 1 if self.previous is None else self.previous.generation + 1
        diff = build_token_universe_diff(decisions, previous=self.previous, generation=generation)
        self._publish(diff)
        self.previous = diff
        return decisions

    def mark_book_ready(self, asset_id: str, *, now: datetime = NOW) -> list[UniverseDecision]:
        self.tokens[asset_id] = replace(
            self.tokens[asset_id],
            latest_book_at=now,
            book_status="ok",
            best_bid=Decimal("0.49"),
            best_ask=Decimal("0.51"),
        )
        return self.recompute(now=now)

    def mark_closed(self, condition_id: str, *, now: datetime = NOW) -> list[UniverseDecision]:
        for asset_id, token in list(self.tokens.items()):
            if token.condition_id == condition_id:
                self.tokens[asset_id] = replace(token, active=False, closed=True, resolved=False)
        return self.recompute(now=now)

    def mark_resolved(self, condition_id: str, winning_asset_id: str, *, now: datetime = NOW) -> list[UniverseDecision]:
        self.resolution_facts[condition_id] = winning_asset_id
        for asset_id, token in list(self.tokens.items()):
            if token.condition_id == condition_id:
                self.tokens[asset_id] = replace(token, active=False, closed=True, resolved=True)
        return self.recompute(now=now)

    def restart(self) -> "_Harness":
        return _Harness(
            config=self.config,
            tokens=dict(self.tokens),
            previous=self.previous,
            outbox=[],
            sync_status=[],
            resolution_facts=dict(self.resolution_facts),
        )

    def republish_current_subscriptions(self) -> None:
        assert self.previous is not None
        for asset_id in sorted(self.previous.subscription_asset_ids):
            self.outbox.append(("ASSET_SUBSCRIBE_REQUESTED", asset_id))

    def _publish(self, diff: TokenUniverseDiff) -> None:
        for asset_id in sorted(diff.added_subscription_asset_ids):
            self.outbox.append(("ASSET_SUBSCRIBE_REQUESTED", asset_id))
        for asset_id in sorted(diff.removed_subscription_asset_ids):
            self.outbox.append(("ASSET_UNSUBSCRIBE_REQUESTED", asset_id))


def _decision(decisions: list[UniverseDecision], asset_id: str) -> UniverseDecision:
    return next(decision for decision in decisions if decision.asset_id == asset_id)


def test_case_1_empty_database_startup_full_sync_populates_subscription_and_outbox() -> None:
    harness = _Harness(MarketUniverseConfig(), {})
    decisions = harness.full_sync([_token("asset-yes"), _token("asset-no")])

    assert harness.sync_status == ["SUCCESS"]
    assert {decision.asset_id for decision in decisions if decision.subscription_eligible} == {"asset-yes", "asset-no"}
    assert ("ASSET_SUBSCRIBE_REQUESTED", "asset-yes") in harness.outbox


def test_registry_bulk_writes_lifecycle_and_token_rows_in_two_batches() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def executemany(self, sql: str, rows: list[tuple[object, ...]]) -> None:
            self.executemany_calls.append((sql, rows))

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self) -> FakeCursor:
            return self.cursor_obj

    conn = FakeConnection()
    repo = MarketRegistryRepository(conn)
    decisions = compute_universe_decisions([_token("asset-yes"), _token("asset-no")], now=NOW)

    transitions, upserts = repo._upsert_tokens_and_events(
        decisions,
        {},
        run_id=101,
        source="test",
    )

    assert transitions == 4
    assert upserts == 2
    assert len(conn.cursor_obj.executemany_calls) == 2
    assert "paper_market_lifecycle_events" in conn.cursor_obj.executemany_calls[0][0]
    assert len(conn.cursor_obj.executemany_calls[0][1]) == 4
    assert "paper_market_registry_tokens" in conn.cursor_obj.executemany_calls[1][0]
    assert len(conn.cursor_obj.executemany_calls[1][1]) == 2


def test_full_persist_publishes_from_current_rows_not_stale_snapshot(monkeypatch) -> None:
    repo = MarketRegistryRepository(object())
    decisions = compute_universe_decisions([_token("asset-yes"), _token("asset-no")], now=NOW)
    calls: list[str] = []

    monkeypatch.setattr(repo, "latest_universe_diff", lambda: None)
    monkeypatch.setattr(repo, "_previous_states", lambda _asset_ids: {})
    monkeypatch.setattr(repo, "_guard_decision_state_regressions", lambda rows, _previous: list(rows))
    monkeypatch.setattr(repo, "_upsert_tokens_and_events", lambda *_args, **_kwargs: (4, 2))
    monkeypatch.setattr(repo, "_upsert_market_aggregates", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "insert_universe_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "repair_ineligible_subscription_targets", lambda **_kwargs: 0)

    def publish_current(*_args: object, **_kwargs: object) -> int:
        calls.append("current")
        return 3

    def publish_stale_diff(*_args: object, **_kwargs: object) -> int:
        raise AssertionError("full sync must not replay the stale universe diff")

    monkeypatch.setattr(repo, "upsert_subscription_targets_for_decisions", publish_current)
    monkeypatch.setattr(repo, "write_outbox_for_diff", publish_stale_diff)

    summary = repo.persist_decisions(decisions, run_id=101, source="full_sync")

    assert calls == ["current"]
    assert summary.outbox_events == 3


def test_targeted_subscription_repair_never_scans_unrelated_assets() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.sql = ""
            self.params: tuple[object, ...] = ()

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[object, ...]) -> None:
            self.sql = sql
            self.params = params

        def fetchone(self) -> dict[str, int]:
            return {"repaired": 0}

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self) -> FakeCursor:
            return self.cursor_obj

    conn = FakeConnection()
    repo = MarketRegistryRepository(conn)

    assert repo.repair_ineligible_subscription_targets(
        run_id=101,
        asset_ids=["asset-b", "asset-a", "asset-a"],
    ) == 0
    assert "t.asset_id = ANY(%s)" in conn.cursor_obj.sql
    assert conn.cursor_obj.params == (["asset-a", "asset-b"], 101)


def test_empty_targeted_subscription_repair_is_a_noop() -> None:
    class NoCursorConnection:
        def cursor(self) -> object:
            raise AssertionError("empty target scope must not execute SQL")

    repo = MarketRegistryRepository(NoCursorConnection())

    assert repo.repair_ineligible_subscription_targets(
        run_id=101,
        asset_ids=[],
    ) == 0


def test_case_2_new_market_added_then_book_ready_enters_execution() -> None:
    harness = _Harness(MarketUniverseConfig(), {})
    harness.full_sync([_token("asset-yes"), _token("asset-no")])
    decisions = harness.full_sync([_token("asset-new-yes", market_id=101), _token("asset-new-no", market_id=101)])

    assert _decision(decisions, "asset-new-yes").market_state == "TRADABLE_PENDING_BOOK"
    assert ("ASSET_SUBSCRIBE_REQUESTED", "asset-new-yes") in harness.outbox

    decisions = harness.mark_book_ready("asset-new-yes")

    assert _decision(decisions, "asset-new-yes").market_state == "LIVE"
    assert _decision(decisions, "asset-new-yes").execution_eligible is True


def test_case_3_new_market_without_book_stays_pending_and_not_executable() -> None:
    harness = _Harness(MarketUniverseConfig(), {})
    decisions = harness.full_sync([_token("asset-yes"), _token("asset-no")])

    assert _decision(decisions, "asset-yes").market_state == "TRADABLE_PENDING_BOOK"
    assert _decision(decisions, "asset-yes").subscription_eligible is True
    assert _decision(decisions, "asset-yes").execution_eligible is False


def test_case_4_book_stale_removes_execution_but_keeps_subscription() -> None:
    harness = _Harness(MarketUniverseConfig(book_ttl_seconds=60), {})
    harness.full_sync([_token("asset-yes"), _token("asset-no")])
    harness.mark_book_ready("asset-yes", now=NOW)

    decisions = harness.recompute(now=NOW + timedelta(seconds=120))

    assert _decision(decisions, "asset-yes").market_state == "STALE"
    assert _decision(decisions, "asset-yes").subscription_eligible is True
    assert _decision(decisions, "asset-yes").execution_eligible is False


def test_case_5_market_closed_not_resolved_enters_closing_without_settlement_truth() -> None:
    harness = _Harness(MarketUniverseConfig(), {})
    harness.full_sync([_token("asset-yes"), _token("asset-no")])
    harness.mark_book_ready("asset-yes")

    decisions = harness.mark_closed("0xcond")

    assert _decision(decisions, "asset-yes").market_state == "CLOSING"
    assert _decision(decisions, "asset-yes").execution_eligible is False
    assert harness.resolution_facts == {}
    assert ("ASSET_UNSUBSCRIBE_REQUESTED", "asset-yes") in harness.outbox


def test_case_6_market_resolved_records_truth_and_removes_both_universes() -> None:
    harness = _Harness(MarketUniverseConfig(), {})
    harness.full_sync([_token("asset-yes"), _token("asset-no")])
    harness.mark_book_ready("asset-yes")

    decisions = harness.mark_resolved("0xcond", "asset-yes")

    assert harness.resolution_facts["0xcond"] == "asset-yes"
    assert _decision(decisions, "asset-yes").market_state == "RESOLVED"
    assert _decision(decisions, "asset-yes").subscription_eligible is False
    assert _decision(decisions, "asset-yes").execution_eligible is False


def test_case_7_gamma_polling_corrects_missing_ws_resolved_signal() -> None:
    harness = _Harness(MarketUniverseConfig(), {})
    harness.full_sync([_token("asset-yes"), _token("asset-no")])
    harness.mark_book_ready("asset-yes")

    decisions = harness.full_sync([_token("asset-yes", active=False, closed=True), _token("asset-no", active=False, closed=True)])

    assert _decision(decisions, "asset-yes").market_state == "CLOSING"
    assert _decision(decisions, "asset-yes").market_state != "LIVE"


def test_case_8_daemon_restart_recovers_state_republishes_and_increments_generation() -> None:
    harness = _Harness(MarketUniverseConfig(), {})
    harness.full_sync([_token("asset-yes"), _token("asset-no")])
    initial_generation = harness.previous.generation if harness.previous else 0

    restarted = harness.restart()
    decisions = restarted.recompute()
    restarted.republish_current_subscriptions()

    assert restarted.previous is not None
    assert restarted.previous.generation == initial_generation + 1
    assert len(restarted.tokens) == len(harness.tokens)
    assert {decision.asset_id for decision in decisions if decision.subscription_eligible} == {"asset-yes", "asset-no"}
    assert ("ASSET_SUBSCRIBE_REQUESTED", "asset-yes") in restarted.outbox


def test_daemon_run_enables_embedded_ws_lifecycle_by_default() -> None:
    args = build_parser().parse_args(["run", "--run-seconds", "1", "--no-api"])

    assert args.no_ws is False
    assert args.ws_url == "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    assert args.ws_batch_size == 500
    assert args.ws_ping_timeout == 0
    assert args.ws_application_ping_interval == 10
    assert args.ws_health_grace_seconds == 300
    assert args.ws_mark_assets_disconnected_on_drop is False
    assert args.skip_startup_probe is False
    assert args.no_embedded_delta_poll is False
    assert args.no_embedded_heartbeat is False
    assert args.heartbeat_seconds == 30
    assert args.periodic_full_with_api is False

    disabled = build_parser().parse_args(["run", "--run-seconds", "1", "--no-api", "--no-ws", "--skip-startup-probe"])

    assert disabled.no_ws is True
    assert disabled.skip_startup_probe is True

    serial_delta = build_parser().parse_args(["run", "--no-api", "--no-embedded-delta-poll"])
    assert serial_delta.no_embedded_delta_poll is True

    periodic_api = build_parser().parse_args(["run", "--periodic-full-with-api"])
    assert periodic_api.periodic_full_with_api is True


def test_embedded_delta_worker_uses_configured_poll_interval() -> None:
    args = build_parser().parse_args(["run", "--no-api", "--poll-seconds", "17"])
    worker = _EmbeddedDeltaPollWorker(args, MarketUniverseConfig())

    assert worker.args.poll_seconds == 17
    assert worker.completed_cycles == 0
    assert worker.last_error is None


def test_daemon_probe_cycle_limit_preserves_fairness_without_truncating_cli_probe() -> None:
    assert _bounded_cycle_limit(1000, 100) == 100
    assert _bounded_cycle_limit(50, 100) == 50
    assert _bounded_cycle_limit(0, 100) == 100
    assert _bounded_cycle_limit(1000, 0) == 1000


def test_embedded_heartbeat_worker_uses_independent_interval() -> None:
    args = build_parser().parse_args(["run", "--no-api", "--heartbeat-seconds", "19"])
    worker = _EmbeddedRegistryHeartbeatWorker(args, None)

    assert worker.args.heartbeat_seconds == 19
    assert worker.completed_cycles == 0
    assert worker.last_error is None


def test_delta_poll_reconciles_active_candidates_and_desired_tokens_only() -> None:
    active = _token("asset-new")
    desired = _token("asset-old", active=False, closed=True, completion_status="CLOSED")

    class FakeConnection:
        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    class FakeExisting:
        def __init__(self) -> None:
            self.desired_ids: list[str] | None = None

        def fetch_tokens(self, **kwargs: object) -> list[MarketRegistryToken]:
            assert kwargs["candidates_only"] is True
            assert kwargs["include_book"] is False
            return [active]

        def fetch_tokens_by_asset_ids(self, asset_ids: list[str], **kwargs: object) -> list[MarketRegistryToken]:
            self.desired_ids = asset_ids
            assert kwargs["include_book"] is False
            return [desired]

    class FakeRepository:
        def __init__(self) -> None:
            self.state_ids: set[str] = set()
            self.decisions: list[UniverseDecision] = []
            self.cycle_locked = False

        def list_desired_subscription_asset_ids(self, *, limit: int | None) -> list[str]:
            # Potentially large discovery reads must not hold the registry
            # writer lock and starve the independent subscription outbox.
            assert self.cycle_locked is False
            assert limit is None
            return ["asset-old"]

        def fetch_state_tokens(self, asset_ids: list[str]) -> list[MarketRegistryToken]:
            assert self.cycle_locked is False
            self.state_ids = set(asset_ids)
            return []

        def begin_sync_run(self, sync_type: str, *, meta: object) -> int:
            assert sync_type == "delta_poll"
            self.cycle_locked = True
            return 123

        def persist_decisions(self, decisions: list[UniverseDecision], **kwargs: object) -> RegistryPersistSummary:
            assert self.cycle_locked is True
            assert kwargs["aggregate_changed_only"] is True
            self.decisions = decisions
            return RegistryPersistSummary(tokens_seen=len(decisions))

        def finish_sync_run(self, *args: object, **kwargs: object) -> None:
            return None

    service = MarketRegistryService.__new__(MarketRegistryService)
    service.conn = FakeConnection()
    service.config = MarketUniverseConfig()
    service.existing = FakeExisting()
    service.repo = FakeRepository()
    service.api_client = None
    service.api_errors = []

    summary = service.delta_poll(include_api=False)

    assert service.existing.desired_ids == ["asset-old"]
    assert service.repo.state_ids == {"asset-new", "asset-old"}
    assert {decision.asset_id for decision in service.repo.decisions} == {"asset-new", "asset-old"}
    assert summary.tokens_seen == 2


def test_delta_poll_uses_persisted_source_window_for_bounded_changes() -> None:
    changed = _token("asset-changed")
    cursor = NOW - timedelta(minutes=2)

    class FakeConnection:
        def rollback(self) -> None:
            return None

    class FakeExisting:
        def source_watermark(self) -> datetime:
            return NOW

        def list_changed_asset_ids(self, *, since: datetime, until: datetime) -> list[str]:
            assert since == cursor - timedelta(minutes=5)
            assert until == NOW
            return ["asset-changed"]

        def fetch_tokens_by_asset_ids(self, asset_ids: list[str], **kwargs: object) -> list[MarketRegistryToken]:
            assert asset_ids == ["asset-changed"]
            assert kwargs["include_book"] is False
            return [changed]

    class FakeRepository:
        def __init__(self) -> None:
            self.decisions: list[UniverseDecision] = []
            self.cursor_state: dict[str, object] = {}

        def begin_sync_run(self, sync_type: str, *, meta: object) -> int:
            assert sync_type == "delta_poll"
            return 124

        def get_state_json(self, key: str) -> dict[str, object]:
            assert key == "db_market_delta_cursor"
            return {"source_until": cursor.isoformat()}

        def set_state_json(self, key: str, value: dict[str, object]) -> None:
            assert key == "db_market_delta_cursor"
            self.cursor_state = value

        def latest_successful_sync_at(self, sync_type: str) -> datetime | None:
            raise AssertionError("persisted cursor should win")

        def fetch_state_tokens(self, asset_ids: list[str]) -> list[MarketRegistryToken]:
            assert asset_ids == ["asset-changed"]
            return []

        def persist_partial_decisions(
            self,
            decisions: list[UniverseDecision],
            **kwargs: object,
        ) -> RegistryPersistSummary:
            assert kwargs["aggregate_changed_only"] is True
            self.decisions = decisions
            return RegistryPersistSummary(tokens_seen=len(decisions))

        def finish_sync_run(self, *args: object, **kwargs: object) -> None:
            assert kwargs["meta"]["incremental"] is True

    service = MarketRegistryService.__new__(MarketRegistryService)
    service.conn = FakeConnection()
    service.config = MarketUniverseConfig()
    service.existing = FakeExisting()
    service.repo = FakeRepository()
    service.api_client = None
    service.api_errors = []

    summary = service.delta_poll(include_api=False)

    assert [decision.asset_id for decision in service.repo.decisions] == ["asset-changed"]
    assert service.repo.cursor_state["source_until"] == NOW.isoformat()
    assert summary.db_tokens_seen == 1


def test_book_probe_persists_only_the_probed_batch() -> None:
    token = _token("asset-probed")
    probe = BookProbeResult(
        asset_id=token.asset_id,
        ok=True,
        book_status="ready",
        book_quality="READY_HIGH",
        best_bid=Decimal("0.40"),
        best_ask=Decimal("0.41"),
        observed_at=NOW,
    )

    class FakeConnection:
        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    class FakeApi:
        def probe_books(self, asset_ids: list[str], *, batch_size: int) -> dict[str, BookProbeResult]:
            assert asset_ids == [token.asset_id]
            assert batch_size == 50
            return {token.asset_id: probe}

    class FakeRepository:
        def __init__(self) -> None:
            self.persisted: list[UniverseDecision] = []
            self.updated: list[str] = []

        def list_probe_candidates(self, *, limit: int | None) -> list[str]:
            assert limit == 1
            return [token.asset_id]

        def fetch_state_tokens(self, asset_ids: list[str]) -> list[MarketRegistryToken]:
            assert asset_ids == [token.asset_id]
            return [token]

        def begin_sync_run(self, sync_type: str, *, meta: object) -> int:
            assert sync_type == "book_probe"
            return 125

        def update_probe_result(self, decision: UniverseDecision, result: BookProbeResult, *, run_id: int) -> None:
            assert result is probe
            assert run_id == 125
            self.updated.append(decision.asset_id)

        def persist_partial_decisions(
            self,
            decisions: list[UniverseDecision],
            **kwargs: object,
        ) -> RegistryPersistSummary:
            assert kwargs["source"] == "book_probe"
            self.persisted = decisions
            return RegistryPersistSummary(tokens_seen=len(decisions))

        def finish_sync_run(self, *args: object, **kwargs: object) -> None:
            return None

    service = MarketRegistryService.__new__(MarketRegistryService)
    service.conn = FakeConnection()
    service.config = MarketUniverseConfig()
    service.repo = FakeRepository()
    service.api_client = FakeApi()
    service.api_errors = []

    summary = service.probe_pending_books(limit=1, batch_size=50)

    assert service.repo.updated == [token.asset_id]
    assert [decision.asset_id for decision in service.repo.persisted] == [token.asset_id]
    assert summary.tokens_seen == 1


def test_force_recheck_persists_only_requested_assets() -> None:
    token = _token("asset-force-rechecked")
    probe = BookProbeResult(
        asset_id=token.asset_id,
        ok=True,
        book_status="ready",
        book_quality="READY_HIGH",
        best_bid=Decimal("0.40"),
        best_ask=Decimal("0.41"),
        observed_at=NOW,
    )

    class FakeApi:
        def probe_books(self, asset_ids: list[str], *, batch_size: int) -> dict[str, BookProbeResult]:
            assert asset_ids == [token.asset_id]
            assert batch_size == 500
            return {token.asset_id: probe}

    class FakeRepository:
        def __init__(self) -> None:
            self.persisted: list[UniverseDecision] = []

        def begin_sync_run(self, sync_type: str, *, meta: object) -> int:
            assert sync_type == "force_recheck"
            return 126

        def fetch_state_tokens(self, asset_ids: list[str]) -> list[MarketRegistryToken]:
            assert asset_ids == [token.asset_id]
            return [token]

        def fetch_all_state_tokens(self, **kwargs: object) -> list[MarketRegistryToken]:
            raise AssertionError("targeted recheck must not scan the full registry")

        def update_probe_result(
            self,
            decision: UniverseDecision,
            result: BookProbeResult,
            *,
            run_id: int,
        ) -> None:
            assert decision.asset_id == token.asset_id
            assert result is probe
            assert run_id == 126

        def persist_partial_decisions(
            self,
            decisions: list[UniverseDecision],
            **kwargs: object,
        ) -> RegistryPersistSummary:
            assert kwargs == {
                "run_id": 126,
                "source": "force_recheck",
                "repair_global_targets": False,
                "include_universe_counts": False,
            }
            self.persisted = decisions
            return RegistryPersistSummary(tokens_seen=len(decisions))

        def finish_sync_run(self, *args: object, **kwargs: object) -> None:
            return None

    service = MarketRegistryService.__new__(MarketRegistryService)
    service.config = MarketUniverseConfig()
    service.repo = FakeRepository()
    service.api_client = FakeApi()

    payload = _force_recheck_assets(service, [token.asset_id])

    assert payload["assets"] == 1
    assert payload["probes_ok"] == 1
    assert [decision.asset_id for decision in service.repo.persisted] == [token.asset_id]


def test_full_sync_reconciles_current_universe_without_reprocessing_terminal_history() -> None:
    active = _token("asset-live")
    closing = _token("asset-closing", active=False, closed=True, completion_status="CLOSED")

    class FakeConnection:
        def rollback(self) -> None:
            return None

    class FakeExisting:
        def fetch_tokens(self, **kwargs: object) -> list[MarketRegistryToken]:
            assert kwargs["candidates_only"] is True
            assert kwargs["include_book"] is False
            return [active]

        def fetch_tokens_by_asset_ids(self, asset_ids: list[str], **kwargs: object) -> list[MarketRegistryToken]:
            assert asset_ids == ["asset-closing"]
            assert kwargs["include_book"] is False
            return [closing]

    class FakeRepository:
        def __init__(self) -> None:
            self.decisions: list[UniverseDecision] = []
            self.locked = False

        def list_reconciliation_asset_ids(self, *, limit: int | None) -> list[str]:
            assert limit is None
            assert self.locked is False
            return ["asset-closing"]

        def fetch_state_tokens(self, asset_ids: list[str]) -> list[MarketRegistryToken]:
            assert asset_ids == ["asset-closing"]
            assert self.locked is False
            return []

        def begin_sync_run(self, sync_type: str, *, meta: dict[str, object]) -> int:
            assert sync_type == "full_sync"
            assert meta["reconciliation_asset_count"] == 1
            self.locked = True
            return 321

        def persist_decisions(self, decisions: list[UniverseDecision], **kwargs: object) -> RegistryPersistSummary:
            assert self.locked is True
            assert kwargs["aggregate_changed_only"] is True
            self.decisions = decisions
            return RegistryPersistSummary(tokens_seen=len(decisions))

        def finish_sync_run(self, *args: object, **kwargs: object) -> None:
            return None

    service = MarketRegistryService.__new__(MarketRegistryService)
    service.conn = FakeConnection()
    service.config = MarketUniverseConfig()
    service.existing = FakeExisting()
    service.repo = FakeRepository()
    service.api_client = None
    service.api_errors = []

    summary = service.full_sync(include_api=False)

    assert {decision.asset_id for decision in service.repo.decisions} == {"asset-live", "asset-closing"}
    assert summary.tokens_seen == 2


def test_full_sync_ignores_active_api_rows_for_terminal_history_assets() -> None:
    terminal = _token("asset-terminal")
    fresh = _token("asset-fresh")

    class FakeConnection:
        def rollback(self) -> None:
            return None

    class FakeExisting:
        def fetch_tokens(self, **kwargs: object) -> list[MarketRegistryToken]:
            assert kwargs["include_book"] is False
            return []

        def fetch_tokens_by_asset_ids(self, asset_ids: list[str], **kwargs: object) -> list[MarketRegistryToken]:
            assert asset_ids == []
            assert kwargs["include_book"] is False
            return []

    class FakeRepository:
        def __init__(self) -> None:
            self.decisions: list[UniverseDecision] = []

        def terminal_asset_ids(self, asset_ids: list[str]) -> set[str]:
            assert set(asset_ids) == {"asset-terminal", "asset-fresh"}
            return {"asset-terminal"}

        def list_reconciliation_asset_ids(self, *, limit: int | None) -> list[str]:
            return []

        def fetch_state_tokens(self, asset_ids: list[str]) -> list[MarketRegistryToken]:
            return []

        def begin_sync_run(self, sync_type: str, *, meta: dict[str, object]) -> int:
            assert meta["terminal_api_tokens_ignored"] == 1
            return 654

        def persist_decisions(self, decisions: list[UniverseDecision], **kwargs: object) -> RegistryPersistSummary:
            self.decisions = decisions
            return RegistryPersistSummary(tokens_seen=len(decisions))

        def finish_sync_run(self, *args: object, **kwargs: object) -> None:
            return None

    class TestService(MarketRegistryService):
        def _fetch_api_tokens(self, **kwargs: object) -> tuple[list[MarketRegistryToken], bool]:
            return [terminal, fresh], True

    service = TestService.__new__(TestService)
    service.conn = FakeConnection()
    service.config = MarketUniverseConfig()
    service.existing = FakeExisting()
    service.repo = FakeRepository()
    service.api_client = object()
    service.api_errors = []

    summary = service.full_sync(include_api=True)

    assert [decision.asset_id for decision in service.repo.decisions] == ["asset-fresh"]
    assert summary.tokens_seen == 1


def test_delta_aggregate_filter_only_selects_changed_markets() -> None:
    unchanged = UniverseDecision(
        token=_token("asset-unchanged"),
        subscription_eligible=True,
        execution_eligible=False,
        market_state="TRADABLE_PENDING_BOOK",
        subscription_reason="metadata_ready",
        execution_reason="book_missing",
        book_quality="MISSING",
    )
    changed = replace(
        unchanged,
        token=_token("asset-changed", condition_id="0xchanged"),
        market_state="LIVE",
        execution_eligible=True,
        execution_reason="book_ready",
        book_quality="READY_HIGH",
    )
    previous = {
        "asset-unchanged": {
            "market_id": 100,
            "gamma_market_id": "gamma-100",
            "condition_id": "0xcond",
            "market_slug": "demo-market",
            "market_title": "Demo market",
            "outcome_name": "NO",
            "outcome_index": 1,
            "market_state": "TRADABLE_PENDING_BOOK",
            "subscription_eligible": True,
            "execution_eligible": False,
        },
        "asset-changed": {
            "market_id": 100,
            "gamma_market_id": "gamma-100",
            "condition_id": "0xchanged",
            "market_slug": "demo-market",
            "market_title": "Demo market",
            "outcome_name": "NO",
            "outcome_index": 1,
            "market_state": "STALE",
            "subscription_eligible": True,
            "execution_eligible": False,
        },
    }

    assert _changed_market_keys([unchanged, changed], previous) == {"0xchanged"}


def test_clob_explicit_open_status_beats_core_row_without_status_snapshot() -> None:
    core_without_status = _token(
        "asset-yes",
        status_present=False,
        completion_status=None,
        source="core.market_tokens",
    )
    clob_open = _token(
        "asset-yes",
        status_present=True,
        completion_status="OPEN",
        source="clob_markets_api",
    )

    merged = _merge_tokens([core_without_status], [clob_open])

    assert len(merged) == 1
    assert merged[0].source == "clob_markets_api"
    assert merged[0].status_present is True


def test_gamma_api_corrects_stale_core_asset_condition_mapping() -> None:
    core = _token(
        "asset-yes",
        market_id=42,
        condition_id="0xstale",
        source="core.market_status_snapshot",
    )
    gamma = _token(
        "asset-yes",
        market_id=0,
        condition_id="0xcurrent",
        gamma_market_id="gamma-current",
        source="gamma_api_open_book",
    )

    merged = _merge_tokens([gamma], [core])

    assert len(merged) == 1
    assert merged[0].condition_id == "0xcurrent"
    assert merged[0].gamma_market_id == "gamma-current"


def test_lob_projection_status_truth_beats_core_row_without_status_snapshot() -> None:
    core_without_status = _token(
        "asset-yes",
        status_present=False,
        completion_status=None,
        source="core.market_tokens",
    )
    projected = _token(
        "asset-yes",
        status_present=True,
        completion_status="OPEN",
        latest_book_at=NOW,
        book_status="ok",
        best_bid=Decimal("0.49"),
        best_ask=Decimal("0.51"),
        source="lob_projection_refresh",
    )

    merged = _merge_tokens([projected], [core_without_status])

    assert len(merged) == 1
    assert merged[0].source == "lob_projection_refresh"
    assert merged[0].status_present is True
    assert merged[0].latest_book_at == NOW


def test_core_closed_status_still_beats_clob_open_status() -> None:
    core_closed = _token(
        "asset-yes",
        closed=True,
        status_present=True,
        completion_status="CLOSED",
        source="core.market_status_snapshot",
    )
    clob_open = _token(
        "asset-yes",
        status_present=True,
        completion_status="OPEN",
        source="clob_markets_api",
    )

    merged = _merge_tokens([clob_open], [core_closed])

    assert len(merged) == 1
    assert merged[0].closed is True
    assert merged[0].source == "core.market_status_snapshot"


def test_outbox_event_must_match_current_registry_state() -> None:
    assert _outbox_matches_current_state({
        "event_type": "ASSET_SUBSCRIBE_REQUESTED",
        "current_desired_subscribed": True,
    }) is True
    assert _outbox_matches_current_state({
        "event_type": "ASSET_SUBSCRIBE_REQUESTED",
        "current_desired_subscribed": False,
    }) is False
    assert _outbox_matches_current_state({
        "event_type": "ASSET_UNSUBSCRIBE_REQUESTED",
        "current_desired_subscribed": False,
    }) is True
    assert _outbox_matches_current_state({
        "event_type": "ASSET_EXECUTION_ENABLED",
        "current_execution_eligible": False,
    }) is False


def test_deadlock_detection_accepts_driver_class_or_message() -> None:
    class DeadlockDetected(Exception):
        pass

    assert _is_deadlock(DeadlockDetected("database conflict"))
    assert _is_deadlock(RuntimeError("deadlock detected while inserting index tuple"))
    assert not _is_deadlock(RuntimeError("network timeout"))


def test_ws_subscription_batches_support_dynamic_subscribe_and_unsubscribe() -> None:
    fake = _FakeWebSocket()

    asyncio.run(
        _send_ws_subscription_batches(
            fake,
            {"a", "b", "c"},
            batch_size=2,
            subscribe=True,
            initial=True,
        )
    )
    asyncio.run(_send_ws_subscription_batches(fake, {"d"}, batch_size=2, subscribe=True))
    asyncio.run(_send_ws_subscription_batches(fake, {"b"}, batch_size=2, subscribe=False))

    assert fake.sent[0] == {"type": "market", "assets_ids": ["a", "b"], "custom_feature_enabled": True}
    assert fake.sent[1] == {
        "type": "market",
        "assets_ids": ["c"],
        "custom_feature_enabled": True,
        "operation": "subscribe",
    }
    assert fake.sent[2] == {
        "type": "market",
        "assets_ids": ["d"],
        "custom_feature_enabled": True,
        "operation": "subscribe",
    }
    assert fake.sent[3] == {"type": "market", "assets_ids": ["b"], "operation": "unsubscribe"}


def test_ws_proxy_respects_registry_proxy_mode() -> None:
    explicit = build_parser().parse_args(
        ["ws-lifecycle", "--proxy-mode", "explicit", "--proxy-url", "http://127.0.0.1:45203"]
    )
    direct = build_parser().parse_args(["ws-lifecycle", "--proxy-mode", "direct"])
    env = build_parser().parse_args(["ws-lifecycle", "--proxy-mode", "env"])

    assert _ws_proxy_from_args(explicit) == "http://127.0.0.1:45203"
    assert _ws_proxy_from_args(direct) is None
    assert _ws_proxy_from_args(env) is True


def test_ws_proxy_candidates_rotate_from_profile_12_to_profile_8() -> None:
    args = build_parser().parse_args(
        [
            "ws-lifecycle",
            "--proxy-mode",
            "explicit",
            "--proxy-url",
            "http://127.0.0.1:17890",
            "--fallback-proxy-urls",
            "http://127.0.0.1:18080,http://127.0.0.1:17890",
        ]
    )

    assert _ws_proxy_candidates(args) == [
        "http://127.0.0.1:17890",
        "http://127.0.0.1:18080",
    ]


def test_ws_ping_timeout_zero_disables_timeout() -> None:
    assert _positive_float_or_none(0) is None
    assert _positive_float_or_none("0") is None
    assert _positive_float_or_none(12.5) == 12.5


def test_embedded_ws_worker_health_uses_recent_heartbeat_grace() -> None:
    args = build_parser().parse_args(["run", "--run-seconds", "1", "--no-api", "--ws-health-grace-seconds", "300"])
    worker = _EmbeddedWsLifecycleWorker(args, MarketUniverseConfig())

    assert worker.is_connected() is False
    worker._set_connected(True)
    assert worker.is_connected() is True
    worker._set_connected(False)
    assert worker.is_connected() is True
    with worker.lock:
        worker.last_heartbeat_monotonic = time.monotonic() - 301
    assert worker.is_connected() is False


def test_reconnect_delay_backs_off_with_jitter_cap() -> None:
    delay = _reconnect_delay(base_seconds=5, max_seconds=60, attempt=3)

    assert 20 <= delay <= 21


def test_soak_monitor_ws_health_allows_recent_connected_heartbeat() -> None:
    now = datetime(2026, 7, 8, 8, 0, tzinfo=timezone.utc)

    assert _ws_healthy(
        latest_health={"ws_connected": False},
        latest_ws_connected_at=now - timedelta(seconds=120),
        now=now,
        grace_seconds=300,
    )
    assert not _ws_healthy(
        latest_health={"ws_connected": False},
        latest_ws_connected_at=now - timedelta(seconds=600),
        now=now,
        grace_seconds=300,
    )


def test_soak_monitor_pending_outbox_has_startup_grace() -> None:
    now = datetime(2026, 7, 8, 8, 0, tzinfo=timezone.utc)

    assert not _pending_outbox_stale(
        pending_count=100,
        oldest_pending_at=now - timedelta(seconds=120),
        now=now,
        grace_seconds=300,
    )
    assert _pending_outbox_stale(
        pending_count=100,
        oldest_pending_at=now - timedelta(seconds=600),
        now=now,
        grace_seconds=300,
    )


def test_ws_decode_ignores_non_json_control_payloads() -> None:
    assert _decode_ws_payload("PONG") == []
    assert _decode_ws_payload("connected") == []


def test_market_resolved_requires_resolution_truth_before_direct_resolve() -> None:
    assert _has_resolution_truth({"type": "market_resolved", "market": "0xcond"}) is False
    assert _has_resolution_truth({"type": "market_resolved", "market": "0xcond", "winningOutcome": "YES"}) is True


def test_market_resolved_recomputes_only_affected_condition_tokens() -> None:
    token = _token("asset-yes")

    class FakeConnection:
        def rollback(self) -> None:
            return None

    class FakeRepository:
        def begin_sync_run(self, sync_type: str, *, meta: object) -> int:
            assert sync_type == "ws_lifecycle"
            return 501

        def mark_condition_resolved(self, condition_id: str, **kwargs: object) -> list[str]:
            assert condition_id == "0xcond"
            return [token.asset_id]

        def fetch_state_tokens(self, asset_ids: list[str]) -> list[MarketRegistryToken]:
            assert asset_ids == [token.asset_id]
            return [replace(token, resolved=True, closed=True, active=False)]

        def fetch_all_state_tokens(self, **_kwargs: object) -> list[MarketRegistryToken]:
            raise AssertionError("one resolution must not scan the complete registry")

        def persist_partial_decisions(
            self,
            decisions: list[UniverseDecision],
            **kwargs: object,
        ) -> RegistryPersistSummary:
            assert [decision.asset_id for decision in decisions] == [token.asset_id]
            assert kwargs == {
                "run_id": 501,
                "source": "ws_lifecycle",
                "repair_global_targets": False,
                "include_universe_counts": False,
            }
            return RegistryPersistSummary(tokens_seen=1)

        def finish_sync_run(self, *args: object, **kwargs: object) -> None:
            assert kwargs["status"] == "success"

    service = MarketRegistryService.__new__(MarketRegistryService)
    service.conn = FakeConnection()
    service.config = MarketUniverseConfig()
    service.repo = FakeRepository()

    result = service.handle_lifecycle_event(
        {"type": "market_resolved", "market": "0xcond", "winningOutcome": "YES"}
    )

    assert result["affected_asset_ids"] == [token.asset_id]


def test_lifecycle_database_failure_does_not_disconnect_ws_feed() -> None:
    class FakeConnection:
        def __init__(self) -> None:
            self.rollbacks = 0

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            self.rollbacks += 1

    class FakeService:
        def __init__(self) -> None:
            self.conn = FakeConnection()

        def handle_lifecycle_event(self, _event: object) -> dict[str, object]:
            raise RuntimeError("temporary file size exceeds temp_file_limit")

    class FakeRepository:
        def __init__(self, conn: FakeConnection) -> None:
            self.conn = conn
            self.health: list[dict[str, object]] = []

        def record_health_snapshot(self, **kwargs: object) -> None:
            self.health.append(dict(kwargs))

    service = FakeService()
    repo = FakeRepository(service.conn)

    result = _process_ws_lifecycle_event(
        service,  # type: ignore[arg-type]
        repo,  # type: ignore[arg-type]
        {"type": "market_resolved"},
        event_type="market_resolved",
    )

    assert result["event_type"] == "LIFECYCLE_EVENT_PROCESSING_FAILED"
    assert result["ws_connection_preserved"] is True
    assert service.conn.rollbacks == 1
    assert repo.health[0]["ws_connected"] is True


def test_lifecycle_events_use_spec_named_event_types() -> None:
    pending = compute_universe_decisions([_token("asset-yes")], now=NOW)[0]
    assert _lifecycle_event_types(None, pending, state_changed=True, execution_changed=False) == [
        "MARKET_DISCOVERED",
        "TOKEN_ADDED",
    ]

    previous = {"market_state": "TRADABLE_PENDING_BOOK", "execution_eligible": False}
    ready = compute_universe_decisions(
        [
            _token(
                "asset-yes",
                latest_book_at=NOW,
                book_status="ok",
                best_bid=Decimal("0.49"),
                best_ask=Decimal("0.51"),
            )
        ],
        now=NOW,
    )[0]
    closed = compute_universe_decisions([_token("asset-yes", closed=True)], now=NOW)[0]
    resolved = compute_universe_decisions([_token("asset-yes", resolved=True)], now=NOW)[0]

    assert _lifecycle_event_types(previous, ready, state_changed=True, execution_changed=True) == ["BOOK_READY"]
    assert _lifecycle_event_types(previous, closed, state_changed=True, execution_changed=False) == ["MARKET_CLOSED"]
    assert _lifecycle_event_types(previous, resolved, state_changed=True, execution_changed=False) == ["MARKET_RESOLVED"]


def test_market_aggregate_key_prefers_condition_id_over_gamma_id() -> None:
    token = _token("asset-yes", gamma_market_id="gamma-100", condition_id="0xcond")

    assert _market_key(token) == "0xcond"


def test_resolution_truth_backfill_counts_as_metadata_change() -> None:
    resolved_at = NOW + timedelta(minutes=1)
    decision = compute_universe_decisions(
        [
            _token(
                "asset-yes",
                closed=True,
                resolved=True,
                completion_status="RESOLVED",
                winning_asset_id="asset-yes",
                winning_outcome="YES",
                resolution_status="RESOLVED",
                resolution_source="gamma_api",
                resolved_time=resolved_at,
            )
        ],
        now=NOW,
    )[0]
    previous = {
        "market_id": 100,
        "gamma_market_id": "gamma-100",
        "condition_id": "0xcond",
        "market_slug": "demo-market",
        "market_title": "Demo market",
        "outcome_name": "YES",
        "outcome_index": 0,
        "active": True,
        "closed": True,
        "resolved": True,
        "archived": False,
        "deprecated": False,
        "status_present": True,
        "completion_status": "RESOLVED",
        "token_count": 2,
        "winning_asset_id": "asset-yes",
        "winning_outcome": "YES",
        "resolution_status": "RESOLVED",
        "resolution_source": "gamma_api",
        "resolved_time": None,
    }

    assert _decision_market_identity_changed(previous, decision) is True
    previous["resolved_time"] = resolved_at
    assert _decision_market_identity_changed(previous, decision) is False


def test_tradable_metadata_regression_is_guarded_as_subscribed_stale() -> None:
    decision = UniverseDecision(
        token=_token("asset-yes", status_present=False, market_slug="trade-indexer-placeholder-cond"),
        subscription_eligible=False,
        execution_eligible=False,
        market_state="DISCOVERED",
        subscription_reason="status_missing,placeholder_market",
        execution_reason="status_missing,placeholder_market",
        book_quality="NOT_CHECKED",
    )

    for old_state in ("LIVE", "STALE", "TRADABLE_PENDING_BOOK"):
        guarded = _guard_illegal_state_regression(
            {"market_state": old_state, "execution_eligible": old_state == "LIVE"},
            decision,
        )

        assert guarded.market_state == "STALE"
        assert guarded.subscription_eligible is True
        assert guarded.execution_eligible is False
        assert guarded.execution_reason == "metadata_regressed:status_missing,placeholder_market"


def test_closing_market_cannot_regress_without_explicit_reopen_truth() -> None:
    previous = {"market_state": "CLOSING", "execution_eligible": False}
    decision = compute_universe_decisions([_token("asset-yes")], now=NOW)[0]

    guarded = _guard_illegal_state_regression(previous, decision)

    assert guarded.market_state == "CLOSING"
    assert guarded.subscription_eligible is False
    assert guarded.execution_eligible is False
    assert guarded.execution_reason.startswith("closing_state_guard:")

    reopened = replace(
        decision,
        subscription_reason="explicit_reopen:oracle_correction",
        execution_reason="explicit_reopen:oracle_correction",
    )
    assert _guard_illegal_state_regression(previous, reopened).market_state != "CLOSING"


def test_closing_market_reopens_from_explicit_clob_open_truth() -> None:
    previous = {"market_state": "CLOSING", "execution_eligible": False}
    token = replace(_token("asset-yes"), source="clob_markets_api")
    decision = compute_universe_decisions([token], now=NOW)[0]

    reopened = _guard_illegal_state_regression(previous, decision)

    assert reopened.market_state != "CLOSING"
    assert reopened.subscription_eligible is True
    assert reopened.execution_reason.startswith("explicit_reopen:clob_markets_api:")


def test_closing_market_reopens_from_gamma_accepting_book_truth() -> None:
    previous = {"market_state": "CLOSING", "execution_eligible": False}
    token = replace(_token("asset-yes"), source="gamma_api_open_book")
    decision = compute_universe_decisions([token], now=NOW)[0]

    reopened = _guard_illegal_state_regression(previous, decision)

    assert reopened.market_state != "CLOSING"
    assert reopened.subscription_eligible is True
    assert reopened.execution_reason.startswith("explicit_reopen:gamma_api_open_book:")


def test_gamma_active_without_accepting_book_does_not_reopen_closing_market() -> None:
    previous = {"market_state": "CLOSING", "execution_eligible": False}
    token = replace(_token("asset-yes"), source="gamma_api")
    decision = compute_universe_decisions([token], now=NOW)[0]

    guarded = _guard_illegal_state_regression(previous, decision)

    assert guarded.market_state == "CLOSING"
    assert guarded.subscription_eligible is False


def test_closing_market_does_not_reopen_from_untrusted_cached_state() -> None:
    previous = {"market_state": "CLOSING", "execution_eligible": False}
    token = replace(_token("asset-yes"), source="clob_markets_refresh")
    decision = compute_universe_decisions([token], now=NOW)[0]

    guarded = _guard_illegal_state_regression(previous, decision)

    assert guarded.market_state == "CLOSING"
    assert guarded.subscription_eligible is False


def test_explicit_clob_open_truth_does_not_reopen_terminal_market() -> None:
    token = replace(_token("asset-yes"), source="clob_markets_api")
    decision = compute_universe_decisions([token], now=NOW)[0]

    for terminal_state in ("RESOLVED", "ARCHIVED"):
        guarded = _guard_illegal_state_regression(
            {"market_state": terminal_state, "execution_eligible": False},
            decision,
        )
        assert guarded.market_state == terminal_state
        assert guarded.subscription_eligible is False


def test_clob_book_probe_can_probe_pending_assets_from_loader() -> None:
    client = _FakeBookClient()
    probe = ClobBookProbe(client, pending_asset_loader=lambda limit: ["asset-a", "asset-b"][:limit])

    results = asyncio.run(probe.probe_pending_assets(limit=1))

    assert client.probed_asset_ids == ["asset-a"]
    assert [result.asset_id for result in results] == ["asset-a"]
    assert results[0].ok is True


class _FakeWebSocket:
    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []

    async def send(self, payload: str) -> None:
        self.sent.append(json.loads(payload))


class _FakeBookProbe:
    asset_id = "asset-a"
    ok = True
    book_quality = "READY_MEDIUM"
    book_status = "ok"
    observed_at = NOW
    payload = {"market": "0xcond", "hash": "hash", "min_order_size": "1", "tick_size": "0.01"}
    best_bid = Decimal("0.49")
    best_ask = Decimal("0.51")
    error = None


class _FakeBookClient:
    def __init__(self) -> None:
        self.probed_asset_ids: list[str] = []

    def probe_books(self, asset_ids: object, *, batch_size: int = 50) -> dict[str, _FakeBookProbe]:
        self.probed_asset_ids = [str(asset_id) for asset_id in asset_ids]
        return {"asset-a": _FakeBookProbe()}
