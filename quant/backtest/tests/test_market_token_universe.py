from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from quant.market.token_universe import (
    MarketRegistryToken,
    MarketUniverseConfig,
    build_token_universe_diff,
    classify_market_token,
    compute_universe_decisions,
)
from quant.market.service import (
    _append_absent_registry_removals,
    _merge_tokens,
    _reconcile_authoritative_active_snapshot,
)


NOW = datetime(2026, 7, 6, 8, 0, tzinfo=timezone.utc)


def _token(**overrides: object) -> MarketRegistryToken:
    base = {
        "asset_id": "asset-yes",
        "market_id": 123,
        "condition_id": "0xcondition",
        "gamma_market_id": "2801173",
        "market_slug": "demo-market",
        "market_title": "Demo market",
        "outcome_name": "YES",
        "outcome_index": 0,
        "active": True,
        "closed": False,
        "resolved": False,
        "status_present": True,
        "completion_status": "OPEN",
        "token_count": 2,
    }
    base.update(overrides)
    return MarketRegistryToken(**base)


def test_pending_book_is_subscribed_but_not_executable() -> None:
    decision = classify_market_token(_token(), now=NOW)

    assert decision.subscription_eligible is True
    assert decision.execution_eligible is False
    assert decision.market_state == "TRADABLE_PENDING_BOOK"
    assert decision.subscription_reason == "metadata_ready"
    assert decision.execution_reason == "no_book_snapshot"


def test_clob_only_condition_id_is_enough_for_subscription_probe() -> None:
    decision = classify_market_token(
        _token(
            market_id=0,
            gamma_market_id=None,
            condition_id="0xclobonly",
            source="clob_markets_api",
        ),
        now=NOW,
    )

    assert decision.subscription_eligible is True
    assert decision.execution_eligible is False
    assert decision.market_state == "TRADABLE_PENDING_BOOK"
    assert decision.subscription_reason == "metadata_ready"


def test_execution_requires_fresh_ready_two_sided_book() -> None:
    decision = classify_market_token(
        _token(
            latest_book_at=NOW - timedelta(seconds=5),
            book_status="ok",
            best_bid=Decimal("0.41"),
            best_ask=Decimal("0.43"),
        ),
        now=NOW,
    )

    assert decision.subscription_eligible is True
    assert decision.execution_eligible is True
    assert decision.market_state == "LIVE"
    assert decision.book_quality == "READY_MEDIUM"
    assert decision.book_age_ms == 5_000


def test_stale_book_keeps_subscription_and_blocks_execution() -> None:
    decision = classify_market_token(
        _token(
            latest_book_at=NOW - timedelta(seconds=90),
            book_status="ok",
            best_bid=Decimal("0.41"),
            best_ask=Decimal("0.43"),
        ),
        config=MarketUniverseConfig(book_ttl_seconds=60),
        now=NOW,
    )

    assert decision.subscription_eligible is True
    assert decision.execution_eligible is False
    assert decision.market_state == "STALE"
    assert decision.execution_reason == "stale_book"


def test_stale_not_ready_book_still_keeps_subscription() -> None:
    decision = classify_market_token(
        _token(
            latest_book_at=NOW - timedelta(seconds=90),
            book_status="not_ready",
        ),
        config=MarketUniverseConfig(book_ttl_seconds=60),
        now=NOW,
    )

    assert decision.subscription_eligible is True
    assert decision.execution_eligible is False
    assert decision.market_state == "STALE"
    assert decision.execution_reason == "stale_book"


def test_disconnected_book_is_stale_not_pending_book() -> None:
    decision = classify_market_token(
        _token(
            latest_book_at=NOW,
            book_status="disconnected",
            best_bid=Decimal("0.41"),
            best_ask=Decimal("0.43"),
        ),
        now=NOW,
    )

    assert decision.subscription_eligible is True
    assert decision.execution_eligible is False
    assert decision.market_state == "STALE"
    assert decision.execution_reason == "book_disconnected"


def test_no_clob_book_stays_subscribed_for_reprobe_but_not_executable() -> None:
    decision = classify_market_token(
        _token(
            latest_book_at=NOW,
            book_status="no_clob_book",
        ),
        now=NOW,
    )

    assert decision.subscription_eligible is True
    assert decision.execution_eligible is False
    assert decision.market_state == "TRADABLE_PENDING_BOOK"
    assert decision.subscription_reason == "metadata_ready"
    assert decision.execution_reason == "no_clob_book"


def test_one_sided_book_stays_subscribed_but_not_executable() -> None:
    decision = classify_market_token(
        _token(
            latest_book_at=NOW,
            book_status="one_sided",
            best_bid=Decimal("0.41"),
            best_ask=None,
        ),
        now=NOW,
    )

    assert decision.subscription_eligible is True
    assert decision.execution_eligible is False
    assert decision.market_state == "TRADABLE_PENDING_BOOK"


def test_status_missing_and_placeholders_are_excluded_by_default() -> None:
    decisions = compute_universe_decisions(
        [
            _token(asset_id="missing-status", status_present=False),
            _token(asset_id="placeholder", market_slug="trade-indexer-placeholder-123"),
        ],
        now=NOW,
    )

    assert {decision.asset_id for decision in decisions if decision.subscription_eligible} == set()
    assert decisions[0].subscription_reason == "status_missing"
    assert decisions[1].subscription_reason == "placeholder_market"


def test_closed_resolved_and_incomplete_token_mapping_are_not_subscribed() -> None:
    decisions = compute_universe_decisions(
        [
            _token(asset_id="closed", closed=True),
            _token(asset_id="resolved", resolved=True),
            _token(asset_id="one-token", token_count=1),
        ],
        now=NOW,
    )

    assert {decision.asset_id for decision in decisions if decision.subscription_eligible} == set()
    assert decisions[0].market_state == "CLOSING"
    assert decisions[1].market_state == "RESOLVED"
    assert decisions[2].subscription_reason == "insufficient_token_mapping"


def test_universe_diff_reports_added_and_removed_assets() -> None:
    previous_decisions = compute_universe_decisions(
        [
            _token(
                asset_id="asset-a",
                latest_book_at=NOW - timedelta(seconds=5),
                book_status="ok",
                best_bid=Decimal("0.41"),
                best_ask=Decimal("0.43"),
            ),
            _token(asset_id="asset-b"),
        ],
        now=NOW,
    )
    previous = build_token_universe_diff(previous_decisions, generation=1)
    next_decisions = compute_universe_decisions(
        [
            _token(asset_id="asset-b"),
            _token(
                asset_id="asset-c",
                latest_book_at=NOW - timedelta(seconds=5),
                book_status="ok",
                best_bid=Decimal("0.22"),
                best_ask=Decimal("0.24"),
            ),
        ],
        now=NOW,
    )

    diff = build_token_universe_diff(next_decisions, previous=previous)

    assert diff.generation == 2
    assert diff.added_subscription_asset_ids == {"asset-c"}
    assert diff.removed_subscription_asset_ids == {"asset-a"}
    assert diff.added_execution_asset_ids == {"asset-c"}
    assert diff.removed_execution_asset_ids == {"asset-a"}


def test_full_reconcile_marks_absent_known_tokens_as_closing() -> None:
    current = [_token(asset_id="asset-b")]
    known = [_token(asset_id="asset-a")]

    reconciled, removed_count = _append_absent_registry_removals(current, known_tokens=known)
    decisions = compute_universe_decisions(reconciled, now=NOW)
    by_asset = {decision.asset_id: decision for decision in decisions}

    assert removed_count == 1
    assert by_asset["asset-a"].market_state == "CLOSING"
    assert by_asset["asset-a"].subscription_eligible is False


def test_complete_discovery_snapshot_preserves_absent_history_without_lifecycle_truth() -> None:
    active = [_token(asset_id="asset-b")]
    known = [
        _token(asset_id="asset-a", status_present=False, source="book_probe"),
        _token(
            asset_id="asset-b",
            latest_book_at=NOW,
            book_status="ok",
            best_bid=Decimal("0.41"),
            best_ask=Decimal("0.43"),
            source="book_probe",
        ),
    ]

    reconciled, removed_count = _reconcile_authoritative_active_snapshot(
        active_tokens=active,
        known_tokens=known,
        authoritative_complete=True,
    )
    decisions = compute_universe_decisions(reconciled, now=NOW)
    by_asset = {decision.asset_id: decision for decision in decisions}

    assert removed_count == 0
    assert by_asset["asset-a"].market_state != "CLOSING"
    assert by_asset["asset-a"].token.active is True
    assert by_asset["asset-a"].token.closed is False
    assert by_asset["asset-a"].subscription_reason == "status_missing"
    assert by_asset["asset-b"].execution_eligible is True


def test_explicit_lifecycle_snapshot_can_mark_absent_known_tokens_as_closing() -> None:
    reconciled, removed_count = _reconcile_authoritative_active_snapshot(
        active_tokens=[_token(asset_id="asset-b")],
        known_tokens=[_token(asset_id="asset-a")],
        authoritative_complete=True,
        absence_is_lifecycle_truth=True,
    )
    decisions = compute_universe_decisions(reconciled, now=NOW)
    by_asset = {decision.asset_id: decision for decision in decisions}

    assert removed_count == 1
    assert by_asset["asset-a"].market_state == "CLOSING"
    assert by_asset["asset-a"].subscription_eligible is False


def test_incomplete_active_snapshot_preserves_known_subscriptions() -> None:
    reconciled, removed_count = _reconcile_authoritative_active_snapshot(
        active_tokens=[_token(asset_id="asset-b")],
        known_tokens=[_token(asset_id="asset-a")],
        authoritative_complete=False,
    )
    decisions = compute_universe_decisions(reconciled, now=NOW)

    assert removed_count == 0
    assert {decision.asset_id for decision in decisions if decision.subscription_eligible} == {
        "asset-a",
        "asset-b",
    }


def test_metadata_merge_preserves_existing_book_evidence_for_gamma_tokens() -> None:
    previous = _token(
        asset_id="asset-a",
        latest_book_at=NOW,
        book_status="ok",
        best_bid=Decimal("0.41"),
        best_ask=Decimal("0.43"),
        source="full_sync",
    )
    gamma = _token(asset_id="asset-a", gamma_market_id="gamma-new", market_id=0, source="gamma_api")

    merged = _merge_tokens([previous], [gamma])

    assert len(merged) == 1
    assert merged[0].gamma_market_id == "gamma-new"
    assert merged[0].latest_book_at == NOW
    assert merged[0].book_status == "ok"


def test_metadata_merge_preserves_gamma_open_truth_when_core_identity_wins() -> None:
    core = _token(
        asset_id="asset-a",
        market_slug="core-slug",
        source="core.market_status_snapshot",
    )
    gamma = _token(
        asset_id="asset-a",
        gamma_market_id="gamma-new",
        market_slug="gamma-slug",
        source="gamma_api",
    )

    merged = _merge_tokens([core], [gamma])

    assert len(merged) == 1
    assert merged[0].market_slug == "core-slug"
    assert merged[0].source == "gamma_api"


def test_metadata_merge_allows_gamma_to_reopen_unresolved_core_closed_row() -> None:
    core = _token(
        asset_id="asset-a",
        active=True,
        closed=True,
        resolved=False,
        completion_status="GAMMA_CLOSED",
        source="core.market_status_snapshot",
    )
    gamma = _token(
        asset_id="asset-a",
        active=True,
        closed=False,
        resolved=False,
        completion_status="OPEN",
        source="gamma_api",
    )

    merged = _merge_tokens([core], [gamma])

    assert len(merged) == 1
    assert merged[0].closed is False
    assert merged[0].completion_status == "OPEN"
    assert merged[0].source == "gamma_api"


def test_metadata_merge_accepts_exact_gamma_inactive_lifecycle_truth() -> None:
    core = _token(
        asset_id="asset-a",
        active=True,
        closed=False,
        resolved=False,
        source="core.market_status_snapshot",
    )
    gamma = _token(
        asset_id="asset-a",
        active=False,
        closed=False,
        resolved=False,
        completion_status="GAMMA_CLOSED",
        source="gamma_api",
    )

    merged = _merge_tokens([core], [gamma])

    assert len(merged) == 1
    assert merged[0].active is False
    assert merged[0].completion_status == "GAMMA_CLOSED"
    assert merged[0].source == "gamma_api"


def test_metadata_merge_prefers_gamma_identity_over_duplicate_clob_identity() -> None:
    gamma = _token(asset_id="asset-a", gamma_market_id="gamma-new", market_slug="gamma-slug", source="gamma_api")
    clob = _token(
        asset_id="asset-a",
        market_id=0,
        gamma_market_id=None,
        condition_id="0xcondition",
        market_slug="clob-slug",
        source="clob_markets_api",
    )

    merged = _merge_tokens([clob], [gamma])

    assert len(merged) == 1
    assert merged[0].source == "gamma_api"
    assert merged[0].gamma_market_id == "gamma-new"
    assert merged[0].market_slug == "gamma-slug"


def test_gamma_market_mapping_overrides_core_placeholder_for_same_asset() -> None:
    placeholder = _token(
        asset_id="asset-a",
        condition_id="0xplaceholder",
        market_slug="trade-indexer-placeholder-0xplaceholder",
        market_title="Trade indexer placeholder market",
        latest_book_at=NOW,
        book_status="ok",
        best_bid=Decimal("0.49"),
        best_ask=Decimal("0.51"),
        source="core",
    )
    gamma = _token(
        asset_id="asset-a",
        condition_id="0xreal",
        market_slug="btc-updown-5m-real",
        market_title="Bitcoin Up or Down",
        gamma_market_id="2844918",
        market_id=0,
        source="gamma_api",
    )

    merged = _merge_tokens([placeholder], [gamma])

    assert len(merged) == 1
    assert merged[0].condition_id == "0xreal"
    assert merged[0].market_slug == "btc-updown-5m-real"
    assert merged[0].gamma_market_id == "2844918"
    assert merged[0].latest_book_at == NOW
    assert merged[0].book_status == "ok"
