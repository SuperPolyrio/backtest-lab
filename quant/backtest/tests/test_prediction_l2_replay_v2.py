from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from flask import Flask

from quant.backtest.pml2.adapters import _build_transport_coverage_windows
from quant.backtest.pml2.contracts import (
    BookLevel,
    BookSnapshotEvent,
    BookValidityMode,
    MarketLifecycleEvent,
    OrderStatus,
    Outcome,
    Pml2OrderIntent,
    RawOrderSide,
    SubmissionOnStaleBook,
    SubmissionPolicy,
    TimeInForce,
    TradingMode,
    TransportCoverageState,
    TransportCoverageWindow,
)
from quant.backtest.pml2.profiles import get_pml2_profile
from quant.backtest.pml2.service import Pml2RequestError, run_pml2_replay
from quant.backtest.pml2.service_v2 import (
    build_prediction_l2_v2_readiness,
    list_prediction_l2_v2_profiles,
    run_prediction_l2_v2_execution_matrix,
    run_prediction_l2_v2_gap_forecast,
    run_prediction_l2_v2_replay,
)
from quant.backtest.pml2.session import ReplayExecutionSession
from scripts.api.routes import quant as quant_routes

T0 = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)


def _snapshot(
    event_id: str,
    exchange_ts: datetime,
    *,
    local_ts: datetime | None = None,
    ask_size: str = "20",
) -> dict[str, object]:
    return {
        "type": "SNAPSHOT",
        "eventId": event_id,
        "snapshotId": event_id,
        "conditionId": "condition-1",
        "marketId": "market-1",
        "assetId": "yes",
        "outcome": "YES",
        "exchangeTs": exchange_ts.isoformat(),
        "localTs": (local_ts or exchange_ts).isoformat(),
        "bookEpoch": 0,
        "source": "native_l2",
        "bids": [{"price": "0.49", "size": "20"}],
        "asks": [{"price": "0.50", "size": ask_size}],
    }


def _order(
    order_id: str,
    submit_ts: datetime,
    *,
    size: str = "5",
    limit: str = "0.50",
    tif: str = "FAK",
    expires_at: datetime | None = None,
    metadata: dict[str, object] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "orderId": order_id,
        "strategyId": "strategy-1",
        "conditionId": "condition-1",
        "marketId": "market-1",
        "assetId": "yes",
        "outcome": "YES",
        "side": "BUY",
        "size": size,
        "limitPrice": limit,
        "tif": tif,
        "signalTs": submit_ts.isoformat(),
        "entryLatencyMs": 0,
        "venueDelayMs": 0,
        "responseLatencyMs": 0,
        "metadata": metadata or {},
    }
    if expires_at is not None:
        result["expiresAt"] = expires_at.isoformat()
    return result


def _requires_snapshot(event_id: str, at: datetime) -> dict[str, object]:
    return {
        "type": "LIFECYCLE",
        "eventId": event_id,
        "conditionId": "condition-1",
        "marketId": "market-1",
        "exchangeTs": at.isoformat(),
        "localTs": at.isoformat(),
        "source": "native_l2",
        "tradingMode": "LIVE",
        "requiresFreshSnapshot": True,
    }


def _payload(
    *,
    events: list[dict[str, object]],
    orders: list[dict[str, object]],
    submission: dict[str, object] | None = None,
    modeled: dict[str, object] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "runId": "v2-test",
        "profile": "realistic",
        "events": events,
        "orders": orders,
    }
    if submission is not None:
        result["submissionPolicy"] = submission
    if modeled is not None:
        result["modeledExecution"] = modeled
    return result


def _api_client():
    app = Flask(__name__)
    app.register_blueprint(quant_routes.create_quant_blueprint({}))
    return app.test_client()


def test_v2_features_off_preserve_v1_observed_result() -> None:
    payload = _payload(
        events=[_snapshot("book-1", T0)],
        orders=[_order("order-1", T0 + timedelta(seconds=1))],
    )

    v1 = run_pml2_replay(payload)
    v2 = run_prediction_l2_v2_replay(payload)

    assert v1["filled_size"] == v2["filled_size"] == "5.0000000000"
    assert v1["status_counts"] == v2["status_counts"]
    assert (
        v1["execution_matches"][0]["raw_price"]
        == v2["execution_matches"][0]["raw_price"]
    )
    assert (
        v1["execution_matches"][0]["source_event_ids"]
        == v2["execution_matches"][0]["source_event_ids"]
    )
    assert v2["execution_matches"][0]["evidence_tier"] == "OBSERVED_L2"
    assert v2["modeled_fill_estimates"] == []


def test_wait_for_fresh_releases_on_local_delivery_once() -> None:
    submit = T0 + timedelta(seconds=10)
    fresh_exchange = T0 + timedelta(seconds=11)
    fresh_local = T0 + timedelta(seconds=12)
    payload = _payload(
        events=[
            _snapshot("old-book", T0),
            _requires_snapshot("transport-restart", T0 + timedelta(seconds=5)),
            _snapshot("fresh-book", fresh_exchange, local_ts=fresh_local),
            _snapshot(
                "second-fresh-book",
                T0 + timedelta(seconds=12),
                local_ts=T0 + timedelta(seconds=13),
            ),
        ],
        orders=[_order("waiting-order", submit)],
        submission={
            "onStaleBook": "WAIT_FOR_FRESH",
            "maxDataWaitMs": 30_000,
            "onTimeout": "DATA_NOT_READY",
        },
    )

    result = run_prediction_l2_v2_replay(payload)
    audit = result["submission_audit"][0]

    assert result["filled_size"] == "5.0000000000"
    assert audit["requested_submit_ts"] == submit.isoformat()
    assert audit["effective_submit_ts"] == fresh_local.isoformat()
    assert audit["exchange_arrival_ts"] == fresh_local.isoformat()
    assert audit["release_source_event_id"] == "fresh-book"
    assert audit["release_count"] == 1


def test_event_driven_book_age_does_not_wait_for_fresh_delta() -> None:
    submit = T0 + timedelta(seconds=10)
    delta_at = T0 + timedelta(seconds=11)
    payload = _payload(
        events=[
            _snapshot("old-book", T0),
            {
                "type": "DELTA",
                "eventId": "fresh-delta",
                "conditionId": "condition-1",
                "marketId": "market-1",
                "assetId": "yes",
                "outcome": "YES",
                "exchangeTs": delta_at.isoformat(),
                "localTs": delta_at.isoformat(),
                "bookEpoch": 0,
                "source": "native_l2",
                "side": "ASK",
                "price": "0.50",
                "newSize": "20",
            },
        ],
        orders=[_order("delta-release", submit)],
        submission={"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 5_000},
    )

    result = run_prediction_l2_v2_replay(payload)

    assert result["filled_size"] == "5.0000000000"
    assert result["submission_audit"][0]["effective_submit_ts"] == submit.isoformat()
    assert result["submission_audit"][0]["release_source_event_id"] is None


def test_max_age_profile_remains_available_as_a_control() -> None:
    profile = replace(
        get_pml2_profile("realistic"),
        book_validity_mode=BookValidityMode.MAX_AGE,
    )
    session = ReplayExecutionSession(run_id="ttl-control", profile=profile)
    session.ingest_snapshot(
        BookSnapshotEvent(
            snapshot_id="old-book",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            exchange_ts=T0,
            local_ts=T0,
            book_epoch=0,
            bids=(BookLevel("0.49", "20"),),
            asks=(BookLevel("0.50", "20"),),
            source="native_l2",
        )
    )
    order_at = T0 + timedelta(seconds=10)
    session.submit_order(
        Pml2OrderIntent(
            run_id="ttl-control",
            order_id="ttl-order",
            strategy_id="strategy-1",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            side=RawOrderSide.BUY,
            size="5",
            limit_price="0.50",
            tif=TimeInForce.FAK,
            signal_ts=order_at,
            observed_ts=order_at,
            submit_ts=order_at,
            entry_latency_ms=0,
        )
    )

    session.run()

    assert session.result("ttl-order").status == OrderStatus.DATA_NOT_READY
    assert session.result("ttl-order").reason == "book_stale"


def test_max_age_control_uses_receive_clock_not_old_exchange_clock() -> None:
    received = T0 + timedelta(seconds=9)
    submit = T0 + timedelta(seconds=10)
    profile = replace(
        get_pml2_profile("realistic"),
        book_validity_mode=BookValidityMode.MAX_AGE,
    )
    session = ReplayExecutionSession(run_id="receive-clock", profile=profile)
    session.ingest_snapshot(
        BookSnapshotEvent(
            snapshot_id="delayed-book",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            exchange_ts=T0,
            source_received_ts=received,
            local_ts=received,
            book_epoch=0,
            bids=(BookLevel("0.49", "20"),),
            asks=(BookLevel("0.50", "20"),),
            source="native_l2",
        )
    )
    session.submit_order(
        Pml2OrderIntent(
            run_id="receive-clock",
            order_id="receive-clock-order",
            strategy_id="strategy-1",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            side=RawOrderSide.BUY,
            size="5",
            limit_price="0.50",
            tif=TimeInForce.FAK,
            signal_ts=submit,
            observed_ts=submit,
            submit_ts=submit,
            entry_latency_ms=0,
        )
    )
    session.run()

    assert session.result("receive-clock-order").status == OrderStatus.FILLED


def test_quiet_but_covered_book_remains_valid_and_links_coverage_proof() -> None:
    session = ReplayExecutionSession(
        run_id="coverage-run",
        profile=get_pml2_profile("realistic"),
    )
    proof = TransportCoverageWindow(
        proof_id="quiet-proof",
        condition_id="condition-1",
        market_id="market-1",
        start_ts=T0,
        end_ts=T0 + timedelta(minutes=10),
        allowed=True,
        state=TransportCoverageState.QUIET_BUT_COVERED,
        reason="connection_heartbeat_continuous",
        asset_ids=("yes", "no"),
        source="coverage-test",
    )
    session.register_transport_coverage(proof)
    session.ingest_snapshot(
        BookSnapshotEvent(
            snapshot_id="quiet-book",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            exchange_ts=T0,
            source_received_ts=T0,
            local_ts=T0,
            book_epoch=0,
            bids=(BookLevel("0.49", "20"),),
            asks=(BookLevel("0.50", "20"),),
            source="native_l2",
        )
    )
    order_at = T0 + timedelta(minutes=5)
    session.submit_order(
        Pml2OrderIntent(
            run_id="coverage-run",
            order_id="quiet-order",
            strategy_id="strategy-1",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            side=RawOrderSide.BUY,
            size="5",
            limit_price="0.50",
            tif=TimeInForce.FAK,
            signal_ts=order_at,
            observed_ts=order_at,
            submit_ts=order_at,
            entry_latency_ms=0,
        )
    )

    session.run()
    result = session.result("quiet-order")

    assert result.status == OrderStatus.FILLED
    assert "quiet-proof" in result.fills[0].source_event_ids
    assert session.coverage_manifest().transport_coverage_window_count == 1


def test_known_transport_gap_rejects_even_recent_book() -> None:
    session = ReplayExecutionSession(
        run_id="gap-run",
        profile=get_pml2_profile("realistic"),
    )
    session.register_transport_coverage(
        TransportCoverageWindow(
            proof_id="gap-proof",
            condition_id="condition-1",
            market_id="market-1",
            start_ts=T0,
            end_ts=T0 + timedelta(minutes=1),
            allowed=False,
            state=TransportCoverageState.COVERAGE_GAP,
            reason="dual_feed_gap_overlap",
            asset_ids=("yes", "no"),
            source="coverage-test",
        )
    )
    recent = T0 + timedelta(seconds=9)
    session.ingest_snapshot(
        BookSnapshotEvent(
            snapshot_id="recent-but-gapped",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            exchange_ts=recent,
            source_received_ts=recent,
            local_ts=recent,
            book_epoch=0,
            bids=(BookLevel("0.49", "20"),),
            asks=(BookLevel("0.50", "20"),),
            source="native_l2",
        )
    )
    order_at = T0 + timedelta(seconds=10)
    session.submit_order(
        Pml2OrderIntent(
            run_id="gap-run",
            order_id="gap-order",
            strategy_id="strategy-1",
            condition_id="condition-1",
            market_id="market-1",
            asset_id="yes",
            outcome=Outcome.YES,
            side=RawOrderSide.BUY,
            size="5",
            limit_price="0.50",
            tif=TimeInForce.FAK,
            signal_ts=order_at,
            observed_ts=order_at,
            submit_ts=order_at,
            entry_latency_ms=0,
        )
    )

    session.run()
    result = session.result("gap-order")

    assert result.status == OrderStatus.DATA_NOT_READY
    assert result.reason == "transport_coverage_dual_feed_gap_overlap"


def test_ended_transport_gap_requires_a_later_full_snapshot() -> None:
    def build(*, snapshot_at: datetime) -> ReplayExecutionSession:
        session = ReplayExecutionSession(
            run_id=f"gap-recovery-{snapshot_at.timestamp()}",
            profile=get_pml2_profile("realistic"),
        )
        for start, end, allowed, state, reason in (
            (0, 10, True, TransportCoverageState.COVERAGE_PROVEN, "ready"),
            (10, 20, False, TransportCoverageState.COVERAGE_GAP, "feed_gap"),
            (20, 60, True, TransportCoverageState.COVERAGE_PROVEN, "ready"),
        ):
            session.register_transport_coverage(
                TransportCoverageWindow(
                    proof_id=f"proof-{start}-{end}-{snapshot_at.timestamp()}",
                    condition_id="condition-1",
                    market_id="market-1",
                    start_ts=T0 + timedelta(seconds=start),
                    end_ts=T0 + timedelta(seconds=end),
                    allowed=allowed,
                    state=state,
                    reason=reason,
                    asset_ids=("yes", "no"),
                    source="coverage-test",
                )
            )
        session.ingest_snapshot(
            BookSnapshotEvent(
                snapshot_id=f"snapshot-{snapshot_at.timestamp()}",
                condition_id="condition-1",
                market_id="market-1",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=snapshot_at,
                source_received_ts=snapshot_at,
                local_ts=snapshot_at,
                book_epoch=0,
                bids=(BookLevel("0.49", "20"),),
                asks=(BookLevel("0.50", "20"),),
                source="native_l2",
            )
        )
        order_at = T0 + timedelta(seconds=30)
        session.submit_order(
            Pml2OrderIntent(
                run_id=session.run_id,
                order_id="gap-recovery-order",
                strategy_id="strategy-1",
                condition_id="condition-1",
                market_id="market-1",
                asset_id="yes",
                outcome=Outcome.YES,
                side=RawOrderSide.BUY,
                size="5",
                limit_price="0.50",
                tif=TimeInForce.FAK,
                signal_ts=order_at,
                observed_ts=order_at,
                submit_ts=order_at,
                entry_latency_ms=0,
            )
        )
        session.run()
        return session

    before_gap = build(snapshot_at=T0)
    after_gap = build(snapshot_at=T0 + timedelta(seconds=21))

    assert before_gap.result("gap-recovery-order").status == OrderStatus.DATA_NOT_READY
    assert (
        before_gap.result("gap-recovery-order").reason
        == "book_waiting_for_snapshot_after_transport_gap"
    )
    assert after_gap.result("gap-recovery-order").status == OrderStatus.FILLED


def test_active_active_coverage_is_split_around_exact_gap() -> None:
    rows = [
        {
            "asset_id": asset_id,
            "hour_start": T0,
            "fill_depth_ready": True,
            "fill_depth_reason": "ready",
            "outside_ready": True,
            "continuity_state": "COVERAGE_PROVEN",
            "dual_gap_count": 1,
        }
        for asset_id in ("yes", "no")
    ]
    gaps = [
        {
            "asset_id": "yes",
            "gap_start": T0 + timedelta(seconds=10),
            "recovered_at": T0 + timedelta(seconds=20),
        }
    ]

    windows = _build_transport_coverage_windows(
        condition_id="condition-1",
        market_id="market-1",
        asset_ids=("yes", "no"),
        start=T0,
        end=T0 + timedelta(seconds=30),
        source="active-active-test",
        rows=rows,
        gaps=gaps,
        active_active=True,
    )

    assert [item.allowed for item in windows] == [True, False, True]
    assert windows[1].state == TransportCoverageState.COVERAGE_GAP
    assert windows[1].start_ts == T0 + timedelta(seconds=10)
    assert windows[1].end_ts == T0 + timedelta(seconds=20)


def test_wait_for_fresh_does_not_release_at_exchange_time() -> None:
    submit = T0 + timedelta(seconds=10)
    fresh_exchange = T0 + timedelta(seconds=11)
    fresh_local = T0 + timedelta(seconds=20)
    payload = _payload(
        events=[
            _snapshot("old-book", T0),
            _requires_snapshot("transport-restart", T0 + timedelta(seconds=5)),
            _snapshot("delayed-local-book", fresh_exchange, local_ts=fresh_local),
        ],
        orders=[_order("delayed-order", submit)],
        submission={"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 5_000},
    )

    result = run_prediction_l2_v2_replay(payload)

    assert result["execution_matches"] == []
    assert result["status_counts"] == {"DATA_NOT_READY": 1}
    assert result["submission_audit"][0]["effective_submit_ts"] is None
    assert result["submission_audit"][0]["exchange_arrival_ts"] is None
    assert result["submission_audit"][0]["planned_exchange_arrival_ts"] == (
        submit.isoformat()
    )


def test_fresh_local_delivery_at_deadline_precedes_timeout() -> None:
    submit = T0 + timedelta(seconds=10)
    deadline = submit + timedelta(seconds=30)
    payload = _payload(
        events=[
            _snapshot("old-book", T0),
            _requires_snapshot("transport-restart", T0 + timedelta(seconds=5)),
            _snapshot(
                "deadline-book",
                deadline - timedelta(seconds=1),
                local_ts=deadline,
            ),
        ],
        orders=[_order("deadline-order", submit)],
        submission={"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 30_000},
    )

    result = run_prediction_l2_v2_replay(payload)

    assert result["filled_size"] == "5.0000000000"
    assert result["submission_audit"][0]["effective_submit_ts"] == deadline.isoformat()


def test_gtd_absolute_expiry_is_not_extended_by_data_wait() -> None:
    submit = T0 + timedelta(seconds=10)
    expires = submit + timedelta(seconds=5)
    payload = _payload(
        events=[
            _snapshot("old-book", T0),
            _requires_snapshot("transport-restart", T0 + timedelta(seconds=5)),
            _snapshot(
                "too-late-book",
                submit + timedelta(seconds=5),
                local_ts=submit + timedelta(seconds=6),
            ),
        ],
        orders=[
            _order(
                "expiring-order",
                submit,
                tif="GTD",
                expires_at=expires,
            )
        ],
        submission={"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 30_000},
    )

    result = run_prediction_l2_v2_replay(payload)

    assert result["status_counts"] == {"EXPIRED": 1}
    assert result["execution_matches"] == []
    assert result["submission_audit"][0]["effective_submit_ts"] is None


def test_fak_partial_and_fok_atomicity_remain_distinct() -> None:
    submit = T0 + timedelta(seconds=1)
    events = [_snapshot("book-small", T0, ask_size="3")]
    fak = run_prediction_l2_v2_replay(
        _payload(events=events, orders=[_order("fak", submit, tif="FAK")])
    )
    fok = run_prediction_l2_v2_replay(
        _payload(events=events, orders=[_order("fok", submit, tif="FOK")])
    )

    assert fak["filled_size"] == "3.0000000000"
    assert fak["status_counts"] == {"PARTIAL": 1}
    assert fok["filled_size"] == "0.0000000000"
    assert fok["status_counts"] == {"REJECTED": 1}


def test_maker_expected_estimate_never_becomes_observed_match() -> None:
    submit = T0 + timedelta(seconds=1)
    payload = _payload(
        events=[_snapshot("book-1", T0)],
        orders=[
            _order(
                "maker-order",
                submit,
                limit="0.40",
                tif="GTD",
                expires_at=submit + timedelta(seconds=30),
                metadata={"maker_survival_horizon_seconds": 30},
            )
        ],
        modeled={"maker": "EXPECTED", "gap": "OFF", "randomSeed": 7},
    )

    result = run_prediction_l2_v2_replay(payload)

    assert result["execution_matches"] == []
    assert result["observed_execution"]["filled_size"] == "0"
    assert len(result["modeled_fill_estimates"]) == 1
    assert result["modeled_fill_estimates"][0]["evidence_tier"] == "MODELED_EXPECTED"
    estimate = result["modeled_fill_estimates"][0]
    assert "conditional_fill_fraction" in result["orders"][0]["maker_survival_forecast"]
    assert float(estimate["expected_size"]) == pytest.approx(
        float(estimate["p_any_fill"]) * float(estimate["conditional_expected_size"]),
        abs=1e-9,
    )
    assert (
        result["modeled_execution"]["pnl_eligibility"]
        == "EXPECTED_OR_SCENARIO_PNL_ONLY"
    )


def test_gap_expected_and_monte_carlo_are_separate_and_reproducible() -> None:
    submit = T0 + timedelta(seconds=10)
    base = _payload(
        events=[
            _snapshot("old-book", T0),
            _requires_snapshot("transport-restart", T0 + timedelta(seconds=5)),
        ],
        orders=[_order("gap-order", submit)],
        submission={"onStaleBook": "WAIT_FOR_FRESH", "maxDataWaitMs": 5_000},
        modeled={"maker": "OFF", "gap": "MONTE_CARLO", "randomSeed": 123},
    )

    first = run_prediction_l2_v2_replay(base)
    second = run_prediction_l2_v2_replay(base)

    assert first["execution_matches"] == []
    assert first["modeled_fill_estimates"] == second["modeled_fill_estimates"]
    assert first["modeled_fill_estimates"][0]["evidence_tier"] == "MODELED_MONTE_CARLO"


def test_gap_model_abstains_beyond_thirty_seconds() -> None:
    result = run_prediction_l2_v2_gap_forecast(
        {
            "decisionTs": T0.isoformat(),
            "gapMs": 216_000,
            "requestedSize": "10",
            "price": "0.979",
        }
    )

    assert result["forecast"]["p_executable"] == "0"
    assert result["forecast"]["domain_status"] == "ABSTAIN_GAP_OUTSIDE_TRAINED_SUPPORT"


def test_v2_strict_unknown_fields_and_profiles_readiness() -> None:
    with pytest.raises(Pml2RequestError, match="unknown submission policy fields"):
        run_prediction_l2_v2_replay(
            {
                **_payload(
                    events=[_snapshot("book", T0)],
                    orders=[_order("order", T0 + timedelta(seconds=1))],
                ),
                "submissionPolicy": {
                    "onStaleBook": "WAIT_FOR_FRESH",
                    "maxDataWaitMs": 30_000,
                    "maxDataWiatMs": 30_000,
                },
            }
        )

    profiles = list_prediction_l2_v2_profiles()
    readiness = build_prediction_l2_v2_readiness()
    assert any(item["name"] == "wait30_fak" for item in profiles["execution_variants"])
    assert readiness["hard_boundaries"]["stale_l2_observed_fill_allowed"] is False
    assert (
        readiness["hard_boundaries"]["elapsed_age_alone_invalidates_synced_book"]
        is False
    )
    assert (
        readiness["hard_boundaries"]["known_transport_gap_requires_later_snapshot"]
        is True
    )
    assert readiness["capabilities"]["event_driven_book_validity"] is True
    assert readiness["capabilities"]["ttl_fallback_without_coverage_proof"] is False
    assert readiness["snapshot_batch_backend"]["supported_tif"] == [
        "FAK",
        "FOK",
        "IOC",
    ]
    assert (
        readiness["snapshot_batch_backend"]["dynamic_delta_queue_gtd_backend"]
        == "PYTHON"
    )
    assert readiness["gap_availability"]["maximum_supported_gap_ms"] == 30_000


def test_execution_matrix_reports_observed_and_modeled_separately() -> None:
    payload = _payload(
        events=[_snapshot("book-1", T0)],
        orders=[_order("matrix-order", T0 + timedelta(seconds=1))],
    )
    payload["variants"] = ["strict_fok", "realistic_fak"]

    result = run_prediction_l2_v2_execution_matrix(payload)

    assert result["comparison"]["strict_fok"]["observed_filled_size"] == "5.0000000000"
    assert result["comparison"]["realistic_fak"]["modeled_expected_size"] == "0"


def test_gtd_resting_order_uses_trade_to_advance_maker_queue() -> None:
    submit = T0 + timedelta(seconds=1)
    payload = _payload(
        events=[
            _snapshot("book-1", T0),
            {
                "type": "TRADE",
                "eventId": "seller-trade",
                "conditionId": "condition-1",
                "marketId": "market-1",
                "assetId": "yes",
                "outcome": "YES",
                "exchangeTs": (T0 + timedelta(seconds=2)).isoformat(),
                "localTs": (T0 + timedelta(seconds=2)).isoformat(),
                "source": "native_l2",
                "price": "0.49",
                "size": "100",
                "aggressorSide": "SELL",
                "sourceEventIds": ["raw-seller-trade"],
            },
        ],
        orders=[
            _order(
                "maker-gtd",
                submit,
                limit="0.49",
                tif="GTD",
                expires_at=submit + timedelta(seconds=30),
            )
        ],
    )

    result = run_prediction_l2_v2_replay(payload)

    assert result["filled_size"] == "5.0000000000"
    assert result["execution_matches"][0]["liquidity_role"] == "MAKER"
    assert result["execution_matches"][0]["source_event_ids"] == ["seller-trade"]


def test_wait_for_fresh_chunked_run_equals_one_shot() -> None:
    submit = T0 + timedelta(seconds=10)
    fresh_exchange = T0 + timedelta(seconds=11)
    fresh_local = T0 + timedelta(seconds=12)
    policy = SubmissionPolicy(
        on_stale_book=SubmissionOnStaleBook.WAIT_FOR_FRESH,
        max_data_wait_ms=30_000,
        on_timeout=OrderStatus.DATA_NOT_READY,
    )

    def build(run_id: str) -> ReplayExecutionSession:
        session = ReplayExecutionSession(
            run_id=run_id,
            profile=get_pml2_profile("realistic"),
            submission_policy=policy,
            execution_model_name="PREDICTION_L2_REPLAY_V2",
        )
        session.ingest_snapshot(
            BookSnapshotEvent(
                snapshot_id="old",
                condition_id="condition-1",
                market_id="market-1",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=T0,
                local_ts=T0,
                book_epoch=0,
                bids=(BookLevel("0.49", "20"),),
                asks=(BookLevel("0.50", "20"),),
                source="native_l2",
            )
        )
        session.ingest_lifecycle(
            MarketLifecycleEvent(
                event_id="transport-restart",
                condition_id="condition-1",
                market_id="market-1",
                exchange_ts=T0 + timedelta(seconds=5),
                local_ts=T0 + timedelta(seconds=5),
                trading_mode=TradingMode.LIVE,
                source="native_l2",
                requires_fresh_snapshot=True,
            )
        )
        session.ingest_snapshot(
            BookSnapshotEvent(
                snapshot_id="fresh",
                condition_id="condition-1",
                market_id="market-1",
                asset_id="yes",
                outcome=Outcome.YES,
                exchange_ts=fresh_exchange,
                local_ts=fresh_local,
                book_epoch=0,
                bids=(BookLevel("0.49", "20"),),
                asks=(BookLevel("0.50", "20"),),
                source="native_l2",
            )
        )
        session.submit_order(
            Pml2OrderIntent(
                run_id=run_id,
                order_id="chunk-order",
                strategy_id="strategy-1",
                condition_id="condition-1",
                market_id="market-1",
                asset_id="yes",
                outcome=Outcome.YES,
                side=RawOrderSide.BUY,
                size="5",
                limit_price="0.50",
                tif=TimeInForce.FAK,
                signal_ts=submit,
                observed_ts=submit,
                submit_ts=submit,
                entry_latency_ms=0,
            )
        )
        return session

    chunked = build("same-run")
    chunked.run(until=fresh_exchange)
    assert chunked.orders["chunk-order"].status == OrderStatus.WAITING_FOR_DATA
    chunked.run()
    one_shot = build("same-run")
    one_shot.run()

    assert (
        chunked.result("chunk-order").as_dict()
        == one_shot.result("chunk-order").as_dict()
    )
    assert chunked.replay_hash == one_shot.replay_hash


def test_v2_http_routes_are_callable() -> None:
    client = _api_client()
    profiles = client.get("/quant/prediction-l2/v2/profiles")
    readiness = client.get("/quant/prediction-l2/v2/readiness")
    gap = client.post(
        "/quant/prediction-l2/v2/gap-forecast",
        json={
            "decisionTs": T0.isoformat(),
            "gapMs": 347_000,
            "requestedSize": "10",
        },
    )
    invalid = client.post(
        "/quant/prediction-l2/v2/gap-forecast",
        json={
            "decisionTs": T0.isoformat(),
            "gapMs": 1_000,
            "requestedSize": "10",
            "typo": True,
        },
    )
    replay = client.post(
        "/quant/prediction-l2/v2/replay",
        json=_payload(
            events=[_snapshot("api-book", T0)],
            orders=[_order("api-order", T0 + timedelta(seconds=1))],
        ),
    )
    matrix_payload = _payload(
        events=[_snapshot("matrix-book", T0)],
        orders=[_order("matrix-order", T0 + timedelta(seconds=1))],
    )
    matrix_payload["variants"] = ["strict_fok", "strict_fak_control"]
    matrix = client.post(
        "/quant/prediction-l2/v2/execution-matrix",
        json=matrix_payload,
    )
    maker = client.post(
        "/quant/prediction-l2/v2/maker-forecast",
        json={
            "decisionTs": T0.isoformat(),
            "horizonSeconds": 30,
            "side": "BUY",
            "orderSize": "5",
        },
    )

    assert profiles.status_code == 200
    assert readiness.status_code == 200
    assert gap.status_code == 200
    assert replay.status_code == 200
    assert matrix.status_code == 200
    assert maker.status_code == 200
    assert invalid.status_code == 400


def test_v2_chain_only_audit_is_explicit_and_unknown_fields_stay_strict() -> None:
    payload = _payload(
        events=[_snapshot("chain-book", T0)],
        orders=[_order("chain-order", T0 + timedelta(seconds=1))],
    )
    result = run_prediction_l2_v2_replay({**payload, "auditMode": "CHAIN_ONLY"})

    assert result["audit_mode"] == "CHAIN_ONLY"
    assert result["audit_contract"]["mode"] == "CHAIN_ONLY"
    assert result["audit_contract"]["event_count"] > 0
    assert result["audit_contract"]["stored_event_count"] == 0
    with pytest.raises(Pml2RequestError, match="unknown V2 request fields"):
        run_prediction_l2_v2_replay({**payload, "auditModeTypo": "CHAIN_ONLY"})
