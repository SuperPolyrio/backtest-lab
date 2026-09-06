from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from quant.backtest.backtest_engine import (
    PML2_LEGACY_PMXT_SOURCE,
    PML2_XUE_NATIVE_SOURCE,
    PREDICTION_L2_REPLAY_V1_MODE,
    BacktestParameters,
    PricePoint,
    _fill_decision,
    _pml2_l2_source_for_run,
    setup_main_pml2_execution,
)
from quant.backtest.pml2.adapters import Pml2ArchiveEventRestoreResult
from quant.backtest.pml2.archive_execution import XueNativeExecutionProvider
from quant.backtest.pml2.contracts import (
    BookLevel,
    BookSnapshotEvent,
    Outcome,
    TransportCoverageState,
    TransportCoverageWindow,
)


T0 = datetime(2026, 8, 27, 22, 50, tzinfo=timezone.utc)


class _MetadataCursor:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _query, _params):
        return None

    def fetchone(self):
        return {
            "market_id": 2331666,
            "condition_id": "0xCondition",
            "token_id": "yes-token",
            "token_id_hex": "0x01",
            "market_slug": "warriors-west",
            "token_side": "YES",
            "yes_token_id": "yes-token",
            "no_token_id": "no-token",
        }


class _MetadataConnection:
    def cursor(self):
        return _MetadataCursor()


class _ArchiveLoader:
    archive_dir = Path("/xue/native/l2")

    def restore_condition_events(
        self, **kwargs: object
    ) -> Pml2ArchiveEventRestoreResult:
        end_time = kwargs["end_time"]
        assert isinstance(end_time, datetime)
        events = (
            BookSnapshotEvent(
                snapshot_id="xue-yes-old-but-covered",
                condition_id="0xcondition",
                market_id="2331666",
                asset_id="yes-token",
                outcome=Outcome.YES,
                exchange_ts=T0,
                source_received_ts=T0,
                local_ts=T0,
                book_epoch=0,
                bids=(BookLevel("0.49", "10"),),
                asks=(BookLevel("0.50", "10"),),
                source="xue_native_l2_archive",
            ),
            BookSnapshotEvent(
                snapshot_id="xue-no-old-but-covered",
                condition_id="0xcondition",
                market_id="2331666",
                asset_id="no-token",
                outcome=Outcome.NO,
                exchange_ts=T0,
                source_received_ts=T0,
                local_ts=T0,
                book_epoch=0,
                bids=(BookLevel("0.50", "10"),),
                asks=(BookLevel("0.51", "10"),),
                source="xue_native_l2_archive",
            ),
        )
        coverage = TransportCoverageWindow(
            proof_id="xue-heartbeat-proof",
            condition_id="0xcondition",
            market_id="2331666",
            start_ts=T0,
            end_ts=end_time + timedelta(microseconds=1),
            allowed=True,
            state=TransportCoverageState.QUIET_BUT_COVERED,
            reason="connection_heartbeat_continuous",
            asset_ids=("yes-token", "no-token"),
            source="quant.l2_active_active_coverage_hourly",
        )
        return Pml2ArchiveEventRestoreResult(
            events=events,
            source_files=("xue-fixture.parquet",),
            source="xue_native_l2_archive",
            restored=True,
            reason="xue_native_l2_event_timeline_restore_complete",
            row_count=2,
            source_manifest_hash="xue-manifest",
            snapshot_count=2,
            clock_verified=True,
            clock_evidence="BASELINE_AND_RAW_EVENT_DUAL_CLOCK_FRAME_VERIFIED",
            baseline_clock_count=2,
            raw_event_clock_count=0,
            frame_evidence_verified=True,
            transport_coverage_windows=(coverage,),
        )


def test_main_engine_defaults_to_xue_and_uses_coverage_proof(monkeypatch) -> None:
    from quant.backtest.pml2 import archive_execution

    monkeypatch.delenv("POLYDATA_QUANT_PML2_L2_SOURCE", raising=False)
    monkeypatch.setattr(
        archive_execution,
        "Pml2ArchiveSnapshotLoader",
        _ArchiveLoader,
    )
    params = BacktestParameters(
        execution_price_mode=PREDICTION_L2_REPLAY_V1_MODE,
        execution_profile="realistic",
        order_role="taker",
        max_entry_price=Decimal("0.50"),
        allow_partial_fill=True,
    )
    point = PricePoint(
        1,
        Decimal("0.50"),
        Decimal(1),
        timestamp=T0 + timedelta(minutes=5),
    )
    run = {
        "run_id": 99,
        "market_id": 2331666,
        "market_slug": "warriors-west",
        "token_side": "YES",
        "meta": {},
    }

    pmxt, snapshots, setup = setup_main_pml2_execution(
        _MetadataConnection(),
        run,
        [point],
        params,
    )
    fill = _fill_decision(
        params,
        point,
        run,
        "BUY_YES",
        target_size=Decimal(5),
    )

    assert pmxt is None
    assert snapshots == []
    assert setup["source_mode"] == PML2_XUE_NATIVE_SOURCE
    assert "_pmxt_compact_book_provider" not in run
    assert run["_pml2_session"].audit_mode.value == "CHAIN_ONLY"
    assert fill["fill_status"] == "FILLED"
    assert fill["filled_size"] == Decimal("5.0000000000")
    assert "xue-heartbeat-proof" in fill["execution_audit"]["fills"][0][
        "source_event_ids"
    ]
    native_provider = run["_pml2_native_provider"]
    assert isinstance(native_provider, XueNativeExecutionProvider)
    context = native_provider.context()
    assert context["coverage_proof_count"] == 1
    assert context["clock_verified"] is True


def test_legacy_pmxt_requires_explicit_source_selection() -> None:
    assert _pml2_l2_source_for_run({"meta": {}}) == PML2_XUE_NATIVE_SOURCE
    assert (
        _pml2_l2_source_for_run(
            {"meta": {"pml2_l2_source": "LEGACY_PMXT"}}
        )
        == PML2_LEGACY_PMXT_SOURCE
    )
