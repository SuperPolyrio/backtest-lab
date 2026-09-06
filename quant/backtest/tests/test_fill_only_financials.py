from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from quant.backtest.fill_only_financials import (
    FillOnlyExecutionPosition,
    FillOnlyFinancialError,
    finalize_fill_only_positions,
    iter_fill_only_execution_positions,
    load_fill_only_execution_position_market_ids,
    load_fill_only_execution_positions,
    write_fill_only_execution_positions,
    write_fill_only_financial_bundle,
)
from quant.backtest.market_settlement import (
    EXACT_CANONICAL_BLOCK_HEADER_GRADE,
    ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE,
    ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE,
    SETTLED_YES,
    UNRESOLVED,
    MarketSettlementRecord,
)

UTC = timezone.utc
CUTOFF = datetime(2026, 8, 25, 13, 14, 50, tzinfo=UTC)
FINALIZED = CUTOFF - timedelta(days=1)
CONDITION = "0x" + "a" * 64
TOKEN = "123"


def _settlement(*, market_id: int, classification: str) -> MarketSettlementRecord:
    payable = classification == SETTLED_YES
    return MarketSettlementRecord(
        market_id=market_id,
        condition_id=CONDITION,
        slug=f"market-{market_id}",
        title="Fixture",
        category="fixture",
        cutoff_ts=CUTOFF,
        classification=classification,
        classification_reason="fixture",
        completion_status="SETTLED" if payable else "ENDED_AWAITING_ORACLE",
        settlement_code=1 if payable else None,
        settlement_outcome="YES" if payable else None,
        settlement_source="fixture" if payable else None,
        protocol_finalized_at=FINALIZED if payable else None,
        protocol_finalized_block=1000 if payable else None,
        settlement_event_id=1 if payable else None,
        settlement_tx_hash="0x" + "b" * 64 if payable else None,
        tokens=(
            {"outcome": "YES", "outcome_index": 0, "token_id": TOKEN},
            {"outcome": "NO", "outcome_index": 1, "token_id": "456"},
        ),
        payout_by_token={TOKEN: Decimal(1), "456": Decimal(0)} if payable else None,
        evidence={"fixture": True},
        record_sha256=(str(market_id) * 64)[:64],
    )


def _position(*, market_id: int, position_id: str) -> FillOnlyExecutionPosition:
    return FillOnlyExecutionPosition(
        execution_run_id="run-1",
        profile="primary",
        position_id=position_id,
        market_id=market_id,
        condition_id=CONDITION,
        asset_id=TOKEN,
        outcome="YES",
        filled_size=Decimal(10),
        entry_notional=Decimal(6),
        fee_paid=Decimal("0.1"),
        first_fill_ts=FINALIZED - timedelta(minutes=2),
        last_fill_ts=FINALIZED - timedelta(minutes=1),
        first_fill_block=900,
        last_fill_block=901,
        source_order_ids=(f"order-{position_id}",),
        source_fill_ids=(f"fill-{position_id}",),
    )


def test_complete_portfolio_has_real_financial_metrics() -> None:
    summary, rows = finalize_fill_only_positions(
        [_position(market_id=1, position_id="p1")],
        {1: _settlement(market_id=1, classification=SETTLED_YES)},
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
    )

    assert summary["status"] == "FINANCIAL_COMPLETE"
    assert summary["full_portfolio"]["settlement_payout"] == Decimal(10)
    assert summary["full_portfolio"]["net_pnl"] == Decimal("3.9")
    assert summary["full_portfolio"]["risk_metrics"] is not None
    assert summary["full_portfolio"]["risk_metrics"][
        "maximum_drawdown_fraction"
    ] == Decimal(0)
    assert rows[0]["settlement_complete"] is True


def test_partial_portfolio_reports_bounds_not_fake_metrics() -> None:
    summary, rows = finalize_fill_only_positions(
        [
            _position(market_id=1, position_id="p1"),
            _position(market_id=2, position_id="p2"),
        ],
        {
            1: _settlement(market_id=1, classification=SETTLED_YES),
            2: _settlement(market_id=2, classification=UNRESOLVED),
        },
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
    )

    assert summary["status"] == "FINANCIAL_PARTIAL"
    assert summary["full_portfolio"]["net_pnl"] is None
    bounds = summary["unresolved_and_technical_bounds"]
    assert bounds["portfolio_payout_lower_bound"] == Decimal(10)
    assert bounds["portfolio_payout_upper_bound"] == Decimal(20)
    assert {row["settlement_complete"] for row in rows} == {True, False}


def test_block_only_settlement_computes_pnl_but_not_time_risk_metrics() -> None:
    settlement = replace(
        _settlement(market_id=1, classification=SETTLED_YES),
        protocol_finalized_at=None,
    )

    summary, rows = finalize_fill_only_positions(
        [_position(market_id=1, position_id="p1")],
        {1: settlement},
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
    )

    assert rows[0]["settlement_complete"] is True
    assert summary["status"] == "FINANCIAL_COMPLETE"
    assert summary["full_portfolio"]["net_pnl"] == Decimal("3.9")
    assert summary["full_portfolio"]["risk_metrics"] is None
    assert summary["full_portfolio"]["risk_metrics_reason"] == (
        "SETTLEMENT_TIMESTAMPS_INCOMPLETE"
    )


def test_formal_finalization_rejects_weaker_cutoff_block_evidence() -> None:
    settlement = replace(
        _settlement(market_id=1, classification=SETTLED_YES),
        evidence={
            "evidence_grade": ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE
        },
    )

    summary, rows = finalize_fill_only_positions(
        [_position(market_id=1, position_id="p1")],
        {1: settlement},
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
        required_evidence_grade="exact-header",
    )

    assert summary["status"] == "FINANCIAL_PARTIAL"
    assert rows[0]["settlement_complete"] is False
    assert rows[0]["settlement_classification"] == "TECHNICAL_EVIDENCE_MISSING"
    assert rows[0]["settlement_reason"].startswith(
        "SETTLEMENT_EVIDENCE_GRADE_BELOW_REQUIRED"
    )


def test_normal_finalization_accepts_oracle_time_without_exact_header() -> None:
    settlement = replace(
        _settlement(market_id=1, classification=SETTLED_YES),
        evidence={
            "evidence_grade": ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE
        },
    )

    summary, rows = finalize_fill_only_positions(
        [_position(market_id=1, position_id="p1")],
        {1: settlement},
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
        required_evidence_grade="oracle-finalized",
    )

    assert summary["status"] == "FINANCIAL_COMPLETE"
    assert summary["required_settlement_evidence"] == "oracle-finalized"
    assert rows[0]["settlement_complete"] is True
    assert rows[0]["net_pnl_after_recorded_fee"] == Decimal("3.9")


def test_normal_finalization_rejects_legacy_block_only_evidence() -> None:
    settlement = replace(
        _settlement(market_id=1, classification=SETTLED_YES),
        evidence={
            "evidence_grade": ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE
        },
    )

    summary, rows = finalize_fill_only_positions(
        [_position(market_id=1, position_id="p1")],
        {1: settlement},
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
        required_evidence_grade="oracle-finalized",
    )

    assert summary["status"] == "FINANCIAL_PARTIAL"
    assert rows[0]["settlement_complete"] is False
    assert rows[0]["settlement_reason"].startswith(
        "SETTLEMENT_EVIDENCE_GRADE_BELOW_REQUIRED"
    )


def test_normal_finalization_requires_fill_and_settlement_blocks() -> None:
    settlement = replace(
        _settlement(market_id=1, classification=SETTLED_YES),
        evidence={
            "evidence_grade": ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE
        },
    )
    position = replace(
        _position(market_id=1, position_id="p1"),
        first_fill_block=None,
        last_fill_block=None,
    )

    summary, rows = finalize_fill_only_positions(
        [position],
        {1: settlement},
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
        required_evidence_grade="oracle-finalized",
    )

    assert summary["status"] == "FINANCIAL_PARTIAL"
    assert rows[0]["settlement_reason"] == (
        "FILL_TO_SETTLEMENT_BLOCK_ORDERING_EVIDENCE_MISSING"
    )


def test_normal_finalization_uses_strict_block_order_with_second_level_tie() -> None:
    settlement = replace(
        _settlement(market_id=1, classification=SETTLED_YES),
        evidence={"evidence_grade": ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE},
    )
    position = replace(
        _position(market_id=1, position_id="p1"),
        first_fill_ts=FINALIZED,
        last_fill_ts=FINALIZED,
        first_fill_block=998,
        last_fill_block=999,
    )

    summary, rows = finalize_fill_only_positions(
        [position],
        {1: settlement},
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
        required_evidence_grade="oracle-finalized",
    )

    assert summary["status"] == "FINANCIAL_COMPLETE"
    assert rows[0]["settlement_complete"] is True


def test_research_finalization_accepts_exact_or_cutoff_block_evidence() -> None:
    exact = replace(
        _settlement(market_id=1, classification=SETTLED_YES),
        evidence={"evidence_grade": EXACT_CANONICAL_BLOCK_HEADER_GRADE},
    )
    central = replace(
        _settlement(market_id=2, classification=SETTLED_YES),
        evidence={
            "evidence_grade": ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK_GRADE
        },
    )

    summary, rows = finalize_fill_only_positions(
        [
            _position(market_id=1, position_id="p1"),
            _position(market_id=2, position_id="p2"),
        ],
        {1: exact, 2: central},
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
        required_evidence_grade="oracle-event-cutoff-block",
    )

    assert summary["status"] == "FINANCIAL_COMPLETE"
    assert summary["required_settlement_evidence"] == (
        "oracle-event-cutoff-block"
    )
    assert all(row["settlement_complete"] for row in rows)


def test_financial_bundle_contains_all_ledgers(tmp_path: Path) -> None:
    summary, rows = finalize_fill_only_positions(
        [_position(market_id=1, position_id="p1")],
        {1: _settlement(market_id=1, classification=SETTLED_YES)},
        execution_manifest_sha256="e" * 64,
        settlement_catalog_sha256="s" * 64,
        cutoff_ts=CUTOFF,
    )
    output = tmp_path / "financials"

    manifest = write_fill_only_financial_bundle(
        output_dir=output, summary=summary, positions=rows
    )

    assert manifest["schema_version"] == "fill_only_financial_bundle_manifest_v1"
    assert pq.read_table(output / "position_ledger.parquet").num_rows == 1
    cashflows = pq.read_table(output / "cashflow_ledger.parquet").to_pylist()
    assert {row["cashflow_type"] for row in cashflows} == {
        "ENTRY_NOTIONAL",
        "RECORDED_FEE",
        "SETTLEMENT_PAYOUT",
    }
    daily = pq.read_table(output / "daily_realized_equity.parquet").to_pylist()
    assert daily[0]["cumulative_net_pnl"] == "3.9"


def test_execution_position_manifest_round_trip(tmp_path: Path) -> None:
    output = tmp_path / "positions"
    position = _position(market_id=1, position_id="p1")

    manifest = write_fill_only_execution_positions(
        [position], output_dir=output, execution_source={"fixture": True}
    )
    loaded_manifest, loaded = load_fill_only_execution_positions(output)

    assert manifest == loaded_manifest
    assert loaded == (position,)
    inventory_manifest, market_ids = load_fill_only_execution_position_market_ids(
        output
    )
    assert inventory_manifest == manifest
    assert market_ids == (1,)
    assert tuple(iter_fill_only_execution_positions(output, batch_size=1)) == (
        position,
    )


def test_execution_position_write_is_atomic_on_validation_failure(
    tmp_path: Path,
) -> None:
    output = tmp_path / "positions"
    duplicate = _position(market_id=1, position_id="p1")

    with pytest.raises(
        FillOnlyFinancialError, match="duplicate execution position_id"
    ):
        write_fill_only_execution_positions(
            [duplicate, duplicate],
            output_dir=output,
            execution_source={"fixture": True},
        )

    assert not output.exists()
    assert not tuple(tmp_path.glob(".positions.*"))
