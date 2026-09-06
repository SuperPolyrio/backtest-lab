from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from quant.backtest import market_settlement
from quant.backtest.market_settlement import (
    CANCELLED_REFUND,
    IDENTITY_CONFLICT,
    ORACLE_EVENT_CUTOFF_BLOCK,
    ORACLE_FINALIZED_REQUIRED,
    ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE,
    POST_CUTOFF,
    SETTLED_NO,
    SETTLED_YES,
    TECHNICAL_MISSING,
    UNRESOLVED,
    classify_market_settlement,
    finalize_partitioned_market_settlement_catalog,
    initialize_partitioned_market_settlement_catalog,
    load_market_settlement_catalog,
    write_market_settlement_catalog,
    write_market_settlement_catalog_part,
)

UTC = timezone.utc
CUTOFF = datetime(2026, 8, 25, 13, 14, 50, tzinfo=UTC)
BLOCK_TIME = CUTOFF - timedelta(days=1)
CONDITION = "0x" + "a" * 64
YES_TOKEN = "1" * 20
NO_TOKEN = "2" * 20
TX_HASH = "0x" + "b" * 64
BLOCK_HASH = "0x" + "c" * 64


def _row(*, code: int = 1, completion_status: str = "SETTLED") -> dict[str, object]:
    outcome = {1: "YES", 2: "NO", 3: "CANCELLED"}[code]
    return {
        "requested_market_id": 7,
        "market_found": True,
        "status_found": True,
        "oracle_event_found": True,
        "market_id": 7,
        "condition_id": CONDITION,
        "slug": "fixture-market",
        "title": "Fixture market?",
        "category": "fixture",
        "yes_token_id": YES_TOKEN,
        "no_token_id": NO_TOKEN,
        "tokens": [
            {
                "outcome": "YES",
                "outcome_index": 0,
                "token_id": YES_TOKEN,
                "condition_id": CONDITION,
            },
            {
                "outcome": "NO",
                "outcome_index": 1,
                "token_id": NO_TOKEN,
                "condition_id": CONDITION,
            },
        ],
        "has_settle": True,
        "is_resolved": True,
        "is_final": True,
        "completion_status": completion_status,
        "settlement_code": code,
        "settlement_outcome": outcome,
        "settlement_source": "oracle_settled_price",
        "settlement_event_id": 99,
        "settlement_event_time": BLOCK_TIME,
        "settlement_transaction": TX_HASH,
        "registry_updated_at": BLOCK_TIME + timedelta(seconds=1),
        "settlement_block_number": 1000,
        "settlement_tx_hash": TX_HASH,
        "source_event_time": BLOCK_TIME,
        "source_event_status": "settle",
        "source_condition_id": CONDITION,
        "settled_price": "0.5" if code == 3 else str(code == 1 and 1 or 0),
        "source_adapter": "0x" + "d" * 40,
        "source_oracle": "0x" + "e" * 40,
    }


def _headers(*, block_time: datetime = BLOCK_TIME) -> list[dict[str, object]]:
    return [
        {
            "block_number": 1000,
            "block_hash": BLOCK_HASH,
            "block_time": block_time,
            "source": "rpc_fixture",
        }
    ]


def _credible_time_rows(
    *, block_time: datetime = BLOCK_TIME
) -> list[dict[str, object]]:
    return [
        {
            "block_number": 1000,
            "block_hash": None,
            "block_time": block_time,
            "source": "trade_time:pg_api_trades_tx_path",
        }
    ]


def _boundary() -> dict[str, object]:
    value = {
        "schema_version": "polygon_cutoff_block_boundary_v1",
        "cutoff_ts": CUTOFF,
        "at_or_before": {
            "block_number": 1000,
            "block_hash": BLOCK_HASH,
            "block_time": CUTOFF - timedelta(seconds=1),
            "sources": ("polygon_rpc_json_rpc",),
            "header_rows_sha256": "1" * 64,
        },
        "after": {
            "block_number": 1001,
            "block_hash": "0x" + "f" * 64,
            "block_time": CUTOFF + timedelta(seconds=1),
            "sources": ("polygon_rpc_json_rpc",),
            "header_rows_sha256": "2" * 64,
        },
    }
    return {
        **value,
        "boundary_sha256": market_settlement._canonical_sha256(value),
    }


@pytest.mark.parametrize(
    ("code", "expected", "yes_payout", "no_payout"),
    [
        (1, SETTLED_YES, Decimal(1), Decimal(0)),
        (2, SETTLED_NO, Decimal(0), Decimal(1)),
        (3, CANCELLED_REFUND, Decimal("0.5"), Decimal("0.5")),
    ],
)
def test_exact_protocol_payout_vectors(
    code: int, expected: str, yes_payout: Decimal, no_payout: Decimal
) -> None:
    record = classify_market_settlement(
        _row(code=code, completion_status="CANCELLED" if code == 3 else "SETTLED"),
        cutoff_ts=CUTOFF,
        trusted_header_rows=_headers(),
    )

    assert record.classification == expected
    assert record.payable is True
    assert record.payout_by_token == {YES_TOKEN: yes_payout, NO_TOKEN: no_payout}


def test_post_cutoff_settlement_is_not_payable() -> None:
    after = CUTOFF + timedelta(seconds=1)
    row = _row()
    row["settlement_event_time"] = after
    row["source_event_time"] = after

    record = classify_market_settlement(
        row,
        cutoff_ts=CUTOFF,
        trusted_header_rows=_headers(block_time=after),
    )

    assert record.classification == POST_CUTOFF
    assert record.payout_by_token is None


def test_oracle_event_policy_uses_one_canonical_cutoff_boundary() -> None:
    record = classify_market_settlement(
        _row(),
        cutoff_ts=CUTOFF,
        evidence_policy=ORACLE_EVENT_CUTOFF_BLOCK,
        cutoff_block_boundary=_boundary(),
    )

    assert record.classification == SETTLED_YES
    assert record.evidence["evidence_grade"] == (
        "ORACLE_EVENT_WITH_CANONICAL_CUTOFF_BLOCK"
    )


def test_oracle_event_policy_supports_block_only_ordering() -> None:
    row = _row()
    row["settlement_event_time"] = None
    row["source_event_time"] = None

    record = classify_market_settlement(
        row,
        cutoff_ts=CUTOFF,
        evidence_policy=ORACLE_EVENT_CUTOFF_BLOCK,
        cutoff_block_boundary=_boundary(),
    )

    assert record.classification == SETTLED_YES
    assert record.protocol_finalized_at is None
    assert record.protocol_finalized_block == 1000


def test_normal_oracle_policy_accepts_final_event_with_credible_time() -> None:
    record = classify_market_settlement(
        _row(),
        cutoff_ts=CUTOFF,
        evidence_policy=ORACLE_FINALIZED_REQUIRED,
        cutoff_block_boundary=_boundary(),
    )

    assert record.classification == SETTLED_YES
    assert record.protocol_finalized_at == BLOCK_TIME
    assert record.evidence["evidence_grade"] == (
        ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE
    )
    assert record.evidence["settlement_time_source"] == (
        "oracle.oracle_events.event_time"
    )


def test_normal_oracle_policy_recovers_credible_time_from_trade_block() -> None:
    row = _row()
    row["settlement_event_time"] = None
    row["source_event_time"] = None

    record = classify_market_settlement(
        row,
        cutoff_ts=CUTOFF,
        trusted_header_rows=_credible_time_rows(),
        evidence_policy=ORACLE_FINALIZED_REQUIRED,
        cutoff_block_boundary=_boundary(),
    )

    assert record.classification == SETTLED_YES
    assert record.protocol_finalized_at == BLOCK_TIME
    assert record.evidence["evidence_grade"] == (
        ORACLE_FINALIZED_WITH_CREDIBLE_TIME_GRADE
    )
    assert record.evidence["settlement_time_source"] == "block_timestamps"
    assert record.evidence["credible_time_sources"] == (
        "trade_time:pg_api_trades_tx_path",
    )


def test_normal_oracle_policy_rejects_missing_credible_time() -> None:
    row = _row()
    row["settlement_event_time"] = None
    row["source_event_time"] = None

    record = classify_market_settlement(
        row,
        cutoff_ts=CUTOFF,
        evidence_policy=ORACLE_FINALIZED_REQUIRED,
        cutoff_block_boundary=_boundary(),
    )

    assert record.classification == TECHNICAL_MISSING
    assert record.classification_reason == (
        "CREDIBLE_SETTLEMENT_TIME_MISSING_OR_CONFLICTING"
    )
    assert record.payout_by_token is None


def test_unresolved_is_distinct_from_missing_strict_evidence() -> None:
    unresolved = _row()
    unresolved.update(
        {
            "has_settle": False,
            "is_resolved": False,
            "is_final": False,
            "completion_status": "ENDED_AWAITING_ORACLE",
            "settlement_code": None,
            "settlement_outcome": None,
        }
    )
    unresolved_record = classify_market_settlement(unresolved, cutoff_ts=CUTOFF)

    missing = _row()
    missing.update(
        {
            "oracle_event_found": False,
            "settlement_event_id": None,
            "settlement_block_number": None,
        }
    )
    missing_record = classify_market_settlement(missing, cutoff_ts=CUTOFF)

    assert unresolved_record.classification == UNRESOLVED
    assert missing_record.classification == TECHNICAL_MISSING


def test_token_mapping_conflict_fails_closed() -> None:
    row = _row()
    row["yes_token_id"] = "999"

    record = classify_market_settlement(
        row, cutoff_ts=CUTOFF, trusted_header_rows=_headers()
    )

    assert record.classification == IDENTITY_CONFLICT
    assert record.payout_by_token is None


def test_catalog_round_trip_revalidates_hashes(tmp_path: Path) -> None:
    records = [
        classify_market_settlement(
            _row(code=1), cutoff_ts=CUTOFF, trusted_header_rows=_headers()
        )
    ]
    output = tmp_path / "catalog"

    manifest = write_market_settlement_catalog(
        records,
        output_dir=output,
        cutoff_ts=CUTOFF,
        source_scope={"market_inventory": "fixture"},
    )
    loaded = load_market_settlement_catalog(output)

    assert manifest["record_count"] == 1
    assert loaded[7].record_sha256 == records[0].record_sha256


def test_partitioned_catalog_resumes_and_loads_selected_market(tmp_path: Path) -> None:
    row_7 = _row(code=1)
    row_8 = _row(code=2)
    row_8["requested_market_id"] = 8
    row_8["market_id"] = 8
    records = [
        classify_market_settlement(
            row_7, cutoff_ts=CUTOFF, trusted_header_rows=_headers()
        ),
        classify_market_settlement(
            row_8, cutoff_ts=CUTOFF, trusted_header_rows=_headers()
        ),
    ]
    output = tmp_path / "partitioned"
    contract = initialize_partitioned_market_settlement_catalog(
        output_dir=output,
        cutoff_ts=CUTOFF,
        source_scope={"market_inventory": "fixture", "market_count": 2},
        partition_size=1,
        resume=False,
    )
    for index, record in enumerate(records):
        write_market_settlement_catalog_part(
            [record],
            output_dir=output,
            part_index=index,
            expected_market_ids=[record.market_id],
            build_contract_sha256=str(contract["build_contract_sha256"]),
            resume=False,
        )
    manifest = finalize_partitioned_market_settlement_catalog(
        output_dir=output,
        expected_part_count=2,
        expected_record_count=2,
    )

    resumed = write_market_settlement_catalog_part(
        (),
        output_dir=output,
        part_index=1,
        expected_market_ids=[8],
        build_contract_sha256=str(contract["build_contract_sha256"]),
        resume=True,
    )
    selected = load_market_settlement_catalog(
        output, market_ids=[8], verify_all_files=False
    )

    assert manifest["record_count"] == 2
    assert resumed["record_count"] == 1
    assert set(selected) == {8}
    assert selected[8].classification == SETTLED_NO
