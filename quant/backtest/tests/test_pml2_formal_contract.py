from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from quant.backtest.pml2.service import (
    Pml2DataNotReadyError,
    Pml2RequestError,
    run_pml2_replay,
)

T0 = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def _formal_payload() -> dict[str, object]:
    order_ts = T0 + timedelta(seconds=1)
    return {
        "run_id": "formal-contract",
        "profile": "optimistic",
        "contract_validation": {
            "mode": "FORMAL",
            "identity_mappings": [
                {
                    "condition_id": "condition-1",
                    "market_id": "market-1",
                    "yes_asset_id": "yes-token",
                    "no_asset_id": "no-token",
                }
            ],
            "require_binary_pair": True,
            # This source uses a per-token contiguous sequence.  Sources with
            # collector-wide sequences must omit this declaration.
            "sequence_step_by_source": {"native_l2": 1},
        },
        "events": [
            {
                "type": "SNAPSHOT",
                "snapshot_id": "yes-snapshot",
                "condition_id": "condition-1",
                "market_id": "market-1",
                "asset_id": "yes-token",
                "outcome": "YES",
                "exchange_ts": T0.isoformat(),
                "local_ts": T0.isoformat(),
                "source": "native_l2",
                "sequence": 1,
                "bids": [{"price": "0.49", "size": "10"}],
                "asks": [{"price": "0.50", "size": "3"}],
            },
            {
                "type": "SNAPSHOT",
                "snapshot_id": "no-snapshot",
                "condition_id": "condition-1",
                "market_id": "market-1",
                "asset_id": "no-token",
                "outcome": "NO",
                "exchange_ts": T0.isoformat(),
                "local_ts": T0.isoformat(),
                "source": "native_l2",
                "sequence": 2,
                "bids": [{"price": "0.50", "size": "3"}],
                "asks": [{"price": "0.51", "size": "10"}],
            },
        ],
        "fee_schedules": [
            {
                "schedule_id": "formal-fee-v1",
                "asset_id": "yes-token",
                "condition_id": "condition-1",
                "effective_from": (T0 - timedelta(days=1)).isoformat(),
                "platform_fee_rate": "0",
                "source": "frozen-historical-fee-table",
            }
        ],
        "orders": [
            {
                "order_id": "formal-ioc",
                "strategy_id": "pmq071",
                "condition_id": "condition-1",
                "market_id": "market-1",
                "asset_id": "yes-token",
                "outcome": "YES",
                "side": "BUY",
                "size": "5",
                "limit_price": "0.50",
                "tif": "IOC",
                "signal_ts": order_ts.isoformat(),
                "entry_latency_ms": 0,
                "response_latency_ms": 0,
                "venue_delay_ms": 0,
            }
        ],
    }


def test_formal_ioc_keeps_label_and_cancels_partial_remainder() -> None:
    result = run_pml2_replay(_formal_payload())

    order = result["orders"][0]
    assert order["order"]["tif"] == "IOC"
    assert order["status"] == "PARTIAL"
    assert order["filled_size"] == "3.0000000000"
    assert order["remaining_size"] == "2.0000000000"
    assert order["reason"] == "ioc_remainder_cancelled"
    assert result["contract_coverage"]["status"] == "VALID"
    assert result["contract_coverage"]["binary_pair"]["status"] == "COMPLETE"
    assert result["contract_coverage"]["fee_policy"] == (
        "EFFECTIVE_DATED_SCHEDULE_REQUIRED"
    )


def test_formal_wrong_market_identity_fails_closed() -> None:
    payload = _formal_payload()
    payload["orders"][0]["market_id"] = "wrong-market"

    with pytest.raises(Pml2RequestError, match="market_id does not match"):
        run_pml2_replay(payload)


def test_formal_wrong_outcome_token_identity_fails_closed() -> None:
    payload = _formal_payload()
    payload["orders"][0]["asset_id"] = "no-token"

    with pytest.raises(Pml2RequestError, match="YES token mapping"):
        run_pml2_replay(payload)


def test_formal_missing_binary_book_returns_structured_coverage_reason() -> None:
    payload = _formal_payload()
    payload["events"] = payload["events"][:1]

    with pytest.raises(Pml2DataNotReadyError) as caught:
        run_pml2_replay(payload)

    error = caught.value.as_dict()
    coverage = error["details"]["contract_coverage"]
    assert coverage["status"] == "INCOMPLETE"
    assert coverage["conditions"][0]["missing_snapshot_outcomes"] == ["NO"]
    assert "BINARY_PAIR_INCOMPLETE" in coverage["reasons"][0]


def test_formal_binary_snapshot_after_order_arrival_is_not_pit_coverage() -> None:
    payload = _formal_payload()
    too_late = T0 + timedelta(seconds=2)
    payload["events"][1]["exchange_ts"] = too_late.isoformat()
    payload["events"][1]["local_ts"] = too_late.isoformat()

    with pytest.raises(Pml2DataNotReadyError) as caught:
        run_pml2_replay(payload)

    condition = caught.value.as_dict()["details"]["contract_coverage"][
        "conditions"
    ][0]
    assert condition["snapshot_outcomes_before_arrival"] == ["YES"]
    assert condition["missing_snapshot_outcomes"] == ["NO"]


def test_formal_request_cannot_fall_back_to_silent_zero_fee() -> None:
    payload = _formal_payload()
    payload["fee_schedules"] = []

    with pytest.raises(Pml2RequestError, match="effective-dated fee_schedules"):
        run_pml2_replay(payload)


def test_explicit_research_zero_fee_requires_opt_in() -> None:
    payload = _formal_payload()
    payload["contract_validation"] = {
        "mode": "RESEARCH",
        "identity_mappings": payload["contract_validation"]["identity_mappings"],
        "require_binary_pair": False,
    }
    payload["fee_schedules"] = []

    with pytest.raises(Pml2RequestError, match="allow_research_zero_fee=true"):
        run_pml2_replay(payload)

    opted_in = deepcopy(payload)
    opted_in["contract_validation"]["allow_research_zero_fee"] = True
    result = run_pml2_replay(opted_in)
    assert result["contract_coverage"]["fee_policy"] == (
        "EXPLICIT_RESEARCH_ZERO_FEE"
    )
    assert "RESEARCH_ZERO_FEE_EXPLICIT" in result["contract_coverage"]["reasons"]


def test_legacy_research_request_without_mapping_remains_compatible() -> None:
    payload = _formal_payload()
    payload.pop("contract_validation")
    payload["events"] = payload["events"][:1]
    payload["fee_schedules"] = []

    result = run_pml2_replay(payload)

    assert result["orders"][0]["status"] == "PARTIAL"
    assert result["contract_coverage"]["status"] == "RESEARCH_UNVERIFIED"
    assert "LEGACY_RESEARCH_REQUEST_NO_EXPLICIT_VALIDATION" in result[
        "contract_coverage"
    ]["reasons"]
