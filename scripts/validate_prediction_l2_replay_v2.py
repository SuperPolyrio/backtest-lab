#!/usr/bin/env python3
"""Validate Prediction L2 Replay V2 on synthetic and frozen real L2 data."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.pml2.adapters import _xue_rows_to_pml2_events
from quant.backtest.pml2.contracts import (
    BookFrameBatchEvent,
    BookLevelBatchEvent,
    BookSnapshotEvent,
    Outcome,
    TradeEvent,
)
from quant.backtest.pml2.profiles import get_pml2_profile
from quant.backtest.pml2.service_v2 import (
    build_prediction_l2_v2_readiness,
    list_prediction_l2_v2_profiles,
    run_prediction_l2_v2_execution_matrix,
    run_prediction_l2_v2_gap_forecast,
)

UTC = timezone.utc
DEFAULT_COHORT = (
    PROJECT_ROOT / "runtime_outputs" / "real_execution_model_comparison" / "latest.json"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT / "runtime_outputs" / "prediction_l2_v2_validation" / "latest.json"
)
FOCUSED_TESTS = (
    "quant/backtest/tests/test_prediction_l2_replay_v1.py",
    "quant/backtest/tests/test_prediction_l2_replay_v2.py",
)


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _run_focused_tests() -> dict[str, Any]:
    command = [sys.executable, "-m", "pytest", "-q", *FOCUSED_TESTS]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    output = "\n".join(
        item.strip() for item in (completed.stdout, completed.stderr) if item.strip()
    )
    if completed.returncode:
        raise RuntimeError(f"focused PML2 tests failed:\n{output}")
    return {
        "status": "PASS",
        "command": command,
        "output": output,
    }


EventClocks = tuple[datetime, datetime, datetime]


def _load_legacy_dynamic_events(
    archive: Path,
    *,
    condition_id: str,
    market_id: str,
    asset_id: str,
) -> tuple[list[dict[str, Any]], dict[str, EventClocks]]:
    try:
        import pyarrow.compute as pc
        import pyarrow.dataset as ds
    except ImportError as exc:
        raise RuntimeError(
            "real cohort validation requires pyarrow in the active environment"
        ) from exc

    dataset = ds.dataset(str(archive), format="parquet", partitioning="hive")
    rows = dataset.to_table(filter=pc.field("asset_id") == asset_id).to_pandas()
    if rows.empty:
        raise RuntimeError(f"no archive rows found for asset_id={asset_id}")
    converted = _xue_rows_to_pml2_events(
        rows,
        condition_id=condition_id,
        market_id=market_id,
        outcomes={asset_id: Outcome.NO},
        book_epoch=0,
        feed_latency_ms=0,
        require_complete_top_hints=False,
    )
    payload: list[dict[str, Any]] = []
    event_clocks: dict[str, EventClocks] = {}
    for event in converted:
        if isinstance(event, BookFrameBatchEvent):
            for batch in event.batches:
                payload.append(_level_batch_payload(batch))
                _record_batch_clocks(event_clocks, batch)
        elif isinstance(event, BookLevelBatchEvent):
            payload.append(_level_batch_payload(event))
            _record_batch_clocks(event_clocks, event)
        elif isinstance(event, BookSnapshotEvent):
            payload.append(_snapshot_payload(event))
            event_clocks[event.snapshot_id] = _event_clocks(event)
        elif isinstance(event, TradeEvent):
            payload.append(_trade_payload(event))
            event_clocks[event.event_id] = _event_clocks(event)
    if not any(row["type"] == "SNAPSHOT" for row in payload):
        raise RuntimeError("dynamic archive cohort has no full L2 baseline")
    return payload, event_clocks


def _event_common(event: Any) -> dict[str, Any]:
    return {
        "conditionId": event.condition_id,
        "marketId": event.market_id,
        "assetId": event.asset_id,
        "outcome": event.outcome.value,
        "exchangeTs": event.exchange_ts.isoformat(),
        "sourceReceivedTs": (
            event.source_received_ts.isoformat()
            if event.source_received_ts is not None
            else None
        ),
        "localTs": event.local_ts.isoformat(),
        "bookEpoch": event.book_epoch,
        "source": event.source,
    }


def _snapshot_payload(event: BookSnapshotEvent) -> dict[str, Any]:
    return {
        "type": "SNAPSHOT",
        "eventId": event.snapshot_id,
        "snapshotId": event.snapshot_id,
        **_event_common(event),
        "sequence": event.sequence,
        "bids": [
            {"price": format(level.price, "f"), "size": format(level.size, "f")}
            for level in event.bids
        ],
        "asks": [
            {"price": format(level.price, "f"), "size": format(level.size, "f")}
            for level in event.asks
        ],
        "isFullDepth": event.is_full_depth,
        "isTruncated": event.is_truncated,
        "depthScope": event.depth_scope,
        "bookHash": event.book_hash,
    }


def _level_batch_payload(event: BookLevelBatchEvent) -> dict[str, Any]:
    return {
        "type": "LEVEL_BATCH",
        "eventId": event.event_id,
        **_event_common(event),
        "sequence": event.source_sequence,
        "updates": [
            {
                "eventId": update.event_id,
                "sequence": update.sequence,
                "side": update.side.value,
                "price": format(update.price, "f"),
                "newSize": format(update.new_size, "f"),
                "linkedTradeEventIds": list(update.linked_trade_event_ids),
            }
            for update in event.updates
        ],
    }


def _event_clocks(event: Any) -> EventClocks:
    return (
        event.exchange_ts,
        event.source_received_ts or event.exchange_ts,
        event.local_ts,
    )


def _record_batch_clocks(
    clocks: dict[str, EventClocks], event: BookLevelBatchEvent
) -> None:
    clocks[event.event_id] = _event_clocks(event)
    for update in event.updates:
        clocks[update.event_id] = _event_clocks(update)


def _trade_payload(event: TradeEvent) -> dict[str, Any]:
    return {
        "type": "TRADE",
        "eventId": event.event_id,
        **_event_common(event),
        "sequence": event.source_sequence,
        "price": format(event.price, "f"),
        "size": format(event.size, "f"),
        "aggressorSide": event.aggressor_side.value,
        "eventGroupId": event.event_group_id,
        "evidenceLinkId": event.evidence_link_id,
        "evidenceKind": event.evidence_kind,
        "sourceEventIds": list(event.source_event_ids),
    }


def _cohort_request(
    report: Mapping[str, Any], events: list[dict[str, Any]]
) -> dict[str, Any]:
    market = report["market"]
    orders = []
    for row in report["orders"]:
        order = row["order"]
        orders.append(
            {
                "orderId": row["order_id"],
                "strategyId": "frozen-real-l2-v2-validation",
                "conditionId": market["condition_id"],
                "marketId": str(market["id"]),
                "assetId": str(market["no_token_id"]),
                "outcome": "NO",
                "side": "BUY",
                "size": order["size"],
                "limitPrice": order["limit_price"],
                "tif": "FAK",
                "signalTs": order["signal_ts"],
                "observedTs": order["signal_ts"],
                "submitTs": order["signal_ts"],
                "entryLatencyMs": int(order["latency_seconds"]) * 1000,
                "venueDelayMs": 0,
                "responseLatencyMs": 0,
                "metadata": {
                    "category": market.get("category") or "GLOBAL",
                    "tte_bucket": "UNKNOWN",
                    "liquidity_regime": "UNKNOWN",
                    "maker_survival_horizon_seconds": int(order["horizon_seconds"]),
                },
            }
        )
    return {
        "runId": "pml2-v2-frozen-real-cohort",
        "profile": "realistic",
        "events": events,
        "orders": orders,
        "contractValidation": {
            "mode": "RESEARCH",
            "identityMappings": [
                {
                    "conditionId": market["condition_id"],
                    "marketId": str(market["id"]),
                    "yesAssetId": str(market["yes_token_id"]),
                    "noAssetId": str(market["no_token_id"]),
                }
            ],
            "requireBinaryPair": False,
            "allowResearchZeroFee": True,
        },
    }


def _validate_matrix(
    matrix: Mapping[str, Any],
    event_clocks: Mapping[str, EventClocks],
) -> dict[str, Any]:
    failures: list[str] = []
    details: dict[str, Any] = {}
    for variant, result in matrix["results"].items():
        orders = {row["order"]["order_id"]: row for row in result.get("orders", [])}
        matches = list(result.get("execution_matches", []))
        estimates = list(result.get("modeled_fill_estimates", []))
        profile_max_age = int(result["profile"]["max_book_age_ms"])
        validity_mode = str(
            result["profile"].get("book_validity_mode") or "MAX_AGE"
        )
        unknown_source_ids: set[str] = set()
        age_over_legacy_ttl_count = 0
        limit_violation_count = 0
        for match in matches:
            source_ids = list(match.get("source_event_ids") or [])
            if not source_ids:
                failures.append(f"{variant}:{match['fill_id']}:missing_source_event")
            known = [
                event_clocks[item][1] for item in source_ids if item in event_clocks
            ]
            unknown_source_ids.update(
                item
                for item in source_ids
                if item not in event_clocks and not item.startswith("l2-coverage-")
            )
            fill_ts = _datetime(match["fill_exchange_ts"])
            if (
                known
                and (fill_ts - max(known)).total_seconds() * 1000 > profile_max_age
            ):
                age_over_legacy_ttl_count += 1
            order = orders[match["order_id"]]["order"]
            fill_price = Decimal(str(match["raw_price"]))
            limit = Decimal(str(order["limit_price"]))
            side = str(order["side"])
            if (side == "BUY" and fill_price > limit) or (
                side == "SELL" and fill_price < limit
            ):
                limit_violation_count += 1
        if validity_mode == "MAX_AGE" and age_over_legacy_ttl_count:
            failures.append(
                f"{variant}:stale_observed_matches={age_over_legacy_ttl_count}"
            )
        if limit_violation_count:
            failures.append(f"{variant}:limit_violations={limit_violation_count}")
        fill_ids = [str(item["fill_id"]) for item in matches]
        if len(fill_ids) != len(set(fill_ids)):
            failures.append(f"{variant}:duplicate_fill_ids")
        if any(item.get("evidence_tier") != "OBSERVED_L2" for item in matches):
            failures.append(f"{variant}:observed_evidence_tier_violation")
        if any(item.get("evidence_tier") == "OBSERVED_L2" for item in estimates):
            failures.append(f"{variant}:modeled_estimate_promoted_to_observed")
        if variant == "strict_fok":
            for row in orders.values():
                requested = Decimal(str(row["order"]["size"]))
                filled = Decimal(str(row["filled_size"]))
                if filled not in {Decimal(0), requested}:
                    failures.append(
                        f"{variant}:{row['order']['order_id']}:non_atomic_fok"
                    )
        if "_fak" in variant:
            forbidden = {"WORKING", "PENDING", "WAITING_FOR_DATA"}
            if forbidden & set(result["status_counts"]):
                failures.append(f"{variant}:fak_left_working_order")
        for audit in result.get("submission_audit", []):
            source_id = audit.get("release_source_event_id")
            effective = audit.get("effective_submit_ts")
            if (
                source_id
                and effective
                and source_id in event_clocks
                and _datetime(effective) < event_clocks[source_id][2]
            ):
                failures.append(
                    f"{variant}:{audit['order_id']}:released_before_local_delivery"
                )
            if int(audit.get("release_count") or 0) > 1:
                failures.append(
                    f"{variant}:{audit['order_id']}:released_more_than_once"
                )
        details[variant] = {
            "observed_match_count": len(matches),
            "modeled_estimate_count": len(estimates),
            "book_validity_mode": validity_mode,
            "age_over_legacy_ttl_count": age_over_legacy_ttl_count,
            "limit_violation_count": limit_violation_count,
            "unknown_source_event_ids": sorted(unknown_source_ids),
            "status_counts": dict(result["status_counts"]),
        }
    if failures:
        raise RuntimeError("PML2 V2 matrix invariants failed: " + "; ".join(failures))
    return {"status": "PASS", "variants": details}


def _fill_with_source_clocks(
    fill: Mapping[str, Any],
    event_clocks: Mapping[str, EventClocks],
) -> dict[str, Any]:
    source_ids = list(fill.get("source_event_ids") or [])
    source_clocks = {
        source_id: {
            "exchange_ts": event_clocks[source_id][0],
            "source_received_ts": event_clocks[source_id][1],
            "local_ts": event_clocks[source_id][2],
        }
        for source_id in source_ids
        if source_id in event_clocks
    }
    fill_ts = _datetime(fill["fill_exchange_ts"])
    matched_receive_times = [
        event_clocks[source_id][1]
        for source_id in source_ids
        if source_id in event_clocks
    ]
    return {
        **dict(fill),
        "source_event_clocks": source_clocks,
        "evidence_age_ms": (
            max(
                0,
                int((fill_ts - max(matched_receive_times)).total_seconds() * 1000),
            )
            if matched_receive_times
            else None
        ),
        "archive_event_clock_count": len(event_clocks),
    }


def _waterfall(matrix: Mapping[str, Any]) -> list[dict[str, Any]]:
    comparison = matrix["comparison"]
    observed = {
        key: Decimal(str(value["observed_filled_size"]))
        for key, value in comparison.items()
    }
    modeled = {
        key: Decimal(str(value["modeled_expected_size"]))
        for key, value in comparison.items()
    }
    rows = [
        {
            "step": "strict_fok",
            "basis": "OBSERVED_L2",
            "incremental_size": observed.get("strict_fok", Decimal(0)),
            "cumulative_observed_size": observed.get("strict_fok", Decimal(0)),
        },
        {
            "step": "fak_partial_control",
            "basis": "OBSERVED_L2",
            "incremental_size": observed.get("strict_fak_control", Decimal(0))
            - observed.get("strict_fok", Decimal(0)),
            "cumulative_observed_size": observed.get("strict_fak_control", Decimal(0)),
        },
        {
            "step": "realistic_profile",
            "basis": "OBSERVED_L2",
            "incremental_size": observed.get("realistic_fak", Decimal(0))
            - observed.get("strict_fak_control", Decimal(0)),
            "cumulative_observed_size": observed.get("realistic_fak", Decimal(0)),
        },
        {
            "step": "wait_for_fresh_30s",
            "basis": "OBSERVED_L2",
            "incremental_size": observed.get("wait30_fak", Decimal(0))
            - observed.get("realistic_fak", Decimal(0)),
            "cumulative_observed_size": observed.get("wait30_fak", Decimal(0)),
        },
        {
            "step": "gtd_maker_queue",
            "basis": "OBSERVED_L2",
            "incremental_size": observed.get("wait30_gtd30", Decimal(0))
            - observed.get("wait30_fak", Decimal(0)),
            "cumulative_observed_size": observed.get("wait30_gtd30", Decimal(0)),
        },
        {
            "step": "maker_expected_overlay",
            "basis": "MODELED_EXPECTED",
            "incremental_size": modeled.get("wait30_gtd30_maker_expected", Decimal(0)),
            "cumulative_observed_size": observed.get(
                "wait30_gtd30_maker_expected", Decimal(0)
            ),
        },
        {
            "step": "gap_expected_overlay",
            "basis": "MODELED_EXPECTED",
            "incremental_size": modeled.get("wait30_fak_gap_expected", Decimal(0)),
            "cumulative_observed_size": observed.get(
                "wait30_fak_gap_expected", Decimal(0)
            ),
        },
    ]
    return rows


def _run_real_cohort(path: Path) -> dict[str, Any]:
    report = json.loads(path.read_text(encoding="utf-8"))
    market = report["market"]
    archive = Path(report["data"]["l2_archive"])
    events, event_clocks = _load_legacy_dynamic_events(
        archive,
        condition_id=str(market["condition_id"]),
        market_id=str(market["id"]),
        asset_id=str(market["no_token_id"]),
    )
    request = _cohort_request(report, events)
    matrix = run_prediction_l2_v2_execution_matrix(request)
    validation = _validate_matrix(matrix, event_clocks)
    per_order = {}
    for variant, result in matrix["results"].items():
        audits = {row["order_id"]: row for row in result.get("submission_audit", [])}
        estimates = {
            row["order_id"]: row for row in result.get("modeled_fill_estimates", [])
        }
        per_order[variant] = [
            {
                "order_id": row["order"]["order_id"],
                "status": row["status"],
                "reason": row["reason"],
                "filled_size": row["filled_size"],
                "avg_fill_price": row["avg_fill_price"],
                "fills": [
                    _fill_with_source_clocks(fill, event_clocks)
                    for fill in row["fills"]
                ],
                "submission_audit": audits.get(row["order"]["order_id"]),
                "modeled_estimate": estimates.get(row["order"]["order_id"]),
            }
            for row in result.get("orders", [])
        ]
    return {
        "status": "PASS",
        "cohort_source": path,
        "archive": archive,
        "market": {
            "id": market["id"],
            "title": market["title"],
            "condition_id": market["condition_id"],
            "asset_id": market["no_token_id"],
        },
        "order_count": len(request["orders"]),
        "dynamic_event_count": len(events),
        "event_types": dict(Counter(row["type"] for row in events)),
        "archive_contract": (
            "Legacy local archive is treated as the frozen cohort source. "
            "No hot/cold hash reconciliation or future snapshot fallback is used."
        ),
        "matrix_validation": validation,
        "comparison": matrix["comparison"],
        "waterfall": _waterfall(matrix),
        "per_order": per_order,
    }


def _datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def run(*, cohort: Path, skip_tests: bool, skip_real_cohort: bool) -> dict[str, Any]:
    profiles = list_prediction_l2_v2_profiles()
    readiness = build_prediction_l2_v2_readiness()
    if not readiness["ready_for_observed_replay"]:
        raise RuntimeError("PML2 V2 observed replay readiness is false")
    gap_216s = run_prediction_l2_v2_gap_forecast(
        {
            "decisionTs": "2026-08-28T00:00:00+00:00",
            "gapMs": 216_000,
            "requestedSize": "10",
            "price": "0.979",
        }
    )
    if not gap_216s["forecast"]["domain_status"].startswith("ABSTAIN"):
        raise RuntimeError("216 second stale book was not rejected by gap model")
    result: dict[str, Any] = {
        "schema_version": "prediction-l2-replay-v2-validation-v1",
        "generated_at": datetime.now(tz=UTC),
        "status": "PASS",
        "profiles": profiles,
        "readiness": readiness,
        "hard_boundary_checks": {
            "stale_216s_gap": gap_216s["forecast"],
            "event_driven_book_validity": True,
            "known_transport_gap_requires_later_snapshot": True,
            "realistic_max_book_age_ms": get_pml2_profile("realistic").max_book_age_ms,
            "modeled_estimates_separate_from_execution_matches": True,
        },
    }
    if not skip_tests:
        result["focused_tests"] = _run_focused_tests()
    if not skip_real_cohort:
        result["real_frozen_cohort"] = _run_real_cohort(cohort)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", type=Path, default=DEFAULT_COHORT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--skip-real-cohort", action="store_true")
    args = parser.parse_args()
    report = run(
        cohort=args.cohort.resolve(),
        skip_tests=args.skip_tests,
        skip_real_cohort=args.skip_real_cohort,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(_json_ready(report), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            _json_ready(
                {
                    "status": report["status"],
                    "output": args.output.resolve(),
                    "focused_tests": report.get("focused_tests"),
                    "real_frozen_cohort": (
                        {
                            "market": report["real_frozen_cohort"]["market"],
                            "comparison": report["real_frozen_cohort"]["comparison"],
                            "waterfall": report["real_frozen_cohort"]["waterfall"],
                        }
                        if "real_frozen_cohort" in report
                        else None
                    ),
                }
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
