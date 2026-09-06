from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json

from quant.backtest.trade_only_v3.models import (
    ExecutionMode,
    get_trade_only_profile,
)
from quant.calibration.v3_live_calibration import (
    build_pml2_live_replay_payload,
    build_v3_arrival_replay_payload,
    evaluate_v3_live_predictions,
    load_paper_live_samples,
    normalize_paper_live_record,
)
from quant.backtest.pml2.service import run_pml2_replay


def _record(*, actual: str = "FULL", tif: str = "FOK") -> dict:
    filled = "10" if actual == "FULL" else "4" if actual == "PARTIAL" else "0"
    paper_status = {
        "FULL": "FILLED",
        "PARTIAL": "PARTIAL",
        "REJECT": "REJECTED",
        "NO_FILL": "CANCELLED",
    }[actual]
    return {
        "schema_version": "live_vs_paper_trade_record_v3",
        "run_id": f"run-{actual.lower()}",
        "market": {
            "market_id": 42,
            "asset_id": "123456",
            "condition_id": "0xabc",
            "outcome": "YES",
            "market_slug": "will-example-happen",
            "market_title": "Will example happen?",
            "category": "politics",
        },
        "paper_trade": {
            "prediction": {
                "decision_ts": "2026-07-01T11:59:58+00:00",
                "arrival_ts": "2026-07-01T11:59:58.100000+00:00",
                "status": paper_status,
                "model_version": "paper_taker_l2_v7_shadow_head",
                "amount_unit": "SHARES",
                "requested_amount": "10",
                "filled_size": filled,
                "avg_fill_price": "0.40" if Decimal(filled) > 0 else None,
                "total_fee": "0",
            },
            "market_snapshot": {
                "shadow_checkpoint_id": "checkpoint-1",
                "shadow_generation": 1,
                "shadow_observed_at": "2026-07-01T11:59:57+00:00",
                "condition_id": "0xabc",
                "market_id": "42",
                "asset_id": "123456",
                "outcome_name": "YES",
                "tick_size": "0.01",
                "min_order_size": "1",
                "fee_rate": "0",
                "fee_exponent": "1",
                "bids": [["0.39", "100"]],
                "asks": [["0.40", "100"]],
            },
        },
        "real_trade": {
            "side": "BUY",
            "order_type": tif,
            "signed_order_audit": {
                "worst_price": "0.40",
                "taker_amount": "10000000",
                "exchange_submit_called": True,
            },
            "rest_order": {
                "created_at": 1782907200,
                "original_size": "10",
                "size_matched": filled,
            },
        },
        "reconciliation": {"actual_class": actual},
        "live_vs_paper_comparison": {
            "final_probe_state": "CALIBRATABLE",
            "real_status": actual,
            "real_filled_size": filled,
            "real_average_price": "0.40" if Decimal(filled) > 0 else None,
            "real_fee": "0",
            "paper_status": paper_status,
            "paper_filled_size": filled,
            "paper_average_price": "0.40" if Decimal(filled) > 0 else None,
            "paper_fee": "0",
            "price_error_ticks": "0",
            "filled_size_relative_error": "0",
            "fee_error": "0",
            "verdict": "MATCH",
        },
    }


def test_normalize_and_build_arrival_only_fok_request() -> None:
    sample = normalize_paper_live_record(_record(), source_path="sample.json")

    assert sample.requested_size == Decimal("10")
    assert sample.actual_filled_size == Decimal("10")
    assert sample.arrival_ts == datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
    assert sample.execution_label_eligible is True

    payload = build_v3_arrival_replay_payload(sample)
    order = payload["orders"][0]
    assert order["signalTs"] == "2026-07-01T12:00:00+00:00"
    assert order["latencySeconds"] == 0
    assert order["allowPartialFill"] is False
    assert payload["profile"] == "taker_arrival_probability_only"


def test_liquidity_fok_rejection_is_negative_execution_label() -> None:
    payload = _record(actual="REJECT")
    payload["schema_version"] = "live_vs_paper_rejection_record_v1"
    payload["real_trade"]["rest_order"] = None
    payload["real_trade"]["signed_order_audit"]["timestamp"] = "1782907200000"
    payload["real_trade"]["signed_order_audit"]["maker_amount"] = "10000000"
    payload["real_trade"]["side"] = "SELL"
    payload["reconciliation"]["rejection_reason"] = (
        "order couldn't be fully filled. FOK orders are fully filled or killed."
    )

    sample = normalize_paper_live_record(payload)

    assert sample.execution_label_eligible is True
    assert sample.any_fill_label == 0
    assert sample.full_fill_label == 0


def test_non_liquidity_rejection_is_not_a_no_fill_label() -> None:
    payload = _record(actual="REJECT")
    payload["schema_version"] = "live_vs_paper_rejection_record_v1"
    payload["real_trade"]["rest_order"] = None
    payload["real_trade"]["signed_order_audit"]["timestamp"] = "1782907200000"
    payload["reconciliation"]["rejection_reason"] = "insufficient balance"

    sample = normalize_paper_live_record(payload)

    assert sample.execution_label_eligible is False
    assert sample.exclusion_reason == "admission_rejection_not_execution_no_fill"


def test_report_scores_fok_full_probability_and_keeps_coverage_abstain_separate() -> None:
    full = normalize_paper_live_record(_record(actual="FULL"))
    no_fill_payload = _record(actual="NO_FILL")
    no_fill_payload["run_id"] = "run-no-fill"
    no_fill = normalize_paper_live_record(no_fill_payload)
    predictions = {
        full.sample_id: {
            "order": {
                "status": "MODELED_EXPECTATION",
                "filled_size": "8",
                "probability_bounds": {
                    "full_fill_proxy": "0.8",
                    "any_fill_execution_horizon": "0.9",
                },
            }
        },
        no_fill.sample_id: {
            "status": "DATA_COVERAGE_ABSTAIN",
            "error": "coverage gap",
        },
    }

    report = evaluate_v3_live_predictions(
        [full, no_fill], predictions, profile="taker_arrival_probability_only"
    )

    assert report["counts"]["scored_predictions"] == 1
    assert report["counts"]["prediction_statuses"] == {
        "DATA_COVERAGE_ABSTAIN": 1,
        "SCORED": 1,
    }
    assert report["metrics"]["brier_score"] == 0.04
    assert report["paper_live_mechanical_validation"]["match_rate"] == 1.0
    assert report["same_order_model_comparison"]["paper_l2_shadow"][
        "binary_accuracy"
    ] == 1.0
    assert report["decision_to_venue_record_seconds"]["samples"] == 2
    assert report["quality_gates"]["scored_coverage"] == 0.5
    assert report["quality_gates"]["promotion_allowed"] is False
    assert report["status"] == "BLOCKED_INSUFFICIENT_REAL_LABELS"
    assert report["selection_bias"]["population_probability_claim_allowed"] is False
    assert report["collection_requirements"]["additional_scored_labels_needed"] == 199
    assert report["collection_requirements"]["additional_negative_labels_needed"] == 20
    assert report["collection_requirements"]["required_sampling_change"].startswith(
        "RANDOMIZED_OR_KNOWN_PROBABILITY_POLICY"
    )


def test_same_order_report_runs_real_pml2_engine_separately_from_paper_l2() -> None:
    sample = normalize_paper_live_record(_record(actual="FULL"))
    pml2 = run_pml2_replay(build_pml2_live_replay_payload(sample))
    predictions = {
        sample.sample_id: {
            "order": {
                "status": "MODELED_EXPECTATION",
                "filled_size": "7",
                "probability_bounds": {"full_fill_proxy": "0.7"},
            }
        }
    }
    report = evaluate_v3_live_predictions(
        [sample],
        predictions,
        profile="taker_arrival_probability_only",
        pml2_predictions={
            sample.sample_id: {"status": "OK", "order": pml2["orders"][0]}
        },
    )

    comparison = report["same_order_model_comparison"]
    assert comparison["paper_l2_shadow"]["model_versions"] == {
        "paper_taker_l2_v7_shadow_head": 1
    }
    assert comparison["pml2"]["scored_orders"] == 1
    assert comparison["pml2"]["checkpoint_hydrated_orders"] == 0
    assert comparison["pml2"]["binary_accuracy"] == 1.0
    assert comparison["pml2"]["predicted_total_filled_size"] == "10.0000000000"
    assert report["orders"][0]["pml2_replay"]["execution_model"] == (
        "PREDICTION_L2_REPLAY_V1"
    )


def test_pml2_live_replay_preserves_quote_budget_and_signed_amounts() -> None:
    payload = _record(actual="FULL")
    prediction = payload["paper_trade"]["prediction"]
    prediction.update(
        amount_unit="QUOTE",
        requested_amount="4",
        signed_quote_amount="4",
        signed_share_amount="10",
    )
    payload["real_trade"]["signed_order_audit"].update(
        maker_amount="4000000",
        taker_amount="10000000",
        side="BUY",
    )
    sample = normalize_paper_live_record(payload)

    request = build_pml2_live_replay_payload(sample)
    order = request["orders"][0]
    assert order["size"] == "4"
    assert order["amountUnit"] == "QUOTE"
    assert order["signedMakerAmount"] == "4"
    assert order["signedTakerAmount"] == "10"
    assert order["venueAdmission"] == "ACCEPTED"
    assert order["venueAdmissionEvidenceId"] == "paper-live-terminal:run-full"

    result = run_pml2_replay(request)["orders"][0]
    assert result["status"] == "FILLED"
    assert result["filled_size"] == "10.0000000000"
    assert result["filled_amount"] == "4.0000000000"
    assert result["remaining_amount"] == "0.0000000000"


def test_pml2_live_replay_hydrates_legacy_checkpoint_levels() -> None:
    payload = _record(actual="FULL")
    snapshot = payload["paper_trade"]["market_snapshot"]
    bids = snapshot.pop("bids")
    asks = snapshot.pop("asks")
    sample = normalize_paper_live_record(payload)

    request = build_pml2_live_replay_payload(
        sample,
        checkpoint_loader=lambda checkpoint_id: {
            "checkpoint_id": checkpoint_id,
            "asset_id": "123456",
            "market_id": "42",
            "condition_id": "0xabc",
            "book_fingerprint": None,
            "bids": bids,
            "asks": asks,
        },
    )

    assert request["events"][0]["source"] == "paper_live_checkpoint_hydrated"
    result = run_pml2_replay(request)["orders"][0]
    assert result["status"] == "FILLED"
    assert result["filled_size"] == "10.0000000000"


def test_pml2_checkpoint_hydration_rejects_wrong_asset() -> None:
    payload = _record(actual="FULL")
    snapshot = payload["paper_trade"]["market_snapshot"]
    snapshot.pop("bids")
    snapshot.pop("asks")
    sample = normalize_paper_live_record(payload)

    try:
        build_pml2_live_replay_payload(
            sample,
            checkpoint_loader=lambda checkpoint_id: {
                "checkpoint_id": checkpoint_id,
                "asset_id": "wrong-asset",
                "market_id": "42",
                "condition_id": "0xabc",
                "bids": [["0.39", "100"]],
                "asks": [["0.40", "100"]],
            },
        )
    except ValueError as exc:
        assert str(exc) == "PML2_CHECKPOINT_ASSET_MISMATCH"
    else:
        raise AssertionError("mismatched checkpoint asset should fail closed")


def test_normalize_preserves_explicit_selection_contract() -> None:
    payload = _record()
    payload["submission_propensity"] = "0.25"
    payload["selection_contract"] = {
        "sampling_policy": "STRATIFIED_RANDOM_V1",
        "population_identifiable": True,
        "expected_outcome": "ANY",
    }

    sample = normalize_paper_live_record(payload)

    assert sample.submission_propensity == Decimal("0.25")
    assert sample.selection_policy == "STRATIFIED_RANDOM_V1"
    assert sample.selection_population_identifiable is True
    assert sample.expected_outcome == "ANY"


def test_loader_deduplicates_run_id_and_prefers_selection_contract(tmp_path) -> None:
    legacy = _record()
    enriched = {
        **legacy,
        "selection_contract": {
            "sampling_policy": "MANUAL_MARKET_ALLOWLIST_AND_APPROVED_ASSET",
            "population_identifiable": False,
            "expected_outcome": "ANY",
        },
    }
    (tmp_path / "a-legacy.json").write_text(json.dumps(legacy), encoding="utf-8")
    (tmp_path / "b-enriched.json").write_text(json.dumps(enriched), encoding="utf-8")

    samples, errors = load_paper_live_samples(tmp_path)

    assert errors == []
    assert len(samples) == 1
    assert samples[0].selection_policy == "MANUAL_MARKET_ALLOWLIST_AND_APPROVED_ASSET"


def test_enough_labels_cannot_hide_probability_quality_failure() -> None:
    samples = []
    predictions = {}
    for index, actual in enumerate(("FULL", "FULL", "NO_FILL", "NO_FILL")):
        payload = _record(actual=actual)
        payload["run_id"] = f"quality-{index}"
        payload["submission_propensity"] = "0.5"
        sample = normalize_paper_live_record(payload)
        samples.append(sample)
        wrong_probability = "0.1" if actual == "FULL" else "0.9"
        predictions[sample.sample_id] = {
            "order": {
                "status": "MODELED_EXPECTATION",
                "filled_size": wrong_probability,
                "probability_bounds": {"full_fill_proxy": wrong_probability},
            }
        }

    report = evaluate_v3_live_predictions(
        samples,
        predictions,
        profile="taker_arrival_probability_only",
        min_labels=4,
        min_positive=2,
        min_negative=2,
        min_independent_markets=1,
        min_independent_dates=1,
    )

    assert report["quality_gates"]["sample_support_passed"] is True
    assert report["quality_gates"]["probability_quality_passed"] is False
    assert report["status"] == "FAIL_LIVE_CALIBRATION_QUALITY"
    assert report["selection_bias"]["population_probability_claim_allowed"] is False


def test_arrival_probability_profile_never_routes_to_future_source_fill() -> None:
    profile = get_trade_only_profile("taker_arrival_probability_only")

    assert profile.execution_mode == ExecutionMode.HIERARCHICAL_EXPECTED_FILL
    assert profile.augment_source_with_modeled_residual is False
    assert profile.result_role == "LIVE_LABEL_CALIBRATION_PREDICTION"
