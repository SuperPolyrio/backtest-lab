"""Pair historical Paper-Live probes with point-in-time Fill-only V3 scores."""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from quant.calibration.calibration_metrics import wilson_interval
from quant.calibration.market_taxonomy import normalize_market_domain

SUPPORTED_SCHEMAS = frozenset(
    {
        "live_vs_paper_trade_record_v2",
        "live_vs_paper_trade_record_v3",
        "live_vs_paper_rejection_record_v1",
    }
)
LIQUIDITY_REJECTION_MARKERS = (
    "couldn't be fully filled",
    "could not be fully filled",
    "fully filled or killed",
)
Q = Decimal("0.0000000001")


@dataclass(frozen=True)
class V3LiveCalibrationSample:
    sample_id: str
    source_path: str
    schema_version: str
    run_id: str
    market_id: int
    asset_id: str
    market_slug: str
    market_title: str
    category: str
    domain: str
    side: str
    tif: str
    limit_price: Decimal
    requested_size: Decimal
    actual_filled_size: Decimal
    actual_avg_fill_price: Decimal | None
    actual_fee: Decimal | None
    actual_class: str
    any_fill_label: int
    full_fill_label: int
    execution_label_eligible: bool
    exclusion_reason: str
    decision_ts: datetime | None
    arrival_ts: datetime | None
    arrival_time_source: str
    decision_to_arrival_seconds: Decimal | None
    paper_status: str
    paper_class: str
    paper_model_version: str
    paper_filled_size: Decimal
    paper_avg_fill_price: Decimal | None
    paper_fee: Decimal | None
    paper_price_error_ticks: Decimal | None
    paper_filled_size_relative_error: Decimal | None
    paper_fee_error: Decimal | None
    paper_live_verdict: str
    amount_unit: str
    requested_amount: Decimal
    signed_quote_amount: Decimal | None
    signed_share_amount: Decimal | None
    condition_id: str
    outcome_role: str
    paper_modeled_arrival_ts: datetime | None
    submission_propensity: Decimal | None
    selection_policy: str
    selection_population_identifiable: bool
    expected_outcome: str
    paper_snapshot: Mapping[str, Any] = field(repr=False, compare=False)

    @property
    def actual_fill_fraction(self) -> Decimal:
        if self.requested_size <= 0:
            return Decimal(0)
        return min(Decimal(1), self.actual_filled_size / self.requested_size)

    def as_dict(self) -> dict[str, Any]:
        row = asdict(self)
        row["decision_ts"] = (
            self.decision_ts.isoformat() if self.decision_ts is not None else None
        )
        row["arrival_ts"] = (
            self.arrival_ts.isoformat() if self.arrival_ts is not None else None
        )
        row["paper_modeled_arrival_ts"] = (
            self.paper_modeled_arrival_ts.isoformat()
            if self.paper_modeled_arrival_ts is not None
            else None
        )
        row.pop("paper_snapshot", None)
        row["actual_fill_fraction"] = str(self.actual_fill_fraction.quantize(Q))
        for key, value in tuple(row.items()):
            if isinstance(value, Decimal):
                row[key] = str(value)
        return row


def load_paper_live_samples(
    records_dir: Path,
) -> tuple[list[V3LiveCalibrationSample], list[dict[str, str]]]:
    selected: dict[str, tuple[V3LiveCalibrationSample, bool]] = {}
    errors: list[dict[str, str]] = []
    for path in sorted(records_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, Mapping):
                continue
            if payload.get("schema_version") not in SUPPORTED_SCHEMAS:
                continue
            sample = normalize_paper_live_record(payload, source_path=path)
            has_selection_contract = isinstance(
                payload.get("selection_contract"), Mapping
            )
            current = selected.get(sample.sample_id)
            if current is None or (has_selection_contract and not current[1]):
                selected[sample.sample_id] = (sample, has_selection_contract)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            errors.append({"source_path": str(path.resolve()), "error": str(exc)})
    samples = [item[0] for item in selected.values()]
    samples.sort(key=lambda sample: (sample.arrival_ts or datetime.min.replace(tzinfo=timezone.utc), sample.sample_id))
    return samples, errors


def normalize_paper_live_record(
    payload: Mapping[str, Any], *, source_path: Path | str = ""
) -> V3LiveCalibrationSample:
    schema_version = _text(payload.get("schema_version"))
    if schema_version not in SUPPORTED_SCHEMAS:
        raise ValueError(f"unsupported Paper-Live schema: {schema_version or 'missing'}")

    market = _mapping(payload.get("market"))
    real = _mapping(payload.get("real_trade") or payload.get("real_order"))
    paper = _mapping(payload.get("paper_trade") or payload.get("paper_order"))
    paper_order = _mapping(paper.get("order"))
    prediction = _mapping(paper.get("prediction"))
    paper_snapshot = _mapping(paper.get("market_snapshot"))
    audit = _mapping(real.get("signed_order_audit"))
    rest_order = _mapping(real.get("rest_order"))
    reconciliation = _mapping(payload.get("reconciliation"))
    comparison = _mapping(payload.get("live_vs_paper_comparison"))
    selection_contract = _mapping(payload.get("selection_contract"))

    run_id = _text(payload.get("run_id"))
    sample_id = run_id or _stable_sample_id(payload, source_path)
    side = _upper(
        _first(real, "side")
        or _first(audit, "side")
        or _first(rest_order, "side")
        or _first(paper_order, "side")
        or _first(prediction, "side")
    )
    tif = _upper(
        _first(real, "order_type")
        or _first(rest_order, "order_type")
        or _first(paper, "order_type")
        or _first(paper_order, "order_type")
        or reconciliation.get("order_type")
    )
    limit_price = _decimal(
        _first(audit, "worst_price", "implied_price")
        or _first(rest_order, "price")
        or _first(paper_order, "limit_price")
        or _first(prediction, "signed_worst_price", "avg_fill_price")
    )
    requested_size = _requested_shares(
        side=side,
        real=real,
        audit=audit,
        rest_order=rest_order,
        paper=paper,
        paper_order=paper_order,
        prediction=prediction,
        limit_price=limit_price,
    )
    actual_filled_size = _decimal(
        comparison.get("real_filled_size")
        or _mapping(reconciliation.get("order")).get("actual_matched_size")
        or rest_order.get("size_matched")
        or _sum_trade_sizes(real.get("rest_trades"))
        or 0
    )
    actual_avg_fill_price = _optional_decimal(
        comparison.get("real_average_price")
        or _mapping(reconciliation.get("order")).get("actual_avg_price")
    )
    actual_fee = _optional_decimal(
        comparison.get("real_fee") or reconciliation.get("actual_fee")
    )
    actual_class = _canonical_actual_class(
        reconciliation.get("actual_class") or comparison.get("real_status")
    )
    decision_ts = _datetime(
        prediction.get("decision_ts")
        or _mapping(paper.get("decision_and_submission_timestamps")).get(
            "decision_ts"
        )
    )
    arrival_ts, arrival_source = _arrival_time(real, audit, rest_order)
    rejection_reason = _text(
        reconciliation.get("rejection_reason")
        or _mapping(real.get("http_response")).get("error")
        or _mapping(real.get("http_response")).get("errorMsg")
    )
    label_eligible, exclusion_reason = _execution_label_eligibility(
        actual_class=actual_class,
        side=side,
        tif=tif,
        limit_price=limit_price,
        requested_size=requested_size,
        arrival_ts=arrival_ts,
        probe_state=_text(comparison.get("final_probe_state")),
        rejection_reason=rejection_reason,
        exchange_submit_called=audit.get("exchange_submit_called"),
    )
    market_title = _text(market.get("market_title"))
    market_slug = _text(market.get("market_slug"))
    category = _text(market.get("category"))
    propensity = _optional_decimal(
        payload.get("submission_propensity")
        or paper.get("submission_propensity")
        or _mapping(paper.get("paired_probe")).get("submission_propensity")
    )
    latency = None
    if decision_ts is not None and arrival_ts is not None:
        latency = Decimal(str((arrival_ts - decision_ts).total_seconds())).quantize(Q)
    paper_status = _upper(comparison.get("paper_status") or prediction.get("status"))
    paper_filled_size = _decimal(
        comparison.get("paper_filled_size") or prediction.get("filled_size") or 0
    )
    amount_unit = _upper(
        prediction.get("amount_unit")
        or paper.get("amount_unit")
        or real.get("amount_unit")
        or audit.get("amount_unit")
    )
    requested_amount = _decimal(
        prediction.get("approved_requested_amount")
        or prediction.get("requested_amount")
        or paper.get("requested_amount")
        or real.get("requested_amount")
        or audit.get("amount")
        or requested_size
    )
    signed_quote_amount, signed_share_amount = _normalized_signed_amounts(
        side=side,
        audit=audit,
        prediction=prediction,
    )
    condition_id = _text(
        paper_snapshot.get("condition_id") or market.get("condition_id")
    )
    outcome_role = _outcome_role(paper_snapshot, _text(market.get("asset_id")))

    return V3LiveCalibrationSample(
        sample_id=sample_id,
        source_path=str(Path(source_path).resolve()) if source_path else "",
        schema_version=schema_version,
        run_id=run_id,
        market_id=_integer(market.get("market_id"), "market_id"),
        asset_id=_text(market.get("asset_id")),
        market_slug=market_slug,
        market_title=market_title,
        category=category,
        domain=normalize_market_domain(
            category,
            market_title=market_title,
            market_slug=market_slug,
        ),
        side=side,
        tif=tif,
        limit_price=limit_price,
        requested_size=requested_size,
        actual_filled_size=actual_filled_size,
        actual_avg_fill_price=actual_avg_fill_price,
        actual_fee=actual_fee,
        actual_class=actual_class,
        any_fill_label=int(actual_filled_size > 0),
        full_fill_label=int(
            requested_size > 0 and actual_filled_size >= requested_size
        ),
        execution_label_eligible=label_eligible,
        exclusion_reason=exclusion_reason,
        decision_ts=decision_ts,
        arrival_ts=arrival_ts,
        arrival_time_source=arrival_source,
        decision_to_arrival_seconds=latency,
        paper_status=paper_status,
        paper_class=_canonical_prediction_class(
            paper_status, paper_filled_size, requested_size
        ),
        paper_model_version=_text(
            prediction.get("execution_model") or prediction.get("model_version")
        ),
        paper_filled_size=paper_filled_size,
        paper_avg_fill_price=_optional_decimal(
            comparison.get("paper_average_price")
            or prediction.get("avg_fill_price")
        ),
        paper_fee=_optional_decimal(
            comparison.get("paper_fee") or prediction.get("total_fee")
        ),
        paper_price_error_ticks=_optional_decimal(
            comparison.get("price_error_ticks")
        ),
        paper_filled_size_relative_error=_optional_decimal(
            comparison.get("filled_size_relative_error")
        ),
        paper_fee_error=_optional_decimal(comparison.get("fee_error")),
        paper_live_verdict=_upper(comparison.get("verdict")),
        amount_unit=amount_unit,
        requested_amount=requested_amount,
        signed_quote_amount=signed_quote_amount,
        signed_share_amount=signed_share_amount,
        condition_id=condition_id,
        outcome_role=outcome_role,
        paper_modeled_arrival_ts=_datetime(prediction.get("arrival_ts")),
        submission_propensity=propensity,
        selection_policy=_upper(selection_contract.get("sampling_policy")),
        selection_population_identifiable=bool(
            selection_contract.get("population_identifiable")
        ),
        expected_outcome=_upper(selection_contract.get("expected_outcome")) or "UNKNOWN",
        paper_snapshot=dict(paper_snapshot),
    )


def build_v3_arrival_replay_payload(
    sample: V3LiveCalibrationSample,
    *,
    profile: str = "taker_arrival_probability_only",
    matcher_backend: str = "auto",
    anchor_max_distance_seconds: int = 120,
) -> dict[str, Any]:
    if not sample.execution_label_eligible:
        raise ValueError(f"sample is not execution-label eligible: {sample.exclusion_reason}")
    if sample.arrival_ts is None:
        raise ValueError("sample has no venue-arrival timestamp")
    return {
        "requestId": f"v3-live-calibration:{sample.sample_id}",
        "profile": profile,
        "matcherBackend": matcher_backend,
        "anchorMaxDistanceSeconds": anchor_max_distance_seconds,
        "orders": [
            {
                "orderId": sample.sample_id,
                "marketId": sample.market_id,
                "assetId": sample.asset_id,
                "side": sample.side,
                "limitPrice": str(sample.limit_price),
                "size": str(sample.requested_size),
                # The target is execution conditional on a submitted order.  The
                # probability snapshot is therefore taken at venue arrival, not
                # at the earlier strategy decision or later fill timestamp.
                "signalTs": sample.arrival_ts.isoformat(),
                "tif": sample.tif,
                "liquidityIntent": "TAKER",
                "allowPartialFill": sample.tif != "FOK",
                "latencySeconds": 0,
                "horizonSeconds": 1,
                "lookbackSeconds": 300,
                "lookbackBlocks": 300,
                "horizonBlocks": 1,
                "marketSlug": sample.market_slug or None,
                "marketTitle": sample.market_title or None,
                "category": sample.category or None,
            }
        ],
    }


def build_pml2_live_replay_payload(
    sample: V3LiveCalibrationSample,
    *,
    profile: str = "realistic",
    checkpoint_loader: Callable[[str], Mapping[str, Any] | None] | None = None,
) -> dict[str, Any]:
    """Build PML2 replay when amount, depth, and binary identity are compatible."""

    if not sample.execution_label_eligible:
        raise ValueError(f"sample is not execution-label eligible: {sample.exclusion_reason}")
    if sample.amount_unit not in {"SHARES", "QUOTE"}:
        raise ValueError(f"PML2_AMOUNT_UNIT_UNSUPPORTED:{sample.amount_unit or 'UNKNOWN'}")
    if sample.amount_unit == "QUOTE" and sample.side != "BUY":
        raise ValueError("PML2_QUOTE_AMOUNT_REQUIRES_BUY")
    if sample.amount_unit == "QUOTE" and sample.tif not in {"FOK", "FAK", "IOC"}:
        raise ValueError("PML2_QUOTE_AMOUNT_REQUIRES_IMMEDIATE_TIF")
    if sample.decision_ts is None:
        raise ValueError("PML2_DECISION_TS_MISSING")
    if sample.outcome_role not in {"YES", "NO"}:
        raise ValueError("PML2_BINARY_OUTCOME_ROLE_UNRESOLVED")
    snapshot = dict(sample.paper_snapshot)
    bids = _snapshot_levels(snapshot.get("bids"))
    asks = _snapshot_levels(snapshot.get("asks"))
    required_levels = asks if sample.side == "BUY" else bids
    checkpoint_hydrated = False
    if not required_levels and checkpoint_loader is not None:
        checkpoint_id = _text(snapshot.get("shadow_checkpoint_id"))
        if not checkpoint_id:
            raise ValueError("PML2_CHECKPOINT_ID_MISSING")
        checkpoint = checkpoint_loader(checkpoint_id)
        if checkpoint is None:
            raise ValueError("PML2_CHECKPOINT_NOT_FOUND")
        _validate_pml2_checkpoint(sample, snapshot, checkpoint, checkpoint_id)
        snapshot["bids"] = checkpoint.get("bids") or []
        snapshot["asks"] = checkpoint.get("asks") or []
        bids = _snapshot_levels(snapshot.get("bids"))
        asks = _snapshot_levels(snapshot.get("asks"))
        required_levels = asks if sample.side == "BUY" else bids
        checkpoint_hydrated = True
    if not required_levels:
        raise ValueError("PML2_FULL_OPPOSITE_LEVELS_MISSING")
    snapshot_ts = _datetime(
        snapshot.get("shadow_observed_at")
        or snapshot.get("last_receive_ts")
        or snapshot.get("observed_at")
    )
    if snapshot_ts is None:
        raise ValueError("PML2_SNAPSHOT_TS_MISSING")
    modeled_arrival = sample.paper_modeled_arrival_ts
    entry_latency_ms = 100
    if modeled_arrival is not None:
        entry_latency_ms = max(
            0,
            round((modeled_arrival - sample.decision_ts).total_seconds() * 1_000),
        )
    event: dict[str, Any] = {
        "type": "SNAPSHOT",
        "snapshotId": _text(snapshot.get("shadow_checkpoint_id"))
        or f"paper-live-snapshot:{sample.sample_id}",
        "conditionId": sample.condition_id,
        "marketId": str(snapshot.get("market_id") or sample.market_id),
        "assetId": sample.asset_id,
        "outcome": sample.outcome_role,
        # Historical Paper-Live checkpoints only preserve the receive clock.
        # Reuse it for exchange ordering and expose that approximation in the report.
        "exchangeTs": snapshot_ts.isoformat(),
        "sourceReceivedTs": snapshot_ts.isoformat(),
        "localTs": snapshot_ts.isoformat(),
        "bookEpoch": 0,
        "source": (
            "paper_live_checkpoint_hydrated"
            if checkpoint_hydrated
            else "paper_live_shadow_snapshot"
        ),
        "sequence": int(snapshot.get("shadow_generation") or 0),
        "isFullDepth": True,
        "isTruncated": False,
        "depthScope": "PAPER_LIVE_CAPTURED_FULL_DEPTH",
        "bookHash": _text(
            snapshot.get("shadow_book_fingerprint") or snapshot.get("rest_book_hash")
        ),
        "bids": bids,
        "asks": asks,
    }
    if snapshot.get("tick_size") not in (None, ""):
        event["tickSize"] = str(snapshot["tick_size"])
    if snapshot.get("min_order_size") not in (None, ""):
        event["minOrderSize"] = str(snapshot["min_order_size"])
    order: dict[str, Any] = {
        "orderId": sample.sample_id,
        "strategyId": "pml2-live-calibration",
        "conditionId": sample.condition_id,
        "marketId": str(snapshot.get("market_id") or sample.market_id),
        "assetId": sample.asset_id,
        "outcome": sample.outcome_role,
        "side": sample.side,
        "size": str(sample.requested_amount),
        "amountUnit": sample.amount_unit,
        "limitPrice": str(sample.limit_price),
        "tif": sample.tif,
        "signalTs": sample.decision_ts.isoformat(),
        "observedTs": sample.decision_ts.isoformat(),
        "submitTs": sample.decision_ts.isoformat(),
        "entryLatencyMs": entry_latency_ms,
        "responseLatencyMs": 0,
        "venueDelayMs": 0,
        "venueAdmission": "ACCEPTED",
        "venueAdmissionEvidenceId": f"paper-live-terminal:{sample.sample_id}",
        "feeRate": str(snapshot.get("fee_rate") or 0),
        "feeExponent": str(snapshot.get("fee_exponent") or 1),
    }
    if sample.signed_quote_amount is not None and sample.signed_share_amount is not None:
        if sample.side == "BUY":
            order["signedMakerAmount"] = str(sample.signed_quote_amount)
            order["signedTakerAmount"] = str(sample.signed_share_amount)
        else:
            order["signedMakerAmount"] = str(sample.signed_share_amount)
            order["signedTakerAmount"] = str(sample.signed_quote_amount)
    return {
        "requestId": f"pml2-live-calibration:{sample.sample_id}",
        "runId": f"pml2-live-calibration:{sample.sample_id}",
        "profile": profile,
        "events": [event],
        "orders": [order],
    }


def _validate_pml2_checkpoint(
    sample: V3LiveCalibrationSample,
    snapshot: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    checkpoint_id: str,
) -> None:
    actual_checkpoint_id = _text(checkpoint.get("checkpoint_id"))
    if actual_checkpoint_id and actual_checkpoint_id != checkpoint_id:
        raise ValueError("PML2_CHECKPOINT_ID_MISMATCH")
    if _text(checkpoint.get("asset_id")) != sample.asset_id:
        raise ValueError("PML2_CHECKPOINT_ASSET_MISMATCH")
    checkpoint_market_id = _text(checkpoint.get("market_id"))
    if checkpoint_market_id and checkpoint_market_id != str(sample.market_id):
        raise ValueError("PML2_CHECKPOINT_MARKET_MISMATCH")
    checkpoint_condition_id = _text(checkpoint.get("condition_id")).lower()
    if (
        checkpoint_condition_id
        and sample.condition_id
        and checkpoint_condition_id != sample.condition_id.lower()
    ):
        raise ValueError("PML2_CHECKPOINT_CONDITION_MISMATCH")
    expected_fingerprint = _text(snapshot.get("shadow_book_fingerprint"))
    actual_fingerprint = _text(checkpoint.get("book_fingerprint"))
    if (
        expected_fingerprint
        and actual_fingerprint
        and expected_fingerprint != actual_fingerprint
    ):
        raise ValueError("PML2_CHECKPOINT_FINGERPRINT_MISMATCH")


def evaluate_v3_live_predictions(
    samples: Iterable[V3LiveCalibrationSample],
    predictions: Mapping[str, Mapping[str, Any]],
    *,
    profile: str,
    pml2_predictions: Mapping[str, Mapping[str, Any]] | None = None,
    pml2_profile: str = "realistic",
    parse_errors: Iterable[Mapping[str, str]] = (),
    min_labels: int = 200,
    min_positive: int = 20,
    min_negative: int = 20,
    min_independent_markets: int = 20,
    min_independent_dates: int = 10,
    min_scored_coverage: float = 0.80,
    max_brier_score: float = 0.20,
    max_brier_regret: float = 0.01,
    max_abs_calibration_bias: float = 0.10,
) -> dict[str, Any]:
    sample_rows = list(samples)
    pml2_rows = pml2_predictions or {}
    parse_error_rows = list(parse_errors)
    evaluation_rows: list[dict[str, Any]] = []
    per_order: list[dict[str, Any]] = []
    replay_status: Counter[str] = Counter()
    for sample in sample_rows:
        prediction = dict(predictions.get(sample.sample_id) or {})
        row = _evaluate_one(sample, prediction)
        row["pml2_replay"] = _evaluate_pml2_one(
            sample, dict(pml2_rows.get(sample.sample_id) or {})
        )
        per_order.append(row)
        replay_status[row["prediction_status"]] += 1
        if row.get("scored"):
            evaluation_rows.append(row)

    labels = [int(row["label"]) for row in evaluation_rows]
    positives = sum(labels)
    negatives = len(labels) - positives
    eligible_count = sum(sample.execution_label_eligible for sample in sample_rows)
    propensity_count = sum(
        sample.submission_propensity is not None
        for sample in sample_rows
        if sample.execution_label_eligible
    )
    metrics = _probability_metrics(evaluation_rows)
    paper_l2_metrics = _paper_l2_metrics(sample_rows)
    pml2_metrics = _pml2_metrics(per_order)
    independent_markets = len({row["market_id"] for row in evaluation_rows})
    independent_dates = len(
        {
            str(row.get("arrival_ts") or "")[:10]
            for row in evaluation_rows
            if row.get("arrival_ts")
        }
    )
    scored_coverage = len(evaluation_rows) / eligible_count if eligible_count else 0.0
    sample_checks = {
        "minimum_labels": len(evaluation_rows) >= min_labels,
        "minimum_positive_labels": positives >= min_positive,
        "minimum_negative_labels": negatives >= min_negative,
        "minimum_independent_markets": (
            independent_markets >= min_independent_markets
        ),
        "minimum_independent_dates": independent_dates >= min_independent_dates,
        "minimum_scored_coverage": scored_coverage >= min_scored_coverage,
    }
    metric_checks = {
        "maximum_brier_score": _metric_at_most(
            metrics.get("brier_score"), max_brier_score
        ),
        "maximum_brier_regret_vs_base_rate": _metric_at_most(
            metrics.get("brier_regret_vs_base_rate"), max_brier_regret
        ),
        "maximum_abs_calibration_in_the_large": _metric_at_most(
            abs(float(metrics["calibration_in_the_large"]))
            if metrics.get("calibration_in_the_large") is not None
            else None,
            max_abs_calibration_bias,
        ),
    }
    sample_support_passed = all(sample_checks.values())
    probability_quality_passed = all(metric_checks.values())
    selection_support_passed = bool(
        eligible_count > 0
        and propensity_count == eligible_count
        and all(
            sample.selection_population_identifiable
            for sample in sample_rows
            if sample.execution_label_eligible
        )
    )
    mechanical_rows = [
        sample for sample in sample_rows if sample.paper_live_verdict
    ]
    mechanical_matches = sum(
        sample.paper_live_verdict == "MATCH" for sample in mechanical_rows
    )
    population_transfer = bool(
        sample_support_passed
        and probability_quality_passed
        and selection_support_passed
    )
    if not sample_support_passed:
        status = "BLOCKED_INSUFFICIENT_REAL_LABELS"
    elif not probability_quality_passed:
        status = "FAIL_LIVE_CALIBRATION_QUALITY"
    elif not selection_support_passed:
        status = "BLOCKED_SELECTION_BIAS"
    else:
        status = "READY_FOR_CONDITIONAL_LIVE_CALIBRATION"
    generated_at = datetime.now(timezone.utc).isoformat()
    collection_requirements = {
        "additional_scored_labels_needed": max(0, min_labels - len(evaluation_rows)),
        "additional_positive_labels_needed": max(0, min_positive - positives),
        "additional_negative_labels_needed": max(0, min_negative - negatives),
        "additional_independent_markets_needed": max(
            0, min_independent_markets - independent_markets
        ),
        "additional_independent_dates_needed": max(
            0, min_independent_dates - independent_dates
        ),
        "logged_propensity_rows_needed": max(0, eligible_count - propensity_count),
        "required_sampling_change": (
            "RANDOMIZED_OR_KNOWN_PROBABILITY_POLICY_WITH_LOGGED_PROPENSITY"
            if not selection_support_passed
            else "NONE"
        ),
        "negative_examples_must_be_real": True,
        "paper_or_l2_proxy_negatives_can_replace_real_labels": False,
    }
    source_digest = hashlib.sha256(
        json.dumps(
            [
                {
                    "sample_id": sample.sample_id,
                    "source_path": sample.source_path,
                    "actual_class": sample.actual_class,
                    "arrival_ts": sample.arrival_ts.isoformat()
                    if sample.arrival_ts
                    else None,
                }
                for sample in sample_rows
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return {
        "schema_version": "fill_only_v3_live_order_calibration_v2",
        "generated_at": generated_at,
        "profile": profile,
        "status": status,
        "source_contract": {
            "records": "Paper-Live paired probes with real venue terminal outcomes",
            "probability_snapshot": "ORDER_ARRIVAL_PRE_OUTCOME_ORDERFILLED_ONLY",
            "v3_runtime_lob_usage": "NONE",
            "pml2_runtime_lob_usage": "SAVED_PAPER_LIVE_L2_SNAPSHOT",
            "future_source_fill_used_for_prediction": False,
            "source_digest": source_digest,
        },
        "selection_bias": {
            "sampling_policy": "PAPER_POLICY_SELECTED_LIVE_PROBES",
            "observed_sampling_policies": dict(
                sorted(
                    Counter(
                        sample.selection_policy or "UNRECORDED"
                        for sample in sample_rows
                        if sample.execution_label_eligible
                    ).items()
                )
            ),
            "expected_outcomes": dict(
                sorted(
                    Counter(
                        sample.expected_outcome
                        for sample in sample_rows
                        if sample.execution_label_eligible
                    ).items()
                )
            ),
            "eligible_rows_with_logged_submission_propensity": propensity_count,
            "eligible_rows": eligible_count,
            "inverse_propensity_weighting_applied": False,
            "selection_support_passed": selection_support_passed,
            "population_probability_claim_allowed": population_transfer,
            "reason": (
                "logged propensities and broad independent coverage support transfer"
                if population_transfer
                else "current live probes are policy-selected; evaluate the probe policy only and collect broader negatives"
            ),
        },
        "counts": {
            "source_records": len(sample_rows),
            "parse_errors": len(parse_error_rows),
            "execution_label_eligible": sum(
                sample.execution_label_eligible for sample in sample_rows
            ),
            "scored_predictions": len(evaluation_rows),
            "positive_labels": positives,
            "negative_labels": negatives,
            "independent_markets": independent_markets,
            "independent_dates": independent_dates,
            "actual_classes": dict(
                sorted(Counter(sample.actual_class for sample in sample_rows).items())
            ),
            "prediction_statuses": dict(sorted(replay_status.items())),
        },
        "metrics": metrics,
        "same_order_model_comparison": {
            "contract": "SAME_REAL_SUBMITTED_ORDERS_AND_TERMINAL_OUTCOMES",
            "actual": {
                "eligible_orders": eligible_count,
                "positive_orders": sum(
                    sample.any_fill_label
                    for sample in sample_rows
                    if sample.execution_label_eligible
                ),
                "total_filled_size": _decimal_text(
                    sum(
                        (
                            sample.actual_filled_size
                            for sample in sample_rows
                            if sample.execution_label_eligible
                        ),
                        Decimal(0),
                    )
                ),
            },
            "paper_l2_shadow": paper_l2_metrics,
            "pml2": {
                "profile": pml2_profile,
                **pml2_metrics,
                "input_clock_contract": (
                    "SAVED_PAPER_LIVE_RECEIVE_TS_REUSED_FOR_EVENT_ORDERING"
                ),
            },
            "fill_only_v3": {
                "profile": profile,
                "scored_orders": len(evaluation_rows),
                "abstained_or_failed_orders": eligible_count - len(evaluation_rows),
                "expected_positive_orders": metrics.get("expected_positive_orders"),
                "actual_positive_orders_on_scored_subset": metrics.get(
                    "actual_positive_orders"
                ),
                "brier_score": metrics.get("brier_score"),
                "brier_regret_vs_base_rate": metrics.get(
                    "brier_regret_vs_base_rate"
                ),
            },
            "claim_boundary": (
                "Paper-L2 and PML2 are deterministic L2 execution checks; V3 is an "
                "OrderFilled-only probability forecast. Policy-selected probes do not "
                "establish population-wide accuracy for either model."
            ),
        },
        "paper_live_mechanical_validation": {
            "samples": len(mechanical_rows),
            "matches": mechanical_matches,
            "match_rate": (
                round(mechanical_matches / len(mechanical_rows), 10)
                if mechanical_rows
                else None
            ),
            "contract": "PAPER_PREDICTION_VS_REAL_PRICE_SIZE_FEE_AND_TERMINAL_STATE",
            "calibrates_v3_probability_by_itself": False,
            "execution_models": paper_l2_metrics["model_versions"],
        },
        "decision_to_venue_record_seconds": _latency_metrics(sample_rows),
        "requirements": {
            "min_labels": min_labels,
            "min_positive": min_positive,
            "min_negative": min_negative,
            "min_independent_markets": min_independent_markets,
            "min_independent_dates": min_independent_dates,
            "min_scored_coverage": min_scored_coverage,
            "max_brier_score": max_brier_score,
            "max_brier_regret": max_brier_regret,
            "max_abs_calibration_bias": max_abs_calibration_bias,
            "enough_labels": (
                sample_checks["minimum_labels"]
                and sample_checks["minimum_positive_labels"]
                and sample_checks["minimum_negative_labels"]
            ),
        },
        "quality_gates": {
            "scored_coverage": round(scored_coverage, 10),
            "sample_checks": sample_checks,
            "metric_checks": metric_checks,
            "sample_support_passed": sample_support_passed,
            "probability_quality_passed": probability_quality_passed,
            "selection_support_passed": selection_support_passed,
            "promotion_allowed": population_transfer,
        },
        "collection_requirements": collection_requirements,
        "claim_boundaries": {
            "paper_simulation_calibrates_live_probability_by_itself": False,
            "current_report_can_fit_probe_policy_calibrator": (
                sample_support_passed
            ),
            "current_report_can_refit_runtime_probability": population_transfer,
            "current_report_can_promote_live_probability": population_transfer,
            "current_report_can_validate_positive_order_consistency": bool(
                evaluation_rows
            ),
            "replay_coverage_failures_are_model_failures": False,
        },
        "parse_errors": parse_error_rows,
        "by_dimension": {
            key: _group_metrics(evaluation_rows, key)
            for key in ("tif", "side", "domain", "expected_outcome")
        },
        "orders": per_order,
    }


def _metric_at_most(value: Any, threshold: float) -> bool:
    if value is None:
        return False
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(parsed) and parsed <= threshold


def _evaluate_one(
    sample: V3LiveCalibrationSample, prediction: Mapping[str, Any]
) -> dict[str, Any]:
    base = sample.as_dict()
    if not sample.execution_label_eligible:
        return {
            **base,
            "prediction_status": "LABEL_EXCLUDED",
            "scored": False,
            "prediction_error": sample.exclusion_reason,
        }
    if not prediction:
        return {
            **base,
            "prediction_status": "PREDICTION_NOT_RUN",
            "scored": False,
            "prediction_error": "no V3 replay result was supplied",
        }
    if prediction.get("error"):
        return {
            **base,
            "prediction_status": _upper(prediction.get("status") or "REPLAY_ERROR"),
            "scored": False,
            "prediction_error": _text(prediction.get("error")),
        }

    result = _mapping(prediction.get("order") or prediction)
    probability_bounds = _mapping(result.get("probability_bounds"))
    if sample.tif == "FOK":
        probability = _optional_decimal(
            probability_bounds.get("full_fill_proxy")
            or probability_bounds.get("horizon")
        )
        label = sample.full_fill_label
        target = "FOK_FULL_FILL"
    else:
        probability = _optional_decimal(
            probability_bounds.get("any_fill_execution_horizon")
            or probability_bounds.get("horizon")
        )
        label = sample.any_fill_label
        target = "FAK_ANY_FILL"
    if probability is None:
        return {
            **base,
            "prediction_status": "MODEL_ABSTAIN",
            "scored": False,
            "prediction_error": _text(result.get("reason")) or "missing_probability",
            "v3_result": result,
        }
    expected_fraction = min(
        Decimal(1),
        _decimal(result.get("filled_size")) / sample.requested_size,
    )
    return {
        **base,
        "prediction_status": "SCORED",
        "scored": True,
        "score_target": target,
        "score_probability": str(probability.quantize(Q)),
        "label": label,
        "expected_fill_fraction": str(expected_fraction.quantize(Q)),
        "fill_fraction_error": str(
            (expected_fraction - sample.actual_fill_fraction).quantize(Q)
        ),
        "v3_status": _text(result.get("status")),
        "v3_reason": _text(result.get("reason")),
        "v3_result_role": _text(result.get("result_role")),
        "v3_calibration_status": _text(result.get("calibration_status")),
        "v3_model_diagnostics": result.get("model_diagnostics") or {},
    }


def _evaluate_pml2_one(
    sample: V3LiveCalibrationSample,
    prediction: Mapping[str, Any],
) -> dict[str, Any]:
    if not sample.execution_label_eligible:
        return {
            "prediction_status": "LABEL_EXCLUDED",
            "scored": False,
            "prediction_error": sample.exclusion_reason,
        }
    if not prediction:
        return {
            "prediction_status": "PREDICTION_NOT_RUN",
            "scored": False,
            "prediction_error": "no PML2 replay result was supplied",
        }
    if prediction.get("error"):
        return {
            "prediction_status": _upper(
                prediction.get("status") or "PML2_REPLAY_ERROR"
            ),
            "scored": False,
            "prediction_error": _text(prediction.get("error")),
        }
    result = _mapping(prediction.get("order") or prediction)
    filled_size = _decimal(result.get("filled_size"))
    predicted_class = _canonical_prediction_class(
        result.get("status"), filled_size, sample.requested_size
    )
    predicted_positive = int(filled_size > 0)
    raw_fills = result.get("fills")
    fills: list[Any] = raw_fills if isinstance(raw_fills, list) else []
    fee = sum(
        (_decimal(_mapping(item).get("fee")) for item in fills), Decimal(0)
    )
    source_event_ids: set[str] = set()
    for item in fills:
        raw_source_ids = _mapping(item).get("source_event_ids")
        if isinstance(raw_source_ids, list):
            source_event_ids.update(str(source_id) for source_id in raw_source_ids)
    avg_price = _optional_decimal(result.get("avg_fill_price"))
    return {
        "prediction_status": "SCORED",
        "scored": True,
        "status": _upper(result.get("status")),
        "reason": _text(result.get("reason")),
        "predicted_class": predicted_class,
        "actual_class": sample.actual_class,
        "predicted_positive": predicted_positive,
        "actual_positive": sample.any_fill_label,
        "binary_match": predicted_positive == sample.any_fill_label,
        "terminal_class_match": predicted_class == sample.actual_class,
        "filled_size": _decimal_text(filled_size),
        "actual_filled_size": _decimal_text(sample.actual_filled_size),
        "filled_size_error": _decimal_text(filled_size - sample.actual_filled_size),
        "avg_fill_price": _optional_decimal_text(avg_price),
        "actual_avg_fill_price": _optional_decimal_text(
            sample.actual_avg_fill_price
        ),
        "price_abs_error": _optional_decimal_text(
            abs(avg_price - sample.actual_avg_fill_price)
            if avg_price is not None and sample.actual_avg_fill_price is not None
            else None
        ),
        "fee": _decimal_text(fee),
        "actual_fee": _optional_decimal_text(sample.actual_fee),
        "fee_abs_error": _optional_decimal_text(
            abs(fee - sample.actual_fee) if sample.actual_fee is not None else None
        ),
        "execution_model": "PREDICTION_L2_REPLAY_V1",
        "amount_unit": sample.amount_unit,
        "checkpoint_hydrated": bool(prediction.get("checkpoint_hydrated")),
        "admission_diagnostics": result.get("admission_diagnostics") or [],
        "source_event_ids": sorted(source_event_ids),
    }


def _paper_l2_metrics(samples: list[V3LiveCalibrationSample]) -> dict[str, Any]:
    rows = [
        sample
        for sample in samples
        if sample.execution_label_eligible and sample.paper_status
    ]
    if not rows:
        return {
            "scored_orders": 0,
            "model_versions": {},
            "binary_accuracy": None,
            "terminal_class_accuracy": None,
        }
    binary_matches = sum(
        int(sample.paper_filled_size > 0) == sample.any_fill_label
        for sample in rows
    )
    class_matches = sum(
        sample.paper_class == sample.actual_class for sample in rows
    )
    return {
        "scored_orders": len(rows),
        "abstained_orders": sum(sample.execution_label_eligible for sample in samples)
        - len(rows),
        "model_versions": dict(
            sorted(
                Counter(
                    sample.paper_model_version or "UNRECORDED" for sample in rows
                ).items()
            )
        ),
        "predicted_positive_orders": sum(
            sample.paper_filled_size > 0 for sample in rows
        ),
        "actual_positive_orders": sum(sample.any_fill_label for sample in rows),
        "binary_matches": binary_matches,
        "binary_accuracy": round(binary_matches / len(rows), 10),
        "terminal_class_matches": class_matches,
        "terminal_class_accuracy": round(class_matches / len(rows), 10),
        "predicted_total_filled_size": _decimal_text(
            sum((sample.paper_filled_size for sample in rows), Decimal(0))
        ),
        "actual_total_filled_size": _decimal_text(
            sum((sample.actual_filled_size for sample in rows), Decimal(0))
        ),
        "filled_size_absolute_error_sum": _decimal_text(
            sum(
                (
                    abs(sample.paper_filled_size - sample.actual_filled_size)
                    for sample in rows
                ),
                Decimal(0),
            )
        ),
        "mean_price_error_ticks": _decimal_mean_text(
            sample.paper_price_error_ticks for sample in rows
        ),
        "max_price_error_ticks": _decimal_max_text(
            sample.paper_price_error_ticks for sample in rows
        ),
        "mean_filled_size_relative_error": _decimal_mean_text(
            sample.paper_filled_size_relative_error for sample in rows
        ),
        "max_filled_size_relative_error": _decimal_max_text(
            sample.paper_filled_size_relative_error for sample in rows
        ),
        "mean_fee_error": _decimal_mean_text(
            sample.paper_fee_error for sample in rows
        ),
        "mechanical_match_rate": round(
            sum(sample.paper_live_verdict == "MATCH" for sample in rows)
            / len(rows),
            10,
        ),
        "note": "Original Paper-Live L2 engine; this is not the PML2 runtime.",
    }


def _pml2_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    eligible = [row for row in rows if row.get("execution_label_eligible")]
    scored_rows = [row for row in eligible if row["pml2_replay"].get("scored")]
    scored = [row["pml2_replay"] for row in scored_rows]
    abstained = [
        row["pml2_replay"]
        for row in eligible
        if not row["pml2_replay"].get("scored")
    ]
    if not scored:
        return {
            "scored_orders": 0,
            "abstained_orders": len(abstained),
            "scored_coverage": 0.0,
            "abstention_reasons": dict(
                sorted(
                    Counter(
                        _text(row.get("prediction_error")) or "UNKNOWN"
                        for row in abstained
                    ).items()
                )
            ),
            "binary_accuracy": None,
            "terminal_class_accuracy": None,
        }
    true_positive = sum(
        row["predicted_positive"] == 1 and row["actual_positive"] == 1
        for row in scored
    )
    true_negative = sum(
        row["predicted_positive"] == 0 and row["actual_positive"] == 0
        for row in scored
    )
    false_positive = sum(
        row["predicted_positive"] == 1 and row["actual_positive"] == 0
        for row in scored
    )
    false_negative = sum(
        row["predicted_positive"] == 0 and row["actual_positive"] == 1
        for row in scored
    )
    binary_matches = true_positive + true_negative
    class_matches = sum(row["terminal_class_match"] for row in scored)
    predicted_size = sum(
        (_decimal(row["filled_size"]) for row in scored), Decimal(0)
    )
    actual_size = sum(
        (_decimal(row["actual_filled_size"]) for row in scored), Decimal(0)
    )
    return {
        "scored_orders": len(scored),
        "abstained_orders": len(abstained),
        "scored_coverage": round(len(scored) / len(eligible), 10) if eligible else 0.0,
        "abstention_reasons": dict(
            sorted(
                Counter(
                    _text(row.get("prediction_error")) or "UNKNOWN"
                    for row in abstained
                ).items()
            )
        ),
        "checkpoint_hydrated_orders": sum(
            bool(row.get("checkpoint_hydrated")) for row in scored
        ),
        "predicted_positive_orders": sum(row["predicted_positive"] for row in scored),
        "actual_positive_orders": sum(row["actual_positive"] for row in scored),
        "binary_confusion": {
            "true_positive": true_positive,
            "true_negative": true_negative,
            "false_positive": false_positive,
            "false_negative": false_negative,
        },
        "binary_matches": binary_matches,
        "binary_accuracy": round(binary_matches / len(scored), 10),
        "terminal_class_matches": class_matches,
        "terminal_class_accuracy": round(class_matches / len(scored), 10),
        "predicted_total_filled_size": _decimal_text(predicted_size),
        "actual_total_filled_size": _decimal_text(actual_size),
        "filled_size_absolute_error_sum": _decimal_text(
            sum(
                (abs(_decimal(row["filled_size_error"])) for row in scored),
                Decimal(0),
            )
        ),
        "mean_price_abs_error": _decimal_mean_text(
            _optional_decimal(row.get("price_abs_error")) for row in scored
        ),
        "max_price_abs_error": _decimal_max_text(
            _optional_decimal(row.get("price_abs_error")) for row in scored
        ),
        "mean_fee_abs_error": _decimal_mean_text(
            _optional_decimal(row.get("fee_abs_error")) for row in scored
        ),
        "amount_units": dict(
            sorted(Counter(_upper(row.get("amount_unit")) for row in scored_rows).items())
        ),
        "min_order_size_contract_conflicts": sum(
            "MIN_ORDER_SIZE_CONTRACT_CONFLICT"
            in (row.get("admission_diagnostics") or [])
            for row in scored
        ),
        "supported_domain": (
            "QUOTE_BUY_AND_SHARE_ORDERS_WITH_CAPTURED_FULL_OPPOSITE_LEVELS"
        ),
        "quote_buy_orders_silently_converted_to_shares": False,
    }


def _probability_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "brier_score": None,
            "log_loss": None,
            "base_rate": None,
            "expected_positive_orders": None,
            "actual_positive_orders": 0,
            "expected_fill_fraction_sum": None,
            "actual_fill_fraction_sum": None,
            "fill_fraction_mae": None,
        }
    probabilities = [float(row["score_probability"]) for row in rows]
    labels = [int(row["label"]) for row in rows]
    fill_errors = [abs(float(row["fill_fraction_error"])) for row in rows]
    base_rate = sum(labels) / len(labels)
    brier = sum((p - y) ** 2 for p, y in zip(probabilities, labels, strict=True)) / len(
        labels
    )
    epsilon = 1e-12
    log_loss = -sum(
        y * math.log(max(epsilon, min(1 - epsilon, p)))
        + (1 - y) * math.log(max(epsilon, min(1 - epsilon, 1 - p)))
        for p, y in zip(probabilities, labels, strict=True)
    ) / len(labels)
    expected_fraction = sum(float(row["expected_fill_fraction"]) for row in rows)
    actual_fraction = sum(float(row["actual_fill_fraction"]) for row in rows)
    return {
        "brier_score": round(brier, 10),
        "base_rate_brier_score": round(base_rate * (1 - base_rate), 10),
        "brier_regret_vs_base_rate": round(
            brier - base_rate * (1 - base_rate), 10
        ),
        "log_loss": round(log_loss, 10),
        "base_rate": round(base_rate, 10),
        "base_rate_wilson_95": wilson_interval(sum(labels), len(labels)),
        "mean_predicted_probability": round(sum(probabilities) / len(labels), 10),
        "calibration_in_the_large": round(
            sum(probabilities) / len(labels) - base_rate, 10
        ),
        "expected_positive_orders": round(sum(probabilities), 10),
        "actual_positive_orders": sum(labels),
        "expected_fill_fraction_sum": round(expected_fraction, 10),
        "actual_fill_fraction_sum": round(actual_fraction, 10),
        "fill_fraction_mae": round(sum(fill_errors) / len(fill_errors), 10),
        "calibration_bins": _calibration_bins(rows),
    }


def _calibration_bins(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bins: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        probability = float(row["score_probability"])
        bins[min(4, int(probability * 5))].append(row)
    result = []
    for index in range(5):
        members = bins.get(index, [])
        if not members:
            continue
        result.append(
            {
                "lower": index / 5,
                "upper": (index + 1) / 5,
                "samples": len(members),
                "mean_probability": round(
                    sum(float(row["score_probability"]) for row in members)
                    / len(members),
                    10,
                ),
                "observed_rate": round(
                    sum(int(row["label"]) for row in members) / len(members), 10
                ),
            }
        )
    return result


def _group_metrics(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[_text(row.get(key)) or "UNKNOWN"].append(row)
    return {
        name: {"samples": len(members), **_probability_metrics(members)}
        for name, members in sorted(groups.items())
    }


def _latency_metrics(samples: list[V3LiveCalibrationSample]) -> dict[str, Any]:
    values = sorted(
        float(sample.decision_to_arrival_seconds)
        for sample in samples
        if sample.decision_to_arrival_seconds is not None
        and sample.decision_to_arrival_seconds >= 0
    )
    if not values:
        return {"samples": 0, "median": None, "p90": None, "p95": None, "max": None}

    def quantile(q: float) -> float:
        index = round((len(values) - 1) * q)
        return round(values[index], 10)

    return {
        "samples": len(values),
        "median": quantile(0.50),
        "p90": quantile(0.90),
        "p95": quantile(0.95),
        "max": round(values[-1], 10),
        "note": "end-to-end probe delay; not exchange network latency alone",
    }


def _execution_label_eligibility(
    *,
    actual_class: str,
    side: str,
    tif: str,
    limit_price: Decimal,
    requested_size: Decimal,
    arrival_ts: datetime | None,
    probe_state: str,
    rejection_reason: str,
    exchange_submit_called: Any,
) -> tuple[bool, str]:
    missing = []
    if side not in {"BUY", "SELL"}:
        missing.append("side")
    if tif not in {"FAK", "FOK"}:
        missing.append("supported_tif")
    if not (Decimal(0) < limit_price <= Decimal(1)):
        missing.append("limit_price")
    if requested_size <= 0:
        missing.append("requested_size")
    if arrival_ts is None:
        missing.append("arrival_ts")
    if probe_state and probe_state != "CALIBRATABLE":
        return False, f"probe_state_{probe_state.lower()}"
    if missing:
        return False, "missing_or_invalid_" + ",".join(missing)
    if actual_class in {"FULL", "PARTIAL", "NO_FILL"}:
        return True, ""
    if actual_class == "REJECT":
        is_liquidity_reject = tif == "FOK" and any(
            marker in rejection_reason.lower()
            for marker in LIQUIDITY_REJECTION_MARKERS
        )
        if is_liquidity_reject and exchange_submit_called is not False:
            return True, ""
        return False, "admission_rejection_not_execution_no_fill"
    return False, f"unsupported_actual_class_{actual_class.lower() or 'unknown'}"


def _arrival_time(
    real: Mapping[str, Any],
    audit: Mapping[str, Any],
    rest_order: Mapping[str, Any],
) -> tuple[datetime | None, str]:
    created_at = rest_order.get("created_at")
    if created_at not in (None, ""):
        return _epoch_datetime(created_at, milliseconds=False), "rest_order.created_at"
    rest_trades = real.get("rest_trades")
    if isinstance(rest_trades, list):
        match_times: list[float] = []
        for row in rest_trades:
            value = _mapping(row).get("match_time")
            try:
                match_times.append(float(str(value)))
            except (TypeError, ValueError):
                continue
        if match_times:
            return _epoch_datetime(min(match_times), milliseconds=False), "rest_trade.match_time"
    timestamp = audit.get("timestamp")
    if timestamp not in (None, ""):
        return _epoch_datetime(timestamp, milliseconds=True), "signed_order_audit.timestamp"
    signed_at = _datetime(audit.get("signed_at"))
    return signed_at, "signed_order_audit.signed_at" if signed_at else ""


def _requested_shares(
    *,
    side: str,
    real: Mapping[str, Any],
    audit: Mapping[str, Any],
    rest_order: Mapping[str, Any],
    paper: Mapping[str, Any],
    paper_order: Mapping[str, Any],
    prediction: Mapping[str, Any],
    limit_price: Decimal,
) -> Decimal:
    original_size = _optional_decimal(rest_order.get("original_size"))
    if original_size is not None:
        return original_size
    raw_units = (
        audit.get("taker_amount") if side == "BUY" else audit.get("maker_amount")
    )
    raw_shares = _optional_decimal(raw_units)
    if raw_shares is not None:
        return raw_shares / Decimal(1_000_000)
    signed_shares = _optional_decimal(prediction.get("signed_share_amount"))
    if signed_shares is not None:
        return signed_shares
    amount = _optional_decimal(
        real.get("requested_amount")
        or paper.get("requested_amount")
        or paper_order.get("amount")
        or prediction.get("requested_amount")
    )
    unit = _upper(
        real.get("amount_unit")
        or paper.get("amount_unit")
        or paper_order.get("amount_unit")
        or prediction.get("amount_unit")
    )
    if amount is None:
        return Decimal(0)
    if unit == "QUOTE" and limit_price > 0:
        return amount / limit_price
    return amount


def _normalized_signed_amounts(
    *,
    side: str,
    audit: Mapping[str, Any],
    prediction: Mapping[str, Any],
) -> tuple[Decimal | None, Decimal | None]:
    quote = _optional_decimal(prediction.get("signed_quote_amount"))
    shares = _optional_decimal(prediction.get("signed_share_amount"))
    if quote is not None and shares is not None:
        return quote, shares
    maker = _optional_decimal(audit.get("maker_amount"))
    taker = _optional_decimal(audit.get("taker_amount"))
    if maker is None or taker is None or maker <= 0 or taker <= 0:
        return None, None
    scale = Decimal(1_000_000)
    if side == "BUY":
        return maker / scale, taker / scale
    if side == "SELL":
        return taker / scale, maker / scale
    return None, None


def _sum_trade_sizes(value: Any) -> Decimal:
    if not isinstance(value, list):
        return Decimal(0)
    return sum((_decimal(_mapping(row).get("size")) for row in value), Decimal(0))


def _canonical_actual_class(value: Any) -> str:
    text = _upper(value).replace("PARTIAL_FILLED", "PARTIAL")
    if text in {"FILLED", "MATCHED"}:
        return "FULL"
    if text in {"REJECTED", "FAILED"}:
        return "REJECT"
    if text in {"UNFILLED", "CANCELED", "CANCELLED", "EXPIRED"}:
        return "NO_FILL"
    return text or "UNKNOWN"


def _canonical_prediction_class(
    status: Any,
    filled_size: Decimal,
    requested_size: Decimal,
) -> str:
    text = _upper(status).replace("PARTIAL_FILLED", "PARTIAL")
    if text in {"REJECTED", "REJECT", "FAILED"}:
        return "REJECT"
    if filled_size > 0:
        if text == "PARTIAL" or (requested_size > 0 and filled_size < requested_size):
            return "PARTIAL"
        return "FULL"
    if text in {"FILLED", "MATCHED"}:
        return "FULL"
    return "NO_FILL"


def _outcome_role(snapshot: Mapping[str, Any], asset_id: str) -> str:
    literal = _upper(snapshot.get("outcome_name"))
    if literal in {"YES", "NO"}:
        return literal
    raw_assets = _mapping(snapshot.get("raw_metadata")).get("assets")
    if isinstance(raw_assets, list):
        for value in raw_assets:
            row = _mapping(value)
            if _text(row.get("asset_id")) != asset_id:
                continue
            index = row.get("outcome_index")
            if str(index) == "0":
                return "YES"
            if str(index) == "1":
                return "NO"
    tokens = _mapping(snapshot.get("clob_market_info")).get("t")
    if isinstance(tokens, list):
        for index, value in enumerate(tokens):
            row = _mapping(value)
            if _text(row.get("t")) != asset_id:
                continue
            token_outcome = _upper(row.get("o"))
            if token_outcome in {"YES", "NO"}:
                return token_outcome
            if index in {0, 1}:
                return "YES" if index == 0 else "NO"
    return "UNKNOWN"


def _snapshot_levels(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    result: list[dict[str, str]] = []
    for item in value:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            result.append({"price": str(item[0]), "size": str(item[1])})
            continue
        row = _mapping(item)
        if row.get("price") not in (None, "") and row.get("size") not in (
            None,
            "",
        ):
            result.append({"price": str(row["price"]), "size": str(row["size"])})
    return result


def _decimal_text(value: Decimal) -> str:
    return format(value, "f")


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return _decimal_text(value) if value is not None else None


def _decimal_mean_text(values: Iterable[Decimal | None]) -> str | None:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return _decimal_text(sum(present, Decimal(0)) / len(present))


def _decimal_max_text(values: Iterable[Decimal | None]) -> str | None:
    present = [value for value in values if value is not None]
    return _decimal_text(max(present)) if present else None


def _stable_sample_id(payload: Mapping[str, Any], source_path: Path | str) -> str:
    text = json.dumps(
        {
            "path": str(source_path),
            "market": payload.get("market"),
            "trade_time": payload.get("trade_time"),
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return "paper-live-" + hashlib.sha256(text.encode()).hexdigest()[:24]


def _epoch_datetime(value: Any, *, milliseconds: bool) -> datetime | None:
    try:
        number = float(str(value))
    except (TypeError, ValueError):
        return None
    if milliseconds:
        number /= 1_000
    return datetime.fromtimestamp(number, timezone.utc)


def _datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row[key]
    return None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _integer(value: Any, field: str) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is required and must be an integer") from exc


def _decimal(value: Any) -> Decimal:
    parsed = _optional_decimal(value)
    return parsed if parsed is not None else Decimal(0)


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None
