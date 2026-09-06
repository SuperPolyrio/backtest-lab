#!/usr/bin/env python3
"""Validate Fill-only V3 probabilities across disjoint real-data windows."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.probability_quality import (  # noqa: E402
    adaptive_probability_quality,
)
from scripts import batch_compare_real_execution_models as comparison  # noqa: E402

_QUALITY_STATUS_RANK = {
    "PASS": 0,
    "INCONCLUSIVE": 1,
    "INSUFFICIENT_SAMPLE": 2,
    "FAIL": 3,
}


def combine_quality_statuses(*statuses: str) -> str:
    """Return the most conservative status without collapsing uncertainty to FAIL."""

    if not statuses:
        return "INSUFFICIENT_SAMPLE"
    unknown = sorted(set(statuses) - _QUALITY_STATUS_RANK.keys())
    if unknown:
        raise ValueError(f"unknown quality status: {unknown}")
    return max(statuses, key=_QUALITY_STATUS_RANK.__getitem__)


def _load_rows(summary_path: Path) -> list[dict[str, Any]]:
    orders_path = summary_path.with_name("orders.jsonl")
    return [
        json.loads(line)
        for line in orders_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _inside(value: float, lower: float, upper: float) -> bool:
    return lower <= value <= upper


def _ratio(numerator: Decimal | int, denominator: Decimal | int) -> float | None:
    numerator_decimal = Decimal(str(numerator))
    denominator_decimal = Decimal(str(denominator))
    return (
        float(numerator_decimal / denominator_decimal) if denominator_decimal else None
    )


def _brier_regret(brier: float, positive_rate: float) -> float:
    return brier - positive_rate * (1.0 - positive_rate)


def _passes_brier(brier: float, positive_rate: float, args: argparse.Namespace) -> bool:
    return (
        brier <= args.maximum_brier_score
        and _brier_regret(brier, positive_rate) <= args.maximum_brier_regret
    )


def _quality_observations(
    rows: list[dict[str, Any]], *, model: str, reference: str
) -> dict[str, list[Any]]:
    observations: dict[str, list[Any]] = {
        "probabilities": [],
        "labels": [],
        "expected_quantities": [],
        "reference_quantities": [],
        "quantity_scales": [],
        "cluster_ids": [],
    }
    for row in rows:
        predicted = row["models"][model]
        observed = row["models"][reference]
        if reference.startswith("pml2_") and observed["status"] == "DATA_NOT_READY":
            continue
        if predicted.get("reason") in {
            "orderfilled_probability_model_out_of_domain",
            "probability_model_contract_violation",
        }:
            continue
        decision_ts = str(row.get("decision_ts") or "")
        cluster_id = row.get("cluster_id") or (
            f"{row['market_id']}:{decision_ts[:10]}"
            if decision_ts
            else row["market_id"]
        )
        observed_size = float(observed.get("filled_size") or 0)
        observations["probabilities"].append(
            float(comparison._expected_fill_probability(predicted))
        )
        observations["labels"].append(float(observed_size > 0))
        observations["expected_quantities"].append(
            float(predicted.get("filled_size") or 0)
        )
        observations["reference_quantities"].append(observed_size)
        observations["quantity_scales"].append(
            float(row.get("order_size") or row.get("size") or 1)
        )
        observations["cluster_ids"].append(cluster_id)
    return observations


def _adaptive_quality(
    observations: dict[str, list[Any]], args: argparse.Namespace
) -> dict[str, Any]:
    if not observations["probabilities"]:
        return {
            "schema_version": "adaptive_probability_quality_v2",
            "status": "INSUFFICIENT_SAMPLE",
            "samples": 0,
            "positive_samples": 0,
            "negative_samples": 0,
            "independent_clusters": 0,
            "sample_checks": {"non_empty": False},
            "quality_checks": {},
            "guardrail_checks": {},
        }
    return adaptive_probability_quality(
        observations["probabilities"],
        observations["labels"],
        expected_quantities=observations["expected_quantities"],
        reference_quantities=observations["reference_quantities"],
        quantity_scales=observations["quantity_scales"],
        cluster_ids=observations["cluster_ids"],
        bootstrap_replicates=int(getattr(args, "bootstrap_replicates", 1_000)),
        confidence_level=float(getattr(args, "confidence_level", 0.95)),
        random_seed=int(getattr(args, "bootstrap_seed", 73)),
        minimum_samples=int(getattr(args, "minimum_adaptive_samples", 200)),
        minimum_positive_samples=int(
            getattr(args, "minimum_adaptive_positive_samples", 20)
        ),
        minimum_negative_samples=int(
            getattr(args, "minimum_adaptive_negative_samples", 20)
        ),
        minimum_clusters=int(getattr(args, "minimum_adaptive_clusters", 20)),
    )


def validate(args: argparse.Namespace) -> dict[str, Any]:
    quality_gate_mode = str(getattr(args, "quality_gate_mode", "fixed")).lower()
    summaries: list[tuple[Path, dict[str, Any]]] = []
    for path in args.summaries:
        summaries.append((path, json.loads(path.read_text(encoding="utf-8"))))

    fingerprints = [str(row["cohort"]["fingerprint"]) for _, row in summaries]
    failures: list[str] = []
    inconclusive_reasons: list[str] = []
    insufficient_sample_reasons: list[str] = []
    total_orders = sum(int(row["cohort"]["orders"]) for _, row in summaries)
    if total_orders < args.minimum_total_orders:
        failures.append(
            f"total orders {total_orders} below minimum {args.minimum_total_orders}"
        )
    if len(summaries) < args.minimum_windows:
        failures.append(
            f"successful windows {len(summaries)} below minimum {args.minimum_windows}"
        )
    if len(fingerprints) != len(set(fingerprints)):
        failures.append("duplicate cohort fingerprint")

    windows = sorted(
        (
            datetime.fromisoformat(str(row["cohort"]["start"])),
            datetime.fromisoformat(str(row["cohort"]["end"])),
            path,
        )
        for path, row in summaries
    )
    for previous, current in pairwise(windows):
        if current[0] <= previous[1]:
            failures.append(f"overlapping cohorts: {previous[2]} and {current[2]}")

    cohort_results: list[dict[str, Any]] = []
    aggregate: dict[str, dict[str, Decimal | int]] = {
        reference: {
            "reference_ready_samples": 0,
            "supported_samples": 0,
            "expected_orders": Decimal(0),
            "reference_orders": Decimal(0),
            "expected_quantity": Decimal(0),
            "reference_quantity": Decimal(0),
            "weighted_brier": Decimal(0),
            "all_expected_orders": Decimal(0),
            "all_reference_orders": Decimal(0),
            "all_expected_quantity": Decimal(0),
            "all_reference_quantity": Decimal(0),
        }
        for reference in args.references
    }
    aggregate_observations: dict[str, dict[str, list[Any]]] = {
        reference: {
            "probabilities": [],
            "labels": [],
            "expected_quantities": [],
            "reference_quantities": [],
            "quantity_scales": [],
            "cluster_ids": [],
        }
        for reference in args.references
    }

    for path, summary in summaries:
        rows = _load_rows(path)
        comparisons: dict[str, Any] = {}
        for reference in args.references:
            metrics = comparison._binary_reference_metrics(
                rows, model=args.model, reference=reference
            )
            comparisons[reference] = metrics
            support = float(metrics["model_support_rate"] or 0)
            order_ratio = float(metrics["expected_positive_order_ratio"] or 0)
            quantity_ratio = float(metrics["expected_quantity_ratio"] or 0)
            brier = float(metrics["probability_brier_score"] or 1)
            positive_rate = float(metrics["reference_positive_rate"] or 0)
            brier_regret = _brier_regret(brier, positive_rate)
            metrics["climatology_brier_score"] = positive_rate * (1.0 - positive_rate)
            metrics["brier_regret"] = brier_regret
            observations = _quality_observations(
                rows, model=args.model, reference=reference
            )
            adaptive = _adaptive_quality(observations, args)
            metrics["adaptive_probability_quality"] = adaptive
            if quality_gate_mode == "adaptive":
                checks = {
                    "minimum_support_rate": support >= args.minimum_support_rate,
                    "no_window_level_calibration_failure": (
                        adaptive["status"] != "FAIL"
                    ),
                }
            else:
                checks = {
                    "minimum_support_rate": support >= args.minimum_support_rate,
                    "expected_order_ratio": _inside(
                        order_ratio, args.minimum_ratio, args.maximum_ratio
                    ),
                    "expected_quantity_ratio": _inside(
                        quantity_ratio, args.minimum_ratio, args.maximum_ratio
                    ),
                    "maximum_brier_score_and_regret": _passes_brier(
                        brier, positive_rate, args
                    ),
                }
            metrics["gates"] = checks
            for name, passed in checks.items():
                if not passed:
                    failures.append(f"{path}:{reference}:{name}")
            if quality_gate_mode == "adaptive":
                adaptive_status = str(adaptive["status"])
                if adaptive_status == "INCONCLUSIVE":
                    inconclusive_reasons.append(f"{path}:{reference}")
                elif adaptive_status == "INSUFFICIENT_SAMPLE":
                    insufficient_sample_reasons.append(f"{path}:{reference}")

            target = aggregate[reference]
            samples = int(metrics["samples"])
            target["reference_ready_samples"] += int(metrics["reference_ready_samples"])
            target["supported_samples"] += samples
            target["expected_orders"] += Decimal(
                str(metrics["expected_positive_orders"])
            )
            target["reference_orders"] += Decimal(
                str(metrics["reference_positive_orders"])
            )
            target["expected_quantity"] += Decimal(str(metrics["expected_quantity"]))
            target["reference_quantity"] += Decimal(str(metrics["reference_quantity"]))
            target["weighted_brier"] += Decimal(
                str(metrics["probability_brier_score"])
            ) * Decimal(samples)
            target["all_expected_orders"] += Decimal(
                str(metrics["all_sample_expected_positive_orders"])
            )
            target["all_reference_orders"] += Decimal(
                str(metrics["all_sample_reference_positive_orders"])
            )
            target["all_expected_quantity"] += Decimal(
                str(metrics["all_sample_expected_quantity"])
            )
            target["all_reference_quantity"] += Decimal(
                str(metrics["all_sample_reference_quantity"])
            )
            for name, observation_values in observations.items():
                aggregate_observations[reference][name].extend(observation_values)

        cohort_results.append(
            {
                "summary": str(path),
                "fingerprint": summary["cohort"]["fingerprint"],
                "start": summary["cohort"]["start"],
                "end": summary["cohort"]["end"],
                "orders": summary["cohort"]["orders"],
                "comparisons": comparisons,
            }
        )

    aggregate_results: dict[str, Any] = {}
    for reference, aggregate_values in aggregate.items():
        supported = int(aggregate_values["supported_samples"])
        ready = int(aggregate_values["reference_ready_samples"])
        expected_order_ratio = _ratio(
            aggregate_values["expected_orders"],
            aggregate_values["reference_orders"],
        )
        expected_quantity_ratio = _ratio(
            aggregate_values["expected_quantity"],
            aggregate_values["reference_quantity"],
        )
        brier_score: float | None = (
            float(aggregate_values["weighted_brier"] / Decimal(supported))
            if supported
            else None
        )
        support_rate = supported / ready if ready else None
        positive_rate = (
            float(Decimal(aggregate_values["reference_orders"]) / Decimal(supported))
            if supported
            else 0.0
        )
        climatology_brier_score = positive_rate * (1.0 - positive_rate)
        aggregate_brier_regret = (
            _brier_regret(brier_score, positive_rate)
            if brier_score is not None
            else None
        )
        checks = {
            "minimum_support_rate": (
                support_rate is not None and support_rate >= args.minimum_support_rate
            ),
            "expected_order_ratio": (
                expected_order_ratio is not None
                and _inside(
                    expected_order_ratio, args.minimum_ratio, args.maximum_ratio
                )
            ),
            "expected_quantity_ratio": (
                expected_quantity_ratio is not None
                and _inside(
                    expected_quantity_ratio, args.minimum_ratio, args.maximum_ratio
                )
            ),
            "maximum_brier_score_and_regret": (
                brier_score is not None
                and _passes_brier(brier_score, positive_rate, args)
            ),
        }
        adaptive = _adaptive_quality(aggregate_observations[reference], args)
        if quality_gate_mode == "adaptive":
            checks = {
                "minimum_support_rate": (
                    support_rate is not None
                    and support_rate >= args.minimum_support_rate
                ),
                "no_detected_adaptive_quality_failure": adaptive["status"] != "FAIL",
            }
        for name, passed in checks.items():
            if not passed:
                failures.append(f"aggregate:{reference}:{name}")
        if quality_gate_mode == "adaptive":
            adaptive_status = str(adaptive["status"])
            if adaptive_status == "INCONCLUSIVE":
                inconclusive_reasons.append(f"aggregate:{reference}")
            elif adaptive_status == "INSUFFICIENT_SAMPLE":
                insufficient_sample_reasons.append(f"aggregate:{reference}")
        aggregate_results[reference] = {
            "reference_ready_samples": ready,
            "supported_samples": supported,
            "support_rate": support_rate,
            "expected_positive_order_ratio": expected_order_ratio,
            "expected_quantity_ratio": expected_quantity_ratio,
            "probability_brier_score": brier_score,
            "climatology_brier_score": climatology_brier_score,
            "brier_regret": aggregate_brier_regret,
            "adaptive_probability_quality": adaptive,
            "all_sample_expected_positive_order_ratio": _ratio(
                aggregate_values["all_expected_orders"],
                aggregate_values["all_reference_orders"],
            ),
            "all_sample_expected_quantity_ratio": _ratio(
                aggregate_values["all_expected_quantity"],
                aggregate_values["all_reference_quantity"],
            ),
            "gates": checks,
        }

    if failures:
        status = "FAIL"
    elif quality_gate_mode == "adaptive":
        aggregate_status = combine_quality_statuses(
            *(
                str(row["adaptive_probability_quality"]["status"])
                for row in aggregate_results.values()
            )
        )
        status = aggregate_status
    else:
        status = "PASS"

    if quality_gate_mode == "adaptive":
        quality_gates = {
            "mode": "adaptive",
            "reference": "MATCHING_CONTRACT_SAMPLE_CLIMATOLOGY",
            "decision_rule": (
                "PAIRED_MARKET_DAY_CLUSTER_BOOTSTRAP; PASS requires positive "
                "reference-relative Brier skill, no detected log-loss harm, "
                "and confidence intervals covering zero calibration bias and "
                "unit aggregate ratios"
            ),
            "minimum_total_orders": args.minimum_total_orders,
            "minimum_windows": args.minimum_windows,
            "minimum_support_rate": args.minimum_support_rate,
            "confidence_level": float(getattr(args, "confidence_level", 0.95)),
            "bootstrap_replicates": int(getattr(args, "bootstrap_replicates", 1_000)),
            "minimum_adaptive_samples": int(
                getattr(args, "minimum_adaptive_samples", 200)
            ),
            "minimum_adaptive_positive_samples": int(
                getattr(args, "minimum_adaptive_positive_samples", 20)
            ),
            "minimum_adaptive_negative_samples": int(
                getattr(args, "minimum_adaptive_negative_samples", 20)
            ),
            "minimum_adaptive_clusters": int(
                getattr(args, "minimum_adaptive_clusters", 20)
            ),
            "legacy_fixed_thresholds_applied": False,
        }
    else:
        quality_gates = {
            "mode": "fixed",
            "minimum_total_orders": args.minimum_total_orders,
            "minimum_windows": args.minimum_windows,
            "minimum_support_rate": args.minimum_support_rate,
            "minimum_ratio": args.minimum_ratio,
            "maximum_ratio": args.maximum_ratio,
            "maximum_brier_score": args.maximum_brier_score,
            "maximum_brier_regret": args.maximum_brier_regret,
            "legacy_fixed_thresholds_applied": True,
        }

    return {
        "schema_version": "fill_only_v3_cross_window_stability_v3",
        "status": status,
        "model": args.model,
        "sample": {
            "successful_windows": len(summaries),
            "total_orders": total_orders,
        },
        "quality_gates": quality_gates,
        "cohorts": cohort_results,
        "aggregate": aggregate_results,
        "failures": failures,
        "inconclusive_reasons": inconclusive_reasons,
        "insufficient_sample_reasons": insufficient_sample_reasons,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summaries", type=Path, nargs="+")
    parser.add_argument("--model", default="v3_l2_expected")
    parser.add_argument("--references", nargs="+", default=["pml2_fak", "nautilus_fak"])
    parser.add_argument("--minimum-support-rate", type=float, default=0.80)
    parser.add_argument("--minimum-total-orders", type=int, default=10_001)
    parser.add_argument("--minimum-windows", type=int, default=10)
    parser.add_argument("--minimum-ratio", type=float, default=0.85)
    parser.add_argument("--maximum-ratio", type=float, default=1.15)
    parser.add_argument("--maximum-brier-score", type=float, default=0.20)
    parser.add_argument("--maximum-brier-regret", type=float, default=0.01)
    parser.add_argument(
        "--quality-gate-mode",
        choices=("adaptive", "fixed"),
        default="adaptive",
    )
    parser.add_argument("--confidence-level", type=float, default=0.95)
    parser.add_argument("--bootstrap-replicates", type=int, default=1_000)
    parser.add_argument("--bootstrap-seed", type=int, default=73)
    parser.add_argument("--minimum-adaptive-samples", type=int, default=200)
    parser.add_argument("--minimum-adaptive-positive-samples", type=int, default=20)
    parser.add_argument("--minimum-adaptive-negative-samples", type=int, default=20)
    parser.add_argument("--minimum-adaptive-clusters", type=int, default=20)
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result = validate(args)
    payload = json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
