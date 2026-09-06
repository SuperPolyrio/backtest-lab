#!/usr/bin/env python3
"""Validate one immutable Fill-only V3 artifact on saved order-level labels."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.probability_quality import (  # noqa: E402
    adaptive_probability_quality,
)
from scripts.calibrate_fill_only_v3_hierarchical_overlay import (  # noqa: E402
    CellStats,
    _base_probability,
    _category,
    _conditional_fraction,
    _evaluate,
    _family,
    _family_brier_baseline,
    _load,
)

STRATUM_DIMENSIONS = (
    "category",
    "price_bucket",
    "activity_regime",
    "side",
    "order_size_bucket",
    "depth_regime",
    "spread_regime",
)
GATED_STRATUM_DIMENSIONS = (
    "category",
    "price_bucket",
    "activity_regime",
    "side",
    "order_size_bucket",
)


def _orders_path(path: Path) -> Path:
    return path / "orders.jsonl" if path.is_dir() else path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_cells(artifact: Mapping[str, Any]) -> dict[str, CellStats]:
    raw = (artifact.get("hierarchical_probability") or {}).get("cells") or {}
    return {
        str(key): (
            int(value.get("positive_samples") or 0),
            int(value.get("samples") or 0),
            int(value.get("independent_windows") or 0),
            int(value.get("unique_markets") or 0),
            float(value.get("conditional_fill_fraction") or 0)
            * int(value.get("positive_samples") or 0),
        )
        for key, value in raw.items()
    }


def _prepare_rows(
    paths: Sequence[Path], artifact: Mapping[str, Any], reference: str
) -> list[dict[str, Any]]:
    hierarchy = artifact.get("hierarchical_probability") or {}
    rows = _load(
        paths,
        reference_model=reference,
        key_scheme=str(hierarchy.get("key_scheme") or "legacy_v1"),
    )
    identities = [row["identity"] for row in rows]
    if len(identities) != len(set(identities)):
        raise RuntimeError(f"{reference} inputs contain duplicate order identities")
    for row in rows:
        row["base_probability"] = _base_probability(row, artifact)
        runtime_probability = row.get("runtime_base_probability")
        if (
            runtime_probability is not None
            and row.get("runtime_probability_model_version")
            == artifact.get("model_version")
            and abs(row["base_probability"] - float(runtime_probability)) > 1e-8
        ):
            raise RuntimeError(
                "saved features do not reproduce runtime probability for "
                f"{row['identity']}: saved={row['base_probability']}, "
                f"runtime={runtime_probability}"
            )
        row["conditional_fraction_model"] = _conditional_fraction(row, artifact)
    return rows


def _domain_supported(row: Mapping[str, Any], artifact: Mapping[str, Any]) -> bool:
    if row.get("source_positive"):
        return True
    domain = artifact.get("domain_gate") or {}
    dimensions = row.get("dimensions") or {}
    contract = artifact.get("model_contract") or {}
    sides = {str(value).upper() for value in contract.get("supported_sides") or ()}
    tifs = {str(value).upper() for value in contract.get("supported_tifs") or ()}
    amount_units = {
        str(value).upper() for value in contract.get("supported_amount_units") or ()
    }
    side = str(dimensions.get("side") or "UNKNOWN").upper()
    tif = str(dimensions.get("tif") or "FAK").upper()
    amount_unit = str(dimensions.get("amount_unit") or "SHARES").upper()
    size = float(dimensions.get("order_size") or 0)
    log_ratio = float(dimensions.get("log_order_to_tape_ratio") or 0)
    return (
        _category(row) not in set(domain.get("abstain_categories") or ())
        and _family(row) not in set(domain.get("abstain_category_families") or ())
        and str(dimensions.get("activity_regime") or "")
        not in set(domain.get("abstain_activity_regimes") or ())
        and (not sides or side in sides)
        and (not tifs or tif in tifs)
        and (not amount_units or amount_unit in amount_units)
        and (
            contract.get("minimum_order_size") is None
            or size >= float(contract["minimum_order_size"])
        )
        and (
            contract.get("maximum_order_size") is None
            or size <= float(contract["maximum_order_size"])
        )
        and (
            contract.get("minimum_log_order_to_tape_ratio") is None
            or log_ratio >= float(contract["minimum_log_order_to_tape_ratio"])
        )
        and (
            contract.get("maximum_log_order_to_tape_ratio") is None
            or log_ratio <= float(contract["maximum_log_order_to_tape_ratio"])
        )
    )


def _evaluate_rows(
    rows: Sequence[Mapping[str, Any]],
    artifact: Mapping[str, Any],
    *,
    supported_only: bool = True,
    include_observations: bool = False,
) -> dict[str, Any]:
    selected_rows = (
        [row for row in rows if _domain_supported(row, artifact)]
        if supported_only
        else list(rows)
    )
    if not selected_rows:
        raise RuntimeError("probability artifact supports no validation rows")
    domain = artifact.get("domain_gate") or {}
    abstain_categories = set(domain.get("abstain_categories") or ())
    abstain_families = set(domain.get("abstain_category_families") or ())
    abstain_activity_regimes = set(domain.get("abstain_activity_regimes") or ())
    hierarchy = artifact.get("hierarchical_probability")
    if not isinstance(hierarchy, Mapping):
        return _evaluate(
            selected_rows,
            artifact,
            {},
            minimum=sys.maxsize,
            beta_strength=0,
            blend_strength=0,
            global_probability=0.5,
            global_conditional_fraction=1,
            supported_categories=set(),
            supported_families=set(),
            abstain_categories=abstain_categories,
            abstain_families=abstain_families,
            abstain_activity_regimes=abstain_activity_regimes,
            include_observations=include_observations,
        )
    return _evaluate(
        selected_rows,
        artifact,
        _artifact_cells(artifact),
        minimum=int(hierarchy["minimum_samples"]),
        beta_strength=float(hierarchy["beta_strength"]),
        blend_strength=float(hierarchy["blend_strength"]),
        global_probability=float(hierarchy["global_probability"]),
        global_conditional_fraction=float(
            hierarchy.get("global_conditional_fill_fraction") or 1
        ),
        probability_scale=float(hierarchy.get("probability_scale") or 1),
        supported_categories=set(hierarchy.get("supported_categories") or ()),
        supported_families=set(hierarchy.get("supported_category_families") or ()),
        fallback_categories=set(hierarchy.get("fallback_categories") or ()),
        fallback_families=set(hierarchy.get("fallback_category_families") or ()),
        abstain_categories=abstain_categories,
        abstain_families=abstain_families,
        abstain_activity_regimes=abstain_activity_regimes,
        include_observations=include_observations,
    )


def _adaptive_quality(
    metrics: Mapping[str, Any],
    args: argparse.Namespace,
    *,
    minimum_clusters: int | None = None,
) -> dict[str, Any]:
    observations = metrics.get("_observations")
    if not isinstance(observations, Mapping):
        raise RuntimeError("adaptive quality gate requires order-level observations")
    return adaptive_probability_quality(
        observations["probabilities"],
        observations["labels"],
        expected_quantities=observations["expected_fractions"],
        reference_quantities=observations["reference_fractions"],
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
        minimum_clusters=(
            int(minimum_clusters)
            if minimum_clusters is not None
            else int(getattr(args, "minimum_adaptive_clusters", 20))
        ),
    )


def _public_metrics(metrics: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in metrics.items() if key != "_observations"}


def _gate(metrics: Mapping[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    if str(getattr(args, "quality_gate_mode", "adaptive")) == "adaptive":
        quality = _adaptive_quality(metrics, args)
        return {
            "status": quality["status"],
            "decision_rule": quality["decision_rule"],
            "legacy_fixed_thresholds_applied": False,
            "adaptive_probability_quality": quality,
        }

    order_ratio = metrics.get("expected_positive_order_ratio")
    quantity_ratio = metrics.get("expected_quantity_ratio")
    baseline = _family_brier_baseline(metrics)
    regret = float(metrics["brier_score"]) - baseline
    enough_reference = float(metrics["reference_positive_orders"]) > 0
    passed = (
        enough_reference
        and order_ratio is not None
        and quantity_ratio is not None
        and args.minimum_ratio <= float(order_ratio) <= args.maximum_ratio
        and args.minimum_ratio <= float(quantity_ratio) <= args.maximum_ratio
        and float(metrics["brier_score"]) <= args.maximum_brier_score
        and regret <= args.maximum_brier_regret
    )
    return {
        "status": "PASS" if passed else "FAIL",
        "decision_rule": "LEGACY_FIXED_ABSOLUTE_THRESHOLDS",
        "legacy_fixed_thresholds_applied": True,
        "expected_order_ratio": order_ratio,
        "expected_quantity_ratio": quantity_ratio,
        "brier_score": metrics["brier_score"],
        "climatology_brier_score": baseline,
        "brier_regret": regret,
        "reference_positive_orders": metrics["reference_positive_orders"],
    }


def _coverage_gate(
    metrics: Mapping[str, Any],
    args: argparse.Namespace,
    *,
    probability_gate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if str(getattr(args, "quality_gate_mode", "adaptive")) == "adaptive":
        adaptive = (
            (probability_gate or {}).get("adaptive_probability_quality")
            if probability_gate is not None
            else _adaptive_quality(metrics, args)
        )
        if not isinstance(adaptive, Mapping):
            raise RuntimeError("adaptive coverage gate requires probability quality")
        checks = adaptive.get("quality_checks") or {}
        sample_status = str(adaptive.get("status") or "INSUFFICIENT_SAMPLE")
        if sample_status == "INSUFFICIENT_SAMPLE":
            status = "DATA_INSUFFICIENT"
        else:
            status = (
                "PASS"
                if checks.get("expected_order_count")
                and checks.get("expected_quantity")
                else "FAIL"
            )
        return {
            "status": status,
            "decision_rule": "PAIRED_MARKET_DAY_CLUSTER_INTERVAL_CONTAINS_ONE",
            "legacy_fixed_thresholds_applied": False,
            "expected_order_ratio": adaptive.get("expected_positive_order_ratio"),
            "expected_quantity_ratio": adaptive.get("expected_quantity_ratio"),
            "confidence_intervals": {
                name: (adaptive.get("confidence_intervals") or {}).get(name)
                for name in ("expected_order_ratio", "expected_quantity_ratio")
            },
            "quality_checks": {
                name: checks.get(name)
                for name in ("expected_order_count", "expected_quantity")
            },
            "reference_positive_orders": adaptive.get("reference_positive_orders"),
        }

    order_ratio = metrics.get("expected_positive_order_ratio")
    quantity_ratio = metrics.get("expected_quantity_ratio")
    if float(metrics["reference_positive_orders"]) <= 0:
        status = "DATA_INSUFFICIENT"
    elif order_ratio is None or quantity_ratio is None:
        status = "FAIL"
    else:
        status = (
            "PASS"
            if float(order_ratio) >= args.minimum_ratio
            and float(quantity_ratio) >= args.minimum_ratio
            else "FAIL"
        )
    return {
        "status": status,
        "expected_order_ratio": order_ratio,
        "expected_quantity_ratio": quantity_ratio,
        "minimum_ratio": args.minimum_ratio,
        "reference_positive_orders": metrics["reference_positive_orders"],
    }


def _price_bucket(value: Any) -> str:
    price = float(value)
    boundaries = (
        (0.10, "00_0.00_0.10"),
        (0.25, "01_0.10_0.25"),
        (0.40, "02_0.25_0.40"),
        (0.60, "03_0.40_0.60"),
        (0.75, "04_0.60_0.75"),
        (0.90, "05_0.75_0.90"),
        (1.01, "06_0.90_1.00"),
    )
    return next(label for upper, label in boundaries if price < upper)


def _order_size_bucket(value: Any) -> str:
    size = float(value)
    boundaries = (
        (1.0, "00_LE_1"),
        (5.0, "01_GT_1_LE_5"),
        (10.0, "02_GT_5_LE_10"),
        (25.0, "03_GT_10_LE_25"),
        (100.0, "04_GT_25_LE_100"),
        (float("inf"), "05_GT_100"),
    )
    return next(label for upper, label in boundaries if size <= upper)


def _group_name(row: Mapping[str, Any], key: str) -> str:
    if key == "window":
        return str(row["window"])
    dimensions = row.get("dimensions") or {}
    if key == "category":
        return _category(row)
    if key == "price_bucket":
        return _price_bucket(dimensions.get("limit_price", 0))
    if key == "order_size_bucket":
        return _order_size_bucket(dimensions.get("order_size", 0))
    return str(dimensions.get(key) or "unknown")


def _group_metrics(
    rows: Sequence[Mapping[str, Any]],
    artifact: Mapping[str, Any],
    args: argparse.Namespace,
    *,
    key: str,
) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_group_name(row, key)].append(row)
    result: dict[str, Any] = {}
    for name, values in sorted(grouped.items()):
        supported = [row for row in values if _domain_supported(row, artifact)]
        markets = len({int(row["identity"][0]) for row in supported})
        item: dict[str, Any] = {
            "orders": len(values),
            "supported_orders": len(supported),
            "support_rate": len(supported) / len(values),
            "unique_markets": markets,
        }
        minimum_orders = (
            args.minimum_window_orders
            if key == "window"
            else args.minimum_stratum_orders
        )
        minimum_markets = (
            args.minimum_window_markets
            if key == "window"
            else args.minimum_stratum_markets
        )
        if len(supported) < minimum_orders or markets < minimum_markets:
            item.update(
                status="DATA_INSUFFICIENT",
                reason=(
                    "MODEL_DOMAIN_ABSTENTION"
                    if values and not supported
                    else "INSUFFICIENT_INDEPENDENT_MARKET_SAMPLE"
                ),
            )
        else:
            metrics = _evaluate_rows(
                supported,
                artifact,
                include_observations=(
                    str(getattr(args, "quality_gate_mode", "adaptive")) == "adaptive"
                ),
            )
            group_args = argparse.Namespace(
                **{
                    **vars(args),
                    "minimum_adaptive_clusters": int(
                        getattr(args, "minimum_group_adaptive_clusters", 5)
                    ),
                }
            )
            gate = _gate(metrics, group_args)
            coverage_gate = _coverage_gate(
                metrics,
                group_args,
                probability_gate=gate,
            )
            item.update(
                metrics=_public_metrics(metrics),
                gate=gate,
                coverage_gate=coverage_gate,
            )
            item["status"] = item["gate"]["status"]
            if item["status"] == "INSUFFICIENT_SAMPLE":
                item.update(
                    status="DATA_INSUFFICIENT",
                    reason="INSUFFICIENT_ADAPTIVE_QUALITY_SAMPLE",
                )
        result[name] = item
    return result


def _coverage_summary(groups: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    evaluated = [
        value
        for value in groups.values()
        if (value.get("coverage_gate") or {}).get("status") in {"PASS", "FAIL"}
    ]
    passed = sum(
        (value.get("coverage_gate") or {}).get("status") == "PASS"
        for value in evaluated
    )
    return {
        "evaluated": len(evaluated),
        "passed": passed,
        "pass_rate": passed / len(evaluated) if evaluated else None,
    }


def validate(args: argparse.Namespace) -> dict[str, Any]:
    artifact = json.loads(args.artifact.read_text(encoding="utf-8"))
    paths = tuple(_orders_path(path) for path in args.orders)
    promotion_allowed = bool(artifact.get("promotion_allowed"))
    if not promotion_allowed and not args.allow_research_candidate:
        raise RuntimeError("candidate artifact is not promotion-eligible")
    runtime_lob_usage = (artifact.get("hierarchical_probability") or {}).get(
        "runtime_lob_usage"
    ) or (artifact.get("data_contract") or {}).get("runtime_lob_usage")
    if runtime_lob_usage != "NONE":
        raise RuntimeError("candidate artifact is not trade-only at runtime")

    target = str(
        (artifact.get("model_contract") or {}).get("probability_target") or ""
    ).upper()
    references_to_run = (
        ("pml2_fok", "nautilus_fok")
        if target == "FOK_FULL_FILL"
        else ("pml2_fak", "nautilus_fak")
    )
    references: dict[str, Any] = {}
    breadth_rows: list[dict[str, Any]] | None = None
    for reference in references_to_run:
        rows = _prepare_rows(paths, artifact, reference)
        if reference.startswith("nautilus_"):
            breadth_rows = rows
        metrics = _evaluate_rows(
            rows,
            artifact,
            include_observations=(args.quality_gate_mode == "adaptive"),
        )
        all_sample_metrics = _evaluate_rows(rows, artifact, supported_only=False)
        supported_samples = sum(_domain_supported(row, artifact) for row in rows)
        support_rate = supported_samples / len(rows)
        windows = _group_metrics(rows, artifact, args, key="window")
        strata = {
            dimension: _group_metrics(rows, artifact, args, key=dimension)
            for dimension in STRATUM_DIMENSIONS
        }
        categories = strata["category"]
        evaluated_windows = {
            name: value
            for name, value in windows.items()
            if value["status"] != "DATA_INSUFFICIENT"
        }
        evaluated_categories = {
            name: value
            for name, value in categories.items()
            if value["status"] != "DATA_INSUFFICIENT"
        }
        coverage = {
            "windows": _coverage_summary(windows),
            **{
                dimension: _coverage_summary(strata[dimension])
                for dimension in GATED_STRATUM_DIMENSIONS
            },
        }
        if args.quality_gate_mode == "adaptive":
            broad_coverage = all(
                summary["evaluated"] > 0 and summary["passed"] == summary["evaluated"]
                for summary in coverage.values()
            )
        else:
            broad_coverage = all(
                summary["pass_rate"] is not None
                and float(summary["pass_rate"]) >= args.minimum_coverage_pass_rate
                for summary in coverage.values()
            )
        strict_groups_pass = all(
            value["status"] == "PASS"
            for groups in (
                windows,
                *(strata[dimension] for dimension in GATED_STRATUM_DIMENSIONS),
            )
            for value in groups.values()
            if value["status"] != "DATA_INSUFFICIENT"
        )
        overall_gate = _gate(metrics, args)
        overall_coverage_gate = _coverage_gate(
            metrics,
            args,
            probability_gate=overall_gate,
        )
        detected_group_failure = any(
            value["status"] == "FAIL"
            for groups in (
                windows,
                *(strata[dimension] for dimension in GATED_STRATUM_DIMENSIONS),
            )
            for value in groups.values()
        )
        checks = {
            "overall_calibration": overall_gate["status"] == "PASS",
            "overall_coverage": overall_coverage_gate["status"] == "PASS",
            "minimum_support_rate": support_rate >= args.minimum_support_rate,
            "broad_group_coverage": broad_coverage,
            "minimum_evaluated_windows": (
                len(evaluated_windows) >= args.minimum_windows
            ),
            "minimum_evaluated_categories": (
                len(evaluated_categories) >= args.minimum_categories
            ),
        }
        if args.quality_gate_mode == "adaptive":
            checks["no_detected_group_quality_failure"] = not detected_group_failure
        accepted = all(checks.values())
        reference_status = (
            "PASS"
            if accepted and strict_groups_pass
            else "PASS_WITH_CALIBRATION_WARNINGS"
            if accepted
            else "FAIL"
        )
        references[reference] = {
            "status": reference_status,
            "checks": checks,
            "overall": _public_metrics(metrics),
            "all_sample": all_sample_metrics,
            "supported_samples": supported_samples,
            "support_rate": support_rate,
            "overall_gate": overall_gate,
            "overall_coverage_gate": overall_coverage_gate,
            "coverage": coverage,
            "strict_group_calibration_passed": strict_groups_pass,
            "windows": windows,
            "strata": strata,
        }

    assert breadth_rows is not None
    dates = {str(row["identity"][2])[:10] for row in breadth_rows}
    markets = {int(row["identity"][0]) for row in breadth_rows}
    market_days = {
        (int(row["identity"][0]), str(row["identity"][2])[:10]) for row in breadth_rows
    }
    breadth = {
        "orders": len(breadth_rows),
        "dates": len(dates),
        "unique_markets": len(markets),
        "market_days": len(market_days),
    }
    breadth_checks = {
        "minimum_orders": breadth["orders"] >= args.minimum_orders,
        "minimum_dates": breadth["dates"] >= args.minimum_dates,
        "minimum_markets": breadth["unique_markets"] >= args.minimum_markets,
        "minimum_market_days": breadth["market_days"] >= args.minimum_market_days,
    }
    accepted = all(breadth_checks.values()) and all(
        str(value["status"]).startswith("PASS") for value in references.values()
    )
    status = (
        "PASS"
        if accepted and all(value["status"] == "PASS" for value in references.values())
        else "PASS_WITH_CALIBRATION_WARNINGS"
        if accepted
        else "FAIL"
    )
    result = {
        "schema_version": "fill-only-v3-saved-label-validation-v2",
        "status": status,
        "scope": "PROBABILITY_AND_CONDITIONAL_SIZE_TRANSFER_VALIDATION",
        "runtime_lob_usage": "NONE",
        "label_sources": {
            references_to_run[0]: "SAVED_EVENT_DRIVEN_L2_EXECUTABILITY_REFERENCE",
            references_to_run[1]: "SAVED_STATIC_L2_IMPLEMENTATION_CONTROL",
        },
        "does_not_claim": [
            "OBSERVED_EXECUTION",
            "LIVE_ORDER_FILL_PROBABILITY",
            "FRESH_END_TO_END_CAPACITY_LEDGER_REPLAY",
        ],
        "artifact": {
            "path": str(args.artifact.resolve()),
            "sha256": _sha256(args.artifact),
            "model_version": artifact["model_version"],
            "promotion_allowed": promotion_allowed,
            "candidate_validation_only": not promotion_allowed,
        },
        "inputs": [
            {"path": str(path.resolve()), "sha256": _sha256(path)} for path in paths
        ],
        "gates": {
            "quality_gate_mode": args.quality_gate_mode,
            "decision_rule": (
                "REFERENCE_RELATIVE_PROPER_SCORES_AND_PAIRED_MARKET_DAY_"
                "CLUSTER_BOOTSTRAP"
                if args.quality_gate_mode == "adaptive"
                else "LEGACY_FIXED_ABSOLUTE_THRESHOLDS"
            ),
            "legacy_fixed_thresholds_applied": args.quality_gate_mode == "fixed",
            "minimum_windows": args.minimum_windows,
            "minimum_categories": args.minimum_categories,
            "confidence_level": args.confidence_level,
            "bootstrap_replicates": args.bootstrap_replicates,
            "minimum_adaptive_clusters": args.minimum_adaptive_clusters,
            "minimum_group_adaptive_clusters": args.minimum_group_adaptive_clusters,
        },
        "breadth": breadth,
        "breadth_checks": breadth_checks,
        "references": references,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument(
        "--allow-research-candidate",
        action="store_true",
        help="Evaluate an unpromoted candidate without activating or modifying it.",
    )
    parser.add_argument("--orders", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum-orders", type=int, default=10_001)
    parser.add_argument("--minimum-dates", type=int, default=3)
    parser.add_argument("--minimum-markets", type=int, default=100)
    parser.add_argument("--minimum-market-days", type=int, default=150)
    parser.add_argument("--minimum-categories", type=int, default=5)
    parser.add_argument("--minimum-windows", type=int, default=8)
    parser.add_argument("--minimum-window-orders", type=int, default=100)
    parser.add_argument("--minimum-window-markets", type=int, default=5)
    parser.add_argument("--minimum-stratum-orders", type=int, default=100)
    parser.add_argument("--minimum-stratum-markets", type=int, default=5)
    parser.add_argument("--minimum-ratio", type=float, default=0.80)
    parser.add_argument("--maximum-ratio", type=float, default=1.20)
    parser.add_argument("--maximum-brier-score", type=float, default=0.20)
    parser.add_argument("--maximum-brier-regret", type=float, default=0.01)
    parser.add_argument("--minimum-coverage-pass-rate", type=float, default=0.80)
    parser.add_argument("--minimum-support-rate", type=float, default=0.80)
    parser.add_argument(
        "--quality-gate-mode",
        choices=("adaptive", "fixed"),
        default="adaptive",
        help=(
            "adaptive compares proper scores with cohort climatology and uses "
            "market-day cluster intervals; fixed reproduces legacy thresholds"
        ),
    )
    parser.add_argument("--confidence-level", type=float, default=0.95)
    parser.add_argument("--bootstrap-replicates", type=int, default=1_000)
    parser.add_argument("--bootstrap-seed", type=int, default=73)
    parser.add_argument("--minimum-adaptive-samples", type=int, default=200)
    parser.add_argument("--minimum-adaptive-positive-samples", type=int, default=20)
    parser.add_argument("--minimum-adaptive-negative-samples", type=int, default=20)
    parser.add_argument("--minimum-adaptive-clusters", type=int, default=20)
    parser.add_argument("--minimum-group-adaptive-clusters", type=int, default=5)
    return parser


def main() -> int:
    args = _parser().parse_args()
    result = validate(args)

    def summary(gate: Mapping[str, Any]) -> dict[str, Any]:
        adaptive = gate.get("adaptive_probability_quality") or {}
        intervals = adaptive.get("confidence_intervals") or {}
        return {
            "status": gate.get("status"),
            "brier_score": adaptive.get("brier_score", gate.get("brier_score")),
            "reference_brier_score": adaptive.get(
                "reference_brier_score", gate.get("climatology_brier_score")
            ),
            "brier_regret": adaptive.get("brier_regret", gate.get("brier_regret")),
            "brier_regret_interval": intervals.get("brier_regret"),
            "expected_order_ratio": adaptive.get(
                "expected_positive_order_ratio", gate.get("expected_order_ratio")
            ),
            "expected_order_ratio_interval": intervals.get("expected_order_ratio"),
            "expected_quantity_ratio": adaptive.get(
                "expected_quantity_ratio", gate.get("expected_quantity_ratio")
            ),
            "expected_quantity_ratio_interval": intervals.get(
                "expected_quantity_ratio"
            ),
            "quantity_fraction_mse": adaptive.get("quantity_fraction_mse"),
            "reference_quantity_fraction_mse": adaptive.get(
                "reference_quantity_fraction_mse"
            ),
            "quantity_regret": adaptive.get("quantity_regret"),
            "quantity_regret_interval": intervals.get("quantity_regret"),
        }

    print(
        json.dumps(
            {
                "status": result["status"],
                "breadth": result["breadth"],
                "references": {
                    name: {
                        "status": value["status"],
                        "overall": summary(value["overall_gate"]),
                    }
                    for name, value in result["references"].items()
                },
                "output": str(args.output),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if result["status"].startswith("PASS") else 2


if __name__ == "__main__":
    raise SystemExit(main())
