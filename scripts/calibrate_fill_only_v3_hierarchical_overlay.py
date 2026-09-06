#!/usr/bin/env python3
"""Fit a metadata hierarchy over an OrderFilled probability model."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.orderfilled_probability import (  # noqa: E402
    ARRIVAL_FEATURE_CONTRACT,
    HIERARCHY_KEY_SCHEME_LEGACY,
    HIERARCHY_KEY_SCHEME_VALIDATION_ALIGNED,
    PROBABILITY_TARGET_FAK_ANY_FILL,
    PROBABILITY_TARGET_FOK_FULL_FILL,
    augment_orderfilled_probability_vector,
    hierarchical_probability_keys,
)
from quant.backtest.probability_quality import (  # noqa: E402
    adaptive_probability_quality,
)
from scripts.calibrate_fill_only_v3_l2_reference import _source_result  # noqa: E402

DEFAULT_ARTIFACT = (
    PROJECT_ROOT
    / "config"
    / "execution"
    / "fill_only_v3_l2_reference_probability.v1.json"
)
MINIMUM_MARKET_CELL_WINDOWS = 3
MINIMUM_CONTEXT_CELL_WINDOWS = 3
MINIMUM_CATEGORY_CELL_MARKETS = 3
MINIMUM_FAMILY_CELL_MARKETS = 5
MINIMUM_GENERIC_CELL_MARKETS = 10

CellStats = tuple[int, int, int, int, float]


def _orders_path(path: Path) -> Path:
    return path / "orders.jsonl" if path.is_dir() else path


def _load(
    paths: Sequence[Path],
    *,
    reference_model: str = "pml2_fak",
    key_scheme: str = HIERARCHY_KEY_SCHEME_LEGACY,
    probability_target: str = PROBABILITY_TARGET_FAK_ANY_FILL,
    source_model: str | None = None,
    source_required: bool = True,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in paths:
        path = _orders_path(raw)
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            row = json.loads(line)
            feature_contract = row.get("fill_only_feature_contract")
            if feature_contract != ARRIVAL_FEATURE_CONTRACT:
                raise RuntimeError(
                    "unsupported fill-only feature contract "
                    f"{feature_contract!r} at {path}:{line_number}; "
                    f"expected {ARRIVAL_FEATURE_CONTRACT}"
                )
            reference = row["models"][reference_model]
            if (
                reference_model.startswith("pml2_")
                and reference.get("status") == "DATA_NOT_READY"
            ):
                continue
            source = _source_result(
                row["models"],
                preferred=source_model,
                allow_missing=not source_required,
            )
            size = float(row["size"])
            reference_size = float(reference["filled_size"])
            source_size = float(source["filled_size"])
            row_tif = str(row.get("tif") or "FAK").upper()
            expected_tif = (
                "FOK"
                if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
                else "FAK"
            )
            if row_tif != expected_tif:
                continue
            label = (
                float(reference_size >= size - 1e-9)
                if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
                else float(reference_size > 0)
            )
            runtime_result = (
                row["models"].get("v3_contract_expected")
                or row["models"].get("v3_l2_expected")
                or {}
            )
            runtime_diagnostics = runtime_result.get("model_diagnostics") or {}
            features = row["fill_only_features"]
            vector = augment_orderfilled_probability_vector(
                features["vector"],
                last_same_age_seconds=features.get("last_same_age_seconds"),
                last_opposite_age_seconds=features.get("last_opposite_age_seconds"),
                order_size=size,
            )
            context = SimpleNamespace(
                market_id=int(row["market_id"]),
                asset_id=str(row["asset_id"]),
                category=row.get("category"),
                market_slug=row.get("slug"),
                market_title=row.get("title"),
                league=None,
                limit_price=float(row["limit_price"]),
                side=str(row.get("side") or "BUY").upper(),
                tif=row_tif,
                size=size,
                signal_ts=datetime.fromisoformat(str(row["decision_ts"])),
                market_end_ts=(
                    datetime.fromisoformat(str(row["market_end_ts"]))
                    if row.get("market_end_ts")
                    else None
                ),
            )
            rows.append(
                {
                    "identity": (
                        int(row["market_id"]),
                        str(row["asset_id"]),
                        str(row["decision_ts"]),
                    ),
                    "window": datetime.fromisoformat(str(row["decision_ts"])).strftime(
                        "%Y-%m-%dT%H"
                    ),
                    "keys": hierarchical_probability_keys(
                        context, vector, key_scheme=key_scheme
                    ),
                    "vector": {str(key): float(value) for key, value in vector.items()},
                    "label": label,
                    "reference_fraction": (
                        1.0
                        if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
                        and label
                        else max(0.0, min(1.0, reference_size / size))
                        if size
                        else 0.0
                    ),
                    "source_positive": source_size > 0,
                    "source_fraction": (
                        max(0.0, min(1.0, source_size / size)) if size else 0.0
                    ),
                    "runtime_probability_model_version": runtime_diagnostics.get(
                        "probability_model_version"
                    ),
                    "runtime_base_probability": runtime_diagnostics.get(
                        "base_orderfilled_probability"
                    ),
                    "dimensions": {
                        "category": str(row.get("category") or "unknown"),
                        "limit_price": float(row["limit_price"]),
                        "activity_regime": (
                            "SPARSE_LE_5"
                            if int(features["trailing_same_count"])
                            + int(features["trailing_opposite_count"])
                            <= 5
                            else "MEDIUM_6_TO_50"
                            if int(features["trailing_same_count"])
                            + int(features["trailing_opposite_count"])
                            <= 50
                            else "ACTIVE_GT_50"
                        ),
                        "depth_regime": str(row.get("depth_regime") or "unknown"),
                        "spread_regime": str(row.get("spread_regime") or "unknown"),
                        "side": str(row.get("side") or "unknown").upper(),
                        "tif": row_tif,
                        "amount_unit": str(row.get("amount_unit") or "SHARES").upper(),
                        "order_size": size,
                        "log_order_to_tape_ratio": float(
                            vector.get("log_order_to_tape_ratio", 0)
                        ),
                    },
                }
            )
    if not rows:
        raise RuntimeError("no PML2-labeled rows were loaded")
    return rows


def _assert_disjoint(
    calibration: Sequence[Mapping[str, Any]], validation: Sequence[Mapping[str, Any]]
) -> None:
    for name, rows in (("calibration", calibration), ("validation", validation)):
        identities = [row["identity"] for row in rows]
        if len(identities) != len(set(identities)):
            raise RuntimeError(f"{name} contains duplicate order identities")
    overlap = {row["identity"] for row in calibration}.intersection(
        row["identity"] for row in validation
    )
    if overlap:
        raise RuntimeError(f"calibration and validation overlap: {len(overlap)}")


def _sigmoid(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-max(-40.0, min(40.0, value))))


def _base_probability(row: Mapping[str, Any], artifact: Mapping[str, Any]) -> float:
    model = artifact["model"]
    score = float(model["intercept"])
    vector = row["vector"]
    for name, coefficient in model["coefficients"].items():
        value = float(vector.get(name, 0.0))
        mean = float(model["feature_means"].get(name, 0.0))
        scale = max(1e-10, float(model["feature_scales"].get(name, 1.0)))
        score += float(coefficient) * ((value - mean) / scale)
    raw = _sigmoid(score)
    calibration = artifact.get("calibration") or {}
    family_calibration = (calibration.get("by_category_family") or {}).get(_family(row))
    if isinstance(family_calibration, Mapping):
        calibration = family_calibration
    xs = [float(value) for value in calibration.get("x", ())]
    ys = [float(value) for value in calibration.get("y", ())]
    if len(xs) < 2 or len(xs) != len(ys):
        return raw
    if raw <= xs[0]:
        return ys[0]
    if raw >= xs[-1]:
        return ys[-1]
    index = bisect_left(xs, raw)
    width = xs[index] - xs[index - 1]
    if width <= 0:
        return ys[index]
    weight = (raw - xs[index - 1]) / width
    return ys[index - 1] + weight * (ys[index] - ys[index - 1])


def _conditional_fraction(row: Mapping[str, Any], artifact: Mapping[str, Any]) -> float:
    capacity = artifact.get("conditional_capacity") or {}
    model = (capacity.get("models") or {}).get("expected") or {}
    value = float(model.get("intercept", 1.0))
    probability_model = artifact["model"]
    for name, coefficient in (model.get("coefficients") or {}).items():
        raw = float(row["vector"].get(name, 0.0))
        mean = float(probability_model["feature_means"].get(name, 0.0))
        scale = max(1e-10, float(probability_model["feature_scales"].get(name, 1.0)))
        value += float(coefficient) * ((raw - mean) / scale)
    return max(float(model.get("floor", 0.0)), min(1.0, value))


def _cells(rows: Sequence[Mapping[str, Any]]) -> dict[str, CellStats]:
    values: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "positives": 0,
            "samples": 0,
            "windows": set(),
            "markets": set(),
            "positive_fraction_sum": 0.0,
        }
    )
    for row in rows:
        for key in row["keys"]:
            positive = int(row["label"] > 0.5)
            values[key]["positives"] += positive
            values[key]["samples"] += 1
            values[key]["windows"].add(str(row.get("window") or row["identity"][2]))
            values[key]["markets"].add(int(row["identity"][0]))
            if positive:
                values[key]["positive_fraction_sum"] += float(
                    row.get("reference_fraction", row["label"])
                )
    return {
        key: (
            int(value["positives"]),
            int(value["samples"]),
            len(value["windows"]),
            len(value["markets"]),
            float(value["positive_fraction_sum"]),
        )
        for key, value in values.items()
    }


def _cell_has_independent_support(
    level: str, *, independent_windows: int, unique_markets: int
) -> bool:
    if level == "global":
        return True
    if level.startswith("market"):
        return independent_windows >= MINIMUM_MARKET_CELL_WINDOWS
    if level.startswith("category"):
        return (
            independent_windows >= MINIMUM_CONTEXT_CELL_WINDOWS
            and unique_markets >= MINIMUM_CATEGORY_CELL_MARKETS
        )
    if level.startswith("family"):
        return (
            independent_windows >= MINIMUM_CONTEXT_CELL_WINDOWS
            and unique_markets >= MINIMUM_FAMILY_CELL_MARKETS
        )
    return (
        independent_windows >= MINIMUM_CONTEXT_CELL_WINDOWS
        and unique_markets >= MINIMUM_GENERIC_CELL_MARKETS
    )


def _resolve(
    row: Mapping[str, Any],
    cells: Mapping[str, CellStats],
    minimum: int,
    *,
    skip_category: bool = False,
    skip_family: bool = False,
) -> tuple[str, int, int, float]:
    for key in row["keys"]:
        level = str(key).split("|", 1)[0]
        if skip_category and level.startswith("category"):
            continue
        if skip_family and level.startswith("family"):
            continue
        if (skip_category or skip_family) and key == "global":
            continue
        positives, samples, windows, markets, fraction_sum = cells.get(
            key, (0, 0, 0, 0, 0.0)
        )
        if not _cell_has_independent_support(
            level, independent_windows=windows, unique_markets=markets
        ):
            continue
        if samples >= minimum:
            return key, positives, samples, fraction_sum
    if skip_category or skip_family:
        return "base_probability_fallback", 0, 0, 0.0
    positives, samples, _, _, fraction_sum = cells["global"]
    return "global", positives, samples, fraction_sum


def _evaluate(
    rows: Sequence[Mapping[str, Any]],
    artifact: Mapping[str, Any],
    cells: Mapping[str, CellStats],
    *,
    minimum: int,
    beta_strength: float,
    blend_strength: float,
    global_probability: float,
    global_conditional_fraction: float,
    probability_scale: float = 1.0,
    supported_categories: set[str] | None = None,
    supported_families: set[str] | None = None,
    fallback_categories: set[str] | None = None,
    fallback_families: set[str] | None = None,
    abstain_categories: set[str] | None = None,
    abstain_families: set[str] | None = None,
    abstain_activity_regimes: set[str] | None = None,
    include_observations: bool = False,
) -> dict[str, Any]:
    fallback_categories = fallback_categories or set()
    fallback_families = fallback_families or set()
    abstain_categories = abstain_categories or set()
    abstain_families = abstain_families or set()
    abstain_activity_regimes = abstain_activity_regimes or set()
    probabilities: list[float] = []
    expected_fractions: list[float] = []
    labels: list[float] = []
    reference_fractions: list[float] = []
    selected: dict[str, int] = defaultdict(int)
    for row in rows:
        labels.append(float(row["label"]))
        reference_fractions.append(float(row["reference_fraction"]))
        if row["source_positive"]:
            probabilities.append(1.0)
            expected_fractions.append(
                max(
                    float(row["source_fraction"]),
                    float(row["conditional_fraction_model"]),
                )
            )
            selected["source_confirmed_plus_modeled_residual"] += 1
            continue
        base = float(row["base_probability"])
        category = _category(row)
        family = _family(row)
        activity = str((row.get("dimensions") or {}).get("activity_regime") or "")
        if (
            category in abstain_categories
            or family in abstain_families
            or activity in abstain_activity_regimes
        ):
            probabilities.append(0.0)
            expected_fractions.append(0.0)
            selected["domain_abstain"] += 1
            continue
        skip_category = category in fallback_categories or (
            supported_categories is not None and category not in supported_categories
        )
        skip_family = family in fallback_families or (
            supported_families is not None and family not in supported_families
        )
        key, positives, samples, fraction_sum = _resolve(
            row,
            cells,
            minimum,
            skip_category=skip_category,
            skip_family=skip_family,
        )
        if key == "base_probability_fallback":
            probability = max(0.0, min(1.0, base * probability_scale))
            probabilities.append(probability)
            expected_fractions.append(
                probability * float(row["conditional_fraction_model"])
            )
            selected[key] += 1
            continue
        prior = (positives + beta_strength * global_probability) / (
            samples + beta_strength
        )
        weight = samples / (samples + blend_strength) if blend_strength else 1.0
        probability = max(
            0.0,
            min(1.0, (weight * prior + (1.0 - weight) * base) * probability_scale),
        )
        probabilities.append(probability)
        base_conditional = float(row["conditional_fraction_model"])
        if positives:
            conditional_prior = (
                fraction_sum + beta_strength * global_conditional_fraction
            ) / (positives + beta_strength)
            conditional_weight = (
                positives / (positives + blend_strength) if blend_strength else 1.0
            )
            conditional_fraction = (
                conditional_weight * conditional_prior
                + (1.0 - conditional_weight) * base_conditional
            )
        else:
            conditional_fraction = base_conditional
        expected_fractions.append(
            probability * max(0.0, min(1.0, conditional_fraction))
        )
        selected[key] += 1
    brier = sum(
        (probability - label) ** 2
        for probability, label in zip(probabilities, labels, strict=True)
    ) / len(rows)
    positive_count = sum(labels)
    reference_quantity = sum(reference_fractions)
    result: dict[str, Any] = {
        "samples": len(rows),
        "unique_markets": len({int(row["identity"][0]) for row in rows}),
        "brier_score": brier,
        "expected_positive_orders": sum(probabilities),
        "reference_positive_orders": positive_count,
        "expected_positive_order_ratio": (
            sum(probabilities) / positive_count if positive_count else None
        ),
        "expected_quantity_fraction": sum(expected_fractions),
        "reference_quantity_fraction": reference_quantity,
        "expected_quantity_ratio": (
            sum(expected_fractions) / reference_quantity if reference_quantity else None
        ),
        "selected_cells": dict(sorted(selected.items())),
    }
    if include_observations:
        result["_observations"] = {
            "probabilities": probabilities,
            "labels": labels,
            "expected_fractions": expected_fractions,
            "reference_fractions": reference_fractions,
            "cluster_ids": [
                f"{row['identity'][0]}:{str(row['identity'][2])[:10]}" for row in rows
            ],
        }
    return result


def _adaptive_quality(
    metrics: dict[str, Any],
    args: argparse.Namespace,
    *,
    minimum_clusters: int | None = None,
) -> dict[str, Any]:
    observations = metrics.pop("_observations")
    return adaptive_probability_quality(
        observations["probabilities"],
        observations["labels"],
        expected_quantities=observations["expected_fractions"],
        reference_quantities=observations["reference_fractions"],
        cluster_ids=observations["cluster_ids"],
        bootstrap_replicates=int(getattr(args, "bootstrap_replicates", 1_000)),
        confidence_level=float(getattr(args, "confidence_level", 0.95)),
        random_seed=int(getattr(args, "bootstrap_seed", 73)),
        minimum_samples=200,
        minimum_positive_samples=20,
        minimum_negative_samples=20,
        minimum_clusters=(
            int(minimum_clusters)
            if minimum_clusters is not None
            else int(getattr(args, "minimum_adaptive_clusters", 20))
        ),
    )


def _passes(metrics: Mapping[str, Any], args: argparse.Namespace) -> bool:
    count_ratio = metrics.get("expected_positive_order_ratio")
    quantity_ratio = metrics.get("expected_quantity_ratio")
    baseline = _family_brier_baseline(metrics)
    maximum_regret = float(getattr(args, "maximum_brier_regret", 0.01))
    return (
        count_ratio is not None
        and quantity_ratio is not None
        and float(metrics["brier_score"]) <= args.maximum_brier_score
        and float(metrics["brier_score"]) - baseline <= maximum_regret
        and args.minimum_ratio <= float(count_ratio) <= args.maximum_ratio
        and args.minimum_ratio <= float(quantity_ratio) <= args.maximum_ratio
    )


def _family_brier_baseline(metrics: Mapping[str, Any]) -> float:
    samples = max(1, int(metrics["samples"]))
    positive_rate = float(metrics["reference_positive_orders"]) / samples
    return positive_rate * (1.0 - positive_rate)


def _passes_family(metrics: Mapping[str, Any], args: argparse.Namespace) -> bool:
    """Require calibrated size and probability skill within each broad family."""

    count_ratio = metrics.get("expected_positive_order_ratio")
    quantity_ratio = metrics.get("expected_quantity_ratio")
    baseline = _family_brier_baseline(metrics)
    maximum_regret = float(getattr(args, "maximum_family_brier_regret", 0.01))
    return (
        count_ratio is not None
        and quantity_ratio is not None
        and float(metrics["brier_score"])
        <= float(getattr(args, "maximum_group_brier_score", 0.25))
        and float(metrics["brier_score"]) <= baseline + maximum_regret
        and args.minimum_ratio <= float(count_ratio) <= args.maximum_ratio
        and args.minimum_ratio <= float(quantity_ratio) <= args.maximum_ratio
    )


def _family(row: Mapping[str, Any]) -> str:
    for key in row["keys"]:
        if str(key).startswith("family|") and str(key).count("|") == 1:
            return str(key).split("|", 1)[1]
    return "other"


def _category(row: Mapping[str, Any]) -> str:
    for key in row["keys"]:
        if str(key).startswith("category|") and str(key).count("|") == 1:
            return str(key).split("|", 1)[1]
    return "unknown"


def _score(metrics: Mapping[str, Any]) -> float:
    return (
        float(metrics["brier_score"])
        + 0.5 * abs(1.0 - float(metrics["expected_positive_order_ratio"]))
        + 0.5 * abs(1.0 - float(metrics["expected_quantity_ratio"]))
    )


def _adaptive_candidate_score(metrics: Mapping[str, Any]) -> tuple[float, float, float]:
    """Rank candidates without importing legacy absolute pass thresholds."""

    count_ratio = float(metrics.get("expected_positive_order_ratio") or 0.0)
    quantity_ratio = float(metrics.get("expected_quantity_ratio") or 0.0)
    baseline = _family_brier_baseline(metrics)
    ratio_error = abs(math.log(max(count_ratio, 1e-12))) + abs(
        math.log(max(quantity_ratio, 1e-12))
    )
    return (
        float(metrics["brier_score"]) - baseline,
        ratio_error,
        float(metrics["brier_score"]),
    )


def _prefer_group_fallback(
    raw: Mapping[str, Any], fallback: Mapping[str, Any], args: argparse.Namespace
) -> bool:
    """Use a broader hierarchy only when holdout evidence says it is better."""

    raw_passes = _passes_family(raw, args)
    fallback_passes = _passes_family(fallback, args)
    if raw_passes != fallback_passes:
        return fallback_passes
    return _score(fallback) < _score(raw)


def _gate_penalty(
    metrics: Mapping[str, Any],
    args: argparse.Namespace,
    *,
    maximum_brier_score: float | None = None,
) -> float:
    count_ratio = metrics.get("expected_positive_order_ratio")
    quantity_ratio = metrics.get("expected_quantity_ratio")
    if count_ratio is None or quantity_ratio is None:
        return float("inf")
    ratio_penalties = (
        max(0.0, args.minimum_ratio - float(count_ratio)),
        max(0.0, float(count_ratio) - args.maximum_ratio),
        max(0.0, args.minimum_ratio - float(quantity_ratio)),
        max(0.0, float(quantity_ratio) - args.maximum_ratio),
    )
    brier_penalty = max(
        0.0,
        float(metrics["brier_score"])
        - (
            args.maximum_brier_score
            if maximum_brier_score is None
            else maximum_brier_score
        ),
        float(metrics["brier_score"])
        - _family_brier_baseline(metrics)
        - float(getattr(args, "maximum_family_brier_regret", 0.01)),
    )
    return max(*ratio_penalties, brier_penalty)


def _candidate_stability(
    rows: Sequence[Mapping[str, Any]],
    artifact: Mapping[str, Any],
    cells: Mapping[str, CellStats],
    *,
    minimum: int,
    beta_strength: float,
    blend_strength: float,
    global_probability: float,
    global_conditional_fraction: float,
    probability_scale: float,
    supported_categories: set[str],
    supported_families: set[str],
    args: argparse.Namespace,
) -> dict[str, Any]:
    specifications = (
        (
            "window",
            lambda row: str(row.get("window") or row["identity"][2]),
            100,
            5,
        ),
        (
            "category",
            _category,
            args.minimum_category_samples,
            args.minimum_category_markets,
        ),
        (
            "family",
            _family,
            args.minimum_family_samples,
            args.minimum_family_markets,
        ),
    )
    penalties: dict[str, float] = {}
    for dimension, key_fn, minimum_rows, minimum_markets in specifications:
        groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[key_fn(row)].append(row)
        for name, group in groups.items():
            if (
                len(group) < minimum_rows
                or len({int(row["identity"][0]) for row in group}) < minimum_markets
            ):
                continue
            metrics = _evaluate(
                group,
                artifact,
                cells,
                minimum=minimum,
                beta_strength=beta_strength,
                blend_strength=blend_strength,
                global_probability=global_probability,
                global_conditional_fraction=global_conditional_fraction,
                probability_scale=probability_scale,
                supported_categories=supported_categories,
                supported_families=supported_families,
            )
            penalties[f"{dimension}:{name}"] = _gate_penalty(
                metrics,
                args,
                maximum_brier_score=(
                    args.maximum_brier_score
                    if dimension == "window"
                    else float(getattr(args, "maximum_group_brier_score", 0.25))
                ),
            )
    failures = sorted(name for name, penalty in penalties.items() if penalty > 0)
    return {
        "passed": bool(penalties) and not failures,
        "evaluated_groups": len(penalties),
        "failed_groups": failures,
        "maximum_penalty": max(penalties.values(), default=float("inf")),
        "mean_penalty": (
            sum(penalties.values()) / len(penalties) if penalties else float("inf")
        ),
    }


def _fingerprint(rows: Sequence[Mapping[str, Any]]) -> str:
    payload = json.dumps(sorted(row["identity"] for row in rows), separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def _supported_groups(
    rows: Sequence[Mapping[str, Any]],
    key_fn: Any,
    *,
    minimum_rows: int,
    minimum_markets: int,
) -> set[str]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[key_fn(row)].append(row)
    return {
        name
        for name, group in groups.items()
        if len(group) >= minimum_rows
        and len({int(row["identity"][0]) for row in group}) >= minimum_markets
    }


def calibrate(args: argparse.Namespace) -> dict[str, Any]:
    artifact = json.loads(args.base_artifact.read_text(encoding="utf-8"))
    key_scheme = getattr(args, "key_scheme", HIERARCHY_KEY_SCHEME_LEGACY)
    probability_target = str(
        getattr(args, "probability_target", None)
        or (artifact.get("model_contract") or {}).get("probability_target")
        or PROBABILITY_TARGET_FAK_ANY_FILL
    ).upper()
    reference_model = str(
        getattr(args, "reference_model", None)
        or (
            "pml2_fok"
            if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
            else "pml2_fak"
        )
    )
    fit_scope = str(
        (artifact.get("model_contract") or {}).get("fit_scope") or "residual"
    )
    loader_options = {
        "reference_model": reference_model,
        "key_scheme": key_scheme,
        "probability_target": probability_target,
        "source_model": getattr(args, "source_model", None),
        "source_required": fit_scope != "all",
    }
    calibration = _load(args.calibration_orders, **loader_options)
    validation = _load(args.validation_orders, **loader_options)
    _assert_disjoint(calibration, validation)
    if fit_scope == "all":
        for row in (*calibration, *validation):
            row["observed_source_positive"] = row["source_positive"]
            row["source_positive"] = False
            row["source_fraction"] = 0.0
    for row in (*calibration, *validation):
        row["base_probability"] = _base_probability(row, artifact)
        row["conditional_fraction_model"] = _conditional_fraction(row, artifact)
    cells = _cells(calibration)
    global_probability = sum(row["label"] for row in calibration) / len(calibration)
    calibration_positives = sum(row["label"] for row in calibration)
    global_conditional_fraction = (
        sum(row["reference_fraction"] for row in calibration) / calibration_positives
        if calibration_positives
        else 0.0
    )
    supported_categories = _supported_groups(
        validation,
        _category,
        minimum_rows=args.minimum_category_samples,
        minimum_markets=args.minimum_category_markets,
    )
    supported_families = _supported_groups(
        validation,
        _family,
        minimum_rows=args.minimum_family_samples,
        minimum_markets=args.minimum_family_markets,
    )
    quality_gate_mode = str(getattr(args, "quality_gate_mode", "fixed")).lower()
    candidates: list[dict[str, Any]] = []
    for minimum in args.minimum_samples:
        for beta_strength in args.beta_strength:
            for blend_strength in args.blend_strength:
                for probability_scale in getattr(args, "probability_scale", [1.0]):
                    metrics = _evaluate(
                        validation,
                        artifact,
                        cells,
                        minimum=minimum,
                        beta_strength=beta_strength,
                        blend_strength=blend_strength,
                        global_probability=global_probability,
                        global_conditional_fraction=global_conditional_fraction,
                        probability_scale=probability_scale,
                        supported_categories=supported_categories,
                        supported_families=supported_families,
                    )
                    stability = _candidate_stability(
                        validation,
                        artifact,
                        cells,
                        minimum=minimum,
                        beta_strength=beta_strength,
                        blend_strength=blend_strength,
                        global_probability=global_probability,
                        global_conditional_fraction=global_conditional_fraction,
                        probability_scale=probability_scale,
                        supported_categories=supported_categories,
                        supported_families=supported_families,
                        args=args,
                    )
                    candidates.append(
                        {
                            "minimum_samples": minimum,
                            "beta_strength": beta_strength,
                            "blend_strength": blend_strength,
                            "probability_scale": probability_scale,
                            "metrics": metrics,
                            "stability": stability,
                            "passed": (
                                _passes(metrics, args) and stability["passed"]
                                if quality_gate_mode == "fixed"
                                else float(metrics["brier_score"])
                                <= _family_brier_baseline(metrics)
                            ),
                        }
                    )
    passing = [candidate for candidate in candidates if candidate["passed"]]
    if quality_gate_mode == "adaptive":
        selected = min(
            passing or candidates,
            key=lambda row: _adaptive_candidate_score(row["metrics"]),
        )
    else:
        selected = min(
            passing or candidates,
            key=lambda row: (
                float(row["stability"]["maximum_penalty"]),
                float(row["stability"]["mean_penalty"]),
                _score(row["metrics"]),
            ),
        )
    minimum = int(selected["minimum_samples"])
    beta_strength = float(selected["beta_strength"])
    blend_strength = float(selected["blend_strength"])
    probability_scale = float(selected["probability_scale"])

    def validate_groups(
        key_fn: Any,
        *,
        minimum_rows: int,
        minimum_markets: int,
        fallback_level: str,
    ) -> tuple[dict[str, Any], set[str]]:
        groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for value in validation:
            groups[key_fn(value)].append(value)
        results: dict[str, Any] = {}
        for name, rows in sorted(groups.items()):
            raw_metrics = _evaluate(
                rows,
                artifact,
                cells,
                minimum=minimum,
                beta_strength=beta_strength,
                blend_strength=blend_strength,
                global_probability=global_probability,
                global_conditional_fraction=global_conditional_fraction,
                probability_scale=probability_scale,
                supported_categories=supported_categories,
                supported_families=supported_families,
                include_observations=True,
            )
            fallback_metrics = _evaluate(
                rows,
                artifact,
                cells,
                minimum=minimum,
                beta_strength=beta_strength,
                blend_strength=blend_strength,
                global_probability=global_probability,
                global_conditional_fraction=global_conditional_fraction,
                probability_scale=probability_scale,
                supported_categories=supported_categories,
                supported_families=supported_families,
                fallback_categories=(
                    {name}
                    if fallback_level == "category"
                    else {_category(row) for row in rows}
                ),
                fallback_families={name} if fallback_level == "family" else set(),
                include_observations=True,
            )
            raw_adaptive = _adaptive_quality(
                raw_metrics, args, minimum_clusters=minimum_markets
            )
            fallback_adaptive = _adaptive_quality(
                fallback_metrics, args, minimum_clusters=minimum_markets
            )
            minimum_samples_met = len(rows) >= minimum_rows
            minimum_markets_met = int(raw_metrics["unique_markets"]) >= minimum_markets
            evaluated = minimum_samples_met and minimum_markets_met
            if quality_gate_mode == "adaptive":
                quality_rank = {
                    "PASS": 3,
                    "INCONCLUSIVE": 2,
                    "INSUFFICIENT_SAMPLE": 1,
                    "FAIL": 0,
                }
                use_fallback = evaluated and (
                    (
                        quality_rank[fallback_adaptive["status"]]
                        > quality_rank[raw_adaptive["status"]]
                    )
                    or (
                        raw_adaptive["status"] == fallback_adaptive["status"]
                        and _score(fallback_metrics) < _score(raw_metrics)
                    )
                )
            else:
                use_fallback = evaluated and _prefer_group_fallback(
                    raw_metrics, fallback_metrics, args
                )
            metrics = fallback_metrics if use_fallback else raw_metrics
            adaptive_quality = fallback_adaptive if use_fallback else raw_adaptive
            results[name] = {
                **metrics,
                "raw_metrics": raw_metrics,
                "fallback_metrics": fallback_metrics,
                "selected_path": "fallback" if use_fallback else "specific",
                "minimum_samples_met": minimum_samples_met,
                "minimum_markets_met": minimum_markets_met,
                "evaluated": evaluated,
                "brier_baseline": _family_brier_baseline(metrics),
                "brier_regret": (
                    float(metrics["brier_score"]) - _family_brier_baseline(metrics)
                ),
                "adaptive_quality": adaptive_quality,
                "passes_gates": (
                    adaptive_quality["status"] == "PASS"
                    if quality_gate_mode == "adaptive" and evaluated
                    else _passes_family(metrics, args)
                    if evaluated
                    else None
                ),
            }
        fallbacks = {
            name
            for name, metrics in results.items()
            if metrics["evaluated"] and metrics["selected_path"] == "fallback"
        }
        return results, fallbacks

    validation_by_family, fallback_families = validate_groups(
        _family,
        minimum_rows=args.minimum_family_samples,
        minimum_markets=args.minimum_family_markets,
        fallback_level="family",
    )
    validation_by_category, fallback_categories = validate_groups(
        _category,
        minimum_rows=args.minimum_category_samples,
        minimum_markets=args.minimum_category_markets,
        fallback_level="category",
    )
    adaptive_failed_families = {
        name
        for name, metrics in validation_by_family.items()
        if metrics.get("evaluated")
        and metrics.get("adaptive_quality", {}).get("status") == "FAIL"
    }
    adaptive_failed_categories = {
        name
        for name, metrics in validation_by_category.items()
        if metrics.get("evaluated")
        and metrics.get("adaptive_quality", {}).get("status") == "FAIL"
    }
    abstain_families = (
        adaptive_failed_families if quality_gate_mode == "adaptive" else set()
    )
    abstain_categories = (
        adaptive_failed_categories if quality_gate_mode == "adaptive" else set()
    )
    supported_validation = [
        row
        for row in validation
        if _family(row) not in abstain_families
        and _category(row) not in abstain_categories
    ]
    if supported_validation:
        fallback_metrics = _evaluate(
            supported_validation,
            artifact,
            cells,
            minimum=minimum,
            beta_strength=beta_strength,
            blend_strength=blend_strength,
            global_probability=global_probability,
            global_conditional_fraction=global_conditional_fraction,
            probability_scale=probability_scale,
            supported_categories=supported_categories,
            supported_families=supported_families,
            fallback_categories=fallback_categories,
            fallback_families=fallback_families,
            include_observations=True,
        )
        adaptive_quality = _adaptive_quality(fallback_metrics, args)
    else:
        fallback_metrics = {
            "samples": 0,
            "positive_samples": 0,
            "negative_samples": 0,
            "unique_markets": 0,
            "brier_score": None,
            "expected_order_ratio": None,
            "expected_quantity_ratio": None,
        }
        adaptive_quality = {
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
    refit_with_validation = bool(getattr(args, "refit_with_validation", False))
    runtime_rows = (*calibration, *validation) if refit_with_validation else calibration
    runtime_cells = _cells(runtime_rows)
    runtime_global_probability = sum(row["label"] for row in runtime_rows) / len(
        runtime_rows
    )
    runtime_positives = sum(row["label"] for row in runtime_rows)
    runtime_global_conditional_fraction = (
        sum(row["reference_fraction"] for row in runtime_rows) / runtime_positives
        if runtime_positives
        else 0.0
    )
    excluded_rows = len(validation) - len(supported_validation)
    excluded_positives = sum(
        float(row["label"])
        for row in validation
        if _family(row) in abstain_families or _category(row) in abstain_categories
    )
    all_positives = sum(float(row["label"]) for row in validation)
    excluded_share = excluded_rows / len(validation) if validation else 0.0
    excluded_positive_share = (
        excluded_positives / all_positives if all_positives else 0.0
    )
    domain_gate_passed = (
        bool(supported_validation)
        and excluded_share <= args.maximum_abstain_row_share
        and excluded_positive_share <= args.maximum_abstain_positive_share
    )
    adaptive_group_gate = domain_gate_passed
    promotion_allowed = (
        adaptive_quality["status"] == "PASS" and adaptive_group_gate
        if quality_gate_mode == "adaptive"
        else _passes(fallback_metrics, args) and bool(selected["stability"]["passed"])
    )
    selected["passed"] = promotion_allowed
    selected["quality_gate_mode"] = quality_gate_mode
    selected["adaptive_quality"] = adaptive_quality
    selected["adaptive_group_gate_passed"] = adaptive_group_gate
    selected["all_sample_metrics"] = selected["metrics"]
    selected["supported_metrics"] = fallback_metrics
    selected["fallback_metrics"] = fallback_metrics
    selected["validation_by_family"] = validation_by_family
    selected["validation_by_category"] = validation_by_category
    selected["fallback_families"] = sorted(fallback_families)
    selected["fallback_categories"] = sorted(fallback_categories)
    selected["failed_families"] = sorted(adaptive_failed_families)
    selected["failed_categories"] = sorted(adaptive_failed_categories)
    selected["domain_gate_passed"] = domain_gate_passed
    serialized_cells = {}
    for key, (
        positives,
        samples,
        independent_windows,
        unique_markets,
        positive_fraction_sum,
    ) in sorted(runtime_cells.items()):
        level = key.split("|", 1)[0]
        if not _cell_has_independent_support(
            level,
            independent_windows=independent_windows,
            unique_markets=unique_markets,
        ):
            continue
        probability = (positives + beta_strength * runtime_global_probability) / (
            samples + beta_strength
        )
        serialized_cells[key] = {
            "probability": str(probability),
            "conditional_fill_fraction": str(
                (
                    positive_fraction_sum
                    + beta_strength * runtime_global_conditional_fraction
                )
                / (positives + beta_strength)
                if positives or beta_strength
                else runtime_global_conditional_fraction
            ),
            "samples": samples,
            "positive_samples": positives,
            "independent_windows": independent_windows,
            "unique_markets": unique_markets,
        }
    artifact["model_version"] = args.model_version
    artifact["activation"] = (
        "PML2_HOLDOUT_HIERARCHICAL_PROMOTED_LOCAL_RESEARCH"
        if promotion_allowed
        else "PML2_HOLDOUT_HIERARCHICAL_RESEARCH_ONLY"
    )
    artifact["promotion_allowed"] = promotion_allowed
    artifact["domain_gate"] = {
        "abstain_categories": sorted(abstain_categories),
        "abstain_category_families": sorted(abstain_families),
        "validation_abstain_row_share": excluded_share,
        "validation_abstain_positive_share": excluded_positive_share,
        "maximum_abstain_row_share": args.maximum_abstain_row_share,
        "maximum_abstain_positive_share": args.maximum_abstain_positive_share,
        "passed": domain_gate_passed,
    }
    artifact["hierarchical_probability"] = {
        "contract": ("CATEGORY_FAMILY_PRICE_ACTIVITY_WITH_ACTIVITY_LEVEL_FALLBACK"),
        "probability_target": probability_target,
        "reference_model": reference_model,
        "fit_scope": fit_scope,
        "runtime_lob_usage": "NONE",
        "key_scheme": key_scheme,
        "minimum_samples": minimum,
        "minimum_market_cell_windows": MINIMUM_MARKET_CELL_WINDOWS,
        "minimum_context_cell_windows": MINIMUM_CONTEXT_CELL_WINDOWS,
        "minimum_category_cell_markets": MINIMUM_CATEGORY_CELL_MARKETS,
        "minimum_family_cell_markets": MINIMUM_FAMILY_CELL_MARKETS,
        "minimum_generic_cell_markets": MINIMUM_GENERIC_CELL_MARKETS,
        "beta_strength": str(beta_strength),
        "blend_strength": str(selected["blend_strength"]),
        "probability_scale": str(selected["probability_scale"]),
        "supported_categories": sorted(supported_categories),
        "supported_category_families": sorted(supported_families),
        "global_probability": str(runtime_global_probability),
        "global_conditional_fill_fraction": str(runtime_global_conditional_fraction),
        "refit_contract": (
            "CALIBRATION_PLUS_SELECTION_HOLDOUT"
            if refit_with_validation
            else "CALIBRATION_ONLY"
        ),
        "fallback_categories": sorted(fallback_categories),
        "fallback_category_families": sorted(fallback_families),
        "cells": serialized_cells,
        "selection": selected,
        "gates": (
            {
                "mode": "adaptive",
                "reference": "MATCHING_CONTRACT_SAMPLE_CLIMATOLOGY",
                "decision_rule": "PAIRED_MARKET_DAY_CLUSTER_BOOTSTRAP",
                "confidence_level": float(getattr(args, "confidence_level", 0.95)),
                "bootstrap_replicates": int(
                    getattr(args, "bootstrap_replicates", 1_000)
                ),
                "minimum_adaptive_clusters": int(
                    getattr(args, "minimum_adaptive_clusters", 20)
                ),
                "legacy_fixed_thresholds_applied": False,
            }
            if quality_gate_mode == "adaptive"
            else {
                "mode": "fixed",
                "maximum_brier_score": args.maximum_brier_score,
                "maximum_group_brier_score": getattr(
                    args, "maximum_group_brier_score", 0.25
                ),
                "maximum_brier_regret": getattr(args, "maximum_brier_regret", 0.01),
                "maximum_family_brier_regret": getattr(
                    args, "maximum_family_brier_regret", 0.01
                ),
                "minimum_ratio": args.minimum_ratio,
                "maximum_ratio": args.maximum_ratio,
                "legacy_fixed_thresholds_applied": True,
            }
        ),
    }
    artifact.setdefault("cohorts", {}).update(
        {
            "hierarchical_calibration_fingerprint": _fingerprint(calibration),
            "hierarchical_validation_fingerprint": _fingerprint(validation),
            "hierarchical_calibration_rows": len(calibration),
            "hierarchical_validation_rows": len(validation),
            "hierarchical_runtime_refit_rows": len(runtime_rows),
            "hierarchical_runtime_refit_fingerprint": _fingerprint(runtime_rows),
        }
    )
    artifact["generated_at"] = datetime.now(timezone.utc).isoformat()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return artifact


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-artifact", type=Path, default=DEFAULT_ARTIFACT)
    parser.add_argument("--calibration-orders", type=Path, nargs="+", required=True)
    parser.add_argument("--validation-orders", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_ARTIFACT)
    parser.add_argument(
        "--probability-target",
        choices=(PROBABILITY_TARGET_FAK_ANY_FILL, PROBABILITY_TARGET_FOK_FULL_FILL),
    )
    parser.add_argument("--reference-model")
    parser.add_argument("--source-model")
    parser.add_argument(
        "--model-version", default="fill_only_v3_l2_reference_hierarchical_v2"
    )
    parser.add_argument(
        "--key-scheme",
        choices=(
            HIERARCHY_KEY_SCHEME_LEGACY,
            HIERARCHY_KEY_SCHEME_VALIDATION_ALIGNED,
        ),
        default=HIERARCHY_KEY_SCHEME_VALIDATION_ALIGNED,
    )
    parser.add_argument("--minimum-samples", type=int, nargs="+", default=[10, 20, 30])
    parser.add_argument(
        "--beta-strength", type=float, nargs="+", default=[2, 5, 10, 20]
    )
    parser.add_argument(
        "--blend-strength", type=float, nargs="+", default=[5, 10, 20, 50]
    )
    parser.add_argument("--probability-scale", type=float, nargs="+", default=[1.0])
    parser.add_argument("--maximum-brier-score", type=float, default=0.20)
    parser.add_argument("--maximum-group-brier-score", type=float, default=0.25)
    parser.add_argument("--maximum-brier-regret", type=float, default=0.01)
    parser.add_argument("--maximum-family-brier-regret", type=float, default=0.01)
    parser.add_argument("--minimum-ratio", type=float, default=0.85)
    parser.add_argument("--maximum-ratio", type=float, default=1.15)
    parser.add_argument(
        "--quality-gate-mode",
        choices=("adaptive", "fixed"),
        default="adaptive",
        help="adaptive uses clustered confidence intervals; fixed reproduces legacy gates",
    )
    parser.add_argument("--confidence-level", type=float, default=0.95)
    parser.add_argument("--bootstrap-replicates", type=int, default=1_000)
    parser.add_argument("--bootstrap-seed", type=int, default=73)
    parser.add_argument("--minimum-adaptive-clusters", type=int, default=20)
    parser.add_argument("--minimum-family-samples", type=int, default=100)
    parser.add_argument("--minimum-family-markets", type=int, default=20)
    parser.add_argument("--minimum-category-samples", type=int, default=100)
    parser.add_argument("--minimum-category-markets", type=int, default=5)
    parser.add_argument("--maximum-abstain-row-share", type=float, default=0.35)
    parser.add_argument("--maximum-abstain-positive-share", type=float, default=0.15)
    parser.add_argument("--refit-with-validation", action="store_true")
    return parser


def main() -> int:
    args = _parser().parse_args()
    artifact = calibrate(args)
    print(
        json.dumps(
            {
                "output": str(args.output),
                "promotion_allowed": artifact["promotion_allowed"],
                "selection": artifact["hierarchical_probability"]["selection"],
            },
            indent=2,
        )
    )
    return 0 if artifact["promotion_allowed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
