#!/usr/bin/env python3
"""Calibrate an OrderFilled-only immediate-FAK model from offline PML2 labels.

The fitted artifact contains only coefficients for pre-arrival OrderFilled
features. PML2 is used here as an offline executability label and is never read
by the runtime model.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.orderfilled_probability import (  # noqa: E402
    ARRIVAL_FEATURE_CONTRACT,
    FEATURE_NAMES,
    PROBABILITY_TARGET_FAK_ANY_FILL,
    PROBABILITY_TARGET_FOK_FULL_FILL,
    augment_orderfilled_probability_vector,
)
from quant.backtest.probability_quality import (  # noqa: E402
    adaptive_probability_quality,
)

DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "config"
    / "execution"
    / "fill_only_v3_l2_reference_probability.v1.json"
)


def _orders_path(path: Path) -> Path:
    return path / "orders.jsonl" if path.is_dir() else path


def _load_rows(
    paths: Sequence[Path],
    *,
    reference_model: str = "pml2_fak",
    probability_target: str = PROBABILITY_TARGET_FAK_ANY_FILL,
    source_model: str | None = None,
    source_required: bool = True,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw_path in paths:
        path = _orders_path(raw_path)
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
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
                try:
                    reference = row["models"][reference_model]
                    source = _source_result(
                        row["models"],
                        preferred=source_model,
                        allow_missing=not source_required,
                    )
                    vector = _augmented_vector(row)
                except KeyError as exc:
                    raise RuntimeError(
                        f"missing calibration field {exc} at {path}:{line_number}"
                    ) from exc
                if reference.get("status") == "DATA_NOT_READY":
                    continue
                row_tif = str(
                    row.get("tif")
                    or (
                        "FOK"
                        if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
                        else "FAK"
                    )
                ).upper()
                expected_tif = (
                    "FOK"
                    if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
                    else "FAK"
                )
                if row_tif != expected_tif:
                    continue
                size = float(row["size"])
                reference_size = float(reference["filled_size"])
                label = (
                    int(reference_size >= size - 1e-9)
                    if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
                    else int(reference_size > 0)
                )
                rows.append(
                    {
                        "order_id": str(row["order_id"]),
                        "market_id": int(row["market_id"]),
                        "asset_id": str(row["asset_id"]),
                        "category": str(row.get("category") or "unknown").lower(),
                        "category_family": _category_family(row),
                        "decision_ts": str(row["decision_ts"]),
                        "side": str(row.get("side") or "BUY").upper(),
                        "tif": row_tif,
                        "amount_unit": str(row.get("amount_unit") or "SHARES").upper(),
                        "order_size": size,
                        "label": label,
                        "conditional_fraction": (
                            1.0
                            if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
                            and label
                            else max(0.0, min(1.0, reference_size / size))
                            if size > 0
                            else 0.0
                        ),
                        "source_positive": float(source["filled_size"]) > 0,
                        "source_fraction": (
                            max(0.0, min(1.0, float(source["filled_size"]) / size))
                            if size > 0
                            else 0.0
                        ),
                        "features": [float(vector[name]) for name in FEATURE_NAMES],
                    }
                )
    if not rows:
        raise RuntimeError("no labeled PML2 rows were loaded")
    return rows


def _source_result(
    models: Mapping[str, Any],
    *,
    preferred: str | None = None,
    allow_missing: bool = False,
) -> Mapping[str, Any]:
    for name in (
        preferred,
        "v3_source_contract",
        "v3_source_fok",
        "v3_source_fak",
    ):
        if not name:
            continue
        source = models.get(name)
        if isinstance(source, Mapping):
            return source

    # Contract-aware expected results retain the observed source lower bound in
    # diagnostics even when the standalone source model was not requested.
    for name in ("v3_contract_expected", "v3_contract_probability"):
        result = models.get(name)
        if not isinstance(result, Mapping):
            continue
        diagnostics = result.get("model_diagnostics")
        if not isinstance(diagnostics, Mapping):
            continue
        source_size = diagnostics.get("source_confirmed_lower_bound_size")
        if source_size is not None:
            return {"filled_size": source_size}
        selected_route = diagnostics.get("selected_route")
        if selected_route == "taker_source_confirmed":
            return result
        if selected_route == "taker_hierarchical_expected":
            return {"filled_size": "0"}

    central = models.get("v3_l2_expected")
    if not isinstance(central, Mapping):
        if allow_missing:
            return {"filled_size": "0"}
        raise KeyError("v3_source_fak")
    diagnostics = central.get("model_diagnostics")
    selected_route = (
        diagnostics.get("selected_route") if isinstance(diagnostics, Mapping) else None
    )
    if selected_route == "taker_source_confirmed":
        return central
    return {"filled_size": "0"}


def _category_family(row: Mapping[str, Any]) -> str:
    text = " ".join(
        str(row.get(name) or "").strip().lower().replace("_", "-")
        for name in ("category", "slug", "title")
    )
    groups = (
        (
            "esports",
            (
                "league-of-legends",
                " lol ",
                "lol",
                "esports",
                "counter-strike",
                "valorant",
                "dota",
            ),
        ),
        (
            "sports",
            (
                "sports",
                "nba",
                "nfl",
                "mlb",
                "nhl",
                "soccer",
                "football",
                "basketball",
                "baseball",
                "hockey",
                "ufc",
                "tennis",
                "atp",
                "wta",
                "cfb",
                "ncaa",
                "golf",
                "pga",
                "cricket",
                "formula-1",
            ),
        ),
        ("crypto", ("crypto", "bitcoin", "ethereum", "solana", "xrp", "dogecoin")),
        (
            "economics",
            (
                "economy",
                "finance",
                "fed-rate",
                "interest-rate",
                "inflation",
                "gdp",
                "recession",
            ),
        ),
        (
            "politics",
            (
                "politic",
                "democratic",
                "republican",
                "congress",
                "senate",
                "election",
                "president",
                "prime-minister",
                "geopolit",
                "war",
                "ceasefire",
                "iran",
                "israel",
                "ukraine",
                "russia",
                "china",
                "japan",
                "cuba",
                "fedorov",
            ),
        ),
    )
    for family, markers in groups:
        if any(marker in text for marker in markers):
            return family
    return "other"


def _activity_regime(row: Mapping[str, Any]) -> str:
    total = round(math.expm1(float(row["features"][15])))
    if total <= 5:
        return "SPARSE_LE_5"
    if total <= 50:
        return "MEDIUM_6_TO_50"
    return "ACTIVE_GT_50"


def _unstable_activity_regimes(
    earlier: Sequence[Mapping[str, Any]],
    later: Sequence[Mapping[str, Any]],
    args: argparse.Namespace,
) -> tuple[set[str], dict[str, dict[str, Any]]]:
    diagnostics: dict[str, dict[str, Any]] = {}
    unstable: set[str] = set()
    for regime in ("SPARSE_LE_5", "MEDIUM_6_TO_50", "ACTIVE_GT_50"):
        left = [row for row in earlier if _activity_regime(row) == regime]
        right = [row for row in later if _activity_regime(row) == regime]
        left_markets = len({row["market_id"] for row in left})
        right_markets = len({row["market_id"] for row in right})
        enough = (
            len(left) >= args.minimum_temporal_activity_rows
            and len(right) >= args.minimum_temporal_activity_rows
            and left_markets >= args.minimum_temporal_activity_markets
            and right_markets >= args.minimum_temporal_activity_markets
        )
        left_rate = sum(row["label"] for row in left) / len(left) if left else None
        right_rate = sum(row["label"] for row in right) / len(right) if right else None
        drift = (
            abs(float(left_rate) - float(right_rate))
            if left_rate is not None and right_rate is not None
            else None
        )
        if (
            enough
            and drift is not None
            and drift > args.maximum_temporal_activity_drift
        ):
            unstable.add(regime)
        diagnostics[regime] = {
            "earlier_rows": len(left),
            "later_rows": len(right),
            "earlier_markets": left_markets,
            "later_markets": right_markets,
            "earlier_positive_rate": left_rate,
            "later_positive_rate": right_rate,
            "absolute_drift": drift,
            "enough_sample": enough,
            "supported": regime not in unstable,
        }
    return unstable, diagnostics


def _feature_value(
    row: Mapping[str, Any], vector: Mapping[str, Any], name: str
) -> float:
    if name in vector:
        return float(vector[name])
    features = row["fill_only_features"]
    same_count = int(features["trailing_same_count"])
    opposite_count = int(features["trailing_opposite_count"])
    family = _category_family(row)
    derived = {
        "local_trade_present": float(same_count + opposite_count > 0),
        "two_sided_trade_present": float(same_count > 0 and opposite_count > 0),
        "log_total_trade_count": math.log1p(same_count + opposite_count),
        "limit_price": float(row["limit_price"]),
        "category_sports": float(family == "sports"),
        "category_esports": float(family == "esports"),
        "category_crypto": float(family == "crypto"),
        "category_economics": float(family == "economics"),
        "category_politics": float(family == "politics"),
        "category_other": float(family == "other"),
    }
    return float(derived[name])


def _augmented_vector(row: Mapping[str, Any]) -> dict[str, Any]:
    features = row["fill_only_features"]
    raw = dict(features["vector"])
    for name in (
        "local_trade_present",
        "two_sided_trade_present",
        "log_total_trade_count",
        "limit_price",
        "category_sports",
        "category_esports",
        "category_crypto",
        "category_economics",
        "category_politics",
        "category_other",
    ):
        raw.setdefault(name, _feature_value(row, raw, name))
    return augment_orderfilled_probability_vector(
        raw,
        last_same_age_seconds=features.get("last_same_age_seconds"),
        last_opposite_age_seconds=features.get("last_opposite_age_seconds"),
        order_size=row.get("size") or row.get("order_size"),
    )


def _fingerprint(rows: Iterable[Mapping[str, Any]]) -> str:
    identity = [
        (row["order_id"], row["market_id"], row["asset_id"], row["decision_ts"])
        for row in rows
    ]
    payload = json.dumps(sorted(identity), separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def _assert_disjoint(
    calibration: Sequence[Mapping[str, Any]],
    validation: Sequence[Mapping[str, Any]],
    probability_calibration: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    cohorts = {
        "fit": calibration,
        "probability_calibration": probability_calibration or (),
        "validation": validation,
    }
    identities = {
        name: {(row["market_id"], row["asset_id"], row["decision_ts"]) for row in rows}
        for name, rows in cohorts.items()
    }
    names = tuple(identities)
    for left_index, left_name in enumerate(names):
        for right_name in names[left_index + 1 :]:
            overlap = identities[left_name].intersection(identities[right_name])
            if overlap:
                raise RuntimeError(
                    f"{left_name} and {right_name} overlap: {len(overlap)} rows"
                )


def _matrix(rows: Sequence[Mapping[str, Any]]) -> tuple[np.ndarray, np.ndarray]:
    x = np.asarray([row["features"] for row in rows], dtype=np.float64)
    y = np.asarray([row["label"] for row in rows], dtype=np.float64)
    return x, y


def _sigmoid(values: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(values, -40.0, 40.0)))


def _fit_logistic(
    x: np.ndarray,
    y: np.ndarray,
    *,
    ridge: float,
    positive_weight: float,
    iterations: int = 100,
) -> np.ndarray:
    design = np.column_stack([np.ones(len(x)), x])
    beta = np.zeros(design.shape[1], dtype=np.float64)
    penalties = np.ones_like(beta) * ridge
    penalties[0] = 0.0
    sample_weight = np.where(y > 0.5, positive_weight, 1.0)
    for _ in range(iterations):
        probability = _sigmoid(design @ beta)
        variance = np.maximum(probability * (1.0 - probability), 1e-7)
        gradient = design.T @ (sample_weight * (y - probability)) - penalties * beta
        hessian = design.T @ ((sample_weight * variance)[:, None] * design) + np.diag(
            penalties
        )
        try:
            step = np.linalg.solve(hessian, gradient)
        except np.linalg.LinAlgError:
            step = np.linalg.lstsq(hessian, gradient, rcond=None)[0]
        beta += step
        if float(np.max(np.abs(step))) < 1e-8:
            break
    return beta


def _probabilities(x: np.ndarray, beta: np.ndarray) -> np.ndarray:
    return _sigmoid(np.column_stack([np.ones(len(x)), x]) @ beta)


def _fit_platt(
    probability: np.ndarray, y: np.ndarray
) -> tuple[np.ndarray, float, float, np.ndarray, np.ndarray]:
    clipped = np.clip(probability, 1e-6, 1.0 - 1e-6)
    logits = np.log(clipped / (1.0 - clipped))
    mean = float(np.mean(logits))
    scale = float(np.std(logits))
    if scale < 1e-8:
        scale = 1.0
    beta = _fit_logistic(
        ((logits - mean) / scale)[:, None],
        y,
        ridge=1.0,
        positive_weight=1.0,
    )
    grid_x = np.unique(np.r_[0.0, probability, 1.0])
    grid_logits = np.log(
        np.clip(grid_x, 1e-6, 1.0 - 1e-6) / (1.0 - np.clip(grid_x, 1e-6, 1.0 - 1e-6))
    )
    grid_y = _sigmoid(beta[0] + beta[1] * ((grid_logits - mean) / scale))
    calibrated = np.asarray(np.interp(probability, grid_x, grid_y), dtype=np.float64)
    return calibrated, mean, scale, grid_x, grid_y


def _fit_binned_isotonic(
    probability: np.ndarray,
    y: np.ndarray,
    *,
    minimum_bin_size: int,
) -> tuple[np.ndarray, float, float, np.ndarray, np.ndarray]:
    order = np.argsort(probability, kind="stable")
    sorted_probability = probability[order]
    sorted_y = y[order]
    blocks: list[list[float]] = []
    bin_size = max(10, int(minimum_bin_size))
    for start in range(0, len(order), bin_size):
        stop = min(len(order), start + bin_size)
        weight = float(stop - start)
        blocks.append(
            [
                float(np.mean(sorted_probability[start:stop])),
                float(np.mean(sorted_y[start:stop])),
                weight,
            ]
        )
    index = 1
    while index < len(blocks):
        if blocks[index - 1][1] <= blocks[index][1]:
            index += 1
            continue
        left = blocks[index - 1]
        right = blocks[index]
        weight = left[2] + right[2]
        blocks[index - 1 : index + 1] = [
            [
                (left[0] * left[2] + right[0] * right[2]) / weight,
                (left[1] * left[2] + right[1] * right[2]) / weight,
                weight,
            ]
        ]
        index = max(1, index - 1)
    grid_x = np.asarray([0.0, *(row[0] for row in blocks), 1.0], dtype=np.float64)
    grid_y = np.asarray(
        [blocks[0][1], *(row[1] for row in blocks), blocks[-1][1]],
        dtype=np.float64,
    )
    calibrated = np.interp(probability, grid_x, grid_y)
    return calibrated, 0.0, 1.0, grid_x, grid_y


def _fit_calibration_curve(
    probability: np.ndarray,
    labels: np.ndarray,
    args: argparse.Namespace,
) -> tuple[float, float, np.ndarray, np.ndarray]:
    if args.calibration_method == "isotonic":
        _, mean, scale, grid_x, grid_y = _fit_binned_isotonic(
            probability,
            labels,
            minimum_bin_size=args.minimum_isotonic_bin_size,
        )
    else:
        _, mean, scale, grid_x, grid_y = _fit_platt(probability, labels)
    return mean, scale, grid_x, grid_y


def _family_calibration_curves(
    probability: np.ndarray,
    labels: np.ndarray,
    rows: Sequence[Mapping[str, Any]],
    args: argparse.Namespace,
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        groups[str(row["category_family"])].append(index)
    result: dict[str, dict[str, Any]] = {}
    for family, indexes in groups.items():
        selected = np.asarray(indexes, dtype=int)
        family_labels = labels[selected]
        markets = len({rows[index]["market_id"] for index in indexes})
        if (
            len(indexes) < args.minimum_family_calibration_rows
            or markets < args.minimum_family_calibration_markets
            or len(np.unique(family_labels)) < 2
        ):
            continue
        _, _, grid_x, grid_y = _fit_calibration_curve(
            probability[selected], family_labels, args
        )
        result[family] = {
            "x": grid_x,
            "y": grid_y,
            "rows": len(indexes),
            "markets": markets,
        }
    return result


def _apply_calibration_curves(
    probability: np.ndarray,
    rows: Sequence[Mapping[str, Any]],
    global_x: np.ndarray,
    global_y: np.ndarray,
    family_curves: Mapping[str, Mapping[str, Any]],
) -> np.ndarray:
    calibrated = np.asarray(
        np.interp(probability, global_x, global_y), dtype=np.float64
    )
    for family, curve in family_curves.items():
        selected = np.asarray(
            [row["category_family"] == family for row in rows], dtype=bool
        )
        if np.any(selected):
            calibrated[selected] = np.interp(
                probability[selected], curve["x"], curve["y"]
            )
    return calibrated


def _metric(
    y: np.ndarray, predicted: np.ndarray, probability: np.ndarray
) -> dict[str, Any]:
    positive = y > 0.5
    tp = int(np.sum(predicted & positive))
    fp = int(np.sum(predicted & ~positive))
    tn = int(np.sum(~predicted & ~positive))
    fn = int(np.sum(~predicted & positive))

    def ratio(numerator: int, denominator: int) -> float | None:
        return numerator / denominator if denominator else None

    precision = ratio(tp, tp + fp)
    recall = ratio(tp, tp + fn)
    fpr = ratio(fp, fp + tn)
    specificity = ratio(tn, tn + fp)
    balanced_accuracy = (
        ((recall or 0.0) + (specificity or 0.0)) / 2.0
        if (tp + fn) and (tn + fp)
        else 0.0
    )
    return {
        "samples": len(y),
        "positive_samples": int(np.sum(positive)),
        "predicted_positive_samples": int(np.sum(predicted)),
        "true_positive": tp,
        "false_positive": fp,
        "true_negative": tn,
        "false_negative": fn,
        "precision": precision,
        "recall": recall,
        "false_positive_rate": fpr,
        "false_negative_rate": ratio(fn, tp + fn),
        "accuracy": ratio(tp + tn, len(y)),
        "balanced_accuracy": balanced_accuracy,
        "positive_count_ratio": ratio(tp + fp, tp + fn),
        "brier_score": float(np.mean((probability - y) ** 2)),
    }


def _passes_metric_gates(metrics: Mapping[str, Any], args: argparse.Namespace) -> bool:
    values = (
        metrics.get("precision"),
        metrics.get("recall"),
        metrics.get("false_positive_rate"),
        metrics.get("positive_count_ratio"),
    )
    if any(value is None for value in values):
        return False
    precision_raw, recall_raw, fpr_raw, ratio_raw = values
    assert precision_raw is not None
    assert recall_raw is not None
    assert fpr_raw is not None
    assert ratio_raw is not None
    precision = float(precision_raw)
    recall = float(recall_raw)
    fpr = float(fpr_raw)
    ratio = float(ratio_raw)
    return (
        precision >= args.minimum_precision
        and recall >= args.minimum_recall
        and fpr <= args.maximum_false_positive_rate
        and args.minimum_positive_count_ratio
        <= ratio
        <= args.maximum_positive_count_ratio
    )


def _thresholds(probability: np.ndarray) -> list[float]:
    values = {float(value) for value in probability}
    values.update(float(value) for value in np.linspace(0.01, 0.99, 99))
    return sorted(values)


def _candidate_thresholds(
    probability: np.ndarray, selection_objective: str
) -> Sequence[float]:
    # Expected-fill profiles do not hard-reject on this classification threshold.
    return (0.0,) if selection_objective == "expected" else _thresholds(probability)


def _score(metrics: Mapping[str, Any]) -> tuple[float, float, float]:
    precision = float(metrics.get("precision") or 0.0)
    recall = float(metrics.get("recall") or 0.0)
    fpr = float(metrics.get("false_positive_rate") or 1.0)
    ratio = float(metrics.get("positive_count_ratio") or 0.0)
    f1 = 2.0 * precision * recall / (precision + recall) if precision + recall else 0.0
    return (
        f1 - 0.35 * fpr - 0.15 * abs(1.0 - ratio),
        float(metrics.get("balanced_accuracy") or 0.0),
        precision,
    )


def _expected_metrics(
    rows: Sequence[Mapping[str, Any]],
    probability: np.ndarray,
    conditional_fraction: np.ndarray,
) -> dict[str, float | int | None]:
    labels = np.asarray([row["label"] for row in rows], dtype=np.float64)
    source = np.asarray([row["source_positive"] for row in rows], dtype=bool)
    source_fraction = np.asarray(
        [row["source_fraction"] for row in rows], dtype=np.float64
    )
    reference_fraction = np.asarray(
        [row["conditional_fraction"] for row in rows], dtype=np.float64
    )
    combined_probability = np.where(source, 1.0, probability)
    conditional_fraction = np.clip(conditional_fraction, 0.0, 1.0)
    expected_fraction = np.where(
        source,
        np.maximum(source_fraction, conditional_fraction),
        combined_probability * conditional_fraction,
    )
    reference_orders = float(np.sum(labels))
    reference_quantity = float(np.sum(reference_fraction))
    brier_score = float(np.mean((combined_probability - labels) ** 2))
    positive_rate = float(np.mean(labels))
    climatology_brier_score = positive_rate * (1.0 - positive_rate)
    return {
        "samples": len(rows),
        "brier_score": brier_score,
        "climatology_brier_score": climatology_brier_score,
        "brier_regret": brier_score - climatology_brier_score,
        "expected_positive_orders": float(np.sum(combined_probability)),
        "reference_positive_orders": reference_orders,
        "expected_positive_order_ratio": (
            float(np.sum(combined_probability)) / reference_orders
            if reference_orders
            else None
        ),
        "expected_quantity_fraction": float(np.sum(expected_fraction)),
        "reference_quantity_fraction": reference_quantity,
        "expected_quantity_ratio": (
            float(np.sum(expected_fraction)) / reference_quantity
            if reference_quantity
            else None
        ),
    }


def _adaptive_expected_quality(
    rows: Sequence[Mapping[str, Any]],
    probability: np.ndarray,
    conditional_fraction: np.ndarray,
    args: argparse.Namespace,
    *,
    minimum_clusters: int | None = None,
) -> dict[str, Any]:
    if not rows:
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
    labels = np.asarray([row["label"] for row in rows], dtype=np.float64)
    source = np.asarray([row["source_positive"] for row in rows], dtype=bool)
    source_fraction = np.asarray(
        [row["source_fraction"] for row in rows], dtype=np.float64
    )
    reference_fraction = np.asarray(
        [row["conditional_fraction"] for row in rows], dtype=np.float64
    )
    order_sizes = np.asarray(
        [float(row.get("order_size") or 1.0) for row in rows], dtype=np.float64
    )
    combined_probability = np.where(source, 1.0, probability)
    modeled_fraction = np.clip(conditional_fraction, 0.0, 1.0)
    expected_fraction = np.where(
        source,
        np.maximum(source_fraction, modeled_fraction),
        combined_probability * modeled_fraction,
    )
    cluster_ids = [
        f"{row['market_id']}:{str(row.get('decision_ts') or '')[:10]}" for row in rows
    ]
    return adaptive_probability_quality(
        combined_probability,
        labels,
        expected_quantities=expected_fraction * order_sizes,
        reference_quantities=reference_fraction * order_sizes,
        quantity_scales=order_sizes,
        cluster_ids=cluster_ids,
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


def _adaptive_expected_strata_quality(
    rows: Sequence[Mapping[str, Any]],
    probability: np.ndarray,
    conditional_fraction: np.ndarray,
    args: argparse.Namespace,
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        groups[str(row["category_family"])].append(index)
    result: dict[str, dict[str, Any]] = {}
    for name, indexes in sorted(groups.items()):
        selected = np.asarray(indexes, dtype=int)
        result[name] = _adaptive_expected_quality(
            [rows[index] for index in indexes],
            probability[selected],
            conditional_fraction[selected],
            args,
            minimum_clusters=int(getattr(args, "minimum_stratum_markets", 20)),
        )
    return result


def _expected_score(metrics: Mapping[str, Any]) -> float:
    order_ratio = float(metrics.get("expected_positive_order_ratio") or 0.0)
    quantity_ratio = float(metrics.get("expected_quantity_ratio") or 0.0)
    return (
        float(metrics["brier_score"])
        + 0.5 * abs(1.0 - order_ratio)
        + 0.5 * abs(1.0 - quantity_ratio)
    )


def _passes_expected_gates(
    metrics: Mapping[str, Any], args: argparse.Namespace
) -> bool:
    order_ratio = metrics.get("expected_positive_order_ratio")
    quantity_ratio = metrics.get("expected_quantity_ratio")
    return (
        order_ratio is not None
        and quantity_ratio is not None
        and float(metrics["brier_score"]) <= args.maximum_expected_brier_score
        and float(metrics["brier_regret"]) <= args.maximum_expected_brier_regret
        and args.minimum_expected_ratio
        <= float(order_ratio)
        <= args.maximum_expected_ratio
        and args.minimum_expected_ratio
        <= float(quantity_ratio)
        <= args.maximum_expected_ratio
    )


def _expected_gate_penalty(
    metrics: Mapping[str, Any], args: argparse.Namespace
) -> float:
    order_ratio = metrics.get("expected_positive_order_ratio")
    quantity_ratio = metrics.get("expected_quantity_ratio")
    if order_ratio is None or quantity_ratio is None:
        return float("inf")
    brier_penalty = max(
        max(
            0.0,
            float(metrics["brier_score"]) - args.maximum_expected_brier_score,
        ),
        max(
            0.0,
            float(metrics["brier_regret"]) - args.maximum_expected_brier_regret,
        ),
    )
    return max(
        brier_penalty,
        args.minimum_expected_ratio - float(order_ratio),
        float(order_ratio) - args.maximum_expected_ratio,
        args.minimum_expected_ratio - float(quantity_ratio),
        float(quantity_ratio) - args.maximum_expected_ratio,
        0.0,
    )


def _fit_capacity(
    x: np.ndarray, y: np.ndarray, fractions: np.ndarray, *, ridge: float
) -> tuple[np.ndarray, float, dict[str, Any]]:
    positive = y > 0.5
    if not np.any(positive):
        return (
            np.r_[0.0, np.zeros(x.shape[1])],
            0.0,
            {"method": "NO_POSITIVE_ROWS", "positive_rows": 0, "full_fill_share": 0.0},
        )
    positive_fractions = fractions[positive]
    full_fill_share = float(np.mean(positive_fractions >= 1.0 - 1e-9))
    target_range = float(np.ptp(positive_fractions))
    if target_range <= 1e-12:
        beta = np.r_[float(np.mean(positive_fractions)), np.zeros(x.shape[1])]
        method = "CONSTANT_TARGET"
    else:
        design = np.column_stack([np.ones(int(np.sum(positive))), x[positive]])
        penalty = np.eye(design.shape[1]) * ridge
        penalty[0, 0] = 0.0
        beta = np.linalg.solve(
            design.T @ design + penalty, design.T @ positive_fractions
        )
        method = "RIDGE_LINEAR"
    conservative = float(np.quantile(positive_fractions, 0.25))
    return (
        beta,
        conservative,
        {
            "method": method,
            "positive_rows": int(np.sum(positive)),
            "full_fill_share": full_fill_share,
            "target_range": target_range,
            "conditional_mean": float(np.mean(positive_fractions)),
        },
    )


def _strata_metrics(
    rows: Sequence[Mapping[str, Any]], probability: np.ndarray, threshold: float
) -> dict[str, Any]:
    groups: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        groups[str(row["category_family"])].append(index)
    result: dict[str, Any] = {}
    labels = np.asarray([row["label"] for row in rows], dtype=np.float64)
    for name, indexes in sorted(groups.items()):
        selected = np.asarray(indexes, dtype=int)
        result[name] = {
            **_metric(
                labels[selected],
                probability[selected] >= threshold,
                probability[selected],
            ),
            "unique_markets": len({rows[index]["market_id"] for index in indexes}),
        }
    return result


def _expected_strata_metrics(
    rows: Sequence[Mapping[str, Any]],
    probability: np.ndarray,
    conditional_fraction: np.ndarray,
) -> dict[str, Any]:
    groups: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(rows):
        groups[str(row["category_family"])].append(index)
    result: dict[str, Any] = {}
    for name, indexes in sorted(groups.items()):
        selected = np.asarray(indexes, dtype=int)
        result[name] = {
            **_expected_metrics(
                [rows[index] for index in indexes],
                probability[selected],
                conditional_fraction[selected],
            ),
            "unique_markets": len({rows[index]["market_id"] for index in indexes}),
        }
    return result


def calibrate(args: argparse.Namespace) -> dict[str, Any]:
    quality_gate_mode = str(getattr(args, "quality_gate_mode", "fixed")).lower()
    probability_target = str(
        getattr(args, "probability_target", PROBABILITY_TARGET_FAK_ANY_FILL)
    ).upper()
    reference_model = str(
        getattr(args, "reference_model", None)
        or (
            "pml2_fok"
            if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
            else "pml2_fak"
        )
    )
    source_model = getattr(args, "source_model", None)
    fit_scope = str(getattr(args, "fit_scope", "residual")).lower()
    calibration = _load_rows(
        args.calibration_orders,
        reference_model=reference_model,
        probability_target=probability_target,
        source_model=source_model,
        source_required=fit_scope != "all",
    )
    probability_calibration_paths = getattr(
        args, "probability_calibration_orders", None
    )
    probability_calibration = (
        _load_rows(
            probability_calibration_paths,
            reference_model=reference_model,
            probability_target=probability_target,
            source_model=source_model,
            source_required=fit_scope != "all",
        )
        if probability_calibration_paths
        else calibration
    )
    validation = _load_rows(
        args.validation_orders,
        reference_model=reference_model,
        probability_target=probability_target,
        source_model=source_model,
        source_required=fit_scope != "all",
    )
    _assert_disjoint(
        calibration,
        validation,
        probability_calibration if probability_calibration_paths else None,
    )
    if fit_scope == "all":
        for cohort in (calibration, probability_calibration, validation):
            for row in cohort:
                row["observed_source_positive"] = row["source_positive"]
                row["source_positive"] = False
                row["source_fraction"] = 0.0
        fit_rows = list(calibration)
        probability_calibration_rows = list(probability_calibration)
    else:
        fit_rows = [row for row in calibration if not row["source_positive"]]
        probability_calibration_rows = [
            row for row in probability_calibration if not row["source_positive"]
        ]
    if len(fit_rows) < 20:
        raise RuntimeError(
            f"need at least 20 {fit_scope} calibration rows, got {len(fit_rows)}"
        )
    x_fit_raw, y_fit = _matrix(fit_rows)
    if len(np.unique(y_fit)) < 2:
        raise RuntimeError("calibration rows need both PML2 label classes")
    means = x_fit_raw.mean(axis=0)
    scales = x_fit_raw.std(axis=0)
    scales[scales < 1e-8] = 1.0
    x_fit = (x_fit_raw - means) / scales
    x_probability_calibration_raw, y_probability_calibration = _matrix(
        probability_calibration_rows
    )
    if len(np.unique(y_probability_calibration)) < 2:
        raise RuntimeError("probability calibration rows need both PML2 label classes")
    x_probability_calibration = (x_probability_calibration_raw - means) / scales
    x_validation_raw, y_validation = _matrix(validation)
    x_validation = (x_validation_raw - means) / scales
    validation_source = np.asarray(
        [row["source_positive"] for row in validation], dtype=bool
    )
    unstable_activity_regimes, activity_stability = _unstable_activity_regimes(
        probability_calibration, validation, args
    )

    candidates: list[dict[str, Any]] = []
    for ridge in args.ridge:
        capacity_beta, _, _ = _fit_capacity(
            x_fit,
            y_fit,
            np.asarray(
                [row["conditional_fraction"] for row in fit_rows], dtype=np.float64
            ),
            ridge=ridge,
        )
        validation_conditional_fraction = np.clip(
            np.column_stack([np.ones(len(x_validation)), x_validation]) @ capacity_beta,
            0.0,
            1.0,
        )
        for positive_weight in args.positive_weight:
            beta = _fit_logistic(
                x_fit,
                y_fit,
                ridge=ridge,
                positive_weight=positive_weight,
            )
            model_probability = _probabilities(x_validation, beta)
            probability_calibration_model_probability = _probabilities(
                x_probability_calibration, beta
            )
            (
                platt_mean,
                platt_scale,
                calibration_x,
                calibration_y,
            ) = _fit_calibration_curve(
                probability_calibration_model_probability,
                y_probability_calibration,
                args,
            )
            family_calibrations = _family_calibration_curves(
                probability_calibration_model_probability,
                y_probability_calibration,
                probability_calibration_rows,
                args,
            )
            calibrated_probability = _apply_calibration_curves(
                model_probability,
                validation,
                calibration_x,
                calibration_y,
                family_calibrations,
            )
            combined_probability = np.where(
                validation_source, 1.0, calibrated_probability
            )
            expected_metrics = _expected_metrics(
                validation,
                calibrated_probability,
                validation_conditional_fraction,
            )
            expected_strata = _expected_strata_metrics(
                validation,
                calibrated_probability,
                validation_conditional_fraction,
            )
            adaptive_quality = _adaptive_expected_quality(
                validation,
                calibrated_probability,
                validation_conditional_fraction,
                args,
            )
            adaptive_strata_quality = _adaptive_expected_strata_quality(
                validation,
                calibrated_probability,
                validation_conditional_fraction,
                args,
            )
            expected_strata_penalties = {
                family: _expected_gate_penalty(metrics, args)
                for family, metrics in expected_strata.items()
                if int(metrics["samples"]) >= args.minimum_stratum_rows
                and int(metrics["unique_markets"]) >= args.minimum_stratum_markets
                and int(metrics["reference_positive_orders"]) > 0
            }
            for threshold in _candidate_thresholds(
                combined_probability, args.selection_objective
            ):
                predicted = validation_source | (calibrated_probability >= threshold)
                metrics = _metric(y_validation, predicted, combined_probability)
                candidates.append(
                    {
                        "ridge": ridge,
                        "positive_weight": positive_weight,
                        "threshold": threshold,
                        "metrics": metrics,
                        "expected_metrics": expected_metrics,
                        "adaptive_probability_quality": adaptive_quality,
                        "adaptive_strata_quality": adaptive_strata_quality,
                        "expected_strata_penalties": expected_strata_penalties,
                        "expected_strata_gate_passed": bool(expected_strata_penalties)
                        and not any(expected_strata_penalties.values()),
                        "passes_metric_gates": _passes_metric_gates(metrics, args),
                        "beta": beta,
                        "model_probability": model_probability,
                        "calibrated_probability": calibrated_probability,
                        "combined_probability": combined_probability,
                        "platt_mean": platt_mean,
                        "platt_scale": platt_scale,
                        "calibration_x": calibration_x,
                        "calibration_y": calibration_y,
                        "family_calibrations": family_calibrations,
                    }
                )
    if args.selection_objective == "expected":
        if quality_gate_mode == "adaptive":
            expected_gated = [
                row
                for row in candidates
                if row["adaptive_probability_quality"]["status"] == "PASS"
                and not any(
                    quality["status"] == "FAIL"
                    for quality in row["adaptive_strata_quality"].values()
                )
            ]
            quality_rank = {
                "PASS": 0,
                "INCONCLUSIVE": 1,
                "INSUFFICIENT_SAMPLE": 2,
                "FAIL": 3,
            }
            selected = min(
                expected_gated or candidates,
                key=lambda row: (
                    quality_rank[row["adaptive_probability_quality"]["status"]],
                    sum(
                        quality["status"] == "FAIL"
                        for quality in row["adaptive_strata_quality"].values()
                    ),
                    _expected_score(row["expected_metrics"]),
                    tuple(-value for value in _score(row["metrics"])),
                ),
            )
        else:
            expected_gated = [
                row
                for row in candidates
                if _passes_expected_gates(row["expected_metrics"], args)
                and row["expected_strata_gate_passed"]
            ]
            selected = min(
                expected_gated or candidates,
                key=lambda row: (
                    max(
                        row["expected_strata_penalties"].values(),
                        default=float("inf"),
                    ),
                    _expected_score(row["expected_metrics"]),
                    tuple(-value for value in _score(row["metrics"])),
                ),
            )
    else:
        gated = [row for row in candidates if row["passes_metric_gates"]]
        selected = max(gated or candidates, key=lambda row: _score(row["metrics"]))
    sample_gates = {
        "independent_probability_calibration": bool(probability_calibration_paths),
        "minimum_validation_rows": len(validation) >= args.minimum_validation_rows,
        "minimum_validation_markets": len({row["market_id"] for row in validation})
        >= args.minimum_validation_markets,
        "minimum_validation_categories": len({row["category"] for row in validation})
        >= args.minimum_validation_categories,
    }
    validation_strata = _strata_metrics(
        validation,
        selected["combined_probability"],
        float(selected["threshold"]),
    )
    validation_expected_strata = _expected_strata_metrics(
        validation,
        selected["calibrated_probability"],
        validation_conditional_fraction,
    )
    if args.selection_objective == "expected":
        eligible_expected_strata = [
            (family, metrics)
            for family, metrics in validation_expected_strata.items()
            if int(metrics["samples"]) >= args.minimum_stratum_rows
            and int(metrics["unique_markets"]) >= args.minimum_stratum_markets
            and int(metrics["reference_positive_orders"]) > 0
        ]
        if quality_gate_mode == "adaptive":
            failed_families = {
                family
                for family, _metrics in eligible_expected_strata
                if selected["adaptive_strata_quality"][family]["status"] == "FAIL"
            }
        else:
            failed_families = {
                family
                for family, metrics in eligible_expected_strata
                if not _passes_expected_gates(metrics, args)
            }
    else:
        eligible_strata = [
            (family, metrics)
            for family, metrics in validation_strata.items()
            if int(metrics["samples"]) >= args.minimum_stratum_rows
            and int(metrics["unique_markets"]) >= args.minimum_stratum_markets
            and int(metrics["positive_samples"]) > 0
            and int(metrics["positive_samples"]) < int(metrics["samples"])
        ]
        failed_families = {
            family
            for family, metrics in eligible_strata
            if not (
                float(metrics.get("precision") or 0) >= args.minimum_stratum_precision
                and float(metrics.get("recall") or 0) >= args.minimum_stratum_recall
                and float(metrics.get("false_positive_rate") or 1)
                <= args.maximum_stratum_false_positive_rate
            )
        }
    excluded = np.asarray(
        [
            (
                row["category_family"] in failed_families
                or _activity_regime(row) in unstable_activity_regimes
            )
            and not row["source_positive"]
            for row in validation
        ],
        dtype=bool,
    )
    effective_probability = np.where(excluded, 0.0, selected["combined_probability"])
    effective_model_probability = np.where(
        excluded, 0.0, selected["calibrated_probability"]
    )
    effective_conditional_fraction = np.where(
        excluded, 0.0, validation_conditional_fraction
    )
    effective_predicted = validation_source | (
        ~excluded & (selected["calibrated_probability"] >= float(selected["threshold"]))
    )
    supported_indexes = np.flatnonzero(~excluded)
    supported_rows = [validation[int(index)] for index in supported_indexes]
    if not supported_rows:
        raise RuntimeError("all validation rows were excluded by the model-domain gate")
    metrics = _metric(
        y_validation[supported_indexes],
        effective_predicted[supported_indexes],
        effective_probability[supported_indexes],
    )
    all_sample_metrics = _metric(
        y_validation, effective_predicted, effective_probability
    )
    expected_metrics = _expected_metrics(
        supported_rows,
        effective_model_probability[supported_indexes],
        effective_conditional_fraction[supported_indexes],
    )
    all_sample_expected_metrics = _expected_metrics(
        validation,
        effective_model_probability,
        effective_conditional_fraction,
    )
    supported_strata = _strata_metrics(
        validation, effective_probability, float(selected["threshold"])
    )
    supported_expected_strata = _expected_strata_metrics(
        validation,
        effective_model_probability,
        effective_conditional_fraction,
    )
    adaptive_supported_quality = _adaptive_expected_quality(
        supported_rows,
        effective_model_probability[supported_indexes],
        effective_conditional_fraction[supported_indexes],
        args,
    )
    adaptive_supported_strata_quality = _adaptive_expected_strata_quality(
        supported_rows,
        effective_model_probability[supported_indexes],
        effective_conditional_fraction[supported_indexes],
        args,
    )
    if args.selection_objective == "expected":
        supported_eligible_expected_strata = [
            (family, metrics)
            for family, metrics in supported_expected_strata.items()
            if family not in failed_families
            and int(metrics["samples"]) >= args.minimum_stratum_rows
            and int(metrics["unique_markets"]) >= args.minimum_stratum_markets
            and int(metrics["reference_positive_orders"]) > 0
        ]
        if quality_gate_mode == "adaptive":
            strata_gate_passed = bool(supported_eligible_expected_strata) and all(
                adaptive_supported_strata_quality[family]["status"] != "FAIL"
                for family, _metrics in supported_eligible_expected_strata
            )
        else:
            strata_gate_passed = bool(supported_eligible_expected_strata) and all(
                _passes_expected_gates(metrics, args)
                for _family, metrics in supported_eligible_expected_strata
            )
    else:
        supported_eligible_strata = [
            metrics
            for family, metrics in supported_strata.items()
            if family not in failed_families
            and int(metrics["samples"]) >= args.minimum_stratum_rows
            and int(metrics["unique_markets"]) >= args.minimum_stratum_markets
            and int(metrics["positive_samples"]) > 0
            and int(metrics["positive_samples"]) < int(metrics["samples"])
        ]
        strata_gate_passed = bool(supported_eligible_strata) and all(
            float(metrics.get("precision") or 0) >= args.minimum_stratum_precision
            and float(metrics.get("recall") or 0) >= args.minimum_stratum_recall
            and float(metrics.get("false_positive_rate") or 1)
            <= args.maximum_stratum_false_positive_rate
            for metrics in supported_eligible_strata
        )
    excluded_share = float(np.mean(excluded))
    positive = y_validation > 0.5
    excluded_positive_share = (
        float(np.sum(excluded & positive) / np.sum(positive))
        if np.any(positive)
        else 0.0
    )
    domain_gate_passed = (
        excluded_share <= args.maximum_abstain_row_share
        and excluded_positive_share <= args.maximum_abstain_positive_share
    )
    selected_model_gate = (
        adaptive_supported_quality["status"] == "PASS"
        if args.selection_objective == "expected" and quality_gate_mode == "adaptive"
        else _passes_expected_gates(expected_metrics, args)
        if args.selection_objective == "expected"
        else _passes_metric_gates(metrics, args)
    )
    promotion_allowed = (
        selected_model_gate
        and all(sample_gates.values())
        and strata_gate_passed
        and domain_gate_passed
    )
    runtime_calibration_x = selected["calibration_x"]
    runtime_calibration_y = selected["calibration_y"]
    runtime_family_calibrations = selected["family_calibrations"]
    refit_calibration = bool(args.refit_calibration_with_validation)
    if refit_calibration:
        residual_indexes = np.flatnonzero(~validation_source)
        residual_rows = [
            *probability_calibration_rows,
            *(validation[int(index)] for index in residual_indexes),
        ]
        residual_probability = np.concatenate(
            (
                _probabilities(x_probability_calibration, selected["beta"]),
                selected["model_probability"][residual_indexes],
            )
        )
        residual_labels = np.concatenate(
            (y_probability_calibration, y_validation[residual_indexes])
        )
        (
            _,
            _,
            runtime_calibration_x,
            runtime_calibration_y,
        ) = _fit_calibration_curve(
            residual_probability,
            residual_labels,
            args,
        )
        runtime_family_calibrations = _family_calibration_curves(
            residual_probability,
            residual_labels,
            residual_rows,
            args,
        )
        # Both recent calibration stages now participate in the runtime curve;
        # only a later external validation may promote this artifact.
        promotion_allowed = False
    capacity_beta, conservative_fraction, capacity_fit = _fit_capacity(
        x_fit,
        y_fit,
        np.asarray([row["conditional_fraction"] for row in fit_rows], dtype=np.float64),
        ridge=float(selected["ridge"]),
    )
    activation = (
        "PML2_HOLDOUT_PROMOTED_LOCAL_RESEARCH"
        if promotion_allowed
        else "PML2_HOLDOUT_RESEARCH_ONLY"
    )
    profile_suffix = (
        "fok_full_fill"
        if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
        else "fak_any_fill"
    )
    profile_name = f"fill_only_v3_contract_{profile_suffix}"
    contract_rows_by_identity = {
        (row["market_id"], row["asset_id"], row["decision_ts"]): row
        for row in (*calibration, *probability_calibration)
    }
    contract_rows = list(contract_rows_by_identity.values())
    ratio_index = FEATURE_NAMES.index("log_order_to_tape_ratio")
    order_sizes = [float(row["order_size"]) for row in contract_rows]
    log_ratios = [float(row["features"][ratio_index]) for row in contract_rows]
    training_times = sorted(str(row["decision_ts"]) for row in contract_rows)
    profile = {
        "schema_version": "orderfilled_probability_profile_v3",
        "profile_name": profile_name,
        "model_version": args.model_version,
        "activation": activation,
        "training_rows": len(fit_rows),
        "promotion_allowed": promotion_allowed,
        "domain_gate": {
            "abstain_category_families": sorted(failed_families),
            "abstain_activity_regimes": sorted(unstable_activity_regimes),
            "activity_stability": activity_stability,
            "validation_abstain_row_share": excluded_share,
            "validation_abstain_positive_share": excluded_positive_share,
            "maximum_abstain_row_share": args.maximum_abstain_row_share,
            "maximum_abstain_positive_share": args.maximum_abstain_positive_share,
            "passed": domain_gate_passed,
        },
        "data_contract": {
            "runtime_input": "trade_prints_one_sided_pre_arrival_only",
            "feature_contract": ARRIVAL_FEATURE_CONTRACT,
            "runtime_lob_usage": "NONE",
            "offline_label": (
                "PML2_IMMEDIATE_FOK_FULL_FILL"
                if probability_target == PROBABILITY_TARGET_FOK_FULL_FILL
                else "PML2_IMMEDIATE_FAK_ANY_FILL"
            ),
            "offline_lob_usage": "LABEL_ONLY",
            "nautilus_role": "STATIC_L2_IMPLEMENTATION_CONTROL_ONLY",
            "label_horizon_seconds": 1,
            "source_confirmed_route_precedes_modeled_route": True,
            "modeled_results_are_observed_execution": False,
            "probability_calibration": (
                "RECENT_CALIBRATION_PLUS_SELECTION_REFIT_REQUIRES_EXTERNAL_VALIDATION"
                if refit_calibration
                else "INDEPENDENT_COHORT"
                if probability_calibration_paths
                else "IN_SAMPLE_LEGACY_RESEARCH_ONLY"
            ),
        },
        "model_contract": {
            "probability_target": probability_target,
            "supported_sides": sorted({row["side"] for row in contract_rows}),
            "supported_tifs": sorted({row["tif"] for row in contract_rows}),
            "supported_amount_units": sorted(
                {row["amount_unit"] for row in contract_rows}
            ),
            "minimum_order_size": str(min(order_sizes)),
            "maximum_order_size": str(max(order_sizes)),
            "minimum_log_order_to_tape_ratio": str(min(log_ratios)),
            "maximum_log_order_to_tape_ratio": str(max(log_ratios)),
            "training_period_start": training_times[0],
            "training_period_end": training_times[-1],
            "fit_scope": fit_scope,
            "reference_model": reference_model,
            "source_model": source_model,
            "runtime_amount_unit": "SHARES_AFTER_ORDER_NORMALIZATION",
        },
        "params": {
            "name": profile_name,
            "min_probability": str(selected["threshold"]),
            "medium_probability": str(selected["threshold"]),
            "high_probability": str(min(0.99, selected["threshold"] + 0.15)),
            "capacity_floor": "0",
            "capacity_mode": "conditional_source_capacity",
            "capacity_variant": "expected",
            "hard_reject_below_probability": False,
            "lookback_seconds": args.lookback_seconds,
            "lookback_blocks": args.lookback_blocks,
            "tick_size": str(args.tick_size),
            "threshold_policy": "VALIDATION_GATED_PML2_EXECUTABILITY",
        },
        "model": {
            "type": "logistic_regression",
            "version": "numpy_newton_l2_nonlinear_v2",
            "feature_names": list(FEATURE_NAMES),
            "intercept": str(float(selected["beta"][0])),
            "coefficients": {
                name: str(float(value))
                for name, value in zip(FEATURE_NAMES, selected["beta"][1:])
            },
            "feature_means": {
                name: str(float(value)) for name, value in zip(FEATURE_NAMES, means)
            },
            "feature_scales": {
                name: str(float(value)) for name, value in zip(FEATURE_NAMES, scales)
            },
        },
        "calibration": {
            "type": f"{args.calibration_method}_piecewise",
            "x": [str(float(value)) for value in runtime_calibration_x],
            "y": [str(float(value)) for value in runtime_calibration_y],
            "by_category_family": {
                family: {
                    "x": [str(float(value)) for value in curve["x"]],
                    "y": [str(float(value)) for value in curve["y"]],
                    "calibration_rows": int(curve["rows"]),
                    "calibration_markets": int(curve["markets"]),
                }
                for family, curve in sorted(runtime_family_calibrations.items())
            },
            "logit_mean": selected["platt_mean"],
            "logit_scale": selected["platt_scale"],
        },
        "conditional_capacity": {
            "mode": "conditional_source_capacity",
            "default_variant": "expected",
            "fit": capacity_fit,
            "feature_names": list(FEATURE_NAMES),
            "models": {
                "expected": {
                    "name": "expected",
                    "target": "pml2_fill_fraction_given_executable",
                    "quantile": None,
                    "floor": "0",
                    "intercept": str(float(capacity_beta[0])),
                    "coefficients": {
                        name: str(float(value))
                        for name, value in zip(FEATURE_NAMES, capacity_beta[1:])
                    },
                    "training_rows": int(np.sum(y_fit > 0.5)),
                },
                "conservative": {
                    "name": "conservative",
                    "target": "pml2_fill_fraction_q25_given_executable",
                    "quantile": "0.25",
                    "floor": "0",
                    "intercept": str(conservative_fraction),
                    "coefficients": {name: "0" for name in FEATURE_NAMES},
                    "training_rows": int(np.sum(y_fit > 0.5)),
                },
            },
        },
        "selection": {
            "ridge": selected["ridge"],
            "positive_weight": selected["positive_weight"],
            "threshold": selected["threshold"],
            "calibration_refit_with_validation": refit_calibration,
            "metric_gates_passed": _passes_metric_gates(metrics, args),
            "expected_metric_gates_passed": _passes_expected_gates(
                selected["expected_metrics"], args
            ),
            "quality_gate_mode": quality_gate_mode,
            "adaptive_probability_quality": adaptive_supported_quality,
            "adaptive_probability_quality_before_domain_gate": selected[
                "adaptive_probability_quality"
            ],
            "adaptive_quality_by_category_family_before_domain_gate": selected[
                "adaptive_strata_quality"
            ],
            "adaptive_quality_by_category_family": (adaptive_supported_strata_quality),
            "selection_objective": args.selection_objective,
            "expected_strata_selection_gate_passed": selected[
                "expected_strata_gate_passed"
            ],
            "expected_strata_selection_penalties": selected[
                "expected_strata_penalties"
            ],
            "sample_gates": sample_gates,
            "strata_gate_passed": strata_gate_passed,
            "validation_metrics": metrics,
            "validation_expected_metrics": expected_metrics,
            "validation_all_sample_expected_metrics": all_sample_expected_metrics,
            "validation_expected_metrics_before_domain_gate": selected[
                "expected_metrics"
            ],
            "validation_all_sample_metrics": all_sample_metrics,
            "validation_metrics_before_domain_gate": selected["metrics"],
            "validation_by_category_family_before_domain_gate": validation_strata,
            "validation_by_category_family": supported_strata,
            "validation_expected_by_category_family_before_domain_gate": (
                validation_expected_strata
            ),
            "validation_expected_by_category_family": supported_expected_strata,
            "gates": (
                {
                    "mode": "adaptive",
                    "reference": "MATCHING_CONTRACT_SAMPLE_CLIMATOLOGY",
                    "decision_rule": "PAIRED_MARKET_DAY_CLUSTER_BOOTSTRAP",
                    "confidence_level": float(getattr(args, "confidence_level", 0.95)),
                    "bootstrap_replicates": int(
                        getattr(args, "bootstrap_replicates", 1_000)
                    ),
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
                    "minimum_stratum_rows": args.minimum_stratum_rows,
                    "minimum_stratum_markets": args.minimum_stratum_markets,
                    "legacy_fixed_thresholds_applied": False,
                }
                if quality_gate_mode == "adaptive"
                else {
                    "mode": "fixed",
                    "minimum_precision": args.minimum_precision,
                    "minimum_recall": args.minimum_recall,
                    "maximum_false_positive_rate": args.maximum_false_positive_rate,
                    "minimum_positive_count_ratio": (args.minimum_positive_count_ratio),
                    "maximum_positive_count_ratio": (args.maximum_positive_count_ratio),
                    "minimum_stratum_rows": args.minimum_stratum_rows,
                    "minimum_stratum_markets": args.minimum_stratum_markets,
                    "minimum_stratum_precision": args.minimum_stratum_precision,
                    "minimum_stratum_recall": args.minimum_stratum_recall,
                    "minimum_family_calibration_rows": (
                        args.minimum_family_calibration_rows
                    ),
                    "minimum_family_calibration_markets": (
                        args.minimum_family_calibration_markets
                    ),
                    "maximum_stratum_false_positive_rate": (
                        args.maximum_stratum_false_positive_rate
                    ),
                    "maximum_expected_brier_regret": (
                        args.maximum_expected_brier_regret
                    ),
                    "legacy_fixed_thresholds_applied": True,
                }
            ),
        },
        "cohorts": {
            "calibration_fingerprint": _fingerprint(calibration),
            "probability_calibration_fingerprint": _fingerprint(
                probability_calibration
            ),
            "validation_fingerprint": _fingerprint(validation),
            "calibration_rows": len(calibration),
            "probability_calibration_rows": len(probability_calibration),
            "probability_calibration_is_independent": bool(
                probability_calibration_paths
            ),
            "residual_fit_rows": len(fit_rows),
            "fit_scope": fit_scope,
            "probability_target": probability_target,
            "reference_model": reference_model,
            "validation_rows": len(validation),
            "calibration_markets": len({row["market_id"] for row in calibration}),
            "validation_markets": len({row["market_id"] for row in validation}),
            "calibration_categories": dict(
                Counter(row["category"] for row in calibration)
            ),
            "validation_categories": dict(
                Counter(row["category"] for row in validation)
            ),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    return profile


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calibration-orders", type=Path, nargs="+", required=True)
    parser.add_argument("--probability-calibration-orders", type=Path, nargs="+")
    parser.add_argument("--validation-orders", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--lookback-seconds", type=int, default=900)
    parser.add_argument("--lookback-blocks", type=int, default=600)
    parser.add_argument("--tick-size", default="0.001")
    parser.add_argument("--model-version", default="fill_only_v3_l2_reference_fak_v2")
    parser.add_argument(
        "--probability-target",
        choices=(PROBABILITY_TARGET_FAK_ANY_FILL, PROBABILITY_TARGET_FOK_FULL_FILL),
        default=PROBABILITY_TARGET_FAK_ANY_FILL,
    )
    parser.add_argument(
        "--reference-model",
        help="saved PML2 model key; defaults to pml2_fak or pml2_fok by target",
    )
    parser.add_argument(
        "--source-model",
        help="optional saved source-confirmed model key",
    )
    parser.add_argument(
        "--fit-scope",
        choices=("residual", "all"),
        default="residual",
        help="fit residual modeled capacity or direct arrival-time probability",
    )
    parser.add_argument(
        "--calibration-method",
        choices=("isotonic", "platt"),
        default="isotonic",
    )
    parser.add_argument("--minimum-isotonic-bin-size", type=int, default=100)
    parser.add_argument("--minimum-family-calibration-rows", type=int, default=200)
    parser.add_argument("--minimum-family-calibration-markets", type=int, default=3)
    parser.add_argument("--minimum-temporal-activity-rows", type=int, default=40)
    parser.add_argument("--minimum-temporal-activity-markets", type=int, default=3)
    parser.add_argument("--maximum-temporal-activity-drift", type=float, default=0.20)
    parser.add_argument("--refit-calibration-with-validation", action="store_true")
    parser.add_argument(
        "--ridge", type=float, nargs="+", default=[0.1, 1.0, 10.0, 100.0, 1000.0]
    )
    parser.add_argument(
        "--positive-weight", type=float, nargs="+", default=[1.0, 1.5, 2.0]
    )
    parser.add_argument("--minimum-precision", type=float, default=0.70)
    parser.add_argument("--minimum-recall", type=float, default=0.80)
    parser.add_argument("--maximum-false-positive-rate", type=float, default=0.30)
    parser.add_argument("--minimum-positive-count-ratio", type=float, default=0.75)
    parser.add_argument("--maximum-positive-count-ratio", type=float, default=1.25)
    parser.add_argument(
        "--selection-objective",
        choices=("expected", "classification"),
        default="expected",
    )
    parser.add_argument(
        "--quality-gate-mode",
        choices=("adaptive", "fixed"),
        default="adaptive",
        help="adaptive uses market-day clustered intervals; fixed reproduces legacy gates",
    )
    parser.add_argument("--confidence-level", type=float, default=0.95)
    parser.add_argument("--bootstrap-replicates", type=int, default=1_000)
    parser.add_argument("--bootstrap-seed", type=int, default=73)
    parser.add_argument("--minimum-adaptive-samples", type=int, default=200)
    parser.add_argument("--minimum-adaptive-positive-samples", type=int, default=20)
    parser.add_argument("--minimum-adaptive-negative-samples", type=int, default=20)
    parser.add_argument("--minimum-adaptive-clusters", type=int, default=20)
    parser.add_argument("--maximum-expected-brier-score", type=float, default=0.20)
    parser.add_argument("--maximum-expected-brier-regret", type=float, default=0.01)
    parser.add_argument("--minimum-expected-ratio", type=float, default=0.85)
    parser.add_argument("--maximum-expected-ratio", type=float, default=1.15)
    parser.add_argument("--minimum-validation-rows", type=int, default=100)
    parser.add_argument("--minimum-validation-markets", type=int, default=8)
    parser.add_argument("--minimum-validation-categories", type=int, default=3)
    parser.add_argument("--minimum-stratum-rows", type=int, default=10)
    parser.add_argument("--minimum-stratum-markets", type=int, default=20)
    parser.add_argument("--minimum-stratum-precision", type=float, default=0.60)
    parser.add_argument("--minimum-stratum-recall", type=float, default=0.60)
    parser.add_argument(
        "--maximum-stratum-false-positive-rate", type=float, default=0.50
    )
    parser.add_argument("--maximum-abstain-row-share", type=float, default=0.35)
    parser.add_argument("--maximum-abstain-positive-share", type=float, default=0.15)
    return parser


def main() -> int:
    args = _parser().parse_args()
    profile = calibrate(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(profile, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "promotion_allowed": profile["promotion_allowed"],
                "selection": profile["selection"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
