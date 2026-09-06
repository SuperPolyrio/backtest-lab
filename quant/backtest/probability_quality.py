"""Reference-relative quality checks for binary execution probabilities."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Hashable, Sequence
from typing import Any

import numpy as np
from numpy.typing import ArrayLike


def _interval(values: Sequence[float], confidence_level: float) -> list[float] | None:
    finite = np.asarray(
        [value for value in values if math.isfinite(value)], dtype=float
    )
    if not len(finite):
        return None
    tail = (1.0 - confidence_level) / 2.0
    return [
        float(np.quantile(finite, tail)),
        float(np.quantile(finite, 1.0 - tail)),
    ]


def _contains(interval: Sequence[float] | None, target: float) -> bool:
    if interval is None or len(interval) < 2:
        return False
    tolerance = 1e-12
    return (
        float(interval[0]) - tolerance <= target
        and target <= float(interval[1]) + tolerance
    )


def _endpoint_non_positive(interval: Sequence[float] | None, endpoint: int) -> bool:
    return interval is not None and len(interval) >= 2 and interval[endpoint] <= 0.0


def _calibration_intercept_slope(
    probabilities: np.ndarray, labels: np.ndarray
) -> tuple[float | None, float | None]:
    if len(np.unique(labels)) < 2:
        return None, None
    clipped = np.clip(probabilities, 1e-6, 1.0 - 1e-6)
    logits = np.log(clipped / (1.0 - clipped))
    design = np.column_stack([np.ones(len(logits)), logits])
    beta = np.asarray([0.0, 1.0], dtype=float)
    converged = False
    for _ in range(50):
        fitted = 1.0 / (1.0 + np.exp(-np.clip(design @ beta, -40.0, 40.0)))
        variance = np.maximum(fitted * (1.0 - fitted), 1e-8)
        gradient = design.T @ (labels - fitted)
        hessian = design.T @ (variance[:, None] * design)
        try:
            step = np.linalg.solve(hessian, gradient)
        except np.linalg.LinAlgError:
            return None, None
        beta += step
        if float(np.max(np.abs(step))) < 1e-9:
            converged = True
            break
    # Perfect or near-perfect separation has no finite unpenalized logistic
    # calibration slope. Reporting the diverging iterate would look precise but
    # has no statistical meaning.
    if not converged or np.any(~np.isfinite(beta)) or np.max(np.abs(beta)) > 100:
        return None, None
    return float(beta[0]), float(beta[1])


def _wilson(successes: float, samples: int, confidence_level: float) -> list[float]:
    if samples <= 0:
        return [0.0, 1.0]
    # 1.95996 is sufficient for the supported default 95% interval. For other
    # confidence levels use a deterministic normal approximation.
    z = 1.959963984540054
    if abs(confidence_level - 0.95) > 1e-12:
        from statistics import NormalDist

        z = NormalDist().inv_cdf(0.5 + confidence_level / 2.0)
    rate = successes / samples
    denominator = 1.0 + z * z / samples
    center = (rate + z * z / (2.0 * samples)) / denominator
    radius = (
        z
        * math.sqrt(rate * (1.0 - rate) / samples + z * z / (4.0 * samples**2))
        / denominator
    )
    return [max(0.0, center - radius), min(1.0, center + radius)]


def _reliability_bins(
    probabilities: np.ndarray,
    labels: np.ndarray,
    *,
    confidence_level: float,
    minimum_bin_size: int,
    maximum_bins: int,
) -> list[dict[str, Any]]:
    order = np.argsort(probabilities, kind="stable")
    bins = max(1, min(maximum_bins, len(order) // max(1, minimum_bin_size)))
    result: list[dict[str, Any]] = []
    for indexes in np.array_split(order, bins):
        if not len(indexes):
            continue
        observed = float(np.mean(labels[indexes]))
        predicted = float(np.mean(probabilities[indexes]))
        result.append(
            {
                "samples": len(indexes),
                "minimum_probability": float(np.min(probabilities[indexes])),
                "maximum_probability": float(np.max(probabilities[indexes])),
                "mean_probability": predicted,
                "observed_rate": observed,
                "observed_rate_interval": _wilson(
                    float(np.sum(labels[indexes])),
                    len(indexes),
                    confidence_level,
                ),
                "absolute_calibration_error": abs(predicted - observed),
            }
        )
    return result


def adaptive_probability_quality(
    probabilities: ArrayLike,
    labels: ArrayLike,
    *,
    expected_quantities: ArrayLike | None = None,
    reference_quantities: ArrayLike | None = None,
    quantity_scales: ArrayLike | None = None,
    cluster_ids: Sequence[Hashable] | None = None,
    bootstrap_replicates: int = 1_000,
    confidence_level: float = 0.95,
    random_seed: int = 73,
    minimum_samples: int = 200,
    minimum_positive_samples: int = 20,
    minimum_negative_samples: int = 20,
    minimum_clusters: int = 20,
    minimum_reliability_bin_size: int = 200,
    maximum_reliability_bins: int = 10,
) -> dict[str, Any]:
    """Evaluate calibration relative to the cohort's matching-contract base rate.

    The bootstrap unit is a caller-provided market-day (or broader independent)
    cluster. This avoids treating many nearby orders in one market as independent.
    """

    probability = np.asarray(probabilities, dtype=float)
    observed = np.asarray(labels, dtype=float)
    if probability.ndim != 1 or observed.ndim != 1 or len(probability) != len(observed):
        raise ValueError("probabilities and labels must be equal-length vectors")
    if not len(probability):
        raise ValueError("at least one probability observation is required")
    if np.any(~np.isfinite(probability)) or np.any(
        (probability < 0) | (probability > 1)
    ):
        raise ValueError("probabilities must be finite values in [0, 1]")
    if np.any(~np.isfinite(observed)) or np.any((observed < 0) | (observed > 1)):
        raise ValueError("labels must be finite values in [0, 1]")

    expected_quantity = np.asarray(
        expected_quantities if expected_quantities is not None else probability,
        dtype=float,
    )
    reference_quantity = np.asarray(
        reference_quantities if reference_quantities is not None else observed,
        dtype=float,
    )
    if len(expected_quantity) != len(probability) or len(reference_quantity) != len(
        probability
    ):
        raise ValueError("quantity vectors must match probability vector length")
    quantity_scale = np.asarray(
        quantity_scales if quantity_scales is not None else np.ones(len(probability)),
        dtype=float,
    )
    if len(quantity_scale) != len(probability):
        raise ValueError("quantity_scales must match probability vector length")
    if np.any(~np.isfinite(quantity_scale)) or np.any(quantity_scale <= 0):
        raise ValueError("quantity_scales must be finite positive values")

    clusters = (
        list(cluster_ids) if cluster_ids is not None else list(range(len(observed)))
    )
    if len(clusters) != len(observed):
        raise ValueError("cluster_ids must match probability vector length")
    grouped: dict[Hashable, list[int]] = defaultdict(list)
    for index, cluster in enumerate(clusters):
        grouped[cluster].append(index)

    model_loss = (probability - observed) ** 2
    observed_rate = float(np.mean(observed))
    reference_brier = observed_rate * (1.0 - observed_rate)
    brier = float(np.mean(model_loss))
    brier_regret = brier - reference_brier
    brier_skill = 1.0 - brier / reference_brier if reference_brier > 0 else None
    clipped_probability = np.clip(probability, 1e-12, 1.0 - 1e-12)
    model_log_loss = -(
        observed * np.log(clipped_probability)
        + (1.0 - observed) * np.log(1.0 - clipped_probability)
    )
    clipped_rate = min(1.0 - 1e-12, max(1e-12, observed_rate))
    reference_log_loss = -(
        observed_rate * math.log(clipped_rate)
        + (1.0 - observed_rate) * math.log(1.0 - clipped_rate)
    )
    log_loss = float(np.mean(model_log_loss))
    log_loss_regret = log_loss - reference_log_loss
    log_loss_skill = (
        1.0 - log_loss / reference_log_loss if reference_log_loss > 0 else None
    )
    expected_orders = float(np.sum(probability))
    reference_orders = float(np.sum(observed))
    expected_total_quantity = float(np.sum(expected_quantity))
    reference_total_quantity = float(np.sum(reference_quantity))
    order_ratio = expected_orders / reference_orders if reference_orders > 0 else None
    quantity_ratio = (
        expected_total_quantity / reference_total_quantity
        if reference_total_quantity > 0
        else None
    )

    expected_fraction = expected_quantity / quantity_scale
    reference_fraction = reference_quantity / quantity_scale
    quantity_loss = (expected_fraction - reference_fraction) ** 2
    reference_fraction_mean = float(np.mean(reference_fraction))
    reference_quantity_loss = (reference_fraction - reference_fraction_mean) ** 2
    quantity_mse = float(np.mean(quantity_loss))
    reference_quantity_mse = float(np.mean(reference_quantity_loss))
    quantity_regret = quantity_mse - reference_quantity_mse
    quantity_skill = (
        1.0 - quantity_mse / reference_quantity_mse
        if reference_quantity_mse > 0
        else None
    )

    cluster_rows: list[
        tuple[int, float, float, float, float, float, float, float, float, float]
    ] = []
    for indexes in grouped.values():
        selected = np.asarray(indexes, dtype=int)
        cluster_rows.append(
            (
                len(indexes),
                float(np.sum(model_loss[selected])),
                float(np.sum(model_log_loss[selected])),
                float(np.sum(probability[selected])),
                float(np.sum(observed[selected])),
                float(np.sum(expected_quantity[selected])),
                float(np.sum(reference_quantity[selected])),
                float(np.sum(quantity_loss[selected])),
                float(np.sum(reference_fraction[selected])),
                float(np.sum(reference_fraction[selected] ** 2)),
            )
        )
    cluster_matrix = np.asarray(cluster_rows, dtype=float)
    rng = np.random.default_rng(random_seed)
    bootstrap: dict[str, list[float]] = defaultdict(list)
    for _ in range(max(0, int(bootstrap_replicates))):
        selected = rng.integers(0, len(cluster_matrix), size=len(cluster_matrix))
        totals = np.sum(cluster_matrix[selected], axis=0)
        (
            samples,
            loss_sum,
            log_loss_sum,
            predicted_sum,
            observed_sum,
            quantity_sum,
            reference_sum,
            quantity_loss_sum,
            reference_fraction_sum,
            reference_quantity_squared_sum,
        ) = totals
        if samples <= 0:
            continue
        replicate_brier = loss_sum / samples
        replicate_rate = observed_sum / samples
        replicate_reference_brier = replicate_rate * (1.0 - replicate_rate)
        replicate_regret = replicate_brier - replicate_reference_brier
        bootstrap["brier_regret"].append(replicate_regret)
        if replicate_reference_brier > 0:
            bootstrap["brier_skill_score"].append(
                1.0 - replicate_brier / replicate_reference_brier
            )
        replicate_rate_clipped = min(1.0 - 1e-12, max(1e-12, replicate_rate))
        replicate_reference_log_loss = -(
            replicate_rate * math.log(replicate_rate_clipped)
            + (1.0 - replicate_rate) * math.log(1.0 - replicate_rate_clipped)
        )
        replicate_log_loss = log_loss_sum / samples
        bootstrap["log_loss_regret"].append(
            replicate_log_loss - replicate_reference_log_loss
        )
        if replicate_reference_log_loss > 0:
            bootstrap["log_loss_skill_score"].append(
                1.0 - replicate_log_loss / replicate_reference_log_loss
            )
        bootstrap["calibration_in_the_large"].append(
            (predicted_sum - observed_sum) / samples
        )
        if observed_sum > 0:
            bootstrap["expected_order_ratio"].append(predicted_sum / observed_sum)
        if reference_sum > 0:
            bootstrap["expected_quantity_ratio"].append(quantity_sum / reference_sum)
        replicate_quantity_mse = quantity_loss_sum / samples
        replicate_reference_quantity_mse = max(
            0.0,
            reference_quantity_squared_sum / samples
            - (reference_fraction_sum / samples) ** 2,
        )
        replicate_quantity_regret = (
            replicate_quantity_mse - replicate_reference_quantity_mse
        )
        bootstrap["quantity_regret"].append(replicate_quantity_regret)
        if replicate_reference_quantity_mse > 0:
            bootstrap["quantity_skill_score"].append(
                1.0 - replicate_quantity_mse / replicate_reference_quantity_mse
            )

    intervals = {
        name: _interval(values, confidence_level) for name, values in bootstrap.items()
    }
    positive_samples = int(np.sum(observed > 0.5))
    negative_samples = int(len(observed) - positive_samples)
    sample_checks = {
        "minimum_samples": len(observed) >= minimum_samples,
        "minimum_positive_samples": positive_samples >= minimum_positive_samples,
        "minimum_negative_samples": negative_samples >= minimum_negative_samples,
        "minimum_independent_clusters": len(grouped) >= minimum_clusters,
        "bootstrap_available": bootstrap_replicates > 0
        and bool(intervals.get("brier_regret")),
    }
    quality_checks = {
        "reference_relative_brier_skill": _endpoint_non_positive(
            intervals.get("brier_regret"), 1
        ),
        "reference_relative_quantity_skill": _endpoint_non_positive(
            intervals.get("quantity_regret"), 1
        ),
        "calibration_in_the_large": _contains(
            intervals.get("calibration_in_the_large"), 0.0
        ),
        "expected_order_count": _contains(intervals.get("expected_order_ratio"), 1.0),
        "expected_quantity": _contains(intervals.get("expected_quantity_ratio"), 1.0),
    }
    guardrail_checks = {
        "no_detected_brier_harm": _endpoint_non_positive(
            intervals.get("brier_regret"), 0
        ),
        "no_detected_log_loss_harm": _endpoint_non_positive(
            intervals.get("log_loss_regret"), 0
        ),
        "no_detected_quantity_harm": _endpoint_non_positive(
            intervals.get("quantity_regret"), 0
        ),
        "calibration_in_the_large": quality_checks["calibration_in_the_large"],
        "expected_order_count": quality_checks["expected_order_count"],
        "expected_quantity": quality_checks["expected_quantity"],
    }
    intercept, slope = _calibration_intercept_slope(probability, observed)
    bins = _reliability_bins(
        probability,
        observed,
        confidence_level=confidence_level,
        minimum_bin_size=minimum_reliability_bin_size,
        maximum_bins=maximum_reliability_bins,
    )
    sample_ready = all(sample_checks.values())
    status = (
        "INSUFFICIENT_SAMPLE"
        if not sample_ready
        else "PASS"
        if all(quality_checks.values())
        else "INCONCLUSIVE"
        if all(guardrail_checks.values())
        else "FAIL"
    )
    return {
        "schema_version": "adaptive_probability_quality_v2",
        "status": status,
        "samples": len(observed),
        "positive_samples": positive_samples,
        "negative_samples": negative_samples,
        "independent_clusters": len(grouped),
        "confidence_level": confidence_level,
        "bootstrap_replicates": int(bootstrap_replicates),
        "reference": "MATCHING_CONTRACT_SAMPLE_CLIMATOLOGY",
        "decision_rule": (
            "PAIRED_CLUSTER_BOOTSTRAP_RELATIVE_TO_MATCHING_CONTRACT_CLIMATOLOGY"
        ),
        "brier_score": brier,
        "reference_brier_score": reference_brier,
        "brier_regret": brier_regret,
        "brier_skill_score": brier_skill,
        "log_loss": log_loss,
        "reference_log_loss": reference_log_loss,
        "log_loss_regret": log_loss_regret,
        "log_loss_skill_score": log_loss_skill,
        "mean_probability": float(np.mean(probability)),
        "observed_rate": observed_rate,
        "calibration_in_the_large": float(np.mean(probability - observed)),
        "calibration_intercept": intercept,
        "calibration_slope": slope,
        "expected_positive_orders": expected_orders,
        "reference_positive_orders": reference_orders,
        "expected_positive_order_ratio": order_ratio,
        "expected_quantity": expected_total_quantity,
        "reference_quantity": reference_total_quantity,
        "expected_quantity_ratio": quantity_ratio,
        "quantity_fraction_mse": quantity_mse,
        "reference_quantity_fraction_mse": reference_quantity_mse,
        "quantity_regret": quantity_regret,
        "quantity_skill_score": quantity_skill,
        "confidence_intervals": intervals,
        "reliability_bins": bins,
        "integrated_calibration_index": float(
            sum(row["samples"] * row["absolute_calibration_error"] for row in bins)
            / len(observed)
        ),
        "sample_checks": sample_checks,
        "quality_checks": quality_checks,
        "guardrail_checks": guardrail_checks,
    }


__all__ = ["adaptive_probability_quality"]
