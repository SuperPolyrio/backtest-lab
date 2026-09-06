"""Small calibration toolkit for the conditional favorite-longshot strategy."""

from __future__ import annotations

import hashlib
import math
from statistics import NormalDist
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Protocol

import numpy as np


EPS = 1e-6


class CalibrationRow(Protocol):
    market_id: int
    signal_time: datetime
    probability_yes: float
    yes_won: bool


@dataclass(frozen=True)
class CalibrationEstimate:
    selected_model: str
    probability: float
    lower_bound: float
    upper_bound: float
    sample_count: int
    market_count: int
    validation_count: int
    validation_brier: float
    validation_baseline_brier: float
    model_predictions: dict[str, float]
    model_briers: dict[str, float]
    model_sign_agreement: float
    stable_periods: int
    stable_period_sign_ratio: float
    stable_seasons: int
    stable_season_sign_ratio: float
    stable: bool
    rejection_reasons: tuple[str, ...]


@dataclass
class _Model:
    name: str
    params: tuple[float, ...] = ()
    x: np.ndarray | None = None
    y: np.ndarray | None = None
    bandwidth: float = 0.08

    def predict(self, values: Iterable[float]) -> np.ndarray:
        p = np.clip(np.asarray(list(values), dtype=float), EPS, 1.0 - EPS)
        if self.name == "power":
            odds = np.power(p / (1.0 - p), self.params[0])
            return np.clip(odds / (1.0 + odds), EPS, 1.0 - EPS)
        if self.name == "platt":
            z = self.params[0] + self.params[1] * np.log(p / (1.0 - p))
            return np.clip(1.0 / (1.0 + np.exp(-np.clip(z, -30.0, 30.0))), EPS, 1.0 - EPS)
        if self.name == "isotonic":
            assert self.x is not None and self.y is not None
            return np.clip(np.interp(p, self.x, self.y, left=self.y[0], right=self.y[-1]), EPS, 1.0 - EPS)
        if self.name == "local_linear":
            assert self.x is not None and self.y is not None
            return np.asarray([_local_linear_predict(self.x, self.y, value, self.bandwidth) for value in p])
        raise ValueError(f"unknown calibration model: {self.name}")


def estimate_calibration(
    rows: Iterable[CalibrationRow],
    *,
    probability: float,
    min_samples: int,
    min_validation_samples: int = 20,
    validation_fraction: float = 0.25,
    bootstrap_samples: int = 100,
    confidence_level: float = 0.90,
    price_tolerance: float = 0.05,
    min_stability_periods: int = 3,
    min_period_samples: int = 8,
    min_period_sign_ratio: float = 0.67,
    min_stability_seasons: int = 1,
    min_season_samples: int = 30,
    min_season_sign_ratio: float = 0.67,
    min_model_sign_agreement: float = 0.75,
    require_validation_improvement: bool = True,
    seed_key: str = "",
) -> CalibrationEstimate:
    ordered = sorted(rows, key=lambda row: (row.signal_time, row.market_id))
    sample_count = len(ordered)
    market_count = len({row.market_id for row in ordered})
    if sample_count < min_samples:
        return _rejected_estimate("insufficient_samples", sample_count, market_count)

    validation_count = max(min_validation_samples, int(round(sample_count * validation_fraction)))
    validation_count = min(validation_count, max(1, sample_count // 2))
    fit_rows = ordered[:-validation_count]
    validation_rows = ordered[-validation_count:]
    if len(fit_rows) < max(10, min_samples // 2):
        return _rejected_estimate("insufficient_training_samples", sample_count, market_count)

    fit_x, fit_y = _arrays(fit_rows)
    val_x, val_y = _arrays(validation_rows)
    validation_models = _fit_models(fit_x, fit_y)
    model_briers = {
        name: _brier(model.predict(val_x), val_y)
        for name, model in validation_models.items()
    }
    selected_name = min(model_briers, key=lambda name: (model_briers[name], name))
    baseline_brier = _brier(val_x, val_y)

    all_x, all_y = _arrays(ordered)
    fitted = _fit_models(all_x, all_y)
    model_predictions = {
        name: float(model.predict([probability])[0])
        for name, model in fitted.items()
    }
    calibrated = model_predictions[selected_name]
    direction = _sign(calibrated - probability)
    model_sign_agreement = (
        sum(_sign(value - probability) == direction for value in model_predictions.values())
        / len(model_predictions)
        if direction
        else 0.0
    )

    period_signs = _period_signs(
        ordered,
        probability=probability,
        tolerance=price_tolerance,
        min_period_samples=min_period_samples,
    )
    stable_periods = len(period_signs)
    stable_period_sign_ratio = (
        sum(sign == direction for sign in period_signs) / stable_periods
        if stable_periods and direction
        else 0.0
    )
    season_signs = _season_signs(
        ordered,
        probability=probability,
        tolerance=price_tolerance,
        min_season_samples=min_season_samples,
    )
    stable_seasons = len(season_signs)
    stable_season_sign_ratio = (
        sum(sign == direction for sign in season_signs) / stable_seasons
        if stable_seasons and direction
        else 0.0
    )

    pre_bootstrap_stable = (
        direction != 0
        and model_sign_agreement >= min_model_sign_agreement
        and stable_periods >= min_stability_periods
        and stable_period_sign_ratio >= min_period_sign_ratio
        and stable_seasons >= min_stability_seasons
        and stable_season_sign_ratio >= min_season_sign_ratio
        and (not require_validation_improvement or model_briers[selected_name] < baseline_brier)
    )
    bootstrap_predictions = (
        _cluster_bootstrap_predictions(
            ordered,
            model_name=selected_name,
            probability=probability,
            samples=bootstrap_samples,
            seed_key=seed_key,
        )
        if pre_bootstrap_stable
        else []
    )
    alpha = max(0.0, min(0.49, (1.0 - confidence_level) / 2.0))
    lower = float(np.quantile(bootstrap_predictions, alpha)) if bootstrap_predictions else calibrated
    upper = float(np.quantile(bootstrap_predictions, 1.0 - alpha)) if bootstrap_predictions else calibrated
    local_rows = [row for row in ordered if abs(row.probability_yes - probability) <= price_tolerance]
    if not local_rows:
        local_rows = ordered
    local_wins = sum(row.yes_won for row in local_rows)
    wilson_lower, wilson_upper = _wilson_interval(
        local_wins,
        len(local_rows),
        confidence_level=confidence_level,
    )
    lower = min(lower, wilson_lower)
    upper = max(upper, wilson_upper)

    rejection_reasons: list[str] = []
    if direction == 0:
        rejection_reasons.append("zero_calibration_edge")
    if model_sign_agreement < min_model_sign_agreement:
        rejection_reasons.append("model_direction_disagreement")
    if stable_periods < min_stability_periods:
        rejection_reasons.append("insufficient_stability_periods")
    elif stable_period_sign_ratio < min_period_sign_ratio:
        rejection_reasons.append("unstable_period_direction")
    if stable_seasons < min_stability_seasons:
        rejection_reasons.append("insufficient_stability_seasons")
    elif stable_season_sign_ratio < min_season_sign_ratio:
        rejection_reasons.append("unstable_season_direction")
    if require_validation_improvement and model_briers[selected_name] >= baseline_brier:
        rejection_reasons.append("no_oos_brier_improvement")
    if direction > 0 and lower <= probability:
        rejection_reasons.append("bootstrap_interval_crosses_market_price")
    if direction < 0 and upper >= probability:
        rejection_reasons.append("bootstrap_interval_crosses_market_price")

    return CalibrationEstimate(
        selected_model=selected_name,
        probability=calibrated,
        lower_bound=max(0.0, lower),
        upper_bound=min(1.0, upper),
        sample_count=sample_count,
        market_count=market_count,
        validation_count=validation_count,
        validation_brier=model_briers[selected_name],
        validation_baseline_brier=baseline_brier,
        model_predictions=model_predictions,
        model_briers=model_briers,
        model_sign_agreement=model_sign_agreement,
        stable_periods=stable_periods,
        stable_period_sign_ratio=stable_period_sign_ratio,
        stable_seasons=stable_seasons,
        stable_season_sign_ratio=stable_season_sign_ratio,
        stable=not rejection_reasons,
        rejection_reasons=tuple(rejection_reasons),
    )


def _fit_models(x: np.ndarray, y: np.ndarray) -> dict[str, _Model]:
    return {
        "power": _fit_power(x, y),
        "platt": _fit_platt(x, y),
        "isotonic": _fit_isotonic(x, y),
        "local_linear": _Model("local_linear", x=x.copy(), y=y.copy()),
    }


def _fit_power(x: np.ndarray, y: np.ndarray) -> _Model:
    best_alpha = 1.0
    best_loss = math.inf
    low, high = 0.20, 5.0
    for _ in range(4):
        for alpha in np.linspace(low, high, 81):
            odds = np.power(x / (1.0 - x), alpha)
            q = np.clip(odds / (1.0 + odds), EPS, 1.0 - EPS)
            loss = _log_loss(q, y)
            if loss < best_loss:
                best_alpha, best_loss = float(alpha), loss
        width = (high - low) / 10.0
        low, high = max(0.05, best_alpha - width), best_alpha + width
    return _Model("power", params=(best_alpha,))


def _fit_platt(x: np.ndarray, y: np.ndarray) -> _Model:
    feature = np.log(x / (1.0 - x))
    design = np.column_stack([np.ones_like(feature), feature])
    beta = np.asarray([0.0, 1.0], dtype=float)
    ridge = np.diag([1e-6, 1e-5])
    for _ in range(30):
        z = np.clip(design @ beta, -30.0, 30.0)
        q = 1.0 / (1.0 + np.exp(-z))
        weights = np.clip(q * (1.0 - q), 1e-6, None)
        hessian = design.T @ (design * weights[:, None]) + ridge
        gradient = design.T @ (q - y) + ridge @ beta
        try:
            step = np.linalg.solve(hessian, gradient)
        except np.linalg.LinAlgError:
            break
        beta -= step
        if float(np.max(np.abs(step))) < 1e-8:
            break
    return _Model("platt", params=(float(beta[0]), float(beta[1])))


def _fit_isotonic(x: np.ndarray, y: np.ndarray) -> _Model:
    order = np.argsort(x, kind="stable")
    sx, sy = x[order], y[order]
    unique_x, inverse = np.unique(sx, return_inverse=True)
    sums = np.bincount(inverse, weights=sy)
    weights = np.bincount(inverse).astype(float)
    means = sums / weights

    blocks: list[list[float]] = []
    for index, (mean, weight) in enumerate(zip(means, weights)):
        blocks.append([float(index), float(index), float(weight), float(mean)])
        while len(blocks) >= 2 and blocks[-2][3] > blocks[-1][3]:
            right = blocks.pop()
            left = blocks.pop()
            total_weight = left[2] + right[2]
            blocks.append([
                left[0],
                right[1],
                total_weight,
                (left[3] * left[2] + right[3] * right[2]) / total_weight,
            ])

    fitted = np.empty(len(unique_x), dtype=float)
    for start, end, _, mean in blocks:
        fitted[int(start) : int(end) + 1] = mean
    return _Model("isotonic", x=unique_x, y=np.clip(fitted, EPS, 1.0 - EPS))


def _local_linear_predict(x: np.ndarray, y: np.ndarray, point: float, bandwidth: float) -> float:
    distances = (x - point) / max(EPS, bandwidth)
    weights = np.exp(-0.5 * distances * distances)
    if float(np.sum(weights)) < EPS:
        return float(np.clip(np.mean(y), EPS, 1.0 - EPS))
    centered = x - point
    design = np.column_stack([np.ones_like(centered), centered])
    matrix = design.T @ (design * weights[:, None]) + np.diag([1e-8, 1e-8])
    target = design.T @ (weights * y)
    try:
        intercept = float(np.linalg.solve(matrix, target)[0])
    except np.linalg.LinAlgError:
        intercept = float(np.average(y, weights=weights))
    return float(np.clip(intercept, EPS, 1.0 - EPS))


def _cluster_bootstrap_predictions(
    rows: list[CalibrationRow],
    *,
    model_name: str,
    probability: float,
    samples: int,
    seed_key: str,
) -> list[float]:
    if samples <= 0:
        return []
    by_market: dict[int, list[CalibrationRow]] = {}
    for row in rows:
        by_market.setdefault(row.market_id, []).append(row)
    market_ids = sorted(by_market)
    if len(market_ids) < 2:
        return []
    digest = hashlib.sha256(seed_key.encode("utf-8")).digest()
    rng = np.random.default_rng(int.from_bytes(digest[:8], "big"))
    predictions: list[float] = []
    for _ in range(samples):
        picked = rng.choice(market_ids, size=len(market_ids), replace=True)
        sample_rows = [row for market_id in picked for row in by_market[int(market_id)]]
        x, y = _arrays(sample_rows)
        try:
            model = _fit_models(x, y)[model_name]
            predictions.append(float(model.predict([probability])[0]))
        except (ValueError, FloatingPointError, np.linalg.LinAlgError):
            continue
    return predictions


def _period_signs(
    rows: list[CalibrationRow],
    *,
    probability: float,
    tolerance: float,
    min_period_samples: int,
) -> list[int]:
    periods: dict[str, list[CalibrationRow]] = {}
    for row in rows:
        if abs(row.probability_yes - probability) <= tolerance:
            periods.setdefault(row.signal_time.strftime("%Y-%m"), []).append(row)
    signs: list[int] = []
    for period_rows in periods.values():
        if len(period_rows) < min_period_samples:
            continue
        residual = float(np.mean([
            (1.0 if row.yes_won else 0.0) - row.probability_yes
            for row in period_rows
        ]))
        signs.append(_sign(residual))
    return signs


def _season_signs(
    rows: list[CalibrationRow],
    *,
    probability: float,
    tolerance: float,
    min_season_samples: int,
) -> list[int]:
    seasons: dict[str, list[CalibrationRow]] = {}
    for row in rows:
        if abs(row.probability_yes - probability) <= tolerance:
            category = str(getattr(row, "category", "")).lower()
            if category == "sports":
                start_year = row.signal_time.year if row.signal_time.month >= 7 else row.signal_time.year - 1
                season = f"{start_year}-{start_year + 1}"
            else:
                season = str(row.signal_time.year)
            seasons.setdefault(season, []).append(row)
    signs: list[int] = []
    for season_rows in seasons.values():
        if len(season_rows) < min_season_samples:
            continue
        residual = float(np.mean([
            (1.0 if row.yes_won else 0.0) - row.probability_yes
            for row in season_rows
        ]))
        signs.append(_sign(residual))
    return signs


def _wilson_interval(wins: int, samples: int, *, confidence_level: float) -> tuple[float, float]:
    if samples <= 0:
        return 0.0, 1.0
    level = max(0.50, min(0.999, confidence_level))
    z = NormalDist().inv_cdf(0.5 + level / 2.0)
    n = float(samples)
    p = float(wins) / n
    denominator = 1.0 + z * z / n
    center = (p + z * z / (2.0 * n)) / denominator
    margin = z * math.sqrt((p * (1.0 - p) + z * z / (4.0 * n)) / n) / denominator
    return max(0.0, center - margin), min(1.0, center + margin)


def _arrays(rows: Iterable[CalibrationRow]) -> tuple[np.ndarray, np.ndarray]:
    values = list(rows)
    x = np.clip(np.asarray([row.probability_yes for row in values], dtype=float), EPS, 1.0 - EPS)
    y = np.asarray([1.0 if row.yes_won else 0.0 for row in values], dtype=float)
    return x, y


def _brier(prediction: np.ndarray, outcome: np.ndarray) -> float:
    return float(np.mean(np.square(prediction - outcome)))


def _log_loss(prediction: np.ndarray, outcome: np.ndarray) -> float:
    q = np.clip(prediction, EPS, 1.0 - EPS)
    return float(-np.mean(outcome * np.log(q) + (1.0 - outcome) * np.log(1.0 - q)))


def _sign(value: float, tolerance: float = 1e-8) -> int:
    if value > tolerance:
        return 1
    if value < -tolerance:
        return -1
    return 0


def _rejected_estimate(reason: str, sample_count: int, market_count: int) -> CalibrationEstimate:
    return CalibrationEstimate(
        selected_model="",
        probability=0.0,
        lower_bound=0.0,
        upper_bound=1.0,
        sample_count=sample_count,
        market_count=market_count,
        validation_count=0,
        validation_brier=0.0,
        validation_baseline_brier=0.0,
        model_predictions={},
        model_briers={},
        model_sign_agreement=0.0,
        stable_periods=0,
        stable_period_sign_ratio=0.0,
        stable_seasons=0,
        stable_season_sign_ratio=0.0,
        stable=False,
        rejection_reasons=(reason,),
    )
