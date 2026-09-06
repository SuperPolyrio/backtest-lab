#!/usr/bin/env python3
"""Train the probabilistic trade-tape profile from OrderFilled data only."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.calibration import calibration_curve
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import QuantileRegressor, Ridge
from sklearn.metrics import (
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_pinball_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from quant.backtest.orderfilled_probability import (  # noqa: E402
    FEATURE_NAMES,
    extract_orderfilled_probability_features,
    future_same_side_fill_label,
)
from quant.backtest.orderfilled_v2_replay import (  # noqa: E402
    V2TakerOrder,
    V2TradePrint,
    trade_print_from_row,
)
from quant.core.db import ClickHouseClient  # noqa: E402


DEFAULT_PROFILE = (
    PROJECT_ROOT / "config" / "execution" / "orderfilled_probability_profile.v1.json"
)
DEFAULT_REPORT = (
    PROJECT_ROOT
    / "runtime_outputs"
    / "fill_trade_validation"
    / "orderfilled_probability_calibration.json"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-id", action="append", type=int, default=[])
    parser.add_argument("--auto-markets", type=int, default=3)
    parser.add_argument("--rows-per-market", type=int, default=10000)
    parser.add_argument("--sample-stride", type=int, default=2)
    parser.add_argument("--lookback-seconds", type=int, default=300)
    parser.add_argument("--lookback-blocks", type=int, default=300)
    parser.add_argument("--horizon-seconds", type=int, default=30)
    parser.add_argument("--horizon-blocks", type=int, default=100)
    parser.add_argument("--limit-offset", type=Decimal, default=Decimal("0.01"))
    parser.add_argument("--price-buffer", type=Decimal, default=Decimal("0.005"))
    parser.add_argument("--target-precision", type=float, default=0.90)
    parser.add_argument("--target-recall", type=float, default=0.95)
    parser.add_argument("--capacity-floor", type=Decimal, default=Decimal("0.50"))
    parser.add_argument("--conditional-capacity-quantile", type=float, default=0.25)
    parser.add_argument(
        "--expected-capacity-floor", type=Decimal, default=Decimal("0.25")
    )
    parser.add_argument(
        "--conservative-capacity-floor", type=Decimal, default=Decimal("0.05")
    )
    parser.add_argument("--min-threshold-support", type=int, default=30)
    parser.add_argument("--output-profile", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--output-report", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = ClickHouseClient()
    candidates = _resolve_candidates(client, args)
    samples: list[dict[str, Any]] = []
    market_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        trades = _load_market_trades(client, candidate, args.rows_per_market)
        rows = _build_samples(trades, args)
        samples.extend(rows)
        market_rows.append(
            {
                **candidate,
                "trade_rows": len(trades),
                "training_first_block": trades[0].block_number if trades else None,
                "training_last_block": trades[-1].block_number if trades else None,
                "samples": len(rows),
                "positive_samples": sum(int(row["label"]) for row in rows),
            }
        )
    if len(samples) < 100:
        raise RuntimeError(
            f"insufficient OrderFilled samples for calibration: {len(samples)}"
        )

    train, calibration, test = _chronological_split(samples)
    model, scaler = _fit(train)
    calibration_raw = _predict(model, scaler, calibration)
    calibrator = IsotonicRegression(out_of_bounds="clip")
    calibrator.fit(
        calibration_raw, np.asarray([row["label"] for row in calibration], dtype=int)
    )
    calibration_prob = calibrator.predict(calibration_raw)
    threshold, threshold_policy = _select_threshold(
        np.asarray([row["label"] for row in calibration], dtype=int),
        calibration_prob,
        target_precision=args.target_precision,
        target_recall=args.target_recall,
        min_support=args.min_threshold_support,
    )
    test_raw = _predict(model, scaler, test)
    test_prob = calibrator.predict(test_raw)
    metrics = _metrics(
        np.asarray([row["label"] for row in test], dtype=int),
        test_prob,
        threshold=threshold,
    )
    capacity_models = _fit_capacity_models(
        train, scaler, quantile=args.conditional_capacity_quantile
    )
    capacity_metrics = _capacity_metrics(test, scaler, capacity_models)
    activation = (
        "READY_SOURCE_CONFIRMED"
        if (
            metrics["roc_auc"] >= 0.60
            and metrics["brier_score"] <= 0.20
            and metrics["calibration_error"] <= 0.15
            and capacity_metrics["samples"] >= 100
            and capacity_metrics["expected_mae"] <= 0.35
            and len(samples) >= 1000
        )
        else "REVIEW"
    )
    profile = _build_profile(
        args,
        samples=samples,
        model=model,
        scaler=scaler,
        calibrator=calibrator,
        threshold=threshold,
        metrics=metrics,
        activation=activation,
        market_rows=market_rows,
        threshold_policy=threshold_policy,
        capacity_models=capacity_models,
        capacity_metrics=capacity_metrics,
    )
    report = {
        "schema_version": "orderfilled_probability_calibration_report_v1",
        "status": "pass",
        "data_source": "trade_prints_one_sided",
        "samples": len(samples),
        "split_samples": {
            "train": len(train),
            "calibration": len(calibration),
            "test": len(test),
        },
        "class_balance": {
            "positive": sum(int(row["label"]) for row in samples),
            "negative": sum(1 - int(row["label"]) for row in samples),
        },
        "threshold": threshold,
        "threshold_policy": threshold_policy,
        "target_precision": args.target_precision,
        "target_recall": args.target_recall,
        "test_metrics": metrics,
        "conditional_capacity_metrics": capacity_metrics,
        "markets": market_rows,
        "profile_activation": activation,
    }
    args.output_profile.parent.mkdir(parents=True, exist_ok=True)
    args.output_report.parent.mkdir(parents=True, exist_ok=True)
    args.output_profile.write_text(
        json.dumps(profile, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    args.output_report.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": "pass",
                "samples": len(samples),
                "activation": activation,
                "threshold": threshold,
                "precision": metrics["precision"],
                "recall": metrics["recall"],
                "conditional_capacity_expected_mae": capacity_metrics["expected_mae"],
                "conditional_capacity_conservative_coverage": capacity_metrics[
                    "conservative_lower_bound_coverage"
                ],
                "profile": str(args.output_profile),
                "report": str(args.output_report),
            },
            ensure_ascii=False,
        )
    )
    return 0


def _resolve_candidates(
    client: ClickHouseClient, args: argparse.Namespace
) -> list[dict[str, Any]]:
    if args.market_id:
        joined = ",".join(str(int(value)) for value in sorted(set(args.market_id)))
        rows = client.query_json_rows(
            f"""
            SELECT market_id, asset_id, condition_id, outcome, count() AS rows
            FROM trade_prints_one_sided
            PREWHERE market_id IN ({joined})
            WHERE price > 0 AND price <= 1 AND size_shares > 0
            GROUP BY market_id, asset_id, condition_id, outcome
            ORDER BY rows DESC
            """,
            timeout_seconds=120,
        )
        return rows
    return client.query_json_rows(
        f"""
        SELECT market_id, asset_id, condition_id, outcome, count() AS rows
        FROM trade_prints_one_sided
        WHERE price > 0 AND price <= 1 AND size_shares > 0
        GROUP BY market_id, asset_id, condition_id, outcome
        HAVING countIf(aggressor_side = 'BUY') >= 100
           AND countIf(aggressor_side = 'SELL') >= 100
        ORDER BY rows DESC
        LIMIT {max(1, int(args.auto_markets))}
        """,
        timeout_seconds=180,
    )


def _load_market_trades(
    client: ClickHouseClient, candidate: dict[str, Any], limit: int
) -> list[V2TradePrint]:
    asset = str(candidate["asset_id"]).replace("\\", "\\\\").replace("'", "\\'")
    rows = client.query_json_rows(
        f"""
        SELECT
            trade_id, market_id, condition_id, asset_id, outcome, block_number,
            block_time, tx_hash, tx_index, tx_index_source, price, size_shares,
            notional_usdc, aggressor_side, passive_side, source_log_indexes,
            source_fill_count
        FROM trade_prints_one_sided
        PREWHERE market_id = {int(candidate["market_id"])}
          AND asset_id = '{asset}'
        WHERE price > 0 AND price <= 1 AND size_shares > 0
        ORDER BY block_number ASC, tx_index ASC, tx_hash ASC, arrayMin(source_log_indexes) ASC
        LIMIT {max(100, int(limit))}
        """,
        timeout_seconds=180,
    )
    return [trade_print_from_row(row) for row in rows]


def _build_samples(
    trades: list[V2TradePrint], args: argparse.Namespace
) -> list[dict[str, Any]]:
    if len(trades) < 20:
        return []
    rows: list[dict[str, Any]] = []
    start = 0
    end = 1
    lookback = timedelta(seconds=max(1, args.lookback_seconds))
    horizon = timedelta(seconds=max(1, args.horizon_seconds))
    stride = max(1, int(args.sample_stride))
    for index in range(5, len(trades) - 1, stride):
        anchor = trades[index]
        while start < index and trades[start].block_time < anchor.block_time - lookback:
            start += 1
        end = max(end, index + 1)
        while (
            end < len(trades) and trades[end].block_time <= anchor.block_time + horizon
        ):
            end += 1
        context = trades[start : index + 1]
        future = trades[index + 1 : end]
        if not context:
            continue
        reference_size = max(Decimal("0.1"), anchor.size * Decimal("0.025"))
        for side in ("BUY", "SELL"):
            limit = (
                anchor.price + args.limit_offset
                if side == "BUY"
                else anchor.price - args.limit_offset
            )
            limit = max(Decimal("0.001"), min(Decimal("0.999"), limit))
            order = V2TakerOrder(
                order_id=f"cal-{anchor.trade_id}-{side}",
                market_id=anchor.market_id,
                asset_id=anchor.asset_id,
                side=side,  # type: ignore[arg-type]
                limit_price=limit,
                size=reference_size,
                signal_block=anchor.block_number,
                signal_ts=anchor.block_time,
                latency_blocks=1,
                latency=timedelta(microseconds=1),
                horizon_blocks=max(1, int(args.horizon_blocks)),
                horizon=horizon,
                participation_rate=Decimal("0.025"),
                price_buffer=max(Decimal("0"), args.price_buffer),
            )
            features = extract_orderfilled_probability_features(
                order,
                context,
                lookback=lookback,
                tick_size=Decimal("0.01"),
            )
            label, future_volume = future_same_side_fill_label(
                order, future, price_buffer=args.price_buffer
            )
            rows.append(
                {
                    "market_id": anchor.market_id,
                    "asset_id": anchor.asset_id,
                    "anchor_block": anchor.block_number,
                    "anchor_time": anchor.block_time.isoformat(),
                    "side": side,
                    "label": label,
                    "future_eligible_volume": float(future_volume),
                    "order_size": float(reference_size),
                    "participation_rate": 0.025,
                    "conditional_fill_fraction": float(
                        min(
                            Decimal("1"),
                            future_volume
                            * Decimal("0.025")
                            / max(Decimal("0.0000000001"), reference_size),
                        )
                    ),
                    "features": {
                        name: float(features.vector[name]) for name in FEATURE_NAMES
                    },
                }
            )
    return rows


def _chronological_split(
    samples: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[tuple[Any, Any], list[dict[str, Any]]] = {}
    for row in samples:
        grouped.setdefault((row["market_id"], row["asset_id"]), []).append(row)
    train: list[dict[str, Any]] = []
    calibration: list[dict[str, Any]] = []
    test: list[dict[str, Any]] = []
    for rows in grouped.values():
        ordered = sorted(rows, key=lambda row: (row["anchor_time"], row["side"]))
        first = max(1, int(len(ordered) * 0.60))
        second = max(first + 1, int(len(ordered) * 0.80))
        train.extend(ordered[:first])
        calibration.extend(ordered[first:second])
        test.extend(ordered[second:])
    return train, calibration, test


def _fit(samples: list[dict[str, Any]]) -> tuple[LogisticRegression, StandardScaler]:
    x = np.asarray(
        [[row["features"][name] for name in FEATURE_NAMES] for row in samples],
        dtype=float,
    )
    y = np.asarray([row["label"] for row in samples], dtype=int)
    if len(np.unique(y)) < 2:
        raise RuntimeError(
            "training split needs both positive and negative OrderFilled labels"
        )
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)
    model = LogisticRegression(max_iter=2000, random_state=20260723)
    model.fit(x_scaled, y)
    return model, scaler


def _predict(
    model: LogisticRegression, scaler: StandardScaler, samples: list[dict[str, Any]]
) -> np.ndarray:
    x = np.asarray(
        [[row["features"][name] for name in FEATURE_NAMES] for row in samples],
        dtype=float,
    )
    return model.predict_proba(scaler.transform(x))[:, 1]


def _fit_capacity_models(
    samples: list[dict[str, Any]],
    scaler: StandardScaler,
    *,
    quantile: float,
) -> dict[str, Any]:
    positive = [row for row in samples if int(row["label"]) == 1]
    if len(positive) < 30:
        raise RuntimeError(
            f"conditional capacity training needs at least 30 positive rows, got {len(positive)}"
        )
    x = np.asarray(
        [[row["features"][name] for name in FEATURE_NAMES] for row in positive],
        dtype=float,
    )
    y = np.asarray([row["conditional_fill_fraction"] for row in positive], dtype=float)
    x_scaled = scaler.transform(x)
    expected = Ridge(alpha=0.25)
    expected.fit(x_scaled, y)
    conservative = QuantileRegressor(
        quantile=max(0.05, min(0.50, float(quantile))),
        alpha=0.001,
        solver="highs",
    )
    conservative.fit(x_scaled, y)
    return {
        "expected": expected,
        "conservative": conservative,
        "training_rows": len(positive),
        "quantile": float(conservative.quantile),
    }


def _capacity_metrics(
    samples: list[dict[str, Any]],
    scaler: StandardScaler,
    models: dict[str, Any],
) -> dict[str, Any]:
    positive = [row for row in samples if int(row["label"]) == 1]
    if not positive:
        return {
            "samples": 0,
            "expected_mae": 1.0,
            "conservative_pinball_loss": 1.0,
            "conservative_lower_bound_coverage": 0.0,
            "actual_fill_fraction_mean": 0.0,
            "expected_fill_fraction_mean": 0.0,
            "conservative_fill_fraction_mean": 0.0,
        }
    x = np.asarray(
        [[row["features"][name] for name in FEATURE_NAMES] for row in positive],
        dtype=float,
    )
    y = np.asarray([row["conditional_fill_fraction"] for row in positive], dtype=float)
    x_scaled = scaler.transform(x)
    expected = np.clip(models["expected"].predict(x_scaled), 0.0, 1.0)
    conservative = np.clip(models["conservative"].predict(x_scaled), 0.0, 1.0)
    return {
        "samples": int(len(y)),
        "expected_mae": float(mean_absolute_error(y, expected)),
        "conservative_pinball_loss": float(
            mean_pinball_loss(y, conservative, alpha=float(models["quantile"]))
        ),
        "conservative_lower_bound_coverage": float(np.mean(conservative <= y)),
        "actual_fill_fraction_mean": float(np.mean(y)),
        "expected_fill_fraction_mean": float(np.mean(expected)),
        "conservative_fill_fraction_mean": float(np.mean(conservative)),
    }


def _select_threshold(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    *,
    target_precision: float,
    target_recall: float,
    min_support: int,
) -> tuple[float, str]:
    candidates = sorted(set(float(value) for value in probabilities))
    precision_selected: float | None = None
    for threshold in candidates:
        predicted = probabilities >= threshold
        if int(predicted.sum()) < max(1, int(min_support)):
            continue
        precision = precision_score(y_true, predicted, zero_division=0)
        if precision >= target_precision:
            precision_selected = threshold
            break
    positive_probabilities = probabilities[y_true == 1]
    if len(positive_probabilities):
        recall_selected = float(
            np.quantile(positive_probabilities, max(0.0, min(1.0, 1.0 - target_recall)))
        )
    else:
        recall_selected = 0.10
    selected = (
        min(precision_selected, recall_selected)
        if precision_selected is not None
        else recall_selected
    )
    policy = "recall_with_source_confirmation"
    return float(max(0.05, min(0.99, selected))), policy


def _metrics(
    y_true: np.ndarray, probabilities: np.ndarray, *, threshold: float
) -> dict[str, Any]:
    predicted = probabilities >= threshold
    fraction_positive, mean_predicted = calibration_curve(
        y_true, probabilities, n_bins=10, strategy="quantile"
    )
    calibration_rows = [
        {
            "mean_predicted": float(predicted_value),
            "fraction_positive": float(actual_value),
        }
        for predicted_value, actual_value in zip(mean_predicted, fraction_positive)
    ]
    calibration_error = (
        float(
            np.mean(
                [
                    abs(row["mean_predicted"] - row["fraction_positive"])
                    for row in calibration_rows
                ]
            )
        )
        if calibration_rows
        else 1.0
    )
    return {
        "samples": int(len(y_true)),
        "positive_samples": int(y_true.sum()),
        "predicted_positive_samples": int(predicted.sum()),
        "precision": float(precision_score(y_true, predicted, zero_division=0)),
        "recall": float(recall_score(y_true, predicted, zero_division=0)),
        "brier_score": float(brier_score_loss(y_true, probabilities)),
        "log_loss": float(log_loss(y_true, probabilities, labels=[0, 1])),
        "roc_auc": float(roc_auc_score(y_true, probabilities))
        if len(np.unique(y_true)) > 1
        else 0.5,
        "calibration_error": calibration_error,
        "calibration_curve": calibration_rows,
    }


def _build_profile(
    args: argparse.Namespace,
    *,
    samples: list[dict[str, Any]],
    model: LogisticRegression,
    scaler: StandardScaler,
    calibrator: IsotonicRegression,
    threshold: float,
    metrics: dict[str, Any],
    activation: str,
    market_rows: list[dict[str, Any]],
    threshold_policy: str,
    capacity_models: dict[str, Any],
    capacity_metrics: dict[str, Any],
) -> dict[str, Any]:
    expected = capacity_models["expected"]
    conservative = capacity_models["conservative"]
    return {
        "schema_version": "orderfilled_probability_profile_v2",
        "profile_name": "orderfilled_two_stage_trade_tape",
        "model_version": "orderfilled_arrival_and_conditional_capacity_v2",
        "activation": activation,
        "training_rows": len(samples),
        "data_contract": {
            "execution_input": "trade_prints_one_sided",
            "training_input": "trade_prints_one_sided",
            "labels": "future_same_side_limit_eligible_orderfilled_within_horizon",
            "fills_require_source_trade": True,
            "probability_role": "arrival_probability_and_order_admission_only",
            "capacity_role": "conditional_fill_fraction_given_source_trade",
            "label_horizon_seconds": int(args.horizon_seconds),
            "label_horizon_blocks": int(args.horizon_blocks),
        },
        "params": {
            "name": "orderfilled_two_stage_trade_tape",
            "min_probability": str(Decimal(str(threshold))),
            "medium_probability": str(Decimal(str(threshold))),
            "high_probability": str(
                Decimal(str(min(0.99, max(0.75, threshold + 0.15))))
            ),
            "capacity_floor": str(
                max(Decimal("0"), min(Decimal("1"), args.capacity_floor))
            ),
            "capacity_mode": "conditional_source_capacity",
            "capacity_variant": "expected",
            "hard_reject_below_probability": False,
            "lookback_seconds": int(args.lookback_seconds),
            "lookback_blocks": int(args.lookback_blocks),
            "tick_size": "0.01",
            "threshold_policy": threshold_policy,
        },
        "model": {
            "type": "logistic_regression",
            "version": "orderfilled_logistic_v1",
            "feature_names": list(FEATURE_NAMES),
            "intercept": str(float(model.intercept_[0])),
            "coefficients": {
                name: str(float(value))
                for name, value in zip(FEATURE_NAMES, model.coef_[0])
            },
            "feature_means": {
                name: str(float(value))
                for name, value in zip(FEATURE_NAMES, scaler.mean_)
            },
            "feature_scales": {
                name: str(float(value))
                for name, value in zip(FEATURE_NAMES, scaler.scale_)
            },
        },
        "calibration": {
            "type": "isotonic",
            "x": [str(float(value)) for value in calibrator.X_thresholds_],
            "y": [str(float(value)) for value in calibrator.y_thresholds_],
        },
        "conditional_capacity": {
            "mode": "conditional_source_capacity",
            "default_variant": "expected",
            "feature_names": list(FEATURE_NAMES),
            "models": {
                "source_confirmed": {
                    "name": "source_confirmed",
                    "target": "observed_source_participation_cap",
                    "quantile": None,
                    "floor": "1",
                    "intercept": "1",
                    "coefficients": {name: "0" for name in FEATURE_NAMES},
                    "training_rows": int(capacity_models["training_rows"]),
                },
                "expected": {
                    "name": "expected",
                    "target": "conditional_fill_fraction",
                    "quantile": None,
                    "floor": str(
                        max(
                            Decimal("0"),
                            min(Decimal("1"), args.expected_capacity_floor),
                        )
                    ),
                    "intercept": str(float(expected.intercept_)),
                    "coefficients": {
                        name: str(float(value))
                        for name, value in zip(FEATURE_NAMES, expected.coef_)
                    },
                    "training_rows": int(capacity_models["training_rows"]),
                },
                "conservative": {
                    "name": "conservative",
                    "target": "conditional_fill_fraction",
                    "quantile": str(capacity_models["quantile"]),
                    "floor": str(
                        max(
                            Decimal("0"),
                            min(Decimal("1"), args.conservative_capacity_floor),
                        )
                    ),
                    "intercept": str(float(conservative.intercept_)),
                    "coefficients": {
                        name: str(float(value))
                        for name, value in zip(FEATURE_NAMES, conservative.coef_)
                    },
                    "training_rows": int(capacity_models["training_rows"]),
                },
            },
            "test_metrics": capacity_metrics,
        },
        "test_metrics": metrics,
        "trained_markets": [
            {
                key: value
                for key, value in row.items()
                if key
                in {
                    "market_id",
                    "asset_id",
                    "condition_id",
                    "outcome",
                    "trade_rows",
                    "training_first_block",
                    "training_last_block",
                    "samples",
                    "positive_samples",
                }
            }
            for row in market_rows
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())
