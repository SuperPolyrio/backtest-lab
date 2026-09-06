from __future__ import annotations

import json
import math
from argparse import Namespace
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import numpy as np
import pytest

from quant.backtest.probability_quality import adaptive_probability_quality
from quant.backtest.orderfilled_probability import hierarchical_probability_keys
from quant.backtest.trade_only_v3.hierarchical_prior import (
    HierarchicalContext,
    resolve_hierarchical_prior,
)
from quant.backtest.trade_only_v3.live_labels import build_live_order_label_readiness
from quant.backtest.trade_only_v3.price_buffer import (
    load_price_buffer_profile,
    resolve_price_buffer,
)
from scripts import calibrate_fill_only_v3_hierarchical_overlay as overlay
from scripts.calibrate_fill_only_v3_l2_reference import (
    _assert_disjoint,
    _candidate_thresholds,
    _expected_gate_penalty,
    _expected_metrics,
    _fit_capacity,
    _passes_expected_gates,
    _source_result,
    _thresholds,
    _unstable_activity_regimes,
)
from scripts.calibrate_toolkit_markout_buffer import (
    _coverage,
    _observation,
    _observation_quality,
)

CONTEXT = HierarchicalContext(
    market_id="42",
    category="sports",
    league="nba",
    price_bucket="60_75",
    tte_bucket="90_240m",
    liquidity_regime="sparse",
)


def test_adaptive_quality_accepts_skillful_clustered_probabilities() -> None:
    labels = [value for _ in range(40) for value in (1.0,) * 5 + (0.0,) * 5]
    probabilities = [0.9 if value else 0.1 for value in labels]
    clusters = [cluster for cluster in range(40) for _ in range(10)]

    quality = adaptive_probability_quality(
        probabilities,
        labels,
        cluster_ids=clusters,
        bootstrap_replicates=200,
    )

    assert quality["status"] == "PASS"
    assert quality["brier_skill_score"] > 0
    assert all(quality["quality_checks"].values())
    assert quality["calibration_intercept"] is None
    assert quality["calibration_slope"] is None


def test_adaptive_quality_does_not_use_point_two_as_a_universal_cutoff() -> None:
    labels = [value for _ in range(40) for value in (1.0,) * 5 + (0.0,) * 5]
    probabilities = [0.55 if value else 0.45 for value in labels]
    clusters = [cluster for cluster in range(40) for _ in range(10)]

    quality = adaptive_probability_quality(
        probabilities,
        labels,
        cluster_ids=clusters,
        bootstrap_replicates=200,
    )

    assert quality["brier_score"] > 0.20
    assert quality["brier_score"] < quality["reference_brier_score"]
    assert quality["status"] == "PASS"


def test_adaptive_quality_reports_reference_relative_log_loss() -> None:
    labels = [value for _ in range(40) for value in (1.0,) * 5 + (0.0,) * 5]
    probabilities = [0.9 if value else 0.1 for value in labels]
    clusters = [cluster for cluster in range(40) for _ in range(10)]

    quality = adaptive_probability_quality(
        probabilities,
        labels,
        cluster_ids=clusters,
        bootstrap_replicates=200,
    )

    assert quality["log_loss"] < quality["reference_log_loss"]
    assert quality["log_loss_regret"] < 0
    assert quality["log_loss_skill_score"] > 0
    assert quality["confidence_intervals"]["log_loss_regret"][1] < 0
    assert quality["guardrail_checks"]["no_detected_log_loss_harm"] is True


def test_adaptive_quality_rejects_correct_total_on_wrong_orders() -> None:
    labels = [value for _ in range(40) for value in (1.0,) * 5 + (0.0,) * 5]
    probabilities = [0.0 if value else 1.0 for value in labels]
    clusters = [cluster for cluster in range(40) for _ in range(10)]

    quality = adaptive_probability_quality(
        probabilities,
        labels,
        cluster_ids=clusters,
        bootstrap_replicates=200,
    )

    assert quality["expected_positive_order_ratio"] == pytest.approx(1.0)
    assert quality["status"] == "FAIL"
    assert quality["quality_checks"]["reference_relative_brier_skill"] is False


def test_adaptive_quality_rejects_correct_total_quantity_on_wrong_orders() -> None:
    labels = [value for _ in range(40) for value in (1.0,) * 5 + (0.0,) * 5]
    probabilities = [0.9 if value else 0.1 for value in labels]
    reference_quantities = labels
    expected_quantities = [1.0 - value for value in labels]
    clusters = [cluster for cluster in range(40) for _ in range(10)]

    quality = adaptive_probability_quality(
        probabilities,
        labels,
        expected_quantities=expected_quantities,
        reference_quantities=reference_quantities,
        cluster_ids=clusters,
        bootstrap_replicates=200,
    )

    assert quality["expected_quantity_ratio"] == pytest.approx(1.0)
    assert quality["quantity_regret"] > 0
    assert quality["status"] == "FAIL"
    assert quality["quality_checks"]["reference_relative_quantity_skill"] is False


def test_adaptive_quantity_score_is_normalized_by_requested_size() -> None:
    labels = [value for _ in range(40) for value in (1.0,) * 5 + (0.0,) * 5]
    probabilities = [0.9 if value else 0.1 for value in labels]
    size_cycle = (1.0, 5.0, 10.0, 25.0, 100.0)
    scales = [size_cycle[index % 5] for index in range(len(labels))]
    reference_quantities = [label * scale for label, scale in zip(labels, scales)]
    expected_quantities = [
        probability * scale for probability, scale in zip(probabilities, scales)
    ]
    clusters = [cluster for cluster in range(40) for _ in range(10)]

    quality = adaptive_probability_quality(
        probabilities,
        labels,
        expected_quantities=expected_quantities,
        reference_quantities=reference_quantities,
        quantity_scales=scales,
        cluster_ids=clusters,
        bootstrap_replicates=200,
    )

    assert quality["quantity_fraction_mse"] == pytest.approx(0.01)
    assert quality["status"] == "PASS"


def test_adaptive_quality_requires_independent_clusters() -> None:
    labels = [value for _ in range(25) for value in (1.0,) * 5 + (0.0,) * 5]
    probabilities = [0.8 if value else 0.2 for value in labels]

    quality = adaptive_probability_quality(
        probabilities,
        labels,
        cluster_ids=["one-market-day"] * len(labels),
        bootstrap_replicates=50,
    )

    assert quality["status"] == "INSUFFICIENT_SAMPLE"
    assert quality["sample_checks"]["minimum_independent_clusters"] is False


def test_probability_calibration_recovers_source_route_from_central_result() -> None:
    central = {
        "filled_size": "2.5",
        "model_diagnostics": {"selected_route": "taker_source_confirmed"},
    }
    modeled = {
        "filled_size": "8.0",
        "model_diagnostics": {"selected_route": "taker_hierarchical_expected"},
    }

    assert _source_result({"v3_l2_expected": central}) is central
    assert _source_result({"v3_l2_expected": modeled}) == {"filled_size": "0"}


def test_direct_probability_fit_can_explicitly_omit_source_model() -> None:
    assert _source_result({}, allow_missing=True) == {"filled_size": "0"}
    with pytest.raises(KeyError, match="v3_source_fak"):
        _source_result({})


def test_hierarchical_direct_fit_can_load_without_source_model(tmp_path) -> None:
    path = tmp_path / "orders.jsonl"
    path.write_text(
        json.dumps(
            {
                "market_id": 1,
                "asset_id": "asset",
                "decision_ts": "2026-07-01T00:00:00+00:00",
                "limit_price": "0.50",
                "size": "10",
                "side": "BUY",
                "tif": "FOK",
                "models": {"pml2_fok": {"status": "FILLED", "filled_size": "10"}},
                "fill_only_feature_contract": (
                    "ARRIVAL_BLOCK_AND_TIME_SIGNAL_EXCLUDED_V2"
                ),
                "fill_only_features": {
                    "vector": {
                        "log_order_to_tape_ratio": "0",
                        "limit_price": "0.50",
                        "same_flow_share": "0.5",
                    },
                    "trailing_same_count": 0,
                    "trailing_opposite_count": 0,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    rows = overlay._load(
        [path],
        reference_model="pml2_fok",
        probability_target="FOK_FULL_FILL",
        source_required=False,
    )

    assert rows[0]["source_positive"] is False
    assert rows[0]["vector"]["log_order_size"] == pytest.approx(math.log1p(10))


def test_saved_label_validator_uses_family_calibration_curve() -> None:
    row = {
        "keys": ("family|sports", "global"),
        "vector": {},
    }
    artifact = {
        "model": {
            "intercept": "0",
            "coefficients": {},
            "feature_means": {},
            "feature_scales": {},
        },
        "calibration": {
            "x": ["0", "1"],
            "y": ["0.2", "0.2"],
            "by_category_family": {"sports": {"x": ["0", "1"], "y": ["0.8", "0.8"]}},
        },
    }

    assert overlay._base_probability(row, artifact) == pytest.approx(0.8)


def test_temporally_unstable_activity_regime_is_abstained() -> None:
    def rows(count: int, positives: int, log_count: float) -> list[dict]:
        return [
            {
                "market_id": index % 4,
                "label": float(index < positives),
                "features": [0.0] * 15 + [log_count],
            }
            for index in range(count)
        ]

    unstable, diagnostics = _unstable_activity_regimes(
        rows(100, 20, math.log1p(51)),
        rows(100, 80, math.log1p(51)),
        Namespace(
            minimum_temporal_activity_rows=40,
            minimum_temporal_activity_markets=3,
            maximum_temporal_activity_drift=0.20,
        ),
    )

    assert unstable == {"ACTIVE_GT_50"}
    assert diagnostics["ACTIVE_GT_50"]["absolute_drift"] == pytest.approx(0.6)


def test_probability_calibration_cohort_must_be_disjoint() -> None:
    row = {"market_id": 1, "asset_id": "asset", "decision_ts": "2026-01-01"}

    with pytest.raises(RuntimeError, match="fit and probability_calibration overlap"):
        _assert_disjoint([row], [], [row])


def test_conditional_capacity_keeps_partial_fill_signal_when_full_fills_dominate() -> None:
    x = np.arange(200, dtype=np.float64).reshape(100, 2)
    y = np.ones(100, dtype=np.float64)
    fractions = np.r_[np.full(5, 0.5), np.ones(95)]

    beta, _, fit = _fit_capacity(x, y, fractions, ridge=0.1)

    assert fit["method"] == "RIDGE_LINEAR"
    assert fit["full_fill_share"] == pytest.approx(0.95)
    assert fit["target_range"] == pytest.approx(0.5)
    assert np.count_nonzero(beta[1:]) > 0


def test_conditional_capacity_uses_constant_only_for_constant_target() -> None:
    x = np.arange(200, dtype=np.float64).reshape(100, 2)
    y = np.ones(100, dtype=np.float64)
    fractions = np.ones(100, dtype=np.float64)

    beta, _, fit = _fit_capacity(x, y, fractions, ridge=0.1)

    assert fit["method"] == "CONSTANT_TARGET"
    assert fit["target_range"] == 0.0
    assert beta[0] == 1.0
    assert np.count_nonzero(beta[1:]) == 0


def test_expected_metrics_treat_source_as_floor_not_capacity_ceiling() -> None:
    rows = [
        {
            "label": 1,
            "source_positive": True,
            "source_fraction": 0.1,
            "conditional_fraction": 1.0,
        }
    ]

    metrics = _expected_metrics(
        rows,
        np.asarray([0.2], dtype=np.float64),
        np.asarray([0.9], dtype=np.float64),
    )

    assert metrics["expected_positive_orders"] == 1.0
    assert metrics["expected_quantity_fraction"] == 0.9


def test_hierarchical_overlay_can_restore_a_previously_abstained_family(
    tmp_path, monkeypatch
) -> None:
    base = tmp_path / "base.json"
    output = tmp_path / "overlay.json"
    base.write_text(
        json.dumps(
            {
                "model_version": "base",
                "model": {
                    "intercept": "0",
                    "coefficients": {},
                    "feature_means": {},
                    "feature_scales": {},
                },
                "calibration": {},
                "conditional_capacity": {
                    "models": {
                        "expected": {
                            "intercept": "1",
                            "floor": "0",
                            "coefficients": {},
                        }
                    }
                },
                "domain_gate": {"abstain_category_families": ["crypto"]},
            }
        ),
        encoding="utf-8",
    )

    def rows(prefix: str) -> list[dict]:
        result = []
        for index in range(40):
            family = "crypto" if index < 20 else "politics"
            category = family
            result.append(
                {
                    "identity": (index, prefix, f"{prefix}-{index}"),
                    "keys": (
                        f"category_price|{category}|50_80",
                        f"category|{category}",
                        f"family_price|{family}|50_80",
                        f"family|{family}",
                        "price|50_80",
                        "global",
                    ),
                    "vector": {},
                    "label": 1.0,
                    "reference_fraction": 1.0,
                    "source_positive": False,
                    "source_fraction": 0.0,
                }
            )
        return result

    calibration = rows("calibration")
    validation = rows("validation")
    monkeypatch.setattr(
        overlay,
        "_load",
        lambda paths, **_kwargs: (
            calibration if paths[0].name == "calibration" else validation
        ),
    )
    artifact = overlay.calibrate(
        Namespace(
            base_artifact=base,
            calibration_orders=[tmp_path / "calibration"],
            validation_orders=[tmp_path / "validation"],
            output=output,
            model_version="hierarchical-test",
            minimum_samples=[10],
            beta_strength=[2.0],
            blend_strength=[5.0],
            probability_scale=[1.05],
            maximum_brier_score=0.20,
            minimum_ratio=0.85,
            maximum_ratio=1.15,
            minimum_family_samples=10,
            minimum_family_markets=10,
            minimum_category_samples=10,
            minimum_category_markets=10,
            maximum_abstain_row_share=0.35,
            maximum_abstain_positive_share=0.15,
            refit_with_validation=True,
        )
    )

    assert artifact["promotion_allowed"] is True
    assert artifact["model_version"] == "hierarchical-test"
    assert artifact["domain_gate"]["abstain_category_families"] == []
    assert artifact["domain_gate"]["abstain_categories"] == []
    assert (
        artifact["hierarchical_probability"]["refit_contract"]
        == "CALIBRATION_PLUS_SELECTION_HOLDOUT"
    )
    assert artifact["cohorts"]["hierarchical_runtime_refit_rows"] == 80
    assert artifact["hierarchical_probability"]["probability_scale"] == "1.05"


def test_hierarchical_family_gate_compares_brier_to_family_baseline() -> None:
    args = Namespace(
        maximum_brier_score=0.25,
        maximum_family_brier_regret=0.01,
        minimum_ratio=0.85,
        maximum_ratio=1.15,
    )
    metrics = {
        "samples": 100,
        "reference_positive_orders": 50,
        "expected_positive_order_ratio": 1.0,
        "expected_quantity_ratio": 1.0,
        "brier_score": 0.24,
    }

    assert overlay._family_brier_baseline(metrics) == 0.25
    assert overlay._passes_family(metrics, args) is True

    metrics["brier_score"] = 0.27
    assert overlay._passes_family(metrics, args) is False


def test_hierarchical_global_gate_requires_absolute_and_relative_brier() -> None:
    args = Namespace(
        maximum_brier_score=0.20,
        maximum_brier_regret=0.01,
        minimum_ratio=0.85,
        maximum_ratio=1.15,
    )
    metrics = {
        "samples": 100,
        "reference_positive_orders": 50,
        "expected_positive_order_ratio": 1.0,
        "expected_quantity_ratio": 1.0,
        "brier_score": 0.21,
    }

    assert overlay._passes(metrics, args) is False
    metrics["brier_score"] = 0.19
    assert overlay._passes(metrics, args) is True


def test_hierarchical_fallback_is_selected_only_when_holdout_improves() -> None:
    args = Namespace(
        maximum_brier_score=0.25,
        maximum_family_brier_regret=0.01,
        minimum_ratio=0.85,
        maximum_ratio=1.15,
    )
    raw = {
        "samples": 100,
        "reference_positive_orders": 50,
        "expected_positive_order_ratio": 0.70,
        "expected_quantity_ratio": 0.70,
        "brier_score": 0.20,
    }
    fallback = {
        **raw,
        "expected_positive_order_ratio": 1.0,
        "expected_quantity_ratio": 1.0,
    }

    assert overlay._prefer_group_fallback(raw, fallback, args) is True
    assert overlay._prefer_group_fallback(fallback, raw, args) is False


def test_hierarchical_candidate_penalizes_undercoverage() -> None:
    args = Namespace(
        maximum_brier_score=0.25,
        maximum_family_brier_regret=0.01,
        minimum_ratio=0.80,
        maximum_ratio=1.20,
    )
    metrics = {
        "samples": 100,
        "reference_positive_orders": 50,
        "expected_positive_order_ratio": 0.70,
        "expected_quantity_ratio": 0.75,
        "brier_score": 0.20,
    }

    assert overlay._gate_penalty(metrics, args) == pytest.approx(0.10)


def test_market_prior_requires_multiple_independent_windows() -> None:
    key = "market_asset|1|token"
    rows = [
        {
            "keys": (key, "global"),
            "label": 1.0,
            "window": "2026-07-01T00",
            "identity": (1, "token", index),
        }
        for index in range(20)
    ]
    cells = overlay._cells(rows)

    assert cells[key] == (20, 20, 1, 1, 20.0)
    assert overlay._resolve(rows[0], cells, 10)[0] == "global"


def test_category_prior_requires_multiple_markets_and_windows() -> None:
    key = "category_price|sports|50_80"
    repeated_market = [
        {
            "keys": (key, "global"),
            "label": 1.0,
            "reference_fraction": 0.5,
            "window": f"2026-07-0{index % 3 + 1}T00",
            "identity": (1, "token", index),
        }
        for index in range(30)
    ]
    independent = [
        {
            **row,
            "identity": (index % 3 + 1, "token", index),
        }
        for index, row in enumerate(repeated_market)
    ]

    repeated_cells = overlay._cells(repeated_market)
    independent_cells = overlay._cells(independent)

    assert overlay._resolve(repeated_market[0], repeated_cells, 10)[0] == "global"
    assert overlay._resolve(independent[0], independent_cells, 10)[0] == key


def test_l2_reference_expected_gate_requires_absolute_and_relative_brier() -> None:
    rows = [
        {
            "label": float(index < 65),
            "source_positive": False,
            "source_fraction": 0.0,
            "conditional_fraction": float(index < 65),
        }
        for index in range(100)
    ]
    metrics = _expected_metrics(
        rows,
        np.full(100, 0.65),
        np.ones(100),
    )
    args = Namespace(
        maximum_expected_brier_score=0.20,
        maximum_expected_brier_regret=0.01,
        minimum_expected_ratio=0.80,
        maximum_expected_ratio=1.20,
    )

    assert metrics["brier_score"] > 0.20
    assert abs(float(metrics["brier_regret"])) < 1e-12
    assert _passes_expected_gates(metrics, args) is False


def test_l2_reference_expected_penalty_catches_family_ratio_drift() -> None:
    args = Namespace(
        maximum_expected_brier_score=0.20,
        maximum_expected_brier_regret=0.01,
        minimum_expected_ratio=0.85,
        maximum_expected_ratio=1.15,
    )
    metrics = {
        "brier_score": 0.19,
        "brier_regret": -0.01,
        "expected_positive_order_ratio": 0.84,
        "expected_quantity_ratio": 1.0,
    }

    assert _expected_gate_penalty(metrics, args) == pytest.approx(0.01)

    metrics["expected_positive_order_ratio"] = 0.9
    assert _expected_gate_penalty(metrics, args) == 0


def test_hierarchical_prior_uses_most_specific_cell_then_global() -> None:
    profile = {
        "horizons": {
            "30": {
                "global": {"p_fill": "0.10", "samples": 100},
                "levels": [
                    {
                        "fields": ["category"],
                        "cells": {"sports": {"p_fill": "0.20", "samples": 50}},
                    },
                    {
                        "fields": ["market_id", "price_bucket"],
                        "cells": {"42|60_75": {"p_fill": "0.40", "samples": 25}},
                    },
                ],
            }
        }
    }

    resolved = resolve_hierarchical_prior(profile, horizon_seconds=30, context=CONTEXT)
    missing = resolve_hierarchical_prior(
        profile,
        horizon_seconds=30,
        context=HierarchicalContext(
            "99", "politics", "unknown", "25_40", "1d", "active"
        ),
    )

    assert resolved["p_fill"] == "0.40"
    assert resolved["level"] == "market_id+price_bucket"
    assert missing["p_fill"] == "0.10"
    assert missing["level"] == "global"


def test_hierarchical_prior_skips_cells_below_required_support() -> None:
    profile = {
        "horizons": {
            "30": {
                "global": {"p_fill": "0.10", "samples": 1_000},
                "levels": [
                    {
                        "fields": ["category"],
                        "cells": {"sports": {"p_fill": "0.20", "samples": 50}},
                    },
                    {
                        "fields": ["market_id", "price_bucket"],
                        "cells": {"42|60_75": {"p_fill": "0.40", "samples": 25}},
                    },
                ],
            }
        }
    }

    resolved = resolve_hierarchical_prior(
        profile,
        horizon_seconds=30,
        context=CONTEXT,
        minimum_samples=100,
    )

    assert resolved["p_fill"] == "0.10"
    assert resolved["level"] == "global"


def test_hierarchical_prior_can_separate_buy_and_sell_direction() -> None:
    profile = {
        "horizons": {
            "5": {
                "global": {"p_fill": "0.10", "samples": 1_000},
                "levels": [
                    {
                        "fields": ["category", "side"],
                        "cells": {
                            "sports|buy": {"p_fill": "0.20", "samples": 200},
                            "sports|sell": {"p_fill": "0.05", "samples": 200},
                        },
                    }
                ],
            }
        }
    }
    buy = HierarchicalContext(
        "42", "sports", "nba", "60_75", "90_240m", "sparse", "buy"
    )
    sell = HierarchicalContext(
        "42", "sports", "nba", "60_75", "90_240m", "sparse", "sell"
    )

    buy_prior = resolve_hierarchical_prior(
        profile, horizon_seconds=5, context=buy, minimum_samples=100
    )
    sell_prior = resolve_hierarchical_prior(
        profile, horizon_seconds=5, context=sell, minimum_samples=100
    )

    assert buy_prior["p_fill"] == "0.20"
    assert sell_prior["p_fill"] == "0.05"


def test_execution_contract_keeps_price_before_category_only_fallback() -> None:
    order = SimpleNamespace(
        market_id="market-1",
        asset_id="asset-1",
        category="politics",
        side="BUY",
        tif="FOK",
        size=Decimal("25"),
        limit_price=Decimal("0.52"),
        signal_ts=datetime(2026, 8, 1, 12, tzinfo=timezone.utc),
        market_end_ts=None,
    )
    keys = hierarchical_probability_keys(
        order,
        {"log_total_trade_count": Decimal("2")},
        key_scheme="validation_aligned_v2",
    )

    assert keys.index("contract_price_activity|buy|fok|le_25|40_60|medium") < keys.index(
        "category_contract|politics|buy|fok|le_25"
    )


def test_probability_threshold_grid_remains_available_for_classification() -> None:
    probability = np.asarray([0.2, 0.8])

    assert tuple(_candidate_thresholds(probability, "expected")) == (0.0,)
    assert _thresholds(probability)[0] == pytest.approx(0.01)
    assert _thresholds(probability)[-1] == pytest.approx(0.99)


def test_execution_hierarchy_identifies_crypto_five_minute_product() -> None:
    order = SimpleNamespace(
        market_id="market-1",
        asset_id="asset-1",
        category="crypto",
        market_slug="btc-updown-5m-1786037100",
        market_title="Bitcoin Up or Down - August 6, 1:25PM-1:30PM ET",
        side="BUY",
        tif="FOK",
        size=Decimal("10"),
        limit_price=Decimal("0.52"),
        signal_ts=datetime(2026, 8, 1, 12, tzinfo=timezone.utc),
        market_end_ts=None,
    )
    keys = hierarchical_probability_keys(
        order,
        {"log_total_trade_count": Decimal("1")},
        key_scheme="validation_aligned_v2",
    )

    assert (
        "product_contract_price_activity|crypto_updown_5m|buy|fok|le_10|40_60|sparse"
        in keys
    )
    assert keys.index(
        "product_contract_price|crypto_updown_5m|buy|fok|le_10|40_60"
    ) < keys.index(
        "category_contract_price_activity|crypto|buy|fok|le_10|40_60|sparse"
    )
    assert keys.index(
        "product_price_activity|crypto_updown_5m|40_60|sparse"
    ) < keys.index(
        "category_contract_price_activity|crypto|buy|fok|le_10|40_60|sparse"
    )
    assert keys.index(
        "product_contract|crypto_updown_5m|buy|fok|le_10"
    ) < keys.index(
        "category_contract_price_activity|crypto|buy|fok|le_10|40_60|sparse"
    )


def test_hierarchical_prior_does_not_treat_unknown_taxonomy_as_a_segment() -> None:
    profile = {
        "horizons": {
            "5": {
                "global": {"p_fill": "0.10", "samples": 1_000},
                "levels": [
                    {
                        "fields": ["category", "league", "side"],
                        "cells": {
                            "unknown|unknown|buy": {
                                "p_fill": "0.99",
                                "samples": 999,
                            }
                        },
                    }
                ],
            }
        }
    }
    unresolved = HierarchicalContext(
        "42", "unknown", "unknown", "60_75", "unknown", "sparse", "buy"
    )

    resolved = resolve_hierarchical_prior(
        profile, horizon_seconds=5, context=unresolved, minimum_samples=100
    )

    assert resolved["p_fill"] == "0.10"
    assert resolved["level"] == "global"


def test_price_buffer_activates_only_for_ready_profile(tmp_path) -> None:
    path = tmp_path / "buffer.json"
    path.write_text(
        json.dumps(
            {
                "status": "READY",
                "global": {"buffer": "0.007", "samples": 200},
                "levels": [
                    {
                        "fields": ["category", "league"],
                        "cells": {"sports|nba": {"buffer": "0.009", "samples": 100}},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    ready = resolve_price_buffer(str(path), context=CONTEXT, fallback=Decimal("0.005"))
    path.write_text(json.dumps({"status": "INSUFFICIENT_SAMPLE"}), encoding="utf-8")
    load_price_buffer_profile.cache_clear()
    blocked = resolve_price_buffer(
        str(path), context=CONTEXT, fallback=Decimal("0.005")
    )

    assert ready["buffer"] == "0.009"
    assert ready["fallback_used"] is False
    assert blocked["buffer"] == "0.005"
    assert blocked["fallback_used"] is True


def test_markout_coverage_uses_requested_fills_as_denominator() -> None:
    rows = [
        {"samples": 499, "requested_samples": 500},
        {"samples": 123, "requested_samples": 500},
        {"samples": 155, "requested_samples": 500},
    ]

    assert _coverage(rows) == Decimal("0.518")


def test_markout_calibration_rejects_reference_windows_reaching_fill_time() -> None:
    payload = {
        "fills": 100,
        "markets": 10,
        "vwapHalfWindowSec": 30,
        "results": [
            {
                "tau": 30,
                "mine": {"n": 100},
                "coverage": 1,
                "excessCents": -1,
            }
        ],
    }

    assert _observation(payload, tau=30, source="touches-fill.json") is None

    payload["vwapHalfWindowSec"] = 15
    observation = _observation(payload, tau=30, source="post-fill.json")

    assert observation is not None
    assert observation["reference_window"] == {
        "start_seconds_after_fill": 15,
        "end_seconds_after_fill": 45,
    }


def test_markout_quality_rejects_low_coverage_and_concentrated_observations() -> None:
    rows = [
        {"samples": 98, "coverage": Decimal("0.98")},
        {"samples": 39, "coverage": Decimal("0.39")},
        {"samples": 20, "coverage": Decimal("0.20")},
    ]

    qualifying, largest_share = _observation_quality(rows, Decimal("0.50"))

    assert qualifying == 1
    assert largest_share > Decimal("0.50")


class _Cursor:
    def __init__(self, rows: list[tuple[int, ...]]) -> None:
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _query: str) -> None:
        return None

    def fetchone(self) -> tuple[int, ...]:
        return self.rows.pop(0)


class _Connection:
    def __init__(self, rows: list[tuple[int, ...]]) -> None:
        self.rows = rows

    def cursor(self) -> _Cursor:
        return _Cursor(self.rows)


def test_live_label_readiness_requires_positive_and_no_fill_labels() -> None:
    blocked = build_live_order_label_readiness(_Connection([(0, 0, 0, 0), (0, 0)]))
    ready = build_live_order_label_readiness(
        _Connection([(250, 250, 180, 70), (900, 250)])
    )

    assert blocked["status"] == "BLOCKED_INSUFFICIENT_REAL_LABELS"
    assert blocked["ready_for_live_transfer_claim"] is False
    assert ready["status"] == "READY"
    assert ready["ready_for_live_transfer_claim"] is True
