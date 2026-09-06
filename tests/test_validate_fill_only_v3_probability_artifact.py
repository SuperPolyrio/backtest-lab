from argparse import Namespace

import pytest

from scripts import validate_fill_only_v3_probability_artifact as validation
from scripts.calibrate_fill_only_v3_l2_reference import _source_result


def _args() -> Namespace:
    return Namespace(
        quality_gate_mode="fixed",
        minimum_ratio=0.80,
        maximum_ratio=1.20,
        maximum_brier_score=0.20,
        maximum_brier_regret=0.01,
        minimum_window_orders=100,
        minimum_window_markets=5,
        minimum_stratum_orders=100,
        minimum_stratum_markets=5,
        minimum_coverage_pass_rate=0.80,
        confidence_level=0.95,
        bootstrap_replicates=500,
        bootstrap_seed=73,
        minimum_adaptive_samples=200,
        minimum_adaptive_positive_samples=20,
        minimum_adaptive_negative_samples=20,
        minimum_adaptive_clusters=20,
        minimum_group_adaptive_clusters=5,
    )


def test_gate_accepts_exact_eighty_percent_coverage() -> None:
    result = validation._gate(
        {
            "samples": 100,
            "reference_positive_orders": 50,
            "expected_positive_order_ratio": 0.80,
            "expected_quantity_ratio": 0.80,
            "brier_score": 0.20,
        },
        _args(),
    )

    assert result["status"] == "PASS"


def test_coverage_gate_accepts_overprediction_but_strict_gate_warns() -> None:
    metrics = {
        "samples": 100,
        "reference_positive_orders": 50,
        "expected_positive_order_ratio": 1.30,
        "expected_quantity_ratio": 1.25,
        "brier_score": 0.19,
    }

    assert validation._coverage_gate(metrics, _args())["status"] == "PASS"
    assert validation._gate(metrics, _args())["status"] == "FAIL"


def test_coverage_summary_accepts_four_of_five_groups() -> None:
    groups = {
        str(index): {"coverage_gate": {"status": "PASS" if index < 4 else "FAIL"}}
        for index in range(5)
    }

    summary = validation._coverage_summary(groups)

    assert summary == {"evaluated": 5, "passed": 4, "pass_rate": 0.8}


def test_adaptive_gate_can_accept_brier_above_legacy_absolute_cutoff() -> None:
    args = _args()
    args.quality_gate_mode = "adaptive"
    labels = [0.0, 1.0] * 200
    probabilities = [0.46, 0.54] * 200
    metrics = {
        "_observations": {
            "probabilities": probabilities,
            "labels": labels,
            "expected_fractions": probabilities,
            "reference_fractions": labels,
            "cluster_ids": [f"market-{index}" for index in range(400)],
        }
    }

    result = validation._gate(metrics, args)

    assert result["status"] == "PASS"
    assert result["adaptive_probability_quality"]["brier_score"] > 0.20
    assert result["legacy_fixed_thresholds_applied"] is False


def test_adaptive_gate_rejects_low_absolute_brier_that_harms_climatology() -> None:
    args = _args()
    args.quality_gate_mode = "adaptive"
    labels = [1.0] * 40 + [0.0] * 360
    probabilities = [0.35] * 400
    metrics = {
        "_observations": {
            "probabilities": probabilities,
            "labels": labels,
            "expected_fractions": probabilities,
            "reference_fractions": labels,
            "cluster_ids": [f"market-{index}" for index in range(400)],
        }
    }

    result = validation._gate(metrics, args)

    assert result["status"] == "FAIL"
    assert result["adaptive_probability_quality"]["brier_score"] < 0.20
    assert result["adaptive_probability_quality"]["brier_regret"] > 0


def test_stratum_requires_independent_markets(monkeypatch) -> None:
    rows = [
        {
            "identity": (index % 4, "asset", f"2026-08-01T00:{index:02d}:00"),
            "window": "2026-08-01T00",
            "keys": ("category|sports",),
            "dimensions": {"category": "sports"},
        }
        for index in range(100)
    ]
    monkeypatch.setattr(validation, "_evaluate_rows", lambda rows, artifact: {})

    result = validation._group_metrics(rows, {}, _args(), key="category")

    assert result["sports"]["status"] == "DATA_INSUFFICIENT"


def test_price_bucket_boundaries_match_broad_validator() -> None:
    assert validation._price_bucket("0.099") == "00_0.00_0.10"
    assert validation._price_bucket("0.10") == "01_0.10_0.25"
    assert validation._price_bucket("0.90") == "06_0.90_1.00"


def test_l2_only_strata_are_diagnostics_not_runtime_gates() -> None:
    assert "depth_regime" in validation.STRATUM_DIMENSIONS
    assert "spread_regime" in validation.STRATUM_DIMENSIONS
    assert "depth_regime" not in validation.GATED_STRATUM_DIMENSIONS
    assert "spread_regime" not in validation.GATED_STRATUM_DIMENSIONS


def test_runtime_observable_side_and_size_are_quality_gates() -> None:
    assert "side" in validation.GATED_STRATUM_DIMENSIONS
    assert "order_size_bucket" in validation.GATED_STRATUM_DIMENSIONS
    assert validation._order_size_bucket("1") == "00_LE_1"
    assert validation._order_size_bucket("1.001") == "01_GT_1_LE_5"
    assert validation._order_size_bucket("100") == "04_GT_25_LE_100"


def test_research_candidate_validation_is_explicit_opt_in() -> None:
    args = validation._parser().parse_args(
        [
            "--artifact",
            "candidate.json",
            "--orders",
            "orders.jsonl",
            "--output",
            "result.json",
            "--allow-research-candidate",
        ]
    )

    assert args.allow_research_candidate is True


def test_prepare_rows_rejects_runtime_probability_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(
        validation,
        "_load",
        lambda _paths, reference_model, key_scheme: [
            {
                "identity": (1, "asset", "2026-08-01T00:00:00Z"),
                "runtime_probability_model_version": "model-v1",
                "runtime_base_probability": "0.1",
            }
        ],
    )
    monkeypatch.setattr(validation, "_base_probability", lambda _row, _artifact: 0.9)

    with pytest.raises(RuntimeError, match="do not reproduce runtime probability"):
        validation._prepare_rows([], {"model_version": "model-v1"}, "pml2_fak")


def test_contract_expected_diagnostics_recover_source_confirmed_lower_bound() -> None:
    result = _source_result(
        {
            "v3_contract_expected": {
                "filled_size": "8.0",
                "model_diagnostics": {
                    "source_confirmed_lower_bound_size": "1.25"
                },
            }
        }
    )

    assert result == {"filled_size": "1.25"}


def test_contract_expected_route_recovers_source_or_proves_zero_source() -> None:
    source = _source_result(
        {
            "v3_contract_expected": {
                "filled_size": "0.5",
                "model_diagnostics": {"selected_route": "taker_source_confirmed"},
            }
        }
    )
    no_source = _source_result(
        {
            "v3_contract_expected": {
                "filled_size": "0",
                "model_diagnostics": {"selected_route": "taker_hierarchical_expected"},
            }
        }
    )

    assert source["filled_size"] == "0.5"
    assert no_source == {"filled_size": "0"}


def test_base_probability_artifact_uses_same_expected_fill_gates() -> None:
    metrics = validation._evaluate_rows(
        [
            {
                "identity": (1, "asset", "2026-08-01T00:00:00Z"),
                "keys": ("global",),
                "dimensions": {"category": "sports"},
                "label": 1.0,
                "reference_fraction": 1.0,
                "source_positive": False,
                "source_fraction": 0.0,
                "base_probability": 0.8,
                "conditional_fraction_model": 0.5,
            }
        ],
        {"data_contract": {"runtime_lob_usage": "NONE"}},
    )

    assert metrics["expected_positive_order_ratio"] == 0.8
    assert metrics["expected_quantity_ratio"] == 0.4


def test_base_artifact_domain_abstention_matches_runtime() -> None:
    metrics = validation._evaluate_rows(
        [
            {
                "identity": (1, "asset", "2026-08-01T00:00:00Z"),
                "keys": ("category|economy", "family|economics", "global"),
                "dimensions": {"category": "economy"},
                "label": 1.0,
                "reference_fraction": 1.0,
                "source_positive": False,
                "source_fraction": 0.0,
                "base_probability": 0.9,
                "conditional_fraction_model": 1.0,
            }
        ],
        {
            "data_contract": {"runtime_lob_usage": "NONE"},
            "domain_gate": {"abstain_category_families": ["economics"]},
        },
        supported_only=False,
    )

    assert metrics["expected_positive_orders"] == 0
    assert metrics["expected_quantity_fraction"] == 0
    assert metrics["selected_cells"] == {"domain_abstain": 1}


def test_activity_domain_abstention_is_removed_from_supported_metrics() -> None:
    row = {
        "identity": (1, "asset", "2026-08-01T00:00:00Z"),
        "keys": ("category|sports", "family|sports", "global"),
        "dimensions": {
            "category": "sports",
            "activity_regime": "ACTIVE_GT_50",
        },
        "label": 1.0,
        "reference_fraction": 1.0,
        "source_positive": False,
        "source_fraction": 0.0,
        "base_probability": 0.9,
        "conditional_fraction_model": 1.0,
    }
    artifact = {
        "domain_gate": {"abstain_activity_regimes": ["ACTIVE_GT_50"]}
    }

    assert validation._domain_supported(row, artifact) is False
    metrics = validation._evaluate_rows([row], artifact, supported_only=False)
    assert metrics["selected_cells"] == {"domain_abstain": 1}
