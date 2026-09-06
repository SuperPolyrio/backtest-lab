from argparse import Namespace
from datetime import UTC, datetime
from pathlib import Path

from scripts.run_fill_only_v3_large_cross_validation import (
    OBSERVABLE_PERFORMANCE_DIMENSIONS,
    REQUIRED_STRATA,
    _batch_command,
    _breadth_quality,
    _normalize_attempt_statuses,
    _order_size_bucket,
    _registered_market_ids,
    _result_receipt,
    _utc_session,
)
from scripts.validate_fill_only_v3_cross_window_stability import (
    _brier_regret,
    _passes_brier,
    combine_quality_statuses,
)


def _args() -> Namespace:
    return Namespace(
        minimum_stratum_orders=100,
        minimum_stratum_markets=5,
        minimum_dates=3,
        minimum_unique_markets=100,
        minimum_market_days=150,
        minimum_category_strata=5,
        minimum_support_rate=0.80,
        minimum_stratum_ratio=0.80,
        maximum_stratum_ratio=1.20,
        minimum_coverage_pass_rate=0.80,
        references=("pml2_fak", "nautilus_fak"),
    )


def _broad() -> dict[str, object]:
    counts = {
        dimension: {name: 100 for name in names}
        for dimension, names in REQUIRED_STRATA.items()
    }
    counts["category"] = {f"category_{index}": 100 for index in range(6)}
    counts["side"] = {"BUY": 300, "SELL": 300}
    counts["order_size_bucket"] = {
        "00_LE_1": 120,
        "01_GT_1_LE_5": 120,
        "02_GT_5_LE_10": 120,
        "03_GT_10_LE_25": 120,
        "04_GT_25_LE_100": 120,
    }
    counts["utc_session"] = {"00_04": 100}
    broad = {
        "dates": 3,
        "unique_markets": 100,
        "market_days": 150,
        "dimension_counts": counts,
        "strata": {
            dimension: {
                name: {
                    "orders": orders,
                    "unique_markets": 5,
                    "comparisons": None,
                }
                for name, orders in values.items()
            }
            for dimension, values in counts.items()
        },
    }
    metrics = {
        "model_support_rate": 1.0,
        "reference_positive_orders": 80,
        "all_sample_expected_positive_order_ratio": 1.0,
        "all_sample_expected_quantity_ratio": 1.0,
    }
    for dimension in OBSERVABLE_PERFORMANCE_DIMENSIONS:
        for item in broad["strata"][dimension].values():
            item["comparisons"] = {
                "pml2_fak": dict(metrics),
                "nautilus_fak": dict(metrics),
            }
    return broad


def test_breadth_quality_accepts_genuinely_broad_sample() -> None:
    result = _breadth_quality(_broad(), _args())

    assert result["status"] == "PASS"
    assert result["failures"] == []


def test_order_size_bucket_boundaries() -> None:
    assert _order_size_bucket("1") == "00_LE_1"
    assert _order_size_bucket("1.0001") == "01_GT_1_LE_5"
    assert _order_size_bucket("5") == "01_GT_1_LE_5"
    assert _order_size_bucket("10") == "02_GT_5_LE_10"
    assert _order_size_bucket("25") == "03_GT_10_LE_25"
    assert _order_size_bucket("100") == "04_GT_25_LE_100"
    assert _order_size_bucket("101") == "05_GT_100"


def test_utc_session_matches_runtime_hierarchy_bucket() -> None:
    assert _utc_session(datetime(2026, 8, 1, 0, tzinfo=UTC)) == "00_04"
    assert _utc_session(datetime(2026, 8, 1, 3, 59, tzinfo=UTC)) == "00_04"
    assert _utc_session(datetime(2026, 8, 1, 4, tzinfo=UTC)) == "04_08"
    assert _utc_session(datetime(2026, 8, 1, 23, 59, tzinfo=UTC)) == "20_24"


def test_result_receipt_keeps_full_result_on_disk_and_terminal_summary_small(
    tmp_path: Path,
) -> None:
    result = {
        "status": "FAIL",
        "successful_orders": 10_500,
        "successful_windows": 5,
        "failed_or_unavailable_windows": 1,
        "quality": {
            "status": "PASS",
            "aggregate": {
                "pml2_fak": {
                    "support_rate": 0.95,
                    "expected_positive_order_ratio": 1.01,
                    "expected_quantity_ratio": 0.99,
                    "all_sample_expected_positive_order_ratio": 1.01,
                    "all_sample_expected_quantity_ratio": 0.99,
                    "adaptive_probability_quality": {
                        "status": "PASS",
                        "brier_score": 0.18,
                        "reference_brier_score": 0.22,
                        "brier_regret": -0.04,
                        "reliability_bins": list(range(1_000)),
                    },
                    "large_unused_detail": list(range(1_000)),
                }
            },
        },
        "breadth_quality": {
            "status": "FAIL",
            "failures": ["price_bucket:pml2_fak:no_calibration_failure"],
            "calibration_warnings": ["price_bucket:04_0.75_0.90:pml2_fak"],
        },
    }

    receipt = _result_receipt(result, tmp_path)

    assert receipt["result_path"] == str(tmp_path / "result.json")
    assert receipt["comparisons"]["pml2_fak"]["expected_order_ratio"] == 1.01
    assert receipt["comparisons"]["pml2_fak"]["support_rate"] == 0.95
    assert receipt["comparisons"]["pml2_fak"]["status"] == "PASS"
    assert "large_unused_detail" not in receipt["comparisons"]["pml2_fak"]
    assert "reliability_bins" not in receipt["comparisons"]["pml2_fak"][
        "adaptive_probability_quality"
    ]


def test_adaptive_breadth_does_not_count_insufficient_stratum_as_pass() -> None:
    broad = _broad()
    args = _args()
    args.quality_gate_mode = "adaptive"
    for dimension in OBSERVABLE_PERFORMANCE_DIMENSIONS:
        for item in broad["strata"][dimension].values():
            for metrics in item["comparisons"].values():
                metrics["adaptive_probability_quality"] = {
                    "status": "INCONCLUSIVE"
                }
    broad["strata"]["side"]["BUY"]["comparisons"]["pml2_fak"][
        "adaptive_probability_quality"
    ] = {"status": "INSUFFICIENT_SAMPLE"}

    result = _breadth_quality(broad, args)

    assert (
        result["observable_strata"]["side"]["BUY"]["pml2_fak"]["passed"]
        is None
    )


def test_breadth_quality_rejects_repeated_orders_from_too_few_markets() -> None:
    broad = _broad()
    broad["unique_markets"] = 20

    result = _breadth_quality(broad, _args())

    assert result["status"] == "FAIL"
    assert result["failures"] == ["minimum_unique_markets"]


def test_breadth_quality_rejects_missing_liquidity_regime() -> None:
    broad = _broad()
    del broad["dimension_counts"]["activity_regime"]["ACTIVE_GT_50"]

    result = _breadth_quality(broad, _args())

    assert result["status"] == "FAIL"
    assert result["failures"] == ["activity_regime_coverage"]


def test_breadth_quality_allows_one_warning_when_eighty_percent_pass() -> None:
    broad = _broad()
    broad["strata"]["price_bucket"]["00_0.00_0.10"]["comparisons"]["pml2_fak"][
        "all_sample_expected_quantity_ratio"
    ] = 0.79

    result = _breadth_quality(broad, _args())

    assert result["status"] == "PASS"
    assert result["failures"] == []
    assert result["calibration_warnings"] == ["price_bucket:00_0.00_0.10:pml2_fak"]


def test_breadth_quality_rejects_when_less_than_eighty_percent_pass() -> None:
    broad = _broad()
    for name in ("00_0.00_0.10", "01_0.10_0.25"):
        broad["strata"]["price_bucket"][name]["comparisons"]["pml2_fak"][
            "all_sample_expected_quantity_ratio"
        ] = 0.79

    result = _breadth_quality(broad, _args())

    assert result["status"] == "FAIL"
    assert result["failures"] == ["price_bucket:pml2_fak:coverage_pass_rate"]


def test_breadth_quality_does_not_treat_repeated_orders_as_independent_stratum() -> (
    None
):
    broad = _broad()
    broad["strata"]["category"]["category_0"]["unique_markets"] = 2
    broad["strata"]["category"]["category_0"]["comparisons"] = None

    result = _breadth_quality(broad, _args())

    assert result["status"] == "PASS"
    assert result["eligible_category_strata"] == 5
    assert "category:category_0:pml2_fak" not in result["checks"]


def test_breadth_quality_does_not_tune_to_hidden_l2_strata() -> None:
    broad = _broad()
    broad["strata"]["depth_regime"]["DEEP_GE_10X"]["comparisons"] = {
        "pml2_fak": {
            "model_support_rate": 0.0,
            "reference_positive_orders": 100,
            "all_sample_expected_positive_order_ratio": 0.0,
            "all_sample_expected_quantity_ratio": 0.0,
        }
    }

    result = _breadth_quality(broad, _args())

    assert result["status"] == "PASS"


def test_breadth_quality_allows_explicit_category_abstention_but_counts_it() -> None:
    broad = _broad()
    for metrics in broad["strata"]["category"]["category_0"]["comparisons"].values():
        metrics.update(
            model_support_rate=0.0,
            reference_ready_samples=100,
            excluded={"MODEL_OUT_OF_DOMAIN": 100},
        )

    result = _breadth_quality(broad, _args())

    assert result["status"] == "PASS"
    assert result["eligible_category_strata"] == 6
    assert result["supported_category_strata"] == 5
    assert (
        result["observable_strata"]["category"]["category_0"]["pml2_fak"]["status"]
        == "MODEL_DOMAIN_UNSUPPORTED"
    )


def test_brier_requires_absolute_score_and_regret() -> None:
    brier = 0.22642965596744272
    positive_rate = 0.6518181818181819

    assert brier > 0.20
    assert _brier_regret(brier, positive_rate) < 0.01
    assert not _passes_brier(
        brier,
        positive_rate,
        Namespace(maximum_brier_score=0.20, maximum_brier_regret=0.01),
    )


def test_quality_status_preserves_inconclusive_and_insufficient_sample() -> None:
    assert combine_quality_statuses("PASS", "INCONCLUSIVE") == "INCONCLUSIVE"
    assert (
        combine_quality_statuses("PASS", "INSUFFICIENT_SAMPLE") == "INSUFFICIENT_SAMPLE"
    )
    assert combine_quality_statuses("INCONCLUSIVE", "FAIL") == "FAIL"


def test_never_reuse_policy_excludes_markets_from_non_overlapping_windows(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "registry.jsonl"
    registry.write_text(
        '{"start":"2026-08-01T00:00:00+00:00",'
        '"end":"2026-08-01T00:15:00+00:00",'
        '"market_asset_pairs":[{"market_id":7,"asset_id":"a"}]}\n',
        encoding="utf-8",
    )
    later_start = datetime(2026, 8, 2, tzinfo=UTC)
    later_end = datetime(2026, 8, 2, 0, 15, tzinfo=UTC)

    assert _registered_market_ids(
        registry,
        start=later_start,
        end=later_end,
        exclude_all_registered=True,
    ) == (7,)
    assert (
        _registered_market_ids(
            registry,
            start=later_start,
            end=later_end,
            exclude_all_registered=False,
        )
        == ()
    )


def test_insufficient_market_attempt_is_not_reported_as_engine_failure() -> None:
    manifest = {
        "attempts": [
            {
                "status": "FAILED",
                "reason": "BATCH_COMPARISON_FAILED",
                "error_tail": (
                    "requested 25 markets but only 13 produced 44 valid orders"
                ),
            }
        ]
    }

    _normalize_attempt_statuses(manifest)

    assert manifest["attempts"][0]["status"] == "DATA_INSUFFICIENT"
    assert manifest["attempts"][0]["reason"] == "INSUFFICIENT_ELIGIBLE_MARKETS"


def test_batch_command_can_use_local_archive_without_remote_slice_cache(
    tmp_path: Path,
) -> None:
    args = Namespace(
        python=Path("python"),
        markets_per_window=5,
        orders_per_market=10,
        candidate_multiplier=2,
        models="v3_l2_expected,pml2_fak,nautilus_fak",
        archive=tmp_path / "archive",
        archive_baseline_lookback_hours=0,
        archive_shard_count=2,
        l2_source="polymarket_market_ws_archive",
        window_registry=tmp_path / "registry.jsonl",
        nautilus_python=Path("nautilus-python"),
        v3_probability_artifact=None,
        v3_fak_probability_artifact=None,
        v3_fok_probability_artifact=None,
        order_sizes=(10,),
        order_sides=("BUY", "SELL"),
        order_tif="FOK",
        cohort_split="validation",
        no_l2_slice_cache=True,
    )

    command = _batch_command(
        args,
        start=datetime(2026, 7, 24, tzinfo=UTC),
        end=datetime(2026, 7, 24, 1, tzinfo=UTC),
        output_dir=tmp_path / "output",
    )

    assert "--no-l2-slice-cache" in command
    assert command[command.index("--order-sizes") + 1] == "10"
    assert command[command.index("--order-sides") + 1] == "BUY,SELL"
    assert command[command.index("--order-tif") + 1] == "FOK"
    assert command[command.index("--cohort-split") + 1] == "validation"
    assert command[command.index("--archive-shard-count") + 1] == "2"
