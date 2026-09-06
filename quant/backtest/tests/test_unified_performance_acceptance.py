from __future__ import annotations

import json
from pathlib import Path

from quant.backtest.unified_performance_acceptance import (
    PRIMARY_15_LEDGER_SHA256,
    PRIMARY_15_RESULT_SHA256,
    PRIMARY_30_LEDGER_SHA256,
    PRIMARY_30_RESULT_SHA256,
    PRIMARY_60_LEDGER_SHA256,
    PRIMARY_60_RESULT_SHA256,
    PRIMARY_173_LEDGER_SHA256,
    PRIMARY_173_RESULT_SHA256,
    V3_SOURCE_LEDGER_SHA256,
    V3_SOURCE_RESULT_SHA256,
    PerformanceAcceptanceInputs,
    build_performance_acceptance_report,
)


def _write(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _profile(
    name: str,
    *,
    results_sha256: str = "result",
    ledger_sha256: str = "ledger",
    orders: int = 372_479,
    batches: int = 173,
) -> dict[str, object]:
    return {
        "profile": name,
        "complete": True,
        "orders": orders,
        "batches": batches,
        "clickhouse_query_count": 0,
        "results_sha256": results_sha256,
        "ledger_sha256": ledger_sha256,
        "candidate_rows_scanned": 100,
        "naive_rows_scanned": 10_000,
    }


def _receipt(
    profiles: list[dict[str, object]],
    *,
    elapsed: float,
    orders: int,
    peak: int = 1_000,
) -> dict[str, object]:
    return {
        "schema_version": "UnifiedFillOnlyLongRunReceiptV1",
        "complete": True,
        "orders": orders,
        "elapsed_seconds": elapsed,
        "peak_worker_rss_kb": peak,
        "receipts": profiles,
        "os_cache_eviction": {
            "requested": True,
            "supported": True,
            "files": 2,
            "bytes": 100,
            "errors": [],
        },
    }


def test_performance_acceptance_passes_and_keeps_density_diagnostic(tmp_path) -> None:
    timings = _write(
        tmp_path / "timings.json",
        [
            {"total_seconds": 1.0, "trade_rows_indexed": 1_000_000}
            for _ in range(20)
        ],
    )
    primary_30 = _profile(
        "probabilistic_trade_tape",
        results_sha256=PRIMARY_30_RESULT_SHA256,
        ledger_sha256=PRIMARY_30_LEDGER_SHA256,
        orders=64_956,
        batches=30,
    )
    days_60 = _profile(
        "probabilistic_trade_tape",
        results_sha256=PRIMARY_60_RESULT_SHA256,
        ledger_sha256=PRIMARY_60_LEDGER_SHA256,
        orders=147_038,
        batches=60,
    )
    primary_173 = _profile(
        "probabilistic_trade_tape",
        results_sha256=PRIMARY_173_RESULT_SHA256,
        ledger_sha256=PRIMARY_173_LEDGER_SHA256,
    )
    primary_173["batch_timings_path"] = str(timings)
    names = [f"profile-{index}" for index in range(6)]
    six_profiles = [_profile(name) for name in names]
    source = _profile(
        "taker_source_confirmed",
        results_sha256=V3_SOURCE_RESULT_SHA256,
        ledger_sha256=V3_SOURCE_LEDGER_SHA256,
        orders=100,
        batches=1,
    )
    source["candidate_rows_scanned"] = 1_000
    source["naive_rows_scanned"] = 100_000

    inputs = PerformanceAcceptanceInputs(
        primary_15=_write(
            tmp_path / "15.json",
            _receipt(
                [
                    {
                        **_profile(
                            "probabilistic_trade_tape",
                            results_sha256=PRIMARY_15_RESULT_SHA256,
                            ledger_sha256=PRIMARY_15_LEDGER_SHA256,
                            orders=26_369,
                            batches=15,
                        ),
                        "matching_backends": {"rust": 15},
                    }
                ],
                elapsed=50,
                orders=26_369,
            ),
        ),
        prefix_30=_write(
            tmp_path / "prefix.json",
            _receipt([primary_30], elapsed=100, orders=64_956),
        ),
        dense_30=_write(
            tmp_path / "dense.json",
            _receipt([_profile("dense")], elapsed=150, orders=82_082),
        ),
        days_60=_write(
            tmp_path / "60.json",
            _receipt([days_60], elapsed=250, orders=147_038),
        ),
        primary_173=_write(
            tmp_path / "173.json",
            _receipt([primary_173], elapsed=1_000, orders=372_479),
        ),
        six_warm=_write(
            tmp_path / "warm.json",
            _receipt(six_profiles, elapsed=2_000, orders=372_479),
        ),
        six_cold=_write(
            tmp_path / "cold.json",
            _receipt(six_profiles, elapsed=2_500, orders=372_479),
        ),
        rust_benchmark=_write(
            tmp_path / "rust.json",
            {
                "passed": True,
                "result_equal": True,
                "ledger_equal": True,
                "median_speedup": 4.0,
            },
        ),
        v3_reference=_write(
            tmp_path / "v3.json",
            _receipt([source], elapsed=2, orders=100),
        ),
    )

    report = build_performance_acceptance_report(inputs)

    assert report["passed"] is True
    assert report["failed_gates"] == []
    assert report["diagnostics"][0]["within_reference"] is False
    assert report["diagnostics"][0]["classification"] == (
        "DENSITY_MISMATCH_DIAGNOSTIC"
    )


def test_performance_acceptance_fails_cold_cache_without_eviction(tmp_path) -> None:
    timings = _write(
        tmp_path / "timings.json",
        [
            {"total_seconds": 1.0, "trade_rows_indexed": 1_000_000}
            for _ in range(20)
        ],
    )
    primary = _profile(
        "probabilistic_trade_tape",
        results_sha256=PRIMARY_173_RESULT_SHA256,
        ledger_sha256=PRIMARY_173_LEDGER_SHA256,
    )
    primary["batch_timings_path"] = str(timings)
    prefix = _profile(
        "probabilistic_trade_tape",
        results_sha256=PRIMARY_30_RESULT_SHA256,
        ledger_sha256=PRIMARY_30_LEDGER_SHA256,
        orders=64_956,
        batches=30,
    )
    days_60 = _profile(
        "probabilistic_trade_tape",
        results_sha256=PRIMARY_60_RESULT_SHA256,
        ledger_sha256=PRIMARY_60_LEDGER_SHA256,
        orders=147_038,
        batches=60,
    )
    six = [_profile(f"profile-{index}") for index in range(6)]
    cold = _receipt(six, elapsed=2_000, orders=372_479)
    cold["os_cache_eviction"] = {
        "requested": False,
        "supported": True,
        "files": 0,
        "bytes": 0,
        "errors": [],
    }
    source = _profile(
        "taker_source_confirmed",
        results_sha256=V3_SOURCE_RESULT_SHA256,
        ledger_sha256=V3_SOURCE_LEDGER_SHA256,
        orders=100,
        batches=1,
    )
    paths = PerformanceAcceptanceInputs(
        primary_15=_write(
            tmp_path / "15.json",
            _receipt(
                [
                    {
                        **_profile(
                            "probabilistic_trade_tape",
                            results_sha256=PRIMARY_15_RESULT_SHA256,
                            ledger_sha256=PRIMARY_15_LEDGER_SHA256,
                            orders=26_369,
                            batches=15,
                        ),
                        "matching_backends": {"rust": 15},
                    }
                ],
                elapsed=50,
                orders=26_369,
            ),
        ),
        prefix_30=_write(
            tmp_path / "prefix.json",
            _receipt([prefix], elapsed=100, orders=64_956),
        ),
        dense_30=_write(
            tmp_path / "dense.json",
            _receipt([_profile("dense")], elapsed=150, orders=82_082),
        ),
        days_60=_write(
            tmp_path / "60.json",
            _receipt([days_60], elapsed=250, orders=147_038),
        ),
        primary_173=_write(
            tmp_path / "173.json",
            _receipt([primary], elapsed=1_000, orders=372_479),
        ),
        six_warm=_write(
            tmp_path / "warm.json",
            _receipt(six, elapsed=2_000, orders=372_479),
        ),
        six_cold=_write(tmp_path / "cold.json", cold),
        rust_benchmark=_write(
            tmp_path / "rust.json",
            {
                "passed": True,
                "result_equal": True,
                "ledger_equal": True,
                "median_speedup": 4.0,
            },
        ),
        v3_reference=_write(
            tmp_path / "v3.json",
            _receipt([source], elapsed=2, orders=100),
        ),
    )

    report = build_performance_acceptance_report(paths)

    assert report["passed"] is False
    assert "cold_cache_eviction_proven" in report["failed_gates"]
