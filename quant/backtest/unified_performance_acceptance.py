"""Machine-readable acceptance gates for unified Fill-only replay benchmarks."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PRIMARY_30_RESULT_SHA256 = (
    "b52c52b7781fa23a04cc8fab2787145684e0af595ede03f192979aa99103095b"
)
PRIMARY_15_RESULT_SHA256 = (
    "973698306ad83e43dca932dc65587807d169688c6bb0e80033fd1a02cdfcd728"
)
PRIMARY_15_LEDGER_SHA256 = (
    "40ce7e983d9dcfd993e2c419f543aede81db7f9c7cd94256a13e2a3b08b6ba74"
)
PRIMARY_30_LEDGER_SHA256 = (
    "2b88fd01cce7505ec9ff7baa41d862ca7fcca84997d998a708eb2661a6f366b7"
)
PRIMARY_60_RESULT_SHA256 = (
    "eb557e46a8aaafcced3d0df0e9c9422719717ec619bed35b29ed768121f33ff5"
)
PRIMARY_60_LEDGER_SHA256 = (
    "f37452e93efcde3cd8a2d64a8f3a3dbaa484fe5e1b53ac62fad28d5b569f0805"
)
PRIMARY_173_RESULT_SHA256 = (
    "07f3139926820340038c4d693196bbe461188bd26514a565d7276d9d92d34aae"
)
PRIMARY_173_LEDGER_SHA256 = (
    "24e369e0b98a64af5e0cc3b22e895428660cecbb7651c1294841e4afc579f242"
)
V3_SOURCE_RESULT_SHA256 = (
    "ad0ebe2b7956045c307c1e6d0f262b94d4bcb4cf10acdff43760fbdc14332263"
)
V3_SOURCE_LEDGER_SHA256 = (
    "2901b6295f7fb4b9b898281937e3f54e7e99061489abaf7a2a3866016a63ea58"
)


@dataclass(frozen=True)
class PerformanceAcceptanceInputs:
    primary_15: Path
    prefix_30: Path
    dense_30: Path
    days_60: Path
    primary_173: Path
    six_warm: Path
    six_cold: Path
    rust_benchmark: Path
    v3_reference: Path


def build_performance_acceptance_report(
    inputs: PerformanceAcceptanceInputs,
) -> dict[str, Any]:
    """Validate frozen benchmark artifacts without rerunning a backtest."""

    primary_15 = _load_receipt(inputs.primary_15)
    prefix_30 = _load_receipt(inputs.prefix_30)
    dense_30 = _load_receipt(inputs.dense_30)
    days_60 = _load_receipt(inputs.days_60)
    primary_173 = _load_receipt(inputs.primary_173)
    six_warm = _load_receipt(inputs.six_warm)
    six_cold = _load_receipt(inputs.six_cold)
    rust = _load_json(inputs.rust_benchmark)
    v3 = _load_receipt(inputs.v3_reference)

    gates: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []

    for label, receipt in (
        ("primary_15", primary_15),
        ("prefix_30", prefix_30),
        ("dense_30", dense_30),
        ("days_60", days_60),
        ("primary_173", primary_173),
        ("six_warm", six_warm),
        ("six_cold", six_cold),
        ("v3_reference", v3),
    ):
        _add_gate(
            gates,
            f"{label}_complete",
            bool(receipt.get("complete")),
            actual=receipt.get("complete"),
            expected=True,
        )
        query_count = sum(
            int(row.get("clickhouse_query_count", 0))
            for row in receipt.get("receipts", [])
        )
        _add_gate(
            gates,
            f"{label}_no_clickhouse",
            query_count == 0,
            actual=query_count,
            expected=0,
        )

    _check_primary_hashes(
        gates,
        "primary_15",
        primary_15,
        PRIMARY_15_RESULT_SHA256,
        PRIMARY_15_LEDGER_SHA256,
    )
    _check_primary_hashes(
        gates,
        "prefix_30",
        prefix_30,
        PRIMARY_30_RESULT_SHA256,
        PRIMARY_30_LEDGER_SHA256,
    )
    _check_primary_hashes(
        gates,
        "days_60",
        days_60,
        PRIMARY_60_RESULT_SHA256,
        PRIMARY_60_LEDGER_SHA256,
    )
    _check_primary_hashes(
        gates,
        "primary_173",
        primary_173,
        PRIMARY_173_RESULT_SHA256,
        PRIMARY_173_LEDGER_SHA256,
    )

    prefix_ratio = _elapsed(days_60) / _elapsed(prefix_30)
    workload_ratio = _elapsed(days_60) / _elapsed(dense_30)
    diagnostics.append(
        {
            "name": "prefix_t60_over_t30",
            "classification": "DENSITY_MISMATCH_DIAGNOSTIC",
            "actual": prefix_ratio,
            "reference": 2.2,
            "within_reference": prefix_ratio <= 2.2,
            "orders_ratio": int(days_60["orders"]) / int(prefix_30["orders"]),
            "note": (
                "The first 60 days contain materially more trade rows than the "
                "first 30 days; this diagnostic is not the workload-controlled gate."
            ),
        }
    )
    _add_gate(
        gates,
        "workload_controlled_t60_over_t30",
        workload_ratio <= 2.2,
        actual=workload_ratio,
        maximum=2.2,
    )

    normalized = _normalized_tail_ratio(primary_173)
    _add_gate(
        gates,
        "normalized_last10_over_first10",
        normalized <= 1.10,
        actual=normalized,
        maximum=1.10,
    )
    peak_rss = max(
        int(primary_173.get("peak_worker_rss_kb", 0)),
        int(six_warm.get("peak_worker_rss_kb", 0)),
        int(six_cold.get("peak_worker_rss_kb", 0)),
    )
    _add_gate(
        gates,
        "peak_worker_rss_under_6_gib",
        peak_rss < 6 * 1024 * 1024,
        actual=peak_rss,
        maximum=6 * 1024 * 1024,
        unit="KiB",
    )
    for name, receipt, maximum in (
        ("primary_173_under_90_minutes", primary_173, 90 * 60),
        ("six_warm_under_3_hours", six_warm, 3 * 60 * 60),
        ("six_cold_under_4_hours", six_cold, 4 * 60 * 60),
    ):
        elapsed = _elapsed(receipt)
        _add_gate(
            gates,
            name,
            elapsed < maximum,
            actual=elapsed,
            maximum=maximum,
            unit="seconds",
        )

    eviction = six_cold.get("os_cache_eviction", {})
    eviction_ok = (
        eviction.get("requested") is True
        and eviction.get("supported") is True
        and int(eviction.get("files", 0)) > 0
        and int(eviction.get("bytes", 0)) > 0
        and not eviction.get("errors")
    )
    _add_gate(
        gates,
        "cold_cache_eviction_proven",
        eviction_ok,
        actual=eviction,
        expected="requested, supported, non-empty, error-free",
    )

    _check_six_profile_contract(gates, six_warm, "six_warm")
    _check_six_profile_contract(gates, six_cold, "six_cold")
    warm_hashes = _profile_hashes(six_warm)
    cold_hashes = _profile_hashes(six_cold)
    _add_gate(
        gates,
        "warm_cold_profile_hashes_equal",
        warm_hashes == cold_hashes,
        actual=cold_hashes,
        expected=warm_hashes,
    )
    primary_15_profile = _only_profile(primary_15)
    _add_gate(
        gates,
        "primary_15_full_inventory",
        int(primary_15.get("orders", 0)) == 26_369
        and int(primary_15_profile.get("batches", 0)) == 15
        and primary_15_profile.get("matching_backends") == {"rust": 15},
        actual={
            "orders": primary_15.get("orders"),
            "batches": primary_15_profile.get("batches"),
            "matching_backends": primary_15_profile.get("matching_backends"),
        },
        expected={"orders": 26_369, "batches": 15, "matching_backends": {"rust": 15}},
    )

    speedup = float(rust.get("median_speedup", 0.0))
    _add_gate(
        gates,
        "rust_python_exact",
        bool(rust.get("passed"))
        and bool(rust.get("result_equal"))
        and bool(rust.get("ledger_equal")),
        actual={
            "passed": rust.get("passed"),
            "result_equal": rust.get("result_equal"),
            "ledger_equal": rust.get("ledger_equal"),
        },
        expected=True,
    )
    _add_gate(
        gates,
        "rust_median_speedup",
        speedup >= 3.0,
        actual=speedup,
        minimum=3.0,
    )

    v3_source = _profile(v3, "taker_source_confirmed")
    reduction = 1.0 - (
        int(v3_source["candidate_rows_scanned"])
        / int(v3_source["naive_rows_scanned"])
    )
    _add_gate(
        gates,
        "v3_source_scan_reduction",
        reduction >= 0.90,
        actual=reduction,
        minimum=0.90,
    )
    _add_gate(
        gates,
        "v3_source_result_hash",
        v3_source.get("results_sha256") == V3_SOURCE_RESULT_SHA256,
        actual=v3_source.get("results_sha256"),
        expected=V3_SOURCE_RESULT_SHA256,
    )
    _add_gate(
        gates,
        "v3_source_ledger_hash",
        v3_source.get("ledger_sha256") == V3_SOURCE_LEDGER_SHA256,
        actual=v3_source.get("ledger_sha256"),
        expected=V3_SOURCE_LEDGER_SHA256,
    )

    failed = [row["name"] for row in gates if not row["passed"]]
    return {
        "schema_version": "UnifiedFillOnlyPerformanceAcceptanceV1",
        "passed": not failed,
        "failed_gates": failed,
        "gates": gates,
        "diagnostics": diagnostics,
        "artifacts": {
            key: str(value.resolve())
            for key, value in vars(inputs).items()
        },
    }


def _load_receipt(path: Path) -> dict[str, Any]:
    payload = _load_json(path)
    if payload.get("schema_version") != "UnifiedFillOnlyLongRunReceiptV1":
        raise ValueError(f"unsupported replay receipt: {path}")
    return payload


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"JSON artifact must contain an object: {path}")
    return payload


def _elapsed(receipt: dict[str, Any]) -> float:
    elapsed = float(receipt.get("elapsed_seconds", 0.0))
    if elapsed <= 0:
        raise ValueError("benchmark elapsed_seconds must be positive")
    return elapsed


def _profile(receipt: dict[str, Any], name: str) -> dict[str, Any]:
    matches = [row for row in receipt.get("receipts", []) if row.get("profile") == name]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one {name!r} profile receipt")
    return matches[0]


def _only_profile(receipt: dict[str, Any]) -> dict[str, Any]:
    rows = receipt.get("receipts", [])
    if len(rows) != 1:
        raise ValueError("expected a single-profile benchmark receipt")
    return rows[0]


def _check_primary_hashes(
    gates: list[dict[str, Any]],
    label: str,
    receipt: dict[str, Any],
    result_sha256: str,
    ledger_sha256: str,
) -> None:
    profile = _only_profile(receipt)
    for suffix, key, expected in (
        ("result_hash", "results_sha256", result_sha256),
        ("ledger_hash", "ledger_sha256", ledger_sha256),
    ):
        _add_gate(
            gates,
            f"{label}_{suffix}",
            profile.get(key) == expected,
            actual=profile.get(key),
            expected=expected,
        )


def _normalized_tail_ratio(receipt: dict[str, Any]) -> float:
    profile = _only_profile(receipt)
    path = Path(str(profile["batch_timings_path"]))
    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or len(rows) < 20:
        raise ValueError("173-day batch timing artifact must contain at least 20 rows")

    def seconds_per_million(chunk: list[dict[str, Any]]) -> float:
        elapsed = sum(float(row["total_seconds"]) for row in chunk)
        trades = sum(int(row["trade_rows_indexed"]) for row in chunk)
        if trades <= 0:
            raise ValueError("batch timing trade_rows_indexed must be positive")
        return elapsed * 1_000_000 / trades

    return seconds_per_million(rows[-10:]) / seconds_per_million(rows[:10])


def _check_six_profile_contract(
    gates: list[dict[str, Any]], receipt: dict[str, Any], label: str
) -> None:
    profiles = receipt.get("receipts", [])
    valid = (
        len(profiles) == 6
        and int(receipt.get("orders", 0)) == 372_479
        and all(
            row.get("complete") is True
            and int(row.get("orders", 0)) == 372_479
            and int(row.get("batches", 0)) == 173
            for row in profiles
        )
    )
    _add_gate(
        gates,
        f"{label}_full_inventory",
        valid,
        actual={
            "profiles": len(profiles),
            "orders": receipt.get("orders"),
            "batches": sorted({row.get("batches") for row in profiles}),
        },
        expected={"profiles": 6, "orders": 372_479, "batches": [173]},
    )


def _profile_hashes(receipt: dict[str, Any]) -> dict[str, tuple[str, str]]:
    return {
        str(row["profile"]): (
            str(row["results_sha256"]),
            str(row["ledger_sha256"]),
        )
        for row in receipt.get("receipts", [])
    }


def _add_gate(
    gates: list[dict[str, Any]],
    name: str,
    passed: bool,
    *,
    actual: Any,
    **contract: Any,
) -> None:
    gates.append(
        {
            "name": name,
            "passed": bool(passed),
            "actual": actual,
            **contract,
        }
    )
