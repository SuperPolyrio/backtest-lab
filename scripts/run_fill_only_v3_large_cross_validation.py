#!/usr/bin/env python3
"""Run resumable, broad Fill-only V3 comparisons on more than 10,000 orders."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import batch_compare_real_execution_models as comparison  # noqa: E402
from scripts import (  # noqa: E402
    validate_fill_only_v3_cross_window_stability as stability,
)

DEFAULT_ARCHIVE = Path("/data/jiahuaiyu/prediction-market-quant/lob_l2_archive_xue")
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "backtest_framework"
    / "nautilus_trader_comparison"
    / "fill_only_v3_large_cross_validation"
)
DEFAULT_MODELS = "v3_source_fak,v3_l2_reference,v3_l2_expected,pml2_fak,nautilus_fak"
DEFAULT_MODEL = "v3_l2_expected"
DEFAULT_REFERENCES = ("pml2_fak", "nautilus_fak")
REQUIRED_STRATA = {
    "price_bucket": {
        "00_0.00_0.10",
        "01_0.10_0.25",
        "02_0.25_0.40",
        "03_0.40_0.60",
        "04_0.60_0.75",
        "05_0.75_0.90",
        "06_0.90_1.00",
    },
    "depth_regime": {"DEEP_GE_10X", "MEDIUM_1X_TO_10X", "SHALLOW_LT_1X"},
    "activity_regime": {"ACTIVE_GT_50", "MEDIUM_6_TO_50", "SPARSE_LE_5"},
    "spread_regime": {
        "NORMAL_0_002_TO_0_02",
        "TIGHT_LE_0_002",
        "WIDE_GT_0_02",
    },
}
OBSERVABLE_PERFORMANCE_DIMENSIONS = (
    "category",
    "price_bucket",
    "activity_regime",
    "side",
    "order_size_bucket",
    "utc_session",
)


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date, Decimal, Path)):
        return str(value)
    raise TypeError(f"cannot encode {type(value).__name__}")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"expected object in {path}")
    return value


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_hours(value: str) -> tuple[int, ...]:
    hours = tuple(int(item.strip()) for item in value.split(",") if item.strip())
    if not hours or any(hour < 0 or hour > 23 for hour in hours):
        raise argparse.ArgumentTypeError("hours must be comma-separated integers 0..23")
    return hours


def _parse_window_starts(value: str) -> tuple[datetime, ...]:
    starts: list[datetime] = []
    for item in value.split(","):
        text = item.strip()
        if not text:
            continue
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        starts.append(parsed.astimezone(UTC))
    return tuple(starts)


def _window(
    start_day: date, index: int, hours: tuple[int, ...]
) -> tuple[datetime, datetime]:
    day = start_day + timedelta(days=index)
    start = datetime.combine(day, time(hour=hours[index % len(hours)]), tzinfo=UTC)
    return start, start + timedelta(hours=1) - timedelta(seconds=1)


def _window_key(start: datetime, end: datetime) -> str:
    return f"{start.isoformat()}__{end.isoformat()}"


def _window_dir(root: Path, start: datetime) -> Path:
    return root / f"window_{start:%Y%m%d_%H%M%S}"


def _archive_window_exists(archive: Path, start: datetime) -> bool:
    return (archive / f"dt={start:%Y-%m-%d}" / f"hour={start:%H}").is_dir()


def _registered_market_ids(
    registry: Path,
    *,
    start: datetime,
    end: datetime,
    exclude_all_registered: bool = False,
) -> tuple[int, ...]:
    if not registry.exists():
        return ()
    result: set[int] = set()
    with registry.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if not exclude_all_registered:
                previous_start = datetime.fromisoformat(str(row["start"]))
                previous_end = datetime.fromisoformat(str(row["end"]))
                if start >= previous_end or previous_start >= end:
                    continue
            result.update(
                int(item["market_id"]) for item in row.get("market_asset_pairs", [])
            )
    return tuple(sorted(result))


def _tail(path: Path, lines: int = 40) -> str:
    if not path.exists():
        return ""
    values = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(values[-lines:])


def _normalize_attempt_statuses(manifest: dict[str, Any]) -> None:
    for attempt in manifest.get("attempts", []):
        error_tail = str(attempt.get("error_tail") or "")
        if (
            attempt.get("status") == "FAILED"
            and "requested " in error_tail
            and " markets but only " in error_tail
            and " produced " in error_tail
        ):
            attempt["status"] = "DATA_INSUFFICIENT"
            attempt["reason"] = "INSUFFICIENT_ELIGIBLE_MARKETS"
        elif attempt.get("status") == "FAILED" and any(
            marker in error_tail
            for marker in (
                "remote L2 slice materialization failed",
                "Connection timed out",
                "No route to host",
                "Transport endpoint is not connected",
            )
        ):
            attempt["status"] = "DATA_NOT_AVAILABLE"
            attempt["reason"] = "L2_ARCHIVE_REMOTE_UNAVAILABLE"


def _batch_command(
    args: argparse.Namespace,
    *,
    start: datetime,
    end: datetime,
    output_dir: Path,
) -> list[str]:
    command = [
        str(args.python),
        str(PROJECT_ROOT / "scripts" / "batch_compare_real_execution_models.py"),
        "--start",
        start.isoformat(),
        "--end",
        end.isoformat(),
        "--market-limit",
        str(args.markets_per_window),
        "--orders-per-market",
        str(args.orders_per_market),
        "--order-sizes",
        ",".join(str(value) for value in args.order_sizes),
        "--order-sides",
        ",".join(args.order_sides),
        "--order-tif",
        args.order_tif,
        "--candidate-multiplier",
        str(args.candidate_multiplier),
        "--models",
        args.models,
        "--archive",
        str(args.archive),
        "--archive-baseline-lookback-hours",
        str(args.archive_baseline_lookback_hours),
        "--archive-shard-count",
        str(args.archive_shard_count),
        "--l2-source",
        args.l2_source,
        "--output-dir",
        str(output_dir),
        "--cohort-registry",
        str(args.window_registry),
        "--cohort-split",
        args.cohort_split,
        "--nautilus-python",
        str(args.nautilus_python),
        "--pml2-backend",
        getattr(args, "pml2_backend", "auto"),
    ]
    excluded = _registered_market_ids(
        args.window_registry,
        start=start,
        end=end,
        exclude_all_registered=(
            str(getattr(args, "market_reuse_policy", "overlap_only")) == "never"
        ),
    )
    if excluded:
        output_dir.mkdir(parents=True, exist_ok=True)
        exclude_path = output_dir / "excluded_market_ids.txt"
        exclude_path.write_text(
            "\n".join(str(value) for value in excluded) + "\n", encoding="utf-8"
        )
        command.extend(["--exclude-market-ids-file", str(exclude_path)])
    if args.v3_probability_artifact is not None:
        command.extend(
            [
                "--v3-probability-artifact",
                str(args.v3_probability_artifact),
            ]
        )
    if args.v3_fak_probability_artifact is not None:
        command.extend(
            [
                "--v3-fak-probability-artifact",
                str(args.v3_fak_probability_artifact),
            ]
        )
    if args.v3_fok_probability_artifact is not None:
        command.extend(
            [
                "--v3-fok-probability-artifact",
                str(args.v3_fok_probability_artifact),
            ]
        )
    if args.no_l2_slice_cache:
        command.append("--no-l2-slice-cache")
    return command


def _project_model(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value.get(key)
        for key in (
            "status",
            "reason",
            "filled_size",
            "avg_price",
            "evidence_tier",
            "probability_bounds",
        )
    }


def _price_bucket(value: Any) -> str:
    price = Decimal(str(value))
    boundaries = (
        (Decimal("0.10"), "00_0.00_0.10"),
        (Decimal("0.25"), "01_0.10_0.25"),
        (Decimal("0.40"), "02_0.25_0.40"),
        (Decimal("0.60"), "03_0.40_0.60"),
        (Decimal("0.75"), "04_0.60_0.75"),
        (Decimal("0.90"), "05_0.75_0.90"),
        (Decimal("1.01"), "06_0.90_1.00"),
    )
    return next(label for upper, label in boundaries if price < upper)


def _order_size_bucket(value: Any) -> str:
    size = Decimal(str(value))
    boundaries = (
        (Decimal("1"), "00_LE_1"),
        (Decimal("5"), "01_GT_1_LE_5"),
        (Decimal("10"), "02_GT_5_LE_10"),
        (Decimal("25"), "03_GT_10_LE_25"),
        (Decimal("100"), "04_GT_25_LE_100"),
    )
    return next((label for upper, label in boundaries if size <= upper), "05_GT_100")


def _utc_session(value: datetime) -> str:
    lower = (value.hour // 4) * 4
    return f"{lower:02d}_{lower + 4:02d}"


def _compact_row(
    row: Mapping[str, Any], *, model: str, references: tuple[str, ...]
) -> dict[str, Any]:
    models = row["models"]
    return {
        "market_id": int(row["market_id"]),
        "order_size": str(row["size"]),
        "cluster_id": f"{row['market_id']}:{str(row['decision_ts'])[:10]}",
        "models": {
            name: _project_model(models[name])
            for name in (model, *references)
            if name in models
        },
    }


def _iter_rows(paths: Iterable[Path]) -> Iterable[dict[str, Any]]:
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    value = json.loads(line)
                    if not isinstance(value, dict):
                        raise TypeError(f"expected object row in {path}")
                    yield value


def _broad_metrics(
    summary_paths: list[Path],
    *,
    model: str,
    references: tuple[str, ...],
    minimum_stratum_orders: int,
    minimum_stratum_markets: int,
    quality_args: argparse.Namespace,
) -> dict[str, Any]:
    orders_paths = [path.with_name("orders.jsonl") for path in summary_paths]
    dimension_counts: dict[str, Counter[str]] = {
        name: Counter()
        for name in (
            "category",
            "price_bucket",
            "depth_regime",
            "activity_regime",
            "spread_regime",
            "side",
            "order_size_bucket",
            "utc_session",
        )
    }
    groups: dict[str, dict[str, list[dict[str, Any]]]] = {
        name: defaultdict(list) for name in dimension_counts
    }
    model_totals: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "orders": 0,
            "positive_quantity_orders": 0,
            "quantity": Decimal(0),
            "statuses": Counter(),
            "reasons": Counter(),
        }
    )
    markets: set[int] = set()
    market_days: set[tuple[int, str]] = set()
    dates: set[str] = set()
    prices: list[Decimal] = []
    total_orders = 0

    for row in _iter_rows(orders_paths):
        total_orders += 1
        market_id = int(row["market_id"])
        decision = datetime.fromisoformat(str(row["decision_ts"]))
        day = decision.date().isoformat()
        markets.add(market_id)
        market_days.add((market_id, day))
        dates.add(day)
        price = Decimal(str(row["limit_price"]))
        prices.append(price)
        dimensions = {
            "category": str(row.get("category") or "unknown"),
            "price_bucket": _price_bucket(price),
            "depth_regime": str(row.get("depth_regime") or "unknown"),
            "activity_regime": str(row.get("activity_regime") or "unknown"),
            "spread_regime": str(row.get("spread_regime") or "unknown"),
            "side": str(row.get("side") or "unknown").upper(),
            "order_size_bucket": _order_size_bucket(row["size"]),
            "utc_session": _utc_session(decision),
        }
        compact = _compact_row(row, model=model, references=references)
        for dimension, value in dimensions.items():
            dimension_counts[dimension][value] += 1
            groups[dimension][value].append(compact)

        for name, value in row["models"].items():
            target = model_totals[name]
            size = Decimal(str(value.get("filled_size") or 0))
            target["orders"] += 1
            target["positive_quantity_orders"] += int(size > 0)
            target["quantity"] += size
            target["statuses"][str(value.get("status") or "UNKNOWN")] += 1
            target["reasons"][str(value.get("reason") or "UNKNOWN")] += 1

    strata: dict[str, Any] = {}
    for dimension, values in groups.items():
        strata[dimension] = {}
        for name, rows in sorted(values.items()):
            unique_markets = len({int(row["market_id"]) for row in rows})
            item: dict[str, Any] = {
                "orders": len(rows),
                "unique_markets": unique_markets,
            }
            if (
                len(rows) >= minimum_stratum_orders
                and unique_markets >= minimum_stratum_markets
            ):
                item["comparisons"] = {}
                for reference in references:
                    metrics = comparison._binary_reference_metrics(
                        rows, model=model, reference=reference
                    )
                    observations = stability._quality_observations(
                        rows, model=model, reference=reference
                    )
                    metrics["adaptive_probability_quality"] = (
                        stability._adaptive_quality(observations, quality_args)
                    )
                    item["comparisons"][reference] = metrics
            else:
                item["comparisons"] = None
                item["reason"] = "INSUFFICIENT_INDEPENDENT_STRATUM_SAMPLE"
            strata[dimension][name] = item

    return {
        "orders": total_orders,
        "dates": len(dates),
        "unique_markets": len(markets),
        "market_days": len(market_days),
        "minimum_price": min(prices) if prices else None,
        "maximum_price": max(prices) if prices else None,
        "dimension_counts": {
            name: dict(sorted(values.items()))
            for name, values in dimension_counts.items()
        },
        "models": {
            name: {
                **{
                    key: value
                    for key, value in values.items()
                    if key not in {"statuses", "reasons"}
                },
                "statuses": dict(values["statuses"]),
                "reasons": dict(values["reasons"]),
            }
            for name, values in sorted(model_totals.items())
        },
        "strata": strata,
    }


def _breadth_quality(
    broad: Mapping[str, Any], args: argparse.Namespace
) -> dict[str, Any]:
    counts = broad["dimension_counts"]
    category_strata = sum(
        int(item["orders"]) >= args.minimum_stratum_orders
        and int(item["unique_markets"]) >= args.minimum_stratum_markets
        for item in broad["strata"]["category"].values()
    )
    checks = {
        "minimum_dates": int(broad["dates"]) >= args.minimum_dates,
        "minimum_unique_markets": (
            int(broad["unique_markets"]) >= args.minimum_unique_markets
        ),
        "minimum_market_days": (int(broad["market_days"]) >= args.minimum_market_days),
        "minimum_category_strata": (category_strata >= args.minimum_category_strata),
    }
    for dimension, required in REQUIRED_STRATA.items():
        available = {
            name
            for name, value in counts[dimension].items()
            if int(value) >= args.minimum_stratum_orders
        }
        checks[f"{dimension}_coverage"] = required <= available

    observable_strata: dict[str, Any] = {}
    coverage_summaries: dict[str, Any] = {}
    for dimension in OBSERVABLE_PERFORMANCE_DIMENSIONS:
        observable_strata[dimension] = {}
        for name, item in broad["strata"][dimension].items():
            comparisons = item.get("comparisons")
            if comparisons is None:
                continue
            observable_strata[dimension][name] = {}
            for reference, metrics in comparisons.items():
                support_rate = float(metrics["model_support_rate"])
                out_of_domain = int(
                    metrics.get("excluded", {}).get("MODEL_OUT_OF_DOMAIN", 0)
                )
                reference_ready = int(metrics.get("reference_ready_samples", 0))
                if (
                    dimension == "category"
                    and reference_ready > 0
                    and out_of_domain == reference_ready
                ):
                    observable_strata[dimension][name][reference] = {
                        "orders": int(item["orders"]),
                        "support_rate": support_rate,
                        "status": "MODEL_DOMAIN_UNSUPPORTED",
                        "passed": None,
                    }
                    continue
                adaptive = metrics.get("adaptive_probability_quality") or {}
                order_ratio = metrics.get("all_sample_expected_positive_order_ratio")
                quantity_ratio = metrics.get("all_sample_expected_quantity_ratio")
                reference_positive = int(metrics["reference_positive_orders"])
                if (
                    reference_positive <= 0
                    or order_ratio is None
                    or quantity_ratio is None
                ):
                    continue
                if str(getattr(args, "quality_gate_mode", "fixed")) == "adaptive":
                    adaptive_status = str(adaptive.get("status") or "")
                    passed = (
                        None
                        if adaptive_status == "INSUFFICIENT_SAMPLE"
                        else support_rate >= args.minimum_support_rate
                        and adaptive_status in {"PASS", "INCONCLUSIVE"}
                    )
                else:
                    passed = (
                        support_rate >= args.minimum_support_rate
                        and args.minimum_stratum_ratio
                        <= float(order_ratio)
                        <= args.maximum_stratum_ratio
                        and args.minimum_stratum_ratio
                        <= float(quantity_ratio)
                        <= args.maximum_stratum_ratio
                    )
                observable_strata[dimension][name][reference] = {
                    "orders": int(item["orders"]),
                    "support_rate": support_rate,
                    "expected_order_ratio": float(order_ratio),
                    "expected_quantity_ratio": float(quantity_ratio),
                    "adaptive_probability_quality": adaptive,
                    "passed": passed,
                }
        for reference in args.references:
            evaluated = [
                values[reference]
                for values in observable_strata[dimension].values()
                if reference in values and values[reference].get("passed") is not None
            ]
            passed_count = sum(value["passed"] is True for value in evaluated)
            pass_rate = passed_count / len(evaluated) if evaluated else None
            key = f"{dimension}:{reference}:coverage_pass_rate"
            coverage_summaries[key] = {
                "evaluated": len(evaluated),
                "passed": passed_count,
                "pass_rate": pass_rate,
            }
            if str(getattr(args, "quality_gate_mode", "fixed")) == "adaptive":
                evaluable_key = f"{dimension}:{reference}:has_evaluable_strata"
                checks[evaluable_key] = bool(evaluated)
                coverage_summaries[evaluable_key] = {
                    "evaluated": len(evaluated),
                }
                adaptive_failures = sum(
                    value.get("adaptive_probability_quality", {}).get("status")
                    == "FAIL"
                    for value in evaluated
                )
                failure_key = f"{dimension}:{reference}:no_calibration_failure"
                checks[failure_key] = adaptive_failures == 0
                coverage_summaries[failure_key] = {
                    "evaluated": len(evaluated),
                    "failed": adaptive_failures,
                }
            else:
                checks[key] = (
                    pass_rate is not None
                    and pass_rate >= args.minimum_coverage_pass_rate
                )
    supported_category_strata = sum(
        any(
            metrics.get("status") != "MODEL_DOMAIN_UNSUPPORTED"
            for metrics in references.values()
        )
        for references in observable_strata["category"].values()
    )
    checks["minimum_supported_category_strata"] = (
        supported_category_strata >= args.minimum_category_strata
    )
    result = {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "minimum_dates": args.minimum_dates,
        "minimum_unique_markets": args.minimum_unique_markets,
        "minimum_market_days": args.minimum_market_days,
        "minimum_category_strata": args.minimum_category_strata,
        "minimum_stratum_orders": args.minimum_stratum_orders,
        "minimum_stratum_markets": args.minimum_stratum_markets,
        "minimum_coverage_pass_rate": args.minimum_coverage_pass_rate,
        "eligible_category_strata": category_strata,
        "supported_category_strata": supported_category_strata,
        "observable_performance_dimensions": list(OBSERVABLE_PERFORMANCE_DIMENSIONS),
        "observable_strata": observable_strata,
        "coverage_summaries": coverage_summaries,
        "calibration_warnings": [
            f"{dimension}:{name}:{reference}"
            for dimension, groups in observable_strata.items()
            for name, references in groups.items()
            for reference, metrics in references.items()
            if metrics.get("passed") is False
        ],
        "failures": [name for name, passed in checks.items() if not passed],
    }
    if str(getattr(args, "quality_gate_mode", "fixed")) == "fixed":
        result["minimum_stratum_ratio"] = args.minimum_stratum_ratio
        result["maximum_stratum_ratio"] = args.maximum_stratum_ratio
        result["legacy_fixed_thresholds_applied"] = True
    else:
        result["decision_rule"] = (
            "REFERENCE_RELATIVE_BRIER_AND_LOG_LOSS_WITH_PAIRED_"
            "MARKET_DAY_CLUSTER_BOOTSTRAP"
        )
        result["legacy_fixed_thresholds_applied"] = False
    return result


def _successful_summaries(manifest: Mapping[str, Any]) -> list[Path]:
    result: list[Path] = []
    for value in manifest.get("attempts", []):
        if value.get("status") != "PASS":
            continue
        path = Path(str(value["summary_path"]))
        if path.exists():
            result.append(path)
    return sorted(set(result))


def _total_orders(paths: Iterable[Path]) -> int:
    return sum(int(_load_json(path)["cohort"]["orders"]) for path in paths)


def _attempt_window(
    args: argparse.Namespace,
    manifest: dict[str, Any],
    *,
    start: datetime,
    end: datetime,
) -> None:
    key = _window_key(start, end)
    existing = next(
        (item for item in manifest["attempts"] if item["window_key"] == key), None
    )
    if existing is not None and not (
        args.retry_failures and existing.get("status") != "PASS"
    ):
        return
    if existing is not None:
        manifest["attempts"].remove(existing)

    output_dir = _window_dir(args.output_root, start)
    summary_path = output_dir / "summary.json"
    log_path = output_dir / "run.log"
    output_dir.mkdir(parents=True, exist_ok=True)
    excluded_registered = _registered_market_ids(
        args.window_registry,
        start=start,
        end=end,
        exclude_all_registered=(
            str(getattr(args, "market_reuse_policy", "overlap_only")) == "never"
        ),
    )
    attempt: dict[str, Any] = {
        "window_key": key,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "output_dir": str(output_dir),
        "summary_path": str(summary_path),
        "excluded_registered_markets": len(excluded_registered),
    }
    started = perf_counter()
    if not _archive_window_exists(args.archive, start):
        attempt.update(
            status="DATA_NOT_AVAILABLE",
            reason="L2_ARCHIVE_HOUR_MISSING",
            elapsed_seconds=perf_counter() - started,
        )
    else:
        command = _batch_command(args, start=start, end=end, output_dir=output_dir)
        with log_path.open("w", encoding="utf-8") as log:
            log.write("$ " + shlex.join(command) + "\n")
            log.flush()
            completed = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
                env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
            )
        if completed.returncode == 0 and summary_path.exists():
            summary = _load_json(summary_path)
            attempt.update(
                status="PASS",
                orders=int(summary["cohort"]["orders"]),
                markets=int(summary["cohort"]["markets"]),
                categories=summary["cohort"].get("categories", {}),
                elapsed_seconds=perf_counter() - started,
            )
        else:
            error_tail = _tail(log_path)
            if "cannot be bracketed by trade_prints_one_sided" in error_tail:
                status = "TRADE_TAPE_NOT_AVAILABLE"
                reason = "TRADE_TAPE_ANCHOR_UNRESOLVABLE"
            elif "no identity-valid L2/trade-tape candidate markets" in error_tail:
                status = "DATA_NOT_AVAILABLE"
                reason = "NO_IDENTITY_VALID_L2_TRADE_TAPE_MARKETS"
            elif any(
                marker in error_tail
                for marker in (
                    "remote L2 slice materialization failed",
                    "Connection timed out",
                    "No route to host",
                    "Transport endpoint is not connected",
                )
            ):
                status = "DATA_NOT_AVAILABLE"
                reason = "L2_ARCHIVE_REMOTE_UNAVAILABLE"
            else:
                status = "FAILED"
                reason = "BATCH_COMPARISON_FAILED"
            attempt.update(
                status=status,
                reason=reason,
                return_code=completed.returncode,
                error_tail=error_tail,
                elapsed_seconds=perf_counter() - started,
            )
    manifest["attempts"].append(attempt)
    manifest["updated_at"] = datetime.now(UTC).isoformat()
    _write_json(args.output_root / "run_manifest.json", manifest)
    print(
        f"[{attempt['status']}] {start.isoformat()} orders={attempt.get('orders', 0)} "
        f"elapsed={attempt['elapsed_seconds']:.1f}s",
        flush=True,
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    args.output_root.mkdir(parents=True, exist_ok=True)
    manifest_path = args.output_root / "run_manifest.json"
    run_contract = {
        "model": args.model,
        "references": list(args.references),
        "models": args.models,
        "order_sizes": [str(value) for value in args.order_sizes],
        "order_sides": list(args.order_sides),
        "order_tif": args.order_tif,
        "cohort_split": args.cohort_split,
        "market_reuse_policy": args.market_reuse_policy,
        "window_minutes": args.window_minutes,
        "archive_shard_count": args.archive_shard_count,
        "l2_source": args.l2_source,
        "v3_probability_artifact": str(args.v3_probability_artifact or ""),
        "v3_fak_probability_artifact": str(args.v3_fak_probability_artifact or ""),
        "v3_fok_probability_artifact": str(args.v3_fok_probability_artifact or ""),
    }
    manifest = (
        _load_json(manifest_path)
        if manifest_path.exists()
        else {
            "schema_version": "fill_only_v3_large_cross_validation_run_v2",
            "created_at": datetime.now(UTC).isoformat(),
            "target_orders": args.target_orders,
            "run_contract": run_contract,
            "attempts": [],
        }
    )
    existing_contract = manifest.get("run_contract")
    if existing_contract is None and manifest.get("attempts"):
        raise RuntimeError(
            "existing run manifest predates the versioned run contract; use a new output root"
        )
    if existing_contract is not None and existing_contract != run_contract:
        raise RuntimeError(
            "output root already belongs to a different run contract; use a new output root"
        )
    _normalize_attempt_statuses(manifest)

    maximum_windows = (
        min(args.maximum_windows, len(args.window_starts))
        if args.window_starts
        else args.maximum_windows
    )
    for index in range(maximum_windows):
        paths = _successful_summaries(manifest)
        if (
            _total_orders(paths) >= args.target_orders
            and len(paths) >= args.minimum_successful_windows
        ):
            break
        if args.window_starts:
            start = args.window_starts[index]
            end = start + timedelta(minutes=args.window_minutes) - timedelta(seconds=1)
        else:
            start, end = _window(args.start_date, index, args.hours)
        _attempt_window(args, manifest, start=start, end=end)

    summaries = _successful_summaries(manifest)
    total_orders = _total_orders(summaries)
    manifest["successful_windows"] = len(summaries)
    manifest["successful_orders"] = total_orders
    manifest["target_orders"] = args.target_orders
    manifest["target_reached"] = total_orders >= args.target_orders
    manifest["updated_at"] = datetime.now(UTC).isoformat()
    _write_json(manifest_path, manifest)

    result: dict[str, Any] = {
        "schema_version": "fill_only_v3_large_cross_validation_v2",
        "status": "INSUFFICIENT_SAMPLE",
        "run_manifest": str(manifest_path),
        "target_orders": args.target_orders,
        "successful_orders": total_orders,
        "successful_windows": len(summaries),
        "failed_or_unavailable_windows": sum(
            item.get("status") != "PASS" for item in manifest["attempts"]
        ),
    }
    if total_orders >= args.target_orders:
        quality_args = argparse.Namespace(
            summaries=summaries,
            model=args.model,
            references=list(args.references),
            minimum_support_rate=args.minimum_support_rate,
            minimum_total_orders=args.target_orders,
            minimum_windows=args.minimum_successful_windows,
            minimum_ratio=args.minimum_ratio,
            maximum_ratio=args.maximum_ratio,
            maximum_brier_score=args.maximum_brier_score,
            maximum_brier_regret=args.maximum_brier_regret,
            quality_gate_mode=args.quality_gate_mode,
            confidence_level=args.confidence_level,
            bootstrap_replicates=args.bootstrap_replicates,
            bootstrap_seed=args.bootstrap_seed,
            minimum_adaptive_samples=args.minimum_adaptive_samples,
            minimum_adaptive_positive_samples=args.minimum_adaptive_positive_samples,
            minimum_adaptive_negative_samples=args.minimum_adaptive_negative_samples,
            minimum_adaptive_clusters=args.minimum_adaptive_clusters,
            output=None,
        )
        quality = stability.validate(quality_args)
        broad = _broad_metrics(
            summaries,
            model=args.model,
            references=args.references,
            minimum_stratum_orders=args.minimum_stratum_orders,
            minimum_stratum_markets=args.minimum_stratum_markets,
            quality_args=quality_args,
        )
        breadth_quality = _breadth_quality(broad, args)
        result.update(
            status=stability.combine_quality_statuses(
                str(quality["status"]), str(breadth_quality["status"])
            ),
            quality=quality,
            breadth=broad,
            breadth_quality=breadth_quality,
            successful_summaries=[str(path) for path in summaries],
        )

    _write_json(args.output_root / "result.json", result)
    return result


def _result_receipt(result: Mapping[str, Any], output_root: Path) -> dict[str, Any]:
    quality = result.get("quality") or {}
    comparisons = quality.get("aggregate") or quality.get("comparisons") or {}
    return {
        "status": result["status"],
        "result_path": str(output_root / "result.json"),
        "successful_orders": result["successful_orders"],
        "successful_windows": result["successful_windows"],
        "failed_or_unavailable_windows": result["failed_or_unavailable_windows"],
        "quality_status": quality.get("status"),
        "comparisons": {
            reference: {
                "status": (metrics.get("adaptive_probability_quality") or {}).get(
                    "status"
                ),
                "support_rate": metrics.get("support_rate"),
                "expected_order_ratio": metrics.get("expected_positive_order_ratio"),
                "expected_quantity_ratio": metrics.get("expected_quantity_ratio"),
                "all_sample_expected_order_ratio": metrics.get(
                    "all_sample_expected_positive_order_ratio"
                ),
                "all_sample_expected_quantity_ratio": metrics.get(
                    "all_sample_expected_quantity_ratio"
                ),
                "adaptive_probability_quality": {
                    key: (metrics.get("adaptive_probability_quality") or {}).get(key)
                    for key in (
                        "status",
                        "brier_score",
                        "reference_brier_score",
                        "brier_regret",
                        "log_loss_regret",
                        "calibration_in_the_large",
                    )
                },
            }
            for reference, metrics in comparisons.items()
        },
        "breadth_status": (result.get("breadth_quality") or {}).get("status"),
        "breadth_failures": (result.get("breadth_quality") or {}).get("failures", []),
        "calibration_warnings": (result.get("breadth_quality") or {}).get(
            "calibration_warnings", []
        ),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=_parse_date, default=date(2026, 7, 24))
    parser.add_argument("--hours", type=_parse_hours, default=(0, 4, 8, 12, 16, 20))
    parser.add_argument(
        "--window-starts",
        type=_parse_window_starts,
        default=(),
        help="optional comma-separated UTC timestamps; overrides start-date/hours",
    )
    parser.add_argument("--target-orders", type=int, default=10_001)
    parser.add_argument("--window-minutes", type=int, default=60)
    parser.add_argument("--maximum-windows", type=int, default=20)
    parser.add_argument("--minimum-successful-windows", type=int, default=10)
    parser.add_argument("--markets-per-window", type=int, default=50)
    parser.add_argument("--orders-per-market", type=int, default=20)
    parser.add_argument("--candidate-multiplier", type=int, default=4)
    parser.add_argument("--models", default=DEFAULT_MODELS)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument(
        "--references",
        default=",".join(DEFAULT_REFERENCES),
        help="comma-separated reference models used by all quality gates",
    )
    parser.add_argument("--order-sizes", default="10")
    parser.add_argument("--order-sides", default="BUY")
    parser.add_argument("--order-tif", choices=("FAK", "FOK"), default="FAK")
    parser.add_argument(
        "--cohort-split",
        choices=("calibration", "validation", "test", "sensitivity"),
        default="test",
    )
    parser.add_argument(
        "--market-reuse-policy",
        choices=("never", "overlap_only"),
        default="never",
        help="never excludes every market already recorded in this validation registry",
    )
    parser.add_argument("--v3-probability-artifact", type=Path)
    parser.add_argument("--v3-fak-probability-artifact", type=Path)
    parser.add_argument("--v3-fok-probability-artifact", type=Path)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--archive-baseline-lookback-hours", type=int, default=0)
    parser.add_argument("--archive-shard-count", type=int, default=48)
    parser.add_argument("--l2-source", default="polymarket_market_ws_archive")
    parser.add_argument("--no-l2-slice-cache", action="store_true")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--window-registry",
        type=Path,
        default=(
            PROJECT_ROOT
            / "backtest_framework"
            / "nautilus_trader_comparison"
            / "fill_only_cross_validation_registry.jsonl"
        ),
    )
    parser.add_argument(
        "--python",
        type=Path,
        default=Path("/home/jiahuaiyu/.conda/envs/prediction-market-quant/bin/python"),
    )
    parser.add_argument(
        "--nautilus-python",
        type=Path,
        default=Path("/home/jiahuaiyu/.conda/envs/polymonitor-nautilus312/bin/python"),
    )
    parser.add_argument(
        "--pml2-backend",
        choices=("auto", "python", "rust"),
        default="auto",
    )
    parser.add_argument("--retry-failures", action="store_true")
    parser.add_argument("--minimum-stratum-orders", type=int, default=100)
    parser.add_argument("--minimum-stratum-markets", type=int, default=5)
    parser.add_argument("--minimum-stratum-ratio", type=float, default=0.80)
    parser.add_argument("--maximum-stratum-ratio", type=float, default=1.20)
    parser.add_argument("--minimum-coverage-pass-rate", type=float, default=0.80)
    parser.add_argument("--minimum-dates", type=int, default=3)
    parser.add_argument("--minimum-unique-markets", type=int, default=100)
    parser.add_argument("--minimum-market-days", type=int, default=150)
    parser.add_argument("--minimum-category-strata", type=int, default=5)
    parser.add_argument("--minimum-support-rate", type=float, default=0.80)
    parser.add_argument("--minimum-ratio", type=float, default=0.85)
    parser.add_argument("--maximum-ratio", type=float, default=1.15)
    parser.add_argument("--maximum-brier-score", type=float, default=0.20)
    parser.add_argument("--maximum-brier-regret", type=float, default=0.01)
    parser.add_argument(
        "--quality-gate-mode",
        choices=("adaptive", "fixed"),
        default="adaptive",
    )
    parser.add_argument("--confidence-level", type=float, default=0.95)
    parser.add_argument("--bootstrap-replicates", type=int, default=1_000)
    parser.add_argument("--bootstrap-seed", type=int, default=73)
    parser.add_argument("--minimum-adaptive-samples", type=int, default=200)
    parser.add_argument("--minimum-adaptive-positive-samples", type=int, default=20)
    parser.add_argument("--minimum-adaptive-negative-samples", type=int, default=20)
    parser.add_argument("--minimum-adaptive-clusters", type=int, default=20)
    parser.add_argument(
        "--print-full-result",
        action="store_true",
        help="print the full result; it is always persisted to result.json",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    args.references = tuple(
        item.strip() for item in args.references.split(",") if item.strip()
    )
    args.order_sizes = tuple(
        Decimal(item.strip()) for item in args.order_sizes.split(",") if item.strip()
    )
    args.order_sides = tuple(
        item.strip().upper() for item in args.order_sides.split(",") if item.strip()
    )
    if args.target_orders <= 10_000:
        raise ValueError("target-orders must exceed 10,000")
    if args.window_minutes <= 0:
        raise ValueError("--window-minutes must be positive")
    if not args.references:
        raise ValueError("at least one --references model is required")
    if not args.order_sizes or any(value <= 0 for value in args.order_sizes):
        raise ValueError("--order-sizes must contain positive values")
    if not args.order_sides or set(args.order_sides) - {"BUY", "SELL"}:
        raise ValueError("--order-sides must contain BUY and/or SELL")
    selected_models = {item.strip() for item in args.models.split(",") if item.strip()}
    missing = {args.model, *args.references} - selected_models
    if missing:
        raise ValueError(f"--models is missing quality model(s): {sorted(missing)}")
    result = run(args)
    output = (
        result if args.print_full_result else _result_receipt(result, args.output_root)
    )
    print(json.dumps(output, ensure_ascii=False, indent=2, default=_json_default))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
